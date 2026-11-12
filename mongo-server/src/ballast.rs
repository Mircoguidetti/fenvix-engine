//! The step 9 ballast generator: `mongo-server <the round's server options> --ballast PLAN.json`.
//!
//! generator-1tb (2026-11-12): one database of any size on several cores, resumable. The database's customers are cut
//! into slices (whole customers, whole planned groups); several writers take slices from a queue. Each writer runs the
//! server's insert code against its own private copy of the database's catalog (a scratch database on an in-memory
//! store holding a copy of the real catalog's rows, checked byte for byte against the real catalog before generating
//! and before publishing), writes its slice's customer rows straight into sealed runs, and cuts its slice's shared rows into key
//! partitions. A slice is committed in the pantry (`generator/<db>/slices/`), so a killed run resumes where it stopped,
//! on any machine. At the end the key partitions are merged in parallel and one version is published, as before.
//! Insertion numbers and ObjectId counters start at each slice's base (slice << 36), so the stored data does not
//! depend on the number of writers or on where a run was interrupted.
//!
//! It writes ballast databases (the 100 GB and 1 TB sizes of step 9) straight into the pantry in the stored format the
//! server reads with compaction 2 on: sealed runs and one published version of our manifest per database, keys of
//! layout v2. No client, no wire, no WAL, no compaction rewrite.
//!
//! Same code paths as the server, by construction: this IS the server process, started with the same options as the
//! round's cook (`--customer-key`, `--sealed-runs`, envelopes, group size), and every row comes from the server's own
//! code:
//! - the catalog (collections, the indexes the founder's script creates, collection options, the customer key) through
//!   the server's command handler, in this process (`Server::handle`, no socket), committed normally to SlateDB;
//! - every document through `Writes::put`, the insert path of the `insert` command: the document row with its
//!   insertion number, every index entry, the locator of a customer-keyed collection, the insertion-order entry and
//!   counter, the storage counters (`collstat::ops_for`, as `commit` does), mapped to layout v2 by `layout2::phys_ops`
//!   (as `Entry::apply` does). What changes is only where the rows go: not to the WAL, but to sealed runs
//!   (`fx/build.rs`, [`GroupWriter`] and [`publish_generated`]).
//!
//! The documents: the founder's script's shapes (`prova_infinite.py`, `doc_messaggio`, `doc_evento`, `doc_ordine`, the
//! same vocabulary with its zipf weights, the same mix 50/35/15), its customers (the plan gives each customer's size and
//! the dormant ones, computed by the script's own `prepara_clienti` with the line's seed: `step9/machine/ballast_plan.py`)
//! and its timeline (one simulated year; dormant customers write in the first 60% only; each customer's bytes split
//! between the two phases as the script's weighted draw splits them on average). Our own random generator, not
//! Python's: the same distributions, not the same bytes. Each database gets a report (`<out>/<db>.gen.json`) from which
//! `ballast_states.py` writes a state file in the script's format (customers, dormant ones, bytes per customer, one
//! order `_id` per customer for Q2), so the script's read rounds run on the ballast unchanged.

use crate::cmd::Server;
use crate::coll::Writes;
use crate::dbs::Entry;
use anyhow::{anyhow, bail, ensure, Context, Result};
use bson::oid::ObjectId;
use bson::{doc, Bson, Document};
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use tenant_bench::fx::build::{BuildOpts, GroupWriter, PartRows, SortedRows};
use tenant_bench::fx::manifest::{self, GroupLine};
use tenant_bench::lifecycle::Op;

const GB: f64 = (1u64 << 30) as f64;
/// Documents per write set: the founder's script inserts in batches of 1,000 (`lotto`), so this is the client's own
/// write set; one write set per customer kept thousands of documents in its overlay (MEASURED: 18% slower).
const BATCH: usize = 1000;
/// Raw bytes of the customer space (documents and co-located index entries) per BSON byte (MEASURED 1.499 on 1 GiB,
/// seed 2001): used only to plan the group cuts at customer boundaries.
const RAW_PER_BSON: f64 = 1.5;
/// Raw bytes of the shared keyspace (locators, the stato_ts index, insertion order) per BSON byte (MEASURED 0.445 on the
/// same 1 GiB): used only to choose how many key partitions.
const SHARED_RAW_PER_BSON: f64 = 0.445;
/// Insertion numbers and ObjectId counters of slice s start at s << SLICE_BITS.
const SLICE_BITS: u32 = 36;
const COLLS: [&str; 3] = ["messaggi", "eventi", "ordini"];
const RUOLI: [&str; 3] = ["user", "assistant", "tool"];
const MODELLI: [&str; 3] = ["small-2026", "medium-2026", "large-2026"];
const TIPI_EV: [&str; 6] = ["lettura", "stato", "allarme", "batteria", "posizione", "avvio"];
const STATI: [&str; 5] = ["nuovo", "pagato", "spedito", "consegnato", "reso"];
const CITTA: [&str; 10] = ["Milano", "Roma", "Torino", "Napoli", "Bologna", "Firenze", "Berlin", "Paris", "Madrid", "Lyon"];

#[derive(Clone)]
pub struct DbPlan {
    pub db: String,
    pub seed: u64,
    /// GiB of BSON, as the script's `--gb`.
    pub gb: f64,
    /// Each customer's size in MB and the dormant customers, from the script's `prepara_clienti`.
    pub taglie: Vec<f64>,
    pub riservati: Vec<usize>,
    /// One byte of every `_id` of this database (unique `_id`s across the ballast).
    pub tag: u8,
}

pub struct Plan {
    pub databases: Vec<DbPlan>,
    pub parallel: usize,
    pub spill_dir: String,
    pub spill_mb: usize,
    pub out: String,
    pub vocab: Vec<String>,
    pub cum_vocab: Vec<f64>,
    pub t_inizio: i64,
    pub t_span_s: i64,
    pub fine_dormienti: f64,
    /// Dump every document of every `sample_every`-th customer (0: none), at most `sample_max` customers per database:
    /// the reference the check loads into mongod.
    pub sample_every: usize,
    pub sample_max: usize,
    /// Writers per database (0: `parallel`, then the machine's cores). Databases are generated one after another.
    pub workers: usize,
    /// GiB of BSON per slice (0: at least 1, at most 256 slices per database).
    pub slice_gb: f64,
    /// Key partitions of the shared rows (0: twice the writers).
    pub parts: usize,
    /// Customers generated to choose the partition bounds.
    pub bound_sample: usize,
}

fn parse_plan(v: &serde_json::Value) -> Result<Plan> {
    let f = |k: &str| v.get(k).ok_or_else(|| anyhow!("plan: {k} missing"));
    let u = |k: &str, d: u64| v.get(k).and_then(|x| x.as_u64()).unwrap_or(d) as usize;
    let st = |x: &serde_json::Value| -> Result<String> { Ok(x.as_str().ok_or_else(|| anyhow!("plan: a string"))?.to_string()) };
    let fl = |x: &serde_json::Value| -> Result<f64> { x.as_f64().ok_or_else(|| anyhow!("plan: a number")) };
    let mut databases = Vec::new();
    for d in f("databases")?.as_array().ok_or_else(|| anyhow!("plan: databases"))? {
        let g = |k: &str| d.get(k).ok_or_else(|| anyhow!("plan: databases[].{k} missing"));
        databases.push(DbPlan {
            db: st(g("db")?)?,
            seed: g("seed")?.as_u64().ok_or_else(|| anyhow!("plan: seed"))?,
            gb: fl(g("gb")?)?,
            taglie: g("taglie")?.as_array().ok_or_else(|| anyhow!("plan: taglie"))?.iter().map(fl).collect::<Result<_>>()?,
            riservati: g("riservati")?.as_array().ok_or_else(|| anyhow!("plan: riservati"))?.iter().map(|x| x.as_u64().map(|n| n as usize).ok_or_else(|| anyhow!("plan: riservati"))).collect::<Result<_>>()?,
            tag: g("tag")?.as_u64().ok_or_else(|| anyhow!("plan: tag"))? as u8,
        });
    }
    Ok(Plan {
        databases,
        parallel: u("parallel", 1),
        spill_dir: st(f("spill_dir")?)?,
        spill_mb: u("spill_mb", 256),
        out: st(f("out")?)?,
        vocab: f("vocab")?.as_array().ok_or_else(|| anyhow!("plan: vocab"))?.iter().map(st).collect::<Result<_>>()?,
        cum_vocab: f("cum_vocab")?.as_array().ok_or_else(|| anyhow!("plan: cum_vocab"))?.iter().map(fl).collect::<Result<_>>()?,
        t_inizio: f("t_inizio")?.as_i64().ok_or_else(|| anyhow!("plan: t_inizio"))?,
        t_span_s: f("t_span_s")?.as_i64().ok_or_else(|| anyhow!("plan: t_span_s"))?,
        fine_dormienti: fl(f("fine_dormienti")?)?,
        sample_every: u("sample_every", 0),
        sample_max: u("sample_max", 0),
        workers: u("workers", 0),
        slice_gb: v.get("slice_gb").and_then(|x| x.as_f64()).unwrap_or(0.0),
        parts: u("parts", 0),
        bound_sample: u("bound_sample", 64),
    })
}

/// SplitMix64: small, fast, seeded per customer (the generator needs speed, not Python's exact bytes).
pub struct SmallRng(u64);

impl SmallRng {
    pub fn seed_from_u64(s: u64) -> SmallRng {
        SmallRng(s ^ 0x9E37_79B9_7F4A_7C15)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// Uniform in [0, 1).
    pub fn f(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }
    /// Uniform integer in [lo, hi).
    pub fn int(&mut self, lo: i64, hi: i64) -> i64 {
        lo + ((self.next_u64() as u128 * (hi - lo) as u128) >> 64) as i64
    }
    /// Uniform in [lo, hi).
    pub fn real(&mut self, lo: f64, hi: f64) -> f64 {
        lo + self.f() * (hi - lo)
    }
}

struct Words {
    vocab: Vec<String>,
    cum: Vec<f64>,
}

impl Words {
    /// The script's `testo(rng, n)`: zipf words, sometimes a number, cut at n characters.
    fn testo(&self, rng: &mut SmallRng, n: usize) -> String {
        let k = n / 5 + 2;
        let total = *self.cum.last().unwrap();
        let mut words: Vec<&str> = (0..k)
            .map(|_| {
                let x = rng.f() * total;
                let i = self.cum.partition_point(|c| *c <= x).min(self.vocab.len() - 1);
                self.vocab[i].as_str()
            })
            .collect();
        let num;
        if rng.f() < 0.5 {
            num = rng.int(0, 1_000_000).to_string();
            let j = rng.int(0, words.len() as i64) as usize;
            words[j] = num.as_str();
        }
        let mut s = words.join(" ");
        if s.len() > n {
            let mut cut = n;
            while !s.is_char_boundary(cut) {
                cut -= 1;
            }
            s.truncate(cut);
        }
        s
    }
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

fn gen_doc(w: &Words, rng: &mut SmallRng, coll: &str, cli: &str, ts: bson::DateTime) -> Document {
    match coll {
        "messaggi" => {
            let conv = format!("conv-{}", rng.int(0, 40));
            let ruolo = RUOLI[rng.int(0, 3) as usize];
            let modello = MODELLI[rng.int(0, 3) as usize];
            let token = rng.int(20, 2000) as i32;
            let n = rng.int(60, 340) as usize;
            doc! { "cliente": cli, "ts": ts, "conv": conv, "ruolo": ruolo, "modello": modello, "token": token, "testo": w.testo(rng, n) }
        }
        "eventi" => {
            let disp = format!("dev-{}", rng.int(0, 8));
            let tipo = TIPI_EV[rng.int(0, 6) as usize];
            let temp = round2(rng.real(15.0, 40.0));
            let batt = rng.int(0, 101) as i32;
            let rssi = -(rng.int(30, 95) as i32);
            let ok = rng.f() < 0.97;
            let n = rng.int(10, 120) as usize;
            doc! { "cliente": cli, "ts": ts, "disp": disp, "tipo": tipo, "fw": "1.4.2",
                   "valori": { "temp": temp, "batt": batt, "rssi": rssi, "ok": ok }, "nota": w.testo(rng, n) }
        }
        _ => {
            let nrighe = rng.int(1, 5) as usize;
            let mut righe = Vec::with_capacity(nrighe);
            let mut totale = 0.0;
            for _ in 0..nrighe {
                let sku = format!("SKU-{:05}", rng.int(0, 5000));
                let q = rng.int(1, 4) as i32;
                let prezzo = round2(rng.real(3.0, 250.0));
                let n = rng.int(20, 60) as usize;
                totale += prezzo * q as f64;
                righe.push(Bson::Document(doc! { "sku": sku, "q": q, "prezzo": prezzo, "desc": w.testo(rng, n) }));
            }
            let stato = STATI[rng.int(0, 5) as usize];
            let via = w.testo(rng, 24);
            let citta = CITTA[rng.int(0, CITTA.len() as i64) as usize];
            let cap = format!("{}", rng.int(10000, 99999));
            let n = rng.int(40, 200) as usize;
            doc! { "cliente": cli, "ts": ts, "stato": stato, "righe": righe, "totale": round2(totale), "valuta": "EUR",
                   "indirizzo": { "via": via, "citta": citta, "cap": cap }, "note": w.testo(rng, n) }
        }
    }
}

// ----------------------------------------------------------------------------------------------- the sorter on disk

/// Rows of the shared keyspace, sorted on local disk: sorted chunks (zstd), then a k-way merge. For a key written more
/// than once the last write wins (the insertion counter `meta/nat/...` is written with every document).
struct Spill {
    dir: PathBuf,
    limit: usize,
    buf: Vec<(Vec<u8>, Vec<u8>)>,
    bytes: usize,
    files: Vec<PathBuf>,
    pub rows_in: u64,
    pub disk_bytes: u64,
}

impl Spill {
    fn new(dir: PathBuf, limit: usize) -> Result<Spill> {
        std::fs::create_dir_all(&dir)?;
        Ok(Spill { dir, limit, buf: Vec::new(), bytes: 0, files: Vec::new(), rows_in: 0, disk_bytes: 0 })
    }

    fn push(&mut self, k: Vec<u8>, v: Vec<u8>) -> Result<()> {
        self.bytes += k.len() + v.len() + 64;
        self.rows_in += 1;
        self.buf.push((k, v));
        if self.bytes >= self.limit {
            self.flush()?;
        }
        Ok(())
    }

    fn sorted_dedup(buf: &mut Vec<(Vec<u8>, Vec<u8>)>) -> Vec<(Vec<u8>, Vec<u8>)> {
        // stable: equal keys keep their write order, and the last one is kept
        buf.sort_by(|a, b| a.0.cmp(&b.0));
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(buf.len());
        for r in buf.drain(..) {
            match out.last_mut() {
                Some(l) if l.0 == r.0 => *l = r,
                _ => out.push(r),
            }
        }
        out
    }

    fn flush(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let rows = Self::sorted_dedup(&mut self.buf);
        let path = self.dir.join(format!("chunk-{:05}.zst", self.files.len()));
        let f = std::fs::File::create(&path).with_context(|| format!("spill file {}", path.display()))?;
        let mut z = zstd::stream::write::Encoder::new(BufWriter::with_capacity(1 << 20, f), 1)?;
        for (k, v) in &rows {
            z.write_all(&(k.len() as u32).to_le_bytes())?;
            z.write_all(k)?;
            z.write_all(&(v.len() as u32).to_le_bytes())?;
            z.write_all(v)?;
        }
        let mut w = z.finish()?;
        w.flush()?;
        drop(w);
        self.disk_bytes += std::fs::metadata(&path)?.len();
        self.files.push(path);
        self.bytes = 0;
        Ok(())
    }

    /// The merged, sorted rows. The last chunk stays in memory.
    fn into_rows(mut self) -> Result<Merge> {
        let mem = Self::sorted_dedup(&mut self.buf);
        let mut sources: Vec<Source> = Vec::new();
        for p in &self.files {
            let f = std::fs::File::open(p)?;
            sources.push(Source::File(zstd::stream::read::Decoder::with_buffer(BufReader::with_capacity(1 << 20, f))?));
        }
        sources.push(Source::Mem(mem.into_iter()));
        let mut m = Merge { sources, heap: BinaryHeap::new(), files: self.files, wrap: true, strict: false, dups: 0 };
        for i in 0..m.sources.len() {
            if let Some((k, v)) = m.sources[i].next()? {
                m.heap.push(Head { key: k, src: i, val: v });
            }
        }
        Ok(m)
    }
}

enum Source {
    File(zstd::stream::read::Decoder<'static, BufReader<std::fs::File>>),
    Mem(std::vec::IntoIter<(Vec<u8>, Vec<u8>)>),
}

impl Source {
    fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        match self {
            Source::Mem(it) => Ok(it.next()),
            Source::File(r) => {
                let mut n = [0u8; 4];
                match r.read_exact(&mut n) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
                    Err(e) => return Err(e.into()),
                }
                let mut k = vec![0u8; u32::from_le_bytes(n) as usize];
                r.read_exact(&mut k)?;
                r.read_exact(&mut n)?;
                let mut v = vec![0u8; u32::from_le_bytes(n) as usize];
                r.read_exact(&mut v)?;
                Ok(Some((k, v)))
            }
        }
    }
}

#[derive(PartialEq, Eq)]
struct Head {
    key: Vec<u8>,
    src: usize,
    val: Vec<u8>,
}
impl Ord for Head {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        // a min-heap on the key; among equal keys the newest source (the highest index) first
        o.key.cmp(&self.key).then(self.src.cmp(&o.src))
    }
}
impl PartialOrd for Head {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}

struct Merge {
    sources: Vec<Source>,
    heap: BinaryHeap<Head>,
    files: Vec<PathBuf>,
    /// Values are raw (wrap them in a live envelope) or already stored envelopes.
    wrap: bool,
    /// A key in two sources is an error (the slices of one database never write the same shared key).
    strict: bool,
    /// Older writes of a key dropped.
    dups: u64,
}

impl Merge {
    /// A merge of files of (key, stored envelope) rows, each sorted with one row per key, and rows given in memory.
    fn of_files(files: Vec<PathBuf>, mem: Vec<(Vec<u8>, Vec<u8>)>, strict: bool) -> Result<Merge> {
        let mut sources: Vec<Source> = Vec::new();
        for p in &files {
            let f = std::fs::File::open(p).with_context(|| format!("partition file {}", p.display()))?;
            sources.push(Source::File(zstd::stream::read::Decoder::with_buffer(BufReader::with_capacity(64 << 10, f))?));
        }
        sources.push(Source::Mem(mem.into_iter()));
        let mut m = Merge { sources, heap: BinaryHeap::new(), files, wrap: false, strict, dups: 0 };
        for i in 0..m.sources.len() {
            if let Some((k, v)) = m.sources[i].next()? {
                m.heap.push(Head { key: k, src: i, val: v });
            }
        }
        Ok(m)
    }

    fn advance(&mut self, src: usize) -> Result<()> {
        if let Some((k, v)) = self.sources[src].next()? {
            self.heap.push(Head { key: k, src, val: v });
        }
        Ok(())
    }
}

impl SortedRows for Merge {
    fn next_row(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let Some(top) = self.heap.pop() else {
            for f in &self.files {
                let _ = std::fs::remove_file(f);
            }
            return Ok(None);
        };
        self.advance(top.src)?;
        // older writes of the same key (lower sources) are dropped
        while self.heap.peek().map_or(false, |h| h.key == top.key) {
            let h = self.heap.pop().unwrap();
            ensure!(!self.strict, "shared key {} written by two slices", String::from_utf8_lossy(&top.key));
            self.dups += 1;
            self.advance(h.src)?;
        }
        if self.wrap {
            Ok(Some((top.key, tenant_bench::runfmt::live(&top.val))))
        } else {
            Ok(Some((top.key, top.val)))
        }
    }
}

// ----------------------------------------------------------------------------------------------- one database

fn oid(ts_s: i64, tag: u8, counter: u64) -> ObjectId {
    let mut b = [0u8; 12];
    b[..4].copy_from_slice(&(ts_s as u32).to_be_bytes());
    b[4] = tag;
    b[5..].copy_from_slice(&counter.to_be_bytes()[1..]);
    ObjectId::from_bytes(b)
}

async fn create_catalog(server: &Arc<Server>, db: &str) -> Result<()> {
    // the indexes of the script's `carica`, created the way the script creates them (createIndexes on an empty collection)
    let mut cmds = Vec::new();
    for c in COLLS {
        cmds.push(doc! { "createIndexes": c, "indexes": [ { "key": { "cliente": 1, "ts": -1 }, "name": "cliente_ts" } ] });
    }
    cmds.push(doc! { "createIndexes": "ordini", "indexes": [ { "key": { "stato": 1, "ts": -1 }, "name": "stato_ts" } ] });
    for cmd in cmds {
        let r = server.handle(db, cmd.clone(), 0).await;
        let ok = matches!(r.get("ok"), Some(Bson::Double(x)) if *x == 1.0) || matches!(r.get("ok"), Some(Bson::Int32(1)));
        ensure!(ok, "{db}: {cmd} failed: {r}");
    }
    Ok(())
}

/// Every row of a database that is not generated data (the catalog), without the envelope marker: what the insert
/// code reads. The writers' scratch catalogs must equal the real one.
async fn catalog_rows(g: &mut Entry) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let rows = g.scan_range(b"", None, false, None, 4 << 20).await?;
    Ok(rows.into_iter().filter(|(k, _)| &k[..] != tenant_bench::envelope::MARKER).map(|(k, v)| (k.to_vec(), v.to_vec())).collect())
}

fn same_catalog(db: &str, what: &str, real: &[(Vec<u8>, Vec<u8>)], other: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
    if real == other {
        return Ok(());
    }
    let a: BTreeMap<_, _> = real.iter().cloned().collect();
    let b: BTreeMap<_, _> = other.iter().cloned().collect();
    let mut diff = Vec::new();
    for k in a.keys().chain(b.keys()).collect::<std::collections::BTreeSet<_>>() {
        if a.get(k) != b.get(k) {
            diff.push(String::from_utf8_lossy(k).into_owned());
        }
    }
    bail!("{db}: the catalog of {what} differs from the real database's at {} keys: {:?}", diff.len(), &diff[..diff.len().min(10)])
}

fn fnv(h: &mut u64, bytes: &[u8]) {
    for b in bytes {
        *h ^= *b as u64;
        *h = h.wrapping_mul(0x100_0000_01b3);
    }
}

/// What a database's generated bytes depend on: a resumed run must be given the same.
fn plan_digest(plan: &Plan, p: &DbPlan, group_bytes: u64, slice_bytes: f64, parts: usize) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let v = serde_json::json!({
        "db": p.db, "seed": p.seed, "gb": p.gb, "tag": p.tag, "taglie": p.taglie, "riservati": p.riservati,
        "vocab": plan.vocab, "cum_vocab": plan.cum_vocab, "t_inizio": plan.t_inizio, "t_span_s": plan.t_span_s,
        "fine_dormienti": plan.fine_dormienti, "sample_every": plan.sample_every, "sample_max": plan.sample_max,
        "group_bytes": group_bytes, "slice_bytes": slice_bytes, "parts": parts, "batch": BATCH, "slice_bits": SLICE_BITS,
    });
    fnv(&mut h, v.to_string().as_bytes());
    format!("{h:016x}")
}

/// Each customer's BSON bytes in the two phases of the script's load (what `one_db` of the first generator computed).
fn targets(p: &DbPlan, fd: f64) -> Vec<(f64, f64)> {
    let n = p.taglie.len();
    let reserved: HashSet<usize> = p.riservati.iter().copied().collect();
    let total = p.gb * GB;
    let s_a: f64 = (0..n).filter(|i| !reserved.contains(i)).map(|i| p.taglie[i]).sum();
    let w1: f64 = (0..n).map(|i| if reserved.contains(&i) { p.taglie[i] / fd } else { p.taglie[i] }).sum();
    (0..n)
        .map(|i| if reserved.contains(&i) { (fd * total * (p.taglie[i] / fd) / w1, 0.0) } else { (fd * total * p.taglie[i] / w1, (1.0 - fd) * total * p.taglie[i] / s_a) })
        .collect()
}

/// Group cuts at customer boundaries (about `group_bytes` of raw rows each) and slices of whole groups (at least
/// `slice_bytes` of BSON each, the last one may be smaller). Both depend only on the plan.
fn layout(t: &[(f64, f64)], group_bytes: u64, slice_bytes: f64) -> (Vec<usize>, Vec<(usize, usize)>) {
    let mut cuts = vec![0usize];
    let mut acc = 0f64;
    for (i, (b1, b2)) in t.iter().enumerate() {
        if acc >= group_bytes as f64 && i > 0 {
            cuts.push(i);
            acc = 0.0;
        }
        acc += RAW_PER_BSON * (b1 + b2);
    }
    let mut slices = Vec::new();
    let (mut lo, mut acc) = (0usize, 0f64);
    for (gi, &c) in cuts.iter().enumerate() {
        let end = cuts.get(gi + 1).copied().unwrap_or(t.len());
        acc += t[c..end].iter().map(|(a, b)| a + b).sum::<f64>();
        if acc >= slice_bytes || end == t.len() {
            slices.push((lo, end));
            lo = end;
            acc = 0.0;
        }
    }
    (cuts, slices)
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct GenState {
    db: String,
    digest: String,
    x0: u64,
    cuts: Vec<usize>,
    slices: Vec<(usize, usize)>,
    /// Lower bounds of the key partitions (hex), the first one empty.
    bounds: Vec<String>,
    created_ms: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
struct DeltaRec {
    docs: i64,
    bytes: i64,
    id_bytes: i64,
    idx: BTreeMap<String, i64>,
}

/// A committed slice (`generator/<db>/slices/s<n>.json`): its runs are in the pantry, its partition files and sample
/// too. Written last: it is the commit point.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
struct SliceRec {
    slice: usize,
    lo: usize,
    hi: usize,
    groups: Vec<GroupLine>,
    direct_last: Option<String>,
    byte: Vec<u64>,
    q2_id: Vec<Option<String>>,
    per_collezione: BTreeMap<String, u64>,
    documenti: u64,
    byte_bson: u64,
    customer_rows: u64,
    customer_run_bytes: u64,
    shared_rows: u64,
    shared_dups: u64,
    stats: BTreeMap<String, DeltaRec>,
    /// The last insertion-counter row of each collection (`meta/nat/...`, hex key -> hex value).
    nat_last: BTreeMap<String, String>,
    /// (partition, bytes, rows) of each non-empty partition file.
    parts: Vec<(usize, u64, u64)>,
    sample_customers: Vec<usize>,
    sample_bytes: u64,
    secs: f64,
    /// Seconds by phase: documents, insert code, key mapping, sort, sealed runs, partition files and uploads.
    times: [f64; 6],
    writer: usize,
    pid: u32,
}

fn gen_prefix(db: &str) -> String {
    format!("generator/{db}")
}

fn slice_rec_path(db: &str, n: usize) -> object_store::path::Path {
    object_store::path::Path::from(format!("{}/slices/s{n:05}.json", gen_prefix(db)))
}

fn part_path(db: &str, n: usize, p: usize) -> object_store::path::Path {
    object_store::path::Path::from(format!("{}/spill/s{n:05}/p{p:03}.zst", gen_prefix(db)))
}

fn sample_path(db: &str, n: usize) -> object_store::path::Path {
    object_store::path::Path::from(format!("{}/sample/s{n:05}.bson", gen_prefix(db)))
}

async fn get_opt(store: &Arc<dyn ObjectStore>, p: &object_store::path::Path) -> Result<Option<bytes::Bytes>> {
    match store.get(p).await {
        Ok(r) => Ok(Some(r.bytes().await?)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

async fn put_file(store: &Arc<dyn ObjectStore>, p: &object_store::path::Path, f: &Path) -> Result<()> {
    let b = std::fs::read(f).with_context(|| format!("read {}", f.display()))?;
    store.put(p, PutPayload::from(b)).await.with_context(|| format!("PUT {p}"))?;
    Ok(())
}

/// What one customer gave.
#[derive(Default)]
struct Cust {
    docs: u64,
    bytes: u64,
    per_coll: [u64; 3],
    q2: Option<String>,
    /// The customer-space rows (sorted, one per key) and the shared rows, as the commit would store them.
    mine: Vec<(Vec<u8>, Vec<u8>)>,
    shared: Vec<(Vec<u8>, Vec<u8>)>,
}

struct Ctx {
    db: String,
    plan: Arc<Plan>,
    words: Arc<Words>,
    p: DbPlan,
    t: Vec<(f64, f64)>,
}

/// One customer through the insert code, in write sets of [`BATCH`] documents, as the script inserts them. `counter`
/// is the next ObjectId counter (slice base plus the documents already made in the slice).
#[allow(clippy::too_many_arguments)]
async fn customer(g: &mut Entry, indexes: &mut HashMap<&'static str, Vec<crate::index::IndexDef>>, cx: &Ctx, i: usize, counter: &mut u64, stats: &mut HashMap<String, crate::collstat::Delta>, mut sample: Option<&mut Vec<u8>>, times: &mut [f64; 6]) -> Result<Cust> {
    let t_ = std::time::Instant::now();
    let (db, plan, p) = (&cx.db, &cx.plan, &cx.p);
    let cli = format!("cli-{i:06}");
    let mut rng = SmallRng::seed_from_u64(p.seed.wrapping_mul(1_000_003).wrapping_add(i as u64));
    let (b1, b2) = cx.t[i];
    let fd = plan.fine_dormienti;
    // the documents of the customer, in time order (the script writes them as its load moves through the year)
    let mut docs: Vec<(i64, &'static str, Document, usize)> = Vec::new();
    let mut bytes = 0f64;
    for (lo, hi, target) in [(0.0, fd, b1), (fd, 1.0, b1 + b2)] {
        while bytes < target {
            let x: f64 = rng.f();
            let coll = if x < 0.5 { "messaggi" } else if x < 0.85 { "eventi" } else { "ordini" };
            let pp: f64 = rng.real(lo, hi);
            let ts_ms = (plan.t_inizio as f64 * 1000.0 + pp * plan.t_span_s as f64 * 1000.0 + rng.real(0.0, 600_000.0)) as i64;
            let d = gen_doc(&cx.words, &mut rng, coll, &cli, bson::DateTime::from_millis(ts_ms));
            let mut full = doc! { "_id": Bson::Null };
            full.extend(d);
            let size = full.to_vec()?.len() + 7; // with the ObjectId instead of the null
            bytes += size as f64;
            docs.push((ts_ms, coll, full, size));
        }
    }
    docs.sort_by_key(|d| d.0);
    times[0] += t_.elapsed().as_secs_f64();
    let mut out = Cust::default();
    let mut orders = Vec::new();
    let mut it = docs.into_iter().peekable();
    while it.peek().is_some() {
        let t_ = std::time::Instant::now();
        let mut w = Writes::new();
        for (ts_ms, coll, mut d, size) in it.by_ref().take(BATCH) {
            *counter += 1;
            ensure!(*counter < 1u64 << 56, "{db}: ObjectId counter overflow");
            let id = oid(ts_ms / 1000, p.tag, *counter);
            d.insert("_id", id);
            let ns = format!("{db}.{coll}");
            w.put(g, coll, &ns, indexes.get_mut(coll).unwrap(), &d, None).await.map_err(|e| anyhow!("{ns}: insert: {e:?}"))?;
            out.bytes += size as u64;
            out.docs += 1;
            out.per_coll[COLLS.iter().position(|c| *c == coll).unwrap()] += 1;
            if coll == "ordini" {
                orders.push(id);
            }
            if let Some(f) = sample.as_deref_mut() {
                f.extend(doc! { "c": coll, "d": d }.to_vec()?);
            }
        }
        // the write set's batch: what `commit` would apply, as `Entry::apply` stores it
        for (c, d) in std::mem::take(&mut w.stat) {
            let s = stats.entry(c).or_default();
            s.docs += d.docs;
            s.bytes += d.bytes;
            s.id_bytes += d.id_bytes;
            for (k, v) in d.idx {
                *s.idx.entry(k).or_insert(0) += v;
            }
        }
        ensure!(!w.indexes_dirty, "{db}: an index became multikey (not in the script's shapes)");
        times[1] += t_.elapsed().as_secs_f64();
        let t_ = std::time::Instant::now();
        for op in crate::layout2::phys_ops(w.final_ops().map_err(|e| anyhow!("{e:?}"))?) {
            match op {
                Op::Put(k, v) if k.starts_with(b"u/") => out.mine.push((k, v)),
                Op::Put(k, v) => out.shared.push((k, v)),
                Op::Delete(k) => bail!("{db}: the insert path deleted a key ({})", String::from_utf8_lossy(&k)),
            }
        }
        times[2] += t_.elapsed().as_secs_f64();
    }
    if !orders.is_empty() {
        out.q2 = Some(orders[rng.int(0, orders.len() as i64) as usize].to_hex());
    }
    let t_ = std::time::Instant::now();
    out.mine = Spill::sorted_dedup(&mut out.mine);
    times[3] += t_.elapsed().as_secs_f64();
    Ok(out)
}

fn is_nat_counter(k: &[u8]) -> bool {
    k.starts_with(b"meta/nat/")
}

/// The insert code numbers a slice's documents from the slice's base.
fn start_slice_numbers(g: &mut Entry, s: usize) {
    for c in COLLS {
        g.nat.insert(c.to_string(), (((s as u64) << SLICE_BITS) + 1, u64::MAX));
    }
}

struct Shared {
    cx: Ctx,
    store: Arc<dyn ObjectStore>,
    id: String,
    epoch: Option<u64>,
    st: GenState,
    bounds: Vec<Vec<u8>>,
    cut_set: HashSet<usize>,
    queue: tokio::sync::Mutex<VecDeque<usize>>,
    /// The committed slices, and the run paths the reference object lists.
    done: tokio::sync::Mutex<(BTreeMap<usize, SliceRec>, Vec<String>)>,
    spill_dir: PathBuf,
    spill_bytes: usize,
    failed: std::sync::atomic::AtomicBool,
    /// Test control: stop the process after this many slices committed here (0: never).
    stop_after: usize,
    committed_here: std::sync::atomic::AtomicUsize,
}

fn refs_path(id: &str) -> object_store::path::Path {
    object_store::path::Path::from(format!("{}/refs/generator", manifest::root(id)))
}

/// The reference object that keeps GC away from the runs of committed slices until the version lists them.
async fn write_refs(store: &Arc<dyn ObjectStore>, id: &str, runs: &[String]) -> Result<()> {
    let body = serde_json::to_vec(&serde_json::json!({ "what": "ballast generator: runs of committed slices, not published yet", "runs": runs }))?;
    store.put(&refs_path(id), PutPayload::from(body)).await?;
    Ok(())
}

/// Write the partition files of a slice from its sorted shared rows; returns (partition, file, bytes, rows).
fn write_parts(mut rows: Merge, bounds: &[Vec<u8>], dir: &Path) -> Result<(Vec<(usize, PathBuf, u64, u64)>, u64)> {
    std::fs::create_dir_all(dir)?;
    let mut out = Vec::new();
    let mut cur: Option<(usize, PathBuf, zstd::stream::write::Encoder<'static, BufWriter<std::fs::File>>, u64)> = None;
    let finish = |c: (usize, PathBuf, zstd::stream::write::Encoder<'static, BufWriter<std::fs::File>>, u64), out: &mut Vec<(usize, PathBuf, u64, u64)>| -> Result<()> {
        let (p, path, z, n) = c;
        let mut w = z.finish()?;
        w.flush()?;
        w.into_inner().map_err(|e| anyhow!("{e}"))?.sync_all()?;
        out.push((p, path.clone(), std::fs::metadata(&path)?.len(), n));
        Ok(())
    };
    while let Some((k, v)) = rows.next_row()? {
        let p = bounds.partition_point(|lo| &lo[..] <= &k[..]) - 1;
        if cur.as_ref().map_or(true, |c| c.0 != p) {
            if let Some(c) = cur.take() {
                ensure!(c.0 < p, "shared rows out of order");
                finish(c, &mut out)?;
            }
            let path = dir.join(format!("p{p:03}.zst"));
            let f = std::fs::File::create(&path).with_context(|| format!("partition file {}", path.display()))?;
            cur = Some((p, path, zstd::stream::write::Encoder::new(BufWriter::with_capacity(1 << 20, f), 1)?, 0));
        }
        let c = cur.as_mut().unwrap();
        c.2.write_all(&(k.len() as u32).to_le_bytes())?;
        c.2.write_all(&k)?;
        c.2.write_all(&(v.len() as u32).to_le_bytes())?;
        c.2.write_all(&v)?;
        c.3 += 1;
    }
    if let Some(c) = cur.take() {
        finish(c, &mut out)?;
    }
    Ok((out, rows.dups))
}

async fn run_slice(sh: &Shared, wi: usize, g: &mut Entry, indexes: &mut HashMap<&'static str, Vec<crate::index::IndexDef>>, s: usize) -> Result<SliceRec> {
    let t0 = std::time::Instant::now();
    let (lo, hi) = sh.st.slices[s];
    let db = &sh.cx.db;
    let plan = &sh.cx.plan;
    let mut times = [0f64; 6];
    start_slice_numbers(g, s);
    let mut counter: u64 = (s as u64) << SLICE_BITS;
    let opts = BuildOpts { group_bytes: u64::MAX, ..Default::default() };
    // group ids are renumbered at publish (run paths are unique by themselves)
    let mut direct = GroupWriter::new(&sh.store, &sh.id, sh.st.x0, sh.epoch, &opts, ((s as u32) % 4000 + 1) * 1_000_000, false);
    let dir = sh.spill_dir.join(format!("s{s:05}"));
    let _ = std::fs::remove_dir_all(&dir);
    let mut spill = Spill::new(dir.join("pieces"), sh.spill_bytes)?;
    let mut stats: HashMap<String, crate::collstat::Delta> = HashMap::new();
    let mut rec = SliceRec { slice: s, lo, hi, writer: wi, pid: std::process::id(), ..Default::default() };
    let mut nat_last: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut sample_buf: Vec<u8> = Vec::new();
    for i in lo..hi {
        if sh.failed.load(std::sync::atomic::Ordering::Relaxed) {
            bail!("another writer failed");
        }
        if sh.cut_set.contains(&i) {
            direct.cut();
        }
        let sampled = plan.sample_every > 0 && i % plan.sample_every == 0 && i / plan.sample_every < plan.sample_max;
        if sampled {
            rec.sample_customers.push(i);
        }
        let c = customer(g, indexes, &sh.cx, i, &mut counter, &mut stats, if sampled { Some(&mut sample_buf) } else { None }, &mut times).await?;
        rec.byte.push(c.bytes);
        rec.q2_id.push(c.q2);
        rec.byte_bson += c.bytes;
        rec.documenti += c.docs;
        for (j, n) in c.per_coll.iter().enumerate() {
            *rec.per_collezione.entry(COLLS[j].to_string()).or_insert(0) += n;
        }
        let t_ = std::time::Instant::now();
        for (k, v) in c.shared {
            if is_nat_counter(&k) {
                nat_last.insert(k, v);
            } else {
                spill.push(k, v)?;
            }
        }
        times[5] += t_.elapsed().as_secs_f64();
        let t_ = std::time::Instant::now();
        for (k, v) in c.mine {
            direct.add(&k, sh.st.x0, &tenant_bench::runfmt::live(&v)).await.with_context(|| format!("{db}: customer {i}"))?;
        }
        times[4] += t_.elapsed().as_secs_f64();
    }
    let t_ = std::time::Instant::now();
    rec.customer_rows = direct.rows;
    rec.customer_run_bytes = direct.bytes;
    rec.direct_last = direct.last_key().map(manifest::hex);
    rec.groups = direct.finish().await?;
    times[4] += t_.elapsed().as_secs_f64();
    let t_ = std::time::Instant::now();
    rec.shared_rows = spill.rows_in;
    let merged = spill.into_rows()?;
    let bounds = sh.bounds.clone();
    let pdir = dir.join("parts");
    let (files, dups) = tokio::task::block_in_place(|| write_parts(merged, &bounds, &pdir))?;
    rec.shared_dups = dups;
    for (p, f, b, n) in &files {
        put_file(&sh.store, &part_path(db, s, *p), f).await?;
        rec.parts.push((*p, *b, *n));
    }
    if !sample_buf.is_empty() {
        rec.sample_bytes = sample_buf.len() as u64;
        sh.store.put(&sample_path(db, s), PutPayload::from(std::mem::take(&mut sample_buf))).await?;
    }
    let _ = std::fs::remove_dir_all(&dir);
    for (k, v) in stats {
        rec.stats.insert(k, DeltaRec { docs: v.docs, bytes: v.bytes, id_bytes: v.id_bytes, idx: v.idx.into_iter().collect() });
    }
    rec.nat_last = nat_last.iter().map(|(k, v)| (manifest::hex(k), manifest::hex(v))).collect();
    times[5] += t_.elapsed().as_secs_f64();
    rec.times = times;
    rec.secs = t0.elapsed().as_secs_f64();
    Ok(rec)
}

/// Commit a slice: the runs into the reference object, then the record (the commit point).
async fn commit_slice(sh: &Shared, rec: SliceRec) -> Result<()> {
    let mut d = sh.done.lock().await;
    let mut runs = d.1.clone();
    runs.extend(rec.groups.iter().filter_map(|g| g.sealed.as_ref().map(|r| r.path.clone())));
    write_refs(&sh.store, &sh.id, &runs).await?;
    d.1 = runs;
    sh.store.put(&slice_rec_path(&sh.cx.db, rec.slice), PutPayload::from(serde_json::to_vec(&rec)?)).await?;
    d.0.insert(rec.slice, rec);
    Ok(())
}

async fn writer(sh: Arc<Shared>, wi: usize, scratch: Arc<Server>) -> Result<()> {
    let db = sh.cx.db.clone();
    let mut g = scratch.dbs.lock(&db).await;
    let mut indexes = HashMap::new();
    for c in COLLS {
        indexes.insert(c, scratch.load_indexes(&mut *g, &db, c).await.map_err(|e| anyhow!("{db}.{c}: {e:?}"))?);
    }
    loop {
        let Some(s) = sh.queue.lock().await.pop_front() else { break };
        let r = run_slice(&sh, wi, &mut g, &mut indexes, s).await;
        let rec = match r {
            Ok(r) => r,
            Err(e) => {
                sh.failed.store(true, std::sync::atomic::Ordering::Relaxed);
                return Err(e.context(format!("{db}: slice {s}")));
            }
        };
        let (secs, mb) = (rec.secs, rec.byte_bson as f64 / 1e6);
        commit_slice(&sh, rec).await?;
        let n = sh.committed_here.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        let total = sh.done.lock().await.0.len();
        eprintln!("ballast {db}: slice {s} done by writer {wi}: {mb:.0} MB of BSON in {secs:.1} s ({:.1} MB/s); {total} of {} slices", mb / secs.max(1e-9), sh.st.slices.len());
        if sh.stop_after > 0 && n >= sh.stop_after {
            eprintln!("ballast {db}: FENVIX_GEN_STOP_AFTER={} reached: stopping the process (test control)", sh.stop_after);
            std::process::exit(3);
        }
    }
    Ok(())
}

/// A partition's rows for the publish: downloads its files from the pantry when the publish first reads it.
struct LazyPart {
    store: Arc<dyn ObjectStore>,
    db: String,
    p: usize,
    slices: Vec<usize>,
    extra: Vec<(Vec<u8>, Vec<u8>)>,
    dir: PathBuf,
    m: Option<Merge>,
}

impl SortedRows for LazyPart {
    fn next_row(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.m.is_none() {
            std::fs::create_dir_all(&self.dir)?;
            let mut files = Vec::new();
            let h = tokio::runtime::Handle::current();
            tokio::task::block_in_place(|| -> Result<()> {
                for &s in &self.slices {
                    let f = self.dir.join(format!("s{s:05}.zst"));
                    let b = h.block_on(async { self.store.get(&part_path(&self.db, s, self.p)).await?.bytes().await })?;
                    std::fs::write(&f, &b)?;
                    files.push(f);
                }
                Ok(())
            })?;
            self.m = Some(Merge::of_files(files, std::mem::take(&mut self.extra), true)?);
        }
        self.m.as_mut().unwrap().next_row()
    }
}

/// Open a database as its writer, with its envelope marker (E1's wait the first time); returns the cut.
async fn begin_writer(server: Arc<Server>, db: String, say: bool) -> Result<u64> {
    let mut g = server.dbs.lock(&db).await;
    loop {
        match g.ballast_begin().await? {
            Some(x) => return Ok(x),
            None => {
                if say {
                    eprintln!("ballast {db}: waiting for the envelope marker (E1's wait)");
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    }
}

#[derive(Default)]
struct DbReport {
    json: serde_json::Value,
}

async fn one_db(server: Arc<Server>, plan: Arc<Plan>, words: Arc<Words>, p: DbPlan, make_scratch: fn() -> Arc<Server>) -> Result<DbReport> {
    use std::sync::atomic::Ordering::Relaxed;
    let t0 = std::time::Instant::now();
    let db = p.db.clone();
    let workers = if plan.workers > 0 { plan.workers } else if plan.parallel > 1 { plan.parallel } else { std::thread::available_parallelism().map_or(1, |n| n.get()) };
    let group_bytes = tenant_bench::fx::GROUP_BYTES.load(Relaxed);
    // key partitions of the shared rows: about one group each (the shared keyspace holds about 0.445 raw bytes per BSON
    // byte, MEASURED on 1 GiB), independent of the writers, so the groups do not depend on how many writers ran
    let parts_n = if plan.parts > 0 { plan.parts } else { ((p.gb * GB * SHARED_RAW_PER_BSON / group_bytes as f64).round() as usize).clamp(1, 256) };
    let slice_bytes = if plan.slice_gb > 0.0 { plan.slice_gb * GB } else { (p.gb / 256.0).max(1.0) * GB };
    let digest = plan_digest(&plan, &p, group_bytes, slice_bytes, parts_n);
    let t = targets(&p, plan.fine_dormienti);
    let cx = Ctx { db: db.clone(), plan: Arc::clone(&plan), words: Arc::clone(&words), p: p.clone(), t: t.clone() };

    // the real database, and the state of an earlier run if any (generator/<db>/ in the pantry, outside tenants/)
    // a run killed a moment ago still holds the database's lease until it expires: wait for it (bounded)
    let t_lease = std::time::Instant::now();
    let exists = loop {
        let mut g = server.dbs.lock(&db).await;
        match g.collections().await {
            Ok(c) => break !c.is_empty(),
            Err(e) if e.downcast_ref::<crate::lease::LeaseHeld>().is_some() && t_lease.elapsed().as_secs() < 300 => {
                drop(g);
                eprintln!("ballast {db}: {e}: waiting for that lease to expire");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
            Err(e) => return Err(e),
        }
    };
    let raw_store = server.dbs.raw_store();
    if let Some(done) = get_opt(&raw_store, &object_store::path::Path::from(format!("{}/done.json", gen_prefix(&db)))).await? {
        eprintln!("ballast {db}: already generated and published (generator/{db}/done.json): nothing to do");
        let v: serde_json::Value = serde_json::from_slice(&done)?;
        ensure!(v.get("digest").and_then(|d| d.as_str()) == Some(digest.as_str()), "{db}: published by a generator run with another plan");
        let mut json = v.get("report").cloned().unwrap_or_default();
        if let Some(n) = v.get("sample_file_name").and_then(|x| x.as_str()) {
            json["sample_file"] = serde_json::json!(Path::new(&plan.out).join(n).display().to_string());
        }
        return Ok(DbReport { json });
    }
    let old: Option<GenState> = match get_opt(&raw_store, &object_store::path::Path::from(format!("{}/state.json", gen_prefix(&db)))).await? {
        Some(b) => Some(serde_json::from_slice(&b)?),
        None => None,
    };
    match &old {
        None => {
            ensure!(!exists, "database {db} exists already and no generator state is in the pantry: the generator writes new databases only");
            create_catalog(&server, &db).await?;
        }
        Some(st) => {
            ensure!(st.digest == digest, "{db}: the pantry holds a generator state for another plan (digest {} here {digest}): refusing to mix them", st.digest);
            ensure!(exists, "{db}: a generator state exists but the database has no catalog");
            eprintln!("ballast {db}: RESUMING the generator state of the pantry");
        }
    }
    // the writers' private catalogs: a copy of the real catalog's rows (collection UUIDs included), one batch each
    let real_catalog0 = {
        let mut g = server.dbs.lock(&db).await;
        catalog_rows(&mut g).await?
    };
    let mut scratch = Vec::new();
    for _ in 0..workers {
        let s = make_scratch();
        s.dbs.lock(&db).await.apply(real_catalog0.iter().map(|(k, v)| Op::Put(k.clone(), v.clone())).collect()).await?;
        scratch.push(s);
    }
    // E1's wait, on the real database and on every private copy at once: each gets its envelope marker the normal way, so
    // the insert code's reads take the same path everywhere (MEASURED: on a copy without the marker a point read cost
    // 7.2 us instead of 3.5, because every read looked for the marker again, and the insert code ran 23% slower)
    let mut begins = vec![begin_writer(Arc::clone(&server), db.clone(), true)];
    for s in &scratch {
        begins.push(begin_writer(Arc::clone(s), db.clone(), false));
    }
    let x_now = futures::future::try_join_all(begins).await?[0];
    let mut g = server.dbs.lock(&db).await;
    let (store, id, epoch) = g.ballast_target()?;
    let real_catalog = catalog_rows(&mut g).await?;
    drop(g);
    same_catalog(&db, "the real database after E1's wait", &real_catalog0, &real_catalog)?;
    for (w, s) in scratch.iter().enumerate() {
        let mut sg = s.dbs.lock(&db).await;
        same_catalog(&db, &format!("writer {w}'s scratch database"), &real_catalog, &catalog_rows(&mut sg).await?)?;
    }

    let st = match old {
        Some(st) => st,
        None => {
            let (cuts, slices) = layout(&t, group_bytes, slice_bytes);
            ensure!(slices.len() < (1 << 20), "{db}: too many slices");
            // partition bounds: the shared keys of a few customers spread over the database, through the same code
            let mut keys: Vec<Vec<u8>> = Vec::new();
            {
                let mut sg = scratch[0].dbs.lock(&db).await;
                let mut idx = HashMap::new();
                for c in COLLS {
                    idx.insert(c, scratch[0].load_indexes(&mut *sg, &db, c).await.map_err(|e| anyhow!("{db}.{c}: {e:?}"))?);
                }
                let n = p.taglie.len();
                let k = plan.bound_sample.clamp(1, n);
                let mut times = [0f64; 6];
                for j in 0..k {
                    let i = j * n / k;
                    let s = slices.partition_point(|sl| sl.1 <= i);
                    start_slice_numbers(&mut sg, s);
                    let mut counter = (s as u64) << SLICE_BITS;
                    let mut stats = HashMap::new();
                    let c = customer(&mut sg, &mut idx, &cx, i, &mut counter, &mut stats, None, &mut times).await?;
                    keys.extend(c.shared.into_iter().map(|(k, _)| k).filter(|k| !is_nat_counter(k)));
                }
                sg.nat.clear();
            }
            keys.sort();
            let mut bounds: Vec<Vec<u8>> = vec![Vec::new(), b"u/".to_vec()];
            for q in 1..parts_n {
                if let Some(k) = keys.get(q * keys.len() / parts_n) {
                    bounds.push(k.clone());
                }
            }
            bounds.sort();
            bounds.dedup();
            let st = GenState { db: db.clone(), digest: digest.clone(), x0: x_now, cuts, slices, bounds: bounds.iter().map(|b| manifest::hex(b)).collect(), created_ms: manifest::now_ms() };
            raw_store.put(&object_store::path::Path::from(format!("{}/state.json", gen_prefix(&db))), PutPayload::from(serde_json::to_vec(&st)?)).await?;
            st
        }
    };
    ensure!(st.x0 <= x_now, "{db}: the stored cut {} is above the database's cut {x_now}", st.x0);
    let bounds: Vec<Vec<u8>> = st.bounds.iter().map(|b| manifest::unhex(b)).collect::<Result<_>>()?;

    // the committed slices
    let mut done: BTreeMap<usize, SliceRec> = BTreeMap::new();
    for s in 0..st.slices.len() {
        if let Some(b) = get_opt(&raw_store, &slice_rec_path(&db, s)).await? {
            let r: SliceRec = serde_json::from_slice(&b)?;
            ensure!(r.slice == s && (r.lo, r.hi) == st.slices[s], "{db}: slice record {s} does not match the state");
            done.insert(s, r);
        }
    }
    let runs: Vec<String> = done.values().flat_map(|r| r.groups.iter().filter_map(|g| g.sealed.as_ref().map(|x| x.path.clone()))).collect();
    write_refs(&store, &id, &runs).await?;
    let todo: VecDeque<usize> = (0..st.slices.len()).filter(|s| !done.contains_key(s)).collect();
    let bson_planned: f64 = t.iter().map(|(a, b)| a + b).sum();
    eprintln!("ballast {db}: {} customers, {:.2} GiB planned, {} groups, {} slices ({} committed before, {} to do), {} writers, {} key partitions, cut {}",
        t.len(), bson_planned / GB, st.cuts.len(), st.slices.len(), done.len(), todo.len(), workers, bounds.len(), st.x0);
    let stop_after = std::env::var("FENVIX_GEN_STOP_AFTER").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    let spill_dir = Path::new(&plan.spill_dir).join(&db);
    let sh = Arc::new(Shared {
        cx,
        store: Arc::clone(&store),
        id: id.clone(),
        epoch,
        cut_set: st.cuts.iter().copied().collect(),
        st: st.clone(),
        bounds: bounds.clone(),
        queue: tokio::sync::Mutex::new(todo),
        done: tokio::sync::Mutex::new((done, runs)),
        spill_dir: spill_dir.clone(),
        spill_bytes: plan.spill_mb << 20,
        failed: std::sync::atomic::AtomicBool::new(false),
        stop_after,
        committed_here: std::sync::atomic::AtomicUsize::new(0),
    });
    let tgen = std::time::Instant::now();
    let mut tasks = Vec::new();
    for (wi, s) in scratch.iter().enumerate() {
        let (sh, s) = (Arc::clone(&sh), Arc::clone(s));
        tasks.push(tokio::spawn(async move { writer(sh, wi, s).await }));
    }
    let mut first_err = None;
    for t in tasks {
        match t.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => first_err = first_err.or(Some(e)),
            Err(e) => first_err = first_err.or(Some(anyhow!("writer task: {e}"))),
        }
    }
    if let Some(e) = first_err {
        return Err(e);
    }
    let generate_secondi = tgen.elapsed().as_secs_f64();
    // the private catalogs are still the real one (nothing the insert code did changed a catalog)
    {
        let mut g = server.dbs.lock(&db).await;
        let real_now = catalog_rows(&mut g).await?;
        same_catalog(&db, "the real database after generating", &real_catalog, &real_now)?;
        for (w, s) in scratch.iter().enumerate() {
            let mut sg = s.dbs.lock(&db).await;
            same_catalog(&db, &format!("writer {w}'s scratch database after generating"), &real_now, &catalog_rows(&mut sg).await?)?;
        }
    }
    drop(scratch);

    // publish: the customer-space groups in slice order, the key partitions merged in parallel
    let (done, _) = std::mem::take(&mut *sh.done.lock().await);
    ensure!(done.len() == st.slices.len(), "{db}: {} of {} slices committed", done.len(), st.slices.len());
    let tp = std::time::Instant::now();
    let n = p.taglie.len();
    let mut byte: Vec<u64> = Vec::with_capacity(n);
    let mut q2_id = Vec::with_capacity(n);
    let mut per_collezione: BTreeMap<String, u64> = BTreeMap::new();
    let (mut documenti, mut byte_bson, mut customer_rows, mut customer_run_bytes, mut shared_rows, mut shared_dups, mut spill_disk_bytes) = (0u64, 0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
    let mut stats: HashMap<String, crate::collstat::Delta> = HashMap::new();
    let mut nat_final: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut direct: Vec<GroupLine> = Vec::new();
    let mut direct_last: Option<Vec<u8>> = None;
    let mut sample_customers: Vec<usize> = Vec::new();
    let (mut slice_secs, mut times) = (0f64, [0f64; 6]);
    let (mut bson_here, mut slices_here) = (0u64, 0usize);
    let mut part_slices: Vec<Vec<usize>> = vec![Vec::new(); bounds.len()];
    for (s, r) in &done {
        ensure!(r.byte.len() == r.hi - r.lo && r.q2_id.len() == r.hi - r.lo, "{db}: slice {s}: customer lists");
        byte.extend(&r.byte);
        q2_id.extend(r.q2_id.iter().cloned());
        for (c, k) in &r.per_collezione {
            *per_collezione.entry(c.clone()).or_insert(0) += k;
        }
        documenti += r.documenti;
        byte_bson += r.byte_bson;
        customer_rows += r.customer_rows;
        customer_run_bytes += r.customer_run_bytes;
        shared_rows += r.shared_rows;
        shared_dups += r.shared_dups;
        for (c, d) in &r.stats {
            let e = stats.entry(c.clone()).or_default();
            e.docs += d.docs;
            e.bytes += d.bytes;
            e.id_bytes += d.id_bytes;
            for (k, v) in &d.idx {
                *e.idx.entry(k.clone()).or_insert(0) += v;
            }
        }
        for (k, v) in &r.nat_last {
            let (k, v) = (manifest::unhex(k)?, manifest::unhex(v)?);
            ensure!(v.len() == 8, "{db}: insertion counter value");
            // the counter holds the next free number: the highest wins (the newest slice numbers above the others)
            match nat_final.get(&k) {
                Some(o) if o >= &v => {}
                _ => {
                    nat_final.insert(k, v);
                }
            }
        }
        direct.extend(r.groups.iter().cloned());
        if let Some(l) = &r.direct_last {
            direct_last = Some(manifest::unhex(l)?);
        }
        sample_customers.extend(&r.sample_customers);
        for (pi, b, _) in &r.parts {
            part_slices[*pi].push(*s);
            spill_disk_bytes += b;
        }
        slice_secs += r.secs;
        for j in 0..6 {
            times[j] += r.times[j];
        }
        if r.pid == std::process::id() {
            bson_here += r.byte_bson;
            slices_here += 1;
        }
    }
    let mut g = server.dbs.lock(&db).await;
    // the database may have been closed while idle (the generation does not touch it): open it as the writer again
    let x_pub = loop {
        match g.ballast_begin().await? {
            Some(x) => break x,
            None => tokio::time::sleep(std::time::Duration::from_secs(2)).await,
        }
    };
    ensure!(st.x0 <= x_pub, "{db}: the stored cut {} is above the database's cut {x_pub}", st.x0);
    // the storage counters, once, as `commit` writes them (`collstat::ops_for` reads the stored ones and adds the deltas)
    let mut extra: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for op in crate::collstat::ops_for(&mut g, &mut stats).await? {
        match op {
            Op::Put(k, v) => extra.push((crate::layout2::phys(&k).into_owned(), tenant_bench::runfmt::live(&v))),
            Op::Delete(_) => bail!("{db}: the counters deleted a key"),
        }
    }
    for (k, v) in nat_final {
        extra.push((crate::layout2::phys(&k).into_owned(), tenant_bench::runfmt::live(&v)));
    }
    extra.sort();
    let mut extra_by: Vec<Vec<(Vec<u8>, Vec<u8>)>> = vec![Vec::new(); bounds.len()];
    for (k, v) in extra {
        let pi = bounds.partition_point(|lo| &lo[..] <= &k[..]) - 1;
        extra_by[pi].push((k, v));
    }
    let pub_dir = spill_dir.join("publish");
    let parts: Vec<PartRows> = bounds
        .iter()
        .enumerate()
        .zip(extra_by)
        .map(|((pi, lo), extra)| PartRows {
            lo: lo.clone(),
            rows: Box::new(LazyPart { store: Arc::clone(&raw_store), db: db.clone(), p: pi, slices: part_slices[pi].clone(), extra, dir: pub_dir.join(format!("p{pi:03}")), m: None }),
        })
        .collect();
    let opts = BuildOpts { group_bytes, ..Default::default() };
    let b = g.ballast_publish_parts(&opts, st.x0, direct, direct_last, parts, workers).await?;
    drop(g);
    let publish_secondi = tp.elapsed().as_secs_f64();
    let _ = std::fs::remove_dir_all(&pub_dir);
    // the version lists every run now: the reference object goes, and so do the partition files
    store.delete(&refs_path(&id)).await.ok();
    let spill_objs: Vec<_> = {
        use futures::TryStreamExt;
        raw_store.list(Some(&object_store::path::Path::from(format!("{}/spill", gen_prefix(&db))))).try_collect().await?
    };
    for o in spill_objs {
        raw_store.delete(&o.location).await.ok();
    }
    // the reference sample, in customer order
    let mut sample_file = None;
    if !sample_customers.is_empty() {
        let f = Path::new(&plan.out).join(format!("{db}.sample.bson"));
        let mut w = BufWriter::new(std::fs::File::create(&f)?);
        for (s, r) in &done {
            if r.sample_bytes > 0 {
                let b = raw_store.get(&sample_path(&db, *s)).await?.bytes().await?;
                w.write_all(&b)?;
            }
        }
        w.flush()?;
        sample_file = Some(f.display().to_string());
    }
    let secondi = t0.elapsed().as_secs_f64();
    let json = serde_json::json!({
        "db": db, "customers": n, "byte": byte, "q2_id": q2_id, "per_collezione": per_collezione, "documenti": documenti,
        "byte_bson": byte_bson, "secondi": secondi, "mb_s": byte_bson as f64 / 1e6 / secondi,
        "generate_secondi": generate_secondi, "publish_secondi": publish_secondi, "x0": st.x0,
        "customer_rows": customer_rows, "customer_run_bytes": customer_run_bytes, "shared_rows": shared_rows,
        "shared_dups_in_slices": shared_dups, "spill_disk_bytes": spill_disk_bytes,
        "build": serde_json::to_value(&b).unwrap_or_default(),
        "sample_file": sample_file, "sample_customers": sample_customers,
        "generator": {
            "workers": workers, "parts": bounds.len(), "slices": st.slices.len(), "groups_planned": st.cuts.len(),
            "slice_gib": slice_bytes / GB, "digest": digest,
            "slices_generated_here": slices_here, "bson_generated_here": bson_here,
            "mb_s_here": bson_here as f64 / 1e6 / generate_secondi.max(1e-9),
            "writer_seconds": slice_secs, "mb_s_per_writer": byte_bson as f64 / 1e6 / slice_secs.max(1e-9),
            "seconds_by_phase": {"documents": times[0], "insert_code": times[1], "key_mapping": times[2], "sort": times[3],
                                 "sealed_runs": times[4], "partition_files_and_uploads": times[5]},
        },
    });
    raw_store
        .put(
            &object_store::path::Path::from(format!("{}/done.json", gen_prefix(&db))),
            PutPayload::from(serde_json::to_vec(&serde_json::json!({ "digest": digest, "report": json, "sample_file_name": format!("{db}.sample.bson") }))?),
        )
        .await?;
    Ok(DbReport { json })
}

/// The publish merges one file per slice and key partition, several partitions at a time (at 1 TB about 256 slices
/// times the writers): the soft limit on open files goes up to the hard one.
fn raise_open_files() {
    unsafe {
        let mut r = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut r) == 0 && r.rlim_cur < r.rlim_max {
            let want = libc::rlimit { rlim_cur: r.rlim_max, rlim_max: r.rlim_max };
            if libc::setrlimit(libc::RLIMIT_NOFILE, &want) == 0 {
                eprintln!("ballast: open files limit {} -> {}", r.rlim_cur, r.rlim_max);
            }
        }
    }
}

/// `--ballast PLAN.json`: generate every database of the plan, one after another, each with several writers; one report
/// per database and a summary (`<out>/ballast-gen-summary.json`). Run it again with the same plan after an interruption:
/// it resumes from the pantry. Exit status: error if any database failed.
pub async fn run(server: Arc<Server>, plan_path: &str, make_scratch: fn() -> Arc<Server>) -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    ensure!(tenant_bench::fx::ENABLED.load(Relaxed), "--ballast writes sealed runs: start it with --sealed-runs (the round's server options)");
    ensure!(crate::locallog::get().is_none(), "--ballast writes no log: start it without --quaderno");
    ensure!(tenant_bench::envelope::START.load(Relaxed), "--ballast publishes runs, which needs envelopes on: start it with --envelopes write (release E1)");
    // the generator publishes each database once, at the end; no automatic publishes in between
    tenant_bench::fx::PUBLISH_EVERY.store(0, Relaxed);
    raise_open_files();
    let plan: Plan = parse_plan(&serde_json::from_slice(&std::fs::read(plan_path).with_context(|| format!("plan {plan_path}"))?)?)?;
    ensure!(!plan.vocab.is_empty() && plan.vocab.len() == plan.cum_vocab.len(), "plan: vocab and cum_vocab");
    std::fs::create_dir_all(&plan.out)?;
    let words = Arc::new(Words { vocab: plan.vocab.clone(), cum: plan.cum_vocab.clone() });
    let plan = Arc::new(plan);
    let t0 = std::time::Instant::now();
    let mut results = Vec::new();
    for p in plan.databases.clone() {
        let name = p.db.clone();
        let r = one_db(Arc::clone(&server), Arc::clone(&plan), Arc::clone(&words), p, make_scratch).await;
        match &r {
            Ok(rep) => {
                std::fs::write(Path::new(&plan.out).join(format!("{name}.gen.json")), serde_json::to_vec_pretty(&rep.json).unwrap_or_default()).ok();
                let j = &rep.json;
                eprintln!("BALLAST-DB {name}: {:.2} GiB of BSON, {} documents in {:.0} s ({:.1} MB/s); version {} with {} groups",
                    j["byte_bson"].as_f64().unwrap_or(0.0) / GB, j["documenti"], j["secondi"].as_f64().unwrap_or(0.0), j["mb_s"].as_f64().unwrap_or(0.0),
                    j["build"]["version"], j["build"]["groups"]);
            }
            Err(e) => eprintln!("BALLAST-DB {name}: FAILED: {e:#}"),
        }
        results.push((name, r.map(|rep| (rep.json["byte_bson"].as_u64().unwrap_or(0), rep.json["documenti"].as_u64().unwrap_or(0), rep.json["secondi"].as_f64().unwrap_or(0.0))).map_err(|e| format!("{e:#}"))));
    }
    let secs = t0.elapsed().as_secs_f64();
    let ok = results.iter().all(|(_, r)| r.is_ok()) && results.len() == plan.databases.len();
    let bytes: u64 = results.iter().filter_map(|(_, r)| r.as_ref().ok()).map(|r| r.0).sum();
    let summary = serde_json::json!({
        "ok": ok, "seconds": secs, "bson_bytes": bytes, "mb_s_total": bytes as f64 / 1e6 / secs.max(1e-9),
        "databases": results.iter().map(|(n, r)| match r {
            Ok((b, d, s)) => serde_json::json!({"db": n, "ok": true, "bson_bytes": b, "documents": d, "seconds": s}),
            Err(e) => serde_json::json!({"db": n, "ok": false, "error": e}),
        }).collect::<Vec<_>>(),
    });
    std::fs::write(Path::new(&plan.out).join("ballast-gen-summary.json"), serde_json::to_vec_pretty(&summary)?)?;
    eprintln!("BALLAST: {}: {} databases, {:.2} GiB of BSON in {:.0} s ({:.1} MB/s together)", if ok { "DONE" } else { "FAILED" }, results.len(), bytes as f64 / GB, secs, bytes as f64 / 1e6 / secs.max(1e-9));
    ensure!(ok, "at least one ballast database failed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spill_merges_in_order_and_last_write_wins() {
        let dir = std::env::temp_dir().join(format!("fx-ballast-spill-{}", std::process::id()));
        let mut s = Spill::new(dir.clone(), 200).unwrap();
        let mut want = std::collections::BTreeMap::new();
        let mut rng = SmallRng::seed_from_u64(7);
        for i in 0..5000u32 {
            let k = format!("k{:05}", rng.int(0, 1500)).into_bytes();
            let v = i.to_be_bytes().to_vec();
            want.insert(k.clone(), v.clone());
            s.push(k, v).unwrap();
        }
        assert!(s.files.len() > 3, "the test must spill");
        let mut m = s.into_rows().unwrap();
        let mut got = Vec::new();
        while let Some((k, v)) = m.next_row().unwrap() {
            got.push((k, v[1..].to_vec()));
        }
        let want: Vec<_> = want.into_iter().collect();
        assert_eq!(got, want);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn layout_cuts_groups_at_customers_and_slices_at_groups() {
        // 1,000 customers of 1 MB of BSON: groups of 64 MB raw (about 43 customers), slices of at least 100 MB
        let t: Vec<(f64, f64)> = (0..1000).map(|i| (0.6e6 + (i % 7) as f64 * 1e4, 0.4e6)).collect();
        let (cuts, slices) = layout(&t, 64 << 20, 100e6);
        assert_eq!(cuts[0], 0);
        assert!(cuts.windows(2).all(|w| w[0] < w[1]));
        for w in cuts.windows(2) {
            let raw: f64 = t[w[0]..w[1]].iter().map(|(a, b)| RAW_PER_BSON * (a + b)).sum();
            assert!(raw >= (64 << 20) as f64 && raw < (64 << 20) as f64 + 2e6, "a group is cut right after it reaches the size");
        }
        // slices cover every customer once, in order, and start at group cuts
        assert_eq!(slices[0].0, 0);
        assert_eq!(slices.last().unwrap().1, t.len());
        for w in slices.windows(2) {
            assert_eq!(w[0].1, w[1].0);
        }
        assert!(slices.iter().all(|s| cuts.contains(&s.0)));
        for s in &slices[..slices.len() - 1] {
            assert!(t[s.0..s.1].iter().map(|(a, b)| a + b).sum::<f64>() >= 100e6);
        }
        // the plan depends on the data only
        assert_eq!(layout(&t, 64 << 20, 100e6), (cuts, slices));
    }

    #[test]
    fn partition_files_hold_every_row_once_and_a_merge_of_slices_is_strict() {
        let dir = std::env::temp_dir().join(format!("fx-ballast-parts-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let bounds: Vec<Vec<u8>> = vec![Vec::new(), b"k1".to_vec(), b"k3".to_vec(), b"u/".to_vec()];
        // two slices, disjoint keys; each spilled in several pieces
        let mut files_by_part: Vec<Vec<PathBuf>> = vec![Vec::new(); bounds.len()];
        let mut want = std::collections::BTreeMap::new();
        for sl in 0..2u32 {
            let mut sp = Spill::new(dir.join(format!("s{sl}/pieces")), 300).unwrap();
            for i in 0..2000u32 {
                let k = format!("k{}{:05}-{sl}", i % 5, i).into_bytes();
                let v = i.to_be_bytes().to_vec();
                want.insert(k.clone(), tenant_bench::runfmt::live(&v));
                sp.push(k, v).unwrap();
            }
            assert!(sp.files.len() > 3, "the test must spill");
            let (files, dups) = write_parts(sp.into_rows().unwrap(), &bounds, &dir.join(format!("s{sl}/parts"))).unwrap();
            assert_eq!(dups, 0);
            for (p, f, _, n) in files {
                assert!(n > 0);
                files_by_part[p].push(f);
            }
        }
        let mut got = Vec::new();
        for (p, files) in files_by_part.into_iter().enumerate() {
            let mut m = Merge::of_files(files, Vec::new(), true).unwrap();
            while let Some((k, v)) = m.next_row().unwrap() {
                assert!(k >= bounds[p] && bounds.get(p + 1).map_or(true, |h| &k < h), "row in the wrong partition");
                got.push((k, v));
            }
        }
        assert_eq!(got, want.into_iter().collect::<Vec<_>>());
        // the same key in two slices is an error, never a silent choice
        let a = dir.join("dup-a");
        let b = dir.join("dup-b");
        for f in [&a, &b] {
            let mut sp = Spill::new(f.join("pieces"), 1 << 20).unwrap();
            sp.push(b"k1same".to_vec(), b"v".to_vec()).unwrap();
            write_parts(sp.into_rows().unwrap(), &bounds, &f.join("parts")).unwrap();
        }
        let mut m = Merge::of_files(vec![a.join("parts/p001.zst"), b.join("parts/p001.zst")], Vec::new(), true).unwrap();
        assert!(m.next_row().is_err());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn object_ids_are_unique_and_carry_the_time() {
        let a = oid(1_761_955_200, 3, 1);
        let b = oid(1_761_955_200, 3, 2);
        assert_ne!(a, b);
        assert_eq!(a.timestamp().timestamp_millis(), 1_761_955_200_000);
    }
}
