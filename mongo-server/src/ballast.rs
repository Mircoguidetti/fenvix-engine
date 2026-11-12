//! The step 9 ballast generator: `mongo-server <the round's server options> --ballast PLAN.json`.
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
use std::collections::{BinaryHeap, HashMap};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tenant_bench::fx::build::{BuildOpts, Generated, GroupWriter, SortedRows};
use tenant_bench::lifecycle::Op;

const GB: f64 = (1u64 << 30) as f64;
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
        let mut m = Merge { sources, heap: BinaryHeap::new(), files: self.files };
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
}

impl Merge {
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
            self.advance(h.src)?;
        }
        Ok(Some((top.key, tenant_bench::runfmt::live(&top.val))))
    }
}

// ----------------------------------------------------------------------------------------------- one database

#[derive(Default)]
struct DbReport {
    db: String,
    customers: usize,
    byte: Vec<u64>,
    q2_id: Vec<Option<String>>,
    per_collezione: HashMap<String, u64>,
    documenti: u64,
    byte_bson: u64,
    secondi: f64,
    mb_s: f64,
    generate_secondi: f64,
    publish_secondi: f64,
    x0: u64,
    customer_rows: u64,
    customer_run_bytes: u64,
    shared_rows: u64,
    spill_disk_bytes: u64,
    build: Option<tenant_bench::fx::build::BuildReport>,
    sample_file: Option<String>,
    sample_customers: Vec<usize>,
}

impl DbReport {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "db": self.db, "customers": self.customers, "byte": self.byte, "q2_id": self.q2_id,
            "per_collezione": self.per_collezione, "documenti": self.documenti, "byte_bson": self.byte_bson,
            "secondi": self.secondi, "mb_s": self.mb_s, "generate_secondi": self.generate_secondi,
            "publish_secondi": self.publish_secondi, "x0": self.x0, "customer_rows": self.customer_rows,
            "customer_run_bytes": self.customer_run_bytes, "shared_rows": self.shared_rows,
            "spill_disk_bytes": self.spill_disk_bytes,
            "build": self.build.as_ref().map(|b| serde_json::to_value(b).unwrap_or_default()),
            "sample_file": self.sample_file, "sample_customers": self.sample_customers,
        })
    }
}

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

async fn one_db(server: Arc<Server>, plan: Arc<Plan>, words: Arc<Words>, p: DbPlan) -> Result<DbReport> {
    let t0 = std::time::Instant::now();
    let db = p.db.clone();
    ensure!(server.dbs.lock(&db).await.collections().await?.is_empty(), "database {db} exists already: the generator writes new databases only");
    create_catalog(&server, &db).await?;
    let mut g = server.dbs.lock(&db).await;
    let x0 = loop {
        match g.ballast_begin().await? {
            Some(x) => break x,
            None => {
                eprintln!("ballast {db}: waiting for the envelope marker (E1's wait)");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    };
    let (store, id, epoch) = g.ballast_target()?;
    let opts = BuildOpts { group_bytes: tenant_bench::fx::GROUP_BYTES.load(std::sync::atomic::Ordering::Relaxed), ..Default::default() };
    let mut direct = GroupWriter::new(&store, &id, x0, epoch, &opts, 1 << 20, false);
    let mut spill = Spill::new(Path::new(&plan.spill_dir).join(&db), plan.spill_mb << 20)?;
    let mut indexes = HashMap::new();
    for c in COLLS {
        indexes.insert(c, server.load_indexes(&mut *g, &db, c).await.map_err(|e| anyhow!("{db}.{c}: {e:?}"))?);
    }

    // the customers: sizes and dormant ones as the script made them; each customer's bytes split between the two phases
    // of the script's load as its weighted draw splits them on average
    let n = p.taglie.len();
    let reserved: std::collections::HashSet<usize> = p.riservati.iter().copied().collect();
    let total = p.gb * GB;
    let fd = plan.fine_dormienti;
    let s_a: f64 = (0..n).filter(|i| !reserved.contains(i)).map(|i| p.taglie[i]).sum();
    let w1: f64 = (0..n).map(|i| if reserved.contains(&i) { p.taglie[i] / fd } else { p.taglie[i] }).sum();
    let mut rep = DbReport { db: db.clone(), customers: n, byte: vec![0; n], q2_id: vec![None; n], x0, ..Default::default() };
    let mut stats: HashMap<String, crate::collstat::Delta> = HashMap::new();
    let mut counter: u64 = 0;
    let mut sample: Option<BufWriter<std::fs::File>> = None;
    if plan.sample_every > 0 {
        let f = Path::new(&plan.out).join(format!("{db}.sample.bson"));
        sample = Some(BufWriter::new(std::fs::File::create(&f)?));
        rep.sample_file = Some(f.display().to_string());
    }
    let mut last_print = std::time::Instant::now();
    let tgen = std::time::Instant::now();
    for i in 0..n {
        let cli = format!("cli-{i:06}");
        let mut rng = SmallRng::seed_from_u64(p.seed.wrapping_mul(1_000_003).wrapping_add(i as u64));
        let (b1, b2) = if reserved.contains(&i) {
            (fd * total * (p.taglie[i] / fd) / w1, 0.0)
        } else {
            (fd * total * p.taglie[i] / w1, (1.0 - fd) * total * p.taglie[i] / s_a)
        };
        // the documents of the customer, in time order (the script writes them as its load moves through the year)
        let mut docs: Vec<(i64, &'static str, Document, usize)> = Vec::new();
        let mut bytes = 0f64;
        for (lo, hi, target) in [(0.0, fd, b1), (fd, 1.0, b1 + b2)] {
            while bytes < target {
                let x: f64 = rng.f();
                let coll = if x < 0.5 { "messaggi" } else if x < 0.85 { "eventi" } else { "ordini" };
                let pp: f64 = rng.real(lo, hi);
                let ts_ms = (plan.t_inizio as f64 * 1000.0 + pp * plan.t_span_s as f64 * 1000.0 + rng.real(0.0, 600_000.0)) as i64;
                let d = gen_doc(&words, &mut rng, coll, &cli, bson::DateTime::from_millis(ts_ms));
                let mut full = doc! { "_id": Bson::Null };
                full.extend(d);
                let size = full.to_vec()?.len() + 7; // with the ObjectId instead of the null
                bytes += size as f64;
                docs.push((ts_ms, coll, full, size));
            }
        }
        docs.sort_by_key(|d| d.0);
        let mut w = Writes::new();
        let mut orders = Vec::new();
        let sampled = plan.sample_every > 0 && i % plan.sample_every == 0 && rep.sample_customers.len() < plan.sample_max;
        if sampled {
            rep.sample_customers.push(i);
        }
        for (ts_ms, coll, mut d, size) in docs {
            counter += 1;
            let id = oid(ts_ms / 1000, p.tag, counter);
            d.insert("_id", id);
            let ns = format!("{db}.{coll}");
            w.put(&mut g, coll, &ns, indexes.get_mut(coll).unwrap(), &d, None).await.map_err(|e| anyhow!("{ns}: insert: {e:?}"))?;
            rep.byte[i] += size as u64;
            rep.byte_bson += size as u64;
            rep.documenti += 1;
            *rep.per_collezione.entry(coll.to_string()).or_insert(0) += 1;
            if coll == "ordini" {
                orders.push(id);
            }
            if sampled {
                let f = sample.as_mut().unwrap();
                f.write_all(&doc! { "c": coll, "d": d }.to_vec()?)?;
            }
        }
        if !orders.is_empty() {
            rep.q2_id[i] = Some(orders[rng.int(0, orders.len() as i64) as usize].to_hex());
        }
        // the batch of this customer: what `commit` would apply, as `Entry::apply` stores it
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
        let ops = crate::layout2::phys_ops(w.final_ops().map_err(|e| anyhow!("{e:?}"))?);
        let mut mine: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for op in ops {
            match op {
                Op::Put(k, v) if k.starts_with(b"u/") => mine.push((k, v)),
                Op::Put(k, v) => spill.push(k, v)?,
                Op::Delete(k) => bail!("{db}: the insert path deleted a key ({})", String::from_utf8_lossy(&k)),
            }
        }
        for (k, v) in Spill::sorted_dedup(&mut mine) {
            direct.add(&k, x0, &tenant_bench::runfmt::live(&v)).await.with_context(|| format!("{db}: customer {cli}"))?;
        }
        if last_print.elapsed().as_secs() >= 30 {
            last_print = std::time::Instant::now();
            let s = tgen.elapsed().as_secs_f64();
            eprintln!("ballast {db}: {} of {n} customers, {:.2} of {:.2} GiB of BSON, {} documents, {:.1} MB/s", i + 1, rep.byte_bson as f64 / GB, p.gb, rep.documenti, rep.byte_bson as f64 / 1e6 / s);
        }
    }
    if let Some(mut f) = sample.take() {
        f.flush()?;
    }
    // the storage counters, once, as `commit` writes them (`collstat::ops_for` reads the stored ones and adds the deltas)
    for op in crate::collstat::ops_for(&mut g, &mut stats).await? {
        match op {
            Op::Put(k, v) => spill.push(crate::layout2::phys(&k).into_owned(), v)?,
            Op::Delete(_) => bail!("{db}: the counters deleted a key"),
        }
    }
    rep.generate_secondi = tgen.elapsed().as_secs_f64();
    rep.customer_rows = direct.rows;
    rep.customer_run_bytes = direct.bytes;
    rep.shared_rows = spill.rows_in;
    let direct_last = direct.last_key().map(|k| k.to_vec());
    let groups = direct.finish().await?;
    spill.flush_if_large()?;
    rep.spill_disk_bytes = spill.disk_bytes;
    let tp = std::time::Instant::now();
    let merge = spill.into_rows()?;
    let gen = Generated { x0, direct: groups, direct_last, sorted: Box::new(merge) };
    let b = g.ballast_publish(&opts, gen).await?;
    rep.publish_secondi = tp.elapsed().as_secs_f64();
    rep.build = Some(b);
    rep.secondi = t0.elapsed().as_secs_f64();
    rep.mb_s = rep.byte_bson as f64 / 1e6 / rep.secondi;
    drop(g);
    Ok(rep)
}

impl Spill {
    /// Keep the last chunk in memory unless it is large (the merge reads it from memory).
    fn flush_if_large(&mut self) -> Result<()> {
        if self.bytes > self.limit / 2 {
            self.flush()?;
        }
        Ok(())
    }
}

/// `--ballast PLAN.json`: generate every database of the plan, `parallel` at a time; one report per database and a
/// summary (`<out>/ballast-gen-summary.json`). Exit status: error if any database failed.
pub async fn run(server: Arc<Server>, plan_path: &str) -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    ensure!(tenant_bench::fx::ENABLED.load(Relaxed), "--ballast writes sealed runs: start it with --sealed-runs (the round's server options)");
    ensure!(crate::locallog::get().is_none(), "--ballast writes no log: start it without --quaderno");
    ensure!(tenant_bench::envelope::START.load(Relaxed), "--ballast publishes runs, which needs envelopes on: start it with --envelopes write (release E1)");
    // the generator publishes each database once, at the end; no automatic publishes in between
    tenant_bench::fx::PUBLISH_EVERY.store(0, Relaxed);
    let plan: Plan = parse_plan(&serde_json::from_slice(&std::fs::read(plan_path).with_context(|| format!("plan {plan_path}"))?)?)?;
    ensure!(!plan.vocab.is_empty() && plan.vocab.len() == plan.cum_vocab.len(), "plan: vocab and cum_vocab");
    std::fs::create_dir_all(&plan.out)?;
    let words = Arc::new(Words { vocab: plan.vocab.clone(), cum: plan.cum_vocab.clone() });
    let plan = Arc::new(plan);
    let t0 = std::time::Instant::now();
    let queue = Arc::new(tokio::sync::Mutex::new(plan.databases.clone()));
    let mut tasks = Vec::new();
    for _ in 0..plan.parallel.max(1) {
        let (server, plan, words, queue) = (Arc::clone(&server), Arc::clone(&plan), Arc::clone(&words), Arc::clone(&queue));
        tasks.push(tokio::spawn(async move {
            let mut out = Vec::new();
            loop {
                let Some(p) = ({ let mut q = queue.lock().await; if q.is_empty() { None } else { Some(q.remove(0)) } }) else { break };
                let name = p.db.clone();
                let r = one_db(Arc::clone(&server), Arc::clone(&plan), Arc::clone(&words), p).await;
                match &r {
                    Ok(rep) => {
                        std::fs::write(Path::new(&plan.out).join(format!("{name}.gen.json")), serde_json::to_vec_pretty(&rep.to_json()).unwrap_or_default()).ok();
                        eprintln!("BALLAST-DB {name}: {:.2} GiB of BSON, {} documents in {:.0} s ({:.1} MB/s); version {} with {} groups",
                            rep.byte_bson as f64 / GB, rep.documenti, rep.secondi, rep.mb_s,
                            rep.build.as_ref().map_or(0, |b| b.version), rep.build.as_ref().map_or(0, |b| b.groups));
                    }
                    Err(e) => eprintln!("BALLAST-DB {name}: FAILED: {e:#}"),
                }
                out.push((name, r.map(|rep| (rep.byte_bson, rep.documenti, rep.secondi)).map_err(|e| format!("{e:#}"))));
            }
            out
        }));
    }
    let mut results = Vec::new();
    for t in tasks {
        results.extend(t.await.map_err(|e| anyhow!("generator task: {e}"))?);
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
    fn object_ids_are_unique_and_carry_the_time() {
        let a = oid(1_761_955_200, 3, 1);
        let b = oid(1_761_955_200, 3, 2);
        assert_ne!(a, b);
        assert_eq!(a.timestamp().timestamp_millis(), 1_761_955_200_000);
    }
}
