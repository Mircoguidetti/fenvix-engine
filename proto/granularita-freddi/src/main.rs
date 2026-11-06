//! M21 · quante GET costa una find fredda vera.
//!
//! `carica`: scrive un carico realistico (storico chat o eventi per dispositivo) con la disposizione delle chiavi del motore,
//! in molti "giorni", con il lavaggio (compattatore size-tiered di SlateDB) acceso durante il caricamento.
//! `misura`: apre il database con un bancone vuoto per i dati (con o senza indici dei blocchi e filtri) e conta le GET di
//! ogni find fredda, con lo stesso piano di lettura del motore (scansione dell'indice, poi get_many dei documenti).

mod bancone;
mod conta;

use anyhow::{bail, Context, Result};
use bancone::{Bancone, Tipo};
use bytes::Bytes;
use conta::Conta;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use slatedb::config::{CompactorOptions, FlushOptions, FlushType, GarbageCollectorDirectoryOptions, GarbageCollectorOptions, ScanOptions};
use slatedb::{Db, DbReader, Settings, SstBlockSize, WriteBatch};
use std::collections::HashMap;
use std::io::Write as _;
use std::ops::Bound;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::{Duration, Instant};

const DB_PATH: &str = "tenant";
const DAY_MS: i64 = 86_400_000;
const T0_MS: i64 = 1_767_225_600_000; // 2026-01-01

// ---------------------------------------------------------------- codifica delle chiavi (come keyenc.rs e layout.rs)

fn sortable_i64(v: i64) -> [u8; 8] {
    ((v as u64) ^ (1 << 63)).to_be_bytes()
}

/// keyenc::component di una stringa (tipo 4, byte con 0 sfuggiti, terminatore 0 1).
fn comp_str(s: &str, asc: bool) -> Vec<u8> {
    let mut out = vec![4u8];
    for &b in s.as_bytes() {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
    out.extend([0, 1]);
    if !asc {
        out.iter_mut().for_each(|x| *x = !*x);
    }
    out
}

/// keyenc::component di una data (tipo 10, millisecondi ordinabili).
fn comp_date(ms: i64, asc: bool) -> Vec<u8> {
    let mut out = vec![10u8];
    out.extend(sortable_i64(ms));
    if !asc {
        out.iter_mut().for_each(|x| *x = !*x);
    }
    out
}

/// layout::encode_sortable di un ObjectId (tipo 8 + 12 byte).
fn id_enc(oid: &[u8; 12]) -> Vec<u8> {
    let mut v = vec![8u8];
    v.extend(oid);
    v
}

fn oid(ts_ms: i64, n: u64) -> [u8; 12] {
    let mut o = [0u8; 12];
    o[..4].copy_from_slice(&((ts_ms / 1000) as u32).to_be_bytes());
    o[4..].copy_from_slice(&n.to_be_bytes());
    o
}

fn doc_key(coll: &str, id: &[u8]) -> Vec<u8> {
    let mut k = format!("doc/{coll}/").into_bytes();
    k.extend(id);
    k
}

fn idx_prefix(coll: &str, name: &str) -> Vec<u8> {
    format!("idx/{coll}/{name}/").into_bytes()
}

/// index::entry_key
fn entry_key(prefix: &[u8], comps: &[u8], id: &[u8]) -> Vec<u8> {
    let mut k = prefix.to_vec();
    k.extend(comps);
    k.extend(id);
    k.extend((id.len() as u16).to_be_bytes());
    k
}

/// Rimedio (a): il documento sta sotto una chiave che comincia con utente e data (raggruppato per utente).
fn cluster_prefix(coll: &str) -> Vec<u8> {
    format!("cdoc/{coll}/").into_bytes()
}

fn prefix_end(p: &[u8]) -> Vec<u8> {
    let mut e = p.to_vec();
    while let Some(last) = e.pop() {
        if last != 0xFF {
            e.push(last + 1);
            return e;
        }
    }
    vec![0xFF; 8]
}

/// Rimedio (c): filtri per prefisso. Il prefisso di una chiave d'indice (o raggruppata) è tutto fino alla fine del
/// componente del proprietario: `idx/{coll}/{indice}/` + utente, oppure `cdoc/{coll}/` + utente.
fn owner_prefix_len(k: &[u8]) -> Option<usize> {
    let slashes = if k.starts_with(b"idx/") {
        3
    } else if k.starts_with(b"cdoc/") {
        2
    } else {
        return None;
    };
    let mut seen = 0;
    let mut pos = None;
    for (i, &b) in k.iter().enumerate() {
        if b == b'/' {
            seen += 1;
            if seen == slashes {
                pos = Some(i + 1);
                break;
            }
        }
    }
    let pos = pos?;
    if k.get(pos) != Some(&4) {
        return None;
    }
    (pos + 1..k.len().saturating_sub(1)).find(|&i| k[i] == 0 && k[i + 1] == 1).map(|i| i + 2)
}

struct PrefissoProprietario;

impl slatedb::PrefixExtractor for PrefissoProprietario {
    fn name(&self) -> &str {
        "fenvix-proprietario-v1"
    }
    fn prefix_len(&self, target: &slatedb::PrefixTarget) -> Option<usize> {
        match target {
            slatedb::PrefixTarget::Point(k) | slatedb::PrefixTarget::Prefix(k) => owner_prefix_len(k),
        }
    }
}

fn politiche_filtri() -> Vec<Arc<dyn slatedb::FilterPolicy>> {
    vec![Arc::new(slatedb::BloomFilterPolicy::new(10).with_prefix_extractor(Arc::new(PrefissoProprietario)))]
}

// ---------------------------------------------------------------- carichi

#[derive(Clone, Copy, PartialEq)]
enum Carico {
    Chat,
    Eventi,
}

impl Carico {
    fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "chat" => Carico::Chat,
            "eventi" => Carico::Eventi,
            _ => bail!("carico sconosciuto {s}"),
        })
    }
    fn coll(&self) -> &'static str {
        match self {
            Carico::Chat => "msgs",
            Carico::Eventi => "events",
        }
    }
    fn index_name(&self) -> &'static str {
        match self {
            Carico::Chat => "user_1_ts_-1",
            Carico::Eventi => "device_1_ts_-1",
        }
    }
    fn owner(&self, u: u32) -> String {
        match self {
            Carico::Chat => format!("u{u:07}"),
            Carico::Eventi => format!("d{u:06}"),
        }
    }
    /// Finestra di Q3 in giorni.
    fn q3_days(&self) -> i64 {
        match self {
            Carico::Chat => 30,
            Carico::Eventi => 7,
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Disp {
    Base,
    Gruppo,
}

struct Args(HashMap<String, String>);

impl Args {
    fn parse() -> (String, Self) {
        let mut it = std::env::args().skip(1);
        let cmd = it.next().unwrap_or_default();
        let mut m = HashMap::new();
        let v: Vec<String> = it.collect();
        let mut i = 0;
        while i < v.len() {
            let k = v[i].trim_start_matches("--").to_string();
            let val = v.get(i + 1).cloned().unwrap_or_default();
            m.insert(k, val);
            i += 2;
        }
        (cmd, Args(m))
    }
    fn s(&self, k: &str, d: &str) -> String {
        self.0.get(k).cloned().unwrap_or_else(|| d.to_string())
    }
    fn n(&self, k: &str, d: u64) -> u64 {
        self.0.get(k).map(|v| v.parse().unwrap()).unwrap_or(d)
    }
}

fn store_locale(dir: &str) -> Result<Arc<dyn ObjectStore>> {
    std::fs::create_dir_all(dir)?;
    Ok(Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir)?))
}

/// Campionatore zipf (pesi r^-s) su n proprietari, con i ranghi mescolati sugli id.
struct Zipf {
    cdf: Vec<f64>,
    perm: Vec<u32>,
}

impl Zipf {
    fn new(n: usize, s: f64, rng: &mut SmallRng) -> Self {
        let mut cdf = Vec::with_capacity(n);
        let mut acc = 0.0;
        for r in 0..n {
            acc += 1.0 / ((r + 1) as f64).powf(s);
            cdf.push(acc);
        }
        let mut perm: Vec<u32> = (0..n as u32).collect();
        for i in (1..n).rev() {
            let j = rng.gen_range(0..=i);
            perm.swap(i, j);
        }
        Zipf { cdf, perm }
    }
    fn sample(&self, rng: &mut SmallRng) -> u32 {
        let x = rng.gen::<f64>() * self.cdf.last().unwrap();
        let r = self.cdf.partition_point(|c| *c < x);
        self.perm[r.min(self.perm.len() - 1)]
    }
}

fn valore(owner: u32, ts: i64, len: usize, rng: &mut SmallRng) -> Vec<u8> {
    let mut v = Vec::with_capacity(len);
    v.push(b'M');
    v.extend(owner.to_be_bytes());
    v.extend(ts.to_be_bytes());
    while v.len() < len {
        v.push(rng.gen_range(b'a'..=b'z'));
    }
    v
}

fn writer_settings(gc: bool, consolida: u64) -> Settings {
    // Come `lifecycle::writer_settings` del motore (memtable/L0 8 MB, nessuna compressione), più il compattatore
    // size-tiered di default acceso nel processo (il motore lo fa girare a parte quando L0 >= 4).
    let mut s = Settings::default();
    s.l0_sst_size_bytes = 8 << 20;
    s.max_unflushed_bytes = 32 << 20;
    let mut co = CompactorOptions::default();
    if consolida > 0 {
        // tetto ai sorted run: il lavaggio unisce i run quando ce ne sarebbero più di `consolida`
        co.scheduler_options.insert("sorted_run_consolidation_threshold".into(), consolida.to_string());
    }
    s.compactor_options = Some(co);
    s.garbage_collector_options = if gc {
        let d = GarbageCollectorDirectoryOptions { interval: Some(Duration::from_secs(10)), min_age: Duration::from_secs(60), dry_run: false };
        let mut g = GarbageCollectorOptions::default();
        g.wal_options = Some(d.clone());
        g.compacted_options = Some(d.clone());
        g.manifest_options = Some(d);
        Some(g)
    } else {
        None
    };
    s
}

fn block_size(kb: u64) -> SstBlockSize {
    match kb {
        4 => SstBlockSize::Block4Kib,
        8 => SstBlockSize::Block8Kib,
        16 => SstBlockSize::Block16Kib,
        32 => SstBlockSize::Block32Kib,
        64 => SstBlockSize::Block64Kib,
        _ => panic!("blocco {kb}"),
    }
}

#[derive(serde::Serialize, Default)]
struct Verita {
    /// (proprietario, documenti, ts min, ts max) dei candidati alle query (almeno 20 documenti).
    candidati: Vec<(u32, u32, i64, i64)>,
    /// (oid hex, proprietario, ts) di documenti a caso per Q2.
    ids: Vec<(String, u32, i64)>,
}

async fn carica(a: &Args) -> Result<()> {
    let carico = Carico::parse(&a.s("carico", "chat"))?;
    let disp = if a.s("disposizione", "base") == "gruppo" { Disp::Gruppo } else { Disp::Base };
    let dir = a.s("dir", "data/x");
    let blk = a.n("blocco", 4);
    let coll = carico.coll();
    let mut rng = SmallRng::seed_from_u64(a.n("seme", 42));
    let (owners, total, days, s, lmin, lmax) = match carico {
        Carico::Chat => (a.n("utenti", 200_000) as usize, a.n("doc", 8_000_000), a.n("giorni", 90) as i64, 0.7, 150usize, 450usize),
        Carico::Eventi => (a.n("utenti", 20_000) as usize, a.n("doc", 7_200_000), a.n("giorni", 30) as i64, 0.0, 120, 280),
    };
    let zipf = Zipf::new(owners, s, &mut rng);
    let raw = store_locale(&dir)?;
    let conta = Arc::new(Conta::new(raw));
    let store: Arc<dyn ObjectStore> = conta.clone();
    let prefisso = a.n("prefisso", 0) == 1;
    let mut builder = Db::builder(DB_PATH, store).with_settings(writer_settings(true, a.n("consolida", 0))).with_sst_block_size(block_size(blk));
    if prefisso {
        builder = builder.with_filter_policies(politiche_filtri());
    }
    let db = builder.build().await?;
    let ip = idx_prefix(coll, carico.index_name());
    let cp = cluster_prefix(coll);
    let mut stat: HashMap<u32, (u32, i64, i64)> = HashMap::new();
    let mut ids: Vec<(String, u32, i64)> = Vec::new();
    let per_day = total / days as u64;
    let mut n: u64 = 0;
    let t_start = Instant::now();
    let mut byte_logici: u64 = 0;
    // eventi: ogni dispositivo scrive a intervalli regolari (12 al giorno), con una fase sua e un po' di rumore
    let fase: Vec<i64> = (0..owners).map(|_| rng.gen_range(0..DAY_MS / 12)).collect();
    for d in 0..days {
        let mut giorno: Vec<(i64, u32)> = match carico {
            Carico::Chat => (0..per_day).map(|_| (T0_MS + d * DAY_MS + rng.gen_range(0..DAY_MS), zipf.sample(&mut rng))).collect(),
            Carico::Eventi => {
                let per_owner = (per_day as usize / owners).max(1) as i64;
                let step = DAY_MS / per_owner;
                let mut v = Vec::with_capacity(owners * per_owner as usize);
                for o in 0..owners {
                    for k in 0..per_owner {
                        let ts = T0_MS + d * DAY_MS + fase[o] + k * step + rng.gen_range(0..60_000);
                        v.push((ts.min(T0_MS + (d + 1) * DAY_MS - 1), o as u32));
                    }
                }
                v
            }
        };
        giorno.sort_unstable();
        for chunk in giorno.chunks(1000) {
            let mut b = WriteBatch::new();
            for &(ts, owner) in chunk {
                n += 1;
                let o = oid(ts, n);
                let id = id_enc(&o);
                let len = rng.gen_range(lmin..=lmax);
                let v = valore(owner, ts, len, &mut rng);
                let name = carico.owner(owner);
                let mut comps = comp_str(&name, true);
                comps.extend(comp_date(ts, false));
                match disp {
                    Disp::Base => {
                        let dk = doc_key(coll, &id);
                        let ek = entry_key(&ip, &comps, &id);
                        byte_logici += (dk.len() + v.len() + ek.len()) as u64;
                        b.put(dk, v);
                        b.put(ek, b"");
                    }
                    Disp::Gruppo => {
                        let mut ck = cp.clone();
                        ck.extend(&comps);
                        ck.extend(&id);
                        let dk = doc_key(coll, &id);
                        byte_logici += (ck.len() + v.len() + dk.len() + ck.len()) as u64;
                        b.put(dk, &ck[..]);
                        b.put(ck, v);
                    }
                }
                let e = stat.entry(owner).or_insert((0, i64::MAX, i64::MIN));
                e.0 += 1;
                e.1 = e.1.min(ts);
                e.2 = e.2.max(ts);
                if rng.gen_ratio(1, 4000) {
                    ids.push((o.iter().map(|x| format!("{x:02x}")).collect(), owner, ts));
                }
            }
            db.write(b).await?;
        }
        let m = db.manifest();
        eprintln!(
            "giorno {d}: {n} documenti, {:.0} s, L0 {}, sorted run {}, PUT {}",
            t_start.elapsed().as_secs_f64(),
            m.l0().len(),
            m.compacted().len(),
            conta.c.put.load(Relaxed)
        );
    }
    db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await?;
    // Il lavaggio finisce quello che ha in corso: aspetta che il foglio indice (manifest) non cambi per 20 s.
    let mut last = String::new();
    let mut stable_since = Instant::now();
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let _ = db.refresh_manifest().await;
        let m = db.manifest();
        let sig = format!("{}:{:?}", m.l0().len(), m.compacted().iter().map(|r| (r.id, r.sst_views().len())).collect::<Vec<_>>());
        if sig != last {
            last = sig;
            stable_since = Instant::now();
            eprintln!("lavaggio: {last}");
        } else if stable_since.elapsed() > Duration::from_secs(20) {
            break;
        }
    }
    db.close().await?;
    pulisci(&dir).await?;
    let mut v = Verita::default();
    let mut cand: Vec<_> = stat.iter().filter(|(_, s)| s.0 >= 20).map(|(o, s)| (*o, s.0, s.1, s.2)).collect();
    cand.sort_unstable();
    v.candidati = cand;
    v.ids = ids;
    std::fs::write(format!("{dir}/verita.json"), serde_json::to_vec(&v)?)?;
    let info = serde_json::json!({
        "carico": a.s("carico", "chat"), "disposizione": a.s("disposizione", "base"), "blocco_kb": blk, "prefisso": prefisso, "consolida": a.n("consolida", 0),
        "documenti": n, "proprietari": owners, "giorni": days, "byte_logici": byte_logici,
        "secondi": t_start.elapsed().as_secs_f64(), "put": conta.c.put.load(Relaxed), "put_byte": conta.c.put_byte.load(Relaxed),
        "candidati": v.candidati.len(),
    });
    std::fs::write(format!("{dir}/carico.json"), serde_json::to_vec_pretty(&info)?)?;
    println!("{info}");
    Ok(())
}

/// Toglie dal disco le scatole che il foglio indice non usa più (sostituite dal lavaggio) e i WAL già scaricati in L0.
/// Il raccoglitore di SlateDB non le toglie durante la prova perché i checkpoint del compattatore durano 15 minuti.
async fn pulisci(dir: &str) -> Result<(u64, u64)> {
    let admin = slatedb::admin::Admin::builder(DB_PATH, store_locale(dir)?).build();
    let m = admin.read_manifest(None).await?.context("manifest")?;
    let mut vivi = std::collections::HashSet::new();
    for v in m.l0() {
        vivi.insert(v.sst.id.value().to_string());
    }
    for sr in m.compacted() {
        for v in sr.sst_views() {
            vivi.insert(v.sst.id.value().to_string());
        }
    }
    // sicurezza: ogni scatola viva deve esistere sul disco, altrimenti non si tocca niente
    let files: std::collections::HashSet<String> = std::fs::read_dir(format!("{dir}/{DB_PATH}/compacted"))?
        .map(|e| e.unwrap().file_name().to_string_lossy().trim_end_matches(".sst").to_string())
        .collect();
    for v in &vivi {
        anyhow::ensure!(files.contains(v), "scatola viva {v} non trovata sul disco: pulizia annullata");
    }
    let (mut tolti, mut byte) = (0u64, 0u64);
    for e in std::fs::read_dir(format!("{dir}/{DB_PATH}/compacted"))? {
        let e = e?;
        let name = e.file_name().to_string_lossy().trim_end_matches(".sst").to_string();
        if !vivi.contains(&name) {
            byte += e.metadata()?.len();
            std::fs::remove_file(e.path())?;
            tolti += 1;
        }
    }
    let replay = m.replay_after_wal_id();
    for e in std::fs::read_dir(format!("{dir}/{DB_PATH}/wal"))? {
        let e = e?;
        let name = e.file_name().to_string_lossy().trim_end_matches(".sst").to_string();
        if name.parse::<u64>().map(|id| id <= replay).unwrap_or(false) {
            byte += e.metadata()?.len();
            std::fs::remove_file(e.path())?;
            tolti += 1;
        }
    }
    anyhow::ensure!(vivi.len() as u64 == (m.l0().len() + m.compacted().iter().map(|r| r.sst_views().len()).sum::<usize>()) as u64);
    eprintln!("pulizia: {tolti} file tolti, {byte} byte; scatole vive {}", vivi.len());
    Ok((tolti, byte))
}

/// Lo stato del foglio indice di un database qualsiasi (anche quello del motore vero: `--percorso tenants/{nome}`).
async fn stato(dir: &str, percorso: &str) -> Result<()> {
    let admin = slatedb::admin::Admin::builder(percorso.to_string(), store_locale(dir)?).build();
    let m = admin.read_manifest(None).await?.context("manifest")?;
    let run: Vec<(usize, u64)> = m.compacted().iter().map(|r| (r.sst_views().len(), r.estimate_size())).collect();
    let (mut b, mut i, mut f) = (0u64, 0u64, 0u64);
    for v in m.l0().iter().chain(m.compacted().iter().flat_map(|r| r.sst_views().iter())) {
        b += v.sst.estimate_size();
        i += v.sst.info.index_len;
        f += v.sst.info.filter_len;
    }
    println!("{}", serde_json::json!({"l0": m.l0().len(), "sorted_run": run.len(), "run": run, "byte_scatole": b, "byte_indici_blocchi": i, "byte_filtri": f}));
    Ok(())
}

// ---------------------------------------------------------------- misura

struct Lettore {
    r: DbReader,
    prefisso: bool,
}

impl Lettore {
    /// `Entry::scan_range` del motore (read-ahead, 4 fetch, blocchi in cache).
    async fn scan(&self, start: Vec<u8>, end: Vec<u8>, limit: usize, read_ahead: usize) -> Result<Vec<(Bytes, Bytes)>> {
        let opts = ScanOptions {
            read_ahead_bytes: read_ahead,
            max_fetch_tasks: 4,
            cache_blocks: true,
            order: slatedb::IterationOrder::Ascending,
            ..Default::default()
        };
        let mut it = match owner_prefix_len(&start).filter(|_| self.prefisso) {
            // rimedio (c): scansione per prefisso, così i filtri per prefisso saltano le scatole senza quel proprietario
            Some(n) => {
                let p = start[..n].to_vec();
                let hi = if end.starts_with(&p) { Bound::Excluded(end[n..].to_vec()) } else { Bound::Unbounded };
                self.r.scan_prefix_with_options(&p, (Bound::Included(start[n..].to_vec()), hi), &opts).await?
            }
            None => self.r.scan_with_options((Bound::Included(start), Bound::Excluded(end)), &opts).await?,
        };
        let mut out = Vec::new();
        while let Some(kv) = it.next().await? {
            out.push((kv.key, kv.value));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// `Entry::get_many` del motore: GET in parallelo, 32 alla volta.
    async fn get_many(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Bytes>>> {
        let futs: Vec<_> = keys.into_iter().map(|k| async move { self.r.get(k).await }).collect();
        Ok(futures::stream::iter(futs).buffered(32).try_collect().await?)
    }
}

const INDEX_READ_AHEAD: usize = 64 << 10;

/// Una find per indice secondario come in `coll.rs`: scansione dell'indice (con il completamento del gruppo di chiavi
/// uguali), poi i documenti con get_many.
async fn find_indice(l: &Lettore, carico: Carico, start: Vec<u8>, end: Vec<u8>, n: usize, prefix_len: usize) -> Result<Vec<Bytes>> {
    let coll = carico.coll();
    let rows = l.scan(start, end.clone(), n, INDEX_READ_AHEAD).await?;
    if rows.len() >= n {
        // completa l'ultimo gruppo di chiavi uguali (coll.rs: IxCursor::next_batch)
        let mut st = rows.last().unwrap().0.to_vec();
        st.push(0);
        let _more = l.scan(st, end, 64, INDEX_READ_AHEAD).await?;
    }
    let _ = prefix_len;
    let keys: Vec<Vec<u8>> = rows
        .iter()
        .map(|(k, _)| {
            let n = u16::from_be_bytes([k[k.len() - 2], k[k.len() - 1]]) as usize;
            doc_key(coll, &k[k.len() - 2 - n..k.len() - 2])
        })
        .collect();
    Ok(l.get_many(keys).await?.into_iter().flatten().collect())
}

#[derive(serde::Serialize, Default, Clone)]
struct Riga {
    q: String,
    get: u64,
    get_byte: u64,
    mancate_blocchi: u64,
    mancate_indici: u64,
    mancate_filtri: u64,
    mancate_stat: u64,
    ms: f64,
    docs: usize,
    max_in_volo: u64,
}

fn verifica(docs: &[Bytes], owner: u32) -> Result<()> {
    for d in docs {
        if d.len() < 13 || d[0] != b'M' || u32::from_be_bytes([d[1], d[2], d[3], d[4]]) != owner {
            bail!("documento sbagliato per il proprietario {owner}");
        }
    }
    Ok(())
}

fn ts_of(d: &Bytes) -> i64 {
    i64::from_be_bytes(d[5..13].try_into().unwrap())
}

async fn misura(a: &Args) -> Result<()> {
    let carico = Carico::parse(&a.s("carico", "chat"))?;
    let disp = if a.s("disposizione", "base") == "gruppo" { Disp::Gruppo } else { Disp::Base };
    let dir = a.s("dir", "data/x");
    let bancone_s = a.s("bancone", "S1");
    let nq = a.n("n", 300) as usize;
    let ritardo_ms = a.n("ritardo-ms", 0);
    let out = a.s("out", "risultati/misure.jsonl");
    let etichetta = a.s("etichetta", &dir);
    let coll = carico.coll();
    let v: serde_json::Value = serde_json::from_slice(&std::fs::read(format!("{dir}/verita.json"))?)?;
    let cand: Vec<(u32, u32, i64, i64)> = serde_json::from_value(v["candidati"].clone())?;
    let ids: Vec<(String, u32, i64)> = serde_json::from_value(v["ids"].clone())?;
    let raw = store_locale(&dir)?;
    let conta = Arc::new(Conta::new(raw));
    let store: Arc<dyn ObjectStore> = conta.clone();
    let tieni_meta = bancone_s == "S1";
    let banco = Arc::new(Bancone::new(tieni_meta));
    let mut ro = slatedb::config::DbReaderOptions::default();
    ro.manifest_poll_interval = Duration::from_secs(3600);
    ro.checkpoint_lifetime = Duration::from_secs(4 * 3600);
    let prefisso = a.n("prefisso", 0) == 1;
    let mut rb = DbReader::builder(DB_PATH, store).with_options(ro).with_db_cache(banco.clone(), 1);
    if prefisso {
        rb = rb.with_filter_policies(politiche_filtri());
    }
    let r = rb.build().await?;
    let l = Lettore { r, prefisso };

    // Lo stato lasciato dal lavaggio, dal foglio indice: scatole, sorted run, byte di dati, indici dei blocchi e filtri.
    let admin = slatedb::admin::Admin::builder(DB_PATH, store_locale(&dir)?).build();
    let m = admin.read_manifest(None).await?.context("manifest")?;
    let mut n_sst = 0u64;
    let (mut b_tot, mut b_idx, mut b_filt) = (0u64, 0u64, 0u64);
    let mut hs: Vec<(u64, u64, u64)> = m.l0().iter().map(|v| (v.sst.estimate_size(), v.sst.info.index_len, v.sst.info.filter_len)).collect();
    for sr in m.compacted() {
        hs.extend(sr.sst_views().iter().map(|v| (v.sst.estimate_size(), v.sst.info.index_len, v.sst.info.filter_len)));
    }
    for (t, i, fl) in &hs {
        n_sst += 1;
        b_tot += t;
        b_idx += i;
        b_filt += fl;
    }
    let stato = serde_json::json!({
        "etichetta": etichetta, "tipo": "stato", "l0": m.l0().len(), "sorted_run": m.compacted().len(), "scatole": n_sst,
        "byte_scatole": b_tot, "byte_indici_blocchi": b_idx, "byte_filtri": b_filt,
        "run": m.compacted().iter().map(|r| (r.sst_views().len(), r.estimate_size())).collect::<Vec<_>>(),
    });
    eprintln!("{stato}");

    let mut rng = SmallRng::seed_from_u64(a.n("seme", 7));
    let pick = |rng: &mut SmallRng| cand[rng.gen_range(0..cand.len())];
    let ip = idx_prefix(coll, carico.index_name());
    let cp = cluster_prefix(coll);

    // Una query; restituisce (nome, documenti).
    async fn una(l: &Lettore, carico: Carico, disp: Disp, q: &str, c: (u32, u32, i64, i64), idr: &(String, u32, i64), ip: &[u8], cp: &[u8], rng_ts: i64) -> Result<usize> {
        let coll = carico.coll();
        let name = carico.owner(c.0);
        let pref_owner = |p: &[u8]| {
            let mut k = p.to_vec();
            k.extend(comp_str(&name, true));
            k
        };
        match (q, disp) {
            ("Q1", Disp::Base) => {
                let p = pref_owner(ip);
                let docs = find_indice(l, carico, p.clone(), prefix_end(&p), 20, p.len()).await?;
                verifica(&docs, c.0)?;
                anyhow::ensure!(docs.len() == 20, "Q1: {} documenti", docs.len());
                Ok(docs.len())
            }
            ("Q1", Disp::Gruppo) => {
                let p = pref_owner(cp);
                let rows = l.scan(p.clone(), prefix_end(&p), 20, INDEX_READ_AHEAD).await?;
                let docs: Vec<Bytes> = rows.into_iter().map(|(_, v)| v).collect();
                verifica(&docs, c.0)?;
                anyhow::ensure!(docs.len() == 20, "Q1: {} documenti", docs.len());
                Ok(docs.len())
            }
            ("Q2", _) => {
                let o: Vec<u8> = (0..12).map(|i| u8::from_str_radix(&idr.0[2 * i..2 * i + 2], 16).unwrap()).collect();
                let k = doc_key(coll, &id_enc(&o.try_into().unwrap()));
                let v = l.r.get(&k).await?.context("Q2: documento mancante")?;
                let d = if disp == Disp::Gruppo { l.r.get(&v).await?.context("Q2: documento raggruppato mancante")? } else { v };
                verifica(&[d.clone()], idr.1)?;
                anyhow::ensure!(ts_of(&d) == idr.2);
                Ok(1)
            }
            ("Q3", _) => {
                // documenti dell'utente tra hi - finestra e hi, al massimo 100
                let hi = rng_ts;
                let lo = hi - carico.q3_days() * DAY_MS;
                let base = match disp {
                    Disp::Base => pref_owner(ip),
                    Disp::Gruppo => pref_owner(cp),
                };
                let mut s = base.clone();
                s.extend(comp_date(hi, false));
                let mut e = base.clone();
                e.extend(comp_date(lo - 1, false));
                let docs = match disp {
                    Disp::Base => find_indice(l, carico, s, e, 100, base.len()).await?,
                    Disp::Gruppo => l.scan(s, e, 100, INDEX_READ_AHEAD).await?.into_iter().map(|(_, v)| v).collect(),
                };
                verifica(&docs, c.0)?;
                for d in &docs {
                    let t = ts_of(d);
                    anyhow::ensure!(t >= lo && t <= hi, "Q3: data fuori finestra");
                }
                Ok(docs.len())
            }
            _ => unreachable!(),
        }
    }

    // Riscaldamento degli indici dei blocchi e dei filtri (solo S1): query su proprietari e id diversi da quelli misurati.
    if tieni_meta {
        let t = Instant::now();
        let mut wrng = SmallRng::seed_from_u64(999);
        let nw = a.n("riscaldo", 1500) as usize;
        for i in 0..nw {
            let c = pick(&mut wrng);
            let idr = &ids[wrng.gen_range(0..ids.len())];
            let q = ["Q1", "Q2", "Q3"][i % 3];
            let hi = wrng.gen_range(c.2..=c.3);
            una(&l, carico, disp, q, c, idr, &ip, &cp, hi).await?;
            banco.svuota_dati();
        }
        eprintln!(
            "riscaldo: {nw} query in {:.0} s; bancone: indici {} voci {} byte, filtri {} voci {} byte, stat {} voci",
            t.elapsed().as_secs_f64(),
            banco.voci(Tipo::Indice),
            banco.byte_tenuti(Tipo::Indice),
            banco.voci(Tipo::Filtro),
            banco.byte_tenuti(Tipo::Filtro),
            banco.voci(Tipo::Stat)
        );
    } else {
        banco.svuota_tutto();
    }

    conta.ritardo_us.store(ritardo_ms * 1000, Relaxed);
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&out)?;
    writeln!(f, "{}", stato)?;
    let mut righe: Vec<Riga> = Vec::new();
    for i in 0..nq * 3 {
        let q = ["Q1", "Q2", "Q3"][i % 3];
        let c = pick(&mut rng);
        let idr = &ids[rng.gen_range(0..ids.len())];
        let hi = rng.gen_range(c.2..=c.3);
        banco.svuota_dati();
        banco.azzera_contatori();
        conta.c.azzera();
        let t0 = Instant::now();
        let docs = una(&l, carico, disp, q, c, idr, &ip, &cp, hi).await?;
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        let riga = Riga {
            q: q.into(),
            get: conta.c.get_totali(),
            get_byte: conta.c.get_byte.load(Relaxed),
            mancate_blocchi: banco.mancate[Tipo::Blocco as usize].load(Relaxed),
            mancate_indici: banco.mancate[Tipo::Indice as usize].load(Relaxed),
            mancate_filtri: banco.mancate[Tipo::Filtro as usize].load(Relaxed),
            mancate_stat: banco.mancate[Tipo::Stat as usize].load(Relaxed),
            ms,
            docs,
            max_in_volo: conta.c.max_in_volo.load(Relaxed),
        };
        righe.push(riga);
    }
    // riassunto per query
    for q in ["Q1", "Q2", "Q3"] {
        let rs: Vec<&Riga> = righe.iter().filter(|r| r.q == q).collect();
        let mut g: Vec<u64> = rs.iter().map(|r| r.get).collect();
        g.sort_unstable();
        let mut ms: Vec<f64> = rs.iter().map(|r| r.ms).collect();
        ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let k = rs.len() as f64;
        let mean = |f: &dyn Fn(&Riga) -> f64| rs.iter().map(|r| f(r)).sum::<f64>() / k;
        let pct = |v: &Vec<u64>, p: f64| v[((v.len() as f64 - 1.0) * p).round() as usize];
        let j = serde_json::json!({
            "etichetta": etichetta, "tipo": "riassunto", "bancone": bancone_s, "ritardo_ms": ritardo_ms, "q": q, "n": rs.len(),
            "get_media": mean(&|r| r.get as f64), "get_p50": pct(&g, 0.5), "get_p90": pct(&g, 0.9), "get_max": g.last(),
            "byte_media": mean(&|r| r.get_byte as f64), "docs_media": mean(&|r| r.docs as f64),
            "mancate_blocchi": mean(&|r| r.mancate_blocchi as f64), "mancate_indici": mean(&|r| r.mancate_indici as f64),
            "mancate_filtri": mean(&|r| r.mancate_filtri as f64), "mancate_stat": mean(&|r| r.mancate_stat as f64),
            "ms_p50": ms[ms.len() / 2], "ms_media": mean(&|r| r.ms), "ms_p90": ms[((ms.len() as f64 - 1.0) * 0.9) as usize],
            "giri_p50": if ritardo_ms > 0 { ms[ms.len() / 2] / ritardo_ms as f64 } else { 0.0 },
            "max_in_volo_media": mean(&|r| r.max_in_volo as f64),
        });
        println!("{j}");
        writeln!(f, "{j}")?;
    }
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let (cmd, a) = Args::parse();
    match cmd.as_str() {
        "carica" => carica(&a).await,
        "misura" => misura(&a).await,
        "stato" => stato(&a.s("dir", "data/x"), &a.s("percorso", DB_PATH)).await,
        "pulisci" => pulisci(&a.s("dir", "data/x")).await.map(|_| ()),
        _ => bail!("uso: carica|misura --carico chat|eventi --disposizione base|gruppo --blocco 4|64 --dir D"),
    }
}
