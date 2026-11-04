//! M31 · i millisecondi a freddo su S3 vero (da M29: stessa disposizione dei dati, pile a gradini T = 4, blocchi 64 KB,
//! lz4, lettore regolato). Novità: il magazzino si sceglie (`--magazzino locale|s3`; S3 compatibile in locale con
//! `AWS_ENDPOINT`), `copia` porta una cartella su S3, `tempi` misura i millisecondi delle tre find come su Atlas.
//!
//! (Da M26:) quanto costa il lavaggio che ordina i dati per cliente.
//!
//! `carica`: carico "misto" (chat, eventi, ordini con aggiornamenti) per clienti con attività zipf, prima riempimento
//! fino alla taglia, poi regime con ritenzione (i documenti più vecchi si cancellano). Conta i byte e le richieste del
//! lavaggio, la CPU del processo e le pile (sorted run + L0) durante il regime.
//! `misura`: find fredde (Q1, Q2, Q3) e "scaldare un cliente" con il bancone S1 di M21.

mod bancone;
mod conta;
mod documenti;
mod livelli;

use anyhow::{bail, Context, Result};
use bancone::{Bancone, Tipo};
use bytes::Bytes;
use conta::Conta;
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use slatedb::config::{
    CompactorOptions, FlushOptions, FlushType, GarbageCollectorDirectoryOptions, GarbageCollectorOptions, ScanOptions,
    WriteOptions,
};
use slatedb::{Db, DbReader, Settings, WriteBatch};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write as _;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

const DB_PATH: &str = "tenant";
const T0_MS: i64 = 1_767_225_600_000; // 2026-01-01
const PASSO_MS: i64 = 1_000; // ogni scrittura fa avanzare l'orologio di 1 s
const MSGS: u8 = 0;
const EVENTS: u8 = 1;
const ORDERS: u8 = 2;

// ---------------------------------------------------------------- disposizioni delle chiavi

#[derive(Clone, Copy, PartialEq, Debug)]
enum Disp {
    /// Come il motore: documento sotto `_id`, indice {cliente, ts} per collezione.
    Oggi,
    /// Rimedio di M21: documento sotto collezione + cliente + ts; biglietto `_id` → chiave.
    Collezione,
    /// Cliente in testa a tutto: cliente + collezione + ts; biglietto `_id` → chiave.
    Cliente,
}

impl Disp {
    fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "oggi" => Disp::Oggi,
            "collezione" => Disp::Collezione,
            "cliente" => Disp::Cliente,
            _ => bail!("disposizione {s}"),
        })
    }
}

fn ts_desc(ts: i64) -> [u8; 8] {
    (!((ts as u64) ^ (1 << 63))).to_be_bytes()
}
#[allow(dead_code)]
fn ts_asc(ts: i64) -> [u8; 8] {
    ((ts as u64) ^ (1 << 63)).to_be_bytes()
}
fn hash32(x: u32) -> u32 {
    let mut h = x.wrapping_mul(0x9E37_79B1);
    h ^= h >> 15;
    h = h.wrapping_mul(0x85EB_CA77);
    h ^ (h >> 13)
}

#[derive(Clone, Copy)]
struct Rec {
    coll: u8,
    status: u8,
    live: bool,
    cust: u32,
    bytes: u32,
    ts: i64,
}

#[derive(Clone)]
struct Chiavi {
    disp: Disp,
    secchi: u32,
}

impl Chiavi {
    fn secchio(&self, cust: u32) -> u8 {
        if self.secchi <= 1 {
            0
        } else {
            (hash32(cust) % self.secchi) as u8
        }
    }
    fn id(n: u64, ts: i64) -> [u8; 12] {
        let mut o = [0u8; 12];
        o[..4].copy_from_slice(&((ts / 1000) as u32).to_be_bytes());
        o[4..].copy_from_slice(&n.to_be_bytes());
        o
    }
    /// Prefisso del proprietario per una collezione (i filtri per prefisso lavorano su questi 6 byte).
    fn prefisso_cliente(&self, cust: u32, coll: u8) -> Vec<u8> {
        let mut k = match self.disp {
            Disp::Oggi => vec![b'I', coll],
            Disp::Collezione => vec![b'C', coll],
            Disp::Cliente => vec![b'K', self.secchio(cust)],
        };
        k.extend(cust.to_be_bytes());
        if self.disp == Disp::Cliente {
            k.push(coll);
        }
        k
    }
    /// Tutte le chiavi di un cliente (solo disposizione "cliente").
    fn prefisso_tutto(&self, cust: u32) -> Vec<u8> {
        let mut k = vec![b'K', self.secchio(cust)];
        k.extend(cust.to_be_bytes());
        k
    }
    fn doc(&self, coll: u8, id: &[u8; 12]) -> Vec<u8> {
        let mut k = vec![b'D', coll];
        k.extend(id);
        k
    }
    /// Chiave del dato: per "oggi" è la chiave del documento, per le altre la chiave raggruppata.
    fn dato(&self, r: &Rec, id: &[u8; 12]) -> Vec<u8> {
        match self.disp {
            Disp::Oggi => self.doc(r.coll, id),
            _ => {
                let mut k = self.prefisso_cliente(r.cust, r.coll);
                k.extend(ts_desc(r.ts));
                k.extend(id);
                k
            }
        }
    }
    fn voce_indice(&self, r: &Rec, id: &[u8; 12]) -> Vec<u8> {
        let mut k = self.prefisso_cliente(r.cust, r.coll);
        k.extend(ts_desc(r.ts));
        k.extend(id);
        k
    }
    fn globale(&self, status: u8, ts: i64, id: &[u8; 12]) -> Vec<u8> {
        // M31: date all'indietro (la più nuova prima), così "gli ultimi 100" sono una scansione in avanti.
        let mut k = vec![b'G', status];
        k.extend(ts_desc(ts));
        k.extend(id);
        k
    }
}

fn owner_prefix_len(k: &[u8]) -> Option<usize> {
    match k.first() {
        Some(b'I') | Some(b'C') | Some(b'K') if k.len() >= 6 => Some(6),
        _ => None,
    }
}

struct PrefissoProprietario;
impl slatedb::PrefixExtractor for PrefissoProprietario {
    fn name(&self) -> &str {
        "m26-proprietario-v1"
    }
    fn prefix_len(&self, target: &slatedb::PrefixTarget) -> Option<usize> {
        match target {
            slatedb::PrefixTarget::Point(k) | slatedb::PrefixTarget::Prefix(k) => owner_prefix_len(k),
        }
    }
}

/// Segmenti (partizioni con lavaggio indipendente): i primi 2 byte di ogni chiave.
struct Segmento;
impl slatedb::PrefixExtractor for Segmento {
    fn name(&self) -> &str {
        "m26-segmento-v1"
    }
    fn prefix_len(&self, target: &slatedb::PrefixTarget) -> Option<usize> {
        match target {
            slatedb::PrefixTarget::Point(k) | slatedb::PrefixTarget::Prefix(k) => {
                if k.len() >= 2 {
                    Some(2)
                } else {
                    None
                }
            }
        }
    }
}

fn politiche_filtri() -> Vec<Arc<dyn slatedb::FilterPolicy>> {
    vec![Arc::new(slatedb::BloomFilterPolicy::new(10).with_prefix_extractor(Arc::new(PrefissoProprietario)))]
}

// ---------------------------------------------------------------- utilità

struct Args(HashMap<String, String>);
impl Args {
    fn parse() -> (String, Self) {
        let mut it = std::env::args().skip(1);
        let cmd = it.next().unwrap_or_default();
        let v: Vec<String> = it.collect();
        let mut m = HashMap::new();
        let mut i = 0;
        while i < v.len() {
            m.insert(v[i].trim_start_matches("--").to_string(), v.get(i + 1).cloned().unwrap_or_default());
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
    fn f(&self, k: &str, d: f64) -> f64 {
        self.0.get(k).map(|v| v.parse().unwrap()).unwrap_or(d)
    }
}

fn store_locale(dir: &str) -> Result<Arc<dyn ObjectStore>> {
    std::fs::create_dir_all(dir)?;
    Ok(Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir)?))
}

/// Il magazzino: `locale` (cartella `dir`, com'era in M29) oppure `s3` (S3 vero o compatibile). Per `s3` tutto viene
/// dall'ambiente, niente chiavi nei file: credenziali della macchina (ruolo dell'istanza) o `AWS_ACCESS_KEY_ID` e
/// `AWS_SECRET_ACCESS_KEY`; `AWS_ENDPOINT` e `AWS_ALLOW_HTTP=true` per un S3 compatibile in locale; regione da
/// `AWS_REGION` (di serie eu-west-1); bucket da `--bucket` o `M31_BUCKET`; gli oggetti stanno sotto `--prefisso`.
fn magazzino(a: &Args, dir: &str) -> Result<Arc<dyn ObjectStore>> {
    match a.s("magazzino", "locale").as_str() {
        "locale" => store_locale(dir),
        "s3" => {
            let bucket = a.s("bucket", &std::env::var("M31_BUCKET").unwrap_or_default());
            anyhow::ensure!(!bucket.is_empty(), "manca il bucket (--bucket o M31_BUCKET)");
            let regione = std::env::var("AWS_REGION").unwrap_or_else(|_| "eu-west-1".into());
            let s3 = object_store::aws::AmazonS3Builder::from_env()
                .with_region(regione)
                .with_bucket_name(bucket)
                .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
                .build()?;
            let prefisso = a.s("prefisso", std::path::Path::new(dir).file_name().and_then(|x| x.to_str()).unwrap_or("m31"));
            Ok(Arc::new(object_store::prefix::PrefixStore::new(s3, prefisso.as_str())))
        }
        x => bail!("magazzino {x}: locale o s3"),
    }
}

fn cpu_s() -> f64 {
    unsafe {
        let mut u: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut u);
        let t = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 / 1e6;
        t(u.ru_utime) + t(u.ru_stime)
    }
}

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

/// Documenti realistici (M29) o byte casuali (M26).
static VERI: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

fn valore(r: &Rec, len: usize, rng: &mut SmallRng) -> Vec<u8> {
    let mut v = Vec::with_capacity(len);
    v.push(b'M');
    v.extend(r.cust.to_be_bytes());
    v.extend(r.ts.to_be_bytes());
    v.push(r.coll);
    v.push(r.status);
    if VERI.load(Relaxed) {
        v.extend(documenti::corpo(r.coll, r.cust, r.ts, r.status, len.saturating_sub(v.len()), rng));
        v.truncate(len);
        return v;
    }
    let seed: u64 = rng.gen();
    let mut x = seed;
    while v.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        v.extend_from_slice(&x.to_le_bytes());
    }
    v.truncate(len);
    v
}

fn val_cust(d: &[u8]) -> u32 {
    u32::from_be_bytes([d[1], d[2], d[3], d[4]])
}
fn val_ts(d: &[u8]) -> i64 {
    i64::from_be_bytes(d[5..13].try_into().unwrap())
}
fn val_coll(d: &[u8]) -> u8 {
    d[13]
}

fn opzioni_lavaggio(consolida: u64) -> CompactorOptions {
    let mut co = CompactorOptions::default();
    co.checkpoint_lifetime = Duration::from_secs(30);
    co.poll_interval = Duration::from_millis(500); // il lavaggio guarda spesso: le L0 non aspettano 5 s
    if consolida > 0 {
        co.scheduler_options.insert("sorted_run_consolidation_threshold".into(), consolida.to_string());
    }
    co
}

fn codec(nome: &str) -> Option<slatedb::config::CompressionCodec> {
    match nome {
        "lz4" => Some(slatedb::config::CompressionCodec::Lz4),
        "zstd" => Some(slatedb::config::CompressionCodec::Zstd),
        _ => None,
    }
}

fn blocco(kb: u64) -> slatedb::config::SstBlockSize {
    use slatedb::config::SstBlockSize::*;
    match kb {
        4 => Block4Kib,
        16 => Block16Kib,
        64 => Block64Kib,
        _ => panic!("blocco {kb} KB"),
    }
}

fn settings(lavaggio: bool, consolida: u64, esterno: bool, comp: &str) -> Settings {
    let mut s = Settings::default();
    s.compression_codec = codec(comp);
    s.l0_sst_size_bytes = 8 << 20;
    s.max_unflushed_bytes = 32 << 20;
    s.wal_enabled = false; // nel disegno il quaderno sostituisce il WAL
    if esterno {
        s.compactor_options = None; // lavaggio con uno schedulatore nostro, in un compattatore a parte
    } else if lavaggio {
        s.compactor_options = Some(opzioni_lavaggio(consolida));
    } else {
        s.compactor_options = None;
        s.l0_max_ssts = 1_000_000;
        s.l0_max_ssts_per_key = 1_000_000;
    }
    let d = GarbageCollectorDirectoryOptions { interval: Some(Duration::from_secs(10)), min_age: Duration::from_secs(45), dry_run: false };
    let mut g = GarbageCollectorOptions::default();
    g.wal_options = Some(d.clone());
    g.compacted_options = Some(d.clone());
    g.manifest_options = Some(d);
    s.garbage_collector_options = Some(g);
    s
}

/// Pile viste da una find di un cliente: sorted run + L0 del suo albero (segmento o radice).
/// Restituisce (media sui segmenti dei clienti, massimo, numero di alberi dei clienti).
fn composizione(m: &slatedb::manifest::VersionedManifest) -> (f64, f64) {
    let segs: Vec<(usize, usize)> = m.segments().iter().filter(|s| s.prefix().first() == Some(&b'K')).map(|s| (s.l0().len(), s.compacted().len())).collect();
    if segs.is_empty() {
        (m.l0().len() as f64, m.compacted().len() as f64)
    } else {
        let n = segs.len() as f64;
        (segs.iter().map(|x| x.0).sum::<usize>() as f64 / n, segs.iter().map(|x| x.1).sum::<usize>() as f64 / n)
    }
}

fn pile(m: &slatedb::manifest::VersionedManifest) -> (f64, usize, usize) {
    let segs: Vec<usize> = m.segments().iter().filter(|s| s.prefix().first() == Some(&b'K')).map(|s| s.l0().len() + s.compacted().len()).collect();
    if segs.is_empty() {
        let n = m.l0().len() + m.compacted().len();
        (n as f64, n, 1)
    } else {
        (segs.iter().sum::<usize>() as f64 / segs.len() as f64, *segs.iter().max().unwrap(), segs.len())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct Verita {
    /// (cliente, msgs vivi, events vivi, orders vivi, ultimo ts scritto)
    clienti: Vec<(u32, u32, u32, u32, i64)>,
    /// (n, coll, cliente, ts) di documenti vivi a caso, per Q2
    ids: Vec<(u64, u8, u32, i64)>,
    ts_min: i64,
    ts_max: i64,
    /// M31: clienti con quello che serve alle tre find come su Atlas
    #[serde(default)]
    m31: Vec<ClienteM31>,
}

/// M31: un cliente pronto per le tre find. Q1 ultimi 20 messaggi (ne ha almeno 20); Q2 un suo documento per `_id`;
/// Q3 finestre di date con esattamente 85 eventi (circa 85 come gli eventi di 14 giorni in P1).
#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct ClienteM31 {
    c: u32,
    dormiente: bool,
    msgs: u32,
    /// (n, collezione, ts) di un documento vivo a caso
    q2: (u64, u8, i64),
    /// (hi, lo) inclusi
    q3: Vec<(i64, i64)>,
    /// Q4 (aggiunta del fondatore): finestre con esattamente 500 messaggi, vuota se il cliente ne ha meno
    #[serde(default)]
    q4: Vec<(i64, i64)>,
}

const MESSAGGI_Q4: usize = 500;

const EVENTI_Q3: usize = 85;

fn verita_m31(fifo: &VecDeque<Rec>, base: u64, vivi: &[[u32; 3]], _ultimo: &[i64], dormiente: &[bool], rng: &mut SmallRng) -> Vec<ClienteM31> {
    let n = vivi.len();
    let mut ev: Vec<Vec<i64>> = vec![Vec::new(); n];
    let mut ms: Vec<Vec<i64>> = vec![Vec::new(); n];
    let mut q2: Vec<Option<(u64, u8, i64)>> = vec![None; n];
    let mut visti = vec![0u32; n];
    for (i, r) in fifo.iter().enumerate() {
        if !r.live {
            continue;
        }
        let c = r.cust as usize;
        if r.coll == EVENTS {
            ev[c].push(r.ts);
        }
        if r.coll == MSGS {
            ms[c].push(r.ts);
        }
        visti[c] += 1;
        if rng.gen_range(0..visti[c]) == 0 {
            q2[c] = Some((base + i as u64, r.coll, r.ts));
        }
    }
    let mut out = Vec::new();
    for c in 0..n {
        if vivi[c][0] < 20 || ev[c].len() < EVENTI_Q3 || q2[c].is_none() {
            continue;
        }
        let e = &mut ev[c];
        e.sort_unstable_by(|a, b| b.cmp(a));
        let q3 = (0..4)
            .map(|_| {
                let i = rng.gen_range(0..=e.len() - EVENTI_Q3);
                (e[i], e[i + EVENTI_Q3 - 1])
            })
            .collect();
        let m = &mut ms[c];
        m.sort_unstable_by(|a, b| b.cmp(a));
        let q4 = if m.len() >= MESSAGGI_Q4 {
            (0..2)
                .map(|_| {
                    let i = rng.gen_range(0..=m.len() - MESSAGGI_Q4);
                    (m[i], m[i + MESSAGGI_Q4 - 1])
                })
                .collect()
        } else {
            Vec::new()
        };
        out.push(ClienteM31 { c: c as u32, dormiente: dormiente[c], msgs: vivi[c][0], q2: q2[c].unwrap(), q3, q4 });
    }
    out
}

// ---------------------------------------------------------------- carica

async fn carica(a: &Args) -> Result<()> {
    let disp = Disp::parse(&a.s("disposizione", "cliente"))?;
    let dir = a.s("dir", "data/x");
    let taglia = (a.f("taglia-gb", 0.5) * (1u64 << 30) as f64) as u64;
    let regime = a.f("regime", 2.0);
    let lavaggio = a.n("lavaggio", 1) == 1;
    let consolida = a.n("consolida", 0);
    let nseg = a.n("segmenti", 0) as u32;
    let ritmo = a.f("ritmo-mbs", 0.0) * (1u64 << 20) as f64;
    let out = a.s("out", "risultati/carichi.jsonl");
    let etichetta = a.s("etichetta", &dir);
    let mut rng = SmallRng::seed_from_u64(a.n("seme", 42));
    // circa 450 byte logici per documento, circa 50 documenti vivi per cliente in media
    let clienti = a.n("clienti", (taglia / 450 / 50).max(1000)) as usize;
    let zipf = Zipf::new(clienti, 0.7, &mut rng);
    let ch = Chiavi { disp, secchi: nseg.max(1) };
    let _ = std::fs::remove_dir_all(&dir);
    let conta = Arc::new(Conta::new(magazzino(a, &dir)?));
    let store: Arc<dyn ObjectStore> = conta.clone();
    let livelli_t = a.f("livelli", 0.0);
    let esterno = livelli_t > 0.0 && lavaggio;
    let comp = a.s("compressione", "nessuna");
    let blocco_kb = a.n("blocco-kb", 64);
    VERI.store(a.s("documenti", "veri") == "veri", Relaxed);
    let attesa_fine = a.n("attesa-fine", 1) == 1;
    // M31: taglia media dei documenti (circa 310 byte di serie, come Atlas P1; 2.000 per la prova da 2 KB)
    let scala_doc = a.f("doc-byte", 310.0) / 310.0;
    // M31: clienti dormienti (come P1 su Atlas): una quota dei clienti smette di scrivere a una frazione del carico
    let quota_dorm = a.f("dormienti", 0.0);
    let fine_dorm = a.f("fine-dormienti", 0.6);
    let dormiente: Vec<bool> = (0..clienti).map(|c| (hash32(c as u32 ^ 0x5eed) % 1000) < (quota_dorm * 1000.0) as u32).collect();
    let byte_previsti = taglia as f64 * (1.0 + regime);
    let mut b = Db::builder(DB_PATH, store.clone()).with_settings(settings(lavaggio, consolida, esterno, &comp)).with_filter_policies(politiche_filtri()).with_sst_block_size(blocco(blocco_kb));
    if esterno {
        // M29: il lavaggio a gradini gira dentro il processo del database, così le scatole riscritte usano il formato del
        // database (blocchi e compressione). Il compattatore separato di SlateDB 0.17 usa sempre il formato di serie.
        b = b.with_compactor_builder(
            slatedb::CompactorBuilder::new(DB_PATH, store.clone())
                .with_options(opzioni_lavaggio(0))
                .with_scheduler_supplier(Arc::new(livelli::Livelli { t: livelli_t, min_l0: 4, max_attivi: 4 }))
                .with_filter_policies(politiche_filtri()),
        );
    }
    if nseg > 0 {
        anyhow::ensure!(disp == Disp::Cliente, "i segmenti solo con la disposizione cliente");
        b = b.with_segment_extractor(Arc::new(Segmento));
    }
    let db = b.build().await?;
    let wo = WriteOptions::default();

    let mut fifo: VecDeque<Rec> = VecDeque::new();
    let mut base: u64 = 0; // n del primo elemento di fifo
    let mut vivi: Vec<[u32; 3]> = vec![[0; 3]; clienti];
    let mut ultimo: Vec<i64> = vec![0; clienti];
    let mut ordini: Vec<Vec<u64>> = vec![Vec::new(); clienti];
    let (mut live_bytes, mut user_bytes, mut tomb_bytes) = (0u64, 0u64, 0u64);
    let mut n: u64 = 0;
    let mut ts = T0_MS;
    let t_start = Instant::now();
    let cpu0 = cpu_s();
    let mut fase_regime: Option<(f64, f64, u64, u64, u64, u64, u64, u64)> = None; // cpu, s, user, put_sst, put_sst_b, get_sst, get_sst_b, tomb
    let mut campioni: Vec<(f64, usize)> = Vec::new();
    let (mut comp_l0, mut comp_sr) = (0.0f64, 0.0f64);
    let mut ultimo_campione = Instant::now();
    let mut ultimo_log = Instant::now();
    let fine_regime_byte = |u0: u64| u0 + (regime * taglia as f64) as u64;
    let mut obiettivo_regime: Option<u64> = None;

    loop {
        let mut batch = WriteBatch::new();
        for _ in 0..1000 {
            ts += PASSO_MS;
            let mut cust = zipf.sample(&mut rng);
            if quota_dorm > 0.0 && user_bytes as f64 >= fine_dorm * byte_previsti {
                while dormiente[cust as usize] {
                    cust = zipf.sample(&mut rng);
                }
            }
            let x: f64 = rng.gen();
            let (coll, upd) = if x < 0.50 {
                (MSGS, None)
            } else if x < 0.85 {
                (EVENTS, None)
            } else if x < 0.95 || ordini[cust as usize].is_empty() {
                (ORDERS, None)
            } else {
                let l = &ordini[cust as usize];
                (ORDERS, Some(l[rng.gen_range(0..l.len())]))
            };
            ultimo[cust as usize] = ts;
            if let Some(on) = upd {
                // aggiornamento dello stato di un ordine ancora vivo
                if on >= base {
                    let r = fifo[(on - base) as usize];
                    if r.live {
                        let id = Chiavi::id(on, r.ts);
                        let nuovo = Rec { status: (r.status + 1) % 5, ..r };
                        let v = valore(&nuovo, r.bytes as usize - 0, &mut rng);
                        let dk = ch.dato(&nuovo, &id);
                        let og = ch.globale(r.status, r.ts, &id);
                        let ng = ch.globale(nuovo.status, r.ts, &id);
                        user_bytes += (dk.len() + v.len() + ng.len()) as u64;
                        tomb_bytes += og.len() as u64;
                        batch.put(&dk, &v);
                        batch.delete(&og);
                        batch.put(&ng, b"");
                        fifo[(on - base) as usize].status = nuovo.status;
                        continue;
                    }
                }
            }
            let len = match coll {
                MSGS => rng.gen_range(150..450),
                EVENTS => rng.gen_range(120..280),
                _ => rng.gen_range(400..800),
            };
            let len = ((len as f64) * scala_doc) as usize;
            let r = Rec { coll, status: 0, live: true, cust, bytes: len as u32, ts };
            let id = Chiavi::id(n, ts);
            let v = valore(&r, len, &mut rng);
            let dk = ch.dato(&r, &id);
            let mut tot = dk.len() + v.len();
            batch.put(&dk, &v);
            match disp {
                Disp::Oggi => {
                    let ik = ch.voce_indice(&r, &id);
                    tot += ik.len();
                    batch.put(&ik, b"");
                }
                _ => {
                    let tk = ch.doc(coll, &id);
                    tot += tk.len() + dk.len();
                    batch.put(&tk, &dk);
                }
            }
            if coll == ORDERS {
                let g = ch.globale(0, ts, &id);
                tot += g.len();
                batch.put(&g, b"");
                let l = &mut ordini[cust as usize];
                l.push(n);
                if l.len() > 4 {
                    l.remove(0);
                }
            }
            user_bytes += tot as u64;
            live_bytes += tot as u64;
            vivi[cust as usize][coll as usize] += 1;
            fifo.push_back(Rec { bytes: tot as u32, ..r });
            n += 1;
            // ritenzione: oltre la taglia si cancellano i documenti più vecchi
            while live_bytes > taglia {
                let old = fifo.pop_front().unwrap();
                let on = base;
                base += 1;
                if !old.live {
                    continue;
                }
                let id = Chiavi::id(on, old.ts);
                let mut ks = vec![ch.dato(&old, &id)];
                match disp {
                    Disp::Oggi => ks.push(ch.voce_indice(&old, &id)),
                    _ => ks.push(ch.doc(old.coll, &id)),
                }
                if old.coll == ORDERS {
                    ks.push(ch.globale(old.status, old.ts, &id));
                }
                for k in ks {
                    tomb_bytes += k.len() as u64;
                    batch.delete(&k);
                }
                live_bytes -= old.bytes as u64;
                vivi[old.cust as usize][old.coll as usize] -= 1;
            }
        }
        db.write_with_options(batch, &wo).await?;
        if ritmo > 0.0 {
            // scrittore a ritmo fisso (byte dell'utente al secondo), come un database con un traffico dato
            let atteso = user_bytes as f64 / ritmo;
            let ora = t_start.elapsed().as_secs_f64();
            if atteso > ora {
                tokio::time::sleep(Duration::from_secs_f64(atteso - ora)).await;
            }
        }

        if fase_regime.is_none() && live_bytes + 4096 >= taglia {
            // inizio del regime
            fase_regime = Some((
                cpu_s(),
                t_start.elapsed().as_secs_f64(),
                user_bytes,
                conta.c.put_sst.load(Relaxed),
                conta.c.put_sst_byte.load(Relaxed),
                conta.c.get_sst.load(Relaxed),
                conta.c.get_sst_byte.load(Relaxed),
                tomb_bytes,
            ));
            obiettivo_regime = Some(fine_regime_byte(user_bytes));
            eprintln!("regime: inizio a {:.0} s, {} documenti", t_start.elapsed().as_secs_f64(), n);
        }
        if fase_regime.is_some() && ultimo_campione.elapsed() > Duration::from_millis(500) {
            ultimo_campione = Instant::now();
            let man = db.manifest();
            let (m, mx, _) = pile(&man);
            campioni.push((m, mx));
            let (a0, b0) = composizione(&man);
            comp_l0 += a0;
            comp_sr += b0;
        }
        if ultimo_log.elapsed() > Duration::from_secs(20) {
            ultimo_log = Instant::now();
            let (m, mx, k) = pile(&db.manifest());
            eprintln!(
                "{:.0} s: {} doc, vivi {} MB, utente {} MB, scatole scritte {} MB, pile {:.1} (max {}, alberi {})",
                t_start.elapsed().as_secs_f64(),
                n,
                live_bytes >> 20,
                user_bytes >> 20,
                conta.c.put_sst_byte.load(Relaxed) >> 20,
                m,
                mx,
                k
            );
        }
        if let Some(o) = obiettivo_regime {
            if user_bytes >= o {
                break;
            }
        }
    }
    db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await?;
    let t_fine_scritture = t_start.elapsed().as_secs_f64();
    // Il lavaggio finisce quello che ha in corso: il foglio indice non cambia per 20 s.
    // M29: con --attesa-fine 0 si ferma subito, così la fotografia per le GET ha le pile del regime.
    if lavaggio && attesa_fine {
        let mut last = String::new();
        let mut stable = Instant::now();
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let m = db.manifest();
            let (pm, px, _) = pile(&m);
            let sig = format!("{}:{}:{}:{}", m.l0().len(), m.compacted().len(), pm, px);
            if sig != last {
                last = sig;
                stable = Instant::now();
            } else if stable.elapsed() > Duration::from_secs(20) {
                break;
            }
        }
    }
    let cpu_fine = cpu_s();
    let m = db.manifest();
    let (pm, px, alberi) = pile(&m);
    db.close().await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let (pulite, pulite_b) = match if a.s("magazzino", "locale") == "locale" { pulisci(&dir).await } else { Ok((0, 0)) } {
        Ok(x) => x,
        Err(e) => {
            eprintln!("pulizia saltata: {e}");
            (0, 0)
        }
    };
    let r = fase_regime.context("regime mai iniziato")?;
    let reg_user = user_bytes - r.2;
    let campioni_media = campioni.iter().map(|c| c.0).sum::<f64>() / campioni.len().max(1) as f64;
    let campioni_max = campioni.iter().map(|c| c.1).max().unwrap_or(0);
    let mut cm: Vec<f64> = campioni.iter().map(|c| c.0).collect();
    cm.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p90 = cm.get(((cm.len() as f64 - 1.0) * 0.9) as usize).copied().unwrap_or(0.0);

    // verità per le query
    let mut v = Verita::default();
    for c in 0..clienti {
        let t = vivi[c];
        if t[0] >= 20 {
            v.clienti.push((c as u32, t[0], t[1], t[2], ultimo[c]));
        }
    }
    for _ in 0..3000 {
        let i = rng.gen_range(0..fifo.len());
        let r = fifo[i];
        if r.live {
            v.ids.push((base + i as u64, r.coll, r.cust, r.ts));
        }
    }
    v.ts_min = fifo.front().map(|r| r.ts).unwrap_or(T0_MS);
    v.ts_max = ts;
    v.m31 = verita_m31(&fifo, base, &vivi, &ultimo, &dormiente, &mut rng);
    std::fs::write(format!("{dir}/verita.json"), serde_json::to_vec(&v)?)?;

    let info = serde_json::json!({
        "etichetta": etichetta, "tipo": "carico", "compressione": comp, "blocco_kb": blocco_kb, "documenti_veri": VERI.load(Relaxed), "attesa_fine": attesa_fine, "disposizione": format!("{disp:?}"), "lavaggio": lavaggio, "consolida": consolida,
        "segmenti": nseg, "livelli": livelli_t, "ritmo_mbs": ritmo / (1u64 << 20) as f64, "taglia_byte": taglia, "clienti": clienti, "documenti": n, "vivi_byte": live_bytes,
        "utente_byte": user_bytes, "lapidi_byte": tomb_bytes, "secondi": t_start.elapsed().as_secs_f64(), "secondi_scritture": t_fine_scritture,
        "cpu_s": cpu_fine - cpu0,
        "riempimento": {"utente_byte": r.2, "put_sst": r.3, "put_sst_byte": r.4, "get_sst": r.5, "get_sst_byte": r.6, "cpu_s": r.0 - cpu0, "secondi": r.1},
        "regime": {
            "utente_byte": reg_user,
            "lapidi_byte": tomb_bytes - r.7,
            "put_sst": conta.c.put_sst.load(Relaxed) - r.3,
            "put_sst_byte": conta.c.put_sst_byte.load(Relaxed) - r.4,
            "get_sst": conta.c.get_sst.load(Relaxed) - r.5,
            "get_sst_byte": conta.c.get_sst_byte.load(Relaxed) - r.6,
            "cpu_s": cpu_fine - r.0,
            "secondi": t_start.elapsed().as_secs_f64() - r.1,
            "pile_media": campioni_media, "l0_media": comp_l0 / campioni.len().max(1) as f64, "sr_media": comp_sr / campioni.len().max(1) as f64, "pile_p90": p90, "pile_max": campioni_max, "campioni": campioni.len(),
        },
        "totale": {"put": conta.c.put.load(Relaxed), "put_byte": conta.c.put_byte.load(Relaxed), "put_sst": conta.c.put_sst.load(Relaxed), "put_sst_byte": conta.c.put_sst_byte.load(Relaxed), "get_sst": conta.c.get_sst.load(Relaxed), "get_sst_byte": conta.c.get_sst_byte.load(Relaxed)},
        "fine": {"pile_media": pm, "pile_max": px, "alberi": alberi, "l0_radice": m.l0().len(), "sr_radice": m.compacted().len()},
        "pulizia": {"file": pulite, "byte": pulite_b},
        "candidati": v.clienti.len(),
        "doc_byte": a.f("doc-byte", 310.0), "dormienti_quota": quota_dorm, "fine_dormienti": fine_dorm,
        "m31_clienti": v.m31.len(), "m31_dormienti": v.m31.iter().filter(|c| c.dormiente).count(),
    });
    std::fs::create_dir_all(std::path::Path::new(&out).parent().unwrap())?;
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&out)?;
    writeln!(f, "{info}")?;
    println!("{info}");
    Ok(())
}

/// Toglie le scatole che il foglio indice non usa più (rimaste dopo la chiusura).
async fn pulisci(dir: &str) -> Result<(u64, u64)> {
    let admin = slatedb::admin::Admin::builder(DB_PATH, store_locale(dir)?).build();
    let m = admin.read_manifest(None).await?.context("manifest")?;
    let mut vivi = HashSet::new();
    let mut add = |l0: &VecDeque<slatedb::manifest::SsTableView>, c: &Vec<slatedb::manifest::SortedRun>| {
        for v in l0 {
            vivi.insert(v.sst.id.value().to_string());
        }
        for sr in c {
            for v in sr.sst_views() {
                vivi.insert(v.sst.id.value().to_string());
            }
        }
    };
    add(m.l0(), m.compacted());
    for s in m.segments() {
        add(s.l0(), s.compacted());
    }
    let cdir = format!("{dir}/{DB_PATH}/compacted");
    let files: HashSet<String> = std::fs::read_dir(&cdir)?.filter_map(|e| e.ok()).map(|e| e.file_name().to_string_lossy().trim_end_matches(".sst").to_string()).collect();
    for v in &vivi {
        anyhow::ensure!(files.contains(v), "scatola viva {v} non trovata: pulizia annullata");
    }
    let (mut tolti, mut byte) = (0u64, 0u64);
    for e in std::fs::read_dir(&cdir)? {
        // un file può sparire tra la lista e la lettura (il raccoglitore lavora ancora): si salta
        let Ok(e) = e else { continue };
        let name = e.file_name().to_string_lossy().trim_end_matches(".sst").to_string();
        if !vivi.contains(&name) {
            let n = e.metadata().map(|m| m.len()).unwrap_or(0);
            if std::fs::remove_file(e.path()).is_ok() {
                byte += n;
                tolti += 1;
            }
        }
    }
    Ok((tolti, byte))
}

// ---------------------------------------------------------------- misura

struct Lettore {
    r: DbReader,
    /// read-ahead e letture in anticipo per pila: (find), (scaldare)
    ra_find: usize,
    ft_find: usize,
    ra_scalda: usize,
    ft_scalda: usize,
}

impl Lettore {
    async fn scan(&self, start: Vec<u8>, end: Vec<u8>, limit: usize) -> Result<Vec<(Bytes, Bytes)>> {
        let opts = ScanOptions { read_ahead_bytes: self.ra_find, max_fetch_tasks: self.ft_find, cache_blocks: true, order: slatedb::IterationOrder::Ascending, ..Default::default() };
        let mut it = match owner_prefix_len(&start) {
            Some(n) if end.len() >= n && end[..n] == start[..n] || end.len() < n => {
                let p = start[..n].to_vec();
                let hi = if end.starts_with(&p) { Bound::Excluded(end[n..].to_vec()) } else { Bound::Unbounded };
                self.r.scan_prefix_with_options(&p, (Bound::Included(start[n..].to_vec()), hi), &opts).await?
            }
            _ => self.r.scan_with_options((Bound::Included(start), Bound::Excluded(end)), &opts).await?,
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
    async fn get_many(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Bytes>>> {
        let futs: Vec<_> = keys.into_iter().map(|k| async move { self.r.get(k).await }).collect();
        Ok(futures::stream::iter(futs).buffered(32).try_collect().await?)
    }
}

fn fine_prefisso(p: &[u8]) -> Vec<u8> {
    let mut e = p.to_vec();
    while let Some(last) = e.pop() {
        if last != 0xFF {
            e.push(last + 1);
            return e;
        }
    }
    vec![0xFF; 8]
}

/// Documenti di un cliente in una collezione, dal più nuovo, tra `hi` e `lo` (date), al massimo `limit`.
async fn documenti(l: &Lettore, ch: &Chiavi, cust: u32, coll: u8, hi: i64, lo: i64, limit: usize) -> Result<Vec<Bytes>> {
    let p = ch.prefisso_cliente(cust, coll);
    let mut s = p.clone();
    s.extend(ts_desc(hi));
    let e = if lo == i64::MIN {
        fine_prefisso(&p)
    } else {
        let mut e = p.clone();
        e.extend(ts_desc(lo - 1));
        e
    };
    let rows = l.scan(s, e, limit).await?;
    match ch.disp {
        Disp::Oggi => {
            let keys: Vec<Vec<u8>> = rows.iter().map(|(k, _)| ch.doc(coll, &k[k.len() - 12..].try_into().unwrap())).collect();
            Ok(l.get_many(keys).await?.into_iter().flatten().collect())
        }
        _ => Ok(rows.into_iter().map(|(_, v)| v).collect()),
    }
}

fn verifica(docs: &[Bytes], cust: u32, coll: u8) -> Result<()> {
    for d in docs {
        anyhow::ensure!(d.len() >= 15 && d[0] == b'M' && val_cust(d) == cust && val_coll(d) == coll, "documento sbagliato per {cust}/{coll}");
    }
    Ok(())
}

#[derive(Default, Clone, serde::Serialize)]
struct Riga {
    q: String,
    get: u64,
    get_byte: u64,
    ms: f64,
    docs: usize,
    /// GET per pila (indice nella fotografia)
    per_pila: Vec<u64>,
}

async fn una(l: &Lettore, ch: &Chiavi, q: &str, c: (u32, u32, u32, u32, i64), id: (u64, u8, u32, i64), hi: i64, w: i64) -> Result<usize> {
    match q {
        "Q1" => {
            let d = documenti(l, ch, c.0, MSGS, i64::MAX, i64::MIN, 20).await?;
            verifica(&d, c.0, MSGS)?;
            anyhow::ensure!(d.len() == 20, "Q1 {} documenti", d.len());
            Ok(d.len())
        }
        "Q2" => {
            let k = ch.doc(id.1, &Chiavi::id(id.0, id.3));
            let x = l.r.get(&k).await?.context("Q2 mancante")?;
            let d = if ch.disp == Disp::Oggi { x } else { l.r.get(&x).await?.context("Q2 dato mancante")? };
            verifica(&[d.clone()], id.2, id.1)?;
            anyhow::ensure!(val_ts(&d) == id.3);
            Ok(1)
        }
        "Q3" => {
            let d = documenti(l, ch, c.0, EVENTS, hi, hi - w, 100).await?;
            verifica(&d, c.0, EVENTS)?;
            for x in &d {
                let t = val_ts(x);
                anyhow::ensure!(t <= hi && t >= hi - w, "Q3 fuori finestra");
            }
            Ok(d.len())
        }
        "SCALDA" => {
            let mut tot = 0;
            if ch.disp == Disp::Cliente {
                let p = ch.prefisso_tutto(c.0);
                let opts = ScanOptions { read_ahead_bytes: l.ra_scalda, max_fetch_tasks: l.ft_scalda, cache_blocks: true, order: slatedb::IterationOrder::Ascending, ..Default::default() };
                let mut rows = l.r.scan_prefix_with_options(&p, .., &opts).await?;
                let mut cnt = [0u32; 3];
                while let Some(kv) = rows.next().await? {
                    let d = kv.value;
                    anyhow::ensure!(val_cust(&d) == c.0);
                    cnt[val_coll(&d) as usize] += 1;
                    tot += 1;
                }
                anyhow::ensure!(cnt == [c.1, c.2, c.3], "SCALDA {:?} contro {:?}", cnt, (c.1, c.2, c.3));
            } else {
                for (coll, atteso) in [(MSGS, c.1), (EVENTS, c.2), (ORDERS, c.3)] {
                    let d = documenti(l, ch, c.0, coll, i64::MAX, i64::MIN, usize::MAX).await?;
                    verifica(&d, c.0, coll)?;
                    anyhow::ensure!(d.len() as u32 == atteso, "SCALDA coll {coll}: {} contro {atteso}", d.len());
                    tot += d.len();
                }
            }
            Ok(tot)
        }
        _ => unreachable!(),
    }
}

/// Una pila della fotografia: L0 (sempre sul bancone nel disegno) o sorted run, con la taglia in byte.
#[derive(Clone, serde::Serialize)]
struct Pila {
    l0: bool,
    byte: u64,
}

async fn misura(a: &Args) -> Result<()> {
    let disp = Disp::parse(&a.s("disposizione", "cliente"))?;
    let dir = a.s("dir", "data/x");
    let nseg = a.n("segmenti", 0) as u32;
    let nq = a.n("n", 200) as usize;
    let out = a.s("out", "risultati/misure.jsonl");
    let etichetta = a.s("etichetta", &dir);
    let ritardo_ms = a.n("ritardo-ms", 0);
    let taglia = (a.f("taglia-gb", 0.0) * (1u64 << 30) as f64) as u64;
    let comp = a.s("compressione", "nessuna");
    // configurazioni del lettore: "ra_find:ft_find:ra_scalda:ft_scalda,..." (0 = tutto il cliente)
    let configs: Vec<[usize; 4]> = a
        .s("lettori", "65536:4:65536:4,65536:1:16777216:1")
        .split(',')
        .map(|c| {
            let v: Vec<usize> = c.split(':').map(|x| x.parse().unwrap()).map(|x: usize| if x == 0 { 1 << 40 } else { x }).collect();
            [v[0], v[1], v[2], v[3]]
        })
        .collect();
    let ch = Chiavi { disp, secchi: nseg.max(1) };
    let v: Verita = serde_json::from_slice(&std::fs::read(format!("{dir}/verita.json"))?)?;
    let conta = Arc::new(Conta::new(store_locale(&dir)?));
    let store: Arc<dyn ObjectStore> = conta.clone();
    let banco = Arc::new(Bancone::new(true));
    let mut ro = slatedb::config::DbReaderOptions::default();
    ro.manifest_poll_interval = Duration::from_secs(3600);
    ro.checkpoint_lifetime = Duration::from_secs(4 * 3600);
    let _ = codec(&comp); // il lettore legge il codec dal piede di ogni scatola
    let mut rb = DbReader::builder(DB_PATH, store).with_options(ro).with_db_cache(banco.clone(), 1).with_filter_policies(politiche_filtri());
    if nseg > 0 {
        rb = rb.with_segment_extractor(Arc::new(Segmento));
    }
    let r = rb.build().await?;

    let admin = slatedb::admin::Admin::builder(DB_PATH, store_locale(&dir)?).build();
    let m = admin.read_manifest(None).await?.context("manifest")?;
    anyhow::ensure!(m.segments().is_empty(), "M29: segmenti non previsti");
    // fotografia: pile e scatole
    let mut pile_v: Vec<Pila> = Vec::new();
    let mut scatola_pila: HashMap<String, usize> = HashMap::new();
    for x in m.l0() {
        scatola_pila.insert(x.sst.id.value().to_string(), pile_v.len());
        pile_v.push(Pila { l0: true, byte: x.estimate_size() });
    }
    for sr in m.compacted() {
        for x in sr.sst_views() {
            scatola_pila.insert(x.sst.id.value().to_string(), pile_v.len());
        }
        pile_v.push(Pila { l0: false, byte: sr.estimate_size() });
    }
    let npile = pile_v.len();
    let byte_tot: u64 = pile_v.iter().map(|p| p.byte).sum();
    let taglia = if taglia > 0 { taglia } else { byte_tot };
    eprintln!("fotografia: {} pile ({} L0), {:?} MB", npile, pile_v.iter().filter(|p| p.l0).count(), pile_v.iter().map(|p| p.byte >> 20).collect::<Vec<_>>());

    let span = v.ts_max - v.ts_min;
    let cl = &v.clienti;
    let dormienti: Vec<_> = cl.iter().filter(|c| c.4 < v.ts_min + span / 2).cloned().collect();
    let w_q3 = (span / 3).max(1);
    let mut l = Lettore { r, ra_find: 65536, ft_find: 4, ra_scalda: 65536, ft_scalda: 4 };
    // riscaldamento di indici dei blocchi e filtri (bancone S1)
    let t = Instant::now();
    let mut wr = SmallRng::seed_from_u64(999);
    for i in 0..a.n("riscaldo", 1500) as usize {
        let c = cl[wr.gen_range(0..cl.len())];
        let id = v.ids[wr.gen_range(0..v.ids.len())];
        let q = ["Q1", "Q2", "Q3"][i % 3];
        let hi = wr.gen_range(v.ts_min + w_q3..=v.ts_max);
        una(&l, &ch, q, c, id, hi, w_q3).await?;
        banco.svuota_dati();
    }
    eprintln!("riscaldo {:.0} s: indici {} MB, filtri {} MB", t.elapsed().as_secs_f64(), banco.byte_tenuti(Tipo::Indice) >> 20, banco.byte_tenuti(Tipo::Filtro) >> 20);
    let byte_indici = banco.byte_tenuti(Tipo::Indice);
    let byte_filtri = banco.byte_tenuti(Tipo::Filtro);
    if ritardo_ms > 0 {
        *conta.finto.write().unwrap() = Some(conta::Finto { mediana_ms: ritardo_ms as f64, sigma: 0.0, banda_mbs: 1e9 });
    }

    // soglie del bancone delle pile recenti: nessuna, solo L0, e sorted run sotto una frazione della taglia
    let soglie: Vec<(String, Option<f64>)> = vec![("nessuno".into(), None), ("solo_l0".into(), Some(0.0)), ("1%".into(), Some(0.01)), ("2%".into(), Some(0.02)), ("5%".into(), Some(0.05))];
    let fissata = |p: &Pila, f: Option<f64>| -> bool {
        match f {
            None => false,
            Some(f) => p.l0 || (p.byte as f64) < f * taglia as f64,
        }
    };
    let mut bancone_soglie = serde_json::Map::new();
    for (nome, f) in &soglie {
        let fis: Vec<bool> = pile_v.iter().map(|p| fissata(p, *f)).collect();
        bancone_soglie.insert(nome.clone(), serde_json::json!({
            "pile_fissate": fis.iter().filter(|x| **x).count(),
            "byte_fissati": pile_v.iter().zip(&fis).filter(|(_, x)| **x).map(|(p, _)| p.byte).sum::<u64>(),
            "pile_su_s3": fis.iter().filter(|x| !**x).count(),
        }));
    }

    let qs = ["Q1", "Q2", "Q3", "SCALDA", "Q1-dormiente", "SCALDA-dormiente"];
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&out)?;
    for cfg in &configs {
        l.ra_find = cfg[0];
        l.ft_find = cfg[1];
        l.ra_scalda = cfg[2];
        l.ft_scalda = cfg[3];
        let mut rng = SmallRng::seed_from_u64(a.n("seme", 7));
        let mut righe: Vec<Riga> = Vec::new();
        for i in 0..nq * qs.len() {
            let qn = qs[i % qs.len()];
            let dorm = qn.ends_with("dormiente");
            if dorm && dormienti.is_empty() {
                continue;
            }
            let q = qn.trim_end_matches("-dormiente");
            let c = if dorm { dormienti[rng.gen_range(0..dormienti.len())] } else { cl[rng.gen_range(0..cl.len())] };
            let id = v.ids[rng.gen_range(0..v.ids.len())];
            let hi = rng.gen_range(v.ts_min + w_q3..=v.ts_max);
            banco.svuota_dati();
            conta.c.azzera();
            conta.per_scatola.lock().unwrap().clear();
            let t0 = Instant::now();
            let docs = una(&l, &ch, q, c, id, hi, w_q3).await?;
            let mut per_pila = vec![0u64; npile];
            for (nome, g) in conta.per_scatola.lock().unwrap().iter() {
                let k = *scatola_pila.get(nome).with_context(|| format!("scatola {nome} fuori dalla fotografia"))?;
                per_pila[k] += g;
            }
            righe.push(Riga { q: qn.into(), get: conta.c.get_totali(), get_byte: conta.c.get_byte.load(Relaxed), ms: t0.elapsed().as_secs_f64() * 1000.0, docs, per_pila });
        }
        let mut ris = serde_json::Map::new();
        for q in qs {
            let rs: Vec<&Riga> = righe.iter().filter(|r| r.q == q).collect();
            if rs.is_empty() {
                continue;
            }
            let mut g: Vec<u64> = rs.iter().map(|r| r.get).collect();
            g.sort_unstable();
            let k = rs.len() as f64;
            let mut ms: Vec<f64> = rs.iter().map(|r| r.ms).collect();
            ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
            // GET su S3 con il bancone delle pile recenti, per soglia
            let mut s3 = serde_json::Map::new();
            for (nome, fr) in &soglie {
                let fis: Vec<bool> = pile_v.iter().map(|p| fissata(p, *fr)).collect();
                let mut gs: Vec<u64> = rs.iter().map(|r| r.get - r.per_pila.iter().zip(&fis).filter(|(_, x)| **x).map(|(g, _)| *g).sum::<u64>()).collect();
                let media = gs.iter().sum::<u64>() as f64 / k;
                gs.sort_unstable();
                let su_s3 = fis.iter().filter(|x| !**x).count().max(1) as f64;
                s3.insert(nome.clone(), serde_json::json!({"get_media": media, "get_p90": gs[((gs.len() as f64 - 1.0) * 0.9) as usize], "get_per_pila_s3": media / su_s3}));
            }
            let pile_toccate = rs.iter().map(|r| r.per_pila.iter().filter(|x| **x > 0).count() as f64).sum::<f64>() / k;
            let mut gpp: Vec<f64> = rs.iter().map(|r| r.get as f64 / npile as f64).collect();
            gpp.sort_by(|a, b| a.partial_cmp(b).unwrap());
            ris.insert(q.into(), serde_json::json!({
                "n": rs.len(), "get_media": rs.iter().map(|r| r.get as f64).sum::<f64>() / k,
                "get_p50": g[g.len() / 2], "get_p90": g[((g.len() as f64 - 1.0) * 0.9) as usize], "get_max": g.last(),
                "get_per_pila": rs.iter().map(|r| r.get as f64).sum::<f64>() / k / npile as f64,
                "get_per_pila_p90": gpp[((gpp.len() as f64 - 1.0) * 0.9) as usize],
                "pile_toccate": pile_toccate,
                "byte_media": rs.iter().map(|r| r.get_byte as f64).sum::<f64>() / k, "docs_media": rs.iter().map(|r| r.docs as f64).sum::<f64>() / k,
                "ms_p50": ms[ms.len() / 2], "giri_p50": if ritardo_ms > 0 { ms[ms.len() / 2] / ritardo_ms as f64 } else { 0.0 },
                "s3_con_bancone": s3,
            }));
        }
        let g = |q: &str| ris.get(q).and_then(|x| x["get_media"].as_f64()).unwrap_or(f64::NAN);
        let tipica = 0.6 * g("Q1") + 0.2 * g("Q2") + 0.2 * g("Q3");
        let j = serde_json::json!({
            "etichetta": etichetta, "tipo": "misura", "lettore": {"ra_find": cfg[0], "ft_find": cfg[1], "ra_scalda": cfg[2], "ft_scalda": cfg[3]},
            "disposizione": format!("{disp:?}"), "compressione": comp, "ritardo_ms": ritardo_ms, "taglia_byte": taglia,
            "pile": npile, "pile_l0": pile_v.iter().filter(|p| p.l0).count(), "pile_byte": pile_v.iter().map(|p| p.byte).collect::<Vec<_>>(),
            "byte_scatole": byte_tot, "bancone_indici_byte": byte_indici, "bancone_filtri_byte": byte_filtri, "bancone_soglie": bancone_soglie,
            "candidati": cl.len(), "dormienti": dormienti.len(),
            "find_tipica": tipica, "query": ris,
        });
        writeln!(f, "{j}")?;
        println!("{j}");
    }
    Ok(())
}

// ---------------------------------------------------------------- M31: copia e tempi

/// Porta su un magazzino (`--magazzino s3`) tutti i file della cartella locale `--dir` (scatole, foglio indice, verità),
/// con lo stesso percorso relativo. Il database è chiuso: è una fotografia, come quella che M29 misura.
async fn copia(a: &Args) -> Result<()> {
    let dir = a.s("dir", "data/x");
    anyhow::ensure!(a.s("magazzino", "locale") != "locale", "copia: scegli --magazzino s3");
    let dst = magazzino(a, &dir)?;
    let mut file = Vec::new();
    let mut pila = vec![std::path::PathBuf::from(&dir)];
    while let Some(p) = pila.pop() {
        for e in std::fs::read_dir(&p)? {
            let e = e?;
            if e.file_type()?.is_dir() {
                pila.push(e.path());
            } else {
                file.push(e.path());
            }
        }
    }
    let t = Instant::now();
    let tot: u64 = file.iter().map(|f| std::fs::metadata(f).map(|m| m.len()).unwrap_or(0)).sum();
    let n = file.len();
    let dirp = std::path::PathBuf::from(&dir);
    futures::stream::iter(file.into_iter().map(|f| {
        let dst = dst.clone();
        let rel = f.strip_prefix(&dirp).unwrap().to_string_lossy().to_string();
        async move {
            let b = tokio::fs::read(&f).await?;
            let path = object_store::path::Path::from(rel.as_str());
            if b.len() > 64 << 20 {
                let mut up = object_store::WriteMultipart::new_with_chunk_size(dst.put_multipart(&path).await?, 32 << 20);
                up.write(&b);
                up.finish().await?;
            } else {
                dst.put(&path, Bytes::from(b).into()).await?;
            }
            anyhow::Ok(())
        }
    }))
    .buffer_unordered(16)
    .try_collect::<Vec<_>>()
    .await?;
    let j = serde_json::json!({"tipo": "copia", "dir": dir, "file": n, "byte": tot, "secondi": t.elapsed().as_secs_f64()});
    println!("{j}");
    Ok(())
}

fn pct(v: &[f64], p: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    // rango più vicino
    let k = ((p / 100.0) * s.len() as f64).ceil().max(1.0) as usize - 1;
    s[k.min(s.len() - 1)]
}

fn riassunto(v: &[f64]) -> serde_json::Value {
    let n = v.len().max(1) as f64;
    serde_json::json!({"n": v.len(), "p50": pct(v, 50.0), "p95": pct(v, 95.0), "p99": pct(v, 99.0), "max": pct(v, 100.0),
        "media": v.iter().sum::<f64>() / n,
        "quota_sotto_5ms": v.iter().filter(|x| **x < 5.0).count() as f64 / n, "quota_sopra_20ms": v.iter().filter(|x| **x > 20.0).count() as f64 / n})
}

/// Una find delle tre di Atlas P1 per un cliente M31. Restituisce i documenti letti (controllati).
async fn find_m31(l: &Lettore, ch: &Chiavi, q: &str, c: &ClienteM31, k: usize) -> Result<usize> {
    match q {
        "Q1" => {
            let d = documenti(l, ch, c.c, MSGS, i64::MAX, i64::MIN, 20).await?;
            verifica(&d, c.c, MSGS)?;
            anyhow::ensure!(d.len() == 20, "Q1 {} documenti", d.len());
            Ok(20)
        }
        "Q2" => {
            let (n, coll, ts) = c.q2;
            let x = l.r.get(&ch.doc(coll, &Chiavi::id(n, ts))).await?.context("Q2 biglietto mancante")?;
            let d = l.r.get(&x).await?.context("Q2 documento mancante")?;
            verifica(&[d.clone()], c.c, coll)?;
            anyhow::ensure!(val_ts(&d) == ts, "Q2 documento sbagliato");
            Ok(1)
        }
        "Q3" => {
            let (hi, lo) = c.q3[k % c.q3.len()];
            let d = documenti(l, ch, c.c, EVENTS, hi, lo, 200).await?;
            verifica(&d, c.c, EVENTS)?;
            for x in &d {
                let t = val_ts(x);
                anyhow::ensure!(t <= hi && t >= lo, "Q3 fuori finestra");
            }
            anyhow::ensure!(d.len() == EVENTI_Q3, "Q3 {} documenti invece di {EVENTI_Q3}", d.len());
            Ok(d.len())
        }
        "Q4" => {
            let (hi, lo) = c.q4[k % c.q4.len()];
            let d = documenti(l, ch, c.c, MSGS, hi, lo, 2 * MESSAGGI_Q4).await?;
            verifica(&d, c.c, MSGS)?;
            anyhow::ensure!(d.len() == MESSAGGI_Q4, "Q4 {} documenti invece di {MESSAGGI_Q4}", d.len());
            Ok(d.len())
        }
        _ => unreachable!(),
    }
}

/// I millisecondi a freddo e a caldo delle tre find, come su Atlas P1: freddo = cliente mai letto prima, con il bancone
/// che tiene solo indici dei blocchi e filtri (S1 di M21); caldo = la stessa find subito dopo, con i blocchi sul bancone.
/// Tempo misurato dentro il processo (niente rete del cliente). Varianti (`--varianti`, separate da virgola, ognuna
/// fatta di parole unite da `+`): `serie` (una GET alla volta: le pile lette una dopo l'altra) o `parallelo`
/// (SlateDB 0.17 com'è, che apre le pile insieme), `bancone` (pile recenti servite dalla copia vicina), `doppia`
/// (seconda GET al p95).
async fn tempi(a: &Args) -> Result<()> {
    let dir = a.s("dir", "data/x");
    let nq = a.n("n", 150) as usize;
    let out = a.s("out", "risultati/tempi.jsonl");
    let etichetta = a.s("etichetta", &dir);
    let quota_bancone = a.f("bancone-quota", 0.02);
    let ra_find = a.n("ra-find", 65536) as usize;
    let varianti: Vec<String> = a.s("varianti", "serie,parallelo,parallelo+bancone,parallelo+doppia,parallelo+bancone+doppia").split(',').map(|x| x.to_string()).collect();
    // ritardo finto per GET "mediana_ms:sigma:banda_mbs" (solo per le prove in locale; su S3 vero niente)
    let finto: Option<conta::Finto> = match a.s("finto", "").as_str() {
        "" => None,
        f => {
            let v: Vec<f64> = f.split(':').map(|x| x.parse().unwrap()).collect();
            Some(conta::Finto { mediana_ms: v[0], sigma: v[1], banda_mbs: v[2] })
        }
    };
    let ch = Chiavi { disp: Disp::Cliente, secchi: 1 };
    let v: Verita = serde_json::from_slice(&std::fs::read(format!("{dir}/verita.json"))?)?;
    anyhow::ensure!(!v.m31.is_empty(), "verita.json senza clienti M31: rifai il carico con m31");
    let lontano = magazzino(a, &dir)?;
    let conta = Arc::new(Conta::con_vicino(lontano.clone(), Some(store_locale(&dir)?)));
    let store: Arc<dyn ObjectStore> = conta.clone();
    let banco = Arc::new(Bancone::new(true));
    let mut ro = slatedb::config::DbReaderOptions::default();
    ro.manifest_poll_interval = Duration::from_secs(3600);
    ro.checkpoint_lifetime = Duration::from_secs(4 * 3600);
    let t_apri = Instant::now();
    let r = DbReader::builder(DB_PATH, store.clone()).with_options(ro).with_db_cache(banco.clone(), 1).with_filter_policies(politiche_filtri()).build().await?;
    let ms_apri = t_apri.elapsed().as_secs_f64() * 1000.0;

    // fotografia: pile e scatole (dal magazzino scelto)
    let admin = slatedb::admin::Admin::builder(DB_PATH, lontano.clone()).build();
    let m = admin.read_manifest(None).await?.context("manifest")?;
    anyhow::ensure!(m.segments().is_empty(), "M31: segmenti non previsti");
    let mut pile_v: Vec<Pila> = Vec::new();
    let mut scatole_pila: Vec<Vec<String>> = Vec::new();
    for x in m.l0() {
        pile_v.push(Pila { l0: true, byte: x.estimate_size() });
        scatole_pila.push(vec![x.sst.id.value().to_string()]);
    }
    for sr in m.compacted() {
        pile_v.push(Pila { l0: false, byte: sr.estimate_size() });
        scatole_pila.push(sr.sst_views().iter().map(|x| x.sst.id.value().to_string()).collect());
    }
    let byte_tot: u64 = pile_v.iter().map(|p| p.byte).sum();
    let fissata: Vec<bool> = pile_v.iter().map(|p| p.l0 || (p.byte as f64) < quota_bancone * byte_tot as f64).collect();
    let sul_bancone: HashSet<String> = scatole_pila.iter().zip(&fissata).filter(|(_, f)| **f).flat_map(|(s, _)| s.iter().cloned()).collect();
    eprintln!(
        "fotografia: {} pile ({} L0), MB {:?}; sul bancone con la quota {}: {} pile, {} MB",
        pile_v.len(),
        pile_v.iter().filter(|p| p.l0).count(),
        pile_v.iter().map(|p| p.byte >> 20).collect::<Vec<_>>(),
        quota_bancone,
        fissata.iter().filter(|x| **x).count(),
        pile_v.iter().zip(&fissata).filter(|(_, f)| **f).map(|(p, _)| p.byte).sum::<u64>() >> 20
    );

    let mut l = Lettore { r, ra_find, ft_find: 1, ra_scalda: 1 << 40, ft_scalda: 1 };
    // riscaldo di indici dei blocchi e filtri (bancone S1), su clienti che poi non si misurano
    let mut tutti: Vec<ClienteM31> = v.m31.clone();
    let mut rs = SmallRng::seed_from_u64(a.n("seme", 31));
    for i in (1..tutti.len()).rev() {
        let j = rs.gen_range(0..=i);
        tutti.swap(i, j);
    }
    let nris = (a.n("riscaldo", 400) as usize).min(tutti.len() / 4);
    let (riscaldo, misura_cl) = tutti.split_at(nris);
    let t = Instant::now();
    *conta.finto.write().unwrap() = None; // il riscaldo non serve a misurare
    let chr = &ch;
    let lr = &l;
    futures::stream::iter(riscaldo.iter().enumerate().map(|(i, c)| async move {
        for q in ["Q1", "Q2", "Q3"] {
            find_m31(lr, chr, q, c, i).await?;
        }
        anyhow::Ok(())
    }))
    .buffer_unordered(16)
    .try_collect::<Vec<_>>()
    .await?;
    banco.svuota_dati();
    let get_riscaldo = conta.c.get_totali();
    eprintln!("riscaldo {:.0} s, {} clienti, {} GET: indici {} KB, filtri {} KB", t.elapsed().as_secs_f64(), nris, get_riscaldo, banco.byte_tenuti(Tipo::Indice) >> 10, banco.byte_tenuti(Tipo::Filtro) >> 10);
    *conta.finto.write().unwrap() = finto;

    let dorm: Vec<&ClienteM31> = misura_cl.iter().filter(|c| c.dormiente).collect();
    let attivi: Vec<&ClienteM31> = misura_cl.iter().filter(|c| !c.dormiente).collect();
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&out)?;
    let mut usati: HashMap<u32, u32> = HashMap::new();
    let varianti: Vec<String> = if a.n("solo-q5", 0) == 1 { Vec::new() } else { varianti };
    for (vi, var) in varianti.iter().enumerate() {
        let parole: HashSet<&str> = var.split('+').collect();
        // "serie": una GET alla volta (le pile di una find lette una dopo l'altra); "parallelo": SlateDB 0.17 com'è
        *conta.semaforo.write().unwrap() = if parole.contains("serie") { Some(Arc::new(tokio::sync::Semaphore::new(1))) } else { None };
        *conta.sul_bancone.write().unwrap() = if parole.contains("bancone") { sul_bancone.clone() } else { HashSet::new() };
        // "ra256": lettura in anticipo da 256 KB per pila (una GET sola anche per le find con molti documenti)
        l.ra_find = if parole.contains("ra256") { 262144 } else { ra_find };
        conta.doppia_percentile.store(if parole.contains("doppia") { a.n("doppia-percentile", 95) } else { 0 }, Relaxed);
        for (gnome, gruppo) in [("dormienti", &dorm), ("attivi", &attivi)] {
            if gruppo.is_empty() {
                continue;
            }
            // clienti di questa variante: si gira la lista, ogni variante parte da un punto diverso
            let quanti = nq.min(gruppo.len());
            let inizio = (vi * quanti) % gruppo.len();
            let mut per_q: HashMap<&str, (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>)> = HashMap::new();
            let mut get_ms: Vec<f64> = Vec::new();
            let (mut doppie, mut doppie_vinte, mut get_tot, mut meta_mancate, mut max_volo, mut caldo_get) = (0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
            let t_var = Instant::now();
            for k in 0..quanti {
                let c = gruppo[(inizio + k) % gruppo.len()];
                *usati.entry(c.c).or_default() += 1;
                for q in ["Q1", "Q2", "Q3", "Q4"] {
                    if q == "Q4" && c.q4.is_empty() {
                        continue;
                    }
                    banco.svuota_dati();
                    banco.azzera_contatori();
                    conta.azzera();
                    let t0 = Instant::now();
                    let docs = find_m31(&l, &ch, q, c, vi + k).await?;
                    let freddo = t0.elapsed().as_secs_f64() * 1000.0;
                    let g = conta.c.get_totali();
                    let gb = conta.c.get_byte.load(Relaxed);
                    let gban = conta.c.get_bancone.load(Relaxed);
                    get_tot += g;
                    doppie += conta.c.doppie.load(Relaxed);
                    doppie_vinte += conta.c.doppie_vinte.load(Relaxed);
                    max_volo = max_volo.max(conta.c.max_in_volo.load(Relaxed));
                    meta_mancate += banco.mancate[Tipo::Indice as usize].load(Relaxed) + banco.mancate[Tipo::Filtro as usize].load(Relaxed);
                    get_ms.extend(conta.misurate.lock().unwrap().iter().map(|x| x.ms));
                    // caldo: la stessa find, subito, con i blocchi del cliente sul bancone
                    conta.azzera();
                    let t1 = Instant::now();
                    find_m31(&l, &ch, q, c, vi + k).await?;
                    let caldo = t1.elapsed().as_secs_f64() * 1000.0;
                    caldo_get += conta.c.get_totali();
                    let e = per_q.entry(q).or_default();
                    e.0.push(freddo);
                    e.1.push(caldo);
                    e.2.push(freddo - caldo);
                    e.3.push(g as f64);
                    e.4.push(if g > 0 { gb as f64 / g as f64 } else { 0.0 });
                    e.5.push(gban as f64);
                    e.6.push(docs as f64);
                }
            }
            let mut ris = serde_json::Map::new();
            for q in ["Q1", "Q2", "Q3", "Q4"] {
                let Some(e) = per_q.get(q) else { continue };
                let p50f = pct(&e.0, 50.0);
                let p50c = pct(&e.1, 50.0);
                ris.insert(q.into(), serde_json::json!({
                    "freddo_ms": riassunto(&e.0), "caldo_ms": riassunto(&e.1), "freddo_meno_caldo_ms": riassunto(&e.2),
                    "p50_freddo_meno_p50_caldo": p50f - p50c,
                    "get_per_find": riassunto(&e.3), "byte_per_get": riassunto(&e.4), "get_dal_bancone_per_find": riassunto(&e.5),
                    "documenti_media": e.6.iter().sum::<f64>() / e.6.len().max(1) as f64,
                }));
            }
            let finds = per_q.values().map(|e| e.0.len()).sum::<usize>() as f64;
            let j = serde_json::json!({
                "etichetta": etichetta, "tipo": "tempi", "magazzino": a.s("magazzino", "locale"), "finto": a.s("finto", ""),
                "variante": var, "gruppo": gnome, "clienti": quanti, "ra_find": ra_find, "doppia_percentile": a.n("doppia-percentile", 95),
                "pile": pile_v.len(), "pile_l0": pile_v.iter().filter(|p| p.l0).count(), "pile_mb": pile_v.iter().map(|p| p.byte as f64 / 1048576.0).collect::<Vec<_>>(),
                "pile_sul_bancone": if parole.contains("bancone") { fissata.iter().filter(|x| **x).count() } else { 0 },
                "query": ris,
                "get_ms": riassunto(&get_ms),
                "get_per_find_media": get_tot as f64 / finds, "doppie_per_find": doppie as f64 / finds, "doppie_vinte": doppie_vinte,
                "get_in_piu_doppia_pct": if get_tot > doppie { 100.0 * doppie as f64 / (get_tot - doppie) as f64 } else { 0.0 },
                "indici_filtri_mancati_per_find": meta_mancate as f64 / finds, "max_get_in_volo": max_volo, "get_a_caldo_totali": caldo_get,
                "secondi": t_var.elapsed().as_secs_f64(), "ms_apertura_lettore": ms_apri, "get_riscaldo": get_riscaldo,
            });
            eprintln!(
                "{var} {gnome} ({quanti}): Q1 freddo p50 {:.1} p99 {:.1}; Q2 {:.1} {:.1}; Q3 {:.1} {:.1}; caldo Q1 p50 {:.2}; GET per find {:.2}; doppie {:.3}",
                pct(&per_q["Q1"].0, 50.0), pct(&per_q["Q1"].0, 99.0), pct(&per_q["Q2"].0, 50.0), pct(&per_q["Q2"].0, 99.0),
                pct(&per_q["Q3"].0, 50.0), pct(&per_q["Q3"].0, 99.0), pct(&per_q["Q1"].1, 50.0), get_tot as f64 / finds, doppie as f64 / finds
            );
            writeln!(f, "{j}")?;
            println!("{j}");
        }
    }
    // letture fredde in parallelo: 10, 50, 100 clienti insieme (variante "parallelo", senza bancone né doppia)
    *conta.semaforo.write().unwrap() = None;
    l.ra_find = ra_find;
    conta.sul_bancone.write().unwrap().clear();
    conta.doppia_percentile.store(0, Relaxed);
    let gruppo_ins: &Vec<&ClienteM31> = if dorm.is_empty() { &attivi } else { &dorm };
    let insieme_k = if a.n("solo-q5", 0) == 1 { String::new() } else { a.s("insieme", "10,50,100") };
    for k in insieme_k.split(',').filter(|x| !x.is_empty()).map(|x| x.parse::<usize>().unwrap()) {
        for q in ["Q1", "Q2", "Q3"] {
            let mut ms = Vec::new();
            let mut giri = Vec::new();
            for giro in 0..a.n("giri-insieme", 3) as usize {
                let r = insieme(&l, &ch, &conta, &banco, gruppo_ins, k, q, giro + 7).await?;
                ms.extend(r["find_ms"].as_array().unwrap().iter().map(|x| x.as_f64().unwrap()));
                giri.push(serde_json::json!({"muro_s": r["muro_s"], "get": r["get"], "get_al_secondo": r["get_al_secondo"], "max_get_in_volo": r["max_get_in_volo"]}));
            }
            let j = serde_json::json!({"etichetta": etichetta, "tipo": "insieme", "magazzino": a.s("magazzino", "locale"), "finto": a.s("finto", ""), "k": k, "q": q, "find_ms": riassunto(&ms), "giri": giri});
            eprintln!("insieme {k} {q}: p50 {:.1} p99 {:.1}", pct(&ms, 50.0), pct(&ms, 99.0));
            writeln!(f, "{j}")?;
            println!("{j}");
        }
    }
    // Q5: ordini di tutti i clienti con uno stato negli ultimi 7 giorni (di tempo simulato), i 100 più nuovi
    let stato = a.n("q5-stato", 0) as u8;
    let sette = 7 * 86_400_000i64;
    let mut q5 = serde_json::Map::new();
    for (nome, quante) in [("indice_100", 100usize), ("indice_tutte", usize::MAX)] {
        let (mut fr, mut ca, mut gg, mut bb, mut voci) = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let giri = if quante == usize::MAX { a.n("q5-n-tutte", 3) } else { a.n("q5-n", 20) };
        for i in 0..giri {
            // finestre diverse a ogni giro (fredde), spostate di 7 giorni all'indietro
            let hi = v.ts_max - i as i64 * sette;
            let lo = hi - sette;
            banco.svuota_dati();
            conta.azzera();
            let t0 = Instant::now();
            let (nv, _) = q5_indice(&l, &ch, stato, hi, lo, quante).await?;
            fr.push(t0.elapsed().as_secs_f64() * 1000.0);
            gg.push(conta.c.get_totali() as f64);
            bb.push(conta.c.get_byte.load(Relaxed) as f64);
            voci.push(nv as f64);
            let t1 = Instant::now();
            q5_indice(&l, &ch, stato, hi, lo, quante).await?;
            ca.push(t1.elapsed().as_secs_f64() * 1000.0);
        }
        q5.insert(nome.into(), serde_json::json!({"freddo_ms": riassunto(&fr), "caldo_ms": riassunto(&ca), "get": riassunto(&gg), "byte": riassunto(&bb), "documenti": riassunto(&voci)}));
        eprintln!("Q5 {nome}: freddo p50 {:.1} ms p99 {:.1}, GET p50 {:.0}, documenti p50 {:.0}", pct(&fr, 50.0), pct(&fr, 99.0), pct(&gg, 50.0), pct(&voci, 50.0));
    }
    if a.n("q5-senza-indice", 1) == 1 {
        banco.svuota_dati();
        conta.azzera();
        let t0 = Instant::now();
        let n = q5_senza_indice(&l, stato, v.ts_max, v.ts_max - sette).await?;
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        q5.insert("senza_indice".into(), serde_json::json!({"ms": ms, "get": conta.c.get_totali(), "byte": conta.c.get_byte.load(Relaxed), "trovati": n, "byte_scatole": byte_tot}));
        eprintln!("Q5 senza indice: {:.0} ms, {} GET, {} MB, trovati {n}", ms, conta.c.get_totali(), conta.c.get_byte.load(Relaxed) >> 20);
    }
    let j = serde_json::json!({"etichetta": etichetta, "tipo": "q5", "magazzino": a.s("magazzino", "locale"), "finto": a.s("finto", ""), "stato": stato, "q5": q5});
    writeln!(f, "{j}")?;
    println!("{j}");
    let riusati = usati.values().filter(|x| **x > 1).count();
    let j = serde_json::json!({"etichetta": etichetta, "tipo": "tempi-clienti", "clienti_misurati": usati.len(), "clienti_in_piu_varianti": riusati, "clienti_riscaldo": nris});
    writeln!(f, "{j}")?;
    println!("{j}");
    Ok(())
}

/// Q5 (aggiunta del fondatore): gli ordini con uno stato negli ultimi 7 giorni, di tutti i clienti, i 100 più nuovi.
/// Il prototipo ha un solo indice "di tutti": le voci `G | stato | ts | _id` degli ordini (da M26), come l'indice
/// stato_ts di P1. Con l'indice: scansione delle voci (all'indietro), poi biglietto e documento per ognuno dei 100.
/// Restituisce (voci nella finestra lette, documenti).
async fn q5_indice(l: &Lettore, ch: &Chiavi, stato: u8, hi: i64, lo: i64, limit: usize) -> Result<(usize, usize)> {
    let mut a0 = vec![b'G', stato];
    a0.extend(ts_desc(hi));
    let mut b0 = vec![b'G', stato];
    b0.extend(ts_desc(lo - 1));
    let opts = ScanOptions { read_ahead_bytes: l.ra_find, max_fetch_tasks: 1, cache_blocks: true, order: slatedb::IterationOrder::Ascending, ..Default::default() };
    let mut it = l.r.scan_with_options((Bound::Included(a0), Bound::Excluded(b0)), &opts).await?;
    let mut ids = Vec::new();
    while let Some(kv) = it.next().await? {
        ids.push(kv.key[kv.key.len() - 12..].to_vec());
        if ids.len() >= limit {
            break;
        }
    }
    let biglietti: Vec<Vec<u8>> = ids.iter().map(|id| ch.doc(ORDERS, &id[..].try_into().unwrap())).collect();
    let dati: Vec<Vec<u8>> = l.get_many(biglietti).await?.into_iter().map(|x| x.map(|b| b.to_vec())).collect::<Option<Vec<_>>>().context("Q5 biglietto mancante")?;
    let docs = l.get_many(dati).await?;
    for d in &docs {
        let d = d.as_ref().context("Q5 documento mancante")?;
        anyhow::ensure!(val_coll(d) == ORDERS, "Q5 non è un ordine");
        let t = val_ts(d);
        anyhow::ensure!(t >= lo && t <= hi, "Q5 fuori finestra");
    }
    Ok((ids.len(), docs.len()))
}

/// Q5 senza indice: scansione di tutti i dati dei clienti (prefisso `K`), filtro su collezione, stato e date.
/// Così farebbe oggi un database senza l'indice. Letture grandi (16 MB) per non pagare troppe GET.
async fn q5_senza_indice(l: &Lettore, stato: u8, hi: i64, lo: i64) -> Result<usize> {
    let opts = ScanOptions { read_ahead_bytes: 16 << 20, max_fetch_tasks: 4, cache_blocks: false, order: slatedb::IterationOrder::Ascending, ..Default::default() };
    let mut it = l.r.scan_with_options((Bound::Included(vec![b'K']), Bound::Excluded(vec![b'L'])), &opts).await?;
    let mut n = 0;
    while let Some(kv) = it.next().await? {
        let d = &kv.value;
        if d.len() >= 15 && d[0] == b'M' && val_coll(d) == ORDERS && d[14] == stato {
            let t = val_ts(d);
            if t >= lo && t <= hi {
                n += 1;
            }
        }
    }
    Ok(n)
}

/// Letture fredde in parallelo (aggiunta del fondatore): k clienti freddi diversi partono insieme, ognuno una find.
async fn insieme(l: &Lettore, ch: &Chiavi, conta: &Conta, banco: &Bancone, gruppo: &[&ClienteM31], k: usize, q: &str, giro: usize) -> Result<serde_json::Value> {
    banco.svuota_dati();
    conta.azzera();
    let inizio = (giro * k) % gruppo.len().max(1);
    let scelti: Vec<&ClienteM31> = (0..k.min(gruppo.len())).map(|i| gruppo[(inizio + i) % gruppo.len()]).collect();
    let t0 = Instant::now();
    let ms: Vec<f64> = futures::future::try_join_all(scelti.iter().enumerate().map(|(i, c)| async move {
        let t = Instant::now();
        find_m31(l, ch, q, c, giro + i).await?;
        anyhow::Ok(t.elapsed().as_secs_f64() * 1000.0)
    }))
    .await?;
    let muro = t0.elapsed().as_secs_f64();
    let g = conta.c.get_totali();
    Ok(serde_json::json!({"k": scelti.len(), "q": q, "find_ms": ms, "muro_s": muro, "get": g, "get_al_secondo": g as f64 / muro, "max_get_in_volo": conta.c.max_in_volo.load(Relaxed)}))
}

/// Scritture come su Atlas P1 (aggiunta del fondatore): documenti da circa 1,9 KB, singole e a lotti, con il WAL di
/// SlateDB acceso (il prototipo non ha il quaderno: qui il "fino a S3" è il PUT del WAL). Misura la latenza di ogni
/// scrittura fino a quando è durevole su S3, i PUT e i byte scritti su S3 per byte inserito (WAL, scarico, lavaggio).
async fn scrivi(a: &Args) -> Result<()> {
    let dir = a.s("dir", "data/scrivi");
    let out = a.s("out", "risultati/scritture.jsonl");
    let etichetta = a.s("etichetta", &dir);
    let secondi = a.f("secondi", 60.0);
    let fili = a.n("fili", 32) as usize;
    let lotto = a.n("lotto", 1) as usize;
    let doc_byte = a.n("doc-byte", 1922) as usize;
    let flush_ms = a.n("flush-ms", 100);
    if a.s("magazzino", "locale") == "locale" {
        let _ = std::fs::remove_dir_all(&dir);
    }
    let conta = Arc::new(Conta::new(magazzino(a, &dir)?));
    if let Some(f) = a.0.get("finto") {
        let v: Vec<f64> = f.split(':').map(|x| x.parse().unwrap()).collect();
        *conta.finto.write().unwrap() = Some(conta::Finto { mediana_ms: v[0], sigma: v[1], banda_mbs: v[2] });
    }
    let store: Arc<dyn ObjectStore> = conta.clone();
    let mut st = settings(true, 0, true, "lz4");
    st.wal_enabled = true;
    st.flush_interval = Some(Duration::from_millis(flush_ms));
    let db = Db::builder(DB_PATH, store.clone())
        .with_settings(st)
        .with_filter_policies(politiche_filtri())
        .with_sst_block_size(blocco(64))
        .with_compactor_builder(
            slatedb::CompactorBuilder::new(DB_PATH, store.clone())
                .with_options(opzioni_lavaggio(0))
                .with_scheduler_supplier(Arc::new(livelli::Livelli { t: 4.0, min_l0: 4, max_attivi: 4 }))
                .with_filter_policies(politiche_filtri()),
        )
        .build()
        .await?;
    let db = Arc::new(db);
    let ch = Chiavi { disp: Disp::Cliente, secchi: 1 };
    let user = Arc::new(AtomicU64::new(0));
    let ndoc = Arc::new(AtomicU64::new(0));
    conta.c.azzera();
    let t0 = Instant::now();
    let fine = t0 + Duration::from_secs_f64(secondi);
    let lat: Vec<Vec<f64>> = futures::future::try_join_all((0..fili).map(|fi| {
        let db = db.clone();
        let ch = ch.clone();
        let user = user.clone();
        let ndoc = ndoc.clone();
        async move {
            let mut rng = SmallRng::seed_from_u64(1000 + fi as u64);
            let mut v = Vec::new();
            while Instant::now() < fine {
                let mut b = WriteBatch::new();
                for _ in 0..lotto {
                    let n = ndoc.fetch_add(1, Relaxed);
                    let ts = T0_MS + n as i64 * PASSO_MS;
                    let r = Rec { coll: MSGS, status: 0, live: true, cust: rng.gen_range(0..5000), bytes: doc_byte as u32, ts };
                    let id = Chiavi::id(n, ts);
                    let val = valore(&r, doc_byte, &mut rng);
                    let dk = ch.dato(&r, &id);
                    let tk = ch.doc(MSGS, &id);
                    // byte inseriti = byte dei documenti, come "byte inseriti" su Atlas P1 (chiavi e biglietti esclusi)
                    user.fetch_add(val.len() as u64, Relaxed);
                    b.put(&dk, &val);
                    b.put(&tk, &dk);
                }
                let t = Instant::now();
                let h = db.write_with_options(b, &WriteOptions::default()).await?;
                h.await_durable().await?;
                v.push(t.elapsed().as_secs_f64() * 1000.0);
            }
            anyhow::Ok(v)
        }
    }))
    .await?;
    let durata = t0.elapsed().as_secs_f64();
    let c_scrittura = (conta.c.put.load(Relaxed), conta.c.put_byte.load(Relaxed), conta.c.put_sst.load(Relaxed), conta.c.put_sst_byte.load(Relaxed));
    // poi il lavaggio finisce quello che ha in coda (fino a 120 s), per contare anche i suoi byte
    db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await?;
    tokio::time::sleep(Duration::from_secs(a.n("attesa-lavaggio", 60))).await;
    let tutte: Vec<f64> = lat.into_iter().flatten().collect();
    let u = user.load(Relaxed) as f64;
    let gb = u / (1u64 << 30) as f64;
    let (put, put_b, put_sst, put_sst_b) = (conta.c.put.load(Relaxed), conta.c.put_byte.load(Relaxed), conta.c.put_sst.load(Relaxed), conta.c.put_sst_byte.load(Relaxed));
    let j = serde_json::json!({
        "etichetta": etichetta, "tipo": "scritture", "magazzino": a.s("magazzino", "locale"), "finto": a.s("finto", ""),
        "fili": fili, "lotto": lotto, "doc_byte": doc_byte, "flush_ms": flush_ms, "secondi": durata,
        "documenti": ndoc.load(Relaxed), "documenti_al_secondo": ndoc.load(Relaxed) as f64 / durata, "mb_al_secondo": u / 1048576.0 / durata,
        "latenza_fino_a_s3_ms": riassunto(&tutte),
        "durante": {"put": c_scrittura.0, "put_byte": c_scrittura.1, "put_sst": c_scrittura.2, "put_sst_byte": c_scrittura.3,
            "put_per_gb": c_scrittura.0 as f64 / gb, "byte_scritti_per_byte_inserito": c_scrittura.1 as f64 / u},
        "con_il_lavaggio": {"put": put, "put_byte": put_b, "put_sst": put_sst, "put_sst_byte": put_sst_b, "put_wal": put - put_sst,
            "put_per_gb": put as f64 / gb, "byte_scritti_per_byte_inserito": put_b as f64 / u, "byte_wal_per_byte_inserito": (put_b - put_sst_b) as f64 / u},
        "utente_byte": u,
    });
    match Arc::try_unwrap(db) {
        Ok(d) => d.close().await?,
        Err(_) => {}
    }
    std::fs::create_dir_all(std::path::Path::new(&out).parent().unwrap())?;
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&out)?;
    writeln!(f, "{j}")?;
    println!("{j}");
    Ok(())
}

// ---------------------------------------------------------------- banco della compressione

/// Compressione e decompressione dei soli blocchi, sugli stessi documenti del carico, ordinati come in una scatola
/// (cliente in testa). MB/s per core su questa macchina.
fn banco_compressione(a: &Args) -> Result<()> {
    let mb = a.n("mb", 64) as usize;
    let mut rng = SmallRng::seed_from_u64(42);
    let clienti = 200u32;
    // righe chiave+valore come nel carico, raggruppate per cliente
    let mut righe: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let ch = Chiavi { disp: Disp::Cliente, secchi: 1 };
    let mut tot = 0usize;
    let mut n = 0u64;
    let mut ts = T0_MS;
    while tot < mb << 20 {
        ts += PASSO_MS;
        let cust = rng.gen_range(0..clienti);
        let x: f64 = rng.gen();
        let coll = if x < 0.5 { MSGS } else if x < 0.85 { EVENTS } else { ORDERS };
        let len = match coll { MSGS => rng.gen_range(150..450), EVENTS => rng.gen_range(120..280), _ => rng.gen_range(400..800) };
        let r = Rec { coll, status: 0, live: true, cust, bytes: len as u32, ts };
        let id = Chiavi::id(n, ts);
        let v = valore(&r, len, &mut rng);
        let k = ch.dato(&r, &id);
        tot += k.len() + v.len();
        righe.push((k, v));
        n += 1;
    }
    righe.sort();
    let mut ris = Vec::new();
    for blocco in [4096usize, 65536] {
        // blocchi: chiave (lunghezza + byte) e valore come in un blocco di SlateDB (senza la compressione dei prefissi)
        let mut blocchi: Vec<Vec<u8>> = Vec::new();
        let mut cur = Vec::new();
        for (k, v) in &righe {
            cur.extend((k.len() as u16).to_le_bytes());
            cur.extend(k);
            cur.extend((v.len() as u32).to_le_bytes());
            cur.extend(v);
            if cur.len() >= blocco {
                blocchi.push(std::mem::take(&mut cur));
            }
        }
        let crudi: usize = blocchi.iter().map(|b| b.len()).sum();
        for (nome, livello) in [("lz4", 0), ("zstd1", 1), ("zstd3", 3)] {
            let t = Instant::now();
            let c0 = cpu_s();
            let comp: Vec<Vec<u8>> = blocchi.iter().map(|b| if livello == 0 { lz4_flex::block::compress_prepend_size(b) } else { zstd::bulk::compress(b, livello).unwrap() }).collect();
            let cpu_c = cpu_s() - c0;
            let tc = t.elapsed().as_secs_f64();
            let compressi: usize = comp.iter().map(|b| b.len()).sum();
            let c1 = cpu_s();
            let mut chk = 0usize;
            for (b, o) in comp.iter().zip(&blocchi) {
                let d = if livello == 0 { lz4_flex::block::decompress_size_prepended(b).unwrap() } else { zstd::bulk::decompress(b, o.len() + 64).unwrap() };
                chk += d.len();
            }
            let cpu_d = cpu_s() - c1;
            anyhow::ensure!(chk == crudi);
            let j = serde_json::json!({"tipo": "banco-compressione", "blocco": blocco, "codec": nome, "mb_crudi": crudi as f64 / 1048576.0,
                "rapporto": crudi as f64 / compressi as f64, "comp_mbs_core": crudi as f64 / 1048576.0 / cpu_c.max(1e-9), "decomp_mbs_core": crudi as f64 / 1048576.0 / cpu_d.max(1e-9), "secondi_comp": tc});
            println!("{j}");
            ris.push(j);
        }
    }
    let out = a.s("out", "risultati/banco-compressione.jsonl");
    std::fs::create_dir_all(std::path::Path::new(&out).parent().unwrap())?;
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&out)?;
    for j in ris {
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
        "banco-compressione" => banco_compressione(&a),
        "copia" => copia(&a).await,
        "tempi" => tempi(&a).await,
        "scrivi" => scrivi(&a).await,
        _ => bail!("uso: carica|misura --disposizione oggi|collezione|cliente --taglia-gb X --consolida K --segmenti S --dir D"),
    }
}
