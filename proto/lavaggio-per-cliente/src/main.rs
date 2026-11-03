//! M26 · quanto costa il lavaggio che ordina i dati per cliente.
//!
//! `carica`: carico "misto" (chat, eventi, ordini con aggiornamenti) per clienti con attività zipf, prima riempimento
//! fino alla taglia, poi regime con ritenzione (i documenti più vecchi si cancellano). Conta i byte e le richieste del
//! lavaggio, la CPU del processo e le pile (sorted run + L0) durante il regime.
//! `misura`: find fredde (Q1, Q2, Q3) e "scaldare un cliente" con il bancone S1 di M21.

mod bancone;
mod conta;
mod livelli;

use anyhow::{bail, Context, Result};
use bancone::{Bancone, Tipo};
use bytes::Bytes;
use conta::Conta;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
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
use std::sync::atomic::Ordering::Relaxed;
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
        let mut k = vec![b'G', status];
        k.extend(ts_asc(ts));
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

fn valore(r: &Rec, len: usize, rng: &mut SmallRng) -> Vec<u8> {
    let mut v = Vec::with_capacity(len);
    v.push(b'M');
    v.extend(r.cust.to_be_bytes());
    v.extend(r.ts.to_be_bytes());
    v.push(r.coll);
    v.push(r.status);
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
    if consolida > 0 {
        co.scheduler_options.insert("sorted_run_consolidation_threshold".into(), consolida.to_string());
    }
    co
}

fn settings(lavaggio: bool, consolida: u64, esterno: bool) -> Settings {
    let mut s = Settings::default();
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
    let conta = Arc::new(Conta::new(store_locale(&dir)?));
    let store: Arc<dyn ObjectStore> = conta.clone();
    let livelli_t = a.f("livelli", 0.0);
    let esterno = livelli_t > 0.0;
    let mut b = Db::builder(DB_PATH, store.clone()).with_settings(settings(lavaggio, consolida, esterno)).with_filter_policies(politiche_filtri());
    if nseg > 0 {
        anyhow::ensure!(disp == Disp::Cliente, "i segmenti solo con la disposizione cliente");
        b = b.with_segment_extractor(Arc::new(Segmento));
    }
    let db = b.build().await?;
    let compattatore = if esterno {
        let c = Arc::new(
            slatedb::CompactorBuilder::new(DB_PATH, store.clone())
                .with_options(opzioni_lavaggio(0))
                .with_scheduler_supplier(Arc::new(livelli::Livelli { t: livelli_t, min_l0: 4, max_attivi: 4 }))
                .with_filter_policies(politiche_filtri())
                .build(),
        );
        let c2 = c.clone();
        tokio::spawn(async move {
            if let Err(e) = c2.run().await {
                eprintln!("compattatore: {e}");
            }
        });
        Some(c)
    } else {
        None
    };
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
    let mut ultimo_campione = Instant::now();
    let mut ultimo_log = Instant::now();
    let fine_regime_byte = |u0: u64| u0 + (regime * taglia as f64) as u64;
    let mut obiettivo_regime: Option<u64> = None;

    loop {
        let mut batch = WriteBatch::new();
        for _ in 0..1000 {
            ts += PASSO_MS;
            let cust = zipf.sample(&mut rng);
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
            let (m, mx, _) = pile(&db.manifest());
            campioni.push((m, mx));
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
    if lavaggio {
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
    if let Some(c) = &compattatore {
        c.stop().await?;
    }
    db.close().await?;
    let (pulite, pulite_b) = pulisci(&dir).await?;
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
    std::fs::write(format!("{dir}/verita.json"), serde_json::to_vec(&v)?)?;

    let info = serde_json::json!({
        "etichetta": etichetta, "tipo": "carico", "disposizione": format!("{disp:?}"), "lavaggio": lavaggio, "consolida": consolida,
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
            "pile_media": campioni_media, "pile_p90": p90, "pile_max": campioni_max, "campioni": campioni.len(),
        },
        "totale": {"put": conta.c.put.load(Relaxed), "put_byte": conta.c.put_byte.load(Relaxed), "put_sst": conta.c.put_sst.load(Relaxed), "put_sst_byte": conta.c.put_sst_byte.load(Relaxed), "get_sst": conta.c.get_sst.load(Relaxed), "get_sst_byte": conta.c.get_sst_byte.load(Relaxed)},
        "fine": {"pile_media": pm, "pile_max": px, "alberi": alberi, "l0_radice": m.l0().len(), "sr_radice": m.compacted().len()},
        "pulizia": {"file": pulite, "byte": pulite_b},
        "candidati": v.clienti.len(),
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
    let files: HashSet<String> = std::fs::read_dir(&cdir)?.map(|e| e.unwrap().file_name().to_string_lossy().trim_end_matches(".sst").to_string()).collect();
    for v in &vivi {
        anyhow::ensure!(files.contains(v), "scatola viva {v} non trovata: pulizia annullata");
    }
    let (mut tolti, mut byte) = (0u64, 0u64);
    for e in std::fs::read_dir(&cdir)? {
        let e = e?;
        let name = e.file_name().to_string_lossy().trim_end_matches(".sst").to_string();
        if !vivi.contains(&name) {
            byte += e.metadata()?.len();
            std::fs::remove_file(e.path())?;
            tolti += 1;
        }
    }
    Ok((tolti, byte))
}

// ---------------------------------------------------------------- misura

struct Lettore {
    r: DbReader,
}

const READ_AHEAD: usize = 64 << 10;

impl Lettore {
    async fn scan(&self, start: Vec<u8>, end: Vec<u8>, limit: usize) -> Result<Vec<(Bytes, Bytes)>> {
        let opts = ScanOptions { read_ahead_bytes: READ_AHEAD, max_fetch_tasks: 4, cache_blocks: true, order: slatedb::IterationOrder::Ascending, ..Default::default() };
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
}

async fn misura(a: &Args) -> Result<()> {
    let disp = Disp::parse(&a.s("disposizione", "cliente"))?;
    let dir = a.s("dir", "data/x");
    let nseg = a.n("segmenti", 0) as u32;
    let nq = a.n("n", 200) as usize;
    let out = a.s("out", "risultati/misure.jsonl");
    let etichetta = a.s("etichetta", &dir);
    let ritardo_ms = a.n("ritardo-ms", 0);
    let ch = Chiavi { disp, secchi: nseg.max(1) };
    let v: Verita = serde_json::from_slice(&std::fs::read(format!("{dir}/verita.json"))?)?;
    let conta = Arc::new(Conta::new(store_locale(&dir)?));
    let store: Arc<dyn ObjectStore> = conta.clone();
    let banco = Arc::new(Bancone::new(true));
    let mut ro = slatedb::config::DbReaderOptions::default();
    ro.manifest_poll_interval = Duration::from_secs(3600);
    ro.checkpoint_lifetime = Duration::from_secs(4 * 3600);
    let mut rb = DbReader::builder(DB_PATH, store).with_options(ro).with_db_cache(banco.clone(), 1).with_filter_policies(politiche_filtri());
    if nseg > 0 {
        rb = rb.with_segment_extractor(Arc::new(Segmento));
    }
    let l = Lettore { r: rb.build().await? };

    let admin = slatedb::admin::Admin::builder(DB_PATH, store_locale(&dir)?).build();
    let m = admin.read_manifest(None).await?.context("manifest")?;
    let (pm, px, alberi) = {
        let segs: Vec<usize> = m.segments().iter().filter(|s| s.prefix().first() == Some(&b'K')).map(|s| s.l0().len() + s.compacted().len()).collect();
        if segs.is_empty() {
            let n = m.l0().len() + m.compacted().len();
            (n as f64, n, 1)
        } else {
            (segs.iter().sum::<usize>() as f64 / segs.len() as f64, *segs.iter().max().unwrap(), segs.len())
        }
    };
    let span = v.ts_max - v.ts_min;
    let mut rng = SmallRng::seed_from_u64(a.n("seme", 7));
    let cl = &v.clienti;
    // dormienti: l'ultima scrittura è nella metà più vecchia della finestra viva
    let dormienti: Vec<_> = cl.iter().filter(|c| c.4 < v.ts_min + span / 2).cloned().collect();

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
                    let opts = ScanOptions { read_ahead_bytes: READ_AHEAD, max_fetch_tasks: 4, cache_blocks: true, order: slatedb::IterationOrder::Ascending, ..Default::default() };
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

    let w_q3 = (span / 3).max(1);
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

    conta.ritardo_us.store(ritardo_ms * 1000, Relaxed);
    let mut righe: Vec<Riga> = Vec::new();
    let qs = ["Q1", "Q2", "Q3", "SCALDA", "Q1-dormiente", "SCALDA-dormiente"];
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
        let t0 = Instant::now();
        let docs = una(&l, &ch, q, c, id, hi, w_q3).await?;
        righe.push(Riga { q: qn.into(), get: conta.c.get_totali(), get_byte: conta.c.get_byte.load(Relaxed), ms: t0.elapsed().as_secs_f64() * 1000.0, docs });
    }
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&out)?;
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
        ris.insert(q.into(), serde_json::json!({
            "n": rs.len(), "get_media": rs.iter().map(|r| r.get as f64).sum::<f64>() / k,
            "get_p50": g[g.len() / 2], "get_p90": g[((g.len() as f64 - 1.0) * 0.9) as usize], "get_max": g.last(),
            "byte_media": rs.iter().map(|r| r.get_byte as f64).sum::<f64>() / k, "docs_media": rs.iter().map(|r| r.docs as f64).sum::<f64>() / k,
            "ms_p50": ms[ms.len() / 2], "giri_p50": if ritardo_ms > 0 { ms[ms.len() / 2] / ritardo_ms as f64 } else { 0.0 },
        }));
    }
    let g = |q: &str| ris.get(q).and_then(|x| x["get_media"].as_f64()).unwrap_or(f64::NAN);
    let tipica = 0.6 * g("Q1") + 0.2 * g("Q2") + 0.2 * g("Q3");
    let j = serde_json::json!({
        "etichetta": etichetta, "tipo": "misura", "disposizione": format!("{disp:?}"), "segmenti": nseg, "ritardo_ms": ritardo_ms,
        "pile_media": pm, "pile_max": px, "alberi": alberi, "candidati": cl.len(), "dormienti": dormienti.len(),
        "find_tipica": tipica, "query": ris,
    });
    writeln!(f, "{j}")?;
    println!("{j}");
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let (cmd, a) = Args::parse();
    match cmd.as_str() {
        "carica" => carica(&a).await,
        "misura" => misura(&a).await,
        _ => bail!("uso: carica|misura --disposizione oggi|collezione|cliente --taglia-gb X --consolida K --segmenti S --dir D"),
    }
}
