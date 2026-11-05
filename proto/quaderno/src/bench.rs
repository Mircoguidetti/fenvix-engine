//! Generatori di carico e controlli dei banchi. Ogni funzione stampa un oggetto JSON per riga (risultati) su stdout.

use crate::client::{CallErr, Cluster, Outcome};
use crate::codec::*;
use crate::window::Window;
use bytes::Bytes;
use hdrhistogram::Histogram;
use parking_lot::Mutex;
use serde_json::json;
use std::collections::HashMap;
use std::io::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::*};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub fn db_id(n: u64) -> Db {
    let mut d = [0u8; 16];
    d[..2].copy_from_slice(b"db");
    d[8..].copy_from_slice(&n.to_be_bytes());
    d
}
pub fn txn_id() -> TxnId {
    rand::random()
}
fn hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 120_000_000, 3).unwrap()
}
pub fn unix_ms_f() -> f64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as f64 / 1000.0
}
fn hj(h: &Histogram<u64>) -> serde_json::Value {
    // microsecondi
    json!({"n": h.len(), "p50_us": h.value_at_quantile(0.5), "p90_us": h.value_at_quantile(0.9), "p99_us": h.value_at_quantile(0.99),
           "p999_us": h.value_at_quantile(0.999), "max_us": h.max()})
}
fn doc_key(s: &str) -> Bytes {
    Bytes::from(format!("doc/{s}"))
}

// ================================================================================================ banco 1a

/// Costo della validazione sul leader, isolato: validazione + codifica della voce + indicizzazione nella finestra
/// (+ potatura per restare a regime), su un thread solo.
pub fn valbench(records: u64, keys_per_rec: usize, dbs: u64, keyspace: u64, val: usize, window_recs: u64) {
    let mut win = Window::new(true, std::path::PathBuf::from("/nonesiste"));
    let mut pos: Pos = pos_make(now_ms(), 0);
    let value = Bytes::from(vec![7u8; val]);
    let mk = |i: u64| -> Vec<Write> {
        (0..keys_per_rec).map(|_| Write { key: doc_key(&format!("{:012}", rand::random::<u64>() % keyspace)), kind: Kind::Put, val: value.clone() }).collect::<Vec<_>>().into_iter().map(|w| { let _ = i; w }).collect()
    };
    let per_db = (window_recs / dbs).max(1);
    let mut index = 0u64;
    let mut step = |win: &mut Window, timed: bool, tv: &mut Duration, tall: &mut Duration, conflicts: &mut u64| {
        index += 1;
        pos += 1;
        let db = db_id(rand::random::<u64>() % dbs);
        let writes = mk(index);
        let t0 = Instant::now();
        let ok = win.validate(&db, pos.saturating_sub(64), &writes, true, &[]).is_ok();
        let t1 = Instant::now();
        if !ok {
            *conflicts += 1;
        }
        let e = std::sync::Arc::new(Entry::encode(1, index, pos, Body::Rec { txn: txn_id(), db, session: None, writes }));
        win.apply(&e, crate::window::Loc::default());
        // a regime: il database esce dalla finestra dal fondo (già su S3)
        win.set_flushed(&db, pos, &Bytes::new());
        let front = {
            let d = win.db(&db).unwrap();
            let d = d.read();
            if d.recs.len() as u64 > per_db { d.recs.front().map(|r| r.pos) } else { None }
        };
        if let Some(p) = front {
            win.trim_db(&db, p + 1, pos, Instant::now() + Duration::from_secs(1));
        }
        let t2 = Instant::now();
        if timed {
            *tv += t1 - t0;
            *tall += t2 - t0;
        }
    };
    let (mut tv, mut tall, mut c) = (Duration::ZERO, Duration::ZERO, 0u64);
    // riempimento della finestra (non misurato)
    for _ in 0..window_recs {
        step(&mut win, false, &mut tv, &mut tall, &mut c);
    }
    c = 0;
    let wall = Instant::now();
    for _ in 0..records {
        step(&mut win, true, &mut tv, &mut tall, &mut c);
    }
    let wall = wall.elapsed();
    let us_v = tv.as_secs_f64() * 1e6 / records as f64;
    let us_all = tall.as_secs_f64() * 1e6 / records as f64;
    println!(
        "{}",
        json!({"bench": "1a-validazione", "records": records, "keys_per_rec": keys_per_rec, "dbs": dbs, "keyspace": keyspace, "val_bytes": val,
               "window_versions": win.versions, "us_per_rec_validate": us_v, "us_per_rec_validate_encode_index_trim": us_all,
               "recs_per_s_per_core_validate_only": 1e6 / us_v, "recs_per_s_per_core_full": 1e6 / us_all,
               "wall_recs_per_s": records as f64 / wall.as_secs_f64(), "conflicts": c})
    );
}

// ================================================================================================ banco 1b

pub async fn commit_bench(cl: Arc<Cluster>, conc: usize, secs: u64, warm: u64, keys: usize, val: usize, dbs: u64) {
    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let h = Arc::new(Mutex::new(hist()));
    let ok = Arc::new(AtomicU64::new(0));
    let conf = Arc::new(AtomicU64::new(0));
    let unk = Arc::new(AtomicU64::new(0));
    let value = Bytes::from(vec![9u8; val]);
    let mut tasks = vec![];
    for _ in 0..conc {
        let (cl, stop, measuring, h, ok, conf, unk, value) = (cl.clone(), stop.clone(), measuring.clone(), h.clone(), ok.clone(), conf.clone(), unk.clone(), value.clone());
        tasks.push(tokio::spawn(async move {
            while !stop.load(Relaxed) {
                let db = db_id(rand::random::<u64>() % dbs);
                let writes = (0..keys).map(|_| Write { key: doc_key(&format!("{:016x}", rand::random::<u64>())), kind: Kind::Put, val: value.clone() }).collect();
                let c = CommitReq { txn: txn_id(), db, read_pos: cl.seen.load(Relaxed), mode: Mode::Validated, writes, reads: vec![], session: None };
                let t0 = Instant::now();
                let r = cl.commit(&c, Duration::from_secs(10)).await;
                let us = t0.elapsed().as_micros() as u64;
                if !measuring.load(Relaxed) {
                    continue;
                }
                match r {
                    Outcome::Committed { .. } => {
                        ok.fetch_add(1, Relaxed);
                        h.lock().record(us.max(1)).ok();
                    }
                    Outcome::Conflict(_) => {
                        conf.fetch_add(1, Relaxed);
                    }
                    Outcome::Unknown | Outcome::Rejected(_) => {
                        unk.fetch_add(1, Relaxed);
                    }
                }
            }
        }));
    }
    tokio::time::sleep(Duration::from_secs(warm)).await;
    measuring.store(true, Relaxed);
    let t0 = Instant::now();
    tokio::time::sleep(Duration::from_secs(secs)).await;
    measuring.store(false, Relaxed);
    let el = t0.elapsed().as_secs_f64();
    stop.store(true, Relaxed);
    for t in tasks {
        let _ = t.await;
    }
    let n = ok.load(Relaxed) as f64;
    println!(
        "{}",
        json!({"bench": "1b-commit", "conc": conc, "keys_per_rec": keys, "val_bytes": val, "dbs": dbs, "secs": el,
               "recs_per_s": n / el, "payload_bytes_per_s": n * (keys * val) as f64 / el, "latency": hj(&h.lock()),
               "conflicts": conf.load(Relaxed), "unknown": unk.load(Relaxed)})
    );
}

// ================================================================================================ banco 2 / 7

pub struct MixedCfg {
    pub secs: u64,
    pub small_dbs: u64,
    pub small_rate: f64,
    pub hot_rate: f64,
    pub reads: f64,
    pub ryw: f64,
    pub val: usize,
    pub hot_keys: u64,
    pub small_keys: u64,
    pub report_every: u64,
    pub read_lag_ms: u64,
    /// testimoni: scritture con chiave unica in un database a parte, per controllare a fine prova (sugli SST) che
    /// ogni scrittura confermata sia arrivata su S3
    pub witness_rate: f64,
    pub witness_out: String,
}

pub const WITNESS_DB: u64 = 777_777_777;

const HOT_DB: u64 = 999_999_999;

#[derive(Default)]
struct Iv {
    commit: Option<Histogram<u64>>,
    rwin: Option<Histogram<u64>>,
    rcold: Option<Histogram<u64>>,
    ryw: Option<Histogram<u64>>,
    /// le stesse misure partendo dall'invio vero (non dal tick pianificato)
    commit_s: Option<Histogram<u64>>,
    rwin_s: Option<Histogram<u64>>,
    rcold_s: Option<Histogram<u64>>,
}

const SHARDS: usize = 32;

struct MixedState {
    cl: Arc<Cluster>,
    // istogrammi a frammenti: un lock globale a 130.000 operazioni/s serializzava il generatore
    iv: Vec<Mutex<Iv>>,
    tot: Vec<Mutex<Iv>>,
    ok: AtomicU64,
    conflicts: AtomicU64,
    unknown: AtomicU64,
    reads_ok: AtomicU64,
    reads_ow: AtomicU64,
    reads_behind: AtomicU64,
    reads_err: AtomicU64,
    ryw_bad: AtomicU64,
    /// commit rifiutati dal freno fino alla scadenza; commit confermati dopo almeno un "ritenta" del freno
    rejected: AtomicU64,
    slowed: AtomicU64,
    witness: Mutex<Vec<(u64, Pos)>>,
    witness_unknown: AtomicU64,
    /// chiavi scritte di recente, impacchettate (db << 32 | numero del documento), senza lock
    recent: Vec<AtomicU64>,
    recent_i: AtomicU64,
    /// posizione "un po' indietro" per le letture non lineari, calcolata in background
    lag_at: AtomicU64,
    val: Bytes,
}

fn rec(hs: &[Mutex<Iv>], which: u8, us: u64) {
    let mut g = hs[rand::random::<usize>() % hs.len()].lock();
    let slot = match which {
        0 => &mut g.commit,
        1 => &mut g.rwin,
        2 => &mut g.rcold,
        3 => &mut g.ryw,
        10 => &mut g.commit_s,
        11 => &mut g.rwin_s,
        _ => &mut g.rcold_s,
    };
    slot.get_or_insert_with(hist).record(us.max(1)).ok();
}

fn merge(hs: &[Mutex<Iv>], reset: bool) -> Iv {
    let mut o = Iv::default();
    for h in hs {
        let mut g = h.lock();
        let x = if reset {
            std::mem::take(&mut *g)
        } else {
            Iv { commit: g.commit.clone(), rwin: g.rwin.clone(), rcold: g.rcold.clone(), ryw: g.ryw.clone(), commit_s: g.commit_s.clone(), rwin_s: g.rwin_s.clone(), rcold_s: g.rcold_s.clone() }
        };
        for (a, b) in [
            (&mut o.commit, x.commit),
            (&mut o.rwin, x.rwin),
            (&mut o.rcold, x.rcold),
            (&mut o.ryw, x.ryw),
            (&mut o.commit_s, x.commit_s),
            (&mut o.rwin_s, x.rwin_s),
            (&mut o.rcold_s, x.rcold_s),
        ] {
            if let Some(b) = b {
                a.get_or_insert_with(hist).add(&b).ok();
            }
        }
    }
    o
}

fn ivj(i: &Iv) -> serde_json::Value {
    let e = hist();
    json!({"commit": hj(i.commit.as_ref().unwrap_or(&e)), "read_window": hj(i.rwin.as_ref().unwrap_or(&e)),
           "read_other": hj(i.rcold.as_ref().unwrap_or(&e)), "ryw": hj(i.ryw.as_ref().unwrap_or(&e)),
           "commit_from_send": hj(i.commit_s.as_ref().unwrap_or(&e)), "read_window_from_send": hj(i.rwin_s.as_ref().unwrap_or(&e)),
           "read_other_from_send": hj(i.rcold_s.as_ref().unwrap_or(&e))})
}

/// Pianifica `rate` operazioni al secondo (ciclo aperto, tick da 1 ms); la latenza parte dall'istante pianificato.
fn open_loop<F, Fut>(rate: f64, stop: Arc<AtomicBool>, f: F) -> tokio::task::JoinHandle<()>
where
    F: Fn(Instant) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        if rate <= 0.0 {
            return;
        }
        let start = Instant::now();
        let mut issued = 0u64;
        let mut tick = tokio::time::interval(Duration::from_millis(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
        while !stop.load(Relaxed) {
            // arrivi a gruppi ogni ms: la latenza parte dall'istante in cui il tick doveva scattare
            let t = tick.tick().await.into_std();
            let due = (t.duration_since(start).as_secs_f64() * rate) as u64;
            while issued < due {
                issued += 1;
                tokio::spawn(f(t));
            }
        }
    })
}

/// Giro 6 (M16): `rate > 0` ciclo aperto come `open_loop`; `rate <= 0` ciclo chiuso con `workers` lavoratori, ognuno
/// manda il suo lotto e aspetta l'esito prima del successivo (come il generatore di mongod, `proto/mongobench`): la
/// latenza parte dall'invio.
fn drive<F, Fut>(rate: f64, workers: usize, stop: Arc<AtomicBool>, f: F) -> tokio::task::JoinHandle<()>
where
    F: Fn(Instant) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    if rate > 0.0 {
        return open_loop(rate, stop, f);
    }
    let f = Arc::new(f);
    tokio::spawn(async move {
        let mut hs = vec![];
        for _ in 0..workers {
            let (f, stop) = (f.clone(), stop.clone());
            hs.push(tokio::spawn(async move {
                while !stop.load(Relaxed) {
                    f(Instant::now()).await;
                }
            }));
        }
        for h in hs {
            let _ = h.await;
        }
    })
}

pub async fn mixed(cl: Arc<Cluster>, c: MixedCfg) {
    let st = Arc::new(MixedState {
        cl: cl.clone(),
        iv: (0..SHARDS).map(|_| Mutex::new(Iv::default())).collect(),
        tot: (0..SHARDS).map(|_| Mutex::new(Iv::default())).collect(),
        ok: AtomicU64::new(0),
        conflicts: AtomicU64::new(0),
        unknown: AtomicU64::new(0),
        reads_ok: AtomicU64::new(0),
        reads_ow: AtomicU64::new(0),
        reads_behind: AtomicU64::new(0),
        reads_err: AtomicU64::new(0),
        ryw_bad: AtomicU64::new(0),
        rejected: AtomicU64::new(0),
        slowed: AtomicU64::new(0),
        witness: Mutex::new(vec![]),
        witness_unknown: AtomicU64::new(0),
        recent: (0..1 << 16).map(|_| AtomicU64::new(u64::MAX)).collect(),
        recent_i: AtomicU64::new(0),
        lag_at: AtomicU64::new(0),
        val: Bytes::from(vec![5u8; c.val]),
    });
    let stop = Arc::new(AtomicBool::new(false));
    // posizione "un po' indietro" per le letture non lineari
    {
        let (st, stop) = (st.clone(), stop.clone());
        let lag = Duration::from_millis(c.read_lag_ms);
        tokio::spawn(async move {
            let mut q: std::collections::VecDeque<(Instant, Pos)> = Default::default();
            while !stop.load(Relaxed) {
                q.push_back((Instant::now(), st.cl.seen.load(Relaxed)));
                while q.len() > 1 && q[1].0.elapsed() >= lag {
                    q.pop_front();
                }
                if q[0].0.elapsed() >= lag {
                    st.lag_at.store(q[0].1, Relaxed);
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
    }
    let lagged = |st: &MixedState| -> Pos { st.lag_at.load(Relaxed) };
    let write = |st: Arc<MixedState>, db: u64, kn: u64, sched: Instant| async move {
        let key = doc_key(&kn.to_string());
        let c = CommitReq {
            txn: txn_id(),
            db: db_id(db),
            read_pos: st.cl.seen.load(Relaxed),
            mode: Mode::Validated,
            writes: vec![Write { key: key.clone(), kind: Kind::Put, val: st.val.clone() }],
            reads: vec![],
            session: None,
        };
        let t_send = Instant::now();
        match st.cl.commit(&c, Duration::from_secs(10)).await {
            Outcome::Committed { busy, .. } => {
                if busy > 0 {
                    st.slowed.fetch_add(1, Relaxed);
                }
                let us = sched.elapsed().as_micros() as u64;
                let us_s = t_send.elapsed().as_micros() as u64;
                st.ok.fetch_add(1, Relaxed);
                rec(&st.iv, 0, us);
                rec(&st.tot, 0, us);
                rec(&st.iv, 10, us_s);
                rec(&st.tot, 10, us_s);
                let i = st.recent_i.fetch_add(1, Relaxed) as usize & 0xffff;
                st.recent[i].store((db << 32) | kn, Relaxed);
            }
            Outcome::Conflict(_) => {
                st.conflicts.fetch_add(1, Relaxed);
            }
            Outcome::Unknown => {
                st.unknown.fetch_add(1, Relaxed);
            }
            Outcome::Rejected(_) => {
                st.rejected.fetch_add(1, Relaxed);
            }
        }
    };
    let mut hs = vec![];
    {
        let st2 = st.clone();
        let (n, k) = (c.small_dbs, c.small_keys);
        hs.push(open_loop(c.small_rate * c.small_dbs as f64, stop.clone(), move |sched| {
            let db = rand::random::<u64>() % n;
            write(st2.clone(), db, rand::random::<u64>() % k, sched)
        }));
    }
    {
        let st2 = st.clone();
        let k = c.hot_keys;
        hs.push(open_loop(c.hot_rate, stop.clone(), move |sched| {
            write(st2.clone(), HOT_DB, rand::random::<u64>() % k, sched)
        }));
    }
    {
        let st2 = st.clone();
        hs.push(open_loop(c.reads, stop.clone(), move |sched| {
            let st = st2.clone();
            let at = lagged(&st);
            async move {
                let window = rand::random::<bool>();
                let (db, key) = if window {
                    let i = rand::random::<usize>() & 0xffff;
                    let x = st.recent[i].load(Relaxed);
                    if x == u64::MAX {
                        return;
                    }
                    (x >> 32, doc_key(&(x & 0xffff_ffff).to_string()))
                } else {
                    (HOT_DB, doc_key(&format!("cold-{}", rand::random::<u64>())))
                };
                let r = rand::random::<usize>() % st.cl.n();
                let t_send = Instant::now();
                match st.cl.call(r, &Req::Get { db: db_id(db), key, at, wait_ms: 50, spec: false, dal: 0 }, Duration::from_secs(2)).await {
                    Ok(Resp::Get(g)) => {
                        let us = sched.elapsed().as_micros() as u64;
                        let us_s = t_send.elapsed().as_micros() as u64;
                        let ow = matches!(g, GetRes::OutOfWindow { .. });
                        if ow {
                            st.reads_ow.fetch_add(1, Relaxed);
                        }
                        st.reads_ok.fetch_add(1, Relaxed);
                        let w = if window { 1 } else { 2 };
                        rec(&st.iv, w, us);
                        rec(&st.tot, w, us);
                        rec(&st.iv, w + 10, us_s);
                        rec(&st.tot, w + 10, us_s);
                    }
                    Ok(Resp::Behind(_)) => {
                        st.reads_behind.fetch_add(1, Relaxed);
                    }
                    _ => {
                        st.reads_err.fetch_add(1, Relaxed);
                    }
                }
            }
        }));
    }
    {
        let st2 = st.clone();
        let k = c.hot_keys;
        hs.push(open_loop(c.ryw, stop.clone(), move |_sched| {
            let st = st2.clone();
            async move {
                let key = doc_key(&format!("ryw-{}", rand::random::<u64>() % k));
                let v = Bytes::copy_from_slice(&rand::random::<u64>().to_le_bytes());
                let cr = CommitReq { txn: txn_id(), db: db_id(HOT_DB), read_pos: 0, mode: Mode::Blind, writes: vec![Write { key: key.clone(), kind: Kind::Put, val: v.clone() }], reads: vec![], session: None };
                let Outcome::Committed { pos, .. } = st.cl.commit(&cr, Duration::from_secs(10)).await else { return };
                let t0 = Instant::now();
                // leggi la tua scrittura da un follower: get(S = C) con attesa (= attendi(C) + leggi)
                let l = st.cl.leader.load(Relaxed);
                let mut r = rand::random::<usize>() % st.cl.n();
                if r as i32 == l {
                    r = (r + 1) % st.cl.n();
                }
                match st.cl.call(r, &Req::Get { db: db_id(HOT_DB), key, at: pos, wait_ms: 1000, spec: false, dal: 0 }, Duration::from_secs(2)).await {
                    Ok(Resp::Get(GetRes::Value(p, got))) if p >= pos => {
                        let us = t0.elapsed().as_micros() as u64;
                        if p == pos && got != v {
                            st.ryw_bad.fetch_add(1, Relaxed);
                        }
                        rec(&st.iv, 3, us);
                        rec(&st.tot, 3, us);
                    }
                    _ => {
                        st.ryw_bad.fetch_add(1, Relaxed);
                    }
                }
            }
        }));
    }
    {
        let st2 = st.clone();
        let seq = Arc::new(AtomicU64::new(0));
        hs.push(open_loop(c.witness_rate, stop.clone(), move |_sched| {
            let st = st2.clone();
            let n = seq.fetch_add(1, Relaxed);
            async move {
                let c = CommitReq {
                    txn: txn_id(),
                    db: db_id(WITNESS_DB),
                    read_pos: 0,
                    mode: Mode::Blind,
                    writes: vec![Write { key: doc_key(&format!("w-{n}")), kind: Kind::Put, val: Bytes::copy_from_slice(&n.to_le_bytes()) }],
                    reads: vec![],
                    session: None,
                };
                match st.cl.commit(&c, Duration::from_secs(30)).await {
                    Outcome::Committed { pos, .. } => st.witness.lock().push((n, pos)),
                    Outcome::Rejected(_) => {}
                    _ => {
                        st.witness_unknown.fetch_add(1, Relaxed);
                    }
                }
            }
        }));
    }
    let t0 = Instant::now();
    let mut last = (0u64, 0u64, 0u64, 0u64);
    let mut lt = Instant::now();
    while t0.elapsed().as_secs() < c.secs {
        tokio::time::sleep(Duration::from_secs(c.report_every.min(c.secs))).await;
        let iv = merge(&st.iv, true);
        let el = lt.elapsed().as_secs_f64();
        lt = Instant::now();
        let now = (st.ok.load(Relaxed), st.reads_ok.load(Relaxed), st.conflicts.load(Relaxed), st.unknown.load(Relaxed));
        let mut reps = vec![];
        for r in 0..cl.n() {
            if let Some(s) = cl.status(r).await {
                reps.push(json!({"id": s.id, "role": s.role, "rss_kb": s.rss_kb, "resident_mb": s.resident_bytes as f64 / 1e6, "versions": s.versions,
                    "mem_mb": s.mem_used as f64 / 1e6, "mem_est_mb": s.mem_est as f64 / 1e6, "mem_cap_mb": s.mem_cap as f64 / 1e6, "disk_used_mb": s.disk_used as f64 / 1e6,
                    "spill_runs": s.spill_runs, "spill_disk_mb": s.spill_disk as f64 / 1e6, "spilled": s.spilled_recs, "oldest_s": s.oldest_unflushed_s,
                    "alarm": s.alarm, "brake": s.brake, "busy": s.busy, "delayed": s.delayed,
                    "keys": s.keys, "dbs": s.dbs, "unflushed": s.unflushed_recs, "evicted": s.evicted, "log_len": s.last_index + 1 - s.log_front,
                    "disk_mb": s.disk_bytes as f64 / 1e6, "flush_lag_s": if s.min_unflushed_pos > 0 { (now_ms() as f64 - pos_ms(s.min_unflushed_pos) as f64) / 1000.0 } else { 0.0 }}));
            }
        }
        println!(
            "{}",
            json!({"t": t0.elapsed().as_secs_f64(), "commits_per_s": (now.0 - last.0) as f64 / el, "reads_per_s": (now.1 - last.1) as f64 / el,
                   "conflicts": now.2 - last.2, "unknown": now.3 - last.3, "rejected": st.rejected.load(Relaxed), "slowed": st.slowed.load(Relaxed),
                   "diag": st.cl.diag.json(), "lat": ivj(&iv), "replicas": reps})
        );
        let _ = std::io::stdout().flush();
        last = now;
    }
    stop.store(true, Relaxed);
    for h in hs {
        let _ = h.await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let el = t0.elapsed().as_secs_f64();
    let tot = merge(&st.tot, false);
    println!(
        "{}",
        json!({"final": true, "secs": el, "commits_ok": st.ok.load(Relaxed), "commits_per_s": st.ok.load(Relaxed) as f64 / el,
               "conflicts": st.conflicts.load(Relaxed), "unknown": st.unknown.load(Relaxed), "reads_ok": st.reads_ok.load(Relaxed),
               "reads_per_s": st.reads_ok.load(Relaxed) as f64 / el, "reads_out_of_window": st.reads_ow.load(Relaxed),
               "reads_behind": st.reads_behind.load(Relaxed), "reads_err": st.reads_err.load(Relaxed), "ryw_bad": st.ryw_bad.load(Relaxed),
               "rejected": st.rejected.load(Relaxed), "slowed": st.slowed.load(Relaxed),
               "witness_ok": st.witness.lock().len(), "witness_unknown": st.witness_unknown.load(Relaxed),
               "diag": st.cl.diag.json(), "lat": ivj(&tot)})
    );
    if !c.witness_out.is_empty() {
        let mut f = std::io::BufWriter::new(std::fs::File::create(&c.witness_out).unwrap());
        for (n, p) in st.witness.lock().iter() {
            writeln!(f, "{n},{p}").unwrap();
        }
    }
}

// ================================================================================================ banco 3

pub async fn hot(cl: Arc<Cluster>, mode: &str, clients: usize, secs: u64, max_attempts: u32) {
    let db = db_id(HOT_DB - 1);
    let key = doc_key("hot");
    // documento a 0
    let init = CommitReq { txn: txn_id(), db, read_pos: 0, mode: Mode::Blind, writes: vec![Write { key: key.clone(), kind: Kind::Put, val: Bytes::copy_from_slice(&0i64.to_le_bytes()) }], reads: vec![], session: None };
    let Outcome::Committed { .. } = cl.commit(&init, Duration::from_secs(10)).await else { panic!("init") };
    let stop = Arc::new(AtomicBool::new(false));
    let h = Arc::new(Mutex::new(hist()));
    let ok = Arc::new(AtomicU64::new(0));
    let aborts = Arc::new(AtomicU64::new(0));
    let unknown = Arc::new(AtomicU64::new(0));
    let tries = Arc::new(AtomicU64::new(0));
    let mut ts = vec![];
    let mode = mode.to_string();
    for _ in 0..clients {
        let (cl, stop, h, ok, aborts, unknown, tries, key, mode) = (cl.clone(), stop.clone(), h.clone(), ok.clone(), aborts.clone(), unknown.clone(), tries.clone(), key.clone(), mode.clone());
        ts.push(tokio::spawn(async move {
            while !stop.load(Relaxed) {
                let t0 = Instant::now();
                let mut done = false;
                for _a in 0..max_attempts {
                    tries.fetch_add(1, Relaxed);
                    let c = if mode == "blind" {
                        CommitReq { txn: txn_id(), db, read_pos: 0, mode: Mode::Blind, writes: vec![Write { key: key.clone(), kind: Kind::Delta, val: Bytes::copy_from_slice(&1i64.to_le_bytes()) }], reads: vec![], session: None }
                    } else if mode == "esiste" {
                        // scelta di docs/protocollo.md: $inc su _id di un documento esistente = delta con dipendenza
                        // (_id, esiste), preparato alla posizione confermata più recente nota al nodo d'ingresso
                        CommitReq { txn: txn_id(), db, read_pos: cl.seen.load(Relaxed), mode: Mode::Blind, writes: vec![Write { key: key.clone(), kind: Kind::Delta, val: Bytes::copy_from_slice(&1i64.to_le_bytes()) }], reads: vec![(key.clone(), Dep::Esiste)], session: None }
                    } else {
                        // leggi-modifica-scrivi validato: dal leader, alla posizione confermata più recente vista ("validated")
                        // oppure speculativa sull'ultima versione accettata ("spec")
                        let spec = mode == "spec";
                        let at = cl.seen.load(Relaxed);
                        let r = cl.call_leader(&Req::Get { db, key: key.clone(), at, wait_ms: 100, spec, dal: 0 }, Duration::from_secs(5)).await;
                        let (vp, v) = match r {
                            Some(Resp::Get(GetRes::Value(p, b))) => (p, i64::from_le_bytes(b[..8].try_into().unwrap())),
                            _ => continue,
                        };
                        let read_pos = if spec { vp } else { at.max(vp) };
                        CommitReq { txn: txn_id(), db, read_pos, mode: Mode::Validated, writes: vec![Write { key: key.clone(), kind: Kind::Put, val: Bytes::copy_from_slice(&(v + 1).to_le_bytes()) }], reads: vec![], session: None }
                    };
                    match cl.commit(&c, Duration::from_secs(10)).await {
                        Outcome::Committed { .. } => {
                            done = true;
                            ok.fetch_add(1, Relaxed);
                            h.lock().record(t0.elapsed().as_micros() as u64).ok();
                            break;
                        }
                        Outcome::Conflict(_) => continue,
                        Outcome::Unknown | Outcome::Rejected(_) => {
                            unknown.fetch_add(1, Relaxed);
                            done = true;
                            break;
                        }
                    }
                }
                if !done {
                    aborts.fetch_add(1, Relaxed);
                }
            }
        }));
    }
    let t0 = Instant::now();
    tokio::time::sleep(Duration::from_secs(secs)).await;
    stop.store(true, Relaxed);
    for t in ts {
        let _ = t.await;
    }
    let el = t0.elapsed().as_secs_f64();
    // controllo: il contatore vale quanto gli aggiornamenti riusciti
    let p = cl.confirmed(Duration::from_secs(5)).await.unwrap_or(0);
    let fin = match cl.call_leader(&Req::Get { db, key: key.clone(), at: p, wait_ms: 1000, spec: false, dal: 0 }, Duration::from_secs(5)).await {
        Some(Resp::Get(GetRes::Value(_, b))) => i64::from_le_bytes(b[..8].try_into().unwrap()),
        _ => -1,
    };
    let n = ok.load(Relaxed);
    let ab = aborts.load(Relaxed);
    println!(
        "{}",
        json!({"bench": "3-chiave-calda", "mode": mode, "clients": clients, "secs": el, "max_attempts": max_attempts,
               "updates_ok": n, "updates_per_s": n as f64 / el, "latency": hj(&h.lock()), "aborts": ab,
               "abort_pct": 100.0 * ab as f64 / (n + ab).max(1) as f64, "attempts_total": tries.load(Relaxed), "unknown": unknown.load(Relaxed),
               "final_counter": fin, "counter_matches": fin == n as i64})
    );
}

// ================================================================================================ banco 4

/// Storico per il controllo di linearizzabilità. Righe CSV:
/// `w,key,val,t0_us,t1_us,status,pos` e `r,key,val,t0_us,t1_us,pos,replica,S,behind_attempts`.
pub async fn lin(cl: Arc<Cluster>, secs: u64, keys: u64, writers: usize, readers: usize, out: String, pause_us: u64) {
    let db = db_id(4242);
    let start = Instant::now();
    let f = Arc::new(Mutex::new(std::io::BufWriter::with_capacity(4 << 20, std::fs::File::create(&out).unwrap())));
    let stop = Arc::new(AtomicBool::new(false));
    let behind = Arc::new([AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)]);
    let served = Arc::new([AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)]);
    let cnt = Arc::new([AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)]); // w ok, w unknown, reads
    let mut ts = vec![];
    for w in 0..writers {
        let (cl, f, stop, cnt) = (cl.clone(), f.clone(), stop.clone(), cnt.clone());
        ts.push(tokio::spawn(async move {
            let mut seq = 0u64;
            while !stop.load(Relaxed) {
                seq += 1;
                let k = rand::random::<u64>() % keys;
                let val = ((w as u64 + 1) << 40) | seq;
                let c = CommitReq { txn: txn_id(), db, read_pos: 0, mode: Mode::Blind, writes: vec![Write { key: doc_key(&k.to_string()), kind: Kind::Put, val: Bytes::copy_from_slice(&val.to_le_bytes()) }], reads: vec![], session: None };
                let t0 = start.elapsed().as_micros();
                let r = cl.commit(&c, Duration::from_secs(30)).await;
                let t1 = start.elapsed().as_micros();
                let line = match r {
                    Outcome::Committed { pos, .. } => {
                        cnt[0].fetch_add(1, Relaxed);
                        format!("w,{k},{val},{t0},{t1},ok,{pos}\n")
                    }
                    _ => {
                        cnt[1].fetch_add(1, Relaxed);
                        format!("w,{k},{val},{t0},{t1},unknown,0\n")
                    }
                };
                f.lock().write_all(line.as_bytes()).unwrap();
                if pause_us > 0 {
                    tokio::time::sleep(Duration::from_micros(pause_us)).await;
                }
            }
        }));
    }
    for _ in 0..readers {
        let (cl, f, stop, behind, served, cnt) = (cl.clone(), f.clone(), stop.clone(), behind.clone(), served.clone(), cnt.clone());
        ts.push(tokio::spawn(async move {
            // una replica che ha risposto "indietro" si evita per 1 s (il nodo d'ingresso se lo ricorda)
            let mut avoid = [Instant::now(); 3];
            while !stop.load(Relaxed) {
                let k = rand::random::<u64>() % keys;
                let t0 = start.elapsed().as_micros();
                let Some(s) = cl.confirmed(Duration::from_secs(30)).await else { continue };
                let mut r = rand::random::<usize>() % 3;
                for _ in 0..3 {
                    if avoid[r] > Instant::now() {
                        r = (r + 1) % 3;
                    }
                }
                let mut nb = 0;
                let res = loop {
                    match cl.call(r, &Req::Get { db, key: doc_key(&k.to_string()), at: s, wait_ms: 20, spec: false, dal: 0 }, Duration::from_millis(1000)).await {
                        Ok(Resp::Get(g)) => break Some(g),
                        Ok(Resp::Behind(_)) | Err(CallErr::Timeout) | Err(CallErr::Closed) => {
                            behind[r].fetch_add(1, Relaxed);
                            avoid[r] = Instant::now() + Duration::from_secs(1);
                            nb += 1;
                            r = (r + 1) % 3;
                            if nb > 30 {
                                break None;
                            }
                        }
                        Ok(_) => break None,
                    }
                };
                let t1 = start.elapsed().as_micros();
                let Some(g) = res else { continue };
                served[r].fetch_add(1, Relaxed);
                cnt[2].fetch_add(1, Relaxed);
                let (val, pos) = match g {
                    GetRes::Value(p, b) => (u64::from_le_bytes(b[..8].try_into().unwrap()), p),
                    GetRes::Absent(p) => (0, p),
                    GetRes::OutOfWindow { .. } | GetRes::Delta { .. } => (u64::MAX, 0),
                };
                f.lock().write_all(format!("r,{k},{val},{t0},{t1},{pos},{r},{s},{nb}\n").as_bytes()).unwrap();
                if pause_us > 0 {
                    tokio::time::sleep(Duration::from_micros(pause_us)).await;
                }
            }
        }));
    }
    let mut last = [0u64; 3];
    for i in 0..secs {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if i % 10 == 9 {
            let now = [cnt[0].load(Relaxed), cnt[1].load(Relaxed), cnt[2].load(Relaxed)];
            eprintln!(
                "t={}s scritture/s {} ignote {} letture/s {} servite per replica {:?} rinvii per replica {:?}",
                i + 1,
                (now[0] - last[0]) / 10,
                now[1] - last[1],
                (now[2] - last[2]) / 10,
                served.iter().map(|x| x.load(Relaxed)).collect::<Vec<_>>(),
                behind.iter().map(|x| x.load(Relaxed)).collect::<Vec<_>>()
            );
            last = now;
        }
    }
    stop.store(true, Relaxed);
    for t in ts {
        let _ = t.await;
    }
    f.lock().flush().unwrap();
    println!(
        "{}",
        json!({"bench": "4-storico", "secs": secs, "writes_ok": cnt[0].load(Relaxed), "writes_unknown": cnt[1].load(Relaxed), "reads": cnt[2].load(Relaxed),
               "served_by_replica": served.iter().map(|x| x.load(Relaxed)).collect::<Vec<_>>(),
               "behind_by_replica": behind.iter().map(|x| x.load(Relaxed)).collect::<Vec<_>>(), "start_unix_ms": unix_ms_f() - start.elapsed().as_secs_f64() * 1000.0})
    );
}

/// Controllo diretto: nessuna lettura più vecchia della posizione confermata. Per ogni lettura iniziata a t0, ogni
/// scrittura confermata prima di t0 deve essere visibile (la versione letta ha posizione ≥ della sua); e il valore letto
/// deve essere quello scritto a quella posizione (o di una scrittura dall'esito ignoto), scritta prima della fine della
/// lettura. `iso` = (replica, da_us, a_us) per contare le letture servite dalla replica isolata.
pub fn check_lin(path: &str, iso: Option<(usize, u64, u64)>) {
    let s = std::fs::read_to_string(path).unwrap();
    struct W {
        val: u64,
        t0: u64,
        t1: u64,
        ok: bool,
        pos: u64,
    }
    struct Rd {
        key: u64,
        val: u64,
        t0: u64,
        t1: u64,
        pos: u64,
        rep: usize,
    }
    let mut ws: HashMap<u64, Vec<W>> = HashMap::new();
    let mut rs = vec![];
    for l in s.lines() {
        let p: Vec<&str> = l.split(',').collect();
        let n = |i: usize| p[i].parse::<u64>().unwrap();
        if p[0] == "w" {
            ws.entry(n(1)).or_default().push(W { val: n(2), t0: n(3), t1: n(4), ok: p[5] == "ok", pos: n(6) });
        } else {
            rs.push(Rd { key: n(1), val: n(2), t0: n(3), t1: n(4), pos: n(5), rep: n(6) as usize });
        }
    }
    // per chiave: scritture confermate ordinate per fine, con massimo prefisso di posizione
    let mut idx: HashMap<u64, (Vec<(u64, u64)>, HashMap<u64, (u64, u64, bool, u64)>)> = HashMap::new();
    for (k, v) in &ws {
        let mut ends: Vec<(u64, u64)> = v.iter().filter(|w| w.ok).map(|w| (w.t1, w.pos)).collect();
        ends.sort();
        let mut m = 0;
        for e in ends.iter_mut() {
            m = m.max(e.1);
            e.1 = m;
        }
        let byval = v.iter().map(|w| (w.val, (w.pos, w.t0, w.ok, w.t1))).collect();
        idx.insert(*k, (ends, byval));
    }
    let (mut stale, mut bogus, mut ow, mut iso_reads) = (0u64, 0u64, 0u64, 0u64);
    let mut examples = vec![];
    for r in &rs {
        if r.val == u64::MAX {
            ow += 1;
            continue;
        }
        if let Some((rep, a, b)) = iso {
            if r.rep == rep && r.t0 >= a && r.t1 <= b {
                iso_reads += 1;
            }
        }
        let (ends, byval) = match idx.get(&r.key) {
            Some(x) => (&x.0[..], Some(&x.1)),
            None => (&[][..], None),
        };
        // massima posizione confermata prima dell'inizio della lettura
        let n = ends.partition_point(|e| e.0 < r.t0);
        let must = if n > 0 { ends[n - 1].1 } else { 0 };
        if r.pos < must {
            stale += 1;
            if examples.len() < 5 {
                examples.push(format!("lettura chiave {} a t0={} ha visto pos {} ma {} era già confermata", r.key, r.t0, r.pos, must));
            }
        }
        if r.val != 0 {
            match byval.and_then(|m| m.get(&r.val)) {
                Some((p, wt0, ok, _)) => {
                    if (*ok && *p != r.pos) || *wt0 > r.t1 {
                        bogus += 1;
                    }
                }
                None => bogus += 1,
            }
        }
    }
    println!(
        "{}",
        json!({"check": "letture-fresche", "reads": rs.len(), "writes": ws.values().map(|v| v.len()).sum::<usize>(), "stale_reads": stale,
               "invented_or_future_reads": bogus, "out_of_window": ow, "reads_served_by_isolated_during_isolation": iso_reads, "examples": examples})
    );
}

// ================================================================================================ banco 5

/// Ogni database scrive una volta ogni `period_ms`; ogni scrittura viene rinviata con lo stesso id finché è confermata.
/// Alla fine: controlla il registro (ogni commit confermato presente una volta sola, alla stessa posizione) e rinvia un
/// campione di commit già confermati (stessa posizione attesa).
pub async fn failover(cl: Arc<Cluster>, secs: u64, dbs: u64, period_ms: u64, out: String) {
    let f = Arc::new(Mutex::new(std::io::BufWriter::new(std::fs::File::create(&out).unwrap())));
    let done: Arc<Mutex<Vec<(CommitReq, Pos, bool)>>> = Arc::new(Mutex::new(vec![]));
    let unknown = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let start = Instant::now();
    let mut hs = vec![];
    let per_task = 100u64;
    for t in 0..dbs.div_ceil(per_task) {
        let (cl, f, done, unknown, stop) = (cl.clone(), f.clone(), done.clone(), unknown.clone(), stop.clone());
        hs.push(tokio::spawn(async move {
            // sfasa i database nel periodo
            tokio::time::sleep(Duration::from_millis(rand::random::<u64>() % period_ms)).await;
            let mut tick = tokio::time::interval(Duration::from_millis(period_ms));
            let mut seq = 0u64;
            while !stop.load(Relaxed) && start.elapsed().as_secs() < secs {
                tick.tick().await;
                seq += 1;
                for d in t * per_task..((t + 1) * per_task).min(dbs) {
                    let (cl, f, done, unknown) = (cl.clone(), f.clone(), done.clone(), unknown.clone());
                    tokio::spawn(async move {
                        let c = CommitReq { txn: txn_id(), db: db_id(d), read_pos: 0, mode: Mode::Blind, writes: vec![Write { key: doc_key(&format!("{}", seq % 10)), kind: Kind::Put, val: Bytes::copy_from_slice(&seq.to_le_bytes()) }], reads: vec![], session: None };
                        let ts = unix_ms_f();
                        let r = cl.commit(&c, Duration::from_secs(60)).await;
                        let te = unix_ms_f();
                        match r {
                            Outcome::Committed { pos, attempts, ambiguous, .. } => {
                                f.lock().write_all(format!("{d},{ts:.3},{te:.3},{pos},{attempts},{}\n", ambiguous as u8).as_bytes()).unwrap();
                                done.lock().push((c, pos, ambiguous));
                            }
                            _ => {
                                unknown.fetch_add(1, Relaxed);
                                f.lock().write_all(format!("{d},{ts:.3},{te:.3},0,0,9\n").as_bytes()).unwrap();
                            }
                        }
                    });
                }
            }
        }));
    }
    let mut last = 0usize;
    while start.elapsed().as_secs() < secs {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let n = done.lock().len();
        eprintln!("t={:.0}s confermati {} (+{}/s) leader {}", start.elapsed().as_secs_f64(), n, (n - last) / 5, cl.leader.load(Relaxed));
        last = n;
    }
    stop.store(true, Relaxed);
    for h in hs {
        let _ = h.await;
    }
    tokio::time::sleep(Duration::from_secs(65)).await; // tutti i commit in volo finiscono (scadenza 60 s)
    f.lock().flush().unwrap();
    let done = done.lock().clone();
    // 1) registro confermato completo, da ogni replica
    let mut per_rep = vec![];
    for r in 0..cl.n() {
        let mut m: HashMap<TxnId, (Pos, u32)> = HashMap::new();
        let mut from = 0u64;
        let mut ok = true;
        loop {
            match cl.call(r, &Req::Dump { from_index: from, max: 200_000 }, Duration::from_secs(30)).await {
                Ok(Resp::Dump(v)) => {
                    if v.is_empty() {
                        break;
                    }
                    for (i, t, p) in &v {
                        let e = m.entry(*t).or_insert((*p, 0));
                        e.1 += 1;
                        from = i + 1;
                    }
                }
                _ => {
                    ok = false;
                    break;
                }
            }
        }
        if !ok {
            per_rep.push(json!({"replica": r, "error": "dump non riuscito"}));
            continue;
        }
        let dup = m.values().filter(|x| x.1 > 1).count();
        let (mut lost, mut wrongpos) = (0, 0);
        for (c, p, _) in &done {
            match m.get(&c.txn) {
                None => lost += 1,
                Some((q, _)) if q != p => wrongpos += 1,
                _ => {}
            }
        }
        per_rep.push(json!({"replica": r, "log_records": m.len(), "duplicated_txn_ids": dup, "confirmed_missing": lost, "confirmed_wrong_pos": wrongpos}));
    }
    // 2) rinvio di commit già confermati (anche quelli ambigui): stessa posizione
    let mut sample: Vec<&(CommitReq, Pos, bool)> = done.iter().filter(|x| x.2).collect();
    let amb = sample.len();
    for x in done.iter().step_by((done.len() / 2000).max(1)) {
        sample.push(x);
    }
    let (mut same, mut diff) = (0, 0);
    for (c, p, _) in sample.iter() {
        match cl.commit(c, Duration::from_secs(10)).await {
            Outcome::Committed { pos, .. } if pos == *p => same += 1,
            _ => diff += 1,
        }
    }
    println!(
        "{}",
        json!({"bench": "5-guasto-leader", "confirmed": done.len(), "unknown": unknown.load(Relaxed), "ambiguous_then_confirmed": amb,
               "resend_same_pos": same, "resend_different": diff, "replicas": per_rep})
    );
}

// ================================================================================================ prova "esiste" in gara

/// `$inc` con la modalità "delta + dipendenza esiste" mentre un altro client cancella e ricrea i documenti.
/// Storico CSV: `op,chiave,valore,t0_us,t1_us,esito` con op ∈ put|del|inc|read ed esito ∈ ok|nomatch|absent|unknown.
/// `senza_esiste` = controllo negativo: lo stesso `$inc` senza la dipendenza (deve essere bocciato dal controllo).
pub async fn esiste_race(cl: Arc<Cluster>, secs: u64, keys: u64, incs: usize, readers: usize, out: String, senza_esiste: bool) {
    let db = db_id(5151);
    let start = Instant::now();
    let f = Arc::new(Mutex::new(std::io::BufWriter::new(std::fs::File::create(&out).unwrap())));
    let us = move || start.elapsed().as_micros() as u64;
    let put0 = move |k: u64| CommitReq { txn: txn_id(), db, read_pos: 0, mode: Mode::Blind, writes: vec![Write { key: doc_key(&format!("e{k}")), kind: Kind::Put, val: Bytes::copy_from_slice(&0i64.to_le_bytes()) }], reads: vec![], session: None };
    for k in 0..keys {
        let t0 = us();
        let r = cl.commit(&put0(k), Duration::from_secs(10)).await;
        let st = if matches!(r, Outcome::Committed { .. }) { "ok" } else { "unknown" };
        writeln!(f.lock(), "put,{k},0,{t0},{},{st}", us()).unwrap();
    }
    let stop = Arc::new(AtomicBool::new(false));
    let cnt = Arc::new([AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)]); // inc ok, nomatch, conflitti, del, put
    let mut ts = vec![];
    // chi cancella e ricrea
    {
        let (cl, f, stop, cnt) = (cl.clone(), f.clone(), stop.clone(), cnt.clone());
        ts.push(tokio::spawn(async move {
            while !stop.load(Relaxed) {
                let k = rand::random::<u64>() % keys;
                let key = doc_key(&format!("e{k}"));
                let t0 = us();
                let c = CommitReq { txn: txn_id(), db, read_pos: 0, mode: Mode::Blind, writes: vec![Write { key: key.clone(), kind: Kind::Del, val: Bytes::new() }], reads: vec![], session: None };
                let r = cl.commit(&c, Duration::from_secs(10)).await;
                let st = if matches!(r, Outcome::Committed { .. }) { "ok" } else { "unknown" };
                writeln!(f.lock(), "del,{k},0,{t0},{},{st}", us()).unwrap();
                cnt[3].fetch_add(1, Relaxed);
                tokio::time::sleep(Duration::from_millis(5 + rand::random::<u64>() % 20)).await;
                let t0 = us();
                let r = cl.commit(&put0(k), Duration::from_secs(10)).await;
                let st = if matches!(r, Outcome::Committed { .. }) { "ok" } else { "unknown" };
                writeln!(f.lock(), "put,{k},0,{t0},{},{st}", us()).unwrap();
                cnt[4].fetch_add(1, Relaxed);
                tokio::time::sleep(Duration::from_millis(5 + rand::random::<u64>() % 20)).await;
            }
        }));
    }
    // $inc: leggi a S (il documento esiste?), poi delta con dipendenza (chiave, esiste) preparato a S
    for _ in 0..incs {
        let (cl, f, stop, cnt) = (cl.clone(), f.clone(), stop.clone(), cnt.clone());
        ts.push(tokio::spawn(async move {
            while !stop.load(Relaxed) {
                let k = rand::random::<u64>() % keys;
                let key = doc_key(&format!("e{k}"));
                let t0 = us();
                let st = loop {
                    let s = cl.seen.load(Relaxed);
                    let r = rand::random::<usize>() % cl.n();
                    let g = match cl.call(r, &Req::Get { db, key: key.clone(), at: s, wait_ms: 100, spec: false, dal: 0 }, Duration::from_secs(2)).await {
                        Ok(Resp::Get(g)) => g,
                        _ => continue,
                    };
                    match g {
                        GetRes::Value(..) => {}
                        GetRes::Absent(_) => break "nomatch",
                        _ => continue,
                    }
                    let c = CommitReq { txn: txn_id(), db, read_pos: s, mode: Mode::Blind, writes: vec![Write { key: key.clone(), kind: Kind::Delta, val: Bytes::copy_from_slice(&1i64.to_le_bytes()) }], reads: if senza_esiste { vec![] } else { vec![(key.clone(), Dep::Esiste)] }, session: None };
                    match cl.commit(&c, Duration::from_secs(10)).await {
                        Outcome::Committed { .. } => break "ok",
                        Outcome::Conflict(_) => {
                            cnt[2].fetch_add(1, Relaxed);
                            continue;
                        }
                        _ => break "unknown",
                    }
                };
                writeln!(f.lock(), "inc,{k},1,{t0},{},{st}", us()).unwrap();
                cnt[if st == "nomatch" { 1 } else { 0 }].fetch_add(1, Relaxed);
            }
        }));
    }
    // letture lineari
    for _ in 0..readers {
        let (cl, f, stop) = (cl.clone(), f.clone(), stop.clone());
        ts.push(tokio::spawn(async move {
            while !stop.load(Relaxed) {
                let k = rand::random::<u64>() % keys;
                let t0 = us();
                let Some(s) = cl.confirmed(Duration::from_secs(10)).await else { continue };
                let r = rand::random::<usize>() % cl.n();
                let line = match cl.call(r, &Req::Get { db, key: doc_key(&format!("e{k}")), at: s, wait_ms: 100, spec: false, dal: 0 }, Duration::from_secs(2)).await {
                    Ok(Resp::Get(GetRes::Value(_, b))) => format!("read,{k},{},{t0},{},ok", i64::from_le_bytes(b[..8].try_into().unwrap()), us()),
                    Ok(Resp::Get(GetRes::Absent(_))) => format!("read,{k},0,{t0},{},absent", us()),
                    _ => continue,
                };
                writeln!(f.lock(), "{line}").unwrap();
            }
        }));
    }
    tokio::time::sleep(Duration::from_secs(secs)).await;
    stop.store(true, Relaxed);
    for t in ts {
        let _ = t.await;
    }
    f.lock().flush().unwrap();
    println!(
        "{}",
        json!({"bench": "esiste-in-gara", "secs": secs, "keys": keys, "inc_ok": cnt[0].load(Relaxed), "inc_nomatch": cnt[1].load(Relaxed),
               "conflicts_retried": cnt[2].load(Relaxed), "deletes": cnt[3].load(Relaxed), "recreates": cnt[4].load(Relaxed)})
    );
}

// ================================================================================================ giro 4: lotti

/// Prova dei lotti (metodo del revisore delle idee pazze 3): carico fisso `rate` record/s a ciclo aperto, lotti da
/// `batch` commit per messaggio (1 = richiesta normale), al più `clients` richieste del cliente in volo (un lotto ne
/// tiene `batch`). La latenza di un record parte dall'istante pianificato. La CPU del capo la misura lo script, da fuori.
const WORDS: [&str; 64] = [
    "alpha", "bravo", "rosso", "verde", "milano", "roma", "torino", "napoli", "ordine", "spedito", "pagato", "annullato",
    "premium", "base", "pro", "gratis", "mario", "luca", "giulia", "anna", "marco", "sara", "paolo", "elena", "scarpe",
    "maglia", "zaino", "libro", "tazza", "lampada", "sedia", "tavolo", "nuovo", "usato", "offerta", "saldi", "carta",
    "bonifico", "paypal", "contanti", "nord", "sud", "est", "ovest", "piccolo", "medio", "grande", "extra", "blu",
    "nero", "bianco", "giallo", "app", "web", "ios", "android", "admin", "utente", "ospite", "attivo", "sospeso",
    "chiuso", "aperto", "bozza",
];

/// Documento JSON "da applicazione" di circa `target` byte (giro 4, prova della compressione): id esadecimali casuali,
/// date, numeri, parole da un vocabolario piccolo, nomi di campo ripetuti come in una collezione vera (STIMATO
/// realistico: non è un campione di clienti veri).
pub fn json_doc(target: usize) -> Bytes {
    use rand::Rng;
    let mut g = rand::thread_rng();
    let w = |g: &mut rand::rngs::ThreadRng| WORDS[g.gen_range(0..WORDS.len())];
    let mut s = format!(
        "{{\"_id\":\"{:024x}\",\"userId\":\"{:024x}\",\"createdAt\":\"2026-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z\",\"status\":\"{}\",\"plan\":\"{}\",\"customer\":{{\"name\":\"{} {}\",\"email\":\"{}.{}{}@example.com\",\"city\":\"{}\",\"zip\":\"{:05}\"}},\"payment\":{{\"method\":\"{}\",\"amount\":{}.{:02},\"currency\":\"EUR\"}},\"items\":[",
        g.gen::<u128>() & ((1u128 << 96) - 1), g.gen::<u128>() & ((1u128 << 96) - 1),
        g.gen_range(1..13), g.gen_range(1..29), g.gen_range(0..24), g.gen_range(0..60), g.gen_range(0..60), g.gen_range(0..1000),
        w(&mut g), w(&mut g), w(&mut g), w(&mut g), w(&mut g), w(&mut g), g.gen_range(1..999), w(&mut g), g.gen_range(0..99999),
        w(&mut g), g.gen_range(1..2000), g.gen_range(0..100)
    );
    let mut i = 0;
    while s.len() < target.saturating_sub(120) {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(
            "{{\"sku\":\"{}-{:06}\",\"name\":\"{} {}\",\"qty\":{},\"price\":{}.{:02},\"tags\":[\"{}\",\"{}\"]}}",
            w(&mut g), g.gen_range(0..1_000_000), w(&mut g), w(&mut g), g.gen_range(1..10), g.gen_range(1..500), g.gen_range(0..100), w(&mut g), w(&mut g)
        ));
        i += 1;
    }
    s.push_str(&format!("],\"notes\":\"{} {} {}\",\"version\":{}}}", w(&mut g), w(&mut g), w(&mut g), g.gen_range(1..20)));
    Bytes::from(s)
}

/// Record "vero" (giro 5, prova del tetto): un inserimento MongoDB in una collezione con `keys - 1` indici
/// secondari. Chiave 0 = il documento (JSON da circa 1 KB, come nel giro 4); chiavi 1.. = voci d'indice
/// `ix/<indice>/<valore>/<_id>` con valore corto (12 byte, l'_id). STIMATO realistico: non è un campione di clienti.
pub fn real_record(keys: usize) -> Vec<Write> {
    use rand::Rng;
    let mut g = rand::thread_rng();
    let id = format!("{:024x}", g.gen::<u128>() & ((1u128 << 96) - 1));
    let mut w = Vec::with_capacity(keys);
    // documenti presi da una scorta di 8.192 già pronti: generarli al volo costava al generatore più CPU di quanta ne
    // abbia la macchina da 2 vCPU a 25.000 record/s. Il capo non vede differenza (byte e chiavi uguali).
    static POOL: std::sync::OnceLock<Vec<Bytes>> = std::sync::OnceLock::new();
    let pool = POOL.get_or_init(|| (0..8192).map(|_| json_doc(1024)).collect());
    w.push(Write { key: doc_key(&id), kind: Kind::Put, val: pool[g.gen_range(0..pool.len())].clone() });
    for i in 1..keys {
        let v = match i % 4 {
            1 => format!("{:024x}", g.gen::<u128>() & ((1u128 << 96) - 1)), // userId
            2 => format!("2026-{:02}-{:02}T{:02}:{:02}", g.gen_range(1..13), g.gen_range(1..29), g.gen_range(0..24), g.gen_range(0..60)), // createdAt
            3 => WORDS[g.gen_range(0..WORDS.len())].to_string(), // status
            _ => format!("{}", g.gen_range(0..100_000)), // amount, zip, ...
        };
        w.push(Write { key: Bytes::from(format!("ix/{i}/{v}/{id}")), kind: Kind::Put, val: Bytes::from(id.clone().into_bytes()).slice(..12) });
    }
    w
}

/// Record della miscela B1 (giro 6, M16), con il formato di oggi (documento intero nel record):
/// - 25% inserimento: come `real_record` (documento + `keys - 1` voci d'indice nuove);
/// - 70% `$set` su 1–2 campi di un documento esistente (preso a caso tra 1 milione di _id: quasi sempre fuori dalla
///   finestra): il documento intero nuovo; nella metà dei casi cambia un campo indicizzato, quindi la voce vecchia
///   si cancella e la nuova si scrive (1 o 3 chiavi);
/// - 5% cancellazione: il documento e le sue `keys - 1` voci d'indice (cancellazioni).
/// Media con 8 chiavi: 0,25·8 + 0,05·8 + 0,7·(0,5·1 + 0,5·3) = 3,8 chiavi per record (STIMATO: è la definizione).
pub fn b1_record(keys: usize) -> Vec<Write> {
    use rand::Rng;
    let mut g = rand::thread_rng();
    let r: f64 = g.gen();
    if r < 0.25 || keys <= 1 {
        return real_record(keys);
    }
    static POOL: std::sync::OnceLock<Vec<Bytes>> = std::sync::OnceLock::new();
    let pool = POOL.get_or_init(|| (0..8192).map(|_| json_doc(1024)).collect());
    let id = format!("{:024x}", 0xabc0_0000_0000u64 + g.gen_range(0..1_000_000u64));
    let ixv = |g: &mut rand::rngs::ThreadRng, i: usize| match i % 4 {
        1 => format!("{:024x}", g.gen::<u128>() & ((1u128 << 96) - 1)),
        2 => format!("2026-{:02}-{:02}T{:02}:{:02}", g.gen_range(1..13), g.gen_range(1..29), g.gen_range(0..24), g.gen_range(0..60)),
        3 => WORDS[g.gen_range(0..WORDS.len())].to_string(),
        _ => format!("{}", g.gen_range(0..100_000)),
    };
    if r < 0.95 {
        let mut w = vec![Write { key: doc_key(&id), kind: Kind::Put, val: pool[g.gen_range(0..pool.len())].clone() }];
        if g.gen_bool(0.5) {
            let i = g.gen_range(1..keys);
            let (old, new) = (ixv(&mut g, i), ixv(&mut g, i));
            w.push(Write { key: Bytes::from(format!("ix/{i}/{old}/{id}")), kind: Kind::Del, val: Bytes::new() });
            w.push(Write { key: Bytes::from(format!("ix/{i}/{new}/{id}")), kind: Kind::Put, val: Bytes::from(id.clone().into_bytes()).slice(..12) });
        }
        w
    } else {
        let mut w = vec![Write { key: doc_key(&id), kind: Kind::Del, val: Bytes::new() }];
        for i in 1..keys {
            w.push(Write { key: Bytes::from(format!("ix/{i}/{}/{id}", ixv(&mut g, i))), kind: Kind::Del, val: Bytes::new() });
        }
        w
    }
}

pub async fn lotti(cl: Arc<Cluster>, rate: f64, batch: usize, clients: usize, secs: u64, warm: u64, keys: usize, val: usize, dbs: u64, real_mode: u64, reads: f64) {
    // real_mode: 0 = chiavi casuali, 1 = record vero d'inserimento (giro 5), 2 = miscela B1 (giro 6)
    let real = real_mode >= 1;
    // val = 0: documenti JSON da circa 1 KB invece di un valore fisso (prova della compressione)
    // real: record "vero" (un documento + voci d'indice, giro 5)
    let json = val == 0;
    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let h = Arc::new(Mutex::new(hist()));
    let ok = Arc::new(AtomicU64::new(0));
    let conf = Arc::new(AtomicU64::new(0));
    let unk = Arc::new(AtomicU64::new(0));
    let rej = Arc::new(AtomicU64::new(0));
    let sem = Arc::new(tokio::sync::Semaphore::new(clients.max(batch)));
    let value = Bytes::from(vec![9u8; val]);
    let gen = {
        let (cl, h, ok, conf, unk, rej, sem, measuring, value) = (cl.clone(), h.clone(), ok.clone(), conf.clone(), unk.clone(), rej.clone(), sem.clone(), measuring.clone(), value.clone());
        drive(rate / batch as f64, (clients / batch).max(1), stop.clone(), move |sched| {
            let (cl, h, ok, conf, unk, rej, sem, measuring, value) = (cl.clone(), h.clone(), ok.clone(), conf.clone(), unk.clone(), rej.clone(), sem.clone(), measuring.clone(), value.clone());
            async move {
                let _permit = sem.acquire_many(batch as u32).await.unwrap();
                let rp = cl.seen.load(Relaxed);
                let cs: Vec<CommitReq> = (0..batch)
                    .map(|_| {
                        let db = db_id(rand::random::<u64>() % dbs);
                        let writes = if real_mode == 2 {
                            b1_record(keys)
                        } else if real {
                            real_record(keys)
                        } else {
                            (0..keys)
                                .map(|_| Write { key: doc_key(&format!("{:016x}", rand::random::<u64>())), kind: Kind::Put, val: if json { json_doc(1024) } else { value.clone() } })
                                .collect()
                        };
                        CommitReq { txn: txn_id(), db, read_pos: rp, mode: Mode::Validated, writes, reads: vec![], session: None }
                    })
                    .collect();
                let rs = cl.commit_batch(&cs, Duration::from_secs(10)).await;
                let us = sched.elapsed().as_micros() as u64;
                if !measuring.load(Relaxed) {
                    return;
                }
                for r in rs {
                    match r {
                        Outcome::Committed { .. } => {
                            ok.fetch_add(1, Relaxed);
                            h.lock().record(us.max(1)).ok();
                        }
                        Outcome::Conflict(_) => {
                            conf.fetch_add(1, Relaxed);
                        }
                        Outcome::Unknown => {
                            unk.fetch_add(1, Relaxed);
                        }
                        Outcome::Rejected(_) => {
                            rej.fetch_add(1, Relaxed);
                        }
                    }
                }
            }
        })
    };
    // giro 6 (M16): letture sulle repliche (tutte e 3, a caso) mentre si scrive: 90% get di un documento della miscela
    // B1 (1 milione di _id), 10% scan di 10 voci d'indice; alla posizione confermata vista dal client
    let rh = Arc::new(Mutex::new(hist()));
    let (rok, row) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let rgen = {
        let (cl, rh, rok, row, measuring) = (cl.clone(), rh.clone(), rok.clone(), row.clone(), measuring.clone());
        open_loop(reads, stop.clone(), move |_sched| {
            let (cl, rh, rok, row, measuring) = (cl.clone(), rh.clone(), rok.clone(), row.clone(), measuring.clone());
            async move {
                let db = db_id(rand::random::<u64>() % dbs);
                let at = cl.seen.load(Relaxed);
                let r = rand::random::<usize>() % cl.n();
                let t = Instant::now();
                let q = if rand::random::<u8>() < 26 {
                    let from = Bytes::from(format!("ix/3/{}/", WORDS[rand::random::<usize>() % WORDS.len()]));
                    let mut to = from.to_vec();
                    *to.last_mut().unwrap() = b'0';
                    Req::Scan { db, from, to: Bytes::from(to), at, limit: 10, wait_ms: 50, dal: 0 }
                } else {
                    Req::Get { db, key: doc_key(&format!("{:024x}", 0xabc0_0000_0000u64 + rand::random::<u64>() % 1_000_000)), at, wait_ms: 50, spec: false, dal: 0 }
                };
                let res = cl.call(r, &q, Duration::from_secs(2)).await;
                if !measuring.load(Relaxed) {
                    return;
                }
                match res {
                    Ok(Resp::Get(GetRes::OutOfWindow { .. })) => {
                        row.fetch_add(1, Relaxed);
                        rok.fetch_add(1, Relaxed);
                        rh.lock().record(t.elapsed().as_micros().max(1) as u64).ok();
                    }
                    Ok(Resp::Get(_)) | Ok(Resp::Scan(_)) => {
                        rok.fetch_add(1, Relaxed);
                        rh.lock().record(t.elapsed().as_micros().max(1) as u64).ok();
                    }
                    _ => {}
                }
            }
        })
    };
    tokio::time::sleep(Duration::from_secs(warm)).await;
    measuring.store(true, Relaxed);
    let t0 = Instant::now();
    tokio::time::sleep(Duration::from_secs(secs)).await;
    measuring.store(false, Relaxed);
    let el = t0.elapsed().as_secs_f64();
    stop.store(true, Relaxed);
    let _ = gen.await;
    let _ = rgen.await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let n = ok.load(Relaxed) as f64;
    println!(
        "{}",
        json!({"reads_per_s": rok.load(Relaxed) as f64 / el, "reads_out_of_window": row.load(Relaxed), "read_latency": hj(&rh.lock()),
               "bench": "lotti", "rate": rate, "batch": batch, "clients": clients, "keys_per_rec": keys, "val_bytes": val, "dbs": dbs, "real": real_mode,
               "secs": el, "recs_per_s": n / el, "latency": hj(&h.lock()), "conflicts": conf.load(Relaxed),
               "unknown": unk.load(Relaxed), "rejected": rej.load(Relaxed), "diag": cl.diag.json()})
    );
}

// ------------------------------------------------------------------------------------------------ Q8 · corsie

/// Q8: configurazione di un generatore della prova delle corsie (un processo per gruppo di database).
pub struct CorsieCfg {
    /// database `db_from .. db_from + dbs`
    pub db_from: u64,
    pub dbs: u64,
    /// record al secondo PER database (ciclo aperto); 0 = nessun flusso costante
    pub rate: f64,
    /// "steady" (flusso costante), "burst" (raffiche), "big" (record grandi a `rate`)
    pub mode: String,
    pub doc: usize,
    pub big_kb: usize,
    pub burst: usize,
    pub burst_every_ms: u64,
    pub batch: usize,
    pub inflight: usize,
    pub deadline_ms: u64,
    pub warm: u64,
    pub secs: u64,
    pub label: String,
}

/// Record della prova: un inserimento di un documento JSON da `doc` byte (1,9 KB come P1) con due voci d'indice
/// (userId e createdAt), modalità validata alla posizione vista dal client.
fn corsie_record(doc: &Bytes) -> Vec<Write> {
    use rand::Rng;
    let mut g = rand::thread_rng();
    let id = format!("{:024x}", g.gen::<u128>() & ((1u128 << 96) - 1));
    let user = format!("{:024x}", g.gen::<u128>() & ((1u128 << 96) - 1));
    let when = format!("2026-{:02}-{:02}T{:02}:{:02}:{:02}", g.gen_range(1..13), g.gen_range(1..29), g.gen_range(0..24), g.gen_range(0..60), g.gen_range(0..60));
    let idb = Bytes::from(id.clone().into_bytes()).slice(..12);
    vec![
        Write { key: doc_key(&id), kind: Kind::Put, val: doc.clone() },
        Write { key: Bytes::from(format!("ix/1/{user}/{id}")), kind: Kind::Put, val: idb.clone() },
        Write { key: Bytes::from(format!("ix/2/{when}/{id}")), kind: Kind::Put, val: idb },
    ]
}

struct CorsieAcc {
    h: Histogram<u64>,
    /// Q8c: latency from the actual send (the open-loop tick can fire up to ~1 ms after its scheduled instant)
    h_send: Histogram<u64>,
    /// Q8c: round trip of a request the replica answers at once (like the "hello" subtracted in the Atlas numbers)
    ping: Histogram<u64>,
    /// per secondo della misura: istogramma (per vedere quando succede qualcosa)
    per_s: Vec<Histogram<u64>>,
    ok: u64,
    busy_retries: u64,
    rejected: u64,
    unknown: u64,
    conflict: u64,
    dropped: u64,
    /// Q8c: latency per log of the cell (to compare the normals on the noisy neighbour's log with the others)
    per_log: Vec<Histogram<u64>>,
}

/// Q8c: database -> log of the cell (like a Kafka partition). Fixed rule for the tests: database number modulo the
/// number of logs (in the product it is a directory entry, so a database can be moved).
pub fn log_of(db_num: u64, logs: usize) -> usize {
    (db_num % logs as u64) as usize
}

pub async fn corsie(cls: Vec<Arc<Cluster>>, c: CorsieCfg) {
    let cl = cls[0].clone();
    let nlogs = cls.len();
    use rand::Rng;
    let pool: Arc<Vec<Bytes>> = Arc::new(if c.mode == "big" { (0..4).map(|_| { let mut v = vec![0u8; c.big_kb << 10]; rand::thread_rng().fill(&mut v[..]); Bytes::from(v) }).collect() } else { (0..2048).map(|_| json_doc(c.doc)).collect() });
    let acc = Arc::new(Mutex::new(CorsieAcc { h: hist(), h_send: hist(), ping: hist(), per_s: (0..c.secs.min(900) + 2).map(|_| hist()).collect(), ok: 0, busy_retries: 0, rejected: 0, unknown: 0, conflict: 0, dropped: 0, per_log: (0..nlogs).map(|_| hist()).collect() }));
    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let t_meas = Arc::new(Mutex::new(Instant::now()));
    let sem = Arc::new(tokio::sync::Semaphore::new(c.inflight));
    let sent = Arc::new(AtomicU64::new(0));
    let deadline = Duration::from_millis(c.deadline_ms);
    let record = {
        let (acc, measuring, t_meas) = (acc.clone(), measuring.clone(), t_meas.clone());
        move |sched: Instant, sent_at: Instant, lg: usize, outs: Vec<Outcome>| {
            if !measuring.load(Relaxed) {
                return;
            }
            let us = sched.elapsed().as_micros() as u64;
            let us_send = sent_at.elapsed().as_micros() as u64;
            let sec = sched.saturating_duration_since(*t_meas.lock()).as_secs() as usize;
            let mut a = acc.lock();
            for o in outs {
                match o {
                    Outcome::Committed { busy, .. } => {
                        a.ok += 1;
                        a.busy_retries += busy as u64;
                        a.h.record(us.max(1)).ok();
                        a.h_send.record(us_send.max(1)).ok();
                        a.per_log[lg].record(us.max(1)).ok();
                        if sec < a.per_s.len() {
                            a.per_s[sec].record(us.max(1)).ok();
                        }
                    }
                    Outcome::Rejected(_) => a.rejected += 1,
                    Outcome::Unknown => a.unknown += 1,
                    Outcome::Conflict(_) => a.conflict += 1,
                }
            }
        }
    };
    let record = Arc::new(record);
    // posizione di lettura di partenza: quella confermata adesso (con 0 la convalida rifiuterebbe ogni commit di un
    // database la cui finestra è già stata potata)
    for cl in &cls {
        for _ in 0..50 {
            if let Some(Resp::Pos(p)) = cl.call_leader(&Req::Confirmed, Duration::from_secs(2)).await {
                cl.seen.fetch_max(p, Relaxed);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    // flusso costante (steady e big): la k-esima scrittura va al database db_from + k % dbs, all'istante pianificato
    let gen = if c.rate > 0.0 && c.mode != "burst" {
        let (cls, pool, sem, sent, record, acc) = (Arc::new(cls.clone()), pool.clone(), sem.clone(), sent.clone(), record.clone(), acc.clone());
        let (from, dbs) = (c.db_from, c.dbs);
        let k = Arc::new(AtomicU64::new(0));
        Some(open_loop(c.rate * c.dbs as f64, stop.clone(), move |sched| {
            let (cls, pool, sem, sent, record, acc, k) = (cls.clone(), pool.clone(), sem.clone(), sent.clone(), record.clone(), acc.clone(), k.clone());
            async move {
                let Ok(_p) = sem.try_acquire() else {
                    acc.lock().dropped += 1;
                    return;
                };
                let n = k.fetch_add(1, Relaxed);
                let dbn = from + n % dbs;
                let cl = cls[log_of(dbn, nlogs)].clone();
                let db = db_id(dbn);
                let doc = pool[rand::random::<usize>() % pool.len()].clone();
                let cr = CommitReq { txn: txn_id(), db, read_pos: cl.seen.load(Relaxed), mode: Mode::Validated, writes: corsie_record(&doc), reads: vec![], session: None };
                sent.fetch_add(1, Relaxed);
                let ts = Instant::now();
                let o = cl.commit(&cr, deadline).await;
                record(sched, ts, log_of(dbn, nlogs), vec![o]);
            }
        }))
    } else {
        None
    };
    // raffiche: ogni `burst_every_ms` un'onda di `burst` record in lotti da `batch` (CommitBatch), tutti insieme
    let bgen = if c.mode == "burst" {
        let (cls, pool, sem, sent, record, stop) = (cls.clone(), pool.clone(), sem.clone(), sent.clone(), record.clone(), stop.clone());
        let (from, dbs, burst, batch, every) = (c.db_from, c.dbs, c.burst, c.batch.max(1), c.burst_every_ms);
        Some(tokio::spawn(async move {
            let mut wave = 0u64;
            while !stop.load(Relaxed) {
                let sched = Instant::now();
                let dbn = from + wave % dbs;
                let lg = log_of(dbn, nlogs);
                let cl = cls[lg].clone();
                let db = db_id(dbn);
                wave += 1;
                let mut hs = vec![];
                for _ in 0..burst.div_ceil(batch) {
                    let (cl, pool, sem, sent, record) = (cl.clone(), pool.clone(), sem.clone(), sent.clone(), record.clone());
                    hs.push(tokio::spawn(async move {
                        let _p = sem.acquire().await;
                        let rp = cl.seen.load(Relaxed);
                        let cs: Vec<CommitReq> = (0..batch)
                            .map(|_| CommitReq { txn: txn_id(), db, read_pos: rp, mode: Mode::Validated, writes: corsie_record(&pool[rand::random::<usize>() % pool.len()]), reads: vec![], session: None })
                            .collect();
                        sent.fetch_add(cs.len() as u64, Relaxed);
                        let ts = Instant::now();
                        let rs = cl.commit_batch(&cs, deadline).await;
                        record(sched, ts, lg, rs);
                    }));
                }
                let _ = tokio::time::timeout(Duration::from_millis(every), async {
                    for h in hs {
                        let _ = h.await;
                    }
                })
                .await;
                let el = sched.elapsed();
                if el < Duration::from_millis(every) {
                    tokio::time::sleep(Duration::from_millis(every) - el).await;
                }
            }
        }))
    } else {
        None
    };
    // Q8c: ping every 50 ms to each log (served at once by the network thread of the replica)
    {
        let (cls, acc, measuring, stop) = (cls.clone(), acc.clone(), measuring.clone(), stop.clone());
        tokio::spawn(async move {
            while !stop.load(Relaxed) {
                for cl in &cls {
                    let t = Instant::now();
                    if let Some(Resp::Pos(_)) = cl.call_leader(&Req::Wait { pos: 0, timeout_ms: 0 }, Duration::from_secs(1)).await {
                        if measuring.load(Relaxed) {
                            acc.lock().ping.record((t.elapsed().as_micros() as u64).max(1)).ok();
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    }
    // resoconto periodico su stderr (per i generatori rumorosi, che vengono uccisi senza arrivare alla fine)
    {
        let (acc, sent, label, stop) = (acc.clone(), sent.clone(), c.label.clone(), stop.clone());
        tokio::spawn(async move {
            let t0 = Instant::now();
            while !stop.load(Relaxed) {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let a = acc.lock();
                eprintln!("corsie {label} t={:.0}s inviati {} confermati(misura) {} rifiutati {} ignoti {} scartati {} rifiuti_freno {}", t0.elapsed().as_secs_f64(), sent.load(Relaxed), a.ok, a.rejected, a.unknown, a.dropped, a.busy_retries);
            }
        });
    }
    tokio::time::sleep(Duration::from_secs(c.warm)).await;
    *t_meas.lock() = Instant::now();
    let sent0 = sent.load(Relaxed);
    measuring.store(true, Relaxed);
    let t0 = Instant::now();
    tokio::time::sleep(Duration::from_secs(c.secs)).await;
    let el = t0.elapsed().as_secs_f64();
    let sent1 = sent.load(Relaxed);
    // si aspettano gli esiti dei record pianificati nella misura (al più la scadenza), poi si chiude
    stop.store(true, Relaxed);
    tokio::time::sleep(Duration::from_millis(c.deadline_ms.min(3000) + 500)).await;
    measuring.store(false, Relaxed);
    if let Some(g) = gen {
        g.abort();
    }
    if let Some(g) = bgen {
        g.abort();
    }
    let a = acc.lock();
    let per_s: Vec<serde_json::Value> = a.per_s.iter().take(c.secs as usize).map(|h| json!([h.len(), h.value_at_quantile(0.5), h.value_at_quantile(0.99), h.max()])).collect();
    println!(
        "{}",
        json!({"bench": "corsie", "label": c.label, "mode": c.mode, "db_from": c.db_from, "dbs": c.dbs, "rate_per_db": c.rate, "doc": c.doc, "secs": el,
               "sent_per_s": (sent1 - sent0) as f64 / el, "ok": a.ok, "ok_per_s": a.ok as f64 / el, "busy_retries": a.busy_retries, "rejected": a.rejected, "unknown": a.unknown,
               "conflict": a.conflict, "dropped": a.dropped,
               "lat": {"n": a.h.len(), "p50_us": a.h.value_at_quantile(0.5), "p90_us": a.h.value_at_quantile(0.9), "p99_us": a.h.value_at_quantile(0.99),
                       "p999_us": a.h.value_at_quantile(0.999), "p9999_us": a.h.value_at_quantile(0.9999), "max_us": a.h.max(), "mean_us": a.h.mean()},
               "per_s_n_p50_p99_max": per_s, "logs": nlogs,
               "lat_send": {"n": a.h_send.len(), "p50_us": a.h_send.value_at_quantile(0.5), "p99_us": a.h_send.value_at_quantile(0.99), "p999_us": a.h_send.value_at_quantile(0.999), "max_us": a.h_send.max()},
               "ping": {"n": a.ping.len(), "p50_us": a.ping.value_at_quantile(0.5), "p99_us": a.ping.value_at_quantile(0.99)},
               "per_log": a.per_log.iter().map(|h| json!({"n": h.len(), "p50_us": h.value_at_quantile(0.5), "p99_us": h.value_at_quantile(0.99), "p999_us": h.value_at_quantile(0.999), "max_us": h.max()})).collect::<Vec<_>>(),
               "diag": cl.diag.json()})
    );
}
