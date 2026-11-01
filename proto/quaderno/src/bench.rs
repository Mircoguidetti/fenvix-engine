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
    let mut win = Window::new(true);
    let mut pos: Pos = pos_make(now_ms(), 0);
    let value = Bytes::from(vec![7u8; val]);
    let mut noload = |_: u64, _: u16| -> Bytes { unreachable!() };
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
        let e = Entry::encode(1, index, pos, Body::Rec { txn: txn_id(), db, session: None, writes });
        if let Body::Rec { txn, db, writes, .. } = &e.body {
            win.apply(index, pos, *txn, db, writes, e.raw.len() as u32);
        }
        // a regime: il database esce dalla finestra dal fondo (già su S3)
        let d = win.dbs.get_mut(&db).unwrap();
        d.flushed = pos;
        if d.recs.len() as u64 > per_db {
            let p = d.recs.front().unwrap().pos;
            win.trim_one(&db, p + 1, pos, &mut noload);
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
                    Outcome::Unknown => {
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
}

const HOT_DB: u64 = 999_999_999;

#[derive(Default)]
struct Iv {
    commit: Option<Histogram<u64>>,
    rwin: Option<Histogram<u64>>,
    rcold: Option<Histogram<u64>>,
    ryw: Option<Histogram<u64>>,
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
        _ => &mut g.ryw,
    };
    slot.get_or_insert_with(hist).record(us.max(1)).ok();
}

fn merge(hs: &[Mutex<Iv>], reset: bool) -> Iv {
    let mut o = Iv::default();
    for h in hs {
        let mut g = h.lock();
        let x = if reset { std::mem::take(&mut *g) } else { Iv { commit: g.commit.clone(), rwin: g.rwin.clone(), rcold: g.rcold.clone(), ryw: g.ryw.clone() } };
        for (a, b) in [(&mut o.commit, x.commit), (&mut o.rwin, x.rwin), (&mut o.rcold, x.rcold), (&mut o.ryw, x.ryw)] {
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
           "read_other": hj(i.rcold.as_ref().unwrap_or(&e)), "ryw": hj(i.ryw.as_ref().unwrap_or(&e))})
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
        match st.cl.commit(&c, Duration::from_secs(10)).await {
            Outcome::Committed { .. } => {
                let us = sched.elapsed().as_micros() as u64;
                st.ok.fetch_add(1, Relaxed);
                rec(&st.iv, 0, us);
                rec(&st.tot, 0, us);
                let i = st.recent_i.fetch_add(1, Relaxed) as usize & 0xffff;
                st.recent[i].store((db << 32) | kn, Relaxed);
            }
            Outcome::Conflict(_) => {
                st.conflicts.fetch_add(1, Relaxed);
            }
            Outcome::Unknown => {
                st.unknown.fetch_add(1, Relaxed);
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
                match st.cl.call(r, &Req::Get { db: db_id(db), key, at, wait_ms: 50, spec: false, dal: 0 }, Duration::from_secs(2)).await {
                    Ok(Resp::Get(g)) => {
                        let us = sched.elapsed().as_micros() as u64;
                        let ow = matches!(g, GetRes::OutOfWindow { .. });
                        if ow {
                            st.reads_ow.fetch_add(1, Relaxed);
                        }
                        st.reads_ok.fetch_add(1, Relaxed);
                        let w = if window { 1 } else { 2 };
                        rec(&st.iv, w, us);
                        rec(&st.tot, w, us);
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
                    "keys": s.keys, "dbs": s.dbs, "unflushed": s.unflushed_recs, "evicted": s.evicted, "log_len": s.last_index + 1 - s.log_front,
                    "disk_mb": s.disk_bytes as f64 / 1e6, "flush_lag_s": if s.min_unflushed_pos > 0 { (now_ms() as f64 - pos_ms(s.min_unflushed_pos) as f64) / 1000.0 } else { 0.0 }}));
            }
        }
        println!(
            "{}",
            json!({"t": t0.elapsed().as_secs_f64(), "commits_per_s": (now.0 - last.0) as f64 / el, "reads_per_s": (now.1 - last.1) as f64 / el,
                   "conflicts": now.2 - last.2, "unknown": now.3 - last.3, "lat": ivj(&iv), "replicas": reps})
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
               "lat": ivj(&tot)})
    );
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
                        Outcome::Unknown => {
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
                            Outcome::Committed { pos, attempts, ambiguous } => {
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
