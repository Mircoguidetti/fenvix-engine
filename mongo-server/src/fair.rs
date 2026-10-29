//! Fairness between databases when the server is saturated, very high safety limits per database, and per-database
//! metrics for the control plane (`GET /v1/metrics/databases` on the admin port).
//!
//! **Fair queue, only under saturation.** A ticker measures how late the runtime runs a 10 ms timer (scheduling lag, a
//! direct measure of CPU saturation of the worker threads). While the lag is below `--fair-lag-ms` every command starts at
//! once: no database is slowed while there is capacity. Above it, a new command does not start while others are in flight:
//! it waits in its database's queue, and each command that finishes lets the next one start, taking first a command that
//! wakes a sleeping database, then the databases in turn (round robin). A database sending 64 commands at once then gets the
//! same turns as one sending a single command; nobody is refused. When the lag falls back, every queued command starts.
//!
//! **Safety limits.** Per database, at most `--db-max-inflight` commands running or queued and at most `--db-max-rate`
//! commands in one second. The defaults (10 000 and 100 000) are far above what a database can be served at (results-round3.md
//! measures the ceiling of one database): they exist only against runaway clients and infinite loops. A command over a limit
//! gets error 462 `IngressRequestRateLimitExceeded` with a message that names the limit, and the database's `safetyRejections`
//! metric grows.
//!
//! **Metrics per database.** Commands, commands per second (last 10 s and last 60 s), CPU time spent in its commands (time in
//! `poll` on the worker threads), time waited in the fair queue, safety rejections, and an estimate of the memory its unflushed
//! writes hold (bytes written since the database was opened, of which the memtable holds at most `--memtable-max-mb`).

use bson::{doc, Document};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

pub static LAG_THRESHOLD_US: AtomicU64 = AtomicU64::new(10_000);
pub static MAX_INFLIGHT: AtomicU64 = AtomicU64::new(10_000);
pub static MAX_RATE: AtomicU64 = AtomicU64::new(100_000);
/// Exponential average of the scheduling lag (microseconds) and whether the gate is engaged.
pub static LAG_US: AtomicU64 = AtomicU64::new(0);
pub static LAG_MAX_US: AtomicU64 = AtomicU64::new(0);
static SATURATED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
pub static SATURATED_MS: AtomicU64 = AtomicU64::new(0);
pub static QUEUED_TOTAL: AtomicU64 = AtomicU64::new(0);

struct DbStat {
    commands: u64,
    cpu_us: u64,
    queue_waits: u64,
    queue_wait_us: u64,
    queue_wait_max_us: u64,
    rejections: u64,
    inflight: u64,
    /// commands in the current second, and that second
    sec: u64,
    sec_count: u64,
    /// per-second counts of the last 60 seconds (ring)
    ring: [u32; 60],
    ring_sec: [u64; 60],
    unflushed_bytes: u64,
}

impl Default for DbStat {
    fn default() -> Self {
        DbStat { commands: 0, cpu_us: 0, queue_waits: 0, queue_wait_us: 0, queue_wait_max_us: 0, rejections: 0, inflight: 0, sec: 0, sec_count: 0, ring: [0; 60], ring_sec: [0; 60], unflushed_bytes: 0 }
    }
}

struct Waiter {
    tx: oneshot::Sender<()>,
}

#[derive(Default)]
struct Gate {
    inflight: u64,
    wakes: VecDeque<(String, Waiter)>,
    queues: HashMap<String, VecDeque<Waiter>>,
    order: VecDeque<String>,
}

static STATS: std::sync::LazyLock<Mutex<HashMap<String, DbStat>>> = std::sync::LazyLock::new(Default::default);
static GATE: std::sync::LazyLock<Mutex<Gate>> = std::sync::LazyLock::new(Default::default);

fn now_sec() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
}

pub fn install(lag_ms: f64, max_inflight: u64, max_rate: u64) {
    LAG_THRESHOLD_US.store((lag_ms * 1000.0) as u64, Relaxed);
    MAX_INFLIGHT.store(max_inflight, Relaxed);
    MAX_RATE.store(max_rate, Relaxed);
    tokio::spawn(async {
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last = Instant::now();
        loop {
            tick.tick().await;
            let now = Instant::now();
            let lag = now.duration_since(last).as_micros().saturating_sub(10_000) as u64;
            last = now;
            let old = LAG_US.load(Relaxed);
            let avg = (old * 7 + lag) / 8;
            LAG_US.store(avg, Relaxed);
            LAG_MAX_US.fetch_max(lag, Relaxed);
            let thr = LAG_THRESHOLD_US.load(Relaxed);
            let sat = thr > 0 && avg > thr;
            if sat {
                SATURATED_MS.fetch_add(10, Relaxed);
            }
            if SATURATED.swap(sat, Relaxed) && !sat {
                // saturation is over: start everything that waits, wakes first, then in turn
                let mut g = GATE.lock().unwrap();
                while let Some(w) = next_waiter(&mut g) {
                    g.inflight += 1;
                    let _ = w.tx.send(());
                }
            }
        }
    });
}

fn next_waiter(g: &mut Gate) -> Option<Waiter> {
    if let Some((_, w)) = g.wakes.pop_front() {
        return Some(w);
    }
    while let Some(db) = g.order.pop_front() {
        let Some(q) = g.queues.get_mut(&db) else { continue };
        let w = q.pop_front();
        if q.is_empty() {
            g.queues.remove(&db);
        } else {
            g.order.push_back(db);
        }
        if let Some(w) = w {
            return Some(w);
        }
    }
    None
}

/// A command's place in the gate; dropping it lets the next queued command start.
pub struct Permit {
    db: String,
}

impl Drop for Permit {
    fn drop(&mut self) {
        if let Some(s) = STATS.lock().unwrap().get_mut(&self.db) {
            s.inflight = s.inflight.saturating_sub(1);
        }
        let mut g = GATE.lock().unwrap();
        g.inflight = g.inflight.saturating_sub(1);
        // under saturation one finishing command starts one queued command; otherwise the queue is empty anyway
        if let Some(w) = next_waiter(&mut g) {
            g.inflight += 1;
            let _ = w.tx.send(());
        }
    }
}

/// The safety limit a command broke, as a MongoDB error reply.
pub fn rejection(db: &str, what: &str, limit: u64) -> Document {
    doc! { "ok": 0.0, "code": 462, "codeName": "IngressRequestRateLimitExceeded",
        "errmsg": format!("database {db} is over the server's safety limit of {limit} {what}; this limit exists only against runaway clients (a loop sending commands without end); contact support if your application really needs more"),
        "errorLabels": ["SystemOverloadedError"] }
}

/// Take a place for a command of `db` (`wakes`: it will wake a sleeping database). Err: a safety limit, with the reply.
pub async fn admit(db: &str, wakes: bool) -> Result<Permit, Document> {
    {
        let mut st = STATS.lock().unwrap();
        let s = st.entry(db.to_string()).or_default();
        let sec = now_sec();
        if s.sec != sec {
            s.sec = sec;
            s.sec_count = 0;
        }
        s.sec_count += 1;
        let i = (sec % 60) as usize;
        if s.ring_sec[i] != sec {
            s.ring_sec[i] = sec;
            s.ring[i] = 0;
        }
        s.ring[i] = s.ring[i].saturating_add(1);
        let max_rate = MAX_RATE.load(Relaxed);
        if max_rate > 0 && s.sec_count > max_rate {
            s.rejections += 1;
            return Err(rejection(db, "commands per second (--db-max-rate)", max_rate));
        }
        let max_in = MAX_INFLIGHT.load(Relaxed);
        if max_in > 0 && s.inflight >= max_in {
            s.rejections += 1;
            return Err(rejection(db, "commands running or queued at once (--db-max-inflight)", max_in));
        }
        s.inflight += 1;
        s.commands += 1;
    }
    let rx = {
        let mut g = GATE.lock().unwrap();
        if !SATURATED.load(Relaxed) || g.inflight == 0 {
            g.inflight += 1;
            return Ok(Permit { db: db.to_string() });
        }
        let (tx, rx) = oneshot::channel();
        if wakes {
            g.wakes.push_back((db.to_string(), Waiter { tx }));
        } else {
            if !g.queues.contains_key(db) {
                g.order.push_back(db.to_string());
            }
            g.queues.entry(db.to_string()).or_default().push_back(Waiter { tx });
        }
        rx
    };
    QUEUED_TOTAL.fetch_add(1, Relaxed);
    let t = Instant::now();
    let _ = rx.await;
    let us = t.elapsed().as_micros() as u64;
    if let Some(s) = STATS.lock().unwrap().get_mut(db) {
        s.queue_waits += 1;
        s.queue_wait_us += us;
        s.queue_wait_max_us = s.queue_wait_max_us.max(us);
    }
    Ok(Permit { db: db.to_string() })
}

/// Bytes a database wrote since it was opened (reset at close). Its memtable holds at most `--memtable-max-mb` of them.
pub fn note_written(db: &str, bytes: u64, reset: bool) {
    let mut st = STATS.lock().unwrap();
    let s = st.entry(db.to_string()).or_default();
    if reset {
        s.unflushed_bytes = 0;
    }
    s.unflushed_bytes += bytes;
}

/// A future whose time inside `poll` is added to its database's CPU time.
pub struct Timed<F> {
    inner: Pin<Box<F>>,
    db: String,
    us: u64,
}

impl<F: Future> Future for Timed<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let t = Instant::now();
        let r = self.inner.as_mut().poll(cx);
        self.us += t.elapsed().as_micros() as u64;
        if r.is_ready() {
            if let Some(s) = STATS.lock().unwrap().get_mut(&self.db) {
                s.cpu_us += self.us;
            }
        }
        r
    }
}

pub fn timed<F: Future>(db: &str, f: F) -> Timed<F> {
    Timed { inner: Box::pin(f), db: db.to_string(), us: 0 }
}

/// `GET /v1/metrics/databases`
pub fn metrics(open: impl Fn(&str) -> String) -> serde_json::Value {
    let st = STATS.lock().unwrap();
    let sec = now_sec();
    let mut dbs = serde_json::Map::new();
    for (db, s) in st.iter() {
        let last = |n: u64| -> u64 { (0..n).map(|k| sec - k).filter(|t| s.ring_sec[(t % 60) as usize] == *t).map(|t| s.ring[(t % 60) as usize] as u64).sum() };
        dbs.insert(db.clone(), serde_json::json!({
            "commands": s.commands, "commandsPerSec10s": last(10) as f64 / 10.0, "commandsPerSec60s": last(60) as f64 / 60.0,
            "cpuMs": s.cpu_us / 1000, "queueWaits": s.queue_waits, "queueWaitMs": s.queue_wait_us / 1000, "queueWaitMaxMs": s.queue_wait_max_us / 1000,
            "safetyRejections": s.rejections, "inflight": s.inflight, "bytesWrittenSinceOpen": s.unflushed_bytes, "memtableHoldsAtMostBytes": s.unflushed_bytes.min(tenant_bench::lifecycle::MEMTABLE_MAX_BYTES.load(Relaxed)), "state": open(db),
        }));
    }
    serde_json::json!({
        "server": { "schedulingLagMs": LAG_US.load(Relaxed) as f64 / 1000.0, "schedulingLagMaxMs": LAG_MAX_US.load(Relaxed) as f64 / 1000.0,
                    "saturated": SATURATED.load(Relaxed), "saturatedMs": SATURATED_MS.load(Relaxed), "queuedCommands": QUEUED_TOTAL.load(Relaxed),
                    "inflight": GATE.lock().unwrap().inflight, "fairLagThresholdMs": LAG_THRESHOLD_US.load(Relaxed) as f64 / 1000.0,
                    "dbMaxInflight": MAX_INFLIGHT.load(Relaxed), "dbMaxRate": MAX_RATE.load(Relaxed) },
        "databases": dbs,
    })
}

/// Commands that are not queued: cheap, or that wait on purpose (monitoring, awaitable cursors, authentication).
pub fn exempt(name: &str) -> bool {
    matches!(name, "hello" | "isMaster" | "ismaster" | "ping" | "getMore" | "killCursors" | "endSessions" | "saslStart" | "saslContinue" | "buildInfo" | "buildinfo" | "getLastError" | "logout" | "authenticate")
}
