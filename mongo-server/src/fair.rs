//! Isolation between databases on a shared server: a unit of cost per operation, quotas per database in that unit, a
//! weighted fair queue when the server is saturated, very high safety limits per database, and per-database metrics for
//! the control plane (`GET /v1/metrics/databases` on the admin port). Results and the choice of the unit:
//! docs/isolamento.md.
//!
//! **The unit: points (pt).** Every command is charged
//! `pt = w_cpu * CPU ms + w_written * KiB written + w_read * KiB read + w_put * S3 PUT-class requests + w_get * S3 GET-class requests`.
//! CPU is the time spent in `poll` of the command's future on the worker threads; KiB written are the keys and values
//! the command put in its write batches; KiB read are the keys and values it got from the store (block cache, memtable or
//! S3); S3 requests are charged to the database whose objects they touch (`tenants/{db}/...`), when they happen
//! (flushes, compaction and lease renewals run in the background, after the command that caused them). The weights are
//! `--unit-weights` (default in [`DEFAULT_WEIGHTS`]); one point is about $2e-9, the cost to us of 0.1 ms of CPU on the
//! shared server. The same counters serve the per-database accounting (metrics: points by part) and the quotas.
//!
//! **Quotas per database.** Each database has a bucket of points that refills at its quota (`--db-quota-pts` points per
//! second, or the per-database value set with `PUT /v1/databases/{db}/quota`) up to its burst credit (`--db-burst-pts`).
//! A command starts while the bucket is positive (its expected cost, the database's moving average, is taken at the start
//! and corrected with the real cost at the end). Over the quota the command waits in its database's own queue until the
//! bucket refills; beyond `--db-quota-queue` waiting commands a new one gets the retryable error 462 at once. Quota 0 =
//! no quota (the default for databases without their own value when `--db-quota-pts` is 0).
//!
//! **Fair queue, only under saturation.** A ticker measures how late the runtime runs a 10 ms timer (scheduling lag, a
//! direct measure of CPU saturation of the worker threads). While the lag is below `--fair-lag-ms` (2 ms) every command
//! within its quota starts at once. Above it, at most `--fair-max-inflight` commands run at once (8 per CPU by default)
//! and at most `--fair-max-flushing` databases have their writes flushed at once (4 per CPU);
//! a new command beyond that waits in its database's queue, and each command that finishes lets the next one start:
//! first a command that wakes a sleeping database, then the database that used the fewest points lately relative to its
//! weight (its quota; decayed usage, half-life [`USAGE_HALF_LIFE_S`]). A light database is then served before databases
//! that send without pause; nobody is refused. When the lag falls back, every queued command starts.
//!
//! **Safety limits.** Per database, at most `--db-max-inflight` commands running or queued and at most `--db-max-rate`
//! commands in one second. The defaults (10 000 and 100 000) are far above what a database can be served at: they exist
//! only against runaway clients and infinite loops. A command over a limit gets error 462 `IngressRequestRateLimitExceeded`
//! with a message that names the limit, and the database's `safetyRejections` metric grows.

use bson::{doc, Document};
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

pub static LAG_THRESHOLD_US: AtomicU64 = AtomicU64::new(2_000);
/// Commands that may run at once while the server is saturated (`--fair-max-inflight`; 0 = 8 per CPU the process may use;
/// see docs/isolamento.md for the choice).
/// Without this cap the gate only chose which command starts, while hundreds already running shared the CPU, so each of
/// them, the light database's too, became slow (measured: results-round3.md, noisy neighbours).
pub static SAT_MAX_INFLIGHT: AtomicU64 = AtomicU64::new(0);

/// CPUs the process may use, read once: `available_parallelism` reads the cgroup files at every call (tens of µs), and
/// the gate asked for it at every command start and finish while saturated: the heavy databases of the noisy test lost
/// about 45% of their throughput to it (docs/isolamento.md).
static CPUS: std::sync::LazyLock<u64> = std::sync::LazyLock::new(|| std::thread::available_parallelism().map(|n| n.get() as u64).unwrap_or(2));

fn sat_cap() -> u64 {
    match SAT_MAX_INFLIGHT.load(Relaxed) {
        0 => 8 * *CPUS,
        n => n,
    }
}
pub static MAX_INFLIGHT: AtomicU64 = AtomicU64::new(10_000);
pub static MAX_RATE: AtomicU64 = AtomicU64::new(100_000);
/// Exponential average of the scheduling lag (microseconds) and whether the gate is engaged.
pub static LAG_US: AtomicU64 = AtomicU64::new(0);
pub static LAG_MAX_US: AtomicU64 = AtomicU64::new(0);
static SATURATED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
pub static SATURATED_MS: AtomicU64 = AtomicU64::new(0);
pub static QUEUED_TOTAL: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------------------------------------------------
// The unit

/// Points per CPU ms, per KiB written, per KiB read, per S3 PUT-class request, per S3 GET-class request.
/// Derivation (docs/isolamento.md): 1 pt = about $2e-9, our cost of 0.1 ms of CPU on the shared server (estimated: a
/// 2-vCPU Graviton node at about $0.08 an hour used at 60%). A PUT costs $0.005 per 1000 and a GET $0.0004 per 1000 (S3
/// Standard list price): 2500 and 200 points. A KiB written costs its later rewrites by compaction (CPU outside the
/// command; the compaction's own S3 requests are charged as requests) and memtable memory: 0.5; a KiB read, the
/// block-cache memory it occupies: 0.05 (both estimated).
pub const DEFAULT_WEIGHTS: [f64; 5] = [10.0, 0.5, 0.05, 2500.0, 200.0];
static WEIGHTS: [AtomicU64; 5] = [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];

fn w(i: usize) -> f64 {
    f64::from_bits(WEIGHTS[i].load(Relaxed))
}

/// `--unit-weights cpu_ms,kib_written,kib_read,put,get` (empty = the defaults).
pub fn set_weights(s: &str) -> Result<(), String> {
    let v: Vec<f64> = if s.trim().is_empty() {
        DEFAULT_WEIGHTS.to_vec()
    } else {
        s.split(',').map(|x| x.trim().parse::<f64>().map_err(|e| format!("--unit-weights: {x}: {e}"))).collect::<Result<_, _>>()?
    };
    if v.len() != 5 || v.iter().any(|x| !x.is_finite() || *x < 0.0) {
        return Err("--unit-weights needs 5 non-negative numbers: cpu_ms,kib_written,kib_read,put,get".into());
    }
    for (i, x) in v.into_iter().enumerate() {
        WEIGHTS[i].store(x.to_bits(), Relaxed);
    }
    Ok(())
}

/// What one command used, collected while it runs (task-local, see [`timed`]).
#[derive(Default)]
pub struct OpCost {
    written: AtomicU64,
    read: AtomicU64,
    /// CPU spent for the command outside its own future (the JavaScript thread, see [`carry`]).
    cpu_extra_us: AtomicU64,
    /// The command's gate slot: given back as soon as the command waits for durability ([`io_wait`]), or when it ends.
    gate: Mutex<Option<GateSlot>>,
    db: String,
}

tokio::task_local! {
    static OP: Arc<OpCost>;
}

/// The running command now waits for its writes to become durable (S3, or the local log): it gives its slot in the
/// saturated gate to the next command, since it uses no CPU until then.
/// Then, under saturation, it waits for a place among the flushes (the least used databases first) and holds it while
/// its data is flushed: keep the returned slot until the wait for durability is over.
pub async fn io_wait() -> Option<GateSlot> {
    let (slot, db) = match OP.try_with(|c| (c.gate.lock().unwrap().take(), c.db.clone())) {
        Ok(x) => x,
        Err(_) => return None,
    };
    drop(slot);
    Some(enter(Kind::Flush, &db, false).await)
}

/// Run `f` (part of the current command, on another thread or task) with the command's cost record: what it reads and
/// writes, and the CPU time of its polls, are charged to the command.
pub fn carry<F: Future>(f: F) -> impl Future<Output = F::Output> {
    let cost = OP.try_with(Arc::clone).ok();
    async move {
        let inner = Carried { inner: Box::pin(f), cost: cost.clone() };
        match cost {
            Some(c) => OP.scope(c, inner).await,
            None => inner.await,
        }
    }
}

struct Carried<F> {
    inner: Pin<Box<F>>,
    cost: Option<Arc<OpCost>>,
}

impl<F: Future> Future for Carried<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let t = Instant::now();
        let r = self.inner.as_mut().poll(cx);
        if let Some(c) = &self.cost {
            c.cpu_extra_us.fetch_add(t.elapsed().as_micros() as u64, Relaxed);
        }
        r
    }
}

/// The running command read `bytes` (keys and values) from its database.
pub fn note_read(bytes: u64) {
    let _ = OP.try_with(|c| c.read.fetch_add(bytes, Relaxed));
}

fn pts_command(cpu_us: u64, written: u64, read: u64) -> (f64, f64, f64) {
    (w(0) * cpu_us as f64 / 1000.0, w(1) * written as f64 / 1024.0, w(2) * read as f64 / 1024.0)
}

// ---------------------------------------------------------------------------------------------------------------------
// Quotas

/// `--db-quota-pts` (points per second, 0 = no quota) and `--db-burst-pts` for databases without their own value.
static QUOTA_PTS: AtomicU64 = AtomicU64::new(0);
static BURST_PTS: AtomicU64 = AtomicU64::new(0);
/// `--db-quota-queue`: commands that may wait over the quota per database before new ones are refused.
pub static QUOTA_QUEUE: AtomicU64 = AtomicU64::new(256);
/// How long the gate stays engaged after the scheduling lag was last over the threshold.
const SAT_HOLD: Duration = Duration::from_millis(500);
/// Half-life of the decayed usage that orders the databases in the saturated queue.
pub const USAGE_HALF_LIFE_S: f64 = 1.0;
/// Smallest expected cost taken at the start of a command (points), so that a database whose commands are all tiny
/// cannot start an unbounded number of them on a bucket that is barely positive.
const MIN_EST: f64 = 0.5;

fn f(a: &AtomicU64) -> f64 {
    f64::from_bits(a.load(Relaxed))
}

/// Per-database quota set by the control plane: (points per second, burst points). Kept for databases not open yet.
static OVERRIDES: std::sync::LazyLock<Mutex<HashMap<String, (f64, f64)>>> = std::sync::LazyLock::new(Default::default);

fn default_quota() -> (f64, f64) {
    let r = f(&QUOTA_PTS);
    let b = f(&BURST_PTS);
    (r, if b > 0.0 { b } else { r * 10.0 })
}

fn quota_of(db: &str) -> (f64, f64) {
    OVERRIDES.lock().unwrap().get(db).copied().unwrap_or_else(default_quota)
}

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
    /// per-second counts and points of the last 60 seconds (ring)
    ring: [u32; 60],
    ring_pts: [f32; 60],
    ring_sec: [u64; 60],
    unflushed_bytes: u64,
    // accounting in the unit
    pts: f64,
    pts_cpu: f64,
    pts_written: f64,
    pts_read: f64,
    pts_s3: f64,
    bytes_written: u64,
    bytes_read: u64,
    s3_put: u64,
    s3_get: u64,
    // quota
    rate: f64,
    burst: f64,
    balance: f64,
    refilled: Instant,
    est: f64,
    quota_q: VecDeque<oneshot::Sender<()>>,
    quota_waits: u64,
    quota_wait_us: u64,
    quota_wait_max_us: u64,
    quota_rejections: u64,
    flush_waits: u64,
    flush_wait_us: u64,
    flush_wait_max_us: u64,
    /// A command opened or wrote the database (see [`register`]); false while it has counters only from S3 requests.
    registered: bool,
}

impl DbStat {
    fn new(db: &str) -> Self {
        let (rate, burst) = quota_of(db);
        DbStat {
            commands: 0, cpu_us: 0, queue_waits: 0, queue_wait_us: 0, queue_wait_max_us: 0, rejections: 0, inflight: 0, sec: 0, sec_count: 0,
            ring: [0; 60], ring_pts: [0.0; 60], ring_sec: [0; 60], unflushed_bytes: 0,
            pts: 0.0, pts_cpu: 0.0, pts_written: 0.0, pts_read: 0.0, pts_s3: 0.0, bytes_written: 0, bytes_read: 0, s3_put: 0, s3_get: 0,
            rate, burst, balance: burst, refilled: Instant::now(), est: MIN_EST, quota_q: VecDeque::new(), quota_waits: 0, quota_wait_us: 0,
            quota_wait_max_us: 0, quota_rejections: 0, flush_waits: 0, flush_wait_us: 0, flush_wait_max_us: 0, registered: false,
        }
    }

    fn slot(&mut self, sec: u64) -> usize {
        let i = (sec % 60) as usize;
        if self.ring_sec[i] != sec {
            self.ring_sec[i] = sec;
            self.ring[i] = 0;
            self.ring_pts[i] = 0.0;
        }
        i
    }

    fn refill(&mut self) {
        let now = Instant::now();
        if self.rate > 0.0 {
            let dt = now.duration_since(self.refilled).as_secs_f64();
            self.balance = (self.balance + dt * self.rate).min(self.burst.max(self.rate * 0.01));
        }
        self.refilled = now;
    }

    fn charge(&mut self, pts: f64) {
        self.pts += pts;
        let i = self.slot(now_sec());
        self.ring_pts[i] += pts as f32;
        if self.rate > 0.0 {
            self.refill();
            self.balance -= pts;
        }
    }
}

struct GateWaiter {
    /// Sends the generation of the flush slot the waiter shares (0 in the CPU gate).
    tx: oneshot::Sender<u64>,
}

/// Decayed point usage of a database and its weight in the saturated queue.
struct Usage {
    recent: f64,
    at: Instant,
    weight: f64,
}

impl Usage {
    fn decayed(&self, now: Instant) -> f64 {
        self.recent * (0.5f64).powf(now.duration_since(self.at).as_secs_f64() / USAGE_HALF_LIFE_S)
    }
}

#[derive(Default)]
struct Gate {
    inflight: u64,
    wakes: VecDeque<(String, GateWaiter)>,
    queues: HashMap<String, VecDeque<GateWaiter>>,
    /// Flush gate only: commands per database holding its one slot, and the slot's generation. Commands of a database
    /// whose data is being flushed share that flush (group commit), so they share its slot instead of filling the gate.
    /// The slot is given back when the database's WAL PUT starts ([`flush_started`]): the encoding (CPU) is done then,
    /// and the wait for S3 uses no CPU of the server; the commands still holding a share of that generation then give
    /// back nothing.
    held: HashMap<String, (u32, u64)>,
    /// Last flush slot generation given.
    gen: u64,
}

/// The two saturated gates: running commands (CPU), and flushes of written data that commands wait for (each flush is
/// CPU work in the background tasks of its database: encoding and sending the WAL object).
#[derive(Clone, Copy, PartialEq, Debug)]
enum Kind {
    Cpu,
    Flush,
}

fn gate(k: Kind) -> &'static Mutex<Gate> {
    match k {
        Kind::Cpu => &GATE,
        Kind::Flush => &FLUSH_GATE,
    }
}

fn cap(k: Kind) -> u64 {
    match k {
        Kind::Cpu => sat_cap(),
        Kind::Flush => flush_cap(),
    }
}

/// Databases whose writes may be flushed at once while the server is saturated (`--fair-max-flushing`; 0 = 4 per CPU).
pub static SAT_MAX_FLUSHING: AtomicU64 = AtomicU64::new(0);

fn flush_cap() -> u64 {
    match SAT_MAX_FLUSHING.load(Relaxed) {
        0 => 4 * *CPUS,
        n => n,
    }
}

// Locks: STATS, the gates and QUOTA_WAITING are never held together; OVERRIDES may be taken while STATS is held and
// USAGE while a gate is held (both innermost).
static STATS: std::sync::LazyLock<Mutex<HashMap<String, DbStat>>> = std::sync::LazyLock::new(Default::default);
static GATE: std::sync::LazyLock<Mutex<Gate>> = std::sync::LazyLock::new(Default::default);
static FLUSH_GATE: std::sync::LazyLock<Mutex<Gate>> = std::sync::LazyLock::new(Default::default);
static USAGE: std::sync::LazyLock<Mutex<HashMap<String, Usage>>> = std::sync::LazyLock::new(Default::default);
/// Databases with commands waiting over their quota (the ticker releases them as their buckets refill).
static QUOTA_WAITING: std::sync::LazyLock<Mutex<HashSet<String>>> = std::sync::LazyLock::new(Default::default);

fn now_sec() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
}

fn weight_of(rate: f64) -> f64 {
    if rate > 0.0 {
        rate
    } else {
        f(&QUOTA_PTS).max(1.0)
    }
}

/// Add `pts` to the decayed usage of `db` (orders the saturated queue).
fn note_usage(db: &str, pts: f64, rate: f64) {
    let now = Instant::now();
    let mut g = USAGE.lock().unwrap();
    let u = g.entry(db.to_string()).or_insert(Usage { recent: 0.0, at: now, weight: weight_of(rate) });
    u.recent = u.decayed(now) + pts;
    u.at = now;
    u.weight = weight_of(rate);
}

/// Charge `pts` to a database (accounting and quota). Returns its quota rate for [`note_usage`].
fn charge(db: &str, pts: f64, part: impl FnOnce(&mut DbStat)) -> Option<f64> {
    let mut st = STATS.lock().unwrap();
    let s = st.get_mut(db)?;
    part(s);
    s.charge(pts);
    Some(s.rate)
}

/// An S3 request on `path`. Who pays (review of S4, error 3): the database of the running command when the request is
/// made inside a command (the task-local `OP`); else the database whose own store makes it (`counting::owned`: its
/// writer, reader and compactor; a branch made with fork "clone" reads its parent's objects `tenants/{parent}/...`, and
/// those reads are the branch's); else, for the rest of the background work (lease renewals, cleanup), the database
/// whose objects it touches, `tenants/{db}/...`. Counted even if this process has not opened that database (it gets its
/// counters).
fn on_s3_request(path: &str, put: bool) {
    let by_path = path.strip_prefix("tenants/").map(|rest| rest.split('/').next().unwrap_or("")).filter(|d| !d.is_empty());
    if put && path.contains("/wal/") {
        if let Some(db) = by_path {
            flush_started(db);
        }
    }
    let pts = if put { w(3) } else { w(4) };
    let part = |s: &mut DbStat| {
        s.pts_s3 += pts;
        if put {
            s.s3_put += 1;
        } else {
            s.s3_get += 1;
        }
    };
    // the command that runs (when the request is made in its task), else the database whose store makes the request
    // (counting::owned: SlateDB makes most of its reads in tasks of its own)
    let by_command = OP.try_with(|c| c.db.clone()).ok();
    let by_store = tenant_bench::counting::request_owner();
    for db in [by_command.as_deref(), by_store.as_deref()].into_iter().flatten() {
        if let Some(rate) = charge(db, pts, part) {
            note_usage(db, pts, rate);
            return;
        }
    }
    let Some(db) = by_store.as_deref().or(by_path) else { return };
    let rate = {
        let mut st = STATS.lock().unwrap();
        let s = st.entry(db.to_string()).or_insert_with(|| DbStat::new(db));
        part(s);
        s.charge(pts);
        s.rate
    };
    note_usage(db, pts, rate);
}

#[allow(clippy::too_many_arguments)]
pub fn install(lag_ms: f64, max_inflight: u64, max_rate: u64, sat_max_inflight: u64, sat_max_flushing: u64, quota_pts: f64, burst_pts: f64, quota_queue: u64, weights: &str) -> Result<(), String> {
    set_weights(weights)?;
    LAG_THRESHOLD_US.store((lag_ms * 1000.0) as u64, Relaxed);
    SAT_MAX_INFLIGHT.store(sat_max_inflight, Relaxed);
    SAT_MAX_FLUSHING.store(sat_max_flushing, Relaxed);
    MAX_INFLIGHT.store(max_inflight, Relaxed);
    MAX_RATE.store(max_rate, Relaxed);
    QUOTA_PTS.store(quota_pts.max(0.0).to_bits(), Relaxed);
    BURST_PTS.store(burst_pts.max(0.0).to_bits(), Relaxed);
    QUOTA_QUEUE.store(quota_queue, Relaxed);
    let _ = tenant_bench::counting::ON_REQUEST.set(on_s3_request);
    tokio::spawn(async {
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last = Instant::now();
        let mut over_at: Option<Instant> = None;
        loop {
            tick.tick().await;
            let now = Instant::now();
            let lag = now.duration_since(last).as_micros().saturating_sub(10_000) as u64;
            last = now;
            let old = LAG_US.load(Relaxed);
            // a short average (the last ~4 ticks): the gate engages within a few ticks of saturation, and stays
            // engaged HOLD after the lag was last over the threshold, so it does not flap on every quiet tick
            let avg = (old * 3 + lag) / 4;
            LAG_US.store(avg, Relaxed);
            LAG_MAX_US.fetch_max(lag, Relaxed);
            let thr = LAG_THRESHOLD_US.load(Relaxed);
            if thr > 0 && avg > thr {
                over_at = Some(now);
            }
            let sat = thr > 0 && over_at.map(|t| now.duration_since(t) < SAT_HOLD).unwrap_or(false);
            if sat {
                SATURATED_MS.fetch_add(10, Relaxed);
            }
            if SATURATED.swap(sat, Relaxed) && !sat {
                // saturation is over: start everything that waits, wakes first, then by usage
                for k in [Kind::Cpu, Kind::Flush] {
                    let mut g = gate(k).lock().unwrap();
                    while start_next(&mut g, k) {}
                }
            }
            release_quota_waiters();
        }
    });
    Ok(())
}

/// Start the commands waiting over their quota whose database's bucket has refilled.
fn release_quota_waiters() {
    let dbs: Vec<String> = QUOTA_WAITING.lock().unwrap().iter().cloned().collect();
    if dbs.is_empty() {
        return;
    }
    let mut done = Vec::new();
    {
        let mut st = STATS.lock().unwrap();
        for db in &dbs {
            let Some(s) = st.get_mut(db) else {
                done.push(db.clone());
                continue;
            };
            s.refill();
            // each released command takes its expected cost when it wakes; count them here so one refill does not
            // release the whole queue
            let mut budget = s.balance;
            while budget > 0.0 || s.rate <= 0.0 {
                let Some(tx) = s.quota_q.pop_front() else { break };
                if tx.send(()).is_ok() {
                    budget -= s.est.max(MIN_EST);
                }
            }
            if s.quota_q.is_empty() {
                done.push(db.clone());
            }
        }
    }
    if !done.is_empty() {
        let mut w = QUOTA_WAITING.lock().unwrap();
        for d in done {
            w.remove(&d);
        }
    }
}

/// The next command to start in the saturated gate: a wake first, then the queued database with the least decayed usage
/// per weight.
fn next_waiter(g: &mut Gate) -> Option<GateWaiter> {
    next_waiter_db(g).map(|(_, w)| w)
}

fn next_waiter_db(g: &mut Gate) -> Option<(String, GateWaiter)> {
    if let Some(x) = g.wakes.pop_front() {
        return Some(x);
    }
    let now = Instant::now();
    let usage = USAGE.lock().unwrap();
    let db = g
        .queues
        .keys()
        .map(|d| (d, usage.get(d).map(|u| u.decayed(now) / u.weight).unwrap_or(0.0)))
        .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(d, _)| d.clone())?;
    let q = g.queues.get_mut(&db)?;
    let w = q.pop_front();
    if q.is_empty() {
        g.queues.remove(&db);
    }
    w.map(|w| (db, w))
}

/// Start the next waiter of gate `k` (in the flush gate, every waiting command of that database, sharing one slot).
/// False when nobody waits.
fn start_next(g: &mut Gate, k: Kind) -> bool {
    loop {
        let Some((db, w)) = next_waiter_db(g) else { return false };
        let gen = if k == Kind::Flush {
            g.gen += 1;
            g.gen
        } else {
            0
        };
        let mut n = u32::from(w.tx.send(gen).is_ok());
        if k == Kind::Flush {
            for w in g.queues.remove(&db).unwrap_or_default() {
                n += u32::from(w.tx.send(gen).is_ok());
            }
            if n > 0 {
                g.held.insert(db, (n, gen));
            }
        }
        if n > 0 {
            g.inflight += 1;
            return true;
        }
    }
}

/// A command's place in its database's counters (inflight, expected cost); released on drop.
struct DbSlot {
    db: String,
    counted: bool,
    est: f64,
}

impl Drop for DbSlot {
    fn drop(&mut self) {
        if self.counted {
            if let Some(s) = STATS.lock().unwrap().get_mut(&self.db) {
                s.inflight = s.inflight.saturating_sub(1);
                if s.rate > 0.0 {
                    // the real cost was charged when the command finished (Timed); give back the expected one
                    s.balance += self.est;
                }
            }
        }
    }
}

/// A place in a gate (in the flush gate, a share of its database's slot, with the slot's generation); dropping it lets
/// the next queued command start.
pub struct GateSlot(Kind, Option<(String, u64)>);

fn release_slot(k: Kind, db: Option<(&str, u64)>) {
    let mut g = gate(k).lock().unwrap();
    if let Some((db, gen)) = db {
        let left = match g.held.get_mut(db) {
            Some((n, g_gen)) if *g_gen == gen => {
                *n = n.saturating_sub(1);
                *n
            }
            // the slot of this generation was already given back when the database's WAL PUT started
            _ => return,
        };
        if left > 0 {
            return;
        }
        g.held.remove(db);
    }
    g.inflight = g.inflight.saturating_sub(1);
    fill(&mut g, k);
}

/// Under saturation, start queued commands up to the cap (wakes first, then by usage); otherwise all of them.
fn fill(g: &mut Gate, k: Kind) {
    while !SATURATED.load(Relaxed) || g.inflight < cap(k) {
        if !start_next(g, k) {
            break;
        }
    }
}

/// The WAL PUT of `db` starts: the data is encoded, what is left is the wait for S3, which uses no CPU of the server.
/// Its flush slot goes back now (not when S3 answers), so the flushes of other databases are not queued behind the
/// network (review of S4, error 1).
pub fn flush_started(db: &str) {
    let mut g = FLUSH_GATE.lock().unwrap();
    if g.held.remove(db).is_some() {
        g.inflight = g.inflight.saturating_sub(1);
        fill(&mut g, Kind::Flush);
    }
}

impl Drop for GateSlot {
    fn drop(&mut self) {
        release_slot(self.0, self.1.as_ref().map(|(d, g)| (d.as_str(), *g)));
    }
}

/// Waiting for a gate slot; if the command is dropped after the slot was given but before it saw it, the slot goes back.
struct GateWait {
    rx: Option<oneshot::Receiver<u64>>,
    kind: Kind,
    db: Option<String>,
}

impl Drop for GateWait {
    fn drop(&mut self) {
        if let Some(mut rx) = self.rx.take() {
            if let Ok(gen) = rx.try_recv() {
                release_slot(self.kind, self.db.as_deref().map(|d| (d, gen)));
            }
        }
    }
}

/// Take a slot in gate `k` for a command of `db`: at once unless the server is saturated and the gate is full.
async fn enter(k: Kind, db: &str, wakes: bool) -> GateSlot {
    let share = (k == Kind::Flush).then(|| db.to_string());
    let rx = {
        let mut g = gate(k).lock().unwrap();
        if let Some((n, gen)) = share.as_ref().and_then(|d| g.held.get_mut(d)) {
            *n += 1;
            let gen = *gen;
            return GateSlot(k, share.map(|d| (d, gen)));
        }
        if !SATURATED.load(Relaxed) || g.inflight < cap(k) {
            g.inflight += 1;
            let mut gen = 0;
            if let Some(d) = &share {
                g.gen += 1;
                gen = g.gen;
                g.held.insert(d.clone(), (1, gen));
            }
            return GateSlot(k, share.map(|d| (d, gen)));
        }
        let (tx, rx) = oneshot::channel();
        if wakes {
            g.wakes.push_back((db.to_string(), GateWaiter { tx }));
        } else {
            g.queues.entry(db.to_string()).or_default().push_back(GateWaiter { tx });
        }
        rx
    };
    QUEUED_TOTAL.fetch_add(1, Relaxed);
    let t = Instant::now();
    let mut wait = GateWait { rx: Some(rx), kind: k, db: share.clone() };
    let gen = wait.rx.as_mut().unwrap().await.unwrap_or(0);
    wait.rx = None;
    let us = t.elapsed().as_micros() as u64;
    if let Some(s) = STATS.lock().unwrap().get_mut(db) {
        if k == Kind::Cpu {
            s.queue_waits += 1;
            s.queue_wait_us += us;
            s.queue_wait_max_us = s.queue_wait_max_us.max(us);
        } else {
            s.flush_waits += 1;
            s.flush_wait_us += us;
            s.flush_wait_max_us = s.flush_wait_max_us.max(us);
        }
    }
    GateSlot(k, share.map(|d| (d, gen)))
}

/// The places a command holds while it runs: its database's counters and a gate slot (moved into the command's cost
/// record by [`timed`], so that [`io_wait`] can give it back early).
pub struct Permit {
    gate: Option<GateSlot>,
    _db: DbSlot,
}

/// The safety limit a command broke, as a MongoDB error reply.
pub fn rejection(db: &str, what: &str, limit: u64) -> Document {
    doc! { "ok": 0.0, "code": 462, "codeName": "IngressRequestRateLimitExceeded",
        "errmsg": format!("database {db} is over the server's safety limit of {limit} {what}; this limit exists only against runaway clients (a loop sending commands without end); contact support if your application really needs more"),
        "errorLabels": ["SystemOverloadedError"] }
}

/// The database is over its quota and too many of its commands already wait: retryable.
pub fn quota_rejection(db: &str, rate: f64, queued: u64) -> Document {
    doc! { "ok": 0.0, "code": 462, "codeName": "IngressRequestRateLimitExceeded",
        "errmsg": format!("database {db} is over its quota of {rate:.0} points per second and {queued} of its commands are already waiting; retry later or ask for a larger quota"),
        "errorLabels": ["SystemOverloadedError", "RetryableError"] }
}

/// Take a place for a command of `db` (`wakes`: it will wake a sleeping database). Err: a limit, with the reply.
pub async fn admit(db: &str, wakes: bool) -> Result<Permit, Document> {
    // Per-database counters and limits exist only for databases this process opened or wrote (`register`), so a client
    // naming arbitrary databases cannot grow this map; such commands still pass through the gate below.
    let mut slot = DbSlot { db: db.to_string(), counted: false, est: 0.0 };
    let quota_rx = {
        let mut st = STATS.lock().unwrap();
        match st.get_mut(db) {
            None => None,
            Some(s) => {
                let sec = now_sec();
                if s.sec != sec {
                    s.sec = sec;
                    s.sec_count = 0;
                }
                s.sec_count += 1;
                let i = s.slot(sec);
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
                let mut rx = None;
                if s.rate > 0.0 {
                    s.refill();
                    if s.balance <= 0.0 || !s.quota_q.is_empty() {
                        let max_q = QUOTA_QUEUE.load(Relaxed);
                        if s.quota_q.len() as u64 >= max_q {
                            s.quota_rejections += 1;
                            return Err(quota_rejection(db, s.rate, max_q));
                        }
                        let (tx, r) = oneshot::channel();
                        s.quota_q.push_back(tx);
                        rx = Some(r);
                    } else {
                        slot.est = s.est.max(MIN_EST);
                        s.balance -= slot.est;
                    }
                }
                s.inflight += 1;
                s.commands += 1;
                // counted here: `register` must not count this command again
                s.registered = true;
                slot.counted = true;
                rx
            }
        }
    };
    if let Some(rx) = quota_rx {
        QUOTA_WAITING.lock().unwrap().insert(db.to_string());
        let t = Instant::now();
        let _ = rx.await;
        let us = t.elapsed().as_micros() as u64;
        if let Some(s) = STATS.lock().unwrap().get_mut(db) {
            s.quota_waits += 1;
            s.quota_wait_us += us;
            s.quota_wait_max_us = s.quota_wait_max_us.max(us);
            if s.rate > 0.0 {
                slot.est = s.est.max(MIN_EST);
                s.refill();
                s.balance -= slot.est;
            }
        }
    }
    let gate = enter(Kind::Cpu, db, wakes).await;
    Ok(Permit { gate: Some(gate), _db: slot })
}

/// The database exists (it was opened or written): from now on it has counters, limits, a quota and metrics.
pub fn register(db: &str) {
    registered(&mut STATS.lock().unwrap(), db);
}

/// The counters of `db`, made now if missing (see [`first_stat`]). Counters made earlier only by its S3 requests (the
/// opening or creating command's own requests come before its database is registered) count that command now.
fn registered<'a>(st: &'a mut HashMap<String, DbStat>, db: &str) -> &'a mut DbStat {
    let s = st.entry(db.to_string()).or_insert_with(|| first_stat(db));
    if !s.registered {
        s.registered = true;
        s.commands += 1;
    }
    s
}

/// The counters of a database seen for the first time: the command that opened or created it was admitted before they
/// existed, so it is counted here.
fn first_stat(db: &str) -> DbStat {
    let mut s = DbStat::new(db);
    s.commands = 1;
    s.registered = true;
    s
}

/// The database was dropped or erased: its counters go (a quota set by the control plane stays until it is cleared).
pub fn forget(db: &str) {
    if let Some(s) = STATS.lock().unwrap().remove(db) {
        for tx in s.quota_q {
            let _ = tx.send(());
        }
    }
    USAGE.lock().unwrap().remove(db);
}

/// Bytes a database wrote since it was opened (reset at close). Its memtable holds at most `--memtable-max-mb` of them.
/// Also the bytes the running command wrote (its cost).
pub fn note_written(db: &str, bytes: u64, reset: bool) {
    let _ = OP.try_with(|c| c.written.fetch_add(bytes, Relaxed));
    let mut st = STATS.lock().unwrap();
    let s = registered(&mut st, db);
    if reset {
        s.unflushed_bytes = 0;
    }
    s.unflushed_bytes += bytes;
}

/// Set (`Some`) or clear (`None`) a database's own quota: points per second (0 = none) and burst points (0 = 10 s of
/// quota). Returns the quota now in force.
pub fn set_quota(db: &str, q: Option<(f64, f64)>) -> serde_json::Value {
    match q {
        Some((r, b)) => {
            let b = if b > 0.0 { b } else { r * 10.0 };
            OVERRIDES.lock().unwrap().insert(db.to_string(), (r.max(0.0), b.max(0.0)));
        }
        None => {
            OVERRIDES.lock().unwrap().remove(db);
        }
    }
    let (rate, burst) = quota_of(db);
    let mut waiting = false;
    if let Some(s) = STATS.lock().unwrap().get_mut(db) {
        s.refill();
        s.rate = rate;
        s.burst = burst;
        s.balance = s.balance.min(burst.max(rate * 0.01));
        if rate <= 0.0 {
            s.balance = 0.0;
        }
        waiting = !s.quota_q.is_empty();
    }
    if waiting {
        QUOTA_WAITING.lock().unwrap().insert(db.to_string());
    }
    quota_json(db)
}

pub fn quota_json(db: &str) -> serde_json::Value {
    let (rate, burst) = quota_of(db);
    let own = OVERRIDES.lock().unwrap().contains_key(db);
    let balance = STATS.lock().unwrap().get_mut(db).map(|s| {
        s.refill();
        s.balance
    });
    serde_json::json!({ "database": db, "pts_per_sec": rate, "burst_pts": burst, "own": own, "balance_pts": balance })
}

/// A future whose time inside `poll` is added to its database's CPU time, and whose cost in points (CPU, bytes written,
/// bytes read) is charged to its database when it finishes.
pub struct Timed<F> {
    inner: Pin<Box<F>>,
    db: String,
    us: u64,
    cost: Arc<OpCost>,
}

impl<F: Future> Future for Timed<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let t = Instant::now();
        let r = self.inner.as_mut().poll(cx);
        self.us += t.elapsed().as_micros() as u64;
        if r.is_ready() {
            let (us, written, read) = (self.us + self.cost.cpu_extra_us.load(Relaxed), self.cost.written.load(Relaxed), self.cost.read.load(Relaxed));
            let (pc, pw, pr) = pts_command(us, written, read);
            let pts = pc + pw + pr;
            let mut rate = None;
            if let Some(s) = STATS.lock().unwrap().get_mut(&self.db) {
                s.cpu_us += us;
                s.bytes_written += written;
                s.bytes_read += read;
                s.pts_cpu += pc;
                s.pts_written += pw;
                s.pts_read += pr;
                s.charge(pts);
                // expected cost of the next command of this database: moving average of the last ones
                s.est = s.est * 0.9 + pts * 0.1;
                rate = Some(s.rate);
            }
            if let Some(rate) = rate {
                note_usage(&self.db, pts, rate);
            }
        }
        r
    }
}

pub fn timed<F: Future>(db: &str, permit: Option<&mut Permit>, f: F) -> Timed<tokio::task::futures::TaskLocalFuture<Arc<OpCost>, F>> {
    let cost = Arc::new(OpCost { gate: Mutex::new(permit.and_then(|p| p.gate.take())), db: db.to_string(), ..Default::default() });
    Timed { inner: Box::pin(OP.scope(Arc::clone(&cost), f)), db: db.to_string(), us: 0, cost }
}

/// `GET /v1/metrics/databases`
pub fn metrics(open: impl Fn(&str) -> String) -> serde_json::Value {
    let mut st = STATS.lock().unwrap();
    let sec = now_sec();
    let mut dbs = serde_json::Map::new();
    for (db, s) in st.iter_mut() {
        s.refill();
        let last = |n: u64| -> u64 { (0..n).map(|k| sec - k).filter(|t| s.ring_sec[(t % 60) as usize] == *t).map(|t| s.ring[(t % 60) as usize] as u64).sum() };
        let last_pts = |n: u64| -> f64 { (0..n).map(|k| sec - k).filter(|t| s.ring_sec[(t % 60) as usize] == *t).map(|t| s.ring_pts[(t % 60) as usize] as f64).sum() };
        dbs.insert(db.clone(), serde_json::json!({
            "commands": s.commands, "commandsPerSec10s": last(10) as f64 / 10.0, "commandsPerSec60s": last(60) as f64 / 60.0,
            "cpuMs": s.cpu_us / 1000, "queueWaits": s.queue_waits, "queueWaitMs": s.queue_wait_us / 1000, "queueWaitMaxMs": s.queue_wait_max_us / 1000,
            "safetyRejections": s.rejections, "inflight": s.inflight, "bytesWrittenSinceOpen": s.unflushed_bytes, "memtableHoldsAtMostBytes": s.unflushed_bytes.min(tenant_bench::lifecycle::MEMTABLE_MAX_BYTES.load(Relaxed)), "state": open(db),
            "points": s.pts, "pointsPerSec10s": last_pts(10) / 10.0, "pointsPerSec60s": last_pts(60) / 60.0,
            "pointsCpu": s.pts_cpu, "pointsWritten": s.pts_written, "pointsRead": s.pts_read, "pointsS3": s.pts_s3,
            "bytesWritten": s.bytes_written, "bytesRead": s.bytes_read, "s3Put": s.s3_put, "s3Get": s.s3_get, "cpuUs": s.cpu_us,
            "quotaPtsPerSec": s.rate, "burstPts": s.burst, "balancePts": if s.rate > 0.0 { Some(s.balance) } else { None }, "expectedPtsPerCommand": s.est,
            "quotaQueued": s.quota_q.len(), "quotaWaits": s.quota_waits, "quotaWaitMs": s.quota_wait_us / 1000, "quotaWaitMaxMs": s.quota_wait_max_us / 1000, "quotaRejections": s.quota_rejections,
            "flushWaits": s.flush_waits, "flushWaitMs": s.flush_wait_us / 1000, "flushWaitMaxMs": s.flush_wait_max_us / 1000,
        }));
    }
    let (qr, qb) = default_quota();
    serde_json::json!({
        "server": { "schedulingLagMs": LAG_US.load(Relaxed) as f64 / 1000.0, "schedulingLagMaxMs": LAG_MAX_US.load(Relaxed) as f64 / 1000.0,
                    "saturated": SATURATED.load(Relaxed), "saturatedMs": SATURATED_MS.load(Relaxed), "queuedCommands": QUEUED_TOTAL.load(Relaxed),
                    "inflight": GATE.lock().unwrap().inflight, "fairLagThresholdMs": LAG_THRESHOLD_US.load(Relaxed) as f64 / 1000.0, "fairMaxInflightWhenSaturated": sat_cap(), "fairMaxFlushingWhenSaturated": flush_cap(), "flushing": FLUSH_GATE.lock().unwrap().inflight,
                    "dbMaxInflight": MAX_INFLIGHT.load(Relaxed), "dbMaxRate": MAX_RATE.load(Relaxed),
                    "unitWeights": { "ptsPerCpuMs": w(0), "ptsPerKiBWritten": w(1), "ptsPerKiBRead": w(2), "ptsPerS3Put": w(3), "ptsPerS3Get": w(4) },
                    "defaultQuotaPtsPerSec": qr, "defaultBurstPts": qb, "quotaQueueMax": QUOTA_QUEUE.load(Relaxed) },
        "databases": dbs,
    })
}

/// Commands that are not queued: cheap, or that wait on purpose (monitoring, authentication). Their cost is still
/// charged to their database. `getMore` is not here: it is queued unless its cursor waits on purpose (cmd.rs).
pub fn exempt(name: &str) -> bool {
    matches!(name, "hello" | "isMaster" | "ismaster" | "ping" | "killCursors" | "endSessions" | "saslStart" | "saslContinue" | "buildInfo" | "buildinfo" | "getLastError" | "logout" | "authenticate")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weights_parse() {
        assert!(set_weights("1,2,3").is_err());
        assert!(set_weights("1,2,3,4,-1").is_err());
        set_weights("").unwrap();
        assert_eq!(w(3), DEFAULT_WEIGHTS[3]);
    }

    #[test]
    fn usage_decays_by_half_life() {
        let now = Instant::now();
        let u = Usage { recent: 100.0, at: now - Duration::from_secs_f64(USAGE_HALF_LIFE_S), weight: 1.0 };
        let d = u.decayed(now);
        assert!((d - 50.0).abs() < 1.0, "{d}");
    }

    #[test]
    fn bucket_refills_up_to_burst() {
        let mut s = DbStat::new("x");
        s.rate = 1000.0;
        s.burst = 500.0;
        s.balance = -100.0;
        s.refilled = Instant::now() - Duration::from_millis(100);
        s.refill();
        assert!(s.balance > -1.0 && s.balance < 10.0, "{}", s.balance);
        s.refilled = Instant::now() - Duration::from_secs(10);
        s.refill();
        assert_eq!(s.balance, 500.0);
    }

    #[test]
    fn least_used_database_goes_first() {
        let mut g = Gate::default();
        let now = Instant::now();
        let (t1, _r1) = oneshot::channel();
        let (t2, mut r2) = oneshot::channel();
        g.queues.entry("heavy".into()).or_default().push_back(GateWaiter { tx: t1 });
        g.queues.entry("light".into()).or_default().push_back(GateWaiter { tx: t2 });
        USAGE.lock().unwrap().insert("heavy".into(), Usage { recent: 1000.0, at: now, weight: 1.0 });
        USAGE.lock().unwrap().insert("light".into(), Usage { recent: 1.0, at: now, weight: 1.0 });
        let w = next_waiter(&mut g).unwrap();
        w.tx.send(0).unwrap();
        assert!(r2.try_recv().is_ok(), "the light database was not served first");
    }
}
