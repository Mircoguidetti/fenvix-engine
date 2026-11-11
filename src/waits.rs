//! Where a command's time goes (perf-latency, 2026-11-11): every command handled by the server runs inside a [`WAITS`]
//! scope, and the places it can wait add their time to it (lock waits, the log's acknowledgement, WAL flushes, store
//! calls, a manifest refresh, ...). A command slower than `FX_WAITS_MS` (default 50) prints one `WAITS` line with its
//! breakdown; `WAITS_STALL` lines say when the runtime's worker threads were busy and could not poll a ready task
//! (a probe task that sleeps 1 ms and measures how late it wakes), and `WAITS_POLL` lines name a background job whose
//! single poll ran longer than 10 ms (a poll that long keeps every other task of that worker waiting).
//!
//! Off unless `FX_WAITS` is set: the cost when off is one task-local lookup per instrumented place.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub enum W {
    /// the fair queue's admission (fair.rs)
    Admit,
    /// waiting for a database's exclusive lock
    LockW,
    /// waiting for a database's shared lock
    LockR,
    /// opening, a manifest refresh or a compactor version install under the exclusive lock
    Refresh,
    /// staging a batch into SlateDB (memtable write; SlateDB's backpressure shows here)
    Stage,
    /// the snapshot taken before staging (the confirmed state readers see)
    StageSnap,
    /// the replicated log's acknowledgement of a batch
    LogAck,
    /// waiting until every earlier batch is confirmed (visibility)
    Visible,
    /// the S3-WAL fallback when the log did not confirm
    Fallback,
    /// group commit leader lock and WAL flush (log off)
    WalFlush,
    /// the fair queue's io gate
    IoWait,
    /// object-store GET in this task
    Get,
    /// object-store PUT in this task
    Put,
    /// object-store LIST / HEAD / DELETE in this task
    Other,
    /// starting the database's compactor (first write)
    CompStart,
    /// SlateDB point read or scan setup (the writer's or the reader's, including its block fetches in this task)
    SlateRead,
    /// the merged reader's sealed-run part (fx view: index, blocks)
    FxRead,
}

pub const N: usize = 17;
pub const NAMES: [&str; N] = [
    "admit", "lock_w", "lock_r", "refresh", "stage", "stage_snap", "log_ack", "visible", "fallback", "wal_flush", "io_wait", "get", "put",
    "store_other", "comp_start", "read_get", "read_scan",
];

pub static ON: AtomicBool = AtomicBool::new(false);
pub static SLOW_MS: AtomicU64 = AtomicU64::new(50);

#[derive(Default)]
pub struct Waits {
    pub ns: [AtomicU64; N],
    pub n: [AtomicU64; N],
    pub bytes_get: AtomicU64,
    /// the command's OpMeter (gets, bytes, micros), including block reads in tasks SlateDB spawns
    pub meter: [AtomicU64; 3],
}

tokio::task_local! {
    pub static WAITS: Arc<Waits>;
}

/// Add `d` to category `w` of the current command, if there is one.
pub fn add(w: W, d: Duration) {
    if !ON.load(Relaxed) {
        return;
    }
    let _ = WAITS.try_with(|x| {
        x.ns[w as usize].fetch_add(d.as_nanos() as u64, Relaxed);
        x.n[w as usize].fetch_add(1, Relaxed);
    });
}

pub fn add_get_bytes(b: u64) {
    if !ON.load(Relaxed) {
        return;
    }
    let _ = WAITS.try_with(|x| x.bytes_get.fetch_add(b, Relaxed));
}

/// Time `f` into category `w`.
pub async fn timed<T>(w: W, f: impl Future<Output = T>) -> T {
    if !ON.load(Relaxed) {
        return f.await;
    }
    let t = Instant::now();
    let r = f.await;
    add(w, t.elapsed());
    r
}

static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

/// Milliseconds since the process started the tracer (the time base of every line).
pub fn now_ms() -> f64 {
    START.get_or_init(Instant::now).elapsed().as_secs_f64() * 1000.0
}

/// One command's line, when it was slow. `start_ms` is [`now_ms`] at its start.
pub fn report(cmd: &str, coll: &str, start_ms: f64, total: Duration, x: &Waits, meter: (u64, u64, u64)) {
    let ms = total.as_secs_f64() * 1000.0;
    if !ON.load(Relaxed) || ms < SLOW_MS.load(Relaxed) as f64 {
        return;
    }
    let mut s = format!("WAITS t={start_ms:.1} ms={ms:.1} cmd={cmd} coll={coll}");
    let mut sum = 0u64;
    for i in 0..N {
        let ns = x.ns[i].load(Relaxed);
        if ns > 0 {
            s.push_str(&format!(" {}={:.1}/{}", NAMES[i], ns as f64 / 1e6, x.n[i].load(Relaxed)));
        }
        // the store calls and SlateDB reads overlap the read categories they happen inside: not summed
        if !matches!(i, 11..=13) {
            sum += ns;
        }
    }
    let (g, b, us) = meter;
    s.push_str(&format!(" meter_gets={g} meter_bytes={b} meter_ms={:.1} rest={:.1}", us as f64 / 1e3, ms - sum as f64 / 1e6));
    eprintln!("{s}");
}

/// Start the stall probe: a task on the server's runtime that sleeps 1 ms and prints how late it woke when it was more
/// than 10 ms late (the worker it runs on, or all of them, were busy polling something else).
pub fn spawn_probe() {
    let _ = START.get_or_init(Instant::now);
    tokio::spawn(async {
        loop {
            let t = Instant::now();
            tokio::time::sleep(Duration::from_millis(1)).await;
            let late = t.elapsed().as_secs_f64() * 1000.0 - 1.0;
            if late >= 10.0 {
                eprintln!("WAITS_STALL t={:.1} ms={late:.1}", now_ms() - late - 1.0);
            }
        }
    });
}

/// A background job's future whose polls are timed: a poll longer than 10 ms prints a `WAITS_POLL` line with `what`, and
/// the job's start and end print `WAITS_JOB` lines.
pub struct Polled<F> {
    f: Pin<Box<F>>,
    what: &'static str,
    who: String,
    t0: Option<f64>,
    polls_ms: f64,
    max_ms: f64,
}

pub fn polled<F: Future>(what: &'static str, who: String, f: F) -> Polled<F> {
    Polled { f: Box::pin(f), what, who, t0: None, polls_ms: 0.0, max_ms: 0.0 }
}

impl<F: Future> Future for Polled<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        if !ON.load(Relaxed) {
            return self.f.as_mut().poll(cx);
        }
        let start = now_ms();
        if self.t0.is_none() {
            self.t0 = Some(start);
        }
        let t = Instant::now();
        let r = self.f.as_mut().poll(cx);
        let d = t.elapsed().as_secs_f64() * 1000.0;
        self.polls_ms += d;
        if d > self.max_ms {
            self.max_ms = d;
        }
        if d >= 10.0 {
            eprintln!("WAITS_POLL t={start:.1} ms={d:.1} job={} db={}", self.what, self.who);
        }
        if r.is_ready() {
            let t0 = self.t0.unwrap_or(start);
            eprintln!("WAITS_JOB t={t0:.1} ms={:.1} cpu_polls_ms={:.1} max_poll_ms={:.1} job={} db={}", now_ms() - t0, self.polls_ms, self.max_ms, self.what, self.who);
        }
        r
    }
}
