//! The compaction pool of a cell (plan round 2, fix 7): one per process, shared by every database, at most `slots` jobs
//! at once (about 70% of the cores), jobs taken by priority.
//!
//! Priorities, highest first:
//! 0. absorption (and SlateDB's fold, which SlateDB's own compactor runs; it is never deferred): L0 must stay at or below
//!    8, or the writer stops. Absorption never waits for a slot: it runs at once, and counts as busy.
//! 1. erasures (24-hour deadline);
//! 2. delta tier merges;
//! 3. reseals (theta, R3, R2), which can wait;
//! 4. GC.
//!
//! Within one priority, first come first served. The pool bounds how many jobs run, not how much CPU each takes: the
//! bench runs the jobs on a runtime of `slots` threads, so the cap is also a CPU cap there (`bench`).

use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub enum Prio {
    Absorb = 0,
    Erase = 1,
    Merge = 2,
    Reseal = 3,
    Gc = 4,
}

pub const PRIOS: [Prio; 5] = [Prio::Absorb, Prio::Erase, Prio::Merge, Prio::Reseal, Prio::Gc];

struct Waiter {
    prio: Prio,
    seq: u64,
    tx: tokio::sync::oneshot::Sender<()>,
}

impl PartialEq for Waiter {
    fn eq(&self, o: &Self) -> bool {
        self.prio == o.prio && self.seq == o.seq
    }
}
impl Eq for Waiter {}
impl PartialOrd for Waiter {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Waiter {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        // BinaryHeap is a max-heap: the lowest priority number and then the lowest seq first
        o.prio.cmp(&self.prio).then(o.seq.cmp(&self.seq))
    }
}

#[derive(Default)]
struct State {
    busy: usize,
    seq: u64,
    waiting: BinaryHeap<Waiter>,
}

/// Counters per priority: jobs run, time waited for a slot, time held.
#[derive(Default)]
pub struct PrioStats {
    pub jobs: AtomicU64,
    pub wait_us: AtomicU64,
    pub max_wait_us: AtomicU64,
    pub held_us: AtomicU64,
}

pub struct Pool {
    slots: usize,
    st: Mutex<State>,
    pub stats: [PrioStats; 5],
    /// Highest number of jobs running at once.
    pub max_busy: AtomicU64,
}

impl Pool {
    pub fn new(slots: usize) -> Arc<Pool> {
        Arc::new(Pool { slots: slots.max(1), st: Mutex::new(State::default()), stats: Default::default(), max_busy: AtomicU64::new(0) })
    }

    /// The default size: 70% of the cores, at least 1.
    pub fn default_slots() -> usize {
        let n = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        ((n as f64 * 0.7).floor() as usize).max(1)
    }

    pub fn slots(&self) -> usize {
        self.slots
    }

    pub fn busy(&self) -> usize {
        self.st.lock().unwrap().busy
    }

    pub fn waiting(&self) -> usize {
        self.st.lock().unwrap().waiting.len()
    }

    /// Wait for a slot by priority (absorption never waits).
    pub async fn acquire(self: &Arc<Self>, prio: Prio) -> Permit {
        let t0 = Instant::now();
        let rx = {
            let mut st = self.st.lock().unwrap();
            if prio == Prio::Absorb || (st.busy < self.slots && st.waiting.is_empty()) {
                st.busy += 1;
                self.max_busy.fetch_max(st.busy as u64, Relaxed);
                None
            } else {
                let (tx, rx) = tokio::sync::oneshot::channel();
                st.seq += 1;
                let seq = st.seq;
                st.waiting.push(Waiter { prio, seq, tx });
                Some(rx)
            }
        };
        if let Some(rx) = rx {
            // the slot is handed over by `release` (busy already counted for us)
            let _ = rx.await;
        }
        let waited = t0.elapsed().as_micros() as u64;
        let s = &self.stats[prio as usize];
        s.jobs.fetch_add(1, Relaxed);
        s.wait_us.fetch_add(waited, Relaxed);
        s.max_wait_us.fetch_max(waited, Relaxed);
        Permit { pool: Arc::clone(self), prio, since: Instant::now() }
    }

    fn release(&self) {
        let mut st = self.st.lock().unwrap();
        st.busy -= 1;
        while st.busy < self.slots {
            let Some(w) = st.waiting.pop() else { break };
            st.busy += 1;
            self.max_busy.fetch_max(st.busy as u64, Relaxed);
            if w.tx.send(()).is_err() {
                st.busy -= 1; // the waiter was dropped
            }
        }
    }
}

pub struct Permit {
    pool: Arc<Pool>,
    prio: Prio,
    since: Instant,
}

impl Permit {
    pub fn held(&self) -> Duration {
        self.since.elapsed()
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.pool.stats[self.prio as usize].held_us.fetch_add(self.since.elapsed().as_micros() as u64, Relaxed);
        self.pool.release();
    }
}

static GLOBAL: std::sync::OnceLock<Arc<Pool>> = std::sync::OnceLock::new();

/// The process's pool (`--sealed-pool-slots`, default 70% of the cores).
pub fn global() -> Arc<Pool> {
    Arc::clone(GLOBAL.get_or_init(|| Pool::new(Pool::default_slots())))
}

/// How much lower than the server's threads the background threads run (`setpriority`, nice +10: when a customer's
/// command and a compaction job both want the CPU, the kernel runs the command first).
/// perf-latency round 2: when the process may use fewer cores than the CPUs it may run on (a container's CPU quota, as
/// `available_parallelism` reports it), pin the whole process to that many CPUs. A quota is counted per 100 ms period
/// over every thread: threads running at once on several CPUs used a one-core quota in a fraction of the period, and then
/// the kernel froze all of them, the commands too, to its end (MEASURED: throttled in 15-29% of the periods). On as many
/// CPUs as the quota, the threads take turns, the quota is not overrun, and the background's lower priority works.
/// Call before any thread is started (threads inherit the mask). Returns the CPUs chosen, None when nothing was changed.
/// `FENVIX_PIN_CPUS=0` turns it off.
pub fn pin_to_quota() -> Option<Vec<usize>> {
    if std::env::var("FENVIX_PIN_CPUS").as_deref() == Ok("0") {
        return None;
    }
    let quota = std::thread::available_parallelism().ok()?.get();
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return None;
        }
        let allowed: Vec<usize> = (0..libc::CPU_SETSIZE as usize).filter(|c| libc::CPU_ISSET(*c, &set)).collect();
        if allowed.len() <= quota {
            return None;
        }
        // spread processes of one host over its CPUs at random (a pid is no help: in containers every server is pid 7)
        let start = rand::random::<usize>() % allowed.len();
        let chosen: Vec<usize> = (0..quota).map(|i| allowed[(start + i) % allowed.len()]).collect();
        let mut pin: libc::cpu_set_t = std::mem::zeroed();
        for c in &chosen {
            libc::CPU_SET(*c, &mut pin);
        }
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &pin) != 0 {
            return None;
        }
        Some(chosen)
    }
}

static FLUSH: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();

/// perf-latency round 3: the runtime of every writer's own background tasks (SlateDB's memtable flusher and WAL flusher).
/// They are on the write path: a memtable not uploaded counts against `max_unflushed_bytes`, and at that limit every write
/// stops. On the background runtime (nice +10, pinned to the same CPU as the commands) the flusher was starved exactly
/// when writes were busiest. Two threads at normal priority: they take turns with the commands, and compaction (the
/// background runtime) yields to both.
pub fn flush_runtime() -> tokio::runtime::Handle {
    FLUSH
        .get_or_init(|| tokio::runtime::Builder::new_multi_thread().worker_threads(2).thread_name("fx-flush").thread_stack_size(16 << 20).enable_all().build().expect("flush runtime"))
        .handle()
        .clone()
}

static WRITE_PATH: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();

/// perf-latency round 3: the runtime of the compaction work the writes wait on: absorption (L0 SSTs into delta runs) and
/// SlateDB's fold (which then removes the absorbed L0 SSTs from the writer's manifest). SlateDB stops a database's
/// writes when its L0 (with the uploads in flight) reaches `l0_max_ssts`; at nice +10, on the one CPU of the commands,
/// absorption and the fold got the CPU the commands left, which was little exactly when writes were busiest: writes
/// stopped for 1.2-5 s with 5-7 L0 SSTs visible (medium m18). Two threads at normal priority, apart from the flushers so
/// that a long absorption never holds a memtable upload. Merges, reseals, erasures and GC stay on [`bg_runtime`].
pub fn write_path_runtime() -> tokio::runtime::Handle {
    WRITE_PATH
        .get_or_init(|| tokio::runtime::Builder::new_multi_thread().worker_threads(2).thread_name("fx-wpath").thread_stack_size(16 << 20).enable_all().build().expect("write-path runtime"))
        .handle()
        .clone()
}

/// The runtime for a compactor's absorption and fold: [`write_path_runtime`] when the compactor runs on [`bg_runtime`]
/// (the server), else the compactor's own runtime (tests and benches that give one).
pub fn write_path_for(runtime: &tokio::runtime::Handle) -> tokio::runtime::Handle {
    if BG_ON.load(std::sync::atomic::Ordering::Relaxed) && super::compactor::SERVER_ON.load(std::sync::atomic::Ordering::Relaxed) && WRITE_PATH_ON.load(std::sync::atomic::Ordering::Relaxed) {
        write_path_runtime()
    } else {
        runtime.clone()
    }
}

/// `FENVIX_WRITE_PATH_RUNTIME=0` keeps absorption and the fold on the background runtime (the round-2 behaviour, to compare).
pub static WRITE_PATH_ON: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

/// Whether the server's compactors use [`bg_runtime`] (`--compaction-threads own`, the default) or the server's runtime.
pub static BG_ON: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

/// The runtime a compactor started from the server runs its jobs on (see [`bg_runtime`]).
pub fn compaction_runtime() -> tokio::runtime::Handle {
    if BG_ON.load(std::sync::atomic::Ordering::Relaxed) {
        bg_runtime()
    } else {
        tokio::runtime::Handle::current()
    }
}

pub static BG_NICE: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(10);

static BG: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();

/// Share of the cores the process may use (`available_parallelism`, which follows a container's CPU quota) that the
/// background threads may take, in percent (`--compaction-cpu-pct`, default 50). Perf-latency: in a container with a CPU
/// quota, the kernel freezes every thread of the container once the quota of its period (100 ms) is used up, whatever the
/// threads' priority: compaction that used it up froze the command threads for the rest of the period (MEASURED: the
/// server was throttled in 626 of 3,162 periods, 36 s in all, on the medium profile with one core). The jobs pace
/// themselves (`pace`) so the background stays under this share on average, and the commands keep the rest.
pub static BG_CPU_PCT: AtomicU64 = AtomicU64::new(50);
/// CPU the background may run ahead of its share before a job pauses (microseconds).
const BG_BURST_US: f64 = 10_000.0;

thread_local! {
    static BG_THREAD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static LAST_CPU: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// The CPU the background used beyond its share (microseconds), and when that was last computed.
static DEBT: Mutex<(f64, Option<Instant>)> = Mutex::new((0.0, None));
/// Pauses taken by `pace`, and their total time (serverStatus-free counters, for measurements).
pub static PACE_PAUSES: AtomicU64 = AtomicU64::new(0);
pub static PACE_PAUSE_US: AtomicU64 = AtomicU64::new(0);

tokio::task_local! {
    /// The job is urgent (absorption with L0 close to the writer's stop): `pace` accounts its CPU but never pauses it.
    pub static PACE_EXEMPT: Arc<dyn Fn() -> bool + Send + Sync>;
}

fn bg_rate_us_per_s() -> f64 {
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1) as f64;
    cores * 1e6 * BG_CPU_PCT.load(Relaxed) as f64 / 100.0
}

/// Share of what the cores allow in a 100 ms window (percent) that the whole process may have used before the background
/// pauses to the end of the window (`--compaction-window-pct`, default 70; 100 = never).
pub static WINDOW_PCT: AtomicU64 = AtomicU64::new(70);
const WINDOW: Duration = Duration::from_millis(100);
/// (start of the current window, the process's CPU at its start, microseconds)
static WIN: Mutex<Option<(Instant, u64)>> = Mutex::new(None);

/// The time to the end of the current window when the process already used its allowance in it (None: go on).
fn window_full() -> Option<Duration> {
    let pct = WINDOW_PCT.load(Relaxed);
    if pct >= 100 {
        return None;
    }
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1) as u64;
    let now = Instant::now();
    let cpu = super::compactor::process_cpu_us();
    let mut w = WIN.lock().unwrap();
    let (start, at) = match *w {
        Some((s, c)) if now.duration_since(s) < WINDOW => (s, c),
        _ => {
            *w = Some((now, cpu));
            (now, cpu)
        }
    };
    let allowance = cores * WINDOW.as_micros() as u64 * pct / 100;
    (cpu.saturating_sub(at) >= allowance).then(|| (start + WINDOW).saturating_duration_since(now).max(Duration::from_millis(1)))
}

/// A pace point of background work (between two blocks of a run being written or read, between two L0 blocks): on a
/// background thread, account the CPU this thread used since its last pace point, and when the background as a whole is
/// more than `BG_BURST_US` ahead of its share, sleep until it is back within it. Elsewhere (a command, a test): nothing.
/// Answers never depend on it; only when the work runs.
pub async fn pace() {
    if !BG_THREAD.with(|b| b.get()) {
        return;
    }
    let cpu = super::compactor::thread_cpu_us();
    let used = cpu.saturating_sub(LAST_CPU.with(|c| c.replace(cpu))) as f64;
    let rate = bg_rate_us_per_s();
    let excess = {
        let mut d = DEBT.lock().unwrap();
        let now = Instant::now();
        let dt = d.1.map_or(0.0, |t| now.duration_since(t).as_secs_f64());
        d.0 = (d.0 + used - dt * rate).max(0.0);
        d.1 = Some(now);
        d.0 - BG_BURST_US
    };
    if PACE_EXEMPT.try_with(|f| f()).unwrap_or(false) {
        tokio::task::yield_now().await;
        return;
    }
    // perf-latency round 2: the whole process's CPU in the current 100 ms window. A CPU quota is enforced per period of
    // 100 ms: once the container used it, the kernel freezes every thread to the end of the period (the commands too).
    // The background stops for the rest of the window once the process used WINDOW_PCT of what its cores allow in it, so
    // the commands keep the rest (standard profile: throttled in 29% of the periods with the average share alone).
    if let Some(left) = window_full() {
        PACE_PAUSES.fetch_add(1, Relaxed);
        PACE_PAUSE_US.fetch_add(left.as_micros() as u64, Relaxed);
        tokio::time::sleep(left).await;
        LAST_CPU.with(|c| c.set(super::compactor::thread_cpu_us()));
        return;
    }
    if excess <= 0.0 {
        tokio::task::yield_now().await;
        return;
    }
    let wait = Duration::from_secs_f64(excess / rate);
    PACE_PAUSES.fetch_add(1, Relaxed);
    PACE_PAUSE_US.fetch_add(wait.as_micros() as u64, Relaxed);
    tokio::time::sleep(wait).await;
    // the thread that resumes the job starts a new interval (its CPU before is accounted at its own pace points)
    LAST_CPU.with(|c| c.set(super::compactor::thread_cpu_us()));
}

/// The runtime the server's compaction work runs on (perf-latency, 2026-11-11): the jobs of every database's compactor
/// (absorption, erasures, merges, reseals, GC) and SlateDB's own compactor and worker (the fold). Before, they ran on the
/// server's runtime, and in a machine or container with one core that runtime has one worker thread: a reseal that
/// encoded a run kept that thread for up to 165 ms in one poll, and every command and every log acknowledgement waited
/// behind it (MEASURED, medium profile). Its own threads (the pool's slots plus two), at a lower priority: the
/// kernel time-slices them with the server's thread instead of the runtime running them to their next await.
pub fn bg_runtime() -> tokio::runtime::Handle {
    BG.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            // the pool's slots, plus two: absorption never waits for a slot, and SlateDB's fold (on this runtime too) must
            // run while every slot is in a long reseal, or L0 fills and the writer stops (MEASURED: writes stopped for
            // 1.7-3.4 s with one thread). The kernel shares the CPU between them; the pool still bounds how many jobs run.
            .worker_threads(global().slots() + 2)
            .thread_name("fx-bg")
            .thread_stack_size(16 << 20)
            .on_thread_start(|| {
                BG_THREAD.with(|b| b.set(true));
                LAST_CPU.with(|c| c.set(super::compactor::thread_cpu_us()));
                let nice = BG_NICE.load(std::sync::atomic::Ordering::Relaxed);
                if nice != 0 {
                    // a thread id with PRIO_PROCESS sets that thread's nice value only (Linux)
                    unsafe {
                        let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
                        libc::setpriority(libc::PRIO_PROCESS, tid, nice);
                    }
                }
            })
            .enable_all()
            .build()
            .expect("background runtime")
    })
    .handle()
    .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn priorities_and_absorption_never_waits() {
        let p = Pool::new(1);
        let a = p.acquire(Prio::Reseal).await;
        // queue a reseal, a merge and an erasure; then release: the erasure goes first, then the merge
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut hs = Vec::new();
        for (i, pr) in [Prio::Reseal, Prio::Merge, Prio::Erase].into_iter().enumerate() {
            let (p, o) = (Arc::clone(&p), Arc::clone(&order));
            hs.push(tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(10 * i as u64)).await;
                let _g = p.acquire(pr).await;
                o.lock().unwrap().push(pr);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }));
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(p.waiting(), 3);
        // absorption runs although the only slot is taken
        let ab = tokio::time::timeout(Duration::from_millis(50), p.acquire(Prio::Absorb)).await.expect("absorption waited");
        assert_eq!(p.busy(), 2);
        drop(ab);
        drop(a);
        for h in hs {
            h.await.unwrap();
        }
        assert_eq!(*order.lock().unwrap(), vec![Prio::Erase, Prio::Merge, Prio::Reseal]);
        assert_eq!(p.busy(), 0);
    }
}
