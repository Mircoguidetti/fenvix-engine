//! Common test bed for the journal3z spikes (criteria: `docs/journal3z-libraries.md`).
//!
//! - [`DelayLine`]: every message between two different nodes goes through it and is delivered after a one-way delay
//!   drawn uniformly in `[min_us, max_us]`, keeping per-link FIFO order (like a TCP connection). A dedicated thread
//!   with a precise timer (condvar wait + short spin), because tokio's timer has 1 ms resolution.
//! - [`LogFile`]: one real file per node; `append` queues bytes, a dedicated thread writes everything queued, calls
//!   `fdatasync` once, then runs the callbacks in order (group commit). Every candidate uses it for its log.
//! - [`closed_loop`]: N concurrent writers, each waits for its write to be acknowledged before the next; latency of
//!   every measured write; process CPU time and peak RSS.

use bytes::Bytes;
use futures::future::BoxFuture;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use std::collections::{BinaryHeap, HashMap};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

pub type Task = Box<dyn FnOnce() + Send + 'static>;

// ------------------------------------------------------------------------------------------------------ delay line

#[derive(Clone, Copy, Debug, Serialize)]
pub struct DelayCfg {
    pub min_us: u64,
    pub max_us: u64,
    pub seed: u64,
}

impl DelayCfg {
    /// "0.5-1.0" style (milliseconds) -> cfg.
    pub fn parse(s: &str, seed: u64) -> anyhow::Result<Self> {
        let (a, b) = s.split_once('-').ok_or_else(|| anyhow::anyhow!("delay: expected MIN-MAX in ms"))?;
        let ms = |x: &str| -> anyhow::Result<u64> { Ok((x.trim().parse::<f64>()? * 1000.0).round() as u64) };
        Ok(DelayCfg { min_us: ms(a)?, max_us: ms(b)?, seed })
    }
}

struct Item {
    at: Instant,
    seq: u64,
    f: Task,
}
impl PartialEq for Item {
    fn eq(&self, o: &Self) -> bool {
        self.at == o.at && self.seq == o.seq
    }
}
impl Eq for Item {}
impl PartialOrd for Item {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Item {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        // min-heap on (at, seq)
        o.at.cmp(&self.at).then(o.seq.cmp(&self.seq))
    }
}

struct DlState {
    heap: BinaryHeap<Item>,
    seq: u64,
    last: HashMap<(usize, usize), Instant>,
    rng: rand::rngs::StdRng,
    stop: bool,
}

pub struct DelayLine {
    cfg: DelayCfg,
    /// Linux thread id of the delay thread (its CPU time is the simulated network, reported apart).
    pub tid: AtomicU64,
    st: Mutex<DlState>,
    cv: Condvar,
    pub messages: AtomicU64,
    pub bytes: AtomicU64,
}

impl DelayLine {
    pub fn new(cfg: DelayCfg) -> Arc<Self> {
        let dl = Arc::new(DelayLine {
            cfg,
            st: Mutex::new(DlState { heap: BinaryHeap::new(), seq: 0, last: HashMap::new(), rng: rand::rngs::StdRng::seed_from_u64(cfg.seed), stop: false }),
            cv: Condvar::new(),
            tid: AtomicU64::new(0),
            messages: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        });
        let d = Arc::clone(&dl);
        std::thread::Builder::new().name("delay-line".into()).spawn(move || d.run()).unwrap();
        dl
    }

    /// Deliver `f` on the delay thread after the link's delay. `from == to` (local) runs after no delay, still on the
    /// delay thread (so no candidate gets re-entrant calls from it). `size` is only counted.
    pub fn send(&self, from: usize, to: usize, size: usize, f: Task) {
        let now = Instant::now();
        let mut st = self.st.lock().unwrap();
        let at = if from == to {
            now
        } else {
            self.messages.fetch_add(1, Relaxed);
            self.bytes.fetch_add(size as u64, Relaxed);
            let d = st.rng.gen_range(self.cfg.min_us..=self.cfg.max_us);
            let at = now + Duration::from_micros(d);
            // per-link FIFO, like one TCP connection
            let last = st.last.entry((from, to)).or_insert(now);
            let at = at.max(*last);
            *last = at;
            at
        };
        st.seq += 1;
        let seq = st.seq;
        let wake = st.heap.peek().map_or(true, |top| at < top.at);
        st.heap.push(Item { at, seq, f });
        drop(st);
        if wake {
            self.cv.notify_one();
        }
    }

    pub fn stop(&self) {
        self.st.lock().unwrap().stop = true;
        self.cv.notify_one();
    }

    fn run(&self) {
        self.tid.store(unsafe { libc::syscall(libc::SYS_gettid) } as u64, Relaxed);
        let mut st = self.st.lock().unwrap();
        loop {
            if st.stop {
                return;
            }
            let now = Instant::now();
            match st.heap.peek().map(|t| t.at) {
                None => st = self.cv.wait(st).unwrap(),
                Some(at) if at <= now => {
                    let it = st.heap.pop().unwrap();
                    drop(st);
                    (it.f)();
                    st = self.st.lock().unwrap();
                }
                Some(at) => {
                    let left = at - now;
                    if left > Duration::from_micros(120) {
                        st = self.cv.wait_timeout(st, left - Duration::from_micros(80)).unwrap().0;
                    } else {
                        drop(st);
                        std::hint::spin_loop();
                        std::thread::yield_now();
                        st = self.st.lock().unwrap();
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------------------- log file

struct LfOp {
    bytes: Vec<u8>,
    cb: Option<Task>,
}

/// A real file with group commit: write everything queued, one `fdatasync`, then the callbacks in order.
pub struct LogFile {
    tx: std::sync::mpsc::Sender<LfOp>,
    pub fsyncs: Arc<AtomicU64>,
    pub written: Arc<AtomicU64>,
}

impl LogFile {
    pub fn create(path: &Path) -> std::io::Result<Arc<Self>> {
        let mut f = OpenOptions::new().create(true).write(true).truncate(true).open(path)?;
        let (tx, rx) = std::sync::mpsc::channel::<LfOp>();
        let fsyncs = Arc::new(AtomicU64::new(0));
        let written = Arc::new(AtomicU64::new(0));
        let (fs, wr) = (fsyncs.clone(), written.clone());
        std::thread::Builder::new()
            .name("logfile".into())
            .spawn(move || {
                while let Ok(first) = rx.recv() {
                    let mut batch = vec![first];
                    while let Ok(m) = rx.try_recv() {
                        batch.push(m);
                    }
                    let mut buf = Vec::new();
                    for op in &batch {
                        buf.extend_from_slice(&op.bytes);
                    }
                    f.write_all(&buf).expect("log write");
                    f.sync_data().expect("fdatasync");
                    fs.fetch_add(1, Relaxed);
                    wr.fetch_add(buf.len() as u64, Relaxed);
                    for op in batch {
                        if let Some(cb) = op.cb {
                            cb();
                        }
                    }
                }
            })?;
        Ok(Arc::new(LogFile { tx, fsyncs, written }))
    }

    /// Queue `bytes`; `cb` runs (on the file's thread) after they are written and fdatasync'd.
    pub fn append(&self, bytes: Vec<u8>, cb: Task) {
        let _ = self.tx.send(LfOp { bytes, cb: Some(cb) });
    }

    /// Like `append`, returning a future.
    pub fn append_wait(&self, bytes: Vec<u8>) -> tokio::sync::oneshot::Receiver<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.append(bytes, Box::new(move || {
            let _ = tx.send(());
        }));
        rx
    }
}

// ------------------------------------------------------------------------------------------------- process metrics

pub fn cpu_time() -> Duration {
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut ru);
        Duration::from_secs((ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) as u64) + Duration::from_micros((ru.ru_utime.tv_usec + ru.ru_stime.tv_usec) as u64)
    }
}

fn status_kb(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with(field)).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse().ok()))
        .unwrap_or(0)
}

/// CPU time of one thread of this process (from /proc, 10 ms resolution).
pub fn thread_cpu(tid: u64) -> Duration {
    let s = std::fs::read_to_string(format!("/proc/self/task/{tid}/stat")).unwrap_or_default();
    let after = s.rsplit_once(')').map(|x| x.1).unwrap_or("");
    let f: Vec<&str> = after.split_whitespace().collect();
    let ticks: u64 = f.get(11).and_then(|x| x.parse().ok()).unwrap_or(0) + f.get(12).and_then(|x| x.parse().ok()).unwrap_or(0);
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as u64;
    Duration::from_micros(ticks * 1_000_000 / hz.max(1))
}

pub fn rss_kb() -> u64 {
    status_kb("VmRSS:")
}

pub fn hwm_kb() -> u64 {
    status_kb("VmHWM:")
}

// ------------------------------------------------------------------------------------------------------ closed loop

#[derive(Debug, Serialize, Clone)]
pub struct LoadResult {
    pub conc: usize,
    pub size: usize,
    pub n: usize,
    pub errors: u64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub writes_per_s: f64,
    pub cpu_us_per_write: f64,
    /// Same, without the delay-line thread (the simulated network is not part of any candidate).
    pub cpu_us_per_write_excl_net: f64,
    pub rss_mb_end: f64,
    pub hwm_mb: f64,
    pub messages_per_write: f64,
}

pub type WriteFn = Arc<dyn Fn(Bytes) -> BoxFuture<'static, anyhow::Result<()>> + Send + Sync>;

/// `conc` writers; each issues its next write only after the previous one is acknowledged. The first `warmup` writes
/// (all writers together) are not measured; then `n` writes are.
pub async fn closed_loop(write: WriteFn, conc: usize, size: usize, warmup: usize, n: usize, dl: Option<Arc<DelayLine>>) -> LoadResult {
    let issued = Arc::new(AtomicU64::new(0));
    let total = (warmup + n) as u64;
    let lat = Arc::new(Mutex::new(Vec::<u64>::with_capacity(n)));
    let errors = Arc::new(AtomicU64::new(0));
    let start_measure = Arc::new(Mutex::new(None::<(Instant, Duration, u64, Duration)>));
    let mut hs = Vec::new();
    for w in 0..conc {
        let (write, issued, lat, errors, sm) = (write.clone(), issued.clone(), lat.clone(), errors.clone(), start_measure.clone());
        let dl = dl.clone();
        hs.push(tokio::spawn(async move {
            loop {
                let i = issued.fetch_add(1, Relaxed);
                if i >= total {
                    return;
                }
                if i == warmup as u64 {
                    let m = dl.as_ref().map(|d| d.messages.load(Relaxed)).unwrap_or(0);
                    let nc = dl.as_ref().map(|d| thread_cpu(d.tid.load(Relaxed))).unwrap_or_default();
                    *sm.lock().unwrap() = Some((Instant::now(), cpu_time(), m, nc));
                }
                let mut payload = vec![(w as u8).wrapping_add(i as u8); size];
                payload[..8.min(size)].copy_from_slice(&i.to_le_bytes()[..8.min(size)]);
                let t = Instant::now();
                match write(Bytes::from(payload)).await {
                    Ok(()) => {
                        if i >= warmup as u64 {
                            lat.lock().unwrap().push(t.elapsed().as_nanos() as u64);
                        }
                    }
                    Err(_) => {
                        errors.fetch_add(1, Relaxed);
                    }
                }
            }
        }));
    }
    for h in hs {
        let _ = h.await;
    }
    let (t0, c0, m0, nc0) = start_measure.lock().unwrap().unwrap_or((Instant::now(), cpu_time(), 0, Duration::ZERO));
    let wall = t0.elapsed().as_secs_f64();
    let cpu = (cpu_time() - c0).as_secs_f64();
    let net_cpu = dl.as_ref().map(|d| thread_cpu(d.tid.load(Relaxed)).saturating_sub(nc0).as_secs_f64()).unwrap_or(0.0);
    let m1 = dl.as_ref().map(|d| d.messages.load(Relaxed)).unwrap_or(0);
    let mut v = lat.lock().unwrap().clone();
    v.sort_unstable();
    let pct = |p: f64| -> f64 {
        if v.is_empty() {
            return f64::NAN;
        }
        let i = ((p / 100.0) * (v.len() as f64 - 1.0)).round() as usize;
        v[i] as f64 / 1e6
    };
    let mean = if v.is_empty() { f64::NAN } else { v.iter().sum::<u64>() as f64 / v.len() as f64 / 1e6 };
    LoadResult {
        conc,
        size,
        n: v.len(),
        errors: errors.load(Relaxed),
        p50_ms: pct(50.0),
        p95_ms: pct(95.0),
        p99_ms: pct(99.0),
        max_ms: pct(100.0),
        mean_ms: mean,
        writes_per_s: v.len() as f64 / wall,
        cpu_us_per_write: cpu * 1e6 / v.len().max(1) as f64,
        cpu_us_per_write_excl_net: (cpu - net_cpu).max(0.0) * 1e6 / v.len().max(1) as f64,
        rss_mb_end: rss_kb() as f64 / 1024.0,
        hwm_mb: hwm_kb() as f64 / 1024.0,
        messages_per_write: (m1 - m0) as f64 / v.len().max(1) as f64,
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct IdleResult {
    pub groups: usize,
    pub secs: f64,
    pub messages_per_s: f64,
    pub cpu_pct_of_one_core: f64,
    pub cpu_pct_excl_net: f64,
    pub rss_mb: f64,
    pub startup_s: f64,
}

/// Measure an idle system for `secs` (after the caller has started `groups` groups and waited for them to settle).
pub async fn idle(dl: &DelayLine, groups: usize, secs: f64, startup_s: f64) -> IdleResult {
    let (m0, c0, t0, n0) = (dl.messages.load(Relaxed), cpu_time(), Instant::now(), thread_cpu(dl.tid.load(Relaxed)));
    tokio::time::sleep(Duration::from_secs_f64(secs)).await;
    let wall = t0.elapsed().as_secs_f64();
    let cpu = (cpu_time() - c0).as_secs_f64();
    let net = thread_cpu(dl.tid.load(Relaxed)).saturating_sub(n0).as_secs_f64();
    IdleResult {
        groups,
        secs: wall,
        messages_per_s: (dl.messages.load(Relaxed) - m0) as f64 / wall,
        cpu_pct_of_one_core: cpu / wall * 100.0,
        cpu_pct_excl_net: (cpu - net).max(0.0) / wall * 100.0,
        rss_mb: rss_kb() as f64 / 1024.0,
        startup_s,
    }
}

/// Common command line of every spike binary.
#[derive(Debug, Clone)]
pub struct Args {
    pub mode: String,
    pub delay: String,
    pub conc: usize,
    pub size: usize,
    pub warmup: usize,
    pub n: usize,
    pub groups: usize,
    pub secs: f64,
    pub dir: String,
    pub out: String,
    pub seed: u64,
}

impl Args {
    /// `--mode latency|idle|fence --delay 0.5-1.0 --conc 8 --size 1024 --warmup 2000 --n 20000 --groups 1000
    ///  --secs 10 --dir /tmp/x --out result.json --seed 1`
    pub fn parse() -> Self {
        let mut a = Args { mode: "latency".into(), delay: "0.5-1.0".into(), conc: 8, size: 1024, warmup: 2000, n: 20000, groups: 1, secs: 10.0, dir: std::env::temp_dir().join("j3z-spike").to_string_lossy().into(), out: String::new(), seed: 1 };
        let v: Vec<String> = std::env::args().collect();
        let mut i = 1;
        while i + 1 < v.len() {
            let x = v[i + 1].clone();
            match v[i].as_str() {
                "--mode" => a.mode = x,
                "--delay" => a.delay = x,
                "--conc" => a.conc = x.parse().unwrap(),
                "--size" => a.size = x.parse().unwrap(),
                "--warmup" => a.warmup = x.parse().unwrap(),
                "--n" => a.n = x.parse().unwrap(),
                "--groups" => a.groups = x.parse().unwrap(),
                "--secs" => a.secs = x.parse().unwrap(),
                "--dir" => a.dir = x,
                "--out" => a.out = x,
                "--seed" => a.seed = x.parse().unwrap(),
                o => panic!("unknown argument {o}"),
            }
            i += 2;
        }
        a
    }

    pub fn delay_cfg(&self) -> DelayCfg {
        DelayCfg::parse(&self.delay, self.seed).unwrap()
    }

    /// Fresh, empty directory for this run's node files.
    pub fn fresh_dir(&self) -> std::path::PathBuf {
        let d = std::path::PathBuf::from(&self.dir);
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Print the JSON result and write it to `--out` if given.
    pub fn emit<T: Serialize>(&self, candidate: &str, extra: serde_json::Value, r: &T) {
        let v = serde_json::json!({"candidate": candidate, "mode": self.mode, "delay_ms": self.delay, "result": r, "extra": extra});
        println!("{v}");
        if !self.out.is_empty() {
            std::fs::write(&self.out, serde_json::to_string_pretty(&v).unwrap()).unwrap();
        }
    }
}

/// Fixed-size frame for a log file: `len u32 | payload` (candidates may use their own encoding inside).
pub fn frame(payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(payload.len() + 4);
    v.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    v.extend_from_slice(payload);
    v
}
