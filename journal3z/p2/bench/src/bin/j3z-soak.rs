//! journal3z soak run (phase-2 plan §2a; with `--mutation`, §2b): 3 nodes + 3 writers + MinIO + fault proxy on this
//! machine, open-loop load, one fault at a time, checks.
//!
//! `j3z-soak --out <results dir> --work <scratch dir> --seed <n> --minutes <m> [--mutation <name>]
//!           [--check-every-min <m>] [--rate <writes/s>] [--dbs <n>] [--kinds <Kind,Kind,...>]`
//!
//! Setup (§2a): 3 nodes, 3 writers (engine stand-in of P5 with the production values: lease 10 s, upload 30 s, 32/128
//! MB, release after 5 s), 30 databases, writes of 1 KB at 200/s in total (open loop: sent whatever the answers).
//! Zone z = writer z + node z (the proxy sits on every writer→node connection).
//!
//! Faults, one at a time, then wait until the system is healthy again, then a pause of 10–30 s: drawn from a shuffled
//! bag whose proportions are the §2a minimum counts (plus three extra kinds, marked "extra"):
//! node kill -9 (restart after 1–10 s); writer kill -9 (restart after 1–10 s); disk loss (directory deleted, node back
//! with a new identity); network: one node isolated from every writer, one direction only, or a whole zone isolated
//! (2–20 s); clock jump of a writer (±0.5–5 s, absolute offset within ±5 s); two writers (the owner of databases
//! stopped with SIGSTOP for lease + 2–8 s with writes in flight, then SIGCONT); fsync error; disk full;
//! extra: power cut of one node (unsynced bytes dropped), power cut of all three nodes at once, object store paused
//! 1–4 s.
//!
//! Checks: every `--check-every-min` (30 by default) and at the end: each acknowledged write at or below the store
//! marker is in the store at its index; disk rules on copies of the nodes' directories; at the end (after healing and
//! a final upload of every database) every acknowledged write. Continuously: node self-check lines (`VIOLATION`),
//! processes ending by themselves (expected only after fsync error, disk full, power cut), late acknowledgements.
//! Recovery: for each fault and each database it touches, time from the fault to the first acknowledgement of a write
//! sent after the fault.
//! Stops at the first failure (§2d). Results: `<out>/summary.json`, `<out>/faults.csv`, `<out>/events.txt`.

use j3z_bench::*;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const NODE_MUTATIONS: [&str; 6] = ["no_fencing", "ack_gap", "keep_diverged", "forget_fence", "ack_before_fsync", "ignore_fsync_error"];
const WRITER_MUTATIONS: [&str; 5] = ["ack_on_one", "recovery_longest", "recovery_single", "trim_before_upload", "skip_store_fence"];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Kind {
    NodeKill,
    WriterKill,
    DiskLoss,
    CutNode,
    CutOneWay,
    CutZone,
    ClockJump,
    TwoWriters,
    FsyncError,
    DiskFull,
    PowerCut,
    PowerCutAll,
    S3Pause,
}

/// One bag = §2a minimum counts / 10 (+ the extra kinds).
const BAG: [(Kind, usize); 13] = [
    (Kind::NodeKill, 30),
    (Kind::WriterKill, 30),
    (Kind::DiskLoss, 5),
    (Kind::CutNode, 18),
    (Kind::CutOneWay, 8),
    (Kind::CutZone, 5),
    (Kind::ClockJump, 15),
    (Kind::TwoWriters, 15),
    (Kind::FsyncError, 5),
    (Kind::DiskFull, 5),
    (Kind::PowerCut, 5),
    (Kind::PowerCutAll, 2),
    (Kind::S3Pause, 3),
];

#[derive(Clone, Debug)]
struct Fault {
    t: Instant,
    kind: Kind,
    detail: String,
    /// databases whose writer the fault hits (rule: 15 s in 99 %, 40 s in 100 %)
    writer_dbs: Vec<u64>,
    /// databases that only lose a node (rule: 5 s in 100 %)
    node_dbs: Vec<u64>,
    healed: Option<Instant>,
}

struct World {
    nodes: Vec<NodeProc>,
    writers: Vec<WriterProc>,
    /// node index -> (exit codes allowed, until)
    expect_exit: HashMap<usize, (Vec<i32>, Instant)>,
    stopped_writer: Option<usize>,
    clock: Vec<i64>,
    failures: Vec<String>,
    notes: Vec<String>,
}

fn arg(a: &[String], k: &str) -> Option<String> {
    a.iter().position(|x| x == k).and_then(|i| a.get(i + 1)).cloned()
}

struct Run {
    t0: Instant,
    world: Arc<Mutex<World>>,
    proxy: Proxy,
    minio: Minio,
    client: Arc<Client>,
    dbs: u64,
    mutation: Option<String>,
    work: PathBuf,
    log: Mutex<Vec<String>>,
}

impl Run {
    fn note(&self, s: String) {
        let line = format!("{:>9.1}s {s}", self.t0.elapsed().as_secs_f64());
        eprintln!("{line}");
        self.log.lock().unwrap().push(line);
    }
    fn fail(&self, s: String) {
        self.note(format!("FAILURE {s}"));
        self.world.lock().unwrap().failures.push(s);
    }
    fn failed(&self) -> bool {
        !self.world.lock().unwrap().failures.is_empty()
    }

    /// Processes ending by themselves, node self-check lines. Expected node exits are restarted.
    fn monitor(&self, seen_violations: &mut usize) {
        let mut w = self.world.lock().unwrap();
        let now = Instant::now();
        for i in 0..w.nodes.len() {
            if let Some(code) = w.nodes[i].exited() {
                let ok = w.expect_exit.get(&i).map_or(false, |(codes, until)| codes.contains(&code) && now <= *until);
                if ok {
                    w.expect_exit.remove(&i);
                    w.nodes[i].full = false;
                    let id = w.nodes[i].id;
                    let r = w.nodes[i].try_restart();
                    drop(w);
                    match r {
                        Ok(()) => self.note(format!("node {i} (id {id}) exited with {code} as expected; restarted")),
                        Err(e) => self.fail(format!("node {i} does not restart after an expected exit {code}: {e}")),
                    }
                    w = self.world.lock().unwrap();
                } else {
                    let tail = std::fs::read_to_string(w.nodes[i].dir.with_extension("stderr.log")).unwrap_or_default();
                    let last = tail.lines().last().unwrap_or("").to_string();
                    drop(w);
                    self.fail(format!("node {i} ended by itself with code {code}: {last}"));
                    w = self.world.lock().unwrap();
                }
            }
        }
        for i in 0..w.writers.len() {
            if let Some(code) = w.writers[i].exited() {
                drop(w);
                self.fail(format!("writer {i} ended by itself with code {code}"));
                w = self.world.lock().unwrap();
            }
        }
        let v: Vec<String> = w.nodes.iter().flat_map(|n| n.events.lines()).filter(|(_, l)| l.starts_with("VIOLATION")).map(|(_, l)| l).collect();
        drop(w);
        if v.len() > *seen_violations {
            for l in &v[*seen_violations..] {
                self.fail(format!("node self-check: {l}"));
            }
            *seen_violations = v.len();
        }
    }

    /// All processes up and running, no network fault, every database owned by a running writer with a recent
    /// acknowledgement, every lease naming the current node identities. Returns the reason when not healthy.
    async fn unhealthy(&self) -> Option<String> {
        let ids = {
            let w = self.world.lock().unwrap();
            if let Some(i) = w.nodes.iter().position(|n| !n.running()) {
                return Some(format!("node {i} not running"));
            }
            if let Some(i) = w.writers.iter().position(|n| !n.running()) {
                return Some(format!("writer {i} not running"));
            }
            if w.stopped_writer.is_some() {
                return Some("a writer is stopped".into());
            }
            let mut ids: Vec<u32> = w.nodes.iter().map(|n| n.id).collect();
            ids.sort();
            ids
        };
        if !self.proxy.all_pass() {
            return Some("network fault active".into());
        }
        let o = self.client.owners();
        if let Some(d) = (1..=self.dbs).find(|d| !o.contains_key(d)) {
            return Some(format!("db {d} has no owner"));
        }
        let now = Instant::now();
        let mut last: HashMap<u64, Instant> = HashMap::new();
        for a in self.client.acks.lock().unwrap().iter().rev().take(20_000) {
            last.entry(a.db).or_insert(a.at);
        }
        if let Some(d) = (1..=self.dbs).find(|d| last.get(d).map_or(true, |t| now - *t > Duration::from_secs(5))) {
            return Some(format!("db {d}: no acknowledgement in the last 5 s"));
        }
        match markers(&self.minio.meta(), self.dbs).await {
            Ok((_, mem)) => {
                for (d, m) in mem {
                    let mut m = m.clone();
                    m.sort();
                    if m != ids {
                        return Some(format!("db {d}: lease members {m:?} != node identities {ids:?}"));
                    }
                }
            }
            Err(e) => return Some(format!("store: {e}")),
        }
        None
    }

    fn owned_by(&self, w: usize) -> Vec<u64> {
        let mut v: Vec<u64> = self.client.owners().into_iter().filter(|(_, x)| *x == w).map(|(d, _)| d).collect();
        v.sort();
        v
    }

    async fn sleep_monitored(&self, d: Duration, seen: &mut usize) {
        let end = Instant::now() + d;
        while Instant::now() < end && !self.failed() {
            self.monitor(seen);
            tokio::time::sleep(Duration::from_millis(200).min(end.saturating_duration_since(Instant::now()))).await;
        }
    }

    /// Inject one fault, hold it, heal it. Returns the fault record.
    async fn inject(&self, kind: Kind, rng: &mut rand::rngs::StdRng, seen: &mut usize) -> Fault {
        let all: Vec<u64> = (1..=self.dbs).collect();
        let t = Instant::now();
        let mut f = Fault { t, kind, detail: String::new(), writer_dbs: vec![], node_dbs: vec![], healed: None };
        match kind {
            Kind::NodeKill => {
                let i = rng.gen_range(0..3);
                let back = rng.gen_range(1000..10_000);
                self.world.lock().unwrap().nodes[i].kill9();
                f.detail = format!("node {i} restarted after {back} ms");
                f.node_dbs = all;
                self.sleep_monitored(Duration::from_millis(back), seen).await;
                let r = self.world.lock().unwrap().nodes[i].try_restart();
                if let Err(e) = r {
                    self.fail(format!("node {i} does not restart after kill -9: {e}"));
                }
            }
            Kind::WriterKill => {
                let i = rng.gen_range(0..3);
                let back = rng.gen_range(1000..10_000);
                f.writer_dbs = self.owned_by(i); // the other writers' databases are not touched
                self.world.lock().unwrap().writers[i].kill9();
                f.detail = format!("writer {i} (owned {:?}) restarted after {back} ms", f.writer_dbs);
                self.sleep_monitored(Duration::from_millis(back), seen).await;
                self.world.lock().unwrap().writers[i].restart();
            }
            Kind::DiskLoss => {
                let i = rng.gen_range(0..3);
                let same = self.mutation.as_deref() == Some("wipe_same_id");
                let r = {
                    let mut w = self.world.lock().unwrap();
                    let old = w.nodes[i].id;
                    let r = if same { w.nodes[i].wipe_same_id() } else { w.nodes[i].try_wipe() };
                    f.detail = format!("node {i} disk lost: id {old} -> {}", w.nodes[i].id);
                    r
                };
                if let Err(e) = r {
                    self.fail(format!("node {i} does not start after disk loss: {e}"));
                }
                f.node_dbs = all;
            }
            Kind::CutNode | Kind::CutOneWay => {
                let i = rng.gen_range(0..3);
                let mode = if kind == Kind::CutNode { ["blackhole", "cut"][rng.gen_range(0..2)] } else { ["drop_c2s", "drop_s2c"][rng.gen_range(0..2)] };
                let hold = rng.gen_range(2000..20_000);
                self.proxy.set_many(&[(0, i, mode), (1, i, mode), (2, i, mode)]);
                f.detail = format!("node {i} {mode} from every writer for {hold} ms");
                f.node_dbs = all;
                self.sleep_monitored(Duration::from_millis(hold), seen).await;
                self.proxy.heal();
            }
            Kind::CutZone => {
                let z = rng.gen_range(0..3);
                let hold = rng.gen_range(2000..20_000);
                let mut routes = vec![];
                for w in 0..3 {
                    for n in 0..3 {
                        if (w == z) != (n == z) {
                            routes.push((w, n, "blackhole"));
                        }
                    }
                }
                f.writer_dbs = self.owned_by(z);
                f.node_dbs = all.iter().copied().filter(|d| !f.writer_dbs.contains(d)).collect();
                self.proxy.set_many(&routes);
                f.detail = format!("zone {z} isolated for {hold} ms (writer {z} owned {:?})", f.writer_dbs);
                self.sleep_monitored(Duration::from_millis(hold), seen).await;
                self.proxy.heal();
            }
            Kind::ClockJump => {
                let i = rng.gen_range(0..3);
                let mag = rng.gen_range(500..=5000) as i64;
                let off = {
                    let mut w = self.world.lock().unwrap();
                    let cur = w.clock[i];
                    let up = if cur + mag > 5000 { false } else if cur - mag < -5000 { true } else { rng.gen_bool(0.5) };
                    let off = if up { cur + mag } else { cur - mag };
                    w.clock[i] = off;
                    w.writers[i].set_clock_offset(off);
                    off
                };
                f.writer_dbs = self.owned_by(i);
                f.detail = format!("writer {i} clock offset now {off} ms");
            }
            Kind::TwoWriters => {
                // the writer owning the most databases
                let o = self.client.owners();
                let i = (0..3).max_by_key(|w| o.values().filter(|x| *x == w).count()).unwrap();
                let hold = rng.gen_range(12_000..18_000);
                f.writer_dbs = self.owned_by(i);
                {
                    let mut w = self.world.lock().unwrap();
                    w.writers[i].stop();
                    w.stopped_writer = Some(i);
                }
                f.detail = format!("writer {i} (owned {:?}) stopped for {hold} ms", f.writer_dbs);
                self.sleep_monitored(Duration::from_millis(hold), seen).await;
                let mut w = self.world.lock().unwrap();
                w.writers[i].cont();
                w.stopped_writer = None;
            }
            Kind::FsyncError | Kind::DiskFull | Kind::PowerCut => {
                let i = rng.gen_range(0..3);
                let mut w = self.world.lock().unwrap();
                let code = if kind == Kind::PowerCut { 99 } else { 75 };
                w.expect_exit.insert(i, (vec![code], Instant::now() + Duration::from_secs(10)));
                let n = &mut w.nodes[i];
                match kind {
                    Kind::FsyncError => n.fail_syncs += 1,
                    Kind::DiskFull => n.full = true,
                    _ => n.power_cuts += 1,
                }
                n.write_faults();
                f.detail = format!("node {i} {kind:?}");
                f.node_dbs = all;
            }
            Kind::PowerCutAll => {
                let mut w = self.world.lock().unwrap();
                for i in 0..3 {
                    w.expect_exit.insert(i, (vec![99], Instant::now() + Duration::from_secs(10)));
                    w.nodes[i].power_cuts += 1;
                    w.nodes[i].write_faults();
                }
                f.detail = "power cut of all three nodes".into();
            }
            Kind::S3Pause => {
                let hold = rng.gen_range(1000..4000);
                self.minio.stop();
                f.detail = format!("object store paused {hold} ms");
                self.sleep_monitored(Duration::from_millis(hold), seen).await;
                self.minio.cont();
            }
        }
        f.healed = Some(Instant::now());
        f
    }

    /// Loss check against the store: `full` = every acknowledgement, else only those at or below the store marker.
    async fn loss_check(&self, full: bool) -> Result<(Report, usize), String> {
        let meta = self.minio.meta();
        let acks = self.client.acks.lock().unwrap().clone();
        let opens: Vec<(u64, u64, u64)> = self.world.lock().unwrap().writers.iter().flat_map(|w| w.opens()).map(|(_, d, e, b)| (d, e, b)).collect();
        let (m, _) = markers(&meta, self.dbs).await.map_err(|e| e.to_string())?;
        let sel: Vec<Ack> = if full { acks } else { acks.into_iter().filter(|a| a.idx <= m[&a.db]).collect() };
        let n = sel.len();
        let r = check(&meta, &sel, &opens, self.dbs).await.map_err(|e| e.to_string())?;
        Ok((r, n))
    }

    async fn disk_check(&self, tag: &str) -> Result<Vec<String>, String> {
        let mut snaps = vec![];
        let mut errs = vec![];
        for i in 0..3 {
            let r = {
                let w = self.world.lock().unwrap();
                snapshot(&w.nodes[i], &self.work.join(format!("snap-{tag}-n{i}")))
            };
            match r {
                Ok(c) => snaps.push(c),
                Err(e) if e == "not running" => {}
                Err(e) => errs.push(e),
            }
        }
        let (m, _) = markers(&self.minio.meta(), self.dbs).await.map_err(|e| e.to_string())?;
        let mut v = disk_violations(&snaps, &m);
        v.extend(errs);
        for i in 0..3 {
            let _ = std::fs::remove_dir_all(self.work.join(format!("snap-{tag}-n{i}")));
        }
        Ok(v)
    }

    async fn checkpoint(&self, tag: &str, full: bool) -> Value {
        let lc = self.loss_check(full).await;
        let dc = self.disk_check(tag).await;
        let mut out = json!({"tag": tag, "t_s": self.t0.elapsed().as_secs()});
        match lc {
            Ok((r, n)) => {
                out["checked_acks"] = json!(n);
                out["lost"] = json!(r.lost.len());
                out["late"] = json!(r.late.len());
                out["first_lost"] = json!(r.lost.iter().take(10).map(|a| format!("db {} idx {} epoch {} id {} writer {}", a.db, a.idx, a.epoch, a.id, a.writer)).collect::<Vec<_>>());
                out["first_late"] = json!(r.late.iter().take(10).map(|a| format!("db {} idx {} epoch {} id {}", a.db, a.idx, a.epoch, a.id)).collect::<Vec<_>>());
                if !r.lost.is_empty() {
                    self.fail(format!("{tag}: {} acknowledged writes lost (first: {:?})", r.lost.len(), r.lost.first().map(|a| (a.db, a.idx, a.epoch, a.id))));
                }
                if !r.late.is_empty() {
                    self.fail(format!("{tag}: {} late acknowledgements from an old writer", r.late.len()));
                }
            }
            Err(e) => {
                out["loss_check_error"] = json!(e);
                self.fail(format!("{tag}: loss check could not run: {e}"));
            }
        }
        match dc {
            Ok(v) => {
                out["disk_violations"] = json!(v.len());
                out["first_disk_violations"] = json!(v.iter().take(10).collect::<Vec<_>>());
                if !v.is_empty() {
                    self.fail(format!("{tag}: {} disk rule violations (first: {})", v.len(), v[0]));
                }
            }
            Err(e) => {
                out["disk_check_error"] = json!(e);
                self.fail(format!("{tag}: disk check could not run: {e}"));
            }
        }
        self.note(format!("checkpoint {out}"));
        out
    }
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let i = ((p / 100.0) * (v.len() as f64 - 1.0)).round() as usize;
    v[i.min(v.len() - 1)]
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let out = PathBuf::from(arg(&a, "--out").expect("--out"));
    let work = PathBuf::from(arg(&a, "--work").expect("--work"));
    let seed: u64 = arg(&a, "--seed").expect("--seed").parse().unwrap();
    let minutes: f64 = arg(&a, "--minutes").expect("--minutes").parse().unwrap();
    let check_every = Duration::from_secs_f64(arg(&a, "--check-every-min").map_or(30.0, |x| x.parse().unwrap()) * 60.0);
    let rate: u64 = arg(&a, "--rate").map_or(200, |x| x.parse().unwrap());
    let dbs: u64 = arg(&a, "--dbs").map_or(30, |x| x.parse().unwrap());
    let mutation = arg(&a, "--mutation");
    // optional: only these fault kinds (targeted runs), e.g. `--kinds CutZone,TwoWriters`
    let only: Option<Vec<String>> = arg(&a, "--kinds").map(|k| k.split(',').map(|x| x.to_string()).collect());
    let node_mut = mutation.as_deref().filter(|m| NODE_MUTATIONS.contains(m));
    let writer_mut = mutation.as_deref().filter(|m| WRITER_MUTATIONS.contains(m));
    if let Some(m) = &mutation {
        assert!(node_mut.is_some() || writer_mut.is_some() || m == "wipe_same_id", "unknown mutation {m}");
    }
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).unwrap();
    std::fs::create_dir_all(&out).unwrap();
    let _ = bins();
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

    let minio = Minio::start(&work.join("minio"));
    minio.ready().await;
    let nodes: Vec<NodeProc> = (0..3).map(|i| NodeProc::start(&work.join(format!("n{i}")), node_mut)).collect();
    let addrs: Vec<String> = nodes.iter().map(|n| n.addr()).collect();
    let proxy = Proxy::start(&work.join("proxy"), 3, &addrs);
    let writers: Vec<WriterProc> = (0..3)
        .map(|i| {
            WriterProc::start(WriterSpec {
                index: i,
                nwriters: 3,
                nodes: proxy.node_addrs(i, 3),
                dbs,
                minio: &minio,
                config: json!({}), // the production values (ServerCfg::default)
                dir: &work.join("writers"),
                mutation: writer_mut,
                env: vec![],
            })
        })
        .collect();
    let client = Client::new(&writers.iter().map(|w| w.client.clone()).collect::<Vec<_>>());
    let world = Arc::new(Mutex::new(World {
        nodes,
        writers,
        expect_exit: HashMap::new(),
        stopped_writer: None,
        clock: vec![0; 3],
        failures: vec![],
        notes: vec![],
    }));
    let run = Run { t0: Instant::now(), world, proxy, minio, client: client.clone(), dbs, mutation: mutation.clone(), work: work.clone(), log: Mutex::new(vec![]) };
    run.note(format!("start seed {seed} minutes {minutes} mutation {mutation:?} rate {rate}/s dbs {dbs} kinds {only:?}"));
    let load = client.open_loop(dbs, rate, 1024, seed ^ 0x5eed);
    let mut seen = 0usize;

    // wait for the first owner of every database
    let t_start = Instant::now();
    let mut healthy_at_start = false;
    while t_start.elapsed() < Duration::from_secs(120) && !run.failed() {
        run.sleep_monitored(Duration::from_millis(500), &mut seen).await;
        if run.unhealthy().await.is_none() {
            healthy_at_start = true;
            break;
        }
    }
    if !healthy_at_start {
        let why = run.unhealthy().await.unwrap_or_default();
        run.fail(format!("not healthy 120 s after start: {why}"));
    }

    let mut faults: Vec<Fault> = vec![];
    let mut bag: Vec<Kind> = vec![];
    let mut checkpoints = vec![];
    let mut next_check = Instant::now() + check_every;
    let deadline = run.t0 + Duration::from_secs_f64(minutes * 60.0);
    let mut unhealthy_waits: Vec<f64> = vec![];
    while Instant::now() < deadline && !run.failed() {
        // pause, then wait until healthy (one fault at a time)
        let pause = rng.gen_range(10_000..30_000);
        run.sleep_monitored(Duration::from_millis(pause), &mut seen).await;
        let tw = Instant::now();
        let mut why = run.unhealthy().await;
        while why.is_some() && tw.elapsed() < Duration::from_secs(120) && !run.failed() {
            run.sleep_monitored(Duration::from_millis(500), &mut seen).await;
            why = run.unhealthy().await;
        }
        if let Some(w) = why {
            run.fail(format!("not healthy 120 s after the previous fault ({:?}): {w}", faults.last().map(|f| f.kind)));
            break;
        }
        unhealthy_waits.push(tw.elapsed().as_secs_f64());
        if run.failed() || Instant::now() >= deadline {
            break;
        }
        if Instant::now() >= next_check {
            checkpoints.push(run.checkpoint(&format!("ckpt{}", checkpoints.len() + 1), false).await);
            next_check = Instant::now() + check_every;
            if run.failed() {
                break;
            }
        }
        if bag.is_empty() {
            for (k, n) in BAG {
                if only.as_ref().map_or(true, |o| o.contains(&format!("{k:?}"))) {
                    bag.extend(std::iter::repeat(k).take(n));
                }
            }
            bag.shuffle(&mut rng);
        }
        let kind = bag.pop().unwrap();
        let f = run.inject(kind, &mut rng, &mut seen).await;
        run.note(format!("fault {} {:?}: {}", faults.len() + 1, f.kind, f.detail));
        faults.push(f);
    }

    // end: heal everything, wait healthy, stop the load, upload everything, final checks
    run.note("end: healing".into());
    run.proxy.heal();
    run.minio.cont();
    {
        let mut w = run.world.lock().unwrap();
        if let Some(i) = w.stopped_writer.take() {
            w.writers[i].cont();
        }
    }
    let te = Instant::now();
    let mut why = run.unhealthy().await;
    while why.is_some() && te.elapsed() < Duration::from_secs(120) {
        run.monitor(&mut seen);
        tokio::time::sleep(Duration::from_millis(500)).await;
        why = run.unhealthy().await;
    }
    if let Some(w) = &why {
        run.fail(format!("not healthy 120 s after the end: {w}"));
    }
    client.stop(load).await;
    let settled = client.settle(dbs, Duration::from_secs(60)).await;
    if !settled {
        run.fail("final upload of every database did not complete in 60 s".into());
    }
    let fin = run.checkpoint("final", settled).await;
    checkpoints.push(fin);

    // recovery times
    let acks = client.acks.lock().unwrap().clone();
    let mut per_db: HashMap<u64, Vec<(Instant, Instant)>> = HashMap::new();
    for a in &acks {
        per_db.entry(a.db).or_default().push((a.sent_at, a.at));
    }
    let mut suffix: HashMap<u64, (Vec<Instant>, Vec<Instant>)> = HashMap::new();
    for (db, mut v) in per_db {
        v.sort();
        let sent: Vec<Instant> = v.iter().map(|x| x.0).collect();
        let mut mins = vec![v.last().map_or(Instant::now(), |x| x.1); v.len()];
        let mut m: Option<Instant> = None;
        for i in (0..v.len()).rev() {
            m = Some(m.map_or(v[i].1, |m| m.min(v[i].1)));
            mins[i] = m.unwrap();
        }
        suffix.insert(db, (sent, mins));
    }
    let first_ack_after = |db: u64, t: Instant| -> Option<f64> {
        let (sent, mins) = suffix.get(&db)?;
        let i = sent.partition_point(|s| *s < t);
        mins.get(i).map(|x| (*x - t).as_secs_f64())
    };
    let mut writer_rec: Vec<f64> = vec![];
    let mut node_rec: Vec<f64> = vec![];
    let mut other_rec: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    let mut never = 0usize;
    let mut csv = String::from("n,t_s,kind,detail,heal_s,writer_dbs,node_dbs,max_writer_recovery_s,max_node_recovery_s\n");
    for (k, f) in faults.iter().enumerate() {
        let (mut mw, mut mn) = (0f64, 0f64);
        for &d in &f.writer_dbs {
            match first_ack_after(d, f.t) {
                Some(x) => {
                    writer_rec.push(x);
                    mw = mw.max(x);
                }
                None => never += 1,
            }
        }
        for &d in &f.node_dbs {
            match first_ack_after(d, f.t) {
                Some(x) => {
                    node_rec.push(x);
                    mn = mn.max(x);
                }
                None => never += 1,
            }
        }
        if f.writer_dbs.is_empty() && f.node_dbs.is_empty() {
            let v: Vec<f64> = (1..=dbs).filter_map(|d| first_ack_after(d, f.t)).collect();
            mn = v.iter().cloned().fold(0.0, f64::max);
            other_rec.entry(format!("{:?}", f.kind)).or_default().push(mn);
        }
        csv.push_str(&format!(
            "{},{:.1},{:?},\"{}\",{:.1},{},{},{:.2},{:.2}\n",
            k + 1,
            (f.t - run.t0).as_secs_f64(),
            f.kind,
            f.detail,
            f.healed.map_or(0.0, |h| (h - f.t).as_secs_f64()),
            f.writer_dbs.len(),
            f.node_dbs.len(),
            mw,
            mn
        ));
    }
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for f in &faults {
        *counts.entry(format!("{:?}", f.kind)).or_default() += 1;
    }
    let stats = client.stats.lock().unwrap().clone();
    let w = run.world.lock().unwrap();
    let opens: usize = w.writers.iter().map(|x| x.opens().len()).sum();
    let ev: Vec<String> = w.writers.iter().enumerate().flat_map(|(i, x)| x.events.lines().into_iter().map(move |(t, l)| (t, format!("w{i} {l}")))).map(|(t, l)| format!("{:.3} {l}", (t - run.t0).as_secs_f64())).collect();
    let n_ev: Vec<String> = w.nodes.iter().enumerate().flat_map(|(i, x)| x.events.lines().into_iter().map(move |(t, l)| (t, format!("n{i} {l}")))).map(|(t, l)| format!("{:.3} {l}", (t - run.t0).as_secs_f64())).collect();
    let mut wr = writer_rec.clone();
    let mut nr = node_rec.clone();
    let summary = json!({
        "seed": seed,
        "minutes_planned": minutes,
        "minutes_run": run.t0.elapsed().as_secs_f64() / 60.0,
        "mutation": mutation,
        "kinds": only,
        "rate_per_s": rate,
        "dbs": dbs,
        "writes_sent": stats.sent,
        "writes_acked": acks.len(),
        "writes_refused": stats.refused,
        "faults": faults.len(),
        "fault_counts": counts,
        "writer_opens": opens,
        "checkpoints": checkpoints,
        "recovery_writer_faults": {"n": wr.len(), "p50_s": pct(&mut wr, 50.0), "p99_s": pct(&mut wr, 99.0), "max_s": pct(&mut wr, 100.0),
            "over_15s": writer_rec.iter().filter(|x| **x > 15.0).count(), "over_40s": writer_rec.iter().filter(|x| **x > 40.0).count()},
        "recovery_node_faults": {"n": nr.len(), "p50_s": pct(&mut nr, 50.0), "p99_s": pct(&mut nr, 99.0), "max_s": pct(&mut nr, 100.0),
            "over_5s": node_rec.iter().filter(|x| **x > 5.0).count()},
        "recovery_other_faults_max_s": other_rec,
        "databases_never_recovered_after_a_fault": never,
        "wait_until_healthy_s": {"p50": pct(&mut unhealthy_waits.clone(), 50.0), "max": pct(&mut unhealthy_waits.clone(), 100.0)},
        "failures": w.failures,
        "notes": w.notes,
        "harness": {"work_dir": work.display().to_string(), "settled": settled},
    });
    drop(w);
    std::fs::write(out.join("summary.json"), serde_json::to_string_pretty(&summary).unwrap()).unwrap();
    std::fs::write(out.join("faults.csv"), csv).unwrap();
    let mut log = run.log.lock().unwrap().join("\n");
    log.push_str("\n--- writer events ---\n");
    log.push_str(&ev.join("\n"));
    log.push_str("\n--- node events ---\n");
    log.push_str(&n_ev.join("\n"));
    std::fs::write(out.join("events.txt"), log).unwrap();
    eprintln!("{}", serde_json::to_string_pretty(&summary).unwrap());
    let failed = run.failed();
    drop(run);
    std::process::exit(if failed { 1 } else { 0 });
}
