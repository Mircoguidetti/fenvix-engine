//! `--experiment lifecycle`: Steps 1-5 of the wake-lifecycle prototype (see results3.md).

use super::{latency, make_doc, request_cost, usd, Args, Latency};
use crate::counting::{CountingStore, Snapshot};
use crate::experiment::{shape, storage};
use crate::lifecycle::*;
use anyhow::{anyhow, Context, Result};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use rand::seq::index::sample;
use rand::Rng;
use slatedb::admin::Admin;
use slatedb::config::DbReaderOptions;
use slatedb::{DbReader, DbReaderMode, WalReader};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};

const STEP1_SAMPLES: usize = 100;
const STEP1_UNCLEAN: usize = 20;
const STEP2_WAKES: usize = 2000;
const TRACE_EVERY: usize = 4;
const STEP3_TENANTS: usize = 20;
const STEP3_SESSIONS: usize = 50;
const STEP3_STRESS_SESSIONS: usize = 50;
const CONTROL_SESSIONS: usize = 12;
const CRASH_TENANTS: usize = 20;
const CRASH_WRITES: usize = 10;
const STALL_SECS: u64 = 2;
const GC_AFTER: Duration = Duration::from_secs(20);
const GC_MIN_AGE: Duration = Duration::from_secs(15);

static DOC_UID: AtomicU64 = AtomicU64::new(0);

fn vhash(b: &[u8]) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    b.hash(&mut h);
    h.finish()
}

type Expected = Arc<Mutex<HashMap<usize, Vec<(String, u64)>>>>;

// ---------------------------------------------------------------------------
// Observation helpers
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Default)]
struct PhaseObs {
    dur: Duration,
    req: Snapshot,
    steps: Option<usize>,
}

async fn phase<T>(lc: &Arc<Lifecycle>, traced: bool, timeout: Option<Duration>, name: &str, f: impl Future<Output = Result<T>>) -> Result<(T, PhaseObs)> {
    let before = lc.counting.counters.snapshot();
    if traced {
        lc.counting.trace.start();
    }
    let t0 = Instant::now();
    let out = match timeout {
        Some(d) => tokio::time::timeout(d, f).await.map_err(|_| anyhow!("STALL in phase {name} (> {d:?})"))??,
        None => f.await?,
    };
    let dur = t0.elapsed();
    let steps = traced.then(|| shape(&lc.counting.trace.take()).steps);
    let req = lc.counting.counters.snapshot().since(&before);
    Ok((out, PhaseObs { dur, req, steps }))
}

struct Session {
    kind: &'static str,
    l0_at_wake: usize,
    phases: Vec<(&'static str, PhaseObs)>,
    unclean: bool,
}

impl Session {
    fn total(&self) -> Duration {
        self.phases.iter().map(|(_, p)| p.dur).sum()
    }
    fn steps(&self) -> Option<usize> {
        self.phases.iter().map(|(_, p)| p.steps).sum()
    }
    fn req(&self) -> Snapshot {
        self.phases.iter().fold(Snapshot::default(), |a, (_, p)| super::add(&a, &p.req))
    }
}

async fn read_session(lc: &Arc<Lifecycle>, id: usize, gets: usize, traced: bool, rng: &mut impl Rng) -> Result<Session> {
    let keys: Vec<String> = (0..gets).map(|_| format!("doc:{}", rng.gen_range(0..super::DOCS_PER_TENANT))).collect();
    let (t, wake) = phase(lc, traced, None, "wake_get", async {
        let t = lc.wake(id).await?;
        for k in &keys {
            anyhow::ensure!(t.get(k).await?.is_some(), "tenant {id} missing {k}");
        }
        Ok(t)
    })
    .await?;
    let unclean = t.unclean;
    let ((), close) = phase(lc, traced, None, "close", t.close()).await?;
    Ok(Session { kind: "read", l0_at_wake: 0, phases: vec![("wake+get", wake), ("close", close)], unclean })
}

async fn write_session(lc: &Arc<Lifecycle>, id: usize, docs: usize, traced: bool, timeout: Option<Duration>, expected: &Expected, rng: &mut impl Rng) -> Result<Session> {
    let (mut t, wake) = phase(lc, traced, timeout, "wake", lc.wake(id)).await?;
    let unclean = t.unclean;
    let l0_at_wake = t.l0_at_wake;
    let ((), upgrade) = phase(lc, traced, timeout, "upgrade", t.upgrade()).await?;
    let mut written = Vec::new();
    let (_, work) = phase(lc, traced, timeout, "work", async {
        for _ in 0..docs {
            let uid = DOC_UID.fetch_add(1, Relaxed);
            let key = format!("new:{uid}");
            let val = make_doc(id, uid as usize, rng);
            t.put(&key, &val).await?;
            written.push((key, vhash(&val)));
        }
        let (k, h) = &written[rng.gen_range(0..written.len())];
        let got = t.get(k).await?.ok_or_else(|| anyhow!("own write {k} not readable"))?;
        anyhow::ensure!(vhash(&got) == *h, "own write {k} has wrong value");
        Ok(())
    })
    .await?;
    let ((), close) = phase(lc, traced, timeout, "close", t.close()).await?;
    // Only record writes once the session closed cleanly and they are durable.
    expected.lock().unwrap().entry(id).or_default().extend(written);
    Ok(Session { kind: "write", l0_at_wake, phases: vec![("wake", wake), ("upgrade", upgrade), ("work", work), ("close", close)], unclean })
}

#[derive(Default)]
struct Agg {
    n: usize,
    lat: Vec<Duration>,
    req: Snapshot,
    steps: Vec<usize>,
}

impl Agg {
    fn add(&mut self, p: &PhaseObs) {
        self.n += 1;
        self.lat.push(p.dur);
        self.req = super::add(&self.req, &p.req);
        if let Some(s) = p.steps {
            self.steps.push(s);
        }
    }
    fn mean_steps(&self) -> f64 {
        if self.steps.is_empty() { f64::NAN } else { self.steps.iter().sum::<usize>() as f64 / self.steps.len() as f64 }
    }
    fn mean(&self, f: impl Fn(&Snapshot) -> u64) -> f64 {
        f(&self.req) as f64 / self.n.max(1) as f64
    }
    fn lat(&self) -> Latency {
        latency(self.lat.clone())
    }
    fn cost(&self) -> f64 {
        request_cost(&self.req) / self.n.max(1) as f64
    }
}

fn phase_row(s: &mut String, label: &str, a: &Agg) {
    let l = a.lat();
    let _ = writeln!(
        s, "| {label} | {} | {:.1} | {:.2} · {:.2} · {:.2} | {:.1} / {:.1} | {} |",
        a.n, a.mean_steps(),
        a.mean(|r| r.billed_put_tier()), a.mean(|r| r.billed_get_tier()), a.mean(|r| r.delete),
        l.p50_ms, l.p99_ms, usd(a.cost())
    );
}

const PHASE_HEADER: &str = "| Phase | Samples | Sequential steps (mean) | Requests: PUT/LIST-tier · GET-tier · DELETE | Latency p50 / p99 (ms, MinIO) | Request cost |\n|---|---|---|---|---|---|\n";

fn pct(v: &mut Vec<f64>, p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((p * v.len() as f64).ceil() as usize).max(1) - 1]
}

// ---------------------------------------------------------------------------
// Derived "is there unflushed WAL" check (independent of the marker)
// ---------------------------------------------------------------------------

/// Independent oracle for "this tenant has WAL data that is not in L0": the number of WAL rows
/// whose sequence number is above the manifest's `last_l0_seq`. (Counting WAL *files* newer than
/// `replay_after_wal_id` is not enough: a clean close leaves one redundant non-empty WAL whose
/// rows are already in L0.) Zero-byte WAL files are writer-fencing markers and are skipped.
async fn unflushed_wal_rows(raw: &Arc<dyn ObjectStore>, id: usize) -> Result<usize> {
    let admin = Admin::builder(tenant_path(id), Arc::clone(raw)).build();
    let m = admin.read_manifest(None).await?.context("no manifest")?;
    let (after, last_l0_seq) = (m.replay_after_wal_id(), m.last_l0_seq());
    let mut rows = 0;
    for f in WalReader::new(tenant_path(id), Arc::clone(raw)).list(after..).await? {
        if f.metadata().await?.metadata.size == 0 {
            continue;
        }
        let mut it = f.iterator().await?;
        while let Some(e) = it.next().await? {
            if e.seq > last_l0_seq {
                rows += 1;
            }
        }
    }
    Ok(rows)
}

async fn marker_present(raw: &Arc<dyn ObjectStore>, id: usize) -> Result<bool> {
    match raw.head(&marker_path(id)).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

async fn bytes_per_tenant(s3: &Arc<dyn ObjectStore>) -> Result<HashMap<usize, u64>> {
    let prefix = Path::from("tenants");
    let mut m: HashMap<usize, u64> = HashMap::new();
    let mut it = s3.list(Some(&prefix));
    while let Some(o) = it.try_next().await? {
        let id: usize = o.location.parts().nth(1).context("bad path")?.as_ref().parse()?;
        *m.entry(id).or_default() += o.size;
    }
    Ok(m)
}

// ---------------------------------------------------------------------------
// Crash child
// ---------------------------------------------------------------------------

fn crash_key(i: usize) -> String {
    format!("crash:{i}")
}

fn crash_val(tenant: usize, i: usize) -> Vec<u8> {
    let mut v = format!("crash-{tenant}-{i}-").into_bytes();
    v.resize(1024, b'x');
    v
}

/// Runs in a child process: open a writer through the normal lifecycle, write 10 documents,
/// await durability for the first `acks` of them (printing `ACK i` once durable), then print
/// `DONE` and wait to be killed.
pub async fn crash_child(args: &Args, s3: Arc<dyn ObjectStore>) -> Result<()> {
    let tenant = args.tenant.context("--tenant required")?;
    let acks = args.acks.context("--acks required")?;
    let lc = Lifecycle::new(Arc::new(CountingStore::new(s3)), None);
    let mut t = lc.wake(tenant).await?;
    for i in 0..CRASH_WRITES {
        let h = t.put(&crash_key(i), &crash_val(tenant, i)).await?;
        if i < acks {
            h.await_durable().await?;
            println!("ACK {i}");
        }
    }
    println!("DONE");
    tokio::time::sleep(Duration::from_secs(600)).await;
    Ok(())
}

/// Diagnostic: print a tenant's WAL position and WAL files (`--experiment probe-wal --tenant N`).
pub async fn probe_wal(args: &Args, s3: Arc<dyn ObjectStore>) -> Result<()> {
    let id = args.tenant.context("--tenant required")?;
    println!("unflushed WAL rows (seq > last_l0_seq): {}", unflushed_wal_rows(&s3, id).await?);
    let admin = Admin::builder(tenant_path(id), Arc::clone(&s3)).build();
    let m = admin.read_manifest(None).await?.context("no manifest")?;
    println!(
        "manifest {} replay_after_wal_id={} next_wal_sst_id={} last_l0_seq={} l0={} marker={}",
        m.id(), m.replay_after_wal_id(), m.next_wal_sst_id(), m.last_l0_seq(), m.l0().len(), marker_present(&s3, id).await?
    );
    println!("sorted runs={} sst views referenced by manifest={} checkpoints={}", m.compacted().len(), m.all_sst_views().count(), m.checkpoints().len());
    for f in WalReader::new(tenant_path(id), Arc::clone(&s3)).list(..).await? {
        println!("  wal {} size {}", f.id, f.metadata().await?.metadata.size);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Main experiment
// ---------------------------------------------------------------------------

struct Step3Tenant {
    max_l0_after_close: usize,
    max_l0_at_wake: usize,
    max_upgrade: Duration,
    max_close: Duration,
    sessions: usize,
    stalls: usize,
}

async fn step3_sessions(
    lc: &Arc<Lifecycle>, raw: &Arc<dyn ObjectStore>, expected: &Expected, ids: &[usize], sessions: usize,
    round_robin: bool, rng: &mut impl Rng,
) -> Result<Vec<Step3Tenant>> {
    let mut stats: Vec<Step3Tenant> = ids
        .iter()
        .map(|_| Step3Tenant { max_l0_after_close: 0, max_l0_at_wake: 0, max_upgrade: Duration::ZERO, max_close: Duration::ZERO, sessions: 0, stalls: 0 })
        .collect();
    let order: Vec<usize> = if round_robin {
        (0..sessions).flat_map(|_| 0..ids.len()).collect()
    } else {
        (0..ids.len()).flat_map(|i| std::iter::repeat(i).take(sessions)).collect()
    };
    for idx in order {
        let id = ids[idx];
        let docs = rng.gen_range(1..=10);
        let st = &mut stats[idx];
        st.sessions += 1;
        match write_session(lc, id, docs, false, Some(Duration::from_secs(15)), expected, rng).await {
            Ok(s) => {
                st.max_l0_at_wake = st.max_l0_at_wake.max(s.l0_at_wake);
                for (name, p) in &s.phases {
                    if *name == "upgrade" { st.max_upgrade = st.max_upgrade.max(p.dur); }
                    if *name == "close" { st.max_close = st.max_close.max(p.dur); }
                    if p.dur >= Duration::from_secs(STALL_SECS) {
                        st.stalls += 1;
                    }
                }
            }
            Err(e) if format!("{e:#}").contains("STALL") => {
                st.stalls += 1;
                eprintln!("   STALL tenant {id}: {e:#}");
                continue;
            }
            Err(e) => return Err(e),
        }
        let l0 = l0_count(raw, id).await?;
        st.max_l0_after_close = st.max_l0_after_close.max(l0);
    }
    Ok(stats)
}

pub async fn run(args: &Args, s3: Arc<dyn ObjectStore>) -> Result<()> {
    let n = args.tenants;
    anyhow::ensure!(n >= 400, "lifecycle experiment needs --tenants >= 400");
    let raw = Arc::clone(&s3);
    let counting = Arc::new(CountingStore::new(Arc::clone(&s3)));
    let mut rng = rand::thread_rng();
    let expected: Expected = Arc::new(Mutex::new(HashMap::new()));

    // ---- fresh load: existing load phase, plus the clean marker --------------------------
    eprintln!("load {n} tenants (existing load phase + clean marker)");
    let prefix = Path::from("tenants");
    let stale = s3.list(Some(&prefix)).map_ok(|m| m.location).boxed();
    s3.delete_stream(stale).try_collect::<Vec<_>>().await?;
    let load_store: Arc<dyn ObjectStore> = Arc::new(CountingStore::new(Arc::clone(&s3)));
    futures::stream::iter(0..n)
        .map(|id| {
            let st = Arc::clone(&load_store);
            async move {
                super::load_tenant(Arc::clone(&st), id).await?;
                st.put(&marker_path(id), object_store::PutPayload::from_static(b"clean")).await?;
                anyhow::Ok(())
            }
        })
        .buffer_unordered(32)
        .try_collect::<()>()
        .await?;
    let (bytes_loaded, _, _) = storage(&s3, n).await?;
    eprintln!("   loaded: {bytes_loaded:.0} B/tenant");

    // ---- lifecycle + background maintenance -------------------------------------------------
    let (queue, rx) = CompactionQueue::new();
    let lc = Lifecycle::new(Arc::clone(&counting), Some(Arc::clone(&queue)));
    let maint = Maintenance::start(Arc::clone(&s3), Arc::clone(&raw), Arc::clone(&queue), rx, Arc::clone(&lc.touched), Arc::clone(&lc.maint_locks), 4, GC_AFTER, GC_MIN_AGE);

    // Reserve disjoint tenant sets: step 3 (20), stress (1), control (1), crash (20).
    let pool: Vec<usize> = sample(&mut rng, n, STEP3_TENANTS + 2 + CRASH_TENANTS).into_vec();
    let step3_ids = pool[..STEP3_TENANTS].to_vec();
    let stress_id = pool[STEP3_TENANTS];
    let control_id = pool[STEP3_TENANTS + 1];
    let crash_ids = pool[STEP3_TENANTS + 2..].to_vec();
    let reserved: HashSet<usize> = pool.iter().copied().collect();
    let free: Vec<usize> = (0..n).filter(|i| !reserved.contains(i)).collect();

    // ================= Step 1: per-phase steps and requests (all traced) =================
    eprintln!("step 1: traced phases");
    let ids1: Vec<usize> = sample(&mut rng, free.len(), 2 * STEP1_SAMPLES + STEP1_UNCLEAN).into_iter().map(|i| free[i]).collect();
    let (read_ids, rest) = ids1.split_at(STEP1_SAMPLES);
    let (write_ids, unclean_ids) = rest.split_at(STEP1_SAMPLES);
    let mut s1_read_wake = Agg::default();
    let mut s1_read_close = Agg::default();
    let mut s1_w_wake = Agg::default();
    let mut s1_w_upgrade = Agg::default();
    let mut s1_w_work = Agg::default();
    let mut s1_w_close = Agg::default();
    let mut s1_unclean = Agg::default();
    for &id in read_ids {
        let s = read_session(&lc, id, 1, true, &mut rng).await?;
        s1_read_wake.add(&s.phases[0].1);
        s1_read_close.add(&s.phases[1].1);
    }
    for &id in write_ids {
        let docs = rng.gen_range(1..=10);
        let s = write_session(&lc, id, docs, true, None, &expected, &mut rng).await?;
        s1_w_wake.add(&s.phases[0].1);
        s1_w_upgrade.add(&s.phases[1].1);
        s1_w_work.add(&s.phases[2].1);
        s1_w_close.add(&s.phases[3].1);
    }
    // Cost of the unclean path, measured on tenants whose marker was removed by hand (their
    // data is in fact flushed; real unclean tenants are exercised in step 4).
    for &id in unclean_ids {
        raw.delete(&marker_path(id)).await?;
    }
    for &id in unclean_ids {
        let s = read_session(&lc, id, 1, true, &mut rng).await?;
        anyhow::ensure!(s.unclean, "expected the unclean path");
        s1_unclean.add(&s.phases[0].1);
    }
    // Restore the markers (a read-only wake does not repair them).
    for &id in unclean_ids {
        raw.put(&marker_path(id), object_store::PutPayload::from_static(b"clean")).await?;
    }

    // ================= Step 2: mixed workload =================
    eprintln!("step 2: {STEP2_WAKES} mixed wakes");
    let mut s2_read = (Agg::default(), Agg::default()); // (total, traced total for steps)
    let mut s2_write = (Agg::default(), Agg::default());
    let mut s2_phase: BTreeMap<String, Agg> = BTreeMap::new();
    let mut s2_reads = 0;
    let mut s2_writes = 0;
    let s2_before = counting.counters.snapshot();
    let s2_start = Instant::now();
    for i in 0..STEP2_WAKES {
        let id = free[rng.gen_range(0..free.len())];
        let traced = i % TRACE_EVERY == 0;
        let is_write = rng.gen_bool(0.2);
        let s = if is_write {
            let docs = rng.gen_range(1..=10);
            s2_writes += 1;
            write_session(&lc, id, docs, traced, None, &expected, &mut rng).await?
        } else {
            let gets = rng.gen_range(1..=5);
            s2_reads += 1;
            read_session(&lc, id, gets, traced, &mut rng).await?
        };
        let (total, tr) = if is_write { (&mut s2_write.0, &mut s2_write.1) } else { (&mut s2_read.0, &mut s2_read.1) };
        let tot = PhaseObs { dur: s.total(), req: s.req(), steps: None };
        total.add(&tot);
        if let Some(steps) = s.steps() {
            tr.add(&PhaseObs { dur: s.total(), req: s.req(), steps: Some(steps) });
        }
        for (name, p) in &s.phases {
            s2_phase.entry(format!("{}: {}", s.kind, name)).or_default().add(p);
        }
    }
    let s2_secs = s2_start.elapsed().as_secs_f64();
    let s2_req = counting.counters.snapshot().since(&s2_before);
    eprintln!("   done in {s2_secs:.1}s ({s2_reads} read, {s2_writes} write)");

    // ================= Step 3: 20 tenants x 50 sessions, plus stress and control =================
    eprintln!("step 3: {STEP3_TENANTS} tenants x {STEP3_SESSIONS} sessions with the compactor running");
    let guard_before = lc.stats.lock().unwrap().guard_waits;
    let s3_rows = step3_sessions(&lc, &raw, &expected, &step3_ids, STEP3_SESSIONS, true, &mut rng).await?;
    let s3_guard_waits = lc.stats.lock().unwrap().guard_waits - guard_before;
    eprintln!("   stress: one tenant, {STEP3_STRESS_SESSIONS} back-to-back sessions");
    let guard_before = lc.stats.lock().unwrap().guard_waits;
    let stress = step3_sessions(&lc, &raw, &expected, &[stress_id], STEP3_STRESS_SESSIONS, false, &mut rng).await?;
    let stress_guard_waits = lc.stats.lock().unwrap().guard_waits - guard_before;
    eprintln!("   control: no compactor, no guard, {CONTROL_SESSIONS} sessions");
    let control_lc = Lifecycle::with_guard(Arc::new(CountingStore::new(Arc::clone(&s3))), None, false);
    let control_expected: Expected = Arc::new(Mutex::new(HashMap::new()));
    let mut control_log = Vec::new();
    for sess in 1..=CONTROL_SESSIONS {
        let before_l0 = l0_count(&raw, control_id).await?;
        let t0 = Instant::now();
        let r = write_session(&control_lc, control_id, 5, false, Some(Duration::from_secs(5)), &control_expected, &mut rng).await;
        let after_l0 = l0_count(&raw, control_id).await?;
        match r {
            Ok(_) => control_log.push(format!("session {sess}: ok in {:.0} ms, L0 {before_l0} -> {after_l0}", t0.elapsed().as_secs_f64() * 1000.0)),
            Err(e) => {
                control_log.push(format!("session {sess}: {e:#} (L0 at start {before_l0}, now {after_l0})"));
                break;
            }
        }
    }

    let unclean_before_step4 = lc.stats.lock().unwrap().unclean_wakes;
    // ================= Step 4: crash safety =================
    eprintln!("step 4: {CRASH_TENANTS} SIGKILLed writers");
    let exe = std::env::current_exe()?;
    struct CrashRow { tenant: usize, acked: usize, unflushed_rows: usize, marker_after_kill: bool, unclean_path: bool, acked_lost: usize, unacked_found: usize, unsafe_visible: Option<usize>, repaired: bool, second_wake_ok: bool }
    let mut crash_rows = Vec::new();
    for (ci, &id) in crash_ids.iter().enumerate() {
        let acks = rng.gen_range(3..=CRASH_WRITES);
        let mut child = tokio::process::Command::new(&exe)
            .args(["--experiment", "crash-child", "--tenant", &id.to_string(), "--acks", &acks.to_string()])
            .stdout(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let mut acked = Vec::new();
        loop {
            let line = tokio::time::timeout(Duration::from_secs(30), lines.next_line()).await.context("child timed out")??
                .ok_or_else(|| anyhow!("child exited early"))?;
            if let Some(i) = line.strip_prefix("ACK ") {
                acked.push(i.parse::<usize>()?);
            } else if line == "DONE" {
                break;
            }
        }
        child.kill().await?; // SIGKILL
        anyhow::ensure!(acked.len() == acks, "child acknowledged {} of {acks}", acked.len());

        let unflushed_rows = unflushed_wal_rows(&raw, id).await?;
        let marker_after_kill = marker_present(&raw, id).await?;
        // Normal lifecycle wake.
        let t = lc.wake(id).await?;
        let unclean_path = t.unclean;
        let mut lost = 0;
        for &i in &acked {
            match t.get(&crash_key(i)).await? {
                Some(v) if v.as_ref() == crash_val(id, i).as_slice() => {}
                _ => lost += 1,
            }
        }
        let mut unacked_found = 0;
        for i in acks..CRASH_WRITES {
            if t.get(&crash_key(i)).await?.is_some() {
                unacked_found += 1;
            }
        }
        // The original data must still be there too.
        anyhow::ensure!(t.get("doc:0").await?.is_some(), "tenant {id} lost its loaded data");
        t.close().await?;
        // Negative control (first 5): what the unsafe skip_wal_replay path would have shown.
        let unsafe_visible = if ci < 5 {
            let r = DbReader::builder(tenant_path(id), Arc::clone(&s3))
                .with_reader_mode(DbReaderMode::FollowLatest)
                .with_options({ let mut o = DbReaderOptions::default(); o.skip_wal_replay = true; o })
                .build().await?;
            let mut seen = 0;
            for &i in &acked {
                if r.get(&crash_key(i)).await?.is_some() { seen += 1; }
            }
            r.close().await?;
            Some(seen)
        } else { None };
        // Recovery: a write session persists the replayed data and restores the marker.
        let docs = 2;
        write_session(&lc, id, docs, false, None, &expected, &mut rng).await?;
        let repaired = marker_present(&raw, id).await? && unflushed_wal_rows(&raw, id).await? == 0;
        let t = lc.wake(id).await?;
        let mut ok = !t.unclean;
        for &i in &acked {
            ok &= matches!(t.get(&crash_key(i)).await?, Some(v) if v.as_ref() == crash_val(id, i).as_slice());
        }
        t.close().await?;
        crash_rows.push(CrashRow { tenant: id, acked: acked.len(), unflushed_rows, marker_after_kill, unclean_path, acked_lost: lost, unacked_found, unsafe_visible, repaired, second_wake_ok: ok });
    }

    // ================= Quiesce, final maintenance, final verification =================
    eprintln!("quiesce: drain compactions, final compaction + GC sweep");
    let log = maint.shutdown().await;
    let (storage_before_final, _, by_dir_before) = storage(&s3, n).await?;
    let all: Vec<usize> = (0..n).filter(|&i| i != control_id).collect();
    let sweep_log: Arc<Mutex<Vec<CompactionRecord>>> = Arc::new(Mutex::new(Vec::new()));
    futures::stream::iter(all.clone())
        .map(|id| {
            let (s3, raw, sweep_log) = (Arc::clone(&s3), Arc::clone(&raw), Arc::clone(&sweep_log));
            async move {
                if let Some(rec) = compact_tenant(&s3, &raw, id).await? {
                    sweep_log.lock().unwrap().push(rec);
                }
                anyhow::Ok(())
            }
        })
        .buffer_unordered(8)
        .try_collect::<()>()
        .await?;
    let final_gc: Vec<GcRecord> = futures::stream::iter(all.clone())
        .map(|id| { let s3 = Arc::clone(&s3); async move { gc_tenant(&s3, id, Duration::ZERO).await } })
        .buffer_unordered(16)
        .try_collect()
        .await?;
    let (storage_final, objects_final, by_dir_final) = storage(&s3, n).await?;
    let per_tenant = bytes_per_tenant(&s3).await?;
    let mut final_l0_max = 0;
    let mut final_cp_max = 0;
    for &id in &all {
        final_l0_max = final_l0_max.max(l0_count(&raw, id).await?);
        final_cp_max = final_cp_max.max(checkpoint_count(&raw, id).await?);
    }

    // Every acknowledged write of steps 1-4 must be readable through the normal lifecycle.
    eprintln!("verify all acknowledged writes");
    let verify_lc = Lifecycle::new(Arc::new(CountingStore::new(Arc::clone(&s3))), None);
    let (mut verified, mut missing, mut tenants_verified) = (0usize, 0usize, 0usize);
    let exp = expected.lock().unwrap().clone();
    for (&id, writes) in &exp {
        let t = verify_lc.wake(id).await?;
        anyhow::ensure!(!t.unclean, "tenant {id} is still unclean after clean closes");
        for (k, h) in writes {
            match t.get(k).await? {
                Some(v) if vhash(&v) == *h => verified += 1,
                _ => missing += 1,
            }
        }
        t.close().await?;
        tenants_verified += 1;
    }
    let log = log.lock().unwrap();

    // ---------------- report ----------------
    let mut r = String::new();
    let _ = writeln!(r, "## Step 1 — Lazy-writer lifecycle: sequential steps and requests per phase ({STEP1_SAMPLES} traced samples each, distinct tenants)\n");
    let _ = writeln!(r, "Wake = clean-marker HEAD in parallel with a speculative `DbReader` (FollowLatest, `skip_wal_replay`, 1 h poll) open. Upgrade = marker DELETE in parallel with the V4 writer open (reader closed first). Close = `Db::close()` flush, then marker PUT. Request counts are exact; steps merge overlapping calls (see results2.md).\n");
    r.push_str(PHASE_HEADER);
    phase_row(&mut r, "(1) Read-only wake (wake + 1 get)", &s1_read_wake);
    phase_row(&mut r, "(1b) Read-only close", &s1_read_close);
    phase_row(&mut r, "Write session: reader wake only", &s1_w_wake);
    phase_row(&mut r, "(2) Upgrade reader → writer", &s1_w_upgrade);
    phase_row(&mut r, "Write session: 1–10 puts + 1 get (in memory)", &s1_w_work);
    phase_row(&mut r, "(3) Clean writer close (flush + marker)", &s1_w_close);
    phase_row(&mut r, "Unclean-path wake (marker missing; wake + 1 get)*", &s1_unclean);
    let _ = writeln!(r, "\n\\* Marker removed by hand on {STEP1_UNCLEAN} tenants whose data was actually flushed, to price the slow path. Real unclean tenants are in Step 4.\n");

    let (rl, wl) = (s2_read.0.lat(), s2_write.0.lat());
    let _ = writeln!(r, "## Step 2 — Mixed workload: {STEP2_WAKES} wakes on {n} tenants ({s2_reads} read-only, {s2_writes} write sessions), run sequentially with the compactor and GC loops active\n");
    let _ = writeln!(r, "| Wake type | Wakes | Latency p50 / p99 (ms, MinIO) | Sequential steps (mean, {} traced) | Requests per wake: PUT/LIST-tier · GET-tier · DELETE | Total requests: PUT/LIST-tier · GET-tier |", TRACE_EVERY);
    let _ = writeln!(r, "|---|---|---|---|---|---|");
    for (label, a, tr, l) in [("Read-only (1–5 gets, then idle/closed)", &s2_read.0, &s2_read.1, &rl), ("Write session (1–10 puts, 1 get, clean close)", &s2_write.0, &s2_write.1, &wl)] {
        let _ = writeln!(
            r, "| {label} | {} | {:.1} / {:.1} | {:.1} | {:.2} · {:.2} · {:.2} | {} · {} |",
            a.n, l.p50_ms, l.p99_ms, tr.mean_steps(),
            a.mean(|x| x.billed_put_tier()), a.mean(|x| x.billed_get_tier()), a.mean(|x| x.delete),
            a.req.billed_put_tier(), a.req.billed_get_tier()
        );
    }
    let _ = writeln!(r, "| **All wakes (lifecycle only, excl. maintenance)** | {} | | | | **{} · {}** |", STEP2_WAKES, s2_req.billed_put_tier(), s2_req.billed_get_tier());
    let _ = writeln!(r, "\nWorkload wall time {s2_secs:.1} s. Per-phase breakdown of the same wakes:\n");
    r.push_str(PHASE_HEADER);
    for (name, a) in &s2_phase {
        phase_row(&mut r, name, a);
    }
    let _ = writeln!(r, "\nUnclean wakes taken in steps 1–3: {} (expected {} from the deliberate marker removal).\n", unclean_before_step4, STEP1_UNCLEAN);

    let _ = writeln!(r, "## Step 3 — Offline compaction and GC while the lifecycle runs\n");
    let _ = writeln!(r, "Compaction: a bounded standalone compactor pass (`CompactorBuilder::run`, 50 ms poll, embedded worker) per tenant, queued when a writer closes with ≥ {L0_COMPACT_THRESHOLD} L0 SSTs, 4 passes at a time. GC: `Admin::run_gc_once` {}s after a tenant's last write, files older than {}s. Writers (V4 settings) have no in-process compactor or GC. A writer that wakes with ≥ {L0_GUARD} L0 SSTs waits for compaction first (the L0 limit is 8).\n", GC_AFTER.as_secs(), GC_MIN_AGE.as_secs());
    let _ = writeln!(r, "| Scenario | Tenants × sessions | Max L0 SSTs after any close | Max L0 at wake | Slowest upgrade / close (ms) | Ops ≥ {STALL_SECS} s or timed out | Guard waits |\n|---|---|---|---|---|---|---|");
    let fmt_s3 = |label: &str, rows: &[Step3Tenant], guard: u64| {
        let ms = |d: Duration| d.as_secs_f64() * 1000.0;
        format!(
            "| {label} | {} × {} | {} | {} | {:.0} / {:.0} | {} | {} |\n",
            rows.len(), rows[0].sessions,
            rows.iter().map(|x| x.max_l0_after_close).max().unwrap_or(0),
            rows.iter().map(|x| x.max_l0_at_wake).max().unwrap_or(0),
            ms(rows.iter().map(|x| x.max_upgrade).max().unwrap_or_default()),
            ms(rows.iter().map(|x| x.max_close).max().unwrap_or_default()),
            rows.iter().map(|x| x.stalls).sum::<usize>(), guard
        )
    };
    r.push_str(&fmt_s3("Round-robin sessions", &s3_rows, s3_guard_waits));
    r.push_str(&fmt_s3("Stress: one tenant, back-to-back", &stress, stress_guard_waits));
    let _ = writeln!(r, "\nPer-tenant maximum L0 count after a session, 20-tenant run: {}.\n", s3_rows.iter().map(|x| x.max_l0_after_close.to_string()).collect::<Vec<_>>().join(", "));
    let _ = writeln!(r, "**Control (detector check): no compactor, no guard, one tenant, 5 puts per session, 5 s timeout per phase:**\n");
    for l in &control_log {
        let _ = writeln!(r, "- {l}");
    }
    let comp: Vec<&CompactionRecord> = log.compactions.iter().chain(std::iter::empty()).collect();
    let mut secs: Vec<f64> = comp.iter().map(|c| c.secs).collect();
    let sum_req = |v: &[&CompactionRecord]| v.iter().fold(Snapshot::default(), |a, c| super::add(&a, &c.requests));
    let creq = sum_req(&comp);
    let nc = comp.len().max(1) as f64;
    let ngc = log.gcs.len().max(1) as f64;
    let gcreq = log.gcs.iter().fold(Snapshot::default(), |a, g| super::add(&a, &g.requests));
    let _ = writeln!(r, "\n| Maintenance (during steps 1–4) | Passes | Requests per pass: PUT/LIST-tier · GET-tier · DELETE | Wall time per pass p50 / p99 (s) | Mean L0 before → after, checkpoints deleted per pass |\n|---|---|---|---|---|");
    let _ = writeln!(
        r, "| Compaction (standalone compactor, incl. checkpoint cleanup) | {} | {:.2} · {:.2} · {:.2} | {:.2} / {:.2} | {:.1} → {:.1}, {:.1} |",
        comp.len(), creq.billed_put_tier() as f64 / nc, creq.billed_get_tier() as f64 / nc, creq.delete as f64 / nc,
        pct(&mut secs, 0.5), pct(&mut secs, 0.99),
        comp.iter().map(|c| c.l0_before as f64).sum::<f64>() / nc, comp.iter().map(|c| c.l0_after as f64).sum::<f64>() / nc,
        comp.iter().map(|c| c.checkpoints_deleted as f64).sum::<f64>() / nc
    );
    let _ = writeln!(
        r, "| GC (`run_gc_once`, min_age {}s) | {} | {:.2} · {:.2} · {:.2} | | |",
        GC_MIN_AGE.as_secs(), log.gcs.len(), gcreq.billed_put_tier() as f64 / ngc, gcreq.billed_get_tier() as f64 / ngc, gcreq.delete as f64 / ngc
    );
    let sweep = sweep_log.lock().unwrap();
    let fgc = final_gc.iter().fold(Snapshot::default(), |a, g| super::add(&a, &g.requests));
    let _ = writeln!(r, "\nMaintenance errors: {}. Final sweep after quiesce: {} tenants still needed compaction; final GC (min_age 0) over {} tenants used {} PUT/LIST-tier + {} GET-tier + {} DELETE requests in total. Maximum L0 count across all tenants after the final sweep: {final_l0_max}; maximum checkpoints left in any manifest: {final_cp_max}.\n",
        log.errors.len(), sweep.len(), all.len(), fgc.billed_put_tier(), fgc.billed_get_tier(), fgc.delete);
    if !log.errors.is_empty() {
        for e in log.errors.iter().take(5) {
            let _ = writeln!(r, "- error: {e}");
        }
    }
    let _ = writeln!(r, "| Storage per tenant | Bytes | Objects |\n|---|---|---|");
    let _ = writeln!(r, "| After load | {bytes_loaded:.0} | |");
    let _ = writeln!(r, "| After steps 1–4, before final compaction/GC | {storage_before_final:.0} | |");
    let _ = writeln!(r, "| **After final compaction + GC (min_age 0)** | **{storage_final:.0}** | {objects_final:.2} |");
    let dirs = |m: &BTreeMap<String, f64>| m.iter().map(|(k, v)| format!("{k}/ {v:.0}")).collect::<Vec<_>>().join(" · ");
    let avg = |ids: &mut dyn Iterator<Item = usize>| {
        let v: Vec<u64> = ids.filter_map(|i| per_tenant.get(&i).copied()).collect();
        (v.iter().sum::<u64>() as f64 / v.len().max(1) as f64, v.len())
    };
    let never_written: Vec<usize> = (0..n).filter(|i| *i != control_id && !exp.contains_key(i)).collect();
    let (b20, _) = avg(&mut step3_ids.iter().copied());
    let (bstress, _) = avg(&mut std::iter::once(stress_id));
    let (bnever, nn) = avg(&mut never_written.iter().copied());
    let (bwritten, nw) = avg(&mut exp.keys().copied());
    let _ = writeln!(r, "\nFinal bytes per tenant by usage: never written {bnever:.0} ({nn} tenants); written at least once {bwritten:.0} ({nw} tenants); the 20 tenants with 50 sessions each {b20:.0}; the stress tenant (50 back-to-back sessions) {bstress:.0}.");
    let _ = writeln!(r, "\nBy directory, before final pass: {}. After: {}.\n", dirs(&by_dir_before), dirs(&by_dir_final));

    let _ = writeln!(r, "## Step 4 — Crash safety: {CRASH_TENANTS} writers killed with SIGKILL\n");
    let _ = writeln!(r, "Each child opens a writer through the lifecycle, writes {CRASH_WRITES} documents, awaits durability (`WriteHandle::await_durable`) for the first K (3–10) and prints `ACK i` for each; the parent kills it with SIGKILL as soon as all writes were issued. \"WAL rows not in L0\" is an independent oracle that does not use the marker: rows in non-empty WAL files whose sequence number is above the manifest's `last_l0_seq`.\n");
    let _ = writeln!(r, "| Tenant | Acked | WAL rows not in L0 (seq > `last_l0_seq`) | Marker present after kill | Unclean path taken | Acked writes lost | Un-acked writes recovered | Acked visible with unsafe `skip_wal_replay` | Clean after recovery session | Re-wake clean and complete |\n|---|---|---|---|---|---|---|---|---|---|");
    for c in &crash_rows {
        let _ = writeln!(r, "| {} | {} | {} | {} | {} | **{}** | {} | {} | {} | {} |", c.tenant, c.acked, c.unflushed_rows,
            if c.marker_after_kill { "yes" } else { "no" }, if c.unclean_path { "yes" } else { "no" }, c.acked_lost, c.unacked_found,
            c.unsafe_visible.map(|v| format!("{v} of {}", c.acked)).unwrap_or_else(|| "–".into()),
            if c.repaired { "yes" } else { "no" }, if c.second_wake_ok { "yes" } else { "no" });
    }
    let total_acked: usize = crash_rows.iter().map(|c| c.acked).sum();
    let total_lost: usize = crash_rows.iter().map(|c| c.acked_lost).sum();
    let _ = writeln!(r, "\n**Totals:** {} acknowledged writes, **{total_lost} lost**; unclean path taken for {} of {} tenants; {} of {} tenants had WAL rows not yet in L0 (so skipping WAL replay would have lost data).\n",
        total_acked, crash_rows.iter().filter(|c| c.unclean_path).count(), crash_rows.len(),
        crash_rows.iter().filter(|c| c.unflushed_rows > 0).count(), crash_rows.len());
    let _ = writeln!(r, "**End-to-end check after final compaction and GC:** every write acknowledged in steps 1–4 (incl. post-recovery writes) was read back through the normal lifecycle: {verified} verified, **{missing} missing or wrong**, across {tenants_verified} tenants.\n");

    // ---- Step 5: costs and projections ----
    let read_cost = s2_read.0.cost();
    let write_cost = s2_write.0.cost();
    let up_cost = s1_w_upgrade.cost();
    let compaction_cost = request_cost(&creq) / nc;
    let gc_cost = request_cost(&gcreq) / ngc;
    let _ = writeln!(r, "## Step 5 — Cost (S3 Standard list prices: PUT/COPY/POST/LIST $0.005 per 1,000; GET $0.0004 per 1,000) and projected latency\n");
    let _ = writeln!(r, "| Operation | Basis | Request cost |\n|---|---|---|");
    let _ = writeln!(r, "| Read-only wake (wake + 1–5 gets + close) | mean of {} wakes | **{}** |", s2_read.0.n, usd(read_cost));
    let _ = writeln!(r, "| Write session (wake + upgrade + puts/get + close) | mean of {} sessions | **{}** |", s2_write.0.n, usd(write_cost));
    let _ = writeln!(r, "| of which the upgrade | {} samples, step 1 | {} |", s1_w_upgrade.n, usd(up_cost));
    let _ = writeln!(r, "| One compaction pass | mean of {} passes | **{}** |", comp.len(), usd(compaction_cost));
    let _ = writeln!(r, "| One GC pass per tenant | mean of {} passes | {} |", log.gcs.len(), usd(gc_cost));
    let write_sessions_total = (STEP1_SAMPLES + s2_writes + STEP3_TENANTS * STEP3_SESSIONS + STEP3_STRESS_SESSIONS + CRASH_TENANTS) as f64;
    let amort = (request_cost(&creq) + request_cost(&gcreq)) / write_sessions_total;
    let _ = writeln!(r, "| Compaction + GC amortised over all {write_sessions_total:.0} write sessions of steps 1–4 | {} compaction + {} GC passes | {} per write session |", comp.len(), log.gcs.len(), usd(amort));
    let _ = writeln!(r, "| **All-in write session** (session + amortised maintenance) | | **{}** |", usd(write_cost + amort));
    let _ = writeln!(r, "\n**Projected** latency (NOT measured): sequential steps × 5 ms (S3 Express One Zone assumption) or × 30 ms (S3 Standard assumption). Ignores transfer time and tail latency.\n");
    let _ = writeln!(r, "| Phase / wake type | Sequential steps (traced mean) | Projected @ 5 ms | Projected @ 30 ms |\n|---|---|---|---|");
    let proj = |r: &mut String, label: &str, steps: f64| {
        let _ = writeln!(r, "| {label} | {steps:.1} | {:.0} ms | {:.0} ms |", steps * 5.0, steps * 30.0);
    };
    proj(&mut r, "Read-only wake + 1 get (step 1)", s1_read_wake.mean_steps());
    proj(&mut r, "Upgrade reader → writer (step 1)", s1_w_upgrade.mean_steps());
    proj(&mut r, "Clean writer close (step 1)", s1_w_close.mean_steps());
    proj(&mut r, "Unclean-path wake + 1 get (step 1)", s1_unclean.mean_steps());
    proj(&mut r, "Whole read-only session incl. close (step 2)", s2_read.1.mean_steps());
    proj(&mut r, "Whole write session, wake→close (step 2)", s2_write.1.mean_steps());

    std::fs::create_dir_all(&args.out_dir)?;
    std::fs::write(format!("{}/lifecycle_n{n}.md", args.out_dir), &r)?;
    println!("{r}");
    Ok(())
}
