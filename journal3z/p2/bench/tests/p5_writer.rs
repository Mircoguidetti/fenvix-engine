//! P5: the writer client on real processes (3 nodes, writers, MinIO, fault proxy), all on this machine.
//! Every check has a twin run with a bug switched on, which must make the same check fail.
//!
//! - `lease_is_exclusive`: three servers race for the same lease; exactly one wins each round (conditional PUT).
//!   Twin: unconditional writes → several winners.
//! - `store_fence_rejects_the_old_writer`: after a newer writer opened the store, the older one's upload is refused.
//!   Twin: `skip_store_fence` → accepted.
//! - `writer_kill_takeover`: the owner is killed (-9) while one node lags; the other writer takes over while it can
//!   reach only the lagging node, then all of them. 0 acknowledged writes lost. Twin: `recovery_single` → lost.
//! - `clock_jump_old_writer_fenced`: the second writer's clock jumps +60 s, it takes over while the old owner still
//!   believes it holds the lease. 0 losses, 0 acknowledgements from the old epoch at or above the new barrier.
//!   Twin: nodes with `no_fencing` → late acknowledgements.
//! - `lost_disk_reconfig`: a node loses its disk (new identity); the writer replaces it at an upload point; then
//!   another node dies for good and writes still get acknowledged. Twin: reconfiguration switched off → no more acks.
use j3z_bench::*;
use j3z_writer::meta::{LeaseOps, MetaError, StoreOps};
use journal3z_core::lease::{LeaseBug, Observer, TakeoverRule};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn report(name: &str, s: &str) {
    eprintln!("[{name}] {s}");
}

// ------------------------------------------------------------------------------------------------ lease / store fence

async fn lease_rounds(unconditional: bool) -> (usize, usize) {
    let dir = tempfile::tempdir().unwrap();
    let minio = Minio::start(&dir.path().join("minio"));
    minio.ready().await;
    #[allow(unused_mut)]
    let mut meta = minio.meta();
    meta.unconditional = unconditional;
    let (mut rounds_ok, mut rounds_bad) = (0, 0);
    for db in 1..=100u64 {
        let ws: Vec<_> = (0..3)
            .map(|i| {
                let meta = meta.clone();
                tokio::spawn(async move {
                    let holder = format!("s{i}");
                    let rule = TakeoverRule { ttl: 10_000, margin: 1_500, bug: LeaseBug::None };
                    let ops = LeaseOps { meta: &meta, holder: &holder, rule };
                    // each reads the lease and tries to take it when the rule allows (free, or ours); whoever reads
                    // after another server took it must not take it
                    let cur = ops.read(db).await.unwrap();
                    if !ops.may_take(&cur, &mut Observer::default(), 0, 0) {
                        return false;
                    }
                    match ops.take(db, &cur, &[1, 2, 3], 0).await {
                        Ok(_) => true,
                        Err(MetaError::Conflict) => false,
                        Err(e) => panic!("round {db}: server {i}: store error (not a lost race): {e}"),
                    }
                })
            })
            .collect();
        let mut winners = 0;
        for w in ws {
            winners += w.await.unwrap() as usize;
        }
        if winners == 1 {
            rounds_ok += 1;
        } else {
            eprintln!("round {db}: {winners} winners");
            rounds_bad += 1;
        }
    }
    (rounds_ok, rounds_bad)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lease_is_exclusive() {
    let (ok, bad) = lease_rounds(false).await;
    report("lease_is_exclusive", &format!("correct: {ok} rounds with exactly 1 winner, {bad} with 0 or more than 1"));
    assert_eq!((ok, bad), (100, 0));
    let (ok2, bad2) = lease_rounds(true).await;
    report("lease_is_exclusive", &format!("twin (unconditional writes): {ok2} rounds with 1 winner, {bad2} with 0 or more than 1"));
    assert!(bad2 > 0, "the check does not see unconditional writes");
}

async fn store_fence(skip: bool) -> bool {
    let dir = tempfile::tempdir().unwrap();
    let minio = Minio::start(&dir.path().join("minio"));
    minio.ready().await;
    let meta = minio.meta();
    let ops = StoreOps { meta: &meta, skip_fence: skip };
    assert_eq!(ops.open(1, 1).await.unwrap(), 0);
    ops.upload(1, 1, 10, &[(5, 55), (10, 100)]).await.unwrap();
    assert_eq!(ops.open(1, 2).await.unwrap(), 10); // the newer writer reads the marker
    ops.upload(1, 2, 12, &[(11, 111), (12, 112)]).await.unwrap();
    let old = ops.upload(1, 1, 11, &[(11, 999)]).await;
    let (m, content) = meta.content(1).await.unwrap();
    report("store_fence", &format!("skip_fence={skip}: old writer's upload -> {old:?}; marker {}; index 11 holds {:?}", m.applied, content.get(&11)));
    matches!(old, Err(MetaError::Conflict))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn store_fence_rejects_the_old_writer() {
    assert!(store_fence(false).await, "the old writer's upload was accepted");
    assert!(!store_fence(true).await, "twin: without the fence the old upload must go through");
}

/// Regression (bench trial 3): two uploads at the same index and epoch must both stay readable; the second (empty)
/// one must not replace the first. Twin: the old object naming (index, epoch), which overwrote.
async fn uploads_case(fixed_names: bool) -> Option<u64> {
    let dir = tempfile::tempdir().unwrap();
    let minio = Minio::start(&dir.path().join("minio"));
    minio.ready().await;
    let mut meta = minio.meta();
    meta.fixed_chunk_names = fixed_names;
    let ops = StoreOps { meta: &meta, skip_fence: false };
    ops.open(1, 35).await.unwrap();
    ops.upload(1, 35, 971, &[(610, 20579), (971, 7)]).await.unwrap();
    ops.upload(1, 35, 971, &[]).await.unwrap();
    let (_, content) = meta.content(1).await.unwrap();
    report("uploads_never_replace_each_other", &format!("fixed_names={fixed_names}: index 610 holds {:?}", content.get(&610)));
    content.get(&610).copied()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uploads_never_replace_each_other() {
    assert_eq!(uploads_case(false).await, Some(20579));
    assert_eq!(uploads_case(true).await, None, "twin: with the old naming the record must be gone");
}

// ------------------------------------------------------------------------------------------------ end to end

struct Bench {
    _dir: tempfile::TempDir,
    minio: Minio,
    nodes: Vec<NodeProc>,
    proxy: Proxy,
    writers: Vec<WriterProc>,
    dbs: u64,
}

impl Bench {
    fn new(nwriters: usize, dbs: u64, node_mutation: Option<&str>) -> Bench {
        let dir = tempfile::tempdir().unwrap();
        let minio = Minio::start(&dir.path().join("minio"));
        let nodes: Vec<NodeProc> = (0..3).map(|i| NodeProc::start(&dir.path().join(format!("n{i}")), node_mutation)).collect();
        let addrs: Vec<String> = nodes.iter().map(|n| n.addr()).collect();
        let proxy = Proxy::start(&dir.path().join("proxy"), nwriters, &addrs);
        Bench { _dir: dir, minio, nodes, proxy, writers: vec![], dbs }
    }

    fn add_writer(&mut self, nwriters: usize, mutation: Option<&str>, env: Vec<(&str, &str)>) {
        self.add_writer_cfg(nwriters, mutation, env, fast_config());
    }

    fn add_writer_cfg(&mut self, nwriters: usize, mutation: Option<&str>, env: Vec<(&str, &str)>, config: serde_json::Value) {
        let i = self.writers.len();
        let w = WriterProc::start(WriterSpec {
            index: i,
            nwriters,
            nodes: self.proxy.node_addrs(i, 3),
            dbs: self.dbs,
            minio: &self.minio,
            config,
            dir: &self._dir.path().join("writers"),
            mutation,
            env,
        });
        self.writers.push(w);
    }

    fn client(&self) -> Arc<Client> {
        Client::new(&self.writers.iter().map(|w| w.client.clone()).collect::<Vec<_>>())
    }

    async fn wait_owner(&self, c: &Client, w: usize, limit: Duration) -> bool {
        let t0 = Instant::now();
        while t0.elapsed() < limit {
            let o = c.owners();
            if (1..=self.dbs).all(|d| o.get(&d) == Some(&w)) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    fn opens(&self) -> Vec<(u64, u64, u64)> {
        self.writers.iter().flat_map(|w| w.opens()).map(|(_, d, e, b)| (d, e, b)).collect()
    }

    async fn check(&self, c: &Client) -> Report {
        let acks = c.acks.lock().unwrap().clone();
        check(&self.minio.meta(), &acks, &self.opens(), self.dbs).await.unwrap()
    }
}

fn summary(r: &Report, c: &Client) -> String {
    let s = c.stats.lock().unwrap().clone();
    format!("sent {} acked {} refused {:?}; store holds {} records; lost {} late {}", s.sent, r.acked, s.refused, r.store_records, r.lost.len(), r.late.len())
}

/// Returns (report, takeover seconds after the kill, per db).
async fn writer_kill_case(w1_mutation: Option<&str>) -> (Report, String, Vec<f64>) {
    let mut b = Bench::new(2, 4, None);
    b.minio.ready().await;
    b.add_writer(2, None, vec![]);
    let c = b.client();
    assert!(b.wait_owner(&c, 0, Duration::from_secs(20)).await, "writer 0 did not take the databases");
    b.add_writer(2, w1_mutation, vec![]);
    let c = b.client();
    assert!(b.wait_owner(&c, 0, Duration::from_secs(5)).await);
    b.proxy.set(0, 2, "blackhole"); // node 3 lags behind writer 0
    let load = c.run(b.dbs, 16, 64);
    tokio::time::sleep(Duration::from_secs(3)).await;
    let t_kill = Instant::now();
    b.writers[0].kill9();
    b.proxy.set(1, 0, "blackhole"); // writer 1 reaches only the lagging node for a while
    b.proxy.set(1, 1, "blackhole");
    tokio::time::sleep(Duration::from_secs(5)).await;
    b.proxy.set(1, 0, "pass");
    b.proxy.set(1, 1, "pass");
    b.proxy.set(0, 2, "pass");
    let moved = b.wait_owner(&c, 1, Duration::from_secs(20)).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    c.stop(load).await;
    let settled = c.settle(b.dbs, Duration::from_secs(20)).await;
    let r = b.check(&c).await;
    let mut takeover = vec![];
    for d in 1..=b.dbs {
        if let Some((t, ..)) = b.writers[1].opens().into_iter().filter(|(t, db, ..)| *db == d && *t > t_kill).last() {
            takeover.push((t - t_kill).as_secs_f64());
        }
    }
    let s = format!("{}; moved to writer 1: {moved}; settled: {settled}", summary(&r, &c));
    (r, s, takeover)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_kill_takeover() {
    let (r, s, t) = writer_kill_case(None).await;
    report("writer_kill_takeover", &format!("correct: {s}; open after the kill (s): {t:.1?}"));
    assert!(r.acked > 1000, "too few writes acknowledged to mean anything");
    assert_eq!((r.lost.len(), r.late.len()), (0, 0), "first lost: {:?}", r.lost.first());
    let (r2, s2, _) = writer_kill_case(Some("recovery_single")).await;
    report("writer_kill_takeover", &format!("twin recovery_single: {s2}"));
    assert!(!r2.lost.is_empty(), "the twin lost nothing: the check does not see the bug");
}

/// Two writers at the same time, on purpose: writer 1 runs with the lease bug `lease_holder_only`, so it takes the
/// databases while writer 0 is alive, renewing and writing (the lease is not needed for safety: the fences are). With
/// the protocol's nodes: 0 losses, 0 acknowledgements from the old epoch at or above the new barrier.
/// Twin: the same with nodes running `no_fencing` → late acknowledgements / losses.
async fn two_writers_case(node_mutation: Option<&str>) -> (Report, String) {
    let mut b = Bench::new(2, 4, node_mutation);
    b.minio.ready().await;
    b.add_writer(2, None, vec![]);
    let c = b.client();
    assert!(b.wait_owner(&c, 0, Duration::from_secs(20)).await);
    let load = c.run(b.dbs, 16, 64);
    tokio::time::sleep(Duration::from_secs(2)).await;
    b.add_writer(2, Some("lease_holder_only"), vec![]);
    let c2 = b.client();
    let moved = b.wait_owner(&c2, 1, Duration::from_secs(15)).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    c.stop(load).await;
    let settled = c2.settle(b.dbs, Duration::from_secs(20)).await;
    let acks = c.acks.lock().unwrap().clone();
    let r = check(&b.minio.meta(), &acks, &b.opens(), b.dbs).await.unwrap();
    let ov = lease_overlaps(&b.writers.iter().collect::<Vec<_>>());
    let lost: Vec<String> = b.writers[0].events.lines().into_iter().filter(|(_, l)| l.starts_with("LOST")).map(|(_, l)| l).collect();
    (r.clone(), format!("{}; moved: {moved}; settled: {settled}; lease overlaps (forced) {}; writer 0: {lost:?}", summary(&r, &c), ov.len()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_writers_fenced() {
    let (r, s) = two_writers_case(None).await;
    report("two_writers_fenced", &format!("correct nodes: {s}"));
    assert!(r.acked > 1000);
    assert_eq!((r.lost.len(), r.late.len()), (0, 0), "first lost {:?}, first late {:?}", r.lost.first(), r.late.first());
    let (r2, s2) = two_writers_case(Some("no_fencing")).await;
    report("two_writers_fenced", &format!("twin no_fencing (nodes): {s2}"));
    assert!(!r2.late.is_empty() || !r2.lost.is_empty(), "the twin shows nothing: the check does not see the bug");
}

// ------------------------------------------------------------------------------------- the takeover rule (§5.1)

/// Lease tests: 2 writers, 1 database, no load needed; fast polls so the observer reads the lease right after a change.
fn lease_cfg() -> serde_json::Value {
    let mut c = fast_config();
    c["preferred_poll_ms"] = 50.into();
    c["other_poll_ms"] = 50.into();
    c
}

/// Episodes: `rates` = (writer 0 ppm, writer 1 ppm) for the owner/observer; each episode freezes the current owner
/// of database 1 for `freeze_ms` (`None` = no freeze: the owner keeps renewing) and, if `jump_ms` is set, makes the
/// observer's wall clock jump by that much 200 ms after the freeze. Returns (lease overlaps, takeovers, summary).
async fn lease_case(observer_bug: Option<&str>, episodes: usize, freeze_ms: Option<u64>, jump_ms: i64, drift_ppm: i64) -> (Vec<String>, usize, String) {
    let mut b = Bench::new(2, 1, None);
    b.minio.ready().await;
    b.add_writer_cfg(2, None, vec![], lease_cfg());
    let c = b.client();
    assert!(b.wait_owner(&c, 0, Duration::from_secs(20)).await);
    b.add_writer_cfg(2, observer_bug, vec![], lease_cfg());
    let c = b.client();
    let mut takeovers = 0;
    for _ in 0..episodes {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let Some(owner) = c.owners().get(&1).copied() else { continue };
        let other = 1 - owner;
        // the owner's monotonic clock slow, the observer's fast (the worst case for the margin)
        b.writers[owner].set_clock(0, -drift_ppm);
        b.writers[other].set_clock(0, drift_ppm);
        match freeze_ms {
            Some(f) => {
                b.writers[owner].stop();
                if jump_ms != 0 {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    b.writers[other].set_clock(jump_ms, drift_ppm);
                }
                tokio::time::sleep(Duration::from_millis(f)).await;
                b.writers[owner].cont();
            }
            None => tokio::time::sleep(Duration::from_secs(6)).await,
        }
        b.writers[other].set_clock(0, drift_ppm);
        if b.wait_owner(&c, other, Duration::from_secs(3)).await {
            takeovers += 1;
        }
    }
    let ov = lease_overlaps(&b.writers.iter().collect::<Vec<_>>());
    let taken = b.writers.iter().map(|w| w.events.lines().iter().filter(|(_, l)| l.starts_with("TAKEN")).count()).sum::<usize>();
    let s = format!("{episodes} episodes, {takeovers} ended with the other writer owning; TAKEN lines {taken}; lease overlaps {}{}", ov.len(), ov.first().map(|x| format!(" (first: {x})")).unwrap_or_default());
    (ov, takeovers, s)
}

/// The owner keeps running and renewing: nobody takes the lease. Twin `lease_holder_only` (renewals not seen as
/// changes): the other writer takes it while the owner still holds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn renewing_holder_is_never_taken() {
    let (ov, t, s) = lease_case(None, 2, None, 0, 0).await;
    report("renewing_holder_is_never_taken", &format!("correct: {s}"));
    assert_eq!((ov.len(), t), (0, 0));
    let (ov2, _, s2) = lease_case(Some("lease_holder_only"), 2, None, 0, 0).await;
    report("renewing_holder_is_never_taken", &format!("twin lease_holder_only: {s2}"));
    assert!(!ov2.is_empty(), "the twin shows no overlap: the check does not see the bug");
}

/// The owner is frozen; the observer's wall clock jumps +5 s while it waits: the takeover still waits lease + margin
/// on the monotonic clock (no overlap). Twin `lease_wall_clock`: it takes over early, while the owner still holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wall_clock_jump_changes_nothing() {
    let (ov, t, s) = lease_case(None, 3, Some(3500), 5000, 0).await;
    report("wall_clock_jump_changes_nothing", &format!("correct: {s}"));
    assert_eq!(ov.len(), 0);
    assert!(t >= 2, "the frozen owner's databases were not taken over");
    let (ov2, _, s2) = lease_case(Some("lease_wall_clock"), 3, Some(3500), 5000, 0).await;
    report("wall_clock_jump_changes_nothing", &format!("twin lease_wall_clock: {s2}"));
    assert!(!ov2.is_empty(), "the twin shows no overlap: the check does not see the bug");
}

/// Clock rates ±5 % (owner slow, observer fast), owner frozen: the margin covers the drift (no overlap). Twin
/// `lease_no_margin`: overlap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn margin_covers_clock_drift() {
    let (ov, t, s) = lease_case(None, 4, Some(3500), 0, 50_000).await;
    report("margin_covers_clock_drift", &format!("correct: {s}"));
    assert_eq!(ov.len(), 0);
    assert!(t >= 3, "the frozen owner's databases were not taken over");
    let (ov2, _, s2) = lease_case(Some("lease_no_margin"), 4, Some(3500), 0, 50_000).await;
    report("margin_covers_clock_drift", &format!("twin lease_no_margin: {s2}"));
    assert!(!ov2.is_empty(), "the twin shows no overlap: the check does not see the bug");
}

/// Returns (report, acknowledgements after the second node died, summary).
async fn lost_disk_case(no_reconfig: bool) -> (Report, usize, String) {
    let mut b = Bench::new(1, 3, None);
    b.minio.ready().await;
    let env = if no_reconfig { vec![("J3Z_TESTBUG_NO_RECONFIG", "1")] } else { vec![] };
    b.add_writer(1, None, env);
    let c = b.client();
    assert!(b.wait_owner(&c, 0, Duration::from_secs(20)).await);
    let load = c.run(b.dbs, 16, 64);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let old_id = b.nodes[2].id;
    b.nodes[2].wipe();
    let new_id = b.nodes[2].id;
    tokio::time::sleep(Duration::from_secs(4)).await; // a few upload points
    let reconfigs = b.writers[0].events.lines().iter().filter(|(_, l)| l.starts_with("RECONFIG")).count();
    b.nodes[0].kill9(); // a second node gone for good
    let t_dead = Instant::now();
    tokio::time::sleep(Duration::from_secs(4)).await;
    c.stop(load).await;
    let after = c.acks.lock().unwrap().iter().filter(|a| a.at > t_dead + Duration::from_millis(500)).count();
    let settled = c.settle(b.dbs, Duration::from_secs(10)).await;
    let r = b.check(&c).await;
    let s = format!("{}; node 3 id {old_id} -> {new_id}; RECONFIG events {reconfigs}; acks after node 1 died {after}; settled {settled}", summary(&r, &c));
    (r, after, s)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_disk_reconfig() {
    let (r, after, s) = lost_disk_case(false).await;
    report("lost_disk_reconfig", &format!("correct: {s}"));
    assert_eq!(r.lost.len(), 0, "first lost {:?}", r.lost.first());
    assert!(after > 100, "writes stopped after the second node died");
    let (_, after2, s2) = lost_disk_case(true).await;
    report("lost_disk_reconfig", &format!("twin no reconfiguration: {s2}"));
    assert_eq!(after2, 0, "twin: without reconfiguration nothing can be acknowledged with one old member left");
}
