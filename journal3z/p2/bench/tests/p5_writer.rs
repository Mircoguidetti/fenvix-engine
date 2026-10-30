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
                    let ops = LeaseOps { meta: &meta, holder: &holder, ttl_ms: 10_000, skew_ms: 1_500 };
                    ops.acquire(db, 0, &[1, 2, 3]).await.is_ok()
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
        let i = self.writers.len();
        let w = WriterProc::start(WriterSpec {
            index: i,
            nwriters,
            nodes: self.proxy.node_addrs(i, 3),
            dbs: self.dbs,
            minio: &self.minio,
            config: fast_config(),
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

async fn clock_jump_case(node_mutation: Option<&str>) -> (Report, String) {
    let mut b = Bench::new(2, 4, node_mutation);
    b.minio.ready().await;
    b.add_writer(2, None, vec![]);
    let c = b.client();
    assert!(b.wait_owner(&c, 0, Duration::from_secs(20)).await);
    b.add_writer(2, None, vec![]);
    let c = b.client();
    assert!(b.wait_owner(&c, 0, Duration::from_secs(5)).await);
    let load = c.run(b.dbs, 16, 64);
    tokio::time::sleep(Duration::from_secs(2)).await;
    b.writers[1].set_clock_offset(60_000); // writer 1 thinks writer 0's lease expired long ago
    let moved = b.wait_owner(&c, 1, Duration::from_secs(15)).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    c.stop(load).await;
    let settled = c.settle(b.dbs, Duration::from_secs(20)).await;
    let r = b.check(&c).await;
    let lost_lines: Vec<String> = b.writers[0].events.lines().into_iter().filter(|(_, l)| l.starts_with("LOST")).map(|(_, l)| l).collect();
    (r.clone(), format!("{}; moved: {moved}; settled: {settled}; writer 0: {lost_lines:?}", summary(&r, &c)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clock_jump_old_writer_fenced() {
    let (r, s) = clock_jump_case(None).await;
    report("clock_jump_old_writer_fenced", &format!("correct: {s}"));
    assert!(r.acked > 1000);
    assert_eq!((r.lost.len(), r.late.len()), (0, 0), "first lost {:?}, first late {:?}", r.lost.first(), r.late.first());
    let (r2, s2) = clock_jump_case(Some("no_fencing")).await;
    report("clock_jump_old_writer_fenced", &format!("twin no_fencing (nodes): {s2}"));
    assert!(!r2.late.is_empty() || !r2.lost.is_empty(), "the twin shows nothing: the check does not see the bug");
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
