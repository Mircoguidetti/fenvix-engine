//! fix-close-pass-gate (2026-11-12, rule 8): a close-time compaction pass must not move L0 SSTs that our compactor has
//! not absorbed. If it does, after a reopen the next absorption moves X_d past their rows, which then sit in neither our
//! runs nor SlateDB's part of the merged read (rows at or below X_d are taken from our runs only).
//!
//! fix-write-stalls found this on perf-latency c9596de: the scenario below, with the pass ungated, lost 800 of 800 gap
//! rows (`ws_close_time_pass_keeps_unabsorbed_l0` on fenvix-engine `fix-write-stalls`). integration-4 guards the pass
//! since c2-fix-clone-uploads (`lifecycle::compact_tenant_pass_with`): with compactors on (`SERVER_ON`) it does not run
//! while any L0 row is above X_d; with compactors off it runs ungated but marks the database (`note_ungated`), and a
//! compactor that starts on it later lets the builder catch up first. This test runs the scenario in both modes on
//! integration-4 and requires every gap row back.

use super::compactor::{Compactor, Knobs};
use super::tests::*;
use super::*;
use crate::lifecycle::Tenant;
use crate::runfmt::{CustomerKind, RunOptions};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::{Duration, Instant};

const HOLD_MS: u64 = 3000;

fn knobs() -> Knobs {
    Knobs {
        theta: 0.25,
        r3: false,
        r3_gets_per_gib: 470.0,
        r2_age: None,
        tier_t: 4,
        tier_unit: 2 << 10,
        absorb_min_l0: 1,
        absorb_max_l0: 8,
        local_max_bytes: 0,
        split_bytes: 40 << 10,
        merge_bytes: 18 << 10,
        min_seal_bytes: 8 << 10,
        sealed: RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2),
        delta: RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2),
        drop: false,
        fold_one_run: false,
        fold_max_runs: 6,
        fold_at_l0: 6,
        lease: Duration::from_millis(400),
        remote_lease: Duration::from_millis(HOLD_MS + 600),
        serving_lease: Duration::from_millis(300),
        put_timeout: Duration::from_millis(250),
        gc_grace: Duration::from_millis(HOLD_MS + 800),
        version_retention: Duration::from_millis(0),
        gc_every: Duration::from_secs(3600),
        tick: Duration::from_millis(10),
        reseal_parallel: 1,
        read_ahead: 64 << 10,
        derive: None,
        recognise_in_background: false,
    }
}

fn setup() {
    enable();
    hold::HOLD_LIFE_MS.store(HOLD_MS, Relaxed);
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(3 << 10, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(64, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(1, Relaxed);
    view::clear_cache();
}

fn teardown() {
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(30, Relaxed);
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
    compactor::SERVER_ON.store(false, Relaxed);
}

/// The compactor's start as the server does it (`mongo-server/src/dbs.rs`, `maybe_start_fxc`): when an ungated pass
/// marked the database (or it was never caught up), a builder publish first, whose scan reads the sorted runs too.
/// `catch_up` false is the control: the compactor starts without it.
async fn start(t: &mut Tenant, store: &Arc<dyn ObjectStore>, k: &Knobs, catch_up: bool) -> Arc<Compactor> {
    if catch_up && manifest::needs_catch_up(store, t.id()).await.unwrap() {
        let scan_ms = manifest::now_ms();
        let mut o = t.fx_auto_opts();
        o.reseal = build::Reseal::None;
        // None: nothing above X_d to publish (here: right after the first publish), so nothing to catch up
        let _ = t.fx_publish(&o, None).await.unwrap();
        manifest::note_caught_up(store, t.id(), scan_ms).await.unwrap();
    }
    let c = Compactor::start(t, Arc::clone(store), k.clone(), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    c.run();
    c
}

async fn flush(t: &Tenant) {
    let db = t.fx_writer_db().unwrap();
    db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
}

/// Returns (L0 at the close, L0 after the pass, gap rows read wrong after the reopen and an absorption, gap rows, every
/// row read wrong, the pass's time).
async fn pass_gap_scenario(server_on: bool, catch_up: bool) -> (usize, usize, usize, usize, usize, Duration) {
    compactor::SERVER_ON.store(server_on, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = format!("cpg-{}-{}", if server_on { "on" } else { "off" }, if catch_up { "cu" } else { "nocu" });
    let id = id.as_str();
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..100 {
        apply(&mut t, &mut m, vec![(ckey(0, i % 40), Some(vec![b'a'; 40]))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let mut k = knobs();
    k.absorb_min_l0 = 1000; // nothing is absorbed before the close
    let c = start(&mut t, &store, &k, catch_up).await;
    c.paused.store(true, Relaxed); // not even near the stall
    // the gap: rows written only now (new keys), flushed into L0 SSTs that are never absorbed before the close
    for r in 0..10 {
        for i in 0..80 {
            apply(&mut t, &mut m, vec![(ckey(20 + r, i), Some(format!("gap-{r}-{i}").into_bytes()))]).await;
        }
        flush(&t).await;
    }
    let l0_at_close = t.writer_l0().unwrap();
    c.stop().await;
    t.close().await.unwrap();
    // the close-time pass (L0 at or above the threshold), as the maintenance queue runs it
    let t_pass = Instant::now();
    let pass = crate::lifecycle::compact_tenant(&store, &store, id).await.unwrap();
    let pass_took = t_pass.elapsed();
    let l0_after_pass = crate::lifecycle::l0_count(&store, id).await.unwrap();
    // reopen: new rows, absorbed, then every key read
    let mut t = lc(&store).wake(id).await.unwrap();
    t.upgrade().await.unwrap();
    k.absorb_min_l0 = 1;
    let c = start(&mut t, &store, &k, catch_up).await;
    for i in 0..200 {
        apply(&mut t, &mut m, vec![(ckey(30, i % 70), Some(format!("late-{i}").into_bytes()))]).await;
    }
    flush(&t).await;
    c.drain(false).await.unwrap();
    t.fx_sync();
    let gap_keys: Vec<Vec<u8>> = m.keys().filter(|key| (20..30).any(|r| key.starts_with(&ckey(r, 0)[..ckey(r, 0).len() - 5]))).cloned().collect();
    let mut gap_wrong = 0;
    for key in &gap_keys {
        if t.get(key).await.unwrap().as_deref() != m.get(key).map(|v| &v[..]) {
            gap_wrong += 1;
        }
    }
    let mut wrong = 0;
    for (key, want) in &m {
        if t.get(key).await.unwrap().as_deref() != Some(&want[..]) {
            wrong += 1;
        }
    }
    eprintln!(
        "close pass gate (compactors {}, catch-up {catch_up}): L0 at the close {l0_at_close}; pass {:?} in {pass_took:?}, L0 after it {l0_after_pass}; x_d now {:?}; gap rows {} wrong {gap_wrong}; all rows {} wrong {wrong}",
        if server_on { "on" } else { "off" },
        pass.as_ref().map(|p| (p.l0_before, p.l0_after)),
        t.fx_view().map(|v| v.x_d),
        gap_keys.len(),
        m.len()
    );
    c.stop().await;
    t.close().await.unwrap();
    compactor::SERVER_ON.store(false, Relaxed);
    (l0_at_close, l0_after_pass, gap_wrong, gap_keys.len(), wrong, pass_took)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpg_close_time_pass_never_loses_unabsorbed_l0() {
    let _s = SERIAL.lock().await;
    setup();
    let on = pass_gap_scenario(true, true).await;
    let off = pass_gap_scenario(false, true).await;
    // control: compactors off for the pass and no catch-up before the next compactor: the scenario must lose gap rows,
    // or this test does not reach the case
    let control = pass_gap_scenario(false, false).await;
    teardown();
    assert!(control.2 > 0, "control: without the gate and the catch-up the gap rows must be lost ({} of {})", control.2, control.3);
    for (mode, (l0, _l0_after, gap_wrong, gap, wrong, took)) in [("on", on), ("off", off)] {
        assert!(l0 >= crate::lifecycle::L0_COMPACT_THRESHOLD, "setup ({mode}): the close-time pass must have L0 to compact ({l0})");
        assert_eq!(gap, 800, "setup ({mode}): 800 gap rows");
        assert_eq!(gap_wrong, 0, "compactors {mode}: {gap_wrong} of {gap} gap rows lost (rule 8)");
        assert_eq!(wrong, 0, "compactors {mode}: wrong reads");
        assert!(took < Duration::from_secs(30), "compactors {mode}: the pass took {took:?}");
    }
    // compactors on: the pass must not have moved the unabsorbed L0 SSTs
    assert_eq!(on.1, on.0, "compactors on: the pass moved unabsorbed L0 SSTs ({} -> {})", on.0, on.1);
}

/// runwriter-and-stall (CI d660ea9, test_51r sealed runs 6, 7 and 9: no insert taken within 30 s after the store came
/// back). A database with a compactor is reopened (after a lease loss) with L0 at SlateDB's limit, every L0 SST absorbed,
/// and several memtables of WAL to replay. SlateDB's replay applies backpressure for each memtable: it waited for a flush
/// that waits for L0 to shrink, which only the compactor's fold does, and the compactor starts after the writer's open,
/// so the open never returned. Now the wake runs the gated compaction pass first (every L0 row is at or below X_d, so it
/// may move them, rule 8): the open returns, and every row reads back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wake_with_full_absorbed_l0_and_wal_opens_the_writer() {
    let _s = SERIAL.lock().await;
    setup();
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(7, Relaxed); // flushes stop with 6 SSTs in L0 (6 + 1 reaches 7)
    let tiers0 = crate::tiered::TIERS.load(Relaxed);
    crate::tiered::TIERS.store(4, Relaxed); // the server's default scheduler (the pass needs one)
    compactor::SERVER_ON.store(true, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (queue, rx) = crate::lifecycle::CompactionQueue::new();
    let lc = crate::lifecycle::Lifecycle::new(Arc::new(crate::counting::CountingStore::new(Arc::clone(&store))), Some(Arc::clone(&queue)));
    let maint = crate::lifecycle::Maintenance::start(Arc::clone(&store), Arc::clone(&store), queue, rx, Arc::clone(&lc.touched), Arc::clone(&lc.maint_locks), 2, Duration::from_secs(3600), Duration::from_secs(3600));
    let id = "wake-full-l0";
    let mut t = lc.create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..100 {
        apply(&mut t, &mut m, vec![(ckey(0, i % 40), Some(vec![b'a'; 40]))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let k = knobs();
    let c = start(&mut t, &store, &k, true).await;
    // L0 up to 6 SSTs, all absorbed (the fold is off: they stay in L0)
    let mut r = 0;
    while t.writer_l0().unwrap() < 6 {
        for i in 0..80 {
            apply(&mut t, &mut m, vec![(ckey(10 + r % 10, i), Some(format!("l0-{r}-{i}").into_bytes()))]).await;
        }
        flush(&t).await;
        r += 1;
        assert!(r < 60, "setup: L0 did not reach 6 SSTs");
    }
    c.drain(false).await.unwrap();
    let l0 = t.writer_l0().unwrap();
    c.stop().await;
    // rows only in the WAL: about 4 memtables (3 KiB each), which cannot flush while L0 is full, over the next writer's
    // limit on unflushed bytes (8 KiB) and well within what an emptied L0 holds (as in the server, where a memtable is
    // MiBs and the WAL of a stalled writer fits in L0 once it is compacted)
    for i in 0..140 {
        apply(&mut t, &mut m, vec![(ckey(30 + i % 5, i), Some(format!("wal-{i}-{}", "w".repeat(60)).into_bytes()))]).await;
    }
    t.abandon().await; // a lease loss: no clean close, the WAL is replayed by the next writer
    // the next writer's limit on unflushed bytes is under what its replay holds (in the server the live writer stalls at
    // that limit with L0 full, so its WAL holds about that much: here the limit is lowered for the replay only)
    crate::lifecycle::MAX_UNFLUSHED_OVERRIDE.store(8 << 10, Relaxed);
    let passes0 = crate::lifecycle::WAKE_GATED_PASSES.load(Relaxed);
    let mut t = lc.wake(id).await.unwrap();
    let t0 = Instant::now();
    let opened = tokio::time::timeout(Duration::from_secs(30), t.upgrade()).await;
    let open_took = t0.elapsed();
    let gated = crate::lifecycle::WAKE_GATED_PASSES.load(Relaxed) - passes0;
    let mut wrong = 0;
    if matches!(opened, Ok(Ok(()))) {
        for (key, want) in &m {
            if t.get(key).await.unwrap().as_deref() != Some(&want[..]) {
                wrong += 1;
            }
        }
    }
    let l0_after = crate::lifecycle::l0_count(&store, id).await.unwrap();
    t.abandon().await;
    eprintln!("wake full L0: L0 at the abandon {l0}, in the stored manifest after the wake {l0_after}; the writer's open {} after {open_took:?}; gated passes {gated}; rows {} wrong {wrong}", match &opened { Ok(Ok(())) => "returned".to_string(), Ok(Err(e)) => format!("failed: {e:#}"), Err(_) => "did not return".to_string() }, m.len());
    let _ = maint.shutdown().await;
    crate::lifecycle::MAX_UNFLUSHED_OVERRIDE.store(0, Relaxed);
    compactor::SERVER_ON.store(false, Relaxed);
    crate::tiered::TIERS.store(tiers0, Relaxed);
    teardown();
    assert!(l0 >= crate::lifecycle::L0_GUARD, "setup: L0 at the abandon {l0}, under the guard");
    assert!(matches!(opened, Ok(Ok(()))), "the writer's open did not return within 30 s with L0 full ({open_took:?})");
    assert_eq!(wrong, 0, "rows read wrong after the reopen (rule 8)");
}

// ------------------------------------------------------------------------------------------------------------------
// runwriter-and-stall, the open case (coordinator's decision): L0 full of SSTs NOT yet absorbed, and a WAL over the next
// writer's limit on unflushed bytes. The gated pass may not run (rule 8), so the open's limit is raised by the WAL's size.

/// Holds, while armed, the GETs of WAL objects (a writer's open in the middle of its replay).
#[derive(Debug)]
struct HoldWal {
    inner: Arc<dyn ObjectStore>,
    armed: Arc<std::sync::atomic::AtomicBool>,
    held: Arc<std::sync::atomic::AtomicU64>,
}

impl std::fmt::Display for HoldWal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HoldWal")
    }
}

#[async_trait::async_trait]
impl ObjectStore for HoldWal {
    async fn put_opts(&self, l: &object_store::path::Path, p: object_store::PutPayload, o: object_store::PutOptions) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(&self, l: &object_store::path::Path, o: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, l: &object_store::path::Path, o: object_store::GetOptions) -> object_store::Result<object_store::GetResult> {
        if self.armed.load(Relaxed) && l.as_ref().contains("/wal/") {
            self.held.fetch_add(1, Relaxed);
            while self.armed.load(Relaxed) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        self.inner.get_opts(l, o).await
    }
    fn delete_stream(&self, s: futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(s)
    }
    fn list(&self, p: Option<&object_store::path::Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(p)
    }
    fn list_with_offset(&self, p: Option<&object_store::path::Path>, o: &object_store::path::Path) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list_with_offset(p, o)
    }
    async fn list_with_delimiter(&self, p: Option<&object_store::path::Path>) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, f: &object_store::path::Path, t: &object_store::path::Path, o: object_store::CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(f, t, o).await
    }
    async fn rename_opts(&self, f: &object_store::path::Path, t: &object_store::path::Path, o: object_store::RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(f, t, o).await
    }
}

struct Server {
    lc: Arc<crate::lifecycle::Lifecycle>,
    maint: crate::lifecycle::Maintenance,
}

fn server(store: &Arc<dyn ObjectStore>) -> Server {
    let (queue, rx) = crate::lifecycle::CompactionQueue::new();
    let lc = crate::lifecycle::Lifecycle::new(Arc::new(crate::counting::CountingStore::new(Arc::clone(store))), Some(Arc::clone(&queue)));
    let maint = crate::lifecycle::Maintenance::start(Arc::clone(store), Arc::clone(store), queue, rx, Arc::clone(&lc.touched), Arc::clone(&lc.maint_locks), 2, Duration::from_secs(3600), Duration::from_secs(3600));
    Server { lc, maint }
}

/// The state: a version, L0 at 6 SSTs none of which is absorbed (rows above X_d: the gated pass may not run), and
/// `wal_rows` rows of `row_bytes` only in the WAL; the writer then lost (abandoned, no close). Returns the model.
async fn full_unabsorbed_l0(lc: &Arc<crate::lifecycle::Lifecycle>, store: &Arc<dyn ObjectStore>, id: &str, wal_rows: usize, row_bytes: usize) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut t = lc.create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..100 {
        apply(&mut t, &mut m, vec![(ckey(0, i % 40), Some(vec![b'a'; 40]))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let mut k = knobs();
    k.absorb_min_l0 = 1000; // nothing absorbed: every L0 row stays above X_d
    let c = start(&mut t, store, &k, true).await;
    c.paused.store(true, Relaxed);
    let mut r = 0;
    while t.writer_l0().unwrap() < 6 {
        for i in 0..80 {
            apply(&mut t, &mut m, vec![(ckey(10 + r % 10, i), Some(format!("l0-{r}-{i}").into_bytes()))]).await;
        }
        flush(&t).await;
        r += 1;
        assert!(r < 60, "setup: L0 did not reach 6 SSTs");
    }
    c.stop().await;
    let mut i = 0;
    while i < wal_rows {
        let ops: Vec<_> = (i..(i + 50).min(wal_rows)).map(|j| (ckey(30 + j % 5, j), Some(format!("wal-{j}-{}", "w".repeat(row_bytes)).into_bytes()))).collect();
        i += ops.len();
        apply(&mut t, &mut m, ops).await;
    }
    t.abandon().await;
    m
}

async fn wrong_rows(t: &Tenant, m: &BTreeMap<Vec<u8>, Vec<u8>>) -> usize {
    let mut wrong = 0;
    for (key, want) in m {
        if t.get(key).await.unwrap().as_deref() != Some(&want[..]) {
            wrong += 1;
        }
    }
    wrong
}

fn rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status").ok().and_then(|s| s.lines().find(|l| l.starts_with("VmRSS:")).and_then(|l| l.split_whitespace().nth(1)?.parse().ok())).unwrap_or(0)
}

fn open_case_setup(sst_bytes: u64) -> u32 {
    setup();
    crate::lifecycle::MAX_UNFLUSHED_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(7, Relaxed);
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(sst_bytes, Relaxed);
    let tiers0 = crate::tiered::TIERS.load(Relaxed);
    crate::tiered::TIERS.store(4, Relaxed);
    compactor::SERVER_ON.store(true, Relaxed);
    tiers0
}

fn open_case_teardown(tiers0: u32) {
    crate::lifecycle::MAX_UNFLUSHED_OVERRIDE.store(0, Relaxed);
    compactor::SERVER_ON.store(false, Relaxed);
    crate::tiered::TIERS.store(tiers0, Relaxed);
    teardown();
}

/// The open returns (before: it waited for good), every row reads right, and after our compactor absorbs the L0 the
/// rows still read right (rule 8).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn open_with_full_unabsorbed_l0_and_wal_over_the_limit_returns() {
    let _s = SERIAL.lock().await;
    let tiers0 = open_case_setup(3 << 10);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let srv = server(&store);
    let id = "open-unabsorbed";
    let m = full_unabsorbed_l0(&srv.lc, &store, id, 140, 60).await;
    let gate = crate::lifecycle::pass_can_run_gated(&store, id).await.unwrap();
    crate::lifecycle::MAX_UNFLUSHED_OVERRIDE.store(8 << 10, Relaxed);
    let raises0 = crate::lifecycle::REPLAY_RAISES.load(Relaxed);
    let mut t = srv.lc.wake(id).await.unwrap();
    let t0 = Instant::now();
    let opened = tokio::time::timeout(Duration::from_secs(30), t.upgrade()).await;
    let took = t0.elapsed();
    let raised = crate::lifecycle::REPLAY_RAISES.load(Relaxed) - raises0;
    let (mut wrong, mut wrong_after_absorb) = (usize::MAX, usize::MAX);
    if matches!(opened, Ok(Ok(()))) {
        wrong = wrong_rows(&t, &m).await;
        let mut k = knobs();
        k.absorb_min_l0 = 1;
        let c = start(&mut t, &store, &k, true).await;
        c.drain(false).await.unwrap();
        t.fx_sync();
        wrong_after_absorb = wrong_rows(&t, &m).await;
        c.stop().await;
    }
    eprintln!("open case: gated pass possible {gate}; open {} after {took:?}; raised {raised}; rows {} wrong {wrong}, after absorption {wrong_after_absorb}", if matches!(opened, Ok(Ok(()))) { "returned" } else { "did not return" }, m.len());
    t.abandon().await;
    let _ = srv.maint.shutdown().await;
    open_case_teardown(tiers0);
    assert!(!gate, "setup: L0 must hold rows above X_d (the gated pass may not run)");
    assert!(matches!(opened, Ok(Ok(()))), "the writer's open did not return within 30 s");
    assert_eq!(wrong, 0, "rows read wrong after the open");
    assert_eq!(wrong_after_absorb, 0, "rows read wrong after the absorption (rule 8)");
}

/// A process killed during that open (its task cut in the middle of the replay, nothing closed): a new process opens
/// the database and every acknowledged row reads right.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn open_case_killed_mid_replay_then_reopened() {
    let _s = SERIAL.lock().await;
    let tiers0 = open_case_setup(3 << 10);
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (armed, held) = (Arc::new(std::sync::atomic::AtomicBool::new(false)), Arc::new(std::sync::atomic::AtomicU64::new(0)));
    let store: Arc<dyn ObjectStore> = Arc::new(HoldWal { inner: Arc::clone(&inner), armed: Arc::clone(&armed), held: Arc::clone(&held) });
    let srv = server(&store);
    let id = "open-killed";
    let m = full_unabsorbed_l0(&srv.lc, &store, id, 140, 60).await;
    crate::lifecycle::MAX_UNFLUSHED_OVERRIDE.store(8 << 10, Relaxed);
    // process A: its open is held in the replay, then the process dies (the task is cut; nothing is closed)
    let mut ta = srv.lc.wake(id).await.unwrap();
    armed.store(true, Relaxed);
    let job = tokio::spawn(async move {
        let _ = ta.upgrade().await;
        ta
    });
    let t0 = Instant::now();
    while held.load(Relaxed) == 0 {
        assert!(t0.elapsed() < Duration::from_secs(20), "setup: the open never read the WAL");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    job.abort();
    let _ = job.await;
    let Server { lc, maint } = srv;
    let _ = maint.shutdown().await;
    drop(lc);
    // A's hold is never released: its open runs in a task of its own (the flush runtime), which cutting the caller does
    // not stop; a process killed with -9 would stop it, so here it stays held for good (B uses the store directly)
    // process B, on the same store
    let srv = server(&inner);
    let mut t = srv.lc.wake(id).await.unwrap();
    let opened = tokio::time::timeout(Duration::from_secs(30), t.upgrade()).await;
    let wrong = if matches!(opened, Ok(Ok(()))) { wrong_rows(&t, &m).await } else { usize::MAX };
    eprintln!("open case killed: WAL reads held {} (A stays held, as if dead); the next open {}; rows {} wrong {wrong}", held.load(Relaxed), if matches!(opened, Ok(Ok(()))) { "returned" } else { "did not return" }, m.len());
    t.abandon().await;
    let _ = srv.maint.shutdown().await;
    open_case_teardown(tiers0);
    assert!(matches!(opened, Ok(Ok(()))), "the open after the killed one did not return within 30 s");
    assert_eq!(wrong, 0, "acknowledged rows read wrong after the kill and the reopen (rule 8)");
}

/// Memory of that open (MEASURED, printed): 64 KiB SSTs, a 1 MiB limit on unflushed bytes, about 8 MiB of WAL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn open_case_peak_memory() {
    let _s = SERIAL.lock().await;
    let tiers0 = open_case_setup(64 << 10);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let srv = server(&store);
    let id = "open-memory";
    let m = full_unabsorbed_l0(&srv.lc, &store, id, 8000, 1000).await;
    let wal = crate::lifecycle::wal_bytes(&store, id).await.unwrap();
    crate::lifecycle::MAX_UNFLUSHED_OVERRIDE.store(1 << 20, Relaxed);
    let mut t = srv.lc.wake(id).await.unwrap();
    let base = rss_kib();
    let peak = Arc::new(std::sync::atomic::AtomicU64::new(base));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler = {
        let (peak, stop) = (Arc::clone(&peak), Arc::clone(&stop));
        tokio::spawn(async move {
            while !stop.load(Relaxed) {
                peak.fetch_max(rss_kib(), Relaxed);
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
    };
    let t0 = Instant::now();
    let opened = tokio::time::timeout(Duration::from_secs(60), t.upgrade()).await;
    let took = t0.elapsed();
    stop.store(true, Relaxed);
    let _ = sampler.await;
    let wrong = if matches!(opened, Ok(Ok(()))) { wrong_rows(&t, &m).await } else { usize::MAX };
    let rise = peak.load(Relaxed).saturating_sub(base);
    eprintln!("open case memory: WAL {} KiB; the open {} in {took:?}; RSS {base} KiB before, peak +{rise} KiB during the open; rows {} wrong {wrong}", wal / 1024, if matches!(opened, Ok(Ok(()))) { "returned" } else { "did not return" }, m.len());
    t.abandon().await;
    let _ = srv.maint.shutdown().await;
    open_case_teardown(tiers0);
    assert!(matches!(opened, Ok(Ok(()))), "the open did not return");
    assert_eq!(wrong, 0);
}

/// R-final-engine (review of 24bbdbc, the WAL replay raise): the raise is the size of EVERY WAL object under the
/// prefix (`lifecycle::wal_bytes`), not of the WAL the open replays (objects at or above SlateDB's
/// `replay_after_wal_id`). A database with a restore window keeps its WAL objects for the whole window on purpose
/// (`WAL_RETAIN`, PITR), so at 1 TB with tens of thousands of writes per second the raise is days of WAL, and it stays
/// for the writer's life. Here the history is simply not collected yet (GC every hour), as the window keeps it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf_replay_raise_counts_only_the_wal_the_open_replays() {
    let _s = SERIAL.lock().await;
    let tiers0 = open_case_setup(3 << 10);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let srv = server(&store);
    let id = "rf-raise";
    // history: 2,000 rows of about 1 KiB, flushed into L0 and published (below the replay boundary, never replayed)
    {
        let mut t = srv.lc.create(id).await.unwrap();
        let mut m = BTreeMap::new();
        for b in 0..40 {
            let ops: Vec<_> = (0..50).map(|j| (ckey(50 + b % 5, b * 50 + j), Some(vec![b'h'; 1000]))).collect();
            apply(&mut t, &mut m, ops).await;
        }
        flush(&t).await;
        t.close().await.unwrap();
    }
    let _ = crate::lifecycle::compact_tenant(&store, &store, id).await;
    // then the open case of the commit, on the same database (its writer is lost with L0 full and a WAL tail)
    let m = {
        let mut t = srv.lc.wake(id).await.unwrap();
        t.upgrade().await.unwrap();
        let mut m = BTreeMap::new();
        for i in 0..100 {
            apply(&mut t, &mut m, vec![(ckey(0, i % 40), Some(vec![b'a'; 40]))]).await;
        }
        publish(&mut t, &small_opts(build::Reseal::All)).await;
        let mut k = knobs();
        k.absorb_min_l0 = 1000;
        let c = start(&mut t, &store, &k, true).await;
        c.paused.store(true, Relaxed);
        let mut r = 0;
        while t.writer_l0().unwrap() < 6 {
            for i in 0..80 {
                apply(&mut t, &mut m, vec![(ckey(10 + r % 10, i), Some(format!("l0-{r}-{i}").into_bytes()))]).await;
            }
            flush(&t).await;
            r += 1;
            assert!(r < 60, "setup: L0 did not reach 6 SSTs");
        }
        c.stop().await;
        for i in (0..140).step_by(50) {
            let ops: Vec<_> = (i..(i + 50).min(140)).map(|j| (ckey(30 + j % 5, j), Some(format!("wal-{j}-{}", "w".repeat(60)).into_bytes()))).collect();
            apply(&mut t, &mut m, ops).await;
        }
        t.abandon().await;
        m
    };
    // what the open will replay: the WAL objects at or above the replay boundary
    let admin = slatedb::admin::Admin::builder(crate::lifecycle::tenant_path(id), Arc::clone(&store)).build();
    let boundary = admin.read_manifest(None).await.unwrap().unwrap().replay_after_wal_id();
    use futures::TryStreamExt;
    let metas: Vec<object_store::ObjectMeta> = store.list(Some(&object_store::path::Path::from(format!("{}/wal", crate::lifecycle::tenant_path(id))))).try_collect().await.unwrap();
    let id_of = |m: &object_store::ObjectMeta| m.location.filename().and_then(|f| f.strip_suffix(".sst")).and_then(|n| n.parse::<u64>().ok()).unwrap_or(0);
    let replayed: u64 = metas.iter().filter(|m| id_of(m) >= boundary).map(|m| m.size).sum();
    let all: u64 = metas.iter().map(|m| m.size).sum();
    crate::lifecycle::MAX_UNFLUSHED_OVERRIDE.store(8 << 10, Relaxed);
    crate::lifecycle::REPLAY_RAISED_BYTES_MAX.store(0, Relaxed);
    let mut t = srv.lc.wake(id).await.unwrap();
    let opened = tokio::time::timeout(Duration::from_secs(30), t.upgrade()).await;
    let raise = crate::lifecycle::REPLAY_RAISED_BYTES_MAX.load(Relaxed);
    let wrong = if matches!(opened, Ok(Ok(()))) { wrong_rows(&t, &m).await } else { usize::MAX };
    eprintln!("rf replay raise: WAL objects {} ({all} bytes), replay boundary {boundary}, bytes the open replays {replayed}; raise {raise} bytes ({:.1}x); rows wrong {wrong}", metas.len(), raise as f64 / replayed.max(1) as f64);
    t.abandon().await;
    let _ = srv.maint.shutdown().await;
    open_case_teardown(tiers0);
    assert!(matches!(opened, Ok(Ok(()))), "setup: the open must return");
    assert!(raise > 0, "setup: the open must take the raise (L0 at the guard)");
    assert!(raise <= 2 * replayed, "the raise ({raise} bytes) counts WAL the open never replays ({replayed} bytes replayed, {all} under the prefix)");
}

/// P-fix-slow-store item 7, the margin: small rows (keys of a few bytes, values of 1 byte) make SlateDB's memtable count
/// each entry with its overhead, so a raise of the WAL's bytes alone (the old raise, margin 1) leaves the replay under its
/// limit and the open waits for L0 for good. With the margin (2) it returns. Every row reads right.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fss_replay_raise_margin_covers_small_rows() {
    let _s = SERIAL.lock().await;
    let mut out = Vec::new();
    for margin in [1u64, 2] {
        let tiers0 = open_case_setup(3 << 10);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let srv = server(&store);
        let id = format!("fss-margin-{margin}");
        let m = full_unabsorbed_l0(&srv.lc, &store, &id, 3000, 0).await;
        let wal = crate::lifecycle::wal_bytes(&store, &id).await.unwrap();
        crate::lifecycle::MAX_UNFLUSHED_OVERRIDE.store(8 << 10, Relaxed);
        crate::lifecycle::REPLAY_RAISE_MARGIN.store(margin, Relaxed);
        let mut t = srv.lc.wake(&id).await.unwrap();
        let t0 = Instant::now();
        let opened = tokio::time::timeout(Duration::from_secs(15), t.upgrade()).await;
        let took = t0.elapsed();
        let ok = matches!(opened, Ok(Ok(())));
        let wrong = if ok { wrong_rows(&t, &m).await } else { usize::MAX };
        eprintln!("margin {margin}: WAL {wal} bytes, raise {}; open {} after {took:?}; rows {} wrong {wrong}", t.replay_raised(), if ok { "returned" } else { "did not return" }, m.len());
        out.push((margin, ok, wrong));
        crate::lifecycle::REPLAY_RAISE_MARGIN.store(2, Relaxed);
        if ok {
            t.abandon().await;
        }
        let _ = srv.maint.shutdown().await;
        open_case_teardown(tiers0);
    }
    assert!(!out[0].1, "control: with margin 1 the open returned (the setup does not show the per-entry overhead)");
    assert!(out[1].1, "with the margin the open did not return within 15 s");
    assert_eq!(out[1].2, 0, "rows read wrong after the open");
}

/// P-fix-slow-store item 7, the cap: a full unabsorbed L0 plus about 8 MiB of WAL, the writer's limit 1 MiB, under a
/// memory budget whose memtable share is 32 MiB: the raise (2 x the WAL) fits the share, the open returns, every row
/// reads right, and the process's RSS rise during the open is MEASURED (printed) and stays under the share. A second
/// case with a 4 MiB share: the raise is cut to the share (counted as capped), never above it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fss_replay_raise_under_the_memory_budget() {
    let _s = SERIAL.lock().await;
    let tiers0 = open_case_setup(64 << 10);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let srv = server(&store);
    let id = "fss-budget";
    let m = full_unabsorbed_l0(&srv.lc, &store, id, 8000, 1000).await;
    let wal = crate::lifecycle::wal_bytes(&store, id).await.unwrap();
    crate::lifecycle::MAX_UNFLUSHED_OVERRIDE.store(1 << 20, Relaxed);
    let share: usize = 32 << 20;
    crate::lifecycle::REPLAY_RAISE_CAP_OVERRIDE.store(share as u64, Relaxed);
    let mut t = srv.lc.wake(id).await.unwrap();
    let base = rss_kib();
    let peak = Arc::new(std::sync::atomic::AtomicU64::new(base));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler = {
        let (peak, stop) = (Arc::clone(&peak), Arc::clone(&stop));
        tokio::spawn(async move {
            while !stop.load(Relaxed) {
                peak.fetch_max(rss_kib(), Relaxed);
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
    };
    let t0 = Instant::now();
    let opened = tokio::time::timeout(Duration::from_secs(60), t.upgrade()).await;
    let took = t0.elapsed();
    stop.store(true, Relaxed);
    let _ = sampler.await;
    let ok = matches!(opened, Ok(Ok(())));
    let wrong = if ok { wrong_rows(&t, &m).await } else { usize::MAX };
    let rise = peak.load(Relaxed).saturating_sub(base);
    let raised = t.replay_raised();
    eprintln!("budget case: WAL {} KiB, raise {} KiB, memtable share {} KiB; open {} in {took:?}; RSS {base} KiB before, peak +{rise} KiB during the open; rows {} wrong {wrong}", wal / 1024, raised / 1024, share / 1024, if ok { "returned" } else { "did not return" }, m.len());
    if ok {
        t.abandon().await;
    }
    // the capped case: the same database, a 4 MiB share
    let capped0 = crate::lifecycle::REPLAY_RAISES_CAPPED.load(Relaxed);
    crate::lifecycle::REPLAY_RAISE_CAP_OVERRIDE.store(4 << 20, Relaxed);
    let mut t2 = srv.lc.wake(id).await.unwrap();
    let opened2 = tokio::time::timeout(Duration::from_secs(5), t2.upgrade()).await;
    let raised2 = t2.replay_raised();
    let capped = crate::lifecycle::REPLAY_RAISES_CAPPED.load(Relaxed) - capped0;
    eprintln!("capped case: raise {} KiB under a 4096 KiB share (capped {capped}); open {}", raised2 / 1024, if matches!(opened2, Ok(Ok(()))) { "returned" } else { "did not return within 5 s (the replay waits for L0: the cap trades the open for the memory bound)" });
    crate::lifecycle::REPLAY_RAISE_CAP_OVERRIDE.store(0, Relaxed);
    if matches!(opened2, Ok(Ok(()))) {
        t2.abandon().await;
    }
    let _ = srv.maint.shutdown().await;
    open_case_teardown(tiers0);
    assert!(ok, "the open did not return under the budget");
    assert_eq!(wrong, 0);
    assert!(raised >= 2 * wal && raised <= share, "raise {raised} for a WAL of {wal} (share {share})");
    assert!((rise as usize) * 1024 < share, "RSS rose {rise} KiB during the open, over the memtable share");
    assert!(raised2 <= 4 << 20 && capped == 1, "the capped raise {raised2} was not cut to the share");
}

/// P-fix-slow-store item 7, the drop: after a raised open, once our compactor has absorbed and L0 is below the
/// compaction threshold, the writer is droppable; closed and opened again, it has no raise, and every row reads right.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fss_replay_raise_is_dropped_once_l0_drained() {
    let _s = SERIAL.lock().await;
    let tiers0 = open_case_setup(3 << 10);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let srv = server(&store);
    let id = "fss-drop";
    let m = full_unabsorbed_l0(&srv.lc, &store, id, 140, 60).await;
    crate::lifecycle::MAX_UNFLUSHED_OVERRIDE.store(8 << 10, Relaxed);
    let mut t = srv.lc.wake(id).await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), t.upgrade()).await.expect("open").unwrap();
    let raised = t.replay_raised();
    let before = t.replay_raise_droppable();
    let mut k = knobs();
    k.absorb_min_l0 = 1;
    let c = start(&mut t, &store, &k, true).await;
    c.drain(false).await.unwrap();
    t.fx_sync();
    // SlateDB's own compaction of the absorbed L0 (the maintenance pass, as the server's L0 watch asks) drains it
    let t0 = Instant::now();
    while !t.replay_raise_droppable() && t0.elapsed() < Duration::from_secs(30) {
        let _ = crate::lifecycle::flush_writer_memtable(id).await;
        if let Some(q) = &srv.lc.queue {
            let (tx, rx) = tokio::sync::oneshot::channel();
            q.request(id, Some(tx));
            let _ = tokio::time::timeout(Duration::from_secs(5), rx).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let after = t.replay_raise_droppable();
    let l0 = t.writer_l0();
    c.stop().await;
    t.close().await.unwrap();
    let mut t2 = srv.lc.wake(id).await.unwrap();
    t2.upgrade().await.unwrap();
    let raised2 = t2.replay_raised();
    let wrong = wrong_rows(&t2, &m).await;
    eprintln!("drop case: raise {raised}; droppable before the absorption {before}, after {after} (L0 {l0:?}); reopened with raise {raised2}; rows {} wrong {wrong}", m.len());
    t2.abandon().await;
    let _ = srv.maint.shutdown().await;
    open_case_teardown(tiers0);
    assert!(raised > 0 && !before, "setup: the open must be raised and L0 full");
    assert!(after, "L0 did not drain below the threshold (L0 {l0:?})");
    assert_eq!(raised2, 0, "the reopened writer still has a raise");
    assert_eq!(wrong, 0);
}

// ------------------------------------------------------------------------------------------------------------------
// fix-pass-stop-bound: a compaction pass on a database whose open writer no longer answers (its store stalled) must end,
// and the maintenance's shutdown must return. Found as the unit-suite hang (integration-6, 2026-11-13): a box_first_trip
// test left its writer registered (`WRITER_DBS`) with its runtime gone; the next test's maintenance queued a pass for it,
// whose `refresh_manifest` on that writer never returned, and `Maintenance::shutdown` waited for the pass with no bound.

/// Every request waits while armed (a store that stopped answering).
#[derive(Debug)]
struct Stall {
    inner: Arc<dyn ObjectStore>,
    armed: Arc<std::sync::atomic::AtomicBool>,
}

impl std::fmt::Display for Stall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Stall")
    }
}

impl Stall {
    async fn wait(armed: &std::sync::atomic::AtomicBool) {
        while armed.load(Relaxed) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    fn held<T: Send + 'static>(&self, s: futures::stream::BoxStream<'static, T>) -> futures::stream::BoxStream<'static, T> {
        use futures::StreamExt;
        let armed = Arc::clone(&self.armed);
        futures::stream::once(async move {
            Stall::wait(&armed).await;
            s
        })
        .flatten()
        .boxed()
    }
}

#[async_trait::async_trait]
impl ObjectStore for Stall {
    async fn put_opts(&self, l: &object_store::path::Path, p: object_store::PutPayload, o: object_store::PutOptions) -> object_store::Result<object_store::PutResult> {
        Stall::wait(&self.armed).await;
        self.inner.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(&self, l: &object_store::path::Path, o: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        Stall::wait(&self.armed).await;
        self.inner.put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, l: &object_store::path::Path, o: object_store::GetOptions) -> object_store::Result<object_store::GetResult> {
        Stall::wait(&self.armed).await;
        self.inner.get_opts(l, o).await
    }
    fn delete_stream(&self, s: futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.held(self.inner.delete_stream(s))
    }
    fn list(&self, p: Option<&object_store::path::Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.held(self.inner.list(p))
    }
    fn list_with_offset(&self, p: Option<&object_store::path::Path>, o: &object_store::path::Path) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.held(self.inner.list_with_offset(p, o))
    }
    async fn list_with_delimiter(&self, p: Option<&object_store::path::Path>) -> object_store::Result<object_store::ListResult> {
        Stall::wait(&self.armed).await;
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, f: &object_store::path::Path, t: &object_store::path::Path, o: object_store::CopyOptions) -> object_store::Result<()> {
        Stall::wait(&self.armed).await;
        self.inner.copy_opts(f, t, o).await
    }
    async fn rename_opts(&self, f: &object_store::path::Path, t: &object_store::path::Path, o: object_store::RenameOptions) -> object_store::Result<()> {
        Stall::wait(&self.armed).await;
        self.inner.rename_opts(f, t, o).await
    }
}

/// A writer with L0 over the compaction threshold, whose store then stops answering; the maintenance (on the same bucket,
/// still answering) compacts it, and its passes nudge that writer (`refresh_manifest`) before and after. Fix: each nudge
/// gives up after `WRITER_NUDGE_TIMEOUT`, the shutdown returns, and once the store answers again a fresh opening reads
/// every acknowledged row. Control (`CONTROL_UNBOUNDED_NUDGE`, the code before): the nudge waits for good and the
/// shutdown with it (it does not return within `limit`). Returns (a pass ran, the shutdown returned within `limit`, how
/// long it took, rows, rows read wrong).
async fn stalled_writer_pass(control: bool, limit: Duration) -> (bool, bool, Duration, usize, usize) {
    crate::lifecycle::CONTROL_UNBOUNDED_NUDGE.store(control, Relaxed);
    let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stalling: Arc<dyn ObjectStore> = Arc::new(Stall { inner: Arc::clone(&raw), armed: Arc::clone(&armed) });
    let (queue, rx) = crate::lifecycle::CompactionQueue::new();
    let lc = crate::lifecycle::Lifecycle::new(Arc::new(crate::counting::CountingStore::new(Arc::clone(&stalling))), Some(Arc::clone(&queue)));
    let id = if control { "pps-stalled-ctl" } else { "pps-stalled" };
    let mut t = lc.create(id).await.unwrap();
    let mut m = BTreeMap::new();
    let mut r = 0;
    while t.writer_l0().unwrap() < crate::lifecycle::L0_COMPACT_THRESHOLD + 1 {
        for i in 0..80 {
            apply(&mut t, &mut m, vec![(ckey(r % 10, i + 100 * r), Some(format!("v-{r}-{i}").into_bytes()))]).await;
        }
        flush(&t).await;
        r += 1;
        assert!(r < 60, "setup: L0 did not reach the threshold");
    }
    // the writer's store stops answering; the maintenance works on the bucket directly
    armed.store(true, Relaxed);
    let maint = crate::lifecycle::Maintenance::start(Arc::clone(&raw), Arc::clone(&raw), queue, rx, Arc::clone(&lc.touched), Arc::clone(&lc.maint_locks), 2, Duration::from_secs(3600), Duration::from_secs(3600));
    // the L0 watch queues a pass from the writer's view; it compacts the bucket, then nudges the stalled writer
    let t0 = Instant::now();
    let mut ran = false;
    while t0.elapsed() < Duration::from_secs(20) {
        if crate::lifecycle::PASSES_RUNNING.load(Relaxed) > 0 {
            ran = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await; // the pass reaches its nudge
    let t1 = Instant::now();
    let shut = tokio::time::timeout(limit, maint.shutdown()).await;
    let took = t1.elapsed();
    // the store answers again; the stalled writer is dropped as after a lease loss, and a fresh opening reads every row
    armed.store(false, Relaxed);
    crate::lifecycle::CONTROL_UNBOUNDED_NUDGE.store(false, Relaxed);
    let _ = tokio::time::timeout(Duration::from_secs(30), t.abandon()).await;
    let lc2 = crate::lifecycle::Lifecycle::new(Arc::new(crate::counting::CountingStore::new(Arc::clone(&raw))), None);
    let t2 = lc2.wake(id).await.unwrap();
    let mut wrong = 0;
    for (k, v) in &m {
        if t2.get(k).await.ok().flatten().as_deref() != Some(&v[..]) {
            wrong += 1;
        }
    }
    let _ = t2.close().await;
    eprintln!("pass on a stalled writer (control {control}): a pass ran {ran}; shutdown {} in {took:?}; rows {} wrong {wrong}", if shut.is_ok() { "returned" } else { "did not return" }, m.len());
    (ran, shut.is_ok(), took, m.len(), wrong)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pps_pass_on_a_stalled_writer_ends_and_shutdown_returns() {
    let _s = SERIAL.lock().await;
    setup();
    let (ran_c, shut_c, _, _, wrong_c) = stalled_writer_pass(true, Duration::from_secs(20)).await;
    let (ran, shut, took, n, wrong) = stalled_writer_pass(false, Duration::from_secs(60)).await;
    teardown();
    assert!(ran_c && ran, "setup: no compaction pass ran (the test did not reach its case)");
    assert!(!shut_c, "control: with the unbounded nudge the shutdown must not return, or the test does not reach the case");
    assert!(shut, "the maintenance's shutdown did not return within 60 s with a pass on a stalled writer");
    assert!(took < Duration::from_secs(30), "the shutdown took {took:?}");
    assert_eq!((wrong_c, wrong), (0, 0), "rows read wrong after the stall ({n} rows; rule 8)");
}
