//! P-big-speed tests (fix 2 and fix 3): the customer's box is warmed in one wave and then answers every read of that
//! customer with no GET; single flight of run block fetches; runs pinned when a version loads.
//!
//! The store adds a fixed delay to every GET and counts waves: a wave starts when a GET starts while no GET is in flight
//! (the trips of `simstore::TripMeter`, but local to the test, so tests running in parallel do not disturb it). Only GETs
//! of our runs (`/fx/` in the path) are counted: SlateDB's manifest polls are not the reads under test.

use super::build::{BuildOpts, Reseal};
use super::counter;
use super::tests::{apply, as_vec, enable, lc, model_range, publish, SERIAL};
use super::view;
use crate::lifecycle::Tenant;
use crate::runfmt::{CustomerKind, RunOptions};
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug)]
struct WaveStore {
    inner: Arc<dyn ObjectStore>,
    delay: Duration,
    on: AtomicBool,
    inflight: AtomicU64,
    waves: AtomicU64,
    gets: AtomicU64,
}

impl std::fmt::Display for WaveStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WaveStore({})", self.inner)
    }
}

impl WaveStore {
    fn new() -> Arc<WaveStore> {
        Arc::new(WaveStore { inner: Arc::new(InMemory::new()), delay: Duration::from_millis(25), on: AtomicBool::new(false), inflight: AtomicU64::new(0), waves: AtomicU64::new(0), gets: AtomicU64::new(0) })
    }
    fn start(&self) {
        self.waves.store(0, Relaxed);
        self.gets.store(0, Relaxed);
        self.on.store(true, Relaxed);
    }
    fn stop(&self) -> (u64, u64) {
        self.on.store(false, Relaxed);
        (self.waves.load(Relaxed), self.gets.load(Relaxed))
    }
}

#[async_trait]
impl ObjectStore for WaveStore {
    async fn put_opts(&self, l: &Path, p: PutPayload, o: PutOptions) -> object_store::Result<PutResult> {
        self.inner.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(&self, l: &Path, o: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, l: &Path, o: GetOptions) -> object_store::Result<GetResult> {
        let counted = self.on.load(Relaxed) && l.as_ref().contains("/fx/");
        if counted {
            self.gets.fetch_add(1, Relaxed);
            if self.inflight.fetch_add(1, Relaxed) == 0 {
                self.waves.fetch_add(1, Relaxed);
            }
        }
        if self.on.load(Relaxed) {
            tokio::time::sleep(self.delay).await;
        }
        let r = self.inner.get_opts(l, o).await;
        if counted {
            self.inflight.fetch_sub(1, Relaxed);
        }
        r
    }
    fn delete_stream(&self, l: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(l)
    }
    fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(p)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, f: &Path, t: &Path, o: object_store::CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(f, t, o).await
    }
    async fn rename_opts(&self, f: &Path, t: &Path, o: object_store::RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(f, t, o).await
    }
}

fn comp(c: usize) -> Vec<u8> {
    let name = format!("c{c:06}");
    let mut k = vec![name.len() as u8];
    k.extend(name.as_bytes());
    k
}

fn key(c: usize, rest: &str) -> Vec<u8> {
    let mut k = b"u/".to_vec();
    k.extend(comp(c));
    k.extend(rest.as_bytes());
    k
}

fn doc(c: usize, i: usize) -> Vec<u8> {
    key(c, &format!("ckpt/d/{i:06}"))
}

fn idx(c: usize, i: usize) -> Vec<u8> {
    key(c, &format!("ckpt/x/ts/{i:06}"))
}

fn wdoc(c: usize, i: usize) -> Vec<u8> {
    key(c, &format!("writes/d/{i:06}"))
}

fn range(c: usize, area: &str) -> (Vec<u8>, Vec<u8>) {
    let lo = key(c, area);
    let hi = crate::runfmt::prefix_successor(&lo).unwrap();
    (lo, hi)
}

fn opts(reseal: Reseal) -> BuildOpts {
    BuildOpts { reseal, group_bytes: 1 << 30, sealed: RunOptions::unchecked(32 << 10, 1 << 20, CustomerKind::V2), delta: RunOptions::unchecked(4096, 1 << 20, CustomerKind::V2) }
}

/// A database whose customers' rows sit in a sealed run, two delta runs above it and SlateDB above X_d: 80 customers of
/// 20 checkpoints (document and index entry) and 10 pending writes each; then updates of customers 0..40 in two delta
/// publishes; then writes to customers 0..10 that stay in SlateDB.
async fn build(store: &Arc<dyn ObjectStore>, name: &str) -> (Tenant, BTreeMap<Vec<u8>, Vec<u8>>) {
    let mut t = lc(store).create(name).await.unwrap();
    let mut m = BTreeMap::new();
    for c in 0..80 {
        let mut ops = Vec::new();
        for i in 0..20 {
            ops.push((doc(c, i), Some(vec![(c % 251) as u8; 300])));
            ops.push((idx(c, i), Some(doc(c, i))));
        }
        for i in 0..10 {
            ops.push((wdoc(c, i), Some(vec![7u8; 120])));
        }
        apply(&mut t, &mut m, ops).await;
    }
    publish(&mut t, &opts(Reseal::All)).await;
    for round in 0..2 {
        for c in 0..40 {
            let ops = vec![(doc(c, 20 + round), Some(vec![1u8; 300])), (idx(c, 20 + round), Some(doc(c, 20 + round))), (doc(c, 3), Some(vec![round as u8 + 2; 300]))];
            apply(&mut t, &mut m, ops).await;
        }
        publish(&mut t, &opts(Reseal::None)).await;
    }
    for c in 0..10 {
        apply(&mut t, &mut m, vec![(doc(c, 30), Some(vec![9u8; 300])), (idx(c, 30), Some(doc(c, 30)))]).await;
    }
    // the runs' maps, as `View::pin_runs` opens them in the background when a version loads
    t.fx_view().unwrap().open_all(16).await.unwrap();
    (t, m)
}

/// The reads of one visit: the newest checkpoint through the index and its document, its pending writes, the last 10,
/// a time travel, the whole history. Every answer checked against the model.
async fn visit(t: &Tenant, m: &BTreeMap<Vec<u8>, Vec<u8>>, c: usize) {
    let (lo, hi) = range(c, "ckpt/x/ts/");
    let ix = t.scan_range(&lo, Some(&hi), true, Some(1), 64 << 10).await.unwrap();
    assert_eq!(as_vec(ix.clone()), model_range(m, &lo, Some(&hi), true, Some(1)), "newest of customer {c}");
    let d = t.get(&ix[0].1[..]).await.unwrap();
    assert_eq!(d.as_deref(), m.get(&ix[0].1[..]).map(|v| &v[..]), "document of customer {c}");
    let (wlo, whi) = range(c, "writes/d/");
    assert_eq!(as_vec(t.scan_range(&wlo, Some(&whi), false, None, 64 << 10).await.unwrap()), model_range(m, &wlo, Some(&whi), false, None), "writes of {c}");
    assert_eq!(as_vec(t.scan_range(&lo, Some(&hi), true, Some(10), 64 << 10).await.unwrap()), model_range(m, &lo, Some(&hi), true, Some(10)), "last 10 of {c}");
    let old = doc(c, 3);
    assert_eq!(t.get(&old).await.unwrap().as_deref(), m.get(&old).map(|v| &v[..]), "time travel of {c}");
    let (clo, chi) = range(c, "");
    assert_eq!(as_vec(t.scan_range(&clo, Some(&chi), false, None, 64 << 10).await.unwrap()), model_range(m, &clo, Some(&chi), false, None), "box of {c}");
}

/// Fix 3: the warm-up of a customer's box is ONE wave of GETs (every run that holds the customer at once), and after it
/// every read of the customer's visit is answered with no GET, exactly. The control: the same visit of a customer not
/// warmed waits for more than one wave.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn box_warm_is_one_wave_then_the_visit_needs_no_get() {
    let _s = SERIAL.lock().await;
    enable();
    let ws = WaveStore::new();
    let store: Arc<dyn ObjectStore> = ws.clone();
    let (t, m) = build(&store, "bs-box").await;
    let (opened, all) = t.fx_view().unwrap().runs_opened();
    assert_eq!(opened, all, "every run open");
    assert!(all >= 3, "a sealed run and two delta runs: {all}");
    for c in [2usize, 5, 17, 33, 60] {
        view::clear_cache();
        let (lo, hi) = range(c, "");
        ws.start();
        let (runs, blocks, _rows) = t.warm_range(&lo, &hi, 8 << 20).await.unwrap();
        let (waves, gets) = ws.stop();
        assert_eq!(waves, 1, "customer {c}: the warm-up is one wave ({gets} GETs, {runs} runs, {blocks} blocks)");
        assert!(gets >= 1 && blocks >= 1, "customer {c}: the warm-up read the runs");
        ws.start();
        visit(&t, &m, c).await;
        let (waves, gets) = ws.stop();
        assert_eq!((waves, gets), (0, 0), "customer {c}: the visit after the warm-up needs no GET");
    }
    // control: the same visit, cold, without the warm-up
    view::clear_cache();
    ws.start();
    visit(&t, &m, 21).await;
    let (waves, gets) = ws.stop();
    assert!(waves >= 2, "control: a cold visit without the warm-up waits for {waves} waves ({gets} GETs)");
}

/// Fix 2, single flight: two reads of the same customer at once on a cold counter send each run block GET once.
/// Control: with single flight off, the same two reads send them twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_flight_sends_each_block_get_once() {
    let _s = SERIAL.lock().await;
    enable();
    let ws = WaveStore::new();
    let store: Arc<dyn ObjectStore> = ws.clone();
    let (t, m) = build(&store, "bs-flight").await;
    let (lo, hi) = range(44, "");
    let want = model_range(&m, &lo, Some(&hi), false, None);
    // one read alone: its GETs
    view::clear_cache();
    ws.start();
    assert_eq!(as_vec(t.scan_range(&lo, Some(&hi), false, None, 8 << 20).await.unwrap()), want);
    let (_, one) = ws.stop();
    assert!(one >= 1);
    for (on, expect) in [(true, one), (false, 2 * one)] {
        counter::SINGLE_FLIGHT.store(on, Relaxed);
        view::clear_cache();
        ws.start();
        let (a, b) = futures::future::join(t.scan_range(&lo, Some(&hi), false, None, 8 << 20), t.scan_range(&lo, Some(&hi), false, None, 8 << 20)).await;
        let (_, gets) = ws.stop();
        counter::SINGLE_FLIGHT.store(true, Relaxed);
        assert_eq!(as_vec(a.unwrap()), want);
        assert_eq!(as_vec(b.unwrap()), want);
        assert_eq!(gets, expect, "single flight {on}: {gets} GETs for two reads at once (one read alone: {one})");
    }
}

/// Fix 2, the runs' maps pinned: a version's runs are opened in the background as soon as it loads (`View::pin_runs`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runs_open_when_a_version_loads() {
    let _s = SERIAL.lock().await;
    enable();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut t = lc(&store).create("bs-pin").await.unwrap();
    let mut m = BTreeMap::new();
    for c in 0..30 {
        apply(&mut t, &mut m, (0..10).map(|i| (doc(c, i), Some(vec![3u8; 200]))).collect()).await;
    }
    publish(&mut t, &opts(Reseal::All)).await;
    for c in 0..30 {
        apply(&mut t, &mut m, vec![(doc(c, 11), Some(vec![4u8; 200]))]).await;
    }
    publish(&mut t, &opts(Reseal::None)).await;
    // a new process: the version loaded from the store, nothing read yet
    t.close().await.unwrap();
    let t = lc(&store).wake("bs-pin").await.unwrap();
    let v = t.fx_view().unwrap().clone();
    let mut ok = false;
    for _ in 0..100 {
        let (o, a) = v.runs_opened();
        if o == a && a >= 2 {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(ok, "runs opened {:?} without any read", v.runs_opened());
    for c in [0usize, 7, 29] {
        let (lo, hi) = range(c, "");
        assert_eq!(as_vec(t.scan_range(&lo, Some(&hi), false, None, 64 << 10).await.unwrap()), model_range(&m, &lo, Some(&hi), false, None));
    }
}

/// Fix 3, "in memory, then on local disk": after the warm-up, the runs' RAM cache is emptied (the box left the RAM);
/// the customer's visit is then served from the disk floor with no GET, exactly. A dropped database leaves no byte of
/// its blocks in the floor's files.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn box_comes_back_from_the_disk_floor() {
    let _s = SERIAL.lock().await;
    enable();
    let dir = std::env::temp_dir().join(format!("bs-floor-{}", std::process::id()));
    super::floor::install(dir.clone(), 256 << 20).unwrap();
    let ws = WaveStore::new();
    let store: Arc<dyn ObjectStore> = ws.clone();
    let (t, m) = build(&store, "bs-floor").await;
    for c in [4usize, 12, 50] {
        view::clear_cache();
        let (lo, hi) = range(c, "");
        t.warm_range(&lo, &hi, 8 << 20).await.unwrap();
        view::clear_cache();
        let h0 = super::floor::HITS.load(Relaxed);
        ws.start();
        visit(&t, &m, c).await;
        let (waves, gets) = ws.stop();
        assert_eq!((waves, gets), (0, 0), "customer {c}: the visit from the disk floor needs no GET");
        assert!(super::floor::HITS.load(Relaxed) > h0, "customer {c}: served by the floor");
    }
    super::floor::forget_db("bs-floor");
    assert_eq!(super::floor::usage().1, 0, "every block of the dropped database forgotten");
    let filler = vec![9u8; 300];
    let on_disk: Vec<u8> = std::fs::read_dir(&dir).unwrap().flat_map(|e| std::fs::read(e.unwrap().path()).unwrap()).collect();
    assert!(on_disk.iter().all(|b| *b == 0) || !on_disk.windows(filler.len()).any(|w| w == &filler[..]), "zeroed");
    assert!(on_disk.iter().all(|b| *b == 0), "a dropped database's run blocks are still in the floor's files");
    super::floor::install(dir.clone(), 0).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- Review of big-speed (2026-11-13): tests written by the reviewer ----

/// Review R2: a run the new version retires is wiped from the disk floor (`forget_runs`), but a hold on the older
/// version that reads it afterwards ("those runs are read again only by holds on older versions, which fetch what they
/// need", lifecycle.rs `fx_install`) writes its blocks back into the floor, and no later step wipes them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rvbs_retired_run_comes_back_into_the_floor_after_its_wipe() {
    let _s = SERIAL.lock().await;
    enable();
    let dir = std::env::temp_dir().join(format!("rv-floor-{}", std::process::id()));
    super::floor::install(dir.clone(), 256 << 20).unwrap();
    let ws = WaveStore::new();
    let store: Arc<dyn ObjectStore> = ws.clone();
    let (mut t, mut m) = build(&store, "rv-floor").await;
    let c = 4usize;
    let (lo, hi) = range(c, "");
    view::clear_cache();
    t.warm_range(&lo, &hi, 8 << 20).await.unwrap();
    let old = t.fx_view().unwrap().clone();
    apply(&mut t, &mut m, vec![(doc(c, 40), Some(vec![5u8; 300]))]).await;
    publish(&mut t, &opts(Reseal::All)).await;
    let new = t.fx_view().unwrap().clone();
    let gone = super::counter::retired(&old, &new);
    let held = |ids: &std::collections::HashSet<[u8; crate::runfmt::RUN_ID_LEN]>| -> usize {
        ids.iter().map(|id| (0..4096u32).filter(|b| super::floor::get(id, *b).is_some()).count()).sum()
    };
    let after_wipe = held(&gone);
    view::clear_cache();
    old.warm_range(&lo, &hi).await.unwrap();
    let after_hold = held(&gone);
    println!("RVBS retired runs {}, floor blocks of retired runs: after the wipe {after_wipe}, after a hold's read {after_hold}", gone.len());
    super::floor::install(dir.clone(), 0).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(!gone.is_empty(), "the reseal retired no run");
    assert_eq!(after_wipe, 0, "forget_runs left blocks of retired runs in the floor");
    assert_eq!(after_hold, 0, "blocks of a retired run were written back into the disk floor after its wipe");
}

/// Review R3: single flight with a cancelled leader: a read waiting for a cancelled fetch must still finish, exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rvbs_single_flight_survives_a_cancelled_leader() {
    let _s = SERIAL.lock().await;
    enable();
    let ws = WaveStore::new();
    let store: Arc<dyn ObjectStore> = ws.clone();
    let (t, m) = build(&store, "rv-sf").await;
    let v = t.fx_view().unwrap().clone();
    let mut total_waits = 0;
    for c in [3usize, 21, 60] {
        view::clear_cache();
        let (lo, hi) = range(c, "");
        ws.start();
        let v1 = Arc::clone(&v);
        let (lo1, hi1) = (lo.clone(), hi.clone());
        let leader = tokio::spawn(async move { v1.warm_range(&lo1, &hi1).await });
        tokio::time::sleep(Duration::from_millis(8)).await;
        let w0 = counter::SINGLE_FLIGHT_WAITS.load(Relaxed);
        let v2 = Arc::clone(&v);
        let (lo2, hi2) = (lo.clone(), hi.clone());
        let follower = tokio::spawn(async move { v2.warm_range(&lo2, &hi2).await });
        tokio::time::sleep(Duration::from_millis(4)).await;
        leader.abort();
        let r = tokio::time::timeout(Duration::from_secs(10), follower).await;
        ws.stop();
        assert!(r.is_ok(), "customer {c}: the follower hung after its leader was cancelled");
        r.unwrap().unwrap().unwrap();
        total_waits += counter::SINGLE_FLIGHT_WAITS.load(Relaxed) - w0;
        visit(&t, &m, c).await;
    }
    println!("RVBS single-flight waits on a cancelled leader: {total_waits}");
    assert!(total_waits > 0, "the follower never waited: the test did not exercise single flight");
}
