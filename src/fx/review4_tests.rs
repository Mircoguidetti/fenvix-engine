//! Review of compaction 2, step 4 (reviewer session, 2026-11-09). Tests the review prompt asks for:
//! - Q1: a checkpoint lease taken while a drop job runs (the lease is checked only at job start): pairs stay right?
//! - the lease format step 4 reads (`until_ms`) against the one step 7a writes (`expires_ms`);
//! - Q4: step 7a's `take_lease`, two takers after an expired lease (copied verbatim from `c2-step7a-paths` 06fb271,
//!   `src/fx/pair.rs`, because that branch is not under this one);
//! - Q5: R1 copies on a real local directory, a crash with a run half written, and a damaged copy;
//! - Q6: what a restart's first floor computation reads, and the size of a version with 1,000 groups.

use super::compactor::{Compactor, Knobs};
use super::tests::*;
use super::*;
use crate::lifecycle::Tenant;
use crate::runfmt::{CustomerKind, RunOptions};
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutMode, PutOptions, PutPayload, PutResult};
use object_store::{CopyOptions, RenameOptions};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed, Ordering::SeqCst};
use std::sync::Arc;
use std::time::{Duration, Instant};

const HOLD_MS: u64 = 3000;

fn knobs(drop: bool) -> Knobs {
    Knobs {
        theta: 0.25,
        r3: true,
        r3_gets_per_gib: 470.0,
        r2_age: None,
        tier_t: 4,
        tier_unit: 2 << 10,
        absorb_min_l0: 1,
        absorb_max_l0: 8,
        recognise_in_background: false,
        local_max_bytes: 16 << 10,
        split_bytes: 40 << 10,
        merge_bytes: 18 << 10,
        min_seal_bytes: 8 << 10,
        sealed: RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2),
        delta: RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2),
        drop,
        fold_one_run: std::env::var("FX_FOLD_ONE").is_ok(),
        fold_max_runs: 6,
        fold_at_l0: 6,
        lease: Duration::from_millis(400),
        remote_lease: Duration::from_millis(HOLD_MS + 600),
        serving_lease: Duration::from_millis(300),
        put_timeout: Duration::from_millis(250),
        gc_grace: Duration::from_millis(HOLD_MS + 800),
        version_retention: Duration::from_millis(0),
        gc_every: Duration::from_millis(500),
        tick: Duration::from_millis(10),
        reseal_parallel: 1,
        read_ahead: 64 << 10,
        derive: None,
    }
}

fn setup() {
    enable();
    hold::HOLD_LIFE_MS.store(HOLD_MS, Relaxed);
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(3 << 10, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(32, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(1, Relaxed);
    local::set_store(Some(Arc::new(InMemory::new())));
    view::clear_cache();
}

fn teardown() {
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(30, Relaxed);
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
    local::set_store(None);
    compactor::REVIEW_PLANT.store(0, Relaxed);
}

async fn start(t: &mut Tenant, store: &Arc<dyn ObjectStore>, k: &Knobs) -> Arc<Compactor> {
    let c = Compactor::start(t, Arc::clone(store), k.clone(), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    c.run();
    c
}

async fn flush(t: &Tenant) {
    let db = t.fx_writer_db().unwrap();
    db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
}

fn lease_path(id: &str) -> Path {
    Path::from(format!("{}/drop-lease", manifest::root(id)))
}

// ------------------------------------------------------------------------------------------------------------------
// Q1: a checkpoint lease taken while a drop job runs

/// Pairing as step 7a does it (rule 18), with the lease in the format step 4's drop reads: take the lease, let a drop
/// job that was running finish under it, create SlateDB's checkpoint, pick the newest version with X_d <= the
/// checkpoint's `last_l0_seq`, release the lease. Then every key is read through the checkpoint and the picked version
/// (the merged rule) and compared with the model at that moment. `control`: pair with the oldest listed version
/// instead (a pair that is too old must show wrong answers once rows were dropped: the test can see a lost row).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r4_q1_lease_taken_during_a_running_drop() {
    let _s = SERIAL.lock().await;
    setup();
    let rounds: usize = std::env::var("R4_Q1_ROUNDS").ok().and_then(|s| s.parse().ok()).unwrap_or(60);
    for control in [false, true] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = if control { "r4-q1-ctl" } else { "r4-q1" };
        let keys = universe();
        let mut rng = StdRng::seed_from_u64(7 + control as u64);
        let mut t = lc(&store).create(id).await.unwrap();
        let mut m: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        for (i, k) in keys.iter().enumerate() {
            if i % 3 == 0 {
                apply(&mut t, &mut m, vec![(k.clone(), Some(vec![b'a'; 40]))]).await;
            }
        }
        publish(&mut t, &small_opts(build::Reseal::All)).await;
        let mut k = knobs(true);
        if control {
            k.version_retention = Duration::from_secs(3600);
        }
        let c = start(&mut t, &store, &k).await;
        let admin = slatedb::admin::Admin::builder(crate::lifecycle::tenant_path(id), Arc::clone(&store)).build();
        let (mut pairs, mut wrong, mut overlapped, mut dropped_under_lease, mut errors) = (0u64, 0u64, 0u64, 0u64, 0u64);
        for round in 0..rounds {
            // writes: new values, rewrites and deletes; the compactor absorbs and the drop folds in the background
            for _ in 0..rng.gen_range(20..80) {
                let key = keys[rng.gen_range(0..keys.len())].clone();
                let v = if rng.gen_bool(0.15) { None } else { Some(vec![b'0' + (round % 10) as u8; rng.gen_range(1..60)]) };
                apply(&mut t, &mut m, vec![(key, v)]).await;
                t.fx_sync();
            }
            if round % 3 != 2 {
                tokio::time::sleep(Duration::from_millis(rng.gen_range(0..200))).await;
                continue;
            }
            // the pairing; the memtable first goes to L0, so the checkpoint holds every row (writes are paused)
            flush(&t).await;
            t.fx_sync();
            let snapshot = m.clone();
            // hold the next drop job once its floor is computed (it started before the lease), take the lease, let it go
            super::drop::REVIEW_JOB_HELD.store(false, Relaxed);
            super::drop::REVIEW_HOLD_JOB.store(true, Relaxed);
            let t0 = Instant::now();
            while !super::drop::REVIEW_JOB_HELD.load(Relaxed) && t0.elapsed() < Duration::from_secs(3) {
                c.drop.request_refold();
                c.drop.maybe_refold();
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            let running = super::drop::REVIEW_JOB_HELD.load(Relaxed);
            let job_floor = c.drop.last_floor.load(Relaxed);
            let (done0, dropped0) = (c.drop.jobs_done.load(Relaxed), c.drop.rows_dropped.load(Relaxed));
            let until = manifest::now_ms() + 30_000;
            let body = serde_json::json!({ "until_ms": until, "holder": "review" });
            store.put_opts(&lease_path(id), PutPayload::from(serde_json::to_vec(&body).unwrap()), PutOptions { mode: PutMode::Create, ..Default::default() }).await.unwrap();
            super::drop::REVIEW_HOLD_JOB.store(false, Relaxed);
            // the job that was running commits under the lease
            let t0 = Instant::now();
            while running && c.drop.jobs_done.load(Relaxed) == done0 && t0.elapsed() < Duration::from_secs(3) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            // and SlateDB's manifest has its output before the checkpoint
            tokio::time::sleep(Duration::from_millis(50)).await;
            if running && c.drop.jobs_done.load(Relaxed) > done0 && job_floor > 0 {
                overlapped += 1;
            }
            dropped_under_lease += c.drop.rows_dropped.load(Relaxed) - dropped0;
            let cp = admin.create_detached_checkpoint(&slatedb::config::CheckpointOptions::default()).await.unwrap();
            let sm = admin.read_manifest(Some(cp.manifest_id)).await.unwrap().unwrap();
            let last_l0 = sm.last_l0_seq();
            let versions = manifest::list_versions(&store, id).await.unwrap();
            let mut pick = None;
            if control {
                // a version below the floor of the job that just ran (kept by the long retention of the control)
                let fl = c.drop.last_floor.load(Relaxed);
                for &n in versions.iter().rev() {
                    let v = manifest::load(&store, id, n).await.unwrap();
                    if v.x_d < fl {
                        pick = Some(v);
                        break;
                    }
                }
                if pick.is_none() {
                    store.delete(&lease_path(id)).await.unwrap();
                    continue;
                }
            } else {
                for &n in versions.iter().rev() {
                    let v = manifest::load(&store, id, n).await.unwrap();
                    if v.x_d <= last_l0 {
                        pick = Some(v);
                        break;
                    }
                }
            }
            store.delete(&lease_path(id)).await.unwrap();
            let v = pick.expect("no version at or below the checkpoint's last_l0_seq");
            let env = v.e_env;
            let view = view::View::new(Arc::clone(&store), v, None).unwrap();
            let reader = read::open_checkpoint_reader(crate::lifecycle::tenant_path(id), Arc::clone(&store), cp.id, Default::default()).await.unwrap();
            for key in &keys {
                match read::get(&reader, &Default::default(), Some(&view), env, key).await {
                    Ok(got) => {
                        if got.map(|b| b.to_vec()) != snapshot.get(key).cloned() {
                            wrong += 1;
                        }
                    }
                    Err(_) => errors += 1,
                }
            }
            pairs += 1;
            reader.close().await.ok();
        }
        eprintln!("Q1 (control {control}): pairs {pairs}, pairs whose lease was taken during a running drop job that then committed {overlapped}, rows dropped by jobs that committed under the lease {dropped_under_lease}, rows dropped in all {}, wrong {wrong}, errors {errors}", c.drop.rows_dropped.load(Relaxed));
        assert!(c.drop.rows_dropped.load(Relaxed) > 0, "no drop: the test proves nothing");
        if control {
            assert!(wrong > 0, "the control should miss rows");
        } else {
            assert!(overlapped > 0, "no lease was taken during a running drop");
            assert_eq!(wrong + errors, 0, "a paired read lost a row");
        }
        c.stop().await;
        t.abandon().await;
    }
    teardown();
}

// ------------------------------------------------------------------------------------------------------------------
// The lease format: step 7a writes `expires_ms`, step 4 reads `until_ms`

/// Step 7a's `take_lease` writes `{"holder", "nonce", "taken_ms", "expires_ms"}` (pair.rs, 06fb271); step 4's drop reads
/// `until_ms` and treats a lease without it as held forever. A control layer that dies holding the lease then stops
/// every drop of the database until some later checkpoint replaces the object. Fails today.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r4_expired_lease_in_step7a_format_must_not_stop_the_drop() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "r4-lease-fmt";
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..300 {
        apply(&mut t, &mut m, vec![(ckey(1, i % 50), Some(vec![b'v'; 50]))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let mut k = knobs(true);
    k.serving_lease = Duration::from_millis(10);
    hold::HOLD_LIFE_MS.store(10, Relaxed);
    k.remote_lease = Duration::from_millis(400);
    k.gc_grace = Duration::from_millis(400);
    let c = start(&mut t, &store, &k).await;
    for i in 0..300 {
        apply(&mut t, &mut m, vec![(ckey(2, i % 50), Some(vec![b'w'; 50]))]).await;
    }
    flush(&t).await;
    c.drain(false).await.unwrap();
    t.fx_sync();
    tokio::time::sleep(Duration::from_millis(600)).await;
    c.drop.floor().await.unwrap(); // first sights
    tokio::time::sleep(Duration::from_millis(600)).await;
    let before = c.drop.floor().await.unwrap();
    // a lease exactly as step 7a writes it, expired an hour ago (its holder died)
    let now = manifest::now_ms();
    let l = serde_json::json!({ "holder": "pid-1", "nonce": 42u64, "taken_ms": now - 3_660_000, "expires_ms": now - 3_600_000 });
    store.put(&lease_path(id), PutPayload::from(serde_json::to_vec(&l).unwrap())).await.unwrap();
    let after = c.drop.floor().await.unwrap();
    eprintln!("lease format: floor before the lease {before}, with a step 7a lease expired one hour ago {after}");
    c.stop().await;
    t.abandon().await;
    teardown();
    assert!(before > 0, "the floor must be above 0 before the lease (test setup)");
    assert_eq!(after, before, "an expired lease (step 7a's format) still stops the drop");
}

// ------------------------------------------------------------------------------------------------------------------
// Q4: step 7a's take_lease, two takers after an expired lease

mod step7a_copy {
    //! The reviewer copied step 7a's `Lease`, `lease_held` and `take_lease` here verbatim, because `c2-step7a-paths`
    //! was not under the review branch. The fix round merged it: these now name the real code (`fx/pair.rs`), so the
    //! test below, unchanged, runs against the fix.
    #[allow(unused_imports)]
    pub use super::super::pair::{lease_held, lease_path, take_lease, Lease, LEASE_LIFE_MS, SKEW_MARGIN_MS};
}

/// Q4 (step 7a review, note 2): two control-layer processes take the drop lease after it expired. Both read the
/// expired lease; A deletes it and creates its own; B's delete arrives after A's create and removes A's fresh lease; B
/// creates its own. Both believe they hold the lease, and between B's delete and B's create no lease exists (a drop job
/// starting then sees none). Fails today: both takers return a lease.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r4_q4_two_takers_of_an_expired_lease() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "r4-q4";
    let now = manifest::now_ms();
    let dead = step7a_copy::Lease { holder: "dead".into(), nonce: 1, taken_ms: now - 120_000, expires_ms: now - 60_000 };
    inner.put(&step7a_copy::lease_path(id), PutPayload::from(serde_json::to_vec(&dead).unwrap())).await.unwrap();
    // the interleaving, made exact with a store wrapper: B's delete waits for A's create
    let gate = Arc::new(GateStore::new(Arc::clone(&inner)));
    let store: Arc<dyn ObjectStore> = gate.clone();
    let (sa, sb) = (Arc::clone(&store), Arc::clone(&store));
    let a = tokio::spawn(async move { step7a_copy::take_lease(&sa, "r4-q4").await });
    let b = tokio::spawn(async move { step7a_copy::take_lease(&sb, "r4-q4").await });
    let (a, b) = (a.await.unwrap(), b.await.unwrap());
    let in_store: step7a_copy::Lease = serde_json::from_slice(&inner.get(&step7a_copy::lease_path(id)).await.unwrap().bytes().await.unwrap()).unwrap();
    let holders: Vec<u64> = [&a, &b].iter().filter_map(|r| r.as_ref().ok().map(|l| l.nonce)).collect();
    eprintln!("Q4: taker A {:?}, taker B {:?}; the lease in the store has nonce {}; windows with no lease in the store seen by a drop: {}", a.as_ref().map(|l| l.nonce), b.as_ref().map(|l| l.nonce), in_store.nonce, gate.empty_seen.load(SeqCst));
    assert!(holders.len() <= 1, "two takers both hold the drop lease: {holders:?}");
}

/// The wrapper of Q4: the second DELETE waits (asynchronously) until a create succeeded after the first DELETE; while
/// it waits, a drop job's check (a GET) is simulated and counted when it finds no lease.
#[derive(Debug)]
pub(super) struct GateStore {
    inner: Arc<dyn ObjectStore>,
    deletes: AtomicUsize,
    gets_blocked: AtomicUsize,
    created: Arc<tokio::sync::watch::Sender<bool>>,
    pub(super) empty_seen: Arc<AtomicU64>,
}

impl GateStore {
    pub(super) fn new(inner: Arc<dyn ObjectStore>) -> GateStore {
        let (tx, _rx) = tokio::sync::watch::channel(false);
        GateStore { inner, deletes: AtomicUsize::new(0), gets_blocked: AtomicUsize::new(0), created: Arc::new(tx), empty_seen: Arc::new(AtomicU64::new(0)) }
    }
}

impl std::fmt::Display for GateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GateStore")
    }
}

#[async_trait]
impl ObjectStore for GateStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        let r = self.inner.put_opts(location, payload, opts).await;
        eprintln!("gate: put {} -> {}", location, if r.is_ok() { "ok" } else { "exists" });
        if r.is_ok() && self.deletes.load(SeqCst) >= 1 {
            self.created.send_replace(true);
        }
        r
    }
    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        // both takers read the expired lease before either acts on it: each GET returns only once two GETs were served
        let r = self.inner.get_opts(location, options).await;
        let n = self.gets_blocked.fetch_add(1, SeqCst);
        eprintln!("gate: get #{n} {}", if r.is_ok() { "found" } else { "missing" });
        if n < 2 {
            let t0 = Instant::now();
            while self.gets_blocked.load(SeqCst) < 2 && t0.elapsed() < Duration::from_secs(2) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        r
    }
    fn delete_stream(&self, locations: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        use futures::StreamExt;
        let n = self.deletes.fetch_add(1, SeqCst);
        let inner = Arc::clone(&self.inner);
        let mut rx = self.created.subscribe();
        let empty = Arc::clone(&self.empty_seen);
        eprintln!("gate: delete #{n} called");
        let fut = async move {
            if n == 1 {
                // B: its delete reaches the store after A's create
                let _ = tokio::time::timeout(Duration::from_secs(2), rx.wait_for(|c| *c)).await;
            }
            let res: Vec<object_store::Result<Path>> = inner.delete_stream(locations).collect().await;
            if n == 1 {
                for p in res.iter().flatten() {
                    // a drop job checking the lease now finds none
                    if inner.head(p).await.is_err() {
                        empty.fetch_add(1, SeqCst);
                    }
                }
            }
            futures::stream::iter(res)
        };
        futures::stream::once(fut).flatten().boxed()
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

// ------------------------------------------------------------------------------------------------------------------
// Q5: R1 copies on a real local directory

/// Delta runs with NVMe copies on a real directory (object_store's LocalFileSystem, as `--sealed-local-dir` uses).
/// (a) A crash while a run is half written: LocalFileSystem writes a multipart upload to a staging file and renames it
/// at completion, so a crash leaves a staging file and nothing at the run's path; a new waiter on the same disk reads.
/// (b) A copy cut to half its size (a torn write). (c) A copy of the right size with a damaged block. Reads must fall
/// back to the store in each case, and the compactor's own jobs (merges, reseals, erasures) must still run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r4_q5_nvme_copies_half_written_or_damaged() {
    let _s = SERIAL.lock().await;
    setup();
    let dir = std::env::temp_dir().join(format!("r4-q5-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let nvme = local::dir_store(&dir).unwrap();
    local::set_store(Some(Arc::clone(&nvme)));
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "r4-q5";
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..400 {
        apply(&mut t, &mut m, vec![(ckey(i % 6, i), Some(vec![b'v'; 30]))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let mut k = knobs(false);
    k.theta = 1000.0; // the rows stay in delta runs until the test asks
    k.r3 = false;
    k.local_max_bytes = 1 << 20;
    let c = Compactor::start(&mut t, Arc::clone(&store), k.clone(), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    for round in 0..4 {
        for i in 0..150 {
            apply(&mut t, &mut m, vec![(ckey(i % 6, 1000 + i + round * 150), Some(vec![b'n'; 30]))]).await;
        }
        flush(&t).await;
        c.absorb_once().await.unwrap();
    }
    t.fx_sync();
    let v = c.current().await;
    let locals: Vec<_> = v.delta.iter().filter(|d| d.local).cloned().collect();
    assert!(locals.len() >= 2, "need two delta runs with copies, got {}", locals.len());
    // (a) a crash mid-write: a staging file of LocalFileSystem's multipart upload, never completed
    let half = Path::from(format!("{}/run/delta/half-written", manifest::root(id)));
    let mut up = nvme.put_multipart(&half).await.unwrap();
    up.put_part(PutPayload::from(vec![7u8; 4096])).await.unwrap();
    std::mem::forget(up); // the process dies: no complete, no abort
    let staging: Vec<String> = walk(&dir).into_iter().filter(|p| p.contains("half-written")).collect();
    // (b) a torn copy: the first local run cut to half its size; (c) a damaged copy: a block of the second zeroed
    let p_b = dir.join(locals[0].path.as_str());
    let f = std::fs::OpenOptions::new().write(true).open(&p_b).unwrap();
    f.set_len(locals[0].size / 2).unwrap();
    let p_c = dir.join(locals[1].path.as_str());
    let mut bytes = std::fs::read(&p_c).unwrap();
    let mid = bytes.len() / 3;
    for b in &mut bytes[mid..mid + 64] {
        *b ^= 0xff;
    }
    std::fs::write(&p_c, &bytes).unwrap();
    // reads, in this process and in a new waiter on the same disk (the restart of a crashed waiter)
    view::clear_cache();
    let mut wrong = 0;
    for (key, want) in &m {
        if t.get(key).await.unwrap().as_deref() != Some(&want[..]) {
            wrong += 1;
        }
    }
    let r = lc(&store).wake(id).await.unwrap();
    view::clear_cache();
    let mut wrong_new = 0;
    for (key, want) in &m {
        if r.get(key).await.unwrap().as_deref() != Some(&want[..]) {
            wrong_new += 1;
        }
    }
    r.abandon().await;
    // the compactor's jobs read the same runs: a merge of the delta tier, then a reseal of every group
    let mut job_errors = Vec::new();
    let mut tries = 0;
    loop {
        tries += 1;
        match c.merge_once().await {
            Ok(true) => continue,
            Ok(false) => break,
            Err(e) => {
                job_errors.push(format!("merge: {e:#}"));
                if tries > 3 {
                    break;
                }
            }
        }
    }
    let nv = c.current().await;
    for gi in 0..nv.groups.len() {
        if let Err(e) = c.reseal_once(Some(gi)).await {
            job_errors.push(format!("reseal {gi}: {e:#}"));
        }
    }
    eprintln!("Q5: staging files left by the half-written run {staging:?}; reads wrong {wrong} (this waiter), {wrong_new} (a new waiter on the same disk); compactor job errors {}: {:?}", job_errors.len(), job_errors);
    c.stop().await;
    t.abandon().await;
    local::set_store(None);
    let _ = std::fs::remove_dir_all(&dir);
    teardown();
    assert_eq!(wrong + wrong_new, 0, "a read through a damaged NVMe copy gave a wrong answer");
    assert!(job_errors.is_empty(), "the compactor's jobs fail on a damaged NVMe copy (the reads fall back, the jobs do not)");
}

fn walk(d: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(d) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(walk(&p));
            } else {
                out.push(p.to_string_lossy().to_string());
            }
        }
    }
    out
}

// ------------------------------------------------------------------------------------------------------------------
// Q6: what a restart reads before its first drop, and the size of a version with 1,000 groups

/// A store that counts GETs and their bytes.
#[derive(Debug)]
struct Count {
    inner: Arc<dyn ObjectStore>,
    gets: AtomicU64,
    get_bytes: AtomicU64,
    lists: AtomicU64,
}

impl std::fmt::Display for Count {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Count")
    }
}

#[async_trait]
impl ObjectStore for Count {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let r = self.inner.get_opts(location, options).await?;
        self.gets.fetch_add(1, Relaxed);
        self.get_bytes.fetch_add(r.range.end - r.range.start, Relaxed);
        Ok(r)
    }
    fn delete_stream(&self, locations: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.lists.fetch_add(1, Relaxed);
        self.inner.list(prefix)
    }
    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// Q6. (1) The size of one version with 1,000 groups and 12 delta runs, from the lines of a real version. (2) The first
/// floor computation of a new waiter (a restart or a takeover) loads every version in the store, one GET each, one
/// after the other, before the first drop job can start; with the 7-day retention of restore points that is every
/// version of 7 days.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r4_q6_manifest_size_and_restart_reads() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "r4-q6";
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..600 {
        apply(&mut t, &mut m, vec![(ckey(i % 30, i), Some(vec![b'v'; 40]))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let c = Compactor::start(&mut t, Arc::clone(&store), knobs(false), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    for i in 0..150 {
        apply(&mut t, &mut m, vec![(ckey(i % 30, 5000 + i), Some(vec![b'n'; 40]))]).await;
    }
    flush(&t).await;
    c.absorb_once().await.unwrap();
    let v = c.current().await;
    assert!(!v.groups.is_empty() && !v.delta.is_empty());
    // (1) 1,000 groups and 12 delta runs, each line a copy of a real one with production-like paths and sizes
    let mut big = v.clone();
    let g0 = v.groups.iter().find(|g| g.sealed.is_some()).unwrap().clone();
    big.groups = (0..1000u32)
        .map(|i| {
            let mut g = g0.clone();
            g.id = i;
            g.lo = manifest::hex(&ckey(i as usize, 0));
            g.x_g = 123_456_789_012;
            g.unsealed = 987_654_321;
            if let Some(s) = g.sealed.as_mut() {
                s.path = manifest::run_path("acme-production-database-0001", &format!("g{i}"), 123_456_789_012, Some(42));
                s.size = 1_073_741_824;
                s.raw = 2_147_483_648;
                s.entries = 2_684_354;
                s.min_seq = 1;
                s.max_seq = 123_456_789_012;
            }
            g
        })
        .collect();
    let d0 = v.delta[0].clone();
    big.delta = (0..12)
        .map(|i| {
            let mut d = d0.clone();
            d.path = manifest::run_path("acme-production-database-0001", "delta", 123_456_789_012 - i, Some(42));
            d
        })
        .collect();
    big.next_group_id = 1000;
    let bytes_1000 = serde_json::to_vec(&big).unwrap().len();
    let bytes_now = serde_json::to_vec(&v).unwrap().len();
    // (2) a new waiter's first floor: every version in the store is loaded
    let n_versions = 600u64;
    let newest = manifest::list_versions(&store, id).await.unwrap().last().copied().unwrap();
    let mut fake = v.clone();
    for n in newest + 1..=newest + n_versions {
        fake.version = n;
        store.put(&manifest::manifest_path(id, n), PutPayload::from(serde_json::to_vec(&fake).unwrap())).await.unwrap();
    }
    let count = Arc::new(Count { inner: Arc::clone(&store), gets: AtomicU64::new(0), get_bytes: AtomicU64::new(0), lists: AtomicU64::new(0) });
    let mut k = knobs(true);
    k.serving_lease = Duration::from_millis(10);
    let sh = compactor::Shared::new(id, Arc::new(hold::Holds::default()));
    let ctl = super::drop::DropCtl::new(sh, count.clone() as Arc<dyn ObjectStore>, &k);
    let t0 = Instant::now();
    ctl.floor().await.unwrap();
    let first = t0.elapsed();
    let (g1, b1) = (count.gets.load(Relaxed), count.get_bytes.load(Relaxed));
    ctl.floor().await.unwrap();
    let (g2, l2) = (count.gets.load(Relaxed) - g1, count.lists.load(Relaxed));
    eprintln!("Q6: version now {bytes_now} bytes ({} groups, {} delta runs); with 1,000 groups and 12 delta runs {bytes_1000} bytes; a new waiter's first floor with {} versions in the store: {g1} GETs, {b1} bytes, {first:?} in memory; the next floor: {g2} GETs (lists so far {l2})", v.groups.len(), v.delta.len(), n_versions + 1);
    c.stop().await;
    t.abandon().await;
    teardown();
    // the floor needs only the newest versions (X_d never goes down from one version to the next): a restart must not
    // read every retained version before its first drop job can start (fails today: it reads all of them)
    assert!(g1 <= 10, "a new waiter's first floor read {g1} versions");
}

// ------------------------------------------------------------------------------------------------------------------
// perf-latency round 3, review of d9dbc7c, R1: a paced job outlasting the GC grace

/// A job (a reseal split in several runs, slowed by the CPU share) writes its first output, then runs past the GC
/// grace before its commit lists that output. GC must not delete it meanwhile: every unreferenced run written since
/// the oldest running job started is kept until that job ends. Once the job is over, the same run (never published:
/// a job that failed) goes after the grace as before. `control`: with no job registered the run is deleted after the
/// grace (the test can see a deletion).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r3_gc_keeps_the_output_of_a_running_job_past_the_grace() {
    let _s = SERIAL.lock().await;
    setup();
    for control in [false, true] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = if control { "r3-gc-job-ctl" } else { "r3-gc-job" };
        let mut t = lc(&store).create(id).await.unwrap();
        let mut m = BTreeMap::new();
        for i in 0..200 {
            apply(&mut t, &mut m, vec![(ckey(1, i % 40), Some(vec![b'v'; 50]))]).await;
        }
        publish(&mut t, &small_opts(build::Reseal::All)).await;
        let mut k = knobs(false);
        k.serving_lease = Duration::from_millis(10);
        hold::HOLD_LIFE_MS.store(10, Relaxed);
        k.remote_lease = Duration::from_millis(400);
        k.gc_grace = Duration::from_millis(400);
        k.gc_every = Duration::from_secs(3600); // passes only when the test calls them
        let c = start(&mut t, &store, &k).await;
        let job = if control { None } else { Some(c.begin_job_for_test()) };
        // the job's first output: a run object no version lists yet
        let out = Path::from(manifest::run_path(id, "g0s1", 99_999, Some(1)));
        store.put(&out, PutPayload::from(vec![7u8; 100])).await.unwrap();
        gc::gc_once(&c).await.unwrap(); // first sight
        tokio::time::sleep(Duration::from_millis(900)).await; // the job outlasts the grace
        gc::gc_once(&c).await.unwrap();
        let kept_while_running = store.head(&out).await.is_ok();
        drop(job);
        gc::gc_once(&c).await.unwrap();
        tokio::time::sleep(Duration::from_millis(900)).await;
        gc::gc_once(&c).await.unwrap();
        let gone_after = store.head(&out).await.is_err();
        eprintln!("R1 (control {control}): kept while the job ran {kept_while_running}, deleted after it ended {gone_after}");
        c.stop().await;
        t.abandon().await;
        if control {
            assert!(!kept_while_running, "control: with no running job the run must go after the grace");
        } else {
            assert!(kept_while_running, "GC deleted the output of a running job after the grace");
            assert!(gone_after, "an output nothing publishes must still go once its job is over");
        }
    }
    teardown();
}

/// R2: the store answers the version's PUT late (500 ms) while armed.
#[derive(Debug)]
struct SlowManifestPut {
    inner: Arc<dyn ObjectStore>,
    armed: std::sync::atomic::AtomicBool,
}

impl std::fmt::Display for SlowManifestPut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlowManifestPut")
    }
}

#[async_trait]
impl ObjectStore for SlowManifestPut {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        if self.armed.load(SeqCst) && location.as_ref().contains("/fx/") && location.as_ref().contains("manifest") {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(&self, locations: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// R2 (review of d9dbc7c): a job cut while it waits for its version's PUT (`Compactor::create` runs the PUT in its own
/// task) left that PUT running: the version could land after `stop` returned, so after an erase's listing. `stop` now
/// waits for it: once `stop` returns, the version is in the store (it landed before, never after).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r3_no_version_lands_after_stop() {
    let _s = SERIAL.lock().await;
    setup();
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let slow = Arc::new(SlowManifestPut { inner: Arc::clone(&inner), armed: std::sync::atomic::AtomicBool::new(false) });
    let store: Arc<dyn ObjectStore> = slow.clone();
    let id = "r3-late-put";
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..100 {
        apply(&mut t, &mut m, vec![(ckey(1, i % 20), Some(vec![b'v'; 50]))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let mut k = knobs(false);
    k.lease = Duration::from_secs(5);
    k.put_timeout = Duration::from_secs(2);
    k.remote_lease = Duration::from_secs(6);
    k.gc_grace = Duration::from_secs(6);
    k.gc_every = Duration::from_secs(3600);
    let c = start(&mut t, &store, &k).await;
    let n = c.next_version_for_test().await;
    slow.armed.store(true, SeqCst);
    // the job: waits for its version's PUT and is cut meanwhile (as `stop` aborts its jobs)
    let c2 = Arc::clone(&c);
    let job = tokio::spawn(async move { c2.create_next_for_test().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    job.abort();
    let _ = job.await;
    let t0 = Instant::now();
    c.stop().await;
    let stop_ms = t0.elapsed().as_millis();
    let present_at_stop = inner.head(&manifest::manifest_path(id, n)).await.is_ok();
    eprintln!("R2: stop took {stop_ms} ms; version {n} in the store when stop returned: {present_at_stop}");
    t.abandon().await;
    teardown();
    assert!(present_at_stop, "the cut job's version landed after stop returned");
}

// ------------------------------------------------------------------------------------------------------------------
// perf-latency round 3, item 4: the L0 read of a starting compactor, out of the exclusive lock

/// An absorption records the L0 SSTs it absorbed (with their highest seq) in the version (`absorbed_l0`); a compactor
/// starting on that version takes them from it and reads none of them (before: every L0 SST was read, under the
/// database's exclusive lock, inside the first write after an opening). The fold is off here, so the absorbed SSTs stay
/// in L0 across the restart. Every key reads its model value afterwards, and the second compactor absorbs nothing again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r3_restart_takes_absorbed_l0_from_the_version() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "r3-absorbed-l0";
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..200 {
        apply(&mut t, &mut m, vec![(ckey(1, i % 40), Some(vec![b'v'; 50]))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let k = knobs(false);
    let c1 = start(&mut t, &store, &k).await;
    for round in 0..4 {
        for i in 0..150 {
            apply(&mut t, &mut m, vec![(ckey(2 + round, i % 60), Some(format!("r{round}-{i}").into_bytes()))]).await;
        }
        flush(&t).await;
    }
    c1.drain(false).await.unwrap();
    let l0_absorbed = c1.absorbed_count();
    c1.stop().await;
    t.fx_sync();
    let (from_v, by_read) = (compactor::L0_RECOGNISED_FROM_VERSION.load(SeqCst), compactor::L0_RECOGNISED_BY_READ.load(SeqCst));
    let c2 = start(&mut t, &store, &k).await;
    let (from_v2, by_read2) = (compactor::L0_RECOGNISED_FROM_VERSION.load(SeqCst) - from_v, compactor::L0_RECOGNISED_BY_READ.load(SeqCst) - by_read);
    let absorbs_before = c2.absorbed_count();
    let l0_at_restart = c2.l0_count();
    c2.drain(false).await.unwrap();
    t.fx_sync();
    let mut wrong = 0;
    for (key, want) in &m {
        if t.get(key).await.unwrap().as_deref() != Some(&want[..]) {
            wrong += 1;
        }
    }
    eprintln!("item 4: {l0_absorbed} L0 SSTs absorbed before the restart, {l0_at_restart} in L0 at the restart: {from_v2} taken from the version, {by_read2} read; wrong reads {wrong}");
    c2.stop().await;
    t.abandon().await;
    teardown();
    assert!(l0_absorbed >= 2, "setup: the first compactor must absorb L0 SSTs");
    assert!(l0_at_restart >= 2, "setup: absorbed SSTs must still be in L0 at the restart");
    assert_eq!(from_v2 as usize, l0_at_restart, "every absorbed L0 SST still in L0 must come from the version");
    assert_eq!(by_read2, 0, "no absorbed L0 SST may be read again at the start");
    assert_eq!(absorbs_before, l0_at_restart, "the restarted compactor knows every L0 SST as absorbed");
    assert_eq!(wrong, 0);
}
