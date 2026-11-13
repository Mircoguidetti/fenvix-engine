//! audit-fencing: late writes of a process that lost its database to another one (P-audit-fencing, 2026-11-13).
//!
//! Each test lands one write of the old owner AFTER the takeover, deterministically (a held build, a held PUT), and
//! checks what readers see afterwards. The "control" of each test runs the code of integration-4 4f74574 (a switch,
//! `CONTROL_*` in `manifest.rs`) and must show the damage, or the test does not reach its case.
//!
//! - `fence_first_build_lands_after_takeover`: a first build of the old owner that ends after the new owner published
//!   (the a60f4fe case on integration-4 itself): its version number was taken from a LIST, so it lands ON TOP of the new
//!   owner's versions; the new owner's compactor and every later opening build on it. Acknowledged writes vanish.
//! - `fence_scrub_lands_in_a_slot_gc_freed`: an erasure scrub of the old owner whose create is in flight across the
//!   takeover; the new owner's fence took its slot, then GC deleted that fence (retention 0), so the late create finds
//!   the slot free and lands below the newest version. The old owner records the erasure as scrubbed; the retire step
//!   (later, by the new owner) deletes the runs the scrub replaced, which the newest version still lists. Reads fail.

use super::compactor::{Compactor, Knobs};
use super::erase::{self, Batch, Targets};
use super::tests::*;
use super::*;
use crate::lifecycle::{Op, Tenant};
use crate::runfmt::{CustomerKind, RunOptions};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------------------------------------------------
// A store that holds the PUTs of matching paths (a slow network or a slow S3 answer for ONE process).

#[derive(Debug)]
struct Gate {
    inner: Arc<dyn ObjectStore>,
    pattern: Mutex<Option<String>>,
    open: tokio::sync::Notify,
    waiting: std::sync::atomic::AtomicU64,
}

impl Gate {
    fn new(inner: Arc<dyn ObjectStore>) -> Arc<Gate> {
        Arc::new(Gate { inner, pattern: Mutex::new(None), open: tokio::sync::Notify::new(), waiting: Default::default() })
    }
    fn arm(&self, p: &str) {
        *self.pattern.lock().unwrap() = Some(p.to_string());
    }
    fn release(&self) {
        *self.pattern.lock().unwrap() = None;
        self.open.notify_waiters();
    }
    async fn hold(&self, l: &Path) {
        loop {
            let w = self.open.notified();
            let held = self.pattern.lock().unwrap().as_ref().is_some_and(|p| l.as_ref().contains(p.as_str()));
            if !held {
                return;
            }
            self.waiting.fetch_add(1, SeqCst);
            w.await;
            self.waiting.fetch_sub(1, SeqCst);
        }
    }
}

impl std::fmt::Display for Gate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Gate({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Gate {
    async fn put_opts(&self, l: &Path, p: object_store::PutPayload, o: object_store::PutOptions) -> object_store::Result<object_store::PutResult> {
        self.hold(l).await;
        self.inner.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(&self, l: &Path, o: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, l: &Path, o: object_store::GetOptions) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(l, o).await
    }
    fn delete_stream(&self, l: futures::stream::BoxStream<'static, object_store::Result<Path>>) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(l)
    }
    fn list(&self, p: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(p)
    }
    fn list_with_offset(&self, p: Option<&Path>, o: &Path) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list_with_offset(p, o)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, f: &Path, t: &Path, o: object_store::CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(f, t, o).await
    }
    async fn rename_opts(&self, f: &Path, t: &Path, o: object_store::RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(f, t, o).await
    }
}

// ---------------------------------------------------------------------------------------------------------------------

const HOLD_MS: u64 = 3000;

fn knobs(put_timeout: Duration) -> Knobs {
    let lease = (put_timeout * 2).max(Duration::from_millis(400));
    Knobs {
        theta: 1e9,
        r3: false,
        r3_gets_per_gib: 470.0,
        r2_age: None,
        tier_t: 64,
        tier_unit: 2 << 10,
        absorb_min_l0: 1,
        absorb_max_l0: 64,
        recognise_in_background: false,
        local_max_bytes: 0,
        split_bytes: 1 << 30,
        merge_bytes: 0,
        min_seal_bytes: 0,
        sealed: RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2),
        delta: RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2),
        drop: false,
        fold_one_run: false,
        fold_max_runs: 6,
        fold_at_l0: 6,
        lease,
        remote_lease: Duration::from_millis(HOLD_MS) + put_timeout + Duration::from_millis(600),
        serving_lease: Duration::from_millis(300),
        put_timeout,
        gc_grace: Duration::from_millis(HOLD_MS) + put_timeout + Duration::from_millis(800),
        version_retention: Duration::from_millis(0),
        gc_every: Duration::from_secs(3600),
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
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(64, Relaxed);
    view::clear_cache();
}

fn teardown() {
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(0, Relaxed);
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
    build::PAUSE_AFTER_SNAPSHOT_MS.store(0, Relaxed);
    build::PAUSE_BEFORE_CREATE_MS.store(0, Relaxed);
    manifest::CONTROL_LISTED_SLOT.store(false, Relaxed);
    manifest::CONTROL_NO_MAIN_LINE.store(false, Relaxed);
}

async fn flush(t: &Tenant) {
    let db = t.fx_writer_db().unwrap();
    db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
}

/// Reads of every acknowledged key that do not return its value (an error counts as wrong).
async fn wrong_reads(t: &Tenant, m: &BTreeMap<Vec<u8>, Vec<u8>>) -> (usize, usize) {
    let (mut wrong, mut errors) = (0, 0);
    for (k, want) in m {
        match t.get(k).await {
            Ok(v) if v.as_deref() == Some(&want[..]) => {}
            Ok(_) => wrong += 1,
            Err(_) => errors += 1,
        }
    }
    (wrong, errors)
}

// ---------------------------------------------------------------------------------------------------------------------
// 1. The first build of the old owner lands on top after a takeover.

/// Returns (versions of the old writer's epoch above a version of the new one, wrong reads, read errors, acknowledged
/// keys) after a fresh opening.
async fn first_build_scenario(control: bool, gc_frees_slot_1: bool) -> (usize, usize, usize, usize) {
    manifest::CONTROL_LISTED_SLOT.store(control, Relaxed);
    manifest::CONTROL_NO_MAIN_LINE.store(control, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = format!("fn-first-{control}-{gc_frees_slot_1}");
    let id = id.as_str();
    // the old owner (writer epoch 1): writes, then its first build starts off the lock and is slow after its cut
    let mut t1 = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..60 {
        apply(&mut t1, &mut m, vec![(ckey(0, i), Some(format!("old-{i}").into_bytes()))]).await;
    }
    build::PAUSE_AFTER_SNAPSHOT_MS.store(2500, Relaxed);
    let mut started = false;
    for _ in 0..100 {
        if t1.fx_first_start(&small_opts(build::Reseal::All)).await.unwrap() {
            started = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(TEST_T_PIN_MS / 4)).await;
    }
    assert!(started, "the first build did not start");
    tokio::time::sleep(Duration::from_millis(200)).await; // it reaches its pause
    build::PAUSE_AFTER_SNAPSHOT_MS.store(0, Relaxed);
    let t_takeover = Instant::now();
    // the takeover: another process opens the database as writer (epoch 2), publishes, runs its compactor
    let mut t2 = lc(&store).wake(id).await.unwrap();
    t2.upgrade().await.unwrap();
    for r in 0..2 {
        for i in 0..80 {
            apply(&mut t2, &mut m, vec![(ckey(1 + r, i), Some(format!("new-{r}-{i}").into_bytes()))]).await;
        }
    }
    publish(&mut t2, &small_opts(build::Reseal::All)).await;
    let c2 = Compactor::start(&mut t2, Arc::clone(&store), knobs(Duration::from_millis(250)), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    for r in 2..5 {
        for i in 0..80 {
            apply(&mut t2, &mut m, vec![(ckey(1 + r, i), Some(format!("new-{r}-{i}").into_bytes()))]).await;
        }
        flush(&t2).await;
        c2.drain(false).await.unwrap();
    }
    t2.fx_sync();
    if gc_frees_slot_1 {
        // retention 0: GC deletes every version but the newest (nothing holds them)
        gc::gc_once(&c2).await.unwrap();
    }
    let before: Vec<u64> = manifest::list_versions(&store, id).await.unwrap();
    // the old owner's build ends now, after the takeover
    let r = t1.fx_first_poll(true).await;
    let landed_ms = t_takeover.elapsed().as_millis();
    // the new owner goes on writing; its compactor commits
    for r in 5..8 {
        for i in 0..80 {
            apply(&mut t2, &mut m, vec![(ckey(1 + r, i), Some(format!("new-{r}-{i}").into_bytes()))]).await;
        }
        flush(&t2).await;
        let _ = c2.drain(false).await;
        let _ = c2.drain(false).await;
    }
    c2.stop().await;
    t2.close().await.unwrap();
    let _ = t1.abandon().await;
    // versions of the old writer (epoch 1) numbered above a version of a newer one
    let mut stale_above = 0;
    let mut max_new = 0;
    for n in manifest::list_versions(&store, id).await.unwrap() {
        let v = manifest::load(&store, id, n).await.unwrap();
        if v.writer_epoch >= Some(2) {
            max_new = max_new.max(n);
        } else if max_new > 0 {
            stale_above += 1;
        }
    }
    manifest::CONTROL_LISTED_SLOT.store(false, Relaxed);
    manifest::CONTROL_NO_MAIN_LINE.store(false, Relaxed);
    // a fresh opening of the database reads every acknowledged key
    let mut t3 = lc(&store).wake(id).await.unwrap();
    t3.upgrade().await.unwrap();
    let (wrong, errors) = wrong_reads(&t3, &m).await;
    let newest = manifest::load_newest(&store, id).await.unwrap().unwrap();
    eprintln!(
        "first build after a takeover (control {control}, gc frees slot 1 {gc_frees_slot_1}): versions before its create {before:?}; its poll {:?} {landed_ms} ms after the takeover; \
         newest version {} epoch {:?} x_d {}; wrong {wrong}, errors {errors} of {} acknowledged keys",
        r.as_ref().map(|x| x.as_ref().map(|r| r.version)).map_err(|e| format!("{e:#}")),
        newest.version,
        newest.writer_epoch,
        newest.x_d,
        m.len()
    );
    let _ = t3.close().await;
    (stale_above, wrong, errors, m.len())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fence_first_build_lands_after_takeover() {
    let _s = SERIAL.lock().await;
    setup();
    let (_, wrong_ctl, err_ctl, _) = first_build_scenario(true, false).await;
    let (_, wrong, errors, n) = first_build_scenario(false, false).await;
    let (_, wrong_gc, errors_gc, _) = first_build_scenario(false, true).await;
    teardown();
    assert!(wrong_ctl + err_ctl > 0, "control (integration-4): the late first build must lose acknowledged writes, or the test does not reach the case");
    assert_eq!((wrong, errors), (0, 0), "acknowledged writes lost ({n} keys)");
    assert_eq!((wrong_gc, errors_gc), (0, 0), "acknowledged writes lost when GC freed slot 1 ({n} keys)");
}

// ---------------------------------------------------------------------------------------------------------------------
// 2. An erasure scrub of the old owner lands in a slot GC freed; the retire step then deletes live runs.

fn cust_range(c: usize) -> (Vec<u8>, Vec<u8>) {
    let k = ckey(c, 0);
    let lo = k[..k.len() - "msgs/d/00000".len()].to_vec();
    let mut hi = lo.clone();
    *hi.last_mut().unwrap() += 1;
    (lo, hi)
}

fn targets(c: usize) -> Targets {
    let mut t = Targets::default();
    t.ranges.push(cust_range(c));
    t
}

async fn flush_absorb(t: &mut Tenant, c: &Compactor) {
    flush(t).await;
    while c.absorb_once().await.unwrap() > 0 {}
    t.fx_sync();
}

/// Returns (the old owner's scrub reported published, runs the retire deleted that the newest version lists, wrong
/// reads, read errors, erased rows readable) after a fresh opening.
async fn scrub_scenario(control: bool) -> (bool, usize, usize, usize, usize) {
    manifest::CONTROL_LISTED_SLOT.store(control, Relaxed);
    manifest::CONTROL_NO_MAIN_LINE.store(control, Relaxed);
    let base_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let gate = Gate::new(Arc::clone(&base_store));
    let s1: Arc<dyn ObjectStore> = gate.clone();
    let id = format!("fn-scrub-{control}");
    let id = id.as_str();
    // the old owner: customers 0..40, a first version of several groups, its compactor (paused: the test runs its jobs);
    // its PUT timeout is long, so a held create still answers within it
    let mut t1 = lc(&s1).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    let mut ops = Vec::new();
    for c in 0..40 {
        for i in 0..20 {
            ops.push(Op::Put(ckey(c, i), format!("v-{c}-{i}").into_bytes()));
            m.insert(ckey(c, i), format!("v-{c}-{i}").into_bytes());
        }
    }
    t1.apply(ops).await.unwrap();
    publish(&mut t1, &small_opts(build::Reseal::All)).await;
    let c1 = Compactor::start(&mut t1, Arc::clone(&s1), knobs(Duration::from_secs(8)), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    c1.paused.store(true, Relaxed);
    assert!(t1.fx_view().unwrap().groups.len() >= 2, "the test needs several groups");
    // erase customer 0
    t1.apply((0..20).map(|i| Op::Delete(ckey(0, i))).collect()).await.unwrap();
    for i in 0..20 {
        m.remove(&ckey(0, i));
    }
    let mut e = t1.fx_erasure_register("msgs", b"c00", &targets(0), None).await.unwrap().unwrap();
    flush_absorb(&mut t1, &c1).await;
    let base = c1.current().await;
    assert!(base.x_d >= e.s_e);
    let tg = targets(0);
    let batch = Batch::single(&tg, e.s_e);
    let v = view::View::new(Arc::clone(&s1), base.clone(), None).unwrap();
    let s = erase::scrub_version(&s1, id, &v, base.version + 1, base.writer_epoch, &batch, &erase::ScrubOpts::default()).await.unwrap();
    assert!(s.rows > 0 && !s.retired.is_empty(), "the scrub erased nothing");
    // its create is in flight (held on the way to the store) across the takeover
    gate.arm("/fx/manifest/");
    let c1b = Arc::clone(&c1);
    let (sb, bb, mx) = (s.clone(), base.clone(), batch.max_s_e);
    let commit = tokio::spawn(async move { c1b.commit_scrub(&bb, &sb, mx).await });
    for _ in 0..200 {
        if gate.waiting.load(SeqCst) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(gate.waiting.load(SeqCst) > 0, "the scrub's create did not reach the store");
    let t_takeover = Instant::now();
    // the takeover: the new owner opens (its fence takes the next slot), absorbs and publishes, GC deletes the fence
    let mut t2 = lc(&base_store).wake(id).await.unwrap();
    t2.upgrade().await.unwrap();
    let fence = t2.fx_view().unwrap().version;
    assert_eq!(fence, base.version + 1, "setup: the new owner's fence takes the slot the old owner's scrub targets");
    let c2 = Compactor::start(&mut t2, Arc::clone(&base_store), knobs(Duration::from_millis(250)), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    c2.paused.store(true, Relaxed);
    for i in 0..20 {
        t2.apply(vec![Op::Put(ckey(50, i), format!("n-{i}").into_bytes())]).await.unwrap();
        m.insert(ckey(50, i), format!("n-{i}").into_bytes());
    }
    flush_absorb(&mut t2, &c2).await;
    let newest2 = c2.current().await;
    assert!(newest2.version > fence, "setup: the new owner published after its fence");
    gc::gc_once(&c2).await.unwrap();
    let freed = manifest::load(&base_store, id, fence).await.is_err();
    // the old owner's create lands now
    gate.release();
    let published = commit.await.unwrap();
    let landed_ms = t_takeover.elapsed().as_millis();
    let published_v = match &published {
        Ok(Some(v)) => Some(v.clone()),
        _ => None,
    };
    if let Some(v) = &published_v {
        // the old owner's erasure batch goes on as `ctl/erase.rs` does: the record, then the retire step
        erase::record_scrub(&base_store, id, &mut e, &s, v.version, &tg, v).await.unwrap();
    }
    let _ = c1.stop().await;
    let _ = t1.abandon().await;
    // later, the new owner's erasure pass retires what the record says was scrubbed (the grace has passed)
    let newest = manifest::load_newest(&base_store, id).await.unwrap().unwrap();
    let listed: HashSet<String> = newest.groups.iter().filter_map(|g| g.sealed.as_ref().map(|r| r.path.clone())).chain(newest.delta.iter().map(|d| d.path.clone())).collect();
    let mut recs: Vec<erase::Erasure> = erase::list(&base_store, id).await.unwrap();
    {
        let mut open: Vec<&mut erase::Erasure> = recs.iter_mut().filter(|e| e.done_ms.is_none() && e.scrubbed_version.is_some()).collect();
        erase::retire_all(&base_store, id, &mut open, newest.version, newest.version, 0).await.unwrap();
    }
    let deleted_live: usize = recs.iter().flat_map(|e| e.retired.iter()).filter(|r| r.deleted_ms.is_some() && listed.contains(&r.path)).count();
    c2.stop().await;
    t2.close().await.unwrap();
    manifest::CONTROL_LISTED_SLOT.store(false, Relaxed);
    manifest::CONTROL_NO_MAIN_LINE.store(false, Relaxed);
    view::clear_cache();
    let mut t3 = lc(&base_store).wake(id).await.unwrap();
    t3.upgrade().await.unwrap();
    let (wrong, errors) = wrong_reads(&t3, &m).await;
    let mut erased_readable = 0;
    for i in 0..20 {
        if matches!(t3.get(ckey(0, i)).await, Ok(Some(_))) {
            erased_readable += 1;
        }
    }
    let _ = t3.close().await;
    eprintln!(
        "scrub across a takeover (control {control}): fence {fence} freed by GC {freed}; the old owner's commit {:?} {landed_ms} ms after the takeover; \
         newest {} (epoch {:?}); retired runs deleted while the newest lists them {deleted_live}; wrong {wrong}, errors {errors} of {}; erased rows readable {erased_readable}",
        published.as_ref().map(|x| x.as_ref().map(|v| v.version)).map_err(|e| format!("{e:#}")),
        newest.version,
        newest.writer_epoch,
        m.len()
    );
    (published_v.is_some(), deleted_live, wrong, errors, erased_readable)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fence_scrub_lands_in_a_slot_gc_freed() {
    let _s = SERIAL.lock().await;
    setup();
    let (pub_ctl, del_ctl, wrong_ctl, err_ctl, _) = scrub_scenario(true).await;
    let (published, deleted_live, wrong, errors, erased_readable) = scrub_scenario(false).await;
    teardown();
    assert!(pub_ctl && del_ctl > 0 && wrong_ctl + err_ctl > 0, "control (integration-4): the late scrub must land and its retire delete live runs, or the test does not reach the case");
    assert!(!published, "the old owner's scrub, landed below the newest version, was taken as published");
    assert_eq!(deleted_live, 0, "the retire deleted runs the newest version lists");
    assert_eq!((wrong, errors), (0, 0), "acknowledged writes lost");
    assert_eq!(erased_readable, 0, "erased rows readable");
}
