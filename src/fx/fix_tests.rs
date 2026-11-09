//! Fix round of compaction 2 step 3: rule 25's fence-pending state (review defect 2) and the manifest PUT timeout of
//! review round 2 of step 0 (F6), each with a control.

use super::build::{BuildOpts, Reseal};
use super::*;
use crate::counting::CountingStore;
use crate::lifecycle::{Lifecycle, Op, Tenant};
use crate::runfmt::{CustomerKind, RunOptions};
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// What the store does with the next conditional creates of a manifest version.
#[derive(Clone, Copy, Debug)]
enum Act {
    /// Lands, then the caller gets an error (outcome unknown).
    LandThenErr,
    /// Does not land; the caller gets an error.
    Err,
    /// Lands at once; the answer comes after this many ms (F6).
    LandAnswerLate(u64),
    /// The answer never comes in time; the PUT lands this many ms later, in the background (a late PUT).
    LandLate(u64),
}

#[derive(Debug)]
struct Store {
    inner: Arc<dyn ObjectStore>,
    acts: Mutex<std::collections::VecDeque<Act>>,
    /// Late PUTs that the store refused because their slot was taken (a fence got there first).
    late_refused: Arc<std::sync::atomic::AtomicU64>,
}

impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FixStore({})", self.inner)
    }
}

fn err(p: &Path) -> object_store::Error {
    object_store::Error::Generic { store: "FixStore", source: format!("request failed: PUT {p}").into() }
}

#[async_trait]
impl ObjectStore for Store {
    async fn put_opts(&self, l: &Path, p: PutPayload, o: PutOptions) -> object_store::Result<PutResult> {
        if matches!(o.mode, PutMode::Create) && l.as_ref().contains("/fx/manifest/") {
            let act = self.acts.lock().unwrap().pop_front();
            match act {
                Some(Act::LandThenErr) => {
                    self.inner.put_opts(l, p, o).await?;
                    return Err(err(l));
                }
                Some(Act::Err) => return Err(err(l)),
                Some(Act::LandAnswerLate(ms)) => {
                    let r = self.inner.put_opts(l, p, o).await;
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    return r;
                }
                Some(Act::LandLate(ms)) => {
                    let (inner, l2, refused) = (Arc::clone(&self.inner), l.clone(), Arc::clone(&self.late_refused));
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(ms)).await;
                        if inner.put_opts(&l2, p, o).await.is_err() {
                            refused.fetch_add(1, Relaxed);
                        }
                    });
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    return Err(err(l));
                }
                None => {}
            }
        }
        self.inner.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(&self, l: &Path, o: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, l: &Path, o: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(l, o).await
    }
    fn delete_stream(&self, s: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(s)
    }
    fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(p)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, o: object_store::CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, o).await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, o: object_store::RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, o).await
    }
}

fn opts(reseal: Reseal) -> BuildOpts {
    BuildOpts { reseal, group_bytes: 16 << 10, sealed: RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2), delta: RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2) }
}

fn key(i: usize) -> Vec<u8> {
    format!("doc/fix/{i:05}").into_bytes()
}

/// A tenant with one published version and newer writes, ready for a second publish.
async fn setup(id: &str) -> (Arc<Store>, Arc<dyn ObjectStore>, Tenant) {
    ENABLED.store(true, Relaxed);
    PUBLISH_EVERY.store(0, Relaxed);
    T_PIN_MS.store(200, Relaxed);
    E1_WAIT_MS.store(0, Relaxed);
    let st = Arc::new(Store { inner: Arc::new(InMemory::new()), acts: Mutex::new(Default::default()), late_refused: Default::default() });
    let store: Arc<dyn ObjectStore> = st.clone();
    let lc = Lifecycle::new(Arc::new(CountingStore::new(Arc::clone(&store))), None);
    let mut t = lc.create(id).await.unwrap();
    for i in 0..60 {
        t.apply(vec![Op::Put(key(i), b"one".to_vec())]).await.unwrap();
    }
    while t.fx_publish(&opts(Reseal::All), None).await.unwrap().is_none() {}
    for i in 0..60 {
        t.apply(vec![Op::Put(key(i), b"two".to_vec())]).await.unwrap();
    }
    (st, store, t)
}

fn restore() {
    E1_WAIT_MS.store(u64::MAX, Relaxed);
    PUT_TIMEOUT_MS.store(20_000, Relaxed);
    CONTROL_NO_FENCE_PENDING.store(false, Relaxed);
}

/// The scenario of review defect 2: a publish lands and errs (outcome unknown), and its fence and the next fences fail.
/// Rule 25: no hold until a fence succeeds. Returns Err when a hold was taken on an older version than the store's
/// newest while no fence was published.
async fn defect2_scenario(control: bool) -> Result<(), String> {
    let (st, store, mut t) = setup(if control { "fx-fix-pending-ctl" } else { "fx-fix-pending" }).await;
    CONTROL_NO_FENCE_PENDING.store(control, Relaxed);
    st.acts.lock().unwrap().extend([Act::LandThenErr, Act::Err, Act::Err, Act::Err, Act::Err, Act::Err, Act::Err, Act::Err, Act::Err, Act::Err]);
    let r = t.fx_publish(&opts(Reseal::None), None).await;
    assert!(r.is_err(), "the publish's PUT failed");
    let id = if control { "fx-fix-pending-ctl" } else { "fx-fix-pending" };
    let newest = manifest::load_newest(&store, id).await.unwrap().unwrap().version;
    let mut out = Ok(());
    // a plain read
    let read = t.get(key(1)).await;
    let known = t.fx_view().unwrap().version;
    if read.is_ok() && known < newest {
        out = Err(format!("a read took a hold on version {known} while version {newest} is in the store and no fence was published"));
    }
    if !control {
        let e = read.as_ref().expect_err("the read must refuse while the fence is pending");
        assert!(e.downcast_ref::<Retry>().is_some(), "{e:#}");
        assert!(t.fx_fence_pending());
        // a range read, a transaction and a publish refuse too (the transaction and the publish retry the fence first)
        assert!(t.scan_range(b"doc/", None, false, None, 4096).await.is_err());
        assert!(t.txn_snapshot().await.is_err());
        assert!(t.fx_publish(&opts(Reseal::None), None).await.is_err());
        // the store answers again: the next exclusive step's refresh publishes the fence, and reads use the newest
        st.acts.lock().unwrap().clear();
        assert!(t.fx_refresh_due());
        t.fx_refresh().await.unwrap();
        assert!(!t.fx_fence_pending());
        let v = t.fx_view().unwrap().clone();
        assert!(v.version > newest && v.meta.kind == "fence", "fenced at {} over the landed {newest}", v.version);
        assert_eq!(t.get(key(1)).await.unwrap().as_deref(), Some(&b"two"[..]));
        assert!(t.txn_snapshot().await.is_ok());
    }
    st.acts.lock().unwrap().clear();
    restore();
    t.abandon().await;
    out
}

/// Review defect 2 with its remedy: no hold while a fence is pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rule25_no_hold_while_a_fence_is_pending() {
    let _s = super::tests::SERIAL.lock().await;
    let r = defect2_scenario(false).await;
    assert_eq!(r, Ok(()));
}

/// The control: without the fence-pending state the same scenario takes a hold on the old version (the test above
/// would fail).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rule25_control_without_the_pending_state_breaks() {
    let _s = super::tests::SERIAL.lock().await;
    let r = defect2_scenario(true).await;
    assert!(r.is_err(), "the control must show the broken state");
}

/// F6: the publish's PUT lands at once, and its answer arrives after the PUT timeout. The publish counts as timed out,
/// the fence of rule 25 follows, and the next hold reads with the fence version, newer than the landed one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f6_a_put_whose_answer_is_late_is_a_timeout_and_the_next_hold_fences_first() {
    let _s = super::tests::SERIAL.lock().await;
    let id = "fx-fix-f6";
    let (st, store, mut t) = setup(id).await;
    PUT_TIMEOUT_MS.store(200, Relaxed);
    // the publish's answer is late; the fence written right after it is late too, so the fence stays pending
    st.acts.lock().unwrap().extend([Act::LandAnswerLate(1_000), Act::LandAnswerLate(1_000)]);
    let t0 = std::time::Instant::now();
    let e = t.fx_publish(&opts(Reseal::None), None).await.unwrap_err();
    assert!(format!("{e:#}").contains("timed out"), "{e:#}");
    assert!(t0.elapsed() < Duration::from_millis(900), "the publish waited {:?}, past the timeout", t0.elapsed());
    let landed = manifest::load_newest(&store, id).await.unwrap().unwrap();
    // the publish's PUT and the timed-out fence both landed; the waiter knows neither
    assert!(t.fx_view().unwrap().version < landed.version);
    assert!(t.fx_fence_pending());
    // the next hold fences first: a read refuses until a fence succeeds
    assert!(t.get(key(1)).await.unwrap_err().downcast_ref::<Retry>().is_some());
    t.fx_refresh().await.unwrap();
    let v = t.fx_view().unwrap().clone();
    assert!(v.version > landed.version && v.meta.kind == "fence");
    assert_eq!(manifest::load_newest(&store, id).await.unwrap().unwrap().version, v.version);
    assert_eq!(t.get(key(7)).await.unwrap().as_deref(), Some(&b"two"[..]));
    st.acts.lock().unwrap().clear();
    restore();
    t.abandon().await;
}

/// F6, the late PUT: the publish times out before its PUT lands; the fence takes the next slot first, so the late PUT
/// is refused when it arrives and the waiter reads with the fence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f6_a_late_put_finds_its_slot_taken_by_the_fence() {
    let _s = super::tests::SERIAL.lock().await;
    let id = "fx-fix-f6-late";
    let (st, store, mut t) = setup(id).await;
    PUT_TIMEOUT_MS.store(200, Relaxed);
    let before = t.fx_view().unwrap().version;
    st.acts.lock().unwrap().extend([Act::LandLate(600)]);
    assert!(t.fx_publish(&opts(Reseal::None), None).await.is_err());
    // the fence written right after the timeout took the publish's slot
    assert!(!t.fx_fence_pending());
    let v = t.fx_view().unwrap().clone();
    assert_eq!(v.version, before + 1);
    assert_eq!(v.meta.kind, "fence");
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert_eq!(st.late_refused.load(Relaxed), 1, "the late PUT must find its slot taken");
    let newest = manifest::load_newest(&store, id).await.unwrap().unwrap();
    assert_eq!((newest.version, newest.kind.as_str()), (before + 1, "fence"));
    assert_eq!(t.get(key(3)).await.unwrap().as_deref(), Some(&b"two"[..]));
    restore();
    t.abandon().await;
}

/// F6: the startup check refuses timings that break the drop floor's assumptions.
#[tokio::test]
async fn f6_config_check_refuses_bad_timings() {
    let _s = super::tests::SERIAL.lock().await;
    let saved = [PUT_TIMEOUT_MS.load(Relaxed), LEASE_MS.load(Relaxed), LEASE_MARGIN_MS.load(Relaxed), REMOTE_LEASE_MS.load(Relaxed), GC_GRACE_MS.load(Relaxed), hold::HOLD_LIFE_MS.load(Relaxed), T_PIN_MS.load(Relaxed)];
    let set = |p: u64, l: u64, m: u64, r: u64, g: u64, h: u64, tp: u64| {
        PUT_TIMEOUT_MS.store(p, Relaxed);
        LEASE_MS.store(l, Relaxed);
        LEASE_MARGIN_MS.store(m, Relaxed);
        REMOTE_LEASE_MS.store(r, Relaxed);
        GC_GRACE_MS.store(g, Relaxed);
        hold::HOLD_LIFE_MS.store(h, Relaxed);
        T_PIN_MS.store(tp, Relaxed);
    };
    set(20_000, 60_000, 30_000, 180_000, 600_000, 120_000, 30_000);
    let ok = check_config();
    set(30_000, 60_000, 30_000, 180_000, 600_000, 120_000, 30_000);
    let put_too_long = check_config();
    set(20_000, 60_000, 30_000, 140_000, 600_000, 120_000, 30_000);
    let remote_too_short = check_config();
    set(20_000, 60_000, 30_000, 180_000, 140_000, 120_000, 30_000);
    let grace_too_short = check_config();
    set(20_000, 60_000, 30_000, 180_000, 600_000, 120_000, 60_000);
    let lease_not_above_tpin = check_config();
    set(saved[0], saved[1], saved[2], saved[3], saved[4], saved[5], saved[6]);
    assert!(ok.is_ok(), "{ok:?}");
    assert!(put_too_long.is_err() && remote_too_short.is_err() && grace_too_short.is_err() && lease_not_above_tpin.is_err());
}
