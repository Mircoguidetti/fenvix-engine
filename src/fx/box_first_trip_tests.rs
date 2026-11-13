//! fix-box-first-trip (fenvix-brain `prompts/P-fix-box-first-trip.md`): a customer's first request takes one trip to the
//! object store in sealed mode too.
//!
//! The cause, MEASURED on test_79 (sealed2-derive, T_pin 1 s): a reader's state of our manifest is renewed at half T_pin,
//! in front of the request that finds it due: one LIST, the newest version, the version below it (three trips in a row),
//! then the customer's box (one trip). These tests pin the three parts of the fix, each with its control:
//!
//! - `bft_unchanged_renewal_sends_no_get`: a renewal whose LIST finds the version already served sends no GET
//!   (control: `load_newest_trusted`, the path before, sends GETs; and after a publish the renewal reads the new version);
//! - `bft_step_does_not_wait_for_a_renewal_in_progress`: while a renewal runs (`Tenant::renew_alongside`, as the box
//!   starts it), a step whose state is still valid reads at once and right (control: the same step alone renews in front
//!   of its read and waits for the LIST);
//! - `bft_step_near_expiry_waits_for_the_renewal`: past 3/4 of T_pin a step waits for the renewal in progress instead
//!   (the safety side: it never reads with a state that could pass T_pin);
//! - `bft_reader_refresh_due_only_when_expired`: the exclusive refresh in front of a command (`fx_refresh_due`) is due
//!   for a reader only once its state is older than T_pin, no longer at half T_pin.

use super::review_long_reads_tests::{faulty_lc, setup, stable_db, teardown, T_PIN};
use super::tests::*;
use super::*;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bft_unchanged_renewal_sends_no_get() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-bft-unchanged";
    let (mut w, want) = stable_db(&store, id, ckey, &small_opts(build::Reseal::All)).await;
    let (rlc, f) = faulty_lc(&store);
    let mut r = rlc.wake(id).await.unwrap();
    r.fx_own_holds();
    r.fx_refresh().await.unwrap();
    // control: the path before (a LIST, the newest version, the version below it)
    let (l0, g0) = (f.lists.load(Relaxed), f.manifest_gets.load(Relaxed));
    manifest::load_newest_trusted(&rlc.store, id).await.unwrap().unwrap();
    let (old_lists, old_gets) = (f.lists.load(Relaxed) - l0, f.manifest_gets.load(Relaxed) - g0);
    // a step past half T_pin renews, and nothing was published since
    tokio::time::sleep(Duration::from_millis(T_PIN / 2 + 20)).await;
    let (l0, g0, n0, u0) = (f.lists.load(Relaxed), f.manifest_gets.load(Relaxed), RENEWALS.load(Relaxed), manifest::RENEWALS_UNCHANGED.load(Relaxed));
    let k = ckey(1, 7);
    assert_eq!(r.get(&k).await.unwrap().as_deref(), want.get(&k).map(|v| &v[..]));
    let (lists, gets, renewals, unchanged) = (f.lists.load(Relaxed) - l0, f.manifest_gets.load(Relaxed) - g0, RENEWALS.load(Relaxed) - n0, manifest::RENEWALS_UNCHANGED.load(Relaxed) - u0);
    eprintln!("bft unchanged: before {old_lists} LIST + {old_gets} manifest GETs; now {renewals} renewal, {lists} LIST, {gets} manifest GETs ({unchanged} unchanged)");
    // (one GET here: this database has version 1 only, so there is no version below it to read; test_79's has two)
    assert!(old_gets >= 1 && old_lists == 1, "control: the path before reads the newest version ({old_lists} LIST, {old_gets} GETs)");
    assert_eq!((renewals, lists, gets, unchanged), (1, 1, 0, 1), "an unchanged renewal is one LIST and no GET");
    // after a publish the renewal reads the new version, and the reader sees the new row
    let mut m = BTreeMap::new();
    let k2 = ckey(2, 9001);
    apply(&mut w, &mut m, vec![(k2.clone(), Some(b"after-publish".to_vec()))]).await;
    publish(&mut w, &small_opts(build::Reseal::None)).await;
    tokio::time::sleep(Duration::from_millis(T_PIN / 2 + 20)).await;
    let g0 = f.manifest_gets.load(Relaxed);
    assert_eq!(r.get(&k2).await.unwrap().as_deref(), Some(&b"after-publish"[..]));
    assert!(f.manifest_gets.load(Relaxed) - g0 >= 1, "a new version is read with GETs");
    // fix-pass-stop-bound: a writer left registered (`WRITER_DBS`) after its test ends was compacted and nudged by the next
    // test's maintenance, whose shutdown then hung on it (the unit-suite hang); end it as a lease loss does
    w.abandon().await;
    teardown();
}

/// A reader of a stable database whose LISTs of the manifest directory answer `delay` ms late.
/// Also returns the writer, which stays open for the test (no close in the middle of it); fix-pass-stop-bound: the test
/// ends it (`abandon`, as a lease loss does) instead of leaving it registered (`WRITER_DBS`) with its runtime gone, where
/// the next test's maintenance compacted and nudged it, and its shutdown hung (the unit-suite hang).
async fn slow_list_reader(id: &str) -> (crate::lifecycle::Tenant, Arc<review_long_reads_tests::Faults>, BTreeMap<Vec<u8>, Vec<u8>>, crate::lifecycle::Tenant) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (w, want) = stable_db(&store, id, ckey, &small_opts(build::Reseal::All)).await;
    let (rlc, f) = faulty_lc(&store);
    let mut r = rlc.wake(id).await.unwrap();
    r.fx_own_holds();
    r.fx_refresh().await.unwrap();
    (r, f, want, w)
}

const LIST_DELAY_MS: u64 = 120;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bft_step_does_not_wait_for_a_renewal_in_progress() {
    let _s = SERIAL.lock().await;
    setup();
    let (r, f, want, w) = slow_list_reader("fx-bft-alongside").await;
    f.delay_list_ms.store(LIST_DELAY_MS, Relaxed);
    let k = ckey(3, 11);
    // control: past half T_pin, a step alone renews in front of its read and waits for the slow LIST
    tokio::time::sleep(Duration::from_millis(T_PIN / 2 + 10)).await;
    let t0 = Instant::now();
    assert_eq!(r.get(&k).await.unwrap().as_deref(), want.get(&k).map(|v| &v[..]));
    let alone = t0.elapsed();
    // the fix: the renewal runs beside the step (as the customer's box starts it); the step reads at once. The state the
    // control renewed is as old as that renewal (its clock starts when the renewal starts): past half T_pin again here
    tokio::time::sleep(Duration::from_millis(T_PIN / 2 + 10).saturating_sub(alone)).await;
    let (n0, p0, w0) = (RENEWALS.load(Relaxed), STEPS_PAST_RENEWAL.load(Relaxed), RENEWALS_WITH_BOX.load(Relaxed));
    let t0 = Instant::now();
    let renewal = r.renew_alongside();
    let step = async {
        let v = r.get(&k).await.unwrap();
        (v, t0.elapsed())
    };
    let ((), (v, beside)) = futures::future::join(renewal, step).await;
    let total = t0.elapsed();
    assert_eq!(v.as_deref(), want.get(&k).map(|v| &v[..]));
    let (renewals, past, with_box) = (RENEWALS.load(Relaxed) - n0, STEPS_PAST_RENEWAL.load(Relaxed) - p0, RENEWALS_WITH_BOX.load(Relaxed) - w0);
    eprintln!("bft alongside: step alone {alone:?} (renews in front); beside the renewal {beside:?}, renewal done after {total:?}; renewals {renewals}, steps past it {past}, with box {with_box}");
    assert!(alone >= Duration::from_millis(LIST_DELAY_MS), "control: a step alone waits for its renewal's LIST ({alone:?})");
    assert!(beside < Duration::from_millis(LIST_DELAY_MS / 2), "a step beside a renewal in progress must not wait for it ({beside:?})");
    assert!(total >= Duration::from_millis(LIST_DELAY_MS), "the renewal did run ({total:?})");
    assert_eq!((renewals, past, with_box), (1, 1, 1));
    w.abandon().await;
    teardown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bft_step_near_expiry_waits_for_the_renewal() {
    let _s = SERIAL.lock().await;
    setup();
    let (r, f, want, w) = slow_list_reader("fx-bft-near-expiry").await;
    let k = ckey(4, 5);
    // a state past 3/4 of T_pin: `renew_alongside` leaves it to the steps, and a step renews (and waits) as before
    tokio::time::sleep(Duration::from_millis(T_PIN * 3 / 4 + 20)).await;
    f.delay_list_ms.store(LIST_DELAY_MS, Relaxed);
    let (n0, w0, p0) = (RENEWALS.load(Relaxed), RENEWALS_WITH_BOX.load(Relaxed), STEPS_PAST_RENEWAL.load(Relaxed));
    let t0 = Instant::now();
    let ((), v) = futures::future::join(r.renew_alongside(), r.get(&k)).await;
    let took = t0.elapsed();
    assert_eq!(v.unwrap().as_deref(), want.get(&k).map(|v| &v[..]));
    let (renewals, with_box, past) = (RENEWALS.load(Relaxed) - n0, RENEWALS_WITH_BOX.load(Relaxed) - w0, STEPS_PAST_RENEWAL.load(Relaxed) - p0);
    eprintln!("bft near expiry: step took {took:?}; renewals {renewals}, with box {with_box}, steps past a renewal {past}");
    assert_eq!((renewals, with_box, past), (1, 0, 0), "near expiry the step renews itself");
    assert!(took >= Duration::from_millis(LIST_DELAY_MS), "near expiry the step waits for the renewal ({took:?})");
    // and a renewal in progress near expiry is waited for: start one, then a step
    tokio::time::sleep(Duration::from_millis(T_PIN * 3 / 4 + 20)).await;
    let t0 = Instant::now();
    let k2 = ckey(5, 6);
    let r = &r;
    let renewal = async move {
        // a step older than half T_pin takes the renewal lock (as another command's step would)
        let _ = r.get(ckey(6, 1)).await;
    };
    let k2c = k2.clone();
    let step = async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let v = r.get(&k2c).await.unwrap();
        (v, t0.elapsed())
    };
    let ((), (v, waited)) = futures::future::join(renewal, step).await;
    assert_eq!(v.as_deref(), want.get(&k2).map(|v| &v[..]));
    eprintln!("bft near expiry: second step waited {waited:?} for the renewal in progress");
    assert!(waited >= Duration::from_millis(LIST_DELAY_MS), "near expiry a step waits for the renewal in progress ({waited:?})");
    w.abandon().await;
    teardown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bft_reader_refresh_due_only_when_expired() {
    let _s = SERIAL.lock().await;
    setup();
    let (mut r, _f, _want, w) = slow_list_reader("fx-bft-due").await;
    assert!(!r.fx_refresh_due(), "fresh");
    tokio::time::sleep(Duration::from_millis(T_PIN / 2 + 20)).await;
    assert!(!r.fx_refresh_due(), "past half T_pin a reader is renewed by its steps, not refreshed in front of a command");
    tokio::time::sleep(Duration::from_millis(T_PIN / 2 + 20)).await;
    assert!(r.fx_refresh_due(), "past T_pin the exclusive refresh is due");
    r.fx_refresh().await.unwrap();
    assert!(!r.fx_refresh_due());
    w.abandon().await;
    teardown();
}
