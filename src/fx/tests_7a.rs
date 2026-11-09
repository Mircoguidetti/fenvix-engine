//! Tests of compaction 2, step 7a: release E1 behind its announcement (`--envelopes write`, rules 6 and 26), the X_d
//! check of a reloaded version at open (step 3 review note), and the holds of one operation across several reads
//! (rules 4, 5, 7 and 8; step 3 review note on `$lookup`). Export and PITR are tested end to end in
//! `ctl-tests/test_47_c2_step7a_paths.py`.

use super::build::{BuildOpts, Reseal};
use super::*;
use crate::counting::CountingStore;
use crate::lifecycle::{Lifecycle, Op, Tenant};
use crate::runfmt::{CustomerKind, RunOptions};
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::Arc;
use std::time::Duration;

const T_PIN: u64 = 300;

fn lc(store: &Arc<dyn ObjectStore>) -> Arc<Lifecycle> {
    Lifecycle::new(Arc::new(CountingStore::new(Arc::clone(store))), None)
}

fn opts() -> BuildOpts {
    BuildOpts { reseal: Reseal::All, group_bytes: 16 << 10, sealed: RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2), delta: RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2) }
}

fn key(i: usize) -> Vec<u8> {
    format!("doc/c/{i:05}").into_bytes()
}

async fn put(t: &mut Tenant, i: usize, v: &str) {
    t.apply(vec![Op::Put(key(i), v.as_bytes().to_vec())]).await.unwrap();
}

/// Negative controls: `FX_7A_CONTROL=e1now|xdloop|opholds` removes one fix (the test must then fail).
fn control(name: &str, flag: &std::sync::atomic::AtomicBool) {
    flag.store(std::env::var("FX_7A_CONTROL").ok().as_deref() == Some(name), Relaxed);
}

/// Settings of these tests; `sealed`: `--sealed-runs` on (no publish by itself).
fn settings(sealed: bool) {
    ENABLED.store(sealed, Relaxed);
    PUBLISH_EVERY.store(0, Relaxed);
    T_PIN_MS.store(T_PIN, Relaxed);
    E1_WAIT_MS.store(u64::MAX, Relaxed); // T_pin, as rule 26 asks
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
    crate::envelope::START.store(false, Relaxed);
}

async fn publish(t: &mut Tenant) {
    for _ in 0..50 {
        if t.fx_publish(&opts(), None).await.unwrap().is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(T_PIN / 4)).await;
    }
    panic!("no publish");
}

/// `--envelopes write` (release E1) without sealed runs: the first write announces E1 and stays plain; the marker and
/// the first envelope come only once the announcement is at least T_pin old (rule 26). A reader that saw the
/// announcement refuses to read past T_pin without a refresh and reads again after one (rule 6).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn envelopes_write_waits_one_t_pin_after_its_announcement() {
    let _s = super::tests::SERIAL.lock().await;
    settings(false);
    control("e1now", &CONTROL_E1_AT_ONCE);
    crate::envelope::START.store(true, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "e1-write";
    let mut t = lc(&store).create(id).await.unwrap();
    let t0 = std::time::Instant::now();
    put(&mut t, 1, "a").await;
    let announced = manifest::announced(&store, id).await.unwrap();
    let early_env = t.envelope_seq();
    // a reader in another process, opened after the announcement
    let r = lc(&store).wake(id).await.unwrap();
    while t0.elapsed() < Duration::from_millis(T_PIN + 50) {
        put(&mut t, 2, "b").await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    put(&mut t, 3, "c").await;
    t.apply(vec![Op::Delete(key(1))]).await.unwrap();
    let late_env = t.envelope_seq();
    let stale = r.get(key(2)).await;
    crate::envelope::START.store(false, Relaxed);
    assert!(announced, "the first write of release E1 must announce it first (rule 26)");
    assert_eq!(early_env, None, "release E1 wrote the marker before its announcement was T_pin old (rule 26)");
    assert!(late_env.is_some(), "release E1 never started envelopes");
    assert_eq!(t.get(key(1)).await.unwrap(), None);
    assert_eq!(t.get(key(3)).await.unwrap().as_deref(), Some(&b"c"[..]));
    let e = stale.expect_err("a reader that saw the announcement must refuse to read past T_pin without a refresh (rule 6)");
    assert!(e.chain().any(|c| c.downcast_ref::<Retry>().is_some()), "{e:#}");
    t.abandon().await;
    r.abandon().await;
}

/// A writer that loses its fence race at open reloads the newest version; that version's X_d is checked against
/// SlateDB like the first one (step 3 review: "check X_d inside the load_fx loop").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reloaded_version_above_slatedb_is_refused_at_open() {
    let _s = super::tests::SERIAL.lock().await;
    settings(true);
    control("xdloop", &CONTROL_NO_XD_LOOP_CHECK);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "xd-loop";
    let mut a = lc(&store).create(id).await.unwrap();
    for i in 0..40 {
        put(&mut a, i, "a").await;
    }
    publish(&mut a).await;
    a.close().await.unwrap();
    let newest = manifest::load_newest(&store, id).await.unwrap().unwrap();
    let gate = Arc::new(tokio::sync::Notify::new());
    let waiting = manifest::SLOW_PUT_WAITING.load(SeqCst);
    *manifest::SLOW_PUT.lock().unwrap() = Some(gate.clone());
    let s2 = Arc::clone(&store);
    let open = tokio::spawn(async move {
        let mut b = lc(&s2).wake(id).await?;
        b.upgrade().await?;
        Ok::<_, anyhow::Error>(b)
    });
    while manifest::SLOW_PUT_WAITING.load(SeqCst) == waiting {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    *manifest::SLOW_PUT.lock().unwrap() = None;
    // another publisher's version lands in the fence's slot, with an X_d this writer's SlateDB does not have
    let mut bad = newest.clone();
    bad.version += 1;
    bad.x_d += 1_000_000;
    store.put(&manifest::manifest_path(id, bad.version), PutPayload::from(serde_json::to_vec(&bad).unwrap())).await.unwrap();
    gate.notify_one();
    let r = open.await.unwrap();
    match r {
        Ok(b) => {
            let x = b.fx_view().map(|v| v.x_d);
            b.abandon().await;
            panic!("the writer opened reading with X_d {x:?}, above its SlateDB's last seq: rows at or below X_d would be hidden");
        }
        Err(e) => assert!(format!("{e:#}").contains("X_d"), "{e:#}"),
    }
}

/// One operation that reads several times (an aggregation with `$lookup` takes `Dbs::read` once per collection) keeps
/// the version of its first read in the drop floor until it ends, also when a publish lands between its reads; and it
/// fails with the retryable error once it outlives the hold life (rules 4, 5, 7, 8).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_operation_keeps_the_floor_across_its_reads() {
    let _s = super::tests::SERIAL.lock().await;
    settings(true);
    control("opholds", &CONTROL_NO_OP_HOLDS);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "op-holds";
    let mut t = lc(&store).create(id).await.unwrap();
    for i in 0..40 {
        put(&mut t, i, "a").await;
    }
    publish(&mut t).await;
    let x1 = t.fx_view().unwrap().x_d;
    let holds = Arc::clone(t.fx_holds());
    let (gap_floor, end_floor, t) = hold::op_scope(async move {
        let mut t = t;
        assert_eq!(t.get(key(1)).await.unwrap().as_deref(), Some(&b"a"[..]));
        // between two reads of the operation: more writes and a publish
        for i in 40..80 {
            put(&mut t, i, "b").await;
        }
        publish(&mut t).await;
        assert!(t.fx_view().unwrap().x_d > x1);
        let gap = holds.floor();
        assert_eq!(t.get(key(50)).await.unwrap().as_deref(), Some(&b"b"[..]));
        (gap, holds.floor(), t)
    })
    .await;
    let after = t.fx_holds().floor();
    // an operation longer than the hold life fails at its next read
    hold::HOLD_LIFE_MS.store(300, Relaxed);
    let (second, t) = hold::op_scope(async move {
        t.get(key(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let r = t.get(key(2)).await;
        (r, t)
    })
    .await;
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
    t.abandon().await;
    assert_eq!(gap_floor, Some(x1), "between two reads of one operation the floor must keep the version it read with");
    assert_eq!(end_floor, Some(x1));
    assert_eq!(after, None, "the operation's holds end with it");
    let e = second.expect_err("an operation past the hold life must fail at its next read (rule 5)");
    assert!(e.chain().any(|c| c.downcast_ref::<Retry>().is_some()), "{e:#}");
}
