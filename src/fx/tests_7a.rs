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

/// Negative controls: `FX_7A_CONTROL=e1now|xdloop|opholds` removes one fix (the test must then fail; `opholds` brings
/// back step 7a's operation holds, which option (b) replaced).
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

/// One operation that reads several times (an aggregation with `$lookup` takes `Dbs::read` once per collection). Option
/// (b) of the founder (DECISIONS 2026-11-09, step 7b fix round): only reads in flight are in the drop floor, so between
/// two reads of one operation the floor is empty; the second read reads the newest version (a publish landed between);
/// and an operation longer than the hold life goes on (step 7a failed it). Control `FX_7A_CONTROL=opholds`: step 7a's
/// operation holds again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_operation_keeps_only_its_reads_in_flight_in_the_floor() {
    let _s = super::tests::SERIAL.lock().await;
    settings(true);
    control("opholds", &hold::CONTROL_OP_HOLDS);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "op-holds";
    let mut t = lc(&store).create(id).await.unwrap();
    for i in 0..40 {
        put(&mut t, i, "a").await;
    }
    publish(&mut t).await;
    let x1 = t.fx_view().unwrap().x_d;
    let holds = Arc::clone(t.fx_holds());
    let (gap_floor, read2, t) = hold::op_scope(async move {
        let mut t = t;
        assert_eq!(t.get(key(1)).await.unwrap().as_deref(), Some(&b"a"[..]));
        // between two reads of the operation: more writes and a publish
        for i in 40..80 {
            put(&mut t, i, "b").await;
        }
        publish(&mut t).await;
        assert!(t.fx_view().unwrap().x_d > x1);
        let gap = holds.floor();
        let r = t.get(key(50)).await.unwrap();
        (gap, r, t)
    })
    .await;
    let after = t.fx_holds().floor();
    // an operation longer than the hold life goes on
    hold::HOLD_LIFE_MS.store(300, Relaxed);
    let (second, t) = hold::op_scope(async move {
        t.get(key(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let r = t.get(key(2)).await;
        (r, t)
    })
    .await;
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
    hold::CONTROL_OP_HOLDS.store(false, Relaxed);
    t.abandon().await;
    assert_eq!(gap_floor, None, "between two reads of one operation only reads in flight count in the floor (option b)");
    assert_eq!(read2.as_deref(), Some(&b"b"[..]), "the second read reads the newest version");
    assert_eq!(after, None, "no hold outlives its read");
    let r = second.expect("an operation longer than the hold life must go on (option b): only a single read step that long fails");
    assert_eq!(r.as_deref(), Some(&b"a"[..]));
}

/// Pairing (rule 18): the checkpoint is paired with the newest version at or below its `last_l0_seq`; a checkpoint whose
/// drop lease expired before it existed is deleted and taken again [CkNoRecheck]; a drop does not start while the lease
/// is held (rule 11); a paired reader of the checkpoint answers rows a later drop removed from SlateDB.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_checkpoint_is_paired_under_the_drop_lease() {
    let _s = super::tests::SERIAL.lock().await;
    settings(true);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "pairing";
    let mut t = lc(&store).create(id).await.unwrap();
    for i in 0..60 {
        put(&mut t, i, "a").await;
    }
    publish(&mut t).await;
    t.close().await.unwrap();
    let v = manifest::load_newest(&store, id).await.unwrap().unwrap();
    let admin = slatedb::admin::Admin::builder(crate::lifecycle::tenant_path(id), Arc::clone(&store)).build();
    pair::LEASE_LIFE_MS.store(400, Relaxed);
    pair::SKEW_MARGIN_MS.store(50, Relaxed);
    let calls = std::sync::atomic::AtomicU64::new(0);
    let held_during = std::sync::Mutex::new(Vec::new());
    let (cp, p) = pair::checkpoint_paired(&store, id, || async {
        held_during.lock().unwrap().push(pair::lease_held(&store, id).await.unwrap());
        let refused = droptest::drop_at_or_below(&store, id, v.x_d).await.is_err();
        assert!(refused, "a drop started while a checkpoint held the drop lease (rule 11)");
        if calls.fetch_add(1, SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(500)).await; // past the lease's life: this checkpoint must go
        }
        Ok(admin.create_detached_checkpoint(&slatedb::config::CheckpointOptions::default()).await?)
    })
    .await
    .unwrap();
    pair::LEASE_LIFE_MS.store(60_000, Relaxed);
    pair::SKEW_MARGIN_MS.store(5_000, Relaxed);
    assert_eq!(calls.load(SeqCst), 2, "the checkpoint taken after the lease expired was not taken again");
    assert!(held_during.lock().unwrap().iter().all(|h| *h));
    let cps = admin.list_checkpoints(None).await.unwrap();
    assert_eq!(cps.len(), 1, "the checkpoint of the expired lease must be deleted: {cps:?}");
    assert_eq!(cps[0].id, cp.id);
    let p = p.unwrap();
    assert_eq!((p.version, p.exact), (Some(v.version), true), "{p:?}");
    assert!(!pair::lease_held(&store, id).await.unwrap(), "the lease is released");
    // a drop now removes the rows from SlateDB (the checkpoint above keeps its own SSTs); a checkpoint taken after the
    // drop has them only through its pair
    let rep = droptest::drop_at_or_below(&store, id, v.x_d).await.unwrap();
    assert!(rep.dropped >= 60, "{rep:?}");
    let (cp2, p2) = pair::checkpoint_paired(&store, id, || async { Ok(admin.create_detached_checkpoint(&slatedb::config::CheckpointOptions::default()).await?) }).await.unwrap();
    assert_eq!(p2.unwrap().version, Some(v.version));
    let mut o = slatedb::config::DbReaderOptions::default();
    o.skip_wal_replay = true;
    for c in [cp.id, cp2.id] {
        let r = pair::PairedReader::open_checkpoint(&store, id, c, o.clone()).await.unwrap();
        assert_eq!(r.version(), Some(v.version));
        let rows = r.scan(b"doc/", Some(b"doc0"), false, None, 1 << 20).await.unwrap();
        assert_eq!(rows.len(), 60);
        assert_eq!(r.get(&key(7)).await.unwrap().as_deref(), Some(&b"a"[..]));
        r.close().await.unwrap();
    }
    // the checkpoint after the drop read without its pair misses them (what export read before step 7a)
    let raw = read::open_checkpoint_reader(crate::lifecycle::tenant_path(id), Arc::clone(&store), cp2.id, o).await.unwrap();
    assert!(raw.get(&key(7)).await.unwrap().is_none(), "the drop did not remove the row from SlateDB");
    raw.close().await.unwrap();
}
