//! The fix round of step 4 (review `sessions/2026-11-09-review-c2-step4/REPORT.md`, FIX 4): one negative control per fix,
//! each the fix switched off, each failing. The reviewer's own failing tests are in `review4_tests.rs`, unchanged.

use super::compactor::{Compactor, Knobs};
use super::tests::*;
use super::*;
use crate::lifecycle::Tenant;
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Duration;

fn knobs() -> Knobs {
    Knobs {
        tier_unit: 2 << 10,
        absorb_min_l0: 1,
        local_max_bytes: 1 << 20,
        split_bytes: 40 << 10,
        merge_bytes: 18 << 10,
        min_seal_bytes: 8 << 10,
        sealed: crate::runfmt::RunOptions::unchecked(2048, 1 << 20, crate::runfmt::CustomerKind::V2),
        delta: crate::runfmt::RunOptions::unchecked(1024, 1 << 20, crate::runfmt::CustomerKind::V2),
        drop: true,
        lease: Duration::from_millis(400),
        remote_lease: Duration::from_millis(400),
        serving_lease: Duration::from_millis(10),
        put_timeout: Duration::from_millis(250),
        gc_grace: Duration::from_millis(400),
        version_retention: Duration::ZERO,
        gc_every: Duration::from_secs(3600),
        tick: Duration::from_millis(10),
        ..Default::default()
    }
}

fn setup() {
    enable();
    hold::HOLD_LIFE_MS.store(10, Relaxed);
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(3 << 10, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(32, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(1, Relaxed);
    view::clear_cache();
}

fn teardown() {
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(30, Relaxed);
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
    local::set_store(None);
}

async fn flush(t: &Tenant) {
    t.fx_writer_db().unwrap().flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
}

/// A writer with a version, a compactor, rows absorbed, and a floor above 0.
async fn ready(store: &Arc<dyn ObjectStore>, id: &str, k: &Knobs) -> (Tenant, Arc<Compactor>, u64) {
    let mut t = lc(store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..300 {
        apply(&mut t, &mut m, vec![(ckey(1, i % 50), Some(vec![b'v'; 50]))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let c = Compactor::start(&mut t, Arc::clone(store), k.clone(), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    for i in 0..300 {
        apply(&mut t, &mut m, vec![(ckey(2, i % 50), Some(vec![b'w'; 50]))]).await;
    }
    flush(&t).await;
    c.drain(false).await.unwrap();
    t.fx_sync();
    tokio::time::sleep(Duration::from_millis(500)).await;
    c.drop.floor().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    c.drop.floor().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let f = c.drop.floor().await.unwrap();
    (t, c, f)
}

/// FIX 1: an expired lease in step 7a's format, and an object that is not a lease at all, do not stop the drop; with
/// the code before the fix (`until_ms`, missing means forever) both stop it. A live lease stops it either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fix1_lease_format_with_control() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "f4-lease";
    let (t, c, before) = ready(&store, id, &knobs()).await;
    assert!(before > 0);
    let now = manifest::now_ms();
    let expired = serde_json::json!({ "holder": "pid-1", "nonce": 7u64, "taken_ms": now - 120_000, "expires_ms": now - 60_000 });
    let live = serde_json::json!({ "holder": "pid-1", "nonce": 8u64, "taken_ms": now, "expires_ms": now + 60_000 });
    let mut out = Vec::new();
    for (name, body) in [("expired", serde_json::to_vec(&expired).unwrap()), ("garbage", b"not a lease".to_vec()), ("live", serde_json::to_vec(&live).unwrap())] {
        store.put(&pair::lease_path(id), PutPayload::from(body)).await.unwrap();
        let fixed = c.drop.floor().await.unwrap();
        drop::CONTROL_LEASE_UNTIL_MS.store(true, Relaxed);
        let control = c.drop.floor().await.unwrap();
        drop::CONTROL_LEASE_UNTIL_MS.store(false, Relaxed);
        out.push((name, fixed, control));
    }
    eprintln!("FIX 1: floor without a lease {before}; (lease, floor with the fix, floor with the control) {out:?}");
    c.stop().await;
    t.abandon().await;
    teardown();
    assert_eq!(out[0].1, before, "an expired step 7a lease stopped the drop");
    assert_eq!(out[1].1, before, "an unreadable lease object stopped the drop");
    assert_eq!(out[2].1, 0, "a live lease must stop the drop");
    assert_eq!((out[0].2, out[1].2), (0, 0), "the control should stop the drop forever");
}

/// FIX 2's control: the reviewer's interleaving (`review4_tests::r4_q4_two_takers_of_an_expired_lease`) with the code
/// before the fix (delete, then create): both takers hold the lease.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fix2_control_delete_then_create_gives_two_holders() {
    let _s = SERIAL.lock().await;
    pair::CONTROL_DELETE_CREATE.store(true, Relaxed);
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "f4-q4";
    let now = manifest::now_ms();
    let dead = pair::Lease { holder: "dead".into(), nonce: 1, taken_ms: now - 120_000, expires_ms: now - 60_000 };
    inner.put(&pair::lease_path(id), PutPayload::from(serde_json::to_vec(&dead).unwrap())).await.unwrap();
    let gate = Arc::new(super::review4_tests::GateStore::new(Arc::clone(&inner)));
    let store: Arc<dyn ObjectStore> = gate.clone();
    let (sa, sb) = (Arc::clone(&store), Arc::clone(&store));
    let a = tokio::spawn(async move { pair::take_lease(&sa, "f4-q4").await });
    let b = tokio::spawn(async move { pair::take_lease(&sb, "f4-q4").await });
    let (a, b) = (a.await.unwrap(), b.await.unwrap());
    pair::CONTROL_DELETE_CREATE.store(false, Relaxed);
    let holders: Vec<u64> = [&a, &b].iter().filter_map(|r| r.as_ref().ok().map(|l| l.nonce)).collect();
    eprintln!("FIX 2 control: holders {holders:?}");
    assert_eq!(holders.len(), 2, "the control should give two holders");
}

/// FIX 3: a delta run whose NVMe copy has a damaged block. With the fix, a merge that reads it falls back to the store
/// and succeeds (and the bad copy is deleted); with the control (no fallback), every attempt fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fix3_jobs_fall_back_from_a_damaged_nvme_copy_with_control() {
    let _s = SERIAL.lock().await;
    let mut out = Vec::new();
    for control in [true, false] {
        setup();
        let nvme: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        local::set_store(Some(Arc::clone(&nvme)));
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = if control { "f4-nvme-ctl" } else { "f4-nvme" };
        let mut k = knobs();
        k.drop = false;
        k.theta = 1000.0;
        k.r3 = false;
        k.tier_t = 2;
        k.tier_unit = 1 << 30; // every delta run in tier 0: any two merge
        let (mut t, c, _) = ready(&store, id, &k).await;
        // two more absorptions, not merged: two fresh delta runs with NVMe copies
        let mut m = BTreeMap::new();
        for round in 0..2 {
            for i in 0..100 {
                apply(&mut t, &mut m, vec![(ckey(3 + round, i), Some(vec![b'x'; 40]))]).await;
            }
            flush(&t).await;
            c.absorb_once().await.unwrap();
        }
        // damage one block of every NVMe copy (same size, flipped bytes in the middle)
        let v = c.current().await;
        let mut damaged = 0;
        for d in v.delta.iter().filter(|d| d.local) {
            let p = object_store::path::Path::from(d.path.as_str());
            if let Ok(g) = nvme.get(&p).await {
                let mut b = g.bytes().await.unwrap().to_vec();
                let mid = b.len() / 3;
                for x in b[mid..mid + 16].iter_mut() {
                    *x ^= 0xff;
                }
                nvme.put(&p, PutPayload::from(b)).await.unwrap();
                damaged += 1;
            }
        }
        compactor::CONTROL_SCAN_NO_FALLBACK.store(control, Relaxed);
        let mut errors = 0;
        let mut merged = 0;
        // force merges over every delta run (T = 2 makes any two runs mergeable)
        for _ in 0..5 {
            match c.merge_once().await {
                Ok(true) => merged += 1,
                Ok(false) => {}
                Err(_) => errors += 1,
            }
        }
        compactor::CONTROL_SCAN_NO_FALLBACK.store(false, Relaxed);
        out.push((control, damaged, merged, errors, c.current().await.delta.len()));
        c.stop().await;
        t.abandon().await;
        teardown();
    }
    eprintln!("FIX 3: (control, copies damaged, merges done, merge errors, delta runs left) {out:?}");
    assert!(out[0].1 > 0 && out[0].3 > 0, "the control should fail its merges");
    assert!(out[1].1 > 0 && out[1].3 == 0, "with the fix the merges must not fail");
}

/// FIX 4: a new waiter's floor with 600 versions in the store loads at most a few; with the control (list and load every
/// version) it loads them all. GC spreads its loads over passes and deletes nothing until it knows every version.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fix4_paging_with_control() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "f4-page";
    let (t, c, _) = ready(&store, id, &knobs()).await;
    let v = c.current().await;
    let mut fake = v.clone();
    for n in v.version + 1..=v.version + 600 {
        fake.version = n;
        store.put(&manifest::manifest_path(id, n), PutPayload::from(serde_json::to_vec(&fake).unwrap())).await.unwrap();
    }
    let mut res = Vec::new();
    for control in [false, true] {
        drop::CONTROL_FULL_LOAD.store(control, Relaxed);
        let sh = compactor::Shared::new(id, Arc::new(hold::Holds::default()));
        let ctl = drop::DropCtl::new(sh, Arc::clone(&store), &knobs());
        ctl.floor().await.unwrap();
        let first = ctl.versions_loaded.load(Relaxed);
        ctl.floor().await.unwrap();
        res.push((control, first, ctl.versions_loaded.load(Relaxed) - first));
        drop::CONTROL_FULL_LOAD.store(false, Relaxed);
    }
    // GC: a fresh compactor state (a restart) with 600 + versions; passes until it has loaded them all
    let mut passes = 0;
    let deleted_before = c.gc.versions_deleted.load(Relaxed);
    let mut deleted_while_loading = 0;
    loop {
        passes += 1;
        let loaded_before = c.gc.versions_loaded.load(Relaxed);
        gc::gc_once(&c).await.unwrap();
        let loaded = c.gc.versions_loaded.load(Relaxed) - loaded_before;
        if loaded == gc::GC_LOAD_PER_PASS as u64 {
            deleted_while_loading += c.gc.versions_deleted.load(Relaxed) - deleted_before;
        }
        if loaded < gc::GC_LOAD_PER_PASS as u64 || passes > 20 {
            break;
        }
    }
    eprintln!("FIX 4: (control, versions loaded by the first floor, by the second) {res:?}; GC passes to load every version {passes}, versions deleted while still loading {deleted_while_loading}, deleted after {}", c.gc.versions_deleted.load(Relaxed) - deleted_before);
    c.stop().await;
    t.abandon().await;
    teardown();
    assert!(res[0].1 <= 2 && res[0].2 <= 1, "the fix loads at most a version or two");
    assert!(res[1].1 >= 600, "the control should load every version");
    assert!(passes >= 3, "GC should spread its loads over passes");
    assert_eq!(deleted_while_loading, 0, "GC deleted while it did not know every version");
}
