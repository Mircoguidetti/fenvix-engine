//! audit-fencing: late writes of a process that lost its database to another one (P-audit-fencing, 2026-11-13).
//!
//! Each test lands one write of the old owner AFTER the takeover, deterministically (a held build, a held PUT), and
//! checks what readers see afterwards. The "control" of each test runs the code of integration-4 4f74574 (a switch,
//! `CONTROL_*` in `manifest.rs`) and must show the damage, or the test does not reach its case.
//!
//! - `fence_first_build_lands_after_takeover`: a first build of the old owner that ends after the new owner published
//!   (the a60f4fe case on integration-4 itself): its version number was taken from a LIST, so it lands ON TOP of the new
//!   owner's versions; the new owner's compactor and every later opening build on it. Acknowledged writes vanish.

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
    teardown();
    assert!(wrong_ctl + err_ctl > 0, "control (integration-4): the late first build must lose acknowledged writes, or the test does not reach the case");
    assert_eq!((wrong, errors), (0, 0), "acknowledged writes lost ({n} keys)");
}
