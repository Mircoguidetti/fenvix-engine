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
