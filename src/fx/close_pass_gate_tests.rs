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

/// runwriter-and-stall (CI d660ea9, test_51r sealed runs 6, 7 and 9: no insert taken within 30 s after the store came
/// back). A database with a compactor is reopened (after a lease loss) with L0 at SlateDB's limit, every L0 SST absorbed,
/// and several memtables of WAL to replay. SlateDB's replay applies backpressure for each memtable: it waited for a flush
/// that waits for L0 to shrink, which only the compactor's fold does, and the compactor starts after the writer's open,
/// so the open never returned. Now the wake runs the gated compaction pass first (every L0 row is at or below X_d, so it
/// may move them, rule 8): the open returns, and every row reads back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wake_with_full_absorbed_l0_and_wal_opens_the_writer() {
    let _s = SERIAL.lock().await;
    setup();
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(7, Relaxed); // flushes stop with 6 SSTs in L0 (6 + 1 reaches 7)
    let tiers0 = crate::tiered::TIERS.load(Relaxed);
    crate::tiered::TIERS.store(4, Relaxed); // the server's default scheduler (the pass needs one)
    compactor::SERVER_ON.store(true, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (queue, rx) = crate::lifecycle::CompactionQueue::new();
    let lc = crate::lifecycle::Lifecycle::new(Arc::new(crate::counting::CountingStore::new(Arc::clone(&store))), Some(Arc::clone(&queue)));
    let maint = crate::lifecycle::Maintenance::start(Arc::clone(&store), Arc::clone(&store), queue, rx, Arc::clone(&lc.touched), Arc::clone(&lc.maint_locks), 2, Duration::from_secs(3600), Duration::from_secs(3600));
    let id = "wake-full-l0";
    let mut t = lc.create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..100 {
        apply(&mut t, &mut m, vec![(ckey(0, i % 40), Some(vec![b'a'; 40]))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let k = knobs();
    let c = start(&mut t, &store, &k, true).await;
    // L0 up to 6 SSTs, all absorbed (the fold is off: they stay in L0)
    let mut r = 0;
    while t.writer_l0().unwrap() < 6 {
        for i in 0..80 {
            apply(&mut t, &mut m, vec![(ckey(10 + r % 10, i), Some(format!("l0-{r}-{i}").into_bytes()))]).await;
        }
        flush(&t).await;
        r += 1;
        assert!(r < 60, "setup: L0 did not reach 6 SSTs");
    }
    c.drain(false).await.unwrap();
    let l0 = t.writer_l0().unwrap();
    c.stop().await;
    // rows only in the WAL: about 4 memtables (3 KiB each), which cannot flush while L0 is full, over the next writer's
    // limit on unflushed bytes (8 KiB) and well within what an emptied L0 holds (as in the server, where a memtable is
    // MiBs and the WAL of a stalled writer fits in L0 once it is compacted)
    for i in 0..140 {
        apply(&mut t, &mut m, vec![(ckey(30 + i % 5, i), Some(format!("wal-{i}-{}", "w".repeat(60)).into_bytes()))]).await;
    }
    t.abandon().await; // a lease loss: no clean close, the WAL is replayed by the next writer
    // the next writer's limit on unflushed bytes is under what its replay holds (in the server the live writer stalls at
    // that limit with L0 full, so its WAL holds about that much: here the limit is lowered for the replay only)
    crate::lifecycle::MAX_UNFLUSHED_OVERRIDE.store(8 << 10, Relaxed);
    let passes0 = crate::lifecycle::WAKE_GATED_PASSES.load(Relaxed);
    let mut t = lc.wake(id).await.unwrap();
    let t0 = Instant::now();
    let opened = tokio::time::timeout(Duration::from_secs(30), t.upgrade()).await;
    let open_took = t0.elapsed();
    let gated = crate::lifecycle::WAKE_GATED_PASSES.load(Relaxed) - passes0;
    let mut wrong = 0;
    if matches!(opened, Ok(Ok(()))) {
        for (key, want) in &m {
            if t.get(key).await.unwrap().as_deref() != Some(&want[..]) {
                wrong += 1;
            }
        }
    }
    let l0_after = crate::lifecycle::l0_count(&store, id).await.unwrap();
    t.abandon().await;
    eprintln!("wake full L0: L0 at the abandon {l0}, in the stored manifest after the wake {l0_after}; the writer's open {} after {open_took:?}; gated passes {gated}; rows {} wrong {wrong}", match &opened { Ok(Ok(())) => "returned".to_string(), Ok(Err(e)) => format!("failed: {e:#}"), Err(_) => "did not return".to_string() }, m.len());
    let _ = maint.shutdown().await;
    crate::lifecycle::MAX_UNFLUSHED_OVERRIDE.store(0, Relaxed);
    compactor::SERVER_ON.store(false, Relaxed);
    crate::tiered::TIERS.store(tiers0, Relaxed);
    teardown();
    assert!(l0 >= crate::lifecycle::L0_GUARD, "setup: L0 at the abandon {l0}, under the guard");
    assert!(matches!(opened, Ok(Ok(()))), "the writer's open did not return within 30 s with L0 full ({open_took:?})");
    assert_eq!(wrong, 0, "rows read wrong after the reopen (rule 8)");
}
