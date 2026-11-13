//! fix-write-stalls (2026-11-12): no write may wait seconds for the first publish or for the compactor's start.
//!
//! - The first build runs off the database's exclusive lock (`Tenant::fx_first_start`, `fx_first_poll`): writes go on
//!   while it runs, nothing written meanwhile is lost or sealed twice, the pre-snapshot L0 SSTs are listed as absorbed.
//! - A close, an erasure's registration or an explicit publish during the build waits for it; a close right after it
//!   and a reopen read everything.
//! - The compactor's start reads no L0 SST under the lock: listed SSTs are taken from the version, the others are read
//!   by a job started at once, even when L0 never reaches the absorption threshold (perf-latency's reverted attempt left
//!   them unfoldable after a reopen, 8075895); a stop right after the start is clean.
//! - A compaction pass and a compactor's claim of the database exclude each other.
//! - A close-time compaction pass must not move L0 SSTs that are not absorbed (rule 8).

use super::compactor::{Compactor, Knobs};
use super::tests::*;
use super::*;
use crate::lifecycle::Tenant;
use crate::runfmt::{CustomerKind, RunOptions};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::Arc;
use std::time::{Duration, Instant};

const HOLD_MS: u64 = 3000;

fn knobs(drop: bool, background: bool) -> Knobs {
    Knobs {
        theta: 0.25,
        r3: false,
        r3_gets_per_gib: 470.0,
        r2_age: None,
        tier_t: 4,
        tier_unit: 2 << 10,
        absorb_min_l0: 1,
        absorb_max_l0: 8,
        recognise_in_background: background,
        local_max_bytes: 0,
        split_bytes: 40 << 10,
        merge_bytes: 18 << 10,
        min_seal_bytes: 8 << 10,
        sealed: RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2),
        delta: RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2),
        drop,
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
    build::PAUSE_AFTER_SNAPSHOT_MS.store(0, Relaxed);
    build::PAUSE_BEFORE_CREATE_MS.store(0, Relaxed);
    compactor::PAUSE_RECOGNISE_MS.store(0, Relaxed);
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

/// Start the first build off the lock, waiting out E1's announcement (rule 26). Returns how long the call that started
/// the build took (what the write that triggers it pays under the lock).
async fn first_start(t: &mut Tenant) -> Duration {
    for _ in 0..100 {
        let t0 = Instant::now();
        if t.fx_first_start(&small_opts(build::Reseal::All)).await.unwrap() {
            return t0.elapsed();
        }
        tokio::time::sleep(Duration::from_millis(TEST_T_PIN_MS / 4)).await;
    }
    panic!("the first build did not start");
}

async fn wrong_reads(t: &Tenant, m: &BTreeMap<Vec<u8>, Vec<u8>>, deleted: &HashSet<Vec<u8>>) -> usize {
    let mut wrong = 0;
    for (k, want) in m {
        if t.get(k).await.unwrap().as_deref() != Some(&want[..]) {
            wrong += 1;
        }
    }
    for k in deleted {
        if !m.contains_key(k) && t.get(k).await.unwrap().is_some() {
            wrong += 1;
        }
    }
    wrong
}

/// Cause 1: the first build runs off the lock. While it is paused right after its snapshot, writes (new keys,
/// overwrites of rows it seals, deletes) go on without waiting for it; its version is installed at a later step; every
/// key then reads its last value; the pre-snapshot L0 SSTs are listed as absorbed, so the compactor reads none of them;
/// and no row is sealed twice: every delta run the compactor writes afterwards starts above the first version's X_d.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_first_build_runs_off_the_lock_and_loses_nothing() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "ws-first";
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    let mut deleted = HashSet::new();
    for round in 0..3 {
        for i in 0..120 {
            apply(&mut t, &mut m, vec![(ckey(round, i % 50), Some(format!("pre-{round}-{i}").into_bytes()))]).await;
        }
        flush(&t).await;
    }
    let l0_pre: HashSet<String> = t.fx_writer_db().unwrap().manifest().l0().iter().map(|v| v.id.to_string()).collect();
    build::PAUSE_AFTER_SNAPSHOT_MS.store(1500, Relaxed);
    let start_call = first_start(&mut t).await;
    // writes while the build runs: none waits for it
    let mut during = 0usize;
    let mut longest = Duration::ZERO;
    for i in 0..300 {
        let w0 = Instant::now();
        match i % 3 {
            0 => apply(&mut t, &mut m, vec![(ckey(10, i % 70), Some(format!("during-{i}").into_bytes()))]).await,
            1 => apply(&mut t, &mut m, vec![(ckey(i % 3, i % 50), Some(format!("over-{i}").into_bytes()))]).await,
            _ => {
                let k = ckey(i % 3, (i * 7) % 50);
                deleted.insert(k.clone());
                apply(&mut t, &mut m, vec![(k, None)]).await
            }
        }
        longest = longest.max(w0.elapsed());
        if !t.fx_first_done() {
            during += 1;
        }
        if i % 60 == 59 {
            flush(&t).await;
        }
    }
    assert!(t.fx_view().is_none(), "the version is installed only by a poll");
    let rep = t.fx_first_poll(true).await.unwrap().expect("the first build's version is installed");
    let v1 = t.fx_view().unwrap().meta.clone();
    let x1 = v1.x_d;
    let wrong_after_install = wrong_reads(&t, &m, &deleted).await;
    // the compactor: listed SSTs from the version, the rest (flushed during the build) read by its job
    let jobs0 = compactor::RECOGNISE_JOBS.load(SeqCst);
    let from_v0 = compactor::L0_RECOGNISED_FROM_VERSION.load(SeqCst);
    assert!(t.fx_claim(), "the database is claimed for the compactor");
    let k = knobs(false, true);
    // the job is held, so what `start` itself knows is visible: only the listed SSTs
    compactor::PAUSE_RECOGNISE_MS.store(300, Relaxed);
    let l0_at_start = t.writer_l0().unwrap();
    let c = start(&mut t, &store, &k).await;
    let known_at_start = c.absorbed_count();
    let by_read_in_start = l0_at_start - known_at_start; // read by the job, not by `start`
    let jobs = compactor::RECOGNISE_JOBS.load(SeqCst) - jobs0;
    let from_v = compactor::L0_RECOGNISED_FROM_VERSION.load(SeqCst) - from_v0;
    for i in 0..200 {
        apply(&mut t, &mut m, vec![(ckey(11, i % 60), Some(format!("after-{i}").into_bytes()))]).await;
    }
    flush(&t).await;
    c.drain(false).await.unwrap();
    t.fx_sync();
    let cur = c.current().await;
    let wrong = wrong_reads(&t, &m, &deleted).await;
    let deltas_at_or_below: Vec<_> = cur.delta.iter().filter(|d| d.min_seq <= x1).map(|d| (d.path.clone(), d.min_seq)).collect();
    let listed: HashSet<String> = v1.absorbed_l0.iter().map(|(id, _)| id.clone()).collect();
    eprintln!(
        "ws first build: start call {start_call:?}; {during} of 300 writes while the build ran, longest write {longest:?}; x_d {x1}, rows scanned {}; \
         L0 before the snapshot {}, listed {}; at the compactor's start {from_v} from the version, {by_read_in_start} left to its job; wrong after install {wrong_after_install}, after absorption {wrong}",
        rep.rows_scanned,
        l0_pre.len(),
        listed.len()
    );
    c.stop().await;
    t.close().await.unwrap();
    compactor::PAUSE_RECOGNISE_MS.store(0, Relaxed);
    teardown();
    assert!(l0_pre.len() >= 3, "setup: L0 SSTs before the snapshot");
    assert!(during >= 100, "writes must go on while the build runs ({during})");
    assert!(longest < Duration::from_millis(1000), "a write waited for the build: {longest:?}");
    assert!(start_call < Duration::from_millis(1000), "the call that starts the build must not wait for it: {start_call:?}");
    assert!(l0_pre.is_subset(&listed), "every L0 SST flushed before the snapshot is listed as absorbed");
    assert!(v1.absorbed_l0.iter().all(|(_, s)| *s == x1));
    assert_eq!(known_at_start as u64, from_v, "the compactor's start knows only the listed SSTs (it reads none)");
    assert!(by_read_in_start == 0 || jobs == 1, "the unlisted SSTs are left to one job");
    assert!(from_v as usize >= l0_pre.len().min(1), "listed SSTs are taken from the version");
    assert_eq!(wrong_after_install, 0);
    assert_eq!(wrong, 0);
    assert!(deltas_at_or_below.is_empty(), "rows at or below the first X_d sealed twice: {deltas_at_or_below:?}");
}

/// A close while the first build runs waits for it (as when it ran under the lock), the version lands, and a reopen
/// with a compactor (fold on) reads every key; a fold and a close right after the start are clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_close_during_the_first_build_then_reopen_and_fold() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "ws-close";
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    let deleted = HashSet::new();
    for i in 0..400 {
        apply(&mut t, &mut m, vec![(ckey(i % 5, i % 80), Some(format!("a-{i}").into_bytes()))]).await;
        if i % 100 == 99 {
            flush(&t).await;
        }
    }
    build::PAUSE_BEFORE_CREATE_MS.store(700, Relaxed);
    first_start(&mut t).await;
    for i in 0..50 {
        apply(&mut t, &mut m, vec![(ckey(7, i), Some(format!("b-{i}").into_bytes()))]).await;
    }
    let t0 = Instant::now();
    t.close().await.unwrap();
    let close_took = t0.elapsed();
    build::PAUSE_BEFORE_CREATE_MS.store(0, Relaxed);
    let newest = manifest::load_newest(&store, id).await.unwrap();
    assert!(!compactor::is_active(id), "the claim is given back at the close");
    // reopen, compactor with the fold, a close right after its start, then again
    let mut t = lc(&store).wake(id).await.unwrap();
    t.upgrade().await.unwrap();
    let k = knobs(true, true);
    assert!(t.fx_claim());
    let c = start(&mut t, &store, &k).await;
    c.stop().await;
    t.close().await.unwrap();
    let mut t = lc(&store).wake(id).await.unwrap();
    t.upgrade().await.unwrap();
    assert!(t.fx_claim());
    let c = start(&mut t, &store, &k).await;
    for i in 0..300 {
        apply(&mut t, &mut m, vec![(ckey(8, i % 90), Some(format!("c-{i}").into_bytes()))]).await;
        if i % 50 == 49 {
            flush(&t).await;
        }
    }
    c.drain(false).await.unwrap();
    t.fx_sync();
    // the fold runs: wait until L0 holds only what was not absorbed yet
    let t1 = Instant::now();
    while t.writer_l0().unwrap_or(0) > 2 && t1.elapsed() < Duration::from_secs(20) {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = t.fx_writer_db().unwrap().refresh_manifest().await;
    }
    let l0_left = t.writer_l0().unwrap_or(0);
    let wrong = wrong_reads(&t, &m, &deleted).await;
    eprintln!("ws close: close during the build took {close_took:?}; newest version after it {:?}; L0 after the fold {l0_left}; wrong {wrong}", newest.as_ref().map(|v| (v.version, v.x_d)));
    c.stop().await;
    t.close().await.unwrap();
    teardown();
    assert!(close_took >= Duration::from_millis(500), "the close waits for the build ({close_took:?})");
    assert!(newest.is_some(), "the build's version landed");
    assert!(l0_left <= 2, "the fold must see the absorbed L0 SSTs ({l0_left} left)");
    assert_eq!(wrong, 0);
}

/// An erasure registered while the first build runs waits for it, then is registered on the installed version; the
/// erased customer's rows read as absent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_erasure_during_the_first_build() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "ws-erase";
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    let mut deleted = HashSet::new();
    for i in 0..300 {
        apply(&mut t, &mut m, vec![(ckey(i % 3, i % 60), Some(format!("v-{i}").into_bytes()))]).await;
    }
    flush(&t).await;
    build::PAUSE_AFTER_SNAPSHOT_MS.store(600, Relaxed);
    first_start(&mut t).await;
    // the erasure: delete customer 1's keys (committed now, above the build's cut), then register it
    let victim: Vec<Vec<u8>> = m.keys().filter(|k| k.starts_with(&ckey(1, 0)[..ckey(1, 0).len() - 5])).cloned().collect();
    assert!(!victim.is_empty());
    for k in &victim {
        deleted.insert(k.clone());
        apply(&mut t, &mut m, vec![(k.clone(), None)]).await;
    }
    let lo = ckey(1, 0)[..ckey(1, 0).len() - 5].to_vec();
    let mut hi = lo.clone();
    *hi.last_mut().unwrap() += 1;
    let targets = erase::Targets { ranges: vec![(lo, hi)], keys: Default::default() };
    let t0 = Instant::now();
    let e = t.fx_erasure_register("msgs", b"c01", &targets, None).await.unwrap();
    let waited = t0.elapsed();
    let installed = t.fx_view().map(|v| v.x_d);
    let wrong = wrong_reads(&t, &m, &deleted).await;
    eprintln!("ws erase: registration waited {waited:?}; version installed {installed:?}; erasure {:?}; wrong {wrong}", e.as_ref().map(|e| e.s_e));
    t.close().await.unwrap();
    teardown();
    assert!(installed.is_some(), "the registration installs the first build's version first");
    assert!(e.is_some_and(|e| Some(e.s_e) >= installed));
    assert_eq!(wrong, 0);
}

/// A compaction pass and a compactor's claim of the database exclude each other: a claim fails while a pass holds its
/// slot; a pass does not enter a claimed database; an unclaim lets it in again.
#[test]
fn ws_pass_and_claim_exclude_each_other() {
    let id = "ws-claim";
    let slot = compactor::pass_enter(id).expect("no claim yet: the pass enters");
    assert!(!compactor::claim(id), "a claim must fail while a pass runs");
    drop(slot);
    assert!(compactor::claim(id));
    assert!(compactor::pass_enter(id).is_none(), "a pass must not enter a claimed database");
    compactor::unclaim(id);
    assert!(compactor::pass_enter(id).is_some());
}

/// Cause 2: a compactor starting on a version that lists none of its L0 SSTs (here: the second build of the offline
/// builder, which lists none) reads nothing in `start`; its job then reads them at once, even with an absorption
/// threshold L0 never reaches (perf-latency's deferral to the first absorption left them unfoldable, 8075895), and the
/// fold folds them. A stop right after the start aborts the job cleanly, and a restart finds them again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_recognition_runs_off_the_lock_at_once() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "ws-recognise";
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    let deleted = HashSet::new();
    for i in 0..100 {
        apply(&mut t, &mut m, vec![(ckey(0, i % 40), Some(vec![b'a'; 40]))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    for r in 0..4 {
        for i in 0..120 {
            apply(&mut t, &mut m, vec![(ckey(1 + r, i % 60), Some(format!("r{r}-{i}").into_bytes()))]).await;
        }
        flush(&t).await;
    }
    // the second build (a delta run) covers every row: its version lists no L0 SST
    publish(&mut t, &small_opts(build::Reseal::None)).await;
    assert!(t.fx_view().unwrap().meta.absorbed_l0.is_empty());
    let l0 = t.writer_l0().unwrap();
    let mut k = knobs(true, true);
    k.absorb_min_l0 = 1000; // L0 never reaches it: only the recognition job can make the SSTs foldable
    // a stop right after the start: the job is cut, nothing breaks
    let by_read0 = compactor::L0_RECOGNISED_BY_READ.load(SeqCst);
    assert!(t.fx_claim());
    compactor::PAUSE_RECOGNISE_MS.store(5000, Relaxed);
    let t_stop = Instant::now();
    let c = start(&mut t, &store, &k).await;
    let absorbed_at_start = c.absorbed_count();
    c.stop().await;
    let stop_took = t_stop.elapsed();
    compactor::PAUSE_RECOGNISE_MS.store(0, Relaxed);
    t.close().await.unwrap();
    let mut t = lc(&store).wake(id).await.unwrap();
    t.upgrade().await.unwrap();
    assert!(t.fx_claim());
    let t0 = Instant::now();
    let c = start(&mut t, &store, &k).await;
    let start_took = t0.elapsed();
    let l0_restart = c.l0_count();
    let t1 = Instant::now();
    while c.absorbed_count() < l0_restart && t1.elapsed() < Duration::from_secs(10) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let recognised_after = t1.elapsed();
    let absorbed = c.absorbed_count();
    // the fold takes them
    let t2 = Instant::now();
    while t.writer_l0().unwrap_or(0) > 0 && t2.elapsed() < Duration::from_secs(20) {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = t.fx_writer_db().unwrap().refresh_manifest().await;
    }
    let l0_after_fold = t.writer_l0().unwrap_or(0);
    let wrong = wrong_reads(&t, &m, &deleted).await;
    eprintln!(
        "ws recognise: {l0} L0 SSTs unlisted; first start knew {absorbed_at_start}, its stop (job held) took {stop_took:?}; restart start took {start_took:?}, recognised {absorbed} of {l0_restart} after {recognised_after:?}; \
         L0 after the fold {l0_after_fold}; read by jobs {}; wrong {wrong}",
        compactor::L0_RECOGNISED_BY_READ.load(SeqCst) - by_read0
    );
    c.stop().await;
    t.close().await.unwrap();
    teardown();
    assert!(l0 >= 3, "setup: unlisted L0 SSTs");
    assert_eq!(absorbed_at_start, 0, "start reads nothing: no SST is known before the job");
    assert!(stop_took < Duration::from_secs(3), "a stop aborts the held job at once ({stop_took:?})");
    assert!(l0_restart >= 3);
    assert_eq!(absorbed, l0_restart, "the job recognises every SST at or below X_d without any absorption");
    assert_eq!(l0_after_fold, 0, "the fold folds the recognised SSTs");
    assert_eq!(wrong, 0);
}

// Rule 8 (the close-time pass and unabsorbed L0): integration-4's guard (`lifecycle::compact_tenant_pass_with`,
// `tiered::OnlyAbsorbed`, `manifest::note_ungated` and the catch-up) is tested by `close_pass_gate_tests`.

/// Cause 3: with the drop off, a merge of sorted runs no longer keeps absorbed L0 SSTs in L0. A merge is held at its
/// first row (`drop::TEST_HOLD_MERGE`) while 12 more L0 SSTs are flushed and absorbed: an L0-only job runs beside it and
/// L0 drains. The control (`drop::CONTROL_ONE_JOB`: no job proposed while one runs, the code before) keeps all of them
/// in L0, the stop SlateDB then holds writes at (8 on the server). Every key reads its last value afterwards.
async fn merge_hold_scenario(control: bool) -> (usize, usize, u64, usize) {
    merge_hold_scenario_with(control, false).await
}

/// `drop`: with the drop on and aged runs (the server's setting since perf-bytes; `fold_max_runs` 2, so a cap merge of
/// two runs comes soon), the held job is that merge and the L0 tail is folded into a new run beside it.
async fn merge_hold_scenario_with(control: bool, drop_on: bool) -> (usize, usize, u64, usize) {
    drop::CONTROL_ONE_JOB.store(control, Relaxed);
    drop::TEST_HOLD_MERGE.store(true, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = format!("ws-merge{}{}", if control { "-control" } else { "" }, if drop_on { "-drop" } else { "" });
    let id = id.as_str();
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    let deleted = HashSet::new();
    for i in 0..100 {
        apply(&mut t, &mut m, vec![(ckey(0, i % 40), Some(vec![b'a'; 40]))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let mut k = knobs(drop_on, true);
    if drop_on {
        k.fold_max_runs = 2;
    }
    assert!(t.fx_claim());
    let c = start(&mut t, &store, &k).await;
    // write until SlateDB proposes a merge of sorted runs, which the hook holds
    let t0 = Instant::now();
    let mut r = 0usize;
    while !drop::TEST_MERGE_HELD.load(Relaxed) {
        assert!(t0.elapsed() < Duration::from_secs(90), "no merge of sorted runs was proposed");
        for i in 0..60 {
            apply(&mut t, &mut m, vec![(ckey(1 + r % 30, i), Some(format!("r{r}-{i}").into_bytes()))]).await;
        }
        flush(&t).await;
        r += 1;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let beside0 = drop::L0_JOBS_BESIDE_MERGE.load(Relaxed);
    // 12 more L0 SSTs while the merge is held
    for f in 0..12 {
        for i in 0..60 {
            apply(&mut t, &mut m, vec![(ckey(40 + f, i), Some(format!("held-{f}-{i}").into_bytes()))]).await;
        }
        flush(&t).await;
    }
    let l0_written = t.writer_l0().unwrap();
    // give the compactor time to absorb them and SlateDB to compact what it may
    let t1 = Instant::now();
    while t1.elapsed() < Duration::from_secs(8) && t.writer_l0().unwrap() >= 4 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = t.fx_writer_db().unwrap().refresh_manifest().await;
    }
    let l0_during = t.writer_l0().unwrap();
    let beside = drop::L0_JOBS_BESIDE_MERGE.load(Relaxed) - beside0;
    drop::TEST_HOLD_MERGE.store(false, Relaxed);
    c.drain(false).await.unwrap();
    t.fx_sync();
    let wrong = wrong_reads(&t, &m, &deleted).await;
    eprintln!("ws merge hold (control {control}, drop {drop_on}): L0 after the 12 flushes {l0_written}, after 8 s with the merge held {l0_during}; L0 jobs beside it {beside}; wrong {wrong}");
    c.stop().await;
    t.close().await.unwrap();
    drop::CONTROL_ONE_JOB.store(false, Relaxed);
    (l0_written, l0_during, beside, wrong)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_l0_drains_beside_a_held_merge() {
    let _s = SERIAL.lock().await;
    setup();
    let (_, l0_ctl, beside_ctl, wrong_ctl) = merge_hold_scenario(true).await;
    let (l0_written, l0, beside, wrong) = merge_hold_scenario(false).await;
    drop::TEST_HOLD_MERGE.store(false, Relaxed);
    teardown();
    assert_eq!(beside_ctl, 0, "control: no job beside the merge");
    assert!(l0_ctl >= 12, "control: the merge must keep the L0 SSTs in L0 ({l0_ctl}), or the test does not reach the case");
    assert!(l0_written >= 4, "setup: L0 must have grown ({l0_written})");
    assert!(beside >= 1, "an L0 job must run beside the held merge");
    assert!(l0 < 4, "L0 must drain while the merge is held ({l0_written} -> {l0})");
    assert_eq!(wrong + wrong_ctl, 0, "wrong reads");
}

/// Cause 3 on integration-4, where the server runs with the drop on and aged runs: a cap merge of two runs is held at
/// its first row while 12 more L0 SSTs are flushed and absorbed; the absorbed L0 tail is folded into a new run beside it
/// and L0 drains. The control (`CONTROL_ONE_JOB`) keeps them in L0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_l0_drains_beside_a_held_merge_with_the_drop_on() {
    let _s = SERIAL.lock().await;
    setup();
    let (_, l0_ctl, beside_ctl, wrong_ctl) = merge_hold_scenario_with(true, true).await;
    let (l0_written, l0, beside, wrong) = merge_hold_scenario_with(false, true).await;
    drop::TEST_HOLD_MERGE.store(false, Relaxed);
    teardown();
    assert_eq!(beside_ctl, 0, "control: no job beside the merge");
    assert!(l0_ctl >= 12, "control: the merge must keep the L0 SSTs in L0 ({l0_ctl}), or the test does not reach the case");
    assert!(l0_written >= 4, "setup: L0 must have grown ({l0_written})");
    assert!(beside >= 1, "an L0 fold must run beside the held merge");
    // the aged fold leaves L0 below `fold_at_l0` (6) while the floor has not passed it: drained means below that, far
    // from SlateDB's stop at 8
    assert!(l0 < 6, "L0 must drain below fold_at_l0 while the merge is held ({l0_written} -> {l0})");
    assert_eq!(wrong + wrong_ctl, 0, "wrong reads");
}

/// Cause 3, the other side: with the drop on, the fold takes the newest sorted run with the L0 SSTs, so it must never
/// start beside a job on the sorted runs. A re-fold of the one run (sorted runs only) is held at its first row while 12
/// L0 SSTs are flushed and absorbed: no other SlateDB job is proposed until it is released, and every key reads right.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_fold_never_runs_beside_a_merge() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "ws-fold-alone";
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    let deleted = HashSet::new();
    for i in 0..100 {
        apply(&mut t, &mut m, vec![(ckey(0, i % 40), Some(vec![b'a'; 40]))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let mut k = knobs(true, true);
    k.fold_one_run = true;
    assert!(t.fx_claim());
    let c = start(&mut t, &store, &k).await;
    // one sorted run first (a fold of absorbed L0 SSTs)
    let t0 = Instant::now();
    let mut r = 0usize;
    while t.fx_writer_db().unwrap().manifest().compacted().len() != 1 || t.writer_l0().unwrap() > 0 {
        assert!(t0.elapsed() < Duration::from_secs(60), "no fold into one sorted run");
        if r < 6 {
            for i in 0..60 {
                apply(&mut t, &mut m, vec![(ckey(1 + r, i), Some(format!("r{r}-{i}").into_bytes()))]).await;
            }
            flush(&t).await;
            r += 1;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = t.fx_writer_db().unwrap().refresh_manifest().await;
    }
    // a re-fold of the run alone, held
    drop::TEST_HOLD_MERGE.store(true, Relaxed);
    c.drop.request_refold();
    let t1 = Instant::now();
    while !drop::TEST_MERGE_HELD.load(Relaxed) {
        assert!(t1.elapsed() < Duration::from_secs(30), "the re-fold was not proposed");
        c.drop.request_refold();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let jobs_at_hold = c.drop.jobs.load(Relaxed);
    let beside0 = drop::L0_JOBS_BESIDE_MERGE.load(Relaxed);
    for f in 0..12 {
        for i in 0..60 {
            apply(&mut t, &mut m, vec![(ckey(40 + f, i), Some(format!("held-{f}-{i}").into_bytes()))]).await;
        }
        flush(&t).await;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    let jobs_during = c.drop.jobs.load(Relaxed) - jobs_at_hold;
    let beside = drop::L0_JOBS_BESIDE_MERGE.load(Relaxed) - beside0;
    let l0_during = t.writer_l0().unwrap();
    drop::TEST_HOLD_MERGE.store(false, Relaxed);
    c.drain(false).await.unwrap();
    t.fx_sync();
    let wrong = wrong_reads(&t, &m, &deleted).await;
    eprintln!("ws fold alone: jobs proposed while the re-fold was held {jobs_during}, beside it {beside}, L0 then {l0_during}; wrong {wrong}");
    c.stop().await;
    t.close().await.unwrap();
    teardown();
    assert_eq!(jobs_during, 0, "a job was proposed beside the held re-fold (drop on)");
    assert_eq!(beside, 0);
    assert!(l0_during >= 12, "setup: the L0 SSTs must wait in L0 while the re-fold is held ({l0_during})");
    assert_eq!(wrong, 0);
}

/// A store whose multipart parts each take `part_ms`, counting uploads left open (created, neither completed nor
/// aborted) and parts cut in the middle (their request dropped before it ended: on S3 such an upload stays listed while
/// the dead request holds it, and an erase cannot prove the database gone).
#[derive(Debug)]
struct SlowParts {
    inner: Arc<dyn ObjectStore>,
    part_ms: u64,
    open: Arc<std::sync::atomic::AtomicI64>,
    in_flight: Arc<std::sync::atomic::AtomicI64>,
    cut: Arc<std::sync::atomic::AtomicU64>,
}

impl std::fmt::Display for SlowParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlowParts({})", self.inner)
    }
}

#[derive(Debug)]
struct SlowUpload {
    inner: Box<dyn object_store::MultipartUpload>,
    part_ms: u64,
    open: Arc<std::sync::atomic::AtomicI64>,
    in_flight: Arc<std::sync::atomic::AtomicI64>,
    cut: Arc<std::sync::atomic::AtomicU64>,
    closed: bool,
}

/// Counts a part in flight; dropped before `done`, the part was cut.
struct PartGuard {
    in_flight: Arc<std::sync::atomic::AtomicI64>,
    cut: Arc<std::sync::atomic::AtomicU64>,
    done: bool,
}

impl Drop for PartGuard {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, SeqCst);
        if !self.done {
            self.cut.fetch_add(1, SeqCst);
        }
    }
}

#[async_trait::async_trait]
impl object_store::MultipartUpload for SlowUpload {
    fn put_part(&mut self, data: object_store::PutPayload) -> object_store::UploadPart {
        let fut = self.inner.put_part(data);
        let (ms, in_flight, cut) = (self.part_ms, Arc::clone(&self.in_flight), Arc::clone(&self.cut));
        in_flight.fetch_add(1, SeqCst);
        let g = PartGuard { in_flight, cut, done: false };
        Box::pin(async move {
            let mut g = g; // the whole guard moves into the future (not only its field)
            tokio::time::sleep(Duration::from_millis(ms)).await;
            let r = fut.await;
            g.done = true;
            r
        })
    }
    async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
        let r = self.inner.complete().await;
        if r.is_ok() && !self.closed {
            self.closed = true;
            self.open.fetch_sub(1, SeqCst);
        }
        r
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        let r = self.inner.abort().await;
        if !self.closed {
            self.closed = true;
            self.open.fetch_sub(1, SeqCst);
        }
        r
    }
}

#[async_trait::async_trait]
impl ObjectStore for SlowParts {
    async fn put_opts(&self, l: &object_store::path::Path, p: object_store::PutPayload, o: object_store::PutOptions) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(&self, l: &object_store::path::Path, o: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        let inner = self.inner.put_multipart_opts(l, o).await?;
        self.open.fetch_add(1, SeqCst);
        Ok(Box::new(SlowUpload { inner, part_ms: self.part_ms, open: Arc::clone(&self.open), in_flight: Arc::clone(&self.in_flight), cut: Arc::clone(&self.cut), closed: false }))
    }
    async fn get_opts(&self, l: &object_store::path::Path, o: object_store::GetOptions) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(l, o).await
    }
    fn delete_stream(&self, l: futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(l)
    }
    fn list(&self, p: Option<&object_store::path::Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(p)
    }
    async fn list_with_delimiter(&self, p: Option<&object_store::path::Path>) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, f: &object_store::path::Path, t: &object_store::path::Path, o: object_store::CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(f, t, o).await
    }
}

/// Returns (parts cut, uploads left open, the abandon's time).
async fn abandon_during_first_build(control: bool) -> (u64, i64, Duration) {
    crate::lifecycle::CONTROL_CUT_FIRST_BUILD.store(control, Relaxed);
    let open = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let in_flight = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let cut = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let store: Arc<dyn ObjectStore> = Arc::new(SlowParts { inner: Arc::new(InMemory::new()), part_ms: 300, open: Arc::clone(&open), in_flight: Arc::clone(&in_flight), cut: Arc::clone(&cut) });
    let id = if control { "ws-abandon-control" } else { "ws-abandon" };
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..300 {
        apply(&mut t, &mut m, vec![(ckey(i % 5, i), Some(vec![b'a'; 200]))]).await;
    }
    first_start(&mut t).await;
    // the build is writing a run's part when the database is abandoned (an erase, a drop, a lost lease)
    let t0 = Instant::now();
    while in_flight.load(SeqCst) == 0 {
        assert!(!t.fx_first_done() && t0.elapsed() < Duration::from_secs(30), "the first build ended without a part in flight");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let t1 = Instant::now();
    t.abandon_waiting_first_build().await;
    let took = t1.elapsed();
    // what a dropped run writer aborts is spawned: let it run
    crate::runfmt::wait_dropped_aborts(Duration::from_secs(5)).await;
    crate::lifecycle::CONTROL_CUT_FIRST_BUILD.store(false, Relaxed);
    eprintln!("abandon during the first build (control {control}): parts cut {}, uploads open {}, abandon took {took:?}", cut.load(SeqCst), open.load(SeqCst));
    (cut.load(SeqCst), open.load(SeqCst), took)
}

/// fix-compactor-start (GitHub CI 6fbd6cd, ctl test_73): an erase or a drop abandons a database whose first build runs
/// off the lock. The build ends by itself before the writer is given up: no run upload is cut in the middle of a part
/// (on S3 such an upload stayed listed and the erase answered `erase_incomplete`), and none is left open. Control (cut at
/// once, the code of 6fbd6cd): a part is cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_abandon_lets_the_first_build_end() {
    let _s = SERIAL.lock().await;
    setup();
    let (cut_ctl, _, _) = abandon_during_first_build(true).await;
    let (cut, open, took) = abandon_during_first_build(false).await;
    teardown();
    assert!(cut_ctl >= 1, "control: the abandon must cut a part in flight ({cut_ctl}), or the test does not reach the case");
    assert_eq!(cut, 0, "a run part was cut in the middle");
    assert_eq!(open, 0, "a run upload was left open");
    assert!(took < Duration::from_secs(10), "the abandon waited {took:?}");
}

/// Review of a60f4fe (rule 8): a first build that ends after another process took the database over must not create a
/// version: the new owner would build on it and lose every row between the two X_d. The build is paused right before its
/// version create while a second writer opens the database (a takeover: SlateDB's writer epoch goes up), then resumes.
/// Fix: the create is refused, no version. Control (`CONTROL_NO_FENCE_CHECK`, a60f4fe): the stale version lands.
/// Also: an abandon for a lost lease (`abandon`) cuts a running first build at once.
async fn late_first_build(control: bool) -> (usize, bool) {
    build::CONTROL_NO_FENCE_CHECK.store(control, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = if control { "ws-late-control" } else { "ws-late" };
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..200 {
        apply(&mut t, &mut m, vec![(ckey(i % 5, i), Some(vec![b'a'; 100]))]).await;
    }
    build::PAUSE_BEFORE_CREATE_MS.store(1500, Relaxed);
    first_start(&mut t).await;
    tokio::time::sleep(Duration::from_millis(300)).await; // the build reaches its pause
    // the takeover: another writer opens the database (the old one is fenced)
    let next = crate::lifecycle::writer_builder(id, &store).build().await.unwrap();
    let r = t.fx_first_poll(true).await;
    build::PAUSE_BEFORE_CREATE_MS.store(0, Relaxed);
    build::CONTROL_NO_FENCE_CHECK.store(false, Relaxed);
    let versions = manifest::list_versions(&store, id).await.unwrap().len();
    eprintln!("late first build (control {control}): {:?}; versions in the store {versions}", r.as_ref().map(|x| x.is_some()));
    let _ = next.close().await;
    drop(t);
    (versions, r.is_err())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_first_build_after_a_takeover_creates_no_version() {
    let _s = SERIAL.lock().await;
    setup();
    let (v_ctl, _) = late_first_build(true).await;
    let (v, refused) = late_first_build(false).await;
    // a lost lease: the abandon cuts the build at once (no wait)
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut t = lc(&store).create("ws-lost").await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..200 {
        apply(&mut t, &mut m, vec![(ckey(i % 5, i), Some(vec![b'a'; 100]))]).await;
    }
    build::PAUSE_BEFORE_CREATE_MS.store(3000, Relaxed);
    first_start(&mut t).await;
    let t0 = Instant::now();
    t.abandon().await;
    let took = t0.elapsed();
    build::PAUSE_BEFORE_CREATE_MS.store(0, Relaxed);
    tokio::time::sleep(Duration::from_millis(3500)).await;
    let v_lost = manifest::list_versions(&store, "ws-lost").await.unwrap().len();
    teardown();
    assert_eq!(v_ctl, 1, "control: the stale version must land, or the test does not reach the case");
    assert!(refused, "the build must fail at its create");
    assert_eq!(v, 0, "a version of the old writer landed after the takeover");
    assert!(took < Duration::from_secs(1), "an abandon for a lost lease waited for the build ({took:?})");
    assert_eq!(v_lost, 0, "the cut build created a version");
}

/// Review of a60f4fe, defence in depth (rule 8): a version of an OLDER writer (a lower writer epoch) that lands at the
/// next slot (a late publish after a takeover) is never a compactor's base. Version 1 is published by writer epoch 1;
/// the database is reopened (epoch 2) and its compactor publishes; then version 1's content is planted at the next slot
/// with epoch 1. More rows are absorbed. Fix: the compactor refuses it, fences over it, and a fresh opening reads every
/// row. Control (`CONTROL_ADOPT_OLDER`, the code before): the compactor builds on it and rows are lost.
async fn older_version_scenario(control: bool) -> (u64, usize, Option<u64>) {
    compactor::CONTROL_ADOPT_OLDER.store(control, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = if control { "ws-older-control" } else { "ws-older" };
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..100 {
        apply(&mut t, &mut m, vec![(ckey(0, i % 40), Some(format!("a-{i}").into_bytes()))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let v1 = manifest::load_newest(&store, id).await.unwrap().unwrap();
    t.close().await.unwrap();
    // reopen: writer epoch 2
    let mut t = lc(&store).wake(id).await.unwrap();
    t.upgrade().await.unwrap();
    let k = knobs(false, false);
    assert!(t.fx_claim());
    let c = start(&mut t, &store, &k).await;
    for r in 0..3 {
        for i in 0..80 {
            apply(&mut t, &mut m, vec![(ckey(1 + r, i), Some(format!("b-{r}-{i}").into_bytes()))]).await;
        }
        flush(&t).await;
    }
    c.drain(false).await.unwrap();
    // the late version of the old writer, at the next slot
    let cur = manifest::load_newest(&store, id).await.unwrap().unwrap();
    let mut stale = v1.clone();
    stale.version = cur.version + 1;
    stale.kind = "publish".into();
    assert!(stale.writer_epoch < cur.writer_epoch, "setup: epochs {:?} {:?}", stale.writer_epoch, cur.writer_epoch);
    assert!(manifest::create(&store, id, &stale).await.unwrap());
    let refused0 = compactor::OLDER_VERSIONS_REFUSED.load(SeqCst);
    for r in 3..6 {
        for i in 0..80 {
            apply(&mut t, &mut m, vec![(ckey(1 + r, i), Some(format!("b-{r}-{i}").into_bytes()))]).await;
        }
        flush(&t).await;
    }
    let _ = c.drain(false).await;
    let _ = c.drain(false).await;
    let refused = compactor::OLDER_VERSIONS_REFUSED.load(SeqCst) - refused0;
    c.stop().await;
    t.close().await.unwrap();
    compactor::CONTROL_ADOPT_OLDER.store(false, Relaxed);
    let newest_epoch = manifest::load_newest(&store, id).await.unwrap().unwrap().writer_epoch;
    // a fresh opening reads from the newest version in the store
    let t = lc(&store).wake(id).await.unwrap();
    let wrong = wrong_reads(&t, &m, &HashSet::new()).await;
    eprintln!("older version (control {control}): refused {refused}, newest version's epoch {newest_epoch:?}, wrong reads after a reopen {wrong} of {}", m.len());
    let _ = t.close().await;
    (refused, wrong, newest_epoch)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_compactor_refuses_an_older_writers_version() {
    let _s = SERIAL.lock().await;
    setup();
    let (_, wrong_ctl, _) = older_version_scenario(true).await;
    let (refused, wrong, epoch) = older_version_scenario(false).await;
    teardown();
    assert!(wrong_ctl > 0, "control: building on the older version must lose rows, or the test does not reach the case");
    assert!(refused >= 1, "the older version was not refused");
    assert_eq!(wrong, 0, "rows lost");
    assert!(epoch >= Some(2), "the newest version is not the compactor's ({epoch:?})");
}

/// Review of a60f4fe, the opening (rule 8): a version of an older writer that landed at the newest slot (a late publish
/// inside the fence window) is not what an opening serves or fences: it takes the newest version of the highest writer
/// epoch. Same setup as `older_version_scenario`, but the stale version stays the newest (no compactor publishes after
/// it); then a fresh opening reads, upgrades to a writer (its fence) and reads again. Control (`CONTROL_TRUST_NEWEST`,
/// the code before): rows are lost.
async fn older_newest_opening(control: bool) -> (usize, usize, u64) {
    manifest::CONTROL_TRUST_NEWEST.store(control, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = if control { "ws-open-older-control" } else { "ws-open-older" };
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..100 {
        apply(&mut t, &mut m, vec![(ckey(0, i % 40), Some(format!("a-{i}").into_bytes()))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let v1 = manifest::load_newest(&store, id).await.unwrap().unwrap();
    t.close().await.unwrap();
    let mut t = lc(&store).wake(id).await.unwrap();
    t.upgrade().await.unwrap();
    // the drop on, as the server runs it: SlateDB's rows at or below the newer X_d are gone, so only the runs hold them
    let k = knobs(true, false);
    assert!(t.fx_claim());
    let c = start(&mut t, &store, &k).await;
    for r in 0..3 {
        for i in 0..80 {
            apply(&mut t, &mut m, vec![(ckey(1 + r, i), Some(format!("b-{r}-{i}").into_bytes()))]).await;
        }
        flush(&t).await;
    }
    c.drain(false).await.unwrap();
    t.fx_sync(); // the tenant serves the newest version, so nothing holds the old floor
    let x = c.current().await.x_d;
    let dr = c.drop_through(x, Duration::from_secs(60)).await.unwrap();
    let _ = dr;
    c.stop().await;
    t.close().await.unwrap();
    // the older writer's late version lands at the newest slot, after everything
    let cur = manifest::load_newest(&store, id).await.unwrap().unwrap();
    let mut stale = v1.clone();
    stale.version = cur.version + 1;
    stale.kind = "publish".into();
    assert!(stale.writer_epoch < cur.writer_epoch);
    assert!(manifest::create(&store, id, &stale).await.unwrap());
    let ignored0 = manifest::OLDER_VERSIONS_IGNORED.load(SeqCst);
    view::clear_cache(); // a new process: no view of this database in memory
    let mut t = lc(&store).wake(id).await.unwrap();
    let wrong_reader = wrong_reads(&t, &m, &HashSet::new()).await;
    t.upgrade().await.unwrap();
    let wrong_writer = wrong_reads(&t, &m, &HashSet::new()).await;
    let ignored = manifest::OLDER_VERSIONS_IGNORED.load(SeqCst) - ignored0;
    manifest::CONTROL_TRUST_NEWEST.store(false, Relaxed);
    eprintln!("opening with an older newest version (control {control}): wrong as reader {wrong_reader}, as writer {wrong_writer} of {}; ignored {ignored}", m.len());
    let _ = t.close().await;
    (wrong_reader, wrong_writer, ignored)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_opening_ignores_an_older_writers_newest_version() {
    let _s = SERIAL.lock().await;
    setup();
    let (r_ctl, w_ctl, _) = older_newest_opening(true).await;
    let (r, w, ignored) = older_newest_opening(false).await;
    teardown();
    assert!(r_ctl + w_ctl > 0, "control: serving the older version must lose rows, or the test does not reach the case");
    assert!(ignored >= 1, "the older version was not ignored");
    assert_eq!((r, w), (0, 0), "rows lost (as reader, as writer)");
}
