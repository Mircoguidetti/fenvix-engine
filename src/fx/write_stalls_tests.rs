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
    let e = t.fx_erasure_register("msgs", b"c01", &targets).await.unwrap();
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

/// Rule 8, found while doing this (not caused by it): L0 SSTs not absorbed when the compactor stops (a close) must
/// not be moved into a sorted run by the close-time compaction pass. If they were, the next absorption after a reopen
/// would move X_d past their rows, which are then in neither our runs nor SlateDB's part of the merged read. The scenario
/// runs twice: with the pass's gate off (the code before this branch, the negative control: it must lose rows, or the
/// test does not reach the case) and with the gate on (nothing lost).
async fn pass_gap_scenario(control: bool) -> (usize, usize, usize, usize) {
    crate::lifecycle::CONTROL_NO_PASS_GATE.store(control, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = if control { "ws-pass-gap-control" } else { "ws-pass-gap" };
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    let deleted = HashSet::new();
    for i in 0..100 {
        apply(&mut t, &mut m, vec![(ckey(0, i % 40), Some(vec![b'a'; 40]))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let mut k = knobs(false, true);
    k.absorb_min_l0 = 1000; // nothing is absorbed: every L0 SST written now stays unabsorbed
    assert!(t.fx_claim());
    let c = start(&mut t, &store, &k).await;
    // the gap: rows written only now (new keys), flushed into L0 SSTs that are never absorbed before the close
    for r in 0..5 {
        for i in 0..80 {
            apply(&mut t, &mut m, vec![(ckey(20 + r, i), Some(format!("gap-{r}-{i}").into_bytes()))]).await;
        }
        flush(&t).await;
    }
    let l0_at_close = t.writer_l0().unwrap();
    c.stop().await;
    t.close().await.unwrap();
    // the close-time pass (L0 at or above the threshold), as the maintenance queue runs it
    let pass = crate::lifecycle::compact_tenant(&store, &store, id).await.unwrap();
    let l0_after_pass = crate::lifecycle::l0_count(&store, id).await.unwrap();
    // reopen: new rows, absorbed, then every key read
    let mut t = lc(&store).wake(id).await.unwrap();
    t.upgrade().await.unwrap();
    k.absorb_min_l0 = 1;
    assert!(t.fx_claim());
    let c = start(&mut t, &store, &k).await;
    for i in 0..200 {
        apply(&mut t, &mut m, vec![(ckey(30, i % 70), Some(format!("late-{i}").into_bytes()))]).await;
    }
    flush(&t).await;
    c.drain(false).await.unwrap();
    t.fx_sync();
    let gap_keys: Vec<Vec<u8>> = m.keys().filter(|k| (20..25).any(|r| k.starts_with(&ckey(r, 0)[..ckey(r, 0).len() - 5]))).cloned().collect();
    let mut missing = 0;
    for key in &gap_keys {
        if t.get(key).await.unwrap().as_deref() != m.get(key).map(|v| &v[..]) {
            missing += 1;
        }
    }
    let wrong = wrong_reads(&t, &m, &deleted).await;
    eprintln!(
        "ws pass gap (gate {}): L0 at the close {l0_at_close} (all unabsorbed); pass {:?}, L0 after it {l0_after_pass}; x_d now {:?}; gap keys {} missing {missing}; wrong {wrong}",
        if control { "off" } else { "on" },
        pass.as_ref().map(|p| (p.l0_before, p.l0_after)),
        t.fx_view().map(|v| v.x_d),
        gap_keys.len()
    );
    c.stop().await;
    t.close().await.unwrap();
    crate::lifecycle::CONTROL_NO_PASS_GATE.store(false, Relaxed);
    (l0_at_close, l0_after_pass, missing, wrong)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_close_time_pass_keeps_unabsorbed_l0() {
    let _s = SERIAL.lock().await;
    setup();
    let (l0c, l0c_after, missing_c, _) = pass_gap_scenario(true).await;
    let (l0, l0_after, missing, wrong) = pass_gap_scenario(false).await;
    teardown();
    assert!(l0c >= crate::lifecycle::L0_COMPACT_THRESHOLD && l0 >= crate::lifecycle::L0_COMPACT_THRESHOLD, "setup: the close-time pass must have L0 to compact");
    assert!(l0c_after < l0c, "control: the ungated pass moves the unabsorbed L0 SSTs");
    assert!(missing_c > 0, "control: without the gate the scenario must lose rows, or the test does not reach the case");
    assert!(l0_after >= 1 && l0_after < l0, "the gated pass moves the absorbed L0 SSTs and keeps the unabsorbed ones ({l0} -> {l0_after})");
    assert_eq!(missing, 0, "acknowledged writes lost: the pass moved L0 SSTs that were not absorbed");
    assert_eq!(wrong, 0);
}
