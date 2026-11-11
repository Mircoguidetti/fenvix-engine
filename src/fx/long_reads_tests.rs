//! c2-fix-long-reads: a long read on a reader is never refused because of T_pin, and it never reads a deleted run.
//!
//! Before the fix, a reader renewed its state (our manifest's version, E_env, release E1's announcement) only under the
//! database's exclusive lock, between commands. One command made of many steps under the shared lock (a find of a whole
//! collection: 523,000 ids, about 60 s, in the integration-3 takeovers) read every step with the state of the refresh
//! before it started, and its steps were refused once that state was T_pin old. The fix renews the state at the start of
//! a step, under the shared lock (`Tenant::step_state`).
//!
//! The tests run a paged scan on a reader in another process (its own lifecycle and holds), one step per page, for many
//! times T_pin, while the writer writes, its compactor and GC run, and every group is resealed again and again, so every
//! run the reader's first version lists is retired and deleted by GC under the scan. Every stable id read is compared
//! with the writer's. Negative control (`CONTROL_NO_RENEW`, the old behaviour): the same scan without renewal is refused
//! by the T_pin guard, also after GC deleted runs its state lists, and never answers.

use super::compactor::{Compactor, Knobs};
use super::tests::*;
use super::*;
use crate::lifecycle::Tenant;
use crate::runfmt::{CustomerKind, RunOptions};
use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::{Duration, Instant};

const T_PIN: u64 = 300;
const HOLD_MS: u64 = 1500;
/// The scan runs this long: more than 3 T_pin (the prompt's floor) and more than the GC grace after the first
/// retirement, so GC deletes runs the reader's first version lists while it scans.
const SCAN_MS: u64 = 6000;
/// Customers 0..CUSTOMERS, STABLE ids each: written once before the scan, never again.
const CUSTOMERS: usize = 8;
const STABLE: usize = 300;
/// Rows per step of the scan (one page of a find), and the pause between two steps (a slow consumer).
const PAGE: usize = 16;
const PAUSE_MS: u64 = 25;

fn knobs() -> Knobs {
    Knobs {
        theta: 0.25,
        r3: true,
        r3_gets_per_gib: 470.0,
        r2_age: None,
        tier_t: 4,
        tier_unit: 2 << 10,
        absorb_min_l0: 1,
        absorb_max_l0: 8,
        local_max_bytes: 16 << 10,
        split_bytes: 40 << 10,
        merge_bytes: 18 << 10,
        min_seal_bytes: 8 << 10,
        sealed: RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2),
        delta: RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2),
        drop: true,
        fold_one_run: false,
        fold_max_runs: 6,
        fold_at_l0: 6,
        lease: Duration::from_millis(T_PIN + 100),
        remote_lease: Duration::from_millis(HOLD_MS + 600),
        serving_lease: Duration::from_millis(300),
        put_timeout: Duration::from_millis(250),
        gc_grace: Duration::from_millis(HOLD_MS + 800),
        version_retention: Duration::from_millis(0),
        gc_every: Duration::from_millis(300),
        tick: Duration::from_millis(10),
        reseal_parallel: 1,
        read_ahead: 64 << 10,
        derive: None,
    }
}

fn setup() {
    enable();
    T_PIN_MS.store(T_PIN, Relaxed);
    hold::HOLD_LIFE_MS.store(HOLD_MS, Relaxed);
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(3 << 10, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(32, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(1, Relaxed);
    local::set_store(Some(Arc::new(InMemory::new())));
    view::clear_cache();
}

fn teardown() {
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(30, Relaxed);
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
    T_PIN_MS.store(TEST_T_PIN_MS, Relaxed);
    E1_WAIT_MS.store(u64::MAX, Relaxed);
    CONTROL_NO_RENEW.store(false, Relaxed);
    local::set_store(None);
}

fn runs_of(v: &manifest::Version) -> Vec<String> {
    v.groups.iter().filter_map(|g| g.sealed.as_ref().map(|s| s.path.clone())).chain(v.delta.iter().map(|d| d.path.clone())).collect()
}

/// A churn key, inside the scanned range, between the stable ids of customer `c` (rewritten all the time).
fn churn_key(c: usize, j: usize) -> Vec<u8> {
    ckey(c, 50_000 + j)
}

fn is_churn(k: &[u8]) -> bool {
    k.len() >= 5 && k[k.len() - 5..] >= b"50000"[..]
}

#[derive(Default, Debug)]
struct Scan {
    steps: u64,
    rows: u64,
    stable_seen: u64,
    wrong: u64,
    refused: u64,
    other_errors: u64,
    first_error: String,
    /// Refusals after GC had deleted a run the reader's first state listed.
    refused_after_delete: u64,
    elapsed_ms: u64,
    passes: u64,
}

/// A paged scan of customers 0..CUSTOMERS on reader `r`, one step per page, repeated until `SCAN_MS` has passed (a pass
/// that ends early starts over: the command is "long"). `deleted` tells whether GC deleted a run of the first state.
/// Every stable row is compared with the writer's value (`want`); churn rows are only checked to be churn rows.
async fn long_scan(r: Arc<Tenant>, want: Arc<BTreeMap<Vec<u8>, Vec<u8>>>, deleted: Arc<std::sync::atomic::AtomicBool>) -> Scan {
    let mut s = Scan::default();
    let lo = ckey(0, 0)[..ckey(0, 0).len() - "msgs/d/00000".len()].to_vec();
    let hi = ckey(CUSTOMERS, 0)[..ckey(CUSTOMERS, 0).len() - "msgs/d/00000".len()].to_vec();
    let t0 = Instant::now();
    'outer: while t0.elapsed() < Duration::from_millis(SCAN_MS) {
        let mut start = lo.clone();
        let mut seen = std::collections::HashSet::new();
        loop {
            if t0.elapsed() > Duration::from_millis(SCAN_MS * 3) {
                break 'outer;
            }
            s.steps += 1;
            match r.scan_range(&start, Some(&hi), false, Some(PAGE), 4096).await {
                Ok(rows) => {
                    let done = rows.len() < PAGE;
                    for (k, v) in &rows {
                        s.rows += 1;
                        if let Some(w) = want.get(&k[..]) {
                            s.stable_seen += 1;
                            seen.insert(k.clone());
                            if &v[..] != &w[..] {
                                s.wrong += 1;
                            }
                        } else if !is_churn(k) || !v.starts_with(b"churn") {
                            s.wrong += 1; // a row nobody wrote
                        }
                    }
                    if let Some((k, _)) = rows.last() {
                        start = k.to_vec();
                        start.push(0); // resume after the last key: a logical key, as the server's pages do
                    }
                    if done {
                        // every stable id must have been seen in this pass
                        s.wrong += want.keys().filter(|k| !seen.contains(&Bytes::copy_from_slice(k))).count() as u64;
                        s.passes += 1;
                        break;
                    }
                }
                Err(e) => {
                    let msg = format!("{e:#}");
                    if e.downcast_ref::<Retry>().is_some() && msg.contains("T_pin") {
                        s.refused += 1;
                        if deleted.load(Relaxed) {
                            s.refused_after_delete += 1;
                        }
                    } else {
                        s.other_errors += 1;
                    }
                    if s.first_error.is_empty() {
                        s.first_error = msg.chars().take(200).collect();
                    }
                    // the command's retry of the same page (a client would start over; the guard must keep firing)
                }
            }
            tokio::time::sleep(Duration::from_millis(PAUSE_MS)).await;
        }
    }
    s.elapsed_ms = t0.elapsed().as_millis() as u64;
    s
}

/// While the scan runs: the writer rewrites churn keys, every group is resealed and the delta runs merged again and
/// again (each pass retires every run of the version before), and GC runs. Returns whether GC deleted a run of `first`.
async fn churn(w: &mut Tenant, c: &Compactor, store: &Arc<dyn ObjectStore>, first: &[String], until: &std::sync::atomic::AtomicBool, deleted: &std::sync::atomic::AtomicBool) -> (u64, u64) {
    let mut i = 0usize;
    let mut reseals = 0u64;
    while !until.load(Relaxed) {
        let mut m = BTreeMap::new();
        for _ in 0..20 {
            apply(w, &mut m, vec![(churn_key(i % CUSTOMERS, i % 40), Some(format!("churn{i}").into_bytes()))]).await;
            i += 1;
        }
        w.fx_sync();
        let v = c.current().await;
        for gi in 0..v.groups.len() {
            if c.reseal_once(Some(gi)).await.is_ok() {
                reseals += 1;
            }
        }
        while c.merge_once().await.unwrap_or(false) {}
        w.fx_sync();
        let _ = gc::gc_once(c).await;
        if !deleted.load(Relaxed) {
            for p in first {
                if matches!(store.head(&object_store::path::Path::from(p.as_str())).await, Err(object_store::Error::NotFound { .. })) {
                    deleted.store(true, Relaxed);
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    (i as u64, reseals)
}

/// The main test (with a version of ours) and its negative control.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_reader_scan_with_compactor_and_gc() {
    let _s = SERIAL.lock().await;
    for control in [false, true] {
        setup();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = if control { "fx-long-ctl" } else { "fx-long" };
        let mut w = lc(&store).create(id).await.unwrap();
        let mut want = BTreeMap::new();
        for c in 0..CUSTOMERS {
            for i in 0..STABLE {
                apply(&mut w, &mut want, vec![(ckey(c, i), Some(format!("stable-{c}-{i}-{}", "x".repeat(i % 40)).into_bytes()))]).await;
            }
        }
        publish(&mut w, &small_opts(build::Reseal::All)).await;
        let k = knobs();
        check_config().unwrap();
        let comp = Compactor::start(&mut w, Arc::clone(&store), k.clone(), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
        comp.run();
        // the reader: another process (its own lifecycle and holds), reader mode, the newest version
        let mut r = lc(&store).wake(id).await.unwrap();
        r.fx_own_holds();
        r.fx_refresh().await.unwrap();
        assert!(!r.is_writer());
        let first = runs_of(&r.fx_view().expect("the reader reads with a version").meta);
        CONTROL_NO_RENEW.store(control, Relaxed);
        let renewals0 = RENEWALS.load(Relaxed);
        let r = Arc::new(r);
        let deleted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let until = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let scan = tokio::spawn(long_scan(Arc::clone(&r), Arc::new(want.clone()), Arc::clone(&deleted)));
        let churn_until = Arc::clone(&until);
        let (writes, reseals) = {
            let stop = async {
                let s = scan.await.unwrap();
                churn_until.store(true, Relaxed);
                s
            };
            let ch = churn(&mut w, &comp, &store, &first, &until, &deleted);
            let (s, (writes, reseals)) = tokio::join!(stop, ch);
            let renewals = RENEWALS.load(Relaxed) - renewals0;
            let gone = {
                let mut n = 0;
                for p in &first {
                    n += matches!(store.head(&object_store::path::Path::from(p.as_str())).await, Err(object_store::Error::NotFound { .. })) as u64;
                }
                n
            };
            eprintln!(
                "long reader scan (control {control}): {:.1} s = {:.1} T_pin, {} steps, {} passes, {} rows, {} stable rows compared, wrong {}, refused {} ({} after GC deleted a run of the reader's first state), other errors {} ({}), renewals {renewals}; writer: {writes} churn writes, {reseals} reseals, GC deleted {gone} of the {} runs of the reader's first version, {} runs in all",
                s.elapsed_ms as f64 / 1000.0,
                s.elapsed_ms as f64 / T_PIN as f64,
                s.steps,
                s.passes,
                s.rows,
                s.stable_seen,
                s.wrong,
                s.refused,
                s.refused_after_delete,
                s.other_errors,
                s.first_error,
                first.len(),
                comp.gc.runs_deleted.load(Relaxed),
            );
            assert_eq!(s.wrong, 0, "a wrong answer");
            assert!(s.elapsed_ms >= 3 * T_PIN, "the scan must last at least 3 T_pin");
            assert!(gone > 0, "GC must delete runs of the reader's first version under the scan");
            if control {
                assert!(s.refused > 0, "the control must be refused by the T_pin guard");
                assert!(s.refused_after_delete > 0, "the guard must still fire after GC deleted a run the stale state lists");
                assert_eq!(s.other_errors, 0, "the control must be refused, never read a deleted run");
            } else {
                assert_eq!(s.refused, 0, "a long read was refused because of T_pin");
                assert_eq!(s.other_errors, 0, "a long read failed: {}", s.first_error);
                assert!(s.passes >= 1, "at least one whole pass");
                assert!(renewals > 0, "the reader renewed its state while it read");
            }
            (writes, reseals)
        };
        let _ = (writes, reseals);
        CONTROL_NO_RENEW.store(false, Relaxed);
        comp.stop().await;
        drop(r);
        w.abandon().await;
        teardown();
    }
}

/// The integration-3 case: a reader with no version of ours that follows release E1's start (announced, no marker yet):
/// the takeovers left the database announced, and no writer lived the E1 wait. The scan runs many T_pin with the state
/// unchanged in the store (a reader-mode tenant holds the lease: no other process writes the marker meanwhile), and
/// every renewal reads E_env, the announcement and the newest version again. Every stable id is compared with the
/// writer's. Control: without renewal, the T_pin guard refuses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_reader_scan_following_e1() {
    let _s = SERIAL.lock().await;
    for control in [false, true] {
        setup();
        // envelopes start much later: the reader wakes with the announcement and no marker
        E1_WAIT_MS.store(3_600_000, Relaxed);
        E1_WAIT_TEST.store(true, Relaxed);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = if control { "fx-long-e1-ctl" } else { "fx-long-e1" };
        let mut w = lc(&store).create(id).await.unwrap();
        let mut want = BTreeMap::new();
        for c in 0..CUSTOMERS {
            for i in 0..STABLE {
                apply(&mut w, &mut want, vec![(ckey(c, i), Some(format!("stable-{c}-{i}").into_bytes()))]).await;
            }
        }
        // announces E1 and returns: the marker comes one E1 wait later
        assert!(w.fx_publish(&small_opts(build::Reseal::All), None).await.unwrap().is_none());
        let mut r = lc(&store).wake(id).await.unwrap();
        r.fx_own_holds();
        r.fx_refresh().await.unwrap();
        assert!(r.fx_view().is_none() && r.envelope_seq().is_none(), "the reader follows E1's start");
        CONTROL_NO_RENEW.store(control, Relaxed);
        let renewals0 = RENEWALS.load(Relaxed);
        let r = Arc::new(r);
        let deleted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let scan = tokio::spawn(long_scan(Arc::clone(&r), Arc::new(want.clone()), Arc::clone(&deleted)));
        // the old writer is gone (abandoned, as a killed process): the store keeps the announcement and no marker
        w.abandon().await;
        let published = 0;
        while !scan.is_finished() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let s = scan.await.unwrap();
        let renewals = RENEWALS.load(Relaxed) - renewals0;
        let (env, ver) = (r.envelope_seq(), r.fx_view().map(|v| v.version));
        eprintln!(
            "long reader scan following E1 (control {control}): {:.1} s = {:.1} T_pin, {} steps, {} passes, {} rows, {} stable rows compared, wrong {}, refused {}, other errors {} ({}), renewals {renewals}; writer published {published} versions; reader's exclusive state at the end: E_env {env:?}, version {ver:?}",
            s.elapsed_ms as f64 / 1000.0,
            s.elapsed_ms as f64 / T_PIN as f64,
            s.steps,
            s.passes,
            s.rows,
            s.stable_seen,
            s.wrong,
            s.refused,
            s.other_errors,
            s.first_error,
        );
        assert_eq!(s.wrong, 0, "a wrong answer");
        assert_eq!(published, 0);
        assert!(s.elapsed_ms >= 3 * T_PIN, "the scan must last at least 3 T_pin");
        if control {
            assert!(s.refused > 0, "the control must be refused by the T_pin guard");
            assert_eq!(s.other_errors, 0);
        } else {
            assert_eq!(s.refused, 0, "a long read was refused because of T_pin");
            assert_eq!(s.other_errors, 0, "a long read failed: {}", s.first_error);
            assert!(s.passes >= 1, "at least one whole pass");
            assert!(renewals > 0);
        }
        CONTROL_NO_RENEW.store(false, Relaxed);
        E1_WAIT_TEST.store(false, Relaxed);
        drop(r);
        teardown();
    }
}
