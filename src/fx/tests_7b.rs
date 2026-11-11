//! Tests of compaction 2, step 7b: forks read through a pair (rule 20, review of step 7a note 6), erasure (plan round
//! 2 fix 5; DECISIONS 2026-11-07), and the review notes 3, 4 and 5 of step 7a. The fork is tested end to end, through
//! the control API, in `ctl-tests/test_48_c2_step7b_forks_erasure.py`.
//!
//! Negative controls: `FX_7B_CONTROL=keepcp|afteranswer|noscrub|nodelta|noseqbound|noretire|forkraw|noownership` removes one fix
//! (the test that covers it must then fail).

use super::build::{BuildOpts, Reseal};
use super::erase::{ScrubOpts, Targets};
use super::*;
use crate::counting::CountingStore;
use crate::lifecycle::{Lifecycle, Op, Tenant};
use crate::runfmt::{CustomerKind, RunOptions};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::{Duration, Instant};

const T_PIN: u64 = 300;

fn lc(store: &Arc<dyn ObjectStore>) -> Arc<Lifecycle> {
    Lifecycle::new(Arc::new(CountingStore::new(Arc::clone(store))), None)
}

fn opts(reseal: Reseal) -> BuildOpts {
    BuildOpts { reseal, group_bytes: 4 << 10, sealed: RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2), delta: RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2) }
}

fn scrub_opts() -> ScrubOpts {
    let o = opts(Reseal::None);
    ScrubOpts { sealed: o.sealed, delta: o.delta }
}

fn control_is(name: &str) -> bool {
    std::env::var("FX_7B_CONTROL").ok().as_deref() == Some(name)
}

fn settings(sealed: bool) {
    ENABLED.store(sealed, Relaxed);
    PUBLISH_EVERY.store(0, Relaxed);
    T_PIN_MS.store(T_PIN, Relaxed);
    E1_WAIT_MS.store(u64::MAX, Relaxed);
    E1_WAIT_TEST.store(false, Relaxed);
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
    GC_GRACE_MS.store(600_000, Relaxed);
    crate::envelope::START.store(false, Relaxed);
    erase::CONTROL_NO_SCRUB.store(control_is("noscrub"), Relaxed);
    erase::CONTROL_NO_DELTA_SCRUB.store(control_is("nodelta"), Relaxed);
    erase::CONTROL_NO_SEQ_BOUND.store(control_is("noseqbound"), Relaxed);
    erase::CONTROL_NO_RETIRE.store(control_is("noretire"), Relaxed);
    erase::CONTROL_NO_OWNERSHIP.store(control_is("noownership"), Relaxed);
    pair::CONTROL_KEEP_FAILED_CHECKPOINT.store(control_is("keepcp"), Relaxed);
    CONTROL_LOADED_AFTER_ANSWER.store(control_is("afteranswer"), Relaxed);
}

async fn publish(t: &mut Tenant, reseal: Reseal) {
    for _ in 0..50 {
        if t.fx_publish(&opts(reseal.clone()), None).await.unwrap().is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(T_PIN / 4)).await;
    }
    panic!("no publish");
}

async fn put(t: &mut Tenant, k: &[u8], v: &[u8]) {
    t.apply(vec![Op::Put(k.to_vec(), v.to_vec())]).await.unwrap();
}

/// The GC grace at 0 for one test (rule 23's grace is what keeps retired runs; the erasure tests run with windows at 0),
/// back to its default when the test ends, panics included.
struct Grace0;

impl Grace0 {
    fn new() -> Grace0 {
        GC_GRACE_MS.store(0, Relaxed);
        Grace0
    }
}

impl Drop for Grace0 {
    fn drop(&mut self) {
        GC_GRACE_MS.store(600_000, Relaxed);
    }
}

/// Every object of a database that holds `needle`: our runs decoded block by block (they are lz4), every other object
/// (SlateDB's WAL, L0 and sorted-run SSTs, manifests, our records) as stored.
async fn holders(store: &Arc<dyn ObjectStore>, id: &str, needle: &[u8]) -> Vec<String> {
    let metas: Vec<ObjectMeta> = store.list(Some(&Path::from(crate::lifecycle::tenant_path(id)))).try_collect().await.unwrap();
    let has = |b: &[u8]| b.windows(needle.len()).any(|w| w == needle);
    let mut out = Vec::new();
    for m in metas {
        let p = m.location.to_string();
        if p.contains("/fx/run/") {
            let (run, _) = crate::runfmt::Run::open(store, m.location.clone(), m.size).await.unwrap();
            let all = store.get(&m.location).await.unwrap().bytes().await.unwrap();
            let mut hit = false;
            for i in 0..run.fence.blocks() {
                let (x, y) = run.fence.range(i, i);
                let b = crate::runfmt::decode_checked(&run, i, &all[x as usize..y as usize]).unwrap();
                if (0..b.len()).any(|j| has(b.key(j)) || has(b.env(j))) {
                    hit = true;
                    break;
                }
            }
            if hit {
                out.push(p);
            }
        } else {
            let b = store.get(&m.location).await.unwrap().bytes().await.unwrap();
            if has(&b) {
                out.push(p);
            }
        }
    }
    out
}

/// The runs a version lists that hold `needle` (decoded).
async fn version_holders(store: &Arc<dyn ObjectStore>, id: &str, needle: &[u8]) -> Vec<String> {
    let v = manifest::load_newest(store, id).await.unwrap().unwrap();
    let listed: Vec<String> = v.groups.iter().filter_map(|g| g.sealed.as_ref().map(|r| r.path.clone())).chain(v.delta.iter().map(|r| r.path.clone())).collect();
    holders(store, id, needle).await.into_iter().filter(|p| listed.contains(p)).collect()
}

// ---- forks ----------------------------------------------------------------------------------------------------------

/// A fork taken after a drop reads every row of its source through its pair: its first version is the version paired
/// with the checkpoint it was cloned from, its runs shared by reference and named in a fork reference of the source
/// (rule 20). Its own first write gets a seq above that version's X_d and is read. Control `forkraw`: the fork clones the
/// latest state with no pair, as before step 7b, and misses the dropped rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fork_after_a_drop_has_every_row() {
    let _s = super::tests::SERIAL.lock().await;
    settings(true);
    E1_WAIT_MS.store(0, Relaxed);
    let raw = control_is("forkraw");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (src, fork) = ("fork-src", "fork-dst");
    let mut t = lc(&store).create(src).await.unwrap();
    for i in 0..80 {
        put(&mut t, format!("doc/c/{i:05}").as_bytes(), format!("v{i}").as_bytes()).await;
    }
    publish(&mut t, Reseal::All).await;
    t.apply(vec![Op::Delete(b"doc/c/00003".to_vec())]).await.unwrap();
    publish(&mut t, Reseal::None).await;
    t.close().await.unwrap();
    let v = manifest::load_newest(&store, src).await.unwrap().unwrap();
    droptest::drop_at_or_below(&store, src, v.x_d).await.unwrap();
    let admin = slatedb::admin::Admin::builder(crate::lifecycle::tenant_path(src), Arc::clone(&store)).build();
    let fadmin = slatedb::admin::Admin::builder(crate::lifecycle::tenant_path(fork), Arc::clone(&store)).build();
    if raw {
        fadmin.create_clone_builder_from_source(slatedb::CloneSourceSpec::new(crate::lifecycle::tenant_path(src))).build().await.unwrap();
    } else {
        let (cp, p) = pair::checkpoint_paired(&store, src, || async { Ok(admin.create_detached_checkpoint(&slatedb::config::CheckpointOptions::default()).await?) }).await.unwrap();
        let p = p.unwrap();
        assert_eq!(p.version, Some(v.version));
        fadmin.create_clone_builder_from_source(slatedb::CloneSourceSpec::with_checkpoint(crate::lifecycle::tenant_path(src), cp.id)).build().await.unwrap();
        assert_eq!(pair::seed_fork(&store, src, fork, &p, pair::ForkRuns::Shared).await.unwrap(), Some(v.version));
        // the source's fork reference names the shared runs (GC keeps them while the fork lives)
        let r: pair::ForkRef = serde_json::from_slice(&store.get(&pair::fork_ref_path(src, fork)).await.unwrap().bytes().await.unwrap()).unwrap();
        assert_eq!(r.version, v.version);
        assert!(!r.runs.is_empty());
        let refs = erase::referenced_versions(&store, src).await.unwrap();
        assert!(refs.iter().any(|(n, w, _)| *n == v.version && w.contains(fork)), "{refs:?}");
    }
    let mut f = lc(&store).create(fork).await.unwrap();
    let mut missing = Vec::new();
    for i in 0..80 {
        let got = f.get(format!("doc/c/{i:05}").into_bytes()).await.unwrap();
        let want = (i != 3).then(|| format!("v{i}").into_bytes());
        if got.as_deref() != want.as_deref() {
            missing.push(i);
        }
    }
    put(&mut f, b"doc/c/99999", b"forked").await;
    let own = f.get(b"doc/c/99999".to_vec()).await.unwrap();
    let rows = f.scan_prefix(b"doc/").await.unwrap().len();
    f.close().await.unwrap();
    assert!(missing.is_empty(), "the fork misses rows of its source after a drop: {} wrong of 80 ({:?}...)", missing.len(), &missing[..missing.len().min(5)]);
    assert_eq!(own.as_deref(), Some(&b"forked"[..]), "the fork's own write must read (its seq is above the paired X_d)");
    assert_eq!(rows, 80);
    // the source is untouched by the fork's write
    let s = lc(&store).wake(src).await.unwrap();
    assert!(s.get(b"doc/c/99999".to_vec()).await.unwrap().is_none());
    s.abandon().await;
}

// ---- erasure --------------------------------------------------------------------------------------------------------

const ERASED: &[u8] = b"ERASEDMARK";
const ORDINARY: &[u8] = b"ORDINARYMARK";

fn a_key(i: usize) -> Vec<u8> {
    format!("u/CUSTA/c/d/{i:04}").into_bytes()
}

fn loc_key(i: usize) -> Vec<u8> {
    format!("loc/c/{i:04}A").into_bytes()
}

fn b_key(i: usize) -> Vec<u8> {
    format!("u/CUSTB/c/d/{i:04}").into_bytes()
}

fn c_key(i: usize) -> Vec<u8> {
    format!("u/CUSTC/c/d/{i:04}").into_bytes()
}

/// The targets of erasing customer A: its range and its locators (what the server derives from the deletes).
fn a_targets() -> Targets {
    let mut t = Targets::default();
    t.ranges.push((b"u/CUSTA/".to_vec(), b"u/CUSTA0".to_vec()));
    for i in 0..30 {
        t.add_key(loc_key(i));
        t.add_key(a_key(i)); // inside the range: not added twice
    }
    t
}

/// Customer A in a sealed run and a delta run, B and C beside it; returns the writer.
async fn erasure_setup(store: &Arc<dyn ObjectStore>, id: &str) -> Tenant {
    let mut t = lc(store).create(id).await.unwrap();
    for i in 0..30 {
        put(&mut t, &a_key(i), format!("{}-a-{i}-{}", std::str::from_utf8(ERASED).unwrap(), "x".repeat(40)).as_bytes()).await;
        put(&mut t, &loc_key(i), format!("{}-loc-{i}", std::str::from_utf8(ERASED).unwrap()).as_bytes()).await;
        put(&mut t, &b_key(i), format!("{}-b-{i}", std::str::from_utf8(ORDINARY).unwrap()).as_bytes()).await;
        put(&mut t, &c_key(i), format!("c-{i}-{}", "y".repeat(40)).as_bytes()).await;
    }
    publish(&mut t, Reseal::All).await;
    // A changes again: its older versions now sit in the sealed run, the newer ones in a delta run
    for i in 0..30 {
        put(&mut t, &a_key(i), format!("{}-a2-{i}", std::str::from_utf8(ERASED).unwrap()).as_bytes()).await;
    }
    publish(&mut t, Reseal::None).await;
    t
}

/// Erase customer A the way the server does: delete its keys, then record the erasure under the same exclusive lock.
async fn erase_a(t: &mut Tenant) -> erase::Erasure {
    let mut ops: Vec<Op> = (0..30).map(|i| Op::Delete(a_key(i))).collect();
    ops.extend((0..30).map(|i| Op::Delete(loc_key(i))));
    t.apply(ops).await.unwrap();
    t.fx_erasure_register("c", b"CUSTA", &a_targets(), None).await.unwrap().expect("sealed runs are on: the erasure is recorded")
}

/// An erasure (DECISIONS 2026-11-07, plan fix 5): customer A, aged into a sealed run and a delta run, is erased; B gets
/// an ordinary delete. After the erasure pass, no run of ours holds A (sealed and delta, the newest version and the
/// retired runs), the targets object is gone, and A's document written again after the erasure is kept. After the drop
/// (stand-in) and SlateDB's GC with windows at 0, no object of the database holds A: L0, sorted runs and WAL included.
/// B's ordinary delete stays in the sealed run until the group's next reseal, as decided, and leaves with it.
/// Controls: `noscrub` (the pass rewrites nothing), `nodelta` (delta runs not scrubbed), `noseqbound` (the new document
/// is erased too), `noretire` (the replaced runs stay).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_erasure_leaves_every_run_and_slatedb() {
    let _s = super::tests::SERIAL.lock().await;
    settings(true);
    let _g = Grace0::new();
    E1_WAIT_MS.store(0, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "erase-a";
    let mut t = erasure_setup(&store, id).await;
    // the scanner sees the markers where they are (it would otherwise prove nothing)
    let before = holders(&store, id, ERASED).await;
    assert!(before.iter().any(|p| p.contains("/fx/run/g")), "A is in a sealed run: {before:?}");
    assert!(before.iter().any(|p| p.contains("/fx/run/delta")), "A is in a delta run: {before:?}");
    assert!(before.iter().any(|p| !p.contains("/fx/")), "A is in SlateDB's objects: {before:?}");
    // an ordinary delete of B's first document, then the erasure of A, then A writes again
    t.apply(vec![Op::Delete(b_key(0))]).await.unwrap();
    let e = erase_a(&mut t).await;
    assert_eq!((e.state.as_str(), e.ranges, e.keys), ("pending", 1, 30));
    assert!(e.deadline_ms > e.requested_ms);
    // the targets are durable before the scrub: in the record (perf-erase), or in `{id}.keys` (earlier records)
    assert!(e.targets.is_some() || store.head(&erase::keys_path(id, &e.id)).await.is_ok());
    put(&mut t, &a_key(5), b"NEWDATA after the erasure").await;
    let all = t.fx_erase_pass(&scrub_opts()).await.unwrap();
    let e = all.into_iter().find(|x| x.id == e.id).unwrap();
    // reads: A gone except its new document; B's deleted document gone, the others there; C there
    let mut wrong = Vec::new();
    for i in 0..30 {
        let a = t.get(a_key(i)).await.unwrap();
        if (i == 5) != a.is_some() {
            wrong.push(format!("A{i}: {:?}", a.map(|b| String::from_utf8_lossy(&b).to_string())));
        }
        if t.get(loc_key(i)).await.unwrap().is_some() {
            wrong.push(format!("loc{i}"));
        }
        if (i == 0) != t.get(b_key(i)).await.unwrap().is_none() {
            wrong.push(format!("B{i}"));
        }
        if t.get(c_key(i)).await.unwrap().is_none() {
            wrong.push(format!("C{i}"));
        }
    }
    let a5 = t.get(a_key(5)).await.unwrap();
    let in_newest = version_holders(&store, id, ERASED).await;
    let in_runs: Vec<String> = holders(&store, id, ERASED).await.into_iter().filter(|p| p.contains("/fx/")).collect();
    let ordinary_kept = version_holders(&store, id, b"ORDINARYMARK-b-0").await;
    // the ordinary delete leaves with the group's next reseal
    publish(&mut t, Reseal::All).await;
    let ordinary_after_reseal = version_holders(&store, id, b"ORDINARYMARK-b-0").await;
    t.close().await.unwrap();
    assert!(wrong.is_empty(), "wrong reads after the erasure: {wrong:?}");
    assert_eq!(a5.as_deref(), Some(&b"NEWDATA after the erasure"[..]), "a document written after the erasure is new data and must stay");
    assert!(in_newest.is_empty(), "the newest version still lists runs holding the erased customer: {in_newest:?}");
    assert!(in_runs.is_empty(), "objects of ours still hold the erased customer after the pass (grace 0): {in_runs:?}");
    assert_eq!(e.state, "scrubbed");
    assert!(e.rows_erased >= 60, "{e:?}");
    assert!(e.live_done_ms.is_some() && e.done_ms.is_some(), "{e:?}");
    assert!(!e.overdue(manifest::now_ms()));
    assert!(store.head(&erase::keys_path(id, &e.id)).await.is_err(), "the targets object (the customer's own keys) must go with the scrub");
    assert!(e.targets.is_none(), "the targets in the record (the customer's own keys) must go with the scrub");
    assert!(!ordinary_kept.is_empty(), "an ordinary delete is not an erasure: its older row stays until the group's next reseal");
    assert!(ordinary_after_reseal.is_empty(), "the reseal must take the ordinary delete's older row out: {ordinary_after_reseal:?}");
    // SlateDB: the drop at X_d >= S_e, then GC with windows at 0
    let v = manifest::load_newest(&store, id).await.unwrap().unwrap();
    assert!(v.x_d >= e.s_e);
    let slate_before = holders(&store, id, ERASED).await;
    droptest::drop_at_or_below(&store, id, v.x_d).await.unwrap();
    crate::lifecycle::gc_tenant(&store, id, Duration::ZERO).await.unwrap();
    let left = holders(&store, id, ERASED).await;
    let r = lc(&store).wake(id).await.unwrap();
    let a5_after = r.get(a_key(5)).await.unwrap();
    let a6_after = r.get(a_key(6)).await.unwrap();
    let c_after = r.get(c_key(7)).await.unwrap();
    r.abandon().await;
    assert!(!slate_before.is_empty(), "before the drop SlateDB still holds the customer (the drop is what removes it)");
    assert!(left.is_empty(), "objects still hold the erased customer after the drop and GC at 0: {left:?}");
    assert_eq!(a5_after.as_deref(), Some(&b"NEWDATA after the erasure"[..]));
    assert!(a6_after.is_none());
    assert!(c_after.is_some());
}

/// A restore point taken before the erasure keeps the runs it pairs with (DECISIONS: restore points after 30 days):
/// the pass deletes them only once the checkpoint and its pair are deleted. The live runs are clean at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restore_point_keeps_erased_runs_until_it_goes() {
    let _s = super::tests::SERIAL.lock().await;
    settings(true);
    let _g = Grace0::new();
    E1_WAIT_MS.store(0, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "erase-rp";
    // closed first: step 3's builder sets X_d at a WAL seq, so a checkpoint of an open writer may pair with an older
    // version (step 7a's report, "exact: false"); a clean close puts every row in L0
    erasure_setup(&store, id).await.close().await.unwrap();
    let admin = slatedb::admin::Admin::builder(crate::lifecycle::tenant_path(id), Arc::clone(&store)).build();
    let (cp, p) = pair::checkpoint_paired(&store, id, || async { Ok(admin.create_detached_checkpoint(&slatedb::config::CheckpointOptions::default()).await?) }).await.unwrap();
    let paired = p.unwrap().version.unwrap();
    let mut t = lc(&store).wake(id).await.unwrap();
    let e = erase_a(&mut t).await;
    let e = t.fx_erase_pass(&scrub_opts()).await.unwrap().into_iter().find(|x| x.id == e.id).unwrap();
    let live = version_holders(&store, id, ERASED).await;
    let kept: Vec<String> = holders(&store, id, ERASED).await.into_iter().filter(|p| p.contains("/fx/run/")).collect();
    assert!(live.is_empty(), "{live:?}");
    assert!(!kept.is_empty(), "the restore point's runs must stay (30 days)");
    assert!(e.live_done_ms.is_some() && e.done_ms.is_none(), "{e:?}");
    assert!(e.retired.iter().any(|r| r.kept_by.iter().any(|w| w.contains(&cp.id.to_string()))), "{:?}", e.retired);
    assert!(e.versions_kept.iter().any(|v| v.contains(&format!("version {paired}"))), "{:?}", e.versions_kept);
    // the restore point expires: checkpoint and pair go, the next pass deletes the runs
    admin.delete_checkpoint(cp.id).await.unwrap();
    pair::delete_pair(&store, id, &cp.id).await.unwrap();
    let e = t.fx_erase_pass(&scrub_opts()).await.unwrap().into_iter().find(|x| x.id == e.id).unwrap();
    let after: Vec<String> = holders(&store, id, ERASED).await.into_iter().filter(|p| p.contains("/fx/")).collect();
    t.close().await.unwrap();
    assert!(after.is_empty(), "{after:?}");
    assert!(e.done_ms.is_some(), "{e:?}");
}

/// The deadline: an erasure is overdue once 24 hours (here 200 ms) pass without its pass; one done in time is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_erasure_reports_its_deadline() {
    let _s = super::tests::SERIAL.lock().await;
    settings(true);
    let _g = Grace0::new();
    E1_WAIT_MS.store(0, Relaxed);
    erase::DEADLINE_MS.store(200, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "erase-deadline";
    let mut t = erasure_setup(&store, id).await;
    let e = erase_a(&mut t).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let late = erase::list(&store, id).await.unwrap().pop().unwrap();
    let e2 = {
        let mut ops: Vec<Op> = (0..30).map(|i| Op::Delete(b_key(i))).collect();
        ops.push(Op::Delete(c_key(0)));
        t.apply(ops).await.unwrap();
        let mut tb = Targets::default();
        tb.ranges.push((b"u/CUSTB/".to_vec(), b"u/CUSTB0".to_vec()));
        t.fx_erasure_register("c", b"CUSTB", &tb, None).await.unwrap().unwrap()
    };
    let all = t.fx_erase_pass(&scrub_opts()).await.unwrap();
    t.close().await.unwrap();
    erase::DEADLINE_MS.store(24 * 3600 * 1000, Relaxed);
    assert!(late.overdue(manifest::now_ms()), "an erasure past its deadline without a pass must say so");
    assert!(all.iter().find(|x| x.id == e.id).unwrap().overdue(manifest::now_ms()), "done after its deadline: still reported late");
    assert!(!all.iter().find(|x| x.id == e2.id).unwrap().overdue(manifest::now_ms()), "done in time");
}

/// Review of step 7b, FIX 1: an erasure inside a `clone` fork. The fork's first version lists its source's runs; the
/// fork's scrub replaces them in the fork, and its retire step must never delete them (they are the source's, and the
/// source's versions list them): it releases them from its fork reference instead. The source keeps every row and
/// every run; the fork's erasure is done and the customer is gone from the fork's own objects. Control `noownership`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_erasure_in_a_fork_never_deletes_a_run_of_its_source() {
    let _s = super::tests::SERIAL.lock().await;
    settings(true);
    let _g = Grace0::new();
    E1_WAIT_MS.store(0, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (src, fork) = ("own-src", "own-fork");
    erasure_setup(&store, src).await.close().await.unwrap();
    let src_runs = |store: Arc<dyn ObjectStore>| async move {
        let m: Vec<ObjectMeta> = store.list(Some(&Path::from(format!("{}/run", manifest::root(src))))).try_collect().await.unwrap();
        m.into_iter().map(|m| m.location.to_string()).collect::<std::collections::BTreeSet<_>>()
    };
    let before = src_runs(Arc::clone(&store)).await;
    let admin = slatedb::admin::Admin::builder(crate::lifecycle::tenant_path(src), Arc::clone(&store)).build();
    let fadmin = slatedb::admin::Admin::builder(crate::lifecycle::tenant_path(fork), Arc::clone(&store)).build();
    let (cp, p) = pair::checkpoint_paired(&store, src, || async { Ok(admin.create_detached_checkpoint(&slatedb::config::CheckpointOptions::default()).await?) }).await.unwrap();
    fadmin.create_clone_builder_from_source(slatedb::CloneSourceSpec::with_checkpoint(crate::lifecycle::tenant_path(src), cp.id)).build().await.unwrap();
    pair::seed_fork(&store, src, fork, p.as_ref().unwrap(), pair::ForkRuns::Shared).await.unwrap();
    admin.delete_checkpoint(cp.id).await.unwrap();
    pair::delete_pair(&store, src, &cp.id).await.unwrap();
    let mut f = lc(&store).create(fork).await.unwrap();
    let e = erase_a(&mut f).await;
    let e = f.fx_erase_pass(&scrub_opts()).await.unwrap().into_iter().find(|x| x.id == e.id).unwrap();
    let fork_a = f.get(a_key(3)).await.unwrap();
    f.close().await.unwrap();
    let after = src_runs(Arc::clone(&store)).await;
    let deleted: Vec<&String> = before.difference(&after).collect();
    let r: pair::ForkRef = serde_json::from_slice(&store.get(&pair::fork_ref_path(src, fork)).await.unwrap().bytes().await.unwrap()).unwrap();
    let in_fork: Vec<String> = holders(&store, fork, ERASED).await.into_iter().filter(|p| p.contains("/fx/")).collect();
    let s = lc(&store).wake(src).await.unwrap();
    let mut lost = 0;
    for i in 0..30 {
        if s.get(a_key(i)).await.map(|v| v.is_none()).unwrap_or(true) || s.get(c_key(i)).await.map(|v| v.is_none()).unwrap_or(true) {
            lost += 1;
        }
    }
    s.abandon().await;
    assert!(deleted.is_empty(), "the fork's erasure deleted {} runs of its source: {deleted:?}", deleted.len());
    assert_eq!(lost, 0, "the source lost rows after an erasure in its fork");
    assert!(fork_a.is_none());
    assert!(in_fork.is_empty(), "the fork's own objects still hold the customer: {in_fork:?}");
    assert!(e.done_ms.is_some(), "{e:?}");
    for x in &e.retired {
        if erase::owns(fork, &x.path) {
            assert!(x.deleted_ms.is_some(), "the fork's own replaced run must go: {x:?}");
        } else {
            assert!(x.released_ms.is_some() && x.deleted_ms.is_none(), "a run of the source is released, never deleted: {x:?}");
        }
    }
    assert!(e.retired.iter().any(|x| !erase::owns(fork, &x.path)), "the test must make the fork replace a run of its source: {e:?}");
    assert!(e.retired.iter().all(|x| !r.runs.contains(&x.path)), "the released runs must leave the fork's reference: {:?}", r.runs);
    assert!(!r.runs.is_empty() || before.len() <= e.retired.len(), "only the released runs leave the reference");
    // and a direct delete of a run the database does not own is refused
    let foreign = before.iter().next().unwrap();
    assert!(erase::delete_run(&store, fork, foreign).await.is_err() || control_is("noownership"));
}

// ---- review notes of step 7a ----------------------------------------------------------------------------------------

/// Note 3: a pairing that fails after its checkpoint exists deletes the checkpoint (injected error right after
/// `make()`). Control `keepcp`: the checkpoint stays, as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_pairing_deletes_its_checkpoint() {
    let _s = super::tests::SERIAL.lock().await;
    settings(true);
    E1_WAIT_MS.store(0, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "pair-fail";
    let mut t = lc(&store).create(id).await.unwrap();
    for i in 0..20 {
        put(&mut t, format!("doc/c/{i:05}").as_bytes(), b"a").await;
    }
    publish(&mut t, Reseal::All).await;
    t.close().await.unwrap();
    let admin = slatedb::admin::Admin::builder(crate::lifecycle::tenant_path(id), Arc::clone(&store)).build();
    let n0 = admin.list_checkpoints(None).await.unwrap().len();
    pair::FAIL_AFTER_MAKE.store(true, Relaxed);
    let r = pair::checkpoint_paired(&store, id, || async { Ok(admin.create_detached_checkpoint(&slatedb::config::CheckpointOptions::default()).await?) }).await;
    pair::FAIL_AFTER_MAKE.store(false, Relaxed);
    let left = admin.list_checkpoints(None).await.unwrap();
    let pairs: Vec<ObjectMeta> = store.list(Some(&Path::from(format!("{}/pairs", manifest::root(id))))).try_collect().await.unwrap();
    assert!(r.is_err(), "the injected failure must fail the pairing");
    assert_eq!(left.len(), n0, "a pairing that failed after make() left its checkpoint: {left:?}");
    assert!(pairs.is_empty());
    assert!(!pair::lease_held(&store, id).await.unwrap(), "the lease is released");
}

/// Note 4: an E1 wait below T_pin is refused at startup unless the test flag is set.
#[test]
fn a_short_e1_wait_is_refused_without_the_test_flag() {
    let _s = super::tests::SERIAL.blocking_lock();
    let t_pin = T_PIN_MS.load(Relaxed);
    T_PIN_MS.store(30_000, Relaxed);
    GC_GRACE_MS.store(600_000, Relaxed);
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
    E1_WAIT_MS.store(0, Relaxed);
    E1_WAIT_TEST.store(false, Relaxed);
    let refused = check_config();
    E1_WAIT_TEST.store(true, Relaxed);
    let with_flag = check_config();
    E1_WAIT_TEST.store(false, Relaxed);
    E1_WAIT_MS.store(30_000, Relaxed);
    let equal = check_config();
    E1_WAIT_MS.store(u64::MAX, Relaxed);
    let default = check_config();
    T_PIN_MS.store(t_pin, Relaxed);
    let e = refused.expect_err("an E1 wait of 0 ms with T_pin 30 s must be refused (rule 26)");
    assert!(format!("{e:#}").contains("T_pin"), "{e:#}");
    with_flag.unwrap();
    equal.unwrap();
    default.unwrap();
}

/// A store whose answer to a HEAD of release E1's announcement arrives late: the store serves the request at once (the
/// state it answers is the one before any later write) and the answer reaches the caller `delay` later.
#[derive(Debug)]
struct LateHead {
    inner: Arc<dyn ObjectStore>,
    delay: Duration,
    served: std::sync::Mutex<Option<Instant>>,
}

impl std::fmt::Display for LateHead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LateHead({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for LateHead {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let late = options.head && location.as_ref().ends_with("e1-announced");
        let r = self.inner.get_opts(location, options).await;
        if late {
            *self.served.lock().unwrap() = Some(Instant::now());
            tokio::time::sleep(self.delay).await;
        }
        r
    }
    fn delete_stream(&self, locations: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// Note 5: a follower's T_pin window starts before its HEAD request, not after the answer. The store serves the HEAD
/// before E1's announcement lands and the answer arrives 200 ms later; the writer announces meanwhile. At the request
/// plus T_pin (before the answer plus T_pin), the follower must refuse to read plain. Control `afteranswer`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_follower_window_starts_before_its_head_request() {
    let _s = super::tests::SERIAL.lock().await;
    settings(false);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "late-head";
    let mut w = lc(&store).create(id).await.unwrap();
    put(&mut w, b"doc/c/00001", b"a").await;
    let late = Arc::new(LateHead { inner: Arc::clone(&store), delay: Duration::from_millis(200), served: Default::default() });
    let flc = Lifecycle::new(Arc::new(CountingStore::new(late.clone() as Arc<dyn ObjectStore>)), None);
    let mut f = flc.wake(id).await.unwrap();
    // release E1 starts in this fleet
    crate::envelope::START.store(true, Relaxed);
    let t_req = Instant::now();
    let refresh = tokio::spawn(async move {
        f.fx_refresh().await.unwrap();
        f
    });
    while late.served.lock().unwrap().is_none() {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    // the store answered "not announced"; the writer's next write announces E1 now
    put(&mut w, b"doc/c/00002", b"b").await;
    assert!(manifest::announced(&store, id).await.unwrap());
    let f = refresh.await.unwrap();
    let answered = t_req.elapsed();
    let at = Duration::from_millis(T_PIN + 30);
    tokio::time::sleep(at.saturating_sub(t_req.elapsed())).await;
    let r = f.get(b"doc/c/00001".to_vec()).await;
    crate::envelope::START.store(false, Relaxed);
    f.abandon().await;
    w.abandon().await;
    assert!(answered >= Duration::from_millis(200), "the answer was not late: {answered:?}");
    let e = r.expect_err("past T_pin from its HEAD request the follower must refuse (its answer may predate the announcement)");
    assert!(e.chain().any(|c| c.downcast_ref::<Retry>().is_some()), "{e:#}");
}
