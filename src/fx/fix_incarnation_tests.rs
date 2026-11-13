//! fix-incarnation (2026-11-13): a late write of an old incarnation never lands in a database created again.
//!
//! Before this fix every path was computed at each call from a process-wide incarnation table, which an existence check,
//! the admin erase handler or a later lease acquisition could move. Now a writer captures the incarnation it opened with
//! (`Tenant::id`, a `DbAt`), its builder, compactor, erasure and generator use that captured value, and only the lease
//! holder moves the table. Each test moves the table under a live writer of incarnation 0 (what the lease acquisition
//! does when this process takes the name again after it was dropped and created again elsewhere) and checks that the
//! writer's next writes stay under the old prefix. The control (`lifecycle::CONTROL_TABLE_PATHS`) makes every path follow
//! the table again, as before the fix, and must show writes under the new prefix: the damage the fix removes.

use super::build::{GroupWriter, Reseal};
use super::compactor::Compactor;
use super::compactor_tests::{flush, setup, teardown, test_knobs};
use super::tests::*;
use super::*;
use crate::lifecycle::{set_incarnation, DbAt, CONTROL_TABLE_PATHS};
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

/// Every object under the new incarnation's prefix (`tenants/{id}/i1/`).
async fn under_i1(store: &Arc<dyn ObjectStore>, id: &str) -> Vec<String> {
    store.list(Some(&Path::from(format!("tenants/{id}/i1")))).map_ok(|m| m.location.to_string()).try_collect().await.unwrap()
}

struct ControlGuard;
impl ControlGuard {
    fn set(on: bool) -> ControlGuard {
        CONTROL_TABLE_PATHS.store(on, Relaxed);
        ControlGuard
    }
}
impl Drop for ControlGuard {
    fn drop(&mut self) {
        CONTROL_TABLE_PATHS.store(false, Relaxed);
    }
}

/// The builder's publish (runs and version create), SlateDB's WAL and flushes, and the clean marker of the close: a
/// writer of incarnation 0 keeps all of them under `tenants/{id}/` after the table moved to 1, and its data reads back
/// from incarnation 0 in full.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fi_builder_publish_wal_and_close_stay_in_the_opened_incarnation() {
    let _s = SERIAL.lock().await;
    enable();
    view::clear_cache();
    for control in [false, true] {
        let _c = ControlGuard::set(control);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = format!("fi-build-{control}");
        let id = id.as_str();
        set_incarnation(id, 0);
        let mut t = lc(&store).create(id).await.unwrap();
        assert_eq!(t.id(), &DbAt::new(id, 0));
        let mut m = BTreeMap::new();
        for i in 0..40 {
            apply(&mut t, &mut m, vec![(ckey(0, i), Some(format!("a-{i}").into_bytes()))]).await;
        }
        publish(&mut t, &small_opts(Reseal::All)).await;
        // the table moves under the live writer
        set_incarnation(id, 1);
        for i in 0..40 {
            apply(&mut t, &mut m, vec![(ckey(1, i), Some(format!("b-{i}").into_bytes()))]).await;
        }
        let r = t.fx_publish(&small_opts(Reseal::All), None).await;
        let _ = t.close().await;
        let stray = under_i1(&store, id).await;
        eprintln!("builder/WAL/close, control {control}: publish after the move {:?}; objects under i1/: {}", r.as_ref().map(|x| x.as_ref().map(|r| r.version)).map_err(|e| format!("{e:#}")), stray.len());
        if control {
            assert!(!stray.is_empty(), "control: with paths following the table the writer must write under i1/ (the damage)");
        } else {
            assert!(r.is_ok(), "the writer's publish after the move: {:?}", r.err());
            assert!(stray.is_empty(), "a writer of incarnation 0 wrote under i1/: {stray:?}");
            // incarnation 0 holds every row, as any process reads it
            set_incarnation(id, 0);
            let mut t0 = lc(&store).wake(id).await.unwrap();
            let mut wrong = 0;
            for (k, v) in &m {
                if t0.get(k).await.unwrap().as_deref() != Some(&v[..]) {
                    wrong += 1;
                }
            }
            let _ = t0.close().await;
            assert_eq!(wrong, 0, "rows of incarnation 0 read wrong after the table moved");
        }
        set_incarnation(id, 0);
    }
}

/// The compactor's commits (absorption, version creates and its GC): started on a writer of incarnation 0, it keeps
/// every object under `tenants/{id}/` after the table moved to 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fi_compactor_commits_stay_in_the_opened_incarnation() {
    let _s = SERIAL.lock().await;
    setup();
    for control in [false, true] {
        let _c = ControlGuard::set(control);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = format!("fi-compactor-{control}");
        let id = id.as_str();
        set_incarnation(id, 0);
        let mut t = lc(&store).create(id).await.unwrap();
        let mut m = BTreeMap::new();
        for i in 0..300 {
            apply(&mut t, &mut m, vec![(ckey(i % 7, i), Some(vec![b'v'; 40]))]).await;
        }
        publish(&mut t, &small_opts(Reseal::All)).await;
        let c = Compactor::start(&mut t, Arc::clone(&store), test_knobs(false), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
        c.run();
        let before = manifest::list_versions(&store, &DbAt::new(id, 0)).await.unwrap();
        // the table moves under the live writer and its compactor
        set_incarnation(id, 1);
        for i in 0..300 {
            apply(&mut t, &mut m, vec![(ckey(i % 7, 1000 + i), Some(vec![b'n'; 40]))]).await;
        }
        flush(&t).await;
        let d = c.drain(true).await;
        t.fx_sync();
        c.stop().await;
        t.abandon().await;
        let after_i0 = manifest::list_versions(&store, &DbAt::new(id, 0)).await.unwrap();
        let stray = under_i1(&store, id).await;
        eprintln!("compactor, control {control}: drain {:?}; versions of i0 before {before:?} after {after_i0:?}; objects under i1/: {}", d.as_ref().err().map(|e| format!("{e:#}")), stray.len());
        if control {
            // the damage: the compactor writes under i1/, or reads its own SSTs and versions there (its jobs then fail on a
            // prefix that is not its database's; MEASURED first run: the absorb job read `i1/compacted/...`)
            let misread = d.as_ref().err().is_some_and(|e| format!("{e:#}").contains(&format!("tenants/{id}/i1/")));
            assert!(!stray.is_empty() || misread, "control: with paths following the table the compactor must write or read under i1/ (the damage)");
        } else {
            assert!(after_i0.last() > before.last(), "the compactor committed no version after the move (the test shows nothing): {before:?} {after_i0:?}");
            assert!(stray.is_empty(), "a compactor of incarnation 0 wrote under i1/: {stray:?}");
        }
        set_incarnation(id, 0);
    }
    teardown();
}

/// The step 9 generator's runs (`GroupWriter`, the ballast path): a writer created with incarnation 0 writes its runs
/// under `tenants/{id}/` after the table moved to 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fi_generator_runs_stay_in_the_opened_incarnation() {
    let _s = SERIAL.lock().await;
    enable();
    for control in [false, true] {
        let _c = ControlGuard::set(control);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = format!("fi-gen-{control}");
        let id = id.as_str();
        set_incarnation(id, 0);
        let mut gw = GroupWriter::new(&store, &DbAt::new(id, 0), 10, Some(1), &small_opts(Reseal::All), 1, true);
        set_incarnation(id, 1);
        for i in 0..200 {
            gw.add(&ckey(3, i), 10, &crate::runfmt::live(format!("g-{i}-{}", "x".repeat(40)).as_bytes())).await.unwrap();
        }
        let groups = gw.finish().await.unwrap();
        let paths: Vec<String> = groups.iter().filter_map(|g| g.sealed.as_ref().map(|r| r.path.clone())).collect();
        let stray = under_i1(&store, id).await;
        eprintln!("generator, control {control}: {} runs {:?}; objects under i1/: {}", paths.len(), paths.first(), stray.len());
        assert!(!paths.is_empty());
        if control {
            assert!(!stray.is_empty(), "control: with paths following the table the generator must write under i1/");
        } else {
            assert!(stray.is_empty(), "a generator of incarnation 0 wrote under i1/: {stray:?}");
            assert!(paths.iter().all(|p| p.starts_with(&format!("tenants/{id}/fx/run/"))), "{paths:?}");
        }
        set_incarnation(id, 0);
    }
}

/// The read-only lookups (`db_exists`, used by listDatabases and the admin API's existence check; `read_incarnation`,
/// used by the admin erase handler before it holds the lease) never move this process's table. Control: the lease
/// holder's `load_incarnation` does move it (what the lookups called before this fix).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fi_read_only_lookups_leave_the_table_alone() {
    let _s = SERIAL.lock().await;
    enable();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fi-lookups";
    set_incarnation(id, 0);
    // another process dropped the name and created it again: the store says incarnation 1, with data under i1/
    assert_eq!(crate::lifecycle::bump_incarnation(&store, id).await.unwrap(), 1);
    let t = lc(&store).create(id).await.unwrap();
    assert_eq!(t.id(), &DbAt::new(id, 1));
    let _ = t.close().await;
    store.put(&Path::from(format!("tenants/{id}/i1/fx/marker-for-the-test")), object_store::PutPayload::from_static(b"x")).await.unwrap();
    // this process never took the lease again: its table still says 0
    set_incarnation(id, 0);
    assert!(crate::lifecycle::db_exists(&store, id).await.unwrap());
    assert_eq!(crate::lifecycle::incarnation(id), 0, "db_exists moved the table");
    assert_eq!(crate::lifecycle::read_incarnation(&store, id).await.unwrap(), 1);
    assert_eq!(crate::lifecycle::incarnation(id), 0, "read_incarnation moved the table");
    // control: the lease holder's read moves it
    assert_eq!(crate::lifecycle::load_incarnation(&store, id).await.unwrap(), 1);
    assert_eq!(crate::lifecycle::incarnation(id), 1);
    set_incarnation(id, 0);
}
