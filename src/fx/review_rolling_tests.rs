//! R-rolling (reviewer, 2026-11-13): audit-fencing item 5 (19502ab).
//!
//! `lifecycle::db_exists` calls `load_incarnation`, which also *sets* the process-wide incarnation map that every later
//! `tenant_path(id)` of this process reads (fx manifest and run paths are computed at each call, `manifest::root`). Since
//! 19502ab, listDatabases and `meta::exists` (the admin API's existence check) call it for every name, in any process,
//! lease or not. A process that lost a database (its publish in flight) re-points its late publish into the NEW
//! incarnation's prefix as soon as anything in it lists databases. The slot fence (base + 1) does not stop it: the new
//! incarnation's newest version is lower than the old owner's base.

use super::build::Reseal;
use super::tests::*;
use super::*;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Duration;

struct Outcome {
    versions_new: Vec<u64>,
    stale_versions_in_new: usize,
    wrong: usize,
    errors: usize,
    old_rows_seen: usize,
    open_err: Option<String>,
}

async fn scenario(check_in_old_process: bool, new_rows: usize) -> Outcome {
    scenario_with(check_in_old_process, false, new_rows).await
}

/// `via_load`: the old process reads the incarnation the way the admin erase handler does first (`ctl/erase.rs:34`,
/// before any lock or lease; present since audit-fencing round 2, so also on integration-6 0ed53558).
async fn scenario_with(check_in_old_process: bool, via_load: bool, new_rows: usize) -> Outcome {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = format!("rr-inc-{check_in_old_process}-{via_load}-{new_rows}");
    let id = id.as_str();
    crate::lifecycle::set_incarnation(id, 0);
    // the old owner A, incarnation 0: three publishes (versions 1..3)
    let mut t1 = lc(&store).create(id).await.unwrap();
    let mut m_old = BTreeMap::new();
    for r in 0..3 {
        for i in 0..40 {
            apply(&mut t1, &mut m_old, vec![(ckey(0, r * 40 + i), Some(format!("old-{r}-{i}").into_bytes()))]).await;
        }
        publish(&mut t1, &small_opts(Reseal::All)).await;
    }
    for i in 0..40 {
        apply(&mut t1, &mut m_old, vec![(ckey(0, 200 + i), Some(format!("old-late-{i}").into_bytes()))]).await;
    }
    // A's next publish is slow right before its create (a slow store answer, a GC pause); its runs are written
    build::PAUSE_BEFORE_CREATE_MS.store(3000, Relaxed);
    let h = tokio::spawn(async move {
        let r = t1.fx_publish(&small_opts(Reseal::All), None).await;
        (t1, r)
    });
    tokio::time::sleep(Duration::from_millis(1200)).await;
    build::PAUSE_BEFORE_CREATE_MS.store(0, Relaxed);
    // process B (A lost the lease): dropDatabase / erase moves the name to incarnation 1 and deletes the old prefix, then
    // the same name is created again with new data and published once (version 1 of incarnation 1)
    assert_eq!(crate::lifecycle::bump_incarnation(&store, id).await.unwrap(), 1);
    let old: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(store.list(Some(&Path::from(format!("tenants/{id}"))))).await.unwrap();
    for o in old.iter().filter(|o| !o.location.as_ref().contains("/i1/")) {
        let _ = store.delete(&o.location).await;
    }
    let mut t2 = lc(&store).create(id).await.unwrap();
    let mut m_new = BTreeMap::new();
    for i in 0..new_rows {
        apply(&mut t2, &mut m_new, vec![(ckey(5, i), Some(format!("new-{i}").into_bytes()))]).await;
    }
    publish(&mut t2, &small_opts(Reseal::All)).await;
    t2.close().await.unwrap();
    // back in A's process: its map says incarnation 0 (A never took the lease again)
    crate::lifecycle::set_incarnation(id, 0);
    if check_in_old_process {
        // what listDatabases (dbs.rs) and meta::exists (admin API) now do in A's process, for every name
        if via_load {
            assert_eq!(crate::lifecycle::load_incarnation(&store, id).await.unwrap(), 1);
        } else {
            assert!(crate::lifecycle::db_exists(&store, id).await.unwrap());
        }
    }
    let (t1, r) = h.await.unwrap();
    eprintln!("A's late publish (existence check in A: {check_in_old_process}): {:?}", r.as_ref().map(|x| x.as_ref().map(|r| r.version)).map_err(|e| format!("{e:#}")));
    let _ = tokio::time::timeout(Duration::from_secs(15), t1.abandon()).await;
    // the new incarnation, as any process reads it
    crate::lifecycle::set_incarnation(id, 1);
    let versions_new = manifest::list_versions(&store, id).await.unwrap();
    let mut stale_versions_in_new = 0;
    for n in &versions_new {
        let v = manifest::load(&store, id, *n).await.unwrap();
        let runs: Vec<&str> = v.groups.iter().filter_map(|g| g.sealed.as_ref().map(|r| r.path.as_str())).chain(v.delta.iter().map(|r| r.path.as_str())).collect();
        if runs.iter().any(|p| !p.contains("/i1/")) {
            stale_versions_in_new += 1;
        }
    }
    let (mut wrong, mut errors, mut old_rows_seen, mut open_err) = (0, 0, 0, None);
    match lc(&store).wake(id).await {
        Ok(mut t3) => {
            if let Err(e) = t3.upgrade().await {
                open_err = Some(format!("upgrade: {e:#}"));
            }
            for (k, want) in &m_new {
                match t3.get(k).await {
                    Ok(v) if v.as_deref() == Some(&want[..]) => {}
                    Ok(_) => wrong += 1,
                    Err(_) => errors += 1,
                }
            }
            for k in m_old.keys() {
                if let Ok(Some(_)) = t3.get(k).await {
                    old_rows_seen += 1;
                }
            }
            let _ = t3.close().await;
        }
        Err(e) => open_err = Some(format!("{e:#}")),
    }
    eprintln!(
        "incarnation 1 after A's late publish (check in A: {check_in_old_process}): versions {versions_new:?}, versions naming runs outside i1/ {stale_versions_in_new}; \
         new rows wrong {wrong}, errors {errors} of {}; erased rows read back {old_rows_seen} of {}; open error {open_err:?}",
        m_new.len(),
        m_old.len()
    );
    crate::lifecycle::set_incarnation(id, 0);
    Outcome { versions_new, stale_versions_in_new, wrong, errors, old_rows_seen, open_err }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rr_existence_check_repoints_a_stale_publish_into_the_new_incarnation() {
    let _s = SERIAL.lock().await;
    enable();
    view::clear_cache();
    // baseline: nothing in A reads the incarnation, its late publish lands under the old prefix (harmless)
    let base = scenario(false, 30).await;
    // listDatabases / meta::exists in A's process before its late publish lands
    let hit = scenario(true, 30).await;
    // the same, with a busier new database (its seqs pass the old owner's)
    let busy = scenario(true, 40).await;
    eprintln!("busy: versions {:?} stale {} wrong {} errors {} old rows {} open {:?}", busy.versions_new, busy.stale_versions_in_new, busy.wrong, busy.errors, busy.old_rows_seen, busy.open_err);
    build::PAUSE_BEFORE_CREATE_MS.store(0, Relaxed);
    assert_eq!(base.stale_versions_in_new, 0, "baseline: the stale publish must stay out of the new incarnation (versions {:?})", base.versions_new);
    assert_eq!((base.wrong, base.errors, base.old_rows_seen), (0, 0, 0), "baseline broken: {:?}", base.open_err);
    assert_eq!(hit.stale_versions_in_new, 0, "a process that lost the database published a version into the new incarnation (versions {:?})", hit.versions_new);
    assert_eq!((hit.wrong, hit.errors, hit.old_rows_seen), (0, 0, 0), "the new incarnation reads wrong (open error {:?})", hit.open_err);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rr_erase_handler_load_repoints_a_stale_publish_into_the_new_incarnation() {
    let _s = SERIAL.lock().await;
    enable();
    view::clear_cache();
    let hit = scenario_with(true, true, 40).await;
    build::PAUSE_BEFORE_CREATE_MS.store(0, Relaxed);
    assert_eq!(hit.stale_versions_in_new, 0, "a process that lost the database published a version into the new incarnation (versions {:?})", hit.versions_new);
    assert_eq!((hit.wrong, hit.errors, hit.old_rows_seen), (0, 0, 0), "the new incarnation reads wrong (open error {:?})", hit.open_err);
}

/// R-rolling, fix-incarnation review: the background sweep of old incarnations (`lease.rs`, run after a lease acquisition
/// that read incarnation k) deletes everything under the name except `i{k}/`, without reading the store's incarnation
/// again and without the lease. Process P took the lease at k = 1 and its sweep is delayed (a pause); P loses the lease;
/// process Q drops the name (k = 2) and creates it again, with acknowledged writes. P's sweep then runs with k = 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rr_delayed_sweep_deletes_the_newer_incarnation() {
    let _s = SERIAL.lock().await;
    enable();
    view::clear_cache();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "rr-sweep";
    // incarnation 1 exists (P took its lease and read k = 1)
    crate::lifecycle::set_incarnation(id, 0);
    assert_eq!(crate::lifecycle::bump_incarnation(&store, id).await.unwrap(), 1);
    crate::lifecycle::set_incarnation(id, 1);
    let mut t1 = lc(&store).create(id).await.unwrap();
    let mut m1 = BTreeMap::new();
    for i in 0..20 {
        apply(&mut t1, &mut m1, vec![(ckey(0, i), Some(b"inc1".to_vec()))]).await;
    }
    t1.close().await.unwrap();
    // Q: drop (k = 2), the same name created again, acknowledged writes, published
    assert_eq!(crate::lifecycle::bump_incarnation(&store, id).await.unwrap(), 2);
    crate::lifecycle::set_incarnation(id, 2);
    let mut t2 = lc(&store).create(id).await.unwrap();
    let mut m2 = BTreeMap::new();
    for i in 0..50 {
        apply(&mut t2, &mut m2, vec![(ckey(1, i), Some(format!("inc2-{i}").into_bytes()))]).await;
    }
    publish(&mut t2, &small_opts(Reseal::All)).await;
    t2.close().await.unwrap();
    let under_i2 = |s: Arc<dyn ObjectStore>| async move {
        futures::TryStreamExt::try_collect::<Vec<_>>(s.list(Some(&Path::from(format!("tenants/{id}/i2"))))).await.unwrap().len()
    };
    let before = under_i2(Arc::clone(&store)).await;
    // P's delayed sweep, with the k its lease acquisition read
    let swept = crate::lifecycle::sweep_old_incarnations(&store, id, 1).await.unwrap();
    let after = under_i2(Arc::clone(&store)).await;
    // the current owner reads its acknowledged writes
    crate::lifecycle::set_incarnation(id, 2);
    let (mut wrong, mut errors, mut open_err) = (0, 0, None);
    match lc(&store).wake(id).await {
        Ok(t3) => {
            for (k, want) in &m2 {
                match t3.get(k).await {
                    Ok(v) if v.as_deref() == Some(&want[..]) => {}
                    Ok(_) => wrong += 1,
                    Err(_) => errors += 1,
                }
            }
            let _ = t3.close().await;
        }
        Err(e) => open_err = Some(format!("{e:#}")),
    }
    crate::lifecycle::set_incarnation(id, 0);
    eprintln!("delayed sweep with k = 1 after the name moved to 2: swept {swept}; objects under i2/ {before} -> {after}; incarnation 2 reads: wrong {wrong}, errors {errors} of {}, open error {open_err:?}", m2.len());
    assert_eq!(after, before, "a delayed sweep deleted objects of the current incarnation");
    assert_eq!((wrong, errors, open_err), (0, 0, None), "acknowledged writes of the current incarnation lost");
}
