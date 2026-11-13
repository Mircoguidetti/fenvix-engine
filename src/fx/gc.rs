//! GC of our objects (RULES-FOR-THE-CODE rules 17, 19, 23, 30; F6).
//!
//! What a database keeps, decided from the store, never from what a publisher believes (rule 17):
//! - versions: the newest; every version published within the retention window (restore points, 7 days by default);
//!   every version a checkpoint pair (`fx/pairs/*`), a reference (`fx/refs/*`) or a fork's reference (`fx/forks/*`, step 7b) names (and nothing at all is deleted
//!   while a checkpoint holds the drop lease: the lease names no version); and every version from the oldest one this process holds or serves. Other versions are deleted;
//! - runs: every run listed by a kept version, plus the runs a reference lists by path (a fork's first manifest lists
//!   its parent's runs; the parent keeps them through the fork's reference object, rule 30).
//!
//! A run that nothing keeps is deleted only after the grace (rule 23: longer than the hold life plus the PUT timeout,
//! F6), counted both from this process's first sight of it unreferenced (a restart starts the count again) and from
//! the object's own last-modified time (a run just written by a job that has not committed yet is young). GC lists
//! and deletes only under its own database's prefix (rule 30): a fork never deletes its parent's runs.
//!
//! The NVMe copies (R1) are a cache: a copy whose run the newest version does not list is deleted at once.

use super::compactor::Compactor;
use super::manifest::{self, Version};
use anyhow::Result;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Unreferenced runs a GC pass kept because a job running then may have written them (review R1).
pub static KEPT_FOR_RUNNING_JOB: AtomicU64 = AtomicU64::new(0);
/// The store's clock may be ahead of ours: a run up to this much older than a job's start is treated as its output.
const JOB_CLOCK_MARGIN_MS: i64 = 60_000;

#[derive(Default)]
pub struct GcState {
    first_unref: Mutex<HashMap<String, Instant>>,
    /// Runs listed by each version (versions never change).
    listed: Mutex<HashMap<u64, (Vec<String>, u64)>>,
    /// FIX 4: the newest version listed, the versions listed and not loaded yet, versions loaded (diagnostics).
    listed_upto: AtomicU64,
    to_load: Mutex<std::collections::VecDeque<u64>>,
    pub versions_loaded: AtomicU64,
    pub runs_deleted: AtomicU64,
    pub versions_deleted: AtomicU64,
    pub local_deleted: AtomicU64,
    pub passes: AtomicU64,
    /// fix-idle-poll: the last pass was whole and left nothing to delete before `next_due_ms` (wall clock; u64::MAX:
    /// nothing at all), while the database served version `settled_serving`. See [`GcState::idle_skip`].
    settled: std::sync::atomic::AtomicBool,
    next_due_ms: AtomicU64,
    settled_serving: AtomicU64,
}

/// fix-idle-poll: an idle database's GC pass (5 LISTs and 2 to 3 GETs, MEASURED) runs at least this often.
pub const GC_IDLE_EVERY: std::time::Duration = std::time::Duration::from_secs(600);
/// Passes an idle database skipped (serverStatus `sealedRuns.gcPassesSkippedIdle`).
pub static GC_PASSES_SKIPPED_IDLE: AtomicU64 = AtomicU64::new(0);

impl GcState {
    /// fix-idle-poll. A GC pass that is due may be skipped while the database is idle: the last pass was whole, nothing
    /// it kept becomes deletable before now (a version leaving its retention, a run reaching its grace), the version the
    /// database serves is the one it saw, and the last pass ran less than `GC_IDLE_EVERY` ago. Skipping only deletes
    /// later: what a pass keeps never depends on when it runs (references, holds, the drop lease and the grace are read
    /// or checked by the pass itself), so a later pass is as safe as an earlier one; what others change meanwhile (a
    /// fork, a pair, a hold released) is deleted at most `GC_IDLE_EVERY` later. `FENVIX_SLATE_IDLE_POLL_MS=0`: never
    /// skipped (the negative control).
    pub fn idle_skip(&self, since_last: std::time::Duration, serving: u64) -> bool {
        super::drop::slate_idle_poll_ms() > 0
            && self.settled.load(Relaxed)
            && since_last < GC_IDLE_EVERY
            && manifest::now_ms() < self.next_due_ms.load(Relaxed)
            && self.settled_serving.load(Relaxed) == serving
    }

    fn unsettle(&self) {
        self.settled.store(false, Relaxed);
    }
}

pub(crate) fn runs_of(v: &Version) -> Vec<String> {
    v.groups.iter().filter_map(|g| g.sealed.as_ref().map(|s| s.path.clone())).chain(v.delta.iter().map(|d| d.path.clone())).collect()
}

/// The references in the store: versions named and run paths listed.
pub async fn references(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized)) -> Result<(HashSet<u64>, HashSet<String>)> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    let mut versions = HashSet::new();
    let mut runs = HashSet::new();
    let root = manifest::root(id);
    let mut objs = Vec::new();
    // integration-3: step 7b writes a fork's reference at `fx/forks/{fork}` (`pair::ForkRef`: "version" and "runs")
    for d in ["pairs", "refs", "forks"] {
        let metas: Vec<_> = store.list(Some(&Path::from(format!("{root}/{d}")))).try_collect().await?;
        objs.extend(metas.into_iter().map(|m| m.location));
    }
    for p in objs {
        let b = match store.get(&p).await {
            Ok(r) => r.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(e) => return Err(e.into()),
        };
        let v: serde_json::Value = serde_json::from_slice(&b)?;
        if let Some(n) = v.get("version").and_then(|x| x.as_u64()) {
            versions.insert(n);
        }
        if let Some(a) = v.get("runs").and_then(|x| x.as_array()) {
            runs.extend(a.iter().filter_map(|x| x.as_str().map(String::from)));
        }
    }
    Ok((versions, runs))
}

/// Versions GC loads per pass at most (a restart with a week of versions spreads its loads over passes).
pub const GC_LOAD_PER_PASS: usize = 200;

/// One GC pass of a database.
pub async fn gc_once(c: &Compactor) -> Result<()> {
    let store = &c.store;
    let id = c.id();
    let st = &c.gc;
    // FIX 4 of the review: list only after the newest version listed so far, and load at most `GC_LOAD_PER_PASS` new
    // versions per pass; until every listed version is loaded, this pass deletes nothing (GC must know every run a
    // retained version lists). The versions are then the loaded ones (`listed`), minus those GC itself deleted.
    let fresh = manifest::list_versions_after(store, id, st.listed_upto.load(Relaxed)).await?;
    if let Some(&m) = fresh.last() {
        st.listed_upto.fetch_max(m, Relaxed);
    }
    st.to_load.lock().unwrap().extend(fresh);
    let mut loaded_now = 0;
    loop {
        let next = st.to_load.lock().unwrap().pop_front();
        let Some(n) = next else { break };
        match manifest::load(store, id, n).await {
            Ok(v) => {
                st.listed.lock().unwrap().insert(n, (runs_of(&v), v.published_ms));
                st.versions_loaded.fetch_add(1, Relaxed);
            }
            Err(e) if is_not_found(&e) || CONTROL_GC_ERRORS_AS_DELETED.load(Relaxed) => {} // deleted meanwhile
            Err(e) => {
                // a transient error is not a deletion: its runs would count as unreferenced (review of 7f93591)
                st.to_load.lock().unwrap().push_front(n);
                eprintln!("GC of {id}: version {n} could not be read ({e:#}); this pass is skipped");
                VERSION_READ_ERRORS.fetch_add(1, Relaxed);
                return Err(e);
            }
        }
        loaded_now += 1;
        if loaded_now >= GC_LOAD_PER_PASS {
            break;
        }
    }
    if !st.to_load.lock().unwrap().is_empty() {
        st.unsettle();
        st.passes.fetch_add(1, Relaxed);
        return Ok(());
    }
    let versions: Vec<u64> = {
        let mut v: Vec<u64> = st.listed.lock().unwrap().keys().copied().collect();
        v.sort_unstable();
        v
    };
    let Some(&newest) = versions.last() else { return Ok(()) };
    // rule 19: while a checkpoint holds the drop lease, the version it will pair with is not named anywhere yet (the lease
    // names none, step 7a's format): GC deletes nothing until the lease ends (review of step 4, FIX 1)
    if super::pair::lease_held(store, id).await? {
        // fix-idle-poll: nothing can be deleted while the lease is held; an idle database looks again when a lease
        // left by a holder that died would have expired (its life), not at every `gc_every` (MEASURED in sealed2-derive,
        // `gc_every` 5 s: an idle database read the lease and listed the versions every 5 s for good)
        st.next_due_ms.store(manifest::now_ms().saturating_add(super::pair::LEASE_LIFE_MS.load(Relaxed)), Relaxed);
        st.settled_serving.store(c.sh.serving_version.load(Relaxed), Relaxed);
        st.settled.store(true, Relaxed);
        st.passes.fetch_add(1, Relaxed);
        return Ok(());
    }
    st.unsettle();
    let serving_at_start = c.sh.serving_version.load(Relaxed);
    // fix-idle-poll: the earliest wall-clock time something this pass keeps can become deletable
    let mut next_due = u64::MAX;
    let (ref_versions, ref_runs) = references(store, id).await?;
    let retention = c.knobs.version_retention.as_millis() as u64;
    let now_ms = manifest::now_ms();
    // the oldest version this process still holds or serves: it and every newer one stay
    let mut floor_v = newest;
    // review of e3b04c5 (rule 8): an older writer's late version at the newest slot is not what readers serve
    // (`manifest::load_newest_trusted`): the version they serve, and every newer one (the comparison that tells a late
    // version needs the version below it), stay with their runs
    floor_v = floor_v.min(trusted_floor(store, id, newest).await?);
    if let Some(h) = c.sh.holds.oldest_version() {
        floor_v = floor_v.min(h);
    }
    if let Some(p) = c.sh.pending_version() {
        floor_v = floor_v.min(p);
    }
    let serving = c.sh.serving_version.load(Relaxed);
    if serving != u64::MAX {
        floor_v = floor_v.min(serving);
    }
    let mut referenced: HashSet<String> = ref_runs;
    for &n in &versions {
        let Some((runs, stamp)) = st.listed.lock().unwrap().get(&n).cloned() else { continue };
        let young = retention > 0 && now_ms.saturating_sub(stamp) < retention;
        if young && n < floor_v && !ref_versions.contains(&n) {
            next_due = next_due.min(stamp.saturating_add(retention));
        }
        if n >= floor_v || ref_versions.contains(&n) || young {
            referenced.extend(runs);
            continue;
        }
        // a restore point past its retention (or retention 0) that nothing names: the version object goes
        match store.delete(&manifest::manifest_path(id, n)).await {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => {
                st.versions_deleted.fetch_add(1, Relaxed);
                st.listed.lock().unwrap().remove(&n);
            }
            Err(e) => return Err(e.into()),
        }
    }
    // runs under this database's own prefix only (rule 30)
    let run_dir = Path::from(format!("{}/run", manifest::root(id)));
    let metas: Vec<_> = store.list(Some(&run_dir)).try_collect().await?;
    let grace = c.knobs.gc_grace;
    let job_floor = c.oldest_job_start_ms();
    let mut fu = st.first_unref.lock().unwrap().clone();
    let mut doomed = Vec::new();
    let mut live_unref = HashSet::new();
    for m in metas {
        let p = m.location.to_string();
        if referenced.contains(&p) {
            continue;
        }
        live_unref.insert(p.clone());
        let first = *fu.entry(p.clone()).or_insert_with(Instant::now);
        let age = std::time::Duration::from_millis((manifest::now_ms() as i64 - m.last_modified.timestamp_millis()).max(0) as u64);
        // review R1: a run written since the oldest running job started may be that job's output, not published yet (a
        // reseal publishes all its split outputs at its commit; slowed by the CPU share, a job can outlast the grace after
        // its first output): kept until the job ends. A minute of margin for the store's clock against ours.
        if let Some(t) = job_floor {
            if m.last_modified.timestamp_millis() + JOB_CLOCK_MARGIN_MS >= t as i64 {
                KEPT_FOR_RUNNING_JOB.fetch_add(1, Relaxed);
                next_due = 0;
                continue;
            }
        }
        if first.elapsed() >= grace && age >= grace {
            doomed.push(m.location);
        } else {
            let wait = grace.saturating_sub(first.elapsed()).max(grace.saturating_sub(age));
            next_due = next_due.min(manifest::now_ms().saturating_add(wait.as_millis() as u64));
        }
    }
    fu.retain(|p, _| live_unref.contains(p));
    *st.first_unref.lock().unwrap() = fu;
    for p in doomed {
        match store.delete(&p).await {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => {
                st.runs_deleted.fetch_add(1, Relaxed);
                st.first_unref.lock().unwrap().remove(&p.to_string());
            }
            Err(e) => return Err(e.into()),
        }
    }
    // NVMe copies: only what the newest version and the versions in use list
    if let Some(local) = super::local::store() {
        let in_use: HashSet<String> = {
            let l = st.listed.lock().unwrap();
            l.iter().filter(|(n, _)| **n >= floor_v).flat_map(|(_, (r, _))| r.iter().cloned()).collect()
        };
        let metas: Vec<_> = local.list(Some(&run_dir)).try_collect().await.unwrap_or_default();
        for m in metas {
            // a young copy may belong to a version published after this pass listed the versions
            let age = std::time::Duration::from_millis((manifest::now_ms() as i64 - m.last_modified.timestamp_millis()).max(0) as u64);
            if !in_use.contains(&m.location.to_string()) {
                if age >= grace {
                    if local.delete(&m.location).await.is_ok() {
                        st.local_deleted.fetch_add(1, Relaxed);
                    } else {
                        next_due = 0;
                    }
                } else {
                    next_due = next_due.min(manifest::now_ms().saturating_add(grace.saturating_sub(age).as_millis() as u64));
                }
            }
        }
    }
    st.next_due_ms.store(next_due, Relaxed);
    st.settled_serving.store(serving_at_start, Relaxed);
    st.settled.store(true, Relaxed);
    st.passes.fetch_add(1, Relaxed);
    Ok(())
}


/// Whether an error loading a version means it is gone (and only that): any other error skips the GC pass.
fn is_not_found(e: &anyhow::Error) -> bool {
    e.downcast_ref::<object_store::Error>().is_some_and(|e| matches!(e, object_store::Error::NotFound { .. }))
}

/// Negative control of the tests: every error loading a version counts as "deleted" (the code before the review of
/// 7f93591).
pub static CONTROL_GC_ERRORS_AS_DELETED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
/// GC passes (live and closed) skipped because a version could not be read (serverStatus `gcVersionReadErrors`).
pub static VERSION_READ_ERRORS: AtomicU64 = AtomicU64::new(0);

/// Negative control of the tests: the GC keeps from the plain newest version (the code before the review of e3b04c5).
pub static CONTROL_GC_PLAIN_NEWEST: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The lowest version the live GC must keep for readers: the version `manifest::trusted_at` keeps when the newest is an
/// older writer's late version (otherwise the newest itself).
async fn trusted_floor(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized), newest: u64) -> Result<u64> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    if CONTROL_GC_PLAIN_NEWEST.load(Relaxed) {
        return Ok(newest);
    }
    match manifest::load(store, id, newest).await {
        Ok(v) => Ok(manifest::trusted_at(store, id, v).await?.version),
        Err(e) if is_not_found(&e) || CONTROL_GC_ERRORS_AS_DELETED.load(Relaxed) => Ok(newest), // a newer version was published meanwhile: the next pass
        Err(e) => {
            eprintln!("GC of {id}: version {newest} could not be read ({e:#}); this pass is skipped");
                VERSION_READ_ERRORS.fetch_add(1, Relaxed);
            Err(e)
        }
    }
}

/// GC of the runs of a database this process closed (P-perf-bytes). The live GC above runs in the compactor, which stops
/// when the database closes (60 s idle by default): runs retired within the grace before the close stayed in the store
/// until the next open, for an idle customer for good (MEASURED: the medium bench kept 0.58 GB of retired runs for
/// 1.07 GB logical after its database closed). Called by the maintenance pass of a closed tenant, under its lock.
///
/// The same rules as [`gc_once`], decided from the store only (rule 17): it keeps the newest version, every version a
/// pair, a reference or a fork names, and every version inside the retention window, and deletes nothing while a
/// checkpoint holds the drop lease (rule 19). A run that no kept version or reference lists is deleted only once the
/// newest version was published at least the GC grace ago (every retired run was retired by that publish or an earlier
/// one, so it has been unreferenced at least that long: the grace of rule 23, F6) and the object itself is that old. A
/// process that opened the database meanwhile reads the newest version or a newer one, whose runs are all kept. Returns
/// how long to wait before calling again when something is left for later.
pub async fn gc_closed(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized)) -> Result<Option<std::time::Duration>> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    use std::time::Duration;
    let versions = manifest::list_versions(store, id).await?;
    let Some(&newest_n) = versions.last() else { return Ok(None) };
    if super::pair::lease_held(store, id).await? {
        return Ok(Some(Duration::from_secs(60)));
    }
    let newest = match manifest::load(store, id, newest_n).await {
        Ok(v) => v,
        Err(_) => return Ok(Some(Duration::from_secs(60))), // a newer one was published meanwhile: next time
    };
    let (ref_versions, ref_runs) = references(store, id).await?;
    let retention = super::VERSION_RETENTION_MS.load(Relaxed);
    let grace = super::GC_GRACE_MS.load(Relaxed);
    let now_ms = manifest::now_ms();
    let mut referenced: HashSet<String> = ref_runs;
    referenced.extend(runs_of(&newest));
    // review of e3b04c5 (rule 8): the version an opening serves when the newest is an older writer's late version, and
    // every version above it, stay with their runs (see `gc_once`)
    let keep_from = if CONTROL_GC_PLAIN_NEWEST.load(Relaxed) { newest_n } else { manifest::trusted_at(store, id, newest.clone()).await?.version };
    for &n in &versions {
        if n == newest_n {
            continue;
        }
        if n >= keep_from {
            match manifest::load(store, id, n).await {
                Ok(v) => referenced.extend(runs_of(&v)),
                Err(e) if is_not_found(&e) || CONTROL_GC_ERRORS_AS_DELETED.load(Relaxed) => {}
                Err(e) => {
                    eprintln!("GC of closed {id}: version {n} could not be read ({e:#}); this pass is skipped");
                VERSION_READ_ERRORS.fetch_add(1, Relaxed);
                    return Err(e);
                }
            }
            continue;
        }
        let named = ref_versions.contains(&n);
        if named || retention > 0 {
            match manifest::load(store, id, n).await {
                Ok(v) if named || now_ms.saturating_sub(v.published_ms) < retention => {
                    referenced.extend(runs_of(&v));
                    continue;
                }
                Ok(_) => {}
                Err(e) if is_not_found(&e) || CONTROL_GC_ERRORS_AS_DELETED.load(Relaxed) => continue, // deleted meanwhile
                // a transient error is not a deletion: the runs of a named or retained version would be deleted with it
                // (review of 7f93591; at 1 TB, S3 errors are normal)
                Err(e) => {
                    eprintln!("GC of closed {id}: version {n} could not be read ({e:#}); this pass is skipped");
                VERSION_READ_ERRORS.fetch_add(1, Relaxed);
                    return Err(e);
                }
            }
        }
        match store.delete(&manifest::manifest_path(id, n)).await {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => {
                CLOSED_VERSIONS_DELETED.fetch_add(1, Relaxed);
            }
            Err(e) => return Err(e.into()),
        }
    }
    let run_dir = Path::from(format!("{}/run", manifest::root(id)));
    let metas: Vec<_> = store.list(Some(&run_dir)).try_collect().await?;
    let since_publish = now_ms.saturating_sub(newest.published_ms);
    let mut wait = 0u64;
    for m in metas {
        let p = m.location.to_string();
        if referenced.contains(&p) {
            continue;
        }
        let age = (now_ms as i64 - m.last_modified.timestamp_millis()).max(0) as u64;
        let left = grace.saturating_sub(since_publish.min(age));
        if left > 0 {
            wait = wait.max(left);
            continue;
        }
        match store.delete(&m.location).await {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => {
                CLOSED_RUNS_DELETED.fetch_add(1, Relaxed);
            }
            Err(e) => return Err(e.into()),
        }
    }
    CLOSED_PASSES.fetch_add(1, Relaxed);
    Ok((wait > 0).then(|| Duration::from_millis(wait + 1000)))
}

/// Passes of [`gc_closed`], runs and versions it deleted (diagnostics, serverStatus `fenvix.fxGcClosed`).
pub static CLOSED_PASSES: AtomicU64 = AtomicU64::new(0);
pub static CLOSED_RUNS_DELETED: AtomicU64 = AtomicU64::new(0);
pub static CLOSED_VERSIONS_DELETED: AtomicU64 = AtomicU64::new(0);
