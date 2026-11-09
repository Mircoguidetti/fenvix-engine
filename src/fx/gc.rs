//! GC of our objects (RULES-FOR-THE-CODE rules 17, 19, 23, 30; F6).
//!
//! What a database keeps, decided from the store, never from what a publisher believes (rule 17):
//! - versions: the newest; every version published within the retention window (restore points, 7 days by default);
//!   every version a checkpoint pair (`fx/pairs/*`) or a reference (`fx/refs/*`) names (and nothing at all is deleted
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
}

fn runs_of(v: &Version) -> Vec<String> {
    v.groups.iter().filter_map(|g| g.sealed.as_ref().map(|s| s.path.clone())).chain(v.delta.iter().map(|d| d.path.clone())).collect()
}

/// The references in the store: versions named and run paths listed.
pub async fn references(store: &Arc<dyn ObjectStore>, id: &str) -> Result<(HashSet<u64>, HashSet<String>)> {
    let mut versions = HashSet::new();
    let mut runs = HashSet::new();
    let root = manifest::root(id);
    let mut objs = Vec::new();
    for d in ["pairs", "refs"] {
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
            Err(_) => {} // deleted meanwhile
        }
        loaded_now += 1;
        if loaded_now >= GC_LOAD_PER_PASS {
            break;
        }
    }
    if !st.to_load.lock().unwrap().is_empty() {
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
        st.passes.fetch_add(1, Relaxed);
        return Ok(());
    }
    let (ref_versions, ref_runs) = references(store, id).await?;
    let retention = c.knobs.version_retention.as_millis() as u64;
    let now_ms = manifest::now_ms();
    // the oldest version this process still holds or serves: it and every newer one stay
    let mut floor_v = newest;
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
        if first.elapsed() >= grace && age >= grace {
            doomed.push(m.location);
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
            if !in_use.contains(&m.location.to_string()) && age >= grace {
                if local.delete(&m.location).await.is_ok() {
                    st.local_deleted.fetch_add(1, Relaxed);
                }
            }
        }
    }
    st.passes.fetch_add(1, Relaxed);
    Ok(())
}

