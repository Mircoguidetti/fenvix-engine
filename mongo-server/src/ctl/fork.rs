//! Fork: an instant copy of a database as a new database with its own state.
//!
//! Two modes:
//!
//! * `clone` (default): slatedb's shallow clone. The new tenant's manifest points at the source's SSTs (no data is
//!   copied) and the source gets a checkpoint that stops its garbage collector deleting them; only the WAL objects
//!   still outside an SST are copied, and the source is closed cleanly first so there are none. Cost: a handful of
//!   requests, independent of the data size. The fork reads the source's objects until its own compaction has
//!   rewritten them, so the source cannot be erased meanwhile (`control/forks/{source}/{fork}` records the
//!   dependency; erase answers 409 `has_forks`).
//! * `copy`: server-side copy of every object (one COPY request each, no data through this process). The fork is
//!   fully independent, at the price of one request per object and a second copy of the bytes in the bucket.
//!
//! Either way writes to one side never reach the other: both sides are separate slatedb databases with their own
//! manifests, WALs and leases, and the shared SSTs are immutable.

use super::purge;
use super::rec::{self, now_ms};
use super::usage::Counts;
use super::{meta, ops, ApiError, ApiResult, Ctl};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use serde_json::{json, Value};
use slatedb::admin::Admin;
use std::sync::Arc;
use tenant_bench::lifecycle::{marker_path, maint_lock, Op};

pub fn fork_marker(ancestor: &str, db: &str) -> Path {
    rec::path(&["control", "forks", ancestor, db])
}

fn db_of_path(p: &str) -> Option<String> {
    p.strip_prefix("tenants/").map(|s| s.to_string())
}

/// Forks that still read this database's objects (stale records of erased forks are removed on the way).
pub async fn dependents(ctl: &Ctl, db: &str) -> anyhow::Result<Vec<String>> {
    let prefix = rec::path(&["control", "forks", db]);
    let mut out = Vec::new();
    let marks: Vec<Path> = ctl.store.list(Some(&prefix)).map_ok(|m| m.location).try_collect().await?;
    for m in marks {
        let child = m.filename().unwrap_or("").to_string();
        if meta::exists(ctl, &child).await? {
            out.push(child);
        } else if ops::get(ctl, &child).await?.is_none() {
            rec::delete(&ctl.store, &m).await?;
        }
    }
    Ok(out)
}

/// Make the source's latest manifest the complete truth: flushed, marker present, no tenant open on it.
/// The caller holds the entry lock.
pub(crate) async fn quiesce(ctl: &Arc<Ctl>, g: &mut crate::dbs::Entry, src: &str) -> ApiResult<()> {
    if g.is_open() {
        g.close().await?;
    }
    let clean = ctl.store.head(&marker_path(src)).await.is_ok();
    if !clean {
        // A writer died without closing: its last writes are in the WAL beyond the manifest. Open it as a writer
        // (replays the WAL), rewrite a value with itself to make that real, and close it cleanly.
        let cur = g.get(crate::layout::META_COLLECTIONS).await?;
        let Some(cur) = cur else { return Err(ApiError::new(500, "internal", format!("database {src} has no collection list"))) };
        g.apply(vec![Op::Put(crate::layout::META_COLLECTIONS.to_vec(), cur.to_vec())]).await?;
        g.close().await?;
        if ctl.store.head(&marker_path(src)).await.is_err() {
            return Err(ApiError::new(500, "internal", format!("database {src} could not be closed cleanly")));
        }
    }
    Ok(())
}

pub async fn fork(ctl: &Arc<Ctl>, src: &str, target: &str, mode: &str) -> ApiResult<Value> {
    meta::check_name(src)?;
    meta::check_name(target)?;
    if src == target {
        return Err(ApiError::bad("source and target are the same database"));
    }
    if mode != "clone" && mode != "copy" {
        return Err(ApiError::bad("mode must be \"clone\" or \"copy\""));
    }
    let (_g1, _g2) = ctl.ops.lock2(src, target).await?;
    if !meta::exists(ctl, src).await? {
        return Err(ApiError::not_found(format!("database {src} does not exist")));
    }
    if meta::exists(ctl, target).await? || ops::get(ctl, target).await?.is_some() {
        return Err(ApiError::conflict("exists", format!("database {target} already exists")));
    }
    meta::ensure_no_auth(ctl, target).await?;
    let t0 = std::time::Instant::now();
    let before = ctl.usage.today_sum(&[src, target]);
    let ancestors: Vec<String> = purge::ancestors(&ctl.store, src).await?.into_iter().filter_map(|(p, _)| db_of_path(&p)).collect();
    if mode == "copy" && !ancestors.is_empty() {
        return Err(ApiError::conflict("source_is_shallow_fork", format!("database {src} is itself a shallow fork of {ancestors:?}; fork it with mode \"clone\" (or erase its parents' dependency first)")));
    }
    let mut chain: Vec<String> = vec![src.to_string()];
    chain.extend(ancestors);
    ops::begin(ctl, target, "fork", json!({ "source": src, "mode": mode, "ancestors": chain })).await?;
    super::failpoint("fork.after_intent").await;
    let result = do_fork(ctl, src, target, mode, &chain).await;
    match result {
        Ok(source_manifest) => {
            ops::finish(ctl, target).await?;
            ctl.usage.touch(src);
            ctl.usage.touch(target);
            let cost = ctl.usage.today_sum(&[src, target]).minus(&before);
            let mut d = meta::describe(ctl, target, true).await?;
            d["fork"] = json!({ "source": src, "mode": mode, "source_manifest_id": source_manifest, "elapsed_ms": t0.elapsed().as_millis() as u64, "cost": cost.to_json() });
            Ok(d)
        }
        Err(e) => {
            // All or nothing: undo what was done. If this fails too, the intent record makes the next start do it.
            if let Err(r) = rollback(ctl, target, &json!({ "ancestors": chain })).await {
                eprintln!("fork {src} -> {target}: rollback failed ({r}); it is retried at the next start");
            }
            Err(e)
        }
    }
}

async fn do_fork(ctl: &Arc<Ctl>, src: &str, target: &str, mode: &str, chain: &[String]) -> ApiResult<Option<u64>> {
    // Everything below happens with the source closed and its entry lock held: no command runs on it meanwhile.
    let mut g = ctl.dbs.lock(src).await;
    quiesce(ctl, &mut g, src).await?;
    let lock = maint_lock(&ctl.lc.maint_locks, src);
    let _maint = lock.lock().await;
    ctl.dbs.leases.acquire(src).await?;
    let r = async {
        // Dependencies are recorded before the first byte is shared, so a crash cannot leave an unrecorded one.
        if mode == "clone" {
            for a in chain {
                rec::put(&ctl.store, &fork_marker(a, target), &json!({ "fork": target, "source": src, "at_ms": now_ms() })).await?;
            }
        }
        super::failpoint("fork.after_markers").await;
        ops::phase(ctl, target, "copying").await?;
        let manifest_id = match mode {
            "clone" => {
                let admin = Admin::builder(purge::tenant_path(target), Arc::clone(&ctl.store)).build();
                admin.create_clone_builder_from_source(slatedb::CloneSourceSpec::new(purge::tenant_path(src))).build().await.map_err(anyhow::Error::from)?;
                Admin::builder(purge::tenant_path(src), Arc::clone(&ctl.store)).build().read_manifest(None).await.map_err(anyhow::Error::from)?.map(|m| m.id())
            }
            _ => {
                copy_objects(ctl, src, target).await?;
                None
            }
        };
        super::failpoint("fork.after_clone").await;
        ctl.store.put(&marker_path(target), PutPayload::from_static(b"clean")).await.map_err(anyhow::Error::from)?;
        super::failpoint("fork.after_marker").await;
        let fm = json!({ "database": src, "at_ms": now_ms(), "mode": mode });
        let m = meta::default_meta(target, now_ms(), "fork");
        let mut m = m;
        m["forked_from"] = fm;
        rec::put(&ctl.store, &meta::meta_path(target), &m).await?;
        // a fresh record: the fork has no allow list (like it has no users)
        crate::netguard::get().forget_cached(target);
        super::failpoint("fork.after_meta").await;
        Ok::<_, anyhow::Error>(manifest_id)
    }
    .await;
    ctl.dbs.leases.release(src).await;
    drop(g);
    Ok(r?)
}

/// Server-side copy of every data object of `src` to `target` (not the lease or the marker).
async fn copy_objects(ctl: &Arc<Ctl>, src: &str, target: &str) -> anyhow::Result<()> {
    let from = Path::from(purge::tenant_path(src));
    let metas: Vec<object_store::ObjectMeta> = ctl.store.list(Some(&from)).try_collect().await?;
    let pre_from = format!("{}/", purge::tenant_path(src));
    let pre_to = format!("{}/", purge::tenant_path(target));
    let copies = metas.into_iter().filter(|m| !matches!(m.location.filename(), Some("_lease") | Some("_clean") | Some(".deleting"))).map(|m| {
        let to = Path::from(format!("{pre_to}{}", m.location.as_ref().strip_prefix(&pre_from).unwrap_or(m.location.as_ref())));
        let store = Arc::clone(&ctl.store);
        async move { store.copy(&m.location, &to).await }
    });
    futures::stream::iter(copies).buffer_unordered(16).try_collect::<Vec<()>>().await?;
    Ok(())
}

/// Undo a fork: release the pins it took, delete its objects, its dependency records and its registry record.
pub async fn rollback(ctl: &Arc<Ctl>, target: &str, op: &Value) -> ApiResult<()> {
    let _g = ctl.dbs.lock(target).await;
    purge::purge_tenant(ctl, target).await?;
    if let Some(a) = op["ancestors"].as_array() {
        for a in a.iter().filter_map(|a| a.as_str()) {
            rec::delete(&ctl.store, &fork_marker(a, target)).await?;
        }
    }
    rec::delete(&ctl.store, &meta::meta_path(target)).await?;
    ops::finish(ctl, target).await?;
    Ok(())
}

/// Called by the Mongo `dropDatabase` command before it deletes a database's objects: a database that forks still
/// read cannot be dropped, and a fork that is dropped releases the pins it holds in its source.
pub async fn on_drop(db: &str) -> anyhow::Result<()> {
    let Some(ctl) = super::get() else { return Ok(()) };
    let deps = dependents(ctl, db).await?;
    if !deps.is_empty() {
        anyhow::bail!("database {db} cannot be dropped: databases {deps:?} are forks that still read its data");
    }
    for (path, _) in purge::ancestors(&ctl.store, db).await? {
        if let Some(a) = db_of_path(&path) {
            rec::delete(&ctl.store, &fork_marker(&a, db)).await?;
        }
    }
    purge::release_pins(&ctl.store, db).await?;
    Ok(())
}

#[allow(dead_code)]
fn _unused(_: Counts) {}
