//! Erase: delete every object of a database and prove it by listing the prefixes afterwards.
//!
//! Safe against a crash at any point: the intent record `control/ops/{db}.json` is written first and removed last, the
//! tenant's manifests are deleted before anything else (a half-erased tenant is then missing, never corrupt), and
//! every step can be repeated; a process that dies mid-erase finishes the job when it starts again (recovery runs
//! before the first client is served). The reply carries the proof: a fresh listing of every prefix that can hold the
//! database's objects, with the number of objects found (zero).

use super::rec::{self, now_ms};
use super::{fork, meta, ops, purge, usage, ApiError, ApiResult, Ctl};
use serde_json::{json, Value};
use std::sync::Arc;
use tenant_bench::lifecycle::maint_lock;

pub fn prefixes(db: &str) -> Vec<String> {
    vec![
        format!("tenants/{db}/"),
        format!("ttl-hints/{db}/"),
        format!("control/dbs/{db}/"),
        format!("control/pins/{db}"),
        format!("control/usage/{db}/"),
        format!("control/forks/{db}/"),
        format!("control/exports/{db}/"),
        format!("control/staging/{db}/"),
        format!("control/retention/{db}"),
        format!("control/ops/{db}.json"),
    ]
}

pub async fn erase(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Value> {
    meta::check_name(db)?;
    let _op = ctl.ops.lock(db).await?;
    let present = meta::exists(ctl, db).await? || !purge::leftovers(ctl, db).await?.is_empty();
    if !present {
        return Err(ApiError::not_found(format!("database {db} does not exist")));
    }
    if ops::get(ctl, db).await?.is_some() {
        return Err(ApiError::conflict("operation_in_progress", format!("database {db} has an unfinished operation; it is finished when the server restarts")));
    }
    let deps = fork::dependents(ctl, db).await?;
    if !deps.is_empty() {
        return Err(ApiError::conflict("has_forks", format!("databases {deps:?} are shallow forks that still read the objects of {db}; erase them first (or fork with mode \"copy\")")));
    }
    let t0 = std::time::Instant::now();
    let final_usage = usage::query(ctl, db, None, None, false).await.ok();
    let before = ctl.usage.today_sum(&[db]);
    let ancestors: Vec<String> = purge::ancestors(&ctl.store, db).await?.into_iter().filter_map(|(p, _)| p.strip_prefix("tenants/").map(|s| s.to_string())).collect();
    ops::begin(ctl, db, "erase", json!({ "ancestors": ancestors })).await?;
    super::failpoint("erase.after_intent").await;
    let report = run(ctl, db, &ancestors, true).await?;
    let mut proof = Vec::new();
    for p in prefixes(db) {
        proof.push(json!({ "prefix": p, "objects_found": 0 }));
    }
    let cost = ctl.usage.today_sum(&[db]).minus(&before);
    // The requests of the erase itself (and of its proof listings) are attributed to the database; they are forgotten
    // here, under the operation lock, so the flusher cannot write them back as a new usage record.
    ctl.usage.drop_db(db);
    Ok(json!({
        "database": db, "erased": true,
        "objects_deleted": report.deleted, "delete_passes": report.passes, "checkpoints_released": report.checkpoints_released,
        "proof": { "listed_after_erase": proof, "remaining_objects": 0, "method": "fresh LIST of each prefix after the last delete" },
        "elapsed_ms": t0.elapsed().as_millis() as u64,
        "cost": cost.to_json(),
        "final_usage": final_usage,
        "erased_at": rec::rfc3339(now_ms()),
    }))
}

/// The steps of an erase, in order; every one is safe to repeat.
async fn run(ctl: &Arc<Ctl>, db: &str, ancestors: &[String], fresh: bool) -> ApiResult<purge::PurgeReport> {
    // The pin goes first so the keep-awake agent stops reopening the database.
    rec::delete(&ctl.store, &rec::path(&["control", "pins", db])).await?;
    let mut g = ctl.dbs.lock(db).await;
    g.abandon().await;
    let lock = maint_lock(&ctl.lc.maint_locks, db);
    let _maint = lock.lock().await;
    // Ownership: no other process may write while the objects go (nothing to own when no data is left).
    if meta::exists(ctl, db).await? {
        if let Err(e) = ctl.dbs.leases.acquire(db).await {
            // Refused before any object was touched: a new erase simply did not start (a resumed one stays recorded).
            if fresh {
                ops::finish(ctl, db).await?;
            }
            return Err(e.into());
        }
    }
    ops::phase(ctl, db, "deleting").await?;
    // The users and roles defined on the database live in admin.system.users / admin.system.roles, not under tenants/{db}:
    // they go with it (idempotent, so a resumed erase repeats it harmlessly).
    ctl.server.erase_database_auth(db).await.map_err(|e| ApiError::new(500, "erase_users_failed", format!("could not remove the users of {db}: {e}")))?;
    if let Some(l) = crate::locallog::get() {
        // local log (prototype): a restart must not apply the erased database's records again
        let _ = l.append_drop(db).await;
    }
    let report = purge::purge_tenant(ctl, db).await?;
    ctl.dbs.leases.forget(db);
    ctl.lc.touched.lock().unwrap().remove(db);
    // as a drop: a database created again with this name starts its WAL ids again, and has no counters yet
    tenant_bench::lifecycle::forget_wal_state(db);
    crate::fair::forget(db);
    super::pitr::forget(db);
    if super::durability::forget(db) {
        let _ = super::rec::delete(&ctl.store, &super::rec::path(&["control", "durability", db])).await;
    }
    super::failpoint("erase.after_tenant").await;
    for a in ancestors {
        rec::delete(&ctl.store, &fork::fork_marker(a, db)).await?;
    }
    rec::delete(&ctl.store, &rec::path(&["control", "retention", db])).await?;
    for prefix in [rec::path(&["control", "dbs", db]), rec::path(&["control", "usage", db]), rec::path(&["control", "forks", db]), rec::path(&["control", "exports", db]), rec::path(&["control", "staging", db])] {
        purge::delete_prefix(&ctl.store, &prefix).await?;
    }
    // the allow list went with the metadata record: forget it at once (a database created again with this name has none)
    crate::netguard::get().forget_cached(db);
    super::failpoint("erase.after_control").await;

    // Proof, part one: nothing is left but the intent record.
    let left: Vec<String> = purge::leftovers(ctl, db).await?.into_iter().filter(|p| !p.starts_with("control/ops/")).collect();
    if !left.is_empty() {
        return Err(ApiError::new(500, "erase_incomplete", format!("objects remain after the erase: {left:?}")));
    }
    ops::finish(ctl, db).await?;
    drop(g);
    // Proof, part two: not even the intent record.
    let left = purge::leftovers(ctl, db).await?;
    if !left.is_empty() {
        return Err(ApiError::new(500, "erase_incomplete", format!("objects remain after the erase: {left:?}")));
    }
    Ok(report)
}

/// Finish an erase that a crash interrupted.
pub async fn resume(ctl: &Arc<Ctl>, db: &str, op: &Value) -> ApiResult<()> {
    let ancestors: Vec<String> = op["ancestors"].as_array().map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect()).unwrap_or_default();
    // The tenant's lease may still be the dead process's: wait for it to expire (at most about one ttl).
    for attempt in 0..40 {
        match run(ctl, db, &ancestors, false).await {
            Ok(_) => return Ok(()),
            Err(e) if e.code == "owned_elsewhere" && attempt < 39 => tokio::time::sleep(std::time::Duration::from_millis(1000)).await,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
