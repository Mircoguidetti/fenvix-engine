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
        format!("control/erasures/{db}/"),
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
        "incomplete_uploads_aborted": report.uploads_aborted,
        "proof": { "listed_after_erase": proof, "remaining_objects": 0, "remaining_incomplete_uploads": 0, "method": "fresh LIST of each prefix after the last delete, and of the incomplete multipart uploads under tenants/{db}/" },
        "elapsed_ms": t0.elapsed().as_millis() as u64,
        "cost": cost.to_json(),
        "final_usage": final_usage,
        "erased_at": rec::rfc3339(now_ms()),
    }))
}

/// What this process remembers of a database besides its open tenant: the WAL cleanup floor, the per-database counters,
/// the retention window, the relaxed durability setting and the cached IP allow list. Erase and dropDatabase both forget
/// it, so a database created again with the same name starts with none of it.
pub fn forget_in_memory(db: &str) {
    tenant_bench::lifecycle::forget_wal_state(db);
    crate::fair::forget(db);
    super::pitr::forget(db);
    super::durability::forget(db);
    crate::netguard::get().forget_cached(db);
}

/// dropDatabase: the settings records erase also deletes (the metadata record, which holds the retention window and the IP
/// allow list, the retention and durability markers and the keep-awake pin), then the in-memory state. Usage records,
/// exports and staging are kept (history; erase deletes them, a drop is not an erase). Every step is safe to repeat.
pub async fn forget_settings(store: &rec::Store, db: &str) -> anyhow::Result<()> {
    for p in [rec::path(&["control", "durability", db]), rec::path(&["control", "retention", db]), rec::path(&["control", "pins", db])] {
        rec::delete(store, &p).await?;
    }
    purge::delete_prefix(store, &rec::path(&["control", "dbs", db])).await?;
    forget_in_memory(db);
    Ok(())
}

/// The steps of an erase, in order; every one is safe to repeat.
async fn run(ctl: &Arc<Ctl>, db: &str, ancestors: &[String], fresh: bool) -> ApiResult<purge::PurgeReport> {
    // The pin goes first so the keep-awake agent stops reopening the database.
    rec::delete(&ctl.store, &rec::path(&["control", "pins", db])).await?;
    let mut g = ctl.dbs.lock(db).await;
    // A close that did not finish (store slow or down) leaves slatedb's tasks of the tenant alive, and they can still write
    // a WAL object or an SST under tenants/{db}/ after the purge below (no fencing: this process holds the lease). Stop
    // before any object is touched; the client retries once the store answers again (the close goes on in the background).
    let closed = g.abandon().await;
    if !closed || tenant_bench::lifecycle::close_pending(db) {
        if fresh {
            ops::finish(ctl, db).await?;
        }
        return Err(close_pending_error(db));
    }
    // a compaction pass running (maybe a long merge) stops instead of making this wait for it
    let _preempt = tenant_bench::lifecycle::preempt_compaction(db);
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
        l.forget_pinned(db);
    }
    // the databases holding a fork reference of this one (read before its manifest goes)
    let owners = tenant_bench::fx::pair::fork_ref_owners(&ctl.store, db, ancestors).await?;
    let report = purge::purge_tenant(ctl, db).await?;
    ctl.dbs.leases.forget(db);
    ctl.lc.touched.lock().unwrap().remove(db);
    // as a drop: a database created again with this name starts with none of the erased one's state
    forget_in_memory(db);
    let _ = rec::delete(&ctl.store, &rec::path(&["control", "durability", db])).await;
    super::failpoint("erase.after_tenant").await;
    for a in ancestors {
        rec::delete(&ctl.store, &fork::fork_marker(a, db)).await?;
    }
    // the fork references this database held on its ancestors' runs (compaction 2, step 7b)
    tenant_bench::fx::pair::drop_fork_refs(&ctl.store, &owners, db).await?;
    rec::delete(&ctl.store, &rec::path(&["control", "retention", db])).await?;
    for prefix in [rec::path(&["control", "erasures", db]), rec::path(&["control", "dbs", db]), rec::path(&["control", "usage", db]), rec::path(&["control", "forks", db]), rec::path(&["control", "exports", db]), rec::path(&["control", "staging", db])] {
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
    // After the erase's last S3 request: its own requests (deletes, proof listings) charged the database's counters until
    // here; now they go (C9: forgotten earlier, they were made again by the requests that followed).
    crate::fair::forget(db);
    Ok(report)
}

/// The erase stopped before deleting anything because the database's close has not finished: retry later.
fn close_pending_error(db: &str) -> ApiError {
    ApiError::new(503, "close_pending", format!("database {db} is still closing (its storage is not answering); nothing was deleted, retry the erase"))
}

/// Finish an erase that a crash interrupted.
pub async fn resume(ctl: &Arc<Ctl>, db: &str, op: &Value) -> ApiResult<()> {
    let ancestors: Vec<String> = op["ancestors"].as_array().map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect()).unwrap_or_default();
    // The tenant's lease may still be the dead process's: wait for it to expire (at most about one ttl).
    for attempt in 0..40 {
        match run(ctl, db, &ancestors, false).await {
            Ok(_) => return Ok(()),
            Err(e) if (e.code == "owned_elsewhere" || e.code == "close_pending") && attempt < 39 => tokio::time::sleep(std::time::Duration::from_millis(1000)).await,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

// ---- customer erasures (compaction 2, step 7b; `tenant_bench::fx::erase`) -------------------------------------------

/// Wakes the erasure agent at once after an erasure is recorded.
static WAKE: tokio::sync::Notify = tokio::sync::Notify::const_new();
/// How often the agent looks for erasures not done (`--sealed-erasure-poll-ms`).
pub static POLL_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(60_000);

fn index_path(db: &str, eid: &str) -> object_store::path::Path {
    rec::path(&["control", "erasures", db, eid])
}

/// An erasure of `db` was recorded: index it under `control/erasures/` (so the agent finds it after a crash without
/// listing every database) and wake the agent. A failure here only delays the pass to the agent's next start.
pub async fn note_erasure(db: &str, eid: &str) {
    if let Some(ctl) = super::get() {
        if let Err(e) = object_store::ObjectStoreExt::put(&ctl.store, &index_path(db, eid), object_store::PutPayload::from_static(b"{}")).await {
            eprintln!("erasure {eid} of {db}: index not written ({e}); the pass runs at the database's next erasure or the agent's start");
        }
    }
    WAKE.notify_one();
}

/// Run the erasure pass of `db` now, under its entry lock; the index entries of erasures that are done go.
pub async fn run_pass(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Vec<tenant_bench::fx::erase::Erasure>> {
    let all = {
        let mut g = ctl.dbs.lock(db).await;
        g.fx_erase_pass().await?
    };
    for e in all.iter().filter(|e| e.done_ms.is_some()) {
        rec::delete(&ctl.store, &index_path(db, &e.id)).await?;
    }
    Ok(all)
}

/// `GET /v1/databases/{db}/erasures`: every erasure of the database with its state, deadline and whether it is late.
pub async fn status(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Value> {
    meta::check_name(db)?;
    let all = tenant_bench::fx::erase::list(&ctl.store, db).await?;
    let now = now_ms() as u64;
    Ok(json!({ "database": db, "erasures": all.iter().map(|e| e.to_json()).collect::<Vec<_>>(), "overdue": all.iter().filter(|e| e.overdue(now)).count() }))
}

/// `POST /v1/databases/{db}/erasures/run`: the pass now (operators, tests).
pub async fn run_now(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Value> {
    meta::check_name(db)?;
    let _op = ctl.ops.lock(db).await?;
    let all = run_pass(ctl, db).await?;
    Ok(json!({ "database": db, "erasures": all.iter().map(|e| e.to_json()).collect::<Vec<_>>() }))
}

/// The erasure agent: every `POLL_MS`, and at once when an erasure is recorded, runs the pass of every database with an
/// erasure not done. Priority and the CPU cap across databases are step 4's pool (plan round 2: erasures come after
/// absorption and drops, before tier merges and reseals).
pub fn spawn_agent(ctl: &Arc<Ctl>) {
    let ctl = Arc::clone(ctl);
    tokio::spawn(async move {
        loop {
            let poll = std::time::Duration::from_millis(POLL_MS.load(std::sync::atomic::Ordering::Relaxed).max(10));
            let _ = tokio::time::timeout(poll, WAKE.notified()).await;
            if !tenant_bench::fx::ENABLED.load(std::sync::atomic::Ordering::Relaxed) {
                continue;
            }
            let Some(_gate) = super::agent_gate().await else { return };
            use futures::StreamExt;
            let listed: Vec<object_store::path::Path> = ctl.store.list(Some(&rec::path(&["control", "erasures"]))).filter_map(|m| async { m.ok().map(|m| m.location) }).collect().await;
            let mut dbs: Vec<String> = listed.iter().filter_map(|p| p.parts().nth(2).map(|x| x.as_ref().to_string())).collect();
            dbs.dedup();
            for db in dbs {
                let Ok(_op) = ctl.ops.lock(&db).await else { continue };
                if let Err(e) = run_pass(&ctl, &db).await {
                    eprintln!("erasure pass of {db}: {}", e.message);
                }
            }
        }
    });
}
