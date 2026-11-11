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
        "proof": { "listed_after_erase": proof, "remaining_objects": 0, "method": "fresh LIST of each prefix after the last delete" },
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
    forget_due(db);
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
//
// perf-erase (2026-11-11): the delete that records an erasure returns once its deletes are committed and its record is
// written (one PUT, with the index beside it); nothing more waits on it. The physical erasure runs later, in batches:
// a database's pending erasures are scrubbed together when the oldest one has waited `BATCH_MS` (default 1 hour, and
// never more than an eighth of the 24-hour deadline), or `BATCH_MAX` are pending, so one rewrite of each run serves
// every erasure of the batch. The scrub reads and writes runs without the database's lock; only the short first step
// (list, flush, ask the compactor to absorb) takes it. With a compactor the scrub is published through it
// (`Compactor::commit_scrub`), so it never races the compactor's own publishes.

/// Wakes the erasure agent at once (a batch is full, or a test asks).
static WAKE: tokio::sync::Notify = tokio::sync::Notify::const_new();
/// How often the agent looks at the databases with erasures not done (`--sealed-erasure-poll-ms`).
pub static POLL_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(60_000);
/// A batch is due when its oldest pending erasure has waited this long (`--sealed-erasure-batch-ms`, default 1 hour).
pub static BATCH_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(3_600_000);
/// ... or when this many erasures of the database are pending (`--sealed-erasure-batch-max`).
pub static BATCH_MAX: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1_000);

/// What the agent knows of a database's erasures without reading the store: pending ones (count, oldest request) and
/// when the retire step of scrubbed ones is next worth trying. A database not in the map (after a restart) is looked at
/// once from its records.
#[derive(Clone, Debug, Default)]
struct Due {
    pending: u64,
    oldest_ms: Option<u64>,
    retire_at_ms: Option<u64>,
    /// a pending erasure waits for its deletes to be absorbed: look again at the next poll
    retry_soon: bool,
    /// erasures noted so far (never decreases): a pass counts those noted after it listed the records
    noted: u64,
}

static DUE: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<String, Due>>> = std::sync::LazyLock::new(Default::default);

fn ord() -> std::sync::atomic::Ordering {
    std::sync::atomic::Ordering::Relaxed
}

/// The batch window in effect: `BATCH_MS`, but never more than an eighth of the deadline (the pass, its retries and the
/// GC grace must fit in the deadline).
fn window_ms() -> u64 {
    BATCH_MS.load(ord()).min(tenant_bench::fx::erase::DEADLINE_MS.load(ord()) / 8)
}

fn index_path(db: &str, eid: &str) -> object_store::path::Path {
    rec::path(&["control", "erasures", db, eid])
}

/// The index object of an erasure (`control/erasures/{db}/{id}`), written beside its record by `register`.
pub fn index_of(db: &str) -> impl Fn(&str) -> object_store::path::Path + Sync + '_ {
    move |eid: &str| index_path(db, eid)
}

/// An erasure of `db` was recorded (its record and index are written): count it for the batch; wake the agent when
/// the batch is full.
pub async fn note_erasure(db: &str, _eid: &str) {
    let full = {
        let mut m = DUE.lock().unwrap();
        let d = m.entry(db.to_string()).or_default();
        d.pending += 1;
        d.noted += 1;
        d.oldest_ms.get_or_insert(now_ms() as u64);
        d.pending >= BATCH_MAX.load(ord())
    };
    if full {
        WAKE.notify_one();
    }
}

/// Whether the agent should run the pass of `db` now.
fn due(db: &str, now: u64) -> bool {
    let m = DUE.lock().unwrap();
    let Some(d) = m.get(db) else { return true };
    (d.pending > 0 && (d.retry_soon || d.pending >= BATCH_MAX.load(ord()) || d.oldest_ms.is_some_and(|o| now >= o + window_ms()))) || d.retire_at_ms.is_some_and(|t| now >= t)
}

fn noted(db: &str) -> u64 {
    DUE.lock().unwrap().get(db).map_or(0, |d| d.noted)
}

/// After a pass: what is left to do, from the records as the pass read them, plus the erasures noted since `noted0`
/// (recorded while the pass ran: not in its listing; their request is at or after `since_ms`).
fn remember(db: &str, all: &[tenant_bench::fx::erase::Erasure], waiting: bool, noted0: u64, since_ms: u64) {
    let grace = tenant_bench::fx::GC_GRACE_MS.load(ord());
    let now = now_ms() as u64;
    let pending: Vec<&tenant_bench::fx::erase::Erasure> = all.iter().filter(|e| e.done_ms.is_none() && e.state == "pending").collect();
    let retire_at = all
        .iter()
        .filter(|e| e.done_ms.is_none() && e.state != "pending")
        .map(|e| {
            let g = e.scrubbed_ms.unwrap_or(now) + grace;
            // kept by something else than the grace (a restore point, the served version): look again later
            if g <= now { now + grace.max(POLL_MS.load(ord())) } else { g }
        })
        .min();
    let mut m = DUE.lock().unwrap();
    let d = m.entry(db.to_string()).or_default();
    let late = if crate::ctl::fork::control("FENVIX_CONTROL_FORGET_NOTED") { 0 } else { d.noted.saturating_sub(noted0) };
    d.pending = pending.len() as u64 + late;
    d.oldest_ms = pending.iter().map(|e| e.requested_ms).chain((late > 0).then_some(since_ms)).min();
    d.retire_at_ms = retire_at;
    d.retry_soon = waiting && !pending.is_empty();
}

/// The newest version of `db` and the one its writer serves (a compactor's served version is an atomic; without one the
/// tenant installs a scrub at once). Without an open writer, the newest version of the store for both.
async fn served_versions(ctl: &Arc<Ctl>, db: &str, all: &[tenant_bench::fx::erase::Erasure]) -> (u64, u64) {
    let g = ctl.dbs.lock(db).await;
    let (n, s) = g.fx_versions();
    drop(g);
    let top = all.iter().filter_map(|e| e.scrubbed_version).max().unwrap_or(0);
    match (n, s) {
        (Some(n), Some(s)) => (n.max(top), s),
        _ => {
            let n = tenant_bench::fx::manifest::list_versions(&ctl.store, db).await.ok().and_then(|v| v.last().copied()).unwrap_or(top);
            (n, n)
        }
    }
}

/// Forget a database (erased or dropped).
pub fn forget_due(db: &str) {
    DUE.lock().unwrap().remove(db);
}

/// One erasure pass of `db` (the agent's when a batch is due, or `POST .../erasures/run`): every pending erasure whose
/// deletes are absorbed is scrubbed in one batch, then the retire step of every scrubbed erasure runs. Only the first,
/// short step holds the database's lock. The index entries of erasures that are done go.
pub async fn run_pass(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Vec<tenant_bench::fx::erase::Erasure>> {
    use tenant_bench::fx::erase;
    let t0 = std::time::Instant::now();
    let (noted0, since_ms) = (noted(db), now_ms() as u64);
    let c = &ctl.dbs.global.counters;
    let (g0, r0, b0) = (c.get.load(ord()), c.get_range.load(ord()), c.get_bytes.load(ord()));
    let mut waiting = false;
    let mut lock_ms = 0u64;
    let mut batch_n = 0usize;
    let mut all = Vec::new();
    // a scrub that its commit refused (the version changed under it) is tried again at most twice in this pass; a
    // partial scrub (it stopped at its time budget) is committed and the next part follows at once
    let (mut abandons, mut parts) = (0u32, 0u32);
    for attempt in 0..1_000u32 {
        let t_l = std::time::Instant::now();
        let prep = {
            let mut g = ctl.dbs.lock(db).await;
            g.fx_erase_prepare().await?
        };
        lock_ms += t_l.elapsed().as_millis() as u64;
        let Some(p) = prep else { return Ok(Vec::new()) };
        super::failpoint("erase.after_prepare").await;
        all = p.all;
        if all.iter().all(|e| e.done_ms.is_some()) {
            break;
        }
        let need = all.iter().filter(|e| e.state == "pending").map(|e| e.s_e).max().unwrap_or(0);
        if need == 0 {
            break;
        }
        // the base version: the compactor's newest once its X_d reaches the pending erasures (waited without the lock)
        let base = match &p.fxc {
            Some(fxc) => {
                let t_w = std::time::Instant::now();
                loop {
                    let v = fxc.current().await;
                    if v.x_d >= need || t_w.elapsed() > std::time::Duration::from_secs(30) || fxc.stopped.load(ord()) {
                        break Some(v);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
            None => p.base.clone(),
        };
        let Some(base) = base else { break };
        let ready: Vec<usize> = all.iter().enumerate().filter(|(_, e)| e.state == "pending" && e.s_e <= base.x_d).map(|(i, _)| i).collect();
        waiting = ready.len() < all.iter().filter(|e| e.state == "pending").count();
        if ready.is_empty() {
            break;
        }
        let mut targets = Vec::with_capacity(ready.len());
        for &i in &ready {
            match erase::targets_of(&ctl.store, db, &all[i]).await? {
                Some(t) => targets.push((i, t)),
                None => return Err(ApiError::new(500, "erasure_targets_lost", format!("database {db}: erasure {} lost its targets before its scrub", all[i].id))),
            }
        }
        let items: Vec<(&erase::Targets, u64)> = targets.iter().map(|(i, t)| (t, all[*i].s_e)).collect();
        let batch = erase::Batch::new(&items);
        let view = tenant_bench::fx::view::View::new(Arc::clone(&ctl.store), base.clone(), None)?;
        let t_s = std::time::Instant::now();
        let mut opts = erase::ScrubOpts::default();
        // the runs a part writes are published well within the GC grace, which counts from their write (`gc.rs`)
        opts.budget = Some(std::time::Duration::from_millis((tenant_bench::fx::GC_GRACE_MS.load(ord()) / 4).max(1_000)));
        // no merge or reseal of the compactor starts meanwhile: they would make the commit refuse the scrub
        let _pause = p.fxc.as_ref().map(|c| c.pause_rewrites());
        let s = erase::scrub_version(&ctl.store, db, &view, base.version + 1, base.writer_epoch, &batch, &opts).await?;
        let published = match &s.version {
            None => Some(base.clone()),
            Some(v) => match &p.fxc {
                Some(fxc) => fxc.commit_scrub(&base, &s, batch.max_s_e).await?,
                None => {
                    let mut g = ctl.dbs.lock(db).await;
                    g.fx_erase_publish(base.version, v).await?.then(|| v.clone())
                }
            },
        };
        drop(_pause);
        erase::trace(json!({"ev": "batch", "db": db, "attempt": attempt, "erasures": ready.len(), "base_version": base.version, "groups": base.groups.len(), "delta_runs": base.delta.len(),
            "runs_rewritten": s.runs, "runs_retired": s.retired.len(), "rows": s.rows, "complete": s.complete, "published": published.as_ref().map(|v| v.version), "scrub_ms": t_s.elapsed().as_millis() as u64}));
        let Some(newv) = published else {
            // the version changed under the scrub: its runs were never listed, they go now; try again on the newer one
            erase::SCRUB_ABANDONS.fetch_add(1, ord());
            for (_, _, n) in &s.sealed {
                if let Some(l) = n {
                    erase::delete_run(&ctl.store, db, &l.path).await?;
                }
            }
            for (_, n) in &s.delta {
                if let Some(l) = n {
                    erase::delete_run(&ctl.store, db, &l.path).await?;
                }
            }
            abandons += 1;
            if abandons >= 3 {
                break;
            }
            continue;
        };
        if p.fxc.is_some() {
            // the writer serves the scrub's version from now on (a short step: the version is installed in memory)
            let mut g = ctl.dbs.lock(db).await;
            g.fx_sync();
        }
        if !s.complete {
            // a part: its replaced runs are recorded in every erasure of the batch (they stay pending) before the next
            // part, so a crash leaves nothing untracked; the next part starts from the version just published
            for (i, _) in &targets {
                erase::record_partial(&mut all[*i], &s);
                erase::save(&ctl.store, db, &all[*i]).await?;
            }
            parts += 1;
            erase::trace(json!({"ev": "batch_part", "db": db, "part": parts, "runs_rewritten": s.runs}));
            continue;
        }
        let bid = all[ready[0]].id.clone();
        for (i, t) in &targets {
            let e = &mut all[*i];
            e.batch = Some(bid.clone());
            e.batch_erasures = ready.len() as u64;
            erase::record_scrub(&ctl.store, db, e, &s, newv.version, t, &newv).await?;
        }
        erase::BATCHES.fetch_add(1, ord());
        erase::BATCHED_ERASURES.fetch_add(ready.len() as u64, ord());
        batch_n = ready.len();
        break;
    }
    // retire, without the lock: the runs scrubbed out once the grace passed and the writer serves the scrub's version
    let (newest, served) = served_versions(ctl, db, &all).await;
    {
        let mut open: Vec<&mut erase::Erasure> = all.iter_mut().filter(|e| e.done_ms.is_none() && e.scrubbed_version.is_some()).collect();
        let before: Vec<String> = open.iter().map(|e| serde_json::to_string(&**e).unwrap_or_default()).collect();
        erase::retire_all(&ctl.store, db, &mut open, newest, served, tenant_bench::fx::GC_GRACE_MS.load(ord())).await?;
        for (e, b) in open.iter().zip(before) {
            if serde_json::to_string(&**e).unwrap_or_default() != b {
                erase::save(&ctl.store, db, e).await?;
            }
        }
    }
    for e in all.iter().filter(|e| e.done_ms.is_some()) {
        rec::delete(&ctl.store, &index_path(db, &e.id)).await?;
    }
    remember(db, &all, waiting, noted0, since_ms);
    erase::trace(json!({"ev": "pass", "db": db, "lock_ms": lock_ms, "ms": t0.elapsed().as_millis() as u64, "batch": batch_n,
        "gets_process": c.get.load(ord()) - g0 + c.get_range.load(ord()) - r0, "get_bytes_process": c.get_bytes.load(ord()) - b0,
        "open": all.iter().filter(|e| e.done_ms.is_none()).count(), "pending": all.iter().filter(|e| e.state == "pending").count()}));
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

/// The erasure agent: every `POLL_MS` (and at once when a batch is full) it runs the pass of every database with
/// erasures not done whose batch is due (`due`) or whose scrubbed erasures may be retired now. Priority and the CPU cap
/// across databases are step 4's pool (plan round 2: erasures come after absorption and drops, before tier merges and
/// reseals).
pub fn spawn_agent(ctl: &Arc<Ctl>) {
    let ctl = Arc::clone(ctl);
    tokio::spawn(async move {
        loop {
            let poll = std::time::Duration::from_millis(POLL_MS.load(ord()).max(10));
            let _ = tokio::time::timeout(poll, WAKE.notified()).await;
            if !tenant_bench::fx::ENABLED.load(ord()) {
                continue;
            }
            let Some(_gate) = super::agent_gate().await else { return };
            use futures::StreamExt;
            let listed: Vec<object_store::path::Path> = ctl.store.list(Some(&rec::path(&["control", "erasures"]))).filter_map(|m| async { m.ok().map(|m| m.location) }).collect().await;
            let mut dbs: Vec<String> = listed.iter().filter_map(|p| p.parts().nth(2).map(|x| x.as_ref().to_string())).collect();
            dbs.dedup();
            let now = now_ms() as u64;
            for db in dbs {
                if !due(&db, now) {
                    continue;
                }
                let Ok(_op) = ctl.ops.lock(&db).await else { continue };
                if let Err(e) = run_pass(&ctl, &db).await {
                    eprintln!("erasure pass of {db}: {}", e.message);
                    // look again at the next poll
                    DUE.lock().unwrap().entry(db.clone()).or_default().retry_soon = true;
                }
            }
        }
    });
}
