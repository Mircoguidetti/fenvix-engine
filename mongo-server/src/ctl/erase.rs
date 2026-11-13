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
    forget_due(db);
    tenant_bench::lifecycle::forget_wal_state(db);
    crate::fair::forget(db);
    super::pitr::forget(db);
    super::durability::forget(db);
    crate::netguard::get().forget_cached(db);
    // P-big-speed fix 3: the customer boxes' marks and the runs' blocks on the local disk floor (zeroed)
    crate::custbox::forget_db(db);
    tenant_bench::fx::floor::forget_db(db);
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
    // this process holds the lease: a first build off the lock may end first, so none of its uploads is cut mid-part
    let closed = g.abandon_owned().await;
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
    super::failpoint("erase.before_proof_two").await;
    // Proof, part two: not even the intent record. Still under the entry lock (fix-erase-500): a read of the name opened
    // in between would take tenants/{db}/_lease and the listing would report it as left over (500 erase_incomplete).
    let left = purge::leftovers(ctl, db).await?;
    drop(g);
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

// erase-cost-1tb ("one reseal serves many"): with `RIDE` on (the default, `--sealed-erasure-ride`), the physical erasure
// rides the compactor's ordinary reseals and merges (`tenant_bench::fx::erase`, "one reseal serves many"); a check every
// `CHECK_MS` of an open database records what they did, and nothing is rewritten for an erasure until its force time
// (`FORCE_AFTER_MS` after the request, at most three quarters of its deadline). Then the scheduler reseals each group still
// owing and cleanses each delta run still holding the customer's dead rows, for every erasure whose force time falls
// within the next batch window (`window_ms`), so one forced reseal serves the erasures of a whole window. With `RIDE` off
// (or without a compactor) the perf-erase batch scrub runs as before.

/// The physical erasure rides ordinary reseals and merges, and the deadline forces the rest (`--sealed-erasure-ride`).
pub static RIDE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);
/// An erasure's force time: this long after its request (`--sealed-erasure-force-after-ms`, default 18 hours), never
/// later than three quarters of its deadline (the forced reseals, the GC grace and the drop must fit in the rest).
pub static FORCE_AFTER_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(18 * 3_600_000);
/// How often an open database with erasures pending is checked for what ordinary jobs did (`--sealed-erasure-check-ms`).
/// A closed database is not opened for it: nothing is resealed while it is closed.
pub static CHECK_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(600_000);
/// The rewrite speed the scheduler counts on for a forced reseal (stored bytes a second; MEASURED on the 10 GiB
/// ballast: about 20 MB/s for one reseal at a time, reads included). A database whose owing groups would take longer to
/// reseal than the reserve the force time keeps (6 hours of 24 by default) is forced earlier, by the estimate.
pub static RESEAL_BYTES_PER_S: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(20_000_000);

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
    /// erase-cost-1tb: the earliest force time of the pending erasures, and when the last ride check ran
    force_at_ms: Option<u64>,
    checked_ms: u64,
    /// the forced work the last pass saw owing (ms at `RESEAL_BYTES_PER_S`): the force time moves this much earlier
    work_ms: u64,
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
fn due(db: &str, now: u64, open: bool) -> bool {
    let m = DUE.lock().unwrap();
    let Some(d) = m.get(db) else { return true };
    if RIDE.load(ord()) {
        // a forced pass when the earliest force time enters the window; a check of an open database every CHECK_MS (a
        // new erasure counts as nothing to check: its deletes are not even absorbed yet)
        let force = force_every(d.pending)
            || d.force_at_ms.is_some_and(|f| now + window_ms() + d.work_ms >= f)
            || (d.pending > 0 && d.oldest_ms.is_some_and(|o| now + window_ms() >= force_at(o, o + tenant_bench::fx::erase::DEADLINE_MS.load(ord()))));
        let check = open && d.pending > 0 && now >= d.checked_ms + CHECK_MS.load(ord());
        return (d.pending > 0 && (d.retry_soon || force || check)) || d.retire_at_ms.is_some_and(|t| now >= t);
    }
    (d.pending > 0 && (d.retry_soon || d.pending >= BATCH_MAX.load(ord()) || d.oldest_ms.is_some_and(|o| now >= o + window_ms()))) || d.retire_at_ms.is_some_and(|t| now >= t)
}

/// Every pending erasure is due now: a batch window of 0 (`--sealed-erasure-batch-ms 0`, tests: the physical erasure at
/// the next poll), or `BATCH_MAX` erasures pending (a bound on the records each pass reads).
fn force_every(pending: u64) -> bool {
    BATCH_MS.load(ord()) == 0 || pending >= BATCH_MAX.load(ord())
}

/// An erasure's force time: `FORCE_AFTER_MS` after its request, at most three quarters of the way to its deadline.
pub fn force_at(requested_ms: u64, deadline_ms: u64) -> u64 {
    let span = deadline_ms.saturating_sub(requested_ms);
    requested_ms + FORCE_AFTER_MS.load(ord()).min(span / 4 * 3)
}

fn noted(db: &str) -> u64 {
    DUE.lock().unwrap().get(db).map_or(0, |d| d.noted)
}

/// After a pass: what is left to do, from the records as the pass read them, plus the erasures noted since `noted0`
/// (recorded while the pass ran: not in its listing; their request is at or after `since_ms`).
fn remember(db: &str, all: &[tenant_bench::fx::erase::Erasure], waiting: bool, noted0: u64, since_ms: u64, work_ms: Option<u64>) {
    let grace = tenant_bench::fx::GC_GRACE_MS.load(ord());
    let now = now_ms() as u64;
    let pending: Vec<&tenant_bench::fx::erase::Erasure> = all.iter().filter(|e| e.done_ms.is_none() && e.state == "pending").collect();
    let retire_at = all
        .iter()
        .filter(|e| e.done_ms.is_none() && e.state != "pending")
        .map(|e| {
            let g = e.scrubbed_ms.unwrap_or(now) + grace;
            // kept by something else than the grace (the restore window, a restore point, the served version): look
            // again later, at most hourly
            let window = tenant_bench::fx::compactor::VERSION_RETENTION_SECS.load(ord()) * 1000 / 4;
            if g <= now { now + grace.max(window.min(3_600_000)) } else { g }
        })
        .min();
    let mut m = DUE.lock().unwrap();
    let d = m.entry(db.to_string()).or_default();
    let late = if crate::ctl::fork::control("FENVIX_CONTROL_FORGET_NOTED") { 0 } else { d.noted.saturating_sub(noted0) };
    d.pending = pending.len() as u64 + late;
    d.oldest_ms = pending.iter().map(|e| e.requested_ms).chain((late > 0).then_some(since_ms)).min();
    d.retire_at_ms = retire_at;
    d.retry_soon = waiting && !pending.is_empty();
    d.force_at_ms = pending.iter().map(|e| force_at(e.requested_ms, e.deadline_ms)).min();
    if late > 0 {
        let f = force_at(since_ms, since_ms + tenant_bench::fx::erase::DEADLINE_MS.load(ord()));
        d.force_at_ms = Some(d.force_at_ms.map_or(f, |x| x.min(f)));
    }
    d.checked_ms = now;
    if let Some(w) = work_ms {
        d.work_ms = w;
    }
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
pub async fn run_pass(ctl: &Arc<Ctl>, db: &str, force_all: bool) -> ApiResult<Vec<tenant_bench::fx::erase::Erasure>> {
    if RIDE.load(ord()) {
        return run_pass_ride(ctl, db, force_all).await;
    }
    run_pass_scrub(ctl, db).await
}

/// The perf-erase pass: every pending erasure whose deletes are absorbed is scrubbed in one batch now.
pub async fn run_pass_scrub(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Vec<tenant_bench::fx::erase::Erasure>> {
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
    // erase-cost-1tb: a scrub in parts first removes every target row but the delete envelopes (they keep the keys
    // hidden whatever part is published), then the envelopes (`ScrubOpts::keep_envelopes`)
    // review fix: the batch is frozen at the first part (`erase::PartsPlan`): an erasure absorbed meanwhile waits for
    // the next pass, so none joins in phase 2
    let mut plan = erase::PartsPlan::default();
    for attempt in 0..1_000u32 {
        // the records, read without the database's lock (one GET each, in parallel)
        all = erase::list(&ctl.store, db).await?;
        let need = all.iter().filter(|e| e.state == "pending").map(|e| e.s_e).max().unwrap_or(0);
        if need == 0 {
            break;
        }
        let t_l = std::time::Instant::now();
        let prep = {
            let mut g = ctl.dbs.lock(db).await;
            g.fx_erase_prepare(need).await?
        };
        lock_ms += t_l.elapsed().as_millis() as u64;
        let Some(p) = prep else { return Ok(Vec::new()) };
        super::failpoint("erase.after_prepare").await;
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
        let absorbed: Vec<String> = all.iter().filter(|e| e.state == "pending" && e.s_e <= base.x_d).map(|e| e.id.clone()).collect();
        let admitted: std::collections::BTreeSet<String> = plan.admit(&absorbed).into_iter().collect();
        let ready: Vec<usize> = all.iter().enumerate().filter(|(_, e)| e.state == "pending" && admitted.contains(&e.id)).map(|(i, _)| i).collect();
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
        opts.keep_envelopes = plan.keep_envelopes;
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
        if !s.complete || plan.keep_envelopes {
            // a part: its replaced runs are recorded in every erasure of the batch (they stay pending) before the next
            // part, so a crash leaves nothing untracked; the next part starts from the version just published. The end
            // of the first phase (every target row but the envelopes gone) starts the second.
            if !s.retired.is_empty() {
                for (i, _) in &targets {
                    erase::record_partial(&mut all[*i], &s);
                    erase::save(&ctl.store, db, &all[*i]).await?;
                }
            }
            plan.after_part(s.complete);
            parts += 1;
            erase::trace(json!({"ev": "batch_part", "db": db, "part": parts, "runs_rewritten": s.runs, "keep_envelopes": opts.keep_envelopes}));
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
    // done: the record goes to the archive (passes no longer read it) and its index entry goes
    for e in all.iter().filter(|e| e.done_ms.is_some()) {
        erase::archive(&ctl.store, db, e).await?;
        rec::delete(&ctl.store, &index_path(db, &e.id)).await?;
    }
    remember(db, &all, waiting, noted0, since_ms, None);
    erase::trace(json!({"ev": "pass", "db": db, "lock_ms": lock_ms, "ms": t0.elapsed().as_millis() as u64, "batch": batch_n,
        "gets_process": c.get.load(ord()) - g0 + c.get_range.load(ord()) - r0, "get_bytes_process": c.get_bytes.load(ord()) - b0,
        "open": all.iter().filter(|e| e.done_ms.is_none()).count(), "pending": all.iter().filter(|e| e.state == "pending").count()}));
    Ok(all)
}

/// erase-cost-1tb: the retire step, the archive of erasures that are done and the agent's memory, after a pass (as the
/// perf-erase pass ends).
async fn finish_pass(ctl: &Arc<Ctl>, db: &str, all: &mut [tenant_bench::fx::erase::Erasure], waiting: bool, noted0: u64, since_ms: u64, work_ms: Option<u64>) -> ApiResult<()> {
    use tenant_bench::fx::erase;
    let (newest, served) = served_versions(ctl, db, all).await;
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
        erase::archive(&ctl.store, db, e).await?;
        rec::delete(&ctl.store, &index_path(db, &e.id)).await?;
    }
    remember(db, all, waiting, noted0, since_ms, work_ms);
    Ok(())
}

/// erase-cost-1tb: one pass of `db` with the physical erasure riding ordinary reseals and merges (`RIDE`). For every
/// pending erasure whose deletes are absorbed:
/// 1. if its force time falls within the batch window (or `force_all`: an operator's run), the groups still owing it are
///    resealed now, through the compactor (each forced reseal serves every erasure of that group);
/// 2. once no group owes it, the delta runs that may still hold its (now dead) rows are recorded; each leaves when a
///    merge or a retire replaces it, or, when it is due, when the scheduler cleanses it (only if a probe finds a row);
/// 3. once none is left, a probe of every run that may hold its targets confirms it, and it is "scrubbed": the retire
///    step then waits for GC to delete what the compactor replaced, as for a scrub.
/// A closed database is not opened unless an erasure is due: nothing is resealed while it is closed. Without a compactor
/// the perf-erase batch scrub runs instead.
pub async fn run_pass_ride(ctl: &Arc<Ctl>, db: &str, force_all: bool) -> ApiResult<Vec<tenant_bench::fx::erase::Erasure>> {
    use std::collections::{BTreeMap, BTreeSet};
    use tenant_bench::fx::erase;
    let t0 = std::time::Instant::now();
    let (noted0, since_ms) = (noted(db), now_ms() as u64);
    let c = &ctl.dbs.global.counters;
    let (g0, r0, b0) = (c.get.load(ord()), c.get_range.load(ord()), c.get_bytes.load(ord()));
    let mut all = erase::list(&ctl.store, db).await?;
    let now = now_ms() as u64;
    let horizon = now + window_ms();
    let pending: Vec<usize> = all.iter().enumerate().filter(|(_, e)| e.state == "pending").map(|(i, _)| i).collect();
    let force_all = force_all || force_every(pending.len() as u64);
    let any_due = force_all || pending.iter().any(|&i| force_at(all[i].requested_ms, all[i].deadline_ms) <= horizon);
    let need = pending.iter().map(|&i| all[i].s_e).max().unwrap_or(0);
    let mut waiting = false;
    let mut work_ms: Option<u64> = None;
    let (mut n_resealed, mut n_cleansed, mut n_clean) = (0u64, 0u64, 0usize);
    if need > 0 && (any_due || ctl.dbs.is_open(db)) {
        let t_l = std::time::Instant::now();
        let prep = {
            let mut g = ctl.dbs.lock(db).await;
            g.fx_erase_prepare(need).await?
        };
        let lock_ms = t_l.elapsed().as_millis() as u64;
        let Some(p) = prep else { return Ok(Vec::new()) };
        let Some(fxc) = p.fxc.clone() else {
            // no compactor (sealed compactors off): the perf-erase batch scrub, when the batch is due
            if any_due {
                return run_pass_scrub(ctl, db).await;
            }
            finish_pass(ctl, db, &mut all, false, noted0, since_ms, None).await?;
            return Ok(all);
        };
        super::failpoint("erase.after_prepare").await;
        // the compactor's newest version once its X_d reaches the pending erasures (waited without the lock)
        let t_w = std::time::Instant::now();
        let mut v = loop {
            let v = fxc.current().await;
            if v.x_d >= need || t_w.elapsed() > std::time::Duration::from_secs(30) || fxc.stopped.load(ord()) {
                break v;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        };
        let ready: Vec<usize> = pending.iter().copied().filter(|&i| all[i].s_e <= v.x_d).collect();
        waiting = ready.len() < pending.len();
        let mut targets: BTreeMap<usize, erase::Targets> = BTreeMap::new();
        for &i in &ready {
            match erase::targets_of(&ctl.store, db, &all[i]).await? {
                Some(t) => {
                    targets.insert(i, t);
                }
                None => return Err(ApiError::new(500, "erasure_targets_lost", format!("database {db}: erasure {} lost its targets before its physical erasure", all[i].id))),
            }
        }
        let owing = |v: &tenant_bench::fx::manifest::Version, i: usize| -> ApiResult<Vec<u32>> { Ok(erase::owing_groups(v, &targets[&i], all[i].s_e)?) };
        // the forced work owing now (every group owing a ready erasure, at the measured reseal speed): with it, the
        // erasures are forced early enough to finish well before their deadline on a large database
        let mut owing_all: BTreeSet<u32> = BTreeSet::new();
        for &i in &ready {
            owing_all.extend(owing(&v, i)?);
        }
        let owing_bytes: u64 = v.groups.iter().filter(|g| owing_all.contains(&g.id)).map(|g| g.sealed.as_ref().map_or(0, |l| l.size)).sum();
        let work = owing_bytes.saturating_mul(1000) / RESEAL_BYTES_PER_S.load(ord()).max(1);
        work_ms = Some(work);
        let due: BTreeSet<usize> = ready.iter().copied().filter(|&i| force_all || force_at(all[i].requested_ms, all[i].deadline_ms) <= horizon + work).collect();
        // 1. forced reseals of the groups still owing a due erasure (a few rounds: a reseal can split or merge groups, or
        // lose its commit to another job; what is left is looked at again at the next poll)
        if !due.is_empty() {
            for _round in 0..4 {
                let mut gids: BTreeSet<u32> = BTreeSet::new();
                for &i in &due {
                    gids.extend(owing(&v, i)?);
                }
                if gids.is_empty() {
                    break;
                }
                for gid in gids {
                    ctl.dbs.touch(db);
                    if fxc.stopped.load(ord()) {
                        break;
                    }
                    if fxc.force_reseal_group(gid).await? {
                        n_resealed += 1;
                        super::failpoint("erase.after_forced_reseal").await;
                    }
                }
                v = fxc.current().await;
            }
        }
        // 2. what no group owes any more: the delta runs that may still hold the dead rows (recorded once, then each
        // leaves when the version stops listing it)
        let mut changed: BTreeSet<usize> = BTreeSet::new();
        let listed = |v: &tenant_bench::fx::manifest::Version| -> BTreeSet<String> { v.delta.iter().map(|d| d.path.clone()).collect() };
        let clear_at = |all: &mut Vec<erase::Erasure>, changed: &mut BTreeSet<usize>, v: &tenant_bench::fx::manifest::Version| -> ApiResult<()> {
            let now_listed = listed(v);
            for &i in &ready {
                if !erase::owing_groups(v, &targets[&i], all[i].s_e)?.is_empty() {
                    continue;
                }
                let e = &mut all[i];
                if e.groups_clear_version.is_none() {
                    e.groups_clear_version = Some(v.version);
                    e.delta_owed = erase::owed_delta(v, &targets[&i], e.s_e)?;
                    changed.insert(i);
                } else {
                    let n = e.delta_owed.len();
                    e.delta_owed.retain(|p| now_listed.contains(p));
                    if e.delta_owed.len() != n {
                        changed.insert(i);
                    }
                }
            }
            Ok(())
        };
        clear_at(&mut all, &mut changed, &v)?;
        for &i in &changed {
            erase::save(&ctl.store, db, &all[i]).await?;
        }
        if !changed.is_empty() {
            super::failpoint("erase.after_clear_saved").await;
        }
        // 3. forced cleanses: the delta runs still owed by a due erasure whose groups are all clear; a run is rewritten
        // only if a probe finds one of their rows in it
        let due_clear: Vec<usize> = due.iter().copied().filter(|&i| all[i].groups_clear_version.is_some() && !all[i].delta_owed.is_empty()).collect();
        if !due_clear.is_empty() {
            let paths: BTreeSet<String> = due_clear.iter().flat_map(|&i| all[i].delta_owed.iter().cloned()).collect();
            let items: Vec<(&erase::Targets, u64)> = due_clear.iter().map(|&i| (&targets[&i], all[i].s_e)).collect();
            let batch = erase::Batch::new(&items);
            let view = tenant_bench::fx::view::View::new(Arc::clone(&ctl.store), v.clone(), None)?;
            let mut clean_paths: BTreeSet<String> = BTreeSet::new();
            for h in view.delta.iter().filter(|h| paths.contains(&h.line.path)) {
                ctl.dbs.touch(db);
                if erase::probe_rows(&ctl.store, h, &batch, 1).await?.is_empty() {
                    clean_paths.insert(h.line.path.clone());
                    continue;
                }
                if fxc.cleanse_delta(&h.line.path).await? == Some(true) {
                    n_cleansed += 1;
                    super::failpoint("erase.after_cleanse").await;
                }
            }
            v = fxc.current().await;
            for &i in &due_clear {
                let n = all[i].delta_owed.len();
                all[i].delta_owed.retain(|p| !clean_paths.contains(p));
                if all[i].delta_owed.len() != n {
                    changed.insert(i);
                }
            }
            clear_at(&mut all, &mut changed, &v)?;
        }
        for &i in &due {
            all[i].forced_reseals += n_resealed;
            all[i].forced_cleanses += n_cleansed;
            if n_resealed + n_cleansed > 0 {
                changed.insert(i);
            }
        }
        // 4. the confirmation: every run of the newest version that may hold a target, probed once for all candidates
        let cands: Vec<usize> = ready.iter().copied().filter(|&i| all[i].groups_clear_version.is_some() && all[i].delta_owed.is_empty()).collect();
        let mut marked: BTreeSet<usize> = BTreeSet::new();
        if !cands.is_empty() {
            let items: Vec<(&erase::Targets, u64)> = cands.iter().map(|&i| (&targets[&i], all[i].s_e)).collect();
            let view = tenant_bench::fx::view::View::new(Arc::clone(&ctl.store), v.clone(), None)?;
            let clean = erase::confirm_clean(&ctl.store, &view, &items, None).await?;
            super::failpoint("erase.before_mark_clean").await;
            let mut left = Vec::new();
            let served = clean.iter().filter(|c| **c).count() as u64;
            let first = cands.iter().zip(&clean).find(|(_, c)| **c).map(|(&i, _)| all[i].id.clone());
            for (k, &i) in cands.iter().enumerate() {
                if clean[k] {
                    // the erasures this pass found clean together (the batch of perf-erase's records)
                    all[i].batch = first.clone();
                    all[i].batch_erasures = served;
                    let how = if due.contains(&i) && all[i].forced_reseals + all[i].forced_cleanses > 0 { "forced" } else { "ordinary" };
                    if how == "forced" {
                        erase::FORCED.fetch_add(1, ord());
                    }
                    erase::record_clean(&ctl.store, db, &mut all[i], v.version, how).await?;
                    marked.insert(i);
                    n_clean += 1;
                } else {
                    left.push(i);
                }
            }
            if !left.is_empty() {
                // never expected (the reseal drops every row the erasure deleted): say so; a due erasure falls back to
                // the perf-erase scrub, which removes the rows whatever they are
                eprintln!("erasure pass of {db}: {} erasures with every group resealed past them still have rows in the newest version; {}", left.len(), if left.iter().any(|i| due.contains(i)) { "the batch scrub runs for the due ones" } else { "looked at again later" });
                erase::trace(json!({"ev": "ride_unexpected_rows", "db": db, "erasures": left.len()}));
                if left.iter().any(|i| due.contains(i)) {
                    for &i in &changed {
                        if !marked.contains(&i) {
                            erase::save(&ctl.store, db, &all[i]).await?;
                        }
                    }
                    return run_pass_scrub(ctl, db).await;
                }
            }
        }
        for &i in &changed {
            if !marked.contains(&i) {
                erase::save(&ctl.store, db, &all[i]).await?;
            }
        }
        if n_resealed + n_cleansed + n_clean as u64 > 0 {
            // the writer serves the newest version from now on (installed in memory)
            let mut g = ctl.dbs.lock(db).await;
            g.fx_sync();
        }
        erase::trace(json!({"ev": "ride", "db": db, "lock_ms": lock_ms, "owing_bytes": owing_bytes, "work_ms": work, "ready": ready.len(), "due": due.len(), "forced_reseals": n_resealed, "cleanses": n_cleansed, "clean": n_clean, "version": v.version}));
    }
    finish_pass(ctl, db, &mut all, waiting, noted0, since_ms, work_ms).await?;
    erase::trace(json!({"ev": "pass", "mode": "ride", "db": db, "ms": t0.elapsed().as_millis() as u64,
        "gets_process": c.get.load(ord()) - g0 + c.get_range.load(ord()) - r0, "get_bytes_process": c.get_bytes.load(ord()) - b0,
        "open": all.iter().filter(|e| e.done_ms.is_none()).count(), "pending": all.iter().filter(|e| e.state == "pending").count()}));
    Ok(all)
}

/// `GET /v1/databases/{db}/erasures`: every erasure of the database with its state, deadline and whether it is late.
pub async fn status(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Value> {
    meta::check_name(db)?;
    let all = tenant_bench::fx::erase::list_all(&ctl.store, db).await?;
    let now = now_ms() as u64;
    Ok(json!({ "database": db, "erasures": all.iter().map(|e| e.to_json()).collect::<Vec<_>>(), "overdue": all.iter().filter(|e| e.overdue(now)).count() }))
}

/// `POST /v1/databases/{db}/erasures/run`: the pass now (operators, tests).
pub async fn run_now(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Value> {
    meta::check_name(db)?;
    let _op = ctl.ops.lock(db).await?;
    // operators and tests: every pending erasure is due now
    let all = run_pass(ctl, db, true).await?;
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
                if !due(&db, now, ctl.dbs.is_open(&db)) {
                    continue;
                }
                let Ok(_op) = ctl.ops.lock(&db).await else { continue };
                if let Err(e) = run_pass(&ctl, &db, false).await {
                    eprintln!("erasure pass of {db}: {}", e.message);
                    // look again at the next poll
                    DUE.lock().unwrap().entry(db.clone()).or_default().retry_soon = true;
                }
            }
        }
    });
}
