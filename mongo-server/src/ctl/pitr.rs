//! Point-in-time restore of one database.
//!
//! **What is kept (retention).** A database with a retention window `W` gets *restore points*: slatedb checkpoints named
//! `ret:{unix_ms}` that expire after `W`, taken by a background agent at most once per `interval` while the database
//! is being written (and once when it goes to sleep with unsaved changes, and when retention is switched on). A
//! checkpoint pins the manifest version it was taken at, and through it every SST that version reads (compaction and
//! garbage collection cannot delete them) and the WAL objects of its replay range. The WAL objects written after
//! the checkpoint are kept for `W` as well, by raising the minimum age of the WAL garbage collection of that database
//! to `W` (`tenant_bench::lifecycle::WAL_RETAIN`). Cost on the object store, for a database written continuously:
//!   * storage: the WAL bytes written during `W`, plus every SST that was replaced by a compaction during `W` (a
//!     compaction rewrites data: with `k` compactions over `W` roughly `1 + k` times the live data in the worst case),
//!     plus a few KB of manifests;
//!   * requests: per point one manifest LIST + GET + conditional PUT; the WAL and SSTs are written anyway.
//!
//! **How a restore works.** State at time `T` = the newest restore point at or before `T` plus the WAL objects written
//! after it up to `T` (a WAL object is one durable write batch; its object-store timestamp has one second of
//! resolution). The restore builds that state in a scratch database (a shallow clone of the point, with the extra WAL
//! objects copied in), reads it, and writes the difference between it and the live database into the live database:
//! puts for keys that differ, deletes for keys that should not exist. Nothing else is touched. The difference is
//! idempotent, so a restore interrupted by a crash is simply run again at the next start (`control/ops/{db}.json`).
//! A restore point of "just before the restore" is taken first, so a restore can itself be undone by restoring to the
//! moment before it.

use super::rec::{self, now_ms};
use super::{fork, meta, ops, purge, ApiError, ApiResult, Ctl};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::ObjectStoreExt;
use serde_json::{json, Value};
use slatedb::admin::Admin;
use slatedb::config::{CheckpointOptions, DbReaderOptions};
use slatedb::{DbReader, DbReaderMode};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tenant_bench::lifecycle::{maint_lock, Op};

// ---- retention settings, shared with the garbage collector --------------------------------------

static DEFAULT_WINDOW: AtomicU64 = AtomicU64::new(0);
static WINDOWS: Mutex<Option<HashMap<String, u64>>> = Mutex::new(None);

/// Retention window of a database in seconds (its own setting, else the process default).
pub fn window_secs(db: &str) -> u64 {
    WINDOWS.lock().unwrap().as_ref().and_then(|m| m.get(db).copied()).unwrap_or_else(|| DEFAULT_WINDOW.load(Relaxed))
}

fn set_window(db: &str, w: Option<u64>) {
    let mut g = WINDOWS.lock().unwrap();
    let m = g.get_or_insert_with(HashMap::new);
    match w {
        Some(w) => drop(m.insert(db.to_string(), w)),
        None => drop(m.remove(db)),
    }
}

fn wal_retention(db: &str) -> Duration {
    // Test control: `CTL_DISABLE_WAL_RETENTION=1` shows what the retention of WAL objects prevents (see the tests).
    static OFF: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if *OFF.get_or_init(|| std::env::var("CTL_DISABLE_WAL_RETENTION").is_ok()) {
        return Duration::ZERO;
    }
    Duration::from_secs(window_secs(db))
}

pub fn install_wal_retention(ctl: &Ctl) {
    DEFAULT_WINDOW.store(ctl.args.retention_window_secs, Relaxed);
    let _ = tenant_bench::lifecycle::WAL_RETAIN.set(wal_retention);
}

pub fn marker_path(db: &str) -> Path {
    rec::path(&["control", "retention", db])
}

/// At start: read the per-database settings of every database that has one.
pub async fn load_settings(ctl: &Arc<Ctl>) {
    let marks: Vec<Path> = ctl.store.list(Some(&rec::path(&["control", "retention"]))).filter_map(|m| async { m.ok().map(|m| m.location) }).collect().await;
    for m in marks {
        let db = m.filename().unwrap_or("").to_string();
        if let Ok(Some(meta_v)) = rec::get(&ctl.store, &meta::meta_path(&db)).await {
            if let Some(w) = meta_v["retention"]["window_secs"].as_u64() {
                set_window(&db, Some(w));
            }
        }
    }
}

/// Settings in effect for a database: `(window_secs, interval_secs)`.
pub fn settings_of(ctl: &Ctl, db: &str) -> (u64, u64) {
    let interval = RETENTION_INTERVALS.lock().unwrap().as_ref().and_then(|m| m.get(db).copied()).unwrap_or(ctl.args.retention_interval_secs);
    (window_secs(db), interval)
}

static RETENTION_INTERVALS: Mutex<Option<HashMap<String, u64>>> = Mutex::new(None);

/// Forget a database's in-memory retention settings (it was erased).
pub fn forget(db: &str) {
    set_window(db, None);
    set_interval(db, None);
}

fn set_interval(db: &str, i: Option<u64>) {
    let mut g = RETENTION_INTERVALS.lock().unwrap();
    let m = g.get_or_insert_with(HashMap::new);
    match i {
        Some(i) => drop(m.insert(db.to_string(), i)),
        None => drop(m.remove(db)),
    }
}

// ---- restore points ---------------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Point {
    pub id: uuid::Uuid,
    pub at_ms: i64,
    pub manifest_id: u64,
    pub expires_ms: Option<i64>,
}

pub async fn points(ctl: &Ctl, db: &str) -> anyhow::Result<Vec<Point>> {
    let admin = Admin::builder(purge::tenant_path(db), Arc::clone(&ctl.store)).build();
    let mut v: Vec<Point> = admin
        .list_checkpoints(None)
        .await?
        .into_iter()
        .filter_map(|c| {
            let n = c.name.as_deref()?;
            let at = n.strip_prefix("ret:")?.split(':').next()?.parse::<i64>().ok()?;
            Some(Point { id: c.id, at_ms: at, manifest_id: c.manifest_id, expires_ms: c.expire_time.map(|t| t.timestamp_millis()) })
        })
        .collect();
    v.sort_by_key(|p| p.at_ms);
    Ok(v)
}

/// Take a restore point now: the manifest as it is, pinned for the retention window.
pub async fn make_point(ctl: &Ctl, db: &str, window_secs: u64, tag: &str) -> anyhow::Result<Point> {
    let admin = Admin::builder(purge::tenant_path(db), Arc::clone(&ctl.store)).build();
    let now = now_ms();
    let name = if tag.is_empty() { format!("ret:{now}") } else { format!("ret:{now}:{tag}") };
    let cp = admin.create_detached_checkpoint(&CheckpointOptions { lifetime: Some(Duration::from_secs(window_secs.max(1))), source: None, name: Some(name) }).await?;
    Ok(Point { id: cp.id, at_ms: now, manifest_id: cp.manifest_id, expires_ms: Some(now + window_secs as i64 * 1000) })
}

/// Delete restore points whose window has passed (slatedb's garbage collector would, but not for a sleeping database).
pub async fn prune(ctl: &Ctl, db: &str, all: bool) -> anyhow::Result<usize> {
    let admin = Admin::builder(purge::tenant_path(db), Arc::clone(&ctl.store)).build();
    let now = now_ms();
    let mut n = 0;
    for p in points(ctl, db).await? {
        if all || p.expires_ms.map_or(false, |e| e < now) {
            admin.delete_checkpoint(p.id).await?;
            n += 1;
        }
    }
    Ok(n)
}

pub async fn set_retention(ctl: &Arc<Ctl>, db: &str, window: u64, interval: Option<u64>) -> ApiResult<Value> {
    meta::check_name(db)?;
    let _op = ctl.ops.lock(db).await?;
    if !meta::exists(ctl, db).await? {
        return Err(ApiError::not_found(format!("database {db} does not exist")));
    }
    let interval_v = interval.unwrap_or(ctl.args.retention_interval_secs);
    if window > 0 && interval_v == 0 {
        return Err(ApiError::bad("interval_secs must be at least 1"));
    }
    meta::modify(ctl, db, |m| m["retention"] = json!({ "window_secs": window, "interval_secs": interval_v })).await?;
    if window > 0 {
        rec::put(&ctl.store, &marker_path(db), &json!({ "window_secs": window })).await?;
        set_window(db, Some(window));
        set_interval(db, Some(interval_v));
        // A first point right away, so the database can be restored to any time from now on. The database is closed
        // and flushed first when it is open, so the point holds everything acknowledged so far.
        let mut g = ctl.dbs.lock(db).await;
        if g.is_open() {
            g.close().await?;
        }
        drop(g);
        make_point(ctl, db, window, "enabled").await?;
    } else {
        rec::delete(&ctl.store, &marker_path(db)).await?;
        set_window(db, Some(0));
        set_interval(db, None);
        prune(ctl, db, true).await?;
    }
    drop(_op);
    list_points(ctl, db).await
}

pub async fn list_points(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Value> {
    meta::check_name(db)?;
    if !meta::exists(ctl, db).await? {
        return Err(ApiError::not_found(format!("database {db} does not exist")));
    }
    let pts = points(ctl, db).await?;
    let (w, i) = settings_of(ctl, db);
    let now = now_ms();
    Ok(json!({
        "database": db,
        "retention": { "window_secs": w, "interval_secs": i },
        "points": pts.iter().map(|p| json!({ "id": p.id.to_string(), "at": rec::rfc3339(p.at_ms), "at_ms": p.at_ms, "manifest_id": p.manifest_id, "expires_at_ms": p.expires_ms })).collect::<Vec<_>>(),
        "earliest_restorable": pts.first().map(|p| rec::rfc3339(p.at_ms)),
        "earliest_restorable_ms": pts.first().map(|p| p.at_ms),
        "latest_restorable_ms": now,
        "precision": "the object store's timestamp of a WAL object (one write batch), 1 second resolution on S3",
    }))
}

// ---- restore ---------------------------------------------------------------------------------------

fn wal_id(p: &Path) -> Option<u64> {
    p.filename()?.strip_suffix(".sst")?.parse().ok()
}

pub struct Plan {
    pub point: Point,
    pub target_ms: i64,
}

/// Choose the point to start from for time `t`.
async fn choose(ctl: &Ctl, db: &str, t: i64) -> ApiResult<Point> {
    let (w, _) = settings_of(ctl, db);
    let pts = points(ctl, db).await?;
    if w == 0 && pts.is_empty() {
        return Err(ApiError::conflict("retention_off", format!("database {db} has no retention window; enable one with PUT /v1/databases/{db}/retention")));
    }
    let earliest = pts.first().map(|p| p.at_ms);
    match earliest {
        None => Err(ApiError::conflict("no_restore_points", format!("database {db} has no restore points"))),
        Some(e) if t < e => Err(ApiError::bad(format!("{} is before the earliest restorable time {}", rec::rfc3339(t), rec::rfc3339(e)))),
        Some(_) => Ok(pts.into_iter().filter(|p| p.at_ms <= t).last().unwrap()),
    }
}

#[derive(Default, Debug, Clone)]
pub struct Diff {
    pub puts: u64,
    pub deletes: u64,
    pub same: u64,
    pub bytes_put: u64,
}

pub async fn restore(ctl: &Arc<Ctl>, db: &str, to_ms: i64, dry_run: bool) -> ApiResult<Value> {
    meta::check_name(db)?;
    let _op = ctl.ops.lock(db).await?;
    if !meta::exists(ctl, db).await? {
        return Err(ApiError::not_found(format!("database {db} does not exist")));
    }
    let now = now_ms();
    if to_ms > now + 1000 {
        return Err(ApiError::bad("the time to restore to is in the future"));
    }
    let point = choose(ctl, db, to_ms).await?;
    let opid = format!("{:016x}", rand::random::<u64>());
    let detail = json!({ "target_ms": to_ms, "point_id": point.id.to_string(), "point_at_ms": point.at_ms, "staging": opid, "dry_run": dry_run });
    if !dry_run {
        ops::begin(ctl, db, "restore", detail.clone()).await?;
    }
    super::failpoint("restore.after_intent").await;
    let t0 = std::time::Instant::now();
    let before = ctl.usage.today_sum(&[db]);
    let r = run(ctl, db, &detail, !dry_run).await;
    match r {
        Ok(mut out) => {
            if !dry_run {
                ops::finish(ctl, db).await?;
            }
            ctl.usage.touch(db);
            out["elapsed_ms"] = json!(t0.elapsed().as_millis() as u64);
            out["cost"] = ctl.usage.today_sum(&[db]).minus(&before).to_json();
            Ok(out)
        }
        Err(e) => {
            // Nothing in the live database changes before the apply phase; a failure before it leaves nothing to undo.
            if dry_run {
                return Err(e);
            }
            let intent = ops::get(ctl, db).await?;
            match intent {
                Some(o) if o["phase"] == "applying" => {
                    // half applied: roll forward now (the next start would do the same)
                    eprintln!("restore of {db} failed while applying ({e}); retrying");
                    resume(ctl, db, &o).await?;
                    Err(ApiError::new(500, "restore_retried", format!("the restore failed once ({e}) and was completed by a retry; check the result with GET /v1/databases/{db}")))
                }
                _ => {
                    purge_staging(ctl, db, &opid).await;
                    ops::finish(ctl, db).await?;
                    Err(e)
                }
            }
        }
    }
}

/// Everything of a restore after the intent is recorded. Safe to run again from the top.
async fn run(ctl: &Arc<Ctl>, db: &str, detail: &Value, apply: bool) -> ApiResult<Value> {
    let target = detail["target_ms"].as_i64().unwrap_or(0);
    let opid = detail["staging"].as_str().unwrap_or("").to_string();
    let pid = uuid::Uuid::parse_str(detail["point_id"].as_str().unwrap_or("")).map_err(|e| ApiError::new(500, "internal", e.to_string()))?;
    let mut g = ctl.dbs.lock(db).await;
    if g.active_txns > 0 {
        return Err(ApiError::conflict("transactions_open", "multi-document transactions are open on this database"));
    }
    // Quiesce: flushed and closed, no process writing, no compaction or GC pass running.
    fork::quiesce(ctl, &mut g, db).await?;
    let lock = maint_lock(&ctl.lc.maint_locks, db);
    let _maint = lock.lock().await;
    ctl.dbs.leases.acquire(db).await?;
    let result = run_locked(ctl, &mut g, db, detail, target, &opid, pid, apply).await;
    purge_staging(ctl, db, &opid).await;
    if result.is_ok() || !apply {
        // leave the database asleep and clean: the next command wakes it
        let _ = g.close().await;
    }
    ctl.dbs.leases.release(db).await;
    result
}

#[allow(clippy::too_many_arguments)]
async fn run_locked(ctl: &Arc<Ctl>, g: &mut crate::dbs::Entry, db: &str, detail: &Value, target: i64, opid: &str, pid: uuid::Uuid, apply: bool) -> ApiResult<Value> {
    let admin = Admin::builder(purge::tenant_path(db), Arc::clone(&ctl.store)).build();
    // Safety point of the current state, so this restore can be undone by restoring to now.
    let (w, _) = settings_of(ctl, db);
    if apply {
        make_point(ctl, db, w.max(1), "prerestore").await?;
    }
    // ---- stage: the state at `target`, as a scratch database ----
    let staging = format!("control/staging/{db}/{opid}");
    let point = points(ctl, db).await?.into_iter().find(|p| p.id == pid).ok_or_else(|| ApiError::conflict("point_expired", "the restore point expired while the restore was starting"))?;
    let pm = admin.read_manifest(Some(point.manifest_id)).await.map_err(anyhow::Error::from)?.ok_or_else(|| ApiError::conflict("point_missing", "the manifest of the restore point is gone"))?;
    let replay_after = pm.replay_after_wal_id();
    ops::phase(ctl, db, "staging").await?;
    let sadmin = Admin::builder(staging.clone(), Arc::clone(&ctl.store)).build();
    let mut spec = slatedb::CloneSourceSpec::new(purge::tenant_path(db));
    spec.checkpoint = Some(pid);
    sadmin.create_clone_builder_from_source(spec).build().await.map_err(anyhow::Error::from)?;
    let latest_next_wal = admin.read_manifest(None).await.map_err(anyhow::Error::from)?.map(|m| m.next_wal_sst_id()).unwrap_or(0);
    let wal_copied = copy_wal_forward(ctl, db, &staging, replay_after, latest_next_wal, target).await?;
    super::failpoint("restore.after_staging").await;
    // ---- diff ----
    let diff = diff_and_apply(ctl, g, db, &staging, apply, opid).await?;
    Ok(json!({
        "database": db, "restored_to": rec::rfc3339(target), "restored_to_ms": target, "applied": apply,
        "from_point": { "id": pid.to_string(), "at": rec::rfc3339(point.at_ms), "wal_objects_replayed_after_it": wal_copied },
        "keys": { "written": diff.puts, "deleted": diff.deletes, "unchanged": diff.same, "bytes_written": diff.bytes_put },
        "precision": "1 s (object-store timestamp of the WAL objects)",
    }))
}

/// Copy the WAL objects after the point's replay watermark, up to those written at or before `target`, into the
/// staging database. The clone already holds the ones inside the point's own WAL range.
async fn copy_wal_forward(ctl: &Ctl, db: &str, staging: &str, replay_after: u64, next_wal: u64, target: i64) -> ApiResult<usize> {
    let src_dir = Path::from(format!("{}/wal/", purge::tenant_path(db)));
    let dst_dir = Path::from(format!("{staging}/wal/"));
    let mut src: Vec<(u64, Path, i64)> = ctl.store.list(Some(&src_dir)).try_collect::<Vec<_>>().await.map_err(anyhow::Error::from)?.into_iter().filter_map(|m| Some((wal_id(&m.location)?, m.location, m.last_modified.timestamp_millis()))).collect();
    src.sort_by_key(|x| x.0);
    let have: std::collections::HashSet<u64> = ctl.store.list(Some(&dst_dir)).try_collect::<Vec<_>>().await.map_err(anyhow::Error::from)?.into_iter().filter_map(|m| wal_id(&m.location)).collect();
    // WAL ids are consecutive (every flush and every writer fence takes the next one), so a missing id below the first
    // object that is newer than `target` is a WAL object that was written at or before `target` and no longer exists:
    // refuse rather than return a state that silently lacks it.
    let mut expect = replay_after + 1;
    let mut copied = 0;
    let mut newer_found = false;
    for (id, path, mtime) in src.into_iter().filter(|x| x.0 > replay_after) {
        if id != expect {
            return Err(ApiError::conflict("retention_gap", format!("WAL object {expect} needed to replay up to {} is missing (the next one is {id}); the retention window was changed or the WAL was collected", rec::rfc3339(target))));
        }
        if mtime > target {
            newer_found = true;
            break;
        }
        expect += 1;
        if !have.contains(&id) {
            ctl.store.copy(&path, &Path::from(format!("{staging}/wal/{id:020}.sst"))).await.map_err(anyhow::Error::from)?;
            copied += 1;
        }
    }
    if !newer_found && expect < next_wal {
        return Err(ApiError::conflict("retention_gap", format!("WAL objects {expect}..{next_wal} are missing; the WAL was collected")));
    }
    Ok(copied)
}

async fn open_reader(ctl: &Ctl, path: &str, skip_replay: bool) -> anyhow::Result<DbReader> {
    let mut o = DbReaderOptions::default();
    o.skip_wal_replay = skip_replay;
    o.manifest_poll_interval = Duration::from_secs(3600);
    Ok(DbReader::builder(path.to_string(), Arc::clone(&ctl.store)).with_reader_mode(DbReaderMode::FollowLatest).with_options(o).build().await?)
}

async fn diff_and_apply(ctl: &Arc<Ctl>, g: &mut crate::dbs::Entry, db: &str, staging: &str, apply: bool, _opid: &str) -> ApiResult<Diff> {
    let past = open_reader(ctl, staging, false).await?;
    let live = open_reader(ctl, &purge::tenant_path(db), true).await?;
    let opts = slatedb::config::ScanOptions { read_ahead_bytes: 4 << 20, max_fetch_tasks: 4, cache_blocks: false, ..Default::default() };
    let range = (std::ops::Bound::<Vec<u8>>::Unbounded, std::ops::Bound::<Vec<u8>>::Unbounded);
    let mut pi = past.scan_with_options(range.clone(), &opts).await.map_err(anyhow::Error::from)?;
    let mut li = live.scan_with_options(range, &opts).await.map_err(anyhow::Error::from)?;
    let mut d = Diff::default();
    let mut batch: Vec<Op> = Vec::new();
    let mut batch_bytes = 0usize;
    let mut applying = false;
    let (mut p, mut l) = (pi.next().await.map_err(anyhow::Error::from)?, li.next().await.map_err(anyhow::Error::from)?);
    macro_rules! flush {
        () => {
            if apply && !batch.is_empty() {
                if !applying {
                    ops::phase(ctl, db, "applying").await?;
                    applying = true;
                }
                g.apply(std::mem::take(&mut batch)).await?;
                batch_bytes = 0;
                super::failpoint("restore.mid_apply").await;
            }
        };
    }
    loop {
        use std::cmp::Ordering::*;
        let ord = match (&p, &l) {
            (None, None) => break,
            (Some(_), None) => Less,
            (None, Some(_)) => Greater,
            (Some(a), Some(b)) => a.key.cmp(&b.key),
        };
        match ord {
            Less => {
                let kv = p.take().unwrap();
                d.puts += 1;
                d.bytes_put += kv.value.len() as u64;
                batch_bytes += kv.key.len() + kv.value.len();
                batch.push(Op::Put(kv.key.to_vec(), kv.value.to_vec()));
                p = pi.next().await.map_err(anyhow::Error::from)?;
            }
            Greater => {
                let kv = l.take().unwrap();
                d.deletes += 1;
                batch.push(Op::Delete(kv.key.to_vec()));
                l = li.next().await.map_err(anyhow::Error::from)?;
            }
            Equal => {
                let (a, b) = (p.take().unwrap(), l.take().unwrap());
                if a.value == b.value {
                    d.same += 1;
                } else {
                    d.puts += 1;
                    d.bytes_put += a.value.len() as u64;
                    batch_bytes += a.key.len() + a.value.len();
                    batch.push(Op::Put(a.key.to_vec(), a.value.to_vec()));
                }
                p = pi.next().await.map_err(anyhow::Error::from)?;
                l = li.next().await.map_err(anyhow::Error::from)?;
            }
        }
        if batch.len() >= 1000 || batch_bytes >= 4 << 20 {
            flush!();
            if !apply {
                batch.clear();
                batch_bytes = 0;
            }
        }
    }
    flush!();
    let _ = Bytes::new();
    past.close().await.map_err(anyhow::Error::from)?;
    live.close().await.map_err(anyhow::Error::from)?;
    Ok(d)
}

/// Delete the scratch database of a restore: its objects, and the pin its clone took in the live database.
pub async fn purge_staging(ctl: &Ctl, db: &str, opid: &str) {
    let path = format!("control/staging/{db}/{opid}");
    let r: anyhow::Result<()> = async {
        // release every pin the scratch database holds (in the live database and in the live database's own ancestors)
        purge::release_pins_at(&ctl.store, &path).await?;
        purge::delete_prefix(&ctl.store, &Path::from(path.clone())).await?;
        Ok(())
    }
    .await;
    if let Err(e) = r {
        eprintln!("restore: removing the scratch database {path}: {e:#}");
    }
}

/// Finish a restore that a crash interrupted (rolls forward: the difference is applied again from the top).
pub async fn resume(ctl: &Arc<Ctl>, db: &str, op: &Value) -> ApiResult<()> {
    // The scratch database of the interrupted run is thrown away; a new one is built (same point, same WAL).
    if let Some(old) = op["staging"].as_str() {
        purge_staging(ctl, db, old).await;
    }
    let mut detail = op.clone();
    detail["staging"] = json!(format!("{:016x}", rand::random::<u64>()));
    for attempt in 0..40 {
        match run(ctl, db, &detail, true).await {
            Ok(_) => {
                ops::finish(ctl, db).await?;
                return Ok(());
            }
            Err(e) if e.code == "owned_elsewhere" && attempt < 39 => tokio::time::sleep(Duration::from_millis(1000)).await,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

// ---- the agent that takes restore points ---------------------------------------------------------------

#[derive(Default)]
struct Seen {
    /// Time of the newest point of the database; read from its checkpoints the first time it is needed.
    last_point_ms: Option<i64>,
    /// The entry's write counter when the newest point was taken (writes since then make the database "dirty").
    last_seq: Option<u64>,
}

/// Every second: for each database that has a retention window, take a point if it was written (here) since the last
/// one and the interval has passed; when a written database has gone to sleep, take the final point of that session
/// (its manifest is complete then, so the point holds the exact end state).
pub fn spawn_agent(ctl: &Arc<Ctl>) {
    let ctl = Arc::clone(ctl);
    tokio::spawn(async move {
        let mut seen: HashMap<String, Seen> = HashMap::new();
        let mut tick = tokio::time::interval(Duration::from_millis(1000));
        let prune_every = Duration::from_secs(ctl.args.retention_prune_secs.max(1));
        let mut next_prune = std::time::Instant::now() + prune_every;
        loop {
            tick.tick().await;
            let Some(_gate) = super::agent_gate().await else { return };
            let entries = ctl.dbs.entries_snapshot();
            for e in entries {
                let (name, writer, _open, seq) = match e.try_read() {
                    Ok(g) => (g.name().to_string(), g.is_writer_open(), g.is_open(), g.seq),
                    Err(_) => continue,
                };
                let (w, interval) = settings_of(&ctl, &name);
                if w == 0 {
                    seen.remove(&name);
                    continue;
                }
                let first = !seen.contains_key(&name);
                let s = seen.entry(name.clone()).or_default();
                if first && !writer {
                    s.last_seq = Some(seq); // nothing written by this process yet that no point covers
                }
                let dirty = s.last_seq != Some(seq);
                if !dirty {
                    continue;
                }
                let now = now_ms();
                if s.last_point_ms.is_none() {
                    s.last_point_ms = Some(points(&ctl, &name).await.ok().and_then(|p| p.last().map(|p| p.at_ms)).unwrap_or(0));
                }
                let due = if writer {
                    now - s.last_point_ms.unwrap_or(0) >= interval as i64 * 1000
                } else {
                    true // the writer is gone (closed; a reader has no unflushed writes): take the final point now
                };
                if !due {
                    continue;
                }
                let Some(_op) = ctl.ops.try_lock(&name) else { continue };
                if !meta::exists(&ctl, &name).await.unwrap_or(false) {
                    seen.remove(&name);
                    continue;
                }
                match make_point(&ctl, &name, w, if writer { "" } else { "close" }).await {
                    Ok(_) => {
                        let s = seen.get_mut(&name).unwrap();
                        s.last_point_ms = Some(now);
                        s.last_seq = Some(seq);
                    }
                    Err(err) => eprintln!("restore point of {name}: {err:#}"),
                }
            }
            if std::time::Instant::now() >= next_prune {
                next_prune = std::time::Instant::now() + prune_every;
                let marks: Vec<Path> = ctl.store.list(Some(&rec::path(&["control", "retention"]))).filter_map(|m| async { m.ok().map(|m| m.location) }).collect().await;
                for m in marks {
                    let db = m.filename().unwrap_or("").to_string();
                    if let Some(_op) = ctl.ops.try_lock(&db) {
                        let _ = prune(&ctl, &db, false).await;
                    }
                }
            }
        }
    });
}
