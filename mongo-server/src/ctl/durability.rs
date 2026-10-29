//! Relaxed durability of one database (`PUT /v1/databases/{db}/durability`).
//!
//! With `relaxed_flush_ms` set (1000 to 10000, per database; `--relaxed-flush-ms` is the default of databases without a
//! setting, 0 = off), a write whose write concern is `{w: 1, j: false}` (or `w: 0`) is acknowledged once it is applied in
//! memory, before it is in object storage. The database's writer then flushes its WAL on its own at least every
//! `relaxed_flush_ms` (one PUT for every write of that interval), so such a write becomes durable within that time (plus
//! the PUT). A write with `j: true`, `w: "majority"`, no write concern at all, or in a transaction is durable before it
//! is acknowledged, exactly as without the setting, and it also makes durable every relaxed write staged before it.
//! Reads of such a database see the relaxed writes at once (read-your-writes, as mongod with `j: false`): a process that
//! dies before the flush loses those writes, and a reader may have seen them. A clean shutdown or idle close flushes.
//! Setting the value closes the database if it is open, so its writer reopens with the new flush interval.

use super::{meta, rec, ApiError, ApiResult, Ctl};
use object_store::path::Path;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

static DEFAULT_MS: AtomicU64 = AtomicU64::new(0);
static SETTINGS: Mutex<Option<HashMap<String, u64>>> = Mutex::new(None);

pub const MIN_MS: u64 = 1000;
pub const MAX_MS: u64 = 10_000;

/// The relaxed flush interval of a database in milliseconds (0 = off: every write is durable before its acknowledgement).
pub fn relaxed_ms(db: &str) -> u64 {
    SETTINGS.lock().unwrap().as_ref().and_then(|m| m.get(db).copied()).unwrap_or_else(|| DEFAULT_MS.load(Relaxed))
}

fn window(db: &str) -> Duration {
    Duration::from_millis(relaxed_ms(db))
}

fn set_local(db: &str, ms: Option<u64>) {
    let mut g = SETTINGS.lock().unwrap();
    let m = g.get_or_insert_with(HashMap::new);
    match ms {
        Some(v) => drop(m.insert(db.to_string(), v)),
        None => drop(m.remove(db)),
    }
}

/// Check the process default (`--relaxed-flush-ms`) and wire the setting into the writers.
pub fn install(default_ms: u64) -> anyhow::Result<()> {
    anyhow::ensure!(default_ms == 0 || (MIN_MS..=MAX_MS).contains(&default_ms), "--relaxed-flush-ms must be 0 (off) or between {MIN_MS} and {MAX_MS}");
    DEFAULT_MS.store(default_ms, Relaxed);
    let _ = tenant_bench::lifecycle::RELAXED_WINDOW.set(window);
    Ok(())
}

fn marker_path(db: &str) -> Path {
    rec::path(&["control", "durability", db])
}

/// At start: the settings of every database that has one (one LIST of `control/durability/`).
pub async fn load_settings(ctl: &Arc<Ctl>) {
    use futures::StreamExt;
    let marks: Vec<Path> = ctl.store.list(Some(&rec::path(&["control", "durability"]))).filter_map(|m| async { m.ok().map(|m| m.location) }).collect().await;
    for m in marks {
        let db = m.filename().unwrap_or("").to_string();
        if let Ok(Some(v)) = rec::get(&ctl.store, &m).await {
            if let Some(ms) = v["relaxed_flush_ms"].as_u64() {
                set_local(&db, Some(ms));
            }
        }
    }
}

/// Forget a database's setting (it was erased); true if it had one (then its marker object exists).
pub fn forget(db: &str) -> bool {
    SETTINGS.lock().unwrap().as_mut().map_or(false, |m| m.remove(db).is_some())
}

pub async fn get(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Value> {
    meta::check_name(db)?;
    if !meta::exists(ctl, db).await? {
        return Err(ApiError::not_found(format!("database {db} does not exist")));
    }
    Ok(json!({ "database": db, "relaxed_flush_ms": relaxed_ms(db) }))
}

pub async fn set(ctl: &Arc<Ctl>, db: &str, ms: u64) -> ApiResult<Value> {
    meta::check_name(db)?;
    if ms != 0 && !(MIN_MS..=MAX_MS).contains(&ms) {
        return Err(ApiError::bad(format!("relaxed_flush_ms must be 0 (off) or between {MIN_MS} and {MAX_MS}")));
    }
    let _op = ctl.ops.lock(db).await?;
    if !meta::exists(ctl, db).await? {
        return Err(ApiError::not_found(format!("database {db} does not exist")));
    }
    meta::modify(ctl, db, |m| m["durability"] = json!({ "relaxed_flush_ms": ms })).await?;
    rec::put(&ctl.store, &marker_path(db), &json!({ "relaxed_flush_ms": ms })).await?;
    set_local(db, Some(ms));
    // The writer's flush interval is fixed when it opens: close the database (flushing everything) so the next open uses
    // the new value.
    let mut g = ctl.dbs.lock(db).await;
    if g.is_open() {
        g.close().await?;
    }
    drop(g);
    drop(_op);
    get(ctl, db).await
}
