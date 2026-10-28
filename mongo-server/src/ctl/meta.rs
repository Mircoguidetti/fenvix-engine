//! The database registry: `control/dbs/{db}/meta.json` plus what can be read off the tenant itself.
//!
//! A database exists when `tenants/{db}/` holds an object other than `_lease` and `_clean` (the engine creates
//! databases implicitly on first write, so most databases have no registry record until the control layer first sees
//! them; then `created_at` is the oldest object's timestamp and `created_via` says `discovered`).

use super::rec::{self, now_ms};
use super::{ApiError, ApiResult, Ctl};
use crate::dbs::valid_db_name;
use futures::StreamExt;
use object_store::path::Path;
use object_store::ObjectStoreExt;
use serde_json::{json, Value};
use std::sync::Arc;

pub fn meta_path(db: &str) -> Path {
    rec::path(&["control", "dbs", db, "meta.json"])
}

pub fn tenant_prefix(db: &str) -> Path {
    rec::path(&["tenants", db])
}

pub fn check_name(db: &str) -> ApiResult<()> {
    if !valid_db_name(db) {
        return Err(ApiError::bad(format!("invalid database name {db:?}")));
    }
    if matches!(db, "admin" | "local" | "config") {
        return Err(ApiError::bad(format!("{db} is a reserved database name")));
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub struct Footprint {
    pub objects: u64,
    pub bytes: u64,
    pub oldest_ms: Option<i64>,
    pub newest_ms: Option<i64>,
}

/// Objects under a prefix with their total size (a full listing: one LIST request per 1000 objects).
pub async fn footprint(ctl: &Ctl, prefix: &Path) -> anyhow::Result<Footprint> {
    let mut f = Footprint::default();
    let mut it = ctl.store.list(Some(prefix));
    while let Some(m) = it.next().await {
        let m = m?;
        f.objects += 1;
        f.bytes += m.size;
        let t = m.last_modified.timestamp_millis();
        f.oldest_ms = Some(f.oldest_ms.map_or(t, |o| o.min(t)));
        f.newest_ms = Some(f.newest_ms.map_or(t, |o| o.max(t)));
    }
    Ok(f)
}

/// Does the database exist (has data objects, not just a lease or marker)?
pub async fn exists(ctl: &Ctl, db: &str) -> anyhow::Result<bool> {
    let mut it = ctl.store.list(Some(&tenant_prefix(db)));
    while let Some(m) = it.next().await {
        let name = m?.location.filename().unwrap_or("").to_string();
        if name != "_lease" && name != "_clean" {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn default_meta(db: &str, created_ms: i64, via: &str) -> Value {
    json!({ "v": 1, "name": db, "created_at_ms": created_ms, "created_via": via, "forked_from": null, "keep_awake": false, "keep_awake_since_ms": null, "retention": null })
}

/// The registry record, created on first sight for databases the engine made implicitly.
pub async fn ensure(ctl: &Ctl, db: &str) -> anyhow::Result<Value> {
    if let Some(m) = rec::get(&ctl.store, &meta_path(db)).await? {
        return Ok(m);
    }
    let fp = footprint(ctl, &tenant_prefix(db)).await?;
    let created = fp.oldest_ms.unwrap_or_else(now_ms);
    let fresh = default_meta(db, created, "discovered");
    let stored = rec::update(&ctl.store, &meta_path(db), |cur| Ok(if cur.is_none() { Some(fresh.clone()) } else { None })).await?;
    Ok(stored.unwrap_or(fresh))
}

pub async fn modify(ctl: &Ctl, db: &str, f: impl Fn(&mut Value)) -> anyhow::Result<Value> {
    ensure(ctl, db).await?;
    let v = rec::update(&ctl.store, &meta_path(db), |cur| {
        let mut m = cur.unwrap_or_else(|| default_meta(db, now_ms(), "discovered"));
        f(&mut m);
        Ok(Some(m))
    })
    .await?;
    Ok(v.unwrap())
}

/// `asleep` / `awake` and where. Awake means some process has the tenant open: this one (entry open), or another one
/// (a live lease held by someone else).
pub async fn state_of(ctl: &Ctl, db: &str) -> anyhow::Result<Value> {
    let h = ctl.dbs.lock_handle(db);
    let local = match h.try_read() {
        Ok(g) => Some((g.is_open(), g.mode())),
        Err(_) => None, // locked by an operation or a command right now
    };
    if let Some((true, mode)) = local {
        return Ok(json!({ "state": "awake", "mode": mode, "holder": ctl.holder }));
    }
    if let Some((holder, exp)) = read_lease(ctl, db).await? {
        if !holder.is_empty() && exp > now_ms() {
            return Ok(json!({ "state": "awake", "mode": "unknown", "holder": holder }));
        }
    }
    Ok(json!({ "state": "asleep", "mode": "closed", "holder": null }))
}

/// Full description of one database.
pub async fn describe(ctl: &Arc<Ctl>, db: &str, with_storage: bool) -> ApiResult<Value> {
    if !exists(ctl, db).await? {
        return Err(ApiError::not_found(format!("database {db} does not exist")));
    }
    let fp = if with_storage { Some(footprint(ctl, &tenant_prefix(db)).await?) } else { None };
    let m = ensure(ctl, db).await?;
    let st = state_of(ctl, db).await?;
    let (open_ops, last_active) = (super::ops::active_op(ctl, db).await?, ctl.usage.last_active_ms(db));
    let last_active_ms = match (last_active, fp.as_ref().and_then(|f| f.newest_ms)) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    };
    let retention = effective_retention(ctl, &m);
    Ok(json!({
        "name": db,
        "state": st["state"], "mode": st["mode"], "holder": st["holder"],
        "keep_awake": m["keep_awake"],
        "keep_awake_lease": if m["keep_awake"] == json!(true) { super::pin::lease_of(ctl, db).await? } else { Value::Null },
        "storage_bytes": fp.as_ref().map(|f| f.bytes),
        "storage_objects": fp.as_ref().map(|f| f.objects),
        "created_at": rec::rfc3339(m["created_at_ms"].as_i64().unwrap_or(0)),
        "created_at_ms": m["created_at_ms"],
        "created_via": m["created_via"],
        "last_active_at": last_active_ms.map(rec::rfc3339),
        "last_active_at_ms": last_active_ms,
        "forked_from": m["forked_from"],
        "retention": retention,
        "operation": open_ops,
    }))
}

/// Names of the databases that exist (tenants), sorted.
pub async fn names(ctl: &Ctl) -> anyhow::Result<Vec<String>> {
    ctl.dbs.list_databases().await
}

/// Create an empty database: the first write makes the tenant exist (an empty collection list).
/// A new database starts with no users and no roles: a name that still has some (defined before the database existed, or
/// left by an erase of an older version) is refused, never silently adopted.
pub async fn ensure_no_auth(ctl: &Ctl, db: &str) -> ApiResult<()> {
    if ctl.server.database_has_auth(db).await.map_err(|e| ApiError::new(500, "internal", e.to_string()))? {
        return Err(ApiError::conflict("users_exist", format!("users or roles are still defined on database {db}; drop them (dropAllUsersFromDatabase, dropAllRolesFromDatabase) or erase the database first")));
    }
    Ok(())
}

pub async fn create(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Value> {
    check_name(db)?;
    let _op = ctl.ops.lock(db).await?;
    if exists(ctl, db).await? {
        return Err(ApiError::conflict("exists", format!("database {db} already exists")));
    }
    ensure_no_auth(ctl, db).await?;
    // A leftover record of an erased database must not be inherited by a new one with the same name.
    rec::delete(&ctl.store, &meta_path(db)).await?;
    {
        let mut g = ctl.dbs.lock(db).await;
        g.apply(vec![crate::dbs::Entry::collections_op(&[])?]).await?;
    }
    let m = default_meta(db, now_ms(), "admin");
    rec::put(&ctl.store, &meta_path(db), &m).await?;
    ctl.usage.touch(db);
    describe(ctl, db, true).await
}

/// The retention window and restore-point interval that apply to a database (its own setting, else the process
/// default; a window of 0 means no restore points).
pub fn effective_retention(ctl: &Ctl, m: &Value) -> Value {
    let own = &m["retention"];
    let window = own["window_secs"].as_u64().unwrap_or(ctl.args.retention_window_secs);
    let interval = own["interval_secs"].as_u64().unwrap_or(ctl.args.retention_interval_secs);
    json!({ "window_secs": window, "interval_secs": interval, "source": if own.is_null() { "default" } else { "database" } })
}

/// The tenant lease `(holder, expires_ms)`; its body is BSON (see `lease.rs`).
pub async fn read_lease(ctl: &Ctl, db: &str) -> anyhow::Result<Option<(String, i64)>> {
    match ctl.store.get(&crate::lease::lease_path(db)).await {
        Ok(g) => {
            let b = g.bytes().await?;
            let d = bson::Document::from_reader(&b[..])?;
            Ok(Some((d.get_str("holder").unwrap_or("").to_string(), d.get_i64("expiresMs").unwrap_or(0))))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}
