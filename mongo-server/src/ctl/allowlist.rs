//! Admin-port routes of the per-database allow lists and of the network limits' state.
//!
//! The list is stored in the database's metadata record (`control/dbs/{db}/meta.json`, field `ip_allowlist`): it survives
//! sleep, wake and restarts, an erase deletes it with the record, a fork starts with a new record (no list) and a restore
//! does not touch the record. There is no data-port command for any of this.

use super::rec;
use super::{meta, ApiError, ApiResult, Ctl};
use crate::netguard::{self, Net};
use serde_json::{json, Value};
use std::sync::Arc;

fn reply(ctl: &Ctl, db: &str, entries: &[Net]) -> Value {
    json!({
        "database": db,
        "entries": entries.iter().map(|n| n.to_string()).collect::<Vec<_>>(),
        "allow_all": entries.is_empty(),
        "max_entries": netguard::MAX_ENTRIES,
        // without --auth no client authenticates, so nothing checks the list
        "enforced": ctl.server.auth.enabled.load(std::sync::atomic::Ordering::Relaxed),
    })
}

async fn existing(ctl: &Ctl, db: &str) -> ApiResult<()> {
    meta::check_name(db)?;
    if !meta::exists(ctl, db).await? {
        return Err(ApiError::not_found(format!("database {db} does not exist")));
    }
    Ok(())
}

fn invalid(msg: impl Into<String>) -> ApiError {
    ApiError::new(400, "invalid_allowlist", msg)
}

pub async fn get(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Value> {
    existing(ctl, db).await?;
    let m = rec::get(&ctl.store, &meta::meta_path(db)).await?;
    let entries = match m.as_ref().and_then(netguard::stored_entries) {
        None => Vec::new(),
        Some(Ok(l)) => l,
        Some(Err(e)) => return Err(ApiError::new(500, "internal", format!("the stored list of {db} is not valid: {e}"))),
    };
    Ok(reply(ctl, db, &entries))
}

pub async fn set(ctl: &Arc<Ctl>, db: &str, body: &Value) -> ApiResult<Value> {
    meta::check_name(db)?;
    let Some(obj) = body.as_object() else { return Err(invalid("the body must be a JSON object {\"entries\": [...]}")) };
    if let Some(k) = obj.keys().find(|k| k.as_str() != "entries") {
        let shown: String = k.chars().take(40).collect();
        return Err(invalid(format!("unknown field {shown:?}; the body is {{\"entries\": [...]}}")));
    }
    let Some(v) = obj.get("entries") else { return Err(invalid("the body needs a field `entries` (an array; an empty array allows every address)")) };
    let entries = netguard::validate_entries(v).map_err(invalid)?;
    let _op = ctl.ops.lock(db).await?;
    existing(ctl, db).await?;
    let stored: Vec<String> = entries.iter().map(|n| n.to_string()).collect();
    meta::modify(ctl, db, |m| m["ip_allowlist"] = json!(stored)).await?;
    netguard::get().set_cached(db, Some(entries.clone()));
    let dropped = ctl.server.drop_auth_outside(db, &entries);
    let mut r = reply(ctl, db, &entries);
    r["connections_deauthenticated"] = json!(dropped);
    Ok(r)
}

pub async fn clear(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Value> {
    meta::check_name(db)?;
    let _op = ctl.ops.lock(db).await?;
    existing(ctl, db).await?;
    meta::modify(ctl, db, |m| {
        if let Some(o) = m.as_object_mut() {
            o.remove("ip_allowlist");
        }
    })
    .await?;
    netguard::get().set_cached(db, None);
    let mut r = reply(ctl, db, &[]);
    r["cleared"] = json!(true);
    Ok(r)
}

pub fn state() -> Value {
    netguard::get().state()
}

pub fn unblock(ip: &str) -> ApiResult<Value> {
    let addr: std::net::IpAddr = ip.parse().map_err(|_| ApiError::bad("not an IPv4 or IPv6 address"))?;
    let was = netguard::get().unblock(addr);
    Ok(json!({ "ip": addr.to_string(), "unblocked": was }))
}
