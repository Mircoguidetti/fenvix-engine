//! Intent records: `control/ops/{db}.json` says that a fork, restore or erase of `db` was started and not yet
//! finished. It is written before the first object is touched and deleted after the last, so a process that dies in
//! between leaves a record that [`recover`] acts on at the next start (forks roll back, restores and erases roll
//! forward; every step is written to be repeated).

use super::rec::{self, now_ms};
use super::{ApiError, ApiResult, Ctl};
use futures::StreamExt;
use object_store::path::Path;
use serde_json::{json, Value};
use std::sync::Arc;

pub fn path(db: &str) -> Path {
    rec::path(&["control", "ops", &format!("{db}.json")])
}

/// Record the intent. Fails with 409 if another operation's record is there (a crashed one is recovered at start;
/// until then the database is busy).
pub async fn begin(ctl: &Ctl, db: &str, kind: &str, mut detail: Value) -> ApiResult<()> {
    detail["kind"] = json!(kind);
    detail["db"] = json!(db);
    detail["started_at_ms"] = json!(now_ms());
    detail["phase"] = json!("started");
    if !rec::create(&ctl.store, &path(db), &detail).await? {
        let cur = rec::get(&ctl.store, &path(db)).await?.unwrap_or(Value::Null);
        return Err(ApiError::conflict("operation_in_progress", format!("database {db} has an unfinished {} (started {})", cur["kind"], cur["started_at_ms"])));
    }
    Ok(())
}

pub async fn get(ctl: &Ctl, db: &str) -> anyhow::Result<Option<Value>> {
    rec::get(&ctl.store, &path(db)).await
}

pub async fn phase(ctl: &Ctl, db: &str, phase: &str) -> anyhow::Result<()> {
    rec::update(&ctl.store, &path(db), |cur| {
        Ok(cur.map(|mut c| {
            c["phase"] = json!(phase);
            c
        }))
    })
    .await?;
    Ok(())
}

pub async fn finish(ctl: &Ctl, db: &str) -> anyhow::Result<()> {
    rec::delete(&ctl.store, &path(db)).await
}

/// `null`, or what is running on the database now.
pub async fn active_op(ctl: &Arc<Ctl>, db: &str) -> anyhow::Result<Value> {
    Ok(match get(ctl, db).await? {
        Some(o) => json!({ "kind": o["kind"], "phase": o["phase"], "started_at": rec::rfc3339(o["started_at_ms"].as_i64().unwrap_or(0)) }),
        None => Value::Null,
    })
}

/// Finish what a crash interrupted, before the first client is served.
pub async fn recover(ctl: &Arc<Ctl>) {
    let dir = rec::path(&["control", "ops"]);
    let metas: Vec<_> = ctl.store.list(Some(&dir)).collect::<Vec<_>>().await.into_iter().filter_map(|m| m.ok()).collect();
    for m in metas {
        let name = m.location.filename().unwrap_or("").trim_end_matches(".json").to_string();
        let Ok(Some(op)) = rec::get(&ctl.store, &m.location).await else { continue };
        let kind = op["kind"].as_str().unwrap_or("").to_string();
        eprintln!("ctl recovery: {kind} of {name} was interrupted; finishing it");
        let r: ApiResult<()> = match kind.as_str() {
            "erase" => super::erase::resume(ctl, &name, &op).await,
            "restore" => super::pitr::resume(ctl, &name, &op).await,
            "fork" => super::fork::rollback(ctl, &name, &op).await,
            other => Err(ApiError::new(500, "internal", format!("unknown intent kind {other}"))),
        };
        match r {
            Ok(()) => eprintln!("ctl recovery: {kind} of {name} finished"),
            Err(e) => eprintln!("ctl recovery: {kind} of {name} failed: {e}"),
        }
    }
}
