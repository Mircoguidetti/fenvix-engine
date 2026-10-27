//! Small JSON records on object storage (everything the control layer keeps lives under `control/`), written
//! with conditional puts so two processes cannot silently overwrite each other's update.

use anyhow::{anyhow, Result};
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde_json::Value;
use std::sync::Arc;

pub type Store = Arc<dyn ObjectStore>;

pub fn path(parts: &[&str]) -> Path {
    Path::from_iter(parts.iter().copied())
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64
}

pub async fn get(store: &Store, p: &Path) -> Result<Option<Value>> {
    match store.get(p).await {
        Ok(g) => Ok(Some(serde_json::from_slice(&g.bytes().await?)?)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub async fn put(store: &Store, p: &Path, v: &Value) -> Result<()> {
    store.put(p, PutPayload::from(Bytes::from(serde_json::to_vec(v)?))).await?;
    Ok(())
}

/// Create the record only if it does not exist. `Ok(false)`: it already existed.
pub async fn create(store: &Store, p: &Path, v: &Value) -> Result<bool> {
    let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
    match store.put_opts(p, PutPayload::from(Bytes::from(serde_json::to_vec(v)?)), opts).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::AlreadyExists { .. }) | Err(object_store::Error::Precondition { .. }) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

pub async fn delete(store: &Store, p: &Path) -> Result<()> {
    match store.delete(p).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Read-modify-write with a conditional put, retried on conflict. `f` maps the current value (None if absent) to
/// the new one (None: leave as is). Returns the value that is stored afterwards.
pub async fn update(store: &Store, p: &Path, mut f: impl FnMut(Option<Value>) -> Result<Option<Value>>) -> Result<Option<Value>> {
    for _ in 0..8 {
        let (cur, version) = match store.get(p).await {
            Ok(g) => {
                let (e_tag, version) = (g.meta.e_tag.clone(), g.meta.version.clone());
                (Some(serde_json::from_slice::<Value>(&g.bytes().await?)?), Some(UpdateVersion { e_tag, version }))
            }
            Err(object_store::Error::NotFound { .. }) => (None, None),
            Err(e) => return Err(e.into()),
        };
        let Some(new) = f(cur.clone())? else { return Ok(cur) };
        let mode = match version {
            Some(v) => PutMode::Update(v),
            None => PutMode::Create,
        };
        let opts = PutOptions { mode, ..Default::default() };
        match store.put_opts(p, PutPayload::from(Bytes::from(serde_json::to_vec(&new)?)), opts).await {
            Ok(_) => return Ok(Some(new)),
            Err(object_store::Error::Precondition { .. }) | Err(object_store::Error::AlreadyExists { .. }) | Err(object_store::Error::NotFound { .. }) => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Err(anyhow!("record {p} is contended"))
}

pub fn rfc3339(ms: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms).map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)).unwrap_or_default()
}

pub fn parse_time(s: &str) -> Option<i64> {
    if let Ok(ms) = s.parse::<i64>() {
        return Some(ms);
    }
    chrono::DateTime::parse_from_rfc3339(s).ok().map(|d| d.timestamp_millis())
}
