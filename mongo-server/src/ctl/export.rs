//! Export: a full export of one database in `mongodump` directory format, importable with `mongorestore --dir`,
//! streamed over HTTP and resumable.
//!
//! * `POST /v1/databases/{db}/exports` freezes the database: it is closed cleanly (everything flushed) and a named
//!   checkpoint (`export:{id}`, expiring after `--export-ttl-secs`) pins the state. The snapshot record
//!   `control/exports/{db}/{id}.json` lists the files of the dump.
//! * `GET /v1/exports/{id}/files/{db}/{collection}.bson` streams a collection as concatenated BSON documents (the format
//!   of `mongodump`'s `.bson` files) read from the checkpoint; `{collection}.metadata.json` holds the indexes. Both are
//!   deterministic, so a download that broke off continues with `Range: bytes=N-` (the server re-reads the first N bytes
//!   to find its place; no extra state is kept, so it also works after a server restart).
//! * When a pass over a collection reaches its end, the byte count, document count and SHA-256 are recorded in the
//!   snapshot record (`GET /v1/exports/{id}`), so a client can verify what it assembled.
//!
//! Writes after the snapshot do not show up in it; the snapshot costs the SSTs of the old state until it is deleted or
//! expires.

use super::http::{Body, Response};
use super::rec::{self, now_ms};
use super::{fork, meta, purge, ApiError, ApiResult, Ctl};
use bson::{doc, Bson, Document};
use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use slatedb::admin::Admin;
use slatedb::config::{CheckpointOptions, DbReaderOptions};
use slatedb::{DbReader, DbReaderMode};
use std::sync::Arc;
use std::time::Duration;
use tenant_bench::lifecycle::maint_lock;

pub fn record_path(db: &str, id: &str) -> Path {
    rec::path(&["control", "exports", db, &format!("{id}.json")])
}

fn split_id(id: &str) -> ApiResult<(&str, &str)> {
    id.split_once('.').filter(|(d, r)| !d.is_empty() && !r.is_empty()).ok_or_else(|| ApiError::not_found(format!("no export {id}")))
}

/// File name of a collection in a dump (mongodump escapes the characters that are not allowed in file names).
pub fn escape_name(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars() {
        match c {
            '/' | '\\' | '%' | '*' | '"' | '<' | '>' | '|' | '?' | ':' | '\0' => out.push_str(&format!("%{:02X}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

async fn open_snapshot(ctl: &Ctl, db: &str, checkpoint: &str) -> anyhow::Result<DbReader> {
    let id = uuid::Uuid::parse_str(checkpoint)?;
    let mut o = DbReaderOptions::default();
    o.skip_wal_replay = true; // the snapshot was taken after a clean close: nothing is left in the WAL
    o.manifest_poll_interval = Duration::from_secs(3600);
    Ok(DbReader::builder(purge::tenant_path(db), Arc::clone(&ctl.store)).with_reader_mode(DbReaderMode::Checkpoint(id)).with_options(o).build().await?)
}

fn index_specs(idx: &[crate::index::IndexDef]) -> Vec<Bson> {
    let mut v = vec![Bson::Document(doc! { "v": 2, "key": { "_id": 1 }, "name": "_id_" })];
    for i in idx.iter().filter(|i| i.state == crate::index::State::Ready) {
        v.push(Bson::Document(i.spec.clone()));
    }
    v
}

fn metadata_json(coll: &str, indexes: &[Bson]) -> Vec<u8> {
    let uuid = {
        let h = Sha256::digest(coll.as_bytes());
        h[..16].iter().map(|b| format!("{b:02x}")).collect::<String>()
    };
    // like mongodump: `options` appears only for a collection that has some (none are stored here)
    let d = doc! { "indexes": indexes.to_vec(), "uuid": uuid, "collectionName": coll, "type": "collection" };
    serde_json::to_vec(&Bson::Document(d).into_canonical_extjson()).unwrap()
}

pub async fn create(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Value> {
    meta::check_name(db)?;
    let _op = ctl.ops.lock(db).await?;
    if !meta::exists(ctl, db).await? {
        return Err(ApiError::not_found(format!("database {db} does not exist")));
    }
    let t0 = std::time::Instant::now();
    let before = ctl.usage.today_sum(&[db]);
    let id = format!("{db}.{:016x}", rand::random::<u64>());
    // Freeze: no command runs on the database and everything acknowledged is in the manifest.
    let mut g = ctl.dbs.lock(db).await;
    fork::quiesce(ctl, &mut g, db).await?;
    let lock = maint_lock(&ctl.lc.maint_locks, db);
    let _maint = lock.lock().await;
    ctl.dbs.leases.acquire(db).await?;
    let r = async {
        let admin = Admin::builder(purge::tenant_path(db), Arc::clone(&ctl.store)).build();
        let ttl = Duration::from_secs(ctl.args.export_ttl_secs.max(60));
        let cp = admin.create_detached_checkpoint(&CheckpointOptions { lifetime: Some(ttl), source: None, name: Some(format!("export:{id}")) }).await.map_err(anyhow::Error::from)?;
        super::failpoint("export.after_checkpoint").await;
        let reader = open_snapshot(ctl, db, &cp.id.to_string()).await?;
        let Some(raw) = reader.get(crate::layout::META_COLLECTIONS).await.map_err(anyhow::Error::from)? else { anyhow::bail!("database {db} has no collection list") };
        let names: Vec<String> = Document::from_reader(&raw[..])?.get_array("names")?.iter().filter_map(|b| b.as_str().map(str::to_string)).collect();
        let mut files = Vec::new();
        for n in &names {
            let esc = escape_name(n);
            let idx = match reader.get(&crate::layout::index_key(n)).await.map_err(anyhow::Error::from)? {
                Some(v) => {
                    let d = Document::from_reader(&v[..])?;
                    d.get_array("indexes")?.iter().filter_map(|b| b.as_document().and_then(crate::index::IndexDef::from_stored)).collect::<Vec<_>>()
                }
                None => vec![],
            };
            let meta_bytes = metadata_json(n, &index_specs(&idx));
            files.push(json!({ "collection": n, "kind": "bson", "path": format!("{db}/{esc}.bson"), "complete": null }));
            files.push(json!({ "collection": n, "kind": "metadata", "path": format!("{db}/{esc}.metadata.json"), "bytes": meta_bytes.len(), "sha256": hex(&Sha256::digest(&meta_bytes)) }));
        }
        reader.close().await.map_err(anyhow::Error::from)?;
        let record = json!({
            "id": id, "database": db, "checkpoint_id": cp.id.to_string(), "manifest_id": cp.manifest_id,
            "created_at_ms": now_ms(), "expires_at_ms": now_ms() + ttl.as_millis() as i64, "files": files,
        });
        rec::put(&ctl.store, &record_path(db, &id), &record).await?;
        Ok::<_, anyhow::Error>(record)
    }
    .await;
    ctl.dbs.leases.release(db).await;
    drop(g);
    let record = r?;
    ctl.usage.touch(db);
    let cost = ctl.usage.today_sum(&[db]).minus(&before);
    Ok(describe(&record, Some(json!({ "elapsed_ms": t0.elapsed().as_millis() as u64, "cost": cost.to_json() }))))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn describe(r: &Value, snapshot_cost: Option<Value>) -> Value {
    let mut v = json!({
        "id": r["id"], "database": r["database"], "created_at": rec::rfc3339(r["created_at_ms"].as_i64().unwrap_or(0)), "expires_at": rec::rfc3339(r["expires_at_ms"].as_i64().unwrap_or(0)),
        "manifest_id": r["manifest_id"], "files": r["files"],
        "restore_with": format!("mongorestore --dir <download dir> --nsInclude '{}.*'", r["database"].as_str().unwrap_or("")),
    });
    if let Some(c) = snapshot_cost {
        v["snapshot"] = c;
    }
    v
}

async fn load(ctl: &Ctl, id: &str) -> ApiResult<Value> {
    let (db, _) = split_id(id)?;
    let r = rec::get(&ctl.store, &record_path(db, id)).await?.ok_or_else(|| ApiError::not_found(format!("no export {id}")))?;
    if r["expires_at_ms"].as_i64().unwrap_or(0) < now_ms() {
        return Err(ApiError::new(410, "expired", format!("export {id} has expired")));
    }
    Ok(r)
}

pub async fn get(ctl: &Arc<Ctl>, id: &str) -> ApiResult<Value> {
    Ok(describe(&load(ctl, id).await?, None))
}

pub async fn delete(ctl: &Arc<Ctl>, id: &str) -> ApiResult<Value> {
    let (db, _) = split_id(id)?;
    let r = rec::get(&ctl.store, &record_path(db, id)).await?.ok_or_else(|| ApiError::not_found(format!("no export {id}")))?;
    release(ctl, db, &r).await?;
    Ok(json!({ "id": id, "deleted": true }))
}

/// Release the checkpoint of a snapshot and drop its record.
async fn release(ctl: &Arc<Ctl>, db: &str, r: &Value) -> anyhow::Result<()> {
    if let Some(cp) = r["checkpoint_id"].as_str().and_then(|s| uuid::Uuid::parse_str(s).ok()) {
        let admin = Admin::builder(purge::tenant_path(db), Arc::clone(&ctl.store)).build();
        if admin.read_manifest(None).await?.is_some() {
            admin.delete_checkpoint(cp).await?;
        }
    }
    rec::delete(&ctl.store, &record_path(db, r["id"].as_str().unwrap_or(""))).await
}

/// Parse `Range: bytes=N-`.
fn range_start(h: &std::collections::HashMap<String, String>) -> ApiResult<u64> {
    match h.get("range") {
        None => Ok(0),
        Some(v) => v.strip_prefix("bytes=").and_then(|r| r.strip_suffix('-')).and_then(|n| n.parse().ok()).ok_or_else(|| ApiError::new(416, "bad_range", "only `bytes=N-` is supported")),
    }
}

/// Stream one file of a snapshot.
pub async fn download(ctl: &Arc<Ctl>, id: &str, file: &str, headers: &std::collections::HashMap<String, String>) -> ApiResult<Response> {
    let rec_v = load(ctl, id).await?;
    let (db, _) = split_id(id)?;
    let entry = rec_v["files"].as_array().and_then(|f| f.iter().find(|x| x["path"] == json!(file))).cloned().ok_or_else(|| ApiError::not_found(format!("no file {file} in export {id}")))?;
    let start = range_start(headers)?;
    let coll = entry["collection"].as_str().unwrap_or("").to_string();
    if entry["kind"] == "metadata" {
        let reader = open_snapshot(ctl, db, rec_v["checkpoint_id"].as_str().unwrap_or("")).await?;
        let idx = match reader.get(&crate::layout::index_key(&coll)).await.map_err(anyhow::Error::from)? {
            Some(v) => Document::from_reader(&v[..]).map_err(anyhow::Error::from)?.get_array("indexes").map_err(anyhow::Error::from)?.iter().filter_map(|b| b.as_document().and_then(crate::index::IndexDef::from_stored)).collect::<Vec<_>>(),
            None => vec![],
        };
        reader.close().await.map_err(anyhow::Error::from)?;
        let body = metadata_json(&coll, &index_specs(&idx));
        let body = body.get(start as usize..).unwrap_or(&[]).to_vec();
        let mut r = Response { status: if start > 0 { 206 } else { 200 }, headers: vec![("Content-Type".into(), "application/json".into())], body: Body::Bytes(body) };
        r.headers.push(("Accept-Ranges".into(), "bytes".into()));
        return Ok(r);
    }
    let (tx, rx) = tokio::sync::mpsc::channel::<std::io::Result<Bytes>>(4);
    let (ctl2, id2, db2, cp) = (Arc::clone(ctl), id.to_string(), db.to_string(), rec_v["checkpoint_id"].as_str().unwrap_or("").to_string());
    // Open the snapshot before answering, so a missing or expired one is an error status and not a broken body.
    let reader = open_snapshot(ctl, db, &cp).await?;
    tokio::spawn(async move {
        if let Err(e) = stream_collection(&ctl2, reader, &db2, &id2, &coll, start, tx.clone()).await {
            eprintln!("export {id2} {coll}: {e:#}");
            let _ = tx.send(Err(std::io::Error::other(e.to_string()))).await;
        }
    });
    let mut headers = vec![("Content-Type".into(), "application/octet-stream".into()), ("Accept-Ranges".into(), "bytes".into()), ("X-Export-Id".into(), id.to_string())];
    let status = if start > 0 {
        headers.push(("Content-Range".into(), format!("bytes {start}-*/*")));
        206
    } else {
        200
    };
    Ok(Response { status, headers, body: Body::Stream(rx) })
}

async fn stream_collection(ctl: &Arc<Ctl>, reader: DbReader, db: &str, id: &str, coll: &str, start: u64, tx: tokio::sync::mpsc::Sender<std::io::Result<Bytes>>) -> anyhow::Result<()> {
    let opts = slatedb::config::ScanOptions { read_ahead_bytes: 4 << 20, max_fetch_tasks: 4, cache_blocks: false, ..Default::default() };
    let mut it = reader.scan_prefix_with_options(crate::layout::doc_prefix(coll), .., &opts).await?;
    let (mut offset, mut docs) = (0u64, 0u64);
    let mut hasher = Sha256::new();
    let mut buf: Vec<u8> = Vec::with_capacity(512 * 1024);
    while let Some(kv) = it.next().await? {
        let v = &kv.value[..];
        hasher.update(v);
        let end = offset + v.len() as u64;
        if end > start {
            let from = start.saturating_sub(offset) as usize;
            buf.extend_from_slice(&v[from..]);
        }
        offset = end;
        docs += 1;
        if buf.len() >= 256 * 1024 {
            if tx.send(Ok(Bytes::from(std::mem::take(&mut buf)))).await.is_err() {
                return Ok(()); // the client went away; it resumes with a Range request
            }
        }
        if docs % 5000 == 0 {
            super::failpoint("export.mid_stream").await;
        }
    }
    if !buf.is_empty() && tx.send(Ok(Bytes::from(buf))).await.is_err() {
        return Ok(());
    }
    reader.close().await?;
    // The whole collection has been read (from the start, even when only a tail was sent): remember what it is.
    let (dbn, idn) = (db.to_string(), id.to_string());
    let (bytes, sha, collection) = (offset, hex(&hasher.finalize()), coll.to_string());
    rec::update(&ctl.store, &record_path(&dbn, &idn), |cur| {
        Ok(cur.map(|mut r| {
            if let Some(files) = r["files"].as_array_mut() {
                for f in files.iter_mut() {
                    if f["collection"] == json!(collection) && f["kind"] == "bson" {
                        f["complete"] = json!({ "bytes": bytes, "docs": docs, "sha256": sha });
                    }
                }
            }
            r
        }))
    })
    .await?;
    Ok(())
}

/// Delete snapshots that have expired (their checkpoint would be collected anyway; this frees the SSTs sooner).
pub fn spawn_agent(ctl: &Arc<Ctl>) {
    let ctl = Arc::clone(ctl);
    tokio::spawn(async move {
        let mut every = tokio::time::interval(Duration::from_secs(300));
        loop {
            every.tick().await;
            let Some(_gate) = super::agent_gate().await else { return };
            let listed: Vec<Path> = ctl.store.list(Some(&rec::path(&["control", "exports"]))).filter_map(|m| async { m.ok().map(|m| m.location) }).collect().await;
            for p in listed {
                if let Ok(Some(r)) = rec::get(&ctl.store, &p).await {
                    if r["expires_at_ms"].as_i64().unwrap_or(i64::MAX) < now_ms() {
                        let db = r["database"].as_str().unwrap_or("").to_string();
                        if let Ok(_op) = ctl.ops.lock(&db).await {
                            if let Err(e) = release(&ctl, &db, &r).await {
                                eprintln!("export expiry of {}: {e:#}", r["id"]);
                            }
                        }
                    }
                }
            }
        }
    });
}
