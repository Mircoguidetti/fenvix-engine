//! Reads and writes: find, count, distinct, aggregate, insert, update, findAndModify, delete.
//! Documents are found through [`crate::coll::find_docs`] and written through [`crate::coll::Writes`].

use crate::agg::run_pipeline;
use crate::cmd::{bool_arg, collection_name, int_arg, write_error, Server, R};
use crate::coll::{find_docs, resolve_hint, FindOpts, Hint, Writes};
use crate::dbs::Entry;
use crate::txn::{attach, take_writes, tx_ov, Txn};
use crate::error::MongoError;
use crate::index::{self, IndexDef};
use crate::layout::{doc_prefix, encode_sortable};
use crate::query::{self, parse_projection, parse_sort, path_values, project, sort_docs};
use crate::update::{apply_update, upsert_document};
use bson::{doc, oid::ObjectId, Bson, Document};
use std::collections::VecDeque;

pub(crate) fn now_ms() -> i64 {
    bson::DateTime::now().timestamp_millis()
}

impl Server {
    /// Before a write to a collection with TTL indexes, remove its expired documents (at most once a second),
    /// so an expired document cannot hold an `_id` or a unique key.
    pub(crate) async fn presweep(&self, g: &mut Entry, coll: &str, indexes: &[IndexDef]) -> R<()> {
        if !indexes.iter().any(|i| i.ttl.is_some() && i.state == index::State::Ready) {
            return Ok(());
        }
        if g.swept.get(coll).map(|t| t.elapsed().as_millis() < 1000).unwrap_or(false) {
            return Ok(());
        }
        g.swept.insert(coll.to_string(), std::time::Instant::now());
        let n = crate::ttl::sweep_coll(g, coll, now_ms(), 1).await?;
        self.dbs.ttl.docs_expired.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Matching documents of `coll`: (docs, indexes) with TTL-expired documents hidden.
    pub(crate) async fn matching(&self, g: &mut Entry, db: &str, coll: &str, filter: &Document, hint: Option<&Bson>, ov: Option<&crate::coll::Overlay>) -> R<Vec<Document>> {
        let indexes = self.load_indexes(g, db, coll).await?;
        let hint = resolve_hint(&indexes, hint)?;
        let specs = index::ttl_specs(&indexes);
        let (docs, _, _) = find_docs(g, coll, &indexes, &FindOpts { filter, sort: &[], want: None, hint, ttl: ttl_ctx(&specs), ov }).await?;
        Ok(docs)
    }

    /// Every visible document of a collection (TTL-expired ones hidden).
    pub(crate) async fn scan_visible(&self, g: &mut Entry, db: &str, coll: &str) -> R<Vec<Document>> {
        self.matching(g, db, coll, &Document::new(), None, None).await
    }

    pub(crate) async fn find(&self, db: &str, cmd: &Document, tx: &mut Option<Txn>) -> R<Document> {
        let coll = collection_name(cmd, "find")?;
        let filter = cmd.get_document("filter").cloned().unwrap_or_default();
        let sort = match cmd.get_document("sort") {
            Ok(s) => parse_sort(s)?,
            Err(_) => vec![],
        };
        let projection = match cmd.get_document("projection") {
            Ok(p) => parse_projection(p)?,
            Err(_) => None,
        };
        let skip = int_arg(cmd.get("skip")).unwrap_or(0).max(0) as usize;
        let limit = int_arg(cmd.get("limit")).unwrap_or(0);
        let batch = int_arg(cmd.get("batchSize")).map(|b| b.max(0) as usize);
        let single = bool_arg(cmd.get("singleBatch"), false);
        let mut docs = {
            let mut g = self.dbs.lock(db).await;
        attach(&mut g, tx);
            let indexes = self.load_indexes(&mut g, db, coll).await?;
            let hint = resolve_hint(&indexes, cmd.get("hint"))?;
            let specs = index::ttl_specs(&indexes);
            let want = if limit > 0 { Some(skip + limit as usize) } else { None };
            let (docs, _, _) = find_docs(&mut g, coll, &indexes, &FindOpts { filter: &filter, sort: &sort, want, hint, ttl: ttl_ctx(&specs), ov: tx_ov(tx) }).await?;
            docs
        };
        sort_docs(&mut docs, &sort);
        let mut docs: VecDeque<Document> = docs.into_iter().skip(skip).collect();
        if limit > 0 {
            docs.truncate(limit as usize);
        }
        if let Some(p) = &projection {
            for d in docs.iter_mut() {
                *d = project(d, p);
            }
        }
        Ok(self.cursor_reply(format!("{db}.{coll}"), docs, batch, single))
    }

    pub(crate) async fn count(&self, db: &str, cmd: &Document, tx: &mut Option<Txn>) -> R<Document> {
        let coll = collection_name(cmd, "count")?;
        let filter = cmd.get_document("query").cloned().unwrap_or_default();
        let skip = int_arg(cmd.get("skip")).unwrap_or(0).max(0) as usize;
        let limit = int_arg(cmd.get("limit")).unwrap_or(0).unsigned_abs() as usize;
        let n = {
            let mut g = self.dbs.lock(db).await;
        attach(&mut g, tx);
            self.matching(&mut g, db, coll, &filter, cmd.get("hint"), tx_ov(tx)).await?.len()
        };
        let mut n = n.saturating_sub(skip);
        if limit > 0 {
            n = n.min(limit);
        }
        Ok(doc! { "n": n as i32, "ok": 1.0 })
    }

    pub(crate) async fn distinct(&self, db: &str, cmd: &Document, tx: &mut Option<Txn>) -> R<Document> {
        let coll = collection_name(cmd, "distinct")?;
        let key = cmd.get_str("key").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'distinct.key' is missing but a required field".into()))?;
        let filter = cmd.get_document("query").cloned().unwrap_or_default();
        let docs = {
            let mut g = self.dbs.lock(db).await;
        attach(&mut g, tx);
            self.matching(&mut g, db, coll, &filter, cmd.get("hint"), tx_ov(tx)).await?
        };
        let mut values: Vec<Bson> = Vec::new();
        for d in &docs {
            for v in path_values(d, key) {
                let items: Vec<&Bson> = match v {
                    Bson::Array(a) => a.iter().collect(),
                    other => vec![other],
                };
                for it in items {
                    if !values.iter().any(|x| query::equal(x, it)) {
                        values.push(it.clone());
                    }
                }
            }
        }
        values.sort_by(query::compare);
        Ok(doc! { "values": values, "ok": 1.0 })
    }

    pub(crate) async fn aggregate(&self, db: &str, cmd: &Document, tx: &mut Option<Txn>) -> R<Document> {
        let coll = collection_name(cmd, "aggregate")?;
        let pipeline = cmd.get_array("pipeline").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'aggregate.pipeline' is missing but a required field".into()))?;
        let cursor = cmd.get_document("cursor").map_err(|_| MongoError::failed_to_parse("The 'cursor' option is required, except for aggregate with the explain argument".into()))?;
        let batch = int_arg(cursor.get("batchSize")).map(|b| b.max(0) as usize);
        let first_match = pipeline.first().and_then(|s| s.as_document()).and_then(|s| s.get_document("$match").ok()).cloned().unwrap_or_default();
        let docs = {
            let mut g = self.dbs.lock(db).await;
        attach(&mut g, tx);
            self.matching(&mut g, db, coll, &first_match, cmd.get("hint"), tx_ov(tx)).await?
        };
        let mut foreign = crate::agg::Foreign::new();
        for from in crate::agg::lookup_collections(pipeline) {
            let fd = {
                let mut g = self.dbs.lock(db).await;
        attach(&mut g, tx);
                self.scan_visible(&mut g, db, &from).await?
            };
            foreign.insert(from, fd);
        }
        let out = run_pipeline(docs, pipeline, &foreign)?;
        Ok(self.cursor_reply(format!("{db}.{coll}"), out.into(), batch, false))
    }

    // ---- writes ----------------------------------------------------------------------------

    /// Apply a write set (plus catalog changes) as one batch, or, inside a transaction, keep it in the transaction.
    pub(crate) async fn commit(&self, g: &mut Entry, coll: &str, mut w: Writes, indexes: &[IndexDef], new_coll: Option<Vec<String>>, tx: &mut Option<Txn>) -> R<()> {
        if w.indexes_dirty {
            w.meta.insert(coll.to_string(), indexes.to_vec());
            w.indexes_dirty = false;
        }
        if new_coll.is_some() {
            w.names = new_coll;
        }
        if let Some(t) = tx {
            t.w = w;
            return Ok(());
        }
        let ops = w.final_ops()?;
        g.apply(ops).await?;
        Ok(())
    }

    pub(crate) async fn insert(&self, db: &str, cmd: &Document, tx: &mut Option<Txn>) -> R<Document> {
        let coll = collection_name(cmd, "insert")?;
        let docs = cmd.get_array("documents").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'insert.documents' is missing but a required field".into()))?;
        let ordered = bool_arg(cmd.get("ordered"), true) || tx.is_some();
        let ns = format!("{db}.{coll}");
        let mut g = self.dbs.lock(db).await;
        attach(&mut g, tx);
        let names = g.collections().await?;
        let mut indexes = self.load_indexes(&mut g, db, coll).await?;
        self.presweep(&mut g, coll, &indexes).await?;
        let mut w = take_writes(tx);
        let names = w.names.clone().unwrap_or(names);
        let mut errors: Vec<Bson> = Vec::new();
        let mut n = 0;
        for (i, raw) in docs.iter().enumerate() {
            let Some(d) = raw.as_document() else {
                errors.push(Bson::Document(write_error(i, 14, "document to insert must be an object".into())));
                if ordered { break } else { continue }
            };
            let mut d = d.clone();
            if d.get("_id").is_none() {
                let mut with_id = doc! { "_id": ObjectId::new() };
                for (k, v) in d {
                    with_id.insert(k, v);
                }
                d = with_id;
            } else if d.keys().next().map(String::as_str) != Some("_id") {
                // MongoDB always stores `_id` as the first field.
                let id = d.remove("_id").unwrap();
                let mut with_id = doc! { "_id": id };
                for (k, v) in d {
                    with_id.insert(k, v);
                }
                d = with_id;
            }
            let id = d.get("_id").unwrap().clone();
            if matches!(id, Bson::Array(_) | Bson::RegularExpression(_) | Bson::Undefined) {
                errors.push(Bson::Document(write_error(i, 53, format!("The '_id' value cannot be of type {}", crate::update::type_name(&id)))));
                if ordered { break } else { continue }
            }
            // An expired-but-not-yet-removed document still holds its _id and its unique keys.
            if w.exists(&mut g, coll, &id).await? {
                errors.push(Bson::Document(MongoError::duplicate_key(&ns, "_id_", &doc! { "_id": 1 }, &doc! { "_id": id }).to_write_error(i)));
                if ordered { break } else { continue }
            }
            if let Err(e) = w.put(&mut g, coll, &ns, &mut indexes, &d, None).await {
                if e.code == 112 {
                    return Err(e);
                }
                errors.push(Bson::Document(e.to_write_error(i)));
                if ordered { break } else { continue }
            }
            n += 1;
        }
        let new_coll = (!names.iter().any(|x| x == coll) && n > 0).then(|| {
            let mut v = names.clone();
            v.push(coll.to_string());
            v
        });
        self.commit(&mut g, coll, w, &indexes, new_coll, tx).await?;
        let mut reply = doc! { "n": n as i32 };
        if !errors.is_empty() {
            reply.insert("writeErrors", errors);
        }
        reply.insert("ok", 1.0);
        Ok(reply)
    }

    pub(crate) async fn update(&self, db: &str, cmd: &Document, tx: &mut Option<Txn>) -> R<Document> {
        let coll = collection_name(cmd, "update")?;
        let specs = cmd.get_array("updates").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'update.updates' is missing but a required field".into()))?;
        let ordered = bool_arg(cmd.get("ordered"), true) || tx.is_some();
        let ns = format!("{db}.{coll}");
        let mut g = self.dbs.lock(db).await;
        attach(&mut g, tx);
        let names = g.collections().await?;
        let mut indexes = self.load_indexes(&mut g, db, coll).await?;
        self.presweep(&mut g, coll, &indexes).await?;
        let ttl_specs = index::ttl_specs(&indexes);
        let mut w = take_writes(tx);
        let names = w.names.clone().unwrap_or(names);
        let mut acc = UpdAcc::default();
        let mut errors: Vec<Bson> = Vec::new();

        'specs: for (i, spec) in specs.iter().enumerate() {
            let step: R<()> = update_stmt(&mut g, coll, &ns, &mut indexes, &ttl_specs, &mut w, spec, i, &mut acc).await;
            if let Err(e) = step {
                if e.code == 112 {
                    return Err(e);
                }
                errors.push(Bson::Document(e.to_write_error(i)));
                if ordered {
                    break 'specs;
                }
            }
        }
        let new_coll = (!acc.upserted.is_empty() && !names.iter().any(|x| x == coll)).then(|| {
            let mut v = names.clone();
            v.push(coll.to_string());
            v
        });
        self.commit(&mut g, coll, w, &indexes, new_coll, tx).await?;
        let mut reply = doc! { "n": acc.n, "nModified": acc.n_modified };
        if !acc.upserted.is_empty() {
            reply.insert("upserted", acc.upserted);
        }
        if !errors.is_empty() {
            reply.insert("writeErrors", errors);
        }
        reply.insert("ok", 1.0);
        Ok(reply)
    }

    pub(crate) async fn find_and_modify(&self, db: &str, cmd: &Document, tx: &mut Option<Txn>) -> R<Document> {
        let key = cmd.keys().next().cloned().unwrap_or_default();
        let coll = collection_name(cmd, &key)?;
        let ns = format!("{db}.{coll}");
        let q = match cmd.get("query") {
            None | Some(Bson::Null) => Document::new(),
            Some(Bson::Document(d)) => d.clone(),
            Some(_) => return Err(MongoError::type_mismatch("BSON field 'findAndModify.query' is the wrong type, expected object".into())),
        };
        let sort = match cmd.get("sort") {
            Some(Bson::Document(s)) => parse_sort(s)?,
            _ => vec![],
        };
        let remove = bool_arg(cmd.get("remove"), false);
        let want_new = bool_arg(cmd.get("new"), false);
        let upsert = bool_arg(cmd.get("upsert"), false);
        let projection = match cmd.get("fields") {
            Some(Bson::Document(p)) => parse_projection(p)?,
            _ => None,
        };
        if let Some(Bson::Array(a)) = cmd.get("arrayFilters") {
            if !a.is_empty() {
                return Err(MongoError::new(238, "NotImplemented", "arrayFilters are not supported by this server".into()));
            }
        }
        let u = match cmd.get("update") {
            None => None,
            Some(Bson::Document(u)) => Some(u),
            Some(Bson::Array(_)) => return Err(MongoError::new(238, "NotImplemented", "pipeline-style updates are not supported by this server".into())),
            Some(_) => return Err(MongoError::new(14, "TypeMismatch", "BSON field 'findAndModify.update' is the wrong type, expected object or array".into())),
        };
        if remove && u.is_some() {
            return Err(MongoError::failed_to_parse("Cannot specify both an update and remove=true".into()));
        }
        if remove && upsert {
            return Err(MongoError::failed_to_parse("Cannot specify both upsert=true and remove=true".into()));
        }
        if remove && want_new {
            return Err(MongoError::failed_to_parse("Cannot specify both new=true and remove=true; 'remove' always returns the deleted document".into()));
        }
        if !remove && u.is_none() {
            return Err(MongoError::failed_to_parse("Either an update or remove=true must be specified".into()));
        }
        let shape = |d: Option<Document>| -> Bson {
            match d {
                None => Bson::Null,
                Some(d) => Bson::Document(match &projection {
                    Some(p) => project(&d, p),
                    None => d,
                }),
            }
        };
        let mut g = self.dbs.lock(db).await;
        attach(&mut g, tx);
        let names = g.collections().await?;
        let mut indexes = self.load_indexes(&mut g, db, coll).await?;
        self.presweep(&mut g, coll, &indexes).await?;
        let ttl_specs = index::ttl_specs(&indexes);
        let hint = resolve_hint(&indexes, cmd.get("hint"))?;
        let mut w = take_writes(tx);
        let names = w.names.clone().unwrap_or(names);
        let (mut matched, _, _) = find_docs(&mut g, coll, &indexes, &FindOpts { filter: &q, sort: &sort, want: Some(1), hint, ttl: ttl_ctx(&ttl_specs), ov: Some(&w.ov) }).await?;
        sort_docs(&mut matched, &sort);
        let hit = matched.into_iter().next();
        if remove {
            return Ok(match hit {
                None => doc! { "lastErrorObject": { "n": 0 }, "value": Bson::Null, "ok": 1.0 },
                Some(old) => {
                    w.delete(&mut g, coll, &indexes, &old)?;
                    self.commit(&mut g, coll, w, &indexes, None, tx).await?;
                    doc! { "lastErrorObject": { "n": 1 }, "value": shape(Some(old)), "ok": 1.0 }
                }
            });
        }
        let u = u.unwrap();
        match hit {
            Some(old) => {
                let new = apply_update(&old, u)?;
                if new.to_vec()? != old.to_vec()? {
                    w.put(&mut g, coll, &ns, &mut indexes, &new, Some(&old)).await?;
                    self.commit(&mut g, coll, w, &indexes, None, tx).await?;
                }
                let value = shape(Some(if want_new { new } else { old }));
                Ok(doc! { "lastErrorObject": { "n": 1, "updatedExisting": true }, "value": value, "ok": 1.0 })
            }
            None if upsert => {
                let nd = upsert_document(&q, u)?;
                let id = nd.get("_id").cloned().unwrap();
                if w.exists(&mut g, coll, &id).await? {
                    return Err(MongoError::duplicate_key(&ns, "_id_", &doc! { "_id": 1 }, &doc! { "_id": nd.get("_id").cloned().unwrap_or(Bson::Null) }));
                }
                w.put(&mut g, coll, &ns, &mut indexes, &nd, None).await?;
                let new_coll = (!names.iter().any(|x| x == coll)).then(|| {
                    let mut v = names.clone();
                    v.push(coll.to_string());
                    v
                });
                self.commit(&mut g, coll, w, &indexes, new_coll, tx).await?;
                let value = if want_new { shape(Some(nd)) } else { Bson::Null };
                Ok(doc! { "lastErrorObject": { "n": 1, "updatedExisting": false, "upserted": id }, "value": value, "ok": 1.0 })
            }
            None => Ok(doc! { "lastErrorObject": { "n": 0, "updatedExisting": false }, "value": Bson::Null, "ok": 1.0 }),
        }
    }

    pub(crate) async fn delete(&self, db: &str, cmd: &Document, tx: &mut Option<Txn>) -> R<Document> {
        let coll = collection_name(cmd, "delete")?;
        let specs = cmd.get_array("deletes").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'delete.deletes' is missing but a required field".into()))?;
        let mut g = self.dbs.lock(db).await;
        attach(&mut g, tx);
        let indexes = self.load_indexes(&mut g, db, coll).await?;
        self.presweep(&mut g, coll, &indexes).await?;
        let ttl_specs = index::ttl_specs(&indexes);
        let mut w = take_writes(tx);
        let mut n = 0;
        let mut errors: Vec<Bson> = Vec::new();
        let ordered = bool_arg(cmd.get("ordered"), true) || tx.is_some();
        for (i, spec) in specs.iter().enumerate() {
            let res: R<()> = delete_stmt(&mut g, coll, &indexes, &ttl_specs, &mut w, spec, &mut n).await;
            if let Err(e) = res {
                if e.code == 112 {
                    return Err(e);
                }
                errors.push(Bson::Document(write_error(i, e.code, e.msg)));
                if ordered {
                    break;
                }
            }
        }
        self.commit(&mut g, coll, w, &indexes, None, tx).await?;
        let mut reply = doc! { "n": n };
        if !errors.is_empty() {
            reply.insert("writeErrors", errors);
        }
        reply.insert("ok", 1.0);
        Ok(reply)
    }
}


#[derive(Default)]
struct UpdAcc {
    n: i32,
    n_modified: i32,
    upserted: Vec<Bson>,
}

#[allow(clippy::too_many_arguments)]
async fn update_stmt(g: &mut Entry, coll: &str, ns: &str, indexes: &mut Vec<IndexDef>, ttl_specs: &[index::TtlSpec], w: &mut Writes, spec: &Bson, i: usize, acc: &mut UpdAcc) -> R<()> {
                let spec = spec.as_document().ok_or_else(|| MongoError::new(14, "TypeMismatch", "update statement must be an object".into()))?;
                let q = spec.get_document("q").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'updates.q' is missing but a required field".into()))?;
                let u = match spec.get("u") {
                    Some(Bson::Document(u)) => u,
                    Some(Bson::Array(_)) => return Err(MongoError::new(238, "NotImplemented", "pipeline-style updates are not supported by this server".into())),
                    _ => return Err(MongoError::new(40414, "Location40414", "BSON field 'updates.u' is missing but a required field".into())),
                };
                if let Some(Bson::Array(a)) = spec.get("arrayFilters") {
                    if !a.is_empty() {
                        return Err(MongoError::new(238, "NotImplemented", "arrayFilters are not supported by this server".into()));
                    }
                }
                let multi = bool_arg(spec.get("multi"), false);
                let upsert = bool_arg(spec.get("upsert"), false);
                let hint = resolve_hint(indexes, spec.get("hint"))?;
                let (mut matched, _, _) = find_docs(g, coll, indexes, &FindOpts { filter: q, sort: &[], want: if multi { None } else { Some(1) }, hint, ttl: ttl_ctx(ttl_specs), ov: Some(&w.ov) }).await?;
                if !multi {
                    matched.truncate(1);
                }
                if matched.is_empty() {
                    if !upsert {
                        return Ok(());
                    }
                    let nd = upsert_document(q, u)?;
                    let id = nd.get("_id").cloned().unwrap();
                    if w.exists(g, coll, &id).await? {
                        return Err(MongoError::duplicate_key(ns, "_id_", &doc! { "_id": 1 }, &doc! { "_id": id }));
                    }
                    w.put(g, coll, ns, indexes, &nd, None).await?;
                    acc.upserted.push(Bson::Document(doc! { "index": i as i32, "_id": id }));
                    acc.n += 1;
                    return Ok(());
                }
                for old in matched {
                    let new = apply_update(&old, u)?;
                    acc.n += 1;
                    if new.to_vec()? == old.to_vec()? {
                        continue;
                    }
                    w.put(g, coll, ns, indexes, &new, Some(&old)).await?;
                    acc.n_modified += 1;
                }
                Ok(())
}


async fn delete_stmt(g: &mut Entry, coll: &str, indexes: &[IndexDef], ttl_specs: &[index::TtlSpec], w: &mut Writes, spec: &Bson, n: &mut i32) -> R<()> {
                let spec = spec.as_document().ok_or_else(|| MongoError::new(14, "TypeMismatch", "delete statement must be an object".into()))?;
                let q = spec.get_document("q").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'deletes.q' is missing but a required field".into()))?;
                let limit = int_arg(spec.get("limit")).unwrap_or(0);
                let hint = resolve_hint(indexes, spec.get("hint"))?;
                let (mut found, _, _) = find_docs(g, coll, indexes, &FindOpts { filter: q, sort: &[], want: if limit == 0 { None } else { Some(limit as usize) }, hint, ttl: ttl_ctx(ttl_specs), ov: Some(&w.ov) }).await?;
                if limit > 0 {
                    found.truncate(limit as usize);
                }
                for d in found {
                    w.delete(g, coll, indexes, &d)?;
                    *n += 1;
                }
                Ok(())
}

/// TTL filter argument for [`FindOpts`]: `None` when the collection has no TTL index.
pub(crate) fn ttl_ctx(specs: &[index::TtlSpec]) -> Option<(&[index::TtlSpec], i64)> {
    if specs.is_empty() {
        None
    } else {
        Some((specs, now_ms()))
    }
}

#[allow(dead_code)]
fn _unused(_: &Hint) {
    let _ = (doc_prefix, encode_sortable);
}
