//! Index and collection management: createIndexes (restartable backfill), dropIndexes, listIndexes,
//! drop, explain, validate, collStats / dbStats.

use crate::cmd::{bool_arg, collection_name, fake_uuid, int_arg, ok, Server, R};
use crate::coll::{decode, find_docs, plan_for, resolve_hint, FindOpts, Hint};
use crate::cmd_data::ttl_ctx;
use crate::dbs::{valid_db_name, Dbs, Entry};
use crate::error::MongoError;
use crate::index::{self, entry_id, entry_key, IndexDef, Plan, State};
use crate::keyenc::prefix_end;
use crate::layout::{doc_prefix, encode_sortable, idx_collection_prefix, index_key};
use crate::query::{parse_sort, path_values};
use crate::update::fmt_doc;
use bson::{doc, Bson, Document};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tenant_bench::lifecycle::Op;

/// Documents indexed (or index entries deleted) per backfill step.
pub const BUILD_CHUNK: usize = 2000;

pub enum Step {
    Done,
    More,
    Failed(MongoError),
}

/// Number of index entries to delete or documents to index in one batch; tests lower it.
fn chunk() -> usize {
    std::env::var("MONGO_BUILD_CHUNK").ok().and_then(|s| s.parse().ok()).unwrap_or(BUILD_CHUNK)
}

/// One backfill / drop step of the first unfinished index of `coll`, as one atomic batch.
pub async fn build_step(dbs: &Arc<Dbs>, db: &str, coll: &str) -> R<Step> {
    let mut g = dbs.lock(db).await;
    let mut indexes = g.indexes(coll).await?;
    let Some(pos) = indexes.iter().position(|i| i.state != State::Ready) else { return Ok(Step::Done) };
    let n = chunk();
    let prefix = indexes[pos].prefix(coll);
    if indexes[pos].state == State::Dropping {
        let end = prefix_end(&prefix);
        let rows = g.scan_range(&prefix, end.as_deref(), false, Some(n), 1 << 20).await?;
        let mut ops: Vec<Op> = rows.iter().map(|(k, _)| Op::Delete(k.to_vec())).collect();
        let last = rows.len() < n;
        if last {
            indexes.remove(pos);
            ops.push(Entry::indexes_op(coll, &indexes)?);
        }
        g.apply(ops).await?;
        return Ok(if last && !indexes.iter().any(|i| i.state != State::Ready) { Step::Done } else { Step::More });
    }
    // Building.
    let dp = doc_prefix(coll);
    let dend = prefix_end(&dp);
    let start = indexes[pos].cursor.clone().unwrap_or_else(|| dp.clone());
    let rows = g.scan_range(&start, dend.as_deref(), false, Some(n), 4 << 20).await?;
    let idx = indexes[pos].clone();
    let mut ops: Vec<Op> = Vec::new();
    let mut seen: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let ns = format!("{db}.{coll}");
    let mut failure: Option<MongoError> = None;
    let mut multikey = false;
    'docs: for (k, v) in &rows {
        let d = decode(v)?;
        let id_enc = k[dp.len()..].to_vec();
        let (entries, mk) = match idx.entries(&d) {
            Ok(x) => x,
            Err(e) => {
                failure = Some(e);
                break;
            }
        };
        multikey |= mk;
        for (c, kv) in entries {
            if idx.unique {
                let dup = match seen.get(&c) {
                    Some(o) => *o != id_enc,
                    None => {
                        let mut p = prefix.clone();
                        p.extend(&c);
                        let end = prefix_end(&p);
                        g.scan_range(&p, end.as_deref(), false, Some(4), 4096).await?.iter().any(|(ek, _)| entry_id(ek).map(|o| o != id_enc.as_slice()).unwrap_or(false))
                    }
                };
                if dup {
                    let mut e = MongoError::duplicate_key(&ns, &idx.name, &idx.key_pattern(), &kv);
                    e.msg = format!("Index build failed: {}: Collection {ns} ( {} ) :: caused by :: {}", fake_uuid(), fake_uuid(), e.msg);
                    failure = Some(e);
                    break 'docs;
                }
                seen.insert(c.clone(), id_enc.clone());
            }
            ops.push(Op::Put(entry_key(&prefix, &c, &id_enc), crate::natural::entry_value(crate::natural::seq_of(v))));
        }
    }
    if let Some(e) = failure {
        // Abort the build: a multi-index build is all or nothing (as in mongod), so every index still building becomes a
        // Dropping one and its entries are removed by the next steps.
        for i in indexes.iter_mut().filter(|i| i.state == State::Building) {
            i.state = State::Dropping;
            i.cursor = None;
        }
        g.apply(vec![Entry::indexes_op(coll, &indexes)?]).await?;
        return Ok(Step::Failed(e));
    }
    let done = rows.len() < n;
    if multikey {
        indexes[pos].multikey = true;
    }
    if done {
        indexes[pos].state = State::Ready;
        indexes[pos].cursor = None;
    } else {
        let mut c = rows.last().unwrap().0.to_vec();
        c.push(0);
        indexes[pos].cursor = Some(c);
    }
    ops.push(Entry::indexes_op(coll, &indexes)?);
    g.apply(ops).await?;
    Ok(if done && !indexes.iter().any(|i| i.state != State::Ready) { Step::Done } else { Step::More })
}

/// Run steps until nothing is left to build or drop on `coll`. Returns the error of a failed build.
pub async fn run_builds(dbs: &Arc<Dbs>, db: &str, coll: &str) -> R<()> {
    let mut first_err = None;
    loop {
        match build_step(dbs, db, coll).await? {
            Step::More => {}
            Step::Failed(e) => first_err = first_err.or(Some(e)),
            Step::Done => break,
        }
        tokio::task::yield_now().await;
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

fn spec_name(key: &Document) -> String {
    key.iter().map(|(k, v)| format!("{k}_{}", match v { Bson::String(s) => s.clone(), other => other.to_string() })).collect::<Vec<_>>().join("_")
}

fn num_opt(v: &Bson) -> Option<f64> {
    match v {
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Double(f) => Some(*f),
        _ => None,
    }
}

const RENAME_MAX_OPS: usize = 1_000_000;

fn bad_spec(spec_name: &str, key: &Document, msg: &str) -> MongoError {
    MongoError::new(67, "CannotCreateIndex", format!("Error in specification {{ name: \"{spec_name}\", key: {} }} :: caused by :: {msg}", fmt_doc(key)))
}

/// The stored spec of a 2dsphere / 2d index, or `None` when `key` has no geo field.
fn normalize_geo(spec: &Document, key: &Document, name: &str) -> R<Option<Document>> {
    let is = |v: &Bson, t: &str| matches!(v, Bson::String(s) if s == t);
    let n_sphere = key.values().filter(|v| is(v, "2dsphere")).count();
    let n_plane = key.values().filter(|v| is(v, "2d")).count();
    if n_sphere + n_plane == 0 {
        return Ok(None);
    }
    if key.values().any(|v| matches!(v, Bson::String(s) if s != "2d" && s != "2dsphere")) || (n_sphere > 0 && n_plane > 0) {
        return Err(bad_spec(name, key, "Can't use more than one index plugin for a single index."));
    }
    let mut idx = doc! { "v": 2, "key": key.clone(), "name": name };
    if n_plane > 0 {
        if n_plane > 1 {
            return Err(MongoError::new(16800, "Location16800", "can't have 2 geo fields".into()));
        }
        if !key.values().next().map(|v| is(v, "2d")).unwrap_or(false) {
            return Err(MongoError::new(16801, "Location16801", "2d has to be first in index".into()));
        }
        let bits = match spec.get("bits") {
            None => None,
            Some(v) => {
                let n = crate::index::num_of(v).ok_or_else(|| MongoError::new(72, "InvalidOptions", "bits must be a number".into()))?;
                if !(1.0..=32.0).contains(&n) {
                    return Err(MongoError::new(72, "InvalidOptions", format!("bits for hash must be > 0 and <= 32, but {} bits were specified", n as i64)));
                }
                Some(v.clone())
            }
        };
        let num = |f: &str| spec.get(f).and_then(crate::index::num_of);
        let (min, max) = (num("min"), num("max"));
        if min.is_some() || max.is_some() {
            let (lo, hi) = (min.unwrap_or(-180.0), max.unwrap_or(180.0));
            if !(lo < hi) {
                return Err(MongoError::new(72, "InvalidOptions", format!("region for hash must be valid and have positive area, but [{}, {}] was specified", crate::geo::fmt_num(lo), crate::geo::fmt_num(hi))));
            }
        }
        // geo settings come after the generic options, which the caller appends before calling `finish_geo_spec`
        let mut extra = Document::new();
        if let Some(b) = bits {
            extra.insert("bits", b);
        }
        if let Some(lo) = min {
            extra.insert("min", lo);
        }
        if let Some(hi) = max {
            extra.insert("max", hi);
        }
        idx.insert("geoSettings", extra);
    } else {
        let version = match spec.get("2dsphereIndexVersion") {
            None => 3,
            Some(v) => match crate::index::num_of(v) {
                Some(n) if (1.0..=3.0).contains(&n) => n as i32,
                _ => return Err(bad_spec(name, key, &format!("unsupported geo index version {{ 2dsphereIndexVersion : {} }}", crate::update::fmt_bson(v)))),
            },
        };
        let mut extra = doc! { "2dsphereIndexVersion": version };
        for f in ["coarsestIndexedLevel", "finestIndexedLevel"] {
            if let Some(v) = spec.get(f) {
                extra.insert(f, v.clone());
            }
        }
        idx.insert("geoSettings", extra);
    }
    Ok(Some(idx))
}

/// The stored spec of a text index, or `None` when `key` has no `"text"` field. `listIndexes` shows the key as
/// `{..prefix, _fts: "text", _ftsx: 1, ..suffix}` with `weights`, `default_language`, `language_override` and
/// `textIndexVersion`; the user's key pattern is kept as the internal `textKey`.
fn normalize_text(spec: &Document, key: &Document, name: &str) -> R<Option<Document>> {
    let is_text = |v: &Bson| matches!(v, Bson::String(s) if s == "text");
    if !key.values().any(is_text) {
        return Ok(None);
    }
    if key.values().any(|v| matches!(v, Bson::String(s) if s != "text")) {
        return Err(bad_spec(name, key, "Can't use more than one index plugin for a single index."));
    }
    let mut stored_key = Document::new();
    let mut paths: Vec<String> = Vec::new();
    let mut seen_text = false;
    for (k, v) in key {
        if is_text(v) {
            if !seen_text {
                stored_key.insert("_fts", "text");
                stored_key.insert("_ftsx", 1);
                seen_text = true;
            }
            paths.push(k.clone());
        } else {
            stored_key.insert(k.clone(), v.clone());
        }
    }
    let mut weights: std::collections::BTreeMap<String, i32> = paths.iter().map(|p| (p.clone(), 1)).collect();
    if let Some(w) = spec.get("weights") {
        let Bson::Document(wd) = w else { return Err(bad_spec(name, key, "weights must be a document")) };
        for (k, v) in wd {
            let n = crate::index::num_of(v).ok_or_else(|| bad_spec(name, key, &format!("weight for text index needs to be numeric: {k}")))?;
            if n < 1.0 || n > 2147483647.0 {
                return Err(bad_spec(name, key, &format!("text index weight must be in the interval [1, 2147483647], but found: {}", fmt_doc(&doc! { "w": v.clone() }))));
            }
            weights.insert(k.clone(), n as i32);
        }
    }
    let lang = match spec.get("default_language") {
        None => "english".to_string(),
        Some(Bson::String(l)) if crate::text::parse_lang(l).is_some() => l.clone(),
        Some(Bson::String(l)) => return Err(bad_spec(name, key, &format!("default_language is not valid: {l}"))),
        Some(_) => return Err(bad_spec(name, key, "default_language needs to be a string")),
    };
    let over = match spec.get("language_override") {
        None => "language".to_string(),
        Some(Bson::String(l)) if !l.starts_with('$') && !l.is_empty() => l.clone(),
        Some(Bson::String(l)) => return Err(bad_spec(name, key, &format!("language_override is not valid: {l}"))),
        Some(_) => return Err(bad_spec(name, key, "language_override needs to be a string")),
    };
    let version = match spec.get("textIndexVersion") {
        None => 3,
        Some(v) => match crate::index::num_of(v) {
            Some(n) if (1.0..=3.0).contains(&n) => n as i32,
            _ => return Err(bad_spec(name, key, &format!("bad textIndexVersion: {}", fmt_doc(&doc! { "v": v.clone() })))),
        },
    };
    let mut idx = doc! { "v": 2, "key": stored_key, "name": name };
    let mut w = Document::new();
    for (k, v) in &weights {
        w.insert(k.clone(), *v);
    }
    // options other than the text settings are added by the caller; the text settings must come last
    idx.insert("weights", w);
    idx.insert("default_language", lang);
    idx.insert("language_override", over);
    idx.insert("textIndexVersion", version);
    idx.insert("textKey", key.clone());
    Ok(Some(idx))
}

impl Server {
    /// Load a collection's indexes; unfinished builds / drops (after a crash or restart) are resumed in the background.
    pub(crate) async fn load_indexes(&self, g: &mut impl crate::coll::Rd, db: &str, coll: &str) -> R<Vec<IndexDef>> {
        tenant_bench::counting::set_phase(4);
        let indexes = g.indexes(coll).await?;
        tenant_bench::counting::set_phase(0);
        if indexes.iter().any(|i| i.state != State::Ready) {
            self.resume_builds(db, coll);
        }
        if indexes.iter().any(|i| i.ttl.is_some()) {
            g.note_ttl();
        }
        Ok(indexes)
    }

    pub(crate) fn resume_builds(&self, db: &str, coll: &str) {
        let key = format!("{db}\0{coll}");
        if !self.builds.lock().unwrap().insert(key.clone()) {
            return;
        }
        let dbs = Arc::clone(&self.dbs);
        let builds = Arc::clone(&self.builds);
        let (db, coll) = (db.to_string(), coll.to_string());
        tokio::spawn(async move {
            if let Err(e) = run_builds(&dbs, &db, &coll).await {
                eprintln!("background index build on {db}.{coll} failed: {e}");
            }
            builds.lock().unwrap().remove(&key);
        });
    }

    /// Wait until no build / drop is unfinished on `coll` (the one running in the background finishes it).
    async fn wait_builds(&self, db: &str, coll: &str) -> R<()> {
        loop {
            {
                let mut g = self.dbs.lock(db).await;
                if !g.indexes(coll).await?.iter().any(|i| i.state != State::Ready) {
                    return Ok(());
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    pub(crate) async fn drop_collection(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "drop")?;
        let mut g = self.dbs.lock(db).await;
        let mut names = g.collections().await?;
        let Some(pos) = names.iter().position(|n| n == coll) else {
            // MongoDB 7.0: dropping a collection that does not exist succeeds.
            return Ok(ok());
        };
        let n_indexes = 1 + g.indexes(coll).await?.iter().filter(|i| i.state == State::Ready).count() as i32;
        let mut ops: Vec<Op> = Vec::new();
        for prefix in [doc_prefix(coll), idx_collection_prefix(coll), crate::natural::prefix(coll)] {
            for (k, _) in g.scan(&prefix).await? {
                ops.push(Op::Delete(k.to_vec()));
            }
        }
        ops.push(Op::Delete(index_key(coll)));
        ops.push(Op::Delete(crate::natural::meta_key(coll)));
        names.remove(pos);
        ops.push(Entry::collections_op(&names)?);
        g.apply(ops).await?;
        self.dbs.changes.publish(vec![crate::changes::EvSpec { db: db.to_string(), coll: Some(coll.to_string()), op: "drop", doc_key: None, full: None, desc: None, txn: None }]);
        Ok(doc! { "nIndexesWas": n_indexes, "ns": format!("{db}.{coll}"), "ok": 1.0 })
    }

    /// `renameCollection` within one database (a database is one tenant; a rename across databases would have to
    /// copy between two tenants and is not supported). One atomic batch moves the documents, the index entries and the
    /// index metadata to the new name, so a crash leaves either the old or the new collection.
    pub(crate) async fn rename_collection(&self, cmd: &Document) -> R<Document> {
        let split = |field: &str| -> R<(String, String)> {
            let ns = cmd.get_str(field).map_err(|_| MongoError::new(40414, "IDLFailedToParse", format!("BSON field '{field}' is missing but a required field")))?;
            match ns.split_once('.') {
                Some((d, c)) if !d.is_empty() && !c.is_empty() => Ok((d.to_string(), c.to_string())),
                _ => Err(MongoError::new(73, "InvalidNamespace", format!("Invalid namespace specified '{ns}'"))),
            }
        };
        let (from_db, from) = split("renameCollection")?;
        let (to_db, to) = split("to")?;
        let drop_target = bool_arg(cmd.get("dropTarget"), false);
        if from_db != to_db {
            return Err(MongoError::new(238, "NotImplemented", "renameCollection across databases is not supported by this server".into()));
        }
        if !valid_db_name(&from_db) {
            return Err(MongoError::new(73, "InvalidNamespace", format!("Invalid database name: '{from_db}'")));
        }
        if to.contains('$') || to.contains('\0') || to.starts_with("system.") || from.starts_with("system.") {
            return Err(MongoError::new(20, "IllegalOperation", format!("Invalid target namespace: {to_db}.{to}")));
        }
        let db = from_db.as_str();
        let mut g = self.dbs.lock(db).await;
        let mut names = g.collections().await?;
        if !names.iter().any(|n| *n == from) {
            return Err(MongoError::namespace_not_found(format!("Source collection {db}.{from} does not exist")));
        }
        if from == to {
            return Err(MongoError::new(20, "IllegalOperation", "Can't rename a collection to itself".into()));
        }
        let target_exists = names.iter().any(|n| *n == to);
        if target_exists && !drop_target {
            return Err(MongoError::new(48, "NamespaceExists", format!("target namespace exists")));
        }
        let from_ix = g.indexes(&from).await?;
        if from_ix.iter().any(|i| i.state != State::Ready) {
            return Err(MongoError::new(2, "BadValue", format!("cannot rename {db}.{from}: an index build is in progress")));
        }
        let mut ops: Vec<Op> = Vec::new();
        if target_exists {
            for prefix in [doc_prefix(&to), idx_collection_prefix(&to), crate::natural::prefix(&to)] {
                for (k, _) in g.scan(&prefix).await? {
                    ops.push(Op::Delete(k.to_vec()));
                }
            }
            ops.push(Op::Delete(index_key(&to)));
            ops.push(Op::Delete(crate::natural::meta_key(&to)));
        }
        for (old_prefix, new_prefix) in [(doc_prefix(&from), doc_prefix(&to)), (idx_collection_prefix(&from), idx_collection_prefix(&to)), (crate::natural::prefix(&from), crate::natural::prefix(&to))] {
            for (k, v) in g.scan(&old_prefix).await? {
                let mut nk = new_prefix.clone();
                nk.extend_from_slice(&k[old_prefix.len()..]);
                ops.push(Op::Delete(k.to_vec()));
                ops.push(Op::Put(nk, v.to_vec()));
            }
        }
        if ops.len() > RENAME_MAX_OPS {
            return Err(MongoError::new(238, "NotImplemented", format!("collection {db}.{from} is too large to rename atomically ({} keys, limit {RENAME_MAX_OPS})", ops.len() / 2)));
        }
        if let Some(v) = g.get(&index_key(&from)).await? {
            ops.push(Op::Put(index_key(&to), v.to_vec()));
            ops.push(Op::Delete(index_key(&from)));
        }
        if let Some(v) = g.get(&crate::natural::meta_key(&from)).await? {
            ops.push(Op::Put(crate::natural::meta_key(&to), v.to_vec()));
            ops.push(Op::Delete(crate::natural::meta_key(&from)));
        }
        names.retain(|n| *n != from && *n != to);
        names.push(to.clone());
        ops.push(Entry::collections_op(&names)?);
        g.apply(ops).await?;
        Ok(ok())
    }

    pub(crate) async fn list_indexes(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "listIndexes")?;
        let mut g = self.dbs.lock(db).await;
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Err(MongoError::namespace_not_found(format!("ns does not exist: {db}.{coll}")));
        }
        let mut docs: VecDeque<Document> = VecDeque::new();
        docs.push_back(doc! { "v": 2, "key": { "_id": 1 }, "name": "_id_" });
        let indexes = self.load_indexes(&mut g, db, coll).await?;
        docs.extend(indexes.iter().filter(|i| i.state == State::Ready).map(|i| i.spec.clone()));
        drop(g);
        let batch = cmd.get_document("cursor").ok().and_then(|c| int_arg(c.get("batchSize"))).map(|b| b as usize);
        Ok(self.cursor_reply(format!("{db}.{coll}"), docs, batch, false))
    }

    pub(crate) async fn create_indexes(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "createIndexes")?;
        let specs = cmd.get_array("indexes").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'createIndexes.indexes' is missing but a required field".into()))?;
        let ns = format!("{db}.{coll}");
        let mut g = self.dbs.lock(db).await;
        let mut names = g.collections().await?;
        let existed = names.iter().any(|n| n == coll);
        let mut indexes = self.load_indexes(&mut g, db, coll).await?;
        if indexes.iter().any(|i| i.state != State::Ready) {
            drop(g);
            self.wait_builds(db, coll).await?;
            g = self.dbs.lock(db).await;
            indexes = g.indexes(coll).await?;
        }
        let before = 1 + indexes.len() as i32;
        let has_docs = existed && !g.scan_range(&doc_prefix(coll), prefix_end(&doc_prefix(coll)).as_deref(), false, Some(1), 4096).await?.is_empty();
        let mut changed = false;
        let mut building = false;
        let mut new_names: Vec<String> = Vec::new();
        let mut skipped_existing = false;
        for spec in specs {
            let spec = spec.as_document().ok_or_else(|| MongoError::new(14, "TypeMismatch", "Each element of the 'indexes' array must be an object".into()))?;
            let key = spec.get_document("key").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'indexes.key' is missing but a required field".into()))?;
            if key.is_empty() {
                return Err(MongoError::new(67, "CannotCreateIndex", "Index keys cannot be empty.".into()));
            }
            let name = spec.get_str("name").map(str::to_string).unwrap_or_else(|_| spec_name(key));
            if name == "_id_" || (key.len() == 1 && matches!(key.get("_id"), Some(Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_)))) {
                continue; // the implicit _id index
            }
            crate::wild::plugin_conflicts(spec, key)?;
            // Normalised spec: the user's options in MongoDB's order.
            let text_idx = normalize_text(spec, key, &name)?;
            let geo_idx = if text_idx.is_none() { normalize_geo(spec, key, &name)? } else { None };
            if text_idx.is_none() && geo_idx.is_none() {
                let flag = |n: &str| bool_arg(spec.get(n), false);
                crate::wild::validate_spec(spec, key, flag("unique"), flag("sparse"), spec.get("expireAfterSeconds").is_some())?;
            }
            let mut idx = match (&text_idx, &geo_idx) {
                (Some(d), _) => d.clone(),
                (None, Some(d)) => {
                    let mut d = d.clone();
                    d.remove("geoSettings");
                    d
                }
                _ => doc! { "v": 2, "key": key.clone(), "name": name.clone() },
            };
            let ikey: Document = idx.get_document("key").cloned().unwrap_or_else(|_| key.clone());
            let key = &ikey;
            for (k, v) in spec {
                if text_idx.is_some() && matches!(k.as_str(), "weights" | "default_language" | "language_override" | "textIndexVersion") {
                    continue;
                }
                if geo_idx.is_some() && matches!(k.as_str(), "bits" | "min" | "max" | "2dsphereIndexVersion" | "coarsestIndexedLevel" | "finestIndexedLevel") {
                    continue;
                }
                if !matches!(k.as_str(), "key" | "name" | "v") {
                    // `unique: false` and `sparse: false` are the defaults and are not stored
                    if matches!(k.as_str(), "unique" | "sparse") && !bool_arg(Some(v), false) {
                        continue;
                    }
                    idx.insert(k.clone(), v.clone());
                }
            }
            if let Some(Bson::Document(gs)) = geo_idx.as_ref().and_then(|d| d.get("geoSettings")) {
                for (k, v) in gs {
                    idx.insert(k.clone(), v.clone());
                }
            }
            if let Some(t) = &text_idx {
                for f in ["weights", "default_language", "language_override", "textIndexVersion"] {
                    if let Some(v) = t.get(f).cloned() {
                        idx.remove(f);
                        idx.insert(f, v);
                    }
                }
            }
            for opt in ["unique", "sparse"] {
                if let Some(v) = idx.get(opt) {
                    if !matches!(v, Bson::Boolean(_)) && num_opt(v).is_none() {
                        return Err(MongoError::new(14, "TypeMismatch", format!("The field '{opt}' must be a boolean or number")));
                    }
                }
            }
            if idx.get("expireAfterSeconds").is_some() && key.len() > 1 {
                return Err(MongoError::new(67, "CannotCreateIndex", format!("TTL indexes are single-field indexes, compound indexes do not support TTL. Index spec: {}", fmt_doc(&idx))));
            }
            if let Some(v) = idx.get("expireAfterSeconds") {
                match num_opt(v) {
                    Some(n) if n >= 0.0 && n <= 2147483647.0 => {}
                    Some(_) => return Err(MongoError::new(67, "CannotCreateIndex", "TTL index 'expireAfterSeconds' option must be within an acceptable range, try a different number than ".to_string() + &fmt_doc(&doc! {"v": v.clone()}).replace("{ v: ", "").replace(" }", ""))),
                    None => return Err(MongoError::new(67, "CannotCreateIndex", format!("TTL index 'expireAfterSeconds' option must be numeric, but received a type of '{}'. Index spec: {}", crate::update::type_name(v), fmt_doc(&idx)))),
                }
                if key.contains_key("_id") && key.len() == 1 {
                    return Err(MongoError::new(67, "CannotCreateIndex", "The field 'expireAfterSeconds' is not valid for an _id index specification.".into()));
                }
            }
            if let Some(p) = idx.get("partialFilterExpression") {
                let Bson::Document(pd) = p else {
                    return Err(MongoError::type_mismatch("'partialFilterExpression' for an index has to be a document".into()));
                };
                validate_partial(pd)?;
                if bool_arg(idx.get("sparse"), false) {
                    return Err(MongoError::new(67, "CannotCreateIndex", "cannot mix \"partialFilterExpression\" and \"sparse\" options".into()));
                }
            }
            let wanted = IndexDef::from_stored(&idx).ok_or_else(|| MongoError::new(67, "CannotCreateIndex", "bad index specification".into()))?;
            if let Some(ex) = indexes.iter().find(|i| i.name == name) {
                if ex.spec.get_document("key").map(|k| k == key).unwrap_or(false) && same_options(&ex.spec, &idx) {
                    if new_names.contains(&name) {
                        return Err(MongoError::new(96, "OperationFailed", format!("Index build failed: {}: Collection {ns} ( {} ) :: caused by :: Cannot build two identical indexes. Try again without duplicate indexes.", fake_uuid(), fake_uuid())));
                    }
                    skipped_existing = true;
                    continue;
                }
                let without_ttl = |d: &Document| { let mut d = d.clone(); for f in ["expireAfterSeconds", "weights", "default_language", "language_override", "textIndexVersion", "textKey"] { d.remove(f); } d };
                if ex.spec.get_document("key").map(|k| k == key).unwrap_or(false) && same_options(&without_ttl(&ex.spec), &without_ttl(&idx)) {
                    return Err(MongoError::new(85, "IndexOptionsConflict", format!("An equivalent index already exists with the same name but different options. Requested index: {}, existing index: {}", fmt_doc(&idx), fmt_doc(&ex.spec))));
                }
                return Err(MongoError::new(86, "IndexKeySpecsConflict", format!("An existing index has the same name as the requested index. When index names are not specified, they are auto generated and can cause conflicts. Please refer to our documentation. Requested index: {}, existing index: {}", fmt_doc(&idx), fmt_doc(&ex.spec))));
            }
            if let Some(ex) = indexes.iter().find(|i| i.spec.get_document("key").map(|k| k == key).unwrap_or(false)) {
                if same_options(&ex.spec, &idx) {
                    return Err(MongoError::new(85, "IndexOptionsConflict", format!("Index already exists with a different name: {}", ex.name)));
                }
                return Err(MongoError::new(85, "IndexOptionsConflict", format!("Index already exists with a different name: {}", ex.name)));
            }
            let mut def = wanted;
            if has_docs && !def.special {
                def.state = State::Building;
                building = true;
            }
            new_names.push(def.name.clone());
            indexes.push(def);
            changed = true;
        }
        let mut ops = Vec::new();
        if !existed {
            names.push(coll.to_string());
            ops.push(Entry::collections_op(&names)?);
        }
        if changed {
            ops.push(Entry::indexes_op(coll, &indexes)?);
        }
        g.apply(ops).await?;
        drop(g);
        let after = 1 + indexes.len() as i32;
        if building {
            // Backfill in chunks, releasing the database between chunks; a build already running in the background is joined.
            let key = format!("{db}\0{coll}");
            if self.builds.lock().unwrap().insert(key.clone()) {
                let mut r = run_builds(&self.dbs, db, coll).await;
                if r.is_err() {
                    // all or nothing: the indexes of this request that finished before the failing one go too
                    let mut g = self.dbs.lock(db).await;
                    let mut ix = g.indexes(coll).await?;
                    for i in ix.iter_mut().filter(|i| new_names.contains(&i.name)) {
                        i.state = State::Dropping;
                        i.cursor = None;
                    }
                    g.apply(vec![Entry::indexes_op(coll, &ix)?]).await?;
                    drop(g);
                    let _ = run_builds(&self.dbs, db, coll).await;
                }
                self.builds.lock().unwrap().remove(&key);
                r.map_err(|mut e| {
                    if e.code == 11000 && !e.msg.starts_with("Index build failed") {
                        e.msg = format!("Index build failed: {}: Collection {ns} ( {} ) :: caused by :: {}", fake_uuid(), fake_uuid(), e.msg);
                    }
                    e
                })?;
            } else {
                self.wait_builds(db, coll).await?;
            }
        }
        let mut reply = doc! { "createdCollectionAutomatically": !existed, "numIndexesBefore": before, "numIndexesAfter": after };
        if !changed {
            reply.insert("note", "all indexes already exist");
        } else if skipped_existing {
            reply.insert("note", "index already exists");
        }
        reply.insert("ok", 1.0);
        Ok(reply)
    }


    pub(crate) async fn coll_mod(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "collMod")?;
        let mut g = self.dbs.lock(db).await;
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Err(MongoError::namespace_not_found(format!("ns does not exist: {db}.{coll}")));
        }
        let Some(Bson::Document(spec)) = cmd.get("index") else { return Ok(ok()) };
        let mut indexes = self.load_indexes(&mut g, db, coll).await?;
        let pos = if let Ok(name) = spec.get_str("name") {
            indexes.iter().position(|i| i.name == name && i.state == State::Ready)
        } else if let Ok(kp) = spec.get_document("keyPattern") {
            indexes.iter().position(|i| i.state == State::Ready && i.spec.get_document("key").map(|k| k == kp).unwrap_or(false))
        } else {
            return Err(MongoError::new(2, "BadValue", "Cannot specify both key pattern and name.".into()));
        };
        let Some(pos) = pos else { return Err(MongoError::new(27, "IndexNotFound", "cannot find index in ns".to_string() + &format!(" {db}.{coll} for the given spec"))) };
        let Some(new) = spec.get("expireAfterSeconds") else { return Ok(ok()) };
        let Some(newn) = num_opt(new).filter(|n| *n >= 0.0) else {
            return Err(MongoError::new(2, "BadValue", "TTL index 'expireAfterSeconds' option must be within an acceptable range".into()));
        };
        let old = indexes[pos].spec.get("expireAfterSeconds").and_then(num_opt);
        let mut stored = indexes[pos].to_stored();
        stored.insert("expireAfterSeconds", if (newn as i64) <= i32::MAX as i64 { Bson::Int32(newn as i32) } else { Bson::Int64(newn as i64) });
        indexes[pos] = IndexDef::from_stored(&stored).unwrap();
        g.apply(vec![Entry::indexes_op(coll, &indexes)?]).await?;
        let mut reply = Document::new();
        if let Some(o) = old {
            reply.insert("expireAfterSeconds_old", Bson::Int64(o as i64));
        }
        reply.insert("expireAfterSeconds_new", Bson::Int64(newn as i64));
        reply.insert("ok", 1.0);
        Ok(reply)
    }

    pub(crate) async fn drop_indexes(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "dropIndexes")?;
        let which = cmd.get("index").ok_or_else(|| MongoError::new(40414, "Location40414", "BSON field 'dropIndexes.index' is missing but a required field".into()))?;
        let mut g = self.dbs.lock(db).await;
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Err(MongoError::namespace_not_found(format!("ns not found {db}.{coll}")));
        }
        let mut indexes = self.load_indexes(&mut g, db, coll).await?;
        let n_was = 1 + indexes.iter().filter(|i| i.state == State::Ready).count() as i32;
        let targets: Vec<usize> = match which {
            Bson::String(s) if s == "*" => (0..indexes.len()).filter(|i| indexes[*i].state == State::Ready).collect(),
            Bson::String(s) => {
                if s == "_id_" {
                    return Err(MongoError::new(72, "InvalidOptions", "cannot drop _id index".into()));
                }
                match indexes.iter().position(|i| i.name == *s && i.state == State::Ready) {
                    Some(p) => vec![p],
                    None => return Err(MongoError::new(27, "IndexNotFound", format!("index not found with name [{s}]"))),
                }
            }
            Bson::Document(k) => {
                if k.len() == 1 && k.contains_key("_id") {
                    return Err(MongoError::new(72, "InvalidOptions", "cannot drop _id index".into()));
                }
                match indexes.iter().position(|i| i.state == State::Ready && i.spec.get_document("key").map(|x| x == k).unwrap_or(false)) {
                    Some(p) => vec![p],
                    None => return Err(MongoError::new(27, "IndexNotFound", format!("can't find index with key: {}", fmt_doc(k)))),
                }
            }
            Bson::Array(a) => {
                let mut v = Vec::new();
                for n in a {
                    let s = n.as_str().ok_or_else(|| MongoError::type_mismatch("dropIndexes index array elements must be strings".into()))?;
                    match indexes.iter().position(|i| i.name == s && i.state == State::Ready) {
                        Some(p) => v.push(p),
                        None => return Err(MongoError::new(27, "IndexNotFound", format!("index not found with name [{s}]"))),
                    }
                }
                v
            }
            _ => return Err(MongoError::new(14, "TypeMismatch", "BSON field 'dropIndexes.index' is the wrong type, expected string or object".into())),
        };
        for p in &targets {
            indexes[*p].state = State::Dropping;
        }
        let ttl_none = targets.is_empty();
        if !ttl_none {
            g.apply(vec![Entry::indexes_op(coll, &indexes)?]).await?;
        }
        drop(g);
        if !ttl_none {
            let key = format!("{db}\0{coll}");
            if self.builds.lock().unwrap().insert(key.clone()) {
                let r = run_builds(&self.dbs, db, coll).await;
                self.builds.lock().unwrap().remove(&key);
                let _ = r;
            } else {
                self.wait_builds(db, coll).await?;
            }
        }
        let mut reply = doc! { "nIndexesWas": n_was };
        if matches!(which, Bson::String(s) if s == "*") {
            reply.insert("msg", "non-_id indexes dropped for collection");
        }
        reply.insert("ok", 1.0);
        Ok(reply)
    }

    // ---- explain ---------------------------------------------------------------------------

    pub(crate) async fn explain(&self, db: &str, cmd: &Document) -> R<Document> {
        let inner = cmd.get_document("explain").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'explain.explain' is missing but a required field".into()))?;
        let verbosity = cmd.get_str("verbosity").unwrap_or("allPlansExecution").to_string();
        let name = inner.keys().next().cloned().unwrap_or_default();
        let coll = collection_name(inner, &name)?;
        let ns = format!("{db}.{coll}");
        let (filter, sort, limit, skip, hint_b, projection) = match name.as_str() {
            "find" => (
                inner.get_document("filter").cloned().unwrap_or_default(),
                inner.get_document("sort").ok().map(parse_sort).transpose()?.unwrap_or_default(),
                int_arg(inner.get("limit")).unwrap_or(0),
                int_arg(inner.get("skip")).unwrap_or(0),
                inner.get("hint").cloned(),
                inner.get_document("projection").ok().cloned(),
            ),
            "count" | "distinct" => (inner.get_document("query").cloned().unwrap_or_default(), vec![], int_arg(inner.get("limit")).unwrap_or(0), int_arg(inner.get("skip")).unwrap_or(0), inner.get("hint").cloned(), None),
            "aggregate" => {
                let first = inner.get_array("pipeline").ok().and_then(|p| p.first().and_then(|s| s.as_document()).and_then(|s| s.get_document("$match").ok()).cloned()).unwrap_or_default();
                (first, vec![], 0, 0, inner.get("hint").cloned(), None)
            }
            "update" | "delete" | "findAndModify" => {
                let q = inner.get_array("updates").or_else(|_| inner.get_array("deletes")).ok().and_then(|a| a.first().and_then(|s| s.as_document()).and_then(|s| s.get_document("q").ok()).cloned()).or_else(|| inner.get_document("query").ok().cloned()).unwrap_or_default();
                (q, vec![], 0, 0, None, None)
            }
            other => return Err(MongoError::command_not_found(other)),
        };
        let mut g = self.dbs.lock(db).await;
        let indexes = self.load_indexes(&mut g, db, coll).await?;
        let hint = resolve_hint(&indexes, hint_b.as_ref())?;
        let specs = index::ttl_specs(&indexes);
        let want = if limit > 0 { Some((skip.max(0) + limit.abs()) as usize) } else { None };
        let opts = FindOpts { filter: &filter, sort: &sort, want, hint: hint.clone(), ttl: ttl_ctx(&specs), ov: None };
        let plan = plan_for(&indexes, &opts);
        let mut exec = None;
        if verbosity != "queryPlanner" {
            let (docs, stats, _) = find_docs(&mut g, coll, &indexes, &opts).await?;
            let n = docs.len().saturating_sub(skip.max(0) as usize);
            let n = if limit > 0 { n.min(limit as usize) } else { n };
            exec = Some((n, stats));
        }
        drop(g);
        let winning = winning_plan(&plan, &indexes, &filter, &sort, limit, skip, projection.as_ref(), &exec);
        let mut qp = doc! {
            "namespace": ns.clone(),
            "indexFilterSet": false,
            "parsedQuery": filter.clone(),
            "queryHash": "00000000",
            "planCacheKey": "00000000",
            "maxIndexedOrSolutionsReached": false,
            "maxIndexedAndSolutionsReached": false,
            "maxScansToExplodeReached": false,
            "winningPlan": winning.clone(),
            "rejectedPlans": [],
        };
        let _ = &mut qp;
        let mut out = doc! { "explainVersion": "1", "queryPlanner": qp };
        if let Some((n, stats)) = exec {
            out.insert(
                "executionStats",
                doc! {
                    "executionSuccess": true,
                    "nReturned": n as i32,
                    "executionTimeMillis": 0,
                    "totalKeysExamined": if matches!(plan, Plan::Coll { .. }) { 0 } else { stats.keys as i32 },
                    "totalDocsExamined": stats.docs as i32,
                    "executionStages": winning,
                },
            );
        }
        out.insert("command", Bson::Document(inner.clone()));
        out.insert("serverInfo", doc! { "host": "mongo-server", "port": 27017, "version": "7.0.14", "gitVersion": "none" });
        out.insert("serverParameters", doc! { "internalQueryFacetBufferSizeBytes": 104857600_i32 });
        out.insert("ok", 1.0);
        Ok(out)
    }

    // ---- validate, stats -------------------------------------------------------------------

    /// `validate`: every document has exactly its entries in every ready/building index and no entry points to a
    /// missing document.
    pub(crate) async fn validate(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "validate")?;
        let mut g = self.dbs.lock(db).await;
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Err(MongoError::namespace_not_found(format!("Collection '{db}.{coll}' does not exist to validate.")));
        }
        let indexes = g.indexes(coll).await?;
        let dp = doc_prefix(coll);
        let rows = g.scan(&dp).await?;
        let mut errors: Vec<Bson> = Vec::new();
        let mut keys_per_index = Document::new();
        let mut missing_total = 0_i64;
        let mut extra_total = 0_i64;
        let mut docs: Vec<(Vec<u8>, Document)> = Vec::new();
        for (k, v) in &rows {
            docs.push((k[dp.len()..].to_vec(), decode(v)?));
        }
        let mut ids: HashSet<Vec<u8>> = docs.iter().map(|(i, _)| i.clone()).collect();
        for idx in &indexes {
            if !idx.has_entries() || idx.state == State::Dropping {
                continue;
            }
            let prefix = idx.prefix(coll);
            let stored = g.scan(&prefix).await?;
            keys_per_index.insert(idx.name.clone(), stored.len() as i64);
            let mut have: HashSet<Vec<u8>> = stored.iter().map(|(k, _)| k.to_vec()).collect();
            // Documents behind the build cursor must be fully indexed; a building index only covers the part before its cursor
            // plus whatever concurrent writes added, so only check completeness for ready indexes.
            let check_missing = idx.state == State::Ready;
            for (id, d) in &docs {
                let (e, _) = idx.entries(d)?;
                for c in e.keys() {
                    let key = entry_key(&prefix, c, id);
                    if !have.remove(&key) && check_missing {
                        missing_total += 1;
                        if errors.len() < 20 {
                            errors.push(Bson::String(format!("index {} is missing an entry for document {}", idx.name, fmt_doc(&doc! {"_id": d.get("_id").cloned().unwrap_or(Bson::Null)}))));
                        }
                    } else if !check_missing {
                        have.remove(&key);
                    }
                }
            }
            for k in have {
                // Remaining entries belong to no document (or to a document whose values changed).
                if let Some(id) = entry_id(&k) {
                    let doc_exists = ids.contains(id);
                    extra_total += 1;
                    if errors.len() < 20 {
                        errors.push(Bson::String(format!("index {} has an entry that {} (extra key)", idx.name, if doc_exists { "does not match its document" } else { "points to a missing document" })));
                    }
                }
            }
        }
        ids.clear();
        Ok(doc! {
            "ns": format!("{db}.{coll}"),
            "nrecords": docs.len() as i64,
            "nIndexes": 1 + indexes.iter().filter(|i| i.state == State::Ready).count() as i32,
            "keysPerIndex": keys_per_index,
            "missingEntries": missing_total,
            "extraEntries": extra_total,
            "valid": errors.is_empty(),
            "warnings": [],
            "errors": errors,
            "extraIndexEntries": [],
            "missingIndexEntries": [],
            "ok": 1.0,
        })
    }

    /// Document count, data size and per-index sizes of a collection (O(n): read from the store).
    pub(crate) async fn storage_stats(&self, g: &mut Entry, db: &str, coll: &str) -> R<Document> {
        let indexes = g.indexes(coll).await?;
        let rows = g.scan(&doc_prefix(coll)).await?;
        let size: usize = rows.iter().map(|(_, v)| v.len()).sum();
        let mut sizes = doc! { "_id_": 0_i64 };
        let mut total = 0_i64;
        let mut n_idx = 1;
        for idx in indexes.iter().filter(|i| i.state == State::Ready) {
            let r = g.scan(&idx.prefix(coll)).await?;
            let bytes: usize = r.iter().map(|(k, v)| k.len() + v.len()).sum();
            sizes.insert(idx.name.clone(), bytes as i64);
            total += bytes as i64;
            n_idx += 1;
        }
        let id_bytes: usize = rows.iter().map(|(k, _)| k.len() - doc_prefix(coll).len()).sum();
        sizes.insert("_id_", id_bytes as i64);
        total += id_bytes as i64;
        let count = rows.len() as i64;
        let _ = db;
        Ok(doc! {
            "size": size as i64,
            "count": rows.len() as i32,
            "avgObjSize": if count > 0 { size as i64 / count } else { 0 },
            "storageSize": size as i64,
            "freeStorageSize": 0_i64,
            "capped": false,
            "nindexes": n_idx,
            "indexDetails": {},
            "indexBuilds": [],
            "totalIndexSize": total,
            "totalSize": size as i64 + total,
            "indexSizes": sizes,
            "scaleFactor": 1,
        })
    }

    pub(crate) async fn coll_stats(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "collStats")?;
        let mut g = self.dbs.lock(db).await;
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Err(MongoError::namespace_not_found(format!("Collection [{db}.{coll}] not found.")));
        }
        let mut st = self.storage_stats(&mut g, db, coll).await?;
        let mut out = doc! { "ns": format!("{db}.{coll}") };
        out.extend(std::mem::take(&mut st));
        out.insert("ok", 1.0);
        Ok(out)
    }

    pub(crate) async fn db_stats(&self, db: &str, _cmd: &Document) -> R<Document> {
        let mut g = self.dbs.lock(db).await;
        let names = g.collections().await?;
        let (mut objects, mut data, mut indexes_n, mut index_size) = (0_i64, 0_i64, 0_i64, 0_i64);
        for c in &names {
            let s = self.storage_stats(&mut g, db, c).await?;
            objects += s.get_i32("count").unwrap_or(0) as i64;
            data += s.get_i64("size").unwrap_or(0);
            indexes_n += s.get_i32("nindexes").unwrap_or(0) as i64;
            index_size += s.get_i64("totalIndexSize").unwrap_or(0);
        }
        Ok(doc! {
            "db": db, "collections": names.len() as i64, "views": 0_i64, "objects": objects, "avgObjSize": if objects > 0 { data as f64 / objects as f64 } else { 0.0 },
            "dataSize": data, "storageSize": data, "indexes": indexes_n, "indexSize": index_size, "totalSize": data + index_size, "scaleFactor": 1, "ok": 1.0,
        })
    }
}

fn same_options(a: &Document, b: &Document) -> bool {
    let norm = |d: &Document| -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> = d
            .iter()
            .filter(|(k, _)| !matches!(k.as_str(), "v" | "name" | "textKey" | "2dsphereIndexVersion"))
            .filter(|(k, v)| !(matches!(k.as_str(), "unique" | "sparse") && !crate::cmd::bool_arg(Some(v), false)))
            .map(|(k, v)| (k.clone(), crate::update::fmt_bson(v)))
            .collect();
        v.sort();
        v
    };
    norm(a) == norm(b)
}

/// `partialFilterExpression` accepts only equality, `$exists: true`, `$gt/$gte/$lt/$lte`, `$type`, `$in` (single-type) and top-level `$and`.
fn validate_partial(p: &Document) -> R<()> {
    for (k, v) in p {
        match k.as_str() {
            "$and" => {
                let Bson::Array(a) = v else { return Err(MongoError::bad_value("$and must be an array".into())) };
                for s in a {
                    match s {
                        Bson::Document(d) => validate_partial(d)?,
                        _ => return Err(MongoError::bad_value("$and/$or/$nor entries need to be objects".into())),
                    }
                }
            }
            k if k.starts_with('$') => return Err(MongoError::new(67, "CannotCreateIndex", format!("Expression not supported in partial index: {}", fmt_doc(&doc! { k: v.clone() })))),
            _ => {
                if let Bson::Document(d) = v {
                    if d.keys().any(|o| o.starts_with('$')) {
                        for (op, arg) in d {
                            let ok = match op.as_str() {
                                "$eq" | "$gt" | "$gte" | "$lt" | "$lte" | "$type" | "$in" => true,
                                "$exists" => matches!(arg, Bson::Boolean(true)) || num_opt(arg).map(|n| n != 0.0).unwrap_or(false),
                                _ => false,
                            };
                            if !ok {
                                return Err(MongoError::new(67, "CannotCreateIndex", format!("Expression not supported in partial index: {}", fmt_doc(&doc! { k: v.clone() }))));
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

// ---- explain plan trees ----------------------------------------------------------------------

/// `"[a, b)"` read backwards is `"(b, a]"`.
fn mirror(l: &str) -> String {
    let flip = |c: char| match c {
        '[' => ']',
        ']' => '[',
        '(' => ')',
        ')' => '(',
        o => o,
    };
    let (open, close) = (l.chars().next().unwrap_or('['), l.chars().last().unwrap_or(']'));
    let inner = &l[open.len_utf8()..l.len() - close.len_utf8()];
    match inner.split_once(", ") {
        Some((a, b)) => format!("{}{b}, {a}{}", flip(close), flip(open)),
        None => l.to_string(),
    }
}

fn ix_stage(idx: &IndexDef, s: &index::IxScan) -> Document {
    let mut mk_paths = Document::new();
    for (f, _) in &idx.key {
        mk_paths.insert(f.clone(), if idx.multikey { Bson::Array(vec![Bson::String(f.clone())]) } else { Bson::Array(vec![]) });
    }
    let mut bounds = Document::new();
    for (f, labels) in &s.bounds {
        let mut ls: Vec<String> = labels.clone();
        if s.reverse {
            ls = ls.iter().rev().map(|l| mirror(l)).collect();
        }
        bounds.insert(f.clone(), ls.into_iter().map(Bson::String).collect::<Vec<_>>());
    }
    doc! {
        "stage": "IXSCAN",
        "keyPattern": idx.key_pattern(),
        "indexName": idx.name.clone(),
        "isMultiKey": idx.multikey,
        "multiKeyPaths": mk_paths,
        "isUnique": idx.unique,
        "isSparse": idx.sparse,
        "isPartial": idx.partial.is_some(),
        "indexVersion": 2,
        "direction": if s.reverse { "backward" } else { "forward" },
        "indexBounds": bounds,
    }
}

#[allow(clippy::too_many_arguments)]
fn winning_plan(plan: &Plan, indexes: &[IndexDef], filter: &Document, sort: &[(String, bool)], limit: i64, skip: i64, projection: Option<&Document>, exec: &Option<(usize, crate::coll::Stats)>) -> Document {
    let counts = |d: &mut Document, keys: usize, docs: usize, n: usize| {
        if exec.is_some() {
            d.insert("nReturned", n as i32);
            d.insert("executionTimeMillisEstimate", 0);
            d.insert("works", n as i32 + 1);
            d.insert("advanced", n as i32);
            if keys > 0 {
                d.insert("keysExamined", keys as i32);
            }
            if docs > 0 {
                d.insert("docsExamined", docs as i32);
            }
        }
    };
    let (nret, stats) = exec.as_ref().map(|(n, s)| (*n, s.clone())).unwrap_or((0, Default::default()));
    let mut node = match plan {
        Plan::Id(_) => {
            let mut d = doc! { "stage": "IDHACK" };
            counts(&mut d, stats.keys, stats.docs, nret);
            d
        }
        Plan::Coll { reverse } => {
            let mut d = doc! { "stage": "COLLSCAN" };
            if !filter.is_empty() {
                d.insert("filter", filter.clone());
            }
            d.insert("direction", if *reverse { "backward" } else { "forward" });
            counts(&mut d, 0, stats.docs, nret);
            d
        }
        Plan::Ix(s) => {
            let mut ix = ix_stage(&indexes[s.idx], s);
            counts(&mut ix, stats.keys, 0, stats.docs);
            let mut f = doc! { "stage": "FETCH" };
            let residual: Document = filter.iter().filter(|(k, _)| !s.bounds.iter().take(s.bounded_fields).any(|(bf, _)| bf == *k)).map(|(k, v)| (k.clone(), v.clone())).collect();
            if !residual.is_empty() {
                f.insert("filter", residual);
            }
            counts(&mut f, 0, stats.docs, nret);
            f.insert("inputStage", ix);
            f
        }
        Plan::Or(subs) => {
            let inputs: Vec<Bson> = subs.iter().map(|p| Bson::Document(winning_plan(p, indexes, &Document::new(), &[], 0, 0, None, &None).get_document("inputStage").cloned().unwrap_or_else(|_| doc! { "stage": "IDHACK" }))).collect();
            let mut f = doc! { "stage": "FETCH" };
            counts(&mut f, 0, stats.docs, nret);
            f.insert("inputStage", doc! { "stage": "OR", "inputStages": inputs });
            f
        }
    };
    let provided = matches!(plan, Plan::Ix(s) if s.sorted);
    if filter.len() == 1 && filter.contains_key("$or") {
        let mut d = doc! { "stage": "SUBPLAN" };
        counts(&mut d, 0, 0, nret);
        d.insert("inputStage", node);
        node = d;
    }
    if !sort.is_empty() && !provided {
        let mut sp = Document::new();
        for (f, asc) in sort {
            sp.insert(f.clone(), if *asc { 1 } else { -1 });
        }
        let mut d = doc! { "stage": "SORT", "sortPattern": sp, "memLimit": 104857600_i64 };
        if limit > 0 {
            d.insert("limitAmount", limit + skip.max(0));
        }
        d.insert("type", "simple");
        counts(&mut d, 0, 0, nret);
        d.insert("inputStage", node);
        node = d;
    }
    if limit > 0 && sort.is_empty() || (limit > 0 && provided) {
        let mut d = doc! { "stage": "LIMIT", "limitAmount": limit };
        counts(&mut d, 0, 0, nret);
        d.insert("inputStage", node);
        node = d;
    }
    if skip > 0 {
        let mut d = doc! { "stage": "SKIP", "skipAmount": skip };
        counts(&mut d, 0, 0, nret);
        d.insert("inputStage", node);
        node = d;
    }
    if let Some(p) = projection {
        let simple = p.iter().all(|(k, v)| !k.starts_with('$') && matches!(v, Bson::Int32(_) | Bson::Int64(_) | Bson::Boolean(_) | Bson::Double(_)));
        let mut d = doc! { "stage": if simple { "PROJECTION_SIMPLE" } else { "PROJECTION_DEFAULT" }, "transformBy": p.clone() };
        counts(&mut d, 0, 0, nret);
        d.insert("inputStage", node);
        node = d;
    }
    node
}

