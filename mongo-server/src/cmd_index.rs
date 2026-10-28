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
                    let mut e = MongoError::duplicate_key_c(&ns, &idx.name, &idx.key_pattern(), &kv, idx.collation.as_deref());
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
        let opts = g.coll_opts(coll).await?;
        crate::collation::note_default(opts.and_then(|o| o.collation.clone()));
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
        let uuid = g.collection_uuid(coll).await?;
        let mut ops: Vec<Op> = Vec::new();
        for prefix in [doc_prefix(coll), idx_collection_prefix(coll), crate::natural::prefix(coll), crate::collopts::idc_prefix(coll)] {
            for (k, _) in g.scan(&prefix).await? {
                ops.push(Op::Delete(k.to_vec()));
            }
        }
        ops.push(Op::Delete(index_key(coll)));
        ops.push(Op::Delete(crate::natural::meta_key(coll)));
        ops.push(Op::Delete(crate::collopts::meta_key(coll)));
        ops.push(Op::Delete(crate::collopts::uuid_key(coll)));
        ops.push(Op::Delete(crate::capped::stat_key(coll)));
        ops.push(Server::search_idx_delete_op(coll));
        names.remove(pos);
        ops.push(Entry::collections_op(&names)?);
        g.apply(ops).await?;
        g.set_coll_opts(coll, None);
        self.publish_drop(db, coll, uuid);
        Ok(doc! { "nIndexesWas": n_indexes, "ns": format!("{db}.{coll}"), "ok": 1.0 })
    }

    /// `renameCollection` within one database (a database is one tenant; a rename across databases would have to
    /// copy between two tenants and is not supported). One atomic batch moves the documents, the index entries and the
    /// index metadata to the new name, so a crash leaves either the old or the new collection.
    pub(crate) async fn rename_collection(&self, cmd_db: &str, cmd: &Document) -> R<Document> {
        let r = crate::xdb::parse_rename(cmd_db, cmd)?;
        self.view_rename_check(&r.from_db, &r.from, &r.to_db, &r.to).await?;
        if r.from_db != r.to_db {
            return self.rename_across(&r).await;
        }
        let (from_db, from, to_db, to, drop_target) = (r.from_db, r.from, r.to_db, r.to, r.drop_target);
        let _ = &to_db;
        let db = from_db.as_str();
        let mut g = self.dbs.lock(db).await;
        let mut names = g.collections().await?;
        if !names.iter().any(|n| *n == from) {
            return Err(MongoError::namespace_not_found("source namespace does not exist".into()));
        }
        let target_exists = names.iter().any(|n| *n == to);
        if target_exists && !drop_target {
            return Err(MongoError::new(48, "NamespaceExists", format!("target namespace exists")));
        }
        let from_ix = g.indexes(&from).await?;
        if from_ix.iter().any(|i| i.state != State::Ready) {
            return Err(MongoError::new(2, "BadValue", format!("cannot rename {db}.{from}: an index build is in progress")));
        }
        let src_uuid = g.collection_uuid(&from).await?;
        let dropped_uuid = if target_exists { Some(g.collection_uuid(&to).await?) } else { None };
        let mut ops: Vec<Op> = Vec::new();
        if target_exists {
            for prefix in [doc_prefix(&to), idx_collection_prefix(&to), crate::natural::prefix(&to), crate::collopts::idc_prefix(&to)] {
                for (k, _) in g.scan(&prefix).await? {
                    ops.push(Op::Delete(k.to_vec()));
                }
            }
            ops.push(Op::Delete(index_key(&to)));
            ops.push(Op::Delete(crate::natural::meta_key(&to)));
            ops.push(Op::Delete(crate::collopts::meta_key(&to)));
            ops.push(Op::Delete(crate::collopts::uuid_key(&to)));
            ops.push(Op::Delete(crate::capped::stat_key(&to)));
        }
        for (old_prefix, new_prefix) in [(doc_prefix(&from), doc_prefix(&to)), (idx_collection_prefix(&from), idx_collection_prefix(&to)), (crate::natural::prefix(&from), crate::natural::prefix(&to)), (crate::collopts::idc_prefix(&from), crate::collopts::idc_prefix(&to))] {
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
        if let Some(v) = g.get(&crate::collopts::meta_key(&from)).await? {
            ops.push(Op::Put(crate::collopts::meta_key(&to), v.to_vec()));
            ops.push(Op::Delete(crate::collopts::meta_key(&from)));
        }
        if let Some(v) = g.get(&crate::collopts::uuid_key(&from)).await? {
            ops.push(Op::Put(crate::collopts::uuid_key(&to), v.to_vec()));
            ops.push(Op::Delete(crate::collopts::uuid_key(&from)));
        }
        if let Some(v) = g.get(&crate::capped::stat_key(&from)).await? {
            ops.push(Op::Put(crate::capped::stat_key(&to), v.to_vec()));
            ops.push(Op::Delete(crate::capped::stat_key(&from)));
        }
        g.forget_coll_opts(&from);
        g.forget_coll_opts(&to);
        names.retain(|n| *n != from && *n != to);
        names.push(to.clone());
        ops.push(Entry::collections_op(&names)?);
        g.apply(ops).await?;
        drop(g);
        self.publish_rename(db, &from, db, &to, src_uuid, dropped_uuid);
        Ok(ok())
    }

    pub(crate) async fn list_indexes(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "listIndexes")?;
        let mut g = self.dbs.lock(db).await;
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Err(MongoError::namespace_not_found(format!("ns does not exist: {db}.{coll}")));
        }
        let mut docs: VecDeque<Document> = VecDeque::new();
        let indexes = self.load_indexes(&mut g, db, coll).await?;
        let mut id_spec = doc! { "v": 2, "key": { "_id": 1 }, "name": "_id_" };
        if let Some(c) = g.collation_of(coll) {
            id_spec.insert("collation", c.to_doc());
        }
        let kind = g.coll_opts(coll).await?;
        match kind.as_ref() {
            Some(k) if k.clustered.is_some() => {
                let mut spec = doc! { "v": 2, "key": { "_id": 1 }, "name": k.clustered.clone().unwrap(), "unique": true };
                if let Some(e) = k.raw.get("expireAfterSeconds").and_then(|e| e.as_i64().or(e.as_i32().map(i64::from))) {
                    spec.insert("expireAfterSeconds", e.min(i32::MAX as i64) as i32);
                }
                spec.insert("clustered", true);
                docs.push_back(spec)
            }
            Some(k) if k.series.is_some() => {}
            _ => docs.push_back(id_spec),
        }
        docs.extend(indexes.iter().filter(|i| i.state == State::Ready && !crate::special_colls::is_internal(i)).map(|i| i.spec.clone()));
        drop(g);
        let batch = cmd.get_document("cursor").ok().and_then(|c| int_arg(c.get("batchSize"))).map(|b| b as usize);
        Ok(self.cursor_reply(format!("{db}.{coll}"), docs, batch, false))
    }

    /// The `commitQuorum` of `createIndexes`: only a replica set takes one (the engine is a single data-bearing member).
    fn commit_quorum(&self, v: Option<&Bson>) -> R<Option<Bson>> {
        let Some(v) = v else { return Ok(self.is_rs().then(|| Bson::String("votingMembers".into()))) };
        let n = match v {
            Bson::String(_) => None,
            Bson::Int32(i) => Some(*i as f64),
            Bson::Int64(i) => Some(*i as f64),
            Bson::Double(f) => Some(*f),
            Bson::Decimal128(d) => d.to_string().parse::<f64>().ok(),
            other => {
                return Err(MongoError::type_mismatch(format!("BSON field 'createIndexes.commitQuorum' is the wrong type '{}', expected types '[long, int, decimal, double, string']", crate::update::type_name(other))));
            }
        };
        if matches!(n, Some(n) if !(0.0..=50.0).contains(&n)) {
            return Err(MongoError::failed_to_parse("commitQuorum has to be a non-negative number and not greater than 50".into()));
        }
        if self.rs.lock().unwrap().is_none() {
            return Err(MongoError::bad_value("Standalones can't specify commitQuorum".into()));
        }
        match (v, n) {
            (Bson::String(s), _) => {
                if s != "votingMembers" && s != "majority" {
                    return Err(MongoError::new(79, "UnknownReplWriteConcern", format!("No write concern mode named '{s}' found in replica set configuration")));
                }
                Ok(Some(v.clone()))
            }
            (_, Some(n)) => {
                if !(0.0..=50.0).contains(&n) {
                    return Err(MongoError::failed_to_parse("commitQuorum has to be a non-negative number and not greater than 50".into()));
                }
                if n >= 2.0 {
                    return Err(MongoError::new(278, "UnsatisfiableCommitQuorum", "Not enough data-bearing nodes to satisfy commit quorum".into()));
                }
                Ok(Some(Bson::Int32(n as i32)))
            }
            _ => Ok(None),
        }
    }

    pub(crate) async fn create_indexes(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "createIndexes")?;
        let specs = cmd.get_array("indexes").map_err(|_| MongoError::new(40414, "IDLFailedToParse", "BSON field 'createIndexes.indexes' is missing but a required field".into()))?;
        let ns = format!("{db}.{coll}");
        let commit_quorum = self.commit_quorum(cmd.get("commitQuorum"))?;
        if specs.is_empty() {
            return Err(MongoError::bad_value("Must specify at least one index to create".into()));
        }
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
        let default_collation = g.coll_opts(coll).await?.and_then(|o| o.collation.clone());
        let series_base = if g.coll_opts(coll).await?.map(|k| k.series.is_some()).unwrap_or(false) { 0 } else { 1 };
        let before = series_base + indexes.iter().filter(|i| !crate::special_colls::is_internal(i)).count() as i32;
        let has_docs = existed && !g.scan_range(&doc_prefix(coll), prefix_end(&doc_prefix(coll)).as_deref(), false, Some(1), 4096).await?.is_empty();
        let mut changed = false;
        let mut building = false;
        let mut new_names: Vec<String> = Vec::new();
        let mut skipped_existing = false;
        for spec in specs {
            let spec = spec.as_document().ok_or_else(|| MongoError::new(14, "TypeMismatch", "Each element of the 'indexes' array must be an object".into()))?;
            let key = spec.get_document("key").map_err(|_| MongoError::new(40414, "IDLFailedToParse", "BSON field 'indexes.key' is missing but a required field".into()))?;
            if key.is_empty() {
                return Err(MongoError::new(67, "CannotCreateIndex", "Index keys cannot be empty.".into()));
            }
            let name = spec.get_str("name").map(str::to_string).unwrap_or_else(|_| spec_name(key));
            if name == "_id_" || (key.len() == 1 && matches!(key.get("_id"), Some(Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_)))) {
                if spec.contains_key("hidden") {
                    let mut shown = spec.clone();
                    shown.insert("v", 2);
                    return Err(MongoError::new(197, "InvalidIndexSpecificationOption", format!("The field 'hidden' is not valid for an _id index specification. Specification: {}", fmt_doc(&shown))));
                }
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
                if !matches!(k.as_str(), "key" | "name" | "v" | "collation") {
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
            if let Some(v) = idx.get("hidden") {
                if !matches!(v, Bson::Boolean(_)) && num_opt(v).is_none() {
                    // (mongod names the field 'name' in this message)
                    return Err(MongoError::new(14, "TypeMismatch", format!("Error in specification {} :: caused by :: The field 'name' must be a bool, but got {}", fmt_doc(spec), crate::update::type_name(v))));
                }
                if key.len() == 1 && key.contains_key("_id") {
                    let mut shown = spec.clone();
                    shown.insert("v", 2);
                    return Err(MongoError::new(197, "InvalidIndexSpecificationOption", format!("The field 'hidden' is not valid for an _id index specification. Specification: {}", fmt_doc(&shown))));
                }
            }
            for opt in ["unique", "sparse"] {
                if let Some(v) = idx.get(opt) {
                    if !matches!(v, Bson::Boolean(_)) && num_opt(v).is_none() {
                        return Err(MongoError::new(14, "TypeMismatch", format!("The field '{opt}' must be a boolean or number")));
                    }
                }
            }
            if bool_arg(idx.get("unique"), false) && bool_arg(idx.get("prepareUnique"), false) {
                let mut shown = idx.clone();
                shown.remove("v");
                return Err(MongoError::new(67, "CannotCreateIndex", format!("Error in specification {} :: caused by :: Invalid index specification {}; cannot create an index with the 'unique' option and the 'prepareUnique' option", fmt_doc(&shown), fmt_doc(&shown))));
            }
            if idx.get("expireAfterSeconds").is_some() && g.cap_of(coll).is_some() {
                return Err(MongoError::new(67, "CannotCreateIndex", "Cannot create a TTL index on a capped collection".into()));
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
                    let mut shown = Document::new();
                    for (k, v) in idx.iter().filter(|(k, _)| !matches!(k.as_str(), "v" | "key")) {
                        shown.insert(k.clone(), v.clone());
                    }
                    for k in ["key", "v"] {
                        if let Some(v) = idx.get(k) {
                            shown.insert(k, v.clone());
                        }
                    }
                    return Err(MongoError::new(67, "CannotCreateIndex", format!("Error in specification {} :: caused by :: cannot mix \"partialFilterExpression\" and \"sparse\" options", fmt_doc(&shown))));
                }
            }
            // The collation: the one named, or the default of the collection; it ends the specification.
            let index_collation = match spec.get("collation") {
                None => default_collation.clone(),
                Some(v) if !matches!(v, Bson::Document(_)) => {
                    return Err(MongoError::new(14, "TypeMismatch", format!("Error in specification {} :: caused by :: The field 'collation' must be an object, but got {}", fmt_doc(spec), crate::update::type_name(v))));
                }
                Some(Bson::Document(d)) if d.is_empty() => {
                    return Err(MongoError::new(2, "BadValue", format!("Error in specification {} :: caused by :: The field 'collation' cannot be an empty object.", fmt_doc(spec))));
                }
                Some(v) => match crate::collation::for_index(v) {
                    Ok(c) => c,
                    Err(e) => {
                        let mut shown = spec.clone();
                        shown.insert("v", 2);
                        return Err(MongoError::new(2, "BadValue", format!("failed to add collation information to index spec for index creation: {} :: caused by :: {}", fmt_doc(&shown), e.msg)));
                    }
                },
            };
            if let Some(c) = &index_collation {
                if text_idx.is_some() {
                    let mut shown: Document = spec.iter().filter(|(k, _)| k.as_str() != "collation").map(|(k, v)| (k.clone(), v.clone())).collect();
                    if !shown.contains_key("v") {
                        shown.insert("v", 2);
                    }
                    shown.insert("collation", c.to_doc());
                    return Err(MongoError::new(67, "CannotCreateIndex", format!("Error in specification {} :: caused by :: Index type 'text' does not support collation: {}", fmt_doc(&shown), fmt_doc(&c.to_doc()))));
                }
                idx.insert("collation", c.to_doc());
            }
            let wanted = IndexDef::from_stored(&idx).ok_or_else(|| MongoError::new(67, "CannotCreateIndex", "bad index specification".into()))?;
            if let Some(ex) = indexes.iter().find(|i| i.name == name) {
                let no_hidden = |d: &Document| { let mut d = d.clone(); d.remove("hidden"); d };
                if ex.spec.get_document("key").map(|k| k == key).unwrap_or(false) && same_options(&no_hidden(&ex.spec), &no_hidden(&idx)) {
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
            // Two indexes are the same index when their key pattern and the options that decide which entries they hold agree:
            // sparse, unique, partialFilterExpression, collation, wildcardProjection.
            let identity = |d: &Document| -> Vec<(String, String)> {
                ["sparse", "unique", "partialFilterExpression", "wildcardProjection"]
                    .iter()
                    .filter_map(|f| d.get(*f).map(|v| (f.to_string(), v.clone())))
                    .filter(|(f, v)| !(matches!(f.as_str(), "sparse" | "unique") && !bool_arg(Some(v), false)))
                    .map(|(f, v)| (f, crate::update::fmt_bson(&v)))
                    .collect()
            };
            if let Some(ex) = indexes.iter().find(|i| i.spec.get_document("key").map(|k| k == key).unwrap_or(false) && crate::collation::same(&i.collation, &index_collation) && identity(&i.spec) == identity(&idx)) {
                let drop_hidden = |d: &Document| { let mut d = d.clone(); d.remove("hidden"); d };
                if same_options(&drop_hidden(&ex.spec), &drop_hidden(&idx)) {
                    return Err(MongoError::new(85, "IndexOptionsConflict", format!("Index already exists with a different name: {}", ex.name)));
                }
                return Err(MongoError::new(85, "IndexOptionsConflict", format!("An equivalent index already exists with a different name and options. Requested index: {}, existing index: {}", fmt_doc(&idx), fmt_doc(&ex.spec))));
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
        let after = series_base + indexes.iter().filter(|i| !crate::special_colls::is_internal(i)).count() as i32;
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
        if changed {
            let (uuid, specs) = {
                let mut g = self.dbs.lock(db).await;
                let ix = g.indexes(coll).await?;
                let specs: Vec<Document> = new_names.iter().filter_map(|n| ix.iter().find(|i| i.name == *n)).map(|i| i.spec.clone()).collect();
                (g.collection_uuid(coll).await?, specs)
            };
            if !existed {
                self.publish_create(db, coll, uuid.clone(), &Document::new(), false);
            }
            if building {
                self.publish_create_indexes(db, coll, uuid, &specs);
            } else {
                // an index built on an empty collection is a build of its own
                for s in &specs {
                    self.publish_create_indexes(db, coll, uuid.clone(), std::slice::from_ref(s));
                }
            }
        }
        let mut reply = doc! { "numIndexesBefore": before, "numIndexesAfter": after };
        if changed {
            reply.insert("createdCollectionAutomatically", !existed);
        }
        if let Some(q) = commit_quorum {
            reply.insert("commitQuorum", q);
        }
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
        // the state the change event reports as "before"
        let before = {
            let mut g = self.dbs.lock(db).await;
            if g.collections().await?.iter().any(|n| n == coll) {
                let mut opts = doc! { "uuid": Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: g.collection_uuid(coll).await? }) };
                if let Some(o) = g.coll_opts(coll).await? {
                    opts.extend(o.raw.iter().filter(|(k, _)| !matches!(k.as_str(), "capped" | "size" | "max" | "collation" | "storageEngine" | "indexOptionDefaults")).map(|(k, v)| (k.clone(), v.clone())));
                }
                let uuid = match opts.get("uuid") { Some(Bson::Binary(b)) => b.bytes.clone(), _ => Vec::new() };
                Some((opts, uuid))
            } else {
                None
            }
        };
        let reply = self.coll_mod_inner(db, cmd).await?;
        if let Some((opts, uuid)) = before {
            let mut desc = Document::new();
            for (k, v) in cmd.iter().skip(1) {
                if !k.starts_with('$') && !matches!(k.as_str(), "lsid" | "txnNumber" | "comment" | "writeConcern" | "readConcern" | "maxTimeMS" | "apiVersion" | "apiStrict" | "apiDeprecationErrors") {
                    desc.insert(k.clone(), v.clone());
                }
            }
            if !reply.keys().any(|k| k.ends_with("_old") || k.ends_with("_new")) {
                // an index change that changed nothing is not part of the event
                desc.remove("index");
            }
            let mut state = doc! { "collectionOptions": opts };
            let mut index_opts = Document::new();
            for (k, v) in &reply {
                if let Some(f) = k.strip_suffix("_old") {
                    index_opts.insert(f, v.clone());
                }
            }
            if !index_opts.is_empty() {
                state.insert("indexOptions", index_opts);
            }
            self.publish_ddl(crate::changes::EvSpec { uuid: Some(uuid), op_desc: Some(desc), state_before: Some(state), ..crate::changes::EvSpec::on(db, coll, "modify") }, true);
        }
        Ok(reply)
    }

    async fn coll_mod_inner(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "collMod")?;
        let mut g = self.dbs.lock(db).await;
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Err(MongoError::namespace_not_found("ns does not exist".to_string()));
        }
        for f in ["collation", "storageEngine", "indexOptionDefaults"] {
            if cmd.contains_key(f) {
                return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field 'collMod.{f}' is an unknown field.")));
            }
        }
        if cmd.contains_key("validator") || cmd.contains_key("validationLevel") || cmd.contains_key("validationAction") {
            let changes = crate::collopts::validation_options("collMod", cmd)?;
            let opts = g.coll_opts(coll).await?;
            let mut raw = opts.as_ref().map(|o| o.raw.clone()).unwrap_or_default();
            raw.extend(changes);
            // collMod stores both validation settings once it touches one of them (the other keeps its default)
            if !raw.contains_key("validationLevel") {
                raw.insert("validationLevel", "strict");
            }
            if !raw.contains_key("validationAction") {
                raw.insert("validationAction", "error");
            }
            let raw = crate::collopts::canonical(&raw);
            g.apply(vec![Op::Put(crate::collopts::meta_key(coll), raw.to_vec()?)]).await?;
            g.set_coll_opts(coll, Some(crate::collopts::CollOpts::parse(raw)));
        }
        if let Some(images) = crate::collopts::images_option("collMod", cmd)? {
            let opts = g.coll_opts(coll).await?;
            let mut raw = opts.as_ref().map(|o| o.raw.clone()).unwrap_or_default();
            if images.get_document("changeStreamPreAndPostImages").ok().and_then(|d| d.get_bool("enabled").ok()) == Some(true) {
                raw.extend(images);
            } else {
                raw.remove("changeStreamPreAndPostImages");
            }
            let raw = crate::collopts::canonical(&raw);
            g.apply(vec![Op::Put(crate::collopts::meta_key(coll), raw.to_vec()?)]).await?;
            g.set_coll_opts(coll, Some(crate::collopts::CollOpts::parse(raw)));
        }
        if cmd.contains_key("cappedSize") || cmd.contains_key("cappedMax") {
            self.coll_mod_capped(&mut g, coll, cmd).await?;
        }
        if cmd.contains_key("expireAfterSeconds") || cmd.contains_key("timeseries") {
            self.coll_mod_special(&mut g, db, coll, cmd).await?;
        }
        let Some(spec) = cmd.get("index") else { return Ok(ok()) };
        self.coll_mod_index(&mut g, db, coll, spec).await
    }

    pub(crate) async fn drop_indexes(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "dropIndexes")?;
        let which = cmd.get("index").ok_or_else(|| MongoError::new(40414, "IDLFailedToParse", "BSON field 'dropIndexes.index' is missing but a required field".into()))?;
        let mut g = self.dbs.lock(db).await;
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Err(MongoError::namespace_not_found(format!("ns not found {db}.{coll}")));
        }
        let mut indexes = self.load_indexes(&mut g, db, coll).await?;
        let kind = g.coll_opts(coll).await?;
        let base = if kind.as_ref().map(|k| k.series.is_some()).unwrap_or(false) { 0 } else { 1 };
        let n_was = base + indexes.iter().filter(|i| i.state == State::Ready && !crate::special_colls::is_internal(i)).count() as i32;
        let targets: Vec<usize> = match which {
            Bson::String(s) if s == "*" => (0..indexes.len()).filter(|i| indexes[*i].state == State::Ready && !crate::special_colls::is_internal(&indexes[*i])).collect(),
            Bson::String(s) => {
                if kind.as_ref().and_then(|k| k.clustered.as_deref()) == Some(s.as_str()) {
                    return Err(MongoError::new(5979800, "Location5979800", "It is illegal to drop the clusteredIndex".into()));
                }
                if s == crate::special_colls::TTL_NAME {
                    return Err(MongoError::new(27, "IndexNotFound", format!("index not found with name [{s}]")));
                }
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
        let dropped: Vec<Document> = targets.iter().map(|p| indexes[*p].spec.clone()).collect();
        for p in &targets {
            indexes[*p].state = State::Dropping;
        }
        let ttl_none = targets.is_empty();
        let uuid = if dropped.is_empty() { Vec::new() } else { g.collection_uuid(coll).await? };
        if !ttl_none {
            g.apply(vec![Entry::indexes_op(coll, &indexes)?]).await?;
        }
        drop(g);
        for spec in &dropped {
            self.publish_drop_index(db, coll, uuid.clone(), spec);
        }
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
        let inner = cmd.get_document("explain").map_err(|_| MongoError::new(40414, "IDLFailedToParse", "BSON field 'explain.explain' is missing but a required field".into()))?;
        let verbosity = cmd.get_str("verbosity").unwrap_or("allPlansExecution").to_string();
        let name = inner.keys().next().cloned().unwrap_or_default();
        let coll = collection_name(inner, &name)?;
        let ns = format!("{db}.{coll}");
        let mut absorbed = false;
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
                // the leading $match / $sort / $skip / $limit stages become the query plan
                let stages = inner.get_array("pipeline").cloned().unwrap_or_default();
                let mut it = stages.iter().filter_map(|s| s.as_document()).peekable();
                let first = if matches!(it.peek(), Some(d) if d.contains_key("$match")) { it.next().and_then(|d| d.get_document("$match").ok().cloned()).unwrap_or_default() } else { Document::new() };
                let mut sort = vec![];
                if matches!(it.peek(), Some(d) if d.contains_key("$sort")) {
                    sort = it.next().and_then(|d| d.get_document("$sort").ok()).map(parse_sort).transpose()?.unwrap_or_default();
                }
                let mut skip = 0;
                if matches!(it.peek(), Some(d) if d.contains_key("$skip")) {
                    skip = int_arg(it.next().and_then(|d| d.get("$skip"))).unwrap_or(0);
                }
                let mut limit = 0;
                if matches!(it.peek(), Some(d) if d.contains_key("$limit")) {
                    limit = int_arg(it.next().and_then(|d| d.get("$limit"))).unwrap_or(0);
                }
                absorbed = it.peek().is_none();
                (first, sort, limit, skip, inner.get("hint").cloned(), None)
            }
            "update" | "delete" | "findAndModify" => {
                let q = inner.get_array("updates").or_else(|_| inner.get_array("deletes")).ok().and_then(|a| a.first().and_then(|s| s.as_document()).and_then(|s| s.get_document("q").ok()).cloned()).or_else(|| inner.get_document("query").ok().cloned()).unwrap_or_default();
                (q, vec![], 0, 0, None, None)
            }
            other => return Err(MongoError::command_not_found(other)),
        };
        let mut g = self.dbs.lock(db).await;
        let exists = g.collections().await?.iter().any(|n| n == coll);
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
        if let Some(c) = crate::collation::current() {
            // (after parsedQuery, as mongod lists it)
            let mut with: Document = Document::new();
            for (k, v) in qp {
                let after_parsed = k == "parsedQuery";
                with.insert(k, v);
                if after_parsed {
                    with.insert("collation", c.to_doc());
                }
            }
            qp = with;
        }
        if !exists {
            // nothing to plan: mongod answers EOF and has no query hash
            qp.remove("queryHash");
            qp.remove("planCacheKey");
            qp.insert("winningPlan", doc! { "stage": "EOF" });
        }
        if name == "aggregate" {
            let mut with = Document::new();
            for (k, v) in qp {
                let after_key = k == "planCacheKey" || (k == "parsedQuery" && !exists);
                with.insert(k, v);
                if after_key {
                    with.insert("optimizationTimeMillis", 0);
                    with.insert("optimizedPipeline", absorbed);
                }
            }
            qp = with;
        }
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
        out.insert("serverInfo", doc! { "host": self.host_port_name(), "port": self.listen_port(), "version": crate::diag::VERSION, "gitVersion": "none" });
        out.insert("serverParameters", doc! { "internalQueryFacetBufferSizeBytes": 104857600_i32 });
        out.insert("ok", 1.0);
        Ok(out)
    }

    // ---- validate, stats -------------------------------------------------------------------

    /// `validate`: every document has exactly its entries in every ready/building index and no entry points to a
    /// missing document.
    pub(crate) async fn validate(&self, db: &str, cmd: &Document) -> R<Document> {
        if let Some(v) = cmd.get("validate").filter(|v| !matches!(v, Bson::String(_))) {
            return Err(MongoError::new(73, "InvalidNamespace", format!("collection name has invalid type {}", crate::update::type_name(v))));
        }
        let coll = collection_name(cmd, "validate")?;
        let truthy = |k: &str| match cmd.get(k) {
            None | Some(Bson::Null) | Some(Bson::Boolean(false)) | Some(Bson::Undefined) => false,
            Some(v) => num_opt(v).map(|n| n != 0.0).unwrap_or(true),
        };
        let (metadata, background, full) = (truthy("metadata"), truthy("background"), truthy("full"));
        if metadata && ["full", "background", "checkBSONConformance", "repair", "fixMultikey", "enforceFastCount"].iter().any(|k| truthy(k)) {
            return Err(MongoError::new(72, "InvalidOptions", "Running the validate command with { metadata: true } is not supported with any other options".into()));
        }
        if background && full {
            return Err(MongoError::new(72, "InvalidOptions", "Running the validate command with both { background: true } and { full: true } is not supported.".into()));
        }
        let mut g = self.dbs.lock(db).await;
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Err(MongoError::namespace_not_found(format!("Collection '{db}.{coll}' does not exist to validate.")));
        }
        let uuid = Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: g.collection_uuid(coll).await? });
        if metadata || background {
            let mut warnings: Vec<Bson> = Vec::new();
            if background {
                warnings.push(Bson::String("Collection validation with {background: true} validates the latest checkpoint (data in a snapshot written to disk in a consistent way across all data files). During this validation, some tables have not yet been checkpointed.".into()));
            }
            return Ok(doc! {
                "ns": format!("{db}.{coll}"), "uuid": uuid, "valid": true, "repaired": false, "warnings": warnings, "errors": [],
                "extraIndexEntries": [], "missingIndexEntries": [], "corruptRecords": [], "ok": 1.0,
            });
        }
        let indexes = g.indexes(coll).await?;
        let dp = doc_prefix(coll);
        let rows = g.scan(&dp).await?;
        let mut errors: Vec<Bson> = Vec::new();
        let mut keys_per_index = doc! { "_id_": rows.len() as i32 };
        let mut bad_indexes: HashSet<String> = HashSet::new();
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
            keys_per_index.insert(idx.name.clone(), stored.len() as i32);
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
                        bad_indexes.insert(idx.name.clone());
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
                    bad_indexes.insert(idx.name.clone());
                    if errors.len() < 20 {
                        errors.push(Bson::String(format!("index {} has an entry that {} (extra key)", idx.name, if doc_exists { "does not match its document" } else { "points to a missing document" })));
                    }
                }
            }
        }
        ids.clear();
        let mut details = doc! { "_id_": { "valid": true } };
        for idx in indexes.iter().filter(|i| i.has_entries() && i.state != State::Dropping) {
            details.insert(idx.name.clone(), doc! { "valid": !bad_indexes.contains(&idx.name) });
        }
        let validator = g.validator_of(coll).filter(|v| v.level != crate::validation::Level::Off);
        let mut non_compliant = 0_i32;
        if let Some(v) = &validator {
            for (_, d) in &docs {
                if !crate::validation::satisfies(d, v)? {
                    non_compliant += 1;
                }
            }
        }
        let mut warnings: Vec<Bson> = if non_compliant > 0 { vec![Bson::String("Detected one or more documents not compliant with the collection's schema. Check logs for log id 5363500.".into())] } else { vec![] };
        if full && !docs.is_empty() {
            // mongod's full validation of a collection with documents cannot read the checkpointed tables and says so for the collection
            // and its _id index (the table names are the storage engine's identifiers)
            static PROC_ID: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
            let proc_id = *PROC_ID.get_or_init(|| rand::random::<i64>() >> 2);
            let n = 100 + (coll.bytes().fold(7u32, |h, b| h.wrapping_mul(31).wrapping_add(b as u32)) % 9000) as i64;
            for (kind, num) in [("collection", n), ("index", n + 1)] {
                let w = format!("Could not complete validation of table:{kind}-{num}-{proc_id}. This is a transient issue as the collection was actively in use by other operations.");
                if kind == "index" {
                    details.insert("_id_", doc! { "valid": true, "warnings": [w.clone()] });
                }
                warnings.push(Bson::String(w));
            }
        }
        let _ = (missing_total, extra_total);
        Ok(doc! {
            "ns": format!("{db}.{coll}"),
            "uuid": uuid,
            "nInvalidDocuments": 0,
            "nNonCompliantDocuments": non_compliant,
            "nrecords": docs.len() as i32,
            "nIndexes": 1 + indexes.iter().filter(|i| i.state == State::Ready).count() as i32,
            "keysPerIndex": keys_per_index,
            "indexDetails": details,
            "valid": errors.is_empty(),
            "repaired": false,
            "warnings": warnings,
            "errors": errors,
            "extraIndexEntries": [],
            "missingIndexEntries": [],
            "corruptRecords": [],
            "ok": 1.0,
        })
    }

    /// Document count, data size and per-index sizes of a collection (O(n): read from the store).
    pub(crate) async fn storage_stats(&self, g: &mut Entry, db: &str, coll: &str) -> R<Document> {
        let indexes = g.indexes(coll).await?;
        let rows = g.scan(&doc_prefix(coll)).await?;
        let size: usize = rows.iter().map(|(_, v)| crate::natural::bson_part(v).len()).sum();
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
        let cap = g.coll_opts(coll).await?.and_then(|o| o.cap.clone());
        let mut out = doc! {
            "size": crate::capped::n32(size as i64),
            "count": rows.len() as i32,
            "avgObjSize": crate::capped::n32(if count > 0 { size as i64 / count } else { 0 }),
            "numOrphanDocs": 0,
            "storageSize": size as i64,
            "freeStorageSize": 0_i64,
            "capped": cap.is_some(),
        };
        if let Some(c) = &cap {
            out.insert("max", crate::capped::n32(c.max.unwrap_or(0)));
            out.insert("maxSize", crate::capped::n32(c.size));
            out.insert("storageSize", ((c.size + 4095) / 4096 * 4096).max(4096));
        }
        out.extend(doc! {
            "nindexes": n_idx,
            "indexDetails": {},
            "indexBuilds": [],
            "totalIndexSize": total,
            "indexSizes": sizes,
            "totalSize": size as i64 + total,
            "scaleFactor": 1,
        });
        Ok(out)
    }

    pub(crate) async fn coll_stats(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "collStats")?;
        let scale = crate::stats_scale::scale_arg("collStats.scale", cmd.get("scale"), false)?;
        let mut g = self.dbs.lock(db).await;
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Err(MongoError::namespace_not_found(format!("Collection [{db}.{coll}] not found.")));
        }
        let mut st = self.storage_stats(&mut g, db, coll).await?;
        crate::stats_scale::scale_collection(&mut st, scale);
        let mut out = doc! { "ns": format!("{db}.{coll}") };
        out.extend(std::mem::take(&mut st));
        out.insert("ok", 1.0);
        Ok(out)
    }

    pub(crate) async fn db_stats(&self, db: &str, cmd: &Document) -> R<Document> {
        let scale = crate::stats_scale::scale_arg("dbStats.scale", cmd.get("scale"), true)?;
        let views = self.view_count(db).await?;
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
        let mut out = doc! {
            "db": db, "collections": names.len() as i64, "views": views, "objects": objects, "avgObjSize": if objects > 0 { data as f64 / objects as f64 } else { 0.0 },
            "dataSize": data, "storageSize": data, "indexes": indexes_n, "indexSize": index_size, "totalSize": data + index_size,
        };
        if bool_arg(cmd.get("freeStorage"), false) {
            out.extend(doc! { "freeStorageSize": 0_i64, "indexFreeStorageSize": 0_i64, "totalFreeStorageSize": 0_i64 });
        }
        out.extend(doc! { "scaleFactor": 1_i64, "fsUsedSize": 0.0, "fsTotalSize": 0.0 });
        crate::stats_scale::scale_database(&mut out, scale);
        out.insert("ok", 1.0);
        Ok(out)
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
    let mut stage = doc! {
        "stage": "IXSCAN",
        "keyPattern": idx.key_pattern(),
        "indexName": idx.name.clone(),
    };
    if let Some(c) = &idx.collation {
        stage.insert("collation", c.to_doc());
    }
    stage.extend(doc! {
        "isMultiKey": idx.multikey,
        "multiKeyPaths": mk_paths,
        "isUnique": idx.unique,
        "isSparse": idx.sparse,
        "isPartial": idx.partial.is_some(),
        "indexVersion": 2,
        "direction": if s.reverse { "backward" } else { "forward" },
        "indexBounds": bounds,
    });
    stage
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

