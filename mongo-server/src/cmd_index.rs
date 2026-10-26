//! Index and collection management: createIndexes (restartable backfill), dropIndexes, listIndexes,
//! drop, explain, validate, collStats / dbStats.

use crate::cmd::{bool_arg, collection_name, fake_uuid, int_arg, ok, Server, R};
use crate::coll::{decode, find_docs, plan_for, resolve_hint, FindOpts, Hint};
use crate::cmd_data::ttl_ctx;
use crate::dbs::{Dbs, Entry};
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
        return Ok(if last { Step::Done } else { Step::More });
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
            ops.push(Op::Put(entry_key(&prefix, &c, &id_enc), Vec::new()));
        }
    }
    if let Some(e) = failure {
        // Abort the build: the index becomes a Dropping one and its entries are removed by the next steps.
        indexes[pos].state = State::Dropping;
        indexes[pos].cursor = None;
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
    Ok(if done { Step::Done } else { Step::More })
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

impl Server {
    /// Load a collection's indexes; unfinished builds / drops (after a crash or restart) are resumed in the background.
    pub(crate) async fn load_indexes(&self, g: &mut Entry, db: &str, coll: &str) -> R<Vec<IndexDef>> {
        let indexes = g.indexes(coll).await?;
        if indexes.iter().any(|i| i.state != State::Ready) {
            self.resume_builds(db, coll);
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
        for prefix in [doc_prefix(coll), idx_collection_prefix(coll)] {
            for (k, _) in g.scan(&prefix).await? {
                ops.push(Op::Delete(k.to_vec()));
            }
        }
        ops.push(Op::Delete(index_key(coll)));
        names.remove(pos);
        ops.push(Entry::collections_op(&names)?);
        g.apply(ops).await?;
        Ok(doc! { "nIndexesWas": n_indexes, "ns": format!("{db}.{coll}"), "ok": 1.0 })
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
        for spec in specs {
            let spec = spec.as_document().ok_or_else(|| MongoError::new(14, "TypeMismatch", "Each element of the 'indexes' array must be an object".into()))?;
            let key = spec.get_document("key").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'indexes.key' is missing but a required field".into()))?;
            if key.is_empty() {
                return Err(MongoError::new(67, "CannotCreateIndex", "Index keys cannot be empty.".into()));
            }
            let name = spec.get_str("name").map(str::to_string).unwrap_or_else(|_| spec_name(key));
            if name == "_id_" || (key.len() == 1 && key.get("_id").is_some()) {
                continue; // the implicit _id index
            }
            // Normalised spec: the user's options in MongoDB's order.
            let mut idx = doc! { "v": 2, "key": key.clone(), "name": name.clone() };
            for (k, v) in spec {
                if !matches!(k.as_str(), "key" | "name" | "v") {
                    idx.insert(k.clone(), v.clone());
                }
            }
            for opt in ["unique", "sparse"] {
                if let Some(v) = idx.get(opt) {
                    if !matches!(v, Bson::Boolean(_)) && num_opt(v).is_none() {
                        return Err(MongoError::new(14, "TypeMismatch", format!("The field '{opt}' must be a boolean or number")));
                    }
                }
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
                    continue;
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
                let r = run_builds(&self.dbs, db, coll).await;
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
        }
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
            .filter(|(k, _)| !matches!(k.as_str(), "v" | "name"))
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

