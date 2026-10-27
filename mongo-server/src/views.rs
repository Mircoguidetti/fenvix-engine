//! Views: read-only collections defined by `viewOn` and a `pipeline`. A view is a document of the real `system.views`
//! collection (`_id` = "db.view", `viewOn`, `pipeline`, optional `collation`). `find`, `count`, `distinct` and `aggregate`
//! on a view run an aggregation on the source with the pipeline of the view in front; every other command that needs a
//! collection answers with CommandNotSupportedOnView.

use crate::cmd::{ok, Server, R};
use crate::error::MongoError;
use crate::txn::Txn;
use bson::{doc, Bson, Document};
use std::collections::VecDeque;

const VIEWS: &str = "system.views";
const MAX_DEPTH: usize = 20;
/// First stages that read the source collection itself and cannot be validated against no input.
const SOURCE_STAGES: [&str; 5] = ["$collStats", "$indexStats", "$geoNear", "$planCacheStats", "$search"];

#[derive(Clone)]
pub(crate) struct ViewDef {
    pub view_on: String,
    pub pipeline: Vec<Bson>,
    pub collation: Option<Document>,
}

impl ViewDef {
    fn from_doc(d: &Document) -> Self {
        ViewDef {
            view_on: d.get_str("viewOn").unwrap_or_default().to_string(),
            pipeline: d.get_array("pipeline").cloned().unwrap_or_default(),
            collation: d.get_document("collation").ok().cloned(),
        }
    }

    /// The options of `listCollections`.
    pub(crate) fn options(&self) -> Document {
        let mut o = doc! { "viewOn": self.view_on.clone(), "pipeline": self.pipeline.clone() };
        if let Some(c) = &self.collation {
            o.insert("collation", c.clone());
        }
        o
    }
}

/// A view followed down to the collection it is built on.
pub(crate) struct Resolved {
    pub source: String,
    /// The pipelines of the nested views, the innermost first.
    pub pipeline: Vec<Bson>,
    pub collation: Option<Document>,
}

fn is_view_error(db: &str, coll: &str) -> MongoError {
    MongoError::new(166, "CommandNotSupportedOnView", format!("Namespace {db}.{coll} is a view, not a collection"))
}

fn match_stage(filter: &Document) -> Bson {
    Bson::Document(doc! { "$match": filter.clone() })
}

impl Server {
    /// The definition of the view `db.coll`, if there is one (a collection of that name wins).
    pub(crate) async fn view_def(&self, db: &str, coll: &str) -> R<Option<ViewDef>> {
        Ok(self.view_lookup(db, coll).await?.0)
    }

    /// The view `db.coll`, and whether the name was looked up in a view catalog that exists but has no such view or collection.
    async fn view_lookup(&self, db: &str, coll: &str) -> R<(Option<ViewDef>, bool)> {
        let g = self.dbs.read(db).await?;
        let names = g.collections().await?;
        if !names.iter().any(|n| n == VIEWS) || names.iter().any(|n| n == coll) {
            return Ok((None, false));
        }
        let v = g.get_doc(VIEWS, &Bson::String(format!("{db}.{coll}"))).await?.map(|d| ViewDef::from_doc(&d));
        let missing = v.is_none();
        Ok((v, missing))
    }

    /// Every view of the database: (name, definition).
    pub(crate) async fn list_views(&self, db: &str) -> R<Vec<(String, ViewDef)>> {
        let g = self.dbs.read(db).await?;
        if !g.collections().await?.iter().any(|n| n == VIEWS) {
            return Ok(Vec::new());
        }
        let prefix = format!("{db}.");
        Ok(g.scan_collection(VIEWS).await?.iter().filter_map(|d| d.get_str("_id").ok().and_then(|id| id.strip_prefix(prefix.as_str())).map(|n| (n.to_string(), ViewDef::from_doc(d)))).collect())
    }

    pub(crate) async fn resolve_view(&self, db: &str, coll: &str) -> R<Option<Resolved>> {
        let Some(top) = self.view_def(db, coll).await? else { return Ok(None) };
        let collation = top.collation.clone();
        let mut stages = vec![top.pipeline.clone()];
        let mut source = top.view_on.clone();
        while let Some(v) = self.view_def(db, &source).await? {
            if stages.len() >= MAX_DEPTH {
                break;
            }
            stages.push(v.pipeline);
            source = v.view_on;
        }
        if stages.len() >= MAX_DEPTH {
            return Err(MongoError::new(165, "ViewDepthLimitExceeded", format!("View depth too deep or view cycle detected; maximum depth is {MAX_DEPTH}")));
        }
        stages.reverse();
        Ok(Some(Resolved { source, pipeline: stages.concat(), collation }))
    }

    /// Commands on a name that is a view. `Ok(None)`: not a view, run the command as usual.
    pub(crate) async fn view_route(&self, db: &str, name: &str, cmd: &Document, tx: &mut Option<Txn>) -> R<Option<Document>> {
        let target = match name {
            "find" | "count" | "distinct" | "aggregate" | "insert" | "update" | "delete" | "findAndModify" | "findandmodify" | "collStats" | "validate" | "listIndexes" | "createIndexes" | "dropIndexes" | "collMod" | "drop"
            | "convertToCapped" | "cloneCollectionAsCapped" | "mapReduce" | "mapreduce" => cmd.get_str(name).ok(),
            "explain" => cmd.get_document("explain").ok().and_then(|e| e.iter().next().and_then(|(k, v)| v.as_str().filter(|_| matches!(k.as_str(), "find" | "count" | "distinct" | "aggregate")))),
            _ => None,
        };
        let Some(coll) = target else { return Ok(None) };
        if coll.starts_with("system.") && coll != VIEWS && !coll.starts_with("system.views") {
            return Ok(None);
        }
        let (def, missing) = self.view_lookup(db, coll).await?;
        let Some(def) = def else {
            if missing && name == "find" {
                // mongod answers a find on a namespace that is neither a collection nor a view like a listing: id and ns first
                let mut out = Box::pin(self.find(db, cmd, tx)).await?;
                if let Ok(cur) = out.get_document_mut("cursor") {
                    let (batch, id, ns) = (cur.remove("firstBatch"), cur.remove("id"), cur.remove("ns"));
                    cur.extend([("id", id), ("ns", ns), ("firstBatch", batch)].into_iter().filter_map(|(k, v)| v.map(|v| (k.to_string(), v))));
                }
                return Ok(Some(out));
            }
            return Ok(None);
        };
        let ns = format!("{db}.{coll}");
        let explain_agg;
        let (name, cmd) = if name == "aggregate" && matches!(cmd.get("explain"), Some(Bson::Boolean(true))) {
            // `aggregate` with `explain: true` is the explain of the aggregate command
            let mut inner = Document::new();
            for (k, v) in cmd {
                if k != "explain" && !k.starts_with('$') && !matches!(k.as_str(), "lsid" | "txnNumber") {
                    inner.insert(k.clone(), v.clone());
                }
            }
            if !inner.contains_key("cursor") {
                inner.insert("cursor", doc! {});
            }
            explain_agg = doc! { "explain": inner, "verbosity": "queryPlanner" };
            ("explain", &explain_agg)
        } else {
            (name, cmd)
        };
        match name {
            "find" | "count" | "distinct" | "aggregate" => {
                let Some(r) = self.resolve_view(db, coll).await? else { return Ok(None) };
                let r = Box::pin(self.view_read(db, coll, name, cmd, r, tx)).await?;
                Ok(Some(r))
            }
            "explain" => {
                let inner = cmd.get_document("explain").unwrap();
                let (iname, _) = inner.iter().next().unwrap();
                let Some(r) = self.resolve_view(db, coll).await? else { return Ok(None) };
                let agg = self.view_as_aggregate(db, coll, iname, inner, &r)?;
                let mut c = cmd.clone();
                c.insert("explain", agg);
                let mut out = Box::pin(self.explain(db, &c)).await?;
                if let Ok(co) = out.get_document_mut("command") {
                    co.insert("aggregate", coll);
                }
                Ok(Some(out))
            }
            "insert" | "update" | "delete" => {
                let key = match name {
                    "insert" => "documents",
                    "update" => "updates",
                    _ => "deletes",
                };
                let n = cmd.get_array(key).map(|a| a.len()).unwrap_or(1).max(1);
                let count = if cmd.get_bool("ordered").unwrap_or(true) { 1 } else { n };
                let errs: Vec<Bson> = (0..count).map(|i| Bson::Document(doc! { "index": i as i32, "code": 166, "errmsg": format!("Namespace {ns} is a view, not a collection") })).collect();
                let mut d = doc! { "n": 0, "writeErrors": errs };
                if name == "update" {
                    d.insert("nModified", 0);
                }
                d.insert("ok", 1.0);
                Ok(Some(d))
            }
            "validate" => Err(MongoError::new(166, "CommandNotSupportedOnView", "Cannot validate a view".into())),
            "drop" => {
                self.delete(db, &doc! { "delete": VIEWS, "deletes": [{ "q": { "_id": ns.clone() }, "limit": 1 }] }, &mut None).await?;
                Ok(Some(doc! { "ns": ns, "ok": 1.0 }))
            }
            "collMod" => Ok(Some(self.coll_mod_view(db, coll, cmd, def).await?)),
            _ => Err(is_view_error(db, coll)),
        }
    }

    /// `find`, `count`, `distinct` and `aggregate` on a view.
    async fn view_read(&self, db: &str, coll: &str, name: &str, cmd: &Document, r: Resolved, tx: &mut Option<Txn>) -> R<Document> {
        let ns = format!("{db}.{coll}");
        if let Some(v) = cmd.get("collation") {
            if crate::collation::of_create(v)?.map(|c| c.to_doc()) != r.collation {
                return Err(MongoError::new(167, "OptionNotSupportedOnView", "Cannot override a view's default collation".into()));
            }
        }
        // the pipeline runs with the collation of the view, the simple one when it has none (not that of the source)
        let coll_doc = Some(r.collation.clone().unwrap_or_else(|| doc! { "locale": "simple" }));
        let col = Some(crate::collation::of_create(&Bson::Document(coll_doc.clone().unwrap()))?);
        crate::collation::scope(col, async {
            match name {
                "aggregate" => {
                    let mut pipe = r.pipeline.clone();
                    pipe.extend(cmd.get_array("pipeline").cloned().unwrap_or_default());
                    let mut c = cmd.clone();
                    c.insert("aggregate", r.source.clone());
                    c.insert("pipeline", pipe);
                    if let Some(cd) = coll_doc {
                        c.insert("collation", cd);
                    }
                    let mut out = Box::pin(self.aggregate(db, &c, tx)).await?;
                    if let Ok(cur) = out.get_document_mut("cursor") {
                        cur.insert("ns", ns);
                    }
                    Ok(out)
                }
                "find" => {
                    let filter = cmd.get_document("filter").cloned().unwrap_or_default();
                    crate::query::probe_filter(&filter)?;
                    let skip = crate::cmd::int_arg(cmd.get("skip")).unwrap_or(0).max(0);
                    let limit = crate::cmd::int_arg(cmd.get("limit")).unwrap_or(0);
                    let mut pipe = r.pipeline.clone();
                    if !filter.is_empty() {
                        pipe.push(match_stage(&filter));
                    }
                    if let Ok(s) = cmd.get_document("sort") {
                        if !s.is_empty() {
                            crate::query::parse_sort(s)?;
                            if !(s.len() == 1 && s.contains_key("$natural")) {
                                pipe.push(Bson::Document(doc! { "$sort": s.clone() }));
                            }
                        }
                    }
                    if skip > 0 {
                        pipe.push(Bson::Document(doc! { "$skip": skip }));
                    }
                    if limit != 0 {
                        pipe.push(Bson::Document(doc! { "$limit": limit.abs() }));
                    }
                    let projection = match cmd.get_document("projection") {
                        Ok(p) => crate::projfind::parse(p, &filter)?,
                        Err(_) => None,
                    };
                    let mut docs = self.view_pipeline_docs(db, &r.source, pipe, coll_doc, tx).await?;
                    if let Some(p) = &projection {
                        for d in docs.iter_mut() {
                            *d = p.apply(d)?;
                        }
                    }
                    let batch = crate::cmd::int_arg(cmd.get("batchSize")).map(|b| b.max(0) as usize);
                    let single = crate::cmd::bool_arg(cmd.get("singleBatch"), false) || limit < 0;
                    let limited = limit != 0 && docs.len() >= limit.unsigned_abs() as usize;
                    Ok(self.cursor_reply_open(ns, VecDeque::from(docs), batch, single, limited))
                }
                "count" => {
                    let filter = cmd.get_document("query").cloned().unwrap_or_default();
                    let skip = crate::cmd::int_arg(cmd.get("skip")).unwrap_or(0).max(0);
                    let limit = crate::cmd::int_arg(cmd.get("limit")).unwrap_or(0).abs();
                    let mut pipe = r.pipeline.clone();
                    if !filter.is_empty() {
                        pipe.push(match_stage(&filter));
                    }
                    if skip > 0 {
                        pipe.push(Bson::Document(doc! { "$skip": skip }));
                    }
                    if limit > 0 {
                        pipe.push(Bson::Document(doc! { "$limit": limit }));
                    }
                    pipe.push(Bson::Document(doc! { "$count": "n" }));
                    let docs = self.view_pipeline_docs(db, &r.source, pipe, coll_doc, tx).await?;
                    let n = docs.first().and_then(|d| d.get_i32("n").ok()).unwrap_or(0);
                    Ok(doc! { "n": n, "ok": 1 })
                }
                _ => {
                    let key = cmd.get_str("key").map_err(|_| MongoError::new(40414, "IDLFailedToParse", "BSON field 'distinct.key' is missing but a required field".into()))?;
                    let filter = cmd.get_document("query").cloned().unwrap_or_default();
                    let mut pipe = r.pipeline.clone();
                    if !filter.is_empty() {
                        pipe.push(match_stage(&filter));
                    }
                    let docs = self.view_pipeline_docs(db, &r.source, pipe, coll_doc, tx).await?;
                    Ok(doc! { "values": crate::cmd_data::distinct_values(&docs, key), "ok": 1 })
                }
            }
        })
        .await
    }

    /// The output of `pipeline` run on the collection `source`.
    async fn view_pipeline_docs(&self, db: &str, source: &str, pipeline: Vec<Bson>, collation: Option<Document>, tx: &mut Option<Txn>) -> R<Vec<Document>> {
        let mut c = doc! { "aggregate": source, "pipeline": pipeline, "cursor": { "batchSize": i32::MAX } };
        if let Some(cd) = collation {
            c.insert("collation", cd);
        }
        let col = match c.get("collation") {
            Some(v) => Some(crate::collation::of_create(v)?),
            None => None,
        };
        let out = crate::collation::scope(col, Box::pin(self.aggregate(db, &c, tx))).await?;
        Ok(out.get_document("cursor").ok().and_then(|c| c.get_array("firstBatch").ok()).map(|a| a.iter().filter_map(|d| d.as_document().cloned()).collect()).unwrap_or_default())
    }

    /// The documents of the view `from`, for `$lookup` / `$unionWith`.
    pub(crate) async fn view_docs(&self, db: &str, from: &str, tx: &mut Option<Txn>) -> R<Option<Vec<Document>>> {
        let Some(r) = self.resolve_view(db, from).await? else { return Ok(None) };
        let col = Some(r.collation.clone().unwrap_or_else(|| doc! { "locale": "simple" }));
        Ok(Some(self.view_pipeline_docs(db, &r.source, r.pipeline, col, tx).await?))
    }

    /// The aggregate command that an explained `find` / `count` / `distinct` / `aggregate` of a view amounts to.
    fn view_as_aggregate(&self, _db: &str, _coll: &str, name: &str, inner: &Document, r: &Resolved) -> R<Document> {
        let mut pipe = r.pipeline.clone();
        match name {
            "aggregate" => pipe.extend(inner.get_array("pipeline").cloned().unwrap_or_default()),
            "find" | "count" => {
                let filter = inner.get_document("filter").or_else(|_| inner.get_document("query")).cloned().unwrap_or_default();
                if !filter.is_empty() {
                    pipe.push(match_stage(&filter));
                }
                if let Ok(s) = inner.get_document("sort") {
                    if !s.is_empty() {
                        pipe.push(Bson::Document(doc! { "$sort": s.clone() }));
                    }
                }
                if let Some(s) = crate::cmd::int_arg(inner.get("skip")).filter(|s| *s > 0) {
                    pipe.push(Bson::Document(doc! { "$skip": s }));
                }
                if let Some(l) = crate::cmd::int_arg(inner.get("limit")).filter(|l| *l != 0) {
                    pipe.push(Bson::Document(doc! { "$limit": l.abs() }));
                }
                if let Ok(p) = inner.get_document("projection") {
                    pipe.push(Bson::Document(doc! { "$project": p.clone() }));
                }
                if name == "count" {
                    pipe.push(Bson::Document(doc! { "$group": { "_id": Bson::Null, "n": { "$sum": 1 } } }));
                }
            }
            _ => {
                let filter = inner.get_document("query").cloned().unwrap_or_default();
                if !filter.is_empty() {
                    pipe.push(match_stage(&filter));
                }
            }
        }
        let mut c = doc! { "aggregate": r.source.clone(), "pipeline": pipe, "cursor": {} };
        c.insert("collation", r.collation.clone().unwrap_or_else(|| doc! { "locale": "simple" }));
        Ok(c)
    }

    /// Reject a pipeline that cannot define a view.
    fn check_view_pipeline(&self, db: &str, ns: &str, pipeline: &[Bson]) -> R<()> {
        for (i, s) in pipeline.iter().enumerate() {
            let Some((n, arg)) = s.as_document().and_then(|d| d.iter().next()) else { continue };
            if n == "$out" || n == "$merge" {
                if n == "$out" {
                    crate::xdb::parse_out(arg, db)?;
                } else {
                    crate::xdb::parse_merge(arg, db)?;
                }
                return Err(MongoError::new(167, "OptionNotSupportedOnView", format!("Invalid pipeline for view {ns} :: caused by :: The aggregation stage {n} in location {i} of the pipeline cannot be used in the view definition of {ns} because it writes to disk")));
            }
        }
        let body: Vec<Bson> = pipeline.iter().enumerate().filter(|(i, s)| !(*i == 0 && s.as_document().and_then(|d| d.keys().next()).map(|k| SOURCE_STAGES.contains(&k.as_str())).unwrap_or(false))).map(|(_, s)| s.clone()).collect();
        let mut foreign = crate::agg::Foreign::new();
        for from in crate::agg::lookup_collections(&body) {
            foreign.insert(format!("\u{0}geo:{from}"), Vec::new());
            foreign.insert(from, Vec::new());
        }
        match crate::agg::run_pipeline(Vec::new(), &body, &foreign) {
            Ok(_) => Ok(()),
            Err(e) if e.msg.contains("$where is not allowed in this context") => Err(MongoError::new(e.code, e.name, format!("Invalid pipeline for view {ns} :: caused by :: {}", e.msg))),
            Err(e) => Err(e),
        }
    }

    /// A view that would be defined on `view_on` must not lead back to `new`, nor be nested too deeply.
    async fn check_view_cycle(&self, db: &str, new: &str, view_on: &str) -> R<()> {
        let mut walk = vec![format!("{db}.{new}")];
        let mut cur = view_on.to_string();
        let mut depth = 1;
        loop {
            walk.push(format!("{db}.{cur}"));
            if cur == new {
                let mut chain = vec![format!("{db}.{new}")];
                chain.extend(walk);
                return Err(MongoError::new(5, "GraphContainsCycle", format!("View cycle detected: {}", chain.join(" => "))));
            }
            match self.view_def(db, &cur).await? {
                Some(v) => cur = v.view_on,
                None => return Ok(()),
            }
            depth += 1;
            if depth > MAX_DEPTH {
                return Err(MongoError::new(165, "ViewDepthLimitExceeded", format!("View depth limit exceeded; maximum depth is {MAX_DEPTH}")));
            }
        }
    }

    /// `create` with `viewOn`. `Ok(None)`: a plain collection is created.
    pub(crate) async fn create_view(&self, db: &str, coll: &str, cmd: &Document) -> R<Option<Document>> {
        let ns = format!("{db}.{coll}");
        let existing = self.view_def(db, coll).await?;
        if !cmd.contains_key("viewOn") && !cmd.contains_key("pipeline") {
            if let Some(e) = existing {
                return Err(MongoError::namespace_exists(format!("namespace {ns} already exists, but is a view on {db}.{} rather than {db}.", e.view_on)));
            }
            return Ok(None);
        }
        let wrong = |f: &str, v: &Bson, want: &str| MongoError::type_mismatch(format!("BSON field 'create.{f}' is the wrong type '{}', expected type '{want}'", crate::update::type_name(v)));
        if let Some(v) = cmd.get("viewOn") {
            if v.as_str().is_none() {
                return Err(wrong("viewOn", v, "string"));
            }
        }
        if let Some(v) = cmd.get("pipeline") {
            if !matches!(v, Bson::Array(_)) {
                return Err(wrong("pipeline", v, "array"));
            }
        }
        let Some(view_on) = cmd.get_str("viewOn").ok() else {
            return Err(MongoError::new(72, "InvalidOptions", "'pipeline' requires 'viewOn' to also be specified".into()));
        };
        let pipeline = cmd.get_array("pipeline").cloned().unwrap_or_default();
        crate::capped::parse_create(cmd)?;
        let collation = match cmd.get("collation") {
            Some(v) => crate::collation::of_create(v)?.map(|c| c.to_doc()),
            None => None,
        };
        {
            let g = self.dbs.read(db).await?;
            if g.collections().await?.iter().any(|n| n == coll) {
                let mut shown = doc! { "uuid": "<uuid>" };
                let o = g.coll_opts(coll).await?.map(|o| o.raw.clone()).unwrap_or_default();
                shown.extend(o);
                let body = crate::update::fmt_doc(&shown).replacen("uuid: \"<uuid>\"", &format!("uuid: UUID(\"{}\")", crate::cmd::fake_uuid()), 1);
                return Err(MongoError::namespace_exists(format!("namespace {ns} already exists, but with different options: {body}")));
            }
        }
        if let Some(e) = existing {
            if e.view_on != view_on {
                return Err(MongoError::namespace_exists(format!("namespace {ns} already exists, but is a view on {db}.{} rather than {db}.{view_on}", e.view_on)));
            }
            if e.pipeline != pipeline {
                let shown = |p: &[Bson]| crate::update::fmt_doc(&p.iter().enumerate().map(|(i, s)| (i.to_string(), s.clone())).collect::<Document>());
                return Err(MongoError::namespace_exists(format!("namespace {ns} already exists, but with pipeline {} rather than {}", shown(&e.pipeline), shown(&pipeline))));
            }
            if e.collation != collation {
                let shown = |c: &Option<Document>| c.as_ref().map(|c| crate::update::fmt_doc(&c.iter().filter(|(k, _)| !matches!(k.as_str(), "backwards" | "version")).map(|(k, v)| (k.clone(), v.clone())).collect())).unwrap_or_else(|| "{}".into());
                return Err(MongoError::namespace_exists(format!("namespace {ns} already exists, but with collation: {} rather than {}", shown(&e.collation), shown(&collation))));
            }
            return Ok(Some(ok()));
        }
        self.check_view_cycle(db, coll, view_on).await?;
        self.check_view_pipeline(db, &ns, &pipeline)?;
        let mut d = doc! { "_id": ns, "viewOn": view_on, "pipeline": pipeline.clone() };
        if let Some(c) = collation.clone() {
            d.insert("collation", c);
        }
        let mut options = doc! { "viewOn": view_on, "pipeline": pipeline.clone() };
        if let Some(c) = &collation {
            options.insert("collation", c.clone());
        }
        self.insert(db, &doc! { "insert": VIEWS, "documents": [d] }, &mut None).await?;
        self.publish_ddl(crate::changes::EvSpec { op_desc: Some(options), ..crate::changes::EvSpec::on(db, coll, "create") }, true);
        Ok(Some(ok()))
    }

    /// `collMod` of a view: a new `viewOn` and / or `pipeline`.
    async fn coll_mod_view(&self, db: &str, coll: &str, cmd: &Document, def: ViewDef) -> R<Document> {
        let ns = format!("{db}.{coll}");
        for (k, _) in cmd {
            if !matches!(k.as_str(), "collMod" | "viewOn" | "pipeline" | "$db" | "lsid" | "$clusterTime" | "writeConcern" | "comment" | "txnNumber" | "$readPreference" | "readConcern" | "maxTimeMS") {
                return Err(MongoError::new(72, "InvalidOptions", format!("option not supported on a view: {k}")));
            }
        }
        let wrong = |f: &str, v: &Bson, want: &str| MongoError::type_mismatch(format!("BSON field 'collMod.{f}' is the wrong type '{}', expected type '{want}'", crate::update::type_name(v)));
        if let Some(v) = cmd.get("viewOn") {
            if v.as_str().is_none() {
                return Err(wrong("viewOn", v, "string"));
            }
        }
        if let Some(v) = cmd.get("pipeline") {
            if !matches!(v, Bson::Array(_)) {
                return Err(wrong("pipeline", v, "array"));
            }
        }
        let view_on = cmd.get_str("viewOn").map(str::to_string).unwrap_or(def.view_on.clone());
        let pipeline = cmd.get_array("pipeline").cloned().unwrap_or(def.pipeline.clone());
        self.check_view_cycle(db, coll, &view_on).await?;
        self.check_view_pipeline(db, &ns, &pipeline)?;
        self.update(db, &doc! { "update": VIEWS, "updates": [{ "q": { "_id": ns }, "u": { "$set": { "viewOn": view_on, "pipeline": pipeline } } }] }, &mut None).await?;
        Ok(ok())
    }

    /// `renameCollection` involving a view.
    pub(crate) async fn view_rename_check(&self, from_db: &str, from: &str, to_db: &str, to: &str) -> R<()> {
        if self.view_def(from_db, from).await?.is_some() {
            return Err(MongoError::new(166, "CommandNotSupportedOnView", format!("cannot rename view: {from_db}.{from}")));
        }
        if self.view_def(to_db, to).await?.is_some() {
            return Err(MongoError::namespace_exists(format!("a view already exists with that name: {to_db}.{to}")));
        }
        Ok(())
    }

    /// The number of views of the database.
    pub(crate) async fn view_count(&self, db: &str) -> R<i64> {
        Ok(self.list_views(db).await?.len() as i64)
    }
}
