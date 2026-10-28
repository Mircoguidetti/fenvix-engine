//! Time-series and clustered collections. Their documents are kept like those of any collection (a time-series collection is
//! not bucketed here); what is mongod's is the options, the checks on the documents and on `update` / `delete`, the listing
//! (`type: "timeseries"`, the `system.buckets` companion, no `_id` index) and, for clustered ones, the `_id` order of the
//! documents. `expireAfterSeconds` is a TTL index of our own (named `__ttl`) on the time field or on `_id`.

use crate::cmd::R;
use crate::error::MongoError;
use crate::index::IndexDef;
use bson::{doc, Bson, Document};

/// The name of the hidden TTL index of a time-series or clustered collection.
pub const TTL_NAME: &str = "__ttl";

pub fn is_internal(i: &IndexDef) -> bool {
    i.name == TTL_NAME
}

fn type_error(field: &str, v: &Bson, expected: &str) -> MongoError {
    MongoError::type_mismatch(format!("BSON field 'create.{field}' is the wrong type '{}', expected type '{expected}'", crate::update::type_name(v)))
}

fn number(v: &Bson) -> Option<f64> {
    match v {
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Double(f) => Some(*f),
        Bson::Decimal128(d) => d.to_string().parse().ok(),
        _ => None,
    }
}

/// The options a `create` with `clusteredIndex`, `timeseries` and `expireAfterSeconds` adds to the stored options, and the
/// field of the TTL index (when `expireAfterSeconds` is given).
pub struct Special {
    pub options: Document,
    pub ttl: Option<(String, i64)>,
}

pub fn parse_create(ns: &str, cmd: &Document, capped: bool) -> R<Special> {
    let mut options = Document::new();
    let mut ttl_field: Option<String> = None;
    let clustered = match cmd.get("clusteredIndex") {
        None | Some(Bson::Boolean(false)) => false,
        Some(Bson::Boolean(true)) => {
            options.insert("clusteredIndex", doc! { "v": 2, "key": { "_id": 1 }, "name": "_id_", "unique": true });
            ttl_field = Some("_id".into());
            true
        }
        Some(Bson::Document(c)) => {
            let key_ok = c.get_document("key").map(|k| k.len() == 1 && k.get("_id").and_then(number) == Some(1.0)).unwrap_or(false);
            if !key_ok {
                return Err(MongoError::new(197, "InvalidIndexSpecificationOption", "The clusteredIndex option is only supported for key: {_id: 1}".into()));
            }
            if !matches!(c.get("unique"), Some(Bson::Boolean(true))) {
                return Err(MongoError::new(5979700, "Location5979700", "The clusteredIndex option requires unique: true to be specified".into()));
            }
            if capped {
                return Err(MongoError::new(6127800, "Location6127800", "Clustered capped collection only available with 'enableTestCommands' server parameter".into()));
            }
            let name = c.get_str("name").unwrap_or("_id_");
            options.insert("clusteredIndex", doc! { "v": 2, "key": { "_id": 1 }, "name": name, "unique": true });
            ttl_field = Some("_id".into());
            true
        }
        Some(other) => return Err(type_error("clusteredIndex", other, "bool or object")),
    };
    let mut series = false;
    if let Some(ts) = cmd.get("timeseries") {
        let Bson::Document(ts) = ts else { return Err(type_error("timeseries", ts, "object")) };
        series = true;
        let time_field = match ts.get("timeField") {
            Some(Bson::String(s)) => s.clone(),
            Some(other) => return Err(type_error("timeseries.timeField", other, "string")),
            None => return Err(MongoError::new(40414, "IDLFailedToParse", "BSON field 'create.timeseries.timeField' is missing but a required field".into())),
        };
        if time_field.contains('.') {
            return Err(MongoError::new(72, "InvalidOptions", format!("{ns}: 'timeField' must be a top-level field and not contain a '.'")));
        }
        let meta = match ts.get("metaField") {
            None => None,
            Some(Bson::String(s)) => Some(s.clone()),
            Some(other) => return Err(type_error("timeseries.metaField", other, "string")),
        };
        if let Some(m) = &meta {
            if m.contains('.') {
                return Err(MongoError::new(72, "InvalidOptions", format!("{ns}: 'metaField' must be a top-level field and not contain a '.'")));
            }
            if m == "_id" || *m == time_field {
                return Err(MongoError::new(72, "InvalidOptions", format!("{ns}: 'metaField' cannot be set to the 'timeField' or '_id'")));
            }
        }
        let gran = match ts.get("granularity") {
            None => None,
            Some(Bson::String(g)) if matches!(g.as_str(), "seconds" | "minutes" | "hours") => Some(g.clone()),
            Some(Bson::String(g)) => return Err(MongoError::bad_value(format!("Enumeration value '{g}' for field 'create.timeseries.granularity' is not a valid value."))),
            Some(other) => return Err(type_error("timeseries.granularity", other, "string")),
        };
        let span = ts.get("bucketMaxSpanSeconds").and_then(number);
        let round = ts.get("bucketRoundingSeconds").and_then(number);
        let mut t = doc! { "timeField": time_field.clone() };
        if let Some(m) = meta {
            t.insert("metaField", m);
        }
        match (gran, span, round) {
            (g, None, None) => {
                let g = g.unwrap_or_else(|| "seconds".into());
                let secs = match g.as_str() {
                    "seconds" => 3600,
                    "minutes" => 86400,
                    _ => 2592000,
                };
                t.insert("granularity", g);
                t.insert("bucketMaxSpanSeconds", secs);
            }
            (None, Some(s), Some(r)) if s == r => {
                t.insert("bucketMaxSpanSeconds", s as i32);
                t.insert("bucketRoundingSeconds", r as i32);
            }
            (None, Some(_), Some(_)) => return Err(MongoError::new(72, "InvalidOptions", "Both 'bucketMaxSpanSeconds' and 'bucketRoundingSeconds' must be set to the same value".into())),
            (Some(_), _, _) => return Err(MongoError::new(72, "InvalidOptions", "Time-series 'granularity' cannot be set together with 'bucketMaxSpanSeconds' or 'bucketRoundingSeconds'".into())),
            _ => return Err(MongoError::new(72, "InvalidOptions", "Both 'bucketMaxSpanSeconds' and 'bucketRoundingSeconds' must be set".into())),
        }
        options.insert("timeseries", t);
        ttl_field = Some(time_field);
    }
    let expire = match cmd.get("expireAfterSeconds") {
        None => None,
        Some(v) => {
            let Some(n) = number(v) else { return Err(type_error("expireAfterSeconds", v, "long")) };
            if !clustered && !series {
                return Err(MongoError::new(72, "InvalidOptions", "'expireAfterSeconds' is only supported on time-series collections or when the 'clusteredIndex' option is specified".into()));
            }
            if n < 0.0 {
                return Err(MongoError::bad_value("expireAfterSeconds must be non-negative".into()));
            }
            options.insert("expireAfterSeconds", Bson::Int64(n as i64));
            Some(n as i64)
        }
    };
    Ok(Special { options, ttl: ttl_field.zip(expire) })
}

/// The hidden TTL index.
pub fn ttl_index(field: &str, secs: i64) -> Option<IndexDef> {
    IndexDef::from_stored(&doc! { "v": 2, "key": { field: 1 }, "name": TTL_NAME, "expireAfterSeconds": Bson::Int64(secs) })
}

/// The `system.buckets.<name>` companion that `listCollections` shows for a time-series collection.
pub fn buckets_entry(name: &str, options: &Document, uuid: Bson) -> Document {
    let ts = options.get_document("timeseries").cloned().unwrap_or_default();
    let time = ts.get_str("timeField").unwrap_or("t").to_string();
    let bound = |_: ()| doc! { "bsonType": "object", "required": [time.clone()], "properties": { time.clone(): { "bsonType": "date" } } };
    let props = doc! {
        "_id": { "bsonType": "objectId" },
        "control": { "bsonType": "object", "required": ["version", "min", "max"], "properties": {
            "version": { "bsonType": "number" }, "min": bound(()), "max": bound(()), "closed": { "bsonType": "bool" }, "count": { "bsonType": "number", "minimum": 1 },
        }, "additionalProperties": false },
        "data": { "bsonType": "object" },
        "meta": {},
    };
    let mut opts = doc! { "validator": { "$jsonSchema": { "bsonType": "object", "required": ["_id", "control", "data"], "properties": props, "additionalProperties": false } }, "clusteredIndex": true, "timeseries": ts };
    if let Some(e) = options.get("expireAfterSeconds") {
        opts.insert("expireAfterSeconds", e.clone());
    }
    doc! { "name": format!("system.buckets.{name}"), "type": "collection", "options": opts, "info": { "readOnly": false, "uuid": uuid } }
}

impl crate::cmd::Server {
    /// The stored options of a collection.
    pub(crate) async fn coll_kind(&self, db: &str, coll: &str) -> R<Option<std::sync::Arc<crate::collopts::CollOpts>>> {
        let g = self.dbs.read(db).await?;
        Ok(g.coll_opts(coll).await?)
    }
}

/// Sort documents of a clustered collection by `_id`.
pub fn sort_by_id(docs: &mut [Document], reverse: bool) {
    docs.sort_by(|a, b| {
        let o = crate::query::compare(a.get("_id").unwrap_or(&Bson::Null), b.get("_id").unwrap_or(&Bson::Null));
        if reverse { o.reverse() } else { o }
    });
}

fn query_on_meta_only(q: &Document, meta: &str) -> bool {
    q.iter().all(|(k, v)| match k.as_str() {
        "$and" | "$or" | "$nor" => matches!(v, Bson::Array(a) if a.iter().all(|s| matches!(s, Bson::Document(d) if query_on_meta_only(d, meta)))),
        k if k.starts_with('$') => true,
        k => k == meta || k.starts_with(&format!("{meta}.")),
    })
}

/// Why an `update` statement is not allowed on a time-series collection (only the meta field can be updated).
pub fn series_update_error(series: &(String, Option<String>), spec: &Document) -> Option<MongoError> {
    let err = |m: String| Some(MongoError::new(72, "InvalidOptions", m));
    if spec.get("multi") != Some(&Bson::Boolean(true)) {
        return err("Cannot perform a non-multi update on a time-series collection".into());
    }
    if spec.get("upsert") == Some(&Bson::Boolean(true)) {
        return err("Cannot perform an upsert on a time-series collection".into());
    }
    let u = spec.get("u");
    match u {
        Some(Bson::Array(_)) => return err("Cannot perform an update on a time-series collection using a pipeline update".into()),
        Some(Bson::Document(d)) if !d.keys().any(|k| k.starts_with('$')) => return err("Cannot perform an update on a time-series collection using a replacement document".into()),
        _ => {}
    }
    let Some(meta) = &series.1 else {
        return err("Cannot perform an update on a time-series collection that does not have a metaField".into());
    };
    if let Some(Bson::Document(q)) = spec.get("q") {
        if !query_on_meta_only(q, meta) {
            return err(format!("Cannot perform an update or delete on a time-series collection when querying on a field that is not the metaField '{meta}'"));
        }
    }
    if let Some(Bson::Document(u)) = u {
        for (_, fields) in u {
            if let Bson::Document(f) = fields {
                if f.keys().any(|p| p != meta && !p.starts_with(&format!("{meta}."))) {
                    return err(format!("Cannot perform an update on a time-series collection which updates a field that is not the metaField '{meta}'"));
                }
            }
        }
    }
    None
}

impl crate::cmd::Server {
    /// `collMod` of `expireAfterSeconds` (clustered / time-series) and `timeseries.granularity`.
    pub(crate) async fn coll_mod_special(&self, g: &mut crate::dbs::Entry, db: &str, coll: &str, cmd: &Document) -> crate::cmd::R<()> {
        use crate::dbs::Entry;
        let opts = g.coll_opts(coll).await?;
        let mut raw = opts.as_ref().map(|o| o.raw.clone()).unwrap_or_default();
        let is_series = raw.get_document("timeseries").is_ok();
        let is_clustered = raw.contains_key("clusteredIndex");
        let mut changed = false;
        if let Some(ts) = cmd.get("timeseries") {
            let Bson::Document(ts) = ts else {
                return Err(MongoError::type_mismatch(format!("BSON field 'collMod.timeseries' is the wrong type '{}', expected type 'object'", crate::update::type_name(ts))));
            };
            if !is_series {
                return Err(MongoError::new(72, "InvalidOptions", "collMod on a non-timeseries collection is not supported with the 'timeseries' option".into()));
            }
            let cur = raw.get_document("timeseries").unwrap().clone();
            let mut next = cur.clone();
            if let Some(gv) = ts.get("granularity") {
                let Some(gs) = gv.as_str() else {
                    return Err(MongoError::type_mismatch(format!("BSON field 'collMod.timeseries.granularity' is the wrong type '{}', expected type 'string'", crate::update::type_name(gv))));
                };
                let rank = |x: &str| ["seconds", "minutes", "hours"].iter().position(|y| *y == x);
                let (Some(new), Some(old)) = (rank(gs), cur.get_str("granularity").ok().and_then(rank)) else {
                    return Err(MongoError::new(2, "BadValue", format!("Invalid value for granularity: {gs}")));
                };
                if new < old {
                    return Err(MongoError::new(72, "InvalidOptions", "Invalid transition for timeseries.granularity. Can only transition from 'seconds' to 'minutes' or 'minutes' to 'hours'.".into()));
                }
                next.insert("granularity", gs);
                next.insert("bucketMaxSpanSeconds", [3600i32, 86400, 2592000][new]);
                next.insert("bucketRoundingSeconds", [60i32, 3600, 86400][new]);
            }
            for (k, _) in ts {
                if k != "granularity" && k != "bucketMaxSpanSeconds" {
                    return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field 'collMod.timeseries.{k}' is an unknown field.")));
                }
            }
            if next != cur {
                raw.insert("timeseries", next);
                changed = true;
            }
        }
        let mut index_ops = Vec::new();
        if let Some(v) = cmd.get("expireAfterSeconds") {
            if !is_series && !is_clustered {
                return Err(MongoError::new(72, "InvalidOptions", "option not supported on a collection that is neither time-series nor clustered".into()));
            }
            let secs = match v {
                Bson::String(s) if s == "off" => None,
                Bson::Int32(i) => Some(*i as i64),
                Bson::Int64(i) => Some(*i),
                Bson::Double(f) => Some(*f as i64),
                other => return Err(MongoError::type_mismatch(format!("BSON field 'collMod.expireAfterSeconds' is the wrong type '{}', expected types '[long, int, decimal, double, string]'", crate::update::type_name(other)))),
            };
            if let Some(n) = secs {
                if n < 0 {
                    return Err(MongoError::new(72, "InvalidOptions", "TTL index 'expireAfterSeconds' option cannot be less than 0".into()));
                }
            }
            let field = if is_series { raw.get_document("timeseries").ok().and_then(|t| t.get_str("timeField").ok()).unwrap_or("_id").to_string() } else { "_id".to_string() };
            match secs {
                Some(n) => {
                    raw.insert("expireAfterSeconds", n);
                    if let Some(ix) = ttl_index(&field, n) { index_ops.push(ix); }
                }
                None => { raw.remove("expireAfterSeconds"); }
            }
            changed = true;
            let mut indexes = self.load_indexes(g, db, coll).await?;
            indexes.retain(|i| !is_internal(i));
            indexes.extend(index_ops);
            g.apply(vec![Entry::indexes_op(coll, &indexes)?]).await?;
        }
        if changed {
            let raw = crate::collopts::canonical(&raw);
            g.apply(vec![tenant_bench::lifecycle::Op::Put(crate::collopts::meta_key(coll), raw.to_vec()?)]).await?;
            g.set_coll_opts(coll, Some(crate::collopts::CollOpts::parse(raw)));
        }
        Ok(())
    }
}

/// A time-series measurement as mongod reads it back from its bucket: the time field, the meta field, then the rest in order.
pub fn series_order(mut d: Document, series: &(String, Option<String>)) -> Document {
    let mut out = Document::new();
    for k in [Some(&series.0), series.1.as_ref()].into_iter().flatten() {
        if let Some(v) = d.remove(k) {
            out.insert(k.clone(), v);
        }
    }
    out.extend(d);
    out
}

/// Set once a time-series collection was created in this process: from then on `serverStatus` has a `bucketCatalog` section.
pub static SERIES_SEEN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The `bucketCatalog` section of `serverStatus` (the counters are not tracked: the documents are stored flat).
pub fn bucket_catalog() -> Document {
    let mut d = Document::new();
    for k in ["numBuckets", "numOpenBuckets", "numIdleBuckets", "numArchivedBuckets", "memoryUsage"] {
        d.insert(k, 0i32);
    }
    for k in ["numBucketInserts", "numBucketUpdates", "numBucketsOpenedDueToMetadata", "numBucketsClosedDueToCount", "numBucketsClosedDueToSchemaChange", "numBucketsClosedDueToSize", "numBucketsClosedDueToTimeForward", "numBucketsClosedDueToTimeBackward", "numBucketsClosedDueToMemoryThreshold", "numCommits", "numMeasurementsGroupCommitted", "numWaits", "numMeasurementsCommitted"] {
        d.insert(k, Bson::Int64(0));
    }
    d.insert("avgNumMeasurementsPerCommit", Bson::Int64(0));
    for k in ["numBucketsClosedDueToReopening", "numBucketsArchivedDueToMemoryThreshold", "numBucketsArchivedDueToTimeBackward", "numBucketsReopened", "numBucketsKeptOpenDueToLargeMeasurements", "numBucketsClosedDueToCachePressure", "numBucketsFetched", "numBucketsQueried", "numBucketFetchesFailed", "numBucketQueriesFailed", "numBucketReopeningsFailed", "numDuplicateBucketsReopened", "numBucketDocumentsTooLargeInsert", "numBucketDocumentsTooLargeUpdate"] {
        d.insert(k, Bson::Int64(0));
    }
    d.insert("stateManagement", doc! { "bucketsManaged": 0i64, "currentEra": 0i64, "erasWithRemainingBuckets": 0i64, "trackedClearOperations": 0i64 });
    d
}

/// `create` with `encryptedFields` (Queryable Encryption): validated and listed the way mongod does. The side collections
/// (`enxcol_.<coll>.esc` / `.ecoc`) are not materialised: mongod hides them, and the encrypted payload handling of writes is the client's.
pub fn encrypted_fields(coll: &str, v: &Bson, is_rs: bool) -> R<Document> {
    let missing = |p: &str| MongoError::new(40414, "IDLFailedToParse", format!("BSON field '{p}' is missing but a required field"));
    let Bson::Document(ef) = v else {
        return Err(MongoError::type_mismatch(format!("BSON field 'create.encryptedFields' is the wrong type '{}', expected type 'object'", crate::update::type_name(v))));
    };
    if !is_rs {
        return Err(MongoError::new(6346402, "Location6346402", "Encrypted collections are not supported on standalone".into()));
    }
    let Some(Bson::Array(fields)) = ef.get("fields") else { return Err(missing("create.encryptedFields.fields")) };
    let known = ["double", "string", "object", "array", "binData", "undefined", "objectId", "bool", "date", "null", "regex", "dbPointer", "javascript", "symbol", "javascriptWithScope", "int", "timestamp", "long", "decimal", "minKey", "maxKey"];
    let mut out_fields = Vec::new();
    let mut key_ids: Vec<Bson> = Vec::new();
    for f in fields {
        let Bson::Document(f) = f else {
            return Err(MongoError::type_mismatch(format!("BSON field 'create.encryptedFields.fields' is the wrong type '{}', expected type 'object'", crate::update::type_name(f))));
        };
        let key = match f.get("keyId") {
            None | Some(Bson::Null) => return Err(missing("create.encryptedFields.fields.keyId")),
            Some(k) => k.clone(),
        };
        let Some(Bson::String(path)) = f.get("path") else { return Err(missing("create.encryptedFields.fields.path")) };
        let bt = match f.get("bsonType") {
            Some(Bson::String(bt)) if known.contains(&bt.as_str()) => Some(bt.clone()),
            Some(Bson::String(bt)) => return Err(MongoError::bad_value(format!("Unknown type name: {bt}"))),
            _ => None,
        };
        if key_ids.contains(&key) {
            return Err(MongoError::new(6338401, "Location6338401", "Duplicate key ids are not allowed".into()));
        }
        key_ids.push(key.clone());
        let mut o = doc! { "keyId": key, "path": path.clone() };
        if let Some(bt) = bt {
            o.insert("bsonType", bt);
        }
        let contention = |q: &Document| {
            let mut q = q.clone();
            if !q.contains_key("contention") {
                q.insert("contention", Bson::Int64(8));
            }
            Bson::Document(q)
        };
        match f.get("queries") {
            Some(Bson::Document(q)) => { o.insert("queries", contention(q)); }
            Some(Bson::Array(a)) => { o.insert("queries", a.iter().map(|q| q.as_document().map(contention).unwrap_or_else(|| q.clone())).collect::<Vec<_>>()); }
            _ => {}
        }
        out_fields.push(Bson::Document(o));
    }
    Ok(doc! {
        "escCollection": ef.get_str("escCollection").map(String::from).unwrap_or_else(|_| format!("enxcol_.{coll}.esc")),
        "ecocCollection": ef.get_str("ecocCollection").map(String::from).unwrap_or_else(|_| format!("enxcol_.{coll}.ecoc")),
        "fields": out_fields,
    })
}
