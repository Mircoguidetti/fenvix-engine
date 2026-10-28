//! The `mapReduce` command: the input is read with a pipeline (`$match`, `$sort`, `$limit`), the JavaScript map, reduce and
//! finalize functions run in a sandboxed context ([`crate::js::map_reduce`]), and the `{_id, value}` results are returned
//! inline or written to a collection (`replace`, `merge`, `reduce`).

use crate::cmd::{collection_name, Server, R};
use crate::error::MongoError;
use crate::txn::Txn;
use bson::{doc, Bson, Document};

const ALLOWED: &[&str] = &[
    "mapReduce", "map", "reduce", "finalize", "out", "query", "sort", "limit", "scope", "verbose", "jsMode", "bypassDocumentValidation", "collation", "writeConcern", "readConcern", "maxTimeMS", "comment", "$db",
    "lsid", "txnNumber", "$clusterTime", "$readPreference", "readPreference", "apiVersion", "apiStrict", "apiDeprecationErrors", "autocommit", "startTransaction", "$audit", "$queryOptions", "$configTime", "$topologyTime", "$oplogQueryData", "$replData", "$maxTimeMS",
];

fn bad(msg: impl Into<String>) -> MongoError {
    MongoError::new(2, "BadValue", msg.into())
}

fn code_of(cmd: &Document, key: &str, required: bool) -> R<Option<String>> {
    match cmd.get(key) {
        Some(Bson::String(s)) => Ok(Some(s.clone())),
        Some(Bson::JavaScriptCode(c)) => Ok(Some(c.clone())),
        Some(_) => Err(bad(format!("'{key}' must be of string or code type"))),
        None if required => Err(MongoError::new(40414, "IDLFailedToParse", format!("BSON field 'mapReduce.{key}' is missing but a required field"))),
        None => Ok(None),
    }
}

fn wrong_type(field: &str, got: &Bson, expected: &str) -> MongoError {
    MongoError::new(14, "TypeMismatch", format!("BSON field 'mapReduce.{field}' is the wrong type '{}', expected {expected}", crate::update::type_name(got)))
}

enum Mode {
    Replace,
    Merge,
    Reduce,
}

enum Out {
    Inline,
    Coll { db: Option<String>, name: String, mode: Mode },
}

fn parse_out(v: &Bson) -> R<Out> {
    let special = |name: &str| -> R<()> {
        if name.starts_with("system.") {
            return Err(MongoError::new(17385, "Location17385", format!("Can't output mapReduce results to special collection {name}")));
        }
        if name.is_empty() || name.contains('$') || name.contains('\0') {
            return Err(MongoError::new(73, "InvalidNamespace", format!("Invalid collection name specified '{name}'")));
        }
        Ok(())
    };
    match v {
        Bson::String(s) => {
            special(s)?;
            Ok(Out::Coll { db: None, name: s.clone(), mode: Mode::Replace })
        }
        Bson::Document(d) => {
            if d.contains_key("inline") {
                if d.len() > 1 {
                    return Err(MongoError::new(72, "InvalidOptions", "'inline' must be specified alone".into()));
                }
                return Ok(Out::Inline);
            }
            let modes: Vec<&String> = d.keys().filter(|k| matches!(k.as_str(), "replace" | "merge" | "reduce")).collect();
            if modes.is_empty() {
                return Err(bad("'out' requires 'replace', 'merge' or 'reduce'"));
            }
            if modes.len() > 1 || d.keys().any(|k| !matches!(k.as_str(), "replace" | "merge" | "reduce" | "sharded" | "nonAtomic" | "db")) {
                return Err(bad("'out' supports only output type with collection name, optional 'sharded' and optional 'db'"));
            }
            let mode = match modes[0].as_str() {
                "replace" => Mode::Replace,
                "merge" => Mode::Merge,
                _ => Mode::Reduce,
            };
            let name = match d.get(modes[0]) {
                Some(Bson::String(s)) => s.clone(),
                _ => return Err(bad("'out' collection name must be a string")),
            };
            special(&name)?;
            if matches!(d.get("sharded"), Some(Bson::Boolean(true))) && matches!(mode, Mode::Replace) {
                return Err(MongoError::new(72, "InvalidOptions", "MapReduce internal error :: caused by :: Combination of 'out.sharded' and 'replace' output mode is not supported. Cannot replace an existing sharded collection or create a new sharded collection. Please create the sharded collection first and use a different output mode or consider using an unsharded collection.".into()));
            }
            let db = match d.get("db") {
                None => None,
                Some(Bson::String(s)) => Some(s.clone()),
                Some(_) => return Err(bad("'out.db' must be a string")),
            };
            Ok(Out::Coll { db, name, mode })
        }
        _ => Err(bad("'out' must be either a string or an object")),
    }
}

impl Server {
    pub(crate) async fn map_reduce(&self, db: &str, cmd: &Document, tx: &mut Option<Txn>) -> R<Document> {
        let coll = collection_name(cmd, "mapReduce")?;
        for k in cmd.keys() {
            if !ALLOWED.contains(&k.as_str()) {
                return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field 'mapReduce.{k}' is an unknown field.")));
            }
        }
        let map = code_of(cmd, "map", true)?.unwrap();
        let reduce = code_of(cmd, "reduce", true)?.unwrap();
        let out = match cmd.get("out") {
            Some(v) => parse_out(v)?,
            None => return Err(MongoError::new(40414, "IDLFailedToParse", "BSON field 'mapReduce.out' is missing but a required field".into())),
        };
        if matches!(out, Out::Inline) && cmd.contains_key("writeConcern") {
            return Err(MongoError::new(72, "InvalidOptions", "Command does not support writeConcern".into()));
        }
        for f in ["jsMode", "verbose", "bypassDocumentValidation"] {
            if let Some(v) = cmd.get(f) {
                if !matches!(v, Bson::Boolean(_)) {
                    return Err(wrong_type(f, v, "type 'bool'"));
                }
            }
        }
        let finalize = code_of(cmd, "finalize", false)?;
        let query = match cmd.get("query") {
            None => Document::new(),
            Some(Bson::Document(d)) => d.clone(),
            Some(other) => return Err(wrong_type("query", other, "type 'object'")),
        };
        let sort = match cmd.get("sort") {
            None => None,
            Some(Bson::Document(d)) => Some(d.clone()),
            Some(other) => return Err(wrong_type("sort", other, "type 'object'")),
        };
        let limit = match cmd.get("limit") {
            None => None,
            Some(v @ (Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_))) => {
                let n = crate::index::num_of(v).unwrap_or(1.0);
                if n <= 0.0 {
                    return Err(MongoError::new(15958, "Location15958", "The limit specified to mapReduce must be positive".into()));
                }
                Some(n as i64)
            }
            Some(other) => return Err(wrong_type("limit", other, "types '[long, int, decimal, double']")),
        };
        let scope = match cmd.get("scope") {
            None => Document::new(),
            Some(Bson::Document(d)) => d.clone(),
            Some(_) => return Err(bad("'scope' must be an object")),
        };

        // the input documents
        let mut pipeline: Vec<Bson> = vec![Bson::Document(doc! { "$match": query })];
        if let Some(s) = sort {
            pipeline.push(Bson::Document(doc! { "$sort": s }));
        }
        if let Some(n) = limit {
            pipeline.push(Bson::Document(doc! { "$limit": n }));
        }
        let input = self.agg_docs(db, coll, pipeline, tx).await.map_err(|e| if e.code == 2 && e.msg.contains("$where") { bad("MapReduce internal error :: caused by :: $where is not allowed in this context") } else { e })?;

        let prefixed = |mut e: MongoError| {
            if !e.msg.starts_with("MapReduce internal error") {
                e.msg = format!("MapReduce internal error :: caused by :: {}", e.msg);
            }
            e
        };
        let results = crate::js::map_reduce(&map, &reduce, finalize.as_deref(), &scope, &input).map_err(prefixed)?;

        match out {
            Out::Inline => Ok(doc! { "results": results.into_iter().map(Bson::Document).collect::<Vec<_>>(), "ok": 1.0 }),
            Out::Coll { db: out_db, name, mode } => {
                let target_db = out_db.clone().unwrap_or_else(|| db.to_string());
                let docs = match mode {
                    Mode::Reduce => {
                        let keys: Vec<Bson> = results.iter().filter_map(|d| d.get("_id").cloned()).collect();
                        let existing = self.agg_docs(&target_db, &name, vec![Bson::Document(doc! { "$match": { "_id": { "$in": keys } } })], tx).await?;
                        let mut merged = Vec::with_capacity(results.len());
                        for r in results {
                            let key = r.get("_id").cloned().unwrap_or(Bson::Null);
                            let old = existing.iter().find(|e| e.get("_id").map(|k| crate::query::equal(k, &key)).unwrap_or(false));
                            merged.push(match old {
                                Some(o) => {
                                    let v = crate::js::reduce_pair(&reduce, finalize.as_deref(), &key, o.get("value").unwrap_or(&Bson::Null), r.get("value").unwrap_or(&Bson::Null))?;
                                    doc! { "_id": key, "value": v }
                                }
                                None => r,
                            });
                        }
                        merged
                    }
                    _ => results,
                };
                let stage = match mode {
                    Mode::Replace => Bson::Document(doc! { "$out": name.clone() }),
                    _ => Bson::Document(doc! { "$merge": { "into": name.clone(), "whenMatched": "replace", "whenNotMatched": "insert" } }),
                };
                self.agg_write_stage(&target_db, &stage, docs, tx).await?;
                let result = match out_db {
                    Some(d) => Bson::Document(doc! { "db": d, "collection": name }),
                    None => Bson::String(name),
                };
                Ok(doc! { "result": result, "ok": 1.0 })
            }
        }
    }

    /// All documents an aggregation over `coll` returns.
    async fn agg_docs(&self, db: &str, coll: &str, pipeline: Vec<Bson>, tx: &mut Option<Txn>) -> R<Vec<Document>> {
        let cmd = doc! { "aggregate": coll, "pipeline": pipeline, "cursor": { "batchSize": i32::MAX } };
        let r = self.aggregate(db, &cmd, tx).await?;
        let batch = r.get_document("cursor").ok().and_then(|c| c.get_array("firstBatch").ok()).cloned().unwrap_or_default();
        Ok(batch.into_iter().filter_map(|b| if let Bson::Document(d) = b { Some(d) } else { None }).collect())
    }
}
