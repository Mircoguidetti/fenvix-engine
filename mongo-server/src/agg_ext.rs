//! More aggregation stages: `$replaceRoot`, `$replaceWith`, `$unset`, `$sortByCount`, `$bucket`, `$sample`,
//! `$facet`, `$unionWith`.

use crate::agg::{run_pipeline, Foreign};
use crate::error::MongoError;
use crate::query::compare;
use bson::{doc, Bson, Document};

type R<T> = Result<T, MongoError>;

fn ev(e: &Bson, d: &Document) -> R<Option<Bson>> {
    crate::expr::eval(e, &crate::expr::Scope::new(d))
}

/// Names of the collections a pipeline reads besides its own (for `$lookup`, `$unionWith`, nested in `$facet`).
pub fn extra_collections(pipeline: &[Bson], out: &mut Vec<String>) {
    for stage in pipeline {
        let Some(d) = stage.as_document() else { continue };
        for (name, arg) in d {
            match (name.as_str(), arg) {
                ("$lookup", Bson::Document(l)) => {
                    if let Some(Bson::String(from)) = l.get("from") {
                        out.push(from.clone());
                    }
                    if let Some(Bson::Array(p)) = l.get("pipeline") {
                        extra_collections(p, out);
                    }
                }
                ("$graphLookup", Bson::Document(g)) => {
                    if let Some(Bson::String(from)) = g.get("from") {
                        out.push(from.clone());
                    }
                }
                ("$unionWith", Bson::String(c)) => out.push(c.clone()),
                ("$unionWith", Bson::Document(u)) => {
                    if let Some(Bson::String(c)) = u.get("coll") {
                        out.push(c.clone());
                    }
                    if let Some(Bson::Array(p)) = u.get("pipeline") {
                        extra_collections(p, out);
                    }
                }
                ("$facet", Bson::Document(f)) => {
                    for (_, p) in f {
                        if let Bson::Array(p) = p {
                            extra_collections(p, out);
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

pub fn run_stage(name: &str, arg: &Bson, docs: Vec<Document>, foreign: &Foreign) -> R<Option<Vec<Document>>> {
    Ok(Some(match name {
        "$replaceRoot" | "$replaceWith" => {
            let expr = if name == "$replaceRoot" {
                let d = arg.as_document().ok_or_else(|| MongoError::new(40228, "Location40228", "expected an object as specification for $replaceRoot stage".to_string()))?;
                for k in d.keys() {
                    if k != "newRoot" {
                        return Err(MongoError::new(40415, "Location40415", format!("BSON field '$replaceRoot.{k}' is an unknown field.")));
                    }
                }
                d.get("newRoot").ok_or_else(|| MongoError::new(40414, "IDLFailedToParse", "BSON field '$replaceRoot.newRoot' is missing but a required field".into()))?
            } else {
                arg
            };
            let mut out = Vec::with_capacity(docs.len());
            for d in docs {
                match ev(expr, &d)? {
                    Some(Bson::Document(n)) => out.push(n),
                    other => {
                        let shown = other.as_ref().map(crate::update::fmt_bson).unwrap_or_else(|| "missing".into());
                        return Err(MongoError::new(40228, "Location40228", format!("'newRoot' expression must evaluate to an object, but resulting value was: {shown}. Type of resulting value: '{}'. Input document: {}", other.as_ref().map(crate::update::type_name).unwrap_or("missing"), crate::update::fmt_doc(&d))));
                    }
                }
            }
            out
        }
        "$unset" => {
            let fields: Vec<String> = match arg {
                Bson::String(s) => vec![s.clone()],
                Bson::Array(a) if a.is_empty() => return Err(MongoError::new(31119, "Location31119", "$unset specification must be a string or an array with at least one field".into())),
                Bson::Array(a) => a.iter().map(|x| x.as_str().map(str::to_string).ok_or_else(|| MongoError::new(31120, "Location31120", "$unset specification must be a string or an array containing only string values".into()))).collect::<R<_>>()?,
                _ => return Err(MongoError::new(31002, "Location31002", "$unset specification must be a string or an array".into())),
            };
            let mut spec = Document::new();
            for f in fields {
                spec.insert(f, 0);
            }
            return run_pipeline(docs, &[Bson::Document(doc! { "$project": spec })], foreign).map(Some);
        }
        "$sortByCount" => {
            let grouped = run_pipeline(docs, &[Bson::Document(doc! { "$group": { "_id": arg.clone(), "count": { "$sum": 1 } } })], foreign)?;
            let mut g = grouped;
            // stable: ties keep first-seen order, as the group stage emits it
            g.sort_by(|a, b| compare(b.get("count").unwrap_or(&Bson::Null), a.get("count").unwrap_or(&Bson::Null)));
            g
        }
        "$bucket" => bucket(arg, docs, foreign)?,
        "$sample" => {
            let n = arg.as_document().and_then(|d| d.get("size")).and_then(|s| match s {
                Bson::Int32(i) => Some(*i as i64),
                Bson::Int64(i) => Some(*i),
                Bson::Double(f) => Some(*f as i64),
                _ => None,
            });
            let Some(n) = n.filter(|n| *n >= 0) else {
                return Err(MongoError::new(28747, "Location28747", "size argument to $sample must not be negative".into()));
            };
            let mut docs = docs;
            use rand::seq::SliceRandom;
            docs.shuffle(&mut rand::thread_rng());
            docs.truncate(n as usize);
            docs
        }
        "$facet" => {
            let spec = arg.as_document().ok_or_else(|| MongoError::new(40169, "Location40169", format!("the $facet specification must be a non-empty object, but found: {}", crate::update::type_name(arg))))?;
            if spec.is_empty() {
                return Err(MongoError::new(40169, "Location40169", "the $facet specification must be a non-empty object, but found: {}".into()));
            }
            let mut out = Document::new();
            for (k, p) in spec {
                if k.is_empty() || k.starts_with('$') || k.contains('.') {
                    return Err(MongoError::new(40170, "Location40170", format!("$facet names cannot be empty or contain '.' or start with '$': {k}")));
                }
                let Bson::Array(stages) = p else {
                    return Err(MongoError::new(40170, "Location40170", format!("arguments to $facet must be arrays, {k} is type {}", crate::update::type_name(p))));
                };
                for s in stages {
                    if let Some(sd) = s.as_document() {
                        for forbidden in ["$out", "$merge", "$facet", "$collStats", "$indexStats", "$geoNear", "$changeStream", "$planCacheStats"] {
                            if sd.contains_key(forbidden) {
                                return Err(MongoError::new(40600, "Location40600", format!("{forbidden} is not allowed to be used within a $facet stage")));
                            }
                        }
                    }
                }
                out.insert(k.clone(), run_pipeline(docs.clone(), stages, foreign)?.into_iter().map(Bson::Document).collect::<Vec<_>>());
            }
            vec![out]
        }
        "$unionWith" => {
            let (coll, pipeline): (String, Vec<Bson>) = match arg {
                Bson::String(c) => (c.clone(), vec![]),
                Bson::Document(d) => {
                    for k in d.keys() {
                        if !matches!(k.as_str(), "coll" | "pipeline") {
                            return Err(MongoError::new(40415, "Location40415", format!("BSON field '$unionWith.{k}' is an unknown field.")));
                        }
                    }
                    let c = d.get_str("coll").map_err(|_| MongoError::new(40415, "Location40415", "$unionWith requires 'coll'".into()))?.to_string();
                    let p = match d.get("pipeline") {
                        Some(Bson::Array(a)) => a.clone(),
                        None => vec![],
                        Some(_) => return Err(MongoError::new(14, "TypeMismatch", "'pipeline' must be an array".into())),
                    };
                    (c, p)
                }
                _ => return Err(MongoError::new(9, "FailedToParse", "the $unionWith stage specification must be an object or string".into())),
            };
            let other = foreign.get(&coll).cloned().unwrap_or_default();
            let mut docs = docs;
            docs.extend(run_pipeline(other, &pipeline, foreign)?);
            docs
        }
        "$changeStreamSplitLargeEvent" => {
            if !matches!(arg, Bson::Document(d) if d.is_empty()) {
                return Err(MongoError::new(7182800, "Location7182800", "$changeStreamSplitLargeEvent spec should be an empty object".into()));
            }
            return Err(MongoError::new(20, "IllegalOperation", "$changeStreamSplitLargeEvent can only be used in a $changeStream pipeline".into()));
        }
        "$documents" => return Err(MongoError::new(73, "InvalidNamespace", "$documents is only valid as the first stage of a database-level aggregate ({aggregate: 1})".to_string())),
        "$bucketAuto" => crate::agg_more::bucket_auto(arg, docs, foreign)?,
        "$densify" => crate::agg_more::densify(arg, docs)?,
        "$fill" => crate::agg_more::fill(arg, docs)?,
        "$setWindowFields" => crate::agg_more::set_window_fields(arg, docs, foreign)?,
        "$graphLookup" => crate::agg_more::graph_lookup(arg, docs, foreign)?,
        "$redact" => crate::agg_more::redact(arg, docs)?,
        _ => return Ok(None),
    }))
}

fn bucket(arg: &Bson, docs: Vec<Document>, foreign: &Foreign) -> R<Vec<Document>> {
    let spec = arg.as_document().ok_or_else(|| MongoError::new(40201, "Location40201", "Argument to $bucket stage must be an object".into()))?;
    for k in spec.keys() {
        if !matches!(k.as_str(), "groupBy" | "boundaries" | "default" | "output") {
            return Err(MongoError::new(40197, "Location40197", format!("Unrecognized option to $bucket: {k}.")));
        }
    }
    let group_by = spec.get("groupBy").ok_or_else(|| MongoError::new(40198, "Location40198", "$bucket requires 'groupBy' and 'boundaries' to be specified.".into()))?;
    let bounds = match spec.get("boundaries") {
        Some(Bson::Array(b)) => {
            // a boundary may be a variable
            let empty = Document::new();
            let mut v = Vec::new();
            for x in b {
                v.push(match x {
                    Bson::String(s) if s.starts_with("$$") => crate::expr::eval(x, &crate::expr::Scope::new(&empty))?.unwrap_or(Bson::Null),
                    other => other.clone(),
                });
            }
            v
        }
        Some(_) => return Err(MongoError::new(40200, "Location40200", "The $bucket 'boundaries' field must be an array, but found type".to_string())),
        None => return Err(MongoError::new(40198, "Location40198", "$bucket requires 'groupBy' and 'boundaries' to be specified.".into())),
    };
    if bounds.len() < 2 {
        return Err(MongoError::new(40192, "Location40192", "The $bucket 'boundaries' field must have at least 2 values".to_string()));
    }
    for w in bounds.windows(2) {
        if compare(&w[0], &w[1]) != std::cmp::Ordering::Less {
            return Err(MongoError::new(40194, "Location40194", "The 'boundaries' option to $bucket must be sorted in ascending order".to_string()));
        }
    }
    let default = spec.get("default");
    let output = match spec.get("output") {
        Some(Bson::Document(o)) => o.clone(),
        Some(_) => return Err(MongoError::new(40196, "Location40196", "The $bucket 'output' field must be an object".to_string())),
        None => doc! { "count": { "$sum": 1 } },
    };
    let mut buckets: Vec<Vec<Document>> = vec![Vec::new(); bounds.len() - 1];
    let mut dflt: Vec<Document> = Vec::new();
    for d in docs {
        let v = ev(group_by, &d)?.unwrap_or(Bson::Null);
        let slot = (0..bounds.len() - 1).find(|i| compare(&v, &bounds[*i]) != std::cmp::Ordering::Less && compare(&v, &bounds[i + 1]) == std::cmp::Ordering::Less);
        match slot {
            Some(i) => buckets[i].push(d),
            None => {
                if default.is_none() {
                    return Err(MongoError::new(40066, "Location40066", "$switch could not find a matching branch for an input, and no default was specified.".into()));
                }
                dflt.push(d);
            }
        }
    }
    let mut out = Vec::new();
    let mut emit = |id: Bson, members: Vec<Document>| -> R<()> {
        if members.is_empty() {
            return Ok(());
        }
        let mut g = doc! { "_id": id };
        for (k, v) in &output {
            g.insert(k.clone(), v.clone());
        }
        let mut r = run_pipeline(members, &[Bson::Document(doc! { "$group": g })], foreign)?;
        out.extend(r.drain(..));
        Ok(())
    };
    for (i, members) in buckets.into_iter().enumerate() {
        emit(bounds[i].clone(), members)?;
    }
    if let Some(def) = default {
        emit(def.clone(), dflt)?;
    }
    Ok(out)
}
