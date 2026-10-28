//! `find` options that change what the cursor returns: `min` / `max` (index bounds), `returnKey` (index key documents) and
//! `showRecordId` (`$recordId`), and the text of the planner errors mongod puts in front of its messages.

use crate::cmd::{int_arg, Server, R};
use crate::coll::{find_docs, resolve_hint, FindOpts, Hint, Overlay, Rd};
use crate::error::MongoError;
use crate::index::{self, IndexDef, Plan};
use crate::query::{compare, path_values};
use bson::{doc, Bson, Document};
use std::cmp::Ordering;

#[derive(Default, Clone)]
pub struct ExtOpts {
    pub min: Option<Document>,
    pub max: Option<Document>,
    pub return_key: bool,
    pub show_rid: bool,
}

impl ExtOpts {
    pub fn active(&self) -> bool {
        self.min.is_some() || self.max.is_some() || self.return_key || self.show_rid
    }
}

fn flag(cmd: &Document, name: &str) -> R<bool> {
    match cmd.get(name) {
        None => Ok(false),
        Some(Bson::Boolean(b)) => Ok(*b),
        Some(Bson::Int32(i)) => Ok(*i != 0),
        Some(Bson::Int64(i)) => Ok(*i != 0),
        Some(Bson::Double(f)) => Ok(*f != 0.0),
        Some(v) => Err(MongoError::new(14, "TypeMismatch", format!("Field '{name}' should be a boolean value, but found: {}", crate::update::type_name(v)))),
    }
}

/// The options of a `find` command that this module handles; the type and consistency errors mongod gives at parse time.
pub fn parse(cmd: &Document) -> R<ExtOpts> {
    let bound = |name: &str| -> R<Option<Document>> {
        match cmd.get(name) {
            None => Ok(None),
            Some(Bson::Document(d)) => Ok(Some(d.clone())),
            Some(_) => Err(MongoError::type_mismatch(format!("Expected field {name}to be of type object"))),
        }
    };
    let min = bound("min")?;
    let max = bound("max")?;
    if let (Some(a), Some(b)) = (&min, &max) {
        if a.keys().ne(b.keys()) {
            return Err(MongoError::new(51176, "Location51176", "min and max must have the same field names".into()));
        }
    }
    Ok(ExtOpts { min, max, return_key: flag(cmd, "returnKey")?, show_rid: flag(cmd, "showRecordId")? })
}

/// mongod's debug text of a filter (the "Tree:" of planner errors).
pub fn tree(filter: &Document) -> String {
    fn leaf(path: &str, op: &str, v: &Bson) -> String {
        format!("{path} ${op} {}", crate::update::fmt_bson(v))
    }
    fn clauses(filter: &Document, out: &mut Vec<String>, indent: usize) {
        let pad = " ".repeat(indent);
        for (k, v) in filter {
            match (k.as_str(), v) {
                ("$and" | "$or" | "$nor", Bson::Array(a)) => {
                    let mut inner = Vec::new();
                    for s in a {
                        if let Bson::Document(d) = s {
                            clauses(d, &mut inner, indent + 4);
                        }
                    }
                    out.push(format!("{pad}{k}\n{}", inner.join("\n")));
                }
                (path, Bson::Document(d)) if !d.is_empty() && d.keys().all(|k| k.starts_with('$')) => {
                    for (op, arg) in d {
                        out.push(format!("{pad}{}", leaf(path, op.trim_start_matches('$'), arg)));
                    }
                }
                (path, v) => out.push(format!("{pad}{}", leaf(path, "eq", v))),
            }
        }
    }
    let mut out = Vec::new();
    clauses(filter, &mut out, 0);
    match out.len() {
        0 => "$and\n".to_string(),
        1 if !filter.keys().any(|k| matches!(k.as_str(), "$and" | "$or" | "$nor")) => format!("{}\n", out[0]),
        _ => {
            if filter.len() == 1 && filter.keys().any(|k| matches!(k.as_str(), "$and" | "$or" | "$nor")) {
                format!("{}\n", out[0])
            } else {
                let body: Vec<String> = out.iter().map(|l| l.lines().map(|x| format!("    {x}")).collect::<Vec<_>>().join("\n")).collect();
                format!("$and\n{}\n", body.join("\n"))
            }
        }
    }
}

/// The message mongod gives when the planner fails: the query is printed in front of the reason.
pub fn planner_message(ns: &str, filter: &Document, sort: Option<&Document>, proj: Option<&Document>, reason: &str) -> String {
    let show = |d: Option<&Document>| d.map(|d| if d.is_empty() { "{}".to_string() } else { crate::update::fmt_doc(d) }).unwrap_or_else(|| "{}".to_string());
    format!("error processing query: ns={ns}Tree: {}Sort: {}\nProj: {}\n planner returned error :: caused by :: {reason}", tree(filter), show(sort), show(proj))
}

/// Add the query to the "hint provided does not correspond to an existing index" error of `resolve_hint`.
pub fn hint_error(e: MongoError, ns: &str, filter: &Document, sort: Option<&Document>, proj: Option<&Document>) -> MongoError {
    const GENERIC: &str = "error processing query: planner returned error :: caused by :: ";
    match e.msg.strip_prefix(GENERIC) {
        Some(reason) => MongoError { msg: planner_message(ns, filter, sort, proj, reason), ..e },
        None => e,
    }
}

/// The key document of an index entry for `doc`: the value of every key field (null where the document has none).
pub fn key_doc(idx: &IndexDef, d: &Document) -> Document {
    let mut out = Document::new();
    for (f, _) in idx.key_pattern() {
        out.insert(f.clone(), path_values(d, &f).into_iter().next().cloned().unwrap_or(Bson::Null));
    }
    out
}

fn cmp_keys(a: &[Bson], b: &[Bson], dirs: &[bool]) -> Ordering {
    for ((x, y), asc) in a.iter().zip(b).zip(dirs) {
        let o = compare(x, y);
        let o = if *asc { o } else { o.reverse() };
        if o != Ordering::Equal {
            return o;
        }
    }
    Ordering::Equal
}

impl Server {
    /// `find` with `min`, `max`, `returnKey` or `showRecordId`: the documents, with a `$recordId` field when asked for.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn find_ext(&self, g: &mut impl Rd, db: &str, coll: &str, filter: &Document, sort: &[(String, bool)], hint: Option<&Bson>, sort_doc: Option<&Document>, proj_doc: Option<&Document>, ov: Option<&Overlay>, ext: &ExtOpts) -> R<Vec<Document>> {
        let ns = format!("{db}.{coll}");
        let indexes = self.load_indexes(g, db, coll).await?;
        let hint = resolve_hint(&indexes, hint).map_err(|e| hint_error(e, &ns, filter, sort_doc, proj_doc))?;
        let bounded = ext.min.is_some() || ext.max.is_some();
        let mut bounds: Option<(usize, Vec<bool>)> = None;
        if bounded {
            let fail = |code: i32, name: &'static str, reason: &str| MongoError::new(code, name, planner_message(&ns, filter, sort_doc, proj_doc, reason));
            match &hint {
                Hint::Index(i) => {
                    let idx = &indexes[*i];
                    let kp = idx.key_pattern();
                    for b in [&ext.min, &ext.max].into_iter().flatten() {
                        if !b.is_empty() && b.keys().ne(kp.keys()) {
                            return Err(fail(51174, "Location51174", "The index chosen is not compatible with min/max"));
                        }
                    }
                    let dirs = kp.values().map(|d| !matches!(d, Bson::Int32(-1) | Bson::Int64(-1)) && !matches!(d, Bson::Double(f) if *f < 0.0)).collect();
                    bounds = Some((*i, dirs));
                }
                Hint::Natural(_) => return Err(fail(291, "NoQueryExecutionPlans", "min and max are incompatible with $natural")),
                Hint::None => {
                    if let Some(i) = indexes.iter().position(|_| false) {
                        let _ = i;
                    }
                    return Err(fail(51173, "Location51173", "When using min()/max() a hint of which index to use must be provided"));
                }
            }
        }
        let specs = index::ttl_specs(&indexes);
        let sort_for_plan: &[(String, bool)] = if bounded { &[] } else { sort };
        let (mut docs, _, plan) = find_docs(g, coll, &indexes, &FindOpts { filter, sort: sort_for_plan, want: None, hint: hint.clone(), ttl: crate::cmd_data::ttl_ctx(&specs), ov }).await?;
        if let Some((pos, dirs)) = &bounds {
            let idx = &indexes[*pos];
            let fields: Vec<String> = idx.key_pattern().keys().cloned().collect();
            let bound = |b: &Option<Document>| -> Option<Vec<Bson>> { b.as_ref().filter(|d| !d.is_empty()).map(|d| fields.iter().map(|f| d.get(f).cloned().unwrap_or(Bson::Null)).collect()) };
            let (lo, hi) = (bound(&ext.min), bound(&ext.max));
            if let (Some(l), Some(h)) = (&lo, &hi) {
                if cmp_keys(l, h, dirs) != Ordering::Less {
                    return Err(MongoError::new(51175, "Location51175", planner_message(&ns, filter, sort_doc, proj_doc, "The value provided for min() does not come before the value provided for max() in the hinted index")));
                }
            }
            // every index key of a document (a multikey field gives one per element)
            let keys_of = |d: &Document| -> Vec<Vec<Bson>> {
                let mut keys: Vec<Vec<Bson>> = vec![vec![]];
                for f in &fields {
                    let vals: Vec<Bson> = path_values(d, f).into_iter().flat_map(|v| match v {
                        Bson::Array(a) if !a.is_empty() => a.clone(),
                        other => vec![other.clone()],
                    }).collect();
                    let vals = if vals.is_empty() { vec![Bson::Null] } else { vals };
                    keys = keys.into_iter().flat_map(|k| vals.iter().map(move |v| { let mut k = k.clone(); k.push(v.clone()); k })).collect();
                }
                keys
            };
            let mut kept: Vec<(Vec<Bson>, Document)> = Vec::new();
            for d in docs.drain(..) {
                let mut inside: Vec<Vec<Bson>> = keys_of(&d)
                    .into_iter()
                    .filter(|k| lo.as_ref().map(|l| cmp_keys(k, l, dirs) != Ordering::Less).unwrap_or(true) && hi.as_ref().map(|h| cmp_keys(k, h, dirs) == Ordering::Less).unwrap_or(true))
                    .collect();
                inside.sort_by(|a, b| cmp_keys(a, b, dirs));
                if let Some(k) = inside.into_iter().next() {
                    kept.push((k, d));
                }
            }
            kept.sort_by(|a, b| cmp_keys(&a.0, &b.0, dirs));
            docs = kept.into_iter().map(|(_, d)| d).collect();
        }
        if ext.return_key {
            let idx_for_keys: Option<Vec<IndexDef>> = match &plan {
                Plan::Ix(s) => indexes.get(s.idx).cloned().map(|i| vec![i]),
                Plan::Id(_) => None,
                // the planner reads an index in order instead of sorting
                Plan::Coll { .. } if !sort.is_empty() && matches!(hint, Hint::None) => {
                    let same = |dirs: &[bool]| sort.iter().zip(dirs).all(|((_, a), b)| a == b);
                    indexes.iter().find(|i| {
                        let kp = i.key_pattern();
                        let fields: Vec<&String> = kp.keys().collect();
                        let dirs: Vec<bool> = kp.values().map(|d| !matches!(d, Bson::Int32(-1) | Bson::Int64(-1)) && !matches!(d, Bson::Double(f) if *f < 0.0)).collect();
                        i.state == crate::index::State::Ready
                            && matches!(i.kind, index::Kind::Regular)
                            && fields.len() >= sort.len()
                            && sort.iter().zip(&fields).all(|((f, _), g)| f == *g)
                            && (same(&dirs[..sort.len()]) || sort.iter().zip(&dirs).all(|((_, a), b)| a != b))
                    }).cloned().map(|i| vec![i])
                }
                _ => None,
            };
            let by_id = matches!(plan, Plan::Id(_));
            docs = docs
                .into_iter()
                .map(|d| match (&idx_for_keys, by_id) {
                    (Some(v), _) => key_doc(&v[0], &d),
                    (None, true) => doc! { "_id": d.get("_id").cloned().unwrap_or(Bson::Null) },
                    _ => Document::new(),
                })
                .collect();
            return Ok(docs);
        }
        if ext.show_rid {
            for d in docs.iter_mut() {
                let id = d.get("_id").cloned().unwrap_or(Bson::Null);
                if let Some(raw) = g.get(&crate::layout::doc_key(coll, &id)).await? {
                    if let Some(seq) = crate::natural::seq_of(&raw) {
                        d.insert("$recordId", Bson::Int64(seq as i64));
                    }
                }
            }
        }
        let _ = int_arg;
        Ok(docs)
    }
}
