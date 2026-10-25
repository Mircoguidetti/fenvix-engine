//! Aggregation pipeline: $match, $group ($sum, $avg, $min, $max, $count, $first, $last, $push),
//! $sort, $limit, $skip, $project, $count. Stages run over documents already in memory.

use crate::error::MongoError;
use crate::layout::encode_sortable;
use crate::query::{self, compare, is_number, matches, parse_projection, parse_sort, path_values, project, sort_docs};
use bson::{Bson, Document};
use std::collections::HashMap;

type R<T> = Result<T, MongoError>;

fn add_numbers(a: &Bson, b: &Bson) -> Bson {
    match (a, b) {
        (Bson::Int32(x), Bson::Int32(y)) => match x.checked_add(*y) {
            Some(s) => Bson::Int32(s),
            None => Bson::Int64(*x as i64 + *y as i64),
        },
        (Bson::Int32(_) | Bson::Int64(_), Bson::Int32(_) | Bson::Int64(_)) => {
            let (x, y) = (int(a), int(b));
            x.checked_add(y).map(Bson::Int64).unwrap_or(Bson::Double(x as f64 + y as f64))
        }
        _ => Bson::Double(query::as_f64(a) + query::as_f64(b)),
    }
}

fn int(v: &Bson) -> i64 {
    match v {
        Bson::Int32(x) => *x as i64,
        Bson::Int64(x) => *x,
        _ => 0,
    }
}

/// Evaluate an aggregation expression against a document; `None` means "missing".
fn eval(expr: &Bson, doc: &Document) -> R<Option<Bson>> {
    Ok(match expr {
        Bson::String(s) if s.starts_with("$$") => {
            return Err(MongoError::new(2, "BadValue", format!("aggregation variables are not supported: {s}")));
        }
        Bson::String(s) if s.starts_with('$') => path_values(doc, &s[1..]).first().cloned().cloned(),
        Bson::Document(d) => {
            if d.keys().next().map(|k| k.starts_with('$')).unwrap_or(false) {
                let op = d.keys().next().unwrap();
                return Err(MongoError::new(168, "InvalidPipelineOperator", format!("Unrecognized expression '{op}'")));
            }
            let mut out = Document::new();
            for (k, v) in d {
                if let Some(x) = eval(v, doc)? {
                    out.insert(k.clone(), x);
                }
            }
            Some(Bson::Document(out))
        }
        Bson::Array(a) => {
            let mut out = Vec::new();
            for v in a {
                out.push(eval(v, doc)?.unwrap_or(Bson::Null));
            }
            Some(Bson::Array(out))
        }
        other => Some(other.clone()),
    })
}

enum Acc {
    Sum(Bson),
    Avg(f64, i64),
    Min(Option<Bson>),
    Max(Option<Bson>),
    Count(i64),
    First(Option<Bson>, bool),
    Last(Bson),
    Push(Vec<Bson>),
}

struct AccSpec {
    name: String,
    op: String,
    expr: Bson,
}

fn parse_accumulators(spec: &Document) -> R<Vec<AccSpec>> {
    let mut out = Vec::new();
    for (name, v) in spec {
        if name == "_id" {
            continue;
        }
        let d = v.as_document().filter(|d| d.len() == 1).ok_or_else(|| {
            MongoError::new(40234, "Location40234", format!("The field '{name}' must be an accumulator object"))
        })?;
        let (op, expr) = d.iter().next().unwrap();
        if !matches!(op.as_str(), "$sum" | "$avg" | "$min" | "$max" | "$count" | "$first" | "$last" | "$push") {
            return Err(MongoError::new(15952, "Location15952", format!("unknown group operator '{op}'")));
        }
        out.push(AccSpec { name: name.clone(), op: op.clone(), expr: expr.clone() });
    }
    Ok(out)
}

fn init(op: &str) -> Acc {
    match op {
        "$sum" => Acc::Sum(Bson::Int32(0)),
        "$avg" => Acc::Avg(0.0, 0),
        "$min" => Acc::Min(None),
        "$max" => Acc::Max(None),
        "$count" => Acc::Count(0),
        "$first" => Acc::First(None, false),
        "$last" => Acc::Last(Bson::Null),
        _ => Acc::Push(Vec::new()),
    }
}

fn update_acc(acc: &mut Acc, spec: &AccSpec, doc: &Document) -> R<()> {
    let val = if spec.op == "$count" { None } else { eval(&spec.expr, doc)? };
    match acc {
        Acc::Sum(s) => {
            if let Some(v) = val.filter(is_number) {
                *s = add_numbers(s, &v);
            }
        }
        Acc::Avg(sum, n) => {
            if let Some(v) = val.filter(is_number) {
                *sum += query::as_f64(&v);
                *n += 1;
            }
        }
        Acc::Min(cur) => {
            if let Some(v) = val.filter(|v| !matches!(v, Bson::Null | Bson::Undefined)) {
                if cur.as_ref().map(|c| compare(&v, c).is_lt()).unwrap_or(true) {
                    *cur = Some(v);
                }
            }
        }
        Acc::Max(cur) => {
            if let Some(v) = val.filter(|v| !matches!(v, Bson::Null | Bson::Undefined)) {
                if cur.as_ref().map(|c| compare(&v, c).is_gt()).unwrap_or(true) {
                    *cur = Some(v);
                }
            }
        }
        Acc::Count(n) => *n += 1,
        Acc::First(cur, set) => {
            if !*set {
                *cur = Some(val.unwrap_or(Bson::Null));
                *set = true;
            }
        }
        Acc::Last(cur) => *cur = val.unwrap_or(Bson::Null),
        Acc::Push(v) => {
            if let Some(x) = val {
                v.push(x);
            }
        }
    }
    Ok(())
}

fn finish(acc: Acc) -> Bson {
    match acc {
        Acc::Sum(s) => s,
        Acc::Avg(sum, n) => {
            if n == 0 { Bson::Null } else { Bson::Double(sum / n as f64) }
        }
        Acc::Min(v) | Acc::Max(v) => v.unwrap_or(Bson::Null),
        Acc::Count(n) => Bson::Int32(n as i32),
        Acc::First(v, _) => v.unwrap_or(Bson::Null),
        Acc::Last(v) => v,
        Acc::Push(v) => Bson::Array(v),
    }
}

fn group(docs: Vec<Document>, spec: &Document) -> R<Vec<Document>> {
    let id_expr = spec.get("_id").ok_or_else(|| MongoError::new(15955, "Location15955", "a group specification must include an _id".into()))?;
    let accs = parse_accumulators(spec)?;
    let mut order: Vec<Vec<u8>> = Vec::new();
    let mut groups: HashMap<Vec<u8>, (Bson, Vec<Acc>)> = HashMap::new();
    for d in &docs {
        let key = eval(id_expr, d)?.unwrap_or(Bson::Null);
        let kb = encode_sortable(&key);
        if !groups.contains_key(&kb) {
            order.push(kb.clone());
            groups.insert(kb.clone(), (key, accs.iter().map(|a| init(&a.op)).collect()));
        }
        let (_, state) = groups.get_mut(&kb).unwrap();
        for (acc, spec) in state.iter_mut().zip(&accs) {
            update_acc(acc, spec, d)?;
        }
    }
    let mut out = Vec::with_capacity(order.len());
    for kb in order {
        let (key, state) = groups.remove(&kb).unwrap();
        let mut d = Document::new();
        d.insert("_id", key);
        for (acc, spec) in state.into_iter().zip(&accs) {
            d.insert(spec.name.clone(), finish(acc));
        }
        out.push(d);
    }
    Ok(out)
}

fn computed_projection(docs: Vec<Document>, spec: &Document) -> R<Vec<Document>> {
    let mut out = Vec::with_capacity(docs.len());
    for d in docs {
        let mut r = Document::new();
        let id_off = matches!(spec.get("_id"), Some(Bson::Int32(0)) | Some(Bson::Boolean(false)) | Some(Bson::Int64(0)));
        if !id_off {
            if let Some(id) = d.get("_id") {
                r.insert("_id", id.clone());
            }
        }
        for (k, v) in spec {
            if k == "_id" {
                continue;
            }
            match v {
                Bson::String(s) if s.starts_with('$') => {
                    if let Some(x) = eval(v, &d)? {
                        r.insert(k.clone(), x);
                    }
                }
                Bson::Document(_) => {
                    if let Some(x) = eval(v, &d)? {
                        r.insert(k.clone(), x);
                    }
                }
                other if is_number(other) || matches!(other, Bson::Boolean(_)) => {
                    let keep = match other {
                        Bson::Boolean(b) => *b,
                        o => query::as_f64(o) != 0.0,
                    };
                    if keep {
                        if let Some(x) = path_values(&d, k).first() {
                            r.insert(k.clone(), (*x).clone());
                        }
                    }
                }
                other => r.insert(k.clone(), other.clone()).map(|_| ()).unwrap_or(()),
            }
        }
        out.push(r);
    }
    Ok(out)
}

fn positive_int(v: &Bson, stage: &str) -> R<usize> {
    let code = if stage == "$skip" { 5107200 } else { 5107201 };
    let shown = crate::update::fmt_bson(v);
    let n = match v {
        Bson::Int32(i) => *i as i64,
        Bson::Int64(i) => *i,
        Bson::Double(f) if f.fract() == 0.0 => *f as i64,
        Bson::Double(_) => return Err(MongoError::new(code, if code == 5107200 { "Location5107200" } else { "Location5107201" }, format!("invalid argument to {stage} stage: Expected an integer: {stage}: {shown}"))),
        _ => return Err(MongoError::new(code, if code == 5107200 { "Location5107200" } else { "Location5107201" }, format!("invalid argument to {stage} stage: Expected a number in: {stage}: {shown}"))),
    };
    if n < 0 {
        return Err(MongoError::new(code, if code == 5107200 { "Location5107200" } else { "Location5107201" }, format!("invalid argument to {stage} stage: Expected a non-negative number in: {stage}: {shown}")));
    }
    if stage == "$limit" && n == 0 {
        return Err(MongoError::new(15958, "Location15958", "the limit must be positive".into()));
    }
    Ok(n as usize)
}

pub fn run_pipeline(mut docs: Vec<Document>, pipeline: &[Bson]) -> R<Vec<Document>> {
    for stage in pipeline {
        let sd = stage.as_document().filter(|d| d.len() == 1).ok_or_else(|| {
            MongoError::new(40323, "Location40323", "A pipeline stage specification object must contain exactly one field.".into())
        })?;
        let (name, arg) = sd.iter().next().unwrap();
        match name.as_str() {
            "$match" => {
                let f = arg.as_document().ok_or_else(|| MongoError::new(15959, "Location15959", "the match filter must be an expression in an object".into()))?;
                let mut kept = Vec::new();
                for d in docs {
                    if matches(&d, f)? {
                        kept.push(d);
                    }
                }
                docs = kept;
            }
            "$sort" => {
                let spec = arg.as_document().ok_or_else(|| MongoError::new(15973, "Location15973", "the $sort key specification must be an object".into()))?;
                sort_docs(&mut docs, &parse_sort(spec)?);
            }
            "$limit" => docs.truncate(positive_int(arg, "$limit")?),
            "$skip" => {
                let n = positive_int(arg, "$skip")?;
                docs = docs.into_iter().skip(n).collect();
            }
            "$project" => {
                let spec = arg.as_document().ok_or_else(|| MongoError::new(15969, "Location15969", "$project specification must be an object".into()))?;
                let computed = spec.values().any(|v| matches!(v, Bson::String(s) if s.starts_with('$')) || matches!(v, Bson::Document(_)));
                if computed {
                    docs = computed_projection(docs, spec)?;
                } else if let Some(p) = parse_projection(spec)? {
                    docs = docs.iter().map(|d| project(d, &p)).collect();
                }
            }
            "$group" => {
                let spec = arg.as_document().ok_or_else(|| MongoError::new(15947, "Location15947", "a group's fields must be specified in an object".into()))?;
                docs = group(docs, spec)?;
            }
            "$count" => {
                let field = arg.as_str().filter(|s| !s.is_empty() && !s.starts_with('$') && !s.contains('.')).ok_or_else(|| {
                    MongoError::new(40156, "Location40156", "the count field must be a non-empty string".into())
                })?;
                let n = docs.len();
                docs = if n == 0 { vec![] } else { vec![bson::doc! { field: Bson::Int32(n as i32) }] };
            }
            other => {
                return Err(MongoError::new(40324, "Location40324", format!("Unrecognized pipeline stage name: '{other}'")));
            }
        }
    }
    Ok(docs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn run(docs: Vec<Document>, p: Vec<Document>) -> Vec<Document> {
        run_pipeline(docs, &p.into_iter().map(Bson::Document).collect::<Vec<_>>()).unwrap()
    }

    #[test]
    fn group_accumulators() {
        let docs = vec![doc! {"k": "a", "v": 1}, doc! {"k": "a", "v": 3}, doc! {"k": "b", "v": 5}, doc! {"k": "b"}];
        let out = run(docs, vec![doc! {"$group": {"_id": "$k", "s": {"$sum": "$v"}, "n": {"$sum": 1}, "a": {"$avg": "$v"}, "mn": {"$min": "$v"}, "mx": {"$max": "$v"}, "c": {"$count": {}}}}]);
        assert_eq!(out[0], doc! {"_id": "a", "s": 4, "n": 2, "a": 2.0, "mn": 1, "mx": 3, "c": 2});
        assert_eq!(out[1], doc! {"_id": "b", "s": 5, "n": 2, "a": 5.0, "mn": 5, "mx": 5, "c": 2});
    }

    #[test]
    fn count_via_match_group() {
        let docs = vec![doc! {"a": 1}, doc! {"a": 2}, doc! {"a": 3}];
        let out = run(docs, vec![doc! {"$match": {"a": {"$gt": 1}}}, doc! {"$group": {"_id": 1, "n": {"$sum": 1}}}]);
        assert_eq!(out, vec![doc! {"_id": 1, "n": 2}]);
    }
}
