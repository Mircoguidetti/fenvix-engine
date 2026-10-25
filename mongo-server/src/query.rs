//! MongoDB query semantics on `bson::Document`: BSON comparison order, filters, dotted paths,
//! sort, projection. Pure functions, no I/O.

use crate::error::MongoError;
use bson::{Bson, Document};
use std::cmp::Ordering;

type R<T> = Result<T, MongoError>;

// ---------------------------------------------------------------------------
// BSON comparison order
// ---------------------------------------------------------------------------

/// Canonical type rank from MongoDB's BSON comparison order.
pub fn type_rank(v: &Bson) -> u8 {
    match v {
        Bson::MinKey => 1,
        Bson::Null | Bson::Undefined => 2,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => 3,
        Bson::String(_) | Bson::Symbol(_) => 4,
        Bson::Document(_) => 5,
        Bson::Array(_) => 6,
        Bson::Binary(_) => 7,
        Bson::ObjectId(_) => 8,
        Bson::Boolean(_) => 9,
        Bson::DateTime(_) => 10,
        Bson::Timestamp(_) => 11,
        Bson::RegularExpression(_) => 12,
        Bson::DbPointer(_) => 13,
        Bson::JavaScriptCode(_) => 14,
        Bson::JavaScriptCodeWithScope(_) => 15,
        Bson::MaxKey => 20,
    }
}

pub fn is_number(v: &Bson) -> bool {
    matches!(v, Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_))
}

fn cmp_numbers(a: &Bson, b: &Bson) -> Ordering {
    match (a, b) {
        (Bson::Int32(x), Bson::Int32(y)) => x.cmp(y),
        (Bson::Int32(x), Bson::Int64(y)) => (*x as i64).cmp(y),
        (Bson::Int64(x), Bson::Int32(y)) => x.cmp(&(*y as i64)),
        (Bson::Int64(x), Bson::Int64(y)) => x.cmp(y),
        _ => {
            let (x, y) = (as_f64(a), as_f64(b));
            // NaN sorts before every other number.
            match (x.is_nan(), y.is_nan()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                _ => x.partial_cmp(&y).unwrap(),
            }
        }
    }
}

pub fn as_f64(v: &Bson) -> f64 {
    match v {
        Bson::Int32(x) => *x as f64,
        Bson::Int64(x) => *x as f64,
        Bson::Double(x) => *x,
        _ => f64::NAN,
    }
}

pub fn compare(a: &Bson, b: &Bson) -> Ordering {
    let (ra, rb) = (type_rank(a), type_rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (x, y) if is_number(x) && is_number(y) => cmp_numbers(x, y),
        (Bson::String(x) | Bson::Symbol(x), Bson::String(y) | Bson::Symbol(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Bson::Document(x), Bson::Document(y)) => {
            let mut xi = x.iter();
            let mut yi = y.iter();
            loop {
                match (xi.next(), yi.next()) {
                    (None, None) => return Ordering::Equal,
                    (None, Some(_)) => return Ordering::Less,
                    (Some(_), None) => return Ordering::Greater,
                    (Some((kx, vx)), Some((ky, vy))) => {
                        let o = type_rank(vx).cmp(&type_rank(vy))
                            .then_with(|| kx.as_bytes().cmp(ky.as_bytes()))
                            .then_with(|| compare(vx, vy));
                        if o != Ordering::Equal {
                            return o;
                        }
                    }
                }
            }
        }
        (Bson::Array(x), Bson::Array(y)) => {
            for (vx, vy) in x.iter().zip(y.iter()) {
                let o = compare(vx, vy);
                if o != Ordering::Equal {
                    return o;
                }
            }
            x.len().cmp(&y.len())
        }
        (Bson::Binary(x), Bson::Binary(y)) => x.bytes.len().cmp(&y.bytes.len())
            .then_with(|| u8::from(x.subtype).cmp(&u8::from(y.subtype)))
            .then_with(|| x.bytes.cmp(&y.bytes)),
        (Bson::ObjectId(x), Bson::ObjectId(y)) => x.bytes().cmp(&y.bytes()),
        (Bson::Boolean(x), Bson::Boolean(y)) => x.cmp(y),
        (Bson::DateTime(x), Bson::DateTime(y)) => x.timestamp_millis().cmp(&y.timestamp_millis()),
        (Bson::Timestamp(x), Bson::Timestamp(y)) => (x.time, x.increment).cmp(&(y.time, y.increment)),
        (Bson::RegularExpression(x), Bson::RegularExpression(y)) => {
            x.pattern.cmp(&y.pattern).then_with(|| x.options.cmp(&y.options))
        }
        _ => Ordering::Equal,
    }
}

pub fn equal(a: &Bson, b: &Bson) -> bool {
    compare(a, b) == Ordering::Equal
}

// ---------------------------------------------------------------------------
// Dotted paths
// ---------------------------------------------------------------------------

fn resolve<'a>(v: &'a Bson, parts: &[&str], out: &mut Vec<&'a Bson>) {
    if parts.is_empty() {
        out.push(v);
        return;
    }
    match v {
        Bson::Document(d) => {
            if let Some(x) = d.get(parts[0]) {
                resolve(x, &parts[1..], out);
            }
        }
        Bson::Array(a) => {
            if let Ok(i) = parts[0].parse::<usize>() {
                if let Some(x) = a.get(i) {
                    resolve(x, &parts[1..], out);
                }
            }
            for e in a {
                if matches!(e, Bson::Document(_)) {
                    resolve(e, parts, out);
                }
            }
        }
        _ => {}
    }
}

/// Values a dotted path resolves to (arrays are traversed; the resolved array values themselves
/// are returned, not their elements).
pub fn path_values<'a>(doc: &'a Document, path: &str) -> Vec<&'a Bson> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut out = Vec::new();
    if let Some(x) = doc.get(parts[0]) {
        resolve(x, &parts[1..], &mut out);
    }
    out
}

/// Candidates for matching: each resolved value plus, for arrays, their elements.
fn candidates<'a>(vals: &[&'a Bson]) -> Vec<&'a Bson> {
    let mut c = Vec::new();
    for v in vals {
        c.push(*v);
        if let Bson::Array(a) = v {
            c.extend(a.iter());
        }
    }
    c
}

// ---------------------------------------------------------------------------
// Filters
// ---------------------------------------------------------------------------

pub fn matches(doc: &Document, filter: &Document) -> R<bool> {
    for (k, v) in filter {
        let ok = match k.as_str() {
            "$and" | "$or" | "$nor" => {
                let arr = v.as_array().ok_or_else(|| MongoError::bad_value(format!("{k} must be an array")))?;
                if arr.is_empty() {
                    return Err(MongoError::bad_value(format!("{k} must be a nonempty array")));
                }
                let mut results = Vec::with_capacity(arr.len());
                for sub in arr {
                    let sub = sub.as_document().ok_or_else(|| MongoError::bad_value(format!("{k} argument's entries must be objects")))?;
                    results.push(matches(doc, sub)?);
                }
                match k.as_str() {
                    "$and" => results.iter().all(|x| *x),
                    "$or" => results.iter().any(|x| *x),
                    _ => !results.iter().any(|x| *x),
                }
            }
            op if op.starts_with('$') => {
                return Err(MongoError::new(2, "BadValue", format!("unknown top level operator: {op}")));
            }
            path => match_field(doc, path, v)?,
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
}

fn is_operator_doc(d: &Document) -> bool {
    !d.is_empty() && d.keys().all(|k| k.starts_with('$'))
}

fn match_field(doc: &Document, path: &str, cond: &Bson) -> R<bool> {
    let vals = path_values(doc, path);
    match cond {
        Bson::Document(d) if is_operator_doc(d) => {
            for (op, arg) in d {
                if !match_op(&vals, op, arg)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Bson::RegularExpression(_) => Err(MongoError::new(2, "BadValue", "regular expression matching is not supported by this server".into())),
        other => Ok(eq_match(&vals, other)),
    }
}

fn eq_match(vals: &[&Bson], operand: &Bson) -> bool {
    if matches!(operand, Bson::Null | Bson::Undefined) {
        // null matches a missing field and a field whose value (or an array element) is null.
        return vals.is_empty() || candidates(vals).iter().any(|c| matches!(c, Bson::Null | Bson::Undefined));
    }
    candidates(vals).iter().any(|c| equal(c, operand))
}

fn same_bracket(a: &Bson, b: &Bson) -> bool {
    type_rank(a) == type_rank(b)
}

fn match_op(vals: &[&Bson], op: &str, arg: &Bson) -> R<bool> {
    Ok(match op {
        "$eq" => eq_match(vals, arg),
        "$ne" => !eq_match(vals, arg),
        "$gt" | "$gte" | "$lt" | "$lte" => {
            if matches!(arg, Bson::Null) {
                // $gte/$lte null match null and missing; $gt/$lt null match nothing.
                return Ok(matches!(op, "$gte" | "$lte") && eq_match(vals, arg));
            }
            candidates(vals).iter().any(|c| {
                same_bracket(c, arg) && {
                    let o = compare(c, arg);
                    match op {
                        "$gt" => o == Ordering::Greater,
                        "$gte" => o != Ordering::Less,
                        "$lt" => o == Ordering::Less,
                        _ => o != Ordering::Greater,
                    }
                }
            })
        }
        "$in" | "$nin" => {
            let list = arg.as_array().ok_or_else(|| MongoError::bad_value(format!("{op} needs an array")))?;
            let any = list.iter().any(|x| eq_match(vals, x));
            if op == "$in" { any } else { !any }
        }
        "$exists" => {
            let want = match arg {
                Bson::Boolean(b) => *b,
                Bson::Int32(i) => *i != 0,
                Bson::Int64(i) => *i != 0,
                Bson::Double(f) => *f != 0.0,
                _ => true,
            };
            vals.is_empty() != want
        }
        "$not" => {
            let d = arg.as_document().filter(|d| is_operator_doc(d))
                .ok_or_else(|| MongoError::bad_value("$not needs a document of operators".into()))?;
            let mut all = true;
            for (o, a) in d {
                if !match_op(vals, o, a)? {
                    all = false;
                    break;
                }
            }
            !all
        }
        other => return Err(MongoError::new(2, "BadValue", format!("unknown operator: {other}"))),
    })
}

/// If the filter is exactly an equality on `_id` (`{_id: v}` or `{_id: {$eq: v}}`), return `v`.
pub fn id_equality(filter: &Document) -> Option<&Bson> {
    if filter.len() != 1 {
        return None;
    }
    let v = filter.get("_id")?;
    match v {
        Bson::Document(d) if is_operator_doc(d) => {
            if d.len() == 1 { d.get("$eq") } else { None }
        }
        Bson::RegularExpression(_) | Bson::Null | Bson::Undefined => None,
        other => Some(other),
    }
}

// ---------------------------------------------------------------------------
// Sort
// ---------------------------------------------------------------------------

fn sort_key(doc: &Document, path: &str, ascending: bool) -> Bson {
    let vals = path_values(doc, path);
    if vals.is_empty() {
        return Bson::Null;
    }
    // Arrays sort by their smallest (ascending) or largest (descending) element.
    let mut flat: Vec<&Bson> = Vec::new();
    for v in &vals {
        match v {
            Bson::Array(a) if !a.is_empty() => flat.extend(a.iter()),
            // An empty array sorts below null, in both directions.
            Bson::Array(_) => flat.push(&Bson::MinKey),
            other => flat.push(other),
        }
    }
    let pick = if ascending {
        flat.into_iter().min_by(|a, b| compare(a, b))
    } else {
        flat.into_iter().max_by(|a, b| compare(a, b))
    };
    pick.cloned().unwrap_or(Bson::Null)
}

pub fn parse_sort(spec: &Document) -> R<Vec<(String, bool)>> {
    let mut out = Vec::new();
    for (k, v) in spec {
        let dir = match v {
            Bson::Int32(i) => *i as i64,
            Bson::Int64(i) => *i,
            Bson::Double(f) => *f as i64,
            _ => return Err(MongoError::new(15974, "Location15974", "$meta sort is not supported by this server".into())),
        };
        if dir != 1 && dir != -1 {
            return Err(MongoError::new(15975, "Location15975", "$sort key ordering must be 1 (for ascending) or -1 (for descending)".into()));
        }
        out.push((k.clone(), dir == 1));
    }
    Ok(out)
}

/// Stable sort of documents by a sort specification.
pub fn sort_docs(docs: &mut Vec<Document>, spec: &[(String, bool)]) {
    if spec.is_empty() {
        return;
    }
    let mut keyed: Vec<(Vec<Bson>, Document)> = docs
        .drain(..)
        .map(|d| (spec.iter().map(|(p, asc)| sort_key(&d, p, *asc)).collect(), d))
        .collect();
    keyed.sort_by(|(a, _), (b, _)| {
        for (i, (_, asc)) in spec.iter().enumerate() {
            let o = compare(&a[i], &b[i]);
            let o = if *asc { o } else { o.reverse() };
            if o != Ordering::Equal {
                return o;
            }
        }
        Ordering::Equal
    });
    docs.extend(keyed.into_iter().map(|(_, d)| d));
}

// ---------------------------------------------------------------------------
// Projection
// ---------------------------------------------------------------------------

fn truthy(v: &Bson) -> Option<bool> {
    match v {
        Bson::Boolean(b) => Some(*b),
        Bson::Int32(i) => Some(*i != 0),
        Bson::Int64(i) => Some(*i != 0),
        Bson::Double(f) => Some(*f != 0.0),
        _ => None,
    }
}

pub struct Projection {
    include: bool,
    id: bool,
    paths: Vec<Vec<String>>,
}

pub fn parse_projection(spec: &Document) -> R<Option<Projection>> {
    if spec.is_empty() {
        return Ok(None);
    }
    let mut id = true;
    let mut inc = Vec::new();
    let mut exc = Vec::new();
    for (k, v) in spec {
        let t = truthy(v).ok_or_else(|| MongoError::new(2, "BadValue", "projection expressions are not supported by this server".into()))?;
        if k == "_id" {
            id = t;
        } else if t {
            inc.push(k.split('.').map(str::to_string).collect::<Vec<_>>());
        } else {
            exc.push(k.split('.').map(str::to_string).collect::<Vec<_>>());
        }
    }
    if !inc.is_empty() && !exc.is_empty() {
        return Err(MongoError::new(31254, "Location31254", "Cannot do exclusion on field in inclusion projection".into()));
    }
    if inc.is_empty() && exc.is_empty() {
        // Only `_id` was given: {_id: 1} keeps just the _id, {_id: 0} drops just the _id.
        return Ok(Some(Projection { include: id, id, paths: vec![] }));
    }
    let include = !inc.is_empty();
    Ok(Some(Projection { include, id, paths: if include { inc } else { exc } }))
}

fn include_into(src: &Document, paths: &[&[String]]) -> Document {
    let mut out = Document::new();
    for (k, v) in src {
        // Paths whose first component is k.
        let sub: Vec<&[String]> = paths.iter().filter(|p| p[0] == *k).map(|p| &p[..]).collect();
        if sub.is_empty() {
            continue;
        }
        if sub.iter().any(|p| p.len() == 1) {
            out.insert(k.clone(), v.clone());
            continue;
        }
        let rest: Vec<&[String]> = sub.iter().map(|p| &p[1..]).collect();
        match v {
            Bson::Document(d) => {
                out.insert(k.clone(), Bson::Document(include_into(d, &rest)));
            }
            Bson::Array(a) => {
                let arr: Vec<Bson> = a.iter().filter_map(|e| match e {
                    Bson::Document(d) => Some(Bson::Document(include_into(d, &rest))),
                    _ => None,
                }).collect();
                out.insert(k.clone(), Bson::Array(arr));
            }
            _ => {}
        }
    }
    out
}

fn exclude_from(src: &Document, paths: &[&[String]]) -> Document {
    let mut out = Document::new();
    for (k, v) in src {
        let sub: Vec<&[String]> = paths.iter().filter(|p| p[0] == *k).map(|p| &p[..]).collect();
        if sub.is_empty() {
            out.insert(k.clone(), v.clone());
            continue;
        }
        if sub.iter().any(|p| p.len() == 1) {
            continue;
        }
        let rest: Vec<&[String]> = sub.iter().map(|p| &p[1..]).collect();
        match v {
            Bson::Document(d) => {
                out.insert(k.clone(), Bson::Document(exclude_from(d, &rest)));
            }
            Bson::Array(a) => {
                let arr: Vec<Bson> = a.iter().map(|e| match e {
                    Bson::Document(d) => Bson::Document(exclude_from(d, &rest)),
                    other => other.clone(),
                }).collect();
                out.insert(k.clone(), Bson::Array(arr));
            }
            other => {
                out.insert(k.clone(), other.clone());
            }
        }
    }
    out
}

pub fn project(doc: &Document, p: &Projection) -> Document {
    let paths: Vec<&[String]> = p.paths.iter().map(|v| &v[..]).collect();
    let body = if p.include { include_into(doc, &paths) } else { exclude_from(doc, &paths) };
    let mut out = Document::new();
    if p.id {
        if let Some(id) = doc.get("_id") {
            out.insert("_id", id.clone());
        }
    }
    for (k, v) in body {
        if k != "_id" {
            out.insert(k, v);
        }
    }
    // An exclusion projection with `_id` kept: fields stay in their original order after _id.
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn m(d: Document, f: Document) -> bool {
        matches(&d, &f).unwrap()
    }

    #[test]
    fn numbers_compare_across_types() {
        assert!(equal(&Bson::Int32(1), &Bson::Double(1.0)));
        assert!(equal(&Bson::Int64(2), &Bson::Int32(2)));
        assert_eq!(compare(&Bson::Int32(1), &Bson::Double(1.5)), Ordering::Less);
    }

    #[test]
    fn mixed_type_order() {
        let order = [Bson::Null, Bson::Int32(5), Bson::String("a".into()), Bson::Document(doc! {}), Bson::Array(vec![]), Bson::Boolean(false)];
        for w in order.windows(2) {
            assert_eq!(compare(&w[0], &w[1]), Ordering::Less);
        }
    }

    #[test]
    fn type_bracketing() {
        assert!(!m(doc! {"a": "x"}, doc! {"a": {"$gt": 5}}));
        assert!(m(doc! {"a": 6}, doc! {"a": {"$gt": 5}}));
        assert!(!m(doc! {"a": Bson::Null}, doc! {"a": {"$gt": 5}}));
    }

    #[test]
    fn arrays_and_paths() {
        assert!(m(doc! {"a": [1, 2, 3]}, doc! {"a": 2}));
        assert!(m(doc! {"a": [1, 2, 3]}, doc! {"a": {"$gt": 2}}));
        assert!(m(doc! {"a": [{"b": 1}, {"b": 2}]}, doc! {"a.b": 2}));
        assert!(m(doc! {"a": {"b": {"c": 3}}}, doc! {"a.b.c": 3}));
        assert!(m(doc! {"a": [1, 2]}, doc! {"a": [1, 2]}));
        assert!(!m(doc! {"a": [1, 2]}, doc! {"a": [2, 1]}));
        assert!(m(doc! {"a": [5, 6]}, doc! {"a.1": 6}));
    }

    #[test]
    fn null_and_exists() {
        assert!(m(doc! {"b": 1}, doc! {"a": Bson::Null}));
        assert!(m(doc! {"a": Bson::Null}, doc! {"a": Bson::Null}));
        assert!(!m(doc! {"a": 1}, doc! {"a": Bson::Null}));
        assert!(m(doc! {"a": Bson::Null}, doc! {"a": {"$exists": true}}));
        assert!(m(doc! {"b": 1}, doc! {"a": {"$exists": false}}));
        assert!(m(doc! {"b": 1}, doc! {"a": {"$ne": 5}}));
    }

    #[test]
    fn logic() {
        let d = doc! {"a": 1, "b": 2};
        assert!(m(d.clone(), doc! {"$or": [{"a": 9}, {"b": 2}]}));
        assert!(!m(d.clone(), doc! {"$and": [{"a": 1}, {"b": 3}]}));
        assert!(m(d.clone(), doc! {"a": {"$not": {"$gt": 5}}}));
        assert!(m(d, doc! {"a": {"$in": [0, 1]}, "b": {"$nin": [5]}}));
    }

    #[test]
    fn sort_and_projection() {
        let mut docs = vec![doc! {"_id": 1, "a": 3}, doc! {"_id": 2, "a": 1}, doc! {"_id": 3}];
        sort_docs(&mut docs, &[("a".into(), true)]);
        assert_eq!(docs.iter().map(|d| d.get_i32("_id").unwrap()).collect::<Vec<_>>(), vec![3, 2, 1]);
        let p = parse_projection(&doc! {"a": 1}).unwrap().unwrap();
        assert_eq!(project(&doc! {"_id": 1, "a": 2, "b": 3}, &p), doc! {"_id": 1, "a": 2});
        let p = parse_projection(&doc! {"a": 0, "_id": 0}).unwrap().unwrap();
        assert_eq!(project(&doc! {"_id": 1, "a": 2, "b": 3}, &p), doc! {"b": 3});
    }
}
