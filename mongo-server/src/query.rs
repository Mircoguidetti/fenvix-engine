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

/// Numbers of any type, Decimal128 included (comparison, equality and sorting).
pub fn is_numeric(v: &Bson) -> bool {
    matches!(v, Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_))
}

fn cmp_numbers(a: &Bson, b: &Bson) -> Ordering {
    match (a, b) {
        (Bson::Int32(x), Bson::Int32(y)) => x.cmp(y),
        (Bson::Int32(x), Bson::Int64(y)) => (*x as i64).cmp(y),
        (Bson::Int64(x), Bson::Int32(y)) => x.cmp(&(*y as i64)),
        (Bson::Int64(x), Bson::Int64(y)) => x.cmp(y),
        (Bson::Int32(_) | Bson::Int64(_), Bson::Double(f)) => cmp_int_f64(int_of(a), *f),
        (Bson::Double(f), Bson::Int32(_) | Bson::Int64(_)) => cmp_int_f64(int_of(b), *f).reverse(),
        // a Decimal128 against any number: exactly, as mongod (a double 0.1 is not the decimal 0.1, and 1E+400 is a finite
        // number, not infinity: fix-query-defects P8). Through f64 both were equal.
        (Bson::Decimal128(_), _) | (_, Bson::Decimal128(_)) => exact_num(a).cmp(&exact_num(b)),
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

/// A number as an exact value, for comparisons with a Decimal128. NaN sorts first, as everywhere else.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum ExactNum {
    NaN,
    NegInf,
    Fin(bigdecimal::BigDecimal),
    PosInf,
}

fn exact_num(v: &Bson) -> ExactNum {
    use std::str::FromStr;
    let of_f64 = |f: f64| {
        if f.is_nan() {
            ExactNum::NaN
        } else if f.is_infinite() {
            if f > 0.0 { ExactNum::PosInf } else { ExactNum::NegInf }
        } else {
            bigdecimal::BigDecimal::try_from(f).map(ExactNum::Fin).unwrap_or(ExactNum::NaN)
        }
    };
    match v {
        Bson::Int32(x) => ExactNum::Fin(bigdecimal::BigDecimal::from(*x)),
        Bson::Int64(x) => ExactNum::Fin(bigdecimal::BigDecimal::from(*x)),
        Bson::Double(f) => of_f64(*f),
        Bson::Decimal128(d) => {
            let t = d.to_string();
            match t.as_str() {
                "Infinity" | "+Infinity" => ExactNum::PosInf,
                "-Infinity" => ExactNum::NegInf,
                _ if t.ends_with("NaN") => ExactNum::NaN,
                _ => bigdecimal::BigDecimal::from_str(&t).map(ExactNum::Fin).unwrap_or(ExactNum::NaN),
            }
        }
        _ => ExactNum::NaN,
    }
}

/// How a Decimal128 differs from its nearest double `f`, for its key (`layout::encode_sortable`): `(Some(i), None)` when
/// it is exactly the 64-bit integer `i` but not the double (its key is the integer's); `(None, None)` when it is exactly
/// `f` (or NaN or an infinity); `(None, Some(text))` otherwise, with its exact value, normalized (1.0 and 1.00 are one).
pub fn decimal_key_tail(v: &Bson, f: f64) -> (Option<i64>, Option<String>) {
    // NaN and the infinities are the double ones
    let ExactNum::Fin(d) = exact_num(v) else { return (None, None) };
    if f.is_infinite() {
        // a finite decimal past the double range (its nearest double is an infinity)
        return (None, Some(d.normalized().to_string()));
    }
    if let Ok(fd) = bigdecimal::BigDecimal::try_from(f) {
        if fd == d {
            return (None, None);
        }
    }
    if d.is_integer() {
        if let Some(i) = bigdecimal::ToPrimitive::to_i64(&d) {
            return (Some(i), None);
        }
    }
    (None, Some(d.normalized().to_string()))
}

fn int_of(v: &Bson) -> i64 {
    match v {
        Bson::Int32(x) => *x as i64,
        Bson::Int64(x) => *x,
        _ => 0,
    }
}

/// Exact comparison of an integer with a double (a 64-bit integer does not fit a double).
fn cmp_int_f64(i: i64, f: f64) -> Ordering {
    if f.is_nan() {
        return Ordering::Greater;
    }
    if f >= 9223372036854775808.0 {
        return Ordering::Less;
    }
    if f < -9223372036854775808.0 {
        return Ordering::Greater;
    }
    let t = f.trunc();
    match i.cmp(&(t as i64)) {
        Ordering::Equal => 0.0_f64.partial_cmp(&(f - t)).unwrap_or(Ordering::Equal),
        o => o,
    }
}

fn is_nan_num(v: &Bson) -> bool {
    is_numeric(v) && as_f64(v).is_nan()
}

pub fn as_f64(v: &Bson) -> f64 {
    match v {
        Bson::Int32(x) => *x as f64,
        Bson::Int64(x) => *x as f64,
        Bson::Double(x) => *x,
        Bson::Decimal128(d) => d.to_string().parse().unwrap_or(f64::NAN),
        _ => f64::NAN,
    }
}

pub fn compare(a: &Bson, b: &Bson) -> Ordering {
    let (ra, rb) = (type_rank(a), type_rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (x, y) if is_numeric(x) && is_numeric(y) => cmp_numbers(x, y),
        (Bson::String(x) | Bson::Symbol(x), Bson::String(y) | Bson::Symbol(y)) => crate::collation::cmp_current(x, y).unwrap_or_else(|| x.as_bytes().cmp(y.as_bytes())),
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

/// Whether mongod answers `filter` from the `_id` index (and so, without a sort, in `_id` order): an `_id` condition at the
/// top level or inside `$and` (any operator: `$nin`, `$ne`, `$not`, ranges, `$exists`, `$type`, `$regex`), or a `$nor`
/// with one clause on `_id` alone, or an `$or` whose clauses are all on `_id` alone. MEASURED with explain on mongod 7.0.34 (fix-query-defects P5); a `$nor` of two
/// clauses is a collection scan there.
pub fn uses_id_index(filter: &Document) -> bool {
    filter.iter().any(|(k, v)| match (k.as_str(), v) {
        ("_id", _) => true,
        ("$and", Bson::Array(a)) => a.iter().any(|s| matches!(s, Bson::Document(d) if uses_id_index(d))),
        ("$nor", Bson::Array(a)) if a.len() == 1 => matches!(&a[0], Bson::Document(d) if !d.is_empty() && d.keys().all(|k| k == "_id")),
        // an `$or` of clauses on `_id` alone: one `_id` index scan with the clauses' intervals merged
        ("$or", Bson::Array(a)) => !a.is_empty() && a.iter().all(|c| matches!(c, Bson::Document(d) if !d.is_empty() && d.keys().all(|k| k == "_id"))),
        _ => false,
    })
}

/// The bounds of `_id` range conditions (`$gt`/`$gte` and `$lt`/`$lte`) at the top level or inside `$and`, when they
/// can bound a scan of the documents in key order: one type bracket, and not null, MinKey, MaxKey, a regex, an array or
/// a document. Every document read is still matched against the whole filter: these only shorten the scan.
pub fn id_range(filter: &Document) -> Option<(Option<Bson>, Option<Bson>)> {
    fn walk(filter: &Document, lo: &mut Option<Bson>, hi: &mut Option<Bson>) {
        for (k, v) in filter {
            match (k.as_str(), v) {
                ("_id", Bson::Document(d)) if is_operator_doc(d) => {
                    for (op, x) in d {
                        match op.as_str() {
                            "$gt" | "$gte" if lo.is_none() => *lo = Some(x.clone()),
                            "$lt" | "$lte" if hi.is_none() => *hi = Some(x.clone()),
                            _ => {}
                        }
                    }
                }
                ("$and", Bson::Array(a)) => {
                    for s in a {
                        if let Bson::Document(sd) = s {
                            walk(sd, lo, hi);
                        }
                    }
                }
                _ => {}
            }
        }
    }
    let (mut lo, mut hi) = (None, None);
    walk(filter, &mut lo, &mut hi);
    let usable = |v: &Bson| !matches!(v, Bson::Null | Bson::Undefined | Bson::MinKey | Bson::MaxKey | Bson::RegularExpression(_) | Bson::Array(_) | Bson::Document(_));
    if lo.as_ref().is_some_and(|v| !usable(v)) || hi.as_ref().is_some_and(|v| !usable(v)) || (lo.is_none() && hi.is_none()) {
        return None;
    }
    if let (Some(a), Some(b)) = (&lo, &hi) {
        if type_rank(a) != type_rank(b) {
            return None;
        }
    }
    Some((lo, hi))
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
    let expand = EXPAND.with(|e| e.get());
    let mut c = Vec::new();
    for v in vals {
        c.push(*v);
        if expand {
            if let Bson::Array(a) = v {
                c.extend(a.iter());
            }
        }
    }
    c
}

thread_local! {
    /// False while `$elemMatch` applies operators to one array element: the element itself is the
    /// value, an array element is not searched again.
    static EXPAND: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}

// ---------------------------------------------------------------------------
// Filters
// ---------------------------------------------------------------------------

pub fn matches(doc: &Document, filter: &Document) -> R<bool> {
    // `$expr` runs after the cheaper conditions, as MongoDB orders it.
    for (k, v) in filter.iter().filter(|(k, _)| k.as_str() != "$expr").chain(filter.iter().filter(|(k, _)| k.as_str() == "$expr")) {
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
            "$comment" => true,
            "$sampleRate" => {
                let r = match v {
                    x if is_numeric(x) => as_f64(x),
                    _ => return Err(MongoError::new(2, "BadValue", "numeric argument expected for $sampleRate".to_string())),
                };
                if !(0.0..=1.0).contains(&r) {
                    return Err(MongoError::new(2, "BadValue", "numeric argument to $sampleRate must be in the range [0, 1]".to_string()));
                }
                rand::random::<f64>() < r
            }
            "$jsonSchema" => match v {
                Bson::Document(schema) => crate::json_schema::validate(&Bson::Document(doc.clone()), schema)?,
                _ => return Err(MongoError::new(14, "TypeMismatch", "$jsonSchema must be an object".to_string())),
            },
            "$text" => crate::text::matches_operand(v, doc)?,
            "$where" => crate::js::eval_where(v, doc)?,
            "$expr" => {
                crate::expr::validate(v)?;
                crate::expr::truthy(crate::expr::eval(v, &crate::expr::Scope::new(doc))?.as_ref())
            }
            op if op.starts_with('$') => {
                return Err(MongoError::new(2, "BadValue", format!("unknown top level operator: {op}. If you have a field name that starts with a '$' symbol, consider using $getField or $setField.")));
            }
            path => match_field(doc, path, v)?,
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Check the operators of a filter without a document to match against: mongod parses a filter before it looks at the
/// collection, so a bad operator fails even on a collection that does not exist or is empty.
pub fn probe_filter(filter: &Document) -> R<()> {
    let empty = Document::new();
    for (k, v) in filter {
        match k.as_str() {
            "$and" | "$or" | "$nor" => {
                let arr = v.as_array().ok_or_else(|| MongoError::bad_value(format!("{k} must be an array")))?;
                if arr.is_empty() {
                    return Err(MongoError::bad_value(format!("{k} must be a nonempty array")));
                }
                for sub in arr {
                    let sub = sub.as_document().ok_or_else(|| MongoError::bad_value(format!("{k} argument's entries must be objects")))?;
                    probe_filter(sub)?;
                }
            }
            "$where" | "$text" | "$jsonSchema" | "$comment" | "$sampleRate" => {}
            "$expr" => crate::expr::validate(v)?,
            _ => match v {
                Bson::Document(d) if is_operator_doc(d) && !d.keys().any(|o| matches!(o.as_str(), "$near" | "$nearSphere" | "$geoWithin" | "$within" | "$geoIntersects" | "$elemMatch" | "$not" | "$regex" | "$options")) => {
                    for (op, arg) in d {
                        matches(&empty, &Document::from_iter([(k.clone(), Bson::Document(Document::from_iter([(op.clone(), arg.clone())])))]))?;
                    }
                }
                _ => {
                    if k.starts_with('$') {
                        matches(&empty, &Document::from_iter([(k.clone(), v.clone())]))?;
                    }
                }
            },
        }
    }
    Ok(())
}

fn is_operator_doc(d: &Document) -> bool {
    !d.is_empty() && d.keys().all(|k| k.starts_with('$'))
}

/// Does every value of `path` come out of an array by position (`a.0` where `a[0]` is itself an array)? Such an array is
/// the value as it is: its elements are not candidates of their own.
fn reached_by_position(doc: &Document, path: &str, vals: &[&Bson]) -> bool {
    let Some((parent, last)) = path.rsplit_once('.') else { return false };
    if vals.is_empty() || last.is_empty() || !last.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    let parents = path_values(doc, parent);
    vals.iter().all(|v| matches!(v, Bson::Array(_)) && parents.iter().any(|p| matches!(p, Bson::Array(a) if a.iter().any(|e| std::ptr::eq(e, *v)))))
}

fn match_field(doc: &Document, path: &str, cond: &Bson) -> R<bool> {
    let vals = path_values(doc, path);
    if reached_by_position(doc, path, &vals) {
        let prev = EXPAND.with(|x| x.replace(false));
        let r = match_field_vals(&vals, cond);
        EXPAND.with(|x| x.set(prev));
        return r;
    }
    match_field_vals(&vals, cond)
}

fn match_field_vals(vals: &[&Bson], cond: &Bson) -> R<bool> {
    let vals = vals.to_vec();
    if let Bson::Document(d) = cond {
        if crate::geo_query::has_geo_cond(d) {
            if !crate::geo_query::match_field(&vals, d)? {
                return Ok(false);
            }
            // the other operators of the same condition document still apply
            let mut rest = d.clone();
            for k in ["$geoWithin", "$within", "$geoIntersects", "$near", "$nearSphere", "$maxDistance", "$minDistance"] {
                rest.remove(k);
            }
            return if rest.is_empty() { Ok(true) } else { match_operator_doc(&vals, &rest) };
        }
    }
    match cond {
        Bson::Document(d) if is_operator_doc(d) => match_operator_doc(&vals, d),
        other => eq_match(&vals, other),
    }
}

fn bad(msg: impl Into<String>) -> MongoError {
    MongoError::new(2, "BadValue", msg.into())
}

/// `$pull` element test: operator conditions apply to the element, a plain document is a query
/// against document elements, anything else is equality (or a regex match on strings).
pub fn pull_matches(e: &Bson, cond: &Bson) -> R<bool> {
    match cond {
        Bson::Document(d) if is_operator_doc(d) && !d.keys().any(|k| matches!(k.as_str(), "$and" | "$or" | "$nor")) => match_operator_doc(&[e], d),
        Bson::Document(d) => match e {
            Bson::Document(ed) => matches(ed, d),
            _ => Ok(false),
        },
        Bson::RegularExpression(r) => match e {
            Bson::String(s) => crate::regex::matches_str(r.pattern.as_str(), r.options.as_str(), s),
            other => Ok(equal(other, cond)),
        },
        other => Ok(equal(e, other)),
    }
}

fn match_operator_doc(vals: &[&Bson], d: &Document) -> R<bool> {
    // $regex and $options pair up regardless of their order in the document.
    let options = d.get("$options");
    if options.is_some() && !d.contains_key("$regex") {
        return Err(bad("$options needs a $regex"));
    }
    for (op, arg) in d {
        if op == "$options" {
            continue;
        }
        let ok = if op == "$regex" {
            let (pat, opts) = match (arg, options) {
                (Bson::String(p), None) => (p.clone(), String::new()),
                (Bson::String(p), Some(Bson::String(o))) => (p.clone(), o.clone()),
                (Bson::String(_), Some(_)) => return Err(bad("$options has to be a string")),
                (Bson::RegularExpression(r), None) => (r.pattern.as_str().to_string(), r.options.as_str().to_string()),
                (Bson::RegularExpression(r), Some(Bson::String(o))) if r.options.as_str().is_empty() => (r.pattern.as_str().to_string(), o.clone()),
                (Bson::RegularExpression(_), Some(_)) => return Err(MongoError::new(51075, "Location51075", "options set in both $regex and $options".into())),
                _ => return Err(bad("$regex has to be a string")),
            };
            regex_vals(vals, &pat, &opts)?
        } else {
            match_op(vals, op, arg)?
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
}

fn regex_vals(vals: &[&Bson], pat: &str, opts: &str) -> R<bool> {
    let re = crate::regex::compile(pat, opts)?;
    for c in candidates(vals) {
        if let Bson::String(s) | Bson::Symbol(s) = c {
            if crate::regex::is_match(&re, s)? {
                return Ok(true);
            }
        }
        // a stored regular expression value matches the same pattern and options
        if let Bson::RegularExpression(r) = c {
            if r.pattern.as_str() == pat && r.options.as_str() == opts {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn eq_match(vals: &[&Bson], operand: &Bson) -> R<bool> {
    if let Bson::RegularExpression(r) = operand {
        if regex_vals(vals, r.pattern.as_str(), r.options.as_str())? {
            return Ok(true);
        }
        return Ok(candidates(vals).iter().any(|c| equal(c, operand)));
    }
    if matches!(operand, Bson::Null | Bson::Undefined) {
        // null matches a missing field and a field whose value (or an array element) is null.
        return Ok(vals.is_empty() || candidates(vals).iter().any(|c| matches!(c, Bson::Null | Bson::Undefined)));
    }
    Ok(candidates(vals).iter().any(|c| equal(c, operand)))
}

fn same_bracket(a: &Bson, b: &Bson) -> bool {
    type_rank(a) == type_rank(b)
}

pub fn type_code(v: &Bson) -> i32 {
    match v {
        Bson::Double(_) => 1,
        Bson::String(_) => 2,
        Bson::Document(_) => 3,
        Bson::Array(_) => 4,
        Bson::Binary(_) => 5,
        Bson::Undefined => 6,
        Bson::ObjectId(_) => 7,
        Bson::Boolean(_) => 8,
        Bson::DateTime(_) => 9,
        Bson::Null => 10,
        Bson::RegularExpression(_) => 11,
        Bson::DbPointer(_) => 12,
        Bson::JavaScriptCode(_) => 13,
        Bson::Symbol(_) => 14,
        Bson::JavaScriptCodeWithScope(_) => 15,
        Bson::Int32(_) => 16,
        Bson::Timestamp(_) => 17,
        Bson::Int64(_) => 18,
        Bson::Decimal128(_) => 19,
        Bson::MinKey => -1,
        Bson::MaxKey => 127,
    }
}

/// A `$type` argument: a set of numeric codes (`number` expands to the numeric types).
fn parse_type(arg: &Bson) -> R<Vec<i32>> {
    let one = |b: &Bson| -> R<Vec<i32>> {
        match b {
            Bson::String(s) => Ok(match s.as_str() {
                "double" => vec![1],
                "string" => vec![2],
                "object" => vec![3],
                "array" => vec![4],
                "binData" => vec![5],
                "undefined" => vec![6],
                "objectId" => vec![7],
                "bool" => vec![8],
                "date" => vec![9],
                "null" => vec![10],
                "regex" => vec![11],
                "dbPointer" => vec![12],
                "javascript" => vec![13],
                "symbol" => vec![14],
                "javascriptWithScope" => vec![15],
                "int" => vec![16],
                "timestamp" => vec![17],
                "long" => vec![18],
                "decimal" => vec![19],
                "minKey" => vec![-1],
                "maxKey" => vec![127],
                "number" => vec![1, 16, 18, 19],
                other => return Err(bad(format!("Unknown type name alias: {other}"))),
            }),
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => {
                let f = as_f64(b);
                if f.fract() != 0.0 {
                    return Err(bad(format!("Invalid numerical type code: {f}")));
                }
                let c = f as i32;
                if matches!(c, 1..=19 | -1 | 127) {
                    Ok(vec![c])
                } else {
                    Err(bad(format!("Invalid numerical type code: {c}")))
                }
            }
            _ => Err(MongoError::type_mismatch("type must be represented as a number or a string".into())),
        }
    };
    match arg {
        Bson::Array(a) => {
            if a.is_empty() {
                return Err(MongoError::failed_to_parse("a must match at least one type".into()));
            }
            let mut out = Vec::new();
            for x in a {
                out.extend(one(x)?);
            }
            Ok(out)
        }
        other => one(other),
    }
}

fn match_elem(e: &Bson, arg: &Document) -> R<bool> {
    let query_form = arg.keys().next().map_or(true, |k| matches!(k.as_str(), "$and" | "$or" | "$nor") || !k.starts_with('$'));
    if query_form {
        match e {
            Bson::Document(d) => matches(d, arg),
            _ => Ok(false),
        }
    } else {
        // Operator form: each operator applies to the element itself (not to its sub-elements).
        let v = [e];
        let prev = EXPAND.with(|x| x.replace(false));
        let r = match_operator_doc(&v, arg);
        EXPAND.with(|x| x.set(prev));
        r
    }
}

/// Lists at least this long are searched in a sorted copy (`InSets`), shorter ones one value at a time.
const IN_SET_MIN: usize = 32;

tokio::task_local! {
    /// The sorted `$in`/`$nin` lists of the filter a query matches with, by the address and length of the list. Built
    /// from a copy of the filter that lives as long as the scope (`coll::find_docs`), so no other live list can have the
    /// same address: a hit is always that list.
    pub static IN_SETS: std::cell::RefCell<std::collections::HashMap<(usize, usize), std::sync::Arc<InSet>>>;
}

pub fn in_sets_active() -> bool {
    IN_SETS.try_with(|_| ()).is_ok()
}

/// One long `$in`/`$nin` list: its ordinary values sorted by `compare` (equality there is `compare == Equal`, a total
/// order, so a binary search finds a match exactly when one value-by-value comparison would), and the values with
/// their own matching rules (regex, null, undefined) kept apart.
pub struct InSet {
    sorted: Vec<Bson>,
    special: Vec<Bson>,
}

impl InSet {
    fn matches(&self, vals: &[&Bson]) -> R<bool> {
        for x in &self.special {
            if eq_match(vals, x)? {
                return Ok(true);
            }
        }
        Ok(candidates(vals).iter().any(|c| self.sorted.binary_search_by(|x| compare(x, c)).is_ok()))
    }
}

pub struct InSets;

impl InSets {
    /// The sorted lists of every long `$in`/`$nin` in `filter` (a list holding an operator document is left out: it
    /// is an error, raised by the value-by-value path as before).
    pub fn build(filter: &Document) -> std::collections::HashMap<(usize, usize), std::sync::Arc<InSet>> {
        let mut out = std::collections::HashMap::new();
        fn walk(v: &Bson, out: &mut std::collections::HashMap<(usize, usize), std::sync::Arc<InSet>>) {
            match v {
                Bson::Document(d) => {
                    for (k, x) in d {
                        if let ("$in" | "$nin", Bson::Array(list)) = (k.as_str(), x) {
                            if list.len() >= IN_SET_MIN && !list.iter().any(|e| matches!(e, Bson::Document(od) if is_operator_doc(od))) {
                                let (mut sorted, mut special) = (Vec::new(), Vec::new());
                                for e in list {
                                    if matches!(e, Bson::RegularExpression(_) | Bson::Null | Bson::Undefined) {
                                        special.push(e.clone());
                                    } else {
                                        sorted.push(e.clone());
                                    }
                                }
                                sorted.sort_by(compare);
                                out.insert((list.as_ptr() as usize, list.len()), std::sync::Arc::new(InSet { sorted, special }));
                                continue;
                            }
                        }
                        walk(x, out);
                    }
                }
                Bson::Array(a) => a.iter().for_each(|x| walk(x, out)),
                _ => {}
            }
        }
        // the values of `filter` itself (never a copy: the addresses must be those of the lists matched against)
        for (k, x) in filter {
            if let ("$in" | "$nin", Bson::Array(_)) = (k.as_str(), x) {
                continue; // a top-level `$in` is not a query operator
            }
            walk(x, &mut out);
        }
        out
    }
}

/// Whether `filter` holds an `$in`/`$nin` list long enough for `InSets`.
pub fn has_long_in(filter: &Document) -> bool {
    fn walk(v: &Bson) -> bool {
        match v {
            Bson::Document(d) => d.iter().any(|(k, x)| matches!((k.as_str(), x), ("$in" | "$nin", Bson::Array(l)) if l.len() >= IN_SET_MIN) || walk(x)),
            Bson::Array(a) => a.iter().any(walk),
            _ => false,
        }
    }
    filter.iter().any(|(_, v)| walk(v))
}

pub fn int_arg(arg: &Bson) -> Option<i64> {
    match arg {
        Bson::Int32(i) => Some(*i as i64),
        Bson::Int64(i) => Some(*i),
        Bson::Double(f) if f.fract() == 0.0 && f.is_finite() => Some(*f as i64),
        _ => None,
    }
}

/// The 64-bit mask of a bitwise query operator: a non-negative integer, or an array of bit positions.
fn bit_mask(op: &str, arg: &Bson) -> R<(u64, bool)> {
    let whole = |v: &Bson| -> Option<i64> {
        match v {
            Bson::Int32(i) => Some(*i as i64),
            Bson::Int64(i) => Some(*i),
            Bson::Double(f) if f.fract() == 0.0 && f.is_finite() => Some(*f as i64),
            _ => None,
        }
    };
    match arg {
        Bson::Array(a) => {
            let mut mask = 0u64;
            let mut beyond = false;
            for (i, p) in a.iter().enumerate() {
                match whole(p) {
                    Some(n) if n >= 0 => {
                        if n < 64 {
                            mask |= 1 << n;
                        } else {
                            beyond = true; // bits past the 64th of an integer are copies of its sign
                        }
                    }
                    Some(_) => return Err(bad(format!("Failed to parse bit position. Expected a non-negative number in: {i}: {}", crate::update::fmt_bson(p)))),
                    None => return Err(bad(format!("Failed to parse bit position. Expected an integer: {i}: {}", crate::update::fmt_bson(p)))),
                }
            }
            Ok((mask, beyond))
        }
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => match whole(arg) {
            Some(n) if n >= 0 => Ok((n as u64, false)),
            Some(_) => Err(MongoError::failed_to_parse(format!("Expected a non-negative number in: {op}: {}", crate::update::fmt_bson(arg)))),
            None => Err(MongoError::failed_to_parse(format!("Expected an integer: {op}: {}", crate::update::fmt_bson(arg)))),
        },
        other => Err(bad(format!("a takes an Array, a number, or a BinData but received: {op}: {}", crate::update::fmt_bson(other)))),
    }
}

fn match_op(vals: &[&Bson], op: &str, arg: &Bson) -> R<bool> {
    Ok(match op {
        "$eq" => eq_match(vals, arg)?,
        "$ne" => {
            if matches!(arg, Bson::RegularExpression(_)) {
                return Err(bad("Can't have regex as arg to $ne."));
            }
            !eq_match(vals, arg)?
        }
        "$gt" | "$gte" | "$lt" | "$lte" => {
            if matches!(arg, Bson::Null) {
                // $gte/$lte null match null and missing; $gt/$lt null match nothing.
                return Ok(matches!(op, "$gte" | "$lte") && eq_match(vals, arg)?);
            }
            candidates(vals).iter().any(|c| {
                same_bracket(c, arg) && (is_nan_num(c) == is_nan_num(arg)) && {
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
        "$bitsAllSet" | "$bitsAnySet" | "$bitsAllClear" | "$bitsAnyClear" => {
            let (mask, beyond) = bit_mask(op, arg)?;
            candidates(vals).iter().any(|c| {
                let v = match c {
                    Bson::Int32(i) => *i as i64,
                    Bson::Int64(i) => *i,
                    Bson::Double(f) if f.is_finite() && f.fract() == 0.0 && f.abs() < 9.2e18 => *f as i64,
                    _ => return false,
                };
                let hit = (v as u64) & mask;
                let sign = beyond && v < 0;
                match op {
                    "$bitsAllSet" => hit == mask && (!beyond || v < 0),
                    "$bitsAnySet" => hit != 0 || sign,
                    "$bitsAllClear" => hit == 0 && !sign,
                    _ => hit != mask || (beyond && v >= 0),
                }
            })
        }
        "$in" | "$nin" => {
            let list = arg.as_array().ok_or_else(|| bad(format!("${} needs an array", &op[1..])))?;
            if list.len() >= IN_SET_MIN {
                if let Some(set) = IN_SETS.try_with(|m| m.borrow().get(&(list.as_ptr() as usize, list.len())).cloned()).ok().flatten() {
                    let any = set.matches(vals)?;
                    return Ok(if op == "$in" { any } else { !any });
                }
            }
            let mut any = false;
            for x in list {
                if let Bson::Document(d) = x {
                    if is_operator_doc(d) {
                        return Err(bad("cannot nest $ under $in"));
                    }
                }
                if eq_match(vals, x)? {
                    any = true;
                    break;
                }
            }
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
        "$not" => match arg {
            Bson::RegularExpression(r) => !regex_vals(vals, r.pattern.as_str(), r.options.as_str())?,
            Bson::Document(d) if crate::geo_query::has_geo_cond(d) => !crate::geo_query::match_field(vals, d)?,
            Bson::Document(d) if is_operator_doc(d) => !match_operator_doc(vals, d)?,
            _ => return Err(bad("$not needs a regex or a document")),
        },
        "$size" => {
            let n = match arg {
                Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => int_arg(arg),
                _ => return Err(bad("$size needs a number")),
            };
            match n {
                Some(n) if n < 0 => return Err(bad("$size may not be negative")),
                Some(n) => vals.iter().any(|v| matches!(v, Bson::Array(a) if a.len() as i64 == n)),
                None => return Err(bad("$size must be a whole number")),
            }
        }
        "$all" => {
            let list = arg.as_array().ok_or_else(|| bad("$all needs an array"))?;
            if list.is_empty() {
                return Ok(false);
            }
            let em = |x: &Bson| matches!(x, Bson::Document(d) if d.keys().next().map_or(false, |k| k == "$elemMatch"));
            let any_em = list.iter().any(em);
            if any_em && !list.iter().all(em) {
                return Err(bad("$all/$elemMatch has to be consistent"));
            }
            for x in list {
                let ok = if any_em {
                    let sub = x.as_document().unwrap().get("$elemMatch").unwrap();
                    match_op(vals, "$elemMatch", sub)?
                } else {
                    if let Bson::Document(d) = x {
                        if is_operator_doc(d) {
                            return Err(bad("no $ expressions in $all"));
                        }
                    }
                    eq_match(vals, x)?
                };
                if !ok {
                    return Ok(false);
                }
            }
            true
        }
        "$elemMatch" => {
            let d = arg.as_document().ok_or_else(|| bad("$elemMatch needs an Object"))?;
            for v in vals {
                if let Bson::Array(a) = v {
                    for e in a {
                        if match_elem(e, d)? {
                            return Ok(true);
                        }
                    }
                }
            }
            false
        }
        "$type" => {
            let types = parse_type(arg)?;
            vals.iter().any(|v| match v {
                Bson::Array(a) => types.contains(&4) || a.iter().any(|e| types.contains(&type_code(e))),
                other => types.contains(&type_code(other)),
            })
        }
        "$mod" => {
            let a = arg.as_array().ok_or_else(|| bad("malformed mod, needs to be an array"))?;
            if a.len() < 2 {
                return Err(bad("malformed mod, not enough elements"));
            }
            if a.len() > 2 {
                return Err(bad("malformed mod, too many elements"));
            }
            if !is_number(&a[0]) {
                return Err(bad("malformed mod, divisor not a number"));
            }
            if !is_number(&a[1]) {
                return Err(bad("malformed mod, remainder not a number"));
            }
            let (d, r) = (as_f64(&a[0]).trunc(), as_f64(&a[1]).trunc());
            if d == 0.0 {
                return Err(bad("divisor cannot be 0"));
            }
            let (d, r) = (d as i64, r as i64);
            candidates(vals).iter().any(|c| is_numeric(c) && as_f64(c).is_finite() && as_f64(c).abs() < 9.2e18 && (as_f64(c).trunc() as i64).wrapping_rem(d) == r)
        }
        other => return Err(bad(format!("unknown operator: {other}"))),
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

/// fix-customer-id-plan: no `_id` condition of the filter compares with a string or a regular expression (the places
/// [`uses_id_index`] looks; `$type` and `$exists` operands are not compared). Under a query collation other than the
/// collection's, mongod still reads the `_id` index for such a filter (MEASURED with explain on mongod 7.0.34:
/// `$ne: 5`, ranges of numbers, `$in`, `$nin`, `$not`, `$exists`, `$type`, `$or` and `$nor` of them: IXSCAN; `$ne: "a"`,
/// `$gt: "a"`: COLLSCAN), so it answers in `_id` order, strings in the index's own order.
pub fn id_operands_string_free(filter: &Document) -> bool {
    fn free(v: &Bson) -> bool {
        match v {
            Bson::RegularExpression(_) => false,
            Bson::Document(d) if is_operator_doc(d) => d.iter().all(|(op, x)| match op.as_str() {
                "$type" | "$exists" => true,
                _ => free(x),
            }),
            Bson::Document(d) => d.values().all(free) && !crate::collopts::has_string(v),
            Bson::Array(a) => a.iter().all(free),
            v => !crate::collopts::has_string(v),
        }
    }
    filter.iter().all(|(k, v)| match (k.as_str(), v) {
        ("_id", v) => free(v),
        ("$and" | "$or" | "$nor", Bson::Array(a)) => a.iter().all(|s| match s {
            Bson::Document(d) => id_operands_string_free(d),
            _ => true,
        }),
        _ => true,
    })
}

/// fix-customer-id-plan: an `_id` equality anywhere a document must meet it (the top level or inside `$and`), next to
/// other conditions: the value, under the same rules as [`id_equality`]. At most one document per customer holds an
/// `_id` (one in mongod), so such a filter is answered by looking that `_id` up and matching the whole filter.
pub fn id_equality_within(filter: &Document) -> Option<&Bson> {
    for (k, v) in filter {
        match (k.as_str(), v) {
            ("_id", v) => {
                let v = match v {
                    Bson::Document(d) if is_operator_doc(d) => match (d.len(), d.get("$eq")) {
                        (1, Some(x)) => x,
                        _ => continue,
                    },
                    v => v,
                };
                if !matches!(v, Bson::RegularExpression(_) | Bson::Null | Bson::Undefined) {
                    return Some(v);
                }
            }
            ("$and", Bson::Array(a)) => {
                for s in a {
                    if let Bson::Document(sd) = s {
                        if let Some(x) = id_equality_within(sd) {
                            return Some(x);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Sort
// ---------------------------------------------------------------------------

static EMPTY_ARRAY_KEY: Bson = Bson::Array(Vec::new());

/// Compare two sort keys; the key of an empty array sorts above MinKey and below everything else (null, missing included).
fn cmp_sort_keys(a: &Bson, b: &Bson) -> Ordering {
    let (ea, eb) = (matches!(a, Bson::Array(x) if x.is_empty()), matches!(b, Bson::Array(x) if x.is_empty()));
    match (ea, eb) {
        (true, true) => Ordering::Equal,
        (true, false) => if matches!(b, Bson::MinKey) { Ordering::Greater } else { Ordering::Less },
        (false, true) => if matches!(a, Bson::MinKey) { Ordering::Less } else { Ordering::Greater },
        _ => compare(a, b),
    }
}

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
            // An empty array sorts like undefined: above MinKey, below null, in both directions.
            Bson::Array(_) => flat.push(&EMPTY_ARRAY_KEY),
            other => flat.push(other),
        }
    }
    let pick = if ascending {
        flat.into_iter().min_by(|a, b| cmp_sort_keys(a, b))
    } else {
        flat.into_iter().max_by(|a, b| cmp_sort_keys(a, b))
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
            Bson::Document(d) if d.len() == 1 && d.get_str("$meta").ok() == Some("textScore") => {
                // the field name is ignored: documents sort by their text score, best first
                out.push((crate::text::SCORE_KEY.to_string(), false));
                continue;
            }
            Bson::Document(d) if d.len() == 1 && d.get_str("$meta").ok() == Some("geoNearDistance") => {
                out.push((crate::geo::DIST_KEY.to_string(), false));
                continue;
            }
            Bson::Document(d) if d.len() == 1 && d.contains_key("$meta") => {
                return Err(MongoError::new(2, "BadValue", "Illegal $meta sort: ".to_string() + &crate::update::fmt_bson(d.get("$meta").unwrap())));
            }
            _ => return Err(MongoError::new(15974, "Location15974", "Illegal key in $sort specification".into())),
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
            let o = cmp_sort_keys(&a[i], &b[i]);
            let o = if *asc { o } else { o.reverse() };
            if o != Ordering::Equal {
                return o;
            }
        }
        Ordering::Equal
    });
    docs.extend(keyed.into_iter().map(|(_, d)| d));
}

/// `internalQueryMaxBlockingSortMemoryUsageBytes`: what a sort the plan does not give (a blocking sort) may hold, as in
/// mongod (100 MB by default; set with `setParameter`).
pub static SORT_MEM_LIMIT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(100 * 1024 * 1024);

/// Error 292 of mongod for a blocking sort over [`SORT_MEM_LIMIT`]. mongod 6.0+ would spill to disk (allowDiskUse,
/// on by default); this engine does not, so the error says it, whatever allowDiskUse says.
pub fn sort_mem_error() -> MongoError {
    let limit = SORT_MEM_LIMIT.load(std::sync::atomic::Ordering::Relaxed);
    MongoError::new(
        292,
        "QueryExceededMemoryLimitNoDiskUseAllowed",
        format!("Sort exceeded memory limit of {limit} bytes, but did not opt in to external sorting. This server does not sort on disk (allowDiskUse is not supported): add a limit, an index that gives the order, or a narrower filter."),
    )
}

/// The memory a blocking sort counts for a document: its BSON size, as mongod counts it (not this process's own,
/// larger, in-memory form). Computed without encoding the document.
pub fn bson_size(d: &Document) -> usize {
    5 + d.iter().map(|(k, v)| 2 + k.len() + value_size(v)).sum::<usize>()
}

fn value_size(v: &Bson) -> usize {
    match v {
        Bson::Double(_) | Bson::Int64(_) | Bson::DateTime(_) | Bson::Timestamp(_) => 8,
        Bson::Int32(_) => 4,
        Bson::Boolean(_) => 1,
        Bson::Null | Bson::Undefined | Bson::MinKey | Bson::MaxKey => 0,
        Bson::String(s) | Bson::JavaScriptCode(s) | Bson::Symbol(s) => 5 + s.len(),
        Bson::Document(d) => bson_size(d),
        Bson::Array(a) => 5 + a.iter().enumerate().map(|(i, x)| 2 + if i < 10 { 1 } else if i < 100 { 2 } else { 6 } + value_size(x)).sum::<usize>(),
        Bson::Binary(b) => 5 + b.bytes.len(),
        Bson::ObjectId(_) => 12,
        Bson::Decimal128(_) => 16,
        Bson::RegularExpression(r) => 2 + r.pattern.len() + r.options.len(),
        Bson::JavaScriptCodeWithScope(c) => 14 + c.code.len() + bson_size(&c.scope),
        Bson::DbPointer(_) => 24,
    }
}

/// The first `k` documents in the order of a sort specification, kept while the documents stream by: memory for `k`
/// documents, not for every candidate. Same result as [`sort_docs`] on every pushed document followed by a truncation to
/// `k`: equal sort keys keep their push order (the sort is stable).
pub struct TopK<'a> {
    k: usize,
    spec: &'a [(String, bool)],
    seq: u64,
    /// Max-heap on (sort keys, push order): the top is the worst document kept.
    heap: std::collections::BinaryHeap<TopItem<'a>>,
    /// [`bson_size`] of the documents kept.
    bytes: usize,
    /// The `_id` keys of the documents kept, for [`TopK::push_once`].
    ids: std::collections::HashSet<Vec<u8>>,
}

struct TopItem<'a> {
    keys: Vec<Bson>,
    seq: u64,
    doc: Document,
    size: usize,
    id: Option<Vec<u8>>,
    spec: &'a [(String, bool)],
}

impl TopItem<'_> {
    fn cmp_keys(&self, other: &Self) -> Ordering {
        for (i, (_, asc)) in self.spec.iter().enumerate() {
            let o = cmp_sort_keys(&self.keys[i], &other.keys[i]);
            let o = if *asc { o } else { o.reverse() };
            if o != Ordering::Equal {
                return o;
            }
        }
        Ordering::Equal
    }
}

impl PartialEq for TopItem<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for TopItem<'_> {}
impl PartialOrd for TopItem<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for TopItem<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cmp_keys(other).then(self.seq.cmp(&other.seq))
    }
}

impl<'a> TopK<'a> {
    pub fn new(k: usize, spec: &'a [(String, bool)]) -> Self {
        TopK { k, spec, seq: 0, heap: std::collections::BinaryHeap::with_capacity(k.min(4096) + 1), bytes: 0, ids: Default::default() }
    }

    /// Push the next document (push order breaks ties).
    pub fn push(&mut self, doc: Document) {
        let seq = self.seq;
        self.push_item(seq, None, doc);
    }

    /// Push a document with an explicit position: equal sort keys are ordered by `seq` (a later [`TopK::push`] comes
    /// after every position given so far).
    pub fn push_at(&mut self, seq: u64, doc: Document) {
        self.push_item(seq, None, doc);
    }

    /// [`TopK::push_at`] for candidates that may come more than once (a multikey index, the branches of an `$or`), with
    /// `id` the document's key: the same result as pushing only the first time each document comes, as long as every
    /// later time comes with a larger `seq` and the same document. Then it is skipped while the first time is kept, and
    /// otherwise the first time was worse than the `k` kept (same sort keys, smaller `seq`), so the later one is too.
    /// No list of every candidate seen is needed: memory for `k` documents and their keys.
    pub fn push_once(&mut self, seq: u64, id: Vec<u8>, doc: Document) {
        if self.ids.contains(&id) {
            self.seq = self.seq.max(seq + 1);
            return;
        }
        self.push_item(seq, Some(id), doc);
    }

    fn push_item(&mut self, seq: u64, id: Option<Vec<u8>>, doc: Document) {
        self.seq = self.seq.max(seq + 1);
        if self.k == 0 {
            return;
        }
        let size = bson_size(&doc);
        let item = TopItem { keys: self.spec.iter().map(|(p, asc)| sort_key(&doc, p, *asc)).collect(), seq, doc, size, id, spec: self.spec };
        if self.heap.len() < self.k {
            self.bytes += item.size;
            if let Some(id) = &item.id {
                self.ids.insert(id.clone());
            }
            self.heap.push(item);
        } else if let Some(mut worst) = self.heap.peek_mut() {
            if item < *worst {
                self.bytes = self.bytes + item.size - worst.size;
                if let Some(old) = worst.id.take() {
                    self.ids.remove(&old);
                }
                if let Some(id) = &item.id {
                    self.ids.insert(id.clone());
                }
                *worst = item;
            }
        }
    }

    pub fn is_full(&self) -> bool {
        self.heap.len() >= self.k
    }

    /// [`bson_size`] of the documents kept: what mongod's limit on a blocking sort counts.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// The first sort key of the worst document kept (none while empty).
    pub fn worst_first_key(&self) -> Option<&Bson> {
        self.heap.peek().and_then(|i| i.keys.first())
    }

    /// The documents kept, in sort order.
    pub fn into_sorted(self) -> Vec<Document> {
        self.heap.into_sorted_vec().into_iter().map(|i| i.doc).collect()
    }
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
    /// `$slice` projections: (path, skip, limit); `limit == None` means "last/first |skip| elements" form.
    slices: Vec<(Vec<String>, i64, Option<i64>)>,
    /// `{field: {$meta: "textScore"}}`: output fields that receive the text score.
    pub metas: Vec<(String, &'static str)>,
}

fn parse_slice(arg: &Bson) -> R<(i64, Option<i64>)> {
    let num = |b: &Bson| int_arg(b).ok_or_else(|| MongoError::new(28667, "Location28667", "$slice only supports numbers and [skip, limit] arrays".into()));
    let bad_arr = |m: &str| MongoError::new(28724, "Location28724", m.into());
    match arg {
        Bson::Array(a) => {
            if a.len() != 2 {
                return Err(bad_arr("$slice array argument should be of the form [skip, limit]"));
            }
            let (skip, limit) = (num(&a[0])?, num(&a[1])?);
            if limit <= 0 {
                return Err(bad_arr("$slice limit must be positive"));
            }
            Ok((skip, Some(limit)))
        }
        other => Ok((num(other)?, None)),
    }
}

pub fn parse_projection(spec: &Document) -> R<Option<Projection>> {
    if spec.is_empty() {
        return Ok(None);
    }
    let mut id = true;
    let mut inc: Vec<Vec<String>> = Vec::new();
    let mut exc = Vec::new();
    let mut slices = Vec::new();
    let mut metas: Vec<(String, &'static str)> = Vec::new();
    for (k, v) in spec {
        if let Bson::Document(d) = v {
            if d.len() == 1 && d.contains_key("$meta") {
                match d.get_str("$meta") {
                    Ok("textScore") => metas.push((k.clone(), crate::text::SCORE_KEY)),
                    Ok("searchHighlights") => metas.push((k.clone(), crate::search_more::HIGHLIGHT_KEY)),
                    Ok("geoNearDistance") => metas.push((k.clone(), crate::geo::DIST_KEY)),
                    Ok("geoNearPoint") => metas.push((k.clone(), crate::geo::POINT_KEY)),
                    Ok(other) => return Err(MongoError::new(17308, "Location17308", format!("Unsupported argument to $meta: {other}"))),
                    Err(_) => return Err(MongoError::new(2, "BadValue", "$meta projection requires a string argument".into())),
                }
                continue;
            }
            if d.len() == 1 && d.contains_key("$slice") {
                let (skip, limit) = parse_slice(&d["$slice"])?;
                slices.push((k.split('.').map(str::to_string).collect::<Vec<_>>(), skip, limit));
                continue;
            }
        }
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
    let id_included = matches!(spec.get("_id"), Some(v) if !matches!(v, Bson::Boolean(false) | Bson::Int32(0) | Bson::Int64(0)) && !matches!(v, Bson::Double(f) if *f == 0.0));
    if inc.is_empty() && exc.is_empty() && !metas.is_empty() && id_included {
        // `{_id: 1, s: {$meta: ..}}` keeps just the _id and the meta fields.
        return Ok(Some(Projection { include: true, id: true, paths: vec![], slices, metas }));
    }
    if inc.is_empty() && exc.is_empty() && (!slices.is_empty() || !metas.is_empty()) {
        // Slice-only and $meta-only projections keep every field.
        return Ok(Some(Projection { include: false, id, paths: vec![], slices, metas }));
    }
    if inc.is_empty() && exc.is_empty() {
        // Only `_id` was given: {_id: 1} keeps just the _id, {_id: 0} drops just the _id.
        return Ok(Some(Projection { include: id, id, paths: vec![], slices, metas }));
    }
    let include = !inc.is_empty();
    if include {
        // A sliced field in an inclusion projection is included as well.
        for (p, _, _) in &slices {
            inc.push(p.clone());
        }
    }
    Ok(Some(Projection { include, id, paths: if include { inc } else { exc }, slices, metas }))
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
    for (path, skip, limit) in &p.slices {
        slice_at(&mut out, path, *skip, *limit);
    }
    crate::text::strip_hidden(&mut out);
    for (name, key) in &p.metas {
        if let Some(v) = doc.get(*key) {
            out.insert(name.clone(), v.clone());
        }
    }
    out
}

impl Projection {
    pub fn needs_geo_near(&self) -> bool {
        self.metas.iter().any(|(_, k)| *k == crate::geo::DIST_KEY || *k == crate::geo::POINT_KEY)
    }

    pub fn needs_text_score(&self) -> bool {
        self.metas.iter().any(|(_, k)| *k == crate::text::SCORE_KEY)
    }
}

fn slice_at(doc: &mut Document, path: &[String], skip: i64, limit: Option<i64>) {
    match doc.get_mut(&path[0]) {
        Some(Bson::Array(a)) if path.len() == 1 => {
            let len = a.len() as i64;
            let (start, take) = match limit {
                None if skip >= 0 => (0, skip),
                None => ((len + skip).max(0), len),
                Some(l) if skip >= 0 => (skip.min(len), l),
                Some(l) => ((len + skip).max(0), l),
            };
            let end = (start + take).min(len);
            *a = a[start as usize..end as usize].to_vec();
        }
        Some(Bson::Document(d)) if path.len() > 1 => slice_at(d, &path[1..], skip, limit),
        Some(Bson::Array(a)) if path.len() > 1 => {
            for e in a.iter_mut() {
                if let Bson::Document(d) = e {
                    slice_at(d, &path[1..], skip, limit);
                }
            }
        }
        _ => {}
    }
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
    fn regex_and_array_operators() {
        let d = doc! {"s": "Hello", "a": [1, 2, 3], "items": [{"k": 1, "q": 5}, {"k": 2, "q": 1}]};
        assert!(m(d.clone(), doc! {"s": {"$regex": "^hel", "$options": "i"}}));
        assert!(!m(d.clone(), doc! {"s": {"$regex": "^hel"}}));
        assert!(m(d.clone(), doc! {"s": bson::Regex { pattern: "l+o$".try_into().unwrap(), options: "".try_into().unwrap() }}));
        assert!(m(d.clone(), doc! {"s": {"$regex": "(?<=Hel)lo"}}));
        assert!(m(d.clone(), doc! {"a": {"$size": 3}}));
        assert!(m(d.clone(), doc! {"a": {"$all": [3, 1]}}));
        assert!(m(d.clone(), doc! {"a": {"$elemMatch": {"$gt": 1, "$lt": 3}}}));
        assert!(!m(d.clone(), doc! {"items": {"$elemMatch": {"k": 1, "q": 1}}}));
        assert!(m(d.clone(), doc! {"items": {"$elemMatch": {"k": 2, "q": 1}}}));
        assert!(m(d.clone(), doc! {"a": {"$type": "number"}, "s": {"$type": 2}}));
        assert!(m(d.clone(), doc! {"$expr": {"$gt": [{"$size": "$a"}, 2]}}));
        assert_eq!(matches(&d, &doc! {"s": {"$options": "i"}}).unwrap_err().code, 2);
        assert_eq!(matches(&d, &doc! {"s": {"$regex": "("}}).unwrap_err().code, 51091);
    }

    #[test]
    fn projection_slice() {
        let p = parse_projection(&doc! {"a": {"$slice": [1, 2]}}).unwrap().unwrap();
        assert_eq!(project(&doc! {"_id": 1, "a": [1, 2, 3, 4], "b": 1}, &p), doc! {"_id": 1, "a": [2, 3], "b": 1});
        let p = parse_projection(&doc! {"a": {"$slice": -1}, "b": 1}).unwrap().unwrap();
        assert_eq!(project(&doc! {"_id": 1, "a": [1, 2, 3, 4], "b": 1, "c": 2}, &p), doc! {"_id": 1, "a": [4], "b": 1});
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

    #[test]
    fn top_k_equals_sort_then_truncate() {
        // ties, arrays, missing fields, mixed types; every k and both directions, with plain and explicit push order
        let vals: Vec<Bson> = vec![Bson::Int32(3), Bson::Int32(1), Bson::Null, Bson::String("a".into()), Bson::Int32(1), Bson::Array(vec![Bson::Int32(5), Bson::Int32(0)]), Bson::Array(vec![]), Bson::Double(1.0), Bson::Boolean(true), Bson::Int32(3)];
        let docs: Vec<Document> = vals.iter().enumerate().map(|(i, v)| if i == 2 { doc! {"_id": i as i32} } else { doc! {"_id": i as i32, "a": v.clone(), "b": (i % 3) as i32} }).collect();
        for spec in [vec![("a".to_string(), true)], vec![("a".to_string(), false)], vec![("b".to_string(), true), ("a".to_string(), false)]] {
            let mut full = docs.clone();
            sort_docs(&mut full, &spec);
            for k in 0..=docs.len() + 1 {
                let mut t = TopK::new(k, &spec);
                for d in &docs {
                    t.push(d.clone());
                }
                let expect: Vec<Document> = full.iter().take(k).cloned().collect();
                assert_eq!(t.into_sorted(), expect, "k={k} spec={spec:?}");
                // explicit positions pushed out of order (a range scan returns documents in key order, not plan order)
                let mut t = TopK::new(k, &spec);
                for (i, d) in docs.iter().enumerate().rev() {
                    t.push_at(i as u64, d.clone());
                }
                assert_eq!(t.into_sorted(), expect, "push_at k={k} spec={spec:?}");
            }
        }
    }
}

#[cfg(test)]
mod customer_plan_tests {
    use super::*;
    use bson::doc;

    #[test]
    fn id_equality_next_to_other_conditions() {
        assert_eq!(id_equality_within(&doc! { "t": "a", "_id": 5 }), Some(&Bson::Int32(5)));
        assert_eq!(id_equality_within(&doc! { "t": "a", "_id": { "$eq": "x" } }), Some(&Bson::String("x".into())));
        assert_eq!(id_equality_within(&doc! { "$and": [{ "t": "a" }, { "_id": 7 }] }), Some(&Bson::Int32(7)));
        assert_eq!(id_equality_within(&doc! { "t": "a", "_id": { "$gt": 5 } }), None);
        assert_eq!(id_equality_within(&doc! { "_id": { "$gt": 5 }, "$and": [{ "_id": 6 }] }), Some(&Bson::Int32(6)));
        assert_eq!(id_equality_within(&doc! { "t": "a", "_id": Bson::Null }), None);
        assert_eq!(id_equality_within(&doc! { "$or": [{ "_id": 1 }, { "_id": 2 }] }), None);
    }

    #[test]
    fn id_operands_without_strings() {
        assert!(id_operands_string_free(&doc! { "_id": { "$ne": 5 }, "t": "a" }));
        assert!(id_operands_string_free(&doc! { "_id": { "$type": "number" } }));
        assert!(id_operands_string_free(&doc! { "_id": { "$exists": true } }));
        assert!(id_operands_string_free(&doc! { "$or": [{ "_id": 1 }, { "_id": { "$gt": 4 } }] }));
        assert!(!id_operands_string_free(&doc! { "_id": { "$ne": "a" } }));
        assert!(!id_operands_string_free(&doc! { "_id": { "$in": [1, "a"] } }));
        assert!(!id_operands_string_free(&doc! { "$nor": [{ "_id": { "$gt": "a" } }] }));
        assert!(!id_operands_string_free(&doc! { "_id": { "$not": { "$gt": "a" } } }));
        assert!(!id_operands_string_free(&doc! { "_id": bson::Regex { pattern: "^a".try_into().unwrap(), options: "".try_into().unwrap() } }));
    }
}
