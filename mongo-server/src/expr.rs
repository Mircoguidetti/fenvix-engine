//! Aggregation expression evaluator: `$cond`, `$ifNull`, comparison and boolean operators, `$in`,
//! arithmetic, string and array operators, `$filter`, `$map`, `$setUnion`, `$dateToString`.
//!
//! `eval` returns `None` for a *missing* value (a field that does not exist), which is different
//! from `null` in several operators, as in MongoDB.

use crate::error::MongoError;
use crate::query::{as_f64, compare, equal, is_number};
use bson::{Bson, DateTime, Document};
use std::cmp::Ordering;

mod dates;
mod ops;
mod trig;

type R<T> = Result<T, MongoError>;

thread_local! {
    /// Variables of the enclosing `$lookup` `let` (visible to its sub-pipeline).
    static AMBIENT: std::cell::RefCell<Vec<(String, Bson)>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Stored for a variable whose expression evaluated to "missing" (it reads back as missing, not null).
pub fn missing_var() -> Bson {
    Bson::Symbol("\u{1}missing".to_string())
}

/// Run `f` with extra variables visible to every expression it evaluates.
pub fn with_ambient<T>(vars: Vec<(String, Bson)>, f: impl FnOnce() -> T) -> T {
    let n = vars.len();
    AMBIENT.with(|a| a.borrow_mut().extend(vars));
    let r = f();
    AMBIENT.with(|a| {
        let mut a = a.borrow_mut();
        let keep = a.len() - n;
        a.truncate(keep);
    });
    r
}

#[derive(Clone)]
pub struct Scope<'a> {
    pub root: &'a Document,
    vars: Vec<(String, Bson)>,
}

impl<'a> Scope<'a> {
    pub fn new(root: &'a Document) -> Self {
        Self { root, vars: Vec::new() }
    }

    fn with(&self, name: &str, value: Bson) -> Scope<'a> {
        let mut s = self.clone();
        s.vars.push((name.to_string(), value));
        s
    }
}

fn err(code: i32, msg: impl Into<String>) -> MongoError {
    MongoError::new(code, "Location", msg.into()).named()
}

trait Named {
    fn named(self) -> Self;
}
impl Named for MongoError {
    fn named(mut self) -> Self {
        self.name = match self.code {
            14 => "TypeMismatch",
            9 => "FailedToParse",
            2 => "BadValue",
            168 => "InvalidPipelineOperator",
            _ => "Location",
        };
        if self.name == "Location" {
            // codeName for numbered "Location" errors is `Location<code>`; leak once per distinct code.
            self.name = Box::leak(format!("Location{}", self.code).into_boxed_str());
        }
        self
    }
}

fn type_name(v: &Bson) -> &'static str {
    crate::update::type_name(v)
}

// ---------------------------------------------------------------------------
// Paths and variables
// ---------------------------------------------------------------------------

/// `$a.b` semantics: through arrays of documents the result is the array of the values found.
fn get_path(v: &Bson, parts: &[&str]) -> Option<Bson> {
    if parts.is_empty() {
        return Some(v.clone());
    }
    match v {
        Bson::Document(d) => d.get(parts[0]).and_then(|x| get_path(x, &parts[1..])),
        Bson::Array(a) => Some(Bson::Array(
            a.iter().filter(|e| matches!(e, Bson::Document(_))).filter_map(|e| get_path(e, parts)).collect(),
        )),
        _ => None,
    }
}

fn get_path_doc(d: &Document, parts: &[&str]) -> Option<Bson> {
    d.get(parts[0]).and_then(|x| get_path(x, &parts[1..]))
}

pub fn field_path(doc: &Document, path: &str) -> Option<Bson> {
    let parts: Vec<&str> = path.split('.').collect();
    get_path_doc(doc, &parts)
}

/// User variable names start with a lowercase ASCII letter (or a non-ASCII character); the rest may
/// also be digits and underscores.
fn valid_var_name(name: &str) -> bool {
    let mut cs = name.chars();
    match cs.next() {
        Some(c) if c.is_ascii_lowercase() || !c.is_ascii() => {}
        _ => return false,
    }
    cs.all(|c| c.is_ascii_alphanumeric() || c == '_' || !c.is_ascii())
}

fn variable(sc: &Scope, name_and_path: &str) -> R<Option<Bson>> {
    let mut it = name_and_path.splitn(2, '.');
    let name = it.next().unwrap();
    let rest: Vec<&str> = it.next().map(|r| r.split('.').collect()).unwrap_or_default();
    if name.is_empty() {
        return Err(err(9, "empty variable names are not allowed"));
    }
    if !matches!(name, "ROOT" | "CURRENT" | "REMOVE" | "NOW" | "CLUSTER_TIME" | "DESCEND" | "PRUNE" | "KEEP" | "SEARCH_META" | "USER_ROLES") && !valid_var_name(name) {
        return Err(err(9, format!("'{name}' starts with an invalid character for a user variable name")));
    }
    match name {
        "ROOT" | "CURRENT" => Ok(if rest.is_empty() { Some(Bson::Document(sc.root.clone())) } else { get_path_doc(sc.root, &rest) }),
        "REMOVE" => Ok(None),
        "SEARCH_META" => Ok(sc.root.get(crate::search_more::META_KEY).and_then(|m| if rest.is_empty() { Some(m.clone()) } else { get_path(m, &rest) })),
        "KEEP" | "PRUNE" | "DESCEND" if rest.is_empty() && !sc.vars.iter().any(|(n, _)| n == name) => Ok(Some(Bson::String(name.to_ascii_lowercase()))),
        "NOW" => Ok(if rest.is_empty() { Some(Bson::DateTime(bson::DateTime::now())) } else { None }),
        _ => {
            for (n, v) in sc.vars.iter().rev() {
                if n == name {
                    if matches!(v, Bson::Symbol(x) if x == "\u{1}missing") { return Ok(None); }
                    return Ok(if rest.is_empty() { Some(v.clone()) } else { get_path(v, &rest) });
                }
            }
            if let Some(v) = AMBIENT.with(|a| a.borrow().iter().rev().find(|(n, _)| n == name).map(|(_, v)| v.clone())) {
                if matches!(&v, Bson::Symbol(x) if x == "\u{1}missing") { return Ok(None); }
                return Ok(if rest.is_empty() { Some(v) } else { get_path(&v, &rest) });
            }
            Err(err(17276, format!("Use of undefined variable: {name}")))
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub fn is_known_operator(name: &str) -> bool {
    matches!(
        name,
        "$literal" | "$meta" | "$cond" | "$ifNull" | "$eq" | "$ne" | "$gt" | "$gte" | "$lt" | "$lte" | "$and" | "$or" | "$not" | "$in" | "$add" | "$subtract" | "$multiply" | "$divide" | "$mod" | "$concat" | "$toLower" | "$toUpper" | "$size" | "$isArray" | "$arrayElemAt" | "$concatArrays" | "$setUnion" | "$filter" | "$map" | "$dateToString" | "$slice"
            | "$let" | "$switch" | "$mergeObjects" | "$reduce" | "$objectToArray" | "$arrayToObject" | "$getField" | "$setField" | "$unsetField" | "$range" | "$reverseArray" | "$indexOfArray" | "$first" | "$last" | "$firstN" | "$lastN" | "$maxN" | "$minN"
            | "$sortArray" | "$zip" | "$isNumber" | "$setIntersection" | "$setDifference" | "$setEquals" | "$setIsSubset" | "$anyElementTrue" | "$allElementsTrue" | "$max" | "$min" | "$sum" | "$avg" | "$cmp" | "$type" | "$bsonSize" | "$binarySize"
            | "$abs" | "$ceil" | "$floor" | "$sqrt" | "$exp" | "$ln" | "$log10" | "$trunc" | "$round" | "$pow" | "$log" | "$substr" | "$substrBytes" | "$substrCP" | "$strLenBytes" | "$strLenCP" | "$strcasecmp" | "$trim" | "$ltrim" | "$rtrim" | "$split"
            | "$indexOfBytes" | "$indexOfCP" | "$replaceOne" | "$replaceAll" | "$regexMatch" | "$regexFind" | "$regexFindAll" | "$convert" | "$toInt" | "$toLong" | "$toDouble" | "$toDecimal" | "$toBool" | "$toDate" | "$toString" | "$toObjectId" | "$function" | "$toHashedIndexKey"
            | "$rand" | "$tsSecond" | "$tsIncrement" | "$median" | "$percentile"
            | "$sin" | "$cos" | "$tan" | "$asin" | "$acos" | "$atan" | "$atan2" | "$sinh" | "$cosh" | "$tanh" | "$asinh" | "$acosh" | "$atanh" | "$degreesToRadians" | "$radiansToDegrees"
            | "$bitAnd" | "$bitOr" | "$bitXor" | "$bitNot" | "$stdDevPop" | "$stdDevSamp"
            | "$year" | "$month" | "$dayOfMonth" | "$hour" | "$minute" | "$second" | "$millisecond" | "$dayOfWeek" | "$dayOfYear" | "$week" | "$isoWeek" | "$isoWeekYear" | "$isoDayOfWeek" | "$dateToParts" | "$dateFromParts" | "$dateFromString" | "$dateAdd" | "$dateSubtract" | "$dateDiff" | "$dateTrunc"
    )
}

/// An expression object with several `$` fields: each aggregation stage reports it with its own code.
pub fn check_single_operator(e: &Bson, code: i32, msg: &str) -> R<()> {
    match e {
        Bson::Array(a) => a.iter().try_for_each(|x| check_single_operator(x, code, msg)),
        Bson::Document(d) => {
            if d.len() > 1 && d.keys().next().map_or(false, |k| k.starts_with('$')) {
                return Err(err(code, format!("{msg} {}", crate::update::fmt_doc(d))));
            }
            if d.contains_key("$literal") {
                return Ok(());
            }
            d.values().try_for_each(|x| check_single_operator(x, code, msg))
        }
        _ => Ok(()),
    }
}

fn is_const(e: &Bson) -> bool {
    match e {
        Bson::String(s) => !s.starts_with('$'),
        Bson::Array(a) => a.iter().all(is_const),
        Bson::Document(d) => match d.iter().next() {
            Some((k, _)) if k.starts_with('$') => k == "$literal" || (k != "$meta" && k != "$rand" && k != "$function" && d.len() == 1 && d.values().all(is_const)),
            _ => d.values().all(is_const),
        },
        _ => true,
    }
}

/// MongoDB folds constant sub-expressions while optimizing a pipeline, so an operator over constants
/// that cannot be evaluated (`{$divide: [1, 0]}`) fails even where it would never run (a short-circuited
/// `$and`). This evaluates every maximal constant operator once, up front, to reproduce that.
pub fn validate(e: &Bson) -> R<()> {
    match e {
        Bson::Array(a) => a.iter().try_for_each(validate),
        Bson::Document(d) => {
            let operator = matches!(d.iter().next(), Some((k, _)) if k.starts_with('$')) && d.len() == 1;
            if operator && d.contains_key("$literal") {
                return Ok(());
            }
            // Children first: they are folded (and can fail) before their parent runs.
            d.values().try_for_each(validate)?;
            if operator && is_const(e) {
                let empty = Document::new();
                return eval(e, &Scope::new(&empty)).map(|_| ());
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub fn truthy(v: Option<&Bson>) -> bool {
    match v {
        None | Some(Bson::Null) | Some(Bson::Undefined) => false,
        Some(Bson::Boolean(b)) => *b,
        Some(Bson::Int32(i)) => *i != 0,
        Some(Bson::Int64(i)) => *i != 0,
        Some(Bson::Double(f)) => *f != 0.0,
        _ => true,
    }
}

fn is_nullish(v: &Option<Bson>) -> bool {
    matches!(v, None | Some(Bson::Null) | Some(Bson::Undefined))
}

fn args(arg: &Bson) -> Vec<&Bson> {
    match arg {
        Bson::Array(a) => a.iter().collect(),
        other => vec![other],
    }
}

fn exact_args<'a>(name: &str, arg: &'a Bson, n: usize) -> R<Vec<&'a Bson>> {
    let a = args(arg);
    if a.len() != n {
        return Err(err(16020, format!("Expression {name} takes exactly {n} arguments. {} were passed in.", a.len())));
    }
    Ok(a)
}

fn ev(e: &Bson, sc: &Scope) -> R<Option<Bson>> {
    eval(e, sc)
}

fn ev_null(e: &Bson, sc: &Scope) -> R<Bson> {
    Ok(eval(e, sc)?.unwrap_or(Bson::Null))
}

#[derive(Clone, Copy)]
enum Num {
    I32(i32),
    I64(i64),
    F(f64),
}

fn as_num(v: &Bson) -> Option<Num> {
    match v {
        Bson::Int32(i) => Some(Num::I32(*i)),
        Bson::Int64(i) => Some(Num::I64(*i)),
        Bson::Double(f) => Some(Num::F(*f)),
        _ => None,
    }
}

fn num_bson(n: Num) -> Bson {
    match n {
        Num::I32(i) => Bson::Int32(i),
        Num::I64(i) => Bson::Int64(i),
        Num::F(f) => Bson::Double(f),
    }
}

fn to_f(n: Num) -> f64 {
    match n {
        Num::I32(i) => i as f64,
        Num::I64(i) => i as f64,
        Num::F(f) => f,
    }
}

fn to_i(n: Num) -> Option<i64> {
    match n {
        Num::I32(i) => Some(i as i64),
        Num::I64(i) => Some(i),
        Num::F(_) => None,
    }
}

/// int32 op int32 stays int32 unless it overflows (then int64); anything with int64 stays int64
/// unless it overflows (then double); anything with a double is a double.
fn arith(op: char, a: Num, b: Num) -> Num {
    if let (Some(x), Some(y)) = (to_i(a), to_i(b)) {
        let both32 = matches!((a, b), (Num::I32(_), Num::I32(_)));
        let r = match op {
            '+' => x.checked_add(y),
            '-' => x.checked_sub(y),
            _ => x.checked_mul(y),
        };
        return match r {
            Some(v) if both32 && i32::try_from(v).is_ok() => Num::I32(v as i32),
            Some(v) => Num::I64(v),
            None => Num::F(match op {
                '+' => x as f64 + y as f64,
                '-' => x as f64 - y as f64,
                _ => x as f64 * y as f64,
            }),
        };
    }
    let (x, y) = (to_f(a), to_f(b));
    Num::F(match op {
        '+' => x + y,
        '-' => x - y,
        _ => x * y,
    })
}

fn date_ms(v: &Bson) -> Option<i64> {
    match v {
        Bson::DateTime(d) => Some(d.timestamp_millis()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

pub fn eval(e: &Bson, sc: &Scope) -> R<Option<Bson>> {
    match e {
        Bson::String(s) if s.starts_with("$$") => variable(sc, &s[2..]),
        Bson::String(s) if s.starts_with('$') => {
            if s.len() == 1 {
                return Err(err(16872, "'$' by itself is not a valid FieldPath"));
            }
            if s.ends_with('.') && !s.ends_with("..") {
                return Err(err(40353, "FieldPath must not end with a '.'."));
            }
            if s[1..].split('.').any(str::is_empty) {
                return Err(err(15998, "FieldPath field names may not be empty strings."));
            }
            Ok(field_path(sc.root, &s[1..]))
        }
        Bson::Array(a) => {
            let mut out = Vec::with_capacity(a.len());
            for x in a {
                out.push(ev_null(x, sc)?);
            }
            Ok(Some(Bson::Array(out)))
        }
        Bson::Document(d) => {
            if let Some((k, v)) = d.iter().next() {
                if k.starts_with('$') {
                    if d.len() != 1 {
                        return Err(err(15983, format!("An object representing an expression must have exactly one field: {}", crate::update::fmt_doc(d))));
                    }
                    return operator(k, v, sc);
                }
            }
            let mut out = Document::new();
            for (k, v) in d {
                if let Some(x) = ev(v, sc)? {
                    out.insert(k.clone(), x);
                }
            }
            Ok(Some(Bson::Document(out)))
        }
        other => Ok(Some(other.clone())),
    }
}

fn operator(name: &str, arg: &Bson, sc: &Scope) -> R<Option<Bson>> {
    match name {
        "$literal" => Ok(Some(arg.clone())),
        "$meta" => {
            let m = arg.as_str().ok_or_else(|| err(17307, "$meta only supports string arguments"))?;
            let hidden = match m {
                "textScore" => crate::text::SCORE_KEY,
                "searchScore" => crate::search::SCORE_KEY,
                "searchHighlights" => crate::search_more::HIGHLIGHT_KEY,
                "geoNearDistance" => crate::geo::DIST_KEY,
                "geoNearPoint" => crate::geo::POINT_KEY,
                other => return Err(err(17308, format!("Unsupported argument to $meta: {other}"))),
            };
            match sc.root.get(hidden) {
                Some(v) => Ok(Some(v.clone())),
                None if m == "textScore" => Err(err(40218, "query requires text score metadata, but it is not available")),
                None => Err(err(40218, format!("query requires {m} metadata, but it is not available"))),
            }
        }
        "$cond" => cond(arg, sc),
        "$ifNull" => {
            let a = args(arg);
            if a.len() < 2 {
                return Err(err(1257300, format!("Expression $ifNull needs at least two arguments, but {} were passed in.", a.len())));
            }
            for x in &a[..a.len() - 1] {
                let v = ev(x, sc)?;
                if !is_nullish(&v) {
                    return Ok(v);
                }
            }
            ev(a[a.len() - 1], sc)
        }
        "$eq" | "$ne" | "$gt" | "$gte" | "$lt" | "$lte" => {
            let a = exact_args(name, arg, 2)?;
            let l = ev(a[0], sc)?.unwrap_or(Bson::MinKey);
            let r = ev(a[1], sc)?.unwrap_or(Bson::MinKey);
            let o = compare(&l, &r);
            Ok(Some(Bson::Boolean(match name {
                "$eq" => o == Ordering::Equal,
                "$ne" => o != Ordering::Equal,
                "$gt" => o == Ordering::Greater,
                "$gte" => o != Ordering::Less,
                "$lt" => o == Ordering::Less,
                _ => o != Ordering::Greater,
            })))
        }
        "$and" => {
            for x in args(arg) {
                if !truthy(ev(x, sc)?.as_ref()) {
                    return Ok(Some(Bson::Boolean(false)));
                }
            }
            Ok(Some(Bson::Boolean(true)))
        }
        "$or" => {
            for x in args(arg) {
                if truthy(ev(x, sc)?.as_ref()) {
                    return Ok(Some(Bson::Boolean(true)));
                }
            }
            Ok(Some(Bson::Boolean(false)))
        }
        "$not" => {
            let a = exact_args(name, arg, 1)?;
            Ok(Some(Bson::Boolean(!truthy(ev(a[0], sc)?.as_ref()))))
        }
        "$in" => {
            let a = exact_args(name, arg, 2)?;
            let needle = ev(a[0], sc)?;
            match ev(a[1], sc)? {
                Some(Bson::Array(h)) => Ok(Some(Bson::Boolean(needle.map_or(false, |n| h.iter().any(|x| equal(x, &n)))))),
                other => Err(err(40081, format!("$in requires an array as a second argument, found: {}", other.as_ref().map(type_name).unwrap_or("missing")))),
            }
        }
        "$add" => add(arg, sc),
        "$subtract" => subtract(arg, sc),
        "$multiply" => {
            let mut acc = Num::I32(1);
            for x in args(arg) {
                let v = ev(x, sc)?;
                if is_nullish(&v) {
                    return Ok(Some(Bson::Null));
                }
                let v = v.unwrap();
                match as_num(&v) {
                    Some(n) => acc = arith('*', acc, n),
                    None => return Err(err(14, format!("$multiply only supports numeric types, not {}", type_name(&v)))),
                }
            }
            Ok(Some(num_bson(acc)))
        }
        "$divide" => {
            let a = exact_args(name, arg, 2)?;
            let (l, r) = (ev(a[0], sc)?, ev(a[1], sc)?);
            if is_nullish(&l) || is_nullish(&r) {
                return Ok(Some(Bson::Null));
            }
            let (l, r) = (l.unwrap(), r.unwrap());
            match (as_num(&l), as_num(&r)) {
                (Some(x), Some(y)) => {
                    if to_f(y) == 0.0 {
                        return Err(err(2, "can't $divide by zero"));
                    }
                    Ok(Some(Bson::Double(to_f(x) / to_f(y))))
                }
                _ => Err(err(14, format!("$divide only supports numeric types, not {} and {}", type_name(&l), type_name(&r)))),
            }
        }
        "$mod" => {
            let a = exact_args(name, arg, 2)?;
            let (l, r) = (ev(a[0], sc)?, ev(a[1], sc)?);
            if is_nullish(&l) || is_nullish(&r) {
                return Ok(Some(Bson::Null));
            }
            let (l, r) = (l.unwrap(), r.unwrap());
            match (as_num(&l), as_num(&r)) {
                (Some(x), Some(y)) => {
                    if to_f(y) == 0.0 {
                        return Err(err(16610, "can't $mod by zero"));
                    }
                    Ok(Some(match (x, y) {
                        (Num::I32(p), Num::I32(q)) => Bson::Int32(p.wrapping_rem(q)),
                        (p, q) if to_i(p).is_some() && to_i(q).is_some() => Bson::Int64(to_i(p).unwrap().wrapping_rem(to_i(q).unwrap())),
                        (p, q) => Bson::Double(to_f(p) % to_f(q)),
                    }))
                }
                _ => Err(err(16611, format!("$mod only supports numeric types, not {} and {}", type_name(&l), type_name(&r)))),
            }
        }
        "$concat" => {
            let mut out = String::new();
            for x in args(arg) {
                let v = ev(x, sc)?;
                if is_nullish(&v) {
                    return Ok(Some(Bson::Null));
                }
                match v.unwrap() {
                    Bson::String(s) => out.push_str(&s),
                    other => return Err(err(16702, format!("$concat only supports strings, not {}", type_name(&other)))),
                }
            }
            Ok(Some(Bson::String(out)))
        }
        "$toLower" | "$toUpper" => {
            let a = exact_args(name, arg, 1)?;
            let v = ev(a[0], sc)?;
            let s = match v {
                None | Some(Bson::Null) | Some(Bson::Undefined) => String::new(),
                Some(Bson::String(s)) => s,
                Some(Bson::Int32(i)) => i.to_string(),
                Some(Bson::Int64(i)) => i.to_string(),
                Some(Bson::Double(f)) => if f.is_finite() && f.fract() == 0.0 && f.abs() < 1e15 { format!("{}", f as i64) } else { f.to_string() },
                Some(Bson::DateTime(d)) => format_date(d.timestamp_millis(), "%Y-%m-%dT%H:%M:%S.%LZ", 0)?,
                Some(other) => return Err(err(16007, format!("can't convert from BSON type {} to String", type_name(&other)))),
            };
            // MongoDB lower/upper-cases ASCII letters only.
            Ok(Some(Bson::String(if name == "$toLower" { s.to_ascii_lowercase() } else { s.to_ascii_uppercase() })))
        }
        "$size" => {
            let a = exact_args(name, arg, 1)?;
            match ev(a[0], sc)? {
                Some(Bson::Array(x)) => Ok(Some(Bson::Int32(x.len() as i32))),
                other => Err(err(17124, format!("The argument to $size must be an array, but was of type: {}", other.as_ref().map(type_name).unwrap_or("missing")))),
            }
        }
        "$isArray" => {
            let a = exact_args(name, arg, 1)?;
            Ok(Some(Bson::Boolean(matches!(ev(a[0], sc)?, Some(Bson::Array(_))))))
        }
        "$arrayElemAt" => {
            let a = exact_args(name, arg, 2)?;
            let (arr, idx) = (ev(a[0], sc)?, ev(a[1], sc)?);
            if is_nullish(&arr) || is_nullish(&idx) {
                return Ok(Some(Bson::Null));
            }
            let (arr, idx) = (arr.unwrap(), idx.unwrap());
            let Bson::Array(arr) = arr else {
                return Err(err(28689, format!("$arrayElemAt's first argument must be an array, but is {}", type_name(&arr))));
            };
            let i = match as_num(&idx) {
                Some(n) if to_f(n).fract() == 0.0 && (i32::MIN as f64..=i32::MAX as f64).contains(&to_f(n)) => to_f(n) as i64,
                Some(_) => return Err(err(28691, "$arrayElemAt's second argument must be representable as a 32-bit integer")),
                None => return Err(err(28690, format!("$arrayElemAt's second argument must be a numeric value, but is {}", type_name(&idx)))),
            };
            let i = if i < 0 { arr.len() as i64 + i } else { i };
            Ok(if i >= 0 && (i as usize) < arr.len() { Some(arr[i as usize].clone()) } else { None })
        }
        "$concatArrays" => {
            let mut out = Vec::new();
            for x in args(arg) {
                let v = ev(x, sc)?;
                if is_nullish(&v) {
                    return Ok(Some(Bson::Null));
                }
                match v.unwrap() {
                    Bson::Array(a) => out.extend(a),
                    other => return Err(err(28664, format!("$concatArrays only supports arrays, not {}", type_name(&other)))),
                }
            }
            Ok(Some(Bson::Array(out)))
        }
        "$setUnion" => {
            let mut out: Vec<Bson> = Vec::new();
            for x in args(arg) {
                let v = ev(x, sc)?;
                if is_nullish(&v) {
                    return Ok(Some(Bson::Null));
                }
                match v.unwrap() {
                    Bson::Array(a) => {
                        for item in a {
                            if !out.iter().any(|y| equal(y, &item)) {
                                out.push(item);
                            }
                        }
                    }
                    other => return Err(err(17043, format!("All operands of $setUnion must be arrays. One argument is of type: {}", type_name(&other)))),
                }
            }
            out.sort_by(compare);
            Ok(Some(Bson::Array(out)))
        }
        "$slice" => slice(arg, sc),
        "$filter" => filter(arg, sc),
        "$map" => map(arg, sc),
        other => match ops::operator_ext(other, arg, sc)? {
            Some(v) => Ok(v),
            None => match trig::operator(other, arg, sc)? {
                Some(v) => Ok(v),
                None => Err(err(168, format!("Unrecognized expression '{other}'"))),
            },
        },
    }
}

fn cond(arg: &Bson, sc: &Scope) -> R<Option<Bson>> {
    let (i, t, f) = match arg {
        Bson::Array(a) => {
            if a.len() != 3 {
                return Err(err(16020, format!("Expression $cond takes exactly 3 arguments. {} were passed in.", a.len())));
            }
            (&a[0], &a[1], &a[2])
        }
        Bson::Document(d) => {
            for (k, _) in d {
                if !matches!(k.as_str(), "if" | "then" | "else") {
                    return Err(err(17083, format!("Unrecognized parameter to $cond: {k}")));
                }
            }
            let get = |k: &str, code: i32| d.get(k).ok_or_else(|| err(code, format!("Missing '{k}' parameter to $cond")));
            (get("if", 17080)?, get("then", 17081)?, get("else", 17082)?)
        }
        _ => return Err(err(16020, "Expression $cond takes exactly 3 arguments. 1 were passed in.")),
    };
    if truthy(ev(i, sc)?.as_ref()) { ev(t, sc) } else { ev(f, sc) }
}

fn add(arg: &Bson, sc: &Scope) -> R<Option<Bson>> {
    let mut acc = Num::I32(0);
    let mut date: Option<i64> = None;
    let mut saw_null = false;
    for x in args(arg) {
        let v = ev(x, sc)?;
        if is_nullish(&v) {
            saw_null = true;
            continue;
        }
        let v = v.unwrap();
        if let Some(ms) = date_ms(&v) {
            if date.is_some() {
                return Err(err(16612, "only one date allowed in an $add expression"));
            }
            date = Some(ms);
        } else if let Some(n) = as_num(&v) {
            acc = arith('+', acc, n);
        } else {
            return Err(err(14, format!("$add only supports numeric or date types, not {}", type_name(&v))));
        }
    }
    if saw_null {
        return Ok(Some(Bson::Null));
    }
    Ok(Some(match date {
        Some(ms) => Bson::DateTime(DateTime::from_millis(ms + to_f(acc) as i64)),
        None => num_bson(acc),
    }))
}

fn subtract(arg: &Bson, sc: &Scope) -> R<Option<Bson>> {
    let a = exact_args("$subtract", arg, 2)?;
    let (l, r) = (ev(a[0], sc)?, ev(a[1], sc)?);
    if is_nullish(&l) || is_nullish(&r) {
        return Ok(Some(Bson::Null));
    }
    let (l, r) = (l.unwrap(), r.unwrap());
    if let (Some(x), Some(y)) = (date_ms(&l), date_ms(&r)) {
        return Ok(Some(Bson::Int64(x - y)));
    }
    if let (Some(x), Some(n)) = (date_ms(&l), as_num(&r)) {
        return Ok(Some(Bson::DateTime(DateTime::from_millis(x - to_f(n) as i64))));
    }
    match (as_num(&l), as_num(&r)) {
        (Some(x), Some(y)) => Ok(Some(num_bson(arith('-', x, y)))),
        _ => Err(err(14, format!("can't $subtract {} from {}", type_name(&r), type_name(&l)))),
    }
}

fn as_name<'a>(d: &'a Document, key: &str, default: &'a str) -> R<&'a str> {
    match d.get(key) {
        None => Ok(default),
        Some(Bson::String(s)) if s.is_empty() => Err(err(9, "empty variable names are not allowed")),
        Some(Bson::String(s)) if !valid_var_name(s) => Err(err(9, format!("'{s}' starts with an invalid character for a user variable name"))),
        Some(Bson::String(s)) => Ok(s),
        Some(_) => Err(err(9, "empty variable names are not allowed")),
    }
}

fn slice(arg: &Bson, sc: &Scope) -> R<Option<Bson>> {
    let a = args(arg);
    if a.len() != 2 && a.len() != 3 {
        return Err(err(28667, format!("Expression $slice takes at least 2 arguments, and at most 3, but {} were passed in.", a.len())));
    }
    let arr = ev(a[0], sc)?;
    let mut ints = Vec::new();
    for (i, x) in a[1..].iter().enumerate() {
        let v = ev(x, sc)?;
        if is_nullish(&v) {
            return Ok(Some(Bson::Null));
        }
        let v = v.unwrap();
        let ord = if i == 0 { "Second" } else { "Third" };
        match as_num(&v) {
            None => return Err(err(if i == 0 { 28725 } else { 28727 }, format!("{ord} argument to $slice must be numeric, but is of type: {}", type_name(&v)))),
            Some(n) if to_f(n).fract() != 0.0 || to_f(n) > i32::MAX as f64 || to_f(n) < i32::MIN as f64 => {
                return Err(err(if i == 0 { 28726 } else { 28728 }, format!("{ord} argument to $slice can't be represented as a 32-bit integer: {}", crate::update::fmt_bson(&v))))
            }
            Some(n) => ints.push(to_f(n) as i64),
        }
    }
    let arr = match arr {
        None | Some(Bson::Null) | Some(Bson::Undefined) => return Ok(Some(Bson::Null)),
        Some(Bson::Array(x)) => x,
        Some(other) => return Err(err(28724, format!("First argument to $slice must be an array, but is of type: {}", type_name(&other)))),
    };
    let len = arr.len() as i64;
    let (start, take) = if ints.len() == 1 {
        let n = ints[0];
        if n >= 0 { (0, n) } else { ((len + n).max(0), -n) }
    } else {
        let (pos, n) = (ints[0], ints[1]);
        if n <= 0 {
            return Err(err(28729, format!("Third argument to $slice must be positive: {n}")));
        }
        (if pos >= 0 { pos } else { (len + pos).max(0) }, n)
    };
    let start = start.min(len) as usize;
    let end = (start as i64 + take).min(len) as usize;
    Ok(Some(Bson::Array(arr[start..end].to_vec())))
}

fn filter(arg: &Bson, sc: &Scope) -> R<Option<Bson>> {
    let d = arg.as_document().ok_or_else(|| err(28646, "$filter only supports an object as its argument"))?;
    for (k, _) in d {
        if !matches!(k.as_str(), "input" | "as" | "cond" | "limit") {
            return Err(err(28647, format!("Unrecognized parameter to $filter: {k}")));
        }
    }
    let input = d.get("input").ok_or_else(|| err(28648, "Missing 'input' parameter to $filter"))?;
    let cond = d.get("cond").ok_or_else(|| err(28650, "Missing 'cond' parameter to $filter"))?;
    let var = as_name(d, "as", "this")?;
    let limit = match d.get("limit") {
        None => None,
        Some(l) => match ev(l, sc)? {
            None | Some(Bson::Null) => None,
            Some(v) if as_num(&v).map(|n| to_f(n).fract() == 0.0 && (i32::MIN as f64..=i32::MAX as f64).contains(&to_f(n))).unwrap_or(false) => {
                if as_f64(&v) < 1.0 {
                    return Err(err(327392, format!("$filter: limit must be greater than 0: {}", crate::update::fmt_bson(&v))));
                }
                Some(as_f64(&v) as usize)
            }
            Some(v) => return Err(err(327391, format!("$filter: limit must be represented as a 32-bit integral value: {}", crate::update::fmt_bson(&v)))),
        },
    };
    match ev(input, sc)? {
        None | Some(Bson::Null) | Some(Bson::Undefined) => Ok(Some(Bson::Null)),
        Some(Bson::Array(a)) => {
            let mut out = Vec::new();
            for item in a {
                if limit.map(|l| out.len() >= l).unwrap_or(false) {
                    break;
                }
                let inner = sc.with(var, item.clone());
                if truthy(ev(cond, &inner)?.as_ref()) {
                    out.push(item);
                }
            }
            Ok(Some(Bson::Array(out)))
        }
        Some(other) => Err(err(28651, format!("input to $filter must be an array not {}", type_name(&other)))),
    }
}

fn map(arg: &Bson, sc: &Scope) -> R<Option<Bson>> {
    let d = arg.as_document().ok_or_else(|| err(16878, "$map only supports an object as its argument"))?;
    for (k, _) in d {
        if !matches!(k.as_str(), "input" | "as" | "in") {
            return Err(err(16879, format!("Unrecognized parameter to $map: {k}")));
        }
    }
    let input = d.get("input").ok_or_else(|| err(16880, "Missing 'input' parameter to $map"))?;
    let body = d.get("in").ok_or_else(|| err(16882, "Missing 'in' parameter to $map"))?;
    let var = as_name(d, "as", "this")?;
    match ev(input, sc)? {
        None | Some(Bson::Null) | Some(Bson::Undefined) => Ok(Some(Bson::Null)),
        Some(Bson::Array(a)) => {
            let mut out = Vec::with_capacity(a.len());
            for item in a {
                let inner = sc.with(var, item);
                out.push(ev(body, &inner)?.unwrap_or(Bson::Null));
            }
            Ok(Some(Bson::Array(out)))
        }
        Some(other) => Err(err(16883, format!("input to $map must be an array not {}", type_name(&other)))),
    }
}

// ---------------------------------------------------------------------------
// $dateToString
// ---------------------------------------------------------------------------

/// (year, month, day, hour, minute, second, millisecond, weekday 0=Sunday, day of year 1..)
fn civil(ms: i64) -> (i64, i64, i64, i64, i64, i64, i64, i64, i64) {
    let days = ms.div_euclid(86_400_000);
    let rem = ms.rem_euclid(86_400_000);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let mut y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    if m <= 2 {
        y += 1;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let cum = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let yday = cum[(m - 1) as usize] + d + if leap && m > 2 { 1 } else { 0 };
    ((y), m, d, rem / 3_600_000, (rem / 60_000) % 60, (rem / 1000) % 60, rem % 1000, (days + 4).rem_euclid(7), yday)
}

fn format_date(ms: i64, fmt: &str, offset_min: i64) -> R<String> {
    let (y, mo, d, h, mi, s, milli, wd, yday) = civil(ms + offset_min * 60_000);
    let mut out = String::new();
    let mut it = fmt.chars();
    while let Some(c) = it.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        match it.next() {
            Some('Y') => out.push_str(&format!("{y:04}")),
            Some('m') => out.push_str(&format!("{mo:02}")),
            Some('d') => out.push_str(&format!("{d:02}")),
            Some('H') => out.push_str(&format!("{h:02}")),
            Some('M') => out.push_str(&format!("{mi:02}")),
            Some('S') => out.push_str(&format!("{s:02}")),
            Some('L') => out.push_str(&format!("{milli:03}")),
            Some('j') => out.push_str(&format!("{yday:03}")),
            Some('w') => out.push_str(&(wd + 1).to_string()),
            Some('u') => out.push_str(&(if wd == 0 { 7 } else { wd }).to_string()),
            Some('Z') => out.push_str(&offset_min.to_string()),
            Some('z') => out.push_str(&format!("{}{:02}{:02}", if offset_min < 0 { '-' } else { '+' }, offset_min.abs() / 60, offset_min.abs() % 60)),
            Some('%') => out.push('%'),
            Some(other) => return Err(err(18536, format!("Invalid format character '%{other}' in format string"))),
            None => return Err(err(18535, "Unmatched '%' at end of format string")),
        }
    }
    Ok(out)
}

/// Minutes east of UTC for the time zone names this server understands: UTC aliases and fixed
/// offsets (`+HH`, `+HHMM`, `+HH:MM`).
fn tz_offset(s: &str) -> Option<i64> {
    if matches!(s, "UTC" | "GMT" | "Etc/UTC" | "Etc/GMT" | "Zulu" | "Etc/Zulu" | "Etc/UCT" | "UCT" | "Etc/Universal" | "Universal" | "Etc/Greenwich" | "Greenwich") {
        return Some(0);
    }
    let (sign, rest) = match s.chars().next()? {
        '+' => (1, &s[1..]),
        '-' => (-1, &s[1..]),
        _ => return None,
    };
    let digits: String = rest.chars().filter(|c| *c != ':').collect();
    if !digits.chars().all(|c| c.is_ascii_digit()) || !(digits.len() == 2 || digits.len() == 4) {
        return None;
    }
    if rest.contains(':') && !(rest.len() == 5 && rest.as_bytes()[2] == b':') {
        return None;
    }
    let h: i64 = digits[..2].parse().ok()?;
    let m: i64 = if digits.len() == 4 { digits[2..].parse().ok()? } else { 0 };
    if h > 23 || m > 59 {
        return None;
    }
    Some(sign * (h * 60 + m))
}

fn date_to_string(arg: &Bson, sc: &Scope) -> R<Option<Bson>> {
    let d = arg.as_document().ok_or_else(|| err(18629, "$dateToString only supports an object as its argument"))?;
    for (k, _) in d {
        if !matches!(k.as_str(), "date" | "format" | "timezone" | "onNull") {
            return Err(err(18534, format!("Unrecognized parameter to $dateToString: {k}")));
        }
    }
    let date = d.get("date").ok_or_else(|| err(18628, "Missing 'date' parameter to $dateToString"))?;
    let fmt = match d.get("format") {
        None => "%Y-%m-%dT%H:%M:%S.%LZ".to_string(),
        Some(f) => match ev(f, sc)? {
            Some(Bson::String(s)) => s,
            None | Some(Bson::Null) => return Ok(Some(Bson::Null)),
            Some(other) => return Err(err(18533, format!("$dateToString requires that 'format' be a string, found: {}", type_name(&other)))),
        },
    };
    let mut offset = 0;
    if let Some(tz) = d.get("timezone") {
        match ev(tz, sc)? {
            Some(Bson::String(s)) => match tz_offset(&s) {
                Some(o) => offset = o,
                None => return Err(err(40485, format!("unrecognized time zone identifier: \"{s}\" (this server knows UTC, GMT and fixed UTC offsets only; there is no time zone database)"))),
            },
            None | Some(Bson::Null) => return Ok(Some(Bson::Null)),
            Some(other) => return Err(err(40517, format!("timezone must evaluate to a string, found {}", type_name(&other)))),
        }
    }
    let v = ev(date, sc)?;
    if is_nullish(&v) {
        return match d.get("onNull") {
            Some(x) => ev(x, sc),
            None => Ok(Some(Bson::Null)),
        };
    }
    let ms = match v.unwrap() {
        Bson::DateTime(x) => x.timestamp_millis(),
        Bson::Timestamp(t) => t.time as i64 * 1000,
        Bson::ObjectId(o) => o.timestamp().timestamp_millis(),
        other => return Err(err(16006, format!("can't convert from BSON type {} to Date", type_name(&other)))),
    };
    Ok(Some(Bson::String(format_date(ms, &fmt, offset)?)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn run(e: Bson, d: &Document) -> Option<Bson> {
        eval(&e, &Scope::new(d)).unwrap()
    }

    #[test]
    fn arithmetic_types() {
        let d = doc! {};
        assert_eq!(run(Bson::Document(doc! {"$add": [1, 2]}), &d), Some(Bson::Int32(3)));
        assert_eq!(run(Bson::Document(doc! {"$add": [i32::MAX, 1]}), &d), Some(Bson::Int64(i32::MAX as i64 + 1)));
        assert_eq!(run(Bson::Document(doc! {"$multiply": [2, 2.5]}), &d), Some(Bson::Double(5.0)));
        assert_eq!(run(Bson::Document(doc! {"$divide": [1, 2]}), &d), Some(Bson::Double(0.5)));
        assert_eq!(run(Bson::Document(doc! {"$add": [1, "$nope"]}), &d), Some(Bson::Null));
    }

    #[test]
    fn cond_ifnull_and_paths() {
        let d = doc! {"a": [{"b": 1}, {"b": 2}], "n": Bson::Null};
        assert_eq!(run(Bson::String("$a.b".into()), &d), Some(Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)])));
        assert_eq!(run(Bson::Document(doc! {"$ifNull": ["$n", "x"]}), &d), Some(Bson::String("x".into())));
        assert_eq!(run(Bson::Document(doc! {"$cond": {"if": {"$gt": [1, 0]}, "then": "y", "else": "n"}}), &d), Some(Bson::String("y".into())));
        assert_eq!(run(Bson::Document(doc! {"$map": {"input": "$a", "as": "x", "in": "$$x.b"}}), &d), Some(Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)])));
    }

    #[test]
    fn dates() {
        let d = doc! {"t": Bson::DateTime(DateTime::from_millis(1_700_000_123_456))};
        assert_eq!(
            run(Bson::Document(doc! {"$dateToString": {"date": "$t"}}), &d),
            Some(Bson::String("2023-11-14T22:15:23.456Z".into()))
        );
        assert_eq!(run(Bson::Document(doc! {"$dateToString": {"format": "%j %u", "date": "$t"}}), &d), Some(Bson::String("318 2".into())));
    }
}
