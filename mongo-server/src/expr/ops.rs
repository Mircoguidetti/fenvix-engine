//! Aggregation expression operators beyond the core set in `expr.rs`: variables and branching
//! (`$let`, `$switch`), objects, arrays, sets, math, strings, type conversion.
//! `operator_ext` returns `Ok(None)` for an operator it does not know.

use super::*;
use crate::query::type_rank;
use bson::doc;

type Out = R<Option<Option<Bson>>>;

fn some(v: Bson) -> Out {
    Ok(Some(Some(v)))
}

fn missing() -> Out {
    Ok(Some(None))
}

fn null() -> Out {
    some(Bson::Null)
}

fn int_exact(v: &Bson) -> Option<i64> {
    match v {
        Bson::Int32(i) => Some(*i as i64),
        Bson::Int64(i) => Some(*i),
        Bson::Double(f) if f.fract() == 0.0 && f.is_finite() && f.abs() < 9.2e18 => Some(*f as i64),
        _ => None,
    }
}

fn doc_arg<'a>(name: &str, arg: &'a Bson, code: i32) -> R<&'a Document> {
    arg.as_document().ok_or_else(|| err(code, format!("{name} only supports an object as its argument")))
}

fn num_or_null(v: &Option<Bson>) -> Option<Num> {
    v.as_ref().and_then(as_num)
}

pub(super) fn operator_ext(name: &str, arg: &Bson, sc: &Scope) -> Out {
    match name {
        "$let" => let_op(arg, sc),
        "$switch" => switch(arg, sc),
        "$mergeObjects" => merge_objects(arg, sc),
        "$reduce" => reduce(arg, sc),
        "$objectToArray" => object_to_array(arg, sc),
        "$arrayToObject" => array_to_object(arg, sc),
        "$getField" => get_field(arg, sc),
        "$setField" => set_field(arg, sc, false),
        "$unsetField" => set_field(arg, sc, true),
        "$range" => range(arg, sc),
        "$reverseArray" => {
            let a = exact_args(name, arg, 1)?;
            match ev(a[0], sc)? {
                v if is_nullish(&v) => null(),
                Some(Bson::Array(mut x)) => {
                    x.reverse();
                    some(Bson::Array(x))
                }
                Some(o) => Err(err(34435, format!("The argument to $reverseArray must be an array, but was of type: {}", type_name(&o)))),
                None => null(),
            }
        }
        "$indexOfArray" => index_of_array(arg, sc),
        "$first" | "$last" => {
            let a = exact_args(name, arg, 1)?;
            match ev(a[0], sc)? {
                v if is_nullish(&v) => null(),
                Some(Bson::Array(x)) => Ok(Some(if name == "$first" { x.first().cloned() } else { x.last().cloned() })),
                Some(o) => Err(err(28689, format!("{name}'s argument must be an array, but is {}", type_name(&o)))),
                None => null(),
            }
        }
        "$firstN" | "$lastN" | "$maxN" | "$minN" => n_of(name, arg, sc),
        "$sortArray" => sort_array(arg, sc),
        "$zip" => zip(arg, sc),
        "$isNumber" => {
            let a = exact_args(name, arg, 1)?;
            some(Bson::Boolean(matches!(ev(a[0], sc)?, Some(Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_)))))
        }
        "$setIntersection" | "$setDifference" | "$setEquals" | "$setIsSubset" => set_op(name, arg, sc),
        "$anyElementTrue" | "$allElementsTrue" => {
            let a = exact_args(name, arg, 1)?;
            match ev(a[0], sc)? {
                Some(Bson::Array(x)) => {
                    let t = |v: &Bson| truthy(Some(v));
                    some(Bson::Boolean(if name == "$anyElementTrue" { x.iter().any(t) } else { x.iter().all(t) }))
                }
                o => Err(err(17041, format!("{name}'s argument must be an array, but is {}", o.as_ref().map(type_name).unwrap_or("missing")))),
            }
        }
        "$max" | "$min" | "$sum" | "$avg" => agg_over(name, arg, sc),
        "$cmp" => {
            let a = exact_args(name, arg, 2)?;
            let l = ev(a[0], sc)?.unwrap_or(Bson::MinKey);
            let r = ev(a[1], sc)?.unwrap_or(Bson::MinKey);
            some(Bson::Int32(match compare(&l, &r) {
                Ordering::Less => -1,
                Ordering::Equal => 0,
                Ordering::Greater => 1,
            }))
        }
        "$type" => {
            let a = exact_args(name, arg, 1)?;
            some(Bson::String(match ev(a[0], sc)? {
                None => "missing".into(),
                Some(v) => type_alias(&v).into(),
            }))
        }
        "$bsonSize" => {
            let a = exact_args(name, arg, 1)?;
            match ev(a[0], sc)? {
                v if is_nullish(&v) => null(),
                Some(Bson::Document(d)) => some(Bson::Int32(d.to_vec().map(|v| v.len()).unwrap_or(0) as i32)),
                Some(o) => Err(err(31393, format!("$bsonSize requires a document input, found: {}", type_name(&o)))),
                None => null(),
            }
        }
        "$binarySize" => {
            let a = exact_args(name, arg, 1)?;
            match ev(a[0], sc)? {
                v if is_nullish(&v) => null(),
                Some(Bson::String(s)) => some(Bson::Int32(s.len() as i32)),
                Some(Bson::Binary(b)) => some(Bson::Int32(b.bytes.len() as i32)),
                Some(o) => Err(err(51276, format!("$binarySize requires a string or BinData input, found: {}", type_name(&o)))),
                None => null(),
            }
        }
        // math
        "$abs" | "$ceil" | "$floor" | "$sqrt" | "$exp" | "$ln" | "$log10" | "$trunc" | "$round" | "$pow" | "$log" => math(name, arg, sc),
        // strings
        "$substr" | "$substrBytes" | "$substrCP" => substr(name, arg, sc),
        "$strLenBytes" | "$strLenCP" => {
            let a = exact_args(name, arg, 1)?;
            match ev(a[0], sc)? {
                Some(Bson::String(s)) => some(Bson::Int32(if name == "$strLenBytes" { s.len() } else { s.chars().count() } as i32)),
                o => Err(err(if name == "$strLenBytes" { 34473 } else { 34471 }, format!("{name} requires a string argument, found: {}", o.as_ref().map(type_name).unwrap_or("missing")))),
            }
        }
        "$strcasecmp" => {
            let a = exact_args(name, arg, 2)?;
            let s = |v: Option<Bson>| -> R<String> {
                Ok(match v {
                    None | Some(Bson::Null) | Some(Bson::Undefined) => String::new(),
                    Some(Bson::String(s)) => s,
                    Some(Bson::Int32(i)) => i.to_string(),
                    Some(Bson::Int64(i)) => i.to_string(),
                    Some(Bson::Double(f)) => f.to_string(),
                    Some(o) => return Err(err(16007, format!("can't convert from BSON type {} to String", type_name(&o)))),
                })
            };
            let (l, r) = (s(ev(a[0], sc)?)?.to_ascii_lowercase(), s(ev(a[1], sc)?)?.to_ascii_lowercase());
            some(Bson::Int32(match l.cmp(&r) {
                Ordering::Less => -1,
                Ordering::Equal => 0,
                Ordering::Greater => 1,
            }))
        }
        "$trim" | "$ltrim" | "$rtrim" => trim(name, arg, sc),
        "$split" => {
            let a = exact_args(name, arg, 2)?;
            let (s, d) = (ev(a[0], sc)?, ev(a[1], sc)?);
            if is_nullish(&s) || is_nullish(&d) {
                return null();
            }
            match (s.unwrap(), d.unwrap()) {
                (Bson::String(s), Bson::String(d)) => {
                    if d.is_empty() {
                        return Err(err(40087, "$split requires a non-empty separator"));
                    }
                    some(Bson::Array(s.split(d.as_str()).map(|p| Bson::String(p.to_string())).collect()))
                }
                (Bson::String(_), o) => Err(err(40086, format!("$split requires an expression that evaluates to a string as a second argument, found: {}", type_name(&o)))),
                (o, _) => Err(err(40085, format!("$split requires an expression that evaluates to a string as a first argument, found: {}", type_name(&o)))),
            }
        }
        "$indexOfBytes" | "$indexOfCP" => index_of_str(name, arg, sc),
        "$replaceOne" | "$replaceAll" => replace(name, arg, sc),
        "$regexMatch" | "$regexFind" | "$regexFindAll" => regex_op(name, arg, sc),
        // conversion
        "$convert" => convert_op(arg, sc),
        "$toInt" | "$toLong" | "$toDouble" | "$toDecimal" | "$toBool" | "$toDate" | "$toString" | "$toObjectId" => {
            let a = exact_args(name, arg, 1)?;
            let v = ev(a[0], sc)?;
            if is_nullish(&v) {
                return null();
            }
            let to = &name[3..];
            Ok(Some(Some(convert(&v.unwrap(), &to.to_ascii_lowercase(), name)?)))
        }
        "$function" => function(arg, sc),
        "$toHashedIndexKey" => {
            let v = ev(arg, sc)?.unwrap_or(Bson::Null);
            some(Bson::Int64(crate::wild::hash_value(&v)))
        }
        "$rand" => {
            match arg {
                Bson::Document(d) if d.is_empty() => {}
                _ => return Err(err(10065, "invalid parameter: expected an object ($rand)")),
            }
            some(Bson::Double(rand::random::<f64>()))
        }
        "$tsSecond" | "$tsIncrement" => {
            let a = exact_args(name, arg, 1)?;
            match ev(a[0], sc)? {
                None | Some(Bson::Null) => null(),
                Some(Bson::Timestamp(t)) => some(Bson::Int64(if name == "$tsSecond" { t.time as i64 } else { t.increment as i64 })),
                Some(o) => Err(err(5687301, format!("{name} expects argument of type timestamp, but got {}", type_name(&o)))),
            }
        }
        "$median" | "$percentile" => {
            let (input, ps, multi) = crate::agg_more::percentile_spec(name, arg)?;
            let vals = match ev(&input, sc)? {
                Some(Bson::Array(a)) => a,
                Some(Bson::Null) | None => vec![],
                Some(o) => vec![o],
            };
            let r = crate::agg_more::percentiles(&vals, &ps);
            if multi { some(Bson::Array(r)) } else { some(r.into_iter().next().unwrap_or(Bson::Null)) }
        }
        _ => dates::date_operator(name, arg, sc),
    }
}

// ---------------------------------------------------------------------------------------------

fn let_op(arg: &Bson, sc: &Scope) -> Out {
    let d = doc_arg("$let", arg, 16874)?;
    for k in d.keys() {
        if !matches!(k.as_str(), "vars" | "in") {
            return Err(err(16875, format!("Unrecognized parameter to $let: {k}")));
        }
    }
    let vars = d.get("vars").ok_or_else(|| err(16876, "Missing 'vars' parameter to $let"))?;
    let body = d.get("in").ok_or_else(|| err(16877, "Missing 'in' parameter to $let"))?;
    let vars = vars.as_document().ok_or_else(|| err(16878, "invalid parameter: expected an object (vars)"))?;
    let mut inner = sc.clone();
    for (name, e) in vars {
        if !valid_var_name(name) {
            return Err(err(9, format!("'{name}' starts with an invalid character for a user variable name")));
        }
        // variables are evaluated in the outer scope: they cannot see each other
        let v = ev(e, sc)?.unwrap_or_else(missing_var);
        inner = inner.with(name, v);
    }
    Ok(Some(ev(body, &inner)?))
}

fn switch(arg: &Bson, sc: &Scope) -> Out {
    let d = doc_arg("$switch", arg, 40060)?;
    for k in d.keys() {
        if !matches!(k.as_str(), "branches" | "default") {
            return Err(err(40067, format!("$switch found an unknown argument: {k}")));
        }
    }
    let branches = match d.get("branches") {
        Some(Bson::Array(b)) => b,
        Some(o) => return Err(err(40061, format!("$switch expected an array for 'branches', found: {}", type_name(o)))),
        None => return Err(err(40068, "$switch requires at least one branch.")),
    };
    if branches.is_empty() {
        return Err(err(40068, "$switch requires at least one branch."));
    }
    for b in branches {
        let bd = b.as_document().ok_or_else(|| err(40062, format!("$switch expected each branch to be an object, found: {}", type_name(b))))?;
        for k in bd.keys() {
            if !matches!(k.as_str(), "case" | "then") {
                return Err(err(40063, format!("$switch found an unknown argument to a branch: {k}")));
            }
        }
        if !bd.contains_key("case") {
            return Err(err(40064, "$switch requires each branch have a 'case' expression"));
        }
        if !bd.contains_key("then") {
            return Err(err(40065, "$switch requires each branch have a 'then' expression."));
        }
    }
    for b in branches {
        let bd = b.as_document().unwrap();
        if truthy(ev(bd.get("case").unwrap(), sc)?.as_ref()) {
            return Ok(Some(ev(bd.get("then").unwrap(), sc)?));
        }
    }
    match d.get("default") {
        Some(def) => Ok(Some(ev(def, sc)?)),
        None => Err(err(40069, "$switch could not find a matching branch for an input, and no default was specified.")),
    }
}

fn merge_objects(arg: &Bson, sc: &Scope) -> Out {
    let mut out = Document::new();
    for x in args(arg) {
        match ev(x, sc)? {
            None | Some(Bson::Null) | Some(Bson::Undefined) => {}
            Some(Bson::Document(d)) => {
                for (k, v) in d {
                    out.insert(k, v);
                }
            }
            Some(o) => return Err(err(40400, format!("$mergeObjects requires object inputs, but input {} is of type {}", crate::update::fmt_bson(&o), type_name(&o)))),
        }
    }
    some(Bson::Document(out))
}

fn reduce(arg: &Bson, sc: &Scope) -> Out {
    let d = doc_arg("$reduce", arg, 40075)?;
    for k in d.keys() {
        if !matches!(k.as_str(), "input" | "initialValue" | "in") {
            return Err(err(40076, format!("$reduce found an unknown argument: {k}")));
        }
    }
    let input = d.get("input").ok_or_else(|| err(40077, "Missing 'input' parameter to $reduce"))?;
    let init = d.get("initialValue").ok_or_else(|| err(40079, "Missing 'initialValue' parameter to $reduce"))?;
    let body = d.get("in").ok_or_else(|| err(40078, "Missing 'in' parameter to $reduce"))?;
    let input = ev(input, sc)?;
    if is_nullish(&input) {
        return null();
    }
    let Bson::Array(items) = input.unwrap() else {
        return Err(err(40080, "$reduce requires that 'input' be an array"));
    };
    let mut acc = ev(init, sc)?.unwrap_or(Bson::Null);
    for it in items {
        let inner = sc.with("value", acc.clone()).with("this", it);
        acc = ev(body, &inner)?.unwrap_or(Bson::Null);
    }
    some(acc)
}

fn object_to_array(arg: &Bson, sc: &Scope) -> Out {
    let a = exact_args("$objectToArray", arg, 1)?;
    match ev(a[0], sc)? {
        v if is_nullish(&v) => null(),
        Some(Bson::Document(d)) => some(Bson::Array(d.into_iter().map(|(k, v)| Bson::Document(doc! { "k": k, "v": v })).collect())),
        Some(o) => Err(err(40390, format!("$objectToArray requires a document input, found: {}", type_name(&o)))),
        None => null(),
    }
}

fn array_to_object(arg: &Bson, sc: &Scope) -> Out {
    let a = exact_args("$arrayToObject", arg, 1)?;
    let v = ev(a[0], sc)?;
    if is_nullish(&v) {
        return null();
    }
    let Bson::Array(items) = v.unwrap() else {
        return Err(err(5153215, "$arrayToObject requires an array input"));
    };
    let mut out = Document::new();
    for it in items {
        let (k, v) = match &it {
            Bson::Array(p) => {
                if p.len() != 2 {
                    return Err(err(40397, format!("$arrayToObject requires an array of size 2 arrays,found array of size: {}", p.len())));
                }
                (p[0].clone(), p[1].clone())
            }
            Bson::Document(d) => {
                if d.len() != 2 {
                    return Err(err(40392, format!("$arrayToObject requires an object keys of 'k' and 'v'. Found incorrect number of keys:{}", d.len())));
                }
                match (d.get("k"), d.get("v")) {
                    (Some(k), Some(v)) => (k.clone(), v.clone()),
                    _ => return Err(err(40393, "$arrayToObject requires an object with keys 'k' and 'v'. Missing either or both keys from: ".to_string() + &crate::update::fmt_doc(d))),
                }
            }
            o => return Err(err(40398, format!("Unrecognised input type format for $arrayToObject: {}", type_name(o)))),
        };
        let Bson::String(k) = k else {
            return Err(err(40395, format!("$arrayToObject requires an object keys of 'k' and 'v', where the value of 'k' must be of type string. Found type: {}", type_name(&k))));
        };
        if k.contains('\0') {
            return Err(err(9, "Key field cannot contain an embedded null byte"));
        }
        if matches!(v, Bson::Null) && false {
            continue;
        }
        out.insert(k, v);
    }
    some(Bson::Document(out))
}

fn field_name(v: Option<Bson>, who: &str) -> R<String> {
    match v {
        Some(Bson::String(s)) => Ok(s),
        o => Err(err(3041704, format!("{who} requires 'field' to evaluate to a constant string, but got {}", o.as_ref().map(type_name).unwrap_or("missing")))),
    }
}

fn get_field(arg: &Bson, sc: &Scope) -> Out {
    let (field, input) = match arg {
        Bson::Document(d) if d.contains_key("field") => {
            for k in d.keys() {
                if !matches!(k.as_str(), "field" | "input") {
                    return Err(err(3041701, format!("$getField found an unknown argument: {k}")));
                }
            }
            (ev(d.get("field").unwrap(), sc)?, match d.get("input") {
                Some(i) => ev(i, sc)?,
                None => Some(Bson::Document(sc.root.clone())),
            })
        }
        other => (ev(other, sc)?, Some(Bson::Document(sc.root.clone()))),
    };
    let f = field_name(field, "$getField")?;
    match input {
        v if is_nullish(&v) => null(),
        Some(Bson::Document(d)) => Ok(Some(d.get(&f).cloned())),
        Some(_) => missing(),
        None => null(),
    }
}

fn set_field(arg: &Bson, sc: &Scope, unset: bool) -> Out {
    let who = if unset { "$unsetField" } else { "$setField" };
    let d = doc_arg(who, arg, 4161100)?;
    let field = field_name(ev(d.get("field").ok_or_else(|| err(4161102, format!("{who} requires 'field' to be specified")))?, sc)?, who)?;
    let input = ev(d.get("input").ok_or_else(|| err(4161103, format!("{who} requires 'input' to be specified")))?, sc)?;
    if is_nullish(&input) {
        return null();
    }
    let Some(Bson::Document(mut doc)) = input else {
        return Err(err(4161109, format!("{who} requires 'input' to evaluate to type Object")));
    };
    if unset {
        doc.remove(&field);
    } else {
        let v = ev(d.get("value").ok_or_else(|| err(4161105, "$setField requires 'value' to be specified"))?, sc)?;
        match v {
            Some(v) => {
                doc.insert(field, v);
            }
            None => {
                doc.remove(&field);
            }
        }
    }
    some(Bson::Document(doc))
}

fn range(arg: &Bson, sc: &Scope) -> Out {
    let a = args(arg);
    if !(2..=3).contains(&a.len()) {
        return Err(err(28667, format!("Expression $range takes at least 2 arguments, and at most 3, but {} were passed in.", a.len())));
    }
    let get = |i: usize, label: &str, code: (i32, i32)| -> R<i64> {
        let v = ev(a[i], sc)?;
        match v.as_ref().and_then(as_num) {
            Some(n) => int_exact(&num_bson(n)).and_then(|x| i32::try_from(x).ok().map(|_| x)).ok_or_else(|| err(code.1, format!("$range requires a 32-bit integral {label} value, found value: {}", v.as_ref().map(crate::update::fmt_bson).unwrap_or_default()))),
            None => Err(err(code.0, format!("$range requires a numeric {label} value, found value type: {}", v.as_ref().map(type_name).unwrap_or("missing")))),
        }
    };
    let start = get(0, "starting", (34443, 34444))?;
    let end = get(1, "ending", (34445, 34446))?;
    let step = if a.len() == 3 { get(2, "step", (34447, 34448))? } else { 1 };
    if step == 0 {
        return Err(err(34449, "$range requires a non-zero step value"));
    }
    let mut out = Vec::new();
    let mut x = start;
    while (step > 0 && x < end) || (step < 0 && x > end) {
        out.push(Bson::Int32(x as i32));
        x += step;
        if out.len() > 10_000_000 {
            break;
        }
    }
    some(Bson::Array(out))
}

fn index_of_array(arg: &Bson, sc: &Scope) -> Out {
    let a = args(arg);
    if !(2..=4).contains(&a.len()) {
        return Err(err(28667, format!("Expression $indexOfArray takes at least 2 arguments, and at most 4, but {} were passed in.", a.len())));
    }
    let arr = ev(a[0], sc)?;
    if is_nullish(&arr) {
        return null();
    }
    let Some(Bson::Array(arr)) = arr else {
        return Err(err(40090, "$indexOfArray requires an array as a first argument"));
    };
    let needle = ev(a[1], sc)?.unwrap_or(Bson::Null);
    let idx = |i: usize, dflt: i64| -> R<i64> {
        if a.len() <= i {
            return Ok(dflt);
        }
        match ev(a[i], sc)?.as_ref().and_then(int_exact) {
            Some(n) if n >= 0 => Ok(n),
            Some(_) => Err(err(40096, "$indexOfArray requires a nonnegative integer index")),
            None => Err(err(40096, "$indexOfArray requires an integral index")),
        }
    };
    let start = idx(2, 0)? as usize;
    let end = (idx(3, arr.len() as i64)? as usize).min(arr.len());
    for i in start..end {
        if equal(&arr[i], &needle) {
            return some(Bson::Int32(i as i32));
        }
    }
    some(Bson::Int32(-1))
}

fn n_of(name: &str, arg: &Bson, sc: &Scope) -> Out {
    let d = doc_arg(name, arg, 5787900)?;
    let n = ev(d.get("n").ok_or_else(|| err(5787906, format!("Missing value for 'n'")))?, sc)?;
    let input = ev(d.get("input").ok_or_else(|| err(5787907, "Missing value for 'input'"))?, sc)?;
    let n = match n.as_ref().and_then(int_exact) {
        Some(n) if n > 0 => n as usize,
        Some(_) => return Err(err(5787908, format!("'n' must be greater than 0, found {}", n.as_ref().map(crate::update::fmt_bson).unwrap_or_default()))),
        None => return Err(err(5787902, "Value for 'n' must be of integral type")),
    };
    if is_nullish(&input) {
        return null();
    }
    let Some(Bson::Array(items)) = input else {
        return Err(err(5787902, format!("{name} requires 'input' to be an array")));
    };
    let mut out: Vec<Bson> = match name {
        "$firstN" => items.into_iter().take(n).collect(),
        "$lastN" => {
            let skip = items.len().saturating_sub(n);
            items.into_iter().skip(skip).collect()
        }
        _ => {
            let mut v: Vec<Bson> = items.into_iter().filter(|x| !matches!(x, Bson::Null | Bson::Undefined)).collect();
            v.sort_by(compare);
            if name == "$maxN" {
                v.reverse();
            }
            v.truncate(n);
            v
        }
    };
    let _ = &mut out;
    some(Bson::Array(out))
}

fn sort_array(arg: &Bson, sc: &Scope) -> Out {
    let d = doc_arg("$sortArray", arg, 2942500)?;
    let input = ev(d.get("input").ok_or_else(|| err(2942501, "$sortArray requires an 'input' field"))?, sc)?;
    let by = d.get("sortBy").ok_or_else(|| err(2942502, "$sortArray requires a 'sortBy' field"))?;
    if is_nullish(&input) {
        return null();
    }
    let Some(Bson::Array(mut items)) = input else {
        return Err(err(2942504, "The input to $sortArray must be an array"));
    };
    match by {
        Bson::Document(spec) => {
            let spec = crate::query::parse_sort(spec)?;
            let mut docs: Vec<Document> = items.iter().map(|b| match b { Bson::Document(d) => d.clone(), o => doc! { "__v": o.clone() } }).collect();
            crate::query::sort_docs(&mut docs, &spec);
            items = docs.into_iter().map(Bson::Document).collect();
        }
        other => {
            let dir = int_exact(other).filter(|d| *d == 1 || *d == -1).ok_or_else(|| err(2942506, "The sort specification must be 1 or -1, or an object"))?;
            items.sort_by(compare);
            if dir == -1 {
                items.reverse();
            }
        }
    }
    some(Bson::Array(items))
}

fn zip(arg: &Bson, sc: &Scope) -> Out {
    let d = doc_arg("$zip", arg, 34460)?;
    for k in d.keys() {
        if !matches!(k.as_str(), "inputs" | "useLongestLength" | "defaults") {
            return Err(err(34464, format!("$zip found an unknown argument: {k}")));
        }
    }
    let inputs = match d.get("inputs") {
        Some(Bson::Array(a)) => a.clone(),
        Some(o) => return Err(err(34461, format!("inputs must be an array of expressions, found: {}", type_name(o)))),
        None => return Err(err(34465, "$zip requires at least one input array")),
    };
    let longest = truthy(match d.get("useLongestLength") { Some(e) => ev(e, sc)?, None => Some(Bson::Boolean(false)) }.as_ref());
    let mut arrays: Vec<Vec<Bson>> = Vec::new();
    for e in &inputs {
        match ev(e, sc)? {
            v if is_nullish(&v) => return null(),
            Some(Bson::Array(a)) => arrays.push(a),
            Some(o) => return Err(err(34468, format!("$zip found a non-array expression in input: {}", type_name(&o)))),
            None => return null(),
        }
    }
    let defaults: Vec<Bson> = match d.get("defaults") {
        Some(e) => match ev(e, sc)? {
            Some(Bson::Array(a)) => a,
            _ => return Err(err(34462, "defaults must be an array of expressions")),
        },
        None => vec![],
    };
    let n = if longest { arrays.iter().map(Vec::len).max().unwrap_or(0) } else { arrays.iter().map(Vec::len).min().unwrap_or(0) };
    let mut out = Vec::new();
    for i in 0..n {
        out.push(Bson::Array(arrays.iter().enumerate().map(|(k, a)| a.get(i).cloned().unwrap_or_else(|| defaults.get(k).cloned().unwrap_or(Bson::Null))).collect()));
    }
    some(Bson::Array(out))
}

fn set_op(name: &str, arg: &Bson, sc: &Scope) -> Out {
    let a = args(arg);
    if matches!(name, "$setDifference" | "$setIsSubset") && a.len() != 2 {
        return Err(err(16020, format!("Expression {name} takes exactly 2 arguments. {} were passed in.", a.len())));
    }
    let mut sets: Vec<Vec<Bson>> = Vec::new();
    for x in a {
        match ev(x, sc)? {
            v if is_nullish(&v) => return null(),
            Some(Bson::Array(v)) => sets.push(v),
            Some(o) => {
                let code = match name { "$setIntersection" => 17047, "$setDifference" => 17048, "$setEquals" => 17044, _ => 17046 };
                return Err(err(code, format!("All operands of {name} must be arrays. One argument is of type: {}", type_name(&o))));
            }
            None => return null(),
        }
    }
    let dedupe = |v: Vec<Bson>| -> Vec<Bson> {
        let mut out: Vec<Bson> = Vec::new();
        for x in v {
            if !out.iter().any(|y| equal(y, &x)) {
                out.push(x);
            }
        }
        out
    };
    let has = |s: &Vec<Bson>, x: &Bson| s.iter().any(|y| equal(y, x));
    match name {
        "$setIntersection" => {
            let mut it = sets.into_iter();
            let mut acc = dedupe(it.next().unwrap_or_default());
            for s in it {
                acc.retain(|x| has(&s, x));
            }
            acc.sort_by(compare);
            some(Bson::Array(acc))
        }
        "$setDifference" => {
            let mut acc = dedupe(sets[0].clone());
            acc.retain(|x| !has(&sets[1], x));
            some(Bson::Array(acc))
        }
        "$setEquals" => {
            if sets.len() < 2 {
                return Err(err(17045, "$setEquals needs at least two arguments had: ".to_string() + &sets.len().to_string()));
            }
            let first = dedupe(sets[0].clone());
            some(Bson::Boolean(sets.iter().all(|s| {
                let d = dedupe(s.clone());
                d.len() == first.len() && d.iter().all(|x| has(&first, x))
            })))
        }
        _ => some(Bson::Boolean(sets[0].iter().all(|x| has(&sets[1], x)))),
    }
}

/// `$max`, `$min`, `$sum`, `$avg` as expressions: over several arguments, or over one array.
fn agg_over(name: &str, arg: &Bson, sc: &Scope) -> Out {
    let a = args(arg);
    let mut vals: Vec<Bson> = Vec::new();
    if a.len() == 1 {
        match ev(a[0], sc)? {
            Some(Bson::Array(items)) => vals = items,
            Some(o) => vals.push(o),
            None => {}
        }
    } else {
        for x in a {
            if let Some(v) = ev(x, sc)? {
                vals.push(v);
            }
        }
    }
    match name {
        "$max" | "$min" => {
            let mut best: Option<Bson> = None;
            for v in vals.into_iter().filter(|v| !matches!(v, Bson::Null | Bson::Undefined)) {
                best = Some(match best {
                    None => v,
                    Some(b) => {
                        let o = compare(&v, &b);
                        if (name == "$max" && o == Ordering::Greater) || (name == "$min" && o == Ordering::Less) { v } else { b }
                    }
                });
            }
            some(best.unwrap_or(Bson::Null))
        }
        "$sum" => {
            let mut acc = crate::dsum::DSum::new();
            for v in vals.iter().filter(|v| as_num(v).is_some()) {
                acc.add(v);
            }
            some(acc.value())
        }
        _ => {
            let mut acc = crate::dsum::DSum::new();
            let mut n = 0usize;
            for v in vals.iter().filter(|v| as_num(v).is_some()) {
                acc.add(v);
                n += 1;
            }
            if n == 0 {
                null()
            } else {
                some(Bson::Double(acc.total() / n as f64))
            }
        }
    }
}

pub(super) fn type_alias(v: &Bson) -> &'static str {
    match v {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Document(_) => "object",
        Bson::Array(_) => "array",
        Bson::Binary(_) => "binData",
        Bson::Undefined => "undefined",
        Bson::ObjectId(_) => "objectId",
        Bson::Boolean(_) => "bool",
        Bson::DateTime(_) => "date",
        Bson::Null => "null",
        Bson::RegularExpression(_) => "regex",
        Bson::DbPointer(_) => "dbPointer",
        Bson::JavaScriptCode(_) => "javascript",
        Bson::Symbol(_) => "symbol",
        Bson::JavaScriptCodeWithScope(_) => "javascriptWithScope",
        Bson::Int32(_) => "int",
        Bson::Timestamp(_) => "timestamp",
        Bson::Int64(_) => "long",
        Bson::Decimal128(_) => "decimal",
        Bson::MinKey => "minKey",
        Bson::MaxKey => "maxKey",
    }
}

// ---- math -----------------------------------------------------------------------------------------

fn math(name: &str, arg: &Bson, sc: &Scope) -> Out {
    let a = args(arg);
    let max_args = match name {
        "$pow" | "$log" => (2, 2),
        "$round" | "$trunc" => (1, 2),
        _ => (1, 1),
    };
    if a.len() < max_args.0 || a.len() > max_args.1 {
        return Err(err(
            if max_args.0 == max_args.1 { 16020 } else { 28667 },
            if max_args.0 == max_args.1 {
                format!("Expression {name} takes exactly {} arguments. {} were passed in.", max_args.0, a.len())
            } else {
                format!("Expression {name} takes at least {} arguments, and at most {}, but {} were passed in.", max_args.0, max_args.1, a.len())
            },
        ));
    }
    let vals: Vec<Option<Bson>> = a.iter().map(|x| ev(x, sc)).collect::<R<_>>()?;
    if vals.iter().any(is_nullish) {
        return null();
    }
    let nums: Vec<Num> = vals
        .iter()
        .map(|v| num_or_null(v).ok_or_else(|| err(28765, format!("{name} only supports numeric types, not {}", v.as_ref().map(type_name).unwrap_or("missing")))))
        .collect::<R<_>>()?;
    let x = nums[0];
    let f = to_f(x);
    let double_check = |r: f64, code: i32, msg: &str| -> Out {
        let _ = (code, msg);
        some(Bson::Double(r))
    };
    match name {
        "$abs" => match x {
            Num::I32(i) => some(if i == i32::MIN { Bson::Int64(-(i as i64)) } else { Bson::Int32(i.abs()) }),
            Num::I64(i) => match i.checked_abs() {
                Some(v) => some(Bson::Int64(v)),
                None => Err(err(6045000, "Failed to compute $abs: value overflows 64-bit integer")),
            },
            Num::F(v) => some(Bson::Double(v.abs())),
        },
        "$ceil" | "$floor" => match x {
            Num::F(v) => some(Bson::Double(if name == "$ceil" { v.ceil() } else { v.floor() })),
            other => some(num_bson(other)),
        },
        "$sqrt" => {
            if f < 0.0 {
                return Err(err(28714, "$sqrt's argument must be greater than or equal to 0"));
            }
            double_check(f.sqrt(), 0, "")
        }
        "$exp" => double_check(f.exp(), 0, ""),
        "$ln" => {
            if f <= 0.0 {
                return Err(err(28766, "$ln's argument must be a positive number"));
            }
            double_check(f.ln(), 0, "")
        }
        "$log10" => {
            if f <= 0.0 {
                return Err(err(28761, "$log10's argument must be a positive number"));
            }
            double_check(f.log10(), 0, "")
        }
        "$log" => {
            let b = to_f(nums[1]);
            if f <= 0.0 {
                return Err(err(28758, "$log's argument must be a positive number"));
            }
            if b <= 1.0 {
                return Err(err(28759, "$log's base must be a positive number not equal to 1"));
            }
            double_check(f.ln() / b.ln(), 0, "")
        }
        "$pow" => {
            let (b, e) = (nums[0], nums[1]);
            if to_f(b) == 0.0 && to_f(e) < 0.0 {
                return Err(err(28764, "$pow cannot take a base of 0 and a negative exponent"));
            }
            if let (Some(bi), Some(ei)) = (to_i(b), to_i(e)) {
                if ei >= 0 {
                    if let Some(p) = bi.checked_pow(ei.min(u32::MAX as i64) as u32) {
                        let fits32 = matches!((b, e), (Num::I32(_), Num::I32(_))) && i32::try_from(p).is_ok();
                        return some(if fits32 { Bson::Int32(p as i32) } else { Bson::Int64(p) });
                    }
                }
            }
            some(Bson::Double(to_f(b).powf(to_f(e))))
        }
        "$round" | "$trunc" => {
            let place = if nums.len() == 2 {
                match to_i(nums[1]) {
                    Some(p) if (-20..=100).contains(&p) => p,
                    Some(_) => return Err(err(51083, format!("cannot apply {name} with precision value out of range [-20, 100]"))),
                    None => return Err(err(51082, format!("{name} requires an integral place value"))),
                }
            } else {
                0
            };
            let scale = 10f64.powi(place as i32);
            let round_half_even = |v: f64| -> f64 {
                let r = v.round();
                if (v - v.trunc()).abs() == 0.5 && r % 2.0 != 0.0 { r - v.signum() } else { r }
            };
            let apply = |v: f64| -> f64 {
                if name == "$trunc" {
                    (v * scale).trunc() / scale
                } else {
                    round_half_even(v * scale) / scale
                }
            };
            match x {
                Num::F(v) => {
                    if !v.is_finite() {
                        return some(Bson::Double(v));
                    }
                    some(Bson::Double(apply(v)))
                }
                Num::I32(i) if place >= 0 => some(Bson::Int32(i)),
                Num::I64(i) if place >= 0 => some(Bson::Int64(i)),
                Num::I32(i) => some(Bson::Int32(apply(i as f64) as i32)),
                Num::I64(i) => some(Bson::Int64(apply(i as f64) as i64)),
            }
        }
        _ => Ok(None),
    }
}

// ---- strings --------------------------------------------------------------------------------------

fn to_str_lossy(v: Option<Bson>, who: &str) -> R<Option<String>> {
    Ok(match v {
        None | Some(Bson::Null) | Some(Bson::Undefined) => Some(String::new()),
        Some(Bson::String(s)) => Some(s),
        Some(Bson::Int32(i)) => Some(i.to_string()),
        Some(Bson::Int64(i)) => Some(i.to_string()),
        Some(Bson::Double(f)) => Some(f64_string(f)),
        Some(o) => return Err(err(16007, format!("can't convert from BSON type {} to String ({who})", type_name(&o)))),
    })
}

fn substr(name: &str, arg: &Bson, sc: &Scope) -> Out {
    let a = exact_args(name, arg, 3)?;
    let s = to_str_lossy(ev(a[0], sc)?, name)?.unwrap_or_default();
    let (lo, len) = (ev(a[1], sc)?, ev(a[2], sc)?);
    let lo = lo.as_ref().and_then(as_num).ok_or_else(|| err(16034, format!("{name}: starting index must be a numeric type (is BSON type {})", type_name(&Bson::Null))))?;
    let len = len.as_ref().and_then(as_num).ok_or_else(|| err(16035, format!("{name}: length must be a numeric type (is BSON type {})", type_name(&Bson::Null))))?;
    let (lo, len) = (to_f(lo) as i64, to_f(len) as i64);
    if lo < 0 {
        return some(Bson::String(String::new()));
    }
    let lo = lo as usize;
    if name == "$substrCP" {
        let chars: Vec<char> = s.chars().collect();
        if lo >= chars.len() {
            return some(Bson::String(String::new()));
        }
        let end = if len < 0 { chars.len() } else { (lo + len as usize).min(chars.len()) };
        return some(Bson::String(chars[lo..end].iter().collect()));
    }
    let bytes = s.as_bytes();
    if lo >= bytes.len() {
        return some(Bson::String(String::new()));
    }
    let end = if len < 0 { bytes.len() } else { (lo + len as usize).min(bytes.len()) };
    if !s.is_char_boundary(lo) || !s.is_char_boundary(end) {
        return Err(err(28656, format!("{name}: Invalid range, starting or ending index is a UTF-8 continuation byte.")));
    }
    some(Bson::String(s[lo..end].to_string()))
}

fn trim(name: &str, arg: &Bson, sc: &Scope) -> Out {
    let d = doc_arg(name, arg, 50696)?;
    for k in d.keys() {
        if !matches!(k.as_str(), "input" | "chars") {
            return Err(err(50694, format!("{name} found an unknown argument: {k}")));
        }
    }
    let input = ev(d.get("input").ok_or_else(|| err(50695, format!("{name} requires an 'input' field")))?, sc)?;
    let chars = match d.get("chars") {
        Some(c) => ev(c, sc)?,
        None => Some(Bson::String(" \t\n\u{b}\u{c}\r\u{85}\u{a0}\u{1680}\u{2000}\u{2001}\u{2002}\u{2003}\u{2004}\u{2005}\u{2006}\u{2007}\u{2008}\u{2009}\u{200a}\u{2028}\u{2029}\u{202f}\u{205f}\u{3000}\0".into())),
    };
    if is_nullish(&input) || (d.contains_key("chars") && is_nullish(&chars)) {
        return null();
    }
    let Some(Bson::String(s)) = input else {
        return Err(err(50699, format!("{name} requires its input to be a string, got {} (of type {}) instead.", crate::update::fmt_bson(&Bson::Null), "null")));
    };
    let Some(Bson::String(cs)) = chars else {
        return Err(err(50700, format!("{name} requires 'chars' to be a string")));
    };
    let set: Vec<char> = cs.chars().collect();
    let mut t = s.as_str();
    if name != "$rtrim" {
        t = t.trim_start_matches(|c| set.contains(&c));
    }
    if name != "$ltrim" {
        t = t.trim_end_matches(|c| set.contains(&c));
    }
    some(Bson::String(t.to_string()))
}

fn index_of_str(name: &str, arg: &Bson, sc: &Scope) -> Out {
    let a = args(arg);
    if !(2..=4).contains(&a.len()) {
        return Err(err(28667, format!("Expression {name} takes at least 2 arguments, and at most 4, but {} were passed in.", a.len())));
    }
    let s = ev(a[0], sc)?;
    if is_nullish(&s) {
        return null();
    }
    let Some(Bson::String(s)) = s else {
        return Err(err(40091, format!("{name} requires a string as the first argument, found: {}", type_name(&Bson::Null))));
    };
    let sub = ev(a[1], sc)?;
    let Some(Bson::String(sub)) = sub else {
        return Err(err(40092, format!("{name} requires a string as the second argument")));
    };
    let idx = |i: usize, dflt: i64| -> R<i64> {
        if a.len() <= i {
            return Ok(dflt);
        }
        match ev(a[i], sc)?.as_ref().and_then(int_exact) {
            Some(n) if n >= 0 => Ok(n),
            _ => Err(err(40096, format!("{name} requires a nonnegative integral index"))),
        }
    };
    let cp = name == "$indexOfCP";
    let chars: Vec<char> = s.chars().collect();
    let len = if cp { chars.len() } else { s.len() };
    let start = (idx(2, 0)? as usize).min(len);
    let end = (idx(3, len as i64)? as usize).min(len);
    if start > end {
        return some(Bson::Int32(-1));
    }
    if cp {
        let hay: String = chars[start..end].iter().collect();
        return some(Bson::Int32(match hay.find(&sub) {
            Some(b) => (start + hay[..b].chars().count()) as i32,
            None => -1,
        }));
    }
    let hay = &s.as_bytes()[start..end];
    let nb = sub.as_bytes();
    let pos = if nb.is_empty() { Some(0) } else { hay.windows(nb.len()).position(|w| w == nb) };
    some(Bson::Int32(pos.map(|p| (start + p) as i32).unwrap_or(-1)))
}

fn replace(name: &str, arg: &Bson, sc: &Scope) -> Out {
    let d = doc_arg(name, arg, 51751)?;
    let get = |k: &str| -> R<Option<Bson>> { ev(d.get(k).ok_or_else(|| err(51750, format!("{name} requires '{k}' to be specified")))?, sc) };
    let (input, find, rep) = (get("input")?, get("find")?, get("replacement")?);
    if is_nullish(&input) || is_nullish(&find) || is_nullish(&rep) {
        return null();
    }
    match (input.unwrap(), find.unwrap(), rep.unwrap()) {
        (Bson::String(s), Bson::String(f), Bson::String(r)) => some(Bson::String(if name == "$replaceOne" { s.replacen(&f, &r, 1) } else { s.replace(&f, &r) })),
        (a, _, _) if !matches!(a, Bson::String(_)) => Err(err(51746, format!("{name} requires that 'input' be a string, found: {}", type_name(&a)))),
        (_, b, _) if !matches!(b, Bson::String(_)) => Err(err(51745, format!("{name} requires that 'find' be a string, found: {}", type_name(&b)))),
        (_, _, c) => Err(err(51744, format!("{name} requires that 'replacement' be a string, found: {}", type_name(&c)))),
    }
}

fn regex_op(name: &str, arg: &Bson, sc: &Scope) -> Out {
    let d = doc_arg(name, arg, 51103)?;
    for k in d.keys() {
        if !matches!(k.as_str(), "input" | "regex" | "options") {
            return Err(err(31024, format!("{name} found an unknown argument: {k}")));
        }
    }
    let input = ev(d.get("input").ok_or_else(|| err(31022, format!("{name} requires 'input' parameter")))?, sc)?;
    let re = ev(d.get("regex").ok_or_else(|| err(31023, format!("{name} requires 'regex' parameter")))?, sc)?;
    let opts = match d.get("options") {
        Some(o) => ev(o, sc)?,
        None => None,
    };
    let (mut pat, mut flags) = match re {
        Some(Bson::String(p)) => (Some(p), String::new()),
        Some(Bson::RegularExpression(r)) => (Some(r.pattern.as_str().to_string()), r.options.as_str().to_string()),
        None | Some(Bson::Null) => (None, String::new()),
        Some(o) => return Err(err(51105, format!("{name} needs 'regex' to be of type string or regex, found: {}", type_name(&o)))),
    };
    match opts {
        Some(Bson::String(o)) => {
            if !flags.is_empty() && !o.is_empty() {
                return Err(err(51107, format!("{name}: found regex option(s) specified in both 'regex' and 'option' fields")));
            }
            if !o.is_empty() {
                flags = o;
            }
        }
        None | Some(Bson::Null) => {}
        Some(o) => return Err(err(51106, format!("{name} needs 'options' to be of type string, found: {}", type_name(&o)))),
    }
    let pat = match pat.take() {
        Some(p) => p,
        None => {
            return match name {
                "$regexMatch" => some(Bson::Boolean(false)),
                "$regexFindAll" => some(Bson::Array(vec![])),
                _ => null(),
            }
        }
    };
    let text = match input {
        None | Some(Bson::Null) | Some(Bson::Undefined) => {
            return match name {
                "$regexMatch" => some(Bson::Boolean(false)),
                "$regexFindAll" => some(Bson::Array(vec![])),
                _ => null(),
            }
        }
        Some(Bson::String(s)) => s,
        Some(o) => return Err(err(51104, format!("{name} needs 'input' to be of type string, found: {}", type_name(&o)))),
    };
    let re = crate::regex::compile(&pat, &flags).map_err(|e| if e.code == 51091 { err(51111, format!("Invalid Regex: {}", e.msg)) } else { e })?;
    let byte_to_cp = |b: usize| text[..b].chars().count() as i32;
    let found = |c: &fancy_regex::Captures<'_, String>| -> Bson {
        let m = c.get(0).unwrap();
        let caps: Vec<Bson> = (1..c.len()).map(|i| match c.get(i) {
            Some(g) => Bson::String(g.as_str().to_string()),
            None => Bson::Null,
        }).collect();
        Bson::Document(doc! { "match": m.as_str(), "idx": byte_to_cp(m.start()), "captures": caps })
    };
    let xerr = |e: fancy_regex::Error| err(51156, format!("Regular expression execution failed: {e}"));
    match name {
        "$regexMatch" => some(Bson::Boolean(re.is_match(&text).map_err(xerr)?)),
        "$regexFind" => match re.captures(&text).map_err(xerr)? {
            Some(c) => some(found(&c)),
            None => null(),
        },
        _ => {
            let mut out = Vec::new();
            let mut pos = 0;
            while pos <= text.len() {
                let Some(c) = re.captures_from_pos(&text, pos).map_err(xerr)? else { break };
                let m = c.get(0).unwrap();
                out.push(found(&c));
                pos = if m.end() == m.start() { text[m.end()..].chars().next().map(|ch| m.end() + ch.len_utf8()).unwrap_or(text.len() + 1) } else { m.end() };
            }
            some(Bson::Array(out))
        }
    }
}

// ---- conversion -------------------------------------------------------------------------------------

pub(super) fn f64_string(f: f64) -> String {
    if f.is_nan() {
        return "NaN".into();
    }
    if f.is_infinite() {
        return if f > 0.0 { "Infinity".into() } else { "-Infinity".into() };
    }
    if f == f.trunc() && f.abs() < 1e15 {
        return format!("{}", f as i64);
    }
    let s = format!("{f}");
    if f.abs() >= 1e15 || (f != 0.0 && f.abs() < 1e-5) {
        let e = format!("{f:e}");
        // 1e20 -> "1e+20"
        if let Some((m, ex)) = e.split_once('e') {
            let ex: i32 = ex.parse().unwrap_or(0);
            return format!("{m}e{}{}", if ex < 0 { "-" } else { "+" }, ex.abs());
        }
    }
    s
}

fn convert_fail(from: &Bson, to: &str, name: &str, why: &str) -> MongoError {
    let _ = name;
    err(241, format!("Failed to convert '{}' to {to} in $convert with no onError value: {why}", crate::update::fmt_bson(from))).with_name("ConversionFailure")
}

trait WithName {
    fn with_name(self, n: &'static str) -> Self;
}
impl WithName for MongoError {
    fn with_name(mut self, n: &'static str) -> Self {
        self.name = n;
        self
    }
}

pub(super) fn convert(v: &Bson, to: &str, name: &str) -> R<Bson> {
    let to = to.to_ascii_lowercase();
    let to = match to.as_str() {
        "int" | "toint" => "int",
        "long" | "tolong" => "long",
        "double" | "todouble" => "double",
        "decimal" | "todecimal" => "decimal",
        "bool" | "tobool" => "bool",
        "date" | "todate" => "date",
        "string" | "tostring" => "string",
        "objectid" | "toobjectid" => "objectId",
        "array" | "object" | "bindata" | "regex" | "timestamp" | "null" | "minkey" | "maxkey" | "javascript" | "undefined" | "symbol" => {
            return Err(err(2, format!("Unsupported conversion target type in {name}: {to}")));
        }
        other => return Err(err(2, format!("Unknown type name: {other}"))),
    };
    let fail = |why: &str| convert_fail(v, to, name, why);
    match to {
        "int" | "long" => {
            let (lo, hi) = if to == "int" { (i32::MIN as i64, i32::MAX as i64) } else { (i64::MIN, i64::MAX) };
            let n: i64 = match v {
                Bson::Int32(i) => *i as i64,
                Bson::Int64(i) => *i,
                Bson::Double(f) => {
                    if !f.is_finite() || f.trunc() < lo as f64 || f.trunc() > hi as f64 {
                        return Err(fail("Attempt to convert double to an integer that is out of range"));
                    }
                    f.trunc() as i64
                }
                Bson::Boolean(b) => *b as i64,
                Bson::String(s) => s.trim_end().parse::<i64>().map_err(|_| fail(&format!("Failed to parse number '{s}' in $convert with no onError value: Did not consume whole string.")))?,
                Bson::Decimal128(d) => d.to_string().parse::<f64>().map(|f| f.trunc() as i64).map_err(|_| fail("bad decimal"))?,
                other => return Err(fail(&format!("Unsupported conversion from {} to {to} in $convert with no onError value", type_name(other)))),
            };
            if n < lo || n > hi {
                return Err(fail("Attempt to convert to an integer that is out of range"));
            }
            Ok(if to == "int" { Bson::Int32(n as i32) } else { Bson::Int64(n) })
        }
        "double" => Ok(Bson::Double(match v {
            Bson::Int32(i) => *i as f64,
            Bson::Int64(i) => *i as f64,
            Bson::Double(f) => *f,
            Bson::Boolean(b) => *b as i32 as f64,
            Bson::String(s) => match s.trim() {
                "NaN" => f64::NAN,
                "Infinity" | "+Infinity" | "inf" => f64::INFINITY,
                "-Infinity" | "-inf" => f64::NEG_INFINITY,
                t => t.parse::<f64>().map_err(|_| fail(&format!("Failed to parse number '{s}' in $convert with no onError value: Did not consume whole string.")))?,
            },
            Bson::DateTime(d) => d.timestamp_millis() as f64,
            Bson::Decimal128(d) => d.to_string().parse::<f64>().map_err(|_| fail("bad decimal"))?,
            other => return Err(fail(&format!("Unsupported conversion from {} to double in $convert with no onError value", type_name(other)))),
        })),
        "decimal" => {
            let s = match v {
                Bson::Int32(i) => i.to_string(),
                Bson::Int64(i) => i.to_string(),
                Bson::Double(f) => double_to_decimal_string(*f),
                Bson::Boolean(b) => (*b as i32).to_string(),
                Bson::String(s) => s.clone(),
                Bson::DateTime(d) => d.timestamp_millis().to_string(),
                Bson::Decimal128(d) => return Ok(Bson::Decimal128(*d)),
                other => return Err(fail(&format!("Unsupported conversion from {} to decimal in $convert with no onError value", type_name(other)))),
            };
            s.parse::<bson::Decimal128>().map(Bson::Decimal128).map_err(|_| fail(&format!("Failed to parse number '{s}' in $convert with no onError value: Did not consume whole string.")))
        }
        "bool" => Ok(Bson::Boolean(match v {
            Bson::Boolean(b) => *b,
            Bson::Int32(i) => *i != 0,
            Bson::Int64(i) => *i != 0,
            Bson::Double(f) => *f != 0.0,
            Bson::Decimal128(d) => d.to_string().parse::<f64>().map(|f| f != 0.0).unwrap_or(true),
            _ => true,
        })),
        "date" => match v {
            Bson::DateTime(_) => Ok(v.clone()),
            Bson::Int64(i) => Ok(Bson::DateTime(DateTime::from_millis(*i))),
            Bson::Double(f) => {
                if !f.is_finite() {
                    return Err(fail("Attempt to convert a non-finite number to a date"));
                }
                Ok(Bson::DateTime(DateTime::from_millis(f.trunc() as i64)))
            }
            Bson::Timestamp(t) => Ok(Bson::DateTime(DateTime::from_millis(t.time as i64 * 1000))),
            Bson::ObjectId(o) => Ok(Bson::DateTime(o.timestamp())),
            Bson::String(s) => dates::parse_date_string(s, None, None).map(|ms| Bson::DateTime(DateTime::from_millis(ms))).map_err(|_| fail("Error parsing date string")),
            other => Err(fail(&format!("Unsupported conversion from {} to date in $convert with no onError value", type_name(other)))),
        },
        "string" => Ok(Bson::String(match v {
            Bson::String(s) => s.clone(),
            Bson::Int32(i) => i.to_string(),
            Bson::Int64(i) => i.to_string(),
            Bson::Double(f) => f64_string(*f),
            Bson::Decimal128(d) => d.to_string(),
            Bson::Boolean(b) => b.to_string(),
            Bson::ObjectId(o) => o.to_hex(),
            Bson::DateTime(d) => dates::iso(d.timestamp_millis()),
            other => return Err(fail(&format!("Unsupported conversion from {} to string in $convert with no onError value", type_name(other)))),
        })),
        _ => match v {
            Bson::ObjectId(_) => Ok(v.clone()),
            Bson::String(s) => bson::oid::ObjectId::parse_str(s).map(Bson::ObjectId).map_err(|_| fail(&format!("Failed to parse objectId '{s}' in $convert with no onError value: Invalid string length for parsing to OID, expected 24 but found {}", s.len()))),
            other => Err(fail(&format!("Unsupported conversion from {} to objectId in $convert with no onError value", type_name(other)))),
        },
    }
}

fn convert_op(arg: &Bson, sc: &Scope) -> Out {
    let d = doc_arg("$convert", arg, 4)?;
    for k in d.keys() {
        if !matches!(k.as_str(), "input" | "to" | "onError" | "onNull" | "format" | "base") {
            return Err(err(9, format!("$convert found an unknown argument: {k}")));
        }
    }
    let input = ev(d.get("input").ok_or_else(|| err(9, "Missing 'input' parameter to $convert"))?, sc)?;
    let to = ev(d.get("to").ok_or_else(|| err(9, "Missing 'to' parameter to $convert"))?, sc)?;
    let to = match to {
        Some(Bson::String(s)) => s,
        Some(Bson::Int32(c)) => code_to_name(c as i64)?,
        Some(Bson::Int64(c)) => code_to_name(c)?,
        Some(Bson::Double(c)) => code_to_name(c as i64)?,
        Some(Bson::Document(td)) => match td.get("type") {
            Some(Bson::String(s)) => s.clone(),
            _ => return Err(err(9, "$convert 'to' object needs a 'type'")),
        },
        None | Some(Bson::Null) => return null(),
        Some(o) => return Err(err(4, format!("$convert's 'to' argument must be a string or number, but is {}", type_name(&o)))),
    };
    if is_nullish(&input) {
        return match d.get("onNull") {
            Some(e) => Ok(Some(ev(e, sc)?)),
            None => null(),
        };
    }
    match convert(&input.unwrap(), &to, "$convert") {
        Ok(v) => some(v),
        Err(e) => match d.get("onError") {
            Some(oe) if e.code == 241 => Ok(Some(ev(oe, sc)?)),
            _ => Err(e),
        },
    }
}

fn code_to_name(c: i64) -> R<String> {
    Ok(match c {
        1 => "double",
        2 => "string",
        7 => "objectId",
        8 => "bool",
        9 => "date",
        16 => "int",
        18 => "long",
        19 => "decimal",
        other => return Err(err(2, format!("In $convert, numeric value for 'to' does not correspond to a BSON type: {other}"))),
    }
    .to_string())
}

fn function(arg: &Bson, sc: &Scope) -> Out {
    let d = doc_arg("$function", arg, 31260)?;
    let body = match d.get("body") {
        Some(Bson::String(s)) => s.clone(),
        Some(Bson::JavaScriptCode(c)) => c.clone(),
        None => return Err(err(31261, "$function requires 'body' to be specified")),
        _ => return Err(err(31262, "$function requires 'body' to be a string or code")),
    };
    let args = match d.get("args") {
        Some(Bson::Array(a)) => a.clone(),
        None => return Err(err(31263, "$function requires 'args' to be specified")),
        _ => return Err(err(31266, "$function requires 'args' to be an array")),
    };
    match d.get("lang") {
        Some(Bson::String(l)) if l == "js" => {}
        None => return Err(err(31418, "$function requires 'lang' to be specified")),
        _ => return Err(err(31419, "$function only supports lang: 'js'")),
    }
    let mut vals = Vec::new();
    for a in &args {
        vals.push(ev(a, sc)?.unwrap_or(Bson::Null));
    }
    some(crate::js::call_function(&body, &vals)?)
}

#[allow(dead_code)]
fn _rank(v: &Bson) -> u8 {
    type_rank(v)
}

/// A double as MongoDB converts it to Decimal128: rounded to 15 significant digits, trailing zeros kept.
fn double_to_decimal_string(f: f64) -> String {
    if f.is_nan() {
        return "NaN".into();
    }
    if f.is_infinite() {
        return if f > 0.0 { "Infinity".into() } else { "-Infinity".into() };
    }
    if f == 0.0 {
        return "0".into();
    }
    let e = format!("{:.14e}", f);
    let exp: i32 = e.split('e').nth(1).and_then(|x| x.parse().ok()).unwrap_or(0);
    let decimals = 14 - exp;
    if (0..=33).contains(&decimals) {
        format!("{:.*}", decimals as usize, f)
    } else {
        e.replace('e', "E")
    }
}
