//! `$jsonSchema` (the subset MongoDB supports: draft 4 keywords plus `bsonType`).

use crate::error::MongoError;
use crate::query::{as_f64, compare, equal, is_numeric};
use bson::{Bson, Document};

type R<T> = Result<T, MongoError>;

fn bad(msg: impl Into<String>) -> MongoError {
    MongoError::new(2, "BadValue", msg.into())
}

fn ty(msg: impl Into<String>) -> MongoError {
    MongoError::new(14, "TypeMismatch", msg.into())
}

fn bson_type_matches(v: &Bson, t: &str) -> bool {
    match t {
        "double" => matches!(v, Bson::Double(_)),
        "string" => matches!(v, Bson::String(_)),
        "object" => matches!(v, Bson::Document(_)),
        "array" => matches!(v, Bson::Array(_)),
        "binData" => matches!(v, Bson::Binary(_)),
        "undefined" => matches!(v, Bson::Undefined),
        "objectId" => matches!(v, Bson::ObjectId(_)),
        "bool" => matches!(v, Bson::Boolean(_)),
        "date" => matches!(v, Bson::DateTime(_)),
        "null" => matches!(v, Bson::Null),
        "regex" => matches!(v, Bson::RegularExpression(_)),
        "javascript" => matches!(v, Bson::JavaScriptCode(_)),
        "int" => matches!(v, Bson::Int32(_)),
        "timestamp" => matches!(v, Bson::Timestamp(_)),
        "long" => matches!(v, Bson::Int64(_)),
        "decimal" => matches!(v, Bson::Decimal128(_)),
        "minKey" => matches!(v, Bson::MinKey),
        "maxKey" => matches!(v, Bson::MaxKey),
        "number" => is_numeric(v),
        _ => false,
    }
}

fn json_type_matches(v: &Bson, t: &str) -> bool {
    match t {
        "object" => matches!(v, Bson::Document(_)),
        "array" => matches!(v, Bson::Array(_)),
        "number" => is_numeric(v),
        "boolean" => matches!(v, Bson::Boolean(_)),
        "string" => matches!(v, Bson::String(_)),
        "null" => matches!(v, Bson::Null),
        _ => false,
    }
}

fn types_ok(v: &Bson, spec: &Bson, json: bool) -> R<bool> {
    let names: Vec<&str> = match spec {
        Bson::String(s) => vec![s.as_str()],
        Bson::Array(a) => a.iter().filter_map(|x| x.as_str()).collect(),
        _ => return Err(bad("$jsonSchema keyword 'type'/'bsonType' must be a string or an array of strings")),
    };
    let known = ["double", "string", "object", "array", "binData", "undefined", "objectId", "bool", "date", "null", "regex", "javascript", "int", "timestamp", "long", "decimal", "minKey", "maxKey", "number", "boolean"];
    for n in &names {
        if !known.contains(n) {
            return Err(bad(format!("Unknown type name alias: {n}")));
        }
    }
    Ok(names.iter().any(|n| if json { json_type_matches(v, n) } else { bson_type_matches(v, n) }))
}

/// Does `v` satisfy the schema?
pub fn validate(v: &Bson, schema: &Document) -> R<bool> {
    for (k, s) in schema {
        let ok = match k.as_str() {
            "bsonType" => types_ok(v, s, false)?,
            "type" => types_ok(v, s, true)?,
            "enum" => match s {
                Bson::Array(a) => a.iter().any(|e| equal(e, v)),
                _ => return Err(ty("$jsonSchema keyword 'enum' must be an array")),
            },
            "allOf" | "anyOf" | "oneOf" => {
                let Bson::Array(a) = s else { return Err(ty(format!("$jsonSchema keyword '{k}' must be a non-empty array"))) };
                if a.is_empty() {
                    return Err(bad(format!("$jsonSchema keyword '{k}' must be a non-empty array")));
                }
                let mut hits = 0;
                for sub in a {
                    let Bson::Document(sd) = sub else { return Err(ty(format!("$jsonSchema keyword '{k}' must contain only objects"))) };
                    if validate(v, sd)? {
                        hits += 1;
                    }
                }
                match k.as_str() {
                    "allOf" => hits == a.len(),
                    "anyOf" => hits > 0,
                    _ => hits == 1,
                }
            }
            "not" => match s {
                Bson::Document(sd) => !validate(v, sd)?,
                _ => return Err(ty("$jsonSchema keyword 'not' must be an object")),
            },
            "exclusiveMinimum" | "exclusiveMaximum" if !matches!(s, Bson::Boolean(_)) => {
                if !is_numeric(v) {
                    true
                } else if !is_numeric(s) {
                    return Err(ty(format!("$jsonSchema keyword '{k}' must be a number or a boolean")));
                } else if k == "exclusiveMinimum" {
                    as_f64(v) > as_f64(s)
                } else {
                    as_f64(v) < as_f64(s)
                }
            }
            "exclusiveMinimum" | "exclusiveMaximum" => true,
            "minimum" | "maximum" | "multipleOf" => {
                if !is_numeric(v) {
                    true
                } else {
                    let x = as_f64(v);
                    let b = if is_numeric(s) { as_f64(s) } else { return Err(ty(format!("$jsonSchema keyword '{k}' must be a number"))) };
                    let excl_min = matches!(schema.get("exclusiveMinimum"), Some(Bson::Boolean(true)));
                    let excl_max = matches!(schema.get("exclusiveMaximum"), Some(Bson::Boolean(true)));
                    match k.as_str() {
                        "minimum" => if excl_min { x > b } else { x >= b },
                        "maximum" => if excl_max { x < b } else { x <= b },
                        "multipleOf" => b != 0.0 && ((x / b).fract()).abs() < 1e-12,
                        _ => true,
                    }
                }
            }
            "minLength" | "maxLength" => match v {
                Bson::String(st) => {
                    let n = st.chars().count() as i64;
                    let b = crate::query::int_arg(s).ok_or_else(|| ty(format!("$jsonSchema keyword '{k}' must be a non-negative integer")))?;
                    if k == "minLength" { n >= b } else { n <= b }
                }
                _ => true,
            },
            "pattern" => match (v, s) {
                (Bson::String(st), Bson::String(p)) => crate::regex::matches_str(p, "", st)?,
                (_, Bson::String(_)) => true,
                _ => return Err(ty("$jsonSchema keyword 'pattern' must be a string")),
            },
            "required" => match (v, s) {
                (Bson::Document(d), Bson::Array(a)) => a.iter().all(|f| f.as_str().map(|f| d.contains_key(f)).unwrap_or(false)),
                (_, Bson::Array(_)) => true,
                _ => return Err(ty("$jsonSchema keyword 'required' must be an array of strings")),
            },
            "properties" => match (v, s) {
                (Bson::Document(d), Bson::Document(props)) => {
                    let mut ok = true;
                    for (name, sub) in props {
                        let Bson::Document(sd) = sub else { return Err(ty("$jsonSchema keyword 'properties' must be an object of objects")) };
                        if let Some(val) = d.get(name) {
                            if !validate(val, sd)? {
                                ok = false;
                            }
                        }
                    }
                    ok
                }
                (_, Bson::Document(_)) => true,
                _ => return Err(ty("$jsonSchema keyword 'properties' must be an object")),
            },
            "patternProperties" => match (v, s) {
                (Bson::Document(d), Bson::Document(pp)) => {
                    let mut ok = true;
                    for (pat, sub) in pp {
                        let Bson::Document(sd) = sub else { return Err(ty("$jsonSchema keyword 'patternProperties' must be an object of objects")) };
                        for (name, val) in d {
                            if crate::regex::matches_str(pat, "", name)? && !validate(val, sd)? {
                                ok = false;
                            }
                        }
                    }
                    ok
                }
                _ => true,
            },
            "additionalProperties" => match v {
                Bson::Document(d) => {
                    let props = schema.get_document("properties").ok();
                    let pats: Vec<&String> = schema.get_document("patternProperties").map(|p| p.keys().collect()).unwrap_or_default();
                    let mut ok = true;
                    for (name, val) in d {
                        if props.map(|p| p.contains_key(name)).unwrap_or(false) {
                            continue;
                        }
                        let mut covered = false;
                        for p in &pats {
                            if crate::regex::matches_str(p, "", name)? {
                                covered = true;
                            }
                        }
                        if covered {
                            continue;
                        }
                        match s {
                            Bson::Boolean(false) => ok = false,
                            Bson::Boolean(true) => {}
                            Bson::Document(sd) => {
                                if !validate(val, sd)? {
                                    ok = false;
                                }
                            }
                            _ => return Err(ty("$jsonSchema keyword 'additionalProperties' must be a boolean or an object")),
                        }
                    }
                    ok
                }
                _ => true,
            },
            "minProperties" | "maxProperties" => match v {
                Bson::Document(d) => {
                    let b = crate::query::int_arg(s).ok_or_else(|| ty(format!("$jsonSchema keyword '{k}' must be a non-negative integer")))?;
                    if k == "minProperties" { d.len() as i64 >= b } else { d.len() as i64 <= b }
                }
                _ => true,
            },
            "items" => match v {
                Bson::Array(a) => match s {
                    Bson::Document(sd) => {
                        let mut ok = true;
                        for e in a {
                            if !validate(e, sd)? {
                                ok = false;
                            }
                        }
                        ok
                    }
                    Bson::Array(list) => {
                        let mut ok = true;
                        for (e, sub) in a.iter().zip(list) {
                            if let Bson::Document(sd) = sub {
                                if !validate(e, sd)? {
                                    ok = false;
                                }
                            }
                        }
                        if a.len() > list.len() {
                            match schema.get("additionalItems") {
                                Some(Bson::Boolean(false)) => ok = false,
                                Some(Bson::Document(sd)) => {
                                    for e in &a[list.len()..] {
                                        if !validate(e, sd)? {
                                            ok = false;
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        ok
                    }
                    _ => return Err(ty("$jsonSchema keyword 'items' must be an object or an array")),
                },
                _ => true,
            },
            "minItems" | "maxItems" => match v {
                Bson::Array(a) => {
                    let b = crate::query::int_arg(s).ok_or_else(|| ty(format!("$jsonSchema keyword '{k}' must be a non-negative integer")))?;
                    if k == "minItems" { a.len() as i64 >= b } else { a.len() as i64 <= b }
                }
                _ => true,
            },
            "uniqueItems" => match (v, s) {
                (Bson::Array(a), Bson::Boolean(true)) => (0..a.len()).all(|i| (i + 1..a.len()).all(|j| compare(&a[i], &a[j]) != std::cmp::Ordering::Equal)),
                _ => true,
            },
            "dependencies" => match (v, s) {
                (Bson::Document(d), Bson::Document(deps)) => deps.iter().all(|(field, dep)| {
                    if !d.contains_key(field) {
                        return true;
                    }
                    match dep {
                        Bson::Array(a) => a.iter().all(|x| x.as_str().map(|f| d.contains_key(f)).unwrap_or(false)),
                        Bson::Document(sd) => validate(v, sd).unwrap_or(false),
                        _ => true,
                    }
                }),
                _ => true,
            },
            "title" | "description" | "additionalItems" | "$schema" | "id" | "default" | "definitions" => true,
            other => return Err(MongoError::new(9, "FailedToParse", format!("Unknown $jsonSchema keyword: {other}"))),
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
}
