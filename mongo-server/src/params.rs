//! `getParameter` / `setParameter`: the server parameters of mongod 7.0 (names, values and whether they can be changed at
//! runtime, captured from a mongod; `src/diag/params_{sa,rs}.json`). A parameter set at runtime keeps its new value and is
//! reported by `getParameter`; only a few of them change what the server does (the JavaScript limits and the transaction
//! lifetime).

use crate::cmd::R;
use crate::error::MongoError;
use bson::{doc, Bson, Document};
use std::collections::HashMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Mutex, OnceLock};

const SA: &str = include_str!("diag/params_sa.json");
const RS: &str = include_str!("diag/params_rs.json");

struct Table {
    /// name -> {value, settableAtRuntime, settableAtStartup}
    params: Document,
    help: String,
}

fn table(rs: bool) -> &'static Table {
    static T: OnceLock<(Table, Table)> = OnceLock::new();
    let t = T.get_or_init(|| {
        let load = |s: &str| {
            let d = match serde_json::from_str::<Bson>(s) {
                Ok(Bson::Document(d)) => d,
                _ => Document::new(),
            };
            Table { params: d.get_document("params").cloned().unwrap_or_default(), help: d.get_str("help").unwrap_or_default().to_string() }
        };
        (load(SA), load(RS))
    });
    if rs {
        &t.1
    } else {
        &t.0
    }
}

fn overlay() -> &'static Mutex<HashMap<String, Bson>> {
    static O: OnceLock<Mutex<HashMap<String, Bson>>> = OnceLock::new();
    O.get_or_init(Default::default)
}

/// The current value of a parameter.
fn current(name: &str, captured: &Bson) -> Bson {
    match name {
        "internalQueryJavaScriptFnTimeoutMillis" => return Bson::Int32(crate::js::TIMEOUT_MS.load(Relaxed) as i32),
        "jsHeapLimitMB" => return Bson::Int32(crate::js::HEAP_MB.load(Relaxed) as i32),
        "transactionLifetimeLimitSeconds" => return Bson::Int32(crate::txn::LIFETIME_SECS.load(Relaxed) as i32),
        _ => {}
    }
    overlay().lock().unwrap().get(name).cloned().unwrap_or_else(|| captured.clone())
}

fn flag(v: &Bson) -> bool {
    match v {
        Bson::Boolean(b) => *b,
        Bson::Int32(i) => *i != 0,
        Bson::Int64(i) => *i != 0,
        Bson::Double(f) => *f != 0.0,
        Bson::Null | Bson::Undefined => false,
        _ => true,
    }
}

pub(crate) fn get_parameter(cmd: &Document, rs: bool) -> R<Document> {
    let t = table(rs);
    let (mut all, mut details) = (false, false);
    match cmd.get("getParameter") {
        Some(Bson::String(s)) if s == "*" => all = true,
        Some(Bson::Document(o)) => {
            details = o.get("showDetails").map(flag).unwrap_or(false);
            all = o.get("allParameters").map(flag).unwrap_or(false);
        }
        _ => {}
    }
    let mut reply = Document::new();
    for (name, entry) in &t.params {
        let Bson::Document(e) = entry else { continue };
        if !all && !cmd.contains_key(name) {
            continue;
        }
        // (a parameter without a value, such as a certificate setting that was not given, is not shown)
        let value = e.get("value").map(|v| current(name, v));
        if details {
            let mut d = Document::new();
            if let Some(v) = value {
                d.insert("value", v);
            }
            d.insert("settableAtRuntime", e.get("settableAtRuntime").cloned().unwrap_or(Bson::Boolean(false)));
            d.insert("settableAtStartup", e.get("settableAtStartup").cloned().unwrap_or(Bson::Boolean(false)));
            reply.insert(name.clone(), d);
        } else if let Some(v) = value {
            reply.insert(name.clone(), v);
        }
    }
    if reply.is_empty() {
        return Err(MongoError::new(72, "InvalidOptions", "no option found to get".into()));
    }
    reply.insert("ok", 1.0);
    Ok(reply)
}

/// The new value of a parameter, in the type of the old one.
fn coerce(name: &str, old: &Bson, v: &Bson) -> R<Bson> {
    let num = |v: &Bson| match v {
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Double(f) => Some(*f),
        Bson::Decimal128(d) => d.to_string().parse::<f64>().ok(),
        Bson::Boolean(b) => Some(*b as i32 as f64),
        _ => None,
    };
    let fail = |what: &str| MongoError::bad_value(format!("Failed validating {name}: Unable to coerce value to {what}"));
    match old {
        Bson::Int32(_) | Bson::Int64(_) => {
            let n = num(v).ok_or_else(|| if name == "logLevel" { MongoError::bad_value(format!("Invalid value for logLevel: logLevel: {}", crate::update::fmt_bson(v))) } else { fail("integral type") })?;
            Ok(match old {
                Bson::Int32(_) if n.abs() < i32::MAX as f64 => Bson::Int32(n as i32),
                _ => Bson::Int64(n as i64),
            })
        }
        Bson::Double(_) => Ok(Bson::Double(num(v).ok_or_else(|| fail("double"))?)),
        Bson::Boolean(_) => Ok(Bson::Boolean(flag(v))),
        Bson::String(_) => match v {
            Bson::String(_) => Ok(v.clone()),
            _ => Err(fail("string")),
        },
        _ => Ok(v.clone()),
    }
}

/// Limits mongod puts on a few parameters: (minimum, whether the minimum itself is allowed).
fn minimum(name: &str) -> Option<i64> {
    match name {
        "transactionLifetimeLimitSeconds" => Some(1),
        "ttlMonitorSleepSecs" | "internalQueryJavaScriptFnTimeoutMillis" => Some(1),
        _ => None,
    }
}

pub(crate) fn set_parameter(cmd: &Document, rs: bool) -> R<Document> {
    let t = table(rs);
    if matches!(cmd.get("help"), Some(Bson::Boolean(true))) {
        return Ok(doc! { "help": t.help.clone(), "ok": 1.0 });
    }
    let mut reply = Document::new();
    let mut any = false;
    for (k, v) in cmd.iter().skip(1) {
        if k.starts_with('$') || matches!(k.as_str(), "lsid" | "txnNumber" | "comment" | "maxTimeMS" | "writeConcern" | "readConcern" | "apiVersion" | "apiStrict" | "apiDeprecationErrors" | "help") {
            continue;
        }
        let Some(Bson::Document(e)) = t.params.get(k.as_str()) else {
            return Err(MongoError::new(72, "InvalidOptions", format!("attempted to set unrecognized parameter [{k}], use help:true to see options ")));
        };
        if !matches!(e.get("settableAtRuntime"), Some(Bson::Boolean(true))) {
            return Err(MongoError::new(20, "IllegalOperation", format!("not allowed to change [{k}] at runtime")));
        }
        let old = current(k, e.get("value").unwrap_or(&Bson::Null));
        let new = coerce(k, &old, v)?;
        if let Some(min) = minimum(k) {
            let n = match &new {
                Bson::Int32(i) => *i as i64,
                Bson::Int64(i) => *i,
                _ => min,
            };
            if n < min {
                let phrase = if min == 1 && k == "transactionLifetimeLimitSeconds" { format!("is not greater than or equal to {min}") } else { format!("is not greater than {}", min - 1) };
                return Err(MongoError::bad_value(format!("Invalid value for parameter {k}: {n} {phrase}")));
            }
        }
        match k.as_str() {
            "internalQueryJavaScriptFnTimeoutMillis" => crate::js::TIMEOUT_MS.store(num_u64(&new), Relaxed),
            "jsHeapLimitMB" => crate::js::HEAP_MB.store(num_u64(&new), Relaxed),
            "transactionLifetimeLimitSeconds" => crate::txn::LIFETIME_SECS.store(num_u64(&new), Relaxed),
            _ => {}
        }
        overlay().lock().unwrap().insert(k.clone(), new);
        if !any {
            reply.insert("was", old);
        }
        any = true;
    }
    if !any {
        return Err(MongoError::new(72, "InvalidOptions", "no option found to set, use help:true to see options ".into()));
    }
    reply.insert("ok", 1.0);
    Ok(reply)
}

fn num_u64(v: &Bson) -> u64 {
    match v {
        Bson::Int32(i) => *i as u64,
        Bson::Int64(i) => *i as u64,
        _ => 0,
    }
}
