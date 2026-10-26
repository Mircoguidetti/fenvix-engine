//! Update operators ($set, $unset, $inc, $setOnInsert), replacement updates, upsert document
//! construction, and the shell-style value formatting used in duplicate key messages.

use crate::error::MongoError;
use crate::query::path_values;
use bson::{oid::ObjectId, Bson, Document};

type R<T> = Result<T, MongoError>;

pub fn fmt_bson(v: &Bson) -> String {
    match v {
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        Bson::Double(f) => {
            if f.is_finite() && f.fract() == 0.0 && f.abs() < 1e15 { format!("{f:.1}") } else { f.to_string() }
        }
        Bson::String(s) => format!("\"{s}\""),
        Bson::ObjectId(o) => format!("ObjectId('{}')", o.to_hex()),
        Bson::Boolean(b) => b.to_string(),
        Bson::Null => "null".into(),
        Bson::DateTime(d) => format!("new Date({})", d.timestamp_millis()),
        Bson::Document(d) => fmt_doc(d),
        Bson::Array(a) => {
            if a.is_empty() { "[]".into() } else { format!("[ {} ]", a.iter().map(fmt_bson).collect::<Vec<_>>().join(", ")) }
        }
        other => format!("{other}"),
    }
}

pub fn fmt_doc(d: &Document) -> String {
    if d.is_empty() {
        return "{}".into();
    }
    format!("{{ {} }}", d.iter().map(|(k, v)| format!("{k}: {}", fmt_bson(v))).collect::<Vec<_>>().join(", "))
}

fn path_not_viable(parts: &[&str], at: &str, found: &Bson) -> MongoError {
    MongoError::new(28, "PathNotViable", format!("Cannot create field '{}' in element {{{at}: {}}}", parts.get(1).copied().unwrap_or(""), fmt_bson(found)))
}

pub fn set_path(doc: &mut Document, parts: &[&str], value: Bson) -> R<()> {
    let key = parts[0];
    if parts.len() == 1 {
        doc.insert(key, value);
        return Ok(());
    }
    match doc.get_mut(key) {
        Some(Bson::Document(d)) => set_path(d, &parts[1..], value),
        Some(Bson::Array(a)) => set_in_array(a, &parts[1..], value),
        Some(other) => Err(path_not_viable(parts, key, &other.clone())),
        None => {
            let mut d = Document::new();
            set_path(&mut d, &parts[1..], value)?;
            doc.insert(key, Bson::Document(d));
            Ok(())
        }
    }
}

fn set_in_array(a: &mut Vec<Bson>, parts: &[&str], value: Bson) -> R<()> {
    let idx: usize = parts[0].parse().map_err(|_| {
        MongoError::new(28, "PathNotViable", format!("Cannot create field '{}' in element {{}}", parts[0]))
    })?;
    while a.len() <= idx {
        a.push(Bson::Null);
    }
    if parts.len() == 1 {
        a[idx] = value;
        return Ok(());
    }
    if matches!(a[idx], Bson::Null) {
        a[idx] = Bson::Document(Document::new());
    }
    match &mut a[idx] {
        Bson::Document(d) => set_path(d, &parts[1..], value),
        Bson::Array(inner) => set_in_array(inner, &parts[1..], value),
        other => Err(path_not_viable(parts, parts[0], &other.clone())),
    }
}

pub fn unset_path(doc: &mut Document, parts: &[&str]) {
    let key = parts[0];
    if parts.len() == 1 {
        doc.remove(key);
        return;
    }
    match doc.get_mut(key) {
        Some(Bson::Document(d)) => unset_path(d, &parts[1..]),
        Some(Bson::Array(a)) => unset_in_array(a, &parts[1..]),
        _ => {}
    }
}

fn unset_in_array(a: &mut Vec<Bson>, parts: &[&str]) {
    if let Ok(idx) = parts[0].parse::<usize>() {
        if idx < a.len() {
            if parts.len() == 1 {
                a[idx] = Bson::Null;
            } else if let Bson::Document(d) = &mut a[idx] {
                unset_path(d, &parts[1..]);
            }
        }
    }
}

/// `None` on 64-bit integer overflow (MongoDB rejects those instead of widening to a double).
fn add_numbers(a: &Bson, b: &Bson) -> Option<Bson> {
    Some(match (a, b) {
        (Bson::Int32(x), Bson::Int32(y)) => match x.checked_add(*y) {
            Some(s) => Bson::Int32(s),
            None => Bson::Int64(*x as i64 + *y as i64),
        },
        (Bson::Int32(_) | Bson::Int64(_), Bson::Int32(_) | Bson::Int64(_)) => Bson::Int64(as_i64(a).checked_add(as_i64(b))?),
        _ => Bson::Double(crate::query::as_f64(a) + crate::query::as_f64(b)),
    })
}

fn overflow(op: &str, cur: &Bson, doc: &Document) -> MongoError {
    bad(format!("Failed to apply {op} operations to current value ({}) for document {{_id: {}}}", fmt_bson(cur), id_of(doc)))
}

fn as_i64(v: &Bson) -> i64 {
    match v {
        Bson::Int32(x) => *x as i64,
        Bson::Int64(x) => *x,
        _ => 0,
    }
}

fn bad(msg: impl Into<String>) -> MongoError {
    MongoError::new(2, "BadValue", msg.into())
}

fn split(path: &str) -> Vec<&str> {
    path.split('.').collect()
}

/// Mutable access to the value at `parts`, walking documents and numeric array indexes only.
fn target<'a>(doc: &'a mut Document, parts: &[&str]) -> Option<&'a mut Bson> {
    let mut cur = doc.get_mut(parts[0])?;
    for p in &parts[1..] {
        cur = match cur {
            Bson::Document(d) => d.get_mut(*p)?,
            Bson::Array(a) => a.get_mut(p.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

fn id_of(doc: &Document) -> String {
    doc.get("_id").map(fmt_bson).unwrap_or_default()
}

fn conflict(path: &str, at: &str) -> MongoError {
    MongoError::new(40, "ConflictingUpdateOperators", format!("Updating the path '{path}' would create a conflict at '{at}'"))
}

/// Reject updates that touch the same path (or a path and its ancestor) twice.
fn check_conflicts(update: &Document) -> R<()> {
    let mut seen: Vec<Vec<String>> = Vec::new();
    let mut add = |path: &str, seen: &mut Vec<Vec<String>>| -> R<()> {
        let parts: Vec<String> = path.split('.').map(str::to_string).collect();
        for e in seen.iter() {
            let n = e.len().min(parts.len());
            if e[..n] == parts[..n] {
                return Err(conflict(path, &parts[..n].join(".")));
            }
        }
        seen.push(parts);
        Ok(())
    };
    for (op, spec) in update {
        if let Bson::Document(f) = spec {
            for (path, v) in f {
                add(path, &mut seen)?;
                if op == "$rename" {
                    if let Bson::String(to) = v {
                        add(to, &mut seen)?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn unsupported_path(path: &str) -> Option<MongoError> {
    if path.split('.').any(|p| p == "$" || p.starts_with("$[")) {
        return Some(MongoError::new(238, "NotImplemented", format!("positional and filtered positional operators are not supported by this server (path '{path}')")));
    }
    None
}

fn mul_numbers(a: &Bson, b: &Bson) -> Option<Bson> {
    Some(match (a, b) {
        (Bson::Int32(x), Bson::Int32(y)) => match x.checked_mul(*y) {
            Some(s) => Bson::Int32(s),
            None => Bson::Int64(*x as i64 * *y as i64),
        },
        (Bson::Int32(_) | Bson::Int64(_), Bson::Int32(_) | Bson::Int64(_)) => Bson::Int64(as_i64(a).checked_mul(as_i64(b))?),
        _ => Bson::Double(crate::query::as_f64(a) * crate::query::as_f64(b)),
    })
}

fn zero_like(v: &Bson) -> Bson {
    match v {
        Bson::Int32(_) => Bson::Int32(0),
        Bson::Int64(_) => Bson::Int64(0),
        _ => Bson::Double(0.0),
    }
}

fn int_value(v: &Bson) -> Option<i64> {
    match v {
        Bson::Int32(i) => Some(*i as i64),
        Bson::Int64(i) => Some(*i),
        Bson::Double(f) if f.fract() == 0.0 && f.is_finite() => Some(*f as i64),
        _ => None,
    }
}

/// The array at `path` (`None` when the path does not exist); errors when it is not an array.
fn array_target<'a>(doc: &'a mut Document, path: &str, what: &str) -> R<Option<&'a mut Vec<Bson>>> {
    let id = id_of(doc);
    match target(doc, &split(path)) {
        None => Ok(None),
        Some(Bson::Array(a)) => Ok(Some(a)),
        Some(other) => Err(bad(match what {
            "$push" => format!("The field '{path}' must be an array but is of type {} in document {{_id: {id}}}", type_name(other)),
            "$addToSet" => format!("Cannot apply $addToSet to non-array field. Field named '{path}' has non-array type {}", type_name(other)),
            _ => format!("Cannot apply {what} to a non-array value"),
        })),
    }
}

fn apply_push(doc: &mut Document, path: &str, arg: &Bson) -> R<()> {
    let (mut items, slice, sort, position): (Vec<Bson>, Option<i64>, Option<Bson>, Option<i64>) = match arg {
        Bson::Document(d) if d.contains_key("$each") => {
            let mut slice = None;
            let mut sort = None;
            let mut position = None;
            let mut items = Vec::new();
            for (k, v) in d {
                match k.as_str() {
                    "$each" => match v {
                        Bson::Array(a) => items = a.clone(),
                        other => return Err(bad(format!("The argument to $each in $push must be an array but it was of type {}", type_name(other)))),
                    },
                    "$slice" => slice = Some(int_value(v).ok_or_else(|| bad(format!("The value for $slice must be an integer value but was given type: {}", type_name(v))))?),
                    "$position" => position = Some(int_value(v).ok_or_else(|| bad(format!("The value for $position must be an integer value, not of type: {}", type_name(v))))?),
                    "$sort" => {
                        match v {
                            Bson::Document(sd) => {
                                if sd.is_empty() {
                                    return Err(bad("The $sort pattern is empty when it should be a set of fields."));
                                }
                                crate::query::parse_sort(sd).map_err(|_| bad("The $sort element value must be either 1 or -1"))?;
                            }
                            n if crate::query::is_number(n) && matches!(int_value(n), Some(1 | -1)) => {}
                            _ => return Err(bad("The $sort is invalid: use 1/-1 to sort the whole element, or {field:1/-1} to sort embedded fields")),
                        }
                        sort = Some(v.clone());
                    }
                    other => return Err(bad(format!("Unrecognized clause in $push: {other}"))),
                }
            }
            (items, slice, sort, position)
        }
        other => (vec![other.clone()], None, None, None),
    };
    let parts = split(path);
    let existing = array_target(doc, path, "$push")?.map(|a| std::mem::take(a));
    let mut arr = existing.clone().unwrap_or_default();
    let exists = existing.is_some();
    match position {
        None => arr.append(&mut items),
        Some(p) => {
            let at = if p >= 0 { (p as usize).min(arr.len()) } else { (arr.len() as i64 + p).max(0) as usize };
            for (i, it) in items.drain(..).enumerate() {
                arr.insert(at + i, it);
            }
        }
    }
    if let Some(s) = &sort {
        match s {
            Bson::Document(sd) => {
                let spec = crate::query::parse_sort(sd)?;
                // Elements that are not documents have none of the sort fields and keep their place.
                let mut keyed: Vec<(Document, Bson)> = arr.drain(..).map(|e| (if let Bson::Document(d) = &e { d.clone() } else { Document::new() }, e)).collect();
                let mut docs: Vec<Document> = keyed.iter().enumerate().map(|(i, (d, _))| {
                    let mut d = d.clone();
                    d.insert("\u{0}idx", i as i64);
                    d
                }).collect();
                crate::query::sort_docs(&mut docs, &spec);
                let order: Vec<usize> = docs.iter().map(|d| d.get_i64("\u{0}idx").unwrap() as usize).collect();
                let mut taken: Vec<Option<Bson>> = keyed.drain(..).map(|(_, e)| Some(e)).collect();
                arr = order.into_iter().map(|i| taken[i].take().unwrap()).collect();
            }
            n => {
                let asc = int_value(n) == Some(1);
                arr.sort_by(|a, b| if asc { crate::query::compare(a, b) } else { crate::query::compare(b, a) });
            }
        }
    }
    if let Some(n) = slice {
        let len = arr.len() as i64;
        if n >= 0 {
            arr.truncate(n as usize);
        } else {
            arr.drain(..(len + n).max(0) as usize);
        }
    }
    if exists {
        if let Some(Bson::Array(a)) = target(doc, &parts) {
            *a = arr;
        }
    } else {
        set_path(doc, &parts, Bson::Array(arr))?;
    }
    Ok(())
}

fn apply_add_to_set(doc: &mut Document, path: &str, arg: &Bson) -> R<()> {
    let items: Vec<Bson> = match arg {
        Bson::Document(d) if d.contains_key("$each") => {
            if d.len() > 1 {
                let k = d.keys().find(|k| *k != "$each").unwrap();
                return Err(bad(format!("Found unexpected fields after $each in $addToSet: {}", fmt_doc(d)).replace("%", &k.to_string())));
            }
            match &d["$each"] {
                Bson::Array(a) => a.clone(),
                other => return Err(MongoError::type_mismatch(format!("The argument to $each in $addToSet must be an array but it was of type {}", type_name(other)))),
            }
        }
        other => vec![other.clone()],
    };
    let parts = split(path);
    match array_target(doc, path, "$addToSet")? {
        Some(a) => {
            for it in items {
                if !a.iter().any(|e| crate::query::equal(e, &it)) {
                    a.push(it);
                }
            }
        }
        None => {
            let mut a: Vec<Bson> = Vec::new();
            for it in items {
                if !a.iter().any(|e| crate::query::equal(e, &it)) {
                    a.push(it);
                }
            }
            set_path(doc, &parts, Bson::Array(a))?;
        }
    }
    Ok(())
}

fn apply_pull(doc: &mut Document, path: &str, cond: &Bson) -> R<()> {
    let Some(a) = array_target(doc, path, "$pull")? else { return Ok(()) };
    let mut keep = Vec::with_capacity(a.len());
    for e in a.drain(..) {
        if !crate::query::pull_matches(&e, cond)? {
            keep.push(e);
        }
    }
    *a = keep;
    Ok(())
}

fn now_ms() -> bson::DateTime {
    bson::DateTime::now()
}

/// Apply the operator form of an update to `doc`.
fn apply_operators(doc: &mut Document, update: &Document, upsert_insert: bool) -> R<()> {
    for (op, spec) in update {
        if !matches!(op.as_str(), "$set" | "$setOnInsert" | "$unset" | "$inc" | "$push" | "$pull" | "$pullAll" | "$addToSet" | "$pop" | "$rename" | "$min" | "$max" | "$mul" | "$currentDate" | "$bit") {
            return Err(MongoError::failed_to_parse(format!(
                "Unknown modifier: {op}. Expected a valid update modifier or pipeline-style update specified as an array"
            )));
        }
        let fields = spec.as_document().ok_or_else(|| {
            MongoError::new(9, "FailedToParse", format!("Modifiers operate on fields but we found type {} instead. For example: {{$mod: {{<field>: ...}}}} not {{{op}: {}}}", type_name(spec), fmt_bson(spec)))
        })?;
        for path in fields.keys() {
            if let Some(e) = unsupported_path(path) {
                return Err(e);
            }
        }
    }
    if let Some(Bson::Document(r)) = update.get("$rename") {
        for (from, to) in r {
            let to = match to {
                Bson::String(s) => s,
                other => return Err(bad(format!("The 'to' field for $rename must be a string: {from}: {}", fmt_bson(other)))),
            };
            if from == to {
                return Err(bad(format!("The source and target field for $rename must differ: {from}: \"{to}\"")));
            }
            if let Some(e) = unsupported_path(to) {
                return Err(e);
            }
        }
    }
    check_conflicts(update)?;
    // MongoDB applies the modifiers in path order (one tree over all operators), not in the order
    // they are written; a `$rename` sits at its target path.
    let mut entries: Vec<(Vec<String>, &str, &str, &Bson)> = Vec::new();
    for (op, spec) in update {
        for (path, v) in spec.as_document().unwrap() {
            let key = match (op.as_str(), v) {
                ("$rename", Bson::String(to)) => to,
                _ => path,
            };
            entries.push((key.split('.').map(str::to_string).collect(), op, path, v));
        }
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    for (_, op, path, v) in entries {
        let mut one = Document::new();
        one.insert(path, v.clone());
        let fields = &one;
        match op {
            "$set" => {
                for (path, v) in fields {
                    set_path(doc, &split(path), v.clone())?;
                }
            }
            "$setOnInsert" => {
                if upsert_insert {
                    for (path, v) in fields {
                        set_path(doc, &split(path), v.clone())?;
                    }
                }
            }
            "$unset" => {
                for (path, _) in fields {
                    unset_path(doc, &split(path));
                }
            }
            "$inc" => {
                for (path, inc) in fields {
                    if !crate::query::is_number(inc) {
                        return Err(MongoError::type_mismatch(format!("Cannot increment with non-numeric argument: {{{path}: {}}}", fmt_bson(inc))));
                    }
                    let cur = path_values(doc, path).first().cloned().cloned();
                    let new = match cur {
                        None => inc.clone(),
                        Some(c) if crate::query::is_number(&c) => add_numbers(&c, inc).ok_or_else(|| overflow("$inc", &c, doc))?,
                        Some(c) => {
                            let id = doc.get("_id").map(fmt_bson).unwrap_or_default();
                            return Err(MongoError::type_mismatch(format!(
                                "Cannot apply $inc to a value of non-numeric type. {{_id: {id}}} has the field '{path}' of non-numeric type {}",
                                type_name(&c)
                            )));
                        }
                    };
                    set_path(doc, &split(path), new)?;
                }
            }
            "$mul" => {
                for (path, m) in fields {
                    if !crate::query::is_number(m) {
                        return Err(MongoError::type_mismatch(format!("Cannot multiply with non-numeric argument: {{{path}: {}}}", fmt_bson(m))));
                    }
                    let cur = path_values(doc, path).first().cloned().cloned();
                    let new = match cur {
                        None => zero_like(m),
                        Some(c) if crate::query::is_number(&c) => mul_numbers(&c, m).ok_or_else(|| overflow("$mul", &c, doc))?,
                        Some(c) => {
                            return Err(MongoError::type_mismatch(format!(
                                "Cannot apply $mul to a value of non-numeric type. {{_id: {}}} has the field '{path}' of non-numeric type {}",
                                id_of(doc),
                                type_name(&c)
                            )));
                        }
                    };
                    set_path(doc, &split(path), new)?;
                }
            }
            "$min" | "$max" => {
                for (path, v) in fields {
                    let cur = path_values(doc, path).first().cloned().cloned();
                    let take = match &cur {
                        None => true,
                        Some(c) => {
                            let o = crate::query::compare(v, c);
                            if op == "$min" { o == std::cmp::Ordering::Less } else { o == std::cmp::Ordering::Greater }
                        }
                    };
                    if take {
                        set_path(doc, &split(path), v.clone())?;
                    }
                }
            }
            "$push" => {
                for (path, v) in fields {
                    apply_push(doc, path, v)?;
                }
            }
            "$addToSet" => {
                for (path, v) in fields {
                    apply_add_to_set(doc, path, v)?;
                }
            }
            "$pull" => {
                for (path, v) in fields {
                    apply_pull(doc, path, v)?;
                }
            }
            "$pullAll" => {
                for (path, v) in fields {
                    let list = v.as_array().ok_or_else(|| bad(format!("$pullAll requires an array argument but was given a {}", type_name(v))))?;
                    if let Some(a) = array_target(doc, path, "$pullAll")? {
                        a.retain(|e| !list.iter().any(|x| crate::query::equal(e, x)));
                    }
                }
            }
            "$pop" => {
                for (path, v) in fields {
                    let n = int_value(v).filter(|n| *n == 1 || *n == -1).ok_or_else(|| {
                        MongoError::failed_to_parse(format!("Expected a number in: {path}: {}", fmt_bson(v)))
                    });
                    let n = match (n, int_value(v)) {
                        (Ok(n), _) => n,
                        (Err(_), Some(x)) => return Err(MongoError::failed_to_parse(format!("$pop expects 1 or -1, found: {x}"))),
                        (Err(e), None) => return Err(e),
                    };
                    let id = id_of(doc);
                    match target(doc, &split(path)) {
                        None => {}
                        Some(Bson::Array(a)) => {
                            if !a.is_empty() {
                                if n == 1 { a.pop(); } else { a.remove(0); }
                            }
                        }
                        Some(other) => {
                            let _ = id;
                            return Err(MongoError::type_mismatch(format!("Path '{path}' contains an element of non-array type '{}'", type_name(other))));
                        }
                    }
                }
            }
            "$rename" => {
                for (from, to) in fields {
                    let to = match to {
                        Bson::String(s) => s,
                        other => return Err(bad(format!("The 'to' field for $rename must be a string: {from}: {}", fmt_bson(other)))),
                    };
                    if from == to {
                        return Err(bad(format!("The source and target field for $rename must differ: {from}: \"{to}\"")));
                    }
                    if let Some(e) = unsupported_path(to) {
                        return Err(e);
                    }
                    let fparts = split(from);
                    let cur = target(doc, &fparts).map(|v| v.clone());
                    if let Some(v) = cur {
                        unset_path(doc, &fparts);
                        set_path(doc, &split(to), v)?;
                    }
                }
            }
            "$currentDate" => {
                for (path, t) in fields {
                    let ts = match t {
                        Bson::Boolean(_) => false,
                        Bson::Document(d) => match d.get("$type") {
                            Some(Bson::String(s)) if s == "date" => false,
                            Some(Bson::String(s)) if s == "timestamp" => true,
                            _ => return Err(bad(format!("{} is not valid type for $currentDate. Please use a boolean ('true') or a $type expression ({{$type: 'timestamp/date'}}).", type_name(t)))),
                        },
                        other => return Err(bad(format!("{} is not valid type for $currentDate. Please use a boolean ('true') or a $type expression ({{$type: 'timestamp/date'}}).", type_name(other)))),
                    };
                    let v = if ts {
                        Bson::Timestamp(bson::Timestamp { time: (now_ms().timestamp_millis() / 1000) as u32, increment: 1 })
                    } else {
                        Bson::DateTime(now_ms())
                    };
                    set_path(doc, &split(path), v)?;
                }
            }
            _ => {
                return Err(MongoError::failed_to_parse(format!(
                    "Unknown modifier: {op}. Expected a valid update modifier or pipeline-style update specified as an array"
                )));
            }
        }
    }
    Ok(())
}

pub fn type_name(v: &Bson) -> &'static str {
    match v {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Array(_) => "array",
        Bson::Document(_) => "object",
        Bson::Boolean(_) => "bool",
        Bson::Null => "null",
        Bson::Int32(_) => "int",
        Bson::Int64(_) => "long",
        Bson::ObjectId(_) => "objectId",
        Bson::DateTime(_) => "date",
        _ => "unknown",
    }
}

fn is_operator_update(update: &Document) -> bool {
    update.keys().next().map(|k| k.starts_with('$')).unwrap_or(false)
}

fn immutable_id_by_operator() -> MongoError {
    MongoError::new(66, "ImmutableField", "Performing an update on the path '_id' would modify the immutable field '_id'".into())
}

fn immutable_id_by_replacement(new_id: &Bson) -> MongoError {
    MongoError::new(66, "ImmutableField", format!("After applying the update, the (immutable) field '_id' was found to have been altered to _id: {}", fmt_bson(new_id)))
}

/// Apply an update (operators or replacement) to an existing document, returning the new document.
pub fn apply_update(old: &Document, update: &Document) -> R<Document> {
    let operator_form = is_operator_update(update);
    let new = if operator_form {
        let mut d = old.clone();
        apply_operators(&mut d, update, false)?;
        d
    } else {
        let mut d = Document::new();
        if let Some(id) = old.get("_id") {
            d.insert("_id", id.clone());
        }
        for (k, v) in update {
            if k.starts_with('$') {
                return Err(MongoError::new(52, "DollarPrefixedFieldName", format!("The dollar ($) prefixed field '{k}' in '{k}' is not valid for storage.")));
            }
            d.insert(k.clone(), v.clone());
        }
        d
    };
    if let (Some(a), Some(b)) = (old.get("_id"), new.get("_id")) {
        if !crate::query::equal(a, b) {
            return Err(if operator_form { immutable_id_by_operator() } else { immutable_id_by_replacement(b) });
        }
    } else if old.get("_id").is_some() {
        return Err(immutable_id_by_operator());
    }
    Ok(new)
}

fn collect_equalities(filter: &Document, base: &mut Document) -> R<()> {
    for (k, v) in filter {
        if k == "$and" {
            if let Some(arr) = v.as_array() {
                for sub in arr {
                    if let Some(d) = sub.as_document() {
                        collect_equalities(d, base)?;
                    }
                }
            }
        } else if k.starts_with('$') {
            continue;
        } else {
            match v {
                Bson::Document(d) if d.keys().any(|x| x.starts_with('$')) => {
                    if let Some(eq) = d.get("$eq") {
                        set_path(base, &k.split('.').collect::<Vec<_>>(), eq.clone())?;
                    }
                }
                Bson::RegularExpression(_) => {}
                other => set_path(base, &k.split('.').collect::<Vec<_>>(), other.clone())?,
            }
        }
    }
    Ok(())
}

/// Build the document an upsert inserts when nothing matched.
pub fn upsert_document(filter: &Document, update: &Document) -> R<Document> {
    let mut base = Document::new();
    collect_equalities(filter, &mut base)?;
    let mut doc = if is_operator_update(update) {
        apply_operators(&mut base, update, true)?;
        base
    } else {
        let mut d = Document::new();
        if let Some(id) = base.get("_id") {
            d.insert("_id", id.clone());
        }
        for (k, v) in update {
            if k == "_id" {
                if let Some(id) = base.get("_id") {
                    if !crate::query::equal(id, v) {
                        return Err(immutable_id_by_replacement(v));
                    }
                }
            }
            d.insert(k.clone(), v.clone());
        }
        d
    };
    if doc.get("_id").is_none() {
        doc.insert("_id", Bson::ObjectId(ObjectId::new()));
    }
    // `_id` is always the first field.
    if doc.keys().next().map(String::as_str) != Some("_id") {
        let id = doc.remove("_id").unwrap();
        let mut with_id = Document::new();
        with_id.insert("_id", id);
        for (k, v) in doc {
            with_id.insert(k, v);
        }
        doc = with_id;
    }
    Ok(doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn set_unset_inc() {
        let old = doc! {"_id": 1, "a": 1, "n": {"x": 1}};
        let new = apply_update(&old, &doc! {"$set": {"b": 2, "n.y": 3}, "$unset": {"a": ""}, "$inc": {"c": 5, "n.x": 2}}).unwrap();
        assert_eq!(new, doc! {"_id": 1, "n": {"x": 3, "y": 3}, "b": 2, "c": 5});
    }

    #[test]
    fn inc_types() {
        let o = doc! {"_id": 1, "i": 1_i32, "l": 1_i64, "d": 1.5};
        let n = apply_update(&o, &doc! {"$inc": {"i": 1_i32, "l": 1_i32, "d": 1_i32}}).unwrap();
        assert_eq!(n.get("i"), Some(&Bson::Int32(2)));
        assert_eq!(n.get("l"), Some(&Bson::Int64(2)));
        assert_eq!(n.get("d"), Some(&Bson::Double(2.5)));
        let n = apply_update(&doc! {"_id": 1, "i": i32::MAX}, &doc! {"$inc": {"i": 1}}).unwrap();
        assert_eq!(n.get("i"), Some(&Bson::Int64(i32::MAX as i64 + 1)));
    }

    #[test]
    fn replacement_keeps_id_and_rejects_change() {
        let n = apply_update(&doc! {"_id": 1, "a": 1}, &doc! {"b": 2}).unwrap();
        assert_eq!(n, doc! {"_id": 1, "b": 2});
        assert_eq!(apply_update(&doc! {"_id": 1}, &doc! {"_id": 2}).unwrap_err().code, 66);
    }

    #[test]
    fn upsert_builds_from_filter() {
        let d = upsert_document(&doc! {"a": 1, "b": {"$gt": 3}, "c.d": 2}, &doc! {"$set": {"e": 5}, "$setOnInsert": {"f": 6}}).unwrap();
        assert!(d.get("_id").is_some());
        assert_eq!(d.get_i32("a").unwrap(), 1);
        assert_eq!(d.get_document("c").unwrap(), &doc! {"d": 2});
        assert_eq!(d.get_i32("f").unwrap(), 6);
        assert!(d.get("b").is_none());
    }

    #[test]
    fn array_operators() {
        let old = doc! {"_id": 1, "a": [3, 1, 2]};
        let n = apply_update(&old, &doc! {"$push": {"a": {"$each": [9, 0], "$sort": 1, "$slice": -3}}}).unwrap();
        assert_eq!(n, doc! {"_id": 1, "a": [2, 3, 9]});
        let n = apply_update(&old, &doc! {"$pull": {"a": {"$gt": 1}}, "$addToSet": {"b": {"$each": [1, 1, 2]}}}).unwrap();
        assert_eq!(n, doc! {"_id": 1, "a": [1], "b": [1, 2]});
        let n = apply_update(&old, &doc! {"$pop": {"a": -1}, "$min": {"m": 5}, "$mul": {"z": 3}}).unwrap();
        assert_eq!(n, doc! {"_id": 1, "a": [1, 2], "m": 5, "z": 0});
        assert_eq!(apply_update(&old, &doc! {"$set": {"a": 1}, "$inc": {"a.b": 1}}).unwrap_err().code, 40);
        assert_eq!(apply_update(&doc! {"_id": 1, "a": "x"}, &doc! {"$push": {"a": 1}}).unwrap_err().code, 2);
    }

    #[test]
    fn modifiers_apply_in_path_order() {
        let n = apply_update(&doc! {"_id": 1}, &doc! {"$set": {"b": 1, "a": 2}}).unwrap();
        assert_eq!(n.keys().collect::<Vec<_>>(), vec!["_id", "a", "b"]);
        let big = doc! {"_id": 1, "n": i64::MAX};
        assert_eq!(apply_update(&big, &doc! {"$inc": {"n": 1}}).unwrap_err().code, 2);
    }

    #[test]
    fn formatting() {
        assert_eq!(fmt_doc(&doc! {"_id": 1}), "{ _id: 1 }");
        assert_eq!(fmt_doc(&doc! {"a": "x", "b": 1.0}), "{ a: \"x\", b: 1.0 }");
    }
}
