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

/// Positional context of an operator update: the query filter (for `$`) and the `arrayFilters` (for `$[<id>]`).
pub struct PosCtx<'a> {
    pub filter: Option<&'a Document>,
    pub array_filters: &'a [(String, Document)],
}

const NO_CTX: PosCtx<'static> = PosCtx { filter: None, array_filters: &[] };

fn is_filtered(p: &str) -> Option<&str> {
    p.strip_prefix("$[").and_then(|r| r.strip_suffix(']')).filter(|id| !id.is_empty())
}

pub fn has_positional(path: &str) -> bool {
    path.split('.').any(|p| p == "$" || p.starts_with("$["))
}

fn check_positional_syntax(path: &str) -> R<()> {
    let mut dollars = 0;
    for (i, p) in path.split('.').enumerate() {
        if p == "$" {
            if i == 0 {
                return Err(bad(format!("Cannot have positional (i.e. '$') element in the first position in path '{path}'")));
            }
            dollars += 1;
            if dollars > 1 {
                return Err(bad(format!("Too many positional (i.e. '$') elements found in path '{path}'")));
            }
        } else if p.starts_with("$[") && p != "$[]" {
            match is_filtered(p) {
                Some(id) if valid_filter_id(id) => {}
                _ => return Err(bad(format!("Cannot parse path '{path}': the identifier '{}' must be an alphanumeric string beginning with a lowercase letter", p.trim_start_matches("$[").trim_end_matches(']')))),
            }
        }
    }
    Ok(())
}

fn valid_filter_id(id: &str) -> bool {
    let mut c = id.chars();
    matches!(c.next(), Some(f) if f.is_ascii_lowercase()) && c.all(|x| x.is_ascii_alphanumeric())
}

/// Validated `arrayFilters`: (identifier, filter document) in the order given.
pub fn parse_array_filters(v: Option<&Bson>) -> R<Vec<(String, Document)>> {
    let Some(Bson::Array(a)) = v else {
        return match v {
            None | Some(Bson::Null) => Ok(vec![]),
            Some(_) => Err(MongoError::type_mismatch("BSON field 'arrayFilters' is the wrong type, expected array".into())),
        };
    };
    let mut out: Vec<(String, Document)> = Vec::new();
    for f in a {
        let Bson::Document(d) = f else {
            return Err(MongoError::type_mismatch("BSON field 'arrayFilters.0' is the wrong type, expected object".into()));
        };
        let mut ids: Vec<String> = Vec::new();
        collect_filter_ids(d, &mut ids);
        ids.sort();
        ids.dedup();
        let id = match ids.len() {
            0 => return Err(MongoError::failed_to_parse("Cannot use an expression without a top-level field name in arrayFilters".into())),
            1 => ids.pop().unwrap(),
            _ => return Err(MongoError::failed_to_parse(format!("Error parsing array filter :: caused by :: Expected a single top-level field name, found '{}' and '{}'", ids[0], ids[1]))),
        };
        if !valid_filter_id(&id) {
            return Err(bad(format!("Error parsing array filter :: caused by :: The top-level field name must be an alphanumeric string beginning with a lowercase letter, found '{id}'")));
        }
        if out.iter().any(|(i, _)| *i == id) {
            return Err(MongoError::failed_to_parse(format!("Found multiple array filters with the same top-level field name {id}")));
        }
        out.push((id, d.clone()));
    }
    Ok(out)
}

fn collect_filter_ids(d: &Document, ids: &mut Vec<String>) {
    for (k, v) in d {
        if matches!(k.as_str(), "$and" | "$or" | "$nor") {
            if let Bson::Array(a) = v {
                for s in a {
                    if let Bson::Document(sd) = s {
                        collect_filter_ids(sd, ids);
                    }
                }
            }
        } else if !k.starts_with('$') {
            ids.push(k.split('.').next().unwrap_or("").to_string());
        }
    }
}

fn lookup<'a>(doc: &'a Document, parts: &[String]) -> Option<&'a Bson> {
    let mut cur = doc.get(parts.first()?)?;
    for p in &parts[1..] {
        cur = match cur {
            Bson::Document(d) => d.get(p)?,
            Bson::Array(a) => a.get(p.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

/// Index of the array element that the query matched, for the positional `$` at `array_path`.
fn positional_index(filter: &Document, doc: &Document, array_path: &[String]) -> Option<usize> {
    let Some(Bson::Array(arr)) = lookup(doc, array_path) else { return None };
    let ap = array_path.join(".");
    let mut found: Option<usize> = None;
    let mut visit = |k: &str, cond: &Bson| {
        if k != ap && !k.starts_with(&format!("{ap}.")) {
            return;
        }
        for (i, el) in arr.iter().enumerate() {
            let mut w = Document::new();
            let parts: Vec<&str> = ap.split('.').collect();
            if set_path(&mut w, &parts, Bson::Array(vec![el.clone()])).is_err() {
                return;
            }
            let mut f = Document::new();
            f.insert(k.to_string(), cond.clone());
            if crate::query::matches(&w, &f).unwrap_or(false) {
                if found.is_none() {
                    found = Some(i);
                }
                return;
            }
        }
    };
    fn walk(filter: &Document, visit: &mut dyn FnMut(&str, &Bson)) {
        for (k, v) in filter {
            if k == "$and" {
                if let Bson::Array(a) = v {
                    for s in a {
                        if let Bson::Document(sd) = s {
                            walk(sd, visit);
                        }
                    }
                }
            } else if !k.starts_with('$') {
                visit(k, v);
            }
        }
    }
    walk(filter, &mut visit);
    found
}

enum Kind<'a> {
    Pos,
    All,
    Filtered(&'a str),
}

/// The concrete paths (numeric array indexes) that a path with `$`, `$[]` and `$[<id>]` stands for in `doc`.
fn expand_path(doc: &Document, path: &str, ctx: &PosCtx, used: &mut Vec<String>) -> R<Vec<String>> {
    let mut paths: Vec<Vec<String>> = vec![vec![]];
    let parts: Vec<&str> = path.split('.').collect();
    for (pi, p) in parts.iter().enumerate() {
        let kind = if *p == "$" {
            Some(Kind::Pos)
        } else if *p == "$[]" {
            Some(Kind::All)
        } else {
            is_filtered(p).map(Kind::Filtered)
        };
        let Some(kind) = kind else {
            for c in paths.iter_mut() {
                c.push(p.to_string());
            }
            continue;
        };
        let mut next: Vec<Vec<String>> = Vec::new();
        for c in &paths {
            let arr = match lookup(doc, c) {
                Some(Bson::Array(a)) => a,
                Some(other) => {
                    return Err(bad(format!("Cannot apply array updates to non-array element {}: {}", c.last().cloned().unwrap_or_default(), fmt_bson(other))));
                }
                None => return Err(bad(format!("The path '{}' must exist in the document in order to apply array updates.", c.join(".")))),
            };
            match kind {
                Kind::Pos => {
                    let Some(f) = ctx.filter else {
                        return Err(bad("The positional operator did not find the match needed from the query.".to_string()));
                    };
                    let i = positional_index(f, doc, c).ok_or_else(|| bad("The positional operator did not find the match needed from the query."))?;
                    let mut n = c.clone();
                    n.push(i.to_string());
                    next.push(n);
                }
                Kind::All => {
                    for i in 0..arr.len() {
                        let mut n = c.clone();
                        n.push(i.to_string());
                        next.push(n);
                    }
                }
                Kind::Filtered(id) => {
                    let Some((_, f)) = ctx.array_filters.iter().find(|(i, _)| i == id) else {
                        return Err(bad(format!("No array filter found for identifier '{id}' in path '{}'", parts[..=pi].join("."))));
                    };
                    if !used.iter().any(|u| u == id) {
                        used.push(id.to_string());
                    }
                    for (i, el) in arr.iter().enumerate() {
                        let mut w = Document::new();
                        w.insert(id.to_string(), el.clone());
                        if crate::query::matches(&w, f)? {
                            let mut n = c.clone();
                            n.push(i.to_string());
                            next.push(n);
                        }
                    }
                }
            }
        }
        paths = next;
    }
    Ok(paths.into_iter().map(|c| c.join(".")).collect())
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
fn apply_operators(doc: &mut Document, update: &Document, upsert_insert: bool, ctx: &PosCtx) -> R<()> {
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
            check_positional_syntax(path)?;
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
            check_positional_syntax(to)?;
            if has_positional(from) || has_positional(to) {
                return Err(bad(format!("The source field for $rename may not be dynamic: {from}")));
            }
        }
    }
    check_conflicts(update)?;
    // MongoDB applies the modifiers in path order (one tree over all operators), not in the order
    // they are written; a `$rename` sits at its target path.
    let mut entries: Vec<(Vec<String>, &str, String, &Bson)> = Vec::new();
    let mut used_filters: Vec<String> = Vec::new();
    for (op, spec) in update {
        for (path, v) in spec.as_document().unwrap() {
            if has_positional(path) {
                for concrete in expand_path(doc, path, ctx, &mut used_filters)? {
                    entries.push((concrete.split('.').map(str::to_string).collect(), op, concrete, v));
                }
                continue;
            }
            let key = match (op.as_str(), v) {
                ("$rename", Bson::String(to)) => to,
                _ => path,
            };
            entries.push((key.split('.').map(str::to_string).collect(), op, path.clone(), v));
        }
    }
    for (id, _) in ctx.array_filters {
        if !used_filters.contains(id) {
            return Err(MongoError::failed_to_parse(format!("The array filter for identifier '{id}' was not used in the update {}", fmt_doc(update))));
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
            "$bit" => {
                for (path, spec) in fields {
                    let Bson::Document(ops) = spec else {
                        return Err(bad(format!("The $bit modifier is not compatible with a {}. You must pass in an embedded document: {{$bit: {{field: {{and/or/xor: #}}}}", type_name(spec))));
                    };
                    let mut cur: Option<Bson> = path_values(doc, path).first().cloned().cloned();
                    for (name, operand) in ops {
                        if !matches!(name.as_str(), "and" | "or" | "xor") {
                            return Err(bad(format!("The $bit modifier only supports 'and', 'or', and 'xor', not '{name}' which is an unknown operator: {{{name}: {}}}", fmt_bson(operand))));
                        }
                        let (m, m_long) = match operand {
                            Bson::Int32(i) => (*i as i64, false),
                            Bson::Int64(i) => (*i, true),
                            other => return Err(bad(format!("The $bit modifier field must be an Integer(32/64 bit); a '{}' is not supported here: {{{name}: {}}}", type_name(other), fmt_bson(other)))),
                        };
                        let (c, c_long) = match &cur {
                            None => (0i64, false),
                            Some(Bson::Int32(i)) => (*i as i64, false),
                            Some(Bson::Int64(i)) => (*i, true),
                            Some(other) => return Err(bad(format!("Cannot apply $bit to a value of non-integral type._id: {} has the field {path} of non-integer type {}", id_of(doc), type_name(other)))),
                        };
                        let r = match name.as_str() {
                            "and" => c & m,
                            "or" => c | m,
                            _ => c ^ m,
                        };
                        cur = Some(if c_long || m_long { Bson::Int64(r) } else { Bson::Int32(r as i32) });
                    }
                    if let Some(v) = cur {
                        set_path(doc, &split(path), v)?;
                    }
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
    apply_update_ctx(old, update, &NO_CTX)
}

fn apply_update_ctx(old: &Document, update: &Document, ctx: &PosCtx) -> R<Document> {
    let operator_form = is_operator_update(update);
    let new = if operator_form {
        let mut d = old.clone();
        apply_operators(&mut d, update, false, ctx)?;
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
    upsert_document_ctx(filter, update, &PosCtx { filter: Some(filter), array_filters: &[] })
}

fn upsert_document_ctx(filter: &Document, update: &Document, ctx: &PosCtx) -> R<Document> {
    let mut base = Document::new();
    collect_equalities(filter, &mut base)?;
    let mut doc = if is_operator_update(update) {
        apply_operators(&mut base, update, true, ctx)?;
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


/// One update statement: operators or a replacement document, or an aggregation pipeline, with the query filter
/// (for the positional `$`), the `arrayFilters` and the pipeline's constants (`c`).
pub struct Upd {
    body: UpdBody,
    filter: Document,
    array_filters: Vec<(String, Document)>,
    consts: Vec<(String, Bson)>,
    /// A pipeline removed `_id` and it was put back: MongoDB counts such a document as modified.
    forced: std::cell::Cell<bool>,
}

enum UpdBody {
    Doc(Document),
    Pipeline(Vec<Bson>),
}

impl Upd {
    pub fn new(u: &Bson, filter: &Document, array_filters: Option<&Bson>, consts: Option<&Bson>) -> R<Upd> {
        let array_filters = parse_array_filters(array_filters)?;
        let body = match u {
            Bson::Document(d) => UpdBody::Doc(d.clone()),
            Bson::Array(stages) => {
                if !array_filters.is_empty() {
                    return Err(MongoError::failed_to_parse("arrayFilters may not be specified for pipeline-style updates".into()));
                }
                for s in stages {
                    let name = s.as_document().filter(|d| d.len() == 1).and_then(|d| d.keys().next().cloned());
                    match name.as_deref() {
                        Some("$addFields" | "$set" | "$project" | "$unset" | "$replaceRoot" | "$replaceWith") => {}
                        Some(n) => return Err(MongoError::new(72, "InvalidOptions", format!("Unsupported stage in a pipeline-style update: {n}. Only $addFields, $set, $project, $unset, $replaceRoot and $replaceWith are allowed"))),
                        None => return Err(MongoError::new(40323, "Location40323", "A pipeline stage specification object must contain exactly one field.".into())),
                    }
                }
                if stages.is_empty() {
                    return Err(MongoError::failed_to_parse("Update pipeline must contain at least one stage".to_string()));
                }
                UpdBody::Pipeline(stages.clone())
            }
            _ => return Err(MongoError::new(14, "TypeMismatch", "BSON field 'updates.u' is the wrong type, expected object or array".into())),
        };
        if let UpdBody::Doc(d) = &body {
            if !array_filters.is_empty() && !is_operator_update(d) {
                return Err(MongoError::failed_to_parse("arrayFilters may not be specified for replacement-style updates".into()));
            }
            if is_operator_update(d) {
                // the same checks whether or not a document matches
                let mut used: Vec<&str> = Vec::new();
                for (op, spec) in d {
                    let Bson::Document(fields) = spec else { continue };
                    for path in fields.keys() {
                        check_positional_syntax(path)?;
                        if op == "$rename" && has_positional(path) {
                            return Err(bad(format!("The source field for $rename may not be dynamic: {path}")));
                        }
                        for p in path.split('.') {
                            if let Some(id) = is_filtered(p) {
                                if !array_filters.iter().any(|(i, _)| i == id) {
                                    return Err(bad(format!("No array filter found for identifier '{id}' in path '{path}'")));
                                }
                                used.push(id);
                            }
                        }
                    }
                }
                for (id, _) in &array_filters {
                    if !used.contains(&id.as_str()) {
                        return Err(MongoError::failed_to_parse(format!("The array filter for identifier '{id}' was not used in the update {}", fmt_doc(d))));
                    }
                }
            }
        }
        let consts = match consts {
            Some(Bson::Document(c)) => c.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            _ => vec![],
        };
        Ok(Upd { body, filter: filter.clone(), array_filters, consts, forced: std::cell::Cell::new(false) })
    }

    /// Whether the last pipeline result must count as a modification even if it equals the old document.
    pub fn take_forced(&self) -> bool {
        self.forced.replace(false)
    }

    /// The operator document, for change-stream update descriptions (None for replacements and pipelines).
    pub fn operators(&self) -> Option<&Document> {
        match &self.body {
            UpdBody::Doc(d) if d.keys().any(|k| k.starts_with('$')) => Some(d),
            _ => None,
        }
    }

    fn ctx(&self) -> PosCtx<'_> {
        PosCtx { filter: Some(&self.filter), array_filters: &self.array_filters }
    }

    fn run_pipeline(&self, base: Document, stages: &[Bson]) -> R<Document> {
        let vars: Vec<(String, Bson)> = self.consts.clone();
        let out = crate::expr::with_ambient(vars, || crate::agg::run_pipeline(vec![base.clone()], stages, &Default::default()))?;
        let mut d = out.into_iter().next().ok_or_else(|| MongoError::new(2, "BadValue", "pipeline-style update produced no document".into()))?;
        match (base.get("_id"), d.get("_id")) {
            (Some(a), Some(b)) if crate::query::equal(a, b) => {}
            (Some(_), Some(_)) => return Err(immutable_id_by_operator()),
            // a pipeline that removes `_id` keeps the document's: it is put back
            (Some(a), None) => {
                let mut with_id = Document::new();
                with_id.insert("_id", a.clone());
                for (k, v) in d {
                    with_id.insert(k, v);
                }
                d = with_id;
                self.forced.set(true);
            }
            _ => {}
        }
        Ok(d)
    }

    pub fn apply(&self, old: &Document) -> R<Document> {
        match &self.body {
            UpdBody::Doc(u) => apply_update_ctx(old, u, &self.ctx()),
            UpdBody::Pipeline(stages) => self.run_pipeline(old.clone(), stages),
        }
    }

    pub fn upsert(&self) -> R<Document> {
        match &self.body {
            UpdBody::Doc(u) => upsert_document_ctx(&self.filter, u, &self.ctx()),
            UpdBody::Pipeline(stages) => {
                let mut base = Document::new();
                collect_equalities(&self.filter, &mut base)?;
                let mut d = self.run_pipeline(base, stages)?;
                if d.get("_id").is_none() {
                    let mut with_id = Document::new();
                    with_id.insert("_id", Bson::ObjectId(ObjectId::new()));
                    for (k, v) in d {
                        with_id.insert(k, v);
                    }
                    d = with_id;
                }
                Ok(d)
            }
        }
    }
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
