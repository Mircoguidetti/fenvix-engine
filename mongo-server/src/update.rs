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

fn add_numbers(a: &Bson, b: &Bson) -> Bson {
    match (a, b) {
        (Bson::Int32(x), Bson::Int32(y)) => match x.checked_add(*y) {
            Some(s) => Bson::Int32(s),
            None => Bson::Int64(*x as i64 + *y as i64),
        },
        (Bson::Int32(_) | Bson::Int64(_), Bson::Int32(_) | Bson::Int64(_)) => {
            let (x, y) = (as_i64(a), as_i64(b));
            match x.checked_add(y) {
                Some(s) => Bson::Int64(s),
                None => Bson::Double(x as f64 + y as f64),
            }
        }
        _ => Bson::Double(crate::query::as_f64(a) + crate::query::as_f64(b)),
    }
}

fn as_i64(v: &Bson) -> i64 {
    match v {
        Bson::Int32(x) => *x as i64,
        Bson::Int64(x) => *x,
        _ => 0,
    }
}

/// Apply the operator form of an update to `doc`.
fn apply_operators(doc: &mut Document, update: &Document, upsert_insert: bool) -> R<()> {
    for (op, spec) in update {
        let fields = spec.as_document().ok_or_else(|| {
            MongoError::new(9, "FailedToParse", format!("Modifiers operate on fields but we found type {} instead. For example: {{$mod: {{<field>: ...}}}} not {{{op}: {}}}", type_name(spec), fmt_bson(spec)))
        })?;
        match op.as_str() {
            "$set" => {
                for (path, v) in fields {
                    set_path(doc, &path.split('.').collect::<Vec<_>>(), v.clone())?;
                }
            }
            "$setOnInsert" => {
                if upsert_insert {
                    for (path, v) in fields {
                        set_path(doc, &path.split('.').collect::<Vec<_>>(), v.clone())?;
                    }
                }
            }
            "$unset" => {
                for (path, _) in fields {
                    unset_path(doc, &path.split('.').collect::<Vec<_>>());
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
                        Some(c) if crate::query::is_number(&c) => add_numbers(&c, inc),
                        Some(c) => {
                            let id = doc.get("_id").map(fmt_bson).unwrap_or_default();
                            return Err(MongoError::type_mismatch(format!(
                                "Cannot apply $inc to a value of non-numeric type. {{_id: {id}}} has the field '{path}' of non-numeric type {}",
                                type_name(&c)
                            )));
                        }
                    };
                    set_path(doc, &path.split('.').collect::<Vec<_>>(), new)?;
                }
            }
            other => {
                return Err(MongoError::failed_to_parse(format!(
                    "Unknown modifier: {other}. Expected a valid update modifier or pipeline-style update specified as an array"
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
    fn formatting() {
        assert_eq!(fmt_doc(&doc! {"_id": 1}), "{ _id: 1 }");
        assert_eq!(fmt_doc(&doc! {"a": "x", "b": 1.0}), "{ a: \"x\", b: 1.0 }");
    }
}
