//! Hashed and wildcard indexes.
//!
//! A hashed index stores a 64-bit hash of the field value in place of the value: equality and `$in` lookups use it,
//! ranges and sorts do not. Numbers hash by their value truncated to an integer (so `2.3` and `2.2` share a hash, as in
//! MongoDB) and the hash does not depend on the numeric type; an array anywhere in the key is an error.
//!
//! A wildcard index (`{"$**": 1}`, `{"a.$**": 1}`, optionally with `wildcardProjection`) stores one entry per leaf
//! path/value pair of a document: `component(path) + component(value)`. Documents descend into embedded documents and
//! into arrays of documents (the path does not change), scalars in arrays are indexed under the array's path, arrays inside
//! arrays are indexed as whole values, and an empty document or array is indexed as such.

use crate::error::MongoError;
use crate::keyenc;
use bson::{doc, Bson, Document};
use std::collections::BTreeMap;

type R<T> = Result<T, MongoError>;

// ---------------------------------------------------------------------------------------------
// Hashing
// ---------------------------------------------------------------------------------------------

fn as_f64(v: &Bson) -> Option<f64> {
    match v {
        Bson::Double(f) => Some(*f),
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Decimal128(d) => d.to_string().parse().ok().or(Some(f64::NAN)),
        _ => None,
    }
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    out.extend((s.len() as u32).to_be_bytes());
    out.extend(s.as_bytes());
}


/// Canonical type order, as MongoDB's `BSONElement::canonicalType` numbers it (hashed first, so it must match).
fn canon_type(v: &Bson) -> i32 {
    match v {
        Bson::MinKey => -1,
        Bson::Undefined => 0,
        Bson::Null => 5,
        Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Decimal128(_) => 10,
        Bson::String(_) | Bson::Symbol(_) => 15,
        Bson::Document(_) => 20,
        Bson::Array(_) => 25,
        Bson::Binary(_) => 30,
        Bson::ObjectId(_) => 35,
        Bson::Boolean(_) => 40,
        Bson::DateTime(_) => 45,
        Bson::Timestamp(_) => 47,
        Bson::RegularExpression(_) => 50,
        Bson::DbPointer(_) => 55,
        Bson::JavaScriptCode(_) => 60,
        Bson::JavaScriptCodeWithScope(_) => 65,
        Bson::MaxKey => 127,
    }
}

/// `safeNumberLong`: doubles truncate (NaN is 0, out of range saturates), decimals round half to even.
fn number_long(v: &Bson) -> i64 {
    match v {
        Bson::Int32(i) => *i as i64,
        Bson::Int64(i) => *i,
        Bson::Double(d) => *d as i64,
        Bson::Decimal128(d) => d.to_string().parse::<f64>().map(|x| x.round_ties_even() as i64).unwrap_or(0),
        _ => 0,
    }
}

fn put_cstr(out: &mut Vec<u8>, s: &str) {
    out.extend(s.as_bytes());
    out.push(0);
}

fn put_lstr(out: &mut Vec<u8>, s: &str) {
    out.extend((s.len() as i32 + 1).to_le_bytes());
    put_cstr(out, s);
}

/// MongoDB's `BSONElementHasher::recursiveHash`: the byte stream that is fed to MD5.
fn canon(v: &Bson, name: Option<&str>, out: &mut Vec<u8>) {
    out.extend(canon_type(v).to_le_bytes());
    if let Some(n) = name {
        put_cstr(out, n);
    }
    match v {
        Bson::MinKey | Bson::MaxKey | Bson::Null | Bson::Undefined => {}
        Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Decimal128(_) => out.extend(number_long(v).to_le_bytes()),
        Bson::String(s) | Bson::Symbol(s) | Bson::JavaScriptCode(s) => put_lstr(out, s),
        Bson::Document(d) => {
            for (k, x) in d {
                canon(x, Some(k), out);
            }
            out.extend(0i32.to_le_bytes());
        }
        Bson::Array(a) => {
            for (i, x) in a.iter().enumerate() {
                canon(x, Some(&i.to_string()), out);
            }
            out.extend(0i32.to_le_bytes());
        }
        Bson::Binary(b) => {
            out.extend((b.bytes.len() as i32).to_le_bytes());
            out.push(u8::from(b.subtype));
            out.extend(&b.bytes);
        }
        Bson::ObjectId(o) => out.extend(o.bytes()),
        Bson::Boolean(b) => out.push(*b as u8),
        Bson::DateTime(d) => out.extend(d.timestamp_millis().to_le_bytes()),
        Bson::Timestamp(t) => {
            out.extend(t.increment.to_le_bytes());
            out.extend(t.time.to_le_bytes());
        }
        Bson::RegularExpression(r) => {
            put_cstr(out, r.pattern.as_str());
            put_cstr(out, r.options.as_str());
        }
        Bson::DbPointer(_) => {}
        Bson::JavaScriptCodeWithScope(c) => {
            for (k, x) in &c.scope {
                canon(x, Some(k), out);
            }
            out.extend(0i32.to_le_bytes());
        }
    }
}

/// The 64-bit key a hashed index stores for a value, and what `$toHashedIndexKey` returns: the first eight bytes of
/// MD5(seed 0 ++ canonical form), read little endian. Identical to MongoDB's, so values agree with a real server.
pub fn hash_value(v: &Bson) -> i64 {
    use md5::{Digest, Md5};
    let mut bytes = 0i32.to_le_bytes().to_vec();
    canon(v, None, &mut bytes);
    let d = Md5::digest(&bytes);
    i64::from_le_bytes(d[..8].try_into().unwrap())
}

/// `hashed` entries of a document: `(component bytes, {field: value})`, or no entry for a sparse index when no key
/// field is present.
pub fn hashed_entries(key: &[(String, bool)], hashed_pos: usize, sparse: bool, d: &Document) -> R<(BTreeMap<Vec<u8>, Document>, bool)> {
    let mut comps: Vec<u8> = Vec::new();
    let mut shown = Document::new();
    let mut any_present = false;
    for (i, (path, asc)) in key.iter().enumerate() {
        let vals = crate::query::path_values(d, path);
        let parts: Vec<&str> = path.split('.').collect();
        let through_array = (1..parts.len()).any(|n| crate::query::path_values(d, &parts[..n].join(".")).iter().any(|v| matches!(v, Bson::Array(_))));
        if through_array || vals.iter().any(|v| matches!(v, Bson::Array(_))) {
            return Err(MongoError::new(16766, "Location16766", format!("Error: hashed indexes do not currently support array values. Found array at path: {path}")));
        }
        let v: Bson = match vals.first() {
            Some(v) => {
                any_present = true;
                (*v).clone()
            }
            None => Bson::Null,
        };
        shown.insert(path.clone(), v.clone());
        if i == hashed_pos {
            comps.extend(keyenc::component(&Bson::Int64(hash_value(&v)), *asc));
        } else {
            comps.extend(keyenc::component(&v, *asc));
        }
    }
    if sparse && !any_present {
        return Ok((BTreeMap::new(), false));
    }
    let mut m = BTreeMap::new();
    m.insert(comps, shown);
    Ok((m, false))
}

/// Replace the constraint on the hashed field by an equality on the hash (other constraints cannot use the hash).
pub fn hashed_constraints(cs: &std::collections::HashMap<String, crate::index::Constraint>, field: &str) -> std::collections::HashMap<String, crate::index::Constraint> {
    let mut out = cs.clone();
    match cs.get(field).and_then(|c| c.eq.as_ref()) {
        Some(eq) => {
            let mut c = crate::index::Constraint::default();
            c.eq = Some(eq.iter().map(|v| Bson::Int64(hash_value(v))).collect());
            out.insert(field.to_string(), c);
        }
        None => {
            out.remove(field);
        }
    }
    out
}

// ---------------------------------------------------------------------------------------------
// Wildcard
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct WildSpec {
    /// `a.$**`: the subtree the index covers.
    pub prefix: Option<String>,
    /// `wildcardProjection`: `true` = inclusion, with its paths.
    pub proj: Option<(bool, Vec<String>)>,
    /// `_id` is covered only when the projection lists it.
    pub id_included: bool,
    pub asc: bool,
}

fn is_wild_field(k: &str) -> bool {
    k == "$**" || k.ends_with(".$**")
}

fn truthy(v: &Bson) -> bool {
    match v {
        Bson::Boolean(b) => *b,
        Bson::Int32(i) => *i != 0,
        Bson::Int64(i) => *i != 0,
        Bson::Double(f) => *f != 0.0,
        _ => true,
    }
}

impl WildSpec {
    /// A single-field wildcard index; `None` for anything else (compound wildcard indexes are only recorded).
    pub fn from_stored(stored: &Document) -> Option<WildSpec> {
        let key = stored.get_document("key").ok()?;
        if key.len() != 1 {
            return None;
        }
        let (k, v) = key.iter().next()?;
        if !is_wild_field(k) {
            return None;
        }
        let asc = !matches!(v, Bson::Int32(i) if *i < 0) && !matches!(v, Bson::Int64(i) if *i < 0) && !matches!(v, Bson::Double(f) if *f < 0.0);
        let prefix = k.strip_suffix(".$**").map(str::to_string);
        let mut proj = None;
        let mut id_included = false;
        if let Ok(p) = stored.get_document("wildcardProjection") {
            let mut inc = Vec::new();
            let mut exc = Vec::new();
            for (path, val) in p {
                if path == "_id" {
                    id_included = truthy(val);
                    continue;
                }
                if truthy(val) {
                    inc.push(path.clone());
                } else {
                    exc.push(path.clone());
                }
            }
            proj = Some(if !inc.is_empty() { (true, inc) } else { (false, exc) });
        }
        Some(WildSpec { prefix, proj, id_included, asc })
    }

    /// Is `path` (a leaf path of a document) covered by the index?
    pub fn covers(&self, path: &str) -> bool {
        let under = |p: &str, base: &str| p == base || (p.len() > base.len() && p.starts_with(base) && p.as_bytes()[base.len()] == b'.');
        if let Some(pre) = &self.prefix {
            if !under(path, pre) {
                return false;
            }
        }
        if under(path, "_id") {
            return self.id_included;
        }
        match &self.proj {
            Some((true, inc)) => inc.iter().any(|p| under(path, p)),
            Some((false, exc)) => !exc.iter().any(|p| under(path, p)),
            None => true,
        }
    }

    /// The `(path, value)` pairs a document contributes.
    pub fn values(&self, d: &Document) -> Vec<(String, Bson)> {
        let mut out = Vec::new();
        for (k, v) in d {
            self.walk(k, v, &mut out);
        }
        out
    }

    fn emit(&self, path: &str, v: &Bson, out: &mut Vec<(String, Bson)>) {
        if self.covers(path) {
            out.push((path.to_string(), v.clone()));
        }
    }

    fn walk(&self, path: &str, v: &Bson, out: &mut Vec<(String, Bson)>) {
        match v {
            Bson::Document(d) if d.is_empty() => self.emit(path, v, out),
            Bson::Document(d) => {
                for (k, x) in d {
                    self.walk(&format!("{path}.{k}"), x, out);
                }
            }
            Bson::Array(a) if a.is_empty() => self.emit(path, v, out),
            Bson::Array(a) => {
                for e in a {
                    match e {
                        Bson::Document(_) => self.walk(path, e, out),
                        other => self.emit(path, other, out),
                    }
                }
            }
            other => self.emit(path, other, out),
        }
    }

    pub fn entries(&self, d: &Document) -> BTreeMap<Vec<u8>, Document> {
        let mut m = BTreeMap::new();
        for (path, v) in self.values(d) {
            let mut k = keyenc::component(&Bson::String(path.clone()), true);
            k.extend(keyenc::component(&v, self.asc));
            m.insert(k, doc! { "path": path, "value": v });
        }
        m
    }
}

/// How well the wildcard index can bound a query, as the planner sees it.
pub enum WildBound {
    /// Entries of one path within a value range.
    Path(String),
    /// The whole index (the query only says the path exists, or has a type).
    Whole,
}

fn path_is_positional(path: &str) -> bool {
    path.split('.').any(|c| !c.is_empty() && c.bytes().all(|b| b.is_ascii_digit()))
}

/// The most selective way to answer `filter` from a wildcard index: an indexable equality or range on a covered path,
/// else the whole index when a covered path is required to exist.
pub fn choose_bound(spec: &WildSpec, filter: &Document, cs: &std::collections::HashMap<String, crate::index::Constraint>) -> Option<WildBound> {
    let scalar_ok = |v: &Bson| match v {
        Bson::Null | Bson::Undefined | Bson::RegularExpression(_) | Bson::MinKey | Bson::MaxKey => false,
        Bson::Document(d) => d.is_empty(),
        Bson::Array(a) => a.is_empty(),
        _ => true,
    };
    let mut best: Option<(u8, WildBound)> = None;
    let mut consider = |rank: u8, b: WildBound| {
        if best.as_ref().map(|(r, _)| rank > *r).unwrap_or(true) {
            best = Some((rank, b));
        }
    };
    let mut paths: Vec<&String> = cs.keys().collect();
    paths.sort();
    for path in paths {
        let c = &cs[path];
        if !spec.covers(path) || path_is_positional(path) {
            continue;
        }
        if let Some(eq) = &c.eq {
            if eq.iter().all(scalar_ok) {
                consider(4, WildBound::Path(path.clone()));
                continue;
            }
        }
        if c.lo.is_some() || c.hi.is_some() || c.regex_prefix.is_some() {
            consider(3, WildBound::Path(path.clone()));
        } else if c.exists {
            consider(1, WildBound::Whole);
        }
    }
    for (k, v) in filter {
        if k.starts_with('$') || !spec.covers(k) || path_is_positional(k) {
            continue;
        }
        if matches!(v, Bson::Document(d) if d.contains_key("$type")) {
            consider(1, WildBound::Whole);
        }
    }
    if best.is_none() && usable_somehow(spec, filter) {
        return Some(WildBound::Whole);
    }
    best.map(|(_, b)| b)
}

/// Does `filter` constrain a covered path in a way MongoDB's planner accepts for a wildcard index (anything but an
/// equality to null / a non-empty document or array, or `$ne`)? Such queries scan the whole index when hinted.
fn usable_somehow(spec: &WildSpec, filter: &Document) -> bool {
    let refused = |cond: &Bson| match cond {
        Bson::Null | Bson::Undefined => true,
        Bson::Document(d) if d.keys().all(|k| k.starts_with('$')) && !d.is_empty() => d.iter().any(|(k, v)| match k.as_str() {
            "$ne" | "$nin" | "$not" | "$size" => true,
            "$exists" => !truthy(v),
            "$eq" => matches!(v, Bson::Null | Bson::Undefined) || matches!(v, Bson::Array(a) if !a.is_empty()) || matches!(v, Bson::Document(x) if !x.is_empty()),
            "$in" => matches!(v, Bson::Array(a) if a.iter().any(|x| matches!(x, Bson::Null | Bson::Undefined | Bson::RegularExpression(_)))),
            _ => false,
        }),
        Bson::Document(d) => !d.is_empty(),
        Bson::Array(a) => !a.is_empty(),
        _ => false,
    };
    let mut any = false;
    for (k, v) in filter {
        match (k.as_str(), v) {
            ("$and", Bson::Array(a)) => {
                if a.iter().any(|c| matches!(c, Bson::Document(d) if usable_somehow(spec, d))) {
                    any = true;
                }
            }
            ("$or", Bson::Array(a)) => {
                if !a.is_empty() && a.iter().all(|c| matches!(c, Bson::Document(d) if usable_somehow(spec, d))) {
                    any = true;
                }
            }
            (k, _) if k.starts_with('$') => {}
            (path, cond) => {
                if spec.covers(path) && !refused(cond) {
                    any = true;
                }
            }
        }
    }
    any
}

// ---------------------------------------------------------------------------------------------
// Specs
// ---------------------------------------------------------------------------------------------

fn spec_err(spec: &Document, code: i32, name: &'static str, msg: &str) -> MongoError {
    MongoError::new(code, name, format!("Error in specification {} :: caused by :: {msg}", crate::update::fmt_doc(spec)))
}

/// Validate the hashed / wildcard parts of an index specification (called for specs that are not text or geo).
pub fn validate_spec(spec: &Document, key: &Document, unique: bool, sparse: bool, ttl: bool) -> R<()> {
    let hashed: Vec<&String> = key.iter().filter(|(_, v)| matches!(v, Bson::String(s) if s == "hashed")).map(|(k, _)| k).collect();
    for (k, v) in key {
        if let Bson::String(s) = v {
            if s != "hashed" && s != "text" && s != "2d" && s != "2dsphere" {
                return Err(spec_err(spec, 67, "CannotCreateIndex", &format!("Unknown index plugin '{s}' in index {{ {k}: \"{s}\" }}")));
            }
        }
    }
    let wild: Vec<&String> = key.keys().filter(|k| is_wild_field(k)).collect();
    for (k, v) in key {
        let wild_ok = is_wild_field(k) && !k.trim_end_matches("$**").contains('$') && !matches!(v, Bson::String(s) if s == "hashed");
        if (k.starts_with('$') || k.split('.').any(|c| c.starts_with('$'))) && !wild_ok {
            return Err(spec_err(spec, 67, "CannotCreateIndex", "Index key contains an illegal field name: field name starts with '$'."));
        }
    }
    if hashed.len() > 1 {
        return Err(MongoError::new(31303, "Location31303", format!("A maximum of one index field is allowed to be hashed but found {} for 'key' {}", hashed.len(), crate::update::fmt_doc(key))));
    }
    if !hashed.is_empty() {
        if unique {
            return Err(MongoError::new(16764, "Location16764", "Currently hashed indexes cannot guarantee uniqueness. Use a regular index.".to_string()));
        }
        let _ = (sparse, ttl);
    }
    if wild.len() > 1 {
        return Err(spec_err(spec, 7246201, "Location7246201", "Index Key can contain only one wildcard index field"));
    }
    if let Some(w) = wild.first() {
        for opt in ["unique", "sparse"] {
            let on = if opt == "unique" { unique } else { sparse };
            if on {
                return Err(spec_err(spec, 67, "CannotCreateIndex", &format!("Index type 'wildcard' does not support the '{opt}' option")));
            }
        }
        if ttl {
            return Err(spec_err(spec, 67, "CannotCreateIndex", "cannot make a TTL index :: caused by :: Index type 'wildcard' does not support the 'expireAfterSeconds' option"));
        }
        if let Some(p) = spec.get("wildcardProjection") {
            let Bson::Document(p) = p else {
                return Err(spec_err(spec, 14, "TypeMismatch", "The field 'wildcardProjection' must be a non-empty object"));
            };
            if key.len() == 1 && **w != *"$**" {
                return Err(spec_err(spec, 9, "FailedToParse", "The field 'wildcardProjection' is only allowed when 'key' is {\"$**\": ±1} or {\"$**\": \"columnstore\"}"));
            }
            if p.is_empty() {
                return Err(spec_err(spec, 9, "FailedToParse", "The 'wildcardProjection' field can't be an empty object"));
            }
            let mut inc = false;
            let mut exc = false;
            let mut paths: Vec<&String> = Vec::new();
            for (path, v) in p {
                if !matches!(v, Bson::Boolean(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_)) {
                    return Err(spec_err(spec, 51271, "Location51271", "Failed to parse projection: wildcardProjection :: caused by :: Bad projection specification, cannot include fields or add computed fields during an exclusion projection"));
                }
                if path != "_id" {
                    if truthy(v) {
                        inc = true;
                    } else {
                        exc = true;
                    }
                }
                paths.push(path);
            }
            if inc && exc {
                return Err(spec_err(spec, 31254, "Location31254", "Failed to parse projection: wildcardProjection :: caused by :: Cannot do exclusion on field in inclusion projection"));
            }
            for (i, a) in paths.iter().enumerate() {
                for b in paths.iter().skip(i + 1) {
                    if a.starts_with(&format!("{b}.")) || b.starts_with(&format!("{a}.")) {
                        return Err(spec_err(spec, 31250, "Location31250", "Failed to parse projection: wildcardProjection :: caused by :: Path collision"));
                    }
                }
            }
        } else if key.len() > 1 && **w == *"$**" {
            return Err(spec_err(spec, 67, "CannotCreateIndex", "Compound wildcard indexes on all fields must also specify 'wildcardProjection' option"));
        }
    } else if spec.get("wildcardProjection").is_some() {
        return Err(spec_err(spec, 2, "BadValue", "The field 'wildcardProjection' is only allowed when 'key' is {\"$**\": ±1} or {\"$**\": \"columnstore\"}"));
    }
    Ok(())
}
