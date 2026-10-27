//! Operations that touch two databases (two tenants): `renameCollection` across databases and the `$out` / `$merge`
//! targets in another database. A database is one tenant (one SlateDB), so there is no batch that spans two of them: a
//! cross-database rename writes the whole collection into the target tenant in one batch and then removes the source in
//! another, as mongod does (it clones into the target and drops the source); a crash in between leaves both.

use crate::cmd::{bool_arg, ok, Server, R};
use crate::dbs::{valid_db_name, Entry};
use crate::error::MongoError;
use crate::index::State;
use crate::layout::{doc_prefix, idx_collection_prefix, index_key};
use bson::{doc, Bson, Document};
use tenant_bench::lifecycle::Op;

/// The most keys a collection may have to be moved in one batch.
const MOVE_MAX_OPS: usize = 1_000_000;

pub struct Rename {
    pub from_db: String,
    pub from: String,
    pub to_db: String,
    pub to: String,
    pub drop_target: bool,
}

fn split_ns(ns: &str) -> Option<(&str, &str)> {
    ns.split_once('.').filter(|(d, c)| !d.is_empty() && !c.is_empty())
}

/// Parse and check a `renameCollection` command (run against `db`).
pub fn parse_rename(db: &str, cmd: &Document) -> R<Rename> {
    if db != "admin" {
        return Err(MongoError::new(13, "Unauthorized", "renameCollection may only be run against the admin database.".into()));
    }
    let get = |field: &str| -> R<String> {
        match cmd.get(field) {
            Some(Bson::String(s)) => Ok(s.clone()),
            Some(other) => Err(MongoError::new(14, "TypeMismatch", format!("BSON field 'renameCollection.{}' is the wrong type '{}', expected type 'string'", if field == "renameCollection" { "renameCollection" } else { field }, crate::update::type_name(other)))),
            None => Err(MongoError::new(40414, "IDLFailedToParse", format!("BSON field 'renameCollection.{}' is missing but a required field", if field == "renameCollection" { "renameCollection" } else { field }))),
        }
    };
    let (src, dst) = (get("renameCollection")?, get("to")?);
    // (an empty collection name is shown as the database alone)
    let shown = |ns: &str| ns.strip_suffix('.').unwrap_or(ns).to_string();
    let (from_db, from) = split_ns(&src).ok_or_else(|| MongoError::new(73, "InvalidNamespace", format!("Invalid source namespace: {}", shown(&src))))?;
    let (to_db, to) = split_ns(&dst).ok_or_else(|| MongoError::new(73, "InvalidNamespace", format!("Invalid target namespace: {}", shown(&dst))))?;
    let drop_target = match cmd.get("dropTarget") {
        None => false,
        Some(Bson::Boolean(b)) => *b,
        Some(v @ (Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_))) => bool_arg(Some(v), false),
        Some(other) => return Err(MongoError::new(14, "TypeMismatch", format!("BSON field 'renameCollection.dropTarget' is the wrong type '{}', expected types '[bool, binData']", crate::update::type_name(other)))),
    };
    if !valid_db_name(from_db) {
        return Err(MongoError::new(73, "InvalidNamespace", format!("Invalid source namespace: {src}")));
    }
    if !valid_db_name(to_db) {
        return Err(MongoError::new(73, "InvalidNamespace", format!("Invalid target namespace: {dst}")));
    }
    if from_db == to_db && from == to {
        return Err(MongoError::new(20, "IllegalOperation", "Can't rename a collection to itself".into()));
    }
    if from == "system.js" || to == "system.js" {
        return Err(MongoError::new(20, "IllegalOperation", "renaming system.js collection or renaming to system.js is not allowed".into()));
    }
    if to.starts_with("system.") {
        return Err(MongoError::new(20, "IllegalOperation", format!("error with target namespace: Invalid system namespace: {dst}")));
    }
    if to.contains('$') || to.contains('\0') {
        return Err(MongoError::new(20, "IllegalOperation", format!("error with target namespace: Invalid collection name: {to}")));
    }
    if from.starts_with("system.") {
        return Err(MongoError::new(20, "IllegalOperation", format!("error with source namespace: Invalid system namespace: {src}")));
    }
    Ok(Rename { from_db: from_db.to_string(), from: from.to_string(), to_db: to_db.to_string(), to: to.to_string(), drop_target })
}

/// Everything stored for a collection: (prefix, same keys under the other collection's name) pairs and single metadata keys.
fn parts(coll: &str) -> ([Vec<u8>; 4], [Vec<u8>; 5]) {
    ([doc_prefix(coll), idx_collection_prefix(coll), crate::natural::prefix(coll), crate::collopts::idc_prefix(coll)], [index_key(coll), crate::natural::meta_key(coll), crate::collopts::meta_key(coll), crate::collopts::uuid_key(coll), crate::capped::stat_key(coll)])
}

impl Server {
    /// `renameCollection` from one database to another.
    pub(crate) async fn rename_across(&self, r: &Rename) -> R<Document> {
        // both tenants stay locked for the whole move; locking in name order keeps two opposite renames from deadlocking
        let (first, second) = if r.from_db <= r.to_db { (&r.from_db, &r.to_db) } else { (&r.to_db, &r.from_db) };
        let mut g1 = self.dbs.lock(first).await;
        let mut g2 = self.dbs.lock(second).await;
        let (src, dst): (&mut Entry, &mut Entry) = if r.from_db <= r.to_db { (&mut *g1, &mut *g2) } else { (&mut *g2, &mut *g1) };
        let src_names = src.collections().await?;
        if !src_names.iter().any(|n| *n == r.from) {
            return Err(MongoError::namespace_not_found("source namespace does not exist".into()));
        }
        let mut dst_names = dst.collections().await?;
        let target_exists = dst_names.iter().any(|n| *n == r.to);
        if target_exists && !r.drop_target {
            return Err(MongoError::namespace_exists("target namespace exists".into()));
        }
        if src.indexes(&r.from).await?.iter().any(|i| i.state != State::Ready) {
            return Err(MongoError::new(2, "BadValue", format!("cannot rename {}.{}: an index build is in progress", r.from_db, r.from)));
        }
        let (from_prefixes, from_meta) = parts(&r.from);
        let (to_prefixes, to_meta) = parts(&r.to);
        let mut put: Vec<Op> = Vec::new();
        if target_exists {
            for p in &to_prefixes {
                for (k, _) in dst.scan(p).await? {
                    put.push(Op::Delete(k.to_vec()));
                }
            }
            for k in &to_meta {
                put.push(Op::Delete(k.clone()));
            }
        }
        let mut gone: Vec<Op> = Vec::new();
        for (old, new) in from_prefixes.iter().zip(&to_prefixes) {
            for (k, v) in src.scan(old).await? {
                let mut nk = new.clone();
                nk.extend_from_slice(&k[old.len()..]);
                put.push(Op::Put(nk, v.to_vec()));
                gone.push(Op::Delete(k.to_vec()));
            }
        }
        for (old, new) in from_meta.iter().zip(&to_meta) {
            if let Some(v) = src.get(old).await? {
                put.push(Op::Put(new.clone(), v.to_vec()));
                gone.push(Op::Delete(old.clone()));
            }
        }
        if put.len() > MOVE_MAX_OPS {
            return Err(MongoError::new(238, "NotImplemented", format!("collection {}.{} is too large to rename atomically ({} keys, limit {MOVE_MAX_OPS})", r.from_db, r.from, put.len())));
        }
        dst_names.retain(|n| *n != r.to);
        dst_names.push(r.to.clone());
        put.push(Entry::collections_op(&dst_names)?);
        dst.apply(put).await?;
        dst.forget_coll_opts(&r.to);
        let mut src_names = src_names;
        src_names.retain(|n| *n != r.from);
        gone.push(Entry::collections_op(&src_names)?);
        src.apply(gone).await?;
        src.forget_coll_opts(&r.from);
        Ok(ok())
    }
}

// ---- $out / $merge targets -------------------------------------------------------------------------------------------

pub struct Target {
    pub db: String,
    pub coll: String,
}

fn internal_db(db: &str) -> bool {
    matches!(db, "admin" | "local" | "config")
}

fn bson_type(v: &Bson) -> &'static str {
    crate::update::type_name(v)
}

/// The target of `$out`: a collection of the aggregation's database, or `{db, coll}`.
pub fn parse_out(spec: &Bson, cur_db: &str) -> R<Target> {
    let (db, coll) = match spec {
        Bson::String(s) => (cur_db.to_string(), s.clone()),
        Bson::Document(d) => {
            let mut db = None;
            let mut coll = None;
            for (k, v) in d {
                match k.as_str() {
                    "db" => db = Some(v),
                    "coll" => coll = Some(v),
                    "timeseries" => return Err(MongoError::new(238, "NotImplemented", "$out to a time-series collection is not supported by this server".into())),
                    other => return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field '$out.{other}' is an unknown field."))),
                }
            }
            let Some(coll) = coll else { return Err(MongoError::new(40414, "IDLFailedToParse", "BSON field '$out.coll' is missing but a required field".into())) };
            let Some(db) = db else { return Err(MongoError::new(40414, "IDLFailedToParse", "BSON field '$out.db' is missing but a required field".into())) };
            let s = |v: &Bson, f: &str| -> R<String> {
                match v {
                    Bson::String(s) => Ok(s.clone()),
                    other => Err(MongoError::type_mismatch(format!("BSON field '$out.{f}' is the wrong type '{}', expected type 'string'", bson_type(other)))),
                }
            };
            (s(db, "db")?, s(coll, "coll")?)
        }
        other => return Err(MongoError::new(16990, "Location16990", format!("$out only supports a string or object argument, but found {}", bson_type(other)))),
    };
    if db.contains('\0') || coll.contains('\0') {
        return Err(MongoError::new(73, "InvalidNamespace", "namespaces cannot have embedded null characters".into()));
    }
    if db.len() > 63 {
        return Err(MongoError::new(73, "InvalidNamespace", format!("db name must be at most 63 characters, found: {}", db.len())));
    }
    if coll.is_empty() || db.is_empty() || db.contains('.') {
        let shown = if coll.is_empty() { db.clone() } else { format!("{db}.{coll}") };
        return Err(MongoError::new(73, "InvalidNamespace", format!("Invalid $out target namespace, {shown}")));
    }
    if internal_db(&db) {
        return Err(MongoError::new(31321, "Location31321", format!("Can't $out to internal database: {db}")));
    }
    if coll.starts_with("system.") {
        return Err(MongoError::new(17385, "Location17385", format!("Can't $out to special collection: {coll}")));
    }
    if coll.contains('$') {
        return Err(MongoError::new(20, "IllegalOperation", format!("error with target namespace: Invalid collection name: {coll}")));
    }
    if db.chars().any(|c| "/\\ \"$*<>:|?".contains(c)) {
        return Err(MongoError::new(73, "InvalidNamespace", format!("Invalid namespace: {db}.tmp.agg_out.{}", crate::cmd::fake_uuid())));
    }
    Ok(Target { db, coll })
}

pub struct MergeSpec {
    pub target: Target,
    pub on: Vec<String>,
    /// `whenMatched`: a mode name or a pipeline.
    pub when_matched: Bson,
    pub when_not_matched: String,
    pub on_given: bool,
    pub lets: Option<Document>,
}

/// Parse `$merge` (a collection name, or a document) and check everything that can be checked without the target.
pub fn parse_merge(spec: &Bson, cur_db: &str) -> R<MergeSpec> {
    let into_target = |into: &Bson| -> R<Target> {
        let (db, coll) = match into {
            Bson::String(s) => (cur_db.to_string(), s.clone()),
            Bson::Document(d) => {
                let coll = match d.get("coll") {
                    Some(Bson::String(s)) if !s.is_empty() => s.clone(),
                    _ => return Err(MongoError::new(5786801, "Location5786801", "$merge 'into' field must specify a 'coll' that is not empty, null or undefined".into())),
                };
                let db = match d.get("db") {
                    None | Some(Bson::Null) => cur_db.to_string(),
                    Some(Bson::String(s)) if s.is_empty() => cur_db.to_string(),
                    Some(Bson::String(s)) => s.clone(),
                    Some(other) => return Err(MongoError::type_mismatch(format!("BSON field 'into.db' is the wrong type '{}', expected type 'string'", bson_type(other)))),
                };
                (db, coll)
            }
            other => return Err(MongoError::new(51178, "Location51178", format!("$merge 'into' field  must be either a string or an object, but found {}", bson_type(other)))),
        };
        if db.contains('.') {
            return Err(MongoError::new(73, "InvalidNamespace", format!("'.' is an invalid character in a db name: {db}")));
        }
        if internal_db(&db) {
            return Err(MongoError::new(31320, "Location31320", format!("Cannot $merge to internal database: {db}")));
        }
        if coll.starts_with("system.") {
            return Err(MongoError::new(31319, "Location31319", format!("Cannot $merge to special collection: {coll}")));
        }
        Ok(Target { db, coll })
    };
    let (target, d) = match spec {
        Bson::String(s) => {
            if s.is_empty() {
                return Err(MongoError::new(73, "InvalidNamespace", format!("Invalid $merge target namespace: '{cur_db}.'")));
            }
            (into_target(spec)?, Document::new())
        }
        Bson::Document(d) => {
            for k in d.keys() {
                if !matches!(k.as_str(), "into" | "on" | "let" | "whenMatched" | "whenNotMatched") {
                    return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field '$merge.{k}' is an unknown field.")));
                }
            }
            let Some(into) = d.get("into") else { return Err(MongoError::new(40414, "IDLFailedToParse", "BSON field '$merge.into' is missing but a required field".into())) };
            (into_target(into)?, d.clone())
        }
        other => return Err(MongoError::type_mismatch(format!("$merge requires a string or object argument, but found {}", bson_type(other)))),
    };
    let on_given = d.contains_key("on");
    let on = match d.get("on") {
        None => vec!["_id".to_string()],
        Some(Bson::String(s)) => vec![s.clone()],
        Some(Bson::Array(a)) => {
            if a.is_empty() {
                return Err(MongoError::new(51187, "Location51187", "If explicitly specifying $merge 'on', must include at least one field".into()));
            }
            let mut v = Vec::new();
            for x in a {
                match x {
                    Bson::String(s) => v.push(s.clone()),
                    other => return Err(MongoError::new(51134, "Location51134", format!("$merge 'on' array elements must be strings, but found {}", bson_type(other)))),
                }
            }
            v
        }
        Some(other) => return Err(MongoError::new(51186, "Location51186", format!("$merge 'on' field  must be either a string or an array of strings, but found {}", bson_type(other)))),
    };
    let when_matched = match d.get("whenMatched") {
        None => Bson::String("merge".into()),
        Some(Bson::String(s)) if matches!(s.as_str(), "replace" | "keepExisting" | "merge" | "fail") => Bson::String(s.clone()),
        Some(Bson::String(s)) => return Err(MongoError::bad_value(format!("Enumeration value '{s}' for field 'whenMatched' is not a valid value."))),
        Some(Bson::Array(a)) => Bson::Array(a.clone()),
        Some(other) => return Err(MongoError::new(51191, "Location51191", format!("$merge 'whenMatched' field  must be either a string or an array, but found {}", bson_type(other)))),
    };
    let when_not_matched = match d.get("whenNotMatched") {
        None => "insert".to_string(),
        Some(Bson::String(s)) if matches!(s.as_str(), "insert" | "discard" | "fail") => s.clone(),
        Some(Bson::String(s)) => return Err(MongoError::bad_value(format!("Enumeration value '{s}' for field '$merge.whenNotMatched' is not a valid value."))),
        Some(other) => return Err(MongoError::type_mismatch(format!("BSON field '$merge.whenNotMatched' is the wrong type '{}', expected type 'string'", bson_type(other)))),
    };
    let lets = match d.get("let") {
        None => None,
        Some(Bson::Document(l)) => Some(l.clone()),
        Some(other) => return Err(MongoError::type_mismatch(format!("BSON field '$merge.let' is the wrong type '{}', expected type 'object'", bson_type(other)))),
    };
    if matches!(&when_matched, Bson::String(m) if m == "fail") && when_not_matched == "fail" {
        return Err(MongoError::new(51181, "Location51181", "Combination of $merge modes 'whenMatched: fail' and 'whenNotMatched: fail' is not supported".into()));
    }
    Ok(MergeSpec { target, on, when_matched, when_not_matched, on_given, lets })
}

/// `$merge` needs a unique index on exactly its `on` fields (simple collation, not partial), or the target not to exist and `on` to be `_id`.
pub fn check_merge_on(on: &[String], exists: bool, indexes: &[crate::index::IndexDef], coll_collated: bool) -> R<()> {
    let fail = || MongoError::new(51183, "Location51183", "Cannot find index to verify that join fields will be unique".into());
    if on == ["_id".to_string()] {
        return if coll_collated { Err(fail()) } else { Ok(()) };
    }
    if !exists {
        return Err(fail());
    }
    let mut want: Vec<&str> = on.iter().map(String::as_str).collect();
    want.sort_unstable();
    let ok = indexes.iter().any(|i| {
        let mut keys: Vec<&str> = i.key.iter().map(|(k, _)| k.as_str()).collect();
        keys.sort_unstable();
        i.unique && i.state == State::Ready && i.partial.is_none() && i.collation.is_none() && !i.sparse && matches!(i.kind, crate::index::Kind::Regular) && keys == want
    });
    if ok { Ok(()) } else { Err(fail()) }
}

pub fn ok_doc() -> Document {
    doc! { "ok": 1.0 }
}
