//! Per-collection options stored under `meta/copt/{collection}` (for now the default collation) and the `idc/{collection}/`
//! keyspace of a collection with a default collation: `_id` values that are equal under the collation are the same `_id`
//! there, so `idc/{collection}/{collation key of _id}` -> `{encoded _id}` is the unique index of `_id` (a document itself
//! is stored under the encoding of its `_id` as given, as everywhere else).

use crate::collation::Collation;
use crate::layout::escape_collection;
use bson::{Bson, Document};
use std::sync::Arc;

pub struct CollOpts {
    /// The options document as stored (`capped`, `size`, `max`, `collation` in its expanded form).
    pub raw: Document,
    pub collation: Option<Arc<Collation>>,
    pub cap: Option<crate::capped::Cap>,
    pub validator: Option<crate::validation::Validator>,
}

impl CollOpts {
    pub fn parse(raw: Document) -> Arc<CollOpts> {
        let collation = raw.get("collation").and_then(|c| crate::collation::parse(c).ok().flatten());
        let cap = crate::capped::Cap::from_options(&raw);
        let validator = crate::validation::Validator::from_options(&raw);
        Arc::new(CollOpts { raw, collation, cap, validator })
    }
}

pub fn meta_key(coll: &str) -> Vec<u8> {
    let mut k = b"meta/copt/".to_vec();
    k.extend(escape_collection(coll));
    k
}

pub fn idc_prefix(coll: &str) -> Vec<u8> {
    let mut k = b"idc/".to_vec();
    k.extend(escape_collection(coll));
    k.push(b'/');
    k
}

/// Key of an `_id` in the collation index of its collection.
pub fn idc_key(coll: &str, c: &Collation, id: &Bson) -> Vec<u8> {
    let mut k = idc_prefix(coll);
    k.extend(crate::layout::encode_sortable_with(id, Some(c)));
    k
}

/// Does an `_id` lookup depend on the collation (a string somewhere inside it)?
pub fn has_string(v: &Bson) -> bool {
    match v {
        Bson::String(_) | Bson::Symbol(_) => true,
        Bson::Document(d) => d.values().any(has_string),
        Bson::Array(a) => a.iter().any(has_string),
        _ => false,
    }
}

/// The order mongod lists collection options in.
const ORDER: &[&str] = &[
    "capped", "size", "max", "storageEngine", "indexOptionDefaults", "validator", "validationLevel", "validationAction", "viewOn", "pipeline", "collation", "changeStreamPreAndPostImages", "timeseries", "clusteredIndex", "expireAfterSeconds", "encryptedFields",
];

pub fn canonical(raw: &Document) -> Document {
    let mut out = Document::new();
    for k in ORDER {
        if let Some(v) = raw.get(*k) {
            out.insert(*k, v.clone());
        }
    }
    for (k, v) in raw {
        if !out.contains_key(k) {
            out.insert(k.clone(), v.clone());
        }
    }
    out
}

fn enum_error(cmd: &str, field: &str, v: &str) -> crate::error::MongoError {
    crate::error::MongoError::new(2, "BadValue", format!("Enumeration value '{v}' for field '{cmd}.{field}' is not a valid value."))
}

fn type_error(cmd: &str, field: &str, v: &Bson, expected: &str) -> crate::error::MongoError {
    crate::error::MongoError::new(14, "TypeMismatch", format!("BSON field '{cmd}.{field}' is the wrong type '{}', expected type '{expected}'", crate::update::type_name(v)))
}

/// The validation options of a `create` / `collMod` command, checked: validator, validationLevel, validationAction.
pub fn validation_options(cmd_name: &str, cmd: &Document) -> Result<Document, crate::error::MongoError> {
    let mut out = Document::new();
    if let Some(v) = cmd.get("validator") {
        let Bson::Document(d) = v else { return Err(type_error(cmd_name, "validator", v, "object")) };
        crate::validation::check_expression(d)?;
        out.insert("validator", v.clone());
    }
    for (field, allowed) in [("validationLevel", &["off", "strict", "moderate"][..]), ("validationAction", &["warn", "error"][..])] {
        if let Some(v) = cmd.get(field) {
            let Bson::String(s) = v else { return Err(type_error(cmd_name, field, v, "string")) };
            if !allowed.contains(&s.as_str()) {
                return Err(enum_error(cmd_name, field, s));
            }
            out.insert(field, v.clone());
        }
    }
    Ok(out)
}

/// The other stored options of a `create` command: storageEngine, indexOptionDefaults.
pub fn plain_options(cmd_name: &str, cmd: &Document) -> Result<Document, crate::error::MongoError> {
    let mut out = Document::new();
    for field in ["storageEngine", "indexOptionDefaults"] {
        if let Some(v) = cmd.get(field) {
            let Bson::Document(_) = v else { return Err(type_error(cmd_name, field, v, "object")) };
            out.insert(field, v.clone());
        }
    }
    Ok(out)
}

/// Key of the UUID of a collection (made when it is first asked for, dropped with the collection).
pub fn uuid_key(coll: &str) -> Vec<u8> {
    let mut k = b"meta/cuuid/".to_vec();
    k.extend(escape_collection(coll));
    k
}
