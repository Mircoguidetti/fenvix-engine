//! Per-collection options stored under `meta/copt/{collection}` (for now the default collation) and the `idc/{collection}/`
//! keyspace of a collection with a default collation: `_id` values that are equal under the collation are the same `_id`
//! there, so `idc/{collection}/{collation key of _id}` -> `{encoded _id}` is the unique index of `_id` (a document itself
//! is stored under the encoding of its `_id` as given, as everywhere else).

use crate::collation::Collation;
use crate::layout::escape_collection;
use bson::{Bson, Document};
use std::sync::Arc;

pub struct CollOpts {
    /// The options document as stored (`collation` in its expanded form).
    pub raw: Document,
    pub collation: Option<Arc<Collation>>,
}

impl CollOpts {
    pub fn parse(raw: Document) -> Arc<CollOpts> {
        let collation = raw.get("collation").and_then(|c| crate::collation::parse(c).ok().flatten());
        Arc::new(CollOpts { raw, collation })
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
