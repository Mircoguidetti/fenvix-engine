//! Per-collection options stored under `meta/copt/{collection}` (for now the default collation) and the `idc/{collection}/`
//! keyspace of a collection with a default collation: `_id` values that are equal under the collation are the same `_id`
//! there, so `idc/{collection}/{collation key of _id}` -> `{encoded _id}` is the unique index of `_id` (a document itself
//! is stored under the encoding of its `_id` as given, as everywhere else).

use crate::collation::Collation;
use crate::layout::escape_collection;
use bson::{doc, Bson, Document};
use std::sync::Arc;

pub struct CollOpts {
    /// The options document as stored (`capped`, `size`, `max`, `collation` in its expanded form).
    pub raw: Document,
    pub collation: Option<Arc<Collation>>,
    pub cap: Option<crate::capped::Cap>,
    pub validator: Option<crate::validation::Validator>,
    /// `changeStreamPreAndPostImages: {enabled: true}`: change events keep the document before and after a change.
    pub images: bool,
    /// `clusteredIndex`: the name of the clustered index (documents come back in `_id` order).
    pub clustered: Option<String>,
    /// `timeseries`: the time field and the meta field.
    pub series: Option<(String, Option<String>)>,
}

impl CollOpts {
    pub fn parse(raw: Document) -> Arc<CollOpts> {
        let collation = raw.get("collation").and_then(|c| crate::collation::parse(c).ok().flatten());
        let cap = crate::capped::Cap::from_options(&raw);
        let validator = crate::validation::Validator::from_options(&raw);
        let images = raw.get_document("changeStreamPreAndPostImages").ok().and_then(|d| d.get_bool("enabled").ok()).unwrap_or(false);
        let clustered = match raw.get("clusteredIndex") {
            Some(Bson::Document(c)) => Some(c.get_str("name").unwrap_or("_id_").to_string()),
            Some(Bson::Boolean(true)) => Some("_id_".to_string()),
            _ => None,
        };
        let series = raw.get_document("timeseries").ok().map(|t| (t.get_str("timeField").unwrap_or_default().to_string(), t.get_str("metaField").ok().map(str::to_string)));
        Arc::new(CollOpts { raw, collation, cap, validator, images, clustered, series })
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

/// The `changeStreamPreAndPostImages` option of a `create` / `collMod` command, checked: `{enabled: <bool>}`.
pub fn images_option(cmd_name: &str, cmd: &Document) -> Result<Option<Document>, crate::error::MongoError> {
    let Some(v) = cmd.get("changeStreamPreAndPostImages") else { return Ok(None) };
    let Bson::Document(d) = v else { return Err(type_error(cmd_name, "changeStreamPreAndPostImages", v, "object")) };
    for k in d.keys() {
        if k != "enabled" {
            return Err(crate::error::MongoError::new(40415, "IDLUnknownField", format!("BSON field '{cmd_name}.changeStreamPreAndPostImages.{k}' is an unknown field.")));
        }
    }
    match d.get("enabled") {
        None => Err(crate::error::MongoError::new(40414, "IDLFailedToParse", format!("BSON field '{cmd_name}.changeStreamPreAndPostImages.enabled' is missing but a required field"))),
        Some(Bson::Boolean(b)) => Ok(Some(doc! { "changeStreamPreAndPostImages": { "enabled": *b } })),
        Some(other) => Err(type_error(cmd_name, "changeStreamPreAndPostImages.enabled", other, "bool")),
    }
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

/// WiredTiger's `WT_SESSION::create` keys: the first key of a configuration string that WiredTiger would not know.
fn wt_unknown_key(cfg: &str) -> Option<String> {
    const KEYS: &[&str] = &[
        "access_pattern_hint", "allocation_size", "app_metadata", "assert", "block_allocation", "block_compressor", "cache_resident", "checksum", "colgroups", "collator", "columns",
        "dictionary", "encryption", "exclusive", "extractor", "format", "huffman_key", "huffman_value", "ignore_in_memory_cache_size", "immutable", "import", "internal_item_max",
        "internal_key_max", "internal_key_truncate", "internal_page_max", "key_format", "key_gap", "leaf_item_max", "leaf_key_max", "leaf_page_max", "leaf_value_max", "log", "lsm",
        "memory_page_image_max", "memory_page_max", "os_cache_dirty_max", "os_cache_max", "prefix_compression", "prefix_compression_min", "readonly", "split_deepen_min_child",
        "split_deepen_per_child", "split_pct", "tiered_storage", "type", "value_format", "verbose", "write_timestamp_usage",
    ];
    let (mut depth, mut start) = (0i32, 0usize);
    let mut parts: Vec<&str> = Vec::new();
    for (i, c) in cfg.char_indices() {
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&cfg[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&cfg[start..]);
    parts.iter().map(|p| p.split('=').next().unwrap_or("").trim()).find(|k| !k.is_empty() && !KEYS.contains(k)).map(str::to_string)
}

/// The `storageEngine` document of `create` / `indexOptionDefaults`: only WiredTiger is registered, and it takes a `configString`.
fn check_storage_engine(se: &Document) -> Result<(), crate::error::MongoError> {
    use crate::error::MongoError;
    for (name, v) in se {
        let Bson::Document(opts) = v else {
            return Err(MongoError::bad_value(format!("'storageEngine.{name}' has to be an embedded document.")));
        };
        if name != "wiredTiger" {
            return Err(MongoError::new(72, "InvalidOptions", format!("{name} is not a registered storage engine for this server")));
        }
        for (k, x) in opts {
            if k != "configString" {
                return Err(MongoError::new(72, "InvalidOptions", format!("'{k}' is not a supported option.")));
            }
            let Bson::String(cfg) = x else {
                return Err(MongoError::type_mismatch("'configString' must be a string.".to_string()));
            };
            if let Some(bad) = wt_unknown_key(cfg) {
                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
                return Err(MongoError::bad_value(format!(
                    "22: Invalid argument. [{}:{}][{}:0x{:x}], wiredtiger_config_validate: [WT_VERB_DEFAULT][ERROR]: config_check_search, 65: unknown configuration key: '{bad}': Invalid argument.",
                    now.as_secs(),
                    now.subsec_micros(),
                    std::process::id(),
                    0x7f00_0000_0000u64 + rand::random::<u32>() as u64 * 16
                )));
            }
        }
    }
    Ok(())
}

/// The other stored options of a `create` command: storageEngine, indexOptionDefaults.
pub fn plain_options(cmd_name: &str, cmd: &Document) -> Result<Document, crate::error::MongoError> {
    use crate::error::MongoError;
    let mut out = Document::new();
    for field in ["storageEngine", "indexOptionDefaults"] {
        if let Some(v) = cmd.get(field) {
            let Bson::Document(d) = v else { return Err(type_error(cmd_name, field, v, "object")) };
            if field == "storageEngine" {
                check_storage_engine(d)?;
                if d.is_empty() {
                    continue;
                }
            } else {
                for (k, x) in d {
                    if k != "storageEngine" {
                        return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field '{cmd_name}.indexOptionDefaults.{k}' is an unknown field.")));
                    }
                    let Bson::Document(se) = x else { return Err(type_error(cmd_name, "indexOptionDefaults.storageEngine", x, "object")) };
                    check_storage_engine(se)?;
                }
                if d.is_empty() {
                    continue;
                }
            }
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
