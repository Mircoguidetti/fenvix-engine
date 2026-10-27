//! The Stable API: `apiVersion`, `apiStrict` and `apiDeprecationErrors` on a command. Version "1" is the only one; with
//! `apiStrict` a command that is not in it is refused (the list is the `apiVersions` of mongod's `listCommands`).

use crate::cmd::R;
use crate::error::MongoError;
use bson::{Bson, Document};
use std::collections::HashSet;
use std::sync::OnceLock;

const LIST: &str = include_str!("diag/listCommands.json");
const DOCHUB: &str = "Information on supported commands and migrations in API Version 1 can be found at https://dochub.mongodb.org/core/manual-versioned-api";
/// Aggregation stages that API version 1 leaves out.
const STAGES_OUT: [&str; 5] = ["$currentOp", "$indexStats", "$listLocalSessions", "$listSessions", "$planCacheStats"];

fn v1() -> &'static HashSet<String> {
    static S: OnceLock<HashSet<String>> = OnceLock::new();
    S.get_or_init(|| {
        let d = match serde_json::from_str::<Bson>(LIST) {
            Ok(Bson::Document(d)) => d,
            _ => Document::new(),
        };
        let cmds = d.get_document("commands").cloned().unwrap_or_default();
        cmds.iter()
            .filter(|(_, v)| matches!(v, Bson::Document(c) if c.get_array("apiVersions").map(|a| a.iter().any(|x| x.as_str() == Some("1"))).unwrap_or(false)))
            .map(|(k, _)| k.clone())
            .collect()
    })
}

fn wrong(field: &str, v: &Bson, want: &str) -> MongoError {
    MongoError::type_mismatch(format!("BSON field 'APIParametersFromClient.{field}' is the wrong type '{}', expected type '{want}'", crate::update::type_name(v)))
}

pub(crate) fn check(name: &str, cmd: &Document) -> R<()> {
    let version = match cmd.get("apiVersion") {
        None => None,
        Some(Bson::String(s)) => Some(s.as_str()),
        Some(other) => return Err(wrong("apiVersion", other, "string")),
    };
    let mut strict = false;
    for f in ["apiStrict", "apiDeprecationErrors"] {
        match cmd.get(f) {
            None => {}
            Some(Bson::Boolean(b)) => {
                if f == "apiStrict" {
                    strict = *b;
                }
            }
            Some(other) => return Err(wrong(f, other, "bool")),
        }
    }
    let Some(version) = version else {
        if cmd.contains_key("apiStrict") || cmd.contains_key("apiDeprecationErrors") {
            return Err(MongoError::new(4886600, "Location4886600", "Provided apiStrict and/or apiDeprecationErrors without passing apiVersion".into()));
        }
        return Ok(());
    };
    if version != "1" {
        return Err(MongoError::new(322, "APIVersionError", format!("Cannot accept API version {version}")));
    }
    if !strict {
        return Ok(());
    }
    let strict_err = |m: String| MongoError::new(323, "APIStrictError", m);
    if !v1().contains(name) {
        return Err(strict_err(format!("Provided apiStrict:true, but the command {name} is not in API Version 1. {DOCHUB}")));
    }
    match name {
        "aggregate" => {
            for st in cmd.get_array("pipeline").into_iter().flatten() {
                if let Some(k) = st.as_document().and_then(|d| d.keys().next()) {
                    if STAGES_OUT.contains(&k.as_str()) {
                        return Err(strict_err(format!("{k} is not allowed with 'apiStrict: true' in API Version 1")));
                    }
                }
            }
        }
        "createIndexes" => {
            for spec in cmd.get_array("indexes").into_iter().flatten() {
                if let Some(d) = spec.as_document() {
                    if d.get_document("key").map(|k| k.values().any(|v| matches!(v, Bson::String(s) if s == "text"))).unwrap_or(false) {
                        return Err(strict_err(format!("Error in specification {} :: caused by :: text indexes cannot be created with apiStrict: true", crate::update::fmt_doc(d))));
                    }
                }
            }
        }
        "create" => {
            for f in ["size", "max", "autoIndexId"] {
                if cmd.contains_key(f) {
                    return Err(strict_err(format!("BSON field 'create.{f}' is not allowed with apiStrict:true.")));
                }
            }
        }
        _ => {}
    }
    Ok(())
}
