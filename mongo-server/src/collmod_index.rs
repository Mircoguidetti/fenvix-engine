//! `collMod` with an `index` document: `hidden`, `expireAfterSeconds`, `prepareUnique` and `unique` of one index.

use crate::cmd::{ok, Server, R};
use crate::dbs::Entry;
use crate::error::MongoError;
use crate::index::{IndexDef, State};
use bson::{doc, Bson, Document};
use std::collections::BTreeMap;

const FIELDS: [&str; 6] = ["name", "keyPattern", "hidden", "expireAfterSeconds", "prepareUnique", "unique"];

fn wrong(field: &str, v: &Bson, want: &str) -> MongoError {
    MongoError::type_mismatch(format!("BSON field 'collMod.index.{field}' is the wrong type '{}', expected {want}", crate::update::type_name(v)))
}

fn number(v: &Bson) -> Option<f64> {
    match v {
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Double(f) => Some(*f),
        Bson::Decimal128(d) => d.to_string().parse::<f64>().ok(),
        _ => None,
    }
}

/// A boolean option: a boolean or any number.
fn flag(field: &str, v: &Bson) -> R<bool> {
    match v {
        Bson::Boolean(b) => Ok(*b),
        other => number(other).map(|n| n != 0.0).ok_or_else(|| wrong(field, other, "types '[bool, long, int, decimal, double']")),
    }
}

/// The spec with `field` removed and put last again (collMod rewrites the options it changes at the end).
fn set(spec: &mut Document, field: &str, v: Bson) {
    spec.remove(field);
    spec.insert(field, v);
}

impl Server {
    pub(crate) async fn coll_mod_index(&self, g: &mut Entry, db: &str, coll: &str, spec: &Bson) -> R<Document> {
        let Bson::Document(spec) = spec else { return Err(wrong_index(spec)) };
        for (k, _) in spec {
            if !FIELDS.contains(&k.as_str()) {
                return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field 'collMod.index.{k}' is an unknown field.")));
            }
        }
        if let Some(v) = spec.get("name") {
            if v.as_str().is_none() {
                return Err(wrong("name", v, "type 'string'"));
            }
        }
        if let Some(v) = spec.get("keyPattern") {
            if !matches!(v, Bson::Document(_)) {
                return Err(wrong("keyPattern", v, "type 'object'"));
            }
        }
        let hidden = spec.get("hidden").map(|v| flag("hidden", v)).transpose()?;
        let prepare = spec.get("prepareUnique").map(|v| flag("prepareUnique", v)).transpose()?;
        let unique = spec.get("unique").map(|v| flag("unique", v)).transpose()?;
        let expire = match spec.get("expireAfterSeconds") {
            Some(v) => Some(number(v).ok_or_else(|| wrong("expireAfterSeconds", v, "types '[long, int, decimal, double']"))?),
            None => None,
        };
        match (spec.contains_key("name"), spec.contains_key("keyPattern")) {
            (true, true) => return Err(MongoError::new(72, "InvalidOptions", "Cannot specify both key pattern and name.".into())),
            (false, false) => return Err(MongoError::new(72, "InvalidOptions", "Must specify either index name or key pattern.".into())),
            _ => {}
        }
        if hidden.is_none() && prepare.is_none() && unique.is_none() && expire.is_none() {
            return Err(MongoError::new(72, "InvalidOptions", "no expireAfterSeconds, hidden, unique, or prepareUnique field".into()));
        }
        let mut indexes = self.load_indexes(g, db, coll).await?;
        let ns = format!("{db}.{coll}");
        let pos = if let Ok(name) = spec.get_str("name") {
            let p = indexes.iter().position(|i| i.name == name && i.state == State::Ready);
            if p.is_none() && name != "_id_" {
                return Err(MongoError::new(27, "IndexNotFound", format!("cannot find index {name} for ns {ns}")));
            }
            p
        } else {
            let kp = spec.get_document("keyPattern").unwrap();
            let p = indexes.iter().position(|i| i.state == State::Ready && i.spec.get_document("key").map(|k| k == kp).unwrap_or(false));
            if p.is_none() && !(kp.len() == 1 && kp.contains_key("_id")) {
                return Err(MongoError::new(27, "IndexNotFound", format!("cannot find index {} for ns {ns}", crate::update::fmt_doc(kp))));
            }
            p
        };
        let Some(pos) = pos else {
            // the _id index: hiding it is refused, everything else is already the case
            if hidden == Some(true) {
                return Err(MongoError::bad_value("can't hide _id index".into()));
            }
            return Ok(ok());
        };
        let idx = indexes[pos].clone();
        if hidden == Some(true) && idx.key.len() == 1 && idx.key[0].0 == "_id" {
            return Err(MongoError::bad_value("can't hide _id index".into()));
        }
        if let Some(e) = expire {
            if e < 0.0 {
                return Err(MongoError::new(72, "InvalidOptions", "TTL index 'expireAfterSeconds' option cannot be less than 0".into()));
            }
        }
        match unique {
            Some(false) => return Err(MongoError::bad_value("Cannot make index non-unique".into())),
            Some(true) if !idx.unique && !prepare.unwrap_or(idx.prepare_unique) => {
                return Err(MongoError::new(72, "InvalidOptions", "Cannot make index unique with 'prepareUnique=false'. Run collMod to set it first.".into()));
            }
            Some(true) if !idx.unique => {
                if let Some(violations) = self.unique_violations(g, coll, &idx).await? {
                    let mut e = MongoError::new(359, "CannotConvertIndexToUnique", "Cannot convert the index to unique. Please resolve conflicting documents before running collMod again.".into());
                    e.extra = Some(doc! { "violations": violations });
                    return Err(e);
                }
            }
            _ => {}
        }
        let mut stored = idx.to_stored();
        let mut reply = Document::new();
        let mut hidden_reply = Document::new();
        if let Some(h) = hidden {
            if h != idx.hidden {
                hidden_reply.insert("hidden_old", idx.hidden);
                hidden_reply.insert("hidden_new", h);
                if h {
                    set(&mut stored, "hidden", Bson::Boolean(true));
                } else {
                    stored.remove("hidden");
                }
            }
        }
        if let Some(e) = expire {
            let new = e.min(i32::MAX as f64) as i32;
            if let Some(old) = idx.spec.get("expireAfterSeconds").and_then(number) {
                reply.insert("expireAfterSeconds_old", old.min(i32::MAX as f64) as i64);
            }
            reply.insert("expireAfterSeconds_new", new as i64);
            set(&mut stored, "expireAfterSeconds", Bson::Int32(new));
        }
        reply.extend(hidden_reply);
        if let Some(p) = prepare {
            if p != idx.prepare_unique && !idx.unique {
                reply.insert("prepareUnique_old", idx.prepare_unique);
                reply.insert("prepareUnique_new", p);
                if p {
                    set(&mut stored, "prepareUnique", Bson::Boolean(true));
                } else {
                    stored.remove("prepareUnique");
                }
            }
        }
        if unique == Some(true) && !idx.unique {
            reply.insert("unique_new", true);
            stored.remove("prepareUnique");
            set(&mut stored, "unique", Bson::Boolean(true));
        }
        if stored != idx.to_stored() {
            indexes[pos] = IndexDef::from_stored(&stored).ok_or_else(|| MongoError::internal("index specification lost".into()))?;
            g.apply(vec![Entry::indexes_op(coll, &indexes)?]).await?;
        }
        reply.insert("ok", 1.0);
        Ok(reply)
    }

    /// The groups of documents that share a key of `idx`: `[{ids: [...]}, ...]`, `None` when the keys are distinct.
    async fn unique_violations(&self, g: &mut Entry, coll: &str, idx: &IndexDef) -> R<Option<Vec<Bson>>> {
        let mut seen: BTreeMap<Vec<u8>, Vec<Bson>> = BTreeMap::new();
        for d in g.scan_collection(coll).await? {
            let id = d.get("_id").cloned().unwrap_or(Bson::Null);
            for (k, _) in idx.entries(&d)?.0 {
                let ids = seen.entry(k).or_default();
                if !ids.contains(&id) {
                    ids.push(id.clone());
                }
            }
        }
        let groups: Vec<Bson> = seen.into_values().filter(|ids| ids.len() > 1).map(|ids| Bson::Document(doc! { "ids": ids })).collect();
        Ok(if groups.is_empty() { None } else { Some(groups) })
    }
}

fn wrong_index(v: &Bson) -> MongoError {
    MongoError::type_mismatch(format!("BSON field 'collMod.index' is the wrong type '{}', expected type 'object'", crate::update::type_name(v)))
}
