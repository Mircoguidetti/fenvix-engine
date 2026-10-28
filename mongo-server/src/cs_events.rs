//! Change events of DDL operations (`create`, `createIndexes`, `dropIndexes`, `modify`, `rename`, `drop`), which streams with
//! `showExpandedEvents` also see (`rename` and `drop` are standard events).

use crate::changes::EvSpec;
use crate::cmd::Server;
use bson::{doc, Bson, Document};

/// An index specification as a change event shows it: the flags `unique`, `prepareUnique` and `hidden` come right after `v`.
pub(crate) fn index_spec_for_event(spec: &Document, flags_first: &[&str]) -> Document {
    let mut out = Document::new();
    if let Some(v) = spec.get("v") {
        out.insert("v", v.clone());
    }
    for f in flags_first {
        if let Some(v) = spec.get(*f) {
            out.insert(*f, v.clone());
        }
    }
    for (k, v) in spec {
        if !out.contains_key(k) {
            out.insert(k.clone(), v.clone());
        }
    }
    out
}

impl Server {
    pub(crate) fn publish_ddl(&self, mut e: EvSpec, expanded_only: bool) {
        e.expanded_only = expanded_only;
        self.dbs.changes.publish(vec![e]);
    }

    /// `create` of a collection: its options and the `_id` index.
    pub(crate) fn publish_create(&self, db: &str, coll: &str, uuid: Vec<u8>, options: &Document, view: bool) {
        let mut desc = options.clone();
        if !view {
            let mut id_index = doc! { "v": 2, "key": { "_id": 1 }, "name": "_id_" };
            if let Some(c) = options.get("collation") {
                id_index.insert("collation", c.clone());
            }
            desc.insert("idIndex", id_index);
        }
        self.publish_ddl(EvSpec { uuid: Some(uuid), op_desc: Some(desc), ..EvSpec::on(db, coll, "create") }, true);
    }

    pub(crate) fn publish_create_indexes(&self, db: &str, coll: &str, uuid: Vec<u8>, specs: &[Document]) {
        let list: Vec<Bson> = specs.iter().map(|s| Bson::Document(index_spec_for_event(s, &["unique", "prepareUnique", "hidden"]))).collect();
        self.publish_ddl(EvSpec { uuid: Some(uuid), op_desc: Some(doc! { "indexes": list }), ..EvSpec::on(db, coll, "createIndexes") }, true);
    }

    pub(crate) fn publish_drop_index(&self, db: &str, coll: &str, uuid: Vec<u8>, spec: &Document) {
        let list = vec![Bson::Document(index_spec_for_event(spec, &["unique", "prepareUnique"]))];
        self.publish_ddl(EvSpec { uuid: Some(uuid), op_desc: Some(doc! { "indexes": list }), ..EvSpec::on(db, coll, "dropIndexes") }, true);
    }

    pub(crate) fn publish_rename(&self, db: &str, from: &str, to_db: &str, to: &str, uuid: Vec<u8>, dropped_target: Option<Vec<u8>>) {
        let mut desc = doc! { "to": { "db": to_db, "coll": to } };
        if let Some(u) = dropped_target {
            desc.insert("dropTarget", Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: u }));
        }
        self.publish_ddl(EvSpec { to: Some((to_db.to_string(), to.to_string())), uuid: Some(uuid), op_desc: Some(desc), ..EvSpec::on(db, from, "rename") }, false);
    }

    pub(crate) fn publish_drop(&self, db: &str, coll: &str, uuid: Vec<u8>) {
        self.publish_ddl(EvSpec { uuid: Some(uuid), ..EvSpec::on(db, coll, "drop") }, false);
    }
}
