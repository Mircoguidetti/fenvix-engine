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
    /// A DDL event: with the durable history on (`chglog.rs`), written as a batch of its own under the database's lock
    /// (the command's reply waits for it like for its other writes).
    pub(crate) async fn publish_ddl(&self, mut e: EvSpec, expanded_only: bool) -> crate::cmd::R<()> {
        e.expanded_only = expanded_only;
        if !crate::chglog::enabled() {
            self.dbs.changes.publish(vec![e]);
            return Ok(());
        }
        let db = e.db.clone();
        let mut g = self.dbs.lock(&db).await;
        g.apply_events(Vec::new(), vec![e]).await.map_err(crate::error::MongoError::from)
    }

    /// The `drop` event of a collection (written with the drop's own batch).
    pub(crate) fn drop_event(db: &str, coll: &str, uuid: Vec<u8>) -> EvSpec {
        EvSpec { uuid: Some(uuid), ..EvSpec::on(db, coll, "drop") }
    }

    /// The `create` event of a collection (written with the create's own batch).
    pub(crate) fn create_event(db: &str, coll: &str, uuid: Vec<u8>, options: &Document, view: bool) -> EvSpec {
        let mut desc = options.clone();
        if !view {
            let mut id_index = doc! { "v": 2, "key": { "_id": 1 }, "name": "_id_" };
            if let Some(c) = options.get("collation") {
                id_index.insert("collation", c.clone());
            }
            desc.insert("idIndex", id_index);
        }
        EvSpec { uuid: Some(uuid), op_desc: Some(desc), expanded_only: true, ..EvSpec::on(db, coll, "create") }
    }

    /// `create` of a collection: its options and the `_id` index.
    pub(crate) async fn publish_create(&self, db: &str, coll: &str, uuid: Vec<u8>, options: &Document, view: bool) -> crate::cmd::R<()> {
        self.publish_ddl(Self::create_event(db, coll, uuid, options, view), true).await
    }

    pub(crate) async fn publish_create_indexes(&self, db: &str, coll: &str, uuid: Vec<u8>, specs: &[Document]) -> crate::cmd::R<()> {
        let list: Vec<Bson> = specs.iter().map(|s| Bson::Document(index_spec_for_event(s, &["unique", "prepareUnique", "hidden"]))).collect();
        self.publish_ddl(EvSpec { uuid: Some(uuid), op_desc: Some(doc! { "indexes": list }), ..EvSpec::on(db, coll, "createIndexes") }, true).await
    }

    pub(crate) async fn publish_drop_index(&self, db: &str, coll: &str, uuid: Vec<u8>, spec: &Document) -> crate::cmd::R<()> {
        let list = vec![Bson::Document(index_spec_for_event(spec, &["unique", "prepareUnique"]))];
        self.publish_ddl(EvSpec { uuid: Some(uuid), op_desc: Some(doc! { "indexes": list }), ..EvSpec::on(db, coll, "dropIndexes") }, true).await
    }

    pub(crate) async fn publish_rename(&self, db: &str, from: &str, to_db: &str, to: &str, uuid: Vec<u8>, dropped_target: Option<Vec<u8>>) -> crate::cmd::R<()> {
        let mut desc = doc! { "to": { "db": to_db, "coll": to } };
        if let Some(u) = dropped_target {
            desc.insert("dropTarget", Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: u }));
        }
        self.publish_ddl(EvSpec { to: Some((to_db.to_string(), to.to_string())), uuid: Some(uuid), op_desc: Some(desc), ..EvSpec::on(db, from, "rename") }, false).await
    }

    pub(crate) async fn publish_drop(&self, db: &str, coll: &str, uuid: Vec<u8>) -> crate::cmd::R<()> {
        self.publish_ddl(EvSpec { uuid: Some(uuid), ..EvSpec::on(db, coll, "drop") }, false).await
    }
}
