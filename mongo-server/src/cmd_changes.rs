//! `aggregate` with `$changeStream` and the `getMore` that follows it.

use crate::changes::{Ev, Scope};
use crate::cmd::{int_arg, Server, R};
use crate::error::MongoError;
use bson::{doc, Bson, Document};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, PartialEq)]
pub enum FullDoc {
    Default,
    UpdateLookup,
}

pub struct ChangeCursor {
    pub scope: Scope,
    pub last: u64,
    pub rest: Vec<Bson>,
    pub full: FullDoc,
    pub ns: String,
    /// An `invalidate` event is due after the batch that holds the event that invalidated the stream.
    pub closed: bool,
}

impl Server {
    pub(crate) async fn open_change_stream(&self, db: &str, cmd: &Document, spec: &Document, rest: &[Bson]) -> R<Document> {
        let coll = match cmd.get("aggregate") {
            Some(Bson::String(c)) => Some(c.clone()),
            _ => None,
        };
        let cluster = matches!(spec.get("allChangesForCluster"), Some(Bson::Boolean(true)));
        if cluster && db != "admin" {
            return Err(MongoError::new(73, "InvalidNamespace", "$changeStream with 'allChangesForCluster:true' may only be opened on the 'admin' database, and with {aggregate: 1}".into()));
        }
        let scope = if cluster { Scope { db: None, coll: None } } else { Scope { db: Some(db.to_string()), coll: coll.clone() } };
        let full = match spec.get("fullDocument") {
            None => FullDoc::Default,
            Some(Bson::String(s)) if s == "default" => FullDoc::Default,
            Some(Bson::String(s)) if s == "updateLookup" => FullDoc::UpdateLookup,
            Some(Bson::String(s)) if s == "whenAvailable" || s == "required" => FullDoc::UpdateLookup,
            Some(other) => return Err(MongoError::new(9, "FailedToParse", format!("unrecognized value in enum fullDocument: {other}"))),
        };
        let log = &self.dbs.changes;
        let start = if let Some(Bson::Document(t)) = spec.get("resumeAfter").or_else(|| spec.get("startAfter")) {
            match t.get("_data") {
                Some(Bson::String(d)) => log.parse_token(d)?,
                _ => return Err(MongoError::new(40647, "Location40647", "Attempting to resume a change stream using 'resumeAfter' is not allowed from an invalid resume token.".into())),
            }
        } else if let Some(Bson::Timestamp(ts)) = spec.get("startAtOperationTime") {
            log.seq_at_time(*ts)
        } else {
            log.head()
        };
        let ns = match &coll {
            Some(c) => format!("{db}.{c}"),
            None => format!("{db}.$cmd.aggregate"),
        };
        let batch = cmd.get_document("cursor").ok().and_then(|c| int_arg(c.get("batchSize")));
        let cur = ChangeCursor { scope, last: start, rest: rest.to_vec(), full, ns: ns.clone(), closed: false };
        let id = self.store_change_cursor(cur);
        let _ = batch;
        Ok(doc! { "cursor": { "firstBatch": Vec::<Bson>::new(), "postBatchResumeToken": { "_data": log.token(start) }, "id": id, "ns": ns }, "ok": 1.0 })
    }

    pub(crate) fn store_change_cursor(&self, c: ChangeCursor) -> i64 {
        let mut m = self.change_cursors.lock().unwrap();
        loop {
            let id = (rand::random::<i64>() & i64::MAX) | (1 << 41);
            if !m.contains_key(&id) {
                m.insert(id, c);
                return id;
            }
        }
    }

    fn event_doc(&self, e: &Ev, lookup: Option<Option<Document>>) -> Document {
        let s = &e.spec;
        let mut d = Document::new();
        if let Some((n, lsid)) = &s.txn {
            d.insert("txnNumber", Bson::Int64(*n));
            d.insert("lsid", lsid.clone());
        }
        d.insert("_id", doc! { "_data": self.dbs.changes.token(e.seq) });
        d.insert("operationType", s.op);
        d.insert("clusterTime", Bson::Timestamp(e.time));
        d.insert("wallTime", e.wall);
        match s.op {
            "insert" | "replace" => {
                d.insert("fullDocument", s.full.clone().map(Bson::Document).unwrap_or(Bson::Null));
            }
            _ => {}
        }
        if let Some(l) = lookup {
            d.insert("fullDocument", l.map(Bson::Document).unwrap_or(Bson::Null));
        }
        let mut ns = doc! { "db": s.db.clone() };
        if let Some(c) = &s.coll {
            ns.insert("coll", c.clone());
        }
        d.insert("ns", ns);
        if let Some(k) = &s.doc_key {
            d.insert("documentKey", doc! { "_id": k.clone() });
        }
        if let Some(u) = &s.desc {
            d.insert("updateDescription", u.clone());
        }
        d
    }

    /// `getMore` on a change-stream cursor: wait up to `maxTimeMS` for events.
    pub(crate) async fn change_get_more(&self, id: i64, cmd: &Document) -> R<Document> {
        let mut cur = self.change_cursors.lock().unwrap().remove(&id).ok_or_else(|| MongoError::cursor_not_found(id))?;
        let batch = int_arg(cmd.get("batchSize")).map(|b| b.max(1) as usize).unwrap_or(1000);
        let wait = Duration::from_millis(int_arg(cmd.get("maxTimeMS")).unwrap_or(0).max(0) as u64);
        let deadline = Instant::now() + wait;
        let log = Arc::clone(&self.dbs.changes);
        let (events, last) = loop {
            let notified = log.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (evs, last) = log.read(&cur.scope, cur.last, batch);
            if !evs.is_empty() || Instant::now() >= deadline {
                break (evs, last);
            }
            let _ = tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), notified).await;
        };
        let mut docs = Vec::new();
        let mut closing = false;
        for e in &events {
            let lookup = if cur.full == FullDoc::UpdateLookup && e.spec.op == "update" {
                let mut g = self.dbs.lock(&e.spec.db).await;
                let coll = e.spec.coll.clone().unwrap_or_default();
                let found = match &e.spec.doc_key {
                    Some(k) => g.get_doc(&coll, k).await.map_err(MongoError::from)?,
                    None => None,
                };
                Some(found)
            } else {
                None
            };
            docs.push(self.event_doc(e, lookup));
            let invalidates = (e.spec.op == "drop" && cur.scope.coll.is_some() && cur.scope.coll == e.spec.coll) || (e.spec.op == "dropDatabase" && cur.scope.coll.is_some() && cur.scope.db.as_deref() == Some(e.spec.db.as_str()));
            if invalidates {
                let mut inv = doc! { "_id": { "_data": log.token(e.seq) }, "operationType": "invalidate", "clusterTime": Bson::Timestamp(e.time), "wallTime": e.wall };
                inv.insert("_id", doc! { "_data": log.token(e.seq) });
                docs.push(inv);
                closing = true;
                break;
            }
        }
        let n_scanned = if closing { events.last().map(|e| e.seq).unwrap_or(last) } else { last };
        let out = if cur.rest.is_empty() {
            docs
        } else {
            crate::agg::run_pipeline(docs, &cur.rest, &crate::agg::Foreign::new())?
        };
        cur.last = n_scanned;
        let token = log.token(n_scanned);
        let ns = cur.ns.clone();
        let next_id = if closing { 0 } else { id };
        if !closing {
            self.change_cursors.lock().unwrap().insert(id, cur);
        }
        Ok(doc! { "cursor": { "nextBatch": out.into_iter().map(Bson::Document).collect::<Vec<_>>(), "postBatchResumeToken": { "_data": token }, "id": next_id, "ns": ns }, "ok": 1.0 })
    }
}
