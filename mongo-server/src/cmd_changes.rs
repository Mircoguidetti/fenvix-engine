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
    WhenAvailable,
    Required,
}

/// `fullDocumentBeforeChange`.
#[derive(Clone, Copy, PartialEq)]
pub enum BeforeDoc {
    Off,
    WhenAvailable,
    Required,
}

pub struct ChangeCursor {
    /// Last time the cursor was created or used by a getMore (idle cursors are closed, see `cmd::CURSOR_TIMEOUT_SECS`).
    pub used: Instant,
    pub scope: Scope,
    pub last: u64,
    pub rest: Vec<Bson>,
    pub full: FullDoc,
    pub before: BeforeDoc,
    /// `showExpandedEvents`: DDL events and the collection UUID.
    pub expanded: bool,
    pub ns: String,
    /// An `invalidate` event is due after the batch that holds the event that invalidated the stream.
    pub closed: bool,
    /// A stream of one database (or collection) on its durable history (`chglog.rs`): `last` is a position of that
    /// history, `hist` its id (0: the database had no history when the stream opened; the first one seen is taken).
    pub durable: bool,
    pub hist: u64,
    /// `startAtOperationTime`: events older than it, read on the way to the first one at or after it, are dropped.
    pub min_time: Option<bson::Timestamp>,
}

impl ChangeCursor {
    /// The stream's position of an event.
    fn pos(&self, e: &Ev) -> u64 {
        if self.durable {
            e.dseq
        } else {
            e.seq
        }
    }
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
        for k in spec.keys() {
            if !matches!(k.as_str(), "fullDocument" | "fullDocumentBeforeChange" | "resumeAfter" | "startAfter" | "startAtOperationTime" | "allChangesForCluster" | "showExpandedEvents" | "showSystemEvents" | "showMigrationEvents" | "allowToRunOnConfigDB" | "allowToRunOnSystemNS") {
                return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field '$changeStream.{k}' is an unknown field.")));
            }
        }
        let enum_err = |field: &str, v: &Bson| MongoError::bad_value(format!("Enumeration value '{}' for field '$changeStream.{field}' is not a valid value.", match v { Bson::String(s) => s.clone(), o => o.to_string() }));
        let full = match spec.get("fullDocument") {
            None => FullDoc::Default,
            Some(Bson::String(s)) if s == "default" => FullDoc::Default,
            Some(Bson::String(s)) if s == "updateLookup" => FullDoc::UpdateLookup,
            Some(Bson::String(s)) if s == "whenAvailable" => FullDoc::WhenAvailable,
            Some(Bson::String(s)) if s == "required" => FullDoc::Required,
            Some(other) => return Err(enum_err("fullDocument", other)),
        };
        let before = match spec.get("fullDocumentBeforeChange") {
            None => BeforeDoc::Off,
            Some(Bson::String(s)) if s == "off" => BeforeDoc::Off,
            Some(Bson::String(s)) if s == "whenAvailable" => BeforeDoc::WhenAvailable,
            Some(Bson::String(s)) if s == "required" => BeforeDoc::Required,
            Some(other) => return Err(enum_err("fullDocumentBeforeChange", other)),
        };
        let expanded = matches!(spec.get("showExpandedEvents"), Some(Bson::Boolean(true)));
        if expanded {
            crate::changes::WANTS_UUID.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        // The database is opened first (if it exists), so the log knows whether its history before now is held here
        // (ChangeLog::mark_opened): a stream opened on a closed database is positioned after that open, not before it.
        if let Some(d) = &scope.db {
            if let Err(e) = self.dbs.read(d).await {
                return Err(MongoError::from(e));
            }
        }
        let log = &self.dbs.changes;
        let durable = scope.db.is_some() && crate::chglog::enabled();
        for f in ["resumeAfter", "startAfter"] {
            if let Some(v) = spec.get(f) {
                if !matches!(v, Bson::Document(_)) {
                    return Err(MongoError::type_mismatch(format!("BSON field '$changeStream.{f}' is the wrong type '{}', expected type 'object'", crate::update::type_name(v))));
                }
            }
        }
        let (mut hist, mut min_time) = (0u64, None);
        let start = if durable {
            let db = scope.db.clone().unwrap();
            let head = crate::chglog::head_of(&self.dbs, &db).await?;
            let (cur_hist, cur_next) = head.map(|h| (h.hist, h.next)).unwrap_or((0, 1));
            let lost = |m: String| {
                crate::changes::HISTORY_LOST.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                MongoError::new(286, "ChangeStreamHistoryLost", m)
            };
            if let Some(Bson::Document(t)) = spec.get("resumeAfter").or_else(|| spec.get("startAfter")) {
                let d = match t.get("_data") {
                    Some(Bson::String(d)) => d.clone(),
                    _ => return Err(MongoError::new(40647, "Location40647", format!("Bad resume token: _data of missing or of wrong type. Expected string, got {}", crate::update::fmt_doc(t).replace("{ ", "{").replace(" }", "}")))),
                };
                match crate::changes::ChangeLog::parse_token_db(&d) {
                    Some((th, pos, fp)) => {
                        if fp != crate::changes::name_fingerprint(&db) {
                            return Err(MongoError::new(280, "ChangeStreamFatalError", format!("cannot resume stream; the resume token was not found. {{_data: \"{d}\"}} (the token is from another database than '{db}')")));
                        }
                        if th == 0 {
                            // a token given before the database had any history: the start of the current one
                            hist = cur_hist;
                            0
                        } else if th != cur_hist {
                            return Err(lost(format!("Resume of change stream was not possible: the change history of database '{db}' the token belongs to is gone (the database was dropped, recreated or forked since)")));
                        } else {
                            crate::chglog::check_position(&self.dbs, &db, th, pos).await?;
                            hist = th;
                            pos
                        }
                    }
                    None => {
                        // a token of this process's in-memory log (`82`)
                        let (seq, who) = log.parse_token(&d)?;
                        log.check_same_db(&db, who, &d)?;
                        hist = cur_hist;
                        match log.legacy_to_db(&db, cur_hist, seq) {
                            Some(p) => p,
                            None => return Err(lost(format!("Resume of change stream was not possible: the resume token is not a position of the change history of database '{db}' held here"))),
                        }
                    }
                }
            } else if let Some(Bson::Timestamp(ts)) = spec.get("startAtOperationTime") {
                min_time = Some(*ts);
                hist = cur_hist;
                if cur_hist == 0 {
                    0
                } else {
                    match log.mem_position_at(&db, cur_hist, *ts) {
                        Some(p) => p,
                        None => match crate::chglog::position_at(&self.dbs, &db, *ts).await? {
                            Some((h, p)) => {
                                hist = h;
                                p
                            }
                            None => 0,
                        },
                    }
                }
            } else {
                hist = cur_hist;
                cur_next - 1
            }
        } else if let Some(Bson::Document(t)) = spec.get("resumeAfter").or_else(|| spec.get("startAfter")) {
            match t.get("_data") {
                Some(Bson::String(d)) => {
                    let (seq, who) = log.parse_token(d)?;
                    if let Some(db) = &scope.db {
                        log.check_born(db, seq)?;
                        log.check_same_db(db, who, d)?;
                    }
                    seq
                }
                _ => return Err(MongoError::new(40647, "Location40647", format!("Bad resume token: _data of missing or of wrong type. Expected string, got {}", crate::update::fmt_doc(t).replace("{ ", "{").replace(" }", "}")))),
            }
        } else if let Some(Bson::Timestamp(ts)) = spec.get("startAtOperationTime") {
            log.check_start_time(&scope, *ts)?;
            log.seq_at_time(*ts)
        } else {
            log.head()
        };
        let ns = match &coll {
            Some(c) => format!("{db}.{c}"),
            None => format!("{db}.$cmd.aggregate"),
        };
        let batch = cmd.get_document("cursor").ok().and_then(|c| int_arg(c.get("batchSize")));
        // `$changeStreamSplitLargeEvent` (last stage only) splits events over 16 MB; no event here is that large, so it passes events through
        let mut rest_stages = rest.to_vec();
        for (i, st) in rest.iter().enumerate() {
            if let Some(sp) = st.as_document().and_then(|d| d.get("$changeStreamSplitLargeEvent")) {
                if !matches!(sp, Bson::Document(d) if d.is_empty()) {
                    return Err(MongoError::new(7182800, "Location7182800", "$changeStreamSplitLargeEvent spec should be an empty object".into()));
                }
                if i + 1 != rest.len() {
                    return Err(MongoError::new(7182802, "Location7182802", "$changeStreamSplitLargeEvent must be the last stage in the pipeline".into()));
                }
                rest_stages.pop();
            }
        }
        let scope_db = scope.db.as_deref();
        let token = if durable { log.token_db(hist, start, scope_db.unwrap_or("")) } else { log.token(start, scope_db) };
        let cur = ChangeCursor { used: Instant::now(), scope, last: start, rest: rest_stages, full, before, expanded, ns: ns.clone(), closed: false, durable, hist, min_time };
        let id = self.store_change_cursor(cur);
        let _ = batch;
        Ok(doc! { "cursor": { "firstBatch": Vec::<Bson>::new(), "postBatchResumeToken": { "_data": token }, "id": id, "ns": ns }, "ok": 1.0 })
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

    /// The description of an event in the message of a `required` image that is missing.
    fn event_brief(e: &Ev) -> String {
        let s = &e.spec;
        let mut ns = format!("db: \"{}\"", s.db);
        if let Some(c) = &s.coll {
            ns.push_str(&format!(", coll: \"{c}\""));
        }
        format!("{{operationType: \"{}\", ns: {{{ns}}}, clusterTime: Timestamp({}, {})}}", s.op, e.time.time, e.time.increment)
    }

    /// The change event document, fields in the order of mongod. `lookup`: the `fullDocument` of an `updateLookup`.
    fn event_doc(&self, e: &Ev, cur: &ChangeCursor, lookup: Option<Option<Document>>, uuid: Option<Vec<u8>>) -> R<Document> {
        use crate::changes::EvSpec;
        let s: &EvSpec = &e.spec;
        let mut d = Document::new();
        let mut before_val: Option<Bson> = None;
        if matches!(s.op, "update" | "replace" | "delete") && cur.before != BeforeDoc::Off {
            match (&s.pre, cur.before) {
                (Some(p), _) => before_val = Some(Bson::Document(p.clone())),
                (None, BeforeDoc::Required) => {
                    return Err(MongoError::new(47, "NoMatchingDocument", format!("Executor error during getMore :: caused by :: Change stream was configured to require a pre-image for all update, delete and replace events, but the pre-image was not found for event: {}", Self::event_brief(e))));
                }
                (None, _) => before_val = Some(Bson::Null),
            }
        }
        if let Some((db, coll)) = &s.to {
            d.insert("to", doc! { "db": db.clone(), "coll": coll.clone() });
        }
        if let Some((n, lsid)) = &s.txn {
            d.insert("txnNumber", Bson::Int64(*n));
            d.insert("lsid", lsid.clone());
        }
        d.insert("_id", doc! { "_data": self.tok(cur, cur.pos(e)) });
        d.insert("operationType", s.op);
        d.insert("clusterTime", Bson::Timestamp(e.time));
        if cur.expanded {
            if let Some(u) = s.uuid.clone().or(uuid) {
                d.insert("collectionUUID", Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: u }));
            }
        }
        d.insert("wallTime", e.wall);
        match s.op {
            "insert" | "replace" => {
                d.insert("fullDocument", s.full.clone().map(Bson::Document).unwrap_or(Bson::Null));
            }
            "update" => match cur.full {
                FullDoc::Default => {}
                FullDoc::UpdateLookup => {
                    d.insert("fullDocument", lookup.flatten().map(Bson::Document).unwrap_or(Bson::Null));
                }
                FullDoc::WhenAvailable => {
                    d.insert("fullDocument", s.post.clone().map(Bson::Document).unwrap_or(Bson::Null));
                }
                FullDoc::Required => match &s.post {
                    Some(p) => {
                        d.insert("fullDocument", p.clone());
                    }
                    None => {
                        return Err(MongoError::new(47, "NoMatchingDocument", format!("Executor error during getMore :: caused by :: Change stream was configured to require a post-image for all update events, but the post-image was not found for event: {}", Self::event_brief(e))));
                    }
                },
            },
            _ => {}
        }
        let mut ns = doc! { "db": s.db.clone() };
        if let Some(c) = &s.coll {
            ns.insert("coll", c.clone());
        }
        if s.op != "invalidate" {
            d.insert("ns", ns);
        }
        if let Some(k) = &s.doc_key {
            // a customer-keyed collection: the customer (and time) fields first, then `_id`, as mongod's documentKey
            // of a sharded collection
            let mut dk = s.cust.as_ref().map(|(f, _)| f.clone()).unwrap_or_default();
            dk.insert("_id", k.clone());
            d.insert("documentKey", dk);
        }
        if let Some(u) = &s.desc {
            let mut u = u.clone();
            if cur.expanded {
                u.insert("disambiguatedPaths", doc! {});
            }
            d.insert("updateDescription", u);
        }
        if cur.expanded {
            if let Some(o) = &s.op_desc {
                d.insert("operationDescription", o.clone());
            }
            if let Some(o) = &s.state_before {
                d.insert("stateBeforeChange", o.clone());
            }
        }
        if let Some(b) = before_val {
            d.insert("fullDocumentBeforeChange", b);
        }
        Ok(d)
    }

    /// The resume token of position `pos` of the stream.
    fn tok(&self, cur: &ChangeCursor, pos: u64) -> String {
        if cur.durable {
            self.dbs.changes.token_db(cur.hist, pos, cur.scope.db.as_deref().unwrap_or(""))
        } else {
            self.dbs.changes.token(pos, cur.scope.db.as_deref())
        }
    }

    /// Events of a stream on a durable history past its position: from memory when it holds the position, else from the
    /// store (segments, then the tail). Returns them with the highest position examined.
    async fn read_durable_stream(&self, cur: &mut ChangeCursor, batch: usize) -> R<(Vec<Arc<Ev>>, u64)> {
        use crate::changes::DbRead;
        let log = &self.dbs.changes;
        let db = cur.scope.db.clone().unwrap_or_default();
        let changed = || {
            crate::changes::HISTORY_LOST.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            MongoError::new(286, "ChangeStreamHistoryLost", format!("the change history of database '{db}' this stream follows is gone (the database was dropped, recreated or forked); resume is not possible"))
        };
        if cur.hist == 0 {
            // the database had no history when the stream opened: the first one is its start
            match log.history_of(&db) {
                Some((h, _)) => cur.hist = h,
                None => match crate::chglog::head_of(&self.dbs, &db).await? {
                    Some(h) => cur.hist = h.hist,
                    None => return Ok((Vec::new(), cur.last)),
                },
            }
            if cur.last != 0 {
                return Err(changed());
            }
        }
        match log.read_db(&cur.scope, cur.hist, cur.last, batch) {
            DbRead::Mem(evs, last) => Ok((evs, last)),
            DbRead::Changed => Err(changed()),
            DbRead::Durable => match crate::chglog::read(&self.dbs, &db, cur.hist, cur.last, batch).await? {
                crate::chglog::Durable::Absent => Ok((Vec::new(), cur.last)),
                crate::chglog::Durable::Changed => Err(changed()),
                crate::chglog::Durable::Rows(rows) => {
                    let last = rows.last().map(|r| r.pos).unwrap_or(cur.last);
                    let hist = cur.hist;
                    let evs = rows
                        .into_iter()
                        .filter(|r| cur.scope.sees(&r.spec) && !log.erased(&db, r.pos, &r.spec))
                        .map(|r| Arc::new(Ev { gate: None, seq: 0, time: r.time, wall: r.wall, spec: r.spec, dseq: r.pos, hist }))
                        .collect();
                    Ok((evs, last))
                }
            },
        }
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
            let (evs, last) = if cur.durable { self.read_durable_stream(&mut cur, batch).await? } else { log.read(&cur.scope, cur.last, batch)? };
            let min_time = cur.min_time;
            let evs: Vec<Arc<Ev>> = evs.into_iter().filter(|e| (cur.expanded || !e.spec.expanded_only) && min_time.map_or(true, |t| (e.time.time, e.time.increment) >= (t.time, t.increment))).collect();
            if !evs.is_empty() || Instant::now() >= deadline {
                break (evs, last);
            }
            // (only events the stream cannot see were read: go on from there, at once when it moved)
            let moved = last > cur.last;
            cur.last = cur.last.max(last);
            if moved && cur.durable {
                continue;
            }
            let _ = tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), notified).await;
        };
        let mut out: Vec<Document> = Vec::new();
        let mut used = 0;
        let mut closing = false;
        // the events wholly in the batch so far: the next getMore starts after them
        let mut consumed = cur.last;
        let mut full = false;
        // mongod's getMore of a change stream also keeps room for the batch's resume token: the token document's
        // content (MEASURED on mongod 7.0.34 with research/fix-query-limits/cs_edge.py: two events fit up to 16 MiB - 238
        // bytes, 57 + 178 + 3, with a token document of 183 bytes)
        let token_room = crate::query::bson_size(&doc! { "_data": self.tok(&cur, cur.last) }).saturating_sub(5);
        let fixed = crate::cmd::NEXT_BATCH_FIXED + token_room;
        for e in &events {
            let lookup = if cur.full == FullDoc::UpdateLookup && e.spec.op == "update" {
                let mut g = self.dbs.lock(&e.spec.db).await;
                let coll = e.spec.coll.clone().unwrap_or_default();
                // by the whole document key: in a customer-keyed collection `_id` is unique per customer only, so the
                // lookup takes the event's customer, never the first one holding the `_id` (after a move to another
                // customer the document is not under its key any more, and the lookup finds none, as on mongod)
                let found = match (&e.spec.doc_key, &e.spec.cust) {
                    (Some(k), Some((_, cust))) => g.get_doc_of(&coll, k, cust).await.map_err(MongoError::from)?,
                    (Some(k), None) => g.get_doc(&coll, k).await.map_err(MongoError::from)?,
                    (None, _) => None,
                };
                Some(found)
            } else {
                None
            };
            let uuid = if cur.expanded && e.spec.uuid.is_none() && matches!(e.spec.op, "insert" | "update" | "replace" | "delete") {
                let mut g = self.dbs.lock(&e.spec.db).await;
                match &e.spec.coll {
                    Some(c) if g.collections().await.map_err(MongoError::from)?.iter().any(|n| n == c) => Some(g.collection_uuid(c).await.map_err(MongoError::from)?),
                    _ => None,
                }
            } else {
                None
            };
            let mut docs = vec![self.event_doc(e, &cur, lookup, uuid)?];
            let same_coll = cur.scope.coll.is_some() && cur.scope.coll == e.spec.coll;
            let invalidates = (matches!(e.spec.op, "drop" | "rename") && same_coll)
                || (e.spec.op == "dropDatabase" && cur.scope.db.as_deref() == Some(e.spec.db.as_str()));
            if invalidates {
                let inv = doc! { "_id": { "_data": self.tok(&cur, cur.pos(e)) }, "operationType": "invalidate", "clusterTime": Bson::Timestamp(e.time), "wallTime": e.wall };
                docs.push(inv);
                closing = true;
            }
            // (the stages after `$changeStream` take one document at a time, so the event's own documents are its output)
            let docs = if cur.rest.is_empty() { docs } else { crate::agg::run_pipeline(docs, &cur.rest, &crate::agg::Foreign::new())? };
            // a batch stops at 16 MiB as mongod's getMore (`crate::cmd::BATCH_BYTES`): an event whose output does not fit
            // waits for the next getMore, which starts with it; the first event of a batch always goes in
            let (mut u, mut n) = (used, out.len());
            let mut fits = true;
            for d in &docs {
                let size = crate::query::bson_size(d);
                if !out.is_empty() && !crate::cmd::batch_fits(u, n, size, fixed) {
                    fits = false;
                    break;
                }
                crate::cmd::batch_add(&mut u, n, size);
                n += 1;
            }
            if !fits {
                closing = false;
                full = true;
                break;
            }
            used = u;
            out.extend(docs);
            consumed = cur.pos(e);
            if closing {
                break;
            }
        }
        let n_scanned = if closing || full { consumed } else { last };
        cur.last = n_scanned;
        let token = self.tok(&cur, n_scanned);
        let ns = cur.ns.clone();
        let next_id = if closing { 0 } else { id };
        if !closing {
            cur.used = Instant::now();
            self.change_cursors.lock().unwrap().insert(id, cur);
        }
        Ok(doc! { "cursor": { "nextBatch": out.into_iter().map(Bson::Document).collect::<Vec<_>>(), "postBatchResumeToken": { "_data": token }, "id": next_id, "ns": ns }, "ok": 1.0 })
    }
}
