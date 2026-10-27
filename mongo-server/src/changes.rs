//! Change streams. Writers publish [`EvSpec`]s into an in-memory, process-wide [`ChangeLog`] after a batch is
//! applied; `$changeStream` cursors read events past their last position and wait (up to the `getMore`
//! `maxTimeMS`) when there are none. There is no oplog: events are lost on restart, a resume token from before
//! the start of this process (or older than the retained window) fails with `ChangeStreamHistoryLost`.

use crate::error::MongoError;
use bson::{doc, Bson, Document, Timestamp};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

const RETAINED: usize = 100_000;

tokio::task_local! {
    /// The cluster time of the last event that the running command published (its operation time).
    pub static OP_TIME: std::cell::Cell<Option<(u32, u32)>>;
}

/// What a writer reports.
#[derive(Clone, Debug)]
pub struct EvSpec {
    pub db: String,
    pub coll: Option<String>,
    pub op: &'static str,
    pub doc_key: Option<Bson>,
    pub full: Option<Document>,
    pub desc: Option<Document>,
    /// Transaction number and session id, for events of a committed transaction.
    pub txn: Option<(i64, Document)>,
}

#[derive(Debug)]
pub struct Ev {
    pub seq: u64,
    pub time: Timestamp,
    pub wall: bson::DateTime,
    pub spec: EvSpec,
}

struct Inner {
    events: VecDeque<Arc<Ev>>,
    next_seq: u64,
    last_time: (u32, u32),
}

pub struct ChangeLog {
    inner: Mutex<Inner>,
    pub notify: Notify,
    nonce: u32,
    /// The cluster time (seconds) before the first write.
    boot: u32,
}

impl Default for ChangeLog {
    fn default() -> Self {
        ChangeLog { inner: Mutex::new(Inner { events: VecDeque::new(), next_seq: 1, last_time: (0, 0) }), notify: Notify::new(), nonce: rand::random(), boot: (bson::DateTime::now().timestamp_millis() / 1000) as u32 }
    }
}

#[derive(Clone, Debug)]
pub struct Scope {
    pub db: Option<String>,
    pub coll: Option<String>,
}

impl Scope {
    fn sees(&self, s: &EvSpec) -> bool {
        match (&self.db, &self.coll) {
            (None, _) => true,
            (Some(d), None) => &s.db == d,
            (Some(d), Some(c)) => &s.db == d && s.coll.as_deref() == Some(c.as_str()),
        }
    }
}

impl ChangeLog {
    pub fn publish(&self, specs: Vec<EvSpec>) {
        if specs.is_empty() {
            return;
        }
        {
            let mut g = self.inner.lock().unwrap();
            let now = bson::DateTime::now();
            let secs = (now.timestamp_millis() / 1000) as u32;
            for s in specs {
                let (ts, inc) = if g.last_time.0 == secs { (secs, g.last_time.1 + 1) } else { (secs.max(g.last_time.0), 1) };
                g.last_time = (ts, inc);
                let _ = OP_TIME.try_with(|c| c.set(Some((ts, inc))));
                let seq = g.next_seq;
                g.next_seq += 1;
                g.events.push_back(Arc::new(Ev { seq, time: Timestamp { time: ts, increment: inc }, wall: now, spec: s }));
                if g.events.len() > RETAINED {
                    g.events.pop_front();
                }
            }
        }
        self.notify.notify_waiters();
    }

    /// The cluster time now: the time of the last write, or the wall clock when that is later (a replica set keeps the
    /// time moving with a no-op write of its own).
    pub fn cluster_now(&self) -> Timestamp {
        let g = self.inner.lock().unwrap();
        let (t, i) = if g.last_time.0 == 0 { (self.boot, 1) } else { g.last_time };
        Timestamp { time: t, increment: i }
    }

    /// A new cluster time for a write that published no event.
    pub fn tick(&self) -> Timestamp {
        let mut g = self.inner.lock().unwrap();
        let secs = (bson::DateTime::now().timestamp_millis() / 1000) as u32;
        let (ts, inc) = if g.last_time.0 >= secs { (g.last_time.0, g.last_time.1 + 1) } else { (secs, 1) };
        g.last_time = (ts, inc);
        Timestamp { time: ts, increment: inc }
    }

    /// Move the cluster time forward to one that a client has seen.
    pub fn advance(&self, ts: Timestamp) {
        let mut g = self.inner.lock().unwrap();
        let secs = (bson::DateTime::now().timestamp_millis() / 1000) as u32;
        if ts.time <= secs.saturating_add(365 * 24 * 3600) && (ts.time, ts.increment) > g.last_time {
            g.last_time = (ts.time, ts.increment);
        }
    }

    pub fn token(&self, seq: u64) -> String {
        format!("82{:08X}{:016X}", self.nonce, seq)
    }

    /// Sequence number encoded in a resume token of this process.
    pub fn parse_token(&self, data: &str) -> Result<u64, MongoError> {
        let lost = || MongoError::new(286, "ChangeStreamHistoryLost", "Resume of change stream was not possible, as the resume point may no longer be in the oplog.".into());
        if data.len() != 26 || !data.starts_with("82") {
            return Err(MongoError::new(260, "InvalidResumeToken", "Attempting to resume a change stream using 'resumeAfter' is not allowed from an invalid resume token.".into()));
        }
        let nonce = u32::from_str_radix(&data[2..10], 16).map_err(|_| lost())?;
        let seq = u64::from_str_radix(&data[10..], 16).map_err(|_| lost())?;
        if nonce != self.nonce {
            return Err(lost());
        }
        let g = self.inner.lock().unwrap();
        let oldest = g.events.front().map(|e| e.seq).unwrap_or(g.next_seq);
        if seq + 1 < oldest && seq != 0 {
            return Err(lost());
        }
        Ok(seq)
    }

    /// Position of "now": events published later have a larger sequence number.
    pub fn head(&self) -> u64 {
        self.inner.lock().unwrap().next_seq - 1
    }

    /// First position at or after the operation time.
    pub fn seq_at_time(&self, ts: Timestamp) -> u64 {
        let g = self.inner.lock().unwrap();
        for e in g.events.iter() {
            if (e.time.time, e.time.increment) >= (ts.time, ts.increment) {
                return e.seq - 1;
            }
        }
        g.next_seq - 1
    }

    /// Events past `after` that `scope` sees (at most `limit`), and the highest sequence number examined.
    pub fn read(&self, scope: &Scope, after: u64, limit: usize) -> (Vec<Arc<Ev>>, u64) {
        let g = self.inner.lock().unwrap();
        let mut out = Vec::new();
        let mut last = after;
        for e in g.events.iter().filter(|e| e.seq > after) {
            if out.len() >= limit {
                break;
            }
            last = e.seq;
            if scope.sees(&e.spec) {
                out.push(Arc::clone(e));
            }
        }
        (out, last)
    }
}

/// `updateDescription` of an operator update, derived from the update spec and the documents before and after.
pub fn update_description(old: &Document, new: &Document, update: &Document) -> Document {
    use crate::query::path_values;
    let mut updated = Document::new();
    let mut removed: Vec<Bson> = Vec::new();
    let exists = |d: &Document, p: &str| !path_values(d, p).is_empty();
    let value = |d: &Document, p: &str| path_values(d, p).first().map(|v| (*v).clone());
    // The reported path of `p`: its first ancestor that did not exist before (or `p` itself).
    let reported = |p: &str| -> String {
        let parts: Vec<&str> = p.split('.').collect();
        for i in 1..=parts.len() {
            let prefix = parts[..i].join(".");
            if !exists(old, &prefix) {
                return prefix;
            }
        }
        p.to_string()
    };
    let mut note = |p: &str, updated: &mut Document, removed: &mut Vec<Bson>| {
        let rp = reported(p);
        match (value(old, &rp), value(new, &rp)) {
            (_, Some(nv)) => {
                if value(old, &rp).map(|ov| ov != nv).unwrap_or(true) {
                    updated.insert(rp, nv);
                }
            }
            (Some(_), None) => removed.push(Bson::String(rp)),
            (None, None) => {}
        }
    };
    for (op, spec) in update {
        let Bson::Document(fields) = spec else { continue };
        match op.as_str() {
            "$set" | "$inc" | "$mul" | "$min" | "$max" | "$currentDate" | "$bit" | "$setOnInsert" => {
                for (p, _) in fields {
                    note(p, &mut updated, &mut removed);
                }
            }
            "$unset" => {
                for (p, _) in fields {
                    if exists(old, p) && !exists(new, p) {
                        removed.push(Bson::String(p.clone()));
                    }
                }
            }
            "$rename" => {
                for (from, to) in fields {
                    if let Bson::String(to) = to {
                        note(to, &mut updated, &mut removed);
                        if exists(old, from) && !exists(new, from) {
                            removed.push(Bson::String(from.clone()));
                        }
                    }
                }
            }
            "$push" => {
                for (p, _) in fields {
                    match (value(old, p), value(new, p)) {
                        (Some(Bson::Array(o)), Some(Bson::Array(n))) if n.len() > o.len() && n[..o.len()] == o[..] => {
                            for (i, v) in n.iter().enumerate().skip(o.len()) {
                                updated.insert(format!("{p}.{i}"), v.clone());
                            }
                        }
                        _ => note(p, &mut updated, &mut removed),
                    }
                }
            }
            "$addToSet" | "$pull" | "$pullAll" | "$pop" => {
                for (p, _) in fields {
                    if let Some(nv) = value(new, p) {
                        if value(old, p).map(|o| o != nv).unwrap_or(true) {
                            updated.insert(p.clone(), nv);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    doc! { "updatedFields": updated, "removedFields": removed, "truncatedArrays": Vec::<Bson>::new() }
}

pub fn arc_default() -> Arc<ChangeLog> {
    Arc::new(ChangeLog::default())
}
