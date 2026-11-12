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
/// Memory the retained events may take (estimated), `--change-log-max-mb`. The log keeps whole documents (inserts carry the
/// full document): with only the count limit, 100 000 events of 1 KB documents held ~400 MB, and every write grew the
/// process by ~4 KB until the count limit was reached. Older events are dropped first; a resume token older than the
/// oldest event left fails with `ChangeStreamHistoryLost`, as before.
pub static MAX_BYTES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(32 << 20);

/// Rough in-memory size of an event: the BSON size of its documents times 3 (a parsed `Document` keeps every key and value
/// in its own allocation) plus a fixed part.
fn est_size(s: &EvSpec) -> usize {
    let doc = |d: &Option<Document>| d.as_ref().map_or(0, |d| d.to_vec().map(|v| v.len()).unwrap_or(256));
    let key = s.doc_key.as_ref().map_or(0, |k| bson::doc! { "k": k.clone() }.to_vec().map(|v| v.len()).unwrap_or(64));
    let cust = s.cust.as_ref().map_or(0, |(f, v)| f.to_vec().map(|x| x.len()).unwrap_or(64) + bson::doc! { "k": v.clone() }.to_vec().map(|x| x.len()).unwrap_or(64));
    384 + 3 * (doc(&s.full) + doc(&s.desc) + doc(&s.pre) + doc(&s.post) + doc(&s.op_desc) + doc(&s.state_before) + key + cust)
}

/// Change streams failed with ChangeStreamHistoryLost because they fell behind the log (tenantStats `changeStreamsLost`).
pub static HISTORY_LOST: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Set once a stream with `showExpandedEvents` was opened: data events then carry the UUID of their collection from the start.
pub static WANTS_UUID: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

tokio::task_local! {
    /// The cluster time of the last event that the running command published (its operation time).
    pub static OP_TIME: std::cell::Cell<Option<(u32, u32)>>;
}

/// What a writer reports.
#[derive(Clone, Debug, Default)]
pub struct EvSpec {
    pub db: String,
    pub coll: Option<String>,
    pub op: &'static str,
    pub doc_key: Option<Bson>,
    /// A customer-keyed collection (`custkey.rs`): the customer fields of `documentKey` (before `_id`, as mongod
    /// writes a shard key) and the customer value, which with `_id` names one document (`_id` is unique per customer).
    pub cust: Option<(Document, Bson)>,
    pub full: Option<Document>,
    pub desc: Option<Document>,
    /// Transaction number and session id, for events of a committed transaction.
    pub txn: Option<(i64, Document)>,
    /// `rename`: the new namespace.
    pub to: Option<(String, String)>,
    /// The UUID of the collection (shown by streams with `showExpandedEvents`); looked up when the event is read if not given.
    pub uuid: Option<Vec<u8>>,
    /// `operationDescription` of the expanded events (create, createIndexes, dropIndexes, modify, rename).
    pub op_desc: Option<Document>,
    /// `stateBeforeChange` of `modify`.
    pub state_before: Option<Document>,
    /// The document before and after a change, kept when the collection has `changeStreamPreAndPostImages` enabled.
    pub pre: Option<Document>,
    pub post: Option<Document>,
    /// Whether the collection kept images when the event happened (a missing image is then `null`, or an error for `required`).
    pub images: bool,
    /// Only streams with `showExpandedEvents` see the event.
    pub expanded_only: bool,
}

impl EvSpec {
    /// An event of a data change or a DDL operation on `db.coll`.
    pub fn on(db: &str, coll: &str, op: &'static str) -> EvSpec {
        EvSpec { db: db.to_string(), coll: Some(coll.to_string()), op, ..Default::default() }
    }
}

#[derive(Debug)]
pub struct Ev {
    /// With the log on: the batch the event comes from, until its log record is confirmed (see
    /// `lifecycle::TRACK_VISIBLE`). Readers stop at the first event whose batch is not confirmed yet, and skip one whose
    /// batch can no longer be confirmed (ownership lost: like a rollback, the change may never have happened).
    pub gate: Option<tenant_bench::lifecycle::VisGate>,
    pub seq: u64,
    pub time: Timestamp,
    pub wall: bson::DateTime,
    pub spec: EvSpec,
}

struct Inner {
    events: VecDeque<Arc<Ev>>,
    /// Estimated size of each event in `events` (same order), and their sum.
    sizes: VecDeque<usize>,
    bytes: usize,
    next_seq: u64,
    last_time: (u32, u32),
    /// The highest position pushed out of the log by its bounds: a resume token at or below it is lost. (Events an
    /// erasure removes do not move it: a stream resumed from before an erasure goes on, without the erased events.)
    trimmed: u64,
    /// The cluster time of the event at `trimmed`.
    trimmed_time: (u32, u32),
    /// Databases opened by this process with a history it cannot vouch for (the first open after the process started,
    /// a takeover, a reopen after another process may have owned it): the log position of that open, and the cluster
    /// second it happened in. Events written before it (by another process) are not in this log, so a stream on the
    /// database positioned before it, or a start time at or before that second, fails with ChangeStreamHistoryLost.
    opened: std::collections::HashMap<String, (u64, u32)>,
}

pub struct ChangeLog {
    inner: Mutex<Inner>,
    pub notify: Notify,
    nonce: u32,
    /// The cluster time (seconds) before the first write.
    boot: u32,
    /// Databases forked in this process, with the position of the log when they were born: an earlier resume token
    /// (the source's, or any older one) is not a point in their history.
    born: Mutex<std::collections::HashMap<String, u64>>,
}

impl Default for ChangeLog {
    fn default() -> Self {
        ChangeLog { inner: Mutex::new(Inner { events: VecDeque::new(), sizes: VecDeque::new(), bytes: 0, next_seq: 1, last_time: (0, 0), trimmed: 0, trimmed_time: (0, 0), opened: Default::default() }), notify: Notify::new(), nonce: rand::random(), boot: (bson::DateTime::now().timestamp_millis() / 1000) as u32, born: Mutex::default() }
    }
}

/// The database a resume token belongs to.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TokenDb {
    /// A stream of the whole cluster (`allChangesForCluster`).
    Cluster,
    /// The fingerprint of a database and branch ([`ChangeLog::fingerprint`]).
    Db(u64),
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
                let size = est_size(&s);
                let gate = tenant_bench::lifecycle::visibility_gate(&s.db);
                g.events.push_back(Arc::new(Ev { gate, seq, time: Timestamp { time: ts, increment: inc }, wall: now, spec: s }));
                g.sizes.push_back(size);
                g.bytes += size;
                let max = MAX_BYTES.load(std::sync::atomic::Ordering::Relaxed);
                while g.events.len() > 1 && (g.events.len() > RETAINED || g.bytes > max) {
                    if let Some(e) = g.events.pop_front() {
                        if e.seq > g.trimmed {
                            g.trimmed = e.seq;
                            g.trimmed_time = (e.time.time, e.time.increment);
                        }
                    }
                    let s = g.sizes.pop_front().unwrap_or(0);
                    g.bytes -= s;
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

    /// Resume token of position `seq` on a stream of database `db` (`None`: a stream of the whole cluster). Layout (hex):
    /// `82`, the process nonce (8), the position (16), then the identity of the database: `01` and a 64-bit fingerprint
    /// of its name and of its branch (16), or `00` and zeros for the whole cluster. Tokens sort by position, as before.
    /// Tokens of the old layout (`82`, nonce, position: 26 characters, no database) are still read.
    pub fn token(&self, seq: u64, db: Option<&str>) -> String {
        match db {
            Some(d) => format!("82{:08X}{:016X}01{:016X}", self.nonce, seq, self.fingerprint(d)),
            None => format!("82{:08X}{:016X}00{:016X}", self.nonce, seq, 0u64),
        }
    }

    /// Fingerprint of a database: its name and, for a fork made in this process, the position where it was born (a
    /// fork reusing a name, or a fork of a fork, is another branch). FNV-1a, 64 bits: stable across builds and processes.
    pub fn fingerprint(&self, db: &str) -> u64 {
        let born = self.born.lock().unwrap().get(db).copied();
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        let mut eat = |b: u8| {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        };
        for b in db.bytes() {
            eat(b);
        }
        eat(0);
        if let Some(at) = born {
            eat(1);
            for b in at.to_le_bytes() {
                eat(b);
            }
        }
        h
    }

    /// Position and database identity encoded in a resume token of this process.
    pub fn parse_token(&self, data: &str) -> Result<(u64, TokenDb), MongoError> {
        let lost = || MongoError::new(286, "ChangeStreamHistoryLost", "Resume of change stream was not possible, as the resume point may no longer be in the oplog.".into());
        let invalid = || MongoError::new(260, "InvalidResumeToken", "Attempting to resume a change stream using 'resumeAfter' is not allowed from an invalid resume token.".into());
        if !(data.len() == 26 || data.len() == 44) || !data.starts_with("82") || !data.is_ascii() {
            return Err(invalid());
        }
        let nonce = u32::from_str_radix(&data[2..10], 16).map_err(|_| lost())?;
        let seq = u64::from_str_radix(&data[10..26], 16).map_err(|_| lost())?;
        // A token of the old layout (26 characters, no database in it) was only ever given by a process of the version
        // before this one, whose nonce is gone after the restart that installs this one: no such token is valid any more.
        // Reading it would let a token of another database, cut to its first 26 characters, pass (reviewer of M2, R1).
        if data.len() == 26 {
            return Err(lost());
        }
        let fp = u64::from_str_radix(&data[28..44], 16).map_err(|_| invalid())?;
        let who = match &data[26..28] {
            "00" if fp == 0 => TokenDb::Cluster,
            "01" => TokenDb::Db(fp),
            _ => return Err(invalid()),
        };
        if nonce != self.nonce {
            return Err(lost());
        }
        let g = self.inner.lock().unwrap();
        let oldest = g.trimmed + 1;
        if seq + 1 < oldest && seq != 0 {
            return Err(lost());
        }
        Ok((seq, who))
    }

    /// A stream on `db` resuming from a token that says `who`: a token of another database (or of another branch with
    /// the same name) is not a point of this stream. MongoDB answers such a token with ChangeStreamFatalError (280):
    /// the resumed event is not in the stream. Tokens of the whole cluster (a position in every database's history)
    /// pass: a hand-edited token whose database part is replaced by the cluster's is not told apart (a known limit).
    pub fn check_same_db(&self, db: &str, who: TokenDb, data: &str) -> Result<(), MongoError> {
        match who {
            TokenDb::Db(fp) if fp != self.fingerprint(db) => Err(MongoError::new(280, "ChangeStreamFatalError", format!("cannot resume stream; the resume token was not found. {{_data: \"{data}\"}} (the token is from another database or branch than '{db}')"))),
            _ => Ok(()),
        }
    }

    /// This process just opened `db` with a history it cannot vouch for ([`Inner::opened`]). The open takes a log position
    /// of its own (no event has it), so a stream positioned at or after it started after the open. The cluster clock is
    /// moved past the current second, so every later event of this process has a later time than any event another
    /// process wrote before the open (up to a clock skew of one second between processes).
    pub fn mark_opened(&self, db: &str) {
        let mut g = self.inner.lock().unwrap();
        let secs = (bson::DateTime::now().timestamp_millis() / 1000) as u32;
        let floor = secs.max(self.boot);
        if g.last_time < (floor + 1, 0) {
            g.last_time = (floor + 1, 0);
        }
        let at = g.next_seq;
        g.next_seq += 1;
        g.opened.insert(db.to_string(), (at, floor));
    }

    /// A stream of `scope` starting at cluster time `ts`: refused when the log cannot hold every event since then:
    /// the time is from before this process started, or at or before an open of the database (or, for a stream of the
    /// whole cluster, of any database) whose earlier history is not in this log.
    pub fn check_start_time(&self, scope: &Scope, ts: Timestamp) -> Result<(), MongoError> {
        let g = self.inner.lock().unwrap();
        let floor = match &scope.db {
            Some(d) => g.opened.get(d).map(|o| o.1),
            None => g.opened.values().map(|o| o.1).max(),
        };
        let lost = ts.time < self.boot || floor.is_some_and(|f| ts.time <= f);
        if lost {
            HISTORY_LOST.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(MongoError::new(286, "ChangeStreamHistoryLost", format!("Resume of change stream was not possible: startAtOperationTime {{{}, {}}} is older than this server's change history for {} (events written before this process started or took the database over are not held here)", ts.time, ts.increment, scope.db.as_deref().unwrap_or("the cluster"))));
        }
        Ok(())
    }

    /// A fork of another database was just made under this name.
    pub fn mark_born(&self, db: &str) {
        let at = self.head();
        self.born.lock().unwrap().insert(db.to_string(), at);
    }

    /// A stream on `db` resuming after `seq`: a position from before the database was forked cannot be resumed there.
    pub fn check_born(&self, db: &str, seq: u64) -> Result<(), MongoError> {
        match self.born.lock().unwrap().get(db) {
            Some(&at) if seq < at => Err(MongoError::new(286, "ChangeStreamHistoryLost", format!("Resume of change stream was not possible: the resume token is from before database '{db}' was forked from another database."))),
            _ => Ok(()),
        }
    }

    /// An erasure of customer `value` of `db.coll` (compaction 2 step 7b, `fx/erase.rs`): every event of that customer
    /// leaves the log (inserts carry whole documents, deletes and updates its key), so no stream, resumed or not, returns
    /// the erased customer. A stream resumed from before the erasure goes on with the other events. MongoDB itself keeps
    /// removed documents in the oplog until it rolls over; an erasure here does not wait for that. Returns the number
    /// of events removed.
    pub fn forget_customer(&self, db: &str, coll: &str, field: &str, value: &Bson) -> usize {
        // compared by the customer key's encoding (1 and 1.0 are one customer), on the event's customer and on the
        // customer of any document the event carries (a replace that moved a document to this customer)
        let want = crate::custkey::comp(value);
        let of = |d: &Option<Document>| d.as_ref().is_some_and(|d| d.get(field).is_some() && crate::custkey::comp(&crate::custkey::value(field, d)) == want);
        let mut g = self.inner.lock().unwrap();
        let g = &mut *g;
        let mut kept_e = VecDeque::with_capacity(g.events.len());
        let mut kept_s = VecDeque::with_capacity(g.sizes.len());
        let mut n = 0;
        for (e, sz) in g.events.drain(..).zip(g.sizes.drain(..)) {
            let s = &e.spec;
            let hit = s.db == db && s.coll.as_deref() == Some(coll) && (s.cust.as_ref().is_some_and(|(_, v)| crate::custkey::comp(v) == want) || of(&s.full) || of(&s.pre) || of(&s.post));
            if hit {
                g.bytes -= sz;
                n += 1;
            } else {
                kept_e.push_back(e);
                kept_s.push_back(sz);
            }
        }
        g.events = kept_e;
        g.sizes = kept_s;
        n
    }

    /// Position of "now": events published later have a larger sequence number.
    pub fn head(&self) -> u64 {
        self.inner.lock().unwrap().next_seq - 1
    }

    /// First position at or after the operation time. A time at or before the last event pushed out of the log gives a
    /// position before it, so the stream's first read fails with ChangeStreamHistoryLost (its start is not held).
    pub fn seq_at_time(&self, ts: Timestamp) -> u64 {
        let g = self.inner.lock().unwrap();
        if g.trimmed > 0 && (ts.time, ts.increment) <= g.trimmed_time {
            return g.trimmed - 1;
        }
        for e in g.events.iter() {
            if (e.time.time, e.time.increment) >= (ts.time, ts.increment) {
                return e.seq - 1;
            }
        }
        g.next_seq - 1
    }

    /// Events past `after` that `scope` sees (at most `limit`), and the highest sequence number examined. Fails with
    /// ChangeStreamHistoryLost (286) when an event after `after` was already pushed out of the log by its bounds: the
    /// stream fell behind, and reading on from the oldest event left would skip events with no sign (mongod fails an
    /// open stream whose oplog position is gone the same way). Events an erasure removed do not count.
    pub fn read(&self, scope: &Scope, after: u64, limit: usize) -> Result<(Vec<Arc<Ev>>, u64), MongoError> {
        let g = self.inner.lock().unwrap();
        // the database was opened (taken over, or reopened after another process may have owned it) after the stream's
        // position: events another process wrote in between are not in this log
        let reopened = match &scope.db {
            Some(d) => g.opened.get(d).map(|o| o.0),
            None => g.opened.values().map(|o| o.0).max(),
        };
        if reopened.is_some_and(|at| after < at) {
            HISTORY_LOST.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(MongoError::new(286, "ChangeStreamHistoryLost", format!("the change stream's position is from before {} was opened by this process (a restart or a takeover): events written meanwhile are not in this process's change log; resume is not possible", scope.db.as_deref().unwrap_or("a database of the cluster"))));
        }
        if g.trimmed > after {
            HISTORY_LOST.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(MongoError::new(286, "ChangeStreamHistoryLost", format!("the change stream fell behind: events after its position were dropped from the in-memory change log (--change-log-max-mb, now {} MB); resume is not possible", MAX_BYTES.load(std::sync::atomic::Ordering::Relaxed) >> 20)));
        }
        let mut out = Vec::new();
        let mut last = after;
        for e in g.events.iter().filter(|e| e.seq > after) {
            if out.len() >= limit {
                break;
            }
            // change streams report only confirmed changes, in order (MongoDB: majority-committed only)
            let lost = match e.gate.as_ref().map(|g| g.state()) {
                // a pending event of another database or collection does not hold this stream back
                Some(tenant_bench::lifecycle::VisState::Pending) if scope.sees(&e.spec) => break,
                Some(tenant_bench::lifecycle::VisState::Pending) => true,
                Some(tenant_bench::lifecycle::VisState::Lost) => true,
                _ => false,
            };
            last = e.seq;
            if !lost && scope.sees(&e.spec) {
                out.push(Arc::clone(e));
            }
        }
        Ok((out, last))
    }
}

/// `updateDescription` of an operator update, derived from the update spec and the documents before and after.
pub fn update_description(old: &Document, new: &Document, update: &Document) -> Document {
    use crate::query::path_values;
    if update.contains_key("$pipeline") {
        // an aggregation-pipeline update: the fields that differ
        let mut updated = Document::new();
        let mut removed: Vec<Bson> = Vec::new();
        for (k, v) in new {
            if old.get(k) != Some(v) {
                updated.insert(k.clone(), v.clone());
            }
        }
        for k in old.keys() {
            if !new.contains_key(k) {
                removed.push(Bson::String(k.clone()));
            }
        }
        return doc! { "updatedFields": updated, "removedFields": removed, "truncatedArrays": Vec::<Bson>::new() };
    }
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
