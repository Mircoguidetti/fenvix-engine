//! Retryable writes. A write command that carries `lsid` and `txnNumber` (and no `autocommit: false`) is
//! recorded per statement: a later command with the same session, transaction number and statement id does not
//! run that statement again but answers with the stored result and lists it in `retriedStmtIds` /
//! `retriedStmtId`. Like mongod, only statements that changed something are recorded (an insert, an update that
//! modified or upserted, a delete that removed, a findAndModify that matched or upserted); a statement that
//! changed nothing runs again. A transaction number lower than the session's current one is refused with
//! `TransactionTooOld`; retryable writes and multi-document transactions share one number space per session.
//!
//! The record of a session lives in memory (authoritative while the process runs, kept 30 minutes after the last
//! use) and, written in the same atomic batch as the statements it describes, in the tenant of the command's
//! database under `meta/rw/{session}`; after a restart the record is read back from the database of the command.
//!
//! Next to that record, in its own key `meta/rw/{session}/txn`, lives the outcome of the session's latest
//! multi-document transaction in that database, as mongod's `config.transactions` does: a commit that writes puts
//! `{number, txn: true, committed: true}` into the batch of the transaction itself, so the outcome is durable exactly
//! when the transaction is. Two keys, not one (C10): a retryable write of the session after the transaction rewrites
//! its own record and leaves the outcome alone, so a repeated commit still finds it. A `commitTransaction` repeated for a
//! committed transaction that this process does not remember (a restart, another process) answers ok instead of
//! `NoSuchTransaction` (whose `TransientTransactionError` label makes the drivers run the whole transaction again). A
//! commit names no database (it runs on `admin`), so the session's databases are found through small objects
//! `txn-hints/{session}/{db}`, one per database the session committed in, written before the first commit with writes
//! of the session in that database (and again after [`hint_refresh_ms()`]), and always before the batch. A hint lives
//! [`hint_ttl_ms()`]: the session TTL plus the refresh period (an outcome written up to one refresh period after its hint
//! was, and alive for a session TTL after that) plus one more period of margin for the clock of the store (C10: a hint
//! counted as expired at the session TTL hid outcomes of live sessions). A hint is never
//! rewritten with another database, so a process whose memory of a hint is fresh is never wrong about it, also when
//! another process wrote hints of the same session (C8: one hint per session went stale with two processes). The lookup
//! lists the session's prefix and takes the highest transaction number among the records of those databases.
//!
//! Cleanup, the logical-session rule (`session_ttl_ms()`, 30 minutes, mongod's `localLogicalSessionTimeoutMinutes`): a
//! record or an outcome idle longer than that counts as absent, a hint older than [`hint_ttl_ms()`] too; a write of a
//! session record deletes, in its own batch, the
//! expired records of its database (at most every [`sweep_every_ms()`] per database, [`SWEEP_SCAN`] records looked at per
//! pass, resuming where the last pass stopped); a background pass deletes the expired hints every [`sweep_every_ms()`].

use crate::cmd::Server;
use crate::coll::Writes;
use crate::error::MongoError;
use bson::{doc, Bson, Document};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use object_store::{ObjectStore, ObjectStoreExt};
use tenant_bench::lifecycle::Op;
use tokio::sync::{Mutex as AMutex, OwnedMutexGuard};

type R<T> = Result<T, MongoError>;

/// Idle time after which a logical session (and with it the record of its writes) expires.
const SESSION_TTL_DEFAULT_MS: i64 = 30 * 60 * 1000;

/// The session TTL: 30 minutes, or for the tests only `MONGO_TEST_SESSION_TTL_MS` (honoured only with
/// `MONGO_ALLOW_UNSAFE_TEST_FLAGS=1`). The hint refresh and the cleanup period scale with it (a third of it).
fn session_ttl_ms() -> i64 {
    static TTL: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *TTL.get_or_init(|| {
        if std::env::var("MONGO_ALLOW_UNSAFE_TEST_FLAGS").as_deref() == Ok("1") {
            if let Some(ms) = std::env::var("MONGO_TEST_SESSION_TTL_MS").ok().and_then(|v| v.parse::<i64>().ok()).filter(|v| *v > 0) {
                return ms;
            }
        }
        SESSION_TTL_DEFAULT_MS
    })
}
/// A session's hint is written again after this time (10 minutes), so a live session's hint is never older than the TTL.
fn hint_refresh_ms() -> i64 {
    session_ttl_ms() / 3
}
/// A hint counts as expired (lookup) and is deleted (cleanup) after this time, 50 minutes: see the module comment.
fn hint_ttl_ms() -> i64 {
    session_ttl_ms() + 2 * hint_refresh_ms()
}
/// Expired session records of a database, and expired hints, are removed at most this often (10 minutes).
fn sweep_every_ms() -> i64 {
    session_ttl_ms() / 3
}
/// Session records looked at by one cleanup pass of a database.
pub const SWEEP_SCAN: usize = 10_000;
const SESSION_PREFIX: &[u8] = b"meta/rw/";
const HINTS: &str = "txn-hints";
/// `uid` of a session created without authentication: the SHA-256 of the empty string.
const EMPTY_UID_B64: &str = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=";

#[derive(Default)]
pub struct Registry {
    m: Mutex<HashMap<String, Arc<AMutex<Sess>>>>,
    calls: AtomicU64,
    /// Hints written by this process: (session, database) -> when.
    hinted: Mutex<HashMap<(String, String), i64>>,
    /// Cleanup of each database's session records: (last pass, key to resume from).
    swept: Mutex<HashMap<String, (i64, Vec<u8>)>>,
}

pub struct Sess {
    number: i64,
    /// The current number belongs to a multi-document transaction, not to a retryable write.
    txn: bool,
    last: i64,
    loaded: bool,
    ins: BTreeSet<i32>,
    stmts: BTreeMap<i32, Document>,
    /// The commit of the batch that last wrote this record, until it is known durable: a retry answered from the record
    /// waits for it first (C8: the record is in memory as soon as the batch is staged, before it is durable).
    durable: Option<tenant_bench::lifecycle::Commit>,
}

impl Sess {
    fn new() -> Self {
        Sess { number: -1, txn: false, last: now_ms(), loaded: false, ins: BTreeSet::new(), stmts: BTreeMap::new(), durable: None }
    }

    fn reset(&mut self, number: i64, txn: bool) {
        self.number = number;
        self.txn = txn;
        self.ins.clear();
        self.stmts.clear();
        self.durable = None;
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn uuid_string(b: &[u8]) -> String {
    let h = hex(b);
    if h.len() != 32 {
        return h;
    }
    format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
}

/// `LogicalSessionId::toString()` as it appears in mongod's messages.
fn lsid_text(lsid: &Document) -> String {
    let id = match lsid.get("id") {
        Some(Bson::Binary(b)) => uuid_string(&b.bytes),
        _ => String::new(),
    };
    let uid = match lsid.get("uid") {
        Some(Bson::Binary(b)) => {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.encode(&b.bytes)
        }
        _ => EMPTY_UID_B64.to_string(),
    };
    format!("{id} - {uid} -  - ")
}

fn lsid_key(cmd: &Document) -> Option<String> {
    match cmd.get_document("lsid").ok()?.get("id")? {
        Bson::Binary(b) => Some(hex(&b.bytes)),
        _ => None,
    }
}

/// `txnNumber` of a command, validated like mongod's `OperationSessionInfo` parser.
pub fn txn_number(cmd: &Document) -> R<Option<i64>> {
    match cmd.get("txnNumber") {
        None => Ok(None),
        Some(Bson::Int64(n)) if *n < 0 => Err(MongoError::new(72, "InvalidOptions", "Transaction number cannot be negative".into())),
        Some(Bson::Int64(n)) => Ok(Some(*n)),
        Some(other) => Err(MongoError::type_mismatch(format!("BSON field 'OperationSessionInfo.txnNumber' is the wrong type '{}', expected type 'long'", crate::update::type_name(other)))),
    }
}

impl Server {
    /// Is this command allowed to carry `txnNumber`? mongod refuses it on a standalone server, and, unless it is part of
    /// a multi-document transaction, on any command that is not a retryable write.
    pub(crate) fn check_txn_number(&self, name: &str, cmd: &Document) -> R<()> {
        if txn_number(cmd)?.is_none() {
            return Ok(());
        }
        if self.rs.lock().unwrap().is_none() {
            return Err(MongoError::new(20, "IllegalOperation", "Transaction numbers are only allowed on a replica set member or mongos".into()));
        }
        if !matches!(cmd.get("autocommit"), Some(Bson::Boolean(false))) && !matches!(name, "insert" | "update" | "delete" | "findAndModify" | "findandmodify") {
            return Err(MongoError::new(50768, "NotARetryableWriteCommand", format!("txnNumber may only be provided for multi-document transactions and retryable write commands. autocommit:false was not provided, and {name} is not a retryable write command.")));
        }
        Ok(())
    }
}

/// A retryable write: `lsid` and `txnNumber`, not in a transaction (the statements and their record go in one batch).
pub fn is_retryable_write(cmd: &Document) -> bool {
    matches!(txn_number(cmd), Ok(Some(_))) && !matches!(cmd.get("autocommit"), Some(Bson::Boolean(false))) && lsid_key(cmd).is_some()
}

/// fix-store-errors: a retryable write (`lsid` and `txnNumber`, not in a transaction) that failed because the store
/// refused or did not answer gets the `RetryableWriteError` label, so the driver retries it. Safe because its statements
/// and its record (this file) go in one atomic batch: either both landed and the retry answers from the record without
/// applying anything again, or neither did and the retry applies it once. Any other write keeps code 1 without a label:
/// its outcome is unknown and nothing deduplicates a second attempt. The code and the code name are unchanged.
pub fn label_store_error(name: &str, cmd: &Document, e: &mut MongoError) {
    if e.code != 1 || e.extra.as_ref().is_some_and(|x| x.contains_key("errorLabels")) {
        return;
    }
    if !matches!(name, "insert" | "update" | "delete" | "findAndModify" | "findandmodify") {
        return;
    }
    if !matches!(txn_number(cmd), Ok(Some(_))) || matches!(cmd.get("autocommit"), Some(Bson::Boolean(false))) || lsid_key(cmd).is_none() {
        return;
    }
    let m = e.msg.as_str();
    if m.contains("Generic S3 error") || m.contains("object store error") || m.contains("Error performing") {
        STORE_ERRORS_LABELLED.fetch_add(1, Relaxed);
        let mut x = e.extra.take().unwrap_or_default();
        x.insert("errorLabels", vec![Bson::String("RetryableWriteError".into())]);
        e.extra = Some(x);
    }
}

/// P-fix-slow-store item 5: retries whose original was staged by an opening since given up, answered after reading the
/// session's record back from the database.
pub static RECORDS_RELOADED: AtomicU64 = AtomicU64::new(0);

/// Retryable writes answered with the `RetryableWriteError` label because the store refused or did not answer.
pub static STORE_ERRORS_LABELLED: AtomicU64 = AtomicU64::new(0);

fn too_old(number: i64, lsid: &Document, cur: i64, txn: bool) -> MongoError {
    let what = if txn { "transaction" } else { "retryable write" };
    MongoError::new(225, "TransactionTooOld", format!("Retryable write with txnNumber {number} is prohibited on session {} because a newer {what} with txnNumber {cur} has already started on this session.", lsid_text(lsid)))
}

fn session_key(key: &str) -> Vec<u8> {
    format!("meta/rw/{key}").into_bytes()
}

/// The outcome of the session's latest transaction in a database: a key of its own, never rewritten by a retryable write.
fn outcome_key(key: &str) -> Vec<u8> {
    format!("meta/rw/{key}/txn").into_bytes()
}

fn hint_path(key: &str, db: &str) -> object_store::path::Path {
    object_store::path::Path::from(format!("{HINTS}/{key}/{db}"))
}

fn hint_prefix(key: &str) -> object_store::path::Path {
    object_store::path::Path::from(format!("{HINTS}/{key}"))
}

/// The record of a committed transaction, written in the transaction's own batch (see the module comment).
pub fn txn_outcome_op(key: &str, number: i64) -> Op {
    let rec = doc! { "number": number, "txn": true, "committed": true, "last": now_ms() };
    Op::Put(outcome_key(key), rec.to_vec().unwrap_or_default())
}

/// Statement ids of a batch of `n` statements: `stmtIds`, or `stmtId` (default 0) plus the position.
fn stmt_ids(cmd: &Document, n: usize, name: &str) -> R<Vec<i32>> {
    let single = cmd.get("stmtId");
    let many = cmd.get("stmtIds");
    if single.is_some() && many.is_some() {
        return Err(MongoError::new(72, "InvalidOptions", "May not specify both stmtId and stmtIds in write command".into()));
    }
    if let Some(s) = single {
        let Bson::Int32(first) = s else {
            return Err(MongoError::type_mismatch(format!("BSON field '{name}.stmtId' is the wrong type '{}', expected type 'int'", crate::update::type_name(s))));
        };
        return Ok((0..n as i32).map(|i| first + i).collect());
    }
    if let Some(m) = many {
        let Bson::Array(a) = m else {
            return Err(MongoError::type_mismatch(format!("BSON field '{name}.stmtIds' is the wrong type '{}', expected type 'array'", crate::update::type_name(m))));
        };
        let mut v = Vec::with_capacity(a.len());
        for (i, x) in a.iter().enumerate() {
            match x {
                Bson::Int32(x) => v.push(*x),
                other => return Err(MongoError::type_mismatch(format!("BSON field '{name}.stmtIds.{i}' is the wrong type '{}', expected type 'int'", crate::update::type_name(other)))),
            }
        }
        if v.len() != n {
            return Err(MongoError::new(16, "InvalidLength", format!("Number of statement ids must match the number of batch entries. Got {} statement ids but {n} operations.", v.len())));
        }
        return Ok(v);
    }
    Ok((0..n as i32).collect())
}

/// One command's hold on its session's record.
pub struct Run {
    guard: OwnedMutexGuard<Sess>,
    number: i64,
    ids: Vec<i32>,
    new_ins: Vec<i32>,
    new_stmts: Vec<(i32, Document)>,
    /// Statement ids answered from the record.
    pub retried: Vec<i32>,
    dirty: bool,
}

impl Run {
    pub fn is_retried(&self, i: usize) -> bool {
        let id = self.ids[i];
        self.guard.ins.contains(&id) || self.guard.stmts.contains_key(&id)
    }

    /// The stored result of statement `i` (`None` for a recorded insert).
    pub fn stored(&mut self, i: usize) -> Option<Document> {
        let id = self.ids[i];
        self.retried.push(id);
        self.guard.stmts.get(&id).cloned()
    }

    pub fn id(&self, i: usize) -> i32 {
        self.ids[i]
    }

    pub fn record_insert(&mut self, i: usize) {
        self.new_ins.push(self.ids[i]);
    }

    pub fn record(&mut self, i: usize, d: Document) {
        self.new_stmts.push((self.ids[i], d));
    }

    pub fn retried_ids(&self) -> Bson {
        Bson::Array(self.retried.iter().map(|x| Bson::Int32(*x)).collect())
    }

    /// Put the session's record into the batch that applies the statements.
    pub fn stage(&self, w: &mut Writes, key: &str) {
        if self.new_ins.is_empty() && self.new_stmts.is_empty() && !self.dirty {
            return;
        }
        let ins: Vec<Bson> = self.guard.ins.iter().chain(self.new_ins.iter()).map(|x| Bson::Int32(*x)).collect();
        let mut stmts: Vec<Bson> = Vec::new();
        for (id, d) in self.guard.stmts.iter().filter(|(id, _)| !self.new_stmts.iter().any(|(n, _)| n == *id)).chain(self.new_stmts.iter().map(|(a, b)| (a, b))) {
            let mut d = d.clone();
            d.insert("id", *id);
            stmts.push(Bson::Document(d));
        }
        let rec = doc! { "number": self.number, "txn": false, "last": now_ms(), "ins": ins, "stmts": stmts };
        if let Ok(bytes) = rec.to_vec() {
            w.extra.push(Op::Put(session_key(key), bytes));
        }
    }

    /// The batch was applied: the new statements join the record. A retry answered from them waits for the durability
    /// of this batch (the commit the command just staged).
    pub fn apply(&mut self) {
        let s = &mut *self.guard;
        if !self.new_ins.is_empty() || !self.new_stmts.is_empty() {
            s.durable = crate::dbs::staged_commit();
        }
        s.ins.extend(self.new_ins.drain(..));
        for (id, d) in self.new_stmts.drain(..) {
            s.stmts.insert(id, d);
        }
        s.last = now_ms();
    }
}

/// See [`Server::txn_stored_outcome`].
#[derive(Clone, Copy, Debug)]
pub struct StoredTxn {
    pub committed: bool,
    pub newest: i64,
}

pub struct Begun {
    pub run: Run,
    pub key: String,
}

impl Server {
    /// Start a retryable write of `n` statements (`None`: the command is not one). The returned run holds the
    /// session until it is dropped, so a retry racing the original waits for it.
    pub(crate) async fn retry_begin(&self, db: &str, cmd: &Document, n: usize, name: &str) -> R<Option<Begun>> {
        if matches!(cmd.get("autocommit"), Some(Bson::Boolean(false))) {
            return Ok(None);
        }
        let Some(number) = txn_number(cmd)? else {
            // stmtId(s) are still validated.
            stmt_ids(cmd, n, name)?;
            return Ok(None);
        };
        let ids = stmt_ids(cmd, n, name)?;
        if self.rs.lock().unwrap().is_none() {
            return Err(MongoError::new(20, "IllegalOperation", "Transaction numbers are only allowed on a replica set member or mongos".into()));
        }
        let key = lsid_key(cmd).ok_or_else(|| MongoError::new(72, "InvalidOptions", "Transaction number requires a session ID to also be specified".into()))?;
        let lsid = cmd.get_document("lsid").cloned().unwrap_or_default();
        let sess = self.session_entry(&key);
        let mut guard = sess.lock_owned().await;
        self.session_load(&mut guard, db, &key).await?;
        let mut dirty = false;
        if number < guard.number {
            return Err(too_old(number, &lsid, guard.number, guard.txn));
        }
        if number == guard.number && guard.txn {
            return Err(MongoError::new(217, "IncompleteTransactionHistory", "Cannot retry a retryable write that has been converted into a transaction".into()));
        }
        if number > guard.number {
            guard.reset(number, false);
            dirty = true;
        }
        // a retry of the current write: what it answers from the record must be durable first, as on mongod, where the
        // retry waits for the write concern of the original write
        if number == guard.number {
            if let Some(c) = guard.durable.clone() {
                let relaxed = crate::dbs::RELAX.try_with(|r| r.get()).unwrap_or(false) && crate::ctl::durability::relaxed_ms(c.tenant()) > 0 && crate::locallog::get().is_none();
                if !relaxed {
                    if let Err(e) = c.clone().wait().await {
                        if !c.lost() || crate::error::control_definite() {
                            return Err(crate::error::outcome_unknown(name, MongoError::from(e)));
                        }
                        // P-fix-slow-store item 5: the opening that staged the original was given up; the record in
                        // memory may describe a batch that never became durable. The database's next opening (its WAL
                        // replay) knows: forget the memory and read the record back from it, so the retry applies the
                        // write once if the original did not land, and answers from the record if it did.
                        RECORDS_RELOADED.fetch_add(1, Relaxed);
                        *guard = Sess::new();
                        self.session_load(&mut guard, db, &key).await?;
                        if number < guard.number {
                            return Err(too_old(number, &lsid, guard.number, guard.txn));
                        }
                        if number > guard.number {
                            guard.reset(number, false);
                            dirty = true;
                        }
                    }
                }
                guard.durable = None;
            }
        }
        guard.last = now_ms();
        Ok(Some(Begun { run: Run { guard, number, ids, new_ins: Vec::new(), new_stmts: Vec::new(), retried: Vec::new(), dirty }, key }))
    }

    fn session_entry(&self, key: &str) -> Arc<AMutex<Sess>> {
        let mut m = self.retry.m.lock().unwrap();
        if self.retry.calls.fetch_add(1, Relaxed) % 512 == 511 {
            let cutoff = now_ms() - session_ttl_ms();
            m.retain(|_, v| Arc::strong_count(v) > 1 || v.try_lock().map(|s| s.last >= cutoff || s.txn).unwrap_or(true));
        }
        Arc::clone(m.entry(key.to_string()).or_insert_with(|| Arc::new(AMutex::new(Sess::new()))))
    }

    /// Read the session's record back from the database on first use after a restart; forget an expired one.
    async fn session_load(&self, s: &mut Sess, db: &str, key: &str) -> R<()> {
        if !s.loaded {
            s.loaded = true;
            let mut g = self.dbs.lock(db).await;
            let outcome = g.get_live(&outcome_key(key)).await?;
            if let Some(bytes) = g.get_live(&session_key(key)).await? {
                if let Ok(d) = Document::from_reader(&bytes[..]) {
                    s.number = d.get_i64("number").unwrap_or(-1);
                    s.txn = d.get_bool("txn").unwrap_or(false);
                    s.last = d.get_i64("last").unwrap_or(0);
                    s.ins = d.get_array("ins").map(|a| a.iter().filter_map(|x| if let Bson::Int32(i) = x { Some(*i) } else { None }).collect()).unwrap_or_default();
                    for e in d.get_array("stmts").into_iter().flatten() {
                        if let Bson::Document(e) = e {
                            if let Ok(id) = e.get_i32("id") {
                                let mut e = e.clone();
                                e.remove("id");
                                s.stmts.insert(id, e);
                            }
                        }
                    }
                }
            }
            // a transaction committed after the latest retryable write: the session's number is the transaction's
            if let Some(d) = outcome.and_then(|b| Document::from_reader(&b[..]).ok()) {
                let (n, last) = (d.get_i64("number").unwrap_or(-1), d.get_i64("last").unwrap_or(0));
                if n > s.number {
                    s.reset(n, true);
                }
                s.last = s.last.max(last);
            }
        }
        if s.number >= 0 && s.last < now_ms() - session_ttl_ms() {
            s.reset(-1, false);
        }
        Ok(())
    }

    /// A statement of a transaction (not its start) at a number below the session's latest retryable write.
    pub(crate) async fn retry_txn_continue(&self, lsid_key: &str, lsid: &Document, number: i64) -> R<()> {
        let sess = self.session_entry(lsid_key);
        let s = sess.lock().await;
        if !s.txn && number < s.number {
            return Err(MongoError::new(225, "TransactionTooOld", format!("Cannot start transaction with {{ txnNumber: {number} }} on session {} because a newer retryable write with txnNumberAndRetryCounter {{ txnNumber: {}, txnRetryCounter: -1 }} has already started on this session.", lsid_text(lsid), s.number)));
        }
        Ok(())
    }

    /// The session's latest number as a write or a transaction (the "active transaction number" of mongod's messages).
    pub(crate) async fn retry_session_number(&self, lsid_key: &str) -> i64 {
        self.session_entry(lsid_key).lock().await.number
    }

    /// A multi-document transaction starts at `number` on a session: it shares the number space with retryable writes.
    pub(crate) async fn retry_txn_start(&self, lsid_key: &str, lsid: &Document, number: i64) -> R<()> {
        let sess = self.session_entry(lsid_key);
        let mut s = sess.lock().await;
        if !s.txn && number < s.number {
            return Err(MongoError::new(225, "TransactionTooOld", format!("Cannot start transaction with {{ txnNumber: {number} }} on session {} because a newer retryable write with txnNumberAndRetryCounter {{ txnNumber: {}, txnRetryCounter: -1 }} has already started on this session.", lsid_text(lsid), s.number)));
        }
        if !s.txn && number == s.number {
            return Err(MongoError::new(117, "ConflictingOperationInProgress", "Only servers in a sharded cluster can start a new transaction at the active transaction number".into()));
        }
        if number > s.number {
            s.reset(number, true);
        }
        s.loaded = true;
        s.last = now_ms();
        Ok(())
    }
}

impl Server {
    /// Before the batch of a commit with writes: make sure `txn-hints/{session}/{db}` exists and is fresh.
    pub(crate) async fn txn_hint(&self, key: &str, db: &str) -> R<()> {
        let now = now_ms();
        let k = (key.to_string(), db.to_string());
        if let Some(at) = self.retry.hinted.lock().unwrap().get(&k) {
            if now - at < hint_refresh_ms() {
                return Ok(());
            }
        }
        let body = doc! { "db": db, "last": now }.to_vec().unwrap_or_default();
        self.dbs.store().put(&hint_path(key, db), bytes::Bytes::from(body).into()).await.map_err(|e| MongoError::from(anyhow::anyhow!("transaction hint of the session: {e}")))?;
        let mut h = self.retry.hinted.lock().unwrap();
        if h.len() > 100_000 {
            h.retain(|_, at| now - *at < hint_refresh_ms());
        }
        h.insert(k, now);
        Ok(())
    }

    /// What the store knows of transaction `number` of a session, read from the databases the session's hints name: is
    /// its commit stored (`committed`), and the highest live number of the session (a transaction or a retryable write)
    /// in those databases (`newest`, -1 when none). Used when this process does not remember the transaction `number`. A
    /// database whose records cannot be read (another process owns it, the store fails) makes the answer an error unless
    /// a readable record shows the commit or a higher number, so that an unknown outcome is never reported as
    /// `NoSuchTransaction`.
    pub(crate) async fn txn_stored_outcome(&self, key: &str, number: i64) -> R<StoredTxn> {
        use futures::TryStreamExt;
        let now = now_ms();
        let hints: Vec<object_store::path::Path> = self
            .dbs
            .store()
            .list(Some(&hint_prefix(key)))
            .try_filter(|m| futures::future::ready(m.last_modified.timestamp_millis() >= now - hint_ttl_ms()))
            .map_ok(|m| m.location)
            .try_collect()
            .await
            .map_err(|e| MongoError::from(anyhow::anyhow!("transaction hints of the session: {e}")))?;
        let mut out = StoredTxn { committed: false, newest: -1 };
        let mut failed: Option<MongoError> = None;
        for p in hints {
            let Some(db) = p.filename().map(|s| s.to_string()) else { continue };
            if !crate::dbs::valid_db_name(&db) {
                continue;
            }
            let recs = async {
                let mut g = self.dbs.lock(&db).await;
                let outcome = g.get_live(&outcome_key(key)).await.map_err(MongoError::from)?;
                let writes = g.get_live(&session_key(key)).await.map_err(MongoError::from)?;
                Ok::<_, MongoError>([outcome, writes])
            }
            .await;
            let recs = match recs {
                Ok(r) => r,
                Err(e) => {
                    failed.get_or_insert(e);
                    continue;
                }
            };
            // the outcome, and the record of the retryable writes (before C10 it could hold the outcome too)
            for bytes in recs.into_iter().flatten() {
                let Ok(d) = Document::from_reader(&bytes[..]) else { continue };
                if d.get_i64("last").unwrap_or(0) < now - session_ttl_ms() {
                    continue;
                }
                let n = d.get_i64("number").unwrap_or(-1);
                out.newest = out.newest.max(n);
                if n == number && d.get_bool("txn").unwrap_or(false) && d.get_bool("committed").unwrap_or(false) {
                    out.committed = true;
                }
            }
        }
        match failed {
            // an unreadable database may hold the transaction asked for
            Some(e) if !out.committed && out.newest <= number => Err(e),
            _ => Ok(out),
        }
    }

    /// Deletes of the expired session records of `db`, for the batch that writes the record of `keep` (see the module
    /// comment). Empty unless a pass is due.
    pub(crate) async fn session_sweep_ops(&self, g: &mut crate::dbs::Entry, db: &str, keep: &str) -> Vec<Op> {
        let now = now_ms();
        let from = {
            let sw = self.retry.swept.lock().unwrap();
            match sw.get(db) {
                Some((at, _)) if now - at < sweep_every_ms() => return Vec::new(),
                Some((_, k)) => k.clone(),
                None => SESSION_PREFIX.to_vec(),
            }
        };
        let end = crate::keyenc::prefix_end(SESSION_PREFIX);
        let rows = match g.scan_range_live(&from, end.as_deref(), Some(SWEEP_SCAN)).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("session records of {db}: cleanup skipped: {e:#}");
                return Vec::new();
            }
        };
        // a full page: the next pass resumes after it; a short one reached the end: the next starts over
        let next = if rows.len() >= SWEEP_SCAN { rows.last().map(|(k, _)| { let mut k = k.to_vec(); k.push(0); k }).unwrap_or_else(|| SESSION_PREFIX.to_vec()) } else { SESSION_PREFIX.to_vec() };
        self.retry.swept.lock().unwrap().insert(db.to_string(), (now, next));
        // the session writing now keeps its record and its outcome (a delete would follow the put in the same batch)
        let keep = [session_key(keep), outcome_key(keep)];
        rows.into_iter()
            .filter(|(k, v)| !keep.iter().any(|x| k.as_ref() == x.as_slice()) && Document::from_reader(&v[..]).map(|d| d.get_i64("last").unwrap_or(0) < now - session_ttl_ms()).unwrap_or(true))
            .map(|(k, _)| Op::Delete(k.to_vec()))
            .collect()
    }

    /// Every [`sweep_every_ms()`]: delete the hints not written for longer than [`hint_ttl_ms()`].
    pub fn spawn_txn_hint_sweeper(self: &Arc<Self>) {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(sweep_every_ms() as u64)).await;
                if let Err(e) = me.sweep_txn_hints().await {
                    eprintln!("transaction hints: cleanup failed: {e:#}");
                }
            }
        });
    }

    /// One pass of the hint cleanup; returns the number of hints deleted.
    pub async fn sweep_txn_hints(&self) -> anyhow::Result<usize> {
        use futures::TryStreamExt;
        let store = self.dbs.store();
        let cutoff = now_ms() - hint_ttl_ms();
        let old: Vec<object_store::path::Path> = store
            .list(Some(&object_store::path::Path::from(HINTS)))
            .try_filter(|m| futures::future::ready(m.last_modified.timestamp_millis() < cutoff))
            .map_ok(|m| m.location)
            .try_collect()
            .await?;
        let mut n = 0;
        for p in old {
            if store.delete(&p).await.is_ok() {
                n += 1;
            }
        }
        Ok(n)
    }
}
