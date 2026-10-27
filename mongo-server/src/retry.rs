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

use crate::cmd::Server;
use crate::coll::Writes;
use crate::error::MongoError;
use bson::{doc, Bson, Document};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use tenant_bench::lifecycle::Op;
use tokio::sync::{Mutex as AMutex, OwnedMutexGuard};

type R<T> = Result<T, MongoError>;

/// Idle time after which a logical session (and with it the record of its writes) expires.
const SESSION_TTL_MS: i64 = 30 * 60 * 1000;
/// `uid` of a session created without authentication: the SHA-256 of the empty string.
const EMPTY_UID_B64: &str = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=";

#[derive(Default)]
pub struct Registry {
    m: Mutex<HashMap<String, Arc<AMutex<Sess>>>>,
    calls: AtomicU64,
}

pub struct Sess {
    number: i64,
    /// The current number belongs to a multi-document transaction, not to a retryable write.
    txn: bool,
    last: i64,
    loaded: bool,
    ins: BTreeSet<i32>,
    stmts: BTreeMap<i32, Document>,
}

impl Sess {
    fn new() -> Self {
        Sess { number: -1, txn: false, last: now_ms(), loaded: false, ins: BTreeSet::new(), stmts: BTreeMap::new() }
    }

    fn reset(&mut self, number: i64, txn: bool) {
        self.number = number;
        self.txn = txn;
        self.ins.clear();
        self.stmts.clear();
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

fn too_old(number: i64, lsid: &Document, cur: i64, txn: bool) -> MongoError {
    let what = if txn { "transaction" } else { "retryable write" };
    MongoError::new(225, "TransactionTooOld", format!("Retryable write with txnNumber {number} is prohibited on session {} because a newer {what} with txnNumber {cur} has already started on this session.", lsid_text(lsid)))
}

fn session_key(key: &str) -> Vec<u8> {
    format!("meta/rw/{key}").into_bytes()
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

    /// The batch was applied: the new statements join the record.
    pub fn apply(&mut self) {
        let s = &mut *self.guard;
        s.ins.extend(self.new_ins.drain(..));
        for (id, d) in self.new_stmts.drain(..) {
            s.stmts.insert(id, d);
        }
        s.last = now_ms();
    }
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
        guard.last = now_ms();
        Ok(Some(Begun { run: Run { guard, number, ids, new_ins: Vec::new(), new_stmts: Vec::new(), retried: Vec::new(), dirty }, key }))
    }

    fn session_entry(&self, key: &str) -> Arc<AMutex<Sess>> {
        let mut m = self.retry.m.lock().unwrap();
        if self.retry.calls.fetch_add(1, Relaxed) % 512 == 511 {
            let cutoff = now_ms() - SESSION_TTL_MS;
            m.retain(|_, v| Arc::strong_count(v) > 1 || v.try_lock().map(|s| s.last >= cutoff || s.txn).unwrap_or(true));
        }
        Arc::clone(m.entry(key.to_string()).or_insert_with(|| Arc::new(AMutex::new(Sess::new()))))
    }

    /// Read the session's record back from the database on first use after a restart; forget an expired one.
    async fn session_load(&self, s: &mut Sess, db: &str, key: &str) -> R<()> {
        if !s.loaded {
            s.loaded = true;
            let mut g = self.dbs.lock(db).await;
            if let Some(bytes) = g.get_live(&session_key(key)).await? {
                if let Ok(d) = Document::from_reader(&bytes[..]) {
                    s.number = d.get_i64("number").unwrap_or(-1);
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
        }
        if s.number >= 0 && s.last < now_ms() - SESSION_TTL_MS {
            s.reset(-1, false);
        }
        Ok(())
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
