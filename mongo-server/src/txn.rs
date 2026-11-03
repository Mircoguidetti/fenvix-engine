//! Multi-document transactions. A transaction snapshots its database when it starts (a slatedb snapshot),
//! buffers its writes (documents and index entries) in a [`Writes`] set, shows them to its own reads, and
//! applies them as one atomic batch on commit. A write to a document that changed since the snapshot fails
//! with `WriteConflict` (a `TransientTransactionError`, which drivers retry), checked when the write is made
//! and again at commit. A transaction spans one database (one tenant).

use crate::cmd::{int_arg, ok, Server, R};
use crate::coll::{Overlay, Writes};
use crate::dbs::Entry;
use crate::error::MongoError;
use bson::{doc, Bson, Document};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// MongoDB aborts a transaction after `transactionLifetimeLimitSeconds` (default 60).
pub static LIFETIME_SECS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(60);

pub struct Txn {
    pub id: u64,
    pub lsid_doc: Document,
    pub db: String,
    pub number: i64,
    pub w: Writes,
    pub snap: Arc<slatedb::DbSnapshot>,
    pub seq: u64,
    pub started: Instant,
}

/// How far a running commit is, published by the commit that runs it to the repeated commits that wait for it.
#[derive(Clone)]
enum Phase {
    /// Conflict check, hint and staging of the batch.
    Staging,
    /// The batch is staged; it is durable once this clone of its commit is (`None`: already durable).
    Staged(Option<tenant_bench::lifecycle::Commit>),
}

enum State {
    /// The transaction is idle between commands, or `None` while a command of it runs.
    Active(Option<Txn>),
    /// A commit of the transaction runs: it is staging the batch or waiting for it to be durable. A repeated commit waits
    /// for the same durability before it answers ok (C8: answering at once lost acknowledged transactions on a crash).
    /// Stays here when the durability wait failed or was dropped, so a later repeated commit waits again.
    Committing(tokio::sync::watch::Receiver<Phase>),
    /// Committed and durable.
    Committed,
    Aborted,
}

/// Puts a transaction whose commit stopped while staging (the command was dropped) back to aborted: nothing was staged
/// that its repeated commits could wait for. Disarmed once the commit has an outcome.
struct StagingGuard<'a> {
    sessions: &'a Sessions,
    lsid: &'a str,
    number: i64,
    armed: bool,
}

impl Drop for StagingGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(s) = self.sessions.m.lock().unwrap().get_mut(self.lsid) {
            if s.number == self.number && matches!(&s.state, State::Committing(r) if matches!(*r.borrow(), Phase::Staging)) {
                s.state = State::Aborted;
            }
        }
    }
}

struct Session {
    number: i64,
    state: State,
}

#[derive(Default)]
pub struct Sessions {
    m: Mutex<HashMap<String, Session>>,
}

pub struct TxnCtx {
    pub lsid: String,
    pub lsid_doc: Document,
    pub number: i64,
    pub start: bool,
}

/// mongod's text for a session: `<uuid> - <user digest> -  - `.
fn session_text(tc: &TxnCtx) -> String {
    let uuid = match tc.lsid_doc.get("id") {
        Some(Bson::Binary(b)) if b.bytes.len() == 16 => {
            let h: String = b.bytes.iter().map(|x| format!("{x:02x}")).collect();
            format!("{}-{}-{}-{}-{}", &h[..8], &h[8..12], &h[12..16], &h[16..20], &h[20..])
        }
        _ => String::new(),
    };
    let uid = {
        use base64::Engine;
        use sha2::Digest;
        base64::engine::general_purpose::STANDARD.encode(sha2::Sha256::digest(b""))
    };
    format!("{uuid} - {uid} -  - ")
}

/// mongod's text for a transaction number that matches no transaction of the session.
fn given_msg(tc: &TxnCtx, active: i64) -> String {
    format!("Given transaction number {} on session {} using txnRetryCounter 0 does not match any in-progress transactions. The active transaction number is {active}", tc.number, session_text(tc))
}

/// mongod's text for a transaction number below the session's newest transaction.
fn too_old(tc: &TxnCtx, newest: i64) -> MongoError {
    MongoError::new(225, "TransactionTooOld", format!("Cannot start transaction with {{ txnNumber: {} }} on session {} because a newer transaction with txnNumberAndRetryCounter {{ txnNumber: {newest}, txnRetryCounter: 0 }} has already started on this session.", tc.number, session_text(tc)))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn lsid_of(cmd: &Document) -> Option<String> {
    match cmd.get_document("lsid").ok()?.get("id")? {
        Bson::Binary(b) => Some(hex(&b.bytes)),
        _ => None,
    }
}

/// The transaction fields of a command (`autocommit: false` plus `txnNumber`).
pub fn parse(cmd: &Document) -> R<Option<TxnCtx>> {
    if !matches!(cmd.get("autocommit"), Some(Bson::Boolean(false))) {
        return Ok(None);
    }
    let number = int_arg(cmd.get("txnNumber")).ok_or_else(|| MongoError::new(72, "InvalidOptions", "Transaction number is required for a multi-document transaction".into()))?;
    let lsid = lsid_of(cmd).ok_or_else(|| MongoError::new(72, "InvalidOptions", "A multi-document transaction needs a session id".into()))?;
    Ok(Some(TxnCtx { lsid, lsid_doc: cmd.get_document("lsid").cloned().unwrap_or_default(), number, start: matches!(cmd.get("startTransaction"), Some(Bson::Boolean(true))) }))
}

pub fn attach(g: &mut Entry, tx: &Option<Txn>) {
    if let Some(t) = tx {
        g.snap = Some(Arc::clone(&t.snap));
    }
}

pub fn take_writes(tx: &mut Option<Txn>) -> Writes {
    match tx {
        Some(t) => std::mem::replace(&mut t.w, Writes::new()),
        None => Writes::new(),
    }
}

/// Give the pending writes taken with [`take_writes`] back to the transaction, on a path that ends without
/// [`Server::commit`] (which puts them back itself). Outside a transaction it does nothing.
pub fn put_back(tx: &mut Option<Txn>, w: Writes) {
    if let Some(t) = tx {
        t.w = w;
    }
}

pub fn tx_ov(tx: &Option<Txn>) -> Option<&Overlay> {
    tx.as_ref().map(|t| &t.w.ov)
}

fn no_such(msg: String) -> MongoError {
    let mut e = MongoError::new(251, "NoSuchTransaction", msg);
    e.extra = Some(doc! { "errorLabels": ["TransientTransactionError"] });
    e
}

impl Server {
    /// Start (`startTransaction: true`) or continue a transaction; the caller owns it until [`Server::txn_release`].
    pub(crate) async fn txn_acquire(&self, db: &str, tc: &TxnCtx) -> R<Txn> {
        if !tc.start {
            self.retry_txn_continue(&tc.lsid, &tc.lsid_doc, tc.number).await?;
            let retry_n = self.retry_session_number(&tc.lsid).await;
            let mut m = self.sessions.m.lock().unwrap();
            let Some(s) = m.get_mut(&tc.lsid) else {
                return Err(no_such(given_msg(&tc, retry_n)));
            };
            if s.number > tc.number {
                return Err(too_old(&tc, s.number));
            }
            if s.number != tc.number {
                return Err(no_such(given_msg(&tc, s.number.max(retry_n))));
            }
            return match std::mem::replace(&mut s.state, State::Active(None)) {
                State::Active(Some(t)) => Ok(t),
                State::Active(None) => Err(MongoError::new(117, "ConflictingOperationInProgress", "Another operation of this transaction is in progress".into())),
                State::Aborted => {
                    s.state = State::Aborted;
                    Err(no_such(format!("Transaction with {{ txnNumber: {} }} has been aborted.", tc.number)))
                }
                State::Committed => {
                    s.state = State::Committed;
                    Err(no_such(format!("Transaction {} has been committed.", tc.number)))
                }
                committing @ State::Committing(_) => {
                    s.state = committing;
                    Err(no_such(format!("Transaction {} has been committed.", tc.number)))
                }
            };
        }
        self.retry_txn_start(&tc.lsid, &tc.lsid_doc, tc.number).await?;
        // Start: abort an older transaction of the session first.
        let old = {
            let mut m = self.sessions.m.lock().unwrap();
            let s = m.entry(tc.lsid.clone()).or_insert(Session { number: -1, state: State::Aborted });
            if tc.number < s.number {
                return Err(too_old(&tc, s.number));
            }
            if tc.number == s.number {
                return Err(MongoError::new(50911, "Location50911", format!("Cannot start a transaction at given transaction number {} on session because a transaction with the same number is already started.", tc.number)));
            }
            s.number = tc.number;
            match std::mem::replace(&mut s.state, State::Active(None)) {
                State::Active(Some(t)) => Some(t),
                _ => None,
            }
        };
        if let Some(t) = old {
            let mut g = self.dbs.lock(&t.db).await;
            g.end_txn(t.id);
        }
        let started = async {
            let mut g = self.dbs.lock(db).await;
            let (snap, seq) = g.begin_snapshot().await?;
            let mut w = Writes::new();
            w.snap_seq = Some(seq);
            let id = self.next_txn_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            w.txn_id = Some(id);
            Ok::<_, MongoError>(Txn { id, lsid_doc: tc.lsid_doc.clone(), db: db.to_string(), number: tc.number, w, snap, seq, started: Instant::now() })
        }
        .await;
        match started {
            Ok(t) => Ok(t),
            Err(e) => {
                if let Some(s) = self.sessions.m.lock().unwrap().get_mut(&tc.lsid) {
                    s.state = State::Aborted;
                }
                Err(e)
            }
        }
    }

    /// Give the transaction back after a command, or abort it if the command failed.
    pub(crate) async fn txn_release(&self, tc: &TxnCtx, tx: Option<Txn>, command_ok: bool) {
        let Some(t) = tx else { return };
        if command_ok {
            if let Some(s) = self.sessions.m.lock().unwrap().get_mut(&tc.lsid) {
                s.state = State::Active(Some(t));
            }
            return;
        }
        {
            if let Some(s) = self.sessions.m.lock().unwrap().get_mut(&tc.lsid) {
                s.state = State::Aborted;
            }
        }
        let mut g = self.dbs.lock(&t.db).await;
        g.end_txn(t.id);
    }

    pub(crate) async fn commit_txn(&self, cmd: &Document) -> R<Document> {
        let Some(tc) = parse(cmd)? else {
            return Err(MongoError::new(72, "InvalidOptions", "commitTransaction needs autocommit: false".into()));
        };
        let txn = {
            let retry_n = self.retry_session_number(&tc.lsid).await;
            let unknown = {
                let mut m = self.sessions.m.lock().unwrap();
                match m.get_mut(&tc.lsid) {
                    None => Some(retry_n),
                    Some(s) if s.number > tc.number => return Err(too_old(&tc, s.number)),
                    Some(s) if s.number != tc.number => Some(s.number.max(retry_n)),
                    Some(_) => None,
                }
            };
            // this process does not remember the transaction (a restart, or it ran on another process): its commit may
            // still be stored; a repeated commit of a committed transaction answers ok, as on mongod
            if let Some(active) = unknown {
                let stored = self.txn_stored_outcome(&tc.lsid, tc.number).await?;
                return if stored.committed {
                    // also when a later number of the session exists (mongod answers 225 there): the commit is stored,
                    // and this process answers ok for a remembered committed transaction in the same situation
                    Ok(ok())
                } else if stored.newest > tc.number {
                    Err(too_old(&tc, stored.newest))
                } else {
                    Err(no_such(given_msg(&tc, active)))
                };
            }
            let taken = {
                let mut m = self.sessions.m.lock().unwrap();
                let Some(s) = m.get_mut(&tc.lsid) else {
                    return Err(no_such(given_msg(&tc, retry_n)));
                };
                if s.number != tc.number {
                    return Err(no_such(given_msg(&tc, s.number.max(retry_n))));
                }
                match std::mem::replace(&mut s.state, State::Active(None)) {
                    State::Active(Some(t)) => {
                        let (tx, rx) = tokio::sync::watch::channel(Phase::Staging);
                        s.state = State::Committing(rx);
                        Ok((t, tx))
                    }
                    State::Active(None) => return Err(MongoError::new(117, "ConflictingOperationInProgress", "Another operation of this transaction is in progress".into())),
                    // a commit of this transaction is running (or its durability wait failed): wait for the same durability
                    State::Committing(rx) => {
                        s.state = State::Committing(rx.clone());
                        Err(rx)
                    }
                    State::Committed => {
                        s.state = State::Committed;
                        return Ok(ok());
                    }
                    State::Aborted => {
                        s.state = State::Aborted;
                        return Err(no_such(format!("Transaction with {{ txnNumber: {} }} has been aborted.", tc.number)));
                    }
                }
            };
            match taken {
                Ok(t) => t,
                Err(rx) => return self.commit_wait(&tc, rx).await,
            }
        };
        let (txn, phase) = txn;
        let mut guard = StagingGuard { sessions: &self.sessions, lsid: &tc.lsid, number: tc.number, armed: true };
        let txn_id = txn.id;
        let mut txn = txn;
        let mut events = std::mem::take(&mut txn.w.events);
        let mut lsid = txn.lsid_doc.clone();
        if !lsid.contains_key("uid") {
            lsid.insert("uid", Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55] }));
        }
        for e in events.iter_mut() {
            e.txn = Some((txn.number, lsid.clone()));
        }
        // a commit that writes stores its outcome in its own batch (retry.rs); the session's hint goes first
        let writes = txn.w.has_writes();
        let hinted = if writes { self.txn_hint(&tc.lsid, &txn.db).await } else { Ok(()) };
        let db = txn.db.clone();
        let mut g = self.dbs.lock(&db).await;
        let conflict = txn.w.ov.keys().any(|k| g.modified_after(k, txn.seq));
        let result = if let Err(e) = hinted {
            Err(e)
        } else if conflict {
            Err(MongoError::write_conflict())
        } else {
            match txn.w.final_ops() {
                Ok(mut ops) => {
                    if writes {
                        ops.push(crate::retry::txn_outcome_op(&tc.lsid, txn.number));
                        ops.extend(self.session_sweep_ops(&mut g, &db, &tc.lsid).await);
                    }
                    g.apply(ops).await.map_err(MongoError::from).map(|_| self.dbs.changes.publish(events))
                }
                Err(e) => Err(e),
            }
        };
        // the batch of this commit (when it wrote): durable once this clone of its commit is
        let staged = if result.is_ok() { crate::dbs::take_staged_commit() } else { None };
        g.end_txn(txn_id);
        drop(g);
        guard.armed = false;
        if let Err(e) = result {
            if let Some(s) = self.sessions.m.lock().unwrap().get_mut(&tc.lsid) {
                if s.number == tc.number {
                    s.state = State::Aborted;
                }
            }
            return Err(e);
        }
        // repeated commits arriving from now on wait for this durability, not for this command
        phase.send_replace(Phase::Staged(staged.clone()));
        if let Some(c) = staged {
            c.wait().await.map_err(MongoError::from)?;
        }
        self.mark_committed(&tc);
        Ok(ok())
    }

    fn mark_committed(&self, tc: &TxnCtx) {
        if let Some(s) = self.sessions.m.lock().unwrap().get_mut(&tc.lsid) {
            if s.number == tc.number && matches!(s.state, State::Committing(_)) {
                s.state = State::Committed;
            }
        }
    }

    /// A commit repeated while a commit of the same transaction runs, or after its durability wait failed: wait until the
    /// first commit's batch is durable (as mongod waits for the write concern of a repeated commit), then answer ok.
    async fn commit_wait(&self, tc: &TxnCtx, mut rx: tokio::sync::watch::Receiver<Phase>) -> R<Document> {
        loop {
            let phase = rx.borrow_and_update().clone();
            match phase {
                Phase::Staged(c) => {
                    if let Some(c) = c {
                        c.wait().await.map_err(MongoError::from)?;
                    }
                    self.mark_committed(tc);
                    return Ok(ok());
                }
                Phase::Staging => {
                    if rx.changed().await.is_ok() || matches!(*rx.borrow(), Phase::Staged(_)) {
                        continue;
                    }
                    // the first commit ended without staging anything (a conflict, an error, or it was dropped)
                    let m = self.sessions.m.lock().unwrap();
                    return match m.get(&tc.lsid) {
                        Some(s) if s.number == tc.number => match &s.state {
                            State::Committed => Ok(ok()),
                            State::Committing(r) if matches!(*r.borrow(), Phase::Staged(_)) => {
                                rx = r.clone();
                                drop(m);
                                continue;
                            }
                            _ => Err(no_such(format!("Transaction with {{ txnNumber: {} }} has been aborted.", tc.number))),
                        },
                        Some(s) => Err(no_such(given_msg(tc, s.number))),
                        None => Err(no_such(given_msg(tc, -1))),
                    };
                }
            }
        }
    }

    pub(crate) async fn abort_txn(&self, cmd: &Document) -> R<Document> {
        let Some(tc) = parse(cmd)? else {
            return Err(MongoError::new(72, "InvalidOptions", "abortTransaction needs autocommit: false".into()));
        };
        let taken = {
            let retry_n = self.retry_session_number(&tc.lsid).await;
            let known = self.sessions.m.lock().unwrap().get(&tc.lsid).map(|s| s.number);
            if known.map_or(true, |n| n < tc.number) {
                // not remembered here: an abort of a transaction whose commit is stored is refused, as on mongod
                if self.txn_stored_outcome(&tc.lsid, tc.number).await?.committed {
                    return Err(MongoError::new(256, "TransactionCommitted", format!("Transaction {} has been committed.", tc.number)));
                }
            }
            let mut m = self.sessions.m.lock().unwrap();
            let Some(s) = m.get_mut(&tc.lsid) else {
                return Err(no_such(given_msg(&tc, retry_n)));
            };
            if s.number > tc.number {
                return Err(too_old(&tc, s.number));
            }
            if s.number != tc.number {
                return Err(no_such(given_msg(&tc, s.number.max(retry_n))));
            }
            match std::mem::replace(&mut s.state, State::Aborted) {
                State::Active(Some(t)) => Some(t),
                State::Active(None) => {
                    s.state = State::Active(None);
                    return Err(MongoError::new(117, "ConflictingOperationInProgress", "Another operation of this transaction is in progress".into()));
                }
                State::Committed => {
                    s.state = State::Committed;
                    return Err(MongoError::new(256, "TransactionCommitted", format!("Transaction {} has been committed.", tc.number)));
                }
                committing @ State::Committing(_) => {
                    s.state = committing;
                    return Err(MongoError::new(117, "ConflictingOperationInProgress", "A commit of this transaction is in progress".into()));
                }
                State::Aborted => {
                    return Err(no_such(format!("Transaction with {{ txnNumber: {} }} has been aborted.", tc.number)));
                }
            }
        };
        if let Some(t) = taken {
            let mut g = self.dbs.lock(&t.db).await;
            g.end_txn(t.id);
        }
        Ok(ok())
    }

    /// `endSessions`: abort the sessions' open transactions and forget them.
    pub(crate) async fn end_sessions(&self, cmd: &Document) -> R<Document> {
        let ids: Vec<String> = cmd.get_array("endSessions").map(|a| a.iter().filter_map(|b| b.as_document().and_then(|d| match d.get("id") { Some(Bson::Binary(b)) => Some(hex(&b.bytes)), _ => None })).collect()).unwrap_or_default();
        let mut open = Vec::new();
        {
            let mut m = self.sessions.m.lock().unwrap();
            for id in ids {
                // a running commit keeps its session: its repeated commits must find it and wait for its durability
                if matches!(m.get(&id).map(|s| &s.state), Some(State::Committing(_))) {
                    continue;
                }
                if let Some(Session { state: State::Active(Some(t)), .. }) = m.remove(&id) {
                    open.push((t.db.clone(), t.id));
                }
            }
        }
        for (db, id) in open {
            let mut g = self.dbs.lock(&db).await;
            g.end_txn(id);
        }
        Ok(ok())
    }

    /// Abort transactions that outlived `transactionLifetimeLimitSeconds`.
    pub fn spawn_txn_janitor(self: &Arc<Self>) {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let mut dead = Vec::new();
                {
                    let mut m = me.sessions.m.lock().unwrap();
                    for s in m.values_mut() {
                        if let State::Active(Some(t)) = &s.state {
                            if t.started.elapsed() >= Duration::from_secs(LIFETIME_SECS.load(std::sync::atomic::Ordering::Relaxed)) {
                                if let State::Active(Some(t)) = std::mem::replace(&mut s.state, State::Aborted) {
                                    dead.push((t.db, t.id));
                                }
                            }
                        }
                    }
                }
                for (db, id) in dead {
                    let mut g = me.dbs.lock(&db).await;
                    g.end_txn(id);
                }
            }
        });
    }

    /// Run one data command of a transaction.
    pub(crate) async fn run_in_txn(&self, db: &str, name: &str, cmd: &Document, tc: &TxnCtx) -> R<Document> {
        if !matches!(name, "insert" | "update" | "delete" | "findAndModify" | "findandmodify" | "find" | "aggregate" | "count" | "distinct" | "getMore") {
            return Err(MongoError::new(263, "OperationNotSupportedInTransaction", format!("Cannot run '{name}' in a multi-document transaction.")));
        }
        let txn = self.txn_acquire(db, tc).await?;
        if txn.db != db {
            self.txn_release(tc, Some(txn), false).await;
            return Err(MongoError::new(263, "OperationNotSupportedInTransaction", "Cannot use more than one database in a multi-document transaction on this server.".into()));
        }
        let mut tx = Some(txn);
        // The same collation scope as outside a transaction (`cmd.rs`, before `route`): the command's own collation, or else
        // the default of the collection it works on. Without it a transaction compared strings with the simple collation.
        let col = if matches!(name, "find" | "aggregate" | "count" | "distinct" | "findAndModify" | "findandmodify") { crate::collation::of_command(name, cmd) } else { Ok(None) };
        let r = match col {
            Err(e) => Err(e),
            Ok(col) => {
                crate::collation::scope(col, async {
                    match name {
                        "insert" => self.insert(db, cmd, &mut tx).await,
                        "find" => self.find(db, cmd, &mut tx).await,
                        "getMore" => self.get_more(db, cmd).await,
                        "update" => self.update(db, cmd, &mut tx).await,
                        "delete" => self.delete(db, cmd, &mut tx).await,
                        "findAndModify" | "findandmodify" => self.find_and_modify(db, cmd, &mut tx).await,
                        "count" => self.count(db, cmd, &mut tx).await,
                        "distinct" => self.distinct(db, cmd, &mut tx).await,
                        _ => self.aggregate(db, cmd, &mut tx).await,
                    }
                })
                .await
            }
        };
        let failed = match &r {
            Ok(d) => d.get_array("writeErrors").map(|a| !a.is_empty()).unwrap_or(false),
            Err(_) => true,
        };
        self.txn_release(tc, tx, !failed).await;
        r
    }
}
