//! Q8d · the replicated log ("quaderno", `proto/quaderno`) as the durability of the local log (`locallog.rs`).
//!
//! With `--quaderno <a:p,b:p,c:p>` a write is acknowledged when its record is in the log on 2 copies out of 3 (the log's
//! leader confirms after a majority fsynced it), instead of after an fsync on this machine's disk. Object storage gets
//! the data later, in the background (the local log's uploader, every --local-log-upload-secs); after each upload the
//! engine tells the log "records of this database up to position P are in object storage" (`FlushDone`) and the log
//! drops them. At restart, before accepting connections, the engine reads back from the log every record not yet
//! confirmed (`Pending` + `FlushableFrom`) and applies it (the local log's replay, with its marker check), wherever
//! the engine restarts: the records are not on this machine's disk.
//!
//! This module is the client side only: batching of records into `CommitBatch` messages (records of many databases
//! share one message and one round of the log), the position each database reached, confirmations, and reading back.

use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use quaderno::client::{Cluster, Outcome};
use quaderno::codec::{CommitReq, Db, Kind, Mode, Pos, Req, Resp, Write};
use sha2::Digest;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// Records sent to the log, batches sent, records refused or with an unknown outcome.
pub static SENT: AtomicU64 = AtomicU64::new(0);
pub static BATCHES: AtomicU64 = AtomicU64::new(0);
pub static FAILED: AtomicU64 = AtomicU64::new(0);
pub static CONFIRMED_FLUSH: AtomicU64 = AtomicU64::new(0);

/// How long one record may take to be confirmed (leader changes included) before the write gets an error.
const COMMIT_DEADLINE: Duration = Duration::from_secs(10);
/// At most this many records in one `CommitBatch`, and this many batches in flight.
const BATCH_MAX: usize = 512;
const INFLIGHT_MAX: usize = 64;

/// The log's id for a database name (16 bytes of its SHA-256). The name is also inside every record.
pub fn db_key(name: &str) -> Db {
    let h = sha2::Sha256::digest(name.as_bytes());
    let mut d = [0u8; 16];
    d.copy_from_slice(&h[..16]);
    d
}

struct Item {
    db: String,
    req: CommitReq,
    done: oneshot::Sender<Result<Pos, String>>,
    /// save-p99 tracing (FX_WAITS): the writing command's waits and the enqueue instant
    trace: Option<(Arc<tenant_bench::waits::Waits>, std::time::Instant)>,
}

pub struct Remote {
    cl: Arc<Cluster>,
    tx: mpsc::UnboundedSender<Item>,
    /// Highest log position of a confirmed record, per database.
    committed: Mutex<HashMap<String, Pos>>,
    /// Highest position already confirmed to the log as in object storage, per database.
    flushed: Mutex<HashMap<String, Pos>>,
}

impl Remote {
    /// Connect to the three copies (`a:p,b:p,c:p`). Starts the batching task on the current runtime.
    pub fn start(addrs: &str) -> Result<Arc<Remote>> {
        let addrs: Vec<String> = addrs.split(',').filter(|x| !x.is_empty()).map(String::from).collect();
        if addrs.len() != 3 {
            bail!("--quaderno needs the three copies of the log, a:p,b:p,c:p");
        }
        let cl = Cluster::new(addrs, 4);
        let (tx, rx) = mpsc::unbounded_channel();
        let me = Arc::new(Remote { cl, tx, committed: Mutex::new(HashMap::new()), flushed: Mutex::new(HashMap::new()) });
        tokio::spawn(Arc::clone(&me).batcher(rx));
        Ok(me)
    }

    /// Wait until the log has a leader that answers (at most `wait`).
    pub async fn ready(&self, wait: Duration) -> Result<()> {
        match self.cl.call_leader(&Req::Confirmed, wait).await {
            Some(Resp::Pos(_)) => Ok(()),
            other => bail!("the log (quaderno) has no leader that answers after {} s: {}", wait.as_secs(), other.map(|r| format!("{r:?}")).unwrap_or_else(|| "no answer".into())),
        }
    }

    /// Group the records waiting into one `CommitBatch` (all databases together) and send it; up to `INFLIGHT_MAX` batches
    /// at once. Each record resolves with its position (confirmed on 2 copies of 3) or an error.
    async fn batcher(self: Arc<Self>, mut rx: mpsc::UnboundedReceiver<Item>) {
        let sem = Arc::new(tokio::sync::Semaphore::new(INFLIGHT_MAX));
        while let Some(first) = rx.recv().await {
            let permit = sem.clone().acquire_owned().await.expect("semaphore");
            let mut batch = vec![first];
            while batch.len() < BATCH_MAX {
                match rx.try_recv() {
                    Ok(x) => batch.push(x),
                    Err(_) => break,
                }
            }
            let me = Arc::clone(&self);
            tokio::spawn(async move {
                let _p = permit;
                SENT.fetch_add(batch.len() as u64, Relaxed);
                BATCHES.fetch_add(1, Relaxed);
                let reqs: Vec<CommitReq> = batch.iter().map(|i| i.req.clone()).collect();
                let t_sent = std::time::Instant::now();
                let out = me.cl.commit_batch(&reqs, COMMIT_DEADLINE).await;
                let rtt = t_sent.elapsed();
                let bn = batch.len();
                for item in &batch {
                    if let Some((w, t0)) = &item.trace {
                        use tenant_bench::waits::{add_to, W};
                        add_to(w, W::SvLogQueue, t_sent.saturating_duration_since(*t0));
                        add_to(w, W::SvLogRtt, rtt);
                        add_to(w, W::SvLogBatch, Duration::from_millis(bn as u64));
                    }
                }
                for (item, o) in batch.into_iter().zip(out) {
                    let r = match o {
                        Outcome::Committed { pos, .. } => {
                            let mut c = me.committed.lock().unwrap();
                            let e = c.entry(item.db.clone()).or_insert(0);
                            *e = (*e).max(pos);
                            Ok(pos)
                        }
                        Outcome::Conflict(_) => Err("the log refused the record (conflict)".to_string()),
                        Outcome::Unknown => Err("the log did not confirm the record in time; its outcome is unknown".to_string()),
                        Outcome::Rejected(r) => Err(format!("the log is overloaded (reason {r}); the record was not written")),
                    };
                    if r.is_err() {
                        FAILED.fetch_add(1, Relaxed);
                    }
                    let _ = item.done.send(r);
                }
            });
        }
    }

    /// Send one record of `db` (bytes as written by the local log's `encode`). `key` must be unique per record.
    pub fn append(&self, db: &str, key: Vec<u8>, bytes: Vec<u8>) -> oneshot::Receiver<Result<Pos, String>> {
        let (done, rx) = oneshot::channel();
        let req = CommitReq { txn: rand::random(), db: db_key(db), read_pos: 0, mode: Mode::Blind, writes: vec![Write { key: Bytes::from(key), kind: Kind::Put, val: Bytes::from(bytes) }], reads: vec![], session: None };
        if let Err(e) = self.tx.send(Item { db: db.to_string(), req, done, trace: tenant_bench::waits::current().map(|w| (w, std::time::Instant::now())) }) {
            let _ = e.0.done.send(Err("the log client stopped".into()));
        }
        rx
    }

    /// Highest confirmed position of `db` (0: none in this run).
    pub fn committed_pos(&self, db: &str) -> Pos {
        self.committed.lock().unwrap().get(db).copied().unwrap_or(0)
    }

    /// Tell the log that the records of `db` up to `pos` are in object storage (the log may drop them).
    pub async fn flush_done(&self, db: &str, pos: Pos) -> Result<()> {
        if pos == 0 || self.flushed.lock().unwrap().get(db).map_or(false, |f| *f >= pos) {
            return Ok(());
        }
        match self.cl.call_leader(&Req::FlushDone { items: vec![(db_key(db), pos, Bytes::from_static(b"engine"))] }, Duration::from_secs(10)).await {
            Some(Resp::Committed(_)) => {
                CONFIRMED_FLUSH.fetch_add(1, Relaxed);
                let mut f = self.flushed.lock().unwrap();
                let e = f.entry(db.to_string()).or_insert(0);
                *e = (*e).max(pos);
                Ok(())
            }
            other => Err(anyhow!("the log did not take the confirmation of {db}: {}", other.map(|r| format!("{r:?}")).unwrap_or_else(|| "no answer".into()))),
        }
    }

    /// The leader's confirmed position, and a copy of the log that has applied every record up to it.
    ///
    /// The lists of pending records (`Pending`, `FlushableFrom`) are answered by any copy from the records it has applied
    /// to its window, which can lag behind what the leader confirmed (a busy copy, a follower answering a stale leader
    /// guess). A restart right after a kill read such a list without the last confirmed records (acknowledged writes),
    /// served the database without them, and its next writer epoch then made them a "conflict" that no restart applies
    /// (found by the log-path fix's kill test under load). So: ask the leader for its confirmed position (a ReadIndex
    /// round), then wait (`Wait`) until one copy has applied up to it, and read the lists from that copy only.
    async fn caught_up(&self) -> Result<(usize, Pos)> {
        let upto = match self.cl.call_leader(&Req::Confirmed, Duration::from_secs(10)).await {
            Some(Resp::Pos(p)) => p,
            other => bail!("the log did not give its confirmed position: {}", other.map(|r| format!("{r:?}")).unwrap_or_else(|| "no answer".into())),
        };
        for _ in 0..3 {
            for r in 0..3 {
                if let Ok(Resp::Pos(p)) = self.cl.call(r, &Req::Wait { pos: upto, timeout_ms: 5000 }, Duration::from_secs(6)).await {
                    if p >= upto {
                        return Ok((r, upto));
                    }
                }
            }
        }
        bail!("no copy of the log applied its records up to the confirmed position {upto}")
    }

    /// `q` to copy `r` of the log.
    async fn call_copy(&self, r: usize, q: &Req, timeout: Duration) -> Option<Resp> {
        self.cl.call(r, q, timeout).await.ok()
    }

    /// Every record still in the log (not confirmed as in object storage), of every database, with its position, in
    /// position order per database. Read at restart only.
    pub async fn read_pending(&self) -> Result<Vec<(Db, Vec<(Pos, Bytes)>)>> {
        let (copy, upto) = self.caught_up().await?;
        let list = match self.call_copy(copy, &Req::Pending { limit: u32::MAX }, Duration::from_secs(30)).await {
            Some(Resp::Pending(v)) => v,
            other => bail!("the log did not list its pending records: {}", other.map(|r| format!("{r:?}")).unwrap_or_else(|| "no answer".into())),
        };
        let mut out = Vec::new();
        for p in list {
            let mut recs: Vec<(Pos, Bytes)> = Vec::new();
            let mut from: Pos = 0;
            loop {
                let r = self.call_copy(copy, &Req::FlushableFrom { db: p.db, from, upto }, Duration::from_secs(30)).await;
                let page = match r {
                    Some(Resp::Flushable(v)) => v.into_iter().flat_map(|(_, rs)| rs).collect::<Vec<_>>(),
                    other => bail!("the log did not give the records of a database: {}", other.map(|r| format!("{r:?}")).unwrap_or_else(|| "no answer".into())),
                };
                if page.is_empty() {
                    break;
                }
                for fr in page {
                    from = from.max(fr.pos);
                    for w in fr.writes {
                        recs.push((fr.pos, w.val));
                    }
                }
            }
            if !recs.is_empty() {
                out.push((p.db, recs));
            }
        }
        Ok(out)
    }

    /// The records of one database still in the log (not confirmed as in object storage), in position order. Read by a
    /// warm spare when it takes the database over (`locallog::replay_on_wake`).
    pub async fn read_db(&self, name: &str) -> Result<Vec<(Pos, Bytes)>> {
        let (copy, upto) = self.caught_up().await?;
        let mut recs: Vec<(Pos, Bytes)> = Vec::new();
        let mut from: Pos = 0;
        loop {
            let r = self.call_copy(copy, &Req::FlushableFrom { db: db_key(name), from, upto }, Duration::from_secs(30)).await;
            let page = match r {
                Some(Resp::Flushable(v)) => v.into_iter().flat_map(|(_, rs)| rs).collect::<Vec<_>>(),
                other => bail!("the log did not give the records of {name}: {}", other.map(|r| format!("{r:?}")).unwrap_or_else(|| "no answer".into())),
            };
            if page.is_empty() {
                break;
            }
            for fr in page {
                from = from.max(fr.pos);
                for w in fr.writes {
                    recs.push((fr.pos, w.val));
                }
            }
        }
        Ok(recs)
    }

    /// Mark positions already known as confirmed for `db` (records read back at restart).
    pub fn note_committed(&self, db: &str, pos: Pos) {
        let mut c = self.committed.lock().unwrap();
        let e = c.entry(db.to_string()).or_insert(0);
        *e = (*e).max(pos);
    }
}
