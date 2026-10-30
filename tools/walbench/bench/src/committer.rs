//! Group commit in front of a store.
//!
//! Writes are queued; a batch is cut when its window has elapsed since its first write (window 0 = as soon as a commit
//! slot is free, taking everything already queued), or when it is full. Up to `max_inflight` commits run at once and
//! acknowledgements are released strictly in log order: a write is acknowledged only when its batch AND every earlier
//! batch are durable, as a WAL requires (an acked write implies every earlier write is durable).
//!   perwrite: window 0, 1 write per commit, many commits in flight (one PUT / PutItem per write)
//!   wN:       window N ms, many commits in flight
//!   w0:       window 0, ONE commit in flight (classic group commit: what queues during a commit goes in the next)

use crate::codec::{self, Entry};
use crate::store::Store;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use futures::stream::{FuturesOrdered, StreamExt};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct Mode {
    pub window_us: u64,
    pub max_count: usize,
    pub max_bytes: usize,
    pub max_inflight: usize,
}

impl Mode {
    /// "perwrite", "w0", "w1", "w5", "w20" (ms)
    pub fn parse(s: &str, max_bytes: usize, serial_store: bool) -> Result<Mode> {
        let inflight = if serial_store { 1 } else { 64 };
        match s {
            "perwrite" => {
                if serial_store {
                    return Err(anyhow!("perwrite needs a store that accepts concurrent commits (s3, express, ddb)"));
                }
                Ok(Mode { window_us: 0, max_count: 1, max_bytes, max_inflight: 1024 })
            }
            _ if s.starts_with('w') => {
                let ms: f64 = s[1..].parse().map_err(|_| anyhow!("bad variant {s}"))?;
                let window_us = (ms * 1000.0) as u64;
                Ok(Mode { window_us, max_count: 100_000, max_bytes, max_inflight: if window_us == 0 { 1 } else { inflight } })
            }
            _ => Err(anyhow!("bad variant {s}")),
        }
    }
}

struct Req {
    e: Entry,
    ack: oneshot::Sender<Result<(), String>>,
}

#[derive(Clone)]
pub struct Committer {
    tx: mpsc::UnboundedSender<Req>,
    pub stats: Arc<BatchStats>,
}

#[derive(Default)]
pub struct BatchStats {
    pub batches: AtomicU64,
    pub entries: AtomicU64,
    pub bytes: AtomicU64,
}

impl Committer {
    pub fn new(store: Arc<dyn Store>, mode: Mode) -> Committer {
        let (tx, rx) = mpsc::unbounded_channel();
        let stats = Arc::new(BatchStats::default());
        tokio::spawn(run(store, mode, rx, stats.clone()));
        Committer { tx, stats }
    }

    /// Resolves when the write is durable (and every earlier write too).
    pub async fn write(&self, db: u32, id: u64, data: Bytes) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.tx.send(Req { e: Entry { db, id, data }, ack }).map_err(|_| anyhow!("committer stopped"))?;
        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(anyhow!(e)),
            Err(_) => Err(anyhow!("committer dropped the write")),
        }
    }
}

fn unsafe_early_ack() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *V.get_or_init(|| std::env::var("WALBENCH_UNSAFE_EARLY_ACK").as_deref() == Ok("1"))
}

async fn run(store: Arc<dyn Store>, mode: Mode, mut rx: mpsc::UnboundedReceiver<Req>, stats: Arc<BatchStats>) {
    let mut seq = 0u64;
    let mut pending: Vec<Req> = Vec::new();
    let mut pending_bytes = 0usize;
    let mut deadline = Instant::now();
    let mut inflight = FuturesOrdered::new();
    let mut open = true;
    let mut failed: Option<String> = None;
    loop {
        // cut and start batches while allowed
        loop {
            let full = pending.len() >= mode.max_count || pending_bytes >= mode.max_bytes;
            let due = !pending.is_empty() && (full || Instant::now() >= deadline || !open);
            if !(due && inflight.len() < mode.max_inflight) {
                break;
            }
            if mode.window_us == 0 {
                // take what is already queued (up to the limits)
                while pending.len() < mode.max_count && pending_bytes < mode.max_bytes {
                    match rx.try_recv() {
                        Ok(r) => {
                            pending_bytes += r.e.data.len() + 16;
                            pending.push(r);
                        }
                        Err(_) => break,
                    }
                }
            }
            let mut take = 0;
            let mut bytes = 0;
            while take < pending.len() && take < mode.max_count && (take == 0 || bytes + pending[take].e.data.len() + 16 <= mode.max_bytes) {
                bytes += pending[take].e.data.len() + 16;
                take += 1;
            }
            let batch: Vec<Req> = pending.drain(..take).collect();
            pending_bytes -= bytes;
            seq += 1;
            let entries: Vec<Entry> = batch.iter().map(|r| r.e.clone()).collect();
            let body = codec::encode(seq, &entries);
            stats.batches.fetch_add(1, Relaxed);
            stats.entries.fetch_add(entries.len() as u64, Relaxed);
            stats.bytes.fetch_add(body.len() as u64, Relaxed);
            let st = store.clone();
            if unsafe_early_ack() {
                // NEGATIVE CONTROL ONLY (crash test must detect it): acknowledge before the commit is even sent
                let mut batch = batch;
                for req in batch.drain(..) {
                    let _ = req.ack.send(Ok(()));
                }
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let _ = st.append(seq, body).await;
                });
                continue;
            }
            inflight.push_back(async move {
                let r = st.append(seq, body).await;
                (batch, r)
            });
        }
        if !open && pending.is_empty() && inflight.is_empty() {
            return;
        }
        let wait_deadline = if !pending.is_empty() && inflight.len() < mode.max_inflight { Some(deadline) } else { None };
        tokio::select! {
            biased;
            Some((batch, r)) = inflight.next(), if !inflight.is_empty() => {
                // in-order release: FuturesOrdered yields batches in the order they were started; after a failure
                // nothing later is acknowledged as durable
                if let Err(e) = &r { failed.get_or_insert_with(|| format!("{e:#}")); }
                for req in batch {
                    let _ = req.ack.send(match &failed { None => Ok(()), Some(f) => Err(f.clone()) });
                }
            }
            r = rx.recv(), if open => match r {
                Some(r) => {
                    if pending.is_empty() {
                        deadline = Instant::now() + Duration::from_micros(mode.window_us);
                    }
                    pending_bytes += r.e.data.len() + 16;
                    pending.push(r);
                }
                None => open = false,
            },
            _ = async { tokio::time::sleep_until(wait_deadline.unwrap()).await }, if wait_deadline.is_some() => {}
        }
    }
}
