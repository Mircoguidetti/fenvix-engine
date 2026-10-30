//! Backend 5: three log nodes (one per AZ), each with a local log file (pwrite + fdatasync, as the file backend).
//! The writer sends every batch to all three and acknowledges when two of them answer that it is on their disk.
//!
//! Protocol (TCP, frames `len u32 | type u8 | payload`):
//!   HELLO(log, prealloc_mb)            -> ACK(last seq on this node's disk)
//!   APPEND(log, n, n x (seq, body))    -> ACK(last contiguous seq on disk)   [entries must continue the node's log;
//!                                         anything after a gap is ignored, so a stale node is caught up from the
//!                                         writer's buffer starting at the ACKed seq + 1]
//!   READ(log)                          -> ENTRIES(n x (seq, body))
//!   CRASH(restart_after_ms)            -> the node SIGKILLs itself; its supervisor restarts it after that delay
//! Limits of this protocol (benchmark grade, stated in the report): no epochs/fencing (each writer run uses a new log
//! id and a killed writer is dead), catch-up comes from the writer's in-memory buffer, recovery is the union of the
//! entries found on the nodes it reads (it must read at least 2 of 3 to see every acknowledged write).

use crate::codec;
use crate::counters::{add, C};
use crate::store::{ReadOut, Store};
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Notify};

const T_HELLO: u8 = 1;
const T_APPEND: u8 = 2;
const T_READ: u8 = 3;
const T_CRASH: u8 = 4;
const T_ACK: u8 = 10;
const T_ENTRIES: u8 = 11;

async fn write_msg(s: &mut TcpStream, t: u8, payload: &[u8]) -> Result<usize> {
    let mut b = BytesMut::with_capacity(5 + payload.len());
    b.put_u32_le(payload.len() as u32 + 1);
    b.put_u8(t);
    b.put_slice(payload);
    s.write_all(&b).await?;
    Ok(b.len())
}

async fn read_msg(s: &mut TcpStream) -> Result<(u8, Vec<u8>, usize)> {
    let len = s.read_u32_le().await? as usize;
    if len == 0 || len > 64 << 20 {
        bail!("bad message length {len}");
    }
    let mut buf = vec![0u8; len];
    s.read_exact(&mut buf).await?;
    let t = buf[0];
    buf.remove(0);
    Ok((t, buf, len + 4))
}

fn put_str(b: &mut BytesMut, s: &str) {
    b.put_u16_le(s.len() as u16);
    b.put_slice(s.as_bytes());
}

struct Rd<'a>(&'a [u8], usize);
impl<'a> Rd<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.1 + n > self.0.len() {
            bail!("short message");
        }
        let s = &self.0[self.1..self.1 + n];
        self.1 += n;
        Ok(s)
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into()?))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into()?))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into()?))
    }
    fn str(&mut self) -> Result<String> {
        let n = self.u16()? as usize;
        Ok(String::from_utf8(self.take(n)?.to_vec())?)
    }
}

// ------------------------------------------------------------------------------------------------------------ node

#[allow(dead_code)]
struct NodeLog {
    path: String,
    f: File,
    off: u64,
    last: u64,
}

struct Node {
    dir: String,
    logs: Mutex<HashMap<String, Arc<Mutex<NodeLog>>>>,
}

fn valid_log(s: &str) -> bool {
    !s.is_empty() && s.len() < 100 && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

impl Node {
    fn open(&self, log: &str, prealloc_mb: u32) -> Result<Arc<Mutex<NodeLog>>> {
        if !valid_log(log) {
            bail!("bad log id");
        }
        let mut m = self.logs.lock().unwrap();
        if let Some(l) = m.get(log) {
            return Ok(l.clone());
        }
        let path = format!("{}/{}.wal", self.dir, log);
        let exists = std::path::Path::new(&path).exists();
        let f = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&path)?;
        let (off, last) = if exists {
            let (batches, end, _) = codec::scan_file(&path)?;
            (end as u64, batches.last().map(|b| b.seq).unwrap_or(0))
        } else {
            let zero = vec![0u8; 1 << 20];
            for i in 0..prealloc_mb as u64 {
                f.write_all_at(&zero, i << 20)?;
            }
            f.sync_all()?;
            File::open(&self.dir)?.sync_all()?;
            (0, 0)
        };
        let l = Arc::new(Mutex::new(NodeLog { path, f, off, last }));
        m.insert(log.to_string(), l.clone());
        Ok(l)
    }
}

pub async fn run_node(listen: String, dir: String) -> Result<()> {
    std::fs::create_dir_all(&dir)?;
    let node = Arc::new(Node { dir, logs: Mutex::new(HashMap::new()) });
    let l = TcpListener::bind(&listen).await.with_context(|| format!("bind {listen}"))?;
    eprintln!("node listening on {listen}");
    loop {
        let (s, _) = l.accept().await?;
        s.set_nodelay(true)?;
        let node = node.clone();
        tokio::spawn(async move {
            let _ = serve(node, s).await;
        });
    }
}

async fn serve(node: Arc<Node>, mut s: TcpStream) -> Result<()> {
    loop {
        let (t, p, _) = read_msg(&mut s).await?;
        let mut r = Rd(&p, 0);
        match t {
            T_HELLO => {
                let log = r.str()?;
                let mb = r.u32()?;
                let node = node.clone();
                let last = tokio::task::spawn_blocking(move || -> Result<u64> { Ok(node.open(&log, mb)?.lock().unwrap().last) }).await??;
                write_msg(&mut s, T_ACK, &last.to_le_bytes()).await?;
            }
            T_APPEND => {
                let log = r.str()?;
                let n = r.u32()?;
                let mut es = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    let seq = r.u64()?;
                    let len = r.u32()? as usize;
                    es.push((seq, Bytes::copy_from_slice(r.take(len)?)));
                }
                let node = node.clone();
                let last = tokio::task::spawn_blocking(move || -> Result<u64> {
                    let l = node.open(&log, 0)?;
                    let mut g = l.lock().unwrap();
                    let mut buf = BytesMut::new();
                    let mut last = g.last;
                    for (seq, body) in es {
                        if seq <= last {
                            continue; // already on disk
                        }
                        if seq != last + 1 {
                            break; // gap: the writer resends from last + 1
                        }
                        buf.put_slice(&codec::frame(&body));
                        last = seq;
                    }
                    if !buf.is_empty() {
                        let off = g.off;
                        g.f.write_all_at(&buf, off)?;
                        g.f.sync_data()?; // fdatasync before the ACK
                        g.off = off + buf.len() as u64;
                        g.last = last;
                    }
                    Ok(g.last)
                })
                .await??;
                write_msg(&mut s, T_ACK, &last.to_le_bytes()).await?;
            }
            T_READ => {
                // paged: READ(log, byte offset) -> ENTRIES(n x (seq, body), next offset, done)
                let log = r.str()?;
                let from = r.u64()?;
                let node = node.clone();
                let payload = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
                    if !valid_log(&log) {
                        bail!("bad log id");
                    }
                    let path = format!("{}/{}.wal", node.dir, log);
                    // hold the log's lock (if open) so no append is half-written while we read
                    let l = node.logs.lock().unwrap().get(&log).cloned();
                    let _g = l.as_ref().map(|l| l.lock().unwrap());
                    let mut out = BytesMut::new();
                    let (mut n, mut off, mut done) = (0u32, from, true);
                    let mut body = BytesMut::new();
                    if let Ok(f) = File::open(&path) {
                        let len = f.metadata()?.len();
                        let mut want = 8u64 << 20;
                        loop {
                            let take = want.min(len.saturating_sub(off));
                            let mut buf = vec![0u8; take as usize];
                            f.read_exact_at(&mut buf, off)?;
                            let mut p = 0usize;
                            done = true;
                            while p + 4 <= buf.len() {
                                let fl = u32::from_le_bytes(buf[p..p + 4].try_into().unwrap()) as usize;
                                if fl == 0 {
                                    break;
                                }
                                if p + 4 + fl > buf.len() {
                                    done = off + (p as u64) + 4 + (fl as u64) > len; // torn tail, or just the end of this page
                                    break;
                                }
                                let b = &buf[p + 4..p + 4 + fl];
                                match codec::decode(b) {
                                    Ok(d) => {
                                        body.put_u64_le(d.seq);
                                        body.put_u32_le(fl as u32);
                                        body.put_slice(b);
                                        n += 1;
                                    }
                                    Err(_) => break,
                                }
                                p += 4 + fl;
                            }
                            if n == 0 && !done && want < (64 << 20) {
                                want *= 2; // one frame bigger than the page
                                continue;
                            }
                            off += p as u64;
                            break;
                        }
                    }
                    out.put_u32_le(n);
                    out.put_slice(&body);
                    out.put_u64_le(off);
                    out.put_u8(done as u8);
                    Ok(out.to_vec())
                })
                .await??;
                write_msg(&mut s, T_ENTRIES, &payload).await?;
            }
            T_CRASH => {
                let ms = r.u32()?;
                let _ = std::fs::write(format!("{}/restart_delay_ms", node.dir), ms.to_string());
                eprintln!("CRASH requested: SIGKILL self, restart after {ms} ms");
                unsafe {
                    libc::kill(libc::getpid(), libc::SIGKILL);
                }
            }
            _ => bail!("unknown message {t}"),
        }
    }
}

/// Ask node `addr` to SIGKILL itself (its supervisor restarts it after `restart_after_ms`).
pub async fn crash_node(addr: &str, restart_after_ms: u32) -> Result<()> {
    let mut s = TcpStream::connect(addr).await?;
    write_msg(&mut s, T_CRASH, &restart_after_ms.to_le_bytes()).await?;
    let _ = tokio::time::timeout(Duration::from_secs(2), s.read_u8()).await;
    Ok(())
}

// ---------------------------------------------------------------------------------------------------------- writer

pub struct Shared {
    log: String,
    prealloc_mb: u32,
    buf: Mutex<BTreeMap<u64, Bytes>>,
    head: watch::Sender<u64>,
    pub durable: [AtomicU64; 3],
    pub connected: [AtomicBool; 3],
    /// while set, the replicator of that node stays disconnected (used to keep a returning node stale on purpose)
    pub hold: [AtomicBool; 3],
    acked: Notify,
}

pub struct QuorumStore {
    pub sh: Arc<Shared>,
    nodes: Vec<String>,
    read: Vec<usize>,
    started: std::sync::Once,
}

impl QuorumStore {
    pub fn new(nodes: Vec<String>, read: Vec<usize>, log: String, prealloc_mb: u32) -> Arc<QuorumStore> {
        let (head, _) = watch::channel(0u64);
        let sh = Arc::new(Shared {
            log,
            prealloc_mb,
            buf: Mutex::new(BTreeMap::new()),
            head,
            durable: Default::default(),
            connected: Default::default(),
            hold: Default::default(),
            acked: Notify::new(),
        });
        // replicators start lazily on the first append, so a recovery-only store never connects to write
        Arc::new(QuorumStore { sh, nodes, read, started: std::sync::Once::new() })
    }

    /// Starts the three replicators once (on the first append).
    pub fn start(&self) {
        self.started.call_once(|| {
            for (i, addr) in self.nodes.iter().enumerate() {
            let sh = self.sh.clone();
            let addr = addr.clone();
                tokio::spawn(async move { replicator(sh, i, addr).await });
            }
        });
    }

    pub fn quorum_reached(&self, seq: u64) -> bool {
        self.sh.durable.iter().filter(|d| d.load(SeqCst) >= seq).count() >= 2
    }
}

async fn replicator(sh: Arc<Shared>, i: usize, addr: String) {
    let mut head_rx = sh.head.subscribe();
    loop {
        if sh.hold[i].load(SeqCst) {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        let r: Result<()> = async {
            let mut s = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(&addr)).await??;
            s.set_nodelay(true)?;
            let mut b = BytesMut::new();
            put_str(&mut b, &sh.log);
            b.put_u32_le(sh.prealloc_mb);
            add(&C.q_sent[i], write_msg(&mut s, T_HELLO, &b).await? as u64);
            let (t, p, n) = tokio::time::timeout(Duration::from_secs(30), read_msg(&mut s)).await??;
            add(&C.q_recv[i], n as u64);
            if t != T_ACK {
                bail!("expected ACK");
            }
            let last = Rd(&p, 0).u64()?;
            sh.durable[i].store(last, SeqCst);
            sh.connected[i].store(true, SeqCst);
            sh.acked.notify_waiters();
            let mut next = last + 1;
            loop {
                if sh.hold[i].load(SeqCst) {
                    bail!("held");
                }
                let head = *head_rx.borrow_and_update();
                if next > head {
                    // wait for a new batch (or re-check the hold flag every 50 ms)
                    let _ = tokio::time::timeout(Duration::from_millis(50), head_rx.changed()).await;
                    continue;
                }
                let mut b = BytesMut::new();
                put_str(&mut b, &sh.log);
                let mut n = 0u32;
                let mut body = BytesMut::new();
                {
                    let buf = sh.buf.lock().unwrap();
                    for (seq, e) in buf.range(next..=head) {
                        body.put_u64_le(*seq);
                        body.put_u32_le(e.len() as u32);
                        body.put_slice(e);
                        n += 1;
                        if body.len() > 1 << 20 {
                            break;
                        }
                    }
                }
                b.put_u32_le(n);
                b.put_slice(&body);
                add(&C.q_sent[i], write_msg(&mut s, T_APPEND, &b).await? as u64);
                let (t, p, n) = tokio::time::timeout(Duration::from_secs(10), read_msg(&mut s)).await??;
                add(&C.q_recv[i], n as u64);
                if t != T_ACK {
                    bail!("expected ACK");
                }
                let last = Rd(&p, 0).u64()?;
                sh.durable[i].store(last, SeqCst);
                sh.acked.notify_waiters();
                next = last + 1;
                let all = sh.durable.iter().map(|d| d.load(SeqCst)).min().unwrap_or(0);
                if all > 0 {
                    let mut buf = sh.buf.lock().unwrap();
                    while buf.first_key_value().map(|(k, _)| *k <= all).unwrap_or(false) {
                        buf.pop_first();
                    }
                }
            }
        }
        .await;
        sh.connected[i].store(false, SeqCst);
        if r.is_err() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[async_trait]
impl Store for QuorumStore {
    async fn append(&self, seq: u64, body: Bytes) -> Result<()> {
        self.start();
        self.sh.buf.lock().unwrap().insert(seq, body);
        self.sh.head.send_modify(|h| *h = (*h).max(seq));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let notified = self.sh.acked.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.quorum_reached(seq) {
                return Ok(());
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                add(&C.errors, 1);
                return Err(anyhow!("seq {seq}: no quorum within 30 s"));
            }
        }
    }

    async fn read_all(&self) -> Result<ReadOut> {
        let mut merged: BTreeMap<u64, Bytes> = BTreeMap::new();
        let mut out = ReadOut::default();
        for &i in &self.read {
            let addr = self.nodes[i].clone();
            let mut s = tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(&addr))
                .await
                .map_err(|_| anyhow!("node {i}: connect timeout"))?
                .with_context(|| format!("node {i}: connect"))?;
            let mut off = 0u64;
            loop {
                let mut b = BytesMut::new();
                put_str(&mut b, &self.sh.log);
                b.put_u64_le(off);
                add(&C.q_sent[i], write_msg(&mut s, T_READ, &b).await? as u64);
                let (t, p, n) = tokio::time::timeout(Duration::from_secs(60), read_msg(&mut s)).await??;
                add(&C.q_recv[i], n as u64);
                if t != T_ENTRIES {
                    bail!("node {i}: expected ENTRIES");
                }
                let mut r = Rd(&p, 0);
                let cnt = r.u32()?;
                for _ in 0..cnt {
                    let seq = r.u64()?;
                    let len = r.u32()? as usize;
                    let data = r.take(len)?;
                    merged.entry(seq).or_insert_with(|| Bytes::copy_from_slice(data));
                }
                let next = r.u64()?;
                let done = r.take(1)?[0] == 1;
                if done || cnt == 0 || next == off {
                    break;
                }
                off = next;
            }
        }
        for (seq, b) in merged {
            out.objects += 1;
            match codec::decode(&b) {
                Ok(d) => out.batches.push(d),
                Err(e) => out.bad.push(format!("seq {seq}: {e}")),
            }
        }
        Ok(out)
    }
}
