//! `proto`: the walbench quorum write path (tools/walbench/bench/src/quorum.rs) on the common test bed.
//! The writer is co-located with node 0. It assigns a sequence number, keeps the entry in a buffer, and one replicator
//! per node sends everything new in one APPEND, waits for that node's ACK (after its fdatasync), and repeats (one
//! APPEND in flight per node, as in walbench). A write is acknowledged when 2 of 3 nodes have it durable, in order.
//! No epochs, no fencing, no consistency check: the reference point for "not below the prototype".
use bytes::Bytes;
use j3z_harness::{closed_loop, frame, idle, Args, DelayLine, LogFile, WriteFn};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use tokio::sync::{watch, Notify};

struct Shared {
    buf: Mutex<BTreeMap<u64, Bytes>>,
    next: AtomicU64,
    head: watch::Sender<u64>,
    durable: [AtomicU64; 3],
    acked: Notify,
}

impl Shared {
    fn quorum(&self) -> u64 {
        let mut d: Vec<u64> = self.durable.iter().map(|x| x.load(SeqCst)).collect();
        d.sort_unstable();
        d[1]
    }
}

async fn replicator(sh: Arc<Shared>, dl: Arc<DelayLine>, i: usize, file: Arc<LogFile>) {
    let mut rx = sh.head.subscribe();
    let mut next = 1u64;
    loop {
        let head = *rx.borrow_and_update();
        if next > head {
            if rx.changed().await.is_err() {
                return;
            }
            continue;
        }
        let mut bytes = Vec::new();
        let mut last = next;
        {
            let buf = sh.buf.lock().unwrap();
            for (s, e) in buf.range(next..=head) {
                bytes.extend_from_slice(&s.to_le_bytes());
                bytes.extend_from_slice(&frame(e));
                last = *s;
            }
        }
        let size = bytes.len();
        let (tx, rx_ack) = tokio::sync::oneshot::channel::<u64>();
        let (dl2, file2) = (dl.clone(), file.clone());
        // writer -> node i (delay), node writes + fdatasync, node -> writer (delay)
        dl.send(0, i, size, Box::new(move || {
            let dl3 = dl2.clone();
            file2.append(bytes, Box::new(move || {
                dl3.send(i, 0, 16, Box::new(move || {
                    let _ = tx.send(last);
                }));
            }));
        }));
        let Ok(d) = rx_ack.await else { return };
        sh.durable[i].store(d, SeqCst);
        sh.acked.notify_waiters();
        next = d + 1;
        let all = sh.durable.iter().map(|x| x.load(SeqCst)).min().unwrap();
        let mut buf = sh.buf.lock().unwrap();
        while buf.first_key_value().map_or(false, |(k, _)| *k <= all) {
            buf.pop_first();
        }
    }
}

#[tokio::main]
async fn main() {
    let a = Args::parse();
    let dir = a.fresh_dir();
    let dl = DelayLine::new(a.delay_cfg());
    let groups = if a.mode == "idle" { a.groups } else { 1 };
    let mut writes: Vec<WriteFn> = Vec::new();
    for g in 0..groups {
        let (head, _) = watch::channel(0u64);
        let sh = Arc::new(Shared { buf: Mutex::new(BTreeMap::new()), next: AtomicU64::new(1), head, durable: Default::default(), acked: Notify::new() });
        for i in 0..3 {
            let f = LogFile::create(&dir.join(format!("g{g}-n{i}.log"))).unwrap();
            tokio::spawn(replicator(sh.clone(), dl.clone(), i, f));
        }
        let w: WriteFn = Arc::new(move |p: Bytes| {
            let sh = sh.clone();
            Box::pin(async move {
                let seq = {
                    let mut buf = sh.buf.lock().unwrap();
                    let s = sh.next.fetch_add(1, SeqCst);
                    buf.insert(s, p);
                    s
                };
                sh.head.send_modify(|h| *h = (*h).max(seq));
                loop {
                    let n = sh.acked.notified();
                    tokio::pin!(n);
                    n.as_mut().enable();
                    if sh.quorum() >= seq {
                        return Ok(());
                    }
                    n.await;
                }
            })
        });
        writes.push(w);
    }
    match a.mode.as_str() {
        "latency" => {
            let r = closed_loop(writes[0].clone(), a.conc, a.size, a.warmup, a.n, Some(dl.clone())).await;
            a.emit("proto", serde_json::json!({}), &r);
        }
        "idle" => {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let r = idle(&dl, groups, a.secs, 0.0).await;
            a.emit("proto", serde_json::json!({"note": "no heartbeats by construction"}), &r);
        }
        m => panic!("mode {m} not supported by proto (no fencing: fence test not applicable)"),
    }
    std::process::exit(0);
}
