//! `custom`: the journal3z protocol (journal3z/core: the same state machines the simulator checks) on the common
//! test bed. 3 journal nodes, each a task owning a `NodeCore` and a `LogFile` (records written, one fdatasync per
//! batch, replies sent only after it). The writer (co-located with node 0) owns one `WriterDb` per database: open
//! (fence + recover + barrier), pipelined appends, commit on 2 of 3. All messages for one node from one flush travel
//! as one batch through the delay line (one connection per writer-node pair).
//!
//! Modes: latency (one database), idle (N databases = N logical logs on the same 3 nodes and the same 3 files),
//! fence (epoch from an external lease fences an older writer).
use bytes::Bytes;
use j3z_harness::{closed_loop, idle, Args, DelayLine, LogFile, WriteFn};
use journal3z_core::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

const WRITER_ADDR: Addr = 1000;

type ToNode = mpsc::UnboundedSender<(Addr, Vec<Msg>)>;
type ToWriter = mpsc::UnboundedSender<(NodeId, Vec<Msg>)>;

/// Serialized size of records (for the log file); the encoding is a simple length-prefixed dump.
fn encode(recs: &[Rec]) -> Vec<u8> {
    let mut v = Vec::new();
    for r in recs {
        match r {
            Rec::Append { db, e } => {
                v.extend_from_slice(&(e.data.len() as u32 + 25).to_le_bytes());
                v.push(1);
                v.extend_from_slice(&db.to_le_bytes());
                v.extend_from_slice(&e.idx.to_le_bytes());
                v.extend_from_slice(&e.epoch.to_le_bytes());
                v.extend_from_slice(&e.data);
            }
            Rec::Promise { db, epoch } | Rec::Trim { db, upto: epoch } | Rec::Truncate { db, from: epoch } => {
                v.extend_from_slice(&17u32.to_le_bytes());
                v.push(2);
                v.extend_from_slice(&db.to_le_bytes());
                v.extend_from_slice(&epoch.to_le_bytes());
            }
            Rec::Erase { db } => {
                v.extend_from_slice(&9u32.to_le_bytes());
                v.push(3);
                v.extend_from_slice(&db.to_le_bytes());
            }
        }
    }
    v
}

/// A journal node: handle every queued batch, write all records, one fdatasync, then send the replies.
fn spawn_node(id: NodeId, file: Arc<LogFile>, dl: Arc<DelayLine>, writers: Arc<BTreeMap<Addr, ToWriter>>) -> ToNode {
    let (tx, mut rx) = mpsc::unbounded_channel::<(Addr, Vec<Msg>)>();
    tokio::spawn(async move {
        let mut core = NodeCore::new(id, Mutations::default());
        while let Some(first) = rx.recv().await {
            let mut batches = vec![first];
            while let Ok(b) = rx.try_recv() {
                batches.push(b);
            }
            let mut recs = Vec::new();
            let mut replies: BTreeMap<Addr, Vec<Msg>> = BTreeMap::new();
            for (from, msgs) in batches {
                for m in msgs {
                    let out = core.handle(from, m);
                    assert!(out.broken.is_none());
                    recs.extend(out.recs);
                    for (to, r) in out.replies {
                        replies.entry(to).or_default().push(r);
                    }
                }
            }
            // replies only after every record so far is durable: the LogFile runs callbacks in order after fsync
            let (dl2, ws) = (dl.clone(), writers.clone());
            file.append(
                encode(&recs),
                Box::new(move || {
                    for (to, msgs) in replies {
                        let size = msgs.iter().map(|m| m.size()).sum();
                        let w = ws[&to].clone();
                        dl2.send(id as usize, 0, size, Box::new(move || {
                            let _ = w.send((id, msgs));
                        }));
                    }
                }),
            );
        }
    });
    tx
}

enum Cmd {
    Write(DbId, Bytes, oneshot::Sender<anyhow::Result<()>>),
}

struct WriterState {
    dbs: BTreeMap<DbId, WriterDb>,
    pending: BTreeMap<DbId, BTreeMap<Idx, oneshot::Sender<anyhow::Result<()>>>>,
    opened: BTreeMap<DbId, oneshot::Sender<usize>>,
    stale: Vec<(DbId, Epoch)>,
}

fn send_out(addr: Addr, dl: &DelayLine, nodes: &BTreeMap<NodeId, ToNode>, msgs: Vec<(NodeId, Msg)>) {
    let mut per: BTreeMap<NodeId, Vec<Msg>> = BTreeMap::new();
    for (n, m) in msgs {
        per.entry(n).or_default().push(m);
    }
    for (n, v) in per {
        let size = v.iter().map(|m| m.size()).sum();
        let tx = nodes[&n].clone();
        dl.send(0, n as usize, size, Box::new(move || {
            let _ = tx.send((addr, v));
        }));
    }
}

fn handle_events(st: &mut WriterState, db: DbId, events: Vec<WEvent>) {
    for ev in events {
        match ev {
            WEvent::Active { replay, .. } => {
                if let Some(t) = st.opened.remove(&db) {
                    let _ = t.send(replay.len());
                }
            }
            WEvent::Committed(c) => {
                let p = st.pending.entry(db).or_default();
                let rest = p.split_off(&(c + 1));
                for (_, tx) in std::mem::replace(p, rest) {
                    let _ = tx.send(Ok(()));
                }
            }
            WEvent::Stale(e) => {
                st.stale.push((db, e));
                for (_, tx) in std::mem::take(st.pending.entry(db).or_default()) {
                    let _ = tx.send(Err(anyhow::anyhow!("fenced by epoch {e}")));
                }
            }
        }
    }
}

struct Cluster {
    dl: Arc<DelayLine>,
    nodes: Arc<BTreeMap<NodeId, ToNode>>,
}

fn cluster(a: &Args, writer_rx: Vec<(Addr, ToWriter)>) -> Cluster {
    let dir = a.fresh_dir();
    let dl = DelayLine::new(a.delay_cfg());
    let writers: Arc<BTreeMap<Addr, ToWriter>> = Arc::new(writer_rx.into_iter().collect());
    let mut nodes = BTreeMap::new();
    for i in 0..3u32 {
        let f = LogFile::create(&dir.join(format!("node{i}.j3z"))).unwrap();
        nodes.insert(i, spawn_node(i, f, dl.clone(), writers.clone()));
    }
    Cluster { dl, nodes: Arc::new(nodes) }
}

/// The writer task: client commands, node replies, a 20 ms retransmission tick.
fn spawn_writer(addr: Addr, dl: Arc<DelayLine>, nodes: Arc<BTreeMap<NodeId, ToNode>>, mut from_nodes: mpsc::UnboundedReceiver<(NodeId, Vec<Msg>)>, mut cmds: mpsc::UnboundedReceiver<Cmd>, mut opens: mpsc::UnboundedReceiver<(DbId, Epoch, oneshot::Sender<usize>)>) -> tokio::task::JoinHandle<WriterState> {
    tokio::spawn(async move {
        let mut st = WriterState { dbs: BTreeMap::new(), pending: BTreeMap::new(), opened: BTreeMap::new(), stale: vec![] };
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(20));
        loop {
            tokio::select! {
                o = opens.recv() => {
                    let Some((db, epoch, done)) = o else { break };
                    let (w, out) = WriterDb::open(db, epoch, vec![0, 1, 2], 0, Mutations::default());
                    st.dbs.insert(db, w);
                    st.opened.insert(db, done);
                    send_out(addr, &dl, &nodes, out.msgs);
                }
                c = cmds.recv() => {
                    let Some(first) = c else { break };
                    let mut batch = vec![first];
                    while let Ok(c) = cmds.try_recv() { batch.push(c); }
                    let mut touched = vec![];
                    for Cmd::Write(db, p, tx) in batch {
                        let w = st.dbs.get_mut(&db).unwrap();
                        match w.append(p) {
                            Some(i) => { st.pending.entry(db).or_default().insert(i, tx); touched.push(db); }
                            None => { let _ = tx.send(Err(anyhow::anyhow!("not active"))); }
                        }
                    }
                    touched.dedup();
                    let mut msgs = vec![];
                    for db in touched {
                        let mut out = WOut::default();
                        st.dbs.get_mut(&db).unwrap().flush(&mut out);
                        msgs.extend(out.msgs);
                    }
                    send_out(addr, &dl, &nodes, msgs);
                }
                r = from_nodes.recv() => {
                    let Some((n, msgs)) = r else { break };
                    let mut batch = vec![(n, msgs)];
                    while let Ok(x) = from_nodes.try_recv() { batch.push(x); }
                    let mut out_msgs = vec![];
                    for (n, msgs) in batch {
                        for m in msgs {
                            let db = m.db();
                            let Some(w) = st.dbs.get_mut(&db) else { continue };
                            let out = w.on_msg(n, m);
                            out_msgs.extend(out.msgs);
                            handle_events(&mut st, db, out.events);
                        }
                    }
                    send_out(addr, &dl, &nodes, out_msgs);
                }
                _ = tick.tick() => {
                    let mut msgs = vec![];
                    let mut evs = vec![];
                    for (db, w) in st.dbs.iter_mut() {
                        // only databases with something outstanding retransmit: an idle database sends nothing
                        if !w.is_active() || w.commit < w.last {
                            let out = w.tick();
                            msgs.extend(out.msgs);
                            evs.push((*db, out.events));
                        }
                    }
                    for (db, e) in evs { handle_events(&mut st, db, e); }
                    send_out(addr, &dl, &nodes, msgs);
                }
            }
        }
        st
    })
}

#[tokio::main]
async fn main() {
    let a = Args::parse();
    let (wtx, wrx) = mpsc::unbounded_channel();
    match a.mode.as_str() {
        "latency" | "idle" => {
            let c = cluster(&a, vec![(WRITER_ADDR, wtx)]);
            let (ctx, crx) = mpsc::unbounded_channel();
            let (otx, orx) = mpsc::unbounded_channel();
            spawn_writer(WRITER_ADDR, c.dl.clone(), c.nodes.clone(), wrx, crx, orx);
            let n = if a.mode == "idle" { a.groups } else { 1 };
            let t0 = std::time::Instant::now();
            let mut waits = vec![];
            for db in 0..n as DbId {
                let (tx, rx) = oneshot::channel();
                otx.send((db, 7, tx)).unwrap();
                waits.push(rx);
            }
            for w in waits {
                w.await.unwrap();
            }
            let startup = t0.elapsed().as_secs_f64();
            if a.mode == "latency" {
                let ctx2 = ctx.clone();
                let write: WriteFn = Arc::new(move |p: Bytes| {
                    let ctx = ctx2.clone();
                    Box::pin(async move {
                        let (tx, rx) = oneshot::channel();
                        ctx.send(Cmd::Write(0, p, tx)).map_err(|_| anyhow::anyhow!("writer gone"))?;
                        rx.await?
                    })
                });
                let r = closed_loop(write, a.conc, a.size, a.warmup, a.n, Some(c.dl.clone())).await;
                a.emit("custom", serde_json::json!({"open_s": startup}), &r);
            } else {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let r = idle(&c.dl, n, a.secs, startup).await;
                a.emit("custom", serde_json::json!({"note": "per-database logical logs on 3 shared node files; no heartbeats"}), &r);
            }
        }
        "fence" => {
            // Two writer processes (addresses 1000 and 1001) for the same database, real nodes, real fdatasync, the
            // delay line. A opens with epoch 10 (from "the lease") and writes; the lease moves: B opens with epoch 11;
            // A keeps writing (a frozen/stale writer that still believes it owns the database); then B writes.
            let (b_tx, b_rx) = mpsc::unbounded_channel();
            let c = cluster(&a, vec![(WRITER_ADDR, wtx), (WRITER_ADDR + 1, b_tx)]);
            let (a_cmd, a_crx) = mpsc::unbounded_channel();
            let (a_open, a_orx) = mpsc::unbounded_channel();
            let (b_cmd, b_crx) = mpsc::unbounded_channel();
            let (b_open, b_orx) = mpsc::unbounded_channel();
            spawn_writer(WRITER_ADDR, c.dl.clone(), c.nodes.clone(), wrx, a_crx, a_orx);
            spawn_writer(WRITER_ADDR + 1, c.dl.clone(), c.nodes.clone(), b_rx, b_crx, b_orx);
            async fn writes(cmd: &mpsc::UnboundedSender<Cmd>, from: u64, n: u64) -> (u64, u64) {
                let mut rxs = vec![];
                for i in from..from + n {
                    let (tx, rx) = oneshot::channel();
                    cmd.send(Cmd::Write(0, Bytes::copy_from_slice(&i.to_le_bytes()), tx)).unwrap();
                    rxs.push(rx);
                }
                let (mut ok, mut err) = (0, 0);
                for rx in rxs {
                    match tokio::time::timeout(std::time::Duration::from_secs(2), rx).await {
                        Ok(Ok(Ok(()))) => ok += 1,
                        _ => err += 1,
                    }
                }
                (ok, err)
            }
            let (tx, rx) = oneshot::channel();
            a_open.send((0, 10, tx)).unwrap();
            rx.await.unwrap();
            let (a_ok_before, a_err_before) = writes(&a_cmd, 1, 100).await;
            let (tx, rx) = oneshot::channel();
            b_open.send((0, 11, tx)).unwrap();
            let b_replayed = rx.await.unwrap();
            let (a_ok_after, a_err_after) = writes(&a_cmd, 1001, 100).await;
            let (b_ok, b_err) = writes(&b_cmd, 2001, 100).await;
            let pass = a_ok_before == 100 && b_replayed == 100 && a_ok_after == 0 && b_ok == 100;
            a.emit("custom", serde_json::json!({}), &serde_json::json!({
                "old_writer_epoch": 10, "acked_before_takeover": a_ok_before, "failed_before_takeover": a_err_before,
                "new_writer_epoch_from_lease": 11, "new_writer_replayed_records": b_replayed,
                "old_writer_writes_after_takeover_acked": a_ok_after, "old_writer_writes_after_takeover_failed": a_err_after,
                "new_writer_acked": b_ok, "new_writer_failed": b_err,
                "c5": if pass { "pass: the epoch is an input (the lease seq); B's FENCE at a majority makes every later append of A rejected (A is told stale); B recovered all 100 acknowledged writes of A; no election" } else { "FAIL" }}));
        }
        m => panic!("unknown mode {m}"),
    }
    std::process::exit(0);
}
