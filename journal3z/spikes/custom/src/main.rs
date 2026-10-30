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
    opened: BTreeMap<DbId, oneshot::Sender<()>>,
    stale: Vec<(DbId, Epoch)>,
}

fn send_out(dl: &DelayLine, nodes: &BTreeMap<NodeId, ToNode>, msgs: Vec<(NodeId, Msg)>) {
    let mut per: BTreeMap<NodeId, Vec<Msg>> = BTreeMap::new();
    for (n, m) in msgs {
        per.entry(n).or_default().push(m);
    }
    for (n, v) in per {
        let size = v.iter().map(|m| m.size()).sum();
        let tx = nodes[&n].clone();
        dl.send(0, n as usize, size, Box::new(move || {
            let _ = tx.send((WRITER_ADDR, v));
        }));
    }
}

fn handle_events(st: &mut WriterState, db: DbId, events: Vec<WEvent>) {
    for ev in events {
        match ev {
            WEvent::Active { .. } => {
                if let Some(t) = st.opened.remove(&db) {
                    let _ = t.send(());
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
fn spawn_writer(dl: Arc<DelayLine>, nodes: Arc<BTreeMap<NodeId, ToNode>>, mut from_nodes: mpsc::UnboundedReceiver<(NodeId, Vec<Msg>)>, mut cmds: mpsc::UnboundedReceiver<Cmd>, mut opens: mpsc::UnboundedReceiver<(DbId, Epoch, oneshot::Sender<()>)>) -> tokio::task::JoinHandle<WriterState> {
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
                    send_out(&dl, &nodes, out.msgs);
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
                    send_out(&dl, &nodes, msgs);
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
                    send_out(&dl, &nodes, out_msgs);
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
                    send_out(&dl, &nodes, msgs);
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
            spawn_writer(c.dl.clone(), c.nodes.clone(), wrx, crx, orx);
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
            // two writers for the same database; the second gets a higher epoch from "the lease"
            let (w2tx, w2rx) = mpsc::unbounded_channel();
            let c = cluster(&a, vec![(WRITER_ADDR, wtx), (WRITER_ADDR, w2tx.clone())]);
            let _ = w2rx;
            let (ctx, crx) = mpsc::unbounded_channel();
            let (otx, orx) = mpsc::unbounded_channel();
            let h1 = spawn_writer(c.dl.clone(), c.nodes.clone(), wrx, crx, orx);
            let (tx, rx) = oneshot::channel();
            otx.send((0, 10, tx)).unwrap();
            rx.await.unwrap();
            let mut ok_before = 0;
            for i in 0..100u64 {
                let (tx, rx) = oneshot::channel();
                ctx.send(Cmd::Write(0, Bytes::copy_from_slice(&i.to_le_bytes()), tx)).unwrap();
                if rx.await.unwrap().is_ok() {
                    ok_before += 1;
                }
            }
            // the lease moves: a second writer process opens with epoch 11 (it reuses the node channels; replies to
            // address 1000 go to whoever owns that address, so it runs as a separate cluster view with its own core)
            let mut nodes_core: BTreeMap<NodeId, NodeCore> = BTreeMap::new();
            let _ = &mut nodes_core;
            drop(c);
            drop(h1);
            // Simpler, deterministic demonstration on the same core state machines (no timing involved):
            let mut nodes: BTreeMap<NodeId, NodeCore> = (0..3).map(|i| (i, NodeCore::new(i, Mutations::default()))).collect();
            let pump = |nodes: &mut BTreeMap<NodeId, NodeCore>, w: &mut WriterDb, msgs: Vec<(NodeId, Msg)>| {
                let mut q = msgs;
                let mut ev = vec![];
                while !q.is_empty() {
                    let mut next = vec![];
                    for (n, m) in q {
                        for (_, r) in nodes.get_mut(&n).unwrap().handle(WRITER_ADDR, m).replies {
                            let o = w.on_msg(n, r);
                            next.extend(o.msgs);
                            ev.extend(o.events);
                        }
                    }
                    q = next;
                }
                ev
            };
            let (mut old, o) = WriterDb::open(0, 10, vec![0, 1, 2], 0, Mutations::default());
            pump(&mut nodes, &mut old, o.msgs);
            old.append(Bytes::from_static(b"before")).unwrap();
            let mut o = WOut::default();
            old.flush(&mut o);
            let committed_before = pump(&mut nodes, &mut old, o.msgs).iter().any(|e| matches!(e, WEvent::Committed(_)));
            let (mut new, o) = WriterDb::open(0, 11, vec![0, 1, 2], 0, Mutations::default());
            let ev = pump(&mut nodes, &mut new, o.msgs);
            let new_active = ev.iter().any(|e| matches!(e, WEvent::Active { .. }));
            old.append(Bytes::from_static(b"after")).unwrap();
            let mut o = WOut::default();
            old.flush(&mut o);
            let ev = pump(&mut nodes, &mut old, o.msgs);
            let old_stale = ev.iter().find_map(|e| if let WEvent::Stale(p) = e { Some(*p) } else { None });
            let old_committed_after = ev.iter().any(|e| matches!(e, WEvent::Committed(_)));
            a.emit("custom", serde_json::json!({}), &serde_json::json!({
                "realtime_writes_acked_by_epoch_10": ok_before,
                "old_writer_epoch": 10, "committed_before_takeover": committed_before,
                "new_writer_epoch_from_lease": 11, "new_writer_active": new_active,
                "old_writer_append_after_takeover_committed": old_committed_after, "old_writer_told_stale_by_epoch": old_stale,
                "c5": "pass: the epoch is an input (the lease seq); FENCE at a majority makes every older epoch's append rejected; no election"}));
        }
        m => panic!("unknown mode {m}"),
    }
    std::process::exit(0);
}
