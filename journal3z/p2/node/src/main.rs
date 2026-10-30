//! journal3z journal node (phase-2 plan P3).
//!
//! `j3z-node --dir <dir> --listen <addr> [--segment-max <bytes>] [--faults-file <file>] [--mutation <name>]`
//!
//! - Opens its directory (`j3z-store`): identity, fence table, segments; a node whose directory was lost starts with a
//!   new identity. A directory that cannot come from a crash makes it exit with code 3 (treat the node as lost).
//! - Listens for writers (`j3z-net` handshake + batches). Each connection task decodes batches and passes them to ONE
//!   core thread, which owns the `NodeCore` (the protocol state machine, `journal3z/core`) and the store.
//! - Core loop: take every batch waiting; handle every message; write all the records; **one fdatasync**; **then** send
//!   the replies. A reply never leaves before every record produced so far is durable (also the replies to duplicate
//!   messages that wrote nothing: they wait for the same fsync, or go out at once only if nothing is unsynced).
//! - A failed write or fdatasync (disk full, I/O error) makes the node exit with code 75 (the store is poisoned; a
//!   restart recovers from what is on disk). A record that breaks a log (a protocol bug) exits with code 70.
//! - `--faults-file` (feature `faults`, test bench only): JSON re-read every 50 ms:
//!   `{"fail_syncs": <counter: each increment makes one fdatasync fail>, "full": bool, "slow_sync_us": n,
//!   "power_cuts": <counter: an increment drops every byte not yet fsynced and ends the process with code 99>}`.
//! - Self-check: a reply `Ack` for an epoch below the one the node has promised for that database prints a line
//!   `VIOLATION ack_below_promise ...` on stdout (the bench counts them; only a broken node can do it).
//! - `--mutation` (feature `mutations`, negative controls only): a protocol bug from `Mutations::ALL`.
//! Prints `READY node=<id> listen=<addr>` on stdout once listening.

use j3z_codec::{decode_batch, encode_batch};
use j3z_net::{hello, parse_hello, FrameReader};
use j3z_store::{Fs, RealFs, Store, StoreCfg, StoreError};
use journal3z_core::{Addr, Msg, Mutations};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

enum In {
    Conn { id: u64, out: mpsc::UnboundedSender<Vec<Msg>> },
    Msgs { conn: u64, from: Addr, msgs: Vec<Msg> },
    Gone { conn: u64 },
}

fn arg(a: &[String], k: &str) -> Option<String> {
    a.iter().position(|x| x == k).and_then(|i| a.get(i + 1)).cloned()
}

fn die(code: i32, why: &str) -> ! {
    eprintln!("j3z-node: exit {code}: {why}");
    std::process::exit(code);
}

#[allow(unused_variables)]
fn build_fs(faults_file: Option<String>) -> Arc<dyn Fs> {
    #[cfg(feature = "faults")]
    if let Some(path) = faults_file {
        use std::sync::atomic::Ordering::SeqCst;
        let faults = Arc::new(j3z_store::Faults::default());
        let f2 = faults.clone();
        std::thread::spawn(move || {
            let mut seen_fail: Option<u64> = None;
            let mut seen_cut: Option<u64> = None;
            loop {
                if let Some(v) = std::fs::read_to_string(&path).ok().and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok()) {
                    f2.full.store(v["full"].as_bool().unwrap_or(false), SeqCst);
                    f2.slow_sync_us.store(v["slow_sync_us"].as_u64().unwrap_or(0), SeqCst);
                    let n = v["fail_syncs"].as_u64().unwrap_or(0);
                    match seen_fail {
                        None => seen_fail = Some(n), // counter value at start: not a request
                        Some(s) if n > s => {
                            f2.fail_next_sync.store(true, SeqCst);
                            seen_fail = Some(n);
                        }
                        _ => {}
                    }
                    let c = v["power_cuts"].as_u64().unwrap_or(0);
                    match seen_cut {
                        None => seen_cut = Some(c),
                        Some(s) if c > s => {
                            let _held = f2.power_cut(); // never released: the process ends now
                            eprintln!("j3z-node: exit 99: simulated power cut");
                            std::process::exit(99);
                        }
                        _ => {}
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        });
        return Arc::new(j3z_store::FaultFs { faults });
    }
    #[cfg(not(feature = "faults"))]
    if faults_file.is_some() {
        die(2, "--faults-file needs a build with the `faults` feature");
    }
    Arc::new(RealFs)
}

fn mutations(name: Option<String>) -> Mutations {
    match name {
        None => Mutations::default(),
        #[cfg(feature = "mutations")]
        Some(n) => Mutations::parse(&n).unwrap_or_else(|| die(2, &format!("unknown mutation {n}"))),
        #[cfg(not(feature = "mutations"))]
        Some(_) => die(2, "--mutation needs a build with the `mutations` feature"),
    }
}

/// The core thread: the protocol state machine, the disk, the fsync, the replies.
fn core_loop(rx: std::sync::mpsc::Receiver<In>, mut store: Store, mut core: journal3z_core::NodeCore, ack_before_fsync: bool) {
    let mut conns: BTreeMap<u64, mpsc::UnboundedSender<Vec<Msg>>> = BTreeMap::new();
    loop {
        let Ok(first) = rx.recv() else { return };
        let mut items = vec![first];
        while let Ok(x) = rx.try_recv() {
            items.push(x);
        }
        let mut recs = vec![];
        let mut replies: BTreeMap<u64, Vec<Msg>> = BTreeMap::new();
        for it in items {
            match it {
                In::Conn { id, out } => {
                    conns.insert(id, out);
                }
                In::Gone { conn } => {
                    conns.remove(&conn);
                }
                In::Msgs { conn, from, msgs } => {
                    for m in msgs {
                        let out = core.handle(from, m);
                        if let Some(b) = out.broken {
                            die(70, &format!("protocol bug: {b}"));
                        }
                        recs.extend(out.recs);
                        for (_, r) in &out.replies {
                            if let Msg::Ack { db, epoch, .. } = r {
                                let p = core.dbs.get(db).map_or(0, |d| d.promised);
                                if p > *epoch {
                                    use std::io::Write;
                                    let _ = writeln!(std::io::stdout(), "VIOLATION ack_below_promise db={db} epoch={epoch} promised={p}");
                                }
                            }
                        }
                        replies.entry(conn).or_default().extend(out.replies.into_iter().map(|(_, r)| r));
                    }
                }
            }
        }
        let send = |replies: BTreeMap<u64, Vec<Msg>>, conns: &BTreeMap<u64, mpsc::UnboundedSender<Vec<Msg>>>| {
            for (c, v) in replies {
                if let Some(tx) = conns.get(&c) {
                    let _ = tx.send(v);
                }
            }
        };
        let fail = |e: StoreError| -> ! { die(75, &format!("{e}")) };
        if let Err(e) = store.write(&recs) {
            fail(e);
        }
        if ack_before_fsync {
            // BUG (negative control): replies leave before the data is durable
            send(std::mem::take(&mut replies), &conns);
        }
        if store.has_unsynced() {
            if let Err(e) = store.sync() {
                fail(e);
            }
        }
        send(replies, &conns);
        if let Err(e) = store.gc(&core) {
            fail(e);
        }
    }
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let dir = arg(&a, "--dir").unwrap_or_else(|| die(2, "--dir is required"));
    let listen = arg(&a, "--listen").unwrap_or_else(|| die(2, "--listen is required"));
    let m = mutations(arg(&a, "--mutation"));
    let fs = build_fs(arg(&a, "--faults-file"));
    let mut cfg = StoreCfg { m, ..Default::default() };
    if let Some(s) = arg(&a, "--segment-max") {
        cfg.segment_max = s.parse().unwrap_or_else(|_| die(2, "bad --segment-max"));
    }
    let (store, core, rep) = match Store::open(&dir, fs, cfg) {
        Ok(x) => x,
        Err(StoreError::Corrupt(s)) => die(3, &format!("refusing to start: {s}")),
        Err(e) => die(4, &format!("cannot open {dir}: {e}")),
    };
    let node_id = store.node_id;
    eprintln!("j3z-node {node_id}: recovered {rep:?}");
    let l = TcpListener::bind(&listen).await.unwrap_or_else(|e| die(2, &format!("bind {listen}: {e}")));
    let (tx, rx) = std::sync::mpsc::channel::<In>();
    std::thread::Builder::new().name("j3z-core".into()).spawn(move || core_loop(rx, store, core, m.ack_before_fsync)).unwrap();
    println!("READY node={node_id} listen={listen}");
    let mut next_conn = 0u64;
    loop {
        let Ok((s, _)) = l.accept().await else { continue };
        let _ = s.set_nodelay(true);
        next_conn += 1;
        let (conn, tx) = (next_conn, tx.clone());
        tokio::spawn(async move {
            let (rd, mut wr) = s.into_split();
            let mut reader = FrameReader::new(rd);
            let Ok(Ok(body)) = tokio::time::timeout(j3z_net::DEAD_AFTER, reader.next()).await else { return };
            let Some(from) = parse_hello(&body) else { return };
            if wr.write_all(&hello(node_id)).await.is_err() {
                return;
            }
            let (otx, mut orx) = mpsc::unbounded_channel::<Vec<Msg>>();
            if tx.send(In::Conn { id: conn, out: otx.clone() }).is_err() {
                return;
            }
            let writer = tokio::spawn(async move {
                while let Some(first) = orx.recv().await {
                    let mut v = first;
                    while let Ok(more) = orx.try_recv() {
                        v.extend(more);
                    }
                    if wr.write_all(&encode_batch(&v)).await.is_err() {
                        return;
                    }
                }
            });
            while let Ok(body) = reader.next().await {
                let Ok(msgs) = decode_batch(&body) else { break };
                if msgs.is_empty() {
                    let _ = otx.send(vec![]); // keepalive answer
                    continue;
                }
                if tx.send(In::Msgs { conn, from, msgs }).is_err() {
                    break;
                }
            }
            let _ = tx.send(In::Gone { conn });
            writer.abort();
        });
    }
}
