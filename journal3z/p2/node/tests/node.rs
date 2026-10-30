//! P3/P4: the real node binary, over the real network code, alone and three at a time. Every check that guards a rule
//! has a twin run with the bug switched on (`--mutation`), which must make the check fail.
use bytes::Bytes;
use j3z_net::{spawn_link, LinkEvent};
use journal3z_core::*;
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

struct Node {
    child: std::process::Child,
    port: u16,
    id: NodeId,
}

fn start(dir: &Path, port: u16, faults: &Path, mutation: Option<&str>) -> Node {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_j3z-node"));
    cmd.args(["--dir", dir.to_str().unwrap(), "--listen", &format!("127.0.0.1:{port}"), "--faults-file", faults.to_str().unwrap(), "--segment-max", "4096"]);
    if let Some(m) = mutation {
        cmd.args(["--mutation", m]);
    }
    let mut child = cmd.stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::null()).spawn().unwrap();
    let mut line = String::new();
    BufReader::new(child.stdout.take().unwrap()).read_line(&mut line).unwrap();
    let id = line.split_whitespace().find_map(|w| w.strip_prefix("node=")).unwrap_or_else(|| panic!("no READY: {line:?}")).parse().unwrap();
    Node { child, port, id }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn set_faults(p: &Path, json: &str) {
    let tmp = p.with_extension("tmp");
    std::fs::write(&tmp, json).unwrap();
    std::fs::rename(&tmp, p).unwrap();
    std::thread::sleep(Duration::from_millis(120));
}

/// A writer's view of N nodes: links, node ids learned from the handshake, the event stream.
struct Links {
    tx: Vec<mpsc::UnboundedSender<Vec<Msg>>>,
    ids: BTreeMap<NodeId, usize>,
    ev: mpsc::UnboundedReceiver<LinkEvent>,
}

async fn connect(me: Addr, ports: &[u16]) -> Links {
    let (etx, mut ev) = mpsc::unbounded_channel();
    let tx = ports.iter().enumerate().map(|(i, p)| spawn_link(i, format!("127.0.0.1:{p}"), me, etx.clone())).collect::<Vec<_>>();
    let mut ids = BTreeMap::new();
    while ids.len() < ports.len() {
        if let Some(LinkEvent::Up { slot, node }) = tokio::time::timeout(Duration::from_secs(5), ev.recv()).await.expect("links up") {
            ids.insert(node, slot);
        }
    }
    Links { tx, ids, ev }
}

impl Links {
    fn send(&self, msgs: Vec<(NodeId, Msg)>) {
        for (n, m) in msgs {
            if let Some(&s) = self.ids.get(&n) {
                let _ = self.tx[s].send(vec![m]);
            }
        }
    }

    /// Drive a writer until `done` holds (or the timeout). Returns every event seen.
    async fn drive(&mut self, w: &mut WriterDb, first: WOut, done: impl Fn(&WriterDb, &[WEvent]) -> bool, limit: Duration) -> Vec<WEvent> {
        let mut evs = first.events;
        self.send(first.msgs);
        let t0 = Instant::now();
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        while !done(w, &evs) && t0.elapsed() < limit {
            tokio::select! {
                e = self.ev.recv() => {
                    if let Some(LinkEvent::Msgs { node, msgs, .. }) = e {
                        for m in msgs {
                            let o = w.on_msg(node, m);
                            evs.extend(o.events);
                            self.send(o.msgs);
                        }
                    }
                }
                _ = tick.tick() => {
                    let o = w.tick();
                    evs.extend(o.events);
                    self.send(o.msgs);
                }
            }
        }
        evs
    }
}

/// Test writer for a single node: a quorum of one, to open and to commit (the protocol's quorum is 2 of 3; these
/// single-node tests are about the node's disk and reply rules, not about quorums).
const ONE: Mutations = Mutations { ack_on_one: true, recovery_single: true, no_fencing: false, ack_gap: false, keep_diverged: false, forget_fence: false, recovery_longest: false, trim_before_upload: false, ack_before_fsync: false, ignore_fsync_error: false, skip_store_fence: false, wipe_same_id: false };

fn data(i: u64) -> Bytes {
    Bytes::copy_from_slice(&i.to_le_bytes())
}

fn committed(evs: &[WEvent], i: Idx) -> bool {
    evs.iter().any(|e| matches!(e, WEvent::Committed(c) if *c >= i))
}

/// One node; returns how long the Ack of one append took, with fdatasync slowed to 300 ms.
async fn ack_delay(mutation: Option<&str>) -> Duration {
    let d = tempfile::tempdir().unwrap();
    let faults = d.path().join("faults.json");
    let n = start(&d.path().join("n"), free_port(), &faults, mutation);
    let mut l = connect(1000, &[n.port]).await;
    let (mut w, o) = WriterDb::open(1, 5, vec![n.id], 0, ONE);
    l.drive(&mut w, o, |w, _| w.is_active(), Duration::from_secs(5)).await;
    assert!(w.is_active());
    set_faults(&faults, r#"{"slow_sync_us": 300000}"#);
    let i = w.append(data(1)).unwrap();
    let mut o = WOut::default();
    w.flush(&mut o);
    let t = Instant::now();
    let evs = l.drive(&mut w, o, |_, e| committed(e, i), Duration::from_secs(5)).await;
    assert!(committed(&evs, i));
    t.elapsed()
}

#[tokio::test(flavor = "multi_thread")]
async fn replies_only_after_fsync() {
    let d = ack_delay(None).await;
    assert!(d >= Duration::from_millis(290), "ack after {d:?}, before the 300 ms fdatasync finished");
    let bad = ack_delay(Some("ack_before_fsync")).await;
    println!("ack delay: correct {d:?}, twin ack_before_fsync {bad:?}");
    assert!(bad < Duration::from_millis(200), "twin: with ack_before_fsync the ack must come before the fsync ({bad:?})");
}

/// Writes 3 entries, makes the 4th one's fdatasync fail, restarts the node; returns (acked 4th?, exit code, last index
/// on disk after restart).
async fn fsync_error_case(mutation: Option<&str>) -> (bool, Option<i32>, Idx) {
    let d = tempfile::tempdir().unwrap();
    let faults = d.path().join("faults.json");
    set_faults(&faults, r#"{"fail_syncs": 0}"#);
    let dir = d.path().join("n");
    let port = free_port();
    let mut n = start(&dir, port, &faults, mutation);
    let mut l = connect(1000, &[n.port]).await;
    let (mut w, o) = WriterDb::open(1, 5, vec![n.id], 0, ONE);
    l.drive(&mut w, o, |w, _| w.is_active(), Duration::from_secs(5)).await;
    for k in 1..=3 {
        let i = w.append(data(k)).unwrap();
        let mut o = WOut::default();
        w.flush(&mut o);
        l.drive(&mut w, o, |_, e| committed(e, i), Duration::from_secs(5)).await;
    }
    set_faults(&faults, r#"{"fail_syncs": 1}"#);
    let i = w.append(data(4)).unwrap();
    let mut o = WOut::default();
    w.flush(&mut o);
    let evs = l.drive(&mut w, o, |_, e| committed(e, i), Duration::from_secs(2)).await;
    let acked = committed(&evs, i);
    std::thread::sleep(Duration::from_millis(200));
    let code = n.child.try_wait().unwrap().and_then(|s| s.code());
    drop(n);
    // restart (faults off) and ask what is on disk
    set_faults(&faults, r#"{"fail_syncs": 1}"#);
    let n2 = start(&dir, port, &faults, None);
    let mut l2 = connect(1001, &[n2.port]).await;
    let (mut w2, o) = WriterDb::open(1, 6, vec![n2.id], 0, ONE);
    let evs = l2.drive(&mut w2, o, |w, _| w.is_active(), Duration::from_secs(5)).await;
    let replay = evs.iter().find_map(|e| if let WEvent::Active { replay, .. } = e { Some(replay.len()) } else { None }).unwrap_or(0);
    (acked, code, replay as Idx + 1) // +1: the first writer's barrier is in the log but not in the replay
}

#[tokio::test(flavor = "multi_thread")]
async fn fsync_error_makes_the_node_exit() {
    let (acked, code, last) = fsync_error_case(None).await;
    println!("fsync error: acked={acked} exit={code:?} last on disk={last}");
    assert!(!acked, "a write whose fdatasync failed must not be acknowledged");
    assert_eq!(code, Some(75));
    assert_eq!(last, 4, "barrier + 3 entries survive; the failed 4th is gone");
    let (acked, code, last) = fsync_error_case(Some("ignore_fsync_error")).await;
    println!("twin ignore_fsync_error: acked={acked} exit={code:?} last on disk={last}");
    assert!(acked && code.is_none() && last == 4, "twin: the 4th write is acknowledged although it is not on disk");
}

#[tokio::test(flavor = "multi_thread")]
async fn disk_full_makes_the_node_exit() {
    let d = tempfile::tempdir().unwrap();
    let faults = d.path().join("faults.json");
    let mut n = start(&d.path().join("n"), free_port(), &faults, None);
    let mut l = connect(1000, &[n.port]).await;
    let (mut w, o) = WriterDb::open(1, 5, vec![n.id], 0, ONE);
    l.drive(&mut w, o, |w, _| w.is_active(), Duration::from_secs(5)).await;
    set_faults(&faults, r#"{"full": true}"#);
    let i = w.append(data(1)).unwrap();
    let mut o = WOut::default();
    w.flush(&mut o);
    let evs = l.drive(&mut w, o, |_, e| committed(e, i), Duration::from_secs(2)).await;
    assert!(!committed(&evs, i));
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(n.child.try_wait().unwrap().and_then(|s| s.code()), Some(75));
}

#[tokio::test(flavor = "multi_thread")]
async fn restart_keeps_identity_and_log() {
    let d = tempfile::tempdir().unwrap();
    let faults = d.path().join("faults.json");
    let dir = d.path().join("n");
    let port = free_port();
    let n = start(&dir, port, &faults, None);
    let id = n.id;
    let mut l = connect(1000, &[n.port]).await;
    let (mut w, o) = WriterDb::open(1, 5, vec![n.id], 0, ONE);
    l.drive(&mut w, o, |w, _| w.is_active(), Duration::from_secs(5)).await;
    for k in 1..=20 {
        w.append(data(k));
    }
    let mut o = WOut::default();
    w.flush(&mut o);
    l.drive(&mut w, o, |_, e| committed(e, 21), Duration::from_secs(5)).await;
    drop(n); // kill -9
    let n2 = start(&dir, port, &faults, None);
    assert_eq!(n2.id, id, "same directory, same identity");
    std::fs::remove_dir_all(&dir).unwrap();
    drop(n2);
    let n3 = start(&dir, port, &faults, None);
    assert_ne!(n3.id, id, "lost directory, new identity");
}

/// Three real nodes. Writer A (epoch 10) writes 30 acknowledged writes; writer B (epoch 11, its own connections) opens
/// using nodes 1 and 2 only and must replay all 30; then A appends again and must be told it is stale, with nothing
/// acknowledged. Returns (B replayed, A's late write acknowledged, A told stale).
async fn takeover_case(mutation: Option<&str>) -> (usize, bool, bool) {
    let d = tempfile::tempdir().unwrap();
    let faults = d.path().join("faults.json");
    let nodes: Vec<Node> = (0..3).map(|i| start(&d.path().join(format!("n{i}")), free_port(), &faults, mutation)).collect();
    let ports: Vec<u16> = nodes.iter().map(|n| n.port).collect();
    let members: Vec<NodeId> = nodes.iter().map(|n| n.id).collect();
    let mut la = connect(1000, &ports).await;
    let (mut a, o) = WriterDb::open(1, 10, members.clone(), 0, Mutations::default());
    la.drive(&mut a, o, |w, _| w.is_active(), Duration::from_secs(5)).await;
    for k in 1..=30 {
        a.append(data(k));
    }
    let last = a.last;
    let mut o = WOut::default();
    a.flush(&mut o);
    let evs = la.drive(&mut a, o, |_, e| committed(e, last), Duration::from_secs(5)).await;
    assert!(committed(&evs, last));
    // B reaches only nodes 1 and 2
    let mut lb = connect(2000, &ports[1..]).await;
    let (mut b, o) = WriterDb::open(1, 11, members.clone(), 0, Mutations::default());
    let evs = lb.drive(&mut b, o, |w, _| w.is_active(), Duration::from_secs(5)).await;
    let replayed = evs.iter().find_map(|e| if let WEvent::Active { replay, .. } = e { Some(replay.len()) } else { None }).unwrap_or(0);
    // A is frozen meanwhile; it resumes and appends, but only reaches nodes 1 and 2 as well
    let mut la2 = connect(1000, &ports[1..]).await;
    let i = a.append(data(99)).unwrap();
    let mut o = WOut::default();
    a.flush(&mut o);
    let evs = la2.drive(&mut a, o, |_, e| committed(e, i) || e.iter().any(|x| matches!(x, WEvent::Stale(_))), Duration::from_secs(3)).await;
    drop(la);
    (replayed, committed(&evs, i), evs.iter().any(|x| matches!(x, WEvent::Stale(_))))
}

#[tokio::test(flavor = "multi_thread")]
async fn takeover_on_three_real_nodes() {
    let (replayed, late_acked, stale) = takeover_case(None).await;
    println!("takeover: B replayed {replayed}, A late write acked {late_acked}, A told stale {stale}");
    assert_eq!(replayed, 30);
    assert!(!late_acked && stale);
    let (replayed, late_acked, stale) = takeover_case(Some("no_fencing")).await;
    println!("twin no_fencing: B replayed {replayed}, A late write acked {late_acked}, A told stale {stale}");
    assert!(late_acked && !stale, "twin: without the fence the stale writer's write is acknowledged");
}
