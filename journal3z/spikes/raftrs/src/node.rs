//! The raft-rs adapter: one thread per node driving every `RawNode` (one per group) of that node.
//!
//! - Transport: every `Message` to another node is protobuf-encoded and handed to `DelayLine::send`; the delivery
//!   closure pushes the bytes into the target node's inbox (decoded there).
//! - Storage: `MemStorage` (in-memory index, wrapped to override `snapshot()`, see `J3zStorage`) + the node's
//!   `LogFile` (real file, write + fdatasync). Async ready: entries/hard state are appended to `MemStorage` before
//!   `advance_append_async`, the bytes are queued on the `LogFile`, and only its callback (after fdatasync) releases
//!   the ready's `persisted_messages` and calls `on_persist_ready`. The leader's own entries therefore count towards
//!   the commit quorum only after its fdatasync.
//! - A write is acknowledged when `raft_log.committed` covers its index with the same term (`Ack::Commit`, default),
//!   or when raft-rs hands it out as a committed entry (`Ack::Apply`: needs the entry persisted locally as well).
use bytes::Bytes;
use j3z_harness::{frame, DelayLine, LogFile};
use protobuf::Message as _;
use raft::eraftpb::{ConfState, Entry, HardState, Message, MessageType, Snapshot};
use raft::storage::MemStorage;
use raft::{Config, GetEntriesContext, RaftState, RawNode, StateRole, Storage};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub type AckTx = tokio::sync::oneshot::Sender<anyhow::Result<()>>;

pub enum Ev {
    /// An encoded raft `Message` for group `.0`, delivered by the delay line.
    Msg(u32, Bytes),
    /// A client write on group `.0` (only accepted while this node is the leader).
    Propose(u32, Bytes, AckTx),
    /// The `LogFile` callback: everything up to ready `.1` of group `.0` is written and fdatasync'd.
    Persisted(u32, u64),
    /// Run a closure on the node thread (control / observation in the fence and trim scenarios).
    Ctl(Box<dyn FnOnce(&mut Node) + Send>),
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Ack {
    Commit,
    Apply,
}

#[derive(Clone, Debug)]
pub struct RaftCfg {
    pub tick: Duration,
    pub election_tick: usize,
    pub heartbeat_tick: usize,
    pub ack: Ack,
    /// Keep (index, term, data) of every committed entry handed out (fence/trim scenarios only).
    pub record: bool,
    /// `Config::skip_bcast_commit` (default false: the leader sends an extra append to tell followers the commit).
    pub skip_bcast_commit: bool,
}

/// Simulated partitions: a blocked link drops messages (checked when sent and when delivered).
pub struct Net {
    blocked: [AtomicBool; 9],
}
impl Net {
    pub fn new() -> Arc<Self> {
        Arc::new(Net { blocked: Default::default() })
    }
    pub fn ok(&self, a: usize, b: usize) -> bool {
        !self.blocked[a * 3 + b].load(Relaxed)
    }
    pub fn isolate(&self, n: usize, on: bool) {
        for o in 0..3 {
            if o != n {
                self.blocked[n * 3 + o].store(on, Relaxed);
                self.blocked[o * 3 + n].store(on, Relaxed);
            }
        }
    }
}

// ------------------------------------------------------------------------------------------------------------ storage

/// `MemStorage` with one change: `snapshot()` returns a metadata-only snapshot at the externally confirmed trim point
/// (`marker`, "the S3 marker"), instead of MemStorage's "everything up to the commit index" (which would make a
/// lagging follower skip entries that are NOT in S3). No state-machine data is ever sent.
pub struct J3zStorage {
    pub mem: MemStorage,
    pub marker: AtomicU64,
}

impl Storage for J3zStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        self.mem.initial_state()
    }
    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>, ctx: GetEntriesContext) -> raft::Result<Vec<Entry>> {
        self.mem.entries(low, high, max_size, ctx)
    }
    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.mem.term(idx)
    }
    fn first_index(&self) -> raft::Result<u64> {
        self.mem.first_index()
    }
    fn last_index(&self) -> raft::Result<u64> {
        self.mem.last_index()
    }
    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        let k = self.marker.load(Relaxed);
        if k == 0 || k < request_index {
            return Err(raft::Error::Store(raft::StorageError::SnapshotTemporarilyUnavailable));
        }
        let mut s = Snapshot::default();
        let m = s.mut_metadata();
        m.index = k;
        m.term = self.mem.term(k)?;
        m.set_conf_state(self.mem.initial_state()?.conf_state);
        Ok(s)
    }
}

// --------------------------------------------------------------------------------------------------------------- node

struct Wait {
    number: u64,
    sync: bool,
    msgs: Vec<Message>,
    /// A snapshot (metadata only) in this ready: "applied" moves to its index once it is persisted.
    snap: u64,
}

pub struct Group {
    pub rn: RawNode<J3zStorage>,
    waiting: VecDeque<Wait>,
    pending: BTreeMap<u64, (u64, AckTx)>,
    pub committed_log: Vec<(u64, u64, Bytes)>,
    pub snapshots_applied: u64,
}

pub struct Node {
    pub id: usize,
    pub groups: Vec<Group>,
    cfg: RaftCfg,
    peers: Arc<Vec<Sender<Ev>>>,
    me: Sender<Ev>,
    dl: Arc<DelayLine>,
    pub net: Arc<Net>,
    pub file: Arc<LogFile>,
    dirty: Vec<bool>,
    dirty_list: Vec<u32>,
}

/// Frame kinds in the node's log file (each inside `harness::frame`): `kind u8 | group u32 | fields`.
const K_ENTRY: u8 = 1;
const K_HS: u8 = 2;
const K_SNAP: u8 = 3;
pub const K_TRIM: u8 = 4;

pub fn rec(kind: u8, g: u32, nums: &[u64], data: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(5 + nums.len() * 8 + data.len());
    v.push(kind);
    v.extend_from_slice(&g.to_le_bytes());
    for n in nums {
        v.extend_from_slice(&n.to_le_bytes());
    }
    v.extend_from_slice(data);
    frame(&v)
}

pub fn raft_config(id: u64, c: &RaftCfg) -> Config {
    let cfg = Config {
        id,
        election_tick: c.election_tick,
        heartbeat_tick: c.heartbeat_tick,
        max_size_per_msg: 1 << 20,
        max_inflight_msgs: 256,
        check_quorum: true,
        pre_vote: true,
        skip_bcast_commit: c.skip_bcast_commit,
        ..Default::default()
    };
    cfg.validate().unwrap();
    cfg
}

/// Start the 3 node threads with `groups` groups each. Returns the inbox senders (index = harness node number).
pub fn start(groups: usize, cfg: RaftCfg, dl: Arc<DelayLine>, net: Arc<Net>, files: Vec<Arc<LogFile>>) -> Arc<Vec<Sender<Ev>>> {
    let mut txs = Vec::new();
    let mut rxs = Vec::new();
    for _ in 0..3 {
        let (tx, rx) = std::sync::mpsc::channel::<Ev>();
        txs.push(tx);
        rxs.push(rx);
    }
    let peers = Arc::new(txs);
    for (i, rx) in rxs.into_iter().enumerate() {
        let (peers, cfg, dl, net, file) = (peers.clone(), cfg.clone(), dl.clone(), net.clone(), files[i].clone());
        std::thread::Builder::new()
            .name(format!("raft-node{i}"))
            .spawn(move || {
                let logger = slog::Logger::root(slog::Discard, slog::o!());
                let rc = raft_config(i as u64 + 1, &cfg);
                let gs = (0..groups)
                    .map(|_| {
                        let st = J3zStorage { mem: MemStorage::new_with_conf_state(ConfState::from((vec![1, 2, 3], vec![]))), marker: AtomicU64::new(0) };
                        Group { rn: RawNode::new(&rc, st, &logger).unwrap(), waiting: VecDeque::new(), pending: BTreeMap::new(), committed_log: Vec::new(), snapshots_applied: 0 }
                    })
                    .collect();
                let me = peers[i].clone();
                let mut n = Node { id: i, groups: gs, cfg, peers, me, dl, net, file, dirty: vec![false; groups], dirty_list: Vec::new() };
                n.run(rx);
            })
            .unwrap();
    }
    peers
}

impl Node {
    fn mark(&mut self, g: u32) {
        if !self.dirty[g as usize] {
            self.dirty[g as usize] = true;
            self.dirty_list.push(g);
        }
    }

    fn run(&mut self, rx: Receiver<Ev>) {
        let mut next_tick = Instant::now() + self.cfg.tick;
        loop {
            let wait = next_tick.saturating_duration_since(Instant::now());
            match rx.recv_timeout(wait) {
                Ok(ev) => {
                    self.handle(ev);
                    // drain what is already queued, then process readies once (natural batching)
                    while let Ok(ev) = rx.try_recv() {
                        self.handle(ev);
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return,
            }
            let now = Instant::now();
            if now >= next_tick {
                for g in 0..self.groups.len() {
                    self.groups[g].rn.tick();
                    if self.groups[g].rn.has_ready() {
                        self.mark(g as u32);
                    }
                }
                next_tick += self.cfg.tick;
                if next_tick < now {
                    next_tick = now + self.cfg.tick;
                }
            }
            let list = std::mem::take(&mut self.dirty_list);
            for g in list {
                self.dirty[g as usize] = false;
                self.process(g);
            }
        }
    }

    fn handle(&mut self, ev: Ev) {
        match ev {
            Ev::Msg(g, b) => {
                let m = Message::parse_from_carllerche_bytes(&b).expect("decode");
                let _ = self.groups[g as usize].rn.step(m);
                self.mark(g);
            }
            Ev::Propose(g, data, ack) => {
                let gr = &mut self.groups[g as usize];
                // Application-level guard: a non-leader would *forward* the proposal to the leader (raft-rs 0.7.0
                // has no option to disable that), so the writer only proposes while it is the leader.
                if gr.rn.raft.state != StateRole::Leader {
                    let _ = ack.send(Err(anyhow::anyhow!("not leader")));
                    return;
                }
                match gr.rn.propose(vec![], data.to_vec()) {
                    Ok(()) => {
                        let idx = gr.rn.raft.raft_log.last_index();
                        let term = gr.rn.raft.term;
                        gr.pending.insert(idx, (term, ack));
                    }
                    Err(e) => {
                        let _ = ack.send(Err(anyhow::anyhow!("propose: {e}")));
                    }
                }
                self.mark(g);
            }
            Ev::Persisted(g, number) => {
                let gr = &mut self.groups[g as usize];
                let mut last = 0;
                let mut snap = 0;
                let mut out = Vec::new();
                while let Some(w) = gr.waiting.front() {
                    if w.sync && w.number > number {
                        break;
                    }
                    let w = gr.waiting.pop_front().unwrap();
                    out.extend(w.msgs);
                    last = w.number;
                    snap = snap.max(w.snap);
                }
                if last > 0 {
                    gr.rn.on_persist_ready(last);
                }
                if snap > 0 {
                    gr.rn.advance_apply_to(snap);
                }
                for m in out {
                    self.send(g, m);
                }
                self.mark(g);
            }
            Ev::Ctl(f) => {
                f(self);
                for g in 0..self.groups.len() {
                    self.mark(g as u32);
                }
            }
        }
    }

    fn send(&self, g: u32, m: Message) {
        let (from, to) = (self.id, m.to as usize - 1);
        if !self.net.ok(from, to) {
            return;
        }
        let bytes = Bytes::from(m.write_to_bytes().expect("encode"));
        let (tx, net) = (self.peers[to].clone(), self.net.clone());
        let is_snap = m.get_msg_type() == MessageType::MsgSnapshot;
        let me = self.me.clone();
        self.dl.send(
            from,
            to,
            bytes.len(),
            Box::new(move || {
                if net.ok(from, to) {
                    let _ = tx.send(Ev::Msg(g, bytes));
                    if is_snap {
                        // raft-rs asks the application to report snapshot delivery (Ready::messages doc).
                        let _ = me.send(Ev::Ctl(Box::new(move |n: &mut Node| {
                            n.groups[g as usize].rn.report_snapshot(to as u64 + 1, raft::SnapshotStatus::Finish)
                        })));
                    }
                }
            }),
        );
    }

    fn process(&mut self, g: u32) {
        loop {
            let gr = &mut self.groups[g as usize];
            if !gr.rn.has_ready() {
                break;
            }
            let mut rd = gr.rn.ready();
            let number = rd.number();
            // Leader: append messages go out before the local fsync (Raft thesis 10.2.1).
            let msgs = rd.take_messages();
            let mut bytes = Vec::new();
            let mut snap = 0;
            if !rd.snapshot().is_empty() {
                snap = rd.snapshot().get_metadata().index;
                let s = rd.snapshot().clone();
                bytes.extend(rec(K_SNAP, g, &[s.get_metadata().index, s.get_metadata().term], &[]));
                gr.rn.mut_store().mem.wl().apply_snapshot(s).expect("apply snapshot");
                gr.snapshots_applied += 1;
            }
            let committed = rd.take_committed_entries();
            let ents = rd.take_entries();
            if !ents.is_empty() {
                for e in &ents {
                    bytes.extend(rec(K_ENTRY, g, &[e.index, e.term], &e.data));
                }
                gr.rn.mut_store().mem.wl().append(&ents).expect("append");
            }
            if let Some(hs) = rd.hs() {
                gr.rn.mut_store().mem.wl().set_hardstate(hs.clone());
                if rd.must_sync() {
                    bytes.extend(hs_rec(g, hs));
                }
            }
            let pmsgs = rd.take_persisted_messages();
            let sync = rd.must_sync();
            gr.rn.advance_append_async(rd);
            // committed entries (only those also persisted locally, raft-rs 0.7.0 rule)
            let applied = committed.last().map(|e| e.index);
            for e in committed {
                if self.cfg.ack == Ack::Apply {
                    if let Some((t, _)) = gr.pending.get(&e.index) {
                        let ok = *t == e.term;
                        let (_, a) = gr.pending.remove(&e.index).unwrap();
                        let _ = a.send(if ok { Ok(()) } else { Err(anyhow::anyhow!("overwritten")) });
                    }
                }
                if self.cfg.record {
                    gr.committed_log.push((e.index, e.term, e.data.clone()));
                }
            }
            if let Some(i) = applied {
                gr.rn.advance_apply_to(i);
            }
            if sync {
                gr.waiting.push_back(Wait { number, sync: true, msgs: pmsgs, snap });
                let me = self.me.clone();
                self.file.append(
                    bytes,
                    Box::new(move || {
                        let _ = me.send(Ev::Persisted(g, number));
                    }),
                );
            } else if gr.waiting.is_empty() {
                gr.rn.on_persist_ready(number);
                for m in pmsgs {
                    self.send(g, m);
                }
            } else {
                gr.waiting.push_back(Wait { number, sync: false, msgs: pmsgs, snap });
            }
            for m in msgs {
                self.send(g, m);
            }
        }
        self.ack_committed(g);
    }

    /// `Ack::Commit`: acknowledge every pending write whose index is covered by the commit index (a quorum has it
    /// fdatasync'd; the leader counts itself only after `on_persist_ready`) with the term it was proposed in.
    fn ack_committed(&mut self, g: u32) {
        let gr = &mut self.groups[g as usize];
        let committed = gr.rn.raft.raft_log.committed;
        while let Some((&idx, _)) = gr.pending.first_key_value() {
            if idx > committed {
                break;
            }
            let ok = gr.rn.raft.raft_log.term(idx).ok() == Some(gr.pending[&idx].0);
            if ok && self.cfg.ack == Ack::Apply {
                break; // acknowledged when take_committed_entries hands it out (after the local fsync)
            }
            let (_, a) = gr.pending.remove(&idx).unwrap();
            let _ = a.send(if ok { Ok(()) } else { Err(anyhow::anyhow!("overwritten by a later term")) });
        }
    }
}

fn hs_rec(g: u32, hs: &HardState) -> Vec<u8> {
    rec(K_HS, g, &[hs.term, hs.vote, hs.commit], &[])
}
