//! Driver: one tokio task per OmniPaxos server (per group), the delay line between servers, the node's `LogFile` for
//! durability.
//!
//! "No acknowledgement before fdatasync" is enforced here:
//! 1. After every step (a batch of queued events: appends, incoming messages, a tick) the driver takes the storage
//!    writes of that step and the outgoing messages. If there were writes, they go to the node's `LogFile` (write +
//!    fdatasync, group commit) and every message that may depend on them (Promise, Accepted, AcceptSync, Prepare, ...)
//!    is sent only from that file's callback, i.e. after fdatasync returned. Later messages of the same server wait
//!    behind them (through the same file queue) so nothing overtakes a held message.
//! 2. Exception, for latency: a leader's AcceptDecide / Decide (and BLE heartbeats, which carry no durable state) leave
//!    at once, as Raft lets a leader send entries before its own fsync. OmniPaxos counts the leader's own vote at
//!    append time, before it is durable, so the driver withholds an incoming `Accepted(k)` from the leader until the
//!    leader's own log is fdatasync'd up to `k`. Every decision is then made from 2 durable copies: either the leader
//!    (durable >= k) and one follower, or two followers (each replies only after its own fdatasync).
use crate::storage::{DurableStorage, Pending, PendingRef, E};
use bytes::Bytes;
use j3z_harness::{DelayLine, LogFile};
use omnipaxos::messages::sequence_paxos::{PaxosMessage, PaxosMsg};
use omnipaxos::messages::Message;
use omnipaxos::{ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use serde_json::json;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;

pub type Op = OmniPaxos<E, DurableStorage>;
pub type Ack = oneshot::Sender<anyhow::Result<usize>>;

pub enum Ev {
    Msg(Message<E>),
    /// The node's file has everything of this server up to this accepted index fdatasync'd.
    Durable(usize),
    /// Client write (only accepted by the leader in the accept phase); acknowledged when decided.
    Append(Bytes, Ack),
    /// Run arbitrary code on the server's task (scenarios).
    Ctl(Box<dyn FnOnce(&mut Server) + Send>),
}

#[derive(Clone, Debug)]
pub struct Opts {
    pub tick: Duration,
    pub election_ticks: u64,
    pub resend_ticks: u64,
    /// false: BLE never times out (election_tick_timeout = u64::MAX); leadership only through `try_become_leader`.
    pub ble: bool,
}

impl Opts {
    pub fn from_env(force_ble_off: bool) -> Self {
        let env = |k: &str, d: u64| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
        let ble = !force_ble_off && std::env::var("OMNI_BLE").map_or(true, |v| v != "off");
        Opts { tick: Duration::from_millis(env("OMNI_TICK_MS", 100)), election_ticks: env("OMNI_ELECTION_TICKS", 1), resend_ticks: env("OMNI_RESEND_TICKS", 10), ble }
    }
    pub fn json(&self) -> serde_json::Value {
        json!({
            "tick_ms": self.tick.as_millis() as u64,
            "ble": if self.ble { "on" } else { "off (election_tick_timeout = u64::MAX, leader by try_become_leader on node 0)" },
            "election_tick_timeout": if self.ble { self.election_ticks } else { u64::MAX },
            "heartbeat_round_ms": if self.ble { json!(self.tick.as_millis() as u64 * self.election_ticks) } else { json!("none") },
            "resend_message_tick_timeout": self.resend_ticks,
            "batch_size": 1,
            "leader_priority": "node 0 (pid 1) = 1, others 0",
        })
    }
}

/// Network: the delay line plus routing to (group, node); `cut[from][to]` drops a link (scenarios only).
pub struct Net {
    pub dl: Arc<DelayLine>,
    pub routes: Vec<[UnboundedSender<Ev>; 3]>,
    pub cut: Arc<[[AtomicBool; 3]; 3]>,
}

fn ents(v: &[E]) -> usize {
    v.iter().map(|e| e.0.len() + 12).sum()
}

/// Approximate wire size (only counted by the delay line; messages are not serialized in this spike).
fn msg_size(m: &Message<E>) -> usize {
    40 + match m {
        Message::BLE(_) => 0,
        Message::SequencePaxos(p) => match &p.msg {
            PaxosMsg::AcceptDecide(a) => ents(&a.entries),
            PaxosMsg::AcceptSync(a) => ents(&a.log_sync.suffix),
            PaxosMsg::Promise(pr) => pr.log_sync.as_ref().map_or(0, |l| ents(&l.suffix)),
            PaxosMsg::ProposalForward(v) => ents(v),
            _ => 0,
        },
    }
}

impl Net {
    /// Every message between two servers goes through the delay line (same-server messages too, with no delay).
    pub fn send(&self, g: usize, m: Message<E>) {
        let from = (m.get_sender() - 1) as usize;
        let to = (m.get_receiver() - 1) as usize;
        if self.cut[from][to].load(SeqCst) {
            return;
        }
        let (tx, cut, size) = (self.routes[g][to].clone(), self.cut.clone(), msg_size(&m));
        self.dl.send(from, to, size, Box::new(move || {
            if !cut[from][to].load(SeqCst) {
                let _ = tx.send(Ev::Msg(m));
            }
        }));
    }
}

/// Which groups have a leader in the accept phase.
pub struct Ready {
    pub groups: Vec<AtomicBool>,
    pub count: AtomicUsize,
}

pub struct Server {
    pub g: usize,
    pub node: usize,
    pub op: Op,
    pub pending: PendingRef,
    file: Arc<LogFile>,
    pub net: Arc<Net>,
    me: UnboundedSender<Ev>,
    /// Number of file appends in flight that carry held messages (ordering: nothing may overtake them).
    held_inflight: Arc<AtomicU64>,
    /// This server's accepted index as of its last completed fdatasync.
    pub durable: usize,
    gated: Vec<Message<E>>,
    waiters: VecDeque<(usize, Ack)>,
    outbuf: Vec<Message<E>>,
    ready: Arc<Ready>,
}

impl Server {
    pub fn pid(&self) -> u64 {
        self.node as u64 + 1
    }

    pub fn is_leader(&self) -> bool {
        self.op.get_current_leader() == Some((self.pid(), true))
    }

    fn handle(&mut self, ev: Ev) {
        match ev {
            Ev::Msg(m) => {
                if let Message::SequencePaxos(PaxosMessage { msg: PaxosMsg::Accepted(a), .. }) = &m {
                    if a.accepted_idx > self.durable {
                        self.gated.push(m);
                        return;
                    }
                }
                self.op.handle_incoming(m);
            }
            Ev::Durable(d) => {
                self.durable = d;
                if !self.gated.is_empty() {
                    for m in std::mem::take(&mut self.gated) {
                        match &m {
                            Message::SequencePaxos(PaxosMessage { msg: PaxosMsg::Accepted(a), .. }) if a.accepted_idx > d => self.gated.push(m),
                            _ => self.op.handle_incoming(m),
                        }
                    }
                }
            }
            Ev::Append(p, ack) => {
                if !self.is_leader() {
                    let _ = ack.send(Err(anyhow::anyhow!("not the leader in the accept phase")));
                    return;
                }
                match self.op.append(E(p)) {
                    Ok(()) => self.waiters.push_back((self.op.get_accepted_idx(), ack)),
                    Err(e) => {
                        let _ = ack.send(Err(anyhow::anyhow!("append refused: {e:?}")));
                    }
                }
            }
            Ev::Ctl(f) => f(self),
        }
    }

    /// After each step: persist this step's writes, send messages (held ones only after fdatasync), ack decided writes.
    fn after_step(&mut self) {
        let batch = self.pending.lock().unwrap().take_batch(self.g as u32);
        self.op.take_outgoing_messages(&mut self.outbuf);
        if batch.is_some() || !self.outbuf.is_empty() {
            let mut held = Vec::new();
            let blocked = self.held_inflight.load(SeqCst) > 0;
            for m in self.outbuf.drain(..) {
                let early = match &m {
                    Message::BLE(_) => true,
                    Message::SequencePaxos(p) => !blocked && held.is_empty() && matches!(p.msg, PaxosMsg::AcceptDecide(_) | PaxosMsg::Decide(_)),
                };
                if early {
                    self.net.send(self.g, m);
                } else {
                    held.push(m);
                }
            }
            match batch {
                Some(bytes) => {
                    let acc = self.op.get_accepted_idx();
                    let (net, g, me, inflight) = (self.net.clone(), self.g, self.me.clone(), self.held_inflight.clone());
                    let has_held = !held.is_empty();
                    if has_held {
                        inflight.fetch_add(1, SeqCst);
                    }
                    self.file.append(bytes, Box::new(move || {
                        for m in held {
                            net.send(g, m);
                        }
                        if has_held {
                            inflight.fetch_sub(1, SeqCst);
                        }
                        let _ = me.send(Ev::Durable(acc));
                    }));
                }
                None if held.is_empty() => {}
                None if !blocked => {
                    for m in held {
                        self.net.send(self.g, m);
                    }
                }
                None => {
                    // Keep order behind held messages still waiting for an fdatasync of this node's file.
                    let (net, g, inflight) = (self.net.clone(), self.g, self.held_inflight.clone());
                    inflight.fetch_add(1, SeqCst);
                    self.file.append(Vec::new(), Box::new(move || {
                        for m in held {
                            net.send(g, m);
                        }
                        inflight.fetch_sub(1, SeqCst);
                    }));
                }
            }
        }
        if !self.waiters.is_empty() {
            let d = self.op.get_decided_idx();
            while self.waiters.front().map_or(false, |(i, _)| *i <= d) {
                let (i, ack) = self.waiters.pop_front().unwrap();
                let _ = ack.send(Ok(i));
            }
        }
        if !self.ready.groups[self.g].load(SeqCst) && self.is_leader() && !self.ready.groups[self.g].swap(true, SeqCst) {
            self.ready.count.fetch_add(1, SeqCst);
        }
    }
}

async fn run(mut s: Server, mut rx: UnboundedReceiver<Ev>, tick: Duration) {
    let mut iv = tokio::time::interval_at(tokio::time::Instant::now() + tick, tick);
    iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    s.after_step();
    loop {
        tokio::select! {
            biased;
            ev = rx.recv() => {
                let Some(ev) = ev else { return };
                s.handle(ev);
                for _ in 0..255 {
                    match rx.try_recv() {
                        Ok(ev) => s.handle(ev),
                        Err(_) => break,
                    }
                }
            }
            _ = iv.tick() => s.op.tick(),
        }
        s.after_step();
    }
}

pub struct Cluster {
    pub net: Arc<Net>,
    pub ready: Arc<Ready>,
    pub files: Vec<Arc<LogFile>>,
    pub opts: Opts,
}

impl Cluster {
    /// `groups` independent OmniPaxos groups of 3 servers (pids 1,2,3 = harness nodes 0,1,2), one shared file per node.
    pub fn start(groups: usize, dl: Arc<DelayLine>, dir: &std::path::Path, opts: Opts) -> Self {
        let files: Vec<Arc<LogFile>> = (0..3).map(|i| LogFile::create(&dir.join(format!("n{i}.log"))).unwrap()).collect();
        let mut routes = Vec::with_capacity(groups);
        let mut rxs = Vec::with_capacity(groups);
        for _ in 0..groups {
            let (a, b, c) = (unbounded_channel(), unbounded_channel(), unbounded_channel());
            routes.push([a.0, b.0, c.0]);
            rxs.push([a.1, b.1, c.1]);
        }
        let cut: Arc<[[AtomicBool; 3]; 3]> = Arc::new(Default::default());
        let net = Arc::new(Net { dl, routes, cut });
        let ready = Arc::new(Ready { groups: (0..groups).map(|_| AtomicBool::new(false)).collect(), count: AtomicUsize::new(0) });
        for (g, rx3) in rxs.into_iter().enumerate() {
            for (node, rx) in rx3.into_iter().enumerate() {
                let pid = node as u64 + 1;
                let cfg = OmniPaxosConfig {
                    cluster_config: ClusterConfig { configuration_id: 1, nodes: vec![1, 2, 3], flexible_quorum: None },
                    server_config: ServerConfig {
                        pid,
                        election_tick_timeout: if opts.ble { opts.election_ticks } else { u64::MAX },
                        resend_message_tick_timeout: opts.resend_ticks,
                        buffer_size: 1024,
                        batch_size: 1,
                        leader_priority: if node == 0 { 1 } else { 0 },
                        ..Default::default()
                    },
                };
                let pending: PendingRef = Arc::new(Mutex::new(Pending::default()));
                let op = cfg.build(DurableStorage::new(g as u32, pending.clone())).expect("omnipaxos config");
                let s = Server {
                    g,
                    node,
                    op,
                    pending,
                    file: files[node].clone(),
                    net: net.clone(),
                    me: net.routes[g][node].clone(),
                    held_inflight: Arc::new(AtomicU64::new(0)),
                    durable: 0,
                    gated: Vec::new(),
                    waiters: VecDeque::new(),
                    outbuf: Vec::new(),
                    ready: ready.clone(),
                };
                tokio::spawn(run(s, rx, opts.tick));
            }
        }
        let cl = Cluster { net, ready, files, opts };
        if !cl.opts.ble {
            for g in 0..groups {
                cl.ctl_nowait(g, 0, |s| s.op.try_become_leader());
            }
        }
        cl
    }

    pub fn ctl_nowait(&self, g: usize, node: usize, f: impl FnOnce(&mut Server) + Send + 'static) {
        let _ = self.net.routes[g][node].send(Ev::Ctl(Box::new(f)));
    }

    /// Run `f` on server (g, node) and return its result.
    pub async fn ctl<R: Send + 'static>(&self, g: usize, node: usize, f: impl FnOnce(&mut Server) -> R + Send + 'static) -> R {
        let (tx, rx) = oneshot::channel();
        self.ctl_nowait(g, node, move |s| {
            let _ = tx.send(f(s));
        });
        rx.await.expect("server task gone")
    }

    /// Wait until every group has a leader in the accept phase; returns false on timeout.
    pub async fn wait_ready(&self, groups: usize, timeout: Duration) -> bool {
        let t0 = std::time::Instant::now();
        while self.ready.count.load(SeqCst) < groups {
            if t0.elapsed() > timeout {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        true
    }

    /// Client write on (g, node): appended there, acknowledged when decided (2 of 3 fdatasync'd, see module doc).
    pub fn append(&self, g: usize, node: usize, p: Bytes) -> oneshot::Receiver<anyhow::Result<usize>> {
        let (tx, rx) = oneshot::channel();
        let _ = self.net.routes[g][node].send(Ev::Append(p, tx));
        rx
    }

    pub fn fsyncs(&self) -> Vec<u64> {
        self.files.iter().map(|f| f.fsyncs.load(SeqCst)).collect()
    }
}
