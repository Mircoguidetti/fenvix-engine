//! The simulated world: 3 journal node slots (one per AZ), 3 writer servers (one per AZ), S3 (lease objects with
//! conditional writes + database objects with writer fencing), a network, a fault injector, and the invariant
//! checker run after every event. Single-threaded, one PRNG, events ordered by (time, sequence): a seed replays
//! exactly.
//!
//! Disk model of a node: every record goes to the page cache (`mem`, the node's live state) and is durable (`dur`)
//! only when an fdatasync covering it completes. A crash keeps a random prefix of the not-yet-synced records (page
//! cache write-back may or may not have happened). A failed fdatasync crashes the node (the protocol's rule) or, with
//! the `ignore_fsync_error` mutation, silently loses those records while the node goes on.

use crate::rng::Rng;
use bytes::Bytes;
use journal3z_core::*;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

pub type Time = u64; // microseconds
pub const MS: Time = 1_000;
pub const SEC: Time = 1_000_000;
const WADDR: Addr = 1_000_000;
const BARRIER_VAL: u64 = u64::MAX;

#[derive(Clone, Debug)]
pub struct Params {
    pub dbs: u64,
    pub lease_ttl: Time,
    pub skew_allow: Time,
    pub max_clock_offset: Time,
    pub write_gap: (Time, Time),
    pub upload_every: (Time, Time),
    pub fault_every: (Time, Time),
    pub max_steps: u64,
    pub max_time: Time,
    /// Allow a second disk loss while the first is not yet repaired (double-failure exploration: loss expected).
    pub double: bool,
    /// Only invariant (a) (durability) — used to show the durability check alone catches the negative controls.
    pub only_a: bool,
    pub trace: bool,
    /// No faults at all (liveness baseline).
    pub calm: bool,
    /// Availability accounting: per database, time with at least one active writer.
    pub avail: bool,
    /// Liveness: at this fraction of max_steps every fault is healed and no new fault is injected; every database
    /// must then acknowledge writes again (time to the first acknowledgement after the heal is reported).
    pub heal_frac: f64,
}

impl Default for Params {
    fn default() -> Self {
        Params {
            dbs: 3,
            lease_ttl: 2 * SEC,
            skew_allow: 150 * MS,
            max_clock_offset: 1500 * MS,
            write_gap: (2 * MS, 20 * MS),
            upload_every: (100 * MS, 800 * MS),
            fault_every: (50 * MS, 400 * MS),
            max_steps: 200_000,
            max_time: 600 * SEC,
            double: false,
            only_a: false,
            trace: false,
            calm: false,
            avail: true,
            heal_frac: 0.8,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Ent {
    Node(usize),
    Writer(usize),
    S3,
}

#[derive(Clone, Debug)]
pub enum S3Op {
    Acquire { db: DbId },
    Renew { db: DbId },
    Release { db: DbId },
    OpenStore { db: DbId, epoch: Epoch },
    Upload { db: DbId, epoch: Epoch, upto: Idx, content: Vec<(Idx, u64)> },
    Reconfig { db: DbId, members: Vec<NodeId> },
}

#[derive(Clone, Debug)]
pub enum S3Res {
    Lease { ok: bool, seq: u64, members: Vec<NodeId>, sent_local: Time },
    Store { ok: bool, applied: Idx, epoch: Epoch },
    Upload { ok: bool, upto: Idx, epoch: Epoch },
    Unreachable,
}

#[derive(Clone, Debug)]
pub enum Ev {
    DeliverToNode { slot: usize, node: NodeId, from: usize, msg: Msg },
    DeliverToWriter { w: usize, gen: u64, from_node: NodeId, msg: Msg },
    Fsync { slot: usize, gen: u64 },
    NodeUp { slot: usize, gen: u64 },
    Replace { slot: usize },
    WTick { w: usize, gen: u64 },
    WUp { w: usize },
    Unfreeze { w: usize, token: u64 },
    S3Exec { w: usize, gen: u64, op: S3Op, sent_local: Time },
    S3Reply { w: usize, gen: u64, db: DbId, op: &'static str, res: S3Res },
    ClientWrite { db: DbId },
    Fault,
    Heal { id: u64 },
    ChaosEnd { token: u64 },
    SlowEnd { slot: usize },
    FullEnd { slot: usize },
    S3Back { token: u64 },
}

struct Item {
    t: Time,
    seq: u64,
    ev: Ev,
}
impl PartialEq for Item {
    fn eq(&self, o: &Self) -> bool {
        (self.t, self.seq) == (o.t, o.seq)
    }
}
impl Eq for Item {}
impl PartialOrd for Item {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Item {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        (o.t, o.seq).cmp(&(self.t, self.seq))
    }
}

pub struct Slot {
    pub id: NodeId,
    pub up: bool,
    pub gen: u64,
    pub mem: NodeCore,
    pub dur: NodeCore,
    /// records not yet handed to an fdatasync
    pending: Vec<Rec>,
    /// records of the fdatasync in flight
    inflight: Vec<Rec>,
    syncing: bool,
    written: u64,
    synced: u64,
    waiting: Vec<(u64, Addr, Msg)>,
    pub slow: bool,
    pub full: bool,
    pub fail_next: bool,
    /// the node's disk is gone and no replacement exists yet
    pub dead: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum St {
    Idle,
    Acquiring,
    OpeningStore,
    Opening,
    Active,
    Draining,
    Reconfiguring,
    Releasing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Goal {
    None,
    Sleep,
    Move,
    Reconfig,
}

pub struct WDb {
    pub st: St,
    pub core: Option<WriterDb>,
    pub epoch: Epoch,
    pub members: Vec<NodeId>,
    pub valid_until: Time,
    last_renew_sent: Time,
    renewing: bool,
    pub pending: BTreeMap<Idx, u64>,
    uploading: bool,
    next_upload: Time,
    goal: Goal,
    drain_upload_sent: bool,
    last_try: Time,
    commit_seen: Idx,
}

impl WDb {
    fn new() -> Self {
        WDb { st: St::Idle, core: None, epoch: 0, members: vec![], valid_until: 0, last_renew_sent: 0, renewing: false, pending: BTreeMap::new(), uploading: false, next_upload: 0, goal: Goal::None, drain_upload_sent: false, last_try: 0, commit_seen: 0 }
    }
}

pub struct Writer {
    pub up: bool,
    pub frozen: bool,
    freeze_token: u64,
    deferred: Vec<Ev>,
    pub gen: u64,
    pub offset: i64,
    pub dbs: BTreeMap<DbId, WDb>,
    ticks: u64,
}

pub struct Lease {
    holder: Option<(usize, u64)>,
    expires: Time,
    pub seq: u64,
    pub members: Vec<NodeId>,
}

pub struct Store {
    slate_epoch: Epoch,
    pub applied: Idx,
    pub content: BTreeMap<Idx, u64>,
}

#[derive(Default, Debug, Clone)]
pub struct Stats {
    pub steps: u64,
    pub acks: u64,
    pub client_writes: u64,
    pub write_attempts: u64,
    pub opens: u64,
    pub takeovers: u64,
    pub stale_aborts: u64,
    pub uploads: u64,
    pub reconfigs: u64,
    pub sleeps: u64,
    pub moves: u64,
    pub late_acks: u64,
    pub mismatches_sent: u64,
    pub fenced_replies: u64,
    pub faults: BTreeMap<&'static str, u64>,
    pub unprotected_steps: u64,
    pub lost_in_unprotected: u64,
    pub messages: u64,
    pub healed_at: Option<Time>,
    /// db -> first acknowledgement after the heal (µs after it)
    pub recovered_after_heal: BTreeMap<DbId, Time>,
}

pub struct Violation {
    pub step: u64,
    pub time: Time,
    pub inv: &'static str,
    pub what: String,
}

/// Checker bookkeeping (the checks themselves are World methods: they need the whole world).
#[derive(Default)]
struct Chk {
    /// db -> idx -> (write id, epoch of the writer that acknowledged it)
    acked: BTreeMap<DbId, BTreeMap<Idx, (u64, Epoch)>>,
    /// db -> idx -> (epoch, value) of committed entries
    committed: BTreeMap<DbId, BTreeMap<Idx, (Epoch, u64)>>,
    /// db -> idx -> epoch of the first writer that committed it
    committer: BTreeMap<DbId, BTreeMap<Idx, Epoch>>,
    /// db -> epoch -> barrier index (committed barriers)
    barriers: BTreeMap<DbId, BTreeMap<Epoch, Idx>>,
    /// (node, db) -> highest epoch the node has told someone it promised (Promise or Ack sent)
    promised_seen: BTreeMap<(NodeId, DbId), Epoch>,
    /// (db, idx, epoch) -> value seen in any node's log
    entries: BTreeMap<(DbId, Idx, Epoch), u64>,
}

pub struct World {
    pub p: Params,
    pub m: Mutations,
    pub rng: Rng,
    pub now: Time,
    q: BinaryHeap<Item>,
    qseq: u64,
    pub slots: Vec<Slot>,
    pub writers: Vec<Writer>,
    pub leases: BTreeMap<DbId, Lease>,
    pub stores: BTreeMap<DbId, Store>,
    next_node_id: NodeId,
    dead_ids: BTreeSet<NodeId>,
    preferred: BTreeMap<DbId, usize>,
    blocked: BTreeMap<u64, Vec<(Ent, Ent)>>,
    next_block: u64,
    drop_p: f64,
    dup_p: f64,
    extra_delay: Time,
    chaos_token: u64,
    s3_down: bool,
    s3_token: u64,
    next_write_id: u64,
    chk: Chk,
    pub stats: Stats,
    pub violation: Option<Violation>,
    pub trace: Vec<String>,
}

fn val(e: &Entry) -> u64 {
    if e.barrier {
        BARRIER_VAL
    } else {
        u64::from_le_bytes(e.data[..8].try_into().unwrap())
    }
}

impl World {
    pub fn new(seed: u64, p: Params, m: Mutations) -> Self {
        let mut w = World {
            p: p.clone(),
            m,
            rng: Rng::new(seed),
            now: 0,
            q: BinaryHeap::new(),
            qseq: 0,
            slots: vec![],
            writers: vec![],
            leases: BTreeMap::new(),
            stores: BTreeMap::new(),
            next_node_id: 4,
            dead_ids: BTreeSet::new(),
            preferred: BTreeMap::new(),
            blocked: BTreeMap::new(),
            next_block: 1,
            drop_p: 0.001,
            dup_p: 0.001,
            extra_delay: 0,
            chaos_token: 0,
            s3_down: false,
            s3_token: 0,
            next_write_id: 1,
            chk: Chk::default(),
            stats: Stats::default(),
            violation: None,
            trace: vec![],
        };
        for i in 0..3 {
            let id = i as NodeId + 1;
            w.slots.push(Slot {
                id,
                up: true,
                gen: 0,
                mem: NodeCore::new(id, m),
                dur: NodeCore::new(id, m),
                pending: vec![],
                inflight: vec![],
                syncing: false,
                written: 0,
                synced: 0,
                waiting: vec![],
                slow: false,
                full: false,
                fail_next: false,
                dead: false,
            });
            w.writers.push(Writer { up: true, frozen: false, freeze_token: 0, deferred: vec![], gen: 0, offset: 0, dbs: (0..p.dbs).map(|d| (d, WDb::new())).collect(), ticks: 0 });
        }
        for d in 0..p.dbs {
            w.leases.insert(d, Lease { holder: None, expires: 0, seq: 0, members: vec![1, 2, 3] });
            w.stores.insert(d, Store { slate_epoch: 0, applied: 0, content: BTreeMap::new() });
            let pw = w.rng.range(0, 2) as usize;
            w.preferred.insert(d, pw);
            w.chk.acked.insert(d, BTreeMap::new());
            w.chk.committed.insert(d, BTreeMap::new());
            w.chk.committer.insert(d, BTreeMap::new());
            w.chk.barriers.insert(d, BTreeMap::new());
        }
        for i in 0..3 {
            let t = w.rng.range(0, 5 * MS);
            w.at(t, Ev::WTick { w: i, gen: 0 });
        }
        for d in 0..p.dbs {
            let t = w.rng.range(10 * MS, 50 * MS);
            w.at(t, Ev::ClientWrite { db: d });
        }
        let t = w.rng.range(p.fault_every.0, p.fault_every.1);
        if !p.calm {
            w.at(500 * MS + t, Ev::Fault);
        }
        w
    }

    fn at(&mut self, t: Time, ev: Ev) {
        self.qseq += 1;
        self.q.push(Item { t, seq: self.qseq, ev });
    }

    fn after(&mut self, d: Time, ev: Ev) {
        let t = self.now + d;
        self.at(t, ev);
    }

    fn local(&self, w: usize) -> Time {
        (self.now as i64 + self.writers[w].offset).max(0) as Time
    }

    fn fault(&mut self, name: &'static str) {
        *self.stats.faults.entry(name).or_default() += 1;
        if self.p.trace {
            self.trace.push(format!("{} fault {name}", self.now));
        }
    }

    fn violate(&mut self, inv: &'static str, what: String) {
        if self.violation.is_none() {
            if self.p.only_a && inv != "a" && inv != "a/recovery" {
                return;
            }
            self.violation = Some(Violation { step: self.stats.steps, time: self.now, inv, what });
        }
    }

    // ------------------------------------------------------------------------------------------------ network

    fn is_blocked(&self, a: Ent, b: Ent) -> bool {
        self.blocked.values().any(|v| v.iter().any(|&(x, y)| x == a && y == b))
    }

    fn net_delay(&mut self) -> Time {
        let mut d = self.rng.range(300, 1500);
        if self.rng.chance(0.01) {
            d += self.rng.range(1 * MS, 30 * MS);
        }
        d + if self.extra_delay > 0 { self.rng.range(0, self.extra_delay) } else { 0 }
    }

    fn send_to_node(&mut self, w: usize, node: NodeId, msg: Msg) {
        self.stats.messages += 1;
        let Some(slot) = self.slots.iter().position(|s| s.id == node) else { return };
        if self.is_blocked(Ent::Writer(w), Ent::Node(slot)) || self.rng.chance(self.drop_p) {
            return;
        }
        let copies = if self.rng.chance(self.dup_p) { 2 } else { 1 };
        for _ in 0..copies {
            let d = self.net_delay();
            self.after(d, Ev::DeliverToNode { slot, node, from: w, msg: msg.clone() });
        }
    }

    fn send_to_writer(&mut self, slot: usize, w: usize, msg: Msg) {
        self.stats.messages += 1;
        if self.is_blocked(Ent::Node(slot), Ent::Writer(w)) || self.rng.chance(self.drop_p) {
            return;
        }
        let (gen, node) = (self.writers[w].gen, self.slots[slot].id);
        let copies = if self.rng.chance(self.dup_p) { 2 } else { 1 };
        for _ in 0..copies {
            let d = self.net_delay();
            self.after(d, Ev::DeliverToWriter { w, gen, from_node: node, msg: msg.clone() });
        }
    }

    fn s3(&mut self, w: usize, op: S3Op) {
        let sent_local = self.local(w);
        let gen = self.writers[w].gen;
        let d = self.rng.range(3 * MS, 30 * MS);
        self.after(d, Ev::S3Exec { w, gen, op, sent_local });
    }

    // ------------------------------------------------------------------------------------------------ main loop

    /// Run until the step/time budget or the first violation. Returns false on violation.
    pub fn run(&mut self) -> bool {
        while let Some(it) = self.q.pop() {
            if self.stats.steps >= self.p.max_steps || it.t > self.p.max_time {
                break;
            }
            self.now = it.t;
            self.stats.steps += 1;
            if self.stats.healed_at.is_none() && self.stats.steps as f64 >= self.p.heal_frac * self.p.max_steps as f64 {
                self.heal_all();
            }
            self.step(it.ev);
            if self.violation.is_none() {
                self.check_durability();
            }
            if self.violation.is_some() {
                return false;
            }
        }
        true
    }

    fn step(&mut self, ev: Ev) {
        // events for a frozen writer wait until it resumes
        let target = match &ev {
            Ev::DeliverToWriter { w, .. } | Ev::WTick { w, .. } | Ev::S3Reply { w, .. } => Some(*w),
            _ => None,
        };
        if let Some(w) = target {
            if self.writers[w].frozen {
                self.writers[w].deferred.push(ev);
                return;
            }
        }
        match ev {
            Ev::DeliverToNode { slot, node, from, msg } => self.node_recv(slot, node, from, msg),
            Ev::DeliverToWriter { w, gen, from_node, msg } => {
                let cut = self.slots.iter().position(|s| s.id == from_node).map_or(false, |s| self.is_blocked(Ent::Node(s), Ent::Writer(w)));
                if self.writers[w].up && self.writers[w].gen == gen && !cut {
                    self.writer_recv(w, from_node, msg);
                }
            }
            Ev::Fsync { slot, gen } => self.fsync_done(slot, gen),
            Ev::NodeUp { slot, gen } => {
                let s = &mut self.slots[slot];
                if s.gen == gen && !s.dead {
                    s.up = true;
                }
            }
            Ev::Replace { slot } => self.replace(slot),
            Ev::WTick { w, gen } => {
                if self.writers[w].up && self.writers[w].gen == gen {
                    self.writer_tick(w);
                    let d = self.rng.range(4 * MS, 6 * MS);
                    self.after(d, Ev::WTick { w, gen });
                }
            }
            Ev::WUp { w } => {
                let wr = &mut self.writers[w];
                wr.up = true;
                wr.gen += 1;
                wr.dbs = (0..self.p.dbs).map(|d| (d, WDb::new())).collect();
                let gen = wr.gen;
                self.after(1 * MS, Ev::WTick { w, gen });
            }
            Ev::Unfreeze { w, token } => {
                if self.writers[w].freeze_token == token && self.writers[w].frozen {
                    self.writers[w].frozen = false;
                    let mut evs = std::mem::take(&mut self.writers[w].deferred);
                    // a resumed process may handle its queued network input before its timers, or after
                    if self.rng.chance(0.5) {
                        let (net, other): (Vec<Ev>, Vec<Ev>) = evs.into_iter().partition(|e| matches!(e, Ev::DeliverToWriter { .. }));
                        evs = net.into_iter().chain(other).collect();
                    }
                    for e in evs {
                        self.at(self.now, e);
                    }
                }
            }
            Ev::S3Exec { w, gen, op, sent_local } => self.s3_exec(w, gen, op, sent_local),
            Ev::S3Reply { w, gen, db, op, res } => {
                if self.writers[w].up && self.writers[w].gen == gen {
                    self.s3_reply(w, db, op, res);
                }
            }
            Ev::ClientWrite { db } => {
                self.client_write(db);
                let d = self.rng.range(self.p.write_gap.0, self.p.write_gap.1);
                self.after(d, Ev::ClientWrite { db });
            }
            Ev::Fault => {
                if self.stats.healed_at.is_some() {
                    return;
                }
                self.inject_fault();
                let d = self.rng.range(self.p.fault_every.0, self.p.fault_every.1);
                self.after(d, Ev::Fault);
            }
            Ev::Heal { id } => {
                self.blocked.remove(&id);
            }
            Ev::ChaosEnd { token } => {
                if token == self.chaos_token {
                    self.drop_p = 0.001;
                    self.dup_p = 0.001;
                    self.extra_delay = 0;
                }
            }
            Ev::SlowEnd { slot } => self.slots[slot].slow = false,
            Ev::FullEnd { slot } => self.slots[slot].full = false,
            Ev::S3Back { token } => {
                if token == self.s3_token {
                    self.s3_down = false;
                }
            }
        }
    }

    // ------------------------------------------------------------------------------------------------ nodes

    fn node_recv(&mut self, slot: usize, node: NodeId, from: usize, msg: Msg) {
        if self.slots[slot].id != node || !self.slots[slot].up {
            return;
        }
        if self.is_blocked(Ent::Writer(from), Ent::Node(slot)) {
            return;
        }
        let (db, mepoch, is_append, is_fence) = match &msg {
            Msg::Append { db, epoch, .. } => (*db, *epoch, true, false),
            Msg::Fence { db, epoch, .. } => (*db, *epoch, false, true),
            m => (m.db(), 0, false, false),
        };
        let range = if let Msg::Append { prev_idx, entries, .. } = &msg { Some((*prev_idx, entries.clone())) } else { None };
        let out = self.slots[slot].mem.handle(WADDR + from as Addr, msg);
        if let Some(b) = &out.broken {
            self.violate("c", format!("node log broken: {b}"));
        }
        // (b1) a node never accepts a request older than a promise it already made known
        let seen = *self.chk.promised_seen.get(&(node, db)).unwrap_or(&0);
        let accepted = out.replies.iter().any(|(_, r)| matches!(r, Msg::Ack { .. } | Msg::Promise { .. }));
        if (is_append || is_fence) && accepted && mepoch < seen {
            self.violate("b1", format!("node {node} accepted epoch {mepoch} for db {db} after promising {seen}"));
        }
        // (c) what a node acknowledges it holds, and it agrees with every committed entry
        if let Some((prev, entries)) = range {
            if let Some((_, Msg::Ack { matched, .. })) = out.replies.iter().find(|(_, r)| matches!(r, Msg::Ack { .. })) {
                let mut bad = None;
                if let Some(d) = self.slots[slot].mem.dbs.get(&db) {
                    for i in (prev + 1)..=*matched {
                        if i <= d.trim {
                            continue;
                        }
                        let held = d.get(i).map(|e| (e.epoch, val(e)));
                        let sent = entries.get((i - prev - 1) as usize).map(|e| (e.epoch, val(e)));
                        if held.is_none() || held != sent {
                            bad = Some(format!("node {node} acknowledged db {db} index {i} holding {held:?}, writer sent {sent:?}"));
                            break;
                        }
                        // a writer at least as new as the one that committed index i carries the committed entry
                        // (a stale writer may legitimately get a lagging node to accept something else there)
                        let newer = self.chk.committer[&db].get(&i).map_or(false, |&ce| mepoch >= ce);
                        if let (true, Some(&(ce, cv))) = (newer, self.chk.committed[&db].get(&i)) {
                            if held != Some((ce, cv)) {
                                bad = Some(format!("node {node} acknowledged db {db} index {i} holding {held:?} but committed is {:?}", (ce, cv)));
                                break;
                            }
                        }
                    }
                }
                if let Some(b) = bad {
                    self.violate("c", b);
                }
            }
        }
        for r in &out.recs {
            self.observe_rec(node, r);
        }
        let s = &mut self.slots[slot];
        s.written += out.recs.len() as u64;
        s.pending.extend(out.recs);
        let need = s.written;
        for (to, r) in out.replies {
            if self.m.ack_before_fsync || self.slots[slot].synced >= need {
                self.node_reply(slot, to, r);
            } else {
                self.slots[slot].waiting.push((need, to, r));
            }
        }
        self.start_fsync(slot);
    }

    fn node_reply(&mut self, slot: usize, to: Addr, r: Msg) {
        let node = self.slots[slot].id;
        match &r {
            Msg::Promise { db, epoch, .. } | Msg::Ack { db, epoch, .. } => {
                let e = self.chk.promised_seen.entry((node, *db)).or_insert(0);
                *e = (*e).max(*epoch);
            }
            Msg::Mismatch { .. } => self.stats.mismatches_sent += 1,
            Msg::Reject { .. } => self.stats.fenced_replies += 1,
            _ => {}
        }
        self.send_to_writer(slot, (to - WADDR) as usize, r);
    }

    fn observe_rec(&mut self, node: NodeId, r: &Rec) {
        match r {
            Rec::Append { db, e } => {
                let v = val(e);
                // (c1) log matching on bytes: same (index, epoch) anywhere -> same value
                match self.chk.entries.get(&(*db, e.idx, e.epoch)) {
                    Some(&x) if x != v => self.violate("c", format!("node {node}: db {db} index {} epoch {} holds {v}, elsewhere {x}", e.idx, e.epoch)),
                    None => {
                        self.chk.entries.insert((*db, e.idx, e.epoch), v);
                    }
                    _ => {}
                }
            }
            Rec::Trim { db, upto } => {
                // (d) trim only below what S3 holds
                let applied = self.stores[db].applied;
                if *upto > applied {
                    self.violate("d", format!("node {node} trims db {db} to {upto} while S3 holds only up to {applied}"));
                }
            }
            _ => {}
        }
    }

    fn start_fsync(&mut self, slot: usize) {
        let s = &mut self.slots[slot];
        if s.syncing || s.pending.is_empty() || !s.up {
            return;
        }
        s.inflight = std::mem::take(&mut s.pending);
        s.syncing = true;
        let gen = s.gen;
        let d = if s.slow { self.rng.range(20 * MS, 200 * MS) } else { self.rng.range(50, 600) };
        self.after(d, Ev::Fsync { slot, gen });
    }

    fn fsync_done(&mut self, slot: usize, gen: u64) {
        if self.slots[slot].gen != gen || !self.slots[slot].up {
            return;
        }
        let fail = self.slots[slot].fail_next || self.slots[slot].full;
        self.slots[slot].fail_next = false;
        self.slots[slot].syncing = false;
        let n = self.slots[slot].inflight.len() as u64;
        if fail {
            if self.m.ignore_fsync_error {
                // the records are gone from the disk, the node's memory and acks say otherwise
                self.slots[slot].inflight.clear();
                self.slots[slot].synced += n;
            } else {
                let d = self.rng.range(20 * MS, 500 * MS);
                self.crash_node(slot, d);
                return;
            }
        } else {
            let recs = std::mem::take(&mut self.slots[slot].inflight);
            for r in &recs {
                let _ = self.slots[slot].dur.apply(r);
            }
            self.slots[slot].synced += n;
        }
        let synced = self.slots[slot].synced;
        let (ready, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut self.slots[slot].waiting).into_iter().partition(|(need, _, _)| *need <= synced);
        self.slots[slot].waiting = rest;
        for (_, to, r) in ready {
            self.node_reply(slot, to, r);
        }
        self.start_fsync(slot);
    }

    /// kill -9 (or fsync failure): unsynced records survive only as a random prefix (page cache write-back).
    fn crash_node(&mut self, slot: usize, down_for: Time) {
        let s = &mut self.slots[slot];
        let mut unsynced = std::mem::take(&mut s.inflight);
        unsynced.extend(std::mem::take(&mut s.pending));
        let keep = self.rng.range(0, unsynced.len() as u64) as usize;
        let s = &mut self.slots[slot];
        for r in unsynced.iter().take(keep) {
            let _ = s.dur.apply(r);
        }
        s.mem = s.dur.clone();
        s.waiting.clear();
        s.syncing = false;
        s.written = 0;
        s.synced = 0;
        s.up = false;
        s.gen += 1;
        let gen = s.gen;
        self.after(down_for, Ev::NodeUp { slot, gen });
    }

    /// The node's disk is lost for good. A replacement with a new identity appears later (design §5.8).
    fn wipe_node(&mut self, slot: usize) {
        let s = &mut self.slots[slot];
        s.up = false;
        s.dead = true;
        s.gen += 1;
        s.pending.clear();
        s.inflight.clear();
        s.waiting.clear();
        s.syncing = false;
        self.dead_ids.insert(s.id);
        let d = self.rng.range(200 * MS, 3 * SEC);
        self.after(d, Ev::Replace { slot });
    }

    fn replace(&mut self, slot: usize) {
        let id = if self.m.wipe_same_id {
            self.dead_ids.remove(&self.slots[slot].id);
            self.slots[slot].id
        } else {
            self.next_node_id += 1;
            self.next_node_id - 1
        };
        let m = self.m;
        let s = &mut self.slots[slot];
        s.id = id;
        s.mem = NodeCore::new(id, m);
        s.dur = NodeCore::new(id, m);
        s.written = 0;
        s.synced = 0;
        s.dead = false;
        s.up = true;
        s.slow = false;
        s.full = false;
        s.gen += 1;
    }

    // ------------------------------------------------------------------------------------------------ writers

    fn writer_recv(&mut self, w: usize, from_node: NodeId, msg: Msg) {
        let db = msg.db();
        let Some(core) = self.writers[w].dbs.get_mut(&db).and_then(|d| d.core.as_mut()) else { return };
        let out = core.on_msg(from_node, msg);
        self.writer_out(w, db, out);
    }

    fn writer_out(&mut self, w: usize, db: DbId, out: WOut) {
        for (n, m) in out.msgs {
            self.send_to_node(w, n, m);
        }
        for ev in out.events {
            match ev {
                WEvent::Active { replay: _, barrier } => {
                    self.stats.opens += 1;
                    let d = self.writers[w].dbs.get_mut(&db).unwrap();
                    d.st = St::Active;
                    d.next_upload = self.now + self.rng.range(self.p.upload_every.0, self.p.upload_every.1);
                    let _ = barrier;
                }
                WEvent::Committed(c) => self.on_commit(w, db, c),
                WEvent::Stale(_) => {
                    self.stats.stale_aborts += 1;
                    self.abandon(w, db);
                }
            }
        }
    }

    fn abandon(&mut self, w: usize, db: DbId) {
        let d = self.writers[w].dbs.get_mut(&db).unwrap();
        *d = WDb::new();
    }

    fn on_commit(&mut self, w: usize, db: DbId, c: Idx) {
        let (epoch, barrier, entries) = {
            let d = self.writers[w].dbs.get_mut(&db).unwrap();
            let Some(core) = d.core.as_ref() else { return };
            let lo = d.commit_seen.max(core.applied);
            let entries: Vec<(Idx, Epoch, u64)> = if lo < c { core.log.range(lo + 1..=c).map(|(i, e)| (*i, e.epoch, val(e))).collect() } else { vec![] };
            let r = (core.epoch, core.barrier, entries);
            d.commit_seen = d.commit_seen.max(c);
            r
        };
        // record committed entries; (state machine safety) a committed index never changes
        for (i, e, v) in entries {
            let cm = self.chk.committed.get_mut(&db).unwrap();
            match cm.get(&i) {
                Some(&(ce, cv)) if (ce, cv) != (e, v) => {
                    let msg = format!("db {db} index {i}: committed ({ce},{cv}) and now ({e},{v}) by epoch {epoch}");
                    self.violate("c", msg);
                }
                _ => {
                    cm.insert(i, (e, v));
                    self.chk.committer.get_mut(&db).unwrap().entry(i).or_insert(epoch);
                }
            }
            let st = &self.stores[&db];
            if i <= st.applied {
                if let Some(&sv) = st.content.get(&i) {
                    if sv != v {
                        self.violate("d", format!("db {db} index {i} committed {v} but S3 holds {sv}"));
                    }
                }
            }
        }
        // (b3) barrier of this epoch committed: every write acknowledged by an older writer is before it
        if c >= barrier && !self.chk.barriers[&db].contains_key(&epoch) {
            self.chk.barriers.get_mut(&db).unwrap().insert(epoch, barrier);
            // (e) a writer whose barrier commits (it is now the writer) holds every acknowledged write. A writer that
            // finished its open from stale promises can be locally "active" but never commits: not checked there.
            self.check_recovery(w, db);
            let bad = self.chk.acked[&db].range(barrier..).find(|(_, (_, e))| *e < epoch).map(|(i, x)| (*i, *x));
            if let Some((i, (id, e))) = bad {
                self.violate("b3", format!("db {db}: write {id} at {i} acknowledged by epoch {e} is after the barrier {barrier} of epoch {epoch}"));
            }
        }
        // acknowledge the client writes up to c
        let acked: Vec<(Idx, u64)> = {
            let d = self.writers[w].dbs.get_mut(&db).unwrap();
            let rest = d.pending.split_off(&(c + 1));
            let done = std::mem::replace(&mut d.pending, rest);
            done.into_iter().collect()
        };
        for (i, id) in acked {
            self.ack(w, db, i, id, epoch);
        }
    }

    fn ack(&mut self, _w: usize, db: DbId, idx: Idx, id: u64, epoch: Epoch) {
        self.stats.acks += 1;
        if let Some(h) = self.stats.healed_at {
            let dt = self.now.saturating_sub(h);
            self.stats.recovered_after_heal.entry(db).or_insert(dt);
        }
        // (b2) one index, one acknowledged write
        if let Some(&(oid, oe)) = self.chk.acked[&db].get(&idx) {
            if oid != id {
                self.violate("b2", format!("db {db} index {idx}: acknowledged write {oid} (epoch {oe}) and write {id} (epoch {epoch})"));
            }
        }
        // (b3) before every committed barrier of a newer epoch
        if let Some((e2, b2)) = self.chk.barriers[&db].range(epoch + 1..).find(|(_, b)| idx >= **b).map(|(e, b)| (*e, *b)) {
            self.violate("b3", format!("db {db}: write {id} at {idx} acknowledged by epoch {epoch} is after the barrier {b2} of epoch {e2}"));
        }
        // a late acknowledgement (older epoch than an active writer): the newer writer must hold it
        for w2 in 0..self.writers.len() {
            let Some(c2) = self.writers[w2].dbs.get(&db).and_then(|d| d.core.as_ref()) else { continue };
            if c2.epoch > epoch && c2.is_active() {
                self.stats.late_acks += 1;
                let ok = if idx <= c2.applied { self.stores[&db].content.get(&idx) == Some(&id) } else { c2.log.get(&idx).map(val) == Some(id) };
                if !ok {
                    self.violate("a/recovery", format!("db {db}: write {id} at {idx} acknowledged by epoch {epoch} is not in the log of active epoch {}", c2.epoch));
                }
            }
        }
        if idx <= self.stores[&db].applied && self.stores[&db].content.get(&idx) != Some(&id) {
            self.violate("a", format!("db {db}: acknowledged write {id} at {idx} differs from S3 ({:?})", self.stores[&db].content.get(&idx)));
        }
        self.chk.acked.get_mut(&db).unwrap().insert(idx, (id, epoch));
    }

    /// (e) the writer whose barrier just committed holds every acknowledged write (in S3 below its marker, or its log).
    fn check_recovery(&mut self, w: usize, db: DbId) {
        let core = self.writers[w].dbs[&db].core.as_ref().unwrap();
        let (applied, barrier, epoch) = (core.applied, core.barrier, core.epoch);
        let mut bad = None;
        // acknowledged writes below the barrier; above it they belong to newer epochs (checked by b3)
        for (&i, &(id, e)) in self.chk.acked[&db].range(applied.saturating_sub(2000)..barrier) {
            let ok = if i <= applied { self.stores[&db].content.get(&i) == Some(&id) } else { i < barrier && core.log.get(&i).map(val) == Some(id) };
            if !ok {
                bad = Some((i, id, e));
                break;
            }
        }
        if let Some((i, id, e)) = bad {
            self.violate("a/recovery", format!("db {db}: writer {w} epoch {epoch} opened without acknowledged write {id} at {i} (epoch {e}); applied {applied}, barrier {barrier}"));
        }
    }

    fn client_write(&mut self, db: DbId) {
        let cands: Vec<usize> = (0..self.writers.len())
            .filter(|&w| {
                let wr = &self.writers[w];
                wr.up && !wr.frozen && wr.dbs[&db].st == St::Active && self.local(w) < wr.dbs[&db].valid_until
            })
            .collect();
        self.stats.write_attempts += 1;
        if cands.is_empty() {
            return;
        }
        let w = self.rng.pick(&cands);
        let id = self.next_write_id;
        self.next_write_id += 1;
        self.stats.client_writes += 1;
        let d = self.writers[w].dbs.get_mut(&db).unwrap();
        let core = d.core.as_mut().unwrap();
        if let Some(i) = core.append(Bytes::copy_from_slice(&id.to_le_bytes())) {
            d.pending.insert(i, id);
            let mut out = WOut::default();
            core.flush(&mut out);
            self.writer_out(w, db, out);
        }
    }

    fn writer_tick(&mut self, w: usize) {
        self.writers[w].ticks += 1;
        let retransmit = self.writers[w].ticks % 4 == 0;
        for db in 0..self.p.dbs {
            let local = self.local(w);
            let st = self.writers[w].dbs[&db].st.clone();
            // lease: expired by this writer's own clock -> stop being the writer (the fence does the rest)
            if !matches!(st, St::Idle | St::Acquiring) && local >= self.writers[w].dbs[&db].valid_until {
                self.abandon(w, db);
                continue;
            }
            if !matches!(st, St::Idle | St::Acquiring | St::Releasing) {
                let d = self.writers[w].dbs.get_mut(&db).unwrap();
                if !d.renewing && local >= d.last_renew_sent + self.p.lease_ttl / 3 {
                    d.renewing = true;
                    self.s3(w, S3Op::Renew { db });
                }
            }
            match st {
                St::Idle => {
                    let pref = self.preferred[&db] == w;
                    let d = &self.writers[w].dbs[&db];
                    let interval = if pref { 100 * MS } else { 300 * MS };
                    if self.now >= d.last_try + interval && (pref || self.rng.chance(0.3)) {
                        self.writers[w].dbs.get_mut(&db).unwrap().last_try = self.now;
                        self.writers[w].dbs.get_mut(&db).unwrap().st = St::Acquiring;
                        self.s3(w, S3Op::Acquire { db });
                    }
                }
                St::Opening => {
                    if retransmit {
                        let out = self.writers[w].dbs.get_mut(&db).unwrap().core.as_mut().unwrap().tick();
                        self.writer_out(w, db, out);
                    }
                }
                St::Active | St::Draining => {
                    if retransmit {
                        let out = self.writers[w].dbs.get_mut(&db).unwrap().core.as_mut().unwrap().tick();
                        self.writer_out(w, db, out);
                    }
                    self.writer_maintain(w, db);
                }
                _ => {}
            }
        }
    }

    /// Uploads, sleep, hot move, membership change.
    fn writer_maintain(&mut self, w: usize, db: DbId) {
        let current: Vec<NodeId> = self.slots.iter().map(|s| s.id).collect();
        let all_present = self.slots.iter().all(|s| !s.dead);
        let d = self.writers[w].dbs.get_mut(&db).unwrap();
        if d.st != St::Active && d.st != St::Draining {
            return;
        }
        if d.st == St::Active && d.goal == Goal::None {
            if all_present && d.members != current {
                d.goal = Goal::Reconfig;
            } else if self.preferred[&db] != w {
                d.goal = Goal::Move;
            } else if self.rng.chance(0.0005) {
                d.goal = Goal::Sleep;
            }
            if d.goal != Goal::None {
                d.st = St::Draining;
                d.drain_upload_sent = false;
            }
        }
        let d = self.writers[w].dbs.get_mut(&db).unwrap();
        if d.uploading {
            return;
        }
        let core = d.core.as_ref().unwrap();
        let draining_ready = d.st == St::Draining && core.quiescent() && !d.drain_upload_sent;
        let periodic = d.st == St::Active && self.now >= d.next_upload && core.last > core.applied;
        if !(draining_ready || periodic) {
            return;
        }
        if d.st == St::Draining {
            d.drain_upload_sent = true;
        }
        let upto = core.last;
        let content: Vec<(Idx, u64)> = if upto > core.applied { core.log.range(core.applied + 1..=upto).map(|(i, e)| (*i, val(e))).collect() } else { vec![] };
        let epoch = core.epoch;
        d.uploading = true;
        let tb = if self.m.trim_before_upload { Some(core.trim_unconfirmed(upto)) } else { None };
        if let Some(out) = tb {
            self.writer_out(w, db, out);
        }
        self.s3(w, S3Op::Upload { db, epoch, upto, content });
    }

    // ------------------------------------------------------------------------------------------------ S3

    fn s3_exec(&mut self, w: usize, gen: u64, op: S3Op, sent_local: Time) {
        let reachable = !self.s3_down && !self.is_blocked(Ent::Writer(w), Ent::S3);
        let (db, name, res) = if !reachable {
            let (db, name) = match &op {
                S3Op::Acquire { db } => (*db, "acquire"),
                S3Op::Renew { db } => (*db, "renew"),
                S3Op::Release { db } => (*db, "release"),
                S3Op::OpenStore { db, .. } => (*db, "open"),
                S3Op::Upload { db, .. } => (*db, "upload"),
                S3Op::Reconfig { db, .. } => (*db, "reconfig"),
            };
            (db, name, S3Res::Unreachable)
        } else {
            let holder = Some((w, self.writers[w].gen));
            let local_now = self.local(w);
            match op {
                S3Op::Acquire { db } => {
                    let l = self.leases.get_mut(&db).unwrap();
                    let free = l.holder.is_none() || l.holder == holder || l.expires + self.p.skew_allow < local_now;
                    if free {
                        if l.holder.is_some() && l.holder != holder {
                            self.stats.takeovers += 1;
                        }
                        l.seq += 1;
                        l.holder = holder;
                        l.expires = local_now + self.p.lease_ttl;
                    }
                    (db, "acquire", S3Res::Lease { ok: free, seq: l.seq, members: l.members.clone(), sent_local })
                }
                S3Op::Renew { db } => {
                    let l = self.leases.get_mut(&db).unwrap();
                    let ok = l.holder == holder;
                    if ok {
                        l.seq += 1;
                        l.expires = local_now + self.p.lease_ttl;
                    }
                    (db, "renew", S3Res::Lease { ok, seq: l.seq, members: l.members.clone(), sent_local })
                }
                S3Op::Release { db } => {
                    let l = self.leases.get_mut(&db).unwrap();
                    let ok = l.holder == holder;
                    if ok {
                        l.holder = None;
                        l.seq += 1;
                    }
                    (db, "release", S3Res::Lease { ok, seq: l.seq, members: l.members.clone(), sent_local })
                }
                S3Op::Reconfig { db, members } => {
                    let l = self.leases.get_mut(&db).unwrap();
                    let ok = l.holder == holder;
                    if ok {
                        l.seq += 1;
                        l.members = members;
                        l.expires = local_now + self.p.lease_ttl;
                    }
                    (db, "reconfig", S3Res::Lease { ok, seq: l.seq, members: l.members.clone(), sent_local })
                }
                S3Op::OpenStore { db, epoch } => {
                    let s = self.stores.get_mut(&db).unwrap();
                    // the object-store writer fence: older writers' uploads fail from now on
                    let ok = epoch >= s.slate_epoch;
                    if ok && !self.m.skip_store_fence {
                        s.slate_epoch = epoch;
                    }
                    (db, "open", S3Res::Store { ok, applied: s.applied, epoch })
                }
                S3Op::Upload { db, epoch, upto, content } => {
                    let s = self.stores.get_mut(&db).unwrap();
                    let ok = if self.m.skip_store_fence { true } else { epoch == s.slate_epoch };
                    if ok {
                        for (i, v) in &content {
                            s.content.insert(*i, *v);
                        }
                        if self.m.skip_store_fence {
                            s.applied = upto; // last writer wins: no fence, the marker can go back
                        } else {
                            s.applied = s.applied.max(upto);
                        }
                        self.stats.uploads += 1;
                        // (d) the uploaded state agrees with every committed and acknowledged entry
                        for (i, v) in content {
                            if let Some(&(_, cv)) = self.chk.committed[&db].get(&i) {
                                if cv != v {
                                    self.violate("d", format!("db {db}: upload by epoch {epoch} writes {v} at {i}, committed {cv}"));
                                }
                            }
                        }
                    }
                    (db, "upload", S3Res::Upload { ok, upto, epoch })
                }
            }
        };
        let d = self.rng.range(3 * MS, 30 * MS);
        self.after(d, Ev::S3Reply { w, gen, db, op: name, res });
    }

    fn s3_reply(&mut self, w: usize, db: DbId, op: &'static str, res: S3Res) {
        match (op, res) {
            ("acquire", S3Res::Lease { ok, seq, members, sent_local }) => {
                let d = self.writers[w].dbs.get_mut(&db).unwrap();
                if d.st != St::Acquiring {
                    return;
                }
                if !ok {
                    d.st = St::Idle;
                    return;
                }
                d.epoch = seq;
                d.members = members;
                d.valid_until = sent_local + self.p.lease_ttl;
                d.last_renew_sent = sent_local;
                d.st = St::OpeningStore;
                self.s3(w, S3Op::OpenStore { db, epoch: seq });
            }
            ("acquire", S3Res::Unreachable) => {
                let d = self.writers[w].dbs.get_mut(&db).unwrap();
                if d.st == St::Acquiring {
                    d.st = St::Idle;
                }
            }
            ("renew", res) => {
                let d = self.writers[w].dbs.get_mut(&db).unwrap();
                d.renewing = false;
                match res {
                    S3Res::Lease { ok: true, sent_local, .. } => {
                        if d.st != St::Idle {
                            d.valid_until = d.valid_until.max(sent_local + self.p.lease_ttl);
                            d.last_renew_sent = sent_local;
                        }
                    }
                    S3Res::Lease { ok: false, .. } => {
                        if d.st != St::Idle && d.st != St::Acquiring {
                            self.abandon(w, db);
                        }
                    }
                    _ => {}
                }
            }
            ("open", S3Res::Store { ok, applied, epoch }) => {
                let d = self.writers[w].dbs.get_mut(&db).unwrap();
                if d.st != St::OpeningStore || d.epoch != epoch {
                    return;
                }
                if !ok {
                    self.abandon(w, db);
                    return;
                }
                let (core, out) = WriterDb::open(db, d.epoch, d.members.clone(), applied, self.m);
                d.core = Some(core);
                d.st = St::Opening;
                self.writer_out(w, db, out);
            }
            ("open", S3Res::Unreachable) => {
                let epoch = self.writers[w].dbs[&db].epoch;
                if self.writers[w].dbs[&db].st == St::OpeningStore {
                    self.s3(w, S3Op::OpenStore { db, epoch });
                }
            }
            ("upload", S3Res::Upload { ok, upto, epoch }) => {
                let d = self.writers[w].dbs.get_mut(&db).unwrap();
                if d.core.as_ref().map(|c| c.epoch) != Some(epoch) {
                    return; // reply to an upload of an earlier epoch of this writer
                }
                d.uploading = false;
                d.next_upload = self.now + self.rng.range(self.p.upload_every.0, self.p.upload_every.1);
                if !ok {
                    self.abandon(w, db);
                    return;
                }
                let out = d.core.as_mut().unwrap().uploaded(upto);
                let draining = d.st == St::Draining && d.drain_upload_sent;
                let goal = d.goal;
                self.writer_out(w, db, out);
                if draining {
                    let d = self.writers[w].dbs.get_mut(&db).unwrap();
                    let core = d.core.as_ref().unwrap();
                    if core.applied < core.last {
                        d.drain_upload_sent = false; // something was appended meanwhile: upload again
                        return;
                    }
                    match goal {
                        Goal::Reconfig => {
                            d.st = St::Reconfiguring;
                            let members: Vec<NodeId> = self.slots.iter().map(|s| s.id).collect();
                            self.s3(w, S3Op::Reconfig { db, members });
                        }
                        Goal::Sleep | Goal::Move => {
                            if goal == Goal::Sleep {
                                self.stats.sleeps += 1;
                            } else {
                                self.stats.moves += 1;
                            }
                            d.st = St::Releasing;
                            self.s3(w, S3Op::Release { db });
                        }
                        Goal::None => {}
                    }
                }
            }
            ("upload", S3Res::Unreachable) => {
                let d = self.writers[w].dbs.get_mut(&db).unwrap();
                d.uploading = false;
                d.drain_upload_sent = false;
                d.next_upload = self.now + 50 * MS;
            }
            ("reconfig", S3Res::Lease { ok, seq, members, sent_local }) => {
                let d = self.writers[w].dbs.get_mut(&db).unwrap();
                if d.st != St::Reconfiguring {
                    return;
                }
                if !ok {
                    self.abandon(w, db);
                    return;
                }
                self.stats.reconfigs += 1;
                let applied = d.core.as_ref().unwrap().applied;
                let _ = applied;
                d.epoch = seq;
                d.members = members;
                d.valid_until = d.valid_until.max(sent_local + self.p.lease_ttl);
                d.goal = Goal::None;
                d.core = None;
                d.st = St::OpeningStore;
                self.s3(w, S3Op::OpenStore { db, epoch: seq });
            }
            ("reconfig", S3Res::Unreachable) => {
                let members: Vec<NodeId> = self.slots.iter().map(|s| s.id).collect();
                if self.writers[w].dbs[&db].st == St::Reconfiguring {
                    self.s3(w, S3Op::Reconfig { db, members });
                }
            }
            ("release", _) => {
                let d = self.writers[w].dbs.get_mut(&db).unwrap();
                if d.st == St::Releasing {
                    *d = WDb::new();
                    d.last_try = self.now;
                }
            }
            _ => {}
        }
    }

    // ------------------------------------------------------------------------------------------------ faults

    /// End of the fault phase: every fault is repaired at once (liveness check).
    fn heal_all(&mut self) {
        self.stats.healed_at = Some(self.now);
        self.blocked.clear();
        self.drop_p = 0.001;
        self.dup_p = 0.001;
        self.extra_delay = 0;
        self.s3_down = false;
        for slot in 0..self.slots.len() {
            let s = &mut self.slots[slot];
            s.slow = false;
            s.full = false;
            s.fail_next = false;
            if s.dead {
                self.replace(slot);
            } else if !s.up {
                s.up = true;
                s.gen += 1;
            }
        }
        for w in 0..self.writers.len() {
            self.writers[w].offset = 0;
            if self.writers[w].frozen {
                self.writers[w].freeze_token += 1;
                self.writers[w].frozen = false;
                let evs = std::mem::take(&mut self.writers[w].deferred);
                for e in evs {
                    self.at(self.now, e);
                }
            }
            if !self.writers[w].up {
                self.at(self.now, Ev::WUp { w });
            }
        }
    }

    fn inject_fault(&mut self) {
        let k = self.rng.range(0, 99);
        match k {
            0..=14 => {
                let s = self.rng.range(0, 2) as usize;
                if self.slots[s].up {
                    self.fault("node_kill9");
                    let long = self.rng.chance(0.2);
                    let d = if long { self.rng.range(3 * SEC, 15 * SEC) } else { self.rng.range(10 * MS, 2 * SEC) };
                    self.crash_node(s, d);
                }
            }
            15..=17 => {
                // a disk lost for good: at most one at a time (single failure) unless exploring double failures
                let s = self.rng.range(0, 2) as usize;
                let busy = self.slots.iter().any(|x| x.dead) || self.leases.values().any(|l| l.members.iter().any(|m| !self.slots.iter().any(|x| x.id == *m)));
                if !self.slots[s].dead && (!busy || self.p.double) {
                    self.fault("node_disk_lost");
                    self.wipe_node(s);
                }
            }
            18..=27 => {
                let w = self.rng.range(0, 2) as usize;
                if self.writers[w].up {
                    self.fault("writer_kill9");
                    self.writers[w].up = false;
                    self.writers[w].frozen = false;
                    self.writers[w].deferred.clear();
                    self.writers[w].gen += 1;
                    let d = self.rng.range(50 * MS, 3 * SEC);
                    self.after(d, Ev::WUp { w });
                }
            }
            28..=37 => {
                // half of the freezes hit a writer while it has writes in flight (the late-acknowledgement case)
                let busy: Vec<usize> = (0..3).filter(|&x| self.writers[x].up && !self.writers[x].frozen && self.writers[x].dbs.values().any(|d| !d.pending.is_empty())).collect();
                let w = if !busy.is_empty() && self.rng.chance(0.5) { self.rng.pick(&busy) } else { self.rng.range(0, 2) as usize };
                if self.writers[w].up && !self.writers[w].frozen {
                    self.fault("writer_freeze");
                    self.writers[w].frozen = true;
                    self.writers[w].freeze_token += 1;
                    let token = self.writers[w].freeze_token;
                    let d = if self.rng.chance(0.5) { self.rng.range(2 * SEC, 6 * SEC) } else { self.rng.range(10 * MS, 1 * SEC) };
                    self.after(d, Ev::Unfreeze { w, token });
                }
            }
            38..=49 => {
                self.fault("partition");
                let mut ents: Vec<Ent> = (0..3).map(Ent::Node).chain((0..3).map(Ent::Writer)).collect();
                ents.push(Ent::S3);
                let kind = self.rng.range(0, 3);
                let mut cut = vec![];
                match kind {
                    0 => {
                        // isolate one entity both ways
                        let x = self.rng.pick(&ents);
                        for &y in &ents {
                            if y != x {
                                cut.push((x, y));
                                cut.push((y, x));
                            }
                        }
                    }
                    1 => {
                        // random two-sided split
                        let side: Vec<bool> = ents.iter().map(|_| self.rng.chance(0.5)).collect();
                        for (i, &a) in ents.iter().enumerate() {
                            for (j, &b) in ents.iter().enumerate() {
                                if side[i] != side[j] {
                                    cut.push((a, b));
                                }
                            }
                        }
                    }
                    2 => {
                        // asymmetric: a few one-way links
                        for _ in 0..self.rng.range(1, 4) {
                            let a = self.rng.pick(&ents);
                            let b = self.rng.pick(&ents);
                            if a != b {
                                cut.push((a, b));
                            }
                        }
                        self.fault("partition_asymmetric");
                    }
                    _ => {
                        // zone outage: node + writer of one AZ cut from everything (and S3)
                        let z = self.rng.range(0, 2) as usize;
                        self.fault("zone_outage");
                        for &y in &ents {
                            for x in [Ent::Node(z), Ent::Writer(z)] {
                                if y != Ent::Node(z) && y != Ent::Writer(z) {
                                    cut.push((x, y));
                                    cut.push((y, x));
                                }
                            }
                        }
                        if self.rng.chance(0.5) && self.slots[z].up {
                            let d = self.rng.range(1 * SEC, 8 * SEC);
                            self.crash_node(z, d);
                        }
                        if self.rng.chance(0.5) && self.writers[z].up {
                            self.writers[z].up = false;
                            self.writers[z].gen += 1;
                            let d = self.rng.range(1 * SEC, 8 * SEC);
                            self.after(d, Ev::WUp { w: z });
                        }
                    }
                }
                let id = self.next_block;
                self.next_block += 1;
                self.blocked.insert(id, cut);
                let d = self.rng.range(50 * MS, 5 * SEC);
                self.after(d, Ev::Heal { id });
            }
            50..=57 => {
                self.fault("net_chaos");
                self.chaos_token += 1;
                self.drop_p = self.rng.range(0, 20) as f64 / 100.0;
                self.dup_p = self.rng.range(0, 10) as f64 / 100.0;
                self.extra_delay = self.rng.range(0, 50 * MS);
                let token = self.chaos_token;
                let d = self.rng.range(100 * MS, 3 * SEC);
                self.after(d, Ev::ChaosEnd { token });
            }
            58..=63 => {
                let s = self.rng.range(0, 2) as usize;
                self.fault("slow_disk");
                self.slots[s].slow = true;
                let d = self.rng.range(100 * MS, 3 * SEC);
                self.after(d, Ev::SlowEnd { slot: s });
            }
            64..=69 => {
                let s = self.rng.range(0, 2) as usize;
                self.fault("fsync_error");
                self.slots[s].fail_next = true;
            }
            70..=73 => {
                let s = self.rng.range(0, 2) as usize;
                self.fault("disk_full");
                self.slots[s].full = true;
                let d = self.rng.range(100 * MS, 3 * SEC);
                self.after(d, Ev::FullEnd { slot: s });
            }
            74..=81 => {
                let w = self.rng.range(0, 2) as usize;
                self.fault("clock_skew");
                let m = self.p.max_clock_offset as i64;
                self.writers[w].offset = self.rng.range(0, 2 * m as u64) as i64 - m;
            }
            82..=89 => {
                let db = self.rng.range(0, self.p.dbs - 1);
                self.fault("hot_move");
                let nw = self.rng.range(0, 2) as usize;
                self.preferred.insert(db, nw);
            }
            90..=93 => {
                self.fault("s3_outage");
                self.s3_down = true;
                self.s3_token += 1;
                let token = self.s3_token;
                let d = self.rng.range(100 * MS, 3 * SEC);
                self.after(d, Ev::S3Back { token });
            }
            _ => {
                // a stale node returning: a long kill
                let s = self.rng.range(0, 2) as usize;
                if self.slots[s].up {
                    self.fault("stale_node_return");
                    let d = self.rng.range(5 * SEC, 20 * SEC);
                    self.crash_node(s, d);
                }
            }
        }
    }

    // ------------------------------------------------------------------------------------------------ (a)

    /// (a) Every acknowledged write above the S3 marker is in the log that recovery would choose from every majority
    /// of the database's current members whose disks exist (durable state only: as if every node crashed now).
    fn check_durability(&mut self) {
        for db in 0..self.p.dbs {
            let applied = self.stores[&db].applied;
            let acked = &self.chk.acked[&db];
            if acked.range(applied + 1..).next().is_none() {
                continue;
            }
            let members = self.leases[&db].members.clone();
            let present: Vec<usize> = members.iter().filter_map(|m| self.slots.iter().position(|s| s.id == *m && !s.dead)).collect();
            if members.len() - present.len() >= 2 {
                // two of the three disks are gone: no guarantee; count whether something acknowledged is lost
                self.stats.unprotected_steps += 1;
                let survivor = present.first().and_then(|&s| self.slots[s].dur.dbs.get(&db));
                let lost = self.chk.acked[&db].range(applied + 1..).any(|(&i, &(id, _))| survivor.and_then(|l| l.get(i)).map(val) != Some(id));
                if lost {
                    self.stats.lost_in_unprotected += 1;
                }
                continue;
            }
            let mut pairs = vec![];
            for i in 0..present.len() {
                for j in i + 1..present.len() {
                    pairs.push((present[i], present[j]));
                }
            }
            for (a, b) in pairs {
                let logs: Vec<(NodeId, Idx, Epoch)> = [a, b]
                    .iter()
                    .map(|&s| {
                        let d = self.slots[s].dur.dbs.get(&db);
                        (self.slots[s].id, d.map_or(0, |d| d.last()), d.map_or(0, |d| d.last_epoch()))
                    })
                    .collect();
                let src = choose_source(logs, applied, &self.m);
                let src_log = src.and_then(|(n, _)| self.slots.iter().find(|s| s.id == n)).and_then(|s| s.dur.dbs.get(&db));
                for (&i, &(id, e)) in self.chk.acked[&db].range(applied + 1..) {
                    let got = src_log.and_then(|l| l.get(i)).map(val);
                    if got != Some(id) {
                        let msg = format!(
                            "db {db}: acknowledged write {id} at {i} (epoch {e}) not recoverable from nodes {}+{}: chosen source {:?} gives {got:?}; S3 marker {applied}",
                            self.slots[a].id, self.slots[b].id, src
                        );
                        self.violate("a", msg);
                        return;
                    }
                }
            }
        }
    }
}
