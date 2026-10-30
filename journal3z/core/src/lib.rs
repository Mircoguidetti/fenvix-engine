//! The journal3z protocol (`docs/journal3z-design.md` §5) as sans-IO state machines.
//!
//! - [`NodeCore`]: a journal node (acceptor + store) for many databases. `handle` turns one message into the records
//!   to persist and the replies to send. **The driver must not send a reply before every record produced so far by
//!   this node is durable (fdatasync returned)** — see [`NodeOut`]. State is rebuilt after a crash by folding the
//!   durable records with [`NodeCore::apply`], the same function used live, so live and recovered states cannot
//!   disagree.
//! - [`WriterDb`]: the writer side for one database and one epoch: open (fence a majority, choose the most
//!   up-to-date log, read it, append a barrier), append with pipelining, commit on 2 of 3 counting only entries of its
//!   own epoch, catch-up of lagging nodes, trim after a confirmed upload.
//! - [`Mutations`]: deliberate protocol bugs for the simulator's negative controls. All `false` = the protocol.
//!
//! Types are small integers so the simulator can run millions of steps; payloads are `Bytes`.

use bytes::Bytes;
use std::collections::{BTreeMap, VecDeque};

pub type NodeId = u32;
pub type Addr = u32;
pub type DbId = u64;
pub type Epoch = u64;
pub type Idx = u64;

/// Highest epoch: an erased database answers every request with it.
pub const ERASED: Epoch = u64::MAX;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Entry {
    pub idx: Idx,
    /// Epoch of the writer that created the entry (kept when a later writer copies it).
    pub epoch: Epoch,
    pub barrier: bool,
    pub data: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Msg {
    Fence { db: DbId, epoch: Epoch, applied: Idx },
    Promise { db: DbId, epoch: Epoch, last: Idx, last_epoch: Epoch },
    Reject { db: DbId, epoch: Epoch, promised: Epoch },
    Read { db: DbId, epoch: Epoch, from: Idx, to: Idx },
    Entries { db: DbId, epoch: Epoch, ok: bool, entries: Vec<Entry> },
    Append { db: DbId, epoch: Epoch, applied: Idx, prev_idx: Idx, prev_epoch: Epoch, entries: Vec<Entry>, commit: Idx },
    Ack { db: DbId, epoch: Epoch, matched: Idx },
    Mismatch { db: DbId, epoch: Epoch, hint: Idx },
    Trim { db: DbId, epoch: Epoch, upto: Idx },
    Erase { db: DbId },
}

impl Msg {
    pub fn db(&self) -> DbId {
        match self {
            Msg::Fence { db, .. }
            | Msg::Promise { db, .. }
            | Msg::Reject { db, .. }
            | Msg::Read { db, .. }
            | Msg::Entries { db, .. }
            | Msg::Append { db, .. }
            | Msg::Ack { db, .. }
            | Msg::Mismatch { db, .. }
            | Msg::Trim { db, .. }
            | Msg::Erase { db } => *db,
        }
    }
    /// Approximate size on the wire (for byte counters).
    pub fn size(&self) -> usize {
        match self {
            Msg::Append { entries, .. } | Msg::Entries { entries, .. } => 48 + entries.iter().map(|e| 24 + e.data.len()).sum::<usize>(),
            _ => 40,
        }
    }
}

/// A record of a node's disk. The node's state is the fold of its records with [`NodeCore::apply`].
#[derive(Clone, Debug)]
pub enum Rec {
    Promise { db: DbId, epoch: Epoch },
    Truncate { db: DbId, from: Idx },
    Append { db: DbId, e: Entry },
    Trim { db: DbId, upto: Idx },
    Erase { db: DbId },
}

impl Rec {
    pub fn db(&self) -> DbId {
        match self {
            Rec::Promise { db, .. } | Rec::Truncate { db, .. } | Rec::Append { db, .. } | Rec::Trim { db, .. } | Rec::Erase { db } => *db,
        }
    }
}

/// Deliberate protocol bugs (negative controls). The simulator must catch each one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Mutations {
    /// Node: accept appends from any epoch (no fence check).
    pub no_fencing: bool,
    /// Node: on a gap (prev beyond its log) acknowledge as if it had the entries (wrong catch-up).
    pub ack_gap: bool,
    /// Node: on an entry with the same index but another epoch, keep the old one and acknowledge (wrong catch-up).
    pub keep_diverged: bool,
    /// Node: forget a database's fence record when its log becomes empty (sleep / full trim).
    pub forget_fence: bool,
    /// Writer: acknowledge when 1 node (any) has the entry.
    pub ack_on_one: bool,
    /// Writer: recovery chooses the longest log, ignoring epochs.
    pub recovery_longest: bool,
    /// Writer: recovery uses the first promise only (no majority).
    pub recovery_single: bool,
    /// Driver: trim the journal before the upload is confirmed.
    pub trim_before_upload: bool,
    /// Driver: a node replies before its fdatasync.
    pub ack_before_fsync: bool,
    /// Driver: a node keeps running after a failed fdatasync (the unsynced data is gone, its memory says otherwise).
    pub ignore_fsync_error: bool,
    /// Driver: a new writer reads the S3 marker without fencing the older object-store writer.
    pub skip_store_fence: bool,
    /// Driver: a node whose disk was wiped comes back with the same identity (no reconfiguration).
    pub wipe_same_id: bool,
}

impl Mutations {
    pub fn parse(name: &str) -> Option<Self> {
        let mut m = Mutations::default();
        match name {
            "none" => {}
            "no_fencing" => m.no_fencing = true,
            "ack_gap" => m.ack_gap = true,
            "keep_diverged" => m.keep_diverged = true,
            "forget_fence" => m.forget_fence = true,
            "ack_on_one" => m.ack_on_one = true,
            "recovery_longest" => m.recovery_longest = true,
            "recovery_single" => m.recovery_single = true,
            "trim_before_upload" => m.trim_before_upload = true,
            "ack_before_fsync" => m.ack_before_fsync = true,
            "ignore_fsync_error" => m.ignore_fsync_error = true,
            "skip_store_fence" => m.skip_store_fence = true,
            "wipe_same_id" => m.wipe_same_id = true,
            _ => return None,
        }
        Some(m)
    }
    pub const ALL: [&'static str; 12] = [
        "no_fencing",
        "ack_gap",
        "keep_diverged",
        "forget_fence",
        "ack_on_one",
        "recovery_longest",
        "recovery_single",
        "trim_before_upload",
        "ack_before_fsync",
        "ignore_fsync_error",
        "skip_store_fence",
        "wipe_same_id",
    ];
}

// ============================================================================================================ node

/// One database's log on a node: entries contiguous from `trim + 1`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct DbLog {
    pub promised: Epoch,
    pub trim: Idx,
    pub entries: VecDeque<Entry>,
    pub erased: bool,
}

impl DbLog {
    pub fn last(&self) -> Idx {
        self.trim + self.entries.len() as Idx
    }
    pub fn last_epoch(&self) -> Epoch {
        self.entries.back().map_or(0, |e| e.epoch)
    }
    pub fn get(&self, idx: Idx) -> Option<&Entry> {
        if idx <= self.trim || idx > self.last() {
            return None;
        }
        self.entries.get((idx - self.trim - 1) as usize)
    }
}

/// What a node must do after handling a message: persist `recs` (in order), then send `replies` — but only once
/// every record this node produced so far (these and earlier ones) is durable.
#[derive(Debug, Default)]
pub struct NodeOut {
    pub recs: Vec<Rec>,
    pub replies: Vec<(Addr, Msg)>,
    /// Set when `apply` found a record that breaks the log's contiguity (a protocol bug: the simulator flags it).
    pub broken: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct NodeCore {
    pub id: NodeId,
    pub dbs: BTreeMap<DbId, DbLog>,
    pub m: Mutations,
}

impl NodeCore {
    pub fn new(id: NodeId, m: Mutations) -> Self {
        NodeCore { id, dbs: BTreeMap::new(), m }
    }

    /// Apply one record (live, or when rebuilding from disk). Returns an error string if the record does not fit.
    pub fn apply(&mut self, r: &Rec) -> Result<(), String> {
        let forget = self.m.forget_fence;
        let d = self.dbs.entry(r.db()).or_default();
        match r {
            Rec::Promise { epoch, .. } => d.promised = d.promised.max(*epoch),
            Rec::Truncate { from, .. } => {
                while d.last() >= *from && !d.entries.is_empty() {
                    d.entries.pop_back();
                }
            }
            Rec::Append { e, .. } => {
                if e.idx <= d.trim {
                    return Ok(());
                }
                if e.idx != d.last() + 1 {
                    return Err(format!("node {}: append of index {} after {} (db {})", self.id, e.idx, d.last(), r.db()));
                }
                d.entries.push_back(e.clone());
            }
            Rec::Trim { upto, .. } => {
                if *upto > d.trim {
                    let n = ((*upto - d.trim) as usize).min(d.entries.len());
                    d.entries.drain(..n);
                    d.trim = *upto;
                    if forget && d.entries.is_empty() && !d.erased {
                        d.promised = 0; // the bug: the fence is dropped with the (now empty) log
                    }
                }
            }
            Rec::Erase { .. } => {
                d.erased = true;
                d.promised = ERASED;
                d.entries.clear();
            }
        }
        Ok(())
    }

    fn rec(&mut self, out: &mut NodeOut, r: Rec) {
        if let Err(e) = self.apply(&r) {
            out.broken.get_or_insert(e);
        }
        out.recs.push(r);
    }

    pub fn handle(&mut self, from: Addr, msg: Msg) -> NodeOut {
        let mut out = NodeOut::default();
        let db = msg.db();
        match msg {
            Msg::Fence { epoch, applied, .. } => {
                let d = self.dbs.entry(db).or_default();
                if d.erased || epoch < d.promised {
                    let p = d.promised;
                    out.replies.push((from, Msg::Reject { db, epoch, promised: p }));
                    return out;
                }
                let (trim, promised) = (d.trim, d.promised);
                if applied > trim {
                    self.rec(&mut out, Rec::Trim { db, upto: applied });
                }
                if epoch > promised {
                    self.rec(&mut out, Rec::Promise { db, epoch });
                }
                let d = self.dbs.entry(db).or_default();
                out.replies.push((from, Msg::Promise { db, epoch, last: d.last(), last_epoch: d.last_epoch() }));
            }
            Msg::Read { epoch, from: lo, to, .. } => {
                let d = self.dbs.entry(db).or_default();
                let ok = !d.erased && epoch >= d.promised && lo > d.trim && to <= d.last();
                let entries = if ok { (lo..=to).filter_map(|i| d.get(i).cloned()).collect() } else { vec![] };
                out.replies.push((from, Msg::Entries { db, epoch, ok, entries }));
            }
            Msg::Append { epoch, applied, prev_idx, prev_epoch, entries, .. } => {
                let d = self.dbs.entry(db).or_default();
                if d.erased || (epoch < d.promised && !self.m.no_fencing) {
                    let p = d.promised;
                    out.replies.push((from, Msg::Reject { db, epoch, promised: p }));
                    return out;
                }
                // entries must continue `prev` contiguously (the writer always sends them so)
                if entries.iter().enumerate().any(|(i, e)| e.idx != prev_idx + 1 + i as Idx) {
                    return out;
                }
                let (promised, trim) = (d.promised, d.trim);
                if epoch > promised {
                    self.rec(&mut out, Rec::Promise { db, epoch });
                }
                if applied > trim {
                    self.rec(&mut out, Rec::Trim { db, upto: applied });
                }
                let d = self.dbs.entry(db).or_default();
                // consistency check (Raft): the entry at prev must have prev_epoch; at or below the trim point
                // everything matches (it is in S3, one agreed history)
                let matches = prev_idx <= d.trim || d.get(prev_idx).map_or(false, |e| e.epoch == prev_epoch);
                if !matches {
                    if prev_idx > d.last() {
                        if self.m.ack_gap {
                            out.replies.push((from, Msg::Ack { db, epoch, matched: prev_idx + entries.len() as Idx }));
                            return out;
                        }
                        let hint = d.last();
                        out.replies.push((from, Msg::Mismatch { db, epoch, hint }));
                    } else {
                        // back up to just before the run of the conflicting epoch (Raft's fast back-up)
                        let ce = d.get(prev_idx).map_or(0, |e| e.epoch);
                        let mut j = prev_idx;
                        while j > d.trim + 1 && d.get(j - 1).map_or(false, |e| e.epoch == ce) {
                            j -= 1;
                        }
                        out.replies.push((from, Msg::Mismatch { db, epoch, hint: j - 1 }));
                    }
                    return out;
                }
                let n = entries.len() as Idx;
                for e in entries {
                    let d = self.dbs.entry(db).or_default();
                    if e.idx <= d.trim {
                        continue;
                    }
                    if e.idx <= d.last() {
                        let same = d.get(e.idx).map_or(false, |x| x.epoch == e.epoch);
                        if same || self.m.keep_diverged {
                            continue;
                        }
                        let from_idx = e.idx;
                        self.rec(&mut out, Rec::Truncate { db, from: from_idx });
                    }
                    self.rec(&mut out, Rec::Append { db, e });
                }
                out.replies.push((from, Msg::Ack { db, epoch, matched: prev_idx + n }));
            }
            Msg::Trim { upto, .. } => {
                let d = self.dbs.entry(db).or_default();
                if upto > d.trim && !d.erased {
                    self.rec(&mut out, Rec::Trim { db, upto });
                }
            }
            Msg::Erase { .. } => {
                if !self.dbs.get(&db).map_or(false, |d| d.erased) {
                    self.rec(&mut out, Rec::Erase { db });
                }
            }
            Msg::Promise { .. } | Msg::Reject { .. } | Msg::Entries { .. } | Msg::Ack { .. } | Msg::Mismatch { .. } => {}
        }
        out
    }
}

/// Recovery rule R3 (design §5.2 step 4): among the given node logs, the source is the one with the largest
/// `(last_epoch, last)` among those with entries above `applied`; `None` if no log has any. The checker uses the same
/// function to compute "what a future writer would recover".
pub fn choose_source<I: IntoIterator<Item = (NodeId, Idx, Epoch)>>(logs: I, applied: Idx, m: &Mutations) -> Option<(NodeId, Idx)> {
    let mut best: Option<(NodeId, Idx, Epoch)> = None;
    for (n, last, le) in logs {
        if last <= applied {
            continue;
        }
        let better = match best {
            None => true,
            Some((_, bl, be)) => {
                if m.recovery_longest {
                    last > bl
                } else {
                    (le, last) > (be, bl)
                }
            }
        };
        if better {
            best = Some((n, last, le));
        }
    }
    best.map(|(n, l, _)| (n, l))
}

// ========================================================================================================== writer

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Phase {
    Fencing,
    Reading { source: NodeId, to: Idx },
    Active,
    Stale(Epoch),
}

#[derive(Clone, Debug)]
pub enum WEvent {
    /// Open finished: apply `replay` (entries above `applied`, barrier excluded) and accept writes.
    Active { replay: Vec<Entry>, barrier: Idx },
    /// Every index ≤ this one is committed (acknowledge the writes up to it).
    Committed(Idx),
    /// A node promised a higher epoch: another writer owns the database. Abandon it.
    Stale(Epoch),
}

#[derive(Debug, Default)]
pub struct WOut {
    pub msgs: Vec<(NodeId, Msg)>,
    pub events: Vec<WEvent>,
}

/// Writer state for one database at one epoch.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WriterDb {
    pub db: DbId,
    pub epoch: Epoch,
    pub members: Vec<NodeId>,
    pub applied: Idx,
    pub phase: Phase,
    promises: BTreeMap<NodeId, (Idx, Epoch)>,
    /// Entries above `applied` known to this writer (recovered + own).
    pub log: BTreeMap<Idx, Entry>,
    pub last: Idx,
    pub barrier: Idx,
    pub commit: Idx,
    next: BTreeMap<NodeId, Idx>,
    matched: BTreeMap<NodeId, Idx>,
    ticks: u32,
    pub max_batch: usize,
    pub m: Mutations,
}

impl WriterDb {
    /// Start opening `db` at `epoch` on `members`; `applied` = the database's S3 marker (read after the object-store
    /// writer fence). Sends `FENCE` to every member.
    pub fn open(db: DbId, epoch: Epoch, members: Vec<NodeId>, applied: Idx, m: Mutations) -> (Self, WOut) {
        let mut w = WriterDb {
            db,
            epoch,
            members,
            applied,
            phase: Phase::Fencing,
            promises: BTreeMap::new(),
            log: BTreeMap::new(),
            last: applied,
            barrier: 0,
            commit: applied,
            next: BTreeMap::new(),
            matched: BTreeMap::new(),
            ticks: 0,
            max_batch: 64,
            m,
        };
        let mut out = WOut::default();
        w.send_fences(&mut out);
        (w, out)
    }

    fn send_fences(&mut self, out: &mut WOut) {
        for &n in &self.members {
            if !self.promises.contains_key(&n) {
                out.msgs.push((n, Msg::Fence { db: self.db, epoch: self.epoch, applied: self.applied }));
            }
        }
    }

    pub fn is_active(&self) -> bool {
        self.phase == Phase::Active
    }

    fn prev_epoch(&self, idx: Idx) -> Epoch {
        if idx <= self.applied {
            0
        } else {
            self.log.get(&idx).map_or(0, |e| e.epoch)
        }
    }

    fn send_from(&mut self, n: NodeId, from: Idx, out: &mut WOut) {
        let from = from.max(self.applied + 1);
        let to = self.last.min(from + self.max_batch as Idx - 1);
        let entries: Vec<Entry> = if from <= to { self.log.range(from..=to).map(|(_, e)| e.clone()).collect() } else { vec![] };
        let prev = from - 1;
        out.msgs.push((n, Msg::Append { db: self.db, epoch: self.epoch, applied: self.applied, prev_idx: prev, prev_epoch: self.prev_epoch(prev), entries, commit: self.commit }));
        self.next.insert(n, to + 1);
    }

    /// Send every member what it has not been sent yet (pipelined).
    pub fn flush(&mut self, out: &mut WOut) {
        if self.phase != Phase::Active {
            return;
        }
        for n in self.members.clone() {
            let nx = *self.next.get(&n).unwrap_or(&(self.applied + 1));
            if nx <= self.last {
                self.send_from(n, nx, out);
            }
        }
    }

    /// Append one record; returns its index (only when active). Call [`WriterDb::flush`] after a batch of appends.
    pub fn append(&mut self, data: Bytes) -> Option<Idx> {
        if self.phase != Phase::Active {
            return None;
        }
        self.last += 1;
        let e = Entry { idx: self.last, epoch: self.epoch, barrier: false, data };
        self.log.insert(self.last, e);
        Some(self.last)
    }

    fn become_active(&mut self, recovered: Vec<Entry>, out: &mut WOut) {
        for e in &recovered {
            self.log.insert(e.idx, e.clone());
        }
        self.last = recovered.last().map_or(self.applied, |e| e.idx).max(self.applied);
        self.barrier = self.last + 1;
        self.last = self.barrier;
        self.log.insert(self.barrier, Entry { idx: self.barrier, epoch: self.epoch, barrier: true, data: Bytes::new() });
        self.phase = Phase::Active;
        self.ticks = 0;
        for &n in &self.members {
            self.next.insert(n, self.barrier);
            self.matched.insert(n, 0);
        }
        let replay = recovered.into_iter().filter(|e| !e.barrier).collect();
        out.events.push(WEvent::Active { replay, barrier: self.barrier });
        self.flush(out);
    }

    fn choose(&mut self, out: &mut WOut) {
        let need = if self.m.recovery_single { 1 } else { 2 };
        if self.promises.len() < need {
            return;
        }
        let logs = self.promises.iter().map(|(n, (l, e))| (*n, *l, *e));
        match choose_source(logs, self.applied, &self.m) {
            None => self.become_active(vec![], out),
            Some((source, to)) => {
                self.phase = Phase::Reading { source, to };
                self.ticks = 0;
                out.msgs.push((source, Msg::Read { db: self.db, epoch: self.epoch, from: self.applied + 1, to }));
            }
        }
    }

    fn update_commit(&mut self, out: &mut WOut) {
        let mut v: Vec<Idx> = self.members.iter().map(|n| *self.matched.get(n).unwrap_or(&0)).collect();
        v.sort_unstable_by(|a, b| b.cmp(a));
        let k = if self.m.ack_on_one { 0 } else { 1 };
        let cand = v.get(k).copied().unwrap_or(0).min(self.last);
        // only entries of this epoch are counted (everything from the barrier on); earlier ones commit with it
        if cand >= self.barrier && cand > self.commit {
            self.commit = cand;
            out.events.push(WEvent::Committed(cand));
        }
    }

    pub fn on_msg(&mut self, from: NodeId, msg: Msg) -> WOut {
        let mut out = WOut::default();
        if matches!(self.phase, Phase::Stale(_)) || !self.members.contains(&from) {
            return out;
        }
        match msg {
            Msg::Reject { epoch, promised, .. } if epoch == self.epoch && promised > self.epoch => {
                self.phase = Phase::Stale(promised);
                out.events.push(WEvent::Stale(promised));
            }
            Msg::Promise { epoch, last, last_epoch, .. } if epoch == self.epoch => {
                if self.phase == Phase::Fencing {
                    self.promises.insert(from, (last, last_epoch));
                    self.choose(&mut out);
                }
            }
            Msg::Entries { epoch, ok, entries, .. } if epoch == self.epoch => {
                if let Phase::Reading { source, to } = self.phase.clone() {
                    if from != source {
                        return out;
                    }
                    let contiguous = entries.len() as Idx == to - self.applied && entries.iter().enumerate().all(|(i, e)| e.idx == self.applied + 1 + i as Idx);
                    if ok && contiguous {
                        self.become_active(entries, &mut out);
                    } else {
                        // the source changed or answered for another range: fence again from scratch
                        self.phase = Phase::Fencing;
                        self.promises.clear();
                        self.send_fences(&mut out);
                    }
                }
            }
            Msg::Ack { epoch, matched, .. } if epoch == self.epoch => {
                if self.phase == Phase::Active {
                    let m = self.matched.entry(from).or_insert(0);
                    *m = (*m).max(matched.min(self.last));
                    self.update_commit(&mut out);
                }
            }
            Msg::Mismatch { epoch, hint, .. } if epoch == self.epoch => {
                if self.phase == Phase::Active {
                    let nx = (hint + 1).max(self.applied + 1).max(self.matched.get(&from).copied().unwrap_or(0) + 1);
                    let cur = *self.next.get(&from).unwrap_or(&nx);
                    let nx = nx.min(cur);
                    self.send_from(from, nx, &mut out);
                }
            }
            _ => {}
        }
        out
    }

    /// Periodic retransmission (lost messages, restarted nodes, stuck reads).
    pub fn tick(&mut self) -> WOut {
        let mut out = WOut::default();
        self.ticks = (self.ticks + 1).min(5); // only "5 or more" matters; saturating keeps the state finite
        match self.phase.clone() {
            Phase::Fencing => self.send_fences(&mut out),
            Phase::Reading { source, to } => {
                if self.ticks >= 5 {
                    self.phase = Phase::Fencing;
                    self.promises.clear();
                    self.send_fences(&mut out);
                } else {
                    out.msgs.push((source, Msg::Read { db: self.db, epoch: self.epoch, from: self.applied + 1, to }));
                }
            }
            Phase::Active => {
                for n in self.members.clone() {
                    let m = *self.matched.get(&n).unwrap_or(&0);
                    if m < self.last {
                        self.send_from(n, m + 1, &mut out);
                    }
                }
            }
            Phase::Stale(_) => {}
        }
        out
    }

    /// The database state up to `k` is confirmed durable in object storage: trim (design §5.6).
    pub fn uploaded(&mut self, k: Idx) -> WOut {
        let mut out = WOut::default();
        if k <= self.applied {
            return out;
        }
        self.applied = k;
        let keep = self.log.split_off(&(k + 1));
        self.log = keep;
        if self.last < k {
            self.last = k;
        }
        if self.commit < k {
            self.commit = k;
        }
        for &n in &self.members {
            let nx = self.next.entry(n).or_insert(k + 1);
            if *nx <= k {
                *nx = k + 1;
            }
            out.msgs.push((n, Msg::Trim { db: self.db, epoch: self.epoch, upto: k }));
        }
        out
    }

    /// Trim message without the upload (only for the `trim_before_upload` negative control).
    pub fn trim_unconfirmed(&self, k: Idx) -> WOut {
        let mut out = WOut::default();
        for &n in &self.members {
            out.msgs.push((n, Msg::Trim { db: self.db, epoch: self.epoch, upto: k }));
        }
        out
    }

    /// Everything appended is committed (needed before sleep, move, membership change).
    pub fn quiescent(&self) -> bool {
        self.phase == Phase::Active && self.commit >= self.last
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(x: u64) -> Bytes {
        Bytes::copy_from_slice(&x.to_le_bytes())
    }

    /// Instant network, instant disks: deliver everything until quiet.
    fn pump(nodes: &mut BTreeMap<NodeId, NodeCore>, w: &mut WriterDb, mut q: Vec<(NodeId, Msg)>, down: &[NodeId]) -> Vec<WEvent> {
        let mut ev = vec![];
        while !q.is_empty() {
            let mut next = vec![];
            for (n, m) in q {
                if down.contains(&n) {
                    continue;
                }
                let out = nodes.get_mut(&n).unwrap().handle(1000, m);
                assert!(out.broken.is_none(), "{:?}", out.broken);
                for (_, r) in out.replies {
                    let o = w.on_msg(n, r);
                    next.extend(o.msgs);
                    ev.extend(o.events);
                }
            }
            q = next;
        }
        ev
    }

    #[test]
    fn replicate_takeover_fence() {
        let mut nodes: BTreeMap<NodeId, NodeCore> = (1..=3).map(|i| (i, NodeCore::new(i, Mutations::default()))).collect();
        let (mut a, o) = WriterDb::open(7, 10, vec![1, 2, 3], 0, Mutations::default());
        pump(&mut nodes, &mut a, o.msgs, &[]);
        assert!(a.is_active());
        for x in 0..5 {
            a.append(b(x)).unwrap();
        }
        let mut o = WOut::default();
        a.flush(&mut o);
        // only node 1 and 2 get them
        let ev = pump(&mut nodes, &mut a, o.msgs, &[3]);
        assert!(ev.iter().any(|e| matches!(e, WEvent::Committed(6))));
        // a newer writer recovers from nodes 2 and 3 (3 is behind): must recover all 5 entries
        let (mut bw, o) = WriterDb::open(7, 11, vec![1, 2, 3], 0, Mutations::default());
        let ev = pump(&mut nodes, &mut bw, o.msgs, &[1]);
        let replay = ev.iter().find_map(|e| if let WEvent::Active { replay, .. } = e { Some(replay.clone()) } else { None }).unwrap();
        assert_eq!(replay.len(), 5);
        // the old writer is fenced on the nodes that promised 11
        a.append(b(99)).unwrap();
        let mut o = WOut::default();
        a.flush(&mut o);
        let ev = pump(&mut nodes, &mut a, o.msgs, &[1]);
        assert!(ev.iter().any(|e| matches!(e, WEvent::Stale(11))), "{ev:?}");
    }
}

#[cfg(test)]
mod schedule_tests {
    use super::*;

    fn b(x: u64) -> Bytes {
        Bytes::copy_from_slice(&x.to_le_bytes())
    }

    /// Deliver one message to one node and return the writer's events (replies delivered at once).
    fn step(nodes: &mut BTreeMap<NodeId, NodeCore>, w: &mut WriterDb, n: NodeId, m: Msg) -> (Vec<(NodeId, Msg)>, Vec<WEvent>) {
        let (mut msgs, mut ev) = (vec![], vec![]);
        for (_, r) in nodes.get_mut(&n).unwrap().handle(1000, m).replies {
            let o = w.on_msg(n, r);
            msgs.extend(o.msgs);
            ev.extend(o.events);
        }
        (msgs, ev)
    }

    /// The schedule of the `no_fencing` counterexample (2 writers, 2 indices): A (epoch 1) appends W after B (epoch 2)
    /// fenced nodes 1+2 from logs without W; B's barrier then overwrites W on node 1. With the fence, A is rejected and
    /// nothing is acknowledged; without it, W is acknowledged and then lost.
    fn schedule(m: Mutations) -> (bool, bool) {
        let mut nodes: BTreeMap<NodeId, NodeCore> = (1..=3).map(|i| (i, NodeCore::new(i, m))).collect();
        let (mut a, o) = WriterDb::open(1, 1, vec![1, 2, 3], 0, m);
        let mut q = o.msgs;
        while let Some((n, msg)) = q.pop() {
            let (more, _) = step(&mut nodes, &mut a, n, msg);
            q.extend(more);
        }
        assert!(a.is_active());
        // A appends W at index 2; the APPENDs are in flight
        a.append(b(77)).unwrap();
        let mut o = WOut::default();
        a.flush(&mut o);
        let w_msgs = o.msgs;
        // B opens: fences nodes 1 and 2 (whose logs hold only A's barrier), recovers, appends its barrier at 2
        let (mut bw, o) = WriterDb::open(1, 2, vec![1, 2, 3], 0, m);
        let mut barrier_msgs = vec![];
        let mut q: Vec<(NodeId, Msg)> = o.msgs.into_iter().filter(|(n, _)| *n != 3).collect();
        while let Some((n, msg)) = q.pop() {
            let (more, _) = step(&mut nodes, &mut bw, n, msg);
            for (n2, m2) in more {
                if matches!(m2, Msg::Append { .. }) { barrier_msgs.push((n2, m2)) } else { q.push((n2, m2)) }
            }
        }
        assert!(bw.is_active());
        // A's W reaches nodes 1 and 2
        let mut acked = false;
        for (n, msg) in w_msgs.into_iter().filter(|(n, _)| *n != 3) {
            let (_, ev) = step(&mut nodes, &mut a, n, msg);
            acked |= ev.iter().any(|e| matches!(e, WEvent::Committed(c) if *c >= 2));
        }
        // B's barrier reaches node 1
        for (n, msg) in barrier_msgs.into_iter().filter(|(n, _)| *n == 1) {
            step(&mut nodes, &mut bw, n, msg);
        }
        // what would recovery from nodes 1+2 choose?
        let logs: Vec<(NodeId, Idx, Epoch)> = [1, 2].iter().map(|n| { let d = &nodes[n].dbs[&1]; (*n, d.last(), d.last_epoch()) }).collect();
        let src = choose_source(logs, 0, &Mutations::default()).unwrap().0;
        let has_w = nodes[&src].dbs[&1].get(2).map_or(false, |e| e.data == b(77));
        (acked, acked && !has_w)
    }

    #[test]
    fn no_fencing_loses_an_acknowledged_write() {
        assert_eq!(schedule(Mutations::default()), (false, false), "with the fence: W is never acknowledged");
        let m = Mutations { no_fencing: true, ..Default::default() };
        assert_eq!(schedule(m), (true, true), "without the fence: W is acknowledged, then lost");
    }
}
