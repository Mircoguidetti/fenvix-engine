//! `ThreeZone`: the 3-zone [`Journal`] backend of one engine server process (design §5, §10).
//!
//! One link per journal node address (`j3z_net::spawn_link`, reconnecting); one driver task that receives what the
//! links deliver and ticks the writers; per database a [`WriterDb`] (the protocol, `journal3z/core`) for the epoch this
//! process holds. All state sits behind one `std::sync::Mutex` that is never held across an `.await`: `append` assigns
//! the index and hands the entry to the links synchronously, so the journal order is the order of the `append` calls.
//!
//! - `durable` resolves when the core reports the index committed (2 of 3 nodes have it durable, counting only the
//!   writer's own epoch: design §5.3); it fails with `Fenced` when a node reports a newer epoch, with `Erased` when
//!   the database was erased, with `Unavailable` when the database is abandoned (lease lost) or closed.
//! - Database key → protocol id: `DbId = incarnation` (phase 2: the incarnation is unique per database).
//! - Members: [`ThreeZone::open_members`] takes the member node identities from the database's lease record (the
//!   engine stand-in passes them); the trait's `open` uses the identities currently seen on the three links.

use bytes::Bytes;
use j3z_net::{spawn_link, LinkEvent};
use journal3z_api::{Appended, DbKey, Durable, Epoch as JEpoch, Index, Journal, JournalError, Opened};
use journal3z_core::{Addr, DbId, Idx, Msg, Mutations, NodeId, WEvent, WOut, WriterDb, ERASED};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Debug)]
pub struct JournalCfg {
    /// Retransmission tick (lost messages, restarted nodes).
    pub tick: Duration,
    /// How long `open` waits for a majority before giving up (`Unavailable`).
    pub open_timeout: Duration,
    /// Hard cap of a database's bytes not yet uploaded: `append` fails with `Quota` above it.
    pub hard_cap_bytes: usize,
}

impl Default for JournalCfg {
    fn default() -> Self {
        JournalCfg { tick: Duration::from_millis(100), open_timeout: Duration::from_secs(5), hard_cap_bytes: 128 << 20 }
    }
}

type Reply = oneshot::Sender<Result<(), JournalError>>;

struct Db {
    key: DbKey,
    w: WriterDb,
    durables: BTreeMap<Idx, Reply>,
    opening: Option<oneshot::Sender<Result<Opened, JournalError>>>,
    /// bytes per index not yet uploaded (for the quota)
    sizes: BTreeMap<Idx, usize>,
    unflushed: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct Slot {
    node: Option<NodeId>,
    up: bool,
}

struct Erasing {
    seen: BTreeSet<NodeId>,
    done: Option<oneshot::Sender<()>>,
}

struct Inner {
    slots: Vec<Slot>,
    links: Vec<mpsc::UnboundedSender<Vec<Msg>>>,
    dbs: HashMap<DbId, Db>,
    erasing: HashMap<DbId, Erasing>,
}

pub struct ThreeZone {
    id: String,
    cfg: JournalCfg,
    m: Mutations,
    inner: Arc<Mutex<Inner>>,
}

fn fail_all(d: &mut Db, e: JournalError) {
    for (_, tx) in std::mem::take(&mut d.durables) {
        let _ = tx.send(Err(e.clone()));
    }
    if let Some(tx) = d.opening.take() {
        let _ = tx.send(Err(e));
    }
}

impl Inner {
    fn send(&self, msgs: Vec<(NodeId, Msg)>) {
        let mut per: BTreeMap<usize, Vec<Msg>> = BTreeMap::new();
        for (n, m) in msgs {
            if let Some(s) = self.slots.iter().position(|s| s.node == Some(n)) {
                per.entry(s).or_default().push(m);
            }
        }
        for (s, v) in per {
            let _ = self.links[s].send(v);
        }
    }

    /// Send the core's messages, act on its events. Removes the database when it went stale.
    fn process(&mut self, db: DbId, out: WOut) {
        self.send(out.msgs);
        let mut gone = None;
        if let Some(d) = self.dbs.get_mut(&db) {
            for ev in out.events {
                match ev {
                    WEvent::Active { replay, barrier } => {
                        if let Some(tx) = d.opening.take() {
                            let replay = replay.into_iter().map(|e| (Index(e.idx), e.data)).collect();
                            let _ = tx.send(Ok(Opened { replay, next: Index(barrier + 1) }));
                        }
                    }
                    WEvent::Committed(k) => {
                        let rest = d.durables.split_off(&(k + 1));
                        for (_, tx) in std::mem::replace(&mut d.durables, rest) {
                            let _ = tx.send(Ok(()));
                        }
                    }
                    WEvent::Stale(p) => {
                        gone = Some(if p == ERASED { JournalError::Erased } else { JournalError::Fenced { promised: JEpoch(p) } });
                    }
                }
            }
        }
        if let Some(e) = gone {
            if let Some(mut d) = self.dbs.remove(&db) {
                fail_all(&mut d, e);
            }
        }
    }

    fn on_event(&mut self, ev: LinkEvent) {
        match ev {
            LinkEvent::Up { slot, node } => {
                self.slots[slot] = Slot { node: Some(node), up: true };
                // a (re)connected node gets what it missed at once, not at the next tick
                let ids: Vec<DbId> = self.dbs.keys().copied().collect();
                for db in ids {
                    let out = self.dbs.get_mut(&db).unwrap().w.tick();
                    let out = WOut { msgs: out.msgs.into_iter().filter(|(n, _)| *n == node).collect(), events: out.events };
                    self.process(db, out);
                }
            }
            LinkEvent::Down { slot } => self.slots[slot].up = false,
            LinkEvent::Msgs { node, msgs, .. } => {
                for m in msgs {
                    let db = m.db();
                    if let Some(er) = self.erasing.get_mut(&db) {
                        if matches!(m, Msg::Reject { promised: ERASED, .. }) {
                            er.seen.insert(node);
                            if er.seen.len() >= 2 {
                                if let Some(tx) = er.done.take() {
                                    let _ = tx.send(());
                                }
                            }
                        }
                        continue;
                    }
                    if let Some(d) = self.dbs.get_mut(&db) {
                        let out = d.w.on_msg(node, m);
                        self.process(db, out);
                    }
                }
            }
        }
    }

    fn tick(&mut self) {
        let ids: Vec<DbId> = self.dbs.iter().filter(|(_, d)| !d.w.quiescent()).map(|(k, _)| *k).collect();
        for db in ids {
            let out = self.dbs.get_mut(&db).unwrap().w.tick();
            self.process(db, out);
        }
        // erase probes: Erase then a harmless Fence at epoch 0 (writes nothing); an erased node answers Reject(ERASED)
        let er: Vec<DbId> = self.erasing.keys().copied().collect();
        let nodes: Vec<NodeId> = self.slots.iter().filter_map(|s| s.node).collect();
        for db in er {
            let msgs = nodes.iter().flat_map(|&n| [(n, Msg::Erase { db }), (n, Msg::Fence { db, epoch: 0, applied: 0 })]).collect();
            self.send(msgs);
        }
    }
}

impl ThreeZone {
    /// `id`: this journal's identity (the database marker's journal id). `me`: this process's address in the protocol
    /// (any u32, unique among writers). `nodes`: the journal nodes' addresses (three).
    pub fn new(id: &str, me: Addr, nodes: &[String], cfg: JournalCfg, m: Mutations) -> Arc<Self> {
        let (etx, mut erx) = mpsc::unbounded_channel();
        let links = nodes.iter().enumerate().map(|(i, a)| spawn_link(i, a.clone(), me, etx.clone())).collect();
        let inner = Arc::new(Mutex::new(Inner { slots: vec![Slot::default(); nodes.len()], links, dbs: HashMap::new(), erasing: HashMap::new() }));
        let tz = Arc::new(ThreeZone { id: id.to_string(), cfg: cfg.clone(), m, inner: inner.clone() });
        let weak = Arc::downgrade(&inner);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(cfg.tick);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    ev = erx.recv() => {
                        let (Some(ev), Some(inner)) = (ev, weak.upgrade()) else { return };
                        let mut g = inner.lock().unwrap();
                        g.on_event(ev);
                        while let Ok(ev) = erx.try_recv() {
                            g.on_event(ev);
                        }
                    }
                    _ = tick.tick() => {
                        let Some(inner) = weak.upgrade() else { return };
                        inner.lock().unwrap().tick();
                    }
                }
            }
        });
        tz
    }

    /// Node identities currently seen on the links (None for a link never connected).
    pub fn node_ids(&self) -> Vec<Option<NodeId>> {
        self.inner.lock().unwrap().slots.iter().map(|s| s.node).collect()
    }

    /// How many of `members` have a link up now.
    pub fn members_up(&self, members: &[NodeId]) -> usize {
        let g = self.inner.lock().unwrap();
        members.iter().filter(|n| g.slots.iter().any(|s| s.up && s.node == Some(**n))).count()
    }

    /// Like [`Journal::open`], with the members taken from the database's lease record.
    pub async fn open_members(&self, key: &DbKey, epoch: JEpoch, applied: Index, members: Vec<NodeId>) -> Result<Opened, JournalError> {
        let db = key.incarnation;
        let (tx, rx) = oneshot::channel();
        {
            let mut g = self.inner.lock().unwrap();
            if let Some(mut old) = g.dbs.remove(&db) {
                fail_all(&mut old, JournalError::Fenced { promised: epoch });
            }
            let (w, out) = WriterDb::open(db, epoch.0, members, applied.0, self.m);
            g.dbs.insert(db, Db { key: key.clone(), w, durables: BTreeMap::new(), opening: Some(tx), sizes: BTreeMap::new(), unflushed: 0 });
            g.process(db, out);
        }
        let r = match tokio::time::timeout(self.cfg.open_timeout, rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err(JournalError::Unavailable("journal stopped".into())),
            Err(_) => Err(JournalError::Unavailable("no majority of the members answered".into())),
        };
        if r.is_err() {
            let mut g = self.inner.lock().unwrap();
            if g.dbs.get(&db).map_or(false, |d| d.w.epoch == epoch.0 && !d.w.is_active()) {
                g.dbs.remove(&db);
            }
        }
        r
    }

    fn with_db<T>(&self, key: &DbKey, epoch: JEpoch, f: impl FnOnce(&mut Inner, DbId) -> Result<T, JournalError>) -> Result<T, JournalError> {
        let mut g = self.inner.lock().unwrap();
        let db = key.incarnation;
        match g.dbs.get(&db) {
            Some(d) if d.w.epoch == epoch.0 && d.w.is_active() => f(&mut g, db),
            Some(d) if d.w.epoch > epoch.0 => Err(JournalError::Fenced { promised: JEpoch(d.w.epoch) }),
            _ => Err(JournalError::Unavailable(format!("database {} not open at epoch {}", key.name, epoch.0))),
        }
    }

    /// Highest committed index of the database (None when it is not open at `epoch`).
    pub fn committed(&self, key: &DbKey, epoch: JEpoch) -> Option<Index> {
        self.with_db(key, epoch, |g, db| Ok(Index(g.dbs[&db].w.commit))).ok()
    }

    /// Everything appended is committed.
    pub fn quiescent(&self, key: &DbKey, epoch: JEpoch) -> bool {
        self.with_db(key, epoch, |g, db| Ok(g.dbs[&db].w.quiescent())).unwrap_or(false)
    }

    /// Bytes appended and not yet uploaded.
    pub fn unflushed(&self, key: &DbKey, epoch: JEpoch) -> usize {
        self.with_db(key, epoch, |g, db| Ok(g.dbs[&db].unflushed)).unwrap_or(0)
    }

    /// Stop writing the database here (lease lost or released): pending acknowledgements fail with `Unavailable`.
    pub fn abandon(&self, key: &DbKey, why: &str) {
        let mut g = self.inner.lock().unwrap();
        if let Some(mut d) = g.dbs.remove(&key.incarnation) {
            fail_all(&mut d, JournalError::Unavailable(why.to_string()));
        }
    }

    /// Negative control `trim_before_upload`: trim the journal before the upload is confirmed.
    pub fn trim_unconfirmed(&self, key: &DbKey, epoch: JEpoch, upto: Index) {
        let _ = self.with_db(key, epoch, |g, db| {
            let out = g.dbs[&db].w.trim_unconfirmed(upto.0);
            g.send(out.msgs);
            Ok(())
        });
    }
}

#[async_trait::async_trait]
impl Journal for ThreeZone {
    fn id(&self) -> &str {
        &self.id
    }

    async fn open(&self, db: &DbKey, epoch: JEpoch, applied: Index) -> Result<Opened, JournalError> {
        let members: Vec<NodeId> = self.node_ids().into_iter().flatten().collect();
        if members.len() < 3 {
            return Err(JournalError::Unavailable("not every journal node identity is known yet".into()));
        }
        self.open_members(db, epoch, applied, members).await
    }

    fn append(&self, key: &DbKey, epoch: JEpoch, payload: Bytes) -> Result<Appended, JournalError> {
        let cap = self.cfg.hard_cap_bytes;
        self.with_db(key, epoch, |g, db| {
            let d = g.dbs.get_mut(&db).unwrap();
            if d.unflushed + payload.len() > cap {
                return Err(JournalError::Quota);
            }
            let n = payload.len();
            let idx = d.w.append(payload).expect("active");
            d.sizes.insert(idx, n);
            d.unflushed += n;
            let (tx, durable) = Durable::channel();
            d.durables.insert(idx, tx);
            let mut out = WOut::default();
            d.w.flush(&mut out);
            g.send(out.msgs);
            Ok(Appended { index: Index(idx), durable })
        })
    }

    async fn uploaded(&self, key: &DbKey, epoch: JEpoch, index: Index) -> Result<(), JournalError> {
        self.with_db(key, epoch, |g, db| {
            let d = g.dbs.get_mut(&db).unwrap();
            if index.0 > d.w.commit {
                return Err(JournalError::Invalid(format!("upload {} above the committed index {}", index.0, d.w.commit)));
            }
            let rest = d.sizes.split_off(&(index.0 + 1));
            let freed: usize = std::mem::replace(&mut d.sizes, rest).values().sum();
            d.unflushed -= freed;
            let out = d.w.uploaded(index.0);
            g.process(db, out);
            Ok(())
        })
    }

    async fn close(&self, key: &DbKey, epoch: JEpoch) -> Result<(), JournalError> {
        let mut g = self.inner.lock().unwrap();
        match g.dbs.get(&key.incarnation) {
            Some(d) if d.w.epoch == epoch.0 => {
                // every record appended here was uploaded (`sizes` holds the records, not barriers)
                if !d.sizes.is_empty() {
                    return Err(JournalError::Invalid("records not yet uploaded".into()));
                }
                g.dbs.remove(&key.incarnation);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn erase(&self, key: &DbKey) -> Result<(), JournalError> {
        let db = key.incarnation;
        let (tx, rx) = oneshot::channel();
        {
            let mut g = self.inner.lock().unwrap();
            if let Some(mut d) = g.dbs.remove(&db) {
                fail_all(&mut d, JournalError::Erased);
            }
            g.erasing.insert(db, Erasing { seen: BTreeSet::new(), done: Some(tx) });
            g.tick();
        }
        let r = tokio::time::timeout(self.cfg.open_timeout, rx).await;
        self.inner.lock().unwrap().erasing.remove(&db);
        match r {
            Ok(Ok(())) => Ok(()),
            _ => Err(JournalError::Unavailable("erase not confirmed by a majority".into())),
        }
    }

    /// The databases this process has open with records not yet uploaded. (After a restart this process knows none:
    /// with the 3-zone journal the databases to recover are found from the lease records, not from a local disk.)
    async fn pending(&self) -> Result<Vec<(DbKey, Index)>, JournalError> {
        let g = self.inner.lock().unwrap();
        Ok(g.dbs.values().filter(|d| d.w.last > d.w.applied).map(|d| (d.key.clone(), Index(d.w.last))).collect())
    }
}
