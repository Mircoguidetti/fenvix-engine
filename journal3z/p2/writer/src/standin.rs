//! Engine stand-in for the local bench (until P6 wires the real engine, after PR #1 is merged).
//!
//! One `Server` per writer process. For every database of the bench it runs the ownership cycle the engine will run:
//! lease (conditional PUT) → object-store writer fence + marker read → journal open (replay) → accept writes →
//! upload every `upload_every` (or early at the soft cap) → `journal.uploaded` (trim). A database's "state" is the map
//! `index → write id`; a write's payload is its 8-byte id plus padding.
//!
//! Lines on stdout (the bench's checker reads them; one event per line, `key=value`):
//! - `OPEN db=<d> epoch=<e> applied=<a> barrier=<b> replay=<n>`: this process became the writer at epoch e.
//! - `UPLOAD db=<d> epoch=<e> upto=<k>`; `LOST db=<d> epoch=<e> why=<..>`; `RECONFIG db=<d> epoch=<e> members=<..>`.

use crate::journal::ThreeZone;
use crate::meta::{Held, LeaseOps, Meta, MetaError, StoreOps};
use bytes::Bytes;
use journal3z_api::{DbKey, Epoch as JEpoch, Index, Journal, JournalError};
use journal3z_core::{Idx, Mutations, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Every timing and size of the engine side. Defaults = the values decided for phase 2 (decision 4); all of them can
/// be overridden (`--config <json>` in `j3z-writer`).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerCfg {
    /// Lease duration in journal mode.
    pub lease_ttl_ms: i64,
    /// Clock-skew allowance before taking over an expired lease.
    pub lease_skew_ms: i64,
    /// Upload period.
    pub upload_every_ms: u64,
    /// Early upload when a database's bytes not yet uploaded reach this.
    pub soft_cap_bytes: usize,
    /// Writes refused (quota) above this.
    pub hard_cap_bytes: usize,
    /// Release the lease after this long without a journal majority.
    pub release_after_ms: u64,
    /// How often the preferred writer of a database tries to take it; the others try `other_poll_ms`.
    pub preferred_poll_ms: u64,
    pub other_poll_ms: u64,
    /// Journal open timeout.
    pub open_timeout_ms: u64,
    /// Journal retransmission tick.
    pub tick_ms: u64,
}

impl Default for ServerCfg {
    fn default() -> Self {
        ServerCfg {
            lease_ttl_ms: 10_000,
            lease_skew_ms: 1_500,
            upload_every_ms: 30_000,
            soft_cap_bytes: 32 << 20,
            hard_cap_bytes: 128 << 20,
            release_after_ms: 5_000,
            preferred_poll_ms: 200,
            other_poll_ms: 1_000,
            open_timeout_ms: 5_000,
            tick_ms: 100,
        }
    }
}

/// The process clock: wall clock plus an offset (the bench moves it to test lease expiry with skewed clocks).
#[derive(Clone, Default)]
pub struct Clock(pub Arc<AtomicI64>);

impl Clock {
    pub fn now_ms(&self) -> i64 {
        let wall = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
        wall + self.0.load(SeqCst)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WriteErr {
    NotOwner,
    Fenced,
    Quota,
    Unavailable,
    Erased,
}

struct Active {
    epoch: JEpoch,
    /// state not yet uploaded: index → write id (replayed + own, in index order)
    pending: BTreeMap<Idx, u64>,
    /// marker in the store
    store_applied: Idx,
    /// writes refused while the membership changes
    paused: bool,
}

struct DbRun {
    key: DbKey,
    active: Mutex<Option<Active>>,
    upload_now: tokio::sync::Notify,
}

pub struct Server {
    pub holder: String,
    pub index: usize,
    pub nwriters: usize,
    pub journal: Arc<ThreeZone>,
    pub meta: Meta,
    pub cfg: ServerCfg,
    pub m: Mutations,
    pub clock: Clock,
    dbs: HashMap<u64, Arc<DbRun>>,
}

fn log(line: String) {
    use std::io::Write;
    let mut o = std::io::stdout().lock();
    let _ = writeln!(o, "{line}");
    let _ = o.flush();
}

pub fn payload(id: u64, size: usize) -> Bytes {
    let mut v = id.to_le_bytes().to_vec();
    v.resize(size.max(8), 0);
    Bytes::from(v)
}

fn id_of(p: &[u8]) -> u64 {
    u64::from_le_bytes(p[..8].try_into().unwrap())
}

impl Server {
    /// `ndbs` databases, ids `1..=ndbs` (incarnation = id; name `db<id>`).
    #[allow(clippy::too_many_arguments)]
    pub fn new(holder: &str, index: usize, nwriters: usize, journal: Arc<ThreeZone>, meta: Meta, cfg: ServerCfg, m: Mutations, clock: Clock, ndbs: u64) -> Arc<Self> {
        let dbs = (1..=ndbs).map(|d| (d, Arc::new(DbRun { key: DbKey::new(format!("db{d}"), d), active: Mutex::new(None), upload_now: tokio::sync::Notify::new() }))).collect();
        Arc::new(Server { holder: holder.to_string(), index, nwriters, journal, meta, cfg, m, clock, dbs })
    }

    pub fn start(self: &Arc<Self>) {
        for d in self.dbs.keys().copied() {
            let s = self.clone();
            tokio::spawn(async move { s.run_db(d).await });
        }
    }

    /// The databases this process is the writer of, with their epochs.
    pub fn owned(&self) -> Vec<(u64, u64)> {
        let mut v: Vec<(u64, u64)> = self.dbs.iter().filter_map(|(d, r)| r.active.lock().unwrap().as_ref().map(|a| (*d, a.epoch.0))).collect();
        v.sort();
        v
    }

    /// A client write: append (index assigned under the database's lock, applied to the state), then wait until the
    /// journal acknowledges it. Returns (index, epoch).
    pub async fn write(&self, db: u64, id: u64, size: usize) -> Result<(Idx, u64), WriteErr> {
        let Some(r) = self.dbs.get(&db) else { return Err(WriteErr::NotOwner) };
        let (app, epoch) = {
            let mut g = r.active.lock().unwrap();
            let Some(a) = g.as_mut() else { return Err(WriteErr::NotOwner) };
            if a.paused {
                return Err(WriteErr::Unavailable);
            }
            let app = self.journal.append(&r.key, a.epoch, payload(id, size)).map_err(map_err)?;
            a.pending.insert(app.index.0, id);
            (app, a.epoch)
        };
        if self.journal.unflushed(&r.key, epoch) >= self.cfg.soft_cap_bytes {
            r.upload_now.notify_one();
        }
        let ttl = Duration::from_millis(self.cfg.lease_ttl_ms as u64);
        match tokio::time::timeout(ttl, app.durable).await {
            Ok(Ok(())) => Ok((app.index.0, epoch.0)),
            Ok(Err(e)) => Err(map_err(e)),
            Err(_) => Err(WriteErr::Unavailable),
        }
    }

    /// Upload now (the bench calls this at the end of a run). Returns the store marker after it.
    pub async fn flush(&self, db: u64) -> Option<Idx> {
        let r = self.dbs.get(&db)?;
        r.upload_now.notify_one();
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let g = r.active.lock().unwrap();
            let a = g.as_ref()?;
            let c = self.journal.committed(&r.key, a.epoch)?;
            if a.store_applied >= c.0 && self.journal.quiescent(&r.key, a.epoch) {
                return Some(a.store_applied);
            }
        }
        None
    }

    fn lease(&self) -> LeaseOps<'_> {
        LeaseOps { meta: &self.meta, holder: &self.holder, ttl_ms: self.cfg.lease_ttl_ms, skew_ms: self.cfg.lease_skew_ms }
    }

    fn store(&self) -> StoreOps<'_> {
        StoreOps { meta: &self.meta, skip_fence: self.m.skip_store_fence }
    }

    async fn run_db(self: Arc<Self>, db: u64) {
        let r = self.dbs[&db].clone();
        let preferred = (db as usize) % self.nwriters == self.index;
        let poll = Duration::from_millis(if preferred { self.cfg.preferred_poll_ms } else { self.cfg.other_poll_ms });
        loop {
            tokio::time::sleep(poll).await;
            let ids: Vec<NodeId> = self.journal.node_ids().into_iter().flatten().collect();
            if ids.len() < 3 {
                continue;
            }
            let Ok(mut held) = self.lease().acquire(db, self.clock.now_ms(), &ids).await else { continue };
            let why = self.own(&r, &mut held).await;
            self.journal.abandon(&r.key, &why);
            let epoch = r.active.lock().unwrap().take().map(|a| a.epoch.0).unwrap_or(held.epoch);
            log(format!("LOST db={db} epoch={epoch} why={}", why.replace(' ', "_")));
        }
    }

    /// Open as the writer at `held.epoch` (store fence, journal open, replay).
    async fn open(&self, r: &DbRun, held: &Held) -> Result<(), String> {
        let db = r.key.incarnation;
        let applied = self.store().open(db, held.epoch).await.map_err(|e| format!("store open: {e}"))?;
        let opened = self.journal.open_members(&r.key, JEpoch(held.epoch), Index(applied), held.members.clone()).await.map_err(|e| format!("journal open: {e}"))?;
        let mut pending = BTreeMap::new();
        for (i, p) in &opened.replay {
            if p.len() >= 8 {
                pending.insert(i.0, id_of(p));
            }
        }
        log(format!("OPEN db={db} epoch={} applied={applied} barrier={} replay={}", held.epoch, opened.next.0 - 1, opened.replay.len()));
        *r.active.lock().unwrap() = Some(Active { epoch: JEpoch(held.epoch), pending, store_applied: applied, paused: false });
        Ok(())
    }

    /// Hold the database until the lease is lost; returns why.
    async fn own(&self, r: &DbRun, held: &mut Held) -> String {
        let db = r.key.incarnation;
        if let Err(e) = self.open(r, held).await {
            let _ = self.lease().release(db, held).await;
            return e;
        }
        let ttl = self.cfg.lease_ttl_ms;
        let mut last_renew = self.clock.now_ms();
        let mut last_upload = Instant::now();
        let mut no_journal_since: Option<Instant> = None;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(self.cfg.tick_ms)) => {}
                _ = r.upload_now.notified() => { last_upload = Instant::now() - Duration::from_millis(self.cfg.upload_every_ms); }
            }
            let now = self.clock.now_ms();
            if now >= held.valid_until_ms {
                return "lease expired".into();
            }
            if now - last_renew >= ttl / 3 {
                match self.lease().renew(db, held, now).await {
                    Ok(()) => last_renew = now,
                    Err(MetaError::Conflict) => return "lease taken".into(),
                    Err(MetaError::Unavailable(_)) => {}
                }
            }
            let epoch = match r.active.lock().unwrap().as_ref() {
                Some(a) => a.epoch,
                None => return "abandoned".into(),
            };
            if self.journal.committed(&r.key, epoch).is_none() {
                return "journal fenced".into();
            }
            // decision 4: release the lease after `release_after` without a journal majority
            if self.journal.members_up(&held.members) < 2 {
                let t = *no_journal_since.get_or_insert_with(Instant::now);
                if t.elapsed() >= Duration::from_millis(self.cfg.release_after_ms) {
                    let _ = self.lease().release(db, held).await;
                    return "no journal majority".into();
                }
            } else {
                no_journal_since = None;
            }
            if last_upload.elapsed() >= Duration::from_millis(self.cfg.upload_every_ms) {
                last_upload = Instant::now();
                if let Err(e) = self.upload(r, held).await {
                    return e;
                }
            }
        }
    }

    /// Upload the committed state; at an upload point, also change the membership when a node got a new identity.
    async fn upload(&self, r: &DbRun, held: &mut Held) -> Result<(), String> {
        let db = r.key.incarnation;
        let (epoch, recs, upto) = {
            let g = r.active.lock().unwrap();
            let a = g.as_ref().ok_or("abandoned")?;
            let upto = self.journal.committed(&r.key, a.epoch).ok_or("journal fenced")?.0;
            let recs: Vec<(Idx, u64)> = a.pending.range(..=upto).map(|(i, id)| (*i, *id)).collect();
            (a.epoch, recs, upto)
        };
        if upto > r.active.lock().unwrap().as_ref().map_or(0, |a| a.store_applied) {
            if self.m.trim_before_upload {
                self.journal.trim_unconfirmed(&r.key, epoch, Index(upto));
            }
            match self.store().upload(db, epoch.0, upto, &recs).await {
                Ok(()) => {}
                Err(MetaError::Conflict) => return Err("store fenced".into()),
                Err(MetaError::Unavailable(e)) => {
                    log(format!("UPLOAD_FAILED db={db} epoch={} err={}", epoch.0, e.replace(' ', "_")));
                    return Ok(());
                }
            }
            {
                let mut g = r.active.lock().unwrap();
                let a = g.as_mut().ok_or("abandoned")?;
                a.pending = a.pending.split_off(&(upto + 1));
                a.store_applied = upto;
            }
            self.journal.uploaded(&r.key, epoch, Index(upto)).await.map_err(|e| format!("journal: {e}"))?;
            log(format!("UPLOAD db={db} epoch={} upto={upto}", epoch.0));
        }
        self.maybe_reconfigure(r, held).await
    }

    /// Design §5.8: a member that lost its disk comes back with a new identity; the writer replaces it at an upload
    /// point (everything committed is in the store), with a new epoch.
    async fn maybe_reconfigure(&self, r: &DbRun, held: &mut Held) -> Result<(), String> {
        let db = r.key.incarnation;
        #[cfg(feature = "mutations")]
        if std::env::var("J3Z_TESTBUG_NO_RECONFIG").is_ok() {
            return Ok(()); // negative control: never replace a member that lost its disk
        }
        let ids: Vec<NodeId> = self.journal.node_ids().into_iter().flatten().collect();
        let mut want = ids.clone();
        want.sort();
        let mut cur = held.members.clone();
        cur.sort();
        if ids.len() < 3 || want == cur || self.journal.members_up(&want) < 3 {
            return Ok(());
        }
        // pause writes, wait until everything appended is committed, upload it, then switch
        let epoch = {
            let mut g = r.active.lock().unwrap();
            let a = g.as_mut().ok_or("abandoned")?;
            a.paused = true;
            a.epoch
        };
        let t0 = Instant::now();
        while !self.journal.quiescent(&r.key, epoch) {
            if t0.elapsed() > Duration::from_secs(2) {
                if let Some(a) = r.active.lock().unwrap().as_mut() {
                    a.paused = false;
                }
                return Ok(()); // try at the next upload point
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let (recs, upto) = {
            let g = r.active.lock().unwrap();
            let a = g.as_ref().ok_or("abandoned")?;
            let upto = self.journal.committed(&r.key, a.epoch).ok_or("journal fenced")?.0;
            (a.pending.iter().map(|(i, id)| (*i, *id)).collect::<Vec<_>>(), upto)
        };
        match self.store().upload(db, epoch.0, upto, &recs).await {
            Ok(()) => {}
            Err(MetaError::Conflict) => return Err("store fenced".into()),
            Err(MetaError::Unavailable(_)) => {
                if let Some(a) = r.active.lock().unwrap().as_mut() {
                    a.paused = false;
                }
                return Ok(());
            }
        }
        if let Some(a) = r.active.lock().unwrap().as_mut() {
            a.pending.clear();
            a.store_applied = upto;
        }
        self.journal.uploaded(&r.key, epoch, Index(upto)).await.map_err(|e| format!("journal: {e}"))?;
        self.lease().reconfigure(db, held, want.clone(), self.clock.now_ms()).await.map_err(|e| format!("reconfigure: {e}"))?;
        let ms: Vec<String> = want.iter().map(|n| n.to_string()).collect();
        log(format!("RECONFIG db={db} epoch={} members={}", held.epoch, ms.join(",")));
        self.journal.abandon(&r.key, "membership change");
        self.open(r, held).await
    }
}

fn map_err(e: JournalError) -> WriteErr {
    match e {
        JournalError::Fenced { .. } => WriteErr::Fenced,
        JournalError::Quota => WriteErr::Quota,
        JournalError::Erased => WriteErr::Erased,
        JournalError::Unavailable(_) | JournalError::Io(_) | JournalError::Invalid(_) => WriteErr::Unavailable,
    }
}
