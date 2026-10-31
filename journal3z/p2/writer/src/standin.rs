//! Engine stand-in for the local bench (until P6 wires the real engine, after PR #1 is merged).
//!
//! One `Server` per writer process. For every database of the bench it runs the ownership cycle the engine will run:
//! lease (conditional PUT) → object-store writer fence + marker read → journal open (replay) → accept writes →
//! upload every `upload_every` (or early at the soft cap) → `journal.uploaded` (trim). A database's "state" is the map
//! `index → write id`; a write's payload is its 8-byte id plus padding.
//!
//! Lines on stdout (the bench's checker reads them; one event per line, `key=value`):
//! - `OPEN db=<d> epoch=<e> applied=<a> barrier=<b> replay=<n>`: this process became the writer at epoch e.
//! - `UPLOAD db=<d> epoch=<e> upto=<k>`; `LOST db=<d> epoch=<e> why=<..> at_real_ms=<t>`;
//!   `RECONFIG db=<d> epoch=<e> members=<..>`.
//! - Lease (for the bench's overlap check, real time = the machine's clock, not the faked ones):
//!   `TAKEN db=<d> epoch=<e> at_real_ms=<t>` when this process sent the write that took the lease;
//!   `HOLD db=<d> epoch=<e> now_real_ms=<t> until_real_ms=<u>` each time its deadline changes (acquire, renew, change
//!   of its clock's rate): until `u` this process believes it holds the lease.

use crate::journal::ThreeZone;
use crate::meta::{Held, LeaseOps, Meta, MetaError, StoreOps};
use bytes::Bytes;
use journal3z_api::{DbKey, Epoch as JEpoch, Index, Journal, JournalError};
use journal3z_core::lease::{LeaseBug, Observer, TakeoverRule};
use journal3z_core::{Idx, Mutations, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Every timing and size of the engine side. Defaults = the values decided for phase 2 (decision 4); all of them can
/// be overridden (`--config <json>` in `j3z-writer`).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerCfg {
    /// Lease duration in journal mode.
    pub lease_ttl_ms: i64,
    /// Takeover margin (design §5.1): another server takes over after seeing the lease unchanged for lease + margin
    /// on its own monotonic clock. Covers clock rate drift (each clock up to ≈ 6.9 % with 10 s + 1.5 s).
    pub lease_margin_ms: i64,
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
            lease_margin_ms: 1_500,
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

/// The process clocks. `mono_ms`: monotonic (from `Instant`), running at 1 + rate (the bench sets a rate to test
/// drift; 0 in production). `wall_ms`: the system clock plus an offset (the bench makes it jump; the lease does not
/// use it, only the `lease_wall_clock` negative control does).
#[derive(Clone)]
pub struct Clock(Arc<Mutex<ClockState>>);

struct ClockState {
    base: Instant,
    base_mono: f64,
    rate_ppm: i64,
    wall_offset_ms: i64,
    /// forward jumps of the monotonic clock (bench fault: a paused machine catching up), ms
    mono_jump_ms: u64,
    generation: u64,
}

impl Default for Clock {
    fn default() -> Self {
        Clock(Arc::new(Mutex::new(ClockState { base: Instant::now(), base_mono: 1_000_000.0, rate_ppm: 0, wall_offset_ms: 0, mono_jump_ms: 0, generation: 0 })))
    }
}

pub fn real_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64
}

impl Clock {
    fn mono_of(c: &ClockState, at: Instant) -> f64 {
        c.base_mono + c.mono_jump_ms as f64 + (at - c.base).as_secs_f64() * 1000.0 * (1.0 + c.rate_ppm as f64 / 1e6)
    }
    pub fn mono_ms(&self) -> u64 {
        let c = self.0.lock().unwrap();
        Self::mono_of(&c, Instant::now()) as u64
    }
    pub fn wall_ms(&self) -> u64 {
        (real_ms() + self.0.lock().unwrap().wall_offset_ms).max(0) as u64
    }
    pub fn set_rate_ppm(&self, ppm: i64) {
        let mut c = self.0.lock().unwrap();
        if c.rate_ppm != ppm {
            Self::rebase(&mut c, Instant::now(), ppm, false);
        }
    }

    /// Change the rate without moving the clock. `base_mono` excludes the forward jumps (added by `mono_of`).
    /// (Found by the §2a run, chunk 1: the old version stored `mono_of(now)`, jumps included, as the new base, so the
    /// jumps were counted twice and every rate change made the clock leap forward by all earlier jumps.)
    /// `double_count_jumps` = that bug, kept for the regression test's twin.
    fn rebase(c: &mut ClockState, now: Instant, ppm: i64, double_count_jumps: bool) {
        let jumps = if double_count_jumps { 0.0 } else { c.mono_jump_ms as f64 };
        c.base_mono = Self::mono_of(c, now) - jumps;
        c.base = now;
        c.rate_ppm = ppm;
        c.generation += 1;
    }
    /// Total forward jump of the monotonic clock so far (only grows: a monotonic clock never goes back).
    pub fn set_mono_jump_ms(&self, ms: u64) {
        let mut c = self.0.lock().unwrap();
        if ms > c.mono_jump_ms {
            c.mono_jump_ms = ms;
            c.generation += 1;
        }
    }
    pub fn set_wall_offset_ms(&self, ms: i64) {
        self.0.lock().unwrap().wall_offset_ms = ms;
    }
    /// Changes whenever the rate changes (deadlines in real time move).
    pub fn generation(&self) -> u64 {
        self.0.lock().unwrap().generation
    }
    /// The real time (machine clock, ms) at which this monotonic clock reaches `mono` at the current rate.
    pub fn real_at(&self, mono: u64) -> i64 {
        let c = self.0.lock().unwrap();
        let now = Instant::now();
        let m = Self::mono_of(&c, now);
        real_ms() + ((mono as f64 - m) / (1.0 + c.rate_ppm as f64 / 1e6)) as i64
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
    /// lease deadline on this process's monotonic clock: no write is accepted at or after it
    valid_until: u64,
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
    /// negative control of the takeover rule (`LeaseBug::None` in production)
    pub lease_bug: LeaseBug,
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
    pub fn new(holder: &str, index: usize, nwriters: usize, journal: Arc<ThreeZone>, meta: Meta, cfg: ServerCfg, m: Mutations, lease_bug: LeaseBug, clock: Clock, ndbs: u64) -> Arc<Self> {
        let dbs = (1..=ndbs).map(|d| (d, Arc::new(DbRun { key: DbKey::new(format!("db{d}"), d), active: Mutex::new(None), upload_now: tokio::sync::Notify::new() }))).collect();
        // the holder name is unique per process: a restarted server is another server and waits like the others
        let holder = format!("{holder}-{:08x}", rand::random::<u32>());
        Arc::new(Server { holder, index, nwriters, journal, meta, cfg, m, clock, lease_bug, dbs })
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
            if self.clock.mono_ms() >= a.valid_until {
                return Err(WriteErr::NotOwner);
            }
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

    fn rule(&self) -> TakeoverRule {
        TakeoverRule { ttl: self.cfg.lease_ttl_ms as u64, margin: self.cfg.lease_margin_ms as u64, bug: self.lease_bug }
    }

    fn lease(&self) -> LeaseOps<'_> {
        LeaseOps { meta: &self.meta, holder: &self.holder, rule: self.rule() }
    }

    /// Print the lease deadline in real time (for the bench's overlap check).
    fn hold_line(&self, db: u64, epoch: u64, valid_until: u64) {
        log(format!("HOLD db={db} epoch={epoch} now_real_ms={} until_real_ms={}", real_ms(), self.clock.real_at(valid_until)));
    }

    fn store(&self) -> StoreOps<'_> {
        StoreOps { meta: &self.meta, skip_fence: self.m.skip_store_fence }
    }

    async fn run_db(self: Arc<Self>, db: u64) {
        let r = self.dbs[&db].clone();
        let preferred = (db as usize) % self.nwriters == self.index;
        let poll = Duration::from_millis(if preferred { self.cfg.preferred_poll_ms } else { self.cfg.other_poll_ms });
        let mut observer = Observer::default();
        loop {
            tokio::time::sleep(poll).await;
            let ids: Vec<NodeId> = self.journal.node_ids().into_iter().flatten().collect();
            if ids.len() < 3 {
                continue;
            }
            // a server that cannot reach a journal majority does not take databases: it would only hold the lease
            // until its journal open times out, again and again, and keep the servers that can reach it away
            if self.journal.members_up(&ids) < 2 {
                continue;
            }
            // design §5.1: take the lease only if it is free, ours, or unchanged for lease + margin on our own
            // monotonic clock; the write is conditional on the exact version read
            let Ok(cur) = self.lease().read(db).await else { continue };
            if !self.lease().may_take(&cur, &mut observer, self.clock.mono_ms(), self.clock.wall_ms()) {
                continue;
            }
            let at = real_ms();
            let sent = self.clock.mono_ms();
            let Ok(mut held) = self.lease().take(db, &cur, &ids, sent).await else { continue };
            observer.reset();
            log(format!("TAKEN db={db} epoch={} at_real_ms={at}", held.epoch));
            self.hold_line(db, held.epoch, held.valid_until_mono);
            let (why, release) = self.own(&r, &mut held).await;
            // stop acting first (no more writes accepted), then give the lease up: the order matters (found by the
            // §2a run, attempt 2: releasing first let another server take the lease while this one still accepted
            // writes for a few milliseconds)
            self.journal.abandon(&r.key, &why);
            let epoch = r.active.lock().unwrap().take().map(|a| a.epoch.0).unwrap_or(held.epoch);
            log(format!("LOST db={db} epoch={epoch} why={} at_real_ms={}", why.replace(' ', "_"), real_ms()));
            if release {
                let _ = self.lease().release(db, &held).await;
            }
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
        *r.active.lock().unwrap() = Some(Active { epoch: JEpoch(held.epoch), pending, store_applied: applied, paused: false, valid_until: held.valid_until_mono });
        Ok(())
    }

    /// Hold the database until the lease is lost; returns why.
    /// Hold the database until the lease is lost or must be given up; returns why, and whether the caller must
    /// release the lease (after it has stopped acting).
    async fn own(&self, r: &DbRun, held: &mut Held) -> (String, bool) {
        let db = r.key.incarnation;
        if let Err(e) = self.open(r, held).await {
            return (e, true);
        }
        let ttl = self.cfg.lease_ttl_ms as u64;
        let mut last_renew = self.clock.mono_ms();
        let mut gen = self.clock.generation();
        let mut last_upload = Instant::now();
        let mut no_journal_since: Option<Instant> = None;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(self.cfg.tick_ms)) => {}
                _ = r.upload_now.notified() => { last_upload = Instant::now() - Duration::from_millis(self.cfg.upload_every_ms); }
            }
            let now = self.clock.mono_ms();
            if now >= held.valid_until_mono {
                return ("lease expired".into(), false);
            }
            if now - last_renew >= ttl / 3 {
                match self.lease().renew(db, held, now).await {
                    Ok(()) => {
                        last_renew = now;
                        if let Some(a) = r.active.lock().unwrap().as_mut() {
                            a.valid_until = held.valid_until_mono;
                        }
                        self.hold_line(db, held.epoch, held.valid_until_mono);
                    }
                    Err(MetaError::Conflict) => return ("lease taken".into(), false),
                    Err(MetaError::Unavailable(_)) => {}
                }
            }
            if self.clock.generation() != gen {
                gen = self.clock.generation();
                self.hold_line(db, held.epoch, held.valid_until_mono); // the deadline moved in real time
            }
            let epoch = match r.active.lock().unwrap().as_ref() {
                Some(a) => a.epoch,
                None => return ("abandoned".into(), false),
            };
            if self.journal.committed(&r.key, epoch).is_none() {
                return ("journal fenced".into(), false);
            }
            // decision 4: release the lease after `release_after` without a journal majority
            if self.journal.members_up(&held.members) < 2 {
                let t = *no_journal_since.get_or_insert_with(Instant::now);
                if t.elapsed() >= Duration::from_millis(self.cfg.release_after_ms) {
                    return ("no journal majority".into(), true);
                }
            } else {
                no_journal_since = None;
            }
            if last_upload.elapsed() >= Duration::from_millis(self.cfg.upload_every_ms) {
                last_upload = Instant::now();
                if let Err(e) = self.upload(r, held).await {
                    return (e, false);
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
        let store_applied = r.active.lock().unwrap().as_ref().map_or(0, |a| a.store_applied);
        if upto > store_applied {
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
        }
        if let Some(a) = r.active.lock().unwrap().as_mut() {
            a.pending.clear();
            a.store_applied = upto;
        }
        self.journal.uploaded(&r.key, epoch, Index(upto)).await.map_err(|e| format!("journal: {e}"))?;
        let sent = self.clock.mono_ms();
        self.lease().reconfigure(db, held, want.clone(), sent).await.map_err(|e| format!("reconfigure: {e}"))?;
        self.hold_line(db, held.epoch, held.valid_until_mono);
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

#[cfg(test)]
mod clock_tests {
    use super::*;

    /// After forward jumps, a change of rate must not move the monotonic clock. Twin: the bug found by the §2a run
    /// (jumps counted twice at every rate change) makes it leap forward by the earlier jumps.
    fn leap(double_count: bool) -> f64 {
        let c = Clock::default();
        c.set_mono_jump_ms(3_000);
        let now = Instant::now();
        let mut g = c.0.lock().unwrap();
        let before = Clock::mono_of(&g, now);
        Clock::rebase(&mut g, now, 26_000, double_count);
        Clock::mono_of(&g, now) - before
    }

    #[test]
    fn rate_change_does_not_move_the_clock() {
        assert!(leap(false).abs() < 1e-6, "leap {}", leap(false));
        assert!((leap(true) - 3_000.0).abs() < 1e-6, "twin: the old code leaps by the earlier jumps ({})", leap(true));
    }
}
