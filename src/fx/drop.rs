//! The drop job (plan round 2, fixes 1 and 2; RULES-FOR-THE-CODE rules 7-12, 27; F6 and F7).
//!
//! SlateDB's own compactor runs it, with our scheduler and our compaction filter:
//! - **The fold** (rule 12, the drop on): every job folds the absorbed L0 SSTs (the oldest ones, I3) and SlateDB's one
//!   sorted run into that run; with no L0 SST waiting, a floor that moved up re-folds the run alone. The filter drops
//!   rows with seq at or below the floor; rows above it stay in the run. So SlateDB holds one sorted run, always.
//! - **The gated tiered compaction** (the drop off, the default): today's tiered scheduler (T = 4), except that an L0
//!   SST is compacted only once it is absorbed (otherwise absorption would find it gone). The filter keeps every row:
//!   SlateDB stays a full copy, which export, PITR and `--envelopes write` still read (the rollback window of plan
//!   section 8 has this shape too).
//!
//! The floor, computed inside the job when it starts (rule 7: from the versions in the store, never from a cut in
//! memory), is the smallest of:
//! - the X_d of the newest version this process has seen in the store for at least one lease (rule 8: from its first
//!   sight, on this process's clock; rule 9: a restart forgets first sights). For one serving lease plus one hold life
//!   after this process opened the database, the remote lease replaces the lease (rule 27 with F7's window), so the
//!   holds of an old waiter that was cut off rather than dead end before rows they need leave;
//! - the X_d of every live hold of this process (rule 4) and of the version the tenant serves (`Shared::serving`);
//! - nothing at all while a checkpoint lease is held in the store (rule 11: the floor is 0, the job keeps every row).
//! Drop jobs run only in the waiter that holds the database's holds (rule 10).

use super::compactor::{Compactor, Knobs, Shared};
use super::manifest;
use async_trait::async_trait;

#[allow(unused_imports)]
use object_store::{ObjectStore, ObjectStoreExt};
use slatedb::compactor::{CompactionScheduler, CompactionSchedulerSupplier, CompactionSpec, CompactorStateView, SourceId};
use slatedb::config::CompactorOptions;
use slatedb::{CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier, CompactionJobContext, RowEntry};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Negative control of FIX 1, tests only: the lease read as step 4 read it before the fix.
#[cfg(test)]
pub static CONTROL_LEASE_UNTIL_MS: AtomicBool = AtomicBool::new(false);
/// Negative control of FIX 4, tests only: every version listed and loaded.
#[cfg(test)]
pub static CONTROL_FULL_LOAD: AtomicBool = AtomicBool::new(false);

pub struct DropCtl {
    sh: Arc<Shared>,
    store: Arc<dyn ObjectStore>,
    pub drop: bool,
    lease: Duration,
    remote_lease: Duration,
    serving_lease: Duration,
    /// Versions seen in the store: first sight on this process's clock, and X_d (rules 8, 9).
    seen: Mutex<HashMap<u64, (Instant, u64)>>,
    /// The newest version listed so far (FIX 4: the next floor lists after it), and versions loaded (diagnostics).
    listed_upto: AtomicU64,
    pub versions_loaded: AtomicU64,
    /// L0 SSTs absorbed into a published version, with their highest seq.
    foldable: Mutex<HashMap<ulid::Ulid, u64>>,
    /// Aged folds: the highest seq each sorted run can hold (a run is dropped whole once the floor reaches it).
    sr_max: Mutex<HashMap<u32, u64>>,
    next_sr: AtomicU64,
    /// The newest X_d this process published (an upper bound for runs it does not know).
    pub known_xd: AtomicU64,
    /// One sorted run, always (rule 12 as written), or aged runs (`Knobs::fold_one_run` false).
    one_run: bool,
    max_runs: usize,
    fold_at_l0: usize,
    /// The floor the last job computed, and the floor last applied to the sorted run.
    pub last_floor: AtomicU64,
    pub applied_floor: AtomicU64,
    /// The floor the observer last computed (between jobs), and whether a re-fold of the run is wanted.
    observed: AtomicU64,
    refold: AtomicBool,
    observing: AtomicBool,
    last_observe: Mutex<Option<Instant>>,
    pub jobs: AtomicU64,
    pub jobs_done: AtomicU64,
    pub rows_dropped: AtomicU64,
    pub rows_kept: AtomicU64,
    tiered: crate::tiered::Tiered,
    /// Negative controls (tests): the floor ignores holds and the serving version; the lease counts from the publish stamp.
    pub control_ignore_holds: AtomicBool,
    pub control_lease_from_stamp: AtomicBool,
    pub control_no_takeover_window: AtomicBool,
}

impl DropCtl {
    pub fn new(sh: Arc<Shared>, store: Arc<dyn ObjectStore>, k: &Knobs) -> Arc<DropCtl> {
        Arc::new(DropCtl {
            sh,
            store,
            drop: k.drop,
            lease: k.lease,
            remote_lease: k.remote_lease,
            serving_lease: k.serving_lease,
            seen: Mutex::new(HashMap::new()),
            listed_upto: AtomicU64::new(0),
            versions_loaded: AtomicU64::new(0),
            foldable: Mutex::new(HashMap::new()),
            sr_max: Mutex::new(HashMap::new()),
            next_sr: AtomicU64::new(0),
            known_xd: AtomicU64::new(0),
            one_run: k.fold_one_run,
            max_runs: k.fold_max_runs,
            fold_at_l0: k.fold_at_l0,
            last_floor: AtomicU64::new(0),
            applied_floor: AtomicU64::new(0),
            observed: AtomicU64::new(0),
            refold: AtomicBool::new(false),
            observing: AtomicBool::new(false),
            last_observe: Mutex::new(None),
            jobs: AtomicU64::new(0),
            jobs_done: AtomicU64::new(0),
            rows_dropped: AtomicU64::new(0),
            rows_kept: AtomicU64::new(0),
            tiered: crate::tiered::Tiered::new(4.0),
            control_ignore_holds: AtomicBool::new(false),
            control_lease_from_stamp: AtomicBool::new(false),
            control_no_takeover_window: AtomicBool::new(false),
        })
    }

    pub fn set_foldable(&self, absorbed: &HashMap<ulid::Ulid, u64>) {
        let mut f = self.foldable.lock().unwrap();
        f.extend(absorbed.iter().map(|(k, v)| (*k, *v)));
        if let Some(m) = absorbed.values().max() {
            self.known_xd.fetch_max(*m, Relaxed);
        }
    }

    /// The lease in force now: the remote lease during the takeover window (rule 27, F7).
    pub fn lease_now(&self) -> Duration {
        let window = self.serving_lease + super::hold::hold_life();
        if self.sh.opened.elapsed() < window && !self.control_no_takeover_window.load(Relaxed) {
            self.remote_lease
        } else {
            self.lease
        }
    }

    /// Rule 11: while a checkpoint holds the drop lease (step 7a's `pair.rs`, the one lease format) the floor is 0. An
    /// expired lease, or one that cannot be read as a lease, is not held (review of step 4, FIX 1).
    async fn checkpoint_lease_held(&self) -> anyhow::Result<bool> {
        #[cfg(test)]
        if CONTROL_LEASE_UNTIL_MS.load(Relaxed) {
            // the step 4 code before the fix: `until_ms`, missing means held forever
            if let Some((_, _)) = super::pair::read_lease(&self.store, &self.sh.id).await? {
                let b = self.store.get(&super::pair::lease_path(&self.sh.id)).await?.bytes().await?;
                let v: serde_json::Value = serde_json::from_slice(&b).unwrap_or_default();
                let until = v.get("until_ms").and_then(|x| x.as_u64()).unwrap_or(u64::MAX);
                return Ok(manifest::now_ms() < until.saturating_add(2000));
            }
            return Ok(false);
        }
        super::pair::lease_held(&self.store, &self.sh.id).await
    }

    /// The floor (see the module comment). 0 with the drop off.
    pub async fn floor(&self) -> anyhow::Result<u64> {
        if !self.drop {
            return Ok(0);
        }
        if self.checkpoint_lease_held().await? {
            return Ok(0);
        }
        // FIX 4 of the review: list only the versions after the newest one already listed (one page), starting after
        // restart from the version the tenant knows, and load only the newest of each listing. X_d never goes down from
        // one version to the next, and the versions of one listing share their first sight, so the newest of them is the
        // only one the floor can use (rule 8 per version, unchanged).
        let after = match self.listed_upto.load(Relaxed) {
            0 => {
                let known = [self.sh.pending_version(), Some(self.sh.serving_version.load(Relaxed)).filter(|v| *v != u64::MAX)].into_iter().flatten().min();
                known.map_or(0, |k| k.saturating_sub(1))
            }
            n => n,
        };
        #[cfg(test)]
        if CONTROL_FULL_LOAD.load(Relaxed) {
            // the code before FIX 4: list every version and load every one not seen yet
            for n in manifest::list_versions(&self.store, &self.sh.id).await? {
                if !self.seen.lock().unwrap().contains_key(&n) {
                    if let Ok(v) = manifest::load(&self.store, &self.sh.id, n).await {
                        self.seen.lock().unwrap().insert(n, (Instant::now(), v.x_d));
                        self.versions_loaded.fetch_add(1, Relaxed);
                    }
                }
            }
        }
        let fresh = manifest::list_versions_after(&self.store, &self.sh.id, after).await?;
        let now = Instant::now();
        let mut stamps: HashMap<u64, u64> = HashMap::new();
        for &n in fresh.iter().rev() {
            match manifest::load(&self.store, &self.sh.id, n).await {
                Ok(v) => {
                    stamps.insert(n, v.published_ms);
                    self.seen.lock().unwrap().insert(n, (now, v.x_d));
                    self.versions_loaded.fetch_add(1, Relaxed);
                    self.listed_upto.fetch_max(n, Relaxed);
                    break;
                }
                Err(e) if e.downcast_ref::<object_store::Error>().is_some_and(|e| matches!(e, object_store::Error::NotFound { .. })) => continue, // GC took it
                Err(e) => return Err(e),
            }
        }
        let mut lease = self.lease_now();
        if super::compactor::REVIEW_PLANT.load(Relaxed) == 3 {
            lease = Duration::ZERO; // planted bug 3
        }
        let mut f = {
            let seen = self.seen.lock().unwrap();
            if self.control_lease_from_stamp.load(Relaxed) {
                // negative control of rule 8: the lease counted from the publish stamp, written before the PUT
                let wall = manifest::now_ms();
                seen.iter().filter(|(n, _)| stamps.get(n).is_none_or(|s| wall.saturating_sub(*s) >= lease.as_millis() as u64)).map(|(_, (_, x))| *x).max().unwrap_or(0)
            } else {
                seen.values().filter(|(t, _)| t.elapsed() >= lease).map(|(_, x)| *x).max().unwrap_or(0)
            }
        };
        {
            // keep the newest entry the floor can use and every younger one (the memory stays bounded)
            let mut seen = self.seen.lock().unwrap();
            if let Some(best) = seen.iter().filter(|(_, (t, _))| t.elapsed() >= lease).map(|(n, _)| *n).max() {
                seen.retain(|n, _| *n >= best);
            }
        }
        if !self.control_ignore_holds.load(Relaxed) && super::compactor::REVIEW_PLANT.load(Relaxed) != 4 {
            if let Some(h) = self.sh.holds.floor() {
                f = f.min(h);
            }
            let s = self.sh.serving.load(Relaxed);
            if s != u64::MAX {
                f = f.min(s);
            }
        }
        Ok(f)
    }

    /// Between jobs: compute the floor now and then (this also records first sights early), and ask for a re-fold of the
    /// sorted run when the floor passed the one last applied to it.
    pub fn maybe_refold(self: &Arc<Self>) {
        if !self.drop {
            return;
        }
        {
            let mut lo = self.last_observe.lock().unwrap();
            let every = (self.lease / 8).clamp(Duration::from_millis(50), Duration::from_secs(5));
            if lo.is_some_and(|t| t.elapsed() < every) || self.observing.swap(true, Relaxed) {
                return;
            }
            *lo = Some(Instant::now());
        }
        let me = Arc::clone(self);
        tokio::spawn(async move {
            if let Ok(f) = me.floor().await {
                me.observed.store(f, Relaxed);
                if f > me.applied_floor.load(Relaxed) {
                    me.refold.store(true, Relaxed);
                }
            }
            me.observing.store(false, Relaxed);
        });
    }

    /// Ask for a re-fold now (tests).
    pub fn request_refold(&self) {
        self.refold.store(true, Relaxed);
    }

    /// Aged folds (the default; measured against rule 12's one run in this step's report): every fold writes its absorbed
    /// L0 SSTs into a new sorted run, so a row is written once on SlateDB's side; a run whose highest seq is at or below
    /// the floor is dropped whole (a job over the oldest such runs keeps nothing); at most `max_runs` runs (the two
    /// newest merge past it). One run, always, rewrites the run at every fold, and the run holds about one lease of
    /// writes: its cost grows with write rate times lease (MEASURED in the replay).
    fn propose_aged(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        let m = state.manifest();
        let tail = self.tail(m.l0());
        let srs: Vec<u32> = m.compacted().iter().map(|r| r.id).collect(); // newest first
        let max_id = srs.iter().copied().max().map_or(0, |x| x as u64 + 1);
        self.next_sr.fetch_max(max_id, Relaxed);
        let mut sr_max = self.sr_max.lock().unwrap();
        let known = self.known_xd.load(Relaxed);
        for id in &srs {
            sr_max.entry(*id).or_insert(known); // a run from before (or after a restart): at most every absorbed row
        }
        sr_max.retain(|id, _| srs.contains(id));
        if srs.len() > self.max_runs {
            // past the cap, the adjacent pair with the smallest combined size merges, before any fold (the cap bounds what a
            // cold scan pays: about one GET per run, fix round of the review); tiered, so no run is rewritten at every
            // fold; the oldest can be large while the floor lags, and it leaves whole once the floor passes it
            let sizes: Vec<u64> = m.compacted().iter().map(|r| r.estimate_size()).collect();
            let i = (0..srs.len() - 1).min_by_key(|&i| sizes[i] + sizes[i + 1]).unwrap();
            let (a, b) = (srs[i], srs[i + 1]);
            let hi = sr_max.get(&a).copied().unwrap_or(known).max(sr_max.get(&b).copied().unwrap_or(known));
            sr_max.insert(a.min(b), hi);
            return vec![CompactionSpec::new(vec![SourceId::SortedRun(a), SourceId::SortedRun(b)], a.min(b))];
        }
        // lazy fold: absorbed L0 SSTs whose rows the floor has passed are folded with nothing written; younger ones wait,
        // unless L0 reached `fold_at_l0` (then they go into a new run, one more write of those rows)
        let floor_now = self.observed.load(Relaxed);
        let tail: Vec<ulid::Ulid> = {
            let f = self.foldable.lock().unwrap();
            let below: Vec<ulid::Ulid> = tail.iter().rev().take_while(|id| f.get(id).is_some_and(|x| *x <= floor_now)).copied().collect();
            if m.l0().len() >= self.fold_at_l0 || below.len() == tail.len() {
                tail
            } else {
                let mut b = below;
                b.reverse();
                b
            }
        };
        if !tail.is_empty() {
            let f = self.foldable.lock().unwrap();
            let hi = tail.iter().filter_map(|id| f.get(id)).copied().max().unwrap_or(known);
            let dst = self.next_sr.fetch_add(1, Relaxed) as u32;
            sr_max.insert(dst, hi);
            return vec![CompactionSpec::new(tail.iter().map(|id| SourceId::SstView(*id)).collect(), dst)];
        }
        // the oldest runs wholly at or below the floor: dropped (the job keeps only the envelope marker); a run already
        // emptied this way (its highest seq set to 0) is not proposed again on its own
        let floor = self.observed.load(Relaxed);
        let below: Vec<u32> = srs.iter().rev().take_while(|id| sr_max.get(id).is_some_and(|x| *x <= floor)).copied().collect();
        if below.len() > 1 || below.first().is_some_and(|id| sr_max.get(id).is_some_and(|x| *x > 0)) {
            let dst = *below.iter().min().unwrap();
            let src: Vec<SourceId> = srs.iter().filter(|id| below.contains(id)).map(|id| SourceId::SortedRun(*id)).collect();
            sr_max.insert(dst, 0);
            return vec![CompactionSpec::new(src, dst)];
        }
        vec![]
    }

    fn propose_fold(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        if !self.one_run {
            return self.propose_aged(state);
        }
        let m = state.manifest();
        let tail = self.tail(m.l0());
        let srs: Vec<u32> = m.compacted().iter().map(|r| r.id).collect(); // newest first
        if !tail.is_empty() {
            let mut src: Vec<SourceId> = tail.iter().map(|id| SourceId::SstView(*id)).collect();
            return match srs.first() {
                Some(&newest) => {
                    src.push(SourceId::SortedRun(newest));
                    vec![CompactionSpec::new(src, newest)]
                }
                None => vec![CompactionSpec::new(src, 0)],
            };
        }
        if srs.len() > 1 {
            // runs left by today's compaction (before the drop was on): one run, always (rule 12)
            let dst = *srs.iter().min().unwrap();
            return vec![CompactionSpec::new(srs.iter().map(|id| SourceId::SortedRun(*id)).collect(), dst)];
        }
        if srs.len() == 1 && self.refold.swap(false, Relaxed) {
            return vec![CompactionSpec::new(vec![SourceId::SortedRun(srs[0])], srs[0])];
        }
        vec![]
    }

    fn propose_gated(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        let m = state.manifest();
        let tail = self.tail(m.l0());
        let mut next_sr = m.compacted().iter().map(|r| r.id + 1).max().unwrap_or(0);
        let mut out = Vec::new();
        if !tail.is_empty() && (tail.len() >= self.tiered.min_l0 || m.l0().len() >= 6) {
            out.push(CompactionSpec::new(tail.iter().map(|id| SourceId::SstView(*id)).collect(), next_sr));
            next_sr += 1;
        }
        let _ = next_sr;
        // sorted runs, newest to oldest: a run merges into the next older one once it is 1/T of its size (tiered.rs)
        let srs: Vec<(u32, u64)> = m.compacted().iter().map(|r| (r.id, r.estimate_size())).collect();
        for i in 0..srs.len().saturating_sub(1) {
            let (a, b) = (srs[i], srs[i + 1]);
            if (a.1 as f64) * self.tiered.t >= b.1 as f64 {
                out.push(CompactionSpec::new(vec![SourceId::SortedRun(a.0), SourceId::SortedRun(b.0)], a.0.min(b.0)));
                break;
            }
        }
        out
    }

    /// The oldest L0 SSTs that are absorbed, with no gap, newest first (read order).
    fn tail(&self, l0: &std::collections::VecDeque<slatedb::manifest::SsTableView>) -> Vec<ulid::Ulid> {
        let f = self.foldable.lock().unwrap();
        let mut t: Vec<ulid::Ulid> = l0.iter().rev().take_while(|v| f.contains_key(&v.id)).map(|v| v.id).collect();
        t.reverse();
        t
    }
}

#[derive(Clone)]
pub struct Sched(pub Arc<DropCtl>);

impl CompactionSchedulerSupplier for Sched {
    fn compaction_scheduler(&self, _o: &CompactorOptions) -> Box<dyn CompactionScheduler + Send + Sync> {
        Box::new(self.clone())
    }
}

impl CompactionScheduler for Sched {
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        if let Some(c) = state.compactions() {
            if c.recent_compactions().any(|c| c.active()) {
                return vec![];
            }
        }
        let out = if self.0.drop { self.0.propose_fold(state) } else { self.0.propose_gated(state) };
        self.0.jobs.fetch_add(out.len() as u64, Relaxed);
        out
    }

    fn validate(&self, state: &CompactorStateView, spec: &CompactionSpec) -> Result<(), slatedb::Error> {
        let m = state.manifest();
        let order = crate::tiered::logical_order(m.l0(), m.compacted());
        crate::tiered::check(Some(&order), spec).map_err(slatedb::Error::invalid)?;
        // only absorbed L0 SSTs may leave L0 (absorption reads them)
        let f = self.0.foldable.lock().unwrap();
        for s in spec.sources() {
            if let SourceId::SstView(id) = s {
                if !f.contains_key(id) {
                    return Err(slatedb::Error::invalid(format!("L0 SST {id} is not absorbed")));
                }
            }
        }
        Ok(())
    }
}

/// Review of step 4 only: hold every drop job at its first row (its floor is computed) while set.
pub static REVIEW_HOLD_JOB: AtomicBool = AtomicBool::new(false);
pub static REVIEW_JOB_HELD: AtomicBool = AtomicBool::new(false);

struct Filter {
    floor: u64,
    ctl: Arc<DropCtl>,
    dropped: u64,
    kept: u64,
}

#[async_trait]
impl CompactionFilter for Filter {
    async fn filter(&mut self, entry: &RowEntry) -> Result<CompactionFilterDecision, CompactionFilterError> {
        if self.dropped + self.kept == 0 && REVIEW_HOLD_JOB.load(Relaxed) {
            REVIEW_JOB_HELD.store(true, Relaxed);
            while REVIEW_HOLD_JOB.load(Relaxed) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        // the envelope marker stays in SlateDB whatever its seq: every open reads E_env from it there (`load_env`); a drop
        // of it made the next open fail ("our manifest has E_env, the database's marker None": MEASURED by the model test)
        if entry.key.as_ref() == crate::envelope::MARKER {
            self.kept += 1;
            return Ok(CompactionFilterDecision::Keep);
        }
        Ok(if entry.seq <= self.floor {
            self.dropped += 1;
            CompactionFilterDecision::Drop
        } else {
            self.kept += 1;
            CompactionFilterDecision::Keep
        })
    }

    async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
        self.ctl.rows_dropped.fetch_add(self.dropped, Relaxed);
        self.ctl.rows_kept.fetch_add(self.kept, Relaxed);
        self.ctl.jobs_done.fetch_add(1, Relaxed);
        self.ctl.applied_floor.fetch_max(self.floor, Relaxed);
        Ok(())
    }
}

#[async_trait]
impl CompactionFilterSupplier for Sched {
    async fn create_compaction_filter(&self, _c: &CompactionJobContext) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
        // rule 7: the floor is computed here, when the job starts, from the store
        let floor = match self.0.floor().await {
            Ok(f) => f,
            Err(e) => return Err(CompactionFilterError::CreationError(format!("drop floor: {e:#}").into())),
        };
        self.0.last_floor.store(floor, Relaxed);
        Ok(Box::new(Filter { floor, ctl: Arc::clone(&self.0), dropped: 0, kept: 0 }))
    }
}

/// Start SlateDB's compactor and a worker on the database, with our scheduler and filter.
pub(crate) async fn start_slate(c: &Arc<Compactor>) -> anyhow::Result<()> {
    let id = c.id().to_string();
    let sched = Sched(Arc::clone(&c.drop));
    let mut opts = CompactorOptions::default();
    opts.poll_interval = Duration::from_millis(20);
    opts.worker = None;
    let path = crate::lifecycle::tenant_path(&id);
    let store = crate::counting::owned_background(&c.store, &id);
    // perf-latency: SlateDB's compactor and worker run their jobs on the runtime given here (default: the one `build` runs
    // on, which is the server's, inside the first write after an opening): the compactor's write-path runtime
    // instead (`pool::write_path_for`: the fold removes absorbed L0 SSTs, which the writes wait on)
    let cb = slatedb::CompactorBuilder::new(path.clone(), Arc::clone(&store)).with_options(opts).with_scheduler_supplier(Arc::new(sched.clone())).with_runtime(c.write_path.clone());
    let cb = match crate::lifecycle::filter_policies() {
        Some(p) => cb.with_filter_policies(p),
        None => cb,
    };
    let compactor = Arc::new(cb.build());
    let mut wo = slatedb::config::CompactionWorkerOptions::default();
    wo.min_filter_keys = crate::lifecycle::MIN_FILTER_KEYS;
    wo.compactions_poll_interval = Duration::from_millis(10);
    wo.compression_codec = crate::tiered::codec();
    let wb = slatedb::CompactionWorkerBuilder::<object_store::path::Path>::new(path.into(), store).with_options(wo).with_sst_block_size(crate::tiered::block_size()).with_compaction_filter_supplier(Arc::new(sched)).with_runtime(c.write_path.clone());
    let wb = match crate::lifecycle::filter_policies() {
        Some(f) => wb.with_filter_policies(f),
        None => wb,
    };
    let worker = Arc::new(wb.build().await?);
    let (cc, ww) = (Arc::clone(&compactor), Arc::clone(&worker));
    // SlateDB's compactor and worker register their event loops when `run` starts, on the runtime that polls it: spawned
    // here, on the caller's runtime, as before. A `stop` that came before the registration (a close right after the start)
    // found nothing to stop and the loop then ran on, fencing the next compactor of the database ("detected newer DB
    // client"; an erasure waited 30 s for the maintenance lock, ctl erase loop, perf-latency). The CPU of the fold is in
    // the worker's compaction jobs, which run on the runtime given to `with_runtime` above (the write-path one).
    let h = tokio::spawn(async move {
        let (a, b) = tokio::join!(cc.run(), ww.run());
        if let Err(e) = a.and(b) {
            eprintln!("fx drop: SlateDB compactor of {id}: {e:#}");
        }
    });
    c.set_slate(h, compactor, worker);
    Ok(())
}
