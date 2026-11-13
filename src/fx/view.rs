//! An opened version of our manifest: the runs it lists, opened on first use, and the read rule over them.
//!
//! The read rule for a key k of group g (plan section 2; RULES-FOR-THE-CODE rule 2): the answer is the row with the
//! highest seq among SlateDB rows with X_d < seq <= S, delta rows of g with X_g < seq <= X_d, and g's sealed run (seq
//! <= X_g). This file has the two run tiers; `read.rs` adds SlateDB on top. A winning delete envelope means absent.

use super::manifest::{unhex, GroupLine, RunLine, Version};
use crate::runfmt::{self, Run, RUN_ID_LEN};
use anyhow::{ensure, Result};
use bytes::Bytes;
use object_store::path::Path;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

/// Bytes of decoded run blocks kept in RAM, shared by every database of the process (`--sealed-cache-mb`).
pub static CACHE_BYTES: AtomicU64 = AtomicU64::new(256 << 20);
/// Counters (diagnostics and tests).
pub static RUN_GETS: AtomicU64 = AtomicU64::new(0);

// ---- review of big-speed (2026-11-13, error 1): the customer's box, capped per customer and per process

/// Stored bytes one customer's box warm-up may fetch, and decoded bytes it may keep (0: no cap). Set from the memory
/// budget (`membudget::Budget::box_customer`, the runs' cache / 8) or `--customer-box-max-mb`.
pub static BOX_CUSTOMER_BYTES: AtomicU64 = AtomicU64::new(32 << 20);
/// Stored bytes of one wave (one range GET of one run).
pub static BOX_WAVE_BYTES: AtomicU64 = AtomicU64::new(4 << 20);
/// Stored bytes every box warm-up of the process has in flight together (`membudget::Budget::box_inflight`, or
/// `--customer-box-inflight-mb`); read once, when the first warm-up starts.
pub static BOX_INFLIGHT_BYTES: AtomicU64 = AtomicU64::new(64 << 20);
/// Waves of one warm-up at once.
pub const BOX_PARALLEL: usize = 16;
/// Warm-ups skipped because the box was above the customer's allowance; warm-ups stopped at the allowance; waves sent;
/// waves that waited for the process's allowance.
pub static BOX_TOO_BIG: AtomicU64 = AtomicU64::new(0);
pub static BOX_CAPPED: AtomicU64 = AtomicU64::new(0);
pub static BOX_WAVES: AtomicU64 = AtomicU64::new(0);
pub static BOX_PERMIT_WAITS: AtomicU64 = AtomicU64::new(0);

static BOX_SEM: std::sync::OnceLock<(tokio::sync::Semaphore, u32)> = std::sync::OnceLock::new();

/// A permit of the process-wide box allowance for `bytes` stored bytes (KiB units; a wave larger than the whole
/// allowance takes all of it).
pub async fn box_permit(bytes: u64) -> tokio::sync::SemaphorePermit<'static> {
    let (sem, total) = BOX_SEM.get_or_init(|| {
        let kib = (BOX_INFLIGHT_BYTES.load(Relaxed) >> 10).clamp(1, u32::MAX as u64 >> 4) as u32;
        (tokio::sync::Semaphore::new(kib as usize), kib)
    });
    let want = (bytes.div_ceil(1024) as u32).clamp(1, *total);
    if sem.available_permits() < want as usize {
        BOX_PERMIT_WAITS.fetch_add(1, Relaxed);
    }
    sem.acquire_many(want).await.expect("the box semaphore is never closed")
}

/// (permits free, permits in all) of the process's box allowance, in KiB.
pub fn box_allowance() -> (usize, u32) {
    BOX_SEM.get().map_or((0, 0), |(s, t)| (s.available_permits(), *t))
}
pub static RUN_OPENS: AtomicU64 = AtomicU64::new(0);
pub static BLOCK_HITS: AtomicU64 = AtomicU64::new(0);
pub static ROWS_FROM_RUNS: AtomicU64 = AtomicU64::new(0);
/// Step 4, R1: block reads served by a run's copy on the waiter's NVMe (no GET), and runs opened from it.
pub static LOCAL_READS: AtomicU64 = AtomicU64::new(0);
pub static LOCAL_OPENS: AtomicU64 = AtomicU64::new(0);
/// Step 5: the "one wave" reader of the plan (section 2), off by default. A point read asks every run that may hold the
/// key at once (the delta runs above the group's cut and the sealed run) instead of one after the other, and a range read
/// fetches the first block of every run of a group at once; `read.rs` also starts SlateDB's read beside ours. Fewer trips,
/// sometimes more GETs (the sealed run is read even when a delta run answers).
pub static WAVE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
/// Step 5: a range read inside one customer skips the runs whose customer filter says the customer is not there, as a
/// point read does (on by default; a filter has no false negatives, so the answer cannot change). Step 3's cursor read a
/// block of every delta run above the group's cut, whoever wrote it. Off: the reader as step 3 built it.
pub static RANGE_FILTER: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);
/// Runs a range read skipped by their customer filter (diagnostics).
pub static RANGE_FILTER_SKIPS: AtomicU64 = AtomicU64::new(0);

/// Step 4, R3: GETs that point reads paid in delta runs since a group's last reseal, by group id. The compactor reseals a
/// group early once these GETs cost more than the reseal (`compactor.rs`).
#[derive(Default)]
pub struct R3Gets(pub Mutex<HashMap<u32, u64>>);

impl R3Gets {
    pub fn note(&self, group: u32) {
        *self.0.lock().unwrap().entry(group).or_default() += 1;
    }
    pub fn get(&self, group: u32) -> u64 {
        self.0.lock().unwrap().get(&group).copied().unwrap_or(0)
    }
    pub fn reset(&self, group: u32) {
        self.0.lock().unwrap().remove(&group);
    }
}

/// Negative control of rule 2 [ReadNoCut], tests only: delta rows count whatever their seq (no X_g bound).
#[cfg(test)]
pub static CONTROL_NO_CUT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Negative control, tests only: the delta tier is not read.
#[cfg(test)]
pub static CONTROL_SKIP_DELTA: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[inline]
fn skip_delta() -> bool {
    #[cfg(test)]
    if CONTROL_SKIP_DELTA.load(Relaxed) {
        return true;
    }
    false
}

#[inline]
fn above_cut(seq: u64, x_g: u64) -> bool {
    #[cfg(test)]
    if CONTROL_NO_CUT.load(Relaxed) {
        return true;
    }
    seq > x_g
}

/// Drop every cached block and slice (tests that count GETs).
pub fn clear_cache() {
    super::counter::clear();
}

/// One run of a version: its manifest line, and the opened run (footer, fence, filter) once a read needed it.
pub struct RunH {
    pub line: RunLine,
    pub id: [u8; RUN_ID_LEN],
    pub first: Vec<u8>,
    pub last: Vec<u8>,
    run: tokio::sync::OnceCell<Arc<Run>>,
    /// R1: the store holding the run's NVMe copy, when the line says it has one and this process has an NVMe store.
    local: Option<Arc<dyn ObjectStore>>,
    /// Whether the run was opened from the NVMe copy (then every block read goes there).
    from_local: std::sync::atomic::AtomicBool,
}

impl RunH {
    pub fn new(line: RunLine) -> Result<RunH> {
        let id: [u8; RUN_ID_LEN] = unhex(&line.id)?.try_into().map_err(|_| anyhow::anyhow!("run id of {} is not 16 bytes", line.path))?;
        let local = if line.local { super::local::store() } else { None };
        Ok(RunH { first: unhex(&line.first)?, last: unhex(&line.last)?, id, line, run: tokio::sync::OnceCell::new(), local, from_local: Default::default() })
    }

    /// The store block reads of this run go to: the NVMe copy once the run was opened from it, else `store`.
    pub fn read_store<'a>(&'a self, store: &'a Arc<dyn ObjectStore>) -> &'a Arc<dyn ObjectStore> {
        match &self.local {
            Some(l) if self.from_local.load(Relaxed) => l,
            _ => store,
        }
    }

    /// The run, opened on first use with the id the manifest recorded (another object at the path is an error).
    pub async fn open(&self, store: &Arc<dyn ObjectStore>) -> Result<&Arc<Run>> {
        self.run
            .get_or_try_init(|| async {
                let path = Path::from(self.line.path.as_str());
                let mut opened = None;
                if let Some(l) = &self.local {
                    // R1: the NVMe copy, same path and the same run id; a missing or damaged copy falls back to the store
                    if let Ok((run, _)) = Run::open_expecting(l, path.clone(), self.line.size, &self.id).await {
                        self.from_local.store(true, Relaxed);
                        LOCAL_OPENS.fetch_add(1, Relaxed);
                        opened = Some(run);
                    }
                }
                let run = match opened {
                    Some(r) => r,
                    None => Run::open_expecting(store, path, self.line.size, &self.id).await?.0,
                };
                ensure!(run.min_seq == self.line.min_seq && run.max_seq == self.line.max_seq && run.entries == self.line.entries, "run {} differs from its manifest line", self.line.path);
                RUN_OPENS.fetch_add(1, Relaxed);
                Ok(Arc::new(run))
            })
            .await
    }

    fn holds_key(&self, k: &[u8]) -> bool {
        &self.first[..] <= k && k <= &self.last[..]
    }

    /// Whether the run certainly holds no row in [lo, hi): both ends are keys of one customer and the run's customer filter
    /// says that customer is not in the run. Only for a run already opened (its filter in RAM); an unopened run is kept.
    fn filter_excludes(&self, lo: &[u8], hi: Option<&[u8]>) -> bool {
        let (Some(run), Some(hi)) = (self.run.get(), hi) else { return false };
        if run.customers == runfmt::CustomerKind::V2 {
            // step 5b: also a whole customer, [prefix, successor of the prefix)
            return super::seqfilter::one_customer(lo, (hi, false)).is_some_and(|c| !run.filter.may_contain(c));
        }
        let (Some(a), Some(b)) = (run.customers.customer_of(lo), run.customers.customer_of(hi)) else { return false };
        if lo[a.clone()] != hi[b] {
            return false;
        }
        !run.filter.may_contain(&lo[a])
    }

    /// Whether [lo, hi) meets the run's key range.
    fn meets(&self, lo: &[u8], hi: Option<&[u8]>) -> bool {
        hi.map_or(true, |h| &self.first[..] < h) && &self.last[..] >= lo
    }
}

pub(crate) fn is_local(store: &Arc<dyn ObjectStore>) -> bool {
    super::local::store().is_some_and(|l| Arc::ptr_eq(&l, store))
}

/// Blocks first..=last of a run through the counter (step 6, `counter::fetch`), from the run's NVMe copy when it was
/// opened there (step 4), falling back to the store when the copy is gone or damaged (it is a cache: GC may remove it).
async fn fetch_h<'n>(primary: &Arc<dyn ObjectStore>, h: &RunH, run: &Run, first: usize, last: usize, prefix: Option<&[u8]>, need: &(dyn Fn(usize) -> super::counter::Need<'n> + Sync)) -> Result<Vec<Arc<super::counter::Piece>>> {
    let s = h.read_store(primary);
    if Arc::ptr_eq(s, primary) {
        return super::counter::fetch(primary, run, first, last, prefix, need).await;
    }
    match super::counter::fetch(s, run, first, last, prefix, need).await {
        Ok(b) => Ok(b),
        Err(_) => {
            h.from_local.store(false, Relaxed);
            super::counter::fetch(primary, run, first, last, prefix, need).await
        }
    }
}

/// Whole blocks first..=last of a run of `h` (its NVMe copy when it has one), through the counter, for the
/// derived-document rebuild (`derive.rs`). Integration-3: step 6's counter replaced the block cache this used; a
/// thread's prefetch (`prefetch_thread`) keeps the same whole blocks there, so the rebuild still needs no second trip.
pub(super) async fn fetch_blocks(store: &Arc<dyn ObjectStore>, h: &RunH, run: &Run, first: usize, last: usize) -> Result<Vec<Arc<super::counter::Piece>>> {
    fetch_h(store, h, run, first, last, None, &|_| super::counter::Need::Key(&[])).await
}

/// The number of blocks of a run, and blocks first..=last of it (the erasure's rewrite reads a run whole, a chunk of
/// blocks per range GET, `erase.rs`).
pub async fn block_count(store: &Arc<dyn ObjectStore>, h: &RunH) -> Result<usize> {
    Ok(h.open(store).await?.fence.blocks())
}

pub async fn blocks(store: &Arc<dyn ObjectStore>, h: &RunH, first: usize, last: usize) -> Result<Vec<Arc<runfmt::Block>>> {
    // integration-3: one range GET, decoded and checked, not kept in the counter (step 6 replaced the block cache this
    // used): a scrub reads whole runs once
    let run = h.open(store).await?;
    let (lo, hi) = run.fence.range(first, last);
    let data = runfmt::get_range(store, &run.path, lo, hi).await?;
    RUN_GETS.fetch_add(1, Relaxed);
    (first..=last)
        .map(|b| {
            let (x, y) = run.fence.range(b, b);
            Ok(Arc::new(runfmt::decode_checked(run, b, &data[(x - lo) as usize..(y - lo) as usize])?))
        })
        .collect()
}

/// A row from our runs: seq and envelope (first byte 0 live, 1 deleted). A point read leaves `key` empty (the caller
/// knows it).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RRow {
    pub key: Vec<u8>,
    pub seq: u64,
    pub env: Vec<u8>,
}

impl RRow {
    pub fn is_dead(&self) -> bool {
        self.env.first() == Some(&runfmt::DELETED)
    }
    pub fn doc(&self) -> Bytes {
        Bytes::copy_from_slice(&self.env[1..])
    }
    /// The live document, without a copy.
    pub fn into_doc(self) -> Bytes {
        Bytes::from(self.env).slice(1..)
    }
}

/// The row of `key` in one run, if it holds one.
async fn run_get(store: &Arc<dyn ObjectStore>, h: &RunH, key: &[u8]) -> Result<Option<RRow>> {
    Ok(run_get_counted(store, h, key).await?.0)
}

/// [`run_get`], and whether it paid a GET in the store (not the cache, not the NVMe copy): R3's count.
async fn run_get_counted(store: &Arc<dyn ObjectStore>, h: &RunH, key: &[u8]) -> Result<(Option<RRow>, bool)> {
    if !h.holds_key(key) {
        return Ok((None, false));
    }
    let run = h.open(store).await?;
    let primary = store;
    let store = h.read_store(store);
    if let Some(r) = run.customers.customer_of(key) {
        // the new-user filter: a customer the run has no row of costs no GET
        super::counter::FILTER_CHECKS.fetch_add(1, Relaxed);
        if !run.filter.may_contain(&key[r]) {
            super::counter::FILTER_SKIPS.fetch_add(1, Relaxed);
            return Ok((None, false));
        }
    }
    let i = run.fence.count_below(key, true);
    if i == 0 {
        return Ok((None, false));
    }
    // a point read needs one block (or its slice of the key's area, when the slice covers the key)
    let p = super::counter::point_prefix(run.customers, key, super::counter::slice_mode());
    let need = |_: usize| super::counter::Need::Key(key);
    // R3's count: a GET in the store, not the counter and not the NVMe copy
    let paid = !is_local(store) && !super::counter::cached(run, i - 1, p.as_deref(), &need(i - 1));
    let piece = fetch_h(primary, h, run, i - 1, i - 1, p.as_deref(), &need).await?.pop().unwrap();
    piece.rely(key, &super::counter::succ(key));
    let b = &piece.blk;
    let j = b.lower_bound(key);
    Ok(((j < b.len() && b.key(j) == key).then(|| RRow { key: Vec::new(), seq: b.seq(j), env: b.env(j).to_vec() }), paid))
}

/// Derived documents (`derive.rs`): before a point read of a checkpoint document in a run with derived rows, fetch the
/// thread's message blocks with the document's block in one GET (into the block cache), so the rebuild needs no second
/// trip. Answers never depend on it.
async fn prefetch_thread(store: &Arc<dyn ObjectStore>, h: &RunH, key: &[u8]) -> Result<()> {
    if !h.holds_key(key) {
        return Ok(());
    }
    let run = h.open(store).await?.clone();
    if let Some((a, z)) = super::derive::prefetch_span(&run, key) {
        fetch_h(store, h, &run, a, z, None, &|_| super::counter::Need::Key(&[])).await?;
    }
    Ok(())
}

/// The point read of a group's sealed run, shared by both point-read paths (`View::get` and the one-wave
/// `View::get_wave`, review of c2-delta-log FIX 1): the thread's message blocks prefetched with the document's block,
/// then a derived document rebuilt and a message row never returned. Every point read of a sealed run goes here.
async fn sealed_point(store: &Arc<dyn ObjectStore>, s: &RunH, key: &[u8]) -> Result<Option<RRow>> {
    prefetch_thread(store, s, key).await?;
    match run_get(store, s, key).await? {
        Some(r) if super::derive::is_derived(&r.env) => derived_row(store, s, key, r).await,
        r => Ok(r),
    }
}

/// A sealed row that is a derived document (rebuilt, as a live row) or a message row (None: never a document). The one
/// place where the reader meets `derive.rs`.
async fn derived_row(store: &Arc<dyn ObjectStore>, h: &RunH, key: &[u8], r: RRow) -> Result<Option<RRow>> {
    if r.env.first() == Some(&runfmt::PART) {
        return Ok(None);
    }
    let env = super::derive::rebuild(store, h, key, &r.env).await?;
    Ok(Some(RRow { key: r.key, seq: r.seq, env }))
}

pub struct Group {
    pub id: u32,
    pub lo: Vec<u8>,
    pub x_g: u64,
    pub sealed: Option<Arc<RunH>>,
}

/// An opened version.
pub struct View {
    pub store: Arc<dyn ObjectStore>,
    pub version: u64,
    pub x_d: u64,
    pub e_env: Option<u64>,
    pub groups: Vec<Group>,
    /// Newest first.
    pub delta: Vec<Arc<RunH>>,
    pub meta: Version,
    /// Step 4, R3: where point reads count the delta-run GETs they paid, by group (None: not counted).
    pub r3: Option<Arc<R3Gets>>,
}

impl View {
    /// Open a version; runs already opened by `prev` are shared, not opened again.
    pub fn new(store: Arc<dyn ObjectStore>, v: Version, prev: Option<&View>) -> Result<View> {
        v.check()?;
        let mut known: HashMap<[u8; RUN_ID_LEN], Arc<RunH>> = HashMap::new();
        if let Some(p) = prev {
            for h in p.groups.iter().filter_map(|g| g.sealed.clone()).chain(p.delta.iter().cloned()) {
                known.insert(h.id, h);
            }
        }
        let mut handle = |l: &RunLine| -> Result<Arc<RunH>> {
            let h = RunH::new(l.clone())?;
            Ok(match known.get(&h.id) {
                Some(k) if k.line == *l => k.clone(),
                _ => Arc::new(h),
            })
        };
        let mut groups = Vec::with_capacity(v.groups.len());
        for g in &v.groups {
            let GroupLine { id, lo, x_g, sealed, .. } = g;
            groups.push(Group { id: *id, lo: unhex(lo)?, x_g: *x_g, sealed: sealed.as_ref().map(&mut handle).transpose()? });
        }
        let delta = v.delta.iter().map(&mut handle).collect::<Result<Vec<_>>>()?;
        let r3 = prev.and_then(|p| p.r3.clone());
        let view = View { store, version: v.version, x_d: v.x_d, e_env: v.e_env, groups, delta, meta: v, r3 };
        view.pin_runs();
        Ok(view)
    }

    /// P-big-speed fix 2: the runs' maps pinned. Every run of a new version is opened in the background (its fences and
    /// customer filter stay in RAM with the handle, a few bytes per 256 KiB block), so a cold read never waits for a run's
    /// open before its block GET. Runs a previous version opened are shared (`known`), so only new runs are opened.
    fn pin_runs(&self) {
        if !PIN_RUNS.load(Relaxed) {
            return;
        }
        let Ok(rt) = tokio::runtime::Handle::try_current() else { return };
        let hs: Vec<Arc<RunH>> = self.groups.iter().filter_map(|g| g.sealed.clone()).chain(self.delta.iter().cloned()).filter(|h| h.run.get().is_none()).collect();
        if hs.is_empty() {
            return;
        }
        let store = Arc::clone(&self.store);
        PINS_STARTED.fetch_add(hs.len() as u64, Relaxed);
        rt.spawn(async move {
            use futures::StreamExt;
            let store = &store;
            let mut s = futures::stream::iter(hs.into_iter().map(|h| async move { h.open(store).await.map(|_| ()) })).buffer_unordered(16);
            while let Some(r) = s.next().await {
                if r.is_err() {
                    // a run deleted meanwhile (a newer version retired it) or a store error: a read opens it on demand
                    PIN_ERRORS.fetch_add(1, Relaxed);
                }
            }
        });
    }

    /// Index of the group holding `key`.
    pub fn group_of(&self, key: &[u8]) -> usize {
        self.groups.partition_point(|g| &g.lo[..] <= key).saturating_sub(1)
    }

    fn group_hi(&self, gi: usize) -> Option<&[u8]> {
        self.groups.get(gi + 1).map(|g| &g.lo[..])
    }

    /// The runs' answer for `key` (rows at or below X_d): `None` when no run holds a valid row (the key is absent at
    /// X_d), else the winning row, a delete envelope included.
    pub async fn get(&self, key: &[u8]) -> Result<Option<RRow>> {
        if WAVE.load(Relaxed) {
            return self.get_wave(key).await;
        }
        let g = &self.groups[self.group_of(key)];
        let mut best: Option<RRow> = None;
        for d in self.delta.iter().filter(|_| !skip_delta()) {
            if !above_cut(d.line.max_seq, g.x_g) {
                break; // newest first: every later delta run is at or below X_g too
            }
            if best.as_ref().is_some_and(|b| d.line.max_seq <= b.seq) {
                continue;
            }
            let (r, paid) = run_get_counted(&self.store, d, key).await?;
            if paid {
                if let Some(c) = &self.r3 {
                    c.note(g.id);
                }
            }
            if let Some(r) = r {
                if above_cut(r.seq, g.x_g) && r.seq <= self.x_d && best.as_ref().is_none_or(|b| r.seq > b.seq) {
                    best = Some(r);
                }
            }
        }
        if best.is_none() {
            if let Some(s) = &g.sealed {
                best = sealed_point(&self.store, s, key).await?;
            }
        }
        if best.is_some() {
            ROWS_FROM_RUNS.fetch_add(1, Relaxed);
        }
        Ok(best)
    }

    /// [`View::get`] in one wave: every candidate run at once, the same answer.
    async fn get_wave(&self, key: &[u8]) -> Result<Option<RRow>> {
        let g = &self.groups[self.group_of(key)];
        let cands: Vec<&Arc<RunH>> = self.delta.iter().filter(|d| !skip_delta() && above_cut(d.line.max_seq, g.x_g)).collect();
        let delta_f = futures::future::try_join_all(cands.iter().map(|d| run_get_counted(&self.store, d, key)));
        let sealed_f = async {
            match &g.sealed {
                Some(s) if super::derive::fix_control(1) => run_get(&self.store, s, key).await,
                Some(s) => sealed_point(&self.store, s, key).await,
                None => Ok(None),
            }
        };
        let (ds, sealed) = futures::future::try_join(delta_f, sealed_f).await?;
        let mut best: Option<RRow> = None;
        for (r, paid) in ds {
            if paid {
                if let Some(c) = &self.r3 {
                    c.note(g.id);
                }
            }
            if let Some(r) = r {
                if above_cut(r.seq, g.x_g) && r.seq <= self.x_d && best.as_ref().is_none_or(|b| r.seq > b.seq) {
                    best = Some(r);
                }
            }
        }
        if best.is_none() {
            best = sealed;
        }
        if best.is_some() {
            ROWS_FROM_RUNS.fetch_add(1, Relaxed);
        }
        Ok(best)
    }

    /// P-big-speed fix 3 (the customer's box): the blocks of every run that can hold rows in [lo, hi), brought into the
    /// counter before the customer's query runs. The runs are the ones a read of [lo, hi) would read: each group's
    /// sealed run and the delta runs above its X_g, minus those whose customer filter excludes the range. Returns (runs
    /// read, blocks kept). Answers never depend on it: the blocks of a run are immutable, and every read checks the
    /// counter first.
    ///
    /// Review of big-speed (2026-11-13, error 1): before, every run's whole range was fetched in one GET and held
    /// decoded until the end, with no cap: one read of a 1.5 GiB customer OOM-killed the server under 1 GiB (MEASURED by
    /// the reviewer). Now:
    /// - a box whose missing blocks hold more than the customer's allowance of stored bytes (`BOX_CUSTOMER_BYTES`) is not
    ///   warmed at all (`BOX_TOO_BIG`): its query reads on demand, as with the box off;
    /// - otherwise the blocks come in waves of at most `BOX_WAVE_BYTES` stored bytes (a small box is still one GET per
    ///   run, all runs at once: one trip), each wave holding a permit of the process-wide `BOX_INFLIGHT_BYTES` while its
    ///   GET buffer is alive, and decoding one block at a time into the counter ([`super::counter::warm_wave`]);
    /// - the decoded bytes kept for one customer stop at `BOX_CUSTOMER_BYTES` too (`BOX_CAPPED`; compressible rows can
    ///   decode to many times their stored size).
    pub async fn warm_range(self: &Arc<Self>, lo: &[u8], hi: &[u8]) -> Result<(u64, u64)> {
        let mut cands: Vec<Arc<RunH>> = Vec::new();
        let mut seen: std::collections::HashSet<[u8; RUN_ID_LEN]> = Default::default();
        for gi in self.group_of(lo)..self.groups.len() {
            let g = &self.groups[gi];
            if &g.lo[..] >= hi {
                break;
            }
            let glo = std::cmp::max(lo, &g.lo[..]);
            let ghi = match self.group_hi(gi) {
                Some(b) => std::cmp::min(hi, b),
                None => hi,
            };
            let mut push = |h: &Arc<RunH>| {
                if h.meets(glo, Some(ghi)) && !h.filter_excludes(lo, Some(hi)) && seen.insert(h.id) {
                    cands.push(Arc::clone(h));
                }
            };
            if let Some(s) = &g.sealed {
                push(s);
            }
            for d in self.delta.iter().filter(|d| !skip_delta() && above_cut(d.line.max_seq, g.x_g)) {
                push(d);
            }
        }
        let store = &self.store;
        let runs = cands.len() as u64;
        // the plan: per run, its missing blocks in waves of at most BOX_WAVE_BYTES stored bytes
        let opened = futures::future::try_join_all(cands.into_iter().map(|h| async move {
            let run = h.open(store).await?.clone();
            Ok::<_, anyhow::Error>((h, run))
        }))
        .await?;
        let cap = BOX_CUSTOMER_BYTES.load(Relaxed);
        let wave = BOX_WAVE_BYTES.load(Relaxed).max(1);
        let mut waves: Vec<(Arc<RunH>, Arc<Run>, usize, usize, u64)> = Vec::new();
        let mut stored_total = 0u64;
        for (h, run) in opened {
            let Some((a, z)) = run.fence.blocks_for(lo, Some(hi)) else { continue };
            if let Some(r) = run.customers.customer_of(lo) {
                if !run.filter.may_contain(&lo[r]) {
                    continue;
                }
            }
            let whole = super::counter::Need::Key(&[]);
            let mut cur: Option<(usize, usize, u64)> = None;
            for b in a..=z {
                if super::counter::cached(&run, b, None, &whole) {
                    if let Some((x, y, n)) = cur.take() {
                        waves.push((Arc::clone(&h), run.clone(), x, y, n));
                    }
                    continue;
                }
                let (p, q) = run.fence.range(b, b);
                let n = q - p;
                stored_total += n;
                cur = match cur {
                    Some((x, _, m)) if m + n <= wave => Some((x, b, m + n)),
                    Some((x, y, m)) => {
                        waves.push((Arc::clone(&h), run.clone(), x, y, m));
                        Some((b, b, n))
                    }
                    None => Some((b, b, n)),
                };
            }
            if let Some((x, y, n)) = cur {
                waves.push((Arc::clone(&h), run.clone(), x, y, n));
            }
        }
        if waves.is_empty() {
            return Ok((runs, 0));
        }
        if cap > 0 && stored_total > cap {
            BOX_TOO_BIG.fetch_add(1, Relaxed);
            return Ok((runs, 0));
        }
        let left = std::sync::atomic::AtomicI64::new(if cap > 0 { cap as i64 } else { i64::MAX });
        let left = &left;
        let jobs = waves.into_iter().map(|(h, run, a, z, n)| async move {
            if left.load(Relaxed) <= 0 {
                return Ok::<_, anyhow::Error>(0u64);
            }
            let _permit = box_permit(n).await;
            if left.load(Relaxed) <= 0 {
                return Ok(0);
            }
            BOX_WAVES.fetch_add(1, Relaxed);
            let s = h.read_store(store);
            let r = match super::counter::warm_wave(s, &run, a, z, left).await {
                Ok(r) => r,
                Err(_) if !Arc::ptr_eq(s, store) => {
                    h.from_local.store(false, Relaxed);
                    super::counter::warm_wave(store, &run, a, z, left).await?
                }
                Err(e) => return Err(e),
            };
            Ok(r.0)
        });
        use futures::StreamExt;
        let mut kept = 0u64;
        let mut st = futures::stream::iter(jobs).buffer_unordered(BOX_PARALLEL);
        while let Some(r) = st.next().await {
            kept += r?;
        }
        if left.load(Relaxed) <= 0 {
            BOX_CAPPED.fetch_add(1, Relaxed);
        }
        Ok((runs, kept))
    }

    /// (runs of this version that are open, runs in all).
    pub fn runs_opened(&self) -> (usize, usize) {
        let all: Vec<&Arc<RunH>> = self.groups.iter().filter_map(|g| g.sealed.as_ref()).chain(self.delta.iter()).collect();
        (all.iter().filter(|h| h.run.get().is_some()).count(), all.len())
    }

    /// Open every run of this version (its fences and customer filter: the runs' "maps"), at most `parallel` at once.
    /// P-big-speed fix 2: done when a version is loaded, so a cold read never waits for a run's open.
    pub async fn open_all(self: &Arc<Self>, parallel: usize) -> Result<usize> {
        use futures::{StreamExt, TryStreamExt};
        let hs: Vec<Arc<RunH>> = self.groups.iter().filter_map(|g| g.sealed.clone()).chain(self.delta.iter().cloned()).filter(|h| h.run.get().is_none()).collect();
        let n = hs.len();
        let store = &self.store;
        futures::stream::iter(hs.into_iter().map(|h| async move { h.open(store).await.map(|_| ()) })).buffer_unordered(parallel.max(1)).try_collect::<Vec<_>>().await?;
        Ok(n)
    }

    /// A cursor over the runs' rows in [lo, hi) (read rule applied, delete envelopes included), ascending or descending.
    pub fn cursor(self: &Arc<Self>, lo: &[u8], hi: Option<&[u8]>, reverse: bool, read_ahead: usize) -> RunsCursor {
        let mut gs: Vec<usize> = Vec::new();
        let first = self.group_of(lo);
        for gi in first..self.groups.len() {
            if let Some(h) = hi {
                if &self.groups[gi].lo[..] >= h {
                    break;
                }
            }
            gs.push(gi);
        }
        if reverse {
            gs.reverse();
        }
        RunsCursor { view: Arc::clone(self), lo: lo.to_vec(), hi: hi.map(|h| h.to_vec()), reverse, read_ahead, groups: gs.into(), seg: None }
    }
}

/// The rows of one block a cursor reads: rows i..j of its piece are still to read (i0..j0 at the start), inside the
/// read's range in this block, [need_lo, need_hi).
struct Part {
    piece: Arc<super::counter::Piece>,
    b: usize,
    i: usize,
    j: usize,
    i0: usize,
    j0: usize,
    need_lo: Vec<u8>,
    need_hi: Option<Vec<u8>>,
}

impl Part {
    fn new(piece: Arc<super::counter::Piece>, b: usize, need_lo: Vec<u8>, need_hi: Option<Vec<u8>>) -> Part {
        let blk = &piece.blk;
        let i = blk.lower_bound(&need_lo);
        let j = need_hi.as_deref().map_or(blk.len(), |h| blk.lower_bound(h)).max(i);
        Part { piece, b, i, j, i0: i, j0: j, need_lo, need_hi }
    }

    /// The part of [need_lo, need_hi) that the piece covers.
    fn covered(&self) -> (Vec<u8>, Option<Vec<u8>>) {
        let lo = match self.piece.lo.as_deref() {
            Some(l) if l > &self.need_lo[..] => l.to_vec(),
            _ => self.need_lo.clone(),
        };
        let hi = match (self.piece.hi.as_deref(), self.need_hi.as_deref()) {
            (Some(h), Some(n)) => Some(std::cmp::min(h, n).to_vec()),
            (Some(h), None) => Some(h.to_vec()),
            (None, n) => n.map(|x| x.to_vec()),
        };
        (lo, hi)
    }

    /// Note what the cursor relied on in this piece: everything it covers once it is read to its end, or, mid-way,
    /// from the last row read to the end the read started at.
    fn rely(&self, reverse: bool) {
        let blk = &self.piece.blk;
        let (lo, hi) = self.covered();
        let done = self.i >= self.j;
        let (a, z): (Vec<u8>, Option<Vec<u8>>) = if done {
            (lo, hi)
        } else if reverse {
            if self.j == self.j0 {
                return;
            }
            (blk.key(self.j).to_vec(), hi)
        } else {
            if self.i == self.i0 {
                return;
            }
            (lo, Some(super::counter::succ(blk.key(self.i - 1))))
        };
        // an open end is the end of the prefix: the slice's last row bounds it
        let z = match z {
            Some(z) => z,
            None => match blk.len() {
                0 => return,
                n => super::counter::succ(blk.key(n - 1)),
            },
        };
        self.piece.rely(&a, &z);
    }

    /// The range still to read in this block once the piece's rows are read: below the piece's interval (reverse) or
    /// above it (forward), when the read's range goes on there.
    fn beyond(&self, reverse: bool) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
        // clipped to the read's range in this block: a slice's interval may lie wholly above (reverse) or below it
        if reverse {
            let l = self.piece.lo.as_deref()?;
            let top = match self.need_hi.as_deref() {
                Some(n) => std::cmp::min(l, n),
                None => l,
            };
            (top > &self.need_lo[..]).then(|| (self.need_lo.clone(), Some(top.to_vec())))
        } else {
            let h = self.piece.hi.as_deref()?;
            let bottom = std::cmp::max(h, &self.need_lo[..]);
            self.need_hi.as_deref().is_none_or(|n| bottom < n).then(|| (bottom.to_vec(), self.need_hi.clone()))
        }
    }
}

fn hexs(b: &[u8]) -> String {
    b.iter().map(|c| if c.is_ascii_graphic() { (*c as char).to_string() } else { format!("\\x{c:02x}") }).collect()
}

/// A lazy cursor over one run's rows in [lo, hi).
struct RunCur {
    h: Arc<RunH>,
    lo: Vec<u8>,
    hi: Option<Vec<u8>>,
    reverse: bool,
    /// blocks still to fetch: next..=end (forward) or end..=next (reverse); None once done
    blocks: Option<(usize, usize)>,
    /// read-ahead bytes until the run is open, then blocks per GET
    per_fetch: usize,
    buf: std::collections::VecDeque<Part>,
    head: Option<RRow>,
    started: bool,
    /// what fetched blocks are cut to (`counter.rs`): None, whole blocks
    prefix: Option<Vec<u8>>,
}

impl Drop for RunCur {
    fn drop(&mut self) {
        // what the read relied on in the block it stopped in
        if let Some(p) = self.buf.front() {
            p.rely(self.reverse);
        }
    }
}

impl RunCur {
    fn new(h: Arc<RunH>, lo: &[u8], hi: Option<&[u8]>, reverse: bool, read_ahead: usize) -> RunCur {
        RunCur { h, lo: lo.to_vec(), hi: hi.map(|x| x.to_vec()), reverse, blocks: None, per_fetch: read_ahead, buf: Default::default(), head: None, started: false, prefix: None }
    }

    async fn peek(&mut self, store: &Arc<dyn ObjectStore>) -> Result<Option<&RRow>> {
        if self.head.is_none() {
            self.head = self.next_row(store).await?;
        }
        Ok(self.head.as_ref())
    }

    fn take(&mut self) -> Option<RRow> {
        self.head.take()
    }

    /// The read's range inside block b: [max(lo, sep(b)), min(hi, sep(b + 1))).
    fn need(&self, run: &Run, b: usize) -> (Vec<u8>, Option<Vec<u8>>) {
        let s = run.fence.separator(b);
        let lo = std::cmp::max(self.lo.clone(), s);
        let hi = if b + 1 < run.fence.blocks() {
            let n = run.fence.separator(b + 1);
            Some(match &self.hi {
                Some(h) => std::cmp::min(h.clone(), n),
                None => n,
            })
        } else {
            self.hi.clone()
        };
        (lo, hi)
    }

    async fn next_row(&mut self, store: &Arc<dyn ObjectStore>) -> Result<Option<RRow>> {
        loop {
            if let Some(p) = self.buf.front_mut() {
                if p.i < p.j {
                    let blk = &p.piece.blk;
                    let k = if self.reverse {
                        p.j -= 1;
                        p.j
                    } else {
                        p.i += 1;
                        p.i - 1
                    };
                    return Ok(Some(RRow { key: blk.key(k).to_vec(), seq: blk.seq(k), env: blk.env(k).to_vec() }));
                }
                let p = self.buf.pop_front().unwrap();
                p.rely(self.reverse);
                // a trimmed slice ends before the read's range does in this block: the block again, for the rest
                if let (Some(prefix), Some((lo, hi))) = (self.prefix.clone(), p.beyond(self.reverse)) {
                    let run = self.h.open(store).await?.clone();
                    let piece = super::counter::refetch(store, &run, p.b, &prefix, &p.piece).await?;
                    self.buf.push_front(Part::new(piece, p.b, lo, hi));
                }
                continue;
            }
            let run = self.h.open(store).await?.clone();
            if !self.started {
                self.started = true;
                // blocks per GET from the caller's read-ahead bytes
                self.per_fetch = (self.per_fetch / (run.block_bytes.max(1) as usize)).clamp(1, 64);
                self.blocks = run.fence.blocks_for(&self.lo, self.hi.as_deref()).map(|(a, z)| if self.reverse { (z, a) } else { (a, z) });
                // a read inside one customer: the new-user filter first, then fetched blocks are cut to the read's area
                if let Some(r) = run.customers.customer_of(&self.lo) {
                    if super::counter::slice_prefix(run.customers, &self.lo, self.hi.as_deref(), super::counter::SliceMode::Customer).is_some() {
                        super::counter::FILTER_CHECKS.fetch_add(1, Relaxed);
                        if !run.filter.may_contain(&self.lo[r]) {
                            super::counter::FILTER_SKIPS.fetch_add(1, Relaxed);
                            self.blocks = None;
                        } else if WARM_THREAD.load(Relaxed) {
                            // perf-latency round 2: the first read of an agent thread in a run with derived rows fetches the
                            // thread's whole span at once, as whole blocks: the index read, the document and the messages its
                            // rebuild needs then come from the counter. Before, the index read kept only its area of the
                            // block, and the document's read fetched the same block again (two trips one after the other,
                            // MEASURED: 4 GETs and 337 KB per cold resume on the standard profile). Answers never depend on it.
                            if let Some((a, z)) = super::derive::thread_span(&run, &self.lo) {
                                let whole = |_: usize| super::counter::Need::Key(&[]);
                                if !(a..=z).all(|b| super::counter::cached(&run, b, None, &whole(b))) {
                                    super::derive::THREAD_WARMS.fetch_add(1, Relaxed);
                                    fetch_h(store, &self.h, &run, a, z, None, &whole).await?;
                                }
                            }
                        }
                    }
                }
                self.prefix = super::counter::slice_prefix(run.customers, &self.lo, self.hi.as_deref(), super::counter::slice_mode());
                if self.prefix.is_none() && std::env::var("FENVIX_COUNTER_DEBUG").is_ok() {
                    eprintln!("counter: whole blocks for [{}, {})", hexs(&self.lo), self.hi.as_deref().map(hexs).unwrap_or_default());
                }
            }
            let Some((next, end)) = self.blocks else { return Ok(None) };
            // read-ahead: up to per_fetch blocks in one GET; but when the next blocks are in the counter, only those (a
            // read that stops inside them must not pay a GET for blocks after them)
            let mut ahead = self.per_fetch;
            let mut k = 0;
            while k < ahead {
                let b = if self.reverse { next.checked_sub(k).filter(|b| *b >= end) } else { Some(next + k).filter(|b| *b <= end) };
                let Some(b) = b else { break };
                let (lo, hi) = self.need(&run, b);
                let need = if self.reverse { super::counter::Need::Down(&lo, hi.as_deref()) } else { super::counter::Need::Up(&lo, hi.as_deref()) };
                if !super::counter::cached(&run, b, self.prefix.as_deref(), &need) {
                    break;
                }
                k += 1;
            }
            if k > 0 {
                ahead = k;
            }
            let (a, z, rest) = if self.reverse {
                let a = next.saturating_sub(ahead - 1).max(end);
                (a, next, if a > end { Some((a - 1, end)) } else { None })
            } else {
                let z = (next + ahead - 1).min(end);
                (next, z, if z < end { Some((z + 1, end)) } else { None })
            };
            self.blocks = rest;
            let needs: Vec<(Vec<u8>, Option<Vec<u8>>)> = (a..=z).map(|b| self.need(&run, b)).collect();
            let reverse = self.reverse;
            let need = |b: usize| {
                let (lo, hi) = &needs[b - a];
                if reverse {
                    super::counter::Need::Down(&lo[..], hi.as_deref())
                } else {
                    super::counter::Need::Up(&lo[..], hi.as_deref())
                }
            };
            let got = fetch_h(store, &self.h, &run, a, z, self.prefix.as_deref(), &need).await?;
            let mut parts: Vec<Part> = got.into_iter().zip(needs.iter().cloned()).enumerate().map(|(o, (piece, (lo, hi)))| Part::new(piece, a + o, lo, hi)).collect();
            if self.reverse {
                parts.reverse();
            }
            self.buf.extend(parts);
        }
    }
}

/// P-big-speed fix 2: open every run of a version when it is loaded (`View::pin_runs`); runs whose open was started,
/// and opens that failed (the run is then opened on demand by the read that needs it).
pub static PIN_RUNS: AtomicBool = AtomicBool::new(true);
pub static PINS_STARTED: AtomicU64 = AtomicU64::new(0);
pub static PIN_ERRORS: AtomicU64 = AtomicU64::new(0);

/// perf-latency round 2: the thread warm-up of `RunCur` (on by default; `FENVIX_WARM_THREAD=0` turns it off, measurements).
pub static WARM_THREAD: AtomicBool = AtomicBool::new(true);

/// The runs of one group restricted to [lo, hi) ∩ the group: its sealed run and the delta runs that can hold rows
/// above its X_g.
struct Seg {
    /// Wave mode: the first block of every run was asked for at once.
    primed: bool,
    x_g: u64,
    sealed: Option<RunCur>,
    delta: Vec<RunCur>,
}

pub struct RunsCursor {
    view: Arc<View>,
    lo: Vec<u8>,
    hi: Option<Vec<u8>>,
    reverse: bool,
    read_ahead: usize,
    groups: std::collections::VecDeque<usize>,
    seg: Option<Seg>,
}

impl RunsCursor {
    fn open_seg(&self, gi: usize) -> Seg {
        let v = &self.view;
        let g = &v.groups[gi];
        let lo = std::cmp::max(&self.lo[..], &g.lo[..]).to_vec();
        let hi: Option<Vec<u8>> = match (self.hi.as_deref(), v.group_hi(gi)) {
            (Some(a), Some(b)) => Some(std::cmp::min(a, b).to_vec()),
            (Some(a), None) => Some(a.to_vec()),
            (None, Some(b)) => Some(b.to_vec()),
            (None, None) => None,
        };
        let rf = RANGE_FILTER.load(Relaxed);
        let skip = |h: &RunH| {
            let x = rf && h.filter_excludes(&self.lo, self.hi.as_deref());
            if x {
                RANGE_FILTER_SKIPS.fetch_add(1, Relaxed);
            }
            x
        };
        let sealed = g.sealed.as_ref().filter(|s| s.meets(&lo, hi.as_deref()) && !skip(s)).map(|s| RunCur::new(s.clone(), &lo, hi.as_deref(), self.reverse, self.read_ahead));
        let delta = v
            .delta
            .iter()
            .filter(|d| !skip_delta() && above_cut(d.line.max_seq, g.x_g) && d.meets(&lo, hi.as_deref()) && !skip(d))
            .map(|d| RunCur::new(d.clone(), &lo, hi.as_deref(), self.reverse, self.read_ahead))
            .collect();
        Seg { primed: false, x_g: g.x_g, sealed, delta }
    }

    /// The next row by the read rule (delete envelopes included), in the cursor's order.
    pub async fn next(&mut self) -> Result<Option<RRow>> {
        let store = self.view.store.clone();
        let x_d = self.view.x_d;
        loop {
            if self.seg.is_none() {
                let Some(gi) = self.groups.pop_front() else { return Ok(None) };
                self.seg = Some(self.open_seg(gi));
            }
            let seg = self.seg.as_mut().unwrap();
            if !seg.primed && WAVE.load(Relaxed) {
                seg.primed = true;
                let peeks = seg.sealed.iter_mut().chain(seg.delta.iter_mut()).map(|c| c.peek(&store));
                futures::future::try_join_all(peeks).await?;
            }
            // the next key among every source of the segment
            let mut key: Option<Vec<u8>> = None;
            let rev = self.reverse;
            let better = |a: &[u8], b: &Option<Vec<u8>>| match b {
                None => true,
                Some(b) => {
                    if rev {
                        a > &b[..]
                    } else {
                        a < &b[..]
                    }
                }
            };
            if let Some(s) = seg.sealed.as_mut() {
                if let Some(r) = s.peek(&store).await? {
                    key = Some(r.key.clone());
                }
            }
            for d in seg.delta.iter_mut() {
                if let Some(r) = d.peek(&store).await? {
                    if better(&r.key, &key) {
                        key = Some(r.key.clone());
                    }
                }
            }
            let Some(key) = key else {
                self.seg = None;
                continue;
            };
            let x_g = seg.x_g;
            let mut best: Option<RRow> = None;
            for d in seg.delta.iter_mut() {
                if d.peek(&store).await?.is_some_and(|r| r.key == key) {
                    let r = d.take().unwrap();
                    if above_cut(r.seq, x_g) && r.seq <= x_d && best.as_ref().is_none_or(|b| r.seq > b.seq) {
                        best = Some(r);
                    }
                }
            }
            if let Some(s) = seg.sealed.as_mut() {
                if s.peek(&store).await?.is_some_and(|r| r.key == key) {
                    let r = s.take().unwrap();
                    if best.is_none() {
                        best = if super::derive::is_derived(&r.env) { derived_row(&store, &s.h, &key, r).await? } else { Some(r) };
                    }
                }
            }
            if let Some(b) = best {
                ROWS_FROM_RUNS.fetch_add(1, Relaxed);
                return Ok(Some(b));
            }
            // only delta rows at or below X_g, and no sealed row: the key is absent at X_d
        }
    }
}
