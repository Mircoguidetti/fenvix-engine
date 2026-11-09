//! An opened version of our manifest: the runs it lists, opened on first use, and the read rule over them.
//!
//! The read rule for a key k of group g (plan section 2; RULES-FOR-THE-CODE rule 2): the answer is the row with the
//! highest seq among SlateDB rows with X_d < seq <= S, delta rows of g with X_g < seq <= X_d, and g's sealed run (seq
//! <= X_g). This file has the two run tiers; `read.rs` adds SlateDB on top. A winning delete envelope means absent.

use super::manifest::{unhex, GroupLine, RunLine, Version};
use crate::runfmt::{self, Block, Run, RUN_ID_LEN};
use anyhow::{ensure, Result};
use bytes::Bytes;
use object_store::path::Path;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

/// Bytes of decoded run blocks kept in RAM, shared by every database of the process (`--sealed-cache-mb`).
pub static CACHE_BYTES: AtomicU64 = AtomicU64::new(256 << 20);
/// Counters (diagnostics and tests).
pub static RUN_GETS: AtomicU64 = AtomicU64::new(0);
pub static RUN_OPENS: AtomicU64 = AtomicU64::new(0);
pub static BLOCK_HITS: AtomicU64 = AtomicU64::new(0);
pub static ROWS_FROM_RUNS: AtomicU64 = AtomicU64::new(0);

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
}

impl RunH {
    pub fn new(line: RunLine) -> Result<RunH> {
        let id: [u8; RUN_ID_LEN] = unhex(&line.id)?.try_into().map_err(|_| anyhow::anyhow!("run id of {} is not 16 bytes", line.path))?;
        Ok(RunH { first: unhex(&line.first)?, last: unhex(&line.last)?, id, line, run: tokio::sync::OnceCell::new() })
    }

    /// The run, opened on first use with the id the manifest recorded (another object at the path is an error).
    pub async fn open(&self, store: &Arc<dyn ObjectStore>) -> Result<&Arc<Run>> {
        self.run
            .get_or_try_init(|| async {
                let (run, _) = Run::open_expecting(store, Path::from(self.line.path.as_str()), self.line.size, &self.id).await?;
                ensure!(run.min_seq == self.line.min_seq && run.max_seq == self.line.max_seq && run.entries == self.line.entries, "run {} differs from its manifest line", self.line.path);
                RUN_OPENS.fetch_add(1, Relaxed);
                Ok(Arc::new(run))
            })
            .await
    }

    fn holds_key(&self, k: &[u8]) -> bool {
        &self.first[..] <= k && k <= &self.last[..]
    }

    /// Whether [lo, hi) meets the run's key range.
    fn meets(&self, lo: &[u8], hi: Option<&[u8]>) -> bool {
        hi.map_or(true, |h| &self.first[..] < h) && &self.last[..] >= lo
    }
}

async fn fetch(store: &Arc<dyn ObjectStore>, run: &Run, first: usize, last: usize, prefix: Option<&[u8]>) -> Result<Vec<Arc<Block>>> {
    super::counter::fetch(store, run, first, last, prefix).await
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
    if !h.holds_key(key) {
        return Ok(None);
    }
    let run = h.open(store).await?;
    if let Some(r) = run.customers.customer_of(key) {
        // the new-user filter: a customer the run has no row of costs no GET
        super::counter::FILTER_CHECKS.fetch_add(1, Relaxed);
        if !run.filter.may_contain(&key[r]) {
            super::counter::FILTER_SKIPS.fetch_add(1, Relaxed);
            return Ok(None);
        }
    }
    let i = run.fence.count_below(key, true);
    if i == 0 {
        return Ok(None);
    }
    // a point read needs one block (or its slice of the key's area)
    let p = super::counter::point_prefix(run.customers, key, super::counter::slice_mode());
    let b = fetch(store, run, i - 1, i - 1, p.as_deref()).await?.pop().unwrap();
    let j = b.lower_bound(key);
    Ok((j < b.len() && b.key(j) == key).then(|| RRow { key: Vec::new(), seq: b.seq(j), env: b.env(j).to_vec() }))
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
            let GroupLine { id, lo, x_g, sealed } = g;
            groups.push(Group { id: *id, lo: unhex(lo)?, x_g: *x_g, sealed: sealed.as_ref().map(&mut handle).transpose()? });
        }
        let delta = v.delta.iter().map(&mut handle).collect::<Result<Vec<_>>>()?;
        Ok(View { store, version: v.version, x_d: v.x_d, e_env: v.e_env, groups, delta, meta: v })
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
        let g = &self.groups[self.group_of(key)];
        let mut best: Option<RRow> = None;
        for d in self.delta.iter().filter(|_| !skip_delta()) {
            if !above_cut(d.line.max_seq, g.x_g) {
                break; // newest first: every later delta run is at or below X_g too
            }
            if best.as_ref().is_some_and(|b| d.line.max_seq <= b.seq) {
                continue;
            }
            if let Some(r) = run_get(&self.store, d, key).await? {
                if above_cut(r.seq, g.x_g) && r.seq <= self.x_d && best.as_ref().is_none_or(|b| r.seq > b.seq) {
                    best = Some(r);
                }
            }
        }
        if best.is_none() {
            if let Some(s) = &g.sealed {
                best = run_get(&self.store, s, key).await?;
            }
        }
        if best.is_some() {
            ROWS_FROM_RUNS.fetch_add(1, Relaxed);
        }
        Ok(best)
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
    buf: std::collections::VecDeque<(Arc<Block>, usize, usize)>,
    head: Option<RRow>,
    started: bool,
    /// what fetched blocks are cut to (`counter.rs`): None, whole blocks
    prefix: Option<Vec<u8>>,
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

    async fn next_row(&mut self, store: &Arc<dyn ObjectStore>) -> Result<Option<RRow>> {
        loop {
            if let Some((b, i, j)) = self.buf.front_mut() {
                if *i < *j {
                    let k = if self.reverse {
                        *j -= 1;
                        *j
                    } else {
                        *i += 1;
                        *i - 1
                    };
                    return Ok(Some(RRow { key: b.key(k).to_vec(), seq: b.seq(k), env: b.env(k).to_vec() }));
                }
                self.buf.pop_front();
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
                        }
                    }
                }
                self.prefix = super::counter::slice_prefix(run.customers, &self.lo, self.hi.as_deref(), super::counter::slice_mode());
            }
            let Some((next, end)) = self.blocks else { return Ok(None) };
            let (a, z, rest) = if self.reverse {
                let a = next.saturating_sub(self.per_fetch - 1).max(end);
                (a, next, if a > end { Some((a - 1, end)) } else { None })
            } else {
                let z = (next + self.per_fetch - 1).min(end);
                (next, z, if z < end { Some((z + 1, end)) } else { None })
            };
            self.blocks = rest;
            let got = fetch(store, &run, a, z, self.prefix.as_deref()).await?;
            let mut parts: Vec<(Arc<Block>, usize, usize)> = got
                .into_iter()
                .map(|b| {
                    let i = b.lower_bound(&self.lo);
                    let j = match &self.hi {
                        Some(h) => b.lower_bound(h),
                        None => b.len(),
                    };
                    (b, i, j.max(i))
                })
                .collect();
            if self.reverse {
                parts.reverse();
            }
            self.buf.extend(parts);
        }
    }
}

/// The runs of one group restricted to [lo, hi) ∩ the group: its sealed run and the delta runs that can hold rows
/// above its X_g.
struct Seg {
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
        let sealed = g.sealed.as_ref().filter(|s| s.meets(&lo, hi.as_deref())).map(|s| RunCur::new(s.clone(), &lo, hi.as_deref(), self.reverse, self.read_ahead));
        let delta = v
            .delta
            .iter()
            .filter(|d| !skip_delta() && above_cut(d.line.max_seq, g.x_g) && d.meets(&lo, hi.as_deref()))
            .map(|d| RunCur::new(d.clone(), &lo, hi.as_deref(), self.reverse, self.read_ahead))
            .collect();
        Seg { x_g: g.x_g, sealed, delta }
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
                        best = Some(r);
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
