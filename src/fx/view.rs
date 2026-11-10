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
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

/// Bytes of decoded run blocks kept in RAM, shared by every database of the process (`--sealed-cache-mb`).
pub static CACHE_BYTES: AtomicU64 = AtomicU64::new(256 << 20);
/// Counters (diagnostics and tests).
pub static RUN_GETS: AtomicU64 = AtomicU64::new(0);
pub static RUN_OPENS: AtomicU64 = AtomicU64::new(0);
pub static BLOCK_HITS: AtomicU64 = AtomicU64::new(0);
pub static ROWS_FROM_RUNS: AtomicU64 = AtomicU64::new(0);
/// Step 4, R1: block reads served by a run's copy on the waiter's NVMe (no GET), and runs opened from it.
pub static LOCAL_READS: AtomicU64 = AtomicU64::new(0);
pub static LOCAL_OPENS: AtomicU64 = AtomicU64::new(0);

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

type BKey = ([u8; RUN_ID_LEN], u32);

/// A byte-bounded LRU of decoded blocks, keyed by (run id, block), so versions that share a run share its blocks.
struct Cache {
    used: usize,
    tick: u64,
    map: HashMap<BKey, (Arc<Block>, u64)>,
    order: BTreeMap<u64, BKey>,
}

static CACHE: Mutex<Option<Cache>> = Mutex::new(None);

fn cache_get(k: &BKey) -> Option<Arc<Block>> {
    let mut g = CACHE.lock().unwrap();
    let c = g.as_mut()?;
    c.tick += 1;
    let t = c.tick;
    let (b, old) = c.map.get_mut(k)?;
    let b = b.clone();
    let o = std::mem::replace(old, t);
    c.order.remove(&o);
    c.order.insert(t, *k);
    Some(b)
}

fn cache_put(k: BKey, b: Arc<Block>) {
    let cap = CACHE_BYTES.load(Relaxed) as usize;
    if cap == 0 {
        return;
    }
    let mut g = CACHE.lock().unwrap();
    let c = g.get_or_insert_with(|| Cache { used: 0, tick: 0, map: HashMap::new(), order: BTreeMap::new() });
    c.tick += 1;
    if let Some((old, t)) = c.map.remove(&k) {
        c.order.remove(&t);
        c.used -= old.mem_bytes();
    }
    c.used += b.mem_bytes();
    c.map.insert(k, (b, c.tick));
    c.order.insert(c.tick, k);
    while c.used > cap && c.order.len() > 1 {
        let (&t, &victim) = c.order.iter().next().unwrap();
        c.order.remove(&t);
        if let Some((old, _)) = c.map.remove(&victim) {
            c.used -= old.mem_bytes();
        }
    }
}

/// Drop every cached block (tests that count GETs).
pub fn clear_cache() {
    *CACHE.lock().unwrap() = None;
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

    /// Whether [lo, hi) meets the run's key range.
    fn meets(&self, lo: &[u8], hi: Option<&[u8]>) -> bool {
        hi.map_or(true, |h| &self.first[..] < h) && &self.last[..] >= lo
    }
}

fn is_local(store: &Arc<dyn ObjectStore>) -> bool {
    super::local::store().is_some_and(|l| Arc::ptr_eq(&l, store))
}

/// Blocks first..=last of a run: cached ones from the cache, the others in one range GET.
async fn fetch(store: &Arc<dyn ObjectStore>, run: &Run, first: usize, last: usize) -> Result<Vec<Arc<Block>>> {
    let mut out: Vec<Option<Arc<Block>>> = (first..=last).map(|b| cache_get(&(run.run_id, b as u32))).collect();
    let hits = out.iter().filter(|b| b.is_some()).count();
    BLOCK_HITS.fetch_add(hits as u64, Relaxed);
    let miss: Vec<usize> = (first..=last).filter(|b| out[b - first].is_none()).collect();
    if let (Some(&a), Some(&z)) = (miss.first(), miss.last()) {
        let (lo, hi) = run.fence.range(a, z);
        let data = runfmt::get_range(store, &run.path, lo, hi).await?;
        if is_local(store) {
            LOCAL_READS.fetch_add(1, Relaxed);
        } else {
            RUN_GETS.fetch_add(1, Relaxed);
        }
        for b in a..=z {
            if out[b - first].is_some() {
                continue;
            }
            let (x, y) = run.fence.range(b, b);
            let blk = Arc::new(runfmt::decode_checked(run, b, &data[(x - lo) as usize..(y - lo) as usize])?);
            cache_put((run.run_id, b as u32), blk.clone());
            out[b - first] = Some(blk);
        }
    }
    Ok(out.into_iter().map(|b| b.expect("block fetched")).collect())
}

/// [`fetch`] from a run's NVMe copy when it was opened there, falling back to the store when the copy is gone (it is a
/// cache: GC may remove it).
async fn fetch_h(primary: &Arc<dyn ObjectStore>, h: &RunH, run: &Run, first: usize, last: usize) -> Result<Vec<Arc<Block>>> {
    let s = h.read_store(primary);
    if Arc::ptr_eq(s, primary) {
        return fetch(primary, run, first, last).await;
    }
    match fetch(s, run, first, last).await {
        Ok(b) => Ok(b),
        Err(_) => {
            h.from_local.store(false, Relaxed);
            fetch(primary, run, first, last).await
        }
    }
}

/// Blocks first..=last of a run of `h` (its NVMe copy when it has one), for the derived-document rebuild (`derive.rs`).
pub(super) async fn fetch_blocks(store: &Arc<dyn ObjectStore>, h: &RunH, run: &Run, first: usize, last: usize) -> Result<Vec<Arc<Block>>> {
    fetch_h(store, h, run, first, last).await
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
        if !run.filter.may_contain(&key[r]) {
            return Ok((None, false));
        }
    }
    let i = run.fence.count_below(key, true);
    if i == 0 {
        return Ok((None, false));
    }
    let mut paid = false;
    // a point read needs one block: probe the cache directly before the general fetch
    let b = match cache_get(&(run.run_id, (i - 1) as u32)) {
        Some(b) => {
            BLOCK_HITS.fetch_add(1, Relaxed);
            b
        }
        None => {
            paid = !is_local(store);
            fetch_h(primary, h, run, i - 1, i - 1).await?.pop().unwrap()
        }
    };
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
        fetch_h(store, h, &run, a, z).await?;
    }
    Ok(())
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
        Ok(View { store, version: v.version, x_d: v.x_d, e_env: v.e_env, groups, delta, meta: v, r3 })
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
                prefetch_thread(&self.store, s, key).await?;
                best = run_get(&self.store, s, key).await?;
                if best.as_ref().is_some_and(|r| super::derive::is_derived(&r.env)) {
                    best = derived_row(&self.store, s, key, best.unwrap()).await?;
                }
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
}

impl RunCur {
    fn new(h: Arc<RunH>, lo: &[u8], hi: Option<&[u8]>, reverse: bool, read_ahead: usize) -> RunCur {
        RunCur { h, lo: lo.to_vec(), hi: hi.map(|x| x.to_vec()), reverse, blocks: None, per_fetch: read_ahead, buf: Default::default(), head: None, started: false }
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
            let got = fetch_h(store, &self.h, &run, a, z).await?;
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
