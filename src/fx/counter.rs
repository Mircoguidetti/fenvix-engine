//! The waiter's counter for our runs (compaction 2, step 6): physical slices, the new-user filter's counters, and the
//! staged publish of a reseal.
//!
//! Physical slices. A cold read of one customer fetches whole blocks of a run (256 KiB by default) that also hold other
//! customers' rows. The counter keeps only the rows of the read's own key area: the customer's rows of the block, or
//! finer, the rows of one collection's documents or one index of that customer (`SliceMode`). The rest of the block is
//! dropped after the read. A slice is a [`Block`] of its own (`Block::slice`), keyed by (run id, block, prefix), and it
//! is exact: it holds every row of that block whose key starts with the prefix, so any read whose range lies inside the
//! prefix is answered from it exactly. Runs are immutable, so slices never need invalidation; a run that leaves the
//! manifest takes its slices with it ([`forget_runs`]).
//!
//! A read whose range is not inside one customer (the shared keyspace, `loc/`, a scan across customers) caches whole
//! blocks, as in step 3.
//!
//! The new-user filter is the run's customer filter (`runfmt::CustomerFilter`, 10 bits per customer): `view.rs` asks it
//! before every block fetch of a one-customer read, point or range. If every run says no, our tiers answer "nothing"
//! with 0 GETs. The counters are here.
//!
//! The staged publish ([`stage`]): before a version that retires runs becomes current, the waiter fetches, from the
//! version's new runs, the ranges of every slice and block it holds of a retired run. The wait is bounded
//! (`STAGE_DEADLINE_MS`, 10 s); past it the version is published anyway and what was not warmed is counted. This is
//! the reseal interface: step 4's compactor calls [`stage`] between writing a group's new sealed run and publishing the
//! version; until step 4 lands, the offline builder of step 3 (`build.rs`) calls it in the same place.

use super::view::View;
use crate::runfmt::{self, Block, CustomerKind, Run, RUN_ID_LEN};
use anyhow::Result;
use object_store::ObjectStore;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

/// What a one-customer read keeps of a fetched block (`--sealed-slices`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SliceMode {
    /// The whole decoded block (step 3).
    Off = 0,
    /// The customer's rows of the block.
    Customer = 1,
    /// The rows of the read's area inside the customer: one collection's documents (`{coll}/d/`) or one index
    /// (`{coll}/x/{index}/`); the customer's rows when the read spans areas.
    Area = 2,
}

pub static SLICES: AtomicU8 = AtomicU8::new(SliceMode::Area as u8);
/// The longest wait of a staged publish, in milliseconds (the plan's 10 s).
pub static STAGE_DEADLINE_MS: AtomicU64 = AtomicU64::new(10_000);
/// The staged publish on or off (off: the version becomes current at once, as in step 3; tests compare both).
pub static STAGED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

pub fn slice_mode() -> SliceMode {
    match SLICES.load(Relaxed) {
        0 => SliceMode::Off,
        1 => SliceMode::Customer,
        _ => SliceMode::Area,
    }
}

/// Counters (serverStatus `fenvix.sealedRuns.counter`, tests and benches).
pub static SLICES_MADE: AtomicU64 = AtomicU64::new(0);
pub static SLICE_HITS: AtomicU64 = AtomicU64::new(0);
pub static WHOLE_HITS: AtomicU64 = AtomicU64::new(0);
pub static FILTER_CHECKS: AtomicU64 = AtomicU64::new(0);
pub static FILTER_SKIPS: AtomicU64 = AtomicU64::new(0);
pub static STAGES: AtomicU64 = AtomicU64::new(0);
pub static STAGE_WARMED: AtomicU64 = AtomicU64::new(0);
pub static STAGE_MISSED: AtomicU64 = AtomicU64::new(0);
pub static STAGE_GETS: AtomicU64 = AtomicU64::new(0);
pub static STAGE_TIMEOUTS: AtomicU64 = AtomicU64::new(0);

/// RAM of a cache entry besides its rows: the hash map and order map entries and the key (ESTIMATED from the struct
/// sizes: two map entries of about 40 bytes each, the `Arc` and the block's two vectors).
pub const ENTRY_OVERHEAD: usize = 128;

/// The prefix a one-customer read of [lo, hi) slices by, or None: the read is not inside one customer (whole blocks).
pub fn slice_prefix(kind: CustomerKind, lo: &[u8], hi: Option<&[u8]>, mode: SliceMode) -> Option<Vec<u8>> {
    if mode == SliceMode::Off {
        return None;
    }
    let r = kind.customer_of(lo)?;
    let cust = &lo[..r.end];
    let inside = |p: &[u8]| match (hi, runfmt::prefix_successor(p)) {
        (Some(h), Some(s)) => h <= &s[..] && lo.starts_with(p),
        (None, None) => lo.starts_with(p),
        _ => false,
    };
    if !inside(cust) {
        return None;
    }
    if mode == SliceMode::Area && kind == CustomerKind::V2 {
        if let Some(n) = area_len(lo, r.end) {
            if inside(&lo[..n]) {
                return Some(lo[..n].to_vec());
            }
        }
    }
    Some(cust.to_vec())
}

/// The slice prefix of a point read of `key`: its area (or its customer), None outside the customer space.
pub fn point_prefix(kind: CustomerKind, key: &[u8], mode: SliceMode) -> Option<Vec<u8>> {
    if mode == SliceMode::Off {
        return None;
    }
    let r = kind.customer_of(key)?;
    if mode == SliceMode::Area && kind == CustomerKind::V2 {
        if let Some(n) = area_len(key, r.end) {
            return Some(key[..n].to_vec());
        }
    }
    Some(key[..r.end].to_vec())
}

/// Layout v2 (`custkey.rs`): after the customer component, `{coll}/d/...` or `{coll}/x/{index}/...`. The length of the
/// key's area prefix, if the key has that shape.
fn area_len(key: &[u8], cust_end: usize) -> Option<usize> {
    let rest = &key[cust_end..];
    let slash = rest.iter().position(|&b| b == b'/')?;
    let after = &rest[slash + 1..];
    if after.starts_with(b"d/") {
        return Some(cust_end + slash + 3);
    }
    if let Some(x) = after.strip_prefix(b"x/") {
        let n = x.iter().position(|&b| b == b'/')?;
        return Some(cust_end + slash + 3 + n + 1);
    }
    None
}

/// The rows of `b` whose keys start with `p`, as a block of their own.
pub fn cut(b: &Block, p: &[u8]) -> Block {
    let i = b.lower_bound(p);
    let j = match runfmt::prefix_successor(p) {
        Some(s) => b.lower_bound(&s),
        None => b.len(),
    };
    b.slice(i, j)
}

// ---- the cache: whole blocks and slices, one byte budget (`--sealed-cache-mb`), least recently used out

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CKey {
    Block([u8; RUN_ID_LEN], u32),
    Slice([u8; RUN_ID_LEN], u32, Box<[u8]>),
}

impl CKey {
    fn run(&self) -> &[u8; RUN_ID_LEN] {
        match self {
            CKey::Block(r, _) | CKey::Slice(r, _, _) => r,
        }
    }
    fn extra(&self) -> usize {
        match self {
            CKey::Block(..) => 0,
            CKey::Slice(_, _, p) => p.len(),
        }
    }
}

fn cost(k: &CKey, b: &Block) -> usize {
    b.mem_bytes() + k.extra() + ENTRY_OVERHEAD
}

struct Cache {
    used: usize,
    tick: u64,
    map: HashMap<CKey, (Arc<Block>, u64)>,
    order: BTreeMap<u64, CKey>,
}

static CACHE: Mutex<Option<Cache>> = Mutex::new(None);

pub fn cache_get(k: &CKey) -> Option<Arc<Block>> {
    let mut g = CACHE.lock().unwrap();
    let c = g.as_mut()?;
    c.tick += 1;
    let t = c.tick;
    let (b, old) = c.map.get_mut(k)?;
    let b = b.clone();
    let o = std::mem::replace(old, t);
    c.order.remove(&o);
    c.order.insert(t, k.clone());
    Some(b)
}

pub fn cache_put(k: CKey, b: Arc<Block>) {
    let cap = super::view::CACHE_BYTES.load(Relaxed) as usize;
    if cap == 0 {
        return;
    }
    let mut g = CACHE.lock().unwrap();
    let c = g.get_or_insert_with(|| Cache { used: 0, tick: 0, map: HashMap::new(), order: BTreeMap::new() });
    c.tick += 1;
    if let Some((old, t)) = c.map.remove(&k) {
        c.order.remove(&t);
        c.used -= cost(&k, &old);
    }
    c.used += cost(&k, &b);
    c.map.insert(k.clone(), (b, c.tick));
    c.order.insert(c.tick, k);
    while c.used > cap && c.order.len() > 1 {
        let (&t, _) = c.order.iter().next().unwrap();
        let victim = c.order.remove(&t).unwrap();
        if let Some((old, _)) = c.map.remove(&victim) {
            c.used -= cost(&victim, &old);
        }
    }
}

/// Drop every cached block and slice (tests that count GETs).
pub fn clear() {
    *CACHE.lock().unwrap() = None;
}

/// Drop what is cached of these runs (they left the version this process reads with).
pub fn forget_runs(ids: &HashSet<[u8; RUN_ID_LEN]>) {
    if ids.is_empty() {
        return;
    }
    let mut g = CACHE.lock().unwrap();
    let Some(c) = g.as_mut() else { return };
    let gone: Vec<CKey> = c.map.keys().filter(|k| ids.contains(k.run())).cloned().collect();
    for k in gone {
        if let Some((b, t)) = c.map.remove(&k) {
            c.order.remove(&t);
            c.used -= cost(&k, &b);
        }
    }
}

/// What the cache holds now.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct CacheStats {
    pub bytes: usize,
    pub blocks: usize,
    pub block_bytes: usize,
    pub slices: usize,
    pub slice_bytes: usize,
    pub empty_slices: usize,
}

pub fn stats() -> CacheStats {
    let g = CACHE.lock().unwrap();
    let mut s = CacheStats::default();
    if let Some(c) = g.as_ref() {
        s.bytes = c.used;
        for (k, (b, _)) in &c.map {
            match k {
                CKey::Block(..) => {
                    s.blocks += 1;
                    s.block_bytes += cost(k, b);
                }
                CKey::Slice(..) => {
                    s.slices += 1;
                    s.slice_bytes += cost(k, b);
                    s.empty_slices += b.is_empty() as usize;
                }
            }
        }
    }
    s
}

/// Bytes held per customer component (length byte included; the RAM per hot customer of the bench): slices are charged
/// to their customer; a whole block is charged to the customers of `hot` whose rows it holds, split evenly (to every
/// customer it holds when none is hot, or when `hot` is None). `kind` reads the customer of a key.
pub fn bytes_per_customer(kind: CustomerKind, hot: Option<&HashSet<Vec<u8>>>) -> HashMap<Vec<u8>, f64> {
    let g = CACHE.lock().unwrap();
    let mut out: HashMap<Vec<u8>, f64> = HashMap::new();
    let Some(c) = g.as_ref() else { return out };
    for (k, (b, _)) in &c.map {
        let bytes = cost(k, b) as f64;
        match k {
            CKey::Slice(_, _, p) => {
                if let Some(r) = kind.customer_of(p) {
                    *out.entry(p[r].to_vec()).or_default() += bytes;
                }
            }
            CKey::Block(..) => {
                let mut custs: BTreeSet<Vec<u8>> = BTreeSet::new();
                for i in 0..b.len() {
                    if let Some(r) = kind.customer_of(b.key(i)) {
                        custs.insert(b.key(i)[r].to_vec());
                    }
                }
                if let Some(h) = hot {
                    let mine: BTreeSet<Vec<u8>> = custs.iter().filter(|c| h.contains(*c)).cloned().collect();
                    if !mine.is_empty() {
                        custs = mine;
                    }
                }
                let n = custs.len().max(1) as f64;
                for c in custs {
                    *out.entry(c).or_default() += bytes / n;
                }
            }
        }
    }
    out
}

// ---- fetching blocks of a run: cached whole blocks and slices first, the rest in one range GET

/// Blocks first..=last of `run`, for a read that slices by `prefix` (None: whole blocks). A cached slice of the prefix
/// or a cached whole block answers without a GET; the missing blocks come in one range GET, are decoded and checked,
/// and each is kept whole or as its slice. Every returned block holds every row of its block inside the prefix.
pub async fn fetch(store: &Arc<dyn ObjectStore>, run: &Run, first: usize, last: usize, prefix: Option<&[u8]>) -> Result<Vec<Arc<Block>>> {
    let mut out: Vec<Option<Arc<Block>>> = Vec::with_capacity(last + 1 - first);
    for b in first..=last {
        let mut hit = None;
        if let Some(p) = prefix {
            hit = cache_get(&CKey::Slice(run.run_id, b as u32, p.into()));
            if hit.is_some() {
                SLICE_HITS.fetch_add(1, Relaxed);
            }
        }
        if hit.is_none() {
            hit = cache_get(&CKey::Block(run.run_id, b as u32));
            if hit.is_some() {
                WHOLE_HITS.fetch_add(1, Relaxed);
            }
        }
        out.push(hit);
    }
    let hits = out.iter().filter(|b| b.is_some()).count();
    super::view::BLOCK_HITS.fetch_add(hits as u64, Relaxed);
    let miss: Vec<usize> = (first..=last).filter(|b| out[b - first].is_none()).collect();
    if let (Some(&a), Some(&z)) = (miss.first(), miss.last()) {
        let (lo, hi) = run.fence.range(a, z);
        let data = runfmt::get_range(store, &run.path, lo, hi).await?;
        super::view::RUN_GETS.fetch_add(1, Relaxed);
        for b in a..=z {
            if out[b - first].is_some() {
                continue;
            }
            let (x, y) = run.fence.range(b, b);
            let blk = runfmt::decode_checked(run, b, &data[(x - lo) as usize..(y - lo) as usize])?;
            let kept = match prefix {
                Some(p) => {
                    let s = Arc::new(cut(&blk, p));
                    SLICES_MADE.fetch_add(1, Relaxed);
                    cache_put(CKey::Slice(run.run_id, b as u32, p.into()), s.clone());
                    s
                }
                None => {
                    let s = Arc::new(blk);
                    cache_put(CKey::Block(run.run_id, b as u32), s.clone());
                    s
                }
            };
            out[b - first] = Some(kept);
        }
    }
    Ok(out.into_iter().map(|b| b.expect("block fetched")).collect())
}

// ---- the staged publish (reseal interface)

/// What a staged publish did.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct StageReport {
    /// Runs of the old version that the new one no longer lists.
    pub retired_runs: usize,
    /// Slices and whole blocks held of retired runs.
    pub held: usize,
    /// Slices and blocks of the new runs that cover them and were not cached yet.
    pub wanted: usize,
    /// Of those, fetched before the deadline.
    pub warmed: usize,
    /// Wanted but not fetched before the deadline.
    pub missed: usize,
    pub gets: u64,
    pub millis: u64,
    pub timed_out: bool,
}

/// The runs of `old` that `new` does not list.
pub fn retired(old: &View, new: &View) -> HashSet<[u8; RUN_ID_LEN]> {
    let ids = |v: &View| -> HashSet<[u8; RUN_ID_LEN]> { v.groups.iter().filter_map(|g| g.sealed.as_ref().map(|h| h.id)).chain(v.delta.iter().map(|h| h.id)).collect() };
    let n = ids(new);
    ids(old).into_iter().filter(|i| !n.contains(i)).collect()
}

/// Before `new` becomes current: fetch, from its runs that `old` does not list, the ranges of every slice and whole
/// block this process holds of a run that `new` retires, within the deadline. Reads keep using `old` meanwhile (the
/// version is not published yet), and nothing fetched here is visible to a read before a version that lists the new
/// runs is installed, because cache keys carry the run id.
pub async fn stage(old: &View, new: &View) -> StageReport {
    let t0 = std::time::Instant::now();
    let mut rep = StageReport::default();
    if !STAGED.load(Relaxed) {
        return rep;
    }
    let gone = retired(old, new);
    rep.retired_runs = gone.len();
    if gone.is_empty() {
        return rep;
    }
    STAGES.fetch_add(1, Relaxed);
    // what this process holds of the retired runs: slice prefixes, and the key ranges of whole blocks
    let mut prefixes: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut ranges: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    {
        let g = CACHE.lock().unwrap();
        if let Some(c) = g.as_ref() {
            for (k, (b, _)) in &c.map {
                if !gone.contains(k.run()) {
                    continue;
                }
                rep.held += 1;
                match k {
                    CKey::Slice(_, _, p) => {
                        prefixes.insert(p.to_vec());
                    }
                    CKey::Block(..) => {
                        if !b.is_empty() {
                            ranges.push((b.key(0).to_vec(), b.key(b.len() - 1).to_vec()));
                        }
                    }
                }
            }
        }
    }
    if rep.held == 0 {
        rep.millis = t0.elapsed().as_millis() as u64;
        return rep;
    }
    let old_ids: HashSet<[u8; RUN_ID_LEN]> = old.groups.iter().filter_map(|g| g.sealed.as_ref().map(|h| h.id)).chain(old.delta.iter().map(|h| h.id)).collect();
    let fresh: Vec<Arc<super::view::RunH>> = new.groups.iter().filter_map(|g| g.sealed.clone()).chain(new.delta.iter().cloned()).filter(|h| !old_ids.contains(&h.id)).collect();
    let deadline = std::time::Duration::from_millis(STAGE_DEADLINE_MS.load(Relaxed));
    let gets0 = super::view::RUN_GETS.load(Relaxed);
    let work = warm(&new.store, &fresh, &prefixes, &ranges, &mut rep);
    let done = tokio::time::timeout(deadline, work).await;
    if done.is_err() {
        rep.timed_out = true;
        STAGE_TIMEOUTS.fetch_add(1, Relaxed);
    }
    // what was wanted and is not in the cache now was missed (the deadline, or an error on the way)
    rep.missed = rep.wanted.saturating_sub(rep.warmed);
    rep.gets = super::view::RUN_GETS.load(Relaxed) - gets0;
    STAGE_GETS.fetch_add(rep.gets, Relaxed);
    STAGE_WARMED.fetch_add(rep.warmed as u64, Relaxed);
    STAGE_MISSED.fetch_add(rep.missed as u64, Relaxed);
    rep.millis = t0.elapsed().as_millis() as u64;
    rep
}

async fn warm(store: &Arc<dyn ObjectStore>, fresh: &[Arc<super::view::RunH>], prefixes: &BTreeSet<Vec<u8>>, ranges: &[(Vec<u8>, Vec<u8>)], rep: &mut StageReport) {
    // first what each new run must give: per block, the prefixes to slice it by and whether it is wanted whole
    let mut plan: Vec<(Arc<Run>, BTreeMap<usize, (Vec<Vec<u8>>, bool)>)> = Vec::new();
    rep.wanted = rep.held; // until the plan is known, everything held counts as wanted
    let mut wanted = 0usize;
    for h in fresh {
        let Ok(run) = h.open(store).await else { continue };
        let mut want: BTreeMap<usize, (Vec<Vec<u8>>, bool)> = BTreeMap::new();
        for p in prefixes {
            let hi = runfmt::prefix_successor(p);
            if !run.may_hold(p, hi.as_deref()) {
                continue; // the run's key bounds or its customer filter rule it out
            }
            if let Some((a, z)) = run.fence.blocks_for(p, hi.as_deref()) {
                for b in a..=z {
                    if cache_get(&CKey::Slice(run.run_id, b as u32, p.clone().into())).is_none() {
                        want.entry(b).or_default().0.push(p.clone());
                        wanted += 1;
                    }
                }
            }
        }
        for (lo, last) in ranges {
            let hi = runfmt::prefix_successor(last);
            if let Some((a, z)) = run.fence.blocks_for(lo, hi.as_deref()) {
                for b in a..=z {
                    if cache_get(&CKey::Block(run.run_id, b as u32)).is_none() {
                        let e = want.entry(b).or_default();
                        if !e.1 {
                            e.1 = true;
                            wanted += 1;
                        }
                    }
                }
            }
        }
        plan.push((run.clone(), want));
    }
    rep.wanted = wanted;
    // then contiguous blocks in one range GET, at most 64 blocks each
    for (run, want) in &plan {
        let blocks: Vec<usize> = want.keys().copied().collect();
        let mut i = 0;
        while i < blocks.len() {
            let mut j = i;
            while j + 1 < blocks.len() && blocks[j + 1] == blocks[j] + 1 && j + 1 - i < 64 {
                j += 1;
            }
            if let Ok(n) = warm_blocks(store, run, blocks[i], blocks[j], want).await {
                rep.warmed += n;
            }
            i = j + 1;
        }
    }
}

/// Tests only: a delay before each warm-up GET (a slow store), to drive a staged publish past its deadline.
#[cfg(test)]
pub static TEST_WARM_DELAY_MS: AtomicU64 = AtomicU64::new(0);

async fn warm_blocks(store: &Arc<dyn ObjectStore>, run: &Run, a: usize, z: usize, want: &BTreeMap<usize, (Vec<Vec<u8>>, bool)>) -> Result<usize> {
    #[cfg(test)]
    {
        let d = TEST_WARM_DELAY_MS.load(Relaxed);
        if d > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(d)).await;
        }
    }
    let (lo, hi) = run.fence.range(a, z);
    let data = runfmt::get_range(store, &run.path, lo, hi).await?;
    super::view::RUN_GETS.fetch_add(1, Relaxed);
    let mut n = 0;
    for b in a..=z {
        let Some((ps, whole)) = want.get(&b) else { continue };
        let (x, y) = run.fence.range(b, b);
        let blk = runfmt::decode_checked(run, b, &data[(x - lo) as usize..(y - lo) as usize])?;
        for p in ps {
            cache_put(CKey::Slice(run.run_id, b as u32, p.clone().into()), Arc::new(cut(&blk, p)));
            n += 1;
        }
        if *whole {
            cache_put(CKey::Block(run.run_id, b as u32), Arc::new(blk));
            n += 1;
        }
    }
    Ok(n)
}
