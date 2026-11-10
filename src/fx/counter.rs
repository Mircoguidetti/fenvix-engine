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
//! The new-user filter is the run's customer filter (`runfmt::CustomerFilter`, 16 bits per customer by default): `view.rs` asks it
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

/// Negative controls of the step 6 fix round, tests only: `FX_CONTROL_SIBLING` keeps a sibling slice whose area cannot
/// be read from its key (under the customer's key, from that row), `FX_CONTROL_STAGE_HULL` warms one hull per prefix,
/// open upward, without a byte cap (both as delivered before the review). Read from the environment, so a test can run
/// a reviewer's test, unchanged, in a child process with the old behaviour.
#[cfg(test)]
pub fn control(name: &str) -> bool {
    std::env::var(name).is_ok()
}

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
    if mode == SliceMode::Area && matches!(kind, CustomerKind::V2 | CustomerKind::V2K) {
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
    if mode == SliceMode::Area && matches!(kind, CustomerKind::V2 | CustomerKind::V2K) {
        if let Some(n) = area_len(key, r.end) {
            return Some(key[..n].to_vec());
        }
    }
    Some(key[..r.end].to_vec())
}

/// The area prefix of `key` (layout v2: `u/{customer}{collection}/d/` or `u/{customer}{collection}/x/{index}/`), or
/// None when the key has no area this rule can read. Never the customer prefix.
pub fn area_prefix(kind: CustomerKind, key: &[u8]) -> Option<Vec<u8>> {
    if !matches!(kind, CustomerKind::V2 | CustomerKind::V2K) {
        return None;
    }
    let r = kind.customer_of(key)?;
    area_len(key, r.end).map(|n| key[..n].to_vec())
}

/// Layout v2 (`custkey.rs`): after the customer component, `{coll}/d/...` or `{coll}/x/{index}/...`. The length of the
/// key's area prefix, if the key has that shape. The server escapes '/' in collection and index names (`%2F`,
/// `layout::escape_collection`), so in its keys the first '/' after the customer component ends the collection name and
/// the next one the index name: this reads the area by the key encoding. A key not of that shape (a raw '/' in a name,
/// only in synthetic keys) has no area: its reads slice by the customer, cut from the customer's first row.
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

// ---- pieces: what the counter holds of one block of a run

/// The immediate successor of a key (the smallest key greater than it).
pub fn succ(k: &[u8]) -> Vec<u8> {
    let mut v = k.to_vec();
    v.push(0);
    v
}

/// What the counter holds of one block: a whole block, or a slice of the rows under a prefix. A slice covers the key
/// interval [lo, hi) of its prefix (None: to the prefix's edge): it holds every row of the block whose key starts with the
/// prefix and lies in that interval, so a read inside the interval is answered from it exactly.
///
/// A slice is first kept whole (every row of the prefix in the block), and the reads that use it note the key intervals
/// they relied on (`rely`). After `TRIM_MS` it is trimmed to the hull of those intervals ([`trim_due`]): RAM per hot
/// customer becomes the rows its reads use, not the rows of its area. A read that needs keys outside a trimmed slice's
/// interval fetches the block again; the new slice keeps what the old one relied on.
pub struct Piece {
    pub blk: Block,
    pub lo: Option<Box<[u8]>>,
    pub hi: Option<Box<[u8]>>,
    /// Waiting to be trimmed to what reads relied on (a slice kept whole after a cold read, or warmed by a staged
    /// publish); false once trimmed, and for whole blocks.
    whole_slice: bool,
    /// Warmed by a staged publish: while no read relied on it yet, a trim keeps it as it is (its customer has not come
    /// back yet) instead of dropping it.
    keep_unused: bool,
    born: std::time::Instant,
    relied: Mutex<Option<(Vec<u8>, Vec<u8>)>>,
}

impl Piece {
    fn new(blk: Block, whole_slice: bool) -> Piece {
        Piece { blk, lo: None, hi: None, whole_slice, keep_unused: false, born: std::time::Instant::now(), relied: Mutex::new(None) }
    }

    /// Whether `k` is inside the covered interval.
    pub fn covers(&self, k: &[u8]) -> bool {
        self.lo.as_deref().is_none_or(|l| l <= k) && self.hi.as_deref().is_none_or(|h| k < h)
    }

    /// Whether the interval reaches down to `lo` (inclusive).
    pub fn covers_down_to(&self, lo: &[u8]) -> bool {
        self.lo.as_deref().is_none_or(|l| l <= lo)
    }

    /// Whether the interval reaches up to `hi` (exclusive; None: the prefix's end).
    pub fn covers_up_to(&self, hi: Option<&[u8]>) -> bool {
        match (self.hi.as_deref(), hi) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(h), Some(x)) => h >= x,
        }
    }

    /// A read relied on the rows of [lo, hi) of this piece (hi exclusive).
    pub fn rely(&self, lo: &[u8], hi: &[u8]) {
        if !self.whole_slice || lo >= hi {
            return;
        }
        let mut g = self.relied.lock().unwrap();
        match g.as_mut() {
            None => *g = Some((lo.to_vec(), hi.to_vec())),
            Some((a, b)) => {
                if lo < &a[..] {
                    *a = lo.to_vec();
                }
                if hi > &b[..] {
                    *b = hi.to_vec();
                }
            }
        }
    }

    fn relied(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.relied.lock().unwrap().clone()
    }

    /// The piece trimmed to the hull of what reads relied on, with the widest exact interval: from just above the row
    /// before the first kept row, up to the row after the last kept one. None: nothing was relied on.
    fn trimmed(&self) -> Option<Piece> {
        let (rlo, rhi) = self.relied()?;
        let b = &self.blk;
        let i = b.lower_bound(&rlo);
        let j = b.lower_bound(&rhi); // rows i..j are inside [rlo, rhi)
        let lo = if i == 0 { self.lo.clone() } else { Some(succ(b.key(i - 1)).into_boxed_slice()) };
        let hi = if j >= b.len() { self.hi.clone() } else { Some(b.key(j).to_vec().into_boxed_slice()) };
        Some(Piece { blk: b.slice(i, j), lo, hi, whole_slice: false, keep_unused: false, born: std::time::Instant::now(), relied: Mutex::new(None) })
    }

    pub fn mem_bytes(&self) -> usize {
        self.blk.mem_bytes() + self.lo.as_ref().map_or(0, |x| x.len()) + self.hi.as_ref().map_or(0, |x| x.len())
    }
}

/// How long a slice is kept whole before it is trimmed to what reads relied on (ms; `u64::MAX`: never trimmed).
pub static TRIM_MS: AtomicU64 = AtomicU64::new(1_000);
/// How long a slice warmed by a staged publish waits for its customer's next read before it is dropped (ms).
pub static KEEP_UNUSED_MS: AtomicU64 = AtomicU64::new(600_000);
pub static TRIMS: AtomicU64 = AtomicU64::new(0);
pub static TRIM_DROPS: AtomicU64 = AtomicU64::new(0);
pub static REFETCHES: AtomicU64 = AtomicU64::new(0);

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

fn cost(k: &CKey, b: &Piece) -> usize {
    b.mem_bytes() + k.extra() + ENTRY_OVERHEAD
}

struct Cache {
    used: usize,
    tick: u64,
    map: HashMap<CKey, (Arc<Piece>, u64)>,
    order: BTreeMap<u64, CKey>,
    /// Whole slices waiting to be trimmed, oldest first.
    untrimmed: std::collections::VecDeque<(std::time::Instant, CKey)>,
}

static CACHE: Mutex<Option<Cache>> = Mutex::new(None);

pub fn cache_get(k: &CKey) -> Option<Arc<Piece>> {
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

fn put_locked(c: &mut Cache, k: CKey, b: Arc<Piece>, cap: usize) {
    c.tick += 1;
    if let Some((old, t)) = c.map.remove(&k) {
        c.order.remove(&t);
        c.used -= cost(&k, &old);
    }
    if b.whole_slice && !b.blk.is_empty() {
        c.untrimmed.push_back((std::time::Instant::now(), k.clone()));
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

fn cache_put(k: CKey, b: Arc<Piece>) {
    let cap = super::view::CACHE_BYTES.load(Relaxed) as usize;
    if cap == 0 {
        return;
    }
    let mut g = CACHE.lock().unwrap();
    let c = g.get_or_insert_with(|| Cache { used: 0, tick: 0, map: HashMap::new(), order: BTreeMap::new(), untrimmed: Default::default() });
    put_locked(c, k, b, cap);
    trim_locked(c, Some(std::time::Duration::from_millis(TRIM_MS.load(Relaxed))), 8);
}

/// Trim whole slices older than `age` (None: every one), at most `max` of them.
fn trim_locked(c: &mut Cache, age: Option<std::time::Duration>, max: usize) {
    let now = std::time::Instant::now();
    // each queued slice is looked at most once per call (staged slices nobody relied on go back to the queue)
    let max = max.min(c.untrimmed.len());
    let mut n = 0;
    while n < max {
        let Some((born, _)) = c.untrimmed.front() else { break };
        if let Some(a) = age {
            if now.duration_since(*born) < a {
                break;
            }
        }
        let (_, k) = c.untrimmed.pop_front().unwrap();
        n += 1;
        let Some((p, tick)) = c.map.get(&k).map(|(p, t)| (p.clone(), *t)) else { continue };
        if !p.whole_slice {
            continue; // replaced meanwhile
        }
        if p.keep_unused && p.relied().is_none() && now.duration_since(p.born) < std::time::Duration::from_millis(KEEP_UNUSED_MS.load(Relaxed)) {
            // a staged slice whose customer has not read yet: kept as it is, looked at again later (dropped once it is
            // older than KEEP_UNUSED_MS with no read: its customer is not coming back soon)
            c.untrimmed.push_back((now, k));
            continue;
        }
        c.used -= cost(&k, &p);
        match p.trimmed() {
            Some(t) => {
                TRIMS.fetch_add(1, Relaxed);
                let t = Arc::new(t);
                c.used += cost(&k, &t);
                c.map.insert(k, (t, tick));
            }
            None => {
                TRIM_DROPS.fetch_add(1, Relaxed);
                c.map.remove(&k);
                c.order.remove(&tick);
            }
        }
    }
}

/// Trim the whole slices that are due (older than `TRIM_MS`).
pub fn trim_due() {
    let mut g = CACHE.lock().unwrap();
    if let Some(c) = g.as_mut() {
        trim_locked(c, Some(std::time::Duration::from_millis(TRIM_MS.load(Relaxed))), usize::MAX);
    }
}

/// Trim every whole slice now (tests and benches: the state between two visits).
pub fn trim_all() {
    let mut g = CACHE.lock().unwrap();
    if let Some(c) = g.as_mut() {
        trim_locked(c, None, usize::MAX);
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
    pub untrimmed_slices: usize,
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
                    s.empty_slices += b.blk.is_empty() as usize;
                    s.untrimmed_slices += b.whole_slice as usize;
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
                for i in 0..b.blk.len() {
                    if let Some(r) = kind.customer_of(b.blk.key(i)) {
                        custs.insert(b.blk.key(i)[r].to_vec());
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

/// What a read needs of block b: whether a cached piece will do.
pub enum Need<'a> {
    /// A point read of this key.
    Key(&'a [u8]),
    /// A range read of [lo, hi) in this block that starts at its top (reverse): the piece must cover the top of it.
    Down(&'a [u8], Option<&'a [u8]>),
    /// A range read of [lo, hi) in this block that starts at its bottom (forward).
    Up(&'a [u8], Option<&'a [u8]>),
}

fn good(p: &Piece, need: &Need) -> bool {
    match need {
        Need::Key(k) => p.covers(k),
        // the top is covered, and the interval reaches into the range (not wholly above it)
        Need::Down(lo, hi) => p.covers_up_to(*hi) && p.lo.as_deref().is_none_or(|l| hi.is_none_or(|h| l < h)) && p.hi.as_deref().is_none_or(|h| h > *lo),
        Need::Up(lo, hi) => p.covers_down_to(lo) && p.hi.as_deref().is_none_or(|h| h > *lo) && p.lo.as_deref().is_none_or(|l| hi.is_none_or(|x| l < x)),
    }
}

/// Whether block b of the run is in the counter in a form that serves `need` (no GET), for the read-ahead decision.
pub fn cached(run: &Run, b: usize, prefix: Option<&[u8]>, need: &Need) -> bool {
    if let Some(p) = prefix {
        if cache_get(&CKey::Slice(run.run_id, b as u32, p.into())).is_some_and(|s| good(&s, need)) {
            return true;
        }
    }
    cache_get(&CKey::Block(run.run_id, b as u32)).is_some()
}

/// Blocks first..=last of `run`, for a read that slices by `prefix` (None: whole blocks). A cached piece answers without
/// a GET when it covers where the read starts in that block (`need(b)`); the missing blocks come in one range GET, are
/// decoded and checked, and each is kept whole or as its slice (a new slice keeps what the piece it replaces relied on).
/// A returned slice may end before the read's range does (a trimmed slice): the caller continues with [`refetch`].
pub async fn fetch<'n>(store: &Arc<dyn ObjectStore>, run: &Run, first: usize, last: usize, prefix: Option<&[u8]>, need: &(dyn Fn(usize) -> Need<'n> + Sync)) -> Result<Vec<Arc<Piece>>> {
    // a few due trims on every fetch, so a counter that only serves hits is trimmed too
    {
        let mut g = CACHE.lock().unwrap();
        if let Some(c) = g.as_mut() {
            trim_locked(c, Some(std::time::Duration::from_millis(TRIM_MS.load(Relaxed))), 8);
        }
    }
    let mut out: Vec<Option<Arc<Piece>>> = Vec::with_capacity(last + 1 - first);
    let mut stale: Vec<Option<Arc<Piece>>> = Vec::with_capacity(last + 1 - first);
    for b in first..=last {
        let mut hit = None;
        let mut old = None;
        if let Some(p) = prefix {
            if let Some(s) = cache_get(&CKey::Slice(run.run_id, b as u32, p.into())) {
                if good(&s, &need(b)) {
                    SLICE_HITS.fetch_add(1, Relaxed);
                    hit = Some(s);
                } else {
                    old = Some(s);
                }
            }
        }
        if hit.is_none() {
            hit = cache_get(&CKey::Block(run.run_id, b as u32));
            if hit.is_some() {
                WHOLE_HITS.fetch_add(1, Relaxed);
            }
        }
        out.push(hit);
        stale.push(old);
    }
    let hits = out.iter().filter(|b| b.is_some()).count();
    super::view::BLOCK_HITS.fetch_add(hits as u64, Relaxed);
    let miss: Vec<usize> = (first..=last).filter(|b| out[b - first].is_none()).collect();
    if let (Some(&a), Some(&z)) = (miss.first(), miss.last()) {
        if std::env::var("FENVIX_COUNTER_DEBUG").is_ok() {
            eprintln!("counter: GET blocks {a}..={z} of {} for prefix {:?} (stale piece: {})", run.path, prefix.map(|p| String::from_utf8_lossy(p).to_string()), stale.iter().any(|x| x.is_some()));
        }
        let (lo, hi) = run.fence.range(a, z);
        let data = runfmt::get_range(store, &run.path, lo, hi).await?;
        // a read of the run's NVMe copy (step 4) is not a GET
        if super::view::is_local(store) {
            super::view::LOCAL_READS.fetch_add(1, Relaxed);
        } else {
            super::view::RUN_GETS.fetch_add(1, Relaxed);
        }
        for b in a..=z {
            if out[b - first].is_some() {
                continue;
            }
            let (x, y) = run.fence.range(b, b);
            let blk = runfmt::decode_checked(run, b, &data[(x - lo) as usize..(y - lo) as usize])?;
            out[b - first] = Some(keep(run, b, blk, prefix, stale[b - first].as_deref()));
        }
    }
    Ok(out.into_iter().map(|b| b.expect("block fetched")).collect())
}

/// Block b of `run` again, from the store, for a read that ran past a trimmed slice's interval.
pub async fn refetch(store: &Arc<dyn ObjectStore>, run: &Run, b: usize, prefix: &[u8], old: &Piece) -> Result<Arc<Piece>> {
    REFETCHES.fetch_add(1, Relaxed);
    let (lo, hi) = run.fence.range(b, b);
    let data = runfmt::get_range(store, &run.path, lo, hi).await?;
    super::view::RUN_GETS.fetch_add(1, Relaxed);
    let blk = runfmt::decode_checked(run, b, &data)?;
    Ok(keep(run, b, blk, Some(prefix), Some(old)))
}

/// Keep a fetched block whole (no prefix) or as its whole slice, which inherits what `old` relied on.
fn keep(run: &Run, b: usize, blk: Block, prefix: Option<&[u8]>, old: Option<&Piece>) -> Arc<Piece> {
    match prefix {
        Some(p) => {
            let s = Arc::new(Piece::new(cut(&blk, p), true));
            if let Some(o) = old {
                if let Some((a, z)) = o.relied() {
                    s.rely(&a, &z);
                } else if !o.blk.is_empty() {
                    s.rely(o.blk.key(0), &succ(o.blk.key(o.blk.len() - 1)));
                }
            }
            SLICES_MADE.fetch_add(1, Relaxed);
            cache_put(CKey::Slice(run.run_id, b as u32, p.into()), s.clone());
            // area slices: the customer's other areas in this block (its documents next to the index entries just read)
            // are kept too, whole, so the read's next step finds them without a second GET of the same block; trimming
            // drops the ones no read relies on. Only true areas: a row whose area cannot be read from its key is never
            // kept as a sibling (it would be stored under the customer's key, starting mid-customer, review of step 6
            // FIX 1), and each sibling is cut from the whole block by its own prefix, so it holds every row of its area.
            if let Some(r) = run.customers.customer_of(p) {
                let cust = &p[..r.end];
                if cust.len() < p.len() {
                    let rows = cut(&blk, cust);
                    let mut i = 0;
                    while i < rows.len() {
                        #[cfg(test)]
                        if control("FX_CONTROL_SIBLING") {
                            let Some(a) = point_prefix(run.customers, rows.key(i), SliceMode::Area) else { break };
                            let end = runfmt::prefix_successor(&a);
                            let j = end.as_deref().map_or(rows.len(), |e| rows.lower_bound(e));
                            if a[..] != p[..] {
                                let k = CKey::Slice(run.run_id, b as u32, a.clone().into_boxed_slice());
                                if cache_get(&k).is_none() {
                                    cache_put(k, Arc::new(Piece::new(rows.slice(i, j), true)));
                                }
                            }
                            i = j.max(i + 1);
                            continue;
                        }
                        let Some(a) = area_prefix(run.customers, rows.key(i)) else {
                            i += 1;
                            continue;
                        };
                        let end = runfmt::prefix_successor(&a);
                        let j = end.as_deref().map_or(rows.len(), |e| rows.lower_bound(e));
                        if a[..] != p[..] {
                            let k = CKey::Slice(run.run_id, b as u32, a.clone().into_boxed_slice());
                            if cache_get(&k).is_none() {
                                cache_put(k, Arc::new(Piece::new(cut(&blk, &a), true)));
                            }
                        }
                        i = j.max(i + 1);
                    }
                }
            }
            s
        }
        None => {
            let s = Arc::new(Piece::new(blk, false));
            cache_put(CKey::Block(run.run_id, b as u32), s.clone());
            s
        }
    }
}

// ---- the staged publish (reseal interface)

/// What a staged publish did.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct StageReport {
    /// Runs of the old version that the new one no longer lists.
    pub retired_runs: usize,
    /// Slices and whole blocks held of retired runs.
    pub held: usize,
    /// Their bytes.
    pub held_bytes: usize,
    /// Slices and blocks of the new runs that cover them and were not cached yet.
    pub wanted: usize,
    /// Of those, fetched before the deadline and within the byte cap.
    pub warmed: usize,
    /// Their bytes.
    pub warmed_bytes: usize,
    /// Wanted but not fetched (the deadline or the byte cap).
    pub missed: usize,
    pub gets: u64,
    pub millis: u64,
    pub timed_out: bool,
    /// The warm-up reached `STAGE_BYTES_PCT` of the bytes held and stopped.
    pub capped: bool,
}

/// The staged publish warms at most this share of the bytes it held of the retired runs, in percent, plus
/// `STAGE_BYTES_EXTRA` (the rows written since the retired run, at the top of the areas). Review of step 6, FIX 2: a
/// reseal must not multiply the counter.
pub static STAGE_BYTES_PCT: AtomicU64 = AtomicU64::new(150);
pub static STAGE_BYTES_EXTRA: AtomicU64 = AtomicU64::new(1 << 20);
pub static STAGE_CAPPED: AtomicU64 = AtomicU64::new(0);

/// The runs of `old` that `new` does not list.
pub fn retired(old: &View, new: &View) -> HashSet<[u8; RUN_ID_LEN]> {
    let ids = |v: &View| -> HashSet<[u8; RUN_ID_LEN]> { v.groups.iter().filter_map(|g| g.sealed.as_ref().map(|h| h.id)).chain(v.delta.iter().map(|h| h.id)).collect() };
    let n = ids(new);
    ids(old).into_iter().filter(|i| !n.contains(i)).collect()
}

/// One key interval to warm under a slice prefix: [lo, hi), hi None = open upward to the end of the prefix.
type Interval = (Vec<u8>, Option<Vec<u8>>);

/// Before `new` becomes current: fetch, from its runs that `old` does not list, what this process holds of a run that
/// `new` retires, within the deadline and a byte cap. Reads keep using `old` meanwhile (the version is not published
/// yet), and nothing fetched here is visible to a read before a version that lists the new runs is installed, because
/// cache keys carry the run id.
///
/// Each held slice is carried as its own interval, with its old block's edges for its open ends (review of step 6,
/// FIX 2: one hull per prefix, open upward, fetched whole histories of customers that read their newest and oldest
/// rows). A slice is opened upward to the end of its area only when it reached the top of the area in the old run:
/// the rows written since land there. The warmed bytes are capped against the bytes held.
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
    // what this process holds of the retired runs: per slice its block, prefix and interval (None: to the edge of what
    // it covers), and the key ranges of whole blocks
    let mut held: Vec<([u8; RUN_ID_LEN], usize, Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>)> = Vec::new();
    let mut ranges: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    {
        let g = CACHE.lock().unwrap();
        if let Some(c) = g.as_ref() {
            for (k, (b, _)) in &c.map {
                if !gone.contains(k.run()) {
                    continue;
                }
                rep.held += 1;
                rep.held_bytes += cost(k, b);
                match k {
                    CKey::Slice(run, blk, p) => {
                        // a slice still whole: what reads relied on so far (it would be trimmed to that); a slice nobody
                        // relied on yet is not carried. A trimmed slice, or one warmed earlier and not read since: its
                        // interval.
                        let (lo, hi) = match (b.whole_slice, b.relied()) {
                            (true, Some((x, y))) => (Some(x), Some(y)),
                            (true, None) if !b.keep_unused => continue,
                            _ => (b.lo.as_deref().map(|x| x.to_vec()), b.hi.as_deref().map(|x| x.to_vec())),
                        };
                        held.push((*run, *blk as usize, p.to_vec(), lo, hi));
                    }
                    CKey::Block(..) => {
                        if !b.blk.is_empty() {
                            ranges.push((b.blk.key(0).to_vec(), b.blk.key(b.blk.len() - 1).to_vec()));
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
    // open ends become the old block's edges; a slice opens upward only at the top of its area in the old run
    let old_runs: HashMap<[u8; RUN_ID_LEN], Arc<super::view::RunH>> = old.groups.iter().filter_map(|g| g.sealed.clone()).chain(old.delta.iter().cloned()).map(|h| (h.id, h)).collect();
    let mut prefixes: BTreeMap<Vec<u8>, Vec<Interval>> = BTreeMap::new();
    for (run, b, p, lo, hi) in held {
        let Some(h) = old_runs.get(&run) else { continue };
        let Ok(r) = h.open(&old.store).await else { continue };
        let end = runfmt::prefix_successor(&p);
        let lo = match lo {
            Some(l) => std::cmp::max(l, p.clone()),
            None => std::cmp::max(r.fence.separator(b), p.clone()),
        };
        let hi = match hi {
            Some(x) => Some(x),
            None => {
                let next = (b + 1 < r.fence.blocks()).then(|| r.fence.separator(b + 1));
                match (next, &end) {
                    // the next block starts inside the area: this slice is not its top
                    (Some(n), Some(e)) if n < *e => Some(n),
                    (Some(n), None) => Some(n),
                    _ => None,
                }
            }
        };
        prefixes.entry(p).or_default().push((lo, hi));
    }
    #[cfg(test)]
    if control("FX_CONTROL_STAGE_HULL") {
        // as delivered: one hull per prefix, open upward, no cap
        for v in prefixes.values_mut() {
            let lo = v.iter().map(|x| x.0.clone()).min().unwrap();
            *v = vec![(lo, None)];
        }
    }
    for v in prefixes.values_mut() {
        v.sort();
        let mut merged: Vec<Interval> = Vec::new();
        for (lo, hi) in v.drain(..) {
            if let Some(last) = merged.last_mut() {
                let touches = last.1.as_ref().is_none_or(|h| lo <= *h);
                if touches {
                    last.1 = match (&last.1, hi) {
                        (None, _) | (_, None) => None,
                        (Some(a), Some(b)) => Some(std::cmp::max(a.clone(), b)),
                    };
                    continue;
                }
            }
            merged.push((lo, hi));
        }
        *v = merged;
    }
    let old_ids: HashSet<[u8; RUN_ID_LEN]> = old_runs.keys().copied().collect();
    let fresh: Vec<Arc<super::view::RunH>> = new.groups.iter().filter_map(|g| g.sealed.clone()).chain(new.delta.iter().cloned()).filter(|h| !old_ids.contains(&h.id)).collect();
    let deadline = std::time::Duration::from_millis(STAGE_DEADLINE_MS.load(Relaxed));
    #[allow(unused_mut)]
    let mut cap = rep.held_bytes * STAGE_BYTES_PCT.load(Relaxed) as usize / 100 + STAGE_BYTES_EXTRA.load(Relaxed) as usize;
    #[cfg(test)]
    if control("FX_CONTROL_STAGE_HULL") {
        cap = usize::MAX;
    }
    let gets0 = super::view::RUN_GETS.load(Relaxed);
    let work = warm(&new.store, &fresh, &prefixes, &ranges, cap, &mut rep);
    let done = tokio::time::timeout(deadline, work).await;
    if done.is_err() {
        rep.timed_out = true;
        STAGE_TIMEOUTS.fetch_add(1, Relaxed);
    }
    if rep.capped {
        STAGE_CAPPED.fetch_add(1, Relaxed);
    }
    // what was wanted and is not in the cache now was missed (the deadline, the cap, or an error on the way)
    rep.missed = rep.wanted.saturating_sub(rep.warmed);
    rep.gets = super::view::RUN_GETS.load(Relaxed) - gets0;
    STAGE_GETS.fetch_add(rep.gets, Relaxed);
    STAGE_WARMED.fetch_add(rep.warmed as u64, Relaxed);
    STAGE_MISSED.fetch_add(rep.missed as u64, Relaxed);
    rep.millis = t0.elapsed().as_millis() as u64;
    rep
}

/// The rows of `b` under prefix `p` inside [lo, hi) (hi None: to the end of the prefix), as a staged piece covering
/// exactly that interval: trimmed once a read relies on it, dropped after `KEEP_UNUSED_MS` if none does.
fn cut_range(b: &Block, p: &[u8], lo: &[u8], hi: Option<&[u8]>) -> Piece {
    let plo = std::cmp::max(p, lo);
    let i = b.lower_bound(plo);
    let end = runfmt::prefix_successor(p);
    let phi: Option<&[u8]> = match (hi, end.as_deref()) {
        (Some(x), Some(e)) => Some(std::cmp::min(x, e)),
        (Some(x), None) => Some(x),
        (None, e) => e,
    };
    let j = phi.map_or(b.len(), |x| b.lower_bound(x));
    Piece {
        blk: b.slice(i, j.max(i)),
        lo: Some(plo.to_vec().into_boxed_slice()),
        hi: hi.map(|x| x.to_vec().into_boxed_slice()),
        whole_slice: true,
        keep_unused: true,
        born: std::time::Instant::now(),
        relied: Mutex::new(None),
    }
}

/// Per block of a new run: for each prefix, the hull of the held intervals that meet the block (clipped to the block
/// by the cut), and whether the block is wanted whole.
type Want = BTreeMap<usize, (BTreeMap<Vec<u8>, Interval>, bool)>;

async fn warm(store: &Arc<dyn ObjectStore>, fresh: &[Arc<super::view::RunH>], prefixes: &BTreeMap<Vec<u8>, Vec<Interval>>, ranges: &[(Vec<u8>, Vec<u8>)], cap: usize, rep: &mut StageReport) {
    let mut plan: Vec<(Arc<Run>, Want)> = Vec::new();
    rep.wanted = rep.held; // until the plan is known, everything held counts as wanted
    let mut wanted = 0usize;
    for h in fresh {
        let Ok(run) = h.open(store).await else { continue };
        let mut want: Want = BTreeMap::new();
        for (p, ivs) in prefixes {
            let end = runfmt::prefix_successor(p);
            if !run.may_hold(p, end.as_deref()) {
                continue; // the run's key bounds or its customer filter rule it out
            }
            for (lo, hi) in ivs {
                let top = hi.clone().or_else(|| end.clone());
                let Some((a, z)) = run.fence.blocks_for(lo, top.as_deref()) else { continue };
                for b in a..=z {
                    let e = want.entry(b).or_default();
                    match e.0.get_mut(p) {
                        None => {
                            e.0.insert(p.clone(), (lo.clone(), hi.clone()));
                            wanted += 1;
                        }
                        Some(x) => {
                            x.0 = std::cmp::min(x.0.clone(), lo.clone());
                            x.1 = match (&x.1, hi) {
                                (None, _) | (_, None) => None,
                                (Some(u), Some(v)) => Some(std::cmp::max(u.clone(), v.clone())),
                            };
                        }
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
    // then contiguous blocks in one range GET, at most 64 blocks each, until the byte cap
    for (run, want) in &plan {
        let blocks: Vec<usize> = want.keys().copied().collect();
        let mut i = 0;
        while i < blocks.len() {
            if rep.warmed_bytes >= cap {
                rep.capped = true;
                return;
            }
            let mut j = i;
            while j + 1 < blocks.len() && blocks[j + 1] == blocks[j] + 1 && j + 1 - i < 64 {
                j += 1;
            }
            if let Ok((n, bytes)) = warm_blocks(store, run, blocks[i], blocks[j], want).await {
                rep.warmed += n;
                rep.warmed_bytes += bytes;
            }
            i = j + 1;
        }
    }
}

/// Tests only: a delay before each warm-up GET (a slow store), to drive a staged publish past its deadline.
#[cfg(test)]
pub static TEST_WARM_DELAY_MS: AtomicU64 = AtomicU64::new(0);

async fn warm_blocks(store: &Arc<dyn ObjectStore>, run: &Run, a: usize, z: usize, want: &Want) -> Result<(usize, usize)> {
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
    let (mut n, mut bytes) = (0, 0);
    for b in a..=z {
        let Some((ps, whole)) = want.get(&b) else { continue };
        let (x, y) = run.fence.range(b, b);
        let blk = runfmt::decode_checked(run, b, &data[(x - lo) as usize..(y - lo) as usize])?;
        for (p, (ilo, ihi)) in ps {
            let k = CKey::Slice(run.run_id, b as u32, p.clone().into_boxed_slice());
            let piece = Arc::new(cut_range(&blk, p, ilo, ihi.as_deref()));
            bytes += cost(&k, &piece);
            cache_put(k, piece);
            n += 1;
        }
        if *whole {
            let k = CKey::Block(run.run_id, b as u32);
            let piece = Arc::new(Piece::new(blk, false));
            bytes += cost(&k, &piece);
            cache_put(k, piece);
            n += 1;
        }
    }
    Ok((n, bytes))
}
