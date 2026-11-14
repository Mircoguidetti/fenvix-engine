//! First course (Fable olix-transport idea 1): measurement prototype, off by default (`--first-course`).
//!
//! When a user (one customer of a customer-keyed collection) starts a visit, the waiter keeps the *opening*: the
//! key-value pairs that the visit's first query read. On the user's next visit, the first query is answered from the
//! opening with no object-store read, but only where the opening can give exactly the answer storage would give;
//! every other read takes the normal path.
//!
//! What is kept: the logical pairs, not the SST blocks. An opening holds
//! - `map`: keys with their value (`None` = the key does not exist), each of them known exactly;
//! - `ivs`: key intervals the first query scanned completely: inside an interval a key missing from `map` does not exist;
//! - `regions`: the user's own key ranges (its document range `doc/{coll}/{customer}` and, for every index whose first
//!   field is the customer field, `idx/{coll}/{index}/{customer}`). Every key in `map` and every interval lies inside
//!   the regions, otherwise the opening is not kept.
//!
//! Why the answers stay exact:
//! - Reads and writes of one database never overlap (a read holds the database's shared lock, every write is staged
//!   under its exclusive lock, `dbs.rs`). So a recording sees one state, and every write after it passes through
//!   [`write_hook`] (called from `Entry::apply_mode`, the only place that stages writes) before any later read runs.
//! - Every staged key that falls in a region of an opening is applied to that opening's `map` (put or delete). So for a
//!   key in `map` the value is the latest one, and an interval stays complete.
//! - Region keys are prefix-free (`custkey::comp`, `keyenc::component`): the regions of different users never overlap,
//!   so one lookup finds the only opening a written key can belong to.
//! - Compaction rewrites files but never changes a key's value, so it does not touch openings (no GET, no PUT, no CPU).
//! - Anything that could change data outside that path forgets the database's openings: a metadata write (index,
//!   collection options, drop), a failed stage, and every open or close of the tenant (wake, idle close, lost lease,
//!   drop). A database read with durable reads is served only when every staged write is durable
//!   (`Tenant::all_durable`), so the opening never shows a write that a normal read would not see yet.
//!
//! Size: an opening grows only by the writes to its own user between visits. Above `MAX_BYTES` it is trimmed to the
//! number of entries its first query needed (keeping only exact, complete prefixes of its intervals), and dropped if
//! that is not enough. All openings together stay under `BUDGET_BYTES` (the least recently visited go first) and an
//! opening older than `MAX_AGE_SECS` is not used.
//!
//! Pending openings (`QUERIES` > 1: the opening a visit is still building, which may wait until the user's next visit)
//! obey the same rules: the same cap with the same trim and drop after every write and every recorded query, counted
//! in the same budget (`STATS.bytes`), and evicted in the same least-recently-used order. Dropping a pending opening is
//! always safe: the user's next visit just finds no opening (or the previous one) and reads storage.
//!
//! Eviction order: every opening and pending opening carries a sequence number taken when it was last visited; `LRU`
//! maps sequence numbers to their owner, so the oldest is found in O(log n). Entries whose owner was removed or
//! visited again are stale: they are skipped when popped and swept when they outnumber the live ones.

use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub static ENABLED: AtomicBool = AtomicBool::new(false);
/// A query of a user not seen for this long starts a visit.
pub static IDLE_SECS: AtomicU64 = AtomicU64::new(1800);
/// An opening not refreshed by a visit for this long is not used (T of the idea, default 14 days).
pub static MAX_AGE_SECS: AtomicU64 = AtomicU64::new(14 * 86_400);
/// All openings together (RAM in this prototype; the floor, NVMe, in the design).
pub static BUDGET_BYTES: AtomicU64 = AtomicU64::new(1 << 30);
/// How many queries of a visit the opening keeps (1: the first query only, the idea as specified). With N > 1 the
/// opening of the next visit is what the first N queries of this visit read, and those N queries of the next visit may
/// all be answered from it.
pub static QUERIES: AtomicU64 = AtomicU64::new(1);
/// One opening; above this it is trimmed, then dropped.
pub static MAX_BYTES: AtomicU64 = AtomicU64::new(256 << 10);

/// Counters (serverStatus `fenvix.firstCourse`).
pub struct Stats {
    pub first_queries: AtomicU64,
    pub served_queries: AtomicU64,
    pub partial_queries: AtomicU64,
    pub missed_queries: AtomicU64,
    pub no_opening: AtomicU64,
    pub not_durable: AtomicU64,
    pub served_reads: AtomicU64,
    pub missed_reads: AtomicU64,
    pub kept: AtomicU64,
    pub unkeepable: AtomicU64,
    pub trimmed: AtomicU64,
    pub dropped_too_big: AtomicU64,
    pub evicted: AtomicU64,
    pub expired: AtomicU64,
    pub forgotten: AtomicU64,
    pub writes_applied: AtomicU64,
    pub write_hook_ns: AtomicU64,
    pub record_ns: AtomicU64,
    /// bytes of every opening and every pending opening (what the budget limits)
    pub bytes: AtomicU64,
    pub openings: AtomicU64,
    pub pending: AtomicU64,
    pub evicted_pending: AtomicU64,
    pub dropped_pending_too_big: AtomicU64,
}

pub static STATS: Stats = Stats {
    first_queries: AtomicU64::new(0),
    served_queries: AtomicU64::new(0),
    partial_queries: AtomicU64::new(0),
    missed_queries: AtomicU64::new(0),
    no_opening: AtomicU64::new(0),
    not_durable: AtomicU64::new(0),
    served_reads: AtomicU64::new(0),
    missed_reads: AtomicU64::new(0),
    kept: AtomicU64::new(0),
    unkeepable: AtomicU64::new(0),
    trimmed: AtomicU64::new(0),
    dropped_too_big: AtomicU64::new(0),
    evicted: AtomicU64::new(0),
    expired: AtomicU64::new(0),
    forgotten: AtomicU64::new(0),
    writes_applied: AtomicU64::new(0),
    write_hook_ns: AtomicU64::new(0),
    record_ns: AtomicU64::new(0),
    bytes: AtomicU64::new(0),
    openings: AtomicU64::new(0),
    pending: AtomicU64::new(0),
    evicted_pending: AtomicU64::new(0),
    dropped_pending_too_big: AtomicU64::new(0),
};

/// One user: a collection and the customer component of the user's key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Cust {
    pub coll: String,
    pub comp: Vec<u8>,
}

/// A half-open key interval `[lo, hi)` (`hi` `None` = unbounded) that a scan read completely. `keep` and `rev`: how many
/// entries the scan returned and in which direction, so a trim keeps the part the query needs.
#[derive(Clone, Debug)]
struct Iv {
    lo: Vec<u8>,
    hi: Option<Vec<u8>>,
    keep: usize,
    rev: bool,
}

impl Iv {
    fn contains(&self, k: &[u8]) -> bool {
        k >= &self.lo[..] && self.hi.as_deref().map_or(true, |h| k < h)
    }
}

/// The smallest key after `k`.
fn succ(k: &[u8]) -> Vec<u8> {
    let mut v = k.to_vec();
    v.push(0);
    v
}

fn le_hi(a: &Option<Vec<u8>>, b: &Option<Vec<u8>>) -> bool {
    match (a, b) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(x), Some(y)) => x <= y,
    }
}

#[derive(Clone, Debug, Default)]
pub struct Opening {
    map: BTreeMap<Vec<u8>, Option<Bytes>>,
    ivs: Vec<Iv>,
    /// `[start, end)` of the user's key ranges.
    regions: Vec<(Vec<u8>, Vec<u8>)>,
    /// The collection's document prefix `doc/{coll}/` (index entries end with the document suffix).
    doc_prefix: Vec<u8>,
    bytes: usize,
    visit: Option<Instant>,
    /// position in `LRU` (0: none)
    lru: u64,
}

impl Opening {
    pub fn new(regions: Vec<(Vec<u8>, Vec<u8>)>, doc_prefix: Vec<u8>) -> Self {
        Opening { regions, doc_prefix, ..Default::default() }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn entries(&self) -> usize {
        self.map.len()
    }

    fn recount(&mut self) {
        self.bytes = self.map.iter().map(|(k, v)| k.len() + v.as_ref().map_or(0, |v| v.len()) + 16).sum::<usize>()
            + self.ivs.iter().map(|i| i.lo.len() + i.hi.as_ref().map_or(0, |h| h.len()) + 32).sum::<usize>();
    }

    /// The complete intervals merged into disjoint, sorted ones.
    fn merged(&self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let mut v: Vec<(Vec<u8>, Option<Vec<u8>>)> = self.ivs.iter().map(|i| (i.lo.clone(), i.hi.clone())).collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        let mut out: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::with_capacity(v.len());
        for (lo, hi) in v {
            if let Some(last) = out.last_mut() {
                // overlapping or touching: [a, b) and [c, d) with c <= b
                if last.1.as_ref().map_or(true, |b| &lo <= b) {
                    if !le_hi(&hi, &last.1) {
                        last.1 = hi;
                    }
                    continue;
                }
            }
            out.push((lo, hi));
        }
        out
    }

    fn covered(&self, k: &[u8]) -> bool {
        self.ivs.iter().any(|i| i.contains(k))
    }

    fn in_regions(&self, k: &[u8]) -> bool {
        self.regions.iter().any(|(s, e)| k >= &s[..] && k < &e[..])
    }

    /// Every key and interval inside the user's regions (so every later write to them is seen by [`write_hook`]).
    fn inside_regions(&self) -> bool {
        self.map.keys().all(|k| self.in_regions(k))
            && self.ivs.iter().all(|i| {
                self.regions.iter().any(|(s, e)| i.lo >= *s && i.hi.as_ref().map_or(false, |h| h <= e))
            })
    }

    /// A point read: `Some(value)` when the opening knows the exact answer, `None` when it does not.
    pub fn get(&self, k: &[u8]) -> Option<Option<Bytes>> {
        if let Some(v) = self.map.get(k) {
            return Some(v.clone());
        }
        if self.covered(k) {
            return Some(None);
        }
        None
    }

    /// A range read `[start, end)`, ascending or descending, at most `limit` pairs: `Some` only when the opening knows
    /// the exact answer.
    pub fn scan(&self, start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>) -> Option<Vec<(Bytes, Bytes)>> {
        let merged = self.merged();
        let lim = limit.unwrap_or(usize::MAX);
        if lim == 0 {
            return Some(vec![]);
        }
        let mut out = Vec::new();
        if !reverse {
            // the covered stretch that starts at `start`
            let up = merged.iter().find(|(lo, hi)| &lo[..] <= start && hi.as_deref().map_or(true, |h| start < h))?.1.clone();
            for (k, v) in self.map.range(start.to_vec()..) {
                if end.map_or(false, |e| &k[..] >= e) || up.as_deref().map_or(false, |u| &k[..] >= u) {
                    break;
                }
                if let Some(v) = v {
                    out.push((Bytes::copy_from_slice(k), v.clone()));
                    if out.len() >= lim {
                        return Some(out);
                    }
                }
            }
            // fewer than `limit`: exact only if the covered stretch reaches the end of the range
            let whole = match (end, up.as_deref()) {
                (_, None) => true,
                (None, Some(_)) => false,
                (Some(e), Some(u)) => e <= u,
            };
            return if whole { Some(out) } else { None };
        }
        // descending: the covered stretch that ends at `end`
        let down = merged
            .iter()
            .find(|(lo, hi)| match end {
                Some(e) => &lo[..] < e && hi.as_deref().map_or(true, |h| e <= h),
                None => hi.is_none(),
            })?
            .0
            .clone();
        let lower = if &down[..] > start { down.clone() } else { start.to_vec() };
        let range: Box<dyn DoubleEndedIterator<Item = (&Vec<u8>, &Option<Bytes>)>> = match end {
            Some(e) => Box::new(self.map.range(lower.clone()..e.to_vec())),
            None => Box::new(self.map.range(lower.clone()..)),
        };
        for (k, v) in range.rev() {
            if let Some(v) = v {
                out.push((Bytes::copy_from_slice(k), v.clone()));
                if out.len() >= lim {
                    return Some(out);
                }
            }
        }
        if &down[..] <= start {
            Some(out)
        } else {
            None
        }
    }

    /// Record a point read.
    pub fn note_get(&mut self, k: &[u8], v: &Option<Bytes>) {
        self.map.insert(k.to_vec(), v.clone());
    }

    /// Record a range read and its result.
    pub fn note_scan(&mut self, start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>, rows: &[(Bytes, Bytes)]) {
        for (k, v) in rows {
            self.map.insert(k.to_vec(), Some(v.clone()));
        }
        let full = limit.map_or(false, |l| rows.len() >= l) && !rows.is_empty();
        let (lo, hi) = if !full {
            (start.to_vec(), end.map(|e| e.to_vec()))
        } else if !reverse {
            (start.to_vec(), Some(succ(&rows.last().unwrap().0)))
        } else {
            (rows.last().unwrap().0.to_vec(), end.map(|e| e.to_vec()))
        };
        if limit == Some(0) {
            return;
        }
        self.ivs.push(Iv { lo, hi, keep: rows.len(), rev: reverse });
    }

    /// A write to one of the opening's regions (keeps `bytes` up to date without a full recount).
    fn apply(&mut self, k: Vec<u8>, v: Option<Bytes>) {
        let size = |k: &[u8], v: &Option<Bytes>| k.len() + v.as_ref().map_or(0, |v| v.len()) + 16;
        self.bytes += size(&k, &v);
        let kl = k.len();
        if let Some(old) = self.map.insert(k, v) {
            self.bytes -= kl + old.as_ref().map_or(0, |v| v.len()) + 16;
        }
    }

    /// Shrink to what the first query needed; exact: an interval is cut to a prefix (or suffix) that still holds all its
    /// keys, and only keys outside every interval are forgotten (they become "not known").
    fn trim(&mut self) {
        // Consecutive scans of one query (a first batch, then the next) touch each other: they are trimmed together, as
        // one stretch keeping as many entries as they returned together, so no hole opens between them.
        let mut ivs = std::mem::take(&mut self.ivs);
        ivs.sort_by(|a, b| a.lo.cmp(&b.lo));
        let mut groups: Vec<Vec<Iv>> = Vec::new();
        for iv in ivs {
            let joins = groups.last().map_or(false, |g| g.iter().any(|x| x.hi.as_ref().map_or(true, |h| &iv.lo <= h)));
            if joins {
                groups.last_mut().unwrap().push(iv);
            } else {
                groups.push(vec![iv]);
            }
        }
        let mut out = Vec::new();
        for g in groups {
            let fwd = g.iter().all(|i| !i.rev);
            let rev = g.iter().all(|i| i.rev);
            if !fwd && !rev {
                out.extend(g); // mixed directions: left as recorded
                continue;
            }
            let lo = g.iter().map(|i| i.lo.clone()).min().unwrap();
            let hi = g.iter().map(|i| i.hi.clone()).reduce(|a, b| if le_hi(&a, &b) { b } else { a }).unwrap();
            let keep = g.iter().map(|i| i.keep).sum::<usize>().max(1);
            let present: Vec<Vec<u8>> = {
                let range: Box<dyn DoubleEndedIterator<Item = (&Vec<u8>, &Option<Bytes>)>> = match &hi {
                    Some(h) => Box::new(self.map.range(lo.clone()..h.clone())),
                    None => Box::new(self.map.range(lo.clone()..)),
                };
                if rev {
                    range.rev().filter(|(_, v)| v.is_some()).map(|(k, _)| k.clone()).take(keep).collect()
                } else {
                    range.filter(|(_, v)| v.is_some()).map(|(k, _)| k.clone()).take(keep).collect()
                }
            };
            if present.len() < keep {
                out.extend(g);
                continue;
            }
            let edge = present.last().unwrap().clone();
            out.push(if rev { Iv { lo: edge, hi, keep, rev: true } } else { Iv { lo, hi: Some(succ(&edge)), keep, rev: false } });
        }
        self.ivs = out;
        // documents still pointed to by an index entry the opening keeps
        let mut wanted: HashSet<Vec<u8>> = HashSet::new();
        for (k, v) in &self.map {
            if v.is_some() && k.starts_with(b"idx/") && self.covered(k) {
                if let Some(suf) = crate::index::entry_id(k) {
                    let mut d = self.doc_prefix.clone();
                    d.extend_from_slice(suf);
                    wanted.insert(d);
                }
            }
        }
        let drop: Vec<Vec<u8>> = self.map.keys().filter(|k| !self.covered(k) && !wanted.contains(*k)).cloned().collect();
        for k in drop {
            self.map.remove(&k);
        }
        self.recount();
    }

    /// The cap rule, the same for finished and pending openings: above `MAX_BYTES` trim; `false` when the trim was not
    /// enough and the opening must be dropped.
    fn fit(&mut self) -> bool {
        let max = MAX_BYTES.load(Relaxed) as usize;
        if self.bytes > max {
            self.trim();
            STATS.trimmed.fetch_add(1, Relaxed);
        }
        self.bytes <= max
    }
}

/// The openings of one database.
#[derive(Default)]
struct DbOpenings {
    /// the database's name (the owner recorded in `LRU`)
    name: String,
    openings: HashMap<Cust, Opening>,
    /// region start -> (region end, user); regions never overlap
    regions: BTreeMap<Vec<u8>, (Vec<u8>, Cust)>,
    /// last query of each user
    access: HashMap<Cust, Instant>,
    /// `QUERIES` > 1: the opening a visit is building (queries begun, queries recorded); writes follow it like an
    /// opening, and it obeys the same cap and budget; it becomes the user's opening after its N-th query or when the
    /// next visit starts
    pending: HashMap<Cust, (Opening, u64, u64)>,
    accesses: u64,
}

/// Least recently visited first: sequence number -> (database, user, pending). See the module comment.
static LRU: std::sync::LazyLock<Mutex<BTreeMap<u64, (String, Cust, bool)>>> = std::sync::LazyLock::new(|| Mutex::new(BTreeMap::new()));
static SEQ: AtomicU64 = AtomicU64::new(1);

/// A new position in `LRU` for an opening (finished or pending) visited now. Called with `REG` held (lock order: `REG`
/// then `LRU`).
fn touch(db: &str, c: &Cust, pending: bool, o: &mut Opening) {
    let seq = SEQ.fetch_add(1, Relaxed);
    let mut lru = LRU.lock().unwrap();
    if o.lru != 0 {
        lru.remove(&o.lru);
    }
    lru.insert(seq, (db.to_string(), c.clone(), pending));
    o.lru = seq;
}

fn untouch(o: &Opening) {
    if o.lru != 0 {
        LRU.lock().unwrap().remove(&o.lru);
    }
}

/// Adjust the budget's byte count after an opening changed size in place.
fn resize(before: usize, after: usize) {
    if after >= before {
        STATS.bytes.fetch_add((after - before) as u64, Relaxed);
    } else {
        STATS.bytes.fetch_sub((before - after) as u64, Relaxed);
    }
}

impl DbOpenings {
    fn new(name: &str) -> Self {
        DbOpenings { name: name.to_string(), ..Default::default() }
    }

    fn owner(&self, k: &[u8]) -> Option<&Cust> {
        let (_, (end, c)) = self.regions.range(..=k.to_vec()).next_back()?;
        if k < &end[..] {
            Some(c)
        } else {
            None
        }
    }

    fn remove(&mut self, c: &Cust) -> Option<Opening> {
        let o = self.openings.remove(c)?;
        if !self.pending.contains_key(c) {
            self.unclaim(c, &o.regions);
        }
        untouch(&o);
        STATS.bytes.fetch_sub(o.bytes as u64, Relaxed);
        STATS.openings.fetch_sub(1, Relaxed);
        Some(o)
    }

    fn unclaim(&mut self, c: &Cust, regions: &[(Vec<u8>, Vec<u8>)]) {
        for (s, _) in regions {
            if self.regions.get(s).map_or(false, |(_, owner)| owner == c) {
                self.regions.remove(s);
            }
        }
    }

    fn claim(&mut self, c: &Cust, regions: &[(Vec<u8>, Vec<u8>)]) {
        for (s, e) in regions {
            self.regions.insert(s.clone(), (e.clone(), c.clone()));
        }
    }

    /// A new, empty pending opening for a visit that starts now.
    fn start_pending(&mut self, c: &Cust, mut p: Opening) {
        self.drop_pending(c);
        self.claim(c, &p.regions);
        p.visit = Some(Instant::now());
        p.recount();
        touch(&self.name, c, true, &mut p);
        STATS.bytes.fetch_add(p.bytes as u64, Relaxed);
        STATS.pending.fetch_add(1, Relaxed);
        self.pending.insert(c.clone(), (p, 1, 0));
    }

    /// Take the pending opening out of the bookkeeping (its bytes leave the budget, its regions stay claimed only when
    /// the user still has a finished opening).
    fn take_pending(&mut self, c: &Cust) -> Option<(Opening, u64, u64)> {
        let p = self.pending.remove(c)?;
        if !self.openings.contains_key(c) {
            self.unclaim(c, &p.0.regions);
        }
        untouch(&p.0);
        STATS.bytes.fetch_sub(p.0.bytes as u64, Relaxed);
        STATS.pending.fetch_sub(1, Relaxed);
        Some(p)
    }

    /// Dropping a pending opening is always safe: the user's next visit reads storage.
    fn drop_pending(&mut self, c: &Cust) {
        self.take_pending(c);
    }

    /// After a pending opening changed (writes applied or a query recorded, its `bytes` exact): the same cap rule as
    /// finished openings, and the budget count follows.
    fn settle_pending(&mut self, c: &Cust, before: usize) {
        let Some(p) = self.pending.get_mut(c) else { return };
        let keep = p.0.fit();
        resize(before, p.0.bytes);
        if !keep {
            STATS.dropped_pending_too_big.fetch_add(1, Relaxed);
            self.drop_pending(c);
        }
    }

    /// The visit's pending opening becomes the user's opening (trimmed or dropped when too big).
    fn install_pending(&mut self, c: &Cust) {
        let Some((mut p, _, _)) = self.take_pending(c) else { return };
        p.visit = Some(Instant::now());
        p.lru = 0;
        p.recount();
        if !p.fit() {
            STATS.dropped_too_big.fetch_add(1, Relaxed);
            let regions = p.regions.clone();
            self.remove(c);
            self.unclaim(c, &regions);
            return;
        }
        self.insert(c.clone(), p);
        STATS.kept.fetch_add(1, Relaxed);
    }

    fn insert(&mut self, c: Cust, mut o: Opening) {
        self.remove(&c);
        for (s, e) in &o.regions {
            self.regions.insert(s.clone(), (e.clone(), c.clone()));
        }
        touch(&self.name, &c, false, &mut o);
        STATS.bytes.fetch_add(o.bytes as u64, Relaxed);
        STATS.openings.fetch_add(1, Relaxed);
        self.openings.insert(c, o);
    }
}

static REG: std::sync::LazyLock<Mutex<HashMap<String, DbOpenings>>> = std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// One first query of a visit: what it may serve from, and what it records.
pub struct Session {
    db: String,
    cust: Cust,
    serve: Option<Opening>,
    rec: Opening,
    served: u64,
    missed: u64,
    record_ns: u64,
    /// first-course-short: the customer's box was skipped because this session answers from an opening; it is due at the
    /// first read the opening cannot answer (so a partly answered query still makes one trip, not one per read)
    box_due: bool,
}

impl Session {
    pub fn get(&mut self, k: &[u8]) -> Option<Option<Bytes>> {
        let r = self.serve.as_ref().and_then(|o| o.get(k));
        if r.is_none() && self.serve.is_some() && *TRACE {
            eprintln!("first course: miss get {}", String::from_utf8_lossy(k));
        }
        self.count(r.is_some());
        r
    }

    pub fn scan(&mut self, start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>) -> Option<Vec<(Bytes, Bytes)>> {
        let r = self.serve.as_ref().and_then(|o| o.scan(start, end, reverse, limit));
        if r.is_none() && self.serve.is_some() && *TRACE {
            eprintln!("first course: miss scan {} .. {:?} rev {reverse} limit {limit:?}", String::from_utf8_lossy(start), end.map(String::from_utf8_lossy));
        }
        self.count(r.is_some());
        r
    }

    fn count(&mut self, hit: bool) {
        if !hit && self.serve.is_some() && *TRACE {
            eprintln!("first course: miss");
        }
        if self.serve.is_none() {
            return;
        }
        if hit {
            self.served += 1;
            STATS.served_reads.fetch_add(1, Relaxed);
        } else {
            self.missed += 1;
            STATS.missed_reads.fetch_add(1, Relaxed);
        }
    }

    pub fn note_get(&mut self, k: &[u8], v: &Option<Bytes>) {
        let t = Instant::now();
        self.rec.note_get(k, v);
        self.record_ns += t.elapsed().as_nanos() as u64;
    }

    pub fn note_scan(&mut self, start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>, rows: &[(Bytes, Bytes)]) {
        let t = Instant::now();
        self.rec.note_scan(start, end, reverse, limit, rows);
        self.record_ns += t.elapsed().as_nanos() as u64;
    }
}

/// The customer value of a filter that pins one customer (an equality on the customer field), or `None`.
pub fn customer_value(field: &str, filter: &bson::Document) -> Option<bson::Bson> {
    use bson::Bson;
    let v = match filter.get(field)? {
        Bson::Document(d) if d.len() == 1 && d.contains_key("$eq") => d.get("$eq")?.clone(),
        Bson::Document(_) => return None,
        v => v.clone(),
    };
    match &v {
        Bson::Int32(_) | Bson::Int64(_) | Bson::String(_) | Bson::ObjectId(_) => Some(v),
        Bson::Double(f) if f.is_finite() => Some(v),
        _ => None,
    }
}

/// The regions of a user: its documents and its entries in every index that starts with the customer field.
fn regions_of(coll: &str, field: &str, v: &bson::Bson, indexes: &[crate::index::IndexDef]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    // layout v2 (custkey.rs): a customer-keyed document key is the collection's prefix, the mark, then the customer
    // component (compaction 2 step 6: without the mark every find read outside the user's region and was not kept)
    let mut d = crate::layout::doc_prefix(coll);
    d.push(crate::custkey::MARK);
    d.extend(crate::custkey::comp(v));
    out.push(d);
    for ix in indexes {
        if !ix.has_entries() || !matches!(ix.kind, crate::index::Kind::Regular) || ix.collation.is_some() {
            continue;
        }
        if let Some((f, asc)) = ix.key.first() {
            if f == field {
                let mut p = ix.prefix(coll);
                p.extend(crate::keyenc::component(v, *asc));
                out.push(p);
            }
        }
    }
    out.into_iter().filter_map(|s| crate::keyenc::prefix_end(&s).map(|e| (s, e))).collect()
}

/// Start of a query: a session when this find is the first query of a visit of one user (an equality on the
/// collection's customer field), else `None`.
pub fn begin(db: &str, coll: &str, field: &str, filter: &bson::Document, indexes: &[crate::index::IndexDef], all_durable: bool) -> Option<Session> {
    if !ENABLED.load(Relaxed) {
        return None;
    }
    let v = customer_value(field, filter)?;
    let cust = Cust { coll: coll.to_string(), comp: crate::custkey::comp(&v) };
    let regions = regions_of(coll, field, &v, indexes);
    let now = Instant::now();
    let idle = Duration::from_secs(IDLE_SECS.load(Relaxed));
    let mut reg = REG.lock().unwrap();
    let st = reg.entry(db.to_string()).or_insert_with(|| DbOpenings::new(db));
    let last = st.access.insert(cust.clone(), now);
    st.accesses += 1;
    if st.accesses % 4096 == 0 {
        st.access.retain(|_, t| now.duration_since(*t) < idle);
    }
    let n = QUERIES.load(Relaxed).max(1);
    let mk = |serve: Option<Opening>| Session { db: db.to_string(), cust: cust.clone(), box_due: serve.is_some(), serve, rec: Opening::new(regions.clone(), crate::layout::doc_prefix(coll)), served: 0, missed: 0, record_ns: 0 };
    if last.map_or(false, |t| now.duration_since(t) < idle) {
        // a later query of the visit: with N > 1 the first N queries are answered from (and recorded for) the opening
        let more = st.pending.get_mut(&cust).map_or(false, |p| {
            p.1 += 1;
            p.1 <= n
        });
        if !more || !all_durable {
            if more {
                st.drop_pending(&cust);
            }
            return None;
        }
        return Some(mk(st.openings.get(&cust).cloned()));
    }
    if st.pending.contains_key(&cust) {
        // the previous visit had fewer than N queries: what it read becomes the opening now
        st.install_pending(&cust);
    }
    STATS.first_queries.fetch_add(1, Relaxed);
    if !all_durable {
        // a staged write is not durable yet: a normal read would not see it, the opening would (and a recording now
        // would miss it for good)
        STATS.not_durable.fetch_add(1, Relaxed);
        return None;
    }
    let max_age = Duration::from_secs(MAX_AGE_SECS.load(Relaxed));
    let fresh = st.openings.get(&cust).map(|o| o.visit.map_or(false, |t| now.duration_since(t) <= max_age));
    if fresh == Some(false) {
        st.remove(&cust);
        STATS.expired.fetch_add(1, Relaxed);
    }
    let serve = st.openings.get(&cust).cloned();
    if n > 1 {
        st.start_pending(&cust, Opening::new(regions.clone(), crate::layout::doc_prefix(coll)));
    }
    Some(mk(serve))
}

/// End of the first query: keep what it read as the user's opening (when the query succeeded and the opening is
/// exact and small enough).
pub fn finish(s: Session, ok: bool) {
    let Session { db, cust, serve, mut rec, served, missed, record_ns, .. } = s;
    STATS.record_ns.fetch_add(record_ns, Relaxed);
    match (&serve, served, missed) {
        (None, _, _) => STATS.no_opening.fetch_add(1, Relaxed),
        (Some(_), s, 0) if s > 0 => STATS.served_queries.fetch_add(1, Relaxed),
        (Some(_), s, _) if s > 0 => STATS.partial_queries.fetch_add(1, Relaxed),
        _ => STATS.missed_queries.fetch_add(1, Relaxed),
    };
    if !ENABLED.load(Relaxed) {
        return;
    }
    if QUERIES.load(Relaxed) > 1 {
        let mut reg = REG.lock().unwrap();
        let Some(st) = reg.get_mut(&db) else { return };
        if !st.pending.contains_key(&cust) {
            return; // forgotten meanwhile
        }
        if !ok || !rec.inside_regions() {
            if ok {
                STATS.unkeepable.fetch_add(1, Relaxed);
            }
            st.drop_pending(&cust);
            return;
        }
        // both are exact now (the pending one followed every write since its queries): their union is exact
        let name = st.name.clone();
        let p = st.pending.get_mut(&cust).unwrap();
        let before = p.0.bytes;
        p.0.map.extend(rec.map);
        p.0.ivs.extend(rec.ivs);
        p.0.recount();
        p.0.visit = Some(Instant::now());
        touch(&name, &cust, true, &mut p.0);
        p.2 += 1;
        if p.2 >= QUERIES.load(Relaxed) {
            resize(before, p.0.bytes);
            st.install_pending(&cust);
        } else {
            st.settle_pending(&cust, before);
        }
        evict(&mut reg);
        return;
    }
    if !ok {
        return;
    }
    if rec.map.is_empty() && rec.ivs.is_empty() {
        return;
    }
    if !rec.inside_regions() {
        if *TRACE {
            let k = rec.map.keys().find(|k| !rec.in_regions(k)).map(|k| String::from_utf8_lossy(k).to_string());
            let iv = rec.ivs.iter().find(|i| !rec.regions.iter().any(|(s, e)| i.lo >= *s && i.hi.as_ref().map_or(false, |h| h <= e)));
            eprintln!("first course: unkeepable, key {k:?}, interval {:?}", iv.map(|i| (String::from_utf8_lossy(&i.lo).to_string(), i.hi.as_ref().map(|h| String::from_utf8_lossy(h).to_string()))));
        }
        // the query read keys of other users or of an index not ordered by user: writes there would not be seen
        STATS.unkeepable.fetch_add(1, Relaxed);
        return;
    }
    rec.visit = Some(Instant::now());
    rec.recount();
    if !rec.fit() {
        STATS.dropped_too_big.fetch_add(1, Relaxed);
        return;
    }
    let mut reg = REG.lock().unwrap();
    reg.entry(db.clone()).or_insert_with(|| DbOpenings::new(&db)).insert(cust, rec);
    STATS.kept.fetch_add(1, Relaxed);
    evict(&mut reg);
}

/// Keep every opening and pending opening together inside `BUDGET_BYTES`: the least recently visited go first.
fn evict(reg: &mut HashMap<String, DbOpenings>) {
    let budget = BUDGET_BYTES.load(Relaxed);
    while STATS.bytes.load(Relaxed) > budget {
        let Some((seq, (db, c, pending))) = LRU.lock().unwrap().pop_first() else { break };
        let Some(st) = reg.get_mut(&db) else { continue };
        if pending {
            if st.pending.get(&c).map_or(false, |p| p.0.lru == seq) {
                st.pending.get_mut(&c).unwrap().0.lru = 0;
                st.drop_pending(&c);
                STATS.evicted_pending.fetch_add(1, Relaxed);
            }
        } else if st.openings.get(&c).map_or(false, |o| o.lru == seq) {
            st.openings.get_mut(&c).unwrap().lru = 0;
            st.remove(&c);
            STATS.evicted.fetch_add(1, Relaxed);
        }
    }
    sweep(reg);
}

/// Drop stale `LRU` entries once they outnumber the live ones (amortized: a sweep follows at least as many changes as
/// it visits).
fn sweep(reg: &HashMap<String, DbOpenings>) {
    let live = (STATS.openings.load(Relaxed) + STATS.pending.load(Relaxed)) as usize;
    let mut lru = LRU.lock().unwrap();
    if lru.len() <= 2 * live + 1024 {
        return;
    }
    lru.retain(|seq, (db, c, pending)| {
        reg.get(db).map_or(false, |st| {
            if *pending {
                st.pending.get(c).map_or(false, |p| p.0.lru == *seq)
            } else {
                st.openings.get(c).map_or(false, |o| o.lru == *seq)
            }
        })
    });
}

fn is_meta_key(k: &[u8]) -> bool {
    k == crate::layout::META_COLLECTIONS || k.starts_with(b"meta/indexes/") || k.starts_with(b"meta/copt/")
}

/// What a write batch does to the openings, decided before it is staged.
pub enum Captured {
    Nothing,
    /// a metadata write: the database's openings are forgotten once it is staged
    Forget,
    Apply(Vec<(Vec<u8>, Option<Bytes>)>),
}

/// `FENVIX_FIRST_COURSE_TRACE=1`: why openings are not kept (stderr; debugging only).
static TRACE: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| std::env::var("FENVIX_FIRST_COURSE_TRACE").map_or(false, |v| v == "1"));

/// Negative control of the tests (`FENVIX_FIRST_COURSE_NEGATIVE_CONTROL=1`): writes do not follow the openings, so
/// answers from an opening go stale. The stress test must then find wrong answers. Never set outside tests.
static NEGATIVE_CONTROL: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
    let on = std::env::var("FENVIX_FIRST_COURSE_NEGATIVE_CONTROL").map_or(false, |v| v == "1");
    if on {
        eprintln!("FIRST COURSE NEGATIVE CONTROL ON: openings ignore writes (test only, answers can be wrong)");
    }
    on
});

/// Before a batch is staged: the writes that fall in some opening's regions.
pub fn write_hook(db: &str, ops: &[tenant_bench::lifecycle::Op]) -> Captured {
    use tenant_bench::lifecycle::Op;
    if !ENABLED.load(Relaxed) || *NEGATIVE_CONTROL {
        return Captured::Nothing;
    }
    let t = Instant::now();
    let reg = REG.lock().unwrap();
    let Some(st) = reg.get(db) else { return Captured::Nothing };
    if st.openings.is_empty() && st.pending.is_empty() {
        return Captured::Nothing;
    }
    let mut out = Vec::new();
    for op in ops {
        let (k, v) = match op {
            Op::Put(k, v) => (k, Some(v)),
            Op::Delete(k) => (k, None),
        };
        if is_meta_key(k) {
            STATS.write_hook_ns.fetch_add(t.elapsed().as_nanos() as u64, Relaxed);
            return Captured::Forget;
        }
        if st.owner(k).is_some() {
            out.push((k.clone(), v.map(|v| Bytes::copy_from_slice(v))));
        }
    }
    STATS.write_hook_ns.fetch_add(t.elapsed().as_nanos() as u64, Relaxed);
    if out.is_empty() {
        Captured::Nothing
    } else {
        Captured::Apply(out)
    }
}

/// After the batch was staged (`ok`) or failed to stage.
pub fn after_stage(db: &str, c: Captured, ok: bool) {
    match (c, ok) {
        (Captured::Nothing, true) => {}
        (Captured::Apply(w), true) => {
            let t = Instant::now();
            let mut reg = REG.lock().unwrap();
            if let Some(st) = reg.get_mut(db) {
                // the whole batch first, then the size check: a trim between a new document and its index entry (same
                // batch) would forget the document the entry points to
                // (user, bytes of its opening before, bytes of its pending opening before)
                let mut touched: Vec<(Cust, Option<usize>, Option<usize>)> = Vec::new();
                for (k, v) in w {
                    let Some(c) = st.owner(&k).cloned() else { continue };
                    if !touched.iter().any(|(t, _, _)| *t == c) {
                        touched.push((c.clone(), st.openings.get(&c).map(|o| o.bytes), st.pending.get(&c).map(|p| p.0.bytes)));
                    }
                    if let Some(p) = st.pending.get_mut(&c) {
                        p.0.apply(k.clone(), v.clone());
                    }
                    let Some(o) = st.openings.get_mut(&c) else { continue };
                    o.apply(k, v);
                    STATS.writes_applied.fetch_add(1, Relaxed);
                }
                for (c, before, pending_before) in touched {
                    if let (Some(before), Some(o)) = (before, st.openings.get_mut(&c)) {
                        let keep = o.fit();
                        resize(before, o.bytes);
                        if !keep {
                            st.remove(&c);
                            STATS.dropped_too_big.fetch_add(1, Relaxed);
                        }
                    }
                    if let Some(before) = pending_before {
                        st.settle_pending(&c, before);
                    }
                }
            }
            // writes make openings grow: the budget holds after every batch, not only when an opening is kept
            evict(&mut reg);
            STATS.write_hook_ns.fetch_add(t.elapsed().as_nanos() as u64, Relaxed);
        }
        _ => forget_db(db),
    }
}


/// Size in bytes and entries of every opening of a database (diagnostics: `{fenvixFirstCourse: "<db>"}`).
pub fn sizes(db: &str) -> Vec<(usize, usize)> {
    let reg = REG.lock().unwrap();
    reg.get(db).map(|st| st.openings.values().map(|o| (o.bytes, o.map.len())).collect()).unwrap_or_default()
}

/// Forget every opening of a database (its tenant opened or closed, or a write that the openings cannot follow).
pub fn forget_db(db: &str) {
    if !ENABLED.load(Relaxed) {
        return;
    }
    let mut reg = REG.lock().unwrap();
    if let Some(st) = reg.get_mut(db) {
        let ps: Vec<Cust> = st.pending.keys().cloned().collect();
        for c in ps {
            st.drop_pending(&c);
        }
        let cs: Vec<Cust> = st.openings.keys().cloned().collect();
        for c in cs {
            st.remove(&c);
            STATS.forgotten.fetch_add(1, Relaxed);
        }
        st.regions.clear();
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// first-course-short: openings that outlive a close of their database (the pause between two visits)
// ---------------------------------------------------------------------------------------------------------------------
//
// Without this, every open or close of the tenant forgets the database's openings (`forget_db`): an idle close, a
// restart or a deploy between two visits leaves the returning customer with no opening, and its first read pays the
// full object-store trip. With `--first-course-dir DIR`:
// - at a clean close (`park_db` then `persist`): the database's openings, pending ones installed as the next visit
//   would install them, are written to `DIR/<hex db>.fc` (temp file, fsync, rename) once the tenant has stopped and the
//   lease release has landed, stamped with the lease sequence that release wrote. Every write accepted until the close
//   went through `write_hook`, so the openings are exact for the database as it was closed.
// - at the next open as owner (`restore_db`): the file is read and deleted (it is used at most once). It is loaded only
//   when no other process can have owned, and so written, the database since: the acquire was not a takeover, and the
//   lease this open acquired has exactly the sequence after the stamped release (the same rule as
//   `Leases::continuous`, made to survive a restart). Whether the wake replays the WAL does not matter: every write
//   this process acknowledged is in the opening and in the store alike, and nobody else wrote. Anything else (a takeover, a close that did not finish, a
//   lapsed or failed release, a lease record recreated) leaves the file unused: the customer reads storage, as today.
// - a corrupt or truncated file fails its checksum and is ignored.

/// Measurement only: load kept openings without the ownership check (`FENVIX_FIRST_COURSE_SHORT_NEGATIVE_CONTROL=1`):
/// the safety test must then find wrong reads, or it does not reach the case it guards.
static SHORT_NEGATIVE_CONTROL: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
    let on = std::env::var("FENVIX_FIRST_COURSE_SHORT_NEGATIVE_CONTROL").map_or(false, |v| v == "1");
    if on {
        eprintln!("first course: NEGATIVE CONTROL, kept openings are loaded without the ownership check");
    }
    on
});

pub static PERSIST_DIR: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
pub static PERSISTED: AtomicU64 = AtomicU64::new(0);
pub static RESTORED: AtomicU64 = AtomicU64::new(0);
pub static RESTORE_REFUSED: AtomicU64 = AtomicU64::new(0);
pub static RESTORE_NS: AtomicU64 = AtomicU64::new(0);

fn persist_path(db: &str) -> Option<std::path::PathBuf> {
    let d = PERSIST_DIR.get()?;
    let hex: String = db.bytes().map(|b| format!("{b:02x}")).collect();
    Some(d.join(format!("{hex}.fc")))
}

fn put_b(out: &mut Vec<u8>, b: &[u8]) {
    out.extend((b.len() as u32).to_le_bytes());
    out.extend(b);
}

fn put_ob(out: &mut Vec<u8>, b: Option<&[u8]>) {
    match b {
        None => out.push(0),
        Some(b) => {
            out.push(1);
            put_b(out, b);
        }
    }
}

fn fnv(b: &[u8]) -> u64 {
    b.iter().fold(0xcbf29ce484222325u64, |h, x| (h ^ *x as u64).wrapping_mul(0x100000001b3))
}

struct Rd<'a>(&'a [u8]);

impl<'a> Rd<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.0.len() < n {
            return None;
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Some(a)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }
    fn b(&mut self) -> Option<Vec<u8>> {
        let n = self.u32()? as usize;
        Some(self.take(n)?.to_vec())
    }
    fn ob(&mut self) -> Option<Option<Vec<u8>>> {
        match self.u8()? {
            0 => Some(None),
            1 => Some(Some(self.b()?)),
            _ => None,
        }
    }
}

/// The openings of `db` (pending ones installed), encoded, and forgotten as `forget_db` does. `None` when persistence
/// is off or there is nothing to keep.
pub fn park_db(db: &str) -> Option<Vec<u8>> {
    if !ENABLED.load(Relaxed) || PERSIST_DIR.get().is_none() {
        forget_db(db);
        return None;
    }
    let body = {
        let mut reg = REG.lock().unwrap();
        reg.get_mut(db).map(|st| {
            let ps: Vec<Cust> = st.pending.keys().cloned().collect();
            for c in ps {
                st.install_pending(&c);
            }
            let now = Instant::now();
            let mut out = Vec::new();
            out.extend((st.openings.len() as u32).to_le_bytes());
            for (c, o) in &st.openings {
                put_b(&mut out, c.coll.as_bytes());
                put_b(&mut out, &c.comp);
                out.extend(o.visit.map_or(u64::MAX, |t| now.duration_since(t).as_secs()).to_le_bytes());
                put_b(&mut out, &o.doc_prefix);
                out.extend((o.regions.len() as u32).to_le_bytes());
                for (s, e) in &o.regions {
                    put_b(&mut out, s);
                    put_b(&mut out, e);
                }
                out.extend((o.ivs.len() as u32).to_le_bytes());
                for i in &o.ivs {
                    put_b(&mut out, &i.lo);
                    put_ob(&mut out, i.hi.as_deref());
                    out.extend((i.keep as u64).to_le_bytes());
                    out.push(i.rev as u8);
                }
                out.extend((o.map.len() as u32).to_le_bytes());
                for (k, v) in &o.map {
                    put_b(&mut out, k);
                    put_ob(&mut out, v.as_deref());
                }
            }
            (st.openings.len(), out)
        })
    };
    forget_db(db);
    match body {
        Some((n, out)) if n > 0 => Some(out),
        _ => None,
    }
}

/// After a clean close whose lease release landed with sequence `released_seq`: write the parked openings.
pub fn persist(db: &str, released_seq: u64, body: Vec<u8>) {
    let Some(path) = persist_path(db) else { return };
    let mut out = b"FCSHORT1".to_vec();
    out.extend(released_seq.to_le_bytes());
    out.extend(body);
    let sum = fnv(&out);
    out.extend(sum.to_le_bytes());
    let tmp = path.with_extension("tmp");
    let r = (|| -> std::io::Result<()> {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&out)?;
        f.sync_all()?;
        std::fs::rename(&tmp, &path)
    })();
    match r {
        Ok(()) => {
            PERSISTED.fetch_add(1, Relaxed);
        }
        Err(e) => {
            eprintln!("first course: could not keep the openings of {db}: {e}");
            let _ = std::fs::remove_file(&tmp);
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// Drop any kept openings of `db` (a close that is not clean, or an open that cannot use them).
pub fn discard(db: &str) {
    if let Some(p) = persist_path(db) {
        let _ = std::fs::remove_file(p);
    }
}

/// At an open as owner, after the lease acquire: load the kept openings of `db` when `held_seq` (the sequence of the
/// lease this open acquired) is the one after the stamped release and the open was clean; `leases_off`: no leases at
/// all (one process only), the stamp is not checked. The file is deleted in every case.
pub fn restore_db(db: &str, held_seq: Option<u64>, clean: bool, leases_off: bool) {
    forget_db(db);
    let Some(path) = persist_path(db) else { return };
    let Ok(buf) = std::fs::read(&path) else { return };
    let _ = std::fs::remove_file(&path);
    if !ENABLED.load(Relaxed) {
        return;
    }
    let t0 = Instant::now();
    let ok = (|| -> Option<Vec<(Cust, Opening)>> {
        if buf.len() < 24 || &buf[..8] != b"FCSHORT1" {
            return None;
        }
        let (data, sum) = buf.split_at(buf.len() - 8);
        if fnv(data) != u64::from_le_bytes(sum.try_into().ok()?) {
            return None;
        }
        let mut r = Rd(&data[8..]);
        let stamp = r.u64()?;
        if !*SHORT_NEGATIVE_CONTROL && (!clean || !(leases_off || held_seq == stamp.checked_add(1))) {
            return None;
        }
        let now = Instant::now();
        let max_age = MAX_AGE_SECS.load(Relaxed);
        let n = r.u32()?;
        let mut v = Vec::new();
        for _ in 0..n {
            let coll = String::from_utf8(r.b()?).ok()?;
            let comp = r.b()?;
            let age = r.u64()?;
            let doc_prefix = r.b()?;
            let mut regions = Vec::new();
            for _ in 0..r.u32()? {
                regions.push((r.b()?, r.b()?));
            }
            let mut ivs = Vec::new();
            for _ in 0..r.u32()? {
                ivs.push(Iv { lo: r.b()?, hi: r.ob()?, keep: r.u64()? as usize, rev: r.u8()? != 0 });
            }
            let mut map = BTreeMap::new();
            for _ in 0..r.u32()? {
                let k = r.b()?;
                map.insert(k, r.ob()?.map(Bytes::from));
            }
            if age > max_age {
                continue;
            }
            let Some(visit) = now.checked_sub(Duration::from_secs(age)) else { continue };
            let mut o = Opening { map, ivs, regions, doc_prefix, visit: Some(visit), ..Default::default() };
            o.recount();
            v.push((Cust { coll, comp }, o));
        }
        if !r.0.is_empty() {
            return None;
        }
        Some(v)
    })();
    match ok {
        Some(v) => {
            let mut reg = REG.lock().unwrap();
            let st = reg.entry(db.to_string()).or_insert_with(|| DbOpenings::new(db));
            for (c, o) in v {
                st.insert(c, o);
            }
            evict(&mut reg);
            RESTORED.fetch_add(1, Relaxed);
        }
        None => {
            RESTORE_REFUSED.fetch_add(1, Relaxed);
        }
    }
    RESTORE_NS.fetch_add(t0.elapsed().as_nanos() as u64, Relaxed);
}

/// Whether a first-course session that will answer from an opening is open on this query (the customer box then
/// does not read the customer's range in front of it).
pub fn serves(s: &Session) -> bool {
    s.serve.is_some()
}

/// The customer's box, when it is due on this session (taken once): the customer component to warm.
pub fn take_box(s: &mut Session) -> Option<Vec<u8>> {
    if s.box_due {
        s.box_due = false;
        Some(s.cust.comp.clone())
    } else {
        None
    }
}

/// Whether the session's opening knows every one of `keys` exactly (no counters touched).
pub fn knows(s: &Session, keys: &[Vec<u8>]) -> bool {
    s.serve.as_ref().map_or(false, |o| !keys.is_empty() && keys.iter().all(|k| o.get(k).is_some()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};

    fn b(s: &[u8]) -> Bytes {
        Bytes::copy_from_slice(s)
    }

    /// The model: what storage holds.
    fn model_scan(m: &BTreeMap<Vec<u8>, Vec<u8>>, start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>) -> Vec<(Bytes, Bytes)> {
        let it: Box<dyn DoubleEndedIterator<Item = (&Vec<u8>, &Vec<u8>)>> = match end {
            Some(e) => Box::new(m.range(start.to_vec()..e.to_vec())),
            None => Box::new(m.range(start.to_vec()..)),
        };
        let v: Vec<(Bytes, Bytes)> = if reverse { it.rev().map(|(k, v)| (b(k), b(v))).collect() } else { it.map(|(k, v)| (b(k), b(v))).collect() };
        v.into_iter().take(limit.unwrap_or(usize::MAX)).collect()
    }

    fn key(p: &[u8], n: u32) -> Vec<u8> {
        let mut k = p.to_vec();
        k.extend(n.to_be_bytes());
        k
    }

    #[test]
    fn served_answers_equal_storage_under_random_writes_and_trims() {
        // two users' regions (each a document range and an index range) plus keys outside both
        let regions_a = vec![(b"doc/c/A".to_vec(), b"doc/c/B".to_vec()), (b"idx/c/i/A".to_vec(), b"idx/c/i/B".to_vec())];
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let prefixes: [&[u8]; 4] = [b"doc/c/A", b"idx/c/i/A", b"doc/c/C", b"idx/c/i/C"];
        let (mut served, mut checks) = (0u64, 0u64);
        for round in 0..400 {
            let mut m: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
            for _ in 0..rng.gen_range(0..60) {
                let p = prefixes[rng.gen_range(0..4)];
                m.insert(key(p, rng.gen_range(0..100)), vec![rng.gen::<u8>(); rng.gen_range(1..40)]);
            }
            // record: a scan of A's index (ascending or descending, maybe limited) and point reads of A's documents
            let mut o = Opening::new(regions_a.clone(), b"doc/c/".to_vec());
            let rev = rng.gen_bool(0.3);
            let lim = if rng.gen_bool(0.7) { Some(rng.gen_range(1..12)) } else { None };
            let rows = model_scan(&m, b"idx/c/i/A", Some(b"idx/c/i/B"), rev, lim);
            o.note_scan(b"idx/c/i/A", Some(b"idx/c/i/B"), rev, lim, &rows);
            for _ in 0..rng.gen_range(0..10) {
                let k = key(b"doc/c/A", rng.gen_range(0..100));
                let v = m.get(&k).map(|v| b(v));
                o.note_get(&k, &v);
            }
            assert!(o.inside_regions());
            o.recount();
            // writes between the visits, applied to the opening as the hook does (only region keys)
            for _ in 0..rng.gen_range(0..40) {
                let p = prefixes[rng.gen_range(0..4)];
                let k = key(p, rng.gen_range(0..100));
                let del = rng.gen_bool(0.3);
                let v = vec![rng.gen::<u8>(); rng.gen_range(1..40)];
                if del {
                    m.remove(&k);
                } else {
                    m.insert(k.clone(), v.clone());
                }
                if o.in_regions(&k) {
                    o.apply(k, if del { None } else { Some(b(&v)) });
                }
                if round % 3 == 0 && rng.gen_bool(0.2) {
                    o.trim();
                }
            }
            // every read the opening answers must equal storage
            for _ in 0..30 {
                checks += 1;
                if rng.gen_bool(0.5) {
                    let k = key(prefixes[rng.gen_range(0..4)], rng.gen_range(0..100));
                    if let Some(v) = o.get(&k) {
                        served += 1;
                        assert_eq!(v, m.get(&k).map(|v| b(v)), "round {round} get {k:?}");
                    }
                } else {
                    let rev = rng.gen_bool(0.3);
                    let lim = if rng.gen_bool(0.8) { Some(rng.gen_range(1..15)) } else { None };
                    let (s, e): (Vec<u8>, Option<Vec<u8>>) = match rng.gen_range(0..3) {
                        0 => (b"idx/c/i/A".to_vec(), Some(b"idx/c/i/B".to_vec())),
                        1 => (key(b"idx/c/i/A", rng.gen_range(0..100)), Some(key(b"idx/c/i/A", rng.gen_range(0..100)))),
                        _ => (key(b"idx/c/i/A", rng.gen_range(0..100)), None),
                    };
                    if e.as_ref().map_or(false, |e| e < &s) {
                        continue;
                    }
                    if let Some(r) = o.scan(&s, e.as_deref(), rev, lim) {
                        served += 1;
                        assert_eq!(r, model_scan(&m, &s, e.as_deref(), rev, lim), "round {round} scan");
                    }
                }
            }
        }
        // the test must exercise serving, not only refusals
        assert!(served > checks / 10, "served {served} of {checks}");
    }

    #[test]
    fn newest_first_page_is_served_after_new_messages_and_misses_after_a_delete() {
        let regions = vec![(b"idx/c/i/A".to_vec(), b"idx/c/i/B".to_vec())];
        let mut m: BTreeMap<Vec<u8>, Vec<u8>> = (10..40).map(|i| (key(b"idx/c/i/A", i), vec![1])).collect();
        let mut o = Opening::new(regions, b"doc/c/".to_vec());
        let rows = model_scan(&m, b"idx/c/i/A", Some(b"idx/c/i/B"), false, Some(20));
        o.note_scan(b"idx/c/i/A", Some(b"idx/c/i/B"), false, Some(20), &rows);
        // new messages sort first (newest first): still served, and exact
        for i in 0..3 {
            m.insert(key(b"idx/c/i/A", i), vec![2]);
            o.apply(key(b"idx/c/i/A", i), Some(b(&[2])));
        }
        assert_eq!(o.scan(b"idx/c/i/A", Some(b"idx/c/i/B"), false, Some(20)).unwrap(), model_scan(&m, b"idx/c/i/A", Some(b"idx/c/i/B"), false, Some(20)));
        // after trims the page is still exact
        o.trim();
        assert_eq!(o.scan(b"idx/c/i/A", Some(b"idx/c/i/B"), false, Some(20)).unwrap(), model_scan(&m, b"idx/c/i/A", Some(b"idx/c/i/B"), false, Some(20)));
        // deletes below the page: the page needs keys the opening never saw, so it is not served
        let mut o2 = Opening::new(vec![(b"idx/c/i/A".to_vec(), b"idx/c/i/B".to_vec())], b"doc/c/".to_vec());
        let rows = model_scan(&m, b"idx/c/i/A", Some(b"idx/c/i/B"), false, Some(5));
        o2.note_scan(b"idx/c/i/A", Some(b"idx/c/i/B"), false, Some(5), &rows);
        let first = rows[0].0.to_vec();
        m.remove(&first);
        o2.apply(first, None);
        assert!(o2.scan(b"idx/c/i/A", Some(b"idx/c/i/B"), false, Some(5)).is_none());
        assert_eq!(o2.scan(b"idx/c/i/A", Some(b"idx/c/i/B"), false, Some(4)).unwrap(), model_scan(&m, b"idx/c/i/A", Some(b"idx/c/i/B"), false, Some(4)));
    }

    #[test]
    fn a_trim_keeps_consecutive_scans_of_one_query_together() {
        let p = b"idx/c/i/A";
        let mut m: BTreeMap<Vec<u8>, Vec<u8>> = (100..200).map(|i| (key(p, i), vec![1; 50])).collect();
        let mut o = Opening::new(vec![(p.to_vec(), b"idx/c/i/B".to_vec())], b"doc/c/".to_vec());
        // the query reads 20 entries, then one more from where it stopped
        let first = model_scan(&m, p, Some(b"idx/c/i/B"), false, Some(20));
        o.note_scan(p, Some(b"idx/c/i/B"), false, Some(20), &first);
        let next_start = succ(&first.last().unwrap().0);
        let second = model_scan(&m, &next_start, Some(b"idx/c/i/B"), false, Some(1));
        o.note_scan(&next_start, Some(b"idx/c/i/B"), false, Some(1), &second);
        // many new entries in front, then a trim
        for i in 0..60 {
            m.insert(key(p, i), vec![2; 50]);
            o.apply(key(p, i), Some(b(&[2; 50])));
        }
        o.trim();
        let first2 = o.scan(p, Some(b"idx/c/i/B"), false, Some(20)).expect("first batch served");
        assert_eq!(first2, model_scan(&m, p, Some(b"idx/c/i/B"), false, Some(20)));
        let next2 = succ(&first2.last().unwrap().0);
        assert_eq!(o.scan(&next2, Some(b"idx/c/i/B"), false, Some(1)).expect("next batch served"), model_scan(&m, &next2, Some(b"idx/c/i/B"), false, Some(1)));
    }

    #[test]
    fn apply_keeps_the_byte_count_exact() {
        let mut o = Opening::new(vec![(b"doc/c/A".to_vec(), b"doc/c/B".to_vec())], b"doc/c/".to_vec());
        o.note_scan(b"doc/c/A", Some(b"doc/c/B"), false, Some(2), &[(b(b"doc/c/A1"), b(b"xx")), (b(b"doc/c/A2"), b(b"y"))]);
        o.recount();
        for (k, v) in [(&b"doc/c/A1"[..], Some(b(b"zzzz"))), (&b"doc/c/A3"[..], None), (&b"doc/c/A2"[..], None), (&b"doc/c/A3"[..], Some(b(b"q")))] {
            o.apply(k.to_vec(), v);
            let incremental = o.bytes;
            o.recount();
            assert_eq!(incremental, o.bytes);
        }
    }

    #[test]
    fn reads_outside_the_users_regions_make_the_opening_unkeepable() {
        let mut o = Opening::new(vec![(b"doc/c/A".to_vec(), b"doc/c/B".to_vec())], b"doc/c/".to_vec());
        o.note_get(b"doc/c/A1", &Some(b(b"x")));
        assert!(o.inside_regions());
        o.note_get(b"doc/c/C1", &None);
        assert!(!o.inside_regions());
        let mut o = Opening::new(vec![(b"idx/c/i/A".to_vec(), b"idx/c/i/B".to_vec())], b"doc/c/".to_vec());
        o.note_scan(b"idx/c/i/", None, false, None, &[]);
        assert!(!o.inside_regions());
    }

    #[test]
    fn a_visit_of_three_queries_is_kept_and_follows_writes() {
        let _g = GLOBALS.lock().unwrap_or_else(|e| e.into_inner()); // review: the tests that set the globals run one at a time
        use tenant_bench::lifecycle::Op;
        ENABLED.store(true, Relaxed);
        QUERIES.store(3, Relaxed);
        IDLE_SECS.store(1, Relaxed);
        let db = "fc-unit-visit";
        let f = bson::doc! { "u": 5 };
        let mut base = crate::layout::doc_prefix("c");
        base.push(crate::custkey::MARK);
        base.extend(crate::custkey::comp(&bson::Bson::Int32(5)));
        let k = |n: u8| [base.clone(), vec![n]].concat();
        for (i, n) in [1u8, 2, 3].iter().enumerate() {
            let mut s = begin(db, "c", "u", &f, &[], true).expect("first three queries of the visit");
            assert!(s.serve.is_none(), "query {i}: no opening yet");
            s.note_get(&k(*n), &Some(b(&[*n])));
            finish(s, true);
        }
        assert!(begin(db, "c", "u", &f, &[], true).is_none(), "a fourth query is not recorded");
        // (regression) a write while the database's only opening was still pending is followed too
        let db2 = "fc-unit-pending";
        let mut s = begin(db2, "c", "u", &f, &[], true).unwrap();
        s.note_get(&k(1), &Some(b(&[1])));
        finish(s, true);
        let c = write_hook(db2, &[Op::Delete(k(1))]);
        after_stage(db2, c, true);
        for _ in 0..2 {
            let s = begin(db2, "c", "u", &f, &[], true).unwrap();
            finish(s, true);
        }
        std::thread::sleep(Duration::from_millis(1100));
        let mut s = begin(db2, "c", "u", &f, &[], true).unwrap();
        assert_eq!(s.get(&k(1)), Some(None), "the delete made while pending is in the opening");
        finish(s, true);
        forget_db(db2);
        assert_eq!(sizes(db).len(), 1);
        // a write to the second document, and a document of another user
        let c = write_hook(db, &[Op::Put(k(2), vec![9]), Op::Put(b"doc/c/zzz".to_vec(), vec![1])]);
        after_stage(db, c, true);
        std::thread::sleep(Duration::from_millis(1100));
        let mut s = begin(db, "c", "u", &f, &[], true).expect("next visit");
        assert_eq!(s.get(&k(1)), Some(Some(b(&[1]))));
        assert_eq!(s.get(&k(2)), Some(Some(b(&[9]))), "the write followed the opening");
        assert_eq!(s.get(&k(4)), None, "never read: not known");
        finish(s, true);
        forget_db(db);
        assert!(sizes(db).is_empty());
        QUERIES.store(1, Relaxed);
    }

    #[test]
    fn regions_of_different_users_never_overlap() {
        use bson::Bson;
        let vals = [Bson::Int32(1), Bson::Int32(10), Bson::Double(1.5), Bson::String("a".into()), Bson::String("ab".into()), Bson::String("a\u{0}".into())];
        let mut rs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for v in &vals {
            rs.extend(regions_of("c", "u", v, &[]));
            let mut p = b"idx/c/i/".to_vec();
            p.extend(crate::keyenc::component(v, true));
            rs.push((p.clone(), crate::keyenc::prefix_end(&p).unwrap()));
        }
        for (i, a) in rs.iter().enumerate() {
            for (j, c) in rs.iter().enumerate() {
                if i != j {
                    assert!(a.1 <= c.0 || c.1 <= a.0, "{a:?} overlaps {c:?}");
                }
            }
        }
    }

    // ---- Review (2026-11-07): tests that fail on the delivered commit 847d188 ----

    /// The tests below and `a_visit_of_three_queries_is_kept_and_follows_writes` set the module's globals: one at a time.
    static GLOBALS: Mutex<()> = Mutex::new(());

    /// Puts the globals back when a test ends (also when it fails).
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            QUERIES.store(1, Relaxed);
            IDLE_SECS.store(1800, Relaxed);
            MAX_BYTES.store(256 << 10, Relaxed);
            BUDGET_BYTES.store(1 << 30, Relaxed);
        }
    }

    fn review_key(user: i32, n: u32) -> Vec<u8> {
        let mut k = crate::layout::doc_prefix("c");
        k.push(crate::custkey::MARK);
        k.extend(crate::custkey::comp(&bson::Bson::Int32(user)));
        k.extend(n.to_be_bytes());
        k
    }

    /// One query of a visit (the variant with N > 1), then the user leaves.
    fn review_one_query(db: &str, user: i32) {
        let f = bson::doc! { "u": user };
        let mut s = begin(db, "c", "u", &f, &[], true).expect("first query of the visit");
        s.note_get(&review_key(user, 0), &Some(b(b"first")));
        finish(s, true);
    }

    fn review_write(db: &str, ops: Vec<tenant_bench::lifecycle::Op>) {
        let c = write_hook(db, &ops);
        after_stage(db, c, true);
    }

    /// Bytes held for one database: its openings and the openings still being built (pending).
    fn review_held(db: &str) -> (usize, usize) {
        let reg = REG.lock().unwrap();
        let Some(st) = reg.get(db) else { return (0, 0) };
        let open = st.openings.values().map(|o| o.bytes).sum();
        let pending = st
            .pending
            .values()
            .map(|(p, _, _)| {
                let mut p = p.clone();
                p.recount();
                p.bytes
            })
            .sum();
        (open, pending)
    }

    /// Why it matters: with `--first-course-queries N` (N > 1, the variant the report recommends) a visit of fewer than N
    /// queries leaves its opening pending until the user's next visit, which may be 14 days later. Every write to the
    /// user's ranges is applied to it, with no size check: a user who receives 1 MB of messages meanwhile holds 1 MB in
    /// the waiter's memory, past the 256 KB (here 64 KB) cap that openings obey. At 1 TB with users of 512 KB that is
    /// up to the whole user per pending opening.
    #[test]
    fn review_a_pending_opening_obeys_the_size_cap() {
        let _g = GLOBALS.lock().unwrap_or_else(|e| e.into_inner());
        let _r = Restore;
        ENABLED.store(true, Relaxed);
        QUERIES.store(3, Relaxed);
        IDLE_SECS.store(3600, Relaxed);
        MAX_BYTES.store(64 << 10, Relaxed);
        let db = "fc-review-pending-cap";
        review_one_query(db, 7);
        // 1,000 messages of 1 KB for this user while the opening is pending
        for i in 1..=1000u32 {
            review_write(db, vec![tenant_bench::lifecycle::Op::Put(review_key(7, i), vec![b'x'; 1024])]);
        }
        let (_, pending) = review_held(db);
        forget_db(db);
        assert!(pending <= 64 << 10, "a pending opening holds {pending} bytes, the cap is {}", 64 << 10);
    }

    /// Why it matters: the memory budget (`--first-course-mb`, 1 GiB by default) counts and evicts only finished
    /// openings. Pending ones are neither counted in `STATS.bytes` nor evicted, so with many users whose last visit had
    /// fewer than N queries the waiter's memory grows past the budget without limit (at 1 TB the report counts 658,000
    /// openings; each pending one also takes every write to its user, test above).
    #[test]
    fn review_pending_openings_stay_inside_the_memory_budget() {
        let _g = GLOBALS.lock().unwrap_or_else(|e| e.into_inner());
        let _r = Restore;
        ENABLED.store(true, Relaxed);
        QUERIES.store(3, Relaxed);
        IDLE_SECS.store(3600, Relaxed);
        BUDGET_BYTES.store(1 << 20, Relaxed); // 1 MiB for all openings
        let db = "fc-review-pending-budget";
        // 200 users, one query each, then 20 KB of messages each (every pending opening stays under the 256 KB cap)
        for u in 0..200 {
            review_one_query(db, 1000 + u);
        }
        for u in 0..200 {
            let ops = (1..=20u32).map(|i| tenant_bench::lifecycle::Op::Put(review_key(1000 + u, i), vec![b'y'; 1024])).collect();
            review_write(db, ops);
        }
        let (open, pending) = review_held(db);
        forget_db(db);
        assert!(open + pending <= 1 << 20, "openings {open} bytes + pending {pending} bytes, the budget is {} bytes", 1 << 20);
    }

    // ---- Fix round (2026-11-07): pending openings obey the cap and the budget ----

    /// The budget's count equals the bytes really held, every opening and pending opening is under the cap, the total
    /// is under the budget, and every live opening has its `LRU` entry.
    fn fix_check_invariants(ctx: &str) {
        let reg = REG.lock().unwrap();
        let lru = LRU.lock().unwrap();
        let max = MAX_BYTES.load(Relaxed) as usize;
        let mut held = 0usize;
        let (mut n_open, mut n_pend) = (0u64, 0u64);
        for st in reg.values() {
            for o in st.openings.values() {
                let mut x = o.clone();
                x.recount();
                assert_eq!(x.bytes, o.bytes, "{ctx}: an opening's byte count drifted");
                assert!(o.bytes <= max, "{ctx}: an opening holds {} bytes, cap {max}", o.bytes);
                assert!(lru.contains_key(&o.lru), "{ctx}: an opening is missing from the LRU");
                held += o.bytes;
                n_open += 1;
            }
            for (p, _, _) in st.pending.values() {
                let mut x = p.clone();
                x.recount();
                assert_eq!(x.bytes, p.bytes, "{ctx}: a pending opening's byte count drifted");
                assert!(p.bytes <= max, "{ctx}: a pending opening holds {} bytes, cap {max}", p.bytes);
                assert!(lru.contains_key(&p.lru), "{ctx}: a pending opening is missing from the LRU");
                held += p.bytes;
                n_pend += 1;
            }
        }
        assert_eq!(STATS.bytes.load(Relaxed) as usize, held, "{ctx}: STATS.bytes differs from the bytes held");
        assert_eq!(STATS.openings.load(Relaxed), n_open, "{ctx}: STATS.openings");
        assert_eq!(STATS.pending.load(Relaxed), n_pend, "{ctx}: STATS.pending");
        assert!(held as u64 <= BUDGET_BYTES.load(Relaxed), "{ctx}: {held} bytes held, budget {}", BUDGET_BYTES.load(Relaxed));
    }

    /// Random visits (1 to 4 queries with N = 3), writes of every size, idle gaps, metadata writes and failed stages:
    /// after every step the invariants above hold, and every read an opening answers equals storage (a model).
    #[test]
    fn fix_random_visits_and_writes_keep_cap_budget_and_exactness() {
        use tenant_bench::lifecycle::Op;
        let _g = GLOBALS.lock().unwrap_or_else(|e| e.into_inner());
        let _r = Restore;
        ENABLED.store(true, Relaxed);
        QUERIES.store(3, Relaxed);
        IDLE_SECS.store(0, Relaxed); // every query starts a visit unless the test says otherwise (below)
        MAX_BYTES.store(8 << 10, Relaxed);
        BUDGET_BYTES.store(64 << 10, Relaxed);
        let db = "fc-fix-random";
        forget_db(db);
        fix_check_invariants("start");
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        let mut m: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let users: Vec<i32> = (0..30).collect();
        let (mut served, mut evicted_pending) = (0u64, STATS.evicted_pending.load(Relaxed));
        for step in 0..3000 {
            let u = users[rng.gen_range(0..users.len())];
            let f = bson::doc! { "u": u };
            let lo = review_key(u, 0);
            let hi = review_key(u, u32::MAX);
            match rng.gen_range(0..10) {
                0..=4 => {
                    // a query: one visit's first query, or (idle long) a later one of the same visit
                    IDLE_SECS.store(if rng.gen_bool(0.6) { 3600 } else { 0 }, Relaxed);
                    let Some(mut s) = begin(db, "c", "u", &f, &[], true) else { continue };
                    let lim = Some(rng.gen_range(1..20));
                    let rows: Vec<(Bytes, Bytes)> = model_scan(&m, &lo, Some(&hi), true, lim);
                    if let Some(r) = s.scan(&lo, Some(&hi), true, lim) {
                        assert_eq!(r, rows, "step {step}: an answer from an opening differs from storage");
                        served += 1;
                    }
                    s.note_scan(&lo, Some(&hi), true, lim, &rows);
                    finish(s, rng.gen_bool(0.97));
                }
                5..=8 => {
                    // writes for this user (sometimes a burst), with deletes
                    let n = if rng.gen_bool(0.1) { rng.gen_range(50..300) } else { rng.gen_range(1..5) };
                    let mut ops = Vec::new();
                    for _ in 0..n {
                        let k = review_key(u, rng.gen_range(1..2000));
                        if rng.gen_bool(0.2) {
                            m.remove(&k);
                            ops.push(Op::Delete(k));
                        } else {
                            let v = vec![b'z'; rng.gen_range(10..400)];
                            m.insert(k.clone(), v.clone());
                            ops.push(Op::Put(k, v));
                        }
                    }
                    review_write(db, ops);
                }
                _ => {
                    if rng.gen_bool(0.02) {
                        // a metadata write forgets everything
                        review_write(db, vec![Op::Put(b"meta/indexes/c/x".to_vec(), vec![1])]);
                    }
                }
            }
            fix_check_invariants(&format!("step {step}"));
        }
        assert!(served > 100, "the test must serve from openings: {served}");
        assert!(STATS.evicted_pending.load(Relaxed) > evicted_pending, "the test must evict pending openings");
        forget_db(db);
        fix_check_invariants("end");
    }

    /// The least recently visited goes first, pending or finished; an evicted pending opening leaves the user with a
    /// plain storage read on the next visit.
    #[test]
    fn fix_eviction_takes_the_least_recently_visited_first() {
        let _g = GLOBALS.lock().unwrap_or_else(|e| e.into_inner());
        let _r = Restore;
        ENABLED.store(true, Relaxed);
        QUERIES.store(3, Relaxed);
        IDLE_SECS.store(3600, Relaxed);
        let db = "fc-fix-lru";
        forget_db(db);
        // users 1, 2, 3 visit in this order (one query each: pending openings of about 1 KB)
        for u in 1..=3 {
            let f = bson::doc! { "u": u };
            let mut s = begin(db, "c", "u", &f, &[], true).unwrap();
            s.note_get(&review_key(u, 0), &Some(b(&[b'v'; 1000])));
            finish(s, true);
        }
        let held = STATS.bytes.load(Relaxed);
        // a budget that holds two of the three: user 1 (the oldest) goes
        BUDGET_BYTES.store(held - 1, Relaxed);
        evict(&mut REG.lock().unwrap());
        let pend: HashSet<i32> = {
            let reg = REG.lock().unwrap();
            (1..=3).filter(|u| reg[db].pending.contains_key(&Cust { coll: "c".into(), comp: crate::custkey::comp(&bson::Bson::Int32(*u)) })).collect()
        };
        assert_eq!(pend, HashSet::from([2, 3]), "the oldest pending opening is evicted first");
        // user 1's next query of the visit is not recorded (its pending opening is gone) and nothing breaks
        assert!(begin(db, "c", "u", &bson::doc! { "u": 1 }, &[], true).is_none());
        fix_check_invariants("after eviction");
        forget_db(db);
        fix_check_invariants("end");
    }

    #[test]
    fn kept_openings_survive_a_close_only_for_the_next_lease() {
        let _g = GLOBALS.lock().unwrap_or_else(|e| e.into_inner());
        use tenant_bench::lifecycle::Op;
        ENABLED.store(true, Relaxed);
        QUERIES.store(1, Relaxed);
        IDLE_SECS.store(0, Relaxed); // every query starts a visit
        let dir = std::env::temp_dir().join(format!("fc-short-unit-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let _ = PERSIST_DIR.set(dir.clone());
        let db = "fc-unit-short";
        let f = bson::doc! { "u": 7 };
        let mut base = crate::layout::doc_prefix("c");
        base.push(crate::custkey::MARK);
        base.extend(crate::custkey::comp(&bson::Bson::Int32(7)));
        let k = |n: u8| [base.clone(), vec![n]].concat();
        let record = || {
            let mut s = begin(db, "c", "u", &f, &[], true).unwrap();
            s.note_scan(&k(0), Some(&k(9)), false, None, &[(b(&k(1)), b(b"one")), (b(&k(2)), b(b"two"))]);
            finish(s, true);
            // a write after the visit is followed, then kept
            let c = write_hook(db, &[Op::Put(k(3), b"three".to_vec()), Op::Delete(k(1))]);
            after_stage(db, c, true);
        };
        let first = |db: &str| {
            let mut s = begin(db, "c", "u", &f, &[], true).unwrap();
            let r = s.scan(&k(0), Some(&k(9)), false, None);
            finish(s, true);
            r
        };
        // kept at a release that wrote sequence 6: the next acquire (7) loads it, exactly
        record();
        let body = park_db(db).expect("parked");
        persist(db, 6, body);
        restore_db(db, Some(7), true, false);
        let got = first(db).expect("served after the restore");
        assert_eq!(got, vec![(b(&k(2)), b(b"two")), (b(&k(3)), b(b"three"))]);
        // the file is used once
        assert!(!persist_path(db).unwrap().exists());
        // another owner between (acquire 9 after a release of 6), or a takeover: refused
        for (seq, clean) in [(Some(9), true), (Some(7), false), (None, true)] {
            record();
            let body = park_db(db).expect("parked");
            persist(db, 6, body);
            let refused = RESTORE_REFUSED.load(Relaxed);
            restore_db(db, seq, clean, false);
            assert_eq!(RESTORE_REFUSED.load(Relaxed), refused + 1, "{seq:?} {clean}");
            assert!(first(db).is_none(), "no opening after a refused restore ({seq:?} {clean})");
        }
        // a corrupt file is ignored
        record();
        persist(db, 6, park_db(db).unwrap());
        let p = persist_path(db).unwrap();
        let mut bytes = std::fs::read(&p).unwrap();
        let n = bytes.len() / 2;
        bytes[n] ^= 0x55;
        std::fs::write(&p, bytes).unwrap();
        let refused = RESTORE_REFUSED.load(Relaxed);
        restore_db(db, Some(7), true, false);
        assert_eq!(RESTORE_REFUSED.load(Relaxed), refused + 1);
        assert!(first(db).is_none());
        forget_db(db);
        IDLE_SECS.store(1800, Relaxed);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
