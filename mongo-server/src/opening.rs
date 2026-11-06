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
    pub bytes: AtomicU64,
    pub openings: AtomicU64,
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
        let mut ivs = std::mem::take(&mut self.ivs);
        for iv in ivs.iter_mut() {
            let keep = iv.keep.max(1);
            let present: Vec<Vec<u8>> = {
                let range: Box<dyn DoubleEndedIterator<Item = (&Vec<u8>, &Option<Bytes>)>> = match &iv.hi {
                    Some(h) => Box::new(self.map.range(iv.lo.clone()..h.clone())),
                    None => Box::new(self.map.range(iv.lo.clone()..)),
                };
                if iv.rev {
                    range.rev().filter(|(_, v)| v.is_some()).map(|(k, _)| k.clone()).take(keep).collect()
                } else {
                    range.filter(|(_, v)| v.is_some()).map(|(k, _)| k.clone()).take(keep).collect()
                }
            };
            if present.len() >= keep {
                let edge = present.last().unwrap();
                if iv.rev {
                    iv.lo = edge.clone();
                } else {
                    iv.hi = Some(succ(edge));
                }
            }
        }
        self.ivs = ivs;
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
}

/// The openings of one database.
#[derive(Default)]
struct DbOpenings {
    openings: HashMap<Cust, Opening>,
    /// region start -> (region end, user); regions never overlap
    regions: BTreeMap<Vec<u8>, (Vec<u8>, Cust)>,
    /// last query of each user
    access: HashMap<Cust, Instant>,
    /// `QUERIES` > 1: the opening a visit is building (queries begun, queries recorded); writes follow it like an
    /// opening; it becomes the user's opening after its N-th query or when the next visit starts
    pending: HashMap<Cust, (Opening, u64, u64)>,
    accesses: u64,
}

impl DbOpenings {
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

    fn drop_pending(&mut self, c: &Cust) {
        if let Some((p, _, _)) = self.pending.remove(c) {
            if !self.openings.contains_key(c) {
                self.unclaim(c, &p.regions);
            }
        }
    }

    /// The visit's pending opening becomes the user's opening (trimmed or dropped when too big).
    fn install_pending(&mut self, c: &Cust) {
        let Some((mut p, _, _)) = self.pending.remove(c) else { return };
        p.visit = Some(Instant::now());
        p.recount();
        let max = MAX_BYTES.load(Relaxed) as usize;
        if p.bytes > max {
            p.trim();
            STATS.trimmed.fetch_add(1, Relaxed);
            if p.bytes > max {
                STATS.dropped_too_big.fetch_add(1, Relaxed);
                let regions = p.regions.clone();
                self.remove(c);
                self.unclaim(c, &regions);
                return;
            }
        }
        self.insert(c.clone(), p);
        STATS.kept.fetch_add(1, Relaxed);
    }

    fn insert(&mut self, c: Cust, o: Opening) {
        self.remove(&c);
        for (s, e) in &o.regions {
            self.regions.insert(s.clone(), (e.clone(), c.clone()));
        }
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
}

impl Session {
    pub fn get(&mut self, k: &[u8]) -> Option<Option<Bytes>> {
        let r = self.serve.as_ref().and_then(|o| o.get(k));
        self.count(r.is_some());
        r
    }

    pub fn scan(&mut self, start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>) -> Option<Vec<(Bytes, Bytes)>> {
        let r = self.serve.as_ref().and_then(|o| o.scan(start, end, reverse, limit));
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

fn customer_value(field: &str, filter: &bson::Document) -> Option<bson::Bson> {
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
    let mut d = crate::layout::doc_prefix(coll);
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
    let st = reg.entry(db.to_string()).or_default();
    let last = st.access.insert(cust.clone(), now);
    st.accesses += 1;
    if st.accesses % 4096 == 0 {
        st.access.retain(|_, t| now.duration_since(*t) < idle);
    }
    let n = QUERIES.load(Relaxed).max(1);
    let mk = |serve: Option<Opening>| Session { db: db.to_string(), cust: cust.clone(), serve, rec: Opening::new(regions.clone(), crate::layout::doc_prefix(coll)), served: 0, missed: 0, record_ns: 0 };
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
        st.claim(&cust, &regions);
        st.pending.insert(cust.clone(), (Opening::new(regions.clone(), crate::layout::doc_prefix(coll)), 1, 0));
    }
    Some(mk(serve))
}

/// End of the first query: keep what it read as the user's opening (when the query succeeded and the opening is
/// exact and small enough).
pub fn finish(s: Session, ok: bool) {
    let Session { db, cust, serve, mut rec, served, missed, record_ns } = s;
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
        let p = st.pending.get_mut(&cust).unwrap();
        p.0.map.extend(rec.map);
        p.0.ivs.extend(rec.ivs);
        p.2 += 1;
        if p.2 >= QUERIES.load(Relaxed) {
            st.install_pending(&cust);
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
    let max = MAX_BYTES.load(Relaxed) as usize;
    if rec.bytes > max {
        rec.trim();
        STATS.trimmed.fetch_add(1, Relaxed);
        if rec.bytes > max {
            STATS.dropped_too_big.fetch_add(1, Relaxed);
            return;
        }
    }
    let mut reg = REG.lock().unwrap();
    reg.entry(db).or_default().insert(cust, rec);
    STATS.kept.fetch_add(1, Relaxed);
    evict(&mut reg);
}

fn evict(reg: &mut HashMap<String, DbOpenings>) {
    let budget = BUDGET_BYTES.load(Relaxed);
    while STATS.bytes.load(Relaxed) > budget {
        // least recently visited (linear: fine for a measurement prototype)
        let mut oldest: Option<(String, Cust, Instant)> = None;
        for (db, st) in reg.iter() {
            for (c, o) in &st.openings {
                let t = o.visit.unwrap_or_else(Instant::now);
                if oldest.as_ref().map_or(true, |(_, _, ot)| t < *ot) {
                    oldest = Some((db.clone(), c.clone(), t));
                }
            }
        }
        let Some((db, c, _)) = oldest else { break };
        reg.get_mut(&db).map(|st| st.remove(&c));
        STATS.evicted.fetch_add(1, Relaxed);
    }
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
            let max = MAX_BYTES.load(Relaxed) as usize;
            let mut reg = REG.lock().unwrap();
            if let Some(st) = reg.get_mut(db) {
                // the whole batch first, then the size check: a trim between a new document and its index entry (same
                // batch) would forget the document the entry points to
                let mut touched: Vec<(Cust, usize)> = Vec::new();
                for (k, v) in w {
                    let Some(c) = st.owner(&k).cloned() else { continue };
                    if let Some(p) = st.pending.get_mut(&c) {
                        p.0.apply(k.clone(), v.clone());
                    }
                    let Some(o) = st.openings.get_mut(&c) else { continue };
                    if !touched.iter().any(|(t, _)| *t == c) {
                        touched.push((c.clone(), o.bytes));
                    }
                    o.apply(k, v);
                    STATS.writes_applied.fetch_add(1, Relaxed);
                }
                for (c, before) in touched {
                    let Some(o) = st.openings.get_mut(&c) else { continue };
                    if o.bytes > max {
                        o.trim();
                        STATS.trimmed.fetch_add(1, Relaxed);
                    }
                    let after = o.bytes;
                    STATS.bytes.fetch_add(after as u64, Relaxed);
                    STATS.bytes.fetch_sub(before as u64, Relaxed);
                    if after > max {
                        st.remove(&c);
                        STATS.dropped_too_big.fetch_add(1, Relaxed);
                    }
                }
            }
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
        st.pending.clear();
        let cs: Vec<Cust> = st.openings.keys().cloned().collect();
        for c in cs {
            st.remove(&c);
            STATS.forgotten.fetch_add(1, Relaxed);
        }
        st.regions.clear();
    }
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
        use tenant_bench::lifecycle::Op;
        ENABLED.store(true, Relaxed);
        QUERIES.store(3, Relaxed);
        IDLE_SECS.store(1, Relaxed);
        let db = "fc-unit-visit";
        let f = bson::doc! { "u": 5 };
        let mut base = crate::layout::doc_prefix("c");
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
}
