//! Reading and writing documents through the secondary indexes.
//!
//! * [`find_docs`] plans a filter ([`index::plan`]), fetches candidate documents through the index (or a
//!   collection scan) and re-applies the whole filter, so a plan only has to return a superset.
//! * [`Writes`] collects one command's writes: the document put/delete and every index-entry put/delete go
//!   into the same ops vector, which `Entry::apply` commits as one atomic batch, so an index cannot disagree
//!   with its documents. It also enforces unique indexes against the store plus the command's own pending
//!   writes (the overlay), and is the write set of a transaction.

use crate::dbs::{Entry, ReadView};
use crate::error::MongoError;
use crate::index::{self, entry_id, entry_key, IndexDef, Plan, State};
use crate::keyenc::prefix_end;
use crate::layout::{doc_key, doc_prefix, encode_sortable};
use crate::query::matches;
use bson::{Bson, Document};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use tenant_bench::lifecycle::Op;

type R<T> = Result<T, MongoError>;

/// What the read path needs from a database: the exclusive [`Entry`] (writes, transactions: reads go through the
/// command's snapshot) or a shared [`ReadView`] (plain reads, which run concurrently with each other).
pub trait Rd: Send {
    fn get(&mut self, key: &[u8]) -> impl std::future::Future<Output = anyhow::Result<Option<Bytes>>> + Send;
    fn get_many(&mut self, keys: &[Vec<u8>]) -> impl std::future::Future<Output = anyhow::Result<Vec<Option<Bytes>>>> + Send;
    fn scan_range(&mut self, start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize) -> impl std::future::Future<Output = anyhow::Result<Vec<(Bytes, Bytes)>>> + Send;
    fn approx_bytes(&self) -> u64;
    fn indexes(&mut self, coll: &str) -> impl std::future::Future<Output = anyhow::Result<Vec<IndexDef>>> + Send;
    /// A collection with a TTL index was read: a close of this tenant writes the expiry hint.
    fn note_ttl(&self);
}

impl Rd for Entry {
    async fn get(&mut self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        Entry::get(self, key).await
    }
    async fn get_many(&mut self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Option<Bytes>>> {
        Entry::get_many(self, keys).await
    }
    async fn scan_range(&mut self, start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        Entry::scan_range(self, start, end, reverse, limit, read_ahead).await
    }
    fn approx_bytes(&self) -> u64 {
        Entry::approx_bytes(self)
    }
    async fn indexes(&mut self, coll: &str) -> anyhow::Result<Vec<IndexDef>> {
        Entry::indexes(self, coll).await
    }
    fn note_ttl(&self) {
        self.ttl_seen.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Rd for tokio::sync::OwnedRwLockWriteGuard<Entry> {
    async fn get(&mut self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        Entry::get(self, key).await
    }
    async fn get_many(&mut self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Option<Bytes>>> {
        Entry::get_many(self, keys).await
    }
    async fn scan_range(&mut self, start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        Entry::scan_range(self, start, end, reverse, limit, read_ahead).await
    }
    fn approx_bytes(&self) -> u64 {
        Entry::approx_bytes(self)
    }
    async fn indexes(&mut self, coll: &str) -> anyhow::Result<Vec<IndexDef>> {
        Entry::indexes(self, coll).await
    }
    fn note_ttl(&self) {
        self.ttl_seen.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Rd for ReadView {
    async fn get(&mut self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        ReadView::get(self, key).await
    }
    async fn get_many(&mut self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Option<Bytes>>> {
        ReadView::get_many(self, keys).await
    }
    async fn scan_range(&mut self, start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        ReadView::scan_range(self, start, end, reverse, limit, read_ahead).await
    }
    fn approx_bytes(&self) -> u64 {
        ReadView::approx_bytes(self)
    }
    async fn indexes(&mut self, coll: &str) -> anyhow::Result<Vec<IndexDef>> {
        ReadView::indexes(self, coll).await
    }
    fn note_ttl(&self) {
        ReadView::note_ttl(self)
    }
}

/// Pending writes by document key; `None` is a deletion.
pub type Overlay = HashMap<Vec<u8>, Option<Document>>;

const INDEX_READ_AHEAD: usize = 64 << 10;
const DOC_READ_AHEAD: usize = 4 << 20;

#[derive(Clone, Debug)]
pub enum Hint {
    None,
    Natural(bool),
    Index(usize),
}

#[derive(Default, Debug, Clone)]
pub struct Stats {
    pub keys: usize,
    pub docs: usize,
}

pub struct FindOpts<'a> {
    pub filter: &'a Document,
    pub sort: &'a [(String, bool)],
    /// `skip + limit` when the caller keeps at most that many documents.
    pub want: Option<usize>,
    pub hint: Hint,
    /// TTL indexes and the clock: expired documents are invisible.
    pub ttl: Option<(&'a [index::TtlSpec], i64)>,
    pub ov: Option<&'a Overlay>,
}

pub fn resolve_hint(indexes: &[IndexDef], hint: Option<&Bson>) -> R<Hint> {
    let bad = || MongoError::bad_value("error processing query: planner returned error :: caused by :: hint provided does not correspond to an existing index".to_string());
    match hint {
        None => Ok(Hint::None),
        Some(Bson::String(name)) => {
            if name == "_id_" {
                return Ok(Hint::None);
            }
            indexes.iter().position(|i| i.name == *name && i.state == State::Ready).map(Hint::Index).ok_or_else(bad)
        }
        Some(Bson::Document(d)) => {
            if let Some(n) = d.get("$natural") {
                let asc = !matches!(n, Bson::Int32(-1) | Bson::Int64(-1)) && !matches!(n, Bson::Double(f) if *f < 0.0);
                return Ok(Hint::Natural(asc));
            }
            if d.len() == 1 && d.contains_key("_id") {
                return Ok(Hint::None);
            }
            indexes.iter().position(|i| i.state == State::Ready && i.key_pattern() == *d).map(Hint::Index).ok_or_else(bad)
        }
        Some(_) => Err(MongoError::new(2, "BadValue", "hint must be either a string or a document".into())),
    }
}

pub fn plan_for(indexes: &[IndexDef], o: &FindOpts) -> Plan {
    let (hint, natural) = match &o.hint {
        Hint::None => (None, None),
        Hint::Natural(a) => (None, Some(*a)),
        Hint::Index(i) => (Some(*i), None),
    };
    index::plan(&index::PlanInput { indexes, filter: o.filter, sort: o.sort, has_limit: o.want.is_some(), hint, natural })
}

/// Walks the byte intervals of an index scan in batches.
struct IxCursor {
    prefix: Vec<u8>,
    pending: Vec<Bytes>,
    ivs: Vec<(Vec<u8>, Vec<u8>)>,
    reverse: bool,
    i: usize,
    resume: Option<Vec<u8>>,
}

impl IxCursor {
    fn new(coll: &str, idx: &IndexDef, scan: &index::IxScan) -> Self {
        let prefix = idx.prefix(coll);
        let end_all = prefix_end(&prefix).unwrap_or_else(|| vec![0xFF; prefix.len() + 1]);
        let mut ivs: Vec<(Vec<u8>, Vec<u8>)> = scan
            .intervals
            .iter()
            .map(|(s, e)| {
                let mut a = prefix.clone();
                a.extend(s);
                let b = match e {
                    Some(e) => {
                        let mut b = prefix.clone();
                        b.extend(e);
                        b
                    }
                    None => end_all.clone(),
                };
                (a, b)
            })
            .collect();
        if scan.reverse {
            ivs.reverse();
        }
        IxCursor { prefix, pending: Vec::new(), ivs, reverse: scan.reverse, i: 0, resume: None }
    }

    /// Entries (full keys) of the next batch of at most `n`; empty when exhausted. Runs of equal keys are completed
    /// and put into insertion order; what exceeds `n` is kept for the next call.
    async fn next_batch(&mut self, g: &mut impl Rd, n: usize) -> R<Vec<Bytes>> {
        tenant_bench::counting::set_phase(1);
        if !self.pending.is_empty() {
            let take = n.min(self.pending.len());
            return Ok(self.pending.drain(..take).collect());
        }
        let mut out: Vec<Bytes> = Vec::new();
        while out.is_empty() && self.i < self.ivs.len() {
            let (s, e) = &self.ivs[self.i];
            let mut rows = if self.reverse {
                let end = self.resume.clone().unwrap_or_else(|| e.clone());
                g.scan_range(s, Some(&end), true, Some(n), INDEX_READ_AHEAD).await?
            } else {
                let start = self.resume.clone().unwrap_or_else(|| s.clone());
                g.scan_range(&start, Some(e), false, Some(n), INDEX_READ_AHEAD).await?
            };
            let fetched = rows.len();
            if fetched >= n {
                // complete the last run of equal keys so that it can be put into insertion order
                loop {
                    let last = rows.last().unwrap().0.to_vec();
                    let more = if self.reverse {
                        g.scan_range(s, Some(&last), true, Some(64), INDEX_READ_AHEAD).await?
                    } else {
                        let mut st = last.clone();
                        st.push(0);
                        g.scan_range(&st, Some(e), false, Some(64), INDEX_READ_AHEAD).await?
                    };
                    let k = more.iter().take_while(|(mk, _)| crate::natural::same_group(&last, mk, &self.prefix)).count();
                    let full = k == more.len() && !more.is_empty();
                    rows.extend(more.into_iter().take(k));
                    if !full {
                        break;
                    }
                }
            }
            let scan_last = rows.last().map(|(k, _)| k.to_vec());
            crate::natural::reorder_ties(&mut rows, &self.prefix, self.reverse);
            if fetched < n {
                self.i += 1;
                self.resume = None;
            } else if let Some(last) = scan_last {
                self.resume = Some(if self.reverse {
                    last
                } else {
                    let mut k = last;
                    k.push(0);
                    k
                });
            }
            let mut keys: Vec<Bytes> = rows.into_iter().map(|(k, _)| k).collect();
            if keys.len() > n {
                self.pending = keys.split_off(n);
            }
            out.extend(keys);
        }
        Ok(out)
    }
}

pub fn decode(v: &[u8]) -> R<Document> {
    Ok(Document::from_reader(v)?)
}

/// Bytes read by one point read (a block).
const POINT_READ_BYTES: u64 = 4096;
/// Tenants up to this size are cheap to scan in full.
const SMALL_TENANT_BYTES: u64 = 8 << 20;
/// Bytes per request of the range scan that replaces point reads.
const FETCH_SCAN_READ_AHEAD: usize = 4 << 20;

/// True when reading `n` documents with a range scan over their span is expected to take fewer object-store requests
/// than `n` point reads: the point reads touch about one block each (at most all blocks of the tenant), the scan reads
/// the tenant's data `FETCH_SCAN_READ_AHEAD` bytes per request. `total` is the tenant's SST size (an upper bound of the
/// collection's, so the choice errs towards point reads).
fn prefer_scan(total: u64, n: usize) -> bool {
    static OFF: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if *OFF.get_or_init(|| std::env::var_os("MONGO_NO_FETCH_SCAN").is_some()) {
        return false; // measurement switch: the behaviour before the cost-based choice
    }
    let point = (n as u64).min(total / POINT_READ_BYTES + 1);
    let scan = total / FETCH_SCAN_READ_AHEAD as u64 + 1;
    // A scan reads the whole tenant: it must save a lot of requests (large tenants) or be cheap in bytes (small ones).
    (point > 4 * scan + 8) || (total <= SMALL_TENANT_BYTES && point > scan + 2)
}

async fn docs_by_ids(g: &mut impl Rd, coll: &str, ids: &[Vec<u8>]) -> R<Vec<Document>> {
    let prefix = doc_prefix(coll);
    tenant_bench::counting::set_phase(2);
    if prefer_scan(g.approx_bytes(), ids.len()) {
        return docs_by_scan(g, &prefix, ids).await;
    }
    let keys: Vec<Vec<u8>> = ids
        .iter()
        .map(|id| {
            let mut k = prefix.clone();
            k.extend(id);
            k
        })
        .collect();
    let vals = g.get_many(&keys).await?;
    vals.into_iter().flatten().map(|v| decode(&v)).collect()
}

/// The documents with these ids (in the order of `ids`) from one ascending range scan over the id span.
async fn docs_by_scan(g: &mut impl Rd, prefix: &[u8], ids: &[Vec<u8>]) -> R<Vec<Document>> {
    let want: HashSet<&[u8]> = ids.iter().map(|i| i.as_slice()).collect();
    let (Some(lo), Some(hi)) = (ids.iter().min(), ids.iter().max()) else { return Ok(vec![]) };
    let mut start = prefix.to_vec();
    start.extend(lo);
    let mut stop = prefix.to_vec();
    stop.extend(hi);
    stop.push(0);
    let mut found: HashMap<Vec<u8>, Document> = HashMap::new();
    loop {
        let rows = g.scan_range(&start, Some(&stop), false, Some(FETCH_CHUNK), FETCH_SCAN_READ_AHEAD).await?;
        let n = rows.len();
        for (k, v) in &rows {
            let id = &k[prefix.len()..];
            if want.contains(id) {
                found.insert(id.to_vec(), decode(v)?);
            }
        }
        if n < FETCH_CHUNK {
            break;
        }
        let mut k = rows.last().unwrap().0.to_vec();
        k.push(0);
        if k >= stop {
            break; // the chunk ended on the last wanted key: nothing is left (an empty range would panic the store)
        }
        start = k;
    }
    Ok(ids.iter().filter_map(|i| found.get(i).cloned()).collect())
}

const FETCH_CHUNK: usize = 20_000;

/// Candidate documents of `plan` (index order), still to be filtered.
async fn plan_docs_ids(g: &mut impl Rd, coll: &str, indexes: &[IndexDef], plan: &Plan, stats: &mut Stats) -> R<Vec<Vec<u8>>> {
    let mut ids = Vec::new();
    let mut stack = vec![plan];
    while let Some(p) = stack.pop() {
        match p {
            Plan::Id(id) => ids.push(encode_sortable(id)),
            Plan::Ix(s) => {
                let mut c = IxCursor::new(coll, &indexes[s.idx], s);
                loop {
                    let b = c.next_batch(g, usize::MAX).await?;
                    if b.is_empty() {
                        break;
                    }
                    stats.keys += b.len();
                    ids.extend(b.iter().filter_map(|k| entry_id(k).map(|x| x.to_vec())));
                }
            }
            Plan::Or(subs) => stack.extend(subs.iter().rev()),
            Plan::Coll { .. } => {}
        }
    }
    Ok(ids)
}

fn visible(d: &Document, o: &FindOpts) -> bool {
    match o.ttl {
        Some((specs, now)) => !index::is_expired(d, specs, now),
        None => true,
    }
}

/// Documents matching `o.filter`, in plan order (sort not applied unless the index provides it). A `$text` condition
/// needs a text index; the documents of such a query carry their text score in the hidden [`crate::text::SCORE_KEY`] field.
pub async fn find_docs(g: &mut impl Rd, coll: &str, indexes: &[IndexDef], o: &FindOpts<'_>) -> R<(Vec<Document>, Stats, Plan)> {
    crate::js::validate_filter(o.filter)?;
    if let Hint::Index(i) = &o.hint {
        if matches!(indexes.get(*i).map(|x| &x.kind), Some(index::Kind::Wildcard(_))) && !index::wild_hint_ok(indexes, *i, o.filter) {
            return Err(MongoError::new(291, "NoQueryExecutionPlans", "error processing query: planner returned error :: caused by :: $hint: refusing to build whole-index solution, because it's a wildcard index".to_string()));
        }
    }
    let has_text = crate::text::has_text(o.filter);
    let has_geo = crate::geo_query::filter_has_geo(o.filter);
    if !has_text && !has_geo {
        return find_docs_inner(g, coll, indexes, o).await;
    }
    let mut near = None;
    if has_geo {
        crate::geo_query::validate_filter(o.filter, true)?;
        near = crate::geo_query::find_near(o.filter)?;
    }
    if let Some((path, n)) = &near {
        if has_text {
            return Err(MongoError::bad_value("text and geoNear not allowed in same query".to_string()));
        }
        let ok = indexes.iter().any(|i| match &i.kind {
            index::Kind::Geo(gs) => i.state == State::Ready && gs.field == *path && if n.from_geometry { gs.sphere } else { n.spherical || !gs.sphere },
            _ => false,
        });
        for i in indexes.iter() {
            if let index::Kind::Geo(gs) = &i.kind {
                if !gs.sphere && gs.field == *path && !n.spherical && !(n.center_xy[0] >= gs.min && n.center_xy[0] < gs.max && n.center_xy[1] >= gs.min && n.center_xy[1] < gs.max) {
                    return Err(MongoError::new(16433, "Location16433", format!("point not in interval of [ {}, {} )", crate::geo::fmt_num(gs.min), crate::geo::fmt_num(gs.max))));
                }
            }
        }
        let hinted_elsewhere = match &o.hint {
            Hint::Index(i) => !matches!(indexes.get(*i).map(|x| &x.kind), Some(index::Kind::Geo(gs)) if gs.field == *path),
            Hint::Natural(_) => true,
            Hint::None => false,
        };
        if !ok || hinted_elsewhere {
            return Err(MongoError::new(291, "NoQueryExecutionPlans", "error processing query: planner returned error :: caused by :: unable to find index for $geoNear query".to_string()));
        }
    }
    let (mut docs, stats, plan);
    if has_text {
        let text_index = indexes.iter().find(|i| matches!(i.kind, index::Kind::Text(_)) && i.state == State::Ready).map(|i| i.to_stored());
        let filter2 = crate::text::resolve_filter(o.filter, text_index.as_ref())?;
        let o2 = FindOpts { filter: &filter2, sort: o.sort, want: o.want, hint: o.hint.clone(), ttl: o.ttl, ov: o.ov };
        if let Some(op) = crate::text::find_operand_any(&filter2) {
            crate::text::parse_text_operand(op, crate::text::Lang::English)?; // report a bad operand before planning
        }
        if !matches!(o.hint, Hint::None) {
            return Err(MongoError::bad_value("error processing query: planner returned error :: caused by :: hint provided does not correspond to an existing index".to_string()));
        }
        let p = plan_for(indexes, &o2);
        let planned = if crate::text::find_operand(&filter2).is_some() { matches!(p, Plan::Ix(_)) } else { matches!(p, Plan::Or(_)) };
        if !planned {
            return Err(MongoError::new(291, "NoQueryExecutionPlans", "Failed to produce a solution for TEXT under OR - other non-TEXT clauses under OR have to be indexed as well. Or: failed to use text index to satisfy $text query (if text index is compound, are equality predicates given for all prefix fields?)".to_string()));
        }
        (docs, stats, plan) = find_docs_inner(g, coll, indexes, &o2).await?;
        for d in docs.iter_mut() {
            if let Some(s) = crate::text::score_of(&filter2, d)? {
                d.insert(crate::text::SCORE_KEY, s);
            }
        }
    } else {
        let o3 = FindOpts { filter: o.filter, sort: o.sort, want: if near.is_some() { None } else { o.want }, hint: o.hint.clone(), ttl: o.ttl, ov: o.ov };
        if near.is_some() && !matches!(plan_for(indexes, &o3), Plan::Ix(s) if matches!(indexes[s.idx].kind, index::Kind::Geo(_))) {
            return Err(MongoError::new(291, "NoQueryExecutionPlans", "error processing query: planner returned error :: caused by :: unable to find index for $geoNear query".to_string()));
        }
        // nearest first: grow a ring around the query point until the wanted number of documents is certain
        if let Some((path, n)) = &near {
            if o.ov.map(|m| m.keys().all(|k| !k.starts_with(&doc_prefix(coll)))).unwrap_or(true) {
                let spec = indexes.iter().find_map(|i| match &i.kind {
                    index::Kind::Geo(gs) if gs.field == *path => Some(gs.clone()),
                    _ => None,
                });
                if let Some(r) = near_search(g, coll, indexes, o, path, n, spec.as_ref()).await? {
                    return Ok(r);
                }
            }
        }
        // (transactions with writes of their own to the collection: read every candidate)
        (docs, stats, plan) = find_docs_inner(g, coll, indexes, &o3).await?;
    }
    if let Some((path, n)) = near {
        let spec = indexes.iter().find_map(|i| match &i.kind {
            index::Kind::Geo(gs) if gs.field == path => Some(gs.clone()),
            _ => None,
        });
        docs = order_by_near(docs, &path, &n, spec.as_ref());
    }
    Ok((docs, stats, plan))
}

/// Nearest-first search over a geo index: rings of growing radius around the query point, each reading only the index
/// entries the earlier rings did not cover, until `want` documents lie within the covered radius (all of them when the
/// caller wants everything or the ring reaches `$maxDistance` / the whole index). Same order as sorting every candidate:
/// distance, then index order of the matched location, then `_id`.
async fn near_search(g: &mut impl Rd, coll: &str, indexes: &[IndexDef], o: &FindOpts<'_>, path: &str, n: &crate::geo_query::Near, spec: Option<&crate::geo_query::GeoSpec>) -> R<Option<(Vec<Document>, Stats, Plan)>> {
    let hint = match &o.hint {
        Hint::Index(i) => Some(*i),
        _ => None,
    };
    // distance units: meters for a GeoJSON query, radians for $nearSphere, coordinate units on a flat index
    let planar = spec.map(|s| !s.sphere).unwrap_or(false);
    let (mut radius, limit) = if planar && !n.spherical {
        let s = spec.unwrap();
        ((s.max - s.min) / 1000.0, (s.max - s.min) * std::f64::consts::SQRT_2)
    } else if n.meters {
        (3000.0, std::f64::consts::PI * crate::geo::EARTH_RADIUS_M)
    } else {
        (0.0005, std::f64::consts::PI)
    };
    let cap = if n.max.is_finite() { n.max.min(limit) } else { limit };
    // an explicit sort may reorder the documents: then every candidate is needed
    let want = if o.sort.is_empty() { o.want } else { None };
    let mut stats = Stats::default();
    let mut covered: Vec<(u64, u64)> = Vec::new();
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut pool: Vec<(f64, u64, Vec<u8>, Document)> = Vec::new();
    let mut first_plan: Option<Plan> = None;
    loop {
        let r = radius.min(cap);
        let Some((scan, cov, whole)) = index::geo_near_scan(indexes, o.filter, hint, n, r, &covered) else { return Ok(None) };
        covered = cov;
        let plan = Plan::Ix(scan.clone());
        let mut c = IxCursor::new(coll, &indexes[scan.idx], &scan);
        let mut ids: Vec<Vec<u8>> = Vec::new();
        loop {
            let b = c.next_batch(g, usize::MAX).await?;
            if b.is_empty() {
                break;
            }
            stats.keys += b.len();
            ids.extend(b.iter().filter_map(|k| entry_id(k)).filter(|id| seen.insert(id.to_vec())).map(|id| id.to_vec()));
        }
        if !ids.is_empty() {
            for mut d in docs_by_ids(g, coll, &ids).await? {
                stats.docs += 1;
                if !(visible(&d, o) && matches(&d, o.filter)?) {
                    continue;
                }
                let elems: Vec<(crate::geo::Elem, Bson)> = crate::query::path_values(&d, path).iter().flat_map(|v| crate::geo::elements_raw(v)).collect();
                let (dist, loc) = crate::geo_query::near_distance(n, &elems).unwrap_or((f64::INFINITY, Bson::Null));
                d.insert(crate::geo::DIST_KEY, dist);
                d.insert(crate::geo::POINT_KEY, loc);
                let tie = spec.map(|s| crate::geo_query::tie_key(s, d.get(crate::geo::POINT_KEY).unwrap_or(&Bson::Null))).unwrap_or(0);
                let id = encode_sortable(d.get("_id").unwrap_or(&Bson::Null));
                pool.push((dist, tie, id, d));
            }
        }
        first_plan.get_or_insert(plan);
        let exhausted = whole || r >= cap;
        let safe = if exhausted { f64::INFINITY } else { r * (1.0 - 1e-9) };
        let certain = pool.iter().filter(|p| p.0 <= safe).count();
        if exhausted || want.map(|w| certain >= w).unwrap_or(false) {
            pool.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal).then(a.1.cmp(&b.1)).then_with(|| a.2.cmp(&b.2)));
            let ordered: Vec<(f64, Document)> = pool.into_iter().filter(|p| p.0 <= safe).map(|p| (p.0, p.3)).collect();
            let mut docs: Vec<Document> = heap_ties(ordered).into_iter().map(|(_, d)| d).collect();
            if let Some(w) = want {
                docs.truncate(w);
            }
            return Ok(Some((docs, stats, first_plan.unwrap())));
        }
        radius *= 4.0;
    }
}

/// Documents at exactly the same distance come out of mongod's nearest-first search in the pop order of a binary heap
/// (libstdc++'s `std::priority_queue`), not in insertion order. For a group of four or more the order is reproduced by
/// running the same push / pop sequence over the group (exact when the group is alone in its search interval).
fn heap_ties<T>(items: Vec<(f64, T)>) -> Vec<(f64, T)> {
    // items are in index order, sorted by distance
    let mut out: Vec<(f64, T)> = Vec::with_capacity(items.len());
    let mut group: Vec<(f64, T)> = Vec::new();
    let flush = |group: &mut Vec<(f64, T)>, out: &mut Vec<(f64, T)>| {
        if group.len() < 4 {
            out.append(group);
            return;
        }
        let n = group.len();
        let mut slots: Vec<Option<(f64, T)>> = group.drain(..).map(Some).collect();
        // heap of positions; all keys are equal, so the comparison is always false
        let mut h: Vec<usize> = Vec::with_capacity(n);
        for i in 0..n {
            h.push(i); // pushing an element equal to its parent never moves it
        }
        while !h.is_empty() {
            let top = h[0];
            let value = h.pop().unwrap();
            let len = h.len();
            if len > 0 {
                let mut hole = 0usize;
                let mut second = 0usize;
                while second < (len - 1) / 2 {
                    second = 2 * (second + 1); // the right child: equal keys never prefer the left one
                    h[hole] = h[second];
                    hole = second;
                }
                if len % 2 == 0 && second == (len - 2) / 2 {
                    second = 2 * (second + 1);
                    h[hole] = h[second - 1];
                    hole = second - 1;
                }
                h[hole] = value; // the value never moves up again over equal keys
            }
            out.push(slots[top].take().unwrap());
        }
    };
    for it in items {
        if group.last().map(|l| l.0 == it.0).unwrap_or(false) || group.is_empty() {
            group.push(it);
        } else {
            flush(&mut group, &mut out);
            group.push(it);
        }
    }
    flush(&mut group, &mut out);
    out
}

/// Nearest first (ties in index order, then by `_id`), with the distance and the matched location attached as hidden metadata.
pub fn order_by_near(mut docs: Vec<Document>, path: &str, n: &crate::geo_query::Near, spec: Option<&crate::geo_query::GeoSpec>) -> Vec<Document> {
    docs.sort_by(|a, b| crate::query::compare(a.get("_id").unwrap_or(&Bson::Null), b.get("_id").unwrap_or(&Bson::Null)));
    let mut keyed: Vec<(f64, u64, Document)> = Vec::with_capacity(docs.len());
    for mut d in docs {
        let elems: Vec<(crate::geo::Elem, Bson)> = crate::query::path_values(&d, path).iter().flat_map(|v| crate::geo::elements_raw(v)).collect();
        let (dist, loc) = crate::geo_query::near_distance(n, &elems).unwrap_or((f64::INFINITY, Bson::Null));
        d.insert(crate::geo::DIST_KEY, dist);
        d.insert(crate::geo::POINT_KEY, loc);
        let tie = spec.map(|s| crate::geo_query::tie_key(s, d.get(crate::geo::POINT_KEY).unwrap_or(&Bson::Null))).unwrap_or(0);
        keyed.push((dist, tie, d));
    }
    keyed.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal).then(a.1.cmp(&b.1)));
    heap_ties(keyed.into_iter().map(|(dist, _, d)| (dist, d)).collect()).into_iter().map(|(_, d)| d).collect()
}

async fn find_docs_inner(g: &mut impl Rd, coll: &str, indexes: &[IndexDef], o: &FindOpts<'_>) -> R<(Vec<Document>, Stats, Plan)> {
    let plan = plan_for(indexes, o);
    let mut stats = Stats::default();
    let prefix = doc_prefix(coll);
    let ov_coll: Vec<(&Vec<u8>, &Option<Document>)> = o.ov.map(|m| m.iter().filter(|(k, _)| k.starts_with(&prefix)).collect()).unwrap_or_default();
    let ov_keys: HashSet<&Vec<u8>> = ov_coll.iter().map(|(k, _)| *k).collect();
    let sorted_by_plan = o.sort.is_empty() || matches!(&plan, Plan::Ix(s) if s.sorted);
    let early = o.want.filter(|_| sorted_by_plan && ov_coll.is_empty());
    let mut out: Vec<Document> = Vec::new();
    let mut ov_done: HashSet<Vec<u8>> = HashSet::new();

    match &plan {
        Plan::Id(id) => {
            stats.keys = 1;
            let k = doc_key(coll, id);
            if !ov_keys.contains(&k) {
                if let Some(v) = g.get(&k).await? {
                    stats.docs = 1;
                    let d = decode(&v)?;
                    if visible(&d, o) && matches(&d, o.filter)? {
                        out.push(d);
                    }
                }
            }
        }
        Plan::Coll { reverse } if crate::natural::enabled(g, coll).await? => {
            tenant_bench::counting::set_phase(3);
            let np = crate::natural::prefix(coll);
            let chunk = early.map(|w| (w * 2).max(256)).unwrap_or(4096);
            let (mut start, mut stop) = (np.clone(), prefix_end(&np));
            let mut replaced: HashSet<Vec<u8>> = HashSet::new();
            'nat: loop {
                let rows = g.scan_range(&start, stop.as_deref(), *reverse, Some(chunk), INDEX_READ_AHEAD).await?;
                let n = rows.len();
                let ids: Vec<Vec<u8>> = rows.iter().map(|(_, v)| v.to_vec()).collect();
                for stored in docs_by_ids(g, coll, &ids).await? {
                    stats.docs += 1;
                    let mut d = stored;
                    if !ov_keys.is_empty() {
                        let k = doc_key(coll, d.get("_id").unwrap_or(&Bson::Null));
                        let mut rk = b"reins/".to_vec();
                        rk.extend(&k);
                        if o.ov.map(|m| m.contains_key(&rk)).unwrap_or(false) {
                            continue; // deleted and inserted again in this transaction: listed at the end
                        }
                        if let Some(v) = o.ov.and_then(|m| m.get(&k)) {
                            replaced.insert(k);
                            match v {
                                Some(nd) => d = nd.clone(), // updated in this transaction: same position
                                None => continue,
                            }
                        }
                    }
                    if visible(&d, o) && matches(&d, o.filter)? {
                        out.push(d);
                        if early.map(|w| out.len() >= w).unwrap_or(false) {
                            break 'nat;
                        }
                    }
                }
                if n < chunk {
                    break;
                }
                let last = rows.last().unwrap().0.to_vec();
                if *reverse {
                    stop = Some(last);
                } else {
                    let mut k = last;
                    k.push(0);
                    start = k;
                }
            }
            stats.keys = stats.docs;
            ov_done = replaced;
        }
        Plan::Coll { reverse } => {
            tenant_bench::counting::set_phase(3);
            let end = prefix_end(&prefix);
            let chunk = early.map(|w| (w * 2).max(256)).unwrap_or(usize::MAX);
            let (mut start, mut stop) = (prefix.clone(), end.clone());
            'scan: loop {
                let rows = g.scan_range(&start, stop.as_deref(), *reverse, Some(chunk), DOC_READ_AHEAD).await?;
                let n = rows.len();
                for (k, v) in &rows {
                    stats.docs += 1;
                    if ov_keys.contains(&k.to_vec()) {
                        continue;
                    }
                    let d = decode(v)?;
                    if visible(&d, o) && matches(&d, o.filter)? {
                        out.push(d);
                        if early.map(|w| out.len() >= w).unwrap_or(false) {
                            break 'scan;
                        }
                    }
                }
                if n < chunk {
                    break;
                }
                let last = rows.last().unwrap().0.to_vec();
                if *reverse {
                    stop = Some(last);
                } else {
                    let mut k = last;
                    k.push(0);
                    start = k;
                }
            }
            stats.keys = stats.docs;
        }
        Plan::Ix(s) if early.is_some() => {
            let want = early.unwrap();
            let mut c = IxCursor::new(coll, &indexes[s.idx], s);
            let mut seen: HashSet<Vec<u8>> = HashSet::new();
            let mut batch = want.max(1);
            'ix: loop {
                let b = c.next_batch(g, batch).await?;
                batch = (batch * 2).min(4096);
                if b.is_empty() {
                    break;
                }
                stats.keys += b.len();
                let ids: Vec<Vec<u8>> = b.iter().filter_map(|k| entry_id(k)).filter(|id| seen.insert(id.to_vec())).map(|id| id.to_vec()).collect();
                for d in docs_by_ids(g, coll, &ids).await? {
                    stats.docs += 1;
                    if visible(&d, o) && matches(&d, o.filter)? {
                        out.push(d);
                        if out.len() >= want {
                            break 'ix;
                        }
                    }
                }
            }
        }
        plan => {
            let ids = plan_docs_ids(g, coll, indexes, plan, &mut stats).await?;
            let mut seen: HashSet<Vec<u8>> = HashSet::new();
            let ids: Vec<Vec<u8>> = ids.into_iter().filter(|id| seen.insert(id.clone())).collect();
            let docs = docs_by_ids(g, coll, &ids).await?;
            stats.docs += docs.len();
            for d in docs {
                if !ov_keys.is_empty() {
                    if let Some(id) = d.get("_id") {
                        if ov_keys.contains(&doc_key(coll, id)) {
                            continue;
                        }
                    }
                }
                if visible(&d, o) && matches(&d, o.filter)? {
                    out.push(d);
                }
            }
        }
    }
    let mut ov_coll = ov_coll;
    if let Some(m) = o.ov {
        let seq_of = |k: &Vec<u8>| {
            let mut sk = b"oseq/".to_vec();
            sk.extend(k);
            m.get(&sk).and_then(|d| d.as_ref()).and_then(|d| d.get_i64("s").ok()).unwrap_or(i64::MAX)
        };
        ov_coll.sort_by_key(|(k, _)| seq_of(k));
    }
    for (k, v) in ov_coll {
        if ov_done.contains(k) {
            continue;
        }
        if let Some(d) = v {
            if visible(d, o) && matches(d, o.filter)? {
                out.push(d.clone());
            }
        }
    }
    Ok((out, stats, plan))
}

/// One command's (or transaction's) writes.
pub struct Writes {
    pub ops: Vec<Op>,
    pub ov: Overlay,
    /// (index position, entry components) -> `_id` encoding that holds it in this write set.
    claims: HashMap<(usize, Vec<u8>), Vec<u8>>,
    /// Entries removed by this write set.
    released: HashSet<(usize, Vec<u8>, Vec<u8>)>,
    /// Index definitions changed (multikey flag set).
    pub indexes_dirty: bool,
    /// Index definitions to persist at commit (collection -> all its indexes) and the collection list, for transactions.
    pub meta: HashMap<String, Vec<IndexDef>>,
    pub names: Option<Vec<String>>,
    /// Transaction: write counter at its snapshot; writing a document that changed since is a write conflict.
    pub snap_seq: Option<u64>,
    pub txn_id: Option<u64>,
    /// Change-stream events of this write set, published when it is applied.
    pub events: Vec<crate::changes::EvSpec>,
    /// The update document of the next `put` (an operator update); `None` for inserts and replacements.
    pub pending_update: Option<Document>,
    /// Insertion sequence numbers (see `natural.rs`) of the documents written by this set, by document key.
    seqs: HashMap<Vec<u8>, u64>,
}

impl Writes {
    pub fn new() -> Self {
        Writes { ops: Vec::new(), ov: HashMap::new(), claims: HashMap::new(), released: HashSet::new(), indexes_dirty: false, meta: HashMap::new(), names: None, snap_seq: None, txn_id: None, events: Vec::new(), pending_update: None, seqs: HashMap::new() }
    }

    /// Current version of a document: this write set first, then the store.
    pub async fn get(&self, g: &mut Entry, coll: &str, id: &Bson) -> R<Option<Document>> {
        let k = doc_key(coll, id);
        if let Some(v) = self.ov.get(&k) {
            return Ok(v.clone());
        }
        Ok(match g.get(&k).await? {
            Some(v) => Some(decode(&v)?),
            None => None,
        })
    }

    /// Does a document with this `_id` exist now (latest committed state plus this write set)?
    pub async fn exists(&self, g: &mut Entry, coll: &str, id: &Bson) -> R<bool> {
        let k = doc_key(coll, id);
        if let Some(v) = self.ov.get(&k) {
            return Ok(v.is_some());
        }
        Ok(g.get_live(&k).await?.is_some())
    }

    /// All ops to apply at commit: the document and index writes, changed index definitions, the collection list.
    pub fn final_ops(mut self) -> R<Vec<Op>> {
        let mut ops = std::mem::take(&mut self.ops);
        for (coll, idx) in &self.meta {
            ops.push(Entry::indexes_op(coll, idx)?);
        }
        if let Some(names) = &self.names {
            ops.push(Entry::collections_op(names)?);
        }
        Ok(ops)
    }

    fn check_conflict(&self, g: &mut Entry, key: &[u8]) -> R<()> {
        if let Some(id) = self.txn_id {
            if !g.lock_key(key, id) {
                return Err(MongoError::write_conflict());
            }
        }
        match self.snap_seq {
            Some(seq) if g.modified_after(key, seq) => Err(MongoError::write_conflict()),
            _ => Ok(()),
        }
    }

    /// Insert or replace a document, maintaining every index. Nothing is recorded if it fails.
    pub async fn put(&mut self, g: &mut Entry, coll: &str, ns: &str, indexes: &mut [IndexDef], new: &Document, old: Option<&Document>) -> R<()> {
        let id = new.get("_id").cloned().ok_or_else(|| MongoError::internal("document without _id".into()))?;
        let id_enc = encode_sortable(&id);
        self.check_conflict(g, &doc_key(coll, &id))?;
        let mut plans: Vec<(usize, Vec<Vec<u8>>, Vec<Vec<u8>>, bool)> = Vec::new(); // (idx, to_add, to_remove, multikey)
        for (i, idx) in indexes.iter().enumerate() {
            if !idx.has_entries() || idx.state == State::Dropping {
                continue;
            }
            let (new_e, mk) = idx.entries(new)?;
            let old_e = match old {
                Some(o) => idx.entries(o)?.0,
                None => Default::default(),
            };
            let add: Vec<Vec<u8>> = new_e.keys().filter(|k| !old_e.contains_key(*k)).cloned().collect();
            let rem: Vec<Vec<u8>> = old_e.keys().filter(|k| !new_e.contains_key(*k)).cloned().collect();
            if idx.unique && idx.state == State::Ready {
                for c in &add {
                    let dup = || MongoError::duplicate_key(ns, &idx.name, &idx.key_pattern(), new_e.get(c).unwrap());
                    if let Some(owner) = self.claims.get(&(i, c.clone())) {
                        if *owner != id_enc {
                            return Err(dup());
                        }
                        continue;
                    }
                    let mut p = idx.prefix(coll);
                    p.extend(c);
                    let end = prefix_end(&p);
                    for (k, _) in g.scan_range_live(&p, end.as_deref(), Some(8)).await? {
                        if let Some(owner) = entry_id(&k) {
                            if owner != id_enc.as_slice() && !self.released.contains(&(i, c.clone(), owner.to_vec())) {
                                return Err(dup());
                            }
                        }
                    }
                }
            }
            plans.push((i, add, rem, mk));
        }
        let k = doc_key(coll, &id);
        let mut bytes = new.to_vec()?;
        let seq = match old {
            None => {
                if matches!(self.ov.get(&k), Some(None)) {
                    // deleted earlier in this write set and inserted again: it moves to the end
                    let mut rk = b"reins/".to_vec();
                    rk.extend(&k);
                    self.ov.insert(rk, Some(Document::new()));
                }
                let s = crate::natural::alloc(g, coll).await?;
                self.ops.push(Op::Put(crate::natural::key(coll, s), id_enc.clone()));
                Some(s)
            }
            Some(_) => match self.seqs.get(&k) {
                Some(s) => Some(*s),
                None => crate::natural::stored_seq(g, &k).await?,
            },
        };
        // Commit to the write set.
        for (i, add, rem, mk) in plans {
            let idx = &mut indexes[i];
            let prefix = idx.prefix(coll);
            for c in rem {
                self.ops.push(Op::Delete(entry_key(&prefix, &c, &id_enc)));
                self.claims.remove(&(i, c.clone()));
                self.released.insert((i, c, id_enc.clone()));
            }
            for c in add {
                self.ops.push(Op::Put(entry_key(&prefix, &c, &id_enc), crate::natural::entry_value(seq)));
                self.released.remove(&(i, c.clone(), id_enc.clone()));
                self.claims.insert((i, c), id_enc.clone());
            }
            if mk && !idx.multikey {
                idx.multikey = true;
                self.indexes_dirty = true;
            }
        }
        if let Some(s) = seq {
            bytes = crate::natural::with_seq(bytes, s);
            self.seqs.insert(k.clone(), s);
            let mut sk = b"oseq/".to_vec();
            sk.extend(&k);
            self.ov.insert(sk, Some(bson::doc! { "s": s as i64 })); // lets readers of the overlay list new documents in insertion order
        }
        self.ops.push(Op::Put(k.clone(), bytes));
        self.ov.insert(k, Some(new.clone()));
        let db = ns.split('.').next().unwrap_or("").to_string();
        let spec = match (old, self.pending_update.take()) {
            (None, _) => crate::changes::EvSpec { db, coll: Some(coll.to_string()), op: "insert", doc_key: Some(id.clone()), full: Some(new.clone()), desc: None, txn: None },
            (Some(o), Some(u)) => crate::changes::EvSpec { db, coll: Some(coll.to_string()), op: "update", doc_key: Some(id.clone()), full: None, desc: Some(crate::changes::update_description(o, new, &u)), txn: None },
            (Some(_), None) => crate::changes::EvSpec { db, coll: Some(coll.to_string()), op: "replace", doc_key: Some(id.clone()), full: Some(new.clone()), desc: None, txn: None },
        };
        self.events.push(spec);
        Ok(())
    }

    pub async fn delete(&mut self, g: &mut Entry, db: &str, coll: &str, indexes: &[IndexDef], old: &Document) -> R<()> {
        let id = old.get("_id").cloned().unwrap_or(Bson::Null);
        self.check_conflict(g, &doc_key(coll, &id))?;
        let id_enc = encode_sortable(&id);
        for (i, idx) in indexes.iter().enumerate() {
            if !idx.has_entries() || idx.state == State::Dropping {
                continue;
            }
            let prefix = idx.prefix(coll);
            for c in idx.entries(old)?.0.into_keys() {
                self.ops.push(Op::Delete(entry_key(&prefix, &c, &id_enc)));
                self.claims.remove(&(i, c.clone()));
                self.released.insert((i, c, id_enc.clone()));
            }
        }
        let k = doc_key(coll, &id);
        let seq = match self.seqs.remove(&k) {
            Some(s) => Some(s),
            None => crate::natural::stored_seq(g, &k).await?,
        };
        if let Some(s) = seq {
            self.ops.push(Op::Delete(crate::natural::key(coll, s)));
        }
        self.ops.push(Op::Delete(k.clone()));
        self.ov.insert(k, None);
        self.events.push(crate::changes::EvSpec { db: db.to_string(), coll: Some(coll.to_string()), op: "delete", doc_key: Some(id), full: None, desc: None, txn: None });
        Ok(())
    }
}
