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
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
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
    /// The options of a collection (its default collation).
    fn coll_opts(&mut self, coll: &str) -> impl std::future::Future<Output = anyhow::Result<Option<Arc<crate::collopts::CollOpts>>>> + Send;
    /// The database's name (T1: the read-ahead of `prefetch.rs` reads it on its own).
    fn tenant_name(&self) -> Option<String> {
        None
    }
}

impl Rd for Entry {
    fn tenant_name(&self) -> Option<String> {
        Some(self.name().to_string())
    }
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
    async fn coll_opts(&mut self, coll: &str) -> anyhow::Result<Option<Arc<crate::collopts::CollOpts>>> {
        Entry::coll_opts(self, coll).await
    }
}

impl Rd for tokio::sync::OwnedRwLockWriteGuard<Entry> {
    fn tenant_name(&self) -> Option<String> {
        Some(self.name().to_string())
    }
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
    async fn coll_opts(&mut self, coll: &str) -> anyhow::Result<Option<Arc<crate::collopts::CollOpts>>> {
        Entry::coll_opts(self, coll).await
    }
}

impl Rd for ReadView {
    fn tenant_name(&self) -> Option<String> {
        ReadView::tenant_name(self)
    }
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
    async fn coll_opts(&mut self, coll: &str) -> anyhow::Result<Option<Arc<crate::collopts::CollOpts>>> {
        ReadView::coll_opts(self, coll).await
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
            indexes.iter().position(|i| i.name == *name && i.state == State::Ready && !i.hidden).map(Hint::Index).ok_or_else(bad)
        }
        Some(Bson::Document(d)) => {
            if let Some(n) = d.get("$natural") {
                let asc = !matches!(n, Bson::Int32(-1) | Bson::Int64(-1)) && !matches!(n, Bson::Double(f) if *f < 0.0);
                return Ok(Hint::Natural(asc));
            }
            if d.len() == 1 && d.contains_key("_id") {
                return Ok(Hint::None);
            }
            indexes.iter().position(|i| i.state == State::Ready && !i.hidden && crate::index::same_key_order(&i.key_pattern(), d)).map(Hint::Index).ok_or_else(bad)
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
        return docs_by_scan(g, coll, ids).await;
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
    let mut out = Vec::with_capacity(ids.len());
    for (i, v) in vals.into_iter().enumerate() {
        match v {
            Some(v) => out.push(decode(&v)?),
            None => {
                if let Some(d) = moved_doc(g, coll, &ids[i]).await? {
                    out.push(d);
                }
            }
        }
    }
    Ok(out)
}

/// A customer-keyed document not found under the suffix an index entry (or natural-order entry) gave: its customer
/// changed after the entry was read, and it moved. The locator (`_id` -> current suffix) finds it where it is now, so a
/// read through an index does not miss a document that a concurrent update moved to another customer.
async fn moved_doc(g: &mut impl Rd, coll: &str, suffix: &[u8]) -> R<Option<Document>> {
    let Some(key) = crate::custkey::of(g, coll).await? else { return Ok(None) };
    let Some((_, id_enc)) = crate::custkey::split_dsuf(suffix, key.order.is_some()) else { return Ok(None) };
    // every customer holding this `_id` (it is unique per customer only): the document moved if exactly one other place
    // holds it now (with two or more, the moved one cannot be told apart, and none is returned)
    let lp = crate::custkey::loc_id_prefix(coll, id_enc);
    let rows = g.scan_range(&lp, prefix_end(&lp).as_deref(), false, Some(3), 4096).await?;
    let now: Vec<&Bytes> = rows.iter().map(|(_, v)| v).filter(|v| v.as_ref() != suffix).collect();
    if now.len() != 1 || rows.len() != 1 {
        return Ok(None);
    }
    Ok(match g.get_many(&[[doc_prefix(coll), now[0].to_vec()].concat()]).await?.pop().flatten() {
        Some(v) => Some(decode(&v)?),
        None => None,
    })
}

/// The documents with these ids (in the order of `ids`) from one ascending range scan over the id span, capped
/// ([`scan_span`]): the ids the scan did not reach are read by point reads.
async fn docs_by_scan(g: &mut impl Rd, coll: &str, ids: &[Vec<u8>]) -> R<Vec<Document>> {
    let prefix_v = doc_prefix(coll);
    let prefix = &prefix_v[..];
    let mut found: HashMap<Vec<u8>, Document> = HashMap::new();
    let rest = scan_span(g, prefix, ids, |id, v| {
        found.insert(id.to_vec(), decode(v)?);
        Ok(())
    })
    .await?;
    if let Some(from) = rest {
        let mut late: Vec<&Vec<u8>> = ids.iter().filter(|id| !found.contains_key(*id) && reached_not(prefix, id, &from)).collect();
        late.sort();
        late.dedup();
        let keys: Vec<Vec<u8>> = late.iter().map(|id| [prefix, &id[..]].concat()).collect();
        for (id, v) in late.into_iter().zip(g.get_many(&keys).await?) {
            if let Some(v) = v {
                found.insert(id.clone(), decode(&v)?);
            }
        }
    }
    for id in ids {
        if !found.contains_key(id) {
            if let Some(d) = moved_doc(g, coll, id).await? {
                found.insert(id.clone(), d);
            }
        }
    }
    Ok(ids.iter().filter_map(|i| found.get(i).cloned()).collect())
}

/// P-fix-read-amplification: the rows an id-span scan for `n` documents may read before it stops and leaves the rest to
/// point reads. The span of the ids an index read wants is not the wanted rows: an index over many customers (or any
/// order other than the documents' keys) wants a few documents spread over the whole collection, and the uncapped scan
/// read every row between them (MEASURED: about 4,600 rows per document examined on a 3 GiB generated database after
/// updates, 1.6 GB fetched for 800 documents). With the cap a fetch reads at most about 2 rows per wanted document
/// plus a constant, at any database size; a dense span (the case the scan is for) still ends within it.
fn span_budget(n: usize) -> usize {
    2 * n + 32
}

/// The cap of [`span_budget`] (on by default; `FENVIX_NO_SPAN_BUDGET=1` turns it off: measurements and the negative
/// control of `ctl-tests/test_84_read_amplification.py`).
fn span_budget_on() -> bool {
    static OFF: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    !*OFF.get_or_init(|| std::env::var("FENVIX_NO_SPAN_BUDGET").is_ok_and(|v| v == "1"))
}

/// Id-span scans for a fetch of documents, rows they read, and those that stopped at their cap (serverStatus
/// `fenvix.fetch`, diagnostics and tests).
pub static FETCH_SCANS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static FETCH_SCAN_ROWS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static FETCH_SCAN_STOPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Whether the document key of `id` lies at or after `from`: the scan that stopped at `from` did not reach it.
fn reached_not(prefix: &[u8], id: &[u8], from: &[u8]) -> bool {
    [prefix, id].concat()[..] >= from[..]
}

/// One ascending scan over the span of `ids` (the smallest to the largest), handing every row whose id is wanted to
/// `f` (once per row, whatever the number of times an id is in `ids`). It reads at most [`span_budget`] rows: `None`
/// when it reached the end of the span, else `Some(k)`, the key where it stopped; the wanted ids whose key is at or
/// after `k` ([`reached_not`]) were not looked at, and the caller reads them by point reads.
async fn scan_span(g: &mut impl Rd, prefix: &[u8], ids: &[Vec<u8>], mut f: impl FnMut(&[u8], &Bytes) -> R<()>) -> R<Option<Vec<u8>>> {
    use std::sync::atomic::Ordering::Relaxed;
    let (Some(lo), Some(hi)) = (ids.iter().min(), ids.iter().max()) else { return Ok(None) };
    let want: HashSet<&[u8]> = ids.iter().map(|i| i.as_slice()).collect();
    let mut start = [prefix, &lo[..]].concat();
    let mut stop = [prefix, &hi[..]].concat();
    stop.push(0);
    let capped = span_budget_on();
    let mut left = if capped { span_budget(ids.len()) } else { usize::MAX };
    // the first request of a capped scan reads about what its cap needs (rows of 1 KiB), not 4 MiB per run: a sparse span
    // stops within it; the next requests are sized from the rows seen
    let mut read_ahead = if capped { (left.saturating_mul(1024)).clamp(64 << 10, FETCH_SCAN_READ_AHEAD) } else { FETCH_SCAN_READ_AHEAD };
    FETCH_SCANS.fetch_add(1, Relaxed);
    loop {
        if left == 0 {
            FETCH_SCAN_STOPS.fetch_add(1, Relaxed);
            return Ok(Some(start));
        }
        let chunk = FETCH_CHUNK.min(left);
        let rows = g.scan_range(&start, Some(&stop), false, Some(chunk), read_ahead).await?;
        let n = rows.len();
        FETCH_SCAN_ROWS.fetch_add(n as u64, Relaxed);
        if capped {
            left -= n;
            let bytes: usize = rows.iter().map(|(k, v)| k.len() + v.len()).sum();
            read_ahead = (bytes / n.max(1)).saturating_mul(left.min(FETCH_CHUNK)).clamp(64 << 10, FETCH_SCAN_READ_AHEAD);
        }
        for (k, v) in &rows {
            let id = &k[prefix.len()..];
            if want.contains(id) {
                f(id, v)?;
            }
        }
        if n < chunk {
            return Ok(None);
        }
        let mut k = rows.last().unwrap().0.to_vec();
        k.push(0);
        if k >= stop {
            return Ok(None); // the chunk ended on the last wanted key: nothing is left (an empty range would panic the store)
        }
        start = k;
    }
}

const FETCH_CHUNK: usize = 20_000;

/// [`docs_by_ids`] that hands each document to `f`, with its position in `ids`, instead of keeping them all: the same
/// reads (one range scan over the id span, or point reads), at most one chunk of documents in memory.
async fn each_doc_by_ids(g: &mut impl Rd, coll: &str, ids: &[Vec<u8>], mut f: impl FnMut(usize, Document) -> R<()>) -> R<usize> {
    let prefix = doc_prefix(coll);
    tenant_bench::counting::set_phase(2);
    let mut n_docs = 0;
    if prefer_scan(g.approx_bytes(), ids.len()) {
        // an id given twice is handed over once, at its last position (as the uncapped scan did)
        let pos: HashMap<&[u8], usize> = ids.iter().enumerate().map(|(i, id)| (id.as_slice(), i)).collect();
        let mut seen = vec![false; ids.len()];
        let rest = scan_span(g, &prefix, ids, |id, v| {
            let i = pos[id];
            n_docs += 1;
            seen[i] = true;
            f(i, decode(v)?)
        })
        .await?;
        if let Some(from) = rest {
            // the ids the capped scan did not reach: point reads, a chunk at a time
            let late: Vec<usize> = (0..ids.len()).filter(|&i| !seen[i] && pos[ids[i].as_slice()] == i && reached_not(&prefix, &ids[i], &from)).collect();
            for chunk in late.chunks(FETCH_CHUNK) {
                let keys: Vec<Vec<u8>> = chunk.iter().map(|&i| [&prefix[..], &ids[i][..]].concat()).collect();
                for (&i, v) in chunk.iter().zip(g.get_many(&keys).await?) {
                    if let Some(v) = v {
                        n_docs += 1;
                        seen[i] = true;
                        f(i, decode(&v)?)?;
                    }
                }
            }
        }
        for (i, id) in ids.iter().enumerate() {
            if !seen[i] {
                if let Some(d) = moved_doc(g, coll, id).await? {
                    n_docs += 1;
                    f(i, d)?;
                }
            }
        }
        return Ok(n_docs);
    }
    for (c, chunk) in ids.chunks(FETCH_CHUNK).enumerate() {
        let keys: Vec<Vec<u8>> = chunk
            .iter()
            .map(|id| {
                let mut k = prefix.clone();
                k.extend(id);
                k
            })
            .collect();
        for (j, v) in g.get_many(&keys).await?.into_iter().enumerate() {
            let d = match v {
                Some(v) => Some(decode(&v)?),
                None => moved_doc(g, coll, &chunk[j]).await?,
            };
            if let Some(d) = d {
                n_docs += 1;
                f(c * FETCH_CHUNK + j, d)?;
            }
        }
    }
    Ok(n_docs)
}

/// Can the documents of this type of `_id` be taken in the byte order of their keys as their sort order? True for the
/// types whose key encoding ([`encode_sortable`]) follows MongoDB's comparison: not for embedded documents, binary data
/// and the rare types, and not for strings when a collation decides their order. `rank` is the first byte of the key.
fn id_rank_ordered(rank: u8, collated: bool) -> bool {
    match rank {
        1 | 2 | 3 | 8 | 9 | 10 | 11 | 20 => true,
        4 => !collated,
        _ => false,
    }
}


/// How an `_id` equality is looked up in a collection that may have a default collation.
enum IdRes {
    /// The document key of the value itself (no collation involved).
    Direct,
    /// The `_id` is stored under this encoding.
    Found(Vec<u8>),
    /// No document has an `_id` equal to it.
    Absent,
    /// Customer-keyed collection: several customers hold this `_id` (`_id` is unique per customer), stored under these.
    Many(Vec<Vec<u8>>),
    /// The query's collation is not the one that decides which `_id`s are equal: every document is a candidate.
    Scan,
}

async fn resolve_id(g: &mut impl Rd, coll: &str, id: &Bson, ov: Option<&Overlay>) -> R<IdRes> {
    match resolve_id_collated(g, coll, id, ov).await? {
        IdRes::Direct => match crate::custkey::of(g, coll).await? {
            // customer-keyed documents: the locators give the document suffix under every customer holding the `_id`
            Some(_) => {
                let mut sufs = locate(g, coll, id, ov).await?;
                Ok(match sufs.len() {
                    0 => IdRes::Absent,
                    1 => IdRes::Found(sufs.pop().unwrap()),
                    _ => IdRes::Many(sufs),
                })
            }
            None => Ok(IdRes::Direct),
        },
        r => Ok(r),
    }
}

/// The document suffixes of `_id` in a customer-keyed collection (one per customer holding it), from the locators: the
/// latest state, then the overlay of the write set on top.
async fn locate(g: &mut impl Rd, coll: &str, id: &Bson, ov: Option<&Overlay>) -> R<Vec<Vec<u8>>> {
    let lp = crate::custkey::loc_id_prefix(coll, &encode_sortable(id));
    crate::custkey::LOCATOR_LOOKUPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut found: BTreeMap<Vec<u8>, Vec<u8>> = g.scan_range(&lp, prefix_end(&lp).as_deref(), false, None, 4096).await?.into_iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
    if let Some(m) = ov {
        for (k, v) in m.iter().filter(|(k, _)| k.starts_with(&lp)) {
            match v.as_ref().and_then(loc_suffix) {
                Some(suf) => {
                    found.insert(k.clone(), suf);
                }
                None => {
                    found.remove(k);
                }
            }
        }
    }
    Ok(found.into_values().collect())
}

/// The document suffix an overlay entry of a locator holds.
fn loc_suffix(d: &Document) -> Option<Vec<u8>> {
    match d.get("s") {
        Some(Bson::Binary(b)) => Some(b.bytes.clone()),
        _ => None,
    }
}

fn loc_overlay(suf: &[u8]) -> Document {
    bson::doc! { "s": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: suf.to_vec() } }
}

async fn resolve_id_collated(g: &mut impl Rd, coll: &str, id: &Bson, ov: Option<&Overlay>) -> R<IdRes> {
    if !crate::collopts::has_string(id) {
        return Ok(IdRes::Direct);
    }
    let Some(eff) = crate::collation::current() else { return Ok(IdRes::Direct) };
    let coll_c = g.coll_opts(coll).await?.and_then(|o| o.collation.clone());
    if !crate::collation::same(&Some(eff.clone()), &coll_c) {
        return Ok(IdRes::Scan);
    }
    if crate::custkey::of(g, coll).await?.is_some() {
        // a customer-keyed collection: the collation `_id` index is per customer (`custkey::idc_key_of`), every
        // customer holding a collation-equal `_id` is under one prefix (the latest state, then the write set on top)
        let p = crate::custkey::idc_id_prefix(coll, &eff, id);
        let mut found: BTreeMap<Vec<u8>, Vec<u8>> = g.scan_range(&p, prefix_end(&p).as_deref(), false, None, 4096).await?.into_iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
        if let Some(m) = ov {
            for (k, v) in m.iter().filter(|(k, _)| k.starts_with(&p)) {
                match v.as_ref().and_then(loc_suffix) {
                    Some(suf) => {
                        found.insert(k.clone(), suf);
                    }
                    None => {
                        found.remove(k);
                    }
                }
            }
        }
        let mut sufs: Vec<Vec<u8>> = found.into_values().collect();
        return Ok(match sufs.len() {
            0 => IdRes::Absent,
            1 => IdRes::Found(sufs.pop().unwrap()),
            _ => IdRes::Many(sufs),
        });
    }
    let ck = crate::collopts::idc_key(coll, &eff, id);
    if let Some(v) = ov.and_then(|m| m.get(&ck)) {
        return Ok(match v {
            Some(_) => {
                // written in this transaction: the stored encoding is in the pending ops, the value itself is the best guess
                IdRes::Direct
            }
            None => IdRes::Absent,
        });
    }
    Ok(match g.get(&ck).await? {
        Some(v) => IdRes::Found(v.to_vec()),
        None => IdRes::Absent,
    })
}

/// Candidate documents of `plan` (index order), still to be filtered.
async fn plan_docs_ids(g: &mut impl Rd, coll: &str, indexes: &[IndexDef], plan: &Plan, stats: &mut Stats) -> R<Vec<Vec<u8>>> {
    PlanIds::new(plan).next(g, coll, indexes, stats, usize::MAX).await
}

/// The candidate `_id`s of a plan (index order, still to be filtered), a batch at a time: the same list as
/// [`plan_docs_ids`] without holding it whole.
struct PlanIds<'p> {
    stack: Vec<&'p Plan>,
    cur: Option<IdSource>,
}

enum IdSource {
    Ix(IxCursor),
    /// The keys of every document, from `start` on (an `_id` equality the collation does not decide).
    Docs(Vec<u8>),
    /// The `_id`s of an `_id: {$in: [...]}` still to look up, in `_id` order.
    Ids(std::collections::VecDeque<Bson>),
}

impl<'p> PlanIds<'p> {
    fn new(plan: &'p Plan) -> Self {
        PlanIds { stack: vec![plan], cur: None }
    }

    /// The next candidates, at most `n` (more than one call per plan only with `n` smaller than the list); empty at the end.
    async fn next(&mut self, g: &mut impl Rd, coll: &str, indexes: &[IndexDef], stats: &mut Stats, n: usize) -> R<Vec<Vec<u8>>> {
        let mut ids = Vec::new();
        while ids.len() < n {
            match &mut self.cur {
                Some(IdSource::Ix(c)) => {
                    let b = c.next_batch(g, n - ids.len()).await?;
                    if b.is_empty() {
                        self.cur = None;
                        continue;
                    }
                    stats.keys += b.len();
                    ids.extend(b.iter().filter_map(|k| entry_id(k).map(|x| x.to_vec())));
                }
                Some(IdSource::Docs(start)) => {
                    let prefix = doc_prefix(coll);
                    let want = n - ids.len();
                    let rows = g.scan_range(start, prefix_end(&prefix).as_deref(), false, Some(want), DOC_READ_AHEAD).await?;
                    ids.extend(rows.iter().map(|(k, _)| k[prefix.len()..].to_vec()));
                    match rows.last() {
                        Some((k, _)) if rows.len() >= want => {
                            let mut k = k.to_vec();
                            k.push(0);
                            *start = k;
                        }
                        _ => self.cur = None,
                    }
                }
                Some(IdSource::Ids(q)) => {
                    let Some(id) = q.pop_front() else {
                        self.cur = None;
                        continue;
                    };
                    stats.keys += 1;
                    match resolve_id(g, coll, &id, None).await? {
                        IdRes::Direct => ids.push(encode_sortable(&id)),
                        IdRes::Found(enc) => ids.push(enc),
                        IdRes::Many(encs) => ids.extend(encs),
                        IdRes::Absent => {}
                        // (the planner leaves strings under a collation out of this plan; every document, as for `Plan::Id`)
                        IdRes::Scan => self.cur = Some(IdSource::Docs(doc_prefix(coll))),
                    }
                }
                None => {
                    let Some(p) = self.stack.pop() else { break };
                    match p {
                        Plan::Id(id) => match resolve_id(g, coll, id, None).await? {
                            IdRes::Direct => ids.push(encode_sortable(id)),
                            IdRes::Found(enc) => ids.push(enc),
                            IdRes::Many(encs) => ids.extend(encs),
                            IdRes::Absent => {}
                            IdRes::Scan => self.cur = Some(IdSource::Docs(doc_prefix(coll))),
                        },
                        Plan::Ix(s) => self.cur = Some(IdSource::Ix(IxCursor::new(coll, &indexes[s.idx], s))),
                        Plan::Or(subs) => self.stack.extend(subs.iter().rev()),
                        Plan::IdIn(vals) => self.cur = Some(IdSource::Ids(vals.iter().cloned().collect())),
                        Plan::Coll { .. } => {}
                    }
                }
            }
        }
        Ok(ids)
    }
}

/// Candidates read before choosing how to fetch the documents of a blocking sort: when the plan has no more, they are
/// fetched as before (one range scan or point reads, positions exact); otherwise the documents are read a chunk at a
/// time. About 155 bytes each (MISURATO by the reviewer of N5): about 10 MB.
const SORT_BUFFER_IDS: usize = 65_536;

/// [`SORT_BUFFER_IDS`], the chunk of the reads after it ([`FETCH_CHUNK`]) and a forced switch to the scan after that
/// many candidates (none): settings for the tests (`MONGO_SORT_BUFFER_IDS`, `MONGO_SORT_CHUNK_IDS`,
/// `MONGO_SORT_SCAN_AFTER`), so that a small collection takes every path.
fn sort_chunks() -> (usize, usize, usize) {
    static V: std::sync::OnceLock<(usize, usize, usize)> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        let env = |k: &str, d: usize| std::env::var(k).ok().and_then(|v| v.parse::<usize>().ok()).filter(|n| *n > 0).unwrap_or(d);
        (env("MONGO_SORT_BUFFER_IDS", SORT_BUFFER_IDS), env("MONGO_SORT_CHUNK_IDS", FETCH_CHUNK), env("MONGO_SORT_SCAN_AFTER", usize::MAX))
    })
}

/// Where the documents of a blocking sort go: the first `k` in sort order (a limit), or every one (within the memory
/// limit of a blocking sort, [`crate::query::SORT_MEM_LIMIT`]).
enum Sink<'a, 'b> {
    Top(&'b mut crate::query::TopK<'a>),
    All { docs: Vec<(u64, Document)>, ids: HashSet<Vec<u8>>, bytes: usize },
}

impl Sink<'_, '_> {
    /// A matching document at plan position `seq`, with `id` its key: a candidate that comes again (a multikey index,
    /// the branches of an `$or`) counts once, at its first position, as long as positions only grow.
    fn push(&mut self, seq: u64, id: Vec<u8>, d: Document) -> R<()> {
        let limit = crate::query::SORT_MEM_LIMIT.load(std::sync::atomic::Ordering::Relaxed) as usize;
        match self {
            Sink::Top(t) => {
                t.push_once(seq, id, d);
                if t.bytes() > limit {
                    return Err(crate::query::sort_mem_error());
                }
            }
            Sink::All { docs, ids, bytes } => {
                if ids.insert(id) {
                    *bytes += crate::query::bson_size(&d);
                    if *bytes > limit {
                        return Err(crate::query::sort_mem_error());
                    }
                    docs.push((seq, d));
                }
            }
        }
        Ok(())
    }
}

/// May the documents of `plan` be found by a scan of the whole collection and the filter alone? Not when an index
/// decides which documents are candidates beyond the filter (a hint, a sparse or partial index, a special kind).
fn scan_equivalent(plan: &Plan, indexes: &[IndexDef], hint: &Hint) -> bool {
    fn ok(p: &Plan, indexes: &[IndexDef]) -> bool {
        match p {
            Plan::Ix(s) => indexes.get(s.idx).map(|i| matches!(i.kind, index::Kind::Regular) && !i.sparse && i.partial.is_none()).unwrap_or(false),
            Plan::Or(subs) => subs.iter().all(|p| ok(p, indexes)),
            Plan::Id(_) | Plan::IdIn(_) | Plan::Coll { .. } => true,
        }
    }
    matches!(hint, Hint::None) && ok(plan, indexes)
}

/// The candidates of a plan that does not give the sort order, into `sink`, without the whole list of candidates:
/// - few candidates (the plan ends within [`SORT_BUFFER_IDS`]): fetched as before, one range scan or point reads;
/// - more: point reads a chunk at a time in plan order, until a scan of the whole collection would take fewer requests
///   (the choice of [`prefer_scan`] for the candidates so far); then that scan, with the filter, for the rest.
///   Documents equal in sort order keep plan order up to the switch and `_id` order after it (mongod promises no order
///   for them either).
async fn sort_candidates(g: &mut impl Rd, coll: &str, indexes: &[IndexDef], plan: &Plan, o: &FindOpts<'_>, ov_keys: &HashSet<&Vec<u8>>, stats: &mut Stats, sink: &mut Sink<'_, '_>) -> R<()> {
    let (buffer_ids, chunk_ids, scan_after) = sort_chunks();
    let prefix = doc_prefix(coll);
    let ckey = crate::custkey::of(g, coll).await?;
    let in_ov = |d: &Document| !ov_keys.is_empty() && ov_keys.contains(&crate::custkey::doc_key(coll, ckey.as_ref(), d));
    let mut src = PlanIds::new(plan);
    let mut buf: Vec<Vec<u8>> = Vec::new();
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut more = true;
    while buf.len() <= buffer_ids {
        let b = src.next(g, coll, indexes, stats, buffer_ids + 1 - buf.len()).await?;
        if b.is_empty() {
            more = false;
            break;
        }
        buf.extend(b.into_iter().filter(|id| seen.insert(id.clone())));
    }
    drop(seen);
    if !more {
        let ids = &buf;
        stats.docs += each_doc_by_ids(g, coll, ids, |i, d| if !in_ov(&d) && visible(&d, o) && matches(&d, o.filter)? { sink.push(i as u64, ids[i].clone(), d) } else { Ok(()) }).await?;
        return Ok(());
    }
    let scan_ok = scan_equivalent(plan, indexes, &o.hint);
    let mut pos: u64 = 0;
    loop {
        let so_far = pos as usize + buf.len();
        if scan_ok && (prefer_scan(g.approx_bytes(), so_far) || so_far >= scan_after) {
            tenant_bench::counting::set_phase(3);
            let (mut start, stop) = (prefix.clone(), prefix_end(&prefix));
            loop {
                let rows = g.scan_range(&start, stop.as_deref(), false, Some(4096), DOC_READ_AHEAD).await?;
                let n = rows.len();
                for (k, v) in &rows {
                    stats.docs += 1;
                    if ov_keys.contains(&k.to_vec()) {
                        continue;
                    }
                    let d = decode(v)?;
                    if visible(&d, o) && matches(&d, o.filter)? {
                        sink.push(pos, k[prefix.len()..].to_vec(), d)?;
                    }
                    pos += 1;
                }
                if n < 4096 {
                    break;
                }
                let mut k = rows.last().unwrap().0.to_vec();
                k.push(0);
                start = k;
            }
            return Ok(());
        }
        tenant_bench::counting::set_phase(2);
        for chunk in buf.chunks(chunk_ids) {
            let keys: Vec<Vec<u8>> = chunk.iter().map(|id| [prefix.as_slice(), id].concat()).collect();
            for (j, v) in g.get_many(&keys).await?.into_iter().enumerate() {
                let Some(v) = v else { continue };
                stats.docs += 1;
                let d = decode(&v)?;
                if !in_ov(&d) && visible(&d, o) && matches(&d, o.filter)? {
                    sink.push(pos + j as u64, chunk[j].clone(), d)?;
                }
            }
            pos += chunk.len() as u64;
        }
        buf = src.next(g, coll, indexes, stats, chunk_ids).await?;
        if buf.is_empty() {
            return Ok(());
        }
    }
}

fn visible(d: &Document, o: &FindOpts) -> bool {
    match o.ttl {
        Some((specs, now)) => !index::is_expired(d, specs, now),
        None => true,
    }
}

/// What the reads of one command examined, for its profiler entry (K1): keys and documents examined and the plan of
/// its first read, as mongod's `planSummary` writes it. Set around a command by the server (`EXEC_NOTE.scope`).
#[derive(Clone, Debug, Default)]
pub struct ExecNote {
    pub keys: u64,
    pub docs: u64,
    pub plan: Option<String>,
}

tokio::task_local! {
    pub static EXEC_NOTE: std::cell::RefCell<ExecNote>;
}

/// mongod's `planSummary` of a plan: `COLLSCAN`, `IDHACK`, `IXSCAN { a: 1, b: -1 }` (an `$or`: its branches, comma separated).
pub fn plan_summary(plan: &Plan, indexes: &[IndexDef]) -> String {
    match plan {
        Plan::Coll { .. } => "COLLSCAN".to_string(),
        Plan::Id(_) => "IDHACK".to_string(),
        Plan::Ix(s) => match indexes.get(s.idx) {
            Some(i) => format!("IXSCAN {}", crate::update::fmt_doc(&i.key_pattern())),
            None => "IXSCAN".to_string(),
        },
        Plan::Or(v) => v.iter().map(|p| plan_summary(p, indexes)).collect::<Vec<_>>().join(", "),
        Plan::IdIn(_) => "IXSCAN { _id: 1 }".to_string(),
    }
}

/// Documents matching `o.filter`, in plan order (sort not applied unless the index provides it). A `$text` condition
/// needs a text index; the documents of such a query carry their text score in the hidden [`crate::text::SCORE_KEY`] field.
pub async fn find_docs(g: &mut impl Rd, coll: &str, indexes: &[IndexDef], o: &FindOpts<'_>) -> R<(Vec<Document>, Stats, Plan)> {
    // Long `$in`/`$nin` lists: sorted once for the whole query, so each document costs one search per value it holds,
    // not one comparison per list value (fix-query-defects P2: 100,000 ids made every document read compare 100,000
    // values). The lists are taken from this copy of the filter, which lives as long as the scope: see `query::InSets`.
    let r = if crate::query::has_long_in(o.filter) && !crate::query::in_sets_active() {
        let f = o.filter.clone();
        let sets = crate::query::InSets::build(&f);
        let o2 = FindOpts { filter: &f, sort: o.sort, want: o.want, hint: o.hint.clone(), ttl: o.ttl, ov: o.ov };
        crate::query::IN_SETS.scope(std::cell::RefCell::new(sets), find_docs_any(g, coll, indexes, &o2)).await
    } else {
        find_docs_any(g, coll, indexes, o).await
    };
    if let Ok((_, st, plan)) = &r {
        let _ = EXEC_NOTE.try_with(|c| {
            let mut n = c.borrow_mut();
            n.keys += if matches!(plan, Plan::Coll { .. }) { 0 } else { st.keys as u64 };
            n.docs += st.docs as u64;
            if n.plan.is_none() {
                n.plan = Some(plan_summary(plan, indexes));
            }
        });
    }
    r
}

async fn find_docs_any(g: &mut impl Rd, coll: &str, indexes: &[IndexDef], o: &FindOpts<'_>) -> R<(Vec<Document>, Stats, Plan)> {
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
    // No sort, and mongod would read the `_id` index (`_id` conditions, `$in` with the transaction's own writes, an
    // `$or` of `_id` clauses): mongod answers in `_id` order, so a limit or a single write picks the same document as
    // mongod. Before, a scan here answered in insertion order and a transaction's own writes came last
    // (fix-query-defects P5). Not under a collation other than the collection's (mongod cannot use the `_id` index then).
    let implied_id = o.sort.is_empty()
        && matches!(o.hint, Hint::None)
        && (matches!(plan, Plan::IdIn(_)) || (matches!(plan, Plan::Coll { reverse: false }) && crate::query::uses_id_index(o.filter)))
        && match crate::collation::current() {
            None => true,
            Some(eff) => crate::collation::same(&Some(eff), &g.coll_opts(coll).await?.and_then(|c| c.collation.clone())),
        };
    if implied_id {
        let by_id = [("_id".to_string(), true)];
        if o.want.is_some() {
            // the first `want` in `_id` order: the sort-with-a-limit path keeps only those
            let o2 = FindOpts { filter: o.filter, sort: &by_id, want: o.want, hint: Hint::None, ttl: o.ttl, ov: o.ov };
            return find_docs_with(g, coll, indexes, &o2, plan).await;
        }
        let (mut out, st, p) = find_docs_with(g, coll, indexes, o, plan).await?;
        out.sort_by(|a, b| crate::query::compare(a.get("_id").unwrap_or(&Bson::Null), b.get("_id").unwrap_or(&Bson::Null)));
        return Ok((out, st, p));
    }
    let coll_plan = matches!(plan, Plan::Coll { .. });
    match find_docs_with(g, coll, indexes, o, plan).await {
        // Without a limit the planner prefers a scan and a sort to reading a whole index in order; when that sort is
        // over the memory limit of a blocking sort and an index gives the order, the index is read instead (mongod
        // reads it from the start: same answer, no sort).
        Err(e) if e.code == 292 && coll_plan && o.want.is_none() => {
            let (hint, natural) = match &o.hint {
                Hint::None => (None, None),
                Hint::Natural(a) => (None, Some(*a)),
                Hint::Index(i) => (Some(*i), None),
            };
            match index::plan(&index::PlanInput { indexes, filter: o.filter, sort: o.sort, has_limit: true, hint, natural }) {
                ordered @ Plan::Ix(index::IxScan { sorted: true, .. }) => find_docs_with(g, coll, indexes, o, ordered).await,
                _ => Err(e),
            }
        }
        r => r,
    }
}

/// [`find_docs_inner`] with the plan chosen.
async fn find_docs_with(g: &mut impl Rd, coll: &str, indexes: &[IndexDef], o: &FindOpts<'_>, plan: Plan) -> R<(Vec<Document>, Stats, Plan)> {
    let mut plan = plan;
    let mut id_lookup: Option<Vec<Vec<u8>>> = None;
    let mut id_absent = false;
    if let Plan::Id(id) = &plan {
        match resolve_id(g, coll, id, o.ov).await? {
            IdRes::Direct => {}
            IdRes::Found(enc) => id_lookup = Some(vec![enc]),
            IdRes::Many(encs) => id_lookup = Some(encs),
            IdRes::Absent => id_absent = true,
            IdRes::Scan => plan = Plan::Coll { reverse: false },
        }
    }
    let mut stats = Stats::default();
    let prefix = doc_prefix(coll);
    let ckey = crate::custkey::of(g, coll).await?;
    let ov_coll: Vec<(&Vec<u8>, &Option<Document>)> = o.ov.map(|m| m.iter().filter(|(k, _)| k.starts_with(&prefix)).collect()).unwrap_or_default();
    let ov_keys: HashSet<&Vec<u8>> = ov_coll.iter().map(|(k, _)| *k).collect();
    let sorted_by_plan = o.sort.is_empty() || matches!(&plan, Plan::Ix(s) if s.sorted);
    // The plan gives the order: the scan stops after `want` documents. The transaction's own writes to the collection
    // (the overlay) are skipped by the scan and added at the end, so they do not prevent stopping early.
    let early = o.want.filter(|_| sorted_by_plan);
    // A sort the plan does not give, with a limit: only the first `want` documents in sort order are kept while the
    // candidates stream by (not every candidate). Text score and geo distance are attached after this function.
    let meta_sort = o.sort.iter().any(|(f, _)| f == crate::text::SCORE_KEY || f == crate::geo::DIST_KEY);
    let mut topk = o.want.filter(|_| !sorted_by_plan && !meta_sort).map(|k| crate::query::TopK::new(k, o.sort));
    // A sort the plan does not give and no limit: every match is held for the sort (after this function), within the
    // memory limit of a blocking sort, as in mongod (error 292 beyond it).
    // (A sort on `_id` alone over the collection is not one: mongod reads the `_id` index in order; the documents here are
    // stored in `_id` order too.)
    let id_sorted = matches!((o.sort, &plan, &o.hint), ([(f, _)], Plan::Coll { .. }, Hint::None) if f == "_id");
    // (A sort on the text score or the geo distance with a limit is not counted either: those values are attached after
    // this function, which cannot keep only the first `want`; mongod's sort with a limit holds only those. Counting every
    // match would refuse a `$text` search with a limit that mongod answers.)
    let blocking = !o.sort.is_empty() && !sorted_by_plan && !id_sorted && topk.is_none() && !(meta_sort && o.want.is_some());
    let mut sort_bytes: Option<usize> = blocking.then_some(0);
    // Sort on `_id` alone and no index chosen: documents are stored in `_id` order (the `_id` index), so the scan of
    // the documents in key order can stop once the first `want` are certain.
    let mut id_order = match (o.sort, &plan, &o.hint) {
        ([(f, asc)], Plan::Coll { .. }, Hint::None) if f == "_id" && topk.is_some() => Some(*asc),
        _ => None,
    };
    if id_order.is_some() && crate::custkey::of(g, coll).await?.is_some() {
        // customer-first documents: key order is customer then `_id`, not `_id`
        id_order = None;
    }
    // `_id` range conditions: only the keys between the bounds are read (fix-query-defects P3: a 1,000-document range read
    // the whole collection). Documents are stored in `_id` key order; not for customer-first keys.
    let id_range = if matches!(plan, Plan::Coll { reverse: false }) && matches!(o.hint, Hint::None) && ckey.is_none() {
        crate::query::id_range(o.filter).and_then(|(lo, hi)| crate::layout::id_key_range(&prefix, lo.as_ref(), hi.as_ref(), crate::collation::current().is_some()))
    } else {
        None
    };
    if id_order.is_some() {
        if let Some(eff) = crate::collation::current() {
            // `_id`s are unique under the collection's collation; under another one two `_id`s can be equal, and their
            // order (the plan's) is not the key order: then the documents are read in plan order (still top-k)
            let coll_c = g.coll_opts(coll).await?.and_then(|c| c.collation.clone());
            if !crate::collation::same(&Some(eff), &coll_c) {
                id_order = None;
            }
        }
    }
    let mut out: Vec<Document> = Vec::new();
    let mut ov_done: HashSet<Vec<u8>> = HashSet::new();

    match &plan {
        Plan::Id(_) if id_absent => {}
        Plan::Id(id) => {
            stats.keys = 1;
            let keys: Vec<Vec<u8>> = match &id_lookup {
                Some(encs) => encs.iter().map(|enc| [prefix.as_slice(), enc.as_slice()].concat()).collect(),
                None => vec![doc_key(coll, id)],
            };
            let mut tried: HashSet<Vec<u8>> = HashSet::new();
            let mut docs: Vec<Document> = Vec::new();
            let mut pending = keys;
            // a customer-keyed document that moved (its customer or time changed) after the locators were read: the
            // locators, read again, give where it is now (at most 3 rounds)
            for round in 0..4 {
                let mut missed = false;
                for k in std::mem::take(&mut pending) {
                    if !tried.insert(k.clone()) || ov_keys.contains(&k) {
                        continue;
                    }
                    // (read like the locators: see `Entry::get_doc`)
                    let v = if id_lookup.is_some() { g.get_many(std::slice::from_ref(&k)).await?.pop().flatten() } else { g.get(&k).await? };
                    match v {
                        Some(v) => docs.push(decode(&v)?),
                        None => missed = true,
                    }
                }
                if !missed || id_lookup.is_none() || round == 3 {
                    break;
                }
                pending = locate(g, coll, id, o.ov).await?.into_iter().map(|enc| [prefix.as_slice(), enc.as_slice()].concat()).filter(|k| !tried.contains(k)).collect();
                if pending.is_empty() {
                    break;
                }
            }
            for d in docs {
                stats.docs += 1;
                if visible(&d, o) && matches(&d, o.filter)? {
                    match topk.as_mut() {
                        Some(t) => t.push(d),
                        None => {
                            charge(&mut sort_bytes, &d)?;
                            out.push(d)
                        }
                    }
                }
            }
        }
        Plan::Coll { .. } if id_order.is_some() => {
            tenant_bench::counting::set_phase(3);
            let asc = id_order.unwrap();
            let t = topk.as_mut().unwrap();
            let collated = crate::collation::current().is_some();
            let mut chunk = (o.want.unwrap_or(1) * 2).max(256);
            let (mut start, mut stop) = match &id_range {
                Some((s, e)) => (s.clone(), e.clone()),
                None => (prefix.clone(), prefix_end(&prefix)),
            };
            'ids: loop {
                let rows = g.scan_range(&start, stop.as_deref(), !asc, Some(chunk), DOC_READ_AHEAD).await?;
                let n = rows.len();
                for (k, v) in &rows {
                    let Some(&rank) = k.get(prefix.len()) else { continue };
                    if t.is_full() {
                        // every key from here on sorts after the worst document kept: in a later type, or later in
                        // a type whose key order is its sort order (`_id`s are unique)
                        let worst = crate::query::type_rank(t.worst_first_key().unwrap_or(&Bson::Null));
                        if worst != rank || id_rank_ordered(rank, collated) {
                            break 'ids;
                        }
                    }
                    stats.docs += 1;
                    if ov_keys.contains(&k.to_vec()) {
                        continue;
                    }
                    let d = decode(v)?;
                    if visible(&d, o) && matches(&d, o.filter)? {
                        t.push(d);
                    }
                }
                if n < chunk {
                    break;
                }
                let last = rows.last().unwrap().0.to_vec();
                if asc {
                    let mut k = last;
                    k.push(0);
                    start = k;
                } else {
                    stop = Some(last);
                }
                chunk = (chunk * 2).min(4096);
            }
            stats.keys = stats.docs;
        }
        Plan::Coll { reverse: false } if id_range.is_some() && topk.is_none() && !blocking => {
            tenant_bench::counting::set_phase(3);
            let (mut start, stop) = id_range.clone().unwrap();
            let chunk = early.map(|w| (w * 2).max(256)).unwrap_or(4096);
            'range: loop {
                let rows = g.scan_range(&start, stop.as_deref(), false, Some(chunk), DOC_READ_AHEAD).await?;
                let n = rows.len();
                for (k, v) in &rows {
                    stats.docs += 1;
                    if ov_keys.contains(&k.to_vec()) {
                        continue;
                    }
                    let d = decode(v)?;
                    if visible(&d, o) && matches(&d, o.filter)? {
                        charge(&mut sort_bytes, &d)?;
                        out.push(d);
                        if early.map(|w| out.len() >= w).unwrap_or(false) {
                            break 'range;
                        }
                    }
                }
                if n < chunk {
                    break;
                }
                let mut k = rows.last().unwrap().0.to_vec();
                k.push(0);
                start = k;
            }
            stats.keys = stats.docs;
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
                        let k = crate::custkey::doc_key(coll, ckey.as_ref(), &d);
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
                        match topk.as_mut() {
                            Some(t) => t.push(d),
                            None => {
                                charge(&mut sort_bytes, &d)?;
                                out.push(d)
                            }
                        }
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
            let chunk = early.map(|w| (w * 2).max(256)).unwrap_or(if topk.is_some() { 4096 } else { usize::MAX });
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
                        match topk.as_mut() {
                            Some(t) => t.push(d),
                            None => {
                                charge(&mut sort_bytes, &d)?;
                                out.push(d)
                            }
                        }
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
            let mut last_entry: Option<Bytes> = None;
            'ix: loop {
                let b = c.next_batch(g, batch).await?;
                batch = (batch * 2).min(4096);
                if b.is_empty() {
                    break;
                }
                last_entry = b.last().cloned();
                stats.keys += b.len();
                let ids: Vec<Vec<u8>> = b
                    .iter()
                    .filter_map(|k| entry_id(k))
                    .filter(|id| seen.insert(id.to_vec()))
                    .filter(|id| ov_keys.is_empty() || !ov_keys.contains(&[prefix.as_slice(), id].concat()))
                    .map(|id| id.to_vec())
                    .collect();
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
            // T1: read ahead on a signal (one interval: a client's entries, maybe with a date bound)
            if c.ivs.len() == 1 && o.ov.is_none() {
                if let Some(t) = g.tenant_name() {
                    crate::prefetch::note(&t, coll, &c.ivs[0].0, &c.ivs[0].1, c.reverse, last_entry.as_deref());
                }
            }
        }
        // A sort the plan does not give (a blocking sort): the candidates a chunk at a time, into the first `want` or
        // into every match within the memory limit of a blocking sort.
        plan if topk.is_some() || blocking => {
            let mut sink = match topk.as_mut() {
                Some(t) => Sink::Top(t),
                None => Sink::All { docs: Vec::new(), ids: HashSet::new(), bytes: 0 },
            };
            sort_candidates(g, coll, indexes, plan, o, &ov_keys, &mut stats, &mut sink).await?;
            if let Sink::All { mut docs, bytes, .. } = sink {
                docs.sort_by_key(|(seq, _)| *seq);
                out = docs.into_iter().map(|(_, d)| d).collect();
                sort_bytes = Some(bytes);
            }
        }
        plan => {
            let ids = plan_docs_ids(g, coll, indexes, plan, &mut stats).await?;
            let mut seen: HashSet<Vec<u8>> = HashSet::new();
            let ids: Vec<Vec<u8>> = ids.into_iter().filter(|id| seen.insert(id.clone())).collect();
            let docs = docs_by_ids(g, coll, &ids).await?;
            stats.docs += docs.len();
            for d in docs {
                if !ov_keys.is_empty() && ov_keys.contains(&crate::custkey::doc_key(coll, ckey.as_ref(), &d)) {
                    continue;
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
                match topk.as_mut() {
                    Some(t) => t.push(d.clone()),
                    None => {
                        charge(&mut sort_bytes, d)?;
                        out.push(d.clone())
                    }
                }
            }
        }
    }
    if let Some(t) = topk {
        if t.bytes() > crate::query::SORT_MEM_LIMIT.load(std::sync::atomic::Ordering::Relaxed) as usize {
            return Err(crate::query::sort_mem_error());
        }
        out = t.into_sorted();
    }
    Ok((out, stats, plan))
}

/// Count a document held for a blocking sort without a limit (`bytes` is `None` when there is none).
fn charge(bytes: &mut Option<usize>, d: &Document) -> R<()> {
    if let Some(b) = bytes {
        *b += crate::query::bson_size(d);
        if *b > crate::query::SORT_MEM_LIMIT.load(std::sync::atomic::Ordering::Relaxed) as usize {
            return Err(crate::query::sort_mem_error());
        }
    }
    Ok(())
}

/// One command's (or transaction's) writes.
pub struct Writes {
    pub ops: Vec<Op>,
    pub ov: Overlay,
    /// (index position, entry components) -> `_id` encoding that holds it in this write set.
    claims: HashMap<(usize, Vec<u8>), Vec<u8>>,
    /// Entries removed by this write set.
    released: HashSet<(usize, Vec<u8>, Vec<u8>)>,
    /// Unique values this write set adds (index prefix + entry components, and the collation `_id` keys): a commit marks
    /// them written (`Entry::mark_unique`), and a transaction holding one conflicts with any other writer of the same value.
    pub uniq: HashSet<Vec<u8>>,
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
    /// Extra store operations applied in the same batch (the retryable-write record of the session).
    pub extra: Vec<Op>,
    /// Capped collections: how this write set moves the (document count, byte count) of each.
    pub cap_delta: HashMap<String, (i64, i64)>,
    /// How this write set moves the storage counters of each collection it writes (`collstat.rs`).
    pub stat: HashMap<String, crate::collstat::Delta>,
    /// A document is being dropped by the cap, not by a user: no counts, no change event.
    quiet: bool,
    /// `bypassDocumentValidation` of the command that writes.
    pub bypass_validation: bool,
    /// Deletes of natural-order entries (`nat/{n}`) staged by deletes, to be dropped when the stored entry names another
    /// document (a collection damaged before c2-fix-outage-loss, review defect 4): (position in `ops`, key, the deleted
    /// document's `_id` encoding). Checked for the whole write set at once by [`Writes::settle_nat_deletes`].
    pub nat_dels: Vec<(usize, Vec<u8>, Vec<u8>)>,
}

/// The stored values of the natural-order entries `cands` names (same order). The keys of one collection are
/// `nat/{collection}/{number}`, ordered by number, and a large delete's numbers are mostly contiguous: each dense cluster
/// of numbers is read with one range scan, the scattered ones with one batched point read.
async fn read_nat_entries(g: &mut Entry, cands: &[(usize, Vec<u8>, Vec<u8>)]) -> anyhow::Result<Vec<Option<Bytes>>> {
    const GAP: u64 = 64; // numbers closer than this belong to one cluster
    const MIN_CLUSTER: usize = 32; // smaller clusters are read with point reads
    let parse = |k: &[u8]| -> Option<(Vec<u8>, u64)> {
        let n = k.len().checked_sub(8)?;
        Some((k[..n].to_vec(), u64::from_be_bytes(k[n..].try_into().ok()?)))
    };
    let mut order: Vec<usize> = (0..cands.len()).collect();
    order.sort_by(|a, b| cands[*a].1.cmp(&cands[*b].1));
    let mut out: Vec<Option<Bytes>> = vec![None; cands.len()];
    let mut points: Vec<usize> = Vec::new();
    let mut i = 0;
    while i < order.len() {
        let Some((prefix, first)) = parse(&cands[order[i]].1) else {
            points.push(order[i]);
            i += 1;
            continue;
        };
        let mut j = i + 1;
        let mut last = first;
        while j < order.len() {
            match parse(&cands[order[j]].1) {
                Some((p, n)) if p == prefix && n.saturating_sub(last) <= GAP => {
                    last = n;
                    j += 1;
                }
                _ => break,
            }
        }
        if j - i >= MIN_CLUSTER {
            let mut lo = prefix.clone();
            lo.extend(first.to_be_bytes());
            let mut hi = prefix.clone();
            hi.extend((last + 1).to_be_bytes());
            let rows = g.scan_range(&lo, Some(&hi), false, None, 4 << 20).await?;
            let found: HashMap<Vec<u8>, Bytes> = rows.into_iter().map(|(k, v)| (k.to_vec(), v)).collect();
            for &c in &order[i..j] {
                out[c] = found.get(&cands[c].1).cloned();
            }
        } else {
            points.extend_from_slice(&order[i..j]);
        }
        i = j;
    }
    if !points.is_empty() {
        let keys: Vec<Vec<u8>> = points.iter().map(|c| cands[*c].1.clone()).collect();
        for (c, v) in points.iter().zip(g.get_many(&keys).await?) {
            out[*c] = v;
        }
    }
    Ok(out)
}

impl Writes {
    pub fn new() -> Self {
        Writes { ops: Vec::new(), ov: HashMap::new(), claims: HashMap::new(), released: HashSet::new(), uniq: HashSet::new(), indexes_dirty: false, meta: HashMap::new(), names: None, snap_seq: None, txn_id: None, events: Vec::new(), pending_update: None, seqs: HashMap::new(), extra: Vec::new(), cap_delta: HashMap::new(), stat: HashMap::new(), quiet: false, bypass_validation: false, nat_dels: Vec::new() }
    }

    /// The collection validator on a document that is written: `old` is what an update started from.
    fn validate_document(&self, g: &mut Entry, v: &crate::validation::Validator, new: &Document, old: Option<&Document>) -> R<()> {
        use crate::validation::{Action, Level};
        if v.level == Level::Off || crate::validation::satisfies(new, v)? {
            return Ok(());
        }
        if v.level == Level::Moderate {
            // moderate: a document that did not satisfy the validator before may be updated without satisfying it
            if let Some(o) = old {
                if !crate::validation::satisfies(o, v)? {
                    return Ok(());
                }
            }
        }
        match v.action {
            Action::Error => Err(crate::validation::failure(new, v)),
            Action::Warn => {
                g.log_validation_warning(new);
                Ok(())
            }
        }
    }

    /// Does a document with the `_id` of `d` exist now (latest committed state plus this write set)? In a customer-keyed
    /// collection: under the customer of `d` (`_id` is unique per customer, `custkey.rs`).
    pub async fn exists(&self, g: &mut Entry, coll: &str, d: &Document) -> R<bool> {
        let id = d.get("_id").unwrap_or(&Bson::Null);
        if let Some(key) = crate::custkey::of(g, coll).await? {
            // the locator is the per-customer unique `_id` index of a customer-keyed collection
            let lk = crate::custkey::loc_key_of(coll, &key, d);
            if let Some(v) = self.ov.get(&lk) {
                return Ok(v.is_some());
            }
            return Ok(g.get_live(&lk).await?.is_some());
        }
        let k = doc_key(coll, id);
        if let Some(v) = self.ov.get(&k) {
            return Ok(v.is_some());
        }
        if g.get_live(&k).await?.is_some() {
            return Ok(true);
        }
        // a collection with a default collation: an `_id` equal to this one under the collation
        if let (Some(c), true) = (g.collation_of(coll), crate::collopts::has_string(id)) {
            let ck = crate::collopts::idc_key(coll, &c, id);
            if let Some(v) = self.ov.get(&ck) {
                return Ok(v.is_some());
            }
            return Ok(g.get_live(&ck).await?.is_some());
        }
        Ok(false)
    }

    /// [`Writes::final_ops`] would apply something (a transaction that wrote, not one that only read).
    pub fn has_writes(&self) -> bool {
        !self.ops.is_empty() || !self.meta.is_empty() || self.names.is_some() || !self.extra.is_empty()
    }

    /// All ops to apply at commit: the document and index writes, changed index definitions, the collection list.
    /// Drop the staged deletes of natural-order entries whose stored entry names another document (see `nat_dels`): one
    /// batched read of all of them. Call before the write set's ops are applied. An entry put earlier in this write set
    /// (a document inserted and deleted in one write set) is deleted as staged.
    pub async fn settle_nat_deletes(&mut self, g: &mut Entry) -> anyhow::Result<()> {
        if self.nat_dels.is_empty() {
            return Ok(());
        }
        let cands = std::mem::take(&mut self.nat_dels);
        let stored = read_nat_entries(g, &cands).await?;
        let mut skip: HashSet<usize> = HashSet::new();
        for ((i, k, id), v) in cands.iter().zip(stored) {
            let Some(v) = v else { continue };
            if v[..] != id[..] && !self.ops[..*i].iter().any(|o| matches!(o, Op::Put(pk, _) if pk == k)) {
                skip.insert(*i);
            }
        }
        if !skip.is_empty() {
            let mut j = 0;
            self.ops.retain(|_| {
                let keep = !skip.contains(&j);
                j += 1;
                keep
            });
        }
        Ok(())
    }

    pub fn final_ops(mut self) -> R<Vec<Op>> {
        let mut ops = std::mem::take(&mut self.ops);
        for (coll, idx) in &self.meta {
            ops.push(Entry::indexes_op(coll, idx)?);
        }
        if let Some(names) = &self.names {
            ops.push(Entry::collections_op(names)?);
        }
        ops.append(&mut self.extra);
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

    /// A unique value this write set is about to add. In a transaction it is a write conflict (as on mongod, where both
    /// write the same key of the unique index) when another open transaction holds the value or a commit wrote it after
    /// this transaction's snapshot; otherwise the transaction takes the value until it ends.
    fn claim_unique(&self, g: &mut Entry, key: &[u8]) -> R<()> {
        self.check_conflict(g, key)
    }

    /// Insert or replace a document, maintaining every index. Nothing is recorded if it fails.
    pub async fn put(&mut self, g: &mut Entry, coll: &str, ns: &str, indexes: &mut [IndexDef], new: &Document, old: Option<&Document>) -> R<()> {
        let id = new.get("_id").cloned().ok_or_else(|| MongoError::internal("document without _id".into()))?;
        // the document suffix: the `_id` encoding, after the customer component in a customer-keyed collection
        let ckey = crate::custkey::for_write(g, ns, coll).await?;
        let id_enc = crate::custkey::dsuf(ckey.as_ref(), new);
        if ckey.is_some() {
            crate::custkey::DOCS_WRITTEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        // an update that changes the customer moves the document (and its index entries) to the new customer's place
        let old_enc = old.map(|o| crate::custkey::dsuf(ckey.as_ref(), o)).filter(|e| *e != id_enc);
        if !self.quiet && !self.bypass_validation && !coll.starts_with("system.") {
            if let Some(v) = g.validator_of(coll) {
                self.validate_document(g, &v, new, old)?;
            }
        }
        self.check_conflict(g, &[doc_prefix(coll), id_enc.clone()].concat())?;
        if let Some(oe) = &old_enc {
            self.check_conflict(g, &[doc_prefix(coll), oe.clone()].concat())?;
        }
        // the locator `loc/{coll}/{_id}{customer}` is the per-customer unique `_id` index of a customer-keyed collection
        // (custkey.rs): it is locked like a document key, so two writers of one `_id` for the same customer conflict, and
        // writers for two customers do not
        let new_lk = ckey.as_ref().map(|k| crate::custkey::loc_key_of(coll, k, new));
        let old_lk = match (ckey.as_ref(), old) {
            (Some(k), Some(o)) => Some(crate::custkey::loc_key_of(coll, k, o)),
            _ => None,
        };
        let moved_customer = old_lk.is_some() && old_lk != new_lk;
        if let Some(lk) = &new_lk {
            if old.is_none() || old_enc.is_some() {
                self.check_conflict(g, lk)?;
            }
            if old.is_none() || moved_customer {
                // the customer may already hold this `_id`
                let taken = match self.ov.get(lk) {
                    Some(v) => v.is_some(),
                    None => g.get_live(lk).await?.is_some(),
                };
                if taken {
                    return Err(MongoError::duplicate_key_c(ns, "_id_", &bson::doc! { "_id": 1 }, &bson::doc! { "_id": id.clone() }, g.collation_of(coll).as_deref()));
                }
            }
        }
        if moved_customer {
            self.check_conflict(g, old_lk.as_ref().unwrap())?;
        }
        let is_self = |owner: &[u8]| owner == id_enc.as_slice() || old_enc.as_deref() == Some(owner);
        let mut plans: Vec<(usize, Vec<Vec<u8>>, Vec<Vec<u8>>, bool)> = Vec::new(); // (idx, to_add, to_remove, multikey)
        // (index position, components, value as BSON): values a unique index being built already holds for another document
        let mut suspects: Vec<(usize, Vec<u8>, Vec<u8>)> = Vec::new();
        for (i, idx) in indexes.iter().enumerate() {
            if !idx.has_entries() || idx.state == State::Dropping {
                continue;
            }
            let (new_e, mk) = idx.entries(new)?;
            let old_e = match old {
                Some(o) => idx.entries(o)?.0,
                None => Default::default(),
            };
            // a moved document: every old entry goes (under the old suffix), every new one comes
            let moved = old_enc.is_some();
            let add: Vec<Vec<u8>> = new_e.keys().filter(|k| moved || !old_e.contains_key(*k)).cloned().collect();
            let rem: Vec<Vec<u8>> = old_e.keys().filter(|k| moved || !new_e.contains_key(*k)).cloned().collect();
            if (idx.unique || idx.prepare_unique) && idx.state == State::Ready {
                for c in &add {
                    let dup = || MongoError::duplicate_key_c(ns, &idx.name, &idx.key_pattern(), new_e.get(c).unwrap(), idx.collation.as_deref());
                    if let Some(owner) = self.claims.get(&(i, c.clone())) {
                        if !is_self(owner) {
                            return Err(dup());
                        }
                        continue;
                    }
                    let mut p = idx.prefix(coll);
                    p.extend(c);
                    self.claim_unique(g, &p)?;
                    let end = prefix_end(&p);
                    for (k, _) in g.scan_range_live(&p, end.as_deref(), Some(8)).await? {
                        if let Some(owner) = entry_id(&k) {
                            if !is_self(owner) && !self.released.contains(&(i, c.clone(), owner.to_vec())) {
                                return Err(dup());
                            }
                        }
                    }
                }
            }
            if idx.unique && idx.state == State::Building {
                // A unique index being built (in chunks, the database released between chunks): the write is accepted,
                // as on mongod, but a value another document already holds is recorded, and the build re-checks it before
                // the index becomes ready. The build itself checks only the documents ahead of its cursor, so without this
                // two writes behind the cursor would leave the finished unique index holding a value twice.
                for c in &add {
                    let mut held = matches!(self.claims.get(&(i, c.clone())), Some(owner) if *owner != id_enc);
                    let mut p = idx.prefix(coll);
                    p.extend(c);
                    // a transaction takes the value as for a ready index: two open transactions cannot both add it
                    self.claim_unique(g, &p)?;
                    if !held {
                        let end = prefix_end(&p);
                        for (k, _) in g.scan_range_live(&p, end.as_deref(), Some(8)).await? {
                            if let Some(owner) = entry_id(&k) {
                                if owner != id_enc.as_slice() && !self.released.contains(&(i, c.clone(), owner.to_vec())) {
                                    held = true;
                                    break;
                                }
                            }
                        }
                    }
                    if held {
                        suspects.push((i, c.clone(), new_e.get(c).map(|d| d.to_vec()).transpose()?.unwrap_or_default()));
                    }
                }
            }
            plans.push((i, add, rem, mk));
        }
        let k = [doc_prefix(coll), id_enc.clone()].concat();
        let old_k = old_enc.as_ref().map(|oe| [doc_prefix(coll), oe.clone()].concat());
        if g.cap_of(coll).is_some() {
            if self.txn_id.is_some() || self.snap_seq.is_some() {
                let db = ns.split('.').next().unwrap_or("");
                return Err(MongoError::new(263, "OperationNotSupportedInTransaction", format!("Collection '{db}.{coll}' is a capped collection. Writes in transactions are not allowed on capped collections.")));
            }
            let e = self.cap_delta.entry(coll.to_string()).or_insert((0, 0));
            match old {
                None => {
                    e.0 += 1;
                    e.1 += new.to_vec()?.len() as i64;
                }
                Some(o) => e.1 += new.to_vec()?.len() as i64 - o.to_vec()?.len() as i64,
            }
        }
        if let (Some(c), true, true) = (g.collation_of(coll), crate::collopts::has_string(&id), old.is_none() || old_enc.is_some()) {
            // the collation `_id` index: per customer in a customer-keyed collection (`custkey::idc_key_of`), so a
            // collation-equal `_id` is refused under one customer and accepted under another, as the plain `_id` is
            let ck = crate::custkey::idc_key_of(coll, &c, ckey.as_ref(), new);
            let old_ck = old.map(|o| crate::custkey::idc_key_of(coll, &c, ckey.as_ref(), o));
            // (an overlay entry of a customer-keyed collection holds the document suffix, as the locator's does)
            let ov_val = if ckey.is_some() { loc_overlay(&id_enc) } else { Document::new() };
            if old_ck.as_ref() != Some(&ck) {
                // a new document, or one that moved to another customer: the `_id` must be free under its customer
                // (callers check `exists` first; this one also guards the paths that do not)
                let taken = match self.ov.get(&ck) {
                    Some(v) => v.is_some(),
                    None => {
                        self.claim_unique(g, &ck)?;
                        g.get_live(&ck).await?.map(|v| v.as_ref() != id_enc.as_slice() && old_enc.as_deref() != Some(v.as_ref())).unwrap_or(false)
                    }
                };
                if taken {
                    return Err(MongoError::duplicate_key_c(ns, "_id_", &bson::doc! { "_id": 1 }, &bson::doc! { "_id": id.clone() }, Some(&c)));
                }
                self.uniq.insert(ck.clone());
                if let Some(ok) = old_ck {
                    self.check_conflict(g, &ok)?;
                    self.ops.push(Op::Delete(ok.clone()));
                    self.ov.insert(ok, None);
                }
            }
            // the collation `_id` index points at the document's (new) place
            self.ops.push(Op::Put(ck.clone(), id_enc.clone()));
            self.ov.insert(ck, Some(ov_val));
        }
        if let Some(lk) = &new_lk {
            if old.is_none() || old_enc.is_some() {
                self.ops.push(Op::Put(lk.clone(), id_enc.clone()));
                self.ov.insert(lk.clone(), Some(loc_overlay(&id_enc)));
            }
        }
        if moved_customer {
            let ol = old_lk.clone().unwrap();
            self.ops.push(Op::Delete(ol.clone()));
            self.ov.insert(ol, None);
        }
        let mut bytes = new.to_vec()?;
        let seq = match old {
            None => {
                if matches!(self.ov.get(&k), Some(None)) {
                    // deleted earlier in this write set and inserted again: it moves to the end
                    let mut rk = b"reins/".to_vec();
                    rk.extend(&k);
                    self.ov.insert(rk, Some(Document::new()));
                }
                let s = crate::natural::alloc(g, coll, &mut self.ops).await?;
                self.ops.push(Op::Put(crate::natural::key(coll, s), id_enc.clone()));
                Some(s)
            }
            Some(_) => {
                let at = old_k.as_ref().unwrap_or(&k);
                let s = match self.seqs.get(at) {
                    Some(s) => Some(*s),
                    None => crate::natural::stored_seq(g, at).await?,
                };
                if let (Some(ok), Some(s)) = (&old_k, s) {
                    // a moved document keeps its place in natural order; the entry points at its new key
                    self.seqs.remove(ok);
                    self.ops.push(Op::Put(crate::natural::key(coll, s), id_enc.clone()));
                }
                s
            }
        };
        // Commit to the write set.
        let entry_val_len = crate::natural::entry_value(seq).len() as i64;
        let st = self.stat.entry(coll.to_string()).or_default();
        match old {
            None => {
                st.docs += 1;
                st.bytes += bytes.len() as i64;
                st.id_bytes += id_enc.len() as i64;
            }
            Some(o) => st.bytes += bytes.len() as i64 - o.to_vec()?.len() as i64,
        }
        let rem_enc = old_enc.clone().unwrap_or_else(|| id_enc.clone());
        for (i, c, kv) in suspects {
            let mut sk = indexes[i].suspect_prefix(coll);
            sk.extend(&c);
            self.ops.push(Op::Put(sk, kv));
        }
        for (i, add, rem, mk) in plans {
            let idx = &mut indexes[i];
            let prefix = idx.prefix(coll);
            if (idx.unique || idx.prepare_unique) && idx.state != State::Dropping {
                for c in &add {
                    let mut p = prefix.clone();
                    p.extend(c);
                    self.uniq.insert(p);
                }
            }
            let st = self.stat.entry(coll.to_string()).or_default();
            for c in &rem {
                st.index(&idx.name, -((entry_key(&prefix, c, &rem_enc).len() as i64) + entry_val_len));
            }
            for c in &add {
                st.index(&idx.name, entry_key(&prefix, c, &id_enc).len() as i64 + entry_val_len);
            }
            for c in rem {
                self.ops.push(Op::Delete(entry_key(&prefix, &c, &rem_enc)));
                self.claims.remove(&(i, c.clone()));
                self.released.insert((i, c, rem_enc.clone()));
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
        if let Some(ok) = old_k {
            self.ops.push(Op::Delete(ok.clone()));
            self.ov.insert(ok, None);
        }
        self.ops.push(Op::Put(k.clone(), bytes));
        self.ov.insert(k, Some(new.clone()));
        let db = ns.split('.').next().unwrap_or("").to_string();
        let images = g.images_of(coll);
        // a customer-keyed collection: the event names the document by its customer too; an update or replace by the
        // customer it had before the write (mongod's documentKey of a shard key update, MEASURED on mongos 7.0.34)
        let cust = ckey.as_ref().map(|k| {
            let d = old.unwrap_or(new);
            (crate::custkey::doc_key_fields(k, d), crate::custkey::value(&k.field, d))
        });
        let mut spec = match (old, self.pending_update.take()) {
            (None, _) => crate::changes::EvSpec { doc_key: Some(id.clone()), full: Some(new.clone()), ..crate::changes::EvSpec::on(&db, coll, "insert") },
            (Some(o), Some(u)) => crate::changes::EvSpec { doc_key: Some(id.clone()), desc: Some(crate::changes::update_description(o, new, &u)), ..crate::changes::EvSpec::on(&db, coll, "update") },
            (Some(_), None) => crate::changes::EvSpec { doc_key: Some(id.clone()), full: Some(new.clone()), ..crate::changes::EvSpec::on(&db, coll, "replace") },
        };
        spec.cust = cust;
        if crate::changes::WANTS_UUID.load(std::sync::atomic::Ordering::Relaxed) {
            spec.uuid = Some(g.collection_uuid(coll).await?);
        }
        if let (true, Some(o)) = (images, old) {
            spec.images = true;
            spec.pre = Some(o.clone());
            spec.post = Some(new.clone());
        }
        self.events.push(spec);
        Ok(())
    }

    /// The documents inserted by this write set into `coll` and still there: (sequence number, encoded `_id`).
    pub fn batch_docs(&self, coll: &str) -> Vec<(u64, Vec<u8>)> {
        let prefix = doc_prefix(coll);
        self.seqs.iter().filter(|(k, _)| k.starts_with(&prefix)).map(|(k, s)| (*s, k[prefix.len()..].to_vec())).collect()
    }

    /// Delete a document because a capped collection is over its limit: no change event, and the caller keeps the counts.
    pub async fn delete_quiet(&mut self, g: &mut Entry, db: &str, coll: &str, indexes: &[IndexDef], old: &Document) -> R<()> {
        self.quiet = true;
        let r = self.delete(g, db, coll, indexes, old).await;
        self.quiet = false;
        r
    }

    pub async fn delete(&mut self, g: &mut Entry, db: &str, coll: &str, indexes: &[IndexDef], old: &Document) -> R<()> {
        let id = old.get("_id").cloned().unwrap_or(Bson::Null);
        if !self.quiet && g.cap_of(coll).is_some() {
            if self.txn_id.is_some() || self.snap_seq.is_some() {
                return Err(MongoError::new(20, "IllegalOperation", format!("Cannot remove from a capped collection in a multi-document transaction: {db}.{coll}")));
            }
            let e = self.cap_delta.entry(coll.to_string()).or_insert((0, 0));
            e.0 -= 1;
            e.1 -= old.to_vec()?.len() as i64;
        }
        let ckey = crate::custkey::of(g, coll).await?;
        let id_enc = crate::custkey::dsuf(ckey.as_ref(), old);
        let k = [doc_prefix(coll), id_enc.clone()].concat();
        self.check_conflict(g, &k)?;
        if let Some(key) = &ckey {
            let lk = crate::custkey::loc_key_of(coll, key, old);
            self.check_conflict(g, &lk)?;
            self.ops.push(Op::Delete(lk.clone()));
            self.ov.insert(lk, None);
        }
        let seq = match self.seqs.get(&k) {
            Some(s) => Some(*s),
            None => crate::natural::stored_seq(g, &k).await?,
        };
        let entry_val_len = crate::natural::entry_value(seq).len() as i64;
        let mut st = crate::collstat::Delta { docs: -1, bytes: -(old.to_vec()?.len() as i64), id_bytes: -(id_enc.len() as i64), ..Default::default() };
        for (i, idx) in indexes.iter().enumerate() {
            if !idx.has_entries() || idx.state == State::Dropping {
                continue;
            }
            let prefix = idx.prefix(coll);
            for c in idx.entries(old)?.0.into_keys() {
                st.index(&idx.name, -((entry_key(&prefix, &c, &id_enc).len() as i64) + entry_val_len));
                self.ops.push(Op::Delete(entry_key(&prefix, &c, &id_enc)));
                self.claims.remove(&(i, c.clone()));
                self.released.insert((i, c, id_enc.clone()));
            }
        }
        if let (Some(c), true) = (g.collation_of(coll), crate::collopts::has_string(&id)) {
            let ck = crate::custkey::idc_key_of(coll, &c, ckey.as_ref(), old);
            self.ops.push(Op::Delete(ck.clone()));
            self.ov.insert(ck, None);
        }
        self.seqs.remove(&k);
        let e = self.stat.entry(coll.to_string()).or_default();
        e.docs += st.docs;
        e.bytes += st.bytes;
        e.id_bytes += st.id_bytes;
        for (n, b) in st.idx {
            e.index(&n, b);
        }
        if let Some(s) = seq {
            // Only the entry that names this document: in a collection damaged before c2-fix-outage-loss two documents can
            // share a number, and the entry then names the other one, which would vanish from scans with it (review
            // defect 4). Checked for the whole write set in one batched read (`settle_nat_deletes`), not here: one read
            // per deleted document made a large deleteMany 1.8 times slower (fix round 2).
            let nk = crate::natural::key(coll, s);
            self.nat_dels.push((self.ops.len(), nk.clone(), id_enc.clone()));
            self.ops.push(Op::Delete(nk));
        }
        self.ops.push(Op::Delete(k.clone()));
        self.ov.insert(k, None);
        if !self.quiet {
            let images = g.images_of(coll);
            let uuid = if crate::changes::WANTS_UUID.load(std::sync::atomic::Ordering::Relaxed) { Some(g.collection_uuid(coll).await?) } else { None };
            let cust = ckey.as_ref().map(|k| (crate::custkey::doc_key_fields(k, old), crate::custkey::value(&k.field, old)));
            self.events.push(crate::changes::EvSpec { doc_key: Some(id), cust, images, uuid, pre: images.then(|| old.clone()), ..crate::changes::EvSpec::on(db, coll, "delete") });
        }
        Ok(())
    }
}

/// Whether a find can be answered page by page in natural order (`natural_page`): no sort, no transaction, a plain
/// collection scan in natural order (no index serves the filter), no text, geo or `_id` condition (`_id` conditions get
/// their order from the `_id` index elsewhere), insertion order kept by the collection (fix-long-commands).
pub async fn natural_streamable(g: &mut impl Rd, coll: &str, indexes: &[IndexDef], filter: &Document, hint: &Hint) -> R<bool> {
    fn touches_id(f: &Document) -> bool {
        f.iter().any(|(k, v)| k == "_id" || (k.starts_with('$') && matches!(v, Bson::Array(a) if a.iter().any(|s| matches!(s, Bson::Document(d) if touches_id(d))))))
    }
    if touches_id(filter) || crate::text::has_text(filter) || crate::geo_query::filter_has_geo(filter) || !matches!(hint, Hint::None | Hint::Natural(true)) {
        return Ok(false);
    }
    let o = FindOpts { filter, sort: &[], want: None, hint: hint.clone(), ttl: None, ov: None };
    if !matches!(plan_for(indexes, &o), Plan::Coll { reverse: false }) {
        return Ok(false);
    }
    Ok(crate::natural::enabled(g, coll).await?)
}

/// One page of a natural-order scan: the matching documents after natural-order number `after` (exclusive; None: from
/// the start), at most `want` of them and up to `max_bytes`, and the number of the last entry examined. `done`: the scan
/// reached the end. A page stops exactly at its last document: the entries after it are read again by the next page,
/// so a document deleted or changed in between is seen as it is then (nothing is held between pages). Each page is its
/// own read; the next starts after the stored natural-order number, which is the same in every process and every
/// opening, so pages never repeat or skip a document across a lease renewal, a reopen or a takeover.
pub struct NaturalPage {
    pub docs: Vec<Document>,
    pub last: Option<u64>,
    pub done: bool,
}

pub async fn natural_page(g: &mut impl Rd, coll: &str, indexes: &[IndexDef], filter: &Document, after: Option<u64>, want: usize, max_bytes: usize) -> R<NaturalPage> {
    tenant_bench::counting::set_phase(3);
    let specs = index::ttl_specs(indexes);
    let ttl = if specs.is_empty() { None } else { Some((&specs[..], crate::cmd_data::now_ms())) };
    let o = FindOpts { filter, sort: &[], want: None, hint: Hint::None, ttl, ov: None };
    let np = crate::natural::prefix(coll);
    let stop = prefix_end(&np);
    let mut start = match after {
        Some(s) => crate::natural::key(coll, s.saturating_add(1)),
        None => np.clone(),
    };
    let mut out = Vec::new();
    let mut bytes = 0usize;
    let mut last = after;
    // A chunk is sized to what the page still needs (no document is decoded only to be thrown away at the page
    // stop); when the filter drops documents the floor doubles, so a selective filter still reads in large chunks.
    let mut floor = 256usize;
    loop {
        let chunk = want.saturating_sub(out.len()).clamp(floor, NATURAL_CHUNK);
        let rows = g.scan_range(&start, stop.as_deref(), false, Some(chunk), INDEX_READ_AHEAD).await?;
        let n = rows.len();
        let ids: Vec<Vec<u8>> = rows.iter().map(|(_, v)| v.to_vec()).collect();
        let docs = docs_aligned(g, coll, &ids).await?;
        for ((k, _), (id, d)) in rows.iter().zip(ids.iter().zip(docs)) {
            last = k.get(np.len()..).and_then(|s| s.try_into().ok()).map(u64::from_be_bytes).or(last);
            let d = match d {
                Some(d) => d,
                None => match moved_doc(g, coll, id).await? {
                    Some(d) => d,
                    None => continue, // gone
                },
            };
            if visible(&d, &o) && matches(&d, filter)? {
                bytes += crate::query::bson_size(&d);
                out.push(d);
                if out.len() >= want || bytes >= max_bytes {
                    return Ok(NaturalPage { docs: out, last, done: false });
                }
            }
        }
        if n < chunk {
            return Ok(NaturalPage { docs: out, last, done: true });
        }
        let mut k = rows.last().unwrap().0.to_vec();
        k.push(0);
        start = k;
        floor = (floor * 2).min(NATURAL_CHUNK);
    }
}

const NATURAL_CHUNK: usize = 8192;

/// The documents under these key suffixes, position by position (`None` where nothing is stored under a suffix: the
/// caller looks for a moved document). Same cost-based choice as `docs_by_ids`, without matching documents back to
/// their keys.
async fn docs_aligned(g: &mut impl Rd, coll: &str, ids: &[Vec<u8>]) -> R<Vec<Option<Document>>> {
    let prefix = doc_prefix(coll);
    tenant_bench::counting::set_phase(2);
    if ids.is_empty() {
        return Ok(vec![]);
    }
    if prefer_scan(g.approx_bytes(), ids.len()) {
        let mut found: HashMap<Vec<u8>, Document> = HashMap::with_capacity(ids.len());
        let rest = scan_span(g, &prefix, ids, |id, v| {
            found.insert(id.to_vec(), decode(v)?);
            Ok(())
        })
        .await?;
        if let Some(from) = rest {
            let mut late: Vec<&Vec<u8>> = ids.iter().filter(|id| !found.contains_key(*id) && reached_not(&prefix, id, &from)).collect();
            late.sort();
            late.dedup();
            let keys: Vec<Vec<u8>> = late.iter().map(|id| [&prefix[..], &id[..]].concat()).collect();
            for (id, v) in late.into_iter().zip(g.get_many(&keys).await?) {
                if let Some(v) = v {
                    found.insert(id.clone(), decode(&v)?);
                }
            }
        }
        return Ok(ids.iter().map(|i| found.remove(i)).collect());
    }
    let keys: Vec<Vec<u8>> = ids.iter().map(|id| [&prefix[..], id].concat()).collect();
    let vals = g.get_many(&keys).await?;
    vals.into_iter().map(|v| v.map(|v| decode(&v)).transpose()).collect()
}
