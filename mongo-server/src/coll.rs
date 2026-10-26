//! Reading and writing documents through the secondary indexes.
//!
//! * [`find_docs`] plans a filter ([`index::plan`]), fetches candidate documents through the index (or a
//!   collection scan) and re-applies the whole filter, so a plan only has to return a superset.
//! * [`Writes`] collects one command's writes: the document put/delete and every index-entry put/delete go
//!   into the same ops vector, which `Entry::apply` commits as one atomic batch, so an index cannot disagree
//!   with its documents. It also enforces unique indexes against the store plus the command's own pending
//!   writes (the overlay), and is the write set of a transaction.

use crate::dbs::Entry;
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
        IxCursor { ivs, reverse: scan.reverse, i: 0, resume: None }
    }

    /// Entries (full keys) of the next batch of at most `n`; empty when exhausted.
    async fn next_batch(&mut self, g: &mut Entry, n: usize) -> R<Vec<Bytes>> {
        let mut out: Vec<Bytes> = Vec::new();
        while out.is_empty() && self.i < self.ivs.len() {
            let (s, e) = &self.ivs[self.i];
            let rows = if self.reverse {
                let end = self.resume.clone().unwrap_or_else(|| e.clone());
                g.scan_range(s, Some(&end), true, Some(n), INDEX_READ_AHEAD).await?
            } else {
                let start = self.resume.clone().unwrap_or_else(|| s.clone());
                g.scan_range(&start, Some(e), false, Some(n), INDEX_READ_AHEAD).await?
            };
            if rows.len() < n {
                self.i += 1;
                self.resume = None;
            } else if let Some((last, _)) = rows.last() {
                self.resume = Some(if self.reverse {
                    last.to_vec()
                } else {
                    let mut k = last.to_vec();
                    k.push(0);
                    k
                });
            }
            out.extend(rows.into_iter().map(|(k, _)| k));
        }
        Ok(out)
    }
}

pub fn decode(v: &[u8]) -> R<Document> {
    Ok(Document::from_reader(v)?)
}

async fn docs_by_ids(g: &mut Entry, coll: &str, ids: &[Vec<u8>]) -> R<Vec<Document>> {
    let prefix = doc_prefix(coll);
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

/// Candidate documents of `plan` (index order), still to be filtered.
async fn plan_docs_ids(g: &mut Entry, coll: &str, indexes: &[IndexDef], plan: &Plan, stats: &mut Stats) -> R<Vec<Vec<u8>>> {
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

/// Documents matching `o.filter`, in plan order (sort not applied unless the index provides it).
pub async fn find_docs(g: &mut Entry, coll: &str, indexes: &[IndexDef], o: &FindOpts<'_>) -> R<(Vec<Document>, Stats, Plan)> {
    let plan = plan_for(indexes, o);
    let mut stats = Stats::default();
    let prefix = doc_prefix(coll);
    let ov_coll: Vec<(&Vec<u8>, &Option<Document>)> = o.ov.map(|m| m.iter().filter(|(k, _)| k.starts_with(&prefix)).collect()).unwrap_or_default();
    let ov_keys: HashSet<&Vec<u8>> = ov_coll.iter().map(|(k, _)| *k).collect();
    let sorted_by_plan = o.sort.is_empty() || matches!(&plan, Plan::Ix(s) if s.sorted);
    let early = o.want.filter(|_| sorted_by_plan && ov_coll.is_empty());
    let mut out: Vec<Document> = Vec::new();

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
        Plan::Coll { reverse } => {
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
    for (_, v) in ov_coll {
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
}

impl Writes {
    pub fn new() -> Self {
        Writes { ops: Vec::new(), ov: HashMap::new(), claims: HashMap::new(), released: HashSet::new(), indexes_dirty: false }
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

    /// Insert or replace a document, maintaining every index. Nothing is recorded if it fails.
    pub async fn put(&mut self, g: &mut Entry, coll: &str, ns: &str, indexes: &mut [IndexDef], new: &Document, old: Option<&Document>) -> R<()> {
        let id = new.get("_id").cloned().ok_or_else(|| MongoError::internal("document without _id".into()))?;
        let id_enc = encode_sortable(&id);
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
                    for (k, _) in g.scan_range(&p, end.as_deref(), false, Some(8), 4096).await? {
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
                self.ops.push(Op::Put(entry_key(&prefix, &c, &id_enc), Vec::new()));
                self.released.remove(&(i, c.clone(), id_enc.clone()));
                self.claims.insert((i, c), id_enc.clone());
            }
            if mk && !idx.multikey {
                idx.multikey = true;
                self.indexes_dirty = true;
            }
        }
        let k = doc_key(coll, &id);
        self.ops.push(Op::Put(k.clone(), new.to_vec()?));
        self.ov.insert(k, Some(new.clone()));
        Ok(())
    }

    pub fn delete(&mut self, coll: &str, indexes: &[IndexDef], old: &Document) -> R<()> {
        let id = old.get("_id").cloned().unwrap_or(Bson::Null);
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
        self.ops.push(Op::Delete(k.clone()));
        self.ov.insert(k, None);
        Ok(())
    }
}
