//! Reads and writes of layout v2 at the storage boundary (`custkey.rs` has the key mapping and the reasons).
//!
//! Every read of the server goes through [`Entry`](crate::dbs::Entry) or [`ReadView`](crate::dbs::ReadView), which
//! call the functions here: a point read maps its key, a range read is cut into the plain parts (read as they are, minus
//! the customer space `u/`) and the mapped areas ([`Seg`]) it crosses, each read in the physical customer space with
//! seeks over other collections' keys, and returned with logical keys in logical order.

use crate::custkey::{self, Place, Seg, U, U_END};
use anyhow::Result;
use bytes::Bytes;
use tenant_bench::lifecycle::{Op, Tenant};

/// Keys read per request inside a mapped area when the caller set no limit.
const CHUNK: usize = 1024;

/// Physical seeks made by mapped range reads (each one a new store iterator), since the start (diagnostics).
pub static SEEKS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Where the raw rows come from.
pub enum Raw<'a> {
    /// The tenant's latest state; `true`: durable data only.
    Tenant(&'a Tenant, bool),
    /// The tenant's visible state: on the log path, no write whose record is not confirmed yet (`lifecycle::TRACK_VISIBLE`);
    /// otherwise the latest state. Every read a client sees outside a transaction uses it.
    Visible(&'a Tenant),
    /// A transaction's snapshot: SlateDB's snapshot and the version of our manifest held with it (`fx/read.rs`), with
    /// the database's E_env (`envelope.rs`).
    Snap(&'a tenant_bench::fx::read::Snap),
    /// A reader the control layer opened on its own, with its E_env ([`reader_env`]).
    Reader(&'a slatedb::DbReader, Option<u64>),
    /// A reader of the control layer paired with a version of our manifest (export of a checkpoint, PITR's diff;
    /// `fx/pair.rs`): every call is one page, read through the merged rule with that version.
    Paired(&'a tenant_bench::fx::pair::PairedReader),
}

/// E_env of a database as a reader the control layer opened sees it (`envelope.rs`).
pub async fn reader_env(r: &slatedb::DbReader) -> Result<Option<u64>> {
    Ok(r.get_key_value(tenant_bench::envelope::MARKER).await?.map(|kv| kv.seq))
}

/// A source of physical rows for range reads (the store, or a map in the tests).
pub trait RawScan: Sync {
    fn scan(&self, lo: &[u8], hi: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize) -> impl std::future::Future<Output = Result<Vec<(Bytes, Bytes)>>> + Send;
}

impl RawScan for Raw<'_> {
    async fn scan(&self, lo: &[u8], hi: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize) -> Result<Vec<(Bytes, Bytes)>> {
        self.scan_raw(lo, hi, reverse, limit, read_ahead).await
    }
}

impl Raw<'_> {
    /// Whether [`Raw::prefetch`] can warm run blocks for this source.
    fn prefetches(&self) -> bool {
        matches!(self, Raw::Tenant(..) | Raw::Visible(_) | Raw::Snap(_))
    }

    /// P-fix-read-amplification: warm the run blocks these physical keys share (a cache warm-up: answers never depend
    /// on it).
    async fn prefetch(&self, keys: &[Vec<u8>]) {
        match self {
            Raw::Tenant(t, _) | Raw::Visible(t) => t.prefetch_points(keys).await,
            Raw::Snap(s) => s.prefetch_points(keys).await,
            _ => {}
        }
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        match self {
            Raw::Tenant(t, false) => t.get(key).await,
            Raw::Tenant(t, true) => t.get_durable(key).await,
            Raw::Visible(t) => t.get_visible(key).await,
            Raw::Snap(s) => s.get(key).await,
            Raw::Reader(r, env) => match r.get_key_value(key).await? {
                Some(kv) => tenant_bench::envelope::decode(*env, &kv.key, kv.seq, kv.value),
                None => Ok(None),
            },
            Raw::Paired(p) => p.get(key).await,
        }
    }

    async fn scan_raw(&self, lo: &[u8], hi: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize) -> Result<Vec<(Bytes, Bytes)>> {
        if let Some(h) = hi {
            if lo >= h {
                return Ok(Vec::new());
            }
        }
        match self {
            Raw::Tenant(t, durable) => t.scan_range_at(lo, hi, reverse, limit, read_ahead, *durable).await,
            Raw::Visible(t) => t.scan_range_visible(lo, hi, reverse, limit, read_ahead).await,
            Raw::Snap(s) => s.scan(lo, hi, reverse, limit, read_ahead).await,
            Raw::Paired(p) => p.scan(lo, hi, reverse, limit, read_ahead).await,
            Raw::Reader(r, env) => {
                let opts = slatedb::config::ScanOptions {
                    read_ahead_bytes: read_ahead,
                    max_fetch_tasks: tenant_bench::tiered::scan_fetch_tasks(read_ahead),
                    cache_blocks: true,
                    order: if reverse { slatedb::IterationOrder::Descending } else { slatedb::IterationOrder::Ascending },
                    ..Default::default()
                };
                let range = match hi {
                    Some(e) => (std::ops::Bound::Included(lo.to_vec()), std::ops::Bound::Excluded(e.to_vec())),
                    None => (std::ops::Bound::Included(lo.to_vec()), std::ops::Bound::Unbounded),
                };
                let mut it = r.scan_with_options(range, &opts).await?;
                let mut out = Vec::new();
                while let Some(kv) = it.next().await? {
                    let Some(v) = tenant_bench::envelope::decode(*env, &kv.key, kv.seq, kv.value)? else { continue };
                    out.push((kv.key, v));
                    if limit.map(|l| out.len() >= l).unwrap_or(false) {
                        break;
                    }
                }
                Ok(out)
            }
        }
    }
}

/// The physical key of a logical key.
pub fn phys(key: &[u8]) -> std::borrow::Cow<'_, [u8]> {
    match custkey::to_phys(key) {
        Some(p) => std::borrow::Cow::Owned(p),
        None => std::borrow::Cow::Borrowed(key),
    }
}

/// Map the keys of a batch to their physical keys.
pub fn phys_ops(ops: Vec<Op>) -> Vec<Op> {
    ops.into_iter()
        .map(|o| match o {
            Op::Put(k, v) => match custkey::to_phys(&k) {
                Some(p) => Op::Put(p, v),
                None => Op::Put(k, v),
            },
            Op::Delete(k) => match custkey::to_phys(&k) {
                Some(p) => Op::Delete(p),
                None => Op::Delete(k),
            },
        })
        .collect()
}

/// A point read.
pub async fn get(raw: &Raw<'_>, key: &[u8]) -> Result<Option<Bytes>> {
    raw.get(&phys(key)).await
}

/// Point reads, in key order, at most `parallel` at once.
pub async fn get_many(raw: &Raw<'_>, keys: &[Vec<u8>], parallel: usize) -> Result<Vec<Option<Bytes>>> {
    use futures::{StreamExt, TryStreamExt};
    // P-fix-read-amplification: a batch of keys of two or more customers is read in physical order, a chunk at a time:
    // the run blocks that keys of two or more customers share are fetched once, whole (`View::prefetch_points`), then
    // the chunk's point reads find them cached; the answers are put back in the order of `keys`. Keys of one customer
    // (or outside the customer space) share their block reads already: they are read as before, with no extra work.
    let pks: Vec<std::borrow::Cow<'_, [u8]>> = keys.iter().map(|k| phys(k)).collect();
    let many_customers = raw.prefetches() && {
        let mut first: Option<&[u8]> = None;
        pks.iter().filter_map(|p| custkey::phys_customer(p)).any(|c| match first {
            None => {
                first = Some(c);
                false
            }
            Some(f) => f != c,
        })
    };
    if keys.len() < 2 || !many_customers {
        let futs: Vec<_> = pks.iter().map(|k| async move { raw.get(k).await }).collect();
        return futures::stream::iter(futs).buffered(parallel.max(1)).try_collect().await;
    }
    let mut order: Vec<(Vec<u8>, usize)> = pks.into_iter().enumerate().map(|(i, k)| (k.into_owned(), i)).collect();
    order.sort();
    let mut out: Vec<Option<Bytes>> = vec![None; keys.len()];
    for chunk in order.chunks(PREFETCH_CHUNK) {
        let pk: Vec<Vec<u8>> = chunk.iter().map(|(k, _)| k.clone()).collect();
        raw.prefetch(&pk).await;
        let futs: Vec<_> = chunk.iter().map(|(k, i)| async move { Ok::<_, anyhow::Error>((*i, raw.get(k).await?)) }).collect();
        let got: Vec<(usize, Option<Bytes>)> = futures::stream::iter(futs).buffered(parallel.max(1)).try_collect().await?;
        for (i, v) in got {
            out[i] = v;
        }
    }
    Ok(out)
}

/// Keys whose shared blocks are warmed at once before their point reads (`get_many`).
const PREFETCH_CHUNK: usize = 512;

fn left(limit: Option<usize>, n: usize) -> Option<usize> {
    limit.map(|l| l.saturating_sub(n))
}

/// A logical range read: `segs` are the mapped areas the range may cross (from [`custkey::area_of`] or the catalog), in
/// any order.
pub async fn scan<R: RawScan>(raw: &R, segs: &[Seg], lo: &[u8], hi: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize) -> Result<Vec<(Bytes, Bytes)>> {
    if limit == Some(0) {
        return Ok(Vec::new());
    }
    // The pieces of [lo, hi) in logical order: plain parts and mapped areas.
    let mut areas: Vec<(Vec<u8>, Vec<u8>, &Seg)> = Vec::new();
    for s in segs {
        let (a, b) = (s.lp.clone(), s.lp_end());
        let a = if lo > a.as_slice() { lo.to_vec() } else { a };
        let b = match hi {
            Some(h) if h < b.as_slice() => h.to_vec(),
            _ => b,
        };
        if a < b {
            areas.push((a, b, s));
        }
    }
    areas.sort_by(|x, y| x.0.cmp(&y.0));
    enum Piece<'s> {
        Plain(Vec<u8>, Option<Vec<u8>>),
        Area(Vec<u8>, Vec<u8>, &'s Seg),
    }
    let mut pieces = Vec::new();
    let mut cur = lo.to_vec();
    for (a, b, s) in areas {
        if cur < a {
            pieces.push(Piece::Plain(cur.clone(), Some(a.clone())));
        }
        pieces.push(Piece::Area(a, b.clone(), s));
        cur = b;
    }
    match hi {
        Some(h) if cur.as_slice() < h => pieces.push(Piece::Plain(cur, Some(h.to_vec()))),
        None => pieces.push(Piece::Plain(cur, None)),
        _ => {}
    }
    if reverse {
        pieces.reverse();
    }
    let mut out = Vec::new();
    for p in pieces {
        let more = left(limit, out.len());
        if more == Some(0) {
            break;
        }
        let rows = match p {
            Piece::Plain(a, b) => plain(raw, &a, b.as_deref(), reverse, more, read_ahead).await?,
            Piece::Area(a, b, s) => area(raw, s, &a, &b, reverse, more, read_ahead).await?,
        };
        out.extend(rows);
    }
    Ok(out)
}

/// A plain part: read as it is, without the customer space.
async fn plain<R: RawScan>(raw: &R, lo: &[u8], hi: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize) -> Result<Vec<(Bytes, Bytes)>> {
    let crosses = lo < U_END && hi.map_or(true, |h| h > U);
    if !crosses {
        return raw.scan(lo, hi, reverse, limit, read_ahead).await;
    }
    let mut parts: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
    if lo < U {
        parts.push((lo.to_vec(), Some(U.to_vec())));
    }
    if hi.map_or(true, |h| h > U_END) {
        let a = if lo > U_END { lo.to_vec() } else { U_END.to_vec() };
        parts.push((a, hi.map(|h| h.to_vec())));
    }
    if reverse {
        parts.reverse();
    }
    let mut out = Vec::new();
    for (a, b) in parts {
        let more = left(limit, out.len());
        if more == Some(0) {
            break;
        }
        out.extend(raw.scan(&a, b.as_deref(), reverse, more, read_ahead).await?);
    }
    Ok(out)
}

/// A mapped area, logical bounds `lo..hi` inside it.
///
/// P-fix-read-amplification: the area's keys of one customer lie together, between that customer's other areas (its
/// other collections and indexes). Each physical read is bounded to what can be in the area: inside one customer's
/// area, to that area's end (forward) or start (reverse); where the next customer is not known yet (after leaving one),
/// one row is read to find it, and the read seeks to its area. Before, every read after a seek asked for every row still
/// wanted, up to the end of the range: a range over many customers read each customer's other areas and the following
/// customers again and again, about 17 rows per row returned on a client-written collection with two indexes
/// (MEASURED, `ctl-tests/test_84_read_amplification.py`), and a fetch of 459 documents over 400 customers read 12,562.
/// Now: the rows of the area, plus about one row per customer crossed. `FENVIX_AREA_UNBOUNDED=1`: the reads as before
/// (measurements and the negative control of the unit test).
async fn area<R: RawScan>(raw: &R, s: &Seg, lo: &[u8], hi: &[u8], reverse: bool, limit: Option<usize>, read_ahead: usize) -> Result<Vec<(Bytes, Bytes)>> {
    if area_unbounded() {
        return area_unbounded_reads(raw, s, lo, hi, reverse, limit, read_ahead).await;
    }
    use std::sync::atomic::Ordering::Relaxed;
    let plo = s.phys_bound(lo);
    let phi = s.phys_upper_within(lo, hi);
    let mut out: Vec<(Bytes, Bytes)> = Vec::new();
    let full = |out: &Vec<(Bytes, Bytes)>| limit.map(|l| out.len() >= l).unwrap_or(false);
    if !reverse {
        let mut cur = plo;
        // the customer (`u/{c}`) whose area holds `cur`, and the end of that area; None: not known (find it with one row)
        let mut within = s.area_at(&cur);
        'outer: while cur < phi {
            let want = left(limit, out.len()).unwrap_or(CHUNK).clamp(1, CHUNK);
            let (end, chunk) = match &within {
                Some((_, e)) if e < &phi => (e.clone(), want),
                Some(_) => (phi.clone(), want),
                None => (phi.clone(), 1),
            };
            let rows = raw.scan(&cur, Some(&end), false, Some(chunk), read_ahead).await?;
            let n = rows.len();
            for (pk, v) in rows {
                match s.place(&pk) {
                    Ok(lk) => {
                        out.push((Bytes::from(lk), v));
                        if full(&out) {
                            return Ok(out);
                        }
                        if within.is_none() {
                            within = s.area_at(&pk);
                        }
                        cur = pk.to_vec();
                        cur.push(0);
                    }
                    Err(Place::Before { seek, .. }) => {
                        SEEKS.fetch_add(1, Relaxed);
                        within = s.area_at(&seek);
                        cur = seek;
                        continue 'outer;
                    }
                    Err(Place::After { customer, .. }) => {
                        SEEKS.fetch_add(1, Relaxed);
                        within = None;
                        match crate::keyenc::prefix_end(&customer) {
                            Some(e) => cur = e,
                            None => break 'outer,
                        }
                        continue 'outer;
                    }
                    Err(Place::Skip) => {
                        cur = pk.to_vec();
                        cur.push(0);
                    }
                }
            }
            if n < chunk {
                match within.take() {
                    // this customer's area is read to its end: the next customer, past this one's other areas
                    Some((customer, e)) if e < phi => {
                        SEEKS.fetch_add(1, Relaxed);
                        match crate::keyenc::prefix_end(&customer) {
                            Some(x) => cur = x,
                            None => break,
                        }
                    }
                    _ => break,
                }
            }
        }
    } else {
        let mut cur = phi;
        // the customer whose area ends at or holds the position just below `cur`, and that area's start
        let mut within: Option<(Vec<u8>, Vec<u8>)> = s.area_below(&cur);
        'outer_r: while plo < cur {
            let want = left(limit, out.len()).unwrap_or(CHUNK).clamp(1, CHUNK);
            let (start, chunk) = match &within {
                Some((_, a)) if a > &plo => (a.clone(), want),
                Some(_) => (plo.clone(), want),
                None => (plo.clone(), 1),
            };
            let rows = raw.scan(&start, Some(&cur), true, Some(chunk), read_ahead).await?;
            let n = rows.len();
            for (pk, v) in rows {
                match s.place(&pk) {
                    Ok(lk) => {
                        out.push((Bytes::from(lk), v));
                        if full(&out) {
                            return Ok(out);
                        }
                        if within.is_none() {
                            within = s.area_start_of(&pk);
                        }
                        cur = pk.to_vec();
                    }
                    Err(Place::Before { customer, .. }) => {
                        SEEKS.fetch_add(1, Relaxed);
                        within = None;
                        cur = customer;
                        continue 'outer_r;
                    }
                    Err(Place::After { area_end, .. }) => {
                        SEEKS.fetch_add(1, Relaxed);
                        within = s.area_start_of(&pk);
                        cur = area_end;
                        continue 'outer_r;
                    }
                    Err(Place::Skip) => cur = pk.to_vec(),
                }
            }
            if n < chunk {
                match within.take() {
                    // this customer's area is read down to its start: the customer before, below this one's other areas
                    Some((customer, a)) if a > plo => {
                        SEEKS.fetch_add(1, Relaxed);
                        cur = customer;
                    }
                    _ => break,
                }
            }
        }
    }
    Ok(out)
}

fn area_unbounded() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    #[cfg(test)]
    if AREA_UNBOUNDED_TEST.with(|c| c.get()) {
        return true;
    }
    *V.get_or_init(|| std::env::var("FENVIX_AREA_UNBOUNDED").is_ok_and(|v| v == "1"))
}

#[cfg(test)]
thread_local! {
    /// Tests: [`area`] reads as before P-fix-read-amplification (the negative control), on this thread.
    static AREA_UNBOUNDED_TEST: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// [`area`] as it read before P-fix-read-amplification: after every seek, every row still wanted up to the range's end.
async fn area_unbounded_reads<R: RawScan>(raw: &R, s: &Seg, lo: &[u8], hi: &[u8], reverse: bool, limit: Option<usize>, read_ahead: usize) -> Result<Vec<(Bytes, Bytes)>> {
    use std::sync::atomic::Ordering::Relaxed;
    let plo = s.phys_bound(lo);
    let phi = s.phys_upper_within(lo, hi);
    let mut out: Vec<(Bytes, Bytes)> = Vec::new();
    let full = |out: &Vec<(Bytes, Bytes)>| limit.map(|l| out.len() >= l).unwrap_or(false);
    if !reverse {
        let mut cur = plo;
        'outer: while cur < phi {
            let chunk = left(limit, out.len()).unwrap_or(CHUNK).clamp(1, CHUNK);
            let rows = raw.scan(&cur, Some(&phi), false, Some(chunk), read_ahead).await?;
            let n = rows.len();
            for (pk, v) in rows {
                match s.place(&pk) {
                    Ok(lk) => {
                        out.push((Bytes::from(lk), v));
                        if full(&out) {
                            return Ok(out);
                        }
                        cur = pk.to_vec();
                        cur.push(0);
                    }
                    Err(Place::Before { seek, .. }) => {
                        SEEKS.fetch_add(1, Relaxed);
                        cur = seek;
                        continue 'outer;
                    }
                    Err(Place::After { customer, .. }) => {
                        SEEKS.fetch_add(1, Relaxed);
                        match crate::keyenc::prefix_end(&customer) {
                            Some(e) => cur = e,
                            None => break 'outer,
                        }
                        continue 'outer;
                    }
                    Err(Place::Skip) => {
                        cur = pk.to_vec();
                        cur.push(0);
                    }
                }
            }
            if n < chunk {
                break;
            }
        }
    } else {
        let mut cur = phi;
        'outer_r: while plo < cur {
            let chunk = left(limit, out.len()).unwrap_or(CHUNK).clamp(1, CHUNK);
            let rows = raw.scan(&plo, Some(&cur), true, Some(chunk), read_ahead).await?;
            let n = rows.len();
            for (pk, v) in rows {
                match s.place(&pk) {
                    Ok(lk) => {
                        out.push((Bytes::from(lk), v));
                        if full(&out) {
                            return Ok(out);
                        }
                        cur = pk.to_vec();
                    }
                    Err(Place::Before { customer, .. }) => {
                        SEEKS.fetch_add(1, Relaxed);
                        cur = customer;
                        continue 'outer_r;
                    }
                    Err(Place::After { area_end, .. }) => {
                        SEEKS.fetch_add(1, Relaxed);
                        cur = area_end;
                        continue 'outer_r;
                    }
                    Err(Place::Skip) => cur = pk.to_vec(),
                }
            }
            if n < chunk {
                break;
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    struct Model(BTreeMap<Vec<u8>, Vec<u8>>, std::sync::atomic::AtomicUsize);

    impl RawScan for Model {
        async fn scan(&self, lo: &[u8], hi: Option<&[u8]>, rev: bool, limit: Option<usize>, _ra: usize) -> Result<Vec<(Bytes, Bytes)>> {
            let v = self.scan_rows(lo, hi, rev, limit);
            self.1.fetch_add(v.len(), std::sync::atomic::Ordering::Relaxed);
            Ok(v)
        }
    }

    impl Model {
        fn new(m: BTreeMap<Vec<u8>, Vec<u8>>) -> Model {
            Model(m, Default::default())
        }
        fn rows_read(&self) -> usize {
            self.1.load(std::sync::atomic::Ordering::Relaxed)
        }
        fn scan_rows(&self, lo: &[u8], hi: Option<&[u8]>, rev: bool, limit: Option<usize>) -> Vec<(Bytes, Bytes)> {
            if hi.map_or(false, |h| lo >= h) {
                return Vec::new();
            }
            let it = self.0.range(lo.to_vec()..).take_while(|(k, _)| hi.map_or(true, |h| k.as_slice() < h));
            let mut v: Vec<(Bytes, Bytes)> = it.map(|(k, v)| (Bytes::from(k.clone()), Bytes::from(v.clone()))).collect();
            if rev {
                v.reverse();
            }
            if let Some(l) = limit {
                v.truncate(l);
            }
            v
        }
    }

    /// The range cutting against a model: logical range reads over the mapped keys equal the same reads over the
    /// logical keys, for every pair of bounds drawn from the keys and the area edges, both directions, with limits.
    #[test]
    fn mapped_reads_equal_logical_reads() {
        mapped_reads_equal_logical_reads_in(false);
    }

    /// The same with the reads of a mapped area as before P-fix-read-amplification (the two give the same rows).
    #[test]
    fn mapped_reads_equal_logical_reads_unbounded() {
        AREA_UNBOUNDED_TEST.with(|c| c.set(true));
        mapped_reads_equal_logical_reads_in(true);
        AREA_UNBOUNDED_TEST.with(|c| c.set(false));
    }

    fn mapped_reads_equal_logical_reads_in(_unbounded: bool) {
        use crate::custkey::{comp, doc_key, Ckey};
        use bson::{doc, Bson};
        let key = Ckey { field: "u".into(), order: None };
        let mut logical: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut n = 0u32;
        for u in ["", "a", "ab", "abc", "b", "zz"] {
            for coll in ["m", "n", "m2"] {
                for id in 0..4 {
                    n += 1;
                    logical.insert(doc_key(coll, Some(&key), &doc! { "_id": id, "u": u }), n.to_be_bytes().to_vec());
                    if coll != "n" {
                        let mut ik = Seg::index(coll.as_bytes(), b"u_1").lp;
                        ik.extend(comp(&Bson::String(u.into())));
                        ik.extend(crate::custkey::dsuf(Some(&key), &doc! { "_id": id, "u": u }));
                        ik.extend(9u16.to_be_bytes());
                        logical.insert(ik, n.to_be_bytes().to_vec());
                    }
                }
            }
        }
        // number customers too: the end of one number's component (prefix_end) is itself a whole component, the case
        // `Seg::phys_upper` tightens
        for u in [1, 2, 3, 7] {
            for coll in ["m", "m2"] {
                for id in 0..3 {
                    n += 1;
                    logical.insert(doc_key(coll, Some(&key), &doc! { "_id": id, "u": u }), n.to_be_bytes().to_vec());
                    let mut ik = Seg::index(coll.as_bytes(), b"u_1").lp;
                    ik.extend(comp(&Bson::Int32(u)));
                    ik.extend(crate::custkey::dsuf(Some(&key), &doc! { "_id": id, "u": u }));
                    logical.insert(ik, n.to_be_bytes().to_vec());
                }
            }
        }
        logical.insert(b"meta/collections".to_vec(), vec![1]);
        logical.insert(b"doc/plain/\x03abc".to_vec(), vec![2]);
        logical.insert(b"idx/m/k_1/\x03abc".to_vec(), vec![2]);
        logical.insert(b"zzz".to_vec(), vec![3]);
        let model = Model::new(logical.iter().map(|(k, v)| (phys(k).to_vec(), v.clone())).collect());
        let segs = vec![Seg::docs(b"m"), Seg::docs(b"n"), Seg::docs(b"m2"), Seg::index(b"m", b"u_1"), Seg::index(b"m2", b"u_1")];
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let mut bounds: Vec<Vec<u8>> = logical.keys().cloned().collect();
        bounds.extend([b"".to_vec(), b"doc/".to_vec(), b"doc/m/".to_vec(), b"doc/m0".to_vec(), b"idx/m/".to_vec(), b"idx/m0".to_vec(), b"u/".to_vec(), b"zzzz".to_vec()]);
        for s in &segs {
            bounds.push(s.lp.clone());
            bounds.push(s.lp_end());
            let mut partial = s.lp.clone();
            partial.extend(&comp(&Bson::String("ab".into()))[..2]);
            bounds.push(partial);
            // bounds that are exactly one customer component, and the end of one (a find of one customer reads
            // [lp + c, lp + prefix_end(c)): `Seg::phys_upper` tightens such an upper bound)
            let custs: Vec<Bson> = ["", "a", "ab", "abc", "b", "zz", "aa", "c"].iter().map(|u| Bson::String(u.to_string())).chain([1, 2, 3, 5, 7].map(Bson::Int32)).collect();
            for u in &custs {
                let c = comp(u);
                let mut b = s.lp.clone();
                b.extend(&c);
                bounds.push(b.clone());
                if let Some(e) = crate::keyenc::prefix_end(&c) {
                    let mut b2 = s.lp.clone();
                    b2.extend(e);
                    bounds.push(b2);
                }
            }
        }
        let mut checked = 0;
        for (i, lo) in bounds.iter().enumerate() {
            for hi in bounds.iter().skip(i % 5).step_by(3).map(Some).chain([None]) {
                for rev in [false, true] {
                    for limit in [None, Some(1), Some(3)] {
                        let mut want: Vec<(Vec<u8>, Vec<u8>)> = logical.range(lo.clone()..).take_while(|(k, _)| hi.map_or(true, |h| *k < h)).map(|(k, v)| (k.clone(), v.clone())).collect();
                        if rev {
                            want.reverse();
                        }
                        if let Some(l) = limit {
                            want.truncate(l);
                        }
                        let got = rt.block_on(scan(&model, &segs, lo, hi.map(|h| h.as_slice()), rev, limit, 0)).unwrap();
                        let got: Vec<(Vec<u8>, Vec<u8>)> = got.into_iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
                        assert_eq!(got, want, "lo {lo:?} hi {hi:?} rev {rev} limit {limit:?}");
                        checked += 1;
                    }
                }
            }
        }
        assert!(checked > 1000, "{checked}");
    }

    /// P-fix-read-amplification: a read of one mapped area over many customers reads the area's rows plus about one row
    /// per customer crossed, whatever the customers' other areas hold. Before, every read after a seek asked for every
    /// row still wanted (the negative control below reads many times more).
    #[test]
    fn area_reads_over_many_customers_read_few_rows() {
        use crate::custkey::{comp, doc_key, Ckey};
        use bson::{doc, Bson};
        let key = Ckey { field: "u".into(), order: None };
        let mut logical: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let customers = 200;
        for u in 0..customers {
            let cust = format!("c{u:04}");
            for id in 0..10 {
                let d = doc! { "_id": id, "u": cust.clone() };
                // the area read (collection "m"), and three other areas of the same customer, with more rows each
                logical.insert(doc_key("m", Some(&key), &d), vec![1]);
                for coll in ["a", "z"] {
                    for j in 0..3 {
                        logical.insert(doc_key(coll, Some(&key), &doc! { "_id": id * 10 + j, "u": cust.clone() }), vec![2]);
                    }
                }
                let mut ik = Seg::index(b"m", b"u_1").lp;
                ik.extend(comp(&Bson::String(cust.clone())));
                ik.extend(crate::custkey::dsuf(Some(&key), &d));
                logical.insert(ik, vec![3]);
            }
        }
        let segs = vec![Seg::docs(b"m"), Seg::docs(b"a"), Seg::docs(b"z"), Seg::index(b"m", b"u_1")];
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let lp = Seg::docs(b"m").lp;
        let end = Seg::docs(b"m").lp_end();
        let mut results = Vec::new();
        for unbounded in [false, true] {
            AREA_UNBOUNDED_TEST.with(|c| c.set(unbounded));
            let mut per = Vec::new();
            for (rev, limit) in [(false, None), (false, Some(500)), (true, None), (true, Some(500)), (false, Some(1990))] {
                let model = Model::new(logical.iter().map(|(k, v)| (phys(k).to_vec(), v.clone())).collect());
                let got = rt.block_on(scan(&model, &segs, &lp, Some(&end), rev, limit, 0)).unwrap();
                let mut want: Vec<Vec<u8>> = logical.range(lp.clone()..end.clone()).map(|(k, _)| k.clone()).collect();
                if rev {
                    want.reverse();
                }
                if let Some(l) = limit {
                    want.truncate(l);
                }
                assert_eq!(got.iter().map(|(k, _)| k.to_vec()).collect::<Vec<_>>(), want, "rev {rev} limit {limit:?}");
                per.push((got.len(), model.rows_read()));
            }
            results.push(per);
        }
        AREA_UNBOUNDED_TEST.with(|c| c.set(false));
        for (i, (n, read)) in results[0].iter().enumerate() {
            // the rows wanted, plus at most two per customer crossed (one row to find the next customer's area, and the
            // reverse read's row of the customer's later areas)
            assert!(*read <= n + 2 * customers + 2, "case {i}: {read} rows read for {n} returned");
        }
        let (bounded, before): (usize, usize) = (results[0].iter().map(|x| x.1).sum(), results[1].iter().map(|x| x.1).sum());
        println!("rows read: {bounded} bounded, {before} as before");
        assert!(before > 5 * bounded, "the negative control should read many more rows: {before} against {bounded}");
    }
}
