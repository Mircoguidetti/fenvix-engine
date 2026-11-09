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
    let futs: Vec<_> = keys.iter().map(|k| async move { raw.get(&phys(k)).await }).collect();
    futures::stream::iter(futs).buffered(parallel.max(1)).try_collect().await
}

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
async fn area<R: RawScan>(raw: &R, s: &Seg, lo: &[u8], hi: &[u8], reverse: bool, limit: Option<usize>, read_ahead: usize) -> Result<Vec<(Bytes, Bytes)>> {
    use std::sync::atomic::Ordering::Relaxed;
    let plo = s.phys_bound(lo);
    let phi = s.phys_bound(hi);
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

    struct Model(BTreeMap<Vec<u8>, Vec<u8>>);

    impl RawScan for Model {
        async fn scan(&self, lo: &[u8], hi: Option<&[u8]>, rev: bool, limit: Option<usize>, _ra: usize) -> Result<Vec<(Bytes, Bytes)>> {
            if hi.map_or(false, |h| lo >= h) {
                return Ok(Vec::new());
            }
            let it = self.0.range(lo.to_vec()..).take_while(|(k, _)| hi.map_or(true, |h| k.as_slice() < h));
            let mut v: Vec<(Bytes, Bytes)> = it.map(|(k, v)| (Bytes::from(k.clone()), Bytes::from(v.clone()))).collect();
            if rev {
                v.reverse();
            }
            if let Some(l) = limit {
                v.truncate(l);
            }
            Ok(v)
        }
    }

    /// The range cutting against a model: logical range reads over the mapped keys equal the same reads over the
    /// logical keys, for every pair of bounds drawn from the keys and the area edges, both directions, with limits.
    #[test]
    fn mapped_reads_equal_logical_reads() {
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
        logical.insert(b"meta/collections".to_vec(), vec![1]);
        logical.insert(b"doc/plain/\x03abc".to_vec(), vec![2]);
        logical.insert(b"idx/m/k_1/\x03abc".to_vec(), vec![2]);
        logical.insert(b"zzz".to_vec(), vec![3]);
        let model = Model(logical.iter().map(|(k, v)| (phys(k).to_vec(), v.clone())).collect());
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
}
