//! Natural order: `find` without a sort returns documents in insertion order, like MongoDB's record-id order.
//!
//! * every document value is the BSON document followed by `TRAIL` and its 8-byte big-endian sequence number
//!   (readers that decode with `Document::from_reader` ignore the trailer);
//! * `nat/{collection}/{seq}` -> the encoded `_id` lists the documents in insertion order;
//! * `meta/nat/{collection}` holds a reserved upper bound of the sequence numbers handed out (the numbers are
//!   allocated in blocks of `BLOCK`, a restart skips the unused rest of a block).
//!
//! A collection written before this existed has no marker: it keeps its `_id` order until the first insert,
//! which numbers the existing documents in `_id` order (`migrate`) and then enables the marker.

use crate::dbs::Entry;
use crate::layout::{doc_prefix, escape_collection};
use crate::keyenc::prefix_end;
use tenant_bench::lifecycle::Op;

pub const TRAIL: u8 = 0xFE;
const BLOCK: u64 = 1000;
const MIGRATE_CHUNK: usize = 2000;

pub fn prefix(coll: &str) -> Vec<u8> {
    let mut k = b"nat/".to_vec();
    k.extend(escape_collection(coll));
    k.push(b'/');
    k
}

pub fn key(coll: &str, seq: u64) -> Vec<u8> {
    let mut k = prefix(coll);
    k.extend(seq.to_be_bytes());
    k
}

pub fn meta_key(coll: &str) -> Vec<u8> {
    let mut k = b"meta/nat/".to_vec();
    k.extend(escape_collection(coll));
    k
}

/// The sequence number stored after the BSON document of a raw value, if any.
pub fn seq_of(raw: &[u8]) -> Option<u64> {
    if raw.len() < 5 {
        return None;
    }
    let l = i32::from_le_bytes(raw[..4].try_into().ok()?) as usize;
    (raw.len() == l + 9 && raw[l] == TRAIL).then(|| u64::from_be_bytes(raw[l + 1..].try_into().unwrap()))
}

/// The BSON bytes without the trailer.
pub fn bson_part(raw: &[u8]) -> &[u8] {
    match seq_of(raw) {
        Some(_) => &raw[..raw.len() - 9],
        None => raw,
    }
}

pub fn with_seq(mut bytes: Vec<u8>, seq: u64) -> Vec<u8> {
    bytes.push(TRAIL);
    bytes.extend(seq.to_be_bytes());
    bytes
}

/// Does the collection keep insertion order (has a marker)?
pub async fn enabled(g: &mut impl crate::coll::Rd, coll: &str) -> anyhow::Result<bool> {
    Ok(g.get(&meta_key(coll)).await?.is_some())
}

/// Keys to delete when a collection is dropped or its documents are replaced.
pub fn drop_prefixes(coll: &str) -> Vec<Vec<u8>> {
    vec![prefix(coll)]
}

/// The next sequence number of a collection (enabling insertion order on the first insert).
pub async fn alloc(g: &mut Entry, coll: &str) -> anyhow::Result<u64> {
    if !g.nat.contains_key(coll) {
        let next = match g.get_live(&meta_key(coll)).await? {
            Some(v) => u64::from_be_bytes(v[..8].try_into()?),
            None => migrate(g, coll).await?,
        };
        g.nat.insert(coll.to_string(), (next, next));
    }
    let (next, limit) = g.nat[coll];
    let limit = if next >= limit {
        let l = next + BLOCK;
        g.apply(vec![Op::Put(meta_key(coll), l.to_be_bytes().to_vec())]).await?;
        l
    } else {
        limit
    };
    g.nat.insert(coll.to_string(), (next + 1, limit));
    Ok(next)
}

/// Number the documents a collection already has (in `_id` order); returns the next free number.
async fn migrate(g: &mut Entry, coll: &str) -> anyhow::Result<u64> {
    let dp = doc_prefix(coll);
    let end = prefix_end(&dp);
    let mut start = dp.clone();
    let mut seq = 1u64;
    let np = prefix(coll);
    let stale: Vec<Op> = g.scan(&np).await?.into_iter().map(|(k, _)| Op::Delete(k.to_vec())).collect(); // left by an interrupted migration
    g.apply(stale).await?;
    loop {
        let rows = g.scan_range(&start, end.as_deref(), false, Some(MIGRATE_CHUNK), 4 << 20).await?;
        if rows.is_empty() {
            break;
        }
        let mut ops = Vec::with_capacity(rows.len() * 2);
        for (k, v) in &rows {
            ops.push(Op::Put(key(coll, seq), k[dp.len()..].to_vec()));
            ops.push(Op::Put(k.to_vec(), with_seq(bson_part(v).to_vec(), seq)));
            seq += 1;
        }
        g.apply(ops).await?;
        if rows.len() < MIGRATE_CHUNK {
            break;
        }
        let mut k = rows.last().unwrap().0.to_vec();
        k.push(0);
        start = k;
    }
    Ok(seq)
}

/// Sequence number of the stored document under `doc_key` (latest committed state).
pub async fn stored_seq(g: &mut Entry, doc_key: &[u8]) -> anyhow::Result<Option<u64>> {
    Ok(g.get_live(doc_key).await?.and_then(|v| seq_of(&v)))
}

/// Entry value of an index entry: the document's sequence number (empty for entries written before `natural.rs`).
pub fn entry_value(seq: Option<u64>) -> Vec<u8> {
    seq.map(|s| s.to_be_bytes().to_vec()).unwrap_or_default()
}

fn value_seq(v: &[u8]) -> u64 {
    v.get(..8).map(|b| u64::from_be_bytes(b.try_into().unwrap())).unwrap_or(0)
}

/// Within runs of entries with equal key components the scan returns documents in insertion order (MongoDB breaks
/// index-key ties by record id); `reverse` for a backward scan.
pub fn reorder_ties(rows: &mut [(bytes::Bytes, bytes::Bytes)], prefix: &[u8], reverse: bool) {
    let mut i = 0;
    while i < rows.len() {
        let c = crate::index::entry_comps(&rows[i].0, prefix);
        let mut j = i + 1;
        while j < rows.len() && c.is_some() && crate::index::entry_comps(&rows[j].0, prefix) == c {
            j += 1;
        }
        if j - i > 1 {
            if reverse {
                rows[i..j].sort_by(|a, b| value_seq(&b.1).cmp(&value_seq(&a.1)));
            } else {
                rows[i..j].sort_by(|a, b| value_seq(&a.1).cmp(&value_seq(&b.1)));
            }
        }
        i = j;
    }
}

/// True when the last entry of `rows` has the same key components as `next`.
pub fn same_group(last: &[u8], next: &[u8], prefix: &[u8]) -> bool {
    let a = crate::index::entry_comps(last, prefix);
    a.is_some() && a == crate::index::entry_comps(next, prefix)
}
