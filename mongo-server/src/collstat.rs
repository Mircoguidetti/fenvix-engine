//! Storage counters of each collection (K1): document count, document bytes, `_id` bytes and the bytes of every index,
//! stored in `meta/sstat/{collection}` and moved by every write set in its own batch, so `collStats`, `dbStats` and the
//! `storageStats` of `$collStats` read one small key instead of the whole collection and every index (which, on S3, read
//! the whole database at each call and held the database's exclusive lock while doing so).
//!
//! How the counts stay exact:
//! - a write set ([`crate::coll::Writes`]) knows the old document of every update and delete (it needs it for the
//!   indexes), so it adds up exact deltas; they are applied to the stored counters in the same batch as the writes
//!   ([`ops_for`]), so a crash keeps both or neither;
//! - the counters live in the store, and the open tenant keeps a copy in memory that follows every write of the key
//!   (`Entry::note_sstat`, in `apply_mode`), so a write does not read the store for them;
//! - a collection whose counters were never stored (made before this change, or changed by a command that does not keep
//!   them: rename, convertToCapped) is counted once by reading it, under the exclusive lock, and the result is stored
//!   ([`Server::coll_counters`]); likewise an index whose size is not stored yet (just built): only that index is read;
//! - an index that is created or dropped has its stored size removed (a new index of the same name starts from a read);
//! - `validate` stores the counts it reads.

use crate::layout::escape_collection;
use bson::{doc, Bson, Document};
use std::collections::{BTreeMap, HashMap};
use tenant_bench::lifecycle::Op;

pub const PREFIX: &[u8] = b"meta/sstat/";

pub fn key(coll: &str) -> Vec<u8> {
    let mut k = PREFIX.to_vec();
    k.extend(escape_collection(coll));
    k
}

/// The stored counters of one collection.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CollStat {
    /// Documents.
    pub docs: i64,
    /// Bytes of the documents (BSON, as `collStats.size` counts them).
    pub bytes: i64,
    /// Bytes of the `_id` index (encoded `_id`s).
    pub id_bytes: i64,
    /// Bytes (keys and values) of each other index, by name; an index missing here is not counted yet.
    pub idx: BTreeMap<String, i64>,
}

impl CollStat {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut ix = Document::new();
        for (k, v) in &self.idx {
            ix.insert(k.clone(), *v);
        }
        doc! { "docs": self.docs, "bytes": self.bytes, "id": self.id_bytes, "idx": ix }.to_vec().unwrap_or_default()
    }

    pub fn from_bytes(v: &[u8]) -> Option<CollStat> {
        let d = Document::from_reader(v).ok()?;
        let n = |k: &str| match d.get(k) {
            Some(Bson::Int64(i)) => Some(*i),
            Some(Bson::Int32(i)) => Some(*i as i64),
            _ => None,
        };
        let mut idx = BTreeMap::new();
        if let Ok(ix) = d.get_document("idx") {
            for (k, v) in ix {
                if let Some(b) = match v {
                    Bson::Int64(i) => Some(*i),
                    Bson::Int32(i) => Some(*i as i64),
                    _ => None,
                } {
                    idx.insert(k.clone(), b);
                }
            }
        }
        Some(CollStat { docs: n("docs")?, bytes: n("bytes")?, id_bytes: n("id")?, idx })
    }

    pub fn op(&self, coll: &str) -> Op {
        Op::Put(key(coll), self.to_bytes())
    }

    /// Apply a write set's delta. Index sizes not counted yet stay uncounted (they are read when asked for).
    pub fn apply(&mut self, d: &Delta) {
        self.docs += d.docs;
        self.bytes += d.bytes;
        self.id_bytes += d.id_bytes;
        for (name, b) in &d.idx {
            if let Some(v) = self.idx.get_mut(name) {
                *v += b;
            }
        }
    }
}

/// How a write set moves the counters of one collection.
#[derive(Clone, Debug, Default)]
pub struct Delta {
    pub docs: i64,
    pub bytes: i64,
    pub id_bytes: i64,
    pub idx: HashMap<String, i64>,
}

impl Delta {
    pub fn index(&mut self, name: &str, bytes: i64) {
        *self.idx.entry(name.to_string()).or_insert(0) += bytes;
    }
}

/// The operations that move the stored counters by a write set's deltas (`deltas` is emptied). A collection with no
/// stored counters is left alone (it is counted when asked for), except one that is not in the stored collection list
/// yet: this write set creates it, so it starts from zero.
pub async fn ops_for(g: &mut crate::dbs::Entry, deltas: &mut HashMap<String, Delta>) -> anyhow::Result<Vec<Op>> {
    if deltas.is_empty() {
        return Ok(Vec::new());
    }
    let mut ops = Vec::new();
    let mut stored_names: Option<Vec<String>> = None;
    for (coll, d) in std::mem::take(deltas) {
        let cur = match g.sstat(&coll).await? {
            Some(s) => Some(s),
            None => {
                if stored_names.is_none() {
                    stored_names = Some(g.collections().await?);
                }
                if stored_names.as_ref().unwrap().iter().any(|n| *n == coll) { None } else { Some(CollStat::default()) }
            }
        };
        if let Some(mut s) = cur {
            s.apply(&d);
            ops.push(s.op(&coll));
        }
    }
    Ok(ops)
}

/// The operation that forgets the stored size of some indexes of a collection (they are created or dropped), if the
/// collection has stored counters that have them.
pub async fn forget_indexes_op(g: &mut crate::dbs::Entry, coll: &str, names: &[String]) -> anyhow::Result<Option<Op>> {
    let Some(mut s) = g.sstat(coll).await? else { return Ok(None) };
    let before = s.idx.len();
    s.idx.retain(|k, _| !names.iter().any(|n| n == k));
    Ok((s.idx.len() != before).then(|| s.op(coll)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_apply() {
        let mut s = CollStat { docs: 3, bytes: 300, id_bytes: 30, idx: BTreeMap::from([("a_1".to_string(), 90)]) };
        assert_eq!(CollStat::from_bytes(&s.to_bytes()), Some(s.clone()));
        let mut d = Delta { docs: -1, bytes: -100, id_bytes: -10, idx: HashMap::new() };
        d.index("a_1", -30);
        d.index("b_1", 50); // not counted yet: stays out
        s.apply(&d);
        assert_eq!(s, CollStat { docs: 2, bytes: 200, id_bytes: 20, idx: BTreeMap::from([("a_1".to_string(), 60)]) });
    }

    #[test]
    fn key_escapes_the_name() {
        assert_eq!(key("a/b"), b"meta/sstat/a%2Fb".to_vec());
        assert_ne!(key("a"), key("a/"));
    }
}
