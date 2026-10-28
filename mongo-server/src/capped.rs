//! Capped collections and tailable cursors.
//!
//! A capped collection (`create` with `capped: true, size, [max]`) keeps the documents in insertion order and drops the oldest
//! ones when the sum of the BSON sizes of its documents exceeds `size` bytes or their number exceeds `max`. The options live in
//! the collection options (`meta/copt/{coll}`); the document and byte counts are kept in `meta/cstat/{coll}` and changed by the
//! same atomic batch as the writes (see `Writes`), and the oldest documents are found through the natural-order keyspace.
//!
//! A tailable cursor remembers the sequence number of the last document it looked at and, on `getMore`, continues from there
//! (waiting for new documents for `awaitData` cursors).

use crate::cmd::R;
use crate::coll::{Rd, Writes};
use crate::dbs::Entry;
use crate::error::MongoError;
use crate::index::IndexDef;
use crate::layout::{doc_prefix, escape_collection};
use bson::{doc, Bson, Document};
use tenant_bench::lifecycle::Op;

#[derive(Clone, Debug)]
pub struct Cap {
    pub size: i64,
    pub max: Option<i64>,
}

impl Cap {
    pub fn from_options(raw: &Document) -> Option<Cap> {
        if !matches!(raw.get("capped"), Some(Bson::Boolean(true))) {
            return None;
        }
        let num = |v: Option<&Bson>| match v {
            Some(Bson::Int32(i)) => Some(*i as i64),
            Some(Bson::Int64(i)) => Some(*i),
            Some(Bson::Double(f)) => Some(*f as i64),
            _ => None,
        };
        Some(Cap { size: num(raw.get("size")).unwrap_or(0), max: num(raw.get("max")).filter(|m| *m > 0) })
    }
}

pub fn stat_key(coll: &str) -> Vec<u8> {
    let mut k = b"meta/cstat/".to_vec();
    k.extend(escape_collection(coll));
    k
}

fn type_names(v: &Bson) -> &'static str {
    crate::update::type_name(v)
}

fn num_of(v: &Bson, field: &str, allow_bool: bool) -> R<i64> {
    match v {
        Bson::Int32(i) => Ok(*i as i64),
        Bson::Int64(i) => Ok(*i),
        Bson::Double(f) => Ok(*f as i64),
        Bson::Decimal128(d) => Ok(d.to_string().parse::<f64>().map(|f| f as i64).unwrap_or(0)),
        Bson::Boolean(b) if allow_bool => Ok(*b as i64),
        other => Err(MongoError::type_mismatch(format!(
            "BSON field 'create.{field}' is the wrong type '{}', expected types '[{}long, int, decimal, double']",
            type_names(other),
            if allow_bool { "bool, " } else { "" }
        ))),
    }
}

/// The capped options of a `create` command, validated like mongod does: `Some((size, max))` for a capped collection.
pub fn parse_create(cmd: &Document) -> R<Option<(Bson, Option<Bson>)>> {
    let capped = match cmd.get("capped") {
        None => false,
        Some(v) => num_of(v, "capped", true)? != 0,
    };
    let size = match cmd.get("size") {
        None => None,
        Some(v) => {
            let n = num_of(v, "size", false)?;
            if n < 1 {
                return Err(MongoError::new(51024, "Location51024", format!("BSON field 'size' value must be >= 1, actual value '{n}'")));
            }
            if n > 1125899906842624 {
                return Err(MongoError::new(51024, "Location51024", format!("BSON field 'size' value must be <= 1125899906842624, actual value '{n}'")));
            }
            Some(typed(v, n))
        }
    };
    let max = match cmd.get("max") {
        None => None,
        Some(v) => {
            let n = num_of(v, "max", false)?;
            if n >= 2147483648 {
                return Err(MongoError::new(51024, "Location51024", format!("BSON field 'max' value must be < 2147483648, actual value '{n}'")));
            }
            // a limit of zero or less means none: mongod stores the largest one
            Some(if n <= 0 { Bson::Int32(2147483647) } else { typed(v, n) })
        }
    };
    if capped {
        let Some(size) = size else {
            return Err(MongoError::new(72, "InvalidOptions", "the 'size' field is required when 'capped' is true".into()));
        };
        return Ok(Some((size, max)));
    }
    if size.is_some() || max.is_some() {
        return Err(MongoError::new(72, "InvalidOptions", "the 'capped' field needs to be true when either the 'size' or 'max' fields are present".into()));
    }
    Ok(None)
}

/// A number as mongod stores it in the options: int when the command gave an int, otherwise long.
pub fn typed(v: &Bson, n: i64) -> Bson {
    match v {
        Bson::Int32(_) => Bson::Int32(n as i32),
        Bson::Double(_) => n32(n),
        _ => Bson::Int64(n),
    }
}

/// Int32 when it fits (how the stats commands show sizes), long otherwise.
pub fn n32(n: i64) -> Bson {
    if n >= i32::MIN as i64 && n <= i32::MAX as i64 { Bson::Int32(n as i32) } else { Bson::Int64(n) }
}

/// The options document `listCollections` shows for the capped part of `create`.
pub fn options_doc(size: Bson, max: Option<Bson>) -> Document {
    let mut d = doc! { "capped": true, "size": size };
    if let Some(m) = max {
        d.insert("max", m);
    }
    d
}

async fn read_stat(g: &mut Entry, coll: &str) -> anyhow::Result<(i64, i64)> {
    Ok(match g.get_live(&stat_key(coll)).await? {
        Some(v) => {
            let d = Document::from_reader(&v[..])?;
            (d.get_i64("docs").unwrap_or(0), d.get_i64("bytes").unwrap_or(0))
        }
        None => (0, 0),
    })
}

fn stat_op(coll: &str, docs: i64, bytes: i64) -> Op {
    Op::Put(stat_key(coll), doc! { "docs": docs, "bytes": bytes }.to_vec().unwrap_or_default())
}

/// The counts of a capped collection as stored: (documents, bytes).
pub async fn counts(g: &mut Entry, coll: &str) -> anyhow::Result<(i64, i64)> {
    read_stat(g, coll).await
}

/// Before a write set is applied: the counts of every capped collection it touched move by its deltas and the oldest
/// documents go while the collection is over its size or document limit (never the newest document).
pub async fn enforce(g: &mut Entry, db: &str, w: &mut Writes, indexes: &[IndexDef], coll: &str) -> R<()> {
    let Some(delta) = w.cap_delta.remove(coll) else { return Ok(()) };
    let Some(cap) = g.cap_of(coll) else { return Ok(()) };
    let (mut docs, mut bytes) = read_stat(g, coll).await?;
    docs += delta.0;
    bytes += delta.1;
    let np = crate::natural::prefix(coll);
    let mut from = np.clone();
    let mut batch_seqs: Option<Vec<(u64, Vec<u8>)>> = None;
    let over = |docs: i64, bytes: i64| bytes > cap.size || cap.max.map(|m| docs > m).unwrap_or(false);
    let mut stored: std::collections::VecDeque<(u64, Vec<u8>)> = std::collections::VecDeque::new();
    while over(docs, bytes) && docs > 1 {
        // the oldest document: the committed ones first, then those of this write set
        if stored.is_empty() {
            let end = crate::keyenc::prefix_end(&np);
            let rows = g.scan_range(&from, end.as_deref(), false, Some(256), 1 << 20).await?;
            for (k, v) in &rows {
                let seq = u64::from_be_bytes(k[np.len()..np.len() + 8].try_into().unwrap_or([0; 8]));
                stored.push_back((seq, v.to_vec()));
            }
            if let Some((k, _)) = rows.last() {
                let mut nk = k.to_vec();
                nk.push(0);
                from = nk;
            }
        }
        let next = match stored.pop_front() {
            Some(x) => Some(x),
            None => {
                if batch_seqs.is_none() {
                    let mut v = w.batch_docs(coll);
                    v.sort();
                    batch_seqs = Some(v);
                }
                let v = batch_seqs.as_mut().unwrap();
                if v.is_empty() { None } else { Some(v.remove(0)) }
            }
        };
        let Some((_, id_enc)) = next else { break };
        let mut key = doc_prefix(coll);
        key.extend(&id_enc);
        let victim = match w.ov.get(&key) {
            Some(Some(d)) => Some(d.clone()),
            Some(None) => None, // already deleted in this write set
            None => g.get_live(&key).await?.map(|v| Document::from_reader(crate::natural::bson_part(&v))).transpose()?,
        };
        let Some(victim) = victim else { continue };
        let len = victim.to_vec()?.len() as i64;
        w.delete_quiet(g, db, coll, indexes, &victim).await?;
        docs -= 1;
        bytes -= len;
    }
    w.cap_delta.remove(coll);
    w.extra.push(stat_op(coll, docs, bytes));
    Ok(())
}

/// A tailable cursor: where it stands in the natural order of its collection.
#[derive(Clone)]
pub struct Tail {
    pub db: String,
    pub coll: String,
    pub filter: Document,
    pub await_data: bool,
    /// Sequence number of the last document looked at.
    pub last_seq: u64,
}

/// Documents after `after_seq` in natural order that match `filter`, at most `limit`; with the new position.
pub async fn fetch_after(g: &mut impl Rd, tail: &Tail, limit: usize) -> R<(Vec<Document>, u64)> {
    let np = crate::natural::prefix(&tail.coll);
    let end = crate::keyenc::prefix_end(&np);
    // the document last looked at must still be there
    if tail.last_seq > 0 && g.get(&crate::natural::key(&tail.coll, tail.last_seq)).await?.is_none() {
        return Err(MongoError::new(136, "CappedPositionLost", format!("CollectionScan died due to position in capped collection being deleted. Last seen record id: RecordId({})", tail.last_seq)));
    }
    let mut start = crate::natural::key(&tail.coll, tail.last_seq);
    start.push(0);
    let mut out = Vec::new();
    let mut last = tail.last_seq;
    let prefix = doc_prefix(&tail.coll);
    'outer: loop {
        let rows = g.scan_range(&start, end.as_deref(), false, Some(256), 1 << 20).await?;
        let n = rows.len();
        for (k, id_enc) in &rows {
            let seq = u64::from_be_bytes(k[np.len()..np.len() + 8].try_into().unwrap_or([0; 8]));
            let mut dk = prefix.clone();
            dk.extend(id_enc.iter());
            if let Some(v) = g.get(&dk).await? {
                let d = Document::from_reader(crate::natural::bson_part(&v))?;
                if crate::query::matches(&d, &tail.filter)? {
                    out.push(d);
                }
            }
            last = seq;
            if out.len() >= limit {
                break 'outer;
            }
        }
        if n < 256 {
            break;
        }
        let mut nk = rows.last().unwrap().0.to_vec();
        nk.push(0);
        start = nk;
    }
    Ok((out, last))
}
