//! Step 5b: SlateDB skips its SSTs whose every row is at or below the reader's X_d.
//!
//! Our runs hold the newest row at or below X_d of every key (the manifest invariant), and the merged read ignores every
//! SlateDB row at or below X_d (`read.rs`). Yet SlateDB's own reads still open those SSTs: absorbed L0 SSTs until the drop
//! folds them, and the drop's aged runs, which hold only absorbed rows. A cold range read paid one block of each (step 5,
//! MEASURED: 2-3.5 GETs per range read at 1 TB medium, about 9 at busy).
//!
//! SlateDB 0.17 lets a filter policy see every row (its seq included) while an SST is built, and gives the filter a
//! caller context at read time, for point reads and range scans (`supports_range_queries`). This policy records the
//! SST's lowest and highest seq; at read time the merged reader passes the X_d of the version it holds, and the filter
//! answers "no match" when the SST's highest seq is at or below it. SlateDB checks filters before it fetches any data
//! block, so a skipped SST costs no GET. An SST that also holds rows above X_d (an L0 SST spanning the cut) is read whole;
//! its rows at or below X_d are ignored by the read rule, as before.
//!
//! Safety: only the X_d of the version the read holds is ever passed (the pair of rule 1: one version and one SlateDB
//! view per read), so every skipped row is at or below a cut whose runs the read also reads; rows above X_d are never in a
//! skipped SST. SSTs written without this filter (before this release, or by a builder without it) have no such filter
//! and are read as before. A binary without this policy skips the unknown filter (SlateDB matches filters by name), so a
//! rollback reads every SST.

use bytes::{Buf, BufMut};
use slatedb::{Filter, FilterBuilder, FilterContext, FilterPolicy, FilterQuery, RowEntry};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

pub const NAME: &str = "fx-seq-v1";
const MAGIC: [u8; 8] = *b"fxseq\0\0\x01";

/// On by default: the merged reader passes its X_d to SlateDB. Off: SlateDB opens every SST, as before step 5b.
pub static SKIP_BELOW_XD: AtomicBool = AtomicBool::new(true);
/// SSTs that a read skipped with this filter, and SSTs it asked about (diagnostics).
pub static SKIPPED: AtomicU64 = AtomicU64::new(0);
pub static ASKED: AtomicU64 = AtomicU64::new(0);
/// Negative control (tests and benches only): the reader passes X_d plus this, as a reader that trusted a cut newer
/// than the one its runs hold would. Rows in (X_d, X_d + extra] that only SlateDB holds are then lost. 0: off.
pub static CONTROL_EXTRA: AtomicU64 = AtomicU64::new(0);

/// The filter context of a read that holds a version with this X_d (None when skipping is off).
pub fn context(x_d: u64) -> Option<FilterContext> {
    if !SKIP_BELOW_XD.load(Relaxed) {
        return None;
    }
    let cut = x_d.saturating_add(CONTROL_EXTRA.load(Relaxed));
    let mut b = [0u8; 64];
    b[..8].copy_from_slice(&MAGIC);
    b[8..16].copy_from_slice(&cut.to_le_bytes());
    Some(FilterContext::Inline(b))
}

fn cut_of(q: &FilterQuery) -> Option<u64> {
    match &q.context {
        Some(FilterContext::Inline(b)) if b[..8] == MAGIC => Some(u64::from_le_bytes(b[8..16].try_into().unwrap())),
        _ => None,
    }
}

pub struct SeqRangePolicy;

pub fn policy() -> Arc<dyn FilterPolicy> {
    Arc::new(SeqRangePolicy)
}

impl FilterPolicy for SeqRangePolicy {
    fn name(&self) -> &str {
        NAME
    }
    fn builder(&self) -> Box<dyn FilterBuilder> {
        Box::new(SeqRangeBuilder { min: u64::MAX, max: 0, any: false })
    }
    fn decode(&self, data: &[u8]) -> Arc<dyn Filter> {
        let mut d = data;
        if d.len() < 17 || d.get_u8() != 1 {
            // unknown encoding: never rule anything out
            return Arc::new(SeqRange { min: 0, max: u64::MAX });
        }
        let min = d.get_u64_le();
        let max = d.get_u64_le();
        Arc::new(SeqRange { min, max })
    }
    fn estimate_size(&self, _num_keys: usize) -> usize {
        17
    }
    fn supports_range_queries(&self) -> bool {
        true
    }
}

struct SeqRangeBuilder {
    min: u64,
    max: u64,
    any: bool,
}

impl FilterBuilder for SeqRangeBuilder {
    fn add_entry(&mut self, entry: &RowEntry) {
        self.any = true;
        self.min = self.min.min(entry.seq);
        self.max = self.max.max(entry.seq);
    }
    fn build(&mut self) -> Arc<dyn Filter> {
        if self.any {
            Arc::new(SeqRange { min: self.min, max: self.max })
        } else {
            Arc::new(SeqRange { min: 0, max: 0 })
        }
    }
}

/// The lowest and highest seq of one SST's rows.
pub struct SeqRange {
    pub min: u64,
    pub max: u64,
}

impl Filter for SeqRange {
    fn might_match(&self, query: &FilterQuery) -> bool {
        let Some(cut) = cut_of(query) else { return true };
        ASKED.fetch_add(1, Relaxed);
        let m = self.max > cut;
        if !m {
            SKIPPED.fetch_add(1, Relaxed);
        }
        m
    }
    fn encode(&self, w: &mut dyn BufMut) {
        w.put_u8(1);
        w.put_u64_le(self.min);
        w.put_u64_le(self.max);
    }
    fn size(&self) -> usize {
        17
    }
    fn clamp_allocated_size(&self) -> Arc<dyn Filter> {
        Arc::new(SeqRange { min: self.min, max: self.max })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn skips_only_at_or_below_the_cut() {
        let f = SeqRange { min: 10, max: 20 };
        let q = |x: u64| FilterQuery::point(Bytes::from_static(b"k")).with_context(context(x));
        assert!(f.might_match(&q(19)));
        assert!(!f.might_match(&q(20)));
        assert!(!f.might_match(&q(25)));
        assert!(f.might_match(&FilterQuery::point(Bytes::from_static(b"k"))));
        let mut buf = Vec::new();
        f.encode(&mut buf);
        let g = SeqRangePolicy.decode(&buf);
        assert!(!g.might_match(&q(20)) && g.might_match(&q(19)));
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// Step 5b, second filter: the customers of an SST (layout v2). A range scan whose two ends are keys of one customer
// (a find inside one customer) skips an SST that holds no row of that customer. It answers only range queries (point
// reads have the Bloom filter); a filter never says "no" for a customer it holds, so answers cannot change. It is what
// the plan meant by "the memtable and L0 are checked in RAM": L0 SSTs not absorbed yet hold rows above X_d, so the seq
// filter cannot skip them, but most do not hold the customer read.

/// The customer filter's policy name.
pub const CUST_NAME: &str = "fx-cust-v1";
/// On by default; off: range scans open every SST whose key range meets them (as before 5b).
pub static CUSTOMER_FILTER: AtomicBool = AtomicBool::new(true);
pub static CUST_ASKED: AtomicU64 = AtomicU64::new(0);
pub static CUST_SKIPPED: AtomicU64 = AtomicU64::new(0);
/// Negative control (tests only): the customer filter answers "not here" for every range inside one customer.
pub static CONTROL_CUST_NONE: AtomicBool = AtomicBool::new(false);

const CUST_SEED: u64 = 0x4658_4355_5354_3031; // "FXCUST01"
const CUST_BITS_PER: u64 = 10;
const CUST_HASHES: u8 = 7;

fn cust_of(key: &[u8]) -> Option<&[u8]> {
    crate::runfmt::CustomerKind::V2.customer_of(key).map(|r| &key[r])
}

fn cust_hash(c: &[u8]) -> u64 {
    twox_hash::XxHash64::oneshot(CUST_SEED, c)
}

fn positions(h: u64, k: u8, bits: u64) -> impl Iterator<Item = u64> {
    let h2 = h.rotate_left(32) | 1;
    (0..k as u64).map(move |i| h.wrapping_add(i.wrapping_mul(h2)) % bits)
}

/// The customer component shared by every key of [lo, hi] (`hi.1` true) or [lo, hi) (false), if there is one: lo is a key
/// of customer C (its prefix `u/{component}`), and hi is too, or hi (excluded) is at most the prefix's successor. Every
/// key between them then starts with C's prefix.
pub fn one_customer<'a>(lo: &'a [u8], hi: (&[u8], bool)) -> Option<&'a [u8]> {
    let r = crate::runfmt::CustomerKind::V2.customer_of(lo)?;
    let prefix = &lo[..r.end];
    let (h, included) = hi;
    if h.starts_with(prefix) {
        return Some(&lo[r]);
    }
    if included {
        return None;
    }
    // the successor of the prefix: its last byte plus one (a prefix ending in 0xff has none here)
    let mut succ = prefix.to_vec();
    let last = succ.last_mut()?;
    if *last == 0xff {
        return None;
    }
    *last += 1;
    (h <= &succ[..]).then_some(&lo[r])
}

pub struct CustPolicy;

pub fn cust_policy() -> Arc<dyn FilterPolicy> {
    Arc::new(CustPolicy)
}

impl FilterPolicy for CustPolicy {
    fn name(&self) -> &str {
        CUST_NAME
    }
    fn builder(&self) -> Box<dyn FilterBuilder> {
        Box::new(CustBuilder { hs: Vec::new(), last: Vec::new() })
    }
    fn decode(&self, data: &[u8]) -> Arc<dyn Filter> {
        let mut d = data;
        if d.len() < 10 || d.get_u8() != 1 {
            return Arc::new(Custs { bits: 0, data: Box::new([]), valid: false });
        }
        let bits = d.get_u64_le();
        let _k = d.get_u8();
        if (d.len() as u64) * 8 != bits {
            return Arc::new(Custs { bits: 0, data: Box::new([]), valid: false });
        }
        Arc::new(Custs { bits, data: d.to_vec().into_boxed_slice(), valid: true })
    }
    fn estimate_size(&self, num_keys: usize) -> usize {
        10 + num_keys * CUST_BITS_PER as usize / 8 + 8
    }
    fn supports_range_queries(&self) -> bool {
        true
    }
}

struct CustBuilder {
    hs: Vec<u64>,
    last: Vec<u8>,
}

impl FilterBuilder for CustBuilder {
    fn add_entry(&mut self, entry: &RowEntry) {
        // keys arrive sorted: one hash per customer
        if let Some(c) = cust_of(&entry.key) {
            if c != &self.last[..] {
                self.last = c.to_vec();
                self.hs.push(cust_hash(c));
            }
        }
    }
    fn build(&mut self) -> Arc<dyn Filter> {
        let bits = (self.hs.len() as u64 * CUST_BITS_PER).max(64).div_ceil(8) * 8;
        let mut data = vec![0u8; (bits / 8) as usize];
        for &h in &self.hs {
            for p in positions(h, CUST_HASHES, bits) {
                data[(p / 8) as usize] |= 1 << (p % 8);
            }
        }
        Arc::new(Custs { bits, data: data.into_boxed_slice(), valid: true })
    }
}

pub struct Custs {
    bits: u64,
    data: Box<[u8]>,
    valid: bool,
}

impl Custs {
    fn may_hold(&self, c: &[u8]) -> bool {
        positions(cust_hash(c), CUST_HASHES, self.bits).all(|p| self.data[(p / 8) as usize] & (1 << (p % 8)) != 0)
    }
}

impl Filter for Custs {
    fn might_match(&self, query: &FilterQuery) -> bool {
        if !self.valid || !CUSTOMER_FILTER.load(Relaxed) {
            return true;
        }
        let slatedb::FilterTarget::Range { lower, upper } = &query.target else { return true };
        use std::ops::Bound::*;
        let (Included(lo) | Excluded(lo)) = lower else { return true };
        let hi = match upper {
            Included(h) => (&h[..], true),
            Excluded(h) => (&h[..], false),
            Unbounded => return true,
        };
        let Some(a) = one_customer(lo, hi) else { return true };
        CUST_ASKED.fetch_add(1, Relaxed);
        let m = !CONTROL_CUST_NONE.load(Relaxed) && self.may_hold(a);
        if !m {
            CUST_SKIPPED.fetch_add(1, Relaxed);
        }
        m
    }
    fn encode(&self, w: &mut dyn BufMut) {
        w.put_u8(1);
        w.put_u64_le(self.bits);
        w.put_u8(CUST_HASHES);
        w.put_slice(&self.data);
    }
    fn size(&self) -> usize {
        10 + self.data.len()
    }
    fn clamp_allocated_size(&self) -> Arc<dyn Filter> {
        Arc::new(Custs { bits: self.bits, data: self.data.clone(), valid: self.valid })
    }
}

#[cfg(test)]
mod cust_tests {
    use super::*;
    use bytes::Bytes;
    use std::ops::Bound;

    fn k(name: &str, rest: &str) -> Bytes {
        let mut v = b"u/".to_vec();
        v.push(name.len() as u8);
        v.extend(name.as_bytes());
        v.extend(rest.as_bytes());
        Bytes::from(v)
    }

    #[test]
    fn customer_filter_answers_ranges_inside_one_customer() {
        let mut b = CustPolicy.builder();
        for (n, r) in [("c01", "m/d/1"), ("c01", "m/d/2"), ("c05", "m/d/1")] {
            b.add_entry(&RowEntry { key: k(n, r), value: slatedb::ValueDeletable::Value(Bytes::new()), seq: 1, create_ts: None, expire_ts: None });
        }
        let f = b.build();
        let mut buf = Vec::new();
        f.encode(&mut buf);
        let f = CustPolicy.decode(&buf);
        let q = |n: &str| FilterQuery::range(Bound::Included(k(n, "m/d/")), Bound::Excluded(k(n, "m/d0")));
        assert!(f.might_match(&q("c01")) && f.might_match(&q("c05")));
        let absent = (0..200).filter(|i| !f.might_match(&q(&format!("x{i:03}")))).count();
        assert!(absent > 190, "absent customers are mostly skipped: {absent}");
        // a range across customers, or open-ended, is never skipped
        assert!(f.might_match(&FilterQuery::range(Bound::Included(k("x001", "")), Bound::Excluded(k("x003", "")))));
        assert!(f.might_match(&FilterQuery::range(Bound::Included(k("x001", "")), Bound::Unbounded)));
        assert!(f.might_match(&FilterQuery::point(k("x001", "m/d/1"))));
        // a whole customer: [prefix, successor of the prefix)
        let whole = |n: &str| {
            let lo = k(n, "");
            let mut hi = lo.to_vec();
            *hi.last_mut().unwrap() += 1;
            FilterQuery::range(Bound::Included(lo), Bound::Excluded(Bytes::from(hi)))
        };
        assert!(f.might_match(&whole("c01")));
        assert!((0..200).filter(|i| !f.might_match(&whole(&format!("x{i:03}")))).count() > 190);
        // one byte past the successor reaches the next customer: never skipped
        let mut hi = k("x001", "").to_vec();
        *hi.last_mut().unwrap() += 1;
        hi.push(0);
        assert!(f.might_match(&FilterQuery::range(Bound::Included(k("x001", "")), Bound::Excluded(Bytes::from(hi)))));
    }
}
