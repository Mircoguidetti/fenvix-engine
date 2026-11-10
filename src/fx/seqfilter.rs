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
