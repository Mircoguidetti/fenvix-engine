//! The customer's box (P-big-speed fix 3): the first request of a customer warms the customer's whole key range.
//!
//! In layout v2 (`custkey.rs`) one customer's documents and co-located index entries, of every customer-keyed
//! collection, are one contiguous physical key range `u/{customer component}`. For a LangGraph thread (customer key
//! `thread_id`) that range holds its checkpoints, its pending writes and both collections' index entries: the thread's
//! "box". A customer that comes back after idle asks for a few things of the box at once (the latest checkpoint and its
//! writes; a save reads its unique index and locator first).
//!
//! The first find of a customer not warmed recently brings the box's whole range into the caches once, before the
//! query itself runs, in one wave (`Tenant::warm_range`): every block of our runs that holds the range (one ranged GET
//! per run, all runs at once) and SlateDB's blocks of the range above X_d (a raw scan, the absorbed SSTs skipped as in
//! every read). No row is decoded: a first cut that read the box through the merged reader decoded 3.6 MB of
//! checkpoints per thread (dev bench, MEASURED) and walked the blocks one after the other. The query, and the next requests of that customer, then find every block they need in the
//! caches (SlateDB's block cache or the waiter's counter, our runs' slice cache): no object-store request.
//!
//! Why the answers stay exact: nothing is kept here but the fact that a box was warmed. What the warm-up brings are
//! blocks of immutable SSTs and runs, in the caches every read uses anyway, keyed by their SST or run; a write after
//! the warm-up goes to the memtable, which every read consults first. So a box can be stale only in cost (a block
//! evicted, or a new run after a reseal), never in content.
//!
//! The cost is the stored bytes of the box (one wave of GETs) and their place in the caches. A customer is warmed again only after `TTL_SECS` without a warm-up (its blocks may have left the caches).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// On by default (`--customer-box`).
pub static ENABLED: AtomicBool = AtomicBool::new(true);
/// A customer is warmed again after this long (`--customer-box-ttl-secs`).
pub static TTL_SECS: AtomicU64 = AtomicU64::new(600);
/// Read-ahead of the warm-up's scan (bytes per request of SlateDB's scan and of our runs' cursor).
pub static READ_AHEAD: AtomicU64 = AtomicU64::new(8 << 20);
/// Most customers remembered; past it the oldest half is forgotten (they are warmed again at their next request).
const MAX_REMEMBERED: usize = 200_000;

/// Counters (serverStatus `fenvix.customerBox`).
pub static WARMS: AtomicU64 = AtomicU64::new(0);
pub static WARM_ROWS: AtomicU64 = AtomicU64::new(0);
/// Runs considered, run blocks fetched, SlateDB rows passed by the warm-ups.
pub static WARM_RUNS: AtomicU64 = AtomicU64::new(0);
pub static WARM_BLOCKS: AtomicU64 = AtomicU64::new(0);
pub static WARM_MICROS: AtomicU64 = AtomicU64::new(0);
pub static WARM_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static SKIPPED_RECENT: AtomicU64 = AtomicU64::new(0);

static SEEN: std::sync::LazyLock<Mutex<HashMap<(String, Vec<u8>), Instant>>> = std::sync::LazyLock::new(Default::default);

/// The physical range `[lo, hi)` of a customer's box.
pub fn range_of(comp: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut lo = crate::custkey::U.to_vec();
    lo.extend_from_slice(comp);
    let hi = crate::keyenc::prefix_end(&lo)?;
    Some((lo, hi))
}

/// True when the box of `comp` in `db` should be warmed now (and marks it warmed: a concurrent first request of the same
/// customer does not warm it a second time, it reads as before).
fn claim(db: &str, comp: &[u8]) -> bool {
    let ttl = Duration::from_secs(TTL_SECS.load(Relaxed));
    let now = Instant::now();
    let mut m = SEEN.lock().unwrap();
    let k = (db.to_string(), comp.to_vec());
    if let Some(t) = m.get(&k) {
        if now.duration_since(*t) < ttl {
            SKIPPED_RECENT.fetch_add(1, Relaxed);
            return false;
        }
    }
    if m.len() >= MAX_REMEMBERED {
        let mut v: Vec<_> = m.iter().map(|(k, t)| (*t, k.clone())).collect();
        v.sort();
        for (_, k) in v.into_iter().take(MAX_REMEMBERED / 2) {
            m.remove(&k);
        }
    }
    m.insert(k, now);
    true
}

/// Forget the boxes of `db` (dropped or erased: its next customers start cold, and a new database of the same name
/// warms its own).
pub fn forget_db(db: &str) {
    SEEN.lock().unwrap().retain(|(d, _), _| d != db);
}

/// Warm the box of customer `comp` of `db` on tenant `t`, unless it was warmed in the last `TTL_SECS`.
pub async fn warm(db: &str, comp: &[u8], t: &tenant_bench::lifecycle::Tenant) {
    if !ENABLED.load(Relaxed) {
        return;
    }
    let Some((lo, hi)) = range_of(comp) else { return };
    if !claim(db, comp) {
        return;
    }
    let t0 = Instant::now();
    match t.warm_range(&lo, &hi, READ_AHEAD.load(Relaxed) as usize).await {
        Ok((runs, blocks, rows)) => {
            WARMS.fetch_add(1, Relaxed);
            WARM_RUNS.fetch_add(runs, Relaxed);
            WARM_BLOCKS.fetch_add(blocks, Relaxed);
            WARM_ROWS.fetch_add(rows, Relaxed);
        }
        Err(e) => {
            // a failed warm-up costs nothing but time: the query reads as before
            WARM_ERRORS.fetch_add(1, Relaxed);
            SEEN.lock().unwrap().remove(&(db.to_string(), comp.to_vec()));
            eprintln!("customer box: warm-up of a customer of {db} failed: {e:#}");
        }
    }
    WARM_MICROS.fetch_add(t0.elapsed().as_micros() as u64, Relaxed);
}

#[cfg(test)]
mod tests {
    #[test]
    fn range_is_the_customer_prefix() {
        let c = crate::custkey::comp(&bson::Bson::String("u0001-t0".into()));
        let (lo, hi) = super::range_of(&c).unwrap();
        assert!(lo.starts_with(b"u/"));
        // every physical key of the customer (custkey::to_phys) lies inside, another customer's does not
        let mut d = crate::layout::doc_prefix("checkpoints");
        d.push(crate::custkey::MARK);
        d.extend(&c);
        d.extend(b"rest");
        let p = crate::custkey::to_phys(&d).unwrap();
        assert!(p >= lo && p < hi);
        let c2 = crate::custkey::comp(&bson::Bson::String("u0001-t00".into()));
        let mut d2 = crate::layout::doc_prefix("checkpoints");
        d2.push(crate::custkey::MARK);
        d2.extend(&c2);
        let p2 = crate::custkey::to_phys(&d2).unwrap();
        assert!(!(p2 >= lo && p2 < hi), "a customer whose value extends another's is outside its box");
    }

    #[test]
    fn claim_once_per_ttl() {
        assert!(super::claim("db_claim", b"c1"));
        assert!(!super::claim("db_claim", b"c1"));
        assert!(super::claim("db_claim", b"c2"));
        super::forget_db("db_claim");
        assert!(super::claim("db_claim", b"c1"));
    }
}
