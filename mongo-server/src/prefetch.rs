//! Read-ahead on a signal (T1, "scaldare al primo segnale"): the documents a client is about to ask for come to the
//! counter before the client asks, but only when a read says so.
//!
//! - **First signal.** The first indexed read (sorted, with a limit: "the last 20 messages of client X") of a key prefix
//!   this process has not seen brings, in the background and in one trip, the documents of the next
//!   `READ_AHEAD_DOCS` index entries after the last one returned, in the same direction. With an index `{client, ts: -1}`
//!   that is the client's recent part beyond the first page.
//! - **Continuation signal.** A later read of the same prefix that goes past the middle of what was brought ahead (the
//!   client is scrolling back, "il mese prima") brings the next `READ_AHEAD_DOCS` beyond the frontier. Nothing is read
//!   ahead without such a read.
//!
//! The index entries are read like any data (choice (b), 2026-11-06: collection-index blocks are not maps): usually
//! their block is on the counter already, because the read that gave the signal just read it. The documents are read through a reader of the database whose cache marks their blocks as taken ahead
//! (`Counter::prefetch_cache`): if nobody reads them they are the first to leave the counter.
//! The reader sees the database's SSTs, not the writer's memtable: documents written in the last minutes are not read
//! ahead (they are in the writer's memory already).

use bytes::Bytes;
use futures::StreamExt;
use object_store::ObjectStore;
use slatedb::config::DbReaderOptions;
use slatedb::{DbReader, DbReaderMode, IterationOrder};
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tenant_bench::counter::Counter;

/// Documents brought ahead per signal (`--read-ahead-docs`, 0 = off).
pub static READ_AHEAD_DOCS: AtomicUsize = AtomicUsize::new(0);
/// First signals, continuation signals, documents read ahead, read-ahead runs failed.
pub static FIRST: AtomicU64 = AtomicU64::new(0);
pub static NEXT: AtomicU64 = AtomicU64::new(0);
pub static DOCS: AtomicU64 = AtomicU64::new(0);
pub static FAILS: AtomicU64 = AtomicU64::new(0);
/// Read-aheads running now.
pub static BUSY: AtomicU64 = AtomicU64::new(0);

/// Most prefixes remembered per process (past it the memory starts again from empty).
const MAX_WINDOWS: usize = 200_000;
/// A read-ahead reader unused for this long is closed.
const READER_IDLE: Duration = Duration::from_secs(120);

struct Window {
    /// End of the scanned interval (exclusive bound in the scan direction).
    bound: Vec<u8>,
    reverse: bool,
    /// Last index entry brought ahead, and the middle of the last window.
    frontier: Vec<u8>,
    mid: Vec<u8>,
    coll: String,
}

static INSTALLED: OnceLock<(Arc<Counter>, Arc<dyn ObjectStore>)> = OnceLock::new();
/// (tenant, prefix) -> window. Ordered so that the window of a prefix of a key is found with one range lookup.
static WINDOWS: std::sync::LazyLock<Mutex<BTreeMap<(String, Vec<u8>), Window>>> = std::sync::LazyLock::new(Default::default);
static READERS: std::sync::LazyLock<tokio::sync::Mutex<HashMap<String, (Arc<DbReader>, Instant)>>> = std::sync::LazyLock::new(Default::default);

pub fn install(counter: Arc<Counter>, store: Arc<dyn ObjectStore>) {
    let _ = INSTALLED.set((counter, store));
    tokio::spawn(async {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let old: Vec<Arc<DbReader>> = {
                let mut r = READERS.lock().await;
                let gone: Vec<String> = r.iter().filter(|(_, (_, at))| at.elapsed() > READER_IDLE).map(|(k, _)| k.clone()).collect();
                gone.into_iter().filter_map(|k| r.remove(&k).map(|(d, _)| d)).collect()
            };
            for d in old {
                let _ = d.close().await;
            }
        }
    });
}

/// The database was dropped: forget its windows (they hold its index keys) and close its read-ahead reader.
pub async fn forget(tenant: &str) {
    WINDOWS.lock().unwrap().retain(|(t, _), _| t != tenant);
    let d = READERS.lock().await.remove(tenant);
    if let Some((d, _)) = d {
        let _ = d.close().await;
    }
}

fn lcp(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().zip(b).take_while(|(x, y)| x == y).map(|(x, _)| *x).collect()
}

/// An indexed read finished: `lo..hi` is its interval (full index keys), `reverse` its direction, `last` the last index
/// entry it returned (None: it returned nothing).
pub fn note(tenant: &str, coll: &str, lo: &[u8], hi: &[u8], reverse: bool, last: Option<&[u8]>) {
    let n = READ_AHEAD_DOCS.load(Relaxed);
    let Some(last) = last else { return };
    if n == 0 || INSTALLED.get().is_none() {
        return;
    }
    let scope = lcp(lo, hi);
    let bound = if reverse { lo.to_vec() } else { hi.to_vec() };
    let start_from: Vec<u8>;
    {
        let mut w = WINDOWS.lock().unwrap();
        // a window whose prefix is a prefix of this interval (same client, any date bound)
        let found = w
            .range(..=(tenant.to_string(), lo.to_vec()))
            .next_back()
            .filter(|((t, p), _)| t == tenant && lo.starts_with(p) && hi.starts_with(p))
            .map(|(k, _)| k.clone());
        match found {
            None => {
                if w.len() >= MAX_WINDOWS {
                    w.clear();
                }
                FIRST.fetch_add(1, Relaxed);
                start_from = last.to_vec();
                w.insert((tenant.to_string(), scope), Window { bound, reverse, frontier: last.to_vec(), mid: last.to_vec(), coll: coll.to_string() });
            }
            Some(k) => {
                let win = w.get_mut(&k).unwrap();
                if win.reverse != reverse {
                    return;
                }
                let past_mid = if reverse { last <= win.mid.as_slice() } else { last >= win.mid.as_slice() };
                if !past_mid {
                    return;
                }
                NEXT.fetch_add(1, Relaxed);
                // continue from the frontier, or from where the read ended if it went past the frontier
                let beyond = if reverse { last < win.frontier.as_slice() } else { last > win.frontier.as_slice() };
                start_from = if beyond { last.to_vec() } else { win.frontier.clone() };
                win.bound = bound;
                // mark it in progress: the next read past this point continues from the new frontier
                win.mid = start_from.clone();
            }
        }
    }
    let key = (tenant.to_string(), lcp(lo, hi));
    let tenant = tenant.to_string();
    let coll = coll.to_string();
    let bound = if reverse { lo.to_vec() } else { hi.to_vec() };
    BUSY.fetch_add(1, Relaxed);
    tokio::spawn(async move {
        match run(&tenant, &coll, &start_from, &bound, reverse, n).await {
            Ok(Some((mid, frontier))) => {
                let mut w = WINDOWS.lock().unwrap();
                let k = w.range(..=key.clone()).next_back().filter(|((t, p), _)| *t == key.0 && key.1.starts_with(p)).map(|(k, _)| k.clone());
                if let Some(win) = k.and_then(|k| w.get_mut(&k)) {
                    win.mid = mid;
                    win.frontier = frontier;
                }
            }
            Ok(None) => {}
            Err(e) => {
                if FAILS.fetch_add(1, Relaxed) < 5 {
                    eprintln!("read-ahead of {tenant}.{coll}: {e:#}");
                }
            }
        }
        BUSY.fetch_sub(1, Relaxed);
    });
}

async fn reader(tenant: &str) -> anyhow::Result<Arc<DbReader>> {
    let (counter, store) = INSTALLED.get().ok_or_else(|| anyhow::anyhow!("read-ahead not installed"))?;
    let mut r = READERS.lock().await;
    if let Some((d, at)) = r.get_mut(tenant) {
        *at = Instant::now();
        return Ok(Arc::clone(d));
    }
    let mut o = DbReaderOptions::default();
    o.skip_wal_replay = true;
    let d = DbReader::builder(tenant_bench::lifecycle::tenant_path(tenant), tenant_bench::counting::owned(store, tenant))
        .with_reader_mode(DbReaderMode::FollowLatest)
        .with_options(o)
        .with_db_cache(counter.prefetch_cache(), u64::MAX)
        .build()
        .await?;
    let d = Arc::new(d);
    r.insert(tenant.to_string(), (Arc::clone(&d), Instant::now()));
    Ok(d)
}

/// Read ahead: the next `n` index entries after `from` (exclusive) towards `bound`, and their documents, in parallel.
/// Returns (middle entry, last entry) of what was read, None when nothing was left.
async fn run(tenant: &str, coll: &str, from: &[u8], bound: &[u8], reverse: bool, n: usize) -> anyhow::Result<Option<(Vec<u8>, Vec<u8>)>> {
    let d = reader(tenant).await?;
    let range: (Bound<Vec<u8>>, Bound<Vec<u8>>) =
        if reverse { (Bound::Included(bound.to_vec()), Bound::Excluded(from.to_vec())) } else { (Bound::Excluded(from.to_vec()), Bound::Excluded(bound.to_vec())) };
    if range_empty(&range) {
        return Ok(None);
    }
    let opts = slatedb::config::ScanOptions { order: if reverse { IterationOrder::Descending } else { IterationOrder::Ascending }, cache_blocks: true, ..Default::default() };
    let mut it = d.scan_with_options(range, &opts).await?;
    let mut entries: Vec<Bytes> = Vec::with_capacity(n);
    while let Some(kv) = it.next().await? {
        entries.push(kv.key);
        if entries.len() >= n {
            break;
        }
    }
    drop(it);
    if entries.is_empty() {
        return Ok(None);
    }
    let prefix = crate::layout::doc_prefix(coll);
    let keys: Vec<Vec<u8>> = entries
        .iter()
        .filter_map(|k| crate::index::entry_id(k))
        .map(|id| {
            let mut k = prefix.clone();
            k.extend_from_slice(id);
            k
        })
        .collect();
    let m = keys.len();
    let got: Vec<anyhow::Result<()>> = futures::stream::iter(keys.into_iter().map(|k| {
        let d = Arc::clone(&d);
        async move {
            d.get(k).await?;
            Ok(())
        }
    }))
    .buffer_unordered(m.max(1))
    .collect()
    .await;
    for r in got {
        r?;
    }
    DOCS.fetch_add(m as u64, Relaxed);
    let mid = entries[entries.len() / 2].to_vec();
    let last = entries.last().unwrap().to_vec();
    Ok(Some((mid, last)))
}

fn range_empty(r: &(Bound<Vec<u8>>, Bound<Vec<u8>>)) -> bool {
    match r {
        (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => a >= b,
        _ => false,
    }
}
