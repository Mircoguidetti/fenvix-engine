//! Tenant wake lifecycle prototype.
//!
//! * A tenant wakes as a read-only `DbReader` (FollowLatest, `skip_wal_replay`, 1 h poll).
//! * The first write closes the reader and opens a writer with the in-`Db` GC and
//!   compactor off (variant V4).
//! * `tenants/{id}/_clean` records that the last writer closed cleanly. It is deleted
//!   when a writer opens and written after `Db::close()` returns. A missing marker
//!   means "unclean": the reader then replays the WAL instead of skipping it.
//! * Compaction and GC run outside the writer, in `Maintenance`.

use crate::counting::{CountingStore, Snapshot};
use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use slatedb::admin::Admin;
use slatedb::config::{
    CompactorOptions, DbReaderOptions, GarbageCollectorDirectoryOptions,
    GarbageCollectorOptions, Settings,
};
use slatedb::{CompactorBuilder, Db, DbReader, DbReaderMode, WriteHandle};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Semaphore};

/// slatedb's size-tiered scheduler compacts L0 once it holds this many SSTs
/// (`SizeTieredCompactionSchedulerOptions::min_compaction_sources` default).
pub const L0_COMPACT_THRESHOLD: usize = 4;
/// `Settings::l0_max_ssts` default is 8; a writer whose memtable flush would exceed it blocks.
/// Wait for compaction before writing when a tenant is already this close.
pub const L0_GUARD: usize = 6;
/// Tenants garbage-collected at once by the maintenance loop.
pub const GC_CONCURRENCY: usize = 16;
/// How often (milliseconds) the maintenance loop looks at the L0 of every open writer and queues a compaction for each one
/// at or above `L0_COMPACT_THRESHOLD`; the queue keeps one pass per tenant, so a tenant whose pass is queued or running is
/// not queued again. The L0 count is the writer's own view of its manifest: no request to the store.
///
/// This is what starts the compaction of an open writer. It used to be started by a write that had finished (at most once
/// every 10 s per database): once L0 reached slatedb's `l0_max_ssts` the memtable flushes stopped, every write then blocked
/// in slatedb's backpressure (`max_unflushed_bytes`), no write finished, so no compaction was ever asked for again and the
/// database stopped taking writes for good (docs/fix-stall.md). Here the request depends only on L0, not on the writes.
pub static L0_WATCH_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1000);
/// Compaction requests queued by the L0 watch (a request that joined a pass already queued is not counted).
pub static L0_WATCH_REQUESTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
const LONG_POLL: Duration = Duration::from_secs(3600);

pub fn tenant_path(id: impl std::fmt::Display) -> String {
    format!("tenants/{id}")
}

pub fn marker_path(id: impl std::fmt::Display) -> Path {
    Path::from(format!("tenants/{id}/_clean"))
}

/// Requests made by the standalone compaction and GC passes (they are also counted, by object category, in the
/// store-level counters; these totals say how many of them came from maintenance).
#[derive(Default)]
pub struct MaintTotals {
    pub compact_runs: std::sync::atomic::AtomicU64,
    pub compact_puts: std::sync::atomic::AtomicU64,
    pub compact_gets: std::sync::atomic::AtomicU64,
    pub gc_runs: std::sync::atomic::AtomicU64,
    pub gc_deletes: std::sync::atomic::AtomicU64,
    pub gc_lists: std::sync::atomic::AtomicU64,
    pub gc_gets: std::sync::atomic::AtomicU64,
}

pub static MAINT: MaintTotals = MaintTotals {
    compact_runs: std::sync::atomic::AtomicU64::new(0),
    compact_puts: std::sync::atomic::AtomicU64::new(0),
    compact_gets: std::sync::atomic::AtomicU64::new(0),
    gc_runs: std::sync::atomic::AtomicU64::new(0),
    gc_deletes: std::sync::atomic::AtomicU64::new(0),
    gc_lists: std::sync::atomic::AtomicU64::new(0),
    gc_gets: std::sync::atomic::AtomicU64::new(0),
};

/// WAL flush interval override in milliseconds (0 = slatedb's default of 100 ms).
pub static FLUSH_INTERVAL_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Acknowledge a write once it is in the memtable, before it is durable: it becomes durable at the next periodic
/// WAL flush (at most one flush interval plus one PUT later). Off by default; a crash can lose acknowledged writes.
pub static ACK_BEFORE_DURABLE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
/// Flush the WAL explicitly after every write batch (the baseline). When false a write waits for the next
/// periodic flush, which batches concurrent writes into one WAL object at the cost of up to one interval of latency.
pub static EXPLICIT_FLUSH: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

/// How often an open writer re-reads the manifest, in seconds. A writer learns of compactions only this way, and a
/// memtable flush that stalled on a full L0 (`l0_max_ssts`) retries only this often: with the old 1 h value a tenant
/// written for a few minutes filled its L0 and never recovered (the WAL piled up and `Db::close` never returned).
pub static WRITER_POLL_SECS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(30);
/// How long `Tenant::close` waits for the final flush before closing without it (the WAL is durable; the next
/// wake replays it because no clean marker is written).
pub const CLOSE_FLUSH_TIMEOUT: Duration = Duration::from_secs(20);
/// How long `Tenant::abandon` waits for slatedb's close before leaving the tenant behind (its background tasks stop on their
/// own once fenced or once the store answers). Abandons that hit it are counted in [`ABANDON_CLOSE_TIMEOUTS`].
pub const ABANDON_CLOSE_TIMEOUT: Duration = Duration::from_secs(10);
/// After its deadline a compaction pass stops its compactor in rounds of about 1 s (`compact_tenant_within`) for at most this
/// long, then gives up (the pass fails; slatedb's compactor tasks are left to their store request).
pub(crate) const COMPACTOR_STOP_LIMIT: Duration = Duration::from_secs(120);
pub static ABANDON_CLOSE_TIMEOUTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Closes of abandoned tenants still running, per tenant id ([`Tenant::abandon`] left them behind or is still waiting for them).
static PENDING_CLOSES: std::sync::LazyLock<Mutex<HashMap<String, usize>>> = std::sync::LazyLock::new(Default::default);

/// Whether a close of an abandoned tenant `id` is still running: its slatedb tasks may still write under the tenant's prefix.
pub fn close_pending(id: &str) -> bool {
    PENDING_CLOSES.lock().unwrap().get(id).is_some_and(|n| *n > 0)
}

/// Abandoned closes still running, all tenants (tenantStats).
pub fn pending_closes() -> usize {
    PENDING_CLOSES.lock().unwrap().values().sum()
}

/// `close`, counted in [`PENDING_CLOSES`] from now until it ends (or its task is dropped).
fn pending_close(id: &str, close: impl std::future::Future<Output = ()> + Send + 'static) -> impl std::future::Future<Output = ()> + Send + 'static {
    struct Pending(String);
    impl Drop for Pending {
        fn drop(&mut self) {
            let mut m = PENDING_CLOSES.lock().unwrap();
            if let Some(n) = m.get_mut(&self.0) {
                *n -= 1;
                if *n == 0 {
                    m.remove(&self.0);
                }
            }
        }
    }
    *PENDING_CLOSES.lock().unwrap().entry(id.to_string()).or_default() += 1;
    let pending = Pending(id.to_string());
    async move {
        close.await;
        drop(pending);
    }
}

/// Writers a process has open right now (garbage collection of a whole tenant is left to its close).
static OPEN_WRITERS: std::sync::LazyLock<Mutex<std::collections::HashSet<String>>> = std::sync::LazyLock::new(Default::default);
/// The `Db` of every open writer, so the WAL cleanup can read the writer's own view of the manifest (no request) instead of
/// reading the manifest from the store at every pass.
static WRITER_DBS: std::sync::LazyLock<Mutex<HashMap<String, Db>>> = std::sync::LazyLock::new(Default::default);

pub(crate) fn writer_opened(id: &str, db: &Db) {
    OPEN_WRITERS.lock().unwrap().insert(id.to_string());
    WRITER_DBS.lock().unwrap().insert(id.to_string(), db.clone());
}

pub(crate) fn writer_closed(id: &str) {
    OPEN_WRITERS.lock().unwrap().remove(id);
    WRITER_DBS.lock().unwrap().remove(id);
    UNFLUSHED_SINCE.lock().unwrap().remove(id);
    COMPACT_RETRY.lock().unwrap().remove(id);
    WRITER_SCOPE.lock().unwrap().remove(id);
}

/// Pause before the L0 watch asks again for the compaction of a tenant whose last pass failed: `COMPACT_RETRY_BASE`, doubled
/// at each failure in a row, at most `COMPACT_RETRY_MAX`. A pass that succeeds, or the close of the tenant, clears it. Without
/// it a tenant whose compaction always fails (store answering with errors, a damaged SST) would be compacted again every
/// second: every pass reads its whole L0 from the store and holds one of the process's compaction slots.
/// Only the L0 watch waits for the pause; an explicit request (close, upgrade) still queues a pass at once.
pub const COMPACT_RETRY_BASE: Duration = Duration::from_secs(1);
pub const COMPACT_RETRY_MAX: Duration = Duration::from_secs(60);
/// Open writer -> (compaction failures in a row, the L0 watch asks again only after this instant).
static COMPACT_RETRY: std::sync::LazyLock<Mutex<HashMap<String, (u32, Instant)>>> = std::sync::LazyLock::new(Default::default);
/// L0 watch rounds that left a tenant alone because it was in its pause after a failed compaction.
pub static L0_WATCH_DEFERRED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn compact_retry_pause(failures: u32) -> Duration {
    COMPACT_RETRY_BASE.saturating_mul(1u32 << failures.saturating_sub(1).min(16)).min(COMPACT_RETRY_MAX)
}

/// A compaction pass of `id` failed at `now`: start (or lengthen) its pause. Nothing is kept for a tenant that is not open
/// as a writer here (the watch never asks for it, and its close already cleared the entry).
fn compaction_failed(id: &str, now: Instant) {
    if !OPEN_WRITERS.lock().unwrap().contains(id) {
        return;
    }
    let mut r = COMPACT_RETRY.lock().unwrap();
    let e = r.entry(id.to_string()).or_insert((0, now));
    e.0 = e.0.saturating_add(1);
    e.1 = now + compact_retry_pause(e.0);
}

fn compaction_succeeded(id: &str) {
    COMPACT_RETRY.lock().unwrap().remove(id);
}

/// True while `id` is in its pause after a failed compaction.
fn compaction_deferred(id: &str, now: Instant) -> bool {
    COMPACT_RETRY.lock().unwrap().get(id).is_some_and(|(_, until)| now < *until)
}

/// Tenants written recently, for the WAL-only garbage collection of open writers (see `Maintenance`).
struct HotGc {
    last_write: Instant,
    last_gc: Instant,
    last_sst_gc: Instant,
    /// The writer's manifest version at the last SST cleanup pass (a pass is due again only after something changed).
    sst_manifest: u64,
    /// The last pass saw unreferenced objects that were not old enough yet: run again at this time.
    sst_retry_at: Option<Instant>,
}
/// Tenants a compaction of this process rewrote since their last SST cleanup pass (the compaction left replaced SSTs).
static COMPACTED: std::sync::LazyLock<Mutex<std::collections::HashSet<String>>> = std::sync::LazyLock::new(Default::default);
/// An SST cleanup pass is also due once the writer's manifest has moved this many versions since the last pass (old
/// manifests pile up: one per memtable flush, checkpoint or compaction).
pub const SST_GC_MANIFEST_STEP: u64 = 16;
/// SST cleanup passes that were due by time but skipped because nothing had changed (no request made).
pub static SST_GC_NOOP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Open writers whose own view of the manifest holds at least `L0_COMPACT_THRESHOLD` L0 SSTs (see `L0_WATCH_MS`).
/// A writer in its pause after a failed compaction is left out (see `COMPACT_RETRY_BASE`).
fn writers_over_l0_threshold() -> Vec<String> {
    let dbs: Vec<(String, Db)> = WRITER_DBS.lock().unwrap().iter().map(|(id, db)| (id.clone(), db.clone())).collect();
    let now = Instant::now();
    dbs.into_iter()
        .filter(|(_, db)| db.manifest().l0().len() >= L0_COMPACT_THRESHOLD)
        .filter(|(id, _)| {
            let deferred = compaction_deferred(id, now);
            if deferred {
                L0_WATCH_DEFERRED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            !deferred
        })
        .map(|(id, _)| id)
        .collect()
}

fn writer_manifest_id(id: &str) -> Option<u64> {
    WRITER_DBS.lock().unwrap().get(id).map(|db| db.manifest().id())
}
static GC_HOT: std::sync::LazyLock<Mutex<HashMap<String, HotGc>>> = std::sync::LazyLock::new(Default::default);
/// Seconds between WAL garbage-collection passes over an open writer that is being written (0 = never).
pub static HOT_GC_EVERY_SECS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(20);

/// Record that a writer of tenant `id` just wrote.
pub fn note_write(id: &str) {
    let now = Instant::now();
    let mut hot = GC_HOT.lock().unwrap();
    if let Some(h) = hot.get_mut(id) {
        h.last_write = now;
        return;
    }
    let m = writer_manifest_id(id).unwrap_or(0);
    hot.insert(id.to_string(), HotGc { last_write: now, last_gc: now, last_sst_gc: now, sst_manifest: m, sst_retry_at: None });
}

/// Test control: override slatedb's `l0_max_ssts` (8) for writers, 0 = keep the default. A lower value makes the
/// memtable-flush stall that the shutdown fallback handles happen after fewer writes.
pub static L0_MAX_SSTS_OVERRIDE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Test control: memtable size (bytes) at which a writer flushes it to an L0 SST, 0 = slatedb's default.
pub static L0_SST_BYTES_OVERRIDE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Minimum age of the WAL objects the garbage collector of a tenant may delete, beyond `min_age` (set by the control
/// layer: restore points need the WAL written after them for the retention window).
pub static WAL_RETAIN: std::sync::OnceLock<fn(&str) -> Duration> = std::sync::OnceLock::new();

/// Memtable size (bytes) at which a writer flushes it to an L0 SST (`--memtable-max-mb`). slatedb's default is 64 MiB per
/// database, with no limit across databases; a database written slowly kept growing its memtable for hours.
pub static MEMTABLE_MAX_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(8 << 20);
/// (A forced memtable flush every 256 writes was tried and removed: on real S3 it blocked the writes of a fast database for
/// seconds. The memory is bounded by the memtable size instead, see `MEMTABLE_MAX_BYTES`.)

/// One block and metadata cache for every database of the process (`--block-cache-mb`, `--meta-cache-mb`), instead of
/// slatedb's default of a 512 MiB block cache and a 128 MiB metadata cache per open database, without any limit across them.
static DB_CACHE: std::sync::OnceLock<Arc<dyn slatedb::db_cache::DbCache>> = std::sync::OnceLock::new();
/// Cache scope of each opened `Db`/`DbReader`: a new id per open, so entries of a database that was dropped or rewritten by
/// another process can never be served (WAL objects reuse their numbers after a drop). Old scopes age out of the cache.
static CACHE_SCOPE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

pub fn install_shared_cache(block_bytes: u64, meta_bytes: u64) {
    // slatedb's foyer cache, metered (`meter.rs`): serverStatus reports its size and lookups
    let block_m = Arc::new(crate::meter::MeteredCache::new(block_bytes));
    let meta_m = Arc::new(crate::meter::MeteredCache::new(meta_bytes));
    let _ = crate::meter::CACHES.set((Arc::clone(&block_m), Arc::clone(&meta_m)));
    let block: Arc<dyn slatedb::db_cache::DbCache> = block_m;
    let meta: Arc<dyn slatedb::db_cache::DbCache> = meta_m;
    let split = slatedb::db_cache::SplitCache::new().with_block_cache(Some(block)).with_meta_cache(Some(meta)).build();
    let _ = DB_CACHE.set(Arc::new(split));
}

/// Keep the data blocks of the L0 SSTs a writer flushes in the shared block cache (`--cache-flushed-blocks`, slatedb's
/// default). Off by default: measured (mem-growth branch), those blocks were the second largest share of the memory that
/// grew with the writes and stayed after the close, up to the whole block cache: a new cache scope per open means a
/// closed database's blocks are never read again, they only age out once the cache is full. A block is still cached
/// when it is read; the index and filter blocks of a flushed SST are still cached on flush.
pub static CACHE_FLUSHED_BLOCKS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn flush_cache_policy() -> slatedb::BlockCachePolicy {
    let p = slatedb::BlockCachePolicy::default();
    if CACHE_FLUSHED_BLOCKS.load(std::sync::atomic::Ordering::Relaxed) {
        p
    } else {
        p.with_flush_targets(&[slatedb::CacheTarget::Index, slatedb::CacheTarget::Filters])
    }
}

/// Oldest write of each open writer that may still be only in its memtable: (its sequence number, when the memtable is
/// due for a flush). Set by the first write after the memtable was last flushed; see `MEMTABLE_MAX_AGE_SECS`.
static UNFLUSHED_SINCE: std::sync::LazyLock<Mutex<HashMap<String, (u64, Instant)>>> = std::sync::LazyLock::new(Default::default);
/// A writer flushes its memtable to an L0 SST once its oldest unflushed write is this old (seconds, 0 = never;
/// `--memtable-max-age-secs`). slatedb flushes a memtable only when it is full (`--memtable-max-mb`) or after 4096 WAL
/// flushes, so a database written slowly kept every write in memory for up to ~34 minutes at 2 writes/s, and its
/// memtable was the largest share of the memory that grew with the writes (mem-growth branch). A database written fast
/// fills its memtable long before this age, so the rule never fires for it; a slow one gets at most one more L0 SST
/// (one PUT and one manifest update) per interval.
pub static MEMTABLE_MAX_AGE_SECS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(600);
/// Memtable flushes started because of `MEMTABLE_MAX_AGE_SECS`.
pub static MEMTABLE_AGE_FLUSHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn note_unflushed(id: &str, seq: u64) {
    if MEMTABLE_MAX_AGE_SECS.load(std::sync::atomic::Ordering::Relaxed) == 0 {
        return;
    }
    let mut m = UNFLUSHED_SINCE.lock().unwrap();
    if !m.contains_key(id) {
        // Due between 3/4 of the age and the age: databases opened together do not all flush at the same moment (measured:
        // 50 databases flushing at once raised the RSS by ~40 MB for a minute).
        let age = Duration::from_secs(MEMTABLE_MAX_AGE_SECS.load(std::sync::atomic::Ordering::Relaxed));
        let due = Instant::now() + age.mul_f64(0.75 + 0.25 * rand::random::<f64>());
        m.insert(id.to_string(), (seq, due));
    }
}

/// Flush the memtables whose oldest unflushed write is older than `MEMTABLE_MAX_AGE_SECS` (called every few seconds by
/// the maintenance loop). A writer whose manifest already records an L0 flush past that write is just forgotten.
fn flush_old_memtables() {
    let max = MEMTABLE_MAX_AGE_SECS.load(std::sync::atomic::Ordering::Relaxed);
    if max == 0 {
        return;
    }
    let now = Instant::now();
    let dbs = WRITER_DBS.lock().unwrap();
    let mut due = Vec::new();
    UNFLUSHED_SINCE.lock().unwrap().retain(|id, (seq, at)| {
        let Some(db) = dbs.get(id) else { return false };
        if db.manifest().last_l0_seq() >= *seq {
            return false;
        }
        if now >= *at {
            due.push((id.clone(), db.clone()));
            return false;
        }
        true
    });
    drop(dbs);
    for (id, db) in due {
        MEMTABLE_AGE_FLUSHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tokio::spawn(async move {
            if let Err(e) = db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await {
                eprintln!("memtable flush (age) of tenant {id}: {e}");
            }
        });
    }
}

/// Bloom filter bits per key of the SSTs written by writers and compactions (`--filter-bits-per-key`; slatedb's default
/// is 10). T1: with the filters on the counter, a false positive is the only reason a cold point read waits for a second
/// read in a row (about 1% per SST level at 10 bits; about 0.01% at 20).
pub static FILTER_BITS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(10);

/// The filter policies of every SST written and read: the Bloom filter (slatedb's default at 10 bits), and since step 5b
/// the seq-range filter that lets the merged reader skip SSTs whose rows are all at or below its X_d (`fx/seqfilter.rs`).
/// An SST written without the seq-range filter is read as before; a binary without it ignores the filter by name.
/// Keys an SST needs before slatedb writes its filters (writers and compaction workers alike).
pub const MIN_FILTER_KEYS: u32 = 1;

pub(crate) fn filter_policies() -> Option<Vec<Arc<dyn slatedb::FilterPolicy>>> {
    let bits = FILTER_BITS.load(std::sync::atomic::Ordering::Relaxed);
    Some(vec![Arc::new(slatedb::BloomFilterPolicy::new(bits)) as Arc<dyn slatedb::FilterPolicy>, crate::fx::seqfilter::policy(), crate::fx::seqfilter::cust_policy()])
}

/// The open writers (T1: the maps loop looks at their manifests).
pub fn open_writers() -> Vec<(String, Db)> {
    WRITER_DBS.lock().unwrap().iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// Tenants open as readers (woken, not written yet), weakly: the tenant owns its reader.
static READER_DBS: std::sync::LazyLock<Mutex<HashMap<String, std::sync::Weak<DbReader>>>> = std::sync::LazyLock::new(Default::default);

/// The tenants open as readers (T1: the maps loop looks at their manifests too).
pub fn open_readers() -> Vec<(String, Arc<DbReader>)> {
    let mut m = READER_DBS.lock().unwrap();
    m.retain(|_, w| w.strong_count() > 0);
    m.iter().filter_map(|(k, w)| w.upgrade().map(|r| (k.clone(), r))).collect()
}

/// Install the waiter's two-floor counter (T1, `counter.rs`) as the shared cache instead of `install_shared_cache`:
/// `ram_bytes` of RAM, and `disk_bytes` on `disk_dir` when given. False if a cache is installed already.
pub fn install_counter(ram_bytes: u64, disk_dir: Option<std::path::PathBuf>, disk_bytes: u64) -> std::io::Result<bool> {
    let c = crate::counter::Counter::new(ram_bytes, disk_dir, disk_bytes)?;
    if DB_CACHE.set(c.split_cache()).is_err() {
        return Ok(false);
    }
    let _ = crate::counter::COUNTER.set(c);
    Ok(true)
}

/// The shared cache with the scope `scope`, or a fresh scope.
fn cache_scope_in(scope: Option<u64>) -> Option<(Arc<dyn slatedb::db_cache::DbCache>, u64)> {
    DB_CACHE.get().map(|c| (Arc::clone(c), scope.unwrap_or_else(|| CACHE_SCOPE.fetch_add(1, std::sync::atomic::Ordering::Relaxed))))
}

/// The shared block and metadata cache, when installed (`install_shared_cache`).
pub fn shared_cache() -> Option<Arc<dyn slatedb::db_cache::DbCache>> {
    DB_CACHE.get().cloned()
}

/// Install a given cache as the shared one (tests: a cache that counts its hits). False if one is already installed.
pub fn install_cache(cache: Arc<dyn slatedb::db_cache::DbCache>) -> bool {
    DB_CACHE.set(cache).is_ok()
}

/// Cache scope of the writer last built for each tenant (`writer_builder`): the compaction warm-up puts the new SSTs'
/// blocks under it (`warm.rs`). Removed when the writer closes.
static WRITER_SCOPE: std::sync::LazyLock<Mutex<HashMap<String, u64>>> = std::sync::LazyLock::new(Default::default);

pub fn writer_scope(id: &str) -> Option<u64> {
    WRITER_SCOPE.lock().unwrap().get(id).copied()
}

/// Relaxed durability window of a database (control layer, `ctl/durability.rs`): its writer flushes the WAL on its own at
/// least this often, so writes acknowledged before they were durable (`{w: 1, j: false}`) are durable within it.
pub static RELAXED_WINDOW: std::sync::OnceLock<fn(&str) -> Duration> = std::sync::OnceLock::new();

/// Local log (prototype, `mongo-server/src/locallog.rs`): writers flush their WAL on their own only this often (seconds, 0 =
/// off); the local log's uploader flushes them explicitly on the same schedule.
pub static LOCAL_LOG_FLUSH_SECS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Flush the WAL of the open writer of `id` to object storage. Ok(false): no writer is open.
pub async fn flush_writer(id: &str) -> Result<bool> {
    let db = WRITER_DBS.lock().unwrap().get(id).cloned();
    match db {
        Some(db) => {
            // every batch staged before the flush is in object storage after it
            let group = VIS_GROUPS.lock().unwrap().get(id).cloned();
            let upto = group.as_ref().map(|g| g.staged_seq.load(std::sync::atomic::Ordering::Acquire));
            db.flush().await?;
            if let (Some(g), Some(upto)) = (group, upto) {
                g.vis.confirm_upto(upto);
            }
            Ok(true)
        }
        None => Ok(false),
    }
}

/// Flush the memtable of the open writer of `id` into an L0 SST. Ok(false): no writer is open. The agent lane does this
/// at every upload of the local log, so a takeover replays at most one upload interval of small WAL objects.
pub async fn flush_writer_memtable(id: &str) -> Result<bool> {
    let db = WRITER_DBS.lock().unwrap().get(id).cloned();
    match db {
        Some(db) => {
            db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await?;
            Ok(true)
        }
        None => Ok(false),
    }
}

pub fn relaxed_window(id: &str) -> Duration {
    RELAXED_WINDOW.get().map(|f| f(id)).unwrap_or_default()
}

pub(crate) fn writer_builder(id: &str, store: &Arc<dyn ObjectStore>) -> slatedb::DbBuilder<String> {
    writer_builder_in(id, store, None)
}

/// [`writer_builder`] in the cache scope `scope` (`None`: a fresh one). A reader upgraded to a writer passes its own scope:
/// the blocks the reader cached stay warm for the writer (same database, same immutable objects). With a fresh scope every
/// first write after a wake (the profiler's entries too) left the database with a cold cache.
pub(crate) fn writer_builder_in(id: &str, store: &Arc<dyn ObjectStore>, scope: Option<u64>) -> slatedb::DbBuilder<String> {
    let mut settings = writer_settings();
    let relaxed = relaxed_window(id);
    let local = LOCAL_LOG_FLUSH_SECS.load(std::sync::atomic::Ordering::Relaxed);
    if local > 0 {
        // Writes are durable on the local disk; object storage gets them in blocks.
        settings.flush_interval = Some(Duration::from_secs(local));
    } else if !relaxed.is_zero() && EXPLICIT_FLUSH.load(std::sync::atomic::Ordering::Relaxed) {
        // Durable writes flush explicitly; the periodic flush only has to bound the relaxed ones.
        settings.flush_interval = Some(relaxed);
    }
    let b = Db::builder(tenant_path(id), crate::counting::owned(store, id)).with_settings(settings).with_block_cache_policy(flush_cache_policy()).with_sst_block_size(crate::tiered::block_size());
    let b = match filter_policies() {
        Some(p) => b.with_filter_policies(p),
        None => b,
    };
    match cache_scope_in(scope) {
        Some((c, scope)) => {
            WRITER_SCOPE.lock().unwrap().insert(id.to_string(), scope);
            b.with_db_cache(c, scope)
        }
        None => b,
    }
}

pub fn writer_settings() -> Settings {
    let mut s = Settings::default();
    let mem = MEMTABLE_MAX_BYTES.load(std::sync::atomic::Ordering::Relaxed);
    if mem > 0 {
        s.l0_sst_size_bytes = mem as usize;
        // Writes pause once this much is waiting for object storage (slatedb's default is 1 GiB per database).
        s.max_unflushed_bytes = (mem as usize * 4).max(16 << 20);
    }
    let ms = FLUSH_INTERVAL_MS.load(std::sync::atomic::Ordering::Relaxed);
    if ms > 0 {
        s.flush_interval = Some(Duration::from_millis(ms));
    }
    let l0_max = L0_MAX_SSTS_OVERRIDE.load(std::sync::atomic::Ordering::Relaxed);
    if l0_max > 0 {
        s.l0_max_ssts = l0_max;
        // both limits (the per-key one is also 8 by default): a value above 8 lets a test see L0 pass 8 instead of stalling
        s.l0_max_ssts_per_key = l0_max;
    }
    let l0_bytes = L0_SST_BYTES_OVERRIDE.load(std::sync::atomic::Ordering::Relaxed);
    if l0_bytes > 0 {
        s.l0_sst_size_bytes = l0_bytes as usize;
    }
    // step 5b: filters on every SST, however small (slatedb's default is 1,000 keys): the seq-range filter is what lets
    // the merged reader skip an SST at or below its X_d, and an SST without filters is always read
    s.min_filter_keys = MIN_FILTER_KEYS;
    s.compression_codec = crate::tiered::codec();
    s.garbage_collector_options = None;
    s.compactor_options = None;
    s.manifest_poll_interval = Duration::from_secs(WRITER_POLL_SECS.load(std::sync::atomic::Ordering::Relaxed));
    s
}

fn reader_options(skip_wal_replay: bool) -> DbReaderOptions {
    let mut o = DbReaderOptions::default();
    o.skip_wal_replay = skip_wal_replay;
    o.manifest_poll_interval = LONG_POLL;
    o
}

/// Open a reader in the cache scope `scope` (`None`: a fresh one); also returns the scope used.
async fn open_reader(store: &Arc<dyn ObjectStore>, id: impl std::fmt::Display, skip_wal_replay: bool, scope: Option<u64>) -> Result<(DbReader, Option<u64>)> {
    let b = DbReader::builder(tenant_path(&id), crate::counting::owned(store, &id.to_string())).with_reader_mode(DbReaderMode::FollowLatest).with_options(reader_options(skip_wal_replay));
    let b = match filter_policies() {
        Some(p) => b.with_filter_policies(p),
        None => b,
    };
    let (b, used) = match cache_scope_in(scope) {
        Some((c, scope)) => (b.with_db_cache(c, scope), Some(scope)),
        None => (b, None),
    };
    Ok((b.build().await?, used))
}

// ---------------------------------------------------------------------------
// Compaction queue
// ---------------------------------------------------------------------------

/// De-duplicating queue of tenants that need compaction. A caller may attach a waiter
/// that is told the tenant's L0 count when the compaction pass it joined has finished.
pub struct CompactionQueue {
    pending: Mutex<HashMap<String, Vec<oneshot::Sender<usize>>>>,
    tx: mpsc::UnboundedSender<Option<String>>,
}

impl CompactionQueue {
    pub fn new() -> (Arc<Self>, mpsc::UnboundedReceiver<Option<String>>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Arc::new(Self { pending: Mutex::new(HashMap::new()), tx }), rx)
    }

    /// Queue a compaction pass of `id`. True: a new pass was queued; false: the request joined a pass already queued or
    /// running (that pass reads the manifest when it starts, or the L0 watch asks again once it is over).
    pub fn request(&self, id: &str, waiter: Option<oneshot::Sender<usize>>) -> bool {
        let mut p = self.pending.lock().unwrap();
        match p.entry(id.to_string()) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                if let Some(w) = waiter {
                    e.get_mut().push(w);
                }
                false
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(waiter.into_iter().collect());
                let _ = self.tx.send(Some(id.to_string()));
                true
            }
        }
    }

    /// Tell the waiters of `id` its L0 now (the pass goes on with the merges of sorted runs; later requests join it).
    fn notify(&self, id: &str, l0: usize) {
        let waiters = match self.pending.lock().unwrap().get_mut(id) {
            Some(w) if !w.is_empty() => std::mem::take(w),
            _ => return,
        };
        for w in waiters {
            let _ = w.send(l0);
        }
    }

    fn finish(&self, id: &str, l0_after: usize) {
        let waiters = self.pending.lock().unwrap().remove(id).unwrap_or_default();
        for w in waiters {
            let _ = w.send(l0_after);
        }
    }

    pub fn stop(&self) {
        let _ = self.tx.send(None);
    }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// One lock per tenant, held for the duration of a compaction or GC pass (and by anything that must
/// not race with them, such as deleting the tenant).
pub type MaintLocks = Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>;

pub fn maint_lock(locks: &MaintLocks, id: &str) -> Arc<tokio::sync::Mutex<()>> {
    Arc::clone(locks.lock().unwrap().entry(id.to_string()).or_default())
}

pub struct Lifecycle {
    pub store: Arc<dyn ObjectStore>,
    pub counting: Arc<CountingStore>,
    pub queue: Option<Arc<CompactionQueue>>,
    /// Tenants a writer touched, with the time of the last touch (drives GC).
    pub touched: Arc<Mutex<HashMap<String, Instant>>>,
    pub maint_locks: MaintLocks,
    /// Wait for compaction before upgrading a tenant that is near the L0 limit.
    pub l0_guard: bool,
    pub stats: Arc<Mutex<LifecycleStats>>,
}

#[derive(Default, Debug, Clone)]
pub struct LifecycleStats {
    pub guard_waits: u64,
    pub guard_wait_secs: Vec<f64>,
    pub unclean_wakes: u64,
}

impl Lifecycle {
    /// Open a writer on a tenant that has no database yet (the first write to a new tenant).
    /// There is no marker to delete: a tenant without a database has never been written.
    pub async fn create(self: &Arc<Self>, id: impl std::fmt::Display) -> Result<Tenant> {
        let id = id.to_string();
        let db = writer_builder(&id, &self.store).build().await?;
        let epoch = opened_epoch(&self.store, &id).await?;
        self.touched.lock().unwrap().insert(id.clone(), Instant::now());
        writer_opened(&id, &db);
        let scope = writer_scope(&id);
        let mut t = Tenant { group: Default::default(), lc: Arc::clone(self), id, mode: Some(Mode::Writer(db)), unclean: false, l0_at_wake: 0, scope, env: None, fx: None, holds: Default::default(), fx_loaded: Instant::now(), fx_writes: 0, fx_builds: 0, fx_announced: false, fx_announce_seen: None, fx_fence_pending: false, fx_unknown: None, fx_shared: None, serving_until: Default::default(), epoch: epoch };
        t.holds = crate::fx::hold::for_db(&t.id); // shared by every opening of the database in this process
        t.load_env().await?;
        t.load_fx().await?;
        Ok(t)
    }

    pub fn new(counting: Arc<CountingStore>, queue: Option<Arc<CompactionQueue>>) -> Arc<Self> {
        Self::with_guard(counting, queue, true)
    }

    pub fn with_guard(counting: Arc<CountingStore>, queue: Option<Arc<CompactionQueue>>, l0_guard: bool) -> Arc<Self> {
        Arc::new(Self {
            store: counting.clone(),
            counting,
            queue,
            touched: Arc::new(Mutex::new(HashMap::new())),
            maint_locks: Arc::new(Mutex::new(HashMap::new())),
            l0_guard,
            stats: Arc::new(Mutex::new(LifecycleStats::default())),
        })
    }

    /// Wake a tenant read-only. The clean-marker HEAD runs in parallel with a
    /// speculative `skip_wal_replay` reader open; nothing is served before both finish.
    pub async fn wake(self: &Arc<Self>, id: impl std::fmt::Display) -> Result<Tenant> {
        let id = id.to_string();
        // Opening the database is background work: its reads are not charged to the command that needed it (meter.rs).
        crate::meter::background(&id.clone(), self.wake_inner(id)).await
    }

    async fn wake_inner(self: &Arc<Self>, id: String) -> Result<Tenant> {
        let marker = marker_path(&id);
        let (reader, head) = tokio::join!(open_reader(&self.store, &id, true, None), self.store.head(&marker));
        let (mut reader, scope) = reader?;
        let clean = match head {
            Ok(_) => true,
            Err(object_store::Error::NotFound { .. }) => false,
            Err(e) => return Err(e.into()),
        };
        if !clean {
            // Possible unflushed WAL: the speculative reader may be missing data. Reopen with replay.
            reader.close().await?;
            reader = open_reader(&self.store, &id, false, scope).await?.0;
            self.stats.lock().unwrap().unclean_wakes += 1;
        }
        let l0_at_wake = reader.manifest().l0().len();
        let reader = Arc::new(reader);
        READER_DBS.lock().unwrap().insert(id.clone(), Arc::downgrade(&reader));
        let mut t = Tenant { group: Default::default(), lc: Arc::clone(self), id, mode: Some(Mode::Reader(reader)), unclean: !clean, l0_at_wake, scope, env: None, fx: None, holds: Default::default(), fx_loaded: Instant::now(), fx_writes: 0, fx_builds: 0, fx_announced: false, fx_announce_seen: None, fx_fence_pending: false, fx_unknown: None, fx_shared: None, serving_until: Default::default(), epoch: 0 };
        t.holds = crate::fx::hold::for_db(&t.id); // shared by every opening of the database in this process
        t.load_env().await?;
        t.load_fx().await?;
        Ok(t)
    }
}

/// A write for [`Tenant::apply`].
pub enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

enum Mode {
    /// Shared with the maps loop (T1) through `READER_DBS` (a weak handle: the tenant owns it).
    Reader(Arc<DbReader>),
    Writer(Db),
}

/// Read-your-writes without dirty reads on the log path (`--local-log-dir` / `--quaderno`, set by the server; off
/// otherwise, where reads use slatedb's durable filter as before).
///
/// With the log a write is durable when its log record is confirmed (on the local disk, or on 2 of the 3 copies), long
/// before its WAL object reaches object storage; an agent-lane batch is durable when its WAL object is. Readers must see
/// every durable write (a writer reads its own acknowledged writes) and nothing that is not durable yet (a write staged in
/// the memtable while its record or its WAL object is still on its way can be lost by a crash: a majority read or a
/// change stream must never return it).
///
/// The rule, per key. Every batch is staged under the database's exclusive lock; with tracking on, a snapshot of the state
/// just before the batch is taken at the same moment (a slatedb snapshot is a sequence number, no copy) and the batch is
/// "in flight" with the keys it writes. A read of key `k` returns the value of the snapshot before the oldest in-flight
/// batch that writes `k`, or the latest value when none does: either way the value of a batch that is durable, and every
/// earlier write of `k` is durable too. A batch leaves (becomes visible) once it is durable and no earlier in-flight batch
/// writes one of its keys; its acknowledgement and its change-stream events wait for that, so a writer never gets an ok
/// for a write a read could still miss. Batches on other keys do not wait for each other: a default-path write is not
/// held back by an agent-lane batch waiting for its PUT on other documents. (The log's marker key, written by every log
/// batch, is not counted: only the log's replay reads it.)
pub static TRACK_VISIBLE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
/// A key every log batch writes and no read uses (the log's marker, set by the server): not counted as a batch's key.
pub static VIS_IGNORE_KEY: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
/// Called whenever a database's visible state moves forward (the server wakes change-stream readers with it).
pub static ON_VISIBLE: std::sync::OnceLock<Box<dyn Fn() + Send + Sync>> = std::sync::OnceLock::new();
/// Batches whose log record was refused or not confirmed in time, then made durable by a WAL flush to object storage.
pub static VISIBLE_FALLBACK_FLUSHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Key reads served from the snapshot before an in-flight batch (a durable value older than the latest one).
pub static VISIBLE_SNAPSHOT_READS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct VisBatch {
    before: Arc<slatedb::DbSnapshot>,
    keys: Vec<Vec<u8>>,
    durable: bool,
}

#[derive(Default)]
struct VisInner {
    /// In-flight batches by sequence number.
    batches: std::collections::BTreeMap<u64, VisBatch>,
    /// Key -> in-flight batches that write it.
    by_key: std::collections::BTreeMap<Vec<u8>, std::collections::BTreeSet<u64>>,
    /// Every batch up to this sequence number is in object storage.
    durable_upto: u64,
    /// Ownership was lost: the in-flight batches will never be confirmed by this writer.
    dead: bool,
}

impl VisInner {
    /// Remove every batch of `cands` (and the batches they unblock) that is durable and first in line on all its keys.
    /// Returns whether any left.
    fn settle(&mut self, cands: impl IntoIterator<Item = u64>) -> bool {
        let mut work: std::collections::BTreeSet<u64> = cands.into_iter().collect();
        let mut moved = false;
        while let Some(seq) = work.pop_first() {
            let ready = match self.batches.get(&seq) {
                Some(b) => b.durable && b.keys.iter().all(|k| self.by_key.get(k).and_then(|s| s.first()) == Some(&seq)),
                None => false,
            };
            if !ready {
                continue;
            }
            let b = self.batches.remove(&seq).unwrap();
            moved = true;
            for k in b.keys {
                if let Some(set) = self.by_key.get_mut(&k) {
                    set.remove(&seq);
                    match set.first() {
                        Some(next) => {
                            work.insert(*next);
                        }
                        None => {
                            self.by_key.remove(&k);
                        }
                    }
                }
            }
        }
        moved
    }
}

/// The durable (visible) state of one opening of a database as a writer. See [`TRACK_VISIBLE`].
#[derive(Default)]
pub struct Visible {
    inner: Mutex<VisInner>,
    changed: tokio::sync::Notify,
}

/// Where a batch stands for readers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisState {
    Visible,
    Pending,
    /// Never confirmed and never will be by this writer (ownership lost): its outcome is unknown.
    Lost,
}

impl Visible {
    fn moved(&self) {
        self.changed.notify_waiters();
        if let Some(f) = ON_VISIBLE.get() {
            f();
        }
    }

    fn add(&self, seq: u64, before: Arc<slatedb::DbSnapshot>, mut keys: Vec<Vec<u8>>) {
        if let Some(ignore) = VIS_IGNORE_KEY.get() {
            keys.retain(|k| k != ignore);
        }
        keys.sort();
        keys.dedup();
        let moved = {
            let mut g = self.inner.lock().unwrap();
            for k in &keys {
                g.by_key.entry(k.clone()).or_default().insert(seq);
            }
            let durable = seq <= g.durable_upto;
            g.batches.insert(seq, VisBatch { before, keys, durable });
            durable && g.settle([seq])
        };
        if moved {
            self.moved();
        }
    }

    /// The record of batch `seq` is confirmed by the log.
    pub fn confirm(&self, seq: u64) {
        let moved = {
            let mut g = self.inner.lock().unwrap();
            match g.batches.get_mut(&seq) {
                Some(b) if !b.durable => {
                    b.durable = true;
                    g.settle([seq])
                }
                _ => false,
            }
        };
        if moved {
            self.moved();
        }
    }

    /// Every batch up to `seq` is durable in object storage.
    pub fn confirm_upto(&self, seq: u64) {
        let moved = {
            let mut g = self.inner.lock().unwrap();
            g.durable_upto = g.durable_upto.max(seq);
            let mut cands = Vec::new();
            for (s, b) in g.batches.range_mut(..=seq) {
                b.durable = true;
                cands.push(*s);
            }
            g.settle(cands)
        };
        if moved {
            self.moved();
        }
    }

    /// Everything staged is durable (a clean close flushed it).
    pub fn confirm_all(&self) {
        self.confirm_upto(u64::MAX - 1);
    }

    /// Ownership was lost: nothing in flight will be confirmed here.
    pub fn kill(&self) {
        self.inner.lock().unwrap().dead = true;
        self.moved();
    }

    /// Whether any batch is in flight (reads may then need an older value for some keys).
    pub fn any(&self) -> bool {
        !self.inner.lock().unwrap().batches.is_empty()
    }

    /// The snapshot a read of `key` must use, `None` when its latest value is visible.
    pub fn snapshot_for(&self, key: &[u8]) -> Option<Arc<slatedb::DbSnapshot>> {
        let g = self.inner.lock().unwrap();
        let seq = g.by_key.get(key)?.first()?;
        g.batches.get(seq).map(|b| Arc::clone(&b.before))
    }

    /// Keys in `[start, end)` written by an in-flight batch, in key order, each with the snapshot its read must use.
    pub fn in_range(&self, start: &[u8], end: Option<&[u8]>) -> Vec<(Vec<u8>, Arc<slatedb::DbSnapshot>)> {
        let g = self.inner.lock().unwrap();
        if g.batches.is_empty() {
            return Vec::new();
        }
        let range = (std::ops::Bound::Included(start.to_vec()), end.map_or(std::ops::Bound::Unbounded, |e| std::ops::Bound::Excluded(e.to_vec())));
        g.by_key
            .range::<Vec<u8>, _>(range)
            .filter_map(|(k, set)| set.first().and_then(|s| g.batches.get(s)).map(|b| (k.clone(), Arc::clone(&b.before))))
            .collect()
    }

    pub fn state(&self, seq: u64) -> VisState {
        let g = self.inner.lock().unwrap();
        if !g.batches.contains_key(&seq) {
            VisState::Visible
        } else if g.dead {
            VisState::Lost
        } else {
            VisState::Pending
        }
    }

    async fn wait_until(&self, done: impl Fn(&VisInner) -> bool, limit: Duration, what: &str) -> Result<()> {
        let deadline = tokio::time::Instant::now() + limit;
        loop {
            let n = self.changed.notified();
            tokio::pin!(n);
            n.as_mut().enable();
            {
                let g = self.inner.lock().unwrap();
                if done(&g) {
                    return Ok(());
                }
                if g.dead {
                    return Err(anyhow!("ownership of the database was lost before {what} was confirmed; the outcome is unknown, retry"));
                }
            }
            if tokio::time::timeout_at(deadline, n).await.is_err() {
                return Err(anyhow!("{what} of the database was not confirmed in {} s; the outcome is unknown, retry", limit.as_secs()));
            }
        }
    }

    /// Wait until batch `seq` is visible: durable, and no earlier in-flight batch writes one of its keys (at most `limit`).
    pub async fn wait_visible(&self, seq: u64, limit: Duration) -> Result<()> {
        self.wait_until(|g| !g.batches.contains_key(&seq), limit, "a write (or an earlier write of the same keys)").await
    }

    /// Wait until nothing is in flight (at most `limit`).
    pub async fn wait_all(&self, limit: Duration) -> Result<()> {
        self.wait_until(|g| g.batches.is_empty(), limit, "every staged write").await
    }
}

/// A change-stream event waits for the batch that produced it: (the database's visible state, its sequence number).
#[derive(Clone)]
pub struct VisGate(Arc<GroupFlush>, u64);

impl VisGate {
    pub fn state(&self) -> VisState {
        self.0.vis.state(self.1)
    }
}

impl std::fmt::Debug for VisGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VisGate({})", self.1)
    }
}

/// The open writer of each database (its group-commit state) while tracking is on.
static VIS_GROUPS: std::sync::LazyLock<Mutex<HashMap<String, Arc<GroupFlush>>>> = std::sync::LazyLock::new(Default::default);

/// The gate for an event of `db` published now: it waits for the last batch staged in `db` (None: nothing to wait for).
pub fn visibility_gate(db: &str) -> Option<VisGate> {
    if !TRACK_VISIBLE.load(std::sync::atomic::Ordering::Relaxed) {
        return None;
    }
    let g = VIS_GROUPS.lock().unwrap().get(db).cloned()?;
    let seq = g.staged_seq.load(std::sync::atomic::Ordering::Acquire);
    if g.vis.state(seq) == VisState::Visible {
        return None;
    }
    Some(VisGate(g, seq))
}

/// The smallest key above every key starting with `p` (None: no such key).
fn prefix_end(p: &[u8]) -> Option<Vec<u8>> {
    let mut e = p.to_vec();
    while let Some(last) = e.pop() {
        if last < 0xff {
            e.push(last + 1);
            return Some(e);
        }
    }
    None
}

fn forget_vis(id: &str, group: &Arc<GroupFlush>) {
    let mut m = VIS_GROUPS.lock().unwrap();
    if m.get(id).map_or(false, |g| Arc::ptr_eq(g, group)) {
        m.remove(id);
    }
}

/// How long an acknowledgement waits for earlier batches of its database to be confirmed (the log's own deadline is 10 s,
/// then the fallback flush to object storage).
pub const VISIBLE_WAIT: Duration = Duration::from_secs(60);

/// Per-writer state of group commit: one flush at a time, led by whichever waiting write gets the lock first; a write
/// whose sequence number an earlier flush already covered just returns.
#[derive(Default)]
pub struct GroupFlush {
    lock: tokio::sync::Mutex<()>,
    /// Highest sequence number staged into the WAL buffer.
    staged_seq: std::sync::atomic::AtomicU64,
    /// Highest sequence number known to be in a durable WAL object.
    flushed_seq: std::sync::atomic::AtomicU64,
    /// Writes of this tenant between asking for the tenant lock and their reply (owned by the server's tenant entry).
    inflight: std::sync::OnceLock<Arc<std::sync::atomic::AtomicU64>>,
    /// Writes currently inside `Commit::wait` (staged, waiting for a flush).
    waiting: std::sync::atomic::AtomicU64,
    /// Adaptive group commit: when the last write arrived, the average gap between writes and the average flush time
    /// (microseconds, exponential moving averages).
    last_arrival_us: std::sync::atomic::AtomicU64,
    gap_ewma_us: std::sync::atomic::AtomicU64,
    flush_ewma_us: std::sync::atomic::AtomicU64,
    /// Agent lane: when the current batch window opened (microseconds of [`mono_us`], 0 = no window open).
    agent_open_us: std::sync::atomic::AtomicU64,
    /// The confirmed state readers see (see [`TRACK_VISIBLE`]).
    pub vis: Visible,
}

/// Agent lane (PROTOTYPE, off unless a collection is on the lane, see the server's `agentlane.rs`): a batch of the lane is
/// acknowledged only once the WAL object holding it is in object storage. The first write of a batch opens a window of
/// this many microseconds; at its end one WAL flush (one PUT) covers every write staged meanwhile, of both lanes.
pub static AGENT_WINDOW_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(100_000);
/// Agent-lane writes waited for, WAL flushes issued for them, of which flushes at a transaction's commit (no window).
pub static AGENT_COMMITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static AGENT_FLUSHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static AGENT_TXN_FLUSHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Agent-lane writes not acknowledged: the WAL write failed (fenced by another writer, store error) or ownership was lost.
pub static AGENT_ACK_REFUSED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// TEST ONLY (negative control, `MONGO_TEST_AGENT_ACK_EARLY`): acknowledge agent-lane writes without waiting for the PUT.
pub static AGENT_ACK_EARLY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Adaptive group commit (`--adaptive-group-commit`): the leader of a flush waits a short window, only when more writes of
/// the same database are expected within it (their average gap is at most twice the window) or already on their way.
/// The window is half the database's average flush time, at most `ADAPTIVE_MAX_US`: short next to the flush itself, so
/// it costs little latency, and zero for a database that is written rarely (a lone write never waits).
pub static ADAPTIVE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
pub static ADAPTIVE_MAX_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(5_000);
/// Flushes for which the adaptive leader waited, and the microseconds waited in total.
pub static ADAPTIVE_WAITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static ADAPTIVE_WAIT_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Writes acknowledged before they were durable (relaxed durability).
pub static RELAXED_ACKS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn mono_us() -> u64 {
    static T0: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    T0.get_or_init(Instant::now).elapsed().as_micros() as u64
}

fn ewma(cell: &std::sync::atomic::AtomicU64, sample: u64) {
    use std::sync::atomic::Ordering::Relaxed;
    let old = cell.load(Relaxed);
    cell.store(if old == 0 { sample } else { (old * 7 + sample) / 8 }, Relaxed);
}

/// With a window set, wait for it only while another write of the tenant is still on its way to the flush (in flight but
/// not yet waiting); when none is, flush at once. False: always wait the full window.
pub static GROUP_ADAPTIVE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Microseconds the leader of a group waits before flushing so that more writes can join (0 = flush at once).
pub static GROUP_WINDOW_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Writes waited for (`commits`) and flushes issued for them (`flushes`): writes per WAL object = commits / flushes.
pub static GROUP_COMMITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static GROUP_FLUSHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A staged write batch waiting to become durable. A clone waits for the same batch: a command that answers from a
/// record written by an earlier command (a repeated commit, a retried write) waits on a clone of that command's commit,
/// so it never acknowledges data that is not durable yet (C8).
#[derive(Clone)]
pub struct Commit {
    db: Db,
    handle: WriteHandle,
    group: Arc<GroupFlush>,
    tenant: String,
    /// Local log (prototype): the batch is durable once this resolves (its record is on the local disk).
    local: Option<futures::future::Shared<futures::future::BoxFuture<'static, std::result::Result<(), String>>>>,
    /// Agent lane: `Some(at_commit)` when the batch is durable only in a WAL object in object storage, never in the local
    /// or replicated log. `at_commit` (a transaction's commit) flushes at once instead of waiting for the batch window.
    agent: Option<bool>,
}

impl Commit {
    /// Make the batch's durability the local log's fsync instead of the WAL flush to object storage.
    pub fn with_local(mut self, rx: oneshot::Receiver<std::result::Result<(), String>>) -> Self {
        use futures::FutureExt;
        let fut = rx.map(|r| r.unwrap_or_else(|_| Err("local log stopped".to_string()))).boxed().shared();
        if TRACK_VISIBLE.load(std::sync::atomic::Ordering::Relaxed) {
            // The batch becomes visible when its record is confirmed, whether or not anyone waits for it: a command that
            // staged several batches waits only for its last one, and the earlier ones must not stay in flight.
            let (fut2, group, db, handle, seq, tenant) = (fut.clone(), Arc::clone(&self.group), self.db.clone(), self.handle.clone(), self.handle.seqnum(), self.tenant.clone());
            tokio::spawn(async move {
                match fut2.await {
                    Ok(()) => group.vis.confirm(seq),
                    Err(e) => {
                        if let Err(f) = Commit::fallback_flush(&group, &db, &handle, seq, &tenant, &e).await {
                            eprintln!("log: {f:#}");
                        }
                    }
                }
            });
        }
        self.local = Some(fut);
        self
    }

    /// The log refused the record of batch `seq` or did not confirm it in time, but the batch is in the memtable: it would
    /// stay in flight (invisible) and hold back later writes of its keys. Make it durable in object storage instead (one
    /// WAL flush, fenced like any WAL write); then it counts as confirmed. If that fails too, the write is not acknowledged.
    async fn fallback_flush(group: &Arc<GroupFlush>, db: &Db, handle: &WriteHandle, seq: u64, tenant: &str, e: &str) -> Result<()> {
        use std::sync::atomic::Ordering::{Acquire, Relaxed};
        eprintln!("log: record of {tenant} not confirmed ({e}); flushing its WAL to object storage instead");
        let wal = slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::Wal };
        let target = group.staged_seq.load(Acquire);
        db.flush_with_options(wal).await.map_err(|f| anyhow!("{e}; the WAL flush to object storage failed too: {f}"))?;
        handle.await_durable().await?;
        VISIBLE_FALLBACK_FLUSHES.fetch_add(1, Relaxed);
        group.vis.confirm_upto(target.max(seq));
        Ok(())
    }

    /// Put the batch on the agent lane: durable once its WAL object is in object storage (see [`AGENT_WINDOW_US`]).
    pub fn with_agent(mut self, at_commit: bool) -> Self {
        self.local = None;
        self.agent = Some(at_commit);
        self
    }

    /// Whether the batch is on the agent lane.
    pub fn is_agent(&self) -> bool {
        self.agent.is_some()
    }

    /// Agent lane: wait for the batch window (not at a transaction's commit), flush the WAL once for every write staged
    /// so far, then wait until the WAL object holding this batch is durable. The WAL objects are numbered and written with
    /// create-if-absent (S3 `If-None-Match: *`, slatedb's WAL fencing): a writer fenced by a newer one gets an error from
    /// its next WAL write and never acknowledges, and the newer writer replays every WAL object before its fence.
    async fn wait_agent(self, at_commit: bool) -> Result<()> {
        use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
        GROUP_COMMITS.fetch_add(1, Relaxed);
        AGENT_COMMITS.fetch_add(1, Relaxed);
        if AGENT_ACK_EARLY.load(Relaxed) {
            return Ok(());
        }
        let seq = self.handle.seqnum();
        let r: Result<()> = async {
            if self.group.flushed_seq.load(Acquire) < seq {
                if !at_commit {
                    let now = mono_us();
                    let opened = match self.group.agent_open_us.compare_exchange(0, now, AcqRel, Acquire) {
                        Ok(_) => now,
                        Err(t) => t,
                    };
                    let left = (opened + AGENT_WINDOW_US.load(Relaxed)).saturating_sub(now);
                    if left > 0 {
                        tokio::time::sleep(Duration::from_micros(left)).await;
                    }
                }
                let _lead = self.group.lock.lock().await;
                if self.group.flushed_seq.load(Acquire) < seq {
                    // writes that arrive from now on open the next window
                    self.group.agent_open_us.store(0, Release);
                    let target = self.group.staged_seq.load(Acquire);
                    let wal = slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::Wal };
                    self.db.flush_with_options(wal).await?;
                    AGENT_FLUSHES.fetch_add(1, Relaxed);
                    if at_commit {
                        AGENT_TXN_FLUSHES.fetch_add(1, Relaxed);
                    }
                    GROUP_FLUSHES.fetch_add(1, Relaxed);
                    self.group.flushed_seq.fetch_max(target, Release);
                }
            }
            self.handle.await_durable().await?;
            // the WAL object holds this batch and every earlier one: all of them are durable
            self.group.vis.confirm_upto(seq);
            if TRACK_VISIBLE.load(Relaxed) {
                self.group.vis.wait_visible(seq, VISIBLE_WAIT).await?;
            }
            Ok(())
        }
        .await;
        if r.is_err() {
            AGENT_ACK_REFUSED.fetch_add(1, Relaxed);
            return r;
        }
        if !owner_valid(&self.tenant) {
            AGENT_ACK_REFUSED.fetch_add(1, Relaxed);
            return Err(anyhow!("ownership of database {} was lost before the write was acknowledged; its outcome is unknown, retry", self.tenant));
        }
        Ok(())
    }

    /// Sequence number of the batch in its database's writer.
    pub fn seqnum(&self) -> u64 {
        self.handle.seqnum()
    }

    /// The database the batch was written to.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Acknowledge without waiting for durability (relaxed durability, see `ctl/durability.rs`): the batch is in the
    /// memtable and the WAL buffer, and the writer's periodic flush (the database's relaxed window) makes it durable.
    pub fn relax(self) {
        use std::sync::atomic::Ordering::Relaxed;
        GROUP_COMMITS.fetch_add(1, Relaxed);
        RELAXED_ACKS.fetch_add(1, Relaxed);
    }

    /// Wait until the batch is in a durable WAL object. Writes staged while a flush is in flight are flushed
    /// together by the next one.
    pub async fn wait(mut self) -> Result<()> {
        use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
        if let Some(at_commit) = self.agent {
            return self.wait_agent(at_commit).await;
        }
        if let Some(rx) = self.local.take() {
            GROUP_COMMITS.fetch_add(1, Relaxed);
            let seq = self.handle.seqnum();
            match rx.await {
                Ok(()) => self.group.vis.confirm(seq),
                Err(e) if TRACK_VISIBLE.load(Relaxed) => Commit::fallback_flush(&self.group, &self.db, &self.handle, seq, &self.tenant, &e).await?,
                Err(e) => return Err(anyhow!(e)),
            }
            if TRACK_VISIBLE.load(Relaxed) {
                // acknowledged only once every earlier batch is confirmed too: a read right after the ok sees the write
                self.group.vis.wait_visible(seq, VISIBLE_WAIT).await?;
            }
            // The record is on the local disk, but this process may have lost the database meanwhile (frozen between the
            // ownership check and the fsync, its lease lapsed, another process took over): then the write is not acknowledged.
            if !owner_valid(&self.tenant) {
                LOCAL_ACK_REFUSED.fetch_add(1, Relaxed);
                return Err(anyhow!("ownership of database {} was lost before the write was acknowledged; its outcome is unknown, retry", self.tenant));
            }
            return Ok(());
        }
        struct Waiting<'a>(&'a std::sync::atomic::AtomicU64);
        impl Drop for Waiting<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, std::sync::atomic::Ordering::Release);
            }
        }
        self.group.waiting.fetch_add(1, Release);
        let group = Arc::clone(&self.group);
        let _waiting = Waiting(&group.waiting);
        GROUP_COMMITS.fetch_add(1, Relaxed);
        let seq = self.handle.seqnum();
        let adaptive = ADAPTIVE.load(Relaxed);
        if adaptive {
            let now = mono_us();
            let last = self.group.last_arrival_us.swap(now, Relaxed);
            if last > 0 {
                ewma(&self.group.gap_ewma_us, now.saturating_sub(last).min(10_000_000));
            }
        }
        // Flush now: waiting for the periodic flush (100 ms) would add up to that much to every write.
        if EXPLICIT_FLUSH.load(Relaxed) && self.group.flushed_seq.load(Acquire) < seq {
            let _lead = self.group.lock.lock().await;
            if self.group.flushed_seq.load(Acquire) < seq {
                let window = Duration::from_micros(GROUP_WINDOW_US.load(Relaxed));
                if adaptive && window.is_zero() {
                    let flush = self.group.flush_ewma_us.load(Relaxed);
                    let gap = self.group.gap_ewma_us.load(Relaxed);
                    let w = (flush / 2).min(ADAPTIVE_MAX_US.load(Relaxed));
                    let others = |g: &GroupFlush| g.inflight.get().map_or(0, |c| c.load(Acquire)).saturating_sub(g.waiting.load(Acquire));
                    if w > 0 && (gap > 0 && gap <= 2 * w || others(&self.group) > 0) {
                        let t = Instant::now();
                        let deadline = t + Duration::from_micros(w);
                        tokio::time::sleep_until(deadline.into()).await;
                        ADAPTIVE_WAITS.fetch_add(1, Relaxed);
                        ADAPTIVE_WAIT_US.fetch_add(t.elapsed().as_micros() as u64, Relaxed);
                    }
                }
                if !window.is_zero() {
                    if GROUP_ADAPTIVE.load(Relaxed) {
                        let deadline = Instant::now() + window;
                        loop {
                            let inflight = self.group.inflight.get().map_or(0, |c| c.load(Acquire));
                            let others = inflight.saturating_sub(self.group.waiting.load(Acquire));
                            let left = deadline.saturating_duration_since(Instant::now());
                            if others == 0 || left.is_zero() {
                                break;
                            }
                            tokio::time::sleep(left.min(Duration::from_micros(100))).await;
                        }
                    } else {
                        tokio::time::sleep(window).await;
                    }
                }
                let target = self.group.staged_seq.load(Acquire);
                let t = Instant::now();
                self.db.flush().await?;
                if adaptive {
                    ewma(&self.group.flush_ewma_us, t.elapsed().as_micros() as u64);
                }
                GROUP_FLUSHES.fetch_add(1, Relaxed);
                self.group.flushed_seq.fetch_max(target, Release);
            }
        }
        if !ACK_BEFORE_DURABLE.load(Relaxed) {
            self.handle.await_durable().await?;
            // in a durable WAL object, with every earlier batch
            self.group.vis.confirm_upto(seq);
            if TRACK_VISIBLE.load(Relaxed) {
                self.group.vis.wait_visible(seq, VISIBLE_WAIT).await?;
            }
        }
        Ok(())
    }
}

pub struct Tenant {
    group: Arc<GroupFlush>,
    lc: Arc<Lifecycle>,
    pub id: String,
    mode: Option<Mode>,
    /// The wake took the unclean path (marker missing).
    pub unclean: bool,
    pub l0_at_wake: usize,
    /// Cache scope of the reader or writer (kept by the upgrade to a writer, see [`writer_builder_in`]).
    scope: Option<u64>,
    /// E_env, the sequence number of the envelope marker (`envelope.rs`); `None`: no marker, every row is plain.
    env: Option<u64>,
    /// Compaction 2 (`fx`): the version of our manifest this tenant reads with (None: no version, or `--sealed-runs` off).
    /// It changes only through `&mut self` (open, publish, refresh), so under the database's exclusive lock, and every
    /// operation, which runs under the shared or the exclusive lock, reads one version from its first step to its last.
    fx: Option<Arc<crate::fx::view::View>>,
    /// Holds on versions taken by this tenant's reads and transactions (`fx/hold.rs`).
    holds: Arc<crate::fx::hold::Holds>,
    /// When the version was last loaded from the store (a reader's T_pin, rule 6).
    fx_loaded: Instant,
    /// Write batches since the last publish, and publishes made (`--sealed-runs N`).
    fx_writes: u64,
    fx_builds: u64,
    /// Release E1 was announced in the store (`fx/manifest.rs`, rule 26): while this tenant knows no E_env, a reader reads
    /// the marker before decoding each step's rows.
    fx_announced: bool,
    /// When this process first saw the announcement (the writer starts envelopes one T_pin later, rule 26).
    fx_announce_seen: Option<Instant>,
    /// Rule 25 (review of step 3, defect 2): a publish or a fence whose outcome is unknown (an error, a timeout) left a
    /// version in the store this tenant may not know. Until a fence succeeds, no read takes a hold (it fails with the
    /// retryable error, and the next exclusive step retries the fence) and no publish starts.
    fx_fence_pending: bool,
    /// The version of a publish whose outcome is unknown, until a fence covers its slot. When the store is still empty
    /// (a first publish), the fence is this version itself, created at its own slot (review round 2 of step 3).
    fx_unknown: Option<Box<crate::fx::manifest::Version>>,
    /// Step 4: the state shared with this database's compactor (`fx/compactor.rs`), when one runs.
    fx_shared: Option<Arc<crate::fx::compactor::Shared>>,
    /// Step 4, F7: the end of this process's serving lease of the database, as the server last renewed it. Past it the
    /// tenant begins no hold (no new read, no new transaction): a waiter that was cut off rather than dead must not
    /// start reads after the new waiter's drop window opened (RULES-FOR-THE-CODE rules 27, 28 with F7).
    serving_until: std::sync::Mutex<Option<Instant>>,
    /// With the log on: the writer epoch this writer fenced with, read from object storage when it opened (0: not read).
    epoch: u64,
}

/// The writer epoch a writer just opened on `id` fenced with, read from the stored manifest (with the log on only: the log
/// stamps every record with it, see `writer_epoch`). 0 when the log is off.
async fn opened_epoch(store: &Arc<dyn ObjectStore>, id: &str) -> Result<u64> {
    if LOCAL_LOG_FLUSH_SECS.load(std::sync::atomic::Ordering::Relaxed) == 0 {
        return Ok(0);
    }
    Ok(stored_writer_epoch(store, id).await?.unwrap_or(0))
}

impl Tenant {
    /// Every staged write is in a durable WAL object (explicit flush mode; otherwise unknown here, so false). A read of
    /// durable data then sees exactly what a read of the memtable sees.
    pub fn all_durable(&self) -> bool {
        use std::sync::atomic::Ordering::Acquire;
        EXPLICIT_FLUSH.load(std::sync::atomic::Ordering::Relaxed) && self.group.flushed_seq.load(Acquire) >= self.group.staged_seq.load(Acquire)
    }

    /// slatedb writer epoch of this writer (None for a reader).
    ///
    /// With the log on, the epoch read from object storage when the writer opened: `Db::manifest()` is the writer's cached
    /// view of the manifest, which can still show the previous writer's epoch after this one fenced (MEASURED: most
    /// records of a writer opened at epoch 3 were stamped 2), and a restart then took that writer's acknowledged records
    /// for an older writer's ("opened by another writer after these records": not applied).
    pub fn writer_epoch(&self) -> Option<u64> {
        match &self.mode {
            Some(Mode::Writer(db)) => Some(if self.epoch > 0 { self.epoch } else { db.manifest().writer_epoch() }),
            _ => None,
        }
    }

    /// L0 SSTs of the writer's local manifest view (None for a reader).
    pub fn writer_l0(&self) -> Option<usize> {
        match &self.mode {
            Some(Mode::Writer(db)) => Some(db.manifest().l0().len()),
            _ => None,
        }
    }

    /// Step 5 (cold reads): the ids of the writer's L0 SSTs, as SlateDB prints them (the store paths of these SSTs contain the
    /// ULID), so a meter can tell an L0 GET from a GET in a sorted run. Empty for a reader.
    pub fn writer_l0_ids(&self) -> Vec<String> {
        match &self.mode {
            Some(Mode::Writer(db)) => db.manifest().l0().iter().map(|v| format!("{:?}", v.sst.id)).collect(),
            _ => Vec::new(),
        }
    }

    /// Step 5 (cold reads): SlateDB sorted runs in the writer's local manifest view (None for a reader).
    pub fn writer_sorted_runs(&self) -> Option<usize> {
        match &self.mode {
            Some(Mode::Writer(db)) => Some(db.manifest().compacted().len()),
            _ => None,
        }
    }

    /// Give group commit the tenant's in-flight write counter (see `GroupFlush::inflight`).
    pub fn bind_inflight(&self, counter: Arc<std::sync::atomic::AtomicU64>) {
        let _ = self.group.inflight.set(counter);
    }

    pub fn is_writer(&self) -> bool {
        matches!(self.mode, Some(Mode::Writer(_)))
    }

    pub async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<Bytes>> {
        self.fx_no_fence_pending()?;
        if let Some(v) = &self.fx {
            return self.fx_get(v, key.as_ref(), false).await;
        }
        self.e1_fresh()?;
        if self.env.is_none() && !self.fx_announced {
            return Ok(match self.mode.as_ref().ok_or_else(|| anyhow!("closed"))? {
                Mode::Reader(r) => r.get(key).await?,
                Mode::Writer(db) => db.get(key).await?,
            });
        }
        let kv = match self.mode.as_ref().ok_or_else(|| anyhow!("closed"))? {
            Mode::Reader(r) => r.get_key_value(key).await?,
            Mode::Writer(db) => db.get_key_value(key).await?,
        };
        let env = self.step_env().await?;
        match kv {
            Some(kv) => crate::envelope::decode(env, &kv.key, kv.seq, kv.value),
            None => Ok(None),
        }
    }

    /// E_env of this database (`envelope.rs`): `None` when it has no envelope marker.
    pub fn envelope_seq(&self) -> Option<u64> {
        self.env
    }

    /// The version of our manifest this tenant reads with (None: none, or `--sealed-runs` off).
    pub fn fx_view(&self) -> Option<&Arc<crate::fx::view::View>> {
        self.fx.as_ref()
    }

    /// The holds of this tenant (the drop floor of step 4, tests).
    pub fn fx_holds(&self) -> &Arc<crate::fx::hold::Holds> {
        &self.holds
    }

    /// A reader in reader mode refuses to read with a version older than T_pin (rule 6): before every step that reads
    /// SlateDB. The writer is the only publisher of its database, so its version is always the newest.
    fn fx_fresh(&self) -> Result<()> {
        if matches!(self.mode, Some(Mode::Reader(_))) {
            let t_pin = Duration::from_millis(crate::fx::T_PIN_MS.load(std::sync::atomic::Ordering::Relaxed));
            if self.fx_loaded.elapsed() > t_pin {
                return Err(anyhow::Error::new(crate::fx::Retry(format!("database {}: our manifest was last read {:?} ago, more than T_pin; retry", self.id, self.fx_loaded.elapsed()))));
            }
        }
        Ok(())
    }

    /// Due for a refresh of the version (a reader at half its T_pin, a fence pending, or a writer whose compactor
    /// published a newer version); the caller refreshes under the exclusive lock.
    pub fn fx_refresh_due(&self) -> bool {
        if self.fx_fence_pending {
            return true;
        }
        if let (Some(sh), Some(v)) = (&self.fx_shared, &self.fx) {
            return sh.pending_version().is_some_and(|p| p > v.version);
        }
        (crate::fx::ENABLED.load(std::sync::atomic::Ordering::Relaxed) || self.e1_tracked())
            && matches!(self.mode, Some(Mode::Reader(_)))
            && self.fx_loaded.elapsed() > Duration::from_millis(crate::fx::T_PIN_MS.load(std::sync::atomic::Ordering::Relaxed) / 2)
    }
    /// Whether a fence is pending (rule 25; diagnostics and tests).
    pub fn fx_fence_pending(&self) -> bool {
        self.fx_fence_pending
    }

    /// Rule 25: no read takes a hold while a fence is pending. The read fails with the retryable error; the server's next
    /// exclusive step (`Entry::ensure_open`, `Dbs::read`) retries the fence through `fx_refresh`.
    fn fx_no_fence_pending(&self) -> Result<()> {
        #[cfg(test)]
        if crate::fx::CONTROL_NO_FENCE_PENDING.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }
        if self.fx_fence_pending {
            return Err(anyhow::Error::new(crate::fx::Retry(format!("database {}: a publish of our manifest has an unknown outcome and its fence has not succeeded yet; retry", self.id))));
        }
        Ok(())
    }

    /// A reader that must follow release E1's start (rules 6 and 26): it knows no E_env yet, and this process runs with
    /// sealed runs or `--envelopes write`. Once it knows E_env, every row decides itself by its seq.
    fn e1_tracked(&self) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        self.env.is_none() && matches!(self.mode, Some(Mode::Reader(_))) && (crate::fx::ENABLED.load(Relaxed) || crate::envelope::START.load(Relaxed))
    }

    /// Rule 6 for a reader that has no version of ours (`fx_fresh` covers one that has): while it follows E1's start, it
    /// refuses to read with an announcement state older than T_pin (step 3 review note: "no T_pin refusal without a
    /// version"). Before every step that reads SlateDB.
    fn e1_fresh(&self) -> Result<()> {
        if self.fx.is_none() && self.e1_tracked() {
            let t_pin = Duration::from_millis(crate::fx::T_PIN_MS.load(std::sync::atomic::Ordering::Relaxed));
            if self.fx_loaded.elapsed() > t_pin {
                return Err(anyhow::Error::new(crate::fx::Retry(format!("database {}: release E1's announcement was last read {:?} ago, more than T_pin; retry", self.id, self.fx_loaded.elapsed()))));
            }
        }
        Ok(())
    }

    /// Load the newest version again (a reader's refresh, or the retry of a pending fence), with E_env and E1's
    /// announcement (rule 6). A fence that fails again is a retryable error. A writer with a compactor installs the
    /// compactor's newest version instead (the compactor is then the only publisher).
    pub async fn fx_refresh(&mut self) -> Result<()> {
        let t0 = Instant::now();
        if self.fx_shared.is_some() && !self.fx_fence_pending {
            self.fx_sync();
            return Ok(());
        }
        self.load_env().await?;
        let r = self.load_fx_from(t0).await;
        if self.fx_fence_pending {
            if let Err(e) = r {
                return Err(anyhow::Error::new(crate::fx::Retry(format!("database {}: the fence of our manifest failed again: {e:#}; retry", self.id))));
            }
        }
        r
    }

    /// Before a hold that `&mut self` can take (a transaction, a publish): retry a pending fence first (rule 25).
    async fn fx_settle_fence(&mut self) -> Result<()> {
        if self.fx_fence_pending {
            self.fx_refresh().await?;
        }
        Ok(())
    }

    /// Tests only: give this tenant holds of its own, as a tenant in another process has (step 7a shares the holds of
    /// every opening of a database in one process).
    #[cfg(test)]
    pub fn fx_own_holds(&mut self) {
        self.holds = Default::default();
    }

    /// The database's id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// F7: refuse to begin a hold once the serving lease has expired (retryable).
    fn fx_serving(&self) -> Result<()> {
        if let Some(t) = *self.serving_until.lock().unwrap() {
            if Instant::now() >= t {
                return Err(anyhow::Error::new(crate::fx::Retry(format!("database {}: this process's serving lease expired; it begins no new read (F7); retry", self.id))));
            }
        }
        Ok(())
    }

    /// Set the end of this process's serving lease of the database (the server, at each renewal; tests).
    pub fn set_serving_until(&self, t: Option<Instant>) {
        *self.serving_until.lock().unwrap() = t;
    }

    /// The SlateDB writer, for this database's compactor (`fx/compactor.rs`): it reads the L0 list and the raw L0 SSTs.
    pub fn fx_writer_db(&self) -> Option<&Db> {
        match &self.mode {
            Some(Mode::Writer(db)) => Some(db),
            _ => None,
        }
    }

    /// Attach this database's compactor: its versions are installed here at the next exclusive step (`fx_sync`), the
    /// served version's X_d is published to the drop floor, and point reads count R3's GETs.
    pub fn fx_attach(&mut self, sh: Arc<crate::fx::compactor::Shared>) {
        if let Some(v) = &self.fx {
            if let Ok(mut nv) = crate::fx::view::View::new(Arc::clone(&self.lc.store), v.meta.clone(), Some(v)) {
                nv.r3 = Some(Arc::clone(&sh.r3));
                sh.serving.store(nv.x_d, std::sync::atomic::Ordering::Relaxed);
                sh.serving_version.store(nv.version, std::sync::atomic::Ordering::Relaxed);
                self.fx = Some(Arc::new(nv));
            }
        }
        self.fx_shared = Some(sh);
    }

    /// Install the compactor's newest version, if newer than the one served. Under `&mut self`: the exclusive lock.
    /// Returns whether a version was installed.
    pub fn fx_sync(&mut self) -> bool {
        let (Some(sh), Some(cur)) = (&self.fx_shared, &self.fx) else { return false };
        let p = sh.pending.lock().unwrap().clone();
        let Some(p) = p.filter(|p| p.version > cur.version) else { return false };
        match crate::fx::view::View::new(Arc::clone(&self.lc.store), p, Some(cur)) {
            Ok(mut nv) => {
                nv.r3 = Some(Arc::clone(&sh.r3));
                // the floor may pass the old X_d only once nothing reads with it: holds cover reads in flight
                sh.serving.store(nv.x_d, std::sync::atomic::Ordering::Relaxed);
                sh.serving_version.store(nv.version, std::sync::atomic::Ordering::Relaxed);
                // integration-3: as `fx_install` does, the counter (step 6) drops what it holds of runs this version retires
                crate::fx::counter::forget_runs(&crate::fx::counter::retired(cur, &nv));
                self.fx = Some(Arc::new(nv));
                self.fx_loaded = Instant::now();
                true
            }
            Err(e) => {
                eprintln!("sealed runs: database {}: install of version from the compactor: {e:#}", self.id);
                false
            }
        }
    }

    /// The E_env to decode one step's rows with, read after the step's SlateDB view is open: the known one, or, once E1
    /// was announced and this tenant knows no E_env yet, the marker read now (rule 26). Without an announcement, no read.
    async fn step_env(&self) -> Result<Option<u64>> {
        if self.env.is_some() || !self.fx_announced {
            return Ok(self.env);
        }
        let kv = match self.mode.as_ref().ok_or_else(|| anyhow!("closed"))? {
            Mode::Reader(r) => r.get_key_value(crate::envelope::MARKER).await?,
            Mode::Writer(db) => db.get_key_value(crate::envelope::MARKER).await?,
        };
        Ok(kv.map(|kv| kv.seq))
    }

    async fn fx_get(&self, v: &Arc<crate::fx::view::View>, key: &[u8], durable: bool) -> Result<Option<Bytes>> {
        self.fx_fresh()?;
        self.fx_serving()?;
        crate::fx::hold::op_note(&self.holds, v);
        let v = &crate::fx::hold::op_view(&self.holds, v);
        let hold = self.holds.take(Arc::clone(v));
        let opts = slatedb::config::ReadOptions {
            durability_filter: if durable { slatedb::config::DurabilityLevel::Remote } else { slatedb::config::DurabilityLevel::Memory },
            ..Default::default()
        };
        let r = match self.mode.as_ref().ok_or_else(|| anyhow!("closed"))? {
            Mode::Reader(r) => crate::fx::read::get(&**r, &slatedb::config::ReadOptions::default(), Some(v), self.env, key).await?,
            Mode::Writer(db) => crate::fx::read::get(db, &opts, Some(v), self.env, key).await?,
        };
        hold.check()?;
        crate::fx::hold::op_check()?;
        Ok(r)
    }

    async fn fx_scan(&self, v: &Arc<crate::fx::view::View>, opts: &slatedb::config::ScanOptions, lo: &[u8], hi: Option<&[u8]>, reverse: bool, limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        self.fx_fresh()?;
        self.fx_serving()?;
        crate::fx::hold::op_note(&self.holds, v);
        let v = &crate::fx::hold::op_view(&self.holds, v);
        let hold = self.holds.take(Arc::clone(v));
        let r = match self.mode.as_ref().ok_or_else(|| anyhow!("closed"))? {
            Mode::Reader(r) => crate::fx::read::scan(&**r, opts, Some(v), self.env, lo, hi, reverse, limit).await?,
            Mode::Writer(db) => crate::fx::read::scan(db, opts, Some(v), self.env, lo, hi, reverse, limit).await?,
        };
        hold.check()?;
        crate::fx::hold::op_check()?;
        Ok(r)
    }

    /// Load the newest version of our manifest (at open, after an upgrade, on a reader's refresh). A writer checks that
    /// SlateDB has every row up to X_d and publishes a fence version, a copy of the newest under the next number (review
    /// of step 0, F1): a late PUT of a dead process then cannot land a version this writer does not know.
    async fn load_fx(&mut self) -> Result<()> {
        self.load_fx_from(Instant::now()).await
    }

    /// [`Tenant::load_fx`] for a refresh that started at `t0`: review of step 7a, note 5 (the F6 shape), the T_pin
    /// window of what a refresh learns starts before its first request, not when the answer arrives. A late answer to a
    /// HEAD the store served before E1's announcement landed must not stretch the window past the writer's first
    /// envelope.
    async fn load_fx_from(&mut self, t0: Instant) -> Result<()> {
        use crate::fx::manifest;
        use std::sync::atomic::Ordering::Relaxed;
        if !crate::fx::ENABLED.load(Relaxed) {
            self.fx = None;
            if self.env.is_none() && crate::envelope::START.load(Relaxed) {
                // release E1 without sealed runs: a reader still takes the announcement with every refresh (rules 6, 26)
                self.fx_announced = manifest::announced(&self.lc.store, &self.id).await?;
                self.fx_loaded = crate::fx::loaded_at(t0);
            }
            return Ok(());
        }
        let store = Arc::clone(&self.lc.store);
        if self.env.is_none() {
            self.fx_announced = manifest::announced(&store, &self.id).await?;
        }
        let newest = manifest::load_newest(&store, &self.id).await?;
        let newest = match (newest, self.fx_unknown.take()) {
            (Some(v), _) => Some(v),
            // a publish with an unknown outcome and an empty store (a first publish whose PUT may still land): fence its
            // slot with the version it tried, so a late PUT finds the slot taken; only then is the fence settled
            #[cfg(test)]
            (None, Some(_)) if crate::fx::CONTROL_NO_FIRST_FENCE.load(Relaxed) => None,
            (None, Some(u)) if matches!(self.mode, Some(Mode::Writer(_))) => {
                let mut f = (*u).clone();
                f.kind = "fence".into();
                f.published_ms = manifest::now_ms();
                match manifest::create(&store, &self.id, &f).await {
                    Ok(true) => {
                        crate::fx::FENCES.fetch_add(1, Relaxed);
                        self.fx = Some(Arc::new(crate::fx::view::View::new(Arc::clone(&store), f, self.fx.as_deref())?));
                        self.fx_fence_pending = false;
                        self.fx_loaded = crate::fx::loaded_at(t0);
                        return Ok(());
                    }
                    // the late PUT landed meanwhile: fence above it as usual
                    Ok(false) => manifest::load_newest(&store, &self.id).await?,
                    Err(e) => {
                        self.fx_unknown = Some(u);
                        self.fx_fence_pending = true;
                        return Err(e);
                    }
                }
            }
            (None, u) => {
                self.fx_unknown = u;
                None
            }
        };
        let Some(mut v) = newest else {
            // no version in the store and no publish of unknown outcome: nothing landed, nothing to fence
            self.fx = None;
            if self.fx_unknown.is_none() {
                self.fx_fence_pending = false;
            }
            self.fx_loaded = crate::fx::loaded_at(t0);
            return Ok(());
        };
        if let Some(Mode::Writer(db)) = &self.mode {
            // a fence is pending from here until one is published (a create that errors or times out leaves it so)
            self.fx_fence_pending = true;
            #[cfg(test)]
            let first = v.version;
            let seq = db.snapshot().await?.seq();
            let epoch = Some(db.manifest().writer_epoch());
            let mut fenced = false;
            for _ in 0..8 {
                // on every version this loop reads, not only the first (review of step 3, note 1)
                #[cfg(test)]
                let check = !crate::fx::CONTROL_NO_XD_LOOP_CHECK.load(std::sync::atomic::Ordering::Relaxed) || v.version == first;
                #[cfg(not(test))]
                let check = true;
                if check {
                    anyhow::ensure!(v.x_d <= seq, "database {}: our manifest {} has X_d {} but SlateDB's last seq is {seq}: rows at or below X_d are missing from SlateDB", self.id, v.version, v.x_d);
                }
                let f = v.fence(epoch);
                if manifest::create(&store, &self.id, &f).await? {
                    v = f;
                    fenced = true;
                    crate::fx::FENCES.fetch_add(1, Relaxed);
                    break;
                }
                v = manifest::load_newest(&store, &self.id).await?.ok_or_else(|| anyhow!("database {}: our manifest vanished", self.id))?;
            }
            anyhow::ensure!(fenced, "database {}: could not publish a fence version of our manifest", self.id);
            self.fx_fence_pending = false;
        }
        anyhow::ensure!(v.e_env == self.env, "database {}: our manifest {} has E_env {:?}, the database's marker {:?}", self.id, v.version, v.e_env, self.env);
        self.fx_install(crate::fx::view::View::new(store, v, self.fx.as_deref())?);
        self.fx_loaded = crate::fx::loaded_at(t0);
        Ok(())
    }

    /// Read with `view` from now on. What the counter holds of runs the new version no longer lists is dropped
    /// (`fx/counter.rs`): those runs are read again only by holds on older versions, which fetch what they need.
    fn fx_install(&mut self, view: crate::fx::view::View) {
        if let Some(old) = self.fx.as_deref() {
            crate::fx::counter::forget_runs(&crate::fx::counter::retired(old, &view));
        }
        self.fx = Some(Arc::new(view));
    }

    /// Start envelopes if the database has none (a version needs E_env <= X_d, `fx/manifest.rs`), in two steps (rule
    /// 26): announce release E1 in the store; write the marker (E_env) only once this process saw the announcement at
    /// least T_pin ago, so every follower has taken the announcement with a refresh and reads the marker before each
    /// step. Returns whether E_env is set.
    async fn ensure_marker(&mut self) -> Result<bool> {
        use std::sync::atomic::Ordering::Relaxed;
        if self.env.is_some() {
            return Ok(true);
        }
        self.upgrade().await?;
        #[cfg(test)]
        let at_once = crate::fx::CONTROL_E1_AT_ONCE.load(Relaxed);
        #[cfg(not(test))]
        let at_once = false;
        if !at_once {
            if !self.fx_announced || self.fx_announce_seen.is_none() {
                // the announcement may be known from the open (`load_fx`); confirm it in the store once, write it if missing
                let store = Arc::clone(&self.lc.store);
                if !crate::fx::manifest::announced(&store, &self.id).await? {
                    crate::fx::manifest::announce(&store, &self.id).await?;
                }
                self.fx_announced = true;
            }
            let seen = *self.fx_announce_seen.get_or_insert_with(Instant::now);
            if seen.elapsed() < crate::fx::e1_wait() {
                return Ok(false);
            }
        }
        let Some(Mode::Writer(db)) = self.mode.as_ref() else { unreachable!() };
        let mut m = slatedb::WriteBatch::new();
        m.put(crate::envelope::MARKER, b"E1");
        let h = db.write(m).await?;
        self.env = Some(h.seqnum());
        Ok(true)
    }

    /// Count a write batch for `--sealed-runs N`; true when a publish is due.
    pub fn fx_note_write(&mut self) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        let every = crate::fx::PUBLISH_EVERY.load(Relaxed);
        if every == 0 || !crate::fx::ENABLED.load(Relaxed) {
            return false;
        }
        self.fx_writes += 1;
        if self.fx_writes >= every {
            self.fx_writes = 0;
            return true;
        }
        false
    }

    /// The build options of the server's own publishes: every `RESEAL_EVERY`-th reseals every group, the others write a
    /// delta run.
    pub fn fx_auto_opts(&mut self) -> crate::fx::build::BuildOpts {
        use std::sync::atomic::Ordering::Relaxed;
        self.fx_builds += 1;
        let every = crate::fx::RESEAL_EVERY.load(Relaxed).max(1);
        let reseal = if self.fx.is_none() || self.fx_builds % every == 0 { crate::fx::build::Reseal::All } else { crate::fx::build::Reseal::None };
        crate::fx::build::BuildOpts { reseal, group_bytes: crate::fx::GROUP_BYTES.load(Relaxed), ..Default::default() }
    }

    /// Build runs offline from this database and publish the next version, then read with it (`fx/build.rs`). Needs
    /// `&mut self`: under the database's exclusive lock. `fail` names fail points for tests ("after_runs" before the
    /// publish, "after_publish" before the tenant reads with the new version): an error there leaves orphan runs or a
    /// published version this tenant does not read with yet, as a crash would.
    pub async fn fx_publish(&mut self, opts: &crate::fx::build::BuildOpts, fail: Option<&(dyn Fn(&str) -> Result<()> + Sync)>) -> Result<Option<crate::fx::build::BuildReport>> {
        use std::sync::atomic::Ordering::Relaxed;
        anyhow::ensure!(crate::fx::ENABLED.load(Relaxed), "--sealed-runs is off");
        anyhow::ensure!(self.fx_shared.is_none(), "database {}: a compactor publishes this database's versions", self.id);
        self.upgrade().await?;
        // no publish starts while a fence is pending (rule 25)
        self.fx_settle_fence().await?;
        if !self.ensure_marker().await? {
            return Ok(None); // E1 announced, envelopes start one T_pin later (rule 26)
        }
        let Some(Mode::Writer(db)) = self.mode.as_ref() else { unreachable!() };
        let epoch = Some(db.manifest().writer_epoch());
        let env = self.env.expect("marker written");
        let store = Arc::clone(&self.lc.store);
        let r = crate::fx::build::build_and_publish(db, &store, &self.id, self.fx.as_deref(), env, epoch, opts, fail).await;
        let (v, rep) = match r {
            Ok(Some(x)) => x,
            Ok(None) => return Ok(None),
            Err(e) => return Err(self.fx_publish_failed(e).await),
        };
        if let Some(f) = fail {
            f("after_publish")?;
        }
        self.fx_install(crate::fx::view::View::new(store, v, self.fx.as_deref())?);
        self.fx_loaded = Instant::now();
        crate::fx::PUBLISHES.fetch_add(1, Relaxed);
        Ok(Some(rep))
    }

    /// A publish that failed (the builder's or the erasure's): reload on a lost race (rule 24), fence on an unknown
    /// outcome (rule 25), nothing more when this writer is fenced (rule 28). Returns the error to give back.
    async fn fx_publish_failed(&mut self, e: anyhow::Error) -> anyhow::Error {
        use std::sync::atomic::Ordering::Relaxed;
        let store = Arc::clone(&self.lc.store);
        let r: Result<()> = async {
                crate::fx::PUBLISH_ERRORS.fetch_add(1, Relaxed);
                if e.downcast_ref::<crate::fx::LostRace>().is_some() {
                    // the outcome is known: another publisher holds the slot; read with the newest version (rule 24)
                    if let Some(v) = crate::fx::manifest::load_newest(&store, &self.id).await? {
                        self.fx_install(crate::fx::view::View::new(Arc::clone(&store), v, self.fx.as_deref())?);
                    }
                } else if !crate::fx::is_fenced(&e) {
                    // the outcome of the publish's PUT may be unknown (an error, a timeout: the PUT may have landed):
                    // publish a fence version before another hold or publish (rule 25). If the fence fails too, the fence
                    // stays pending: reads refuse and every exclusive step retries it. A fenced writer publishes nothing
                    // (rule 28).
                    self.fx_fence_pending = true;
                    if let Some(u) = e.downcast_ref::<crate::fx::UnknownPublish>() {
                        self.fx_unknown = Some(u.version.clone());
                    }
                    if let Err(e2) = self.load_fx().await {
                        eprintln!("sealed runs: fence of database {} after a failed publish: {e2:#}; the fence stays pending", self.id);
                    }
                }
                Ok(())
        }
        .await;
        if let Err(r) = r {
            return e.context(format!("and the reload after it failed: {r:#}"));
        }
        e
    }

    /// Record an erasure of the customer whose deletes this writer just committed (`fx/erase.rs`): S_e is the database's
    /// last seq now. The caller holds the database's exclusive lock from before the deletes until after this call.
    /// None with `--sealed-runs` off (nothing of ours holds the rows; SlateDB's compaction removes them as before).
    pub async fn fx_erasure_register(&mut self, collection: &str, customer: &[u8], targets: &crate::fx::erase::Targets) -> Result<Option<crate::fx::erase::Erasure>> {
        if !crate::fx::ENABLED.load(std::sync::atomic::Ordering::Relaxed) || targets.is_empty() {
            return Ok(None);
        }
        self.upgrade().await?;
        let Some(Mode::Writer(db)) = self.mode.as_ref() else { unreachable!() };
        let s_e = db.snapshot().await?.seq();
        let store = Arc::clone(&self.lc.store);
        Ok(Some(crate::fx::erase::register(&store, &self.id, collection, customer, targets, s_e).await?))
    }

    /// The erasure pass of this database (`fx/erase.rs`): for each erasure not done, make X_d reach S_e (a publish),
    /// scrub the runs of the newest version and publish the result, then retire what the scrub replaced. Returns every
    /// erasure of the database as it now stands. Under the exclusive lock (`&mut self`).
    pub async fn fx_erase_pass(&mut self, opts: &crate::fx::erase::ScrubOpts) -> Result<Vec<crate::fx::erase::Erasure>> {
        use crate::fx::erase;
        use std::sync::atomic::Ordering::Relaxed;
        let store = Arc::clone(&self.lc.store);
        let mut all = erase::list(&store, &self.id).await?;
        if !crate::fx::ENABLED.load(Relaxed) || all.iter().all(|e| e.done_ms.is_some()) {
            return Ok(all);
        }
        self.upgrade().await?;
        self.fx_settle_fence().await?;
        for e in all.iter_mut().filter(|e| e.done_ms.is_none()) {
            if e.state == "pending" {
                // 1. our runs hold every row at or below S_e
                if let (Some(sh), true) = (self.fx_shared.clone(), self.fx.as_ref().is_none_or(|v| v.x_d < e.s_e)) {
                    // integration-3: with a compactor (step 4) it is the only publisher, and fx_publish refuses. Ask it to
                    // absorb up to S_e (the deletes must be in an L0 SST first: flush the memtable), take its newest
                    // version, and come back at the next pass until X_d reaches S_e.
                    sh.absorb_upto.fetch_max(e.s_e, Relaxed);
                    if let Some(Mode::Writer(db)) = self.mode.as_ref() {
                        db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await?;
                    }
                    self.fx_sync();
                    if self.fx.as_ref().is_none_or(|v| v.x_d < e.s_e) {
                        continue;
                    }
                }
                if self.fx.as_ref().is_none_or(|v| v.x_d < e.s_e) {
                    let mut o = crate::fx::build::BuildOpts { group_bytes: crate::fx::GROUP_BYTES.load(Relaxed), ..Default::default() };
                    o.sealed = opts.sealed.clone();
                    o.delta = opts.delta.clone();
                    o.reseal = if self.fx.is_none() { crate::fx::build::Reseal::All } else { crate::fx::build::Reseal::None };
                    if self.fx_publish(&o, None).await?.is_none() && self.fx.as_ref().is_none_or(|v| v.x_d < e.s_e) {
                        continue; // release E1 announced, envelopes start one T_pin later: the next pass
                    }
                }
                let cur = Arc::clone(self.fx.as_ref().expect("a version"));
                // 2. scrub
                let Some(t) = erase::load_targets(&store, &self.id, &e.id).await? else {
                    anyhow::bail!("database {}: erasure {} lost its targets before its scrub", self.id, e.id);
                };
                let Some(Mode::Writer(db)) = self.mode.as_ref() else { unreachable!() };
                let epoch = Some(db.manifest().writer_epoch());
                let next = manifest_next(&store, &self.id, cur.version).await?;
                let s = erase::scrub_version(&store, &self.id, &cur, next, epoch, &t, e.s_e, opts).await?;
                let version = match &s.version {
                    Some(v) => {
                        match crate::fx::manifest::create(&store, &self.id, v).await {
                            Ok(true) => {}
                            Ok(false) => return Err(self.fx_publish_failed(anyhow::Error::new(crate::fx::LostRace(format!("erasure version {} of {} lost the race", v.version, self.id)))).await),
                            Err(x) => return Err(self.fx_publish_failed(anyhow::Error::new(crate::fx::UnknownPublish { version: Box::new(v.clone()), msg: format!("{x:#}") })).await),
                        }
                        self.fx_install(crate::fx::view::View::new(Arc::clone(&store), v.clone(), self.fx.as_deref())?);
                        self.fx_loaded = Instant::now();
                        crate::fx::PUBLISHES.fetch_add(1, Relaxed);
                        v.version
                    }
                    None => cur.version,
                };
                let newest = self.fx.as_ref().unwrap().meta.clone();
                erase::record_scrub(&store, &self.id, e, &s, version, &t, &newest).await?;
            }
            // 4. retire
            let newest = self.fx.as_ref().map_or(0, |v| v.version);
            erase::retire(&store, &self.id, e, newest, crate::fx::GC_GRACE_MS.load(Relaxed)).await?;
            erase::save(&store, &self.id, e).await?;
        }
        Ok(all)
    }

    /// The step 9 ballast generator (`mongo-server --ballast`), before it generates: the envelope marker (`Ok(None)` while
    /// E1's wait runs: call again later), then a durable cut. Returns the cut `x0`, the seq every generated row carries.
    pub async fn fx_ballast_begin(&mut self) -> Result<Option<u64>> {
        anyhow::ensure!(crate::fx::ENABLED.load(std::sync::atomic::Ordering::Relaxed), "the ballast generator writes sealed runs: --sealed-runs must be on");
        self.upgrade().await?;
        self.fx_settle_fence().await?;
        if !self.ensure_marker().await? {
            return Ok(None);
        }
        let Some(Mode::Writer(db)) = self.mode.as_ref() else { unreachable!() };
        db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::Wal }).await?;
        let x0 = db.snapshot().await?.seq();
        Ok(Some(x0))
    }

    /// The writer epoch and the store the generated runs go to (the ballast generator's [`crate::fx::build::GroupWriter`]).
    pub fn fx_ballast_target(&self) -> Result<(Arc<dyn ObjectStore>, String, Option<u64>)> {
        let Some(Mode::Writer(db)) = self.mode.as_ref() else { anyhow::bail!("database {} is not open as a writer", self.id) };
        Ok((Arc::clone(&self.lc.store), self.id.clone(), Some(db.manifest().writer_epoch())))
    }

    /// Publish the generated rows merged with SlateDB's (`fx/build.rs`, [`crate::fx::build::publish_generated`]) and read
    /// with that version from now on, as [`Tenant::fx_publish`] does.
    pub async fn fx_publish_generated(&mut self, opts: &crate::fx::build::BuildOpts, gen: crate::fx::build::Generated) -> Result<crate::fx::build::BuildReport> {
        use std::sync::atomic::Ordering::Relaxed;
        self.fx_settle_fence().await?;
        let Some(Mode::Writer(db)) = self.mode.as_ref() else { anyhow::bail!("database {} is not open as a writer", self.id) };
        let epoch = Some(db.manifest().writer_epoch());
        let env = self.env.ok_or_else(|| anyhow!("no envelope marker: call fx_ballast_begin first"))?;
        let store = Arc::clone(&self.lc.store);
        let (v, rep) = crate::fx::build::publish_generated(db, &store, &self.id, self.fx.as_deref(), env, epoch, opts, gen).await.map_err(|e| {
            crate::fx::PUBLISH_ERRORS.fetch_add(1, Relaxed);
            e
        })?;
        self.fx = Some(Arc::new(crate::fx::view::View::new(store, v, self.fx.as_deref())?));
        self.fx_loaded = Instant::now();
        crate::fx::PUBLISHES.fetch_add(1, Relaxed);
        Ok(rep)
    }

    /// Read the envelope marker (at open and after an upgrade).
    async fn load_env(&mut self) -> Result<()> {
        let kv = match self.mode.as_ref().ok_or_else(|| anyhow!("closed"))? {
            Mode::Reader(r) => r.get_key_value(crate::envelope::MARKER).await?,
            Mode::Writer(db) => db.get_key_value(crate::envelope::MARKER).await?,
        };
        self.env = kv.map(|kv| kv.seq);
        Ok(())
    }

    /// With tracking on (the log path, [`TRACK_VISIBLE`]) and a batch in flight: reads must use the visible state.
    fn tracking(&self) -> bool {
        TRACK_VISIBLE.load(std::sync::atomic::Ordering::Relaxed) && self.is_writer() && self.group.vis.any()
    }

    /// The value of `key` in `snap`, a snapshot taken before an in-flight batch (see [`TRACK_VISIBLE`]), read the way every
    /// other read is: through the merged reader with this tenant's version of our manifest (compaction 2, `fx`) and its
    /// envelopes. A version's X_d is a durable cut (`fx/build.rs` flushes the WAL first), so its runs hold no write the
    /// log could still lose.
    async fn snapshot_get(&self, snap: &Arc<slatedb::DbSnapshot>, key: &[u8]) -> Result<Option<Bytes>> {
        self.fx_no_fence_pending()?;
        let opts = slatedb::config::ReadOptions::default();
        match &self.fx {
            Some(v) => {
                self.fx_fresh()?;
                let hold = self.holds.take(Arc::clone(v));
                let r = crate::fx::read::get(&**snap, &opts, Some(v), self.env, key).await?;
                hold.check()?;
                Ok(r)
            }
            None => {
                self.e1_fresh()?;
                let env = self.step_env().await?;
                crate::fx::read::get(&**snap, &opts, None, env, key).await
            }
        }
    }

    /// Whether a read sees the latest staged state: no batch is in flight on the log path (see [`TRACK_VISIBLE`]). A cache
    /// that follows staged writes (the first course, `opening.rs` in the server) may answer only then.
    pub fn reads_see_latest(&self) -> bool {
        !self.tracking()
    }

    /// The snapshot a read of `key` must use (see [`TRACK_VISIBLE`]), `None` when its latest value is visible.
    pub fn visible_snapshot_for(&self, key: &[u8]) -> Option<Arc<slatedb::DbSnapshot>> {
        if !self.tracking() {
            return None;
        }
        let s = self.group.vis.snapshot_for(key);
        if s.is_some() {
            VISIBLE_SNAPSHOT_READS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        s
    }

    /// Wait until every batch staged so far is visible (a transaction starts on a snapshot with nothing in flight).
    pub async fn wait_all_visible(&self, limit: Duration) -> Result<()> {
        if !TRACK_VISIBLE.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }
        self.group.vis.wait_all(limit).await
    }

    /// The visible value of `key` (see [`TRACK_VISIBLE`]): what a read may return.
    pub async fn get_visible<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<Bytes>> {
        match self.visible_snapshot_for(key.as_ref()) {
            Some(s) => self.snapshot_get(&s, key.as_ref()).await,
            None => self.get(key).await,
        }
    }

    /// Replace, in `rows` (key order, ascending or descending), the values of the keys an in-flight batch writes by their
    /// visible values; `overlay` is from [`Visible::in_range`].
    async fn overlay(&self, rows: Vec<(Bytes, Bytes)>, overlay: Vec<(Vec<u8>, Arc<slatedb::DbSnapshot>)>, reverse: bool) -> Result<Vec<(Bytes, Bytes)>> {
        VISIBLE_SNAPSHOT_READS.fetch_add(overlay.len() as u64, std::sync::atomic::Ordering::Relaxed);
        let mut m: std::collections::BTreeMap<Bytes, Bytes> = rows.into_iter().collect();
        for (k, snap) in overlay {
            let k = Bytes::from(k);
            match self.snapshot_get(&snap, &k).await? {
                Some(v) => {
                    m.insert(k, v);
                }
                None => {
                    m.remove(&k);
                }
            }
        }
        Ok(if reverse { m.into_iter().rev().collect() } else { m.into_iter().collect() })
    }

    /// [`Tenant::scan_prefix`] of the visible state.
    pub async fn scan_prefix_visible(&self, prefix: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        if !self.tracking() {
            return self.scan_prefix(prefix).await;
        }
        let end = prefix_end(prefix);
        let over = self.group.vis.in_range(prefix, end.as_deref());
        let rows = self.scan_prefix(prefix).await?;
        if over.is_empty() {
            return Ok(rows);
        }
        self.overlay(rows, over, false).await
    }

    /// Like [`Tenant::get`], but a writer only returns data that is already in a durable WAL object or SST, not
    /// writes that are staged and waiting for their flush (a reader's data is always durable).
    pub async fn get_durable<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<Bytes>> {
        self.fx_no_fence_pending()?;
        if let Some(v) = &self.fx {
            return self.fx_get(v, key.as_ref(), true).await;
        }
        self.e1_fresh()?;
        Ok(match self.mode.as_ref().ok_or_else(|| anyhow!("closed"))? {
            Mode::Reader(r) => match r.get_key_value(key).await? {
                Some(kv) => crate::envelope::decode(self.step_env().await?, &kv.key, kv.seq, kv.value)?,
                None => None,
            },
            Mode::Writer(db) => {
                let opts = slatedb::config::ReadOptions { durability_filter: slatedb::config::DurabilityLevel::Remote, ..Default::default() };
                match db.get_key_value_with_options(key, &opts).await? {
                    Some(kv) => crate::envelope::decode(self.env, &kv.key, kv.seq, kv.value)?,
                    None => None,
                }
            }
        })
    }

    /// Close the reader and open a writer. The marker is deleted concurrently with the
    /// writer open and always before the first `put` is accepted, so a crash at any
    /// point after a write was acknowledged leaves the marker missing.
    pub async fn upgrade(&mut self) -> Result<()> {
        if self.is_writer() {
            return Ok(());
        }
        // Background work, like the open (meter.rs): the writer's open reads are not charged to the write that needed it.
        let id = self.id.clone();
        crate::meter::background(&id, self.upgrade_inner()).await
    }

    async fn upgrade_inner(&mut self) -> Result<()> {
        if self.lc.l0_guard && self.l0_at_wake >= L0_GUARD {
            if let Some(q) = &self.lc.queue {
                let started = Instant::now();
                for _ in 0..3 {
                    let (tx, rx) = oneshot::channel();
                    q.request(&self.id, Some(tx));
                    let l0 = tokio::time::timeout(Duration::from_secs(30), rx)
                        .await
                        .context("compaction wait timed out")??;
                    self.l0_at_wake = l0;
                    if l0 < L0_GUARD {
                        break;
                    }
                }
                let mut st = self.lc.stats.lock().unwrap();
                st.guard_waits += 1;
                st.guard_wait_secs.push(started.elapsed().as_secs_f64());
            }
        }
        if let Some(Mode::Reader(r)) = self.mode.take() {
            r.close().await?;
        }
        let store = &self.lc.store;
        let marker = marker_path(&self.id);
        let (del, db) = tokio::join!(
            store.delete(&marker),
            writer_builder_in(&self.id, store, self.scope).build()
        );
        match del {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
        let db = db?;
        self.epoch = opened_epoch(store, &self.id).await?;
        writer_opened(&self.id, &db);
        self.mode = Some(Mode::Writer(db));
        self.load_env().await?;
        self.load_fx().await?;
        self.lc.touched.lock().unwrap().insert(self.id.clone(), Instant::now());
        Ok(())
    }

    /// All key/value pairs whose key starts with `prefix`, in key order.
    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        self.scan_prefix_at(prefix, false).await
    }

    /// Like [`Tenant::scan_prefix`], but a writer only returns durable data (see [`Tenant::get_durable`]).
    pub async fn scan_prefix_durable(&self, prefix: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        self.scan_prefix_at(prefix, true).await
    }

    async fn scan_prefix_at(&self, prefix: &[u8], durable: bool) -> Result<Vec<(Bytes, Bytes)>> {
        self.fx_no_fence_pending()?;
        if let Some(v) = &self.fx {
            let hi = crate::runfmt::prefix_successor(prefix);
            let mut opts = crate::fx::read::scan_options(4 << 20, false, durable);
            opts.max_fetch_tasks = 4;
            return self.fx_scan(v, &opts, prefix, hi.as_deref(), false, None).await;
        }
        self.e1_fresh()?;
        // slatedb's scan defaults fetch one 4 KiB block per request, one at a time, and do not cache
        // the blocks: a 1 MB scan costs ~250 sequential GETs every time. Read ahead in large
        // requests, fetch in parallel, and keep the blocks.
        let opts = slatedb::config::ScanOptions {
            read_ahead_bytes: 4 << 20,
            max_fetch_tasks: 4,
            cache_blocks: true,
            durability_filter: if durable { slatedb::config::DurabilityLevel::Remote } else { slatedb::config::DurabilityLevel::Memory },
            ..Default::default()
        };
        let mut it = match self.mode.as_ref().ok_or_else(|| anyhow!("closed"))? {
            Mode::Reader(r) => r.scan_prefix_with_options(prefix, .., &opts).await?,
            Mode::Writer(db) => db.scan_prefix_with_options(prefix, .., &opts).await?,
        };
        let env = self.step_env().await?;
        let mut out = Vec::new();
        while let Some(kv) = it.next().await? {
            if let Some(v) = crate::envelope::decode(env, &kv.key, kv.seq, kv.value)? {
                out.push((kv.key, v));
            }
        }
        Ok(out)
    }

    /// A consistent snapshot of the database as of now (upgrades to a writer first: snapshots exist on writers only).
    /// With a version of ours, the snapshot holds it, taken first (so its X_d <= the snapshot's seq) and kept for the
    /// transaction's whole life: the pair of rule 1.
    pub async fn txn_snapshot(&mut self) -> Result<Arc<crate::fx::read::Snap>> {
        self.upgrade().await?;
        self.fx_settle_fence().await?;
        if self.fx.is_some() {
            self.fx_sync();
            self.fx_serving()?;
        }
        let hold = self.fx.as_ref().map(|v| self.holds.take(Arc::clone(v)));
        let Some(Mode::Writer(db)) = self.mode.as_ref() else { unreachable!() };
        Ok(Arc::new(crate::fx::read::Snap { slate: db.snapshot().await?, hold, env: self.env }))
    }

    /// Rough size in bytes of the data in the tenant's SSTs (from the manifest: no request). Unflushed writes are not
    /// counted. Used to choose between point reads and a range scan.
    pub fn approx_bytes(&self) -> u64 {
        let m = match self.mode.as_ref() {
            Some(Mode::Reader(r)) => r.manifest(),
            Some(Mode::Writer(db)) => db.manifest(),
            None => return 0,
        };
        m.l0().iter().map(|v| v.estimate_size()).sum::<u64>() + m.compacted().iter().map(|r| r.estimate_size()).sum::<u64>()
    }

    /// Key/value pairs with `start <= key < end` (`end == None`: to the end of the keyspace), at most `limit`,
    /// ascending or descending. `read_ahead` is the bytes fetched per request (small for short index scans).
    pub async fn scan_range(&self, start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize) -> Result<Vec<(Bytes, Bytes)>> {
        self.scan_range_at(start, end, reverse, limit, read_ahead, false).await
    }

    /// [`Tenant::scan_range`] of the visible state (see [`TRACK_VISIBLE`]).
    pub async fn scan_range_visible(&self, start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize) -> Result<Vec<(Bytes, Bytes)>> {
        if !self.tracking() {
            return self.scan_range_at(start, end, reverse, limit, read_ahead, false).await;
        }
        if let Some(e) = end {
            if start >= e {
                return Ok(Vec::new());
            }
        }
        let over = self.group.vis.in_range(start, end);
        if over.is_empty() {
            return self.scan_range_at(start, end, reverse, limit, read_ahead, false).await;
        }
        // the overlay removes at most one row per key in flight: read that many more, then cut
        let rows = self.scan_range_at(start, end, reverse, limit.map(|l| l + over.len()), read_ahead, false).await?;
        let mut rows = self.overlay(rows, over, reverse).await?;
        if let Some(l) = limit {
            rows.truncate(l);
        }
        Ok(rows)
    }

    /// [`Tenant::scan_range`]; `durable`: a writer returns only durable data (see [`Tenant::get_durable`]).
    pub async fn scan_range_at(&self, start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize, durable: bool) -> Result<Vec<(Bytes, Bytes)>> {
        let opts = slatedb::config::ScanOptions {
            read_ahead_bytes: read_ahead,
            max_fetch_tasks: crate::tiered::scan_fetch_tasks(read_ahead),
            cache_blocks: true,
            order: if reverse { slatedb::IterationOrder::Descending } else { slatedb::IterationOrder::Ascending },
            durability_filter: if durable { slatedb::config::DurabilityLevel::Remote } else { slatedb::config::DurabilityLevel::Memory },
            ..Default::default()
        };
        if let Some(e) = end {
            if start >= e {
                return Ok(Vec::new()); // an empty range would panic the store
            }
        }
        self.fx_no_fence_pending()?;
        if let Some(v) = &self.fx {
            return self.fx_scan(v, &opts, start, end, reverse, limit).await;
        }
        self.e1_fresh()?;
        let s = start.to_vec();
        let range = match end {
            Some(e) => (std::ops::Bound::Included(s), std::ops::Bound::Excluded(e.to_vec())),
            None => (std::ops::Bound::Included(s), std::ops::Bound::Unbounded),
        };
        let mut it = match self.mode.as_ref().ok_or_else(|| anyhow!("closed"))? {
            Mode::Reader(r) => r.scan_with_options(range, &opts).await?,
            Mode::Writer(db) => db.scan_with_options(range, &opts).await?,
        };
        let env = self.step_env().await?;
        let mut out = Vec::new();
        while let Some(kv) = it.next().await? {
            let Some(v) = crate::envelope::decode(env, &kv.key, kv.seq, kv.value)? else { continue };
            out.push((kv.key, v));
            if limit.map(|l| out.len() >= l).unwrap_or(false) {
                break;
            }
        }
        Ok(out)
    }

    /// Apply writes as one atomic batch and wait until they are durable. Upgrades first.
    pub async fn apply(&mut self, ops: Vec<Op>) -> Result<()> {
        match self.stage(ops).await? {
            Some(commit) => commit.wait().await,
            None => Ok(()),
        }
    }

    /// Put writes into the database as one atomic batch (visible to reads, ordered after earlier batches) and return
    /// the [`Commit`] that waits for them to be durable. The write is not acknowledged until `Commit::wait` returns,
    /// so callers can release their locks first and let concurrent writes share the next WAL object (group commit).
    pub async fn stage(&mut self, ops: Vec<Op>) -> Result<Option<Commit>> {
        if ops.is_empty() {
            return Ok(None);
        }
        self.upgrade().await?;
        let track = TRACK_VISIBLE.load(std::sync::atomic::Ordering::Relaxed);
        let keys: Vec<Vec<u8>> = if track { ops.iter().map(|o| match o { Op::Put(k, _) | Op::Delete(k) => k.clone() }).collect() } else { Vec::new() };
        if self.env.is_none() && crate::envelope::START.load(std::sync::atomic::Ordering::Relaxed) {
            // release E1 (rule 26): the first write announces it in the store; envelopes start (the marker alone in its own
            // batch, whose sequence number is E_env, `envelope.rs`) only once this process saw the announcement at least
            // one T_pin ago. Until then the writes stay plain, as release E0 writes them.
            self.ensure_marker().await?;
        }
        let Some(Mode::Writer(db)) = self.mode.as_ref() else { unreachable!() };
        let mut batch = slatedb::WriteBatch::new();
        let enveloped = self.env.is_some();
        for op in ops {
            match op {
                Op::Put(k, v) if enveloped && k != crate::envelope::MARKER => batch.put(k, crate::envelope::live(v)),
                Op::Delete(k) if enveloped && k != crate::envelope::MARKER => batch.put(k, crate::envelope::dead()),
                Op::Put(k, v) => batch.put(k, v),
                Op::Delete(k) => batch.delete(k),
            }
        }
        // the state just before this batch, for readers while the batch is not confirmed (see `TRACK_VISIBLE`); the
        // caller holds the database's exclusive lock, so nothing is staged between the snapshot and the write
        let before = if track { Some(db.snapshot().await?) } else { None };
        let handle = db.write(batch).await?;
        note_unflushed(&self.id, handle.seqnum());
        if let Some(b) = before {
            self.group.vis.add(handle.seqnum(), b, keys);
            let mut m = VIS_GROUPS.lock().unwrap();
            if !m.get(&self.id).map_or(false, |g| Arc::ptr_eq(g, &self.group)) {
                m.insert(self.id.clone(), Arc::clone(&self.group));
            }
        }
        self.group.staged_seq.fetch_max(handle.seqnum(), std::sync::atomic::Ordering::Release);
        Ok(Some(Commit { db: db.clone(), handle, group: Arc::clone(&self.group), tenant: self.id.clone(), local: None, agent: None }))
    }

    pub async fn put(&mut self, key: &str, value: &[u8]) -> Result<WriteHandle> {
        self.upgrade().await?;
        if self.env.is_none() && crate::envelope::START.load(std::sync::atomic::Ordering::Relaxed) {
            self.ensure_marker().await?; // release E1, as in `stage`
        }
        let Some(Mode::Writer(db)) = self.mode.as_ref() else { unreachable!() };
        if self.env.is_some() {
            return Ok(db.put(key, crate::envelope::live(value.to_vec())).await?);
        }
        Ok(db.put(key, value).await?)
    }

    /// Give the tenant up without any of the clean-close bookkeeping (no marker, no compaction request):
    /// used when ownership was lost, where flushing would only be fenced anyway.
    ///
    /// A WAL flush instead of the memtable flush, and a bounded wait. `Db::close` flushes the memtable by default, and with
    /// L0 full that flush waits for a compaction that never comes: the writer is already out of the L0 watch, and the store
    /// may still be down. The caller holds the tenant's lock, so before this every command on the database and the SIGTERM
    /// close waited forever (the engine stuck after losing its lease to a store outage longer than the lease). The WAL
    /// flush needs no room in L0 and still uploads what was acknowledged before the WAL write (local log, relaxed
    /// durability) as the memtable flush did; a fenced WAL write fails, as it should. Without a clean marker the next owner
    /// replays the WAL.
    ///
    /// Returns whether the close finished within [`ABANDON_CLOSE_TIMEOUT`]. When it did not, the close goes on in its own task
    /// (so slatedb's background tasks still stop once the store answers again) and [`close_pending`] says so until it ends:
    /// slatedb 0.17 has no `Drop` for `Db`, so a tenant left behind keeps tasks that can still write a WAL object or an SST.
    /// The lost-lease callers can ignore that (the next owner fences them); a caller about to delete the tenant's objects
    /// (erase) must not.
    pub async fn abandon(mut self) -> bool {
        self.group.vis.kill();
        forget_vis(&self.id, &self.group);
        let close = match self.mode.take() {
            Some(Mode::Reader(r)) => tokio::spawn(pending_close(&self.id, async move {
                let _ = r.close().await;
            })),
            Some(Mode::Writer(db)) => {
                writer_closed(&self.id);
                tokio::spawn(pending_close(&self.id, async move {
                    let wal = slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::Wal };
                    let _ = db.flush_with_options(wal).await;
                    let _ = db.close_with_options(slatedb::config::CloseOptions { flush_type: None }).await;
                }))
            }
            None => return true,
        };
        let done = tokio::time::timeout(ABANDON_CLOSE_TIMEOUT, close).await.is_ok();
        if !done {
            ABANDON_CLOSE_TIMEOUTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            eprintln!("abandon of tenant {}: close did not finish in {:?}; left behind (it goes on in the background)", self.id, ABANDON_CLOSE_TIMEOUT);
        }
        done
    }

    /// Close. A writer flushes its memtable, then the marker is written, and the tenant is
    /// queued for compaction if it now holds enough L0 SSTs. A reader just closes.
    pub async fn close(mut self) -> Result<()> {
        match self.mode.take() {
            Some(Mode::Reader(r)) => r.close().await?,
            Some(Mode::Writer(db)) => {
                // Flush the memtable while the writer is still open and registered, then close it. A flush that waits for
                // room in L0 (slatedb's `l0_max_ssts`) needs the L0 watch of the maintenance loop to start a compaction
                // and that compaction to refresh this writer's manifest; once `Db::close` has begun slatedb refuses the
                // refresh, and the flush would wait for the writer's own manifest poll (`--writer-poll-secs`).
                let id = self.id.clone();
                let closed = tokio::time::timeout(CLOSE_FLUSH_TIMEOUT, async {
                    let memtable = slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable };
                    if let Err(e) = db.flush_with_options(memtable).await {
                        eprintln!("close of tenant {id}: memtable flush before the close failed: {e}");
                    }
                    writer_closed(&id);
                    db.close().await
                })
                .await;
                writer_closed(&self.id);
                match closed {
                    Ok(r) => r?,
                    Err(_) => {
                        // The memtable flush is stalled (L0 full, waiting for a compaction). Everything acknowledged is in
                        // durable WAL objects: close without the flush and leave no marker, so the next wake replays them.
                        eprintln!("close of tenant {}: final flush did not finish in {:?}; closing without it (unclean)", self.id, CLOSE_FLUSH_TIMEOUT);
                        // with the log on, the last writes may be only in the log: the next wake replays them from it
                        // (unclean wake, `dbs.rs`); nothing pending here is confirmed by this writer any more
                        self.group.vis.kill();
                        forget_vis(&self.id, &self.group);
                        let _ = db.close_with_options(slatedb::config::CloseOptions { flush_type: None }).await;
                        return Err(anyhow!("final flush timed out"));
                    }
                }
                // the close flushed the memtable: everything staged is in object storage
                self.group.vis.confirm_all();
                forget_vis(&self.id, &self.group);
                let l0 = db.manifest().l0().len();
                self.lc
                    .store
                    .put(&marker_path(&self.id), PutPayload::from_static(b"clean"))
                    .await?;
                self.lc.touched.lock().unwrap().insert(self.id.clone(), Instant::now());
                if l0 >= L0_COMPACT_THRESHOLD {
                    if let Some(q) = &self.lc.queue {
                        q.request(&self.id, None);
                    }
                }
            }
            None => {}
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Maintenance: compaction and GC outside the writer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize)]
pub struct CompactionRecord {
    pub tenant: String,
    pub l0_before: usize,
    pub l0_after: usize,
    pub checkpoints_deleted: usize,
    pub secs: f64,
    pub requests: Snapshot,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct GcRecord {
    pub tenant: String,
    pub requests: Snapshot,
}

/// Number of checkpoints in a tenant's latest manifest (no request counting).
pub async fn checkpoint_count(raw: &Arc<dyn ObjectStore>, id: impl std::fmt::Display) -> Result<usize> {
    let admin = Admin::builder(tenant_path(id), Arc::clone(raw)).build();
    Ok(admin.list_checkpoints(None).await?.len())
}

/// Read a tenant's L0 SST count straight from the latest manifest (no request counting).
pub async fn l0_count(raw: &Arc<dyn ObjectStore>, id: impl std::fmt::Display) -> Result<usize> {
    let admin = Admin::builder(tenant_path(id), Arc::clone(raw)).build();
    Ok(admin.read_manifest(None).await?.map(|m| m.l0().len()).unwrap_or(0))
}

/// One bounded compaction pass over a closed or idle tenant, using a standalone
/// compactor (`CompactorBuilder::run`) with its own request counter. The harness
/// polls the manifest through `raw` (not counted) to know when to stop it.
pub async fn compact_tenant(s3: &Arc<dyn ObjectStore>, raw: &Arc<dyn ObjectStore>, id: impl std::fmt::Display) -> Result<Option<CompactionRecord>> {
    compact_tenant_within(s3, raw, id, COMPACTION_STALL, COMPACTOR_STOP_LIMIT).await
}

async fn compact_tenant_within(s3: &Arc<dyn ObjectStore>, raw: &Arc<dyn ObjectStore>, id: impl std::fmt::Display, stall: Duration, stop_limit: Duration) -> Result<Option<CompactionRecord>> {
    let warm = crate::warm::COMPACTION_WARM.load(std::sync::atomic::Ordering::Relaxed);
    compact_tenant_pass(s3, raw, id, stall, stop_limit, warm).await
}

/// A compaction pass stops when its compactor moved no compacted-SST bytes for this long (the store does not answer, or
/// nothing can make progress). It is not a limit on the pass: a pass that makes progress runs until its work is done.
pub const COMPACTION_STALL: Duration = Duration::from_secs(30);
/// After this long, a pass whose L0 is already below the threshold (only merges of sorted runs left) gives back its
/// maintenance slot, so that the L0 of the other tenants is not kept waiting behind a long merge.
pub const COMPACTION_SLOT_HOLD: Duration = Duration::from_secs(30);
/// How often a pass with its L0 below the threshold (merges of sorted runs left) reads the manifest and the compaction
/// state. Each read is a LIST and a GET on S3: a merge can last hours on a large database.
const MERGE_POLL: Duration = Duration::from_secs(2);

/// Tenants whose running compaction pass must stop now (drop, erase, fork, export, restore want the maintenance lock).
static PASS_PREEMPT: std::sync::LazyLock<Mutex<HashMap<String, usize>>> = std::sync::LazyLock::new(Default::default);
/// Passes running now, and those that gave back their maintenance slot (long merges).
pub static PASSES_RUNNING: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static PASSES_LONG: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Passes that ended because their compactor made no progress for `COMPACTION_STALL`, and passes stopped on request.
pub static PASSES_STALLED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static PASSES_PREEMPTED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Ask the running compaction pass of `id` (if any) to stop, for as long as the returned guard lives. Taken before the
/// tenant's maintenance lock by work that cannot wait for a merge of sorted runs to end (it can last hours on a large
/// database): once its L0 is below the threshold (the L0 compaction is short and the work that preempts needs it) the pass
/// stops its compactor (the merges in flight are lost, their output is garbage) and gives back the lock.
pub fn preempt_compaction(id: &str) -> PreemptGuard {
    *PASS_PREEMPT.lock().unwrap().entry(id.to_string()).or_default() += 1;
    PreemptGuard(id.to_string())
}

pub struct PreemptGuard(String);

impl Drop for PreemptGuard {
    fn drop(&mut self) {
        let mut m = PASS_PREEMPT.lock().unwrap();
        if let Some(n) = m.get_mut(&self.0) {
            *n -= 1;
            if *n == 0 {
                m.remove(&self.0);
            }
        }
    }
}

/// Tenants with a compaction pass running now.
static PASS_TENANTS: std::sync::LazyLock<Mutex<HashMap<String, usize>>> = std::sync::LazyLock::new(Default::default);

/// Whether the maintenance lock of `id` may be held by a compaction pass only (a pass runs and nothing asked it to stop):
/// the WAL-only GC of an open writer then runs without the lock. A compaction never touches the WAL directory, and a pass
/// can last as long as a merge of sorted runs (hours on a large database): waiting for it, the WAL objects of a database
/// written all along would pile up for that long.
fn only_compaction_holds(id: &str) -> bool {
    PASS_TENANTS.lock().unwrap().contains_key(id) && !PASS_PREEMPT.lock().unwrap().contains_key(id)
}

fn pass_preempted(id: &str, hooks: &PassHooks) -> bool {
    hooks.stop.as_ref().is_some_and(|s| s.load(std::sync::atomic::Ordering::Relaxed)) || PASS_PREEMPT.lock().unwrap().contains_key(id)
}

/// What a pass tells the maintenance loop while it runs (`Maintenance` gives one to each pass it starts).
#[derive(Default)]
pub struct PassHooks {
    /// Called whenever the pass sees its L0 below the threshold (waiters of the queue are told at once, not when the merges
    /// of sorted runs that follow are over).
    pub l0_done: Option<Box<dyn Fn(usize) + Send + Sync>>,
    /// Called once, when the pass gives back its maintenance slot (see `COMPACTION_SLOT_HOLD`).
    pub release_slot: Option<Box<dyn FnOnce() + Send>>,
    /// Set when the maintenance shuts down: the pass stops.
    pub stop: Option<Arc<std::sync::atomic::AtomicBool>>,
}

/// Compacted-SST bytes and requests the pass's compactor made so far (reading its sources, writing its output): its progress.
fn sst_progress(c: &CountingStore) -> u64 {
    use std::sync::atomic::Ordering::Relaxed;
    c.counters.get_cat[2].load(Relaxed) + c.counters.put_cat[2].load(Relaxed) + c.counters.multipart_part.load(Relaxed) + c.counters.put_bytes.load(Relaxed)
}

/// Whether some compaction of the tenant is still submitted, scheduled or running.
async fn compactions_active(raw: &Arc<dyn ObjectStore>, id: &str) -> Result<bool> {
    let admin = Admin::builder(tenant_path(id), Arc::clone(raw)).build();
    Ok(admin.read_compactions(None).await?.is_some_and(|c| c.recent_compactions().any(|x| x.active())))
}

/// One compaction pass; with `warm`, the output that replaces blocks an open writer has in the shared block cache is read
/// into that cache before the writer switches to the new manifest (`warm.rs`).
pub(crate) async fn compact_tenant_pass(
    s3: &Arc<dyn ObjectStore>,
    raw: &Arc<dyn ObjectStore>,
    id: impl std::fmt::Display,
    stall: Duration,
    stop_limit: Duration,
    warm: bool,
) -> Result<Option<CompactionRecord>> {
    compact_tenant_pass_with(s3, raw, id, stall, stop_limit, warm, PassHooks::default()).await
}

/// One compaction pass (see the module comment and `compact_tenant_pass`).
///
/// The pass runs a standalone compactor until the tenant's L0 is below the threshold and no compaction of the tenant is
/// still in flight. It used to be stopped after 30 s whatever it was doing: a merge of sorted runs that needs longer (a
/// large database, a slow store, a busy machine) was stopped every time, started again from the beginning at the next
/// pass (slatedb keeps no progress below one output SST of 256 MB), left its partial output in the store, and the L0
/// compaction that ran beside it was lost too; the failed pass then paused the tenant's compaction (up to 60 s), its L0
/// reached `l0_max_ssts` and its writes stopped. Now a pass ends:
/// - when its work is done (L0 below the threshold, no compaction active): success;
/// - when its compactor moved no compacted-SST byte for `stall`: failure (the store does not answer);
/// - when it is preempted (`preempt_compaction`) or the maintenance shuts down: not a failure.
/// A merge of sorted runs is never stopped while it makes progress; once the L0 is below the threshold and the pass has
/// held its maintenance slot for `COMPACTION_SLOT_HOLD`, it gives the slot back (`PassHooks::release_slot`).
pub(crate) async fn compact_tenant_pass_with(
    s3: &Arc<dyn ObjectStore>,
    raw: &Arc<dyn ObjectStore>,
    id: impl std::fmt::Display,
    stall: Duration,
    stop_limit: Duration,
    warm: bool,
    hooks: PassHooks,
) -> Result<Option<CompactionRecord>> {
    let id = id.to_string();
    let id = id.as_str();
    if crate::fx::compactor::is_active(id) {
        // compaction 2, step 4: this database's compactor runs SlateDB's compaction (the fold, or the gated tiered
        // compaction with the drop off); two compactors on one database would fence each other
        return Ok(None);
    }
    let l0_before = l0_count(raw, id).await?;
    if l0_before < L0_COMPACT_THRESHOLD {
        // The L0 watch queued this pass from the writer's own view. When the stored manifest has fewer L0 SSTs, the writer
        // either has not stored its newest manifest yet (the watch asks again in a moment) or has not seen a compaction yet:
        // make it read the manifest now rather than at its next poll (`--writer-poll-secs`), its memtable flushes may be
        // waiting on that view.
        let writer = WRITER_DBS.lock().unwrap().get(id).cloned();
        if let Some(db) = writer {
            if db.manifest().l0().len() >= L0_COMPACT_THRESHOLD {
                let _ = db.refresh_manifest().await;
            }
        }
        return Ok(None);
    }
    let counting = Arc::new(CountingStore::new(Arc::clone(s3)));
    let store: Arc<dyn ObjectStore> = counting.clone();
    let cleanup_store = Arc::clone(&store);
    // What the open writer's cache can hold before the pass (no writer, no cache, or a writer built without the shared
    // cache: nothing to warm).
    let warm_from = if warm {
        let writer = WRITER_DBS.lock().unwrap().get(id).cloned();
        match (writer, shared_cache(), writer_scope(id)) {
            (Some(db), Some(cache), Some(scope)) => {
                let before = crate::warm::before(&db.manifest());
                // The warm-up gives way to the writer's flushes before its L0 is full (C9 review, E1).
                let watch = crate::warm::L0Watch { db, stop_at: writer_settings().l0_max_ssts.saturating_sub(1).max(1) };
                Some((cache, scope, before, watch))
            }
            _ => None,
        }
    } else {
        None
    };
    let mut opts = CompactorOptions::default();
    opts.poll_interval = Duration::from_millis(50);
    // No embedded worker: it would write 4 KiB blocks without compression (M29); the separate worker writes the
    // configured format (`tiered::worker_builder`).
    opts.worker = None;
    let started = Instant::now();
    let cb = CompactorBuilder::new(tenant_path(id), crate::counting::owned_background(&store, id)).with_options(opts);
    let cb = match filter_policies() {
        Some(p) => cb.with_filter_policies(p),
        None => cb,
    };
    let cb = match crate::tiered::scheduler() {
        Some(s) => cb.with_scheduler_supplier(s),
        None => cb,
    };
    let compactor = Arc::new(cb.build());
    let worker = Arc::new(crate::tiered::worker_builder(tenant_path(id).into(), crate::counting::owned_background(&store, id), filter_policies()).build().await?);
    let mut runner = {
        let c = Arc::clone(&compactor);
        let w = Arc::clone(&worker);
        tokio::spawn(async move {
            let (c, w) = tokio::join!(c.run(), w.run());
            c.and(w)
        })
    };
    let mut l0_after = l0_before;
    let mut hooks = hooks;
    struct Running(bool, String);
    impl Drop for Running {
        fn drop(&mut self) {
            PASSES_RUNNING.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            if self.0 {
                PASSES_LONG.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            }
            let mut m = PASS_TENANTS.lock().unwrap();
            if let Some(n) = m.get_mut(&self.1) {
                *n -= 1;
                if *n == 0 {
                    m.remove(&self.1);
                }
            }
        }
    }
    PASSES_RUNNING.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    *PASS_TENANTS.lock().unwrap().entry(id.to_string()).or_default() += 1;
    let mut running = Running(false, id.to_string());
    let mut last_progress = Instant::now();
    let mut progress_seen = sst_progress(&counting);
    let mut last_active_check: Option<Instant> = None;
    let (mut stalled, mut preempted) = (false, false);
    // Each poll is bounded by what is left of the stall window, and a failed poll ends the polling, not the function:
    // returning early here would leave the compactor running with nobody to stop it.
    let mut poll_err = None;
    let mut merging = false;
    let mut last_checkpoint_cleanup = Instant::now();
    loop {
        // the manifest is read often at first (a small L0 compaction ends in about a second), then every 250 ms while the L0
        // is over the threshold (writes may be waiting), then every `MERGE_POLL` while only merges are left
        let poll = if started.elapsed() < Duration::from_secs(2) {
            Duration::from_millis(25)
        } else if merging {
            MERGE_POLL
        } else {
            Duration::from_millis(250)
        };
        tokio::time::sleep(poll).await;
        let p = sst_progress(&counting);
        if p != progress_seen {
            progress_seen = p;
            last_progress = Instant::now();
        }
        if last_progress.elapsed() >= stall {
            stalled = true;
            break;
        }
        let left = stall.saturating_sub(last_progress.elapsed()).max(Duration::from_millis(100));
        match tokio::time::timeout(left, l0_count(raw, id)).await {
            Ok(Ok(n)) => l0_after = n,
            Ok(Err(e)) => {
                poll_err = Some(e);
                break;
            }
            Err(_) => {
                stalled = true;
                break;
            }
        }
        // A long pass deletes the checkpoints its compactor left that have expired (one per compaction, 15 min lifetime):
        // they would otherwise pile up in the manifest until the pass ends. A failure is left to the end of the pass.
        if last_checkpoint_cleanup.elapsed() >= Duration::from_secs(60) {
            last_checkpoint_cleanup = Instant::now();
            let admin = Admin::builder(tenant_path(id), Arc::clone(&cleanup_store)).build();
            if let Ok(Ok(cps)) = tokio::time::timeout(left, admin.list_checkpoints(None)).await {
                let now = unix_ms() as i64;
                for cp in cps.into_iter().filter(|c| c.name.is_none() && c.expire_time.is_some_and(|t| t.timestamp_millis() < now)) {
                    let _ = tokio::time::timeout(left, admin.delete_checkpoint(cp.id)).await;
                }
            }
        }
        if l0_after >= L0_COMPACT_THRESHOLD {
            // A preemption waits for the L0 to be compacted (as long as it makes progress): the work that preempts writes
            // to the database too (a restore, a fork's source) and would find its L0 full, with no pass able to run.
            merging = false;
            continue;
        }
        if let Some(f) = &hooks.l0_done {
            f(l0_after);
        }
        // Only the merges of sorted runs are left: they stop on request (they can last hours).
        if pass_preempted(id, &hooks) {
            preempted = true;
            break;
        }
        // L0 is below the threshold: the pass is over unless merges of sorted runs are still in flight, which go on
        // (stopped, they would start again from nothing at the next pass).
        if last_active_check.is_some_and(|t| t.elapsed() < MERGE_POLL) {
            continue;
        }
        last_active_check = Some(Instant::now());
        match tokio::time::timeout(left, compactions_active(raw, id)).await {
            Ok(Ok(false)) => break,
            Ok(Ok(true)) => merging = true,
            Ok(Err(e)) => {
                poll_err = Some(e);
                break;
            }
            Err(_) => {
                stalled = true;
                break;
            }
        }
        if started.elapsed() >= COMPACTION_SLOT_HOLD {
            if let Some(release) = hooks.release_slot.take() {
                release();
                running.0 = true;
                PASSES_LONG.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
    if stalled {
        PASSES_STALLED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    if preempted {
        PASSES_PREEMPTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    // `Compactor::stop` stops only the tasks `run` has already registered. Called while `run` is still starting (its first
    // reads of the manifest hang or retry while the store does not answer), it returns at once; `run` then registers its
    // tasks and waits for them forever, and this pass, its maintenance worker and the tenant's maintenance lock never end
    // (maintenance shutdown, and so SIGTERM, then waited forever). Stop again until `run` has returned.
    // Once `run` has registered its tasks, `stop` cancels them and waits for them to end, which lasts as long as the store
    // request in flight (object_store's 30 s timeout and its retries): every `stop` is bounded to 1 s too, so the whole wait
    // after the deadline is at most `stop_limit` (`COMPACTOR_STOP_LIMIT`, plus at most 2 s for the round in progress). A `stop` that hits
    // its bound is not an error: the next round stops again, and `run` returning is what ends the wait.
    let stop_once = || async {
        let w = tokio::time::timeout(Duration::from_secs(1), worker.stop()).await;
        match (tokio::time::timeout(Duration::from_secs(1), compactor.stop()).await, w) {
            (Ok(r), Ok(w)) => r.and(w).map_err(anyhow::Error::from),
            (Ok(r), Err(_)) => r.map_err(anyhow::Error::from),
            (Err(_), _) => Ok(()),
        }
    };
    let stop_until = Instant::now() + stop_limit;
    let mut stopped = stop_once().await;
    while tokio::time::timeout(Duration::from_secs(1), &mut runner).await.is_err() {
        if Instant::now() >= stop_until {
            // This ends the pass (its worker and the maintenance lock come back), not slatedb: the tasks `run` registered
            // run on their own tokio tasks and stay alive until their store request ends, as `stop` left them.
            runner.abort();
            anyhow::bail!("compactor of tenant {id} did not stop in {stop_limit:?}");
        }
        stopped = stop_once().await;
    }
    stopped?;
    if let Some(e) = poll_err {
        return Err(e);
    }
    // A standalone compactor leaves one checkpoint (15 min lifetime) per compaction. Each pins the
    // manifest version it was taken at, and through it old SSTs and WALs, so GC cannot reclaim them
    // until they expire. Nothing else takes checkpoints in this lifecycle (FollowLatest readers
    // create none), so drop them all once the pass is over. Counted in the pass's requests.
    let admin = Admin::builder(tenant_path(id), Arc::clone(&cleanup_store)).build();
    // Only the compactor's own checkpoints (named nothing, with an expiry). A checkpoint without an expiry pins the SSTs
    // a shallow fork (control layer) still reads, and a named one is a restore point or an export snapshot.
    let checkpoints: Vec<_> = admin.list_checkpoints(None).await?.into_iter().filter(|c| c.name.is_none() && c.expire_time.is_some()).collect();
    let checkpoints_deleted = checkpoints.len();
    for cp in checkpoints {
        admin.delete_checkpoint(cp.id).await?;
    }
    COMPACTED.lock().unwrap().insert(id.to_string());
    COMPACTED_SINCE_FULL.lock().unwrap().insert(id.to_string());
    // Before the writer switches to the new manifest, its cache gets the new blocks that replace the ones it had (`warm.rs`).
    // Bounded: past `COMPACTION_WARM_MS`, or once the writer's L0 is one SST from full, the warm-up stops where it is (what
    // it read stays in the cache, its reader is closed) and the writer switches anyway; on an error the reads not warmed
    // miss, as without it.
    if let Some((cache, scope, before, watch)) = &warm_from {
        let limit = Duration::from_millis(crate::warm::COMPACTION_WARM_MS.load(std::sync::atomic::Ordering::Relaxed));
        let watch = crate::warm::COMPACTION_WARM_L0_WATCH.load(std::sync::atomic::Ordering::Relaxed).then_some(watch);
        match crate::warm::warm_compaction_output(&cleanup_store, &tenant_path(id), cache, *scope, before, limit, watch).await {
            Ok(rec) => {
                if rec.timed_out {
                    eprintln!(
                        "compaction warm-up of tenant {id}: stopped after {limit:?} ({} of {} reads done; reader open at {:.2} s, hot keys at {:.2} s, plan at {:.2} s)",
                        rec.calls_done, rec.calls, rec.secs_open, rec.secs_hot, rec.secs_plan
                    );
                }
                if rec.l0_stopped {
                    eprintln!("compaction warm-up of tenant {id}: stopped at {:.2} s, the writer's L0 is one SST from full ({} of {} reads done)", rec.secs, rec.calls_done, rec.calls);
                }
            }
            Err(e) => {
                crate::warm::WARM_FAILS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                eprintln!("compaction warm-up of tenant {id}: {e}");
            }
        }
    }
    // An open writer learns of the compaction now instead of at its next manifest poll (`--writer-poll-secs`): until then it
    // still counts the compacted L0 SSTs, and a writer written fast can fill its L0 and stall its memtable flushes.
    let writer = WRITER_DBS.lock().unwrap().get(id).cloned();
    if let Some(db) = writer {
        let refreshed = db.refresh_manifest().await.is_ok();
        // The consumed SSTs are no longer in the writer's manifest: their blocks would only age out of the cache.
        if let (true, Some((_, _, before, _))) = (refreshed, &warm_from) {
            let consumed = before.consumed(&db.manifest());
            if !consumed.is_empty() {
                crate::warm::evict_consumed(&db, &consumed).await;
            }
        }
    }
    anyhow::ensure!(
        l0_after < L0_COMPACT_THRESHOLD || preempted,
        "compaction of tenant {id} made no progress for {:?} (L0 {l0_after}, pass {:.1} s)",
        stall,
        started.elapsed().as_secs_f64()
    );
    {
        use std::sync::atomic::Ordering::Relaxed;
        let snap = counting.counters.snapshot();
        MAINT.compact_runs.fetch_add(1, Relaxed);
        MAINT.compact_puts.fetch_add(snap.put + snap.multipart_create + snap.multipart_complete, Relaxed);
        MAINT.compact_gets.fetch_add(snap.get + snap.get_range + snap.head, Relaxed);
    }
    Ok(Some(CompactionRecord {
        tenant: id.to_string(),
        l0_before,
        l0_after,
        checkpoints_deleted,
        secs: started.elapsed().as_secs_f64(),
        requests: counting.counters.snapshot(),
    }))
}

pub async fn gc_tenant(s3: &Arc<dyn ObjectStore>, id: impl std::fmt::Display, min_age: Duration) -> Result<GcRecord> {
    let counting = Arc::new(CountingStore::new(Arc::clone(s3)));
    let store: Arc<dyn ObjectStore> = counting.clone();
    let dir = GarbageCollectorDirectoryOptions { interval: None, min_age, dry_run: false };
    let wal_dir = GarbageCollectorDirectoryOptions { min_age: min_age.max(WAL_RETAIN.get().map(|f| f(&id.to_string())).unwrap_or_default()), ..dir };
    let opts = GarbageCollectorOptions {
        manifest_options: Some(dir),
        wal_options: Some(wal_dir),
        compacted_options: Some(dir),
        compactions_options: Some(dir),
        ..GarbageCollectorOptions::default()
    };
    Admin::builder(tenant_path(&id), store).build().run_gc_once(opts).await?;
    {
        use std::sync::atomic::Ordering::Relaxed;
        let snap = counting.counters.snapshot();
        MAINT.gc_runs.fetch_add(1, Relaxed);
        MAINT.gc_deletes.fetch_add(snap.delete, Relaxed);
        MAINT.gc_lists.fetch_add(snap.list, Relaxed);
        MAINT.gc_gets.fetch_add(snap.get + snap.get_range + snap.head, Relaxed);
    }
    Ok(GcRecord { tenant: id.to_string(), requests: counting.counters.snapshot() })
}

/// Tenants compacted since their last full close-time pass, and the manifest version that pass saw.
static FULL_GC: std::sync::LazyLock<Mutex<HashMap<String, u64>>> = std::sync::LazyLock::new(Default::default);
static COMPACTED_SINCE_FULL: std::sync::LazyLock<Mutex<std::collections::HashSet<String>>> = std::sync::LazyLock::new(Default::default);
/// Close-time passes that skipped slatedb's full pass (nothing could have become garbage outside the WAL).
pub static CLOSE_GC_LIGHT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The pass after a tenant closed (`Maintenance`, with `--wal-gc-mode` bulk or single). The WAL is cleaned with the bounded
/// pass (latest manifest: one LIST and one GET). slatedb's own pass over the manifests, compacted SSTs and compaction states
/// lists those directories and reads the manifest of every checkpoint (a database with a retention window has one per restore
/// point: hundreds after a few days), so it runs only when something there can be garbage: after a compaction of this
/// process, once the manifest moved `SST_GC_MANIFEST_STEP` versions since the last full pass, or the first time this
/// process closes the tenant.
pub async fn gc_tenant_closed(s3: &Arc<dyn ObjectStore>, id: impl std::fmt::Display, min_age: Duration) -> Result<GcRecord> {
    use std::sync::atomic::Ordering::Relaxed;
    let id = id.to_string();
    let mode = WAL_GC_MODE.load(Relaxed);
    if mode == 0 || mode == 1 {
        return gc_tenant(s3, &id, min_age).await;
    }
    let counting = Arc::new(CountingStore::new(Arc::clone(s3)));
    let store: Arc<dyn ObjectStore> = counting.clone();
    let admin = Admin::builder(tenant_path(&id), Arc::clone(&store)).build();
    let Some(m) = admin.read_manifest(None).await? else { return Ok(GcRecord { tenant: id, requests: counting.counters.snapshot() }) };
    gc_wal_with(&store, &id, mode, &m).await?;
    let last_full = FULL_GC.lock().unwrap().get(&id).copied();
    let compacted = COMPACTED_SINCE_FULL.lock().unwrap().remove(&id);
    if compacted || last_full.map_or(true, |v| m.id() >= v + SST_GC_MANIFEST_STEP) {
        let dir = GarbageCollectorDirectoryOptions { interval: None, min_age, dry_run: false };
        let opts = GarbageCollectorOptions {
            manifest_options: Some(dir),
            wal_options: None,
            compacted_options: Some(dir),
            compactions_options: Some(dir),
            ..GarbageCollectorOptions::default()
        };
        admin.run_gc_once(opts).await?;
        FULL_GC.lock().unwrap().insert(id.clone(), m.id());
    } else {
        CLOSE_GC_LIGHT.fetch_add(1, Relaxed);
    }
    let snap = counting.counters.snapshot();
    MAINT.gc_runs.fetch_add(1, Relaxed);
    MAINT.gc_deletes.fetch_add(snap.delete, Relaxed);
    MAINT.gc_lists.fetch_add(snap.list, Relaxed);
    MAINT.gc_gets.fetch_add(snap.get + snap.get_range + snap.head, Relaxed);
    Ok(GcRecord { tenant: id, requests: snap })
}

/// Delete the WAL objects of an open writer that its latest manifest no longer needs (everything before
/// `replay_after_wal_id`, which advances each time the writer flushes its memtable to an L0 SST). Only the WAL
/// directory is touched: the writer is live, so manifests, SSTs and compactions are left alone.
async fn gc_tenant_wal_slatedb(s3: &Arc<dyn ObjectStore>, id: impl std::fmt::Display, min_age: Duration) -> Result<GcRecord> {
    let counting = Arc::new(CountingStore::new(Arc::clone(s3)));
    let store: Arc<dyn ObjectStore> = counting.clone();
    let opts = GarbageCollectorOptions {
        manifest_options: None,
        wal_options: Some(GarbageCollectorDirectoryOptions { interval: None, min_age, dry_run: false }),
        compacted_options: None,
        compactions_options: None,
        ..GarbageCollectorOptions::default()
    };
    Admin::builder(tenant_path(&id), store).build().run_gc_once(opts).await?;
    {
        use std::sync::atomic::Ordering::Relaxed;
        let snap = counting.counters.snapshot();
        MAINT.gc_runs.fetch_add(1, Relaxed);
        MAINT.gc_deletes.fetch_add(snap.delete, Relaxed);
        MAINT.gc_lists.fetch_add(snap.list, Relaxed);
        MAINT.gc_gets.fetch_add(snap.get + snap.get_range + snap.head, Relaxed);
    }
    Ok(GcRecord { tenant: id.to_string(), requests: counting.counters.snapshot() })
}

/// How the WAL-only pass of an open writer works (`--wal-gc-mode`): 0 slatedb's own pass (lists the whole `wal/`
/// directory every time and sends one DELETE per object), 1 the same listing without deleting (profiling only),
/// 2 incremental with one DELETE request per object, 3 incremental with bulk deletes (default).
pub static WAL_GC_MODE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(3);
/// Passes made, objects deleted and microseconds spent (listing, deleting, whole pass), for `tenantStats`.
pub static WAL_GC_PASSES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static WAL_GC_DELETED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static WAL_GC_LIST_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static WAL_GC_DELETE_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static WAL_GC_TOTAL_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Passes that found nothing to delete and made no request (the writer's boundary had not moved past what was already deleted).
pub static WAL_GC_NOOP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// The object store returns listings in key order (S3 and S3-compatible stores do; the local-directory store does not).
/// Then the lowest WAL id is the first key of the first page: finding it costs one LIST whatever the history.
pub static LIST_SORTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Per tenant: the first WAL id that may still exist (everything below was deleted by an earlier pass), and the oldest
/// checkpoint seen with its WAL replay boundary (a checkpoint's manifest never changes, so it is read once).
#[derive(Default, Clone)]
struct WalGcState {
    floor: Option<u64>,
    oldest_cp: Option<(u128, u64)>,
}
static WAL_STATE: std::sync::LazyLock<Mutex<HashMap<String, WalGcState>>> = std::sync::LazyLock::new(Default::default);

/// Forget what this process knows about a tenant's WAL (the database was dropped: its WAL ids start again).
/// Writer epoch of the tenant's latest stored manifest (None: no manifest).
/// The number of the next version of our manifest above `cur` (the newest in the store, plus one).
async fn manifest_next(store: &Arc<dyn ObjectStore>, id: &str, cur: u64) -> Result<u64> {
    Ok(crate::fx::manifest::list_versions(store, id).await?.last().copied().unwrap_or(0).max(cur) + 1)
}

pub async fn stored_writer_epoch(store: &Arc<dyn ObjectStore>, id: &str) -> Result<Option<u64>> {
    let admin = Admin::builder(tenant_path(id), Arc::clone(store)).build();
    Ok(admin.read_manifest(None).await?.map(|m| m.writer_epoch()))
}

pub fn forget_wal_state(id: &str) {
    WAL_STATE.lock().unwrap().remove(id);
    STORED_MANIFEST.lock().unwrap().remove(id);
    FULL_GC.lock().unwrap().remove(id);
}

fn wal_object_path(id: &str, n: u64) -> Path {
    Path::from(format!("{}/wal/{:020}.sst", tenant_path(id), n))
}

/// Delete the WAL objects of an open writer that nothing can need any more.
///
/// A WAL object `n` is needed while `n >= replay_after_wal_id` of the live database (its data is not yet in an L0 SST) or
/// while some checkpoint's manifest has a replay boundary `<= n` (restoring or cloning that checkpoint replays it). Restore
/// points are checkpoints, so for a database with a retention window this keeps every WAL object from the oldest restore
/// point on: everything a point-in-time restore can use, which is the WAL of the window. Objects below the oldest
/// checkpoint can serve no restore (there is no point to start from) and are deleted.
///
/// The pass reads the writer's own view of the manifest (no request). That view can be up to `--writer-poll-secs`
/// behind the store, but only for checkpoints taken meanwhile, and a checkpoint taken after the view was read has a
/// replay boundary at least the view's (boundaries only grow with the manifest version), so nothing it needs is below
/// the limit used here. The boundary of the oldest checkpoint costs one GET, once per checkpoint. The lowest existing
/// WAL id is found once per tenant and process (one LIST on a sorted store) and remembered. A pass with nothing new
/// to delete makes no request; one that deletes makes one bulk delete per 1000 objects. So the cost of a pass depends on
/// how many objects became deletable, not on how long the history is (the slatedb pass, `--wal-gc-mode slatedb`, lists
/// the whole WAL directory and reads every manifest at every pass).
pub async fn gc_tenant_wal(s3: &Arc<dyn ObjectStore>, id: impl std::fmt::Display, min_age: Duration) -> Result<GcRecord> {
    use std::sync::atomic::Ordering::Relaxed;
    let t0 = Instant::now();
    let id = id.to_string();
    let mode = WAL_GC_MODE.load(Relaxed);
    let rec = if mode == 0 {
        // slatedb's own pass has no idea of checkpoints-as-restore-points beyond what it reads, so a database with a retention
        // window (control layer, `WAL_RETAIN`) keeps its WAL objects for that long by object age.
        let retain = WAL_RETAIN.get().map(|f| f(&id)).unwrap_or_default();
        // Negative control of the test suite (`ctl-tests/test_09f_wal_retention_guard.py`): with this variable set the age guard
        // is off and the slatedb pass deletes WAL objects that restores need. Never set in production.
        let retain = if std::env::var_os("MONGO_TEST_DISABLE_WAL_RETENTION_GUARD").is_some() { Duration::ZERO } else { retain };
        gc_tenant_wal_slatedb(s3, &id, min_age.max(retain)).await?
    } else {
        let counting = Arc::new(CountingStore::new(Arc::clone(s3)));
        let store: Arc<dyn ObjectStore> = counting.clone();
        gc_wal_incremental(&store, &id, mode).await?;
        let snap = counting.counters.snapshot();
        MAINT.gc_runs.fetch_add(1, Relaxed);
        MAINT.gc_deletes.fetch_add(snap.delete, Relaxed);
        MAINT.gc_lists.fetch_add(snap.list, Relaxed);
        MAINT.gc_gets.fetch_add(snap.get + snap.get_range + snap.head, Relaxed);
        GcRecord { tenant: id.clone(), requests: snap }
    };
    WAL_GC_PASSES.fetch_add(1, Relaxed);
    WAL_GC_TOTAL_US.fetch_add(t0.elapsed().as_micros() as u64, Relaxed);
    Ok(rec)
}

/// Lowest WAL id that exists (u64::MAX if none).
async fn wal_list_min(store: &Arc<dyn ObjectStore>, id: &str) -> Result<u64> {
    use std::sync::atomic::Ordering::Relaxed;
    let t = Instant::now();
    let wal_dir = Path::from(format!("{}/wal", tenant_path(id)));
    let sorted = LIST_SORTED.load(Relaxed);
    let mut min = u64::MAX;
    let mut it = store.list(Some(&wal_dir));
    while let Some(meta) = it.next().await {
        let meta = meta?;
        if let Some(n) = meta.location.filename().and_then(|f| f.strip_suffix(".sst")).and_then(|f| f.parse::<u64>().ok()) {
            min = min.min(n);
            if sorted {
                break; // keys are zero-padded numbers: the first one is the lowest
            }
        }
    }
    WAL_GC_LIST_US.fetch_add(t.elapsed().as_micros() as u64, Relaxed);
    Ok(min)
}

async fn gc_wal_incremental(store: &Arc<dyn ObjectStore>, id: &str, mode: u8) -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    if mode == 1 {
        // profiling only: the full listing, nothing deleted
        let wal_dir = Path::from(format!("{}/wal", tenant_path(id)));
        let mut it = store.list(Some(&wal_dir));
        while let Some(m) = it.next().await {
            m?;
        }
        return Ok(());
    }
    // A process that no longer owns the tenant (its lease lapsed, another process may be replaying the WAL) deletes nothing.
    if !owner_valid(id) {
        WAL_GC_NOT_OWNER.fetch_add(1, Relaxed);
        return Ok(());
    }
    // The writer's in-memory manifest is NOT what is stored: slatedb's memtable flusher moves its replay boundary before the
    // manifest PUT (which may still fail, be retried for minutes, or be fenced). Only the version id is trustworthy: it changes
    // after a successful write, and a manifest version never changes once stored. So the pass uses the STORED manifest of that
    // id, read once per version (one GET); a crash before the next PUT then recovers from a manifest whose WAL still exists.
    // No open writer (it closed meanwhile): the close-time pass collects the tenant.
    let Some(mid) = WRITER_DBS.lock().unwrap().get(id).map(|db| db.manifest().id()) else {
        WAL_GC_NOOP.fetch_add(1, Relaxed);
        return Ok(());
    };
    let cached = STORED_MANIFEST.lock().unwrap().get(id).filter(|(v, _)| *v == mid).map(|(_, m)| Arc::clone(m));
    let m = match cached {
        Some(m) => m,
        None => {
            let admin = Admin::builder(tenant_path(id), Arc::clone(store)).build();
            let Some(m) = admin.read_manifest(Some(mid)).await? else {
                WAL_GC_NOOP.fetch_add(1, Relaxed);
                return Ok(());
            };
            let m = Arc::new(m);
            STORED_MANIFEST.lock().unwrap().insert(id.to_string(), (mid, Arc::clone(&m)));
            m
        }
    };
    if !owner_valid(id) {
        WAL_GC_NOT_OWNER.fetch_add(1, Relaxed);
        return Ok(());
    }
    gc_wal_with(store, id, mode, &m).await
}

/// The stored manifest the hot WAL pass last read, per tenant: (version id, manifest).
static STORED_MANIFEST: std::sync::LazyLock<Mutex<HashMap<String, (u64, Arc<slatedb::VersionedManifest>)>>> = std::sync::LazyLock::new(Default::default);
/// Hot WAL passes skipped because this process no longer held the tenant.
pub static WAL_GC_NOT_OWNER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Local-log writes not acknowledged because ownership was lost by the time their record was on disk.
pub static LOCAL_ACK_REFUSED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Set by the server: whether this process still owns a tenant (its lease is valid). Unset (tests of this crate): always.
pub static OWNER_VALID: std::sync::OnceLock<Box<dyn Fn(&str) -> bool + Send + Sync>> = std::sync::OnceLock::new();
fn owner_valid(id: &str) -> bool {
    OWNER_VALID.get().map_or(true, |f| f(id))
}

/// The bounded WAL pass of [`gc_wal_incremental`] with a given view of the manifest.
async fn gc_wal_with(store: &Arc<dyn ObjectStore>, id: &str, mode: u8, m: &slatedb::VersionedManifest) -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    let boundary = m.replay_after_wal_id();
    let mut st = WAL_STATE.lock().unwrap().get(id).cloned().unwrap_or_default();
    if st.floor.map_or(false, |f| boundary <= f) {
        WAL_GC_NOOP.fetch_add(1, Relaxed);
        return Ok(());
    }
    let mut limit = boundary;
    // With a retention window (`WAL_RETAIN`) the restore points are checkpoints and keep the WAL from the oldest one on.
    // Without one the WAL below the replay boundary goes, as before: a restore that would need it answers retention_gap.
    // Replay boundaries only grow with the manifest version, so the checkpoint on the oldest manifest has the lowest one.
    let retained = WAL_RETAIN.get().map_or(false, |f| !f(id).is_zero());
    if retained && m.checkpoints().is_empty() {
        // Retention just switched on and its first restore point is not taken yet: keep everything until it is.
        WAL_GC_NOOP.fetch_add(1, Relaxed);
        return Ok(());
    }
    if let Some(cp) = m.checkpoints().iter().filter(|_| retained).min_by_key(|c| c.manifest_id) {
        let r = match st.oldest_cp {
            Some((u, r)) if u == cp.id.as_u128() => r,
            _ => {
                let admin = Admin::builder(tenant_path(id), Arc::clone(store)).build();
                match admin.read_manifest(Some(cp.manifest_id)).await? {
                    Some(cm) => cm.replay_after_wal_id(),
                    None => return Ok(()), // cannot tell what the checkpoint needs: delete nothing
                }
            }
        };
        st.oldest_cp = Some((cp.id.as_u128(), r));
        limit = limit.min(r);
    }
    let floor = match st.floor {
        Some(f) => f,
        // First pass for this tenant in this process: the oldest object that still exists.
        None => wal_list_min(store, id).await?.min(limit),
    };
    if floor < limit {
        let paths: Vec<Path> = (floor..limit).map(|n| wal_object_path(id, n)).collect();
        let t = Instant::now();
        let n = paths.len() as u64;
        if mode == 2 {
            futures::stream::iter(paths)
                .for_each_concurrent(8, |p| async move {
                    let _ = store.delete(&p).await;
                })
                .await;
        } else {
            let mut del = store.delete_stream(futures::stream::iter(paths.into_iter().map(Ok)).boxed());
            while let Some(r) = del.next().await {
                match r {
                    Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }
        WAL_GC_DELETED.fetch_add(n, Relaxed);
        WAL_GC_DELETE_US.fetch_add(t.elapsed().as_micros() as u64, Relaxed);
    } else if st.floor.is_some() {
        WAL_GC_NOOP.fetch_add(1, Relaxed);
    }
    st.floor = Some(floor.max(limit));
    WAL_STATE.lock().unwrap().insert(id.to_string(), st);
    Ok(())
}

/// Seconds between cleanups of the compacted SSTs and old manifests of an open writer (0 = never; they then stay
/// until the tenant closes and is collected as a whole).
pub static SST_GC_EVERY_SECS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(60);
/// An object that the latest manifest does not reference is deleted only after this process has seen it unreferenced
/// for this long. A live writer keeps reading the SSTs of the manifest it last loaded (it re-reads the manifest every
/// `--writer-poll-secs`) so an SST that a compaction has just replaced must outlive that view; the clock starts
/// when a pass first sees the object unreferenced, so a restart only delays deletion.
pub static SST_GC_MIN_UNREF_SECS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(120);
/// Negative control for the tests: also delete the oldest SST that the manifest *does* reference.
pub static GC_CONTROL_DELETE_LIVE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
pub static SST_GC_PASSES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static SST_GC_DELETED_SSTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static SST_GC_DELETED_MANIFESTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Per tenant: object name (`compacted/...`, `manifest/...`) -> when a pass first saw it unreferenced.
static SST_UNREF: std::sync::LazyLock<Mutex<HashMap<String, HashMap<String, Instant>>>> = std::sync::LazyLock::new(Default::default);

fn unix_ms() -> u128 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
}

/// Delete the compacted SSTs and old manifests of an open writer that nothing can reference any more: the latest
/// manifest does not name them, no checkpoint exists, and a previous pass saw them unreferenced at least
/// `SST_GC_MIN_UNREF_SECS` ago (longer than the writer's view of the manifest can be stale). Runs under the tenant's
/// maintenance lock, so no compaction of this process is writing a new SST meanwhile; a flush of the writer that
/// uploaded an SST but has not yet put it in the manifest looks unreferenced too and is covered by the same wait.
/// Returns true when the pass saw unreferenced objects that are not old enough yet (another pass is due later).
pub async fn gc_tenant_ssts(s3: &Arc<dyn ObjectStore>, id: &str) -> Result<bool> {
    use std::sync::atomic::Ordering::Relaxed;
    eprintln!("SSTGC begin tenant={id} unix_ms={}", unix_ms());
    let admin = Admin::builder(tenant_path(id), Arc::clone(s3)).build();
    let Some(m) = admin.read_manifest(None).await? else { return Ok(false) };
    // (the checkpoints are in the manifest just read: no second read)
    // The checkpoints of a running compaction pass (named nothing, with an expiry) do not stop the cleanup: the pass deletes
    // them when it ends anyway (`compact_tenant_pass`), and a pass can last as long as a merge of sorted runs (hours on a
    // large database) while the SSTs it already replaced pile up. Their manifests are kept until then. Any other
    // checkpoint (a fork's pin, a restore point, an export snapshot) still stops it.
    let foreign = m.checkpoints().iter().any(|c| c.name.is_some() || c.expire_time.is_none());
    if !m.segments().is_empty() || foreign {
        eprintln!("SSTGC skip tenant={id} (segments or checkpoints present)");
        return Ok(false);
    }
    let pinned_manifests: std::collections::HashSet<u64> = m.checkpoints().iter().map(|c| c.manifest_id).collect();
    let latest = m.id();
    let mut referenced = std::collections::HashSet::new();
    for v in m.l0() {
        referenced.insert(v.sst.id.value().to_string());
    }
    for run in m.compacted() {
        for v in run.sst_views() {
            referenced.insert(v.sst.id.value().to_string());
        }
    }
    // The output an unfinished compaction has already recorded is kept: a later pass resumes the compaction from it.
    if let Some(c) = admin.read_compactions(None).await? {
        for x in c.recent_compactions().filter(|x| x.active()) {
            for h in x.output_ssts() {
                referenced.insert(h.id.value().to_string());
            }
            for sub in x.ctx().map(|ctx| ctx.subcompactions().as_slice()).unwrap_or_default() {
                for h in sub.output_ssts() {
                    referenced.insert(h.id.value().to_string());
                }
            }
        }
    }
    let min_unref = Duration::from_secs(SST_GC_MIN_UNREF_SECS.load(Relaxed));
    let now = Instant::now();
    let base = tenant_path(id);
    // List first (no lock held across the awaits).
    let mut listed: Vec<(&str, object_store::ObjectMeta)> = Vec::new();
    for dir in ["compacted", "manifest", "compactions"] {
        let prefix = Path::from(format!("{base}/{dir}"));
        let mut it = s3.list(Some(&prefix));
        while let Some(meta) = it.next().await {
            listed.push((dir, meta?));
        }
    }
    // Numbered state objects (manifests, compaction states): the highest number is the live one.
    let number = |file: &str| file.split('.').next().and_then(|n| n.parse::<u64>().ok());
    let max_compactions = listed.iter().filter(|(d, _)| *d == "compactions").filter_map(|(_, m)| m.location.filename().and_then(number)).max().unwrap_or(0);
    let mut doomed: Vec<Path> = Vec::new();
    let (mut n_ssts, mut n_manifests) = (0u64, 0u64);
    let mut live_old: Vec<(i64, Path)> = Vec::new();
    let mut waiting = 0u64;
    {
        let mut track = SST_UNREF.lock().unwrap();
        let t = track.entry(id.to_string()).or_default();
        let mut seen = std::collections::HashSet::new();
        for (dir, meta) in listed {
            let Some(file) = meta.location.filename().map(str::to_string) else { continue };
            let key = format!("{dir}/{file}");
            seen.insert(key.clone());
            let live = if dir == "compacted" {
                file.strip_suffix(".sst").map_or(true, |u| referenced.contains(u))
            } else if dir == "compactions" {
                number(&file).map_or(true, |n| n >= max_compactions)
            } else {
                // Manifests: the latest is live; older ones are not needed by the writer or by this process's compactor.
                file.strip_suffix(".manifest").and_then(|n| n.parse::<u64>().ok()).map_or(true, |n| n >= latest || pinned_manifests.contains(&n))
            };
            if live {
                t.remove(&key);
                if dir == "compacted" {
                    live_old.push((meta.last_modified.timestamp(), meta.location.clone()));
                }
                continue;
            }
            let first = *t.entry(key).or_insert(now);
            if now.duration_since(first) < min_unref {
                waiting += 1;
            }
            if now.duration_since(first) >= min_unref {
                if dir == "compacted" { n_ssts += 1 } else { n_manifests += 1 }
                doomed.push(meta.location);
            }
        }
        t.retain(|k, _| seen.contains(k));
    }
    if GC_CONTROL_DELETE_LIVE.load(Relaxed) {
        // Negative control: also delete the oldest referenced SST that is at least 30 s old.
        live_old.sort();
        let now_s = (unix_ms() / 1000) as i64;
        if let Some((lm, p)) = live_old.into_iter().find(|(lm, _)| now_s - *lm >= 30) {
            eprintln!("SSTGC CONTROL deleting a referenced SST {p} (modified {lm})");
            doomed.push(p);
        }
    }
    if !doomed.is_empty() {
        let mut del = s3.delete_stream(futures::stream::iter(doomed.into_iter().map(Ok)).boxed());
        while let Some(r) = del.next().await {
            match r {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
    SST_GC_PASSES.fetch_add(1, Relaxed);
    SST_GC_DELETED_SSTS.fetch_add(n_ssts, Relaxed);
    SST_GC_DELETED_MANIFESTS.fetch_add(n_manifests, Relaxed);
    eprintln!("SSTGC end tenant={id} unix_ms={} deleted_ssts={n_ssts} deleted_manifests={n_manifests} referenced={} waiting={waiting}", unix_ms(), referenced.len());
    Ok(waiting > 0)
}

/// GC passes skipped because the tenant's maintenance lock was held (a compaction pass): they run at a later round.
pub static GC_SKIPPED_BUSY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Errors kept in `MaintenanceLog::errors` (the oldest are dropped; `errors_total` counts them all).
pub const MAINT_ERRORS_KEPT: usize = 1000;

#[derive(Default)]
pub struct MaintenanceLog {
    pub compactions: Vec<CompactionRecord>,
    pub gcs: Vec<GcRecord>,
    /// The last `MAINT_ERRORS_KEPT` errors, oldest first.
    pub errors: std::collections::VecDeque<String>,
    /// Every error since the start, kept or not.
    pub errors_total: u64,
}

impl MaintenanceLog {
    pub fn error(&mut self, e: String) {
        if self.errors.len() >= MAINT_ERRORS_KEPT {
            self.errors.pop_front();
        }
        self.errors.push_back(e);
        self.errors_total += 1;
    }
}

pub struct Maintenance {
    pub log: Arc<Mutex<MaintenanceLog>>,
    queue: Arc<CompactionQueue>,
    dispatcher: tokio::task::JoinHandle<()>,
    watch_stop: Arc<std::sync::atomic::AtomicBool>,
    l0_watch: tokio::task::JoinHandle<()>,
    gc_stop: Arc<std::sync::atomic::AtomicBool>,
    gc_task: tokio::task::JoinHandle<()>,
    workers: usize,
    sem: Arc<Semaphore>,
    /// Set by `shutdown`: running passes stop. `passes`: pass tasks not finished yet.
    stopping: Arc<std::sync::atomic::AtomicBool>,
    passes: Arc<std::sync::atomic::AtomicU64>,
}

impl Maintenance {
    /// Start the compaction dispatcher (`workers` concurrent passes) and a GC loop that
    /// collects a tenant `gc_after` after its last write, with `gc_min_age` file age.
    pub fn start(
        s3: Arc<dyn ObjectStore>,
        raw: Arc<dyn ObjectStore>,
        queue: Arc<CompactionQueue>,
        mut rx: mpsc::UnboundedReceiver<Option<String>>,
        touched: Arc<Mutex<HashMap<String, Instant>>>,
        locks: MaintLocks,
        workers: usize,
        gc_after: Duration,
        gc_min_age: Duration,
    ) -> Self {
        let log = Arc::new(Mutex::new(MaintenanceLog::default()));
        let sem = Arc::new(Semaphore::new(workers));
        let stopping = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let passes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let dispatcher = {
            let (s3, raw, queue, log, sem, locks, stopping, passes) = (s3.clone(), raw.clone(), queue.clone(), log.clone(), sem.clone(), locks.clone(), stopping.clone(), passes.clone());
            tokio::spawn(async move {
                while let Some(Some(id)) = rx.recv().await {
                    let permit = sem.clone().acquire_owned().await.unwrap();
                    let (s3, raw, queue, log, locks, stopping) = (s3.clone(), raw.clone(), queue.clone(), log.clone(), locks.clone(), stopping.clone());
                    passes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let passes = passes.clone();
                    tokio::spawn(async move {
                        let lock = maint_lock(&locks, &id);
                        let _held = lock.lock().await;
                        // The slot goes back to the dispatcher when the pass becomes a long merge, or when it ends.
                        let slot = Arc::new(Mutex::new(Some(permit)));
                        let hooks = {
                            let (q, qid, slot) = (queue.clone(), id.clone(), slot.clone());
                            PassHooks {
                                l0_done: Some(Box::new(move |l0| q.notify(&qid, l0))),
                                release_slot: Some(Box::new(move || drop(slot.lock().unwrap().take()))),
                                stop: Some(stopping),
                            }
                        };
                        let warm = crate::warm::COMPACTION_WARM.load(std::sync::atomic::Ordering::Relaxed);
                        let l0_after = match compact_tenant_pass_with(&s3, &raw, &id, COMPACTION_STALL, COMPACTOR_STOP_LIMIT, warm, hooks).await {
                            Ok(Some(rec)) => {
                                compaction_succeeded(&id);
                                let l0 = rec.l0_after;
                                log.lock().unwrap().compactions.push(rec);
                                l0
                            }
                            Ok(None) => {
                                compaction_succeeded(&id);
                                l0_count(&raw, &id).await.unwrap_or(0)
                            }
                            Err(e) => {
                                compaction_failed(&id, Instant::now());
                                eprintln!("compaction of tenant {id} failed: {e:#}");
                                log.lock().unwrap().error(format!("compact {id}: {e:#}"));
                                usize::MAX / 2
                            }
                        };
                        queue.finish(&id, l0_after);
                        drop(slot.lock().unwrap().take());
                        passes.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    });
                }
            })
        };
        let watch_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let l0_watch = {
            let (queue, stop) = (queue.clone(), watch_stop.clone());
            tokio::spawn(async move {
                use std::sync::atomic::Ordering::Relaxed;
                while !stop.load(Relaxed) {
                    tokio::time::sleep(Duration::from_millis(L0_WATCH_MS.load(Relaxed).max(10))).await;
                    if stop.load(Relaxed) {
                        break;
                    }
                    for id in writers_over_l0_threshold() {
                        if queue.request(&id, None) {
                            L0_WATCH_REQUESTS.fetch_add(1, Relaxed);
                        }
                    }
                }
            })
        };
        let gc_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let gc_task = {
            let (s3, log, stop, locks) = (s3.clone(), log.clone(), gc_stop.clone(), locks.clone());
            tokio::spawn(async move {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    flush_old_memtables();
                    let due: Vec<String> = {
                        let mut t = touched.lock().unwrap();
                        let open = OPEN_WRITERS.lock().unwrap();
                        // A writer still open is collected by the WAL-only pass below and again when it closes.
                        let due: Vec<String> =
                            t.iter().filter(|(id, at)| at.elapsed() >= gc_after && !open.contains(*id)).map(|(id, _)| id.clone()).collect();
                        for id in &due {
                            t.remove(id);
                        }
                        due
                    };
                    let every = Duration::from_secs(HOT_GC_EVERY_SECS.load(std::sync::atomic::Ordering::Relaxed));
                    let hot_due: Vec<String> = if every.is_zero() {
                        Vec::new()
                    } else {
                        let mut m = GC_HOT.lock().unwrap();
                        m.retain(|_, h| h.last_write.elapsed() < every * 3);
                        let now = Instant::now();
                        m.iter_mut().filter(|(_, h)| h.last_gc.elapsed() >= every).map(|(id, h)| { h.last_gc = now; id.clone() }).collect()
                    };
                    let sst_every = Duration::from_secs(SST_GC_EVERY_SECS.load(std::sync::atomic::Ordering::Relaxed));
                    let sst_due: Vec<String> = if sst_every.is_zero() {
                        Vec::new()
                    } else {
                        let mut m = GC_HOT.lock().unwrap();
                        let now = Instant::now();
                        let mut compacted = COMPACTED.lock().unwrap();
                        let mut due = Vec::new();
                        for (id, h) in m.iter_mut().filter(|(_, h)| h.last_sst_gc.elapsed() >= sst_every) {
                            h.last_sst_gc = now;
                            // Due only when something can have become garbage: a compaction replaced SSTs, the manifest
                            // moved on (old manifests), or the last pass left objects waiting for their minimum age.
                            let mid = writer_manifest_id(id).unwrap_or(h.sst_manifest);
                            let retry = h.sst_retry_at.map_or(false, |t| t <= now);
                            if compacted.remove(id) || retry || mid >= h.sst_manifest + SST_GC_MANIFEST_STEP {
                                h.sst_manifest = mid;
                                h.sst_retry_at = None;
                                due.push(id.clone());
                            } else {
                                SST_GC_NOOP.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                        SST_UNREF.lock().unwrap().retain(|id, _| m.contains_key(id));
                        due
                    };
                    futures::stream::iter(sst_due)
                        .for_each_concurrent(GC_CONCURRENCY, |id| {
                            let (s3, log, locks) = (s3.clone(), log.clone(), locks.clone());
                            async move {
                                // A tenant whose lock is held is skipped this round (waiting here would hold up the GC of every
                                // tenant), unless the holder is a compaction pass (maybe a long merge): the SSTs it replaces
                                // must not pile up for its whole length. The SSTs the pass is writing are not referenced yet,
                                // but they are recorded by the compaction (kept) or younger than SST_GC_MIN_UNREF_SECS.
                                let lock = maint_lock(&locks, &id);
                                let held = lock.try_lock();
                                if held.is_err() && !only_compaction_holds(&id) {
                                    GC_SKIPPED_BUSY.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    return;
                                }
                                match gc_tenant_ssts(&s3, &id).await {
                                    Ok(true) => {
                                        let wait = Duration::from_secs(SST_GC_MIN_UNREF_SECS.load(std::sync::atomic::Ordering::Relaxed) + 5);
                                        if let Some(h) = GC_HOT.lock().unwrap().get_mut(&id) {
                                            h.sst_retry_at = Some(Instant::now() + wait);
                                        }
                                    }
                                    Ok(false) => {}
                                    Err(e) => log.lock().unwrap().error(format!("sst gc {id}: {e:#}")),
                                }
                            }
                        })
                        .await;
                    futures::stream::iter(hot_due)
                        .for_each_concurrent(GC_CONCURRENCY, |id| {
                            let (s3, log, locks) = (s3.clone(), log.clone(), locks.clone());
                            async move {
                                let lock = maint_lock(&locks, &id);
                                let held = lock.try_lock();
                                if held.is_err() && !only_compaction_holds(&id) {
                                    GC_SKIPPED_BUSY.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    return;
                                }
                                if let Err(e) = gc_tenant_wal(&s3, &id, gc_min_age).await {
                                    log.lock().unwrap().error(format!("wal gc {id}: {e:#}"));
                                }
                            }
                        })
                        .await;
                    // Concurrent: a pass costs several LIST and DELETE round trips, so one at a time cannot keep up
                    // when tens of tenants a second go idle (the backlog then grows without bound).
                    futures::stream::iter(due)
                        .for_each_concurrent(GC_CONCURRENCY, |id| {
                            let (s3, log, locks, touched) = (s3.clone(), log.clone(), locks.clone(), touched.clone());
                            async move {
                                let lock = maint_lock(&locks, &id);
                                let Ok(_held) = lock.try_lock() else {
                                    // busy (a compaction pass): due again at the next round
                                    GC_SKIPPED_BUSY.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    touched.lock().unwrap().entry(id.clone()).or_insert_with(|| Instant::now().checked_sub(gc_after).unwrap_or_else(Instant::now));
                                    return;
                                };
                                match gc_tenant_closed(&s3, &id, gc_min_age).await {
                                    Ok(rec) => log.lock().unwrap().gcs.push(rec),
                                    Err(e) => log.lock().unwrap().error(format!("gc {id}: {e:#}")),
                                }
                            }
                        })
                        .await;
                }
            })
        };
        Self { log, queue, dispatcher, watch_stop, l0_watch, gc_stop, gc_task, workers, sem, stopping, passes }
    }

    /// Stop accepting work and wait for in-flight compactions and the GC loop.
    pub async fn shutdown(self) -> Arc<Mutex<MaintenanceLog>> {
        use std::sync::atomic::Ordering::Relaxed;
        self.watch_stop.store(true, Relaxed);
        let _ = self.l0_watch.await;
        self.queue.stop();
        let _ = self.dispatcher.await;
        // Running passes stop now (a merge of sorted runs can last hours); each ends within its compactor's stop limit.
        self.stopping.store(true, Relaxed);
        let _ = self.sem.acquire_many(self.workers as u32).await;
        while self.passes.load(Relaxed) > 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.gc_stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = self.gc_task.await;
        self.log
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::sync::atomic::Ordering::Relaxed;

    /// The visible state (log path), per key: a read of a key returns the value of the latest durable batch that wrote it
    /// with every earlier write of the key durable; a batch on other keys does not wait for an earlier one still in flight.
    #[tokio::test]
    async fn visible_state_per_key() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder("vis", store).build().await.unwrap();
        let vis = Visible::default();
        let put = |k: &'static str, v: &'static str| {
            let db = db.clone();
            async move {
                let before = db.snapshot().await.unwrap();
                let h = db.put_with_options(k, v, &Default::default(), &slatedb::config::WriteOptions::default()).await.unwrap();
                (h.seqnum(), before)
            }
        };
        let read = |k: &'static str| {
            let s = vis.snapshot_for(k.as_bytes());
            let db = db.clone();
            async move {
                match s {
                    Some(s) => s.get(k).await.unwrap(),
                    None => db.get(k).await.unwrap(),
                }
                .map(|b| String::from_utf8(b.to_vec()).unwrap())
            }
        };
        // a (key x), b (key y), c (key x again)
        let (a, sa) = put("x", "1").await;
        vis.add(a, sa, vec![b"x".to_vec()]);
        let (b, sb) = put("y", "1").await;
        vis.add(b, sb, vec![b"y".to_vec()]);
        let (c, sc) = put("x", "2").await;
        vis.add(c, sc, vec![b"x".to_vec()]);
        assert_eq!(read("x").await, None, "nothing is durable yet");
        assert_eq!(read("y").await, None);
        // b is on another key: visible as soon as it is confirmed, although a (earlier) is not
        vis.confirm(b);
        assert_eq!(vis.state(b), VisState::Visible);
        assert_eq!(read("y").await.as_deref(), Some("1"));
        // c is confirmed before a, on the same key: it waits for a, and reads of x still see the value before a
        vis.confirm(c);
        assert_eq!(vis.state(c), VisState::Pending);
        assert_eq!(read("x").await, None);
        let waiter = tokio::time::timeout(Duration::from_millis(50), vis.wait_visible(c, Duration::from_secs(5))).await;
        assert!(waiter.is_err(), "c's acknowledgement waits for a");
        vis.confirm(a);
        assert_eq!(vis.state(a), VisState::Visible);
        assert_eq!(vis.state(c), VisState::Visible);
        assert_eq!(read("x").await.as_deref(), Some("2"));
        vis.wait_visible(c, Duration::from_secs(1)).await.unwrap();
        assert!(!vis.any());
        // a scan's overlay: the in-flight key takes its value from before the batch
        let (d, sd) = put("x", "3").await;
        vis.add(d, sd, vec![b"x".to_vec()]);
        let over = vis.in_range(b"a", Some(b"z"));
        assert_eq!(over.len(), 1);
        assert_eq!(over[0].1.get("x").await.unwrap().as_deref(), Some(&b"2"[..]));
        // durable in object storage: confirmed by the watermark, also for a batch added after it
        vis.confirm_upto(d);
        assert!(!vis.any());
        // a lost owner: what was in flight is never confirmed
        let (e, se) = put("z", "1").await;
        vis.add(e, se, vec![b"z".to_vec()]);
        vis.kill();
        assert_eq!(vis.state(e), VisState::Lost);
        assert!(vis.wait_visible(e, Duration::from_secs(1)).await.is_err());
        db.close().await.unwrap();
    }

    /// One minute of L0 watch rounds (one per second) over a tenant whose compaction fails every time: the pause after each
    /// failure (1, 2, 4, 8, 16, 32 s) leaves only a handful of passes, and a pass that succeeds clears the pause.
    #[test]
    fn failed_compaction_backs_off() {
        let id = "test-backoff-sim";
        OPEN_WRITERS.lock().unwrap().insert(id.to_string());
        let start = Instant::now();
        let mut passes = 0;
        for s in 0..60 {
            let now = start + Duration::from_secs(s);
            if !compaction_deferred(id, now) {
                passes += 1;
                compaction_failed(id, now);
            }
        }
        assert!(passes <= 7, "{passes} passes in a minute");
        assert_eq!(compact_retry_pause(1), Duration::from_secs(1));
        assert_eq!(compact_retry_pause(3), Duration::from_secs(4));
        assert_eq!(compact_retry_pause(40), COMPACT_RETRY_MAX);
        compaction_succeeded(id);
        assert!(!compaction_deferred(id, start + Duration::from_secs(60)));
        writer_closed(id);
    }

    /// The real L0 watch and dispatcher over an open writer with 4 L0 SSTs whose compaction always fails (the store the
    /// compactor reads holds a damaged manifest): over 4 s with a 50 ms watch, at most a few passes (1 s, then 2 s of pause),
    /// not one per round.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn always_failing_compaction_is_not_retried_every_round() {
        let id = "test-backoff-live";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder(tenant_path(id), Arc::clone(&store)).with_settings(writer_settings()).build().await.unwrap();
        for i in 0..L0_COMPACT_THRESHOLD {
            db.put(format!("k{i}").as_bytes(), b"v").await.unwrap();
            db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
        }
        assert!(db.manifest().l0().len() >= L0_COMPACT_THRESHOLD);
        writer_opened(id, &db);

        let broken: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        broken
            .put(&Path::from(format!("{}/manifest/00000000000000000001.manifest", tenant_path(id))), PutPayload::from_static(b"not a manifest"))
            .await
            .unwrap();
        L0_WATCH_MS.store(50, Relaxed);
        let (queue, rx) = CompactionQueue::new();
        let m = Maintenance::start(
            Arc::clone(&broken),
            Arc::clone(&broken),
            queue,
            rx,
            Default::default(),
            Default::default(),
            2,
            Duration::from_secs(3600),
            Duration::from_secs(3600),
        );
        tokio::time::sleep(Duration::from_secs(4)).await;
        let log = m.shutdown().await;
        let failed = log.lock().unwrap().errors.iter().filter(|e| e.starts_with(&format!("compact {id}:"))).count();
        writer_closed(id);
        assert!(!COMPACT_RETRY.lock().unwrap().contains_key(id), "close clears the pause");
        L0_WATCH_MS.store(1000, Relaxed);
        db.close().await.unwrap();
        assert!((1..=4).contains(&failed), "{failed} failed passes in 4 s");
    }

    /// A store that can stop answering: while frozen every request waits (no error), like an object store that hangs.
    #[derive(Debug)]
    struct Frozen {
        inner: Arc<dyn ObjectStore>,
        frozen: Arc<std::sync::atomic::AtomicBool>,
        /// Freeze at the first put whose path contains this (then that put waits too).
        freeze_on_put: Option<&'static str>,
    }

    impl Frozen {
        fn new(inner: &Arc<dyn ObjectStore>, frozen: &Arc<std::sync::atomic::AtomicBool>) -> Self {
            Self { inner: Arc::clone(inner), frozen: Arc::clone(frozen), freeze_on_put: None }
        }
        fn put_gate(&self, l: &Path) {
            if self.freeze_on_put.is_some_and(|f| l.as_ref().contains(f)) {
                self.frozen.store(true, Relaxed);
            }
        }
        async fn gate(&self) {
            while self.frozen.load(Relaxed) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    impl std::fmt::Display for Frozen {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Frozen({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for Frozen {
        async fn put_opts(&self, l: &Path, p: PutPayload, o: object_store::PutOptions) -> object_store::Result<object_store::PutResult> {
            self.put_gate(l);
            self.gate().await;
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(&self, l: &Path, o: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.put_gate(l);
            self.gate().await;
            self.inner.put_multipart_opts(l, o).await
        }
        async fn get_opts(&self, l: &Path, o: object_store::GetOptions) -> object_store::Result<object_store::GetResult> {
            self.gate().await;
            self.inner.get_opts(l, o).await
        }
        fn delete_stream(&self, l: futures::stream::BoxStream<'static, object_store::Result<Path>>) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(l)
        }
        fn list(&self, p: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
            self.inner.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<object_store::ListResult> {
            self.gate().await;
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, f: &Path, t: &Path, o: object_store::CopyOptions) -> object_store::Result<()> {
            self.gate().await;
            self.inner.copy_opts(f, t, o).await
        }
    }

    /// A writer whose L0 is full (`l0_max_ssts`) and whose memtable holds a write: abandoning it (lease lost) returns at
    /// once. Before, `Db::close` flushed the memtable and waited forever for room in L0, holding the tenant's lock.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abandon_does_not_wait_for_a_full_l0() {
        let id = "test-abandon-full-l0";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let lc = Lifecycle::new(Arc::new(CountingStore::new(Arc::clone(&store))), None);
        let t = lc.create(id).await.unwrap();
        let l0_max = writer_settings().l0_max_ssts;
        let Some(Mode::Writer(db)) = &t.mode else { panic!("not a writer") };
        for i in 0..l0_max {
            db.put(format!("k{i}").as_bytes(), b"v").await.unwrap();
            db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
        }
        assert_eq!(db.manifest().l0().len(), l0_max);
        // One write in its WAL object (acknowledged as the engine does it), one only in the WAL buffer (acknowledged before
        // the WAL write: local log, relaxed durability). Both stay in the memtable (L0 has no room).
        db.put(b"last", b"v").await.unwrap();
        db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::Wal }).await.unwrap();
        db.put(b"buffered", b"v").await.unwrap();
        let t0 = Instant::now();
        tokio::time::timeout(ABANDON_CLOSE_TIMEOUT + Duration::from_secs(5), t.abandon()).await.expect("abandon did not return");
        assert!(t0.elapsed() < ABANDON_CLOSE_TIMEOUT, "abandon took {:?}", t0.elapsed());
        // Nothing acknowledged is lost: the next owner (a writer open replays the WAL) reads it.
        let next = writer_builder(id, &store).build().await.unwrap();
        assert!(next.get(b"last").await.unwrap().is_some());
        assert!(next.get(b"buffered").await.unwrap().is_some(), "abandon uploads the WAL buffer");
        next.close_with_options(slatedb::config::CloseOptions { flush_type: None }).await.unwrap();
    }

    /// A store whose GETs of compacted SSTs each take `ms` milliseconds (a slow store, or a busy machine).
    #[derive(Debug)]
    struct Slow {
        inner: Arc<dyn ObjectStore>,
        ms: u64,
    }

    impl std::fmt::Display for Slow {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Slow({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for Slow {
        async fn put_opts(&self, l: &Path, p: PutPayload, o: object_store::PutOptions) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(&self, l: &Path, o: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(l, o).await
        }
        async fn get_opts(&self, l: &Path, o: object_store::GetOptions) -> object_store::Result<object_store::GetResult> {
            if l.as_ref().contains("/compacted/") {
                tokio::time::sleep(Duration::from_millis(self.ms)).await;
            }
            self.inner.get_opts(l, o).await
        }
        fn delete_stream(&self, l: futures::stream::BoxStream<'static, object_store::Result<Path>>) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(l)
        }
        fn list(&self, p: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
            self.inner.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, f: &Path, t: &Path, o: object_store::CopyOptions) -> object_store::Result<()> {
            self.inner.copy_opts(f, t, o).await
        }
    }

    /// A tenant whose L0 holds `n` SSTs of about `mb` MB of random values each.
    async fn tenant_with_l0(raw: &Arc<dyn ObjectStore>, id: &str, n: usize, mb: usize) {
        use rand::RngCore;
        let db = Db::builder(tenant_path(id), Arc::clone(raw)).with_settings(writer_settings()).build().await.unwrap();
        let mut rng = rand::thread_rng();
        for i in 0..n {
            for j in 0..mb * 16 {
                let mut v = vec![0u8; 64 * 1024];
                rng.fill_bytes(&mut v);
                db.put(format!("k{j:06}-{i:03}").as_bytes(), &v).await.unwrap();
            }
            db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
        }
        assert!(db.manifest().l0().len() >= L0_COMPACT_THRESHOLD);
        db.close().await.unwrap();
    }

    /// L1: a compaction that needs longer than the stall window, but makes progress all along, finishes in one pass. Before,
    /// every pass was stopped after 30 s whatever it was doing ("did not finish (L0 8)"), the next one started the same
    /// compaction from the beginning, and a database whose compactions need longer (large, slow store, busy machine) never
    /// compacted again: its L0 stayed full and its writes stopped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compaction_pass_longer_than_its_stall_window_finishes() {
        let id = "test-compact-long";
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        tenant_with_l0(&raw, id, 8, 4).await;
        let s3: Arc<dyn ObjectStore> = Arc::new(Slow { inner: Arc::clone(&raw), ms: 400 });
        let stall = Duration::from_secs(1);
        let rec = compact_tenant_pass(&s3, &raw, id, stall, COMPACTOR_STOP_LIMIT, false).await.expect("the pass failed").expect("no pass");
        eprintln!("compaction pass: {:.2} s, L0 {} -> {}", rec.secs, rec.l0_before, rec.l0_after);
        assert!(rec.l0_after < L0_COMPACT_THRESHOLD, "L0 {} after the pass", rec.l0_after);
        assert!(rec.secs > 2.0 * stall.as_secs_f64(), "the compaction took {:.2} s: not longer than the stall window, the test proves nothing", rec.secs);
        assert!(!compactions_active(&raw, id).await.unwrap(), "a compaction is still active after the pass");
    }

    /// L1: a pass is stopped at once by `preempt_compaction` (drop, erase, fork, export, restore) and that is not a failure.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compaction_pass_stops_when_preempted() {
        let id = "test-compact-preempt";
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        tenant_with_l0(&raw, id, 6, 2).await;
        let s3: Arc<dyn ObjectStore> = Arc::new(Slow { inner: Arc::clone(&raw), ms: 400 });
        let pass = {
            let (s3, raw) = (Arc::clone(&s3), Arc::clone(&raw));
            tokio::spawn(async move { compact_tenant_pass(&s3, &raw, id, Duration::from_secs(30), COMPACTOR_STOP_LIMIT, false).await })
        };
        tokio::time::sleep(Duration::from_millis(500)).await;
        let t0 = Instant::now();
        let guard = preempt_compaction(id);
        let r = tokio::time::timeout(Duration::from_secs(20), pass).await.expect("the pass did not stop").unwrap();
        drop(guard);
        assert!(r.is_ok(), "a preempted pass is not a failure: {:?}", r.err());
        assert!(t0.elapsed() < Duration::from_secs(10), "the pass took {:?} to stop", t0.elapsed());
    }

    /// The store stops answering while a compaction pass starts and stays down past the pass's deadline: `Compactor::stop`
    /// then runs before `Compactor::run` has registered its tasks. Once the store is back the pass must end. Before, `run`
    /// started its tasks after the stop and waited for them forever, and the maintenance worker never came back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compaction_pass_ends_when_stopped_while_starting() {
        let id = "test-compact-stop-while-starting";
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder(tenant_path(id), Arc::clone(&raw)).with_settings(writer_settings()).build().await.unwrap();
        for i in 0..L0_COMPACT_THRESHOLD {
            db.put(format!("k{i}").as_bytes(), b"v").await.unwrap();
            db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
        }
        db.close().await.unwrap();
        let frozen = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let s3: Arc<dyn ObjectStore> = Arc::new(Frozen::new(&raw, &frozen));
        let pass = {
            let (s3, raw) = (Arc::clone(&s3), Arc::clone(&raw));
            tokio::spawn(async move { compact_tenant_within(&s3, &raw, id, Duration::from_millis(500), COMPACTOR_STOP_LIMIT).await })
        };
        tokio::time::sleep(Duration::from_secs(2)).await;
        frozen.store(false, Relaxed);
        let r = tokio::time::timeout(Duration::from_secs(20), pass).await.expect("compaction pass did not end after the store came back");
        let _ = r.unwrap();
    }

    /// The store stops answering after a compaction pass has started writing its output: `run` has registered its tasks, so
    /// `Compactor::stop` cancels them and waits for their store request, which does not end. The pass must still end, at
    /// its stop limit, with the store still down. Before, every `stop` was awaited without a bound.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compaction_pass_ends_when_stop_hangs() {
        let id = "test-compact-stop-hangs";
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder(tenant_path(id), Arc::clone(&raw)).with_settings(writer_settings()).build().await.unwrap();
        for i in 0..L0_COMPACT_THRESHOLD {
            db.put(format!("k{i}").as_bytes(), b"v").await.unwrap();
            db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
        }
        db.close().await.unwrap();
        let frozen = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let s3: Arc<dyn ObjectStore> = Arc::new(Frozen { freeze_on_put: Some("compacted/"), ..Frozen::new(&raw, &frozen) });
        let stop_limit = Duration::from_secs(3);
        let t0 = Instant::now();
        let pass = {
            let (s3, raw) = (Arc::clone(&s3), Arc::clone(&raw));
            tokio::spawn(async move { compact_tenant_within(&s3, &raw, id, Duration::from_secs(1), stop_limit).await })
        };
        let r = tokio::time::timeout(Duration::from_secs(15), pass).await;
        let was_frozen = frozen.load(Relaxed);
        frozen.store(false, Relaxed);
        assert!(was_frozen, "the compactor never wrote its output (the test did not reach the case)");
        let r = r.expect("compaction pass did not end with the store down");
        assert!(r.unwrap().is_err(), "the pass must fail: its compactor did not stop");
        assert!(t0.elapsed() < Duration::from_secs(1) + stop_limit + Duration::from_secs(4), "pass took {:?}", t0.elapsed());
    }

    /// `abandon` says whether the close finished. With the store down it returns false after `ABANDON_CLOSE_TIMEOUT` and
    /// `close_pending` stays true (slatedb's tasks of the tenant are still alive) until the store answers and the close ends.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abandon_reports_an_unfinished_close() {
        let id = "test-abandon-store-down";
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let frozen = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let store: Arc<dyn ObjectStore> = Arc::new(Frozen::new(&raw, &frozen));
        let lc = Lifecycle::new(Arc::new(CountingStore::new(Arc::clone(&store))), None);
        // store up: the close finishes
        let t = lc.create(id).await.unwrap();
        let Some(Mode::Writer(db)) = &t.mode else { panic!("not a writer") };
        db.put(b"a", b"v").await.unwrap();
        assert!(t.abandon().await, "close with the store up");
        assert!(!close_pending(id));
        // store down: left behind, and reported
        let t = lc.wake(id).await.unwrap();
        let mut t = t;
        t.put("b", b"v").await.unwrap();
        frozen.store(true, Relaxed);
        let t0 = Instant::now();
        assert!(!t.abandon().await, "close with the store down");
        assert!(t0.elapsed() < ABANDON_CLOSE_TIMEOUT + Duration::from_secs(2), "abandon took {:?}", t0.elapsed());
        assert!(close_pending(id));
        frozen.store(false, Relaxed);
        let t0 = Instant::now();
        while close_pending(id) {
            assert!(t0.elapsed() < Duration::from_secs(30), "the close left behind did not end once the store was back");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[test]
    fn maintenance_log_keeps_the_last_errors() {
        let mut log = MaintenanceLog::default();
        for i in 0..MAINT_ERRORS_KEPT + 500 {
            log.error(format!("e{i}"));
        }
        assert_eq!(log.errors.len(), MAINT_ERRORS_KEPT);
        assert_eq!(log.errors_total, (MAINT_ERRORS_KEPT + 500) as u64);
        assert_eq!(log.errors.front().unwrap(), "e500");
    }
}
