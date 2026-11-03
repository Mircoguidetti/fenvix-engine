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
    CompactionWorkerOptions, CompactorOptions, DbReaderOptions, GarbageCollectorDirectoryOptions,
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
    use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
    let block: Arc<dyn slatedb::db_cache::DbCache> = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions { max_capacity: block_bytes, ..Default::default() }));
    let meta: Arc<dyn slatedb::db_cache::DbCache> = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions { max_capacity: meta_bytes, ..Default::default() }));
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

fn cache_scope() -> Option<(Arc<dyn slatedb::db_cache::DbCache>, u64)> {
    DB_CACHE.get().map(|c| (Arc::clone(c), CACHE_SCOPE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)))
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
            db.flush().await?;
            Ok(true)
        }
        None => Ok(false),
    }
}

pub fn relaxed_window(id: &str) -> Duration {
    RELAXED_WINDOW.get().map(|f| f(id)).unwrap_or_default()
}

pub(crate) fn writer_builder(id: &str, store: &Arc<dyn ObjectStore>) -> slatedb::DbBuilder<String> {
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
    let b = Db::builder(tenant_path(id), Arc::clone(store)).with_settings(settings).with_block_cache_policy(flush_cache_policy());
    match cache_scope() {
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
        s.l0_max_ssts_per_key = s.l0_max_ssts_per_key.min(l0_max);
    }
    let l0_bytes = L0_SST_BYTES_OVERRIDE.load(std::sync::atomic::Ordering::Relaxed);
    if l0_bytes > 0 {
        s.l0_sst_size_bytes = l0_bytes as usize;
    }
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

async fn open_reader(store: &Arc<dyn ObjectStore>, id: impl std::fmt::Display, skip_wal_replay: bool) -> Result<DbReader> {
    let b = DbReader::builder(tenant_path(id), Arc::clone(store)).with_reader_mode(DbReaderMode::FollowLatest).with_options(reader_options(skip_wal_replay));
    let b = match cache_scope() {
        Some((c, scope)) => b.with_db_cache(c, scope),
        None => b,
    };
    Ok(b.build().await?)
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
        self.touched.lock().unwrap().insert(id.clone(), Instant::now());
        writer_opened(&id, &db);
        Ok(Tenant { group: Default::default(), lc: Arc::clone(self), id, mode: Some(Mode::Writer(db)), unclean: false, l0_at_wake: 0 })
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
        let marker = marker_path(&id);
        let (reader, head) = tokio::join!(open_reader(&self.store, &id, true), self.store.head(&marker));
        let mut reader = reader?;
        let clean = match head {
            Ok(_) => true,
            Err(object_store::Error::NotFound { .. }) => false,
            Err(e) => return Err(e.into()),
        };
        if !clean {
            // Possible unflushed WAL: the speculative reader may be missing data. Reopen with replay.
            reader.close().await?;
            reader = open_reader(&self.store, &id, false).await?;
            self.stats.lock().unwrap().unclean_wakes += 1;
        }
        let l0_at_wake = reader.manifest().l0().len();
        Ok(Tenant { group: Default::default(), lc: Arc::clone(self), id, mode: Some(Mode::Reader(reader)), unclean: !clean, l0_at_wake })
    }
}

/// A write for [`Tenant::apply`].
pub enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

enum Mode {
    Reader(DbReader),
    Writer(Db),
}

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
}

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

/// A staged write batch waiting to become durable.
pub struct Commit {
    db: Db,
    handle: WriteHandle,
    group: Arc<GroupFlush>,
    tenant: String,
    /// Local log (prototype): the batch is durable once this resolves (its record is on the local disk).
    local: Option<oneshot::Receiver<std::result::Result<(), String>>>,
}

impl Commit {
    /// Make the batch's durability the local log's fsync instead of the WAL flush to object storage.
    pub fn with_local(mut self, rx: oneshot::Receiver<std::result::Result<(), String>>) -> Self {
        self.local = Some(rx);
        self
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
        if let Some(rx) = self.local.take() {
            GROUP_COMMITS.fetch_add(1, Relaxed);
            rx.await.map_err(|_| anyhow!("local log stopped"))?.map_err(|e| anyhow!(e))?;
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
}

impl Tenant {
    /// slatedb writer epoch of this writer (None for a reader).
    pub fn writer_epoch(&self) -> Option<u64> {
        match &self.mode {
            Some(Mode::Writer(db)) => Some(db.manifest().writer_epoch()),
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

    /// Give group commit the tenant's in-flight write counter (see `GroupFlush::inflight`).
    pub fn bind_inflight(&self, counter: Arc<std::sync::atomic::AtomicU64>) {
        let _ = self.group.inflight.set(counter);
    }

    pub fn is_writer(&self) -> bool {
        matches!(self.mode, Some(Mode::Writer(_)))
    }

    pub async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<Bytes>> {
        Ok(match self.mode.as_ref().ok_or_else(|| anyhow!("closed"))? {
            Mode::Reader(r) => r.get(key).await?,
            Mode::Writer(db) => db.get(key).await?,
        })
    }

    /// Like [`Tenant::get`], but a writer only returns data that is already in a durable WAL object or SST, not
    /// writes that are staged and waiting for their flush (a reader's data is always durable).
    pub async fn get_durable<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<Bytes>> {
        Ok(match self.mode.as_ref().ok_or_else(|| anyhow!("closed"))? {
            Mode::Reader(r) => r.get(key).await?,
            Mode::Writer(db) => {
                let opts = slatedb::config::ReadOptions { durability_filter: slatedb::config::DurabilityLevel::Remote, ..Default::default() };
                db.get_with_options(key, &opts).await?
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
            writer_builder(&self.id, store).build()
        );
        match del {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
        let db = db?;
        writer_opened(&self.id, &db);
        self.mode = Some(Mode::Writer(db));
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
        let mut out = Vec::new();
        while let Some(kv) = it.next().await? {
            out.push((kv.key, kv.value));
        }
        Ok(out)
    }

    /// A consistent snapshot of the database as of now (upgrades to a writer first: snapshots exist on writers only).
    pub async fn snapshot(&mut self) -> Result<Arc<slatedb::DbSnapshot>> {
        self.upgrade().await?;
        let Some(Mode::Writer(db)) = self.mode.as_ref() else { unreachable!() };
        Ok(db.snapshot().await?)
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
        let opts = slatedb::config::ScanOptions {
            read_ahead_bytes: read_ahead,
            max_fetch_tasks: 4,
            cache_blocks: true,
            order: if reverse { slatedb::IterationOrder::Descending } else { slatedb::IterationOrder::Ascending },
            ..Default::default()
        };
        if let Some(e) = end {
            if start >= e {
                return Ok(Vec::new()); // an empty range would panic the store
            }
        }
        let s = start.to_vec();
        let range = match end {
            Some(e) => (std::ops::Bound::Included(s), std::ops::Bound::Excluded(e.to_vec())),
            None => (std::ops::Bound::Included(s), std::ops::Bound::Unbounded),
        };
        let mut it = match self.mode.as_ref().ok_or_else(|| anyhow!("closed"))? {
            Mode::Reader(r) => r.scan_with_options(range, &opts).await?,
            Mode::Writer(db) => db.scan_with_options(range, &opts).await?,
        };
        let mut out = Vec::new();
        while let Some(kv) = it.next().await? {
            out.push((kv.key, kv.value));
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
        let Some(Mode::Writer(db)) = self.mode.as_ref() else { unreachable!() };
        let mut batch = slatedb::WriteBatch::new();
        for op in ops {
            match op {
                Op::Put(k, v) => batch.put(k, v),
                Op::Delete(k) => batch.delete(k),
            }
        }
        let handle = db.write(batch).await?;
        note_unflushed(&self.id, handle.seqnum());
        self.group.staged_seq.fetch_max(handle.seqnum(), std::sync::atomic::Ordering::Release);
        Ok(Some(Commit { db: db.clone(), handle, group: Arc::clone(&self.group), tenant: self.id.clone(), local: None }))
    }

    pub async fn put(&mut self, key: &str, value: &[u8]) -> Result<WriteHandle> {
        self.upgrade().await?;
        let Some(Mode::Writer(db)) = self.mode.as_ref() else { unreachable!() };
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
                        let _ = db.close_with_options(slatedb::config::CloseOptions { flush_type: None }).await;
                        return Err(anyhow!("final flush timed out"));
                    }
                }
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
    compact_tenant_within(s3, raw, id, Duration::from_secs(30), COMPACTOR_STOP_LIMIT).await
}

async fn compact_tenant_within(s3: &Arc<dyn ObjectStore>, raw: &Arc<dyn ObjectStore>, id: impl std::fmt::Display, pass: Duration, stop_limit: Duration) -> Result<Option<CompactionRecord>> {
    let warm = crate::warm::COMPACTION_WARM.load(std::sync::atomic::Ordering::Relaxed);
    compact_tenant_pass(s3, raw, id, pass, stop_limit, warm).await
}

/// One compaction pass; with `warm`, the output that replaces blocks an open writer has in the shared block cache is read
/// into that cache before the writer switches to the new manifest (`warm.rs`).
pub(crate) async fn compact_tenant_pass(
    s3: &Arc<dyn ObjectStore>,
    raw: &Arc<dyn ObjectStore>,
    id: impl std::fmt::Display,
    pass: Duration,
    stop_limit: Duration,
    warm: bool,
) -> Result<Option<CompactionRecord>> {
    let id = id.to_string();
    let id = id.as_str();
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
            (Some(db), Some(cache), Some(scope)) => Some((cache, scope, crate::warm::before(&db.manifest()))),
            _ => None,
        }
    } else {
        None
    };
    let mut opts = CompactorOptions::default();
    opts.poll_interval = Duration::from_millis(50);
    let mut worker = CompactionWorkerOptions::default();
    worker.compactions_poll_interval = Duration::from_millis(50);
    opts.worker = Some(worker);
    let started = Instant::now();
    let compactor = Arc::new(CompactorBuilder::new(tenant_path(id), store).with_options(opts).build());
    let mut runner = {
        let c = Arc::clone(&compactor);
        tokio::spawn(async move { c.run().await })
    };
    let deadline = Instant::now() + pass;
    let mut l0_after = l0_before;
    // Each poll is bounded by what is left of the pass, and a failed poll ends the polling, not the function: returning
    // early here would leave the compactor running with nobody to stop it.
    let mut poll_err = None;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let left = deadline.saturating_duration_since(Instant::now()).max(Duration::from_millis(100));
        match tokio::time::timeout(left, l0_count(raw, id)).await {
            Ok(Ok(n)) => l0_after = n,
            Ok(Err(e)) => {
                poll_err = Some(e);
                break;
            }
            Err(_) => break,
        }
        if l0_after < L0_COMPACT_THRESHOLD {
            break;
        }
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
        match tokio::time::timeout(Duration::from_secs(1), compactor.stop()).await {
            Ok(r) => r.map_err(anyhow::Error::from),
            Err(_) => Ok(()),
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
    // Bounded: past `COMPACTION_WARM_SECS` (or on an error) the pass goes on and the reads not warmed miss, as without it.
    let mut consumed = Vec::new();
    if let Some((cache, scope, before)) = &warm_from {
        let limit = Duration::from_secs(crate::warm::COMPACTION_WARM_SECS.load(std::sync::atomic::Ordering::Relaxed));
        match tokio::time::timeout(limit, crate::warm::warm_compaction_output(&cleanup_store, &tenant_path(id), cache, *scope, before)).await {
            Ok(Ok((c, _rec))) => consumed = c,
            Ok(Err(e)) => {
                crate::warm::WARM_FAILS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                eprintln!("compaction warm-up of tenant {id}: {e}");
            }
            Err(_) => {
                crate::warm::WARM_FAILS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                eprintln!("compaction warm-up of tenant {id}: timed out after {limit:?}");
            }
        }
    }
    // An open writer learns of the compaction now instead of at its next manifest poll (`--writer-poll-secs`): until then it
    // still counts the compacted L0 SSTs, and a writer written fast can fill its L0 and stall its memtable flushes.
    let writer = WRITER_DBS.lock().unwrap().get(id).cloned();
    if let Some(db) = writer {
        let refreshed = db.refresh_manifest().await.is_ok();
        // The consumed SSTs are no longer in the writer's manifest: their blocks would only age out of the cache.
        if refreshed && !consumed.is_empty() {
            crate::warm::evict_consumed(&db, &consumed).await;
        }
    }
    anyhow::ensure!(l0_after < L0_COMPACT_THRESHOLD, "compaction of tenant {id} did not finish (L0 {l0_after})");
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
    if !m.segments().is_empty() || !m.checkpoints().is_empty() {
        eprintln!("SSTGC skip tenant={id} (segments or checkpoints present)");
        return Ok(false);
    }
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
                file.strip_suffix(".manifest").and_then(|n| n.parse::<u64>().ok()).map_or(true, |n| n >= latest)
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
        let dispatcher = {
            let (s3, raw, queue, log, sem, locks) = (s3.clone(), raw.clone(), queue.clone(), log.clone(), sem.clone(), locks.clone());
            tokio::spawn(async move {
                while let Some(Some(id)) = rx.recv().await {
                    let permit = sem.clone().acquire_owned().await.unwrap();
                    let (s3, raw, queue, log, locks) = (s3.clone(), raw.clone(), queue.clone(), log.clone(), locks.clone());
                    tokio::spawn(async move {
                        let lock = maint_lock(&locks, &id);
                        let _held = lock.lock().await;
                        let l0_after = match compact_tenant(&s3, &raw, &id).await {
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
                        drop(permit);
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
                                let lock = maint_lock(&locks, &id);
                                let _held = lock.lock().await;
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
                                let _held = lock.lock().await;
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
                            let (s3, log, locks) = (s3.clone(), log.clone(), locks.clone());
                            async move {
                                let lock = maint_lock(&locks, &id);
                                let _held = lock.lock().await;
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
        Self { log, queue, dispatcher, watch_stop, l0_watch, gc_stop, gc_task, workers, sem }
    }

    /// Stop accepting work and wait for in-flight compactions and the GC loop.
    pub async fn shutdown(self) -> Arc<Mutex<MaintenanceLog>> {
        self.watch_stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = self.l0_watch.await;
        self.queue.stop();
        let _ = self.dispatcher.await;
        let _ = self.sem.acquire_many(self.workers as u32).await;
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
