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

/// Writers a process has open right now (garbage collection of a whole tenant is left to its close).
static OPEN_WRITERS: std::sync::LazyLock<Mutex<std::collections::HashSet<String>>> = std::sync::LazyLock::new(Default::default);

/// Tenants written recently, for the WAL-only garbage collection of open writers (see `Maintenance`).
struct HotGc {
    last_write: Instant,
    last_gc: Instant,
}
static GC_HOT: std::sync::LazyLock<Mutex<HashMap<String, HotGc>>> = std::sync::LazyLock::new(Default::default);
/// Seconds between WAL garbage-collection passes over an open writer that is being written (0 = never).
pub static HOT_GC_EVERY_SECS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(20);

/// Record that a writer of tenant `id` just wrote.
pub fn note_write(id: &str) {
    let now = Instant::now();
    GC_HOT.lock().unwrap().entry(id.to_string()).and_modify(|h| h.last_write = now).or_insert(HotGc { last_write: now, last_gc: now });
}

/// Test control: override slatedb's `l0_max_ssts` (8) for writers, 0 = keep the default. A lower value makes the
/// memtable-flush stall that the shutdown fallback handles happen after fewer writes.
pub static L0_MAX_SSTS_OVERRIDE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

pub fn writer_settings() -> Settings {
    let mut s = Settings::default();
    let ms = FLUSH_INTERVAL_MS.load(std::sync::atomic::Ordering::Relaxed);
    if ms > 0 {
        s.flush_interval = Some(Duration::from_millis(ms));
    }
    let l0_max = L0_MAX_SSTS_OVERRIDE.load(std::sync::atomic::Ordering::Relaxed);
    if l0_max > 0 {
        s.l0_max_ssts = l0_max;
        s.l0_max_ssts_per_key = s.l0_max_ssts_per_key.min(l0_max);
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
    Ok(DbReader::builder(tenant_path(id), Arc::clone(store))
        .with_reader_mode(DbReaderMode::FollowLatest)
        .with_options(reader_options(skip_wal_replay))
        .build()
        .await?)
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

    pub fn request(&self, id: &str, waiter: Option<oneshot::Sender<usize>>) {
        let mut p = self.pending.lock().unwrap();
        match p.entry(id.to_string()) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                if let Some(w) = waiter {
                    e.get_mut().push(w);
                }
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(waiter.into_iter().collect());
                let _ = self.tx.send(Some(id.to_string()));
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
        let db = Db::builder(tenant_path(&id), Arc::clone(&self.store)).with_settings(writer_settings()).build().await?;
        self.touched.lock().unwrap().insert(id.clone(), Instant::now());
        OPEN_WRITERS.lock().unwrap().insert(id.clone());
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
}

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
}

impl Commit {
    /// Wait until the batch is in a durable WAL object. Writes staged while a flush is in flight are flushed
    /// together by the next one.
    pub async fn wait(self) -> Result<()> {
        use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
        GROUP_COMMITS.fetch_add(1, Relaxed);
        let seq = self.handle.seqnum();
        // Flush now: waiting for the periodic flush (100 ms) would add up to that much to every write.
        if EXPLICIT_FLUSH.load(Relaxed) && self.group.flushed_seq.load(Acquire) < seq {
            let _lead = self.group.lock.lock().await;
            if self.group.flushed_seq.load(Acquire) < seq {
                let window = GROUP_WINDOW_US.load(Relaxed);
                if window > 0 {
                    tokio::time::sleep(Duration::from_micros(window)).await;
                }
                let target = self.group.staged_seq.load(Acquire);
                self.db.flush().await?;
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
    /// L0 SSTs of the writer's local manifest view (None for a reader).
    pub fn writer_l0(&self) -> Option<usize> {
        match &self.mode {
            Some(Mode::Writer(db)) => Some(db.manifest().l0().len()),
            _ => None,
        }
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
            Db::builder(tenant_path(&self.id), Arc::clone(store)).with_settings(writer_settings()).build()
        );
        match del {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
        self.mode = Some(Mode::Writer(db?));
        self.lc.touched.lock().unwrap().insert(self.id.clone(), Instant::now());
        OPEN_WRITERS.lock().unwrap().insert(self.id.clone());
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
        self.group.staged_seq.fetch_max(handle.seqnum(), std::sync::atomic::Ordering::Release);
        Ok(Some(Commit { db: db.clone(), handle, group: Arc::clone(&self.group) }))
    }

    pub async fn put(&mut self, key: &str, value: &[u8]) -> Result<WriteHandle> {
        self.upgrade().await?;
        let Some(Mode::Writer(db)) = self.mode.as_ref() else { unreachable!() };
        Ok(db.put(key, value).await?)
    }

    /// Give the tenant up without any of the clean-close bookkeeping (no marker, no compaction request):
    /// used when ownership was lost, where flushing would only be fenced anyway.
    pub async fn abandon(mut self) {
        match self.mode.take() {
            Some(Mode::Reader(r)) => {
                let _ = r.close().await;
            }
            Some(Mode::Writer(db)) => {
                let _ = db.close().await;
                OPEN_WRITERS.lock().unwrap().remove(&self.id);
            }
            None => {}
        }
    }

    /// Close. A writer flushes its memtable, then the marker is written, and the tenant is
    /// queued for compaction if it now holds enough L0 SSTs. A reader just closes.
    pub async fn close(mut self) -> Result<()> {
        match self.mode.take() {
            Some(Mode::Reader(r)) => r.close().await?,
            Some(Mode::Writer(db)) => {
                OPEN_WRITERS.lock().unwrap().remove(&self.id);
                match tokio::time::timeout(CLOSE_FLUSH_TIMEOUT, db.close()).await {
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
    let id = id.to_string();
    let id = id.as_str();
    let l0_before = l0_count(raw, id).await?;
    if l0_before < L0_COMPACT_THRESHOLD {
        return Ok(None);
    }
    let counting = Arc::new(CountingStore::new(Arc::clone(s3)));
    let store: Arc<dyn ObjectStore> = counting.clone();
    let cleanup_store = Arc::clone(&store);
    let mut opts = CompactorOptions::default();
    opts.poll_interval = Duration::from_millis(50);
    let mut worker = CompactionWorkerOptions::default();
    worker.compactions_poll_interval = Duration::from_millis(50);
    opts.worker = Some(worker);
    let started = Instant::now();
    let compactor = Arc::new(CompactorBuilder::new(tenant_path(id), store).with_options(opts).build());
    let runner = {
        let c = Arc::clone(&compactor);
        tokio::spawn(async move { c.run().await })
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut l0_after = l0_before;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
        l0_after = l0_count(raw, id).await?;
        if l0_after < L0_COMPACT_THRESHOLD {
            break;
        }
    }
    compactor.stop().await?;
    let _ = runner.await;
    // A standalone compactor leaves one checkpoint (15 min lifetime) per compaction. Each pins the
    // manifest version it was taken at, and through it old SSTs and WALs, so GC cannot reclaim them
    // until they expire. Nothing else takes checkpoints in this lifecycle (FollowLatest readers
    // create none), so drop them all once the pass is over. Counted in the pass's requests.
    let admin = Admin::builder(tenant_path(id), cleanup_store).build();
    let checkpoints = admin.list_checkpoints(None).await?;
    let checkpoints_deleted = checkpoints.len();
    for cp in checkpoints {
        admin.delete_checkpoint(cp.id).await?;
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
    let opts = GarbageCollectorOptions {
        manifest_options: Some(dir),
        wal_options: Some(dir),
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

/// Per tenant: the first WAL id that may still exist (everything below was deleted by an earlier pass).
static WAL_FLOOR: std::sync::LazyLock<Mutex<HashMap<String, u64>>> = std::sync::LazyLock::new(Default::default);

fn wal_object_path(id: &str, n: u64) -> Path {
    Path::from(format!("{}/wal/{:020}.sst", tenant_path(id), n))
}

/// Delete the WAL objects of an open writer that its latest manifest no longer needs (every id below
/// `replay_after_wal_id`, which advances each time the writer flushes its memtable to an L0 SST). Only the WAL
/// directory is touched: the writer is live, so manifests, SSTs and compactions are left alone. The default pass
/// remembers where the last one stopped, so it neither lists the directory nor deletes object by object.
pub async fn gc_tenant_wal(s3: &Arc<dyn ObjectStore>, id: impl std::fmt::Display, min_age: Duration) -> Result<GcRecord> {
    use std::sync::atomic::Ordering::Relaxed;
    let t0 = Instant::now();
    let mode = WAL_GC_MODE.load(Relaxed);
    let rec = if mode == 0 {
        gc_tenant_wal_slatedb(s3, &id, min_age).await?
    } else {
        let counting = Arc::new(CountingStore::new(Arc::clone(s3)));
        let store: Arc<dyn ObjectStore> = counting.clone();
        gc_wal_incremental(&store, &id.to_string(), mode).await?;
        GcRecord { tenant: id.to_string(), requests: counting.counters.snapshot() }
    };
    WAL_GC_PASSES.fetch_add(1, Relaxed);
    WAL_GC_TOTAL_US.fetch_add(t0.elapsed().as_micros() as u64, Relaxed);
    Ok(rec)
}

async fn gc_wal_incremental(store: &Arc<dyn ObjectStore>, id: &str, mode: u8) -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    let admin = Admin::builder(tenant_path(id), Arc::clone(store)).build();
    let Some(m) = admin.read_manifest(None).await? else { return Ok(()) };
    // A checkpoint pins the WAL objects its manifest needs; this process takes none (the standalone compactor's are
    // gone when its pass ends, and passes are serialized by the tenant's maintenance lock), so skip if one exists.
    if !admin.list_checkpoints(None).await?.is_empty() {
        return Ok(());
    }
    // slatedb keeps the boundary id itself; everything below it is covered by L0 SSTs.
    let boundary = m.replay_after_wal_id();
    let wal_dir = Path::from(format!("{}/wal", tenant_path(id)));
    let list_min = || async {
        let t = Instant::now();
        let mut min = u64::MAX;
        let mut it = store.list(Some(&wal_dir));
        while let Some(meta) = it.next().await {
            let meta = meta?;
            if let Some(n) = meta.location.filename().and_then(|f| f.strip_suffix(".sst")).and_then(|f| f.parse::<u64>().ok()) {
                min = min.min(n);
            }
        }
        WAL_GC_LIST_US.fetch_add(t.elapsed().as_micros() as u64, Relaxed);
        Ok::<u64, anyhow::Error>(min)
    };
    if mode == 1 {
        list_min().await?;
        return Ok(());
    }
    let known = WAL_FLOOR.lock().unwrap().get(id).copied();
    let floor = match known {
        Some(f) => f,
        // First pass for this tenant in this process: one listing to find the oldest object that still exists.
        None => list_min().await?.min(boundary),
    };
    if floor < boundary {
        let paths: Vec<Path> = (floor..boundary).map(|n| wal_object_path(id, n)).collect();
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
    }
    WAL_FLOOR.lock().unwrap().insert(id.to_string(), boundary.max(floor));
    Ok(())
}

#[derive(Default)]
pub struct MaintenanceLog {
    pub compactions: Vec<CompactionRecord>,
    pub gcs: Vec<GcRecord>,
    pub errors: Vec<String>,
}

pub struct Maintenance {
    pub log: Arc<Mutex<MaintenanceLog>>,
    queue: Arc<CompactionQueue>,
    dispatcher: tokio::task::JoinHandle<()>,
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
                                let l0 = rec.l0_after;
                                log.lock().unwrap().compactions.push(rec);
                                l0
                            }
                            Ok(None) => l0_count(&raw, &id).await.unwrap_or(0),
                            Err(e) => {
                                log.lock().unwrap().errors.push(format!("compact {id}: {e:#}"));
                                usize::MAX / 2
                            }
                        };
                        queue.finish(&id, l0_after);
                        drop(permit);
                    });
                }
            })
        };
        let gc_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let gc_task = {
            let (s3, log, stop, locks) = (s3.clone(), log.clone(), gc_stop.clone(), locks.clone());
            tokio::spawn(async move {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_secs(2)).await;
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
                    futures::stream::iter(hot_due)
                        .for_each_concurrent(GC_CONCURRENCY, |id| {
                            let (s3, log, locks) = (s3.clone(), log.clone(), locks.clone());
                            async move {
                                let lock = maint_lock(&locks, &id);
                                let _held = lock.lock().await;
                                if let Err(e) = gc_tenant_wal(&s3, &id, gc_min_age).await {
                                    log.lock().unwrap().errors.push(format!("wal gc {id}: {e:#}"));
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
                                match gc_tenant(&s3, &id, gc_min_age).await {
                                    Ok(rec) => log.lock().unwrap().gcs.push(rec),
                                    Err(e) => log.lock().unwrap().errors.push(format!("gc {id}: {e:#}")),
                                }
                            }
                        })
                        .await;
                }
            })
        };
        Self { log, queue, dispatcher, gc_stop, gc_task, workers, sem }
    }

    /// Stop accepting work and wait for in-flight compactions and the GC loop.
    pub async fn shutdown(self) -> Arc<Mutex<MaintenanceLog>> {
        self.queue.stop();
        let _ = self.dispatcher.await;
        let _ = self.sem.acquire_many(self.workers as u32).await;
        self.gc_stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = self.gc_task.await;
        self.log
    }
}
