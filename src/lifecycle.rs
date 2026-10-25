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
const LONG_POLL: Duration = Duration::from_secs(3600);

pub fn tenant_path(id: impl std::fmt::Display) -> String {
    format!("tenants/{id}")
}

pub fn marker_path(id: impl std::fmt::Display) -> Path {
    Path::from(format!("tenants/{id}/_clean"))
}

pub fn writer_settings() -> Settings {
    let mut s = Settings::default();
    s.garbage_collector_options = None;
    s.compactor_options = None;
    s.manifest_poll_interval = LONG_POLL;
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
        Ok(Tenant { lc: Arc::clone(self), id, mode: Some(Mode::Writer(db)), unclean: false, l0_at_wake: 0 })
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
        Ok(Tenant { lc: Arc::clone(self), id, mode: Some(Mode::Reader(reader)), unclean: !clean, l0_at_wake })
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

pub struct Tenant {
    lc: Arc<Lifecycle>,
    pub id: String,
    mode: Option<Mode>,
    /// The wake took the unclean path (marker missing).
    pub unclean: bool,
    pub l0_at_wake: usize,
}

impl Tenant {
    pub fn is_writer(&self) -> bool {
        matches!(self.mode, Some(Mode::Writer(_)))
    }

    pub async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<Bytes>> {
        Ok(match self.mode.as_ref().ok_or_else(|| anyhow!("closed"))? {
            Mode::Reader(r) => r.get(key).await?,
            Mode::Writer(db) => db.get(key).await?,
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
        Ok(())
    }

    /// All key/value pairs whose key starts with `prefix`, in key order.
    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        // slatedb's scan defaults fetch one 4 KiB block per request, one at a time, and do not cache
        // the blocks: a 1 MB scan costs ~250 sequential GETs every time. Read ahead in large
        // requests, fetch in parallel, and keep the blocks.
        let opts = slatedb::config::ScanOptions {
            read_ahead_bytes: 4 << 20,
            max_fetch_tasks: 4,
            cache_blocks: true,
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
        if ops.is_empty() {
            return Ok(());
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
        // Flush now: waiting for the periodic flush (100 ms) would add up to that much to every write.
        db.flush().await?;
        handle.await_durable().await?;
        Ok(())
    }

    pub async fn put(&mut self, key: &str, value: &[u8]) -> Result<WriteHandle> {
        self.upgrade().await?;
        let Some(Mode::Writer(db)) = self.mode.as_ref() else { unreachable!() };
        Ok(db.put(key, value).await?)
    }

    /// Close. A writer flushes its memtable, then the marker is written, and the tenant is
    /// queued for compaction if it now holds enough L0 SSTs. A reader just closes.
    pub async fn close(mut self) -> Result<()> {
        match self.mode.take() {
            Some(Mode::Reader(r)) => r.close().await?,
            Some(Mode::Writer(db)) => {
                db.close().await?;
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
    Ok(GcRecord { tenant: id.to_string(), requests: counting.counters.snapshot() })
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
                        let due: Vec<String> =
                            t.iter().filter(|(_, at)| at.elapsed() >= gc_after).map(|(id, _)| id.clone()).collect();
                        for id in &due {
                            t.remove(id);
                        }
                        due
                    };
                    for id in due {
                        let lock = maint_lock(&locks, &id);
                        let _held = lock.lock().await;
                        match gc_tenant(&s3, &id, gc_min_age).await {
                            Ok(rec) => log.lock().unwrap().gcs.push(rec),
                            Err(e) => log.lock().unwrap().errors.push(format!("gc {id}: {e:#}")),
                        }
                    }
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
