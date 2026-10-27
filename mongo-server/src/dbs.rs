//! One tenant per MongoDB database, driven through the results3 lifecycle: wake as a reader,
//! upgrade to a writer on the first write, clean close after an idle period.

use crate::layout;
use crate::lease::{self, Leases};
use anyhow::{bail, Result};
use bson::{doc, Bson, Document};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tenant_bench::lifecycle::{Commit, Lifecycle, Op, Tenant};
use std::sync::atomic::AtomicU64;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock as AsyncMutex, Semaphore};

static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

fn now_ms() -> u64 {
    START.get_or_init(Instant::now).elapsed().as_millis() as u64
}

/// Cold wakes that may run at once (None = unlimited). Wakes beyond the limit queue instead of piling onto the store.
static WAKE_SEM: std::sync::OnceLock<Option<Arc<Semaphore>>> = std::sync::OnceLock::new();
pub static WAKE_QUEUED: AtomicI64 = AtomicI64::new(0);
pub static WAKE_WAITS: AtomicU64 = AtomicU64::new(0);
pub static WAKE_WAIT_MS: AtomicU64 = AtomicU64::new(0);
pub static WAKE_MAX_QUEUED: AtomicI64 = AtomicI64::new(0);

/// Reads of one tenant share the entry lock (true) or take it exclusively like writes (false: the behaviour
/// before concurrent reads, one command per tenant at a time; `--serialize-reads`).
pub static SHARED_READS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

/// Group commit: a write is staged under the tenant's exclusive lock and made durable after the lock is released
/// (by the server's `handle`, before the reply is sent), so writes that arrive while a flush is in flight share
/// the next WAL object. False (`--no-group-commit`): the write waits for its own flush under the lock.
pub static GROUP_COMMIT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

/// Counts a write command of a tenant from the moment it asks for the tenant lock until its reply is ready; dropped
/// at the end of `Server::handle`. Group commit uses the count to tell whether another write is on its way.
pub struct InflightToken(Arc<AtomicU64>);

impl Drop for InflightToken {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Relaxed);
    }
}

tokio::task_local! {
    /// In-flight tokens of the command being handled on this task.
    pub static TOKENS: std::cell::RefCell<Vec<InflightToken>>;
    /// Commits staged by the command being handled on this task, awaited before its reply is sent.
    pub static PENDING: std::cell::RefCell<Vec<Commit>>;
}

/// Reads (find, count, distinct, aggregate) see only durable data: a write that is staged and waiting for its group
/// flush is invisible to them until its flush is done. Commands that write still see their own staged data, which
/// they need to decide (duplicate keys, unique indexes). False (`--durable-reads false`) lets reads see staged data.
pub static DURABLE_READS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

pub fn set_wake_limit(n: usize) {
    let _ = WAKE_SEM.set(if n == 0 { None } else { Some(Arc::new(Semaphore::new(n))) });
}

/// Idle and shutdown closes that may run at once.
const CLOSE_CONCURRENCY: usize = 64;

pub struct Dbs {
    lc: Arc<Lifecycle>,
    raw: Arc<dyn ObjectStore>,
    entries: Mutex<HashMap<String, Arc<AsyncMutex<Entry>>>>,
    idle: Duration,
    pub leases: Arc<Leases>,
    pub global: Arc<tenant_bench::counting::CountingStore>,
    pub readers: Arc<AtomicI64>,
    pub writers: Arc<AtomicI64>,
    inflight: Mutex<HashMap<String, Arc<AtomicU64>>>,
}

pub struct Entry {
    name: String,
    lc: Arc<Lifecycle>,
    raw: Arc<dyn ObjectStore>,
    tenant: Option<Tenant>,
    leases: Arc<Leases>,
    readers: Arc<AtomicI64>,
    writers: Arc<AtomicI64>,
    /// 0 closed, 1 reader, 2 writer: what this entry currently contributes to the open-tenant gauges.
    counted: u8,
    last_used_ms: AtomicU64,
    pub wakes: u64,
    pub closes: u64,
    last_compaction_request: Option<Instant>,
    inflight: Arc<AtomicU64>,
}

/// MongoDB database names may not be empty, contain `/\. "$*<>:|?` or a NUL byte.
pub fn valid_db_name(name: &str) -> bool {
    !name.is_empty() && name.len() < 64 && !name.chars().any(|c| "/\\. \"$*<>:|?\0".contains(c))
}

impl Dbs {
    pub fn new(lc: Arc<Lifecycle>, raw: Arc<dyn ObjectStore>, idle: Duration, leases: Arc<Leases>, global: Arc<tenant_bench::counting::CountingStore>) -> Arc<Self> {
        Arc::new(Self { lc, raw, entries: Mutex::new(HashMap::new()), idle, leases, global, readers: Arc::new(AtomicI64::new(0)), writers: Arc::new(AtomicI64::new(0)), inflight: Mutex::new(HashMap::new()) })
    }

    pub fn entry_count(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    /// Abandon tenants whose lease was lost (another process took them over).
    pub fn spawn_lost_handler(self: &Arc<Self>, mut rx: tokio::sync::mpsc::UnboundedReceiver<String>) {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            while let Some(id) = rx.recv().await {
                let e = me.entries.lock().unwrap().get(&id).cloned();
                if let Some(e) = e {
                    let mut g = e.write().await;
                    g.abandon().await;
                }
            }
        });
    }

    /// Lock a database's entry. Commands on one database run one at a time.
    fn entry(&self, name: &str) -> Arc<AsyncMutex<Entry>> {
        {
            let mut m = self.entries.lock().unwrap();
            m.entry(name.to_string())
                .or_insert_with(|| {
                    Arc::new(AsyncMutex::new(Entry {
                        name: name.to_string(),
                        lc: Arc::clone(&self.lc),
                        raw: Arc::clone(&self.raw),
                        tenant: None,
                        leases: Arc::clone(&self.leases),
                        readers: Arc::clone(&self.readers),
                        writers: Arc::clone(&self.writers),
                        counted: 0,
                        last_used_ms: AtomicU64::new(now_ms()),
                        wakes: 0,
                        closes: 0,
                        last_compaction_request: None,
                        inflight: self.inflight_counter(name),
                    }))
                })
                .clone()
        }
    }

    fn inflight_counter(&self, name: &str) -> Arc<AtomicU64> {
        Arc::clone(self.inflight.lock().unwrap().entry(name.to_string()).or_default())
    }

    /// Lock a database's entry exclusively (every command that writes, and tenant state changes). Counts the command
    /// as in flight for its tenant until the end of `Server::handle`.
    pub async fn lock(&self, name: &str) -> OwnedRwLockWriteGuard<Entry> {
        let counter = self.inflight_counter(name);
        counter.fetch_add(1, Relaxed);
        let token = InflightToken(counter);
        // Outside `handle` there is no scope to hold the token: it is dropped (and the count decremented) at once.
        let _ = TOKENS.try_with(|t| t.borrow_mut().push(token));
        self.entry(name).write_owned().await
    }

    /// Open the database for reading and share it with other readers: reads of one tenant run concurrently with
    /// each other and exclude only writes and state changes. The tenant is opened (and its lease taken) under the
    /// exclusive lock first when needed. `ReadView` re-checks ownership after every read.
    pub async fn read(&self, name: &str) -> Result<ReadView> {
        let e = self.entry(name);
        if !SHARED_READS.load(Relaxed) {
            let mut w = Arc::clone(&e).write_owned().await;
            if !w.ensure_open().await? {
                return Ok(ReadView { g: None });
            }
            return Ok(ReadView { g: Some(Guard::Write(w)) });
        }
        loop {
            {
                let g = Arc::clone(&e).read_owned().await;
                if g.tenant.is_some() && g.leases.valid(&g.name) {
                    g.touch();
                    return Ok(ReadView { g: Some(Guard::Read(g)) });
                }
            }
            let mut w = Arc::clone(&e).write_owned().await;
            if !w.ensure_open().await? {
                return Ok(ReadView { g: None });
            }
        }
    }

    /// Cumulative object-store requests made through the lifecycle (not maintenance passes):
    /// (PUT/LIST-tier, GET-tier, DELETE), priced like S3's request tiers.
    pub fn request_counts(&self) -> (u64, u64, u64) {
        let s = self.lc.counting.counters.snapshot();
        (s.billed_put_tier(), s.billed_get_tier(), s.delete)
    }

    /// Close tenants that have been idle for `idle`. Runs until the process exits. Closes run concurrently
    /// (a close flushes, writes the clean marker and releases the lease, so tens of milliseconds each: one at a
    /// time cannot keep up with a few tenants going idle per second).
    pub fn spawn_reaper(self: &Arc<Self>) {
        let me = Arc::clone(self);
        let permits = Arc::new(tokio::sync::Semaphore::new(CLOSE_CONCURRENCY));
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let entries: Vec<Arc<AsyncMutex<Entry>>> = me.entries.lock().unwrap().values().cloned().collect();
                for e in entries {
                    // Cheap pre-check without waiting: skip entries in use or not idle yet.
                    let due = match e.try_read() {
                        Ok(g) => g.tenant.is_some() && g.idle_for() >= me.idle,
                        Err(_) => false,
                    };
                    if !due {
                        continue;
                    }
                    let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else { break };
                    let idle = me.idle;
                    tokio::spawn(async move {
                        let _p = permit;
                        if let Ok(mut g) = e.try_write_owned() {
                            if g.tenant.is_some() && g.idle_for() >= idle {
                                if let Err(err) = g.close().await {
                                    eprintln!("idle close of {} failed: {err:#}", g.name);
                                }
                            }
                        }
                    });
                }
            }
        });
    }

    pub async fn close_all(&self) {
        let entries: Vec<Arc<AsyncMutex<Entry>>> = self.entries.lock().unwrap().values().cloned().collect();
        futures::stream::iter(entries)
            .for_each_concurrent(CLOSE_CONCURRENCY, |e| async move {
                let mut g = e.write().await;
                if let Err(err) = g.close().await {
                    eprintln!("close of {} failed: {err:#}", g.name);
                }
            })
            .await;
    }

    /// Database names (tenants) present in object storage.
    pub async fn list_databases(&self) -> Result<Vec<String>> {
        let r = self.raw.list_with_delimiter(Some(&Path::from("tenants"))).await?;
        let mut names: Vec<String> = r
            .common_prefixes
            .iter()
            .filter_map(|p| p.parts().nth(1).map(|s| s.as_ref().to_string()))
            .filter(|n| valid_db_name(n))
            .collect();
        names.sort();
        Ok(names)
    }
}

impl Entry {
    pub fn touch(&self) {
        self.last_used_ms.store(now_ms(), Relaxed);
    }

    pub fn idle_for(&self) -> Duration {
        Duration::from_millis(now_ms().saturating_sub(self.last_used_ms.load(Relaxed)))
    }

    pub fn mode(&self) -> &'static str {
        match &self.tenant {
            None => "closed",
            Some(t) if t.is_writer() => "writer",
            Some(_) => "reader",
        }
    }

    pub fn unclean_wake(&self) -> Option<bool> {
        self.tenant.as_ref().map(|t| t.unclean)
    }

    async fn db_exists(&self) -> Result<bool> {
        let prefix = Path::from(format!("tenants/{}", self.name));
        let mut it = self.raw.list(Some(&prefix));
        while let Some(m) = it.next().await {
            let name = m?.location.filename().unwrap_or("").to_string();
            if name != "_lease" && name != "_clean" {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn sync_gauges(&mut self) {
        let now = match &self.tenant {
            None => 0,
            Some(t) if t.is_writer() => 2,
            Some(_) => 1,
        };
        if now != self.counted {
            match self.counted {
                1 => drop(self.readers.fetch_sub(1, Relaxed)),
                2 => drop(self.writers.fetch_sub(1, Relaxed)),
                _ => {}
            }
            match now {
                1 => drop(self.readers.fetch_add(1, Relaxed)),
                2 => {
                    self.writers.fetch_add(1, Relaxed);
                    lease::log("writer_open", &self.name, &self.leases.holder, "");
                }
                _ => {}
            }
            if self.counted == 2 && now != 2 {
                lease::log("writer_close", &self.name, &self.leases.holder, "");
            }
            self.counted = now;
        }
    }

    /// A writer has no compactor of its own (it would poll the manifest on every idle tenant), so ask the maintenance
    /// queue for a compaction while the tenant is open once its L0 reaches the compaction threshold; without it a
    /// tenant written for a few minutes fills its L0 and stalls (see `WRITER_POLL_SECS`). At most one request per
    /// `COMPACTION_REQUEST_EVERY`.
    fn request_compaction_if_needed(&mut self) {
        const COMPACTION_REQUEST_EVERY: Duration = Duration::from_secs(10);
        let (Some(t), Some(q)) = (self.tenant.as_ref(), self.lc.queue.as_ref()) else { return };
        if t.writer_l0().map_or(false, |n| n >= tenant_bench::lifecycle::L0_COMPACT_THRESHOLD)
            && self.last_compaction_request.map_or(true, |at| at.elapsed() >= COMPACTION_REQUEST_EVERY)
        {
            self.last_compaction_request = Some(Instant::now());
            q.request(&self.name, None);
        }
    }

    /// Drop the tenant without clean-close bookkeeping (its lease was lost).
    pub async fn abandon(&mut self) {
        if let Some(t) = self.tenant.take() {
            t.abandon().await;
            self.closes += 1;
        }
        self.leases.forget(&self.name);
        self.sync_gauges();
    }

    /// Wake the tenant as a reader if it is closed. `Ok(false)` if the database does not exist.
    /// The tenant's lease is taken first: a process that does not own the tenant gets [`lease::LeaseHeld`].
    pub async fn ensure_open(&mut self) -> Result<bool> {
        self.touch();
        if self.tenant.is_some() {
            if self.leases.valid(&self.name) {
                return Ok(true);
            }
            // The lease lapsed (not renewed in time) or was lost: stop acting as the owner.
            lease::log("invalid", &self.name, &self.leases.holder, "");
            self.abandon().await;
        }
        let _wake_permit = match WAKE_SEM.get().and_then(|o| o.clone()) {
            Some(sem) => {
                let q = WAKE_QUEUED.fetch_add(1, Relaxed) + 1;
                WAKE_MAX_QUEUED.fetch_max(q, Relaxed);
                let t0 = Instant::now();
                let p = sem.acquire_owned().await.ok();
                WAKE_QUEUED.fetch_sub(1, Relaxed);
                WAKE_WAITS.fetch_add(1, Relaxed);
                WAKE_WAIT_MS.fetch_add(t0.elapsed().as_millis() as u64, Relaxed);
                p
            }
            None => None,
        };
        let took = self.leases.acquire(&self.name).await?;
        lease::log("acquired", &self.name, &self.leases.holder, &format!("takeover_from={}", took.as_deref().unwrap_or("-")));
        match self.lc.wake(&self.name).await {
            Ok(t) => {
                if took.is_some() || t.unclean {
                    lease::log("takeover_wake", &self.name, &self.leases.holder, &format!("unclean={} takeover_from={}", t.unclean, took.as_deref().unwrap_or("-")));
                }
                self.tenant = Some(t);
                self.wakes += 1;
                self.sync_gauges();
                Ok(true)
            }
            Err(e) => {
                if self.db_exists().await? {
                    self.leases.release(&self.name).await;
                    Err(e)
                } else {
                    self.leases.discard(&self.name).await;
                    Ok(false)
                }
            }
        }
    }

    pub async fn close(&mut self) -> Result<()> {
        let had = self.tenant.is_some();
        if let Some(t) = self.tenant.take() {
            let r = t.close().await;
            self.closes += 1;
            self.sync_gauges();
            self.leases.release(&self.name).await;
            r?;
        }
        let _ = had;
        Ok(())
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        if !self.ensure_open().await? {
            return Ok(None);
        }
        self.tenant.as_ref().unwrap().get(key).await
    }

    pub async fn scan(&mut self, prefix: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        if !self.ensure_open().await? {
            return Ok(vec![]);
        }
        self.tenant.as_ref().unwrap().scan_prefix(prefix).await
    }

    /// Apply writes atomically and durably, creating the database on its first write.
    pub async fn apply(&mut self, ops: Vec<Op>) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        if !self.ensure_open().await? {
            self.leases.acquire(&self.name).await?;
            lease::log("acquired", &self.name, &self.leases.holder, "takeover_from=- new=true");
            self.tenant = Some(self.lc.create(&self.name).await?);
            self.wakes += 1;
            self.sync_gauges();
        }
        self.touch();
        let staged = self.tenant.as_mut().unwrap().stage(ops).await;
        self.sync_gauges();
        tenant_bench::lifecycle::note_write(&self.name);
        if let Some(t) = self.tenant.as_ref() {
            t.bind_inflight(Arc::clone(&self.inflight));
        }
        self.request_compaction_if_needed();
        let Some(commit) = staged? else { return Ok(()) };
        let mut slot = Some(commit);
        if GROUP_COMMIT.load(Relaxed) {
            let _ = PENDING.try_with(|p| p.borrow_mut().push(slot.take().unwrap()));
        }
        match slot {
            Some(commit) => commit.wait().await,
            None => Ok(()),
        }
    }

    /// Close the tenant and delete every object of the database. Holds the tenant's maintenance
    /// lock so no compaction or GC pass can write into it while (or after) it is being deleted:
    /// a pass that finds no manifest does nothing. Repeats the listing until the prefix is empty.
    pub async fn drop_database(&mut self) -> Result<()> {
        let lock = tenant_bench::lifecycle::maint_lock(&self.lc.maint_locks, &self.name);
        let _held = lock.lock().await;
        if !self.leases.is_held(&self.name) {
            self.leases.acquire(&self.name).await?;
        }
        if let Some(t) = self.tenant.take() {
            t.close().await?;
            self.closes += 1;
            self.sync_gauges();
        }
        let prefix = Path::from(format!("tenants/{}", self.name));
        for _ in 0..5 {
            let objs: Vec<Path> = self.raw.list(Some(&prefix)).map(|m| m.map(|m| m.location)).try_collect().await?;
            if objs.is_empty() {
                break;
            }
            let mut del = self.raw.delete_stream(futures::stream::iter(objs.into_iter().map(Ok)).boxed());
            while let Some(r) = del.next().await {
                r?;
            }
        }
        self.lc.touched.lock().unwrap().remove(&self.name);
        self.leases.forget(&self.name);
        Ok(())
    }

    // ---- document-level helpers --------------------------------------------------------

    pub async fn collections(&mut self) -> Result<Vec<String>> {
        let Some(v) = self.get(layout::META_COLLECTIONS).await? else { return Ok(vec![]) };
        let d = Document::from_reader(&v[..])?;
        Ok(d.get_array("names")?.iter().filter_map(|b| b.as_str().map(str::to_string)).collect())
    }

    pub fn collections_op(names: &[String]) -> Result<Op> {
        let d = doc! { "names": names.iter().map(|n| Bson::String(n.clone())).collect::<Vec<_>>() };
        Ok(Op::Put(layout::META_COLLECTIONS.to_vec(), d.to_vec()?))
    }

    /// User-created indexes of a collection (the implicit `_id_` index is not stored).
    pub async fn indexes(&mut self, coll: &str) -> Result<Vec<Document>> {
        let Some(v) = self.get(&layout::index_key(coll)).await? else { return Ok(vec![]) };
        let d = Document::from_reader(&v[..])?;
        Ok(d.get_array("indexes")?.iter().filter_map(|b| b.as_document().cloned()).collect())
    }

    pub fn indexes_op(coll: &str, idx: &[Document]) -> Result<Op> {
        let d = doc! { "indexes": idx.iter().map(|i| Bson::Document(i.clone())).collect::<Vec<_>>() };
        Ok(Op::Put(layout::index_key(coll), d.to_vec()?))
    }

    pub async fn scan_collection(&mut self, coll: &str) -> Result<Vec<Document>> {
        let rows = self.scan(&layout::doc_prefix(coll)).await?;
        rows.into_iter().map(|(_, v)| Ok(Document::from_reader(&v[..])?)).collect()
    }

    pub async fn get_doc(&mut self, coll: &str, id: &Bson) -> Result<Option<Document>> {
        match self.get(&layout::doc_key(coll, id)).await? {
            Some(v) => Ok(Some(Document::from_reader(&v[..])?)),
            None => Ok(None),
        }
    }

    pub fn put_doc_op(coll: &str, d: &Document) -> Result<Op> {
        let id = match d.get("_id") {
            Some(id) => id,
            None => bail!("document without _id"),
        };
        Ok(Op::Put(layout::doc_key(coll, id), d.to_vec()?))
    }

    pub fn delete_doc_op(coll: &str, id: &Bson) -> Op {
        Op::Delete(layout::doc_key(coll, id))
    }
}

/// A shared (read) hold on a database. Every read re-checks that this process still owns the tenant before
/// the result is returned, so a process that lost its lease while the read was in flight returns a retryable
/// error instead of data. `None` inside means the database does not exist.
pub struct ReadView {
    g: Option<Guard>,
}

enum Guard {
    Read(OwnedRwLockReadGuard<Entry>),
    Write(OwnedRwLockWriteGuard<Entry>),
}

impl std::ops::Deref for Guard {
    type Target = Entry;
    fn deref(&self) -> &Entry {
        match self {
            Guard::Read(g) => g,
            Guard::Write(g) => g,
        }
    }
}

impl ReadView {
    fn done<T>(&self, v: T) -> Result<T> {
        if let Some(g) = &self.g {
            g.leases.check_owner(&g.name)?;
        }
        Ok(v)
    }

    pub async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        let Some(g) = &self.g else { return Ok(vec![]) };
        let t = g.tenant.as_ref().unwrap();
        let r = if DURABLE_READS.load(Relaxed) { t.scan_prefix_durable(prefix).await? } else { t.scan_prefix(prefix).await? };
        self.done(r)
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let Some(g) = &self.g else { return Ok(None) };
        let t = g.tenant.as_ref().unwrap();
        let r = if DURABLE_READS.load(Relaxed) { t.get_durable(key).await? } else { t.get(key).await? };
        self.done(r)
    }

    pub async fn scan_collection(&self, coll: &str) -> Result<Vec<Document>> {
        let rows = self.scan(&layout::doc_prefix(coll)).await?;
        rows.into_iter().map(|(_, v)| Ok(Document::from_reader(&v[..])?)).collect()
    }

    pub async fn get_doc(&self, coll: &str, id: &Bson) -> Result<Option<Document>> {
        match self.get(&layout::doc_key(coll, id)).await? {
            Some(v) => Ok(Some(Document::from_reader(&v[..])?)),
            None => Ok(None),
        }
    }
}
