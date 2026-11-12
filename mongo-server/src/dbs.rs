//! One tenant per MongoDB database, driven through the results3 lifecycle: wake as a reader,
//! upgrade to a writer on the first write, clean close after an idle period.

use crate::layout;
use crate::lease::{self, Leases};
use anyhow::Result;
use bson::{doc, Bson, Document};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering::Relaxed};
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
    /// Storage accesses (`Dbs::read`, `Dbs::lock`) of the command being handled on this task, counted for the test hook
    /// [`interleave_at`].
    pub static STEPS: std::cell::Cell<u32>;
    /// In-flight tokens of the command being handled on this task.
    pub static TOKENS: std::cell::RefCell<Vec<InflightToken>>;
    /// Commits staged by the command being handled on this task, awaited before its reply is sent.
    pub static PENDING: std::cell::RefCell<Vec<Commit>>;
    /// The command's write concern accepts an acknowledgement before durability (`{w: 1, j: false}`, see `wc.rs`).
    pub static RELAX: std::cell::Cell<bool>;
}

/// The commit the current command staged last (group commit on), still waiting for its durability: a clone, for a later
/// command that will answer from what this batch recorded (a repeated commit, a retried write) and must wait for the same
/// durability before it says ok. `None`: nothing staged, or group commit off (the batch was durable before `apply` returned).
pub fn staged_commit() -> Option<Commit> {
    PENDING.try_with(|p| p.borrow().last().cloned()).ok().flatten()
}

/// Like [`staged_commit`], but the commit leaves the command's list: the caller waits for it itself (a transaction's
/// commit, which publishes it to its repeated commits), so the reply does not wait for it a second time.
pub fn take_staged_commit() -> Option<Commit> {
    PENDING.try_with(|p| p.borrow_mut().pop()).ok().flatten()
}

/// Reads (find, count, distinct, aggregate) see only durable data: a write that is staged and waiting for its group
/// flush is invisible to them until its flush is done. Commands that write still see their own staged data, which
/// they need to decide (duplicate keys, unique indexes). False (`--durable-reads false`) lets reads see staged data.
pub static DURABLE_READS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

/// Metadata cache (perf-letture). Every command used to read the collection list, the index definitions and the collection
/// options of its collection from SlateDB: three reads per find besides the document, 59% of the engine's CPU on point
/// reads of one large database. The open tenant keeps the stored values of these keys (`meta/collections`,
/// `meta/indexes/<coll>`, `meta/copt/<coll>`) in memory:
/// - a value is valid only for the opening of the tenant it was read from (`wakes`, `closes`): a reopen, a close, a lost
///   lease, a drop start from nothing (and every place that clears `copts` clears it too);
/// - every write that touches a `meta/` key empties it, in `apply_mode`, under the exclusive lock: no read runs then;
/// - a value is kept only when the latest state and the durable state of the key agree, so it is what a durable read and a
///   read of the latest state would both return: a metadata write that is staged but not yet durable is never cached;
/// - a transaction's reads go through its snapshot, never through the cache.
/// `--metadata-cache false` turns it off.
pub static META_CACHE: AtomicBool = AtomicBool::new(true);

/// Keys of the metadata cache are at most this many per open tenant (values are copied, so a cached value does not keep a
/// SlateDB block alive).
const META_CACHE_MAX: usize = 4096;

fn is_meta_key(key: &[u8]) -> bool {
    key == layout::META_COLLECTIONS || key.starts_with(b"meta/indexes/") || key.starts_with(b"meta/copt/")
}

/// The metadata values of one opening of a tenant (see [`META_CACHE`]).
#[derive(Default)]
pub(crate) struct MetaCache {
    epoch: (u64, u64),
    vals: HashMap<Vec<u8>, Option<Bytes>>,
}

/// Reads of a database with relaxed durability see the writes acknowledged before they were durable (read-your-writes).
fn durable_reads(db: &str) -> bool {
    // With the local or replicated log (`locallog.rs`, Q8d) a write is acknowledged when its record is in the log, long
    // before its WAL object is in object storage (every --local-log-upload-secs): a read of the object-storage state would
    // not see it (a point read right after an acknowledged insert answered nothing: read-your-writes broken), and the
    // agent lane (agentlane.rs) makes object storage hold some later batches and not earlier ones. Reads then see the
    // latest state, as on a MongoDB primary, where a write is visible to local reads before it is majority-acknowledged.
    // With the log, reads see the confirmed state instead (`lifecycle::TRACK_VISIBLE`): every write whose log record is
    // confirmed (or that is in object storage), nothing staged and still waiting for its record. The 8fbdd8d fix read the
    // latest state, which also returned writes the log had not confirmed (dirty reads, review R5).
    DURABLE_READS.load(Relaxed) && crate::ctl::durability::relaxed_ms(db) == 0 && crate::locallog::get().is_none()
}

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
    pub ttl: Arc<crate::ttl::TtlStats>,
    pub changes: Arc<crate::changes::ChangeLog>,
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
    /// A collection with a TTL index was used while this entry was open: a close writes the expiry hint.
    pub(crate) ttl_seen: AtomicBool,
    /// The database was found not to exist during the current command: later reads of the same command answer
    /// "absent" without another lease attempt and LIST (reset when the next command takes the lock).
    absent: bool,
    pub(crate) hint_written: bool,
    /// When each collection's expired documents were last swept by a write command.
    pub(crate) swept: HashMap<String, Instant>,
    /// Snapshot that this command's reads go through (a transaction's), set after taking the lock.
    pub(crate) snap: Option<Arc<tenant_bench::fx::read::Snap>>,
    /// Write counter and, while transactions are open, the last write of each document key (conflict detection).
    pub(crate) seq: u64,
    pub(crate) modified: HashMap<Vec<u8>, u64>,
    pub(crate) active_txns: usize,
    /// Document keys written by open transactions -> transaction id (a second writer gets a write conflict at once).
    pub(crate) locks: HashMap<Vec<u8>, u64>,
    pub(crate) changes: Arc<crate::changes::ChangeLog>,
    inflight: Arc<AtomicU64>,
    /// Natural order: per collection (next sequence number, reserved limit); see `natural.rs`. Cleared whenever the tenant is opened or closed.
    pub(crate) nat: HashMap<String, (u64, u64)>,
    /// Collection options (default collation) by collection; cleared with `nat`.
    pub(crate) copts: HashMap<String, Option<Arc<crate::collopts::CollOpts>>>,
    /// Collections whose customer key (`custkey.rs`) was decided since the options were loaded; cleared with `copts`.
    pub(crate) ckey_decided: std::collections::HashSet<String>,
    /// Storage counters of the collections (`collstat.rs`), by stored key, as the latest state has them (`None`: none
    /// stored); follows every write of the keys (`apply_mode`); cleared with `copts`.
    pub(crate) sstat: HashMap<Vec<u8>, Option<crate::collstat::CollStat>>,
    /// Stored metadata values of the open tenant (see [`META_CACHE`]); cleared with `copts`.
    meta: Mutex<MetaCache>,
    /// Compaction 2, step 4 (`--sealed-compactor`): the compactor of this database while its writer is open.
    fxc: Option<Arc<tenant_bench::fx::compactor::Compactor>>,
}

/// Bytes (keys and values) a read returned: the "KiB read" of the unit of cost (fair.rs).
trait ReadBytes {
    fn read_bytes(&self) -> u64;
}

impl ReadBytes for Option<Bytes> {
    fn read_bytes(&self) -> u64 {
        self.as_ref().map(|v| v.len() as u64).unwrap_or(0)
    }
}

impl ReadBytes for Vec<Option<Bytes>> {
    fn read_bytes(&self) -> u64 {
        self.iter().map(|v| v.read_bytes()).sum()
    }
}

impl ReadBytes for Vec<(Bytes, Bytes)> {
    fn read_bytes(&self) -> u64 {
        self.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum()
    }
}

/// MongoDB database names may not be empty, contain `/\. "$*<>:|?` or a NUL byte.
pub fn valid_db_name(name: &str) -> bool {
    !name.is_empty() && name.len() < 64 && !name.chars().any(|c| "/\\. \"$*<>:|?\0".contains(c))
}

impl Dbs {
    /// The object store (without per-request counting).
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.raw)
    }

    pub fn new(lc: Arc<Lifecycle>, raw: Arc<dyn ObjectStore>, idle: Duration, leases: Arc<Leases>, global: Arc<tenant_bench::counting::CountingStore>) -> Arc<Self> {
        Arc::new(Self { lc, raw, entries: Mutex::new(HashMap::new()), idle, leases, global, readers: Arc::new(AtomicI64::new(0)), writers: Arc::new(AtomicI64::new(0)), ttl: Arc::new(Default::default()), changes: crate::changes::arc_default(), inflight: Mutex::new(HashMap::new()) })
    }

    /// Entries that currently exist (open or not).
    pub(crate) fn entries_snapshot(&self) -> Vec<Arc<AsyncMutex<Entry>>> {
        self.entries.lock().unwrap().values().cloned().collect()
    }

    pub(crate) fn raw_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.raw)
    }

    /// The entry of a database without locking it.
    pub(crate) fn lock_handle(&self, name: &str) -> Arc<AsyncMutex<Entry>> {
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
                    ttl_seen: AtomicBool::new(false),
                    absent: false,
                    hint_written: false,
                    swept: HashMap::new(),
                    snap: None,
                    seq: 0,
                    modified: HashMap::new(),
                    active_txns: 0,
                    locks: HashMap::new(),
                    changes: Arc::clone(&self.changes),
                    inflight: self.inflight_counter(name),
                    nat: HashMap::new(),
                    copts: HashMap::new(),
                    ckey_decided: Default::default(),
                    sstat: HashMap::new(),
                    meta: Default::default(),
                    fxc: None,
                }))
            })
            .clone()
    }

    /// Without waiting: is the database open in this process (a busy entry counts as open)?
    pub fn is_open_hint(&self, name: &str) -> bool {
        let e = self.entries.lock().unwrap().get(name).cloned();
        match e {
            None => false,
            Some(e) => e.try_read().map(|g| g.tenant.is_some()).unwrap_or(true),
        }
    }

    /// "writer", "reader" or "closed", without waiting (a busy entry reads as "busy").
    pub fn state_hint(&self, name: &str) -> String {
        let e = self.entries.lock().unwrap().get(name).cloned();
        match e {
            None => "closed".into(),
            Some(e) => match e.try_read() {
                Ok(g) => match &g.tenant { None => "closed".into(), Some(t) if t.is_writer() => "writer".into(), Some(_) => "reader".into() },
                Err(_) => "busy".into(),
            },
        }
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
                        ttl_seen: AtomicBool::new(false),
                    absent: false,
                        hint_written: false,
                        swept: HashMap::new(),
                    snap: None,
                    seq: 0,
                    modified: HashMap::new(),
                    active_txns: 0,
                    locks: HashMap::new(),
                    changes: Arc::clone(&self.changes),
                        inflight: self.inflight_counter(name),
                    nat: HashMap::new(),
                    copts: HashMap::new(),
                    ckey_decided: Default::default(),
                    sstat: HashMap::new(),
                    meta: Default::default(),
                    fxc: None,
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
        interleave(name).await;
        let counter = self.inflight_counter(name);
        counter.fetch_add(1, Relaxed);
        let token = InflightToken(counter);
        // Outside `handle` there is no scope to hold the token: it is dropped (and the count decremented) at once.
        let _ = TOKENS.try_with(|t| t.borrow_mut().push(token));
        let mut g = self.entry(name).write_owned().await;
        g.snap = None;
        g.absent = false;
        g
    }

    /// Open the database for reading and share it with other readers: reads of one tenant run concurrently with
    /// each other and exclude only writes and state changes. The tenant is opened (and its lease taken) under the
    /// exclusive lock first when needed. `ReadView` re-checks ownership after every read.
    pub async fn read(&self, name: &str) -> Result<ReadView> {
        interleave(name).await;
        let e = self.entry(name);
        if !SHARED_READS.load(Relaxed) {
            let mut w = Arc::clone(&e).write_owned().await;
            w.absent = false;
            if !w.ensure_open().await? {
                return Ok(ReadView { g: None, copts: Default::default(), fc: Default::default() });
            }
            return Ok(ReadView { g: Some(Guard::Write(w)), copts: Default::default(), fc: Default::default() });
        }
        loop {
            {
                let g = Arc::clone(&e).read_owned().await;
                if g.tenant.as_ref().is_some_and(|t| !t.fx_refresh_due()) && g.leases.valid(&g.name) {
                    crate::locallog::check_open(&g.name)?;
                    // F7: the tenant begins no hold past the end of this process's serving lease
                    g.tenant.as_ref().unwrap().set_serving_until(g.leases.serving_until(&g.name));
                    g.touch();
                    return Ok(ReadView { g: Some(Guard::Read(g)), copts: Default::default(), fc: Default::default() });
                }
            }
            let mut w = Arc::clone(&e).write_owned().await;
            w.absent = false;
            if !w.ensure_open().await? {
                return Ok(ReadView { g: None, copts: Default::default(), fc: Default::default() });
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
                        Ok(g) => g.tenant.is_some() && g.active_txns == 0 && g.idle_for() >= me.idle,
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
                            if g.tenant.is_some() && g.active_txns == 0 && g.idle_for() >= idle {
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
    /// `fenvixReseal` (tests and measurements of derived documents, `fx/derive.rs`): flush the memtable, absorb every
    /// L0 SST, merge, and reseal every group of this database's compactor now. Returns (groups resealed, documents
    /// encoded so far by this compactor); None without a compactor.
    pub async fn fx_reseal_now(&mut self) -> Result<Option<(u64, u64)>> {
        let Some(c) = self.fxc.clone() else { return Ok(None) };
        if let Some(db) = self.tenant.as_ref().and_then(|t| t.fx_writer_db()) {
            db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await?;
        }
        while c.absorb_once().await? > 0 {}
        while c.merge_once().await? {}
        let v = c.current().await;
        let mut n = 0;
        for gi in 0..v.groups.len() {
            n += c.reseal_once(Some(gi)).await? as u64;
        }
        while c.merge_once().await? {}
        if let Some(t) = self.tenant.as_mut() {
            t.fx_sync();
        }
        Ok(Some((n, c.sh.stats.derive_encoded.load(Relaxed))))
    }

    /// Stop this database's compactor (before its tenant closes, is abandoned or dropped).
    async fn stop_fxc(&mut self) {
        if let Some(c) = self.fxc.take() {
            c.stop().await;
        }
    }

    /// Empty the metadata cache (and free its memory).
    fn meta_clear(&self) {
        let mut m = self.meta.lock().unwrap();
        m.vals = HashMap::new();
    }

    /// The cached value of a metadata key of the open tenant, if there is one.
    fn meta_lookup(&self, key: &[u8]) -> Option<Option<Bytes>> {
        if !META_CACHE.load(Relaxed) || !is_meta_key(key) {
            return None;
        }
        let m = self.meta.lock().unwrap();
        if m.epoch != (self.wakes, self.closes) {
            return None;
        }
        m.vals.get(key).cloned()
    }

    /// Read a metadata key from the open tenant `t`: the durable state when `durable`, the latest state otherwise. The
    /// value is cached only if the two states agree (no write of the key is staged and still on its way to durability).
    /// `reader`: a read command (not a write deciding on the latest state): with the log on it gets the confirmed state.
    async fn meta_read(&self, t: &Tenant, key: &[u8], durable: bool, reader: bool) -> Result<Option<Bytes>> {
        let latest = t.get(key).await?;
        // A reader's data is all durable; a writer's latest state may hold staged writes.
        let durable_v = if t.is_writer() { t.get_durable(key).await? } else { latest.clone() };
        if latest == durable_v {
            let epoch = (self.wakes, self.closes);
            let mut m = self.meta.lock().unwrap();
            if m.epoch != epoch {
                m.vals = HashMap::new();
                m.epoch = epoch;
            }
            if m.vals.len() < META_CACHE_MAX || m.vals.contains_key(key) {
                m.vals.insert(key.to_vec(), latest.as_ref().map(|v| Bytes::copy_from_slice(v)));
            }
        }
        Ok(if durable {
            durable_v
        } else if let Some(snap) = reader.then(|| t.visible_snapshot_for(key)).flatten() {
            // the log path: an in-flight batch writes this key (see `lifecycle::TRACK_VISIBLE`)
            snap.get(key).await?
        } else {
            latest
        })
    }

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

    /// Drop the tenant without clean-close bookkeeping (its lease was lost). Returns whether its close finished (see
    /// `Tenant::abandon`); true when no tenant was open.
    pub async fn abandon(&mut self) -> bool {
        self.nat.clear();
        self.copts.clear();
        self.ckey_decided.clear();
        self.sstat.clear();
        self.meta_clear();
        if let Some(l) = crate::locallog::get() {
            // Ownership is lost: what was not uploaded can no longer be (another process owns the database). A restart must
            // not apply those records over the new owner's writes.
            if l.has_pending(&self.name) && !l.is_pinned(&self.name) {
                if l.is_remote() {
                    // Q8d: with the replicated log the records are safe on 2 copies of 3: keep them for the restart
                    l.freeze(&self.name);
                } else {
                    eprintln!("local log: ownership of {} lost with writes not yet in object storage; they are lost", self.name);
                    let _ = l.append_drop(&self.name).await;
                }
            }
        }
        let mut closed = true;
        crate::opening::forget_db(&self.name);
        self.stop_fxc().await;
        if let Some(t) = self.tenant.take() {
            closed = t.abandon().await;
            self.closes += 1;
        }
        self.leases.forget(&self.name);
        self.sync_gauges();
        closed
    }

    /// The open tenant, for the control commands (`fenvixSealedPublish`), under this exclusive lock.
    pub fn tenant_mut(&mut self) -> Option<&mut Tenant> {
        self.tenant.as_mut()
    }

    /// Wake the tenant as a reader if it is closed. `Ok(false)` if the database does not exist.
    /// The tenant's lease is taken first: a process that does not own the tenant gets [`lease::LeaseHeld`].
    pub async fn ensure_open(&mut self) -> Result<bool> {
        self.touch();
        // local log: a database whose kept records are not applied yet is refused (its last acknowledged writes are missing)
        crate::locallog::check_open(&self.name)?;
        if self.tenant.is_none() && self.absent {
            return Ok(false);
        }
        if self.tenant.is_some() {
            if self.leases.valid(&self.name) {
                // F7 (compaction 2, step 4): the tenant begins no hold past the end of this process's serving lease
                let until = self.leases.serving_until(&self.name);
                self.tenant.as_ref().unwrap().set_serving_until(until);
                // a reader refreshes its version of our manifest before T_pin (fx, rule 6)
                if self.tenant.as_ref().unwrap().fx_refresh_due() {
                    self.tenant.as_mut().unwrap().fx_refresh().await?;
                }
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
        tenant_bench::counting::set_phase(5);
        let took = self.leases.acquire(&self.name).await?;
        lease::log("acquired", &self.name, &self.leases.holder, &format!("takeover_from={}", took.as_deref().unwrap_or("-")));
        crate::ctl::durability::refresh(&self.name).await;
        match self.lc.wake(&self.name).await {
            Ok(t) => {
                if took.is_some() || t.unclean {
                    lease::log("takeover_wake", &self.name, &self.leases.holder, &format!("unclean={} takeover_from={}", t.unclean, took.as_deref().unwrap_or("-")));
                }
                self.nat.clear();
        self.copts.clear();
        self.ckey_decided.clear();
        self.sstat.clear();
        self.meta_clear();
                crate::opening::forget_db(&self.name);
                let unclean = t.unclean;
                // Another process may have owned (and written) the database since this one last did: its events are not
                // in this process's change log, so a stream positioned before this open cannot be served (P1).
                if !self.leases.continuous(&self.name) {
                    self.changes.mark_opened(&self.name);
                }
                self.tenant = Some(t);
                self.wakes += 1;
                self.sync_gauges();
                crate::fair::register(&self.name);
                tenant_bench::counting::set_phase(0);
                if unclean || took.is_some() {
                    // the replicated log may hold acknowledged writes object storage does not have yet (review R4)
                    if let Err(e) = Box::pin(crate::locallog::replay_on_wake(self)).await {
                        // not served without them: the next command retries the open
                        self.abandon().await;
                        return Err(e.context(format!("database {} was taken over but its records in the log could not be read back; retry", self.name)));
                    }
                }
                Ok(true)
            }
            Err(e) => {
                if self.db_exists().await? {
                    self.leases.release(&self.name).await;
                    Err(e)
                } else {
                    self.leases.discard(&self.name).await;
                    self.absent = true;
                    Ok(false)
                }
            }
        }
    }

    pub async fn close(&mut self) -> Result<()> {
        self.write_ttl_hint().await;
        let had = self.tenant.is_some();
        self.nat.clear();
        self.copts.clear();
        self.ckey_decided.clear();
        self.sstat.clear();
        self.meta_clear();
        crate::opening::forget_db(&self.name);
        self.stop_fxc().await;
        if let Some(t) = self.tenant.take() {
            crate::fair::note_written(&self.name, 0, true);
            let r = t.close().await;
            if r.is_ok() {
                if let Some(l) = crate::locallog::get() {
                    l.closed(&self.name);
                }
            }
            self.closes += 1;
            self.sync_gauges();
            self.leases.release(&self.name).await;
            r?;
        }
        let _ = had;
        Ok(())
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        let r = self.get_uncounted(key).await;
        if let Ok(v) = &r {
            crate::fair::note_read(v.read_bytes());
        }
        r
    }

    async fn get_uncounted(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        if !self.ensure_open().await? {
            return Ok(None);
        }
        if let Some(s) = &self.snap {
            return crate::layout2::get(&crate::layout2::Raw::Snap(s), key).await;
        }
        if META_CACHE.load(Relaxed) && is_meta_key(key) {
            if let Some(v) = self.meta_lookup(key) {
                return Ok(v);
            }
            return self.meta_read(self.tenant.as_ref().unwrap(), key, false, false).await;
        }
        self.tenant.as_ref().unwrap().get(crate::layout2::phys(key)).await
    }

    /// The mapped areas of layout v2 (`custkey.rs`) a logical range read may cross: from its bounds when they tell,
    /// otherwise from the catalog (the collections with a customer key and their co-located indexes).
    async fn segs_for(&mut self, lo: &[u8], hi: Option<&[u8]>) -> Result<Vec<crate::custkey::Seg>> {
        match crate::custkey::area_of(lo, hi) {
            Some(Some(s)) => return Ok(vec![s]),
            Some(None) => return Ok(vec![]),
            None => {}
        }
        let only = crate::custkey::coll_of_range(lo, hi).map(|(_, esc)| esc);
        let mut segs = Vec::new();
        for n in self.collections().await? {
            let esc = layout::escape_collection(&n);
            if only.as_ref().map_or(false, |o| *o != esc) {
                continue;
            }
            if crate::custkey::of_opts(self.coll_opts(&n).await?.as_ref()).is_some() {
                segs.push(crate::custkey::Seg::docs(&esc));
            }
            for i in self.indexes(&n).await? {
                if i.colocated {
                    segs.push(crate::custkey::Seg::index(&esc, &layout::escape_collection(&i.name)));
                }
            }
        }
        Ok(segs)
    }

    /// Like [`Entry::get`] but always reads the latest committed state, not the command's snapshot.
    pub async fn get_live(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        let r = self.get_live_uncounted(key).await;
        if let Ok(v) = &r {
            crate::fair::note_read(v.read_bytes());
        }
        r
    }

    async fn get_live_uncounted(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        if !self.ensure_open().await? {
            return Ok(None);
        }
        self.tenant.as_ref().unwrap().get(crate::layout2::phys(key)).await
    }

    pub async fn scan(&mut self, prefix: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        let r = self.scan_uncounted(prefix).await;
        if let Ok(v) = &r {
            crate::fair::note_read(v.read_bytes());
        }
        r
    }

    async fn scan_uncounted(&mut self, prefix: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        if !self.ensure_open().await? {
            return Ok(vec![]);
        }
        let end = crate::keyenc::prefix_end(prefix);
        self.scan_range_uncounted(prefix, end.as_deref(), false, None, 4 << 20).await
    }

    /// Record an erasure whose deletes this entry just committed (`fx/erase.rs`; the caller holds the exclusive lock).
    pub async fn fx_erasure_register(&mut self, coll: &str, customer: &[u8], t: &tenant_bench::fx::erase::Targets) -> Result<Option<tenant_bench::fx::erase::Erasure>> {
        if !self.ensure_open().await? {
            return Ok(None);
        }
        self.tenant.as_mut().unwrap().fx_erasure_register(coll, customer, t).await
    }

    /// Tests only (`ctl/testhooks.rs`): publish a version now, with a delta run of what SlateDB holds above X_d.
    pub async fn fx_test_publish(&mut self) -> Result<()> {
        if !self.ensure_open().await? {
            return Ok(());
        }
        let t = self.tenant.as_mut().unwrap();
        let mut o = t.fx_auto_opts();
        o.reseal = tenant_bench::fx::build::Reseal::None;
        for _ in 0..100 {
            if t.fx_publish(&o, None).await?.is_some() || t.fx_view().is_some() {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Ok(())
    }

    /// The erasure pass of this database (`fx/erase.rs`). Empty when the database does not exist.
    pub async fn fx_erase_pass(&mut self) -> Result<Vec<tenant_bench::fx::erase::Erasure>> {
        if !self.ensure_open().await? {
            return Ok(Vec::new());
        }
        self.touch();
        let opts = tenant_bench::fx::erase::ScrubOpts::default();
        self.tenant.as_mut().unwrap().fx_erase_pass(&opts).await
    }

    /// Apply writes atomically and durably, creating the database on its first write.
    pub async fn apply(&mut self, ops: Vec<Op>) -> Result<()> {
        self.apply_mode(ops, true, false).await
    }

    /// [`Entry::apply`] for a transaction's commit: on the agent lane its batch is flushed at once (one PUT at the
    /// commit) instead of waiting for the lane's batch window.
    pub async fn apply_txn(&mut self, ops: Vec<Op>) -> Result<()> {
        self.apply_mode(ops, true, true).await
    }

    /// Agent lane (`agentlane.rs`): whether this batch goes on it. Only batches whose collection keys all belong to
    /// agent-lane collections and that touch no catalog key; a batch with a key of an agent-lane collection and anything
    /// else (another collection, the catalog) takes the default path as a whole, so it stays one atomic record.
    async fn agent_lane_batch(&mut self, ops: &[Op]) -> Result<bool> {
        use crate::agentlane::{classify, KeyClass};
        if !crate::agentlane::active() {
            return Ok(false);
        }
        let mut colls: Vec<Vec<u8>> = Vec::new();
        let mut other = false;
        for op in ops {
            let k = match op {
                Op::Put(k, _) | Op::Delete(k) => k,
            };
            match classify(k) {
                KeyClass::Coll(c) => {
                    if !colls.contains(&c) {
                        colls.push(c);
                    }
                }
                KeyClass::Neutral => {}
                KeyClass::Other => {
                    if std::env::var("FENVIX_DEBUG_LANE").is_ok() {
                        eprintln!("agent lane: other key {:?}", String::from_utf8_lossy(k));
                    }
                    other = true
                }
            }
        }
        if colls.is_empty() {
            return Ok(false);
        }
        let (mut any, mut all) = (false, true);
        for c in colls {
            let name = crate::agentlane::unescape(&c);
            let on = crate::agentlane::flag_matches(&self.name, &name) || (!other && self.coll_opts(&name).await?.map_or(false, |o| o.agent));
            any |= on;
            all &= on;
        }
        if (any && !all) || (any && other) {
            crate::agentlane::MIXED_BATCHES.fetch_add(1, Relaxed);
        }
        Ok(all && !other)
    }

    /// Like [`Entry::apply`] but always durable in object storage before it returns, also with the local log on (the
    /// restart of the local log applies its records this way).
    pub async fn apply_durable(&mut self, ops: Vec<Op>) -> Result<()> {
        self.apply_mode(ops, false, false).await
    }

    async fn apply_mode(&mut self, mut ops: Vec<Op>, use_local_log: bool, at_commit: bool) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        // Agent lane (prototype): the batch skips the local and replicated log and is durable once its WAL object is in
        // object storage.
        let agent = use_local_log && self.agent_lane_batch(&ops).await?;
        if agent {
            crate::agentlane::AGENT_BATCHES.fetch_add(1, Relaxed);
            crate::agentlane::note_write(&self.name);
        }
        // Local log (prototype): the batch carries its record's marker, and is durable once the record is on the local disk.
        let local = if use_local_log && !agent { crate::locallog::get() } else { None };
        let local_seq = local.map(|l| {
            let seq = l.next_seq();
            ops.push(l.marker_op(seq));
            seq
        });
        let record = local.map(|l| (l, ops.iter().map(|o| match o {
            Op::Put(k, v) => Op::Put(k.clone(), v.clone()),
            Op::Delete(k) => Op::Delete(k.clone()),
        }).collect::<Vec<Op>>()));
        if !self.ensure_open().await? {
            self.leases.acquire(&self.name).await?;
            lease::log("acquired", &self.name, &self.leases.holder, "takeover_from=- new=true");
            self.nat.clear();
        self.copts.clear();
        self.ckey_decided.clear();
        self.sstat.clear();
        self.meta_clear();
            crate::opening::forget_db(&self.name);
            self.tenant = Some(self.lc.create(&self.name).await?);
            self.absent = false;
            self.wakes += 1;
            self.sync_gauges();
        }
        self.touch();
        // --sealed-runs N: a publish of our manifest every N write batches (fx/build.rs), under this exclusive lock; with
        // --sealed-compactor only the first one, then this database's compactor publishes (fx/compactor.rs)
        if self.fxc.is_none() && self.tenant.as_mut().unwrap().fx_note_write() {
            let t = self.tenant.as_mut().unwrap();
            let opts = t.fx_auto_opts();
            // a failed publish leaves orphan runs only; fx_publish reloads or fences as the failure needs (rules 24, 25)
            if let Err(e) = t.fx_publish(&opts, None).await {
                eprintln!("sealed runs: publish of database {} failed: {e:#}", self.name);
            }
        }
        if self.fxc.is_none() && tenant_bench::fx::compactor::SERVER_ON.load(Relaxed) && self.tenant.as_ref().is_some_and(|t| t.is_writer() && t.fx_view().is_some()) {
            // the registered collections' customer keys, from their stored options (after a restart nothing has been
            // written yet: the compactor must still know which collections are keyed by thread_id)
            for coll in crate::agentck::registered(&self.name) {
                if let Ok(o) = self.coll_opts(&coll).await {
                    let key = crate::custkey::of_opts(o.as_ref());
                    crate::agentck::note_key(&self.name, &coll, key.as_ref().map(|k| k.field.as_str()));
                }
            }
            let t = self.tenant.as_mut().unwrap();
            {
                let mut k = tenant_bench::fx::compactor::server_knobs();
                k.derive = crate::agentck::spec_for(&self.name);
                match tenant_bench::fx::compactor::Compactor::start(t, Arc::clone(&self.raw), k, tenant_bench::fx::pool::global(), tokio::runtime::Handle::current()).await {
                    Ok(c) => {
                        c.run();
                        self.fxc = Some(c);
                    }
                    Err(e) => eprintln!("sealed runs: compactor of database {} did not start: {e:#}", self.name),
                }
            }
        }
        self.seq += 1;
        if self.active_txns > 0 {
            for op in &ops {
                let k = match op {
                    Op::Put(k, _) | Op::Delete(k) => k,
                };
                // documents, the locators of customer-keyed collections (their unique `_id` index, custkey.rs), and the
                // index definitions of a collection (an open transaction that wrote to a collection whose indexes changed
                // since its snapshot did not maintain the new ones: it conflicts at commit, see `txn.rs`)
                if k.starts_with(b"doc/") || k.starts_with(b"loc/") || k.starts_with(b"meta/indexes/") {
                    self.modified.insert(k.clone(), self.seq);
                }
            }
        }
        if ops.iter().any(|o| match o { Op::Put(k, _) | Op::Delete(k) => is_meta_key(k) }) {
            // the metadata cache must not answer with the value this batch replaces (see `META_CACHE`)
            self.meta_clear();
        }
        crate::fair::note_written(&self.name, ops.iter().map(|o| match o { Op::Put(k, v) => k.len() + v.len(), Op::Delete(k) => k.len() } as u64).sum(), false);
        let sstat_ops: Vec<(Vec<u8>, Option<crate::collstat::CollStat>)> = ops
            .iter()
            .filter_map(|o| match o {
                Op::Put(k, v) if k.starts_with(crate::collstat::PREFIX) => Some((k.clone(), crate::collstat::CollStat::from_bytes(v))),
                Op::Delete(k) if k.starts_with(crate::collstat::PREFIX) => Some((k.clone(), None)),
                _ => None,
            })
            .collect();
        // first course (opening.rs): writes to a user's kept opening follow it (logical keys, before layout v2)
        let fc = crate::opening::write_hook(&self.name, &ops);
        // layout v2: customer-keyed documents and co-located index entries are stored under `u/` (custkey.rs)
        let staged = self.tenant.as_mut().unwrap().stage(crate::layout2::phys_ops(ops)).await;
        crate::opening::after_stage(&self.name, fc, staged.is_ok());
        if staged.is_ok() {
            // the counters in memory follow the latest state (`collstat.rs`)
            self.sstat.extend(sstat_ops);
        } else {
            self.sstat.clear();
        }
        self.sync_gauges();
        tenant_bench::lifecycle::note_write(&self.name);
        if let Some(t) = self.tenant.as_ref() {
            t.bind_inflight(Arc::clone(&self.inflight));
        }
        let Some(mut commit) = staged? else { return Ok(()) };
        if agent {
            commit = commit.with_agent(at_commit);
        }
        if let (Some((log, rec_ops)), Some(seq)) = (record, local_seq) {
            let epoch = self.tenant.as_ref().and_then(|t| t.writer_epoch()).unwrap_or(0);
            commit = commit.with_local(log.append(seq, &self.name, &rec_ops, epoch));
        }
        let mut slot = Some(commit);
        // `apply_durable` (the log's replays) waits here, also inside a command: the takeover replay runs inside the
        // command that opened the database, and its reads must see the replayed records
        if GROUP_COMMIT.load(Relaxed) && use_local_log {
            let _ = PENDING.try_with(|p| p.borrow_mut().push(slot.take().unwrap()));
        }
        match slot {
            Some(commit) => {
                let _flushing = crate::fair::io_wait().await;
                commit.wait().await
            }
            None => Ok(()),
        }
    }

    /// Close the tenant and delete every object of the database. Holds the tenant's maintenance
    /// lock so no compaction or GC pass can write into it while (or after) it is being deleted:
    /// a pass that finds no manifest does nothing. Repeats the listing until the prefix is empty.
    pub async fn drop_database(&mut self) -> Result<()> {
        // a compaction pass running (maybe a long merge) stops instead of making the drop wait for it
        let _preempt = tenant_bench::lifecycle::preempt_compaction(&self.name);
        let lock = tenant_bench::lifecycle::maint_lock(&self.lc.maint_locks, &self.name);
        let _held = lock.lock().await;
        // Ownership first: a process that does not own the database (another one serves it) is refused here, before it
        // touches anything (the pins a fork holds in its source, the local log, the objects).
        if !self.leases.valid(&self.name) {
            self.leases.acquire(&self.name).await?;
        }
        let (leases, name) = (Arc::clone(&self.leases), self.name.clone());
        crate::ctl::fork::on_drop(&self.name, &move || leases.valid(&name)).await?;
        if let Some(l) = crate::locallog::get() {
            // A restart must not apply the database's earlier records again.
            l.append_drop(&self.name).await.map_err(|_| anyhow::anyhow!("local log stopped"))?.map_err(|e| anyhow::anyhow!(e))?;
            l.forget_pinned(&self.name);
        }
        self.nat.clear();
        self.copts.clear();
        self.ckey_decided.clear();
        self.sstat.clear();
        self.meta_clear();
        crate::opening::forget_db(&self.name);
        self.stop_fxc().await;
        if let Some(t) = self.tenant.take() {
            t.close().await?;
            self.closes += 1;
            self.sync_gauges();
        }
        // again before the objects go (the steps above can take long)
        self.leases.check_owner(&self.name)?;
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
        // as erase: retention, relaxed durability, allow list, keep-awake pin and the counters go with the database
        match crate::ctl::get() {
            Some(ctl) => crate::ctl::erase::forget_settings(&ctl.store, &self.name).await?,
            None => crate::ctl::erase::forget_in_memory(&self.name),
        }
        Ok(())
    }

    pub(crate) fn is_writer_open(&self) -> bool {
        self.tenant.as_ref().map(|t| t.is_writer()).unwrap_or(false)
    }

    pub(crate) fn is_open(&self) -> bool {
        self.tenant.is_some()
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn raw_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.raw)
    }

    /// Earliest instant (ms) at which any document of this tenant expires, from the first date entry of each TTL index.
    pub async fn next_expiry(&mut self) -> Result<Option<i64>> {
        let mut best: Option<i64> = None;
        for c in self.collections().await? {
            for idx in self.indexes(&c).await? {
                if idx.state != crate::index::State::Ready || idx.ttl.is_none() || idx.key.len() != 1 {
                    continue;
                }
                let asc = idx.key[0].1;
                let prefix = idx.prefix(&c);
                let b = if asc { 10u8 } else { !10u8 };
                let mut start = prefix.clone();
                start.push(b);
                let mut end = prefix.clone();
                end.push(b + 1);
                // first entry of the date bracket (ascending), or the last one for a descending index (smallest date)
                let rows = self.scan_range(&start, Some(&end), !asc, Some(1), 4096).await?;
                if let Some((k, _)) = rows.first() {
                    if let Some(comps) = crate::index::entry_comps(k, &prefix) {
                        if comps.len() >= 9 {
                            let mut b = [0u8; 8];
                            b.copy_from_slice(&comps[1..9]);
                            if !asc {
                                for x in b.iter_mut() {
                                    *x = !*x;
                                }
                            }
                            let ms = (u64::from_be_bytes(b) ^ (1 << 63)) as i64;
                            let exp = ms.saturating_add(idx.ttl.unwrap().saturating_mul(1000));
                            best = Some(best.map(|x| x.min(exp)).unwrap_or(exp));
                        }
                    }
                }
            }
        }
        Ok(best)
    }

    /// Record when this tenant's next document expires as a tiny object outside the tenant's prefix, so a
    /// maintenance pass can find tenants with due documents with one LIST and no wake. Best effort.
    async fn write_ttl_hint(&mut self) {
        if !crate::ttl::WAKE_SLEEPING.load(Relaxed) || !self.is_writer_open() || !(self.ttl_seen.load(Relaxed) || self.hint_written) {
            return;
        }
        let next = match self.next_expiry().await {
            Ok(n) => n,
            Err(e) => {
                eprintln!("ttl hint of {}: {e:#}", self.name);
                return;
            }
        };
        let dir = Path::from(format!("ttl-hints/{}", self.name));
        let old: Vec<Path> = match self.raw.list(Some(&dir)).map(|m| m.map(|m| m.location)).try_collect().await {
            Ok(v) => v,
            Err(_) => vec![],
        };
        let want = next.map(|ms| Path::from(format!("ttl-hints/{}/{:020}", self.name, ms.max(0))));
        for p in old {
            if Some(&p) != want.as_ref() {
                let _ = self.raw.delete(&p).await;
            }
        }
        if let Some(p) = want {
            if let Err(e) = self.raw.put(&p, bytes::Bytes::new().into()).await {
                eprintln!("ttl hint of {}: {e:#}", self.name);
            } else {
                self.hint_written = true;
            }
        } else {
            self.hint_written = false;
        }
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

    /// Indexes of a collection (the implicit `_id_` index is not stored), including building ones.
    pub async fn indexes(&mut self, coll: &str) -> Result<Vec<crate::index::IndexDef>> {
        let Some(v) = self.get(&layout::index_key(coll)).await? else { return Ok(vec![]) };
        let d = Document::from_reader(&v[..])?;
        Ok(d.get_array("indexes")?.iter().filter_map(|b| b.as_document().and_then(crate::index::IndexDef::from_stored)).collect())
    }

    /// The options of a collection (its default collation), cached until the tenant is closed or the options change.
    pub async fn coll_opts(&mut self, coll: &str) -> Result<Option<Arc<crate::collopts::CollOpts>>> {
        if let Some(c) = self.copts.get(coll) {
            return Ok(c.clone());
        }
        let o = match self.get(&crate::collopts::meta_key(coll)).await? {
            Some(v) => Some(crate::collopts::CollOpts::parse(Document::from_reader(&v[..])?)),
            None => None,
        };
        self.copts.insert(coll.to_string(), o.clone());
        Ok(o)
    }

    /// The stored storage counters of a collection (`collstat.rs`), latest state: from memory after the first read.
    pub async fn sstat(&mut self, coll: &str) -> Result<Option<crate::collstat::CollStat>> {
        let k = crate::collstat::key(coll);
        if let Some(v) = self.sstat.get(&k) {
            return Ok(v.clone());
        }
        let v = self.get_live(&k).await?.and_then(|b| crate::collstat::CollStat::from_bytes(&b));
        if self.tenant.is_some() {
            self.sstat.insert(k, v.clone());
        }
        Ok(v)
    }

    /// The default collation of a collection whose options were loaded by [`Entry::coll_opts`] (none otherwise).
    pub fn collation_of(&self, coll: &str) -> Option<Arc<crate::collation::Collation>> {
        self.copts.get(coll).and_then(|o| o.as_ref()).and_then(|o| o.collation.clone())
    }

    /// Forget the cached options of a collection (they changed behind the cache's back).
    pub fn forget_coll_opts(&mut self, coll: &str) {
        self.copts.remove(coll);
        self.ckey_decided.remove(coll);
    }

    /// The UUID of a collection: made (and stored) the first time it is asked for.
    pub async fn collection_uuid(&mut self, coll: &str) -> Result<Vec<u8>> {
        let key = crate::collopts::uuid_key(coll);
        if let Some(v) = self.get_live(&key).await? {
            return Ok(v.to_vec());
        }
        let mut id: [u8; 16] = rand::random();
        id[6] = (id[6] & 0x0f) | 0x40;
        id[8] = (id[8] & 0x3f) | 0x80;
        self.apply(vec![Op::Put(key, id.to_vec())]).await?;
        Ok(id.to_vec())
    }

    /// The validator of a collection whose options were loaded.
    pub fn validator_of(&self, coll: &str) -> Option<crate::validation::Validator> {
        self.copts.get(coll).and_then(|o| o.as_ref()).and_then(|o| o.validator.clone())
    }

    /// A document was written that does not satisfy a validator whose action is `warn`.
    pub fn log_validation_warning(&self, d: &Document) {
        eprintln!("WARN document failed validation (validationAction warn): _id {:?}", d.get("_id"));
    }

    /// The options of a collection whose options were loaded.
    pub fn opts_cached(&self, coll: &str) -> Option<Arc<crate::collopts::CollOpts>> {
        self.copts.get(coll).and_then(|o| o.clone())
    }

    /// Whether a collection whose options were loaded keeps change-stream pre- and post-images.
    pub fn images_of(&self, coll: &str) -> bool {
        self.copts.get(coll).and_then(|o| o.as_ref()).map(|o| o.images).unwrap_or(false)
    }

    /// The capped limits of a collection whose options were loaded.
    pub fn cap_of(&self, coll: &str) -> Option<crate::capped::Cap> {
        self.copts.get(coll).and_then(|o| o.as_ref()).and_then(|o| o.cap.clone())
    }

    pub fn set_coll_opts(&mut self, coll: &str, o: Option<Arc<crate::collopts::CollOpts>>) {
        if o.is_none() {
            self.ckey_decided.remove(coll);
        }
        self.copts.insert(coll.to_string(), o);
    }

    pub fn ckey_decided(&self, coll: &str) -> bool {
        self.ckey_decided.contains(coll)
    }

    pub fn note_ckey_decided(&mut self, coll: &str) {
        self.ckey_decided.insert(coll.to_string());
    }

    pub fn indexes_op(coll: &str, idx: &[crate::index::IndexDef]) -> Result<Op> {
        let d = doc! { "indexes": idx.iter().map(|i| Bson::Document(i.to_stored())).collect::<Vec<_>>() };
        Ok(Op::Put(layout::index_key(coll), d.to_vec()?))
    }

    /// [`Tenant::approx_bytes`] of the open tenant (0 if closed).
    pub fn approx_bytes(&self) -> u64 {
        self.tenant.as_ref().map(|t| t.approx_bytes()).unwrap_or(0)
    }

    pub async fn scan_range(&mut self, start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize) -> Result<Vec<(Bytes, Bytes)>> {
        let r = self.scan_range_uncounted(start, end, reverse, limit, read_ahead).await;
        if let Ok(v) = &r {
            crate::fair::note_read(v.read_bytes());
        }
        r
    }

    async fn scan_range_uncounted(&mut self, start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize) -> Result<Vec<(Bytes, Bytes)>> {
        if !self.ensure_open().await? {
            return Ok(vec![]);
        }
        let segs = self.segs_for(start, end).await?;
        let t = self.tenant.as_ref().unwrap();
        match &self.snap {
            Some(s) => crate::layout2::scan(&crate::layout2::Raw::Snap(s), &segs, start, end, reverse, limit, read_ahead).await,
            None => crate::layout2::scan(&crate::layout2::Raw::Tenant(t, false), &segs, start, end, reverse, limit, read_ahead).await,
        }
    }

    /// [`Entry::scan_range`] on the latest committed state.
    pub async fn scan_range_live(&mut self, start: &[u8], end: Option<&[u8]>, limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        let r = self.scan_range_live_uncounted(start, end, limit).await;
        if let Ok(v) = &r {
            crate::fair::note_read(v.read_bytes());
        }
        r
    }

    async fn scan_range_live_uncounted(&mut self, start: &[u8], end: Option<&[u8]>, limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        if !self.ensure_open().await? {
            return Ok(vec![]);
        }
        // (the catalog as the latest state has it, not the snapshot's)
        let snap = self.snap.take();
        let segs = self.segs_for(start, end).await;
        self.snap = snap;
        let segs = segs?;
        crate::layout2::scan(&crate::layout2::Raw::Tenant(self.tenant.as_ref().unwrap(), false), &segs, start, end, false, limit, 4096).await
    }

    /// Take a snapshot for a transaction (opens the tenant as a writer) and remember the write counter.
    pub async fn begin_snapshot(&mut self) -> Result<(Arc<tenant_bench::fx::read::Snap>, u64)> {
        if !self.ensure_open().await? {
            // The database does not exist yet: create it so the transaction has something to snapshot.
            self.leases.acquire(&self.name).await?;
            self.nat.clear();
        self.copts.clear();
        self.ckey_decided.clear();
        self.sstat.clear();
        self.meta_clear();
            crate::opening::forget_db(&self.name);
            self.tenant = Some(self.lc.create(&self.name).await?);
            self.absent = false;
            self.wakes += 1;
            self.sync_gauges();
        }
        // With the log on, a transaction reads a confirmed snapshot (no write its log has not confirmed yet): it waits,
        // holding the database's exclusive lock (nothing new is staged meanwhile), until the batches staged so far are
        // confirmed, usually one log round trip.
        self.tenant.as_ref().unwrap().wait_all_visible(tenant_bench::lifecycle::VISIBLE_WAIT).await?;
        let snap = self.tenant.as_mut().unwrap().txn_snapshot().await?;
        self.sync_gauges();
        self.active_txns += 1;
        Ok((snap, self.seq))
    }

    /// Try to take the write lock of a document key for transaction `id`.
    pub(crate) fn lock_key(&mut self, key: &[u8], id: u64) -> bool {
        match self.locks.get(key) {
            Some(o) if *o != id => false,
            _ => {
                self.locks.insert(key.to_vec(), id);
                true
            }
        }
    }

    pub(crate) fn end_txn(&mut self, id: u64) {
        self.locks.retain(|_, o| *o != id);
        self.active_txns = self.active_txns.saturating_sub(1);
        if self.active_txns == 0 {
            self.modified.clear();
        }
    }

    /// Record that the batch just applied wrote these unique values (see `Writes::uniq`), so that an open transaction that
    /// writes one of them conflicts. Only needed while transactions are open, like the document keys in `apply`.
    pub(crate) fn mark_unique<'a>(&mut self, keys: impl IntoIterator<Item = &'a Vec<u8>>) {
        if self.active_txns == 0 {
            return;
        }
        for k in keys {
            self.modified.insert(k.clone(), self.seq);
        }
    }

    /// Was the document key written after write counter `seq`?
    pub(crate) fn modified_after(&self, key: &[u8], seq: u64) -> bool {
        self.modified.get(key).map(|s| *s > seq).unwrap_or(false)
    }

    /// Point reads in parallel, in key order.
    pub async fn get_many(&mut self, keys: &[Vec<u8>]) -> Result<Vec<Option<Bytes>>> {
        let r = self.get_many_uncounted(keys).await;
        if let Ok(v) = &r {
            crate::fair::note_read(v.read_bytes());
        }
        r
    }

    async fn get_many_uncounted(&mut self, keys: &[Vec<u8>]) -> Result<Vec<Option<Bytes>>> {
        if keys.is_empty() || !self.ensure_open().await? {
            return Ok(vec![None; keys.len()]);
        }
        let t = self.tenant.as_ref().unwrap();
        let par = GET_MANY_PARALLEL.load(Relaxed);
        match &self.snap {
            Some(s) => crate::layout2::get_many(&crate::layout2::Raw::Snap(s), keys, par).await,
            None => crate::layout2::get_many(&crate::layout2::Raw::Tenant(t, false), keys, par).await,
        }
    }

    pub async fn scan_collection(&mut self, coll: &str) -> Result<Vec<Document>> {
        let rows = self.scan(&layout::doc_prefix(coll)).await?;
        rows.into_iter().map(|(_, v)| Ok(Document::from_reader(&v[..])?)).collect()
    }

    pub async fn get_doc(&mut self, coll: &str, id: &Bson) -> Result<Option<Document>> {
        let ckey = crate::custkey::of_opts(self.coll_opts(coll).await?.as_ref());
        // customer-keyed documents: the locators hold the document's suffix under every customer holding the `_id` (the
        // first one is taken; read again if the document moved meanwhile)
        let lp = crate::custkey::loc_id_prefix(coll, &layout::encode_sortable(id));
        let lend = crate::keyenc::prefix_end(&lp);
        for _ in 0..3 {
            let key = match &ckey {
                Some(_) => match self.scan_range(&lp, lend.as_deref(), false, Some(1), 4096).await?.into_iter().next() {
                    Some((_, suf)) => [layout::doc_prefix(coll), suf.to_vec()].concat(),
                    None => return Ok(None),
                },
                None => layout::doc_key(coll, id),
            };
            // (a customer-keyed document is read like its locator, with a range-read kind of read: a `ReadView` with
            // durable reads answers point reads from durable data only, range reads and `get_many` from the latest
            // state, and the two must agree on where the document is)
            let v = if ckey.is_some() { self.get_many(std::slice::from_ref(&key)).await?.pop().flatten() } else { self.get(&key).await? };
            if let Some(v) = v {
                return Ok(Some(Document::from_reader(&v[..])?));
            }
            if ckey.is_none() {
                break;
            }
        }
        Ok(None)
    }

    /// The document of `_id` under the customer value `cust` (a change event's document key). In a collection that
    /// is not customer-keyed (any more), the document of `_id`.
    pub async fn get_doc_of(&mut self, coll: &str, id: &Bson, cust: &Bson) -> Result<Option<Document>> {
        if crate::custkey::of_opts(self.coll_opts(coll).await?.as_ref()).is_none() {
            return self.get_doc(coll, id).await;
        }
        // the locator of (`_id`, customer) holds the document's suffix; read again if the document moved meanwhile
        // (its time changed), with the same kinds of read as [`Entry::get_doc`]
        let lk = crate::custkey::loc_key_of_value(coll, &layout::encode_sortable(id), cust);
        let mut lend = lk.clone();
        lend.push(0);
        for _ in 0..3 {
            let Some((_, suf)) = self.scan_range(&lk, Some(&lend), false, Some(1), 4096).await?.into_iter().next() else { return Ok(None) };
            let key = [layout::doc_prefix(coll), suf.to_vec()].concat();
            if let Some(v) = self.get_many(std::slice::from_ref(&key)).await?.pop().flatten() {
                return Ok(Some(Document::from_reader(&v[..])?));
            }
        }
        Ok(None)
    }

}

/// A shared (read) hold on a database. Every read re-checks that this process still owns the tenant before
/// the result is returned, so a process that lost its lease while the read was in flight returns a retryable
/// error instead of data. `None` inside means the database does not exist.
/// Point reads of one `get_many` in flight at once (T1: 512 with the waiter's tools, so up to 512 documents are one trip).
pub static GET_MANY_PARALLEL: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(32);

pub struct ReadView {
    g: Option<Guard>,
    copts: std::sync::Mutex<HashMap<String, Option<Arc<crate::collopts::CollOpts>>>>,
    /// First course (opening.rs): the session of this view's query, when it is a user's first query of a visit.
    fc: std::sync::Mutex<Option<crate::opening::Session>>,
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
    pub fn tenant_name(&self) -> Option<String> {
        match &self.g {
            Some(Guard::Read(g)) => Some(g.name.clone()),
            Some(Guard::Write(g)) => Some(g.name.clone()),
            None => None,
        }
    }

    fn done<T>(&self, v: T) -> Result<T> {
        if let Some(g) = &self.g {
            g.leases.check_owner(&g.name)?;
        }
        Ok(v)
    }

    pub async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        let r = self.scan_uncounted(prefix).await;
        if let Ok(v) = &r {
            crate::fair::note_read(v.read_bytes());
        }
        r
    }

    async fn scan_uncounted(&self, prefix: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        let Some(g) = &self.g else { return Ok(vec![]) };
        let end = crate::keyenc::prefix_end(prefix);
        let segs = self.segs_for(prefix, end.as_deref()).await?;
        let t = g.tenant.as_ref().unwrap();
        if let Some(r) = self.fc_with(|s| s.scan(prefix, end.as_deref(), false, None)).flatten() {
            self.fc_with(|s| s.note_scan(prefix, end.as_deref(), false, None, &r));
            return self.done(r);
        }
        let raw = if durable_reads(&g.name) { crate::layout2::Raw::Tenant(t, true) } else { crate::layout2::Raw::Visible(t) };
        let r = crate::layout2::scan(&raw, &segs, prefix, end.as_deref(), false, None, 4 << 20).await?;
        self.fc_with(|s| s.note_scan(prefix, end.as_deref(), false, None, &r));
        self.done(r)
    }

    /// See [`Entry::segs_for`].
    async fn segs_for(&self, lo: &[u8], hi: Option<&[u8]>) -> Result<Vec<crate::custkey::Seg>> {
        match crate::custkey::area_of(lo, hi) {
            Some(Some(s)) => return Ok(vec![s]),
            Some(None) => return Ok(vec![]),
            None => {}
        }
        let only = crate::custkey::coll_of_range(lo, hi).map(|(_, esc)| esc);
        let mut segs = Vec::new();
        for n in self.collections().await? {
            let esc = layout::escape_collection(&n);
            if only.as_ref().map_or(false, |o| *o != esc) {
                continue;
            }
            if crate::custkey::of_opts(self.coll_opts(&n).await?.as_ref()).is_some() {
                segs.push(crate::custkey::Seg::docs(&esc));
            }
            for i in self.indexes(&n).await? {
                if i.colocated {
                    segs.push(crate::custkey::Seg::index(&esc, &layout::escape_collection(&i.name)));
                }
            }
        }
        Ok(segs)
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let r = self.get_uncounted(key).await;
        if let Ok(v) = &r {
            crate::fair::note_read(v.read_bytes());
        }
        r
    }

    async fn get_uncounted(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let Some(g) = &self.g else { return Ok(None) };
        let t = g.tenant.as_ref().unwrap();
        if META_CACHE.load(Relaxed) && is_meta_key(key) {
            let r = match g.meta_lookup(key) {
                Some(v) => v,
                None => g.meta_read(t, key, durable_reads(&g.name), true).await?,
            };
            return self.done(r);
        }
        if let Some(v) = self.fc_with(|s| s.get(key)).flatten() {
            return self.done(v);
        }
        let raw = if durable_reads(&g.name) { crate::layout2::Raw::Tenant(t, true) } else { crate::layout2::Raw::Visible(t) };
        let r = crate::layout2::get(&raw, key).await?;
        self.fc_with(|s| s.note_get(key, &r));
        self.done(r)
    }

    fn fc_with<T>(&self, f: impl FnOnce(&mut crate::opening::Session) -> T) -> Option<T> {
        self.fc.lock().unwrap().as_mut().map(f)
    }

    /// First course: start a session if this find is a user's first query of a visit (opening.rs).
    pub async fn first_course_begin(&self, coll: &str, filter: &Document) {
        if !crate::opening::ENABLED.load(Relaxed) {
            return;
        }
        let Some(g) = &self.g else { return };
        let Ok(opts) = self.coll_opts(coll).await else { return };
        let Some(field) = crate::custkey::of_opts(opts.as_ref()) else { return };
        let Ok(indexes) = self.indexes(coll).await else { return };
        // with the log on (`lifecycle::TRACK_VISIBLE`) an opening holds staged writes at once, but a read sees a write only
        // once its record is confirmed: serve or record only while nothing is in flight
        let all_durable = g.tenant.as_ref().map_or(false, |t| (!durable_reads(&g.name) || t.all_durable()) && t.reads_see_latest());
        *self.fc.lock().unwrap() = crate::opening::begin(&g.name, coll, &field.field, filter, &indexes, all_durable);
    }

    /// First course: end of the query (keeps the opening when `ok`).
    pub fn first_course_end(&self, ok: bool) {
        if let Some(s) = self.fc.lock().unwrap().take() {
            crate::opening::finish(s, ok);
        }
    }

    pub async fn get_many(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Bytes>>> {
        let r = self.get_many_uncounted(keys).await;
        if let Ok(v) = &r {
            crate::fair::note_read(v.read_bytes());
        }
        r
    }

    async fn get_many_uncounted(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Bytes>>> {
        let Some(g) = &self.g else { return Ok(vec![None; keys.len()]) };
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let t = g.tenant.as_ref().unwrap();
        // first course: the keys the opening answers exactly, the others from storage (the visible value of each key on
        // the log path, see `lifecycle::TRACK_VISIBLE`; the latest one otherwise)
        let mut out: Vec<Option<Option<Bytes>>> = match self.fc_with(|s| keys.iter().map(|k| s.get(k)).collect::<Vec<_>>()) {
            Some(v) => v,
            None => vec![None; keys.len()],
        };
        let miss: Vec<usize> = (0..keys.len()).filter(|i| out[*i].is_none()).collect();
        let miss_keys: Vec<Vec<u8>> = miss.iter().map(|i| keys[*i].clone()).collect();
        let fetched = crate::layout2::get_many(&crate::layout2::Raw::Visible(t), &miss_keys, GET_MANY_PARALLEL.load(Relaxed)).await?;
        for (i, v) in miss.into_iter().zip(fetched) {
            out[i] = Some(v);
        }
        let r: Vec<Option<Bytes>> = out.into_iter().map(|v| v.unwrap()).collect();
        self.fc_with(|s| {
            for (k, v) in keys.iter().zip(r.iter()) {
                s.note_get(k, v);
            }
        });
        self.done(r)
    }

    pub async fn scan_range(&self, start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize) -> Result<Vec<(Bytes, Bytes)>> {
        let r = self.scan_range_uncounted(start, end, reverse, limit, read_ahead).await;
        if let Ok(v) = &r {
            crate::fair::note_read(v.read_bytes());
        }
        r
    }

    async fn scan_range_uncounted(&self, start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize) -> Result<Vec<(Bytes, Bytes)>> {
        let Some(g) = &self.g else { return Ok(vec![]) };
        if let Some(r) = self.fc_with(|s| s.scan(start, end, reverse, limit)).flatten() {
            self.fc_with(|s| s.note_scan(start, end, reverse, limit, &r));
            return self.done(r);
        }
        let segs = self.segs_for(start, end).await?;
        let r = crate::layout2::scan(&crate::layout2::Raw::Visible(g.tenant.as_ref().unwrap()), &segs, start, end, reverse, limit, read_ahead).await?;
        self.fc_with(|s| s.note_scan(start, end, reverse, limit, &r));
        self.done(r)
    }

    pub fn approx_bytes(&self) -> u64 {
        self.g.as_ref().map(|g| g.approx_bytes()).unwrap_or(0)
    }

    /// Indexes of a collection (see [`Entry::indexes`]).
    pub async fn indexes(&self, coll: &str) -> Result<Vec<crate::index::IndexDef>> {
        let Some(v) = self.get(&layout::index_key(coll)).await? else { return Ok(vec![]) };
        let d = Document::from_reader(&v[..])?;
        Ok(d.get_array("indexes")?.iter().filter_map(|b| b.as_document().and_then(crate::index::IndexDef::from_stored)).collect())
    }

    pub fn note_ttl(&self) {
        if let Some(g) = &self.g {
            g.ttl_seen.store(true, Relaxed);
        }
    }

    /// The names of the collections of the database.
    pub async fn collections(&self) -> Result<Vec<String>> {
        let Some(v) = self.get(layout::META_COLLECTIONS).await? else { return Ok(vec![]) };
        let d = Document::from_reader(&v[..])?;
        Ok(d.get_array("names")?.iter().filter_map(|b| b.as_str().map(str::to_string)).collect())
    }

    pub async fn coll_opts(&self, coll: &str) -> Result<Option<Arc<crate::collopts::CollOpts>>> {
        if let Some(c) = self.copts.lock().unwrap().get(coll) {
            return Ok(c.clone());
        }
        let o = match self.get(&crate::collopts::meta_key(coll)).await? {
            Some(v) => Some(crate::collopts::CollOpts::parse(Document::from_reader(&v[..])?)),
            None => None,
        };
        self.copts.lock().unwrap().insert(coll.to_string(), o.clone());
        Ok(o)
    }

    pub async fn scan_collection(&self, coll: &str) -> Result<Vec<Document>> {
        let rows = self.scan(&layout::doc_prefix(coll)).await?;
        rows.into_iter().map(|(_, v)| Ok(Document::from_reader(&v[..])?)).collect()
    }

    pub async fn get_doc(&self, coll: &str, id: &Bson) -> Result<Option<Document>> {
        let ckey = crate::custkey::of_opts(self.coll_opts(coll).await?.as_ref());
        // customer-keyed documents: the locators hold the document's suffix under every customer holding the `_id` (the
        // first one is taken; read again if the document moved meanwhile)
        let lp = crate::custkey::loc_id_prefix(coll, &layout::encode_sortable(id));
        let lend = crate::keyenc::prefix_end(&lp);
        for _ in 0..3 {
            let key = match &ckey {
                Some(_) => match self.scan_range(&lp, lend.as_deref(), false, Some(1), 4096).await?.into_iter().next() {
                    Some((_, suf)) => [layout::doc_prefix(coll), suf.to_vec()].concat(),
                    None => return Ok(None),
                },
                None => layout::doc_key(coll, id),
            };
            // (a customer-keyed document is read like its locator, with a range-read kind of read: a `ReadView` with
            // durable reads answers point reads from durable data only, range reads and `get_many` from the latest
            // state, and the two must agree on where the document is)
            let v = if ckey.is_some() { self.get_many(std::slice::from_ref(&key)).await?.pop().flatten() } else { self.get(&key).await? };
            if let Some(v) = v {
                return Ok(Some(Document::from_reader(&v[..])?));
            }
            if ckey.is_none() {
                break;
            }
        }
        Ok(None)
    }
}


/// Test hook of the step 7b fix round (option (b) of note 1), off unless `FENVIX_TEST_INTERLEAVE=K` (and
/// `FENVIX_TEST_DROP=1`): before the K-th storage access of a command, a publish of our manifest and a drop of the
/// database's SlateDB rows at or below the new X_d (the step 7a stand-in) run, so they land between two reads of one
/// command. A command never holds a guard of the database across two accesses (each is a scoped block), so this cannot
/// deadlock; it runs in a task of its own, outside the command's scopes.
pub fn interleave_at() -> Option<u32> {
    static K: std::sync::OnceLock<Option<u32>> = std::sync::OnceLock::new();
    *K.get_or_init(|| std::env::var("FENVIX_TEST_INTERLEAVE").ok().and_then(|v| v.parse().ok()))
}

/// The same hook set at run time (`PUT /v1/test/interleave {"database", "at"}`; `DELETE` clears it): only commands on
/// that database count.
pub static INTERLEAVE_SET: std::sync::Mutex<Option<(String, u32)>> = std::sync::Mutex::new(None);

/// Interleavings run (tests read it through serverStatus is not needed: the log says it).
pub static INTERLEAVED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Boxed as a `Send` future: the hook takes `Dbs::lock` itself, which awaits this function (a type cycle otherwise).
fn interleave(name: &str) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
    Box::pin(interleave_inner(name))
}

async fn interleave_inner(name: &str) {
    let set = INTERLEAVE_SET.lock().unwrap().clone();
    let k = match set {
        Some((db, k)) if db == name => k,
        Some(_) => return,
        None => match interleave_at() {
            Some(k) => k,
            None => return,
        },
    };
    let Ok(n) = STEPS.try_with(|c| {
        let n = c.get() + 1;
        c.set(n);
        n
    }) else {
        return;
    };
    if n != k || name == "admin" || name == "config" || name == "local" {
        return;
    }
    let db = name.to_string();
    // in a task of its own: outside the command's scopes, so its own `Dbs::lock` does not count or recurse
    let r = tokio::spawn(async move { crate::ctl::testhooks::publish_and_drop(&db).await }).await;
    match r {
        Ok(Ok(v)) => {
            INTERLEAVED.fetch_add(1, Relaxed);
            eprintln!("INTERLEAVE {name} before access {k}: {v}");
        }
        Ok(Err(e)) => eprintln!("INTERLEAVE {name} before access {k} skipped: {}", e.message),
        Err(e) => eprintln!("INTERLEAVE {name}: {e}"),
    }
}
