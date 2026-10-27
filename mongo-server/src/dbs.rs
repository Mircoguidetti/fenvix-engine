//! One tenant per MongoDB database, driven through the results3 lifecycle: wake as a reader,
//! upgrade to a writer on the first write, clean close after an idle period.

use crate::layout;
use crate::lease::{self, Leases};
use anyhow::{bail, Result};
use bson::{doc, Bson, Document};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tenant_bench::lifecycle::{Lifecycle, Op, Tenant};
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
    pub(crate) snap: Option<Arc<slatedb::DbSnapshot>>,
    /// Write counter and, while transactions are open, the last write of each document key (conflict detection).
    pub(crate) seq: u64,
    pub(crate) modified: HashMap<Vec<u8>, u64>,
    pub(crate) active_txns: usize,
    /// Document keys written by open transactions -> transaction id (a second writer gets a write conflict at once).
    pub(crate) locks: HashMap<Vec<u8>, u64>,
    pub(crate) changes: Arc<crate::changes::ChangeLog>,
}

/// MongoDB database names may not be empty, contain `/\. "$*<>:|?` or a NUL byte.
pub fn valid_db_name(name: &str) -> bool {
    !name.is_empty() && name.len() < 64 && !name.chars().any(|c| "/\\. \"$*<>:|?\0".contains(c))
}

impl Dbs {
    pub fn new(lc: Arc<Lifecycle>, raw: Arc<dyn ObjectStore>, idle: Duration, leases: Arc<Leases>, global: Arc<tenant_bench::counting::CountingStore>) -> Arc<Self> {
        Arc::new(Self { lc, raw, entries: Mutex::new(HashMap::new()), idle, leases, global, readers: Arc::new(AtomicI64::new(0)), writers: Arc::new(AtomicI64::new(0)), ttl: Arc::new(Default::default()), changes: crate::changes::arc_default() })
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
                }))
            })
            .clone()
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
                    }))
                })
                .clone()
        }
    }

    /// Lock a database's entry exclusively (every command that writes, and tenant state changes).
    pub async fn lock(&self, name: &str) -> OwnedRwLockWriteGuard<Entry> {
        let mut g = self.entry(name).write_owned().await;
        g.snap = None;
        g.absent = false;
        g
    }

    /// Open the database for reading and share it with other readers: reads of one tenant run concurrently with
    /// each other and exclude only writes and state changes. The tenant is opened (and its lease taken) under the
    /// exclusive lock first when needed. `ReadView` re-checks ownership after every read.
    pub async fn read(&self, name: &str) -> Result<ReadView> {
        let e = self.entry(name);
        if !SHARED_READS.load(Relaxed) {
            let mut w = Arc::clone(&e).write_owned().await;
            w.absent = false;
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
            w.absent = false;
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
        if self.tenant.is_none() && self.absent {
            return Ok(false);
        }
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
        tenant_bench::counting::set_phase(5);
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
                tenant_bench::counting::set_phase(0);
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
        if let Some(s) = &self.snap {
            return Ok(s.get(key).await?);
        }
        self.tenant.as_ref().unwrap().get(key).await
    }

    /// Like [`Entry::get`] but always reads the latest committed state, not the command's snapshot.
    pub async fn get_live(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        if !self.ensure_open().await? {
            return Ok(None);
        }
        self.tenant.as_ref().unwrap().get(key).await
    }

    pub async fn scan(&mut self, prefix: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        if !self.ensure_open().await? {
            return Ok(vec![]);
        }
        if self.snap.is_some() {
            let end = crate::keyenc::prefix_end(prefix);
            return self.scan_range(prefix, end.as_deref(), false, None, 4 << 20).await;
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
            self.absent = false;
            self.wakes += 1;
            self.sync_gauges();
        }
        self.touch();
        self.seq += 1;
        if self.active_txns > 0 {
            for op in &ops {
                let k = match op {
                    Op::Put(k, _) | Op::Delete(k) => k,
                };
                if k.starts_with(b"doc/") {
                    self.modified.insert(k.clone(), self.seq);
                }
            }
        }
        let r = self.tenant.as_mut().unwrap().apply(ops).await;
        self.sync_gauges();
        r
    }

    /// Close the tenant and delete every object of the database. Holds the tenant's maintenance
    /// lock so no compaction or GC pass can write into it while (or after) it is being deleted:
    /// a pass that finds no manifest does nothing. Repeats the listing until the prefix is empty.
    pub async fn drop_database(&mut self) -> Result<()> {
        crate::ctl::fork::on_drop(&self.name).await?;
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

    pub(crate) fn is_writer_open(&self) -> bool {
        self.tenant.as_ref().map(|t| t.is_writer()).unwrap_or(false)
    }

    pub(crate) fn is_open(&self) -> bool {
        self.tenant.is_some()
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
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

    pub fn indexes_op(coll: &str, idx: &[crate::index::IndexDef]) -> Result<Op> {
        let d = doc! { "indexes": idx.iter().map(|i| Bson::Document(i.to_stored())).collect::<Vec<_>>() };
        Ok(Op::Put(layout::index_key(coll), d.to_vec()?))
    }

    /// [`Tenant::approx_bytes`] of the open tenant (0 if closed).
    pub fn approx_bytes(&self) -> u64 {
        self.tenant.as_ref().map(|t| t.approx_bytes()).unwrap_or(0)
    }

    pub async fn scan_range(&mut self, start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize) -> Result<Vec<(Bytes, Bytes)>> {
        if !self.ensure_open().await? {
            return Ok(vec![]);
        }
        if let Some(s) = &self.snap {
            let opts = slatedb::config::ScanOptions {
                read_ahead_bytes: read_ahead,
                max_fetch_tasks: 4,
                cache_blocks: true,
                order: if reverse { slatedb::IterationOrder::Descending } else { slatedb::IterationOrder::Ascending },
                ..Default::default()
            };
            let range = match end {
                Some(e) => (std::ops::Bound::Included(start.to_vec()), std::ops::Bound::Excluded(e.to_vec())),
                None => (std::ops::Bound::Included(start.to_vec()), std::ops::Bound::Unbounded),
            };
            let mut it = s.scan_with_options(range, &opts).await?;
            let mut out = Vec::new();
            while let Some(kv) = it.next().await? {
                out.push((kv.key, kv.value));
                if limit.map(|l| out.len() >= l).unwrap_or(false) {
                    break;
                }
            }
            return Ok(out);
        }
        self.tenant.as_ref().unwrap().scan_range(start, end, reverse, limit, read_ahead).await
    }

    /// [`Entry::scan_range`] on the latest committed state.
    pub async fn scan_range_live(&mut self, start: &[u8], end: Option<&[u8]>, limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        if !self.ensure_open().await? {
            return Ok(vec![]);
        }
        self.tenant.as_ref().unwrap().scan_range(start, end, false, limit, 4096).await
    }

    /// Take a snapshot for a transaction (opens the tenant as a writer) and remember the write counter.
    pub async fn begin_snapshot(&mut self) -> Result<(Arc<slatedb::DbSnapshot>, u64)> {
        if !self.ensure_open().await? {
            // The database does not exist yet: create it so the transaction has something to snapshot.
            self.leases.acquire(&self.name).await?;
            self.tenant = Some(self.lc.create(&self.name).await?);
            self.absent = false;
            self.wakes += 1;
            self.sync_gauges();
        }
        let snap = self.tenant.as_mut().unwrap().snapshot().await?;
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

    /// Was the document key written after write counter `seq`?
    pub(crate) fn modified_after(&self, key: &[u8], seq: u64) -> bool {
        self.modified.get(key).map(|s| *s > seq).unwrap_or(false)
    }

    /// Point reads in parallel, in key order.
    pub async fn get_many(&mut self, keys: &[Vec<u8>]) -> Result<Vec<Option<Bytes>>> {
        if keys.is_empty() || !self.ensure_open().await? {
            return Ok(vec![None; keys.len()]);
        }
        let t = self.tenant.as_ref().unwrap();
        if let Some(s) = &self.snap {
            let futs: Vec<_> = keys.iter().map(|k| s.get(k.clone())).collect();
            return Ok(futures::stream::iter(futs).buffered(32).try_collect::<Vec<_>>().await?);
        }
        let futs: Vec<_> = keys.iter().map(|k| t.get(k.clone())).collect();
        futures::stream::iter(futs).buffered(32).try_collect().await
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
        let r = g.tenant.as_ref().unwrap().scan_prefix(prefix).await?;
        self.done(r)
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let Some(g) = &self.g else { return Ok(None) };
        let r = g.tenant.as_ref().unwrap().get(key).await?;
        self.done(r)
    }

    pub async fn get_many(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Bytes>>> {
        let Some(g) = &self.g else { return Ok(vec![None; keys.len()]) };
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let t = g.tenant.as_ref().unwrap();
        let futs: Vec<_> = keys.iter().map(|k| t.get(k.clone())).collect();
        let r = futures::stream::iter(futs).buffered(32).try_collect::<Vec<_>>().await?;
        self.done(r)
    }

    pub async fn scan_range(&self, start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize) -> Result<Vec<(Bytes, Bytes)>> {
        let Some(g) = &self.g else { return Ok(vec![]) };
        let r = g.tenant.as_ref().unwrap().scan_range(start, end, reverse, limit, read_ahead).await?;
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
