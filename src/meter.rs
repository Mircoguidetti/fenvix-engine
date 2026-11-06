//! Meters for the server's statistics (K1): the shared block and metadata cache with its size, hits and
//! misses, and the object-store reads of one operation (bytes and time read from S3), for `serverStatus` and the profiler.
//!
//! - [`MeteredCache`] is slatedb's foyer cache (same weigher, same deduplicated fetch) that also says how many bytes it
//!   holds and counts its lookups;
//! - [`OpMeter`] is set around one command ([`OP_METER`]): every GET the counting store makes while that command's
//!   future is polled adds its bytes and its time (from the request to the end of the response headers; the body of a
//!   small range arrives with them). The block reads of a scan run in tasks slatedb spawns (no task-local there): a GET
//!   made outside any command, for a database (the owner of the request, `counting::request_owner`) that has commands
//!   running ([`register`]), is charged to them: in full when one command runs on that database, split evenly and marked
//!   `shared` when several do. Background work is charged to no command: the compactor (its store is marked background),
//!   and opening or reopening a database, its upgrade to a writer and the writing of profiler entries ([`background`]),
//!   whose reads go to [`BACKGROUND`].

use async_trait::async_trait;
use slatedb::db_cache::{CacheFetch, CacheLoader, CacheLookup, CachedEntry, CachedKey, DbCache};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

/// slatedb's foyer cache, metered.
pub struct MeteredCache {
    inner: foyer::Cache<CachedKey, CachedEntry>,
    capacity: u64,
    /// Lookups answered from memory (a `get_*` that found the entry, a fetch that did not load it).
    pub hits: AtomicU64,
    /// Lookups that had to read the store (a `get_*` that found nothing, a fetch that loaded).
    pub misses: AtomicU64,
    /// Bytes of the entries a fetch loaded from the store.
    pub loaded_bytes: AtomicU64,
    /// Bytes of the entries put in by `insert` (blocks read by a multi-block read, flushed or warmed blocks).
    pub inserted_bytes: AtomicU64,
}

impl MeteredCache {
    pub fn new(capacity: u64) -> Self {
        let shards = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
        let inner = foyer::CacheBuilder::new(capacity as usize).with_weighter(|_, v: &CachedEntry| v.size()).with_shards(shards).build();
        MeteredCache { inner, capacity, hits: AtomicU64::new(0), misses: AtomicU64::new(0), loaded_bytes: AtomicU64::new(0), inserted_bytes: AtomicU64::new(0) }
    }

    /// Bytes the cache holds now.
    pub fn usage(&self) -> u64 {
        self.inner.usage() as u64
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn entries(&self) -> u64 {
        self.inner.entries() as u64
    }

    fn looked(&self, found: bool) {
        if found {
            self.hits.fetch_add(1, Relaxed);
        } else {
            self.misses.fetch_add(1, Relaxed);
        }
    }

    fn get(&self, key: &CachedKey) -> Option<CachedEntry> {
        let r = self.inner.get(key).map(|e| e.value().clone());
        self.looked(r.is_some());
        r
    }

    async fn dedup_fetch(&self, key: CachedKey, loader: CacheLoader) -> Result<CacheFetch, slatedb::Error> {
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ran2 = Arc::clone(&ran);
        let fetch = self.inner.get_or_fetch(&key, move || {
            ran2.store(true, Relaxed);
            loader()
        });
        match fetch.await {
            Ok(entry) => {
                let v = entry.value().clone();
                let miss = ran.load(Relaxed);
                self.looked(!miss);
                if miss {
                    self.loaded_bytes.fetch_add(v.size() as u64, Relaxed);
                }
                Ok(CacheFetch { entry: v, lookup: if miss { CacheLookup::Miss } else { CacheLookup::Hit } })
            }
            Err(e) => Err(slatedb::Error::unavailable(format!("cache fetch failed: {e}"))),
        }
    }
}

#[async_trait]
impl DbCache for MeteredCache {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self.get(key))
    }

    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self.get(key))
    }

    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self.get(key))
    }

    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self.get(key))
    }

    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        self.inserted_bytes.fetch_add(value.size() as u64, Relaxed);
        self.inner.insert(key, value);
    }

    async fn remove(&self, key: &CachedKey) {
        self.inner.remove(key);
    }

    fn entry_count(&self) -> u64 {
        self.inner.entries() as u64
    }

    async fn fetch_block(&self, key: CachedKey, loader: CacheLoader) -> Result<CacheFetch, slatedb::Error> {
        self.dedup_fetch(key, loader).await
    }

    async fn fetch_index(&self, key: CachedKey, loader: CacheLoader) -> Result<CacheFetch, slatedb::Error> {
        self.dedup_fetch(key, loader).await
    }

    async fn fetch_filter(&self, key: CachedKey, loader: CacheLoader) -> Result<CacheFetch, slatedb::Error> {
        self.dedup_fetch(key, loader).await
    }

    async fn fetch_stats(&self, key: CachedKey, loader: CacheLoader) -> Result<CacheFetch, slatedb::Error> {
        self.dedup_fetch(key, loader).await
    }
}

/// The two halves of the shared cache, kept for their meters (`install_shared_cache`).
pub static CACHES: std::sync::OnceLock<(Arc<MeteredCache>, Arc<MeteredCache>)> = std::sync::OnceLock::new();

/// A copy of the meters of the shared cache.
#[derive(Debug, Default, Clone, Copy)]
pub struct CacheStats {
    pub block_usage: u64,
    pub block_capacity: u64,
    pub block_entries: u64,
    pub meta_usage: u64,
    pub meta_capacity: u64,
    pub hits: u64,
    pub misses: u64,
    pub loaded_bytes: u64,
    pub inserted_bytes: u64,
}

pub fn cache_stats() -> Option<CacheStats> {
    if CACHES.get().is_none() {
        // the waiter's counter (T1) instead of the two foyer caches
        let c = crate::counter::COUNTER.get()?;
        let (mu, _, du, dn, _) = c.ram_usage();
        let m = &c.m;
        return Some(CacheStats {
            block_usage: du,
            block_capacity: c.ram_capacity(),
            block_entries: dn,
            meta_usage: mu,
            meta_capacity: 0,
            hits: m.hits_ram.load(Relaxed) + m.hits_disk.load(Relaxed) + m.maps_hits.load(Relaxed),
            misses: m.misses.load(Relaxed) + m.maps_misses.load(Relaxed),
            loaded_bytes: m.loaded_bytes.load(Relaxed),
            inserted_bytes: m.inserted_bytes.load(Relaxed),
        });
    }
    let (b, m) = CACHES.get()?;
    Some(CacheStats {
        block_usage: b.usage(),
        block_capacity: b.capacity(),
        block_entries: b.entries(),
        meta_usage: m.usage(),
        meta_capacity: m.capacity(),
        hits: b.hits.load(Relaxed) + m.hits.load(Relaxed),
        misses: b.misses.load(Relaxed) + m.misses.load(Relaxed),
        loaded_bytes: b.loaded_bytes.load(Relaxed) + m.loaded_bytes.load(Relaxed),
        inserted_bytes: b.inserted_bytes.load(Relaxed) + m.inserted_bytes.load(Relaxed),
    })
}

/// Object-store reads made for one operation.
#[derive(Debug, Default)]
pub struct OpMeter {
    pub gets: AtomicU64,
    pub bytes: AtomicU64,
    pub micros: AtomicU64,
    /// Some reads were split with other commands running on the same database (the numbers are an estimate).
    pub shared: std::sync::atomic::AtomicBool,
}

impl OpMeter {
    pub fn read(&self) -> (u64, u64, u64) {
        (self.gets.load(Relaxed), self.bytes.load(Relaxed), self.micros.load(Relaxed))
    }

    pub fn is_shared(&self) -> bool {
        self.shared.load(Relaxed)
    }

    fn add(&self, gets: u64, bytes: u64, micros: u64) {
        self.gets.fetch_add(gets, Relaxed);
        self.bytes.fetch_add(bytes, Relaxed);
        self.micros.fetch_add(micros, Relaxed);
    }
}

/// The meters of the commands running on each database (see the module comment).
static ACTIVE: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<String, Vec<Arc<OpMeter>>>>> = std::sync::LazyLock::new(Default::default);

/// Keeps a command's meter registered for its database while it runs.
pub struct Registered {
    db: String,
    m: Arc<OpMeter>,
}

impl Drop for Registered {
    fn drop(&mut self) {
        let mut a = ACTIVE.lock().unwrap();
        if let Some(v) = a.get_mut(&self.db) {
            v.retain(|x| !Arc::ptr_eq(x, &self.m));
            if v.is_empty() {
                a.remove(&self.db);
            }
        }
    }
}

/// Register a command's meter for the database it runs on, until the returned guard is dropped.
pub fn register(db: &str, m: &Arc<OpMeter>) -> Registered {
    ACTIVE.lock().unwrap().entry(db.to_string()).or_default().push(Arc::clone(m));
    Registered { db: db.to_string(), m: Arc::clone(m) }
}

tokio::task_local! {
    /// The meter of the operation whose future is being polled (see the module comment).
    pub static OP_METER: Arc<OpMeter>;
}

/// Reads of the background work of every database that are not charged to any command (opening or reopening a
/// database, upgrading it to a writer, writing profiler entries: [`background`]): GETs, bytes, microseconds.
pub static BACKGROUND: OpMeter = OpMeter {
    gets: AtomicU64::new(0),
    bytes: AtomicU64::new(0),
    micros: AtomicU64::new(0),
    shared: std::sync::atomic::AtomicBool::new(false),
};

/// Databases with background work running ([`background`]), with how many such works run on each.
static IN_BACKGROUND: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<String, usize>>> = std::sync::LazyLock::new(Default::default);

/// Run `work`, background work of database `db` (opening or reopening it, its upgrade to a writer, writing profiler entries),
/// so that its reads go to [`BACKGROUND`] and not to a command: the reads made while `work` is polled (also inside a
/// command's future, where they would be charged to that command), and the reads slatedb makes for `db` in tasks it spawns
/// while `work` runs (index, filter and WAL fetches of the open). While it runs, the reads of the commands of `db` made in
/// such spawned tasks are not charged either; there are none in practice: a command of `db` waits for the open it needs.
pub async fn background<F: std::future::Future>(db: &str, work: F) -> F::Output {
    struct Running(String);
    impl Drop for Running {
        fn drop(&mut self) {
            let mut m = IN_BACKGROUND.lock().unwrap();
            if let Some(n) = m.get_mut(&self.0) {
                *n -= 1;
                if *n == 0 {
                    m.remove(&self.0);
                }
            }
        }
    }
    *IN_BACKGROUND.lock().unwrap().entry(db.to_string()).or_default() += 1;
    let _running = Running(db.to_string());
    BACKGROUND_SCOPE.scope((), work).await
}

tokio::task_local! {
    /// Set while background work ([`background`]) is polled.
    static BACKGROUND_SCOPE: ();
}

/// Called by the counting store after each GET (and ranged GET): charge it to the current operation, if one is metered,
/// otherwise to the commands running on the database that made the request (see the module comment). Reads of background
/// work ([`background`], the compactor's background store) are charged to no command.
pub fn note_get(bytes: u64, elapsed: std::time::Duration) {
    let us = elapsed.as_micros() as u64;
    if BACKGROUND_SCOPE.try_with(|_| ()).is_ok() {
        BACKGROUND.add(1, bytes, us);
        return;
    }
    if OP_METER.try_with(|m| m.add(1, bytes, us)).is_ok() {
        return;
    }
    if crate::counting::is_background_request() {
        return;
    }
    let Some(owner) = crate::counting::request_owner() else { return };
    if IN_BACKGROUND.lock().unwrap().contains_key(&*owner) {
        BACKGROUND.add(1, bytes, us);
        return;
    }
    let a = ACTIVE.lock().unwrap();
    let Some(v) = a.get(&*owner) else { return };
    let n = v.len() as u64;
    for m in v {
        m.add(1, bytes / n, us / n);
        if n > 1 {
            m.shared.store(true, Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn op_meter_counts_only_inside_its_scope() {
        note_get(10, std::time::Duration::from_micros(5)); // no meter: nothing
        let m = Arc::new(OpMeter::default());
        OP_METER
            .scope(Arc::clone(&m), async {
                note_get(100, std::time::Duration::from_micros(30));
                tokio::task::yield_now().await;
                note_get(50, std::time::Duration::from_micros(20));
            })
            .await;
        assert_eq!(m.read(), (2, 150, 50));
    }

    #[tokio::test]
    async fn spawned_reads_go_to_the_commands_of_their_database() {
        let a = Arc::new(OpMeter::default());
        let ra = register("dba", &a);
        // a read made by a task slatedb spawned for "dba": no meter in scope, the owner names the database
        crate::counting::with_owner_for_test("dba", || note_get(100, std::time::Duration::from_micros(40)));
        assert_eq!(a.read(), (1, 100, 40));
        assert!(!a.is_shared());
        let b = Arc::new(OpMeter::default());
        let _rb = register("dba", &b);
        crate::counting::with_owner_for_test("dba", || note_get(100, std::time::Duration::from_micros(40)));
        assert_eq!(a.read(), (2, 150, 60));
        assert_eq!(b.read(), (1, 50, 20));
        assert!(a.is_shared() && b.is_shared());
        // another database's read is not charged
        crate::counting::with_owner_for_test("dbz", || note_get(999, std::time::Duration::from_micros(1)));
        assert_eq!(b.read(), (1, 50, 20));
        drop(ra);
        crate::counting::with_owner_for_test("dba", || note_get(10, std::time::Duration::from_micros(2)));
        assert_eq!(a.read(), (2, 150, 60));
        assert_eq!(b.read(), (2, 60, 22));
    }

    #[tokio::test]
    async fn background_work_is_charged_to_no_command() {
        let m = Arc::new(OpMeter::default());
        let _r = register("dbbg", &m);
        let before = BACKGROUND.read();
        OP_METER
            .scope(Arc::clone(&m), async {
                // the open of the database inside the command's future
                background("dbbg", async { note_get(1000, std::time::Duration::from_micros(100)) }).await;
                note_get(7, std::time::Duration::from_micros(3));
            })
            .await;
        assert_eq!(m.read(), (1, 7, 3));
        // a task slatedb spawned for the open, while the open runs
        let gate = Arc::new(tokio::sync::Notify::new());
        let g = Arc::clone(&gate);
        let open = tokio::spawn(background("dbbg", async move { g.notified().await }));
        tokio::task::yield_now().await;
        while !IN_BACKGROUND.lock().unwrap().contains_key("dbbg") {
            tokio::task::yield_now().await;
        }
        crate::counting::with_owner_for_test("dbbg", || note_get(500, std::time::Duration::from_micros(50)));
        assert_eq!(m.read(), (1, 7, 3));
        gate.notify_one();
        open.await.unwrap();
        // once the open is over, the scan reads of the command are charged to it again
        crate::counting::with_owner_for_test("dbbg", || note_get(20, std::time::Duration::from_micros(2)));
        assert_eq!(m.read(), (2, 27, 5));
        let after = BACKGROUND.read();
        assert!(after.0 >= before.0 + 2 && after.1 >= before.1 + 1500);
    }

    #[test]
    fn cache_reports_usage() {
        let c = MeteredCache::new(1 << 20);
        assert_eq!(c.usage(), 0);
        assert_eq!(c.capacity(), 1 << 20);
    }
}
