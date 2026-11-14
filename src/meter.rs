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

// ---- newest10-trips: the box's pins
//
// A customer's first find warms its box (`mongo-server/src/custbox.rs`): one wave of GETs that brings, among the rest, the
// index blocks and data blocks of every SlateDB SST that holds the customer's range. The query of the same request then
// reads those SSTs again. MEASURED (bench/newest10, caches small next to the data as at 10 GB under 512 MiB): between the
// two, foyer's cache had already evicted most of what the box loaded (a new entry sits in a small probation queue and
// leaves at the next inserts), so the query's SST iterators loaded each SST index again, one SST after the other: 14
// GETs in 5 trips instead of 1 trip. A pin keeps every entry the shared cache served, loaded or was given while a box
// request runs, until that request ends, under a byte cap; lookups find a pinned entry when the cache lost it.
//
// Why the answers cannot change: an entry is keyed by (SST id, offset) and an SST's bytes never change (`counter.rs`), so a
// pinned entry is exactly what the store would return; pins only change whether a lookup needs a GET.

/// On by default (`--customer-box-pin`).
pub static PIN_ON: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);
/// Bytes all pins together may hold (set from the box's in-flight allowance).
pub static PIN_CAP: AtomicU64 = AtomicU64::new(64 << 20);
/// Lookups answered by a pin (the cache had lost the entry), entries pinned, pins refused at the cap, most bytes pinned.
pub static PIN_HITS: AtomicU64 = AtomicU64::new(0);
pub static PIN_ENTRIES: AtomicU64 = AtomicU64::new(0);
pub static PIN_FULL: AtomicU64 = AtomicU64::new(0);
pub static PIN_BYTES_MAX: AtomicU64 = AtomicU64::new(0);
static PIN_ACTIVE: AtomicU64 = AtomicU64::new(0);
static PIN_NEXT: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
struct PinTable {
    entries: std::collections::HashMap<CachedKey, (CachedEntry, Vec<u64>)>,
    bytes: u64,
    active: Vec<u64>,
}

static PINS: std::sync::LazyLock<std::sync::Mutex<PinTable>> = std::sync::LazyLock::new(Default::default);

/// Held by one request from the start of its box's warm-up to the end of its query; dropping it unpins what only it held.
pub struct BoxPin(u64);

/// Start pinning for one request (None when pins are off).
pub fn pin_start() -> Option<BoxPin> {
    if !PIN_ON.load(Relaxed) {
        return None;
    }
    let id = PIN_NEXT.fetch_add(1, Relaxed);
    PINS.lock().unwrap().active.push(id);
    PIN_ACTIVE.fetch_add(1, Relaxed);
    Some(BoxPin(id))
}

impl Drop for BoxPin {
    fn drop(&mut self) {
        let mut t = PINS.lock().unwrap();
        t.active.retain(|&a| a != self.0);
        PIN_ACTIVE.fetch_sub(1, Relaxed);
        let mut freed = 0u64;
        t.entries.retain(|_, (e, owners)| {
            owners.retain(|&o| o != self.0);
            if owners.is_empty() {
                freed += e.size() as u64;
                false
            } else {
                true
            }
        });
        t.bytes -= freed;
    }
}

fn pin_note(key: &CachedKey, e: &CachedEntry) {
    if PIN_ACTIVE.load(Relaxed) == 0 {
        return;
    }
    let mut t = PINS.lock().unwrap();
    if t.active.is_empty() {
        return;
    }
    let active = t.active.clone();
    if let Some((_, owners)) = t.entries.get_mut(key) {
        for a in active {
            if !owners.contains(&a) {
                owners.push(a);
            }
        }
        return;
    }
    let n = e.size() as u64;
    if t.bytes + n > PIN_CAP.load(Relaxed) {
        PIN_FULL.fetch_add(1, Relaxed);
        return;
    }
    t.bytes += n;
    PIN_BYTES_MAX.fetch_max(t.bytes, Relaxed);
    PIN_ENTRIES.fetch_add(1, Relaxed);
    t.entries.insert(key.clone(), (e.clone(), active));
}

fn pin_get(key: &CachedKey) -> Option<CachedEntry> {
    if PIN_ACTIVE.load(Relaxed) == 0 {
        return None;
    }
    let r = PINS.lock().unwrap().entries.get(key).map(|(e, _)| e.clone());
    if r.is_some() {
        PIN_HITS.fetch_add(1, Relaxed);
    }
    r
}

/// (entries, bytes) pinned now.
pub fn pinned_now() -> (u64, u64) {
    let t = PINS.lock().unwrap();
    (t.entries.len() as u64, t.bytes)
}

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
        let r = match self.inner.get(key).map(|e| e.value().clone()) {
            Some(v) => {
                pin_note(key, &v);
                Some(v)
            }
            None => pin_get(key),
        };
        self.looked(r.is_some());
        r
    }

    async fn dedup_fetch(&self, key: CachedKey, loader: CacheLoader) -> Result<CacheFetch, slatedb::Error> {
        if let Some(v) = pin_get(&key) {
            self.looked(true);
            return Ok(CacheFetch { entry: v, lookup: CacheLookup::Hit });
        }
        let pin_key = (PIN_ACTIVE.load(Relaxed) > 0).then(|| key.clone());
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ran2 = Arc::clone(&ran);
        // foyer calls this closure (and builds the loader's future) on a hit too, and polls the future only on a miss:
        // the flag is set when the future runs. (Set in the closure, every lookup counted as a miss: serverStatus showed
        // 3,617 hits for 2.2 million lookups and 21 GB "loaded" against 8.4 GB of GETs, perf-latency round 3.)
        let fetch = self.inner.get_or_fetch(&key, move || {
            let fut = loader();
            async move {
                ran2.store(true, Relaxed);
                // copied out of the GET's receive buffer before it is kept (see `insert`)
                fut.await.map(|e| e.clamp_allocated_size())
            }
        });
        match fetch.await {
            Ok(entry) => {
                let v = entry.value().clone();
                let miss = ran.load(Relaxed);
                self.looked(!miss);
                if miss {
                    self.loaded_bytes.fetch_add(v.size() as u64, Relaxed);
                }
                if let Some(k) = &pin_key {
                    pin_note(k, &v);
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
        // P-big-speed fix 1: a block read from the store is a slice of the HTTP receive buffer of its GET; kept as it is,
        // one cached 4 KiB block held that whole buffer (MEASURED, heap profile under load: 412 of 692 MB in HTTP read
        // buffers, with 47 MB of blocks counted by this cache; the server was OOM-killed at 1 GiB). Copied out first.
        let value = value.clamp_allocated_size();
        pin_note(&key, &value);
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
