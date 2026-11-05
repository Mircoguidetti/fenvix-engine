//! Meters for the server's statistics (K1): the shared block and metadata cache (the "bancone") with its size, hits and
//! misses, and the object-store reads of one operation (bytes and time read from S3), for `serverStatus` and the profiler.
//!
//! - [`MeteredCache`] is slatedb's foyer cache (same weigher, same deduplicated fetch) that also says how many bytes it
//!   holds and counts its lookups;
//! - [`OpMeter`] is set around one command ([`OP_METER`]): every GET the counting store makes while that command's
//!   future is polled adds its bytes and its time (from the request to the end of the response headers; the body of a
//!   small range arrives with them). Reads that slatedb hands to a task of its own (read-ahead of a scan) are not seen.

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
}

impl OpMeter {
    pub fn read(&self) -> (u64, u64, u64) {
        (self.gets.load(Relaxed), self.bytes.load(Relaxed), self.micros.load(Relaxed))
    }
}

tokio::task_local! {
    /// The meter of the operation whose future is being polled (see the module comment).
    pub static OP_METER: Arc<OpMeter>;
}

/// Called by the counting store after each GET (and ranged GET): charge it to the current operation, if one is metered.
pub fn note_get(bytes: u64, elapsed: std::time::Duration) {
    let _ = OP_METER.try_with(|m| {
        m.gets.fetch_add(1, Relaxed);
        m.bytes.fetch_add(bytes, Relaxed);
        m.micros.fetch_add(elapsed.as_micros() as u64, Relaxed);
    });
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

    #[test]
    fn cache_reports_usage() {
        let c = MeteredCache::new(1 << 20);
        assert_eq!(c.usage(), 0);
        assert_eq!(c.capacity(), 1 << 20);
    }
}
