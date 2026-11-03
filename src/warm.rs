//! Compaction warm-up: the compaction output that replaces blocks an open database had in its block cache is read into
//! that cache before the database switches to the new manifest.
//!
//! Why: a compaction rewrites SSTs. The block cache is keyed by (cache scope, SST id, block offset), so every block of a
//! new SST is a miss for every reader until somebody reads it back from object storage, even when the same rows were hot
//! in the SSTs it replaces. The standalone compactor of `lifecycle::compact_tenant_within` has no cache at all in slatedb
//! 0.17 (`CompactorBuilder` takes no `DbCache`), so after each pass the hot reads of an open database went back to S3.
//!
//! How (only slatedb's public API):
//! 1. Before the pass, the writer's manifest says which SSTs its cache can hold.
//! 2. After the pass, for every input SST that the pass consumed, its index (from the shared metadata cache when it is
//!    there) gives the offset and first key of each data block; a block whose key is in the shared block cache under the
//!    writer's scope is hot. Its key span (first key up to the next block's first key) is a hot range.
//! 3. A `DbReader` opened with the writer's cache scope sees the new manifest; `warm_sst` on it reads, for each new SST,
//!    the index, the filters and the data blocks that overlap the hot ranges (contiguous blocks in one range GET). The
//!    entries land under the writer's scope, i.e. exactly where the writer will look them up.
//! 4. Then the writer reads the new manifest (`refresh_manifest`, as before) and the blocks of the consumed SSTs are
//!    evicted from its cache (they are no longer reachable from its manifest).
//!
//!
//! Bounds (C9, after the C8 review: 200 MB, 2% hot keys spread out, 20 ms per GET gave 45.5% served and a pass 20 s
//! longer): the ranges are warmed by many `warm_sst` calls in parallel (`COMPACTION_WARM_CONCURRENCY`, a few ranges per
//! call, since one call reads its ranges one after the other); one time limit (`COMPACTION_WARM_MS`, 3 s) covers the whole
//! warm-up, past it the warm-up stops where it is, keeps what it read, closes its reader and the writer switches anyway;
//! the hot keys kept in memory and the ranges read are capped (`COMPACTION_WARM_MAX_KEY_BYTES`, `..._MAX_RANGES`): past a
//! cap a part of the hot set is warmed, not none.
//!
//! Correctness: SSTs are immutable and named by a ULID that is never reused, so a cached block can only be the bytes the
//! writer would have read itself; warming and eviction only add or remove such entries. No read can see older data.
//! A failure or a timeout of the warm-up only leaves the cache as it was before this module existed.

use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use object_store::ObjectStore;
use slatedb::config::DbReaderOptions;
use slatedb::db_cache::{CachedEntry, CachedKey, DbCache};
use slatedb::manifest::{SsTableHandle, SsTableId};
use slatedb::{CacheTarget, Db, DbCacheManagerOps, DbReader, DbReaderMode, SstReader, VersionedManifest};
use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Warm the compaction output of open writers (`--compaction-warm`, on by default).
pub static COMPACTION_WARM: AtomicBool = AtomicBool::new(true);
/// Upper bound of one warm-up, in milliseconds (`--compaction-warm-ms`). Past it the warm-up stops where it is (what it
/// already read stays in the cache), its reader is closed and the writer switches to the new manifest anyway. Short on
/// purpose: until the switch the writer still counts the compacted L0 SSTs (C8: 20 s of delay with a 200 MB database).
pub static COMPACTION_WARM_MS: AtomicU64 = AtomicU64::new(3000);
/// Warm-up requests in flight at once (`--compaction-warm-concurrency`). slatedb's `warm_sst` reads the ranges of one
/// call one after the other, so the parallelism is the number of calls in flight (C8: at most 8 calls, one per SST, gave
/// about 2 GETs at a time). 128 calls at about 40 ms per GET stay under S3's 5,500 GET/s per prefix.
pub static COMPACTION_WARM_CONCURRENCY: AtomicU64 = AtomicU64::new(128);
/// Most bytes of hot keys one warm-up keeps in memory (`--compaction-warm-max-key-mb`). Past it the remaining hot blocks
/// are not warmed (counted in `WARM_TRUNCATED`): a part of the hot set is warmed instead of none.
pub static COMPACTION_WARM_MAX_KEY_BYTES: AtomicU64 = AtomicU64::new(32 << 20);
/// Most ranges (one range GET each) one warm-up reads. Past it the rest are not warmed (`WARM_TRUNCATED`).
pub static COMPACTION_WARM_MAX_RANGES: AtomicU64 = AtomicU64::new(200_000);
/// Data ranges per `warm_sst` call: a call reads its ranges one after the other, calls run in parallel. Tests change it.
pub static RANGES_PER_CALL: AtomicU64 = AtomicU64::new(2);
/// Longest wait for the warm-up reader to close (it has no work left once the warm-up is over).
const READER_CLOSE_LIMIT: Duration = Duration::from_secs(5);
/// Totals: warm-ups run, hot blocks found in the consumed SSTs, hot ranges warmed, warm-ups failed, warm-ups stopped by
/// the time limit, warm-ups that warmed only a part (key-bytes or range limit), warm-up readers open now.
pub static WARM_RUNS: AtomicU64 = AtomicU64::new(0);
pub static WARM_HOT_BLOCKS: AtomicU64 = AtomicU64::new(0);
pub static WARM_RANGES: AtomicU64 = AtomicU64::new(0);
pub static WARM_FAILS: AtomicU64 = AtomicU64::new(0);
pub static WARM_TIMEOUTS: AtomicU64 = AtomicU64::new(0);
pub static WARM_TRUNCATED: AtomicU64 = AtomicU64::new(0);
pub static WARM_READERS_OPEN: AtomicU64 = AtomicU64::new(0);

/// The block-cache key of block `offset` of SST `id` under `scope`. slatedb keeps the fields of `CachedKey` private and
/// builds keys only for its own scope; its serialized form (`db_cache/serde.rs`, for disk caches) is public and stable:
/// `V3(scope, ulid, offset)`.
pub fn block_key(scope: u64, id: &SsTableId, offset: u64) -> Option<CachedKey> {
    serde_json::from_value(serde_json::json!({ "V3": [scope, id.value().to_string(), offset] })).ok()
}

/// The same key under another scope.
fn rescope(key: &CachedKey, scope: u64) -> Option<CachedKey> {
    let mut v = serde_json::to_value(key).ok()?;
    let a = v.get_mut("V3")?.as_array_mut()?;
    *a.get_mut(0)? = scope.into();
    serde_json::from_value(v).ok()
}

/// A view of a shared cache that puts every key under one scope: an `SstReader` (which has no scope of its own) then reads
/// the SST indexes a database already has in the shared metadata cache instead of fetching them again.
struct ScopedCache {
    inner: Arc<dyn DbCache>,
    scope: u64,
}

impl ScopedCache {
    fn key(&self, k: &CachedKey) -> Option<CachedKey> {
        rescope(k, self.scope)
    }
}

#[async_trait::async_trait]
impl DbCache for ScopedCache {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        match self.key(key) {
            Some(k) => self.inner.get_block(&k).await,
            None => Ok(None),
        }
    }
    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        match self.key(key) {
            Some(k) => self.inner.get_index(&k).await,
            None => Ok(None),
        }
    }
    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        match self.key(key) {
            Some(k) => self.inner.get_filter(&k).await,
            None => Ok(None),
        }
    }
    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        match self.key(key) {
            Some(k) => self.inner.get_stats(&k).await,
            None => Ok(None),
        }
    }
    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        if let Some(k) = self.key(&key) {
            self.inner.insert(k, value).await
        }
    }
    async fn remove(&self, key: &CachedKey) {
        if let Some(k) = self.key(key) {
            self.inner.remove(&k).await
        }
    }
    fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }
}

/// What the writer's cache could hold before the pass: its SSTs, by id.
pub struct Before {
    ssts: HashMap<SsTableId, SsTableHandle>,
}

pub fn before(m: &VersionedManifest) -> Before {
    Before { ssts: m.all_sst_views().map(|v| (v.sst.id, v.sst.clone())).collect() }
}

/// Memory one kept key costs, besides its bytes (the `Bytes` handle and its allocation).
const KEY_OVERHEAD: usize = 48;

/// Hot keys of the SSTs the pass consumed: the keys of their data blocks found in the block cache under `scope` (sorted,
/// without duplicates). Keys, not key spans: a block of a sparse L0 SST spans a wide key range that, in the dense output,
/// covers many blocks nobody read. The blocks are read from the cache (no request). At most `max_bytes` of keys are kept
/// (copied out of the blocks, so a kept key does not keep its block alive); the third value says the limit cut the list.
async fn hot_keys(reader: &SstReader, cache: &Arc<dyn DbCache>, scope: u64, consumed: &[SsTableHandle], max_bytes: usize) -> Result<(Vec<Bytes>, u64, bool)> {
    let mut keys = Vec::new();
    let mut hot = 0u64;
    let mut bytes = 0usize;
    'ssts: for h in consumed {
        let file = reader.open_with_handle(h.clone())?;
        let index = file.index().await?;
        for i in 0..index.len() {
            let (offset, _) = index.get(i).expect("in range");
            let Some(key) = block_key(scope, &h.id, offset) else { continue };
            if cache.get_block(&key).await?.is_none() {
                continue;
            }
            let rows = file.read_block(i).await?;
            let add: usize = rows.iter().map(|r| r.key.len() + KEY_OVERHEAD).sum();
            if bytes + add > max_bytes {
                keys.sort();
                keys.dedup();
                return Ok((keys, hot, true));
            }
            hot += 1;
            bytes += add;
            keys.extend(rows.into_iter().map(|r| Bytes::copy_from_slice(&r.key)));
            if keys.len() > 1 << 16 && bytes > max_bytes / 2 {
                // duplicates (the same key in several consumed SSTs) count once
                keys.sort();
                keys.dedup();
                bytes = keys.iter().map(|k| k.len() + KEY_OVERHEAD).sum();
                if bytes > max_bytes {
                    break 'ssts;
                }
            }
        }
    }
    keys.sort();
    keys.dedup();
    Ok((keys, hot, false))
}

/// First keys of the data blocks of an SST, in order.
trait Firsts {
    fn n(&self) -> usize;
    fn first(&self, i: usize) -> &[u8];
}

impl Firsts for [Bytes] {
    fn n(&self) -> usize {
        self.len()
    }
    fn first(&self, i: usize) -> &[u8] {
        &self[i]
    }
}

impl Firsts for slatedb::SstIndex {
    fn n(&self) -> usize {
        self.len()
    }
    fn first(&self, i: usize) -> &[u8] {
        self.get(i).expect("in range").1
    }
}

/// The data blocks of an SST that hold `keys` (sorted), as runs of contiguous blocks, each a key range for `warm_sst`:
/// `[first key of block i, first key of block j + 1)`, the last run of the SST unbounded above (`warm_sst` stays inside
/// the SST). One run is one range GET. The index is read in place (no copy of it).
fn block_runs<F: Firsts + ?Sized>(index: &F, keys: &[Bytes]) -> Vec<(Bound<Bytes>, Bound<Bytes>)> {
    let n = index.n();
    let block_of = |k: &Bytes| {
        let (mut lo, mut hi) = (0usize, n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if index.first(mid) <= k.as_ref() {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo.saturating_sub(1)
    };
    let mut blocks: Vec<usize> = keys.iter().map(block_of).collect();
    blocks.dedup();
    let mut runs = Vec::new();
    let mut i = 0;
    while i < blocks.len() {
        let mut j = i;
        while j + 1 < blocks.len() && blocks[j + 1] == blocks[j] + 1 {
            j += 1;
        }
        let (first, last) = (blocks[i], blocks[j]);
        let end = if last + 1 < n { Bound::Excluded(Bytes::copy_from_slice(index.first(last + 1))) } else { Bound::Unbounded };
        runs.push((Bound::Included(Bytes::copy_from_slice(index.first(first))), end));
        i = j + 1;
    }
    runs
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct WarmRecord {
    pub consumed_ssts: usize,
    pub new_ssts: usize,
    pub hot_blocks: u64,
    pub hot_keys: usize,
    pub ranges: usize,
    /// `warm_sst` calls that finished before the time limit.
    pub calls_done: usize,
    pub calls: usize,
    /// The time limit stopped the warm-up (what was read stays in the cache).
    pub timed_out: bool,
    /// The key-bytes or the range limit left a part of the hot set out.
    pub truncated: bool,
    /// Time to open the reader, to find the hot keys, to plan the reads (new indexes), and in total.
    pub secs_open: f64,
    pub secs_hot: f64,
    pub secs_plan: f64,
    pub secs: f64,
}

/// Warm, under `scope`, the new SSTs of the latest manifest of the database at `path` (those not in `before`) for the hot
/// keys of the SSTs `before` had and the latest manifest no longer has, for at most `limit`. Returns the consumed SSTs (to
/// evict once the writer has switched) and what was done. Past `limit` the warm-up stops where it is and still returns
/// (`timed_out`); its reader is closed in every case before this returns.
pub async fn warm_compaction_output(store: &Arc<dyn ObjectStore>, path: &str, cache: &Arc<dyn DbCache>, scope: u64, before: &Before, limit: Duration) -> Result<(Vec<SsTableHandle>, WarmRecord)> {
    let t0 = Instant::now();
    let deadline = tokio::time::Instant::now() + limit;
    let mut o = DbReaderOptions::default();
    o.skip_wal_replay = true;
    o.manifest_poll_interval = Duration::from_secs(3600);
    let build = DbReader::builder(path.to_string(), Arc::clone(store)).with_reader_mode(DbReaderMode::FollowLatest).with_options(o).with_db_cache(Arc::clone(cache), scope).build();
    let reader = match tokio::time::timeout_at(deadline, build).await {
        Ok(r) => Arc::new(r?),
        Err(_) => {
            WARM_TIMEOUTS.fetch_add(1, Relaxed);
            anyhow::bail!("warm-up reader not open after {limit:?}")
        }
    };
    WARM_READERS_OPEN.fetch_add(1, Relaxed);
    let secs_open = t0.elapsed().as_secs_f64();
    let m = reader.manifest();
    let now: HashSet<SsTableId> = m.all_sst_views().map(|v| v.sst.id).collect();
    let l0: HashSet<SsTableId> = m.l0().iter().map(|v| v.sst.id).collect();
    let consumed: Vec<SsTableHandle> = before.ssts.iter().filter(|(id, _)| !now.contains(id)).map(|(_, h)| h.clone()).collect();
    // Compaction output: SSTs of the sorted runs (or segments) the writer did not have. New L0 SSTs are the writer's own
    // memtable flushes.
    let mut seen = HashSet::new();
    let new: Vec<SsTableHandle> = m.all_sst_views().filter(|v| !before.ssts.contains_key(&v.sst.id) && !l0.contains(&v.sst.id) && seen.insert(v.sst.id)).map(|v| v.sst.clone()).collect();
    let mut rec = WarmRecord { consumed_ssts: consumed.len(), new_ssts: new.len(), secs_open, ..Default::default() };
    let concurrency = COMPACTION_WARM_CONCURRENCY.load(Relaxed).max(1) as usize;
    let max_key_bytes = COMPACTION_WARM_MAX_KEY_BYTES.load(Relaxed) as usize;
    let max_ranges = COMPACTION_WARM_MAX_RANGES.load(Relaxed) as usize;
    let work = async {
        if consumed.is_empty() || new.is_empty() {
            return Ok(());
        }
        // Indexes and blocks through the writer's scope: what the writer has cached is read from the cache, and an index
        // fetched here is cached where `warm_sst` and the writer will look for it.
        let scoped: Arc<dyn DbCache> = Arc::new(ScopedCache { inner: Arc::clone(cache), scope });
        let ssts = SstReader::new(path.to_string(), Arc::clone(store), Some(scoped), None);
        let (keys, hot, cut) = hot_keys(&ssts, cache, scope, &consumed, max_key_bytes).await?;
        rec.hot_blocks = hot;
        rec.hot_keys = keys.len();
        rec.truncated |= cut;
        rec.secs_hot = t0.elapsed().as_secs_f64();
        if keys.is_empty() {
            return Ok(());
        }
        // The new SSTs that hold hot keys, their indexes read in parallel.
        let wanted: Vec<(SsTableHandle, usize, usize)> = new
            .iter()
            .filter_map(|h| {
                let lo = h.info.first_entry.as_ref().map_or(0, |f| keys.partition_point(|k| k < f));
                let hi = h.info.last_entry.as_ref().map_or(keys.len(), |l| keys.partition_point(|k| k <= l));
                (lo < hi).then(|| (h.clone(), lo, hi))
            })
            .collect();
        let mut indexes = futures::stream::iter(wanted.into_iter().map(|(h, lo, hi)| {
            let ssts = &ssts;
            async move {
                let index = ssts.open_with_handle(h.clone())?.index().await?;
                Ok::<_, anyhow::Error>((h.id, lo, hi, index))
            }
        }))
        .buffer_unordered(concurrency);
        // One call per group of `RANGES_PER_CALL` runs of one SST; the first call of an SST also warms its filters.
        let mut calls: Vec<(SsTableId, Vec<CacheTarget>)> = Vec::new();
        let mut ranges = 0usize;
        while let Some(r) = indexes.next().await {
            let (id, lo, hi, index) = r?;
            let mut runs = block_runs(&index, &keys[lo..hi]);
            if ranges + runs.len() > max_ranges {
                runs.truncate(max_ranges.saturating_sub(ranges));
                rec.truncated = true;
            }
            ranges += runs.len();
            let mut first = true;
            for group in runs.chunks(RANGES_PER_CALL.load(Relaxed).max(1) as usize) {
                let mut targets = Vec::with_capacity(group.len() + 1);
                if std::mem::take(&mut first) {
                    targets.push(CacheTarget::Filters);
                }
                targets.extend(group.iter().cloned().map(CacheTarget::Data));
                calls.push((id, targets));
            }
        }
        drop(indexes);
        drop(keys);
        rec.ranges = ranges;
        rec.calls = calls.len();
        rec.secs_plan = t0.elapsed().as_secs_f64();
        let mut jobs = futures::stream::iter(calls)
            .map(|(id, targets)| {
                let reader = Arc::clone(&reader);
                async move { reader.warm_sst(id, &targets).await }
            })
            .buffer_unordered(concurrency);
        while let Some(r) = jobs.next().await {
            r?;
            rec.calls_done += 1;
        }
        Ok::<_, anyhow::Error>(())
    };
    // The limit stops the work where it is: the calls in flight are dropped, the blocks already read stay in the cache.
    let outcome = tokio::time::timeout_at(deadline, work).await;
    // Close the reader in every case (its manifest poller is a background task of its own).
    match tokio::time::timeout(READER_CLOSE_LIMIT, reader.close()).await {
        Ok(_) => {
            WARM_READERS_OPEN.fetch_sub(1, Relaxed);
        }
        Err(_) => eprintln!("compaction warm-up of {path}: reader not closed after {READER_CLOSE_LIMIT:?}"),
    }
    drop(reader);
    match outcome {
        Ok(r) => r?,
        Err(_) => {
            rec.timed_out = true;
            WARM_TIMEOUTS.fetch_add(1, Relaxed);
        }
    }
    if rec.truncated {
        WARM_TRUNCATED.fetch_add(1, Relaxed);
    }
    rec.secs = t0.elapsed().as_secs_f64();
    WARM_RUNS.fetch_add(1, Relaxed);
    WARM_HOT_BLOCKS.fetch_add(rec.hot_blocks, Relaxed);
    WARM_RANGES.fetch_add(rec.ranges as u64, Relaxed);
    Ok((consumed, rec))
}

/// Drop from the writer's cache the entries of SSTs it no longer reads (call after it switched to the new manifest).
pub async fn evict_consumed(db: &Db, consumed: &[SsTableHandle]) {
    for h in consumed {
        let _ = db.evict_cached_sst(h, &CacheTarget::all()).await;
    }
}

#[cfg(test)]
mod tests {
    //! Measure (M27): the hot reads of an open database right after a compaction pass. Fake S3 (`object_store`'s in-memory
    //! store) behind the engine's request counter (`counting.rs`), the engine's shared block cache, the engine's writer and
    //! its standalone compaction pass. A hot read is "served by the cache" when it makes no GET on an SST.
    use super::*;
    use crate::counting::CountingStore;
    use crate::lifecycle::{compact_tenant_pass, install_cache, tenant_path, writer_builder, writer_closed, writer_opened, COMPACTOR_STOP_LIMIT};
    use object_store::memory::InMemory;
    use slatedb::config::{FlushOptions, FlushType};

    /// Threshold written before the measure (prompt M27): after a compaction pass at least this share of the hot reads is
    /// served by the block cache.
    const SERVED_MIN: f64 = 0.95;

    /// Block cache that counts the block lookups of one scope (other tests of the binary use the shared cache too).
    struct CountingCache {
        inner: Arc<dyn DbCache>,
        scope: AtomicU64,
        hits: AtomicU64,
        misses: AtomicU64,
    }

    #[async_trait::async_trait]
    impl DbCache for CountingCache {
        async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
            let r = self.inner.get_block(key).await?;
            if key.db_cache_id() == self.scope.load(Relaxed) {
                if r.is_some() { &self.hits } else { &self.misses }.fetch_add(1, Relaxed);
            }
            Ok(r)
        }
        async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
            self.inner.get_index(key).await
        }
        async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
            self.inner.get_filter(key).await
        }
        async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
            self.inner.get_stats(key).await
        }
        async fn insert(&self, key: CachedKey, value: CachedEntry) {
            self.inner.insert(key, value).await
        }
        async fn remove(&self, key: &CachedKey) {
            self.inner.remove(key).await
        }
        fn entry_count(&self) -> u64 {
            self.inner.entry_count()
        }
    }

    fn counting_cache() -> Arc<CountingCache> {
        static C: std::sync::OnceLock<Arc<CountingCache>> = std::sync::OnceLock::new();
        C.get_or_init(|| {
            use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
            let block: Arc<dyn DbCache> = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions { max_capacity: 256 << 20, ..Default::default() }));
            let meta: Arc<dyn DbCache> = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions { max_capacity: 64 << 20, ..Default::default() }));
            let c = Arc::new(CountingCache { inner: block, scope: AtomicU64::new(u64::MAX), hits: AtomicU64::new(0), misses: AtomicU64::new(0) });
            let split = slatedb::db_cache::SplitCache::new().with_block_cache(Some(c.clone() as Arc<dyn DbCache>)).with_meta_cache(Some(meta)).build();
            assert!(install_cache(Arc::new(split)), "the shared cache was installed before this test");
            c
        })
        .clone()
    }

    const KEYS: usize = 20_000;
    const VALUE: usize = 1000;
    const HOT_EVERY: usize = 50;

    fn key(i: usize) -> Vec<u8> {
        format!("k{i:08}").into_bytes()
    }
    fn value(i: usize, version: u8) -> Vec<u8> {
        (0..VALUE).map(|j| (i.wrapping_mul(31).wrapping_add(j * 7) as u8) ^ version).collect()
    }

    #[derive(Debug, Default)]
    struct Phase {
        reads: u64,
        served: u64,
        sst_gets: u64,
        block_misses: u64,
    }

    impl Phase {
        fn share(&self) -> f64 {
            self.served as f64 / self.reads as f64
        }
    }

    /// Read `keys` once each; a read is served by the cache when it made no GET on an SST. Checks every value.
    async fn read(db: &Db, counting: &CountingStore, cache: &CountingCache, keys: &[usize], expect: &(dyn Fn(usize) -> Vec<u8> + Sync)) -> Phase {
        let mut p = Phase::default();
        let m0 = cache.misses.load(Relaxed);
        for &i in keys {
            let g0 = counting.counters.get_cat[2].load(Relaxed);
            let v = db.get(&key(i)).await.unwrap().expect("key lost");
            assert_eq!(v.as_ref(), expect(i).as_slice(), "stale or wrong value for key {i}");
            let g = counting.counters.get_cat[2].load(Relaxed) - g0;
            p.reads += 1;
            p.sst_gets += g;
            if g == 0 {
                p.served += 1;
            }
        }
        p.block_misses = cache.misses.load(Relaxed) - m0;
        p
    }

    struct Run {
        before: Phase,
        after: Phase,
        cold_after: Phase,
        after2: Phase,
        pass_gets: u64,
    }

    /// Load a database as 4 L0 SSTs (the last one rewrites 100 hot keys: the pass must keep the newest version), warm the
    /// cache with the hot keys (2%, spread over the whole key space), run one compaction pass, read the hot keys again.
    async fn run(id: &str, warm: bool) -> Run {
        let cache = counting_cache();
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let counting = Arc::new(CountingStore::new(Arc::clone(&raw)));
        let store: Arc<dyn ObjectStore> = counting.clone();
        let db = writer_builder(id, &store).build().await.unwrap();
        let scope = crate::lifecycle::writer_scope(id).expect("writer scope");
        cache.scope.store(scope, Relaxed);
        writer_opened(id, &db);
        let per = KEYS / 3;
        for part in 0..3 {
            for i in part * per..((part + 1) * per).min(KEYS) {
                db.put(&key(i), &value(i, 0)).await.unwrap();
            }
            db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await.unwrap();
        }
        for i in (per * 3)..KEYS {
            db.put(&key(i), &value(i, 0)).await.unwrap();
        }
        let hot: Vec<usize> = (0..KEYS).step_by(HOT_EVERY).collect();
        let rewritten: HashSet<usize> = hot.iter().copied().step_by(4).collect();
        for &i in &rewritten {
            db.put(&key(i), &value(i, 1)).await.unwrap();
        }
        db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await.unwrap();
        assert_eq!(db.manifest().l0().len(), 4);
        let mut version: HashMap<usize, u8> = rewritten.iter().map(|&i| (i, 1)).collect();
        let expect = |v: &HashMap<usize, u8>| {
            let v = v.clone();
            move |i: usize| value(i, v.get(&i).copied().unwrap_or(0))
        };
        // Warm the cache: the hot keys are read once.
        let _ = read(&db, &counting, &cache, &hot, &expect(&version)).await;
        let before = read(&db, &counting, &cache, &hot, &expect(&version)).await;
        let rec = compact_tenant_pass(&raw, &raw, id, Duration::from_secs(30), COMPACTOR_STOP_LIMIT, warm).await.unwrap().expect("no pass");
        assert!(db.manifest().l0().is_empty(), "the writer did not switch to the compacted manifest");
        let after = read(&db, &counting, &cache, &hot, &expect(&version)).await;
        let cold: Vec<usize> = (HOT_EVERY / 2..KEYS).step_by(HOT_EVERY).collect();
        let cold_after = read(&db, &counting, &cache, &cold, &expect(&version)).await;
        // Second pass over a database that already has a sorted run: 4 more L0 SSTs rewrite one key in 3 (a third of the
        // hot), the hot keys are read, the pass runs again.
        let mut pass_gets = rec.requests.get + rec.requests.get_range + rec.requests.head;
        let rewrite: Vec<usize> = (0..KEYS).step_by(3).collect();
        for chunk in rewrite.chunks(rewrite.len().div_ceil(4)) {
            for &i in chunk {
                db.put(&key(i), &value(i, 2)).await.unwrap();
                version.insert(i, 2);
            }
            db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await.unwrap();
        }
        let _ = read(&db, &counting, &cache, &hot, &expect(&version)).await;
        let rec = compact_tenant_pass(&raw, &raw, id, Duration::from_secs(30), COMPACTOR_STOP_LIMIT, warm).await.unwrap().expect("no second pass");
        pass_gets += rec.requests.get + rec.requests.get_range + rec.requests.head;
        let after2 = read(&db, &counting, &cache, &hot, &expect(&version)).await;
        // Every key, newest version (no stale read, nothing lost).
        let all: Vec<usize> = (0..KEYS).collect();
        let _ = read(&db, &counting, &cache, &all, &expect(&version)).await;
        writer_closed(id);
        db.close().await.unwrap();
        let _ = tenant_path(id);
        Run { before, after, cold_after, after2, pass_gets }
    }

    /// C8 review (C9: kept as a test): writes and reads while the pass and the warm-up run. Checks that no read returns a
    /// value older than one acknowledged before the read started, the hot share after the pass, the writer's put latency,
    /// and the cache size before and after.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn writes_and_reads_during_the_switch() {
        for warm in [false, true] {
            let id = if warm { "test-c8-conc-on" } else { "test-c8-conc-off" };
            let cache = counting_cache();
            let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let counting = Arc::new(CountingStore::new(Arc::clone(&raw)));
            let store: Arc<dyn ObjectStore> = counting.clone();
            let db = writer_builder(id, &store).build().await.unwrap();
            let scope = crate::lifecycle::writer_scope(id).expect("writer scope");
            cache.scope.store(scope, Relaxed);
            writer_opened(id, &db);
            let per = KEYS / 4;
            for part in 0..4 {
                for i in part * per..((part + 1) * per).min(KEYS) {
                    db.put(&key(i), &value(i, 0)).await.unwrap();
                }
                db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await.unwrap();
            }
            let hot: Vec<usize> = (0..KEYS).step_by(HOT_EVERY).collect();
            let _ = read(&db, &counting, &cache, &hot, &|i| value(i, 0)).await;
            let entries0 = cache.inner.entry_count();
            // writer: keys 25, 75, 125, ... (not hot), value = 8-byte sequence + padding; acked[i] = last acknowledged seq
            let wkeys: Vec<usize> = (HOT_EVERY / 2..KEYS).step_by(HOT_EVERY).collect();
            let acked: Arc<Vec<AtomicU64>> = Arc::new((0..KEYS).map(|_| AtomicU64::new(0)).collect());
            let stop = Arc::new(AtomicBool::new(false));
            let maxlat = Arc::new(AtomicU64::new(0));
            let writes = Arc::new(AtomicU64::new(0));
            let w = {
                let (db, acked, stop, maxlat, writes, wkeys) = (db.clone(), Arc::clone(&acked), Arc::clone(&stop), Arc::clone(&maxlat), Arc::clone(&writes), wkeys.clone());
                tokio::spawn(async move {
                    let mut seq = 1u64;
                    let mut round = 0;
                    while !stop.load(Relaxed) {
                        round += 1;
                        for &i in &wkeys {
                            let mut v = seq.to_be_bytes().to_vec();
                            v.resize(VALUE, 7);
                            let t = Instant::now();
                            db.put(&key(i), &v).await.unwrap();
                            maxlat.fetch_max(t.elapsed().as_micros() as u64, Relaxed);
                            acked[i].store(seq, Relaxed);
                            seq += 1;
                            writes.fetch_add(1, Relaxed);
                        }
                        // two new L0 SSTs while the pass runs (more would fill L0 with no L0 watch in this test)
                        if round == 2 || round == 4 {
                            db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await.unwrap();
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
            };
            let stale = Arc::new(AtomicU64::new(0));
            let reads = Arc::new(AtomicU64::new(0));
            let r = {
                let (db, acked, stop, stale, reads, wkeys) = (db.clone(), Arc::clone(&acked), Arc::clone(&stop), Arc::clone(&stale), Arc::clone(&reads), wkeys.clone());
                tokio::spawn(async move {
                    let mut n = 0usize;
                    while !stop.load(Relaxed) {
                        let i = wkeys[n % wkeys.len()];
                        let h = (n * 7919) % KEYS / HOT_EVERY * HOT_EVERY;
                        n += 1;
                        let floor = acked[i].load(Relaxed);
                        let v = db.get(&key(i)).await.unwrap().expect("key lost");
                        let got = if v.as_ref() == value(i, 0).as_slice() { 0 } else { u64::from_be_bytes(v[..8].try_into().unwrap()) };
                        if got < floor {
                            stale.fetch_add(1, Relaxed);
                        }
                        let hv = db.get(&key(h)).await.unwrap().expect("hot key lost");
                        if hv.as_ref() != value(h, 0).as_slice() {
                            stale.fetch_add(1, Relaxed);
                        }
                        reads.fetch_add(2, Relaxed);
                    }
                })
            };
            tokio::time::sleep(Duration::from_millis(300)).await;
            let t = Instant::now();
            let rec = compact_tenant_pass(&raw, &raw, id, Duration::from_secs(60), COMPACTOR_STOP_LIMIT, warm).await;
            let pass_secs = t.elapsed().as_secs_f64();
            tokio::time::sleep(Duration::from_millis(300)).await;
            stop.store(true, Relaxed);
            w.await.unwrap();
            r.await.unwrap();
            let entries1 = cache.inner.entry_count();
            let after = read(&db, &counting, &cache, &hot, &|i| value(i, 0)).await;
            println!(
                "C8 concurrent warm={warm}: pass ok={} in {pass_secs:.2} s; writes {} (max put {} ms), reads {} stale {}; hot served after {}/{} ({:.1}%); cache entries before {entries0} after {entries1}; L0 after {}",
                rec.is_ok(),
                writes.load(Relaxed),
                maxlat.load(Relaxed) / 1000,
                reads.load(Relaxed),
                stale.load(Relaxed),
                after.served,
                after.reads,
                100.0 * after.share(),
                db.manifest().l0().len()
            );
            assert_eq!(stale.load(Relaxed), 0, "a read returned a value older than one acknowledged before it");
            writer_closed(id);
            db.close().await.unwrap();
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq)]
    enum BigMode {
        /// No warm-up: the pass the warm-up is compared with.
        Off,
        /// The warm-up with its defaults.
        On,
        /// The warm-up with a time limit far too short for the hot set: it must stop, close its reader, let the writer
        /// switch, and keep what it warmed.
        ShortLimit,
        /// The warm-up with a key-bytes limit far too small for the hot set: it must warm a part, not none.
        KeyCap,
    }

    struct BigRun {
        share: f64,
        served: u64,
        reads: u64,
        secs: f64,
        timeouts: u64,
        truncated: u64,
        l0_after: usize,
    }

    /// A database of `n` keys of 1 KB (4 L0 SSTs), 2% hot keys spread over the key space, every GET of the pass `delay_ms`
    /// slower (a crude S3); one compaction pass; then the hot keys are read again.
    async fn big_run(n: usize, delay_ms: u64, mode: BigMode) -> BigRun {
        let id = format!("test-big-{mode:?}").to_lowercase();
        let id = id.as_str();
        let cache = counting_cache();
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let counting = Arc::new(CountingStore::new(Arc::clone(&raw)));
        let store: Arc<dyn ObjectStore> = counting.clone();
        let db = writer_builder(id, &store).build().await.unwrap();
        let scope = crate::lifecycle::writer_scope(id).expect("writer scope");
        cache.scope.store(scope, Relaxed);
        writer_opened(id, &db);
        let per = n / 4;
        for part in 0..4 {
            for i in part * per..((part + 1) * per).min(n) {
                db.put(&key(i), &value(i, 0)).await.unwrap();
            }
            db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await.unwrap();
        }
        let hot: Vec<usize> = (0..n).step_by(HOT_EVERY).collect();
        let _ = read(&db, &counting, &cache, &hot, &|i| value(i, 0)).await;
        let before = read(&db, &counting, &cache, &hot, &|i| value(i, 0)).await;
        assert!(before.share() >= SERVED_MIN, "the cache was not warm before the pass");
        let (ms0, keys0) = (COMPACTION_WARM_MS.load(Relaxed), COMPACTION_WARM_MAX_KEY_BYTES.load(Relaxed));
        match mode {
            BigMode::ShortLimit => COMPACTION_WARM_MS.store(500, Relaxed),
            BigMode::KeyCap => COMPACTION_WARM_MAX_KEY_BYTES.store(64 << 10, Relaxed),
            _ => {}
        }
        let (timeouts0, truncated0) = (WARM_TIMEOUTS.load(Relaxed), WARM_TRUNCATED.load(Relaxed));
        crate::counting::TEST_GET_DELAY_MS.store(delay_ms, Relaxed);
        let t = Instant::now();
        let s3: Arc<dyn ObjectStore> = counting.clone();
        let rec = compact_tenant_pass(&s3, &raw, id, Duration::from_secs(600), COMPACTOR_STOP_LIMIT, mode != BigMode::Off).await.unwrap().expect("no pass");
        let secs = t.elapsed().as_secs_f64();
        crate::counting::TEST_GET_DELAY_MS.store(0, Relaxed);
        COMPACTION_WARM_MS.store(ms0, Relaxed);
        COMPACTION_WARM_MAX_KEY_BYTES.store(keys0, Relaxed);
        let l0_after = db.manifest().l0().len();
        let after = read(&db, &counting, &cache, &hot, &|i| value(i, 0)).await;
        let r = BigRun {
            share: after.share(),
            served: after.served,
            reads: after.reads,
            secs,
            timeouts: WARM_TIMEOUTS.load(Relaxed) - timeouts0,
            truncated: WARM_TRUNCATED.load(Relaxed) - truncated0,
            l0_after,
        };
        println!(
            "big n={n} GET +{delay_ms} ms {mode:?}: hot served after the pass {}/{} ({:.1}%), SST GETs after {}; pass {secs:.2} s, pass GETs {}; warm-up timeouts {}, truncated {}, readers open {}; L0 after {l0_after}",
            r.served, r.reads, 100.0 * r.share, after.sst_gets, rec.requests.get + rec.requests.get_range + rec.requests.head, r.timeouts, r.truncated, WARM_READERS_OPEN.load(Relaxed)
        );
        writer_closed(id);
        db.close().await.unwrap();
        r
    }

    /// The decisive test of C8 (`review_c8_big`, C9): 200 MB, 2% hot keys spread out, every GET 20 ms slower. At least 95%
    /// of the hot reads served by the cache after the pass, and the pass at most 5 s longer than without the warm-up. Then
    /// the two limits: a time limit too short and a key-bytes limit too small each warm a part, the pass goes on, the writer
    /// switches, no warm-up reader stays open. Ignored by default (200 MB in memory per run, global knobs): `cargo test
    /// --release --lib warm::tests::big_db_slow_store -- --ignored --nocapture` (`C8_KEYS`, `C8_GET_MS` change the size and
    /// the delay).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn big_db_slow_store() {
        let n: usize = std::env::var("C8_KEYS").ok().and_then(|v| v.parse().ok()).unwrap_or(200_000);
        let delay: u64 = std::env::var("C8_GET_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(20);
        // 4 L0 SSTs of n/4 keys each: no automatic memtable flush in between (no L0 watch here to make room)
        crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(1 << 30, Relaxed);
        crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(64, Relaxed);
        crate::lifecycle::MEMTABLE_MAX_BYTES.store(1 << 30, Relaxed);
        if let Some(c) = std::env::var("C9_CONC").ok().and_then(|v| v.parse().ok()) {
            COMPACTION_WARM_CONCURRENCY.store(c, Relaxed);
        }
        if let Some(c) = std::env::var("C9_RPC").ok().and_then(|v| v.parse().ok()) {
            RANGES_PER_CALL.store(c, Relaxed);
        }
        let off = big_run(n, delay, BigMode::Off).await;
        let on = big_run(n, delay, BigMode::On).await;
        assert!(off.share < 0.5, "without the warm-up the pass no longer cools the cache: the measure does not see the problem");
        assert!(on.share >= SERVED_MIN, "only {:.1}% of the hot reads served by the cache after the pass", 100.0 * on.share);
        assert!(on.secs <= off.secs + 5.0, "the warm-up made the pass {:.2} s longer", on.secs - off.secs);
        assert_eq!(on.l0_after, 0, "the writer did not switch to the compacted manifest");
        let short = big_run(n, delay, BigMode::ShortLimit).await;
        assert_eq!(short.timeouts, 1, "the short limit did not stop the warm-up");
        assert!(short.secs <= off.secs + 0.5 + 2.0, "a warm-up with a 0.5 s limit made the pass {:.2} s longer", short.secs - off.secs);
        assert!(short.served > 0, "a warm-up stopped by its limit kept nothing");
        assert_eq!(short.l0_after, 0, "after a stopped warm-up the writer did not switch");
        let cap = big_run(n, delay, BigMode::KeyCap).await;
        assert_eq!(cap.truncated, 1, "the key-bytes limit did not cut the hot set");
        assert!(cap.served > 0 && cap.share < SERVED_MIN, "the key-bytes limit warmed {}/{} hot reads: expected a part", cap.served, cap.reads);
        assert_eq!(cap.l0_after, 0);
        assert_eq!(WARM_READERS_OPEN.load(Relaxed), 0, "a warm-up reader is still open");
    }

    #[test]
    fn cache_key_round_trip() {
        let id = SsTableId::from(ulid::Ulid::from_parts(1_700_000_000_000, 42));
        let k = block_key(7, &id, 4096).expect("key");
        assert_eq!(k.db_cache_id(), 7);
        let k2 = rescope(&k, 9).expect("rescope");
        assert_eq!(k2.db_cache_id(), 9);
        assert_eq!(rescope(&k2, 7).unwrap(), k);
    }

    #[test]
    fn runs_of_blocks() {
        let b = |s: &str| Bytes::copy_from_slice(s.as_bytes());
        let index = vec![b(""), b("c"), b("f"), b("i"), b("l")];
        // blocks 0, 1 (contiguous), 3, 4 (contiguous, last)
        let runs = block_runs(index.as_slice(), &[b("a"), b("b"), b("d"), b("j"), b("m"), b("z")]);
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0], (Bound::Included(b("")), Bound::Excluded(b("f"))));
        assert_eq!(runs[1], (Bound::Included(b("i")), Bound::Unbounded));
        assert_eq!(block_runs(index.as_slice(), &[b("f")]), vec![(Bound::Included(b("f")), Bound::Excluded(b("i")))]);
    }

    /// The automated proof of M27: fails if, after a compaction pass, less than `SERVED_MIN` of the hot reads are served by
    /// the block cache. The run without the warm-up is the measure of the problem (and checks the test sees it).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn hot_reads_stay_in_cache_after_compaction() {
        let off = run("test-m27-warm-off", false).await;
        let on = run("test-m27-warm-on", true).await;
        for (name, r) in [("without warm-up", &off), ("with warm-up", &on)] {
            println!(
                "M27 {name}: hot reads served by the cache before the pass {}/{} ({:.1}%), after {}/{} ({:.1}%), SST GETs after {}, block misses after {}; cold reads after served {}/{}; after a second pass {}/{} ({:.1}%); GETs of the two passes {}",
                r.before.served,
                r.before.reads,
                100.0 * r.before.share(),
                r.after.served,
                r.after.reads,
                100.0 * r.after.share(),
                r.after.sst_gets,
                r.after.block_misses,
                r.cold_after.served,
                r.cold_after.reads,
                r.after2.served,
                r.after2.reads,
                100.0 * r.after2.share(),
                r.pass_gets
            );
        }
        assert!(off.before.share() >= SERVED_MIN && on.before.share() >= SERVED_MIN, "the cache was not warm before the pass");
        assert!(off.after.share() < 0.5, "without the warm-up the pass no longer cools the cache: the measure does not see the problem");
        assert!(on.after.share() >= SERVED_MIN, "after a compaction pass only {:.1}% of the hot reads were served by the cache", 100.0 * on.after.share());
        assert!(on.after2.share() >= SERVED_MIN, "after a second pass only {:.1}% of the hot reads were served by the cache", 100.0 * on.after2.share());
        // Only what was hot is warmed: most cold reads still go to the store.
        assert!(on.cold_after.share() < 0.5, "the warm-up loaded cold blocks too ({} of {} cold reads served)", on.cold_after.served, on.cold_after.reads);
    }
}

#[cfg(test)]
mod probe_cost {
    use super::*;

    /// Cost of one probe of the hot-block scan (build the key, look it up in a foyer cache). `cargo test --release --lib
    /// probe_cost -- --ignored --nocapture`.
    #[tokio::test]
    #[ignore]
    async fn probe_cost() {
        use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
        let cache: Arc<dyn DbCache> = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions { max_capacity: 64 << 20, ..Default::default() }));
        let id = SsTableId::from(ulid::Ulid::from_parts(1_700_000_000_000, 42));
        let n = 1_000_000u64;
        let t0 = Instant::now();
        let mut found = 0;
        for i in 0..n {
            let k = block_key(3, &id, i * 4096).unwrap();
            if cache.get_block(&k).await.unwrap().is_some() {
                found += 1;
            }
        }
        let ns = t0.elapsed().as_nanos() as f64 / n as f64;
        println!("M27 probe: {ns:.0} ns per block ({found} found)");
    }
}
