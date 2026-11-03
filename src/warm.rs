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
/// Upper bound of one warm-up (seconds). Past it the pass goes on; the reads not warmed miss as before.
pub static COMPACTION_WARM_SECS: AtomicU64 = AtomicU64::new(20);
/// Totals: warm-ups run, hot blocks found in the consumed SSTs, hot ranges warmed, warm-ups failed or timed out.
pub static WARM_RUNS: AtomicU64 = AtomicU64::new(0);
pub static WARM_HOT_BLOCKS: AtomicU64 = AtomicU64::new(0);
pub static WARM_RANGES: AtomicU64 = AtomicU64::new(0);
pub static WARM_FAILS: AtomicU64 = AtomicU64::new(0);

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

/// Hot keys of the SSTs the pass consumed: the keys of their data blocks found in the block cache under `scope` (sorted,
/// without duplicates). Keys, not key spans: a block of a sparse L0 SST spans a wide key range that, in the dense output,
/// covers many blocks nobody read. The blocks are read from the cache (no request).
async fn hot_keys(reader: &SstReader, cache: &Arc<dyn DbCache>, scope: u64, consumed: &[SsTableHandle]) -> Result<(Vec<Bytes>, u64)> {
    let mut keys = Vec::new();
    let mut hot = 0u64;
    for h in consumed {
        let file = reader.open_with_handle(h.clone())?;
        let index = file.index().await?;
        for i in 0..index.len() {
            let (offset, _) = index.get(i).expect("in range");
            let Some(key) = block_key(scope, &h.id, offset) else { continue };
            if cache.get_block(&key).await?.is_none() {
                continue;
            }
            hot += 1;
            keys.extend(file.read_block(i).await?.into_iter().map(|r| r.key));
        }
    }
    keys.sort();
    keys.dedup();
    Ok((keys, hot))
}

/// The data blocks of an SST that hold `keys` (sorted), as runs of contiguous blocks, each a key range for `warm_sst`:
/// `[first key of block i, first key of block j + 1)`, the last run of the SST unbounded above (`warm_sst` stays inside
/// the SST). One run is one range GET.
fn block_runs(index: &[Bytes], keys: &[Bytes]) -> Vec<(Bound<Bytes>, Bound<Bytes>)> {
    let mut blocks: Vec<usize> = keys.iter().map(|k| index.partition_point(|f| f <= k).saturating_sub(1)).collect();
    blocks.dedup();
    let mut runs = Vec::new();
    let mut i = 0;
    while i < blocks.len() {
        let mut j = i;
        while j + 1 < blocks.len() && blocks[j + 1] == blocks[j] + 1 {
            j += 1;
        }
        let (first, last) = (blocks[i], blocks[j]);
        let end = match index.get(last + 1) {
            Some(next) => Bound::Excluded(next.clone()),
            None => Bound::Unbounded,
        };
        runs.push((Bound::Included(index[first].clone()), end));
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
    pub secs: f64,
}

/// Warm, under `scope`, the new SSTs of the latest manifest of the database at `path` (those not in `before`) for the hot
/// keys of the SSTs `before` had and the latest manifest no longer has. Returns the consumed SSTs (to evict once the
/// writer has switched) and what was done.
pub async fn warm_compaction_output(store: &Arc<dyn ObjectStore>, path: &str, cache: &Arc<dyn DbCache>, scope: u64, before: &Before) -> Result<(Vec<SsTableHandle>, WarmRecord)> {
    let t0 = Instant::now();
    let mut o = DbReaderOptions::default();
    o.skip_wal_replay = true;
    o.manifest_poll_interval = Duration::from_secs(3600);
    let reader = Arc::new(
        DbReader::builder(path.to_string(), Arc::clone(store))
            .with_reader_mode(DbReaderMode::FollowLatest)
            .with_options(o)
            .with_db_cache(Arc::clone(cache), scope)
            .build()
            .await?,
    );
    let result = async {
        let m = reader.manifest();
        let now: HashSet<SsTableId> = m.all_sst_views().map(|v| v.sst.id).collect();
        let l0: HashSet<SsTableId> = m.l0().iter().map(|v| v.sst.id).collect();
        let consumed: Vec<SsTableHandle> = before.ssts.iter().filter(|(id, _)| !now.contains(id)).map(|(_, h)| h.clone()).collect();
        // Compaction output: SSTs of the sorted runs (or segments) the writer did not have. New L0 SSTs are the writer's own
        // memtable flushes.
        let mut seen = HashSet::new();
        let new: Vec<SsTableHandle> =
            m.all_sst_views().filter(|v| !before.ssts.contains_key(&v.sst.id) && !l0.contains(&v.sst.id) && seen.insert(v.sst.id)).map(|v| v.sst.clone()).collect();
        let mut rec = WarmRecord { consumed_ssts: consumed.len(), new_ssts: new.len(), ..Default::default() };
        if consumed.is_empty() || new.is_empty() {
            return Ok((consumed, rec));
        }
        // Indexes and blocks through the writer's scope: what the writer has cached is read from the cache, and an index
        // fetched here is cached where `warm_sst` and the writer will look for it.
        let scoped: Arc<dyn DbCache> = Arc::new(ScopedCache { inner: Arc::clone(cache), scope });
        let ssts = SstReader::new(path.to_string(), Arc::clone(store), Some(scoped), None);
        let (keys, hot) = hot_keys(&ssts, cache, scope, &consumed).await?;
        rec.hot_blocks = hot;
        rec.hot_keys = keys.len();
        if keys.is_empty() {
            return Ok((consumed, rec));
        }
        let mut plans: Vec<(SsTableId, Vec<CacheTarget>)> = Vec::new();
        for h in &new {
            let lo = h.info.first_entry.as_ref().map_or(0, |f| keys.partition_point(|k| k < f));
            let hi = h.info.last_entry.as_ref().map_or(keys.len(), |l| keys.partition_point(|k| k <= l));
            if lo >= hi {
                continue;
            }
            let index = ssts.open_with_handle(h.clone())?.index().await?;
            let firsts: Vec<Bytes> = index.iter().map(|(_, f)| Bytes::copy_from_slice(f)).collect();
            let runs = block_runs(&firsts, &keys[lo..hi]);
            rec.ranges += runs.len();
            let mut targets = vec![CacheTarget::Index, CacheTarget::Filters];
            targets.extend(runs.into_iter().map(CacheTarget::Data));
            plans.push((h.id, targets));
        }
        let mut jobs = futures::stream::iter(plans)
            .map(|(id, targets)| {
                let reader = Arc::clone(&reader);
                async move { reader.warm_sst(id, &targets).await }
            })
            .buffer_unordered(8);
        while let Some(r) = jobs.next().await {
            r?;
        }
        Ok::<_, anyhow::Error>((consumed, rec))
    }
    .await;
    let _ = reader.close().await;
    let (consumed, mut rec) = result?;
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
        let runs = block_runs(&index, &[b("a"), b("b"), b("d"), b("j"), b("m"), b("z")]);
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0], (Bound::Included(b("")), Bound::Excluded(b("f"))));
        assert_eq!(runs[1], (Bound::Included(b("i")), Bound::Unbounded));
        assert_eq!(block_runs(&index, &[b("f")]), vec![(Bound::Included(b("f")), Bound::Excluded(b("i")))]);
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
