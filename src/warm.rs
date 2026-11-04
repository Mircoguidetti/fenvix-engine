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
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult};
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
/// about 2 GETs at a time). Per warm-up; the rate of all warm-ups together is `COMPACTION_WARM_GETS_PER_SEC`.
pub static COMPACTION_WARM_CONCURRENCY: AtomicU64 = AtomicU64::new(128);
/// Most bytes of hot keys one warm-up keeps in memory (`--compaction-warm-max-key-mb`). Past it the remaining hot blocks
/// are not warmed (counted in `WARM_TRUNCATED`): a part of the hot set is warmed instead of none.
pub static COMPACTION_WARM_MAX_KEY_BYTES: AtomicU64 = AtomicU64::new(32 << 20);
/// Most ranges (one range GET each) one warm-up reads. Past it the rest are not warmed (`WARM_TRUNCATED`).
pub static COMPACTION_WARM_MAX_RANGES: AtomicU64 = AtomicU64::new(200_000);
/// Data ranges per `warm_sst` call: a call reads its ranges one after the other, calls run in parallel. Tests change it.
pub static RANGES_PER_CALL: AtomicU64 = AtomicU64::new(2);
/// Most warm-up GETs per second for the whole process, all warm-ups together (`--compaction-warm-gets-per-sec`, 0 = no
/// limit). C9 review (E2): 128 calls per warm-up at 20 ms per GET are 6,400 GETs/s for one database and 12,265 for the
/// two passes the maintenance runs at once, over S3's 5,500 GET/s of one partition; a new bucket can hold every
/// database's prefix in one partition. One shared token bucket keeps all warm-ups under this rate.
pub static COMPACTION_WARM_GETS_PER_SEC: AtomicU64 = AtomicU64::new(3000);
/// The warm-up stops when the writer's L0 is one SST from full (C9 review, E1). Only tests turn it off, to measure the
/// version without it.
pub static COMPACTION_WARM_L0_WATCH: AtomicBool = AtomicBool::new(true);
/// Writer L0 polling period during a warm-up (C9 review, E1).
const L0_POLL: Duration = Duration::from_millis(25);
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
/// Warm-ups stopped (or not started) because the writer's L0 reached `l0_max_ssts - 1`, and warm-up GETs that waited
/// for the shared rate limit.
pub static WARM_L0_STOPS: AtomicU64 = AtomicU64::new(0);
pub static WARM_THROTTLED: AtomicU64 = AtomicU64::new(0);

/// The open writer whose L0 bounds a warm-up (C9 review, E1). Until the writer switches to the new manifest it still
/// counts the L0 SSTs the pass consumed, and once its L0 reaches `l0_max_ssts` its memtable flushes stop (and then its
/// writes, past `max_unflushed_bytes`). The warm-up does not start when the writer's L0 is at `stop_at` already, and
/// stops where it is as soon as it gets there, so the writer switches before its flushes block.
pub struct L0Watch {
    pub db: Db,
    /// `l0_max_ssts - 1`.
    pub stop_at: usize,
}

impl L0Watch {
    fn full(&self) -> bool {
        self.db.manifest().l0().len() >= self.stop_at
    }
    /// Resolves once the writer's L0 is at `stop_at` (never without a watch).
    async fn filled(w: Option<&L0Watch>) {
        let Some(w) = w else { return std::future::pending().await };
        while !w.full() {
            tokio::time::sleep(L0_POLL).await;
        }
    }
}

/// The shared token bucket of the warm-up GETs (GCRA: each GET takes the next free slot of `1 / rate`; at most
/// `rate / 20` can go at once after an idle time). A slot taken by a GET whose warm-up was stopped is lost: at most the
/// calls in flight, a few tens of milliseconds at the default rate.
static BUCKET: std::sync::Mutex<Option<tokio::time::Instant>> = std::sync::Mutex::new(None);

async fn take_token() {
    let rate = COMPACTION_WARM_GETS_PER_SEC.load(Relaxed);
    if rate == 0 {
        return;
    }
    let step = Duration::from_secs_f64(1.0 / rate as f64);
    let burst = step * (rate as u32 / 20).max(1);
    let now = tokio::time::Instant::now();
    let at = {
        let mut next = BUCKET.lock().unwrap();
        let at = next.map_or(now, |n| n.max(now.checked_sub(burst).unwrap_or(now)));
        *next = Some(at + step);
        at
    };
    if at > now {
        WARM_THROTTLED.fetch_add(1, Relaxed);
        tokio::time::sleep_until(at).await;
    }
}

/// The object store of the warm-up: every GET (and HEAD) waits for the shared token bucket first.
#[derive(Debug)]
struct Throttled(Arc<dyn ObjectStore>);

impl std::fmt::Display for Throttled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Throttled({})", self.0)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Throttled {
    async fn put_opts(&self, l: &Path, p: PutPayload, o: PutOptions) -> object_store::Result<PutResult> {
        self.0.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(&self, l: &Path, o: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, l: &Path, o: GetOptions) -> object_store::Result<GetResult> {
        take_token().await;
        self.0.get_opts(l, o).await
    }
    fn delete_stream(&self, s: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        self.0.delete_stream(s)
    }
    fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.0.list(p)
    }
    fn list_with_offset(&self, p: Option<&Path>, o: &Path) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.0.list_with_offset(p, o)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
        self.0.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> object_store::Result<()> {
        self.0.copy_opts(f, t, o).await
    }
}

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

impl Before {
    /// The SSTs the writer had and `m` (its manifest after the switch) no longer has: their blocks can be evicted.
    pub fn consumed(&self, m: &VersionedManifest) -> Vec<SsTableHandle> {
        let now: HashSet<SsTableId> = m.all_sst_views().map(|v| v.sst.id).collect();
        self.ssts.iter().filter(|(id, _)| !now.contains(id)).map(|(_, h)| h.clone()).collect()
    }
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
    for h in consumed {
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
                    // the rest of the hot blocks is left out: a part of the hot set (C9 review: this exit did not say so)
                    return Ok((keys, hot, true));
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
    /// The writer's L0 reached `l0_max_ssts - 1`: the warm-up stopped (or did not start) so the writer can switch.
    pub l0_stopped: bool,
    /// Time to open the reader, to find the hot keys, to plan the reads (new indexes), and in total.
    pub secs_open: f64,
    pub secs_hot: f64,
    pub secs_plan: f64,
    /// Time to close the reader.
    pub secs_close: f64,
    pub secs: f64,
}

/// Warm, under `scope`, the new SSTs of the latest manifest of the database at `path` (those not in `before`) for the hot
/// keys of the SSTs `before` had and the latest manifest no longer has, for at most `limit`, and only while the writer of
/// `watch` has room in L0. Returns what was done. Past `limit`, or once the writer's L0 is at `watch.stop_at`, the warm-up
/// stops where it is and still returns (`timed_out`, `l0_stopped`); it does not start when that L0 is there already. Its
/// reader is closed in every case before this returns. Every GET of the warm-up waits for the process-wide token bucket
/// (`COMPACTION_WARM_GETS_PER_SEC`).
pub async fn warm_compaction_output(store: &Arc<dyn ObjectStore>, path: &str, cache: &Arc<dyn DbCache>, scope: u64, before: &Before, limit: Duration, watch: Option<&L0Watch>) -> Result<WarmRecord> {
    let t0 = Instant::now();
    let deadline = tokio::time::Instant::now() + limit;
    if watch.is_some_and(L0Watch::full) {
        // The writer's flushes are about to block on its L0: it switches now, nothing is warmed.
        WARM_L0_STOPS.fetch_add(1, Relaxed);
        WARM_RUNS.fetch_add(1, Relaxed);
        return Ok(WarmRecord { l0_stopped: true, ..Default::default() });
    }
    let store: Arc<dyn ObjectStore> = Arc::new(Throttled(Arc::clone(store)));
    let store = &store;
    let mut o = DbReaderOptions::default();
    o.skip_wal_replay = true;
    o.manifest_poll_interval = Duration::from_secs(3600);
    let build = DbReader::builder(path.to_string(), Arc::clone(store)).with_reader_mode(DbReaderMode::FollowLatest).with_options(o).with_db_cache(Arc::clone(cache), scope).build();
    let opened = tokio::time::timeout_at(deadline, async {
        tokio::select! {
            r = build => Some(r),
            _ = L0Watch::filled(watch) => None,
        }
    })
    .await;
    let reader = match opened {
        Ok(Some(r)) => Arc::new(r?),
        Ok(None) => {
            WARM_L0_STOPS.fetch_add(1, Relaxed);
            WARM_RUNS.fetch_add(1, Relaxed);
            return Ok(WarmRecord { l0_stopped: true, secs: t0.elapsed().as_secs_f64(), ..Default::default() });
        }
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
    // The limit, or the writer's L0, stops the work where it is: the calls in flight are dropped, the blocks already read
    // stay in the cache.
    let outcome = tokio::time::timeout_at(deadline, async {
        tokio::select! {
            r = work => Some(r),
            _ = L0Watch::filled(watch) => None,
        }
    })
    .await;
    let t_close = Instant::now();
    // Close the reader in every case (its manifest poller is a background task of its own).
    match tokio::time::timeout(READER_CLOSE_LIMIT, reader.close()).await {
        Ok(_) => {
            WARM_READERS_OPEN.fetch_sub(1, Relaxed);
        }
        Err(_) => eprintln!("compaction warm-up of {path}: reader not closed after {READER_CLOSE_LIMIT:?}"),
    }
    drop(reader);
    rec.secs_close = t_close.elapsed().as_secs_f64();
    match outcome {
        Ok(Some(r)) => r?,
        Ok(None) => {
            rec.l0_stopped = true;
            WARM_L0_STOPS.fetch_add(1, Relaxed);
        }
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
    Ok(rec)
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

    /// Revisore di C9 (2026-11-03): prove in più su #47. Tutte ignorate di default, una alla volta:
    /// `cargo test --release --lib warm::tests::rev_c9::<nome> -- --ignored --nocapture --test-threads=1`.
    /// Variabili: `RC9_KEYS`, `RC9_VLEN`, `RC9_GET_MS`, `RC9_DOUBLE` (1 = il ritardo di C8/C9, due volte), `RC9_HOT`
    /// (sparse, dense, clustered), `RC9_DBS` (database che lavano insieme), `RC9_FLUSH_MS`.
    mod rev_c9 {
        use super::*;
        use futures::stream::BoxStream;
        use object_store::path::Path;
        use object_store::{CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions, PutOptions, PutPayload, PutResult};
        use std::alloc::{GlobalAlloc, Layout, System};
        use std::sync::atomic::{AtomicI64, AtomicUsize};
        use std::sync::Mutex;

        // ---- memoria: un contatore dell'allocatore (solo nel binario dei test) ----
        struct Track;
        static CUR: AtomicUsize = AtomicUsize::new(0);
        static PEAK: AtomicUsize = AtomicUsize::new(0);
        unsafe impl GlobalAlloc for Track {
            unsafe fn alloc(&self, l: Layout) -> *mut u8 {
                let p = unsafe { System.alloc(l) };
                if !p.is_null() {
                    let c = CUR.fetch_add(l.size(), Relaxed) + l.size();
                    PEAK.fetch_max(c, Relaxed);
                }
                p
            }
            unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
                unsafe { System.dealloc(p, l) };
                CUR.fetch_sub(l.size(), Relaxed);
            }
            unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
                let q = unsafe { System.realloc(p, l, new) };
                if !q.is_null() {
                    CUR.fetch_sub(l.size(), Relaxed);
                    let c = CUR.fetch_add(new, Relaxed) + new;
                    PEAK.fetch_max(c, Relaxed);
                }
                q
            }
        }
        #[global_allocator]
        static ALLOC: Track = Track;

        // ---- uno S3 finto: ritardo una volta sola, richieste in volo, GET al secondo, guasti ----
        #[derive(Debug, Default)]
        pub struct Probe {
            pub delay_ms: AtomicU64,
            pub inflight: AtomicU64,
            pub peak: AtomicU64,
            pub gets: AtomicU64,
            pub stamps: Mutex<Vec<Instant>>,
            /// 0 nessun guasto, 1 ogni GET su una scatola nuova fallisce, 2 dopo `fail_after` GET sulle nuove, 3 restano appese.
            pub fault: AtomicU64,
            pub fail_after: AtomicI64,
            pub known: Mutex<HashSet<String>>,
            pub faulted: AtomicU64,
        }
        impl Probe {
            /// Il massimo di GET partite in una finestra di 1 s.
            pub fn max_per_sec(&self) -> usize {
                self.max_in(Duration::from_secs(1))
            }
            /// Il massimo di GET partite in una finestra `w` (per il ritmo al secondo: moltiplicare per 1 s / w).
            pub fn max_in(&self, w: Duration) -> usize {
                let mut s = self.stamps.lock().unwrap().clone();
                s.sort();
                let (mut j, mut best) = (0, 0);
                for i in 0..s.len() {
                    while s[i].duration_since(s[j]) >= w {
                        j += 1;
                    }
                    best = best.max(i - j + 1);
                }
                best
            }
        }
        struct Guard<'a>(&'a AtomicU64);
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Relaxed);
            }
        }
        #[derive(Debug)]
        pub struct DelayStore {
            pub inner: Arc<dyn ObjectStore>,
            pub p: Arc<Probe>,
        }
        impl std::fmt::Display for DelayStore {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "DelayStore")
            }
        }
        #[async_trait::async_trait]
        impl ObjectStore for DelayStore {
            async fn put_opts(&self, l: &Path, p: PutPayload, o: PutOptions) -> object_store::Result<PutResult> {
                self.inner.put_opts(l, p, o).await
            }
            async fn put_multipart_opts(&self, l: &Path, o: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
                self.inner.put_multipart_opts(l, o).await
            }
            async fn get_opts(&self, l: &Path, o: GetOptions) -> object_store::Result<GetResult> {
                if !o.head {
                    let n = self.p.inflight.fetch_add(1, Relaxed) + 1;
                    self.p.peak.fetch_max(n, Relaxed);
                    self.p.gets.fetch_add(1, Relaxed);
                    self.p.stamps.lock().unwrap().push(Instant::now());
                }
                let _g = (!o.head).then(|| Guard(&self.p.inflight));
                let d = self.p.delay_ms.load(Relaxed);
                if d > 0 {
                    tokio::time::sleep(Duration::from_millis(d)).await;
                }
                let fault = self.p.fault.load(Relaxed);
                if fault > 0 && l.as_ref().ends_with(".sst") && !self.p.known.lock().unwrap().contains(l.as_ref()) {
                    let hit = match fault {
                        1 | 3 => true,
                        _ => self.p.fail_after.fetch_sub(1, Relaxed) <= 0,
                    };
                    if hit {
                        self.p.faulted.fetch_add(1, Relaxed);
                        if fault == 3 {
                            tokio::time::sleep(Duration::from_secs(3600)).await;
                        }
                        return Err(object_store::Error::Generic { store: "rev-c9", source: "guasto finto di S3".into() });
                    }
                }
                self.inner.get_opts(l, o).await
            }
            fn delete_stream(&self, s: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
                self.inner.delete_stream(s)
            }
            fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
                self.inner.list(p)
            }
            fn list_with_offset(&self, p: Option<&Path>, o: &Path) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
                self.inner.list_with_offset(p, o)
            }
            async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
                self.inner.list_with_delimiter(p).await
            }
            async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> object_store::Result<()> {
                self.inner.copy_opts(f, t, o).await
            }
        }

        fn env<T: std::str::FromStr>(k: &str, d: T) -> T {
            std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
        }
        /// C11: `RC9_PRIMA=1` misura la versione di prima (C9): niente guardia sulle L0 del cameriere, niente tetto al ritmo.
        fn prima() {
            if env("RC9_PRIMA", 0u8) == 1 {
                COMPACTION_WARM_L0_WATCH.store(false, Relaxed);
                COMPACTION_WARM_GETS_PER_SEC.store(0, Relaxed);
            }
        }
        fn val(i: usize, len: usize, version: u8) -> Vec<u8> {
            (0..len).map(|j| (i.wrapping_mul(31).wrapping_add(j * 7) as u8) ^ version).collect()
        }

        #[derive(Clone, Copy, Debug, PartialEq)]
        enum Hot {
            Sparse,
            Dense,
            Clustered,
        }
        fn hot_keys_of(n: usize, h: Hot) -> Vec<usize> {
            let count = n / HOT_EVERY;
            match h {
                Hot::Sparse => (0..n).step_by(HOT_EVERY).collect(),
                Hot::Dense => (n / 3..n / 3 + count).collect(),
                Hot::Clustered => {
                    let per = count / 40;
                    (0..40).flat_map(|c| (c * (n / 40)..c * (n / 40) + per)).collect()
                }
            }
        }

        struct Prepared {
            id: String,
            db: Db,
            raw: Arc<dyn ObjectStore>,
            counting: Arc<CountingStore>,
            hot: Vec<usize>,
            vlen: usize,
        }

        async fn prepare(id: &str, n: usize, vlen: usize, hot: Hot) -> Prepared {
            prima();
            crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(1 << 30, Relaxed);
            crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(env("RC9_L0MAX", 64), Relaxed);
            crate::lifecycle::MEMTABLE_MAX_BYTES.store(1 << 30, Relaxed);
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
                    db.put(&key(i), &val(i, vlen, 0)).await.unwrap();
                }
                db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await.unwrap();
            }
            let hot = hot_keys_of(n, hot);
            let p = Prepared { id: id.to_string(), db, raw, counting, hot, vlen };
            let _ = served(&p).await;
            let (s, r) = served(&p).await;
            assert!(s as f64 / r as f64 >= SERVED_MIN, "il bancone non era caldo prima del lavaggio ({s}/{r})");
            p
        }

        /// Letture calde servite dal bancone (nessuna GET su una scatola), con il valore giusto.
        async fn served(p: &Prepared) -> (u64, u64) {
            let mut s = 0;
            for &i in &p.hot {
                let g0 = p.counting.counters.get_cat[2].load(Relaxed);
                let v = p.db.get(&key(i)).await.unwrap().expect("chiave persa");
                assert_eq!(v.as_ref(), val(i, p.vlen, 0).as_slice(), "valore vecchio o sbagliato");
                if p.counting.counters.get_cat[2].load(Relaxed) == g0 {
                    s += 1;
                }
            }
            (s, p.hot.len() as u64)
        }

        #[derive(Debug, Default, Clone)]
        struct Out {
            served: u64,
            reads: u64,
            secs: f64,
            ok: bool,
            l0_after: usize,
            late_gets: u64,
            inflight_after: u64,
            pass_gets: u64,
        }

        /// Un lavaggio di `p` con le GET del lavaggio che passano da `probe` (ritardo una volta) o, con `double`, dal ritardo
        /// di C8/C9 (due volte).
        async fn pass(p: &Prepared, probe: &Arc<Probe>, delay: u64, double: bool, warm: bool) -> Out {
            let s3: Arc<dyn ObjectStore> = if double {
                crate::counting::TEST_GET_DELAY_MS.store(delay, Relaxed);
                probe.delay_ms.store(0, Relaxed);
                Arc::new(DelayStore { inner: p.counting.clone(), p: Arc::clone(probe) })
            } else {
                probe.delay_ms.store(delay, Relaxed);
                Arc::new(DelayStore { inner: Arc::clone(&p.raw), p: Arc::clone(probe) })
            };
            let t = Instant::now();
            let rec = compact_tenant_pass(&s3, &p.raw, &p.id, Duration::from_secs(600), COMPACTOR_STOP_LIMIT, warm).await;
            let secs = t.elapsed().as_secs_f64();
            let g_end = probe.gets.load(Relaxed);
            let pass_gets = rec.as_ref().ok().and_then(|r| r.as_ref()).map_or(0, |r| r.requests.get + r.requests.get_range + r.requests.head);
            Out { secs, ok: rec.is_ok(), l0_after: p.db.manifest().l0().len(), pass_gets, late_gets: g_end, ..Default::default() }
        }

        async fn finish(p: &Prepared, probe: &Arc<Probe>, mut o: Out) -> Out {
            crate::counting::TEST_GET_DELAY_MS.store(0, Relaxed);
            probe.fault.store(0, Relaxed);
            tokio::time::sleep(Duration::from_secs(2)).await;
            o.late_gets = probe.gets.load(Relaxed) - o.late_gets;
            o.inflight_after = probe.inflight.load(Relaxed);
            probe.delay_ms.store(0, Relaxed);
            let (s, r) = served(p).await;
            o.served = s;
            o.reads = r;
            o
        }

        async fn close(p: Prepared) {
            writer_closed(&p.id);
            p.db.close().await.unwrap();
        }

        fn hot_of_env() -> Hot {
            match std::env::var("RC9_HOT").as_deref() {
                Ok("dense") => Hot::Dense,
                Ok("clustered") => Hot::Clustered,
                _ => Hot::Sparse,
            }
        }

        /// Punto 1: la prova che decide nelle varianti (dimensione, ritardo una o due volte, chiavi calde dense e sparse).
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[ignore]
        async fn variants() {
            let n: usize = env("RC9_KEYS", 200_000);
            let vlen: usize = env("RC9_VLEN", 1000);
            let delay: u64 = env("RC9_GET_MS", 40);
            let double = env("RC9_DOUBLE", 0u8) == 1;
            let hot = hot_of_env();
            let only_on = env("RC9_ONLY_ON", 0u8) == 1;
            let mut res = Vec::new();
            for warm in [false, true] {
                if only_on && !warm {
                    continue;
                }
                let probe = Arc::new(Probe::default());
                let p = prepare(&format!("rc9-var-{warm}"), n, vlen, hot).await;
                let (t0, f0, r0) = (WARM_TIMEOUTS.load(Relaxed), WARM_TRUNCATED.load(Relaxed), WARM_READERS_OPEN.load(Relaxed));
                probe.stamps.lock().unwrap().clear();
                let o = pass(&p, &probe, delay, double, warm).await;
                let o = finish(&p, &probe, o).await;
                println!(
                    "RC9 variants n={n} vlen={vlen} hot={hot:?} GET {delay} ms x{} warm={warm}: servite {}/{} ({:.1}%), lavaggio {:.2} s, ok {}, L0 dopo {}, GET del lavaggio {}, picco in volo {}, max GET in 1 s {} (ritmo massimo su 200 ms: {}/s), tetto scaduto {}, parziale {}, lettori aperti {}, GET dopo la fine {}",
                    if double { 2 } else { 1 },
                    o.served,
                    o.reads,
                    100.0 * o.served as f64 / o.reads as f64,
                    o.secs,
                    o.ok,
                    o.l0_after,
                    o.pass_gets,
                    probe.peak.load(Relaxed),
                    probe.max_per_sec(),
                    probe.max_in(Duration::from_millis(200)) * 5,
                    WARM_TIMEOUTS.load(Relaxed) - t0,
                    WARM_TRUNCATED.load(Relaxed) - f0,
                    WARM_READERS_OPEN.load(Relaxed) as i64 - r0 as i64,
                    o.late_gets
                );
                res.push(o);
                close(p).await;
            }
        }

        /// Punto 2a: più database lavano insieme sulla stessa macchina, GET a `RC9_GET_MS` (una volta): picco di GET in volo e
        /// massimo di GET in un secondo, contro i 5.500 al secondo per prefisso di S3.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[ignore]
        async fn many_dbs_at_once() {
            let n: usize = env("RC9_KEYS", 200_000);
            let k: usize = env("RC9_DBS", 2);
            let delay: u64 = env("RC9_GET_MS", 20);
            let probe = Arc::new(Probe::default());
            let mut ps = Vec::new();
            for i in 0..k {
                ps.push(prepare(&format!("rc9-many-{i}"), n, 1000, Hot::Sparse).await);
            }
            probe.stamps.lock().unwrap().clear();
            let outs = futures::future::join_all(ps.iter().map(|p| pass(p, &probe, delay, false, true))).await;
            let peak = probe.peak.load(Relaxed);
            let maxs = probe.max_per_sec();
            for (p, o) in ps.iter().zip(outs) {
                let o = finish(p, &probe, o).await;
                println!("RC9 many k={k} {}: servite {}/{}, lavaggio {:.2} s, ok {}, L0 dopo {}", p.id, o.served, o.reads, o.secs, o.ok, o.l0_after);
            }
            println!("RC9 many k={k} GET {delay} ms: picco GET in volo {peak}, massimo GET in 1 s {maxs} (ritmo massimo su 200 ms: {}/s), lettori aperti {}", probe.max_in(Duration::from_millis(200)) * 5, WARM_READERS_OPEN.load(Relaxed));
            for p in ps {
                close(p).await;
            }
        }

        /// Punto 2b e 2c: il tetto che scade a metà (anche quello di default, 3 s, con un insieme caldo troppo grande) e i
        /// guasti di S3 durante il riscaldamento: il lavaggio va avanti, il cameriere cambia foglio, il lettore si chiude,
        /// nessuna GET resta in volo né parte dopo.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[ignore]
        async fn limits_and_faults() {
            let n: usize = env("RC9_KEYS", 200_000);
            let delay: u64 = env("RC9_GET_MS", 40);
            // (nome, guasto, fail_after, tetto ms, concorrenza)
            let cases: Vec<(&str, u64, i64, u64, u64)> = vec![
                ("tetto 3 s scade a meta' (concorrenza 16)", 0, 0, 3000, 16),
                ("tetto 1 s scade a meta'", 0, 0, 1000, 128),
                ("ogni GET sulle scatole nuove fallisce", 1, 0, 3000, 128),
                ("GET sulle nuove falliscono dopo 300", 2, 300, 3000, 128),
                ("GET sulle nuove restano appese", 3, 0, 3000, 128),
            ];
            let (ms0, c0) = (COMPACTION_WARM_MS.load(Relaxed), COMPACTION_WARM_CONCURRENCY.load(Relaxed));
            let mut bad = Vec::new();
            for (name, fault, after, ms, conc) in cases {
                let probe = Arc::new(Probe::default());
                let p = prepare(&format!("rc9-lim-{fault}-{ms}-{conc}"), n, 1000, Hot::Sparse).await;
                *probe.known.lock().unwrap() = p.raw.list(None).map(|m| m.unwrap().location.to_string()).collect::<Vec<_>>().await.into_iter().collect();
                probe.fault.store(fault, Relaxed);
                probe.fail_after.store(after, Relaxed);
                COMPACTION_WARM_MS.store(ms, Relaxed);
                COMPACTION_WARM_CONCURRENCY.store(conc, Relaxed);
                let (t0, f0, r0) = (WARM_TIMEOUTS.load(Relaxed), WARM_FAILS.load(Relaxed), WARM_READERS_OPEN.load(Relaxed));
                let o = pass(&p, &probe, delay, false, true).await;
                let o = finish(&p, &probe, o).await;
                let readers = WARM_READERS_OPEN.load(Relaxed) as i64 - r0 as i64;
                println!(
                    "RC9 limits {name}: servite {}/{}, lavaggio {:.2} s, ok {}, L0 dopo {}, tetto scaduto {}, errori {}, GET guaste {}, lettori aperti {readers}, GET in volo dopo {}, GET partite nei 2 s dopo {}",
                    o.served,
                    o.reads,
                    o.secs,
                    o.ok,
                    o.l0_after,
                    WARM_TIMEOUTS.load(Relaxed) - t0,
                    WARM_FAILS.load(Relaxed) - f0,
                    probe.faulted.load(Relaxed),
                    o.inflight_after,
                    o.late_gets
                );
                if !o.ok || o.l0_after != 0 || readers != 0 || o.inflight_after != 0 || o.late_gets != 0 || o.secs > ms as f64 / 1000.0 + 8.0 {
                    bad.push(name);
                }
                close(p).await;
            }
            COMPACTION_WARM_MS.store(ms0, Relaxed);
            COMPACTION_WARM_CONCURRENCY.store(c0, Relaxed);
            assert!(bad.is_empty(), "casi falliti: {bad:?}");
        }

        /// Punto 2d: la memoria delle chiavi calde con il tetto di 32 MB. Un database di documenti piccoli dove ogni blocco è
        /// caldo: le chiavi calde superano il tetto. Misura i byte allocati in più (picco e tenuti) durante `hot_keys`.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[ignore]
        async fn key_memory_cap() {
            let n: usize = env("RC9_KEYS", 2_000_000);
            let vlen: usize = env("RC9_VLEN", 16);
            let p = prepare("rc9-mem", n, vlen, Hot::Sparse).await;
            let cache = crate::lifecycle::shared_cache().unwrap();
            let scope = crate::lifecycle::writer_scope(&p.id).unwrap();
            let consumed: Vec<SsTableHandle> = p.db.manifest().l0().iter().map(|v| v.sst.clone()).collect();
            let scoped: Arc<dyn DbCache> = Arc::new(ScopedCache { inner: Arc::clone(&cache), scope });
            let store: Arc<dyn ObjectStore> = p.counting.clone();
            let ssts = SstReader::new(tenant_path(&p.id), store, Some(scoped), None);
            for cap_mb in [32usize, 8] {
                let base = CUR.load(Relaxed);
                PEAK.store(base, Relaxed);
                let (keys, hot, cut) = hot_keys(&ssts, &cache, scope, &consumed, cap_mb << 20).await.unwrap();
                let peak = PEAK.load(Relaxed) - base;
                let kept = CUR.load(Relaxed) as i64 - base as i64;
                let key_bytes: usize = keys.iter().map(|k| k.len()).sum();
                println!(
                    "RC9 memory cap {cap_mb} MB: blocchi caldi {hot}, chiavi tenute {} ({} byte di chiave, capacità del vettore {}), tagliato {cut}; allocati in più: picco {:.1} MB, tenuti {:.1} MB",
                    keys.len(),
                    key_bytes,
                    keys.capacity(),
                    peak as f64 / (1 << 20) as f64,
                    kept as f64 / (1 << 20) as f64
                );
                drop(keys);
            }
            close(p).await;
        }

        /// C11 (E1): la guardia ferma un riscaldamento già partito. Il cameriere non scarica durante il lavaggio; appena il
        /// riscaldamento è aperto scarica una scatola L0 ogni 50 ms (il primo scarico gli fa vedere il lavaggio, poi le L0
        /// risalgono). Con `l0_max_ssts` 8 il riscaldamento deve fermarsi a 7 L0, molto prima del tetto, chiudere il lettore,
        /// e nessuno scarico deve restare fermo.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[ignore]
        async fn l0_watch_stops_a_running_warm_up() {
            std::env::set_var("RC9_L0MAX", "8");
            let n: usize = env("RC9_KEYS", 200_000);
            let p = prepare("c11-l0-stop", n, 1000, Hot::Sparse).await;
            let probe = Arc::new(Probe::default());
            let (c0, ms0) = (COMPACTION_WARM_CONCURRENCY.load(Relaxed), COMPACTION_WARM_MS.load(Relaxed));
            // un riscaldamento lento (4 chiamate insieme, 40 ms per GET) e un tetto lungo: se non lo ferma la guardia, dura 10 s
            COMPACTION_WARM_CONCURRENCY.store(4, Relaxed);
            COMPACTION_WARM_MS.store(10_000, Relaxed);
            let (stops0, t0) = (WARM_L0_STOPS.load(Relaxed), WARM_TIMEOUTS.load(Relaxed));
            let flushes = Arc::new(Mutex::new(Vec::<u64>::new()));
            let l0max = Arc::new(AtomicU64::new(0));
            let stop = Arc::new(AtomicBool::new(false));
            let w = {
                let (db, flushes, stop, l0max) = (p.db.clone(), Arc::clone(&flushes), Arc::clone(&stop), Arc::clone(&l0max));
                tokio::spawn(async move {
                    while WARM_READERS_OPEN.load(Relaxed) == 0 && !stop.load(Relaxed) {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    let mut j = 0usize;
                    // solo mentre il riscaldamento è aperto: qui nessun lavaggio libera le L0 dopo, e la nona scatola fermerebbe
                    // lo scarico per sempre
                    while !stop.load(Relaxed) && WARM_READERS_OPEN.load(Relaxed) > 0 {
                        for _ in 0..50 {
                            db.put(&key(HOT_EVERY / 2 + j * HOT_EVERY), &val(j, 1000, 9)).await.unwrap();
                            j += 1;
                        }
                        let t = Instant::now();
                        db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await.unwrap();
                        flushes.lock().unwrap().push(t.elapsed().as_millis() as u64);
                        l0max.fetch_max(db.manifest().l0().len() as u64, Relaxed);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                })
            };
            let o = pass(&p, &probe, 40, false, true).await;
            let l0_after = p.db.manifest().l0().len();
            tokio::time::sleep(Duration::from_millis(500)).await;
            stop.store(true, Relaxed);
            w.await.unwrap();
            COMPACTION_WARM_CONCURRENCY.store(c0, Relaxed);
            COMPACTION_WARM_MS.store(ms0, Relaxed);
            let fl = flushes.lock().unwrap().clone();
            let stops = WARM_L0_STOPS.load(Relaxed) - stops0;
            let timeouts = WARM_TIMEOUTS.load(Relaxed) - t0;
            println!(
                "C11 l0 watch: lavaggio {:.2} s ok {}, fermato dalla guardia {stops}, tetto scaduto {timeouts}, lettori aperti {}, L0 alla fine {l0_after}, L0 massimo visto {}, scarichi {} (piu' lento {} ms)",
                o.secs,
                o.ok,
                WARM_READERS_OPEN.load(Relaxed),
                l0max.load(Relaxed),
                fl.len(),
                fl.iter().max().copied().unwrap_or(0)
            );
            assert!(o.ok);
            assert_eq!(stops, 1, "la guardia non ha fermato il riscaldamento");
            assert_eq!(timeouts, 0);
            assert_eq!(WARM_READERS_OPEN.load(Relaxed), 0);
            assert!(fl.iter().all(|&d| d < 500), "uno scarico e' rimasto fermo: {fl:?}");
            close(p).await;
        }

        /// Punto 2e e 2f: scritture e letture mentre il lavaggio e il riscaldamento girano con le GET del lavaggio lente (una
        /// volta, `RC9_GET_MS`); il cameriere scrive di continuo e scarica una scatola L0 ogni `RC9_FLUSH_MS`. Nessuna lettura
        /// vecchia; latenza delle scritture (p50, p99, massimo) con e senza riscaldamento.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[ignore]
        async fn writes_during_slow_warm() {
            let delay: u64 = env("RC9_GET_MS", 40);
            let flush_ms: u64 = env("RC9_FLUSH_MS", 500);
            let n: usize = env("RC9_KEYS", 200_000);
            let rounds: usize = env("RC9_ROUNDS", 2);
            // C11: lo scarico L0 più lento di ogni giro, senza e con riscaldamento
            let mut slowest: Vec<(usize, bool, f64)> = Vec::new();
            for round in 0..rounds {
                for warm in [false, true] {
                    let probe = Arc::new(Probe::default());
                    let p = prepare(&format!("rc9-sw-{warm}-{round}"), n, 1000, Hot::Sparse).await;
                    let wkeys: Vec<usize> = (HOT_EVERY / 2..n).step_by(HOT_EVERY).collect();
                    let acked: Arc<Vec<AtomicU64>> = Arc::new((0..n).map(|_| AtomicU64::new(0)).collect());
                    let stop = Arc::new(AtomicBool::new(false));
                    let lats = Arc::new(Mutex::new(Vec::<u64>::new()));
                    let flat = Arc::new(Mutex::new(Vec::<(Instant, u64)>::new()));
                    let w = {
                        let (db, acked, stop, lats, wkeys, flat) = (p.db.clone(), Arc::clone(&acked), Arc::clone(&stop), Arc::clone(&lats), wkeys.clone(), Arc::clone(&flat));
                        tokio::spawn(async move {
                            let mut seq = 1u64;
                            let mut last = Instant::now();
                            let mut j = 0usize;
                            while !stop.load(Relaxed) {
                                let i = wkeys[j % wkeys.len()];
                                j += 1;
                                let mut v = seq.to_be_bytes().to_vec();
                                v.resize(1000, 7);
                                let t = Instant::now();
                                db.put(&key(i), &v).await.unwrap();
                                lats.lock().unwrap().push(t.elapsed().as_micros() as u64);
                                acked[i].store(seq, Relaxed);
                                seq += 1;
                                if last.elapsed() >= Duration::from_millis(flush_ms) {
                                    let tf = Instant::now();
                                    db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await.unwrap();
                                    flat.lock().unwrap().push((tf, tf.elapsed().as_micros() as u64));
                                    last = Instant::now();
                                }
                                tokio::time::sleep(Duration::from_millis(2)).await;
                            }
                        })
                    };
                    let stale = Arc::new(AtomicU64::new(0));
                    let reads = Arc::new(AtomicU64::new(0));
                    let r = {
                        let (db, acked, stop, stale, reads, wkeys, hot) = (p.db.clone(), Arc::clone(&acked), Arc::clone(&stop), Arc::clone(&stale), Arc::clone(&reads), wkeys.clone(), p.hot.clone());
                        tokio::spawn(async move {
                            let mut k = 0usize;
                            while !stop.load(Relaxed) {
                                let i = wkeys[(k * 13) % wkeys.len()];
                                let h = hot[(k * 7919) % hot.len()];
                                k += 1;
                                let floor = acked[i].load(Relaxed);
                                let v = db.get(&key(i)).await.unwrap().expect("chiave persa");
                                let got = if v.as_ref() == val(i, 1000, 0).as_slice() { 0 } else { u64::from_be_bytes(v[..8].try_into().unwrap()) };
                                if got < floor {
                                    stale.fetch_add(1, Relaxed);
                                }
                                let hv = db.get(&key(h)).await.unwrap().expect("chiave calda persa");
                                if hv.as_ref() != val(h, 1000, 0).as_slice() {
                                    stale.fetch_add(1, Relaxed);
                                }
                                reads.fetch_add(2, Relaxed);
                            }
                        })
                    };
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    let l0_start = p.db.manifest().l0().len();
                    let n0 = lats.lock().unwrap().len();
                    let tp = Instant::now();
                    let l0max = Arc::new(AtomicU64::new(0));
                    let mon = {
                        let (db, l0max, stop) = (p.db.clone(), Arc::clone(&l0max), Arc::clone(&stop));
                        tokio::spawn(async move {
                            while !stop.load(Relaxed) {
                                l0max.fetch_max(db.manifest().l0().len() as u64, Relaxed);
                                tokio::time::sleep(Duration::from_millis(20)).await;
                            }
                        })
                    };
                    let o = pass(&p, &probe, delay, false, warm).await;
                    let tend = Instant::now();
                    let n1 = lats.lock().unwrap().len();
                    let l0_end_pass = p.db.manifest().l0().len();
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    // C11: gli scarichi che si sovrappongono al lavaggio, letti dopo l'attesa, così conta anche quello rimasto
                    // fermo sulle L0 piene fino al cambio del foglio (prima si leggevano subito alla fine del lavaggio e quello
                    // non era ancora finito: senza riscaldamento il più lento risultava spesso 1 ms).
                    let fl: Vec<u64> = flat.lock().unwrap().iter().filter(|(t, d)| *t <= tend && *t + Duration::from_micros(*d) >= tp).map(|(_, d)| *d).collect();
                    stop.store(true, Relaxed);
                    w.await.unwrap();
                    r.await.unwrap();
                    mon.await.unwrap();
                    let mut l: Vec<u64> = lats.lock().unwrap()[n0..n1].to_vec();
                    l.sort();
                    let q = |f: f64| l.get(((l.len() as f64 * f) as usize).min(l.len().saturating_sub(1))).copied().unwrap_or(0) as f64 / 1000.0;
                    probe.delay_ms.store(0, Relaxed);
                    let (s, rr) = served(&p).await;
                    println!(
                        "RC9 switch GET {delay} ms flush {flush_ms} ms warm={warm} giro {round}: lavaggio {:.2} s ok {}; scritture durante il lavaggio {} p50 {:.1} ms p99 {:.1} ms max {:.1} ms; letture {} vecchie {}; L0 all'inizio {l0_start}, alla fine del lavaggio {l0_end_pass}, massimo {}; scarichi L0 nel lavaggio {} (piu' lento {:.0} ms); calde servite dopo {s}/{rr}",
                        o.secs,
                        o.ok,
                        l.len(),
                        q(0.5),
                        q(0.99),
                        l.last().copied().unwrap_or(0) as f64 / 1000.0,
                        reads.load(Relaxed),
                        stale.load(Relaxed),
                        l0max.load(Relaxed),
                        fl.len(),
                        fl.iter().max().copied().unwrap_or(0) as f64 / 1000.0
                    );
                    assert_eq!(stale.load(Relaxed), 0, "una lettura vecchia");
                    slowest.push((round, warm, fl.iter().max().copied().unwrap_or(0) as f64 / 1000.0));
                    close(p).await;
                }
            }
            // C11 (E1): con `RC9_FLUSH_GAP_MS` il test fallisce se in un giro lo scarico più lento con riscaldamento supera
            // quello senza di più di tanto (la prova che decide: 500 ms con `RC9_L0MAX=8 RC9_FLUSH_MS=250`).
            let gap: Option<f64> = std::env::var("RC9_FLUSH_GAP_MS").ok().and_then(|v| v.parse().ok());
            for round in 0..rounds {
                let get = |w: bool| slowest.iter().find(|(r, x, _)| *r == round && *x == w).map_or(0.0, |s| s.2);
                let (off, on) = (get(false), get(true));
                println!("RC9 switch giro {round}: scarico piu' lento senza {off:.0} ms, con {on:.0} ms, differenza {:.0} ms", on - off);
                if let Some(g) = gap {
                    assert!(on <= off + g, "giro {round}: lo scarico piu' lento con riscaldamento ({on:.0} ms) supera quello senza ({off:.0} ms) di piu' di {g} ms");
                }
            }
        }
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

    /// Revisore di C11: il secchio di gettoni (`take_token`) da solo. Ritmo, gettoni persi dopo che i riscaldamenti in
    /// attesa vengono fermati, e ritorno al normale dopo una pausa. Si lancia da solo (il secchio è uno per processo).
    mod rev_c11 {
        use super::super::*;
        use std::time::Instant;

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[ignore]
        async fn token_bucket() {
            COMPACTION_WARM_GETS_PER_SEC.store(3000, Relaxed);
            *BUCKET.lock().unwrap() = None;
            // 1. ritmo: 6.000 GET da 256 chiamate insieme
            let t = Instant::now();
            let done = Arc::new(AtomicU64::new(0));
            let hs: Vec<_> = (0..256)
                .map(|_| {
                    let done = Arc::clone(&done);
                    tokio::spawn(async move {
                        loop {
                            if done.fetch_add(1, Relaxed) >= 6000 {
                                break;
                            }
                            take_token().await;
                        }
                    })
                })
                .collect();
            for h in hs {
                h.await.unwrap();
            }
            let secs = t.elapsed().as_secs_f64();
            println!("RC11 bucket: 6000 gettoni in {secs:.3} s, ritmo {:.0}/s", 6000.0 / secs);
            // 2. 512 chiamate in attesa, fermate dopo 10 ms (come un riscaldamento fermato dal tetto o dalla guardia)
            tokio::time::sleep(Duration::from_millis(500)).await;
            let hs: Vec<_> = (0..512).map(|_| tokio::spawn(async { take_token().await })).collect();
            tokio::time::sleep(Duration::from_millis(10)).await;
            for h in &hs {
                h.abort();
            }
            let t = Instant::now();
            take_token().await;
            let wait_now = t.elapsed().as_secs_f64() * 1000.0;
            // 3. dopo una pausa i gettoni persi non contano più
            tokio::time::sleep(Duration::from_millis(300)).await;
            let t = Instant::now();
            for _ in 0..100 {
                take_token().await;
            }
            let wait_later = t.elapsed().as_secs_f64() * 1000.0;
            println!("RC11 bucket: dopo 512 attese fermate, il gettone dopo aspetta {wait_now:.1} ms; 300 ms dopo, 100 gettoni in {wait_later:.1} ms");
            assert!((2700.0..3300.0).contains(&(6000.0 / secs)));
            assert!(wait_now < 200.0);
            assert!(wait_later < 5.0);
            // 4. 0 = nessun tetto
            COMPACTION_WARM_GETS_PER_SEC.store(0, Relaxed);
            let t = Instant::now();
            for _ in 0..100_000 {
                take_token().await;
            }
            println!("RC11 bucket: tetto 0, 100000 gettoni in {:.1} ms", t.elapsed().as_secs_f64() * 1000.0);
            COMPACTION_WARM_GETS_PER_SEC.store(3000, Relaxed);
        }
    }
}
