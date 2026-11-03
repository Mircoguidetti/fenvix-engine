# Compaction warm-up (M27)

## The problem

A compaction pass rewrites SSTs. slatedb's block cache is keyed by (cache scope, SST id, block offset), so every block of
a new SST is a miss until someone reads it back from object storage, even when the same rows were hot a second before.
The engine compacts with a standalone `CompactorBuilder` (`lifecycle::compact_tenant_within`), which in slatedb 0.17 has
no cache at all. Measured (`warm::tests::hot_reads_stay_in_cache_after_compaction`, in-memory store behind
`CountingStore`): 400 hot keys spread over a 20 MB database, 100% served by the cache before a pass, **0%** after it
(406 SST GETs for 400 reads).

## What slatedb 0.17 offers

* `BlockCachePolicy::with_compaction_output_targets`: caches compaction output as it is written, but only for the
  compactor embedded in a `Db` (`Settings::compactor_options`), and only for key ranges fixed when the `Db` is built.
  Our writers run with the embedded compactor off.
* `CompactorBuilder` (standalone): no `DbCache`, no policy.
* `DbCacheManagerOps::warm_sst` / `evict_cached_sst` on `Db` and `DbReader`: warm or drop the cache entries of one SST
  that is in that handle's manifest. Contiguous blocks are read with one range GET.
* `SstReader` (index and blocks of one SST, through an optional cache) and the public serde form of `CachedKey`.

## What the engine does now (`src/warm.rs`)

1. Before the pass: the open writer's manifest (its SSTs).
2. After the compactor wrote the new manifest, before the writer reads it: for each consumed SST, its index (from the
   shared metadata cache, under the writer's scope) and a probe of the block cache for every block; the keys of the hot
   blocks (read from the cache, no request).
3. A `DbReader` opened with the writer's cache scope sees the new manifest; for each new SST holding hot keys,
   `warm_sst(index, filters, data runs)`. The entries land under the writer's scope.
4. The writer refreshes its manifest (as before) and the consumed SSTs are evicted from its cache.

Bounded by `--compaction-warm-secs` (20); `--compaction-warm false` turns it off. Counters in `tenantStats`
(`maintenance.compactionWarm`). After the fix the same test measures **100%** after the first pass and **99.2%** after a
second pass (65.2% without); cold keys stay cold (0/400 served), so only what was hot is loaded.

## Limits

* The warm-up reads from object storage the blocks the reads would have read: it moves the GETs off the read path, it does
  not save them (two passes: 738 GETs with it, 174 without, of which about 540 are the warm-up).
* Window: the writer can switch on its own (manifest poll every `--writer-poll-secs`, or its own L0 flush) before the
  warm-up ends; those reads miss as before.
* Only the cache of this process, only open writers (not read-only `DbReader` tenants).
* For a large database whose pass rewrites gigabytes of hot data, the right place is inside the compactor (the bytes are in
  its memory as it writes them: zero GETs). That needs the standalone compactor to accept a cache and a hot-key predicate,
  a change to slatedb.
