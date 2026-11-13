# test_04 bounded memory on integration-4 (CI 27014ff: sealed +25 MB, failing; perf-bytes alone passed 6/6)

- The SlateDB side of the export already is perf-bytes' fixed reader (src/fx/read.rs open_checkpoint_reader: no block
  cache, 32 MiB metadata cache), on the only export path (ctl/export.rs -> fx::pair::PairedReader::open_checkpoint).
- The cause on integration-4: since the clone-uploads fix a closed database is absorbed into our sealed runs, so the
  export reads mostly runs, and every run block it read went into the process-wide run cache (fx::counter, up to
  --sealed-cache-mb, 256 MiB). perf-bytes alone had the rows in SlateDB, hence its 6/6.
- Before (332c687 code), local, test_streaming_uses_bounded_memory, server RSS for a 40 MB export (limit < 20 MB):
  sealed2-derive +30, +25, +36 (3 of 3 FAIL); off +16, +7, +6 (3 of 3 pass).
- Fix: fx::counter::without_cache_put, a task-local scope in which cache_put keeps nothing; PairedReader::get and scan
  run in it (export, PITR's diff: one-pass control reads). Unit test fx::counter::no_cache_put_tests.
- After, local: sealed2-derive +1, +6, +5 (3 of 3 pass); off +9, +21, +4, +3, +11, +3 (5 of 6 pass: the off path reads
  no run and is untouched by the fix; +21 is the variance the off mode had before too, NOT KNOWN why).
- Whole test_04 file after: sealed2-derive 9 passed 1 skipped; off 9 passed 1 skipped. test_03_restore,
  test_09g_export_natural_order and test_04b (sealed2-derive): 13 passed, 2 skipped.
