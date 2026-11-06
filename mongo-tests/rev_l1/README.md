# Reviewer of L1 (motore-correzioni, 8e71776): tests

- `rev_l1_profiler_bg_test.py BINARY`: 300 cold index scans with the profiler on and off; compares the profiler's
  bytesRead and fenvixS3Gets with the store's own GET counters and with backgroundReads. Shows the profiler counting every
  GET twice (two CountingStore layers, mongo-server/src/main.rs:657-659, each calling meter::note_get, src/counting.rs:447).
- `rev_l1_kill9_test.py BINARY ROUNDS WORKDIR`: kill -9 at random moments while 4 writers insert with w:majority, small L0
  SSTs (passes and merges all the time), SST and WAL GC every 3 s; checks every acknowledged document after each restart.
- `rev_l1_rest_after_load.sh`: after `l1_compaction_load.sh`, restarts the server on the same store with no load and
  records every 15 s the SSTs on disk that are not in the manifest (`examples/rev_l1_store_check.rs`).
- `rev_l1_compaction_sampler.py`: serverStatus fenvix.compaction every 30 s during the load.
- `rev_l1_giro_piccolo.sh`: the small round of prova_infinite.py (K1's parameters) against the L1 binary.
- `main_a9f46d3_long_pass_test.patch`: L1's test compaction_pass_longer_than_its_stall_window_finishes on main a9f46d3
  (fails there with "did not finish (L0 8)").
