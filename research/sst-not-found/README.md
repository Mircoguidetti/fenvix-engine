# sst-not-found (2026-11-12): runs

All MEASURED on a 4 vCPU sandbox, local MinIO.
- t56-runs.txt: ctl test_56 per binary and mode (i4 = integration-4 292f526, sstfix = this branch): integration-4 fails
  in off and default mode (0 reads answered, 74 failed with "compacted/<sst>.sst not found", code 1); this branch passes 3 of
  3 in each mode (one stale-reader error each, retried); in the judge's sealed2+derive mode the case is not reached
  (staleReaderErrors 0: the small compactor leaves no L0 for a compaction after the close), both pass.
- Unit tests (src/lifecycle.rs sst_not_found_*): a FollowLatest reader across a compaction pass and each GC: no GC 200 of
  200 read; our SST GC 0 of 200 (4 of its 4 SSTs deleted); SlateDB's GC of a closed tenant 50 of 200 (3 of 4 deleted);
  every failure recognised as a stale reader; a fresh reader reads 200 of 200 in every case.
- First sighting: the long-find scenario of the c2-fix-outage-loss review (long_find_serving.py, 6 s lease, 1,000,000
  documents, judge's sealed2+derive exports) on c2-fix-outage-loss d3137e3 + c2-fix-long-reads 75b501d: 13 and 18 failures
  in 2 of 6 runs. repro.sh and loopfind.py are the loops used to reproduce it (the instrumented build that logged each
  deletion and each reader's SSTs is not on this branch).
