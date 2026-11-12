# Merge 2: fix-query-defects f730353 on integration-4 (merge 244eaf3), own ctl tests once per mode
- Conflict: mongo-server/src/cmd.rs, serverStatus maintenance: both counters kept (reopenedWritesRefused from
  c2-fix-outage-loss, changeStreamsLost from P1).
- Joint: with both counters the serverStatus `doc!` passes the default macro recursion limit (128) and the build fails;
  `#![recursion_limit = "256"]` in mongo-server/src/main.rs.
- Renames: test_53_change_stream_falls_behind -> test_67_*, test_53r_review_p1 -> test_67r_*, test_54_query_defects -> test_68_*.
- sealed2-derive: 65 passed (ctl-sealed2-derive.txt). off: 64 passed, 1 failed (ctl-off.txt):
  test_67r::test_a_customer_erasure_between_writes_and_reads. A customer erasure exists only with sealed runs
  (fx/erase.rs, compaction 2 step 7b); with them off `delete_many` is plain deletes and the stream rightly delivers them.
  The test now skips with sealed runs off (as test_54 does): 4 passed, 1 skipped (ctl-off-67r-after-skip.txt).
