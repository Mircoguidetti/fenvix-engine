# Merge 4: perf-bytes bdd947a on integration-4, own tests once per mode (plus the perf-erase tests: a joint)
- Conflicts: mongo-server/src/dbs.rs (integration-4's maybe_start_fxc / flush_then_stop_fxc and perf-bytes'
  fx_publish_now at the same place: all kept); the judge result json (integration-4's kept). No rename (56, 57 free).
- Joint 1 (build): perf-erase and perf-bytes both added `--sealed-version-retention-secs`, with different defaults:
  7 days (perf-erase: the compactors' old versions, `fx::compactor::VERSION_RETENTION_SECS`) and 0 (perf-bytes: GC's
  unheld versions, `fx::VERSION_RETENTION_MS`; with the drop on such a version cannot be read). Duplicate field, no
  build. One flag now sets both, with perf-bytes' default 0. FOR THE COORDINATOR: this changes perf-erase's default
  (old versions are no longer kept 7 days by the compactors; an erasure is done sooner).
- Joint 2: test_56 used --sealed-gc-grace-ms 4000 = T_pin + hold + put exactly; integration-4's check_config refuses it
  (15 errors at start). Now 4100.
- Joint 3: test_57's fill() asked fenvixReseal after one insert batch; since the clone-uploads fix the compactor starts
  at a database's second write batch, so "compactor: false" (3 failures). One update_one (no new document) per round.
- First run (ctl-sealed2-derive.txt, before the fixes): also test_69::test_a_full_batch_runs_without_waiting_for_the_window
  failed once (the first erasure already "scrubbed" 2 s after its delete instead of "pending"). It passed alone with
  FX_ERASE_TRACE=1 and in the grouped rerun: flaky, NOT KNOWN why; proposed for the CI repeats.
- Known, not this merge: test_43::test_a_refused_fork_leaves_no_object_of_its_target (usage files, fixed by
  fix-erase-500, merge 7); test_04 bounded memory sealed (+24 MB locally, as in part 1, section 7).
- After the fixes: sealed2-derive test_56 + test_57 + test_69 21 passed (ctl-sealed2-derive-after-fix.txt); off
  (ctl-off.txt) 15 passed, 19 skipped, 1 failed (test_43, known).
