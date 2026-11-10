# Judge verdict, commit c7b5b76, with --sealed-runs 2

- readers: PASS (raw readers: PASS (24 matches, all in named files))
- build: PASS
- unit: FAIL (205 passed, 1 failed, see unit.log)
- diff-py: FAIL (4124/4127 same answers as mongod 7.0, see diff-py.log)
- diff-node: FAIL (4125/4127 same answers as mongod 7.0, see diff-node.log)
- ctl: FAIL (20 of 71 files: test_02_fork test_07_usage test_08_costs test_20_process_lease test_28_bulk_insert_stall test_31_lease_lost_store_down test_36_isolation_quotas test_36b_review_fixes test_42_customer_compaction test_42r_review_unique_build test_42t_kill_index_build test_43_review_compaction test_44_layout_v2 test_45_envelope_rollback test_46_review_c2_step2 test_47_c2_step2_fix test_47_c2_step7a_paths test_48_c2_step7b_forks_erasure test_49_review_c2_step7b test_50_c2_step7b_fix_long_commands)
- kill: FAIL (see kill.log)
