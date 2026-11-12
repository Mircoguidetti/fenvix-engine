# Merge 3: perf-erase 0b5dc3b on integration-4, own tests once per mode
- Conflicts: as the scratch merge c5f0c80 (listed in the merge commit). Rename test_52_perf_erase -> test_69_perf_erase.
- Joint: test_69's BASE had --sealed-gc-grace-ms 2500, refused since integration-4's check_config rule (grace > T_pin +
  hold + put = 3000): the server exited at start (1 failed, 1 error). Raised to 3100, as test_49 in part 1.
- After: sealed2-derive test_69 + test_49 8 passed; off test_69 3 passed (it sets sealed runs on its own command line).
