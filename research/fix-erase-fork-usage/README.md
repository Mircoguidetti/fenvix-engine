# fix-erase-fork-usage: runs (all MEASURED on a 4 vCPU sandbox, local MinIO; copy of ctl-tests with rvw- buckets)

Before (c2-fix-outage-loss d3137e3, binary bin-fr2):
- ctl test_53 (new): 2 of 2 tests fail: control/usage/src_k/... after an erase then a fork of the erased source; a usage
  record under every absent name probed by erase, fork, describe, usage, restore points, ip-allowlist, durability,
  checkpoints, erasures, keep-awake (quota makes no request); the third test (reads of an erased / never created
  database): control/usage/gone/... and control/usage/never/...
- test_05 test_concurrent_erase_and_fork_are_serialised alone, sealed2+derive exports: 4 failed of 20; default mode 0 of 20
  (the erase must win the race; it did in 11 of 30 races counted on the first fix version).

After (this branch, head 4d6e7f9, binary bin-eu): final-runs.txt (final-runs.sh)
- race test sealed2+derive 0 failed of 20; test_53 3 of 3 runs pass; test_05 whole file sealed2+derive 2 of 2 pass;
  test_07 off 10 passed; test_08 off 3 passed; test_07 and test_08 sealed2+derive: the known failures of CI (the proxy
  comparison is 1 short on d3137e3 too: 403/404, 400/401 there, 399/400, 403/404 here).
- fast judge sealed2+derive on 4d6e7f9: judge/out/4d6e7f9-sealed2-derive (known failures only, ctl 3 files PASS, kill
  0 of 17,562 lost).
