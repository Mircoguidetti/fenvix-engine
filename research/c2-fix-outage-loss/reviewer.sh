#!/usr/bin/env bash
# reviewer.sh: the reviewer's test_store_outage_past_the_lease_loses_no_acknowledged_insert, one outage run per pytest call
# (buckets removed between runs), per binary and mode; appends to /home/user/runs/reviewer-test.txt
export PATH=/root/.fenvix-judge/env/bin:$PATH
M=/root/.fenvix-judge/data/minio
cd /home/user/wt-rev/ctl-tests
for spec in "$@"; do
  IFS=: read bin mode n <<< "$spec"
  for i in $(seq 1 $n); do
    unset FENVIX_SEALED_RUNS FENVIX_SEALED_COMPACTOR FENVIX_SEALED_DERIVE FENVIX_SEALED_CODEC FENVIX_SEALED_E1_WAIT_MS FENVIX_SEALED_TEST_SHORT_E1_WAIT
    case $mode in
      on) export FENVIX_SEALED_RUNS=2 FENVIX_SEALED_COMPACTOR=small FENVIX_SEALED_DERIVE=1 FENVIX_SEALED_CODEC=zstd FENVIX_SEALED_E1_WAIT_MS=0 FENVIX_SEALED_TEST_SHORT_E1_WAIT=1;;
      off0) export FENVIX_SEALED_RUNS=2 FENVIX_SEALED_COMPACTOR=0 FENVIX_SEALED_DERIVE=1 FENVIX_SEALED_CODEC=zstd FENVIX_SEALED_E1_WAIT_MS=0 FENVIX_SEALED_TEST_SHORT_E1_WAIT=1;;
      sealedoff) export FENVIX_SEALED_RUNS=off;;
    esac
    out=$(R51_OUTAGE_RUNS=1 MONGO_SERVER_BIN=/home/user/bins/$bin timeout 900 python -m pytest -q -s test_51r_review_c2_fix_clone_uploads.py -k test_store_outage 2>&1)
    echo "$bin $mode run $i: $(echo "$out" | grep -E "^outage run|^acknowledged" | tr '\n' ' ' | cut -c1-300) => $(echo "$out" | grep -oE "[0-9]+ (passed|failed)" | head -1)" >> /home/user/runs/reviewer-test.txt
    for b in $M/ctl-lstall-*; do [ -d "$b" ] && rm -rf "$b" "$M/.minio.sys/buckets/$(basename $b)"; done
    rm -rf /root/.fenvix-judge/data/minio/.minio.sys/tmp/.trash/*
  done
done
echo DONE >> /home/user/runs/reviewer-test.txt
