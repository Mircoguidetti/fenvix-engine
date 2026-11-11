#!/usr/bin/env bash
# review of c2-fix-outage-loss: the clone-uploads reviewer's test_store_outage_past_the_lease_loses_no_acknowledged_insert,
# one outage run per pytest call, from a copy of ctl-tests whose buckets are named rvw- (the full-set rerun script deletes
# every ctl- bucket between files). Usage: outage-test.sh OUTFILE BIN:MODE:N ...  (MODE on | off0 | sealedoff)
export PATH=/root/.fenvix-judge/env/bin:$PATH
M=/root/.fenvix-judge/data/minio; OUTF=$1; shift
cd /home/user/rv/ctl-tests
for spec in "$@"; do
  IFS=: read bin mode n <<< "$spec"
  for i in $(seq 1 $n); do
    unset FENVIX_SEALED_RUNS FENVIX_SEALED_COMPACTOR FENVIX_SEALED_DERIVE FENVIX_SEALED_CODEC FENVIX_SEALED_E1_WAIT_MS FENVIX_SEALED_TEST_SHORT_E1_WAIT
    case $mode in
      on) export FENVIX_SEALED_RUNS=2 FENVIX_SEALED_COMPACTOR=small FENVIX_SEALED_DERIVE=1 FENVIX_SEALED_CODEC=zstd FENVIX_SEALED_E1_WAIT_MS=0 FENVIX_SEALED_TEST_SHORT_E1_WAIT=1;;
      off0) export FENVIX_SEALED_RUNS=2 FENVIX_SEALED_COMPACTOR=0 FENVIX_SEALED_DERIVE=1 FENVIX_SEALED_CODEC=zstd FENVIX_SEALED_E1_WAIT_MS=0 FENVIX_SEALED_TEST_SHORT_E1_WAIT=1;;
      sealedoff) export FENVIX_SEALED_RUNS=off;;
    esac
    out=$(R51_OUTAGE_RUNS=1 MONGO_SERVER_BIN=/home/user/bin-$bin timeout 900 python -m pytest -q -s test_51r_review_c2_fix_clone_uploads.py -k test_store_outage 2>&1)
    echo "$bin $mode run $i: $(echo "$out" | grep -E "^outage run|^acknowledged|^writers|SIGTERM did not" | tr '\n' ' ' | cut -c1-300) => $(echo "$out" | grep -oE "[0-9]+ (passed|failed)" | head -1)" >> $OUTF
    for b in $M/rvw-lstall-*; do [ -d "$b" ] && rm -rf "$b" "$M/.minio.sys/buckets/$(basename $b)"; done
    rm -rf $M/.minio.sys/tmp/.trash/*
  done
done
echo DONE >> $OUTF
