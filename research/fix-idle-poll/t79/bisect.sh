#!/usr/bin/env bash
# test_79's two first-request tests on fix-box-first-trip's head 6f705ba (its own test file), sealed2-derive, with each of
# the head's changes turned off in turn by its control variable. Usage: bisect.sh <ctl-tests dir> <binary> <runs> <out>
set -u
CT=$1; BIN=$2; N=$3; OUT=$4
export PATH=/root/.fenvix-judge/env/bin:$PATH
S2D="FENVIX_SEALED_RUNS=2 FENVIX_SEALED_TPIN_MS=1000 FENVIX_SEALED_E1_WAIT_MS=0 FENVIX_SEALED_TEST_SHORT_E1_WAIT=1 FENVIX_SEALED_COMPACTOR=small FENVIX_SEALED_DERIVE=1 FENVIX_SEALED_CODEC=zstd"
for variant in "all-on" "gate-off FENVIX_SLATE_IDLE_POLL_MS=0" "next-wait-off FENVIX_FX_NEXT_WAIT_MS=0" "open-seed-off FENVIX_OPEN_SEED=0"; do
  set -- $variant; name=$1; shift
  for i in $(seq 1 $N); do
    r=$(cd $CT && env $S2D "$@" MONGO_SERVER_BIN=$BIN timeout 900 python -m pytest -q -s test_79_big_speed.py -k "first_request" 2>&1)
    echo "$name run $i: $(echo "$r" | grep -E 'passed|failed' | tail -1) | $(echo "$r" | grep -E '^E .*assert' | head -1 | cut -c1-200)" >> $OUT
  done
done
