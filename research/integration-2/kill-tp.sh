#!/bin/bash
# The judge's kill phase (test_group_commit.py --trials 1 --secs 15), alone on a quiet machine, 3 times per setting.
# Settings: sealed runs off; FENVIX_SEALED_RUNS=N for N = 2 (the judge's), 10, 50, 0 (sealed reads on, no server publish).
cd /home/user/fenvix-engine/loadtest || exit 1
PY=/root/.fenvix-judge/env/bin/python
run() { # tag, then env assignments
  local tag=$1; shift
  for i in 1 2 3; do
    out=$(env "$@" $PY test_group_commit.py --trials 1 --secs 15 2>&1 | grep -E "^s3 durable 0")
    echo "$tag trial $i: $out"
  done
}
run off -u FENVIX_SEALED_RUNS
for n in 2 10 50 0; do
  run "sealed$n" FENVIX_SEALED_RUNS=$n FENVIX_SEALED_TPIN_MS=1000 FENVIX_SEALED_E1_WAIT_MS=0
done
