#!/bin/bash
# integration-3: the judge's kill phase (test_group_commit.py --trials 1 --secs 15), alone on a quiet machine, 3 trials
# per setting. Settings:
#   off       FENVIX_SEALED_RUNS=off (the server's default is now on)
#   default   sealed runs on with the product's compactor knobs (FENVIX_SEALED_COMPACTOR unset), first publish after 2
#             batches; T_pin 1 s and no E1 wait so a 15 s test database gets a version at all (production: 30 s)
#   small     the same with the judge's small compactor knobs (FENVIX_SEALED_COMPACTOR=small)
#   step3     sealed runs on, compactor off (FENVIX_SEALED_COMPACTOR=0): step 3's publish every 2 batches (integration-2's 904)
cd /home/user/fenvix-engine/loadtest || exit 1
PY=/root/.fenvix-judge/env/bin/python
run() { # tag, then env assignments
  local tag=$1; shift
  for i in 1 2 3; do
    out=$(env "$@" $PY test_group_commit.py --trials 1 --secs 15 2>&1 | grep -E "^s3 durable 0")
    echo "$tag trial $i: $out"
  done
}
S="FENVIX_SEALED_RUNS=2 FENVIX_SEALED_TPIN_MS=1000 FENVIX_SEALED_E1_WAIT_MS=0 FENVIX_SEALED_TEST_SHORT_E1_WAIT=1"
run off -u FENVIX_SEALED_COMPACTOR FENVIX_SEALED_RUNS=off
run default -u FENVIX_SEALED_COMPACTOR $S
run small $S FENVIX_SEALED_COMPACTOR=small
run step3 $S FENVIX_SEALED_COMPACTOR=0
