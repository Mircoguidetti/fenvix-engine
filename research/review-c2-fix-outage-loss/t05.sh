#!/usr/bin/env bash
# fix check round 2: ctl test_05 test_concurrent_erase_and_fork_are_serialised, sealed2+derive (the judge's exports), per binary
export PATH=/root/.fenvix-judge/env/bin:$PATH FENVIX_SEALED_RUNS=2 FENVIX_SEALED_TPIN_MS=1000 FENVIX_SEALED_E1_WAIT_MS=0 FENVIX_SEALED_TEST_SHORT_E1_WAIT=1 FENVIX_SEALED_COMPACTOR=small FENVIX_SEALED_DERIVE=1 FENVIX_SEALED_CODEC=zstd
cd /home/user/rv/ctl-tests; cp /home/user/w-fr/ctl-tests/test_05_erase.py .
for spec in "$@"; do IFS=: read b n <<< "$spec"; for i in $(seq 1 $n); do
  echo "$b run $i: $(MONGO_SERVER_BIN=/home/user/bin-$b timeout 600 python -m pytest -q -p no:cacheprovider test_05_erase.py -k test_concurrent_erase_and_fork_are_serialised 2>&1 | tail -n 1)"
done; done
