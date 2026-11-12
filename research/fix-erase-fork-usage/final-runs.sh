#!/usr/bin/env bash
# fix-erase-fork-usage: the final runs on the head's binary (/home/user/bin-eu), from a copy of ctl-tests with rvw- buckets
export PATH=/root/.fenvix-judge/env/bin:$PATH
cd /home/user/rv/ctl-tests; cp /home/user/w-eu/ctl-tests/test_05_erase.py /home/user/w-eu/ctl-tests/test_07_usage.py /home/user/w-eu/ctl-tests/test_08_costs.py /home/user/w-eu/ctl-tests/test_53_absent_leaves_no_usage.py .
M=/root/.fenvix-judge/data/minio
clean() { for x in $M/rvw-*; do rm -rf "$x" "$M/.minio.sys/buckets/$(basename $x)"; done; rm -rf $M/.minio.sys/tmp/.trash/*; }
S2D="FENVIX_SEALED_RUNS=2 FENVIX_SEALED_TPIN_MS=1000 FENVIX_SEALED_E1_WAIT_MS=0 FENVIX_SEALED_TEST_SHORT_E1_WAIT=1 FENVIX_SEALED_COMPACTOR=small FENVIX_SEALED_DERIVE=1 FENVIX_SEALED_CODEC=zstd"
f=0; for i in $(seq 1 20); do out=$(env $S2D MONGO_SERVER_BIN=/home/user/bin-eu timeout 600 python -m pytest -q -p no:cacheprovider test_05_erase.py -k test_concurrent_erase_and_fork_are_serialised 2>&1 | tail -n 1); echo "$out" | grep -q failed && f=$((f+1)); done; echo "race test sealed2-derive: $f failed of 20"; clean
for i in 1 2 3; do echo "test_53 run $i: $(MONGO_SERVER_BIN=/home/user/bin-eu timeout 600 python -m pytest -q -p no:cacheprovider test_53_absent_leaves_no_usage.py 2>&1 | tail -n 1)"; done; clean
for i in 1 2; do echo "test_05 whole file sealed2-derive run $i: $(env $S2D MONGO_SERVER_BIN=/home/user/bin-eu timeout 1200 python -m pytest -q -p no:cacheprovider test_05_erase.py 2>&1 | grep -E '^FAILED|passed|failed' | tr '\n' ' ')"; done; clean
for mode in off s2d; do for t in test_07_usage test_08_costs; do
  if [ $mode = off ]; then E="FENVIX_SEALED_RUNS=off"; else E="$S2D"; fi
  echo "$t $mode: $(env $E MONGO_SERVER_BIN=/home/user/bin-eu timeout 1500 python -m pytest -q -p no:cacheprovider $t.py 2>&1 | grep -E '^FAILED|passed|failed' | tr '\n' ' ' | cut -c1-300)"; clean
done; done
echo DONE
