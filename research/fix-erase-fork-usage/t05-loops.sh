#!/usr/bin/env bash
# fix round: the whole ctl test_05 file N times per mode (off and the judge's sealed2+derive), on BIN; one line per run
export PATH=/root/.fenvix-judge/env/bin:$PATH
BIN=$1; N=$2; cd /home/user/rv/ctl-tests; M=/root/.fenvix-judge/data/minio
for mode in off s2d; do f=0; for i in $(seq 1 $N); do
  if [ $mode = off ]; then E="FENVIX_SEALED_RUNS=off"; else E="FENVIX_SEALED_RUNS=2 FENVIX_SEALED_TPIN_MS=1000 FENVIX_SEALED_E1_WAIT_MS=0 FENVIX_SEALED_TEST_SHORT_E1_WAIT=1 FENVIX_SEALED_COMPACTOR=small FENVIX_SEALED_DERIVE=1 FENVIX_SEALED_CODEC=zstd"; fi
  out=$(env $E MONGO_SERVER_BIN=$BIN timeout 1200 python -m pytest -q -p no:cacheprovider test_05_erase.py 2>&1 | grep -E '^FAILED|passed|failed' | tr '\n' ' ')
  echo "$out" | grep -q failed && f=$((f+1))
  echo "$mode run $i: $out"
  for x in $M/rvw-*; do rm -rf "$x" "$M/.minio.sys/buckets/$(basename $x)"; done; rm -rf $M/.minio.sys/tmp/.trash/*
done; echo "TOTAL $mode: $f failed of $N"; done
echo DONE
