#!/usr/bin/env bash
# sst-not-found: the long find of the test_52r check (6 s lease, N documents, judge's sealed2+derive exports), looped;
# keeps every server log. Usage: repro.sh BIN N RUNS OUT
export PATH=/root/.fenvix-judge/env/bin:$PATH FENVIX_SEALED_RUNS=2 FENVIX_SEALED_TPIN_MS=1000 FENVIX_SEALED_E1_WAIT_MS=0 FENVIX_SEALED_TEST_SHORT_E1_WAIT=1 FENVIX_SEALED_COMPACTOR=small FENVIX_SEALED_DERIVE=1 FENVIX_SEALED_CODEC=zstd
BIN=$1; N=$2; R=$3; OUT=$4; mkdir -p $OUT; cd /home/user/rv/ctl-tests; M=/root/.fenvix-judge/data/minio
for i in $(seq 1 $R); do
  line=$(timeout 900 python /home/user/w-fix/research/review-c2-fix-outage-loss/long_find_serving.py $BIN 6 $N 2>&1 | grep -E "^ttl|Error" | cut -c1-300)
  log=$(ls -t out/rvlongfind-*.log | head -1); cp $log $OUT/run$i.log
  echo "run $i: $line | server log: not-found lines $(grep -c 'not found' $log), SSTGC deleted $(grep -oE 'deleted_ssts=[0-9]+' $log | awk -F= '{s+=$2} END{print s+0}')"
  for x in $M/rvw-*; do rm -rf "$x" "$M/.minio.sys/buckets/$(basename $x)"; done; rm -rf $M/.minio.sys/tmp/.trash/*
done
