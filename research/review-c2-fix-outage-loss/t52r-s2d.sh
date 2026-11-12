#!/usr/bin/env bash
# test_52r (with retried reads and the 300 s index bound) in the judge's sealed2+derive mode; per run, every count line
export PATH=/root/.fenvix-judge/env/bin:$PATH FENVIX_SEALED_RUNS=2 FENVIX_SEALED_TPIN_MS=1000 FENVIX_SEALED_E1_WAIT_MS=0 FENVIX_SEALED_TEST_SHORT_E1_WAIT=1 FENVIX_SEALED_COMPACTOR=small FENVIX_SEALED_DERIVE=1 FENVIX_SEALED_CODEC=zstd
cd /home/user/rv/ctl-tests; M=/root/.fenvix-judge/data/minio
for spec in "$@"; do IFS=: read b n <<< "$spec"; for i in $(seq 1 $n); do
  echo "===== $b run $i"
  R52_OLD_BIN=/home/user/bin-i3 MONGO_SERVER_BIN=/home/user/bin-$b timeout 2400 python -m pytest -q -s -p no:cacheprovider --tb=line test_52r_review_c2_fix_outage_loss.py 2>&1 | grep -E "^reopened|^acknowledged|^scanned|^after a restart|^P2 saw|^a fresh|^lease losses|^now|^cycles|^hung|^validate on|^after deleting|^damaged|^deleteMany|^many:|^fam:|^txn:|^ttl:|^one:|^OK|^partial|^failed call|^acknowledged call|^E  |^FAILED|passed|failed" | cut -c1-400
  for x in $M/rvw-*; do rm -rf "$x" "$M/.minio.sys/buckets/$(basename $x)"; done; rm -rf $M/.minio.sys/tmp/.trash/*
done; done
echo DONE
