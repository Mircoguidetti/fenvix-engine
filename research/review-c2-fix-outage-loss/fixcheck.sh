#!/usr/bin/env bash
# review of c2-fix-outage-loss, fix check: the reviewer's tests and the owner's test_52 on the fix round's binary
export PATH=/root/.fenvix-judge/env/bin:$PATH
M=/root/.fenvix-judge/data/minio; OUTF=$1; BIN=${2:-/home/user/bin-fr}
cd /home/user/rv/ctl-tests
run() { # FILE KEY N
  for i in $(seq 1 $3); do
    out=$(R52_OLD_BIN=/home/user/bin-i3 MONGO_SERVER_BIN=$BIN timeout 1500 python -m pytest -q -s $1 -k "$2" 2>&1)
    echo "$2 run $i: $(echo "$out" | grep -E "^reopened|^scanned|^after a restart|^P2 saw|^a fresh|^lease losses|^cycles|^hung|^now|^validate|^after deleting|^acknowledged|^batches|^reopenedWrites|^OK|^E  .*(Assert|Error)" | tr '\n' ' ' | cut -c1-1200) => $(echo "$out" | grep -oE "[0-9]+ (passed|failed)" | head -1)" >> $OUTF
    for b in $M/rvw-*; do rm -rf "$b" "$M/.minio.sys/buckets/$(basename $b)"; done; rm -rf $M/.minio.sys/tmp/.trash/*
  done
}
T=test_52r_review_c2_fix_outage_loss.py
run $T reopen_keeps 3
run $T takeover 3
run $T keep_answering 3
run $T damaged 2
run $T healthy_collections 1
run $T index_build 2
run $T split_insert 2
run $T mixed 2
run test_52_reopen_during_a_command.py "insert_spanning or transaction_spanning or validate_repair" 2
echo DONE >> $OUTF
