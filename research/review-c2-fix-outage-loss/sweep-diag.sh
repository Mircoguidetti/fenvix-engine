#!/usr/bin/env bash
# review of c2-fix-outage-loss, the coordinator's question: the clone-uploads reviewer's sweep test, with diagnostics (missing
# documents looked up by _id, lease losses and reopens in the first process's log). Usage: sweep-diag.sh OUT BIN:N ...
export PATH=/root/.fenvix-judge/env/bin:$PATH
M=/root/.fenvix-judge/data/minio; OUTF=$1; shift
cd /home/user/rv/ctl-tests
for spec in "$@"; do
  IFS=: read bin n <<< "$spec"
  for i in $(seq 1 $n); do
    out=$(MONGO_SERVER_BIN=/home/user/bin-$bin timeout 1200 python -m pytest -q -s test_51x_sweep_diag.py 2>&1)
    echo "$bin run $i: $(echo "$out" | grep -E "^live run|^after a restart|^DIAG|^read with sealed" | tr '\n' ' ' | cut -c1-900) => $(echo "$out" | grep -oE "[0-9]+ (passed|failed)" | head -1)" >> $OUTF
    for b in $M/rv51-sweep-*; do [ -d "$b" ] && rm -rf "$b" "$M/.minio.sys/buckets/$(basename $b)"; done
    rm -rf $M/.minio.sys/tmp/.trash/*
  done
done
echo DONE >> $OUTF
