#!/usr/bin/env bash
# SIGKILL durability tests, one trial per invocation (resumable): WAL cleanup (test_group_commit.py) and
# SST/manifest/compaction-state cleanup (test_open_gc.py), each with its negative control.
cd "$(dirname "$0")/../.."
OUT=mongo-tests/out/final; mkdir -p $OUT
PYV=/home/user/test5-compat-indexes/mongo-tests/.venv/bin/python
cd loadtest
one() {  # name args...
  local name=$1; shift
  local o=../$OUT/kill_$name.txt
  if [ -f "$o" ] && grep -q "^DONE" "$o"; then return; fi
  ../tools/final/env_up.sh > /dev/null || exit 1
  timeout 900 $PYV "$@" > "$o.tmp" 2>&1 && { mv "$o.tmp" "$o"; echo DONE >> "$o"; tail -2 "$o" | head -1 | cut -c1-200; } || { echo "interrupted: $name"; exit 2; }
}
for t in 1 2 3 4; do one wal_$t test_group_commit.py --trials 1 --secs 60; done
for t in 1 2; do one walctl_$t test_group_commit.py --trials 1 --secs 30 --control; done
for t in 1 2 3 4; do one sst_$t test_open_gc.py --trials 1 --secs 100; done
for t in 1 2; do one sstctl_$t test_open_gc.py --trials 1 --secs 60 --control; done
