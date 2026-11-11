#!/bin/bash
# poor man's profiler of the runtime worker thread: N stacks, one per $3 s
P=$1; N=$2; GAP=$3; OUT=$4; BIN=/home/user/H/target/release/mongo-server
TN=$(gdb $BIN -p $P -batch -ex "info threads" 2>/dev/null | grep '"tokio-rt-worker"' | head -1 | sed 's/^\*//' | awk '{print $1}')
: > $OUT
for i in $(seq 1 $N); do echo "=== $i" >> $OUT; gdb $BIN -p $P -batch -ex "thread apply $TN bt 50" 2>/dev/null | grep "^#" >> $OUT; sleep $GAP; done
