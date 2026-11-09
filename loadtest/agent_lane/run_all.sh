#!/usr/bin/env bash
# Every agent-lane test on the current build, in the order used for the report (about 1.5 hours on 4 cores).
# Results under loadtest/agent_lane/results/<test>/ (the test's own log; engine logs and work directories are not kept).
set -uo pipefail
cd "$(dirname "$0")"
PY=${JUDGE_HOME:-$HOME/.fenvix-judge}/env/bin/python
R=results
# durability, one writer and the mixed transactions do not measure time: they run side by side
$PY t1_durability.py $R/t1 --rounds 100 > /dev/null 2>&1 &
$PY t2_one_writer.py $R/t2 --trials 50 > /dev/null 2>&1 &
$PY t4_mixed_txn.py $R/t4-mixed --txns 3000 > /dev/null 2>&1 &
wait
$PY t1_durability.py $R/t1-control --rounds 20 --control > /dev/null 2>&1 &
$PY t2_one_writer.py $R/t2-control --trials 4 --control > /dev/null 2>&1 &
wait
$PY t3_read_your_writes.py $R/t3 > /dev/null 2>&1 &
./t4_langgraph.sh $R/t4-langgraph > /dev/null 2>&1 &
wait
# latency alone on the machine
$PY t5_latency.py $R/t5 > /dev/null 2>&1
grep -h RESULT $R/t1/*.log $R/t1-control/*.log $R/t2/*.log $R/t2-control/*.log $R/t3/*.log $R/t4-mixed/*.log $R/t5/*.log; cat $R/t4-langgraph/compare.txt
