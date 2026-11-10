#!/bin/bash
# The agent-lane reviewer's R1-R5 (loadtest/review_agent_lane, unchanged) on integration-2, sealed runs off and on (2).
cd /home/user/fenvix-engine/loadtest/review_agent_lane || exit 1
PY=/root/.fenvix-judge/env/bin/python; W=$1
for mode in off sealed2; do
  if [ $mode = sealed2 ]; then export FENVIX_SEALED_RUNS=2 FENVIX_SEALED_TPIN_MS=1000 FENVIX_SEALED_E1_WAIT_MS=0; else unset FENVIX_SEALED_RUNS FENVIX_SEALED_TPIN_MS FENVIX_SEALED_E1_WAIT_MS; fi
  for t in r1_log_read_your_writes r2_visible_before_durable r3_spare_cache_after_drop r4_spare_takeover_log_records r5_log_visible_before_durable; do
    s=$(date +%s)
    timeout 1800 $PY $t.py $W/$mode-$t > $W/$mode-$t.out 2>&1
    echo "$mode $t (code $?, $(( $(date +%s) - s )) s): $(grep -E "RESULT" $W/$mode-$t.out | tail -1)"
  done
done
echo R_DONE
