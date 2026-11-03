#!/bin/sh
cd /home/claude/wt-warm
T="cargo test --release --lib"
O="--ignored --nocapture --test-threads=1"
L=/home/claude/logs
$T warm::tests:: -- --nocapture --test-threads=1 > $L/w2-pr-tests.log 2>&1
$T warm::tests::big_db_slow_store -- $O > $L/w1-decisive-2.log 2>&1
RC9_GET_MS=40 $T warm::tests::rev_c9::variants -- $O > $L/w3-var-40-sparse.log 2>&1
RC9_GET_MS=20 $T warm::tests::rev_c9::variants -- $O > $L/w3-var-20-sparse.log 2>&1
RC9_GET_MS=40 RC9_HOT=dense $T warm::tests::rev_c9::variants -- $O > $L/w3-var-40-dense.log 2>&1
RC9_GET_MS=40 RC9_HOT=clustered $T warm::tests::rev_c9::variants -- $O > $L/w3-var-40-clustered.log 2>&1
RC9_GET_MS=20 RC9_DOUBLE=1 $T warm::tests::rev_c9::variants -- $O > $L/w3-var-20x2-sparse.log 2>&1
RC9_GET_MS=20 RC9_DBS=2 $T warm::tests::rev_c9::many_dbs_at_once -- $O > $L/w4-many-2.log 2>&1
RC9_GET_MS=20 RC9_DBS=4 RC9_KEYS=100000 $T warm::tests::rev_c9::many_dbs_at_once -- $O > $L/w4-many-4.log 2>&1
RC9_GET_MS=40 $T warm::tests::rev_c9::limits_and_faults -- $O > $L/w5-limits.log 2>&1
$T warm::tests::rev_c9::key_memory_cap -- $O > $L/w6-mem.log 2>&1
RC9_GET_MS=40 $T warm::tests::rev_c9::writes_during_slow_warm -- $O > $L/w7-switch.log 2>&1
RC9_GET_MS=40 RC9_KEYS=1000000 $T warm::tests::rev_c9::variants -- $O > $L/w8-var-1gb.log 2>&1
echo DONE > $L/warm-done
