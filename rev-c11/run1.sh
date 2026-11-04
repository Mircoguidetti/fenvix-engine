#!/bin/sh
cd /home/claude/fenvix-engine
T="cargo test --release --lib"
O="--ignored --nocapture --test-threads=1"
L=/home/claude/logs11
RC9_ROUNDS=2 RC9_GET_MS=40 RC9_L0MAX=8 RC9_FLUSH_MS=250 RC9_FLUSH_GAP_MS=500 $T warm::tests::rev_c9::writes_during_slow_warm -- $O > $L/e1-dopo.log 2>&1
RC9_GET_MS=20 RC9_DBS=2 RC9_KEYS=400000 $T warm::tests::rev_c9::many_dbs_at_once -- $O > $L/e2-dopo-1.log 2>&1
RC9_GET_MS=20 RC9_DBS=2 RC9_KEYS=400000 $T warm::tests::rev_c9::many_dbs_at_once -- $O > $L/e2-dopo-2.log 2>&1
$T warm::tests::big_db_slow_store -- $O > $L/big-1.log 2>&1
$T warm::tests::big_db_slow_store -- $O > $L/big-2.log 2>&1
$T warm::tests::writes_and_reads_during_the_switch -- --nocapture --test-threads=1 --include-ignored > $L/switch.log 2>&1
RC9_ROUNDS=2 RC9_GET_MS=40 RC9_L0MAX=14 RC9_FLUSH_MS=600 $T warm::tests::rev_c9::writes_during_slow_warm -- $O > $L/e1x-dopo-l0max14.log 2>&1
RC9_PRIMA=1 RC9_ROUNDS=2 RC9_GET_MS=40 RC9_L0MAX=14 RC9_FLUSH_MS=600 $T warm::tests::rev_c9::writes_during_slow_warm -- $O > $L/e1x-prima-l0max14.log 2>&1
$T warm::tests::rev_c9::l0_watch_stops_a_running_warm_up -- $O > $L/l0stop.log 2>&1
echo DONE > $L/run1-done
