#!/bin/sh
cd /home/claude/wt-warm
T="cargo test --release --lib"
O="--ignored --nocapture --test-threads=1"
L=/home/claude/logs
RC9_GET_MS=40 RC9_KEYS=1000000 RC9_ONLY_ON=1 $T warm::tests::rev_c9::variants -- $O > $L/w8-var-1gb-on.log 2>&1
RC9_GET_MS=20 RC9_KEYS=400000 $T warm::tests::rev_c9::variants -- $O > $L/w9-var-400k-20.log 2>&1
RC9_GET_MS=20 RC9_DBS=2 RC9_KEYS=400000 $T warm::tests::rev_c9::many_dbs_at_once -- $O > $L/w9-many-2-400k.log 2>&1
RC9_GET_MS=40 RC9_L0MAX=8 RC9_FLUSH_MS=250 $T warm::tests::rev_c9::writes_during_slow_warm -- $O > $L/w10-switch-l0max8.log 2>&1
echo DONE > $L/warm2-done
