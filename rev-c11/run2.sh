#!/bin/sh
cd /home/claude/fenvix-engine
T="cargo test --release --lib"
O="--ignored --nocapture --test-threads=1"
L=/home/claude/logs11
$T warm::tests::rev_c11::token_bucket -- $O > $L/bucket.log 2>&1
RC9_ROUNDS=2 RC9_GET_MS=40 RC9_L0MAX=11 RC9_FLUSH_MS=600 RC9_FLUSH_GAP_MS=500 $T warm::tests::rev_c9::writes_during_slow_warm -- $O > $L/e1x-dopo-l0max11.log 2>&1
RC9_PRIMA=1 RC9_ROUNDS=2 RC9_GET_MS=40 RC9_L0MAX=11 RC9_FLUSH_MS=600 $T warm::tests::rev_c9::writes_during_slow_warm -- $O > $L/e1x-prima-l0max11.log 2>&1
$T -- --nocapture --test-threads=1 > $L/lib-all.log 2>&1
echo DONE > $L/run2-done
