#!/usr/bin/env bash
# Review of step 5b: the headline cold-read runs again, on the reviewed head (bf2f852, both filters on), plus a second
# seed for the main ones. Run from the repository root after `cargo build --release --bin fx_cold`.
# LANE=settled: time-compressed runs; LANE=realtime: SlateDB's side in real time (run alone: CPU contention changes L0).
set -u
R=research/review-c2-step5b/results
mkdir -p "$R"
one() { # name env D profile r1 r3 [seed]
  local name=$1 envs=$2 d=$3 p=$4 r1=$5 r3=$6 seed=${7:-}
  [ -f "$R/$name.done" ] && return
  env $envs ./target/release/fx_cold $p $r1 $r3 "$R/$name.jsonl" $d $seed > "$R/$name.log" 2>&1
  local rc=$?
  echo "$name exit $rc" >> "$R/status.txt"
  [ $rc = 0 ] && touch "$R/$name.done"
}
if [ "${LANE:-settled}" = settled ]; then
  one 1tbm-r1on-r3on X=1 512 1tb-medium on on
  one 1tbb-r1on-r3on X=1 256 1tb-busy on on
  one 10tbm-r1on-r3on X=1 512 10tb-medium on on
  one 1tbm-r1on-r3on-wave FX_WAVE=1 512 1tb-medium on on
  one 1tbb-r1on-r3on-wave FX_WAVE=1 256 1tb-busy on on
  one 1tbm-r1on-r3on-wave-seed7 FX_WAVE=1 512 1tb-medium on on 7
  one 1tbb-r1on-r3on-wave-seed7 FX_WAVE=1 256 1tb-busy on on 7
  one 1tbm-r1on-r3on-seed7 X=1 512 1tb-medium on on 7
else
  one 1tbb-realtime-2mbs-noskip "FX_REALTIME_MBS=2 FX_NO_SETTLE=1 FX_NO_SKIP=1 FX_NO_CUST=1" 256 1tb-busy on on
  one 1tbb-realtime-2mbs-cust "FX_REALTIME_MBS=2 FX_NO_SETTLE=1" 256 1tb-busy on on
  one 1tbm-realtime-0.4mbs-noskip "FX_REALTIME_MBS=0.4 FX_NO_SETTLE=1 FX_NO_SKIP=1 FX_NO_CUST=1" 128 1tb-medium on on
  one 1tbm-realtime-0.4mbs-cust "FX_REALTIME_MBS=0.4 FX_NO_SETTLE=1" 128 1tb-medium on on
  one 1tbb-realtime-2mbs-cust-wave "FX_REALTIME_MBS=2 FX_NO_SETTLE=1 FX_WAVE=1" 256 1tb-busy on on
  one 1tbb-realtime-2mbs-cust-seed7 "FX_REALTIME_MBS=2 FX_NO_SETTLE=1" 256 1tb-busy on on 7
  one 1tbb-realtime-2mbs-noskip-seed7 "FX_REALTIME_MBS=2 FX_NO_SETTLE=1 FX_NO_SKIP=1 FX_NO_CUST=1" 256 1tb-busy on on 7
fi
echo "LANE ${LANE:-settled} DONE" >> "$R/status.txt"
