#!/usr/bin/env bash
# Step 5b, after the customer filter (commit 8c2fdd6): SlateDB's side in real time with both filters, with the reader as
# built and with the one-wave reader. BIN: the fx_cold binary of that commit.
set -u
R=research/c2-step5b-reader/results
BIN=${BIN:-./target/release/fx_cold}
one() { # name env D profile r1 r3
  local name=$1 envs=$2 d=$3; shift 3
  [ -f "$R/$name.done" ] && return
  env $envs $BIN "$@" "$R/$name.jsonl" "$d" > "$R/$name.log" 2>&1
  local rc=$?
  echo "$name exit $rc" >> "$R/status.txt"
  [ $rc = 0 ] && touch "$R/$name.done"
}
one 1tbb-realtime-2mbs-cust "FX_REALTIME_MBS=2 FX_NO_SETTLE=1" 256 1tb-busy on on
one 1tbb-realtime-2mbs-cust-wave "FX_REALTIME_MBS=2 FX_NO_SETTLE=1 FX_WAVE=1" 256 1tb-busy on on
one 1tbm-realtime-0.4mbs-cust "FX_REALTIME_MBS=0.4 FX_NO_SETTLE=1" 128 1tb-medium on on
one 1tbm-realtime-0.4mbs-cust-wave "FX_REALTIME_MBS=0.4 FX_NO_SETTLE=1 FX_WAVE=1" 128 1tb-medium on on
echo REALTIME2 DONE >> "$R/status.txt"
