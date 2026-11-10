#!/usr/bin/env bash
# Compaction 2 step 5b: step 5's cold-read runs again (run_all, run_extra, run_realtime) on the step 4 fix round with the
# reader skipping SlateDB SSTs at or below X_d, default fold (cap 6, fold at 6). Run from the repository root after
# `cargo build --release --bin fx_cold`. Two lanes: the 1 TB runs one after another (memory), the rest beside them.
# A finished run leaves a .done file and is skipped. FX_FOLD_CAP=3 for the fallback (results in results-cap3/).
set -u
R=research/c2-step5b-reader/${RESULTS:-results}
mkdir -p "$R"
one() { # name env D profile r1 r3
  local name=$1 envs=$2 d=$3; shift 3
  [ -f "$R/$name.done" ] && return
  env $envs ${CAP:+FX_FOLD_CAP=$CAP} ./target/release/fx_cold "$@" "$R/$name.jsonl" "$d" > "$R/$name.log" 2>&1
  local rc=$?
  echo "$name exit $rc" >> "$R/status.txt"
  [ $rc = 0 ] && touch "$R/$name.done"
}
lane_a() {
  one 1tbm-r1on-r3on X=1 512 1tb-medium on on
  one 1tbb-r1on-r3on X=1 256 1tb-busy on on
  one 1tbm-r1on-r3on-wave FX_WAVE=1 512 1tb-medium on on
  one 1tbb-r1on-r3on-wave FX_WAVE=1 256 1tb-busy on on
  one 1tbm-r1off-r3on X=1 512 1tb-medium off on
  one 1tbm-r1on-r3off X=1 512 1tb-medium on off
  one 1tbb-r1off-r3on X=1 256 1tb-busy off on
  one 1tbb-r1on-r3off X=1 256 1tb-busy on off
  one 1tbm-r1off-r3off X=1 512 1tb-medium off off
  one 1tbm-r1on-r3on-unsettled FX_NO_SETTLE=1 512 1tb-medium on on
  one 1tbm-r1on-r3on-unsettled-noskip "FX_NO_SETTLE=1 FX_NO_SKIP=1" 512 1tb-medium on on
}
lane_b() {
  one 1tbb-realtime-2mbs "FX_REALTIME_MBS=2 FX_NO_SETTLE=1" 256 1tb-busy on on
  one 1tbb-realtime-2mbs-noskip "FX_REALTIME_MBS=2 FX_NO_SETTLE=1 FX_NO_SKIP=1" 256 1tb-busy on on
  one 1tbm-realtime-0.4mbs "FX_REALTIME_MBS=0.4 FX_NO_SETTLE=1" 128 1tb-medium on on
  one 1tbm-realtime-0.4mbs-noskip "FX_REALTIME_MBS=0.4 FX_NO_SETTLE=1 FX_NO_SKIP=1" 128 1tb-medium on on
  one 10tbm-r1on-r3on X=1 512 10tb-medium on on
  one 10tbm-r1on-r3on-wave FX_WAVE=1 512 10tb-medium on on
  one 10tbm-r1off-r3on X=1 512 10tb-medium off on
  one 10tbm-r1on-r3off X=1 512 10tb-medium on off
  one 10tbm-r1off-r3off X=1 512 10tb-medium off off
}
lane_a & lane_b & wait
echo ALL DONE >> "$R/status.txt"
