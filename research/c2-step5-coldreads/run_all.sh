#!/usr/bin/env bash
# Compaction 2 step 5: every cold-read configuration. Run from the repository root after
# `cargo build --release --bin fx_cold`. D = 512 MiB (FX_D to change). Two lanes: the 1 TB runs one after another (a
# 1 TB busy run holds up to about 10 GB in the in-memory store), the 10 TB runs (about 1.5 GB) beside them.
set -u
D=${FX_D:-512}
R=research/c2-step5-coldreads/results
mkdir -p "$R"
one() { # name env profile r1 r3
  local name=$1 envs=$2; shift 2
  [ -f "$R/$name.done" ] && return
  env $envs ./target/release/fx_cold "$@" "$R/$name.jsonl" "$D" > "$R/$name.log" 2>&1
  local rc=$?
  echo "$name exit $rc" >> "$R/status.txt"
  [ $rc = 0 ] && touch "$R/$name.done"
}
lane_a() {
  one 1tbm-r1on-r3on X=1 1tb-medium on on
  one 1tbb-r1on-r3on X=1 1tb-busy on on
  one 1tbm-r1off-r3on X=1 1tb-medium off on
  one 1tbm-r1on-r3off X=1 1tb-medium on off
  one 1tbb-r1off-r3on X=1 1tb-busy off on
  one 1tbb-r1on-r3off X=1 1tb-busy on off
  one 1tbm-r1on-r3on-wave FX_WAVE=1 1tb-medium on on
  one 1tbb-r1on-r3on-wave FX_WAVE=1 1tb-busy on on
  one 1tbm-r1on-r3on-unsettled FX_NO_SETTLE=1 1tb-medium on on
}
lane_b() {
  one 10tbm-r1on-r3on X=1 10tb-medium on on
  one 10tbm-r1off-r3on X=1 10tb-medium off on
  one 10tbm-r1on-r3off X=1 10tb-medium on off
  one 10tbm-r1on-r3on-wave FX_WAVE=1 10tb-medium on on
}
lane_a & lane_b & wait
echo ALL DONE >> "$R/status.txt"
