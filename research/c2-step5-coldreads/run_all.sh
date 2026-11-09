#!/usr/bin/env bash
# Compaction 2 step 5: every cold-read configuration, one at a time. Run from the repository root after
# `cargo build --release --bin fx_cold`. D = 512 MiB for the medium profiles and 256 MiB for the busy one: with R3 on, a
# 1 TB busy run at 512 MiB holds more than the sandbox's 16 GB in the in-memory store (killed twice, MEASURED).
# A finished run leaves a .done file and is skipped.
set -u
R=research/c2-step5-coldreads/results
mkdir -p "$R"
one() { # name env D profile r1 r3
  local name=$1 envs=$2 d=$3; shift 3
  [ -f "$R/$name.done" ] && return
  env $envs ./target/release/fx_cold "$@" "$R/$name.jsonl" "$d" > "$R/$name.log" 2>&1
  local rc=$?
  echo "$name exit $rc" >> "$R/status.txt"
  [ $rc = 0 ] && touch "$R/$name.done"
}
one 1tbm-r1on-r3on-no-range-filter FX_NO_RANGE_FILTER=1 512 1tb-medium on on
one 1tbm-r1on-r3on X=1 512 1tb-medium on on
one 10tbm-r1on-r3on X=1 512 10tb-medium on on
one 10tbm-r1off-r3on X=1 512 10tb-medium off on
one 10tbm-r1on-r3off X=1 512 10tb-medium on off
one 10tbm-r1on-r3on-wave FX_WAVE=1 512 10tb-medium on on
one 1tbm-r1off-r3on X=1 512 1tb-medium off on
one 1tbm-r1on-r3off X=1 512 1tb-medium on off
one 1tbb-r1on-r3on X=1 256 1tb-busy on on
one 1tbb-r1off-r3on X=1 256 1tb-busy off on
one 1tbb-r1on-r3off X=1 256 1tb-busy on off
one 1tbm-r1on-r3on-wave FX_WAVE=1 512 1tb-medium on on
one 1tbb-r1on-r3on-wave FX_WAVE=1 256 1tb-busy on on
one 1tbm-r1on-r3on-unsettled FX_NO_SETTLE=1 512 1tb-medium on on
echo ALL DONE >> "$R/status.txt"
