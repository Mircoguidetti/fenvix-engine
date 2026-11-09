#!/usr/bin/env bash
# Compaction 2 step 5: every cold-read configuration, two at a time. Run from the repository root after
# `cargo build --release --bin fx_cold`. D = 512 MiB (FX_D to change). Results in research/c2-step5-coldreads/results/.
set -u
D=${FX_D:-512}
R=research/c2-step5-coldreads/results
mkdir -p "$R"
one() { # name env profile r1 r3
  local name=$1 envs=$2; shift 2
  env $envs ./target/release/fx_cold "$@" "$R/$name.jsonl" "$D" > "$R/$name.log" 2>&1
  echo "$name exit $?" >> "$R/status.txt"
}
jobs_list=(
  "1tbm-r1on-r3on|X=1|1tb-medium on on"
  "1tbb-r1on-r3on|X=1|1tb-busy on on"
  "10tbm-r1on-r3on|X=1|10tb-medium on on"
  "1tbm-r1off-r3on|X=1|1tb-medium off on"
  "1tbm-r1on-r3off|X=1|1tb-medium on off"
  "1tbb-r1off-r3on|X=1|1tb-busy off on"
  "1tbb-r1on-r3off|X=1|1tb-busy on off"
  "10tbm-r1off-r3on|X=1|10tb-medium off on"
  "10tbm-r1on-r3off|X=1|10tb-medium on off"
  "1tbm-r1on-r3on-wave|FX_WAVE=1|1tb-medium on on"
  "1tbb-r1on-r3on-wave|FX_WAVE=1|1tb-busy on on"
  "10tbm-r1on-r3on-wave|FX_WAVE=1|10tb-medium on on"
  "1tbm-r1on-r3on-unsettled|FX_NO_SETTLE=1|1tb-medium on on"
)
i=0
for j in "${jobs_list[@]}"; do
  IFS='|' read -r name envs args <<< "$j"
  one "$name" "$envs" $args &
  i=$((i+1))
  if (( i % 2 == 0 )); then wait -n; fi
  while (( $(jobs -rp | wc -l) >= 2 )); do wait -n; done
done
wait
echo ALL DONE >> "$R/status.txt"
