#!/usr/bin/env bash
# Step 5: SlateDB's share of a cold read without compressing time on SlateDB's side (production L0 size and lease, inserts
# throttled to the profile's rate, no settling). Our tiers stay time-compressed. One at a time.
set -u
R=research/c2-step5-coldreads/results
one() { # name env D profile r1 r3
  local name=$1 envs=$2 d=$3; shift 3
  [ -f "$R/$name.done" ] && return
  env $envs ./target/release/fx_cold "$@" "$R/$name.jsonl" "$d" > "$R/$name.log" 2>&1
  local rc=$?
  echo "$name exit $rc" >> "$R/status.txt"
  [ $rc = 0 ] && touch "$R/$name.done"
}
one 1tbb-realtime-2mbs "FX_REALTIME_MBS=2 FX_NO_SETTLE=1" 256 1tb-busy on on
one 1tbm-realtime-0.4mbs "FX_REALTIME_MBS=0.4 FX_NO_SETTLE=1" 128 1tb-medium on on
echo REALTIME DONE >> "$R/status.txt"
