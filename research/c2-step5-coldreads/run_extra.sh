#!/usr/bin/env bash
# Step 5 extras, one at a time: R1 and R3 both off (the plan as written), and one configuration on a local MinIO next to
# its in-memory twin (D = 256 MiB). MinIO must listen on 127.0.0.1:9000 (judge/setup.sh's minio, minioadmin) with an
# empty bucket "coldreads".
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
one 1tbm-r1off-r3off X=1 512 1tb-medium off off
one 10tbm-r1off-r3off X=1 512 10tb-medium off off
one 1tbm-d256-r1on-r3off-memory X=1 256 1tb-medium on off
one 1tbm-d256-r1on-r3off-minio FX_MINIO=coldreads 256 1tb-medium on off
echo EXTRA DONE >> "$R/status.txt"
