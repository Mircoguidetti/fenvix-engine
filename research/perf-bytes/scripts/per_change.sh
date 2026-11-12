#!/bin/bash
# P-perf-bytes: medium profile, one change at a time on the new binary (cumulative), byte accounting at settle end.
set -u
export BENCH_PW=localbenchpw
H=/home/user/bench-h/bench/agent-checkpoints
FXB=/home/user/fenvix-engine/target/release/fx_bytes
R=/tmp/claude-0/runs
declare -a NAMES=(m1-gcfix m2-drop m3-retention0 m4-sstlz4 m5-zstd)
declare -a FLAGS=(
  "--sealed-drop false --sealed-version-retention-secs 604800 --sealed-codec lz4 --sst-compression none"
  "--sealed-version-retention-secs 604800 --sealed-codec lz4 --sst-compression none"
  "--sealed-codec lz4 --sst-compression none"
  "--sealed-codec lz4"
  ""
)
ONLY=${ONLY:-}
for i in "${!NAMES[@]}"; do
  n=${NAMES[$i]}
  [ -n "$ONLY" ] && [[ " $ONLY " != *" $n "* ]] && continue
  out=$R/$n; rm -rf "$out"
  echo "$(date -u +%T) $n: flags [${FLAGS[$i]}]"
  ( until grep -q '"phase": "erase"' "$out/ops.jsonl" 2>/dev/null; do sleep 10; done
    AWS_ACCESS_KEY_ID=minio-local AWS_SECRET_ACCESS_KEY=minio-local-secret timeout 1500 "$FXB" --endpoint http://172.31.77.10:9000 \
      --bucket fxbench --db app_demo --slate > "$R/$n-bytes.json" 2> "$R/$n-bytes.err" ) &
  P=$!
  (cd "$H" && FXBENCH_EXTRA_FLAGS="${FLAGS[$i]}" ~/.fenvix-agentbench-venv/bin/python lines.py run fenvix-log --out "$out" --profile medium \
      --read-sample 100 --erase-users 3 --cold-threads 50 --min-free-gib 3 > "$R/$n.log" 2>&1)
  echo "$(date -u +%T) $n: lines.py exit $?"
  wait $P
done
echo "$(date -u +%T) all done"
