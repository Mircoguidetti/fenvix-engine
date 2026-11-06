#!/usr/bin/env bash
# Fence-layout test, the baseline: start mongo-server on a bucket loaded by bench/compaction/run_cold.sh (today's layout
# on branch compaction: customer key on, lz4, 64 KiB blocks, tiered compaction) and read the model's 400 users.
# Usage: bench/fence/run_baseline.sh BINARY BUCKET MODEL OUTDIR LABEL "EXTRA SERVER FLAGS" [passes]
#   FENVIX_SIM_S3 (for example 5,0,1000000) is passed through to the server: simulated GET time, measurement only.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd); cd "$ROOT"
BIN=$(readlink -f "$1"); BUCKET=$2; MODEL=$(readlink -f "$3"); OUT=$(mkdir -p "$4/$5" && readlink -f "$4/$5"); EXTRA=${6:-}; PASSES=${7:-cold,hot}
JH=${JUDGE_HOME:-$HOME/.fenvix-judge}; PY="$JH/env/bin/python"
. tools/minio-env.sh
PORT=${PORT:-27045}; URI="mongodb://127.0.0.1:$PORT/?directConnection=true"
# shellcheck disable=SC2086
"$BIN" --listen 127.0.0.1:$PORT --idle-secs 100000 --holder fence-$PORT --bucket "$BUCKET" --no-ttl $EXTRA >> "$OUT/server.log" 2>&1 &
PID=$!
for _ in $(seq 1 100); do (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && break; sleep 0.2; done
"$PY" bench/fence/server_reads.py --uri "$URI" --model "$MODEL" --out "$OUT/result.json" --passes "$PASSES" | tee "$OUT/measure.txt"
kill -TERM $PID; while kill -0 $PID 2>/dev/null; do sleep 0.5; done
