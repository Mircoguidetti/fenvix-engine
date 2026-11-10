#!/usr/bin/env bash
# Measure again a dataset loaded by run_cold.sh, with other server flags (same bucket, same state file).
# Usage: bench/compaction/measure_only.sh BINARY LOADED_OUTDIR LABEL "EXTRA SERVER FLAGS"
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd); cd "$ROOT"
BIN=$(readlink -f "$1"); SRC=$(readlink -f "$2"); LABEL=$3; EXTRA=${4:-}
JH=${JUDGE_HOME:-$HOME/.fenvix-judge}; PY="$JH/env/bin/python"
. tools/minio-env.sh
PORT=${PORT:-27043}; BUCKET=cold-$(basename "$SRC" | tr '_.' '--' | tr 'A-Z' 'a-z')
URI="mongodb://127.0.0.1:$PORT/?directConnection=true"
OUT="$SRC/$LABEL"; mkdir -p "$OUT"
# shellcheck disable=SC2086
"$BIN" --listen 127.0.0.1:$PORT --idle-secs 100000 --holder cold-$PORT --bucket "$BUCKET" --no-ttl $EXTRA >> "$OUT/server.log" 2>&1 &
PID=$!
for _ in $(seq 1 100); do (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && break; sleep 0.2; done
"$PY" bench/compaction/cold_customer.py measure --uri "$URI" --state "$SRC/state.json" --n "${N:-200}" --warmup "${WARMUP:-300}" --seed "${SEED:-11}" --out "$OUT/result.json" | tee "$OUT/measure.txt"
kill -TERM $PID; while kill -0 $PID 2>/dev/null; do sleep 0.5; done
