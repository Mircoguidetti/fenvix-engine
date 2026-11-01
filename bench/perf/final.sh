#!/usr/bin/env bash
# final.sh: the before/after comparison of docs/perf-letture.md. For each cell (workload x layout), 3 runs of 60 s
# warm-up + 60 s measured of: the engine of main (BASE), the engine of this branch (FINAL), mongod 8.0. Each server at
# its saturation point from the sweeps (bench.py sweep). Appends to $BENCH_RESULTS/raw.jsonl with labels
# final-base / final-new / final-mongod.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; BIN="${BENCH_BIN:-$HERE/.bin}"
BASE=${BASE:-$BIN/mongo-server-base}; FINAL=${FINAL:-$BIN/mongo-server-final}
B="python3 $HERE/bench.py run"; O="--warmup 60 --duration 60 --reps 3"
# cell: workload layout engine-clients mongod-clients (saturation points of the sweeps, see docs/perf-letture.md)
for cell in "read 1 8 8" "read 100 16 8" "rw 1 4 16" "rw 100 16 16"; do
  set -- $cell
  $B fenvix "$2" "$1" "$3" $O --label final-base --bin "$BASE"
  $B fenvix "$2" "$1" "$3" $O --label final-new --bin "$FINAL"
  $B mongod "$2" "$1" "$4" $O --label final-mongod
done
