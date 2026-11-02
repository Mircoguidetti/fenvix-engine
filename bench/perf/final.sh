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
# CELLS and LABEL can be given; NO_BASE=1 skips the base binary.
CELLS=${CELLS:-"read:1:8:8 read:100:16:8 rw:1:4:16 rw:100:16:16"}
LABEL=${LABEL:-final}
for cell in $CELLS; do
  IFS=: read -r w layout ec mc <<< "$cell"
  [ -n "${NO_BASE:-}" ] || $B fenvix "$layout" "$w" "$ec" $O --label "$LABEL-base" --bin "$BASE"
  $B fenvix "$layout" "$w" "$ec" $O --label "$LABEL-new" --bin "$FINAL"
  $B mongod "$layout" "$w" "$mc" $O --label "$LABEL-mongod"
done
