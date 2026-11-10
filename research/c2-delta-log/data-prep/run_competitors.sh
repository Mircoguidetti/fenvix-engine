#!/usr/bin/env bash
# Rerun the LangGraph savers (Postgres + Mongo, default and DeltaChannel mode) on every cell the two
# earlier sessions measured, with their own code, unchanged:
#   owner:    fenvix-engine origin/checkpoint-delta              research/checkpoint-delta/ (run_savers.py)
#   reviewer: fenvix-engine origin/claude/quirky-lamport-p2v27w  research/checkpoint-delta/review/
#             (real_run.py, edge_cases.py, convert_real.py)
# The code runs from a working copy in $C2/run so that the published results stay untouched in $C2/src.
# Needs: Postgres 16 on 127.0.0.1:5432 (postgres/pg, pglz TOAST), mongod 7.0 on 27017, the venv in $C2/.venv,
# the converted real sets in $C2/run/research/checkpoint-delta/review/real (see README.md).
set -euo pipefail
C2=${C2:-/home/user/c2data}
PY=$C2/.venv/bin/python
OWNER=$C2/run/research/checkpoint-delta
REV=$OWNER/review
LOG=$C2/logs
mkdir -p "$LOG"

scripted() {
  cd "$OWNER"
  for mode in default delta; do
    for cell in "10 400" "40 150" "100 40" "300 8"; do
      set -- $cell
      echo "== scripted $mode $1 x $2"
      $PY run_savers.py "$mode" "$1" "$2"
    done
  done
}

real() {
  cd "$REV"
  for ds in tau_gpt_4o_airline swe35 swe4; do
    echo "== real $ds"
    $PY real_run.py "$ds" --no-sqlite
  done
  echo "== real tau_sonnet_35_new_retail (300 sampled, seed 20261109)"
  $PY real_run.py tau_sonnet_35_new_retail --sample 300 --no-sqlite
}

edge() {
  cd "$REV"
  # thread counts as the reviewer ran them (results/edge/40.json: 3 threads, 100.json: 8 threads)
  echo "== edge 40 x 3"; $PY edge_cases.py 40 3
  echo "== edge 100 x 8"; $PY edge_cases.py 100 8
}

case "${1:-all}" in
  scripted) scripted 2>&1 | tee "$LOG/scripted.log" ;;
  real) real 2>&1 | tee "$LOG/real.log" ;;
  edge) edge 2>&1 | tee "$LOG/edge.log" ;;
  all) scripted 2>&1 | tee "$LOG/scripted.log"; real 2>&1 | tee "$LOG/real.log"; edge 2>&1 | tee "$LOG/edge.log" ;;
esac
