#!/usr/bin/env bash
# Every cell of the saver measurement. Postgres 16 on 5432 and mongod 7.0 on 27017 must be running.
set -euo pipefail
cd "$(dirname "$0")"
PY=.venv/bin/python
for mode in default delta; do
  for cell in "10 400" "40 150" "100 40" "300 8"; do
    set -- $cell
    echo "== $mode $1 x $2"; $PY run_savers.py $mode $1 $2
  done
done
for cell in "10 400" "40 150" "100 40" "300 8"; do
  set -- $cell
  echo "== default $1 x $2 lz4 TOAST (Postgres only)"; $PY run_savers.py default $1 $2 --pg-toast lz4 --pg-only
done
