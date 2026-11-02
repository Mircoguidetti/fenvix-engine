#!/usr/bin/env bash
# run_ctl.sh OUTDIR: the whole ctl-tests suite on target/release/mongo-server, one test file at a time, against a local
# MinIO on 127.0.0.1:9000 (no cloud). Prints one summary line per file; full outputs in OUTDIR/ctl_<file>.txt.
# Needs: MinIO running (MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin minio server DIR --address 127.0.0.1:9000)
# and a Python with pymongo, boto3, pytest, requests, cryptography ($PY, default /tmp/claude-0/venv/bin/python).
cd "$(dirname "$0")/../.."
OUT=${1:?outdir}; mkdir -p "$OUT"; OUT=$(cd "$OUT" && pwd)
PY=${PY:-/tmp/claude-0/venv/bin/python}
for f in ctl-tests/test_*.py; do
  n=$(basename "$f" .py); o=$OUT/ctl_$n.txt
  if [ -f "$o" ] && grep -Eq "[0-9]+ (passed|failed|error|skipped).* in [0-9.]+s" "$o"; then tail -1 "$o" | sed "s/^/$n: /"; continue; fi
  (cd ctl-tests && timeout 1500 "$PY" -m pytest -q -p no:cacheprovider --tb=short "$n.py" > "$o" 2>&1)
  tail -1 "$o" | sed "s/^/$n: /"
done
