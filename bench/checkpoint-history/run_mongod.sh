#!/usr/bin/env bash
# The baseline: the reference mongod of judge/setup.sh (7.0), standalone, WiredTiger cache 1 GB. Load the same history
# (ckpt.py load: same documents byte for byte), restart mongod and drop the OS page cache so reads are cold, measure.
# Usage: bench/checkpoint-history/run_mongod.sh OUTDIR THREADS LONG [load|measure|both]
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd); cd "$ROOT"
OUT=$(mkdir -p "$1" && readlink -f "$1"); THREADS=$2; LONG=$3; WHAT=${4:-both}
JH=${JUDGE_HOME:-$HOME/.fenvix-judge}; PY=${PY:-/home/user/adapters/venv-lg/bin/python}
PORT=${PORT:-27030}; DBP=${DBPATH:-$JH/data/ckpt-mongod}
URI="mongodb://127.0.0.1:$PORT/?directConnection=true"
start() {
  mkdir -p "$DBP"
  "$JH/env/bin/mongod" --port "$PORT" --bind_ip 127.0.0.1 --dbpath "$DBP" --wiredTigerCacheSizeGB 1 --fork --logpath "$OUT/mongod.log" > /dev/null
}
stop() { "$JH/env/bin/mongod" --dbpath "$DBP" --shutdown > /dev/null || true; }
if [ "$WHAT" != measure ]; then
  stop; rm -rf "$DBP"; start
  "$PY" bench/checkpoint-history/ckpt.py load --uri "$URI" --threads "$THREADS" --long "$LONG" --plan "$OUT/plan.json" --workers "${WORKERS:-8}" | tee "$OUT/load.txt"
  "$PY" -c "
from pymongo import MongoClient; d = MongoClient('$URI').checkpointing_db
for n in ('checkpoints', 'checkpoint_writes'):
    s = d.command('collStats', n); print(n, 'count', s['count'], 'size', s['size'], 'storageSize', s['storageSize'], 'indexSize', s['totalIndexSize'])" | tee -a "$OUT/load.txt"
  du -sb "$DBP" | tee -a "$OUT/load.txt"
fi
if [ "$WHAT" != load ]; then
  stop; sync; echo 3 > /proc/sys/vm/drop_caches || echo "could not drop the page cache"; start
  "$PY" bench/checkpoint-history/ckpt.py measure --uri "$URI" --plan "$OUT/plan.json" --out "$OUT/result.json" ${MEASURE_ARGS:-} | tee "$OUT/measure.txt"
fi
