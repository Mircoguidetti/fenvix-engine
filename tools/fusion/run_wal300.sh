#!/usr/bin/env bash
# WAL-cleanup cost at 300 ops/s on the fused binary only: 4 alternating pairs (off, on, off, on, ...), 300 s each.
# Resumable. Before each run: orphan benchmark tenants are removed from MinIO and free space is checked; a run with no
# operations or with errors is moved to results/objects/invalid-fz/ and the script stops. An "on" run whose "off" is
# older than 15 minutes (a restart came in between) first reruns the "off"; the old file goes to stale-fz/.
cd "$(dirname "$0")/../../loadtest"
BIN=/home/user/test5-compat-indexes/target/release/mongo-server
PY=../mongo-tests/.venv/bin/python
export MINIO_DATA_DIR=/tmp/minio-data
mkdir -p results/objects/invalid-fz results/objects/stale-fz
clean_orphans() {
  pgrep -f exp_objects.py > /dev/null && return
  $PY - <<'P'
import os, shutil
base = os.environ["MINIO_DATA_DIR"] + "/bench/tenants"
if os.path.isdir(base):
    for n in os.listdir(base):
        if n.startswith("ob") and os.path.isdir(os.path.join(base, n)):
            shutil.rmtree(os.path.join(base, n), ignore_errors=True)
P
}
run() {  # label off|on
  MINIO_ONLY=1 ../tools/fusion/env_up.sh > /dev/null || exit 1
  clean_orphans
  free_mb=$(df -m /tmp | awk 'NR==2{print $4}')
  if [ "$free_mb" -lt 3000 ]; then echo "only ${free_mb} MB free, not starting $1" >> results/objects/fusion-restarts.log; exit 3; fi
  if [ "$2" = off ]; then
    $PY exp_objects.py "$1" "$BIN" 300 --rate 300 -- --wal-gc-secs 0 > "/tmp/claude-0/$1.log" 2>&1
  else
    $PY exp_objects.py "$1" "$BIN" 300 --rate 300 > "/tmp/claude-0/$1.log" 2>&1
  fi
  if [ -f "results/objects/$1.json" ] && ! $PY -c "import json,sys; d=json.load(open('results/objects/$1.json')); sys.exit(0 if d['ops']>0 and d['errors']==0 else 1)"; then
    mv "results/objects/$1.json" "results/objects/invalid-fz/$1.json"
    echo "$(date -u +%FT%TZ) $1 invalid (no operations or errors), moved to invalid-fz/" >> results/objects/fusion-restarts.log
    exit 4
  fi
}
for k in 1 2 3 4; do
  off=fz-off$k; on=fz-on$k
  [ -f "results/objects/$on.json" ] && continue
  if [ -f "results/objects/$off.json" ]; then
    age=$(( $(date +%s) - $(stat -c %Y "results/objects/$off.json") ))
    [ "$age" -gt 2700 ] && mv "results/objects/$off.json" "results/objects/stale-fz/$off.json"
  fi
  [ -f "results/objects/$off.json" ] || run $off off
  run $on on
done
echo WAL300-DONE
