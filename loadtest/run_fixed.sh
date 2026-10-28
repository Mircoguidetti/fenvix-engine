#!/usr/bin/env bash
# WAL-cleanup cost at a fixed offered load, merged binary against engine-cost binary, off/on pairs adjacent in time:
#   run_fixed.sh <merged-bin> <engine-cost-bin> [pairs=4] [seconds=300] [rate=300]
# Order per pair k: merged off, merged on, engine-cost off, engine-cost on (off = --wal-gc-secs 0). Resumable after a
# machine restart: finished runs (results/objects/fx-*.json) are skipped; an "on" run whose "off" is older than 15
# minutes (a restart came in between) first reruns the "off", and the old file is kept in results/objects/stale/.
# Safety: tenants left in MinIO by runs that a restart killed are removed before each run (they filled the disk once);
# a run is valid only with ops > 0 and no errors; an invalid run is moved to results/objects/invalid/ and the script stops.
set -u
M=${1:?merged binary}; E=${2:?engine-cost binary}; PAIRS=${3:-4}; SECS=${4:-300}; RATE=${5:-300}
cd "$(dirname "$0")"
PY=${PY:-../mongo-tests/.venv/bin/python}
export MINIO_DATA_DIR=${MINIO_DATA_DIR:-/tmp/minio-data}
if ! (exec 3<>/dev/tcp/127.0.0.1/9000) 2>/dev/null; then
  mkdir -p "$MINIO_DATA_DIR/bench"
  MINIO_PROMETHEUS_AUTH_TYPE=public MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin nohup "${MINIO_BIN:-/tmp/gopath/bin/minio}" server "$MINIO_DATA_DIR" --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 > /tmp/claude-0/minio.log 2>&1 &
  sleep 8
fi
mkdir -p results/objects/stale results/objects/invalid
clean_orphans() {  # only benchmark tenants (ob*) of runs that are not running now
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
run() {  # label binary off|on
  clean_orphans
  free_mb=$(df -m /tmp | awk 'NR==2{print $4}')
  if [ "$free_mb" -lt 3000 ]; then echo "$(date -u +%FT%TZ) only ${free_mb} MB free, not starting $1" >> results/objects/fixed-restarts.log; return 1; fi
  if [ "$3" = off ]; then
    $PY exp_objects.py "$1" "$2" "$SECS" --rate "$RATE" -- --wal-gc-secs 0 > "/tmp/claude-0/$1.log" 2>&1
  else
    $PY exp_objects.py "$1" "$2" "$SECS" --rate "$RATE" > "/tmp/claude-0/$1.log" 2>&1
  fi
  if [ -f "results/objects/$1.json" ] && ! $PY -c "import json,sys; d=json.load(open('results/objects/$1.json')); sys.exit(0 if d['ops']>0 and d['errors']==0 else 1)"; then
    mv "results/objects/$1.json" "results/objects/invalid/$1.json"
    echo "$(date -u +%FT%TZ) $1 invalid (no operations or errors), moved to invalid/" >> results/objects/fixed-restarts.log
    return 1
  fi
}
for k in $(seq 1 "$PAIRS"); do
  for v in m e; do
    if [ "$v" = m ]; then B=$M; else B=$E; fi
    off=fx-$v-off$k; on=fx-$v-on$k
    [ -f "results/objects/$on.json" ] && continue
    if [ -f "results/objects/$off.json" ]; then
      age=$(( $(date +%s) - $(stat -c %Y "results/objects/$off.json") ))
      if [ "$age" -gt 900 ]; then mv "results/objects/$off.json" "results/objects/stale/$off.json"; fi
    fi
    [ -f "results/objects/$off.json" ] || run "$off" "$B" off || exit 1
    run "$on" "$B" on || exit 1
  done
done
touch /tmp/claude-0/fixed2_done
