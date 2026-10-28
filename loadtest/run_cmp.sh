#!/usr/bin/env bash
# Interleaved WAL-cleanup cost, merged binary against engine-cost binary, same machine and session:
#   run_cmp.sh <merged-bin> <engine-cost-bin> [rounds=3] [seconds=300]
# Order per round: merged off, merged on, engine-cost off, engine-cost on (off = --wal-gc-secs 0).
# Resumable: a run whose results/objects/cmp2-<v>-<off|on><i>.json exists is skipped, so after a machine restart
# the same command continues (and starts MinIO again if it is not listening).
set -u
M=${1:?merged binary}; E=${2:?engine-cost binary}; ROUNDS=${3:-3}; SECS=${4:-300}
cd "$(dirname "$0")"
PY=${PY:-../mongo-tests/.venv/bin/python}
export MINIO_DATA_DIR=${MINIO_DATA_DIR:-/tmp/minio-data}
if ! (exec 3<>/dev/tcp/127.0.0.1/9000) 2>/dev/null; then
  mkdir -p "$MINIO_DATA_DIR/bench"
  MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin nohup "${MINIO_BIN:-/tmp/gopath/bin/minio}" server "$MINIO_DATA_DIR" --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 > /tmp/claude-0/minio.log 2>&1 &
  sleep 8
fi
for i in $(seq 1 "$ROUNDS"); do
  for v in m e; do
    if [ "$v" = m ]; then B=$M; else B=$E; fi
    for s in off on; do
      label=cmp2-$v-$s$i
      [ -f "results/objects/$label.json" ] && continue
      if [ "$s" = off ]; then
        $PY exp_objects.py "$label" "$B" "$SECS" -- --wal-gc-secs 0 > "/tmp/claude-0/$label.log" 2>&1
      else
        $PY exp_objects.py "$label" "$B" "$SECS" > "/tmp/claude-0/$label.log" 2>&1
      fi
    done
  done
done
touch /tmp/claude-0/cmp2_done
