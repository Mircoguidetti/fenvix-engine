#!/usr/bin/env bash
# Reproduces the write stall of docs/fix-stall.md: the bulk load of bench/mongod (1 database of 200 000 documents of
# ~1 KB, insertMany of 500 documents, 16 in parallel, {w:1, j:true}) against a fresh engine, RUNS times.
#
#   tools/stall/repro.sh STORE CORES RUNS [OUTDIR]
#     STORE  local | minio   (`--store local --local-dir ...`, or MinIO at 127.0.0.1:9000 with a fresh bucket per run)
#     CORES  1 | 2           (engine pinned to CPU 0 or 0-1 with taskset, load generator on the remaining CPUs)
#
# Environment: ENGINE_BIN (the mongo-server binary), LOADGEN_BIN (bench/mongod/loadgen built with `go build`),
# PY (a python with boto3, for the MinIO buckets), DOCS (documents loaded, default 200000), STALL_SECS (a load that has not finished after this many seconds is
# a stall; default 600: an unstalled load takes 30-90 s), ENGINE_ARGS (extra engine options), PORT (default 27017).
#
# One line per run on stdout and in OUTDIR/runs.txt: "run N ok SECONDS" or "run N STALL". On a stall the evidence is
# collected before the engine is killed: OUTDIR/run-N/{server.log, gdb.txt, cpu.txt, status.json (tenantStats, serverStatus)}, and, with a
# diagnosis build and DIAG=1 (docs/fix-stall.md, SIGUSR1 handler), the tokio task dump at the end of server.log.
set -uo pipefail
STORE=$1; CORES=$2; RUNS=$3; OUT=${4:-$PWD/stall-out}
ENGINE_BIN=${ENGINE_BIN:?set ENGINE_BIN}; LOADGEN_BIN=${LOADGEN_BIN:?set LOADGEN_BIN}
PY=${PY:-python3}; DOCS=${DOCS:-200000}; STALL_SECS=${STALL_SECS:-600}; PORT=${PORT:-27017}; ENGINE_ARGS=${ENGINE_ARGS:-}
DATA=${DATA:-$OUT/data}
if [ "$CORES" = 1 ]; then DB_CPUS=0; GEN_CPUS=1-3; else DB_CPUS=0-1; GEN_CPUS=2-3; fi
mkdir -p "$OUT"
log() { echo "$*" | tee -a "$OUT/runs.txt"; }

stop_engine() {
  local pid=$1
  kill -TERM "$pid" 2>/dev/null
  for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || return 0; sleep 1; done
  echo "engine $pid did not stop in 30 s after SIGTERM: SIGKILL" >>"$OUT/runs.txt"
  kill -KILL "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
}

cpu_ticks() { awk '{print $14+$15}' "/proc/$1/stat" 2>/dev/null; }

# The data of a run is deleted once the engine has stopped (a MinIO bucket per run, ~0.3 GB each, filled the disk after
# ~60 runs: MinIO then refuses every PUT with XMinioStorageFull and the loads fail in a way that looks like a stall).
drop_data() {
  rm -rf "$DATA"
  [ "$STORE" = minio ] && "$PY" - "$BUCKET" <<'PYEOF'
import sys, boto3
b = boto3.resource("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1").Bucket(sys.argv[1])
b.objects.all().delete(); b.delete()
PYEOF
}

for n in $(seq 1 "$RUNS"); do
  free_kb=$(df -Pk "$OUT" | awk 'NR==2{print $4}')
  [ "$free_kb" -lt 3000000 ] && { log "run $n not started: less than 3 GB free on the disk"; exit 1; }
  RD="$OUT/run-$n"; rm -rf "$RD" "$DATA"; mkdir -p "$RD" "$DATA"
  case "$STORE" in
    local) STORE_ARGS=(--store local --local-dir "$DATA/store") ;;
    minio)
      # shellcheck disable=SC1091
      source "$(dirname "$0")/../minio-env.sh"
      BUCKET="stall-$(date +%s)-$n"
      "$PY" -c "import boto3,sys; boto3.client('s3', endpoint_url='http://127.0.0.1:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin', region_name='us-east-1').create_bucket(Bucket=sys.argv[1])" "$BUCKET" || exit 1
      STORE_ARGS=(--store s3 --bucket "$BUCKET" --endpoint http://127.0.0.1:9000) ;;
    *) echo "STORE must be local or minio" >&2; exit 2 ;;
  esac
  # shellcheck disable=SC2086
  taskset -c "$DB_CPUS" "$ENGINE_BIN" --listen "127.0.0.1:$PORT" "${STORE_ARGS[@]}" $ENGINE_ARGS >"$RD/server.log" 2>&1 &
  SPID=$!
  for _ in $(seq 1 120); do grep -q "listening on" "$RD/server.log" && break; sleep 0.5; done
  grep -q "listening on" "$RD/server.log" || { log "run $n engine did not start"; cat "$RD/server.log" >&2; exit 1; }
  t0=$(date +%s.%N)
  timeout "$STALL_SECS" taskset -c "$GEN_CPUS" "$LOADGEN_BIN" -uri "mongodb://127.0.0.1:$PORT/?directConnection=true" \
    -mode load -dbs 1 -docs "$DOCS" -clients 16 >"$RD/load.out" 2>"$RD/load.err"
  rc=$?
  t1=$(date +%s.%N)
  if [ $rc -eq 0 ]; then
    # Compaction passes and L0-watch requests of the run (tenantStats; l0WatchRequests is absent before the fix).
    maint=$("$PY" - "$PORT" 2>/dev/null <<'PYEOF'
import sys
from pymongo import MongoClient
m = MongoClient(f"mongodb://127.0.0.1:{sys.argv[1]}/?directConnection=true", serverSelectionTimeoutMS=5000).admin.command("tenantStats")["maintenance"]
print(f"compactRuns={m.get('compactRuns')} l0WatchRequests={m.get('l0WatchRequests')}")
PYEOF
)
    # Slowest insert the engine logged (SLOW lines): a write pause shorter than the client timeout does not fail the load.
    slow=$(grep -o "SLOW insert db=[^ ]* ms=[0-9]*" "$RD/server.log" | sed 's/.*ms=//' | sort -n | tail -1)
    log "run $n ok $(echo "$t1 - $t0" | bc) $maint slowestInsertMs=${slow:-none}"
    stop_engine $SPID
    drop_data
    grep -q "final flush did not finish" "$RD/server.log" && log "run $n: the close at SIGTERM gave up its final flush (unclean)"
    rm -rf "$RD/server.log.gz"; gzip -f "$RD/server.log"
    continue
  fi
  log "run $n STALL (loadgen exit $rc after $(echo "$t1 - $t0" | bc) s: $(tail -c 200 "$RD/load.err" | tr '\n' ' '))"
  # A load that failed on errors of the store (e.g. a full disk) is not a stall: the commands come back with ok=false.
  errs=$(grep -c "ok=false" "$RD/server.log"); [ "$errs" -gt 0 ] && log "run $n: $errs commands failed with an error (ok=false in server.log): not a stall, see server.log"
  # Evidence, engine still running.
  c0=$(cpu_ticks $SPID); sleep 10; c1=$(cpu_ticks $SPID)
  echo "engine CPU ticks (1/100 s) over 10 s while stalled: $((c1 - c0))" >"$RD/cpu.txt"
  "$PY" - "$PORT" >"$RD/status.json" 2>&1 <<'PYEOF'
import sys, json
from pymongo import MongoClient
c = MongoClient(f"mongodb://127.0.0.1:{sys.argv[1]}/?directConnection=true", serverSelectionTimeoutMS=5000, socketTimeoutMS=10000)
for cmd in ("tenantStats", "serverStatus"):
    print(cmd, json.dumps(c.admin.command(cmd), default=str, indent=1))
PYEOF
  gdb -p $SPID -batch -ex "set pagination off" -ex "thread apply all bt" >"$RD/gdb.txt" 2>&1
  # Only a diagnosis build handles SIGUSR1 (state and tokio task dump); for any other build it would kill the engine.
  [ -n "${DIAG:-}" ] && { kill -USR1 $SPID 2>/dev/null; sleep 15; }
  stop_engine $SPID
  drop_data
done
