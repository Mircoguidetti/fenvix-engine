#!/bin/bash
# Step 9 · a log node: one copy of the 3-zone log (the "quaderno", proto/quaderno, binary `pq replica`), one per
# availability zone. The engine confirms a write when 2 of the 3 copies have it on disk (fsync), like a MongoDB
# replica set with w: majority across 3 zones.
# Flow: this node's address -> wait for log/peers.json from the runner (the three private IPs, written once all three
# machines exist) -> pq replica (peer port 7100, client port 7200) -> "ready" -> every PUSH_S logs, CPU and memory to
# the bucket -> on "stop": SIGTERM, final logs, "stopped", power off.
# A SECOND, separate log group runs beside it on ports 7101/7201 (own folder): only the raw log-ack benchmark
# (programs/log_ack.py, `pq commit`) writes there. Its records are not engine records; written into the kitchen's log
# they make every engine restart and takeover refuse to start ("a record of the log has a bad checksum": found by the
# local validation of 2026-11-09).
# Usage: log.sh NAME BUCKET   (NAME = log-0, log-1 or log-2; the number is the replica id)
set -u
cd "$(dirname "$0")" || exit 1
NAME=$1; BUCKET=$2; P=$NAME
ID=${NAME#log-}
source ./common.sh
DIR=${LOG_DIR:-/var/lib/fenvix-log}
mkdir -p "$DIR" "$DIR-bench"
finish() {
  step "finish: $1"
  [ -n "${BENCH_PID:-}" ] && kill -TERM "$BENCH_PID" 2>/dev/null
  if [ -n "${PQ_PID:-}" ]; then kill -TERM "$PQ_PID" 2>/dev/null; for _ in $(seq 1 20); do kill -0 "$PQ_PID" 2>/dev/null || break; sleep 1; done
    kill -KILL "$PQ_PID" 2>/dev/null; fi
  [ -n "${CPU_PID:-}" ] && kill "$CPU_PID" 2>/dev/null
  du -sb "$DIR" > out/log-dir-bytes.txt 2>/dev/null
  printf '{"finished": true, "outcome": "%s"}\n' "$1" > out/stopped.json
  push; push
  $PY s3io.py put "$BUCKET" "$P/stopped" out/stopped.json 2>/dev/null
  power_off "log node stopped"
}
IP=$(private_ip); TYPE=$(machine_type); AZ=$(zone)
step "log node $ID: $TYPE in $AZ, private IP $IP, $(nproc) vCPU, $(awk '/MemTotal/ {printf "%.1f GiB", $2/1048576}' /proc/meminfo)"
$PY cpu_macchina.py out/cpu-machine.log 5 & CPU_PID=$!
put_json "$P/hello.json" "{\"ip\": \"$IP\", \"az\": \"$AZ\", \"type\": \"$TYPE\"}" || finish NO_BUCKET
for _ in $(seq 1 120); do exists log/peers.json && break; sleep 5; done
$PY s3io.py get "$BUCKET" log/peers.json out/peers.json || finish NO_PEERS
# peers: every other replica as id=ip:7100
peers() { $PY -c "import json,sys; d=json.load(open('out/peers.json')); print(','.join(f'{i}={ip}:{sys.argv[2]}' for i, ip in enumerate(d['ips']) if str(i) != sys.argv[1]))" "$ID" "$1"; }
PEERS=$(peers 7100)
CMD=(./pq replica --id "$ID" --dir "$DIR" --peer-listen "0.0.0.0:7100" --client-listen "0.0.0.0:7200" --peers "$PEERS")
[ -n "${LOG_OPTIONS:-}" ] && read -ra MORE <<<"$LOG_OPTIONS" && CMD+=("${MORE[@]}")
echo "${CMD[*]}" > out/log-command.txt
step "start: ${CMD[*]}"
"${CMD[@]}" > out/pq.log 2>&1 &
PQ_PID=$!
./pq replica --id "$ID" --dir "$DIR-bench" --peer-listen "0.0.0.0:7101" --client-listen "0.0.0.0:7201" --peers "$(peers 7101)" \
  --mem-cap-mb 512 > out/pq-bench.log 2>&1 &
BENCH_PID=$!
sleep 3
kill -0 "$PQ_PID" 2>/dev/null || { tail -20 out/pq.log >> out/steps.log; finish LOG_DID_NOT_START; }
put_json "$P/ready.json" "{\"ip\": \"$IP\", \"port\": 7200, \"peer_port\": 7100, \"id\": $ID, \"az\": \"$AZ\", \"type\": \"$TYPE\"}" || finish NO_BUCKET
step "ready: log replica $ID on $IP:7200"
while :; do
  sleep "$PUSH_S"
  if ! kill -0 "$PQ_PID" 2>/dev/null; then step "WARNING: pq exited by itself"; tail -30 out/pq.log >> out/steps.log; PQ_PID=""; finish LOG_DIED; fi
  echo "$(date -u +%FT%TZ) $(ps -o rss= -p "$PQ_PID" | tr -d ' ')" >> out/rss-kib.log
  push
  exists "$P/stop" && finish STOPPED
done
