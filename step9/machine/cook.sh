#!/bin/bash
# Step 9 · our server (mongo-server, data only on S3) on one machine of the kitchen. Built from kitchen/server.sh
# (minimal kitchen, K2/K2b/K2c) with the full kitchen around it: the writes are confirmed by the 3-zone log
# (--quaderno with the three log nodes' addresses) instead of one S3 PUT per write.
# ROLE:
#   loader     cook 0: the client loads the data through it; switched off after the load (never measured)
#   cook       the measured cook (server-1): cold, new process, new machine, in zone a like the client
#   spare      the warm spare in zone b: the same server, the same pantry and log, idle until a takeover
#   compactor  the separate compactor (compaction 2, step 4): runs only the engine options the runner gives it
# Order (as kitchen/server.sh): TLS certificate for this machine's private IP and the CA from the run bucket ->
# password (only in the processes' memory) -> config.json from the runner (pantry, server options, log addresses) ->
# SST count before the start -> mongo-server (TLS required, users required) -> root user -> "ready" -> every PUSH_S
# logs, CPU, memory, S3 request counters; SSTs every SST_EVERY_S -> on "stop": SIGTERM, SST count, "stopped", power
# off. Cook and spare also run takeover_agent.py, which starts and kills the takeover pair (port 27018) on request of
# the takeover program (programs/takeovers.py).
# Usage: cook.sh ROLE NAME BUCKET
set -u
cd "$(dirname "$0")" || exit 1
ROLE=$1; NAME=$2; BUCKET=$3; P=$NAME
source ./common.sh
PORT=${DB_PORT:-27017}
mkdir -p cert
finish() { # finish OUTCOME
  step "finish: $1"
  [ -n "${AGENT_PID:-}" ] && kill "$AGENT_PID" 2>/dev/null
  if [ -n "${SRV:-}" ]; then
    kill -TERM "$SRV" 2>/dev/null && for _ in $(seq 1 60); do kill -0 "$SRV" 2>/dev/null || break; sleep 1; done
    kill -0 "$SRV" 2>/dev/null && { step "the server did not stop in 60 s: SIGKILL"; kill -KILL "$SRV"; }
  fi
  for x in ${WATCH:-} ${CPU_PID:-} ${CNT_PID:-} ${SST_PID:-}; do kill "$x" 2>/dev/null; done
  [ -n "${SRV:-}" ] && $PY sst_count.py "$PANTRY" out/sst-end.json "$NAME end" 2>&1 | tee -a out/steps.log
  gzip -kf out/mongo-server.log 2>/dev/null
  printf '{"finished": true, "outcome": "%s"}\n' "$1" > out/stopped.json
  push; push
  $PY s3io.py put "$BUCKET" "$P/stopped" out/stopped.json 2>/dev/null
  power_off "$NAME stopped"
}
TYPE=$(machine_type); AZ=$(zone); IP=$(private_ip)
step "start of $NAME ($ROLE; $TYPE in $AZ); $(nproc) vCPU, $(awk '/MemTotal/ {printf "%.1f GiB", $2/1048576}' /proc/meminfo), $(uname -m); private IP $IP"
$PY cpu_macchina.py out/cpu-machine.log 5 & CPU_PID=$!
put_json "$P/hello.json" "{\"ip\": \"$IP\", \"az\": \"$AZ\", \"type\": \"$TYPE\"}" || finish NO_BUCKET
# 1. certificate and CA (the runner makes the certificate for this private IP after the launch)
for _ in $(seq 1 60); do exists "$P/server.pem" && break; sleep 5; done
$PY s3io.py get "$BUCKET" "$P/server.pem" cert/server.pem && $PY s3io.py get "$BUCKET" program/ca.pem cert/ca.pem || finish NO_CERTIFICATE
chmod 600 cert/server.pem
# 2. the password: only in the environment of the processes
CM_PW=$(text secret/pw) && [ -n "$CM_PW" ] || finish NO_PASSWORD
export CM_PW
# 3. this machine's settings from the runner: pantry, extra server options, the log
for _ in $(seq 1 60); do exists "$P/config.json" && break; sleep 5; done
$PY s3io.py get "$BUCKET" "$P/config.json" out/config.json || finish NO_CONFIG
cfg() { $PY cfg.py out/config.json "$1"; }
PANTRY=$(cfg pantry); OPTS=$(cfg server_options); LOGS=$(cfg logs); LOG_ID=$(cfg log_id)
[ -n "$PANTRY" ] || finish BAD_CONFIG
export PANTRY
step "pantry $PANTRY; log ${LOGS:-none (S3 WAL only)}; extra options '${OPTS}'"
# 4. SSTs in the pantry before our server starts
$PY sst_count.py "$PANTRY" out/sst-start.json "$NAME start" 2>&1 | tee -a out/steps.log
# 5. our server
read -ra EXTRA <<<"$OPTS"
# shellcheck disable=SC2207
END=($(s3_endpoint_args))
LOGARGS=(); [ -n "$LOGS" ] && LOGARGS=(--quaderno "$LOGS" --quaderno-id "${LOG_ID:-cell1}")
CMD=(./mongo-server --listen "0.0.0.0:$PORT" --store s3 --bucket "$PANTRY" --region eu-west-1 "${END[@]}"
     --replica-set rs0 --advertise "$IP:$PORT" --auth --tls-mode requireTLS --tls-certificate-key-file cert/server.pem
     "${LOGARGS[@]}" "${EXTRA[@]}")
if [ "$ROLE" = compactor ]; then
  # compaction 2 step 4 decides how the compactor runs; until then the runner passes its whole command line in
  # server_options and this machine only adds store, bucket and region (no clients ever connect to it)
  CMD=(./mongo-server --store s3 --bucket "$PANTRY" --region eu-west-1 "${END[@]}" "${EXTRA[@]}")
fi
echo "${CMD[*]}" > out/server-command.txt
step "start: ${CMD[*]}"
"${CMD[@]}" > out/mongo-server.log 2>&1 &
SRV=$!
( while kill -0 "$SRV" 2>/dev/null; do echo "$(date -u +%FT%TZ) $(ps -o rss= -p "$SRV" | tr -d ' ')" >> out/rss-kib.log; sleep 5; done ) &
WATCH=$!
if [ "$ROLE" = loader ] || [ "$ROLE" = cook ]; then
  $PY create_user.py "$PORT" cert/ca.pem >> out/steps.log 2>&1 || { tail -20 out/mongo-server.log >> out/steps.log; finish SERVER_DID_NOT_START; }
  $PY s3_counters.py "$PORT" cert/ca.pem out/s3-counters.json "$PUSH_S" 2>> out/s3-counters.err & CNT_PID=$!
else
  # the spare (and the compactor) cannot log in: every database, admin included, is owned by the cook's process while
  # it lives (that is the point of a standby). Ready = the server listens. Its S3 requests count in SPESA as a fixed
  # allowance (run.py).
  for _ in $(seq 1 60); do grep -q "listening on" out/mongo-server.log 2>/dev/null && break; kill -0 "$SRV" 2>/dev/null || break; sleep 2; done
  grep -q "listening on" out/mongo-server.log 2>/dev/null || [ "$ROLE" = compactor ] || { tail -20 out/mongo-server.log >> out/steps.log; finish SERVER_DID_NOT_START; }
fi
( while sleep "${SST_EVERY_S:-300}"; do kill -0 "$SRV" 2>/dev/null || break
    $PY sst_count.py "$PANTRY" out/sst-during.jsonl "$NAME during" >> out/sst-during.txt 2>&1; done ) & SST_PID=$!
# 6. cook and spare: the takeover agent (programs/takeovers.py asks it to start or kill the pair on port 27018)
if [ "$ROLE" = cook ] || [ "$ROLE" = spare ]; then
  $PY takeover_agent.py "$BUCKET" "$NAME" "$IP" > out/takeover-agent.log 2>&1 & AGENT_PID=$!
fi
printf '{"ip": "%s", "port": %s, "type": "%s", "az": "%s", "role": "%s", "k": "%s"}\n' "$IP" "$PORT" "$TYPE" "$AZ" "$ROLE" "${NAME#server-}" > out/ready.json
$PY s3io.py put "$BUCKET" "$P/ready.json" out/ready.json || finish NO_BUCKET
step "ready: $ROLE on $IP:$PORT"
while :; do
  sleep "$PUSH_S"
  if ! kill -0 "$SRV" 2>/dev/null; then wait "$SRV"; step "WARNING: mongo-server exited by itself (code $?)"; tail -30 out/mongo-server.log >> out/steps.log; SRV=""; finish SERVER_DIED; fi
  push
  exists "$P/stop" && finish STOPPED
done
