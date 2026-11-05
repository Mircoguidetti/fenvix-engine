#!/bin/bash
# Kitchen (K2, corrected by K2b) · what runs ON A SERVER MACHINE (the cook: an EC2 of one size, Ubuntu 24.04,
# eu-west-1). Started by the user-data written by ci/run.sh. No AWS keys: S3 with the machine role (IMDSv2), both for
# the run's files (boto3) and for our server's data (mongo-server, AmazonS3Builder::from_env).
# Every cook is a NEW machine with a NEW process (reviewer's C1): cook 0 only loads the data and is switched off when
# the load ends; cooks 1..N are the sizes, each starting cold (empty cache, no memtable left from the load).
# Order: TLS certificate of this cook from the run bucket (made by the runner for this machine's private IP) ->
# password from the bucket (only in the processes' memory) -> SST count in the pantry BEFORE our server starts (C6) ->
# mongo-server on the pantry (the data bucket), TLS required, users required, port 27017 open only to the client machine
# (security group) -> root user (localhost exception) -> "ready" in the bucket -> every 30 s log, CPU, memory and the S3
# request counters of our server (point 4: the SPESA line counts S3 for real), SST count every 5 minutes (K5) -> on the
# runner's "stop": SIGTERM, SST
# count AFTER the stop (C6), final logs, "stopped", power off.
# Usage: server.sh RUN_BUCKET K   (PANTRY from server.env, written by the runner into the package; no secrets)
set -u
cd "$(dirname "$0")" || exit 1
BUCKET=$1; K=$2
source ./server.env   # PANTRY (no secrets)
export AWS_DEFAULT_REGION=eu-west-1 AWS_REGION=eu-west-1
export PYTHONPATH="$PWD/vendor" PYTHONUNBUFFERED=1
PY=${PY:-python3}
PORT=${DB_PORT:-27017}
P=server-$K
PUSH_S=${PUSH_S:-30}
mkdir -p out cert
step() { echo "$(date -u +%FT%TZ) $*" | tee -a out/steps.log; }
push() { $PY s3io.py push "$BUCKET" out "results/$P/" 2>> out/push-errors.log; }
finish() { # finish OUTCOME
  step "finish: $1"
  if [ -n "${SRV:-}" ]; then
    kill -TERM "$SRV" 2>/dev/null && for _ in $(seq 1 60); do kill -0 "$SRV" 2>/dev/null || break; sleep 1; done
    kill -0 "$SRV" 2>/dev/null && { step "the server did not stop in 60 s: SIGKILL"; kill -KILL "$SRV"; }
  fi
  [ -n "${WATCH:-}" ] && kill "$WATCH" 2>/dev/null
  [ -n "${CPU_PID:-}" ] && kill "$CPU_PID" 2>/dev/null
  [ -n "${CNT_PID:-}" ] && kill "$CNT_PID" 2>/dev/null
  [ -n "${SST_PID:-}" ] && kill "$SST_PID" 2>/dev/null
  [ -n "${SRV:-}" ] && $PY sst_count.py "$PANTRY" out/sst-end.json "cook $K end" 2>&1 | tee -a out/steps.log
  gzip -kf out/mongo-server.log 2>/dev/null
  printf '{"finished": true, "outcome": "%s"}\n' "$1" > out/stopped.json
  push; push
  $PY s3io.py put "$BUCKET" "$P/stopped" out/stopped.json 2>/dev/null
  [ -n "${LOCAL_TEST:-}" ] || shutdown -h now "kitchen: server stopped"
  exit 0
}
imds() { local t; t=$(curl -fsS -m 5 -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 600" 2>/dev/null) &&
  curl -fsS -m 5 -H "X-aws-ec2-metadata-token: $t" "http://169.254.169.254/latest/meta-data/$1" 2>/dev/null; }
TYPE_K=$(imds instance-type || echo "${FAKE_TYPE:-?}")
step "start of cook $K ($TYPE_K; $([ "$K" = 0 ] && echo "loader, switched off after the load" || echo "size $K, cold")); $(nproc) vCPU, $(awk '/MemTotal/ {printf "%.1f GiB", $2/1048576}' /proc/meminfo), $(uname -m)"
$PY cpu_macchina.py out/cpu-machine.log 5 & CPU_PID=$!
# 1. certificate (chain + key) and CA: the runner makes them after the launch, for this machine's private IP
for _ in $(seq 1 60); do $PY s3io.py exists "$BUCKET" "$P/server.pem" && break; sleep 5; done
$PY s3io.py get "$BUCKET" "$P/server.pem" cert/server.pem && $PY s3io.py get "$BUCKET" program/ca.pem cert/ca.pem \
  || finish NO_CERTIFICATE
chmod 600 cert/server.pem
# 2. the password: only in the processes' environment, never in a file or a log
CM_PW=$($PY s3io.py text "$BUCKET" secret/pw) && [ -n "$CM_PW" ] || finish NO_PASSWORD
export CM_PW
# 2b. this cook's settings from the runner (K2c): the pantry it works on (K3: a fresh copy for a size) and its server
# options (K6: per size; when given they replace server-options.txt)
COOK_OPTS=""
if $PY s3io.py get "$BUCKET" "$P/config.json" out/config.json 2>/dev/null; then
  PANTRY=$($PY -c "import json; print(json.load(open('out/config.json'))['pantry'])") || finish BAD_CONFIG
  COOK_OPTS=$($PY -c "import json; print(json.load(open('out/config.json')).get('server_options') or '')") || finish BAD_CONFIG
fi
# 3. private IP (the one in the certificate and in hello.hosts)
IP=$(imds local-ipv4 || echo "${FAKE_IP:-?}")
step "private IP $IP; pantry $PANTRY"
# 4. the SSTs in the pantry before our server starts (C6)
$PY sst_count.py "$PANTRY" out/sst-start.json "cook $K start" 2>&1 | tee -a out/steps.log
# 5. our server, with the engine's default options (DECISIONS 2026-11-05: the engine as it is) plus server-options.txt
# (empty by default). Local test only: --endpoint to the fake S3.
if [ -n "$COOK_OPTS" ]; then read -ra EXTRA_SRV <<<"$COOK_OPTS"; else read -ra EXTRA_SRV < server-options.txt; fi
END=(); [ -n "${LOCAL_TEST:-}" ] && [ -n "${AWS_ENDPOINT_URL:-}" ] && END=(--endpoint "$AWS_ENDPOINT_URL")
CMD=(./mongo-server --listen "0.0.0.0:$PORT" --store s3 --bucket "$PANTRY" --region eu-west-1 "${END[@]}"
     --replica-set rs0 --advertise "$IP:$PORT" --auth --tls-mode requireTLS --tls-certificate-key-file cert/server.pem
     "${EXTRA_SRV[@]}")
echo "${CMD[*]}" > out/server-command.txt
step "start: ${CMD[*]}"
"${CMD[@]}" > out/mongo-server.log 2>&1 &
SRV=$!
echo "$SRV" > out/server.pid
# memory (RSS) of the process every 5 s
( while kill -0 "$SRV" 2>/dev/null; do echo "$(date -u +%FT%TZ) $(ps -o rss= -p "$SRV" | tr -d ' ')" >> out/rss-kib.log; sleep 5; done ) &
WATCH=$!
$PY create_user.py "$PORT" cert/ca.pem >> out/steps.log 2>&1 || { tail -20 out/mongo-server.log >> out/steps.log; finish SERVER_DID_NOT_START; }
$PY s3_counters.py "$PORT" cert/ca.pem out/s3-counters.json "$PUSH_S" 2>> out/s3-counters.err & CNT_PID=$!
# K5 (reviewer of K2b): the SSTs in the pantry every SST_EVERY_S (5 minutes) while the cook is on, not only at the start
# and the end, so a compaction during the measures is visible and can be placed against the program's phase times
# (one LIST each time, about $0.005 per 1,000 objects, DECLARED)
( while sleep "${SST_EVERY_S:-300}"; do kill -0 "$SRV" 2>/dev/null || break
    $PY sst_count.py "$PANTRY" out/sst-during.jsonl "cook $K during" >> out/sst-during.txt 2>&1; done ) & SST_PID=$!
printf '{"ip": "%s", "port": %s, "type": "%s", "k": %s}\n' "$IP" "$PORT" "$TYPE_K" "$K" > out/ready.json
$PY s3io.py put "$BUCKET" "$P/ready.json" out/ready.json || finish NO_BUCKET
step "ready: mongo-server on $IP:$PORT (TLS, users required)"
# 6. wait for the runner's "stop"; meanwhile logs and counters to the bucket
while :; do
  sleep "$PUSH_S"
  if ! kill -0 "$SRV" 2>/dev/null; then wait "$SRV"; step "WARNING: mongo-server exited by itself (code $?)"; tail -30 out/mongo-server.log >> out/steps.log; finish SERVER_DIED; fi
  push
  $PY s3io.py exists "$BUCKET" "$P/stop" && finish STOPPED
done
