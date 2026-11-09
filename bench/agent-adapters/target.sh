#!/usr/bin/env bash
# Start or stop one target on port 27017 (most adapter suites hard-code localhost:27017):
#   target.sh mongod|ours         fresh, empty target; replica set rs0, as the judge runs both
#   target.sh stop                stop the target on PORT;  target.sh stopall: every target this script started
# KEEP=1 (ours only) restarts on the existing bucket without wiping it. PORT (default 27017) picks the port, MODE=standalone starts it without a replica set (Mastra's suite wants one of each).
# mongod 7.0 and MinIO come from judge/setup.sh. Our server stores in MinIO bucket "adapters", wiped on every start.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd); cd "$ROOT"
JH=${JUDGE_HOME:-$HOME/.fenvix-judge}; ENV="$JH/env"; export PATH="$ENV/bin:$PATH"
W=${ADAPTERS_WORK:-$HOME/.fenvix-adapters}; mkdir -p "$W"
PORT=${PORT:-27017}; MODE=${MODE:-rs}; D="$W/$PORT"; mkdir -p "$D"
up() { for _ in $(seq 1 100); do (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && return 0; sleep 0.2; done; return 1; }
stop_pid() {
  if [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null; then
    kill -TERM "$(cat "$1")"; while kill -0 "$(cat "$1")" 2>/dev/null; do sleep 0.2; done
  fi
  rm -f "$1"
}
stop() { stop_pid "$D/pid"; }
stop_all() { for p in "$W"/*/pid; do [ -e "$p" ] && stop_pid "$p"; done; return 0; }
# the LangGraph JS suite connects as user/password (its docker compose creates that root user): same user on both targets;
# neither target runs with --auth, so every other suite connects without credentials
add_user() {
  "$ENV/bin/python" - "$PORT" <<'P'
import pymongo, sys
c = pymongo.MongoClient(f"mongodb://127.0.0.1:{sys.argv[1]}/?directConnection=true")
c.admin.command("createUser", "user", pwd="password", roles=["root"])
P
}
minio_up() {
  mkdir -p "$JH/data/minio/bench"
  if ! (exec 3<>/dev/tcp/127.0.0.1/9000) 2>/dev/null; then
    MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin nohup minio server "$JH/data/minio" --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 > "$JH/data/minio.log" 2>&1 &
    for _ in $(seq 1 60); do (exec 3<>/dev/tcp/127.0.0.1/9000) 2>/dev/null && break; sleep 0.5; done
  fi
}
case "${1:-}" in
  stop) stop ;;
  stopall) stop_all ;;
  mongod)
    stop; rm -rf "$D/mongod"; mkdir -p "$D/mongod"
    RS=""; [ "$MODE" = rs ] && RS="--replSet rs0"
    mongod $RS --setParameter ttlMonitorSleepSecs=1 --port $PORT --bind_ip 127.0.0.1 --dbpath "$D/mongod" \
      --fork --logpath "$D/mongod.log" --pidfilepath "$D/pid" > /dev/null
    "$ENV/bin/python" - "$PORT" "$MODE" <<'P'
import pymongo, sys, time
p = sys.argv[1]
c = pymongo.MongoClient(f"mongodb://127.0.0.1:{p}/?directConnection=true")
if sys.argv[2] == "rs":
    c.admin.command("replSetInitiate", {"_id": "rs0", "members": [{"_id": 0, "host": f"localhost:{p}"}]})
for _ in range(60):
    if c.admin.command("hello").get("isWritablePrimary") or sys.argv[2] != "rs": break
    time.sleep(0.5)
P
    add_user; echo "mongod 7.0 ($MODE) up on $PORT" ;;
  ours)
    stop; minio_up
    # wipe the bucket with MinIO itself running (fs mode: one directory per bucket); one bucket per port
    B="adapters"; [ "$PORT" != 27017 ] && B="adapters-$PORT"
    "$ENV/bin/python" - "$B" "${KEEP:-0}" <<'P'
import boto3, sys
s3 = boto3.resource("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
b = s3.Bucket(sys.argv[1])
if b.creation_date is None: b.create()
elif sys.argv[2] != "1": b.objects.all().delete()
P
    . tools/minio-env.sh
    nohup ./target/release/mongo-server --listen 127.0.0.1:$PORT --idle-secs 60 --holder "adapters-$PORT" --bucket "$B" \
      --ttl-sweep-secs 1 $([ "$MODE" = rs ] && echo "--replica-set rs0") --advertise localhost:$PORT ${OURS_EXTRA:-} > "$D/ours.log" 2>&1 &
    echo $! > "$D/pid"; up || { cat "$D/ours.log"; exit 1; }
    [ "${KEEP:-0}" = 1 ] || add_user; echo "mongo-server ($(git rev-parse --short HEAD), $MODE) up on $PORT" ;;
  *) echo "usage: target.sh mongod|ours|stop"; exit 2 ;;
esac
