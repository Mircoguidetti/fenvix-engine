#!/bin/bash
# integration-4 part 2: start / stop our server for a merged branch's own probes (local only, $0).
#   part2-ours.sh start PORT BUCKET LOG [extra server args...]   fresh bucket, product defaults, replica set rs0
#   part2-ours.sh stop PORT
# The reference is the judge's mongod replica set on 127.0.0.1:27020 (judge/setup.sh).
set -u
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
JH=/root/.fenvix-judge; PY=$JH/env/bin/python
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
case $1 in
  start)
    PORT=$2; B=$3; LOG=$4; shift 4
    $PY - "$B" <<'P'
import boto3, sys
s3 = boto3.resource("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
b = s3.Bucket(sys.argv[1])
if b.creation_date is None: b.create()
else: b.objects.all().delete()
P
    nohup "$ROOT/target/release/mongo-server" --store s3 --bucket "$B" --region us-east-1 --endpoint http://127.0.0.1:9000 \
      --listen 127.0.0.1:$PORT --replica-set rs0 --advertise 127.0.0.1:$PORT "$@" > "$LOG" 2>&1 &
    echo $! > "/tmp/part2-ours-$PORT.pid"
    for _ in $(seq 1 60); do (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && { echo "ours up on $PORT ($(git -C "$ROOT" rev-parse --short HEAD))"; exit 0; }; sleep 1; done
    echo "ours did not start"; tail -5 "$LOG"; exit 1 ;;
  stop)
    P=$(cat "/tmp/part2-ours-$2.pid" 2>/dev/null) && kill -TERM "$P" 2>/dev/null
    for _ in $(seq 1 30); do kill -0 "$P" 2>/dev/null || exit 0; sleep 1; done; kill -9 "$P" 2>/dev/null; echo "SIGKILL after 30 s" ;;
esac
