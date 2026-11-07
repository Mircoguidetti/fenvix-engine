#!/usr/bin/env bash
# First course: the hostile-write stress test (stress.py) against a fresh bucket.
#
#   bench/first-course/stress.sh BINARY OUTFILE "SERVER FLAGS" "STRESS FLAGS"
#       start BINARY with --first-course --first-course-idle-secs 1 and SERVER FLAGS on a fresh bucket, run stress.py with
#       STRESS FLAGS, stop, delete the bucket. Set NEGATIVE=1 for the negative control (the server ignores writes in its
#       openings; stress.py then runs with --expect-wrong and must find wrong answers).
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd); cd "$ROOT"
BIN=$(readlink -f "$1"); OUTF=$2; SFLAGS=${3:-}; TFLAGS=${4:-}
JH=${JUDGE_HOME:-$HOME/.fenvix-judge}; PY="$JH/env/bin/python"
. tools/minio-env.sh
PORT=${PORT:-27061}
B=fc-stress-$PORT
S3PY="import boto3, sys; s3 = boto3.client('s3', endpoint_url='http://127.0.0.1:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin', region_name='us-east-1'); b = sys.argv[1]"
CLEAN="
try: s3.create_bucket(Bucket=b)
except Exception: pass
for page in s3.get_paginator('list_objects_v2').paginate(Bucket=b):
    objs = [{'Key': o['Key']} for o in page.get('Contents', [])]
    if objs: s3.delete_objects(Bucket=b, Delete={'Objects': objs})"
"$PY" -c "$S3PY$CLEAN" "$B"
LOG=$(mktemp)
if [ -n "${NEGATIVE:-}" ]; then export FENVIX_FIRST_COURSE_NEGATIVE_CONTROL=1; TFLAGS="$TFLAGS --expect-wrong"; fi
# shellcheck disable=SC2086
"$BIN" --listen 127.0.0.1:$PORT --idle-secs 100000 --holder fcs-$PORT --bucket "$B" --no-ttl --first-course --first-course-idle-secs 1 $SFLAGS > "$LOG" 2>&1 &
PID=$!
trap 'kill -TERM $PID 2>/dev/null || true; wait $PID 2>/dev/null || true; rm -f "$LOG"' EXIT
for _ in $(seq 1 150); do (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && break; sleep 0.2; done
echo "binary $(basename "$BIN"), server flags: $SFLAGS, stress flags: $TFLAGS" > "$OUTF"
# shellcheck disable=SC2086
set +e; "$PY" bench/first-course/stress.py --uri "mongodb://127.0.0.1:$PORT/?directConnection=true" $TFLAGS | tee -a "$OUTF"; rc=${PIPESTATUS[0]}; set -e
"$PY" -c "from pymongo import MongoClient; import sys; print('firstCourse at the end:', MongoClient(sys.argv[1]).admin.command('serverStatus')['fenvix']['firstCourse'])" "mongodb://127.0.0.1:$PORT/?directConnection=true" >> "$OUTF"
kill -TERM $PID; wait $PID 2>/dev/null || true
"$PY" -c "$S3PY$CLEAN
s3.delete_bucket(Bucket=b)" "$B"
exit $rc
