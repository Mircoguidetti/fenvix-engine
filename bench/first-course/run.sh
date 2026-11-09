#!/usr/bin/env bash
# First course in the engine (opening.rs): the two-visit test of prompts/P-first-course-engine.md (fenvix-brain).
#
#   bench/first-course/run.sh load BINARY OUTDIR
#       load the 2 GB chat dataset of the compaction bench (cold_customer.py, customer key on, lz4, 64 KiB blocks) into
#       the bucket ${BASE:-fc-base}, let the compaction settle, stop cleanly.
#   bench/first-course/run.sh visits BINARY OUTDIR GAP "EXTRA SERVER FLAGS"
#       copy ${BASE:-fc-base} into a fresh bucket, start the server on it with the simulated store (5 ms per GET), a 1 MiB block
#       cache and EXTRA, run two_visits.py with gap GAP (short, long, busy), stop, delete the bucket.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd); cd "$ROOT"
MODE=$1; BIN=$(readlink -f "$2"); OUT=$(mkdir -p "$3" && readlink -f "$3")
JH=${JUDGE_HOME:-$HOME/.fenvix-judge}; PY="$JH/env/bin/python"
. tools/minio-env.sh
PORT=${PORT:-27051}
URI="mongodb://127.0.0.1:$PORT/?directConnection=true"
FORMAT="--sst-compression lz4 --sst-block-kb 64"
S3PY="import boto3; s3 = boto3.client('s3', endpoint_url='http://127.0.0.1:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin', region_name='us-east-1')"

start() { # BUCKET FLAGS...
  local b=$1; shift
  # shellcheck disable=SC2068
  "$BIN" --listen 127.0.0.1:$PORT --idle-secs 100000 --holder fc-$PORT --bucket "$b" --no-ttl $FORMAT $@ >> "$OUT/server.log" 2>&1 &
  echo $! > "$OUT/server.pid"
  for _ in $(seq 1 150); do (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && return 0; sleep 0.2; done
  echo "server did not start"; exit 1
}
stop() { kill -TERM "$(cat "$OUT/server.pid")" 2>/dev/null || true; while kill -0 "$(cat "$OUT/server.pid")" 2>/dev/null; do sleep 0.5; done; }
trap '[ -f "$OUT/server.pid" ] && stop' EXIT
if (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; then echo "port $PORT is busy"; exit 1; fi

case $MODE in
  load)
    "$PY" -c "$S3PY
try: s3.create_bucket(Bucket='${BASE:-fc-base}')
except Exception as e: print('bucket:', e)"
    # shellcheck disable=SC2086
    start ${BASE:-fc-base} --customer-key chat.messages=userId ${LOAD_EXTRA:-}
    t=$(date +%s)
    "$PY" bench/compaction/cold_customer.py load --uri "$URI" --gb "${GB:-2}" --state "$OUT/state.json" --customer-key | tee "$OUT/load.txt"
    "$PY" bench/first-course/two_visits.py settle --uri "$URI" | tee -a "$OUT/load.txt"
    # compaction 2 step 6 (SEALED=1, with LOAD_EXTRA="--sealed-runs 0 ..."): seal the loaded database into our runs
    if [ -n "${SEALED:-}" ]; then "$PY" bench/c2-step6/sealed.py publish --uri "$URI" --db chat --reseal all | tee -a "$OUT/load.txt"; fi
    echo "load + settle $(( $(date +%s) - t )) s" | tee -a "$OUT/load.txt"
    stop ;;
  visits)
    GAP=$4; EXTRA=${5:-}
    B=fc-$(basename "$OUT" | tr '_.' '--' | tr 'A-Z' 'a-z')
    "$PY" -c "$S3PY
import sys
b = sys.argv[1]
try: s3.create_bucket(Bucket=b)
except Exception as e: print('bucket:', e)
for page in s3.get_paginator('list_objects_v2').paginate(Bucket=b):
    objs = [{'Key': o['Key']} for o in page.get('Contents', [])]
    if objs: s3.delete_objects(Bucket=b, Delete={'Objects': objs})
n = 0
for page in s3.get_paginator('list_objects_v2').paginate(Bucket='${BASE:-fc-base}'):
    for o in page.get('Contents', []):
        s3.copy_object(Bucket=b, Key=o['Key'], CopySource={'Bucket': '${BASE:-fc-base}', 'Key': o['Key']}); n += 1
print(f'copied {n} objects into {b}')" "$B" | tee "$OUT/copy.txt"
    # shellcheck disable=SC2086
    FENVIX_SIM_S3=${SIM:-5,0,100000} start "$B" --block-cache-mb 1 $EXTRA
    "$PY" bench/first-course/two_visits.py visits --uri "$URI" --state "${STATE:-$ROOT/bench/first-course/results/load/state.json}" --gap "$GAP" \
      --users "${USERS:-1000}" --scale "${SCALE:-1}" --warm "${WARM:-300}" --pid "$(cat "$OUT/server.pid")" --out "$OUT/result.json" ${SEALED:+--sealed} | tee "$OUT/visits.txt"
    stop
    "$PY" -c "$S3PY
import sys
b = sys.argv[1]
for page in s3.get_paginator('list_objects_v2').paginate(Bucket=b):
    objs = [{'Key': o['Key']} for o in page.get('Contents', [])]
    if objs: s3.delete_objects(Bucket=b, Delete={'Objects': objs})
s3.delete_bucket(Bucket=b); print('deleted', b)" "$B" ;;
  *) echo "usage: run.sh load|visits ..."; exit 2 ;;
esac
