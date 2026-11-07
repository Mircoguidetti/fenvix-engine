#!/usr/bin/env bash
# Our server: load the checkpoint history (ckpt.py load) into a local MinIO bucket with the customer key on the two
# checkpointer collections, let the compaction settle, stop, let the SST cleanup run, count the bucket, then restart
# with the simulated S3 latency (5 ms per GET) and a 1 MiB block cache, and measure (ckpt.py measure --meter).
# Usage: bench/checkpoint-history/run_ours.sh BINARY OUTDIR THREADS LONG [load|measure|both]
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd); cd "$ROOT"
BIN=$(readlink -f "$1"); OUT=$(mkdir -p "$2" && readlink -f "$2"); THREADS=$3; LONG=$4; WHAT=${5:-both}
JH=${JUDGE_HOME:-$HOME/.fenvix-judge}; PY=${PY:-/home/user/adapters/venv-lg/bin/python}
. tools/minio-env.sh
PORT=${PORT:-27041}; BUCKET=${BUCKET:-ckpt-history}
URI="mongodb://127.0.0.1:$PORT/?directConnection=true"
CK="--customer-key checkpointing_db.checkpoints=thread_id,checkpointing_db.checkpoint_writes=thread_id --sst-compression lz4 --sst-block-kb 64"
"$JH/env/bin/python" - "$BUCKET" <<'P'
import boto3, sys
s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
try: s3.create_bucket(Bucket=sys.argv[1])
except Exception as e: print("bucket:", str(e)[:80])
P
start() {
  # shellcheck disable=SC2086
  "$BIN" --listen 127.0.0.1:$PORT --idle-secs 100000 --holder ckpt-$PORT --bucket "$BUCKET" --no-ttl $CK "$@" >> "$OUT/server.log" 2>&1 &
  echo $! > "$OUT/server.pid"
  for _ in $(seq 1 100); do (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && return 0; sleep 0.2; done
  echo "server did not start"; exit 1
}
stop() { kill -TERM "$(cat "$OUT/server.pid")"; while kill -0 "$(cat "$OUT/server.pid")" 2>/dev/null; do sleep 0.5; done; }
settle() {
  "$PY" - "$URI" <<'P'
import sys, time
from pymongo import MongoClient
c = MongoClient(sys.argv[1])
quiet_since = None
t0 = time.time()
while time.time() - t0 < 7200:
    r = c.admin.command("serverStatus")["fenvix"]["compaction"]["running"]
    if r == 0:
        quiet_since = quiet_since or time.time()
        if time.time() - quiet_since >= 30:
            break
    else:
        quiet_since = None
    time.sleep(1)
print(f"settled after {time.time() - t0:.0f} s")
P
}
sstinfo() {
  "$PY" - "$URI" "$1" <<'P' || true
import sys
from pymongo import MongoClient
c = MongoClient(sys.argv[1])
i = c.admin.command("fenvixSstInfo", "checkpointing_db")
runs = [(r["id"], len(r["ssts"]), sum(s["dataBytes"] for s in r["ssts"])) for r in i["runs"]]
print(f"{sys.argv[2]}: L0 {len(i['l0'])} SSTs, sorted runs {len(runs)}: " + ", ".join(f"run {a}: {n} SSTs {b / 2**20:.0f} MiB" for a, n, b in runs))
codecs = sorted({s["codec"] for s in i["l0"]} | {s["codec"] for r in i["runs"] for s in r["ssts"]})
print(f"  codecs {codecs}")
P
}
bucket() {
  "$JH/env/bin/python" - "$BUCKET" <<'P'
import boto3, sys, collections
s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
by = collections.Counter(); n = collections.Counter()
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=sys.argv[1]):
    for o in page.get("Contents", []):
        k = o["Key"].split("/"); top = "/".join(k[:3]) if k[0] == "tenants" else k[0]
        by[top] += o["Size"]; n[top] += 1
for k in sorted(by): print(f"bucket {k}: {n[k]} objects, {by[k]} bytes ({by[k] / 2**30:.3f} GiB)")
print(f"bucket total: {sum(n.values())} objects, {sum(by.values())} bytes")
P
}
if [ "$WHAT" != measure ]; then
  start
  t=$(date +%s)
  "$PY" bench/checkpoint-history/ckpt.py load --uri "$URI" --threads "$THREADS" --long "$LONG" --plan "$OUT/plan.json" --workers "${WORKERS:-8}" | tee "$OUT/load.txt"
  settle | tee -a "$OUT/load.txt"
  echo "load + settle $(( $(date +%s) - t )) s" | tee -a "$OUT/load.txt"
  sstinfo "after load" | tee -a "$OUT/load.txt"
  "$PY" -c "
from pymongo import MongoClient; d = MongoClient('$URI').checkpointing_db
for n in ('checkpoints', 'checkpoint_writes'):
    s = d.command('collStats', n); print(n, 'count', s['count'], 'size', s['size'])" | tee -a "$OUT/load.txt"
  bucket | sed 's/^/right after settle: /' | tee -a "$OUT/load.txt"
  stop
  # the SST cleanup deletes compacted-away SSTs after a minimum age: restart, one tiny write so the database opens for
  # writing, wait, stop (as sessions/2026-11-06-agent-adapters-local did)
  start
  # the cleanup of an open writer is due only when its manifest has moved on: a trickle of tiny writes (one per second,
  # in a side collection) keeps the memtable flushing; stop when the bucket stops shrinking
  "$PY" - "$URI" "$BUCKET" "${GC_WAIT:-900}" <<'Q'
import sys, time, boto3
from pymongo import MongoClient
c = MongoClient(sys.argv[1]); s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
def size():
    return sum(o["Size"] for p in s3.get_paginator("list_objects_v2").paginate(Bucket=sys.argv[2], Prefix="tenants/checkpointing_db/compacted/") for o in p.get("Contents", []))
t0, last, same = time.time(), None, 0
while time.time() - t0 < float(sys.argv[3]):
    for _ in range(30):
        c.checkpointing_db["_settle"].insert_one({"pad": "x" * 2000})
        time.sleep(1)
    b = size(); print(f"cleanup {time.time() - t0:.0f} s: compacted/ {b / 2**30:.3f} GiB", flush=True)
    same = same + 1 if last is not None and b == last else 0
    last = b
    if same >= 6:
        break
Q
  sstinfo "after cleanup" | tee -a "$OUT/load.txt"
  stop
  bucket | sed 's/^/after cleanup: /' | tee -a "$OUT/load.txt"
fi
if [ "$WHAT" != load ]; then
  FENVIX_SIM_S3=${SIM:-5,0,1000} start --block-cache-mb "${BLOCK_MB:-1}"
  sstinfo "at measure" | tee "$OUT/measure.txt"
  "$PY" bench/checkpoint-history/ckpt.py measure --uri "$URI" --plan "$OUT/plan.json" --out "$OUT/result.json" --meter ${MEASURE_ARGS:-} | tee -a "$OUT/measure.txt"
  stop
fi
