#!/usr/bin/env bash
# One variant of the cold-read measurement (cold_customer.py) on the local MinIO, in its own bucket:
# start the server, load, wait for the compaction to settle, stop (SIGTERM: clean close), start again (empty caches),
# measure, stop. Results and logs in OUTDIR.
# Usage: bench/compaction/run_cold.sh BINARY OUTDIR GB "EXTRA SERVER FLAGS" [--customer-key]
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd); cd "$ROOT"
BIN=$(readlink -f "$1"); OUT=$(mkdir -p "$2" && readlink -f "$2"); GB=$3; EXTRA=${4:-}; CK=${5:-}
JH=${JUDGE_HOME:-$HOME/.fenvix-judge}; PY="$JH/env/bin/python"
. tools/minio-env.sh
PORT=${PORT:-27041}; BUCKET=cold-$(basename "$OUT" | tr '_.' '--' | tr 'A-Z' 'a-z')
URI="mongodb://127.0.0.1:$PORT/?directConnection=true"
"$PY" - "$BUCKET" <<'P'
import boto3, sys
s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
try: s3.create_bucket(Bucket=sys.argv[1])
except Exception as e: print("bucket:", e)
P
start() {
  # shellcheck disable=SC2086
  "$BIN" --listen 127.0.0.1:$PORT --idle-secs 100000 --holder cold-$PORT --bucket "$BUCKET" --no-ttl $EXTRA >> "$OUT/server.log" 2>&1 &
  echo $! > "$OUT/server.pid"
  for _ in $(seq 1 100); do (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && return 0; sleep 0.2; done
  echo "server did not start"; exit 1
}
stop() { kill -TERM "$(cat "$OUT/server.pid")"; while kill -0 "$(cat "$OUT/server.pid")" 2>/dev/null; do sleep 0.5; done; }
settle() {
  # quiet = no compaction pass running for 20 s in a row
  "$PY" - "$URI" <<'P'
import sys, time
from pymongo import MongoClient
c = MongoClient(sys.argv[1])
quiet_since = None
t0 = time.time()
while time.time() - t0 < 3600:
    r = c.admin.command("serverStatus")["fenvix"]["compaction"]["running"]
    if r == 0:
        quiet_since = quiet_since or time.time()
        if time.time() - quiet_since >= 20:
            break
    else:
        quiet_since = None
    time.sleep(1)
print(f"settled after {time.time() - t0:.0f} s")
P
}
sstinfo() {
  "$PY" - "$URI" "$1" <<'P' || true
import sys, json
from pymongo import MongoClient
c = MongoClient(sys.argv[1])
try:
    i = c.admin.command("fenvixSstInfo", "chat")
    i.pop("ok", None)
    runs = [(r["id"], len(r["ssts"]), sum(s["dataBytes"] for s in r["ssts"])) for r in i["runs"]]
    print(f"{sys.argv[2]}: L0 {len(i['l0'])}, sorted runs {len(runs)}: " + ", ".join(f"run {a}: {n} SSTs {b / 2**20:.0f} MiB" for a, n, b in runs))
    codecs = sorted({s["codec"] for s in i["l0"]} | {s["codec"] for r in i["runs"] for s in r["ssts"]})
    blocks = sum(s["blocks"] for r in i["runs"] for s in r["ssts"]); data = sum(s["dataBytes"] for r in i["runs"] for s in r["ssts"])
    print(f"  codecs {codecs}, sorted-run data {data / 2**20:.0f} MiB in {blocks} blocks ({data / max(blocks, 1) / 1024:.1f} KiB stored per block)")
except Exception as e:
    print(f"{sys.argv[2]}: fenvixSstInfo not available ({str(e)[:80]})")
P
}
start
t=$(date +%s)
"$PY" bench/compaction/cold_customer.py load --uri "$URI" --gb "$GB" --state "$OUT/state.json" $CK | tee "$OUT/load.txt"
settle | tee -a "$OUT/load.txt"
echo "load + settle $(( $(date +%s) - t )) s" | tee -a "$OUT/load.txt"
sstinfo "after load" | tee -a "$OUT/load.txt"
stop
"$PY" - "$BUCKET" <<'P' | tee -a "$OUT/load.txt"
import boto3, sys
s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
n = b = 0
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=sys.argv[1], Prefix="tenants/chat/compacted/"):
    for o in page.get("Contents", []):
        n += 1; b += o["Size"]
print(f"bucket: {n} compacted objects, {b / 2**30:.2f} GiB")
P
start
sstinfo "at measure" | tee "$OUT/measure.txt"
"$PY" bench/compaction/cold_customer.py measure --uri "$URI" --state "$OUT/state.json" --n "${N:-200}" --warmup "${WARMUP:-300}" --out "$OUT/result.json" | tee -a "$OUT/measure.txt"
stop
