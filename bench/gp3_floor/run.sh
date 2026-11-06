#!/usr/bin/env bash
# gp3 floor prototype (the SST mirror), points 1, 2 and 5 of its contract, on the local MinIO, $0.
#
#   load      chat-like data (cold_customer.py: many customers, zipf) through the server into its own bucket, wait for
#             compaction to settle, stop
#   base      the same server WITHOUT the mirror (waiter tools, counter RAM $RAM_MB MiB, an empty disk floor): N cold
#             finds of customers never read before; GETs from the server's own meter of object-store reads
#   fill      the server WITH the mirror, started empty: open the database, time until every SST of the manifest is in
#             the mirror (fill GETs counted); the mirror's RAM bookkeeping (indexBytes)
#   mirror    the server WITH the filled mirror, restarted (empty RAM): the same N cold finds (same seed, same customers)
#
# Usage: bench/gp3_floor/run.sh BINARY OUTDIR GB [steps...]      (steps default: load base fill mirror)
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd); cd "$ROOT"
BIN=$(readlink -f "$1"); OUT=$(mkdir -p "$2" && readlink -f "$2"); GB=$3; shift 3
STEPS=${*:-load base fill mirror}
JH=${JUDGE_HOME:-$HOME/.fenvix-judge}; PY="$JH/env/bin/python"
. tools/minio-env.sh
PORT=${PORT:-27051}; BUCKET=gp3-$(basename "$OUT" | tr '_.' '--' | tr 'A-Z' 'a-z')
URI="mongodb://127.0.0.1:$PORT/?directConnection=true"
RAM_MB=${RAM_MB:-256}; N=${N:-500}; WARMUP=${WARMUP:-300}; SEED=${SEED:-11}
WORK=${WORK:-$OUT/work}; mkdir -p "$WORK"
WAITER="--waiter-tools --counter-ram-mb $RAM_MB"

start() {
  # shellcheck disable=SC2086
  "$BIN" --listen 127.0.0.1:$PORT --idle-secs 100000 --holder gp3-$PORT --bucket "$BUCKET" --no-ttl "$@" >> "$OUT/server.log" 2>&1 &
  echo $! > "$OUT/server.pid"
  for _ in $(seq 1 150); do (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && return 0; sleep 0.2; done
  echo "server did not start"; exit 1
}
stop() { kill -TERM "$(cat "$OUT/server.pid")"; while kill -0 "$(cat "$OUT/server.pid")" 2>/dev/null; do sleep 0.5; done; }
rss() { awk '/VmRSS/ {print $2 * 1024}' "/proc/$(cat "$OUT/server.pid")/status"; }
snap() {  # serverStatus fenvix.waiter (and the RSS) as JSON into $1
  "$PY" - "$URI" "$1" "$(rss)" <<'P'
import json, sys
from pymongo import MongoClient
w = MongoClient(sys.argv[1]).admin.command("serverStatus")["fenvix"].get("waiter", {})
w["rssBytes"] = int(sys.argv[3])
json.dump(w, open(sys.argv[2], "w"), indent=1, default=str)
P
}

for step in $STEPS; do
  echo "=== $step $(date -u +%H:%M:%S)" | tee -a "$OUT/steps.log"
  case $step in
    load)
      "$PY" - "$BUCKET" <<'P'
import boto3, sys
s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
try: s3.create_bucket(Bucket=sys.argv[1])
except Exception as e: print("bucket:", e)
P
      start --sst-gc-secs 30 --sst-gc-min-unref-secs 90 --writer-poll-secs 30
      t0=$(date +%s)
      "$PY" bench/gp3_floor/cold_customer.py load --uri "$URI" --gb "$GB" --state "$OUT/state.json" | tee "$OUT/load.txt"
      "$PY" - "$URI" <<'P' | tee -a "$OUT/load.txt"
import sys, time
from pymongo import MongoClient
c = MongoClient(sys.argv[1])
quiet_since, t0 = None, time.time()
while time.time() - t0 < 3600:
    if c.admin.command("serverStatus")["fenvix"]["compaction"]["running"] == 0:
        quiet_since = quiet_since or time.time()
        if time.time() - quiet_since >= 20: break
    else:
        quiet_since = None
    time.sleep(1)
print(f"settled after {time.time() - t0:.0f} s")
P
      # let the SST cleanup remove what compaction replaced (min unreferenced age 90 s, every 30 s)
      sleep 180
      echo "load + settle $(( $(date +%s) - t0 )) s" | tee -a "$OUT/load.txt"
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
      ;;
    base)
      rm -rf "$WORK/floor"; mkdir -p "$WORK/floor"
      start $WAITER --counter-disk-dir "$WORK/floor" --counter-disk-gb 20
      "$PY" bench/gp3_floor/cold_customer.py measure --uri "$URI" --state "$OUT/state.json" --n "$N" --warmup "$WARMUP" --seed "$SEED" --out "$OUT/base.json" | tee "$OUT/base.txt"
      snap "$OUT/base-status.json"; stop; rm -rf "$WORK/floor" ;;
    fill)
      rm -rf "$WORK/mirror"
      start $WAITER --mirror-dir "$WORK/mirror" --mirror-fresh ${FILL_FLAGS:-}
      "$PY" - "$URI" "$OUT/fill.json" <<'P' | tee "$OUT/fill.txt"
import json, sys, time
from pymongo import MongoClient
c = MongoClient(sys.argv[1])
w = lambda: c.admin.command("serverStatus")["fenvix"]["waiter"]
r0 = w()["reads"]
t0 = time.time()
c["chat"]["messages"].find_one({"userId": -1})     # opens the database: the maps loop sees its manifest and fills the mirror
while True:
    x = w()
    m = x["mirror"]
    if x["maps"]["sstsMapped"] > 0 and m["filling"] == 0 and m["files"] >= x["maps"]["sstsMapped"]:
        break
    time.sleep(0.2)
secs = time.time() - t0
out = {"secs": secs, "files": m["files"], "bytes": m["bytes"], "indexBytes": m["indexBytes"], "filled": m["filled"], "filledBytes": m["filledBytes"],
       "fillFails": m["fillFails"], "lastFillMs": m["lastFillMs"], "readsDuringFill": x["reads"] - r0, "sstsMapped": x["maps"]["sstsMapped"]}
json.dump(out, open(sys.argv[2], "w"), indent=1)
print(f"mirror filled: {m['files']} SSTs, {m['bytes'] / 2**30:.2f} GiB in {secs:.1f} s ({m['bytes'] / 1e6 / secs:.0f} MB/s), "
      f"{out['readsDuringFill']} object-store reads, mirror bookkeeping {m['indexBytes']} bytes")
P
      snap "$OUT/fill-status.json"; stop ;;
    mirror)
      rm -rf "$WORK/floor"; mkdir -p "$WORK/floor"
      start $WAITER --counter-disk-dir "$WORK/floor" --counter-disk-gb 20 --mirror-dir "$WORK/mirror"
      "$PY" bench/gp3_floor/cold_customer.py measure --uri "$URI" --state "$OUT/state.json" --n "$N" --warmup "$WARMUP" --seed "$SEED" --out "$OUT/mirror.json" | tee "$OUT/mirror.txt"
      snap "$OUT/mirror-status.json"; stop; rm -rf "$WORK/floor" ;;
  esac
done
echo "=== done $(date -u +%H:%M:%S)" | tee -a "$OUT/steps.log"
