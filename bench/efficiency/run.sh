#!/usr/bin/env bash
# run.sh SERVER LABEL [WRITE_DOCS] [WRITE_MAX_SECS] [READ_SECS]
# One measurement of the E1 study (README.md): the server on CPUs 0-1 (2 cores, like an M30), the load
# generator on CPUs 2-3. Fresh data directory, then
#   1. writes: 6 databases x 16 workers, insertMany of 1,000 (the M30 peak load), as fast as possible,
#      until WRITE_DOCS documents or WRITE_MAX_SECS seconds;
#   2. hot single-document reads (Q2, an order by _id) for READ_SECS seconds, 64 workers.
# SERVER: engine | mongod. Output in $OUT/LABEL/: write.jsonl, read.jsonl (one line per 5 s), server log, sizes.
# With PERF_WRITE_AT=S (seconds into the writes) and/or PERF_READ_AT=S, perf records the server for PERF_SECS
# seconds at 499 Hz with DWARF stacks (profile runs only: perf slows the server, do not use their numbers).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
server=$1; label=$2; wdocs=${3:-10000000}; wsecs=${4:-2400}; rsecs=${5:-150}
OUT="${OUT:-$HERE/results}"; DATA="${DATA:-/home/user/data}"; LG="${LG:-$HERE/.bin/loadgen}"
ENGINE="${ENGINE:-$HERE/.bin/mongo-server}"; MONGO_IMAGE="${MONGO_IMAGE:-mirror.gcr.io/library/mongo:9.0.4}"
PERF="${PERF:-$(ls /usr/lib/linux-tools/*/perf | head -1)}"; PERF_SECS=${PERF_SECS:-30}
d="$OUT/$label"; mkdir -p "$d"

stop_all() {
  pkill -f "$ENGINE" 2>/dev/null || true; docker rm -f mongod-e1 >/dev/null 2>&1 || true; sleep 2
  pkill -9 -f "$ENGINE" 2>/dev/null || true
}
stop_all
rm -rf "$DATA/$server"; mkdir -p "$DATA/$server"
if [ "$server" = engine ]; then
  port=27017
  taskset -c 0,1 "$ENGINE" --listen 127.0.0.1:$port --store local --local-dir "$DATA/engine" --replica-set rs0 \
    --advertise 127.0.0.1:$port ${ENGINE_ARGS:-} >"$d/server.log" 2>&1 &
  pid=$!
  for _ in $(seq 120); do grep -q "listening on" "$d/server.log" && break; sleep 0.5; done
else
  port=27019
  docker run -d --name mongod-e1 --network host --cpuset-cpus 0,1 -v "$DATA/mongod:/data/db" "$MONGO_IMAGE" \
    mongod --replSet rs0 --bind_ip 127.0.0.1 --port $port --wiredTigerCacheSizeGB "${WT_CACHE_GB:-2}" >/dev/null
  for _ in $(seq 60); do docker exec mongod-e1 mongosh --quiet --port $port --eval 1 >/dev/null 2>&1 && break; sleep 1; done
  docker exec mongod-e1 mongosh --quiet --port $port --eval \
    'rs.initiate({_id:"rs0",members:[{_id:0,host:"127.0.0.1:'$port'"}]})' >/dev/null
  for _ in $(seq 60); do [ "$(docker exec mongod-e1 mongosh --quiet --port $port --eval 'db.hello().isWritablePrimary')" = true ] && break; sleep 1; done
  pid=$(docker inspect -f '{{.State.Pid}}' mongod-e1)
  docker exec mongod-e1 mongod --version | head -1 > "$d/server-version.txt"
fi
uri="mongodb://127.0.0.1:$port/?directConnection=true"
echo "server $server pid $pid" | tee "$d/info.txt"
nproc >> "$d/info.txt"; free -m | head -2 >> "$d/info.txt"; lscpu | grep -E "Model name|^CPU\(s\)" >> "$d/info.txt"

perf_at() { # perf_at SECONDS NAME
  sleep "$1"; taskset -c 3 "$PERF" record -F 499 --call-graph dwarf,16384 -p "$pid" -o "$d/$2.perf.data" -- sleep "$PERF_SECS" \
    >/dev/null 2>&1 || true
}
pj=""; if [ -n "${PERF_WRITE_AT:-}" ]; then perf_at "$PERF_WRITE_AT" write & pj=$!; fi
taskset -c 2,3 "$LG" -uri "$uri" -mode write -dbs 6 -workers 16 -max-docs "$wdocs" -duration "$wsecs" -pid "$pid" ${LG_WRITE_ARGS:-} \
  -label "$label" > "$d/write.jsonl" 2> "$d/write.err"
[ -n "$pj" ] && wait "$pj"
du -sm "$DATA/$server" > "$d/disk-after-write.txt"
status() { # status FILE: serverStatus (the engine's own counters under "fenvix") and dbStats of the six databases
  docker run --rm --network host "$MONGO_IMAGE" mongosh --quiet "$uri" --eval \
    'const s=db.adminCommand({serverStatus:1}); let o={fenvix:s.fenvix, opcounters:s.opcounters, mem:s.mem, wiredTiger_cache:(s.wiredTiger||{}).cache}; for (let i=1;i<=6;i++){const x=db.getSiblingDB("app_data_"+i).stats(); o["app_data_"+i]={objects:x.objects,dataSize:x.dataSize,storageSize:x.storageSize,indexSize:x.indexSize}}; print(EJSON.stringify(o,null,1))' \
    > "$1" 2>&1 || true
}
status "$d/status-after-write.json"
# Before the reads, wait until the server is quiet (CPU under 5% of a core for 20 s, at most QUIET_MAX s): the
# compaction left by the writes is write work and must not be counted in the reads.
quiet_wait() {
  local t0=$(date +%s) calm=0 prev cur first
  prev=$(awk '{print $14+$15}' "/proc/$pid/stat"); first=$prev
  while [ $(( $(date +%s) - t0 )) -lt "${QUIET_MAX:-600}" ] && [ $calm -lt 4 ]; do
    sleep 5; cur=$(awk '{print $14+$15}' "/proc/$pid/stat")
    if [ $(( cur - prev )) -lt 25 ]; then calm=$((calm+1)); else calm=0; fi
    prev=$cur
  done
  echo "quiet after $(( $(date +%s) - t0 )) s, server CPU in the wait $(( cur - first )) ticks (1/100 s)" > "$d/quiet.txt"
}
quiet_wait
pj=""; if [ -n "${PERF_READ_AT:-}" ]; then perf_at "$PERF_READ_AT" read & pj=$!; fi
taskset -c 2,3 "$LG" -uri "$uri" -mode read -dbs 6 -workers 64 -duration "$rsecs" -pid "$pid" -label "$label" \
  > "$d/read.jsonl" 2> "$d/read.err"
[ -n "$pj" ] && wait "$pj"
status "$d/status-after-read.json"
[ "$server" = mongod ] && { docker logs mongod-e1 2>&1 | tail -200 > "$d/server.log" || true; }
stop_all
[ -n "${KEEP_DATA:-}" ] || rm -rf "$DATA/$server"
echo done
