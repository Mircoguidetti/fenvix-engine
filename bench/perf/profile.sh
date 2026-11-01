#!/usr/bin/env bash
# profile.sh WORKLOAD LAYOUT CLIENTS [SECONDS] [LABEL]
# CPU profile of the engine (1 core, CPU 0) under one workload, started on a copy of the snapshot of bench.py
# (`bench.py snapshot LAYOUT` first). perf samples the engine at 299 Hz with DWARF call stacks (the release binary has
# no frame pointers) after 30 s of load; the Rust names are demangled with rustfilt (`cargo install rustfilt`) and the
# stacks are split by layer with layers.py.
# Output: $BENCH_RESULTS/prof-LABEL-WORKLOAD-LAYOUT.{layers.txt,script.gz}
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; BIN="${BENCH_BIN:-$HERE/.bin}"; DATA="${BENCH_DATA:-$HOME/benchdata}"
OUT="${BENCH_RESULTS:-$HERE/results}"; mkdir -p "$OUT"
PERF="${PERF:-$(ls /usr/lib/linux-tools/*/perf 2>/dev/null | head -1)}"
w=$1; layout=$2; clients=$3; secs=${4:-30}; label=${5:-base}
SERVER_BIN="${SERVER_BIN:-$BIN/mongo-server}"
if [ "$layout" = 100 ]; then dbs=100; docs=2000; else dbs=1; docs=200000; fi
pkill -x mongo-server 2>/dev/null && sleep 3 || true
pkill -9 -x mongo-server 2>/dev/null || true
rm -rf "$DATA/fenvix-store"; cp -a "$DATA/snap-$layout" "$DATA/fenvix-store"
taskset -c 0 "$SERVER_BIN" --listen 127.0.0.1:27017 --store local --local-dir "$DATA/fenvix-store" >"$DATA/fenvix-server.log" 2>&1 &
pid=$!
for _ in $(seq 1 120); do grep -q "listening on" "$DATA/fenvix-server.log" && break; sleep 0.5; done
taskset -c 1-3 "$BIN/loadgen" -mode run -workload "$w" -dbs $dbs -docs $docs -clients "$clients" -warmup $((secs + 40)) -duration 5 -pid "$pid" \
  > "$OUT/prof-$label-$w-$layout-loadgen.json" &
lg=$!
sleep 30
data="$DATA/prof.data"
taskset -c 3 "$PERF" record -F 299 --call-graph dwarf,32768 -p "$pid" -o "$data" -- sleep "$secs" 2>/dev/null
wait $lg || true
pkill -x mongo-server; sleep 3; pkill -9 -x mongo-server 2>/dev/null || true
taskset -c 1-3 "$PERF" script -i "$data" -F comm,tid,time,event,ip,sym 2>/dev/null | rustfilt | gzip > "$OUT/prof-$label-$w-$layout.script.gz"
python3 "$HERE/layers.py" "$OUT/prof-$label-$w-$layout.script.gz" --top 25 > "$OUT/prof-$label-$w-$layout.layers.txt"
rm -f "$data"
head -40 "$OUT/prof-$label-$w-$layout.layers.txt"
