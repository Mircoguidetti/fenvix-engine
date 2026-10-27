#!/bin/bash
# usage: hang_repro.sh <binary> <store> <secs> <tag> [extra server args]
BIN=$1; STORE=$2; SECS=$3; TAG=$4; shift 4
D=/tmp/hang${PORT:+-$PORT}; mkdir -p $D; (exec 3<>/dev/tcp/127.0.0.1/${PORT:-28031}) 2>/dev/null && { echo "port ${PORT:-28031} busy"; exit 1; }
rm -rf /tmp/hang-store${PORT:+-$PORT}
$BIN --listen 127.0.0.1:${PORT:-28031} --idle-secs 3600 --holder H --store $STORE --local-dir /tmp/hang-store${PORT:+-$PORT} --no-leases "$@" > $D/$TAG.log 2>&1 &
P=$!
sleep 1
${LOADGEN:-target/release/loadgen} load --uris mongodb://127.0.0.1:${PORT:-28031} --prefix h --tenants 1 --first 0 --out /tmp/hang-load.json --close-after false > /dev/null
${LOADGEN:-target/release/loadgen} hot --uris mongodb://127.0.0.1:${PORT:-28031} --prefix h --tenant 0 --connections 64 --read-pct 90 --duration-secs $SECS --warmup-secs 3 --out $D/$TAG-hot.json > /dev/null
kill -TERM $P
for i in $(seq 1 60); do kill -0 $P 2>/dev/null || { echo "$TAG exited after ${i}s"; tail -3 $D/$TAG.log | cut -c1-200; exit 0; }; sleep 1; done
echo "$TAG HUNG (60 s after SIGTERM) pid $P"
