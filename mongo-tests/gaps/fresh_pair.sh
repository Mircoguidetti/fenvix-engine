#!/usr/bin/env bash
# A throwaway pair for tests that change the server for good (setFeatureCompatibilityVersion, shutdown): a real mongod 7.0.45 standalone and an
# engine standalone, both on empty directories.   usage: fresh_pair.sh start <tag> | stop <tag>     (start prints "<real port> <engine port>")
set -u
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
TAG=${2:?tag}
case "$TAG" in fcv) RP=27061; OP=27062;; sd) RP=27063; OP=27064;; wb) RP=27065; OP=27066;; *) echo "unknown tag" >&2; exit 2;; esac
M=$(ls -d $ROOT/.tools/mongodb-linux-*/bin)/mongod
PIDS=$ROOT/mongo-tests/out/fresh_$TAG.pids
mkdir -p "$ROOT/mongo-tests/out"
stop_pid() { [ -n "${1:-}" ] && kill -0 "$1" 2>/dev/null && kill "$1" 2>/dev/null; for _ in $(seq 1 50); do kill -0 "$1" 2>/dev/null || return 0; sleep 0.2; done; kill -9 "$1" 2>/dev/null; return 0; }
if [ "$1" = stop ]; then
  if [ -f "$PIDS" ]; then read -r A B < "$PIDS"; stop_pid "$A"; stop_pid "$B"; rm -f "$PIDS"; fi
  exit 0
fi
[ -f "$PIDS" ] && "$0" stop "$TAG"
STAMP=$(date +%s%N)
D=$ROOT/.tools/data/fresh_${TAG}_$STAMP
mkdir -p "$D" && "$M" --port $RP --dbpath "$D" --bind_ip 127.0.0.1 --fork --logpath "$D.log" --pidfilepath "$D.pid" > /dev/null 2>&1 || { echo "mongod failed" >&2; exit 1; }
(cd "$ROOT/mongo-tests" && SERVER_EXTRA="--ttl-sweep-secs 1 --store local --local-dir /tmp/fresh-$TAG-$STAMP --no-leases" ./server.sh $OP 600 > /dev/null 2>&1) || { echo "engine failed" >&2; exit 1; }
echo "$(cat "$D.pid") $(cat "$ROOT/mongo-tests/out/server-$OP.pid")" > "$PIDS"
echo "$RP $OP"
