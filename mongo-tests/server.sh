#!/usr/bin/env bash
# Start (or restart) the mongo-server binary: ./server.sh <port> <idle-secs> [extra args...]
# The log goes to mongo-tests/out/server-<port>.log. SIGTERM makes the server close its tenants cleanly.
set -euo pipefail
port=${1:?port}; idle=${2:?idle secs}; shift 2
cd "$(dirname "$0")/.."
mkdir -p mongo-tests/out
pidfile=mongo-tests/out/server-$port.pid
if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
  kill -TERM "$(cat "$pidfile")"
  while kill -0 "$(cat "$pidfile")" 2>/dev/null; do sleep 0.2; done
fi
nohup ./target/release/mongo-server --listen 127.0.0.1:$port --idle-secs "$idle" "$@" > mongo-tests/out/server-$port.log 2>&1 &
echo $! > "$pidfile"
for _ in $(seq 1 50); do
  if (exec 3<>/dev/tcp/127.0.0.1/$port) 2>/dev/null; then echo "mongo-server up on $port (pid $(cat "$pidfile"))"; exit 0; fi
  sleep 0.2
done
echo "mongo-server failed to start" >&2; cat mongo-tests/out/server-$port.log >&2; exit 1
