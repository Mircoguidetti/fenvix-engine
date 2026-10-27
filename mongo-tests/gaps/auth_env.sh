#!/usr/bin/env bash
# Servers with access control for the authentication tests: real mongod 7.0 --auth on 27030 and the engine --auth on 27028,
# each with the same root user (root / rootpw) created through the localhost exception.
# usage: auth_env.sh [start|stop]
set -u
SELF=$(readlink -f "$0")
cd "$(dirname "$0")/../.."
ROOT=$PWD
B=$(ls -d $ROOT/.tools/mongodb-linux-*/bin)
if [ "${1:-start}" = stop ]; then
  pkill -u mongotest -f "port 27030" ; pkill -f "[l]isten 127.0.0.1:27028"
  while pgrep -f "[l]isten 127.0.0.1:27028" > /dev/null; do sleep 0.3; done
  while pgrep -u mongotest -f "port 27030" > /dev/null; do sleep 0.3; done; sleep 2; exit 0
fi
"$SELF" stop; rm -rf $ROOT/.tools/data/auth /tmp/gaps-auth-store; mkdir -p $ROOT/.tools/data/auth; chown -R mongotest $ROOT/.tools/data/auth
su mongotest -c "ulimit -n 20000; $B/mongod --port 27030 --dbpath $ROOT/.tools/data/auth --bind_ip 127.0.0.1 --auth --fork --logpath $ROOT/.tools/data/auth.log" | tail -1
SERVER_EXTRA="--store local --local-dir /tmp/gaps-auth-store --no-leases --auth ${AUTH_EXTRA:-}" mongo-tests/server.sh 27028 600
mongo-tests/.venv/bin/python - <<'PY'
from pymongo import MongoClient
for port in (27030, 27028):
    c = MongoClient("localhost", port)
    c.admin.command("createUser", "root", pwd="rootpw", roles=[{"role": "root", "db": "admin"}])
    print("root user created on", port)
PY
