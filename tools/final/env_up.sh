#!/usr/bin/env bash
# Environment of the final-engine verification, idempotent after a machine restart, WITHOUT docker:
#   * MinIO on :9000 (tools/fusion/env_up.sh, MINIO_ONLY)
#   * the reference mongod 7.0.45 (binary extracted from the official mongo:7.0 image into /tmp/claude-0/mongo-img): standalone :27017 and
#     single-node replica set rs0 :27020 (ttlMonitorSleepSecs=1), the pair the differential suites and the gap tests compare against
#   * .tools/mongodb-linux-x86_64-7.0.45/bin -> that binary directory, and the unprivileged user `mongotest`, which the gap tests' own
#     scripts (tls_env.py, auth_env.sh) expect
set -u
cd "$(dirname "$0")/../.."
ROOT=$PWD
SRC=/tmp/claude-0/mongo-img/root/usr/bin
BIN=/tmp/mongo7-bin                      # a copy anyone can run: /tmp/claude-0 is private to root and mongotest must reach it
[ -x $SRC/mongod ] || [ -x $BIN/mongod ] || { echo "no extracted mongod in $SRC" >&2; exit 1; }
if [ ! -x $BIN/mongod ]; then mkdir -p $BIN && cp $SRC/mongod $SRC/mongodump $SRC/mongorestore $BIN/ && chmod 755 $BIN $BIN/*; fi
MINIO_ONLY=1 tools/fusion/env_up.sh > /dev/null || exit 1
id mongotest > /dev/null 2>&1 || useradd -m -s /bin/bash mongotest
mkdir -p .tools/mongodb-linux-x86_64-7.0.45 .tools/data
rm -f .tools/mongodb-linux-x86_64-7.0.45/bin; ln -s $BIN .tools/mongodb-linux-x86_64-7.0.45/bin
chown -R mongotest .tools/data 2>/dev/null
up() { (exec 3<>/dev/tcp/127.0.0.1/$1) 2>/dev/null; }
start() {  # port dbpath extra...
  local port=$1 d=$2; shift 2
  up $port && return 0
  mkdir -p $d; chown -R mongotest $d
  su mongotest -c "ulimit -n 20000; $BIN/mongod --port $port --dbpath $d --bind_ip 127.0.0.1 --setParameter ttlMonitorSleepSecs=1 $* --fork --logpath $d/mongod.log" > /dev/null 2>&1
}
start 27017 /tmp/final-mongo/sa
start 27020 /tmp/final-mongo/rs --replSet rs0
for _ in $(seq 1 60); do up 27017 && up 27020 && break; sleep 1; done
/home/user/test5-compat-indexes/mongo-tests/.venv/bin/python - <<'P'
import time, sys, pymongo
c = pymongo.MongoClient("mongodb://localhost:27020/?directConnection=true", serverSelectionTimeoutMS=20000)
try:
    c.admin.command("replSetGetStatus")
except pymongo.errors.OperationFailure:
    c.admin.command("replSetInitiate", {"_id": "rs0", "members": [{"_id": 0, "host": "localhost:27020"}]})
for _ in range(90):
    if c.admin.command("hello").get("isWritablePrimary"):
        break
    time.sleep(0.5)
else:
    sys.exit("rs0 has no primary")
s = pymongo.MongoClient("mongodb://localhost:27017/?directConnection=true", serverSelectionTimeoutMS=20000)
print("mongod standalone", s.admin.command("buildInfo")["version"], "rs0 primary", c.admin.command("hello")["isWritablePrimary"])
P
