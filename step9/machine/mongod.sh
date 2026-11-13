#!/bin/bash
# Step 9 · the MongoDB line (aws-local-round, 2026-11-13): one member of a 3-member mongod 7.0 replica set, the
# competitor for the five requests of first-next-ops and for reads-after-writes, on the same data as our cook.
# What Atlas gives (RULES, fair comparison): three members in three zones (mongo-0 in zone a with priority 2, so the
# primary is where our cook is; mongo-1 in b, mongo-2 in c), writes acknowledged by the majority with the journal (7.0's
# default: journal always on, writeConcernMajorityJournalDefault true; the client asks w: majority), TLS required
# (the round's CA, a certificate for this machine's private IP), users and member authentication (keyfile derived from
# the round password, never written in the bucket), WiredTiger's default cache (50% of memory minus 1 GB, as Atlas).
# The binary: ./mongod in the node package (MONGOD_BIN of packages.sh: 7.0.48; locally the mongo:7.0 image's binary,
# on AWS the runner downloads the official tarball and checks its sha256). Flow:
#   start -> results/<NAME>/up.json -> mongo-0 initiates the set when the three are up, creates root (localhost
#   exception), waits for a primary -> mongod/ready.json -> loop: the cold restart (serve_cold_restart: stop mongod,
#   drop the page cache, start it, wait until it is in the set again) -> stop: status, sizes, power off.
# Usage: mongod.sh NAME BUCKET   (NAME mongo-0, mongo-1, mongo-2)
set -u
cd "$(dirname "$0")" || exit 1
NAME=$1; BUCKET=$2; P=$NAME
source ./common.sh
K=${NAME##*-}
DB=${MONGOD_DBPATH:-/var/lib/s9-mongod}
PORT=27017
MPID=""
mongod_stop() {
  [ -n "$MPID" ] || return 0
  kill -INT "$MPID" 2>/dev/null
  for _ in $(seq 1 120); do kill -0 "$MPID" 2>/dev/null || break; sleep 1; done
  kill -0 "$MPID" 2>/dev/null && { step "mongod did not stop in 120 s: killed"; kill -9 "$MPID"; }
  MPID=""
}
status() { # status FILE: serverStatus, replSetGetStatus, getCmdLineOpts, hostInfo, listDatabases + dbStats (raw data, RULES)
  $PY - "$IP" "$PORT" cert/ca.pem "$1" <<'PY' >> out/steps.log 2>&1
import json, os, sys
import pymongo
ip, port, ca, out = sys.argv[1:5]
c = pymongo.MongoClient(f"mongodb://root:{os.environ['CM_PW']}@{ip}:{port}/?tls=true&tlsCAFile={ca}&authSource=admin&directConnection=true",
                        serverSelectionTimeoutMS=10000)
r = {}
for k, cmd in (("serverStatus", "serverStatus"), ("replSetGetStatus", "replSetGetStatus"), ("getCmdLineOpts", "getCmdLineOpts"),
               ("hostInfo", "hostInfo"), ("buildInfo", "buildInfo")):
    try:
        r[k] = c.admin.command(cmd)
    except Exception as e:  # noqa: BLE001
        r[k] = {"error": f"{type(e).__name__}: {e}"}
try:
    r["dbStats"] = {d: c[d].command("dbStats") for d in c.list_database_names()}
except Exception as e:  # noqa: BLE001
    r["dbStats"] = {"error": f"{type(e).__name__}: {e}"}
json.dump(r, open(out, "w"), default=str, indent=1)
print(f"status saved: {out}")
PY
}
finish() {
  step "finish: $1"; [ -n "${CPU_PID:-}" ] && kill "$CPU_PID" 2>/dev/null
  [ -n "$MPID" ] && status out/status-end.json
  mongod_stop
  du -sb "$DB" > out/dbpath-bytes.txt 2>/dev/null
  cp "$DB"/../mongod.log out/mongod.log 2>/dev/null; gzip -f out/mongod.log 2>/dev/null
  printf '{"finished": true, "outcome": "%s"}\n' "$1" > out/stopped.json
  push; push; $PY s3io.py put "$BUCKET" "$P/stopped" out/stopped.json 2>/dev/null
  power_off "mongod stopped"
}
IP=$(private_ip); TYPE=$(machine_type); AZ=$(zone)
step "mongod member $K: $TYPE in $AZ, private IP $IP, $(nproc) vCPU, $(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo) MiB"
$PY cpu_macchina.py out/cpu-machine.log 5 & CPU_PID=$!
[ -x ./mongod ] || finish NO_MONGOD
./mongod --version > out/version.txt 2>&1 || { cat out/version.txt >> out/steps.log; finish MONGOD_DOES_NOT_RUN; }
grep -q 'db version v7\.0\.' out/version.txt || { step "not mongod 7.0: $(head -1 out/version.txt)"; finish NOT_MONGOD_7_0; }
sha256sum mongod | cut -d' ' -f1 > out/mongod-sha256.txt
CM_PW=$(text secret/pw) && [ -n "$CM_PW" ] || finish NO_PASSWORD
export CM_PW
mkdir -p cert
for _ in $(seq 1 60); do exists "$P/server.pem" && break; sleep 5; done
$PY s3io.py get "$BUCKET" "$P/server.pem" cert/server.pem && $PY s3io.py get "$BUCKET" program/ca.pem cert/ca.pem || finish NO_CERTIFICATE
chmod 600 cert/server.pem
# member authentication: a keyfile the three members derive from the round password (base64 alphabet, 6-1024 chars)
printf 'step9-mongod-keyfile:%s' "$CM_PW" | sha256sum | cut -c1-64 > cert/keyfile; chmod 400 cert/keyfile
mkdir -p "$DB"
mongod_start() {
  # TLS required on every connection (the members' own too); clients without a certificate are allowed, as on Atlas
  nohup ./mongod --replSet rsm --bind_ip_all --port "$PORT" --dbpath "$DB" --logpath "$DB/../mongod.log" --logappend \
    --tlsMode requireTLS --tlsCertificateKeyFile cert/server.pem --tlsCAFile cert/ca.pem \
    --tlsAllowConnectionsWithoutCertificates --keyFile cert/keyfile > out/mongod.out 2>&1 &
  MPID=$!
  for _ in $(seq 1 180); do
    $PY - "$IP" "$PORT" cert/ca.pem <<'PY' > /dev/null 2>&1 && return 0
import sys, pymongo
ip, port, ca = sys.argv[1:4]
h = pymongo.MongoClient(f"mongodb://{ip}:{port}/?tls=true&tlsCAFile={ca}&directConnection=true", serverSelectionTimeoutMS=2000).admin.command("hello")
# up, and (once the set exists) back in it as primary or secondary
sys.exit(0 if (not h.get("setName") or h.get("isWritablePrimary") or h.get("secondary")) else 1)
PY
    kill -0 "$MPID" 2>/dev/null || { tail -20 "$DB/../mongod.log" >> out/steps.log 2>/dev/null; return 1; }
    sleep 1
  done
  return 1
}
mongod_start || finish MONGOD_DID_NOT_START
step "mongod up on $IP:$PORT (TLS required, keyfile, journal on, replica set rsm)"
put_json "results/$P/up.json" "{\"ip\": \"$IP\", \"az\": \"$AZ\"}"
if [ "$K" = 0 ]; then
  # the set: the three members by private IP (certificates for those IPs), mongo-0 preferred as primary (zone a)
  for _ in $(seq 1 120); do exists results/mongo-1/up.json && exists results/mongo-2/up.json && break; sleep 5; done
  IP1=$($PY -c "import json,sys; print(json.loads(sys.argv[1])['ip'])" "$(text results/mongo-1/up.json)") || finish NO_MEMBER_1
  IP2=$($PY -c "import json,sys; print(json.loads(sys.argv[1])['ip'])" "$(text results/mongo-2/up.json)") || finish NO_MEMBER_2
  $PY - "$IP" "$IP1" "$IP2" "$PORT" cert/ca.pem <<'PY' >> out/steps.log 2>&1 || finish SET_NOT_INITIATED
import json, os, sys, time
import pymongo
ip0, ip1, ip2, port, ca = sys.argv[1:6]
port = int(port)
# before any user exists, only 127.0.0.1 has the localhost exception (MEASURED locally: over the private IP the
# initiate is refused with "requires authentication"); the certificate is for the IP: hostname check off on this link
loc = pymongo.MongoClient(f"mongodb://127.0.0.1:{port}/?tls=true&tlsCAFile={ca}&tlsAllowInvalidHostnames=true&directConnection=true",
                          serverSelectionTimeoutMS=10000)
cfg = {"_id": "rsm", "members": [{"_id": 0, "host": f"{ip0}:{port}", "priority": 2},
                                 {"_id": 1, "host": f"{ip1}:{port}", "priority": 1},
                                 {"_id": 2, "host": f"{ip2}:{port}", "priority": 1}]}
print(loc.admin.command("replSetInitiate", cfg))
t0 = time.time()
while not loc.admin.command("hello").get("isWritablePrimary"):
    if time.time() - t0 > 300:
        sys.exit("no primary in 300 s")
    time.sleep(1)
loc.admin.command("createUser", "root", pwd=os.environ["CM_PW"], roles=["root"], writeConcern={"w": "majority"})
c = pymongo.MongoClient(f"mongodb://root:{os.environ['CM_PW']}@{ip0}:{port},{ip1}:{port},{ip2}:{port}/?replicaSet=rsm&tls=true&tlsCAFile={ca}&authSource=admin",
                        serverSelectionTimeoutMS=60000)
t0 = time.time()
while True:
    st = c.admin.command("replSetGetStatus")
    states = sorted(m["stateStr"] for m in st["members"])
    if states == ["PRIMARY", "SECONDARY", "SECONDARY"]:
        break
    if time.time() - t0 > 300:
        sys.exit(f"members not all up in 300 s: {states}")
    time.sleep(2)
print("set rsm:", states, "write concern majority default:", c.admin.command("getDefaultRWConcern").get("defaultWriteConcern"))
PY
  status out/status-ready.json
  printf '{"members": ["%s:%s", "%s:%s", "%s:%s"], "set": "rsm", "primary_preferred": "%s", "type": "%s", "durability": "3 members in 3 zones, w majority, journal", "tls": "required"}\n' \
    "$IP" "$PORT" "$IP1" "$PORT" "$IP2" "$PORT" "$IP" "$TYPE" > out/ready.json
  $PY s3io.py put "$BUCKET" "mongod/ready.json" out/ready.json || finish NO_BUCKET
  step "replica set ready: $(tr -d '\n' < out/ready.json | cut -c1-200)"
fi
push
while :; do sleep "$PUSH_S"; serve_cold_restart mongod_stop mongod_start; push; exists "$P/stop" && finish STOPPED; done
