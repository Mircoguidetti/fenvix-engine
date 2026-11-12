#!/bin/bash
# Step 9 · the filler machine (only for data "100gb" and "1tb"): loads the ballast through cook 0 at the same time as
# the client loads the Atlas databases, then puts the ballast state files into the pantry (step9-ballast-states/) for
# the ballast rounds, writes filler/end.json and powers off. The runner switches cook 0 off only when both are done.
# Usage: filler.sh BUCKET
set -u
cd "$(dirname "$0")" || exit 1
BUCKET=$1; P=filler
source ./common.sh
SCRIPT_MD5=8396ca455557baaa4e3a9b1f881451f9
finish() {
  step "finish: $1"; [ -n "${CPU_PID:-}" ] && kill "$CPU_PID" 2>/dev/null
  printf '{"finished": true, "outcome": "%s"}\n' "$1" > out/end.json
  push; push; $PY s3io.py put "$BUCKET" filler/end.json out/end.json
  power_off "filler finished"
}
( while sleep "$PUSH_S"; do push; done ) & PUSHER=$!
step "filler start; $(nproc) vCPU"
$PY cpu_macchina.py out/cpu-machine.log 5 & CPU_PID=$!
[ "$(md5sum prova_infinite.py | cut -d' ' -f1)" = "$SCRIPT_MD5" ] || finish SCRIPT_CHANGED
CM_PW=$(text secret/pw) && [ -n "$CM_PW" ] || finish NO_PASSWORD
$PY s3io.py get "$BUCKET" filler/plan.json out/plan.json || finish NO_PLAN
PANTRY=$($PY -c "import json; print(json.load(open('out/plan.json'))['pantry'])")
for _ in $(seq 1 360); do exists server-0/ready.json && break; exists client/enough && finish NO_LOADER; sleep 10; done
r=$(text server-0/ready.json) || finish NO_LOADER
ip=$($PY -c "import json,sys; print(json.loads(sys.argv[1])['ip'])" "$r"); port=$($PY -c "import json,sys; print(json.loads(sys.argv[1])['port'])" "$r")
export FENVIX_P1_URI="mongodb://root:$CM_PW@$ip:$port/?replicaSet=rs0&tls=true&tlsCAFile=$PWD/ca.pem&authSource=admin"
$PY wait_db.py 15 >> out/steps.log 2>&1 || finish SERVER_UNREACHABLE
step "ballast: $($PY -c "import json; p=json.load(open('out/plan.json')); print(len(p['lines']), 'databases,', p['parallel'], 'at once')")"
$PY ballast.py out/plan.json work out/ballast > out/ballast.txt 2>&1; rc=$?
step "$(tail -1 out/ballast.txt)"
for d in work/*/; do n=$(basename "$d"); [ -f "$d/risultati_p1.json" ] && $PY s3io.py put "$PANTRY" "step9-ballast-states/$n/risultati_p1.json" "$d/risultati_p1.json"; done
kill "$PUSHER" 2>/dev/null
[ "$rc" = 0 ] && finish BALLAST_LOADED || finish BALLAST_FAILED
