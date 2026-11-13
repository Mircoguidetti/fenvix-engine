#!/bin/bash
# Step 9 · the MongoDB line's client (aws-local-round, 2026-11-13): the same data and the same requests as our line,
# on the 3-member mongod replica set (mongod.sh), so first_next_compare.py can put the two side by side.
#   1. the founder's script, unchanged (md5), loads the chosen databases through kitchen/load.py exactly as our load
#      does (the Atlas load plan's lines of those states, stopped at the fraction Atlas reached), and, if asked, the
#      ballast database through the script's own "carica" line (decide.py's ballast line: same seed, customers, size);
#   2. the cold restart: every member stops, drops its page cache and starts again (serve_cold_restart), then a primary;
#   3. the programs, as extras.sh runs them on our cook and with the same arguments: first-next-ops (counters: mongod's
#      WiredTiger pages and bytes read into the cache), reads-after-writes, first-next-ops again (label after-writes)
#      and the comparison after/before; with the same state files, seed and labels, so the customers are the same.
# Raw data kept (RULES): status of the set after the load and at the end, load terminal, load seconds, every program's files.
# Usage: mongo_client.sh BUCKET [NAME]   (NAME: mongoclient)
set -u
cd "$(dirname "$0")" || exit 1
BUCKET=$1; P=${2:-mongoclient}
source ./common.sh
SCRIPT_MD5=8396ca455557baaa4e3a9b1f881451f9
outcomes=()
finish() {
  step "finish: $1"; [ -n "${CPU_PID:-}" ] && kill "$CPU_PID" 2>/dev/null; [ -n "${PUSHER:-}" ] && kill "$PUSHER" 2>/dev/null
  printf '{"finished": true, "outcome": "%s", "programs": "%s"}\n' "$1" "${outcomes[*]:-}" > out/end.json
  push; push; $PY s3io.py put "$BUCKET" "$P/end.json" out/end.json
  power_off "mongo client finished"
}
( while sleep "$PUSH_S"; do push; done ) & PUSHER=$!
step "mongod line client start; $(nproc) vCPU"
$PY cpu_macchina.py out/cpu-machine.log 5 & CPU_PID=$!
[ "$(md5sum prova_infinite.py | cut -d' ' -f1)" = "$SCRIPT_MD5" ] || finish SCRIPT_CHANGED
CM_PW=$(text secret/pw) && [ -n "$CM_PW" ] || finish NO_PASSWORD
export CM_PW
for _ in $(seq 1 60); do exists $P/config.json && break; sleep 5; done
$PY s3io.py get "$BUCKET" $P/config.json out/config.json || finish NO_CONFIG
cfg() { $PY cfg.py out/config.json "$1"; }
PROGRAMS=$(cfg programs)
has() { grep -q "\"$1\"" <<<"$PROGRAMS"; }
for _ in $(seq 1 120); do exists mongod/ready.json && break; sleep 5; done
$PY s3io.py get "$BUCKET" mongod/ready.json out/mongod-ready.json || finish SET_NOT_READY
HOSTS=$($PY -c "import json,sys; print(','.join(json.load(open(sys.argv[1]))['members']))" out/mongod-ready.json)
URI="mongodb://root:$CM_PW@$HOSTS/?replicaSet=rsm&tls=true&tlsCAFile=$PWD/ca.pem&authSource=admin"
FENVIX_P1_URI=$URI $PY wait_db.py 15 >> out/steps.log 2>&1 || finish SET_UNREACHABLE
step "the set answers: $HOSTS"
run_prog() { # as extras.sh: one program, its terminal in out/NAME.txt, outcome from its last line
  local name=$1; shift; local t0; t0=$(date +%s)
  step "program $name starts"
  "$@" > "out/$name.txt" 2>&1; local rc=$?
  outcomes+=("$name:rc=$rc")
  step "program $name: code $rc in $(( $(date +%s) - t0 )) s: $(tail -1 "out/$name.txt" | cut -c1-200)"
  push
}
snapshot() { # snapshot FILE: dbStats, collStats, serverStatus and the set's status through the client
  $PY - "$URI" "$1" <<'PY' >> out/steps.log 2>&1
import json, sys
import pymongo
c = pymongo.MongoClient(sys.argv[1], serverSelectionTimeoutMS=30000)
r = {"serverStatus": c.admin.command("serverStatus"), "replSetGetStatus": c.admin.command("replSetGetStatus"), "dbs": {}}
for d in c.list_database_names():
    if d in ("admin", "local", "config"):
        continue
    r["dbs"][d] = {"dbStats": c[d].command("dbStats"), "collStats": {n: c[d].command("collStats", n) for n in c[d].list_collection_names()}}
json.dump(r, open(sys.argv[2], "w"), default=str, indent=1)
print(f"snapshot {sys.argv[2]}: " + ", ".join(f"{d} {v['dbStats'].get('dataSize', 0) / 2**30:.2f} GiB" for d, v in r["dbs"].items()))
PY
}
# 1. the data
mkdir -p atlas fn-states/atlas fn-states/ballast && tar xzf atlas-states.tar.gz -C atlas || finish NO_ATLAS_STATES
cp -r atlas/. fn-states/atlas/
STATES=$(cfg mongod_states)
$PY - "$STATES" load-plan.txt mongod-plan.txt <<'PY' || finish BAD_PLAN
import json, sys
want = set(json.loads(sys.argv[1]) or [])
rows = [r for r in open(sys.argv[2]) if r.strip() and not r.lstrip().startswith("#") and r.split()[0] in want]
if not rows:
    sys.exit(f"no line of the load plan for {sorted(want)}")
open(sys.argv[3], "w").write("".join(rows))
print(f"{len(rows)} lines for {sorted(want)}")
PY
step "load: $(wc -l < mongod-plan.txt) lines of the load plan for $STATES, through the founder's script, into the set"
t0=$(date +%s)
FENVIX_P1_URI=$URI $PY load.py mongod-plan.txt atlas work/load out > out/load.txt 2>&1
rc=$?
echo "$(( $(date +%s) - t0 ))" > out/load-seconds.txt
step "load finished with code $rc in $(( $(date +%s) - t0 )) s: $(tail -1 out/load.txt)"
[ "$rc" = 0 ] || finish LOAD_FAILED
BLINE=$(cfg mongod_ballast_line)
if [ -n "$BLINE" ] && [ "$BLINE" != "[]" ]; then
  # the ballast database through the script's own load (the line decide.py makes for the "script" path)
  mkdir -p ballast-load && cp prova_infinite.py ballast-load/
  ARGS=$($PY -c "import json,sys; print(' '.join(json.loads(sys.argv[1])[1:]))" "$BLINE")
  BDB=$($PY -c "import json,sys; print(json.loads(sys.argv[1])[0])" "$BLINE")
  step "ballast: prova_infinite.py carica --db $BDB $ARGS"
  t0=$(date +%s)
  # shellcheck disable=SC2086
  (cd ballast-load && FENVIX_P1_URI=$URI $PY prova_infinite.py carica --file risultati_p1.json --db "$BDB" $ARGS) > out/ballast-load.txt 2>&1
  rc=$?
  echo "$(( $(date +%s) - t0 ))" > out/ballast-load-seconds.txt
  step "ballast load finished with code $rc in $(( $(date +%s) - t0 )) s: $(tail -1 out/ballast-load.txt | cut -c1-200)"
  [ "$rc" = 0 ] || finish BALLAST_LOAD_FAILED
  mkdir -p "fn-states/ballast/$BDB"
  cp ballast-load/risultati_p1.json "fn-states/ballast/$BDB/" || finish NO_BALLAST_STATE
  cp ballast-load/risultati_p1.json out/ballast-state.json
fi
snapshot out/after-load.json
push
# 2. the cold restart of the three members, then a primary
ID="cold-$(date +%s)"
for m in mongo-0 mongo-1 mongo-2; do printf '%s' "$ID" | $PY s3io.py put "$BUCKET" "$m/restart-request" - ; done
t0=$(date +%s)
for m in mongo-0 mongo-1 mongo-2; do
  until exists "$m/restarted-$ID"; do [ $(( $(date +%s) - t0 )) -ge 900 ] && finish COLD_RESTART_TIMEOUT; sleep 5; done
done
$PY - "$URI" <<'PY' >> out/steps.log 2>&1 || finish NO_PRIMARY_AFTER_RESTART
import sys, time
import pymongo
c = pymongo.MongoClient(sys.argv[1], serverSelectionTimeoutMS=300000)
t0 = time.time()
c.admin.command("ping")
while sorted(m["stateStr"] for m in c.admin.command("replSetGetStatus")["members"]) != ["PRIMARY", "SECONDARY", "SECONDARY"]:
    if time.time() - t0 > 300:
        sys.exit("the set did not come back")
    time.sleep(2)
print(f"after the cold restart: a primary and two secondaries in {time.time() - t0:.0f} s")
PY
step "cold restart $ID: the three members restarted with their page cache dropped ($(for m in mongo-0 mongo-1 mongo-2; do text "$m/restarted-$ID" | tr -d '\n '; done))"
# 3. the programs, as extras.sh runs them on our cook
FN_DB=$(cfg fn_db)
if has first-next-ops; then
  # shellcheck disable=SC2046
  run_prog first-next-ops $PY first_next_ops.py out/first-next-ops --uri "$URI" --states fn-states ${FN_DB:+--db "$FN_DB"} \
    --counters mongod --label before-writes $(cfg first_next_ops_args)
fi
if has reads-after-writes; then
  # shellcheck disable=SC2046
  run_prog reads-after-writes $PY reads_after_writes.py out/reads-after-writes --uri "$URI" --states fn-states ${FN_DB:+--db "$FN_DB"} \
    $(cfg reads_after_writes_args)
  if has first-next-ops; then
    # shellcheck disable=SC2046
    run_prog first-next-ops-after-writes $PY first_next_ops.py out/first-next-ops-after-writes --uri "$URI" --states fn-states \
      ${FN_DB:+--db "$FN_DB"} --counters mongod --label after-writes $(cfg first_next_ops_args)
    [ -s out/first-next-ops/first-next-ops-summary.json ] && $PY first_next_compare.py out/first-next-ops-after-writes/first-next-ops-summary.json \
      out/first-next-ops/first-next-ops-summary.json out/first-next-ops-after-writes/against-before-writes.json --their-name "before the writes" \
      > out/first-next-ops-after-writes/against-before-writes.txt 2>&1
  fi
fi
snapshot out/at-end.json
step "programs done: ${outcomes[*]:-none}"
finish MONGOD_LINE_DONE
