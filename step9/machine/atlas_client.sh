#!/bin/bash
# Step 9 · the Atlas line's client (founder's decisions, 2026-11-13): one machine in eu-west-1a (the Ireland round's
# client zone) against the founder's Atlas cluster, with the same programs as our line.
#   atlas-load      the founder's script, unchanged (md5), loads the chosen Atlas-shaped states through kitchen/load.py
#                   (as our load and the mongod line do) and, if asked, the generated-size database through the script's
#                   own "carica" line (decide.py's: the same seed as our generator, so the same customers). The script's
#                   load cannot resume: it runs at most the job's time left (--max-ore-carico) and records where it
#                   stopped. Every state file it writes goes into the kept pantry (atlas-states/<db>/), where the next
#                   Atlas job reads it (the Atlas cluster keeps the data between jobs; our machines do not).
#   first-next      programs/first_next.py --counters none (Atlas has no tenantStats), on the command's database
#   ballast-rounds  the founder's script's read rounds (Q2, Q3, Q1, Q1d, Q4) through code-irlanda's programma.py, on the
#                   command's database, against the state file of Atlas's own load
#   atlas-ops       programs/atlas_ops.py: Test Failover, restore to another cluster, restore to a past time (Admin API)
# Secrets: the runner puts the Atlas connection strings and the API key into the run bucket (secret/atlas.json); this
# machine reads them into files of mode 600 and deletes the object at once. Never in a log.
# Usage: atlas_client.sh BUCKET [NAME]   (NAME: atlasclient)
set -u
cd "$(dirname "$0")" || exit 1
BUCKET=$1; P=${2:-atlasclient}
source ./common.sh
SCRIPT_MD5=8396ca455557baaa4e3a9b1f881451f9
outcomes=()
finish() {
  step "finish: $1"; [ -n "${CPU_PID:-}" ] && kill "$CPU_PID" 2>/dev/null; [ -n "${PUSHER:-}" ] && kill "$PUSHER" 2>/dev/null
  rm -f secret-atlas.json atlas-key
  printf '{"finished": true, "outcome": "%s", "programs": "%s"}\n' "$1" "${outcomes[*]:-}" > out/end.json
  push; push; $PY s3io.py put "$BUCKET" "$P/end.json" out/end.json
  power_off "atlas client finished"
}
( while sleep "$PUSH_S"; do push; done ) & PUSHER=$!
step "Atlas line client start; $(nproc) vCPU"
$PY cpu_macchina.py out/cpu-machine.log 5 & CPU_PID=$!
[ "$(md5sum prova_infinite.py | cut -d' ' -f1)" = "$SCRIPT_MD5" ] || finish SCRIPT_CHANGED
for _ in $(seq 1 60); do exists $P/config.json && break; sleep 5; done
$PY s3io.py get "$BUCKET" $P/config.json out/config.json || finish NO_CONFIG
cfg() { $PY cfg.py out/config.json "$1"; }
PROGRAMS=$(cfg programs); PANTRY=$(cfg pantry); DB=$(cfg ballast_db)
has() { grep -q "\"$1\"" <<<"$PROGRAMS"; }
umask 077
$PY s3io.py get "$BUCKET" secret/atlas.json secret-atlas.json > /dev/null 2>&1 || finish NO_ATLAS_SECRET
$PY -c "import sys, s3io; s3io.client().delete_object(Bucket=sys.argv[1], Key='secret/atlas.json')" "$BUCKET" > /dev/null 2>&1 || true
sec() { $PY cfg.py secret-atlas.json "$1"; }
URI=$(sec uri); [ -n "$URI" ] || finish NO_ATLAS_URI
FENVIX_P1_URI=$URI $PY wait_db.py 15 >> out/steps.log 2>&1 || finish ATLAS_UNREACHABLE
step "the Atlas cluster answers"
run_prog() { # as extras.sh: one program, its terminal in out/NAME.txt, outcome from its last line
  local name=$1; shift; local t0; t0=$(date +%s)
  step "program $name starts"
  "$@" > "out/$name.txt" 2>&1; local rc=$?
  outcomes+=("$name:rc=$rc")
  step "program $name: code $rc in $(( $(date +%s) - t0 )) s: $(tail -1 "out/$name.txt" | cut -c1-200)"
  push
}
mkdir -p atlas fn-states/atlas fn-states/ballast && tar xzf atlas-states.tar.gz -C atlas || finish NO_ATLAS_STATES
# the state files of earlier Atlas jobs (the cluster's own loads), over the Ireland ones
$PY s3io.py get-dir "$PANTRY" atlas-states/ fn-states/ballast > /dev/null 2>&1
keep_state() { # keep_state DB FILE: into the kept pantry for the next Atlas job
  $PY s3io.py put "$PANTRY" "atlas-states/$1/risultati_p1.json" "$2" > /dev/null 2>&1 && mkdir -p "fn-states/ballast/$1" && cp "$2" "fn-states/ballast/$1/"
}
# 1. the load
if has atlas-load; then
  STATES=$(cfg atlas_load_states)
  if [ -n "$STATES" ] && [ "$STATES" != "[]" ]; then
    cp -r atlas/. fn-states/atlas/
    $PY - "$STATES" load-plan.txt atlas-plan.txt <<'PY' || finish BAD_PLAN
import json, sys
want = set(json.loads(sys.argv[1]) or [])
rows = [r for r in open(sys.argv[2]) if r.strip() and not r.lstrip().startswith("#") and r.split()[0] in want]
if not rows:
    sys.exit(f"no line of the load plan for {sorted(want)}")
open(sys.argv[3], "w").write("".join(rows))
PY
    t0=$(date +%s)
    FENVIX_P1_URI=$URI $PY load.py atlas-plan.txt atlas work/load out > out/load.txt 2>&1; rc=$?
    echo "$(( $(date +%s) - t0 ))" > out/load-seconds.txt
    step "load of $STATES: code $rc in $(( $(date +%s) - t0 )) s: $(tail -1 out/load.txt | cut -c1-200)"
    for f in work/load/*/risultati_p1.json; do [ -e "$f" ] && keep_state "$($PY cfg.py "$f" db)" "$f"; done
    [ "$rc" = 0 ] || finish LOAD_FAILED
  fi
  BLINE=$(cfg atlas_ballast_line)
  if [ -n "$BLINE" ] && [ "$BLINE" != "[]" ]; then
    mkdir -p ballast-load && cp prova_infinite.py ballast-load/
    ARGS=$($PY -c "import json,sys; print(' '.join(json.loads(sys.argv[1])[1:]))" "$BLINE")
    BDB=$($PY -c "import json,sys; print(json.loads(sys.argv[1])[0])" "$BLINE")
    MAXH=$(cfg load_max_h)
    step "Atlas load: prova_infinite.py carica --db $BDB $ARGS --max-ore-carico $MAXH"
    t0=$(date +%s)
    # shellcheck disable=SC2086
    (cd ballast-load && FENVIX_P1_URI=$URI $PY prova_infinite.py carica --file risultati_p1.json --db "$BDB" $ARGS --max-ore-carico "$MAXH") > out/ballast-load.txt 2>&1
    rc=$?
    echo "$(( $(date +%s) - t0 ))" > out/ballast-load-seconds.txt
    step "Atlas load finished with code $rc in $(( $(date +%s) - t0 )) s: $(tail -1 out/ballast-load.txt | cut -c1-200)"
    [ -f ballast-load/risultati_p1.json ] && keep_state "$BDB" ballast-load/risultati_p1.json && cp ballast-load/risultati_p1.json out/ballast-state.json
    # the load's own rate and whether it reached its size (the script stops at --max-ore-carico and says why)
    $PY - ballast-load/risultati_p1.json <<'PY' | tee -a out/steps.log
import json, sys
c = json.load(open(sys.argv[1])).get("caricamento", {})
print(f"Atlas load: {c.get('byte_bson', 0) / 2**30:.2f} GiB in {c.get('secondi')} s = {c.get('mb_s')} MB/s; finished {c.get('finito')} "
      f"({c.get('motivo_fine')})")
PY
    [ "$rc" = 0 ] || finish BALLAST_LOAD_FAILED
  fi
fi
# 2. first-next (no counters: not our server)
if has first-next; then
  # shellcheck disable=SC2046
  run_prog first-next $PY first_next.py out/first-next --uri "$URI" --states fn-states --counters none ${DB:+--db "$DB"} $(cfg first_next_args)
fi
# 3. the founder's script's read rounds on the command's database, against the state file of Atlas's own load
if has ballast-rounds; then
  mkdir -p ballast/lavoro/stati out/ballast-rounds
  cp programma.py prova_infinite.py somma_massimo.py controlla_parallelo.py ballast/
  cp -r "fn-states/ballast/$DB" ballast/lavoro/stati/ 2>/dev/null
  if [ -n "$DB" ] && [ -f "ballast/lavoro/stati/$DB/risultati_p1.json" ]; then
    echo "giro $DB --db $DB --nuova-cartella --giro s9_atlas --solo-fasi inizio,Q2,Q3,Q1,Q1d,Q4 $(cfg ballast_rounds_args)" > ballast/passi.txt
    ln -sfn "$PWD/out/ballast-rounds" ballast/out
    run_prog ballast-rounds bash -c "cd ballast && URI_GIRO='$URI' FENVIX_P1_URI='$URI' $PY programma.py passi.txt"
    cp -r ballast/lavoro/stati out/ballast-rounds/stati 2>/dev/null
  else
    step "ballast-rounds: no state file of an Atlas load of ${DB:-?} in the pantry: not run"; outcomes+=("ballast-rounds:NO_STATE")
  fi
fi
# 4. the Atlas operations (Admin API)
if has atlas-ops; then
  printf '%s' "$(sec api_key)" > atlas-key
  TU=$(sec target_uri); TC=$(sec target_cluster)
  # shellcheck disable=SC2046
  run_prog atlas-ops $PY atlas_ops.py out/atlas-ops --uri "$URI" ${TU:+--target-uri "$TU"} ${TC:+--target-cluster "$TC"} \
    --group "$(sec group)" --cluster "$(sec cluster)" --key-file "$PWD/atlas-key" --states fn-states ${DB:+--db "$DB"} \
    $( [ -n "$(sec api)" ] && echo --api "$(sec api)" ) $(cfg atlas_ops_args)
  rm -f atlas-key
fi
step "programs done: ${outcomes[*]:-none}"
finish ATLAS_LINE_DONE
