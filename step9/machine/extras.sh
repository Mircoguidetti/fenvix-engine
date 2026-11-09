#!/bin/bash
# Step 9 · the extras client: a second client machine (same type and zone as the m30 client), launched after the m30
# client has finished, so nothing here disturbs the like-for-like measurements. It runs the separate programs of the
# round, in this order (each one only if it is in the command's "programs"):
#   s3-first-byte   programs/s3_first_byte.py   S3 first byte by object size, and ranged GETs
#   sealed-run      programs/sealed_run_read.py one-trip read of a sealed run (256 KiB blocks, fence in RAM)
#   log-ack         programs/log_ack.py         3-zone log ack: through the cook (w: majority) and raw (pq commit)
#   ballast-rounds  the same cold read phases as m30-corto (Q2, Q3, Q1, Q1d, Q4), on two ballast databases, through
#                   code-irlanda's programma.py and the founder's script unchanged (100 GB and 1 TB only)
#   takeovers       programs/takeovers.py       N takeovers cook <-> spare, 0 acknowledged writes lost
# Then <NAME>/end.json and power off. Settings from the runner in <NAME>/config.json (no secrets).
#   agent-bench     programs/agent_bench.py     the agent checkpoint benchmark against our cook, the Postgres machine (and
#                   Aurora if given); run by a second client named "agent", after the spare is off (8 vCPU)
# Usage: extras.sh BUCKET [NAME]   (NAME: extras, or agent for the agent benchmark; its folder in the run bucket)
set -u
cd "$(dirname "$0")" || exit 1
BUCKET=$1; P=${2:-extras}
source ./common.sh
SCRIPT_MD5=8396ca455557baaa4e3a9b1f881451f9
outcomes=()
finish() {
  step "finish: $1"; [ -n "${CPU_PID:-}" ] && kill "$CPU_PID" 2>/dev/null; [ -n "${PUSHER:-}" ] && kill "$PUSHER" 2>/dev/null
  printf '{"finished": true, "outcome": "%s", "programs": "%s"}\n' "$1" "${outcomes[*]:-}" > out/end.json
  push; push; $PY s3io.py put "$BUCKET" "$P/end.json" out/end.json
  power_off "extras finished"
}
( while sleep "$PUSH_S"; do push; done ) & PUSHER=$!
step "extras client start; $(nproc) vCPU"
$PY cpu_macchina.py out/cpu-machine.log 5 & CPU_PID=$!
[ "$(md5sum prova_infinite.py | cut -d' ' -f1)" = "$SCRIPT_MD5" ] || finish SCRIPT_CHANGED
CM_PW=$(text secret/pw) && [ -n "$CM_PW" ] || finish NO_PASSWORD
export CM_PW
for _ in $(seq 1 60); do exists $P/config.json && break; sleep 5; done
$PY s3io.py get "$BUCKET" $P/config.json out/config.json || finish NO_CONFIG
cfg() { $PY cfg.py out/config.json "$1"; }
PROGRAMS=$(cfg programs); COOK_IP=$(cfg cook.ip); COOK_PORT=$(cfg cook.port); SPARE_IP=$(cfg spare.ip); LOGS=$(cfg logs)
PANTRY=$(cfg pantry)
URI="mongodb://root:$CM_PW@$COOK_IP:$COOK_PORT/?replicaSet=rs0&tls=true&tlsCAFile=$PWD/ca.pem&authSource=admin"
has() { grep -q "\"$1\"" <<<"$PROGRAMS"; }
run_prog() { # run_prog NAME COMMAND...: one program, its terminal in out/NAME.txt, outcome from its last line
  local name=$1; shift; local t0; t0=$(date +%s)
  step "program $name starts"
  "$@" > "out/$name.txt" 2>&1; local rc=$?
  outcomes+=("$name:rc=$rc")
  step "program $name: code $rc in $(( $(date +%s) - t0 )) s: $(tail -1 "out/$name.txt" | cut -c1-200)"
  push
}
step "programs: $PROGRAMS; cook $COOK_IP:$COOK_PORT; spare ${SPARE_IP:-none}; log ${LOGS:-none}"
FENVIX_P1_URI=$URI $PY wait_db.py 15 >> out/steps.log 2>&1 || finish COOK_UNREACHABLE
has s3-first-byte && run_prog s3-first-byte $PY s3_first_byte.py "$BUCKET" out/s3-first-byte $(cfg s3_first_byte_args)
has sealed-run && run_prog sealed-run $PY sealed_run_read.py "$BUCKET" out/sealed-run $(cfg sealed_run_args)
has log-ack && run_prog log-ack $PY log_ack.py out/log-ack --uri "$URI" --logs "$(cfg bench_logs)" --pq ./pq $(cfg log_ack_args)
if has ballast-rounds; then
  # two ballast databases, their state files from the pantry, the read phases of m30-corto through programma.py
  mkdir -p ballast/lavoro/stati
  cp programma.py prova_infinite.py somma_massimo.py controlla_parallelo.py ballast/
  $PY s3io.py get-dir "$PANTRY" step9-ballast-states/ ballast/lavoro/stati > /dev/null 2>&1
  $PY - "$(cfg ballast_rounds_args)" > ballast/passi.txt <<'PY'
import os, sys
extra = sys.argv[1]
names = sorted(os.listdir("ballast/lavoro/stati"))[:2]
for n in names:
    print(f"giro {n} --db {n} --nuova-cartella --giro s9_ballast --solo-fasi inizio,Q2,Q3,Q1,Q1d,Q4 {extra}")
PY
  ln -sfn "$PWD/out/ballast-rounds" ballast/out; mkdir -p out/ballast-rounds
  if [ -s ballast/passi.txt ]; then
    run_prog ballast-rounds bash -c "cd ballast && URI_GIRO='${URI}&maxConnecting=200' FENVIX_P1_URI='$URI' $PY programma.py passi.txt"
    cp -r ballast/lavoro/stati out/ballast-rounds/stati 2>/dev/null
  else
    step "ballast-rounds: no ballast state files in the pantry (data atlas?): skipped"; outcomes+=("ballast-rounds:SKIPPED")
  fi
fi
if has takeovers; then
  [ -n "$SPARE_IP" ] || { step "takeovers: no spare in this round: skipped"; outcomes+=("takeovers:SKIPPED"); }
  # shellcheck disable=SC2046
  [ -n "$SPARE_IP" ] && run_prog takeovers $PY takeovers.py "$BUCKET" out/takeovers --a "$COOK_IP" --b "$SPARE_IP" --ca "$PWD/ca.pem" $(cfg takeovers_args)
fi
if has agent-bench; then
  PG=$(cfg pgbox.ip); AU=$($PY s3io.py text "$BUCKET" secret/aurora-uri 2>/dev/null || true)
  # shellcheck disable=SC2046
  run_prog agent-bench $PY agent_bench.py out/agent-bench --fenvix-uri "$URI" --pg-host "$PG" --aurora-uri "$AU" --s3-bucket "$PANTRY" $(cfg agent_bench_args)
fi
step "programs done: ${outcomes[*]:-none}"
finish EXTRAS_DONE
