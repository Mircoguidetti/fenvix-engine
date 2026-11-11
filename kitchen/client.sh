#!/bin/bash
# Kitchen (K2, corrected by K2b) · what runs ON THE CLIENT MACHINE (the dining room: EC2 c7i, Ubuntu 24.04, eu-west-1,
# same zone as the cooks). Started by the user-data written by ci/run.sh. No AWS keys: S3 with the machine role.
# The method is the one of the Atlas round from Ireland, one to one (reviewer's A1-A4):
#   - the program is code-irlanda's programs/m30-corto/passi.txt, word for word, run by code-irlanda's programma.py
#     unchanged (phases inizio and fine, dormienti step, CPU of every process and validita-macchina.txt, the copies of
#     carico_massimo) with the script p1-1.6.2 unchanged; only the connection string is ours;
#   - the data: the same databases, seeds and part of the fake year as Atlas (load.py on cook 0, the loader);
#   - the clients: the state files the Atlas round started from, with our order _ids (merge_states.py), checked dry
#     against the Atlas plan before any measurement (compare_plans.py: "PLANS: SAME", else nothing is measured);
#   - every size starts from those same state files, on a NEW cold cook (C1): every size reads the clients Atlas read.
# Flow: password -> Atlas state files (from the package) -> if this run loads: wait for cook 0, load, our state files
# into the pantry (kitchen-states/), "done-0" (the runner switches cook 0 off) / otherwise our state files from the
# pantry -> merge -> dry comparison -> for every size K: wait for cook K, (K5) open every database and settle, the
# program, (K4) the reads against the plan, "done-K" -> results, power off.
# Usage: client.sh RUN_BUCKET   (client.env from the runner: PANTRY, LOAD; no secrets)
set -u
cd "$(dirname "$0")" || exit 1
BUCKET=$1
source ./client.env   # PANTRY, LOAD (1 = this run loads), PROGRAM (name)
export AWS_DEFAULT_REGION=eu-west-1 AWS_REGION=eu-west-1
export PYTHONPATH="$PWD/vendor" PYTHONUNBUFFERED=1   # the script's lines reach the log at once (the script is unchanged)
PY=${PY:-python3}
SCRIPT_MD5=8396ca455557baaa4e3a9b1f881451f9
WAIT_COOK_MIN=${WAIT_COOK_MIN:-45}   # K3: the runner may copy the pantry before launching a cook
WAIT_DB_MIN=${WAIT_DB_MIN:-15}
PUSH_S=${PUSH_S:-30}
mkdir -p out work
step() { echo "$(date -u +%FT%TZ) $*" | tee -a out/steps.log; }
copy_states() { # state files, registers and raw data of the size in progress come back even halfway through
  for z in work/size-*/; do [ -d "$z" ] || continue; n=$(basename "$z")
    [ -d "${z}lavoro/stati" ] && { mkdir -p "out/$n/stati"; cp -r "${z}lavoro/stati/." "out/$n/stati/" 2>/dev/null; }
    [ -d "${z}lavoro/dati" ] && { mkdir -p "out/$n/dati"; cp -r "${z}lavoro/dati/." "out/$n/dati/" 2>/dev/null; }
    [ -d "${z}lavoro/dormienti" ] && { mkdir -p "out/$n/dormienti"; cp -r "${z}lavoro/dormienti/." "out/$n/dormienti/" 2>/dev/null; }
    t=$(ls -t "out/$n"/terminali/*.txt 2>/dev/null | head -1); [ -n "$t" ] && cp "$t" "out/$n/terminal-now.txt" 2>/dev/null
  done
  return 0
}
push() { copy_states; $PY s3io.py push "$BUCKET" out results/client/ 2>> out/push-errors.log; }
( while sleep "$PUSH_S"; do push; done ) &
PUSHER=$!
finish() { # finish OUTCOME
  step "finish: $1"
  kill $PUSHER 2>/dev/null
  [ -n "${CPU_PID:-}" ] && kill "$CPU_PID" 2>/dev/null
  printf '{"finished": true, "outcome": "%s"}\n' "$1" > out/end.json
  push; push
  $PY s3io.py put "$BUCKET" client/end.json out/end.json 2>/dev/null
  [ -n "${LOCAL_TEST:-}" ] || shutdown -h now "kitchen: client finished"
  exit 0
}
done_k() { # done_k K JSON: tell the runner that cook K can be switched off
  echo "$2" > "out/done-$1.json"; push
  $PY s3io.py put "$BUCKET" "client/done-$1.json" "out/done-$1.json"
}
wait_cook() { # wait_cook K: wait for "go" and then "ready" of cook K; sets TYPE and FENVIX_P1_URI. 1 = no cook K
  local k=$1 t0 go ready ip port
  step "waiting for cook $k (at most $WAIT_COOK_MIN minutes)"
  t0=$(date +%s); go=""
  while :; do
    $PY s3io.py exists "$BUCKET" client/enough && return 1
    go=$($PY s3io.py text "$BUCKET" "server-$k/go.json" 2>/dev/null) && [ -n "$go" ] && break
    [ $(( $(date +%s) - t0 )) -ge $(( WAIT_COOK_MIN * 60 )) ] && return 1
    sleep 10
  done
  TYPE=$($PY -c "import json,sys; print(json.loads(sys.argv[1])['type'])" "$go")
  COOK_PANTRY=$($PY -c "import json,sys; print(json.loads(sys.argv[1]).get('pantry') or sys.argv[2])" "$go" "$PANTRY")
  COOK_PREP=$($PY -c "import json,sys; print('1' if json.loads(sys.argv[1]).get('prepare') else '0')" "$go")
  # aws-fixes (B5): the settle cap comes with the cook's go (the round's "settle_max_min"); SETTLE_MAX_MIN (local tests) wins
  COOK_SETTLE_MAX=${SETTLE_MAX_MIN:-$($PY -c "import json,sys; print(json.loads(sys.argv[1]).get('settle_max_min') or 20)" "$go")}
  t0=$(date +%s); ready=""
  while :; do
    ready=$($PY s3io.py text "$BUCKET" "server-$k/ready.json" 2>/dev/null) && [ -n "$ready" ] && break
    $PY s3io.py exists "$BUCKET" "server-$k/stopped" && break
    [ $(( $(date +%s) - t0 )) -ge $(( WAIT_DB_MIN * 60 )) ] && break
    sleep 10
  done
  [ -n "$ready" ] || { step "cook $k ($TYPE): never ready"; done_k "$k" '{"outcome": "SERVER_NOT_READY"}'; return 2; }
  ip=$($PY -c "import json,sys; print(json.loads(sys.argv[1])['ip'])" "$ready")
  port=$($PY -c "import json,sys; print(json.loads(sys.argv[1])['port'])" "$ready")
  # TLS with the CA of the run (certificate for the cook's private IP), user root; replicaSet=rs0 like Atlas
  export FENVIX_P1_URI="mongodb://root:$CM_PW@$ip:$port/?replicaSet=rs0&tls=true&tlsCAFile=$PWD/ca.pem&authSource=admin"
  $PY wait_db.py "$WAIT_DB_MIN" >> out/steps.log 2>&1 || { done_k "$k" '{"outcome": "SERVER_UNREACHABLE"}'; return 2; }
  step "cook $k ($TYPE) answers"
  return 0
}
step "client start; $(nproc) vCPU; $($PY --version 2>&1); program $PROGRAM; this run loads: $LOAD; pantry $PANTRY"
$PY cpu_macchina.py out/cpu-machine.log 5 & CPU_PID=$!   # CPU of the whole machine every 5 s, all day
# 1. the founder's script and code-irlanda's files, byte for byte
md5=$(md5sum prova_infinite.py | cut -d' ' -f1)
[ "$md5" = "$SCRIPT_MD5" ] || { step "ERROR: script md5 $md5, expected $SCRIPT_MD5"; finish SCRIPT_CHANGED; }
md5sum -c --quiet vendored.md5 > out/vendored-check.txt 2>&1 || { step "ERROR: a code-irlanda file is not the original: $(head -3 out/vendored-check.txt)"; finish VENDORED_CHANGED; }
step "script and code-irlanda files: originals (md5)"
# 2. the password: only in the processes' memory
CM_PW=$($PY s3io.py text "$BUCKET" secret/pw) && [ -n "$CM_PW" ] || { step "ERROR: password not read"; finish NO_PASSWORD; }
step "password read (written nowhere)"
# 3. the Atlas state files (code-irlanda, the commit the Atlas round started from; checked by the runner)
mkdir -p atlas && tar xzf atlas-states.tar.gz -C atlas || finish NO_ATLAS_STATES
STATES=$($PY -c "import programma as P; print(' '.join(sorted({p['arg'][0] for p in P.leggi_passi('passi.txt') if p['tipo'] != 'aspetta'})))") || finish BAD_PROGRAM
step "Atlas state files: $(ls atlas | tr '\n' ' '); the program uses: $STATES"
# 4. the data: load now (cook 0) or take our state files of an earlier load from the pantry
if [ "$LOAD" = 1 ]; then
  wait_cook 0 || finish NO_LOADER
  step "load: $(grep -cv '^[[:space:]]*\(#\|$\)' load-plan.txt) steps on cook 0 ($TYPE)"
  t0=$(date +%s)
  $PY load.py load-plan.txt atlas work/load out > out/load.txt 2>&1
  rc=$?
  step "load finished with code $rc in $(( $(date +%s) - t0 )) s: $(tail -1 out/load.txt)"
  mkdir -p loaded
  for s in work/load/*/; do n=$(basename "$s"); mkdir -p "loaded/$n"; cp "$s"risultati_p1.json "loaded/$n/" 2>/dev/null; done
  echo "$(( $(date +%s) - t0 ))" > out/load-seconds.txt
  if [ "$rc" != 0 ]; then done_k 0 '{"outcome": "LOAD_FAILED"}'; finish LOAD_FAILED; fi
  # the loader's compaction calms down before the loader is switched off (risk 1 of the reviewer, C6). aws-fixes (B5):
  # settle.py watches the server's compaction counters and the fx/ objects, and never gives up silently: if the cap
  # is hit, the data are kept (state files pushed: the load itself is complete) and the round stops with
  # LOADED_NOT_SETTLED, so nobody measures on it without reading out/settle.json first
  $PY settle.py "$PANTRY" "$COOK_SETTLE_MAX" "${SETTLE_STEP_S:-60}" out/settle.json "${SETTLE_MIN_S:-150}" > out/settle.txt 2>&1
  src=$?
  step "$(tail -1 out/settle.txt)"
  $PY s3io.py push "$PANTRY" loaded kitchen-states/ 2>> out/push-errors.log
  cp -r loaded out/loaded-states
  if [ "$src" != 0 ]; then done_k 0 '{"outcome": "LOADED_NOT_SETTLED"}'; finish LOADED_NOT_SETTLED; fi
  done_k 0 '{"outcome": "LOADED"}'
else
  $PY s3io.py get-dir "$PANTRY" kitchen-states/ loaded > /dev/null 2>&1 || { step "ERROR: no state files of a load in the pantry $PANTRY"; finish NO_LOADED_STATES; }
  step "state files of the earlier load taken from the pantry: $(ls loaded | tr '\n' ' ')"
fi
# 5. our state files = the Atlas ones + our _ids; then the one-to-one check, dry
# shellcheck disable=SC2086
$PY merge_states.py atlas loaded pristine $STATES > out/merge.txt 2>&1 || { step "$(tail -1 out/merge.txt)"; finish STATES_DIFFERENT; }
cp pristine/merge-report.json out/ 2>/dev/null
step "$(tail -1 out/merge.txt)"
$PY compare_plans.py atlas pristine passi.txt out/plans > out/plans.txt 2>&1
step "$(tail -1 out/plans.txt)"
grep -q "^PLANS: SAME" out/plans.txt || finish PLANS_DIFFERENT
rm -f pristine/merge-report.json
# 6. the sizes, one per cook, each from the same state files (the clients Atlas read)
outcomes=()
K=0
while :; do
  K=$((K + 1))
  wait_cook "$K"; w=$?
  [ "$w" = 1 ] && break
  [ "$w" = 2 ] && { outcomes+=("$K:SERVER_NOT_READY"); continue; }
  Z=work/size-$K; mkdir -p "$Z/lavoro/stati" "out/size-$K"
  cp programma.py prova_infinite.py somma_massimo.py controlla_parallelo.py passi.txt "$Z/"
  cp -r pristine/. "$Z/lavoro/stati/"
  ln -s "$PWD/out/size-$K" "$Z/out"
  ( cd "$Z/lavoro/stati" && find . -type f | sort | xargs -r md5sum ) > "out/size-$K/states-in.md5"
  $PY cpu_macchina.py "out/size-$K/cpu-macchina.log" 5 & SIZE_CPU=$!
  # K5: open every database on this cook (listCollections, a reader: no write, nothing measured), keep them open and wait
  # for the pantry to calm down; then the round. The SST counts of the cook (sst-during.jsonl) show whether the
  # compaction still runs during the measures.
  if [ "$COOK_PREP" = 1 ]; then
    t0=$(date +%s)
    $PY open_all.py "$((${SETTLE_MAX_MIN:-20} * 60 + 120))" "out/size-$K/open-all.json" > "out/size-$K/open-all.txt" 2>&1 & OPEN_PID=$!
    sleep 5
    $PY settle.py "$COOK_PANTRY" "$COOK_SETTLE_MAX" "${SETTLE_STEP_S:-60}" "out/size-$K/settle.json" "${SETTLE_MIN_S:-150}" > "out/size-$K/settle.txt" 2>&1
    src=$?
    kill "$OPEN_PID" 2>/dev/null; wait "$OPEN_PID" 2>/dev/null
    step "size $K ($TYPE): prepared in $(( $(date +%s) - t0 )) s: $(head -1 "out/size-$K/open-all.txt"); $(tail -1 "out/size-$K/settle.txt")"
    if [ "$src" != 0 ]; then
      # aws-fixes (B5): no measure on a pantry whose compaction is not finished (or cannot be seen): stop and say so
      kill "$SIZE_CPU" 2>/dev/null
      step "size $K ($TYPE): NOT_SETTLED: the program does not run (out/size-$K/settle.json says why)"
      outcomes+=("$K:NOT_SETTLED"); done_k "$K" "{\"outcome\": \"NOT_SETTLED\", \"type\": \"$TYPE\"}"
      continue
    fi
  fi
  URI_GIRO="${FENVIX_P1_URI}&maxConnecting=${MAX_CONNECTING:-200}"
  step "size $K ($TYPE): program $PROGRAM starts (maxConnecting in the round's string, like from Ireland)"
  ( cd "$Z" && URI_GIRO=$URI_GIRO $PY programma.py passi.txt ) > "out/size-$K/programma.txt" 2>&1
  rc=$?
  kill "$SIZE_CPU" 2>/dev/null
  copy_states
  outcome=FINITO
  [ "$rc" = 0 ] || outcome="INCOMPLETO_CODICE_$rc"
  grep -q "ATTENZIONE: almeno una fase NON VALIDA" "out/size-$K/validita-macchina.txt" 2>/dev/null && outcome="${outcome}_MACCHINA_LIMITE"
  # K4 (reviewer of K2b): the clients our cook really served, phase by phase, against the plan (plans.json is the same
  # plan: compare_plans.py said SAME); otherwise the size is NON_UNO_A_UNO
  $PY check_reads.py pristine "$Z/lavoro/stati" passi.txt "$Z/lavoro" "out/size-$K/reads.json" > "out/size-$K/reads.txt" 2>&1
  rr=$?
  step "size $K ($TYPE): $(tail -1 "out/size-$K/reads.txt")"
  [ "$rr" = 0 ] || case "$outcome" in FINITO*) outcome=NON_UNO_A_UNO ;; esac   # an incomplete size stays INCOMPLETO
  step "size $K ($TYPE): $outcome; $(tail -1 "out/size-$K/passi.log" 2>/dev/null)"
  outcomes+=("$K:$outcome")
  done_k "$K" "{\"outcome\": \"$outcome\", \"type\": \"$TYPE\"}"
done
step "outcomes of the sizes: ${outcomes[*]:-none}"
all=FINITO
[ ${#outcomes[@]} -gt 0 ] || all=$([ "$LOAD" = 1 ] && echo LOADED_ONLY || echo NO_SIZE)
for e in "${outcomes[@]}"; do
  case "${e#*:}" in FINITO) ;; FINITO_MACCHINA_LIMITE) [ "$all" = FINITO ] && all=FINITO_MACCHINA_LIMITE ;;
    NON_UNO_A_UNO) [ "$all" = INCOMPLETO ] || all=NON_UNO_A_UNO ;; *) all=INCOMPLETO ;; esac
done
finish "$all"
