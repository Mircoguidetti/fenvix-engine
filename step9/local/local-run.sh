#!/bin/bash
# Step 9 local mode: the WHOLE round of the workflow, $0, on this machine, with the same scripts as on AWS, as the
# 8-vCPU plan runs it on AWS (founder, 2026-11-09): three jobs one after the other on a KEPT pantry:
#   A. load the Atlas databases (loader + m30 client), pantry kept      {"phases": ["load"], "pantry": "keep", "ballast_slice": [0, 0]}
#   B. load the ballast (loader + filler) into the kept pantry          {"phases": ["load"], "pantry": "<kept>"}
#   C. measure: cook with log-0, log-1, log-2, the m30 program; then the spare and the extra programs
# Each job: decide.py -> atlas_states.sh -> packages.sh -> run.py -> teardown.py -> check_secrets.py -> final_check.py;
# at the end the kept pantry is dropped (mode drop-pantry) and the final check must say RIMASTO: niente.
# What changes from AWS, and only that:
#   - machines are docker containers (step9/local/Dockerfile) with each type's vCPU (capped by this machine's cores)
#     and memory, on one docker network (a real network, not real zones); the 1-vCPU Graviton log nodes become x86
#     m7i.large containers (this machine is x86), so decide.py runs with S9_VCPU_LIMIT=16 here (8 on AWS);
#   - S3 is MinIO on this machine; the Atlas state files come from a fake Atlas (fake_atlas.sh); program and load plan
#     are SHRUNK (same steps and phases, small numbers); ballast databases 0.05 GB; sealed run 64 MiB; 10 takeovers
#     (the 50-takeover run is validated separately: TAKEOVERS_N=50);
#   - a runner minute is MINUTE_S seconds (15 here) so the SPESA line is printed often.
# Usage: local-run.sh WORK    (BLOCK_TEST=1: a last round with a $0.05 cap that must stop with BLOCCO and leave nothing)
set -u
W=$1
HERE=$(cd "$(dirname "$0")" && pwd); S9=$(cd "$HERE/.." && pwd); ENG=$(cd "$S9/.." && pwd)
PY=${PY:-/root/.fenvix-judge/env/bin/python}
MINIO=${MINIO:-/root/.fenvix-judge/env/bin/minio}
MPORT=${MPORT:-9300}
SRV=${SRV:-$ENG/target/release/mongo-server}
PQ=${PQ:-$ENG/proto/quaderno/target/release/pq}
TAKEOVERS_N=${TAKEOVERS_N:-10}
mkdir -p "$W"; cd "$W" || exit 1
T0=$(date +%s)
export AWS_ACCESS_KEY_ID=minio-local AWS_SECRET_ACCESS_KEY=minio-local-secret AWS_REGION=eu-west-1 AWS_DEFAULT_REGION=eu-west-1
export AWS_ENDPOINT_URL=http://127.0.0.1:$MPORT NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
export S9_VCPU_LIMIT=16
echo "== 0. docker image of the machines, MinIO"
docker build -q -t fenvix-step9-machine "$HERE" > /dev/null || exit 1
pkill -f "minio server $W/minio" 2>/dev/null; rm -rf "$W/minio"
MINIO_ROOT_USER=$AWS_ACCESS_KEY_ID MINIO_ROOT_PASSWORD=$AWS_SECRET_ACCESS_KEY nohup "$MINIO" server "$W/minio" --address "0.0.0.0:$MPORT" --console-address ":$((MPORT + 1))" > minio.log 2>&1 &
MINIO_PID=$!
for _ in $(seq 1 40); do curl -fs "$AWS_ENDPOINT_URL/minio/health/live" > /dev/null && break; sleep 0.5; done
echo "== 1. fake Atlas and the shrunk kitchen (fake_atlas.sh)"
[ -d "$W/atlas-states" ] && [ -d "$W/kitchen-small" ] || bash "$HERE/fake_atlas.sh" "$W" "$SRV" "$PY" | tail -6
mkdir -p "$W/bin/mongo-server-x86_64" "$W/bin/pq-x86_64"
cp "$SRV" "$W/bin/mongo-server-x86_64/mongo-server"; echo "local $(cd "$ENG" && git rev-parse --short HEAD)" > "$W/bin/mongo-server-x86_64/motore-commit.txt"
cp "$PQ" "$W/bin/pq-x86_64/pq"
round() { # round NAME COMMAND_JSON
  local name=$1 c=$2
  export RUN_ID=$((RANDOM * 7 + 1000)) S9_MODE=local MINUTE_S=${MINUTE_S:-15} CA_DIR=$W/ca-$name PACKAGES=$W/packages-$name
  export CM_PW; CM_PW=$(openssl rand -hex 24)
  export GITHUB_STEP_SUMMARY=$W/summary-$name.md
  local OUTD=$W/out-$name; rm -rf "$OUTD" "$PACKAGES"; mkdir -p "$OUTD"
  echo "== round $name: $(cat "$c")"
  "$PY" "$S9/ci/decide.py" "$c" "$W/resolved-$name.json" || return 1
  if [ "$("$PY" "$S9/machine/cfg.py" "$W/resolved-$name.json" pantry_mode)" = existing ]; then   # as the workflow does
    CM_PW=$("$PY" -c "import boto3,sys; print(boto3.client('s3').get_object(Bucket=sys.argv[1], Key='step9-secret/pw')['Body'].read().decode())" \
      "$("$PY" "$S9/machine/cfg.py" "$W/resolved-$name.json" pantry_name)") || return 1
  fi
  # local mode: the same fields with small numbers
  "$PY" - "$W/resolved-$name.json" "$TAKEOVERS_N" <<'PY'
import json, sys
f = sys.argv[1]; c = json.load(open(f))
for l in c["ballast"]["lines"]:
    l[l.index("--gb") + 1] = "0.05"; l[l.index("--thread") + 1] = "2"
    l += ["--mb-cliente", "0.5", "--lotto", "200"]
c["ballast"]["parallel"] = 2
c["s3_first_byte_args"] = "--objects 4 --reads 30 --sizes-kib 1,64,1024 --range-kib 64,256 --big-mib 16"
c["sealed_run_args"] = "--run-mib 64 --block-kib 256 --reads 60"
c["ballast_rounds_args"] = "--n-q2 6 --n-q3 6 --n-q1 6 --n-q1d 6 --n-q4 4 --dorm-q2 3 --dorm-q3 3 --dorm-q1 3 --dorm-q1d 3 --dorm-q4 3 --pausa-s 2 --min-secondi-fase 2"
c["log_ack_args"] = "--n 300 --conc 8 --secs 10"
c["takeovers_args"] = c["takeovers_args"].replace("--n 50", f"--n {sys.argv[2]}")
c["data_gb_upper"] = 1
json.dump(c, open(f, "w"), indent=1)
PY
  ATLAS_STATES_DIR=$W/atlas-states bash "$ENG/kitchen/ci/atlas_states.sh" local "$W/atlas-$name.tgz" m10p m10s m30p m30s vecchio > /dev/null || return 1
  S9=$S9 K=$W/kitchen-small CA_DIR=$CA_DIR PACKAGES=$PACKAGES BIN=$W/bin ATLAS_TGZ=$W/atlas-$name.tgz \
    PANTRY=$("$PY" "$S9/machine/cfg.py" "$W/resolved-$name.json" pantry_name) \
    LOAD=$("$PY" -c "import json,sys; print(1 if json.load(open(sys.argv[1]))['atlas_load'] else 0)" "$W/resolved-$name.json") \
    WAIT_COOK_MIN=120 bash "$S9/ci/packages.sh" > "packages-$name.txt" 2>&1 || { tail -5 "packages-$name.txt"; return 1; }
  local t; t=$(date +%s)
  "$PY" "$S9/ci/run.py" "$W/resolved-$name.json" "$OUTD" > "run-$name.log" 2>&1; local rc=$?
  echo "run.py: code $rc in $(( $(date +%s) - t )) s; $(grep -E "outcome:" "run-$name.log" | tail -1 | cut -c22-300)"
  grep -E "BLOCCO" "run-$name.log" | head -1
  grep "SPESA" "run-$name.log" | tail -1 | cut -c1-200
  "$PY" "$S9/ci/teardown.py" local > "teardown-$name.txt" 2>&1; echo "teardown: code $?"
  "$PY" "$ENG/kitchen/ci/check_secrets.py" "$OUTD"
  "$PY" "$S9/ci/final_check.py" local; echo "final check: code $?"
}
D=$W/commands; mkdir -p "$D"
echo '{"mode": "run", "data": "100gb", "log": "m7i.large", "phases": ["load"], "pantry": "keep", "ballast_slice": [0, 0], "ceiling_min": 60}' > "$D/a.json"
round load-atlas "$D/a.json"
KEPT=$("$PY" "$S9/machine/cfg.py" "$W/resolved-load-atlas.json" pantry_name)
echo "kept pantry: $KEPT (still there: $(aws --version >/dev/null 2>&1; "$PY" -c "import boto3,sys; print(sys.argv[1] in [b['Name'] for b in boto3.client('s3').list_buckets()['Buckets']])" "$KEPT"))"
echo "{\"mode\": \"run\", \"data\": \"100gb\", \"log\": \"m7i.large\", \"phases\": [\"load\"], \"pantry\": \"$KEPT\", \"ceiling_min\": 60}" > "$D/b.json"
round load-ballast "$D/b.json"
echo "{\"mode\": \"run\", \"data\": \"100gb\", \"log\": \"m7i.large\", \"phases\": [\"measure\", \"extras\"], \"pantry\": \"$KEPT\", \"ceiling_min\": 150}" > "$D/c.json"
round measure "$D/c.json"
echo "== drop-pantry: the kept pantry goes"
"$PY" -c "import sys; sys.path.insert(0, sys.argv[2]); import infra; infra.make('local').delete_bucket(sys.argv[1]); print('kept pantry deleted:', sys.argv[1])" "$KEPT" "$S9/ci"
"$PY" "$S9/ci/final_check.py" local; echo "final check after drop-pantry: code $?"
if [ -n "${BLOCK_TEST:-}" ]; then
  echo '{"mode": "run", "data": "atlas", "log": "m7i.large", "phases": ["load"], "pantry": "new", "spend_cap": 0.05, "programs": ["m30"]}' > "$D/block.json"
  round block "$D/block.json"
fi
kill $MINIO_PID 2>/dev/null
echo "== checks"
"$PY" "$HERE/check_local.py" "$W" load-atlas load-ballast measure
echo "total time: $(( $(date +%s) - T0 )) s"
