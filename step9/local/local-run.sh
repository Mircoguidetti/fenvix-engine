#!/bin/bash
# Step 9 local mode: the WHOLE round of the workflow, $0, on this machine, with the same scripts as on AWS:
#   decide.py -> atlas_states.sh -> packages.sh -> run.py -> teardown.py -> check_secrets.py -> final_check.py
# What changes from AWS, and only that:
#   - machines are docker containers (step9/local/Dockerfile: Ubuntu 24.04 + python3, curl), each with the vCPU and
#     memory of its EC2 type (vCPU capped by this machine's cores), all on one docker network: the log nodes, the cook,
#     the spare and the clients talk over a real network, but not across real zones;
#   - S3 is MinIO on this machine (AWS_ENDPOINT_URL); the containers reach it through the docker network's gateway;
#   - the Atlas state files come from a fake Atlas (fake_atlas.sh, like the kitchen's local test), the program and the
#     load plan are SHRUNK (same steps and phases, small numbers), ballast databases are 0.05 GB, sealed run 64 MiB;
#   - a minute of the runner is MINUTE_S seconds (15 here), so the SPESA line is printed often (each line still counts
#     real elapsed minutes at the EC2 prices: the local spend is what the same wall time would cost on AWS).
# Usage: local-run.sh WORK [COMMAND_JSON]   (default command: data 100gb, every phase and program)
#   BLOCK_TEST=1: a second round with a spend cap of $0.05 that must stop with BLOCCO and leave nothing.
set -u
W=$1; CMDF=${2:-}
HERE=$(cd "$(dirname "$0")" && pwd); S9=$(cd "$HERE/.." && pwd); ENG=$(cd "$S9/.." && pwd)
PY=${PY:-/root/.fenvix-judge/env/bin/python}
MINIO=${MINIO:-/root/.fenvix-judge/env/bin/minio}
MPORT=${MPORT:-9300}
SRV=${SRV:-$ENG/target/release/mongo-server}
PQ=${PQ:-$ENG/proto/quaderno/target/release/pq}
mkdir -p "$W"; cd "$W" || exit 1
T0=$(date +%s)
export AWS_ACCESS_KEY_ID=minio-local AWS_SECRET_ACCESS_KEY=minio-local-secret AWS_REGION=eu-west-1 AWS_DEFAULT_REGION=eu-west-1
export AWS_ENDPOINT_URL=http://127.0.0.1:$MPORT NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
echo "== 0. docker image of the machines, MinIO"
docker build -q -t fenvix-step9-machine "$HERE" > /dev/null || exit 1
pkill -f "minio server $W/minio" 2>/dev/null; rm -rf "$W/minio"
MINIO_ROOT_USER=$AWS_ACCESS_KEY_ID MINIO_ROOT_PASSWORD=$AWS_SECRET_ACCESS_KEY nohup "$MINIO" server "$W/minio" --address "0.0.0.0:$MPORT" --console-address ":$((MPORT + 1))" > minio.log 2>&1 &
MINIO_PID=$!
for _ in $(seq 1 40); do curl -fs "$AWS_ENDPOINT_URL/minio/health/live" > /dev/null && break; sleep 0.5; done
echo "== 1. fake Atlas and the shrunk kitchen (fake_atlas.sh)"
[ -d "$W/atlas-states" ] && [ -d "$W/kitchen-small" ] || bash "$HERE/fake_atlas.sh" "$W" "$SRV" "$PY" | tail -6
round() { # round NAME COMMAND_JSON
  local name=$1 c=$2
  export RUN_ID=$((RANDOM * 7 + 1000)) S9_MODE=local MINUTE_S=${MINUTE_S:-15} CA_DIR=$W/ca-$name PACKAGES=$W/packages-$name
  export CM_PW; CM_PW=$(openssl rand -hex 24)
  export GITHUB_STEP_SUMMARY=$W/summary-$name.md
  local OUTD=$W/out-$name; rm -rf "$OUTD" "$PACKAGES"; mkdir -p "$OUTD"
  echo "== round $name: $(cat "$c")"
  echo "-- decide"
  "$PY" "$S9/ci/decide.py" "$c" "$W/resolved-$name.json" "${LOCAL_VCPU_LIMIT:-64}" || return 1   # local: no AWS quota
  # local mode: the same fields with small numbers (ballast 0.05 GB per database, 2 threads; small extra programs)
  "$PY" - "$W/resolved-$name.json" <<'PY'
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
c["data_gb_upper"] = 1
json.dump(c, open(f, "w"), indent=1)
PY
  echo "-- Atlas state files (the fake Atlas's, CATENA checked) and packages"
  ATLAS_STATES_DIR=$W/atlas-states bash "$ENG/kitchen/ci/atlas_states.sh" local "$W/atlas-$name.tgz" m10p m10s m30p m30s vecchio || return 1
  mkdir -p "$W/bin/mongo-server-x86_64" "$W/bin/pq-x86_64"
  cp "$SRV" "$W/bin/mongo-server-x86_64/mongo-server"; echo "local $(cd "$ENG" && git rev-parse --short HEAD)" > "$W/bin/mongo-server-x86_64/motore-commit.txt"
  cp "$PQ" "$W/bin/pq-x86_64/pq"
  S9=$S9 K=$W/kitchen-small CA_DIR=$CA_DIR PACKAGES=$PACKAGES BIN=$W/bin ATLAS_TGZ=$W/atlas-$name.tgz \
    PANTRY=$("$PY" "$S9/machine/cfg.py" "$W/resolved-$name.json" pantry_name) LOAD=$("$PY" -c "import json,sys; print(1 if json.load(open(sys.argv[1]))['atlas_load'] else 0)" "$W/resolved-$name.json") \
    WAIT_COOK_MIN=120 bash "$S9/ci/packages.sh" > "packages-$name.txt" 2>&1 || { tail -5 "packages-$name.txt"; return 1; }
  grep -E "^(client|node)" "packages-$name.txt"
  echo "-- run.py"
  local t; t=$(date +%s)
  "$PY" "$S9/ci/run.py" "$W/resolved-$name.json" "$OUTD" > "run-$name.log" 2>&1; local rc=$?
  echo "run.py: code $rc in $(( $(date +%s) - t )) s"
  grep -E "SPESA|BLOCCO|outcome|load:|m30 program|round stopped" "run-$name.log" | grep -v "^.*  client:" | sed -n '1p;$p'
  grep -E "BLOCCO|outcome:|load:|m30 program|round stopped|TIME CEILING" "run-$name.log"
  echo "-- teardown, secrets, final check"
  "$PY" "$S9/ci/teardown.py" local > "teardown-$name.txt" 2>&1; echo "teardown: code $?; $(grep -c deleted "teardown-$name.txt") deletions"
  "$PY" "$ENG/kitchen/ci/check_secrets.py" "$OUTD"; echo "secrets: code $?"
  "$PY" "$S9/ci/final_check.py" local; echo "final check: code $?"
}
C1=$W/command-full.json
if [ -n "$CMDF" ]; then cp "$CMDF" "$C1"; else
  # the local machine runs x86 only: x86 types for the log (m7i.large) instead of the Graviton default
  echo '{"mode": "run", "data": "100gb", "log": "m7i.large", "ceiling_min": 150}' > "$C1"; fi
round full "$C1"
if [ -n "${BLOCK_TEST:-}" ]; then
  echo '{"mode": "run", "data": "atlas", "log": "m7i.large", "spend_cap": 0.05, "programs": ["m30"]}' > "$W/command-block.json"
  round block "$W/command-block.json"
fi
kill $MINIO_PID 2>/dev/null
echo "== checks"
"$PY" "$HERE/check_local.py" "$W" full
echo "total time: $(( $(date +%s) - T0 )) s"
