#!/bin/bash
# Step 9 · the ballast generator machine (data "100gb" and "1tb", ballast_mode "generate"): runs mongo-server with the
# round's server options and --ballast (mongo-server/src/ballast.rs), which writes the ballast databases straight into
# the pantry as sealed runs and one version of our manifest each (the server's own insert code, no client, no wire, no
# WAL); then writes a state file in the founder's script's format per database (ballast_states.py) into the pantry
# (step9-ballast-states/, where the ballast rounds read them), writes generator/end.json and powers off.
# The plan (generator/plan.json) was made on the runner from the founder's script itself (ballast_plan.py).
# Usage: generator.sh NAME BUCKET
set -u
cd "$(dirname "$0")" || exit 1
P=$1; BUCKET=$2
source ./common.sh
finish() {
  step "finish: $1"; [ -n "${CPU_PID:-}" ] && kill "$CPU_PID" 2>/dev/null; [ -n "${PUSHER:-}" ] && kill "$PUSHER" 2>/dev/null
  printf '{"finished": true, "outcome": "%s"}\n' "$1" > out/end.json
  push; push; $PY s3io.py put "$BUCKET" "$P/end.json" out/end.json
  power_off "generator finished"
}
( while sleep "$PUSH_S"; do push; done ) & PUSHER=$!
step "generator start ($(machine_type) in $(zone)); $(nproc) vCPU, $(awk '/MemTotal/ {printf "%.1f GiB", $2/1048576}' /proc/meminfo), disk $(df -h . | awk 'NR==2 {print $4}') free"
$PY cpu_macchina.py out/cpu-machine.log 5 & CPU_PID=$!
./mongo-server --help 2>/dev/null | grep -q -- "--ballast" || finish NO_GENERATOR_IN_ENGINE_REF
for _ in $(seq 1 60); do exists "$P/config.json" && break; sleep 5; done
$PY s3io.py get "$BUCKET" "$P/config.json" out/config.json || finish NO_CONFIG
$PY s3io.py get "$BUCKET" "$P/plan.json" plan.json || finish NO_PLAN
cfg() { $PY cfg.py out/config.json "$1"; }
PANTRY=$(cfg pantry); OPTS=$(cfg server_options)
[ -n "$PANTRY" ] || finish BAD_CONFIG
read -ra EXTRA <<<"$OPTS"
# shellcheck disable=SC2207
END=($(s3_endpoint_args))
mkdir -p work/spill out/ballast-gen
CMD=(./mongo-server --store s3 --bucket "$PANTRY" --region eu-west-1 "${END[@]}" "${EXTRA[@]}" --ballast plan.json)
echo "${CMD[*]}" > out/generator-command.txt
step "start: ${CMD[*]}"
t0=$(date +%s)
"${CMD[@]}" > out/generator.log 2>&1 &
GEN=$!
while kill -0 "$GEN" 2>/dev/null; do
  sleep 30; l=$(grep -E "^ballast |^BALLAST" out/generator.log | tail -1); [ -n "$l" ] && step "$l"
  step "disk free $(df -h . | awk 'NR==2 {print $4}'), memory used $(free -m | awk '/Mem:/ {print $3}') MiB"
done
wait "$GEN"; rc=$?
grep -E "^BALLAST" out/generator.log | tee -a out/steps.log
step "generator exit code $rc after $(( $(date +%s) - t0 )) s"
gzip -kf out/generator.log
[ "$rc" = 0 ] || finish BALLAST_FAILED
$PY ballast_states.py plan.json out/ballast-gen work/states "mongo-server $(cat motore-commit.txt 2>/dev/null)" >> out/steps.log 2>&1 || finish NO_STATES
for d in work/states/*/; do n=$(basename "$d"); $PY s3io.py put "$PANTRY" "step9-ballast-states/$n/risultati_p1.json" "$d/risultati_p1.json" || finish NO_STATES; done
cp -r work/states out/ballast-states
finish BALLAST_GENERATED
