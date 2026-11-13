#!/bin/bash
# P-B3 step 1 (2026-11-13): our line of bench-big (same harness, cap, data plan, load, settle and mix as command-cpu's
# rerun), then 8 rounds of the measured mix, each after the bench's cold restart: compaction on the cook's CPU ("shared",
# the bench as it is) or on a CPU of its own ("sep", b3sep.py), with MinIO's delay 0 or 20 ms, in the order
# S0 X0 S20 X20 X0 S0 X20 S20. Rounds are smaller than the bench's (300 write and 300 read tasks): every round needs
# threads not used before, and the plan has 4,956.
#   BENCH_PW=<secret, environment only> TAG=... b3_step1.sh
set -u
H=${HARNESS:-/home/user/harness/bench/agent-checkpoints}
PY=${BENCH_PY:-$HOME/.fenvix-agentbench-venv/bin/python}
: "${BENCH_PW:?}"; export BENCH_PW
TAG=${TAG:-$(date -u +%F)-b3s1}
R=${OUT_ROOT:-/home/user/b3}/big-$TAG; mkdir -p "$R"
export B3_LOG=$R/b3sep.jsonl
S="$PY $H/b3sep.py"
r() { echo "\"$1={me} delay $2 && $S prep && {me} restart {line} && $S $3\""; }
ROUNDS="[$(r shared-d0-1 0 shared),$(r sep-d0-1 0 separate),$(r shared-d20-1 20 shared),$(r sep-d20-1 20 separate),$(r sep-d0-2 0 separate),$(r shared-d0-2 0 shared),$(r sep-d20-2 20 separate),$(r shared-d20-2 20 shared)]"
{ echo "engine $(git -C /home/user/eng-base rev-parse HEAD)"; echo "harness $(git -C $H rev-parse HEAD) + working tree"
  sha256sum "$H/../../target/release/mongo-server" "$H/../../proto/quaderno/target/release/pq" "$H/../../target/release/s3delay"; echo "rounds $ROUNDS"; } > "$R/binaries.txt"
"$PY" "$H/lines.py" run fenvix-log --big --mem 1g --profile big --seed 42 --workers 3 --mix-write 300 --mix-read 300 \
  --delay-ms 20 --min-free-gib 6 --server-flags "--box-disk-mb 2048" --rounds "$ROUNDS" --out "$R/fenvix" > "$R/fenvix.runner.txt" 2>&1
echo "exit $?" >> "$R/fenvix.runner.txt"
$S prep || true
