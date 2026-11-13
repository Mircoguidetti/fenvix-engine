#!/bin/bash
# P-B3 step 3 (2026-11-13): A/B in one run. Our line of bench-big (same harness, cap, plan, load, settle as step 1), loaded
# with the base binary (B3_BASE_BIN: compaction inside the cook), then 8 mix rounds, each after the bench's cold restart,
# alternating the base binary and the B3 binary (B3_NEW_BIN with --sealed-remote-compactor, plus a "compactor" container
# running --compactor-only app_demo), MinIO delay 0 or 20 ms: B0 R0 B20 R20 R0 B0 R20 B20.
#   BENCH_PW=... B3_BASE_BIN=... B3_NEW_BIN=... TAG=... b3_step3.sh
set -u
H=${HARNESS:-/home/user/harness/bench/agent-checkpoints}
PY=${BENCH_PY:-$HOME/.fenvix-agentbench-venv/bin/python}
: "${BENCH_PW:?}"; export BENCH_PW
: "${B3_BASE_BIN:?}"; : "${B3_NEW_BIN:?}"; export B3_BASE_BIN B3_NEW_BIN
TAG=${TAG:-$(date -u +%F)-b3s3}
R=${OUT_ROOT:-/home/user/b3}/big-$TAG; mkdir -p "$R/compactor-logs"
export B3_LOG=$R/b3ab.jsonl B3_COMPACTOR_LOGS=$R/compactor-logs
cp "$B3_BASE_BIN" "$H/../../target/release/mongo-server"
S="$PY $H/b3ab.py"
r() { echo "\"$1={me} delay $2 && $S $3 && {me} restart {line} && $S post $3\""; }
ROUNDS="[$(r base-d0-1 0 base),$(r remote-d0-1 0 remote),$(r base-d20-1 20 base),$(r remote-d20-1 20 remote),$(r remote-d0-2 0 remote),$(r base-d0-2 0 base),$(r remote-d20-2 20 remote),$(r base-d20-2 20 base)]"
{ echo "harness $(git -C $H rev-parse HEAD) + working tree"; sha256sum "$B3_BASE_BIN" "$B3_NEW_BIN" "$H/../../proto/quaderno/target/release/pq" "$H/../../target/release/s3delay"; echo "rounds $ROUNDS"; } > "$R/binaries.txt"
"$PY" "$H/lines.py" run fenvix-log --big --mem 1g --profile big --seed 42 --workers 3 --mix-write 300 --mix-read 300 \
  --delay-ms 20 --min-free-gib 6 --server-flags "--box-disk-mb 2048" --rounds "$ROUNDS" --out "$R/fenvix" > "$R/fenvix.runner.txt" 2>&1
echo "exit $?" >> "$R/fenvix.runner.txt"
