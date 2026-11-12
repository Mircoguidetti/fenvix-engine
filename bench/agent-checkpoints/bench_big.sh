#!/bin/bash
# P-bench-big (2026-11-12): the three equal-durability lines on data that does not fit in memory, one at a time, then
# the verdict. Run from this checkout after `bench_local.sh` STEPS=build (binaries in target/release and
# proto/quaderno/target/release) and `cargo build --release` in s3delay/ (copied to target/release/s3delay).
#
#   ENGINE_DIR=... ENGINE_COMMIT=<40 chars> BENCH_PW=<secret, environment only> TAG=2026-11-12 \
#     bench/agent-checkpoints/bench_big.sh [lines...]
#
# Fixed for every line, written before the first measured run: profile big, seed 42, members --cpus 1 --memory 1g,
# load with 3 worker processes, mix rounds A and B of 1000 write tasks and 1200 read tasks each, 20 ms in round B of
# our line. A wrong read (bench.py exit 3) is a STOP (RULES.md 8): nothing else runs.
set -u
HERE=$(cd "$(dirname "$0")" && pwd); ROOT=$(cd "$HERE/../.." && pwd)
: "${ENGINE_DIR:?}" "${ENGINE_COMMIT:?}" "${BENCH_PW:?}"
TAG=${TAG:-$(date -u +%F)}
PY=${BENCH_PY:-$HOME/.fenvix-agentbench-venv/bin/python}
R=$HERE/results/big-$TAG
mkdir -p "$R"
export BENCH_PW
say() { echo "$(date -u +%FT%TZ) $*" | tee -a "$R/driver.log"; }
got=$(git -C "$ENGINE_DIR" rev-parse HEAD)
[ "$got" = "$ENGINE_COMMIT" ] || { say "ENGINE_DIR is at $got, not $ENGINE_COMMIT"; exit 1; }
{ echo "engine $ENGINE_COMMIT ($ENGINE_DIR)"; echo "harness $(git -C "$ROOT" rev-parse HEAD)"
  sha256sum "$ROOT/target/release/mongo-server" "$ROOT/proto/quaderno/target/release/pq" "$ROOT/target/release/s3delay"; } > "$R/binaries.txt"
[ -e "$R/STOP" ] && { say "refused: $R/STOP exists"; exit 3; }
LINES=${*:-postgres-sync mongo-rs3 fenvix-log}
for l in $LINES; do
  case $l in postgres-sync) o=postgres;; mongo-rs3) o=mongo;; fenvix-log) o=fenvix;; *) say "unknown line $l"; exit 1;; esac
  [ -e "$R/$o" ] && mv "$R/$o" "$R/$o.failed-$(date -u +%Y%m%dT%H%M%SZ)"
  say "$l -> $R/$o"
  "$PY" "$HERE/lines.py" run "$l" --big --mem 1g --profile big --seed 42 --workers 3 --mix-write 1000 --mix-read 1200 \
    --delay-ms 20 --min-free-gib 8 --out "$R/$o" > "$R/$o.runner.txt" 2>&1
  rc=$?
  say "$l: exit $rc; $(tail -1 "$R/$o/terminal.txt" 2>/dev/null | cut -c1-300)"
  if [ $rc -eq 3 ]; then echo "$(date -u +%FT%TZ) wrong reads on $l" > "$R/STOP"; say "STOP (RULES.md 8)"; exit 3; fi
done
"$PY" "$HERE/compare_big.py" "$R" > /dev/null 2>&1 && say "verdict: $R/verdict.txt"
