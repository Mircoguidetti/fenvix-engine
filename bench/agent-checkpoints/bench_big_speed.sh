#!/bin/bash
# P-big-speed (2026-11-12): bench-big again on the big-speed engine, the same lines, cap, data plan, mix and rule as
# bench_big.sh, plus (1) the NEXT requests of every read task (bigmix.py) and (2) the three operations that depend on
# data size (bigops.py) on mongod and on ours, with each line left up after its rounds (--keep), then taken down.
#
#   ENGINE_DIR=... ENGINE_COMMIT=<40 chars> BENCH_PW=<secret, environment only> TAG=2026-11-12-speed \
#     bench/agent-checkpoints/bench_big_speed.sh [lines...]
set -u
HERE=$(cd "$(dirname "$0")" && pwd); ROOT=$(cd "$HERE/../.." && pwd)
: "${ENGINE_DIR:?}" "${ENGINE_COMMIT:?}" "${BENCH_PW:?}"
TAG=${TAG:-$(date -u +%F)-speed}
PY=${BENCH_PY:-$HOME/.fenvix-agentbench-venv/bin/python}
R=$HERE/results/big-$TAG${RESULT_SUFFIX:-}
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
  case $l in postgres-sync) o=postgres; keep="";; mongo-rs3) o=mongo; keep=--keep;; fenvix-log) o=fenvix; keep=--keep;; *) say "unknown line $l"; exit 1;; esac
  [ -e "$R/$o" ] && mv "$R/$o" "$R/$o.failed-$(date -u +%Y%m%dT%H%M%SZ)"
  say "$l -> $R/$o"
  # PROFILE / MIX_W / MIX_R: a pilot only (the measured run uses the defaults: big, 1000, 1200)
  "$PY" "$HERE/lines.py" run "$l" --big --mem 1g --profile ${PROFILE:-big} --seed 42 --workers 3 --mix-write ${MIX_W:-1000} --mix-read ${MIX_R:-1200} \
    --delay-ms 20 --min-free-gib 6 $keep ${SERVER_FLAGS:+--server-flags "$SERVER_FLAGS"} --out "$R/$o" > "$R/$o.runner.txt" 2>&1
  rc=$?
  say "$l: exit $rc; $(tail -1 "$R/$o/terminal.txt" 2>/dev/null | cut -c1-300)"
  if [ $rc -eq 3 ]; then echo "$(date -u +%FT%TZ) wrong reads on $l" > "$R/STOP"; say "STOP (RULES.md 8)"; "$PY" "$HERE/lines.py" down; exit 3; fi
  if [ -n "$keep" ]; then
    case $l in mongo-rs3) w=mongo;; fenvix-log) w=fenvix;; esac
    say "$l: data-size operations"
    "$PY" "$HERE/bigops.py" "$w" "$R/$o" > "$R/$o.bigops.txt" 2>&1
    say "$l: bigops exit $?; $(tail -3 "$R/$o.bigops.txt" | tr '\n' ' ' | cut -c1-400)"
    for n in $(docker ps -a --format '{{.Names}}'); do docker logs "$n" 2>&1 | gzip > "$R/$o/logs/$n.after-bigops.log.gz"; done
    "$PY" "$HERE/lines.py" down
  fi
done
"$PY" "$HERE/compare_big.py" "$R" > /dev/null 2>&1 && say "verdict: $R/verdict.txt"
