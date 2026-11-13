#!/bin/bash
# P-command-cpu: bench-big's three lines (rounds A and B) on the command-cpu engine, the same lines, cap, data plan, mix
# and rule as bench_big_speed.sh (harness bench-big-speed 9829dec), without the data-size operations. Our line gets
# --box-disk-mb 2048 as yesterday's run did (SERVER_FLAGS there).
#   BENCH_PW=<secret, environment only> TAG=2026-11-13-cpu bench_big_cpu.sh [lines...]
set -u
H=${HARNESS:-/home/user/harness/bench/agent-checkpoints}
PY=${BENCH_PY:-$HOME/.fenvix-agentbench-venv/bin/python}
: "${BENCH_PW:?}"; export BENCH_PW
TAG=${TAG:-$(date -u +%F)-cpu}
R=$H/results/big-$TAG; mkdir -p "$R"
say() { echo "$(date -u +%FT%TZ) $*" | tee -a "$R/driver.log"; }
{ echo "engine $(git -C /home/user/fenvix-engine rev-parse HEAD) (code b60a55c)"; echo "harness $(git -C $H rev-parse HEAD)"
  sha256sum "$H/../../target/release/mongo-server" "$H/../../proto/quaderno/target/release/pq" "$H/../../target/release/s3delay"; } > "$R/binaries.txt"
for l in ${*:-postgres-sync mongo-rs3 fenvix-log}; do
  case $l in postgres-sync) o=postgres; fl="";; mongo-rs3) o=mongo; fl="";; fenvix-log) o=fenvix; fl="--box-disk-mb 2048";; esac
  [ -e "$R/$o" ] && mv "$R/$o" "$R/$o.failed-$(date -u +%Y%m%dT%H%M%SZ)"
  say "$l -> $R/$o"
  "$PY" "$H/lines.py" run "$l" --big --mem 1g --profile big --seed 42 --workers 3 --mix-write 1000 --mix-read 1200 \
    --delay-ms 20 --min-free-gib 6 ${fl:+--server-flags "$fl"} --out "$R/$o" > "$R/$o.runner.txt" 2>&1
  rc=$?
  say "$l: exit $rc; $(tail -1 "$R/$o/terminal.txt" 2>/dev/null | cut -c1-300)"
  if [ $rc -eq 3 ]; then say "STOP: wrong reads on $l (RULES.md 8)"; "$PY" "$H/lines.py" down; exit 3; fi
done
"$PY" "$H/compare_big.py" "$R" > "$R/compare.out" 2>&1; say "compare_big exit $?"
"$PY" "$H/firstnext.py" "$R" > "$R/firstnext.out" 2>&1; say "firstnext exit $?"
