#!/bin/bash
# P-bench-local (2026-11-11): the one local speed test at equal durability, end to end, rerunnable on any engine head
# with no change. Run from this checkout (branch bench-local-*, made from aws-ready: it holds the harness).
#
#   ENGINE_DIR=/path/to/engine-worktree ENGINE_COMMIT=<full 40-char id> S9WT=/path/to/aws-step9-worktree \
#   BENCH_PW=<any secret, environment only> TAG=2026-11-11 STEPS="build takeovers equal1 equal23 single w8 verdict" \
#     bench/agent-checkpoints/bench_local.sh
#
# Steps (each can be run alone, in this order):
#   build      check ENGINE_DIR is at ENGINE_COMMIT, copy its mongo-server and pq here (target/release/,
#              proto/quaderno/target/release/), record their sha256 in results/local-$TAG-binaries.txt
#              (the binaries are built in ENGINE_DIR beforehand: cargo build --release -p mongo-server; and in
#              proto/quaderno: cargo build --release)
#   takeovers  ENGINE_DIR/research/integration-3/takeover-run.sh (the step 9 harness of aws-step9, takeovers only), 50
#              log on then 50 log off, with SRV and PQ set to ENGINE_DIR's binaries; summaries copied to
#              step9/local/results-$TAG/takeovers-summary-{on,off}.json
#   equal1     repeat 1 of the equal-durability lines: postgres-sync, mongo-rs3, fenvix-log
#              -> results/local-$TAG-equal-r1/{postgres,mongo,fenvix}/  (the gate's input)
#   equal23    repeats 2 and 3 -> results/local-$TAG-equal-r2/, -r3/
#   single     postgres-single, mongo-single, fenvix-s3 -> results/local-$TAG-single/{postgres,mongo,fenvix}/
#   w8         the equal-durability lines with --workers 8 -> results/local-$TAG-equal-w8/{postgres,mongo,fenvix}/
#   verdict    step9/local/benchmark_report.py and step9/ci/gate.py -> results/local-$TAG-verdict.txt; checks the
#              binaries' sha256 again
# Fixed for every line, written before the first measured run (P-bench-local 3): seed 42, profile standard,
# --read-sample 300, --erase-users 100, --cold-threads 200, workers 1 (8 in w8). A line that exits non-zero stops the
# script: exit 3 from bench.py is a wrong read (RULES.md 8, a STOP).
set -u
HERE=$(cd "$(dirname "$0")" && pwd); ROOT=$(cd "$HERE/../.." && pwd)
: "${ENGINE_DIR:?}" "${ENGINE_COMMIT:?}" "${BENCH_PW:?}"
TAG=${TAG:-$(date -u +%F)}
# the engine ref the report names (review of 2026-11-11, item 8: no default, the old default named the wrong ref)
: "${ENGINE_REF:?ENGINE_REF: the name of the engine ref under test, e.g. integration-4}"
STEPS=${STEPS:-build takeovers equal1 equal23 single w8 verdict}
PY=${BENCH_PY:-$HOME/.fenvix-agentbench-venv/bin/python}
JPY=${JUDGE_PY:-$HOME/.fenvix-judge/env/bin/python}
WORK=${WORK:-/tmp/fxbench-work}
R=$HERE/results
BENCH_ARGS="--profile standard --seed 42 --read-sample 300 --erase-users 100 --cold-threads 200"
export BENCH_PW
mkdir -p "$WORK" "$R"
say() { echo "$(date -u +%FT%TZ) $*" | tee -a "$R/local-$TAG-driver.log"; }

STOPF=$R/local-$TAG-STOP
# a line counts as done only when its runner said exit 0 and its summary says ok (review F3: a STOPped or failed line
# left a summary.json behind, and a rerun skipped it and carried it to the verdict)
line_done() { # line_done OUTDIR
  "$PY" - "$1" <<'PYD'
import json, os, sys
d = sys.argv[1]
try:
    lr = json.load(open(os.path.join(d, "line_result.json")))
    s = json.load(open(os.path.join(d, "summary.json")))
except Exception:
    sys.exit(1)
sys.exit(0 if lr.get("bench_rc") == 0 and s.get("ok") is True else 1)
PYD
}

line() { # line LINE OUTDIR [extra args]
  local l=$1 out=$2; shift 2
  [ -e "$STOPF" ] && { say "refused: $STOPF exists (a STOP of RULES.md 8); nothing runs until a person removes it"; exit 3; }
  if line_done "$out"; then say "$l: $out already done (exit 0, ok true), skipped"; return 0; fi
  if [ -e "$out" ]; then local old="$out.failed-$(date -u +%Y%m%dT%H%M%SZ)"; mv "$out" "$old"; say "$l: earlier attempt not ok, kept as ${old#$ROOT/}, run again"; fi
  say "$l -> ${out#$ROOT/} $*"
  "$PY" "$HERE/lines.py" run "$l" --out "$out" $BENCH_ARGS "$@" > "$WORK/$(basename "$(dirname "$out")")-$l.txt" 2>&1
  local rc=$?
  say "$l: exit $rc; $(tail -1 "$out/terminal.txt" 2>/dev/null | cut -c1-400)"
  if [ $rc -eq 3 ]; then say "STOP: wrong reads on $l (RULES.md 8)"; echo "$(date -u +%FT%TZ) wrong reads on $l -> ${out#$ROOT/}" > "$STOPF"; exit 3; fi
  [ $rc -eq 0 ] || { say "line $l failed (exit $rc): stopping"; exit $rc; }
}

for step in $STEPS; do
  case $step in
  build)
    got=$(git -C "$ENGINE_DIR" rev-parse HEAD)
    [ "$got" = "$ENGINE_COMMIT" ] || { say "ENGINE_DIR is at $got, not $ENGINE_COMMIT"; exit 1; }
    mkdir -p "$ROOT/target/release" "$ROOT/proto/quaderno/target/release"
    cp "$ENGINE_DIR/target/release/mongo-server" "$ROOT/target/release/mongo-server" || exit 1
    cp "$ENGINE_DIR/proto/quaderno/target/release/pq" "$ROOT/proto/quaderno/target/release/pq" || exit 1
    { echo "engine $ENGINE_COMMIT ($ENGINE_DIR)"
      sha256sum "$ENGINE_DIR/target/release/mongo-server" "$ROOT/target/release/mongo-server" \
        "$ENGINE_DIR/proto/quaderno/target/release/pq" "$ROOT/proto/quaderno/target/release/pq"; } > "$R/local-$TAG-binaries.txt"
    say "binaries: $(cut -c1-16 "$R/local-$TAG-binaries.txt" | tr '\n' ' ')"
    docker image inspect fenvix-step9-machine > /dev/null 2>&1 || docker build -q -t fenvix-step9-machine "$ROOT/step9/local" > /dev/null || exit 1
    ;;
  takeovers)
    : "${S9WT:?S9WT: a worktree of fenvix-engine aws-step9}"
    TD=$ROOT/step9/local/results-$TAG; mkdir -p "$TD"
    for mode in on off; do
      W=$WORK/takeovers-$mode; rm -rf "$W" "$W.head.sh" "$W.sh"
      say "takeovers log $mode: 50, $ENGINE_DIR/research/integration-3/takeover-run.sh"
      SRV=$ENGINE_DIR/target/release/mongo-server PQ=$ENGINE_DIR/proto/quaderno/target/release/pq \
        bash "$ENGINE_DIR/research/integration-3/takeover-run.sh" "$S9WT" "$W" "$mode" 50 > "$TD/takeovers-$mode.txt" 2>&1
      say "takeovers log $mode: exit $?; $(grep -E "outcome|verdict|lost" "$TD/takeovers-$mode.txt" | tail -3 | tr '\n' ' ' | cut -c1-400)"
      f=$(find "$W" -name 'takeovers-summary.json' -newer "$W.sh" 2>/dev/null | head -1)
      if [ -n "$f" ]; then cp "$f" "$TD/takeovers-summary-$mode.json"; say "takeovers $mode summary: ${f#$W/}"
        # the binary that made it, next to it (the gate checks it against the report's commit)
        sha256sum "$ENGINE_DIR/target/release/mongo-server" | cut -d' ' -f1 > "$TD/takeovers-summary-$mode.json.binary-sha256"
      else say "takeovers $mode: no takeovers-summary.json under $W"; fi
      for x in "$W"/out-*/takeovers*; do [ -e "$x" ] && cp -r "$x" "$TD/out-$mode-$(basename "$x")"; done
    done
    ;;
  equal1)
    for l in postgres-sync:postgres mongo-rs3:mongo fenvix-log:fenvix; do line "${l%%:*}" "$R/local-$TAG-equal-r1/${l##*:}"; done ;;
  equal23)
    for k in 2 3; do
      for l in postgres-sync:postgres mongo-rs3:mongo fenvix-log:fenvix; do line "${l%%:*}" "$R/local-$TAG-equal-r$k/${l##*:}"; done
    done ;;
  single)
    for l in postgres-single:postgres mongo-single:mongo fenvix-s3:fenvix; do line "${l%%:*}" "$R/local-$TAG-single/${l##*:}"; done ;;
  w8)
    for l in postgres-sync:postgres mongo-rs3:mongo fenvix-log:fenvix; do line "${l%%:*}" "$R/local-$TAG-equal-w8/${l##*:}" --workers 8; done ;;
  verdict)
    [ -e "$STOPF" ] && { say "refused: $STOPF exists (a STOP of RULES.md 8)"; exit 3; }
    cd "$ROOT" || exit 1
    V=$R/local-$TAG-verdict.txt
    # the --workers 8 line goes to the report too (fifth argument): the gate checks it like repeat 1 and refuses it
    # when its client was the limit (review F7)
    W8=bench/agent-checkpoints/results/local-$TAG-equal-w8/fenvix/summary.json
    SHA=$(sha256sum "$ENGINE_DIR/target/release/mongo-server" | cut -d' ' -f1)
    OFF=step9/local/results-$TAG/takeovers-summary-off.json
    { echo "\$ python step9/local/benchmark_report.py $ENGINE_REF $ENGINE_COMMIT step9/local/results-$TAG/takeovers-summary-on.json bench/agent-checkpoints/results/local-$TAG-equal-r1/fenvix/summary.json $W8 --takeovers-off $OFF --engine-sha256 $SHA"
      "$JPY" step9/local/benchmark_report.py "$ENGINE_REF" "$ENGINE_COMMIT" "step9/local/results-$TAG/takeovers-summary-on.json" \
        "bench/agent-checkpoints/results/local-$TAG-equal-r1/fenvix/summary.json" "$W8" --takeovers-off "$OFF" --engine-sha256 "$SHA"; echo "(exit $?)"
      echo "\$ ENGINE_REF=$ENGINE_REF ENGINE_COMMIT=$ENGINE_COMMIT python step9/ci/gate.py"
      ENGINE_REF=$ENGINE_REF ENGINE_COMMIT=$ENGINE_COMMIT "$JPY" step9/ci/gate.py; echo "(exit $?)"
      echo "binaries after the last run:"
      sha256sum "$ENGINE_DIR/target/release/mongo-server" "$ROOT/target/release/mongo-server" \
        "$ENGINE_DIR/proto/quaderno/target/release/pq" "$ROOT/proto/quaderno/target/release/pq"; } > "$V" 2>&1
    cat "$V"
    ;;
  *) say "unknown step $step"; exit 1 ;;
  esac
done
say "steps done: $STEPS"
