#!/usr/bin/env bash
# §2a long runs (docs/journal3z-phase2-plan.md): chunks of at most 2 h and 15-minute short runs, each with its own
# seed, results in journal3z/results/phase2/2a/<name>/. Stops at the first failing run (§2d: one failure stops all).
#   run-2a.sh chunk <n> [minutes=115]      one long chunk, seed 1000+n
#   run-2a.sh short <from> <to>            short runs from..to (seeds 2000+i), 15 minutes each
set -u
cd "$(dirname "$0")/../.."   # journal3z/p2
B=./target/release/j3z-soak
R=../results/phase2/2a
WORK=${J3Z_WORK:-/tmp/j3z-soak}
mkdir -p "$R"
run() { # name seed minutes
  local name=$1 seed=$2 min=$3
  "$B" --out "$R/$name" --work "$WORK/$name" --seed "$seed" --minutes "$min" > "$R/$name.stderr.log" 2>&1
  local rc=$?
  gzip -f "$R/$name/events.txt" 2>/dev/null
  gzip -f "$R/$name.stderr.log" 2>/dev/null
  rm -rf "$WORK/$name"
  echo "$name seed $seed exit $rc" >> "$R/runs.log"
  return $rc
}
case "$1" in
  chunk) run "chunk-$2" $((1000 + $2)) "${3:-115}" ;;
  short) for i in $(seq "$2" "$3"); do run "short-$(printf %02d "$i")" $((2000 + i)) 15 || exit 1; done ;;
esac
