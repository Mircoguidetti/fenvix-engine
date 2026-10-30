#!/usr/bin/env bash
# §2b on the real programs: each negative control (12 protocol mutations + 3 takeover-rule bugs) runs on the §2a bench
# with the same faults, checks every 2 minutes, until the checker reports it or 120 minutes pass.
# Results in journal3z/results/phase2/p8/<mutation>/.   run-p8.sh <mutation> [<mutation> ...]
set -u
cd "$(dirname "$0")/../.."
B=./target/release/j3z-soak
R=../results/phase2/p8
WORK=${J3Z_WORK:-/tmp/j3z-soak}
mkdir -p "$R"
for m in "$@"; do
  "$B" --out "$R/$m" --work "$WORK/p8-$m" --seed 3000 --minutes 120 --check-every-min 2 --mutation "$m" > "$R/$m.stderr.log" 2>&1
  rc=$?
  gzip -f "$R/$m/events.txt" "$R/$m.stderr.log" 2>/dev/null
  rm -rf "$WORK/p8-$m"
  echo "$m exit $rc" >> "$R/runs.log"
done
