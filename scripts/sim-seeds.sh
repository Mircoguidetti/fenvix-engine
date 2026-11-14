#!/usr/bin/env bash
# sim-test-2: run seeds of the simulator (src/fx/sim) on one machine and keep the reports of the failing ones.
#   scripts/sim-seeds.sh FIRST COUNT [GEN] [PAR] [PATTERN]
# FIRST..FIRST+COUNT seeds; GEN 1 = sim-test's scripts, 2 = sim-test-2's (restore points, restores, forks, the generator,
# SlateDB's GC, storms, many groups), 3 = sim-test-3's (the replicated log with kept records/retry/takeover replay,
# drop/erase/re-create, incarnations); PAR seeds at a time (default: the number of CPUs); PATTERN (or $SIM_PATTERN) =
# mix (default: the seeds' usual scripts), slow-reads (the slow-read pattern often) or cold-restart. Reports go to sim-out/seed-N.txt,
# the summary to sim-out/summary.log. Exit 1 if a seed found a violation.
set -euo pipefail
FIRST=${1:?first seed}
COUNT=${2:?how many seeds}
GEN=${3:-2}
PAR=${4:-$(nproc)}
PATTERN=${5:-${SIM_PATTERN:-mix}}
case "$GEN" in 1|2|3) ;; *) echo "GEN must be 1, 2 or 3" >&2; exit 2;; esac
case "$PATTERN" in mix|slow-reads|cold-restart) ;; *) echo "PATTERN must be mix, slow-reads or cold-restart" >&2; exit 2;; esac
OUT=${SIM_OUT:-sim-out}
mkdir -p "$OUT"
cargo test --release --lib --no-run
# one test process per chunk of seeds: a seed's process-wide tables (holds, view cache) do not outlive its chunk
CHUNK=${SIM_CHUNK:-40}
A=$FIRST
END=$((FIRST + COUNT))
FAIL=0
while [ "$A" -lt "$END" ]; do
  B=$((A + CHUNK)); [ "$B" -gt "$END" ] && B=$END
  echo "seeds $A..$B (gen $GEN, pattern $PATTERN)"
  SIM_GEN=$GEN SIM_PATTERN=$PATTERN SIM_SEEDS=$A..$B SIM_STEPS=${SIM_STEPS:-90} SIM_PAR=$PAR SIM_REPLAYS=${SIM_REPLAYS:-2} \
    SIM_MIN_BUDGET=${SIM_MIN_BUDGET:-24} SIM_MIN_MAX=${SIM_MIN_MAX:-2} SIM_OUT=$OUT \
    cargo test --release --lib fx::sim::tests::sim_seeds -- --ignored --nocapture 2>&1 | grep -E "^SIM|panicked" | tee -a "$OUT/summary.log" || true
  A=$B
done
if grep -q "SIM VIOLATION" "$OUT/summary.log"; then FAIL=1; fi
grep -c "^SIM seed" "$OUT/summary.log" | sed 's/^/seeds run: /'
grep "SIM VIOLATION" "$OUT/summary.log" | sed 's/^/  /' || true
exit $FAIL
