#!/usr/bin/env bash
# The simulator campaign reported in docs/journal3z-sim.md. Results: journal3z/results/sim-*.json
set -euo pipefail
cd "$(dirname "$0")"
cargo build --release -q
B=./target/release/journal3z-sim
R=../results
mkdir -p $R
$B run --seeds 1-1000 --steps 200000 --out $R/sim-main.json > /dev/null 2>&1
$B run --seeds 1001-1500 --steps 200000 --fault-every-ms 1000-3000 --out $R/sim-moderate.json > /dev/null 2>&1
$B run --seeds 2001-2200 --steps 200000 --dbs 10 --out $R/sim-10dbs.json > /dev/null 2>&1
$B run --seeds 3001-3100 --steps 200000 --calm --out $R/sim-calm.json > /dev/null 2>&1
$B run --seeds 4001-4200 --steps 200000 --double --out $R/sim-double.json > /dev/null 2>&1
$B controls --seeds 1-100 --steps 200000 --out $R/sim-controls.json > /dev/null 2>&1
$B scenario walbench_cycle1 > $R/sim-scenario-walbench-cycle1.json
$B scenario forget_fence > $R/sim-scenario-forget-fence.jsonl
echo done
