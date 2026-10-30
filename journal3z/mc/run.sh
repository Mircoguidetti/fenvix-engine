#!/usr/bin/env bash
# The model-checking runs reported in docs/journal3z-tla.md. Output: journal3z/results/mc-*.json (one JSON line each).
# Total ≈ 2 h on one core per line (some lines run in parallel in the original session).
cd "$(dirname "$0")"
cargo build --release -q
B=./target/release/journal3z-mc
R=../results
ulimit -v 14000000
# one writer: exhaustive
$B --epochs 1 --idx 1 --dup                                   > $R/mc-e1-i1-dup.json
$B --epochs 1 --idx 2                                         > $R/mc-e1-i2.json
$B --epochs 1 --idx 2 --fifo                                  > $R/mc-e1-i2-fifo.json
# two writers (a stale one and a newer one): bounded
$B --epochs 2 --idx 2 --max-states 40000000                   > $R/mc-e2-i2.json
for m in no_fencing ack_on_one recovery_longest recovery_single trim_before_upload keep_diverged ack_gap; do
  $B --epochs 2 --idx 2 --mutation $m --max-states 20000000   > $R/mc-e2-i2-$m.json
done
# Not completed in phase 1 (stopped after 31 min at ~25 M states each, no result recorded):
#   $B --epochs 2 --idx 2 --fifo --max-states 100000000
#   $B --epochs 2 --idx 2 --fifo --mutation no_fencing --max-states 100000000
# Randomized order, FIFO, no_fencing, seeds 1-4 x 1.5 M states: not caught (see docs/journal3z-tla.md).
echo done
