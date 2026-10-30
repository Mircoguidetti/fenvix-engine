#!/usr/bin/env bash
# Exhaustive / bounded model checking runs reported in docs/journal3z-tla.md. Output: journal3z/results/mc-*.json
cd "$(dirname "$0")"
cargo build --release -q
B=./target/release/journal3z-mc
R=../results
ulimit -v 12000000   # 12 GB address space guard
$B --epochs 1 --idx 1 --dup                         > $R/mc-e1-i1-dup.json
$B --epochs 1 --idx 2                --max-states 40000000 > $R/mc-e1-i2.json
$B --epochs 2 --idx 2                --max-states 40000000 > $R/mc-e2-i2.json
for m in no_fencing ack_on_one recovery_longest recovery_single trim_before_upload keep_diverged ack_gap; do
  $B --epochs 2 --idx 2 --mutation $m --max-states 40000000 > $R/mc-e2-i2-$m.json
done
echo done
