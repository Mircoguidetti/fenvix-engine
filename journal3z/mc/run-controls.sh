#!/usr/bin/env bash
# Negative controls for the model checker (epochs 2, idx 2), 3 in parallel, cap 20 M states each.
cd "$(dirname "$0")"
B=./target/release/journal3z-mc
R=../results
ulimit -v 12000000
printf '%s\n' no_fencing ack_on_one recovery_longest recovery_single trim_before_upload keep_diverged ack_gap |
  xargs -P 3 -I{} sh -c "$B --epochs 2 --idx 2 --mutation {} --max-states 20000000 > $R/mc-e2-i2-{}.json 2>/dev/null"
echo done
