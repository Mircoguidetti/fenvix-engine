#!/usr/bin/env bash
# Final spike measurements (docs/journal3z-libraries.md), run SEQUENTIALLY on a quiet machine.
# Results: journal3z/results/spikes/<candidate>-<mode>-<variant>.json
set -uo pipefail
cd "$(dirname "$0")"
OUT=../results/spikes
DIR=${J3Z_BENCH_DIR:-/tmp/j3z-bench}
mkdir -p "$OUT"
for c in proto custom openraft raftrs omnipaxos; do (cd $c && cargo build --release -q); done
bin() { echo "./$1/target/release/spike-$1"; }
run() { # name, env, candidate, args...
  local name=$1 envs=$2 cand=$3; shift 3
  echo "== $name" >&2
  env $envs timeout 600 $(bin $cand) --dir "$DIR/$cand" --out "$OUT/$name.json" "$@" > /dev/null 2>> "$OUT/stderr.log" || echo "FAILED $name" >&2
}
# latency: primary network, 3 runs each
for r in 1 2 3; do
  run proto-latency-r$r "" proto --mode latency --conc 8 --n 20000 --warmup 2000 --delay 0.5-1.0 --seed $r
  run custom-latency-r$r "" custom --mode latency --conc 8 --n 20000 --warmup 2000 --delay 0.5-1.0 --seed $r
  run openraft-latency-r$r "" openraft --mode latency --conc 8 --n 20000 --warmup 2000 --delay 0.5-1.0 --seed $r
  run openraft-batched-r$r "J3Z_INFLIGHT=1" openraft --mode latency_batched --conc 8 --n 20000 --warmup 2000 --delay 0.5-1.0 --seed $r
  run raftrs-latency-r$r "" raftrs --mode latency --conc 8 --n 20000 --warmup 2000 --delay 0.5-1.0 --seed $r
  run omnipaxos-latency-r$r "" omnipaxos --mode latency --conc 8 --n 20000 --warmup 2000 --delay 0.5-1.0 --seed $r
  run omnipaxos-bleoff-latency-r$r "OMNI_BLE=off" omnipaxos --mode latency --conc 8 --n 20000 --warmup 2000 --delay 0.5-1.0 --seed $r
done
# concurrency 64 and the secondary network (one run each)
for v in "proto|" "custom|" "openraft|" "raftrs|" "omnipaxos|"; do
  c=${v%%|*}
  run $c-conc64 "" $c --mode latency --conc 64 --n 20000 --warmup 2000 --delay 0.5-1.0
  run $c-delay1-2 "" $c --mode latency --conc 8 --n 20000 --warmup 2000 --delay 1-2
done
run openraft-batched-conc64 "J3Z_INFLIGHT=1" openraft --mode latency_batched --conc 64 --n 20000 --warmup 2000 --delay 0.5-1.0
run openraft-batched-delay1-2 "J3Z_INFLIGHT=1" openraft --mode latency_batched --conc 8 --n 20000 --warmup 2000 --delay 1-2
run omnipaxos-bleoff-conc64 "OMNI_BLE=off" omnipaxos --mode latency --conc 64 --n 20000 --warmup 2000 --delay 0.5-1.0
# idle groups
for g in 1 100 1000; do
  for c in proto custom openraft raftrs omnipaxos; do
    run $c-idle-$g "" $c --mode idle --groups $g --secs 10 --delay 0.5-1.0
  done
  run omnipaxos-bleoff-idle-$g "OMNI_BLE=off" omnipaxos --mode idle --groups $g --secs 10 --delay 0.5-1.0
done
# fencing / trim scenarios
for c in custom openraft raftrs omnipaxos; do run $c-fence "" $c --mode fence; done
for c in openraft raftrs omnipaxos; do run $c-trim "" $c --mode trim; done
echo done >&2
