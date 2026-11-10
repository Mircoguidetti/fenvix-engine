#!/usr/bin/env bash
# Stored bytes, reads and reseal CPU of agent checkpoints with and without derived documents (fx_derive), on every
# dump of the saver's documents in DUMPS (default /home/user/c2data/dumps). One JSON line per (dump, config) in
# results/bench.jsonl. Configs: today's sealed runs (lz4, plain), zstd plain, zstd + derived documents (the proposal),
# lz4 + derived, and zstd + derived without the reader's prefetch.
set -uo pipefail
cd "$(dirname "$0")/../.."
DUMPS=${DUMPS:-/home/user/c2data/dumps}
OUT=research/c2-delta-log/results/bench.jsonl
SAMPLE=${SAMPLE:-40}
for f in ${CELLS:-$(ls "$DUMPS"/*.fxd)}; do
  IFS=, read -ra CFGS <<< "${CONFIGS:-lz4 off,zstd off,zstd on,lz4 on,zstd on noprefetch}"
  for cfg in "${CFGS[@]}"; do
    set -- $cfg
    if [ -n "${3:-}" ] && [ "$3" = noprefetch ]; then export FX_PREFETCH=0; else unset FX_PREFETCH; fi
    echo "== $f $cfg" >&2
    timeout 3600 ./target/release/fx_derive "$f" "$1" "$2" "$SAMPLE" >> "$OUT" 2> >(grep -v "SIMULATED" >&2) || echo "FAILED $f $cfg" >&2
  done
done
