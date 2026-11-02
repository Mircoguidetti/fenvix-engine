#!/usr/bin/env bash
# Perché il banco 1 del giro 2 è più lento del giro 1: stessi 1b (1 mutazione da 200 B, 48 client, 15 s), 3 giri a
# turno con i binari del giro 1 (f023dae), del giro 2 (bb578d5, con 1 e con 2 thread di rete) e del giro 3 (corrente).
# I binari vecchi vanno compilati prima in /root/pq-work/tgt-<commit> (vedi rapporto).
cd "$(dirname "$0")/.."
OUT=${1:-results/ab-banco1}
mkdir -p "$OUT"
W=${PQ_WORK:-/root/pq-work}
run() { # nome cartella-del-banco variabili
  local name=$1 dir=$2; shift 2
  (cd "$dir" && env "$@" python3 scripts/cluster.py up --fresh > /dev/null && sleep 3 && ./target/release/pq commit --conc 48 --secs 15 --warm 3 --keys 1 --val 200 | sed "s/^{/{\"versione\":\"$name\",/" && python3 scripts/cluster.py down) >> "$OUT/ab.jsonl"
}
for c in f023dae bb578d5; do mkdir -p "$W/wt-$c/proto/quaderno/target/release"; ln -sf "$W/tgt-$c/release/pq" "$W/wt-$c/proto/quaderno/target/release/pq"; done
for rep in 1 2 3; do
  run giro1 "$W/wt-f023dae/proto/quaderno" X=1
  run giro2-1thread "$W/wt-bb578d5/proto/quaderno" PQ_IO_THREADS=1
  run giro2 "$W/wt-bb578d5/proto/quaderno" X=1
  run giro3 "$(pwd)" X=1
done
