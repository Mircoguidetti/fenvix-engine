#!/usr/bin/env bash
# Banco 1: (a) costo della validazione sul leader, isolato; (b) commit completi; (c) record da 5 x 1 KB in byte/s.
# Uso: scripts/banco1.sh [cartella risultati] [a|b|c ...]. Il traffico tra repliche passa dal proxy; i client vanno
# diretti alle repliche. Ogni punto (b, c) si ripete 3 volte su un gruppo appena creato.
set -eu
cd "$(dirname "$0")/.."
OUT=${1:-results/banco1}
shift || true
PARTS=${*:-a b c}
mkdir -p "$OUT"
PQ=./target/release/pq
S="python3 scripts/cluster.py"
cargo build --release -q
$S down
for p in $PARTS; do
  case $p in
    a) for i in 1 2 3; do $PQ valbench --records 2000000 --keys 5 --dbs 10000 --keyspace 1000000 --window 1200000 --val 200; done | tee "$OUT/1a-validazione.jsonl" ;;
    b) for rep in 1 2 3; do
         $S up --fresh > /dev/null; sleep 3
         $PQ commit --conc 16,32,48,64,96 --secs 15 --warm 3 --keys 1 --val 200 --dbs 100 | sed "s/^{/{\"rep\":$rep,/" | tee -a "$OUT/1b-commit-1x200.jsonl"
         $S up --fresh > /dev/null; sleep 3
         $PQ commit --conc 16,32,48,64 --secs 15 --warm 3 --keys 5 --val 200 --dbs 100 | sed "s/^{/{\"rep\":$rep,/" | tee -a "$OUT/1b-commit-5x200.jsonl"
       done ;;
    c) for rep in 1 2 3; do
         $S up --fresh > /dev/null; sleep 3
         $PQ commit --conc 16,32,64 --secs 15 --warm 3 --keys 5 --val 1024 --dbs 100 | sed "s/^{/{\"rep\":$rep,/" | tee -a "$OUT/1c-commit-5x1k.jsonl"
       done ;;
  esac
done
$S down
