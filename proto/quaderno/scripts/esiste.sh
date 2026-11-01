#!/usr/bin/env bash
# Prova "delta + esiste" con cancellazioni e ricreazioni in gara, poi porcupine; e il controllo negativo (stesso $inc
# senza dipendenza: deve risultare Illegal). Uso: scripts/esiste.sh [cartella]
set -eu
cd "$(dirname "$0")/.."
OUT=${1:-results/esiste-in-gara}
mkdir -p "$OUT"
W=${PQ_WORK:-/root/pq-work}
PQ=./target/release/pq
S="python3 scripts/cluster.py"
cargo build --release -q
(cd lincheck && go build -o "$W/pq-lincheck" .)
for neg in "" "--senza-esiste"; do
  $S up --fresh > /dev/null
  $PQ esiste-race --secs 30 --keys 8 --incs 6 --readers 2 $neg --out "$W/esiste$neg.csv" | tee -a "$OUT/risultati.jsonl"
  "$W/pq-lincheck" esiste "$W/esiste$neg.csv" 600 | tee -a "$OUT/risultati.jsonl" || true
done
$S down
