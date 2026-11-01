#!/usr/bin/env bash
# Banco 3: chiave calda. 64 client fanno $inc sullo stesso documento.
#  blind     = delta cieco (+1 applicato in ordine di registro, nessuna validazione)
#  validated = leggi alla posizione confermata più recente, scrivi il valore+1 con validazione (riprova al conflitto)
#  spec      = come validated ma la lettura è speculativa sul leader (ultima versione accettata, anche non confermata)
#  esiste    = scelta di docs/protocollo.md: delta con dipendenza (_id, esiste) preparato alla posizione più recente nota
# Tentativi interni per aggiornamento: 50 (oltre: abort). Uso: scripts/banco3.sh [cartella] [secondi]
set -eu
cd "$(dirname "$0")/.."
OUT=${1:-results/banco3}
SECS=${2:-60}
mkdir -p "$OUT"
PQ=./target/release/pq
S="python3 scripts/cluster.py"
cargo build --release -q
for rep in 1 2; do
  for m in blind esiste validated spec; do
    $S up --fresh > /dev/null; sleep 3
    $PQ hot --mode $m --clients 64 --secs "$SECS" --max-attempts 50 | sed "s/^{/{\"rep\":$rep,/" | tee -a "$OUT/3-chiave-calda.jsonl"
  done
done
$S down
