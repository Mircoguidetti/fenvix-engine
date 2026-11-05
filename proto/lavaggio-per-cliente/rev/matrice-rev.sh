#!/bin/bash
# Revisore M26: prove rifatte (1 GB T=4, 1 GB senza tetto), granularità (pochi clienti grandi), taglia grande (8 GB T=4).
set -u
B=./target/release/lavaggio-per-cliente
OUT=risultati-rev/carichi.jsonl
MIS=risultati-rev/misure.jsonl
run() { # etichetta taglia livelli regime ritmo extra...
  local et=$1 t=$2 v=$3 g=$4 r=$5; shift 5
  echo "$(date +%T) inizio $et"
  $B carica --disposizione cliente --livelli $v --ritmo-mbs $r --taglia-gb $t --regime $g --dir data/$et --etichetta $et --out $OUT "$@" > logs/$et.log 2>&1 || echo "ERRORE carica $et"
  $B misura --disposizione cliente --dir data/$et --etichetta $et --n 200 --out $MIS >> logs/$et.log 2>&1 || echo "ERRORE misura $et"
  du -sh data/$et
  echo "$(date +%T) fine $et"
}
run rev-v4-1gb 1 4 2 2
rm -rf data/rev-v4-1gb
run rev-v0-1gb 1 0 2 2
rm -rf data/rev-v0-1gb
run rev-v4-0.5gb-pochi 0.5 4 2 2 --clienti 250
rm -rf data/rev-v4-0.5gb-pochi
run rev-v4-8gb 8 4 1 2
echo "$(date +%T) tutto fatto"
