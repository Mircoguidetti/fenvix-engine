#!/bin/bash
# Carica e misura i casi. Uso: scripts/matrice.sh [casi...]  (caso = carico:disposizione:blocco:prefisso)
set -e
cd "$(dirname "$0")/.."
BIN=./target/release/granularita-freddi
OUT=risultati/misure.jsonl
CASI=${@:-"chat:gruppo:4:0 chat:base:64:0 chat:gruppo:64:0 eventi:base:4:0 eventi:gruppo:4:0 chat:gruppo:4:1 eventi:base:64:0 eventi:gruppo:64:0 eventi:gruppo:4:1"}
for caso in $CASI; do
  IFS=: read carico disp blk pref <<< "$caso"
  e=$carico-$disp-$blk; [ "$pref" = 1 ] && e=$e-prefisso
  d=data/$e
  if [ ! -f $d/verita.json ]; then
    rm -rf $d
    $BIN carica --carico $carico --disposizione $disp --blocco $blk --prefisso $pref --dir $d 2> risultati/carica-$e.log | tee -a risultati/carichi.jsonl
    du -sb $d | awk -v e=$e '{print "{\"etichetta\":\""e"\",\"tipo\":\"disco\",\"byte\":"$1"}"}' >> $OUT
  fi
  for b in S0 S1; do
    $BIN misura --carico $carico --disposizione $disp --prefisso $pref --dir $d --bancone $b --n 300 --out $OUT --etichetta $e 2>> risultati/misura-$e.log
  done
  $BIN misura --carico $carico --disposizione $disp --prefisso $pref --dir $d --bancone S1 --n 100 --ritardo-ms 20 --out $OUT --etichetta $e 2>> risultati/misura-$e.log
  rm -rf $d
done
echo FATTO
