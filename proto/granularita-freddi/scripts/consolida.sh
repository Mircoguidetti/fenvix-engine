#!/bin/bash
# Caso in più: dati raggruppati con un tetto ai sorted run (sorted_run_consolidation_threshold = 2).
set -e
cd "$(dirname "$0")/.."
BIN=./target/release/granularita-freddi
OUT=risultati/misure.jsonl
for carico in eventi chat; do
  e=$carico-gruppo-4-consolida2; d=data/$e
  rm -rf $d
  $BIN carica --carico $carico --disposizione gruppo --blocco 4 --consolida 2 --dir $d 2> risultati/carica-$e.log | tee -a risultati/carichi.jsonl
  du -sb $d | awk -v e=$e '{print "{\"etichetta\":\""e"\",\"tipo\":\"disco\",\"byte\":"$1"}"}' >> $OUT
  for b in S0 S1; do
    $BIN misura --carico $carico --disposizione gruppo --dir $d --bancone $b --n 300 --out $OUT --etichetta $e 2>> risultati/misura-$e.log
  done
  $BIN misura --carico $carico --disposizione gruppo --dir $d --bancone S1 --n 100 --ritardo-ms 20 --out $OUT --etichetta $e 2>> risultati/misura-$e.log
  rm -rf $d
done
echo FATTO
