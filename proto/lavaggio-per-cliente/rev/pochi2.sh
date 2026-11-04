#!/bin/bash
# Dopo la 8 GB: clienti grandi (2 MB l'uno), stessa prova, misura con read-ahead e fetch diversi.
B=./target/release/lavaggio-per-cliente
while pgrep -f "matrice-rev[.]sh" > /dev/null; do sleep 20; done
ET=rev-v4-0.5gb-pochi-b
echo "$(date +%T) inizio $ET"
$B carica --disposizione cliente --livelli 4 --ritmo-mbs 2 --taglia-gb 0.5 --regime 2 --clienti 250 --dir data/$ET --etichetta $ET --out risultati-rev/carichi.jsonl > logs/$ET.log 2>&1
for cfg in "65536 4" "65536 1" "1048576 1" "4194304 1" "16777216 1"; do
  set -- $cfg
  REV_READ_AHEAD=$1 REV_FETCH_TASKS=$2 $B misura --disposizione cliente --dir data/$ET --etichetta "$ET-ra$1-ft$2" --n 200 --out risultati-rev/misure.jsonl >> logs/$ET.log 2>&1
  echo "$(date +%T) misura $cfg"
done
echo "$(date +%T) fine"
