#!/bin/bash
# Matrice M26: carica (riempimento + regime 2x) e misura, una configurazione alla volta (2 vCPU).
# uso: scripts/matrice.sh "TAGLIE" ; ogni riga di CONF = disposizione consolida segmenti lavaggio
set -u
B=./target/release/lavaggio-per-cliente
OUT=risultati/carichi.jsonl
MIS=risultati/misure.jsonl
run() { # taglia disp cons seg lav
  local t=$1 d=$2 c=$3 s=$4 l=$5
  local et="${d}-c${c}-s${s}-l${l}-${t}gb"
  if grep -q "\"etichetta\":\"$et\"" $OUT 2>/dev/null; then echo "salto $et"; return; fi
  echo "$(date +%T) $et"
  $B carica --disposizione $d --consolida $c --segmenti $s --lavaggio $l --taglia-gb $t --regime 2 --dir data/$et --etichetta $et --out $OUT > logs/$et.log 2>&1 || { echo "ERRORE $et"; tail -3 logs/$et.log; }
  if [ "$l" = "1" ]; then
    $B misura --disposizione $d --segmenti $s --dir data/$et --etichetta $et --n 200 --out $MIS >> logs/$et.log 2>&1 || { echo "ERRORE misura $et"; tail -3 logs/$et.log; }
  fi
  rm -rf data/$et
}
mkdir -p logs
for t in $1; do
  run $t cliente 0 0 1
  run $t cliente 0 0 0
  run $t cliente 4 0 1
  run $t cliente 2 0 1
  run $t cliente 6 0 1
  run $t cliente 4 16 1
  run $t oggi 0 0 1
  run $t collezione 0 0 1
  run $t oggi 0 0 0
done
echo "$(date +%T) fine"
