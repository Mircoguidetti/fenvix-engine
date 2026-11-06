#!/bin/bash
# Matrice M26, seconda parte: scrittore a ritmo fisso (2 MB/s di byte dell'utente), così il lavaggio sta al passo come
# in produzione e le pile dipendono dalla strategia, non dalla CPU che manca. Più le basi senza lavaggio (CPU e L0).
set -u
B=./target/release/lavaggio-per-cliente
OUT=risultati/carichi.jsonl
MIS=risultati/misure.jsonl
run() { # taglia disp cons seg lav livelli ritmo
  local t=$1 d=$2 c=$3 s=$4 l=$5 v=$6 r=$7
  local et="${d}-c${c}-s${s}-l${l}-v${v}-r${r}-${t}gb"
  if grep -q "\"etichetta\":\"$et\"" $OUT 2>/dev/null; then echo "salto $et"; return; fi
  echo "$(date +%T) $et"
  $B carica --disposizione $d --consolida $c --segmenti $s --lavaggio $l --livelli $v --ritmo-mbs $r --taglia-gb $t --regime 2 --dir data/$et --etichetta $et --out $OUT > logs/$et.log 2>&1 || { echo "ERRORE $et"; tail -3 logs/$et.log; }
  if [ "$l" = "1" ]; then
    $B misura --disposizione $d --segmenti $s --dir data/$et --etichetta $et --n 200 --out $MIS >> logs/$et.log 2>&1 || { echo "ERRORE misura $et"; tail -3 logs/$et.log; }
  fi
  rm -rf data/$et
}
mkdir -p logs
# 2 GB in parallelo alla matrice 2 (lo scrittore a ritmo fisso lascia CPU libera)
R=2
run 2 cliente 0 0 1 0 $R
run 2 cliente 0 0 1 4 $R
run 2 cliente 0 0 0 0 0
echo "$(date +%T) fine"
