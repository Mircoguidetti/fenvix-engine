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
# 4 GB, livelli T=4 e senza tetto, ritmo 3 MB/s e regime 1x (per stare nel tempo e nel disco della macchina)
while pgrep -f "matrice3[.]sh" > /dev/null; do sleep 20; done
B4() { local t=$1 d=$2 c=$3 v=$4 r=$5; local et="${d}-c${c}-s0-l1-v${v}-r${r}-g1-${t}gb"
  grep -q "\"etichetta\":\"$et\"" $OUT 2>/dev/null && return
  echo "$(date +%T) $et"
  $B carica --disposizione $d --consolida $c --livelli $v --ritmo-mbs $r --taglia-gb $t --regime 1 --dir data/$et --etichetta $et --out $OUT > logs/$et.log 2>&1 || echo "ERRORE $et"
  $B misura --disposizione $d --dir data/$et --etichetta $et --n 200 --out $MIS >> logs/$et.log 2>&1 || echo "ERRORE misura $et"
  rm -rf data/$et; }
run 2 cliente 0 0 1 0 2
B4 4 cliente 0 4 3
B4 4 cliente 0 0 3
echo "$(date +%T) fine"
