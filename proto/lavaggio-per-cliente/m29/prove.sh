#!/bin/bash
# M29: le prove, una dopo l'altra (la macchina ha 2 vCPU: niente prove in parallelo).
set -u
cd "$(dirname "$0")"
B=./target/release/m29
mkdir -p logs risultati
CAR=risultati/carichi.jsonl
MIS=risultati/misure.jsonl
LETTORI="65536:4:65536:4,65536:1:4194304:1,65536:1:16777216:1,65536:1:0:1,131072:1:0:1"
run() { # etichetta taglia_gb clienti regime blocco_kb compressione lavaggio
  local et=$1 t=$2 c=$3 g=$4 bk=$5 comp=$6 lav=$7
  echo "$(date +%T) inizio $et"
  M29_ZSTD_LEVEL=1 $B carica --disposizione cliente --livelli 4 --lavaggio $lav --ritmo-mbs 2 --taglia-gb $t --regime $g --clienti $c \
    --blocco-kb $bk --compressione $comp --documenti veri --attesa-fine 0 --dir data/$et --etichetta $et --out $CAR > logs/$et.log 2>&1 || echo "ERRORE carica $et"
  if [ "$lav" = 1 ]; then
    $B misura --disposizione cliente --dir data/$et --etichetta $et --n 200 --taglia-gb $t --compressione $comp --lettori $LETTORI --out $MIS >> logs/$et.log 2>&1 || echo "ERRORE misura $et"
  fi
  du -sh data/$et
  rm -rf data/$et
  echo "$(date +%T) fine $et"
}
$B banco-compressione --mb 64 --out risultati/banco-compressione.jsonl > logs/banco.log 2>&1
# 1. la prova che decide: 8 GB, 4.096 clienti da 2 MB, blocchi 64 KB
run m29-8gb 8 4096 1 64 nessuna 1
# 2. compressione e taglia dei blocchi a 1 GB (512 clienti da 2 MB), stessi dati
run m29-1gb-b64-nessuna 1 512 1 64 nessuna 1
run m29-1gb-b64-lz4 1 512 1 64 lz4 1
run m29-1gb-b64-zstd1 1 512 1 64 zstd 1
run m29-1gb-b4-nessuna 1 512 1 4 nessuna 1
run m29-1gb-b4-lz4 1 512 1 4 lz4 1
run m29-1gb-b64-nessuna-senzalav 1 512 1 64 nessuna 0
# 3. 4 GB, 2.048 clienti da 2 MB
run m29-4gb 4 2048 1 64 nessuna 1
echo "$(date +%T) tutto fatto"
