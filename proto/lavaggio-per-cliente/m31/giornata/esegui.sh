#!/bin/bash
# M31 · la giornata su AWS, parte che gira SULLA MACCHINA (EC2 c7i.xlarge, Ubuntu 24.04, eu-west-1).
# La lancia lo user-data scritto da giornata.sh. Niente chiavi: S3 con il ruolo dell'istanza (IMDS).
# Ordine: carico P1 (10 GB) sul disco locale -> copia su S3 -> tempi su S3; carico 2 KB (2 GB) -> copia -> tempi;
# scritture da 1,9 KB su S3. I risultati salgono nel bucket sotto risultati/ ogni minuto e alla fine; poi la
# macchina si spegne da sola (spegnimento = cancellazione, impostato al lancio). Tetto: spegnimento forzato a 240 min.
set -u
cd "$(dirname "$0")"
export M31_BUCKET="$1" AWS_REGION=eu-west-1
B=./m31
mkdir -p out data
[ -n "${PROVA_LOCALE:-}" ] || shutdown -h +240 "M31: tetto di tempo" || true
passo() { echo "$(date -u +%FT%TZ) $*" | tee -a out/passi.log; }
sali() { $B copia --dir out --magazzino s3 --prefisso risultati > /dev/null 2>> out/errori-copia.log; }
( while sleep 60; do sali; done ) &
SALITORE=$!
passo "inizio; macchina $(nproc) vCPU, $(free -g | awk '/Mem/{print $2}') GB"
nproc > out/macchina.txt; lscpu | grep -i "model name" >> out/macchina.txt
# 1. il carico di P1: 10 GB, 5.120 clienti da circa 2 MB, documenti di circa 310 byte, 30% dormienti fino al 60%
passo "carica p1"
$B carica --disposizione cliente --livelli 4 --lavaggio 1 --taglia-gb ${GB_P1:-10} --regime 0 --clienti ${CLIENTI_P1:-5120} --dormienti 0.3 --fine-dormienti 0.6 \
  --blocco-kb 64 --compressione lz4 --documenti veri --attesa-fine 0 --ritmo-mbs ${RITMO:-6} --doc-byte 310 --dir data/p1 --etichetta aws-p1-10gb \
  --out out/carichi.jsonl > out/carica-p1.log 2>&1 || passo "ERRORE carica p1"
du -sb data/p1 >> out/dimensioni.txt
passo "copia p1"
$B copia --dir data/p1 --magazzino s3 --prefisso p1 >> out/copie.jsonl 2>> out/copie.log || passo "ERRORE copia p1"
passo "tempi p1"
$B tempi --dir data/p1 --magazzino s3 --prefisso p1 --n ${N:-150} --riscaldo ${RISCALDO:-400} --insieme 10,50,100 --giri-insieme 3 --q5-n ${Q5_N:-20} --q5-n-tutte ${Q5_N_TUTTE:-3} \
  --varianti serie,parallelo,parallelo+ra256,parallelo+bancone,parallelo+doppia,parallelo+bancone+doppia,parallelo+bancone+doppia+ra256 \
  --etichetta aws-p1-10gb --out out/tempi.jsonl > /dev/null 2> out/tempi-p1.log || passo "ERRORE tempi p1"
sali
# 2. documenti da 2 KB: 2 GB, 1.024 clienti da circa 2 MB
passo "carica d2k"
$B carica --disposizione cliente --livelli 4 --lavaggio 1 --taglia-gb ${GB_D2K:-2} --regime 0 --clienti ${CLIENTI_D2K:-1024} --dormienti 0.3 --fine-dormienti 0.6 \
  --blocco-kb 64 --compressione lz4 --documenti veri --attesa-fine 0 --ritmo-mbs ${RITMO:-6} --doc-byte 2000 --dir data/d2k --etichetta aws-d2k-2gb \
  --out out/carichi.jsonl > out/carica-d2k.log 2>&1 || passo "ERRORE carica d2k"
passo "copia d2k"
$B copia --dir data/d2k --magazzino s3 --prefisso d2k >> out/copie.jsonl 2>> out/copie.log || passo "ERRORE copia d2k"
passo "tempi d2k"
$B tempi --dir data/d2k --magazzino s3 --prefisso d2k --n ${N:-150} --riscaldo ${RISCALDO_D2K:-200} --ra-find 262144 --varianti parallelo,parallelo+bancone,parallelo+doppia,parallelo+bancone+doppia \
  --insieme "" --q5-n 0 --q5-n-tutte 0 --q5-senza-indice 0 --etichetta aws-d2k-2gb --out out/tempi.jsonl > /dev/null 2> out/tempi-d2k.log || passo "ERRORE tempi d2k"
sali
# 3. scritture come su Atlas P1: documenti da 1,9 KB, 32 fili; singole (WAL ogni 100 ms e ogni 10 ms) e a lotti da 100
for x in "1 100" "1 10" "100 100"; do
  set -- $x
  passo "scrivi lotto $1 flush $2"
  ./m31 scrivi --magazzino s3 --prefisso scrivi-l$1-f$2 --dir data/scrivi --secondi ${SECONDI_SCRIVI:-120} --fili 32 --lotto $1 --flush-ms $2 --attesa-lavaggio ${ATTESA_LAVAGGIO:-60} \
    --etichetta aws-scrivi-l$1-f$2 --out out/scritture.jsonl > /dev/null 2> out/scrivi-l$1-f$2.log || passo "ERRORE scrivi $1 $2"
done
sali
passo "fine"
kill $SALITORE 2>/dev/null
echo '{"fine": true}' > out/fine.json
sali
[ -n "${PROVA_LOCALE:-}" ] || shutdown -h now
