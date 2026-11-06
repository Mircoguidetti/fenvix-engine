#!/bin/bash
set -u
G=$HOME/giro
$HOME/bins/ms-l1 --listen 127.0.0.1:27733 --store local --local-dir $G/store --replica-set rs0 > $G/server.log 2>&1 &
SPID=$!
sleep 3
cd $G/run
export NO_PROXY=127.0.0.1 no_proxy=127.0.0.1
unset ATLAS_PUBLIC_KEY ATLAS_PRIVATE_KEY
export FENVIX_P1_URI="mongodb://127.0.0.1:27733/?replicaSet=rs0"
PY=python3
DA=$(date -u -d '-1 hours' +%Y-%m-%dT%H:%M:%SZ)
PICCOLO="--n-q2 40 --dorm-q2 8 --n-q3 30 --dorm-q3 6 --n-q1 30 --dorm-q1 6 --n-q1d 30 --dorm-q1d 6 --n-q4 15
 --dorm-q4 6 --n-q5 20 --n-spiega 3 --livelli 1,10,40 --par-lotti 3,2,1 --par-secondi 6 --pausa-s 2
 --min-secondi-fase 3 --ogni-s 5 --n-scritture 20 --n-lotti-scritture 5 --lotto-scritture 20 --minuti-volume 0.15
 --pausa-volume-s 2 --thread-volume 4 --thread-massimo 8 --minuti-massimo 0.15 --grandi-gib 0.01
 --grandi-clienti 40 --grandi-thread 3 --grandi-lotto 50 --grandi-n-q2 10 --grandi-n-q1 10 --grandi-n-q3 10
 --grandi-n-q4 8 --fredda-gb 0.002 --fredda-lotto-mb 0.3 --fredda-attesa-s 5 --n-fredda 10 --n-fredda-100 3
 --n-cancella 4 --attesa-rete-min 0.3 --atlas-da $DA --n-condizione 6 --min-secondi-condizione 3
 --sfondo-riscaldamento-s 2"
$PY -I prova_infinite.py carica --gb 0.1 --mb-cliente 0.15 --thread 4 --lotto 500 --ogni-dbstats-s 10 --file stato.json > N0_carica.log 2>&1; echo "uscita $?" >> N0_carica.log
T0=$(date +%s)
$PY -I prova_infinite.py giro --senza atlas_inizio,atlas_fine --file stato.json $PICCOLO > N1_giro.log 2>&1; echo "uscita $?" >> N1_giro.log
echo "secondi $(( $(date +%s) - T0 ))" >> N1_giro.log
kill $SPID; wait $SPID
md5sum prova_infinite.py > md5_dopo.txt
echo FINITO > fatto
