#!/bin/bash
# Giro 5 (M12), seconda parte: a regime (finestra piena, potatura attiva) con i cronometri fini del commit.
# Con finestra di 60 s (o 20 s) a 10.000 record/s da 8 chiavi le 3 repliche superano la memoria del contenitore
# (8 GB, repliche uccise dal limite di memoria, MISURATO): qui 5.000 record/s e finestra di 10 s.
cd "$(dirname "$0")/.."
export PQ_GEN_NICE=-10 PQ_REAL=1 PQ_ZSTD=1 PQ_SW_FINE=1 PQ_WINDOW_MS=10000
R=results
rm -rf $R/g5-regime-k8 $R/g5-regime-k5 $R/g5-fine-k8w20*
for k in 8 5; do
  PQ_WARM=30 PQ_SAMPLE=20 python3 scripts/lotti.py $R/g5-regime-k$k 5000 64 512 2 $k > $R/g5-regime-k$k.log 2>&1
  PQ_WARM=3 PQ_SAMPLE=6 python3 scripts/lotti.py $R/g5-avvio-k$k 5000 64 512 2 $k > $R/g5-avvio-k$k.log 2>&1
done
echo FINE > $R/g5b-FINE
