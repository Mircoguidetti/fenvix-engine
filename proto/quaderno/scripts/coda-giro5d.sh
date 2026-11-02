#!/bin/bash
# Giro 5 (M12), quarta parte: a regime a 5.000 record/s (a 10.000 con 5 e 8 chiavi lo scaricatore con moto non tiene
# il passo, la RAM cresce e il contenitore uccide le repliche, MISURATO). Solo misura: conflitti solo sul documento.
cd "$(dirname "$0")/.."
export PQ_FLUSHER=1 PQ_GEN_NICE=-10 PQ_REAL=1 PQ_ZSTD=1 PQ_SW_FINE=1 PQ_WINDOW_MS=10000 PQ_WARM=30 PQ_SAMPLE=20
R=results
for k in 8 5; do PQ_VALIDA_SOLO_PRIMA=1 python3 scripts/lotti.py $R/g5-regime5k-solodoc-k$k 5000 64 512 2 $k > $R/g5-regime5k-solodoc-k$k.log 2>&1; done
python3 scripts/lotti.py $R/g5-regime5k-k3 5000 64 512 2 3 > $R/g5-regime5k-k3.log 2>&1
python3 scripts/lotti.py $R/g5-regime5k-k8-l32 5000 32 512 2 8 > $R/g5-regime5k-k8-l32.log 2>&1
echo FINE > $R/g5d-FINE
