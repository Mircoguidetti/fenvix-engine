#!/bin/bash
# Giro 5 (M12), terza parte: A REGIME, con lo scaricatore acceso (moto come S3) e la finestra che si pota davvero.
# Finestra di 10 s (con 60 s la RAM delle 3 repliche non sta negli 8 GB del contenitore), 30 s di riscaldamento,
# 20 s di misura, cronometri fini del commit accesi.
cd "$(dirname "$0")/.."
export PQ_FLUSHER=1 PQ_GEN_NICE=-10 PQ_REAL=1 PQ_ZSTD=1 PQ_SW_FINE=1 PQ_WINDOW_MS=10000 PQ_WARM=30 PQ_SAMPLE=20
R=results
for k in 8 5 3; do python3 scripts/lotti.py $R/g5-regime10k-k$k 10000 32,64 512 2 $k > $R/g5-regime10k-k$k.log 2>&1; done
for k in 8 5; do python3 scripts/lotti.py $R/g5-regime5k-k$k 5000 64 512 2 $k > $R/g5-regime5k-k$k.log 2>&1; done
# solo misura: conflitti controllati solo sul documento (non sulle voci d'indice)
for k in 8 5; do PQ_VALIDA_SOLO_PRIMA=1 python3 scripts/lotti.py $R/g5-regime10k-solodoc-k$k 10000 64 512 2 $k > $R/g5-regime10k-solodoc-k$k.log 2>&1; done
echo FINE > $R/g5c-FINE
