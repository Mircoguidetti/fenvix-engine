#!/bin/bash
# Giro 5 (M12): tetto del quaderno con record "veri". Macchina da 2 vCPU: capo da solo sul core 0.
cd "$(dirname "$0")/.."
export PQ_GEN_NICE=-10 PQ_REAL=1
R=results
# 1) griglia principale: obiettivo 25.000 record/s, zstd 1 fuori dal thread principale
for k in 3 5 8; do PQ_ZSTD=1 python3 scripts/lotti.py $R/g5-tetto-k$k 25000 32,64 512 3 $k > $R/g5-tetto-k$k.log 2>&1; done
# 2) stesso carico a 10.000 record/s (raggiungibile da tutte le forme): i µs per record cambiano col ritmo?
for k in 3 5 8; do PQ_ZSTD=1 python3 scripts/lotti.py $R/g5-10k-k$k 10000 32,64 512 2 $k > $R/g5-10k-k$k.log 2>&1; done
# 3) confronti a 5 e 8 chiavi, lotto 32 e 64: senza compressione, compressione sul thread principale (giro 4)
for k in 5 8; do
  PQ_ZSTD=0 python3 scripts/lotti.py $R/g5-nozstd-k$k 25000 32,64 512 2 $k > $R/g5-nozstd-k$k.log 2>&1
  PQ_ZSTD=1 PQ_ZSTD_MAIN=1 python3 scripts/lotti.py $R/g5-zmain-k$k 25000 32,64 512 2 $k > $R/g5-zmain-k$k.log 2>&1
done
echo FINE > $R/g5-FINE
