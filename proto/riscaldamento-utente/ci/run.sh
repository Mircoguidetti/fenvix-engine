#!/bin/bash
# Lanciato dal workflow. Legge ci/run.json e fa la parte $PARTE.
set -u
D=proto/riscaldamento-utente
OUT=$D/tracce/$PARTE
mkdir -p $OUT
python3 $D/ci/passo.py "$PARTE" "$OUT" 2>&1 | tee $OUT/log.txt
