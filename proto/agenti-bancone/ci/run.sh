#!/bin/bash
set -u
D=proto/agenti-bancone
OUT=$D/tracce/tau2-classifica
mkdir -p $OUT
python3 $D/ci/scarica.py $D/ci/run.json $OUT 2>&1 | tee $OUT/log.txt
du -sh $OUT
