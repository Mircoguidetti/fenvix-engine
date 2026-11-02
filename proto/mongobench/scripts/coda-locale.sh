#!/bin/bash
# M16 punto 1, in locale (2 vCPU): mongod come riferimento. Risultati in results/locale.jsonl
cd "$(dirname "$0")/.."
for rep in 1 2; do
  for mode in single rs3; do
    if [ $mode = rs3 ]; then W=majority; J=1; else W=1; J=0; fi
    python3 scripts/run.py results/locale.jsonl $mode 4 b1 16 32 $W $J 30 10 100000
    python3 scripts/run.py results/locale.jsonl $mode 7 b1 16 32 $W $J 30 10 100000
    python3 scripts/run.py results/locale.jsonl $mode 7 insert 16 32 $W $J 30 10 100000
    python3 scripts/run.py results/locale.jsonl $mode 7 b1 8 64 $W $J 30 10 100000
  done
done
python3 scripts/mongo.py down
echo FINE
