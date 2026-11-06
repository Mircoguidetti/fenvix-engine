#!/bin/bash
# 20 TB, 7,6 MB/s di byte dell'utente (4.000 scritture/s x 1,9 KB), lavaggio con capacità ampia, GET proiettate.
CFG='['
for l0 in 64 256 1024; do
  CFG+="[\"--taglia-gb\",20480,\"--w-mbs\",7.6,\"--l0-mb\",$l0,\"--consolida\",0],"
  for c in 4 6; do CFG+="[\"--taglia-gb\",20480,\"--w-mbs\",7.6,\"--l0-mb\",$l0,\"--consolida\",$c],"; done
  for k in 3 4; do CFG+="[\"--taglia-gb\",20480,\"--w-mbs\",7.6,\"--l0-mb\",$l0,\"--consolida\",$k,\"--modo\",\"grandi\"],"; done
  for T in 2 3 4 6 8 16; do CFG+="[\"--taglia-gb\",20480,\"--w-mbs\",7.6,\"--l0-mb\",$l0,\"--modo\",\"livelli\",\"--T\",$T],"; done
done
CFG="${CFG%,}]"
nice python3 scripts/proietta.py "$CFG" 1e7 > risultati/proiezione-20tb.jsonl
