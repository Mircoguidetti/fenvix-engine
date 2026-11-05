#!/bin/bash
set -x
cd /home/claude/fenvix-engine/proto/quaderno
source results/q8-corsie/env-q8b.sh
O=results/q8-corsie/rev-q8b
mkdir -p $O
python3 /tmp/claude-0/s/percore.py $O/percore.jsonl & SP=$!
python3 scripts/corsie.py $O --lanes 1 --levels 1 --tag rev-a
python3 scripts/corsie.py $O --lanes 1 --levels 20 --phases nessuno,ventimila --tag rev-a
python3 scripts/corsie.py $O --lanes 1 --levels 1 --tag rev-a2
python3 scripts/corsie.py $O --lanes 1 --levels 20 --phases nessuno,ventimila --tag rev-a2
Q8_CPU_R=1 python3 scripts/corsie.py $O --lanes 1 --levels 20 --phases nessuno,ventimila --tag rev-b
kill $SP
echo FINE
