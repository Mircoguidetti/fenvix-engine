source /home/claude/work/env.sh
S=/home/claude/eng/proto/quaderno/scripts/corsie.py
python3 $S /home/claude/work/q8b --levels 1,1 --tag q8b
python3 $S /home/claude/work/controlli --levels 30 --phases nessuno --tag controllo30
PQ_CORSIE=0 python3 $S /home/claude/work/controlli --lanes 0 --levels 1,2 --phases ventimila,raffiche --tag senzacorsie
echo FINE-CONTROLLI
