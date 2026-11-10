#!/usr/bin/env bash
# Variant of the LangGraph derived check: same loop, then after the warm restart the checkpoint collections are registered
# (register.py: createIndexes of the existing indexes, no data change) before fenvixReseal.
set -uo pipefail
R=/tmp/claude-0/-home-user/9129b5ca-4194-55a1-8a84-bd2fce2b2dd9/scratchpad/fix; H=$R/bench/agent-adapters; O=$H/results/ours-reg; A=/home/user/adapters; mkdir -p $O
export FENVIX_SEALED_COMPACTOR=1 FENVIX_SEALED_TPIN_MS=1000 FENVIX_SEALED_E1_WAIT_MS=0 FENVIX_SEALED_TEST_SHORT_E1_WAIT=1
export OURS_EXTRA="--sealed-runs 2 --sealed-compactor --sealed-derive --sealed-codec zstd --customer-key checkpoints=thread_id,checkpoint_writes=thread_id"
( while sleep 10; do a=$(df -B1M --output=avail / | tail -1); if [ "$a" -lt 4000 ]; then echo "WATCHDOG: only $a MB free, stopping" >&2; pkill -f e2e_py.py; pkill -f reread_; fi; done ) &
WD=$!; trap "kill $WD" EXIT
PY=$A/venv-lg/bin/python
ss() { $PY -c "import pymongo,json;f=pymongo.MongoClient('mongodb://localhost:27017/?directConnection=true').admin.command('serverStatus')['fenvix'];print('$1', json.dumps({'derived':f['derived'],'compactor':f['compactor']}))"; }
$H/target.sh ours > /dev/null 2>&1; ps -o args= -p "$(cat ~/.fenvix-adapters/27017/pid)" > $O/loop.cmdline.txt
E2E_USERS=50 E2E_TURNS=80 $PY $H/e2e_py.py langgraph $O/e2e-langgraph.json > $O/e2e-langgraph.log 2>&1; tail -n 1 $O/e2e-langgraph.log
ss "derived at the end of the loop (loop process):"
$H/target.sh stop
KEEP=1 $H/target.sh ours > /dev/null 2>&1; ps -o args= -p "$(cat ~/.fenvix-adapters/27017/pid)" > $O/derived.cmdline.txt
ss "derived after warm restart:"
$PY $R/register.py e2e_langgraph
ss "derived after register:"
export THREADS_JSON=$O/e2e-langgraph.json; (cd $R && $PY derived_check.py $O/derived-langgraph.json > $O/derived-langgraph.log 2>&1); echo "derived_check exit $?"
$PY -c "import json;d=json.load(open('$O/derived-langgraph.json'));r=d['databases']['e2e_langgraph'];print('reseal',r['reseal']);print('after reseal',r['derived_after_reseal']);[print(k,r[k]) for k in ('checkpoints','checkpoint_writes')]"
(cd $R && $PY reread_lg.py $O/e2e-langgraph.json $O/reread-after-reseal.json)
ss "derived after re-read:"
$H/target.sh stop
KEEP=1 $H/target.sh ours > /dev/null 2>&1
(cd $R && $PY raw_dump.py $O/derived-langgraph.json)
ss "derived after cold restart raw read (before any write):"
(cd $R && $PY reread_lg.py $O/e2e-langgraph.json $O/reread-cold.json)
ss "derived after cold re-read:"
$H/target.sh stop
