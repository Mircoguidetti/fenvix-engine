#!/usr/bin/env bash
# e2e loops on ours with derived documents on, then the derived check on the same bucket for the LangGraph loops:
# plain restart path (no createIndexes, no registration step): warm restart, fenvixReseal, raw bytes before/after,
# saver re-read; cold restart, raw read (counters right after it), saver re-read.
set -uo pipefail
R=/tmp/claude-0/-home-user/9129b5ca-4194-55a1-8a84-bd2fce2b2dd9/scratchpad/fix2; H=$R/bench/agent-adapters; O=$H/results/ours; A=/home/user/adapters
export FENVIX_SEALED_COMPACTOR=${COMPACTOR:-small} FENVIX_SEALED_TPIN_MS=1000 FENVIX_SEALED_E1_WAIT_MS=0 FENVIX_SEALED_TEST_SHORT_E1_WAIT=1
export OURS_EXTRA="--sealed-runs 2 --sealed-compactor --sealed-derive --sealed-codec zstd --customer-key checkpoints=thread_id,checkpoint_writes=thread_id"
( while sleep 10; do a=$(df -B1M --output=avail / | tail -1); echo "$(date -u +%T) $a" >> $R/disk.log; if [ "$a" -lt 4000 ]; then echo "WATCHDOG: only $a MB free, stopping" >&2; pkill -f e2e_py.py; pkill -f e2e_js.mjs; pkill -f reread_; fi; done ) &
WD=$!; trap "kill $WD" EXIT
PY=$A/venv-lg/bin/python
ss() { $PY -c "import pymongo,json;f=pymongo.MongoClient('mongodb://localhost:27017/?directConnection=true').admin.command('serverStatus')['fenvix'];print('$1', json.dumps({'derived':f['derived'],'compactor':f['compactor']}))"; }
env_of() { { ps -o args= -p "$(cat ~/.fenvix-adapters/27017/pid)"; tr '\0' '\n' < /proc/$(cat ~/.fenvix-adapters/27017/pid)/environ | grep '^FENVIX_SEALED'; } > $1; }
for a in ${*:-langgraph langgraph-js mastra}; do
  echo "== $a $(date -u +%T)"
  $H/run_e2e.sh ours $a 2>&1 | grep -v "not a git"
  [ $a = mastra ] && continue
  KEEP=1 $H/target.sh ours > /dev/null 2>&1; env_of $O/derived-$a.cmdline.txt
  ss "[$a] after warm restart:"
  export THREADS_JSON=$O/e2e-$a.json; rm -f $O/derived-$a.json; (cd $R && $PY derived_check.py $O/derived-$a.json > $O/derived-$a.log 2>&1); echo "derived_check exit $?"
  $PY -c "import json;d=json.load(open('$O/derived-$a.json'));[print(db,'reseal',r['reseal'],'\n after reseal',r['derived_after_reseal'],'\n',{k:r[k] for k in ('checkpoints','checkpoint_writes')}) for db,r in d['databases'].items()]"
  if [ $a = langgraph ]; then (cd $R && $PY reread_lg.py $O/e2e-$a.json $O/reread-$a-after-reseal.json)
  else cp $R/reread_lgjs.mjs $A/js-lg/ && (cd $A/js-lg && node reread_lgjs.mjs $O/e2e-$a.json $O/reread-$a-after-reseal.json); fi
  ss "[$a] after re-read:"
  $H/target.sh stop
  KEEP=1 $H/target.sh ours > /dev/null 2>&1
  ss "[$a] after cold restart:"
  (cd $R && $PY raw_dump.py $O/derived-$a.json)
  ss "[$a] after cold raw read:"
  if [ $a = langgraph ]; then (cd $R && $PY reread_lg.py $O/e2e-$a.json $O/reread-$a-cold.json)
  else (cd $A/js-lg && node reread_lgjs.mjs $O/e2e-$a.json $O/reread-$a-cold.json); fi
  ss "[$a] after cold re-read:"
  $H/target.sh stop
done
