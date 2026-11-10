#!/usr/bin/env bash
# e2e loops on ours with derived documents on, then the derived-document check on the same bucket for the LangGraph loops.
set -uo pipefail
R=/tmp/claude-0/-home-user/9129b5ca-4194-55a1-8a84-bd2fce2b2dd9/scratchpad/run; H=$R/bench/agent-adapters; O=$H/results/ours; A=/home/user/adapters
export FENVIX_SEALED_COMPACTOR=${COMPACTOR:-small} FENVIX_SEALED_TPIN_MS=1000 FENVIX_SEALED_E1_WAIT_MS=0
export OURS_EXTRA="--sealed-runs 2 --sealed-compactor --sealed-derive --sealed-codec zstd --customer-key checkpoints=thread_id,checkpoint_writes=thread_id"
# watchdog: the MinIO disk is shared with the judge; stop the loop if less than 4 GB stays free
( while sleep 10; do a=$(df -B1M --output=avail / | tail -1); if [ "$a" -lt 4000 ]; then echo "WATCHDOG: only $a MB free, stopping" >&2; pkill -f e2e_py.py; pkill -f e2e_js.mjs; pkill -f reread_; fi; done ) &
WD=$!; trap "kill $WD" EXIT
for a in ${*:-langgraph langgraph-js mastra}; do
  echo "== $a $(date -u +%T)"
  $H/run_e2e.sh ours $a 2>&1 | grep -v "not a git"
  [ $a = mastra ] && continue
  # same bucket, warm restart: reseal now, compare raw bytes before/after, re-read every thread
  KEEP=1 $H/target.sh ours > /dev/null 2>&1; ps -o args= -p "$(cat ~/.fenvix-adapters/27017/pid)" > $O/derived-$a.cmdline.txt
  export THREADS_JSON=$O/e2e-$a.json; rm -f $O/derived-$a.json; $A/venv-lg/bin/python $R/derived_check.py $O/derived-$a.json > $O/derived-$a.log 2>&1; echo "derived_check exit $?"
  if [ $a = langgraph ]; then $A/venv-lg/bin/python $R/reread_lg.py $O/e2e-$a.json $O/reread-$a-after-reseal.json
  else cp $R/reread_lgjs.mjs $A/js-lg/ && (cd $A/js-lg && node reread_lgjs.mjs $O/e2e-$a.json $O/reread-$a-after-reseal.json); fi
  $A/venv-lg/bin/python -c "import pymongo,json;print('derived after re-read:',json.dumps(pymongo.MongoClient('mongodb://localhost:27017/?directConnection=true').admin.command('serverStatus')['fenvix']['derived']))"
  $H/target.sh stop
  # cold restart: every derived document rebuilt from what is in MinIO
  KEEP=1 $H/target.sh ours > /dev/null 2>&1
  if [ $a = langgraph ]; then $A/venv-lg/bin/python $R/reread_lg.py $O/e2e-$a.json $O/reread-$a-cold.json
  else (cd $A/js-lg && node reread_lgjs.mjs $O/e2e-$a.json $O/reread-$a-cold.json); fi
  $A/venv-lg/bin/python $R/raw_dump.py $O/derived-$a.json
  $A/venv-lg/bin/python -c "import pymongo,json;print('derived after cold re-read:',json.dumps(pymongo.MongoClient('mongodb://localhost:27017/?directConnection=true').admin.command('serverStatus')['fenvix']['derived']))"
  $H/target.sh stop
done
