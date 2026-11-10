#!/usr/bin/env bash
# The end-to-end check: every adapter's agent loop (50 users x 200 turns, fake model) on mongod and on our server,
# each on a fresh target, then sizes. Results: results/<target>/e2e-<adapter>.json and .sizes.json; compare with compare_e2e.py.
#   run_e2e.sh mongod|ours [adapter ...]   adapters: langgraph openai-agents llamaindex mem0 langgraph-js mastra
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); A=${ADAPTERS_HOME:-/home/user/adapters}; PY=${JUDGE_HOME:-$HOME/.fenvix-judge}/env/bin/python
T=${1:?target}; shift; OUT="$HERE/results/$T"; mkdir -p "$OUT"
for a in ${*:-langgraph openai-agents llamaindex mem0 langgraph-js mastra}; do
  "$HERE/target.sh" "$T" > /dev/null || { echo "$a: target failed"; continue; }
  f="$OUT/e2e-$a.json"; db="e2e_${a//-/_}"
  case $a in
    langgraph) "$A/venv-lg/bin/python" "$HERE/e2e_py.py" $a "$f" ;;
    openai-agents) "$A/venv-oai/bin/python" "$HERE/e2e_py.py" $a "$f" ;;
    llamaindex) "$A/venv-llama/bin/python" "$HERE/e2e_py.py" $a "$f" ;;
    mem0) "$A/venv-mem0/bin/python" "$HERE/e2e_py.py" $a "$f" 2>&1 | grep -v "^Exception ignored\|meta_path\|^Traceback" ;;
    langgraph-js) cp "$HERE/e2e_js.mjs" "$A/js-lg/" && (cd "$A/js-lg" && node e2e_js.mjs $a "$f") ;;
    mastra) cp "$HERE/e2e_js.mjs" "$A/mastra/stores/mongodb/" && (cd "$A/mastra/stores/mongodb" && node e2e_js.mjs $a "$f") ;;
  esac > "$OUT/e2e-$a.log" 2>&1
  tail -n 1 "$OUT/e2e-$a.log"
  dbs=$("$PY" "$HERE/sizes.py" dbstats "$db")
  "$HERE/target.sh" stop   # a clean stop: our server flushes and closes the tenant before the bytes are counted
  if [ "$T" = ours ]; then s3=$("$PY" "$HERE/sizes.py" s3 "$db"); else s3=null; fi
  settled=null
  if [ "$T" = ours ]; then
    # the bytes right after the burst include compacted-away SSTs not yet collected: reopen the database, let the SST
    # cleanup run (unreferenced for --sst-gc-min-unref-secs 120, a pass every 60 s), stop cleanly, count again
    KEEP=1 "$HERE/target.sh" ours > /dev/null
    # the SST cleanup runs for databases open for writing: one tiny write (collection _settle, 1 document) opens it so
    "$PY" -c "import pymongo,sys;d=pymongo.MongoClient('mongodb://localhost:27017/?directConnection=true')[sys.argv[1]];d['_settle'].insert_one({'x':1})" "$db"
    sleep ${SETTLE_SECS:-300}; "$HERE/target.sh" stop; settled=$("$PY" "$HERE/sizes.py" s3 "$db")
  fi
  echo "{\"db\": \"$db\", \"dbStats\": $dbs, \"s3_at_stop\": $s3, \"s3_settled\": $settled}" > "$OUT/e2e-$a.sizes.json"
  cat "$OUT/e2e-$a.sizes.json"
done
