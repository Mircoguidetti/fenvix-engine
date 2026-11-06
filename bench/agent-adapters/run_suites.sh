#!/usr/bin/env bash
# Run every adapter's own test suite against one target (mongod 7.0 or our server), both on localhost:27017.
#   run_suites.sh mongod|ours [suite ...]     suites: lg-py-saver lg-py-store lg-js oai llama mem0 mastra
# Each suite gets a fresh, empty target. JUnit XML goes to results/<target>/<suite>.xml, the console to <suite>.log.
# Packages and repositories are installed by install.sh under $ADAPTERS_HOME (default ~/adapters-src); never committed.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
A=${ADAPTERS_HOME:-/home/user/adapters}
T=${1:?target}; shift
SUITES=${*:-lg-py-saver lg-py-store lg-js oai llama mem0 mastra}
OUT="$HERE/results/$T"; mkdir -p "$OUT"
URI="mongodb://localhost:27017/?directConnection=true"
export MONGODB_URI="$URI" MONGODB_URL="$URI" MONGODB_REPLICA_SET_URL="$URI" MONGODB_DB_NAME=adapters_test
for s in $SUITES; do
  if [ $s = mastra ]; then
    # Mastra's CI runs a standalone mongod on 27017 and a replica set on 27019 (and Atlas Local on 27018 for vectors:
    # Atlas-only, not started here); the replica-set tests read MONGODB_RS_URL / MONGODB_REPLICA_SET_URL.
    { MODE=standalone "$HERE/target.sh" "$T" && PORT=27019 "$HERE/target.sh" "$T"; } > "$OUT/$s.target.log" 2>&1 \
      || { echo "$s: target failed to start"; cat "$OUT/$s.target.log"; continue; }
  else
    "$HERE/target.sh" "$T" > "$OUT/$s.target.log" 2>&1 || { echo "$s: target failed to start"; cat "$OUT/$s.target.log"; continue; }
  fi
  x="$OUT/$s.xml"; log="$OUT/$s.log"; start=$(date +%s)
  case $s in
    lg-py-saver) (cd "$A/langchain-mongodb/libs/langgraph-checkpoint-mongodb" && timeout 1800 "$A/venv-lg/bin/python" -m pytest -p no:cacheprovider -q tests --junitxml="$x") > "$log" 2>&1 ;;
    lg-py-store) (cd "$A/langchain-mongodb/libs/langgraph-store-mongodb" && timeout 1800 "$A/venv-lg/bin/python" -m pytest -p no:cacheprovider -q tests --junitxml="$x") > "$log" 2>&1 ;;
    lg-js) (cd "$A/js-lg" && timeout 1800 npx vitest run --mode int --reporter=default --reporter=junit --outputFile.junit="$x.int" && true; \
            timeout 600 npx vitest run --reporter=default --reporter=junit --outputFile.junit="$x.unit"; \
            "$A/venv-lg/bin/python" "$HERE/merge_junit.py" "$x" "$x.int" "$x.unit") > "$log" 2>&1 ;;
    oai) (cd "$A/openai-agents-python" && timeout 900 "$A/venv-oai/bin/python" -m pytest -p no:cacheprovider -q tests/extensions/memory/test_mongodb_session.py --junitxml="$x") > "$log" 2>&1 ;;
    llama) (cd "$A/llama_index/llama-index-integrations/storage/chat_store/llama-index-storage-chat-store-mongo" && \
            PYTHONPATH="$HERE" timeout 900 "$A/venv-llama/bin/python" -m pytest -p no:cacheprovider -p fake_docker -q tests --junitxml="$x") > "$log" 2>&1 ;;
    mem0) (cd "$A/mem0" && timeout 900 "$A/venv-mem0/bin/python" -m pytest -p no:cacheprovider -q tests/vector_stores/test_mongodb.py --junitxml="$x") > "$log" 2>&1 ;;
    mastra) (cd "$A/mastra/stores/mongodb" && unset MONGODB_URL MONGODB_URI && \
             RS="mongodb://localhost:27019/?directConnection=true" MONGODB_RS_URL="mongodb://localhost:27019/?directConnection=true" \
             MONGODB_REPLICA_SET_URL="mongodb://localhost:27019/?directConnection=true" timeout 3600 npx vitest run --reporter=default --reporter=junit --outputFile.junit="$x") > "$log" 2>&1 ;;
    *) echo "unknown suite $s"; continue ;;
  esac
  echo "$s on $T: $(( $(date +%s) - start )) s, $("$A/venv-lg/bin/python" "$HERE/compare.py" --count "$x" 2>/dev/null)"
done
"$HERE/target.sh" stopall
