#!/usr/bin/env bash
# Compaction 2 step 8: the agent adapters' own suites before, during and after a migration to compaction 2.
#   run_migration.sh [suite ...]          suites: lg-py-saver lg-py-store lg-js mastra (default: all four)
# before: mongo-server as today's engine (sealed runs off).
# during: mongo-server with sealed runs, the compactor, and --sealed-test-migrate-all: every database a suite creates is
#         migrated while the suite runs (begin, build, catch-up, publish, a 20 s window, commit).
# after:  the suite runs once to create its databases, every migration is waited for until committed, then the suite runs
#         again on the same server and bucket; the second run is the one reported.
# Results: results-step8/<phase>/<suite>.xml and .log, and the migration states seen (states-<suite>.txt).
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); ROOT=$(cd "$HERE/../.." && pwd)
A=${ADAPTERS_HOME:-/home/user/adapters}
SUITES=${*:-lg-py-saver lg-py-store lg-js mastra}
PY="$A/venv-lg/bin/python"; JPY=${JUDGE_HOME:-$HOME/.fenvix-judge}/env/bin/python
R="$HERE/results-step8"; mkdir -p "$R"
SEALED="--sealed-runs 0 --sealed-t-pin-ms 1000 --sealed-e1-wait-ms 0 --sealed-test-short-e1-wait --sealed-group-mb 1 --sealed-migration-poll-ms 200 --sealed-migration-window-ms 20000 --sealed-test-migrate-all"

states() {  # every database's newest migration state in the bucket(s)
  "$JPY" - <<'P'
import boto3, json, collections
s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
for b in ("adapters", "adapters-27019"):
    try:
        keys = [o["Key"] for p in s3.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix="tenants/") for o in p.get("Contents", []) if "/fx/migration/" in o["Key"]]
    except Exception:
        continue
    newest = collections.defaultdict(str)
    for k in keys:
        db = k.split("/")[1]
        newest[db] = max(newest[db], k)
    for db, k in sorted(newest.items()):
        st = json.loads(s3.get_object(Bucket=b, Key=k)["Body"].read())
        print(b, db, st["phase"], "gen", st["gen"], "groups", len(st["groups"]), "version", st["version"])
P
}

run_one() {  # $1 phase, $2 suite: one run of the suite's commands on the running target(s), without restarting them
  local phase=$1 s=$2 x="$R/$1/$2.xml" log="$R/$1/$2.log"; mkdir -p "$R/$1"
  local URI="mongodb://localhost:27017/?directConnection=true"
  export MONGODB_URI="$URI" MONGODB_URL="$URI" MONGODB_REPLICA_SET_URL="$URI" MONGODB_DB_NAME=adapters_test
  case $s in
    lg-py-saver) (cd "$A/langchain-mongodb/libs/langgraph-checkpoint-mongodb" && timeout 1800 "$PY" -m pytest -p no:cacheprovider -q tests --junitxml="$x") > "$log" 2>&1 ;;
    lg-py-store) (cd "$A/langchain-mongodb/libs/langgraph-store-mongodb" && timeout 1800 "$PY" -m pytest -p no:cacheprovider -q tests --junitxml="$x") > "$log" 2>&1 ;;
    lg-js) (cd "$A/js-lg" && timeout 1800 npx vitest run --mode int --reporter=default --reporter=junit --outputFile.junit="$x.int"; \
            timeout 600 npx vitest run --reporter=default --reporter=junit --outputFile.junit="$x.unit"; \
            "$PY" "$HERE/merge_junit.py" "$x" "$x.int" "$x.unit") > "$log" 2>&1 ;;
    mastra) (cd "$A/mastra/stores/mongodb" && unset MONGODB_URL MONGODB_URI && \
             RS="mongodb://localhost:27019/?directConnection=true" MONGODB_RS_URL="mongodb://localhost:27019/?directConnection=true" \
             MONGODB_REPLICA_SET_URL="mongodb://localhost:27019/?directConnection=true" timeout 3600 npx vitest run --reporter=default --reporter=junit --outputFile.junit="$x") > "$log" 2>&1 ;;
  esac
  echo "$phase $s: $("$PY" "$HERE/compare.py" --count "$x" 2>/dev/null)"
}

targets() {  # fresh targets for a suite
  if [ "$1" = mastra ]; then MODE=standalone "$HERE/target.sh" ours > /dev/null && PORT=27019 "$HERE/target.sh" ours > /dev/null
  else "$HERE/target.sh" ours > /dev/null; fi
}

cd "$ROOT"
for s in $SUITES; do
  # before
  unset FENVIX_SEALED_COMPACTOR; export OURS_EXTRA=""
  targets $s && run_one before $s
  "$HERE/target.sh" stopall
  # during
  export FENVIX_SEALED_COMPACTOR=small OURS_EXTRA="$SEALED"
  targets $s && run_one during $s
  states > "$R/during/states-$s.txt"; echo "  states during: $(awk '{print $3}' "$R/during/states-$s.txt" | sort | uniq -c | tr '\n' ' ')"
  "$HERE/target.sh" stopall
  # after: a first run creates the databases, every migration is committed, then the reported run
  targets $s && run_one after-warmup $s
  for _ in $(seq 1 120); do
    states > "$R/states-now.txt"
    [ -s "$R/states-now.txt" ] && ! grep -qv " committed " "$R/states-now.txt" && break
    sleep 2
  done
  mkdir -p "$R/after"; cp "$R/states-now.txt" "$R/after/states-$s.txt"
  echo "  states before the reported run: $(awk '{print $3}' "$R/after/states-$s.txt" | sort | uniq -c | tr '\n' ' ')"
  run_one after $s
  "$HERE/target.sh" stopall
done
