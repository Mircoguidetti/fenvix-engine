#!/usr/bin/env bash
# Extra, ours only: Mastra's vector suite expects Atlas Local (mongot) on 27018 with user mongodb/mongodb; community mongod
# cannot run it at all (Atlas-only). Run it against our server to see how much of the Atlas-only surface we already cover.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); A=${ADAPTERS_HOME:-/home/user/adapters}; PY=${JUDGE_HOME:-$HOME/.fenvix-judge}/env/bin/python
OUT="$HERE/results/ours-only"; mkdir -p "$OUT"
PORT=27018 "$HERE/target.sh" ours
"$PY" -c "import pymongo;pymongo.MongoClient('mongodb://127.0.0.1:27018/?directConnection=true').admin.command('createUser','mongodb',pwd='mongodb',roles=['root'])"
(cd "$A/mastra/stores/mongodb" && timeout 1800 npx vitest run src/vector --reporter=default --reporter=junit --outputFile.junit="$OUT/mastra-vector.xml") > "$OUT/mastra-vector.log" 2>&1
PORT=27018 "$HERE/target.sh" stop
"$PY" "$HERE/compare.py" --count "$OUT/mastra-vector.xml"
