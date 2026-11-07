#!/usr/bin/env bash
# Compaction 2 step 2: the differential suites (diff-py, diff-node, every test) with customer keys on for the big
# collections on their most common field (often an array, a document or missing: the stress set of compaction-1), and
# envelope values on. Usage: bench/c2-step2/diff_keys.sh <binary> <out dir> [extra server flags...]
# Needs judge/setup.sh (mongod 7.0, MinIO). Same reference mongod and harness as judge/run.sh.
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd); cd "$ROOT"
BIN=${1:?binary}; OUT=${2:?out dir}; shift 2; mkdir -p "$OUT"
JH=${JUDGE_HOME:-$HOME/.fenvix-judge}; PY="$JH/env/bin/python"; export PATH="$JH/env/bin:$PATH"
KEYS="c=a,g=k,people=city,orders=cust,d=cust,a=v,u=k,f=v,e=v,h=v,b=x"
export SERVER_BIN="$BIN"
export SERVER_EXTRA="--ttl-sweep-secs 1 --replica-set rs0 --advertise localhost:%PORT% --customer-key $KEYS $*"
REAL="mongodb://localhost:27020/?directConnection=true"; OURS="mongodb://localhost:27018/?directConnection=true"; IDLE="mongodb://localhost:27019/?directConnection=true"
./mongo-tests/server.sh 27018 60 >/dev/null && ./mongo-tests/server.sh 27019 3 >/dev/null
(cd mongo-tests && "$PY" gen_tests.py tests.json > /dev/null)
(cd mongo-tests && "$PY" run_py.py --real "$REAL" --ours "$OURS" --idle "$IDLE" --out "$OUT/diff-py.json") > "$OUT/diff-py.log" 2>&1
./mongo-tests/server.sh 27018 60 >/dev/null && ./mongo-tests/server.sh 27019 3 >/dev/null
(cd mongo-tests/node && node run_node.js "--real=$REAL" "--ours=$OURS" "--idle=$IDLE" "--out=$OUT/diff-node.json") > "$OUT/diff-node.log" 2>&1
for p in diff-py diff-node; do
  "$PY" -c "import json,sys;r=json.load(open(sys.argv[1]));bad=[x.get('name') or x.get('id') for x in r if x['status']!='pass'];print(sys.argv[2], len(r)-len(bad),'/',len(r),'same as mongod; differ:',bad)" "$OUT/$p.json" $p
done | tee "$OUT/SUMMARY.txt"
grep -h '"docsWritten"\|customerKeys' "$OUT"/*.log 2>/dev/null | head -2
