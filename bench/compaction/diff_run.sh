#!/usr/bin/env bash
# Run the Python differential suite (or part of it) against a given mongo-server binary with extra server flags, the
# same way the judge's diff-py phase does (real mongod on 27020, ours on 27018/27019, MinIO on 9000).
# Usage: bench/compaction/diff_run.sh BINARY OUT.json "EXTRA FLAGS" [NAMES_FILE]
#   NAMES_FILE: one test name per line (only those run); default: every test.
# Example: the suite with customer-first keys on for the collections that have a natural customer field:
#   bench/compaction/diff_run.sh target/release/mongo-server /tmp/keyon.json "--customer-key orders=cust,d=cust"
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd); cd "$ROOT"
BIN=$(readlink -f "$1"); OUTF=$2; EXTRA=${3:-}; NAMES=${4:-}
JH=${JUDGE_HOME:-$HOME/.fenvix-judge}; PY="$JH/env/bin/python"; export PATH="$JH/env/bin:$PATH"
REAL="mongodb://localhost:27020/?directConnection=true"; OURS="mongodb://localhost:27018/?directConnection=true"; IDLE="mongodb://localhost:27019/?directConnection=true"
. tools/minio-env.sh
(exec 3<>/dev/tcp/127.0.0.1/27020) 2>/dev/null || { echo "the reference mongod is not up: run judge/run.sh diff-py once first"; exit 2; }
export SERVER_BIN="$BIN"
export SERVER_EXTRA="--ttl-sweep-secs 1 --replica-set rs0 --advertise localhost:%PORT% $EXTRA"
./mongo-tests/server.sh 27018 60 >/dev/null && ./mongo-tests/server.sh 27019 3 >/dev/null
(cd mongo-tests && "$PY" gen_tests.py tests.json > /dev/null)
TESTS="$ROOT/mongo-tests/tests.json"
if [ -n "$NAMES" ]; then
  TESTS=$(mktemp --suffix .json)
  "$PY" -c "import json,sys;n=set(open(sys.argv[2]).read().split());t=json.load(open(sys.argv[1]));json.dump([x for x in t if x['name'] in n],open(sys.argv[3],'w'))" "$ROOT/mongo-tests/tests.json" "$NAMES" "$TESTS"
fi
(cd mongo-tests && "$PY" run_py.py --tests "$TESTS" --real "$REAL" --ours "$OURS" --idle "$IDLE" --out "$OUTF")
"$PY" -c "import json,sys;r=json.load(open(sys.argv[1]));n=len(r);ok=sum(x['status']=='pass' for x in r);print(f'{ok}/{n} same answers as mongod 7.0');[print('FAIL',x['name']) for x in r if x['status']!='pass']" "$OUTF"
