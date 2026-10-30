#!/usr/bin/env bash
# The measurements after part D, one at a time on MinIO, with the final engine binary ($ROUND3_ENGINE_BIN):
# the two part D runs to redo (with the part D binary), kill -9 in every mode, restart with 500 databases, ceiling of one
# database, noisy neighbours, then the long memory run. Each step is skipped when its result exists.
set -u
cd "$(dirname "$0")/../.."
PY=/tmp/claude-0/venv/bin/python
OUT=results-round3/minio
MINIO_ONLY=1 tools/fusion/env_up.sh > /dev/null
ROUND3_ENGINE_BIN=${D_BIN:?} tools/round3/run_d.sh
for m in before immediate adaptive relaxed local; do [ -f $OUT/kill/kill-$m.json ] || $PY tools/round3/killtest.py $m --trials 3 2>&1 | tail -1; done
[ -f $OUT/restart500.json ] || $PY tools/round3/restart500.py --dbs 500 --secs 120 --wakes 0,32 2>&1 | grep -v "^ *$" | tail -3
$PY tools/round3/ceiling.py 2>&1 | grep -c connections
[ -f $OUT/noisy.json ] || $PY tools/round3/noisy.py 2>&1 | tail -8
[ -f $OUT/memory/memlong.json ] || $PY tools/round3/memgrowth.py memlong 5400 --n 50 --rate 2 --out $OUT/memory/memlong.json 2>&1 | tail -1
echo "REST DONE"
