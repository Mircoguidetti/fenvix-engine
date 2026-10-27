#!/usr/bin/env bash
# Differential suites (pymongo + Node.js) against the current target/release/mongo-server: run_suites.sh <label>
# Needs MongoDB 7.0 on :27017 and MinIO on :9000. Logs: loadtest/results/suites/<label>-{py,node}.log
set -u
label=${1:?label}
cd "$(dirname "$0")/../mongo-tests"
mkdir -p out ../loadtest/results/suites
pkill -x mongo-server; sleep 1
./server.sh 27018 60 >/dev/null && ./server.sh 27019 3 >/dev/null
python3 run_py.py > out/py_run.log 2>&1
cp out/py_run.log ../loadtest/results/suites/$label-py.log
(cd node && node run_node.js > ../out/node_run.log 2>&1)
cp out/node_run.log ../loadtest/results/suites/$label-node.log
pkill -x mongo-server
{ echo "$label"; grep -E "^(core|extra):" out/py_run.log | sed 's/^/py   /'; grep -E "^(core|extra):" out/node_run.log | sed 's/^/node /'; } | tee ../loadtest/results/suites/$label-summary.txt
touch /tmp/suites-$label-done
