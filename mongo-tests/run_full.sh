#!/usr/bin/env bash
# Full differential run against the replica-set reference (mongod --replSet on :27020, ttlMonitorSleepSecs=1):
# pymongo then the Node driver, every test (core, extra, replset).
set -u
cd "$(dirname "$0")"
export SERVER_EXTRA="--ttl-sweep-secs 1 --replica-set rs0 --advertise localhost:%PORT%"
./server.sh 27018 60
./server.sh 27019 3
REAL="mongodb://localhost:27020/?directConnection=true"
OURS="mongodb://localhost:27018/?directConnection=true"
IDLE="mongodb://localhost:27019/?directConnection=true"
.venv/bin/python run_py.py --real "$REAL" --ours "$OURS" --idle "$IDLE" 2>&1 | tee out/full_py.log | grep -v "^\. "
(cd node && node run_node.js "--real=$REAL" "--ours=$OURS" "--idle=$IDLE" 2>&1 | tee ../out/full_node.log | grep -v "^\. ")
