#!/usr/bin/env bash
# run_part.sh <name-substring> : py runner for tests whose name contains the substring, against mongo7 (standalone) and a fresh server of ours
cd "$(dirname "$0")"
export SERVER_EXTRA="--ttl-sweep-secs 1 --replica-set rs0 --advertise localhost:%PORT%"
./server.sh 27018 60 >/dev/null 2>&1; ./server.sh 27019 3 >/dev/null 2>&1
R="mongodb://localhost:27020/?directConnection=true"
.venv/bin/python run_py.py --real "$R" --ours "mongodb://localhost:27018/?directConnection=true" --idle "mongodb://localhost:27019/?directConnection=true" --only "$1" --out out/part_$2.json 2>&1 | grep -v '^\. ' | cut -c1-${3:-500}
