#!/usr/bin/env bash
# Full differential run: real MongoDB 7.0 on :27017, mongo-server on :27018 (60 s idle close) and
# :27019 (3 s idle close, for the idle/wake tests), MinIO on :9000 with bucket "bench".
# Prerequisites: docker (mongo:7.0, MinIO), cargo, python3 (venv at .venv with pymongo), node (npm install in node/).
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p out
docker start mongo7 >/dev/null 2>&1 || docker run -d --name mongo7 -p 27017:27017 mongo:7.0 >/dev/null
(cd .. && cargo build --release -p mongo-server)
./server.sh 27018 60 --ttl-sweep-secs 1 --replica-set rs0 --advertise localhost:27018
./server.sh 27019 3 --replica-set rs0 --advertise localhost:27019
./.venv/bin/python gen_tests.py tests.json
./.venv/bin/python run_py.py            | tee out/py_run.log
(cd node && node run_node.js)           | tee out/node_run.log
./.venv/bin/python bench_latency.py     | tee out/latency.log
./.venv/bin/python bench_requests.py    | tee out/requests.log
./.venv/bin/python report.py            | tee out/report.md
