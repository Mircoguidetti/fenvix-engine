#!/usr/bin/env bash
# ./run_area.sh <area> <files...>: runs one area of the unified spec tests against the engine on 27018.
set -u
area=$1; shift
cd "$(dirname "$0")"
mkdir -p results; rm -f results/$area.jsonl
${PY:-python3} unified_runner.py --uri "${URI:-mongodb://localhost:27018/?replicaSet=rs0&serverSelectionTimeoutMS=5000}" --area "$area" --out results/$area.jsonl "$@"
