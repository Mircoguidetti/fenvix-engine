#!/bin/bash
# M16: quanto è sensibile mongod a 3 membri (7 indici, B1) a scelte che non sono nel confronto principale.
cd "$(dirname "$0")/.."
O=results/sensibilita.jsonl
MB_URI_EXTRA=retryWrites=false python3 scripts/run.py $O rs3 7 b1 16 32 majority 1 40 20 100000
python3 scripts/run.py $O rs3 7 b1 64 32 majority 1 40 20 100000
python3 scripts/run.py $O rs3 7 b1 16 32 majority 0 40 20 100000
python3 scripts/run.py $O rs3 7 b1 16 32 1 0 40 20 100000
python3 scripts/mongo.py down
echo FINE
