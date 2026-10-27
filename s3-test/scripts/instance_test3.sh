#!/usr/bin/env bash
# Test 3 on the instance: dormant databases. See test3.py.
set -u
ROOT=/opt/s3test; OUT=/var/lib/s3test/out
export SERVER_BIN=$ROOT/scripts/engine-wrap.sh
export S3T_MAX_RPS=600   # one engine process only in test 3; at 300 the lease renewals of 2,000 open databases alone would hit the limiter
chmod +x "$SERVER_BIN" "$ROOT/mongo-tests/server.sh"
python3 "$ROOT/scripts/test3.py" --out "$OUT/test3.json" 2>&1 | tee "$OUT/test3.log"
