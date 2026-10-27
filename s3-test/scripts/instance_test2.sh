#!/usr/bin/env bash
# Test 2 on the instance: the engine on real S3 (no custom endpoint, eu-central-1, instance-profile credentials) and a
# representative subset of the differential suites (pymongo core, Node). The suites compare against a real MongoDB 7.0
# started locally on :27017, exactly like run_all.sh does with Docker.
set -u
B=$S3T_BUCKET; ROOT=/opt/s3test; OUT=/var/lib/s3test/out; MV=7.0.14; NV=v22.22.0
cd "$ROOT"
curl -fsSL "https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-amazon2023-$MV.tgz" | tar xz -C /opt && mv /opt/mongodb-linux-x86_64-amazon2023-$MV /opt/mongodb || { echo "mongod download failed"; exit 1; }
curl -fsSL "https://nodejs.org/dist/$NV/node-$NV-linux-x64.tar.xz" | tar xJ -C /opt && mv /opt/node-$NV-linux-x64 /opt/node || { echo "node download failed"; exit 1; }
export PATH=/opt/mongodb/bin:/opt/node/bin:$PATH
mkdir -p /var/lib/mongo-real
mongod --dbpath /var/lib/mongo-real --bind_ip 127.0.0.1 --port 27017 --fork --logpath "$OUT/mongod.log" >/dev/null || { echo "mongod failed"; exit 1; }
(cd mongo-tests/node && npm ci --no-audit --no-fund >/dev/null 2>&1) || { echo "npm ci failed"; exit 1; }
python3 scripts/make_subset.py mongo-tests/tests.json "$OUT/subset.json" 5
export SERVER_BIN=$ROOT/scripts/engine-wrap.sh
chmod +x "$SERVER_BIN" mongo-tests/server.sh
cd mongo-tests
./server.sh 27018 60 && ./server.sh 27019 3 || { echo "engine did not start"; cat out/server-27018.log; exit 1; }
{ echo "engine command line:"; tr '\0' ' ' < /proc/$(cat out/server-27018.pid)/cmdline; echo; } > "$OUT/engine-cmdline.txt"
python3 -c 'import pymongo;print("pymongo",pymongo.version)' > "$OUT/versions.txt"; node -v >> "$OUT/versions.txt"; mongod --version | head -1 >> "$OUT/versions.txt"
python3 run_py.py --tests "$OUT/subset.json" --out "$OUT/py_results.json" > "$OUT/py_run.log" 2>&1; echo "py rc=$?"
(cd node && node run_node.js --tests="$OUT/subset.json" --out="$OUT/node_results.json" > "$OUT/node_run.log" 2>&1); echo "node rc=$?"
grep -E "^(core|extra):" "$OUT/py_run.log" "$OUT/node_run.log" | tee "$OUT/suites-summary.txt"
cp out/server-27018.log "$OUT/engine-27018.log"; cp out/server-27019.log "$OUT/engine-27019.log"
for p in 27018 27019; do python3 - $p <<'P'
import json,sys
from pymongo import MongoClient
r=MongoClient(f"mongodb://127.0.0.1:{sys.argv[1]}").admin.command("tenantStats")["requests"]
open(f"/var/lib/s3test/out/requests-{sys.argv[1]}.json","w").write(json.dumps(r))
P
done
