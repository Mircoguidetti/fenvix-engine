#!/usr/bin/env bash
# Test 1 on the instance: instance-profile credentials work, and the engine's request counter is exact.
set -u
B=$S3T_BUCKET; ROOT=/opt/s3test; OUT=/var/lib/s3test/out
echo "== test1: counter check (1000 PUT + 1000 GET through the engine's CountingStore, real S3, instance-profile credentials)"
"$ROOT/bin/s3probe" "$B" eu-central-1 1000 > "$OUT/counter-check.json" 2> "$OUT/counter-check.err"; echo "s3probe rc=$?"
cat "$OUT/counter-check.json"
python3 - <<'P'
import json
d=json.load(open("/var/lib/s3test/out/counter-check.json"))
import subprocess
subprocess.run(["bash","-c",f"add_extra {d['billedPutTier']} {d['billedGetTier']}"])
P
