#!/bin/bash
# integration-4: the ballast 10 GB proof again on the final head (P-integration-4 section 5), with the ballast check's own
# tools (research/ballast-on-i3: plan, ballast_check.py, mutate.py) and its setup (sessions/2026-11-11-ballast-check,
# section 2): 3 databases of 3.4 GB of BSON generated with the round's layout options, our server with the product
# defaults, mongod 7.0 loaded with the sampled customers as the reference.
#   1. generate (mongo-server --ballast) into the local MinIO bucket "ballast";
#   2. check 1, before compaction: every read of every sampled customer the same as mongod (ballast_check.py);
#   3. the same writes on both servers (mutate.py: inserts past theta, updates, deletes), then wait until the compactor's
#      counters stop moving for 3 minutes (cap 45 minutes);
#   4. check 2, after compaction (--keep-ref, --counts from mutate.py).
# Usage: ballast-proof.sh OUTDIR   (local only, $0; ports: ours 27017, reference mongod 27021)
set -u
OUT=$(mkdir -p "$1" && cd "$1" && pwd)
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
B=$ROOT/research/ballast-on-i3
W=/home/user/work/i4-g10
JH=/root/.fenvix-judge; PY=$JH/env/bin/python; export PATH="$JH/env/bin:$PATH"
SRV=$ROOT/target/release/mongo-server
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
LAYOUT="--envelopes write --customer-key messaggi=cliente:ts,eventi=cliente:ts,ordini=cliente:ts"
say() { echo "$(date -u +%FT%TZ) $*" | tee -a "$OUT/proof.log"; }
# SKIP_GEN=1: the data set of an earlier attempt is kept (bucket "ballast" and $W/out); only the reference is rebuilt
if [ "${SKIP_GEN:-0}" = 1 ]; then rm -rf "$W/ref" "$W/spill"; mkdir -p "$W/spill" "$W/ref"; else rm -rf "$W"; mkdir -p "$W/out" "$W/spill" "$W/ref"; fi
sed "s#/home/user/work/g10#$W#g" "$B/results/plan.json" > "$W/plan.json"
sha256sum "$SRV" > "$OUT/binary.txt"
$PY - <<'P'
import boto3
s = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
try:
    s.create_bucket(Bucket="ballast")
except Exception as e:
    print("bucket:", e)
P
if [ "${SKIP_GEN:-0}" = 1 ]; then
say "1. generate: skipped (SKIP_GEN=1), the data set of the earlier attempt is reused: $(ls "$W/out" | tr '\n' ' ')"
else
say "1. generate (options: $LAYOUT)"
T0=$(date +%s)
"$SRV" --store s3 --bucket ballast --region us-east-1 --endpoint http://127.0.0.1:9000 $LAYOUT --ballast "$W/plan.json" > "$OUT/generator.log" 2>&1
say "generator exit $? in $(( $(date +%s) - T0 )) s; $(tail -2 "$OUT/generator.log" | tr '\n' ' ' | cut -c1-300)"
fi
cp "$W"/out/*.json "$OUT/" 2>/dev/null
say "2. our server (product defaults + layout) and the reference mongod"
"$SRV" --store s3 --bucket ballast --region us-east-1 --endpoint http://127.0.0.1:9000 $LAYOUT --replica-set rs0 --listen 127.0.0.1:27017 > "$OUT/server.log" 2>&1 &
SP=$!
mongod --port 27021 --bind_ip 127.0.0.1 --dbpath "$W/ref" --fork --logpath "$W/ref.log" > /dev/null
for _ in $(seq 1 120); do (exec 3<>/dev/tcp/127.0.0.1/27017) 2>/dev/null && (exec 3<>/dev/tcp/127.0.0.1/27021) 2>/dev/null && break; sleep 1; done
OURS="mongodb://127.0.0.1:27017/?directConnection=true"; REF="mongodb://127.0.0.1:27021/?directConnection=true"
T0=$(date +%s)
$PY "$B/ballast_check.py" --ours "$OURS" --ref "$REF" --plan "$W/plan.json" --gen-dir "$W/out" --out "$OUT/check1.json" > "$OUT/check1.log" 2>&1
RC=$?
say "check 1 (before compaction): exit $RC in $(( $(date +%s) - T0 )) s; $(grep -E 'BALLAST CHECK' "$OUT/check1.log" | tail -1)"
if [ $RC -ne 0 ]; then say "STOP: check 1 failed; nothing is written to the data set"; kill -TERM $SP; mongod --dbpath "$W/ref" --shutdown > /dev/null 2>&1; exit 1; fi
say "3. the same writes on both servers"
T0=$(date +%s)
$PY "$B/mutate.py" --ours "$OURS" --ref "$REF" --plan "$W/plan.json" --gen-dir "$W/out" --counts-out "$OUT/counts.json" > "$OUT/mutate.log" 2>&1
say "mutate exit $? in $(( $(date +%s) - T0 )) s"
T0=$(date +%s); last=""; same=0
while [ $(( $(date +%s) - T0 )) -lt 2700 ]; do
  cur=$($PY -c "import pymongo; c=pymongo.MongoClient('$OURS'); print(c.admin.command('serverStatus')['fenvix']['compactor'])" 2>/dev/null)
  echo "$(date -u +%T) $cur" >> "$OUT/compactor-polls.txt"
  if [ "$cur" = "$last" ]; then same=$((same + 1)); else same=0; last=$cur; fi
  [ $same -ge 6 ] && break
  sleep 30
done
say "compactor idle after $(( $(date +%s) - T0 )) s (same for $same polls of 30 s): $last"
say "4. check 2 (after compaction)"
T0=$(date +%s)
$PY "$B/ballast_check.py" --ours "$OURS" --ref "$REF" --plan "$W/plan.json" --gen-dir "$W/out" --out "$OUT/check2.json" --keep-ref --counts "$OUT/counts.json" > "$OUT/check2.log" 2>&1
say "check 2 (after compaction): exit $? in $(( $(date +%s) - T0 )) s; $(grep -E 'BALLAST CHECK' "$OUT/check2.log" | tail -1)"
kill -TERM $SP; wait $SP 2>/dev/null
mongod --dbpath "$W/ref" --shutdown > /dev/null 2>&1
say "done"
