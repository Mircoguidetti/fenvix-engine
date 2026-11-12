#!/bin/bash
# integration-4: resume the ballast 10 GB proof after its server had to be killed (attempt 3: app_data_b03 stopped
# answering after the disk filled during mutate; see results/ballast-10gb/stuck-b03/). Same bucket, same reference mongod
# (still running on 27021, it holds the writes of mutate). A new server process with the product defaults + layout:
#   1. b03 must open and answer after the restart (lease of the killed process runs out first);
#   2. wait until the compactor's counters stop moving for 3 minutes (cap 45 minutes);
#   3. check 2 on the databases whose mutate finished (--only, from counts.json).
# Usage: ballast-proof-resume.sh OUTDIR SERVER_BINARY   (local only, $0)
set -u
OUT=$(cd "$1" && pwd); SRV=$2
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
B=$ROOT/research/ballast-on-i3
W=/home/user/work/i4-g10
JH=/root/.fenvix-judge; PY=$JH/env/bin/python; export PATH="$JH/env/bin:$PATH"
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
LAYOUT="--envelopes write --customer-key messaggi=cliente:ts,eventi=cliente:ts,ordini=cliente:ts"
say() { echo "$(date -u +%FT%TZ) $*" | tee -a "$OUT/proof.log"; }
sha256sum "$SRV" >> "$OUT/binary.txt"
say "resume: new server process ($SRV)"
"$SRV" --store s3 --bucket ballast --region us-east-1 --endpoint http://127.0.0.1:9000 $LAYOUT --replica-set rs0 --listen 127.0.0.1:27017 > "$OUT/server-resume.log" 2>&1 &
SP=$!
for _ in $(seq 1 120); do (exec 3<>/dev/tcp/127.0.0.1/27017) 2>/dev/null && break; sleep 1; done
OURS="mongodb://127.0.0.1:27017/?directConnection=true"; REF="mongodb://127.0.0.1:27021/?directConnection=true"
say "resume 1. app_data_b03 after the restart: $($PY - <<'P'
import pymongo, time
c = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true", serverSelectionTimeoutMS=5000, socketTimeoutMS=60000)
t0 = time.time(); last = ""
while time.time() - t0 < 300:
    try:
        n = c.app_data_b03.ordini.estimated_document_count(); d = c.app_data_b03.ordini.find_one({}, {"_id": 1})
        print(f"answers after {time.time() - t0:.0f} s: ordini estimated {n}, a document {d is not None}"); break
    except Exception as e:
        last = str(e)[:150]; time.sleep(5)
else:
    print(f"NO ANSWER in 300 s: {last}")
P
)"
T0=$(date +%s); last=""; same=0
while [ $(( $(date +%s) - T0 )) -lt 2700 ]; do
  cur=$($PY -c "import pymongo; c=pymongo.MongoClient('$OURS'); print(c.admin.command('serverStatus')['fenvix']['compactor'])" 2>/dev/null)
  echo "$(date -u +%T) $cur" >> "$OUT/compactor-polls.txt"
  if [ "$cur" = "$last" ]; then same=$((same + 1)); else same=0; last=$cur; fi
  [ $same -ge 6 ] && break
  sleep 30
done
say "resume 2. compactor idle after $(( $(date +%s) - T0 )) s (same for $same polls of 30 s): $last"
ONLY=$($PY -c "import json; print(','.join(json.load(open('$OUT/counts.json'))))")
say "resume 3. check 2 (after compaction) on $ONLY"
T0=$(date +%s)
$PY "$B/ballast_check.py" --ours "$OURS" --ref "$REF" --plan "$W/plan.json" --gen-dir "$W/out" --out "$OUT/check2.json" --keep-ref --counts "$OUT/counts.json" --only "$ONLY" > "$OUT/check2.log" 2>&1
say "check 2 (after compaction): exit $? in $(( $(date +%s) - T0 )) s; $(grep -E 'BALLAST CHECK' "$OUT/check2.log" | tail -1)"
kill -TERM $SP; T0=$(date +%s); for _ in $(seq 1 60); do kill -0 $SP 2>/dev/null || break; sleep 1; done
kill -0 $SP 2>/dev/null && { say "SIGTERM did not stop the server in 60 s; SIGKILL"; kill -9 $SP; } || say "SIGTERM stopped the server in $(( $(date +%s) - T0 )) s"
mongod --dbpath "$W/ref" --shutdown > /dev/null 2>&1
say "done"
