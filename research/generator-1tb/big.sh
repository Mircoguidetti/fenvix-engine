#!/bin/bash
# generator-1tb, task 3: the largest single database this sandbox's disk allows (20 GiB of BSON), generated with 4
# writers, killed with SIGKILL mid-way and resumed; then our server (product defaults) against mongod 7.0 loaded with the
# sampled customers (ballast_check.py); then identical writes on both (mutate.py), a takeover (SIGKILL of server A while
# its compactor works, server B opens the database), the compactor settles on B, and the check again.
# Usage: big.sh STEP  (gen | check1 | mutate | takeover | check2)
set -u
export PATH=/root/.fenvix-judge/env/bin:$PATH AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
E=/home/user/fenvix-engine; D=/home/user/work/big; R=$E/research/generator-1tb; mkdir -p $D
BUCKET=big20; GIB=${GIB:-20}
GOPTS="--envelopes write --customer-key messaggi=cliente:ts,eventi=cliente:ts,ordini=cliente:ts"
SOPTS="$GOPTS --replica-set rs0 --listen 127.0.0.1:27017"
OURS="mongodb://127.0.0.1:27017/?directConnection=true"; REF="mongodb://127.0.0.1:27018/?directConnection=true"
serve() { # serve NAME: our server with the product defaults (sealed runs 2, compactor, derived documents, T_pin 30 s)
  echo "$SOPTS" > $D/server-options.txt
  nohup $E/target/release/mongo-server --store s3 --bucket $BUCKET --region us-east-1 --endpoint http://127.0.0.1:9000 $SOPTS > $D/server-$1.log 2>&1 &
  echo $! > $D/server-$1.pid
  for _ in $(seq 1 120); do python3 -c "import pymongo; pymongo.MongoClient('$OURS', serverSelectionTimeoutMS=500).admin.command('ping')" 2>/dev/null && return 0; sleep 1; done
  return 1
}
ref() {
  pgrep -f "port 27018" > /dev/null && return 0
  mkdir -p $D/mongod; nohup mongod --dbpath $D/mongod --port 27018 --bind_ip 127.0.0.1 --wiredTigerCacheSizeGB 1 > $D/mongod.log 2>&1 &
  sleep 5
}
case "$1" in
gen)
  python3 /home/user/work/mkbucket.py $BUCKET 2>/dev/null
  echo "[[\"app_data_b01\",\"--gb\",\"$GIB\",\"--seme\",\"2001\",\"--thread\",\"16\",\"--quota-riservati\",\"0.4\"]]" > $D/lines.json
  # 35 sampled customers for the reference, as the ballast check; 4 writers (this sandbox's vCPU)
  python3 $E/step9/machine/ballast_plan.py $E/kitchen/irlanda $D/lines.json $D/plan.json --spill-dir $D/spill --out $D/out \
    --parallel 4 --sample-every $(( GIB * 512 / 35 )) --sample-max 35
  ( while sleep 5; do ps -o rss=,pcpu= -C mongo-server | awk -v t=$(date +%s) '{print t, $1, $2}'; df -B1 /home/user | awk -v t=$(date +%s) 'NR==2 {print t, "disk_used", $3}'; done ) > $D/rss.log 2>&1 &
  M=$!
  T0=$(date +%s)
  $E/target/release/mongo-server --store s3 --bucket $BUCKET --region us-east-1 --endpoint http://127.0.0.1:9000 $GOPTS --ballast $D/plan.json > $D/gen-part1.log 2>&1 &
  G=$!
  # SIGKILL once 6 slices are committed (mid-way), then the same command again
  until [ "$(grep -c 'slice .* done' $D/gen-part1.log)" -ge 6 ] || ! kill -0 $G 2>/dev/null; do sleep 2; done
  kill -9 $G; wait $G; T1=$(date +%s)
  echo "part 1: SIGKILL after $(( T1 - T0 )) s, $(grep -c 'slice .* done' $D/gen-part1.log) slices committed" | tee $D/gen-times.txt
  $E/target/release/mongo-server --store s3 --bucket $BUCKET --region us-east-1 --endpoint http://127.0.0.1:9000 $GOPTS --ballast $D/plan.json > $D/gen-part2.log 2>&1
  echo "part 2 (resume): exit $? after $(( $(date +%s) - T1 )) s; total wall $(( $(date +%s) - T0 )) s" | tee -a $D/gen-times.txt
  kill $M
  $E/target/release/fx_rows_digest http://127.0.0.1:9000 $BUCKET app_data_b01 | tee $D/digest.json
  python3 -c "
import boto3; s=boto3.client('s3',endpoint_url='http://127.0.0.1:9000',aws_access_key_id='minioadmin',aws_secret_access_key='minioadmin',region_name='us-east-1')
n=b=0; pre={}
for p in s.get_paginator('list_objects_v2').paginate(Bucket='$BUCKET'):
    for o in p.get('Contents',[]):
        n+=1; b+=o['Size']; k=o['Key'].split('/')[0]+'/'+(o['Key'].split('/')[2] if o['Key'].startswith('tenants/') and len(o['Key'].split('/'))>2 else ''); pre[k]=pre.get(k,0)+o['Size']
print('objects', n, 'bytes', b, pre)" | tee $D/s3-after-gen.txt
  ;;
check1)
  ref; { [ -f $D/server-A.pid ] && kill -0 $(cat $D/server-A.pid) 2>/dev/null; } || serve A || exit 1
  python3 $R/ballast_check.py --ours "$OURS" --ref "$REF" --plan $D/plan.json --gen-dir $D/out --out $D/check1.json --count-threads 8 --stato-window 0.015 > $D/check1.log 2>&1
  echo "check1 exit $?" >> $D/check1.log; tail -3 $D/check1.log
  ;;
mutate)
  python3 $R/mutate.py --ours "$OURS" --ref "$REF" --plan $D/plan.json --gen-dir $D/out --counts-out $D/counts.json > $D/mutate.log 2>&1
  echo "mutate exit $?" >> $D/mutate.log; tail -2 $D/mutate.log
  ;;
takeover)
  # server A dies while its compactor works on the writes; server B opens the database after A's lease expires
  python3 $R/compactor_status.py "$OURS" > $D/compactor-A-before-kill.json 2>&1
  kill -9 $(cat $D/server-A.pid); sleep 2
  serve B || exit 1
  # settle: B's compactor counters unchanged for 3 minutes (after a first read opens the database)
  python3 - "$OURS" > $D/settle-B.log 2>&1 <<'PY'
import json, sys, time, pymongo
sys.path.insert(0, "/home/user/fenvix-engine/research/generator-1tb")
from compactor_status import compactor
c = pymongo.MongoClient(sys.argv[1], directConnection=True)
for _ in range(60):
    try:
        c["app_data_b01"]["ordini"].find_one({"cliente": "cli-000000"}); break
    except Exception as e:
        print("open:", str(e)[:120]); time.sleep(2)
last, same_since = None, time.time()
while True:
    s = compactor(c)
    if s != last:
        last, same_since = s, time.time(); print(time.strftime("%H:%M:%S"), json.dumps(s)[:600], flush=True)
    if time.time() - same_since > 180:
        break
    time.sleep(10)
print("settled", json.dumps(last))
PY
  tail -2 $D/settle-B.log
  ;;
check2)
  python3 $R/ballast_check.py --ours "$OURS" --ref "$REF" --plan $D/plan.json --gen-dir $D/out --out $D/check2.json --keep-ref --counts $D/counts.json --count-threads 8 --stato-window 0.015 > $D/check2.log 2>&1
  echo "check2 exit $?" >> $D/check2.log; tail -3 $D/check2.log
  ;;
esac
