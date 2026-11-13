#!/bin/bash
# reviewer: generate one plan into a fresh bucket.  gen.sh NAME GB SLICE_GB WORKERS [env...]
set -u
NAME=$1; GB=$2; SL=$3; WK=$4; shift 4
export PATH=$HOME/.fenvix-judge/env/bin:$PATH AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
E=/home/user/eng-gen; BIN=/home/user/bins/gen-148bee9; W=/tmp/claude-0/-home-user/65e79c2b-43d3-5019-ac3b-79a2aee6d15e/scratchpad/gen
OPTS="--envelopes write --customer-key messaggi=cliente:ts,eventi=cliente:ts,ordini=cliente:ts"
[ -f $W/plan-$GB.json ] || {
  echo "[[\"app_data_b01\",\"--gb\",\"$GB\",\"--seme\",\"2003\",\"--thread\",\"16\",\"--quota-riservati\",\"0.4\"]]" > $W/lines-$GB.json
  python3 $E/step9/machine/ballast_plan.py $E/kitchen/irlanda $W/lines-$GB.json $W/plan-$GB.json --spill-dir $W/spill --out $W/out-$GB --sample-every 1 --sample-max 100000 > $W/plan-$GB.log 2>&1 || { cat $W/plan-$GB.log; exit 1; }
}
python3 -c "
import json; p=json.load(open('$W/plan-$GB.json')); p['slice_gb']=$SL; p['workers']=$WK; p['out']='$W/out-$NAME'; p['spill_dir']='$W/spill-$NAME'
json.dump(p, open('$W/plan-$NAME.json','w'))"
python3 -c "
import boto3
c=boto3.client('s3',endpoint_url='http://127.0.0.1:9000',region_name='us-east-1')
try: c.create_bucket(Bucket='gen-$NAME')
except c.exceptions.BucketAlreadyOwnedByYou: pass"
env "$@" $BIN/mongo-server --store s3 --bucket gen-$NAME --region us-east-1 --endpoint http://127.0.0.1:9000 $OPTS --ballast $W/plan-$NAME.json >> $W/$NAME.log 2>&1
echo "exit $?" >> $W/$NAME.log
