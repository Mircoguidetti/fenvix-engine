#!/bin/bash
# reviewer: SIGKILL inside the publish (DELAY s after the last slice's line), then the same command; digest vs r2.
set -u
NAME=$1; DELAY=$2
W=/tmp/claude-0/-home-user/65e79c2b-43d3-5019-ac3b-79a2aee6d15e/scratchpad/gen; cd $W
./gen.sh $NAME 2.0 0.5 4 &
until grep -q "3 of 3 slices" $NAME.log 2>/dev/null; do sleep 0.1; done
sleep $DELAY; pkill -9 -f "bucket gen-$NAME "; wait; echo "SIGKILL $DELAY s after the last slice" >> $NAME.log
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin $HOME/.fenvix-judge/env/bin/python -c "
import boto3; s=boto3.resource('s3',endpoint_url='http://127.0.0.1:9000',region_name='us-east-1'); b=s.Bucket('gen-$NAME')
v=sorted(o.key for o in b.objects.filter(Prefix='tenants/app_data_b01/fx/manifest/'))
print('at the kill: versions', v[-3:], 'done.json', any(o.key.endswith('done.json') for o in b.objects.filter(Prefix='generator/')), 'refs', any('/refs/' in o.key for o in b.objects.filter(Prefix='tenants/app_data_b01/fx/refs')))"
./gen.sh $NAME 2.0 0.5 4
grep -E "already|RESUMING|committed before|BALLAST: DONE|FAILED" $NAME.log | cut -c1-200
D=$(/home/user/bins/gen-148bee9/fx_rows_digest http://127.0.0.1:9000 gen-$NAME app_data_b01)
python3 - "$D" <<'PY'
import json,sys
d=json.loads(sys.argv[1]); r=json.load(open("r2.digest"))
k=("rows","rows_digest_without_collection_uuids","groups_digest","groups")
print("SAME" if all(d[x]==r[x] for x in k) else "DIFFERENT", {x:(d[x],r[x]) for x in k if d[x]!=r[x]}, "version", d["version"])
PY
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin $HOME/.fenvix-judge/env/bin/python -c "
import boto3; s=boto3.resource('s3',endpoint_url='http://127.0.0.1:9000',region_name='us-east-1'); b=s.Bucket('gen-$NAME')
left=[o.key for o in b.objects.all() if o.key.startswith('generator/app_data_b01/spill') or '/refs/' in o.key]
print('leftover partition/ref objects', len(left), left[:3])
b.objects.all().delete(); [u.abort() for u in b.multipart_uploads.all()]; b.delete()"
rm -rf $W/out-$NAME $W/spill-$NAME
