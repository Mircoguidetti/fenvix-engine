#!/bin/bash
# reviewer: generate the 2 GiB plan, SIGKILL at each of the given seconds (each kill is followed by the same command again),
# then a final run to the end; digest, compare with r2, delete the bucket.  killrun.sh NAME S1 [S2 ...]
set -u
NAME=$1; shift
W=/tmp/claude-0/-home-user/65e79c2b-43d3-5019-ac3b-79a2aee6d15e/scratchpad/gen; cd $W
for S in "$@"; do
  ./gen.sh $NAME 2.0 0.5 ${WK:-4} & sleep $S; pkill -9 -f "bucket gen-$NAME "; wait; echo "SIGKILL at $S s" >> $NAME.log
done
./gen.sh $NAME 2.0 0.5 ${WK:-4}
D=$(/home/user/bins/gen-148bee9/fx_rows_digest http://127.0.0.1:9000 gen-$NAME app_data_b01)
echo "$NAME $D" >> digests.txt
python3 - "$D" <<'PY'
import json,sys
d=json.loads(sys.argv[1]); r=json.load(open("r2.digest"))
k=("rows","rows_digest_without_collection_uuids","groups_digest","groups")
print("SAME" if all(d[x]==r[x] for x in k) else "DIFFERENT", {x:(d[x],r[x]) for x in k if d[x]!=r[x]})
PY
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin $HOME/.fenvix-judge/env/bin/python -c "
import boto3; s=boto3.resource('s3',endpoint_url='http://127.0.0.1:9000',region_name='us-east-1'); b=s.Bucket('gen-$NAME')
left=[o.key for o in b.objects.all() if '/generator/' in o.key or '/refs/' in o.key]
ups=list(b.multipart_uploads.all())
print('leftover generator/ref objects', len(left), left[:3], 'unfinished uploads', len(ups))
b.objects.all().delete(); [u.abort() for u in ups]; b.delete()"
rm -rf $W/out-$NAME $W/spill-$NAME
