#!/bin/bash
# generator-1tb: determinism of the ballast generator. One 3 GiB plan (seed 2002, slices of 0.5 GiB: 5 slices), generated
# one way, then the stored rows digested (src/bin/fx_rows_digest.rs) and compared with the reference digest.
# Usage: determinism.sh MODE [ARG]   (local MinIO on 127.0.0.1:9000, the judge's tools in ~/.fenvix-judge)
#   ref            4 writers, uninterrupted; writes the reference digest (WORK/ref.digest)
#   writers N      N writers, uninterrupted
#   stop N         4 writers, the process stops itself after N slices committed (FENVIX_GEN_STOP_AFTER), then again
#   kill S         4 writers, SIGKILL S seconds after the start (S=random: 35 to 160), then the same command again
# Exit 0 when the digests without the collection UUIDs (random at every createIndexes) and of the groups equal the
# reference's. MEASURED in this session (4 vCPU): ref, writers 1, stop 2 and kill 75 all gave
# rows c5f11743d8b0e8ca123e57ab0c19d645, groups dbf9d6f26b11e2a93fbe471f36e327fc.
set -u
MODE=$1; ARG=${2:-}
export PATH=$HOME/.fenvix-judge/env/bin:$PATH AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
E=$(cd "$(dirname "$0")/../.." && pwd); W=${WORK:-/tmp/fenvix-gen-det}; mkdir -p $W
OPTS="--envelopes write --customer-key messaggi=cliente:ts,eventi=cliente:ts,ordini=cliente:ts"
N=$MODE-${ARG:-x}-$$; B=det-$(echo $N | tr -c 'a-z0-9-\n' '-')
[ -f $W/plan-base.json ] || {
  echo '[["app_data_b01","--gb","3.0","--seme","2002","--thread","16","--quota-riservati","0.4"]]' > $W/lines.json
  python3 $E/step9/machine/ballast_plan.py $E/kitchen/irlanda $W/lines.json $W/plan-base.json --spill-dir $W/spill --out $W/out
}
w=4; [ "$MODE" = writers ] && w=$ARG
python3 -c "
import json; p=json.load(open('$W/plan-base.json')); p['slice_gb']=0.5; p['workers']=$w; p['out']='$W/out-$N'; p['spill_dir']='$W/spill-$N'
json.dump(p, open('$W/plan-$N.json','w'))"
python3 -c "
import boto3; boto3.client('s3',endpoint_url='http://127.0.0.1:9000',aws_access_key_id='minioadmin',aws_secret_access_key='minioadmin',region_name='us-east-1').create_bucket(Bucket='$B')"
gen() { env "$@" $E/target/release/mongo-server --store s3 --bucket $B --region us-east-1 --endpoint http://127.0.0.1:9000 $OPTS --ballast $W/plan-$N.json >> $W/$N.log 2>&1; echo "exit $?" >> $W/$N.log; }
case "$MODE" in
  ref|writers) gen ;;
  stop) gen FENVIX_GEN_STOP_AFTER=$ARG; gen ;;
  kill)
    S=$ARG; [ "$S" = random ] && S=$(( 35 + RANDOM % 126 ))
    gen & sleep $S; pkill -9 -f "bucket $B "; wait; echo "SIGKILL at $S s" >> $W/$N.log
    gen ;;
  *) echo "mode: ref | writers N | stop N | kill S"; exit 2 ;;
esac
D=$($E/target/release/fx_rows_digest http://127.0.0.1:9000 $B app_data_b01)
echo "$N $D" | tee -a $W/digests.txt
python3 -c "
import boto3; s=boto3.resource('s3',endpoint_url='http://127.0.0.1:9000',aws_access_key_id='minioadmin',aws_secret_access_key='minioadmin',region_name='us-east-1')
b=s.Bucket('$B'); b.objects.all().delete(); b.delete()"
rm -rf $W/spill-$N
key() { python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(d['rows_digest_without_collection_uuids'], d['groups_digest'])" "$1"; }
if [ "$MODE" = ref ]; then echo "$D" > $W/ref.digest; exit 0; fi
[ -f $W/ref.digest ] || { echo "no reference: run 'determinism.sh ref' first"; exit 2; }
if [ "$(key "$D")" = "$(key "$(cat $W/ref.digest)")" ]; then echo "SAME as the reference"; exit 0; fi
echo "DIFFERENT from the reference: $(key "$D") against $(key "$(cat $W/ref.digest)")"; exit 1
