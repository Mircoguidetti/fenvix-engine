#!/bin/bash
# Determinism of the generator: the same 3 GiB plan, four ways; digest of the stored rows of each
set -u
export PATH=/root/.fenvix-judge/env/bin:$PATH AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
E=/home/user/fenvix-engine; D=/home/user/work/det
OPTS="--envelopes write --customer-key messaggi=cliente:ts,eventi=cliente:ts,ordini=cliente:ts"
echo '[["app_data_b01","--gb","3.0","--seme","2002","--thread","16","--quota-riservati","0.4"]]' > $D/lines.json
python3 $E/step9/machine/ballast_plan.py $E/kitchen/irlanda $D/lines.json $D/plan-base.json --spill-dir $D/spill --out $D/out
gen() { # gen NAME WORKERS [env...]
  local n=$1 w=$2; shift 2
  python3 -c "
import json; p=json.load(open('$D/plan-base.json')); p['slice_gb']=0.5; p['workers']=$w; p['out']='$D/out-$n'; p['spill_dir']='$D/spill-$n'
json.dump(p, open('$D/plan-$n.json','w'))"
  env "$@" $E/target/release/mongo-server --store s3 --bucket det-$n --region us-east-1 --endpoint http://127.0.0.1:9000 $OPTS --ballast $D/plan-$n.json >> $D/$n.log 2>&1
  echo "exit $?" >> $D/$n.log
}
digest() { $E/target/release/fx_rows_digest http://127.0.0.1:9000 det-$1 app_data_b01 > $D/$1.digest; cat $D/$1.digest; }
drop() { python3 -c "
import boto3; s=boto3.resource('s3',endpoint_url='http://127.0.0.1:9000',aws_access_key_id='minioadmin',aws_secret_access_key='minioadmin',region_name='us-east-1')
b=s.Bucket('det-$1'); b.objects.all().delete(); b.delete()"; rm -rf /home/user/work/minio/.minio.sys/tmp/.trash 2>/dev/null; }
for n in w4 w1 stop resume-kill; do python3 /home/user/work/mkbucket.py det-$n; done
T=$(date +%s); gen w4 4; echo "w4 wall $(( $(date +%s) - T ))" | tee -a $D/times.txt; digest w4; drop w4
T=$(date +%s); gen w1 1; echo "w1 wall $(( $(date +%s) - T ))" | tee -a $D/times.txt; digest w1; drop w1
# stopped by the test control after 2 slices committed in this process, then the same command again
T=$(date +%s); gen stop 4 FENVIX_GEN_STOP_AFTER=2; gen stop 4; echo "stop wall $(( $(date +%s) - T ))" | tee -a $D/times.txt; digest stop; drop stop
# killed with SIGKILL 75 s after the start (mid-slice), then the same command again
T=$(date +%s); ( gen resume-kill 4 ) & sleep 75; pkill -9 -f "bucket det-resume-kill"; wait; echo "killed at 75 s" >> $D/resume-kill.log
gen resume-kill 4; echo "resume-kill wall $(( $(date +%s) - T ))" | tee -a $D/times.txt; digest resume-kill
echo ALLDONE
