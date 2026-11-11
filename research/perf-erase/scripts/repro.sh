#!/usr/bin/env bash
# Local repro of the erase phase: MinIO + our server on the host (no log), bench.py on a small profile.
# usage: repro.sh BINARY OUTDIR PROFILE ERASE_USERS [extra server args...]
set -euo pipefail
BIN=$1; OUT=$2; PROFILE=$3; EU=$4; shift 4
J=~/.fenvix-judge/env; PY=~/.fenvix-agentbench-venv/bin/python
W=/tmp/claude-0/s/work; rm -rf $W; mkdir -p $W/minio $OUT
P_MINIO=9410; P_OURS=27302
MINIO_API_DELETE_CLEANUP_INTERVAL=10s MINIO_COMPRESSION_ENABLE=on MINIO_COMPRESSION_EXTENSIONS="*" MINIO_COMPRESSION_MIME_TYPES="*" MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin nohup $J/bin/minio server $W/minio --address 127.0.0.1:$P_MINIO --console-address 127.0.0.1:$((P_MINIO+1)) > $W/minio.log 2>&1 & echo $! > $W/minio.pid
up() { for _ in $(seq 1 150); do (exec 3<>/dev/tcp/127.0.0.1/$1) 2>/dev/null && return 0; sleep 0.2; done; return 1; }
up $P_MINIO
$PY -c "import boto3; boto3.client('s3', endpoint_url='http://127.0.0.1:$P_MINIO', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin', region_name='us-east-1').create_bucket(Bucket='agentbench')"
( export FX_ERASE_TRACE=1 AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 AWS_ALLOW_HTTP=true
  nohup $BIN --listen 127.0.0.1:$P_OURS --store s3 --endpoint http://127.0.0.1:$P_MINIO --bucket agentbench --region us-east-1 --holder agentbench \
    --customer-key checkpoints=thread_id,checkpoint_writes=thread_id --admin-listen 127.0.0.1:27399 --admin-token local-test-token-0123456789 "$@" > $OUT/server.log 2>&1 & echo $! > $W/ours.pid )
up $P_OURS
export BENCH_S3_ENDPOINT=http://127.0.0.1:$P_MINIO BENCH_S3_BUCKET=agentbench BENCH_S3_KEY=minioadmin BENCH_S3_SECRET=minioadmin BENCH_S3_REGION=us-east-1
$PY /tmp/claude-0/s/sampler.py "mongodb://127.0.0.1:$P_OURS/?directConnection=true" $OUT/samples.jsonl & SAMP=$!
cd /home/user/bench-harness/bench/agent-checkpoints
set +e
$PY bench.py --local --target fenvix --uri "mongodb://127.0.0.1:$P_OURS/?directConnection=true" --profile $PROFILE --erase-users $EU --read-sample 300 --out $OUT > $OUT/bench.out 2>&1
echo "bench exit $?" >> $OUT/bench.out
if [ -n "${POST:-}" ]; then PROOF_ENDPOINT=http://127.0.0.1:$P_MINIO PROOF_BUCKET=agentbench $PY /tmp/claude-0/s/post.py "mongodb://127.0.0.1:$P_OURS/?directConnection=true" http://127.0.0.1:27399 local-test-token-0123456789 $OUT ${GRACE_S:-600} ${LOAD_S:-20} > $OUT/post.out 2>&1; echo "post exit $?" >> $OUT/post.out; fi
kill $SAMP
kill -TERM $(cat $W/ours.pid); while kill -0 $(cat $W/ours.pid) 2>/dev/null; do sleep 0.2; done
kill $(cat $W/minio.pid)
