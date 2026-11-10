#!/bin/bash
# integration-3: rerun ctl test files one by one, in the judge's environment (sealed2: JUDGE_SEALED_RUNS=2's exports;
# off: FENVIX_SEALED_RUNS=off), deleting the MinIO buckets each file leaves (the ctl tests do not delete theirs: 571
# buckets, 15 GB, filled this sandbox's disk during the full sealed2 judge). Usage: ctl-rerun.sh MODE OUTDIR FILE...
MODE=$1; OUT=$(mkdir -p "$2" && cd "$2" && pwd); shift 2
JH=/root/.fenvix-judge; export PATH="$JH/env/bin:$PATH"
if [ "$MODE" = sealed2 ]; then
  export FENVIX_SEALED_RUNS=2 FENVIX_SEALED_TPIN_MS=1000 FENVIX_SEALED_E1_WAIT_MS=0 FENVIX_SEALED_TEST_SHORT_E1_WAIT=1 FENVIX_SEALED_COMPACTOR=${JUDGE_SEALED_COMPACTOR:-small}
  [ "${JUDGE_SEALED_DERIVE:-0}" = 1 ] && export FENVIX_SEALED_DERIVE=1 FENVIX_SEALED_CODEC=zstd
else
  unset FENVIX_SEALED_COMPACTOR FENVIX_SEALED_DERIVE FENVIX_SEALED_CODEC; export FENVIX_SEALED_RUNS=off
fi
(exec 3<>/dev/tcp/127.0.0.1/9000) 2>/dev/null || { MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin nohup minio server "$JH/data/minio" --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 > "$JH/data/minio.log" 2>&1 & sleep 3; }
cd "$(dirname "$0")/../../ctl-tests" || exit 1
for n in "$@"; do
  timeout 1500 python -m pytest -q -p no:cacheprovider --tb=short "$n.py" > "$OUT/$n.log" 2>&1
  echo "$n: $(tail -1 "$OUT/$n.log")"
  python - <<'P'
import boto3
s = boto3.resource("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
for b in s.buckets.all():
    if b.name.startswith("ctl-"):
        b.objects.all().delete(); b.delete()
P
done
df -h / | tail -1
