#!/usr/bin/env bash
# Local validation of the agent checkpoint benchmark ($0, no cloud): starts MinIO + our server (mongo-server), a local
# Postgres 16 (initdb in a temp dir, own port) and a local mongod 7.0, replays the same generator plan against the three
# through the real LangGraph checkpointers, then the same turns through Mastra's stores (mastra/, MASTRA=0 skips it),
# collects everything under results/local-<date>/, and stops everything.
# The numbers are VALIDATION, NOT RESULTS: one sandbox, client and databases on the same machine, MinIO instead of S3.
#
#   bench/agent-checkpoints/run_local.sh            # profile "small" (default), seed 42
#   PROFILE=tiny SEED=7 bench/agent-checkpoints/run_local.sh
#
# Needs: target/release/mongo-server (cargo build --release -p mongo-server), judge/setup.sh (mongod, MinIO, python 3.12),
# /usr/lib/postgresql/16/bin. Ports (override by env): MinIO 9310, ours 27202, mongod 27201, Postgres 25433.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); ROOT=$(cd "$HERE/../.." && pwd)
JENV=${JUDGE_HOME:-$HOME/.fenvix-judge}/env
PROFILE=${PROFILE:-small}; SEED=${SEED:-42}
P_MINIO=${P_MINIO:-9310}; P_OURS=${P_OURS:-27202}; P_MONGOD=${P_MONGOD:-27201}; P_PG=${P_PG:-25433}
PGBIN=${PGBIN:-/usr/lib/postgresql/16/bin}
VENV=${VENV:-$HOME/.fenvix-agentbench-venv}
WORK=${WORK:-/tmp/fenvix-agentbench}          # under /tmp so the postgres user can reach it
OUT=${OUT:-$HERE/results/local-$(date -u +%Y-%m-%d)}
BUCKET=agentbench
SERVER="$ROOT/target/release/mongo-server"

[ -x "$SERVER" ] || { echo "missing $SERVER: cargo build --release -p mongo-server"; exit 1; }
[ -x "$JENV/bin/mongod" ] && [ -x "$JENV/bin/minio" ] || { echo "missing mongod/minio: run judge/setup.sh"; exit 1; }

# -- python venv with the real framework packages ------------------------------------------------------------------
if [ ! -x "$VENV/bin/python" ]; then "$JENV/bin/python" -m venv "$VENV"; fi
"$VENV/bin/pip" install -q -r "$HERE/requirements.txt"
PY="$VENV/bin/python"

up() { for _ in $(seq 1 150); do (exec 3<>/dev/tcp/127.0.0.1/$1) 2>/dev/null && return 0; sleep 0.2; done; echo "port $1 not up"; return 1; }
as_pg() { if [ "$(id -u)" = 0 ]; then su postgres -c "$*"; else bash -c "$*"; fi; }

stop_all() {
  set +e
  [ -f "$WORK/ours.pid" ] && kill -TERM "$(cat "$WORK/ours.pid")" 2>/dev/null && while kill -0 "$(cat "$WORK/ours.pid")" 2>/dev/null; do sleep 0.2; done
  rm -f "$WORK/ours.pid"
  [ -d "$WORK/mongod" ] && "$JENV/bin/mongod" --dbpath "$WORK/mongod" --shutdown > /dev/null 2>&1
  [ -d "$WORK/pg/data" ] && as_pg "$PGBIN/pg_ctl -D $WORK/pg/data -m fast stop" > /dev/null 2>&1
  [ -f "$WORK/minio.pid" ] && kill "$(cat "$WORK/minio.pid")" 2>/dev/null; rm -f "$WORK/minio.pid"
  return 0
}
trap stop_all EXIT

rm -rf "$WORK"; mkdir -p "$WORK/minio" "$WORK/mongod" "$WORK/pg" "$OUT"; chmod 755 "$WORK"
[ "$(id -u)" = 0 ] && chown postgres "$WORK/pg"

# -- MinIO + our server ----------------------------------------------------------------------------------------------
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin nohup "$JENV/bin/minio" server "$WORK/minio" \
  --address 127.0.0.1:$P_MINIO --console-address 127.0.0.1:$((P_MINIO + 1)) > "$WORK/minio.log" 2>&1 &
echo $! > "$WORK/minio.pid"; up $P_MINIO
export BENCH_S3_ENDPOINT=http://127.0.0.1:$P_MINIO BENCH_S3_BUCKET=$BUCKET BENCH_S3_KEY=minioadmin BENCH_S3_SECRET=minioadmin BENCH_S3_REGION=us-east-1
"$PY" -c "
import boto3; s=boto3.client('s3', endpoint_url='$BENCH_S3_ENDPOINT', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin', region_name='us-east-1')
s.create_bucket(Bucket='$BUCKET')"
# MinIO credentials only (never real ones), as tools/minio-env.sh does
( export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 AWS_ALLOW_HTTP=true
  unset AWS_SESSION_TOKEN AWS_PROFILE
  nohup "$SERVER" --listen 127.0.0.1:$P_OURS --store s3 --endpoint http://127.0.0.1:$P_MINIO --bucket $BUCKET \
    --region us-east-1 --holder agentbench > "$WORK/ours.log" 2>&1 & echo $! > "$WORK/ours.pid" )
up $P_OURS

# -- mongod 7.0 (standalone, the saver needs no replica set) ---------------------------------------------------------
"$JENV/bin/mongod" --port $P_MONGOD --bind_ip 127.0.0.1 --dbpath "$WORK/mongod" --fork --logpath "$WORK/mongod.log" > /dev/null
up $P_MONGOD

# -- Postgres 16 ------------------------------------------------------------------------------------------------------
as_pg "$PGBIN/initdb -D $WORK/pg/data -A trust -U app > $WORK/pg/initdb.log"
as_pg "$PGBIN/pg_ctl -D $WORK/pg/data -o '-p $P_PG -k $WORK/pg -c listen_addresses=127.0.0.1' -l $WORK/pg/log start" > /dev/null
up $P_PG
as_pg "$PGBIN/createdb -h 127.0.0.1 -p $P_PG -U app app_demo"
as_pg "$PGBIN/createdb -h 127.0.0.1 -p $P_PG -U app app_chat"   # Mastra's database

# -- plan, then the three targets -----------------------------------------------------------------------------------
cd "$HERE"
"$PY" generator.py --profile "$PROFILE" --seed "$SEED" --out "$OUT/plan.jsonl" --describe --export-turns "$WORK/turns.jsonl" > "$OUT/workload.json"
{
  echo "VALIDATION, NOT RESULTS. Local sandbox $(uname -n), $(nproc) vCPU, client and databases on one machine, MinIO for S3."
  echo "engine commit $(git -C "$ROOT" rev-parse --short HEAD); profile $PROFILE; seed $SEED; $(date -u +%FT%TZ)"
  "$VENV/bin/pip" freeze | grep -iE '^(langgraph|langchain-core|pymongo|psycopg)'
  "$JENV/bin/mongod" --version | head -1; "$PGBIN/postgres" --version
} > "$OUT/ENVIRONMENT.txt"

"$PY" bench.py --local --target fenvix   --label "local mongo-server on MinIO" --uri "mongodb://127.0.0.1:$P_OURS/?directConnection=true" --profile "$PROFILE" --seed "$SEED" --out "$OUT/fenvix"
"$PY" bench.py --local --target mongo    --label "local mongod 7.0"            --uri "mongodb://127.0.0.1:$P_MONGOD/?directConnection=true" --profile "$PROFILE" --seed "$SEED" --out "$OUT/mongo"
"$PY" bench.py --local --target postgres --label "local Postgres 16"           --uri "postgresql://app@127.0.0.1:$P_PG/app_demo" --profile "$PROFILE" --seed "$SEED" --out "$OUT/postgres"

# Mastra (JS) on the same three databases, database "app_chat"; skipped with MASTRA=0 or without node
if [ "${MASTRA:-1}" = 1 ] && command -v node > /dev/null; then
  ND=${MASTRA_NODE_DIR:-$HOME/.fenvix-agentbench-node}; mkdir -p "$ND"; cp "$HERE/mastra/package.json" "$ND/"
  (cd "$ND" && npm install --silent --no-audit --no-fund)
  export MASTRA_NODE_DIR=$ND
  node mastra/bench.mjs mongo "mongodb://127.0.0.1:$P_OURS/?directConnection=true" "$WORK/turns.jsonl" "$OUT/mastra-fenvix" --local
  node mastra/bench.mjs mongo "mongodb://127.0.0.1:$P_MONGOD/?directConnection=true" "$WORK/turns.jsonl" "$OUT/mastra-mongo" --local
  node mastra/bench.mjs pg "postgresql://app@127.0.0.1:$P_PG/app_chat" "$WORK/turns.jsonl" "$OUT/mastra-postgres" --local
  "$PY" mastra/sizes.py mongo "mongodb://127.0.0.1:$P_OURS/?directConnection=true" > "$OUT/mastra-fenvix/sizes.json"
  "$PY" mastra/sizes.py mongo "mongodb://127.0.0.1:$P_MONGOD/?directConnection=true" > "$OUT/mastra-mongo/sizes.json"
  "$PY" mastra/sizes.py pg "postgresql://app@127.0.0.1:$P_PG/app_chat" > "$OUT/mastra-postgres/sizes.json"
fi

# our server writes to S3 in the background: list the bucket again after its idle close and after a clean stop
sleep 70
"$PY" -c "import json,targets,os; s={'endpoint':os.environ['BENCH_S3_ENDPOINT'],'bucket':'$BUCKET','key':'minioadmin','secret':'minioadmin'}; print(json.dumps(targets.s3_prefix_bytes(s,'tenants/app_demo/')))" > "$OUT/fenvix/s3_after_idle_close.json"
kill -TERM "$(cat "$WORK/ours.pid")"; while kill -0 "$(cat "$WORK/ours.pid")" 2>/dev/null; do sleep 0.2; done; rm -f "$WORK/ours.pid"
"$PY" -c "import json,targets,os; s={'endpoint':os.environ['BENCH_S3_ENDPOINT'],'bucket':'$BUCKET','key':'minioadmin','secret':'minioadmin'}; print(json.dumps(targets.s3_prefix_bytes(s,'tenants/app_demo/')))" > "$OUT/fenvix/s3_after_stop.json"
[ -d "$OUT/mastra-fenvix" ] && "$PY" -c "import json,targets,os; s={'endpoint':os.environ['BENCH_S3_ENDPOINT'],'bucket':'$BUCKET','key':'minioadmin','secret':'minioadmin'}; print(json.dumps(targets.s3_prefix_bytes(s,'tenants/app_chat/')))" > "$OUT/mastra-fenvix/s3_after_stop.json"
cp "$WORK/ours.log" "$OUT/fenvix/server.log"

"$PY" cost.py --all "$OUT" --out "$OUT/cost.json" > "$OUT/cost.txt"
for f in "$OUT"/*/ops.jsonl; do gzip -f "$f"; done
"$PY" report.py "$OUT" | tee "$OUT/REPORT.txt"
