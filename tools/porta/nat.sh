#!/bin/bash
ROOT=${ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}
# usage: nat.sh <tag> <bin> [server flags...]: 1000 clients (one per database) x pool 5 from ONE address (127.0.0.1, not trusted)
tag=$1; bin=$2; shift 2
G=$ROOT/target/release/porta
kill -TERM $(cat srv.pid) 2>/dev/null; sleep 4
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 AWS_ENDPOINT=http://127.0.0.1:9000 AWS_ALLOW_HTTP=true MONGO_ADMIN_TOKEN=test-token-0123456789abcdef
nohup taskset -c 0 $bin --listen 127.0.0.1:27100 --admin-listen 127.0.0.1:27101 --idle-secs 3600 --bucket p3 --endpoint http://127.0.0.1:9000 --holder p3-$tag --no-ttl --auth --tls-mode requireTLS --tls-certificate-key-file srv-ec.pem --trusted-networks "" "$@" > srv-$tag.log 2>&1 &
P=$!; echo $P > srv.pid; sleep 3
echo "== $tag $*"
timeout 120 taskset -c 1 $G --mode hold --port 27100 --ca ca.pem --dbs 1000 --pool 5 --secs 5 --server-pid $P 2>&1 | grep -E "READY|END|rror" | head -3
echo "server fds=$(ls /proc/$P/fd | wc -l)"
curl -s -H "Authorization: Bearer $MONGO_ADMIN_TOKEN" http://127.0.0.1:27101/v1/network | python3 -c "import json,sys; d=json.load(sys.stdin); print('totals', d['totals']); print('conns', d['connections'][:2]); print('dbs', d.get('databases', [])[:2])"
grep -m2 netguard srv-$tag.log
