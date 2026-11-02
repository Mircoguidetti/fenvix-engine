#!/bin/bash
ROOT=${ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}  # run from a scratch folder holding ca.pem, srv-ec.pem (and the fake S3 on 127.0.0.1:9000, bucket p3)
# usage: hold.sh <tag> <tls: ec|plain>
tag=$1; mode=$2
G=$ROOT/target/release/porta
kill -TERM $(cat srv.pid) 2>/dev/null; sleep 4
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 AWS_ENDPOINT=http://127.0.0.1:9000 AWS_ALLOW_HTTP=true MONGO_ADMIN_TOKEN=test-token-0123456789abcdef
if [ $mode = ec ]; then TLS="--tls-mode requireTLS --tls-certificate-key-file srv-ec.pem"; CA="--ca ca.pem"; else TLS=""; CA=""; fi
nohup taskset -c 0 ${BIN:-$ROOT/target/release/mongo-server} --listen 127.0.0.1:27100 --admin-listen 127.0.0.1:27101 --idle-secs 3600 --bucket p3 --endpoint http://127.0.0.1:9000 --holder p3-$tag --no-ttl --auth $TLS > srv-$tag.log 2>&1 &
P=$!; echo $P > srv.pid; sleep 3
taskset -c 1 $G --mode warm --port 27100 $CA --dbs 1000 | tail -1
sleep 15
echo "base fds=$(ls /proc/$P/fd | wc -l) $(grep VmRSS /proc/$P/status)"
(taskset -c 1 $G --mode hold --port 27100 $CA --first 0 --dbs 500 --pool 18 --secs 60 --server-pid $P > hold-$tag-A.log 2>&1 &)
(taskset -c 1 $G --mode hold --port 27100 $CA --first 500 --dbs 500 --pool 17 --secs 50 --server-pid $P > hold-$tag-B.log 2>&1 &)
sleep 45
echo "held fds=$(ls /proc/$P/fd | wc -l) $(grep VmRSS /proc/$P/status)"
cat hold-$tag-A.log hold-$tag-B.log
sleep 20
echo "after fds=$(ls /proc/$P/fd | wc -l) $(grep VmRSS /proc/$P/status)"
