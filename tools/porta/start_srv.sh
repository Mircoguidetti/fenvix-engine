#!/bin/bash
ROOT=${ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}  # run from a scratch folder holding ca.pem, srv-ec.pem (and the fake S3 on 127.0.0.1:9000, bucket p3)
# usage: start_srv.sh <tag> <cert pem> <resumption> [extra flags...]
tag=$1; cert=$2; res=$3; shift 3
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 AWS_ENDPOINT=http://127.0.0.1:9000 AWS_ALLOW_HTTP=true MONGO_ADMIN_TOKEN=test-token-0123456789abcdef
BIN=${BIN:-$ROOT/target/release/mongo-server}
nohup taskset -c 0 $BIN --listen 127.0.0.1:27100 --admin-listen 127.0.0.1:27101 --idle-secs 3600 --bucket p3 --endpoint http://127.0.0.1:9000 --holder p3-$tag --no-ttl \
  --auth --tls-mode requireTLS --tls-certificate-key-file $cert $( [ "$res" != none ] && echo --tls-session-resumption $res) "$@" > ./srv-$tag.log 2>&1 &
echo $!
