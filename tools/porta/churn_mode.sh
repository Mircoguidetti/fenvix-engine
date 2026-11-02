#!/bin/bash
ROOT=${ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}  # run from a scratch folder holding ca.pem, srv-ec.pem (and the fake S3 on 127.0.0.1:9000, bucket p3)
# usage: churn_mode.sh <tag> <cert> <resumption> <rates...>
tag=$1; cert=$2; res=$3; shift 3
G=$ROOT/target/release/porta
[ -f srv.pid ] && kill -TERM $(cat srv.pid) 2>/dev/null; sleep 4
PID=$(./start_srv.sh $tag $cert $res "${EXTRA[@]}"); echo $PID > srv.pid
sleep 3
taskset -c 1 $G --mode warm --port 27100 --ca ca.pem --dbs 1000 | tail -1
sleep 2
echo "== $tag cert=$cert resumption=$res pid=$PID"
for r in "$@"; do
  echo "-- rate $r"
  taskset -c 1 $G --mode churn --port 27100 --ca ca.pem --dbs 1000 --rate $r --secs 30 --server-pid $PID 2>&1 | grep -E "CHURN|CPU|LAST_ERROR"
done
