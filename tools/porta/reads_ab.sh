#!/bin/bash
ROOT=${ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}  # run from a scratch folder holding ca.pem, srv-ec.pem (and the fake S3 on 127.0.0.1:9000, bucket p3)
G=$ROOT/target/release/porta
for i in 1 2; do for b in probe box; do
  kill -TERM $(cat srv.pid) 2>/dev/null; sleep 4
  P=$(BIN=$ROOT/../mongo-server-$b ./start_srv.sh rd-$b-$i srv-ec.pem cache); echo $P > srv.pid; sleep 3
  taskset -c 1 $G --mode warm --port 27100 --ca ca.pem --dbs 100 > /dev/null
  echo "== $b $i"; taskset -c 1 $G --mode reads --port 27100 --ca ca.pem --dbs 100 --pool 4 --secs 20 --server-pid $P
done; done
