#!/bin/bash
# run.sh NAME MEM CLIENTS TOTAL [extra server flags...]
D=$(cd "$(dirname "$0")" && pwd)
S=${S:-$D/out}; mkdir -p $S
N=$1; MEM=$2; C=$3; T=$4; shift 4
BIN=${BIN:-$D/../../target/release/mongo-server}
R=$S/$N; rm -rf $R; mkdir -p $R/data; cd $R
$BIN --listen 127.0.0.1:27061 --store local --local-dir $R/data --holder fc-local --customer-key messages=userId --memory-limit-mb $MEM --idle-secs 100000 "$@" > $R/server.log 2>&1 &
SP=$!; echo $SP > $R/pid
until python3 -c "import socket;socket.create_connection(('127.0.0.1',27061),0.2)" 2>/dev/null; do sleep 0.5; done
python3 $D/mon.py 27061 fenvix-log $R/mon.jsonl > $R/mon.txt 2>&1 &
MP=$!
timeout 1500 python3 $D/pload.py 27061 $C $T $R/res.json --single > $R/load.txt 2>&1; echo "load exit $?"
kill $MP; kill -TERM $SP; wait $SP
tail -16 $R/load.txt
