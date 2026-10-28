#!/usr/bin/env bash
# Gap tests g01..g12 of compat-gaps (pymongo and Node), the authentication servers and the TLS pairs, on the final binary.
# Resumable: an item whose log ends with the harness's summary line is done. Output: out/final/gaps_py_<item>.log, gaps_node_<item>.log.
# The engines of the harness use --store local (the compare-and-swap wrapper of the file store is exercised all through).
cd "$(dirname "$0")/../.."
OUT=mongo-tests/out/final; mkdir -p $OUT
PYV=$PWD/mongo-tests/.venv/bin/python
G=mongo-tests/gaps
tools/final/env_up.sh > /dev/null || exit 1
up() { (exec 3<>/dev/tcp/127.0.0.1/$1) 2>/dev/null; }
setup() {
  # engines on 27018 (replica set rs0) and 27019 (standalone), both on the local store
  up 27018 && up 27019 || (cd mongo-tests && gaps/restart_engines.sh fresh > /dev/null 2>&1)
  up 27028 && up 27030 || $G/auth_env.sh start > /dev/null 2>&1
  up 27048 && up 27044 && up 27052 || $PYV_SYS $G/tls_env.py start > /dev/null 2>&1
}
PYV_SYS=python3          # system python has `cryptography`, which tls_env.py needs to make certificates
# the suites phase leaves its own engines (S3 store) on 27018/27019: stop them, the gap tests need local-store engines on a fresh directory
for port in 27018 27019; do
  pf=mongo-tests/out/server-$port.pid
  [ -f $pf ] && kill -TERM "$(cat $pf)" 2>/dev/null
done
for _ in $(seq 1 100); do up 27018 || up 27019 || break; sleep 0.3; done
setup
for item in g01 g02 g03 g04 g05 g06 g07 g08 g09 g10 g11 g12; do
  o=$OUT/gaps_py_$item.log
  if ! { [ -f $o ] && grep -q "^gaps_py: " $o; }; then
    setup
    $PYV $G/gaps_py.py --item $item --out $OUT/gaps_py_$item.json > $o.tmp 2>&1; mv $o.tmp $o
    echo "gaps py $item: $(grep '^gaps_py: ' $o | tail -1)"
  fi
  o=$OUT/gaps_node_$item.log
  if ! { [ -f $o ] && grep -q "^gaps_node: " $o; }; then
    setup
    (cd $G/node && node gaps_node.js --item=$item > ../../../$o.tmp 2>&1); mv $o.tmp $o
    echo "gaps node $item: $(tail -1 $o | cut -c1-120)"
  fi
done
