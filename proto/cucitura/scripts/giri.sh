#!/bin/sh
# Molti giri di cucitura (frammentazione): base 8 scatole da 32 MB, poi 30 giri con una scatola nuova da 32 MB.
cd "$(dirname "$0")/.."
B=./target/release/cu-prova
EP=${EP:-http://127.0.0.1:9100}
run() { n=$1; shift; echo "== $n $(date +%T)"; $B --endpoint $EP --out risultati/$n.json giri "$@" > risultati/$n.log 2>&1; echo "   exit $? $(date +%T)"; }
run giri-oid-4k-rimedio --blocco-kb 4 --vivo-min 0.7
run giri-oid-4k-senza-rimedio --blocco-kb 4 --vivo-min 0
