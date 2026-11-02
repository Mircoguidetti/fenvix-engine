#!/bin/sh
# Prove della cucitura (M11): 8 scatole da 128 MB, varianti di carico. Finto S3 su 127.0.0.1:9100.
cd "$(dirname "$0")/.."
B=./target/release/cu-prova
EP=${EP:-http://127.0.0.1:9100}
run() { n=$1; shift; echo "== $n $(date +%T)"; $B --endpoint $EP --out risultati/$n.json prova --buco-get-kb 0 --ripiego 1 "$@" > risultati/$n.log 2>&1; echo "   exit $? $(date +%T)"; }
run prova-oid-uniforme --id oid --canc uniforme
run prova-uuid-uniforme --id uuid --canc uniforme
run prova-oid-uniforme-4k --id oid --canc uniforme --blocco-kb 4
run prova-oid-recenti --id oid --canc recenti
run prova-oid-vecchi --id oid --canc vecchi
run prova-oid-uniforme-fin5k --id oid --canc uniforme --recenti 5000
run prova-oid-uniforme-fin1m --id oid --canc uniforme --recenti 1000000
