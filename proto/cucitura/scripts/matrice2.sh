#!/bin/sh
# Secondo giro: GET raggruppati con buchi fino a 64 KB o 1 MB, ripiego sulla riscrittura se la copertura supera il 50%.
cd "$(dirname "$0")/.."
B=./target/release/cu-prova
EP=${EP:-http://127.0.0.1:9100}
run() { n=$1; shift; echo "== $n $(date +%T)"; $B --endpoint $EP --out risultati/$n.json prova "$@" > risultati/$n.log 2>&1; echo "   exit $? $(date +%T)"; }
run g2-oid-uniforme-4k-b64 --id oid --canc uniforme --blocco-kb 4 --modi coda --buco-get-kb 64
run g2-oid-uniforme-4k-b1m --id oid --canc uniforme --blocco-kb 4 --modi coda --buco-get-kb 1024
run g2-uuid-uniforme-ripiego --id uuid --canc uniforme --modi coda,ordine
run g2-oid-uniforme-16k-b64 --id oid --canc uniforme --modi coda --buco-get-kb 64
run g2-uuid-uniforme-4k-ripiego --id uuid --canc uniforme --blocco-kb 4 --modi coda
