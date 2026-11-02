#!/usr/bin/env bash
# Banco 5: raggio del guasto del leader. 10.000 database scrivono una volta al secondo ciascuno (rinvio con lo stesso
# id finché confermato). A t=30 s: (kill) SIGKILL del leader, riavviato dopo 20 s; oppure (part) partizione di rete
# del leader dalle altre repliche e dai client per 30 s. Controlli: per ogni database, tempo dal guasto alla prima
# scrittura confermata; registro di ogni replica (ogni commit confermato presente una volta, alla stessa posizione);
# rinvio dei commit ambigui e di un campione di confermati (stessa posizione).
# Uso: scripts/banco5.sh [cartella] [kill|part ...]
set -eu
cd "$(dirname "$0")/.."
OUT=${1:-results/banco5}
shift || true
MODES=${*:-kill part}
mkdir -p "$OUT"
W=${PQ_WORK:-/root/pq-work}
PQ=./target/release/pq
S="python3 scripts/cluster.py"
cargo build --release -q
for m in $MODES; do
  $S up --fresh > /dev/null; sleep 3
  L=$($S leader)
  $PQ failover --replicas 127.0.0.1:7400,127.0.0.1:7401,127.0.0.1:7402 --secs 90 --dbs 10000 --period-ms 1000 --out "$W/fo-$m.csv" > "$OUT/5-$m.json" 2> "$OUT/5-$m.log" &
  LP=$!
  sleep 30
  T=$(date +%s%3N)
  if [ "$m" = kill ]; then
    $S kill "$L"; echo "SIGKILL del leader $L a $T" | tee "$OUT/5-$m-eventi.txt"
    sleep 20; $S start "$L"; echo "riavviato a $(date +%s%3N)" >> "$OUT/5-$m-eventi.txt"
  else
    $S isolate "$L" --clients; echo "partizione del leader $L a $T" | tee "$OUT/5-$m-eventi.txt"
    sleep 30; $S heal; echo "partizione tolta a $(date +%s%3N)" >> "$OUT/5-$m-eventi.txt"
  fi
  sleep 3; echo "nuovo leader $($S leader)" >> "$OUT/5-$m-eventi.txt"
  wait $LP
  python3 scripts/analizza5.py "$W/fo-$m.csv" "$T" | tee "$OUT/5-$m-disponibilita.json"
  $S status > "$OUT/5-$m-status-fine.jsonl"
done
$S down
