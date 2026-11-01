#!/usr/bin/env bash
# Banco 4: letture fresche sotto partizione. Carico di scritture e letture lineari (posizione_confermata dal leader,
# poi get(S) su una replica a caso, anche quella isolata). Dopo 30 s una replica follower viene isolata dal proxy
# (viva, ma tagliata fuori dalle altre due; i client la raggiungono ancora) per ISO secondi; poi si ricongiunge e il
# carico continua 60 s. I client passano dal proxy. Controlli: diretto (nessuna lettura più vecchia della posizione
# confermata) e porcupine (linearizzabilità completa, registro per chiave).
# Uso: scripts/banco4.sh [cartella] [ISO secondi=600]
set -eu
cd "$(dirname "$0")/.."
OUT=${1:-results/banco4}
ISO=${2:-600}
mkdir -p "$OUT"
W=${PQ_WORK:-/root/pq-work}
PQ=./target/release/pq
S="python3 scripts/cluster.py"
cargo build --release -q
(cd lincheck && go build -o "$W/pq-lincheck" .)
$S up --fresh > /dev/null; sleep 3
L=$($S leader); F=$(( (L + 1) % 3 ))
echo "leader $L, isolata $F" | tee "$OUT/4-eventi.txt"
TOT=$((30 + ISO + 60))
$PQ lin --replicas 127.0.0.1:7400,127.0.0.1:7401,127.0.0.1:7402 --secs $TOT --keys 200 --writers 8 --readers 16 --pause-us 2000 --out "$W/lin.csv" > "$OUT/4-carico.json" 2> "$OUT/4-carico.log" &
LP=$!
sleep 30
T0=$(date +%s%3N); $S isolate $F; echo "isolata $F a $T0" >> "$OUT/4-eventi.txt"
for i in $(seq 1 $((ISO / 60))); do sleep 60; $S status | cut -c1-200 >> "$OUT/4-status-durante.txt"; done
T1=$(date +%s%3N); $S heal; echo "ricongiunta a $T1" >> "$OUT/4-eventi.txt"
wait $LP
$S status > "$OUT/4-status-fine.jsonl"
START=$(python3 -c "import json;print(json.load(open('$OUT/4-carico.json'))['start_unix_ms'])")
A=$(python3 -c "print(int(($T0 - $START) * 1000))"); B=$(python3 -c "print(int(($T1 - $START) * 1000))")
$PQ check-lin --file "$W/lin.csv" --iso "$F,$A,$B" | tee "$OUT/4-controllo-diretto.json"
"$W/pq-lincheck" "$W/lin.csv" 1800 | tee "$OUT/4-porcupine.json"
gzip -c "$W/lin.csv" > "$W/lin.csv.gz"; ls -la "$W/lin.csv.gz"
$S down
