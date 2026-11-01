#!/usr/bin/env bash
# Banco 6: scarico in ritardo. Scritture del banco 2 al 50% (5.000 db piccoli a 1/s... cioè 10.000 db a 0,5/s, più il
# db caldo a 10.000/s; senza letture) con scaricatore su MinIO; dopo 120 s
# MinIO viene sospeso (SIGSTOP) per BLOCK secondi (default 600): lo scarico è fermo. Poi riprende (SIGCONT) e si
# misura quanto ci mette lo scarico a tornare in pari. Tetto di RAM delle repliche: CAP MB.
set -eu
cd "$(dirname "$0")/.."
OUT=${1:-results/banco6}
BLOCK=${2:-600}
CAP=${3:-1024}
mkdir -p "$OUT"
PQ=./target/release/pq
S="python3 scripts/cluster.py"
cargo build --release -q
$S up --fresh --mem-cap-mb "$CAP" > /dev/null
rm -rf "${PQ_WORK:-/root/pq-work}/minio"
$S minio-up
$S flusher-up --age-ms 10000 --interval-ms 1000
sleep 3
TOT=$((120 + BLOCK + 2 * BLOCK + 60))
$PQ mixed --secs "$TOT" --small-rate 0.5 --hot-rate 10000 --reads 0 --ryw 0 --report-every 10 > "$OUT/6-carico.jsonl" &
LP=$!
sleep 120
echo "minio sospeso a $(date +%s%3N)" | tee "$OUT/6-eventi.txt"; $S minio-stop
sleep "$BLOCK"
echo "minio ripreso a $(date +%s%3N)" | tee -a "$OUT/6-eventi.txt"; $S minio-cont
wait $LP
$S status > "$OUT/6-status-fine.jsonl"
tail -20 "${PQ_WORK:-/root/pq-work}/flusher.log" > "$OUT/6-scaricatore.txt"
$S flusher-down
$S down
