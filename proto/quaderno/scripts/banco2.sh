#!/usr/bin/env bash
# Banco 2: carico misto. (a) solo 10.000 database piccoli a 1 scrittura/s, per la RAM per database piccolo;
# (b) carico completo: 10.000 db piccoli a 1 scrittura/s + un db caldo a 20.000 scritture/s + 100.000 letture/s
# (metà su chiavi della finestra) + 1.000 "leggi la tua scrittura"/s; scaricatore su MinIO attivo.
# Uso: scripts/banco2.sh [cartella] [secondi del carico completo]
set -eu
cd "$(dirname "$0")/.."
OUT=${1:-results/banco2}
SECS=${2:-300}
mkdir -p "$OUT"
PQ=./target/release/pq
S="python3 scripts/cluster.py"
cargo build --release -q
start_all() {
  $S up --fresh > /dev/null
  rm -rf "${PQ_WORK:-/root/pq-work}/minio"
  $S minio-up
  $S flusher-up --age-ms 10000 --interval-ms 1000
  sleep 3
}
# (a) RAM per database piccolo: riferimento a vuoto, poi 4 minuti di carico (finestra piena e scarichi a regime)
start_all
$S status > "$OUT/2a-status-vuoto.jsonl"
$PQ mixed --secs 240 --small-dbs 10000 --small-rate 1 --hot-rate 0 --reads 0 --ryw 0 --report-every 30 | tee "$OUT/2a-solo-piccoli.jsonl"
$S status > "$OUT/2a-status-fine.jsonl"
$S flusher-down
# (b) carico completo
start_all
$PQ mixed --secs "$SECS" --report-every 10 | tee "$OUT/2b-misto.jsonl"
$S status > "$OUT/2b-status-fine.jsonl"
tail -3 "${PQ_WORK:-/root/pq-work}/flusher.log" > "$OUT/2b-scaricatore.txt"
$S flusher-down
$S down
