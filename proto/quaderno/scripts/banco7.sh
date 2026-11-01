#!/usr/bin/env bash
# Banco 7: durata. Il carico del banco 2 (completo, con scaricatore su MinIO) per SECS secondi (default 2 ore);
# RAM delle repliche campionata ogni 60 s nell'uscita.
set -eu
cd "$(dirname "$0")/.."
OUT=${1:-results/banco7}
SECS=${2:-7200}
mkdir -p "$OUT"
PQ=./target/release/pq
S="python3 scripts/cluster.py"
cargo build --release -q
$S up --fresh > /dev/null
rm -rf "${PQ_WORK:-/root/pq-work}/minio"
$S minio-up
$S flusher-up --age-ms 10000 --interval-ms 1000
sleep 3
$PQ mixed --secs "$SECS" --report-every 60 > "$OUT/7-durata.jsonl"
$S status > "$OUT/7-status-fine.jsonl"
tail -5 "${PQ_WORK:-/root/pq-work}/flusher.log" > "$OUT/7-scaricatore.txt"
du -sh "${PQ_WORK:-/root/pq-work}"/r? "${PQ_WORK:-/root/pq-work}/minio" > "$OUT/7-disco.txt"
$S flusher-down
$S down
