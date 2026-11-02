#!/usr/bin/env bash
# Banco 2 con la regola di scarico vera: un database si scarica dopo 10 minuti di età o 1 MB non su S3
# (docs/protocollo.md §7.1, DECISIONS 2026-11-01), più lo scarico per pressione sopra il 70% del tetto.
# (a) solo database piccoli (N a 1 scrittura/s) per 11,5 minuti, tetto alto: RAM per database piccolo;
# (b) il 25% del banco 2 (10.000 db piccoli a 0,25/s, caldo a 5.000/s, 25.000 letture/s, 250 ryw/s) per 12 minuti,
#     tetto CAPB MB per replica: RAM, scarico per pressione, latenze (solo come dato).
# Uso: scripts/banco2-10min.sh [cartella] [N=2000] [CAPB=2048]
set -u
cd "$(dirname "$0")/.."
OUT=${1:-results/banco2-scarico-10min}
N=${2:-2000}
CAPB=${3:-2048}
mkdir -p "$OUT"
W=${PQ_WORK:-/root/pq-work}
PQ=./target/release/pq
S="python3 scripts/cluster.py"
cargo build --release -q 2>/dev/null
start_all() {
  $S up --fresh --mem-cap-mb "$1" > /dev/null
  rm -rf "$W/minio"
  $S minio-up
  $S flusher-up --age-ms 600000 --size-kb 1024 --interval-ms 1000
  sleep 3
}
start_all 4096
$S status > "$OUT/2a-status-vuoto.jsonl"
$PQ mixed --secs 690 --small-dbs "$N" --small-rate 1 --hot-rate 0 --reads 0 --ryw 0 --report-every 30 > "$OUT/2a-solo-piccoli.jsonl"
$S status > "$OUT/2a-status-fine.jsonl"
grep -h "pressione" "$W/flusher.log" | head -3 > "$OUT/2a-pressione.txt"
start_all "$CAPB"
$PQ mixed --secs 720 --report-every 30 --small-rate 0.25 --hot-rate 5000 --reads 25000 --ryw 250 > "$OUT/2b-misto-x0.25.jsonl"
$S status > "$OUT/2b-status-fine.jsonl"
grep -h "pressione" "$W/flusher.log" | tail -5 > "$OUT/2b-pressione.txt"
grep -c "pressione" "$W/flusher.log" >> "$OUT/2b-pressione.txt"
$S flusher-down
$S down
