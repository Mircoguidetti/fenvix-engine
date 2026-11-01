#!/usr/bin/env bash
# Banco 6: scarico fermo. Scritture del banco 2 al 50% (10.000 db a 0,5 scritture/s + db caldo a 10.000/s, 15.000/s in
# tutto, senza letture) più 500 scritture testimoni/s (chiave unica) con scaricatore su MinIO (età 10 s). A t=120 s
# MinIO viene sospeso (SIGSTOP) per BLOCK secondi, poi riprende; il carico continua per altri 20 minuti.
# Tetto del quaderno CAP MB per replica (qui le 3 repliche stanno sulla stessa macchina); freno sul disco a
# SOFT/HARD/RO MB per replica. A fine prova: tutte le repliche vive, tutto su S3, ogni testimone confermato negli SST.
# Uso: scripts/banco6.sh [cartella] [BLOCK=600] [CAP=1536] [SOFT=1500] [HARD=2000] [RO=2500]
# Variabili (giro 4, per macchine più piccole): HOT=10000 SMALL=0.5 (scritture/s per db piccolo) WIT=500 PRE=120 POST=1200
set -u
cd "$(dirname "$0")/.."
OUT=${1:-results/banco6}
BLOCK=${2:-600}
CAP=${3:-1536}
SOFT=${4:-1500}
HARD=${5:-2000}
RO=${6:-2500}
mkdir -p "$OUT"
W=${PQ_WORK:-/root/pq-work}
PQ=./target/release/pq
S="python3 scripts/cluster.py"
cargo build --release -q 2>/dev/null
$S up --fresh --mem-cap-mb "$CAP" --disk-soft-mb "$SOFT" --disk-hard-mb "$HARD" --disk-ro-mb "$RO" --alarm-s 60 > /dev/null
rm -rf "$W/minio"
$S minio-up
$S flusher-up --age-ms 10000 --interval-ms 1000
sleep 3
PIDS=$(python3 -c "import json;p=json.load(open('$W/pids.json'));print(p['r0'],p['r1'],p['r2'])")
HOT=${HOT:-10000}; SMALL=${SMALL:-0.5}; WIT=${WIT:-500}; PRE=${PRE:-120}; POST=${POST:-1200}
TOT=$((PRE + BLOCK + POST))
$PQ mixed --secs "$TOT" --small-rate "$SMALL" --hot-rate "$HOT" --reads 0 --ryw 0 --witness-rate "$WIT" --witness-out "$W/witness.csv" --report-every 10 > "$OUT/6-carico.jsonl" &
LP=$!
sleep "$PRE"
echo "minio sospeso a $(date +%s%3N)" | tee "$OUT/6-eventi.txt"; $S minio-stop
sleep "$BLOCK"
echo "minio ripreso a $(date +%s%3N)" | tee -a "$OUT/6-eventi.txt"; $S minio-cont
wait $LP
echo "carico finito a $(date +%s%3N)" >> "$OUT/6-eventi.txt"
# tutto su S3?
for i in $(seq 1 60); do
  U=$($PQ status | python3 -c "import sys,json;print(sum(json.loads(l).get('unflushed_recs',0) for l in sys.stdin))")
  [ "$U" = "0" ] && break
  sleep 5
done
echo "non scaricati a fine attesa: $U ($(date +%s%3N))" >> "$OUT/6-eventi.txt"
$S status > "$OUT/6-status-fine.jsonl"
ALIVE=0; for p in $PIDS; do kill -0 "$p" 2>/dev/null && ALIVE=$((ALIVE+1)); done
echo "repliche vive: $ALIVE su 3" | tee -a "$OUT/6-eventi.txt"
dmesg 2>/dev/null | grep -i "killed process" | tail -3 >> "$OUT/6-eventi.txt"
sleep 15  # ultimo giro dello scaricatore e della compattazione
$PQ check-sst --witness "$W/witness.csv" | tee "$OUT/6-testimoni.json"
grep -h "ALLARME\|freno" "$W"/r?.log | head -40 > "$OUT/6-allarmi-freno.txt"
tail -20 "$W/flusher.log" > "$OUT/6-scaricatore.txt"
$S flusher-down
$S down
