#!/usr/bin/env bash
# Banco 2: carico misto. (a) solo 10.000 database piccoli a 1 scrittura/s, per la RAM per database piccolo;
# (b) il carico al 25%, 50% e 100% di: 10.000 db piccoli a 1 scrittura/s + un db caldo a 20.000 scritture/s +
# 100.000 letture/s (metà su chiavi della finestra) + 1.000 "leggi la tua scrittura"/s; scaricatore su MinIO attivo.
# Le latenze si misurano dal tick pianificato e dall'invio vero. Per ogni giro: CPU per processo, cambi di contesto
# forzati del thread principale delle repliche, cronometro del thread principale (status finale, campo diag).
# Uso: scripts/banco2.sh [cartella] [secondi per giro] [a b]
set -eu
cd "$(dirname "$0")/.."
OUT=${1:-results/banco2}
SECS=${2:-120}
shift 2 || true
PARTS=${*:-a b}
mkdir -p "$OUT"
W=${PQ_WORK:-/root/pq-work}
PQ=./target/release/pq
S="python3 scripts/cluster.py"
cargo build --release -q
start_all() {
  $S up --fresh > /dev/null
  rm -rf "$W/minio"
  $S minio-up
  $S flusher-up --age-ms 10000 --interval-ms 1000
  sleep 3
}
pid() { python3 -c "import json;print(json.load(open('$W/pids.json'))['$1'])"; }
run_mixed() { # nome secondi argomenti...
  local name=$1 secs=$2; shift 2
  $PQ mixed --secs "$secs" --report-every 10 "$@" > "$OUT/$name.jsonl" &
  local LP=$!
  sleep 20
  python3 scripts/ctx.py r0=$(pid r0) r1=$(pid r1) r2=$(pid r2) > "$OUT/$name-ctx-inizio.json"
  python3 scripts/cpu.py 30 r0=$(pid r0) r1=$(pid r1) r2=$(pid r2) proxy=$(pid proxy) minio=$(pid minio) scaricatore=$(pid flusher) generatore=$LP > "$OUT/$name-cpu.json"
  python3 scripts/ctx.py r0=$(pid r0) r1=$(pid r1) r2=$(pid r2) > "$OUT/$name-ctx-fine.json"
  wait $LP
  $S status > "$OUT/$name-status-fine.jsonl"
}
for p in $PARTS; do
  case $p in
    a) start_all
       $S status > "$OUT/2a-status-vuoto.jsonl"
       $PQ mixed --secs 240 --small-dbs 10000 --small-rate 1 --hot-rate 0 --reads 0 --ryw 0 --report-every 30 > "$OUT/2a-solo-piccoli.jsonl"
       $S status > "$OUT/2a-status-fine.jsonl" ;;
    b) for f in 0.25 0.5 1; do
         start_all
         run_mixed "2b-misto-x$f" "$SECS" --small-rate "$f" --hot-rate "$(python3 -c "print(20000*$f)")" --reads "$(python3 -c "print(100000*$f)")" --ryw "$(python3 -c "print(1000*$f)")"
       done ;;
  esac
done
$S flusher-down
$S down
