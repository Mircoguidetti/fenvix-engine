#!/usr/bin/env bash
# prova-zone (N1): copia del quaderno su S3 ogni secondo e ripristino da zero. Soglie in PROVA-ZONE-SOGLIE.md.
#
#   scripts/prova-zone.sh zona  CARTELLA [SECONDO_DELLA_MORTE]   morte della zona (3 repliche + scaricatore, dischi
#                                                                cancellati), ripristino, controllo dei testimoni,
#                                                                poi carico sulla cella nuova, poi oggetto tolto nel
#                                                                mezzo e secondo ripristino (deve fermarsi al buco)
#   scripts/prova-zone.sh s3    CARTELLA [MORTE=0|1]             S3 lento 60 s, poi fermo 120 s (SIGSTOP), poi di nuovo
#                                                                su; ritardo della copia ogni secondo. Con MORTE=1 la
#                                                                zona muore a 115 s del fermo e si ripristina
#   scripts/prova-zone.sh ab    CARTELLA                         commit con e senza copia (p99, CPU)
#
# Carico (2 vCPU: banco 2 al 25% riscalato a metà come in S16): 10.000 db piccoli a 0,125 scritture/s, db caldo a
# 2.500/s, 5.000 letture/s, 300 testimoni/s (chiave unica, per il controllo).
set -u
cd "$(dirname "$0")/.."
CMD=$1
OUT=$2
mkdir -p "$OUT"
W=${PQ_WORK:-/root/pq-work}
PQ=./target/release/pq
S="python3 scripts/cluster.py"
export NO_PROXY='*' no_proxy='*'
LOAD="--small-rate 0.125 --hot-rate 2500 --reads 5000 --ryw 0 --witness-rate 300"

start_all() {
  $S up --fresh "$@" > /dev/null
  rm -rf "$W/minio"
  $S minio-up
  $S flusher-up --s3 http://127.0.0.1:9100 --age-ms 10000 --interval-ms 1000
  sleep 3
}
probe() { python3 scripts/sonda-copia.py "$1" > /dev/null 2>&1 & echo $!; }
last_lag_before() { # file della sonda, istante: ultimo ritardo letto prima dell'istante
  python3 - "$1" "$2" <<'EOF'
import json, sys
best = None
for l in open(sys.argv[1]):
    d = json.loads(l)
    if d.get('capo') and d['t_ms'] <= int(sys.argv[2]):
        best = d
print(best['capo']['copia_ritardo_ms'] if best else 0)
EOF
}

case "$CMD" in
zona)
  KAT=${3:-$((60 + RANDOM % 91))}
  start_all --copy
  SP=$(probe "$OUT/sonda.jsonl")
  $PQ mixed --secs $((KAT + 5)) $LOAD --witness-out "$OUT/testimoni.csv" --report-every 10 > "$OUT/carico.jsonl" &
  LP=$!
  sleep "$KAT"
  K=$($S zone-kill)
  echo "{\"morte_ms\": $K, \"secondo_della_morte\": $KAT}" | tee "$OUT/eventi.jsonl"
  wait $LP
  LAG=$(last_lag_before "$OUT/sonda.jsonl" "$K")
  echo "{\"ritardo_copia_ms_ultima_lettura_prima_della_morte\": $LAG}" | tee -a "$OUT/eventi.jsonl"
  du -sb "$W" > "$OUT/cartelle-dopo-la-morte.txt"; ls "$W" >> "$OUT/cartelle-dopo-la-morte.txt"
  $S restore | tee "$OUT/ripristino.jsonl"
  $S flusher-up --s3 http://127.0.0.1:9100 --age-ms 10000 --interval-ms 1000
  $S status > "$OUT/status-dopo-ripristino.jsonl"
  $PQ check-zona --s3 http://127.0.0.1:9100 --witness "$OUT/testimoni.csv" --kill-ms "$K" --window-ms 1500 | tee "$OUT/controllo.json"
  # la cella nuova lavora: 40 s di carico, con testimoni nuovi
  $PQ mixed --secs 40 $LOAD --witness-start 100000000 --witness-out "$OUT/testimoni-2.csv" --report-every 10 > "$OUT/carico-dopo.jsonl"
  $S status > "$OUT/status-dopo-carico.jsonl"
  # oggetto tolto nel mezzo: seconda morte, buco, secondo ripristino
  K2=$($S zone-kill)
  python3 scripts/s3-oggetti.py elenca > "$OUT/oggetti-prima-del-buco.jsonl"
  python3 scripts/s3-oggetti.py togli-nel-mezzo | tee "$OUT/buco.json"
  $S restore | tee "$OUT/ripristino-buco.jsonl"
  $S flusher-up --s3 http://127.0.0.1:9100 --age-ms 10000 --interval-ms 1000
  $S status > "$OUT/status-dopo-ripristino-buco.jsonl"
  python3 scripts/s3-oggetti.py elenca > "$OUT/oggetti-dopo-il-buco.jsonl"
  GP=$(python3 -c "import json;print(json.load(open('$OUT/buco.json'))['pos_inizio'])")
  $PQ check-zona --s3 http://127.0.0.1:9100 --witness "$OUT/testimoni-2.csv" --kill-ms "$K2" --window-ms 1500 --buco-pos "$GP" | tee "$OUT/controllo-buco.json"
  kill $SP
  grep -h "ALLARME COPIA\|copia:" "$W"/r?.log | head -20 > "$OUT/copia-log.txt"
  $S flusher-down; $S down
  ;;
s3)
  DIE=${3:-0}
  start_all --copy
  SP=$(probe "$OUT/sonda.jsonl")
  TOT=$((30 + 60 + 120 + 90))
  # con la morte il carico finisce 5 s dopo (il file dei testimoni si scrive alla fine del carico)
  [ "$DIE" = "1" ] && TOT=$((30 + 60 + 115 + 5))
  $PQ mixed --secs "$TOT" $LOAD --witness-out "$OUT/testimoni.csv" --report-every 10 > "$OUT/carico.jsonl" &
  LP=$!
  sleep 30
  echo "{\"s3_lento_ms\": $(date +%s%3N), \"ritardo_proxy_ms\": 500}" | tee "$OUT/eventi.jsonl"; $S s3 delay 500
  sleep 60
  $S s3 pass
  echo "{\"s3_fermo_ms\": $(date +%s%3N)}" | tee -a "$OUT/eventi.jsonl"; $S minio-stop
  if [ "$DIE" = "1" ]; then
    sleep 115
    K=$($S zone-kill)
    echo "{\"morte_ms\": $K}" | tee -a "$OUT/eventi.jsonl"
    sleep 5
    echo "{\"s3_ripreso_ms\": $(date +%s%3N)}" | tee -a "$OUT/eventi.jsonl"; $S minio-cont
    wait $LP
    LAG=$(last_lag_before "$OUT/sonda.jsonl" "$K")
    echo "{\"ritardo_copia_ms_ultima_lettura_prima_della_morte\": $LAG}" | tee -a "$OUT/eventi.jsonl"
    $S restore | tee "$OUT/ripristino.jsonl"
    $S flusher-up --s3 http://127.0.0.1:9100 --age-ms 10000 --interval-ms 1000
    # la finestra dichiarata è quella che la metrica mostrava, più 1,5 s
    $PQ check-zona --s3 http://127.0.0.1:9100 --witness "$OUT/testimoni.csv" --kill-ms "$K" --window-ms $((LAG + 1500)) | tee "$OUT/controllo.json"
  else
    sleep 120
    echo "{\"s3_ripreso_ms\": $(date +%s%3N)}" | tee -a "$OUT/eventi.jsonl"; $S minio-cont
    wait $LP
    sleep 10
  fi
  kill $SP
  grep -h "ALLARME COPIA" "$W"/r?.log | head -40 > "$OUT/allarmi.txt"
  grep -h "copia:" "$W"/r?.log | head -40 > "$OUT/copia-log.txt"
  $S flusher-down; $S down
  ;;
lento)
  # S3 lento senza fermarsi: ritardo per pezzo sulla rotta s3 del proxy (pezzi fino a 256 KB), 60 s per livello
  start_all --copy
  SP=$(probe "$OUT/sonda.jsonl")
  $PQ mixed --secs 220 $LOAD --witness-out "$OUT/testimoni.csv" --report-every 10 > "$OUT/carico.jsonl" &
  LP=$!
  sleep 20
  for D in 20 100 200; do
    echo "{\"s3_ritardo_ms\": $D, \"da_ms\": $(date +%s%3N)}" | tee -a "$OUT/eventi.jsonl"; $S s3 delay $D
    sleep 60
  done
  $S s3 pass; echo "{\"s3_normale_ms\": $(date +%s%3N)}" | tee -a "$OUT/eventi.jsonl"
  wait $LP
  kill $SP
  $S flusher-down; $S down
  ;;
ab)
  for rep in 1 2; do
    for mode in senza con; do
      if [ $mode = con ]; then start_all --copy; else start_all; fi
      SP=$(probe "$OUT/sonda-$mode-$rep.jsonl")
      L=$($S leader)
      PID=$(python3 -c "import json;print(json.load(open('$W/pids.json'))['r$L'])")
      C0=$(awk '{print $14+$15}' /proc/$PID/stat)
      $PQ mixed --secs 60 --small-rate 0.125 --hot-rate 2500 --reads 0 --ryw 0 --report-every 60 > "$OUT/carico-$mode-$rep.jsonl"
      C1=$(awk '{print $14+$15}' /proc/$PID/stat)
      kill $SP
      echo "{\"modo\": \"$mode\", \"giro\": $rep, \"cpu_capo_s\": $(( (C1 - C0) ))e-2}" | tee -a "$OUT/cpu.jsonl"
      $S status > "$OUT/status-$mode-$rep.jsonl"
      $S flusher-down; $S down
    done
  done
  ;;
esac
