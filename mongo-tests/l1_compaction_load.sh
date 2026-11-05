#!/bin/bash
# L1: the load that stopped compaction ("did not finish (L0 8)"): two loaders of prova_infinite.py at once, 16 threads
# each, against one mongo-server (--store local), on the same machine. Every 15 s it records the disk used by each
# database (total, compacted SSTs, WAL objects) and how many compaction passes failed. Stops after MAX_MIN minutes, when
# the disk has less than 6 GB free, or when the server dies.
#
# usage: l1_compaction_load.sh NAME BINARY MAX_MIN PROVA_INFINITE_PY PYTHON
#   output in ./l1-load/NAME: server.log, monitor.log, a4/carica.log, a6/carica.log
set -u
NAME=$1; BIN=$2; MAXMIN=${3:-45}; SCRIPT=$4; PY=$5
W=$(pwd)/l1-load/$NAME; rm -rf "$W"; mkdir -p "$W/store" "$W/a4" "$W/a6"
cp "$SCRIPT" "$W/a4/"; cp "$SCRIPT" "$W/a6/"
md5sum "$W/a4/prova_infinite.py" > "$W/md5.txt"
PORT=27801
"$BIN" --listen 127.0.0.1:$PORT --store local --local-dir "$W/store" --replica-set rs0 > "$W/server.log" 2>&1 &
SPID=$!
sleep 3
export NO_PROXY=127.0.0.1 no_proxy=127.0.0.1
export FENVIX_P1_URI="mongodb://127.0.0.1:$PORT/?replicaSet=rs0"
(cd "$W/a4" && $PY -I prova_infinite.py carica --db app_data_4 --gb 6.6 --quota-riservati 0.4883 --seme 104 --thread 16 --file stato.json > carica.log 2>&1; echo "exit $?" >> carica.log) &
L4=$!
(cd "$W/a6" && $PY -I prova_infinite.py carica --db app_data_6 --gb 9.67 --quota-riservati 0.4 --seme 106 --thread 16 --file stato.json > carica.log 2>&1; echo "exit $?" >> carica.log) &
L6=$!
T0=$(date +%s)
while true; do
  EL=$(( $(date +%s) - T0 ))
  FREE=$(df --output=avail -k "$W" | tail -1)
  line="t=$EL free_gb=$((FREE/1048576))"
  for d in app_data_4 app_data_6; do
    P=$W/store/tenants/$d
    if [ -d "$P" ]; then
      line="$line $d:tot_mb=$(du -sm "$P" | cut -f1),compacted_mb=$(du -sm "$P/compacted" 2>/dev/null | cut -f1),ssts=$(ls "$P/compacted" 2>/dev/null | wc -l),wal=$(ls "$P/wal" 2>/dev/null | wc -l)"
    fi
  done
  line="$line failed_passes=$(grep -c "compaction of tenant .* failed" "$W/server.log")"
  echo "$line" >> "$W/monitor.log"
  if [ "$FREE" -lt 6000000 ] || [ $EL -gt $((MAXMIN*60)) ] || ! kill -0 $SPID 2>/dev/null; then
    echo "STOP t=$EL free_kb=$FREE" >> "$W/monitor.log"; break
  fi
  if ! kill -0 $L4 2>/dev/null && ! kill -0 $L6 2>/dev/null; then echo "LOADERS DONE" >> "$W/monitor.log"; break; fi
  sleep 15
done
kill $L4 $L6 2>/dev/null; pkill -f "prova_infinite.py carica --db app_data_[46]"; sleep 2
kill $SPID 2>/dev/null; wait $SPID 2>/dev/null
echo DONE >> "$W/monitor.log"
