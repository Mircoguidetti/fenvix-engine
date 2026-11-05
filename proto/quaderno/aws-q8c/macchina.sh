#!/bin/bash
# Q8c on AWS: what runs ON EACH MACHINE (EC2, Ubuntu 24.04, eu-west-1). Started by the user-data written by
# giornata.sh, with the bucket and the machine's role: r0, r1, r2 (one copy of EVERY log of the cell, one machine per
# zone, one gp3 disk per machine) or gen (generators and the fake flushers, in the zone of r0). No keys: S3 with the
# instance role. Ceiling: the machine shuts down (= is terminated) by itself after 150 minutes (first user-data line).
#
# The cell: LOGS logs (default 4), each a Raft group of three replicas (one per machine) with its own leader, threads
# and files; log g listens on 7100+1000g (replicas) and 7200+1000g (clients); its preferred leader is machine g % 3
# (the other two wait 3 s more before their first vote), so the leaders are spread over the three machines.
set -u
B=$1; RUOLO=$2
cd "$(dirname "$0")"
export AWS_REGION=eu-west-1 AWS_DEFAULT_REGION=eu-west-1
mkdir -p out
passo() { echo "$(date -u +%FT%TZ) $RUOLO $*" | tee -a out/passi-$RUOLO.log; aws s3 cp out/passi-$RUOLO.log "s3://$B/risultati/passi-$RUOLO.log" --only-show-errors || true; }
sali() { aws s3 cp out/ "s3://$B/risultati/$RUOLO/" --recursive --only-show-errors || true; }
passo "start: $(nproc) vCPU, $(free -g | awk '/Mem/{print $2}') GB, zone $(cloud-init query availability_zone 2>/dev/null || echo ?)"
for i in $(seq 1 120); do aws s3 cp "s3://$B/config.json" config.json --only-show-errors 2>/dev/null && break; sleep 5; done
[ -s config.json ] || { passo "ERROR: no config.json after 10 minutes"; sali; shutdown -h now; exit 1; }
IP0=$(jq -r .r0 config.json); IP1=$(jq -r .r1 config.json); IP2=$(jq -r .r2 config.json)
LOGS=$(jq -r '.logs // 4' config.json)
LIVELLI=$(jq -r '.levels // "1,2,20,50,100"' config.json)
# "ip0:7200,ip1:7200,ip2:7200;ip0:8200,..." one group per log
GRUPPI=""
for g in $(seq 0 $((LOGS - 1))); do
  p=$((7200 + 1000 * g)); GRUPPI="$GRUPPI${GRUPPI:+;}$IP0:$p,$IP1:$p,$IP2:$p"
done
# same log configuration as the local Q8c run: lanes (reserve 200 records/s, cell cap 2 MB/s over the reserve),
# segment recycling (with the Q8c fix), the per-database door (1,000 records/s and 2 MB/s, 0.1 s burst, at most 0.5 s
# waiting outside, beyond that an immediate retryable reject with the exact wait), 10 s window; plus zstd 1 on the
# replication stream (less cross-zone traffic)
export PQ_CORSIE=1 PQ_RICICLA=1 PQ_PREALLOC_MBS=64 PQ_RISERVA_RECS=200 PQ_CELLA_MBS=${CELLA_MBS:-2} PQ_SOPRA_FRAZ=0.5 PQ_ZSTD=1
export PQ_DOOR=1 PQ_DOOR_RECS=${DOOR_RECS:-1000} PQ_DOOR_MBS=${DOOR_MBS:-2} PQ_DOOR_BURST_S=0.1 PQ_DOOR_WAIT_MS=500
case "$RUOLO" in
  r0|r1|r2)
    i=${RUOLO#r}
    MEM=$(( $(free -m | awk '/Mem/{print $2}') * 6 / 10 / LOGS ))
    PIDS=""
    for g in $(seq 0 $((LOGS - 1))); do
      peers=""
      for j in 0 1 2; do [ "$j" = "$i" ] && continue; ip=$(jq -r .r$j config.json); peers="$peers${peers:+,}$j=$ip:$((7100 + 1000 * g))"; done
      delay=0; [ $((g % 3)) = "$i" ] || delay=3000
      mkdir -p /opt/q8/dati/g$g
      ./pq replica --id "$i" --dir /opt/q8/dati/g$g --peer-listen 0.0.0.0:$((7100 + 1000 * g)) --client-listen 0.0.0.0:$((7200 + 1000 * g)) --peers "$peers" \
        --mem-cap-mb "$MEM" --window-ms 10000 --first-elect-delay-ms $delay > out/replica-g$g.log 2>&1 &
      PIDS="$PIDS $!"
    done
    passo "replicas of $LOGS logs started (pids$PIDS)"
    python3 campiona.py out/campioni.jsonl $PIDS &
    while ! aws s3 ls "s3://$B/risultati/fine.json" >/dev/null 2>&1; do
      sleep 60; sali
      for p in $PIDS; do kill -0 $p 2>/dev/null || passo "ERROR: replica $p dead"; done
    done
    passo "end"; sali; shutdown -h now ;;
  gen)
    for k in $(seq 1 60); do
      n=0; for g in $(seq 0 $((LOGS - 1))); do ./pq status --replicas "$(cut -d';' -f$((g + 1)) <<<"$GRUPPI")" 2>/dev/null | grep -q Leader && n=$((n + 1)); done
      [ "$n" = "$LOGS" ] && break; sleep 5
    done
    passo "cell ready: $n of $LOGS logs with a leader"
    for g in $(seq 0 $((LOGS - 1))); do
      R=$(cut -d';' -f$((g + 1)) <<<"$GRUPPI")
      passo "log $g: $(./pq status --replicas "$R" 2>/dev/null | jq -c '{id, role}' | tr '\n' ' ')"
      nohup ./pq flusher --replicas "$R" --null 1 --age-ms 2000 --interval-ms 500 --hot-db 0 > out/scaricatore-g$g.log 2>&1 &
    done
    ( while sleep 60; do
        tot=0
        for g in $(seq 0 $((LOGS - 1))); do
          n=$(./pq status --replicas "$(cut -d';' -f$((g + 1)) <<<"$GRUPPI")" 2>/dev/null | jq -s 'map(select(.role=="Leader"))[0].commit_index // 0'); tot=$((tot + n))
        done
        passo "records in the logs so far: $tot"; sali
      done ) &
    Q8_REMOTE=1 Q8_LOGS_REPLICAS="$GRUPPI" python3 q8c.py out/q8c --logs "$LOGS" --levels "$LIVELLI" --secs 60 --secs-small 120 --warm 10 --tag aws > out/q8c.log 2>&1
    passo "test finished: $(wc -l < out/q8c/q8c.jsonl 2>/dev/null) phases"
    sali
    echo '{"fine": true}' > out/fine.json
    aws s3 cp out/fine.json "s3://$B/risultati/fine.json" --only-show-errors
    passo "end"; sali; shutdown -h now ;;
esac
