#!/bin/bash
# Q8 su AWS: la parte che gira SULLA MACCHINA (EC2 c7i.xlarge, Ubuntu 24.04, eu-west-1). La lancia lo user-data
# scritto da giornata.sh, con il bucket e il ruolo della macchina: r0, r1, r2 (una replica del quaderno per zona) o gen
# (generatori e scaricatore finto, nella zona di r0). Niente chiavi: S3 con il ruolo dell'istanza.
# Tetto: la macchina si spegne (= si cancella) da sola dopo 150 minuti; lo user-data lo programma come prima riga.
set -u
B=$1; RUOLO=$2
cd "$(dirname "$0")"
export AWS_REGION=eu-west-1 AWS_DEFAULT_REGION=eu-west-1
mkdir -p out
passo() { echo "$(date -u +%FT%TZ) $RUOLO $*" | tee -a out/passi-$RUOLO.log; aws s3 cp out/passi-$RUOLO.log "s3://$B/risultati/passi-$RUOLO.log" --only-show-errors || true; }
sali() { aws s3 cp out/ "s3://$B/risultati/$RUOLO/" --recursive --only-show-errors || true; }
passo "inizio: $(nproc) vCPU, $(free -g | awk '/Mem/{print $2}') GB, zona $(cloud-init query availability_zone 2>/dev/null || echo ?)"
# il file con gli indirizzi delle tre repliche (lo scrive giornata.sh dopo averle lanciate)
for i in $(seq 1 120); do aws s3 cp "s3://$B/config.json" config.json --only-show-errors 2>/dev/null && break; sleep 5; done
[ -s config.json ] || { passo "ERRORE: niente config.json dopo 10 minuti"; sali; shutdown -h now; exit 1; }
IP0=$(jq -r .r0 config.json); IP1=$(jq -r .r1 config.json); IP2=$(jq -r .r2 config.json)
REPLICHE="$IP0:7200,$IP1:7200,$IP2:7200"
# stessa configurazione della prova in locale (R1): corsie, riserva 200 record/s, tetto di cella 2 MB/s sopra la
# riserva, riciclo dei segmenti, finestra 10 s; in più la replica compressa (zstd 1) per pagare meno il traffico tra zone
export PQ_CORSIE=1 PQ_RICICLA=1 PQ_PREALLOC_MBS=64 PQ_RISERVA_RECS=200 PQ_CELLA_MBS=${CELLA_MBS:-2} PQ_SOPRA_FRAZ=0.5 PQ_ZSTD=1
case "$RUOLO" in
  r0|r1|r2)
    i=${RUOLO#r}
    peers=""
    for j in 0 1 2; do [ "$j" = "$i" ] && continue; ip=$(jq -r .r$j config.json); peers="$peers${peers:+,}$j=$ip:7100"; done
    mkdir -p /opt/q8/dati
    # due vCPU per la replica, come una m7g.large della cella di Fable (le altre due restano al campionatore e al sistema)
    taskset -c 0,1 ./pq replica --id "$i" --dir /opt/q8/dati --peer-listen 0.0.0.0:7100 --client-listen 0.0.0.0:7200 --peers "$peers" \
      --mem-cap-mb 3000 --window-ms 10000 > out/replica.log 2>&1 &
    RP=$!
    passo "replica $i avviata (pid $RP), peers $peers"
    python3 campiona.py "$RP" out/campioni.jsonl &
    # resta viva finché il generatore non scrive fine.json (o il tetto di 150 minuti)
    while ! aws s3 ls "s3://$B/risultati/fine.json" >/dev/null 2>&1; do sleep 60; sali; kill -0 $RP 2>/dev/null || { passo "ERRORE: replica morta"; sali; }; done
    passo "fine"; sali; shutdown -h now ;;
  gen)
    for k in $(seq 1 60); do ./pq status --replicas "$REPLICHE" 2>/dev/null | grep -q Leader && break; sleep 5; done
    passo "gruppo pronto: $(./pq status --replicas "$REPLICHE" 2>/dev/null | jq -c '{id, role}' | tr '\n' ' ')"
    nohup ./pq flusher --replicas "$REPLICHE" --null 1 --age-ms 2000 --interval-ms 500 --hot-db 0 > out/scaricatore.log 2>&1 &
    ( while sleep 60; do
        n=$(./pq status --replicas "$REPLICHE" 2>/dev/null | jq -s 'map(select(.role=="Leader"))[0].commit_index // 0')
        passo "record nel registro finora: $n"; sali
      done ) &
    Q8_REMOTE=1 Q8_REPLICAS="$REPLICHE" python3 corsie.py out/corsie --lanes 1 --levels ${LIVELLI:-1,1,2,20,50,100} --secs 60 --secs-small 120 --warm 10 --tag aws > out/corsie.log 2>&1
    passo "prova finita: $(wc -l < out/corsie/corsie.jsonl 2>/dev/null) fasi"
    sali
    echo '{"fine": true}' > out/fine.json
    aws s3 cp out/fine.json "s3://$B/risultati/fine.json" --only-show-errors
    passo "fine"; sali; shutdown -h now ;;
esac
