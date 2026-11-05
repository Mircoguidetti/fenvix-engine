#!/bin/bash
# Cucina minima (K2) · la parte che gira SULLA MACCHINA SERVER (EC2 della taglia K, Ubuntu 24.04, eu-west-1). La lancia
# lo user-data scritto da ci/giro-ci.sh. Niente chiavi AWS: S3 con il ruolo della macchina (IMDSv2), sia per i file della
# giornata (boto3) sia per i dati del nostro server (mongo-server, AmazonS3Builder::from_env, main.rs).
# Ordine: certificato TLS della taglia dal bucket (lo prepara il runner, con l'IP privato di questa macchina) -> password
# dal bucket (solo nella memoria dei processi) -> mongo-server con i dati sul bucket dei dati (lo stesso per tutte le
# taglie: si carica una volta sola), TLS obbligatorio, utenti obbligatori, porta 27017 aperta dal gruppo di sicurezza
# solo alla macchina cliente -> utente root (eccezione localhost) -> "pronto" nel bucket -> ogni 30 s log, CPU e memoria
# nel bucket -> al comando "ferma" del runner: SIGTERM al server, log finali, "fermato", spegnimento.
# Uso: esegui-server.sh BUCKET K   (BUCKET_DATI e il resto da server.env, scritto dal runner nel pacco)
set -u
cd "$(dirname "$0")" || exit 1
BUCKET=$1; K=$2
source ./server.env   # BUCKET_DATI (nessun segreto)
export AWS_DEFAULT_REGION=eu-west-1 AWS_REGION=eu-west-1
export PYTHONPATH="$PWD/vendor" PYTHONUNBUFFERED=1
PY=${PY:-python3}
PORTA=${PORTA_DB:-27017}
P=server-$K
SALI_S=${SALI_S:-30}
mkdir -p out cert
passo() { echo "$(date -u +%FT%TZ) $*" | tee -a out/passi.log; }
sali() { $PY s3io.py sali "$BUCKET" out "risultati/$P/" 2>> out/errori-sali.log; }
fine() { # fine ESITO
  passo "fine: $1"
  [ -n "${SRV:-}" ] && kill -TERM "$SRV" 2>/dev/null && for _ in $(seq 1 60); do kill -0 "$SRV" 2>/dev/null || break; sleep 1; done
  [ -n "${SRV:-}" ] && kill -0 "$SRV" 2>/dev/null && { passo "il server non si e' fermato in 60 s: SIGKILL"; kill -KILL "$SRV"; }
  [ -n "${GUARDA:-}" ] && kill "$GUARDA" 2>/dev/null
  [ -n "${CPU_PID:-}" ] && kill "$CPU_PID" 2>/dev/null
  gzip -kf out/mongo-server.log 2>/dev/null
  printf '{"fine": true, "esito": "%s"}\n' "$1" > out/fermato.json
  sali; sali
  $PY s3io.py metti "$BUCKET" "$P/fermato" out/fermato.json 2>/dev/null
  [ -n "${PROVA_LOCALE:-}" ] || shutdown -h now "cucina: server fermato"
  exit 0
}
imds() { local t; t=$(curl -fsS -m 5 -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 600" 2>/dev/null) &&
  curl -fsS -m 5 -H "X-aws-ec2-metadata-token: $t" "http://169.254.169.254/latest/meta-data/$1" 2>/dev/null; }
TIPO_K=$(imds instance-type || echo "${TIPO_FINTO:-?}")
passo "inizio server $K ($TIPO_K); $(nproc) vCPU, $(awk '/MemTotal/ {printf "%.1f GiB", $2/1048576}' /proc/meminfo), $(uname -m)"
$PY cpu_macchina.py out/cpu-macchina.log 5 & CPU_PID=$!
# 1. il certificato (catena + chiave) e la CA: li prepara il runner dopo il lancio, con l'IP privato di questa macchina
for _ in $(seq 1 60); do $PY s3io.py c-e "$BUCKET" "$P/server.pem" && break; sleep 5; done
$PY s3io.py scarica "$BUCKET" "$P/server.pem" cert/server.pem && $PY s3io.py scarica "$BUCKET" programma/ca.pem cert/ca.pem \
  || fine SENZA_CERTIFICATO
chmod 600 cert/server.pem
# 2. la password: resta nell'ambiente dei processi, mai in un file o in un log
CM_PW=$($PY s3io.py testo "$BUCKET" segreto/pw) && [ -n "$CM_PW" ] || fine SENZA_PASSWORD
export CM_PW
# 3. l'IP privato (quello del certificato e di hello.hosts)
IP=$(imds local-ipv4 || echo "${IP_FINTO:-?}")
passo "IP privato $IP; bucket dei dati $BUCKET_DATI"
# 4. il nostro server. Opzioni: quelle di serie del motore (DECISIONS 2026-11-05: si misura il motore com'e'), piu'
# server.txt (vuoto di serie). Solo nella prova locale: --endpoint verso l'S3 finto.
read -ra EXTRA_SRV < server.txt
END=(); [ -n "${PROVA_LOCALE:-}" ] && [ -n "${AWS_ENDPOINT_URL:-}" ] && END=(--endpoint "$AWS_ENDPOINT_URL")
CMD=(./mongo-server --listen "0.0.0.0:$PORTA" --store s3 --bucket "$BUCKET_DATI" --region eu-west-1 "${END[@]}"
     --replica-set rs0 --advertise "$IP:$PORTA" --auth --tls-mode requireTLS --tls-certificate-key-file cert/server.pem
     "${EXTRA_SRV[@]}")
echo "${CMD[*]}" > out/comando-server.txt
passo "avvio: ${CMD[*]}"
"${CMD[@]}" > out/mongo-server.log 2>&1 &
SRV=$!
echo "$SRV" > out/server.pid
# memoria (RSS) del processo ogni 5 s
( while kill -0 "$SRV" 2>/dev/null; do echo "$(date -u +%FT%TZ) $(ps -o rss= -p "$SRV" | tr -d ' ')" >> out/rss-kib.log; sleep 5; done ) &
GUARDA=$!
$PY crea_utente.py "$PORTA" cert/ca.pem >> out/passi.log 2>&1 || { tail -20 out/mongo-server.log >> out/passi.log; fine SERVER_NON_PARTITO; }
printf '{"ip": "%s", "porta": %s, "tipo": "%s"}\n' "$IP" "$PORTA" "$TIPO_K" > out/pronto.json
$PY s3io.py metti "$BUCKET" "$P/pronto.json" out/pronto.json || fine SENZA_BUCKET
passo "pronto: mongo-server su $IP:$PORTA (TLS, utenti obbligatori)"
# 5. attesa del comando "ferma" del runner; intanto log nel bucket
n=0
while :; do
  sleep "$SALI_S"
  n=$((n + 1))
  if ! kill -0 "$SRV" 2>/dev/null; then wait "$SRV"; passo "ATTENZIONE: mongo-server uscito da solo (codice $?)"; tail -30 out/mongo-server.log >> out/passi.log; fine SERVER_MORTO; fi
  sali
  $PY s3io.py c-e "$BUCKET" "$P/ferma" && fine FERMATO
done
