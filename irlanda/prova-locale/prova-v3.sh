#!/bin/bash
# Prova locale P1 Irlanda v3 (sessione "analisi Irlanda", 2026-11-05), 0 $, niente AWS e niente Atlas veri:
#   - AWS finto: moto e il finto `aws` (ec2 run-instances esegue lo user-data QUI, come le prove v1 e v2);
#   - "Atlas" finto: il nostro mongo-server (store locale, replica set rs0) su 127.0.0.1;
#   - il "carico P1" (database fenvix_p1, file di stato "vecchio") fatto piccolo con 'carica' dello script, piu' un
#     giro_finale finto con i quattro livelli del parallelo fatti, come dopo il giro M30 dal Mac;
#   - i SETTE programmi veri (carica, m10-a, m10-b, m10-massimo, m30-a, m30-b, m30-massimo) RIMPICCIOLITI con
#     rimpicciolisci.py (stessi passi, stesse fasi, stessi livelli in proporzione, numeri piccoli, pause corte);
#   - per ogni programma i passi del workflow: CATENA, simula_programma.py (deve dire PRONTO), pacco, giro-ci.sh,
#     recupero, pulizia, segreti, file di stato rimessi in stati/ con la CATENA nuova, controllo finale.
# Alla fine: nessun cliente letto due volte (da TUTTE le misure salvate dallo script, non dal file di stato), esiti,
# connessioni del parallelo, validita' della macchina, somma del carico massimo.
# Uso: prova-v3.sh CARTELLA_DI_LAVORO MONGO_SERVER_BINARIO
set -u
W=$1; SRV=$2
QUI=$(cd "$(dirname "$0")" && pwd); I=$(cd "$QUI/.." && pwd)
PORTA=${PORTA:-5067}; DBP=${DBP:-27734}
rm -rf "$W"; mkdir -p "$W"; cd "$W" || exit 1
export SANDBOX=$W/sandbox; mkdir -p "$SANDBOX"; cp -r "$QUI/bin" "$QUI/bin-macchina" "$SANDBOX/"
export MOTO_URL=http://127.0.0.1:$PORTA AWS_ENDPOINT_URL=http://127.0.0.1:$PORTA
export AWS_DEFAULT_REGION=eu-west-1 AWS_REGION=eu-west-1 AWS_PAGER="" AWS_ACCESS_KEY_ID=fresca AWS_SECRET_ACCESS_KEY=fresca
export NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
export TETTO_DOLLARI=5 RUOLO_MACCHINA=fenvix-coldread-machine
export IRL_ACCT=123456789012 IRL_ROLE=arn:aws:iam::123456789012:role/finto
export DEMO_DB_URI="mongodb://127.0.0.1:$DBP/?replicaSet=rs0"
export PROVA_LOCALE=1 IP_FINTO=203.0.113.7 ATTESA_MIN=3 SALI_S=10 IRL_ATTESA_SCARICO=1
export MINUTO_S=${MINUTO_S:-10} MUTA_MIN=${MUTA_MIN:-40}
nohup moto_server -p "$PORTA" > moto.log 2>&1 &
MOTO=$!
for _ in $(seq 1 40); do curl -fs "$MOTO_URL/moto-api/" > /dev/null 2>&1 && break; sleep 0.5; done
export PATH="$SANDBOX/bin:$PATH"
aws iam create-role --role-name fenvix-coldread-machine --assume-role-policy-document '{"Version":"2012-10-17","Statement":[]}' > /dev/null
aws iam create-instance-profile --instance-profile-name fenvix-coldread-machine > /dev/null
aws iam add-role-to-instance-profile --instance-profile-name fenvix-coldread-machine --role-name fenvix-coldread-machine
nohup "$SRV" --listen 127.0.0.1:$DBP --store local --local-dir "$W/db" --replica-set rs0 --advertise 127.0.0.1:$DBP > "$W/db.log" 2>&1 &
DB=$!; sleep 2
echo "== 1. il carico P1 piccolo (fenvix_p1) e il suo file di stato 'vecchio', come dopo il giro M30 dal Mac"
mkdir -p rep/irlanda/stati/vecchio carico && cp "$I/prova_infinite.py" carico/
( cd carico && FENVIX_P1_URI=$DEMO_DB_URI python3 prova_infinite.py carica --db fenvix_p1 --gb 0.03 --mb-cliente 0.03 \
    --thread 2 --lotto 200 --file risultati_p1.json > carica.log 2>&1
  : > risultati_p1.json.usati.log
  FENVIX_P1_URI=$DEMO_DB_URI python3 prova_infinite.py giro --db fenvix_p1 --file risultati_p1.json --cartella-dati d \
    --nuova-cartella --giro m30 --solo-fasi grandi_carica --grandi-gib 0.004 --grandi-clienti 80 --grandi-thread 2 \
    --pausa-s 0 > grandi.log 2>&1 ); echo "carico P1 piccolo: $(tail -1 carico/carica.log | cut -c1-120)"
python3 - carico/risultati_p1.json <<'PY'
import json, sys
f = sys.argv[1]; d = json.load(open(f))
d["giro_finale"] = {"nome": "m30", "cartella": "giro_finale_m30_FINTA", "fasi": {}, "parallelo_livelli_fatti": ["L1", "L10", "L50", "L100"]}
# come dopo il giro M30 dal Mac: una parte dei clienti gia' letti
cl = d["clienti"]; ris = set(cl["riservati"])
att = [i for i in range(cl["n"]) if i not in ris][: cl["n"] // 3]
d["usati"] = sorted(set(d.get("usati", [])) | set(att))
json.dump(d, open(f, "w"))
PY
cp carico/risultati_p1.json carico/risultati_p1.json.usati.log rep/irlanda/stati/vecchio/
( cd rep/irlanda/stati && find . -type f ! -name CATENA.txt | sort | xargs md5sum ) > rep/irlanda/stati/CATENA.txt
echo "== 2. i programmi rimpiccioliti"
python3 "$QUI/rimpicciolisci.py" "$I/programmi" rep/irlanda/programmi
mkdir -p pacco && python3 -m pip install -q --target pacco/vendor 'pymongo>=4.6,<5' backports.zstd boto3 > pip.log 2>&1
tot_err=0
# PROGRAMMI: quali programmi, in fila (di serie i sette della v3; per i giri in un solo programma, correzione B3 del
# revisore: PROGRAMMI="carica m10-tutto m30-tutto" oppure "carica m10-tutto-16 m30-tutto-16")
for PROG in ${PROGRAMMI:-carica m10-a m10-b m10-massimo m30-a m30-b m30-massimo}; do
  echo "== programma $PROG"
  read -r TIPO TETTO < rep/irlanda/programmi/$PROG/macchina.txt
  case "$TIPO" in c7i.xlarge) PREZZO_ORA=0.1915;; c7i.2xlarge) PREZZO_ORA=0.383;; c7i.4xlarge) PREZZO_ORA=0.766;; c7i.8xlarge) PREZZO_ORA=1.532;; esac
  export TIPO PREZZO_ORA TETTO_MACCHINA_MIN=$TETTO TETTO_MIN=$((TETTO + 15)) BUCKET=fenvix-coldread-irl-$RANDOM
  export IRL_OUT=$W/out-$PROG GITHUB_STEP_SUMMARY=$W/summary-$PROG.md IRL_PACCO=$W/irlanda-$PROG.tar.gz
  # (workflow) CATENA
  ( cd rep/irlanda/stati && grep -v '^IN CORSO' CATENA.txt | md5sum -c --quiet ) || { echo "CATENA rotta"; tot_err=$((tot_err+1)); }
  grep -q '^IN CORSO' rep/irlanda/stati/CATENA.txt && { echo "IN CORSO rimasto: FERMO"; tot_err=$((tot_err+1)); break; }
  # (workflow) controllo a secco del programma
  rm -rf stato stato-sim && mkdir stato && cp -r rep/irlanda/stati/. stato/ && rm -f stato/CATENA.txt && cp -r stato stato-sim
  PYTHONPATH=$W/pacco/vendor python3 "$I/simula_programma.py" stato-sim rep/irlanda/programmi/$PROG/passi.txt > sim-$PROG.txt 2>&1
  tail -1 sim-$PROG.txt; grep -q "^STATO: PRONTO" sim-$PROG.txt || { grep FERMO sim-$PROG.txt | head; tot_err=$((tot_err+1)); break; }
  echo "IN CORSO prova $PROG" >> rep/irlanda/stati/CATENA.txt
  # (workflow) pacco
  rm -rf pacco/*.py pacco/*.sh pacco/*.txt
  cp "$I/prova_infinite.py" "$I/esegui.sh" "$I/s3io.py" "$I/attendi_db.py" "$I/programma.py" "$I/cpu_macchina.py" \
    "$I/somma_massimo.py" "$I/controlla_parallelo.py" pacco/
  cp rep/irlanda/programmi/$PROG/passi.txt pacco/passi.txt; echo "$PROG" > pacco/programma.txt
  tar czf "$IRL_PACCO" -C pacco .
  bash "$I/ci/permessi.sh" > permessi-$PROG.txt 2>&1; tail -1 permessi-$PROG.txt
  STATO_DIR=$W/stato bash "$I/ci/giro-ci.sh" > giro-ci-$PROG.log 2>&1; echo "giro-ci.sh: codice $?"
  grep -E "SPESA|esito" giro-ci-$PROG.log | tail -2
  bash "$I/ci/recupera.sh" > /dev/null; bash "$I/ci/pulizia.sh" > pulizia-$PROG.txt 2>&1
  python3 "$I/ci/controlla_segreti.py" "$IRL_OUT" > /dev/null; echo "segreti: codice $?"
  # (workflow) i file di stato tornano in stati/ con la CATENA nuova
  if [ -d "$IRL_OUT/stati" ]; then
    cp -r "$IRL_OUT"/stati/. rep/irlanda/stati/
    ( cd rep/irlanda/stati && find . -type f ! -name CATENA.txt | sort | xargs md5sum ) > rep/irlanda/stati/CATENA.txt
  fi
  IRL_INIZIO=$(date -u -d '1 hour ago' +%FT%TZ) bash "$I/ci/controllo-finale.sh" > finale-$PROG.txt 2>&1; echo "controllo finale: codice $? $(grep -E 'RIMASTO|ALLARME' finale-$PROG.txt | tail -1)"
  cat "$IRL_OUT/fine.json" 2>/dev/null
  grep -E "programma finito|ERRORE|FERMO|connessioni del parallelo|massimo:" "$IRL_OUT/passi.log" | cut -c1-200
  tail -1 "$IRL_OUT/validita-macchina.txt" 2>/dev/null
  grep -q '"esito": "FINITO' "$IRL_OUT/fine.json" 2>/dev/null || tot_err=$((tot_err+1))
done
kill $DB 2>/dev/null; kill $MOTO 2>/dev/null
echo "== nessun cliente letto due volte (dalle misure salvate dallo script in tutte le cartelle dei dati)"
python3 "$QUI/conta_letti.py" "$W" rep/irlanda/stati
echo "== errori dei programmi: $tot_err"
