#!/bin/bash
# Prova locale P1 Irlanda, 0 $, niente AWS e niente Atlas veri:
#   - AWS finto: moto (S3, EC2, IAM, SSM) e il finto `aws` di M31: `ec2 run-instances` esegue lo user-data QUI;
#   - "Atlas" finto: il nostro mongo-server (store locale, replica set rs0) su 127.0.0.1, acceso solo DOPO
#     ACCENDI_DOPO_S secondi, per provare l'attesa della lista di accesso;
#   - dati: il comando `carica` dello script, piccolo; poi il workflow passo per passo: controllo a secco, permessi,
#     giro-ci.sh (con lo script e le opzioni VERE di opzioni.txt), recupero, pulizia, controllo dei segreti,
#     controllo finale; alla fine i conti dei clienti letti e il piano del giro dopo (come M10).
# Uso: prova-giro-locale.sh CARTELLA_DI_LAVORO MONGO_SERVER_BINARIO
set -u
W=$1; SRV=$2
QUI=$(cd "$(dirname "$0")" && pwd); I=$(cd "$QUI/.." && pwd)
PORTA=${PORTA:-5066}; DBP=${DBP:-27733}
rm -rf "$W"; mkdir -p "$W"; cd "$W" || exit 1
export SANDBOX=$W/sandbox; mkdir -p "$SANDBOX"; cp -r "$QUI/bin" "$QUI/bin-macchina" "$SANDBOX/"
export MOTO_URL=http://127.0.0.1:$PORTA AWS_ENDPOINT_URL=http://127.0.0.1:$PORTA
export AWS_DEFAULT_REGION=eu-west-1 AWS_REGION=eu-west-1 AWS_PAGER="" AWS_ACCESS_KEY_ID=fresca AWS_SECRET_ACCESS_KEY=fresca
export NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
export TIPO=c7i.8xlarge PREZZO_ORA=1.532 TETTO_DOLLARI=5 COPIE=${COPIE:-3} RUOLO_MACCHINA=fenvix-coldread-machine
export IRL_ACCT=123456789012 IRL_ROLE=arn:aws:iam::123456789012:role/finto BUCKET=fenvix-coldread-irl-$RANDOM
export IRL_OUT=$W/out GITHUB_STEP_SUMMARY=$W/summary.md IRL_PACCO=$W/irlanda.tar.gz
export DEMO_DB_URI="mongodb://127.0.0.1:$DBP/?replicaSet=rs0"
# per la "macchina" (lo user-data gira qui): niente shutdown vero, ping veloci
export PROVA_LOCALE=1 IP_FINTO=203.0.113.7 ATTESA_MIN=${ATTESA_MIN:-5} SALI_S=10 IRL_ATTESA_SCARICO=1
export MINUTO_S=${MINUTO_S:-15} MUTA_MIN=${MUTA_MIN:-40} TETTO_MIN=${TETTO_MIN:-200}
nohup moto_server -p "$PORTA" > moto.log 2>&1 &
MOTO=$!
for _ in $(seq 1 40); do curl -fs "$MOTO_URL/moto-api/" > /dev/null 2>&1 && break; sleep 0.5; done
export PATH="$SANDBOX/bin:$PATH"
aws iam create-role --role-name fenvix-coldread-machine --assume-role-policy-document '{"Version":"2012-10-17","Statement":[]}' > /dev/null
aws iam create-instance-profile --instance-profile-name fenvix-coldread-machine > /dev/null
aws iam add-role-to-instance-profile --instance-profile-name fenvix-coldread-machine --role-name fenvix-coldread-machine
avvia_db() { nohup "$SRV" --listen 127.0.0.1:$DBP --store local --local-dir "$W/db" --replica-set rs0 --advertise 127.0.0.1:$DBP > "$W/db.log" 2>&1 & echo $! > "$W/db.pid"; sleep 2; }
ferma_db() { kill "$(cat "$W/db.pid")" 2>/dev/null; sleep 1; }
echo "== 1. dati piccoli con il comando 'carica' dello script (come la prova locale dell'inventario)"
avvia_db
mkdir -p carico && cp "$I/prova_infinite.py" carico/
( cd carico && FENVIX_P1_URI=$DEMO_DB_URI python3 prova_infinite.py carica --gb 0.02 --mb-cliente 0.03 --thread 2 --lotto 200 \
    --file risultati_p1.json > carica.log 2>&1 ); echo "carica: codice $?"; tail -2 carico/carica.log
: > carico/risultati_p1.json.usati.log
# come dopo il giro M30: un giro_finale con i quattro livelli del parallelo gia' fatti (senza prepara_stato.py il
# parallelo dall'Irlanda verrebbe saltato)
python3 -c "import json; f='carico/risultati_p1.json'; d=json.load(open(f)); d['giro_finale']={'nome': 'm30', 'cartella': 'giro_finale_m30_FINTA', 'fasi': {}, 'parallelo_livelli_fatti': ['L1', 'L10', 'L50', 'L100']}; json.dump(d, open(f, 'w'))"
ferma_db
mkdir -p stato && cp carico/risultati_p1.json carico/risultati_p1.json.usati.log stato/
md5sum stato/* > stato-prima.md5
echo "== 2. il pacco come nel workflow (librerie per il Python di questa macchina)"
mkdir -p pacco && python3 -m pip install -q --target pacco/vendor 'pymongo>=4.6,<5' backports.zstd boto3 > pip.log 2>&1
cp "$I/prova_infinite.py" "$I/esegui.sh" "$I/s3io.py" "$I/attendi_db.py" "$I/opzioni.txt" "$I/massimo.txt" \
  "$I/cpu_macchina.py" "$I/somma_massimo.py" "$I/controlla_parallelo.py" pacco/
echo "$COPIE" > pacco/copie.txt
tar czf "$IRL_PACCO" -C pacco .
echo "== 3. controllo dei file di stato e piano a secco"
python3 "$I/prepara_stato.py" stato/risultati_p1.json
PYTHONPATH=$W/pacco/vendor python3 "$I/controlla_stato.py" stato | grep -E "^(OK|FERMO|STATO)"
echo "== 4. permessi (moto)"
bash "$I/ci/permessi.sh" | tail -3
echo "== 5. il giro: il database finto si accende tra ${ACCENDI_DOPO_S:-60} s (attesa della lista di accesso)"
( sleep "${ACCENDI_DOPO_S:-60}"; avvia_db; echo "$(date -u +%T) database acceso" >> "$W/eventi.log" ) &
STATO_DIR=$W/stato bash "$I/ci/giro-ci.sh" > giro-ci.log 2>&1; echo "giro-ci.sh: codice $?"
grep -E "IP DA|SPESA|esito|macchina:" giro-ci.log | sed -n '1,4p;$p'
echo "== 6. recupero, pulizia, segreti, controllo finale"
bash "$I/ci/recupera.sh"; bash "$I/ci/pulizia.sh"
python3 "$I/ci/controlla_segreti.py" "$IRL_OUT"; echo "segreti: codice $?"
IRL_INIZIO=$(date -u -d '1 hour ago' +%FT%TZ) bash "$I/ci/controllo-finale.sh"; echo "controllo finale: codice $?"
ferma_db; kill $MOTO 2>/dev/null
echo "== 7. conti"
cat "$IRL_OUT/fine.json" 2>/dev/null
python3 - "$W" <<'PY'
import json, sys, os
w = sys.argv[1]
a = json.load(open(f"{w}/stato/risultati_p1.json")); b = json.load(open(f"{w}/out/stato/risultati_p1.json"))
la = open(f"{w}/stato/risultati_p1.json.usati.log").read().split("\n"); lb = open(f"{w}/out/stato/risultati_p1.json.usati.log").read().split("\n")
nuovi = set(b.get("usati", [])) - set(a.get("usati", []))
print(f"clienti letti prima {len(a.get('usati', []))}, dopo {len(b.get('usati', []))}, nuovi {len(nuovi)}; righe del registro prima {len(la)-1}, dopo {len(lb)-1}")
print("registro = file di stato per i nuovi:", {int(x.split()[1]) for x in lb if x.startswith('a ')} == nuovi)
gf = b.get("giro_finale") or {"nome": None, "fasi": {}}; print("giro:", gf["nome"], "fasi:", {f: [e.get("stato") for e in v] for f, v in gf["fasi"].items() if not f.startswith("pausa")})
PY
ls "$IRL_OUT" "$IRL_OUT"/dati/* | head -30
echo "== 8. parallelo e carico massimo"
cat "$IRL_OUT/parallelo-connessioni.txt"
cat "$IRL_OUT/massimo-somma.txt"
grep -E "carico_massimo|ATTENZIONE|connessioni del parallelo" "$IRL_OUT/passi.log"
python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print('chiavi giro:', sorted(k for k in d if k.startswith('giro_finale')), '- nome', d['giro_finale']['nome'], '- livelli fatti', d['giro_finale'].get('parallelo_livelli_fatti'))" "$IRL_OUT/stato/risultati_p1.json"
ls "$IRL_OUT/massimo"; wc -l < "$IRL_OUT/cpu-macchina.log"
