#!/bin/bash
# Prova locale della cucina minima (K2), 0 $, niente AWS vero:
#   - AWS finto: moto (S3, EC2, IAM, SSM) e il finto `aws` (copia dell'Irlanda): `ec2 run-instances` esegue lo user-data
#     QUI, in una cartella a parte, con un `shutdown` finto; `terminate-instances` uccide i processi della "macchina";
#   - il nostro mongo-server VERO, con i dati sull'S3 finto (--store s3, solo qui --endpoint verso moto), TLS con i
#     certificati della giornata, --auth, replica set rs0: la stessa riga di comando della macchina vera;
#   - lo script del fondatore NON modificato: caricamento piccolo, poi per ogni taglia il giro con le opzioni VERE
#     (opzioni.txt) piu', solo qui, pause corte (OPZIONI_EXTRA), e il carico massimo con COPIE copie;
#   - il workflow passo per passo: permessi, pacchi, giro-ci.sh, recupero, pulizia, segreti, controllo finale.
# Uso: prova-locale.sh CARTELLA_DI_LAVORO CARTELLA_BIN   (CARTELLA_BIN/mongo-server-x86_64/mongo-server; nella prova
# la stessa build fa anche da aarch64, per far passare la strada delle Graviton)
set -u
W=$1; BINSRC=$2
QUI=$(cd "$(dirname "$0")" && pwd); C=$(cd "$QUI/.." && pwd)
PORTA=${PORTA:-5077}
rm -rf "$W"; mkdir -p "$W"; cd "$W" || exit 1
export SANDBOX=$W/sandbox; mkdir -p "$SANDBOX"; cp -r "$QUI/bin" "$QUI/bin-macchina" "$SANDBOX/"
export MOTO_URL=http://127.0.0.1:$PORTA AWS_ENDPOINT_URL=http://127.0.0.1:$PORTA
export AWS_DEFAULT_REGION=eu-west-1 AWS_REGION=eu-west-1 AWS_PAGER="" AWS_ACCESS_KEY_ID=fresca AWS_SECRET_ACCESS_KEY=fresca
export NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
export C TAGLIE=${TAGLIE:-'["c7i.large","c7g.large"]'} GB=${GB:-0.05} CLIENTE_CHIESTO=auto COPIE_CHIESTE=${COPIE_CHIESTE:-2}
export TETTO_DOLLARI=${TETTO_DOLLARI:-5} RUOLO_MACCHINA=fenvix-coldread-machine DISCO_GB=20
export CM_ACCT=123456789012 CM_ROLE=arn:aws:iam::123456789012:role/finto GITHUB_RUN_ID=$RANDOM
export BUCKET=fenvix-coldread-cm-$GITHUB_RUN_ID BUCKET_DATI=fenvix-coldread-cmd-$GITHUB_RUN_ID
export CM_OUT=$W/out GITHUB_STEP_SUMMARY=$W/summary.md GITHUB_ENV=$W/github.env PACCHI=$W/pacchi CA_DIR=$W/ca BIN=$W/bin
export CM_PW=$(openssl rand -hex 24)
# per le "macchine" (lo user-data gira qui): niente shutdown vero, pause corte, il server su 127.0.0.1
export PROVA_LOCALE=1 IP_FINTO=127.0.0.1 IP_SERVER_FINTO=127.0.0.1 PORTA_DB=${PORTA_DB:-27744} SALI_S=10 CM_ATTESA_SCARICO=1
export OPZIONI_EXTRA=${OPZIONI_EXTRA:-"--pausa-s 5 --min-secondi-fase 5 --par-secondi 5"}
export OPZIONI_EXTRA_MASSIMO=${OPZIONI_EXTRA_MASSIMO:-"--pausa-s 5 --minuti-massimo 0.25 --thread-massimo 8"}
export OPZIONI_EXTRA_CARICA=${OPZIONI_EXTRA_CARICA:-"--mb-cliente 0.03 --thread 2 --lotto 200"}
export MINUTO_S=${MINUTO_S:-15} MUTA_MIN=${MUTA_MIN:-40} TETTO_MIN=${TETTO_MIN:-200}
mkdir -p "$BIN/mongo-server-x86_64" "$BIN/mongo-server-aarch64"
for a in x86_64 aarch64; do cp "$BINSRC/mongo-server" "$BIN/mongo-server-$a/"; echo "prova-locale $(cd "$C" && git rev-parse HEAD)" > "$BIN/mongo-server-$a/motore-commit.txt"; done
nohup moto_server -p "$PORTA" > moto.log 2>&1 &
MOTO=$!
for _ in $(seq 1 40); do curl -fs "$MOTO_URL/moto-api/" > /dev/null 2>&1 && break; sleep 0.5; done
export PATH="$SANDBOX/bin:$PATH"
aws iam create-role --role-name fenvix-coldread-machine --assume-role-policy-document '{"Version":"2012-10-17","Statement":[]}' > /dev/null
aws iam create-instance-profile --instance-profile-name fenvix-coldread-machine > /dev/null
aws iam add-role-to-instance-profile --instance-profile-name fenvix-coldread-machine --role-name fenvix-coldread-machine
T0=$(date +%s)
echo "== 1. permessi (moto) e scelta del cliente con la quota finta (8 vCPU, come oggi)"
echo 8.0 > "$SANDBOX/quota-vcpu"
bash "$C/ci/permessi.sh" > permessi.txt 2>&1; echo "permessi: codice $?"; grep -E "quota|PERMESSI|MANCA" permessi.txt
set -a; source "$GITHUB_ENV"; set +a
echo "cliente $CLIENTE, copie $COPIE"
echo "== 2. pacchi e CA"
VENDOR_LOCALE=1 bash "$C/ci/pacchi.sh" > pacchi.txt 2>&1; echo "pacchi: codice $?"; grep -E "^(cliente|server)" pacchi.txt
echo "== 3. la giornata (taglie $TAGLIE)"
t=$(date +%s)
bash "$C/ci/giro-ci.sh" > giro-ci.log 2>&1; echo "giro-ci.sh: codice $? in $(( $(date +%s) - t )) s"
grep -E "^(giornata|gruppi|cliente:|taglia|esito|SPESA)" giro-ci.log | sed -n '1,12p;$p'
echo "== 4. recupero, pulizia, segreti, controllo finale"
bash "$C/ci/recupera.sh"; bash "$C/ci/pulizia.sh"; echo "pulizia: codice $?"
python3 "$C/ci/controlla_segreti.py" "$CM_OUT"; echo "segreti: codice $?"
CM_INIZIO=$(date -u -d '3 hours ago' +%FT%TZ) bash "$C/ci/controllo-finale.sh"; echo "controllo finale: codice $?"
kill $MOTO 2>/dev/null
pgrep -f "mongo-server --listen 0.0.0.0:$PORTA_DB" > /dev/null && echo "ATTENZIONE: mongo-server ancora vivo" || echo "nessun mongo-server rimasto vivo"
echo "== 5. conti"
cat "$CM_OUT/giornata.json"; cat "$CM_OUT/esiti-taglie.txt"
grep -E "caricamento|taglia [0-9]+:|piano:|esiti" "$CM_OUT/cliente/passi.log"
for d in "$CM_OUT"/cliente/taglia-*; do echo "-- $(basename "$d")"; tail -1 "$d/parallelo-connessioni.txt"; grep -E "VERDETTO" "$d/massimo-somma.txt" | head -3; done
for s in "$CM_OUT"/server-*; do echo "-- $(basename "$s"): $(head -1 "$s/passi.log")"; grep -E "pronto|fine|utente|login" "$s/passi.log"; cat "$s/fermato.json"; done
python3 - "$CM_OUT/cliente/stato/risultati_p1.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
print("clienti", len(d.get("clienti", [])) if isinstance(d.get("clienti"), list) else "?", "usati", len(d.get("usati", [])),
      "chiavi giro:", sorted(k for k in d if k.startswith("giro_finale")))
for k in sorted(k for k in d if k.startswith("giro_finale")):
    g = d[k]; print(" ", k, g.get("nome"), {f: [e.get("stato") for e in v] for f, v in g.get("fasi", {}).items() if not f.startswith("pausa")})
PY
echo "durata totale: $(( $(date +%s) - T0 )) s"
