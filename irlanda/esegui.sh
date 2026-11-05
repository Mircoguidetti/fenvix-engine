#!/bin/bash
# P1 · giro corto dall'Irlanda: la parte che gira SULLA MACCHINA (EC2 c7i.xlarge, Ubuntu 24.04, eu-west-1, accanto ad
# Atlas). La lancia lo user-data scritto da ci/giro-ci.sh. Niente chiavi AWS: S3 con il ruolo della macchina (IMDS).
# Ordine: stringa di connessione dal bucket (letta e cancellata subito) -> file di stato dal bucket -> IP pubblico ->
# attesa che Atlas accetti l'IP (al massimo ATTESA_MIN minuti) -> lo script p1-1.6.2, NON modificato (md5 controllato),
# solo Q2, Q3, Q1 -> risultati nel bucket sotto risultati/ ogni 30 secondi e alla fine -> la macchina si spegne da sola
# (spegnimento = cancellazione, impostato al lancio). Tetto di tempo: lo user-data programma lo spegnimento a 100 minuti.
set -u
cd "$(dirname "$0")" || exit 1
BUCKET=$1
export AWS_DEFAULT_REGION=eu-west-1 AWS_REGION=eu-west-1
export PYTHONPATH="$PWD/vendor" PYTHONUNBUFFERED=1   # righe dello script subito nel log (lo script non cambia)
PY=${PY:-python3}
MD5_SCRIPT=8396ca455557baaa4e3a9b1f881451f9
ATTESA_MIN=${ATTESA_MIN:-30}
SALI_S=${SALI_S:-30}
# il comando del giro, dentro la cartella lavoro/: in opzioni.txt (lo stesso controllato a secco dal workflow
# prima di spendere e stampato nel rapporto)
read -ra OPZIONI < opzioni.txt
mkdir -p out/stato lavoro/dati
passo() { echo "$(date -u +%FT%TZ) $*" | tee -a out/passi.log; }
copia_stato() { # il file di stato e il registro dei clienti letti tornano sempre indietro, anche a meta' giro
  [ -f lavoro/risultati_p1.json ] && cp lavoro/risultati_p1.json out/stato/ 2>/dev/null
  [ -f lavoro/risultati_p1.json.usati.log ] && cp lavoro/risultati_p1.json.usati.log out/stato/ 2>/dev/null
  [ -d lavoro/dati ] && cp -r lavoro/dati out/ 2>/dev/null
  return 0
}
sali() { copia_stato; $PY s3io.py sali "$BUCKET" out risultati/ 2>> out/errori-sali.log; }
( while sleep "$SALI_S"; do sali; done ) &
SALITORE=$!
fine() { # fine ESITO
  passo "fine: $1"
  kill $SALITORE 2>/dev/null
  copia_stato
  printf '{"fine": true, "esito": "%s"}\n' "$1" > out/fine.json
  sali; sali
  [ -n "${PROVA_LOCALE:-}" ] || shutdown -h now "irlanda: fine"
  exit 0
}
passo "inizio; macchina $(nproc) vCPU; $($PY --version 2>&1)"
# 1. lo script e' quello del fondatore, byte per byte
md5=$(md5sum prova_infinite.py | cut -d' ' -f1)
[ "$md5" = "$MD5_SCRIPT" ] || { passo "ERRORE: md5 dello script $md5, atteso $MD5_SCRIPT"; fine SCRIPT_DIVERSO; }
# 2. la stringa di connessione: dal bucket, poi l'oggetto si cancella subito; resta solo nella memoria del processo
FENVIX_P1_URI=$($PY s3io.py prendi-e-cancella "$BUCKET" segreto/uri) || { passo "ERRORE: stringa di connessione non letta"; fine SENZA_URI; }
[ -n "$FENVIX_P1_URI" ] || { passo "ERRORE: stringa di connessione vuota"; fine SENZA_URI; }
export FENVIX_P1_URI
passo "stringa di connessione letta (non scritta da nessuna parte) e cancellata dal bucket"
# 3. i file di stato arrivati dal Mac del fondatore
$PY s3io.py scarica "$BUCKET" stato/risultati_p1.json lavoro/risultati_p1.json || { passo "ERRORE: file di stato mancante"; fine SENZA_STATO; }
$PY s3io.py scarica "$BUCKET" stato/risultati_p1.json.usati.log lavoro/risultati_p1.json.usati.log \
  || passo "ATTENZIONE: registro risultati_p1.json.usati.log non arrivato (il workflow lo richiede: non dovrebbe succedere)"
md5sum lavoro/risultati_p1.json* > out/stato-in-ingresso.md5
wc -l lavoro/risultati_p1.json.usati.log >> out/stato-in-ingresso.md5 2>/dev/null
passo "file di stato arrivati: $(tr '\n' ' ' < out/stato-in-ingresso.md5)"
# 4. l'IP pubblico della macchina: e' quello da mettere nella lista di accesso di Atlas
tok=$(curl -fsS -m 5 -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 600" 2>/dev/null)
IP=$(curl -fsS -m 5 -H "X-aws-ec2-metadata-token: $tok" http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "${IP_FINTO:-?}")
echo "$IP" > out/ip.txt
passo "IP pubblico della macchina: $IP (da aggiungere alla lista di accesso di Atlas, come $IP/32)"
# 5. attesa: Atlas risponde solo quando l'IP e' nella lista di accesso
$PY attendi_db.py "$ATTESA_MIN" >> out/passi.log 2>&1 || fine ATLAS_IRRAGGIUNGIBILE
passo "Atlas risponde: parte il giro (Q2, Q3, Q1; 50 clienti attivi mai letti per fase)"
# 6. il giro, con lo script non modificato
cp prova_infinite.py lavoro/
( cd lavoro && $PY prova_infinite.py "${OPZIONI[@]}" ) > out/terminale.txt 2>&1
rc=$?
passo "script finito con codice $rc"
# 7. esito: le tre fasi di QUESTA esecuzione sono 'fatta'?
esito=$($PY - <<'PY' 2>>out/errori-sali.log || echo NON_SO
import json
d = json.load(open("lavoro/risultati_p1.json"))
gf = d.get("giro_finale") or {}
s = (gf.get("sessioni") or [{}])[-1].get("inizio", "")
ok = all(any(e.get("stato") == "fatta" and e.get("inizio", "") >= s for e in gf.get("fasi", {}).get(f, []))
         for f in ("Q2", "Q3", "Q1"))
print("FINITO" if ok and gf.get("nome") == "irlanda" else "INCOMPLETO")
PY
)
[ "$rc" = 0 ] || esito="${esito}_CODICE_$rc"
fine "$esito"
