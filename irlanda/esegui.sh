#!/bin/bash
# P1 · giro dall'Irlanda, versione 3 (analisi del 2026-11-05): la parte che gira SULLA MACCHINA (EC2 in eu-west-1,
# Ubuntu 24.04, accanto ad Atlas). La lancia lo user-data scritto da ci/giro-ci.sh. Niente chiavi AWS: S3 con il ruolo
# della macchina (IMDS). Ordine: stringa di connessione dal bucket (letta e cancellata subito) -> tutti i file di stato
# (stati.tar.gz) -> IP pubblico -> attesa che Atlas accetti l'IP -> programma.py con passi.txt (lo script p1-1.6.2 NON
# modificato, md5 controllato; nel parallelo maxConnecting nella stringa) -> file di stato e risultati nel bucket sotto
# risultati/ ogni 30 secondi e alla fine -> la macchina si spegne da sola (spegnimento = cancellazione). Tetto di
# tempo: lo user-data programma lo spegnimento (TETTO_MACCHINA_MIN).
set -u
cd "$(dirname "$0")" || exit 1
BUCKET=$1
export AWS_DEFAULT_REGION=eu-west-1 AWS_REGION=eu-west-1
export PYTHONPATH="$PWD/vendor" PYTHONUNBUFFERED=1   # righe dello script subito nel log (lo script non cambia)
PY=${PY:-python3}
MD5_SCRIPT=8396ca455557baaa4e3a9b1f881451f9
ATTESA_MIN=${ATTESA_MIN:-30}
SALI_S=${SALI_S:-30}
# il programma del giro: passi.txt (copiato dal workflow da programmi/NOME/passi.txt, controllato a secco prima di spendere)
mkdir -p out/stati lavoro/stati lavoro/dati
passo() { echo "$(date -u +%FT%TZ) $*" | tee -a out/passi.log; }
copia_stato() { # i file di stato e i registri dei clienti letti tornano sempre indietro, anche a meta' giro
  for s in lavoro/stati/*/; do [ -d "$s" ] || continue; n=$(basename "$s"); mkdir -p "out/stati/$n"
    for f in risultati_p1.json risultati_p1.json.usati.log; do [ -f "$s$f" ] && cp "$s$f" "out/stati/$n/" 2>/dev/null; done; done
  [ -d lavoro/dati ] && cp -r lavoro/dati out/ 2>/dev/null
  [ -d lavoro/dormienti ] && for c in lavoro/dormienti/*/; do [ -d "$c" ] || continue; mkdir -p "out/dormienti/$(basename "$c")"
    cp "$c"risultati_p1.json "$c"risultati_p1.json.usati.log "out/dormienti/$(basename "$c")/" 2>/dev/null; done
  # il terminale del passo in corso, per la riga che il runner stampa ogni minuto
  t=$(ls -t out/terminali/*.txt 2>/dev/null | head -1); [ -n "$t" ] && cp "$t" out/terminale.txt 2>/dev/null
  return 0
}
sali() { copia_stato; $PY s3io.py sali "$BUCKET" out risultati/ 2>> out/errori-sali.log; }
( while sleep "$SALI_S"; do sali; done ) &
SALITORE=$!
fine() { # fine ESITO
  passo "fine: $1"
  kill $SALITORE 2>/dev/null
  [ -n "${CPU_PID:-}" ] && kill "$CPU_PID" 2>/dev/null
  copia_stato
  printf '{"fine": true, "esito": "%s"}\n' "$1" > out/fine.json
  sali; sali
  [ -n "${PROVA_LOCALE:-}" ] || shutdown -h now "irlanda: fine"
  exit 0
}
passo "inizio; macchina $(nproc) vCPU; $($PY --version 2>&1); programma $(cat programma.txt 2>/dev/null)"
$PY cpu_macchina.py out/cpu-macchina.log 5 & CPU_PID=$!   # CPU di tutta la macchina ogni 5 s, per tutto il giro
# 1. lo script e' quello del fondatore, byte per byte
md5=$(md5sum prova_infinite.py | cut -d' ' -f1)
[ "$md5" = "$MD5_SCRIPT" ] || { passo "ERRORE: md5 dello script $md5, atteso $MD5_SCRIPT"; fine SCRIPT_DIVERSO; }
# 2. la stringa di connessione: dal bucket, poi l'oggetto si cancella subito; resta solo nella memoria del processo
FENVIX_P1_URI=$($PY s3io.py prendi-e-cancella "$BUCKET" segreto/uri) || { passo "ERRORE: stringa di connessione non letta"; fine SENZA_URI; }
[ -n "$FENVIX_P1_URI" ] || { passo "ERRORE: stringa di connessione vuota"; fine SENZA_URI; }
export FENVIX_P1_URI
passo "stringa di connessione letta (non scritta da nessuna parte) e cancellata dal bucket"
# 3. i file di stato (tutti, uno per database: cartella irlanda/stati del branch, passati dal workflow)
$PY s3io.py scarica "$BUCKET" stato/stati.tar.gz lavoro/stati.tar.gz || { passo "ERRORE: file di stato mancanti"; fine SENZA_STATO; }
tar xzf lavoro/stati.tar.gz -C lavoro/stati || { passo "ERRORE: file di stato illeggibili"; fine SENZA_STATO; }
( cd lavoro/stati && find . -type f | sort | xargs -r md5sum ) > out/stato-in-ingresso.md5
passo "file di stato arrivati: $(wc -l < out/stato-in-ingresso.md5) file ($(ls lavoro/stati | tr '\n' ' '))"
# 4. l'IP pubblico della macchina: e' quello da mettere nella lista di accesso di Atlas
tok=$(curl -fsS -m 5 -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 600" 2>/dev/null)
IP=$(curl -fsS -m 5 -H "X-aws-ec2-metadata-token: $tok" http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "${IP_FINTO:-?}")
echo "$IP" > out/ip.txt
passo "IP pubblico della macchina: $IP (da aggiungere alla lista di accesso di Atlas, come $IP/32)"
# 5. attesa: Atlas risponde solo quando l'IP e' nella lista di accesso
$PY attendi_db.py "$ATTESA_MIN" >> out/passi.log 2>&1 || fine ATLAS_IRRAGGIUNGIBILE
passo "Atlas risponde: parte il programma $(cat programma.txt 2>/dev/null)"
URI_GIRO=$($PY - "${MAX_CONNECTING:-200}" <<'PY'
import os, sys
u = os.environ["FENVIX_P1_URI"]
if "maxconnecting=" in u.lower():
    print(u)
else:
    base, q, opz = u.partition("?")
    if not q:
        dopo = base.split("@", 1)[-1].split("://", 1)[-1]
        base += "" if "/" in dopo else "/"
    print(base + "?" + (opz + "&" if opz else "") + "maxConnecting=" + sys.argv[1])
PY
) || { passo "ERRORE: maxConnecting non aggiunto alla stringa"; fine SENZA_URI; }
export URI_GIRO
$PY programma.py passi.txt > out/programma.txt 2>&1
rc=$?
unset URI_GIRO
passo "programma finito con codice $rc"
copia_stato
esito=FINITO
[ "$rc" = 0 ] || esito="INCOMPLETO_CODICE_$rc"
grep -q "ATTENZIONE: almeno una fase NON VALIDA" out/validita-macchina.txt 2>/dev/null && esito="${esito}_MACCHINA_LIMITE"
fine "$esito"
