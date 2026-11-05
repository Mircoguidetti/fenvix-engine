#!/bin/bash
# P1 · giro dall'Irlanda, versione 2: la parte che gira SULLA MACCHINA (EC2 c7i.8xlarge, Ubuntu 24.04, eu-west-1,
# accanto ad Atlas). La lancia lo user-data scritto da ci/giro-ci.sh. Niente chiavi AWS: S3 con il ruolo della macchina (IMDS).
# Ordine: stringa di connessione dal bucket (letta e cancellata subito) -> file di stato dal bucket -> IP pubblico ->
# attesa che Atlas accetti l'IP (al massimo ATTESA_MIN minuti) -> lo script p1-1.6.2, NON modificato (md5 controllato),
# Q2, Q3, Q1 e parallelo (livelli 10 e 50) sul file di stato VERO (opzioni.txt), con maxConnecting nella stringa di
# connessione (vedi sotto) -> carico_massimo con COPIE copie dello script insieme, ognuna con il suo file di stato
# COPIATO e la sua cartella (massimo.txt): il file di stato vero e il registro dei clienti non li vedono mai -> somma
# delle copie e verdetto (somma_massimo.py, con la CPU della macchina da cpu_macchina.py) -> risultati nel bucket sotto
# risultati/ ogni 30 secondi e alla fine -> la macchina si spegne da sola (spegnimento = cancellazione, impostato al
# lancio). Tetto di tempo: lo user-data programma lo spegnimento (TETTO_MACCHINA_MIN, di serie 120 minuti).
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
read -ra OPZIONI_MASSIMO < massimo.txt
COPIE=$(cat copie.txt 2>/dev/null || echo 20)
# solo la prova locale: opzioni in piu' in fondo (pause corte); sulla macchina vera PROVA_LOCALE non c'e'
EXTRA=(); [ -n "${PROVA_LOCALE:-}" ] && [ -n "${OPZIONI_EXTRA:-}" ] && read -ra EXTRA <<< "$OPZIONI_EXTRA"
EXTRA_M=(); [ -n "${PROVA_LOCALE:-}" ] && [ -n "${OPZIONI_EXTRA_MASSIMO:-}" ] && read -ra EXTRA_M <<< "$OPZIONI_EXTRA_MASSIMO"
mkdir -p out/stato lavoro/dati out/massimo
passo() { echo "$(date -u +%FT%TZ) $*" | tee -a out/passi.log; }
copia_stato() { # il file di stato e il registro dei clienti letti tornano sempre indietro, anche a meta' giro
  [ -f lavoro/risultati_p1.json ] && cp lavoro/risultati_p1.json out/stato/ 2>/dev/null
  [ -f lavoro/risultati_p1.json.usati.log ] && cp lavoro/risultati_p1.json.usati.log out/stato/ 2>/dev/null
  [ -d lavoro/dati ] && cp -r lavoro/dati out/ 2>/dev/null
  for c in massimo/*/; do [ -d "$c" ] || continue; n=$(basename "$c"); mkdir -p "out/massimo/$n"
    cp "$c"terminale.txt "out/massimo/$n/" 2>/dev/null; [ -d "$c"dati ] && cp -r "$c"dati "out/massimo/$n/" 2>/dev/null; done
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
passo "inizio; macchina $(nproc) vCPU; $($PY --version 2>&1); copie per carico_massimo: $COPIE"
$PY cpu_macchina.py out/cpu-macchina.log 5 & CPU_PID=$!   # CPU di tutta la macchina ogni 5 s, per tutto il giro
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
passo "Atlas risponde: parte il giro (Q2, Q3, Q1 da 50 clienti attivi mai letti; parallelo 10 e 50 insieme, 180 clienti)"
# 6. il giro, con lo script non modificato, sul file di stato vero. Nella stringa di connessione si aggiunge
# maxConnecting=MAX_CONNECTING (solo per questo processo): il driver (pymongo) di serie apre al massimo 2 connessioni
# alla volta per server (maxConnecting=2) e intanto riusa quelle che si liberano; cosi' nel riscaldamento del
# parallelo "50 insieme" dal Mac se ne aprivano 24 e "100 insieme" 32. Lo script non passa maxConnecting, quindi vale
# quello della stringa. Non cambia niente per Q2, Q3, Q1 (un filo solo). Le copie del carico massimo NON lo hanno.
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
cp prova_infinite.py lavoro/
( cd lavoro && FENVIX_P1_URI=$URI_GIRO $PY prova_infinite.py "${OPZIONI[@]}" "${EXTRA[@]}" ) > out/terminale.txt 2>&1
rc=$?
unset URI_GIRO
passo "script finito con codice $rc"
cp out/terminale.txt out/terminale-giro.txt 2>/dev/null
# 6b. il parallelo: ogni lettura di ogni lotto aveva la sua connessione pronta? (legge solo i file)
cart=$(ls -d lavoro/dati/giro_finale_irlanda_* 2>/dev/null | tail -1)
[ -n "$cart" ] && $PY controlla_parallelo.py "$cart" > out/parallelo-connessioni.txt 2>&1
passo "connessioni del parallelo: $(tail -1 out/parallelo-connessioni.txt 2>/dev/null || echo 'NON SO')"
# 7. carico massimo con COPIE copie dello script insieme. Ognuna: cartella massimo/NN, COPIA del file di stato (dopo
# il giro) e un registro vuoto suo, nome del giro irlanda_massimo_NN, cartella dei dati sua. Il carico massimo legge a
# caldo clienti GIA' letti e scrive in una collezione a parte: non consuma clienti mai letti. Il file di stato vero
# (lavoro/risultati_p1.json) e il suo registro non si toccano piu' da qui.
copia_stato
md5_vero=$(md5sum lavoro/risultati_p1.json lavoro/risultati_p1.json.usati.log 2>/dev/null)
for k in $(seq -w 1 "$COPIE"); do
  mkdir -p "massimo/$k/dati"
  cp lavoro/risultati_p1.json "massimo/$k/stato.json"; : > "massimo/$k/stato.json.usati.log"
  cp prova_infinite.py "massimo/$k/"
done
passo "carico_massimo: parto con $COPIE copie insieme ($(nproc) vCPU)"
pids=()
for k in $(seq -w 1 "$COPIE"); do
  ( cd "massimo/$k" && exec $PY prova_infinite.py "${OPZIONI_MASSIMO[@]}" --file stato.json --cartella-dati dati \
      --giro "irlanda_massimo_$k" "${EXTRA_M[@]}" ) > "massimo/$k/terminale.txt" 2>&1 &
  pids+=($!)
done
rcm=0; for p in "${pids[@]}"; do wait "$p" || rcm=$((rcm + 1)); done
passo "carico_massimo finito: copie con codice diverso da 0: $rcm su $COPIE"
[ "$md5_vero" = "$(md5sum lavoro/risultati_p1.json lavoro/risultati_p1.json.usati.log 2>/dev/null)" ] \
  || passo "ATTENZIONE: il file di stato vero e' cambiato durante il carico massimo (non dovrebbe)"
copia_stato
$PY somma_massimo.py massimo out/cpu-macchina.log > out/massimo-somma.txt 2>&1
cp massimo/somma.json out/massimo-somma.json 2>/dev/null
passo "carico_massimo: $(grep -c VERDETTO out/massimo-somma.txt 2>/dev/null) parti sommate (out/massimo-somma.txt)"
# 7. esito: le tre fasi di QUESTA esecuzione sono 'fatta'?
esito=$($PY - <<'PY' 2>>out/errori-sali.log || echo NON_SO
import json
d = json.load(open("lavoro/risultati_p1.json"))
gf = d.get("giro_finale") or {}
s = (gf.get("sessioni") or [{}])[-1].get("inizio", "")
ok = all(any(e.get("stato") == "fatta" and e.get("inizio", "") >= s for e in gf.get("fasi", {}).get(f, []))
         for f in ("Q2", "Q3", "Q1", "parallelo"))
print("FINITO" if ok and gf.get("nome") == "irlanda" else "INCOMPLETO")
PY
)
[ "$rc" = 0 ] || esito="${esito}_CODICE_$rc"
[ "$rcm" = 0 ] || esito="${esito}_MASSIMO_$rcm"
fine "$esito"
