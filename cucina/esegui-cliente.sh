#!/bin/bash
# Cucina minima (K2) · la parte che gira SULLA MACCHINA CLIENTE (EC2 c7i, Ubuntu 24.04, eu-west-1, nella stessa zona
# del server). La lancia lo user-data scritto da ci/giro-ci.sh. Niente chiavi AWS: S3 con il ruolo della macchina.
# Stesso metodo del giro dall'Irlanda su Atlas (irlanda/esegui.sh di code-irlanda), con il nostro server al posto di
# Atlas e piu' taglie di server di fila sugli stessi dati:
#   password dal bucket -> per ogni taglia K (il runner accende il server K e scrive server-K/via.json; il server scrive
#   server-K/pronto.json): stringa di connessione (TLS con la CA della giornata, utente root, replicaSet=rs0) ->
#   attesa del ping -> SOLO alla prima taglia: il caricamento con lo script (carica.txt, come su Atlas: carica --gb 10)
#   -> il giro di prima spostato sotto un'altra chiave (prepara_stato.py) -> piano a secco con lo script -> il giro con
#   lo script p1-1.6.2 NON modificato (md5 controllato) e le opzioni di opzioni.txt (quelle dell'Irlanda), con
#   maxConnecting nella stringa -> controllo delle connessioni del parallelo -> carico_massimo con COPIE copie dello
#   script insieme (massimo.txt, file di stato COPIATI) -> somma e verdetto -> "fatto-K" nel bucket -> taglia dopo.
# Quando il runner scrive "basta" (o non arriva nessuna taglia per ATTESA_TAGLIA_MIN minuti): risultati nel bucket e la
# macchina si spegne da sola (spegnimento = cancellazione).
# Uso: esegui-cliente.sh BUCKET   (COPIE e GB da cliente.env, scritto dal runner nel pacco; nessun segreto)
set -u
cd "$(dirname "$0")" || exit 1
BUCKET=$1
source ./cliente.env   # COPIE, GB
export AWS_DEFAULT_REGION=eu-west-1 AWS_REGION=eu-west-1
export PYTHONPATH="$PWD/vendor" PYTHONUNBUFFERED=1   # righe dello script subito nel log (lo script non cambia)
PY=${PY:-python3}
MD5_SCRIPT=8396ca455557baaa4e3a9b1f881451f9
ATTESA_TAGLIA_MIN=${ATTESA_TAGLIA_MIN:-30}
ATTESA_DB_MIN=${ATTESA_DB_MIN:-15}
SALI_S=${SALI_S:-30}
read -ra OPZIONI < opzioni.txt
read -ra OPZIONI_MASSIMO < massimo.txt
read -ra OPZIONI_CARICA < carica.txt
# --gb dal comando della giornata (di serie 10, come su Atlas); l'ultimo --gb vince (argparse)
[ -n "${GB:-}" ] && OPZIONI_CARICA+=(--gb "$GB")
# solo la prova locale: opzioni in piu' in fondo (dati piccoli, pause corte); sulla macchina vera PROVA_LOCALE non c'e'
EXTRA=(); [ -n "${PROVA_LOCALE:-}" ] && [ -n "${OPZIONI_EXTRA:-}" ] && read -ra EXTRA <<< "$OPZIONI_EXTRA"
EXTRA_M=(); [ -n "${PROVA_LOCALE:-}" ] && [ -n "${OPZIONI_EXTRA_MASSIMO:-}" ] && read -ra EXTRA_M <<< "$OPZIONI_EXTRA_MASSIMO"
EXTRA_C=(); [ -n "${PROVA_LOCALE:-}" ] && [ -n "${OPZIONI_EXTRA_CARICA:-}" ] && read -ra EXTRA_C <<< "$OPZIONI_EXTRA_CARICA"
mkdir -p out/stato lavoro/dati
passo() { echo "$(date -u +%FT%TZ) $*" | tee -a out/passi.log; }
copia_stato() { # il file di stato e il registro dei clienti letti tornano sempre indietro, anche a meta' giro
  [ -f lavoro/risultati_p1.json ] && cp lavoro/risultati_p1.json out/stato/ 2>/dev/null
  [ -f lavoro/risultati_p1.json.usati.log ] && cp lavoro/risultati_p1.json.usati.log out/stato/ 2>/dev/null
  [ -d lavoro/dati ] && cp -r lavoro/dati out/ 2>/dev/null
  for c in massimo-*/*/; do [ -d "$c" ] || continue; mkdir -p "out/$c"
    cp "$c"terminale.txt "out/$c" 2>/dev/null; [ -d "$c"dati ] && cp -r "$c"dati "out/$c" 2>/dev/null; done
  return 0
}
sali() { copia_stato; $PY s3io.py sali "$BUCKET" out risultati/cliente/ 2>> out/errori-sali.log; }
( while sleep "$SALI_S"; do sali; done ) &
SALITORE=$!
fine() { # fine ESITO
  passo "fine: $1"
  kill $SALITORE 2>/dev/null
  [ -n "${CPU_PID:-}" ] && kill "$CPU_PID" 2>/dev/null
  copia_stato
  printf '{"fine": true, "esito": "%s"}\n' "$1" > out/fine.json
  sali; sali
  $PY s3io.py metti "$BUCKET" cliente/fine.json out/fine.json 2>/dev/null
  [ -n "${PROVA_LOCALE:-}" ] || shutdown -h now "cucina: cliente finito"
  exit 0
}
passo "inizio cliente; macchina $(nproc) vCPU; $($PY --version 2>&1); copie per carico_massimo: $COPIE; GB da caricare: ${GB:-come carica.txt}"
$PY cpu_macchina.py out/cpu-macchina.log 5 & CPU_PID=$!   # CPU di tutta la macchina ogni 5 s, per tutta la giornata
# 1. lo script e' quello del fondatore, byte per byte
md5=$(md5sum prova_infinite.py | cut -d' ' -f1)
[ "$md5" = "$MD5_SCRIPT" ] || { passo "ERRORE: md5 dello script $md5, atteso $MD5_SCRIPT"; fine SCRIPT_DIVERSO; }
cp prova_infinite.py lavoro/
# 2. la password: resta nella memoria dei processi
CM_PW=$($PY s3io.py testo "$BUCKET" segreto/pw) && [ -n "$CM_PW" ] || { passo "ERRORE: password non letta"; fine SENZA_PASSWORD; }
passo "password letta (non scritta da nessuna parte)"
esiti=()
K=0
while :; do
  K=$((K + 1)); P=server-$K
  # 3. aspetto la taglia K (o "basta")
  passo "aspetto la taglia $K (al massimo $ATTESA_TAGLIA_MIN minuti)"
  t0=$(date +%s); via=""
  while :; do
    $PY s3io.py c-e "$BUCKET" cliente/basta && break
    via=$($PY s3io.py testo "$BUCKET" "$P/via.json" 2>/dev/null) && [ -n "$via" ] && break
    [ $(( $(date +%s) - t0 )) -ge $(( ATTESA_TAGLIA_MIN * 60 )) ] && break
    sleep 10
  done
  [ -z "$via" ] && break
  TIPO=$($PY -c "import json,sys; print(json.loads(sys.argv[1])['tipo'])" "$via")
  NOME="cm_${K}_${TIPO//./_}"
  mkdir -p "out/taglia-$K"
  passo "taglia $K: server $TIPO, giro $NOME; aspetto che il server sia pronto"
  t0=$(date +%s); pronto=""
  while :; do
    pronto=$($PY s3io.py testo "$BUCKET" "$P/pronto.json" 2>/dev/null) && [ -n "$pronto" ] && break
    $PY s3io.py c-e "$BUCKET" "$P/fermato" && break
    [ $(( $(date +%s) - t0 )) -ge $(( ATTESA_DB_MIN * 60 )) ] && break
    sleep 10
  done
  if [ -z "$pronto" ]; then passo "taglia $K: il server non e' mai stato pronto"; esiti+=("$K:SERVER_NON_PRONTO")
    echo '{"esito": "SERVER_NON_PRONTO"}' > "out/taglia-$K/fatto.json"; $PY s3io.py metti "$BUCKET" "cliente/fatto-$K.json" "out/taglia-$K/fatto.json"; continue; fi
  IP=$($PY -c "import json,sys; print(json.loads(sys.argv[1])['ip'])" "$pronto")
  PORTA=$($PY -c "import json,sys; print(json.loads(sys.argv[1])['porta'])" "$pronto")
  # stringa di connessione: TLS con la CA della giornata (certificato del server per il suo IP privato), utente root.
  # replicaSet=rs0 come su Atlas (il server dice di essere un insieme con un membro solo).
  export FENVIX_P1_URI="mongodb://root:$CM_PW@$IP:$PORTA/?replicaSet=rs0&tls=true&tlsCAFile=$PWD/ca.pem&authSource=admin"
  $PY attendi_db.py "$ATTESA_DB_MIN" >> out/passi.log 2>&1 || { esiti+=("$K:SERVER_IRRAGGIUNGIBILE")
    echo '{"esito": "SERVER_IRRAGGIUNGIBILE"}' > "out/taglia-$K/fatto.json"; $PY s3io.py metti "$BUCKET" "cliente/fatto-$K.json" "out/taglia-$K/fatto.json"; continue; }
  passo "taglia $K: il server risponde"
  # 4. solo la prima volta: il caricamento, con lo script (come su Atlas)
  if [ ! -f lavoro/carica-fatto ]; then
    passo "caricamento: ${OPZIONI_CARICA[*]} ${EXTRA_C[*]} (sul server $TIPO)"
    tc=$(date +%s)
    ( cd lavoro && $PY prova_infinite.py "${OPZIONI_CARICA[@]}" "${EXTRA_C[@]}" ) > out/carica.txt 2>&1
    rc=$?
    passo "caricamento finito con codice $rc in $(( $(date +%s) - tc )) secondi: $(tail -1 out/carica.txt)"
    [ "$rc" = 0 ] || { esiti+=("$K:CARICAMENTO_FALLITO_$rc"); echo "{\"esito\": \"CARICAMENTO_FALLITO_$rc\"}" > "out/taglia-$K/fatto.json"
      $PY s3io.py metti "$BUCKET" "cliente/fatto-$K.json" "out/taglia-$K/fatto.json"; fine CARICAMENTO_FALLITO; }
    echo "$(( $(date +%s) - tc ))" > lavoro/carica-fatto
    cp lavoro/carica-fatto out/carica-secondi.txt
  fi
  # 5. il giro di prima sotto un'altra chiave, poi il piano a secco (lo script stesso, nessuna connessione)
  $PY prepara_stato.py lavoro/risultati_p1.json "$NOME" | tee -a out/passi.log
  ( cd lavoro && $PY prova_infinite.py "${OPZIONI[@]}" --giro "$NOME" --a-secco "${EXTRA[@]}" ) > "out/taglia-$K/piano.txt" 2>&1
  passo "piano: $(grep -E "^Il piano usa" "out/taglia-$K/piano.txt" | head -1)"
  # 6. il giro (Q2, Q3, Q1, parallelo 10 e 50). maxConnecting nella stringa, come dall'Irlanda (il driver di serie apre
  # al massimo 2 connessioni alla volta per server); le copie del carico massimo NON lo hanno.
  URI_GIRO="${FENVIX_P1_URI}&maxConnecting=${MAX_CONNECTING:-200}"
  passo "taglia $K: parte il giro $NOME (Q2, Q3, Q1 da 50 clienti attivi mai letti; parallelo 10 e 50 insieme)"
  ( cd lavoro && FENVIX_P1_URI=$URI_GIRO $PY prova_infinite.py "${OPZIONI[@]}" --giro "$NOME" "${EXTRA[@]}" ) > "out/taglia-$K/terminale-giro.txt" 2>&1
  rc=$?
  unset URI_GIRO
  passo "taglia $K: script finito con codice $rc"
  cart=$(ls -d lavoro/dati/giro_finale_"$NOME"_* 2>/dev/null | tail -1)
  [ -n "$cart" ] && $PY controlla_parallelo.py "$cart" > "out/taglia-$K/parallelo-connessioni.txt" 2>&1
  passo "taglia $K: connessioni del parallelo: $(tail -1 "out/taglia-$K/parallelo-connessioni.txt" 2>/dev/null || echo 'NON SO')"
  # 7. carico massimo con COPIE copie insieme, ognuna con il suo file di stato COPIATO (il vero non si tocca)
  copia_stato
  md5_vero=$(md5sum lavoro/risultati_p1.json lavoro/risultati_p1.json.usati.log 2>/dev/null)
  M=massimo-$K
  for c in $(seq -w 1 "$COPIE"); do
    mkdir -p "$M/$c/dati"
    cp lavoro/risultati_p1.json "$M/$c/stato.json"; : > "$M/$c/stato.json.usati.log"
    cp prova_infinite.py "$M/$c/"
  done
  passo "taglia $K: carico_massimo con $COPIE copie insieme ($(nproc) vCPU)"
  pids=()
  for c in $(seq -w 1 "$COPIE"); do
    ( cd "$M/$c" && exec $PY prova_infinite.py "${OPZIONI_MASSIMO[@]}" --file stato.json --cartella-dati dati \
        --giro "${NOME}_massimo_$c" "${EXTRA_M[@]}" ) > "$M/$c/terminale.txt" 2>&1 &
    pids+=($!)
  done
  rcm=0; for p in "${pids[@]}"; do wait "$p" || rcm=$((rcm + 1)); done
  passo "taglia $K: carico_massimo finito: copie con codice diverso da 0: $rcm su $COPIE"
  [ "$md5_vero" = "$(md5sum lavoro/risultati_p1.json lavoro/risultati_p1.json.usati.log 2>/dev/null)" ] \
    || passo "ATTENZIONE: il file di stato vero e' cambiato durante il carico massimo (non dovrebbe)"
  $PY somma_massimo.py "$M" out/cpu-macchina.log > "out/taglia-$K/massimo-somma.txt" 2>&1
  cp "$M/somma.json" "out/taglia-$K/massimo-somma.json" 2>/dev/null
  # 8. esito della taglia: le quattro fasi di QUESTO giro sono 'fatta'?
  esito=$($PY - "$NOME" <<'PY' 2>>out/errori-sali.log || echo NON_SO
import json, sys
d = json.load(open("lavoro/risultati_p1.json"))
gf = d.get("giro_finale") or {}
s = (gf.get("sessioni") or [{}])[-1].get("inizio", "")
ok = all(any(e.get("stato") == "fatta" and e.get("inizio", "") >= s for e in gf.get("fasi", {}).get(f, []))
         for f in ("Q2", "Q3", "Q1", "parallelo"))
print("FINITO" if ok and gf.get("nome") == sys.argv[1] else "INCOMPLETO")
PY
)
  [ "$rc" = 0 ] || esito="${esito}_CODICE_$rc"
  [ "$rcm" = 0 ] || esito="${esito}_MASSIMO_$rcm"
  esiti+=("$K:$esito")
  printf '{"esito": "%s", "tipo": "%s", "giro": "%s"}\n' "$esito" "$TIPO" "$NOME" > "out/taglia-$K/fatto.json"
  passo "taglia $K ($TIPO): $esito"
  sali
  $PY s3io.py metti "$BUCKET" "cliente/fatto-$K.json" "out/taglia-$K/fatto.json"
done
passo "esiti delle taglie: ${esiti[*]:-nessuna}"
tutti=FINITO
[ ${#esiti[@]} -gt 0 ] || tutti=NESSUNA_TAGLIA
for e in "${esiti[@]}"; do [ "${e#*:}" = FINITO ] || tutti=INCOMPLETO; done
fine "$tutti"
