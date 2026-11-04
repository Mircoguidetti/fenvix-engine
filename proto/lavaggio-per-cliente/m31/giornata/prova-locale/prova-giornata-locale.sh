#!/bin/bash
# Prova locale M31 · tutta la giornata dall'inizio alla fine, in piccolo, con moto al posto di AWS e il ritardo finto
# delle GET. Stessi passi e stessi script del workflow m31-freddo-s3.yml, nello stesso ordine:
#   permessi.sh -> pacco del programma -> giornata-ci.sh (bucket, macchina = user-data qui, attesa) -> recupera.sh ->
#   pulizia.sh -> copia dei risultati senza l'account.
# Uso: bash prova-giornata-locale.sh [cartella di lavoro]   (0 $: nessuna chiamata ad AWS vero)
set -u
source "$(dirname "$0")/comune.sh"
BASE=${1:-/tmp/m31-giornata-locale}; mkdir -p "$BASE"; export SANDBOX_BASE=$BASE
nuova_sandbox "$BASE/giornata"
# taglie piccole (la macchina le eredita dall'ambiente, come farebbero i valori di serie di esegui.sh)
export GB_P1=${GB_P1:-0.3} CLIENTI_P1=${CLIENTI_P1:-300} GB_D2K=${GB_D2K:-0.1} CLIENTI_D2K=${CLIENTI_D2K:-100} RITMO=${RITMO:-20}
export N=${N:-20} RISCALDO=${RISCALDO:-40} INSIEME=${INSIEME:-10} Q5_N_TUTTE=${Q5_N_TUTTE:-1}
export N_D2K=${N_D2K:-10} RISCALDO_D2K=${RISCALDO_D2K:-20} SECONDI_SCRIVI=${SECONDI_SCRIVI:-5} ATTESA_LAVAGGIO=${ATTESA_LAVAGGIO:-5}
export FINTO=${FINTO:-25:0.6:90} SALI_S=${SALI_S:-10}
# attesa del workflow: un "minuto" = 5 s
export MINUTO_S=${MINUTO_S:-5} MUTA_MIN=15 TETTO_MIN=${TETTO_MIN:-400}
export ACTIONS_ID_TOKEN_REQUEST_URL="http://127.0.0.1:9/token?x=1" ACTIONS_ID_TOKEN_REQUEST_TOKEN=finto RINNOVO_S=100000
export GITHUB_RUN_ID=locale

echo "1. permessi.sh"; bash "$G_DIR/permessi.sh" > "$SANDBOX/permessi.log" 2>&1; echo "   codice $? ($(tail -1 "$SANDBOX/permessi.log"))"
echo "2. pacco (come il workflow: programma già costruito con --locked, strip)"
mkdir -p "$SANDBOX/pacco"; cp "$M_DIR/target/release/m31" "$G_DIR/esegui.sh" "$SANDBOX/pacco/"; strip "$SANDBOX/pacco/m31"
tar czf "$SANDBOX/m31-giornata.tar.gz" -C "$SANDBOX/pacco" m31 esegui.sh
echo "3. giornata-ci.sh"; t0=$(date +%s)
M31_PACCO=$SANDBOX/m31-giornata.tar.gz bash "$G_DIR/giornata-ci.sh" > "$SANDBOX/giornata.log" 2>&1; rc=$?
echo "   codice $rc, $(( $(date +%s) - t0 )) s; $(cat "$M31_OUT/giornata.json")"
echo "4. recupera.sh"; bash "$G_DIR/recupera.sh" 2>&1 | sed 's/^/   /'
echo "5. pulizia.sh"; bash "$G_DIR/pulizia.sh" 2>&1 | sed 's/^/   /'
echo "6. risultati senza l'account"
R=$BASE/risultati; rm -rf "$R"; mkdir -p "$R"; cp -r "$M31_OUT/." "$R/"
grep -rl "$M31_ACCT" "$R" | xargs -r sed -i "s/$M31_ACCT/<ACCOUNT_ID>/g"
! grep -rq "$M31_ACCT" "$R" && echo "   nessun numero dell'account" || echo "   ACCOUNT ANCORA NEI RISULTATI"
echo "   passi:"; sed 's/^/     /' "$R/passi.log"
