#!/bin/bash
# Prova locale M31 · i tre guasti del revisore (C1, C2, C3), simulati con moto e una "macchina" locale.
# Uso: bash prova-guasti.sh [cartella di lavoro]   (0 $: nessuna chiamata ad AWS vero)
# Ogni scenario gira i passi veri del workflow: giornata-ci.sh, poi recupera.sh, pulizia.sh e controllo-finale.sh.
set -u
source "$(dirname "$0")/comune.sh"
BASE=${1:-/tmp/m31-guasti}; mkdir -p "$BASE"; export SANDBOX_BASE=$BASE
FALLITE=0
# un "minuto" dell'attesa dura 2 secondi: 15 minuti di C2 = 30 s
export MINUTO_S=2 MUTA_MIN=15 TETTO_MIN=40 M31_ATTESA_SCARICO=1
export ACTIONS_ID_TOKEN_REQUEST_URL="http://127.0.0.1:9/token?x=1" ACTIONS_ID_TOKEN_REQUEST_TOKEN=finto  # il rinnovo fallisce sempre qui
FINTO_ESEGUI=$BASE/esegui-finto.sh

stato_moto() { aws ec2 describe-instances --instance-ids "$1" --query 'Reservations[0].Instances[0].State.Name' --output text; }
viva() { kill -0 -- "-$(cat "$SANDBOX/macchina-$1/pid")" 2>/dev/null; }
id_di() { jq -r .id "$M31_OUT/giornata.json"; }
export -f stato_moto viva
dopo() { # recupera e pulizia, come gli ultimi passi del workflow (credenziali fresche)
  AWS_ACCESS_KEY_ID=fresca bash "$G_DIR/recupera.sh" > "$SANDBOX/recupera.log" 2>&1
  AWS_ACCESS_KEY_ID=fresca bash "$G_DIR/pulizia.sh" > "$SANDBOX/pulizia.log" 2>&1
  AWS_ACCESS_KEY_ID=fresca bash "$G_DIR/controllo-finale.sh" > "$SANDBOX/controllo.log" 2>&1  # ultimo passo (M31 tetto 5 $)
  controlla "controllo finale: RIMASTO: niente" grep -q "^RIMASTO: niente" "$SANDBOX/controllo.log"
}

echo "== C1a · scaricamento fallito: la macchina deve spegnersi subito"
nuova_sandbox "$BASE/c1-scarico"; touch "$SANDBOX/guasto-presign"
printf '#!/bin/bash\nsleep 100000\n' > "$FINTO_ESEGUI"; fai_pacco /bin/true "$FINTO_ESEGUI" "$SANDBOX/pacco.tgz"
t0=$(date +%s); M31_PACCO=$SANDBOX/pacco.tgz bash "$G_DIR/giornata-ci.sh" > "$SANDBOX/giornata.log" 2>&1; t1=$(date +%s)
ID=$(id_di); SD=$SANDBOX/macchina-$ID/shutdown.log
controlla "prima riga dello user-data (dopo #!/bin/bash): spegnimento a 240 minuti" bash -c "sed -n 2p $SANDBOX/ud.* | grep -q '^shutdown -h +240'"
controlla "spegnimento +240 chiesto per primo" bash -c "head -1 '$SD' | grep -q 'shutdown -h +240'"
controlla "poi spegnimento subito per scaricamento fallito" bash -c "sed -n 2p '$SD' | grep -q 'now M31: scaricamento fallito'"
controlla "la macchina è cancellata (moto: terminated)" bash -c "[ \"\$(stato_moto $ID)\" = terminated ]"
controlla "il workflow lo vede: esito MACCHINA_SPENTA" bash -c "jq -e '.esito==\"MACCHINA_SPENTA\"' '$M31_OUT/giornata.json' >/dev/null"
dopo; controlla "pulizia: RIMASTO: niente" grep -q "RIMASTO: niente" "$SANDBOX/pulizia.log"
echo "   (attesa del workflow: $((t1 - t0)) s; macchina: $(tr '\n' '|' < "$SD"))"

echo "== C1b · tar fallito (pacco rotto): la macchina deve spegnersi subito"
nuova_sandbox "$BASE/c1-tar"; head -c 5000 /dev/urandom > "$SANDBOX/pacco.tgz"
M31_PACCO=$SANDBOX/pacco.tgz bash "$G_DIR/giornata-ci.sh" > "$SANDBOX/giornata.log" 2>&1
ID=$(id_di); SD=$SANDBOX/macchina-$ID/shutdown.log
controlla "spegnimento +240 chiesto per primo" bash -c "head -1 '$SD' | grep -q 'shutdown -h +240'"
controlla "poi spegnimento subito per tar fallito" bash -c "sed -n 2p '$SD' | grep -q 'now M31: tar fallito'"
controlla "la macchina è cancellata (moto: terminated)" bash -c "[ \"\$(stato_moto $ID)\" = terminated ]"
dopo; controlla "pulizia: RIMASTO: niente" grep -q "RIMASTO: niente" "$SANDBOX/pulizia.log"

echo "== C2 · macchina muta (parte ma non scrive mai passi.log): il workflow smette di aspettare dopo 15 minuti"
nuova_sandbox "$BASE/c2-muta"; printf '#!/bin/bash\nsleep 100000\n' > "$FINTO_ESEGUI"; fai_pacco /bin/true "$FINTO_ESEGUI" "$SANDBOX/pacco.tgz"
t0=$(date +%s); M31_PACCO=$SANDBOX/pacco.tgz bash "$G_DIR/giornata-ci.sh" > "$SANDBOX/giornata.log" 2>&1; t1=$(date +%s)
ID=$(id_di)
controlla "riga MACCHINA MUTA nel log" grep -q "MACCHINA MUTA" "$SANDBOX/giornata.log"
controlla "esito MACCHINA_MUTA" bash -c "jq -e '.esito==\"MACCHINA_MUTA\"' '$M31_OUT/giornata.json' >/dev/null"
controlla "uscita a 15 minuti, non a 40 (tetto della prova)" bash -c "[ $((t1 - t0)) -lt $((25 * MINUTO_S)) ]"
controlla "prima della pulizia la macchina è ancora accesa" viva "$ID"
dopo
controlla "dopo la pulizia la macchina è cancellata (moto)" bash -c "[ \"\$(stato_moto $ID)\" = terminated ]"
controlla "dopo la pulizia i suoi processi sono finiti" bash -c "! viva $ID"
controlla "pulizia: RIMASTO: niente" grep -q "RIMASTO: niente" "$SANDBOX/pulizia.log"
echo "   (attesa del workflow: $((t1 - t0)) s = $(( (t1 - t0) / MINUTO_S )) minuti finti)"

echo "== C3 · rinnovo delle credenziali fallito: i risultati devono restare"
nuova_sandbox "$BASE/c3-credenziali"
cat > "$FINTO_ESEGUI" <<'EOF'
#!/bin/bash
# macchina finta: scrive passi.log e un risultato, lavora un po', poi fine.json e si spegne
B=$1; echo "$(date -u +%T) inizio" > passi.log; echo '{"q":"Q1","p50":30}' > tempi.jsonl
aws s3 cp passi.log "s3://$B/risultati/passi.log" >/dev/null; aws s3 cp tempi.jsonl "s3://$B/risultati/tempi.jsonl" >/dev/null
sleep 20; echo "$(date -u +%T) fine" >> passi.log; echo '{"fine": true}' > fine.json
aws s3 cp passi.log "s3://$B/risultati/passi.log" >/dev/null; aws s3 cp fine.json "s3://$B/risultati/fine.json" >/dev/null
shutdown -h now
EOF
fai_pacco /bin/true "$FINTO_ESEGUI" "$SANDBOX/pacco.tgz"
( sleep 8; touch "$SANDBOX/scadute" ) &   # dopo 8 s le credenziali del workflow scadono (il rinnovo fallisce)
AWS_ACCESS_KEY_ID=vecchia RINNOVO_S=3 TETTO_MIN=30 M31_PACCO=$SANDBOX/pacco.tgz bash "$G_DIR/giornata-ci.sh" > "$SANDBOX/giornata.log" 2>&1
controlla "il rinnovo è fallito (come nel caso del revisore)" grep -q "rinnovo delle credenziali fallito" "$SANDBOX/giornata.log"
controlla "la copia di fine giornata è fallita: niente risultati in mano" bash -c "[ ! -f '$M31_OUT/tempi.jsonl' ]"
dopo
controlla "recupera.sh con credenziali fresche: tempi.jsonl c'è" test -f "$M31_OUT/tempi.jsonl"
controlla "recupera.sh: fine.json e passi.log ci sono" bash -c "[ -f '$M31_OUT/fine.json' ] && grep -q fine '$M31_OUT/passi.log'"
controlla "poi la pulizia ha cancellato il bucket" bash -c "! AWS_ACCESS_KEY_ID=fresca aws s3api head-bucket --bucket $BUCKET 2>/dev/null"
controlla "e i risultati restano in mano" test -s "$M31_OUT/tempi.jsonl"
controlla "pulizia: RIMASTO: niente" grep -q "RIMASTO: niente" "$SANDBOX/pulizia.log"
echo "   (log del recupero: $(cat "$SANDBOX/recupera.log" | tr '\n' ' '))"

echo "== RISULTATO: $FALLITE controlli falliti"
[ "$FALLITE" = 0 ]
