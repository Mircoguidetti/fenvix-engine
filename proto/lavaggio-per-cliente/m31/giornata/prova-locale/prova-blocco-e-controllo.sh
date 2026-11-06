#!/bin/bash
# Prova locale M31 · blocco a 5 $ e controllo finale (regola del fondatore 2026-11-04), con moto e una "macchina" locale.
# Uso: bash prova-blocco-e-controllo.sh [cartella di lavoro]   (0 $: nessuna chiamata ad AWS vero)
# Ogni scenario gira i passi veri del workflow nello stesso ordine: giornata-ci.sh, recupera.sh, pulizia.sh,
# controllo-finale.sh.
#   (a)  spesa finta che passa 5 $ dopo pochi minuti finti: BLOCCO, pulizia, "RIMASTO: niente";
#   (a2) come (a) con una seconda macchina col tag rimasta da prima: la stima la conta due volte;
#   (b)  pulizia che lascia di proposito un bucket e una macchina (più un Elastic IP e un disco non attaccato):
#        il controllo finale scrive ALLARME e il job fallisce;
#   (b2) (b3) letture che non riescono o risposte rotte: ALLARME con NON SO, mai "niente".
# La spesa finta: PREZZO_ORA=60 (1 $ al minuto-macchina) al posto di 0,1915; tetto di serie (TETTO_DOLLARI non impostato).
set -u
source "$(dirname "$0")/comune.sh"
BASE=${1:-/tmp/m31-blocco}; mkdir -p "$BASE"; export SANDBOX_BASE=$BASE
FALLITE=0
# un "minuto" dura 2 s; in (a) e (a2) 5 s, perché il lancio della macchina finta richiede circa 8 s veri
export MINUTO_S=2 MUTA_MIN=15 TETTO_MIN=40 M31_ATTESA_SCARICO=1
export ACTIONS_ID_TOKEN_REQUEST_URL="http://127.0.0.1:9/token?x=1" ACTIONS_ID_TOKEN_REQUEST_TOKEN=finto RINNOVO_S=100000
unset TETTO_DOLLARI
FINTO_ESEGUI=$BASE/esegui-finto.sh
WF=$G_DIR/../../../../.github/workflows/m31-freddo-s3.yml

stato_moto() { aws ec2 describe-instances --instance-ids "$1" --query 'Reservations[0].Instances[0].State.Name' --output text; }
id_di() { jq -r .id "$M31_OUT/giornata.json"; }
export -f stato_moto
dopo() { # gli ultimi passi del workflow, come in m31-freddo-s3.yml: recupero, pulizia, controllo finale
  bash "$G_DIR/recupera.sh" > "$SANDBOX/recupera.log" 2>&1
  bash "$G_DIR/pulizia.sh" > "$SANDBOX/pulizia.log" 2>&1; RC_PULIZIA=$?
  : > "$SANDBOX/summary-finale.md"
  GITHUB_STEP_SUMMARY=$SANDBOX/summary-finale.md bash "$G_DIR/controllo-finale.sh" > "$SANDBOX/controllo.log" 2>&1; RC_CONTROLLO=$?
}
# macchina finta che lavora per sempre e scrive passi.log ogni 2 s (non è muta, non finisce mai: la ferma solo il blocco)
cat > "$FINTO_ESEGUI" <<'EOF'
#!/bin/bash
B=$1; i=0
while :; do i=$((i+1)); echo "$(date -u +%T) passo $i" >> passi.log; aws s3 cp passi.log "s3://$B/risultati/passi.log" >/dev/null; sleep 2; done
EOF

echo "== (a) spesa finta oltre 5 \$ dopo pochi minuti finti: BLOCCO, pulizia, RIMASTO: niente"
nuova_sandbox "$BASE/a-blocco"; fai_pacco /bin/true "$FINTO_ESEGUI" "$SANDBOX/pacco.tgz"
export M31_INIZIO=$(date -u +%FT%TZ)
t0=$(date +%s); MINUTO_S=5 PREZZO_ORA=60 M31_PACCO=$SANDBOX/pacco.tgz bash "$G_DIR/giornata-ci.sh" > "$SANDBOX/giornata.log" 2>&1; rc=$?; t1=$(date +%s)
ID=$(id_di)
controlla "ogni riga SPESA dice 'tetto 5,00 \$' (tetto di serie)" bash -c "grep '^SPESA' '$SANDBOX/giornata.log' | grep -v -q 'tetto 5,00 \\\$' && exit 1; grep -q '^SPESA.*tetto 5,00 \\\$' '$SANDBOX/giornata.log'"
controlla "nessuna riga dice più 'tetto 1,50'" bash -c "! grep -q 'tetto 1,50' '$SANDBOX/giornata.log'"
controlla "riga 'BLOCCO: spesa stimata X \$ ≥ 5,00 \$'" grep -Eq '^BLOCCO: spesa stimata [0-9]+,[0-9]{2} \$ ≥ 5,00 \$' "$SANDBOX/giornata.log"
controlla "esito BLOCCO_SPESA in giornata.json" bash -c "jq -e '.esito==\"BLOCCO_SPESA\"' '$M31_OUT/giornata.json' >/dev/null"
controlla "il blocco arriva dopo pochi minuti finti (meno di 8), non al tetto di tempo (40)" bash -c "[ $(( (t1 - t0) / 5 )) -lt 8 ]"
controlla "la SPESA del blocco è almeno 5 e quella del giro prima sotto 5" bash -c "
  grep -B2 '^BLOCCO' '$SANDBOX/giornata.log' | grep '^SPESA' | tail -1 | grep -Eq '^SPESA: ([5-9]|[1-9][0-9]),' &&
  n=\$(grep -n '^BLOCCO' '$SANDBOX/giornata.log' | cut -d: -f1); head -n \$((n - 2)) '$SANDBOX/giornata.log' | grep '^SPESA' | tail -1 | grep -Eq '^SPESA: [0-4],'"
controlla "la macchina è cancellata subito dal blocco, prima della pulizia" bash -c "[ \"\$(stato_moto $ID)\" = terminated ]"
# per eccesso: la parte macchina della stima al momento del blocco >= minuti veri di vita della macchina × prezzo
lancio=$(stat -c %Y "$SANDBOX/macchina-$ID/pid"); fine_m=$(stat -c %Y "$SANDBOX/macchina-$ID/stato.log")
vero=$(python3 -c "print(($fine_m - $lancio) / 5)")
stima=$(grep -B1 '^BLOCCO' "$SANDBOX/giornata.log" | head -1 | sed -E 's/.*macchine ([0-9]+,[0-9]+) per.*/\1/; s/,/./')
controlla "stima per eccesso: macchina $stima \$ ≥ vero $(printf %.2f "$vero") \$ (vita della macchina in minuti finti × 1 \$)" python3 -c "import sys; sys.exit(0 if $stima >= $vero else 1)"
controlla "il riassunto della giornata comincia con il BLOCCO" bash -c "grep -m1 . '$GITHUB_STEP_SUMMARY' | grep -q '^# BLOCCO'"
controlla "annotazione ::warning:: BLOCCO_SPESA" grep -q '::warning title=BLOCCO_SPESA::' "$SANDBOX/giornata.log"
dopo
controlla "recupero: i passi della macchina sono in mano" test -s "$M31_OUT/passi.log"
controlla "pulizia: RIMASTO: niente" grep -q "RIMASTO: niente" "$SANDBOX/pulizia.log"
controlla "controllo finale: RIMASTO: niente, codice 0" bash -c "grep -q '^RIMASTO: niente' '$SANDBOX/controllo.log' && [ $RC_CONTROLLO = 0 ]"
echo "   ($(grep -B1 '^BLOCCO' "$SANDBOX/giornata.log" | head -1))"
echo "   ($(grep '^BLOCCO' "$SANDBOX/giornata.log"))"
echo "   (attesa: $((t1 - t0)) s = $(( (t1 - t0) / 5 )) minuti finti da 5 s; controllo: $(cat "$SANDBOX/controllo.log"))"

echo "== (a2) come (a), con una seconda macchina col tag rimasta da prima: la stima la conta due volte"
nuova_sandbox "$BASE/a2-due-macchine"; fai_pacco /bin/true "$FINTO_ESEGUI" "$SANDBOX/pacco.tgz"
export M31_INIZIO=$(date -u +%FT%TZ)
AMI=$(aws ssm get-parameter --name x --query Parameter.Value --output text)
VECCHIA=$("${AWS_VERO:-/usr/local/bin/aws}" --endpoint-url "$MOTO_URL" ec2 run-instances --image-id "$AMI" --instance-type c7i.xlarge \
  --tag-specifications "ResourceType=instance,Tags=[{Key=project,Value=fenvix-coldread}]" --query 'Instances[0].InstanceId' --output text)
MINUTO_S=5 PREZZO_ORA=60 M31_PACCO=$SANDBOX/pacco.tgz bash "$G_DIR/giornata-ci.sh" > "$SANDBOX/giornata.log" 2>&1
# ore-macchina nella riga del blocco: con una macchina sono (minuti+1)/60, con due il doppio
mm() { grep -B1 '^BLOCCO' "$1" | head -1 | sed -E 's/^SPESA: .* a ([0-9]+) minuti .*macchine ([0-9]+,[0-9]+) per.*/\1 \2/; s/,/./'; }
read -r m_a e_a <<<"$(mm "$BASE/a-blocco/giornata.log")"; read -r m_a2 e_a2 <<<"$(mm "$SANDBOX/giornata.log")"
controlla "BLOCCO_SPESA anche qui" bash -c "jq -e '.esito==\"BLOCCO_SPESA\"' '$M31_OUT/giornata.json' >/dev/null"
controlla "una macchina: stima macchina $e_a \$ = (minuti $m_a + 1) × 1 \$" python3 -c "import sys; sys.exit(0 if abs($e_a - ($m_a + 1)) < 0.02 else 1)"
controlla "due macchine: stima macchina $e_a2 \$ = 2 × (minuti $m_a2 + 1) × 1 \$ (la seconda si paga)" python3 -c "import sys; sys.exit(0 if abs($e_a2 - 2 * ($m_a2 + 1)) < 0.02 else 1)"
dopo
controlla "pulizia: cancellate tutte e due le macchine" bash -c "[ \"\$(stato_moto $VECCHIA)\" = terminated ] && [ \"\$(stato_moto $(id_di))\" = terminated ]"
controlla "controllo finale: RIMASTO: niente, codice 0" bash -c "grep -q '^RIMASTO: niente' '$SANDBOX/controllo.log' && [ $RC_CONTROLLO = 0 ]"

echo "== (b) pulizia che lascia di proposito un bucket e una macchina: ALLARME e job fallito"
nuova_sandbox "$BASE/b-allarme"
cat > "$BASE/esegui-breve.sh" <<'EOF'
#!/bin/bash
B=$1; echo "$(date -u +%T) inizio" > passi.log; aws s3 cp passi.log "s3://$B/risultati/passi.log" >/dev/null; sleep 4
echo '{"fine": true}' > fine.json; aws s3 cp fine.json "s3://$B/risultati/fine.json" >/dev/null
sleep 100000   # la macchina NON si spegne da sola: la deve cancellare la pulizia (che qui è guasta)
EOF
fai_pacco /bin/true "$BASE/esegui-breve.sh" "$SANDBOX/pacco.tgz"
A="${AWS_VERO:-/usr/local/bin/aws} --endpoint-url $MOTO_URL"
VOL_VECCHIO=$($A ec2 create-volume --availability-zone eu-west-1a --size 8 --query VolumeId --output text)  # non nostro, di prima
sleep 2; export M31_INIZIO=$(date -u +%FT%TZ)
M31_PACCO=$SANDBOX/pacco.tgz bash "$G_DIR/giornata-ci.sh" > "$SANDBOX/giornata.log" 2>&1
ID=$(id_di)
# altre cose lasciate durante la giornata: un Elastic IP col tag e un disco senza tag, non attaccato
EIP=$($A ec2 allocate-address --tag-specifications 'ResourceType=elastic-ip,Tags=[{Key=project,Value=fenvix-coldread}]' --query PublicIp --output text)
VOL=$($A ec2 create-volume --availability-zone eu-west-1a --size 60 --volume-type gp3 --query VolumeId --output text)
touch "$SANDBOX/guasto-pulizia"   # da qui terminate-instances e s3 rb dicono "fatto" ma non fanno niente
dopo
L=$SANDBOX/controllo.log
controlla "la giornata era finita (FINITA)" bash -c "jq -e '.esito==\"FINITA\"' '$M31_OUT/giornata.json' >/dev/null"
controlla "la pulizia guasta ha davvero lasciato macchina e bucket" bash -c "[ \"\$(stato_moto $ID)\" = running ] && $A s3api head-bucket --bucket $BUCKET >/dev/null"
controlla "controllo finale: riga 'ALLARME: rimasto …'" grep -q '^ALLARME: rimasto ' "$L"
controlla "nell'elenco c'è la macchina $ID" bash -c "grep '^ALLARME' '$L' | grep -q 'macchina $ID (running'"
controlla "nell'elenco c'è il bucket $BUCKET" bash -c "grep '^ALLARME' '$L' | grep -q 'bucket $BUCKET'"
controlla "nell'elenco c'è l'Elastic IP $EIP" bash -c "grep '^ALLARME' '$L' | grep -q 'indirizzo $EIP'"
controlla "nell'elenco c'è il disco non attaccato $VOL (creato nella giornata)" bash -c "grep '^ALLARME' '$L' | grep -q 'disco $VOL (available, 60 GB, non attaccato'"
controlla "nell'elenco c'è il disco col tag della macchina rimasta" bash -c "grep '^ALLARME' '$L' | grep -Eq 'disco vol-[0-9a-f]+ \\(in-use, 60 GB, tag\\)'"
controlla "NON c'è il disco non attaccato creato prima della giornata ($VOL_VECCHIO)" bash -c "! grep -q '$VOL_VECCHIO' '$L'"
controlla "annotazione ::error:: (in cima alla pagina della run)" grep -q '^::error title=ALLARME controllo finale::rimasto ' "$L"
controlla "il riassunto del passo comincia con l'ALLARME" bash -c "head -1 '$SANDBOX/summary-finale.md' | grep -q '^# ALLARME'"
controlla "codice 1: il job fallisce" bash -c "[ $RC_CONTROLLO = 1 ]"
# nel workflow: il controllo è l'ultimo passo, gira sempre e non ha continue-on-error, quindi codice 1 = job rosso
controlla "workflow: controllo finale ultimo passo, if: always(), senza continue-on-error, credenziali fresche subito prima" python3 - "$WF" <<'PY'
import sys, yaml
s = yaml.safe_load(open(sys.argv[1]))["jobs"]["run"]["steps"]
ult, pen = s[-1], s[-2]
ok = ("controllo-finale.sh" in ult.get("run", "") and ult.get("if") == "always()" and not ult.get("continue-on-error")
      and pen.get("uses", "").startswith("aws-actions/configure-aws-credentials") and pen.get("if") == "always()")
i_pul = next(i for i, x in enumerate(s) if "pulizia.sh" in x.get("run", ""))
sys.exit(0 if ok and i_pul < len(s) - 2 else 1)
PY
echo "   ($(grep '^ALLARME' "$L"))"
# pulisco a mano quello che la pulizia guasta ha lasciato, e il controllo deve tornare a "niente"
rm -f "$SANDBOX/guasto-pulizia"; $A ec2 release-address --public-ip "$EIP"; $A ec2 delete-volume --volume-id "$VOL"
bash "$G_DIR/pulizia.sh" > "$SANDBOX/pulizia2.log" 2>&1
bash "$G_DIR/controllo-finale.sh" > "$SANDBOX/controllo2.log" 2>&1; rc2=$?
controlla "dopo la pulizia vera e a mano: RIMASTO: niente, codice 0" bash -c "grep -q '^RIMASTO: niente' '$SANDBOX/controllo2.log' && [ $rc2 = 0 ]"

echo "== (b2) lettura che non riesce (credenziali scadute): ALLARME con NON SO, job fallito"
touch "$SANDBOX/scadute"
AWS_ACCESS_KEY_ID=vecchia bash "$G_DIR/controllo-finale.sh" > "$SANDBOX/controllo3.log" 2>&1; rc3=$?
controlla "ALLARME con 'NON SO, lettura non riuscita' e codice 1" bash -c "grep -q '^ALLARME: rimasto NON SO, lettura non riuscita' '$SANDBOX/controllo3.log' && [ $rc3 = 1 ]"

rm -f "$SANDBOX/scadute"

echo "== (b3) risposta rotta con codice 0 (JSON non leggibile): ALLARME con NON SO, mai 'niente'"
touch "$SANDBOX/guasto-json"
bash "$G_DIR/controllo-finale.sh" > "$SANDBOX/controllo4.log" 2>&1; rc4=$?
controlla "ALLARME 'NON SO … macchine EC2: risposta non leggibile' e codice 1" bash -c "grep -q '^ALLARME: rimasto NON SO, lettura non riuscita: macchine EC2: risposta non leggibile' '$SANDBOX/controllo4.log' && [ $rc4 = 1 ]"
rm -f "$SANDBOX/guasto-json"

echo "== RISULTATO: $FALLITE controlli falliti"
[ "$FALLITE" = 0 ]
