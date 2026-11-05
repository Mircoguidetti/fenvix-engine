#!/bin/bash
# P1 Irlanda · il giro da GitHub Actions (modello: M31 giornata-ci.sh). Bucket nuovo; nel bucket il pacco (script,
# librerie Python), la stringa di connessione (oggetto cancellato dalla macchina appena letto) e i due file di stato;
# una macchina $TIPO (c7i.8xlarge di serie) in eu-west-1 con il ruolo fenvix-coldread-machine; l'IP pubblico della macchina nel log (da mettere
# nella lista di accesso di Atlas); attesa con la riga SPESA ogni minuto; risultati in $OUT. La pulizia e' in
# pulizia.sh (gira sempre, if: always()).
# Variabili dal workflow: BUCKET, TIPO, PREZZO_ORA, TETTO_DOLLARI, RUOLO_MACCHINA, IRL_ACCT, IRL_ROLE, DEMO_DB_URI,
# STATO_DIR (cartella con risultati_p1.json e risultati_p1.json.usati.log).
set -uo pipefail
QUI=$(cd "$(dirname "$0")" && pwd)
source "$QUI/user-data.sh"
OUT=${IRL_OUT:-/tmp/irl-out}; mkdir -p "$OUT"
PACCO=${IRL_PACCO:-/tmp/irlanda.tar.gz}
MINUTO_S=${MINUTO_S:-60}; MUTA_MIN=${MUTA_MIN:-15}; RINNOVO_S=${RINNOVO_S:-2700}
T0=$(date +%s); TETTO_MIN=${TETTO_MIN:-110}; ULTIMO_RINNOVO=$T0
DISCO_GB=${DISCO_GB:-16}
rinnova() { # credenziali OIDC nuove per un'altra ora, senza uscire dallo step (come M31)
  local tok j
  tok=$(curl -fsS -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=sts.amazonaws.com" | jq -r .value) || return 1
  j=$(aws sts assume-role-with-web-identity --role-arn "$IRL_ROLE" --role-session-name irlanda-giro --web-identity-token "$tok" --duration-seconds 3600 --output json) || return 1
  AWS_ACCESS_KEY_ID=$(jq -r .Credentials.AccessKeyId <<<"$j"); AWS_SECRET_ACCESS_KEY=$(jq -r .Credentials.SecretAccessKey <<<"$j"); AWS_SESSION_TOKEN=$(jq -r .Credentials.SessionToken <<<"$j")
  export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
  echo "::add-mask::$AWS_SECRET_ACCESS_KEY"; echo "::add-mask::$AWS_SESSION_TOKEN"
  ULTIMO_RINNOVO=$(date +%s)
}
# Spesa stimata PER ECCESSO (regola del fondatore 2026-11-04: blocco a TETTO_DOLLARI, di serie 5 $). Come M31: ogni
# minuto si contano le macchine con il tag ancora vive (almeno 1). Per macchina: $TIPO al prezzo PREZZO_ORA del workflow, disco
# gp3 da DISCO_GB GB, IPv4 pubblico; S3: 0,02 $ fissi (poche centinaia di PUT e GET). Atlas NON e' contato qui: il
# cluster M30 si paga a parte (circa 0,77 $ l'ora, MISURATO in P1) e gira comunque fino al Terminate.
TETTO_DOLLARI=${TETTO_DOLLARI:-5}; MAC_MIN=0; N_MAC=1
spesa() { # spesa MINUTI -> stampa la riga SPESA e mette in SPESA_ORA il numero (punto decimale)
  local riga
  riga=$(python3 - "$1" "$MAC_MIN" "$PREZZO_ORA" "$TETTO_DOLLARI" "$DISCO_GB" <<'PY'
import sys; m=float(sys.argv[1]); mm=float(sys.argv[2]); p=float(sys.argv[3]); t=float(sys.argv[4]); gb=float(sys.argv[5]); h=mm/60
ec2=h*p; ebs=gb*0.088*h/730; ip=0.005*h; s3=0.02; tot=ec2+ebs+ip+s3
v=lambda x,d=2: f"{x:.{d}f}".replace(".", ",")
print(f"{tot:.4f}|SPESA: {v(tot)} $ finora a {m:.0f} minuti (stima per eccesso: macchine {v(ec2)} per {v(h,1)} ore-macchina, disco {v(ebs,3)}, IPv4 {v(ip,3)}, S3 {v(s3)}), tetto {v(t)} $")
PY
)
  SPESA_ORA=${riga%%|*}; echo "${riga#*|}"
}
virgola() { python3 -c "import sys; print(f'{float(sys.argv[1]):.2f}'.replace('.', ','))" "$1"; }
oltre_tetto() { python3 -c "import sys; sys.exit(0 if float(sys.argv[1]) >= float(sys.argv[2]) else 1)" "$SPESA_ORA" "$TETTO_DOLLARI"; }
conta_macchine() {
  local n
  n=$(aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-coldread Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down --query 'length(Reservations[].Instances[])' --output text 2>/dev/null) || n=""
  [[ "$n" =~ ^[0-9]+$ ]] && N_MAC=$(( n > 0 ? n : 1 ))
}
[ -n "${DEMO_DB_URI:-}" ] || { echo "segreto DEMO_DB_URI mancante"; exit 1; }
echo "1/6 bucket $BUCKET"
aws s3api create-bucket --bucket "$BUCKET" --create-bucket-configuration LocationConstraint=eu-west-1 >/dev/null || exit 1
aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-tagging --bucket "$BUCKET" --tagging "TagSet=[{Key=project,Value=fenvix-coldread}]"
aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" --lifecycle-configuration '{"Rules":[{"ID":"scade","Status":"Enabled","Filter":{},"Expiration":{"Days":1},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":1}}]}'
echo "2/6 pacco, file di stato e stringa di connessione nel bucket (la stringa non passa da file ne' da log)"
aws s3 cp "$PACCO" "s3://$BUCKET/programma/irlanda.tar.gz" --only-show-errors || exit 1
aws s3 cp "$STATO_DIR/risultati_p1.json" "s3://$BUCKET/stato/risultati_p1.json" --only-show-errors || exit 1
aws s3 cp "$STATO_DIR/risultati_p1.json.usati.log" "s3://$BUCKET/stato/risultati_p1.json.usati.log" --only-show-errors || exit 1
printf '%s' "$DEMO_DB_URI" | aws s3 cp - "s3://$BUCKET/segreto/uri" --only-show-errors || exit 1
URL=$(aws s3 presign "s3://$BUCKET/programma/irlanda.tar.gz" --expires-in 7200)
echo "3/6 rete e immagine"
VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
SUBNET=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true --query 'sort_by(Subnets,&AvailabilityZone)[0].SubnetId' --output text)
AMI=$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text)
UD=$(mktemp)
scrivi_user_data "$URL" "$BUCKET" > "$UD"
echo "4/6 macchina $TIPO in $SUBNET (si cancella da sola alla fine, al massimo dopo ${TETTO_MACCHINA_MIN:-100} minuti)"
ID=$(aws ec2 run-instances --image-id "$AMI" --instance-type "$TIPO" --subnet-id "$SUBNET" --associate-public-ip-address \
  --iam-instance-profile Name="$RUOLO_MACCHINA" --instance-initiated-shutdown-behavior terminate \
  --metadata-options HttpTokens=required,HttpEndpoint=enabled \
  --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=$DISCO_GB,VolumeType=gp3,DeleteOnTermination=true}" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=project,Value=fenvix-coldread},{Key=Name,Value=fenvix-coldread-irlanda}]" "ResourceType=volume,Tags=[{Key=project,Value=fenvix-coldread}]" \
  --user-data "file://$UD" --query 'Instances[0].InstanceId' --output text) || exit 1
rm -f "$UD"
echo "macchina: $ID"
echo "5/6 IP pubblico della macchina"
IP=""
for _ in $(seq 1 30); do
  IP=$(aws ec2 describe-instances --instance-ids "$ID" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text 2>/dev/null)
  [[ "$IP" =~ ^[0-9.]+$ ]] && break; IP=""; sleep 5
done
riga_ip="IP DA AGGIUNGERE AD ATLAS (Network Access -> Add IP Address): ${IP:-non ancora noto, lo scrive la macchina}/32"
echo "$riga_ip"; echo "::warning title=IP per Atlas::$riga_ip"; echo "$IP" > "$OUT/ip-runner.txt"
echo "6/6 attesa (ogni minuto: spesa, ultima riga della macchina)"
esito=TETTO; visto_passi=0; m_prima=0
while :; do
  sleep "$MINUTO_S"
  m=$(( ($(date +%s) - T0) / MINUTO_S ))
  [ $(( $(date +%s) - ULTIMO_RINNOVO )) -gt "$RINNOVO_S" ] && { rinnova || echo "rinnovo delle credenziali fallito"; }
  conta_macchine
  MAC_MIN=$(( MAC_MIN + N_MAC * (m - m_prima + 1) )); m_prima=$(( m + 1 ))
  spesa "$m"
  if oltre_tetto; then
    echo "BLOCCO: spesa stimata $(virgola "$SPESA_ORA") $ ≥ $(virgola "$TETTO_DOLLARI") $: smetto di aspettare, cancello la macchina, parte la pulizia"
    aws ec2 terminate-instances --instance-ids "$ID" >/dev/null 2>&1 || echo "cancellazione subito non riuscita: la rifà pulizia.sh"
    esito=BLOCCO_SPESA; break
  fi
  if riga=$(aws s3 cp "s3://$BUCKET/risultati/passi.log" - 2>/dev/null | tail -1) && [ -n "$riga" ]; then
    visto_passi=1; echo "  macchina: $riga"
    case "$riga" in *"ATTESA LISTA DI ACCESSO"*) echo "  >>> $riga_ip";; esac
    t=$(aws s3 cp "s3://$BUCKET/risultati/terminale.txt" - 2>/dev/null | tail -1) && [ -n "$t" ] && echo "  script: $t"
  fi
  if [ "$visto_passi" = 0 ] && [ "$m" -ge "$MUTA_MIN" ]; then echo "MACCHINA MUTA: nessun passi.log dopo $m minuti"; esito=MACCHINA_MUTA; break; fi
  if aws s3 ls "s3://$BUCKET/risultati/fine.json" >/dev/null 2>&1; then
    esito=$(aws s3 cp "s3://$BUCKET/risultati/fine.json" - 2>/dev/null | jq -r '.esito // "FINITA"' 2>/dev/null || echo FINITA); break
  fi
  st=$(aws ec2 describe-instances --instance-ids "$ID" --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null)
  if [ "$st" = "terminated" ] || [ "$st" = "shutting-down" ]; then
    sleep $(( MINUTO_S / 2 ))
    if aws s3 ls "s3://$BUCKET/risultati/fine.json" >/dev/null 2>&1; then
      esito=$(aws s3 cp "s3://$BUCKET/risultati/fine.json" - 2>/dev/null | jq -r '.esito // "FINITA"' 2>/dev/null || echo FINITA)
    else esito=MACCHINA_SPENTA; fi
    break
  fi
  [ "$m" -ge "$TETTO_MIN" ] && break
done
echo "esito: $esito"
aws s3 cp "s3://$BUCKET/risultati/" "$OUT/" --recursive --only-show-errors || echo "copia dei risultati fallita: la rifà recupera.sh"
m=$(( ($(date +%s) - T0) / MINUTO_S ))
MAC_MIN=$(( MAC_MIN + N_MAC * (m - m_prima + 1 + 10) ))
spesa "$m" | tee "$OUT/spesa.txt"
echo "{\"esito\": \"$esito\", \"minuti\": $m, \"macchina\": \"$TIPO\", \"id\": \"$ID\", \"ip\": \"$IP\"}" > "$OUT/giro.json"
grep -rl "$IRL_ACCT" "$OUT" 2>/dev/null | xargs -r sed -i "s/$IRL_ACCT/<ACCOUNT_ID>/g"
{ [ "$esito" = BLOCCO_SPESA ] && echo "# BLOCCO: spesa stimata arrivata al tetto di $TETTO_DOLLARI \$"
  echo "## P1 Irlanda: $esito, $m minuti"; echo "$riga_ip"; echo
  echo "**Adesso: togli $IP/32 dalla lista di accesso di Atlas.**"; echo; cat "$OUT/spesa.txt"; echo '```'; tail -20 "$OUT/passi.log" 2>/dev/null; echo '```'; } >> "$GITHUB_STEP_SUMMARY"
[ "$esito" = BLOCCO_SPESA ] && echo "::warning title=BLOCCO_SPESA::spesa stimata ≥ $TETTO_DOLLARI \$: giro fermato, pulizia partita"
[ "$esito" = FINITO ]
