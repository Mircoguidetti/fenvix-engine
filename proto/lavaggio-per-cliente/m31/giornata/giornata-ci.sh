#!/bin/bash
# M31 · la giornata da GitHub Actions: bucket nuovo, programma nel bucket, una c7i.xlarge in eu-west-1 che fa esegui.sh,
# attesa con la spesa stimata ogni minuto, risultati in /tmp/m31-out. La pulizia è in pulizia.sh (gira sempre).
set -uo pipefail
QUI=$(cd "$(dirname "$0")" && pwd)
source "$QUI/user-data.sh"
OUT=${M31_OUT:-/tmp/m31-out}; mkdir -p "$OUT"
PACCO=${M31_PACCO:-/tmp/m31-giornata.tar.gz}
# MINUTO_S: secondi di un "minuto" dell'attesa (60; la prova locale lo accorcia). MUTA_MIN: revisore M31, C2.
MINUTO_S=${MINUTO_S:-60}; MUTA_MIN=${MUTA_MIN:-15}; RINNOVO_S=${RINNOVO_S:-2700}
T0=$(date +%s); TETTO_MIN=${TETTO_MIN:-250}; ULTIMO_RINNOVO=$T0
rinnova() { # credenziali OIDC nuove per un'altra ora, senza uscire dallo step
  local tok j
  tok=$(curl -fsS -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=sts.amazonaws.com" | jq -r .value) || return 1
  j=$(aws sts assume-role-with-web-identity --role-arn "$M31_ROLE" --role-session-name coldread-giornata --web-identity-token "$tok" --duration-seconds 3600 --output json) || return 1
  export AWS_ACCESS_KEY_ID=$(jq -r .Credentials.AccessKeyId <<<"$j") AWS_SECRET_ACCESS_KEY=$(jq -r .Credentials.SecretAccessKey <<<"$j") AWS_SESSION_TOKEN=$(jq -r .Credentials.SessionToken <<<"$j")
  echo "::add-mask::$AWS_SECRET_ACCESS_KEY"; echo "::add-mask::$AWS_SESSION_TOKEN"
  ULTIMO_RINNOVO=$(date +%s)
}
# Spesa stimata PER ECCESSO (regola del fondatore 2026-11-04: blocco a TETTO_DOLLARI, di serie 5 $).
# MAC_MIN = minuti-macchina accumulati: ogni minuto si contano le macchine con il tag ancora vive (almeno 1, la nostra,
# anche se la lettura fallisce), quindi una macchina in più rimasta da prima o lanciata due volte si paga nella stima.
# Per ogni macchina: c7i.xlarge al prezzo DICHIARATO, disco 60 GB gp3, IPv4 pubblico. S3: subito tutta la somma dei
# tetti del freno (650.000 GET + 70.000 PUT/LIST = 0,61 $) più 0,03 $ di copie e controlli, perché le richieste non si
# vedono minuto per minuto: così la stima non può stare sotto il vero finché il freno di esegui.sh regge.
TETTO_DOLLARI=${TETTO_DOLLARI:-5}; MAC_MIN=0; N_MAC=1
spesa() { # spesa MINUTI -> stampa la riga SPESA e mette in SPESA_ORA il numero (punto decimale)
  local riga
  riga=$(python3 - "$1" "$MAC_MIN" "$PREZZO_ORA" "$TETTO_DOLLARI" <<'PY'
import sys; m=float(sys.argv[1]); mm=float(sys.argv[2]); p=float(sys.argv[3]); t=float(sys.argv[4]); h=mm/60
ec2=h*p; ebs=60*0.088*h/730; ip=0.005*h; s3=0.64; tot=ec2+ebs+ip+s3
v=lambda x,d=2: f"{x:.{d}f}".replace(".", ",")
print(f"{tot:.4f}|SPESA: {v(tot)} $ finora a {m:.0f} minuti (stima per eccesso: macchine {v(ec2)} per {v(h,1)} ore-macchina, disco {v(ebs,3)}, IPv4 {v(ip,3)}, S3 {v(s3)} = tetto dei freni), tetto {v(t)} $")
PY
)
  SPESA_ORA=${riga%%|*}; echo "${riga#*|}"
}
virgola() { python3 -c "import sys; print(f'{float(sys.argv[1]):.2f}'.replace('.', ','))" "$1"; }
oltre_tetto() { python3 -c "import sys; sys.exit(0 if float(sys.argv[1]) >= float(sys.argv[2]) else 1)" "$SPESA_ORA" "$TETTO_DOLLARI"; }
conta_macchine() { # macchine con il tag non ancora cancellate; se la lettura fallisce resta il numero di prima (almeno 1)
  local n
  n=$(aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-coldread Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down --query 'length(Reservations[].Instances[])' --output text 2>/dev/null) || n=""
  [[ "$n" =~ ^[0-9]+$ ]] && N_MAC=$(( n > 0 ? n : 1 ))
}
echo "1/5 bucket $BUCKET"
aws s3api create-bucket --bucket "$BUCKET" --create-bucket-configuration LocationConstraint=eu-west-1 >/dev/null || exit 1
aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-tagging --bucket "$BUCKET" --tagging "TagSet=[{Key=project,Value=fenvix-coldread}]"
aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" --lifecycle-configuration '{"Rules":[{"ID":"scade","Status":"Enabled","Filter":{},"Expiration":{"Days":1},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":1}}]}'
echo "2/5 programma nel bucket"
aws s3 cp "$PACCO" "s3://$BUCKET/programma/coldread-giornata.tar.gz" --only-show-errors || exit 1
URL=$(aws s3 presign "s3://$BUCKET/programma/coldread-giornata.tar.gz" --expires-in 7200)
echo "3/5 rete e immagine"
VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
SUBNET=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true --query 'sort_by(Subnets,&AvailabilityZone)[0].SubnetId' --output text)
AMI=$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text)
UD=$(mktemp)
scrivi_user_data "$URL" "$BUCKET" > "$UD"
echo "4/5 macchina $TIPO in $SUBNET (si cancella da sola alla fine, al massimo dopo 240 minuti)"
ID=$(aws ec2 run-instances --image-id "$AMI" --instance-type "$TIPO" --subnet-id "$SUBNET" --associate-public-ip-address \
  --iam-instance-profile Name="$RUOLO_MACCHINA" --instance-initiated-shutdown-behavior terminate \
  --metadata-options HttpTokens=required,HttpEndpoint=enabled \
  --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=60,VolumeType=gp3,DeleteOnTermination=true}' \
  --tag-specifications "ResourceType=instance,Tags=[{Key=project,Value=fenvix-coldread},{Key=Name,Value=fenvix-coldread}]" "ResourceType=volume,Tags=[{Key=project,Value=fenvix-coldread}]" \
  --user-data "file://$UD" --query 'Instances[0].InstanceId' --output text) || exit 1
rm -f "$UD"
echo "macchina: $ID"
echo "5/5 attesa (ogni minuto: passo, spesa)"
esito=TETTO; visto_passi=0; m_prima=0
while :; do
  sleep "$MINUTO_S"
  m=$(( ($(date +%s) - T0) / MINUTO_S ))
  [ $(( $(date +%s) - ULTIMO_RINNOVO )) -gt "$RINNOVO_S" ] && { rinnova || echo "rinnovo delle credenziali fallito"; }
  conta_macchine
  # minuti-macchina: dall'inizio dello script (prima del lancio, quindi per eccesso), arrotondati per eccesso al minuto
  MAC_MIN=$(( MAC_MIN + N_MAC * (m - m_prima + 1) )); m_prima=$(( m + 1 ))
  spesa "$m"
  if oltre_tetto; then
    echo "BLOCCO: spesa stimata $(virgola "$SPESA_ORA") $ ≥ $(virgola "$TETTO_DOLLARI") $: smetto di aspettare, cancello la macchina, parte la pulizia"
    aws ec2 terminate-instances --instance-ids "$ID" >/dev/null 2>&1 || echo "cancellazione subito non riuscita: la rifà pulizia.sh"
    esito=BLOCCO_SPESA; break
  fi
  if riga=$(aws s3 cp "s3://$BUCKET/risultati/passi.log" - 2>/dev/null | tail -1) && [ -n "$riga" ]; then visto_passi=1; echo "$riga"; fi
  # C2: dopo MUTA_MIN minuti senza passi.log nel bucket la macchina è muta (avvio rotto, ruolo senza permessi, S3
  # irraggiungibile): non si aspetta fino al tetto; si esce e la pulizia la cancella
  if [ "$visto_passi" = 0 ] && [ "$m" -ge "$MUTA_MIN" ]; then echo "MACCHINA MUTA: nessun passi.log dopo $m minuti"; esito=MACCHINA_MUTA; break; fi
  if aws s3 ls "s3://$BUCKET/risultati/fine.json" >/dev/null 2>&1; then esito=FINITA; break; fi
  st=$(aws ec2 describe-instances --instance-ids "$ID" --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null)
  if [ "$st" = "terminated" ] || [ "$st" = "shutting-down" ]; then sleep $(( MINUTO_S / 2 )); aws s3 ls "s3://$BUCKET/risultati/fine.json" >/dev/null 2>&1 && esito=FINITA || esito=MACCHINA_SPENTA; break; fi
  [ "$m" -ge "$TETTO_MIN" ] && break
done
echo "esito: $esito"
# se le credenziali sono scadute (rinnovo fallito) questa copia fallisce: la rifà recupera.sh con le credenziali
# fresche della pulizia, prima di cancellare il bucket (revisore M31, C3)
aws s3 cp "s3://$BUCKET/risultati/" "$OUT/" --recursive --only-show-errors || echo "copia dei risultati fallita: la rifà recupera.sh"
m=$(( ($(date +%s) - T0) / MINUTO_S ))
# fino a qui, più 10 minuti per recupero, pulizia e controllo finale (la macchina può essere ancora viva), per eccesso
MAC_MIN=$(( MAC_MIN + N_MAC * (m - m_prima + 1 + 10) ))
spesa "$m" | tee "$OUT/spesa.txt"
echo "{\"esito\": \"$esito\", \"minuti\": $m, \"macchina\": \"$TIPO\", \"id\": \"$ID\"}" > "$OUT/giornata.json"
grep -rl "$M31_ACCT" "$OUT" 2>/dev/null | xargs -r sed -i "s/$M31_ACCT/<ACCOUNT_ID>/g"
{ [ "$esito" = BLOCCO_SPESA ] && echo "# BLOCCO: spesa stimata arrivata al tetto di $TETTO_DOLLARI \$"
  echo "## M31 giornata: $esito, $m minuti"; cat "$OUT/spesa.txt"; tail -20 "$OUT/passi.log" 2>/dev/null; } >> "$GITHUB_STEP_SUMMARY"
[ "$esito" = BLOCCO_SPESA ] && echo "::warning title=BLOCCO_SPESA::spesa stimata ≥ $TETTO_DOLLARI \$: giornata fermata, pulizia partita"
[ "$esito" = FINITA ]
