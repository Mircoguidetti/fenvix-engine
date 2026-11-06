#!/bin/bash
# Q8c on AWS: the day from GitHub Actions (eu-west-1). New bucket, program in the bucket, three machines of type
# TIPO_REPLICA for the three copies of every log of the cell (one per zone: a, b, c, each with its gp3 disk) and one
# TIPO_GEN for the generators in zone a. Default with the vCPU quota at 8: four c7i.large (2 vCPU each); with the quota
# at 16: TIPO_REPLICA=TIPO_GEN=c7i.xlarge (set in the workflow). (Copied from Q8, comments below still in Italian.)
# Attesa con la riga SPESA ogni minuto; BLOCCO a TETTO_DOLLARI (5 $, regola del fondatore 2026-11-04).
# I risultati vanno in $OUT; la pulizia (pulizia.sh) e il controllo finale (controllo-finale.sh) girano sempre dopo.
# Nomi in AWS presi dalla giornata M31 (già autorizzati dal fondatore il 2026-11-04): tag project=fenvix-coldread,
# ruolo della macchina fenvix-coldread-machine, bucket fenvix-coldread-q8-<run>.
set -uo pipefail
QUI=$(cd "$(dirname "$0")" && pwd)
OUT=${Q8_OUT:-/tmp/q8-out}; mkdir -p "$OUT"
PACCO=${Q8_PACCO:-/tmp/q8-giornata.tar.gz}
MINUTO_S=${MINUTO_S:-60}; MUTA_MIN=${MUTA_MIN:-15}; TETTO_MIN=${TETTO_MIN:-140}; RINNOVO_S=${RINNOVO_S:-2700}
T0=$(date +%s); ULTIMO_RINNOVO=$T0
rinnova() {
  local tok j
  tok=$(curl -fsS -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=sts.amazonaws.com" | jq -r .value) || return 1
  j=$(aws sts assume-role-with-web-identity --role-arn "$Q8_ROLE" --role-session-name q8-giornata --web-identity-token "$tok" --duration-seconds 3600 --output json) || return 1
  export AWS_ACCESS_KEY_ID=$(jq -r .Credentials.AccessKeyId <<<"$j") AWS_SECRET_ACCESS_KEY=$(jq -r .Credentials.SecretAccessKey <<<"$j") AWS_SESSION_TOKEN=$(jq -r .Credentials.SessionToken <<<"$j")
  echo "::add-mask::$AWS_SECRET_ACCESS_KEY"; echo "::add-mask::$AWS_SESSION_TOKEN"
  ULTIMO_RINNOVO=$(date +%s)
}
# Spesa stimata PER ECCESSO. MAC_MIN = minuti-macchina (ogni minuto si contano le macchine con il tag ancora vive,
# almeno 4). Per macchina: c7i.xlarge al prezzo DICHIARATO (PREZZO_ORA), disco 30 GB gp3, IPv4 pubblico.
# Traffico tra zone: ogni record confermato conta 4 attraversamenti (cliente -> capo, capo -> 2 copie, copia ->
# scaricatore) da 2,2 KB senza compressione, a 0,02 $ per GB attraversato (0,01 per verso, DICHIARATO). Il numero dei
# record viene dalla macchina dei generatori (passi-gen.log); finché non c'è, si conta il massimo del piano: 13.000
# record al secondo da quando i generatori sono partiti. S3: 0,05 $ fissi (poche centinaia di PUT e GET).
TETTO_DOLLARI=${TETTO_DOLLARI:-5}; MAC_MIN=0; N_MAC=4; RECORD=0; GEN_MIN=0
spesa() {
  local riga
  riga=$(python3 - "$1" "$MAC_MIN" "$PREZZO_ORA" "$TETTO_DOLLARI" "$RECORD" "$GEN_MIN" <<'PY'
import sys; m=float(sys.argv[1]); mm=float(sys.argv[2]); p=float(sys.argv[3]); t=float(sys.argv[4]); rec=float(sys.argv[5]); gm=float(sys.argv[6])
h=mm/60; ec2=h*p; ebs=30*0.088*h/730; ip=0.005*h; s3=0.05
if rec <= 0: rec = 13000*60*gm
xaz = rec*2200*4*0.02/1e9
tot=ec2+ebs+ip+s3+xaz
v=lambda x,d=2: f"{x:.{d}f}".replace(".", ",")
print(f"{tot:.4f}|SPESA: {v(tot)} $ finora a {m:.0f} minuti (stima per eccesso: macchine {v(ec2)} per {v(h,1)} ore-macchina, dischi {v(ebs,3)}, IPv4 {v(ip,3)}, tra zone {v(xaz)} per {rec/1e6:.1f} milioni di record, S3 {v(s3)}), tetto {v(t)} $")
PY
)
  SPESA_ORA=${riga%%|*}; echo "${riga#*|}"
}
oltre_tetto() { python3 -c "import sys; sys.exit(0 if float(sys.argv[1]) >= float(sys.argv[2]) else 1)" "$SPESA_ORA" "$TETTO_DOLLARI"; }
conta_macchine() {
  local n
  n=$(aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-coldread "Name=tag:Name,Values=fenvix-q8-${GITHUB_RUN_ID:-0}-*" Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down --query 'length(Reservations[].Instances[])' --output text 2>/dev/null) || n=""
  [[ "$n" =~ ^[0-9]+$ ]] && N_MAC=$(( n > 4 ? n : 4 ))
}
user_data() { # URL BUCKET RUOLO
  cat <<UDEOF
#!/bin/bash
shutdown -h +150 "Q8: tetto" || true
mkdir -p /opt/q8 && cd /opt/q8 || { shutdown -h now; exit 1; }
ok=0
for i in 1 2 3 4 5; do curl -fsS -o g.tgz "$1" && { ok=1; break; }; sleep 10; done
[ "\$ok" = 1 ] || { shutdown -h now "Q8: scaricamento fallito"; exit 1; }
tar xzf g.tgz || { shutdown -h now "Q8: tar fallito"; exit 1; }
# aws cli e jq (snap e apt nell'immagine Ubuntu 24.04); senza, la macchina non può dare i risultati: si spegne
snap install aws-cli --classic >/dev/null 2>&1 || true
command -v jq >/dev/null || apt-get install -y -q jq >/dev/null 2>&1 || true
command -v aws >/dev/null && command -v jq >/dev/null || { shutdown -h now "Q8: aws o jq mancanti"; exit 1; }
nohup ./macchina.sh "$2" "$3" > macchina-$3.log 2>&1 &
UDEOF
}
echo "1/6 bucket $BUCKET"
aws s3api create-bucket --bucket "$BUCKET" --create-bucket-configuration LocationConstraint=eu-west-1 >/dev/null || exit 1
aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-tagging --bucket "$BUCKET" --tagging "TagSet=[{Key=project,Value=fenvix-coldread}]"
aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" --lifecycle-configuration '{"Rules":[{"ID":"scade","Status":"Enabled","Filter":{},"Expiration":{"Days":1},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":1}}]}'
echo "2/6 programma nel bucket"
aws s3 cp "$PACCO" "s3://$BUCKET/programma/q8.tar.gz" --only-show-errors || exit 1
URL=$(aws s3 presign "s3://$BUCKET/programma/q8.tar.gz" --expires-in 7200)
echo "3/6 rete e immagine"
VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
mapfile -t SUBNETS < <(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true --query 'sort_by(Subnets,&AvailabilityZone)[0:3].SubnetId' --output text | tr '\t' '\n')
[ "${#SUBNETS[@]}" -ge 3 ] || { echo "servono 3 zone con una sottorete di serie"; exit 1; }
AMI=$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text)
lancia() { # RUOLO SUBNET
  local ud; ud=$(mktemp); user_data "$URL" "$BUCKET" "$1" > "$ud"
  local tipo=$TIPO_REPLICA; [ "$1" = gen ] && tipo=$TIPO_GEN
  aws ec2 run-instances --image-id "$AMI" --instance-type "$tipo" --subnet-id "$2" --associate-public-ip-address \
    --iam-instance-profile Name="$RUOLO_MACCHINA" --instance-initiated-shutdown-behavior terminate \
    --metadata-options HttpTokens=required,HttpEndpoint=enabled \
    --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=30,VolumeType=gp3,DeleteOnTermination=true}' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=project,Value=fenvix-coldread},{Key=Name,Value=fenvix-q8-${GITHUB_RUN_ID:-0}-$1}]" "ResourceType=volume,Tags=[{Key=project,Value=fenvix-coldread}]" \
    --user-data "file://$ud" --query 'Instances[0].[InstanceId,PrivateIpAddress]' --output text
  rm -f "$ud"
}
echo "4/6 tre macchine delle copie $TIPO_REPLICA (zone a, b, c) e i generatori $TIPO_GEN (zona a), ${LOGS:-4} quaderni; si cancellano da sole al più tardi dopo 150 minuti"
declare -A ID IP
for i in 0 1 2; do
  read -r ID[r$i] IP[r$i] < <(lancia r$i "${SUBNETS[$i]}") || exit 1
  [ -n "${ID[r$i]}" ] || exit 1
  echo "r$i: ${ID[r$i]} ${IP[r$i]}"
done
jq -n --arg a "${IP[r0]}" --arg b "${IP[r1]}" --arg c "${IP[r2]}" --argjson l "${LOGS:-4}" --arg v "${LIVELLI:-1,2,20,50,100}" '{r0: $a, r1: $b, r2: $c, logs: $l, levels: $v}' > /tmp/q8-config.json
aws s3 cp /tmp/q8-config.json "s3://$BUCKET/config.json" --only-show-errors || exit 1
read -r ID[gen] IP[gen] < <(lancia gen "${SUBNETS[0]}") || exit 1
echo "gen: ${ID[gen]}"
echo "5/6 attesa (ogni minuto: passo, spesa)"
esito=TETTO; visto=0; m_prima=0
while :; do
  sleep "$MINUTO_S"
  m=$(( ($(date +%s) - T0) / MINUTO_S ))
  [ $(( $(date +%s) - ULTIMO_RINNOVO )) -gt "$RINNOVO_S" ] && { rinnova || echo "rinnovo delle credenziali fallito"; }
  conta_macchine
  MAC_MIN=$(( MAC_MIN + N_MAC * (m - m_prima + 1) )); m_prima=$(( m + 1 ))
  if riga=$(aws s3 cp "s3://$BUCKET/risultati/passi-gen.log" - 2>/dev/null | tail -1) && [ -n "$riga" ]; then
    visto=1; echo "$riga"; GEN_MIN=$m
    n=$(grep -o 'records in the logs so far: [0-9]*' <<<"$riga" | grep -o '[0-9]*$') && [ -n "$n" ] && RECORD=$(( n > RECORD ? n : RECORD ))
  fi
  spesa "$m"
  if oltre_tetto; then
    echo "BLOCCO: spesa stimata ≥ $TETTO_DOLLARI \$: smetto di aspettare, cancello le macchine, parte la pulizia"
    aws ec2 terminate-instances --instance-ids "${ID[@]}" >/dev/null 2>&1 || echo "cancellazione subito non riuscita: la rifà pulizia.sh"
    esito=BLOCCO_SPESA; break
  fi
  if [ "$visto" = 0 ] && [ "$m" -ge "$MUTA_MIN" ]; then echo "MACCHINA MUTA: nessun passi-gen.log dopo $m minuti"; esito=MACCHINA_MUTA; break; fi
  if aws s3 ls "s3://$BUCKET/risultati/fine.json" >/dev/null 2>&1; then esito=FINITA; sleep 90; break; fi
  [ "$m" -ge "$TETTO_MIN" ] && break
done
echo "6/6 esito: $esito; risultati"
aws s3 cp "s3://$BUCKET/risultati/" "$OUT/" --recursive --only-show-errors || echo "copia dei risultati fallita: la rifà recupera.sh"
m=$(( ($(date +%s) - T0) / MINUTO_S ))
MAC_MIN=$(( MAC_MIN + N_MAC * (m - m_prima + 1 + 10) ))
spesa "$m" | tee "$OUT/spesa.txt"
echo "{\"esito\": \"$esito\", \"minuti\": $m, \"copie\": \"$TIPO_REPLICA\", \"generatori\": \"$TIPO_GEN\", \"quaderni\": ${LOGS:-4}}" > "$OUT/giornata.json"
grep -rl "$Q8_ACCT" "$OUT" 2>/dev/null | xargs -r sed -i "s/$Q8_ACCT/<ACCOUNT_ID>/g"
{ echo "## Q8c giornata: $esito, $m minuti"; cat "$OUT/spesa.txt"; tail -5 "$OUT/passi-gen.log" 2>/dev/null; } >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
[ "$esito" = FINITA ]
