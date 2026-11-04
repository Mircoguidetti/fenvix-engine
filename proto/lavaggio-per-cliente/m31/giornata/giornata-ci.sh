#!/bin/bash
# M31 · la giornata da GitHub Actions: bucket nuovo, programma nel bucket, una c7i.xlarge in eu-west-1 che fa esegui.sh,
# attesa con la spesa stimata ogni minuto, risultati in /tmp/m31-out. La pulizia è in pulizia.sh (gira sempre).
set -uo pipefail
OUT=/tmp/m31-out; mkdir -p "$OUT"
T0=$(date +%s); TETTO_MIN=${TETTO_MIN:-250}; ULTIMO_RINNOVO=$T0
rinnova() { # credenziali OIDC nuove per un'altra ora, senza uscire dallo step
  local tok j
  tok=$(curl -fsS -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=sts.amazonaws.com" | jq -r .value) || return 1
  j=$(aws sts assume-role-with-web-identity --role-arn "$M31_ROLE" --role-session-name m31-giornata --web-identity-token "$tok" --duration-seconds 3600 --output json) || return 1
  export AWS_ACCESS_KEY_ID=$(jq -r .Credentials.AccessKeyId <<<"$j") AWS_SECRET_ACCESS_KEY=$(jq -r .Credentials.SecretAccessKey <<<"$j") AWS_SESSION_TOKEN=$(jq -r .Credentials.SessionToken <<<"$j")
  echo "::add-mask::$AWS_SECRET_ACCESS_KEY"; echo "::add-mask::$AWS_SESSION_TOKEN"
  ULTIMO_RINNOVO=$(date +%s)
}
spesa() { # stima per eccesso: macchina, disco 60 GB gp3, IPv4 pubblico, richieste S3 al tetto previsto dal programma
  python3 - "$1" "$PREZZO_ORA" <<'PY'
import sys; m=float(sys.argv[1]); p=float(sys.argv[2]); h=m/60
ec2=h*p; ebs=60*0.088*h/730; ip=0.005*h; s3=min(0.40, 0.0016*m)  # richieste: circa 0,25-0,40 $ in tutta la giornata
print(f"SPESA: {ec2+ebs+ip+s3:.2f} $ finora (stima: macchina {ec2:.2f}, disco {ebs:.3f}, IPv4 {ip:.3f}, S3 {s3:.2f}), tetto 1,50 $")
PY
}
echo "1/5 bucket $BUCKET"
aws s3api create-bucket --bucket "$BUCKET" --create-bucket-configuration LocationConstraint=eu-west-1 >/dev/null || exit 1
aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-tagging --bucket "$BUCKET" --tagging "TagSet=[{Key=progetto,Value=fenvix-m31}]"
aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" --lifecycle-configuration '{"Rules":[{"ID":"scade","Status":"Enabled","Filter":{},"Expiration":{"Days":1},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":1}}]}'
echo "2/5 programma nel bucket"
aws s3 cp /tmp/m31-giornata.tar.gz "s3://$BUCKET/programma/m31-giornata.tar.gz" --only-show-errors || exit 1
URL=$(aws s3 presign "s3://$BUCKET/programma/m31-giornata.tar.gz" --expires-in 7200)
echo "3/5 rete e immagine"
VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
SUBNET=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true --query 'sort_by(Subnets,&AvailabilityZone)[0].SubnetId' --output text)
AMI=$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text)
UD=$(mktemp)
cat > "$UD" <<UDEOF
#!/bin/bash
mkdir -p /opt/m31 && cd /opt/m31
for i in 1 2 3 4 5; do curl -fsS -o g.tgz "$URL" && break; sleep 10; done
tar xzf g.tgz && chmod +x m31 esegui.sh
nohup ./esegui.sh "$BUCKET" > esegui.log 2>&1 &
UDEOF
echo "4/5 macchina $TIPO in $SUBNET (si cancella da sola alla fine, al massimo dopo 240 minuti)"
ID=$(aws ec2 run-instances --image-id "$AMI" --instance-type "$TIPO" --subnet-id "$SUBNET" --associate-public-ip-address \
  --iam-instance-profile Name="$RUOLO_MACCHINA" --instance-initiated-shutdown-behavior terminate \
  --metadata-options HttpTokens=required,HttpEndpoint=enabled \
  --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=60,VolumeType=gp3,DeleteOnTermination=true}' \
  --tag-specifications "ResourceType=instance,Tags=[{Key=progetto,Value=fenvix-m31},{Key=Name,Value=fenvix-m31}]" "ResourceType=volume,Tags=[{Key=progetto,Value=fenvix-m31}]" \
  --user-data "file://$UD" --query 'Instances[0].InstanceId' --output text) || exit 1
rm -f "$UD"
echo "macchina: $ID"
echo "5/5 attesa (ogni minuto: passo, spesa)"
esito=TETTO
while :; do
  sleep 60
  m=$(( ($(date +%s) - T0) / 60 ))
  [ $(( $(date +%s) - ULTIMO_RINNOVO )) -gt 2700 ] && { rinnova || echo "rinnovo delle credenziali fallito"; }
  spesa "$m"
  aws s3 cp "s3://$BUCKET/risultati/passi.log" - 2>/dev/null | tail -1
  if aws s3 ls "s3://$BUCKET/risultati/fine.json" >/dev/null 2>&1; then esito=FINITA; break; fi
  st=$(aws ec2 describe-instances --instance-ids "$ID" --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null)
  if [ "$st" = "terminated" ] || [ "$st" = "shutting-down" ]; then sleep 30; aws s3 ls "s3://$BUCKET/risultati/fine.json" >/dev/null 2>&1 && esito=FINITA || esito=MACCHINA_SPENTA; break; fi
  [ "$m" -ge "$TETTO_MIN" ] && break
done
echo "esito: $esito"
aws s3 cp "s3://$BUCKET/risultati/" "$OUT/" --recursive --only-show-errors
m=$(( ($(date +%s) - T0) / 60 ))
spesa "$m" | tee "$OUT/spesa.txt"
echo "{\"esito\": \"$esito\", \"minuti\": $m, \"macchina\": \"$TIPO\"}" > "$OUT/giornata.json"
grep -rl "$M31_ACCT" "$OUT" 2>/dev/null | xargs -r sed -i "s/$M31_ACCT/<ACCOUNT_ID>/g"
{ echo "## M31 giornata: $esito, $m minuti"; cat "$OUT/spesa.txt"; tail -20 "$OUT/passi.log" 2>/dev/null; } >> "$GITHUB_STEP_SUMMARY"
[ "$esito" = FINITA ]
