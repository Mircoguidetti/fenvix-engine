#!/bin/bash
# M31 · la giornata su AWS, parte che si lancia da AWS CloudShell (regione eu-west-1, Irlanda).
#   bash giornata.sh parti      crea bucket, ruolo e macchina e fa partire la prova (circa 2 minuti)
#   bash giornata.sh stato      dice a che punto è (si può lanciare quante volte si vuole)
#   bash giornata.sh raccogli   scarica i risultati in ~/m31-risultati.tar.gz
#   bash giornata.sh smonta     cancella tutto: macchina, ruolo, bucket (chiede conferma scrivendo SI)
# Niente chiavi nei file: usa le credenziali di CloudShell. Tutto porta il tag progetto=fenvix-m31.
set -euo pipefail
export AWS_REGION=eu-west-1 AWS_DEFAULT_REGION=eu-west-1 AWS_PAGER=""
QUI="$(cd "$(dirname "$0")" && pwd)"
STATO=~/.m31-giornata
TAG="Key=progetto,Value=fenvix-m31"
TIPO=c7i.xlarge
RUOLO=fenvix-m31-macchina

bucket() { cat "$STATO/bucket" 2>/dev/null || true; }

parti() {
  mkdir -p "$STATO"
  [ -f "$QUI/m31-giornata.tar.gz" ] || { echo "Manca m31-giornata.tar.gz accanto a questo script."; exit 1; }
  if [ -n "$(bucket)" ]; then echo "Una giornata è già partita (bucket $(bucket)). Usa: stato, raccogli o smonta."; exit 1; fi
  aws sts get-caller-identity --query Arn --output text >/dev/null || { echo "CloudShell non ha credenziali."; exit 1; }
  B="fenvix-m31-$(date +%Y%m%d)-$(tr -dc a-z0-9 </dev/urandom | head -c 6)"
  echo "1/6 bucket $B (eu-west-1, privato, oggetti cancellati da soli dopo 1 giorno)"
  aws s3api create-bucket --bucket "$B" --create-bucket-configuration LocationConstraint=eu-west-1 >/dev/null
  echo "$B" > "$STATO/bucket"
  aws s3api put-public-access-block --bucket "$B" --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
  aws s3api put-bucket-tagging --bucket "$B" --tagging "TagSet=[{Key=progetto,Value=fenvix-m31}]"
  aws s3api put-bucket-lifecycle-configuration --bucket "$B" --lifecycle-configuration '{"Rules":[{"ID":"scade","Status":"Enabled","Filter":{},"Expiration":{"Days":1},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":1}}]}'
  echo "2/6 carico il programma nel bucket"
  aws s3 cp "$QUI/m31-giornata.tar.gz" "s3://$B/programma/m31-giornata.tar.gz" --only-show-errors
  URL=$(aws s3 presign "s3://$B/programma/m31-giornata.tar.gz" --expires-in 21600)
  echo "3/6 ruolo $RUOLO (può solo leggere e scrivere in questo bucket)"
  if ! aws iam get-role --role-name "$RUOLO" >/dev/null 2>&1; then
    aws iam create-role --role-name "$RUOLO" --tags "$TAG" --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null
  fi
  aws iam put-role-policy --role-name "$RUOLO" --policy-name solo-bucket-m31 --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:PutObject\",\"s3:DeleteObject\",\"s3:ListBucket\",\"s3:AbortMultipartUpload\"],\"Resource\":[\"arn:aws:s3:::$B\",\"arn:aws:s3:::$B/*\"]}]}"
  if ! aws iam get-instance-profile --instance-profile-name "$RUOLO" >/dev/null 2>&1; then
    aws iam create-instance-profile --instance-profile-name "$RUOLO" --tags "$TAG" >/dev/null
    aws iam add-role-to-instance-profile --instance-profile-name "$RUOLO" --role-name "$RUOLO"
    echo "   aspetto 15 secondi che il ruolo sia pronto"; sleep 15
  fi
  echo "4/6 rete: VPC di serie, prima zona"
  VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
  [ "$VPC" != "None" ] || { echo "Non c'è una VPC di serie in eu-west-1: fermati e scrivilo al coordinatore."; exit 1; }
  SUBNET=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true --query 'sort_by(Subnets,&AvailabilityZone)[0].SubnetId' --output text)
  AMI=$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text)
  UD=$(mktemp)
  cat > "$UD" <<UDEOF
#!/bin/bash
mkdir -p /opt/m31 && cd /opt/m31
for i in 1 2 3 4 5; do curl -fsS -o g.tgz "$URL" && break; sleep 10; done
tar xzf g.tgz && chmod +x m31 esegui.sh
nohup ./esegui.sh "$B" > esegui.log 2>&1 &
UDEOF
  echo "5/6 macchina $TIPO (si spegne e si cancella da sola alla fine, al massimo dopo 4 ore)"
  ID=$(aws ec2 run-instances --image-id "$AMI" --instance-type "$TIPO" --subnet-id "$SUBNET" --associate-public-ip-address \
    --iam-instance-profile Name="$RUOLO" --instance-initiated-shutdown-behavior terminate \
    --metadata-options HttpTokens=required,HttpEndpoint=enabled \
    --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=60,VolumeType=gp3,DeleteOnTermination=true}' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=progetto,Value=fenvix-m31},{Key=Name,Value=fenvix-m31}]" "ResourceType=volume,Tags=[{Key=progetto,Value=fenvix-m31}]" \
    --user-data "file://$UD" --query 'Instances[0].InstanceId' --output text)
  rm -f "$UD"
  echo "$ID" > "$STATO/macchina"
  echo "6/6 partita: macchina $ID. Tra circa 80 minuti lancia: bash giornata.sh stato"
}

stato() {
  B=$(bucket); [ -n "$B" ] || { echo "Nessuna giornata partita."; exit 0; }
  ID=$(cat "$STATO/macchina" 2>/dev/null || echo "?")
  echo "macchina $ID: $(aws ec2 describe-instances --instance-ids "$ID" --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || echo 'non trovata (già cancellata)')"
  echo "ultimi passi:"; aws s3 cp "s3://$B/risultati/passi.log" - 2>/dev/null | tail -5 || echo "  (ancora nessuno: la macchina si sta preparando)"
  if aws s3 ls "s3://$B/risultati/fine.json" >/dev/null 2>&1; then echo "FINITA: lancia bash giornata.sh raccogli"; fi
}

raccogli() {
  B=$(bucket); [ -n "$B" ] || { echo "Nessuna giornata partita."; exit 1; }
  rm -rf ~/m31-risultati && mkdir -p ~/m31-risultati
  aws s3 cp "s3://$B/risultati/" ~/m31-risultati/ --recursive --only-show-errors
  (cd ~ && tar czf m31-risultati.tar.gz m31-risultati)
  echo "Pronto: ~/m31-risultati.tar.gz ($(du -h ~/m31-risultati.tar.gz | cut -f1)). Scaricalo con Actions > Download file, percorso /home/cloudshell-user/m31-risultati.tar.gz"
}

smonta() {
  B=$(bucket)
  read -r -p "Cancello macchina, ruolo e bucket della giornata M31? Scrivi SI: " ok; [ "$ok" = "SI" ] || { echo "Niente cancellato."; exit 0; }
  for ID in $(aws ec2 describe-instances --filters Name=tag:progetto,Values=fenvix-m31 Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId' --output text); do
    aws ec2 terminate-instances --instance-ids "$ID" >/dev/null && echo "macchina $ID cancellata"; done
  aws iam remove-role-from-instance-profile --instance-profile-name "$RUOLO" --role-name "$RUOLO" 2>/dev/null || true
  aws iam delete-instance-profile --instance-profile-name "$RUOLO" 2>/dev/null || true
  aws iam delete-role-policy --role-name "$RUOLO" --policy-name solo-bucket-m31 2>/dev/null || true
  aws iam delete-role --role-name "$RUOLO" 2>/dev/null && echo "ruolo cancellato" || true
  if [ -n "$B" ]; then aws s3 rb "s3://$B" --force >/dev/null && echo "bucket $B cancellato"; fi
  rm -rf "$STATO"
  echo "Controllo: macchine rimaste $(aws ec2 describe-instances --filters Name=tag:progetto,Values=fenvix-m31 Name=instance-state-name,Values=pending,running,stopping,stopped --query 'length(Reservations[].Instances[])' --output text); bucket rimasti: $(aws s3 ls | grep -c fenvix-m31 || true)"
}

case "${1:-}" in
  parti) parti ;; stato) stato ;; raccogli) raccogli ;; smonta) smonta ;;
  *) echo "uso: bash giornata.sh parti | stato | raccogli | smonta"; exit 1 ;;
esac
