#!/bin/bash
# Cucina minima (K2; da P1 Irlanda permessi.sh) · controllo dei permessi, 0 $: letture, --dry-run, un bucket vuoto creato
# e cancellato subito. In piu' rispetto all'Irlanda: TUTTE le taglie del server ammesse (anche Graviton, immagine
# arm64), i gruppi di sicurezza (creati e cancellati davvero: sono gratis), e la scelta della macchina cliente secondo la quota di vCPU.
# Scrive per ogni permesso "OK" o "MANCA", e alla fine l'elenco di quello che manca.
# Variabili: TAGLIE (lista JSON delle taglie della giornata), CLIENTE_CHIESTO e COPIE_CHIESTE ("auto" o un valore),
# RUOLO_MACCHINA, DISCO_GB. Se GITHUB_ENV c'e', ci scrive CLIENTE e COPIE scelti.
set -u
QUI=$(cd "$(dirname "$0")" && pwd)
manca=()
prova() { local nome=$1; shift; if out=$("$@" 2>&1); then echo "OK    $nome"; else
  if grep -q DryRunOperation <<<"$out"; then echo "OK    $nome (dry-run)"; else echo "MANCA $nome: $(head -c 300 <<<"$out")"; manca+=("$nome"); fi; fi; }
prova "sts:GetCallerIdentity" aws sts get-caller-identity --query Arn --output text
prova "ssm:GetParameter (AMI Ubuntu 24.04 amd64)" aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text
prova "ssm:GetParameter (AMI Ubuntu 24.04 arm64, Graviton)" aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text
prova "ec2:DescribeVpcs" aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text
VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text 2>/dev/null || echo None)
[ "$VPC" != "None" ] && [ -n "$VPC" ] || { echo "MANCA VPC di serie in eu-west-1"; manca+=("VPC di serie"); }
prova "ec2:DescribeSubnets" aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true --query 'Subnets[0].SubnetId' --output text
prova "ec2:DescribeInstances" aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-coldread --query 'length(Reservations)'
prova "ec2:DescribeVolumes (controllo finale)" aws ec2 describe-volumes --filters Name=tag:project,Values=fenvix-coldread --query 'length(Volumes)'
prova "ec2:DescribeAddresses (controllo finale)" aws ec2 describe-addresses --filters Name=tag:project,Values=fenvix-coldread --query 'length(Addresses)'
prova "ec2:DescribeSecurityGroups (pulizia e controllo finale)" aws ec2 describe-security-groups --filters Name=tag:project,Values=fenvix-coldread --query 'length(SecurityGroups)'
# gruppi di sicurezza: si creano davvero (sono gratis), si apre la porta dall'uno all'altro e si cancellano subito
G="fenvix-coldread-cm-permessi-${GITHUB_RUN_ID:-0}"
prova "ec2:CreateSecurityGroup con il tag (cliente)" aws ec2 create-security-group --group-name "$G-cliente" --description "cucina minima prova permessi" \
  --vpc-id "$VPC" --tag-specifications "ResourceType=security-group,Tags=[{Key=project,Value=fenvix-coldread}]"
prova "ec2:CreateSecurityGroup con il tag (server)" aws ec2 create-security-group --group-name "$G-server" --description "cucina minima prova permessi" \
  --vpc-id "$VPC" --tag-specifications "ResourceType=security-group,Tags=[{Key=project,Value=fenvix-coldread}]"
gid() { aws ec2 describe-security-groups --filters Name=group-name,Values="$1" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null; }
GC=$(gid "$G-cliente"); GS=$(gid "$G-server")
prova "ec2:AuthorizeSecurityGroupIngress (27017 solo dal gruppo cliente)" aws ec2 authorize-security-group-ingress --group-id "$GS" \
  --ip-permissions "IpProtocol=tcp,FromPort=27017,ToPort=27017,UserIdGroupPairs=[{GroupId=$GC}]"
prova "ec2:DeleteSecurityGroup (server)" aws ec2 delete-security-group --group-id "$GS"
prova "ec2:DeleteSecurityGroup (cliente)" aws ec2 delete-security-group --group-id "$GC"
prova "iam:GetInstanceProfile ($RUOLO_MACCHINA)" aws iam get-instance-profile --instance-profile-name "$RUOLO_MACCHINA" --query 'InstanceProfile.Roles[0].RoleName' --output text
SUBNET=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true --query 'sort_by(Subnets,&AvailabilityZone)[0].SubnetId' --output text 2>/dev/null)
declare -A AMI
AMI[x86_64]=$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text 2>/dev/null)
AMI[aarch64]=$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text 2>/dev/null)
# tutte le taglie ammesse (server) e tutte le macchine cliente: lancio a secco, con tag, ruolo e disco (iam:PassRole compreso)
for t in c7i.large c7i.xlarge m7i.large c7g.medium c7g.large m7g.large t4g.small c7i.2xlarge c7i.4xlarge c7i.8xlarge; do
  prova "ec2:RunInstances $t ($(python3 "$QUI/tipi.py" arch "$t")) con tag, ruolo e disco" aws ec2 run-instances --dry-run \
    --image-id "${AMI[$(python3 "$QUI/tipi.py" arch "$t")]}" --instance-type "$t" --subnet-id "$SUBNET" \
    --associate-public-ip-address --iam-instance-profile Name="$RUOLO_MACCHINA" --instance-initiated-shutdown-behavior terminate \
    --metadata-options HttpTokens=required,HttpEndpoint=enabled --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=${DISCO_GB:-20},VolumeType=gp3,DeleteOnTermination=true}" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=project,Value=fenvix-coldread},{Key=Name,Value=fenvix-coldread-cm-permessi}]" "ResourceType=volume,Tags=[{Key=project,Value=fenvix-coldread}]"
done
# la quota di vCPU on-demand (famiglie standard A, C, D, H, I, M, R, T, Z, Graviton comprese: codice L-1216C47A,
# DICHIARATO da AWS). Il --dry-run di RunInstances NON la controlla. Nella giornata sono accese insieme la macchina
# cliente e UNA macchina server (le taglie vanno una alla volta).
if q=$(aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A --query Quota.Value --output text 2>&1); then
  if scelta=$(python3 "$QUI/tipi.py" cliente "$q" "${TAGLIE:-[\"c7i.large\"]}" "${CLIENTE_CHIESTO:-auto}" "${COPIE_CHIESTE:-auto}"); then
    read -r cli copie servono <<<"$scelta"
    echo "OK    servicequotas:GetServiceQuota: quota vCPU on-demand standard $q; cliente $cli con $copie copie, servono $servono vCPU insieme (taglie ${TAGLIE:-})"
    [ -n "${GITHUB_ENV:-}" ] && { echo "CLIENTE=$cli"; echo "COPIE=$copie"; } >> "$GITHUB_ENV"
    [ "$cli" = c7i.xlarge ] && echo "AVVISO: con il cliente c7i.xlarge (4 vCPU, $copie copie) il carico massimo rischia di misurare il cliente e non il server: il verdetto di somma_massimo.py lo dice. Con la quota a 16: c7i.2xlarge e 5 copie."
  else
    echo "MANCA quota vCPU: $scelta. Chiedi l'aumento (Service Quotas, L-1216C47A) o togli le taglie piu' grandi"
    manca+=("quota vCPU $q")
  fi
else
  echo "MANCA servicequotas:GetServiceQuota: $(head -c 300 <<<"$q")"; manca+=("servicequotas:GetServiceQuota")
fi
# ec2:TerminateInstances non si prova qui: con la condizione sul tag un --dry-run su una macchina finta non dice niente.
P=fenvix-coldread-cm-permessi-${GITHUB_RUN_ID:-0}
prova "s3:CreateBucket" aws s3api create-bucket --bucket "$P" --create-bucket-configuration LocationConstraint=eu-west-1
prova "s3:PutBucketPublicAccessBlock" aws s3api put-public-access-block --bucket "$P" --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
prova "s3:PutBucketTagging" aws s3api put-bucket-tagging --bucket "$P" --tagging "TagSet=[{Key=project,Value=fenvix-coldread}]"
prova "s3:PutLifecycleConfiguration" aws s3api put-bucket-lifecycle-configuration --bucket "$P" --lifecycle-configuration '{"Rules":[{"ID":"scade","Status":"Enabled","Filter":{},"Expiration":{"Days":1}}]}'
prova "s3:PutObject" bash -c "echo ok | aws s3 cp - s3://$P/prova.txt"
prova "s3:GetObject" aws s3 cp "s3://$P/prova.txt" -
prova "s3:ListBucket" aws s3 ls "s3://$P/"
prova "s3:DeleteObject" aws s3 rm "s3://$P/prova.txt"
prova "s3:DeleteBucket" aws s3api delete-bucket --bucket "$P"
prova "s3:ListAllMyBuckets" aws s3api list-buckets --query 'length(Buckets)'
if [ ${#manca[@]} -eq 0 ]; then echo "PERMESSI: tutti presenti" | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"; else
  { echo "PERMESSI MANCANTI:"; printf -- '- %s\n' "${manca[@]}"; } | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"; exit 1; fi
