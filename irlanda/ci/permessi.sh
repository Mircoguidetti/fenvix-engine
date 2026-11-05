#!/bin/bash
# P1 Irlanda v2 (da M31 permessi.sh; in piu': il tipo di macchina grande e la quota di vCPU) · controllo dei permessi, 0 $ (GetMetricData: 1 metrica, circa 0,00001 $): letture, --dry-run e un bucket vuoto creato e cancellato subito.
# Scrive per ogni permesso "OK" o "MANCA", e alla fine l'elenco di quello che manca.
set -u
manca=()
if [ -n "${DEMO_DB_URI:-}" ]; then echo "OK    segreto GitHub DEMO_DB_URI presente"; else echo "MANCA segreto GitHub DEMO_DB_URI"; manca+=("segreto DEMO_DB_URI"); fi
prova() { local nome=$1; shift; if out=$("$@" 2>&1); then echo "OK    $nome"; else
  if grep -q DryRunOperation <<<"$out"; then echo "OK    $nome (dry-run)"; else echo "MANCA $nome: $(head -c 300 <<<"$out")"; manca+=("$nome"); fi; fi; }
prova "sts:GetCallerIdentity" aws sts get-caller-identity --query Arn --output text
prova "ssm:GetParameter (AMI Ubuntu 24.04 pubblica)" aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text
prova "ec2:DescribeVpcs" aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text
VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text 2>/dev/null || echo None)
[ "$VPC" != "None" ] && [ -n "$VPC" ] || { echo "MANCA VPC di serie in eu-west-1"; manca+=("VPC di serie"); }
prova "ec2:DescribeSubnets" aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true --query 'Subnets[0].SubnetId' --output text
prova "ec2:DescribeInstances" aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-coldread --query 'length(Reservations)'
prova "ec2:DescribeVolumes (controllo finale)" aws ec2 describe-volumes --filters Name=tag:project,Values=fenvix-coldread --query 'length(Volumes)'
prova "cloudwatch:ListMetrics (modo metriche)" aws cloudwatch list-metrics --namespace AWS/EC2 --metric-name CPUUtilization --max-items 1 --query 'length(Metrics)'
CW_FINE=$(date -u +%FT%TZ); CW_INIZIO=$(date -u -d '10 minutes ago' +%FT%TZ)
prova "cloudwatch:GetMetricData (modo metriche)" aws cloudwatch get-metric-data --start-time "$CW_INIZIO" --end-time "$CW_FINE" \
  --metric-data-queries '[{"Id":"q","MetricStat":{"Metric":{"Namespace":"AWS/EC2","MetricName":"CPUUtilization"},"Period":300,"Stat":"Average"}}]' --query 'length(MetricDataResults)'
prova "ec2:DescribeAddresses (controllo finale)" aws ec2 describe-addresses --filters Name=tag:project,Values=fenvix-coldread --query 'length(Addresses)'
prova "iam:GetInstanceProfile ($RUOLO_MACCHINA, creato a mano dal fondatore)" aws iam get-instance-profile --instance-profile-name "$RUOLO_MACCHINA" --query 'InstanceProfile.Roles[0].RoleName' --output text
SUBNET=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true --query 'sort_by(Subnets,&AvailabilityZone)[0].SubnetId' --output text 2>/dev/null)
AMI=$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text 2>/dev/null)
prova "ec2:RunInstances $TIPO con tag, ruolo e disco (iam:PassRole compreso)" aws ec2 run-instances --dry-run --image-id "$AMI" --instance-type "$TIPO" --subnet-id "$SUBNET" \
  --associate-public-ip-address --iam-instance-profile Name="$RUOLO_MACCHINA" --instance-initiated-shutdown-behavior terminate \
  --metadata-options HttpTokens=required,HttpEndpoint=enabled --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=${DISCO_GB:-20},VolumeType=gp3,DeleteOnTermination=true}" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=project,Value=fenvix-coldread},{Key=Name,Value=fenvix-coldread-irlanda}]" "ResourceType=volume,Tags=[{Key=project,Value=fenvix-coldread}]"
# la quota di vCPU on-demand (famiglie standard A, C, D, H, I, M, R, T, Z: codice L-1216C47A, DICHIARATO da AWS):
# il --dry-run di RunInstances NON la controlla. c7i.8xlarge = 32 vCPU, c7i.4xlarge = 16.
case "$TIPO" in c7i.8xlarge) servono=32;; c7i.4xlarge) servono=16;; c7i.2xlarge) servono=8;; *) servono=4;; esac
if q=$(aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A --query Quota.Value --output text 2>&1); then
  if python3 -c "import sys; sys.exit(0 if float(sys.argv[1]) >= float(sys.argv[2]) else 1)" "$q" "$servono" 2>/dev/null; then
    echo "OK    servicequotas:GetServiceQuota: quota vCPU on-demand standard $q, servono $servono ($TIPO)"
  else
    echo "MANCA quota vCPU: $q vCPU on-demand standard, ne servono $servono per $TIPO. Prova \"tipo\": \"c7i.4xlarge\" (16 vCPU) o chiedi l'aumento della quota (Service Quotas, L-1216C47A)"
    manca+=("quota vCPU $q < $servono")
  fi
else
  echo "MANCA servicequotas:GetServiceQuota: $(head -c 300 <<<"$q")"; manca+=("servicequotas:GetServiceQuota")
fi
# ec2:TerminateInstances non si prova qui: con la condizione sul tag un --dry-run su una macchina finta non dice niente.
P=fenvix-coldread-irl-permessi-${GITHUB_RUN_ID:-0}
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
