#!/usr/bin/env bash
# Giro 1 della micro-prova EFS (S13): SOLO controllo dei permessi, 0 $.
# Nessuna risorsa resta accesa: le chiamate sono letture, --dry-run di EC2, e (dove il dry-run non esiste)
# creazione e cancellazione immediata di un oggetto vuoto e gratuito (file system EFS vuoto, bucket S3 vuoto).
# Uscita: $OUT/permessi.txt (una riga per azione: OK / MANCA / NON SO) e $OUT/mancanti.txt (azioni IAM da aggiungere).
set -uo pipefail
OUT=${OUT:-risultati-efs}; mkdir -p "$OUT"
R=eu-west-1; export AWS_DEFAULT_REGION=$R AWS_PAGER=""
TIPO=c7g.large
SIM_SKIP=0
: > "$OUT/permessi.txt"; : > "$OUT/mancanti.txt"; : > "$OUT/dettagli.txt"
TAGS="ResourceType=instance,Tags=[{Key=Name,Value=fenvix-prova-efs-permessi},{Key=project,Value=fenvix-prova-efs},{Key=fenvix:prova,Value=efs}]"
ok()    { echo "OK    $1" | tee -a "$OUT/permessi.txt"; }
manca() { echo "MANCA $1" | tee -a "$OUT/permessi.txt"; echo "$1" >> "$OUT/mancanti.txt"; }
nonso() { echo "NONSO $1 ($2)" | tee -a "$OUT/permessi.txt"; }
# esito di una chiamata: 0 ok, 1 negato, 2 altro errore (scritto in dettagli, già mascherato dal runner)
prova() { # $1 = azione IAM, resto = comando
  local a=$1; shift
  local e; e=$("$@" 2>&1 >/dev/null); local rc=$?
  if [ $rc -eq 0 ] || grep -q "DryRunOperation" <<<"$e"; then ok "$a"; return 0; fi
  echo "[$a] $(head -c 400 <<<"$e" | tr '\n' ' ')" >> "$OUT/dettagli.txt"
  if grep -qiE "AccessDenied|UnauthorizedOperation|not authorized|AuthorizationError|UnauthorizedAccess" <<<"$e"; then manca "$a"; return 1; fi
  nonso "$a" "errore diverso da 'negato', vedi dettagli"; return 2
}

echo "== chi sono (mascherato)"
aws sts get-caller-identity --query Arn --output text >/dev/null && ok "sts:GetCallerIdentity" || { echo "credenziali non valide"; exit 1; }

echo "== letture"
prova elasticfilesystem:DescribeFileSystems aws efs describe-file-systems --query 'length(FileSystems)'
echo "file system EFS già presenti in $R: $(aws efs describe-file-systems --query 'length(FileSystems)' --output text 2>/dev/null || echo NON-SO)" | tee -a "$OUT/dettagli.txt"
prova ec2:DescribeVpcs aws ec2 describe-vpcs --filters Name=isDefault,Values=true
prova ec2:DescribeSubnets aws ec2 describe-subnets --filters Name=default-for-az,Values=true
prova ec2:DescribeImages aws ec2 describe-images --owners amazon --filters 'Name=name,Values=al2023-ami-2023.*-kernel-*-arm64' --query 'Images[0].ImageId'
prova ec2:DescribeInstances aws ec2 describe-instances --max-items 5
prova ec2:DescribeSecurityGroups aws ec2 describe-security-groups --max-items 5
prova ec2:DescribeInstanceTypeOfferings aws ec2 describe-instance-type-offerings --location-type availability-zone --filters Name=instance-type,Values=$TIPO
prova cloudwatch:GetMetricStatistics aws cloudwatch get-metric-statistics --namespace AWS/EFS --metric-name TotalIOBytes --dimensions Name=FileSystemId,Value=fs-00000000 --statistics Sum --period 60 --start-time "$(date -u -d '-10 min' +%FT%TZ)" --end-time "$(date -u +%FT%TZ)"
prova servicequotas:GetServiceQuota aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A
prova pricing:GetProducts aws pricing get-products --region us-east-1 --service-code AmazonEFS --max-items 1

VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text 2>/dev/null)
AMI=$(aws ec2 describe-images --owners amazon --filters 'Name=name,Values=al2023-ami-2023.*-kernel-*-arm64' --query 'sort_by(Images,&CreationDate)[-1].ImageId' --output text 2>/dev/null)
echo "vpc di default: ${VPC:-NON-SO}; ami arm64: ${AMI:-NON-SO}" | tee -a "$OUT/dettagli.txt"
echo "zone che offrono $TIPO: $(aws ec2 describe-instance-type-offerings --location-type availability-zone --filters Name=instance-type,Values=$TIPO --query 'InstanceTypeOfferings[].Location' --output text 2>/dev/null)" | tee -a "$OUT/dettagli.txt"
echo "quota vCPU on-demand standard: $(aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A --query Quota.Value --output text 2>/dev/null || echo NON-SO)" | tee -a "$OUT/dettagli.txt"

echo "== EC2 con --dry-run (nessuna istanza parte)"
for Z in a b c; do
  SN=$(aws ec2 describe-subnets --filters Name=default-for-az,Values=true Name=availability-zone,Values=$R$Z --query 'Subnets[0].SubnetId' --output text 2>/dev/null)
  if [ -z "$SN" ] || [ "$SN" = None ] || [ -z "$AMI" ] || [ "$AMI" = None ]; then nonso "ec2:RunInstances ($R$Z)" "manca subnet o AMI"; continue; fi
  prova "ec2:RunInstances ($TIPO, $R$Z, con tag)" aws ec2 run-instances --dry-run --image-id "$AMI" --instance-type $TIPO --subnet-id "$SN" \
    --instance-initiated-shutdown-behavior terminate --tag-specifications "$TAGS" "ResourceType=volume,Tags=[{Key=project,Value=fenvix-prova-efs}]"
done
SN=$(aws ec2 describe-subnets --filters Name=default-for-az,Values=true Name=availability-zone,Values=${R}a --query 'Subnets[0].SubnetId' --output text 2>/dev/null)
# profilo di istanza per i nodi (S3 dei risultati): esiste? posso passarlo?
e=$(aws ec2 run-instances --dry-run --image-id "$AMI" --instance-type $TIPO --subnet-id "$SN" --iam-instance-profile Name=fenvix-prova-efs-ec2 \
    --tag-specifications "$TAGS" 2>&1); echo "[profilo] $(head -c 300 <<<"$e")" >> "$OUT/dettagli.txt"
if grep -q DryRunOperation <<<"$e"; then ok "iam:PassRole (profilo fenvix-prova-efs-ec2)";
elif grep -qi "Invalid IAM Instance Profile" <<<"$e"; then manca "profilo di istanza fenvix-prova-efs-ec2 (non esiste)";
elif grep -qiE "UnauthorizedOperation|not authorized" <<<"$e"; then manca "iam:PassRole su role/fenvix-prova-efs-ec2 (o ec2:RunInstances)";
else nonso "iam:PassRole (profilo)" "vedi dettagli"; fi
prova "ec2:CreateSecurityGroup (con tag)" aws ec2 create-security-group --dry-run --group-name fenvix-prova-efs-permessi --description permessi --vpc-id "$VPC" \
  --tag-specifications 'ResourceType=security-group,Tags=[{Key=project,Value=fenvix-prova-efs}]'

echo "== EFS: niente dry-run, quindi creo un file system vuoto e lo cancello subito (0 $)"
FS=$(aws efs create-file-system --creation-token "fenvix-prova-efs-permessi-$GITHUB_RUN_ID" --throughput-mode elastic --encrypted \
     --tags Key=Name,Value=fenvix-prova-efs-permessi Key=project,Value=fenvix-prova-efs --query FileSystemId --output text 2>"$OUT/err.txt")
if [ -n "$FS" ] && [ "$FS" != None ]; then
  ok "elasticfilesystem:CreateFileSystem (+ TagResource)"
  for i in $(seq 1 30); do s=$(aws efs describe-file-systems --file-system-id "$FS" --query 'FileSystems[0].LifeCycleState' --output text 2>/dev/null); [ "$s" = available ] && break; sleep 2; done
  prova elasticfilesystem:PutLifecycleConfiguration aws efs put-lifecycle-configuration --file-system-id "$FS" --lifecycle-policies '[]'
  prova elasticfilesystem:DescribeMountTargets aws efs describe-mount-targets --file-system-id "$FS"
  # mount target: provo a crearlo (gratis) e lo cancello subito
  e=$(aws efs create-mount-target --file-system-id "$FS" --subnet-id "$SN" --query MountTargetId --output text 2>&1); rc=$?
  if [ $rc -eq 0 ]; then ok "elasticfilesystem:CreateMountTarget"; MT=$e
    for i in $(seq 1 60); do s=$(aws efs describe-mount-targets --mount-target-id "$MT" --query 'MountTargets[0].LifeCycleState' --output text 2>/dev/null); [ "$s" = available ] && break; sleep 5; done
    prova elasticfilesystem:DeleteMountTarget aws efs delete-mount-target --mount-target-id "$MT"
    for i in $(seq 1 60); do n=$(aws efs describe-mount-targets --file-system-id "$FS" --query 'length(MountTargets)' --output text 2>/dev/null); [ "$n" = 0 ] && break; sleep 5; done
  else echo "[CreateMountTarget] $(head -c 300 <<<"$e")" >> "$OUT/dettagli.txt"
    if grep -qiE "AccessDenied|not authorized|Unauthorized" <<<"$e"; then manca "elasticfilesystem:CreateMountTarget (e ec2:CreateNetworkInterface, ec2:DescribeNetworkInterfaces, ec2:DeleteNetworkInterface)"; else nonso elasticfilesystem:CreateMountTarget "vedi dettagli"; fi
  fi
  # update-file-system a Provisioned: lo provo solo con la simulazione IAM (sul vero cambia il prezzo e blocca 24 ore)
  for t in 1 2 3 4 5 6 7 8 9 10; do
    e=$(aws efs delete-file-system --file-system-id "$FS" 2>&1) && { ok elasticfilesystem:DeleteFileSystem; break; }
    if grep -qiE "AccessDenied|not authorized" <<<"$e"; then manca elasticfilesystem:DeleteFileSystem; echo "ALLARME: file system di prova $FS non cancellato" | tee -a "$OUT/dettagli.txt"; break; fi
    sleep 10
  done
else
  e=$(cat "$OUT/err.txt"); echo "[CreateFileSystem] $(head -c 300 <<<"$e")" >> "$OUT/dettagli.txt"
  if grep -qiE "AccessDenied|not authorized" <<<"$e"; then manca "elasticfilesystem:CreateFileSystem"; manca "elasticfilesystem:TagResource"; else nonso elasticfilesystem:CreateFileSystem "vedi dettagli"; fi
fi
rm -f "$OUT/err.txt"

echo "== S3: bucket dei risultati, creato vuoto e cancellato subito (0 $)"
B="fenvix-prova-efs-permessi-$GITHUB_RUN_ID"
if prova s3:CreateBucket aws s3api create-bucket --bucket "$B" --create-bucket-configuration LocationConstraint=$R; then
  prova s3:PutObject aws s3api put-object --bucket "$B" --key prova.txt
  prova s3:GetObject aws s3api get-object --bucket "$B" --key prova.txt /dev/null
  prova s3:ListBucket aws s3api list-objects-v2 --bucket "$B"
  prova s3:DeleteObject aws s3api delete-object --bucket "$B" --key prova.txt
  prova s3:DeleteBucket aws s3api delete-bucket --bucket "$B"
fi

echo "== simulazione IAM (dice anche le azioni che non si possono provare dal vivo)"
ACTIONS="ec2:RunInstances ec2:TerminateInstances ec2:CreateTags ec2:CreateSecurityGroup ec2:AuthorizeSecurityGroupIngress ec2:DeleteSecurityGroup ec2:DescribeNetworkInterfaces ec2:CreateNetworkInterface ec2:DeleteNetworkInterface elasticfilesystem:CreateFileSystem elasticfilesystem:TagResource elasticfilesystem:CreateMountTarget elasticfilesystem:DeleteMountTarget elasticfilesystem:DeleteFileSystem elasticfilesystem:UpdateFileSystem elasticfilesystem:DescribeFileSystems elasticfilesystem:DescribeMountTargets elasticfilesystem:PutBackupPolicy cloudwatch:GetMetricStatistics iam:PassRole"
e=$(aws iam simulate-principal-policy --policy-source-arn "$WB_ROLE" --action-names $ACTIONS --query 'EvaluationResults[].[EvalActionName,EvalDecision]' --output text 2>&1)
if [ $? -eq 0 ]; then echo "$e" | sed 's/^/SIM   /' | tee -a "$OUT/permessi.txt"
else echo "SIM   non disponibile (il ruolo non ha iam:SimulatePrincipalPolicy): uso solo le prove dal vivo" | tee -a "$OUT/permessi.txt"; fi

echo; echo "== RIASSUNTO"
n=$(grep -c . "$OUT/mancanti.txt" || true)
if [ "$n" -eq 0 ]; then echo "PERMESSI: tutti quelli provati ci sono" | tee "$OUT/esito.txt"; else echo "PERMESSI: ne mancano $n" | tee "$OUT/esito.txt"; cat "$OUT/mancanti.txt"; fi
echo "Nessuna risorsa lasciata accesa da questo giro: $(aws efs describe-file-systems --query "length(FileSystems[?starts_with(Name||'',\`fenvix-prova-efs\`)])" --output text 2>/dev/null || echo NON-SO) file system fenvix-prova-efs rimasti" | tee -a "$OUT/esito.txt"
