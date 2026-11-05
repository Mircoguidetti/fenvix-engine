#!/bin/bash
# Prova locale M31 (nomi coldread) · permessi.sh con IAM ACCESO in moto: un utente che ha come unica policy il blocco
# `fenvix-coldread-test` di PERMESSI.md (o un'altra policy data). 0 $, nessun contatto con AWS vero.
# Limite di moto: controlla la risorsa solo per S3; per EC2, IAM e SSM confronta la policy con la risorsa "*", quindi
# le regole con un ARN preciso (RunInstances, PassRole, GetInstanceProfile) qui risultano sempre "MANCA". Quelle le
# controlla controlla-policy.py. Questa prova serve per S3 (bucket fenvix-coldread-*) e per le letture.
# Uso: bash prova-permessi-iam.sh policy.json [porta]   (SCRIPT=controllo-finale.sh per provare il controllo finale)
set -u
POL=$(cd "$(dirname "$1")" && pwd)/$(basename "$1"); PORTA=${2:-5056}
G_DIR=$(cd "$(dirname "$0")/.." && pwd)
W=$(mktemp -d); mkdir -p "$W/bin"
cat > "$W/bin/aws" <<'EOF'
#!/bin/bash
# moto non ha il parametro pubblico di Canonical: un'AMI di moto letta prima di accendere IAM
if [ "$1 ${2:-}" = "ssm get-parameter" ]; then cat "$AMI_FILE"; exit 0; fi
exec /usr/local/bin/aws --endpoint-url "$MOTO_URL" "$@"
EOF
chmod +x "$W/bin/aws"
export MOTO_URL=http://127.0.0.1:$PORTA AWS_DEFAULT_REGION=eu-west-1 AWS_PAGER="" AWS_ACCESS_KEY_ID=fresca AWS_SECRET_ACCESS_KEY=fresca
pkill -f "moto_server -p $PORTA" 2>/dev/null; sleep 1
# le prime 8 chiamate (preparazione) senza controllo, poi IAM acceso per tutte
INITIAL_NO_AUTH_ACTION_COUNT=8 nohup moto_server -p "$PORTA" > "$W/moto.log" 2>&1 &
for i in $(seq 30); do curl -fs "$MOTO_URL/moto-api/" > /dev/null && break; sleep 0.5; done
A="/usr/local/bin/aws --endpoint-url $MOTO_URL"
$A iam create-role --role-name fenvix-coldread-machine --assume-role-policy-document '{"Version":"2012-10-17","Statement":[]}' > /dev/null
$A iam create-instance-profile --instance-profile-name fenvix-coldread-machine > /dev/null
$A iam add-role-to-instance-profile --instance-profile-name fenvix-coldread-machine --role-name fenvix-coldread-machine
$A ec2 describe-images --query 'Images[0].ImageId' --output text > "$W/ami.txt"
$A iam create-user --user-name gha > /dev/null
$A iam put-user-policy --user-name gha --policy-name fenvix-coldread-test --policy-document "file://$POL"
K=$($A iam create-access-key --user-name gha --output json)
export AWS_ACCESS_KEY_ID=$(jq -r .AccessKey.AccessKeyId <<<"$K") AWS_SECRET_ACCESS_KEY=$(jq -r .AccessKey.SecretAccessKey <<<"$K")
export AMI_FILE=$W/ami.txt PATH=$W/bin:$PATH TIPO=c7i.xlarge RUOLO_MACCHINA=fenvix-coldread-machine GITHUB_RUN_ID=iam
bash "$G_DIR/${SCRIPT:-permessi.sh}"; rc=$?
pkill -f "moto_server -p $PORTA" 2>/dev/null; rm -rf "$W"
exit $rc
