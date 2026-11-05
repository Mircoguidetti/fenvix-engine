# Prova locale M31 · ambiente comune: moto (S3, EC2, IAM, SSM finti) su una porta locale, `aws` finto in testa al PATH,
# credenziali finte, nessun contatto con AWS vero. Si usa con `source`.
QUI_PL=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
G_DIR=$(cd "$QUI_PL/.." && pwd)
M_DIR=$(cd "$G_DIR/.." && pwd)
PORTA=${PORTA:-5055}
export MOTO_URL=http://127.0.0.1:$PORTA
export AWS_DEFAULT_REGION=eu-west-1 AWS_REGION=eu-west-1 AWS_PAGER=""
export AWS_ACCESS_KEY_ID=fresca AWS_SECRET_ACCESS_KEY=fresca
unset AWS_SESSION_TOKEN AWS_PROFILE
# per il programma m31 (object_store): lo stesso moto
export AWS_ENDPOINT=$MOTO_URL AWS_ALLOW_HTTP=true
# come nel workflow
export TIPO=c7i.xlarge PREZZO_ORA=0.1915 RUOLO_MACCHINA=fenvix-coldread-machine
export M31_ACCT=123456789012 M31_ROLE=arn:aws:iam::123456789012:role/finto

avvia_moto() {
  if ! curl -fs "$MOTO_URL/moto-api/" > /dev/null 2>&1; then
    nohup moto_server -p "$PORTA" > "${SANDBOX_BASE:-/tmp}/moto.log" 2>&1 &
    for i in $(seq 1 30); do curl -fs "$MOTO_URL/moto-api/" > /dev/null 2>&1 && break; sleep 0.5; done
  fi
  curl -fs -X POST "$MOTO_URL/moto-api/reset" > /dev/null
}

# nuova sandbox per uno scenario: cartella, aws finto, shutdown finto, ruolo della macchina in moto
nuova_sandbox() {
  export SANDBOX=$1; rm -rf "$SANDBOX"; mkdir -p "$SANDBOX"
  cp -r "$QUI_PL/bin" "$QUI_PL/bin-macchina" "$SANDBOX/"; chmod +x "$SANDBOX"/bin/* "$SANDBOX"/bin-macchina/*
  export PATH="$SANDBOX/bin:${PATH_ORIG:=$PATH}"
  export M31_OUT=$SANDBOX/out GITHUB_STEP_SUMMARY=$SANDBOX/summary.md BUCKET=fenvix-coldread-$RANDOM$RANDOM
  avvia_moto
  aws iam create-role --role-name fenvix-coldread-machine --assume-role-policy-document '{"Version":"2012-10-17","Statement":[]}' > /dev/null
  aws iam create-instance-profile --instance-profile-name fenvix-coldread-machine > /dev/null
  aws iam add-role-to-instance-profile --instance-profile-name fenvix-coldread-machine --role-name fenvix-coldread-machine
}

# il pacco come lo fa il workflow, da un programma e un esegui.sh dati
fai_pacco() { # fai_pacco PROGRAMMA ESEGUI DESTINAZIONE
  local d; d=$(mktemp -d "$SANDBOX/pacco.XXXX"); cp "$1" "$d/m31"; cp "$2" "$d/esegui.sh"
  tar czf "$3" -C "$d" m31 esegui.sh
}

controlla() { # controlla "descrizione" comando...
  if "${@:2}"; then echo "OK      $1"; else echo "FALLITA $1"; FALLITE=$((FALLITE + 1)); fi
}
