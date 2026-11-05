#!/bin/bash
# Cucina minima (K2) · la giornata da GitHub Actions (modello: P1 Irlanda giro-ci.sh e M31 giornata-ci.sh).
# Due bucket nuovi: quello della giornata ($BUCKET: pacchi, password, comandi tra runner e macchine, risultati) e quello
# dei DATI del nostro server ($BUCKET_DATI: la dispensa, caricata una volta e letta da tutte le taglie). Due gruppi di
# sicurezza nuovi: "cliente" (nessuna porta aperta) e "server" (solo la porta 27017, e solo dal gruppo "cliente").
# Una macchina cliente ($CLIENTE, c7i) per tutta la giornata; per ogni taglia di $TAGLIE, una macchina server, una alla
# volta: accesa, certificato TLS per il suo IP privato, "via" al cliente, attesa di "fatto", "ferma", cancellata, poi la
# taglia dopo. Ogni minuto la riga SPESA; blocco a $TETTO_DOLLARI. La pulizia e' in pulizia.sh (gira sempre).
# Variabili dal workflow: BUCKET, BUCKET_DATI, TAGLIE (lista JSON), CLIENTE, COPIE, TETTO_DOLLARI, RUOLO_MACCHINA,
# CM_ACCT, CM_ROLE, CM_PW (password della giornata, mascherata), CA_DIR (ca.key e ca.pem), PACCHI (cartella con
# cliente.tar.gz e server-<cpu>.tar.gz), DISCO_GB, TETTO_MIN.
set -uo pipefail
QUI=$(cd "$(dirname "$0")" && pwd)
source "$QUI/user-data.sh"
OUT=${CM_OUT:-/tmp/cm-out}; mkdir -p "$OUT"
MINUTO_S=${MINUTO_S:-60}; MUTA_MIN=${MUTA_MIN:-15}; RINNOVO_S=${RINNOVO_S:-2700}
T0=$(date +%s); TETTO_MIN=${TETTO_MIN:-320}; ULTIMO_RINNOVO=$T0
DISCO_GB=${DISCO_GB:-20}; TETTO_DOLLARI=${TETTO_DOLLARI:-5}; S3_FISSO=${S3_FISSO:-0.50}
RUN=${GITHUB_RUN_ID:-0}
rinnova() { # credenziali OIDC nuove per un'altra ora, senza uscire dallo step (come M31 e l'Irlanda)
  [ -n "${ACTIONS_ID_TOKEN_REQUEST_URL:-}" ] || return 0
  local tok j
  tok=$(curl -fsS -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=sts.amazonaws.com" | jq -r .value) || return 1
  j=$(aws sts assume-role-with-web-identity --role-arn "$CM_ROLE" --role-session-name cucina-giro --web-identity-token "$tok" --duration-seconds 3600 --output json) || return 1
  AWS_ACCESS_KEY_ID=$(jq -r .Credentials.AccessKeyId <<<"$j"); AWS_SECRET_ACCESS_KEY=$(jq -r .Credentials.SecretAccessKey <<<"$j"); AWS_SESSION_TOKEN=$(jq -r .Credentials.SessionToken <<<"$j")
  export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
  echo "::add-mask::$AWS_SECRET_ACCESS_KEY"; echo "::add-mask::$AWS_SESSION_TOKEN"
  ULTIMO_RINNOVO=$(date +%s)
}
# Spesa stimata PER ECCESSO (regola del fondatore 2026-11-04). Ogni minuto si guardano le macchine con il tag ancora
# vive e il loro tipo; ogni macchina conta al suo prezzo (ci/tipi.py: listino AWS DICHIARATO), piu' disco gp3 e IPv4.
# Il minuto in cui si guarda conta per intero. S3: S3_FISSO dollari messi in conto dall'inizio (STIMATO per eccesso:
# il caricamento di 10 GB fa circa 30.000 lotti, cioe' circa 30.000 PUT del registro a 0,005 $ ogni 1.000 = 0,15 $,
# piu' compattazione e letture; lo spazio di 10-20 GB per poche ore costa meno di 0,01 $).
SPESA=$S3_FISSO; ULTIMA_SPESA=$T0; MAC_MIN=0
somma() { python3 -c "import sys; print(f'{sum(float(x) for x in sys.argv[1:]):.6f}')" "$@"; }
virgola() { python3 -c "import sys; print(f'{float(sys.argv[1]):.2f}'.replace('.', ','))" "$1"; }
oltre_tetto() { python3 -c "import sys; sys.exit(0 if float(sys.argv[1]) >= float(sys.argv[2]) else 1)" "$SPESA" "$TETTO_DOLLARI"; }
VIVE=""
vive() { # VIVE = tipi delle macchine con il tag ancora vive; se non si legge, restano le ultime viste
  local t
  if t=$(aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-coldread Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down \
      --query 'Reservations[].Instances[].InstanceType' --output text 2>/dev/null); then VIVE=$(tr '\t' ' ' <<<"$t" | sed 's/None//g'); fi
}
spesa() { # aggiunge i minuti passati dall'ultima volta (almeno 1) con le macchine vive adesso e stampa la riga SPESA
  local ora min tipi pm
  ora=$(date +%s); min=$(( (ora - ULTIMA_SPESA + MINUTO_S - 1) / MINUTO_S )); [ "$min" -ge 1 ] || min=1
  ULTIMA_SPESA=$ora
  vive; tipi=${VIVE:-$CLIENTE}   # nessuna macchina viva: conto lo stesso il cliente (per eccesso)
  # shellcheck disable=SC2086
  pm=$(python3 "$QUI/tipi.py" spesa-minuto "$DISCO_GB" $tipi)
  SPESA=$(python3 -c "import sys; print(f'{float(sys.argv[1]) + float(sys.argv[2]) * int(sys.argv[3]):.6f}')" "$SPESA" "$pm" "$min")
  MAC_MIN=$(( MAC_MIN + min * $(wc -w <<<"$tipi") ))
  echo "SPESA: $(virgola "$SPESA") \$ finora a $(( (ora - T0) / 60 )) minuti (stima per eccesso; macchine vive: ${tipi:-nessuna}; $(python3 -c "import sys; print(f'{int(sys.argv[1])/60:.1f}'.replace('.', ','))" "$MAC_MIN") ore-macchina; S3 $(virgola "$S3_FISSO") in conto dall'inizio), tetto $(virgola "$TETTO_DOLLARI") \$"
}
ESITO=""
# un minuto della giornata: attesa, credenziali, spesa, blocco, ultima riga del cliente. Codice 1 = basta aspettare.
minuto() {
  sleep "$MINUTO_S"
  [ $(( $(date +%s) - ULTIMO_RINNOVO )) -gt "$RINNOVO_S" ] && { rinnova || echo "rinnovo delle credenziali fallito"; }
  spesa
  if oltre_tetto; then
    echo "BLOCCO: spesa stimata $(virgola "$SPESA") \$ >= $(virgola "$TETTO_DOLLARI") \$: smetto, cancello le macchine, parte la pulizia"
    ids=$(aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-coldread Name=instance-state-name,Values=pending,running \
      --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null)
    # shellcheck disable=SC2086
    [ -n "$ids" ] && aws ec2 terminate-instances --instance-ids $ids >/dev/null 2>&1
    ESITO=BLOCCO_SPESA; return 1
  fi
  local m riga
  m=$(( ($(date +%s) - T0) / 60 ))
  if riga=$(aws s3 cp "s3://$BUCKET/risultati/cliente/passi.log" - 2>/dev/null | tail -1) && [ -n "$riga" ]; then
    VISTO=1; echo "  cliente: $riga"
    case "$riga" in *caricamento:*) t=$(aws s3 cp "s3://$BUCKET/risultati/cliente/carica.txt" - 2>/dev/null | tail -1) && [ -n "$t" ] && echo "  script: $t";; esac
  fi
  if [ "${VISTO:-0}" = 0 ] && [ "$m" -ge "$MUTA_MIN" ]; then echo "MACCHINA MUTA: nessun passi.log del cliente dopo $m minuti"; ESITO=MACCHINA_MUTA; return 1; fi
  if [ "$m" -ge "$TETTO_MIN" ]; then echo "TETTO DI TEMPO: $m minuti"; ESITO=TETTO_TEMPO; return 1; fi
  st=$(aws ec2 describe-instances --instance-ids "$ID_CLIENTE" --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null)
  if { [ "$st" = terminated ] || [ "$st" = shutting-down ]; } && ! c_e cliente/fine.json; then
    echo "CLIENTE SPENTO senza fine.json"; ESITO=CLIENTE_SPENTO; return 1
  fi
  return 0
}
c_e() { aws s3 ls "s3://$BUCKET/$1" >/dev/null 2>&1; }
metti() { printf '%s' "$2" | aws s3 cp - "s3://$BUCKET/$1" --only-show-errors; }
resto_min() { echo $(( TETTO_MIN - ($(date +%s) - T0) / 60 + 5 )); }

[ -n "${CM_PW:-}" ] || { echo "password della giornata mancante"; exit 1; }
mapfile -t LISTA < <(python3 "$QUI/tipi.py" controlla-taglie "$TAGLIE") || exit 1
echo "giornata: taglie ${LISTA[*]}; cliente $CLIENTE con $COPIE copie; tetto $TETTO_DOLLARI \$; $TETTO_MIN minuti"
echo "1/5 bucket $BUCKET (giornata) e $BUCKET_DATI (dati del server)"
for b in "$BUCKET" "$BUCKET_DATI"; do
  aws s3api create-bucket --bucket "$b" --create-bucket-configuration LocationConstraint=eu-west-1 >/dev/null || exit 1
  aws s3api put-public-access-block --bucket "$b" --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
  aws s3api put-bucket-tagging --bucket "$b" --tagging "TagSet=[{Key=project,Value=fenvix-coldread}]"
  aws s3api put-bucket-lifecycle-configuration --bucket "$b" --lifecycle-configuration '{"Rules":[{"ID":"scade","Status":"Enabled","Filter":{},"Expiration":{"Days":1},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":1}}]}'
done
echo "2/5 pacchi, CA e password nel bucket della giornata (la password non passa da file ne' da log)"
for f in "$PACCHI"/*.tar.gz; do aws s3 cp "$f" "s3://$BUCKET/programma/$(basename "$f")" --only-show-errors || exit 1; done
aws s3 cp "$CA_DIR/ca.pem" "s3://$BUCKET/programma/ca.pem" --only-show-errors || exit 1
metti segreto/pw "$CM_PW" || exit 1
url() { aws s3 presign "s3://$BUCKET/programma/$1" --expires-in 21600; }
echo "3/5 rete: gruppi di sicurezza (server: porta 27017 aperta solo al gruppo del cliente), immagini Ubuntu 24.04"
VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
SUBNET=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true --query 'sort_by(Subnets,&AvailabilityZone)[0].SubnetId' --output text)
gruppo() { aws ec2 create-security-group --group-name "fenvix-coldread-cm-$1-$RUN" --description "cucina minima $1 (si cancella a fine giornata)" \
  --vpc-id "$VPC" --tag-specifications "ResourceType=security-group,Tags=[{Key=project,Value=fenvix-coldread}]" --query GroupId --output text; }
SG_CLI=$(gruppo cliente) || exit 1
SG_SRV=$(gruppo server) || exit 1
aws ec2 authorize-security-group-ingress --group-id "$SG_SRV" --ip-permissions "IpProtocol=tcp,FromPort=27017,ToPort=27017,UserIdGroupPairs=[{GroupId=$SG_CLI}]" >/dev/null || exit 1
echo "gruppi: cliente $SG_CLI (nessuna porta), server $SG_SRV (27017 solo dal cliente)"
ami() { aws ssm get-parameter --name "/aws/service/canonical/ubuntu/server/24.04/stable/current/$1/hvm/ebs-gp3/ami-id" --query Parameter.Value --output text; }
declare -A AMI
AMI[x86_64]=$(ami amd64) || exit 1
for t in "${LISTA[@]}"; do [ "$(python3 "$QUI/tipi.py" arch "$t")" = aarch64 ] && { AMI[aarch64]=$(ami arm64) || exit 1; break; }; done
lancia() { # lancia TIPO NOME GRUPPO USER_DATA_FILE -> stampa "ID IP_PRIVATO"
  aws ec2 run-instances --image-id "${AMI[$(python3 "$QUI/tipi.py" arch "$1")]}" --instance-type "$1" --subnet-id "$SUBNET" \
    --security-group-ids "$3" --associate-public-ip-address \
    --iam-instance-profile Name="$RUOLO_MACCHINA" --instance-initiated-shutdown-behavior terminate \
    --metadata-options HttpTokens=required,HttpEndpoint=enabled \
    --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=$DISCO_GB,VolumeType=gp3,DeleteOnTermination=true}" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=project,Value=fenvix-coldread},{Key=Name,Value=fenvix-coldread-cm-$2}]" "ResourceType=volume,Tags=[{Key=project,Value=fenvix-coldread}]" \
    --user-data "file://$4" --query 'Instances[0].[InstanceId,PrivateIpAddress]' --output text
}
echo "4/5 macchina cliente $CLIENTE (resta accesa tutta la giornata; si cancella da sola alla fine)"
UD=$(mktemp)
TETTO_MACCHINA_MIN=$(resto_min) scrivi_user_data "$(url cliente.tar.gz)" "$BUCKET" cliente > "$UD"
read -r ID_CLIENTE _ < <(lancia "$CLIENTE" cliente "$SG_CLI" "$UD") || { rm -f "$UD"; exit 1; }
rm -f "$UD"
[[ "$ID_CLIENTE" == i-* ]] || { echo "lancio del cliente non riuscito"; exit 1; }
echo "cliente: $ID_CLIENTE"
echo "5/5 le taglie, una alla volta, sugli stessi dati (ogni minuto: spesa e ultima riga del cliente)"
declare -a ESITI
K=0
for TIPO in "${LISTA[@]}"; do
  K=$((K + 1)); P=server-$K
  [ -n "$ESITO" ] && break
  c_e cliente/fine.json && { echo "il cliente ha gia' finito: niente taglia $K"; break; }
  UD=$(mktemp)
  TETTO_MACCHINA_MIN=$(resto_min) scrivi_user_data "$(url "server-$(python3 "$QUI/tipi.py" arch "$TIPO").tar.gz")" "$BUCKET" server "$K" > "$UD"
  ID_SRV=""; IP_SRV=""
  read -r ID_SRV IP_SRV < <(lancia "$TIPO" "server-$K" "$SG_SRV" "$UD")
  rm -f "$UD"
  if [[ "${ID_SRV:-}" != i-* ]]; then echo "taglia $K ($TIPO): lancio non riuscito"; ESITI+=("$K $TIPO: LANCIO_NON_RIUSCITO"); ESITO=LANCIO_NON_RIUSCITO; break; fi
  IP_CERT=${IP_SERVER_FINTO:-$IP_SRV}
  echo "taglia $K: server $TIPO $ID_SRV, IP privato $IP_SRV"
  pem=$(mktemp); bash "$QUI/certificati.sh" server "$CA_DIR" "$IP_CERT" "$pem" && aws s3 cp "$pem" "s3://$BUCKET/$P/server.pem" --only-show-errors; rm -f "$pem"
  metti "$P/via.json" "{\"tipo\": \"$TIPO\", \"k\": $K}"
  while ! c_e "cliente/fatto-$K.json"; do
    minuto || break
    c_e cliente/fine.json && { c_e "cliente/fatto-$K.json" || echo "il cliente ha finito senza la taglia $K"; break; }
  done
  f=$(aws s3 cp "s3://$BUCKET/cliente/fatto-$K.json" - 2>/dev/null | jq -r '.esito // "NON_SO"' 2>/dev/null)
  ESITI+=("$K $TIPO: ${f:-NON FATTA}")
  echo "taglia $K ($TIPO): ${f:-NON FATTA}; fermo il server"
  metti "$P/ferma" "1"
  for _ in $(seq 1 18); do c_e "$P/fermato" && break; sleep 10; done
  c_e "$P/fermato" && echo "server $K fermato da se' (log nel bucket)" || echo "server $K non ha risposto a 'ferma' in 3 minuti: lo cancello"
  aws ec2 terminate-instances --instance-ids "$ID_SRV" >/dev/null 2>&1
  aws ec2 wait instance-terminated --instance-ids "$ID_SRV" 2>/dev/null || echo "attesa della cancellazione del server $K non riuscita: la rifa' pulizia.sh"
  spesa
done
echo "fine delle taglie: avviso il cliente"
metti cliente/basta "1"
for _ in $(seq 1 20); do c_e cliente/fine.json && break; sleep 15; done
FINE=$(aws s3 cp "s3://$BUCKET/cliente/fine.json" - 2>/dev/null | jq -r '.esito // "NON_SO"' 2>/dev/null)
[ -n "$ESITO" ] || ESITO=${FINE:-CLIENTE_SENZA_FINE}
aws ec2 terminate-instances --instance-ids "$ID_CLIENTE" >/dev/null 2>&1
echo "esito: $ESITO"
aws s3 cp "s3://$BUCKET/risultati/" "$OUT/" --recursive --only-show-errors || echo "copia dei risultati fallita: la rifa' recupera.sh"
spesa | tee "$OUT/spesa.txt"
printf '%s\n' "${ESITI[@]}" > "$OUT/esiti-taglie.txt"
python3 - "$OUT/giornata.json" "$ESITO" "$(( ($(date +%s) - T0) / 60 ))" "$CLIENTE" "$COPIE" "$TAGLIE" "$SPESA" <<'PY'
import json, sys
f, esito, minuti, cliente, copie, taglie, spesa = sys.argv[1:]
json.dump({"esito": esito, "minuti": int(minuti), "cliente": cliente, "copie": int(copie), "taglie": json.loads(taglie),
           "spesa_stimata_per_eccesso": float(spesa)}, open(f, "w"), indent=1)
PY
grep -rl "$CM_ACCT" "$OUT" 2>/dev/null | xargs -r sed -i "s/$CM_ACCT/<ACCOUNT_ID>/g"
{ [ "$ESITO" = BLOCCO_SPESA ] && echo "# BLOCCO: spesa stimata arrivata al tetto di $TETTO_DOLLARI \$"
  echo "## Cucina minima: $ESITO, $(( ($(date +%s) - T0) / 60 )) minuti"; echo
  sed 's/^/- taglia /' "$OUT/esiti-taglie.txt"; echo; cat "$OUT/spesa.txt"; echo
  [ -f "$OUT/cliente/carica-secondi.txt" ] && echo "caricamento: $(cat "$OUT/cliente/carica-secondi.txt") secondi"
  echo '```'; tail -25 "$OUT/cliente/passi.log" 2>/dev/null; echo '```'
  for d in "$OUT"/cliente/taglia-*; do [ -d "$d" ] || continue
    echo "### $(basename "$d"): parallelo (connessioni) e carico massimo (somma delle copie)"; echo '```'
    tail -1 "$d/parallelo-connessioni.txt" 2>/dev/null; grep -E "^(CARICO|[a-z_0-9]+:|  VERDETTO)" "$d/massimo-somma.txt" 2>/dev/null; echo '```'
  done; } >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
[ "$ESITO" = BLOCCO_SPESA ] && echo "::warning title=BLOCCO_SPESA::spesa stimata >= $TETTO_DOLLARI \$: giornata fermata, pulizia partita"
[ "$ESITO" = FINITO ]
