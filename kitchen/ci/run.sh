#!/bin/bash
# Kitchen (K2, corrected by K2b) · the run from GitHub Actions (model: P1 Ireland giro-ci.sh and M31 giornata-ci.sh).
# Buckets: the run bucket ($BUCKET: packages, password, messages between runner and machines, results; deleted at the
# end) and the pantry ($PANTRY: our server's data). The pantry is either new and deleted at the end (default), new and
# KEPT (fenvix-kitchen-pantry-<run>, founder's road 1 of A2), or an existing kept pantry (no load in this run).
# Two new security groups: "client" (no open port) and "server" (only port 27017, only from the "client" group).
# One client machine ($CLIENT, from the program) for the whole run; then, one at a time, each on a NEW machine:
#   cook 0, the loader ($LOADER, only if LOAD=1): the client loads, then "done-0", "stop", terminated (reviewer's C1:
#   no size ever measures on the process that loaded);
#   cooks 1..N, the sizes ($SIZES): cold, the program, "done-K", "stop", terminated.
# Before every launch the credentials are renewed (reviewer's C2), so the presigned link of the package lives an hour.
# Every minute the SPESA line (machines at their price, plus S3 from our server's own request counters: point 4);
# block at $SPEND_CAP. Cleanup is in cleanup.sh (always runs).
# Variables from the workflow: BUCKET, PANTRY, PANTRY_MODE (new|keep|existing), LOAD (0|1), LOADER, SIZES (JSON),
# CLIENT, SPEND_CAP, MACHINE_ROLE, CM_ACCT, CM_ROLE, CM_PW (password of the run, masked), CA_DIR, PACKAGES, DISK_GB,
# CEILING_MIN; K2c: SERVER_OPTIONS (JSON list, one string per size, K6), PREPARE (JSON list of true/false per size, K5),
# FRESH_PANTRY (1 = K3: every size reads its own server-side copy of the pantry as it was right after the load).
# K2 (reviewer of K2b): an existing kept pantry created more than KEPT_MAX_DAYS (2) days ago is refused: its objects
# expire on the third day, the SSTs written by the load before the manifest that names them (a broken database).
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/user_data.sh"
OUT=${CM_OUT:-/tmp/cm-out}; mkdir -p "$OUT"
MINUTE_S=${MINUTE_S:-60}; MUTE_MIN=${MUTE_MIN:-15}
T0=$(date +%s); CEILING_MIN=${CEILING_MIN:-330}; LAST_RENEW=$T0
DISK_GB=${DISK_GB:-20}; SPEND_CAP=${SPEND_CAP:-5}
# S3 in the SPESA line (point 4): our server counts its own S3 requests (serverStatus.fenvix.s3, K1); each cook writes
# them with their price every 30 s (s3_counters.py). On top: S3_MARGIN for what the counters do not see yet (up to
# 90 s of delay) and for the client's own small files; storage at $0.023 per GB-month for PANTRY_GB (upper bound for
# the m30-corto data: about 25 GB of BSON); if a cook has no counters (an engine without K1), S3_FALLBACK once (upper
# bound for one load of the m30-corto data: about 25 GB, about 1 PUT per WAL flush plus compaction, STIMATO).
S3_MARGIN=${S3_MARGIN:-0.10}; S3_FALLBACK=${S3_FALLBACK:-1.50}; PANTRY_GB=${PANTRY_GB:-60}
SERVER_OPTIONS=${SERVER_OPTIONS:-}; PREPARE=${PREPARE:-}; FRESH_PANTRY=${FRESH_PANTRY:-0}; KEPT_MAX_DAYS=${KEPT_MAX_DAYS:-2}
COPY_DOLLARS=0   # K3: the COPY and LIST requests of the per-size copies ($0.005 per 1,000, DECLARED S3 price)
RUN=${GITHUB_RUN_ID:-0}
renew() { # fresh OIDC credentials for another hour, without leaving the step (like M31 and Ireland)
  [ -n "${ACTIONS_ID_TOKEN_REQUEST_URL:-}" ] || { LAST_RENEW=$(date +%s); return 0; }
  local tok j
  tok=$(curl -fsS -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=sts.amazonaws.com" | jq -r .value) || return 1
  j=$(aws sts assume-role-with-web-identity --role-arn "$CM_ROLE" --role-session-name kitchen-run --web-identity-token "$tok" --duration-seconds 3600 --output json) || return 1
  AWS_ACCESS_KEY_ID=$(jq -r .Credentials.AccessKeyId <<<"$j"); AWS_SECRET_ACCESS_KEY=$(jq -r .Credentials.SecretAccessKey <<<"$j"); AWS_SESSION_TOKEN=$(jq -r .Credentials.SessionToken <<<"$j")
  export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
  echo "::add-mask::$AWS_SECRET_ACCESS_KEY"; echo "::add-mask::$AWS_SESSION_TOKEN"
  LAST_RENEW=$(date +%s)
}
py() { python3 -c "$@"; }
comma() { py "import sys; print(f'{float(sys.argv[1]):.2f}'.replace('.', ','))" "$1"; }
MACHINES=0; LAST_SPEND=$T0; MACH_MIN=0; COOKS=""; SPEND=0
ALIVE=""
alive() { # ALIVE = types of the tagged machines still alive; if it cannot read, keeps the last ones seen
  local t
  if t=$(aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-coldread Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down \
      --query 'Reservations[].Instances[].InstanceType' --output text 2>/dev/null); then ALIVE=$(tr '\t' ' ' <<<"$t" | sed 's/None//g'); fi
}
s3_dollars() { # S3 so far: the cooks' own counters (price included), plus margin, storage, and the fallback if needed
  local k f d tot=0 fb=0 hours
  for k in $COOKS; do
    f=$(aws s3 cp "s3://$BUCKET/results/server-$k/s3-counters.json" - 2>/dev/null) || f=""
    d=$(jq -r 'if .available then .dollars else "none" end' <<<"$f" 2>/dev/null) || d=""
    case "$d" in ""|none|null) fb=1 ;; *) tot=$(py "import sys; print(float(sys.argv[1]) + float(sys.argv[2]))" "$tot" "$d") ;; esac
  done
  hours=$(py "import sys; print((int(sys.argv[1]) - int(sys.argv[2])) / 3600)" "$(date +%s)" "$T0")
  py "import sys; t, fb, m, f, gb, h, c = map(float, sys.argv[1:]); print(f'{t + m + fb * f + gb * 0.023 / 730 * h * (2 if c else 1) + c:.6f}')" \
    "$tot" "$fb" "$S3_MARGIN" "$S3_FALLBACK" "$PANTRY_GB" "$hours" "$COPY_DOLLARS"
}
spend() { # adds the minutes since last time (at least 1) with the machines alive now; prints the SPESA line
  local now min types pm s3
  now=$(date +%s); min=$(( (now - LAST_SPEND + MINUTE_S - 1) / MINUTE_S )); [ "$min" -ge 1 ] || min=1
  LAST_SPEND=$now
  alive; types=${ALIVE:-$CLIENT}   # no machine alive: count the client anyway (upper bound)
  # shellcheck disable=SC2086
  pm=$(python3 "$HERE/machine_types.py" minute-cost "$DISK_GB" $types)
  MACHINES=$(py "import sys; print(f'{float(sys.argv[1]) + float(sys.argv[2]) * int(sys.argv[3]):.6f}')" "$MACHINES" "$pm" "$min")
  MACH_MIN=$(( MACH_MIN + min * $(wc -w <<<"$types") ))
  s3=$(s3_dollars)
  SPEND=$(py "import sys; print(f'{float(sys.argv[1]) + float(sys.argv[2]):.6f}')" "$MACHINES" "$s3")
  echo "SPESA: $(comma "$SPEND") \$ so far at $(( (now - T0) / 60 )) minutes (upper bound; machines $(comma "$MACHINES") \$, alive: ${ALIVE:-none, counting the client}; $(py "import sys; print(f'{int(sys.argv[1])/60:.1f}')" "$MACH_MIN") machine-hours; S3 $(comma "$s3") \$ from the cooks' counters plus margin), cap $(comma "$SPEND_CAP") \$"
}
over_cap() { py "import sys; sys.exit(0 if float(sys.argv[1]) >= float(sys.argv[2]) else 1)" "$SPEND" "$SPEND_CAP"; }
OUTCOME=""
# one minute of the run: wait, credentials, spend, block, last line of the client. Exit 1 = stop waiting.
minute() {
  sleep "$MINUTE_S"
  [ $(( $(date +%s) - LAST_RENEW )) -gt 2700 ] && { renew || echo "credential renewal failed"; }
  spend
  if over_cap; then
    echo "BLOCCO: estimated spend $(comma "$SPEND") \$ >= $(comma "$SPEND_CAP") \$: stopping, terminating the machines, cleanup starts"
    ids=$(aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-coldread Name=instance-state-name,Values=pending,running \
      --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null)
    # shellcheck disable=SC2086
    [ -n "$ids" ] && aws ec2 terminate-instances --instance-ids $ids >/dev/null 2>&1
    OUTCOME=BLOCCO_SPESA; return 1
  fi
  local m line t
  m=$(( ($(date +%s) - T0) / 60 ))
  if line=$(aws s3 cp "s3://$BUCKET/results/client/steps.log" - 2>/dev/null | tail -1) && [ -n "$line" ]; then
    SEEN=1; echo "  client: $line"
    case "$line" in *"load:"*) for f in $(aws s3 ls "s3://$BUCKET/results/client/load/" 2>/dev/null | awk '/carica/ {print $4}'); do
        t=$(aws s3 cp "s3://$BUCKET/results/client/load/$f" - 2>/dev/null | grep -a "caricati" | tail -1) && [ -n "$t" ] && echo "  ${f%%-*}: $t"; done;;
      *"size "*) t=$(aws s3 cp "s3://$BUCKET/results/client/size-$K/passi.log" - 2>/dev/null | tail -1) && [ -n "$t" ] && echo "  program: $t";; esac
  fi
  if [ "${SEEN:-0}" = 0 ] && [ "$m" -ge "$MUTE_MIN" ]; then echo "MACHINE MUTE: no steps.log from the client after $m minutes"; OUTCOME=MACHINE_MUTE; return 1; fi
  if [ "$m" -ge "$CEILING_MIN" ]; then echo "TIME CEILING: $m minutes"; OUTCOME=TIME_CEILING; return 1; fi
  st=$(aws ec2 describe-instances --instance-ids "$ID_CLIENT" --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null)
  if { [ "$st" = terminated ] || [ "$st" = shutting-down ]; } && ! exists client/end.json; then
    echo "CLIENT OFF without end.json"; OUTCOME=CLIENT_OFF; return 1
  fi
  return 0
}
exists() { aws s3 ls "s3://$BUCKET/$1" >/dev/null 2>&1; }
put() { printf '%s' "$2" | aws s3 cp - "s3://$BUCKET/$1" --only-show-errors; }
left_min() { echo $(( CEILING_MIN - ($(date +%s) - T0) / 60 + 5 )); }

[ -n "${CM_PW:-}" ] || { echo "password of the run missing"; exit 1; }
mapfile -t LIST < <(python3 "$HERE/machine_types.py" check-sizes "$SIZES" 4) || exit 1
# per-size options (K6, K5), normalized as in the workflow's "decide" (one entry per size; defaults of size_options.py)
mapfile -t SO < <(python3 "$HERE/size_options.py" "$SIZES" "${SERVER_OPTIONS:-null}" "${PREPARE:-null}") && [ ${#SO[@]} = 2 ] \
  || { echo "per-size options refused: ${SO[*]:-}"; exit 1; }
SERVER_OPTIONS=${SO[0]}; PREPARE=${SO[1]}
echo "run: load $LOAD (loader ${LOADER:-none}); sizes ${LIST[*]:-none}; client $CLIENT; pantry $PANTRY ($PANTRY_MODE); server options $SERVER_OPTIONS; prepare $PREPARE; fresh pantry per size $FRESH_PANTRY; cap $SPEND_CAP \$; $CEILING_MIN minutes"
echo "1/5 buckets"
new_bucket() { # new_bucket NAME DAYS
  aws s3api create-bucket --bucket "$1" --create-bucket-configuration LocationConstraint=eu-west-1 >/dev/null || return 1
  aws s3api put-public-access-block --bucket "$1" --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
  aws s3api put-bucket-tagging --bucket "$1" --tagging "TagSet=[{Key=project,Value=fenvix-coldread}]"
  aws s3api put-bucket-lifecycle-configuration --bucket "$1" --lifecycle-configuration "{\"Rules\":[{\"ID\":\"expire\",\"Status\":\"Enabled\",\"Filter\":{},\"Expiration\":{\"Days\":$2},\"AbortIncompleteMultipartUpload\":{\"DaysAfterInitiation\":1}}]}"
}
new_bucket "$BUCKET" 1 || exit 1
case "$PANTRY_MODE" in
  new) new_bucket "$PANTRY" 1 || exit 1; echo "pantry $PANTRY: new, deleted at the end" ;;
  keep) new_bucket "$PANTRY" 3 || exit 1; echo "pantry $PANTRY: new and KEPT after the run (objects expire after 3 days)" ;;
  existing) aws s3api head-bucket --bucket "$PANTRY" || { echo "pantry $PANTRY not found"; exit 1; }
    aws s3 ls "s3://$PANTRY/kitchen-states/" >/dev/null 2>&1 || { echo "pantry $PANTRY has no kitchen-states/: no finished load"; exit 1; }
    # K2: no pantry on its third day (one LIST of the buckets, $0)
    born=$(aws s3api list-buckets --output json | jq -r --arg b "$PANTRY" '.Buckets[] | select(.Name == $b) | .CreationDate')
    age=$(py "import sys, datetime as d; b = d.datetime.fromisoformat(sys.argv[1].replace('Z', '+00:00')); print(f'{(d.datetime.now(d.timezone.utc) - b).total_seconds() / 86400:.2f}')" "$born") \
      || { echo "pantry $PANTRY: creation date not readable ($born)"; exit 1; }
    py "import sys; sys.exit(0 if float(sys.argv[1]) <= float(sys.argv[2]) else 1)" "$age" "$KEPT_MAX_DAYS" \
      || { echo "pantry $PANTRY created $age days ago, more than $KEPT_MAX_DAYS: its objects expire on the third day (load again)"; exit 1; }
    echo "pantry $PANTRY: created $age days ago (at most $KEPT_MAX_DAYS)"
    echo "pantry $PANTRY: existing, loaded by an earlier run, kept" ;;
  *) echo "pantry mode not valid: $PANTRY_MODE"; exit 1 ;;
esac
echo "2/5 packages, CA and password in the run bucket (the password goes through no file and no log)"
for f in "$PACKAGES"/*.tar.gz; do aws s3 cp "$f" "s3://$BUCKET/program/$(basename "$f")" --only-show-errors || exit 1; done
aws s3 cp "$CA_DIR/ca.pem" "s3://$BUCKET/program/ca.pem" --only-show-errors || exit 1
put secret/pw "$CM_PW" || exit 1
url() { aws s3 presign "s3://$BUCKET/program/$1" --expires-in 21600; }
echo "3/5 network: security groups (server: port 27017 open only to the client group), Ubuntu 24.04 images"
VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
SUBNET=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true --query 'sort_by(Subnets,&AvailabilityZone)[0].SubnetId' --output text)
group() { aws ec2 create-security-group --group-name "fenvix-coldread-cm-$1-$RUN" --description "kitchen $1 (deleted at the end of the run)" \
  --vpc-id "$VPC" --tag-specifications "ResourceType=security-group,Tags=[{Key=project,Value=fenvix-coldread}]" --query GroupId --output text; }
SG_CLI=$(group client) || exit 1
SG_SRV=$(group server) || exit 1
aws ec2 authorize-security-group-ingress --group-id "$SG_SRV" --ip-permissions "IpProtocol=tcp,FromPort=27017,ToPort=27017,UserIdGroupPairs=[{GroupId=$SG_CLI}]" >/dev/null || exit 1
echo "groups: client $SG_CLI (no port), server $SG_SRV (27017 only from the client)"
ami() { aws ssm get-parameter --name "/aws/service/canonical/ubuntu/server/24.04/stable/current/$1/hvm/ebs-gp3/ami-id" --query Parameter.Value --output text; }
declare -A AMI
AMI[x86_64]=$(ami amd64) || exit 1
for t in "${LIST[@]}" ${LOADER:-}; do [ "$(python3 "$HERE/machine_types.py" arch "$t")" = aarch64 ] && { AMI[aarch64]=$(ami arm64) || exit 1; break; }; done
launch() { # launch TYPE NAME GROUP USER_DATA_FILE -> prints "ID PRIVATE_IP"
  aws ec2 run-instances --image-id "${AMI[$(python3 "$HERE/machine_types.py" arch "$1")]}" --instance-type "$1" --subnet-id "$SUBNET" \
    --security-group-ids "$3" --associate-public-ip-address \
    --iam-instance-profile Name="$MACHINE_ROLE" --instance-initiated-shutdown-behavior terminate \
    --metadata-options HttpTokens=required,HttpEndpoint=enabled \
    --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=$DISK_GB,VolumeType=gp3,DeleteOnTermination=true}" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=project,Value=fenvix-coldread},{Key=Name,Value=fenvix-coldread-cm-$2}]" "ResourceType=volume,Tags=[{Key=project,Value=fenvix-coldread}]" \
    --user-data "file://$4" --query 'Instances[0].[InstanceId,PrivateIpAddress]' --output text
}
echo "4/5 client machine $CLIENT (on for the whole run; terminates itself at the end)"
renew || echo "credential renewal failed before the client launch"
UD=$(mktemp)
MACHINE_CEILING_MIN=$(left_min) write_user_data "$(url client.tar.gz)" "$BUCKET" client > "$UD"
read -r ID_CLIENT _ < <(launch "$CLIENT" client "$SG_CLI" "$UD") || { rm -f "$UD"; exit 1; }
rm -f "$UD"
[[ "$ID_CLIENT" == i-* ]] || { echo "client launch failed"; exit 1; }
echo "client: $ID_CLIENT"
echo "5/5 the cooks, one at a time, each on a new machine (every minute: spend and last line of the client)"
declare -a OUTCOMES
COOK_LIST=()
[ "$LOAD" = 1 ] && COOK_LIST+=("$LOADER")
COOK_LIST+=("${LIST[@]}")
K=$([ "$LOAD" = 1 ] && echo -1 || echo 0)
size_field() { # size_field JSON_LIST INDEX DEFAULT: entry INDEX (0-based) of a JSON list from the command
  py "import json, sys; l = json.loads(sys.argv[1]) if sys.argv[1] else []; i = int(sys.argv[2]); v = l[i] if 0 <= i < len(l) else json.loads(sys.argv[3]); print(json.dumps(v) if not isinstance(v, str) else v)" "$1" "$2" "$3"; }
copy_pantry() { # copy_pantry TARGET: K3, a server-side copy of the pantry (no download), with its COPY requests in SPESA
  local t0 n
  new_bucket "$1" 1 || return 1
  t0=$(date +%s)
  aws s3 sync "s3://$PANTRY" "s3://$1" --exclude 'kitchen-states/*' --only-show-errors || return 1
  n=$(aws s3api list-objects-v2 --bucket "$1" --query 'length(Contents || `[]`)' --output text 2>/dev/null || echo 0)
  COPY_DOLLARS=$(py "import sys; print(f'{float(sys.argv[1]) + (int(sys.argv[2]) + 10) * 0.005 / 1000:.6f}')" "$COPY_DOLLARS" "$n")
  echo "fresh pantry for this size: $1, $n objects copied server-side in $(( $(date +%s) - t0 )) s"
}
for TYPE in "${COOK_LIST[@]}"; do
  K=$((K + 1)); P=server-$K
  [ -n "$OUTCOME" ] && break
  exists client/end.json && { echo "the client has already finished: no cook $K"; break; }
  # the pantry this cook works on, its server options (K6) and the prepare step (K5); cook 0 (the loader): the engine as
  # it is, on the pantry
  COOK_PANTRY=$PANTRY; COOK_OPTS=""; COOK_PREP=false
  if [ "$K" -ge 1 ]; then
    COOK_OPTS=$(size_field "$SERVER_OPTIONS" $((K - 1)) '""'); COOK_PREP=$(size_field "$PREPARE" $((K - 1)) false)
    if [ "$FRESH_PANTRY" = 1 ]; then
      renew || echo "credential renewal failed before the copy for cook $K"
      COOK_PANTRY=fenvix-coldread-cmw-$RUN-$K
      copy_pantry "$COOK_PANTRY" || { echo "cook $K: the copy of the pantry failed"; OUTCOMES+=("$K $TYPE: COPY_FAILED"); OUTCOME=COPY_FAILED; break; }
    fi
  fi
  py "import json, sys; print(json.dumps({'pantry': sys.argv[1], 'server_options': sys.argv[2]}))" "$COOK_PANTRY" "$COOK_OPTS" > "$OUT/config-$K.json"
  aws s3 cp "$OUT/config-$K.json" "s3://$BUCKET/$P/config.json" --only-show-errors || { echo "cook $K: config not written"; OUTCOME=NO_BUCKET; break; }
  echo "cook $K: pantry $COOK_PANTRY; server options '${COOK_OPTS}'; prepare $COOK_PREP"
  # C2: fresh credentials right before the presigned link and the launch
  renew || echo "credential renewal failed before cook $K"
  UD=$(mktemp)
  MACHINE_CEILING_MIN=$(left_min) write_user_data "$(url "server-$(python3 "$HERE/machine_types.py" arch "$TYPE").tar.gz")" "$BUCKET" server "$K" > "$UD"
  ID_SRV=""; IP_SRV=""
  read -r ID_SRV IP_SRV < <(launch "$TYPE" "server-$K" "$SG_SRV" "$UD")
  rm -f "$UD"
  if [[ "${ID_SRV:-}" != i-* ]]; then echo "cook $K ($TYPE): launch failed"; OUTCOMES+=("$K $TYPE: LAUNCH_FAILED"); OUTCOME=LAUNCH_FAILED; break; fi
  COOKS="$COOKS $K"
  IP_CERT=${FAKE_SERVER_IP:-$IP_SRV}
  echo "cook $K: $([ "$K" = 0 ] && echo loader || echo size) $TYPE $ID_SRV, private IP $IP_SRV"
  pem=$(mktemp); bash "$HERE/certs.sh" server "$CA_DIR" "$IP_CERT" "$pem" && aws s3 cp "$pem" "s3://$BUCKET/$P/server.pem" --only-show-errors; rm -f "$pem"
  put "$P/go.json" "{\"type\": \"$TYPE\", \"k\": $K, \"pantry\": \"$COOK_PANTRY\", \"prepare\": $COOK_PREP}"
  while ! exists "client/done-$K.json"; do
    minute || break
    exists client/end.json && { exists "client/done-$K.json" || echo "the client finished without cook $K"; break; }
  done
  f=$(aws s3 cp "s3://$BUCKET/client/done-$K.json" - 2>/dev/null | jq -r '.outcome // "NON_SO"' 2>/dev/null)
  OUTCOMES+=("$K $TYPE: ${f:-NOT DONE}")
  echo "cook $K ($TYPE): ${f:-NOT DONE}; stopping it"
  put "$P/stop" "1"
  # after a block (spend, time, client off) no waiting: the machines are already terminated or are being terminated
  [ -z "$OUTCOME" ] && for _ in $(seq 1 18); do exists "$P/stopped" && break; sleep 10; done
  exists "$P/stopped" && echo "cook $K stopped by itself (logs, SST count and S3 counters in the bucket)" || echo "cook $K did not answer 'stop' in 3 minutes: terminating it"
  aws ec2 terminate-instances --instance-ids "$ID_SRV" >/dev/null 2>&1
  aws ec2 wait instance-terminated --instance-ids "$ID_SRV" 2>/dev/null || echo "waiting for the termination of cook $K failed: cleanup.sh does it again"
  if [ "$COOK_PANTRY" != "$PANTRY" ]; then   # K3: the copy goes away with its size (cleanup.sh deletes it anyway)
    renew || true; aws s3 rb "s3://$COOK_PANTRY" --force >/dev/null 2>&1 && echo "copy $COOK_PANTRY deleted" || echo "copy $COOK_PANTRY not deleted: cleanup.sh does it"
  fi
  spend
  [ "$K" = 0 ] && [ "$f" != LOADED ] && { echo "the load did not finish: no size"; OUTCOME=${OUTCOME:-LOAD_FAILED}; break; }
done
echo "end of the cooks: telling the client"
put client/enough "1"
[ -z "$OUTCOME" ] && for _ in $(seq 1 20); do exists client/end.json && break; sleep 15; done
END=$(aws s3 cp "s3://$BUCKET/client/end.json" - 2>/dev/null | jq -r '.outcome // "NON_SO"' 2>/dev/null)
[ -n "$OUTCOME" ] || OUTCOME=${END:-CLIENT_WITHOUT_END}
aws ec2 terminate-instances --instance-ids "$ID_CLIENT" >/dev/null 2>&1
echo "outcome: $OUTCOME"
aws s3 cp "s3://$BUCKET/results/" "$OUT/" --recursive --only-show-errors || echo "copy of the results failed: fetch_results.sh does it again"
spend | tee "$OUT/spend.txt"
printf '%s\n' "${OUTCOMES[@]}" > "$OUT/cook-outcomes.txt"
python3 - "$OUT/run.json" "$OUTCOME" "$(( ($(date +%s) - T0) / 60 ))" "$CLIENT" "$SIZES" "$SPEND" "$PANTRY" "$PANTRY_MODE" "$LOAD" <<'PY'
import json, sys
f, outcome, minutes, client, sizes, spend, pantry, mode, load = sys.argv[1:]
json.dump({"outcome": outcome, "minutes": int(minutes), "client": client, "sizes": json.loads(sizes),
           "estimated_spend_upper_bound": float(spend), "pantry": pantry, "pantry_mode": mode, "loaded_in_this_run": load == "1"},
          open(f, "w"), indent=1)
PY
grep -rl "$CM_ACCT" "$OUT" 2>/dev/null | xargs -r sed -i "s/$CM_ACCT/<ACCOUNT_ID>/g"
{ [ "$OUTCOME" = BLOCCO_SPESA ] && echo "# BLOCCO: estimated spend reached the cap of $SPEND_CAP \$"
  echo "## Kitchen: $OUTCOME, $(( ($(date +%s) - T0) / 60 )) minutes"; echo
  sed 's/^/- cook /' "$OUT/cook-outcomes.txt"; echo; cat "$OUT/spend.txt"; echo
  [ "$PANTRY_MODE" != new ] && echo "pantry KEPT: $PANTRY (for the next runs: \"pantry\": \"$PANTRY\")"
  [ -f "$OUT/client/load-seconds.txt" ] && echo "load: $(cat "$OUT/client/load-seconds.txt") seconds"
  echo '```'; tail -25 "$OUT/client/steps.log" 2>/dev/null; cat "$OUT/client/load.txt" 2>/dev/null | tail -8; echo '```'
  for d in "$OUT"/client/size-*; do [ -d "$d" ] || continue
    echo "### $(basename "$d"): program steps, machine validity, maximum load"; echo '```'
    grep -E "programma finito|ERRORE|FERMO|connessioni del parallelo|massimo:" "$d/passi.log" 2>/dev/null | cut -c1-200
    tail -1 "$d/validita-macchina.txt" 2>/dev/null; echo '```'
  done; } >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
[ "$OUTCOME" = BLOCCO_SPESA ] && echo "::warning title=BLOCCO_SPESA::estimated spend >= $SPEND_CAP \$: run stopped, cleanup started"
[ "$OUTCOME" = NON_UNO_A_UNO ] && echo "::error title=NON_UNO_A_UNO::our cook did not serve exactly the clients of the plan (client/size-*/reads.json)"
case "$OUTCOME" in FINITO|FINITO_MACCHINA_LIMITE|LOADED_ONLY) exit 0 ;; *) exit 1 ;; esac
