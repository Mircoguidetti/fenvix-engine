# shellcheck shell=bash
# Step 9 · helpers shared by the machine scripts (sourced). Needs BUCKET and P (this machine's folder in the bucket).
PY=${PY:-python3}
PUSH_S=${PUSH_S:-30}
mkdir -p out
step() { echo "$(date -u +%FT%TZ) $*" | tee -a out/steps.log; }
push() { $PY s3io.py push "$BUCKET" out "results/$P/" 2>> out/push-errors.log; }
put_json() { printf '%s\n' "$2" > "out/.msg.json"; $PY s3io.py put "$BUCKET" "$1" out/.msg.json; }
exists() { $PY s3io.py exists "$BUCKET" "$1"; }
text() { $PY s3io.py text "$BUCKET" "$1" 2>/dev/null; }
# IMDSv2 on EC2; in a local container: the container's address and the type the runner gave it
imds() { local t; t=$(curl -fsS -m 5 -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 600" 2>/dev/null) &&
  curl -fsS -m 5 -H "X-aws-ec2-metadata-token: $t" "http://169.254.169.254/latest/meta-data/$1" 2>/dev/null; }
private_ip() { if [ -n "${LOCAL_TEST:-}" ]; then hostname -i | awk '{print $1}'; else imds local-ipv4; fi; }
machine_type() { if [ -n "${LOCAL_TEST:-}" ]; then echo "${FAKE_TYPE:-?}"; else imds instance-type || echo "?"; fi; }
zone() { if [ -n "${LOCAL_TEST:-}" ]; then echo "${FAKE_AZ:-?}"; else imds placement/availability-zone || echo "?"; fi; }
power_off() { # power_off REASON: on EC2 the power-off is the termination (set at launch); in a container, exit
  [ -n "${LOCAL_TEST:-}" ] || [ -n "${S9_NO_POWEROFF:-}" ] && exit 0
  shutdown -h now "step9: $1"; exit 0
}
# S3 endpoint for our programs (only in local mode: MinIO)
s3_endpoint_args() { [ -n "${LOCAL_TEST:-}" ] && [ -n "${AWS_ENDPOINT_URL:-}" ] && echo "--endpoint $AWS_ENDPOINT_URL"; }
# The cold phase of the agent benchmark on AWS (total review B9, P-bench-local 2.5): a client writes $P/restart-request
# holding an id; this machine stops its server, drops the page cache, starts the server again and answers
# $P/restarted-<id> once it serves. Called from the machine's main loop. DROP_CACHES_CMD is for tests only.
COLD_DONE=""
serve_cold_restart() { # serve_cold_restart STOP_FUNCTION START_FUNCTION (START returns 0 once the server answers)
  exists "$P/restart-request" || return 0
  local id; id=$(text "$P/restart-request" | tr -cd 'a-zA-Z0-9-')
  { [ -z "$id" ] || [ "$id" = "$COLD_DONE" ]; } && return 0
  local t0 dc=done ok=true; t0=$(date +%s)
  step "cold restart $id: stopping the server"
  "$1"
  sync; bash -c "${DROP_CACHES_CMD:-echo 3 > /proc/sys/vm/drop_caches}" 2>/dev/null || dc="not allowed"
  step "cold restart $id: page cache dropped: $dc; starting the server"
  "$2" || ok=false
  put_json "$P/restarted-$id" "{\"ok\": $ok, \"drop_caches\": \"$dc\", \"seconds\": $(( $(date +%s) - t0 ))}"
  step "cold restart $id: ok=$ok in $(( $(date +%s) - t0 )) s"
  COLD_DONE=$id
}
