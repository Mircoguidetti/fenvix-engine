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
  [ -n "${LOCAL_TEST:-}" ] && exit 0
  shutdown -h now "step9: $1"; exit 0
}
# S3 endpoint for our programs (only in local mode: MinIO)
s3_endpoint_args() { [ -n "${LOCAL_TEST:-}" ] && [ -n "${AWS_ENDPOINT_URL:-}" ] && echo "--endpoint $AWS_ENDPOINT_URL"; }
