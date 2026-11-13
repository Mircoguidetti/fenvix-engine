#!/bin/bash
# Step 9 · the first script every machine runs after the user-data has unpacked its package.
#   node.sh ROLE NAME BUCKET
#     ROLE   log | cooklog | loader | cook | spare | compactor | client | filler | extras | pgbox | generator | mongod | mongoclient
#     NAME   this machine's folder in the run bucket (log-0, server-0, server-1, spare, compactor, client, filler, extras)
#     BUCKET the run bucket (packages, messages between the runner and the machines, results)
# The same package and the same scripts run on EC2 (mode aws) and in a docker container (mode local, LOCAL_TEST=1,
# S3 = MinIO through AWS_ENDPOINT_URL). No AWS keys on EC2: the machine role (IMDSv2).
set -u
cd "$(dirname "$0")" || exit 1
ROLE=$1; NAME=$2; BUCKET=$3
export AWS_DEFAULT_REGION=eu-west-1 AWS_REGION=eu-west-1
export PYTHONPATH="$PWD/vendor" PYTHONUNBUFFERED=1
# aws-fixes (total review, question 1 point 4): every machine records its zone name and zone ID (the ID names the same
# physical zone in every account: Atlas's primary was in euw1-az2) in results/<NAME>/zone.json, before anything else
zq() { local t; t=$(curl -fsS -m 5 -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 600" 2>/dev/null) &&
  curl -fsS -m 5 -H "X-aws-ec2-metadata-token: $t" "http://169.254.169.254/latest/meta-data/placement/$1" 2>/dev/null; }
if [ -n "${LOCAL_TEST:-}" ]; then Z=${FAKE_AZ:-?}; ZID="local-${FAKE_AZ:-?}"; else Z=$(zq availability-zone || echo "?"); ZID=$(zq availability-zone-id || echo "?"); fi
printf '{"name": "%s", "role": "%s", "zone": "%s", "zone_id": "%s"}\n' "$NAME" "$ROLE" "$Z" "$ZID" > zone.json
python3 s3io.py put "$BUCKET" "results/$NAME/zone.json" zone.json > /dev/null 2>&1 || true
case "$ROLE" in
  log)                         exec bash ./log.sh "$NAME" "$BUCKET" ;;
  cooklog)   # the measured cook and log-0 on one machine (8-vCPU plan): the cook never powers the machine off,
             # log-0 does when the runner stops it last
             S9_NO_POWEROFF=1 bash ./cook.sh cook "$NAME" "$BUCKET" > cook.log 2>&1 &
             exec bash ./log.sh log-0 "$BUCKET" ;;
  loader|cook|spare|compactor) exec bash ./cook.sh "$ROLE" "$NAME" "$BUCKET" ;;
  client)                      exec bash ./client.sh "$BUCKET" ;;       # kitchen/client.sh, unchanged
  filler)                      exec bash ./filler.sh "$BUCKET" ;;
  extras)                      exec bash ./extras.sh "$BUCKET" "$NAME" ;;
  pgbox)                       exec bash ./pgbox.sh "$BUCKET" ;;
  generator)                   exec bash ./generator.sh "$NAME" "$BUCKET" ;;
  mongod)                      exec bash ./mongod.sh "$NAME" "$BUCKET" ;;        # the MongoDB line (aws-local-round)
  mongoclient)                 exec bash ./mongo_client.sh "$BUCKET" "$NAME" ;;
  atlasclient)                 exec bash ./atlas_client.sh "$BUCKET" "$NAME" ;;   # the Atlas line (founder, 2026-11-13)
  *) echo "unknown role $ROLE"; [ -n "${LOCAL_TEST:-}" ] || shutdown -h now "step9: unknown role"; exit 1 ;;
esac
