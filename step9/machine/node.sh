#!/bin/bash
# Step 9 · the first script every machine runs after the user-data has unpacked its package.
#   node.sh ROLE NAME BUCKET
#     ROLE   log | cooklog | loader | cook | spare | compactor | client | filler | extras
#     NAME   this machine's folder in the run bucket (log-0, server-0, server-1, spare, compactor, client, filler, extras)
#     BUCKET the run bucket (packages, messages between the runner and the machines, results)
# The same package and the same scripts run on EC2 (mode aws) and in a docker container (mode local, LOCAL_TEST=1,
# S3 = MinIO through AWS_ENDPOINT_URL). No AWS keys on EC2: the machine role (IMDSv2).
set -u
cd "$(dirname "$0")" || exit 1
ROLE=$1; NAME=$2; BUCKET=$3
export AWS_DEFAULT_REGION=eu-west-1 AWS_REGION=eu-west-1
export PYTHONPATH="$PWD/vendor" PYTHONUNBUFFERED=1
case "$ROLE" in
  log)                         exec bash ./log.sh "$NAME" "$BUCKET" ;;
  cooklog)   # the measured cook and log-0 on one machine (8-vCPU plan): the cook never powers the machine off,
             # log-0 does when the runner stops it last
             S9_NO_POWEROFF=1 bash ./cook.sh cook "$NAME" "$BUCKET" > cook.log 2>&1 &
             exec bash ./log.sh log-0 "$BUCKET" ;;
  loader|cook|spare|compactor) exec bash ./cook.sh "$ROLE" "$NAME" "$BUCKET" ;;
  client)                      exec bash ./client.sh "$BUCKET" ;;       # kitchen/client.sh, unchanged
  filler)                      exec bash ./filler.sh "$BUCKET" ;;
  extras)                      exec bash ./extras.sh "$BUCKET" ;;
  *) echo "unknown role $ROLE"; [ -n "${LOCAL_TEST:-}" ] || shutdown -h now "step9: unknown role"; exit 1 ;;
esac
