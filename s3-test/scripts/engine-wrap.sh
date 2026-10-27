#!/usr/bin/env bash
# SERVER_BIN for mongo-tests/server.sh on real S3: appends the store flags to whatever server.sh passes
# (server.sh also runs when a test restarts the server, so the flags cannot be given on its command line).
export S3T_COUNT_DIR=${S3T_COUNT_DIR:-/var/lib/s3test/counts}; mkdir -p "$S3T_COUNT_DIR"
exec /opt/s3test/bin/mongo-server "$@" --store s3-aws --bucket "$S3T_BUCKET" --region eu-central-1 --s3-max-rps ${S3T_MAX_RPS:-300} --s3-process-cap-usd 4.0 ${S3T_ENGINE_FLAGS:-}
