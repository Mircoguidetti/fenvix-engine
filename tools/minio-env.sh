# Settings of the test MinIO (tools/fusion/env_up.sh), as the standard AWS variables the engine reads. The engine has no built-in
# credentials, region or endpoint; every script that starts it against MinIO sources this file (or sets the same variables itself).
# For real S3 do not use this file: give the engine the real credentials (environment variables or the instance role) and --region.
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-minioadmin}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-minioadmin}
export AWS_REGION=${AWS_REGION:-us-east-1}
export AWS_ENDPOINT=${AWS_ENDPOINT:-http://127.0.0.1:9000}
export AWS_ALLOW_HTTP=${AWS_ALLOW_HTTP:-true}
