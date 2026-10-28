# Settings of the test MinIO (tools/fusion/env_up.sh), as the standard AWS variables the engine reads. The engine has no built-in
# credentials, region or endpoint; every script that starts it against MinIO sources this file (or sets the same variables itself).
# They REPLACE any AWS_* already in the environment: a test must never talk to MinIO with real credentials, nor to real S3 with these.
# For real S3 do not use this file: give the engine the real credentials (environment variables or the instance role) and --region.
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1
export AWS_ENDPOINT=http://127.0.0.1:9000
export AWS_ALLOW_HTTP=true
unset AWS_SESSION_TOKEN AWS_PROFILE AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN AWS_CONTAINER_CREDENTIALS_RELATIVE_URI AWS_CONTAINER_CREDENTIALS_FULL_URI
