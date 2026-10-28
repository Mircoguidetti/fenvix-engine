# Source this file. `refresh_aws_creds [max_age_secs]` asks GitHub for a fresh OIDC token and assumes the role again
# (3600 s session), exporting new AWS_* variables into the current shell, unless the current ones are younger than
# max_age_secs (default 2400 = 40 min). A step that runs longer than the 1 h session calls it from its loops, so
# credentials are always fresh. Needs ACTIONS_ID_TOKEN_REQUEST_URL/TOKEN (workflow permission id-token: write) and
# AWS_ROLE_ARN. Set AWS_CREDS_AT=$(date +%s) right after a fresh login (configure-aws-credentials) to start the clock.
refresh_aws_creds() {
  local max=${1:-2400} now tok out
  now=$(date +%s)
  if [ -n "${AWS_CREDS_AT:-}" ] && [ $((now - AWS_CREDS_AT)) -lt "$max" ]; then return 0; fi
  tok=$(curl -sS -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=sts.amazonaws.com" | jq -r .value) || return 1
  [ -n "$tok" ] && [ "$tok" != null ] || { echo "refresh_aws_creds: no OIDC token" >&2; return 1; }
  echo "::add-mask::$tok"
  out=$(env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN aws sts assume-role-with-web-identity \
        --role-arn "$AWS_ROLE_ARN" --role-session-name "gha-$GITHUB_RUN_ID-$now" --web-identity-token "$tok" --duration-seconds 3600 --output json) || {
        echo "refresh_aws_creds: assume-role failed" >&2; return 1; }
  AWS_ACCESS_KEY_ID=$(jq -r .Credentials.AccessKeyId <<<"$out"); AWS_SECRET_ACCESS_KEY=$(jq -r .Credentials.SecretAccessKey <<<"$out")
  AWS_SESSION_TOKEN=$(jq -r .Credentials.SessionToken <<<"$out")
  echo "::add-mask::$AWS_SECRET_ACCESS_KEY"; echo "::add-mask::$AWS_SESSION_TOKEN"
  export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
  export AWS_CREDS_AT=$now
  echo "credentials refreshed at $(date -u +%H:%M:%S)" >&2
}
