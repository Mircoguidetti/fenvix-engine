#!/usr/bin/env bash
# The ctl-tests leave one MinIO bucket per test server (ctl-*) behind; the disk allowance of the session is small and a full disk
# makes the engine's writes fail (it showed as insert errors with no writeErrors). Remove the ctl-* buckets older than 1 minute (it runs between two test files, when no test server is alive).
find /tmp/minio-data -maxdepth 1 -name 'ctl-*' -mmin +1 -exec rm -rf {} + 2>/dev/null
df -h / | tail -1
