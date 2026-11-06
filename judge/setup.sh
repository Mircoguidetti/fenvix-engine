#!/usr/bin/env bash
# One-time setup of the judge's tools, inside any session sandbox: no docker, no GitHub Actions minutes, $0.
# Source: conda-forge (reachable from the session sandboxes; Docker Hub and fastdl.mongodb.org are not).
#   mongod 7.0  the reference: the same queries go to it and to mongo-server, and the answers must match (guarantee 7)
#   minio       the local S3
#   python      with pymongo, boto3, pytest
# Everything goes under $JUDGE_HOME (default ~/.fenvix-judge). Idempotent.
set -euo pipefail
JH=${JUDGE_HOME:-$HOME/.fenvix-judge}; mkdir -p "$JH"
ROOT=$(cd "$(dirname "$0")/.." && pwd)
MM="$JH/micromamba/bin/micromamba"
if [ ! -x "$MM" ]; then
  echo "judge setup: fetching micromamba from conda-forge"
  mkdir -p "$JH/micromamba"
  curl -sSfL https://conda.anaconda.org/conda-forge/linux-64/micromamba-2.10.0-0.tar.bz2 | tar -xj -C "$JH/micromamba"
fi
if [ ! -x "$JH/env/bin/mongod" ] || [ ! -x "$JH/env/bin/minio" ] || [ ! -x "$JH/env/bin/pytest" ]; then
  echo "judge setup: creating the environment (mongod 7.0, minio, python) from conda-forge"
  MAMBA_ROOT_PREFIX="$JH/root" "$MM" create -y -q -p "$JH/env" -c conda-forge --override-channels \
    'mongodb=7.0' minio-server 'python=3.12' 'pymongo>=4.6,<5' boto3 pytest
fi
if [ ! -d "$ROOT/mongo-tests/node/node_modules" ]; then
  (cd "$ROOT/mongo-tests/node" && npm ci --silent)
fi
"$JH/env/bin/mongod" --version | head -1
echo "judge setup: ready in $JH"
