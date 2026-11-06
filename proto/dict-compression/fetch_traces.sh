#!/usr/bin/env bash
# Extract the real agent trajectories already in the repo (M25, branch agenti-bancone: tau2-bench public
# leaderboard, 27 files, 46 MB) into ./traces. Nothing is downloaded from outside the repository.
set -euo pipefail
cd "$(dirname "$0")"
git fetch origin agenti-bancone >/dev/null 2>&1 || true
mkdir -p traces
git -C "$(git rev-parse --show-toplevel)" archive origin/agenti-bancone proto/agenti-bancone/tracce/tau2-classifica | tar x -C traces --strip-components=4
ls traces/*.jsonl.gz | wc -l
