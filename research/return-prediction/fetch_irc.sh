#!/usr/bin/env bash
# Download Ubuntu IRC channel logs (https://irclogs.ubuntu.com, public logs) for a date range.
# Usage: fetch_irc.sh OUT_DIR START_DATE END_DATE CHANNEL...   (dates YYYY-MM-DD, channel names without '#')
# Files land as OUT_DIR/<channel>/<YYYY-MM-DD>.txt. Polite: 4 parallel requests at most, skips files already present.
set -euo pipefail
out=$1; start=$2; end=$3; shift 3
jobs=()
d=$start
while [[ "$d" < "$end" || "$d" == "$end" ]]; do
  for ch in "$@"; do
    mkdir -p "$out/$ch"
    f="$out/$ch/$d.txt"
    [[ -s "$f" ]] && continue
    jobs+=("https://irclogs.ubuntu.com/${d//-//}/%23$ch.txt $f")
  done
  d=$(date -I -d "$d + 1 day")
done
printf '%s\n' "${jobs[@]}" | xargs -P 4 -L 1 sh -c 'curl -s -f -m 60 -o "$1" "$0" || echo "missing $0"'
