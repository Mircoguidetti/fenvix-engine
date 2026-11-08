#!/usr/bin/env bash
# The raw-reader check (compaction 2 plan, round 2, fix 3, and its review): every direct SlateDB reader of the engine
# must be in the `fx` module (src/fx/) or in src/lifecycle.rs, where the merged reader (SlateDB above X_d, our runs at or
# below it) is applied. A reader anywhere else would read SlateDB alone, which is wrong once step 4 drops absorbed rows.
# The files below are named on purpose, each with its reason; any other match fails the check.
# Patterns: DbReader::builder, SstReader::new, the DbSnapshot type, and a SlateDB snapshot taken with `.snapshot().await`.
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd); cd "$ROOT"
declare -A WHY=(
  [src/maps.rs]="T1's maps on the counter: warms the index and map blocks of SlateDB's SSTs, never answers a query; under way (b) it must warm our runs too (review of plan round 2, fix 3)"
  [src/warm.rs]="the warm spare: a FollowLatest reader and SstReader that only warm caches; it must warm our runs (plan round 2, fix 3, item 7; step 6)"
  [src/tiered.rs]="sst_info, a diagnostic, and compaction-1's tiered scheduler, which stays during the rollback window (review of plan round 2, fix 3)"
  [mongo-server/src/prefetch.rs]="read-ahead: warms the counter's prefetch cache, never answers a query; it must warm our runs (plan round 2, fix 3, item 7)"
  [src/counter.rs]="its tests only (a reader built inside #[cfg(test)] mod tests)"
  [src/experiment.rs]="a research experiment of the tenant-bench binary, not a server path"
  [src/lifecycle_exp.rs]="a research experiment of the tenant-bench binary, not a server path"
)
PAT='DbReader::builder|SstReader::new|DbSnapshot|\.snapshot\(\)\.await'
bad=0; named=0
while IFS= read -r line; do
  f=${line%%:*}
  case "$f" in src/fx/*|src/lifecycle.rs) continue ;; esac
  if [ -n "${WHY[$f]:-}" ]; then
    if [ "$f" = src/counter.rs ]; then
      n=${line#*:}; n=${n%%:*}; start=$(grep -n "^mod tests" src/counter.rs | head -1 | cut -d: -f1)
      if [ -z "$start" ] || [ "$n" -lt "$start" ]; then echo "RAW READER (outside counter.rs's tests): $line"; bad=$((bad+1)); continue; fi
    fi
    named=$((named+1)); continue
  fi
  echo "RAW READER: $line"; bad=$((bad+1))
done < <(grep -rnE "$PAT" --include=*.rs src mongo-server/src loadgen 2>/dev/null)
for f in "${!WHY[@]}"; do echo "named: $f: ${WHY[$f]}"; done | sort
if [ $bad -eq 0 ]; then echo "raw readers: PASS ($named matches, all in named files)"; exit 0; fi
echo "raw readers: FAIL ($bad outside src/fx/, src/lifecycle.rs and the named files)"; exit 1
