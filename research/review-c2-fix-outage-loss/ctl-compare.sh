#!/usr/bin/env bash
# file-by-file: the branch's full ctl set (sealed2+derive) against integration-3's (research/integration-3/results/ctl-full-final-sealed2-derive)
A=${1:-/home/user/w-fix/research/review-c2-fix-outage-loss/results/ctl-full-fix-s2d}; B=/home/user/w-fix/research/integration-3/results/ctl-full-final-sealed2-derive
st() { f=$1; [ -f "$f" ] || { echo "absent"; return; }; l=$(tail -1 "$f"); if echo "$l" | grep -qE "[0-9]+ failed|error"; then echo "FAIL: $l"; elif echo "$l" | grep -qE "passed|skipped"; then echo "PASS: $l"; else echo "NO RESULT (timeout): '$l'"; fi; }
pa=0; pb=0; n=0
for f in $(ls $A/test_*.log | xargs -n1 basename); do
  sa=$(st $A/$f); sb=$(st $B/$f); n=$((n+1))
  [ "${sa%%:*}" = PASS ] && pa=$((pa+1)); [ "${sb%%:*}" = PASS ] && pb=$((pb+1))
  [ "${sa%%:*}" != "${sb%%:*}" ] && echo "DIFFERENT ${f%.log}: branch ${sa} | integration-3 ${sb}"
done
echo "branch: $pa of $n files pass; integration-3: $pb pass (of the same files; it has $(ls $B/test_*.log | wc -l) files)"
