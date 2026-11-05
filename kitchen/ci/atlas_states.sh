#!/bin/bash
# K2b · on the runner, before any spending: the Atlas state files our rounds start from. They are code-irlanda's
# irlanda/stati/ at the commit the Atlas round started from (command field "atlas_states", default 501c8fd = the commit
# that launched m30-corto on Atlas). Checks: every file matches that commit's CATENA.txt (md5), and CATENA has no
# "IN CORSO" line (a commit taken while an Atlas round was running would not be a starting point).
# Usage: atlas_states.sh COMMIT OUT_TGZ STATE...   (ATLAS_STATES_DIR instead of git: local test only)
set -euo pipefail
COMMIT=$1; OUT=$2; shift 2
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
if [ -n "${ATLAS_STATES_DIR:-}" ]; then
  cp -r "$ATLAS_STATES_DIR"/. "$T"/
else
  [[ "$COMMIT" =~ ^[0-9a-f]{7,40}$ ]] || { echo "atlas_states: not a commit: $COMMIT"; exit 1; }
  git fetch -q --no-tags origin code-irlanda
  git cat-file -e "$COMMIT^{commit}" || { echo "atlas_states: commit $COMMIT not on code-irlanda"; exit 1; }
  git merge-base --is-ancestor "$COMMIT" FETCH_HEAD || { echo "atlas_states: $COMMIT is not on branch code-irlanda"; exit 1; }
  git archive "$COMMIT" irlanda/stati | tar x -C "$T" --strip-components=2
fi
grep -q '^IN CORSO' "$T/CATENA.txt" && { echo "atlas_states: CATENA.txt of $COMMIT says IN CORSO: not a starting point"; exit 1; }
( cd "$T" && md5sum -c --quiet CATENA.txt ) || { echo "atlas_states: the files do not match CATENA.txt"; exit 1; }
for s in "$@"; do [ -f "$T/$s/risultati_p1.json" ] || { echo "atlas_states: no state file $s at $COMMIT"; exit 1; }; done
tar czf "$OUT" -C "$T" "$@"
echo "Atlas state files of code-irlanda $COMMIT (CATENA checked): $*"
