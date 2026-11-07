#!/usr/bin/env bash
# Runs TLC on the named configurations (MC-<name>.cfg), output in output/<name>.txt.
# Optional TLCARGS (e.g. TLCARGS="-fpmem 0.5"), and DURATION (e.g. 90m) to stop a run that does not close.
# Usage: TLA2TOOLS=/path/tla2tools.jar WORKERS=4 [XMX=10g] ./run.sh main DropBeforePublish ...
#        ./run.sh all        the correct model, then every negative control
set -u
cd "$(dirname "$0")"
JAR=${TLA2TOOLS:-/home/user/tlatools/tla2tools.jar}
W=${WORKERS:-4}
mkdir -p output
names="$*"
[ "$names" = all ] && names="main $(ls MC-*.cfg | sed 's/^MC-//; s/\.cfg$//' | grep -v '^main$' | tr '\n' ' ')"
for m in $names; do
  echo "== $m"
  meta=$(mktemp -d)
  timeout -s INT "${DURATION:-0}" java -XX:+UseParallelGC -Xmx${XMX:-10g} -cp "$JAR" tlc2.TLC -workers "$W" -deadlock ${TLCARGS:-} \
    -metadir "$meta" -config "MC-$m.cfg" MC.tla > "output/$m.txt" 2>&1
  sed -i '/^Picked up JAVA_TOOL_OPTIONS/d' "output/$m.txt"
  grep -E "^Error: Invariant|is violated|states generated|distinct states|^Finished|No error has been found|depth of the complete" "output/$m.txt" | head -8
  rm -rf "$meta"
done
