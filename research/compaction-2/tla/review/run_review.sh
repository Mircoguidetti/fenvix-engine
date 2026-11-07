#!/usr/bin/env bash
# Runs TLC on the review's configurations: ./run_review.sh StaleOwner StaleOwner-fix ...  or  all
set -u
cd "$(dirname "$0")"
JAR=${TLA2TOOLS:-/home/user/tlatools/tla2tools.jar}
mkdir -p output
names="$*"
[ "$names" = all ] && names="$(ls MCR-*.cfg | sed 's/^MCR-//; s/\.cfg$//' | tr '\n' ' ')"
for m in $names; do
  echo "== $m"
  meta=$(mktemp -d)
  java -XX:+UseParallelGC -Xmx${XMX:-10g} -cp "$JAR" tlc2.TLC -workers "${WORKERS:-4}" -deadlock \
    -metadir "$meta" -config "MCR-$m.cfg" MCR.tla > "output/$m.txt" 2>&1
  sed -i '/^Picked up JAVA_TOOL_OPTIONS/d' "output/$m.txt"
  grep -E "^Error|is violated|distinct states|No error has been found|depth of the complete" "output/$m.txt" | head -6
  rm -rf "$meta"
done
