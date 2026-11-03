#!/usr/bin/env bash
# Fa girare TLC sui modelli indicati (nomi senza prefisso), output in output/<modello>.txt
# Uso: TLA2TOOLS=/percorso/tla2tools.jar WORKERS=4 [TIMEOUT=1200] ./run.sh lettori neg-blindput ...
set -u
cd "$(dirname "$0")"
JAR=${TLA2TOOLS:-/home/user/tlatools/tla2tools.jar}
W=${WORKERS:-4}
mkdir -p output
for m in "$@"; do
  extra=""
  echo "== $m"
  # TIMEOUT (secondi, facoltativo) ferma java stesso, non solo questo script
  timeout "${TIMEOUT:-0}" java -XX:+UseParallelGC -Xmx${XMX:-10g} -cp "$JAR" tlc2.TLC -workers "$W" $extra \
    -metadir "/tmp/tlc-states-$m" -config "LavaggioPerCliente-$m.cfg" LavaggioPerCliente.tla \
    > "output/$m.txt" 2>&1
  grep -E "Error:|violated|states generated|distinct states|Finished|No error" "output/$m.txt" | head -8
  rm -rf "/tmp/tlc-states-$m"
done
