#!/usr/bin/env bash
# Esegue TLC sui modelli di tla-rami e salva gli output in tla-rami/output/.
# Uso: ./run.sh [nome-modello ...]   (senza argomenti: tutti; nome = file .cfg senza estensione)
# Richiede Java 17+ e tla2tools.jar (release ufficiale v1.8.0 di tlaplus/tlaplus):
#   curl -sSLO https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
set -u
cd "$(dirname "$0")"
JAR=${TLA2TOOLS:-./tla2tools.jar}
[ -f "$JAR" ] || JAR=../tla-registro/tla2tools.jar
WORKERS=${WORKERS:-auto}
HEAP=${HEAP:-5g}
TIMEOUT=${TIMEOUT:-3600}
mkdir -p output
if [ $# -gt 0 ]; then list=( "$@" ); else list=( $(ls Famiglia-*.cfg | sed 's/\.cfg$//') ); fi
for name in "${list[@]}"; do
  [ -f "$name.cfg" ] || { echo "manca $name.cfg"; continue; }
  echo "== $name ($(date -u +%H:%M:%S))"
  rm -rf "states-$name"
  ( date -u
    timeout "$TIMEOUT" java -Xmx"$HEAP" -XX:+UseParallelGC -cp "$JAR" tlc2.TLC \
        -workers "$WORKERS" -deadlock -config "$name.cfg" -metadir "states-$name" MCFamiglia
    echo "EXIT=$?"; date -u ) 2>&1 | grep --line-buffered -v JAVA_TOOL_OPTIONS > "output/$name.txt"
  grep -E 'violated|Error:|states generated|EXIT' "output/$name.txt" | tail -4
  rm -rf "states-$name" ./*_TTrace_*.tla ./*_TTrace_*.bin 2>/dev/null
done
