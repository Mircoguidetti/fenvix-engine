#!/usr/bin/env bash
# Esegue TLC su tutti i modelli di tla-registro e salva gli output in tla-registro/output/.
# Uso: ./run.sh [nome-modello ...]      (senza argomenti: tutti)
# Richiede Java 17+ e tla2tools.jar nella cartella (scaricato dalle release ufficiali:
#   curl -sSLO https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar )
set -u
cd "$(dirname "$0")"
JAR=${TLA2TOOLS:-./tla2tools.jar}
WORKERS=${WORKERS:-auto}
HEAP=${HEAP:-6g}
TIMEOUT=${TIMEOUT:-3600}   # secondi per modello
mkdir -p output

run() { # modulo cfg
  local mod=$1 cfg=$2 name=${2%.cfg}
  echo "== $name ($(date -u +%H:%M:%S))"
  rm -rf "states-$name"
  ( date -u
    timeout "$TIMEOUT" java -Xmx"$HEAP" -XX:+UseParallelGC -cp "$JAR" tlc2.TLC \
        -workers "$WORKERS" -deadlock -config "$cfg" -metadir "states-$name" "$mod"
    echo "EXIT=$?"; date -u ) > "output/$name.txt" 2>&1
  grep -v JAVA_TOOL_OPTIONS "output/$name.txt" | grep -E 'violated|Error:|states generated|Finished|EXIT' | tail -4
  rm -rf "states-$name" ./*_TTrace_*.tla ./*_TTrace_*.bin 2>/dev/null
}

if [ $# -gt 0 ]; then
  list=( "$@" )                                      # nell'ordine dato
else
  list=( $(ls ValidatingLog-*.cfg LettureScarico-*.cfg Bollettino-*.cfg 2>/dev/null | sed 's/\.cfg$//') )
fi
for name in "${list[@]}"; do
  cfg=$name.cfg
  [ -f "$cfg" ] || { echo "manca $cfg"; continue; }
  case $cfg in
    ValidatingLog-*) run MCValidatingLog "$cfg" ;;
    LettureScarico-*) run MCLettureScarico "$cfg" ;;
    Bollettino-*) run MCBollettino "$cfg" ;;
  esac
done
