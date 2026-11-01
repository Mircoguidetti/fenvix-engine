#!/usr/bin/env bash
# Riproduce tutti i controlli TLC della regola del turno. Serve Java 17+ e tla2tools.jar
# (release ufficiale v1.8.0 di tlaplus/tlaplus). Uso: TLA2TOOLS=/percorso/tla2tools.jar ./run.sh
set -uo pipefail
cd "$(dirname "$0")"
J=${TLA2TOOLS:-/opt/tla/tla2tools.jar}
mkdir -p output
for cfg in Turno.cfg Turno_medio.cfg Turno_medio3.cfg Turno_neg_*.cfg ${GRANDE:+Turno_grande.cfg}; do
  n=${cfg%.cfg}
  java -Xmx4g -XX:+UseParallelGC -cp "$J" tlc2.TLC -workers auto -deadlock -config "$cfg" Turno.tla -metadir "/tmp/tlc-$n" > "output/$n.txt" 2>&1
  printf '%-28s %s\n' "$n" "$(grep -E 'is violated|No error has been found' "output/$n.txt" | head -1)"
done
# Atteso: Turno, Turno_medio, Turno_medio3 (e Turno_grande con GRANDE=1) "No error has been found";
# ogni Turno_neg_* una violazione (A: UnSoloVincitore; A2, B, C, D, E: NessunaConfermaPersa; F, G: ManifestNonPerde).
