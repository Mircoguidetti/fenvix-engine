#!/usr/bin/env bash
# Legge proto/bancone/run.json ({"passo": "nome"}) ed esegue proto/bancone/ci/passo-<nome>.sh.
set -uo pipefail
cd "$(dirname "$0")/../../.."
P=$(python3 -c 'import json;print(json.load(open("proto/bancone/run.json"))["passo"])')
PARTI=$(python3 -c 'import json;print(" ".join(json.load(open("proto/bancone/run.json")).get("parti",["a"])))')
case " $PARTI " in *" ${PARTE:-a} "*) ;; *) echo "parte ${PARTE:-a} non richiesta"; exit 0;; esac
OUT=proto/bancone/risultati/$P/${PARTE:-a}
mkdir -p "$OUT" ~/tracce
export OUT TR=~/tracce PARTE
start=$(date +%s)
bash proto/bancone/ci/passo-$P.sh 2>&1 | tail -c 200000 > "$OUT/log.txt"
echo "durata_s $(( $(date +%s) - start ))" >> "$OUT/log.txt"
du -sh ~/tracce >> "$OUT/log.txt" 2>&1
