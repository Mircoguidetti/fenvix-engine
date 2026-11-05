#!/bin/bash
# Prova locale M31 · metriche.sh (modo "metriche") con un `aws` FINTO (bin-metriche/aws): 0 $, nessun contatto con AWS.
# Controlla per ogni scenario: macchina trovata, tutte le metriche scaricate (metriche x 5 statistiche, Period 60 e 300),
# file salvati con INDICE.txt giusto (dimensione e SHA-256), nessun numero di account nei file, nessuna chiamata che
# scrive su AWS.
set -u
QUI=$(cd "$(dirname "$0")" && pwd); G=$(cd "$QUI/.." && pwd)
BASE=$(mktemp -d); export PATH="$QUI/bin-metriche:$PATH" M31_ACCT=123456789012 FINTO_LOG
falliti=0
ok() { if eval "$2"; then echo "OK      $1"; else echo "FALLITO $1"; falliti=$((falliti+1)); fi; }
# scenario NOME ATTESO_CODICE N_EC2 N_EBS_DISCHI [VAR=valore ...] -- comando.json
scenario() {
  local nome=$1 atteso=$2 nec2=$3 ndischi=$4; shift 4
  local env=(); while [ "$1" != "--" ]; do env+=("$1"); shift; done; shift
  local R=$BASE/$nome/risultati/aws-41999425773 C=$BASE/$nome/comando.json
  mkdir -p "$R"; echo "$1" > "$C"; [ -n "${2:-}" ] && echo "$2" > "$R/giornata.json"
  FINTO_LOG=$BASE/$nome/chiamate.log; : > "$FINTO_LOG"
  echo "== $nome"
  env "${env[@]}" M31_COMANDO="$C" GITHUB_STEP_SUMMARY=/dev/null bash "$G/metriche.sh" 41999425773 "$R" > "$BASE/$nome/out.txt" 2>&1
  local rc=$? O=$R/cloudwatch
  ok "codice di uscita $rc (atteso $atteso)" "[ $rc = $atteso ]"
  [ "$atteso" = 0 ] || { sed 's/^/   /' "$BASE/$nome/out.txt" | tail -3; return; }
  ok "macchina trovata (i-0abc123def4567890 in finestra.json)" "jq -e '.istanze==[\"i-0abc123def4567890\"]' $O/finestra.json >/dev/null"
  local serie_attese=$(( (nec2 + ndischi * 11) * 5 * 2 ))
  local serie; serie=$(cat $O/get-metric-data-*.json | jq -s '[.[].MetricDataResults|length]|add')
  ok "tutte le metriche scaricate: $serie serie (attese $serie_attese = metriche x 5 statistiche x 2 periodi)" "[ $serie = $serie_attese ]"
  ok "Period 60 e 300 presenti per la macchina" "ls $O/get-metric-data-ec2-*-p60-0.json $O/get-metric-data-ec2-*-p300-0.json >/dev/null 2>&1"
  ok "dischi scaricati: $ndischi" "[ \$(ls $O/list-metrics-ebs-vol-*.json 2>/dev/null | wc -l) = $ndischi ]"
  ok "il disco vivo di un'altra macchina è scartato" "! ls $O/*vol-0bbb222* >/dev/null 2>&1"
  ok "nessun numero di account nei file" "! grep -rq 123456789012 $O"
  ok "INDICE.txt: ogni file con dimensione e SHA-256 giusti" "(cd $O && while read -r s h f; do [ \"\$(stat -c %s \$f)\" = \$s ] && [ \"\$(sha256sum \$f|cut -d' ' -f1)\" = \$h ] || exit 1; done < INDICE.txt) && [ \$(wc -l < $O/INDICE.txt) = \$(( \$(ls $O | wc -l) - 1 )) ]"
  ok "solo letture (describe-*, list-metrics, get-metric-data)" "! grep -vE '^(ec2 describe-(instances|volumes)|cloudwatch (list-metrics|get-metric-data)) ' $FINTO_LOG"
  ok "niente domande più grandi di 500 per chiamata" "! grep -q 'più di 500' $BASE/$nome/out.txt"
  sed 's/^/   /' "$O/note.txt"
}
G1='{"esito": "FINITA", "minuti": 97, "macchina": "c7i.xlarge", "id": "i-0abc123def4567890"}'
scenario A-giornata-json 0 17 1 -- '{"modo":"metriche","run":"41999425773"}' "$G1"
scenario B-solo-tag 0 17 1 -- '{"modo":"metriche","run":"41999425773"}'
scenario C-dischi-dalla-macchina 0 17 1 FINTO_MAPPINGS=1 -- '{"modo":"metriche","run":"41999425773"}' "$G1"
scenario D-macchina-non-piu-visibile 0 17 1 FINTO_VISIBILE=0 -- '{"modo":"metriche","run":"41999425773","istanze":["i-0abc123def4567890"]}'
scenario E-comando-con-finestra 0 17 1 -- '{"modo":"metriche","run":"41999425773","istanze":["i-0abc123def4567890"],"inizio":"2026-11-05T08:00:00Z","fine":"2026-11-05T10:00:00Z"}'
scenario F-120-metriche-due-blocchi 0 120 1 FINTO_N_EC2=120 -- '{"modo":"metriche","run":"41999425773"}' "$G1"
scenario G-macchina-introvabile 1 0 0 FINTO_VISIBILE=0 -- '{"modo":"metriche","run":"41999425773"}'
scenario H-cloudwatch-negato 1 0 0 FINTO_CW_NEGATO=1 -- '{"modo":"metriche","run":"41999425773"}' "$G1"
ok "H: l'errore AccessDenied è scritto in note.txt senza il numero dell'account" "grep -q 'MANCA ListMetrics' $BASE/H-cloudwatch-negato/risultati/aws-41999425773/cloudwatch/note.txt && ! grep -rq 123456789012 $BASE/H-cloudwatch-negato/risultati/aws-41999425773/cloudwatch"
ok "E: la finestra di comando.json è usata" "jq -e '.inizio==\"2026-11-05T08:00:00Z\" and .fine==\"2026-11-05T10:00:00Z\"' $BASE/E-comando-con-finestra/risultati/aws-41999425773/cloudwatch/finestra.json >/dev/null"
ok "A: finestra = lancio - 10 min .. cancellazione + 15 min" "jq -e '.inizio==\"2026-11-05T07:57:31Z\" and .fine==\"2026-11-05T09:59:10Z\"' $BASE/A-giornata-json/risultati/aws-41999425773/cloudwatch/finestra.json >/dev/null"
ok "F: 600 domande per Period divise in 2 blocchi" "ls $BASE/F-120-metriche-due-blocchi/risultati/aws-41999425773/cloudwatch/get-metric-data-ec2-*-p60-1.json >/dev/null"
echo "METRICHE PROVA LOCALE: $falliti controlli falliti (cartella $BASE)"
[ "$falliti" = 0 ]
