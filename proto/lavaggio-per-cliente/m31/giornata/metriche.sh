#!/bin/bash
# M31 · scarico delle metriche CloudWatch della macchina di una giornata (modo "metriche" di comando.json).
# SOLO letture: ec2:DescribeInstances, ec2:DescribeVolumes, cloudwatch:ListMetrics, cloudwatch:GetMetricData.
# Non crea, non cambia e non cancella niente su AWS.
#
# Uso: metriche.sh RUN [CARTELLA_RISULTATI]   (RUN = id della run della giornata, per esempio 41999425773)
# Opzioni in comando.json (tutte facoltative, servono solo se il modo automatico non basta):
#   "istanze": ["i-..."]  macchine da scaricare (altrimenti: giornata.json della run, poi DescribeInstances col tag)
#   "volumi":  ["vol-..."] dischi EBS da scaricare (altrimenti: dalla macchina, poi le metriche AWS/EBS recenti)
#   "inizio", "fine": ora UTC AAAA-MM-GGTHH:MM:SSZ (altrimenti: lancio della macchina - 10 min, cancellazione + 15 min)
#
# Come trova la macchina, dal più robusto al meno:
#   1. "istanze" in comando.json;
#   2. il campo "id" di risultati/aws-RUN/giornata.json, scritto dalla giornata e pubblicato sul branch;
#   3. DescribeInstances con il tag project=fenvix-coldread, lanciate nelle ultime 24 ore (una macchina cancellata resta
#      visibile circa un'ora).
# I dischi: dalla macchina (BlockDeviceMappings, se ancora visibili), poi le metriche AWS/EBS attive nelle ultime 3 ore
# di dischi che non esistono più o hanno il tag (il nostro disco si cancella con la macchina; i dischi vivi senza tag
# sono di altre macchine e si scartano).
#
# Risultato in CARTELLA_RISULTATI/cloudwatch/ (di serie $M/risultati/aws-RUN/cloudwatch): JSON grezzi di AWS, finestra.json,
# INDICE.txt (dimensione in byte e SHA-256 di ogni file). Il numero dell'account si sostituisce con <ACCOUNT_ID>.
set -uo pipefail
RUN=${1:?uso: metriche.sh RUN [CARTELLA_RISULTATI]}
[[ "$RUN" =~ ^[0-9]+$ ]] || { echo "run non valida: $RUN"; exit 1; }
QUI=$(cd "$(dirname "$0")" && pwd)
R=${2:-$QUI/../risultati/aws-$RUN}
CMD=${M31_COMANDO:-$QUI/comando.json}
OUT=$R/cloudwatch; mkdir -p "$OUT"
NOTE=$OUT/note.txt; : > "$NOTE"
nota() { echo "$*" | tee -a "$NOTE"; }
ADESSO=$(date -u +%FT%TZ)
opz() { jq -r "$1 // empty" "$CMD" 2>/dev/null; }
# secondi <-> ora UTC
sec() { date -u -d "$1" +%s; }
ora() { date -u -d "@$1" +%FT%TZ; }

# 1. le macchine
mapfile -t IDS < <(jq -r '.istanze[]? // empty' "$CMD" 2>/dev/null)
fonte="comando.json"
if [ ${#IDS[@]} -eq 0 ] && [ -f "$R/giornata.json" ]; then
  mapfile -t IDS < <(jq -r '.id // empty | select(startswith("i-"))' "$R/giornata.json"); fonte="giornata.json della run"
fi
if [ ${#IDS[@]} -eq 0 ]; then
  fonte="DescribeInstances col tag, ultime 24 ore"
  da=$(date -u -d '24 hours ago' +%FT%TZ)
  mapfile -t IDS < <(aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-coldread --output json 2>/dev/null \
    | jq -r --arg da "$da" '.Reservations[].Instances[] | select((.LaunchTime|sub("\\.[0-9]+";"")|sub("\\+00:00$";"Z")) >= $da) | .InstanceId')
fi
[ ${#IDS[@]} -gt 0 ] || { nota "METRICHE: nessuna macchina trovata (né in comando.json, né in giornata.json, né col tag nelle ultime 24 ore)"; exit 1; }
nota "macchine: ${IDS[*]} (fonte: $fonte)"

# descrizione delle macchine: lancio, stato, motivo e ora della cancellazione, dischi (se ancora visibili)
DESC=$OUT/describe-instances.json
if ! aws ec2 describe-instances --instance-ids "${IDS[@]}" --output json > "$DESC" 2>"$OUT/describe-instances.err"; then
  nota "DescribeInstances non riuscito (macchina non più visibile?): $(head -c 300 "$OUT/describe-instances.err")"
  echo '{"Reservations":[]}' > "$DESC"
fi
rm -f "$OUT/describe-instances.err"

# 2. la finestra: dall'inizio della run a fine run + 15 minuti
INIZIO=$(opz .inizio); FINE=$(opz .fine)
if [ -z "$INIZIO" ]; then
  lancio=$(jq -r '[.Reservations[].Instances[].LaunchTime] | min // empty' "$DESC")
  if [ -n "$lancio" ]; then INIZIO=$(ora $(( $(sec "$lancio") - 600 ))); nota "inizio: lancio della macchina $lancio meno 10 minuti"
  else INIZIO=$(date -u -d '6 hours ago' +%FT%TZ); nota "inizio: lancio non visibile, uso le ultime 6 ore"; fi
fi
if [ -z "$FINE" ]; then
  # "User initiated (2026-11-05 10:45:12 GMT)": l'ora della cancellazione, se AWS la scrive
  canc=$(jq -r '[.Reservations[].Instances[].StateTransitionReason | capture("\\((?<t>[0-9-]+ [0-9:]+) GMT\\)").t] | max // empty' "$DESC")
  if [ -n "$canc" ]; then FINE=$(ora $(( $(sec "$canc UTC") + 900 ))); nota "fine: cancellazione della macchina $canc UTC più 15 minuti"
  else FINE=$ADESSO; nota "fine: ora della cancellazione non visibile, uso adesso"; fi
fi
[ "$(sec "$FINE")" -gt "$(sec "$ADESSO")" ] && { FINE=$ADESSO; nota "fine: spostata ad adesso (non si chiedono dati futuri)"; }
[ "$(sec "$FINE")" -gt "$(sec "$INIZIO")" ] || { nota "METRICHE: finestra vuota ($INIZIO - $FINE)"; exit 1; }
nota "finestra: $INIZIO - $FINE"

# 3. i dischi
mapfile -t VOLS < <(jq -r '.volumi[]? // empty' "$CMD" 2>/dev/null)
if [ ${#VOLS[@]} -eq 0 ]; then
  mapfile -t VOLS < <(jq -r '.Reservations[].Instances[].BlockDeviceMappings[]?.Ebs.VolumeId // empty' "$DESC")
  [ ${#VOLS[@]} -gt 0 ] && nota "dischi: dalla macchina"
fi
if [ ${#VOLS[@]} -eq 0 ]; then
  # metriche AWS/EBS attive nelle ultime 3 ore; si tengono i dischi che non esistono più o che hanno il nostro tag
  aws cloudwatch list-metrics --namespace AWS/EBS --recently-active PT3H --output json > "$OUT/list-metrics-ebs-recenti.json" 2>/dev/null \
    || { nota "ListMetrics AWS/EBS non riuscito"; echo '{"Metrics":[]}' > "$OUT/list-metrics-ebs-recenti.json"; }
  for v in $(jq -r '[.Metrics[].Dimensions[] | select(.Name=="VolumeId") | .Value] | unique | .[]' "$OUT/list-metrics-ebs-recenti.json"); do
    if j=$(aws ec2 describe-volumes --volume-ids "$v" --output json 2>"$OUT/.err"); then
      jq -e '.Volumes[0].Tags[]? | select(.Key=="project" and .Value=="fenvix-coldread")' <<<"$j" >/dev/null && VOLS+=("$v")
    elif grep -q 'InvalidVolume.NotFound' "$OUT/.err"; then VOLS+=("$v")
    else nota "disco $v: DescribeVolumes non riuscito, scartato: $(head -c 200 "$OUT/.err")"; fi
  done
  rm -f "$OUT/.err"
  nota "dischi: dalle metriche AWS/EBS recenti (dischi cancellati o col tag)"
fi
nota "dischi: ${VOLS[*]:-nessuno}"

# 4. elenco delle metriche e scarico, Period 60 e 300, cinque statistiche
STAT='["Average","Maximum","Minimum","Sum","SampleCount"]'
scarica() { # scarica NOME NAMESPACE DIMENSIONE VALORE
  local nome=$1 ns=$2 dim=$3 val=$4 lm=$OUT/list-metrics-$1.json n p
  if ! aws cloudwatch list-metrics --namespace "$ns" --dimensions Name="$dim",Value="$val" --output json > "$lm" 2>"$OUT/.err"; then
    nota "MANCA ListMetrics $ns $val: $(head -c 300 "$OUT/.err")"; rm -f "$OUT/.err"; ERRORI=$((ERRORI+1)); return; fi
  rm -f "$OUT/.err"
  n=$(jq '.Metrics | length' "$lm"); nota "$ns $val: $n metriche"
  [ "$n" -gt 0 ] || return
  for p in 60 300; do
    # al massimo 500 domande per chiamata (limite di GetMetricData): blocchi da 500
    python3 - "$lm" "$p" "$STAT" "$OUT/q-$nome-p$p" <<'PY'
import json, sys
lm, period, stats, base = sys.argv[1], int(sys.argv[2]), json.loads(sys.argv[3]), sys.argv[4]
qs = []
for i, m in enumerate(sorted(json.load(open(lm))["Metrics"], key=lambda m: m["MetricName"])):
    for s in stats:
        qs.append({"Id": f"m{i}_{s.lower()}", "Label": f'{m["MetricName"]} {s}',
                   "MetricStat": {"Metric": m, "Period": period, "Stat": s}, "ReturnData": True})
for k in range(0, len(qs), 500):
    json.dump(qs[k:k+500], open(f"{base}-{k//500}.json", "w"))
PY
    for q in "$OUT"/q-"$nome"-p"$p"-*.json; do
      local k=${q##*-}; k=${k%.json} f=$OUT/get-metric-data-$nome-p$p-$k.json
      if aws cloudwatch get-metric-data --metric-data-queries "file://$q" --start-time "$INIZIO" --end-time "$FINE" \
           --scan-by TimestampAscending --output json > "$f" 2>"$OUT/.err"; then
        nota "  Period $p blocco $k: $(jq '.MetricDataResults|length' "$f") serie, $(jq '[.MetricDataResults[].Values|length]|add // 0' "$f") punti"
      else nota "MANCA GetMetricData $val Period $p: $(head -c 300 "$OUT/.err")"; ERRORI=$((ERRORI+1)); rm -f "$f"; fi
      rm -f "$OUT/.err" "$q"
    done
  done
}
ERRORI=0
for i in "${IDS[@]}"; do scarica "ec2-$i" AWS/EC2 InstanceId "$i"; done
for v in "${VOLS[@]}"; do scarica "ebs-$v" AWS/EBS VolumeId "$v"; done

# 5. finestra, account, indice
indice() { ( cd "$OUT" && for f in *; do [ "$f" = INDICE.txt ] || echo "$(stat -c %s "$f") $(sha256sum "$f" | cut -d' ' -f1) $f"; done ) > "$OUT/INDICE.txt"; }
jq -n --arg run "$RUN" --arg inizio "$INIZIO" --arg fine "$FINE" --arg adesso "$ADESSO" --arg fonte "$fonte" \
  --argjson istanze "$(printf '%s\n' "${IDS[@]}" | jq -R . | jq -s .)" \
  --argjson volumi "$(printf '%s\n' "${VOLS[@]}" | jq -R 'select(length>0)' | jq -s .)" \
  '{run:$run, inizio:$inizio, fine:$fine, scaricato:$adesso, fonte_istanze:$fonte, istanze:$istanze, volumi:$volumi,
    periodi:[60,300], statistiche:["Average","Maximum","Minimum","Sum","SampleCount"]}' > "$OUT/finestra.json"
if [ -n "${M31_ACCT:-}" ]; then
  grep -rl "$M31_ACCT" "$OUT" 2>/dev/null | xargs -r sed -i "s/$M31_ACCT/<ACCOUNT_ID>/g"
  ! grep -rq "$M31_ACCT" "$OUT" || { echo "account ancora nei file: non pubblico"; rm -rf "$OUT"; exit 1; }
fi
indice
nota "METRICHE: $(($(wc -l < "$OUT/INDICE.txt"))) file in $OUT, errori $ERRORI"
indice
cat "$NOTE" >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
[ "$ERRORI" -eq 0 ]
