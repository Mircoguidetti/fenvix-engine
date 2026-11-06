#!/bin/bash
# M31 · controllo finale (regola del fondatore 2026-11-04): ultimo passo del job, sempre, con credenziali fresche,
# DOPO pulizia.sh. Non cancella niente: guarda soltanto, in eu-west-1, se è rimasto qualcosa che si paga.
#   - macchine EC2 con il tag project=fenvix-coldread in qualsiasi stato tranne terminated;
#   - dischi EBS con quel tag (qualsiasi stato), oppure non attaccati (available) e creati dopo M31_INIZIO;
#   - indirizzi Elastic IP con quel tag;
#   - bucket che cominciano con fenvix-coldread-.
# Niente: "RIMASTO: niente", codice 0. Qualcosa (o una lettura che non riesce): "ALLARME: rimasto ..." con l'elenco,
# un'annotazione ::error:: (compare in cima alla pagina della run), il riassunto del passo comincia con l'ALLARME,
# codice 1 (job rosso).
# M31_INIZIO: ora d'inizio della giornata in UTC (AAAA-MM-GGTHH:MM:SSZ), messa dal primo passo del workflow.
# Se manca: le ultime 24 ore.
set -u
export INIZIO=${M31_INIZIO:-$(date -u -d '24 hours ago' +%FT%TZ)}   # jq lo legge come $ENV.INIZIO
TAGF="Name=tag:project,Values=fenvix-coldread"
rimasto=()
non_so=()
ERR=$(mktemp); trap 'rm -f "$ERR"' EXIT
breve() { tr '\n' ' ' < "$ERR" | sed 's/  */ /g; s/^ //' | head -c 200; }  # l'errore di AWS su una riga
# guarda COSA COMANDO... : esegue il comando AWS (JSON su stdout, errori a parte) e lo passa a jq con il filtro in $FILTRO;
# ogni riga stampata da jq è una cosa rimasta. Se AWS o jq falliscono (anche JSON rotto) è un NON SO, mai un "niente".
guarda() {
  local cosa=$1; shift; local j righe
  if ! j=$("$@" --output json 2>"$ERR"); then non_so+=("$cosa: $(breve)"); return; fi
  if ! righe=$(jq -r "$FILTRO" <<<"$j" 2>"$ERR"); then non_so+=("$cosa: risposta non leggibile ($(breve))"); return; fi
  while IFS= read -r r; do [ -n "$r" ] && rimasto+=("$r"); done <<<"$righe"
}

# 1. macchine: tutti gli stati tranne terminated
FILTRO='.Reservations[].Instances[] | "macchina \(.InstanceId) (\(.State.Name), \(.InstanceType))"'
guarda "macchine EC2" aws ec2 describe-instances --filters "$TAGF" Name=instance-state-name,Values=pending,running,shutting-down,stopping,stopped

# 2. dischi: con il tag (qualsiasi stato), oppure non attaccati e creati nella giornata (un disco senza tag che la
# cancellazione della macchina non ha tolto); il tempo si confronta senza i decimali dei secondi
FILTRO='.Volumes[] | "disco \(.VolumeId) (\(.State), \(.Size) GB, tag)"'
guarda "dischi EBS con il tag" aws ec2 describe-volumes --filters "$TAGF"
FILTRO='.Volumes[] | select(([.Tags[]? | select(.Key == "project" and .Value == "fenvix-coldread")] | length) == 0)
  | select((.CreateTime | sub("\\.[0-9]+"; "") | sub("\\+00:00$"; "Z")) >= $ENV.INIZIO)
  | "disco \(.VolumeId) (\(.State), \(.Size) GB, non attaccato, creato nella giornata)"'
guarda "dischi EBS non attaccati" aws ec2 describe-volumes --filters Name=status,Values=available

# 3. indirizzi Elastic IP con il tag
FILTRO='.Addresses[] | "indirizzo \(.PublicIp) (\(.AllocationId // "?"))"'
guarda "indirizzi Elastic IP" aws ec2 describe-addresses --filters "$TAGF"

# 4. bucket fenvix-coldread-*
FILTRO='.Buckets[].Name | select(startswith("fenvix-coldread-")) | "bucket \(.)"'
guarda "bucket S3" aws s3api list-buckets

S=${GITHUB_STEP_SUMMARY:-/dev/null}
if [ ${#rimasto[@]} -eq 0 ] && [ ${#non_so[@]} -eq 0 ]; then
  echo "RIMASTO: niente (controllo finale: macchine, dischi, indirizzi IP, bucket in eu-west-1, dal $INIZIO)" | tee -a "$S"
  exit 0
fi
elenco=$( { for r in "${rimasto[@]}"; do echo "$r"; done; for r in "${non_so[@]}"; do echo "NON SO, lettura non riuscita: $r"; done; } )
uno=$(paste -sd ';' <<<"$elenco" | sed 's/;/; /g')
echo "ALLARME: rimasto $uno"
echo "::error title=ALLARME controllo finale::rimasto $uno"
{ echo "# ALLARME: è rimasto qualcosa su AWS"; echo; sed 's/^/- /' <<<"$elenco"; echo
  echo 'Cosa fare: commit di {"modo": "smonta"} in comando.json (vedi PERMESSI.md), poi guardare di nuovo questo controllo.'; } >> "$S"
exit 1
