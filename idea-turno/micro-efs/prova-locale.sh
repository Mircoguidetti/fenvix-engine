#!/usr/bin/env bash
# Prova in locale di tutti i comandi di micro-efs. I numeri locali NON contano:
# serve solo a vedere che il programma funziona e che le verifiche dicono il vero.
# Uso: ./prova-locale.sh [DIR] [CONTROLLO_DIR]
#   DIR           cartella del "quaderno" (in locale un disco qualsiasi, o un mount NFS)
#   CONTROLLO_DIR la stessa cartella vista da un secondo mount senza cache (facoltativo)
set -euo pipefail
cd "$(dirname "$0")"
cargo build --release -q
B=./target/release/micro-efs
D=${1:-$(mktemp -d)}
C=${2:-$D}
R=$(date +%s)
echo "quaderno: $D   controllo: $C   run: $R"

echo "== 1. latenza, poche combinazioni (fsync e dsync; controllo no, si, pipeline)"
$B matrice --dir "$D" --controllo-dir "$C" --lotti-kib 1,4,256 --files 1,8 --modi fsync,dsync \
  --controlli no,si,pipeline --durata-s 0.5 --pausa-s 0 --fase loc --fasi "$D/fasi.jsonl" \
  --anche-solo-controllo si --controllo-modi open,stat,odirect > "$D/matrice.jsonl"
wc -l < "$D/matrice.jsonl"
$B scrivi --dir "$D" --controllo-dir "$C" --file 64 --lotto-kib 4 --durata-s 1 --fase loc-64f --fasi "$D/fasi.jsonl"
$B scrivi --dir "$D" --controllo-dir "$C" --file 4 --lotto-kib 64 --lotti 50 --prealloca-mib 8 --fase loc-prealloca
$B scrivi --dir "$D" --controllo-dir "$C" --file 2 --lotto-kib 4 --durata-s 1 --controllo si --controllo-modo odirect --fase loc-odirect

echo "== 1b. letture dalla coda mentre un altro scrive (con e senza O_DIRECT)"
for od in si no; do
  $B scrivi --dir "$D" --controllo-dir "$C" --file 1 --lotto-kib 4 --durata-s 4 --intervallo-ms 2 \
    --nome-libro coda$od$R --fase loc-coda-$od --fasi "$D/fasi.jsonl" > /dev/null &
  $B leggi-coda --libro "$D/coda/coda$od$R-0.log" --durata-s 3 --letture-s 500 --odirect $od \
    --fase loc-coda-$od --fasi "$D/fasi.jsonl"
  wait
done

echo "== 2. banco 9: gara, 3 'istanze' da 8 concorrenti, 1000 round"
INIZIO=$(( $(date +%s%3N) + 2000 ))
for id in a b c; do
  $B gara --dir "$D" --run "g$R" --id "$id" --concorrenti 8 --round 1000 --periodo-ms 5 --inizio-ms $INIZIO &
done
wait
$B gara-verifica --dir "$D" --run "g$R" --round 1000

for modo in uccidi ferma vivo; do
  echo "== 3. presa, modo $modo (20 cicli, fino a 100 lotti ciascuno)"
  $B presa-subentra --dir "$D" --controllo-dir "$C" --run "p$modo$R" --cicli 20 --id B &
  SUB=$!
  $B presa-titolare --dir "$D" --controllo-dir "$C" --run "p$modo$R" --cicli 20 --lotti 100 --modo $modo 2> "$D/titolare-$modo.log"
  wait $SUB
  $B presa-verifica --dir "$D" --run "p$modo$R" --cicli 20
done

echo "== 4. controllo negativo: il subentrante legge il libro PRIMA di creare l'epoca"
# Si simula copiando il libro di un ciclo a metà scrittura: la verifica deve trovare mancanti.
N="pneg$R"
$B presa-subentra --dir "$D" --controllo-dir "$C" --run "$N" --cicli 3 --id B &
SUB=$!
$B presa-titolare --dir "$D" --controllo-dir "$C" --run "$N" --cicli 3 --lotti 100 --modo vivo 2>/dev/null
wait $SUB
# togli gli ultimi 5 lotti letti: equivale a una presa che non vede la coda confermata
for i in 1 2 3; do f="$D/presa/$N/risultati/letti-$i.txt"; head -n -5 "$f" > "$f.x" && mv "$f.x" "$f"; done
if $B presa-verifica --dir "$D" --run "$N" --cicli 3; then echo "ERRORE: la verifica non ha visto i mancanti"; exit 1; else echo "ok: la verifica li vede"; fi

echo "== 5. metriche con una AWS CLI finta (nessuna chiamata ad AWS)"
FINTA=$(mktemp)
cat > "$FINTA" <<'EOF'
#!/usr/bin/env bash
# restituisce due punti: 1000 operazioni da 32 KiB, 1000 da 4 KiB
case "$*" in
  *TotalIOBytes*) printf '32768000.0\t1000.0\n4096000.0\t1000.0\n';;
  *MeteredIOBytes*) printf '8192000.0\t2000.0\n';;
  *) printf '4096000.0\t1000.0\n';;
esac
EOF
chmod +x "$FINTA"
$B metriche --fs-id fs-finto --fasi "$D/fasi.jsonl" --aws "$FINTA" > "$D/metriche.jsonl"
head -c 600 "$D/metriche.jsonl"; echo
rm -f "$FINTA"
echo "FATTO"
