# shellcheck shell=bash
# P1 Irlanda (v2) · lo user-data della macchina (lo usa giro-ci.sh e la prova locale). Come M31 (revisore M31, C1):
# lo spegnimento a TETTO_MACCHINA_MIN minuti e' la PRIMA riga, prima di scaricare qualsiasi cosa; se lo scaricamento
# o il tar falliscono la macchina si spegne subito (spegnimento = cancellazione, impostato al lancio).
# scrivi_user_data URL BUCKET  ->  stampa lo script che la macchina esegue all'avvio.
scrivi_user_data() {
  local url=$1 bucket=$2
  cat <<UDEOF
#!/bin/bash
shutdown -h +${TETTO_MACCHINA_MIN:-100} "irlanda: tetto di tempo" || true
D=\${IRL_DIR:-/opt/irlanda}
mkdir -p "\$D" && cd "\$D" || { shutdown -h now "irlanda: niente cartella"; exit 1; }
ok=0
for i in 1 2 3 4 5; do curl -fsS -o g.tgz "$url" && { ok=1; break; }; sleep \${IRL_ATTESA_SCARICO:-10}; done
[ "\$ok" = 1 ] || { echo "scaricamento fallito" > errore-avvio.txt; shutdown -h now "irlanda: scaricamento fallito"; exit 1; }
tar xzf g.tgz && chmod +x esegui.sh || { echo "tar fallito" > errore-avvio.txt; shutdown -h now "irlanda: tar fallito"; exit 1; }
nohup ./esegui.sh "$bucket" > esegui.log 2>&1 &
UDEOF
}
