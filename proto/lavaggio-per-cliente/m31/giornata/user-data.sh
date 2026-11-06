# M31 · lo user-data della macchina (lo usa giornata-ci.sh; lo usa anche la prova locale prova-guasti.sh).
# scrivi_user_data URL BUCKET  ->  stampa lo script che la macchina esegue all'avvio.
# Revisore M31, C1: lo spegnimento a 240 minuti è la PRIMA riga (prima di scaricare qualsiasi cosa), e se lo
# scaricamento o il tar falliscono la macchina si spegne subito (spegnimento = cancellazione, impostato al lancio).
# La cartella è /opt/m31; la prova locale la cambia con M31_DIR (valutata sulla macchina, non qui).
scrivi_user_data() {
  local url=$1 bucket=$2
  cat <<UDEOF
#!/bin/bash
shutdown -h +240 "M31: tetto" || true
D=\${M31_DIR:-/opt/m31}
mkdir -p "\$D" && cd "\$D" || { shutdown -h now "M31: niente cartella"; exit 1; }
ok=0
for i in 1 2 3 4 5; do curl -fsS -o g.tgz "$url" && { ok=1; break; }; sleep \${M31_ATTESA_SCARICO:-10}; done
[ "\$ok" = 1 ] || { echo "scaricamento fallito" > errore-avvio.txt; shutdown -h now "M31: scaricamento fallito"; exit 1; }
tar xzf g.tgz && chmod +x m31 esegui.sh || { echo "tar fallito" > errore-avvio.txt; shutdown -h now "M31: tar fallito"; exit 1; }
nohup ./esegui.sh "$bucket" > esegui.log 2>&1 &
UDEOF
}
