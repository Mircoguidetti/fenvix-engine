# shellcheck shell=bash
# Cucina minima (K2) · gli user-data delle due macchine (li usa giro-ci.sh e la prova locale). Come M31 e l'Irlanda:
# lo spegnimento al tetto di tempo e' la PRIMA riga, prima di scaricare qualsiasi cosa; se lo scaricamento o il tar
# falliscono la macchina si spegne subito (spegnimento = cancellazione, impostato al lancio).
#   scrivi_user_data URL BUCKET RUOLO [K]   ->  stampa lo script che la macchina esegue all'avvio.
#     RUOLO = cliente (lancia esegui-cliente.sh BUCKET) oppure server (lancia esegui-server.sh BUCKET K).
#     TETTO_MACCHINA_MIN: minuti dopo i quali la macchina si spegne da sola (giro-ci.sh lo calcola: il tempo che resta
#     al giro, cosi' nessuna macchina sopravvive al job).
scrivi_user_data() {
  local url=$1 bucket=$2 ruolo=$3 k=${4:-}
  local prog=esegui-$ruolo.sh
  cat <<UDEOF
#!/bin/bash
shutdown -h +${TETTO_MACCHINA_MIN:-100} "cucina: tetto di tempo" || true
D=\${CM_DIR:-/opt/cucina}
mkdir -p "\$D" && cd "\$D" || { shutdown -h now "cucina: niente cartella"; exit 1; }
ok=0
for i in 1 2 3 4 5; do curl -fsS -o g.tgz "$url" && { ok=1; break; }; sleep \${CM_ATTESA_SCARICO:-10}; done
[ "\$ok" = 1 ] || { echo "scaricamento fallito" > errore-avvio.txt; shutdown -h now "cucina: scaricamento fallito"; exit 1; }
tar xzf g.tgz && chmod +x $prog || { echo "tar fallito" > errore-avvio.txt; shutdown -h now "cucina: tar fallito"; exit 1; }
nohup ./$prog "$bucket" $k > $ruolo.log 2>&1 &
UDEOF
}
