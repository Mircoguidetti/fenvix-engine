#!/bin/bash
# Prova locale P1 Irlanda v2 (0 $): quante connessioni apre lo script NON modificato nella fase parallelo, al
# livello 10 e 50, con il nostro mongo-server al posto di Atlas dietro un proxy che aggiunge il ritardo della rete.
# Tre casi: (1) "Mac": viaggio 34 ms, stringa di connessione normale; (2) "Irlanda": viaggio 1 ms, stringa normale;
# (3) "Irlanda + maxConnecting": viaggio 1 ms, stringa con maxConnecting=200 (quello che fa esegui.sh). Per ogni caso:
# la riga dello script "connessioni aperte e scaldate N (servono N)" e controlla_parallelo.py (ogni lettura di ogni
# lotto su una connessione gia' pronta, una per cliente).
# Uso: prova-parallelo.sh CARTELLA_DI_LAVORO MONGO_SERVER_BINARIO
set -u
W=$1; SRV=$2
QUI=$(cd "$(dirname "$0")" && pwd); I=$(cd "$QUI/.." && pwd)
DBP=${DBP:-27744}; PX=${PX:-27745}
rm -rf "$W"; mkdir -p "$W"; cd "$W" || exit 1
export NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
nohup "$SRV" --listen 127.0.0.1:$DBP --store local --local-dir "$W/db" --replica-set rs0 --advertise 127.0.0.1:$DBP > db.log 2>&1 &
DB=$!; sleep 2
mkdir -p carico && cp "$I/prova_infinite.py" carico/
( cd carico && FENVIX_P1_URI="mongodb://127.0.0.1:$DBP/?replicaSet=rs0" python3 prova_infinite.py carica --gb 0.02 \
    --mb-cliente 0.03 --thread 2 --lotto 200 --file risultati_p1.json > carica.log 2>&1 ); echo "carica: codice $?"
caso() { # caso NOME RITARDO_MS APERTURA_MS URI_EXTRA
  local nome=$1 rit=$2 ap=$3 extra=$4
  python3 "$QUI/proxy_ritardo.py" $PX 127.0.0.1:$DBP "$rit" "$ap" & local pp=$!; sleep 1
  mkdir -p "$nome" && cp carico/risultati_p1.json "$nome/" && : > "$nome/risultati_p1.json.usati.log" && cp "$I/prova_infinite.py" "$nome/"
  ( cd "$nome" && FENVIX_P1_URI="mongodb://127.0.0.1:$PX/?directConnection=true$extra" python3 prova_infinite.py giro \
      --file risultati_p1.json --cartella-dati dati --giro "par_$nome" --nuova-cartella --solo-fasi parallelo \
      --livelli 10,50 --par-lotti 1,1 --pausa-s 3 --par-secondi 6 > terminale.txt 2>&1 )
  kill $pp 2>/dev/null; wait $pp 2>/dev/null
  echo "== $nome (viaggio $(python3 -c "print(2*$rit)") ms, apertura +$ap ms, stringa: ...directConnection=true$extra)"
  grep -E "connessioni aperte e scaldate|PARALLELO .* FINE" "$nome/terminale.txt"
  python3 "$I/controlla_parallelo.py" "$(ls -d "$nome"/dati/giro_finale_* | tail -1)"; echo "controlla_parallelo: codice $?"
}
caso mac 17 100 ""
caso irlanda 0.5 3 ""
caso irlanda_maxconnecting 0.5 3 "&maxConnecting=200"
caso mac_maxconnecting 17 100 "&maxConnecting=200"
kill $DB 2>/dev/null
