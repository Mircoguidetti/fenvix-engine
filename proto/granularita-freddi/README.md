# proto/granularita-freddi (M21)

Quante GET su S3 costa una find fredda vera, con la libreria di storage del motore (SlateDB 0.17.0) e la sua disposizione
delle chiavi. Il motore non è toccato. Soglie e metodo: `SOGLIE.md` (scritte prima di misurare). Rapporto:
`fenvix-brain/sessions/2026-11-03-granularita-freddi/RAPPORTO.md`.

## Come si rifà

```
cargo build --release
scripts/matrice.sh            # carica e misura i casi (circa 11 minuti ciascuno su 2 vCPU), scrive risultati/misure.jsonl
scripts/consolida.sh          # casi con il tetto ai sorted run
python3 scripts/analisi.py    # riassunto e soldi sul profilo B (risultati/riassunto.json)
```

Controprova con il motore vero (`mongo-server` di main, store locale, bancone dei blocchi 1 MB):

```
mongo-server --store local --local-dir /tmp/ms --idle-secs 100000 --block-cache-mb 1 --meta-cache-mb 64 &
python3 scripts/controprova_motore.py carica 27018
python3 scripts/controprova_motore.py misura 27018
granularita-freddi stato --dir /tmp/ms --percorso tenants/chat
```

## Pezzi

- `src/conta.rs`: finto S3 che conta (ogni `get_opts` è una richiesta S3; HEAD e GET a intervallo compresi) e può aggiungere un ritardo fisso per GET.
- `src/bancone.rs`: cache che separa blocchi di dati da indici dei blocchi, filtri e statistiche; unisce le letture uguali in volo come foyer.
- `src/main.rs`: `carica` (storico chat o eventi per dispositivo, molti giorni, lavaggio size-tiered acceso), `misura` (find fredde con il piano del motore), `stato`, `pulisci`.
- Disposizioni: `base` (come il motore: `doc/{coll}/{_id}` + voci d'indice `idx/...`), `gruppo` (rimedio a: documento sotto `cdoc/{coll}/{utente}{data}{_id}` più puntatore per `_id`). Opzioni: `--blocco 64` (rimedio b), `--prefisso 1` (rimedio c, filtri per prefisso sul proprietario), `--consolida N` (tetto ai sorted run).
