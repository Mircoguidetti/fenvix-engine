# compat/misura: compatibilità con MongoDB misurata (sessione S10, 2026-11-01)

Questa cartella misura quanto il motore di `main` (commit `658c724`) si comporta come MongoDB 8.0. Non corregge il
motore. Il riassunto pubblicabile è in `docs/compatibilita.md`; i risultati grezzi sono in `results/` e
riassunti in `RISULTATI.md`.

Nota: `compat/` esisteva già (audit del 2026-10-27 contro MongoDB 7.0, con test scritti da noi). Quel lavoro non è
stato toccato: questa misura sta tutta in `compat/misura/`.

## Il metodo, e perché

1. **Test ufficiali di MongoDB, non test scritti da noi.** I test in formato "unified" del repository
   `github.com/mongodb/specifications` sono quelli che ogni driver ufficiale deve passare contro un vero mongod.
   Li scrive MongoDB, non noi: un numero misurato con questi test non è un numero che ci siamo dati da soli.
   Usiamo la copia dei file JSON che PyMongo 4.19.0 porta con sé (`test/` del repository
   `mongo-python-driver`, tag `4.19.0`), così ciò che i test si aspettano dal driver coincide con il driver usato.
2. **Un runner scritto qui** (`unified_runner.py`, PyMongo 4.19.0). Il runner ufficiale di PyMongo fa parte della sua
   suite di test e richiede di installare e far girare il loro codice di test; scriverne uno nostro, piccolo e
   leggibile, rende la misura ripetibile e controllabile da chiunque. Il runner legge i file JSON (dati, non codice)
   e segue le regole del formato (documenti "root" che possono avere campi in più, `$$unsetOrMatches`,
   `$$sessionLsid`, eventi di comando, `outcome` confrontato sui dati veri).
3. **Ogni test finisce in PASS, FAIL o SKIP, e ogni SKIP ha un motivo contato.** I motivi sono di tre tipi:
   requisiti del test che il nostro ambiente non ha (sharding, auth, versioni vecchie, secondari), il
   `configureFailPoint` (un aggancio di prova del server per simulare guasti, che il motore non ha), e operazioni che
   il nostro runner non implementa (limite della misura, elencato).
4. **Versione di riferimento 8.0.** I requisiti `minServerVersion`/`maxServerVersion` dei test sono valutati come se
   il server fosse MongoDB 8.0: i test delle funzioni 8.0 girano, quelli per versioni vecchie no. Il motore però si
   presenta come 7.0 (`maxWireVersion` 21): quando un driver rifiuta una funzione 8.0 per questo motivo, il test
   conta come fallito ("mancante").
5. **Ogni famiglia di fallimenti è stata riprodotta a mano** con una richiesta diretta (vedi `probe_features.py`,
   `repro_txn_lost_write.py`), per non confondere un errore del runner con un errore del motore.
6. **L'idea di FerretDB, ripresa:** FerretDB pubblica la sua compatibilità facendo girare suite di test scritte da
   altri (il suo progetto "dance": test di driver e applicazioni vere) e dichiarando per ognuna cosa passa. Facciamo lo
   stesso con i test ufficiali delle specifiche, i programmi di prova dei driver, mongosh e i tool di dump/restore.

## Cosa c'è

- `unified_runner.py`: il runner. `run_area.sh`, `run_all.sh`: lanciano le aree. `summary.py`: conteggi veloci.
- `classifica.py`: mette ogni fallimento in una categoria (bug dei dati, bug di risposta, mancante, diverso) con
  regole scritte e motivate; produce `results/riepilogo.json` e `RISULTATI.md`.
- `probe_features.py`: 108 comandi e operatori tra i più usati, ognuno con il risultato documentato nel manuale 8.0
  scritto accanto. Uscita in `results/funzioni.jsonl`.
- `drivers/`: un piccolo programma per driver (Node.js, Python, Go), `mongosh_prova.js`, `prova_auth.py`. Uscita in
  `results/driver.jsonl`.
- `porta_uscita.py`: riempie un database, mongodump (cartella e archivio gzip), mongorestore, confronta tutto.
  Uscita in `results/porta_uscita.json`.
- `repro_txn_lost_write.py`: riproduce il bug più grave trovato.

## Come rifarla

```
cargo build --release -p mongo-server
./target/release/mongo-server --listen 127.0.0.1:27018 --store local --local-dir /tmp/s10 \
    --replica-set rs0 --advertise localhost:27018 &
pip install pymongo==4.19.0
git clone --depth 1 --branch 4.19.0 https://github.com/mongodb/mongo-python-driver.git /tmp/pydrv
TESTS=/tmp/pydrv/test PY=python3 compat/misura/run_all.sh
python3 compat/misura/classifica.py
python3 compat/misura/probe_features.py --uri 'mongodb://localhost:27018/?replicaSet=rs0' --out compat/misura/results/funzioni.jsonl
```

Per la porta d'uscita servono `mongodump` e `mongorestore` 100.13.0 (qui compilati dal sorgente ufficiale
`github.com/mongodb/mongo-tools`, che contiene già le sue dipendenze: `go build -mod=vendor ./mongodump/main`) e un
secondo motore senza replica set:

```
python3 compat/misura/porta_uscita.py --uri mongodb://localhost:27019/ --bin <cartella dei tool> --work /tmp/uscita
```

## Limiti di questa misura

- **Nessun mongod 8.0 di confronto.** In questo ambiente Docker Hub e i download di mongodb.com sono bloccati, e
  l'esecuzione di un binario mongod scaricato da conda-forge è stata rifiutata dai controlli di sicurezza della
  sessione. Quindi: il runner non è stato tarato contro un mongod vero (lo è stato a mano, riproducendo ogni famiglia
  di fallimenti), e la porta d'uscita è provata solo da noi a noi.
- **Java non provato**: Maven Central non è raggiungibile da qui.
- **Go**: provato il driver 2.4.0, non l'ultimo (2.9.2), perché l'ultimo richiede Go 1.25 e qui c'è Go 1.24.
- 571 test sono saltati per `configureFailPoint`: tutta la parte "retry dopo un guasto" (retryable writes e reads,
  etichette di errore delle transazioni) non è misurata.
