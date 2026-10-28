
## 5. Binario

* **Commit:** `@COMMIT@` (branch `final-engine`).
* **Compilazione:** `@BUILD@`
* **SHA256 del binario `target/release/mongo-server`:** `@SHA@`
* **Flag di avvio** (`mongo-server --help` li elenca tutti). Quelli del servizio:

| flag | uso |
|---|---|
| `--listen ADDR:PORTA` | porta dati MongoDB |
| `--store s3\|local\|memory`, `--bucket B`, `--region R`, `--endpoint URL` | archivio. `s3`: credenziali **solo** dall'ambiente AWS (variabili o ruolo dell'istanza); regione obbligatoria; endpoint solo per servizi compatibili (MinIO). `local` (con `--local-dir`): un solo processo |
| `--auth` | richiede autenticazione e autorizzazione (SCRAM-SHA-1/256, ruoli di mongod 7.0.45) |
| `--tls-mode disabled\|allowTLS\|preferTLS\|requireTLS`, `--tls-certificate-key-file`, `--tls-ca-file`, `--tls-allow-connections-without-certificates` | TLS della porta dati; `rotateCertificates` ricarica i file |
| `--admin-listen ADDR:PORTA` | porta admin di ctl (HTTP, token bearer); token in `MONGO_ADMIN_TOKEN` o `--admin-token-file` |
| `--retention-window-secs`, `--retention-interval-secs`, `--retention-prune-secs` | finestra di ripristino (restore a un istante) |
| `--holder`, `--lease-ttl-secs`, `--idle-secs` | identità del processo, durata dei lease, chiusura dei database inattivi |
| `--wal-gc-secs`, `--wal-gc-mode bulk\|slatedb`, `--sst-gc-secs`, `--group-commit-*` | pulizia WAL/SST e group commit del motore |

Riga di avvio completa (dati con TLS e autenticazione, admin su rete privata, S3 vero con credenziali dall'ambiente o dal ruolo dell'istanza):

```
export MONGO_ADMIN_TOKEN="$(cat /etc/mongo/admin.token)"            # almeno 16 caratteri
# credenziali S3: ruolo dell'istanza (IMDSv2), oppure AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (/ AWS_SESSION_TOKEN)
mongo-server --listen 0.0.0.0:27017 --store s3 --bucket MIO-BUCKET --region eu-central-1 \
  --auth --tls-mode requireTLS --tls-certificate-key-file /etc/mongo/server.pem --tls-ca-file /etc/mongo/ca.pem \
  --admin-listen 10.0.0.5:8081 --retention-window-secs 3600 --holder "$(hostname)" --idle-secs 60
```

La riga con MinIO per i test (stessa cosa, endpoint esplicito): `AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 mongo-server --listen 127.0.0.1:27017 --store s3 --bucket bench --endpoint http://127.0.0.1:9000 ...`.

## 6. Cosa NON è stato provato

* **S3 vero:** nessuna prova su AWS (non richiesto, nessun credito speso). In particolare: la catena di credenziali con il ruolo dell'istanza e il token d'identità web (c'è solo il percorso da variabili d'ambiente, provato con un endpoint finto che registra la firma); le scritture condizionali `If-Match`/`If-None-Match` su AWS (provate su MinIO e lette nella libreria); il versioning del bucket; gli errori 409/503 reali; la latenza.
* **Carico e latenza:** nessun load test e nessuna misura di costo WAL in questa fase (come richiesto). Il costo della pulizia WAL per i database con retention (ora pulizia di slatedb invece della incrementale) non è misurato.
* **Più istanze** sullo stesso bucket: nessuna prova con più processi contemporanei (lease tra processi, pin tra processi, erase con un altro processo proprietario) oltre ai test ctl già esistenti con due processi su MinIO; `--store local` non protegge due processi.
* **TLS:** CRL/OCSP, opzioni di versione del protocollo, SRV con più host (limiti già noti di compat-gaps); TLS sulla porta admin (non c'è).
* **Prestazioni** della regola di concessione dei ruoli con molti ruoli personalizzati (non misurata).
* **Altre cose:** nessuna prova di `userAdmin` che riprende il controllo di un utente più potente cambiandogli la password (vedi R1).

## 7. Rischi aperti e decisioni

### Decisioni mie (da confermare o cambiare)
* **D1 — regola della correzione 1.** "Non più potenti dei propri" l'ho tradotta così: un ruolo si può concedere se ogni suo privilegio è su un database con nome su cui chi concede ha `grantRole`, oppure è già posseduto per intero (cluster, tutti i database, qualunque risorsa). Un `userAdmin` di `app1` può quindi dare `readWrite` o `dbOwner` su `app1` pur non avendo quei privilegi (come in mongod); la regola più stretta "solo ruoli di cui hai tutti i privilegi" renderebbe `userAdmin` inutile per creare utenti applicativi. Se vuoi la regola stretta è una riga in `may_grant`.
* **D2 — `rotateCertificates` con TLS disattivato** risponde ok, perché mongod risponde ok e la suite differenziale lo confronta; con TLS attivo ricarica davvero o dà errore.
* **D3 — `create`/`fork` di ctl rifiutano con 409 un nome che ha ancora utenti o ruoli** (invece di cancellarli o adottarli). Dopo un erase non ce ne sono.
* **D4 — l'erase elimina utenti e ruoli** definiti su quel database; gli utenti di altri database che avevano ruoli su di esso li conservano, ma quei ruoli non esistono più.
* **D5 — un restore non tocca gli utenti** (non sono nei dati del database).
* **D6 — regione obbligatoria** e nessun valore predefinito per l'endpoint: è un cambio di comportamento per chi avviava il motore senza flag (partiva su `localhost:9000` con `minioadmin`).
* **D7 — `s3guard` e `--store s3-aws` di `s3-test-draft` non riportati.**

### Rischi aperti
* **R1** Chi può cambiare la password o eliminare un utente di un database (`changePassword`, `dropUser`) può prendere il posto di un utente più potente definito su quel database: come in mongod, non coperto dalla correzione 1.
* **R2** Porta admin in HTTP semplice, token statico, nessun limite di tentativi: va su rete privata o dietro un terminatore TLS. `--admin-token` espone il token nella riga di comando.
* **R3** L'erase prende il blocco del database e poi quello del database `admin` (per cancellare gli utenti): un'operazione tra due database che tenga `admin` e voglia il database in erase potrebbe, in teoria, bloccarsi. Non osservato, non provato.
* **R4** Pulizia WAL dei database con retention in modalità `bulk`: ora usa la pulizia di slatedb (più richieste per passata). Da misurare su S3 vero.
* **R5** `LocalCas`: un solo processo.
* **R6** Il difetto S4 (spegnimento) era nel ramo fuso già verificato: le verifiche di quel ramo non lo vedevano. Ne possono esistere altri della stessa famiglia (agenti in background che riaprono database durante la chiusura): ho messo il cancello su pin, punti di ripristino, scadenza degli export e contatori d'uso; altri agenti del motore (TTL, reaper) non sono sotto il cancello e non ho cercato altro.
* **R7** Regola delle credenziali nei test: i lanciatori sostituiscono sempre `AWS_*`; chi avvia un test su S3 vero deve avviare il motore a mano.

### Test e strumenti toccati
Nessun test esistente è stato modificato per farlo passare. Modifiche ad altri file di test e strumenti: `ctl-tests/lib.py` (modalità `CTL_TEST_AUTH`, impostazioni MinIO esplicite), blocco con le variabili MinIO nei lanciatori di `loadtest/*.py`, `mongo-tests/*.py`, `ctl-tests/probe_existing.py`, `mongo-tests/server.sh` (sorgente `tools/minio-env.sh`), `mongo-tests/run_py.py` e `run_node.js` (`--slice` e `--shard`). Nuovi: `ctl-tests/test_09*.py`, `test_10`, `test_11`, `test_12`, `tools/final/*`.

## 8. Branch e commit finale

Branch `final-engine`, commit `@COMMIT@`. Pushato solo `origin/final-engine`. `main`, `compat-gaps`, `fusion-ops` e gli altri branch invariati; nessuna PR; nessun merge in `main`. Nessun load test, nessuna misura di costo WAL.
