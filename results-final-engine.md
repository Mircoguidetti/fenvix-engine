# results-final-engine — il motore finale (branch `final-engine`)

Base: `origin/fusion-ops` (e98ed24). Fuso: `origin/compat-gaps` (5df2720). Nessun merge in `main`, nessuna PR; `main` (618c3bd), `compat-gaps` (5df2720), `fusion-ops` e gli altri branch non sono stati toccati; si fa push solo di `final-engine`.
Ambiente: quello delle sessioni precedenti (mongod 7.0.45 di riferimento estratto dall'immagine ufficiale `mongo:7.0`, MinIO, pymongo 4.x, Node). Non si è usato AWS e non si è speso credito. Ciò che non è stato eseguito è scritto "non provato".
Il binario verificato è quello della sezione 5 (compilato dal commit `41ed169`; SHA256 `d39e4038be17d2a1e69706c06bd30173209858f94eda10c2e58528d61e925b9b`); ogni fase della sezione 4 è stata eseguita su di esso. I commit successivi a `41ed169` toccano solo test, strumenti e risultati: `git diff 41ed169..HEAD -- mongo-server src Cargo.toml Cargo.lock` è vuoto.
**Una prima serie completa di verifiche (binario `54f15b2c…`) è stata scartata**: ha fatto scoprire il difetto 8 (sezione 2.8), il motore è stato corretto e **tutte** le fasi sono state rifatte da zero sul binario finale.

## 1. Cosa è stato fuso e conflitti

`compat-gaps` porta 214 file (circa 132.000 righe): listener TLS (disabled/allowTLS/preferTLS/requireTLS, certificati client, X.509), autenticazione SCRAM-SHA-1/256, utenti e ruoli (in `admin.system.users`/`admin.system.roles`), applicazione dei ruoli con `--auth`, comando `shutdown`, `rotateCertificates`, diagnostica, viste, change stream, ecc. Commit di fusione `b8b8132`.

### Conflitti di testo

| file | conflitto | scelta |
|---|---|---|
| `mongo-server/src/main.rs` (3 punti) | `mod ctl;` contro `mod cs_events;` e gli altri moduli | tenuti tutti |
| | argomenti `ctl` (porta admin, retention, ...) contro `--auth`, `--tls-*` | tenuti tutti (stesso `Args`) |
| | per ogni richiesta: sonda d'uso di ctl (`usage::before/after`) contro `diag.note_seq` e gestione del comando `shutdown` | tenute tutte e tre, nell'ordine sonda → note_seq → handle → sonda → controllo `$shutdown` |
| `mongo-server/src/dbs.rs` (3 punti) | campi di `Entry`: `inflight`, `last_compaction_request` (engine-cost) contro `nat`, `copts` (compat-gaps) | tenuti tutti |
| `mongo-server/src/cmd.rs` (`handle`) | engine-cost avvolge il comando negli ambiti del group commit (`TOKENS`, `PENDING`) e attende l'ultimo commit; compat-gaps esegue il comando con diagnostica, `OP_TIME`, kill e, per JavaScript, su un thread a parte | vedi il conflitto semantico S1 |
| `mongo-tests/server.sh`, `run_py.py`, `run_node.js` | `SERVER_BIN` contro `MONGO_SERVER_BIN`; `--slice` contro `--shard` | tenuti entrambi |
| `mongo-server/Cargo.toml`, `Cargo.lock` | dipendenza `sha2` doppia; lock rigenerato da cargo | una riga `sha2`; lock di fusion-ops, completato da cargo |
| `compat/*` (3 file, add/add) | inventario | versione di compat-gaps (è il lavoro più recente, non fa parte del motore) |

### Conflitti semantici (senza conflitto di testo o trovati leggendo il codice)

| | problema | scelta e prova |
|---|---|---|
| S1 | I comandi che eseguono JavaScript girano su un thread a parte (`run_js`); gli ambiti del group commit sono variabili locali del task (`task_local`) e su quel thread non esistono. Avvolti all'esterno, un comando JS avrebbe risposto prima che i suoi commit fossero durevoli. | Gli ambiti si aprono in `dispatch_durable`, dentro `run_command`, nel task che esegue davvero il comando, e la risposta attende l'ultimo commit. Coperto dalle suite JavaScript (js.*, $where, mapReduce) e dalle prove SIGKILL sul binario finale; non ho un test negativo che mostri la differenza con l'avvolgimento esterno (è una conclusione dalla lettura del codice). |
| S2 | Porta admin (token) e SCRAM della porta dati. | Sono indipendenti: la porta admin è un listener HTTP a parte; nessun comando ctl esiste sulla porta dati (anche root riceve rifiuto), e sulla porta admin SCRAM/credenziali non valgono come token. Provato da `test_09_security.py::test_admin_port_is_independent_of_scram` e `test_nothing_of_ctl_is_reachable_from_the_data_port` (token sbagliato, assente, di lunghezza diversa, credenziali SCRAM usate come token, schema `Basic`: sempre 401). Il confronto del token è a tempo costante. |
| S3 | Gli utenti vivono nel tenant `admin` (`system.users`, campo `db`), non nel database a cui appartengono: erase, fork e restore di ctl non li toccavano. | Correzione 4 (sotto). |
| S4 | Avvio e spegnimento. Il comando `shutdown` di compat-gaps e `ctl::shutdown` di control-ops devono dare la stessa uscita pulita. **Difetto trovato:** durante lo spegnimento l'agente dei pin di ctl (e gli altri agenti) continuava a girare mentre `close_all` chiudeva i database: riapriva il database dopo la chiusura, riprendeva il lease del tenant e rinnovava il pin, così un'uscita pulita lasciava pin e lease in mano al processo morto fino alla scadenza del ttl. Per lettura del codice il difetto era già nel ramo fuso (stesso ordine `close_all` → `ctl::shutdown` in `main.rs`; non l'ho rieseguito sul binario di fusion-ops); il mio test di allora controllava solo che il successore prendesse il pin, cosa che avviene comunque alla scadenza. Provato invece sul binario di questo ramo prima della correzione (sotto). | Correzione: un cancello (`ctl::stop_agents`) ferma gli agenti e attende l'iterazione in corso prima di `close_all`; poi `ctl::shutdown` consegna i pin e scrive l'uso, poi si rilasciano i lease. Test nuovo `test_12_shutdown.py` (comando `shutdown` e SIGTERM): prima 2 falliti (pin ancora del processo), dopo 2 passati. |
| S5 | Campi `nat`/`copts` di `Entry` (ordine naturale, collation), azzerati a ogni apertura/chiusura del tenant; restore e fork di ctl sostituiscono i dati del tenant. | Letto: restore e fork chiudono l'entry con `close()`, che azzera i campi. Nessun test specifico (non provato oltre la lettura). |
| S6 | Archivio di oggetti: `ctl::usage::wrap` e `CountingStore` avvolgono lo store; compat-gaps usa `--store local`. | `LocalCas` (correzione 6) sta sotto i due involucri; provato dai test ctl sullo store locale. |

## 2. Correzioni di sicurezza e di allineamento (1–7)

Ordine di lavoro per ognuna: prima il test che mostra il problema (eseguito sul binario senza correzioni, che ho tenuto: `/tmp/claude-0/mongo-server.prefix`, uscita in `results-final-engine/before_*.txt`), poi la correzione, poi lo stesso test che passa.

### 2.1 createUser / updateUser non controllano i ruoli concessi
* **Test prima** (`ctl-tests/test_09_security.py`, 22 falliti su 30 nel file, tra cui tutti i casi ostili): un utente con solo `userAdmin@app1` crea utenti con `root@admin`, ruoli su `app2`, su `admin`, `clusterAdmin`, `readWriteAnyDatabase`, un ruolo personalizzato che eredita `root`; un utente con la sola azione `createUser` crea utenti con ruoli; `userAdminAnyDatabase` crea un `root`; `updateUser` e `grantRolesToUser` su altri e su se stesso; `createRole`/`updateRole`/`grantRolesToRole`/`grantPrivilegesToRole` per far passare un privilegio di nascosto. Tutti riuscivano.
* **Correzione** (`mongo-server/src/auth.rs`, `may_grant`, chiamata da `authorize` dopo il controllo dei privilegi): per ogni privilegio del ruolo concesso (con tutto ciò che eredita) o del comando: se la risorsa è **un database con nome**, chi concede deve avere l'azione `grantRole` su quel database (regola di mongod); se è **il cluster, tutti i database, qualunque risorsa o i bucket time-series**, chi concede deve possedere già quei privilegi (tutte le azioni). Così `userAdmin@app1` dà ruoli su `app1` e nient'altro; `userAdminAnyDatabase` non dà `root`; un ruolo personalizzato che eredita `root` non è una scorciatoia.
* **Test dopo:** `30 passati, 0 falliti` (file `test_09_security.py`), compresi i casi che devono continuare a funzionare (root che crea di tutto, `userAdmin` che dà `readWrite`/`dbOwner` su `app1`, `userAdminAnyDatabase` che dà ruoli su un database con nome, la sola azione `createUser` che crea utenti senza ruoli).
* **Esito:** risolto. Interpretazione della regola "non più potenti dei propri" in sezione 7 (D1).

### 2.2 Gli errori Unauthorized ripetono il comando intero, password compresa
* **Test prima:** `test_unauthorized_errors_of_a_weak_user_carry_no_secret` e `test_secrets_are_not_in_the_profile_or_in_currentop`: l'errore di un utente autenticato ma senza privilegio riportava `createUser: "...", pwd: "<password>"`; il profilo (`system.profile`) e `currentOp` riportavano la password.
* **Correzione:** `redact_command` (`auth.rs`) sostituisce con `xxx` i valori di `pwd`, `password`, `payload`, `key`, `nonce`, `proof`, `clientProof` per `createUser`, `updateUser`, `authenticate`, `saslStart`, `saslContinue`, `changeUserPassword`; è usata nel messaggio Unauthorized, in `currentOp` (`diag.begin`) e nel profilo (`profile_record`). Il motore non scrive comandi nel log (solo nome e database con `--log-commands`).
* **Test dopo:** gli stessi più `test_secrets_are_not_in_the_server_log`, `test_failed_logins_do_not_log_or_return_the_password` (SCRAM-SHA-1 e -256), il test end-to-end e `tools/final/secret_scan.py` su tutti i log (sezione 4.5).
* **Esito:** risolto per i comandi elencati. Non verificato: ogni altro comando che porti segreti nei dati dell'utente (per esempio un documento con un campo `password` inserito con `insert`: è dato dell'utente, non un segreto del motore).

### 2.3 Credenziali e regione S3 fisse nel codice
* **Test prima** (`test_09d_s3_env.py`, 4 falliti su 6): con chiavi sbagliate nell'ambiente il motore scriveva lo stesso (usava `minioadmin`); senza regione partiva; `--region` non esisteva; la regione e la chiave firmate non erano quelle date.
* **Correzione** (`mongo-server/src/main.rs`): `--store s3` usa `AmazonS3Builder::from_env()`: la catena standard di `object_store` (variabili `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`, token d'identità web `AWS_WEB_IDENTITY_TOKEN_FILE`+`AWS_ROLE_ARN`, credenziali del container, e senza nulla di tutto ciò il ruolo dell'istanza via IMDSv2). Regione e endpoint da `--region`/`--endpoint` o da `AWS_REGION`/`AWS_DEFAULT_REGION`/`AWS_ENDPOINT`; l'endpoint vuoto significa AWS; un endpoint `http://` abilita HTTP semplice; **senza regione il motore non parte** e lo dice. Le credenziali non sono mai sulla riga di comando né nel log. Riuso di `s3-test-draft`: stesso principio della sua patch `0001-s3-aws-instance-profile.patch` (nessuna chiave nel costruttore, il ruolo dell'istanza fa il resto). **Non riportato:** `s3guard` (limitatore di richieste e tetto di spesa, strumenti di costo di quei test) e l'alias `--store s3-aws`.
* **Test MinIO:** tutti gli script che avviano il motore contro MinIO impostano esplicitamente le variabili e **sostituiscono** quelle già presenti (`tools/minio-env.sh` per le shell, un blocco esplicito nei 26 file Python di `loadtest/`, `mongo-tests/`, `ctl-tests/`): serve a non parlare mai a MinIO con credenziali vere e a non mandare a S3 vero quelle di prova. Scoperto strada facendo: questo sandbox ha già `AWS_ACCESS_KEY_ID=proxy-inject`; con la correzione il motore lo usa, come deve.
* **Test dopo:** `6 passati, 0 falliti` (file `test_09d_s3_env.py`); la regione e la chiave firmate sono provate con un finto endpoint S3 locale che registra l'intestazione di firma (`Credential=chiave/data/regione/s3/...`), senza costo; la sessione e il token di sessione sono nell'intestazione `x-amz-security-token`.
* **Esito:** risolto per `mongo-server`. `src/main.rs` del crate radice (lo strumento di esperimenti `tenant-bench`, non il motore) ha ancora `minioadmin` fisso: non toccato.

### 2.4 Erase, fork, restore e gli utenti
* **Test prima** (`test_09_security.py`, 3 falliti): dopo l'erase gli utenti e i ruoli del database restavano in `admin.system.users`/`system.roles`; nessun rifiuto se si ricreava un database con utenti residui.
* **Correzione** (`mongo-server/src/auth.rs`, `ctl/erase.rs`, `ctl/meta.rs`, `ctl/fork.rs`): l'erase elimina utenti **e ruoli** definiti su quel database (passo idempotente della ripresa dopo crash, dopo aver preso il lease e prima di cancellare gli oggetti) e le connessioni aperte di quegli utenti perdono i privilegi subito (il contatore di generazione cambia). Il **fork** non eredita gli utenti (gli utenti vivono nel tenant `admin`, il fork copia solo il tenant: provato). **create** e **fork** rifiutano con 409 `users_exist` un nome che ha ancora utenti o ruoli (non li adottano in silenzio). **Restore:** non riguarda gli utenti, che non sono nei dati del database: dopo un restore un utente creato dopo l'istante c'è ancora, uno eliminato dopo l'istante resta eliminato, una password cambiata resta cambiata (documentato e provato in `test_09b_users_restore.py`). I nomi `admin`, `local`, `config` restano vietati per ctl (provato).
* **Test dopo:** `30 passati, 0 falliti`, `1 passati, 0 falliti`, end-to-end `1 passati, 0 falliti`.
* **Esito:** risolto.

### 2.5 `rotateCertificates` risponde ok senza ricaricare
* **Test prima** (`test_09c_tls_rotate.py`, 2 falliti su 3): dopo aver sostituito il file del certificato e dato il comando, le nuove connessioni ricevevano ancora il vecchio; con file inutilizzabili rispondeva ok.
* **Correzione** (`mongo-server/src/tls.rs`, `admin_cmds.rs`): il comando rilegge certificato, chiave e CA dai file e ne sostituisce l'accettatore per le connessioni nuove (le aperte continuano); se i file non sono utilizzabili risponde `OperationFailed` (96) con il motivo e **il certificato precedente resta in uso**. Con TLS disattivato non c'è nulla da ruotare e risponde ok come mongod (la suite differenziale `g11.admin` lo confronta): è una decisione, D2.
* **Test dopo:** `3 passati, 0 falliti` (fingerprint SHA-256 del certificato mostrato prima e dopo; connessione aperta prima che continua a funzionare).
* **Esito:** risolto.

### 2.6 `--store local`: `PutMode::Update not yet implemented`
* **Indagine.** Riprodotto: create funziona (usa `PutMode::Create`), fork, keep-awake, erase, retention e il risveglio di un database addormentato (il lease si rinnova con `PutMode::Update`, `lease.rs`) fallivano con 500. **È un limite solo del file locale della libreria:** `object_store` 0.14.2, `src/local.rs:399-403`, restituisce `NotImplemented` per `PutMode::Update`. **Su S3** (`src/aws/mod.rs:255-300`) con `S3ConditionalPut::ETagMatch`, che è il valore predefinito (`src/aws/precondition.rs:117-128`), `PutMode::Create` invia `If-None-Match: *` e `PutMode::Update` invia `If-Match: <etag>` (con ritentativo sugli 409 che S3 può dare); `Disabled` darebbe lo stesso errore. Il motore usa già queste due operazioni per i lease (`lease.rs`), quindi non aggiunge dipendenze nuove. Su MinIO le stesse operazioni funzionano: sono i test ctl (fork, keep-awake, erase, retention, restore) della sezione 4.3, tutti verdi.
* **Test prima** (`test_09e_local_store.py`, 3 falliti su 4).
* **Correzione** (`mongo-server/src/localcas.rs`): un involucro del file locale che implementa `Update` confrontando l'etag sotto un mutex che copre ogni scrittura del processo. **Limite:** vale per un solo processo sulla directory (S3 lo fa lato server, per oggetto): lo store locale resta per sviluppo e test.
* **Test dopo:** `4 passati, 0 falliti`.
* **Da verificare su S3 vero (non provato qui):** che AWS accetti `If-Match` su `PutObject` con l'etag letto (scritture condizionali per ETag, annunciate da AWS a fine 2024) nel bucket e nella regione scelti, `If-None-Match: *`, il comportamento con il versioning attivo, e il 409 `ConditionalRequestConflict` sotto concorrenza (la libreria lo ritenta).

### 2.7 TLS sulla porta admin
Non serve ora (rete privata): **limite noto documentato.** La porta admin è HTTP semplice con token bearer (almeno 16 caratteri, confronto a tempo costante, nessun limite di tentativi). Va legata a un'interfaccia privata (`--admin-listen 10.x.x.x:porta`) o messa dietro un terminatore TLS. Il token va dato con `--admin-token-file` o `MONGO_ADMIN_TOKEN`: `--admin-token` lo mette nella riga di comando, visibile agli altri utenti della macchina.

### 2.8 Difetto di fusione trovato dalla verifica: l'export scriveva il trailer dell'ordine naturale
* **Come è emerso.** Non era tra le sette richieste: l'ha mostrato la verifica ctl sul binario finale. `test_04_export` (6 falliti su 10), `test_04b_export_vs_real_mongo` (2 su 2) e `test_03b_restore_gc_crash::test_restore_and_export_at_once` (1 su 11) fallivano, e i file `.bson` scaricati non erano BSON valido (`InvalidBSON: bad eoo`, `objsize too large`). Output di prima: `results-final-engine/before_test_04_export.txt`, `before_test_04b_export.txt`, `before_test_03b_export.txt`. Il difetto non c'era su fusion-ops.
* **Causa (provata leggendo il file scaricato).** Dopo la fusione ogni documento salvato è il BSON seguito da un trailer di 9 byte (`0xFE` + numero di sequenza, `natural.rs`, ordine naturale di compat-gaps). L'export di ctl (`ctl/export.rs`, `stream_collection`) scriveva il valore salvato così com'era: i trailer finivano tra i documenti del dump. In più `mongorestore` verso il motore falliva con `IDLUnknownField … insert.bypassEmptyTsReplacement`: l'elenco dei campi ammessi di `insert` (`fields.rs`) non aveva quel campo che `mongorestore` manda e che mongod 7.0.45 accetta.
* **Correzione** (commit `41ed169`): l'export scrive e calcola lo SHA-256 sulla sola parte BSON (`natural::bson_part`); `bypassEmptyTsReplacement` ammesso su `insert`, `update`, `findAndModify`.
* **Test dopo:** `test_04_export` 10 passati, `test_04b_export_vs_real_mongo` 2 passati, `test_03b_restore_gc_crash` 11 passati; nuovo `ctl-tests/test_09g_export_natural_order.py` (dump = BSON puro e nessun byte fuori dai documenti; insert con `bypassEmptyTsReplacement` accettato): 2 passati. **Il test nuovo non ha un "prima" eseguito sul binario vecchio** (avevo già ricompilato): il "prima" è dato dai tre test esistenti sopra.
* **Esito:** risolto. Ho dovuto rifare tutte le verifiche (4.1–4.5) sul binario corretto.

## 3. Controllo negativo della correzione WAL (`gc_tenant_wal`)

`ctl-tests/test_09f_wal_retention_guard.py`, nella suite. Un interruttore di prova (`MONGO_TEST_DISABLE_WAL_RETENTION_GUARD`, letto solo in `gc_tenant_wal`) riporta la pulizia al comportamento di prima della correzione. Storia: 7 raffiche di scritture e aggiornamenti con un database in retention (finestra 3600 s), pulizia ogni 2 s, SST piccole, stato registrato dopo ogni raffica; poi restore a ognuno dei 7 istanti in ordine casuale.

| modalità WAL | senza la correzione | con la correzione |
|---|---|---|
| `--wal-gc-mode slatedb` | **7 restore su 7 falliscono** con 409 `retention_gap` ("WAL object 4 needed ... is missing") | 7 su 7 esatti |
| `--wal-gc-mode bulk` (predefinita) | 7 su 7 esatti | 7 su 7 esatti |

**Correzione di quanto avevo scritto nel rapporto di fusion-ops:** lì dicevo che senza la correzione la pulizia avrebbe cancellato WAL dentro la finestra "e corrotto i restore". Misurato: **solo nelle modalità che usano la pulizia di slatedb**; nella modalità predefinita (`bulk`) la pulizia incrementale salta già ogni database che ha un checkpoint e i punti di ripristino sono checkpoint, quindi era protetta. La correzione serve per le altre modalità; con essa, in `bulk`, i database con retention passano per la pulizia di slatedb con età minima pari alla finestra. Il costo di quella scelta sulla pulizia WAL **non è stato misurato** (richiesto di non misurare il costo WAL in questa fase).
Esito del file: 4 passati, 0 falliti.


## 4. Test suites on the final binary

### 4.1 Existing differential suites (4127 tests, real mongod 7.0.45 as the reference; slices of 150, `tools/final/run_suites.py`)

| driver | core | extra | replset | distinct tests |
|---|---|---|---|---|
| pymongo | 3989/3989 | 49/49 | 89/89 | 4127 |
| Node | 3989/3989 | 49/49 | 89/89 | 4127 |

This includes the 2925 core tests of control-ops' regression and the 1081 of engine-cost (they are part of the 3989 core).

Note (onesto): le fette si eseguono a gruppi da 150 test (in sotto-fette da 50 dopo i riavvii della macchina). Due prime esecuzioni di fette Node non sono passate e sono state **rifatte**; la tabella usa il risultato della riesecuzione. Vedi 4.6.

### 4.2 Gap tests g01-g12 of compat-gaps, auth and TLS (`mongo-tests/gaps`, `tools/final/run_gaps.sh`; engines on the local store, mongod 7.0.45 reference)

| item | pymongo | Node |
|---|---|---|
| g01 | 33/33 identical to mongod 7.0.45 | 25/25 identical to mongod 7.0.45 |
| g02 | 46/46 identical to mongod 7.0.45 | 18/18 identical to mongod 7.0.45 |
| g03 | 14/14 identical to mongod 7.0.45 | 14/14 identical to mongod 7.0.45 |
| g04 | 17/17 identical to mongod 7.0.45 | 12/12 identical to mongod 7.0.45 |
| g05 | 22/23 identical to mongod 7.0.45, 1 known divergences (known_divergences.json) | 20/20 identical to mongod 7.0.45 |
| g06 | 32/32 identical to mongod 7.0.45 | 22/22 identical to mongod 7.0.45 |
| g07 | 426/438 identical to mongod 7.0.45, 12 known divergences (known_divergences.json) | 425/437 identical to mongod 7.0.45, 12 known divergences (known_divergences.json) |
| g08 | 14/14 identical to mongod 7.0.45 | 14/14 identical to mongod 7.0.45 |
| g09 | 140/140 identical to mongod 7.0.45 | 134/134 identical to mongod 7.0.45 |
| g10 | 112/112 identical to mongod 7.0.45 | 96/96 identical to mongod 7.0.45 |
| g11 | 0/0 identical to mongod 7.0.45 | 0/0 identical to mongod 7.0.45 |
| g12 | 8/8 identical to mongod 7.0.45 | 10/10 identical to mongod 7.0.45 |

Note: `g05` pymongo 22/23 e `g07` 12+12 "known divergences" sono divergenze note e documentate di compat-gaps (`mongo-tests/gaps/known_divergences.json`, 13 voci: 12 locale di `g07.locale.*` e `g05.sum_avg.double_accumulation`); non sono nuove. `g11` ha **0 casi**: non esiste nessun modulo `g11_*.py` (i casi `g11.*` compaiono solo nel generatore dello stato): è la risposta giusta della prova, non un errore. `g09` Node: la prima esecuzione ha dato 133/134 (`g09.diag.serverstatus_counters`, contatore `opcounters.command` 3 contro 4); **rieseguito da solo: 134/134**. Causa del primo risultato **non provata** (ipotesi non verificata: un hello del monitor del driver nella finestra di misura). Output della prima esecuzione in `results-final-engine/first-run-failures/gaps_node_g09.first-run.log`. Vedi 4.6.

The auth servers (27028/27030, SCRAM-SHA-1/256, `--auth`) and the TLS pairs (A requireTLS+optional client certificate, B self-signed, C client certificate required, D preferTLS, E requireTLS+`--auth`+X.509) are the ones of compat-gaps (`auth_env.sh`, `tls_env.py`). `ctl-tests/test_11_tls_modes.py` adds the four modes (disabled, allowTLS, preferTLS, requireTLS) x SCRAM-SHA-1/256, plain and TLS.

### 4.3 ctl tests (`ctl-tests/`)

Normal mode (every file) and with access control on (`CTL_TEST_AUTH=1`: server with `--auth`, authenticated root client; files test_01 to test_08b).

| file | passed | failed/error | seconds | with --auth: passed | failed/error |
|---|---|---|---|---|---|
| test_01_databases | 10 | 0 | 13.53 | 10 | 0 |
| test_02_fork | 13 | 0 | 51.35 | 9 | 4 |
| test_02b_fork_crash | 12 | 0 | 42.67 | 0 | 12 |
| test_03_restore | 11 | 0 | 118.49 | 9 | 2 |
| test_03b_restore_gc_crash | 11 | 0 | 150.64 | 9 | 2 |
| test_03c_retention_cost | 1 | 0 | 89.06 | 0 | 1 |
| test_04_export | 10 | 0 | 65.53 | 7 | 3 |
| test_04b_export_vs_real_mongo | 2 | 0 | 4.48 | 2 | 0 |
| test_05_erase | 19 | 0 | 52.44 | 7 | 12 |
| test_06_keepawake | 13 | 0 | 128.57 | 10 | 3 |
| test_07_usage | 10 | 0 | 53.06 | 7 | 3 |
| test_08_costs | 3 | 0 | 347.00 | 0 | 3 |
| test_08b_pin_cost | 1 | 0 | 66.35 | 1 | 0 |
| test_09_security | 30 | 0 | 4.41 | - | - |
| test_09b_users_restore | 1 | 0 | 10.36 | - | - |
| test_09c_tls_rotate | 3 | 0 | 7.43 | - | - |
| test_09d_s3_env | 6 | 0 | 13.33 | - | - |
| test_09e_local_store | 4 | 0 | 26.47 | - | - |
| test_09f_wal_retention_guard | 4 | 0 | 117.25 | - | - |
| test_09g_export_natural_order | 2 | 0 | 4.41 | - | - |
| test_10_end_to_end | 1 | 0 | 14.86 | - | - |
| test_11_tls_modes | 4 | 0 | 49.28 | - | - |
| test_12_shutdown | 2 | 0 | 8.60 | - | - |
| **total** | **173** | **0** | | **71** | **45** |

The 116 tests of the fused branch (111 control-ops + the cost tests) are among these; the files `test_09*` and `test_10`, `test_11`, `test_12` are new in this branch.

**Passata con `--auth`: 71 passati, 45 falliti.** I 45 fallimenti sono **tutti** causati dal limite descritto nel paragrafo "Limite noto: tenantAdmin con --auth" della sezione 7 (44 rifiuti diretti di `tenantAdmin` più 1 conseguenza: `test_08_costs::test_clone_cost_does_not_grow_with_the_data` dà `KeyError: 'fork_clone'` perché `test_costs`, che lo precede, non è arrivato in fondo). Dettaglio per file in `results-final-engine/auth_failures.txt`. I test che non usano `tenantAdmin` passano anche con `--auth`.

### 4.4 SIGKILL durability (8 trials + negative controls, `loadtest/test_group_commit.py`, `loadtest/test_open_gc.py`)

| trial | result |
|---|---|
| sst_1 | `TOTAL safe {'trials': 1, 'killed_inside_cleanup_pass': 1, 'ssts_deleted': 58, 'acked_docs': 10263, 'lost_inserts': 0, 'lost_updates': 0, 'read_violations': {'reads_checked': 29795, 'missing': 0, 'stale': 0, 'padding': 0, 'errors':` |
| sst_2 | `TOTAL safe {'trials': 1, 'killed_inside_cleanup_pass': 1, 'ssts_deleted': 63, 'acked_docs': 10353, 'lost_inserts': 0, 'lost_updates': 0, 'read_violations': {'reads_checked': 29028, 'missing': 0, 'stale': 0, 'padding': 0, 'errors':` |
| sst_3 | `TOTAL safe {'trials': 1, 'killed_inside_cleanup_pass': 1, 'ssts_deleted': 47, 'acked_docs': 10045, 'lost_inserts': 0, 'lost_updates': 0, 'read_violations': {'reads_checked': 30654, 'missing': 0, 'stale': 0, 'padding': 0, 'errors':` |
| sst_4 | `TOTAL safe {'trials': 1, 'killed_inside_cleanup_pass': 1, 'ssts_deleted': 51, 'acked_docs': 10296, 'lost_inserts': 0, 'lost_updates': 0, 'read_violations': {'reads_checked': 27179, 'missing': 0, 'stale': 0, 'padding': 0, 'errors':` |
| sstctl_1 | `TOTAL control {'trials': 1, 'killed_inside_cleanup_pass': 1, 'ssts_deleted': 16, 'acked_docs': 7477, 'lost_inserts': 7477, 'lost_updates': 0, 'read_violations': {'reads_checked': 19876, 'missing': 0, 'stale': 0, 'padding': 0, 'err` |
| sstctl_2 | `TOTAL control {'trials': 1, 'killed_inside_cleanup_pass': 1, 'ssts_deleted': 20, 'acked_docs': 7684, 'lost_inserts': 7684, 'lost_updates': 0, 'read_violations': {'reads_checked': 20674, 'missing': 0, 'stale': 0, 'padding': 0, 'err` |
| wal_1 | `TOTAL s3 durable {'acked_ids': 60877, 'lost_inserts': 0, 'lost_updates': 0}` |
| wal_2 | `TOTAL s3 durable {'acked_ids': 56692, 'lost_inserts': 0, 'lost_updates': 0}` |
| wal_3 | `TOTAL s3 durable {'acked_ids': 60727, 'lost_inserts': 0, 'lost_updates': 0}` |
| wal_4 | `TOTAL s3 durable {'acked_ids': 61539, 'lost_inserts': 0, 'lost_updates': 0}` |
| walctl_1 | `TOTAL s3 control {'acked_ids': 40712, 'lost_inserts': 1738, 'lost_updates': 19}` |
| walctl_2 | `TOTAL s3 control {'acked_ids': 38070, 'lost_inserts': 1302, 'lost_updates': 20}` |

WAL: `wal_1..4` protections on (0 lost); `walctl_1..2` = acknowledge before durable (must lose). SST/manifest/compaction state: `sst_1..4` (kill inside a cleanup pass, 0 lost, 0 reads missing or stale); `sstctl_1..2` = the cleanup also deletes a referenced SST (must lose).

### 4.5 Secrets in logs and outputs

`tools/final/secret_scan.py`: 1063 files scanned (engine logs of every test server, pytest outputs, harness logs) for 14 secrets (every password the tests send, the admin token, the S3 key pair): **none found**.


### 4.6 Prime esecuzioni non passate e cosa è stato fatto (tutte dichiarate)

Tutti i risultati in tabella sono quelli di esecuzioni complete sul binario `d39e4038…`. Le esecuzioni elencate qui sono quelle **che non sono passate al primo colpo**; gli output sono in `results-final-engine/first-run-failures/`.

| cosa | prima esecuzione | causa | riesecuzione |
|---|---|---|---|
| ctl `test_04_export` (1 test: `test_streaming_uses_bounded_memory`) | "memory grew by 20 MB for a 40 MB export" con soglia `< 20` | **misura al limite della soglia** (20 contro 20); il codice di memoria dell'export non è cambiato dal difetto 8 (solo uno slice) | 4 su 4 passati da solo, poi 10 passati nel file. Rischio: test al limite, **non** un difetto provato |
| ctl `test_08_costs[100000]` | `NoSuchBucket` | **colpa mia**: lanciando a mano la pulizia dei bucket ho cancellato il bucket del test in corso | rifatto: 3 passati (347 s) |
| ctl `test_02`, `test_02b`, `test_03`, `test_03b` | errori di scrittura | **disco della sessione pieno** (vedi sotto) | rifatti dopo aver liberato spazio: 13, 12, 11, 11 passati |
| Node fetta 3450:3600 (`js.where_expr_113-115`) | `insert_many` con errore di scrittura senza dettagli | stessa famiglia (disco quasi pieno); **log del motore perso, causa NON provata** | sotto-fetta 3450:3500 rieseguita: 50/50 |
| Node fetta 4100:4127 (`tail.json_schema_*`, 5 test) | stesso errore | disco pieno (log del motore conservato) | rieseguita: 77/77 della fetta |
| gaps `g03` pymongo | timeout DNS nei test `srv` (~21 s ciascuno) | **ambiente di test**: modulo `dnslib` mancante nel venv (serve all'assistente DNS dei test `mongodb+srv://`); installato | rifatto: 14/14 |
| gaps Node `g10`, pymongo `g11`/`g12`, Node `g11` | log vuoti, troncati o `0/0` | scritti a **disco pieno** | rifatti (`g11` ha davvero 0 casi) |
| gaps Node `g09` | 133/134 | non spiegato (vedi 4.2) | 134/134 |

**Disco pieno.** La sessione ha una quota di scrittura piccola (circa 4 GB). Ogni mongod di riferimento dei test gaps tiene circa 300 MB di journal preallocato e a regime ce ne sono una decina; le cartelle `.tools/data/fresh_*` e `tlsA..E` si accumulavano a ogni rilancio (3,6 GB); in più c'erano i dati delle suite in MinIO (566 MB) e quelli del mongod di riferimento (800 MB). Ho cancellato **solo dati di test miei** (con tua autorizzazione per il bucket `bench`); non ho toccato binario, sorgenti, risultati né log. **Osservazione sul motore (non analizzata):** con il disco pieno una `insert` risponde, dopo 6–12 secondi, con un errore di scrittura senza `writeErrors` (un `BulkWriteError` vuoto lato driver): il messaggio potrebbe essere più chiaro. Lo segnalo, non l'ho studiato.

## 5. Binario

* **Commit del motore:** `41ed169` (branch `final-engine`); il binario è compilato dal codice di quel commit (i commit successivi non toccano `mongo-server` né `src`).
* **Compilazione:** `cargo build --release --workspace`
* **SHA256 del binario `target/release/mongo-server`:** `d39e4038be17d2a1e69706c06bd30173209858f94eda10c2e58528d61e925b9b`
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
* **Il control plane e `tenantAdmin`:** **non ho verificato che il control plane (ctl) non usi `tenantAdmin` sulla porta dati.** Lo controllerete voi con la suite e2e sul binario finale. Vedi il paragrafo "Limite noto" nella sezione 7.
* **Test gaps su store locale:** i test gaps eseguono il motore con `--store local` (come nel lavoro di compat-gaps), non con S3/MinIO.
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

### Limite noto: tenantAdmin con --auth

**Il limite esatto.** Con `--auth` nemmeno l'utente `root` può eseguire il comando `tenantAdmin` (il comando di manutenzione del motore che chiude un tenant, `{tenantAdmin: 1, close: true}`) **sulla porta dati**. La tabella dei privilegi (`mongo-server/src/authz.rs`, funzione `required`) non ha una voce per quel comando; il ramo predefinito `_ => (Res::Any, "anyAction")` richiede un privilegio che ha solo l'utente interno `__system`. Il rifiuto è `Unauthorized` (codice 13): `not authorized on <db> to execute command { tenantAdmin: 1, close: true }`. Riprodotto a mano con un motore `--auth` e un utente `root` autenticato (tre tentativi, tre rifiuti, mentre `usersInfo` e `connectionStatus` mostrano root con ruolo `root`).

**Quali test falliscono e perché.** Nella passata ctl con `CTL_TEST_AUTH=1` (motore con `--auth`, client autenticato come root) falliscono 45 test su 116 (71 passano): 44 con il rifiuto di `tenantAdmin` e 1 per conseguenza (`KeyError: 'fork_clone'` in `test_08_costs`, perché il test precedente non è arrivato in fondo). Sono test che usano `tenantAdmin` come appoggio per far dormire il database prima di fork, restore, erase, export, keep-awake, uso o costi. Per file (da `auth_failures.txt`): `test_02_fork` 4, `test_02b_fork_crash` 12, `test_03_restore` 2, `test_03b_restore_gc_crash` 2, `test_03c_retention_cost` 1, `test_04_export` 3, `test_05_erase` 12, `test_06_keepawake` 3, `test_07_usage` 3, `test_08_costs` 3 (2 + 1 di conseguenza). Passano con `--auth`: `test_01_databases` 10/10, `test_04b_export_vs_real_mongo` 2/2, `test_08b_pin_cost` 1/1 e tutti gli altri test dei file elencati che non usano il comando. Senza `--auth` tutti i 173 test ctl passano.

**Come comanda il control plane.** Le operazioni di ctl (creare, forkare, ripristinare, cancellare, esportare, pin, retention) arrivano al motore dalla **porta admin** con il token bearer, che è indipendente dalla SCRAM della porta dati: con `--auth` continuano a funzionare (provato: `test_09_security.py::test_admin_port_is_independent_of_scram` e `test_nothing_of_ctl_is_reachable_from_the_data_port`, e le operazioni di ctl che non passano da `tenantAdmin` nella passata `--auth`).

**Cosa NON ho verificato.** Non ho verificato che il control plane non usi `tenantAdmin` sulla porta dati. Lo controllerete voi con la suite e2e sul binario finale.

**Decisione presa dall'utente.** Non correggere ora e non rifare le verifiche (circa 2,5 ore). Correzione possibile in futuro: una riga, mappare `tenantAdmin` a un'azione di cluster (per esempio `closeAllDatabases`, che `root` ha) in `authz.rs`; richiederebbe di rifare tutte le verifiche perché cambia il motore.

### Test e strumenti toccati
Nessun test esistente è stato modificato per farlo passare (il test di memoria al limite non è stato toccato). Modifiche ad altri file di test e strumenti: `ctl-tests/lib.py` (modalità `CTL_TEST_AUTH`, impostazioni MinIO esplicite), blocco con le variabili MinIO nei lanciatori di `loadtest/*.py`, `mongo-tests/*.py`, `ctl-tests/probe_existing.py`, `mongo-tests/server.sh` (sorgente `tools/minio-env.sh`), `mongo-tests/run_py.py` e `run_node.js` (`--slice` e `--shard`). Nuovi: `ctl-tests/test_09*.py`, `test_10`, `test_11`, `test_12`, `tools/final/*`.

## 8. Branch e commit finale

Branch `final-engine`. Commit del motore (quello da cui è compilato il binario): `41ed169`. Il commit finale con questo rapporto è indicato nel messaggio di consegna (il suo hash non può stare dentro il file che contiene). Pushato solo `origin/final-engine`. `main`, `compat-gaps`, `fusion-ops` e gli altri branch invariati; nessuna PR; nessun merge in `main`. Nessun load test, nessuna misura di costo WAL.
