# results-final-engine — il motore finale (branch `final-engine`)

Base: `origin/fusion-ops` (e98ed24). Fuso: `origin/compat-gaps` (5df2720). Nessun merge in `main`, nessuna PR; `main` (618c3bd), `compat-gaps` (5df2720), `fusion-ops` e gli altri branch non sono stati toccati; si fa push solo di `final-engine`.
Ambiente: quello delle sessioni precedenti (mongod 7.0.45 di riferimento estratto dall'immagine ufficiale `mongo:7.0`, MinIO, pymongo 4.x, Node). Non si è usato AWS e non si è speso credito. Ciò che non è stato eseguito è scritto "non provato".
Il binario verificato è quello della sezione 5 (commit `@COMMIT@`, SHA256 `@SHA@`); ogni fase della sezione 4 è stata eseguita su di esso.

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
* **Test dopo:** `@CTL:test_09_security@` (file `test_09_security.py`), compresi i casi che devono continuare a funzionare (root che crea di tutto, `userAdmin` che dà `readWrite`/`dbOwner` su `app1`, `userAdminAnyDatabase` che dà ruoli su un database con nome, la sola azione `createUser` che crea utenti senza ruoli).
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
* **Test dopo:** `@CTL:test_09d_s3_env@` (file `test_09d_s3_env.py`); la regione e la chiave firmate sono provate con un finto endpoint S3 locale che registra l'intestazione di firma (`Credential=chiave/data/regione/s3/...`), senza costo; la sessione e il token di sessione sono nell'intestazione `x-amz-security-token`.
* **Esito:** risolto per `mongo-server`. `src/main.rs` del crate radice (lo strumento di esperimenti `tenant-bench`, non il motore) ha ancora `minioadmin` fisso: non toccato.

### 2.4 Erase, fork, restore e gli utenti
* **Test prima** (`test_09_security.py`, 3 falliti): dopo l'erase gli utenti e i ruoli del database restavano in `admin.system.users`/`system.roles`; nessun rifiuto se si ricreava un database con utenti residui.
* **Correzione** (`mongo-server/src/auth.rs`, `ctl/erase.rs`, `ctl/meta.rs`, `ctl/fork.rs`): l'erase elimina utenti **e ruoli** definiti su quel database (passo idempotente della ripresa dopo crash, dopo aver preso il lease e prima di cancellare gli oggetti) e le connessioni aperte di quegli utenti perdono i privilegi subito (il contatore di generazione cambia). Il **fork** non eredita gli utenti (gli utenti vivono nel tenant `admin`, il fork copia solo il tenant: provato). **create** e **fork** rifiutano con 409 `users_exist` un nome che ha ancora utenti o ruoli (non li adottano in silenzio). **Restore:** non riguarda gli utenti, che non sono nei dati del database: dopo un restore un utente creato dopo l'istante c'è ancora, uno eliminato dopo l'istante resta eliminato, una password cambiata resta cambiata (documentato e provato in `test_09b_users_restore.py`). I nomi `admin`, `local`, `config` restano vietati per ctl (provato).
* **Test dopo:** `@CTL:test_09_security@`, `@CTL:test_09b_users_restore@`, end-to-end `@CTL:test_10_end_to_end@`.
* **Esito:** risolto.

### 2.5 `rotateCertificates` risponde ok senza ricaricare
* **Test prima** (`test_09c_tls_rotate.py`, 2 falliti su 3): dopo aver sostituito il file del certificato e dato il comando, le nuove connessioni ricevevano ancora il vecchio; con file inutilizzabili rispondeva ok.
* **Correzione** (`mongo-server/src/tls.rs`, `admin_cmds.rs`): il comando rilegge certificato, chiave e CA dai file e ne sostituisce l'accettatore per le connessioni nuove (le aperte continuano); se i file non sono utilizzabili risponde `OperationFailed` (96) con il motivo e **il certificato precedente resta in uso**. Con TLS disattivato non c'è nulla da ruotare e risponde ok come mongod (la suite differenziale `g11.admin` lo confronta): è una decisione, D2.
* **Test dopo:** `@CTL:test_09c_tls_rotate@` (fingerprint SHA-256 del certificato mostrato prima e dopo; connessione aperta prima che continua a funzionare).
* **Esito:** risolto.

### 2.6 `--store local`: `PutMode::Update not yet implemented`
* **Indagine.** Riprodotto: create funziona (usa `PutMode::Create`), fork, keep-awake, erase, retention e il risveglio di un database addormentato (il lease si rinnova con `PutMode::Update`, `lease.rs`) fallivano con 500. **È un limite solo del file locale della libreria:** `object_store` 0.14.2, `src/local.rs:399-403`, restituisce `NotImplemented` per `PutMode::Update`. **Su S3** (`src/aws/mod.rs:255-300`) con `S3ConditionalPut::ETagMatch`, che è il valore predefinito (`src/aws/precondition.rs:117-128`), `PutMode::Create` invia `If-None-Match: *` e `PutMode::Update` invia `If-Match: <etag>` (con ritentativo sugli 409 che S3 può dare); `Disabled` darebbe lo stesso errore. Il motore usa già queste due operazioni per i lease (`lease.rs`), quindi non aggiunge dipendenze nuove. Su MinIO le stesse operazioni funzionano: sono i test ctl (fork, keep-awake, erase, retention, restore) della sezione 4.3, tutti verdi.
* **Test prima** (`test_09e_local_store.py`, 3 falliti su 4).
* **Correzione** (`mongo-server/src/localcas.rs`): un involucro del file locale che implementa `Update` confrontando l'etag sotto un mutex che copre ogni scrittura del processo. **Limite:** vale per un solo processo sulla directory (S3 lo fa lato server, per oggetto): lo store locale resta per sviluppo e test.
* **Test dopo:** `@CTL:test_09e_local_store@`.
* **Da verificare su S3 vero (non provato qui):** che AWS accetti `If-Match` su `PutObject` con l'etag letto (scritture condizionali per ETag, annunciate da AWS a fine 2024) nel bucket e nella regione scelti, `If-None-Match: *`, il comportamento con il versioning attivo, e il 409 `ConditionalRequestConflict` sotto concorrenza (la libreria lo ritenta).

### 2.7 TLS sulla porta admin
Non serve ora (rete privata): **limite noto documentato.** La porta admin è HTTP semplice con token bearer (almeno 16 caratteri, confronto a tempo costante, nessun limite di tentativi). Va legata a un'interfaccia privata (`--admin-listen 10.x.x.x:porta`) o messa dietro un terminatore TLS. Il token va dato con `--admin-token-file` o `MONGO_ADMIN_TOKEN`: `--admin-token` lo mette nella riga di comando, visibile agli altri utenti della macchina.

## 3. Controllo negativo della correzione WAL (`gc_tenant_wal`)

`ctl-tests/test_09f_wal_retention_guard.py`, nella suite. Un interruttore di prova (`MONGO_TEST_DISABLE_WAL_RETENTION_GUARD`, letto solo in `gc_tenant_wal`) riporta la pulizia al comportamento di prima della correzione. Storia: 7 raffiche di scritture e aggiornamenti con un database in retention (finestra 3600 s), pulizia ogni 2 s, SST piccole, stato registrato dopo ogni raffica; poi restore a ognuno dei 7 istanti in ordine casuale.

| modalità WAL | senza la correzione | con la correzione |
|---|---|---|
| `--wal-gc-mode slatedb` | **7 restore su 7 falliscono** con 409 `retention_gap` ("WAL object 4 needed ... is missing") | 7 su 7 esatti |
| `--wal-gc-mode bulk` (predefinita) | 7 su 7 esatti | 7 su 7 esatti |

**Correzione di quanto avevo scritto nel rapporto di fusion-ops:** lì dicevo che senza la correzione la pulizia avrebbe cancellato WAL dentro la finestra "e corrotto i restore". Misurato: **solo nelle modalità che usano la pulizia di slatedb**; nella modalità predefinita (`bulk`) la pulizia incrementale salta già ogni database che ha un checkpoint e i punti di ripristino sono checkpoint, quindi era protetta. La correzione serve per le altre modalità; con essa, in `bulk`, i database con retention passano per la pulizia di slatedb con età minima pari alla finestra. Il costo di quella scelta sulla pulizia WAL **non è stato misurato** (richiesto di non misurare il costo WAL in questa fase).
Esito del file: @CTL:test_09f_wal_retention_guard@.
