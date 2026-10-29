# results-network-limits — lista di IP per database e limiti di rete (branch `network-limits`)

Base: `origin/final-engine` (e038db7). Si fa push solo di `network-limits`; nessun merge, nessuna PR; `final-engine`, `main`, `compat-gaps`, `fusion-ops` non toccati.
Nessuna attività AWS, nessun load test, nessun segreto reale. Ambiente ricostruito da zero in questa sessione: mongod 7.0.45 di riferimento (tgz ufficiale `fastdl.mongodb.org`, database tools 100.14.1), MinIO compilato dai sorgenti, pymongo 4.19.0, driver Node 7.7.0. Ciò che non è stato eseguito è scritto "non provato".

* **Commit del motore:** `1c9fee7` (i commit successivi toccano solo test, strumenti e risultati: `git diff 1c9fee7..HEAD -- mongo-server src Cargo.toml Cargo.lock` è vuoto).
* **Binario:** `cargo build --release -p mongo-server`, `target/release/mongo-server`, **SHA-256 `8b3d7b71e63326d4a6a1913a96ace4fb1e0c3f6efd8ee26161ab48be677632f4`**. Tutte le verifiche "dopo" sono state eseguite su questo file.
* **Numeri "prima" della regressione: vengono da `results-final-engine.md`**, cioè da **un altro ambiente** (un altro container, binario `d39e4038…` compilato dal commit `41ed169`). La passata "prima" in questo ambiente era partita sul binario ricompilato qui da `e038db7` (SHA-256 `0d1c8b6fbe52ab79f8214173299e101825cf53cdc7207794ad90ec290981da7e`, non identico a `d39e4038…`), ma è stata **fermata** per mancanza di tempo dopo la prima fetta (pymongo 0:150, 150/150), e i suoi risultati parziali non sono usati. Il binario ricompilato da `e038db7` è stato usato solo per eseguire i test nuovi "prima" del codice (sezione 3.1).

## 1. Cosa è stato costruito

Metodo per ogni punto: prima il test (`ctl-tests/test_13_ip_allowlist.py`, `ctl-tests/test_14_auth_limits.py`, test unitari in `mongo-server/src/netguard.rs`), eseguito sul binario di partenza (sezione 3.1), poi il codice, poi gli stessi test.

### 1.1 Lista di reti ammesse per database (A1–A8)

| punto | cosa fa |
|---|---|
| A1 | Fino a **100** reti per database: CIDR IPv4 e IPv6, un indirizzo singolo è salvato come `/32` o `/128`. Lista vuota o assente = tutti ammessi (comportamento di prima). Forma canonica: `2001:DB8::1` → `2001:db8::1/128`, `::ffff:10.0.0.0/104` → `10.0.0.0/8`. |
| A2 | Solo dalla porta admin con il token. Comandi nuovi, nello stile di quelli esistenti: **`PUT /v1/databases/{db}/ip-allowlist`** con corpo `{"entries": ["10.0.0.0/8", "203.0.113.7"]}` (imposta, sostituisce tutta la lista), **`GET /v1/databases/{db}/ip-allowlist`** (leggi), **`DELETE /v1/databases/{db}/ip-allowlist`** (svuota). Risposta: `entries`, `allow_all`, `max_entries`, `enforced` (falso se il motore gira senza `--auth`). Rifiuto con **400 `invalid_allowlist`** e un messaggio che indica la voce: CIDR non valido, bit dopo il prefisso (`10.0.0.1/8`), prefisso con zeri, segni o spazi, zone IPv6 (`fe80::1%eth0`), caratteri diversi da cifre esadecimali `. : /`, voce vuota o più lunga di 49 caratteri, voce non stringa, **duplicati** (anche dopo la forma canonica: `1.2.3.4` e `1.2.3.4/32`, `10.0.0.0/8` e `::ffff:10.0.0.0/104`), **più di 100 voci**, campi sconosciuti, corpo non JSON. Database inesistente: 404; `admin`, `local`, `config`: 400. Sulla porta dati non esiste nessun comando equivalente (provato con root). |
| A3 | Salvata nel record dei metadati del database (`control/dbs/{db}/meta.json`, campo `ip_allowlist`): sopravvive a sleep/wake e ai riavvii. Il processo che la cambia la usa subito; gli altri processi sullo stesso bucket la rileggono al più ogni `--ip-allowlist-refresh-secs` (5 s). Se il record non si può leggere e non c'è una copia già letta, l'autenticazione è rifiutata (fallisce chiuso). |
| A4 | Al `saslStart` (SCRAM-SHA-1 e -256) l'indirizzo del peer TCP è confrontato con la lista del database su cui ci si autentica, **prima** che la password sia guardata. Se non è ammesso, lo scambio continua come per una password sbagliata (stessa risposta del primo passo, stesso sale) e si chiude con **lo stesso errore** (`code 18 AuthenticationFailed`, `"Authentication failed."`): la password non viene verificata e la lista non è rivelata. Tempi misurati: mediana 5,07 ms (password sbagliata) contro 5,20 ms (IP non ammesso). Nel log una riga con indirizzo e database, senza nome utente, password, nonce o prova. Quando una lista viene impostata, le connessioni già autenticate su quel database da indirizzi esclusi perdono subito l'autenticazione (il comando successivo riceve `Unauthorized`). |
| A5 | **`--trusted-networks`** (CIDR separati da virgole; predefinito `127.0.0.0/8,::1/128`, cioè solo loopback; valore vuoto = nessuna rete fidata). Queste reti saltano la lista, i blocchi e il limite di connessioni. Un valore non valido impedisce l'avvio con un messaggio che cita il flag. |
| A6 | Fork: il database nuovo ha un record nuovo, quindi **non eredita la lista** (come non eredita gli utenti). Erase: il record è cancellato con tutto `control/dbs/{db}/`, **la lista sparisce** e un database ricreato con lo stesso nome parte senza lista. Restore: **non tocca la lista** (né il ripristino a un istante precedente la riporta indietro, né una lista svuotata riappare). |
| A7 | La porta admin non è soggetta alla lista (provato chiamandola da un indirizzo escluso). |
| A8 | L'indirizzo è **sempre quello della connessione TCP**. Il motore non legge PROXY protocol, intestazioni né campi del client: una riga `PROXY TCP4 …` chiude la connessione (non è un messaggio MongoDB), un campo che dichiara un altro indirizzo non cambia nulla (provato). Un peer IPv4-mappato (`::ffff:a.b.c.d`, se si ascolta su `[::]`) è trattato come l'indirizzo IPv4. |

**Come impostare `--trusted-networks` (A5).** Mettere solo le reti da cui arrivano i processi dello stesso server: il control plane e il Data Explorer. Con processi sullo stesso host: il predefinito (loopback) basta. Se girano in container sull'host: aggiungere la rete dei container, per esempio `--trusted-networks 127.0.0.0/8,::1/128,172.17.0.0/16` (la rete reale si legge con `docker network inspect bridge`). **Non** mettere mai una rete da cui possono arrivare client esterni. **Attenzione con Docker:** se il motore stesso gira in un container con la porta pubblicata tramite il proxy userland di Docker o un NAT che riscrive la sorgente, tutti i client esterni appaiono con l'indirizzo del gateway (es. `172.17.0.1`): in quel caso fidarsi della rete dei container renderebbe fidati tutti. Il motore va eseguito con la rete dell'host (o con una pubblicazione che conservi l'indirizzo sorgente). Lo stesso vale per i limiti della parte B, che senza l'indirizzo reale conterebbero tutti come un solo client.

**Davanti al motore non ci sono proxy (A8).** Nessun bilanciatore, terminatore TLS o proxy TCP deve stare tra i client e la porta dati: il motore vedrebbe l'indirizzo del proxy. Se in futuro servisse un proxy, va prima aggiunto un supporto esplicito (PROXY protocol solo da indirizzi fidati), che oggi non c'è di proposito.

### 1.2 Limiti (B1–B4)

| flag | predefinito | effetto |
|---|---|---|
| `--auth-fail-max` | 10 | autenticazioni fallite da un indirizzo nella finestra che bloccano l'indirizzo **su tutti i database** |
| `--auth-fail-max-per-db` | 5 | fallite da un indirizzo **su un database** che bloccano l'indirizzo solo su quel database |
| `--auth-fail-window-secs` | 60 | finestra (scorrevole) |
| `--auth-block-secs` | 300 | durata del primo blocco; ogni blocco nuovo della stessa chiave la **raddoppia** |
| `--auth-block-max-secs` | 3600 | blocco più lungo |
| `--auth-block-forget-secs` | 86400 | senza blocchi per questo tempo, il raddoppio riparte da capo |
| `--max-conns-per-ip` | 200 | connessioni simultanee per indirizzo (0 = nessun limite); le altre sono chiuse subito, prima del TLS |
| `--handshake-timeout-secs` | 10 | handshake TLS e primo messaggio entro questo tempo dall'apertura; con `--auth`, prima dell'autenticazione ogni messaggio iniziato deve arrivare intero entro questo tempo (0 = spento) |
| `--pre-auth-max-message-bytes` | 65536 | messaggio più grande accettato prima dell'autenticazione (con `--auth`), anche dopo la decompressione; rifiutato dall'intestazione, senza allocare nulla |
| `--ip-allowlist-refresh-secs` | 5 | rilettura della lista dai metadati |
| `--ipv6-group-prefix` | 64 | gli IPv6 sono contati e limitati per /64 (un client cambia facilmente indirizzo dentro il suo /64) |
| `--netguard-max-tracked` | 100000 | indirizzi seguiti al massimo (limite di memoria) |

* Conta ogni autenticazione fallita: password sbagliata, utente inesistente, IP non ammesso dalla lista (così un indirizzo escluso si comporta in tutto come uno con la password sbagliata, blocco compreso). Un indirizzo bloccato riceve l'errore generico `18` **senza che la password sia verificata**; i tentativi durante il blocco non si contano (il blocco non si allunga da solo). Quando scatta il blocco dell'indirizzo, i contatori per database di quell'indirizzo ripartono.
* Gli indirizzi fidati non sono contati, bloccati né limitati nel numero di connessioni.
* Le connessioni inattive dopo il primo messaggio non hanno un limite di tempo: i driver tengono connessioni di monitoraggio che non si autenticano mai e aspettano tra un `hello` e l'altro. Il tetto per indirizzo le limita.
* Senza `--auth` valgono solo il limite di connessioni e il tempo per il primo messaggio; la lista e i blocchi riguardano l'autenticazione.
* **Stato dalla porta admin:** `GET /v1/network` (configurazione, indirizzi bloccati con database, scadenza, secondi rimanenti e numero del blocco, indirizzi con fallimenti nella finestra, connessioni aperte per indirizzo, totali). Nessun nome utente e nessuna password: il motore non li registra. `DELETE /v1/network/blocks/{ip}` toglie i blocchi e i contatori di un indirizzo (utile se un cliente si blocca da solo). Contatori e blocchi sono in memoria, per processo.
* **Correzione aggiunta durante il lavoro:** i decompressori (zlib, zstd, snappy) non avevano un tetto reale: un messaggio compresso piccolo poteva espandersi oltre la dimensione dichiarata. Ora si fermano alla dimensione dichiarata (che prima dell'autenticazione è limitata dal flag sopra).

### 1.3 File toccati

| file | |
|---|---|
| `mongo-server/src/netguard.rs` | nuovo: reti, liste, blocchi, contatori, limiti, stato; test unitari |
| `mongo-server/src/ctl/allowlist.rs` | nuovo: rotte admin della lista e dello stato |
| `mongo-server/src/auth.rs` | controllo dell'indirizzo prima della password, rifiuto identico a una password sbagliata, conteggio dei fallimenti, perdita dell'autenticazione per le connessioni escluse |
| `mongo-server/src/wire.rs` | lettura con dimensione massima e scadenze, decompressione limitata |
| `mongo-server/src/main.rs` | flag, posto per connessione, timeout dell'handshake |
| `mongo-server/src/ctl/api.rs`, `ctl/mod.rs`, `ctl/meta.rs`, `ctl/fork.rs`, `ctl/erase.rs` | rotte; la copia in memoria della lista è dimenticata a create, fork ed erase |
| `ctl-tests/netlib.py`, `test_13_ip_allowlist.py`, `test_14_auth_limits.py` | nuovi test |
| `tools/netlimits/*` | regressione riprendibile, watchdog, mutazioni, scansione dei segreti, tabelle |
| `results-network-limits/`, `results-network-limits.md` | risultati |

Nessun test esistente è stato modificato.

## 2. Test nuovi

| file | test | esito sul binario finale |
|---|---|---|
| `test_13_ip_allowlist.py` | 84 | 84 passati (anche nella passata ctl della regressione) |
| `test_14_auth_limits.py` | 10 | 10 passati (anche nella passata ctl della regressione) |
| unitari `netguard.rs` | 7 | 7 passati (`results-network-limits/unit_tests.txt`) |

Coperti: IP ammesso e non ammesso (wire grezzo e pymongo); bordi `/0`, `/31`, `/32`, `/8`, `/9`, forma IPv4-mappata nella lista, rete IPv6 contro peer IPv4; 37 voci ostili e 13 corpi ostili, limite di 100; persistenza dopo sleep, wake e riavvio; fork che non eredita; erase che cancella; restore che non tocca; reti fidate (predefinita, personalizzata, non valida); porta admin non soggetta; PROXY protocol e campi del client ignorati; connessioni già autenticate che perdono il database; risposta identica (primo passo, sale, documento d'errore, eccezione pymongo, tempi); log senza segreti. Limiti: forza bruta bloccata con la password giusta rifiutata; blocco che scade e raddoppia fino al massimo; finestra; contatori per indirizzo e per (indirizzo, database), utenti inesistenti contati; sblocco dalla porta admin; limite di connessioni (5 e predefinito 200) con fidati esenti; mittente lento e connessione muta chiusi, connessione di monitoraggio inattiva lasciata aperta; messaggio troppo grande prima dell'autenticazione e intestazione da 40 MB rifiutati, 1 MB dopo l'autenticazione accettato; stato senza nomi utente né password; log senza password.
IPv6 e peer IPv4-mappati: **solo test unitari** (il kernel di questo sandbox non ha IPv6, nemmeno `::1`).

## 3. Controlli negativi

### 3.1 I test nuovi sul binario di partenza
`9 failed, 5 passed, 80 errors` (`results-network-limits/before_new_tests.txt`): il motore di partenza non conosce i flag e le rotte nuove, quindi quasi tutti i server di test non partono. I **5 "passati"** sono i test che si aspettano che il motore **rifiuti di partire** con un `--trusted-networks` non valido: il binario vecchio rifiuta anche lui, ma perché il flag non esiste. Non sono una prova; la prova vera sono le mutazioni.

### 3.2 Mutazioni (`tools/netlimits/mutations.py`, diff e output in `results-network-limits/mutations/`)
Ogni controllo è stato tolto con una modifica di una riga, il motore ricompilato, i due file di test eseguiti, il sorgente ripristinato con `git checkout` (alla fine `git status` pulito e binario ricompilato).

| mutazione | effetto | test falliti |
|---|---|---|
| `ip_check` | ogni indirizzo passa la lista | **14** su 94 |
| `block` | nessun indirizzo è mai bloccato | **4** |
| `conn_limit` | nessun limite di connessioni | **2** |
| `handshake_timeout` | nessun timeout | **1** |
| `pre_auth_size` | nessun tetto prima dell'autenticazione | **1** |
| `distinct_refusal` | l'IP escluso riceve un messaggio diverso ("address not allowed") | **4** |

## 4. Regressione sul binario finale

"Prima" = numeri di `results-final-engine.md` (**altro ambiente**, vedi in alto). "Dopo" = questo ambiente, binario `8b3d7b71…`. Output grezzi in `results-network-limits/regression/new/`.

### 4.1 Suite differenziali (4127 test, mongod 7.0.45 di riferimento, fette da 150, `tools/netlimits/run_suites.py`)

| driver | categoria | prima (final-engine, altro ambiente) | dopo |
|---|---|---|---|
| pymongo | core | 3989/3989 | **3989/3989** |
| pymongo | extra | 49/49 | **49/49** |
| pymongo | replset | 89/89 | **89/89** |
| Node | core | 3989/3989 | **3989/3989** |
| Node | extra | 49/49 | **49/49** |
| Node | replset | 89/89 | **89/89** |

### 4.2 ctl (`ctl-tests/`)

| file | prima | dopo | dopo con `--auth` (prima: stessi numeri) |
|---|---|---|---|
| test_01_databases | 10 | 10 | 10 / 0 |
| test_02_fork | 13 | 13 | 9 / 4 |
| test_02b_fork_crash | 12 | 12 | 0 / 12 |
| test_03_restore | 11 | 11 | 9 / 2 |
| test_03b_restore_gc_crash | 11 | 11 | 9 / 2 |
| test_03c_retention_cost | 1 | 1 | 0 / 1 |
| test_04_export | 10 | 10 (vedi nota) | 7 / 3 |
| test_04b_export_vs_real_mongo | 2 | 2 (vedi nota) | 2 / 0 |
| test_05_erase | 19 | 19 | 7 / 12 |
| test_06_keepawake | 13 | 13 | 10 / 3 |
| test_07_usage | 10 | 10 | 7 / 3 |
| test_08_costs | 3 | 3 | 0 / 3 |
| test_08b_pin_cost | 1 | 1 | 1 / 0 |
| **test_09_security** | 30 | **30** | - |
| test_09b_users_restore | 1 | 1 | - |
| test_09c_tls_rotate | 3 | 3 | - |
| test_09d_s3_env | 6 | 6 | - |
| test_09e_local_store | 4 | 4 | - |
| test_09f_wal_retention_guard | 4 | 4 | - |
| test_09g_export_natural_order | 2 | 2 | - |
| test_10_end_to_end | 1 | 1 | - |
| test_11_tls_modes | 4 | 4 | - |
| test_12_shutdown | 2 | 2 | - |
| test_13_ip_allowlist (nuovo) | - | 84 | - |
| test_14_auth_limits (nuovo) | - | 10 | - |
| **totale** | **173 passati, 0 falliti** | **267 passati, 0 falliti** (173 esistenti + 94 nuovi) | **71 / 45** (prima: 71 / 45) |

Con `--auth` i 45 fallimenti sono **gli stessi** di final-engine, file per file: il limite noto di `tenantAdmin` (rifiuto `Unauthorized` sulla porta dati anche per root; controllato nell'output, es. `ctlauth_test_02_fork.txt`). Non è una regressione.

**Nota su test_04_export / test_04b (prima esecuzione non passata):** nella passata ctl il primo giro ha dato `9 passed, 1 skipped` e `2 skipped`: l'ambiente ricostruito aveva `mongodump`/`mongorestore` in `/tmp/mongo7-bin`, i test li cercano in `/tmp/claude-0/mongo-img/root/usr/bin`. Aggiunto il collegamento, rieseguiti due volte sullo stesso binario: **10 passati, 2 passati** (`results-network-limits/rerun_test_04_export.txt`, `rerun_test_04b_export_vs_real_mongo.txt`). Problema dell'ambiente, non del motore.

### 4.3 SIGKILL (`loadtest/test_group_commit.py`, `loadtest/test_open_gc.py`)

| prova | prima (final-engine) | dopo |
|---|---|---|
| wal_1..4 (protezioni attive) | 0 inserimenti e 0 aggiornamenti persi in tutte | **0 / 0 in tutte** (60895, 64735, 67414, 64167 scritture confermate) |
| walctl_1..2 (conferma prima della durabilità: deve perdere) | 1738/19 e 1302/20 persi | **292/21 e 741/17 persi** (il controllo funziona) |
| sst_1..4 (kill dentro una pulizia) | 0 persi, 0 letture mancanti o vecchie | **0 persi, 0 mancanti, 0 vecchie** in tutte (11033, 11088, 10818, 11040 documenti) |
| sstctl_1..2 (la pulizia cancella un SST in uso: deve perdere) | 7477/7477 e 7684/7684 persi | **8351/8351 e 8725/8725 persi** (il controllo funziona) |

I JSON delle prove (che lo script riscrive in `loadtest/results/`, file di final-engine già nel repository) sono copiati in `results-network-limits/regression/kill_json/`; i file tracciati sono stati ripristinati com'erano.

### 4.4 Altro
* Test unitari Rust di `netguard.rs`: 7 passati. Gli altri 51 test unitari del crate **non** sono stati eseguiti.
* Test "gaps" g01–g12 di compat-gaps (auth e TLS con i driver): **non eseguiti** in questa sessione (non richiesti; il limite di 64 KiB prima dell'autenticazione e il timeout potrebbero toccarli solo se un test mandasse comandi grandi prima di autenticarsi).
* **Riavvio della macchina** alle 06:08 durante le suite Node: la regressione è ripresa alle 08:03 dalle fette finite (tutte le fette sono state eseguite intere sul binario finale); da lì un watchdog ha controllato il runner (`results-network-limits/first-run-notes.txt`).

## 5. Scansione dei segreti (`tools/netlimits/secret_scan.py`, `results-network-limits/secret_scan.json`)

* **Log e output:** 877 file (log del motore di ogni server di test, output pytest, risultati), 15 valori cercati (tutte le password dei test vecchi e nuovi, il nonce marcato, il token admin, la coppia MinIO). **Nessuno nei log del motore.** Le password finte di test_13/test_14 compaiono solo in 2 output pytest delle mutazioni (`mutations/ip_check.txt`, `mutations/block.txt`): sono le righe del sorgente del test che pytest stampa quando un test fallisce di proposito.
* **File del repository:** i 154 file nuovi o cambiati rispetto a `origin/final-engine` cercati per chiavi AWS, chiavi private, token GitHub/Slack/Google/API, credenziali negli URL: **nessuno trovato**. I valori fissi dei test (password finte, token di test, `minioadmin`) sono elencati a parte nel JSON: non sono segreti.



## 6. Decisioni che servono a te

* **D1 — soglie per indirizzo e per (indirizzo, database).** Hai indicato 10 errori in 60 s per lo stesso IP. Se anche la coppia avesse 10, non scatterebbe mai prima dell'indirizzo; l'ho messa a **5** (blocca solo quel database), l'indirizzo a **10** (blocca tutto). Con un NAT condiviso (un ufficio) un'applicazione con la password sbagliata blocca per 5 minuti tutti quelli dietro quel NAT; c'è lo sblocco `DELETE /v1/network/blocks/{ip}`. Valori diversi: sono solo flag.
* **D2 — quale lista si applica:** quella del database su cui il client **si autentica** (`authSource`). Un utente definito sul database A con ruoli su B passa con la lista di A. Con gli utenti dei clienti definiti sul loro database (come oggi) coincidono; se vuoi che conti anche la lista di ogni database su cui l'utente ha ruoli, è un'aggiunta piccola ma cambia il comportamento per utenti con ruoli su più database.
* **D3 — utenti su `admin` e `$external`** (i vostri, X.509): nessuna lista (non sono database dei clienti); i blocchi valgono anche per loro, tranne che dalle reti fidate.
* **D4 — una lista nuova toglie subito l'autenticazione** alle connessioni già aperte da indirizzi esclusi (non le chiude). Solo nel processo che riceve il comando; gli altri processi applicano la lista alle autenticazioni nuove entro 5 s, ma non toccano le connessioni già autenticate.
* **D5 — senza `--auth` la lista non ha effetto** (nessuno si autentica); la risposta lo dice (`enforced: false`). Non ho impedito di impostarla.
* **D6 — un IP escluso conta come una password sbagliata** e quindi può essere bloccato: serve a non rivelare la lista (senza, un attaccante vedrebbe che dopo 10 errori non scatta nessun blocco).
* **D7 — una rete IPv6 non contiene mai un peer IPv4**, anche `::/0` (un peer `::ffff:a.b.c.d` è IPv4). Per ammettere tutti: `0.0.0.0/0` e `::/0`, o lista vuota.
* **D8 — IPv6 contato per /64** (flag). "Stesso IP" alla lettera per IPv6 renderebbe i blocchi inutili.
* **D9 — voci con bit dopo il prefisso rifiutate** (`10.0.0.1/8`), invece di correggerle in silenzio: la richiesta era una validazione severa.
* **D10 — limite di 64 KiB prima dell'autenticazione e timeout di 10 s**: nessuna differenza con mongod nelle suite (che non autenticano o autenticano subito); un client che mandasse comandi grandi prima di autenticarsi verrebbe chiuso invece di ricevere `Unauthorized`.
* **D11 — le connessioni inattive non autenticate non scadono** (quelle di monitoraggio dei driver ne hanno bisogno); il tetto di 200 per indirizzo le limita. Non c'è un tetto globale di connessioni (non richiesto).

## 7. Cosa NON ho potuto verificare

* **Numeri "prima" nello stesso ambiente:** non misurati (passata fermata per tempo); i "prima" della sezione 4 vengono da `results-final-engine.md`, altro container e altro binario.
* **Test gaps g01–g12** e gli altri **51 test unitari** del crate: non eseguiti sul binario finale.

* **IPv6 end to end** e peer IPv4-mappati reali: il kernel del sandbox non ha IPv6 (nemmeno `::1`). Coperti solo dai test unitari (parsing, confronto, raggruppamento per /64, peer mappato).
* **Indirizzi diversi da 127.0.0.x:** i client "esterni" dei test sono connessioni dal loopback con sorgente 127.0.0.2…250 (stesso percorso di codice, il peer TCP è quello). Nessuna prova da una rete vera.
* **Comportamento dietro un proxy o un NAT** (Docker userland proxy, bilanciatori): non provato; vedi 1.1 ("davanti al motore non ci sono proxy").
* **S3 vero:** nessuna prova su AWS; la lista è salvata nel record dei metadati come gli altri campi di ctl (provato su MinIO).
* **Carico:** nessun load test (come richiesto). Non misurati: costo del controllo su molti tentativi al secondo, memoria con 100.000 indirizzi seguiti, comportamento sotto una vera inondazione di connessioni.
* **Più processi sullo stesso bucket:** contatori e blocchi sono per processo (un attaccante che parla con due processi ha il doppio dei tentativi); la propagazione della lista entro 5 s tra processi non è stata provata con due processi.
* **TLS:** il timeout dell'handshake TLS è nel codice (stessa scadenza del primo messaggio) ma i test nuovi girano senza TLS; le suite TLS di `test_11_tls_modes` sono nella regressione.

## 8. Branch e commit

Branch `network-limits` (da `origin/final-engine` e038db7). Commit del motore: `1c9fee7`. SHA-256 del binario: `8b3d7b71e63326d4a6a1913a96ace4fb1e0c3f6efd8ee26161ab48be677632f4`. Il commit finale con questo rapporto è indicato nel messaggio di consegna. Pushato solo `origin/network-limits`.
