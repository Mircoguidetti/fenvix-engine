# results-main-candidate: candidato per main e rinnovo delle credenziali IMDS (branch `main-candidate`)

Base: `origin/engine-round2` **`561f05d`**. Pubblicato solo `main-candidate`. Nessun merge, nessuna PR. `main` e gli altri branch non sono stati toccati.
Nessuna attività AWS, nessuna credenziale reale, nessuno screenshot. Le chiavi del test della Parte 2 sono finte: le genera `ctl-tests/fake_aws.py` e valgono solo lì.
Fra parentesi, dopo ogni affermazione: **come l'ho verificata**.

## 1. Commit del branch

| commit | cosa |
|---|---|
| `561f05d` … | tutta la storia di `engine-round2`, invariata (`git log origin/engine-round2..HEAD` mostra solo i commit qui sotto) |
| `ef2a640` | cherry-pick di `34fedce` (release-pipeline): solo `.github/workflows/release-engine.yml`, **nessun conflitto** (`git cherry-pick` riuscito; `git diff origin/release-pipeline HEAD -- .github/workflows/release-engine.yml` vuoto) |
| `5c64701` | `results-aws-test3.md` e `results-aws-test3/` (27 file) presi da `aws-test3` `b7a51af` con `git checkout origin/aws-test3 -- …` (`git diff origin/aws-test3 HEAD -- results-aws-test3.md results-aws-test3` vuoto) |
| `f306ba5` | test della Parte 2 (`ctl-tests/fake_aws.py`, `ctl-tests/test_19_imds_credential_refresh.py`), `tools/main-candidate/` (scansione dei segreti, esperimento sull'interruzione di IMDS) |
| commit di questo report | `results-main-candidate.md`, `results-main-candidate/` |

**Non portati** da aws-test3: `.github/workflows/aws-test3.yml`, `aws-test3/tenantstats-auth.patch`, gli script in `aws-test3/` (`git ls-files .github aws-test3` → solo `.github/workflows/release-engine.yml`).
**Motore invariato** rispetto a engine-round2 (`git diff origin/engine-round2 HEAD -- mongo-server src loadgen Cargo.toml Cargo.lock` vuoto). Nessuna modifica al motore in questo lavoro.

## 2. Controlli sul workflow release-engine.yml

* `Cargo.lock` è tracciato (`git ls-files Cargo.lock`) e fissa `object_store 0.14.2`.
* `RUST_TOOLCHAIN: "1.97.0"`, installato con `rustup toolchain install "$RUST_TOOLCHAIN"`. La build è `cargo +"$RUST_TOOLCHAIN" build --release --workspace --locked`, il binario è `target/release/$ENGINE_BINARY_NAME` con `ENGINE_BINARY_NAME: mongo-server` (lettura del file).
* Il pacchetto `mongo-server` produce davvero `target/release/mongo-server` con quel comando (eseguito qui con rustc 1.97.0).
* **Attenzione (decisione D1):** `--workspace` **non** produce lo stesso binario di `-p mongo-server`, che è quello usato in tutte le regressioni dei round precedenti. Vedi sezione 4.

## 3. Test: prima / dopo

"Prima" sono i numeri del report engine-round2 (binario `615c631a…`). "Dopo" è questo ambiente, con il binario compilato col comando della CI (`--workspace --locked`, SHA-256 `2af97e7f…`). Output grezzi in `results-main-candidate/regression/`.

| controllo | prima (engine-round2) | dopo (main-candidate) | come |
|---|---|---|---|
| pymongo core / extra / replset | 3989/3989, 49/49, 89/89 | **3989/3989, 49/49, 89/89** | `tools/netlimits/run_suites.py`, conteggio sui JSON `py_*.json` |
| Node core / extra / replset | 3989/3989, 49/49, 89/89 | **3989/3989, 49/49, 89/89** | idem, `node_*.json` |
| ctl, tutti i file | 424 passati, 0 falliti | **427 passati, 1 fallito** (i 424 esistenti: 423 + 1 fallito; più i 4 nuovi di test_19) | `ctl_test_*.txt` |
| di cui test_04_export | 10/10 | **9/10** al primo giro (`test_streaming_uses_bounded_memory`: "memory grew by 22 MB for a 40 MB export", limite 20). Vedi sotto | `ctl_test_04_export.txt` |
| di cui test_09_security | 30/30 | **30/30** | |
| di cui test_13 / test_14 | 84/84, 10/10 | **84/84, 10/10** | |
| di cui test_15…18 | 54, 5, 94, 4 | **54, 5, 94, 4** | |
| test_19 (nuovo) | – | **4/4 in 188,6 s** | `ctl_test_19_imds_credential_refresh.txt` |
| ctl con `--auth` (test_01…08b) | 71 / 45 | **71 / 45**, stessi file e stessi numeri per file | `ctlauth_*.txt` |
| motivo dei 45 | tutti tenantAdmin | **tutti tenantAdmin**: 90 messaggi `not authorized … tenantAdmin` (2 per fallimento), 0 per tenantStats | `grep` sugli output |
| SIGKILL wal_1..4 | 0 perdite | **0 inserimenti e 0 aggiornamenti persi** (75649, 77282, 76650, 73800 scritture confermate) | `kill_wal_*.txt` |
| SIGKILL sst_1..4 | 0/0/0 | **0 persi, 0 mancanti, 0 obsoleti** (12931, 13056, 13420, 12539 documenti) | `kill_sst_*.txt` |
| controlli walctl_1..2 (devono perdere) | perdono | **2150/17 e 807/17 persi** | `kill_walctl_*.txt` |
| controlli sstctl_1..2 (devono perdere) | perdono | **9259/9259 e 9343/9343 persi** | `kill_sstctl_*.txt` |
| test Rust (`cargo test --release -p mongo-server --locked`) | 60/60 | **60/60** | output di cargo |

### Il fallimento di test_04_export (memoria dell'export)

Ho rieseguito il file 3 volte per binario (`results-main-candidate/rerun_test_04_export_*.txt`). Poi ho eseguito solo quel test 5 volte per binario, con una copia del test che stampa la crescita della memoria (`results-main-candidate/export_memory_growth.txt`; la copia sta fuori dal repo, il test del repo non è stato modificato):

| binario | file intero ×3 | crescita misurata ×5 (limite < 20 MB) |
|---|---|---|
| `--workspace` (CI) `2af97e7f…` | 1 fallito (23 MB), 2 passati | 16, 17, 16, 8, 12 MB |
| `-p mongo-server` `615c631a…` | 3 passati | 9, 12, 13, 2, 12 MB |

Sul binario della CI il test è fallito 2 volte su 9 (22 e 23 MB). Su quello di engine-round2 è fallito 0 volte su 8. Il campione è piccolo, ma il binario della CI sta più vicino al limite. Un fallimento "al limite" di questo test c'era già stato in final-engine (`results-final-engine/first-run-failures/borderline_ctl_test_04_export.txt`, 20 MB). È un test di memoria con soglia stretta, non un errore funzionale: l'export resta corretto, come mostrano gli altri 9 test del file e test_04b. **Non l'ho corretto e non ho cambiato il test.** Decisione D1.

## 4. SHA-256 del binario (solo informativo)

| build | SHA-256 | come |
|---|---|---|
| `cargo build --release --workspace --locked` (comando della CI), in `/home/user/test5/target` | **`2af97e7fb86773a08c2c9d95f4a0b18994d850f3349be092b40687183a7c73cc`** | due build, la seconda da `target/` vuoto: stesso SHA |
| `cargo build --release -p mongo-server --locked`, in `/home/user/test5/target` | `615c631aea767267e646cacc665995eb77f06f52eb719a94ca8d1b5ba398fcc6` = **identico** al report engine-round2 | build e `sha256sum` |
| `-p mongo-server` con `CARGO_TARGET_DIR=/tmp/claude-0/target-p` | `ddd6370ab266642c5ac3650b5843908e3efdc2bd1c5f97039b9e2854ad9f7af5` | build e `sha256sum` |

* Le build sono riproducibili a parità di comando e di percorso. Il percorso della cartella di build entra nel binario: lo stesso comando in un'altra cartella dà un altro SHA. Quindi **lo SHA della CI (runner in un altro percorso) sarà diverso da tutti questi**, come previsto.
* `--workspace` e `-p` differiscono perché cargo unisce le feature delle dipendenze di tutti i pacchetti: con `--workspace` entrano anche quelle chieste da `loadgen`, per esempio `flate2` con `runtime_detection` e `crossbeam-*` con `default` (diff di `cargo tree -e normal -f "{p} [{f}]"` con `-p mongo-server` e con `--workspace`).
* Tutta la regressione della sezione 3 è stata eseguita sul binario `2af97e7f…`, cioè quello del comando della CI. Rustc 1.97.0 (`rustc --version`).

## 5. Parte 2: rinnovo delle credenziali del ruolo dell'istanza (IMDSv2)

**Esito: sì, il motore segue il rinnovo senza riavvio.** Non serve nessuna modifica al motore. C'è però un limite nel caso di IMDS non raggiungibile durante il rinnovo (5.4).

### 5.1 Come il motore costruisce il client S3 (lettura del codice)

* `mongo-server/src/main.rs:359`: `AmazonS3Builder::from_env().with_bucket_name(&args.bucket)`, poi solo regione ed endpoint (righe 360–371). Nessuna credenziale, nessuna `RetryConfig`, nessun `with_credentials`.
* object_store 0.14.2 (`Cargo.lock`), `src/aws/builder.rs`:
  * `from_env` (righe 606–618) legge ogni variabile `AWS_*`, anche `AWS_METADATA_ENDPOINT` (righe 524, 666).
  * `build()` (righe 1156–1253) usa, in quest'ordine: chiavi statiche se c'è `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (`StaticCredentialProvider`, che non scade mai: è il caso di aws-test3), poi web identity, poi credenziali di container, poi EKS. **Altrimenti `InstanceCredentialProvider`** su `http://169.254.169.254` (riga 43), dentro un `TokenCredentialProvider` (righe 1242–1253).
* `src/aws/credential.rs:648–709` (`instance_creds`): `PUT /latest/api/token` (IMDSv2, TTL 600 s), `GET …/security-credentials/` per il nome del ruolo, `GET …/<ruolo>` per le credenziali. La scadenza è presa dal campo `Expiration` (righe 702–707). Il fallback IMDSv1 è spento per default (righe 668–671).
* `src/client/mod.rs:1112–1119`: `get_credential()` passa dalla `TokenCache`.
* `src/client/token.rs`: la cache tiene la credenziale finché restano **più di 300 s** alla scadenza (`min_ttl`, riga 51). Sotto quella soglia la richiede di nuovo, al massimo ogni 100 ms (`fetch_backoff`, righe 54 e 72–79). Se la nuova richiesta **fallisce, restituisce l'errore** (riga 95, `f().await?`) e non riusa la credenziale vecchia, anche se è ancora valida.
* `src/aws/client.rs:457`: ogni richiesta S3 chiama `get_credential()` prima di firmare. Quindi ogni richiesta usa la credenziale corrente della cache.

### 5.2 Il test (`ctl-tests/test_19_imds_credential_refresh.py`, 4 test, nella suite ctl)

Viene avviato il **vero binario** `mongo-server --store s3`, senza `AWS_ACCESS_KEY_ID`, con `AWS_METADATA_ENDPOINT` puntato al finto IMDS. Quindi il client è costruito dallo stesso codice di produzione, `from_env()` di `main.rs:359`. Il finto IMDSv2 (`fake_aws.py`):
* senza token risponde 401;
* dà credenziali nuove a ogni richiesta, oppure (`rotate_after`) le stesse fino alla rotazione, come fa EC2.

Il finto S3 in memoria verifica ogni richiesta come S3:
* chiave sconosciuta → `InvalidAccessKeyId`;
* chiave scaduta → **403 `ExpiredToken`**;
* `x-amz-security-token` che non è della chiave → rifiutata;
* **firma SigV4 ricalcolata con il segreto di quella chiave** → `SignatureDoesNotMatch` se non torna.

Il traffico è una scrittura e una lettura ogni 0,25 s, più un database nuovo ogni 10 s (apertura a freddo: list/get/head).

| test | scenario | risultato (esecuzione nella suite) |
|---|---|---|
| `test_short_expiry_20s` | credenziali che scadono **20 s** dopo l'emissione | 126 scritture, 0 fallite; 433 richieste S3, **0 rifiutate**; 148 credenziali emesse. Sotto i 300 s di margine la cache le richiede a ogni richiesta (oltre 100 ms) |
| `test_renewal_window` | scadenza 320 s, cioè finestra di **20 s** (320 − 300) | **FAKEKEY0001 usata 0,10–18,91 s, FAKEKEY0002 19,21–39,09 s, FAKEKEY0003 39,44–51,06 s**; 152/193/168 richieste per chiave, 0 rifiutate, solo 3 credenziali chieste. Si vedono insieme la cache e il rinnovo |
| `test_late_rotation` | come su EC2: IMDS restituisce **la stessa** credenziale fino a 25 s, cioè 5 s dopo che object_store inizia a chiederne una nuova | FAKEKEY0001 usata fino a 24,82 s, poi FAKEKEY0002; 0 rifiutate; 20 richieste di credenziali in quei 5 s (limitate dai 100 ms) |
| `test_control_static_credentials_expire` | **controllo negativo**: chiavi statiche dall'ambiente (come aws-test3) che il finto S3 fa scadere a 12 s | 42 scritture confermate prima, poi **256 richieste rifiutate `ExpiredToken`**, sempre `FAKESTATIC01`, scritture fallite. Il test quindi sa accorgersi di un mancato rinnovo |

In tutti e tre i casi IMDS: stesso PID del motore dall'inizio alla fine e processo vivo, 0 richieste ai metadati senza token, 0 chiamate non gestite dal finto S3. Prove: `results-main-candidate/imds_*.txt`.

**Tempo aggiunto alla suite: 188,6 s** (`4 passed in 188.56s`, `ctl_test_19_imds_credential_refresh.txt`). I quattro test durano 45 s + 50 s + 40 s + 20 s di traffico, più avvio e arresto del motore.

### 5.3 La correzione proposta

Nessuna: il motore rinnova. Il motore non è stato modificato.

### 5.4 Limite trovato: IMDS non raggiungibile durante il rinnovo (esperimento, non nella suite)

`tools/main-candidate/imds_outage.py`, eseguito 2 volte, output in `results-main-candidate/imds_outage.txt`. Credenziali da 320 s. IMDS risponde 503 da 12 s a 32 s, cioè proprio quando, a ~19 s, object_store vuole rinnovare.
* Fino a ~19 s funziona tutto.
* **Da ~19 s a ~32 s nessuna richiesta S3**: le scritture restano ferme e una impiega **12,9 s**. Eppure la chiave vecchia aveva ancora ~300 s di validità. Il log del motore mostra solo `SLOW insert … ms=12963`, nessun errore.
* A 32 s IMDS torna: nuova chiave e ripresa automatica, 0 scritture fallite in questo esperimento.

Il motivo è `token.rs:95`, descritto in 5.1: in caso di errore la cache non ripiega sulla credenziale ancora valida. I tentativi seguono la `RetryConfig` di default di object_store (`retry.rs:252–259`: 10 tentativi, 3 minuti; backoff 100 ms → 15 s, `backoff.rs:40–47`). Un'interruzione di IMDS più lunga del timeout del client, dentro gli ultimi 5 minuti di validità, fa quindi fallire le richieste, anche con credenziali valide. **Non testato** oltre i 20 s di interruzione. Decisione D3.

## 6. Scansione dei segreti (`tools/main-candidate/secret_scan.py`, `results-main-candidate/secret_scan.json`)

* **Log e output:** 771 file, 23 valori cercati (password dei test, token admin, coppia MinIO e i valori finti di test_19: segreti, token di sessione, token IMDS). **Nessuno trovato.**
* **File del repo:** i 152 file nuovi o modificati rispetto a `origin/engine-round2`. Pattern cercati: chiavi cloud, chiavi private, token dei provider, credenziali negli URL e, in più, **ARN AWS con ID account e ID account a 12 cifre** (i file di aws-test3 vengono da un'esecuzione reale). **Nessuno trovato.**
* Controllo a mano dei file di aws-test3 (`grep` di `arn:`, `AKIA`/`ASIA`, numeri a 12 cifre, host `amazonaws.com`):
  * i numeri lunghi sono timestamp `unix_ms` a 13 cifre;
  * il nome del bucket compare come `fenvix-test-t3-40846615265` (id del run, bucket già cancellato secondo il report aws-test3);
  * la regione è `eu-central-1`.
  Nessuno di questi è un segreto.

## 7. Decisioni che servono a te

* **D1, binario della CI ≠ binario testato in passato.** La CI costruisce con `--workspace`, e le feature di `loadgen` entrano nelle dipendenze del motore. Tutti i round precedenti avevano testato il binario `-p mongo-server`. Qui ho testato quello `--workspace`: tutto uguale, tranne il test di memoria dell'export, più vicino al limite (2 fallimenti su 9). Due strade:
  * cambiare una riga del workflow in `cargo +"$RUST_TOOLCHAIN" build --release -p mongo-server --locked` (il binario resta `target/release/mongo-server`). È lo stesso binario dei test precedenti e non porta dentro dipendenze del generatore di carico. **È quello che consiglio.**
  * Lasciare `--workspace` e accettare il test al limite, oppure alzarne la soglia.

  Non l'ho cambiato: mi avevi chiesto di portare il commit di release-pipeline così com'è.
* **D2, CI e percorsi.** Lo SHA dipende dal percorso di build, quindi quello della CI non sarà confrontabile con nessuno di questi. Il valore da usare è quello stampato dalla CI.
* **D3, interruzione di IMDS durante il rinnovo (5.4).** Opzioni:
  * (a) accettare: IMDS è un servizio locale dell'host e il motore lo interroga solo negli ultimi 5 minuti di validità;
  * (b) impostare nel motore una `RetryConfig` o un timeout esplicito, poche righe, che però non eliminano lo stallo;
  * (c) far ripiegare il motore sulla credenziale ancora valida. Servirebbe un `CredentialProvider` proprio passato a `with_credentials`, ma `InstanceCredentialProvider` non è pubblico in object_store 0.14.2 (`pub(crate)`, `credential.rs:569`), quindi bisognerebbe riscrivere il client IMDS: non sono poche righe.

  Non ho cambiato nulla.
* **D4, hop limit di IMDSv2 sull'EC2.** Se il motore girasse in un container sull'istanza, serve hop limit ≥ 2. Non verificato: è infrastruttura, non motore.

## 8. Cosa NON ho potuto verificare

* **Su AWS vero:** IMDS reale, durata reale delle credenziali del ruolo, quando EC2 le ruota. Nessuna attività AWS, come richiesto. Volevo citare la documentazione AWS sui tempi di rotazione, ma `docs.aws.amazon.com` non è raggiungibile da questa sessione (dominio non consentito dalla rete). Il caso "EC2 rinnova almeno 5 minuti prima" è coperto dal test con rotazione in ritardo e dal margine di 300 s di object_store, ma il comportamento reale di EC2 **non è verificato**.
* **mongodump/mongorestore 100.14.1:** non li ho potuti compilare dai sorgenti (il clone del repository mongo-tools è stato bloccato dai permessi della sessione). Ho usato **100.20.0 dell'immagine ufficiale `mongo:7.0.45`**. Con questa versione test_04b passa 2/2 (anche con `--auth`).
* **mongod di riferimento:** 7.0.45 dall'immagine Docker Hub `mongo:7.0.45`, come in engine-round2. MinIO compilato dai sorgenti (`go install github.com/minio/minio@latest`, quindi versione non fissata).
* **Numeri "prima" nello stesso ambiente:** non rieseguiti, vengono dal report engine-round2. Però il binario `-p` ricostruito qui è bit per bit identico al suo (`615c631a…`).
* **JSON dei test SIGKILL:** lo script riscrive 4 file tracciati in `loadtest/results/`. Ne ho salvato uno (`regression/kill_json/durability-s3-durable.json`) prima di ripristinarli con `git checkout`. Gli altri tre contenevano solo l'ultima prova, i cui totali sono comunque nei `kill_*.txt`.
* **Interruzione di IMDS più lunga di 20 s,** IMDSv1, web identity / ECS / EKS: non testati.
* **CI GitHub:** il workflow non è stato eseguito (richiede segreti e AWS). Ho verificato solo il contenuto e il comando di build in locale.
