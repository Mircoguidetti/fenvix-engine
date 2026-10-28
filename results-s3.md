# Test del motore su S3 vero (eu-central-1): preparazione — results-s3

Branch `s3-test-draft` (da `engine-cost`, perché `merge-engine` non è ancora pubblicato). **Nulla di quanto descritto qui è stato eseguito su AWS.** Nessun file in `.github/`, nessun workflow, nessuna chiamata AWS, nessuna credenziale. Quello che è stato provato è stato provato solo in locale, con comandi `aws`/`shutdown` finti e con un S3 finto (`s3-test/local/fake_s3.py`).

## 1. Cosa è stato preparato (tutto sotto `s3-test/`, più questo file)

| File | A cosa serve |
|---|---|
| `workflows/s3-test.yml.txt` | Il workflow. Diventa `.github/workflows/s3-test.yml` solo con "AVVIA AWS". |
| `run.json` | File che fa partire il workflow (unico `paths`). `tests` = quali test; `merge_results_blob` per i test 2/3. |
| `patches/0001-s3-aws-instance-profile.patch` | Correzione del motore (vedi §4). Applicata in Actions al momento della build, **non** committata nel sorgente. |
| `engine/s3guard.rs` | Contatore di richieste a livello HTTP (retry inclusi) e limitatore di richieste al secondo. Stessa copia dentro la patch. |
| `probe/` | `s3probe`: 1.000 PUT + 1.000 GET e confronto con il contatore. |
| `scripts/gate.sh` | Prima di qualsiasi credenziale: valida `run.json`, somma del registro + 5 $ ≤ 24 $, `results-merge.md` per i test 2/3. |
| `scripts/build.sh` | Compila in Amazon Linux 2023 (stessa glibc dell'istanza) motore patchato + probe; fa il bundle. |
| `scripts/orchestrate.sh` | Test 1 lato runner: ogni azione provata una per una, errori raccolti tutti insieme con azione IAM e risorsa. Lancia l'istanza, aspetta SSM, esegue i test sull'istanza. |
| `scripts/watchdog.sh` | Secondo controllo di spesa, ogni 30 s, dal workflow. |
| `scripts/guard.py` | Primo controllo di spesa, **sull'istanza**, ogni 10 s, avviato prima del motore. |
| `scripts/run.sh`, `instance_test{1,2,3}.sh`, `engine-wrap.sh`, `make_subset.py`, `test3.py` | Cosa gira sull'istanza. |
| `scripts/cleanup.sh`, `finalize.sh`, `ledger.py`, `cost.py`, `cost_model.py` | Pulizia, registro dei costi, calcolo del costo, modello di costo mensile. |
| `prices.json` | Prezzi eu-central-1 letti dai listini ufficiali AWS (non a memoria). |

Test preparati: **solo 1, 2, 3.** `gate.sh` rifiuta qualsiasi altro numero. I test 4, 5, 6 non sono preparati.

## 2. Prezzi verificati (eu-central-1, listini AWS, scaricati il 2026-10-27)

- PUT, COPY, POST, LIST: **$0,0054 per 1.000** richieste (listino S3 pubblicato 2026-10-15).
- GET e altre: **$0,0043 per 10.000** richieste.
- S3 Standard: **$0,0245 per GB-mese** (primi 50 TB).
- EC2 c6i.large Linux on-demand: **$0,097 all'ora** (listino EC2 pubblicato 2026-10-23).
- EBS gp3: $0,0952 per GB-mese (volume root 8 GB).
- **Non trovato / non incluso:** costo dell'IPv4 pubblico; trasferimenti dati; tasse. DELETE non ha un prezzo nel listino (gratis), ma vedi §6 sul DeleteObjects.

## 3. Limiti di spesa scritti nel codice

| Limite richiesto | Dove | Stato |
|---|---|---|
| Una sola istanza alla volta | `orchestrate.sh`: `DescribeInstances` (tag `project=fenvix-tests`, `pending`/`running`) prima del dry-run **e** subito prima di `RunInstances`; se ce n'è una non lancia niente. `concurrency` del workflow: un run alla volta. `--count 1`. | provato solo con `aws` finto |
| Solo c6i.large | `--instance-type c6i.large` fisso. | |
| Max 90 minuti | `user-data`: `shutdown -h +90` come prima riga, più `InstanceInitiatedShutdownBehavior=terminate`. Non dipende da guard né da GitHub. | **non provato su AWS** |
| Controllo sull'istanza ogni 10 s | `guard.py`, avviato da `run.sh` prima di tutto; se non produce un battito entro 60 s, `run.sh` spegne. Legge i contatori del motore (`tenantStats.s3Http`), calcola il costo lordo (PUT/LIST a prezzo PUT, GET a prezzo GET, ore di istanza), carica i contatori su S3. Ferma motore e spegne se costo > soglia. | provato in locale (con `shutdown` finto) |
| Soglia | **tetto 5 $; il guard ferma a 4,5 $** (margine 0,5 $, vedi sotto). Se il motore gira ma i contatori non sono leggibili per 2 minuti, ferma lo stesso. | |
| Secondo controllo dal workflow ogni 30 s | `watchdog.sh`: calcola il costo dagli stessi contatori su S3 e termina l'istanza a > 5 $; la termina anche se il battito del guard ha più di 90 s o non appare entro 8 minuti. | **non provato** (solo sintassi) |
| Registro dei costi | `s3-test/ledger.json` sul branch `s3-test-results`; `gate.sh` rifiuta se somma + 5 $ > 24 $. Un run senza contatori recuperabili è registrato a **tetto + ore di istanza**, mai a zero. | |
| Tag `project=fenvix-tests` | Istanza, volume, bucket. | |
| Pulizia con `if: always()` | `cleanup.sh`: termina le istanze con il tag, svuota ed elimina il bucket; riautentica il ruolo prima (la sessione da 1 h potrebbe essere scaduta). Un errore di permessi in pulizia finisce in `errors.jsonl`. | |
| Un solo trigger | `push` su `s3-test` con `paths: s3-test/run.json`. Niente `pull_request`, `environment`, `schedule`, `workflow_dispatch`. | verificato leggendo il file |
| Nessuna chiave | Solo `aws-actions/configure-aws-credentials@v4`, ruolo `arn:aws:iam::865859654353:role/fenvix-gha-tests`, `role-duration-seconds: 14400`, secondo passo a 3600 se il primo fallisce. | |
| AMI | `ec2:DescribeImages` (owner `amazon`, `al2023-ami-2023.*-x86_64`). Nessun `ssm:GetParameter`. | |
| Rete | Subnet e security group di default, nessuna porta aperta, IMDSv2 obbligatorio. Comandi solo via SSM `AWS-RunShellScript`. Output lungo scritto su S3 dall'istanza. | |
| Risultati | Branch `s3-test-results`, `permissions: contents: write`. | |

### Perché il tetto regge (il calcolo, non la speranza)

Il punto debole del piano di partenza era "quante richieste al secondo può generare una macchina": nessuno lo aveva misurato. Per questo ho aggiunto **un limitatore di richieste nel motore** (`--s3-max-rps`, default 300 per processo; 600 solo nel test 3, che ha un processo solo). Ogni richiesta HTTP passa dal limitatore, retry compresi. Allora il costo che può accumularsi tra due controlli è un numero:

- Massimo tra due letture del guard: 2 processi × 300 req/s × (10 s + ~25 s per fermare e spegnere) × $0,0054/1.000 ≈ **$0,11**. Il margine di 0,5 $ lo copre.
- Se il guard muore (doppio guasto): il watchdog se ne accorge dopo al massimo ~130 s (90 s di battito vecchio + 30 s di ciclo + terminazione): 600 req/s × 130 s × $5,4e-6 ≈ **$0,42**, quindi al peggio ~4,9 $ lordi in quel caso.
- Cintura in più: ogni processo del motore esce da solo (codice 86) se da solo supera 4 $ di richieste.
- **Registro:** un run parte solo se somma registrata + 5 $ (tetto di un run) non supera 24 $ (tetto scelto perché 24 $ + IVA 22% ≈ 29,3 $), quindi il totale registrato non supera 24 $. Limite residuo: il registro conta il costo lordo calcolato dai contatori; un run senza contatori recuperabili è registrato a tetto + ore di istanza.

Questo vale **se** il limitatore funziona (provato in locale: test unitari e prova contro un S3 finto, §7) e **se** i prezzi e le classi di richiesta sono giusti. Non è una garanzia di AWS.

## 4. Il motore non usava il profilo dell'istanza (problema trovato)

Nel codice di `engine-cost` (`mongo-server/src/main.rs`) lo store `s3` è:

- endpoint personalizzato (`--endpoint`, default MinIO), `allow_http(true)`;
- regione fissa `us-east-1`;
- **credenziali scritte nel codice: `minioadmin` / `minioadmin`**.

Così com'è, il motore su AWS vero **non** userebbe il profilo dell'istanza e sarebbe rifiutato da S3. Correzione (`patches/0001`):

- nuovo `--store s3-aws`: nessun endpoint, nessuna chiave nel builder, `--region` (default `eu-central-1`). Verificato nel sorgente di `object_store 0.14.2`: senza chiavi il builder usa il provider dell'istanza (IMDSv2, `builder.rs` ~riga 1242).
- `--s3-max-rps`, `--s3-process-cap-usd`, e il connettore HTTP di `s3guard.rs`.
- `tenantStats` ha un campo nuovo `s3Http` con i conteggi a livello HTTP.
- `mongo-server/Cargo.toml`: dipendenza `async-trait` (già nel lockfile; `Cargo.lock` aggiornato nella patch).
- Lo store `s3` esistente (MinIO) **non cambia**.

Compila (`cargo build --release --locked`) su `engine-cost`; i 3 test unitari di `s3guard` passano. Non è mai stata provata su S3 vero.

## 5. Il contatore del motore sottostima le richieste fatturate (problema trovato)

`CountingStore` sta sopra lo strato di retry di `object_store`: una richiesta ritentata viene contata una volta ma S3 la fattura più volte. Prova in locale (S3 finto che risponde 500 ogni 7 richieste): il contatore del motore segna **esattamente 1.000 PUT e 1.000 GET**, mentre a livello HTTP le richieste sono **1.166 PUT e 1.167 GET**. Quindi un test "il contatore segna 1.000" passerebbe e allo stesso tempo il costo reale sarebbe più alto del contatore.

Cosa fa il Test 1: `s3probe` esegue 1.000 PUT e 1.000 GET, riporta **entrambi** i conteggi (`counter` e `http`) e il numero di retry visti solo a livello HTTP. Il costo usa il più grande dei due. Su S3 vero si saprà quante volte S3 ha ritentato davvero.

## 6. Cosa NON posso verificare senza eseguire (elenco, non riclassificato)

1. Che il ruolo `fenvix-gha-tests` abbia i permessi: **mancano `iam:PassRole` (sul ruolo dentro il profilo `fenvix-test-instance`) e i permessi SSM** (`ssm:SendCommand` su `AWS-RunShellScript` e sulle istanze col tag, `ssm:GetCommandInvocation`, `ssm:DescribeInstanceInformation`). Il Test 1 deve fallire lì e dirlo, in un colpo solo. Servono anche `ec2:RunInstances` (+`CreateTags`) su istanza, immagine, subnet, security group, interfaccia di rete, volume; `ec2:DescribeImages`, `DescribeInstances`, `DescribeVpcs`; `ec2:TerminateInstances` (condizione sul tag); `s3:CreateBucket`, `PutBucketTagging`, `PutObject`, `GetObject`, `ListBucket`, `DeleteObject`, `DeleteBucket` su `fenvix-test-*`. Se `ec2:DescribeImages` sia nella policy attuale non lo so.
2. Che la durata di 4 ore sia stata salvata: se non lo è, il workflow ripiega su 3600 (e riautentica per la pulizia).
3. Che `iam:PassRole` sia verificato dal dry-run di `RunInstances`: non sempre lo è. La prova vera è il lancio reale.
4. Che l'istanza si registri a SSM (il ruolo dell'istanza deve avere permessi equivalenti a `AmazonSSMManagedInstanceCore`, e serve una rotta verso gli endpoint SSM: la subnet di default ha un IPv4 pubblico, il cui costo **non ho verificato**).
5. `dnf install python3-pip`, `pip install pymongo`, `npm ci`, il download di MongoDB 7.0.14 (`fastdl.mongodb.org`, l'URL risponde 200) e di Node v22.22.0: da Amazon Linux 2023 non provati. Se un download fallisce il test 2 si ferma e lo scrive.
6. Se il **DeleteObjects** (`POST ?delete`) è gratuito: l'ho contato a prezzo PUT in ogni tetto (prudente), separato nei contatori (`deletePost`), e non l'ho verificato.
7. Se i crediti da 100 $ coprono ogni voce (IPv4 pubblico, EBS, tasse). Si vede solo leggendo la fattura dopo il Test 1.
8. Il comportamento reale del limitatore sul motore sotto carico (a 300/600 req/s): potrebbe aumentare le latenze o far scadere richieste. `tenantStats.s3Http.throttled` e `waitedUs` lo riportano; il test 3 salva `throttled` in ogni misura.
9. I tempi: il test 3 è stimato in ~35–45 minuti, ma non è mai girato su S3 vero. Il timer a 90 minuti è fisso.
10. `test3.py` e `guard.py` non sono mai stati eseguiti su AWS. `test3.py` è stato eseguito in forma ridotta contro il motore con store locale (non S3) e `aws` finto.
11. `gate.sh`, `build.sh`, `watchdog.sh` e `finalize.sh` non sono mai stati eseguiti nemmeno in locale (solo controllo di sintassi `bash -n`); `ledger.py` sì. Il passo Docker di `build.sh` (rustup dentro `amazonlinux:2023`) non è stato provato.
12. L'azione `aws-actions/configure-aws-credentials@v4` e `actions/checkout@v4` sono fissate per tag, non per hash di commit.

## 7. Cosa è stato provato in locale (senza AWS)

- Motore patchato compilato; 3 test unitari di `s3guard` (classificazione delle richieste, limitatore: 600 richieste a 200/s impiegano 2–3,5 s, round-trip del file contatori).
- `s3probe` contro `fake_s3.py`: senza errori, contatore esatto = 1.000/1.000 e HTTP identico; con un 500 ogni 7 richieste, contatore 1.000/1.000 e HTTP 1.166/1.167 (§5). 2.000 richieste in 5,7 s con limite 300/s (burst compreso).
- `guard.py` contro un motore locale: scrive i contatori; con soglia bassa ferma il motore e chiama `shutdown` (finto).
- `orchestrate.sh` con `aws` finto: S3 ok, `DescribeImages` negato → errore registrato con azione IAM e risorsa, i passi dipendenti marcati SKIPPED, nessun passo "aggirato". `cleanup.sh`: errore di `TerminateInstances` registrato.
- `test3.py` in modalità ridotta (10/20/30 database): arriva in fondo, scrive il JSON; `cost_model.py` stampa la tabella dai dati ridotti.

Osservazione dal test ridotto locale (da **non** prendere come misura): a riposo il motore ha mostrato un flusso di richieste non nullo (~0,9 PUT-tier/s con 10 database fermi, store locale, `--no-leases`). Se si ripetesse su S3 sarebbe ~$13/mese per nodo, e peserebbe sul costo dei database dormienti. Il test 3 lo misura (finestra di 60 s con tutti i database chiusi) e `cost_model.py` lo include nella voce `idle`.

## 8. Da ricontrollare su `merge-engine` (punto per punto)

Tutto ciò che segue è stato letto su `engine-cost`; `merge-engine` può averlo cambiato.

1. **Credenziali / store.** In `mongo-server/src/main.rs` cercare ancora `minioadmin`, `with_endpoint`, `with_region("us-east-1")`; la patch si aggancia al `match args.store.as_str()` e al blocco `Args`. `git apply --check` deve passare.
2. **`--region`, `--s3-max-rps`, `--s3-process-cap-usd`**: nessun conflitto con argomenti nuovi di `merge-engine`.
3. **Contatore.** `CountingStore` in `src/counting.rs`, `tenant_stats` in `mongo-server/src/cmd.rs` (campi `requests.billedPutTier/billedGetTier`; la patch aggiunge `s3Http`). `guard.py` e `test3.py` leggono quei nomi.
4. **`object_store` ancora `=0.14.2`** (`with_http_connector`, `ReqwestConnector`, `HttpService` esistono in quella versione). Se cambia la versione, `s3guard.rs` può non compilare.
5. **`server.sh`**: usa `SERVER_BIN`, `--holder srv-<porta>`, `cd "$(dirname "$0")/.."`, pidfile in `mongo-tests/out/`. Il bundle ricrea quella struttura sotto `/opt/s3test`. Se cambia (argomenti, nome del binario), cambiare `engine-wrap.sh`.
6. **Suite**: `run_py.py` (`--tests`, `--out`) e `node/run_node.js` (`--tests=`, `--out=`), e che `restart_server` richiami `server.sh` (così `engine-wrap.sh` si applica anche ai riavvii). Il sottoinsieme è fatto da `make_subset.py` (≤5 test per gruppo, solo `core`, più i test `idle`). Su `engine-cost` sono 109 test in 22 gruppi.
7. **Tenant**: prefisso `tenants/<db>/`, comando `tenantAdmin {close:true}`, `tenantStats` campi `openReaders/openWriters/leasesHeld`. `test3.py` li usa.
8. **Default che cambiano il costo**: lease TTL 30 s (rinnovo ogni 10 s per database aperto), `--process-lease` spento, `--idle-secs`, WAL GC / SST GC. Se `merge-engine` cambia i default, i numeri del test 3 (richieste a riposo e con database aperti) cambiano.
9. **`Cargo.lock`**: la patch modifica il lockfile di `engine-cost`; su `merge-engine` va rigenerata (`cargo build --locked` fallisce se non combacia).
10. **`results-merge.md`**: il formato non lo conosco. Il workflow non lo interpreta: richiede che tu l'abbia letto e che `run.json` contenga l'hash esatto del file (`git rev-parse origin/main:results-merge.md`).
11. Quando `merge-engine` sarà pubblicato: copiare `s3-test/`, `results-s3.md` sopra di esso (sono tutti file nuovi), rigenerare la patch.

## 9. Come si usa, quando scrivi "AVVIA AWS"

1. Mostro stima dei costi e elenco dei trigger (qui sotto) e **non lancio niente** finché non ti sono stati mostrati.
2. Copio `s3-test/workflows/s3-test.yml.txt` in `.github/workflows/s3-test.yml`, creo `s3-test` da `s3-test-draft`, push con `run.json` → parte il Test 1.
3. Test 2 e 3: solo se `results-merge.md` (su `main` o `merge-engine`) dice che TUTTE le suite e le prove sono verdi, e solo dopo che ho messo in `run.json` l'hash del file. Altrimenti mi fermo dopo il Test 1.

**Trigger:** uno solo, `push` su `s3-test` con `paths: s3-test/run.json`.

**Stima di costo lordo (prima dei chiarimenti sulla fattura):**

- Test 1: massimo calcolato **≈ $0,16** (istanza 90 min = $0,1455 + EBS $0,002 + al massimo ~1.600 richieste PUT-tier e ~1.900 GET-tier ≈ $0,01). Tipico (~15–20 min): $0,03–0,05.
- Test 2 e 3: **stima, non misura:** da $1 a $3 lordi in tutto (richieste: decine di migliaia di PUT-tier per la creazione di 1.000–2.000 database e per i rinnovi di lease dei database aperti; più ≤ $0,15 di istanza per run). Tetto duro per run: il guard ferma a $4,5.
- Tutto questo è lordo, prima dei crediti, senza IVA e senza IPv4 pubblico.


## 10. Risultati del Test 1 su AWS vero (2026-10-27 / 28)

Dati completi sul branch `s3-test-results`, cartelle `s3-test/runs/<run_id>/`.

| Run | Esito | Costo lordo | Note |
|---|---|---|---|
| 40568199783 | fallito prima di AWS | 0,0002 $ | build rotta (`curl` in conflitto con `curl-minimal` in Amazon Linux 2023). Corretto. |
| 40572465637 | fallito all'accesso AWS | ~0 | partito mentre la trust policy veniva modificata. |
| 40573793637 | fallito all'accesso AWS | ~0 | trust policy con `sub` sbagliato (vedi sotto). |
| 40576555086 | quasi tutto ok | 0,0103 $ | unico errore: `s3:PutBucketTagging` non permesso (bucket creato senza tag). |
| 40612325934 | **tutti i passi verdi** | **0,0101 $** | dopo aver concesso `s3:PutBucketTagging`. |

Registro costi: totale 0,0211 $ (tetto 24 $).

**Verificato su AWS vero (run 40612325934):** bucket creato con tag, oggetto scritto/letto/elencato/cancellato; AMI Amazon Linux 2023 trovata con `ec2:DescribeImages`; una sola c6i.large lanciata col profilo `fenvix-test-instance`; SSM l'ha vista in pochi secondi; comando SSM ha scritto un file su S3 e il file è stato scaricato e confrontato; l'istanza ha parlato ad AWS come `assumed-role/fenvix-test-instance` (nessuna chiave AWS sull'istanza, nessun file di credenziali); `s3probe` (1.000 PUT + 1.000 GET): contatore del motore **esatto** (1.000/1.000), conteggio HTTP identico (1.000/1.000), **zero retry** visti da S3 in questa prova; istanza terminata dopo 149 s, bucket eliminato, nessuna istanza rimasta; il controllo di spesa sull'istanza ha girato (0,0094 $ al momento dell'ultima lettura).

**Cose emerse, da non dimenticare:**
1. **`sub` OIDC personalizzato.** Il repo manda `repo:Mircoguidetti@17613795/test5@1433234171:ref:refs/heads/<branch>` (con gli ID numerici), non `repo:Mircoguidetti/test5:...`. La trust policy di `fenvix-gha-tests` usa ora il valore con gli ID. Il passo diagnostico del workflow (`stop_after_claims` in `run.json`) lo stampa senza usare AWS.
2. **Durata massima della sessione del ruolo: 1 ora.** Il passo da 14.400 s fallisce sempre (circa 1,5 minuti sprecati, poi il ripiego a 3.600 s funziona). Si evita alzando la durata massima del ruolo o togliendo il primo passo.
3. **`iam:PassRole` e SSM:** avevo previsto che mancassero. Non mancavano: la RunInstances reale e SSM hanno funzionato.
4. **Limitatore di richieste:** nel primo run ha frenato 1.575 richieste su 2.000 (burst di 32 in parallelo contro 300 al secondo): 2.000 richieste in 5,7 s. Per i test 2 e 3 le latenze misurate vanno lette tenendo conto del campo `throttled`.
5. **Credito da 20 $** "Explore AWS: Launch an instance using EC2" comparso il 27/10, probabilmente per il primo lancio di un'istanza (deduzione, non confermata). I servizi coperti non sono stati verificati.
6. **Non ancora verificato:** cosa è finito davvero sulla carta (serve Cost Explorer dopo ~24 ore); se `DeleteObjects` è gratuito (non usato in questa prova); costo dell'IPv4 pubblico.

**Non eseguiti:** Test 2 e Test 3. Richiedono `results-merge.md` (su `main` o `merge-engine`) che dica che tutte le suite sono verdi; il file non esiste ancora e `merge-engine` non è pubblicato.


## 11. Credenziali dei test lunghi e prova a secco della pulizia (2026-10-28)

**Modifiche al workflow (in `s3-test-draft` e `s3-test`):**
- Un solo login OIDC da 1 ora (il passo da 14.400 s è tolto: la durata massima della sessione del ruolo è 1 ora).
- `scripts/aws_refresh.sh`: nuovo token OIDC + assume-role, ogni 40 minuti, dentro `orchestrate.sh` e `watchdog.sh`. Se il rinnovo fallisce due volte, `orchestrate.sh` esce subito (codice 3) e il workflow passa alla pulizia; il watchdog termina l'istanza con le credenziali che ha ancora e finisce. Nessun tentativo all'infinito.
- Prima della pulizia il workflow rifà il login (passo "AWS credentials for cleanup").
- Timeout del job: 115 minuti. L'attesa dei test sull'istanza è limitata a 95 minuti dal lancio; ogni 30 secondi controlla lo stato dell'istanza e, se risulta terminata o spenta, smette di aspettare e va alla pulizia. I dati dell'istanza si scaricano una sola volta (anche in caso di uscita anticipata).
- `run.json` accetta `"mode": "cleanup_dryrun"` (con `wait_minutes`): nessun test, nessuna istanza.

**Prova a secco, run 40613989323:** **PASS.** Bucket creato con tag e un oggetto; 65 minuti senza alcuna chiamata AWS; poi pulizia con login nuovo: bucket eliminato, nessuna istanza col tag. Costo lordo 0,000242 $ (registro totale 0,0214 $).
- Limite della prova: le vecchie credenziali sono state rifiutate con `400 Bad Request` su `HeadBucket`, che non nomina la scadenza (una HEAD non ha corpo). Che la sessione sia scaduta è molto probabile (65 minuti contro 60) ma non dimostrato da un messaggio esplicito. Ciò che la prova dimostra con certezza è che il percorso "login nuovo + pulizia" elimina il bucket dopo più di un'ora.
- Non provato: il rinnovo dentro `orchestrate.sh`/`watchdog.sh` su AWS vero (provato solo in locale con comandi finti: due tentativi poi stop, attesa interrotta quando l'istanza risulta terminata). Il primo test lungo (Test 2 o 3) sarà la prova reale; il token OIDC del job potrebbe non essere richiedibile dopo molti minuti: da guardare nel log.
- Test 2 e 3: non eseguiti, in attesa del via di Mirco (`merge-engine` non è ancora finale).
