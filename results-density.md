# results-density: quanti database attivi regge un motore, e quante richieste S3 fanno

Branch `density-test`, creato da `origin/network-limits` (`628273b`). Push solo su `density-test`: nessun merge, nessuna PR, nessuna altra branch toccata. **Il motore non è stato modificato.** Sono stati aggiunti solo script (`tools/density/`) e risultati (`results-density/`, questo file).
Nessuna attività AWS, nessun segreto reale, nessun carico verso internet: tutto in locale contro MinIO.

* **Motore:** commit `1c9fee7`, compilato con `cargo build --release --workspace` (cargo 1.97.0) in un worktree separato. Binario `target/release/mongo-server`, **SHA-256 `b5e976a01dd5c414a6d057734d757784a91180396b5322a5217151e83c543dfe`**. Tra `1c9fee7` e questa branch non cambia nulla del motore: `git diff 1c9fee7..HEAD -- mongo-server src Cargo.toml Cargo.lock loadgen` è vuoto. (Il binario di `results-network-limits.md`, `8b3d7b71…`, era compilato con `-p mongo-server` in un altro ambiente: stesso sorgente, file diverso.)
* **Flag (tutti i run, salvo dove detto):** `--auth --tls-mode requireTLS --tls-certificate-key-file <certificato di prova autofirmato> --store s3 --bucket density --endpoint http://127.0.0.1:9000 --region us-east-1 --idle-secs 60 --retention-window-secs 604800 --wal-gc-mode bulk`, più `--listen`, `--admin-listen`, `--holder`. Tutti gli altri flag ai valori predefiniti (in particolare: lease per database con TTL 30 s, `--wal-gc-secs 20`, `--writer-poll-secs 30`, `--retention-interval-secs 600`, `--max-concurrent-wakes 0`). Riga di comando completa in ogni `results-density/<run>/raw/meta.txt`.
* **MinIO:** compilato dai sorgenti (`go install github.com/minio/minio@latest`, moduli da proxy.golang.org, versione `v0.0.0-20260212201848-7aac2a2c5b7c`), un disco, dati su disco locale. **pymongo 4.19.0** da PyPI (client asincrono `AsyncMongoClient`), OpenSSL 3.0.13, Python 3.13.

## 0. Macchina e metodo

| | |
|---|---|
| CPU | 4 vCPU Intel Xeon @ 2,10 GHz (KVM), 1 thread per core |
| RAM | 15,7 GiB, nessuno swap |
| Disco | disco virtuale locale (MinIO scrive qui) |
| Ripartizione | **motore fissato sulle CPU 0-1** (`taskset -c 0,1`: 2 vCPU come un t3.medium); MinIO, client di carico e campionatore sulle CPU 2-3 |
| Limite file aperti | 20000 (soft e hard) |

**Database di prova.** 2000 database `c0000`…`c1999`, creati con `POST /v1/databases` sulla porta admin (come farebbe il control plane), ognuno con un proprio utente (`readWrite` solo sul proprio database), **200 documenti di circa 1 KB in 2 collezioni** (`items` 100, `events` 100) e **1 indice** (`items.k`). Script: `tools/density/setup_dbs.py` (2000 database in circa 2 minuti).

**Client.** Un `AsyncMongoClient` per database, con TLS e SCRAM-SHA-256 con le credenziali di quel database (come clienti diversi), `maxPoolSize=2`, monitoraggio in modalità `poll` ogni 30 s: **2 connessioni TLS per database**. Connessione, TLS e autenticazione avvengono prima della finestra misurata. Mix: **90% letture** (50% `find_one` per `_id`, 40% `find` sull'indice `k` con `limit 10`), **10% scritture** (5% `update_one` `$inc`, 5% `insert_one` di 1 KB). Ogni richiesta è registrata (istante, database, tipo, latenza, esito, ritardo rispetto all'istante pianificato). Script: `tools/density/load.py`.

**Ogni run parte da un motore nuovo** (tutti i database chiusi su MinIO): il primo minuto contiene quindi le aperture a freddo. Le tabelle riportano la finestra intera (`whole`) e quella da 60 s in poi (`steady`).

**Come è misurato ogni numero:**

| grandezza | come |
|---|---|
| RSS | `/proc/<pid>/statm` del motore, ogni 10 s |
| CPU | `utime+stime` da `/proc/<pid>/stat`; "% di 1 core" = secondi CPU / secondi di finestra × 100 (il motore ha 2 core: 200% è il massimo) |
| CPU per richiesta | secondi CPU del motore nella finestra / richieste client nella finestra (include il lavoro di fondo: lease, pulizie, chiusure) |
| latenza | misurata dal client (pymongo, TLS) per singola richiesta: include la rete locale e lo scheduling Python; p50/p95/p99 per tipo |
| database aperti | dal log del motore: righe `LEASE acquired` (apertura, lettore o scrittore) e `LEASE release`/`lost` (chiusura). Vedi il punto sotto |
| richieste S3 | **metriche di MinIO** (`/minio/metrics/v3/api/requests`, `minio_api_requests_total{type="s3"}` per API), lette ogni 10 s: sono le richieste che il server S3 riceve, cioè quelle che AWS fatturerebbe |
| richieste S3 per DB al mese | `richieste nella finestra / secondi della finestra × 2 592 000 / N` (mese di 30 giorni) |
| costo richieste | prezzi S3 Standard US East **come indicati nella richiesta** (non verificati qui): PUT/COPY/POST/LIST 5 $ per milione, GET/HEAD e altre 0,40 $ per milione, DELETE gratis. `DeleteObjects` (cancellazione multipla, un POST) è contato come DELETE (gratis); rifatturato come POST la differenza è sotto 0,01 $/DB/mese in tutti i run |
| costo storage | `0,023 $/GB-mese × byte del database su MinIO` (listing diretto del bucket) |

**Contatore del motore (`tenantStats`): non usabile con `--auth`.** Non è sulla porta admin ma è un comando della porta dati, e richiede il privilegio `anyAction`: **root riceve `Unauthorized`**, e il ruolo `__system` non si può assegnare (`createUser` con `__system` rifiutato anche a root). Quindi nei run degli scenari le richieste S3 vengono **solo da MinIO** e il numero di database aperti **dal log LEASE**. Limiti di questa scelta: (a) MinIO conta tutto ciò che arriva al server, senza dire quale parte del motore l'ha fatto (durante i run solo il motore usa MinIO; le letture delle metriche non sono richieste S3); (b) il log LEASE conta le aperture con lease, non distingue lettori e scrittori se non con le righe `writer_open`/`writer_close` (riportate come `open_writers`). Per attribuire le richieste alle loro cause ho fatto **un run diagnostico separato senza `--auth`** (sezione 6), dove `tenantStats` funziona.

## 1. Scenario 1: densità con traffico continuo (1 richiesta/s per database, 10 minuti per N)

`results-density/s1_steady_n{50,100,200,500}/`

| N | finestra | richieste/s | DB aperti | RSS MB media / max | CPU % di 1 core | CPU ms / richiesta | letture p50/p95/p99 ms | scritture p50/p95/p99 ms | errori |
|---|---|---|---|---|---|---|---|---|---|
| 50 | 60-600 s | 50,2 | 50 | 61,8 / 67,7 | 8,2 | 1,62 | 2,1 / 3,5 / 6,4 | 6,2 / 9,7 / 16,1 | 0 |
| 100 | 60-600 s | 99,7 | 100 | 96,3 / 106,9 | 15,9 | 1,60 | 2,1 / 3,6 / 6,6 | 6,2 / 11,3 / 23,8 | 0 |
| 200 | 60-600 s | 199,5 | 200 | 167,7 / 188,8 | 29,0 | 1,46 | 1,9 / 3,6 / 8,0 | 5,9 / 16,0 / 37,8 | 0 |
| 500 | 60-600 s | 499,4 | 500 | 411,8 / 465,8 | 64,5 | 1,29 | 1,8 / 8,0 / 26,9 | 7,7 / 62,8 / 139,6 | 0 |
| 500 | 0-600 s (con le 500 aperture a freddo) | 499,5 | 492 | 400,6 / 465,8 | 66,0 | 1,32 | 1,9 / 12,6 / **248** | 8,0 / 85,0 / **1771** | 0 |

Latenze per singolo tipo (`read_id`, `read_idx`, `update`, `insert`) in `latency.csv` di ogni run; serie ogni 10 s in `timeseries.csv`. Nessun client in ritardo di oltre 1 s rispetto al piano (colonna `client_late_gt1s`): i client hanno tenuto il ritmo.

**Memoria.** Separata in tre parti, dai campioni di ogni run (RSS prima dei client, dopo che i client si sono connessi ma prima di aprire database, 20 s dopo l'inizio, fine):

| N | RSS a vuoto | connessioni TLS | memoria per connessione | memoria per DB all'apertura | crescita per DB in 9,5 min di carico |
|---|---|---|---|---|---|
| 50 | 20,5 MB | 105 | 218 KB | 0,16 MB | +0,34 MB |
| 100 | 20,3 MB | 211 | 154 KB | 0,20 MB | +0,35 MB |
| 200 | 20,2 MB | 406 | 132 KB | 0,23 MB | +0,34 MB |
| 500 | 20,7 MB | 1062 | 152 KB | 0,26 MB | +0,32 MB |

Formula: per connessione = (RSS con client connessi − RSS a vuoto) / connessioni (dal numero di file aperti); per DB all'apertura = (RSS a t=20 s − RSS con client connessi) / N; crescita = (RSS a fine run − RSS a t=20 s) / N. **La RSS non si stabilizza durante i 10 minuti**: cresce di circa 0,035 MB per database al minuto (vedi sezione 4.2 per la causa).

**Richieste S3 (MinIO), per database al minuto, finestra intera:**

| N | GET | PUT | LIST | HEAD | DeleteObjects | S3 per richiesta client | classe PUT / DB / mese | classe GET / DB / mese | **$ richieste / DB / mese** |
|---|---|---|---|---|---|---|---|---|---|
| 50 | 53,8 | 13,3 | 11,0 | 1,0 | 0,1 | 1,32 | 1 048 032 | 2 368 915 | **6,19** |
| 100 | 55,9 | 13,2 | 10,7 | 1,0 | 0,1 | 1,35 | 1 031 011 | 2 457 951 | **6,14** |
| 200 | 57,3 | 13,2 | 10,5 | 1,0 | 0,1 | 1,37 | 1 019 369 | 2 519 835 | **6,10** |
| 500 | 56,7 | 13,2 | 10,4 | 1,0 | 0,1 | 1,36 | 1 019 001 | 2 494 419 | **6,09** |

Il costo per database **non dipende da N** (ogni database paga per sé). Da dove vengono (sezione 6): PUT ≈ 6 scritture del WAL (una per scrittura del client) + 6 rinnovi del lease (ogni 10 s) + 1 contatori d'uso; GET e LIST quasi tutti dalla **pulizia del WAL** che gira ogni 20 s su ogni database scritto; le letture dei client sono servite quasi tutte dalla memoria.

**Storage:** 0,4-1,5 MB per database su MinIO (listing del bucket a fine lavoro: `c1999` 0,44 MB, `c0499` 0,59 MB, `c0000` 1,48 MB dopo tutti i run) → **circa 0,00003 $/DB/mese**: trascurabile rispetto alle richieste.

## 2. Scenario 2: traffico a raffiche (N=200, 10% attivo per minuto, a rotazione)

`results-density/s2_burst_n200/`. Nel minuto m sono attivi i database con indice ≡ m (mod 10), 1 richiesta/s ciascuno.

| finestra | richieste/s | DB aperti media (max) | RSS MB media / max | CPU % di 1 core | CPU ms / richiesta | letture p50/p95/p99 ms | scritture p50/p95/p99 ms | aperture | errori |
|---|---|---|---|---|---|---|---|---|---|
| 0-600 s | 20,0 | 35,3 (40) | 99,2 / 103,0 | 8,5 | 4,25 | 3,5 / 9,5 / 30,1 | 7,7 / 34,1 / 60,2 | 200 | 0 |

| GET / DB / min | PUT / DB / min | LIST / DB / min | HEAD / DB / min | S3 per richiesta client | classe PUT / DB / mese | classe GET / DB / mese | **$ / DB / mese** |
|---|---|---|---|---|---|---|---|
| 24,2 | 3,7 | 3,1 | 1,0 | 5,64 | 293 134 | 1 089 396 | **1,90** |

Con `--idle-secs 60` ogni database resta aperto un minuto dopo la sua raffica: sono aperti in media 35-40 database, non 20. Ogni minuto 20 aperture a freddo e circa 20 chiusure. La CPU per richiesta sale da 1,3-1,6 a 4,3 ms (le aperture costano CPU). Il costo per database si dimezza rispetto allo scenario 1 perché ogni database è attivo solo un minuto su dieci, ma **ogni richiesta client costa 5,6 richieste S3** (contro 1,3).

## 3. Scenario 3: "thrash" (N=100, una richiesta ogni 70 s) contro una ogni 45 s

`results-density/s3_period70_n100/`, `results-density/s3_period45_n100/`, 15 minuti ciascuno.

| periodo | richieste client | aperture | chiusure | DB aperti media | CPU ms / richiesta | letture p50/p95/p99 ms | scritture p50/p95/p99 ms | **S3 per richiesta client** | $ / DB / mese |
|---|---|---|---|---|---|---|---|---|---|
| **70 s** (si chiude e si riapre) | 1288 | 1288 | 1200 | 83 | 24,3 | 25,4 / 37,9 / 53,8 | 44,5 / 66,8 / 80,1 | **43,6** | 2,68 |
| **45 s** (resta aperto) | 2000 | 100 | 0 | 97 (100 dopo il primo minuto) | 14,9 | 4,6 / 20,2 / 28,1 (steady: 4,6 / 10,8 / 15,3) | 10,5 / 38,3 / 42,1 | **17,8** | 2,09 |

* **A 70 s ogni richiesta è un'apertura a freddo** (1288 aperture per 1288 richieste). Un ciclo apertura + chiusura costa in media **43,6 richieste S3**: 28,8 GET, 9,1 PUT, 2,9 LIST, 1,9 HEAD, 0,8 DeleteObjects, cioè circa **0,00007 $** per ciclo. La latenza mediana sale da 4,6 a 25 ms (letture) e da 10 a 44 ms (scritture) **con MinIO locale**: su S3 reale ogni passo dell'apertura costa decine di millisecondi, quindi l'apertura sarà molto più lenta (non misurato).
* **A 45 s il database resta aperto e costa comunque**: 17,8 richieste S3 per ogni richiesta client, perché un database aperto genera richieste anche senza traffico (rinnovi del lease, pulizie). Per database: 14,6 GET + 7,1 PUT + 1,3 LIST al minuto ≈ 1 milione di richieste al mese.
* **Confronto per mese:** stare aperto con poco traffico costa circa 0,00005 $/min (2,09 $/mese); un ciclo di riapertura costa 0,00007 $, cioè **circa 1,5 minuti di apertura**. Con `--idle-secs 60` un database che riceve una richiesta ogni 60-90 s paga quasi lo stesso in entrambi i modi, ma con latenza molto peggiore se si chiude.
* La CPU per richiesta è alta in entrambi i casi (24 e 15 ms) perché con così poche richieste domina il lavoro di fondo.

## 4. Scenario 4: limite di memoria

### 4.1 Rampa fino a 2000 database aperti

`results-density/s4_ramp/`. Motore nuovo; ogni 180 s si aggiungono 250 database, ciascuno tenuto aperto da una richiesta ogni 20 s (meno di `--idle-secs`, quindi non si chiudono); stop a RSS > 3 GB o a 2000 database; poi 180 s fermi a 2000.

| DB aperti | RSS MB | CPU % di 1 core | file aperti |
|---|---|---|---|
| 250 | 131 | 9 | 536 |
| 500 | 248 | 17 | 1046 |
| 750 | 372 | 23 | 1557 |
| 1000 | 508 | 37 | 2068 |
| 1250 | 648 | 38 | 2566 |
| 1500 | 800 | 43 | 3079 |
| 1750 | 953 | 54 | 3584 |
| 2000 | 1118 | 61 | 4085 |
| 2000, dopo 180 s fermi | 1183 | 70 | - |

* **Il limite di 3 GB non è stato raggiunto**: a 2000 database aperti (100 richieste/s in totale) il motore usa **1,1-1,2 GB**, circa **0,55 MB per database** con 2 connessioni ciascuno. Nessun errore client, nessun database chiuso (0 righe `LEASE release` di database dei clienti fino allo stop), nessun rifiuto; il motore non è stato ucciso. Quindi il "punto di rottura" richiesto **non è stato osservato** con questo carico.
* Quello che peggiora: la CPU di fondo cresce con i database aperti (61-70% di un core a 2000 con sole 100 richieste/s: circa **0,03% di un core per database aperto**), e la p99 delle letture sale (da 10-20 ms a 50-150 ms nei campioni finali). Latenze intere: letture p50/p95/p99 4,0 / 63 / 167 ms, scritture 21,6 / 183 / 425 ms.
* S3 negli ultimi 180 s (2000 aperti, una richiesta ogni 20 s): per database al minuto 15,9 GET, 7,3 PUT, 2,2 LIST → **2,33 $/DB/mese**.
* **File aperti: circa 2 per database** (le connessioni dei client). Qui il limite era 20000; con il limite predefinito di molti sistemi (1024) il motore non accetterebbe più connessioni oltre ~500 database con 2 connessioni ciascuno (non provato: dedotto dal conteggio).

### 4.2 La memoria cresce con le scritture e non si stabilizza

`results-density/s4_write_growth/`. Aggiunto perché nello scenario 1 la RSS cresceva per tutto il run. 20 database, **10 insert/s ciascuno** (1 KB), 15 minuti, stesso motore e flag.

| t (s) | 50 | 170 | 290 | 410 | 530 | 650 | 770 | 890 |
|---|---|---|---|---|---|---|---|---|
| RSS MB | 85 | 199 | 310 | 469 | 515 | 541 | 570 | 688 |

* La RSS cresce di circa **4,6 KB per scrittura da 1 KB** (≈ 0,9 MB/s per 200 scritture/s) e in 15 minuti (circa 9000 scritture per database) **non si stabilizza**: 20 database → 688 MB, cioè **34 MB per database**.
* Causa probabile (dal codice, non verificata con un profiler): ogni database scritto tiene i dati scritti nella sua memtable finché non la scarica in un SST, cosa che slatedb fa solo a **64 MiB** (`l0_sst_size_bytes`) o dopo **4096 flush del WAL** (`max_wal_flushes_before_l0_flush`); il motore non cambia questi valori. In questo test la soglia di 4096 è stata superata (a ~410 s) e la crescita ha rallentato ma non si è fermata.
* **Conseguenza per lo scenario 1** (stima, non misura): a 6 scritture/min un database arriva a 4096 scritture dopo circa 11 ore; con ~4,6 KB trattenuti per scrittura sarebbero ~19 MB per database, cioè **~3,8 GB per 200 database**, oltre la memoria di un t3.medium. Serve un run lungo (≥ 12 ore) per sapere se la crescita si ferma davvero.
* Scritture: p50/p95/p99 6,0 / 59,6 / 122,8 ms; 151 richieste su 179 450 partite con più di 1 s di ritardo dal lato client.

### 4.3 La memoria cresce con i dati letti, e non torna al sistema

`results-density/s4_bigcache/`. 8 database da 40 MB (40 000 documenti da 1 KB), ciascuno letto per intero due volte.

* Ogni database aperto tiene in memoria circa **1,4 volte i dati letti**: RSS da 28 MB a 474 MB dopo il primo giro, ~500 MB dopo il secondo (non cresce oltre: i dati sono già in cache). Dal codice: ogni database ha **una propria cache dei blocchi** (default slatedb **512 MB per database**, nessun limite complessivo; il motore non la imposta).
* Dopo che i database si sono chiusi per inattività la RSS **resta 496 MB**. Riaprendo e rileggendo gli stessi database per 4 cicli (`bigcache_reopen.json`) la RSS resta 485-519 MB: la memoria **viene riusata, non persa**, ma **non torna al sistema operativo**. Il server va dimensionato sul picco, non sulla media.

## 5. Punto di rottura

**Non raggiunto.** Con database piccoli (200 KB), 2 connessioni ciascuno e traffico leggero, il motore regge 2000 database aperti in 1,2 GB e il 70% di un core; 500 database a 1 richiesta/s in 0,47 GB e il 65% di un core (sui 2 core: c'è margine). Con N=500 la p99 delle scritture sale a 140 ms a regime e a **1,8 s nel primo minuto**, quando 500 database si aprono insieme dopo un riavvio (`--max-concurrent-wakes` è 0, nessun limite).

I limiti reali per un server piccolo vengono da tre fenomeni **che i 10 minuti non bastano a mostrare per intero**: la memoria che cresce con le scritture (4.2), la memoria che cresce con i dati letti (4.3) e il costo S3 che cresce con la storia del database (6.2).

## 6. Da dove vengono le richieste S3

### 6.1 Diagnostica (motore SENZA `--auth`, solo per leggere `tenantStats`)

`results-density/diag/diag_categories.json`. Stessi flag tranne `--auth`; client con TLS e SCRAM; 50 database (`c0000`…`c0049`), 5 minuti per variante, contatori tra t=60 s e la fine (240 s).

| variante | richieste client | PUT | GET | LIST | PUT per categoria | GET per categoria | passate pulizia WAL |
|---|---|---|---|---|---|---|---|
| mix (90/10, 1/s) | 12 098 | 2 620 | 21 901 | 2 096 | wal 1194, lease 1200, other 201, manifest 25 | manifest 17 595, other 4 128, sst 178 | 547 (20 032 GET, 1 641 LIST) |
| solo letture (1/s) | 12 128 | 1 401 | 130 | 9 | lease 1200, other 201 | sst 130 | 0 |
| solo update (1/s) | 12 128 | 13 552 | 23 041 | 2 107 | wal 12 128, lease 1200, other 200 | manifest 18 537, other 4 148, sst 356 | 550 (20 988 GET, 1 650 LIST) |
| aperto quasi fermo (1 lettura ogni 50 s) | 240 | 1 465 | 3 450 | 243 | lease 1183, other 200, wal 49 | manifest 2 244, sst 624, other 582 | 54 |

Cosa dice:
1. **Ogni database aperto, anche solo in lettura, fa 6 PUT al minuto per il lease** (rinnovo ogni 10 s) **+ 1 PUT al minuto** ("other": contatori d'uso, `--usage-flush-secs 60`). Sono ~302 000 PUT al mese = **1,51 $/DB/mese** solo per restare aperto.
2. **Ogni scrittura del client è un PUT del WAL** (12 128 update → 12 128 PUT): 5 $ per milione di scritture.
3. **Le letture dei client non vanno quasi mai su S3** (130 GET per 12 128 letture: la cache).
4. **Il 90% dei GET e quasi tutti i LIST vengono dalla pulizia del WAL dei database scritti**: una passata ogni 20 s per database scritto, ~37 GET (manifest) e 3 LIST per passata, **0 oggetti cancellati**.

### 6.2 `--wal-gc-mode bulk` è ignorato con la retention, e il costo cresce con la storia

Dal codice (`src/lifecycle.rs`, `gc_tenant_wal`): **se il database ha una finestra di retention (> 0), la pulizia usa sempre il passaggio di slatedb** (elenca tutta la cartella WAL, legge i manifest) **qualunque sia `--wal-gc-mode`**, perché il WAL va tenuto per tutta la finestra (qui 7 giorni) e il passaggio "bulk" non guarda l'età degli oggetti. Quindi con questi flag `--wal-gc-mode bulk` non ha effetto, e la pulizia non cancella nulla per 7 giorni ma paga ogni 20 s.

Misura (`results-density/extra_wal_growth/wal_growth.json`): due database con lo stesso traffico (1 update/s per 180 s, uno alla volta):

| database | oggetti WAL | manifest | GET / min | LIST / min | PUT / min |
|---|---|---|---|---|---|
| `walnew` (nuovo) | 3 | 5 | 65,7 | **14,0** | 69,7 |
| `walold` (dopo 20 000 insert) | 5 402 | 6 | 70,7 | **43,3** | 67,7 |

I LIST crescono con il numero di oggetti WAL tenuti (una pagina di LIST ogni 1000 oggetti, a ogni passata). Anche i GET crescono con la storia: gli stessi database `c0000`…`c0049` facevano 54 GET/DB/min nello scenario 1 (primo run) e 110 nella diagnostica, dopo altri ~8 cicli di apertura (più manifest: `c0000` ne ha 28).

**Stima (non misurata):** a 6 scritture/min un database tiene ~60 000 oggetti WAL in 7 giorni; con ~3 passate al minuto e ~60 pagine di LIST per passata sarebbero **~7-8 milioni di LIST al mese, circa 35-40 $/DB/mese**, contro i 6 $ misurati nei primi 10 minuti. Il costo per database attivo crescerebbe quindi per tutta la prima settimana.

## 7. Stima per un t3.medium (2 vCPU, 4 GB) — STIMA, non misura

Ipotesi dichiarate:
* una vCPU del t3.medium vale una vCPU di questa macchina (Xeon 2,1 GHz); **non verificato**;
* il t3.medium ha una **prestazione di base del 20% per vCPU** (0,4 vCPU in continuo) e oltre consuma crediti (o costa di più in modalità unlimited): per traffico continuo conta la base;
* 1 GB lasciato a sistema operativo e page cache: **~3 GB per il motore**;
* database piccoli come questi (200 KB), 2 connessioni ciascuno, traffico come lo scenario 1 (1 richiesta/s, 10% scritture), un solo motore senza altri processi.

| vincolo | conto | database attivi |
|---|---|---|
| CPU alla base (40% di un core) | misurato 29% di un core a N=200 e 65% a N=500 (≈ 0,15% di core per database) | **~260** |
| CPU in burst (200%, a crediti) | lineare dai 500 misurati | ~1000-1300, ma la p99 sale già a 500 |
| memoria nelle prime ore | 20 MB + 0,55 MB per database + crescita 0,035 MB/min per database | 200 DB: ~150 MB all'inizio, ~600 MB dopo 1 ora |
| memoria a regime (scritture) | se la crescita si ferma a 4096 scritture (~19 MB per DB, sezione 4.2) | **~150** (non verificato) |
| memoria con dati più grandi | 1,4 × dati letti per DB aperto (sezione 4.3) | es. 20 MB letti per DB → ~100 |
| file aperti | ~2 per database | richiede `LimitNOFILE` alto |

**Stima prudente: 100-150 database attivi con traffico continuo su un t3.medium**; 200 solo se un run lungo dimostra che la memoria si stabilizza e se i dati letti per database restano piccoli. Con traffico a raffiche (scenario 2) gli aperti sono ~20% del totale, quindi 200 clienti con quel profilo occuperebbero ~40 database aperti: ci stanno.

**Il vincolo più forte non è il server ma S3:** 200 database con il traffico dello scenario 1 costano **~1 200 $/mese** di richieste (6,1 $ × 200) nei primi minuti, e la stima della sezione 6.2 dice che crescerebbe nella prima settimana; un t3.medium costa qualche decina di dollari al mese.

## 8. Problemi trovati nel motore (nessuno corretto: il brief vieta di modificarlo)

1. **`tenantStats` inaccessibile con `--auth`**: richiede `anyAction`, root è rifiutato e `__system` non è assegnabile. In produzione non si possono leggere i contatori S3 del motore né il numero di database aperti (il log LEASE è l'unico modo).
2. **`--wal-gc-mode bulk` ignorato quando c'è una retention**: si ricade nel passaggio slatedb ogni 20 s per database scritto, che non cancella nulla durante la finestra ma fa ~37 GET + 3 LIST (più 1 LIST ogni 1000 oggetti WAL) a ogni passata. È la prima voce di costo nello scenario 1 e cresce con la storia.
3. **Lease per database: 6 PUT/min per ogni database aperto** (1,30 $/mese anche senza traffico). Esiste `--process-lease` (un lease per processo) che li elimina, ma non era tra i flag richiesti.
4. **La memoria cresce con le scritture e, nei 15 minuti misurati, non si ferma** (4,6 KB trattenuti per scrittura da 1 KB): rischio di esaurire la memoria con database sempre aperti e scritti.
5. **Una cache dei blocchi da 512 MB per ogni database**, senza limite complessivo: la memoria cresce con i dati letti da ciascun database aperto.
6. **La memoria non torna al sistema dopo la chiusura** dei database (viene riusata: niente perdita nei 4 cicli provati).
7. **Tempesta di aperture dopo un riavvio**: 500 database che si aprono insieme portano la p99 delle scritture a 1,8 s nel primo minuto; `--max-concurrent-wakes` è 0 per default.
8. Minore: circa 1 HEAD al minuto per database nello scenario 1 (0,1-0,6 negli altri) che non compare nei contatori del motore della diagnostica: origine non identificata (forse il control layer).

## 9. Decisioni che servono a te

1. **Retention di 7 giorni per tutti?** È la causa principale del costo S3 dei database attivi (sezione 6.2). Opzioni: una finestra più corta di default, `--wal-gc-secs` molto più alto quando c'è una retention (es. 300 s: ~15 volte meno GET/LIST, ma più oggetti da elencare dopo), o una modifica al motore: una pulizia incrementale che rispetti l'età degli oggetti (oggi non esiste).
2. **`--process-lease`**: risparmia ~6 PUT/min per database aperto (~1,30 $/DB/mese). Vincolo: tutti i processi sullo stesso bucket devono usarlo.
3. **`--idle-secs`**: con questi prezzi una riapertura costa quanto ~1,5 minuti di apertura (sezione 3); 60 s è vicino al pareggio sul costo, ma chiudere peggiora molto la latenza (e su S3 reale ancora di più).
4. **Memoria per database**: serve una decisione su un limite alla memtable (`l0_sst_size_bytes` / `max_wal_flushes_before_l0_flush`) e alla cache dei blocchi (una cache condivisa con un tetto), che sono modifiche al motore; e serve un **run lungo (≥ 12 ore)** prima di promettere 200 database per t3.medium.
5. **t3 (burstable) o no**: con traffico continuo il t3.medium alla base dà 0,4 vCPU; un'istanza non burstable con 2 vCPU darebbe 5 volte la CPU sostenuta.
6. **`--max-concurrent-wakes`** e il limite di file aperti (`LimitNOFILE`) per la messa in produzione.

## 10. Cosa NON ho potuto misurare

* **S3 reale**: solo MinIO locale. Le latenze delle aperture a freddo (scenari 2 e 3) sono sottostimate; i conteggi delle richieste invece valgono anche per S3 (sono le stesse chiamate).
* **Run lunghi**: ogni scenario dura 10-15 minuti (rampa 27 minuti). Non misurati: se la memoria si stabilizza dopo ore, la crescita del costo S3 nei 7 giorni di retention (solo stimata), i punti di ripristino ogni 600 s nel lungo periodo.
* **Il punto di rottura per memoria**: 3 GB non sono stati raggiunti a 2000 database, quindi non ho visto se il motore rallenta, rifiuta, chiude database o viene ucciso.
* **Un t3.medium**: nessuna attività AWS; tutta la sezione 7 è una stima con ipotesi dichiarate (velocità della CPU, crediti).
* **Contatori interni con `--auth`**: `tenantStats` è rifiutato; la ripartizione delle richieste per causa viene da un run separato senza `--auth`.
* **Client su un'altra macchina**: client e MinIO stavano sulle stesse 2 CPU (2-3), separate da quelle del motore; il ritardo dei client è registrato (0 richieste in ritardo di oltre 1 s negli scenari 1-3).
* **Più connessioni per cliente**: ogni cliente aveva 2 connessioni; applicazioni reali con pool più grandi useranno ~150 KB in più per connessione.
* Dimensioni dei dati diverse da 200 KB per database, se non nel test della sezione 4.3.

## 11. File

| percorso | contenuto |
|---|---|
| `tools/density/` | `setup_dbs.py` (creazione), `load.py` (client), `sampler.py` (campioni ogni 10 s), `run_scenario.py`, `ramp.py`, `run_all.sh`, `run_rest.sh`, `run_extra.sh` (sequenze eseguite), `analyze.py` (CSV e riepiloghi), `tables.py` (tabelle), `diag.py`, `bigcache.py`, `bigcache_reopen.py`, `wal_growth.py`, `secret_scan.py`, `gen_cert.sh`, `engine.py`, `common.py` |
| `results-density/<run>/timeseries.csv` | un campione ogni 10 s: RSS, CPU, file aperti, database aperti e scrittori, aperture e chiusure, richieste MinIO per API al minuto, richieste client, errori, ritardi, p50/p95/p99 per tipo |
| `results-density/<run>/latency.csv` | per tipo di richiesta: conteggio, errori, p50/p95/p99/max, finestra intera e da 60 s |
| `results-density/<run>/summary.json` | i numeri del report con le formule |
| `results-density/<run>/raw/` | dati grezzi: campioni (`samples.jsonl`), ogni richiesta (`lat*.csv.gz`), log del motore (`engine.log.gz`), parametri (`meta.txt`) |
| `results-density/diag/`, `s4_bigcache/`, `extra_wal_growth/` | diagnostica e test aggiunti |
| `results-density/progress.log` | sequenza dei run con orari |
| `results-density/secret_scan.json` | scansione dei segreti (pulita) |

Scansione dei segreti (`tools/density/secret_scan.py`): tutti i file aggiunti rispetto a `origin/network-limits` (anche i `.gz`) cercati per chiavi cloud, chiavi private, certificati, token, credenziali negli URL; nei log del motore cercate le password di prova e il token admin usati dai run. **Nessuna corrispondenza.** Il certificato di prova e la sua chiave sono rimasti fuori dal repository e sono stati cancellati.
