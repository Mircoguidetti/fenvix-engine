# results-round3: meno richieste S3 per un database attivo (branch `round3`)

Branch `round3` da `main` (`c9a4beb`). PR verso `main`, non unita. `infra/` e `control-plane/` non esistono in questo repository e non sono stati toccati.

**Regola di questo report:** ogni numero è misurato. Dove una grandezza non è stata misurata c'è scritto **non misurato**. L'unica eccezione dichiarata è la sezione "Registro di scrittura condiviso", che è un calcolo su tracce simulate, come richiesto.

- **Prezzi usati per i $** (quelli della richiesta):
  - PUT/COPY/POST/LIST: 0,005 $ ogni 1000;
  - GET/HEAD: 0,0004 $ ogni 1000;
  - DELETE: gratis;
  - storage: 0,023 $/GB-mese;
  - mese di 30 giorni.
- **Dove sono misurate le richieste:**
  - su MinIO: contatori di MinIO (`minio_api_requests_total`), cioè quello che il server S3 riceve;
  - su S3 vero: contatori del motore (`tenantStats`), che non vedono i retry interni di `object_store`.

## Sintesi

**Costo delle richieste per database al mese, prima e dopo (50/50 letture/scritture).**

| scenario | prima, MinIO | dopo immediata, MinIO | dopo rilassata 5 s, MinIO | dopo log locale 30 s, MinIO | prima, S3 vero | dopo immediata, S3 vero | dopo rilassata, S3 vero | dopo log locale, S3 vero |
|---|---|---|---|---|---|---|---|---|
| 1 req/s | 11,44 | 7,05 | 2,79 | 1,19 | 11,65 | 8,49 | 4,17 | 2,67 |
| 10 req/s | 69,42 | 64,07 | 3,50 | 1,77 | 63,39 | 59,59 | 4,36 | 2,64 |
| 100 req/s | 630,51 | 619,47 | 3,67 | 1,95 | 139,86 | 111,65 | 4,54 | 2,79 |
| flotta 100 database × 1 scrittura/s | 17,45 | 12,94 | 2,72 | 1,02 | 17,35 | 12,79 | 2,69 | 1,03 |
| flotta mista (50 a raffiche, 20 con letture, 30 ferme) | 2,23 | 1,24 | 0,74 | 0,66 | non misurato | non misurato | non misurato | non misurato |

Come leggere la tabella:
- **Ogni cella è un solo run**, senza ripetizioni: differenze di pochi punti percentuali sono nel rumore.
- **Su S3 vero "prima" e "dopo" vengono da run diversi**, a ore di distanza: "prima" dal run 2 (binario di `main`), "dopo" dal run 5 (motore dopo la parte E, prima della revisione). Il run 6 rifà i passi che le correzioni della revisione toccano (sezione E).
- **A 100 req/s il -20% (139,86 → 111,65) mescola i due run.** Nel run 2, lo stesso run del "prima", la modalità immediata costava 134,12: -4%. Lo stesso passo, con motori che a 100 req/s si comportano allo stesso modo, ha dato 111,65 nel run 5 e 130,34 nel run 6: la variazione tra un run e l'altro è di questo ordine. A 100 req/s quindi la modalità immediata non costa meno di `main` in modo misurabile.
- **Le latenze delle tabelle sono a regime** (dopo il primo minuto). Con il primo minuto, la p99 delle scritture a 100 req/s su S3 vero è 3,3-3,9 s in ogni modalità nel run 5 (2,0-2,9 s nel run 2): è il risveglio del database dopo il riavvio e le prime scritture.

- **Le correzioni della parte A (stessa modalità immediata)** tolgono il costo di fondo di un database aperto:
  - la pulizia del WAL con retention ora fa 0 richieste quando non c'è nulla da cancellare;
  - un lease per processo invece di uno per database;
  - letture del manifest e contatori d'uso meno frequenti.
  - A 1 req/s questo vale -38% su MinIO (da 11,44 a 7,05) e -27% su S3 vero (da 11,65 a 8,49, run 5). Con molte scritture resta ~1 PUT per scrittura, quindi il costo cambia poco.
- **Su S3 vero la modalità immediata a 100 req/s costa molto meno che su MinIO** (111,65 contro 619,47 $). Ogni flush dal runner GitHub a eu-west-1 dura ~200 ms (scrittura p50 225 ms nel run 2), e le scritture arrivate nel frattempo entrano nello stesso PUT (0,089 richieste per richiesta del client nel run 5).
- **La modalità rilassata e il log locale cambiano l'ordine di grandezza**:
  - pochi $ per database al mese a qualunque ritmo;
  - latenza di scrittura sotto 2 ms anche su S3 vero.
  - Il prezzo è la durabilità: kill -9 perde ~2,2 s di scritture confermate in modalità rilassata, 0 con il log locale. Il log locale però perde tutto ciò che non è ancora su S3 se si perde il disco (non misurato: non ho simulato la perdita del disco).
- **Raggruppamento adattivo:** quasi nessun effetto.
  - Su MinIO un flush dura 3-5 ms: nessuna scrittura arriva in tempo per unirsi.
  - Su S3 vero le scritture concorrenti si uniscono già da sole.
  - Al tetto di un database con 4 connessioni dimezza i PUT (da 315 a 133 PUT/s), con le richieste servite che scendono da 622 a 522 al secondo (sezione Tetto).
- **Spesa reale su AWS:** 2,52 USD in 6 run, su un tetto di 10 USD. È la stima per eccesso del driver (richieste contate × 1,1, più 200 richieste per passo e 2 giorni di storage, prezzi di eu-west-1); la fattura AWS è non misurata. Dettaglio e stato dei bucket nella sezione E.

## A. Correzioni

### A1. Pulizia del WAL con retention: costo indipendente dalla storia

**Prima.** Con `--wal-gc-mode bulk` e una finestra di retention, la pulizia di ogni scrittore aperto cadeva sul passaggio di slatedb ogni 20 s:
- elencava tutta la cartella `wal/` e leggeva i manifest;
- non cancellava nulla per 7 giorni;
- cresceva con la storia.

**Ora** (`src/lifecycle.rs`, `gc_tenant_wal`, `gc_wal_with`):
- **Regola.** Un oggetto WAL `n` si cancella quando `n` è sotto il confine di ripresa (`replay_after_wal_id`) del database **e** sotto il confine del checkpoint più vecchio. I punti di ripristino sono checkpoint, quindi con la retention resta tutto il WAL dal punto più vecchio in poi: quello che un ripristino può usare.
- **Zero richieste per decidere.** La decisione usa la vista del manifest che lo scrittore ha già in memoria. Un checkpoint preso dopo quella vista ha un confine almeno uguale, perché i confini crescono con la versione. Quindi la vista vecchia non porta a cancellare niente di necessario.
- **Costi una tantum:**
  - confine del checkpoint più vecchio: 1 GET, una volta per checkpoint;
  - WAL più basso: 1 LIST, una volta per database e processo (su S3 le chiavi sono ordinate, basta la prima pagina).
- **Una passata senza nulla di nuovo da cancellare non fa richieste.**
- **Pulizia SST e manifest degli scrittori aperti.** Prima girava ogni 60 s; ora solo dopo una compattazione di questo processo, dopo 16 versioni del manifest, o quando restano oggetti in attesa dell'età minima.
- **Pulizia dopo la chiusura.**
  - WAL con la stessa regola (1 LIST + 1 GET del manifest).
  - Il passaggio completo di slatedb, che legge il manifest di ogni checkpoint, solo dopo una compattazione o 16 versioni del manifest.

**Misura** (MinIO, 10 database, 1 req/s 50/50, retention 7 giorni, finestra di 300 s):

| | prima | dopo |
|---|---|---|
| LIST per database al minuto | 11,2 | 0,42 |
| GET per database al minuto | 49,3 | 7,5 (letture dei client e manifest) |
| passate della pulizia WAL senza richieste | non misurato (non c'era il contatore) | 150 su 150 |
| passate SST saltate perché nulla era cambiato | non misurato | 50 su 50 |

**Non misurato:**
- il comportamento dopo 7 giorni di storia reale (i run durano minuti);
- il costo della pulizia quando i punti di ripristino iniziano a scadere.

### A2. Un lease per processo (`--process-lease`, ora attivo per default)

- **Il meccanismo esisteva già** (`mongo-server/src/lease.rs`).
  - Il processo rinnova solo `processes/<holder>/_lease` ogni ttl/3.
  - Un database aperto ha un record del proprietario, che nomina la generazione del lease del processo.
  - Se il lease del processo scade, ad esempio con il processo congelato, tutti i database di quella generazione vengono abbandonati.
- **Ora è il default.** `--process-lease false` torna al lease per database.
- **Misura (MinIO, 10 database aperti):**
  - prima, 6 PUT di lease al minuto per database;
  - dopo, 6 PUT al minuto per **processo** (0,6 per database con 10 database, 0,06 con 100).
- **Test dedicato** `ctl-tests/test_20_process_lease.py`: 9 test, verdi sul binario finale.
  - rifiuto (134) per letture e scritture mentre il proprietario è vivo;
  - proprietario congelato oltre il ttl: l'altro subentra, e il proprietario ripreso ha letture e scritture rifiutate;
  - controllo negativo con `--unsafe-skip-ownership-checks`: il processo ripreso serve dati vecchi;
  - SIGKILL: l'altro processo subentra con tutte le 200 scritture confermate;
  - lo stesso holder che riparte riprende subito i suoi database;
  - 40 scrittori concorrenti su due processi: nessuna scrittura confermata persa;
  - nessun PUT di lease per database aperto e fermo;
  - modalità miste che si rifiutano a vicenda.
- **Test esistente modificato:** `test_08b_pin_cost`. Misura apposta il lease per database; ora lo chiede con `--process-lease false`.

### A3. Memoria

Misura con `tools/round3/memgrowth.py`: N database su MinIO, insert di ~1 KB, RSS ogni 10 s.

| binario e opzioni | carico | KB di RSS per scrittura | andamento |
|---|---|---|---|
| prima (main) | 20 db × 10 scritture/s, 300 s | 4,44 | cresce senza fermarsi (299 MB) |
| prima, memtable da 1 MB | 20 × 10, 300 s | 4,29 | cresce |
| log dei change stream limitato a 32 MB | 20 × 10, 300 s | 1,98 | cresce |
| + memtable 1 MB + cache 8 + 4 MB | 20 × 10, 600 s | 0,74 | **si ferma a ~131 MB** dopo ~200 s |
| + memtable 1 MB + cache 8 + 4 MB | 50 × 2, 420 s | 5,49 | cresce: con 2 scritture/s la memtable non si riempie |
| memtable 2 MB, cache 256 + 64 MB (`bca96f6`) | 50 × 2, 480 s | 5,74 | cresce nei primi 8 minuti |
| memtable 2 MB, cache 256 + 64 MB (`bca96f6`), run lungo | 50 × 2, 5400 s | 1,10 | RSS 357 MB a 10 min, 503 MB a 30 min, 565 MB a 60 min, 634 MB a 90 min |
| motore finale, 8 MB, cache 256 + 64 MB (`b72262e`), run lungo | 50 × 2, 3600 s | 2,60 | RSS 374 MB a 10 min, 874 MB a 30 min, 967 MB a 60 min |

**Cause trovate**, in ordine di peso:
1. **Una scrittura non ancora scaricata in L0 occupa ~4,3 KB di memoria del processo** per 1 KB di documento, mentre slatedb ne conta ~1,1 KB per decidere quando scaricare la memtable.
   - slatedb scarica una memtable piccola solo dopo 4096 flush del WAL e rifiuta un valore più basso (`max_wal_flushes_before_l0_flush must be at least 4096`).
   - Quindi un database scritto lentamente teneva in memoria ogni scrittura per ore, fino a 4096 scritture.
2. **Il log degli eventi dei change stream** teneva 100.000 eventi con il documento intero, anche senza change stream aperti. Ora ha anche un limite stimato di 32 MB (`--change-log-max-mb`).
3. **La cache dei blocchi di slatedb, 512 MB + 128 MB per ogni database aperto**, si riempiva anche con i dati scritti. Ora c'è una sola cache condivisa (`--block-cache-mb 256`, `--meta-cache-mb 64`) con uno scope nuovo a ogni apertura.

**Correzione finale.**
- `--memtable-max-mb 8` (slatedb: 64 MB per database): il tetto per database è la dimensione della memtable, non il numero di scritture.
- Il manifest dello scrittore si rilegge subito dopo una compattazione.
- **Provato e scartato 1:** uno scarico forzato della memtable ogni 256 scritture. Fermava la crescita su MinIO (50 × 2: la RSS si fermava a ~160 MB), ma **su S3 vero bloccava le scritture**: p99 di 6,4 s in modalità rilassata a 100 req/s, e costo doppio a 10 req/s (run 3, sezione E2).
- **Provato e scartato 2:** `--memtable-max-mb 2`. Su S3 vero un database a 100 req/s scaricava la memtable così spesso da aspettare le compattazioni: p99 delle scritture 2,1 s (rilassata) e 2,7 s (log locale), costo 9,81 e 8,11 $/mese contro 4,53 e 2,79 con 8 MB (run 4 contro run 2, sezione E). Con 2 MB anche `test_02_fork` è fallito una volta (2002 byte contro un limite di 2000); con 8 MB è passato 3 volte su 3.
- La memoria con 8 MB per molti database scritti a lungo è nel run lungo qui sopra (riga "motore finale, 8 MB"). La memoria di un server con centinaia di database aperti in scrittura è **non misurata**.

### A4. `--max-concurrent-wakes` e `LimitNOFILE`

Riavvio con 500 database (MinIO, motore su 2 CPU).
- Ogni database riceve la prima richiesta entro 0,3 s dagli altri, poi 1 richiesta/s, 50/50, per 120 s.
- Client asincroni con pool fino a 64 connessioni per database.

| `--max-concurrent-wakes` | prima richiesta p50 | p99 | max | scritture p99 nel primo minuto | errori | descrittori aperti max | RSS max |
|---|---|---|---|---|---|---|---|
| 8 | 2,7 s | 16,4 s | 18,2 s | 13,6 s | 0 | 6008 | 966 MB |
| 32 | 3,9 s | 12,1 s | 13,1 s | 11,0 s | 0 | 6186 | 900 MB |
| 128 | 6,1 s | **8,6 s** | 8,8 s | 11,9 s | 0 | 7912 | 1373 MB |
| 0 (senza limite, il default di prima) | 6,3 s | 9,5 s | 9,8 s | 11,2 s | 0 | 7435 | 1258 MB |

- **Nuovo default: 128**, perché ha la coda più corta: il p99 è quello che un cliente aspetta. Un limite basso migliora la mediana e peggiora la coda.
- In tutti i casi 500 risvegli insieme su 2 CPU portano la prima richiesta a secondi: **il riavvio di un server pieno non è invisibile.**
- **Descrittori:** circa 1 per connessione client, più qualche decina del motore. Con 500 database e i pool di questi client si arriva a ~8000. `LimitNOFILE=65536`, già nel file systemd di `fenvix-app/infra`, basta.
- **Non misurato:** quante connessioni userebbero i driver reali dei clienti.

### Altri default cambiati

- `--writer-poll-secs` da 30 a 60: ogni lettura del manifest di uno scrittore aperto costa 1 LIST e 1 GET.
- `--usage-flush-secs` da 60 a 300: 1 PUT per database attivo a ogni flush.

## B. Modalità di scrittura

### B1. Raggruppamento adattivo (`--adaptive-group-commit true`, spento per default)

- Il primo scrittore di un gruppo aspetta metà del tempo medio di flush del database (al massimo 5 ms), solo se altre scritture sono attese o già in arrivo.
- Una scrittura isolata non aspetta mai.
- **Misurato:** nelle tabelle D ed E è quasi uguale all'immediata. L'effetto si vede solo al tetto di un database con più connessioni (sezione Tetto): da 315 a 133 PUT/s con 4 connessioni.

### B2. Durabilità rilassata per database

**Semantica esatta**, anche nel README:
- **Accensione:** `PUT /v1/databases/{db}/durability {"relaxed_flush_ms": 1000..10000}` sulla porta admin, oppure `--relaxed-flush-ms` per tutti i database. Default spento.
- **Conferma prima della durabilità:** solo per un comando di scrittura con write concern `{w: 1, j: false}`, `{j: false}` o `{w: 0}` senza `j: true`, fuori da una transazione, su un database con l'impostazione attiva. La conferma arriva quando la scrittura è applicata in memoria.
- **Durabilità:** lo scrittore fa il flush del WAL da solo ogni `relaxed_flush_ms`.
- **Visibilità:** le letture di quel database vedono subito queste scritture, come mongod con `j: false`.
- **Restano durevoli prima della conferma, come oggi:**
  - `j: true`, `w: "majority"`;
  - nessun write concern, cioè il default dei driver;
  - il commit di una transazione.
  - Rendono durevoli anche le scritture rilassate precedenti.
- **Chiusura e arresto:** una chiusura per inattività o un arresto pulito fanno il flush.
- **Kill -9:** si perdono le scritture rilassate non ancora scaricate (misurato sotto).
- **Correzione fatta strada facendo:** un comando che scrive in più database aspettava solo l'ultimo commit. Ora aspetta l'ultimo di ogni database.

## C. Prototipo: log locale durevole (`--local-log-dir`, spento per default)

`mongo-server/src/locallog.rs`.
- **Scrittura:** conferma dopo un `fdatasync` di gruppo del log locale.
- **Caricamento su S3:** ogni `--local-log-upload-secs` (30-60) ogni database scritto fa il flush del suo WAL, 1 PUT.
- **Riavvio:** prima di accettare connessioni, il motore riapplica una sola volta i record non ancora su S3; ogni batch porta un marcatore con il numero del record. Drop, erase o perdita della proprietà scartano i record precedenti.
- **Se si perde il disco:** si perdono tutte le scritture confermate dopo l'ultimo caricamento di ogni database, fino a `--local-log-upload-secs` di scritture di tutti i database del processo. **Non misurato:** non ho simulato la perdita del disco.
- **Altro limite:** finché il riavvio sullo stesso disco non è avvenuto, un altro processo che prende il database non vede quelle scritture.

## D. Misure su MinIO

**Metodo** (`tools/round3/run.py`):
- bucket nuovo; N database con 200 documenti da ~1 KB; riavvio pulito, quindi tutti i database chiusi;
- carico a ciclo aperto (Poisson) per 240-360 s;
- conteggio nella finestra dopo il primo minuto;
- a 1 req/s 10 database (finestra 300 s), a 10 e 100 req/s 2 database (finestra 180 s);
- motore fissato su 2 CPU, client e MinIO sulle altre 2;
- retention 7 giorni, `--idle-secs 60`, porta admin attiva;
- modalità rilassata con write concern `{w: 1, j: false}` dai client e `--relaxed-flush-ms 5000`; log locale con `--local-log-upload-secs 30`;
- "prima" = binario di `main` con i suoi flag.

**Motore usato per D:** commit `7fb4f5a` (le parti A1-C, prima delle correzioni di memoria e della coda equa).

**Flotte:**
- *flotta 100*: 100 database × 1 scrittura/s;
- *flotta mista*: 50 database a raffiche (2 req/s 50/50 per 30 s ogni 300 s), 20 con 1 lettura/s, 30 fermi, per 600 s.

**Non misurato su MinIO:**
- lo storage per database: il listing usava un prefisso sbagliato e ha contato 0 byte, quindi non lo riporto;
- su S3 vero è misurato (sezione E).

Le letture pure delle modalità adattiva, rilassata e log locale non sono state misurate: senza scritture il percorso del codice è lo stesso dell'immediata.

**Due cose da sapere per leggere le tabelle.**
- Nei run da 1 o 2 database il lease del processo (6 PUT/min) è diviso per pochi database. Per questo "solo letture" costa 0,3-0,9 $ e non zero.
- Nella flotta mista il p99 delle scritture (~38 ms in tutte le modalità) viene dai risvegli a freddo all'inizio di ogni raffica.

**$ di richieste per database al mese (MinIO)**

| scenario | prima (main) | immediata | adattiva | rilassata 5 s | log locale 30 s |
|---|---|---|---|---|---|
| 1 req/s, 50/50 | 11,44 | 7,05 | 7,05 | 2,79 | 1,19 |
| 1 req/s, solo scritture | 17,85 | 13,46 | 13,46 | 2,88 | 1,19 |
| 1 req/s, solo letture | 1,61 | 0,31 | non misurato | non misurato | non misurato |
| 10 req/s, 50/50 | 69,42 | 64,07 | 64,46 | 3,50 | 1,77 |
| 10 req/s, solo scritture | 132,14 | 126,27 | 126,16 | 3,52 | 1,80 |
| 10 req/s, solo letture | 1,73 | 0,86 | non misurato | non misurato | non misurato |
| 100 req/s, 50/50 | 630,51 | 619,47 | 616,15 | 3,67 | 1,95 |
| 100 req/s, solo scritture | 1215,89 | 1191,29 | 1175,66 | 7,39 | 5,58 |
| 100 req/s, solo letture | 1,75 | 0,88 | non misurato | non misurato | non misurato |
| flotta 100 database × 1 scrittura/s | 17,45 | 12,94 | 12,93 | 2,72 | 1,02 |
| flotta mista 50 raffiche / 20 letture / 30 ferme | 2,23 | 1,24 | 1,24 | 0,74 | 0,66 |

**Richieste S3 per richiesta del client (MinIO)**

| scenario | prima (main) | immediata | adattiva | rilassata 5 s | log locale 30 s |
|---|---|---|---|---|---|
| 1 req/s, 50/50 | 1,605 | 0,645 | 0,645 | 0,323 | 0,202 |
| 1 req/s, solo scritture | 2,013 | 1,052 | 1,052 | 0,251 | 0,123 |
| 1 req/s, solo letture | 0,166 | 0,067 | non misurato | non misurato | non misurato |
| 10 req/s, 50/50 | 0,617 | 0,508 | 0,509 | 0,031 | 0,017 |
| 10 req/s, solo scritture | 1,109 | 0,992 | 0,991 | 0,032 | 0,019 |
| 10 req/s, solo letture | 0,013 | 0,007 | non misurato | non misurato | non misurato |
| 100 req/s, 50/50 | 0,504 | 0,489 | 0,486 | 0,003 | 0,002 |
| 100 req/s, solo scritture | 0,976 | 0,949 | 0,937 | 0,014 | 0,013 |
| 100 req/s, solo letture | 0,001 | 0,001 | non misurato | non misurato | non misurato |
| flotta 100 database × 1 scrittura/s | 2,109 | 1,047 | 1,047 | 0,250 | 0,117 |
| flotta mista 50 raffiche / 20 letture / 30 ferme | 1,381 | 0,726 | 0,725 | 0,596 | 0,582 |

**Richieste classe PUT/LIST per database al minuto (MinIO)**

| scenario | prima (main) | immediata | adattiva | rilassata 5 s | log locale 30 s |
|---|---|---|---|---|---|
| 1 req/s, 50/50 | 49,0 | 32,1 | 32,1 | 12,3 | 4,9 |
| 1 req/s, solo scritture | 79,1 | 62,1 | 62,1 | 13,1 | 5,3 |
| 1 req/s, solo letture | 7,2 | 1,2 | non misurato | non misurato | non misurato |
| 10 req/s, 50/50 | 317,5 | 296,4 | 298,2 | 16,0 | 8,0 |
| 10 req/s, solo scritture | 607,9 | 584,4 | 583,9 | 16,1 | 8,1 |
| 10 req/s, solo letture | 8,0 | 4,0 | non misurato | non misurato | non misurato |
| 100 req/s, 50/50 | 2915,1 | 2867,6 | 2852,2 | 16,7 | 8,8 |
| 100 req/s, solo scritture | 5620,2 | 5510,2 | 5437,8 | 29,8 | 21,6 |
| 100 req/s, solo letture | 8,1 | 4,0 | non misurato | non misurato | non misurato |
| flotta 100 database × 1 scrittura/s | 76,9 | 59,7 | 59,7 | 12,4 | 4,5 |
| flotta mista 50 raffiche / 20 letture / 30 ferme | 9,0 | 5,1 | 5,1 | 2,8 | 2,4 |

**Richieste classe GET per database al minuto (MinIO)**

| scenario | prima (main) | immediata | adattiva | rilassata 5 s | log locale 30 s |
|---|---|---|---|---|---|
| 1 req/s, 50/50 | 49,2 | 7,5 | 7,5 | 7,5 | 7,5 |
| 1 req/s, solo scritture | 44,0 | 2,2 | 2,2 | 2,2 | 2,2 |
| 1 req/s, solo letture | 2,9 | 2,9 | non misurato | non misurato | non misurato |
| 10 req/s, 50/50 | 48,8 | 3,1 | 3,1 | 2,5 | 2,5 |
| 10 req/s, solo scritture | 48,6 | 2,9 | 2,9 | 2,9 | 2,9 |
| 10 req/s, solo letture | 0,0 | 0,0 | non misurato | non misurato | non misurato |
| 100 req/s, 50/50 | 48,8 | 3,9 | 3,9 | 3,6 | 3,6 |
| 100 req/s, solo scritture | 111,9 | 62,9 | 63,1 | 54,8 | 53,2 |
| 100 req/s, solo letture | 0,0 | 0,0 | non misurato | non misurato | non misurato |
| flotta 100 database × 1 scrittura/s | 48,1 | 2,4 | 2,4 | 2,4 | 2,4 |
| flotta mista 50 raffiche / 20 letture / 30 ferme | 15,9 | 8,0 | 8,0 | 8,0 | 8,1 |

**Scritture p50 ms dopo il primo minuto (MinIO)**

| scenario | prima (main) | immediata | adattiva | rilassata 5 s | log locale 30 s |
|---|---|---|---|---|---|
| 1 req/s, 50/50 | 5,7 | 5,7 | 5,6 | 1,7 | 3,0 |
| 1 req/s, solo scritture | 5,7 | 5,6 | 5,5 | 1,8 | 2,8 |
| 1 req/s, solo letture | non misurato | non misurato | non misurato | non misurato | non misurato |
| 10 req/s, 50/50 | 5,6 | 5,6 | 5,4 | 1,6 | 2,8 |
| 10 req/s, solo scritture | 5,3 | 5,6 | 5,5 | 1,7 | 2,7 |
| 10 req/s, solo letture | non misurato | non misurato | non misurato | non misurato | non misurato |
| 100 req/s, 50/50 | 4,9 | 4,6 | 4,6 | 1,3 | 2,2 |
| 100 req/s, solo scritture | 4,5 | 4,8 | 4,7 | 1,4 | 2,2 |
| 100 req/s, solo letture | non misurato | non misurato | non misurato | non misurato | non misurato |
| flotta 100 database × 1 scrittura/s | 4,6 | 4,7 | 4,7 | 1,4 | 2,3 |
| flotta mista 50 raffiche / 20 letture / 30 ferme | 5,7 | 5,7 | 6,0 | 1,8 | 3,0 |

**Scritture p99 ms dopo il primo minuto (MinIO)**

| scenario | prima (main) | immediata | adattiva | rilassata 5 s | log locale 30 s |
|---|---|---|---|---|---|
| 1 req/s, 50/50 | 10,6 | 9,8 | 10,5 | 3,8 | 5,7 |
| 1 req/s, solo scritture | 10,7 | 10,6 | 10,0 | 4,0 | 5,6 |
| 1 req/s, solo letture | non misurato | non misurato | non misurato | non misurato | non misurato |
| 10 req/s, 50/50 | 10,8 | 10,9 | 11,0 | 3,6 | 5,9 |
| 10 req/s, solo scritture | 10,5 | 10,4 | 11,5 | 4,3 | 6,2 |
| 10 req/s, solo letture | non misurato | non misurato | non misurato | non misurato | non misurato |
| 100 req/s, 50/50 | 11,9 | 10,3 | 10,9 | 3,5 | 6,3 |
| 100 req/s, solo scritture | 23,9 | 17,7 | 13,3 | 4,1 | 7,1 |
| 100 req/s, solo letture | non misurato | non misurato | non misurato | non misurato | non misurato |
| flotta 100 database × 1 scrittura/s | 16,9 | 8,9 | 9,1 | 3,4 | 5,5 |
| flotta mista 50 raffiche / 20 letture / 30 ferme | 39,8 | 38,6 | 40,2 | 35,8 | 38,8 |

**Letture p50 ms dopo il primo minuto (MinIO)**

| scenario | prima (main) | immediata | adattiva | rilassata 5 s | log locale 30 s |
|---|---|---|---|---|---|
| 1 req/s, 50/50 | 1,8 | 1,8 | 1,8 | 1,8 | 1,9 |
| 1 req/s, solo scritture | non misurato | non misurato | non misurato | non misurato | non misurato |
| 1 req/s, solo letture | 1,7 | 1,9 | non misurato | non misurato | non misurato |
| 10 req/s, 50/50 | 1,7 | 1,7 | 1,7 | 1,8 | 1,8 |
| 10 req/s, solo scritture | non misurato | non misurato | non misurato | non misurato | non misurato |
| 10 req/s, solo letture | 1,7 | 1,6 | non misurato | non misurato | non misurato |
| 100 req/s, 50/50 | 1,4 | 1,3 | 1,3 | 1,3 | 1,3 |
| 100 req/s, solo scritture | non misurato | non misurato | non misurato | non misurato | non misurato |
| 100 req/s, solo letture | 1,2 | 1,1 | non misurato | non misurato | non misurato |
| flotta 100 database × 1 scrittura/s | non misurato | non misurato | non misurato | non misurato | non misurato |
| flotta mista 50 raffiche / 20 letture / 30 ferme | 3,6 | 3,6 | 3,8 | 3,8 | 3,8 |

**Letture p99 ms dopo il primo minuto (MinIO)**

| scenario | prima (main) | immediata | adattiva | rilassata 5 s | log locale 30 s |
|---|---|---|---|---|---|
| 1 req/s, 50/50 | 5,0 | 6,0 | 4,9 | 5,2 | 5,5 |
| 1 req/s, solo scritture | non misurato | non misurato | non misurato | non misurato | non misurato |
| 1 req/s, solo letture | 4,2 | 4,6 | non misurato | non misurato | non misurato |
| 10 req/s, 50/50 | 4,3 | 3,9 | 4,0 | 4,1 | 4,2 |
| 10 req/s, solo scritture | non misurato | non misurato | non misurato | non misurato | non misurato |
| 10 req/s, solo letture | 3,3 | 3,4 | non misurato | non misurato | non misurato |
| 100 req/s, 50/50 | 4,2 | 3,5 | 3,6 | 3,7 | 3,8 |
| 100 req/s, solo scritture | non misurato | non misurato | non misurato | non misurato | non misurato |
| 100 req/s, solo letture | 3,3 | 3,2 | non misurato | non misurato | non misurato |
| flotta 100 database × 1 scrittura/s | non misurato | non misurato | non misurato | non misurato | non misurato |
| flotta mista 50 raffiche / 20 letture / 30 ferme | 19,3 | 19,3 | 20,4 | 20,5 | 20,1 |

**RSS massima del motore, MB (MinIO)**

| scenario | prima (main) | immediata | adattiva | rilassata 5 s | log locale 30 s |
|---|---|---|---|---|---|
| 1 req/s, 50/50 | 46 | 46 | 46 | 45 | 50 |
| 1 req/s, solo scritture | 50 | 50 | 51 | 50 | 54 |
| 1 req/s, solo letture | 37 | 37 | non misurato | non misurato | non misurato |
| 10 req/s, 50/50 | 41 | 40 | 40 | 40 | 42 |
| 10 req/s, solo scritture | 50 | 48 | 48 | 49 | 53 |
| 10 req/s, solo letture | 30 | 27 | non misurato | non misurato | non misurato |
| 100 req/s, 50/50 | 123 | 94 | 97 | 114 | 123 |
| 100 req/s, solo scritture | 219 | 127 | 128 | 188 | 208 |
| 100 req/s, solo letture | 31 | 29 | non misurato | non misurato | non misurato |
| flotta 100 database × 1 scrittura/s | 232 | 180 | 171 | 174 | 232 |
| flotta mista 50 raffiche / 20 letture / 30 ferme | 88 | 97 | 97 | 97 | 116 |

**Errori dei client (MinIO)**

| scenario | prima (main) | immediata | adattiva | rilassata 5 s | log locale 30 s |
|---|---|---|---|---|---|
| 1 req/s, 50/50 | 0 | 0 | 0 | 0 | 0 |
| 1 req/s, solo scritture | 0 | 0 | 0 | 0 | 0 |
| 1 req/s, solo letture | 0 | 0 | non misurato | non misurato | non misurato |
| 10 req/s, 50/50 | 0 | 0 | 0 | 0 | 0 |
| 10 req/s, solo scritture | 0 | 0 | 0 | 0 | 0 |
| 10 req/s, solo letture | 0 | 0 | non misurato | non misurato | non misurato |
| 100 req/s, 50/50 | 0 | 0 | 0 | 0 | 0 |
| 100 req/s, solo scritture | 0 | 0 | 0 | 0 | 0 |
| 100 req/s, solo letture | 0 | 0 | non misurato | non misurato | non misurato |
| flotta 100 database × 1 scrittura/s | 0 | 0 | 0 | 0 | 0 |
| flotta mista 50 raffiche / 20 letture / 30 ferme | 0 | 0 | 0 | 0 | 0 |

### Kill -9 in ogni modalità

`tools/round3/killtest.py`, MinIO.
- 4 database, 20 scritture/s ciascuno, write concern della modalità (rilassata: `{w: 1, j: false}`).
- SIGKILL dopo 20 s, riavvio, poi conteggio di ogni `_id` confermato. 3 prove per modalità.

**Cosa controlla e cosa no:**
- Controlla solo che ogni `_id` confermato sia presente dopo il riavvio, non il contenuto.
- SIGKILL uccide il processo, non la macchina: la page cache sopravvive, quindi **non verifica che `fdatasync` abbia raggiunto il disco**. Perdita di corrente, della VM o del disco: **non misurato**.
- **Controllo negativo del log locale** (`local-noreplay`): il log è scritto ma il riavvio non lo applica (`MONGO_TEST_LOCALLOG_NO_REPLAY`, solo test). Deve perdere scritture, e mostra che il test vede una perdita.

**Motore finale (`4677caf`, sha256 `d811978b…`), dopo la revisione:**

| modalità | scritture confermate (3 prove) | perse | riapplicate dal log locale per prova |
|---|---|---|---|
| immediata | 4962 | 0 | - |
| adattiva | 4962 | 0 | - |
| rilassata 5 s | 4968 | 529 | - |
| log locale 30 s | 4967 | 0 | 1670-1675 |
| **controllo negativo**: log locale senza replay | 4967 | 4967 | 0-0 |

**Misure precedenti** (`results-round3/minio/kill/`):

| modalità | binario | scritture confermate (3 prove) | perse | riapplicate dal log locale |
|---|---|---|---|---|
| prima (main) | `c9a4beb` | 4962 | 0 | - |
| immediata | `ea63a08` | 4962 | 0 | 0 |
| adattiva | `ea63a08` | 4962 | 0 | 0 |
| rilassata 5 s | `ea63a08` | 4968 | **530** (176-177 per prova, ~2,2 s di scritture) | 0 |
| log locale 30 s | `ea63a08` | 4966 | 0 | 1670-1674 per prova |
| log locale, 16 database (`kill-local-final-16db.json`) | `f034e36` (binario costruito con il codice di `f034e36`; il file è stato salvato in `3ada92b`) | 19 291 | 0 | 6506-6512 per prova |

- Le scritture perse in modalità rilassata sono le ultime prima del kill: una finestra di flush di 5 s, in media metà.
- **Non misurato:** kill -9 con `j: true` mescolato a scritture rilassate.

## Registro di scrittura condiviso tra database dello stesso server (solo calcolo)

`tools/round3/sharedlog.py`, `results-round3/sharedlog.json`. Niente è costruito.

**Modello.**
- Tutte le scritture dei database attivi del server sono confermate dopo un unico PUT per finestra W. Una finestra senza scritture non manda nulla.
- La conferma arriva dopo il salvataggio su S3, quindi un crash non perde nulla.
- I dati devono comunque arrivare nei file di ogni database ("materializzazione"). Qui si assume al massimo 1 PUT per database per minuto in cui è stato scritto.

**Tracce simulate:** un'ora, scritture di Poisson.
- **flotta 100:** 100 database da 1 scrittura/s ciascuno.
- **flotta mista:** 50 a raffiche (1 scrittura/s per 30 s ogni 300 s), 20 solo letture, 30 fermi.
- **20 database, 4-5 attivi alla volta:** il gruppo attivo cambia ogni 60 s; 10 scritture/s ciascuno quando attivo.

Prezzo: 0,005 $ per 1000 PUT; mese di 30 giorni.

| traccia | modalità | PUT/s del server | $/mese del server | $/database/mese |
|---|---|---|---|---|
| flotta 100 (100 scritture/s) | immediata (1 PUT per scrittura) | 100,2 | 1298 | 12,98 |
| | rilassata 5 s (per database) | 19,9 | 258 | 2,58 |
| | log locale 30 s (per database) | 3,3 | 43 | 0,43 |
| | condiviso 50 ms (+ materializzazione) | 19,9 (21,5) | 257 (279) | 2,57 (2,79) |
| | condiviso 100 ms | 10,0 (11,7) | 130 (151) | 1,30 (1,51) |
| | condiviso 250 ms | 4,0 (5,7) | 52 (73) | 0,52 (0,73) |
| | condiviso 1 s | 1,0 (2,7) | 13 (35) | 0,13 (0,35) |
| flotta mista (5 scritture/s) | immediata | 5,0 | 65 | 0,65 |
| | rilassata 5 s | 1,1 | 14 | 0,14 |
| | log locale 30 s | 0,32 | 4,2 | 0,04 |
| | condiviso 50 ms | 4,3 (4,6) | 56 (59) | 0,56 (0,59) |
| | condiviso 100 ms | 3,7 (4,0) | 48 (52) | 0,48 (0,52) |
| | condiviso 250 ms | 2,6 (2,8) | 33 (37) | 0,33 (0,37) |
| | condiviso 1 s | 0,9 (1,2) | 12 (15) | 0,12 (0,15) |
| 20 database, 4-5 attivi (45 scritture/s) | immediata | 45,0 | 584 | 29,19 |
| | rilassata 5 s | 0,9 | 12 | 0,59 |
| | log locale 30 s | 0,15 | 2,0 | 0,10 |
| | condiviso 50 ms | 17,9 (17,9) | 231 (232) | 11,57 (11,62) |
| | condiviso 100 ms | 9,9 (9,9) | 128 (129) | 6,40 (6,45) |
| | condiviso 250 ms | 4,0 (4,1) | 52 (53) | 2,59 (2,64) |
| | condiviso 1 s | 1,0 (1,1) | 13 (14) | 0,65 (0,70) |

**Cosa dice il calcolo.**
- **Il registro condiviso conviene davvero solo quando molti database scrivono poco e insieme.** Nella flotta 100, a 250 ms costa circa 1/20 dell'immediata e meno della rilassata a 5 s, **senza perdere nulla in caso di crash**. Una conferma dopo al massimo 250 ms più un PUT è però una latenza di scrittura di 100-300 ms per tutti.
- **Con traffico sparso** (flotta mista) le finestre sono quasi sempre di una sola scrittura: il risparmio sull'immediata è piccolo (da 0,65 a 0,33-0,56 $), e la rilassata o il log locale per database costano meno.
- **Con pochi database molto attivi** (20 con 4-5 attivi) dipende dalla finestra del registro condiviso:
  - a 50 ms il condiviso costa ~20 volte la rilassata (11,57 contro 0,59 $/db) e ~115 volte il log locale (0,10 $/db);
  - a 1 s costa solo 1,1-1,2 volte la rilassata (0,65-0,70 contro 0,59 $/db) e ~7 volte il log locale.

**Cosa renderebbe più difficile:**
- **Ripristino di un solo database.** Il suo WAL è sparso in oggetti condivisi con gli altri database del server. Un ripristino a un istante deve leggere e filtrare i registri di tutti, e la retention di 7 giorni la paga tutto il server.
- **Esportazione:** possibile solo dopo la materializzazione dei suoi dati.
- **Cancellazione di un cliente.** I suoi dati restano negli oggetti condivisi finché non scadono o non vengono riscritti senza di lui. Non basta più cancellare un prefisso, e non si verifica con un listing: è un problema per GDPR e contratti.
- **Isolamento tra clienti.** Un oggetto condiviso danneggiato, o un errore nel filtro per database, tocca tutti i database del server. Cifrare con una chiave per cliente diventa difficile.
- **Latenza dei vicini rumorosi.** Un database che scrive tanto ingrandisce ogni PUT condiviso e allunga la conferma di tutti.
- **Spostamento di un database su un altro server.** Prima va materializzato, poi rilasciato: lo spostamento si allunga e dipende dal registro del server vecchio.

## Tetto di UN SOLO database (MinIO)

`tools/round3/ceiling.py`, `results-round3/minio/ceiling.json`, motore `ea63a08`.

**Metodo:**
- 1 database con 200 documenti;
- 1 client Python con 1, 4, 16, 64 e 256 connessioni parallele, ciascuna in ciclo chiuso;
- 60 s per gradino;
- motore fissato su 2 o 4 CPU, client e MinIO sulle altre;
- "$/mese a questo ritmo" = richieste S3 del gradino × prezzi, come se il ritmo durasse un mese.

**Massimo per profilo e modalità** (req/s, con le connessioni a cui si raggiunge):

| profilo | modalità | 2 CPU | 4 CPU |
|---|---|---|---|
| sole letture | (uguale per tutte) | 12 709 (256) | 12 153 (256) |
| sole scritture | immediata | 2 941 (64) | 2 818 (64) |
| | adattiva | 2 503 (64) | 2 839 (64) |
| | rilassata | 2 668 (16) | 2 514 (16) |
| | log locale | 2 583 (16) | 2 246 (64) |
| 50/50 | immediata | 3 226 (64) | 2 828 (64) |
| | adattiva | 3 218 (64) | 2 985 (64) |
| | rilassata | 3 401 (4) | 2 614 (16) |
| | log locale | 3 015 (16) | 2 371 (64) |

**Tutti i gradini:**

| profilo | modalità | CPU | connessioni | req/s | CPU motore % di 1 core | RSS MB | p50 ms | p99 ms | PUT/s | GET/s | S3 per richiesta | $/mese a questo ritmo | errori |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| sole letture | (tutte) | 2 | 1 | 2092 | 37,9 | 52 | 0,4 | 1,1 | 0,3 | 0,2 | 0,000 | 4 | 0 |
| sole letture | (tutte) | 2 | 4 | 4654 | 54,9 | 52 | 0,8 | 2,0 | 0,2 | 0,0 | 0,000 | 2 | 0 |
| sole letture | (tutte) | 2 | 16 | 5009 | 59,8 | 52 | 2,9 | 6,8 | 0,1 | 0,1 | 0,000 | 2 | 0 |
| sole letture | (tutte) | 2 | 64 | 12347 | 90,6 | 53 | 4,2 | 12,8 | 0,1 | 0,0 | 0,000 | 1 | 0 |
| sole letture | (tutte) | 2 | 256 | 12709 | 92,1 | 58 | 17,4 | 43,6 | 0,1 | 0,0 | 0,000 | 2 | 0 |
| sole scritture | immediata | 2 | 1 | 256 | 23,7 | 78 | 3,6 | 6,3 | 256,3 | 0,5 | 1,004 | 3323 | 0 |
| sole scritture | immediata | 2 | 4 | 622 | 42,9 | 99 | 5,8 | 11,5 | 314,7 | 0,2 | 0,506 | 4078 | 0 |
| sole scritture | immediata | 2 | 16 | 1940 | 96,1 | 180 | 7,3 | 17,8 | 275,1 | 15,1 | 0,150 | 3581 | 0 |
| sole scritture | immediata | 2 | 64 | 2941 | 126,9 | 293 | 19,4 | 53,6 | 166,3 | 20,0 | 0,063 | 2176 | 0 |
| sole scritture | immediata | 2 | 256 | 2664 | 131,6 | 412 | 86,0 | 168,1 | 164,2 | 24,9 | 0,071 | 2154 | 0 |
| 50/50 | immediata | 2 | 1 | 422 | 25,0 | 83 | 2,6 | 5,7 | 211,7 | 0,5 | 0,503 | 2745 | 0 |
| 50/50 | immediata | 2 | 4 | 1074 | 63,5 | 109 | 3,2 | 9,8 | 303,6 | 0,2 | 0,283 | 3934 | 0 |
| 50/50 | immediata | 2 | 16 | 2556 | 118,7 | 168 | 5,8 | 16,7 | 265,1 | 17,5 | 0,111 | 3453 | 0 |
| 50/50 | immediata | 2 | 64 | 3226 | 130,5 | 217 | 18,1 | 42,3 | 177,4 | 16,8 | 0,060 | 2316 | 0 |
| 50/50 | immediata | 2 | 256 | 2706 | 139,0 | 355 | 84,8 | 188,2 | 154,4 | 15,1 | 0,063 | 2017 | 0 |
| sole scritture | adattiva | 2 | 1 | 229 | 22,1 | 76 | 3,9 | 8,9 | 229,2 | 0,5 | 1,004 | 2971 | 0 |
| sole scritture | adattiva | 2 | 4 | 522 | 31,9 | 98 | 7,1 | 11,7 | 133,1 | 0,2 | 0,255 | 1725 | 0 |
| sole scritture | adattiva | 2 | 16 | 1590 | 80,7 | 170 | 9,3 | 21,0 | 157,9 | 12,2 | 0,107 | 2059 | 0 |
| sole scritture | adattiva | 2 | 64 | 2503 | 126,6 | 288 | 19,6 | 89,0 | 92,8 | 15,3 | 0,043 | 1218 | 0 |
| sole scritture | adattiva | 2 | 256 | 2306 | 137,6 | 415 | 99,8 | 201,7 | 88,4 | 17,3 | 0,046 | 1164 | 0 |
| 50/50 | adattiva | 2 | 1 | 409 | 24,4 | 82 | 2,8 | 6,2 | 207,5 | 0,5 | 0,508 | 2690 | 0 |
| 50/50 | adattiva | 2 | 4 | 946 | 50,0 | 110 | 3,0 | 11,5 | 156,2 | 0,2 | 0,165 | 2024 | 0 |
| 50/50 | adattiva | 2 | 16 | 2615 | 111,0 | 165 | 5,5 | 16,3 | 163,2 | 9,6 | 0,066 | 2124 | 0 |
| 50/50 | adattiva | 2 | 64 | 3218 | 131,1 | 218 | 17,4 | 65,1 | 107,4 | 25,0 | 0,041 | 1418 | 0 |
| 50/50 | adattiva | 2 | 256 | 2995 | 144,7 | 358 | 78,4 | 150,2 | 106,0 | 15,7 | 0,041 | 1390 | 0 |
| sole scritture | rilassata | 2 | 1 | 1146 | 54,5 | 236 | 0,8 | 1,7 | 0,9 | 0,8 | 0,001 | 12 | 0 |
| sole scritture | rilassata | 2 | 4 | 2582 | 123,4 | 294 | 1,3 | 4,8 | 3,9 | 19,2 | 0,009 | 71 | 0 |
| sole scritture | rilassata | 2 | 16 | 2668 | 127,6 | 338 | 5,4 | 14,9 | 4,1 | 17,6 | 0,008 | 72 | 0 |
| sole scritture | rilassata | 2 | 64 | 2520 | 128,4 | 401 | 22,9 | 58,8 | 3,9 | 16,9 | 0,008 | 68 | 0 |
| sole scritture | rilassata | 2 | 256 | 2176 | 130,9 | 532 | 106,5 | 210,8 | 3,9 | 13,6 | 0,008 | 65 | 0 |
| 50/50 | rilassata | 2 | 1 | 1140 | 45,8 | 201 | 0,8 | 1,7 | 0,6 | 0,5 | 0,001 | 8 | 0 |
| 50/50 | rilassata | 2 | 4 | 3401 | 135,4 | 228 | 1,0 | 3,1 | 1,1 | 1,0 | 0,001 | 16 | 0 |
| 50/50 | rilassata | 2 | 16 | 3213 | 143,3 | 282 | 4,3 | 13,9 | 3,9 | 37,3 | 0,013 | 89 | 0 |
| 50/50 | rilassata | 2 | 64 | 2736 | 141,9 | 320 | 21,2 | 44,3 | 2,9 | 14,4 | 0,006 | 52 | 0 |
| 50/50 | rilassata | 2 | 256 | 2319 | 144,8 | 418 | 97,5 | 290,4 | 4,0 | 16,1 | 0,009 | 68 | 0 |
| sole scritture | log locale | 2 | 1 | 581 | 39,9 | 119 | 1,6 | 3,6 | 0,5 | 0,5 | 0,002 | 7 | 0 |
| sole scritture | log locale | 2 | 4 | 1820 | 103,5 | 204 | 1,9 | 6,2 | 3,4 | 10,7 | 0,008 | 56 | 0 |
| sole scritture | log locale | 2 | 16 | 2583 | 128,3 | 310 | 5,2 | 19,1 | 4,1 | 14,8 | 0,007 | 68 | 0 |
| sole scritture | log locale | 2 | 64 | 629 | 32,7 | 310 | 24,5 | 44,9 | 0,4 | 0,3 | 0,001 | 5 | 0 |
| sole scritture | log locale | 2 | 256 | 119 | 8,6 | 376 | 105,7 | 28353,7 | 0,4 | 0,5 | 0,008 | 6 | 0 |
| 50/50 | log locale | 2 | 1 | 824 | 40,4 | 126 | 1,1 | 2,9 | 0,5 | 0,5 | 0,001 | 7 | 0 |
| 50/50 | log locale | 2 | 4 | 2473 | 115,3 | 150 | 1,4 | 4,2 | 0,9 | 0,7 | 0,001 | 12 | 0 |
| 50/50 | log locale | 2 | 16 | 3015 | 129,5 | 214 | 4,5 | 16,0 | 3,0 | 34,7 | 0,012 | 74 | 0 |
| 50/50 | log locale | 2 | 64 | 2753 | 126,5 | 291 | 21,1 | 43,5 | 2,9 | 18,9 | 0,008 | 57 | 0 |
| 50/50 | log locale | 2 | 256 | 2380 | 128,3 | 437 | 99,2 | 191,4 | 3,2 | 16,1 | 0,008 | 59 | 0 |
| sole letture | (tutte) | 4 | 1 | 1618 | 37,2 | 53 | 0,6 | 1,3 | 0,3 | 0,2 | 0,000 | 4 | 0 |
| sole letture | (tutte) | 4 | 4 | 4562 | 57,5 | 54 | 0,8 | 2,0 | 0,2 | 0,0 | 0,000 | 2 | 0 |
| sole letture | (tutte) | 4 | 16 | 4596 | 61,7 | 55 | 3,0 | 7,2 | 0,1 | 0,1 | 0,000 | 2 | 0 |
| sole letture | (tutte) | 4 | 64 | 11562 | 105,2 | 57 | 4,8 | 13,3 | 0,1 | 0,0 | 0,000 | 1 | 0 |
| sole letture | (tutte) | 4 | 256 | 12153 | 107,3 | 62 | 18,2 | 48,2 | 0,1 | 0,0 | 0,000 | 2 | 0 |
| sole scritture | immediata | 4 | 1 | 259 | 25,9 | 83 | 3,5 | 6,1 | 259,4 | 0,5 | 1,003 | 3362 | 0 |
| sole scritture | immediata | 4 | 4 | 670 | 52,2 | 121 | 5,4 | 10,2 | 338,6 | 0,3 | 0,506 | 4388 | 0 |
| sole scritture | immediata | 4 | 16 | 1857 | 106,8 | 206 | 7,6 | 17,0 | 274,1 | 13,8 | 0,155 | 3567 | 0 |
| sole scritture | immediata | 4 | 64 | 2818 | 133,3 | 326 | 20,6 | 45,5 | 183,0 | 18,2 | 0,071 | 2391 | 0 |
| sole scritture | immediata | 4 | 256 | 2661 | 135,3 | 411 | 89,4 | 143,9 | 171,5 | 22,2 | 0,073 | 2246 | 0 |
| 50/50 | immediata | 4 | 1 | 438 | 27,7 | 84 | 2,7 | 5,3 | 220,2 | 0,5 | 0,504 | 2854 | 0 |
| 50/50 | immediata | 4 | 4 | 1150 | 71,6 | 112 | 3,3 | 9,0 | 329,8 | 0,2 | 0,287 | 4274 | 0 |
| 50/50 | immediata | 4 | 16 | 2406 | 122,1 | 173 | 6,2 | 15,7 | 261,8 | 9,6 | 0,113 | 3403 | 0 |
| 50/50 | immediata | 4 | 64 | 2828 | 127,2 | 220 | 20,7 | 56,1 | 180,8 | 24,9 | 0,073 | 2369 | 0 |
| 50/50 | immediata | 4 | 256 | 2563 | 134,7 | 339 | 92,5 | 146,7 | 180,3 | 17,3 | 0,077 | 2354 | 0 |
| sole scritture | adattiva | 4 | 1 | 265 | 26,5 | 84 | 3,5 | 6,2 | 265,6 | 0,5 | 1,003 | 3443 | 0 |
| sole scritture | adattiva | 4 | 4 | 553 | 37,6 | 118 | 6,7 | 11,4 | 140,6 | 0,2 | 0,254 | 1822 | 0 |
| sole scritture | adattiva | 4 | 16 | 1600 | 95,3 | 196 | 9,4 | 18,7 | 165,4 | 11,3 | 0,110 | 2155 | 0 |
| sole scritture | adattiva | 4 | 64 | 2839 | 129,4 | 311 | 20,4 | 44,0 | 114,8 | 14,5 | 0,046 | 1503 | 0 |
| sole scritture | adattiva | 4 | 256 | 2803 | 137,1 | 413 | 85,6 | 145,2 | 106,7 | 23,0 | 0,046 | 1406 | 0 |
| 50/50 | adattiva | 4 | 1 | 430 | 27,0 | 81 | 2,7 | 5,4 | 217,3 | 0,5 | 0,506 | 2816 | 0 |
| 50/50 | adattiva | 4 | 4 | 986 | 59,2 | 120 | 3,9 | 10,8 | 166,9 | 0,2 | 0,170 | 2163 | 0 |
| 50/50 | adattiva | 4 | 16 | 2387 | 122,5 | 171 | 6,1 | 16,8 | 159,2 | 7,8 | 0,070 | 2072 | 0 |
| 50/50 | adattiva | 4 | 64 | 2985 | 132,4 | 225 | 19,6 | 53,2 | 110,9 | 24,9 | 0,045 | 1462 | 0 |
| 50/50 | adattiva | 4 | 256 | 2686 | 138,4 | 342 | 88,7 | 140,8 | 109,2 | 16,9 | 0,047 | 1433 | 0 |
| sole scritture | rilassata | 4 | 1 | 1078 | 54,3 | 244 | 0,9 | 1,7 | 0,8 | 0,8 | 0,001 | 11 | 0 |
| sole scritture | rilassata | 4 | 4 | 2232 | 125,1 | 340 | 1,5 | 4,7 | 3,5 | 14,7 | 0,008 | 60 | 0 |
| sole scritture | rilassata | 4 | 16 | 2514 | 133,4 | 366 | 5,8 | 14,4 | 3,7 | 15,1 | 0,007 | 63 | 0 |
| sole scritture | rilassata | 4 | 64 | 2135 | 125,0 | 416 | 27,7 | 54,7 | 3,6 | 14,7 | 0,009 | 62 | 0 |
| sole scritture | rilassata | 4 | 256 | 1981 | 128,2 | 508 | 121,4 | 221,1 | 3,8 | 11,9 | 0,008 | 62 | 0 |
| 50/50 | rilassata | 4 | 1 | 1187 | 46,7 | 220 | 0,8 | 1,6 | 0,6 | 0,5 | 0,001 | 8 | 0 |
| 50/50 | rilassata | 4 | 4 | 2609 | 126,4 | 261 | 1,3 | 3,8 | 0,9 | 0,8 | 0,001 | 12 | 0 |
| 50/50 | rilassata | 4 | 16 | 2614 | 138,8 | 297 | 5,3 | 16,3 | 2,5 | 33,8 | 0,014 | 67 | 0 |
| 50/50 | rilassata | 4 | 64 | 2362 | 139,1 | 314 | 24,9 | 46,4 | 2,6 | 11,9 | 0,006 | 46 | 0 |
| 50/50 | rilassata | 4 | 256 | 2336 | 140,3 | 394 | 102,0 | 166,3 | 0,8 | 0,8 | 0,001 | 11 | 0 |
| sole scritture | log locale | 4 | 1 | 593 | 43,2 | 173 | 1,5 | 3,5 | 0,5 | 0,5 | 0,002 | 7 | 0 |
| sole scritture | log locale | 4 | 4 | 1581 | 120,6 | 269 | 2,2 | 6,0 | 3,3 | 10,2 | 0,009 | 53 | 0 |
| sole scritture | log locale | 4 | 16 | 2222 | 138,3 | 323 | 6,4 | 16,3 | 1,3 | 1,6 | 0,001 | 19 | 0 |
| sole scritture | log locale | 4 | 64 | 2246 | 138,2 | 382 | 26,0 | 56,6 | 5,1 | 26,1 | 0,014 | 93 | 0 |
| sole scritture | log locale | 4 | 256 | 2030 | 142,5 | 496 | 116,3 | 216,5 | 3,6 | 14,9 | 0,009 | 62 | 0 |
| 50/50 | log locale | 4 | 1 | 767 | 41,2 | 125 | 1,1 | 3,2 | 0,5 | 0,5 | 0,001 | 7 | 0 |
| 50/50 | log locale | 4 | 4 | 1977 | 116,8 | 192 | 1,8 | 5,0 | 0,6 | 0,3 | 0,000 | 8 | 0 |
| 50/50 | log locale | 4 | 16 | 2295 | 139,6 | 256 | 6,0 | 19,0 | 2,6 | 36,1 | 0,017 | 71 | 0 |
| 50/50 | log locale | 4 | 64 | 2371 | 139,9 | 315 | 24,8 | 47,7 | 2,5 | 10,2 | 0,005 | 43 | 0 |
| 50/50 | log locale | 4 | 256 | 2271 | 142,8 | 416 | 104,1 | 183,9 | 0,9 | 1,0 | 0,001 | 12 | 0 |

**Cosa limita, dalle misure:**
- **Letture.** Si fermano a ~12 000/s, con il motore a ~92% di un core su 2 CPU e ~107% su 4. Con 4 CPU non salgono. Il limite sembra il client Python unico (un processo) o il percorso per connessione del motore; non l'ho separato (**non misurato**).
- **Scritture.** Si fermano a ~2 500-3 400/s in tutte le modalità, con il motore a ~125-145% di un core anche con 4 CPU disponibili. Quindi:
  - il limite è il percorso di scrittura di un singolo database, che è serializzato (un solo scrittore slatedb, una coda di commit);
  - non sono le CPU, né S3: in modalità rilassata le richieste S3 sono quasi zero e il tetto è lo stesso.
- **La latenza cresce con le connessioni oltre il tetto.** A 256 connessioni p50 ~80-120 ms, senza più throughput.
- **Effetto del raggruppamento:**
  - in modalità immediata le richieste S3 per richiesta scendono da 1,0 (1 connessione) a 0,06 (64 connessioni), perché le scritture concorrenti entrano nello stesso flush;
  - l'adattiva le porta a 0,04-0,05 e dimezza i PUT a 4 connessioni (da 315 a 133 PUT/s);
  - a 64 connessioni è comunque ~1 200-2 400 $/mese per un database che scrive 2 500 volte al secondo, contro 50-90 $ in modalità rilassata o log locale.
- **Anomalia, non risolta:** log locale, sole scritture, 2 CPU, a 64 e 256 connessioni.
  - Il throughput crolla a 629 e 119 req/s, con p99 di 28 s e CPU del motore sotto il 33%.
  - Non si ripete con 4 CPU, né con 50/50.
  - Sembra un'attesa sul `fdatasync` di gruppo con molti scrittori, ma la causa è **non misurata**.

Domanda: un secondo processo in sola lettura può servire le letture senza il lease di scrittura? La risposta è nella sottosezione qui sotto. Non l'ho costruito, quindi i numeri sono **non misurati**.

### Un secondo processo in sola lettura

Dal codice (non costruito, non misurato):
- **Cosa c'è.** Il motore apre già ogni database come `DbReader` di slatedb in modalità `FollowLatest`, e passa a scrittore alla prima scrittura (`src/lifecycle.rs`). Un `DbReader` con `skip_wal_replay = false` ricarica il manifest a ogni `manifest_poll_interval` e **riapplica i WAL nuovi** (`maybe_replay_new_wals` in slatedb 0.17). Un secondo processo può quindi seguire lo scrittore con un ritardo di circa un intervallo di poll più un flush del WAL, senza toccare i dati dello scrittore: legge solo oggetti immutabili.
- **Cosa manca.**
  1. Il motore chiede il lease anche per aprire un lettore (`Entry::ensure_open` → `leases.acquire`). Un secondo processo viene rifiutato con 134. Serve una modalità "solo lettura senza lease".
  2. Il poll del lettore oggi è di 1 ora (`LONG_POLL`). Per leggere dati freschi deve scendere a 1-10 s, e ogni poll costa 1 LIST + 1 GET più le letture dei WAL nuovi. Il costo di questi poll è **non misurato**.
  3. La pulizia SST degli scrittori cancella un SST non più nel manifest dopo `--sst-gc-min-unref-secs` (120 s): il poll del lettore deve restare molto sotto, altrimenti legge oggetti già cancellati.
  4. Instradamento delle letture `secondary` verso quei processi: un replica set con membri secondari annunciati, oppure un nome host di sola lettura.
- **Conclusione:** fattibile con un lavoro medio. Le letture avrebbero un ritardo di qualche secondo (come un secondario di MongoDB), non dati freschi quanto quelli del proprietario.

## Vicino rumoroso

**Cosa è stato fatto** (`mongo-server/src/fair.rs`):
- **Nessuna quota sul traffico normale.**
- **Coda equa tra database**, attiva solo quando il server è saturo.
  - Saturo = il ritardo dello scheduler, misurato da un timer di 10 ms, supera `--fair-lag-ms` (10 ms).
  - Mentre è saturo, i comandi in esecuzione sono al massimo `--fair-max-inflight` (default 4 per CPU). Gli altri aspettano in una coda: prima i risvegli di database chiusi, poi un comando per database a turno.
- **Limiti di sicurezza per database, molto alti:** `--db-max-inflight 10000` comandi contemporanei e `--db-max-rate 100000` req/s.
  - Oltre il limite il comando riceve l'errore MongoDB 462 `IngressRequestRateLimitExceeded`, con etichetta `SystemOverloadedError`.
  - Il rifiuto incrementa una metrica.
- **Metriche per database:** `GET /v1/metrics/databases` sulla porta admin dà, per ogni database:
  - richieste e req/s;
  - CPU dei comandi;
  - memoria stimata;
  - attese in coda (quante, totale e massimo);
  - rifiuti.
  - Test: `ctl-tests/test_21_fair_limits_metrics.py`, 4 test verdi.

**Test: 50 pesanti + 1 leggero, come scritto** (`tools/round3/noisy.py`, MinIO).
- Motore fissato su **1 CPU**; client pesanti su altre 2, client leggero su 1.
- 5, 10, 25 e 50 database pesanti, ciascuno con 8 connessioni in ciclo chiuso (50/50).
- 1 database leggero a 2 req/s.
- 90 s per gradino.
- "off" = coda spenta. "on" = coda accesa con il default. "on2" = soglia di saturazione a 2 ms.
- Motore con la coda equa e il tetto dei comandi (`8eab560`).

| coda | pesanti | pesanti req/s | pesanti p50 / p99 ms | leggero p50 / p99 / max ms | attese in coda del leggero (max ms) | comandi messi in coda | secondi saturi | ritardo scheduler medio / max ms | CPU del motore % di 1 core | errori |
|---|---|---|---|---|---|---|---|---|---|---|
| spenta | 5 | 1870 | 9,4 / 28,8 | 8,8 / 30,5 / 61,3 | 0 (0) | 0 | 0,0 | 0,7 / 1,8 | 80,1 | 0 |
| spenta | 10 | 1992 | 18,7 / 55,3 | 23,8 / 50,4 / 96,4 | 0 (0) | 0 | 0,0 | 2,0 / 5,2 | 86,3 | 0 |
| spenta | 25 | 1621 | 52,7 / 198,0 | 68,6 / 125,1 / 280,4 | 0 (0) | 0 | 0,0 | 3,7 / 9,4 | 88,8 | 0 |
| spenta | 50 | 1504 | 125,2 / 303,6 | 69,0 / 216,1 / 362,4 | 0 (0) | 0 | 0,0 | 8,0 / 15,5 | 91,8 | 0 |
| accesa (10 ms) | 5 | 1795 | 9,7 / 29,4 | 8,5 / 24,1 / 74,5 | 1 (15) | 63 | 0,1 | 1,2 / 6,3 | 80,3 | 0 |
| accesa (10 ms) | 10 | 1812 | 19,4 / 61,3 | 25,2 / 74,7 / 144,5 | 0 (0) | 169 | 0,1 | 2,1 / 6,8 | 82,3 | 0 |
| accesa (10 ms) | 25 | 1790 | 50,4 / 173,3 | 65,4 / 159,4 / 210,0 | 0 (0) | 3 | 0,0 | 2,7 / 6,1 | 90,1 | 0 |
| accesa (10 ms) | 50 | 1596 | 115,1 / 323,9 | 73,6 / 157,4 / 341,9 | 9 (133) | 8322 | 3,2 | 6,8 / 14,1 | 91,4 | 0 |
| accesa (2 ms) | 5 | 1713 | 9,9 / 31,5 | 11,5 / 54,5 / 194,9 | 5 (9) | 1953 | 1,7 | 0,8 / 2,3 | 79,4 | 0 |
| accesa (2 ms) | 10 | 1936 | 18,3 / 60,4 | 19,4 / 60,5 / 101,4 | 13 (16) | 12616 | 7,7 | 1,2 / 2,6 | 83,3 | 0 |
| accesa (2 ms) | 25 | 1422 | 63,4 / 183,3 | 59,0 / 181,1 / 184,4 | 32 (45) | 27540 | 16,1 | 1,9 / 4,0 | 89,9 | 0 |
| accesa (2 ms) | 50 | 1235 | 153,6 / 369,0 | 86,7 / 263,4 / 361,4 | 63 (205) | 45616 | 25,8 | 4,6 / 14,0 | 86,1 | 0 |

Una prima versione della coda, senza il tetto dei comandi, peggiorava il leggero: p99 410 ms con 50 pesanti (`noisy.json`). Per questo è stato aggiunto il tetto.

**Dove satura il server** (1 CPU):
- il throughput totale dei pesanti si ferma a ~1 800-2 000 req/s già con 10 database pesanti;
- la CPU del motore è all'80% con 5 pesanti e al 90% con 25;
- oltre, crescono solo le latenze.

**Quale segnale è più affidabile per scalare:**
- **CPU.** Arriva all'80-90% e poi resta piatta mentre le latenze raddoppiano. Dice "sono vicino al limite", ma non quanto oltre.
- **Ritardo dello scheduler.** Cresce con il carico: da 0,7 a 8 ms di media senza coda, sempre in modo monotono. È il segnale che separa meglio 10, 25 e 50 pesanti, e non dipende dal client. È un buon segnale di scala.
- **p99 dei pesanti.** Cresce (55 → 198 → 304 ms), ma dipende dal tipo di richiesta e da S3, quindi è rumoroso tra carichi diversi.
- **Attese in coda.** Ci sono solo con la coda accesa e solo quando è già saturo: segnale di allarme, non di scala.
- **Raccomandazione dalle misure:** scalare su CPU sopra il 60-70% **oppure** ritardo dello scheduler medio sopra 2 ms. In queste misure il ritardo superava 2 ms già con 10 pesanti, quando la CPU era all'82-86%.
- **Effetto della coda equa:** con 50 pesanti il p99 del leggero passa da 216 ms (spenta) a 157 ms (accesa). Aiuta, ma **non risolve**: con un server saturo anche il leggero aspetta. La soluzione vera è avere margine (scalare prima).

**Non misurato:**
- la coda equa con più CPU;
- con carichi di sole letture;
- su S3 vero.

## E. S3 vero (GitHub Action, eu-west-1)

**Come.**
- `.github/workflows/round3-s3.yml` con il ruolo OIDC esistente: solo S3, nessuna EC2, nessun altro servizio.
- Credenziali solo dall'ambiente di Actions; nessuna chiave nel codice, nei log o nei file. I log passano per `tools/round3/s3/scan.py`, che cerca chiavi e token prima del commit.
- **Bucket nuovo per ogni run:**
  - tag `project=fenvix`, `purpose=round3-s3-test`, `run=<id>`;
  - regola di scadenza a 1 giorno;
  - cancellato alla fine, con verifica che non esista più.
- **Il motore gira sul runner GitHub**, fuori da AWS: ogni richiesta a S3 attraversa Internet, quindi le latenze di scrittura in modalità immediata sono ~100 ms e non i ~10-20 ms di un server nella stessa regione. Latenze da EC2 nella regione: **non misurate**.
- **Metodo** come `results-aws-test3.md` e come D:
  - 5-10 minuti per passo, con la previsione del costo prima di ogni passo (il passo parte solo se la previsione resta sotto lo stop);
  - costo dal vivo ogni minuto in `cost_live.csv` e nel log: `spesa 0,12 / 10,00 USD (PUT 9312, LIST 211, GET 3104; passo s3-immediate-r10)`;
  - stop automatico a 8 USD.
- **La spesa è una stima per eccesso, non le richieste contate × i prezzi.** Il driver prende i contatori del motore (PUT, LIST, GET) e i LIST del runner, li moltiplica per 1,1, aggiunge 200 richieste per passo e conta 2 giorni di storage, ai prezzi di eu-west-1 (0,0054 $ per 1000 PUT/LIST, 0,00043 $ per 1000 GET). La fattura AWS è **non misurata**: non è accessibile da qui.

**Run:**

| run | motore | passi | errori | spesa calcolata | bucket | stop |
|---|---|---|---|---|---|---|
| 1 (`40967620447`) | `1abbea0`, client con 4 connessioni | 20 | 10 000 a 100 req/s: limite del client, non del motore | 0,72 USD | cancellato e verificato | nessuno |
| 2 (`40987254437`) | `ea63a08` | 20 (tutto il piano) | 0 | 0,70 USD | cancellato e verificato | nessuno |
| 3 (`41011005224`) | `8cfe8f6` (memtable forzata, poi scartata) | 10 | 0 | 0,26 USD | cancellato e verificato | nessuno |
| 4 (`41018700920`) | `bca96f6` (memtable 2 MB, poi scartata) | 10, l'ultimo non partito | 0 | 0,34 USD | **non cancellato** (BucketNotEmpty), cancellato dal run 5: cancellato e verificato | nessuno |
| 5 (`41026488028`) | `83cd51f` (prima della revisione) | 12 | 0 | 0,28 USD | cancellato e verificato | nessuno |
| 6 (`41056543332`) | `4677caf`, motore finale dopo la revisione | 7 | 0 | 0,22 USD | cancellato e verificato | nessuno |
| **totale** | | | | **2,52 USD (somma dei valori arrotondati dei 6 run)**, su 10 USD | | |

Il run 1 non ha potuto fare il push dei risultati: mancava l'identità git nel workflow. I riassunti dei passi sono stati recuperati dal log del job (`results-round3/real-s3-run1/`). Run 2 ha ripetuto tutto con 64 connessioni per client.

### E1. Run 2: tutto il piano (motore `ea63a08`)

**$ di richieste per database al mese (S3 vero)**

| scenario | prima (main) | immediata | adattiva | rilassata 5 s | log locale 30 s |
|---|---|---|---|---|---|
| 1 req/s, 50/50 | 11,65 | 8,69 | 8,55 | 4,26 | 2,78 |
| 10 req/s, 50/50 | 63,39 | 56,85 | 60,00 | 4,35 | 2,63 |
| 100 req/s, 50/50 | 139,86 | 134,12 | 133,70 | 4,53 | 2,79 |
| flotta 100 database × 1 scrittura/s | 17,35 | 12,62 | 12,53 | 2,63 | 1,13 |

**Richieste S3 per richiesta del client (S3 vero)**

| scenario | prima (main) | immediata | adattiva | rilassata 5 s | log locale 30 s |
|---|---|---|---|---|---|
| 1 req/s, 50/50 | 1,807 | 0,765 | 0,822 | 0,438 | 0,391 |
| 10 req/s, 50/50 | 0,557 | 0,462 | 0,474 | 0,036 | 0,024 |
| 100 req/s, 50/50 | 0,118 | 0,107 | 0,106 | 0,004 | 0,003 |
| flotta 100 database × 1 scrittura/s | 2,068 | 1,037 | 1,030 | 0,246 | 0,134 |

**Scritture p50 ms (S3 vero)**

| scenario | prima (main) | immediata | adattiva | rilassata 5 s | log locale 30 s |
|---|---|---|---|---|---|
| 1 req/s, 50/50 | 96,7 | 96,8 | 92,4 | 0,9 | 1,4 |
| 10 req/s, 50/50 | 99,4 | 101,1 | 98,6 | 0,9 | 1,4 |
| 100 req/s, 50/50 | 218,2 | 224,9 | 223,9 | 0,7 | 1,1 |
| flotta 100 database × 1 scrittura/s | 94,4 | 95,2 | 94,4 | 0,7 | 1,1 |

**Scritture p99 ms (S3 vero)**

| scenario | prima (main) | immediata | adattiva | rilassata 5 s | log locale 30 s |
|---|---|---|---|---|---|
| 1 req/s, 50/50 | 185,8 | 182,3 | 293,4 | 33,6 | 3,0 |
| 10 req/s, 50/50 | 304,0 | 329,4 | 288,4 | 1,1 | 2,6 |
| 100 req/s, 50/50 | 361,1 | 393,2 | 383,9 | 1,1 | 2,6 |
| flotta 100 database × 1 scrittura/s | 190,9 | 190,3 | 190,1 | 1,0 | 2,7 |

**Letture p50 ms (S3 vero)**

| scenario | prima (main) | immediata | adattiva | rilassata 5 s | log locale 30 s |
|---|---|---|---|---|---|
| 1 req/s, 50/50 | 1,1 | 1,1 | 1,1 | 1,1 | 1,1 |
| 10 req/s, 50/50 | 1,1 | 1,1 | 1,0 | 1,0 | 1,0 |
| 100 req/s, 50/50 | 0,8 | 0,9 | 0,8 | 0,8 | 0,8 |
| flotta 100 database × 1 scrittura/s | non misurato | non misurato | non misurato | non misurato | non misurato |

**Letture p99 ms (S3 vero)**

| scenario | prima (main) | immediata | adattiva | rilassata 5 s | log locale 30 s |
|---|---|---|---|---|---|
| 1 req/s, 50/50 | 136,6 | 108,9 | 103,0 | 101,5 | 301,7 |
| 10 req/s, 50/50 | 1,2 | 1,3 | 1,2 | 1,3 | 1,2 |
| 100 req/s, 50/50 | 1,3 | 1,5 | 1,3 | 1,3 | 1,3 |
| flotta 100 database × 1 scrittura/s | non misurato | non misurato | non misurato | non misurato | non misurato |

**Errori dei client (S3 vero)**

| scenario | prima (main) | immediata | adattiva | rilassata 5 s | log locale 30 s |
|---|---|---|---|---|---|
| 1 req/s, 50/50 | 0 | 0 | 0 | 0 | 0 |
| 10 req/s, 50/50 | 0 | 0 | 0 | 0 | 0 |
| 100 req/s, 50/50 | 0 | 0 | 0 | 0 | 0 |
| flotta 100 database × 1 scrittura/s | 0 | 0 | 0 | 0 | 0 |

**Storage $ per database al mese (S3 vero, listing del bucket a fine passo)**

| scenario | prima (main) | immediata | adattiva | rilassata 5 s | log locale 30 s |
|---|---|---|---|---|---|
| 1 req/s, 50/50 | 0,000015 | 0,000015 | 0,000015 | 0,000015 | 0,000015 |
| 10 req/s, 50/50 | 0,000068 | 0,000069 | 0,000067 | 0,000065 | 0,000069 |
| 100 req/s, 50/50 | 0,000568 | 0,000579 | 0,000576 | 0,000582 | 0,000593 |
| flotta 100 database × 1 scrittura/s | 0,000021 | 0,000021 | 0,000021 | 0,000020 | 0,000021 |

### E2. Run 3: memtable forzata ogni 256 scritture (scartato)

| passo | $/db/mese | PUT/min/db | GET/min/db | S3 per richiesta | scrittura p50 / p99 ms | lettura p50 / p99 ms | errori |
|---|---|---|---|---|---|---|---|
| s3f-fleet100-immediate | 12,53 | 57,8 | 2,7 | 1,032 | 125,0 / 288,7 | non misurato / non misurato | 0 |
| s3f-fleet100-local | 1,24 | 5,5 | 3,4 | 0,149 | 0,8 / 21,7 | non misurato / non misurato | 0 |
| s3f-fleet100-relaxed | 2,65 | 12,1 | 2,7 | 0,252 | 0,5 / 0,8 | non misurato / non misurato | 0 |
| s3f-immediate-r10 | 61,51 | 278,0 | 84,7 | 0,607 | 162,0 / 502,3 | 0,9 / 145,4 | 0 |
| s3f-local-r1 | 2,80 | 12,0 | 12,0 | 0,402 | 1,3 / 1,6 | 0,9 / 186,8 | 0 |
| s3f-local-r10 | 7,61 | 28,3 | 86,0 | 0,179 | 1,3 / 118,2 | 0,9 / 137,4 | 0 |
| s3f-local-r100 | 7,66 | 28,2 | 90,5 | 0,020 | 0,9 / 1981,5 | 0,5 / 2179,5 | 0 |
| s3f-relaxed-r1 | 4,33 | 19,3 | 8,7 | 0,461 | 0,8 / 0,9 | 0,9 / 142,0 | 0 |
| s3f-relaxed-r10 | 9,17 | 36,0 | 80,7 | 0,193 | 0,9 / 96,9 | 0,9 / 133,9 | 0 |
| s3f-relaxed-r100 | 9,18 | 35,7 | 85,6 | 0,020 | 0,5 / 6381,3 | 0,6 / 6689,9 | 0 |

Rispetto al run 2:
- a 10 e 100 req/s la modalità rilassata costa 9,2 $ invece di 4,4;
- il p99 delle scritture sale a 6,4 s (rilassata, 100 req/s) e a 2,0 s (log locale).
- Per questo lo scarico forzato è stato tolto (A3).

### E3. Run 4: memtable 2 MB (scartata)

| passo | $/db/mese | PUT/min/db | GET/min/db | S3 per richiesta | scrittura p50 / p99 ms | lettura p50 / p99 ms | errori |
|---|---|---|---|---|---|---|---|
| s3g-fleet100-immediate | 12,79 | 58,9 | 3,3 | 1,052 | 141,2 / 339,6 | non misurato / non misurato | 0 |
| s3g-fleet100-relaxed | 2,73 | 12,4 | 3,4 | 0,267 | 0,6 / 1,1 | non misurato / non misurato | 0 |
| s3g-immediate-r10 | 54,62 | 252,6 | 2,9 | 0,429 | 184,8 / 469,4 | 0,9 / 1,1 | 0 |
| s3g-local-r1 | 2,76 | 12,0 | 9,7 | 0,361 | 1,2 / 86,9 | 0,9 / 225,5 | 0 |
| s3g-local-r10 | 2,63 | 12,0 | 2,3 | 0,024 | 1,2 / 159,8 | 0,9 / 1,0 | 0 |
| s3g-local-r100 | 8,11 | 29,8 | 96,3 | 0,021 | 1,0 / 2678,5 | 0,6 / 2937,3 | 0 |
| s3g-relaxed-r1 | 4,39 | 19,7 | 8,0 | 0,461 | 0,9 / 43,8 | 0,9 / 150,6 | 0 |
| s3g-relaxed-r10 | 4,35 | 20,0 | 2,0 | 0,037 | 0,8 / 1,1 | 0,9 / 1,1 | 0 |
| s3g-relaxed-r100 | 9,81 | 37,6 | 97,3 | 0,023 | 0,6 / 2116,8 | 0,6 / 2412,2 | 0 |

Passo non eseguito: s3g-fleet100-local (**non misurato** in questo run).

- **Rispetto al run 2:** a 100 req/s la modalità rilassata e il log locale hanno p99 delle scritture di 2,1 e 2,7 s, e costano 9,81 e 8,11 $/mese (run 2: 1,1 e 2,6 ms, 4,53 e 2,79 $). Con la memtable da 2 MB il database scarica L0 molto spesso e aspetta le compattazioni. Le GET salgono a ~97 al minuto per database: sono le letture delle compattazioni. Per questo il default è tornato a 8 MB.
- **L'ultimo passo (flotta 100, log locale) non è partito.**
  - Al riavvio dopo il precaricamento, il motore ha controllato il log locale dei 100 database uno alla volta: 123 s, mentre lo script aspetta la porta al massimo 120 s.
  - Lo script non ha fermato quel motore. Ha continuato a scrivere nel bucket durante la pulizia, quindi la cancellazione del bucket è fallita (`BucketNotEmpty`).
  - **Corretto:**
    - il ripristino del log locale controlla 32 database alla volta (kill -9 con 16 database: 19 291 scritture confermate, 0 perse);
    - lo script ferma un motore che non parte;
    - il run 5 ha cancellato il bucket rimasto (cancellato e verificato).

### E4. Run 5: motore prima della revisione

| passo | $/db/mese | PUT/min/db | GET/min/db | S3 per richiesta | scrittura p50 / p99 ms | lettura p50 / p99 ms | errori |
|---|---|---|---|---|---|---|---|
| s3h-fleet100-immediate | 12,79 | 58,9 | 3,1 | 1,045 | 116,1 / 258,3 | non misurato / non misurato | 0 |
| s3h-fleet100-local | 1,03 | 4,5 | 3,2 | 0,130 | 0,9 / 1,6 | non misurato / non misurato | 0 |
| s3h-fleet100-relaxed | 2,69 | 12,2 | 2,5 | 0,246 | 0,6 / 0,9 | non misurato / non misurato | 0 |
| s3h-immediate-r1 | 8,49 | 38,7 | 8,0 | 0,671 | 117,5 / 212,5 | 0,9 / 142,5 | 0 |
| s3h-immediate-r10 | 59,59 | 275,6 | 2,9 | 0,467 | 135,9 / 341,8 | 0,8 / 1,0 | 0 |
| s3h-immediate-r100 | 111,65 | 516,6 | 3,6 | 0,089 | 267,8 / 584,4 | 0,8 / 1,1 | 0 |
| s3h-local-r1 | 2,67 | 11,7 | 8,7 | 0,333 | 1,3 / 1,6 | 1,0 / 349,0 | 0 |
| s3h-local-r10 | 2,64 | 12,0 | 2,9 | 0,025 | 1,2 / 1,5 | 0,8 / 1,0 | 0 |
| s3h-local-r100 | 2,79 | 12,7 | 3,6 | 0,003 | 1,0 / 1,5 | 0,7 / 1,1 | 0 |
| s3h-relaxed-r1 | 4,17 | 18,3 | 12,3 | 0,548 | 0,8 / 114,1 | 0,9 / 123,8 | 0 |
| s3h-relaxed-r10 | 4,36 | 20,0 | 2,3 | 0,036 | 0,7 / 0,9 | 0,8 / 1,1 | 0 |
| s3h-relaxed-r100 | 4,54 | 20,8 | 3,6 | 0,004 | 0,7 / 1,0 | 0,8 / 1,2 | 0 |

### E5. Run 6: motore dopo la revisione (`4677caf`)

Rifà i passi che le correzioni toccano:
- la pulizia del WAL ora legge il manifest salvato: 1 GET per versione del manifest;
- il lease di processo;
- il log locale;
- un GET dell'impostazione di durabilità a ogni apertura.

| passo | $/db/mese | PUT/min/db | GET/min/db | S3 per richiesta | scrittura p50 / p99 ms | lettura p50 / p99 ms | errori |
|---|---|---|---|---|---|---|---|
| s3i-fleet100-immediate | 12,63 | 58,3 | 2,7 | 1,036 | 96,3 / 200,6 | non misurato / non misurato | 0 |
| s3i-fleet100-local | 1,07 | 4,7 | 3,5 | 0,139 | 1,0 / 1,7 | non misurato / non misurato | 0 |
| s3i-immediate-r1 | 8,60 | 38,9 | 11,0 | 0,853 | 92,9 / 220,7 | 1,0 / 103,0 | 0 |
| s3i-immediate-r10 | 60,85 | 281,5 | 2,6 | 0,472 | 107,1 / 334,7 | 0,9 / 1,2 | 0 |
| s3i-immediate-r100 | 130,34 | 603,2 | 3,2 | 0,102 | 229,1 / 403,4 | 0,7 / 1,3 | 0 |
| s3i-local-r10 | 2,72 | 12,3 | 3,0 | 0,026 | 1,2 / 1,8 | 0,9 / 1,2 | 0 |
| s3i-relaxed-r10 | 4,44 | 20,3 | 3,0 | 0,041 | 0,8 / 1,1 | 0,9 / 1,3 | 0 |

**Non misurato su S3 vero:**
- la flotta mista;
- sole letture e sole scritture (solo 50/50);
- kill -9;
- riavvio con 500 database;
- tetto di un database;
- vicino rumoroso.

## Test

`tools/round3/regression.sh final /tmp/claude-0/final7-bin` sul binario finale (`bca96f6`). Ogni suite è confrontata con `mongod` 7.0 (standalone e replica set).

| parte | binario | risultato |
|---|---|---|
| suite differenziale pymongo contro mongod 7.0 | `4677caf` | 4127 su 4127 passati |
| suite differenziale driver Node contro mongod 7.0 | `4677caf` | 2200 su 2200 passati |
| test ctl | `4677caf` | non eseguito |
| test ctl con autenticazione | `4677caf` | non eseguito |
| test SIGKILL con controlli negativi (0) | `4677caf` |  |

Sul binario precedente (`4677caf`) due test ctl erano falliti:
- `test_03b` (controllo negativo senza retention): la regressione della pulizia WAL, corretta in `4677caf`;
- `test_02_fork` (2002 byte contro un limite di 2000), con la memtable da 2 MB.

Sul binario finale passano entrambi (`test_02_fork` 3 volte su 3).

**Test esistenti modificati:**
- solo `ctl-tests/test_08b_pin_cost.py`: aggiunto `--process-lease false`, perché misura apposta il lease per database (A2).

**Test nuovi:**
- `ctl-tests/test_20_process_lease.py` (9);
- `ctl-tests/test_21_fair_limits_metrics.py` (4).

## Problemi trovati

1. **La memtable di slatedb tiene ~4,3 KB di memoria per 1 KB scritto, ma ne conta ~1,1 KB.** Non si può chiedere a slatedb di scaricarla prima di 4096 flush del WAL. Con molti database scritti lentamente la memoria cresce finché ogni memtable arriva alla sua dimensione (vedi i run lunghi in A3). Va corretto in slatedb o con un limite globale sui database aperti in scrittura.
2. **Scaricare la memtable spesso blocca le scritture su S3 vero.** Lo scarico forzato ogni 256 scritture dava p99 fino a 6,4 s; una memtable da 2 MB dava 2,1-2,7 s a 100 req/s. Entrambi tolti, il default è 8 MB.
3. **La coda equa aiuta poco su un server saturo.** Senza il tetto dei comandi peggiorava il database leggero. L'unica protezione vera è il margine.
4. **500 risvegli insieme su 2 CPU:** la prima richiesta arriva in 3-16 s. Il limite ai risvegli contemporanei sposta il ritardo tra mediana e coda, non lo toglie.
5. **Anomalia del log locale** (sole scritture, 2 CPU, 64+ connessioni): crollo a 119-629 req/s. Causa non trovata.
6. **Il tetto di scrittura di un database (~3 000 req/s su MinIO) non sale con le CPU.** Superarlo richiede di dividere il database (sharding).
7. **Lo storage per database su MinIO non è stato misurato correttamente** (prefisso sbagliato nello script). Su S3 vero è misurato: da 0,00002 a 0,0006 $/database/mese per i passi brevi.
8. **I contatori `tenantStats` non vedono i retry interni di `object_store`.** Su S3 vero il conteggio è quindi un minimo.
9. **Il LIST dei file d'uso cresce con la storia.** Il flush dei contatori d'uso fa un LIST del prefisso `usage/` per database. Ogni 300 s invece di 60 il costo scende, ma resta proporzionale ai file presenti.
10. **Il run 1 su S3 vero** aveva un limite nel client (4 connessioni, ~100 ms per scrittura): 10 000 errori a 100 req/s. Rifatto con 64 connessioni.
11. **Il ripristino del log locale controllava i database uno alla volta** (123 s per 100 database su S3). Corretto: 32 alla volta.
12. **Un motore che non partiva in tempo restava acceso** e ha impedito la cancellazione del bucket del run 4. Corretto nello script; il bucket è stato cancellato dal run 5.
13. **Regressione trovata dai test e corretta.** La nuova pulizia del WAL teneva il WAL dal checkpoint più vecchio anche senza retention. Così `test_03b` (controllo negativo: senza retention il ripristino va rifiutato) falliva. Ora il limite del checkpoint vale solo con la retention.
14. **Misure sporcate e rifatte:**
    - due misure MinIO contaminate da un altro client sullo stesso MinIO;
    - una misura del vicino rumoroso con un motore vecchio rimasto sulla porta. Ora `Engine.start` rifiuta una porta occupata.
    - un disco MinIO pieno;
    - `_id` duplicati riusando un bucket. Ora gli id sono unici per run.

## Quale motore ha prodotto quale misura

| misura | motore |
|---|---|
| D (MinIO, tutte le tabelle per scenario) | `7fb4f5a` (A1-C) |
| kill -9, tetto di un database, S3 run 2 | `ea63a08` (codice del motore uguale da `67ef6bf` a `51164e5`) |
| S3 run 3 | `8cfe8f6` (memtable forzata, scartata) |
| vicino rumoroso con tetto, riavvio con 500 database | `8eab560` (coda equa con tetto; memtable forzata ancora presente) |
| S3 run 4, memoria con memtable 2 MB, suite dei driver | `bca96f6` (memtable 2 MB) |
| S3 run 5 | `83cd51f` (motore finale tranne la correzione della pulizia WAL senza retention, che non tocca questi passi: hanno la retention) |
| memoria con 8 MB | `b72262e` |
| kill -9 del log locale con 16 database | `f034e36` |
| correzioni della revisione e loro test | binari intermedi e di controllo, vedi la sezione "Revisione indipendente" e `results-round3/review/` |
| tutti i test (suite dei driver, ctl, kill, loadtest, unit), kill -9 finale, S3 run 6 | `4677caf` (motore finale dopo la revisione, sha256 `d811978b…`) |

Le parti A1, A2, B e C non sono cambiate dopo `7fb4f5a`.
- A3: dopo `7fb4f5a` è cambiato il valore della memtable (8, poi 2, poi di nuovo 8). Lo scarico forzato è stato aggiunto in `0bf5bbd` e tolto in `bca96f6`.
- A1: in `b72262e` il limite del checkpoint più vecchio vale solo per i database con una finestra di retention (senza, il comportamento è quello di prima: `test_03b`).
- A4: dopo `7fb4f5a` è cambiato il default dei risvegli, da 0 a 128.
- Le misure D delle modalità immediata, adattiva, rilassata e log locale sono quindi valide per il motore finale, tranne la memoria.

**Non misurato:** D ripetuto su MinIO con il motore finale. Al suo posto c'è S3 run 5, sugli stessi passi in modalità 50/50 e flotta 100.

## Revisione indipendente (PR #1): correzioni

Ogni correzione ha un test che **fallisce senza la correzione**:
- su un binario di controllo, cioè lo stesso codice con solo quella correzione spenta;
- oppure su un binario precedente, dove indicato.

Le uscite dei test, con il binario usato, sono in `results-round3/review/`. Gli iniettori di guasti sono solo per i test e vanno attivati con variabili d'ambiente:
- `MONGO_TEST_FAULTS_FILE`: PUT o GET bloccati, ritardati o falliti per percorso, e una risposta trattenuta dopo l'fsync del log locale;
- `MONGO_TEST_WALL_CLOCK_OFFSET_MS`;
- `MONGO_TEST_LOCALLOG_FAIL_AT`;
- `MONGO_TEST_LOCALLOG_NO_REPLAY`.

### Bloccanti

| punto | esito | correzione | test, e risultato senza la correzione |
|---|---|---|---|
| **B1**, pulizia WAL dal manifest in memoria | **corretto** | Il confine viene dal manifest **salvato** della versione dello scrittore: 1 GET per versione, i manifest non cambiano. Il bug era nuovo di round3: `main` leggeva il manifest salvato. | `test_22`: PUT del manifest bloccato, passate della pulizia, kill -9, riavvio. Controllo: 252 oggetti WAL cancellati, **900 su 900 scritture confermate perse**. Con la correzione: 0 cancellati, 0 perse. |
| B1, nessun controllo del lease | **corretto** | La passata non fa nulla se il processo non possiede più il database. | Nessun test può fallire: con il confine dal manifest salvato, un processo fenced cancella solo WAL sotto un confine salvato, che il nuovo proprietario non usa. È una difesa in più. |
| B1, retention appena attivata | **corretto** | Con la retention attiva e nessun punto di ripristino ancora preso, la pulizia non cancella nulla. | Nessun test può fallire. Il WAL sotto il confine salvato non serve al primo punto di ripristino, che parte da quel confine o da uno successivo. È una scelta prudente. |
| **B2**, rinnovo tardivo del lease di processo | **corretto** | Quattro modifiche: (1) chi prende un database revoca prima il lease di processo del vecchio proprietario (scrittura condizionale); (2) un rinnovo arrivato dopo la scadenza fa abbandonare tutti i database; (3) un solo rinnovo alla volta; (4) scadenze su CLOCK_BOOTTIME. | `test_23`: **(a)** rinnovi in ritardo di 3 s con ttl di 4 s: P1 non serve mai dopo la presa di P2. Passa anche sul codice com'era: lì un rinnovo riuscito lascia il lease scaduto finché il successivo non abbandona. **(b)** Rinnovi in ritardo di 1 s: sul codice com'era P1 **ha lasciato il database 4 volte**, perché due rinnovi contemporanei si annullavano a vicenda. Ora 0. **(c)** Orologio di P2 avanti di 20 s: senza revoca P1 **ha servito il valore vecchio per tutti i 12 s**, con la revoca l'ultima lettura vecchia è 1,3 s dopo la presa. |
| B2, solo con orologi sincronizzati | **in parte non d'accordo** | Il testo della revisione dice che un rinnovo lento rende `valid()` di nuovo vero. Con orologi uguali non succede: `valid_until` resta `inizio + ttl` e il rinnovo dopo vede la scadenza (test (a)). Il buco esiste con orologi diversi o con una VM sospesa, cioè i casi (c) e CLOCK_BOOTTIME. | La sospensione di una VM con CLOCK_BOOTTIME non si può simulare in questo container: **non misurato**. |
| **B3a**, numeri di sequenza da 1 a ogni avvio | **corretto** | Numero di avvio salvato in `BOOT`, nei bit alti della sequenza. | `test_24 a`: controllo **10 scritture su 10 perse**, corretto 0. |
| **B3b**, scrittura o fsync fallito | **corretto** | Fail-stop: il processo si ferma. | `test_24 b` (disco pieno a metà record): controllo **302 scritture confermate dopo l'errore, tutte perse**; corretto 0 confermate dopo l'errore, 0 perse. |
| **B3c**, replay sopra un nuovo scrittore | **corretto** | Ogni record porta l'epoca dello scrittore; il replay è rifiutato se il manifest salvato ha un'epoca più nuova. | `test_24 c`: controllo **documento sovrascritto (v=1 invece di 2) e indice sbagliato**; corretto v=2 e indice coerente. |
| **B3d**, nessun controllo dopo l'fsync | **corretto** | Dopo l'fsync, se il lease non vale più la scrittura non è confermata. | `test_24 d` (processo congelato tra fsync e risposta, poi preso da un altro): controllo **conferma una scrittura che non esiste**; corretto risponde con un errore. |

### Da correggere

| punto | esito | test |
|---|---|---|
| Replay del log locale prima di `ctl::start` | **corretto** | `test_24 e`: controllo con la porta admin attiva durante il replay; corretto no. |
| Un database non blocca il riavvio | **corretto**: errore o nessun progresso per `--local-log-replay-timeout-secs` (120 s); i record di quel database restano per il riavvio dopo | `test_24 f`: controllo, il riavvio si ferma; corretto, il riavvio finisce e le 5 scritture vengono applicate al riavvio dopo. |
| Coda equa prima dell'autenticazione, contatori per nomi arbitrari | **corretto**: contatori solo per i database aperti o scritti e solo dopo l'autorizzazione | `test_25`: controllo con **200 voci** da comandi non autenticati e 200 da database inesistenti; corretto 0. |
| Erase senza `forget_wal_state` e senza togliere i contatori | **corretto** | `test_25`: controllo, il database ricreato non pulisce mai il suo WAL (0 oggetti) e tiene i contatori; corretto 505 oggetti e nessun contatore. |
| Ritorno a `--process-lease false` | **corretto**: la modalità per database rispetta il lease di processo nominato dal record | `test_25`: controllo, **database bloccato per sempre**; corretto, preso dopo 5,6 s. |
| Impostazione rilassata caricata solo all'avvio | **corretto**: riletta a ogni apertura (1 GET); la cambia solo il proprietario (409 altrimenti) | `test_25`: controllo, **20 scritture confermate prima di essere durevoli** dopo aver spento la modalità; corretto 0. |
| `--writer-poll-secs` < `--sst-gc-min-unref-secs` | **corretto** (controllo all'avvio) | `test_25`: il controllo partiva lo stesso. |

### Minori

| punto | esito | test o motivo |
|---|---|---|
| Ordine di scrittura del log locale | **in parte**. Con il fail-stop una scrittura con fsync fallito non viene mai confermata né resa durevole: il processo si ferma prima di caricarla. Resta visibile ai lettori dello stesso processo fino all'arresto. Applicarla dopo l'fsync obbligherebbe ad aspettare l'fsync con il lock del database, una scrittura alla volta per database. | Documentato nel README; `test_24 b`. |
| Abandon e record di drop | **documentato**: quando la proprietà è persa il vecchio proprietario è fenced e non può più caricare; le sue scritture solo locali sono perse | README |
| `setDefaultRWConcern {w:1, j:false}` | **corretto**: conta solo un write concern inviato dal client | `test_25`: controllo con **20 conferme rilassate** di scritture senza write concern; corretto 0. |
| Commento di `pitr.rs` | **corretto** | - |
| `--unsafe-skip-ownership-checks` | **corretto**: richiede `MONGO_ALLOW_UNSAFE_TEST_FLAGS=1` | `test_25`: il controllo partiva senza. |
| README | **corretto**: memtable e `max_unflushed_bytes` 32 MiB, contatori d'uso persi fino a 300 s, modalità del lease e ritorno indietro, interruttori di test, garanzie del log locale | - |

### Correzioni del report

- Tabella iniziale: run diversi e un run per cella, run 2 a 100 req/s -4% e non -20%.
- Spesa: stima per eccesso, somma dei run arrotondati.
- Latenze a regime e primo minuto.
- Registro condiviso: corretto il "5-100 volte".
- Kill -9: controllo negativo, cosa non verifica, binario di ogni file.
- Uscite dei test salvate in `results-round3/tests/`.
