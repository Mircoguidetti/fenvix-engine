# results-round3: meno richieste S3 per un database attivo (branch `round3`)

**DRAFT: sezioni con i numeri in arrivo segnate [IN CORSO].**

Branch `round3` da `main` (`c9a4beb`). PR aperta verso `main`, non unita. `infra/` e `control-plane/` non esistono in questo repository e non sono stati toccati.

## Sintesi

[IN CORSO]

## A. Correzioni

### A1. Pulizia del WAL con retention: costo indipendente dalla storia

**Prima.** Con `--wal-gc-mode bulk` e una finestra di retention, ogni 20 s per ogni database scritto la pulizia cadeva sul passaggio di slatedb:
- elenca tutta la cartella `wal/` e legge i manifest (circa 37 GET e 3 LIST a passata in `results-density.md`);
- non cancella nulla per 7 giorni;
- cresce con la storia, perché gli oggetti WAL tenuti aumentano.

**Ora** (`src/lifecycle.rs`, `gc_tenant_wal` / `gc_wal_with`):
- **Cosa si cancella.** Un oggetto WAL `n` serve finché `n >= replay_after_wal_id` del database vivo, oppure finché un checkpoint ha un confine di ripresa `<= n`. I punti di ripristino sono checkpoint, quindi con la retention si tiene tutto il WAL dal punto più vecchio in poi: è esattamente quello che un ripristino a un istante può usare. Sotto il checkpoint più vecchio nessun ripristino può partire, e quegli oggetti si cancellano.
- **Zero richieste per decidere.** La passata legge la vista del manifest che lo scrittore ha già in memoria. La vista può essere indietro (al massimo `--writer-poll-secs`) solo per checkpoint presi nel frattempo, e un checkpoint nuovo ha un confine almeno uguale a quello della vista, perché i confini crescono con la versione. Quindi niente di ciò che gli serve è sotto il limite usato.
- **Confine del checkpoint più vecchio:** 1 GET, una volta per checkpoint.
- **Oggetto WAL più basso:** 1 LIST una volta per database e processo. Su S3 le chiavi sono ordinate, quindi basta la prima chiave della prima pagina.
- **Cancellazione:** 1 DeleteObjects ogni 1000 oggetti.
- **Una passata che non ha nulla di nuovo da cancellare non fa richieste.** Il costo dipende da quanti oggetti diventano cancellabili, non dalla storia.
- **Pulizia SST e manifest degli scrittori aperti.** Prima girava ogni 60 s. Ora gira solo dopo una compattazione di questo processo, dopo 16 versioni nuove del manifest, o se la passata precedente ha lasciato oggetti in attesa dell'età minima. Con checkpoint presenti salta senza una seconda lettura del manifest.
- **Pulizia dopo la chiusura.**
  - WAL con la stessa regola: 1 LIST + 1 GET del manifest.
  - Il passaggio completo di slatedb (manifest, SST compattate, stati di compattazione), che legge il manifest di ogni checkpoint, solo dopo una compattazione o 16 versioni del manifest.
- **Test esistenti che coprono il ripristino** (`test_09f_wal_retention_guard`, `test_03*`): verdi. Tutti i ripristini di 7 stati in ordine casuale sono esatti anche con la nuova regola in modalità bulk.
- **Contatori** in `tenantStats`: `walGc.noop`, `sstGc.noop`, `sstGc.closeLight`.

**Misura** (MinIO, 10 database, 1 richiesta/s 50/50, 300 s, retention 7 giorni):
- **Prima:** 11,2 LIST e 44 GET al minuto per database, tra pulizia e lease.
- **Dopo:** 150 passate della pulizia WAL, **150 senza alcuna richiesta**; 50 passate SST, tutte senza richieste. LIST 0,42/min e GET 7,5/min per database: il resto sono letture dei client e la lettura periodica del manifest.

### A2. Un lease per processo (`--process-lease`, ora attivo per default)

- **Il meccanismo esisteva già** (`mongo-server/src/lease.rs`).
  - Il processo rinnova un solo oggetto, `processes/<holder>/_lease`, ogni ttl/3.
  - Un database aperto ha un record del proprietario, scritto all'apertura e non rinnovato, che nomina il lease del processo e la sua generazione.
  - Se il lease del processo scade (processo congelato), tutti i database della generazione vecchia vengono abbandonati prima di poter tornare validi.
- **Ora è il default.** `--process-lease false` torna al lease per database.
- **Prima:** 6 PUT al minuto per ogni database aperto (1,3 $/database/mese solo per stare aperto).
- **Dopo:** 6 PUT al minuto per processo, qualunque sia il numero di database.
- **Test dedicato** `ctl-tests/test_20_process_lease.py`: 9 test, tutti verdi.
  - Rifiuto (134) per letture e scritture mentre il proprietario è vivo, anche dopo più ttl.
  - **Proprietario congelato** (SIGSTOP) oltre il ttl: l'altro processo subentra; il proprietario ripreso ha letture e scritture rifiutate (134); i dati finali contengono solo le scritture valide.
  - **Controllo negativo:** con `--unsafe-skip-ownership-checks` il processo ripreso serve dati vecchi (la lettura non vede la scrittura dell'altro). È questo che il controllo impedisce. La scrittura tardiva la ferma comunque il fencing di slatedb, la seconda linea di difesa.
  - **SIGKILL:** l'altro aspetta che il lease del processo scada, poi legge tutte le 200 scritture confermate.
  - Lo stesso holder che riparte riprende subito i suoi database (non dopo il ttl).
  - 40 thread che scrivono un database nuovo attraverso i due processi: nessuna scrittura confermata persa.
  - 20 database aperti e fermi: rinnovi e PUT di lease ≤ 8 in 12 s, contro ~120 con il lease per database.
  - **Modalità miste** sullo stesso bucket, nei due versi: ciascun processo rifiuta i record vivi dell'altro.
- **Un test esistente modificato:** `test_08b_pin_cost`. Misura apposta il costo del lease per database ("un PUT ogni 10 s"); ora chiede quella modalità con `--process-lease false`. Nessun'altra modifica ai test esistenti.

### A3. Memoria

**Perché la RSS cresceva di ~4,4 KB per scrittura da 1 KB.** Misurato, `tools/round3/memgrowth.py`: 20 database, 10 insert/s ciascuno, MinIO.

| binario e opzioni | KB di RSS per scrittura | RSS a fine run |
|---|---|---|
| prima (main) | **4,44** | 299 MB a 300 s, cresce |
| prima, memtable da 1 MB (`--l0-sst-bytes`) | 4,29 | 290 MB, cresce: **non è la memtable** |
| dopo, log dei change stream limitato a 32 MB | 1,98 | 159 MB a 300 s, cresce ancora |
| dopo + memtable 1 MB | 1,57 | 227 MB a 600 s, cresce |
| dopo + memtable 1 MB + cache da 8+4 MB | **0,74 su tutto il run, 0 dopo ~200 s** | **si ferma a ~131 MB** |

Tre cause:
1. **Il log degli eventi dei change stream** (`mongo-server/src/changes.rs`) teneva 100.000 eventi con il documento intero, anche senza nessun change stream aperto: circa 2,5 KB per scrittura, fino a ~400 MB.
   - Ora c'è anche un limite di memoria stimata: `--change-log-max-mb 32`.
   - Un token di ripresa più vecchio dell'evento più vecchio tenuto dà `ChangeStreamHistoryLost`, come prima oltre i 100.000 eventi.
2. **La cache dei blocchi di slatedb, da 512 MB per ogni database aperto** (più 128 MB di metadati), senza limite complessivo. Si riempie anche con i blocchi dei dati scritti, cioè i flush.
   - Ora c'è una sola cache condivisa: `--block-cache-mb 256` e `--meta-cache-mb 64`.
   - Ogni apertura ha uno scope nuovo, così un database cancellato o riscritto da un altro processo non viene mai servito dalla cache.
3. **La memtable da 64 MB per database.** Ora è `--memtable-max-mb 8`, e le scritture si fermano oltre 4x (`max_unflushed_bytes`).

**Tetto della memoria del processo, per costruzione:**
- cache: 256 + 64 MB;
- log dei change stream: 32 MB;
- memtable: 8 MB per database aperto in scrittura;
- connessioni: ~150 KB ciascuna (`results-density.md`).

Run lungo: [IN CORSO].

### A4. `--max-concurrent-wakes` e `LimitNOFILE`

- **Default ora 32** (prima 0, senza limite).
- Riavvio con 500 database: [IN CORSO].
- **`LimitNOFILE`:** un descrittore per connessione client, più qualche decina del motore.
  - Regola: `LimitNOFILE` >= 2 x connessioni per database x database aperti + 1024.
  - Per un server fino a 2000 database con driver da 2-4 connessioni: **65536**, che è il valore già messo da `fenvix-app/infra` (`user-data.ts`).

### Altri default cambiati

- **`--writer-poll-secs` da 30 a 60.** Ogni lettura del manifest di uno scrittore aperto costa 1 LIST e 1 GET.
- **`--usage-flush-secs` da 60 a 300.** Un PUT per database attivo a ogni flush.

## B. Modalità di scrittura

### B1. Raggruppamento adattivo (`--adaptive-group-commit true`, spento per default)

- Il primo scrittore di un gruppo aspetta metà del tempo medio di flush del database, al massimo 5 ms.
- Aspetta solo se altre scritture sono attese entro quella finestra (intervallo medio ≤ 2 finestre) o sono già in arrivo.
- Una scrittura isolata non aspetta mai.
- Il raggruppamento naturale c'era già: le scritture che arrivano mentre un flush è in corso vanno nel PUT seguente.

### B2. Durabilità rilassata per database

Semantica esatta (anche nel README):
- **Condizioni per la conferma prima della durabilità.** Deve valere tutto:
  - comando di scrittura (insert, update, delete, findAndModify, bulk);
  - write concern `{w: 1, j: false}`, `{j: false}`, oppure `{w: 0}` senza `j: true`;
  - fuori da una transazione;
  - database con `relaxed_flush_ms` diverso da 0.
- **Come si accende:** `PUT /v1/databases/{db}/durability {"relaxed_flush_ms": 1000..10000}` sulla porta admin, oppure `--relaxed-flush-ms` per tutti i database. Default spento.
- **Conferma:** quando la scrittura è applicata in memoria. È visibile subito a tutti i lettori del server: le letture di un database rilassato vedono anche le scritture in attesa, come mongod con `j: false`.
- **Durabilità:** lo scrittore del database fa il flush del WAL da solo ogni `relaxed_flush_ms`. La scrittura è durevole entro quel tempo più un PUT.
- **Restano durevoli prima della conferma, come oggi:**
  - `j: true`, `w: "majority"`;
  - una scrittura senza write concern, cioè il default dei driver;
  - il commit di una transazione.
  - Queste rendono durevoli anche le scritture rilassate fatte prima di loro.
- **Chiusura e arresto.** Una chiusura per inattività o un arresto pulito fanno il flush.
- **Se il processo muore prima del flush:** si perdono le scritture rilassate dell'intervallo, e un lettore può averle viste.
- Cambiare l'impostazione chiude il database se è aperto, perché il suo scrittore riparta con il nuovo intervallo.
- **Correzione trovata strada facendo:** un comando che scrive in più database ($out, renameCollection) aspettava solo l'ultimo commit. Ora aspetta l'ultimo di ogni database.

## C. Prototipo: log locale durevole (`--local-log-dir`, spento per default)

`mongo-server/src/locallog.rs`.
- **Scrittura.** La scrittura si applica in memoria, poi il record va nel log locale. Un solo `fdatasync` copre tutte le scritture arrivate nel frattempo, di tutti i database. La conferma arriva dopo l'fsync.
- **Caricamento su S3.** Ogni `--local-log-upload-secs` (30-60) ogni database scritto fa il flush del suo WAL su S3: 1 PUT per tutte le scritture dell'intervallo. Un segmento del log si cancella quando tutti i suoi record sono su S3.
- **Riavvio.** Prima di accettare connessioni, il motore riapplica in modo durevole i record non ancora su S3.
  - Ogni batch porta un marcatore (`sys/local-log` = id del log e numero del record), quindi nessun record è applicato due volte.
  - Un record finale strappato da un crash viene ignorato: non era stato confermato.
  - Un drop o un erase del database, o la perdita della proprietà, scartano i record precedenti.
  - Se il marcatore nomina un altro log, i record non vengono applicati e i file restano come `*.conflict`.
- **Cosa si perde se si perde il disco:** tutte le scritture confermate dopo l'ultimo caricamento di ogni database, cioè fino a `--local-log-upload-secs` di scritture di **tutti** i database del processo.
- **Altro limite.** Finché il riavvio sullo stesso disco non è fatto, un altro processo che prende il database (dopo la scadenza del lease) non vede quelle scritture. Se quel processo non è in modalità log locale, il ritorno del disco vecchio può sovrascrivere le sue scritture successive. È un confronto con misure, non un default.

## D. Misure su MinIO

[IN CORSO]

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
- **Con pochi database molto attivi** (20 con 4-5 attivi) la rilassata e il log locale per database costano 5-100 volte meno, perché raggruppano già molte scritture per PUT.

**Cosa renderebbe più difficile:**
- **Ripristino di un solo database.** Il suo WAL è sparso in oggetti condivisi con gli altri database del server. Un ripristino a un istante deve leggere e filtrare i registri di tutti, e la retention di 7 giorni la paga tutto il server.
- **Esportazione:** possibile solo dopo la materializzazione dei suoi dati.
- **Cancellazione di un cliente.** I suoi dati restano negli oggetti condivisi finché non scadono o non vengono riscritti senza di lui. Non basta più cancellare un prefisso, e non si verifica con un listing: è un problema per GDPR e contratti.
- **Isolamento tra clienti.** Un oggetto condiviso danneggiato, o un errore nel filtro per database, tocca tutti i database del server. Cifrare con una chiave per cliente diventa difficile.
- **Latenza dei vicini rumorosi.** Un database che scrive tanto ingrandisce ogni PUT condiviso e allunga la conferma di tutti.
- **Spostamento di un database su un altro server.** Prima va materializzato, poi rilasciato: lo spostamento si allunga e dipende dal registro del server vecchio.

## Tetto di UN SOLO database (MinIO)

[IN CORSO]

### Letture dello stesso database da un secondo processo in sola lettura

Dal codice:
- **Cosa c'è.** Il motore apre già ogni database come `DbReader` di slatedb in modalità `FollowLatest`, e passa a scrittore alla prima scrittura (`src/lifecycle.rs`). Un `DbReader` con `skip_wal_replay = false` ricarica il manifest a ogni `manifest_poll_interval` e **riapplica i WAL nuovi** (`maybe_replay_new_wals` in slatedb 0.17). Un secondo processo può quindi seguire lo scrittore con un ritardo di circa un intervallo di poll più un flush del WAL, senza toccare i dati dello scrittore: legge solo oggetti immutabili.
- **Cosa manca.**
  1. Il motore chiede il lease anche per aprire un lettore (`Entry::ensure_open` → `leases.acquire`). Un secondo processo viene rifiutato con 134. Serve una modalità "solo lettura senza lease".
  2. Il poll del lettore oggi è di 1 ora (`LONG_POLL`). Per leggere dati freschi deve scendere a 1-10 s, e ogni poll costa 1 LIST + 1 GET più le letture dei WAL nuovi. A 1 s sono circa 5 milioni di richieste al mese per database letto così (~13 $/mese); a 10 s circa 1,3 $ **(stima)**.
  3. La pulizia SST degli scrittori cancella un SST non più nel manifest dopo `--sst-gc-min-unref-secs` (120 s): il poll del lettore deve restare molto sotto, altrimenti legge oggetti già cancellati.
  4. Instradamento delle letture `secondary` verso quei processi: un replica set con membri secondari annunciati, oppure un nome host di sola lettura.
- **Conclusione:** fattibile con un lavoro medio. Le letture avrebbero un ritardo di qualche secondo (come un secondario di MongoDB), non dati freschi quanto quelli del proprietario.

## Vicino rumoroso

[IN CORSO]

## E. S3 vero

[IN CORSO]

## Test

[IN CORSO]

## Problemi trovati

[IN CORSO]
