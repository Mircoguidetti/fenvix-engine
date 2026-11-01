# Rapporto perf-letture: letture del motore contro mongod 8.0, per core

**Le letture del motore sono più veloci del 48% sul database grande e del 65% sui 100 database piccoli, ma le soglie non sono raggiunte.** Su 1 core il motore fa 0,31 volte le letture puntuali di mongod su 1 database da 200.000 documenti (soglia 0,5) e 0,67 volte su 100 database da 2.000 (soglia 0,8) (MISURATO i valori, STIMATO i rapporti). Il limite rimasto è nello strato MongoDB ma non si può togliere senza toccare `mongo-server/src/dbs.rs`: ogni lettura fa ancora 3 letture di metadati su SlateDB oltre al documento, e valgono il 59% della CPU sul database grande e il 35% sui 100 piccoli (MISURATO, profilo). Mi sono fermato lì, come chiedono le regole; il cambio proposto è descritto più sotto. Con quel cambio il motore arriverebbe a circa 0,77 e 1,0 volte mongod (STIMATO).

Ogni numero è etichettato: MISURATO (misurato su questa macchina in questo lavoro) o STIMATO (calcolato o dedotto da numeri misurati). Dati grezzi in `bench/perf/results/raw.jsonl`, profili in `bench/perf/results/prof-*.layers.txt`, suite ctl-tests in `bench/perf/results/ctl-final/`.

## Macchina e metodo

- VM con 4 vCPU Intel Xeon (Cascade Lake), 15 GB di RAM, nessuno swap, disco virtio con ext4. La macchina è condivisa: il tempo rubato dall'hypervisor alla CPU 0 è stato tra lo 0,7% e l'1,7% in ogni run (MISURATO da `/proc/stat`).
- Motore: `mongo-server` in release. "main" è il commit `658c724` (con jemalloc), "branch" è questo branch. Opzioni: `--store local --local-dir DIR`, il resto è il default di main (stessa scelta di bench-mongod).
- mongod 8.0 dall'immagine Docker ufficiale `mongo:8.0`, rete host, opzioni di default.
- Il database gira sulla CPU 0 (1 core). Il generatore di carico (lo stesso programma Go di bench-mongod, driver ufficiale v2) gira sulle CPU 1-3 e ha usato al massimo 1,2 core (MISURATO): non è lui il limite.
- Dati: 1 database da 200.000 documenti da circa 1 KB (`{_id: int64, f: stringa di 1.000 caratteri}`), oppure 100 database da 2.000 documenti.
- Carichi: letture puntuali per `_id`; 50% letture per `_id` e 50% aggiornamenti `$set` per `_id`. Write concern `{w:1, j:true}`. Chiavi e database scelti a caso in modo uniforme.
- Dati del motore: caricati una volta per layout e salvati come snapshot della directory dello store; ogni run del motore parte da una copia dello snapshot, quindi tutte le run (e tutti i binari) partono dagli stessi dati. mongod viene caricato da zero a ogni gruppo di run.
- Caricamento del database grande: in un colpo solo si blocca per sempre (lo stallo delle scritture in correzione su fix-stall): 6 tentativi su 6 con 4 client e 6 su 6 con 1 client, sempre fra 100.000 e 120.000 documenti (MISURATO). A blocchi da 10.000 documenti con 12 s di pausa fra i blocchi non si è bloccato (MISURATO, 1 caricamento su 1). Non ho lavorato sullo stallo.
- Numero di client: il punto di saturazione degli sweep da 20 s (1, 2, 4 ... 32 client). Motore: 8 client per le letture sul database grande, 16 sui 100 piccoli, 4 e 16 per letture+aggiornamenti. mongod: 8, 8, 16, 16.
- Misura finale: per ogni cella e ogni server 3 run da 60 s di riscaldamento e 60 s di misura (`bench/perf/final.sh`), riporto la mediana. CPU del server letta da `/proc/PID/stat`. In tutte le run il server era al 97-99% del suo core (MISURATO), quindi le operazioni al secondo sono anche le operazioni per core.
- Misura di ogni correzione: run alternate binario vecchio / binario nuovo (A, B, A, B ...), 3 per parte, 30 s + 30 s, mediana (`bench/perf/ab.py`). Fra due run dello stesso binario il risultato varia fino al 10-20% (MISURATO): una correzione sotto il 5% non si separa dal rumore.
- Profili: `perf record -F 299 --call-graph dwarf` per 30 s sul processo del motore dopo 30 s di carico, nomi Rust decodificati con rustfilt (`bench/perf/profile.sh`). Ogni campione è dato a uno strato guardando tutto lo stack di chiamate (`bench/perf/layers.py`): SlateDB se nello stack c'è una funzione di slatedb, foyer o object_store (compresi malloc, memcpy e decompressione fatti per suo conto); altrimenti MongoDB se c'è una funzione di mongo_server o bson (compresi socket, malloc e copie fatte per suo conto); il resto è rete o runtime.

## Risultati prima e dopo (1 core)

Mediana di 3 run da 60 s dopo 60 s di riscaldamento (`bench/perf/final.sh`). "main" è il motore di main con jemalloc, "branch" è il motore di questo branch. Le operazioni al secondo sono MISURATE; i rapporti con mongod sono STIMATI (calcolati dalle mediane).

### Letture puntuali per `_id`, 1 database da 200.000 documenti

- main: 4.271 op/s (run: 3.945, 4.271, 4.420), p50 1,72 ms, p99 4,42 ms, 8 client.
- branch: 6.319 op/s (run: 6.209, 6.319, 6.379), p50 1,20 ms, p99 2,51 ms, 8 client. **+48%** rispetto a main.
- mongod: 20.105 op/s (run: 17.773, 20.182, 20.105), p50 0,36 ms, p99 1,23 ms, 8 client.
- Rapporto con mongod: **da 0,21 a 0,31** (soglia 0,5).

### Letture puntuali per `_id`, 100 database da 2.000 documenti

- main: 7.584 op/s (run: 6.639, 7.929, 7.584), p50 1,91 ms, p99 5,25 ms, 16 client.
- branch: 12.490 op/s (run: 12.490, 12.074, 12.720), p50 1,18 ms, p99 3,28 ms, 16 client. **+65%** rispetto a main.
- mongod: 18.549 op/s (run: 18.529, 18.549, 18.912), p50 0,40 ms, p99 1,09 ms, 8 client.
- Rapporto con mongod: **da 0,41 a 0,67** (soglia 0,8).

### 50% letture e 50% aggiornamenti, 1 database da 200.000 documenti

- main: 1.891 op/s (run: 1.815, 1.891, 1.901), p50 1,95 ms, p99 5,60 ms, 4 client.
- branch: 2.043 op/s (run: 2.037, 2.055, 2.043), p50 1,76 ms, p99 5,38 ms, 4 client. **+8%** rispetto a main.
- mongod: 6.764 op/s (run: 7.156, 5.594, 6.764), p50 2,18 ms, p99 6,26 ms, 16 client; mongod usa il 94% del core, perché con j:true aspetta il flush del journal (MISURATO).
- Rapporto con mongod: da 0,28 a 0,30 per core; da 0,27 a 0,29 per secondo di CPU usata.

### 50% letture e 50% aggiornamenti, 100 database da 2.000 documenti

- main: 3.379 op/s (run: 3.493, 3.379, 3.210), p50 4,35 ms, p99 11,58 ms, 16 client.
- branch: 3.942 op/s (run: 3.324, 3.942, 4.159), p50 3,60 ms, p99 12,22 ms, 16 client. **+17%** rispetto a main.
- mongod: 6.454 op/s (run: 6.411, 6.454, 6.660), p50 2,28 ms, p99 6,63 ms, 16 client, 95% del core.
- Rapporto con mongod: da 0,52 a 0,61 per core; da 0,50 a 0,58 per secondo di CPU usata.

### Differenze con bench-mongod

- mongod qui fa circa il doppio delle letture misurate in bench-mongod (20.105 contro 9.137 op/s sul database grande, MISURATO in entrambi i lavori, su macchine diverse). Non ho trovato la causa: stessa immagine Docker, stesso generatore, stesse opzioni. Anche il motore di main va più veloce che allora (4.271 contro 2.286), in parte per jemalloc. Per questo i rapporti di partenza qui (0,21 e 0,41) sono più bassi di quelli di bench-mongod (0,25 e 0,66): conta il rapporto misurato sulla stessa macchina nella stessa sessione.
- Il motore parte da uno snapshot caricato a blocchi (vedi sopra), mongod da un caricamento normale.
- Le letture + aggiornamenti scrivono: per il motore lo store locale crea un file per ogni oggetto WAL (vedi bench-mongod), e quel costo del file system è nel numero.

## Profilo per strato

### Prima (main con jemalloc), quota dei campioni (MISURATO)

- Letture, 1 database grande: SlateDB 68,9%, MongoDB 29,4%, rete e runtime 1,7%.
- Letture, 100 database piccoli: SlateDB 50,0%, MongoDB 48,5%, rete e runtime 1,4%.
- Letture + aggiornamenti, 1 database grande: SlateDB 73,0%, MongoDB 22,3%, rete e runtime 4,7%.
- Letture + aggiornamenti, 100 database: SlateDB 60,1%, MongoDB 36,5%, rete e runtime 3,4%.

Il 18% dentro SlateDB del profilo di bench-mongod era il tempo proprio delle sole funzioni di slatedb; qui conto anche malloc, copie e cache fatte per conto di SlateDB, e il quadro cambia: sulle letture SlateDB è più della metà del costo.

Dentro SlateDB, per conto di quale lettura dello strato MongoDB (letture, 1 database grande, MISURATO):
- elenco delle collezioni, letto per sapere se il nome è una vista: 16,3%;
- opzioni della collezione lette una seconda volta, da una seconda vista, solo per sapere se la collezione è "clustered": 13,8%;
- definizione degli indici: 12,8%;
- opzioni della collezione (collation di default): 11,6%;
- il documento richiesto: 8,6%;
- il resto (stack tagliati, lettura di blocchi dopo un miss della cache): 5,9%.

Quindi ogni find per `_id` faceva 5 letture su SlateDB, e solo una era il documento. Sulle letture + aggiornamenti si aggiunge il costo della scrittura: sul database grande il 28,3% della CPU è I/O dello store locale (creazione dei file WAL e SST, MISURATO), il 4,5% compaction e il 4,2% flush.

Dentro SlateDB, per tipo di lavoro (letture, 1 database grande, MISURATO): iteratori e blocchi 17,6%, cache dei blocchi e dei filtri 8,8%, malloc/free 8,3%, contatori di riferimento di `Bytes` 6,7%, filtri bloom 5,9%, percorsi degli SST (`format!` e `object_store::Path`) 5,4%, copie di memoria 5,3%, altro 9,4%, letture di file 1,6%.

Dentro lo strato MongoDB (letture, 1 database grande, MISURATO): logica dei comandi 10,5%, kernel per i socket 7,7%, copie di memoria dello stato dei future asincroni 5,6%, malloc/free 2,6%, BSON 2,1%, copie di dati 0,9%. Sui 100 database le copie di stato dei future erano l'11,0%.

### Dopo le correzioni (MISURATO)

- Letture, 1 database grande: SlateDB 76,4%, MongoDB 22,5%, rete e runtime 1,1%. Dentro SlateDB: elenco delle collezioni 22,9%, definizione degli indici 18,5%, opzioni della collezione 17,9%, il documento 12,1%, lettura di blocchi dopo un miss della cache 4,0%. La seconda lettura delle opzioni non c'è più.
- Letture, 100 database: SlateDB 59,5%, MongoDB 39,4%. Dentro SlateDB: elenco delle collezioni 18,7%, il documento 17,7%, indici 8,7%, opzioni 8,0%.
- Letture + aggiornamenti, 1 database grande: SlateDB 76,8% (di cui I/O dello store locale 30,0%), MongoDB 18,7%.
- Letture + aggiornamenti, 100 database: SlateDB 67,0% (di cui I/O dello store locale 21,4%), MongoDB 29,7%.
- Dentro lo strato MongoDB le copie di stato dei future sono scese dal 5,6% all'1,0% (database grande) e dall'11,0% all'1,9% (100 database). Quello che resta (logica dei comandi 9,9% e 18,7%, socket 6,4% e 9,7%, BSON 2,5% e 4,1%, malloc 1,9% e 3,5%) è distribuito su molte funzioni, nessuna con più dell'1,5% della CPU come tempo proprio (MISURATO: controlli dei lease, lock delle viste, mappe di diagnostica, costruzione della risposta).

La quota dello strato MongoDB è scesa poco in percentuale (dal 29,4% al 22,5% sul database grande) perché la CPU totale per lettura è scesa di circa un terzo: in valore assoluto il costo dello strato MongoDB per lettura è sceso di circa metà (STIMATO: 29,4% di 1/4.271 s contro 22,5% di 1/6.319 s).

## Correzioni (un commit ciascuna, in ordine)

1. **`find` legge le opzioni della collezione dalla vista che ha già** (`cmd_data.rs`). Prima apriva una seconda vista dopo la lettura e rileggeva le opzioni da SlateDB solo per sapere se la collezione è clustered. Guadagno: +14,3% letture al secondo sul database grande (3.836 → 4.385), +9,5% sui 100 piccoli (7.200 → 7.886) (MISURATO).
2. **Niente più copie da 10-22 KB di stato asincrono a ogni comando** (`cmd.rs`, `diag.rs`, `js.rs`, `collation.rs`, `views.rs`). Ogni comando passava per una catena di funzioni async e di involucri che contenevano in linea il future più grande di tutti i comandi: `route` 21,6 KB, `dispatch_commands` 22,0 KB, `dispatch_inner` 22,5 KB, il percorso delle viste 19,4 KB, `authorize` 9,0 KB anche con l'autenticazione spenta (MISURATO con un binario di prova e `size_of_val`). Ogni involucro ricopiava tutto. Ora gli involucri sono funzioni semplici che restituiscono il future dello scope, il future di ogni comando va in un Box una sola volta, il lavoro sulle viste vere va in un Box solo quando il nome è una vista, e `authorize` non viene costruito quando l'autenticazione è spenta. Dopo: `route` 792 byte, `dispatch_commands` 960 byte, `dispatch_inner` 1.144 byte, `dispatch` 352 byte (MISURATO). Guadagno: +21,2% sul database grande (4.746 → 5.754), +16,7% sui 100 piccoli (7.883 → 9.200) (MISURATO).
3. **Il ciclo della connessione passa il comando invece di copiarlo** (`main.rs`, `wire.rs`). Il documento del comando veniva copiato per intero a ogni richiesta solo perché la risposta doveva sapere se comprimere; ora quel dato è preso prima (`wire::ReplyTo`). Guadagno: +7,7% sul database grande (5.070 → 5.458), +2,7% sui 100 piccoli (11.215 → 11.521), +2,8% su letture + aggiornamenti con 100 database (3.323 → 3.416) (MISURATO; il binario nuovo vince 8 coppie su 9).
4. **Il registro di `currentOp` condivide il comando invece di copiarlo** (`cmd.rs`, `diag.rs`); la chiave della sessione è scritta in esadecimale in una sola stringa invece che con un `format!` per byte. Nel profilo di main `Diag::begin` valeva lo 0,9% (database grande), il 2,2% (100 database) e l'1,2% (letture + aggiornamenti) della CPU (MISURATO). Il confronto A/B non lo separa dal rumore: −4,4%, +6,3%, +7,3% (MISURATO, coppie divise 5 a 4). Guadagno atteso circa 1-3% (STIMATO dalla quota nel profilo).

Provata e non tenuta: **decodifica dei documenti BSON senza la copia intermedia** di `Document::from_reader`. Nel profilo la copia valeva circa lo 0,2% (MISURATO); il confronto A/B ha dato −2,0%, −10,9%, −5,8% con coppie divise (MISURATO): nessun guadagno visibile, quindi non è nel branch.

Nessun cambio di comportamento visibile: tutta la suite `ctl-tests` passa sul binario finale (38 file su 38, nessun fallimento; `test_04b_export_vs_real_mongo` è saltato perché richiede un mongod di riferimento, come nei rapporti precedenti) (MISURATO, `bench/perf/results/ctl-final/summary.txt`). `src/lifecycle.rs` e `mongo-server/src/dbs.rs` non sono stati toccati.

## Perché le soglie non sono raggiunte, e cosa serve

Database grande: 0,31 contro la soglia 0,5. 100 database piccoli: 0,67 contro la soglia 0,8 (STIMATO, dalle mediane MISURATE). Le correzioni possibili senza toccare dbs.rs sono fatte; quello che resta nello strato MongoDB fuori dalle letture di metadati è distribuito su molte voci piccole (sopra).

### La leva più grande: 3 letture di metadati per comando (serve dbs.rs)

Dopo le correzioni ogni find per `_id` fa ancora 4 letture su SlateDB: elenco delle collezioni, definizione degli indici, opzioni della collezione, documento. Le prime tre valgono il 59,3% della CPU delle letture sul database grande e il 35,4% sui 100 piccoli; il documento solo il 12,1% e il 17,7% (MISURATO, profilo finale). Sulle letture + aggiornamenti valgono il 18,5% (database grande) e il 19,0% (100 database) (MISURATO). I metadati cambiano di rado e sono sempre gli stessi tre valori per collezione. Se fossero letti una volta e poi tenuti in memoria (STIMATO dalle quote del profilo, ammesso che il resto non cambi):
- letture, database grande: circa 15.500 op/s, cioè circa 0,77 volte mongod (soglia 0,5 superata);
- letture, 100 database: circa 19.300 op/s, cioè circa 1,0 volte mongod (soglia 0,8 superata);
- letture + aggiornamenti: circa 2.500 op/s (0,37 volte mongod) sul database grande e 4.900 op/s (0,75) sui 100 database; qui il limite successivo è l'I/O dello store locale (30% e 21% della CPU, MISURATO), che è una scelta di bench (un file per oggetto WAL) e non esiste così su S3.

Il posto giusto per questa cache è `Entry` in `mongo-server/src/dbs.rs`: solo lì passano tutte le scritture (`apply_mode`), le aperture e chiusure del tenant, le perdite di lease, i drop e i ripristini. Una cache fuori da dbs.rs non è sicura, per due motivi:
- con le letture durevoli (il default) una scrittura diventa visibile alle letture solo quando è durevole, dopo che il comando che scrive ha già lasciato il lock; una lettura in quella finestra vede ancora i metadati vecchi, e una cache riempita in quel momento resterebbe sbagliata finché nessuno la svuota;
- il tenant può essere chiuso, perdere il lease, essere ripreso da un altro processo e riaperto: la cache va svuotata a ogni apertura, e questo succede solo dentro dbs.rs.

Cambio proposto, non fatto (le regole chiedono di non toccare dbs.rs; mi fermo qui):
- in `Entry` una mappa protetta da mutex, chiave → valore, solo per le chiavi di metadati (`layout::META_COLLECTIONS`, `layout::index_key(coll)`, `collopts::meta_key(coll)`), condivisa dalle `ReadView` (che oggi hanno solo una cache per vista) e dai comandi con lock esclusivo;
- svuotata in `apply_mode` quando un'operazione tocca una chiave di metadati, e in apertura, chiusura, `abandon`, `drop_database`;
- riempita solo da letture fatte quando nessuna scrittura di metadati del tenant è ancora in attesa di diventare durevole (un contatore incrementato in `apply_mode` e decrementato quando il commit corrispondente è durevole; il segnale di durabilità è il `Commit` di `src/lifecycle.rs`, che basta usare senza modificarlo);
- le transazioni continuano a leggere dal loro snapshot, senza cache.

### Il limite dentro SlateDB

Anche con i metadati in memoria resta una lettura su SlateDB per documento. Sul database grande quella lettura costa di più che sui piccoli, perché SlateDB costruisce un iteratore e risolve il percorso del file per ogni SST che deve guardare: lo snapshot del database grande ha 45 file SST nella directory `compacted` (non tutti per forza ancora in uso), quello di ogni database piccolo 1 (MISURATO, conteggio dei file). Nel profilo di main il lavoro di SlateDB per una lettura è fatto di iteratori e blocchi (17,6% della CPU), cache (8,8%), malloc/free (8,3%), contatori di `Bytes` (6,7%), filtri bloom (5,9%), percorsi degli SST con `format!` e `object_store::Path` (5,4%) (MISURATO). Nel disegno nuovo il nodo d'ingresso leggerà gli SST con un lettore proprio, quindi non ho lavorato dentro `Db::get`.

## Come rifare le misure

```
bench/perf/install.sh                         # mongod (Docker), motore, generatore
BENCH_RESULTS=$HOME/benchdata/results python3 bench/perf/bench.py snapshot 1
BENCH_RESULTS=$HOME/benchdata/results python3 bench/perf/bench.py snapshot 100
cp target/release/mongo-server bench/perf/.bin/mongo-server-final      # e il binario di main come mongo-server-base
BENCH_RESULTS=$HOME/benchdata/results bench/perf/final.sh
BENCH_RESULTS=$HOME/benchdata/results bench/perf/profile.sh read 1 8 30 final
python3 bench/perf/ab.py BIN_A BIN_B          # confronto alternato di due binari
bench/perf/run_ctl.sh OUTDIR                  # suite ctl-tests con MinIO locale
```
