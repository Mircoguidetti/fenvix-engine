# Letture e scritture del motore contro mongod 8.0, per core: cache dei metadati, profilo per strato, piano fino a 1 volta mongod

**Con la cache dei metadati le letture sono circa alla pari con mongod: 0,93 volte sul database grande e 1,16 volte sui 100 database piccoli. Le scritture no: letture + aggiornamenti 0,38 e 0,81, inserimenti 0,36 e 0,56.** Valori su 1 core contro mongod 8.0 sulla stessa macchina (MISURATO i valori, STIMATO i rapporti). La cache (`mongo-server/src/dbs.rs`) da sola ha triplicato le letture sul database grande (+204%, MISURATO). Per le scritture il costo maggiore è nello strato SlateDB e nello store locale del banco di prova (un file per ogni oggetto WAL), che nel motore di oggi non si tolgono con correzioni piccole: il piano qui sotto dice cosa serve, con il guadagno STIMATO di ogni leva, e cosa sparisce con il quaderno del disegno nuovo.

Ogni numero è etichettato: MISURATO (misurato su questa macchina in questo lavoro), STIMATO (calcolato o dedotto da numeri misurati) o NON SO. Dati grezzi in `bench/perf/results/raw.jsonl`, profili in `bench/perf/results/prof-*.layers.txt`, suite ctl-tests in `bench/perf/results/ctl-cache/`.

Analogia della cucina: il cameriere (lo strato MongoDB: comandi, BSON, risposta) prima di ogni piatto tornava in dispensa a rileggere tre fogli che non cambiano quasi mai (l'elenco delle collezioni, gli indici, le opzioni della collezione). Ora li tiene in tasca e li butta solo quando qualcuno li cambia.

# Giro 2: la cache dei metadati e il traguardo 1 volta mongod

## Risultati contro mongod (1 core, dopo la cache)

Mediana di 3 run da 60 s dopo 60 s di riscaldamento (`bench/perf/final.sh`, etichetta `final2`). Ogni server al suo punto di saturazione negli sweep (`bench/perf/results/sweeps-cache.log`). Valori MISURATI; rapporti STIMATI dalle mediane.

### Letture puntuali per `_id`

- 1 database da 200.000 documenti: motore 19.228 op/s (run 18.673, 19.228, 19.727), p50 0,40 ms, p99 0,89 ms, 8 client. mongod 20.692 op/s (run 20.692, 20.904, 20.500), p50 0,36 ms, p99 0,84 ms, 8 client. **Rapporto 0,93.**
- 100 database da 2.000 documenti: motore 21.794 op/s (run 21.463, 21.794, 22.076), p50 0,69 ms, p99 1,63 ms, 16 client. mongod 18.832 op/s (run 18.832, 19.145, 18.140), p50 0,40 ms, p99 0,91 ms, 8 client. **Rapporto 1,16.**

### 50% letture e 50% aggiornamenti

- 1 database: motore 2.681 op/s (run 2.833, 2.681, 2.651), p50 0,65 ms, p99 2,35 ms, 2 client. mongod 7.104 op/s (run 7.053, 7.104, 7.321), p50 2,08 ms, p99 5,57 ms, 16 client, 94% del core. **Rapporto 0,38** (0,37 per secondo di CPU usata).
- 100 database: motore 5.685 op/s (run 5.685, 5.615, 5.686), p50 1,28 ms, p99 3,96 ms, 8 client. mongod 7.000 op/s (run 7.042, 6.970, 7.000), p50 2,12 ms, p99 5,90 ms, 16 client, 95% del core. **Rapporto 0,81** (0,78 per secondo di CPU usata).

### Inserimenti (`insertOne`, `_id` nuovo, j:true)

- 1 database: motore 3.984 op/s (run 4.012, 3.984, 3.967), p50 6,05 ms, p99 35,6 ms, 32 client. mongod 11.041 op/s (run 10.638, 11.041, 11.292), p50 21,0 ms, p99 45,6 ms, 256 client. **Rapporto 0,36.**
- 100 database: motore 3.674 op/s (run 4.295, 3.674, 3.256), p50 7,67 ms, p99 19,8 ms, 32 client. mongod 6.551 op/s (run 9.408, 6.551, 5.171), p50 36,8 ms, p99 121 ms, 256 client. **Rapporto 0,56.** Attenzione: in questa cella entrambi i server calano da una run all'altra (anche mongod, da 9.408 a 5.171, MISURATO); il disco virtuale della macchina sembra rallentare sotto scritture lunghe (STIMATO, causa non verificata). Il rapporto di questa cella è il meno affidabile.

### Rispetto a prima

- Letture, database grande: main di partenza 4.271 op/s (giro 1, MISURATO), dopo le correzioni del giro 1 circa 6.300-6.750 (MISURATO), con la cache 19.228. Rapporto con mongod da 0,21 a 0,93 (STIMATO).
- Letture, 100 database: 7.584 → circa 12.500-13.400 → 21.794. Rapporto da 0,41 a 1,16 (STIMATO).
- Letture + aggiornamenti: da 0,28 a 0,38 (database grande) e da 0,52 a 0,81 (100 database) (STIMATO). Il numero di client del motore è cambiato (2 e 8 invece di 4 e 16) perché gli sweep dopo la cache danno lì il massimo.
- Gli inserimenti non erano misurati nel giro 1 (lo stallo li bloccava; corretto da S7, ora in main). In bench-mongod erano 0,15 e 0,22 (MISURATO allora, su un'altra macchina).

## La cache dei metadati

### Cosa fa

Ogni comando leggeva da SlateDB tre chiavi di metadati oltre al documento: `meta/collections` (elenco delle collezioni, per sapere se il nome è una vista), `meta/indexes/<collezione>` (definizione degli indici), `meta/copt/<collezione>` (opzioni: collation di default, validatore). Nel profilo del giro 1 valevano il 59% della CPU delle letture sul database grande e il 35% sui 100 piccoli (MISURATO). Ora il database aperto (`Entry` in `dbs.rs`) tiene in memoria i loro valori:

- un valore vale solo per l'apertura del database da cui è stato letto (contatori di aperture e chiusure); a ogni apertura, chiusura, perdita del lease, `dropDatabase` o creazione per una transazione la cache si svuota (negli stessi punti dove si svuotava già la cache delle opzioni `copts`);
- ogni scrittura che tocca una chiave `meta/` la svuota, dentro `apply_mode`, sotto il lock esclusivo del database, prima di mettere in coda la scrittura: lì passano `create`, `drop`, `createIndexes`, `dropIndexes`, `collMod`, `renameCollection`, `dropDatabase` e ogni altra scrittura;
- un valore entra in cache solo se la lettura dello stato più recente e quella dello stato durevole danno lo stesso risultato: una scrittura di metadati già in coda ma non ancora durevole non finisce mai in cache, quindi le letture durevoli (il default) mantengono il loro significato;
- le letture di una transazione passano dal suo snapshot e non usano la cache;
- al massimo 4.096 chiavi per database aperto, valori copiati (una voce in cache non tiene viva una pagina di SlateDB);
- `--metadata-cache false` la spegne. `src/lifecycle.rs` non è toccato.

### Test

`ctl-tests/test_33_metadata_cache.py`, 10 test: ognuno prima legge (la cache tiene i metadati vecchi), poi cambia collezioni, indici o opzioni, poi controlla la lettura successiva. Casi: indice creato e poi usato con `hint`; indice cancellato; 15 giri di crea/cancella con un secondo client; collezione cancellata e sostituita da una vista con lo stesso nome; collezione ricreata senza la sua collation; `collMod` con validatore; `renameCollection` che porta con sé gli indici; `dropDatabase`; chiusura per inattività e riapertura; un secondo processo che prende il database mentre il primo è fermo oltre il suo lease, crea un indice e lo restituisce.

Controlli negativi (MISURATO): con un binario che non svuota la cache in `apply_mode` falliscono 9 test su 10; con un binario che non la svuota alla riapertura fallisce il test del secondo processo. Con il binario vero passano tutti e 10.

Suite ctl-tests completa sul binario con la cache: 40 file su 41 passano e uno (`test_04b_export_vs_real_mongo`) è saltato perché richiede un mongod di riferimento, come sempre (MISURATO, `bench/perf/results/ctl-cache/summary.txt`).

### Guadagno (A/B alternati, mediana di 3 run per parte, 30 s + 30 s, 1 core; MISURATO)

- Letture, 1 database grande: da 6.656 a 20.237 op/s, **+204%**.
- Letture, 100 database: da 13.405 a 22.378 op/s, **+67%**.
- Letture + aggiornamenti, 1 database: da 2.050 a 2.505 op/s, **+22%**.
- Letture + aggiornamenti, 100 database: da 3.729 a 4.430 op/s, **+19%**.

Il guadagno sul database grande è più alto della stima del giro 1 (circa 2,5 volte, STIMATO allora): le tre chiavi di metadati sono vecchie o assenti, quindi SlateDB doveva guardare quasi tutti i suoi file (45 SST nello snapshot) per ognuna, mentre il documento si trova prima (STIMATO, coerente con il profilo).

## Profilo per strato dopo la cache (quota dei campioni perf, MISURATO)

- Letture, 1 database: MongoDB 51,2%, SlateDB 46,7% (lettura del documento 40,3%, blocchi letti dopo un miss della cache 6,4%), rete e runtime 2,0%. Dentro lo strato MongoDB: logica dei comandi 21,2%, kernel per i socket 14,5%, BSON 6,8%, malloc/free 4,7%, copie di memoria 4,0%. Le letture di metadati sono sparite dal profilo.
- Letture, 100 database: MongoDB 55,5%, SlateDB 42,4% (documento 35,8%).
- Letture + aggiornamenti, 1 database: SlateDB 71,1% (I/O dello store locale 35,1%, lettura del documento 16,0%, scrittura 5,6%, blocchi dopo un miss 5,6%, flush 5,4%), MongoDB 22,7%, rete 4,9%.
- Letture + aggiornamenti, 100 database: SlateDB 60,2% (I/O dello store locale 24,2%, documento 14,2%, flush 9,5%), MongoDB 35,4%.
- Inserimenti, 1 database: SlateDB 72,0% (I/O dello store locale 29,2%, scrittura e controllo dell'`_id` 24,9%, compaction 6,9%, flush 6,5%), MongoDB 25,4%. Il controllo dell'`_id` duplicato (`Writes::exists`, una lettura su SlateDB di una chiave che non esiste) vale da solo il 23,7%.
- Inserimenti, 100 database: SlateDB 74,3% (I/O dello store locale 49,2%, flush 11,1%, scrittura 8,9%), MongoDB 21,3%.

"I/O dello store locale" è il kernel che crea e cancella file su ext4: con `--store local` ogni oggetto WAL e ogni SST è un file. È una scelta del banco di prova; in produzione al suo posto c'è una PUT su S3, il cui costo in CPU qui non è misurato (NON SO).

## Piano fino a 1 volta mongod

Le leve rimaste, in ordine di guadagno STIMATO, con file toccati, rischio e chi le fa. Le stime vengono dalle quote del profilo qui sopra: togliere una quota q della CPU vale circa 1/(1−q) volte le operazioni al secondo, ammesso che il resto non cambi.

### Letture (oggi 0,93 e 1,16)

1. **Lettura del documento dentro SlateDB** (40% sul database grande, 36% sui piccoli). Per ogni lettura SlateDB costruisce un iteratore e risolve il percorso del file (con `format!` e `object_store::Path`) per ogni SST che guarda, più la cache dei blocchi. Dimezzarla vale circa +25% (STIMATO). Chi: il motore nuovo, che leggerà gli SST con un lettore proprio. Nel motore di oggi si potrebbe ridurre il numero di SST guardati con una compaction più spinta (`src/lifecycle.rs`, terreno di S7): rischio medio, più scritture di compaction. Basta da sola a portare il database grande sopra 1 volta (STIMATO).
2. **Logica per comando nello strato MongoDB** (21%, molte voci piccole): due viste di lettura per ogni find (`view_lookup` e `find`), controllo del lease dopo ogni lettura anche quando il valore viene dalla cache (circa 2%), contatori di diagnostica e uso (circa 2%), `fair::Timed` che sposta il future in un Box e prende due volte l'ora a ogni poll (circa 3,7% come tempo proprio). Insieme circa +5-10% (STIMATO). Chi: questa sessione per `cmd.rs` e `views.rs` (una vista per find); S4 per `fair.rs` e per il controllo del lease in `dbs.rs`. Rischio basso.
3. **Risposta senza decodificare il documento** (BSON 6,8%, più parte di malloc e copie): per una find per `_id` senza proiezione si può copiare nella risposta il BSON letto da SlateDB, senza trasformarlo in `Document` e ritorno. Circa +5-8% (STIMATO). Chi: questa sessione (`cmd_data.rs`, `coll.rs`). Rischio medio: filtri, collation, TTL e proiezioni devono continuare a passare dalla strada di oggi.
4. **Socket** (14%): è il costo di una lettura e una scrittura per richiesta, uguale per mongod. Nessuna leva.

### Scritture (oggi letture + aggiornamenti 0,38 e 0,81, inserimenti 0,36 e 0,56)

Nel motore di oggi:

1. **Un file per ogni oggetto WAL** (I/O dello store locale: 35% nelle letture + aggiornamenti e 29-49% negli inserimenti). Con lo store locale è il costo del file system; con S3 diventa una PUT per ogni flush del WAL (NON SO quanto costa in CPU). Leva di oggi: il **group commit adattivo** già presente (`--adaptive-group-commit true`), che fa aspettare la prima scrittura di un gruppo pochi millisecondi quando ne arrivano altre dello stesso database, così condividono un oggetto WAL. MISURATO (A/B alternati, mediana di 3): inserimenti su 1 database da 4.768 a 6.727 op/s (**+41%**, p50 da 5,5 a 3,6 ms); su 100 database nessun guadagno visibile (mediana −24%, ma le run calano nel tempo per entrambi i lati: coppie +9%, −24%, −8%); letture + aggiornamenti +0,6% e −3,5% (rumore). Non l'ho acceso di default: cambia la latenza delle scritture e aiuta solo i database con molti scrittori in parallelo. Decisione del fondatore: accenderlo solo per i database molto scritti (STIMATO +40% sugli inserimenti di quei database). File: `main.rs` (default), nessun codice nuovo.
2. **Controllo dell'`_id` duplicato** a ogni inserimento (23,7% degli inserimenti sul database grande): è una lettura su SlateDB di una chiave che non esiste, che deve guardare il filtro bloom di ogni SST. Toglierla quasi del tutto (per esempio con un filtro di esistenza degli `_id` in memoria per i database caldi, o un indice degli `_id` nel lettore del motore nuovo) vale circa +30% sugli inserimenti (STIMATO). Chi: motore nuovo; nel motore di oggi andrebbe dentro SlateDB o in `dbs.rs`/`coll.rs` con un filtro proprio: rischio alto (un falso "non esiste" sarebbe un duplicato in più).
3. **Flush della memtable e compaction** (7-15% della CPU delle scritture): lavoro di SlateDB, in `src/lifecycle.rs` per la parte di regolazione. Chi: S7 o il motore nuovo. Guadagno NON SO senza provare.
4. **Rilettura del documento per il suo numero d'ordine naturale** in ogni aggiornamento (`natural::stored_seq`, 4,0% delle letture + aggiornamenti su 100 database): il valore è già stato letto dalla find dell'aggiornamento e si può passare avanti. Circa +4% (STIMATO). Chi: questa sessione (`coll.rs`, `cmd_data.rs`). Rischio basso. Non fatta: sotto la soglia del 5% che il prompt chiede.
5. **Strato MongoDB delle scritture** (21-35%): molte voci piccole, come per le letture. Circa +5% (STIMATO). Chi: questa sessione e S4.

Con il quaderno del disegno nuovo:

- il WAL non è più un oggetto per flush nello store: le scritture vanno nel quaderno (registro replicato a 3 zone) e allo store arrivano a blocchi. La leva 1 sparisce del tutto: da +30% a +55% sulle letture + aggiornamenti e da +40% a +100% sugli inserimenti (STIMATO dalle quote 24-35% e 29-49%);
- il controllo dell'`_id` (leva 2) si fa sul lettore proprio degli SST e sul quaderno; resta da progettare.

Insieme, leve 1 e 2 porterebbero gli inserimenti sul database grande da 0,36 a circa 0,75 volte mongod e le letture + aggiornamenti da 0,38 a circa 0,6 (STIMATO). **1 volta mongod sulle scritture non è raggiungibile nel motore di oggi con correzioni nello strato MongoDB**: il costo grosso è sotto (store, SlateDB). Arrivarci richiede il quaderno più il lettore proprio degli SST (STIMATO); va verificato sul motore nuovo con lo stesso banco.

## Leve fatte in questo giro e leve provate

- Fatta: **cache dei metadati** (sopra), +204% / +67% / +22% / +19% (MISURATO).
- Provata, non tenuta come default: **group commit adattivo** (sopra), +41% sugli inserimenti di un database molto scritto, nessun guadagno visibile altrove (MISURATO). È una scelta di latenza: decide il fondatore.
- Le altre leve dentro i miei file valgono meno del 5% ciascuna nel profilo (rilettura del numero d'ordine 4,0%, viste doppie e controlli del lease circa 2% ciascuno, STIMATO): come chiede il prompt mi sono fermato. La risposta senza decodifica (lettura, circa +5-8% STIMATO) è sopra la soglia sulla carta ma tocca il percorso di ogni find; la lascio come primo passo del prossimo giro, perché le letture sono già circa a 1.

## Cosa non è confrontabile

- Durabilità: con `--store local` il motore non fa fsync dei file (vedi bench-mongod); mongod con j:true fa fsync del journal. Il vantaggio è del motore.
- Lo store locale crea un file per ogni oggetto; in produzione c'è S3 (PUT HTTP). Il costo delle scritture del motore qui non è quello di produzione.
- mongod negli inserimenti ha bisogno di 256 client per riempire il core (j:true aspetta il flush del journal), il motore di 32: le latenze vanno lette con il numero di client.

# Giro 1 (prima della cache)

### Macchina e metodo

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

### Risultati prima e dopo (1 core)

Mediana di 3 run da 60 s dopo 60 s di riscaldamento (`bench/perf/final.sh`). "main" è il motore di main con jemalloc, "branch" è il motore di questo branch. Le operazioni al secondo sono MISURATE; i rapporti con mongod sono STIMATI (calcolati dalle mediane).

#### Letture puntuali per `_id`, 1 database da 200.000 documenti

- main: 4.271 op/s (run: 3.945, 4.271, 4.420), p50 1,72 ms, p99 4,42 ms, 8 client.
- branch: 6.319 op/s (run: 6.209, 6.319, 6.379), p50 1,20 ms, p99 2,51 ms, 8 client. **+48%** rispetto a main.
- mongod: 20.105 op/s (run: 17.773, 20.182, 20.105), p50 0,36 ms, p99 1,23 ms, 8 client.
- Rapporto con mongod: **da 0,21 a 0,31** (soglia 0,5).

#### Letture puntuali per `_id`, 100 database da 2.000 documenti

- main: 7.584 op/s (run: 6.639, 7.929, 7.584), p50 1,91 ms, p99 5,25 ms, 16 client.
- branch: 12.490 op/s (run: 12.490, 12.074, 12.720), p50 1,18 ms, p99 3,28 ms, 16 client. **+65%** rispetto a main.
- mongod: 18.549 op/s (run: 18.529, 18.549, 18.912), p50 0,40 ms, p99 1,09 ms, 8 client.
- Rapporto con mongod: **da 0,41 a 0,67** (soglia 0,8).

#### 50% letture e 50% aggiornamenti, 1 database da 200.000 documenti

- main: 1.891 op/s (run: 1.815, 1.891, 1.901), p50 1,95 ms, p99 5,60 ms, 4 client.
- branch: 2.043 op/s (run: 2.037, 2.055, 2.043), p50 1,76 ms, p99 5,38 ms, 4 client. **+8%** rispetto a main.
- mongod: 6.764 op/s (run: 7.156, 5.594, 6.764), p50 2,18 ms, p99 6,26 ms, 16 client; mongod usa il 94% del core, perché con j:true aspetta il flush del journal (MISURATO).
- Rapporto con mongod: da 0,28 a 0,30 per core; da 0,27 a 0,29 per secondo di CPU usata.

#### 50% letture e 50% aggiornamenti, 100 database da 2.000 documenti

- main: 3.379 op/s (run: 3.493, 3.379, 3.210), p50 4,35 ms, p99 11,58 ms, 16 client.
- branch: 3.942 op/s (run: 3.324, 3.942, 4.159), p50 3,60 ms, p99 12,22 ms, 16 client. **+17%** rispetto a main.
- mongod: 6.454 op/s (run: 6.411, 6.454, 6.660), p50 2,28 ms, p99 6,63 ms, 16 client, 95% del core.
- Rapporto con mongod: da 0,52 a 0,61 per core; da 0,50 a 0,58 per secondo di CPU usata.

#### Differenze con bench-mongod

- mongod qui fa circa il doppio delle letture misurate in bench-mongod (20.105 contro 9.137 op/s sul database grande, MISURATO in entrambi i lavori, su macchine diverse). Non ho trovato la causa: stessa immagine Docker, stesso generatore, stesse opzioni. Anche il motore di main va più veloce che allora (4.271 contro 2.286), in parte per jemalloc. Per questo i rapporti di partenza qui (0,21 e 0,41) sono più bassi di quelli di bench-mongod (0,25 e 0,66): conta il rapporto misurato sulla stessa macchina nella stessa sessione.
- Il motore parte da uno snapshot caricato a blocchi (vedi sopra), mongod da un caricamento normale.
- Le letture + aggiornamenti scrivono: per il motore lo store locale crea un file per ogni oggetto WAL (vedi bench-mongod), e quel costo del file system è nel numero.

### Profilo per strato

#### Prima (main con jemalloc), quota dei campioni (MISURATO)

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

#### Dopo le correzioni (MISURATO)

- Letture, 1 database grande: SlateDB 76,4%, MongoDB 22,5%, rete e runtime 1,1%. Dentro SlateDB: elenco delle collezioni 22,9%, definizione degli indici 18,5%, opzioni della collezione 17,9%, il documento 12,1%, lettura di blocchi dopo un miss della cache 4,0%. La seconda lettura delle opzioni non c'è più.
- Letture, 100 database: SlateDB 59,5%, MongoDB 39,4%. Dentro SlateDB: elenco delle collezioni 18,7%, il documento 17,7%, indici 8,7%, opzioni 8,0%.
- Letture + aggiornamenti, 1 database grande: SlateDB 76,8% (di cui I/O dello store locale 30,0%), MongoDB 18,7%.
- Letture + aggiornamenti, 100 database: SlateDB 67,0% (di cui I/O dello store locale 21,4%), MongoDB 29,7%.
- Dentro lo strato MongoDB le copie di stato dei future sono scese dal 5,6% all'1,0% (database grande) e dall'11,0% all'1,9% (100 database). Quello che resta (logica dei comandi 9,9% e 18,7%, socket 6,4% e 9,7%, BSON 2,5% e 4,1%, malloc 1,9% e 3,5%) è distribuito su molte funzioni, nessuna con più dell'1,5% della CPU come tempo proprio (MISURATO: controlli dei lease, lock delle viste, mappe di diagnostica, costruzione della risposta).

La quota dello strato MongoDB è scesa poco in percentuale (dal 29,4% al 22,5% sul database grande) perché la CPU totale per lettura è scesa di circa un terzo: in valore assoluto il costo dello strato MongoDB per lettura è sceso di circa metà (STIMATO: 29,4% di 1/4.271 s contro 22,5% di 1/6.319 s).

### Correzioni (un commit ciascuna, in ordine)

1. **`find` legge le opzioni della collezione dalla vista che ha già** (`cmd_data.rs`). Prima apriva una seconda vista dopo la lettura e rileggeva le opzioni da SlateDB solo per sapere se la collezione è clustered. Guadagno: +14,3% letture al secondo sul database grande (3.836 → 4.385), +9,5% sui 100 piccoli (7.200 → 7.886) (MISURATO).
2. **Niente più copie da 10-22 KB di stato asincrono a ogni comando** (`cmd.rs`, `diag.rs`, `js.rs`, `collation.rs`, `views.rs`). Ogni comando passava per una catena di funzioni async e di involucri che contenevano in linea il future più grande di tutti i comandi: `route` 21,6 KB, `dispatch_commands` 22,0 KB, `dispatch_inner` 22,5 KB, il percorso delle viste 19,4 KB, `authorize` 9,0 KB anche con l'autenticazione spenta (MISURATO con un binario di prova e `size_of_val`). Ogni involucro ricopiava tutto. Ora gli involucri sono funzioni semplici che restituiscono il future dello scope, il future di ogni comando va in un Box una sola volta, il lavoro sulle viste vere va in un Box solo quando il nome è una vista, e `authorize` non viene costruito quando l'autenticazione è spenta. Dopo: `route` 792 byte, `dispatch_commands` 960 byte, `dispatch_inner` 1.144 byte, `dispatch` 352 byte (MISURATO). Guadagno: +21,2% sul database grande (4.746 → 5.754), +16,7% sui 100 piccoli (7.883 → 9.200) (MISURATO).
3. **Il ciclo della connessione passa il comando invece di copiarlo** (`main.rs`, `wire.rs`). Il documento del comando veniva copiato per intero a ogni richiesta solo perché la risposta doveva sapere se comprimere; ora quel dato è preso prima (`wire::ReplyTo`). Guadagno: +7,7% sul database grande (5.070 → 5.458), +2,7% sui 100 piccoli (11.215 → 11.521), +2,8% su letture + aggiornamenti con 100 database (3.323 → 3.416) (MISURATO; il binario nuovo vince 8 coppie su 9).
4. **Il registro di `currentOp` condivide il comando invece di copiarlo** (`cmd.rs`, `diag.rs`); la chiave della sessione è scritta in esadecimale in una sola stringa invece che con un `format!` per byte. Nel profilo di main `Diag::begin` valeva lo 0,9% (database grande), il 2,2% (100 database) e l'1,2% (letture + aggiornamenti) della CPU (MISURATO). Il confronto A/B non lo separa dal rumore: −4,4%, +6,3%, +7,3% (MISURATO, coppie divise 5 a 4). Guadagno atteso circa 1-3% (STIMATO dalla quota nel profilo).

Provata e non tenuta: **decodifica dei documenti BSON senza la copia intermedia** di `Document::from_reader`. Nel profilo la copia valeva circa lo 0,2% (MISURATO); il confronto A/B ha dato −2,0%, −10,9%, −5,8% con coppie divise (MISURATO): nessun guadagno visibile, quindi non è nel branch.

Nessun cambio di comportamento visibile: tutta la suite `ctl-tests` passa sul binario finale (38 file su 38, nessun fallimento; `test_04b_export_vs_real_mongo` è saltato perché richiede un mongod di riferimento, come nei rapporti precedenti) (MISURATO, `bench/perf/results/ctl-final/summary.txt`). `src/lifecycle.rs` e `mongo-server/src/dbs.rs` non sono stati toccati.

### Perché le soglie non sono raggiunte, e cosa serve

(Scritto alla fine del giro 1. La cache proposta qui è stata fatta nel giro 2, con il permesso del fondatore di toccare `dbs.rs`: vedi sopra.)

Database grande: 0,31 contro la soglia 0,5. 100 database piccoli: 0,67 contro la soglia 0,8 (STIMATO, dalle mediane MISURATE). Le correzioni possibili senza toccare dbs.rs sono fatte; quello che resta nello strato MongoDB fuori dalle letture di metadati è distribuito su molte voci piccole (sopra).

#### La leva più grande: 3 letture di metadati per comando (serve dbs.rs)

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

#### Il limite dentro SlateDB

Anche con i metadati in memoria resta una lettura su SlateDB per documento. Sul database grande quella lettura costa di più che sui piccoli, perché SlateDB costruisce un iteratore e risolve il percorso del file per ogni SST che deve guardare: lo snapshot del database grande ha 45 file SST nella directory `compacted` (non tutti per forza ancora in uso), quello di ogni database piccolo 1 (MISURATO, conteggio dei file). Nel profilo di main il lavoro di SlateDB per una lettura è fatto di iteratori e blocchi (17,6% della CPU), cache (8,8%), malloc/free (8,3%), contatori di `Bytes` (6,7%), filtri bloom (5,9%), percorsi degli SST con `format!` e `object_store::Path` (5,4%) (MISURATO). Nel disegno nuovo il nodo d'ingresso leggerà gli SST con un lettore proprio, quindi non ho lavorato dentro `Db::get`.

### Come rifare le misure

Giro 2 (cache, inserimenti): `BENCH_RESULTS=... NO_BASE=1 LABEL=final2 FINAL=<binario> CELLS="read:1:8:8 read:100:16:8 rw:1:2:16 rw:100:8:16 insert:1:32:256 insert:100:32:256" bench/perf/final.sh`; sweep con `bench.py sweep SERVER LAYOUT WORKLOAD --clients 1,2,4`. Giro 1:

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