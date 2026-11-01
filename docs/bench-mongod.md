# CPU per operazione: motore FENVIX contro mongod

**Siamo sotto metà di mongod per core sia in lettura sia in scrittura.** Letture puntuali su un database da 200.000 documenti: 0,25 volte mongod (MISURATO). Scritture: tra 0,12 e 0,24 volte mongod in tutte le celle misurate (MISURATO). Inoltre il motore si blocca per sempre sotto inserimenti massivi: in quel caso non si tratta di lentezza, le scritture si fermano del tutto (vedi la sezione "Stallo delle scritture").

Ogni numero è etichettato MISURATO (misurato su questa macchina in questo lavoro) o STIMATO (calcolato o dedotto). Salvo dove scritto diversamente, i valori sono la mediana di 3 run da 3 minuti al punto di saturazione. I dati grezzi sono in `bench/mongod/results/raw.jsonl`, il riepilogo in `bench/mongod/results/summary.txt` e il log completo in `bench/mongod/results/matrix.log`.

## Macchina

- Una VM KVM (Firecracker) con 4 vCPU "Intel Xeon Processor @ 2.80GHz" (famiglia 6, modello 85, quindi Cascade Lake), 1 thread per core, L2 4 MiB, L3 33 MiB (MISURATO, lscpu).
- RAM 15,7 GiB, senza swap (MISURATO).
- Disco virtio `/dev/vda` con ext4. Una scrittura sincrona da 4 KB (dd oflag=dsync) costa circa 0,27 ms (MISURATO: 2.000 scritture in 0,53 s).
- Ubuntu 24.04, kernel 6.18.44-fc. Rust 1.97.0, Go 1.24.7, Docker 29.9.2.
- La macchina è condivisa con l'hypervisor del cloud, quindi tra una run e l'altra c'è rumore. Lo scarto tra le 3 run è indicato sotto quando supera il 10%.

## Cosa è stato confrontato e come

- **Motore**: `mongo-server` compilato in release dal commit `4e2217a` di main (profilo release del Cargo.toml, senza modifiche). Opzioni di avvio: `--listen 127.0.0.1:27017 --store local --local-dir <dir>`, tutto il resto è il default di main.
  - Ho scelto `--store local` perché è lo store più vicino al disco locale previsto da main.rs (`s3`, `local` o `memory`). `local` usa `object_store::local::LocalFileSystem` dentro il wrapper `localcas.rs`, che aggiunge l'overwrite condizionale per lease e control layer. MinIO non serviva: avrebbe aggiunto HTTP e un altro processo da misurare.
  - Durabilità: il default di main, cioè la modalità "immediate" (una scrittura è confermata quando il suo oggetto WAL è scritto nello store), con group commit attivo, process lease attivo, memtable 8 MiB, block cache 256 MiB e meta cache 64 MiB.
- **mongod**: 8.0.34, immagine Docker ufficiale `mongo:8.0` (digest `sha256:b9924c330a8b...`), rete host, nessuna opzione oltre a `--bind_ip 127.0.0.1 --port 27017`. Il journal di WiredTiger è sempre attivo in mongod 8 (non si può spegnere). La cache WiredTiger è di 7.533 MB (MISURATO dal log di avvio), la compressione è snappy (default).
- **Write concern**: `{w: 1, j: true}` per tutti e due i server, impostato dal client. Per mongod, j:true significa che ogni scrittura aspetta il flush (con fsync) del journal. Per il motore, j:true coincide con il default (attesa della scrittura del WAL).
- **Installazione di mongod**: ho seguito l'ordine richiesto. Il repository apt ufficiale (repo.mongodb.org e www.mongodb.org) risponde 403 attraverso il proxy di rete della sessione, quindi non è raggiungibile. Docker ha funzionato al secondo tentativo (il primo ha preso un 429 da Docker Hub). mongodb-memory-server non è servito; in ogni caso scarica da fastdl.mongodb.org, che risponde anche lui 403.
- **Generatore di carico**: un programma scritto per questo lavoro (`bench/mongod/loadgen`), in Go con il driver ufficiale `go.mongodb.org/mongo-driver/v2` v2.3.0. È lo stesso binario per i due server.
  - Documenti: `{_id: int64, f: stringa di 1.000 caratteri}`, circa 1 KB.
  - Chiavi e database scelti in modo uniforme e casuale.
  - Carichi: (a) `find` per `_id`; (b) `insertOne` con `_id` nuovo; (c) 50% `find` per `_id` e 50% `updateOne` con `$set` del campo per `_id`.
- **Fissaggio dei core**: il database gira sulla CPU 0 (oppure sulle CPU 0-1), con `taskset` per il motore e `--cpuset-cpus` per il container di mongod. Il generatore gira sulle CPU restanti (1-3 oppure 2-3). Il generatore ha usato al massimo 1,21 core dei 2 disponibili (MISURATO, mongod con 2 core in lettura), quindi non è mai stato lui il collo di bottiglia.
- **Dati**: 1 database da 200.000 documenti, oppure 100 database da 2.000 documenti. In tutto circa 200 MB, che stanno in memoria in entrambi i casi (STIMATO: 200.000 × 1 KB).
- **Protocollo per ogni cella** (server × core × layout × carico):
  1. Sweep dei client 1, 2, 4, … 256, con 10 s di riscaldamento e 20 s di misura per ogni punto. Lo sweep si ferma quando la CPU del server è almeno al 95% dei suoi core e il throughput non cresce più del 5%, oppure quando il throughput scende.
  2. Punto di saturazione: il numero più piccolo di client con throughput entro il 3% del migliore dello sweep.
  3. Tre run a quel punto, con 60 s di riscaldamento e 180 s di misura.
- **CPU del processo**: utime+stime di tutti i thread del processo, letti da `/proc/PID/stat` all'inizio e alla fine della finestra di misura. Per mongod è il processo nel container, visto dall'host.
- **"Operazioni al secondo per core"**: throughput diviso per i core assegnati (1 o 2). Riporto anche le operazioni per secondo di CPU usata davvero, che conta quando il processo non arriva al 100%.
- **Differenze di metodo tra i due server** (decise per poter misurare il motore, vedi lo stallo): per mongod il server è stato avviato una volta per combinazione (core, layout) e i tre carichi sono stati eseguiti di seguito (letture, 50/50, inserimenti). Per il motore ho usato un server nuovo, appena caricato, per ogni carico, e una run che va in stallo viene registrata e ripetuta su un server nuovo. Nelle run complete del motore riportate qui non è avvenuto nessuno stallo. Il motore parte quindi da condizioni uguali o più favorevoli.

## Risultati per carico

Formato: motore contro mongod; throughput al secondo; latenze p50/p99; CPU usata; client al punto di saturazione. Tutti i numeri sono MISURATI, tranne i rapporti, che sono STIMATI (calcolati dalle mediane misurate).

### a) Letture puntuali per _id

- 1 core, 1 database da 200.000 documenti. Motore: 2.286 op/s, p50 1,61 ms, p99 3,71 ms, CPU 0,97 core, 4 client. mongod: 9.137 op/s, p50 0,40 ms, p99 0,97 ms, CPU 0,96 core, 4 client. **Rapporto per core: 0,25.**
- 1 core, 100 database da 2.000 documenti. Motore: 4.974 op/s, p50 0,38 ms, p99 0,82 ms, CPU 0,96 core, 2 client. mongod: 7.576 op/s, p50 0,48 ms, p99 1,10 ms, CPU 0,96 core, 4 client. **Rapporto per core: 0,66.**
- 2 core, 1 database da 200.000 documenti. **Non misurabile per il motore**: il caricamento si è bloccato 5 volte su 5 nella matrice e 3 su 3 nelle prove a mano (MISURATO). mongod: 23.187 op/s (11.594 per core), p50 0,60 ms, p99 2,77 ms, CPU 1,89 core, 16 client.
- 2 core, 100 database. Motore: 12.660 op/s (6.330 per core), p50 2,43 ms, p99 5,83 ms, CPU 1,93 core, 32 client. mongod: 22.928 op/s (11.464 per core), p50 1,29 ms, p99 3,99 ms, CPU 1,94 core, 32 client. **Rapporto per core: 0,55.**

Per il motore il costo di una lettura dipende dalla dimensione del database: con 100 database piccoli fa 2,2 volte le letture per core che fa con un database grande (MISURATO: 4.974 contro 2.286). Per mongod vale quasi il contrario (7.576 contro 9.137).

### b) Solo inserimenti

- 1 core, 1 database. Motore: 701 op/s, p50 1,26 ms, p99 5,25 ms, CPU 0,87 core, 1 client (con 2 client lo sweep dava meno). In operazioni per secondo di CPU usata: 803. mongod: 4.727 op/s, p50 25,2 ms, p99 54,7 ms, CPU 0,94 core, 128 client (5.039 per secondo di CPU). **Rapporto per core: 0,15** (0,16 per secondo di CPU usata).
- 1 core, 100 database. Motore: 1.023 op/s, p50 1,93 ms, p99 7,53 ms, CPU 0,95 core, 2 client, scarto tra le run 28%. mongod: 4.649 op/s, p50 50 ms, p99 160 ms, CPU 0,96 core, 256 client (l'ultimo punto dello sweep: mongod potrebbe salire ancora un poco con più client). **Rapporto per core: 0,22.**
- 2 core, 1 database: non misurabile per il motore (stesso blocco del caricamento). mongod: 10.578 op/s (5.289 per core), p50 5,2 ms, p99 21,9 ms, CPU 1,79 core, 64 client.
- 2 core, 100 database. Motore: 1.161 op/s (581 per core), p50 3,07 ms, p99 9,53 ms, CPU 1,46 core, 4 client, scarto 18%. mongod: 9.526 op/s (4.763 per core), p50 2,73 ms, p99 15,6 ms, CPU 1,77 core, 32 client. **Rapporto per core: 0,12** (0,15 per secondo di CPU usata). Con 2 core il motore non riesce a usare tutta la CPU: più client non danno più throughput (lo sweep a 8 client è più basso che a 4).

### c) 50% letture e 50% aggiornamenti

- 1 core, 1 database. Motore: 1.067 op/s, p50 3,46 ms, p99 9,42 ms, CPU 0,97 core, 4 client. mongod: 4.514 op/s, p50 3,31 ms, p99 9,10 ms, CPU 0,91 core, 16 client. **Rapporto per core: 0,24.**
- 1 core, 100 database. Motore: 2.205 op/s, p50 0,79 ms, p99 2,46 ms, CPU 0,97 core, 2 client; scarto 70%, con le tre run in calo nel tempo (3.372, poi 1.818, poi 2.205). mongod: 4.216 op/s, p50 3,53 ms, p99 10,6 ms, CPU 0,92 core, 16 client. **Rapporto per core: 0,52.**
- 2 core, 1 database: non misurabile per il motore. mongod: 10.376 op/s (5.188 per core), p50 6,24 ms, p99 16,8 ms, CPU 1,85 core, 64 client.
- 2 core, 100 database. Motore: 2.284 op/s (1.142 per core), p50 2,16 ms, p99 11,7 ms, CPU 1,65 core, 8 client, scarto 25%. mongod: 9.562 op/s (4.781 per core), p50 1,52 ms, p99 5,53 ms, CPU 1,80 core, 16 client. **Rapporto per core: 0,24.**

### Punto di saturazione

Il motore satura il suo core con pochissimi client: da 1 a 4 su 1 core, da 4 a 32 su 2 core (MISURATO, sweep in `summary.txt`). mongod ne vuole da 4 a 256, perché con j:true ogni scrittura aspetta il flush del journal e servono molte scritture in volo per riempire la CPU. Per questo le latenze al punto di saturazione non si confrontano tra loro: mongod ha latenze alte con 128-256 client sugli inserimenti perché ha molti più client in coda. A parità di client, per esempio 2 client in scrittura su 1 core e 1 database, la p50 è 1,86 ms per il motore contro 0,69 ms per mongod (MISURATO, sweep da 20 s).

### Le scritture del motore rallentano nel tempo

Negli sweep, che durano 30 s e partono subito dopo il caricamento, il motore scrive molto più che nelle run da 4 minuti (MISURATO):
- 50/50 con 2 core e 100 database: 4.768 op/s nello sweep a 8 client, 2.284 nella mediana delle run complete;
- inserimenti con 1 core e 100 database: 2.349 op/s nello sweep a 2 client, 1.023 nelle run complete.

mongod invece è stabile tra sweep e run complete (per esempio 4.523 contro 4.514 sul 50/50 con 1 core). Il costo per operazione del motore cresce con la durata delle scritture. La causa più probabile è il lavoro di flush e compaction e il numero di SST letti per ogni get, che crescono nel tempo (STIMATO, non verificato).

## Stallo delle scritture (blocca parte della misura)

Sotto inserimenti massivi (insertMany da 500 documenti, da 4 a 16 in parallelo) e, più di rado, anche con un solo client che inserisce un documento alla volta, il motore smette di rispondere alle scritture per sempre. Dopo una serie di insert lenti (da 0,6 a 1 s ciascuno) nessuna scrittura viene più completata. I client vanno in timeout dopo 60 s e lo stato non si sblocca nemmeno dopo più di 10 minuti. Lo spegnimento pulito (SIGTERM) resta anch'esso appeso, e serve SIGKILL. Il processo intanto usa lo 0% di CPU: tutti i thread sono inattivi (stack in `bench/mongod/results/stall/fenvix-hang-stacks.txt`). Si tratta quindi di un'attesa che nessuno sblocca, non di lavoro arretrato.

Frequenza (MISURATO):
- caricamento del database da 200.000 documenti su 1 core: 9 blocchi su 19 tentativi (matrice, prove a mano e profilo), di cui 2 su 4 con soli 4 batch in parallelo invece di 16;
- caricamento su 2 core: 8 blocchi su 8;
- caricamento di 100 database da 2.000 documenti: 0 blocchi su 6;
- carico di inserimenti con 1 solo client (profilo): 1 blocco su 2 run;
- nelle run complete della matrice: nessuno.

Nel log del motore, l'ultima riga prima del blocco è sempre un passaggio di SST GC con `referenced=16`, e poi nessuna compaction. Ipotesi, non verificata: L0 si riempie (`l0_max_ssts` 8), il flush della memtable si ferma, e la compaction viene chiesta solo dal percorso di scrittura (`mongo-server/src/dbs.rs:646`, `request_compaction_if_needed`, al massimo una volta ogni 10 s). Quando tutte le scritture sono ferme, nessuno chiede più la compaction. Non ho modificato il motore per verificarlo, come previsto dalle regole.

Conseguenze per questa misura: le celle con 2 core e 1 database da 200.000 documenti non sono misurabili per il motore. Le altre celle del motore sono state misurate su un server caricato al primo tentativo che è riuscito.

## Profilo della CPU del motore

Sono sotto la soglia, quindi ho fatto il profilo. Strumenti: `perf record -F 499 -g` sul processo del motore, fissato a 1 core, per 60 s, dopo 90 s di carico (`bench/mongod/profile.sh`). Il binario è quello di release, che ha i simboli ma non le informazioni di debug: le funzioni inline sono contate nella funzione che le chiama, e le catene di chiamata sono affidabili solo per un livello.

**Carico peggiore: inserimenti** (0,12-0,22 volte mongod). Profilo con 1 core, 1 database e 1 client, a 851 op/s durante il campionamento (MISURATO). Le tre funzioni con più tempo proprio (MISURATO):
1. `_int_malloc` (allocatore della glibc): 11,1%
2. `finish_task_switch` (kernel, cambio di contesto): 5,8%
3. `lookup_bh_lru` (kernel ext4, ricerca dei buffer dei metadati del file system): 4,2%

Per gruppi (MISURATO): kernel 34,5%; malloc/free della glibc 24,7%; slatedb 9,5%; memcpy/memcmp 5,0%; core di Rust 4,1%; tokio 3,3%; codice di `mongo_server` 2,8%; bytes 2,7%.

Il kernel pesa così tanto perché lo store locale crea un file per ogni oggetto WAL, cioè uno per ogni flush, e altri per SST e manifest. Le funzioni ext4 (`lookup_bh_lru`, `recently_deleted`, `__brelse`, `ext4_get_group_desc`) sono creazione e cancellazione di inode. Su S3 quel costo diventerebbe il costo di una PUT HTTP, quindi non è detto che sia più basso.

**Letture** (profilo con 1 core, 1 database e 4 client). Le tre funzioni con più tempo proprio (MISURATO):
1. `__memmove_evex_unaligned_erms` (copie di memoria): 7,9%
2. `_int_malloc`: 7,8%
3. `malloc`: 4,3%

Per gruppi (MISURATO): malloc/free 24,2%; slatedb 18,1%; memcpy/memcmp 10,3%; kernel 8,2%; core di Rust 7,6%; bytes 5,8%; `mongo_server` 4,1%. Tra i chiamanti di `malloc` visibili ci sono `slatedb::segment_iterator::build_segment_iter`, l'init di `SstIterator` e `PathResolver::sst_path`, che costruisce con `format!` il percorso dell'SST, poi analizzato da `object_store` (percent-encoding e ricerca di sottostringhe, circa 2% in tutto). In pratica ogni get costruisce iteratori e stringhe di percorso per ogni SST che tocca. Questo spiegherebbe perché le letture costano di più sul database grande, che ha più SST (STIMATO, coerente con il dato 4.974 contro 2.286 ma non provato).

I file del profilo sono `bench/mongod/results/perf-*-top.txt` (le 25 funzioni con più tempo proprio) e `perf-*-groups.txt` (i gruppi). Il primo tentativo di profilo degli inserimenti ha campionato il motore in stallo ed è conservato come `*-stalled.*`.

## Cosa NON è confrontabile tra i due

- **Durabilità**: non è la stessa, e il vantaggio è del motore. Con `--store local` il motore scrive i file senza fsync: `LocalFileSystem::new_with_prefix` senza `with_fsync(true)`, verificato nel sorgente di object_store 0.14.2. Una scrittura confermata è quindi nella page cache, non sul disco, e un crash della macchina (non del solo processo) la può perdere. mongod con j:true fa fsync del journal prima di confermare. Per avere un motore con fsync bisognerebbe cambiare `main.rs` o `localcas.rs`, cosa esclusa dalle regole: non l'ho fatto. Il motore in produzione scrive su S3, dove la durabilità costa una PUT. Nonostante questo vantaggio, il motore resta sotto un quarto di mongod per core in scrittura.
- **Storage**: lo store locale crea un file per ogni oggetto, e il costo del kernel ext4 (circa un terzo della CPU negli inserimenti) non esiste con S3, dove però c'è il costo di HTTP e TLS. Questo confronto misura il motore con lo store locale, non il motore in produzione.
- **Processi e thread**: mongod gira in un container Docker (rete host, nessun proxy), il motore no. La CPU di Docker e containerd non è contata per mongod, ma a riposo è trascurabile. Il runtime tokio del motore vede 1 o 2 CPU per via di taskset. mongod ha molti thread di servizio, che con 1 core si contendono la CPU: per questo mongod con 2 core fa più del doppio che con 1 core in lettura (23.187 contro 9.137, MISURATO).
- **Multi-database**: per mongod 100 database sono solo 100 collezioni con i propri file WiredTiger. Per il motore sono 100 tenant slatedb separati, ognuno con WAL, memtable e lease propri. Il confronto con 100 database misura proprio questa differenza di architettura.
- **Latenze**: al punto di saturazione i due server hanno numeri di client molto diversi (vedi sopra), quindi la p50 e la p99 vanno lette insieme al numero di client.
- **Inserimenti di mongod**: con 1 core lo sweep si è fermato all'ultimo punto (256 client), quindi il valore di mongod potrebbe essere un poco più alto: il rapporto noi/mongod sarebbe ancora più basso (STIMATO).
- **Ordine dei carichi**: per mongod i tre carichi sono stati eseguiti sullo stesso server, uno dopo l'altro. Per il motore ogni carico ha avuto un server nuovo. Gli inserimenti fatti prima non hanno quindi pesato sulle letture del motore, ma su quelle di mongod sì, almeno in teoria: le letture venivano per prime, quindi l'effetto è nullo per le letture e piccolo per il 50/50 (STIMATO).

## Come rifare la misura

Vedi `bench/mongod/README.md`. In breve: `install.sh`, poi `run_matrix.py` (circa 6 ore sulla macchina descritta sopra, più i tentativi ripetuti per gli stalli), poi `summarize.py` e `profile.sh`.
