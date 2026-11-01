# Le scritture non si bloccano più sotto inserimenti in blocco

**In breve.** Sotto inserimenti in blocco il motore smetteva di scrivere per sempre perché la compattazione di un database aperto in scrittura veniva chiesta solo da una scrittura appena terminata, e al massimo una volta ogni 10 secondi. Se L0 arrivava al limite di slatedb (8 SST) prima della richiesta successiva, tutte le scritture si fermavano dentro slatedb in attesa di spazio, nessuna terminava più, e quindi nessuno chiedeva più la compattazione: un'attesa circolare che nessuno sblocca. Ora è la manutenzione a guardare ogni secondo la L0 di ogni scrittore aperto e a chiedere la compattazione quando serve, che le scritture finiscano o no. Lo stesso difetto c'era nella chiusura di un database, ed è corretto anche lì.

Ogni numero è etichettato MISURATO (misurato su questa macchina in questo lavoro) o STIMATO (calcolato o dedotto).

## Macchina e binari

- La stessa VM del banco bench-mongod: 4 vCPU, 15,7 GiB di RAM, disco virtio con ext4 (MISURATO, lscpu e free).
- Motore di main: commit 658c724 (contiene la PR #4 mem-growth e jemalloc), release del Cargo.toml con le sole informazioni di riga per gli stack (`CARGO_PROFILE_RELEASE_DEBUG=line-tables-only`). sha256 `e8bdbfed0854c155…`.
- Motore corretto: questo branch, stesso profilo. sha256 `5a6e2dd6b13ad938…`.
- Motore in produzione: commit c9a4beb, compilato senza modifiche in una cartella separata con il profilo release del repository. sha256 `3c46efae7b5fc2bd…`.
- MinIO compilato dai sorgenti (`go install github.com/minio/minio@latest`, versione `DEVELOPMENT.GOGET`) perché dl.min.io, Docker Hub e quay.io per MinIO rispondono 403 dal proxy della sessione; dati sullo stesso disco locale.
- Generatore di carico: `bench/mongod/loadgen` del branch bench-mongod, compilato senza modifiche (Go, driver ufficiale v2).

## Come si riproduce

Lo script è `tools/stall/repro.sh STORE CORES RUNS`. Per ogni run avvia un motore nuovo su una cartella vuota (store locale) o su un bucket nuovo (MinIO), fissato con taskset alla CPU 0 (1 core) o alle CPU 0-1 (2 core), e lancia sulle CPU restanti lo stesso caricamento del banco: `loadgen -mode load -dbs 1 -docs 200000 -clients 16`, cioè 400 insertMany da 500 documenti di circa 1 KB, 16 in parallelo, write concern `{w: 1, j: true}`. Un run è bloccato se il caricamento fallisce: il client va in timeout dopo 60 s senza risposta. Quando un run si blocca lo script raccoglie le prove con il motore ancora vivo (CPU usata in 10 s, `tenantStats` e `serverStatus`, gdb `thread apply all bt`, e con la build diagnostica il dump delle task tokio), poi prova a fermarlo con SIGTERM e, dopo 30 s, con SIGKILL. Con MinIO lo script cancella il bucket alla fine di ogni run.

Esito su main (MISURATO): **20 blocchi su 20 caricamenti**.
- Store locale, 1 core: 5 su 5.
- Store locale, 2 core: 5 su 5.
- MinIO, 1 core: 5 su 5.
- MinIO, 2 core: 5 su 5.

Ogni blocco arriva nei primi secondi del caricamento: il client va in timeout dopo 63-67 s dall'inizio, quindi l'ultima risposta arriva dopo 3-7 s (MISURATO). In tutti i 20 casi SIGTERM non ha fermato il motore in 30 s ed è servito SIGKILL (MISURATO). Il banco aveva visto 9 blocchi su 19 con 1 core sul commit 4e2217a; qui sono 5 su 5. Non ho verificato perché la frequenza sia salita; la spiegazione più probabile è che main, con jemalloc, scriva più in fretta e riempia L0 prima (STIMATO).

## Prove raccolte

I file sono in `results-fix-stall/evidence/`.

**Il processo è fermo, non lento.** Durante il blocco il motore usa tra 0,08 e 0,12 s di CPU ogni 10 s (MISURATO, 15 run). gdb mostra tutti i thread parcheggiati: i worker di tokio in `epoll_wait` o in attesa su una futex del parcheggio di tokio, il thread di jemalloc in attesa sulla sua condition variable, nessun thread dentro codice del motore o di slatedb (MISURATO, `main-*-gdb-all-threads.txt`). Quindi è un'attesa asincrona, cioè una future che nessuno sveglia, e non un lock del sistema operativo o un ciclo.

**La compattazione non riparte.** `tenantStats` durante il blocco: al massimo 1 compattazione eseguita in tutto il caricamento, e nessuna in tutti e 5 i run con MinIO e 1 core (MISURATO, 15 run in cui ho letto `tenantStats`).

**Strumentazione per la diagnosi.** Per vedere lo stato interno ho compilato una build diagnostica a parte (`tools/stall/diag.patch`, non fa parte della correzione): ogni 5 s scrive per ogni scrittore aperto quante SST ha L0 nella vista dello scrittore e nel manifest salvato nello store, se c'è una compattazione in coda e se il lock di manutenzione del database è libero; registra ogni richiesta, inizio e fine di compattazione; con SIGUSR1 fa il dump delle task tokio (`Handle::dump`, con `tokio_unstable` e la feature `taskdump`); segnala ogni 10 s una scrittura ferma dentro `Db::write` di slatedb. Con questa build il blocco si è riprodotto 5 volte su 5 (store locale, 1 core).

Sequenza del primo run diagnostico (MISURATO, tempi dall'apertura dello scrittore):
- a 1,6 s la prima scrittura che trova L0 = 4 chiede la compattazione;
- la compattazione dura 2,3 s e porta L0 da 4 a 0;
- a 4,5 s lo scrittore ha già L0 = 3;
- entro 9,5 s L0 = 8 sia nella vista dello scrittore sia nello store, cioè il limite `l0_max_ssts` di slatedb;
- da lì, per i 74 s successivi fino al dump: L0 resta 8, la coda di compattazione è vuota, il lock di manutenzione è libero e non arriva nessuna nuova richiesta di compattazione.

Negli altri due run diagnostici la sequenza è una variante (MISURATO): la richiesta arriva a 1,7 s quando lo scrittore vede L0 = 4 ma il manifest salvato nello store ne ha ancora 3, quindi la compattazione non trova niente da fare e finisce in 9 ms; entro 3 s L0 è 8 e non arriva nessuna nuova richiesta.

**Dove sono ferme le richieste.** Nel secondo giro diagnostico il dump delle task tokio mostra (MISURATO, `diag2-local-1core-run1-tokio-task-dump.txt`):
- 1 task, il comando insert che tiene il lock esclusivo del database, ferma in `Entry::apply` → `Tenant::stage` → `Db::write` di slatedb → `maybe_apply_backpressure`, in attesa che venga caricata la memtable immutabile più vecchia (`await_uploaded`); il log della build diagnostica la segnala ferma lì con L0 = 8 a 10, 20, 30 s e così via;
- 13 task in attesa del lock del database in lettura (`dbs.rs`, `Dbs::read`, dal controllo delle viste dell'insert) e 2 in attesa del lock in scrittura (`Dbs::lock`);
- tutte le altre task inattive su timer o code vuote (i task interni di slatedb in attesa di messaggi, il reaper, la manutenzione).

**GC e guardia di L0 non c'entrano.** Il passaggio di pulizia delle SST arriva dopo 60 s e non cancella niente (`deleted_ssts=0 referenced=16 waiting=44`), con il lock di manutenzione libero prima e dopo (MISURATO). La guardia che aspetta la compattazione prima di promuovere uno scrittore vicino al limite (`Tenant::upgrade`, `L0_GUARD`) non entra in gioco: il caricamento crea il database (`Lifecycle::create`, nel log `new=true`) e non lo riapre mai come lettore. L'ipotesi del banco era giusta nella sostanza (la compattazione non riparte quando le scritture sono ferme) ma non passa da quel punto di `lifecycle.rs`.

## Causa

1. Gli scrittori del motore non hanno un compattatore proprio (`compactor_options = None`): la compattazione la fa la manutenzione del processo, su richiesta, attraverso una coda.
2. Per un database aperto in scrittura l'unica richiesta veniva da `Entry::apply_mode` (`request_compaction_if_needed` in `mongo-server/src/dbs.rs`), dopo che `Tenant::stage` era tornato, e al massimo una volta ogni 10 s per database.
3. slatedb scarica una memtable in L0 solo se L0 ha meno di `l0_max_ssts` SST (8). Altrimenti le memtable piene restano in memoria; quando memtable e WAL non ancora scaricati superano `max_unflushed_bytes` (32 MiB con le memtable da 8 MiB di main), `Db::write` aspetta che la memtable più vecchia venga caricata. È un ciclo senza fine: ricontrolla ogni 30 s e torna ad aspettare.
4. La scrittura che aspetta tiene il lock esclusivo del database, quindi tutte le altre richieste a quel database restano in coda; il reaper delle chiusure per inattività salta il database perché il lock è occupato; SIGTERM chiude i database uno per uno e resta in attesa dello stesso lock.
5. Ne viene un'attesa circolare: la scrittura aspetta spazio in L0, lo spazio arriva solo da una compattazione, la compattazione arriva solo da una scrittura che termina. Se la richiesta precedente è di meno di 10 s prima (o non ha trovato niente da compattare perché lo store era un passo indietro rispetto allo scrittore), quando L0 arriva a 8 non c'è più nessuno che possa chiederla.

Lo stesso difetto c'era nella chiusura. `Tenant::close` toglieva lo scrittore dall'elenco degli scrittori aperti e poi chiamava `Db::close`, il cui flush finale della memtable aspetta spazio in L0. Nessuno chiedeva la compattazione di uno scrittore non più in elenco, e in ogni caso slatedb rifiuta `refresh_manifest` quando la chiusura è iniziata, quindi il flush avrebbe visto la compattazione solo al poll del manifest (60 s). Dopo 20 s la chiusura rinunciava al flush e chiudeva "unclean" (alla riapertura il WAL viene rigiocato). Con la sola prima parte della correzione è successo in 2 spegnimenti su 3 dopo il test di regressione (MISURATO).

## Correzione

Commit c6d0feb e 00a6da0.
- **La compattazione la avvia la L0, non le scritture** (`src/lifecycle.rs`, `Maintenance::start`). Un task della manutenzione ogni secondo (`L0_WATCH_MS`) guarda la L0 di ogni scrittore aperto nella sua vista in memoria del manifest, senza nessuna richiesta allo store, e mette in coda una compattazione per ogni database con almeno 4 SST in L0. La coda tiene una sola compattazione per database: se ce n'è già una in coda o in corso la richiesta non ne aggiunge un'altra, e quando finisce la sorveglianza la richiede di nuovo al giro successivo se L0 è ancora sopra la soglia. La richiesta dal percorso di scrittura, con il suo limite di 10 s, è tolta.
- **Lo scrittore vede subito lo stato dello store.** Se una compattazione trova nello store meno SST di quante ne conti lo scrittore, fa rileggere il manifest allo scrittore. Dopo una compattazione riuscita lo faceva già.
- **La chiusura scarica la memtable prima di chiudere** (`Tenant::close`): prima il flush della memtable con lo scrittore ancora aperto e in elenco, così la sorveglianza di L0 vede lo scrittore e la compattazione può aggiornargli il manifest; poi lo scrittore esce dall'elenco e si chiude.
- **Osservabilità.** Gli errori di compattazione ora finiscono nel log (prima restavano in una lista che nessuno leggeva) e `tenantStats` riporta `maintenance.l0WatchRequests`.

Non ho aggiunto timeout. Il timeout di 20 s della chiusura c'era già e resta come rete di sicurezza: con la correzione non è mai scattato in nessun caricamento (MISURATO: 0 messaggi "final flush did not finish" in 80 caricamenti e 5 esecuzioni del test).

Costo (STIMATO): nessuna richiesta allo store in più quando L0 è sotto la soglia; una copia in memoria del manifest per scrittore aperto al secondo, come già fa ogni 2 s lo scarico delle memtable vecchie (`flush_old_memtables`). Le compattazioni fatte sono quelle che servono comunque.

## Test di regressione

`ctl-tests/test_28_bulk_insert_stall.py`: su MinIO, 16 thread inseriscono 200 lotti da 500 documenti di circa 1 KB in un solo database (100.000 documenti), con memtable da 1 MiB (`--memtable-max-mb 1`) perché L0 si riempia dopo pochi MB, e il `l0_max_ssts` di default di slatedb (8). Fallisce se un lotto non riceve risposta entro 60 s, se alla fine mancano documenti, se SIGTERM non ferma il motore entro 45 s, o se la chiusura del database ha rinunciato al flush finale.
- Su main fallisce: dopo 62 lotti le scritture si fermano e non ripartono più (MISURATO, `results-fix-stall/test_28_runs.txt`).
- Con la correzione passa 5 volte su 5: 100.000 documenti in 10,6-10,8 s, lotto più lento tra 1,65 e 2,09 s, 10 compattazioni per esecuzione, tutte chieste dalla sorveglianza di L0 (MISURATO).

## Risultati dei 20+20 caricamenti con la correzione

Stesso script e stesso caricamento della riproduzione (MISURATO, `results-fix-stall/fixed/`):
- Store locale, 1 core: **20 su 20 senza blocchi**, caricamento da 12,1 a 14,6 s (mediana 13,5).
- Store locale, 2 core: **20 su 20 senza blocchi**, da 8,0 a 12,1 s (mediana 8,5).
- MinIO, 1 core: **20 su 20 senza blocchi**, da 12,3 a 14,6 s (mediana 13,2).
- MinIO, 2 core: **20 su 20 senza blocchi**, da 8,6 a 9,5 s (mediana 9,1).

In tutti gli 80 run SIGTERM ha fermato il motore senza bisogno di SIGKILL, e nessuna chiusura è stata unclean (MISURATO).

Un primo giro su MinIO è stato invalidato e rifatto: lo script non cancellava ancora i bucket, il disco si è riempito e dal sedicesimo run MinIO rifiutava ogni scrittura (`XMinioStorageFull`), comprese quelle del lease. Quei fallimenti (5 con 1 core, 3 con 2 core) non sono blocchi del motore e sono conservati come `INVALID-diskfull-*`. Nota a margine, non indagata: con lo store che rifiuta le scritture anche il motore corretto non si ferma con SIGTERM in 30 s (8 casi su 8, MISURATO). È un altro scenario (store guasto) e va guardato a parte.

## Suite ctl-tests

DA COMPLETARE

## Il commit in produzione (c9a4beb)

DA COMPLETARE

## Nuovo confronto con mongod sugli inserimenti

DA COMPLETARE

## File

- `src/lifecycle.rs`, `mongo-server/src/dbs.rs`, `mongo-server/src/cmd.rs`: la correzione.
- `ctl-tests/test_28_bulk_insert_stall.py`: il test di regressione.
- `tools/stall/repro.sh`: la riproduzione; `tools/stall/diag.patch`: la strumentazione usata solo per la diagnosi.
- `results-fix-stall/`: esiti dei run (`baseline-main/`, `fixed/`), prove (`evidence/`), esecuzioni del test, suite e confronto con mongod.
