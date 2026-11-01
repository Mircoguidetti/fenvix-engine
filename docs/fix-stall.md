# Le scritture non si bloccano più sotto inserimenti in blocco

**In breve.** Sotto inserimenti in blocco il motore smetteva di scrivere per sempre. La compattazione di un database aperto in
scrittura veniva chiesta solo da una scrittura appena finita, e al massimo una volta ogni 10 secondi. Se L0 arrivava al limite
di slatedb (8 SST) prima della richiesta successiva, tutte le scritture si fermavano dentro slatedb in attesa di spazio, nessuna
finiva più e quindi nessuno chiedeva più la compattazione: un'attesa circolare che nessuno sblocca. Ora è la manutenzione a
guardare ogni secondo la L0 di ogni database aperto in scrittura e a chiedere la compattazione quando serve, che le scritture
finiscano o no. Lo stesso difetto c'era nella chiusura di un database, ed è corretto anche lì.

Ogni numero è etichettato MISURATO (uscito da una prova fatta in questo lavoro) o STIMATO (calcolato o dedotto). Tutto è stato
misurato in locale: store su disco locale o MinIO locale, nessuna richiesta ad AWS o ad altri cloud.

Il lavoro è stato fatto in due sessioni. La prima ha scritto la correzione, il test e lo script e ha misurato una prima serie
(cartella `results-fix-stall/` esclusa `session2/`). La seconda ha ripreso il branch, non ha dato per buona la diagnosi e
l'ha verificata da capo su un'altra macchina, ha ripetuto tutte le misure e ha fatto le parti mancanti (suite, c9a4beb,
mongod). Salvo dove è scritto "prima sessione", i numeri qui sotto sono della seconda (`results-fix-stall/session2/`).

## Macchina e binari

- VM con 4 vCPU "Intel Xeon Processor @ 2.10GHz", 15 GB di RAM, nessuno swap, disco virtio con ext4 (MISURATO, lscpu e free).
  Non è la stessa macchina del banco bench-mongod (lì 2,80 GHz): i numeri assoluti non si confrontano con quelli del banco,
  solo i rapporti misurati nella stessa prova.
- Motore di main: commit 658c724 (contiene jemalloc e la PR #4), release del Cargo.toml.
- Motore corretto: questo branch. Le 80 corse del criterio sono state fatte con il binario del commit e2c084d; il codice che
  va in PR (HEAD) differisce solo per una riga di indentazione in `dbs.rs`, ma i due binari non sono identici byte per byte,
  quindi il binario di HEAD è stato ricompilato e provato a parte (20 caricamenti e suite completa, sotto).
- Motore in produzione: commit c9a4beb, compilato senza modifiche in una cartella separata.
- Build di diagnosi: main più `tools/stall/diag.patch`, con `RUSTFLAGS="--cfg tokio_unstable --cfg tokio_taskdump"`.
- MinIO compilato dai sorgenti (`go install github.com/minio/minio@latest`) perché dl.min.io risponde 403 dal proxy della
  sessione. mongod 8.0.34 dall'immagine Docker ufficiale `mongo:8.0`.
- Generatore di carico: `bench/mongod/loadgen` del branch bench-mongod, compilato senza modifiche.

Nota di metodo: tutte le cartelle di lavoro condividevano la stessa cartella `target` di cargo. Cargo riconosce i crate del
workspace con un hash che non dipende dal percorso e confronta le date dei file, e alla prima prova ha restituito il binario
del fix al posto di quello di main senza ricompilare. Da lì ogni build è preceduta da un `touch` dei sorgenti della propria
cartella, e i binari sono stati controllati (quello di main non contiene il contatore `l0WatchRequests`, quello del fix sì).

## Come si riproduce

`tools/stall/repro.sh STORE CORES RUNS [OUTDIR]`, con `STORE` = `local` o `minio` e `CORES` = 1 o 2. Variabili:
`ENGINE_BIN` (il binario `mongo-server`), `LOADGEN_BIN` (il loadgen di bench-mongod), facoltative `DOCS` (documenti
caricati, default 200.000), `STALL_SECS`, `ENGINE_ARGS`, `DIAG=1` con la build di diagnosi.

Per ogni corsa lo script avvia un motore nuovo su una cartella vuota (store locale) o su un bucket nuovo (MinIO), fissato con
taskset alla CPU 0 (1 core) o alle CPU 0-1 (2 core), e lancia sulle CPU restanti lo stesso caricamento del banco:
`loadgen -mode load -dbs 1 -docs 200000 -clients 16`, cioè 400 insertMany da 500 documenti di circa 1 KB, 16 in parallelo,
write concern `{w: 1, j: true}`. Una corsa è bloccata se il caricamento fallisce: il client va in timeout dopo 60 s senza
risposta. Su un blocco lo script raccoglie le prove con il motore ancora vivo (CPU in 10 s, `tenantStats` e `serverStatus`,
gdb su tutti i thread, con la build di diagnosi anche il dump dei task tokio), poi SIGTERM e, dopo 30 s, SIGKILL. Per ogni
corsa riuscita registra la durata, le compattazioni, le richieste del controllo di L0 e l'insert più lento del log del motore.
Alla fine di ogni corsa cancella i dati (cartella o bucket, compresi i caricamenti multipart rimasti aperti).

Esito su main (MISURATO, seconda sessione): **12 blocchi su 12 caricamenti**.
- Store locale, 1 core (build di diagnosi): 3 su 3.
- Store locale, 2 core: 3 su 3.
- MinIO, 1 core: 3 su 3.
- MinIO, 2 core: 3 su 3.

Il client va in timeout tra 63 e 69 s dall'inizio, quindi l'ultima risposta arriva dopo 3-9 s di caricamento (MISURATO). In
tutti i 12 casi SIGTERM non ha fermato il motore in 30 s ed è servito SIGKILL (MISURATO). La prima sessione aveva misurato
20 blocchi su 20 sullo stesso main. Il banco bench-mongod, sul commit più vecchio 4e2217a, ne aveva visti 9 su 19 con 1 core.

## Prove

File in `results-fix-stall/session2/evidence/` (seconda sessione) e `results-fix-stall/evidence/` (prima sessione).

**Il processo è fermo, non lento.** Durante il blocco il motore usa 0,08 s di CPU in 10 s, cioè lo 0,8% di un core
(MISURATO, 3 corse). gdb mostra 15 thread tutti in attesa: i 12 thread del blocking pool di tokio parcheggiati su una
condition variable, il worker di tokio in attesa sul timer, il thread principale in attesa (MISURATO). Nessun thread lavora:
è una future che nessuno sveglia, non un ciclo e non un lock del sistema operativo.

**Una sola compattazione in tutto il caricamento.** Sequenza nelle 3 corse con la build di diagnosi (MISURATO):
- una scrittura che trova L0 = 4 chiede la compattazione; è l'unica richiesta di tutta la corsa;
- la compattazione dura 2,5-3,1 s e porta L0 da 4 a 0;
- da 6,0 a 7,4 s dopo quella richiesta le scritture sono già tutte ferme dentro `Db::write` di slatedb, quindi prima che
  passino i 10 s dopo i quali main avrebbe permesso una nuova richiesta;
- da lì fino alla fine (oltre 2 minuti): L0 = 8 sia nella vista del writer sia nel manifest salvato nello store, coda di
  compattazione vuota, lock di manutenzione libero, nessuna nuova richiesta. `tenantStats`: `compactRuns` = 1.

**Dove sono ferme le richieste.** Il dump dei task tokio mostra un insert fermo in `Entry::apply_mode` → `Tenant::stage` →
`Db::write` → `maybe_apply_backpressure` di slatedb, in attesa che la memoria non scaricata scenda sotto il limite, mentre
tiene il lock esclusivo del database, e un altro insert in coda su quel lock (`Dbs::lock` → `RwLock::write_owned`)
(MISURATO). La prima sessione, con più connessioni ancora aperte al momento del dump, aveva visto lo stesso insert fermo e
15 richieste in coda sul lock.

**GC e guardia di L0 non c'entrano.** Il passaggio di pulizia delle SST non cancella niente e lascia libero il lock di
manutenzione; la guardia di `Tenant::upgrade` (`L0_GUARD`) non entra in gioco perché il caricamento crea il database e non lo
riapre come lettore (prima sessione, MISURATO; coerente con i log della seconda).

## Causa

1. I writer del motore non hanno un compattatore proprio (`compactor_options = None`): la compattazione la fa la
   manutenzione del processo, su richiesta, attraverso una coda.
2. Per un database aperto in scrittura l'unica richiesta veniva da `Entry::apply_mode` (`request_compaction_if_needed` in
   `mongo-server/src/dbs.rs`), dopo che `Tenant::stage` era tornato, e al massimo una volta ogni 10 s per database.
3. slatedb scarica una memtable in L0 solo se L0 ha meno di `l0_max_ssts` SST (8). Altrimenti le memtable piene restano in
   memoria; quando memtable e WAL non scaricati superano `max_unflushed_bytes` (32 MiB con le memtable da 8 MiB di main),
   `Db::write` aspetta, e ricontrolla senza fine.
4. La scrittura che aspetta tiene il lock esclusivo del database: tutte le altre richieste a quel database restano in coda,
   e SIGTERM, che chiude i database uno per uno, resta in attesa dello stesso lock.
5. L'attesa è circolare: la scrittura aspetta spazio in L0, lo spazio arriva solo da una compattazione, la compattazione
   arriva solo da una scrittura che finisce. Sotto inserimenti in blocco L0 passa da 4 a 8 in meno di 10 s, quindi quando
   arriva a 8 nessuno può più chiedere la compattazione. La rilettura periodica del manifest da parte del writer (ogni 30 s)
   non aiuta, perché nello store non c'è nessuna compattazione nuova da vedere.

La diagnosi dei commit della prima sessione è quindi confermata. Lo stesso difetto c'era nella chiusura: `Tenant::close`
toglieva il writer dall'elenco dei writer aperti e poi chiamava `Db::close`, il cui flush finale aspetta spazio in L0; nessuno
chiedeva la compattazione di un writer non più in elenco, e slatedb rifiuta di rileggere il manifest quando la chiusura è
cominciata. Dopo 20 s la chiusura rinunciava al flush e chiudeva "sporca" (alla riapertura il WAL viene rigiocato). Con la
sola prima parte della correzione è successo in 2 spegnimenti su 3 (prima sessione, MISURATO).

## Correzione

Commit c6d0feb e 00a6da0.
- **La compattazione la avvia la L0, non le scritture** (`src/lifecycle.rs`, `Maintenance::start`). Un task della
  manutenzione ogni secondo (`L0_WATCH_MS`) guarda la L0 di ogni writer aperto nella sua vista in memoria del manifest, senza
  richieste allo store, e mette in coda una compattazione per ogni database con almeno 4 SST in L0. La coda tiene una sola
  compattazione per database; finita quella, se L0 è ancora sopra la soglia il giro successivo la richiede di nuovo. La
  richiesta dal percorso di scrittura, con il suo limite di 10 s, è tolta.
- **Il writer vede subito lo stato dello store.** Se una compattazione trova nello store meno SST di quante ne conti il
  writer, gli fa rileggere il manifest (dopo una compattazione riuscita lo faceva già).
- **La chiusura scarica la memtable prima di chiudere** (`Tenant::close`): prima il flush con il writer ancora aperto e in
  elenco, così il controllo di L0 lo vede e la compattazione gli aggiorna il manifest; poi il writer esce dall'elenco e si
  chiude.
- **Osservabilità.** Gli errori di compattazione finiscono nel log e `tenantStats` riporta `maintenance.l0WatchRequests`.

Costo (STIMATO): nessuna richiesta allo store in più quando L0 è sotto la soglia; una lettura in memoria del manifest per
writer aperto al secondo. Le compattazioni fatte sono quelle che servono comunque.

La strumentazione di sola diagnosi non è nel codice della PR: è solo nel file `tools/stall/diag.patch`, che nessuna build
applica da sola. Verificato sul binario di HEAD: nessuna stringa "DIAG", nessuna feature `taskdump` nei Cargo.toml.

## Test di regressione

`ctl-tests/test_28_bulk_insert_stall.py`: su MinIO, 16 thread inseriscono 200 lotti da 500 documenti di circa 1 KB in un solo
database, con memtable da 1 MiB perché L0 si riempia dopo pochi MB. Fallisce se un lotto non riceve risposta entro 60 s, se
mancano documenti, se SIGTERM non ferma il motore entro 45 s o se la chiusura ha rinunciato al flush finale.
- Su main **fallisce**: 56 lotti accettati, poi timeout; 1 sola compattazione (MISURATO).
- Con la correzione **passa**: 200 lotti in 11,0 s, lotto più lento 2,0 s, 10 compattazioni, 11 richieste del controllo di
  L0 (MISURATO). Dentro la suite completa, sul binario di HEAD: 200 lotti in 11,1 s, lotto più lento 1,9 s (MISURATO).
- Prima sessione: fallisce su main dopo 62 lotti, passa 5 volte su 5 con la correzione (MISURATO).

## Risultati dei 20+20 caricamenti con la correzione

Criterio: 20 caricamenti su 20 senza blocchi con 1 core e 20 su 20 con 2 core, su store locale e su MinIO. **Raggiunto.**
Binario del commit e2c084d (MISURATO, `results-fix-stall/session2/fixed/`):
- Store locale, 1 core: **20 su 20**, durata media 13,4 s (da 12,1 a 15,2), insert più lento 0,9 s.
- Store locale, 2 core: **20 su 20**, media 9,3 s (da 8,6 a 11,4), insert più lento 0,9 s.
- MinIO, 1 core: **20 su 20**, media 14,9 s (da 13,2 a 16,4), insert più lento 1,5 s.
- MinIO, 2 core: **20 su 20**, media 10,1 s (da 9,4 a 10,8), insert più lento 1,0 s.

In tutte le 80 corse: da 3 a 6 compattazioni per caricamento, tutte chieste dal controllo di L0; SIGTERM ha sempre fermato
il motore senza SIGKILL; nessuna chiusura sporca; nessun errore di compattazione (MISURATO).

Binario esatto di HEAD, 5 caricamenti per combinazione: 20 su 20 senza blocchi, medie 13,6 s (locale, 1 core), 9,1 s
(locale, 2 core), 14,0 s (MinIO, 1 core), 9,6 s (MinIO, 2 core), insert più lento 1,0 s (MISURATO, `session2/head/`).

La prima sessione aveva ottenuto lo stesso esito, 80 su 80, su un'altra macchina (MISURATO, `results-fix-stall/fixed/`).

Corse scartate. Una prima serie MinIO con 2 core aveva dato 6 corse riuscite e poi 14 fallite. Non erano blocchi del motore:
ogni corsa lasciava il suo bucket (circa 0,3 GB), il disco si è riempito e MinIO rispondeva `XMinioStorageFull` a ogni
scrittura, anche a quelle del lease; nel log del motore ogni insert finiva con un errore (`ok=false`), cosa che in un blocco
vero non succede. Lo stesso è capitato in 2 corse del caricamento grande su MinIO (per i caricamenti multipart che MinIO tiene
da parte). Queste corse sono conservate in `session2/invalid/`, e lo script ora cancella i dati di ogni corsa, si rifiuta di
partire con meno di 3 GB liberi e segnala un fallimento dovuto a errori dello store invece di contarlo come blocco. Anche la
prima sessione aveva avuto lo stesso problema. Nota a margine, non indagata: quando lo store rifiuta le scritture, anche il
motore corretto non si ferma con SIGTERM entro 30 s (14 casi su 14, MISURATO). È un altro scenario (store guasto) e va guardato
a parte.

## Caricamenti 10 volte più grandi: nessun blocco, ma pause

Stesso script con `DOCS=2000000` (circa 2 GB in un database), 3 corse per combinazione, binario corretto (MISURATO,
`session2/large-load/`):
- store locale, 1 core: 3 su 3 senza blocchi, 293-325 s, da 42 a 43 compattazioni, insert più lento 14,4 s;
- store locale, 2 core: 3 su 3, 178-207 s, insert più lento 4,9 s;
- MinIO, 2 core: 3 su 3, 181-208 s, insert più lento 6,3 s.

Il blocco permanente non c'è più, ma con 1 core, sotto un carico continuo che riempie L0 più in fretta di quanto un solo core
riesca a compattare mentre serve anche le scritture, le scritture vengono rallentate e alcuni insert aspettano fino a 14 s.
È il freno previsto di slatedb (si riparte appena la compattazione libera L0), non un'attesa senza fine. Ridurlo vorrebbe dire
compattare in modo diverso (per esempio più L0 per passaggio o memtable più grandi); non fa parte di questa correzione.

## Suite ctl-tests

Tutti i 39 file di `ctl-tests` sul binario di HEAD: **tutti passano** (MISURATO, `session2/ctl-suite-summary.txt`).
`test_04b_export_vs_real_mongo` è saltato perché richiede un mongod di riferimento che la suite non avvia, e un test di
`test_04_export` è saltato come su main.

## Il commit in produzione (c9a4beb)

Compilato senza modifiche in una cartella separata e provato con lo stesso script. Solo misure, nessuna modifica.

**Con il caricamento standard non si blocca: 20 su 20** (5 per combinazione, store locale e MinIO, 1 e 2 core), 8-11 s per
caricamento (MISURATO). Ma la prova non dice molto: in c9a4beb il memtable è quello di default di slatedb (64 MiB, contro
8 MiB di main), quindi 200 MB di dati fanno circa 3 SST in L0, la soglia di compattazione (4) non viene mai raggiunta e infatti
in tutte le 20 corse non c'è nessuna compattazione (MISURATO).

**Il difetto però c'è, in forma più leggera.** c9a4beb ha lo stesso meccanismo di main (compattazione chiesta solo da una
scrittura finita, al massimo ogni 10 s), ma due differenze lo rendono meno grave: dopo una compattazione il writer non rilegge
subito il manifest (lo fa solo alla rilettura periodica, ogni 30 s), e il limite di dati non scaricati è quello di slatedb,
1 GiB per database invece di 32 MiB, quindi con L0 piena le scritture continuano in memoria per molto più tempo.
- Caricamento da 2.000.000 di documenti, impostazioni di produzione, store locale, 1 core: 3 su 3 finiti, ma in 279-343 s.
  In ogni corsa ci sono stati uno o due episodi in cui **tutti i 16 client sono rimasti fermi insieme, da 17 a 34 s** (16 o 32
  insert sopra i 10 s per corsa); l'insert più lento ha aspettato 34,1 s. È coerente con l'attesa della rilettura periodica del
  manifest (al massimo 30 s più la compattazione): la compattazione era già fatta, ma il writer non lo sapeva ancora (la causa
  dell'attesa è STIMATA dai tempi, non strumentata). In tutte e 3 le corse la chiusura a SIGTERM ha rinunciato al flush finale (chiusura sporca) (MISURATO).
- Stesso caricamento con 2 core: 3 su 3, 231-245 s, insert più lento 3,1 s, chiusure pulite. Su MinIO con 2 core: 3 su 3,
  212-228 s, insert più lento 2,8 s (MISURATO).
- Controprova con L0 da 8 MiB (`--l0-sst-bytes 8388608`, non è la configurazione di produzione) e il caricamento standard:
  6 su 6 senza blocchi, ma 0 compattazioni riuscite con 1 core e 2 con 2 core, e **chiusura sporca in 6 casi su 6**: i dati
  restano in memoria (fino a 1 GiB) e alla chiusura non c'è spazio in L0 per scaricarli (MISURATO).

Esito per la produzione: **il blocco permanente non si è visto su c9a4beb (0 su 32 caricamenti)**, ma lo stesso difetto
produce pause fino a 34 s di tutte le scritture di un database sotto inserimenti in blocco su 1 core, memoria non scaricata
fino a 1 GiB per database e chiusure sporche dopo un carico forte (MISURATO). Un blocco permanente anche su c9a4beb richiederebbe
che L0 si riempia davvero nello store (4 SST da 64 MiB, cioè 256 MiB) entro 10 s dall'ultima richiesta, cioè più di 25 MB/s
continui su un solo database; nelle prove il carico più veloce è stato di circa 9 MB/s (STIMATO, da 2 GB in 212 s). Se in
produzione il timeout dei client è sotto i 30 s, quelle pause diventano errori lato client (STIMATO).

## Nuovo confronto con mongod sugli inserimenti

Ripetuto una volta il carico "solo inserimenti" del banco bench-mongod (`run_matrix.py --workloads insert`, script del branch
bench-mongod senza modifiche): per ogni cella uno sweep dei client, poi 3 corse da 60 s di riscaldamento e 180 s di misura al
punto di saturazione; server nuovo e caricato per ogni cella; write concern `{w: 1, j: true}`. Motore: binario di HEAD (con
la correzione e con jemalloc, già in main), `--store local`. mongod 8.0.34 in Docker. Dati grezzi in
`results-fix-stall/session2/mongod/`. Mediane di 3 corse (MISURATO); i rapporti sono calcolati dalle mediane (STIMATO).

- 1 core, 1 database da 200.000 documenti. Motore: 723 op/s (1 client, p50 1,24 ms). mongod: 5.842 op/s (256 client).
  **Rapporto per core: 0,12** (0,14 per secondo di CPU usata). Nel banco era 0,15.
- 2 core, 1 database da 200.000 documenti. Motore: 2.257 op/s, cioè 1.128 per core (64 client, scarto tra le corse 25%).
  mongod: 14.001 op/s, 7.001 per core. **Rapporto per core: 0,16.** Nel banco questa cella non era misurabile per il
  motore: il caricamento si bloccava 5 volte su 5.
- 1 core, 100 database da 2.000 documenti. Motore: 1.107 op/s (8 client). mongod: 5.061 op/s (16 client).
  **Rapporto per core: 0,22** (come nel banco).
- 2 core, 100 database. Motore: 1.269 op/s, 635 per core (4 client; usa 1,47 core su 2). mongod: 10.744 op/s, 5.372 per core.
  **Rapporto per core: 0,12** (come nel banco).

In tutta la prova nessun blocco: i 4 caricamenti del motore sono riusciti al primo tentativo (nel banco, con 1 database,
erano riusciti 3 caricamenti su 13) e nessuna delle 12 corse da 4 minuti del motore ha avuto errori (MISURATO).

Come leggere questi numeri. La macchina non è quella del banco, quindi conta il rapporto, non il valore assoluto. In valore
assoluto il motore fa qui un po' più che nel banco (723 contro 701, 1.107 contro 1.023, 1.269 contro 1.161 op/s), ma anche mongod
fa di più (5.842 contro 4.727), quindi non si può attribuire a jemalloc un miglioramento: i rapporti restano tra 0,12 e 0,22
(STIMATO). La correzione serve a non bloccarsi, non a scrivere più in fretta. Resta anche il calo nel tempo già visto nel banco:
con 1 core e 1 database lo sweep da 30 s dà 1.035 op/s e le corse da 4 minuti 723 (MISURATO).

## Problemi incontrati

- Disco pieno due volte per colpa di MinIO (bucket non cancellati, cestino interno svuotato lentamente, caricamenti multipart
  rimasti): corse scartate, script corretto, cestino di MinIO svuotato da un processo a parte durante le prove.
- Cargo che riusa il binario di un'altra cartella di lavoro (vedi "Macchina e binari").
- Durante la seconda sessione la prima sessione, data per interrotta, ha spinto 3 commit su fix-stall (risultati e una bozza di
  questo documento). Nessuna storia è stata riscritta: i commit sono stati uniti con un merge normale; i due lati avevano
  cambiato `repro.sh` nello stesso modo.

## File

- `src/lifecycle.rs`, `mongo-server/src/dbs.rs`, `mongo-server/src/cmd.rs`: la correzione.
- `ctl-tests/test_28_bulk_insert_stall.py`: il test di regressione.
- `tools/stall/repro.sh`: la riproduzione; `tools/stall/diag.patch`: la strumentazione usata solo per la diagnosi.
- `results-fix-stall/`: prima sessione; `results-fix-stall/session2/`: seconda sessione (esiti delle corse, prove, test_28,
  suite, confronto con mongod).
