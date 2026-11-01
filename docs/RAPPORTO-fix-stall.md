# Rapporto finale — branch fix-stall

Ogni numero è MISURATO (uscito da una prova fatta in questo lavoro) o STIMATO (calcolato o dedotto). Tutto in locale: store su
disco locale o MinIO locale, mongod in Docker; nessuna spesa su AWS o altri cloud. Il documento tecnico completo è
`docs/fix-stall.md`; i dati grezzi sono in `results-fix-stall/session2/` (questa sessione) e `results-fix-stall/` (sessione
precedente).

PR in bozza: https://github.com/Mircoguidetti/fenvix-engine/pull/11 (nessun merge).

## In breve

- **Causa.** La compattazione di un database aperto in scrittura veniva chiesta solo da una scrittura appena finita, al
  massimo ogni 10 s. Sotto inserimenti in blocco L0 arriva al limite di slatedb (8 SST) dentro quei 10 s; slatedb smette di
  scaricare la memtable, ogni scrittura si ferma, nessuna finisce più e nessuno chiede più la compattazione. Il blocco è
  permanente e anche SIGTERM resta appeso.
- **Correzione.** La manutenzione guarda ogni secondo la L0 di ogni writer aperto e chiede la compattazione quando serve, che
  le scritture finiscano o no; la chiusura di un database scarica la memtable mentre il writer è ancora visibile a quel
  controllo.
- **Esito.** Main: 12 blocchi su 12 caricamenti. Con la correzione: 80 su 80 senza blocchi (20 per ogni combinazione di
  store locale/MinIO e 1/2 core), più 20 su 20 con il binario esatto della PR. test_28 fallisce su main e passa con la
  correzione. Suite ctl-tests: tutto passa. (MISURATO)
- **Produzione (c9a4beb).** Il blocco permanente non si è visto (0 su 32 caricamenti), ma lo stesso difetto c'è in forma più
  leggera: con un caricamento da 2 GB su 1 core tutte le scritture del database restano ferme insieme fino a 34 s, e la
  chiusura dopo un carico forte è sporca. (MISURATO)
- **Contro mongod sugli inserimenti**, rapporto noi/mongod per core: 0,12 (1 core, 1 database), 0,16 (2 core, 1 database,
  cella prima non misurabile), 0,22 (1 core, 100 database), 0,12 (2 core, 100 database). (rapporti STIMATI da mediane MISURATE)

## Branch

- Ripreso `origin/fix-stall` dai 4 commit della sessione precedente. Main non era andato avanti (il branch parte dall'ultimo
  main, jemalloc incluso): nessun rebase né merge da main.
- Durante questa sessione la sessione precedente, data per interrotta, ha spinto altri 3 commit su fix-stall (risultati e una
  bozza di `docs/fix-stall.md`). Non ho riscritto niente: li ho uniti con un merge normale. Dopo quel momento il branch remoto
  non si è più mosso.
- Commit di questa sessione: una riga di sola indentazione in `dbs.rs`; miglioramenti a `tools/stall/repro.sh` (registra
  compattazioni, richieste del controllo di L0 e insert più lento, cancella i dati di ogni corsa, non parte con poco disco,
  distingue un errore dello store da un blocco, numero di documenti configurabile); risultati; documenti. Nessuna modifica
  alla logica della correzione.

## 1. Causa, verificata con le prove

Build di diagnosi (main più `tools/stall/diag.patch`), caricamento di bench-mongod (200.000 documenti da circa 1 KB,
insertMany da 500, 16 in parallelo), store locale, 1 core: 3 blocchi su 3 (MISURATO). In ognuno:
- una sola richiesta di compattazione in tutta la corsa, quando L0 arriva a 4; la compattazione porta L0 da 4 a 0 in 2,5-3,1 s;
- da 6,0 a 7,4 s dopo quella richiesta tutte le scritture sono ferme dentro `Db::write` di slatedb, prima che scadano i 10 s
  dopo i quali main avrebbe permesso un'altra richiesta;
- durante il blocco: L0 = 8 sia nella vista del writer sia nel manifest salvato, coda di compattazione vuota, lock di
  manutenzione libero, CPU del motore allo 0,8% di un core, tutti i 15 thread in attesa;
- il dump dei task tokio mostra un insert fermo nella backpressure di slatedb (`maybe_apply_backpressure`) mentre tiene il lock
  esclusivo del database, e un altro insert in coda su quel lock.

Main normale: 3 blocchi su 3 anche con 2 core su store locale, con 1 core su MinIO e con 2 core su MinIO; in tutte le 12 corse
il motore non si è fermato entro 30 s da SIGTERM ed è servito SIGKILL (MISURATO). La diagnosi dei commit precedenti è corretta.

## 2. test_28

- Su main **fallisce**: 56 lotti da 500 accettati, poi timeout di 60 s; 1 sola compattazione (MISURATO).
- Con la correzione **passa**: 200 lotti in 11,0 s, lotto più lento 2,0 s, 10 compattazioni (MISURATO). Passa anche dentro la
  suite sul binario della PR (11,1 s, lotto più lento 1,9 s).

## 3. Criterio dei 20+20 caricamenti e suite

Raggiunto. Binario della correzione, nessun blocco (MISURATO):
- store locale, 1 core: 20 su 20, durata media 13,4 s, insert più lento 0,9 s;
- store locale, 2 core: 20 su 20, media 9,3 s, insert più lento 0,9 s;
- MinIO, 1 core: 20 su 20, media 14,9 s, insert più lento 1,5 s;
- MinIO, 2 core: 20 su 20, media 10,1 s, insert più lento 1,0 s.
In tutte le 80 corse: da 3 a 6 compattazioni per caricamento, tutte chieste dal controllo di L0; SIGTERM ha sempre fermato il
motore; nessuna chiusura sporca; nessun errore di compattazione.

Il binario esatto della PR (HEAD) non è identico byte per byte a quello delle 80 corse (cambia una riga di indentazione), quindi
l'ho provato a parte: 20 su 20 senza blocchi, 5 per combinazione (MISURATO).

Suite ctl-tests, tutti i 39 file sul binario della PR: **tutti passano** (MISURATO). `test_04b_export_vs_real_mongo` è saltato
perché richiede un mongod di riferimento, come nelle esecuzioni precedenti della suite.

Corse scartate: una prima serie MinIO con 2 core aveva 14 fallimenti su 20, e 2 corse del caricamento grande su MinIO sono
fallite. Non erano blocchi: il disco era pieno per i dati che MinIO tratteneva (bucket non cancellati, cestino interno,
caricamenti multipart rimasti) e MinIO rifiutava ogni scrittura con `XMinioStorageFull`. Serie ripetute dopo aver corretto lo
script. Effetto collaterale osservato e non indagato: quando lo store rifiuta le scritture anche il motore corretto non si ferma
con SIGTERM entro 30 s (14 casi su 14, MISURATO); è un altro scenario, da guardare a parte.

Limite della correzione: con un caricamento 10 volte più grande (2.000.000 di documenti) non c'è nessun blocco, ma con 1 core
alcuni insert aspettano fino a 14 s, perché un core solo non riesce a compattare alla velocità con cui L0 si riempie mentre serve
anche le scritture (3 su 3 riusciti, 293-325 s; con 2 core insert più lento 4,9 s su store locale e 6,3 s su MinIO). È il freno
di slatedb che si scioglie da solo, non un'attesa senza fine (MISURATO).

## 4. Strumentazione di diagnosi

Non è attiva nel codice della PR: esiste solo come file `tools/stall/diag.patch`, che nessuna build applica. Verificato sul
binario della PR: nessuna stringa "DIAG", nessuna feature `taskdump` nei Cargo.toml.

## 5. c9a4beb (produzione), solo lettura

Compilato senza modifiche in una cartella separata e provato con lo stesso script.
- Caricamento standard (200.000 documenti): **20 su 20 senza blocchi** (MISURATO). Prova poco significativa: c9a4beb usa il
  memtable di default di slatedb (64 MiB contro 8 MiB di main), quindi 200 MB fanno circa 3 SST in L0, la soglia di
  compattazione non viene mai raggiunta e in tutte le corse non c'è nessuna compattazione (MISURATO).
- Caricamento da 2.000.000 di documenti, impostazioni di produzione, store locale, 1 core: 3 su 3 finiti ma lenti (279-343 s).
  In ogni corsa uno o due episodi con **tutti i 16 client fermi insieme da 17 a 34 s**; in tutte e 3 la chiusura a SIGTERM ha
  rinunciato al flush finale (chiusura sporca) (MISURATO). Con 2 core (locale e MinIO) 6 su 6, insert più lento 3,1 s,
  chiusure pulite (MISURATO).
- Controprova con L0 da 8 MiB (non è la configurazione di produzione): 6 su 6 senza blocchi, ma chiusura sporca in 6 casi su 6
  (MISURATO).

Perché c9a4beb non si blocca per sempre come main: ha lo stesso modo di chiedere la compattazione, ma il writer rilegge il
manifest solo ogni 30 s (non subito dopo una compattazione) e il limite di dati non scaricati è 1 GiB per database invece di
32 MiB, quindi con L0 piena le scritture continuano in memoria e la rilettura periodica poi le sblocca. Le pause di 17-34 s
sono coerenti con quell'attesa (STIMATO dai tempi). Un blocco permanente anche lì richiederebbe più di 25 MB/s continui su un
solo database; nelle prove il carico più veloce era circa 9 MB/s (STIMATO).

Esito per la produzione: nessun blocco permanente osservato, ma pause fino a 34 s di tutte le scritture di un database sotto
inserimenti in blocco su 1 core, fino a 1 GiB di dati non scaricati in memoria per database, e chiusure sporche dopo un carico
forte (MISURATO). Con timeout dei client sotto i 30 s quelle pause diventerebbero errori (STIMATO).

## 6. Confronto con mongod sugli inserimenti

Ripetuto una volta `run_matrix.py --workloads insert` del branch bench-mongod (script senza modifiche): sweep dei client, poi 3
corse da 4 minuti al punto di saturazione, `{w: 1, j: true}`. Motore: binario della PR (correzione e jemalloc), store locale.
mongod 8.0.34. Mediane di 3 corse (MISURATO), rapporti calcolati (STIMATO):
- 1 core, 1 database: motore 723 op/s, mongod 5.842. **Rapporto per core 0,12** (banco: 0,15).
- 2 core, 1 database: motore 1.128 op/s per core, mongod 7.001. **Rapporto per core 0,16** (banco: non misurabile, il
  caricamento si bloccava 5 volte su 5).
- 1 core, 100 database: motore 1.107 op/s, mongod 5.061. **Rapporto per core 0,22** (banco: 0,22).
- 2 core, 100 database: motore 635 op/s per core, mongod 5.372. **Rapporto per core 0,12** (banco: 0,12).

Nessun blocco: 4 caricamenti su 4 al primo tentativo, nessun errore nelle 12 corse da 4 minuti del motore (MISURATO). La
macchina non è quella del banco, quindi contano i rapporti. Il motore fa un po' più op/s che nel banco, ma anche mongod, quindi
non si vede un guadagno attribuibile a jemalloc: i rapporti restano tra 0,12 e 0,22 (STIMATO). La correzione evita il blocco,
non rende le scritture più veloci.

## Note di lavoro

- Cargo, con una cartella `target` condivisa tra più cartelle di lavoro, ha restituito il binario del fix al posto di quello
  di main senza ricompilare; scoperto subito (build in 0,4 s), binario scartato, tutte le build successive rifatte forzando la
  ricompilazione e controllate.
- Nessun altro branch toccato; nessun merge della PR.
