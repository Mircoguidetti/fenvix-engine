PRONTO. Il lavaggio che non finisce è di main (provato con il binario a9f46d3) ed è corretto: con lo stesso caricamento 0 lavaggi falliti contro 24, nessuna scrittura ferma oltre 60 secondi contro 32, un rimescolamento da 3,9 GB finito. hostInfo vero, letture della riapertura fuori dal conto degli ordini, note M1–M3 scritte, giro piccolo dello script 22 fasi su 22 "fatta" (tutto MISURATO, in locale su disco, 2 vCPU). Restano da provare S3 vero e una macchina più grande.

# L1 · Il lavaggio che non finisce e le correzioni del revisore di K1

Sessione di lavoro Opus, prompt `prompts/L1-motore-lavaggio-e-k1.md`. Spesa $0: niente AWS, niente Atlas, niente connettori, niente merge. Branch `motore-correzioni` di fenvix-engine, nato da `server-per-script` (c90c44f). Lo script `prova_infinite.py` non l'ho toccato: md5 8396ca455557baaa4e3a9b1f881451f9, controllato su ogni copia usata (MISURATO).

Macchina di tutte le prove: 2 vCPU, 8 GB, store su disco locale (niente S3 vero, niente MinIO). Caricatori e server sulla stessa macchina: per RULES i numeri di velocità sono NON VALIDI come prestazioni; servono solo a confrontare prima e dopo, con lo stesso carico.

---

## In breve, con la cucina

1. **Il lavaggio che non finisce è di main, non di K1.** Con il binario di main a9f46d3 e lo stesso caricamento del revisore il lavaggio (compattazione) fallisce "did not finish" 24 volte in 43,6 minuti (MISURATO). Il motivo: ogni giro di lavaggio aveva un tempo massimo fisso di 30 secondi. Un rimescolamento di pile (merge di sorted run) che chiede di più veniva interrotto, e il giro dopo ricominciava da zero: slatedb salva i progressi solo a fogli interi (un SST di uscita fino a 256 MB per pezzo), quindi in 30 secondi non salvava niente. Il lavoro interrotto restava in dispensa come fogli buttati, il lavaggio fallito metteva il cliente in pausa fino a 60 secondi, intanto il banco degli ordini nuovi (L0) si riempiva e le scritture si fermavano.
2. **La correzione:** il giro di lavaggio non ha più un orologio. Va avanti finché lavora davvero (muove byte dei fogli) e finisce quando il banco L0 è sotto la soglia e non c'è più nessun rimescolamento in corso. Si ferma solo se per 30 secondi non muove niente (dispensa che non risponde), se qualcuno deve prendere il database (drop, erase, fork, export, restore) o se il server si spegne. Un rimescolamento lungo restituisce il suo posto di lavoro agli altri clienti dopo 30 secondi. La pulizia dei fogli vecchi e del quaderno (WAL) non aspetta più la fine del lavaggio.
3. **Con la correzione**, stesso caricamento: 0 lavaggi falliti, caricamento a velocità costante (circa 2 MB/s per caricatore contro 3,1 → 0,8 di main), nessuna scrittura oltre 60 secondi (main: 32), e un rimescolamento che ha prodotto una pila da 3,9 GB è finito (MISURATO).
4. **hostInfo** (la scheda della macchina) ora dice la macchina vera: 2 core e 8.022 MB qui, non 4 core e 16.091 MB del mongod registrato (MISURATO).
5. **Le letture per aprire o riaprire un database** non vanno più sul conto del primo ordine: vanno in un conto a parte, "lavoro di sfondo" (`serverStatus.fenvix.backgroundReads`). In più ho trovato e corretto una seconda causa delle letture "calde" che leggevano dalla dispensa: il primo ordine di scrittura dopo una riapertura (anche quella del profiler) cambiava il cuoco da lettore a scrittore e gli dava un bancone nuovo e vuoto (cache scope nuovo).
6. **Il giro piccolo dello script** contro il server corretto: 22 fasi su 22 "fatta", nessuna riga ERRORE, uscita 0 (MISURATO).

---

## 1. Il lavaggio che non finisce

### 1.1 La prova che decide: è di main

Il caricamento del revisore rifatto uguale, con il binario di main a9f46d3: due caricatori insieme con `prova_infinite.py carica` (app_data_4 `--gb 6.6 --quota-riservati 0.4883 --seme 104 --thread 16`, app_data_6 `--gb 9.67 --quota-riservati 0.4 --seme 106 --thread 16`), server `mongo-server --store local --replica-set rs0`, stessa macchina da 2 vCPU. Lo script che ho usato fa le stesse cose di `mongo-tests/l1_compaction_load.sh` sul branch (che ne è la copia in inglese): server, due caricatori, ogni 15 secondi il disco di ogni database e i lavaggi falliti.

Risultati con main (tutto MISURATO):
- 24 giri di lavaggio falliti con "compaction of tenant … did not finish (L0 7 o 8)", il primo già a 0,5 GB caricati.
- Scritture lente: 823 inserimenti oltre 10 secondi, 32 oltre 60 secondi, il più lungo 72,7 secondi.
- Velocità di caricamento in discesa: da 3,1 a 0,8 MB/s per caricatore; 2,09 e 1,99 GB caricati in 43,6 minuti. (Una parte della discesa è il dbStats di main che rilegge tutto il database, già corretto da K1.)
- Dispensa: app_data_6 occupava 6,46 GB su disco con 2,8 GB di fogli in uso nel manifesto (contati con `examples/inspect_compactions.rs`): circa 3,6 GB di fogli buttati o in attesa.
- Il server di main è stato ucciso per memoria finita a 43,6 minuti (5,0 GB di memoria, dmesg): è il dbStats di main che tiene tutto il database in memoria (K1 l'ha già corretto), più una mia compilazione che girava in parallelo. Per questo il giro di main è durato 43,6 minuti e non 60.

Cosa ho visto dentro la dispensa di main (MISURATO, con `inspect_compactions`): rimescolamenti "Running" da minuti con 0 byte di progresso salvato e nessun foglio di uscita registrato. Esempio app_data_6: un rimescolamento di quattro pile (SR 12, 8, 4, 0, circa 460 MB) in corso da molti giri, sempre a zero.

La stessa cosa in un test piccolo e ripetibile (MISURATO): `compaction_pass_longer_than_its_stall_window_finishes` (in `src/lifecycle.rs`) mette 8 fogli L0 da 4 MB su una dispensa lenta (ogni lettura di un foglio 400 ms) e dà al giro 1 secondo di finestra. Con il codice di main fallisce con lo stesso errore del revisore: "compaction of tenant test-compact-long did not finish (L0 8)". Con la correzione passa: il giro dura 2,25 secondi, più del doppio della finestra, e il banco L0 va da 8 a 0.

Conclusione: il difetto è di main (`src/lifecycle.rs`, `compact_tenant_pass`), non di K1. K1 lo rendeva solo più visibile: caricava più in fretta, quindi più lavaggio per minuto.

### 1.2 Perché con i database grandi peggiora (principio di disegno)

Con il tetto fisso di 30 secondi un rimescolamento finiva solo se stava tutto in 30 secondi. Con la regola delle pile di slatedb (size-tiered, pile simili a gruppi di 4) le pile più grosse crescono con il database: a 1 TB l'ultimo rimescolamento muove centinaia di GB. Anche a 200 MB/s per lavaggio (STIMATO, S3 nella stessa regione) sono molti minuti o ore. Con il tetto non sarebbe finito mai: il database avrebbe accumulato pile non lavate (più letture per ogni ordine), fogli buttati pagati su S3 e scritture ferme. La correzione toglie il tetto: il limite è "nessun progresso per 30 secondi", che non dipende dalla taglia.

### 1.3 La correzione (commit 4be1b66, ba21719, 8e71776)

Tutto in `src/lifecycle.rs`, più due righe in ognuno dei cinque comandi che prendono il database.
- **Niente orologio sul lavoro.** Il giro finisce quando L0 è sotto la soglia di 4 fogli e nessun rimescolamento del database è attivo (letto dallo stato delle compattazioni di slatedb). Si ferma per "nessun progresso" se in 30 secondi il compattatore non legge né scrive nessun byte di fogli (`COMPACTION_STALL`): è il caso della dispensa che non risponde, che il vecchio tetto copriva. Un giro fermato per questo resta un errore, con la pausa di prima.
- **Chi aspettava il banco L0 lo sa subito.** Il primo ordine di scrittura dopo una riapertura, se L0 era quasi pieno, aspettava la fine del giro (al massimo 3 volte 30 secondi, poi errore). Adesso gli si risponde appena L0 è sotto la soglia, anche se il rimescolamento continua.
- **Posto di lavoro restituito.** Il server ha 2 posti di lavaggio per tutti i database. Un giro che dopo 30 secondi sta solo rimescolando pile (L0 già a posto) restituisce il posto: un rimescolamento di ore su un database grande non tiene fermo il banco L0 degli altri clienti.
- **Si può fermare quando serve.** drop, erase, fork, export e restore chiedono al giro di fermarsi (`preempt_compaction`) invece di aspettarlo; anche lo spegnimento del server lo ferma. Il giro prima finisce di lavare il banco L0 (pochi secondi), poi ferma i rimescolamenti. Un giro fermato così non è un errore.
- **La pulizia non aspetta il lavaggio.** Prima la pulizia (GC) aspettava il lucchetto del database: con giri lunghi avrebbe fermato la pulizia di tutti i database. Adesso un database con il lucchetto occupato si salta e si riprova al giro dopo; se il lucchetto lo tiene solo un lavaggio, la pulizia del quaderno (WAL) e dei fogli vecchi gira lo stesso (il lavaggio non tocca il quaderno; i fogli che il lavaggio sta scrivendo sono protetti: o sono registrati nel suo stato, o hanno meno di 120 secondi). I segnalibri (checkpoint) che il compattatore lascia per ogni lavaggio non fermano più la pulizia dei fogli, e quelli scaduti si cancellano ogni minuto anche durante un giro lungo.
- **Lettura del manifesto meno fitta.** Ogni lettura del manifesto su S3 è una LIST e una GET. Il giro ora legge ogni 25 ms solo nei primi 2 secondi, poi ogni 250 ms finché L0 è sopra la soglia, poi ogni 2 secondi mentre rimescola.
- Numeri nuovi in `serverStatus.fenvix.compaction`: giri fatti, in corso, rimescolamenti lunghi, giri fermati senza progresso, giri fermati a richiesta, pulizie saltate.

Errori miei trovati e corretti durante la sessione (MISURATO):
- La prima versione (4be1b66) saltava la pulizia mentre un lavaggio teneva il lucchetto. Sotto carico continuo il lavaggio è quasi sempre acceso, quindi la pulizia non girava mai: dopo 4 minuti app_data_6 aveva 3.792 oggetti del quaderno e 3,9 GB su disco per circa 0,55 GB caricati. Con la correzione (ba21719), allo stesso punto, 186 oggetti e 2,9 GB. Ho fermato quella prova e rifatto tutto con ba21719.
- In ba21719 una fermata a richiesta (drop, fork, restore…) fermava il giro anche con il banco L0 pieno. Chi chiede la fermata spesso scrive subito nel database (un restore) e avrebbe trovato L0 pieno, senza nessun giro che potesse lavare (il lucchetto lo tiene lui): avrebbe aspettato 30 secondi e fallito. Una volta il test ctl `test_03_restore` è fallito con questo errore ("compaction wait timed out") con ba21719, mentre sulla stessa macchina girava lo script; rifatto da solo passa, anche con ba21719. Quindi la causa di quella volta non è provata (CPU occupata, o questa fermata); il caso nel codice è reale e l'ho corretto. In 8e71776 un giro fermato a richiesta prima lava il banco L0 (finché fa progressi), poi ferma solo i rimescolamenti.

### 1.4 La prova con la correzione

Stesso caricamento, binario ba21719 (il finale 8e71776 cambia solo cosa fa un giro fermato a richiesta, vedi 1.3), tutto MISURATO:
- Lavaggi falliti: 0 in 41,8 minuti. Giri fermati senza progresso: 0.
- Caricati 4,59 e 4,82 GB (più del doppio di main nello stesso tempo), poi fermato dal controllo del disco (meno di 6 GB liberi sulla macchina). Velocità per caricatore da 2,5 a 1,9 MB/s, senza crolli; tra due righe di avanzamento del caricatore al massimo 32 secondi (ogni riga è ogni 30 secondi: nessuna fermata).
- Allo stesso punto di main (circa 2 GB per caricatore, dopo 16 minuti): 0 lavaggi falliti contro 24, 132 scritture oltre 10 secondi contro 823, nessuna oltre 60 secondi contro 32, la più lunga 40,7 secondi contro 72,7.
- Su tutto il giro: 276 scritture oltre 10 secondi, 0 oltre 60.
- Un rimescolamento grande è finito: app_data_6 ha una pila da 3.879 MB in 16 fogli (SR0), fatta mentre il carico continuava. Con il tetto di 30 secondi non poteva finire.
- Il banco L0 (campionato ogni 3 secondi) va su e giù tra 0 e 8: arriva a 8 per qualche secondo e torna giù. È la frenata normale quando i caricatori scrivono più in fretta di quanto la CPU lava, non un blocco.
- Dispensa: alla fine 11,5 e 9,1 GB su disco con 6,6 e 6,9 GB di fogli in uso. La differenza sono i fogli che un rimescolamento in corso sta scrivendo (le pile vecchie restano finché lui non finisce) e quelli sostituiti da meno di 120 secondi. Durante il giro la dispensa è salita e scesa (per esempio app_data_6 da 10,1 a 6,0 GB quando è finito il rimescolamento grande): la spazzatura si pulisce mentre il lavaggio continua. Lo spazio in più durante un rimescolamento (fino a circa il doppio delle pile che rimescola) è normale per questo tipo di lavaggio.

### 1.5 Test

- `compaction_pass_longer_than_its_stall_window_finishes`: descritto sopra. Fallisce su main, passa con la correzione.
- `compaction_pass_stops_when_preempted`: un giro su una dispensa lenta, a cui drop o fork chiedono di fermarsi, finisce entro pochi secondi e non è un errore.
- I test di prima sul giro che si blocca (`compaction_pass_ends_when_stopped_while_starting`, `compaction_pass_ends_when_stop_hangs`, `failed_compaction_backs_off`, `always_failing_compaction_is_not_retried_every_round`) passano: la dispensa che non risponde ferma ancora il giro.
- Libreria del motore: 17 passati, 0 falliti, 8 ignorati come prima (erano 14: più i 3 nuovi, i due del lavaggio e quello del conto di sfondo) (MISURATO).
- La prova di carico qui sopra (`mongo-tests/l1_compaction_load.sh`): 0 lavaggi falliti.

### 1.6 Cosa resta aperto sul lavaggio

- **Un rimescolamento fermato si perde.** slatedb salva i progressi solo a fogli interi (256 MB per pezzo). Uno spegnimento del server, un drop o un fork a metà di un rimescolamento di ore lo fanno ripartire da zero, e i fogli già scritti diventano spazzatura (la pulizia li toglie dopo 120 secondi). Prima succedeva ogni 30 secondi, adesso solo in questi casi. Rimedio possibile: fogli di uscita più piccoli per il lavaggio, o più pezzi per rimescolamento. È una scelta di ingegneria: la decide il fondatore. Prova che decide: un rimescolamento da 10 GB su S3 fermato a metà, quanto lavoro si perde con 256 MB e con 64 MB.
- **Nessun tetto alla CPU del lavaggio di tutti i database insieme.** I rimescolamenti lunghi non tengono più il posto di lavoro, quindi con molti database grandi possono girare in tanti insieme. Ognuno usa al massimo i 4 lavori e 4 pezzi di slatedb. Prova: 20 database che rimescolano insieme su una macchina da 8 vCPU, con le letture degli altri clienti.
- **Scritture lente rimaste.** Con la correzione restano scritture oltre 10 secondi (276 oltre 10 secondi in tutto il giro, il più lungo 40,7 secondi; nessuna oltre 60): il banco L0 arriva a 8 per qualche secondo perché su 2 vCPU caricatori, server e lavaggio si contendono la CPU. Non sono più blocchi senza fine; con CPU separata (cucina vera) NON SO, è da misurare.
- **Su S3 vero NON SO.** Tutto qui è su disco locale.

---

## 2. E1 · hostInfo vero

`mongo-server/src/hostinfo.rs` (nuovo) riempie la scheda di mongod con i numeri della macchina:
- `numCores` e `numCoresAvailableToProcess`: le CPU che il processo può usare (CPU accese, ristrette da affinità e quota del cgroup). È il campo che `somma_massimo.py` legge come "core del primario".
- `memSizeMB`: MemTotal di /proc/meminfo. `memLimitMB`: il più piccolo tra quello e il limite di memoria del cgroup.
- `numPhysicalCores`, `numCpuSockets`, `cpuArch`, sistema operativo, kernel, flag della CPU, pagina, file aperti: dalla macchina.

Su questa macchina: `numCores` 2, `memSizeMB` 8.022, `memLimitMB` 8.022, `numPhysicalCores` 2 (MISURATO). Prima: 4 e 16.091.

Test: tre test Rust in `hostinfo.rs` (lettura di meminfo, cgroup, cpuinfo, os-release; la scheda uguale alla macchina) e la prima parte di `mongo-tests/l1_host_reopen_test.py`, che confronta la risposta con `os.sched_getaffinity` e `/proc/meminfo`: 5 controlli su 5 (MISURATO).

## 3. E2 · Le letture della riapertura fuori dal conto del primo ordine

Cosa ho fatto (`src/meter.rs`, `src/lifecycle.rs`, `mongo-server/src/diag.rs`):
- `meter::background(db, lavoro)`: le letture fatte da un lavoro di sfondo di un database vanno nel conto `BACKGROUND`, non sull'ordine in corso. Vale per le letture fatte dentro il lavoro e per quelle che slatedb fa in task suoi per quel database mentre il lavoro gira.
- Lavori di sfondo: l'apertura e la riapertura del database (`Lifecycle::wake`), il passaggio da lettore a scrittore (`Tenant::upgrade`), la scrittura delle voci del profiler (che fa proprio quel passaggio).
- Il conto si vede in `serverStatus.fenvix.backgroundReads` (GET, byte, microsecondi).
- **Seconda causa, trovata dal test:** ogni apertura di un lettore o di uno scrittore prendeva un bancone nuovo (scope della cache). Dopo una riapertura, la prima scrittura (anche quella del profiler) cambiava il cuoco da lettore a scrittore e buttava tutto quello che il lettore aveva già portato sul bancone. Adesso lo scrittore tiene lo scope del lettore: stesso database, stessi fogli. Questo vale anche fuori dal profiler: ogni database che dopo una riapertura riceve la prima scrittura non riparte più con il bancone vuoto.

Test `mongo-tests/l1_host_reopen_test.py` (server con `--idle-secs 2`, 40 MB di documenti, profiler a 2, 6 giri: il database si chiude, poi lo stesso find per _id due volte), tutto MISURATO:
- binario K1 (c90c44f): lettura calda 393–760 KB in 6 giri su 6 (dovrebbe essere 0), nessun conto di sfondo; hostInfo 4 core e 16.091 MB: 5 controlli falliti su 8;
- solo il conto di sfondo, senza la correzione del bancone: la lettura calda leggeva ancora 383–554 KB, la riapertura 135–147 KB nel conto di sfondo;
- binario finale: lettura calda 0 byte in 6 giri su 6; lettura fredda 0,45–1,2 MB (i filtri e gli indici dei fogli che tocca, più il suo blocco: sono suoi, li deve leggere lei); conto di sfondo cresciuto a ogni riapertura (6 su 6). Tutti i controlli passati.
- Test Rust `background_work_is_charged_to_no_command` in `meter.rs`.

Il suggerimento del revisore per il giro (`--idle-secs` più lungo della pausa dello script, per esempio 600) non è codice: lo decidono K2b o il coordinatore. Con E2 corretto il conto per ordine non si sporca più comunque; resta vero che ogni fase partirebbe da una dispensa riaperta, cosa che Atlas non fa.

## 4. Note del revisore M1, M2, M3 (da aggiungere al rapporto di K1)

- **M1. Nel parallelo i byte letti per ordine sono una media, non una misura.** Quando più ordini lavorano sullo stesso database, le letture fatte nei task di slatedb si dividono in parti uguali tra loro, e la voce porta `fenvixS3Shared: true` (nel giro M30 corto del revisore 2.698 voci su 5.400, in pratica tutte le letture fredde a 10, 50 e 100). Quindi nel parallelo i campi dello script calcolati da quei byte ("a 0 byte dalla dispensa", byte e tempo di lettura dallo storage, p95, p99, "solo byte>0") per noi sono STIMATI e non si confrontano uno a uno con Atlas. Nel parallelo si confronta solo `millis`. Va scritto anche nel confronto finale.
- **M2. Cosa non è provato del giro M30.** Il revisore ha provato le righe m30s e m30p con i parametri veri, su 2 GB, su disco locale, su 2 vCPU. Restano non provate: le righe m10s, m10p e "vecchio" (stesse fasi, altri database); la taglia vera (3 GB su Atlas); S3 vero; una macchina con più di 2 vCPU. Il lavaggio corretto qui non cambia questo elenco: va riprovato lì.
- **M3. Un database caricato con un binario vecchio si riconta con la porta chiusa.** Se la cucina della prova usasse un database caricato prima di K1 (per esempio un "vecchio" da 10,7 GB), la prima istantanea delle statistiche lo riconterebbe tenendo la chiave del database: circa 21 secondi per GB su disco (MISURATO da K1), su S3 NON SO. I database del giro vanno caricati con questo binario. Lo stesso vale per i database caricati con main: il giro di main qui sopra non va riusato.

## 5. Il giro piccolo dello script contro il server corretto

Stessi comandi di K1 e del revisore (`carica --gb 0.1 --mb-cliente 0.15 --thread 4 --lotto 500 --ogni-dbstats-s 10`, poi `giro --senza atlas_inizio,atlas_fine` con i parametri piccoli di `gira_noi.sh`), binario finale, store su disco, database nuovo:
- caricamento 0,10 GB, uscita 0;
- giro: 22 fasi su 22 "fatta" (inizio, Q2, Q3, Q1, Q1d, Q4, Q5, spiega, condizioni, mentre_scrivi, parallelo, carico_massimo, scritture, cancellazione, grandi_carica, grandi_lavaggio, grandi_Q2, grandi_Q1, grandi_Q3, grandi_Q4, freddo, fine), nessuna riga ERRORE, uscita 0, circa 530 secondi (K1: 539);
- nel registro del server nessun lavaggio fallito;
- md5 dello script copiato per il giro: 8396ca455557baaa4e3a9b1f881451f9;
- come sempre L49 e L50 (lettura dal secondario) NON POSSIBILE: non abbiamo secondari (limite noto, non di questo compito).

Lo stesso giro l'ho fatto anche con il binario ba21719 (prima dell'ultima correzione della fermata a richiesta): stesso risultato, 22 su 22.

## 6. Altri test fatti

Tutti con il binario finale 8e71776, tutto MISURATO:
- `mongo-tests/k1_stats_test.py` (i contatori, serverStatus, explain e profiler di K1): 64 controlli passati, 0 falliti.
- `mongo-tests/l1_host_reopen_test.py`: tutti passati (sopra).
- Test Rust di mongo-server: 76 passati, 0 falliti (erano 73, più i 3 di hostInfo).
- Libreria del motore: 17 passati, 0 falliti, 8 ignorati.
- Test ctl (pytest, moto al posto di MinIO), scelti perché toccano quello che ho cambiato (lavaggio, pulizia, lucchetto, drop, fork, export, erase, restore, spegnimento) più quelli di K1: test_02_fork, test_03_restore, test_04_export, test_05_erase, test_09e_local_store, test_12_shutdown, test_22_wal_gc_stored_manifest, test_24_local_log, test_26_drop_forgets, test_28_bulk_insert_stall, test_29_compat_data_bugs, test_30_fork_limits_txn_collation, test_32_sort_top1, test_33_metadata_cache, test_35_fix_piccoli, test_36b_review_fixes, test_41_commit_retry: 144 passati, 1 saltato dal test stesso, 0 falliti (test_03_restore a parte: 11 su 11, anche con K1 e con ba21719).
- La volta che test_03_restore è fallito (ba21719, con lo script che girava sulla stessa macchina) è spiegata in 1.3.

## Cosa ho deciso io e perché

1. **"Nessun progresso per 30 secondi" invece di "30 secondi".** Il tetto serviva contro la dispensa che non risponde; il progresso misura proprio quello e non dipende dalla taglia del database.
2. **Il progresso si misura dai byte dei fogli che il compattatore legge e scrive**, contati dal suo store, senza richieste in più. Le letture del manifesto e dello stato non contano: il compattatore le fa sempre, anche fermo.
3. **Il rimescolamento lungo tiene il lucchetto del database ma non il posto di lavoro.** Il lucchetto protegge i fogli che sta scrivendo da drop e fork; per non bloccarli ho aggiunto la richiesta di fermata. Il posto lo restituisce, così gli altri clienti non aspettano.
4. **Non ho cambiato le impostazioni di slatedb** (dimensione dei fogli di uscita, numero di lavori): sono scelte di ingegneria, le scrivo in 1.6 come proposta.
5. **Lo scope della cache tenuto nel passaggio da lettore a scrittore**: non era nel compito, ma era la seconda causa di E2 e costava letture vere a ogni database dopo una riapertura.

## Cosa manca o non so

- S3 vero e MinIO: NON SO (niente MinIO qui, come per K1).
- La suite di confronto con mongod (run_py.py): non l'ho fatta girare, mongod non si scarica da qui (come K1).
- Gli altri test ctl (oltre a quelli elencati in 6) non li ho fatti girare: non toccano il lavaggio né il lucchetto, e su 2 vCPU la lista intera dura ore.
- Dopo la fine del caricamento non ho guardato la dispensa tornare alle sole pile in uso: il server si è fermato insieme ai caricatori. Durante il giro la pulizia ha lavorato (la dispensa saliva e scendeva), ma il valore a riposo è NON SO.

## File toccati (per l'unione con T1, che lavora sulle letture)

- `src/lifecycle.rs`: giro di lavaggio, pulizia, scope della cache, lavori di sfondo in wake e upgrade, test nuovi.
- `src/meter.rs`: conto delle letture di sfondo.
- `mongo-server/src/diag.rs`: hostInfo, profiler come lavoro di sfondo, sezioni `fenvix.compaction` e `fenvix.backgroundReads` di serverStatus.
- `mongo-server/src/hostinfo.rs` (nuovo), `mongo-server/src/main.rs` (una riga: il modulo).
- `mongo-server/src/dbs.rs`: richiesta di fermata del lavaggio nel drop; commento di `collation_of` rimesso al suo posto (nota 5 del revisore).
- `mongo-server/src/ctl/erase.rs`, `export.rs`, `fork.rs`, `pitr.rs`: richiesta di fermata del lavaggio prima del lucchetto.
- `examples/inspect_compactions.rs` (nuovo): stampa pile, L0 e compattazioni di un database su store locale.
- `mongo-tests/l1_host_reopen_test.py` e `mongo-tests/l1_compaction_load.sh` (nuovi).

Il percorso delle letture (find, scan, piani) non l'ho toccato. Possibili punti di contatto con T1: `meter.rs` (se T1 cambia il conto delle letture per ordine) e `lifecycle.rs` (`scan_prefix`, se T1 lo cambia): io ho toccato solo `wake`, `upgrade`, `open_reader`, `writer_builder` e la parte del lavaggio e della pulizia.

## Errori o mancanze trovati nel brain

- Il rapporto del revisore di K1 dice (punto 5) che i fogli buttati "sono spariti solo quando il database è stato riscritto". Con main la pulizia dei fogli gira e li toglie dopo 120 secondi (MISURATO: righe SSTGC con decine di fogli cancellati), ma solo fuori dai giri di lavaggio. Con giri falliti ogni 30–90 secondi la spazzatura cresce più in fretta di quanto si pulisce (STIMATO).
- `sessions/README.md` chiede "Numeri contro le soglie": il prompt L1 non ha soglie numeriche; ho usato main contro correzione con lo stesso carico.

## Link

- Branch: https://github.com/Mircoguidetti/fenvix-engine/tree/motore-correzioni
- Commit: 7dbef19 (E1), 140d6db (E2), 4be1b66 (lavaggio senza orologio, scope della cache), ba21719 (pulizia durante il lavaggio, lettura del manifesto meno fitta, prova di carico), 2b2f154 (test della riapertura anche sui binari vecchi), 8e71776 (fermata a richiesta dopo il banco L0).
- Nessuna PR aperta: il prompt non la chiede.

## Revisore

NON PARTITO. Con il rapporto già nel brain ho chiamato `fire_trigger` con trig_01KasRKiF97wJyxCNvpGs7Pc una volta sola: l'ambiente ha negato il permesso ("Cowork Run Routine Now"). Non ho riprovato in altri modi. Il revisore va fatto partire a mano dal coordinatore o dal fondatore.
