APPROVATO CON CORREZIONI. Le misure di T1 si rifanno e tornano, i conti dei viaggi sono onesti, la regola di uscita regge l'ondata di clienti di passaggio, nessun dato sbagliato in nessuna prova. Tre correzioni prima di accendere `--waiter-tools` sui database grandi o con molti database: le mappe dei database chiusi o cancellati non escono mai, le mappe più grandi della RAM lasciano zero RAM ai dati, e il conto di 1 TB dimentica la RAM per l'elenco delle mappe sul disco e non mette un limite al caricamento.

# Revisore di T1 · Gli attrezzi del cameriere

Revisore (Opus), prompt `prompts/R-T1.md`. Spesa $0: niente AWS, niente Atlas, niente connettori, niente merge.
Ho letto il prompt di T1, il rapporto e tutto il diff `server-per-script...attrezzi-cameriere` (14 file, 2.336 righe).
`PROGRESSI.md` non esiste: né nel brain né nel branch. Lo scrivo qui per il coordinatore.
I miei test sono sul branch nuovo `rev-attrezzi-cameriere` di fenvix-engine (commit f713515, solo test in `src/counter.rs`). I file delle prove sono in `misure/`.

Come ho misurato: stessa macchina di T1 (2 vCPU, 7 GiB). Ho ricaricato da capo 1 GB con `prova_infinite.py carica --gb 1.0 --mb-cliente 0.25` (md5 8396ca455557baaa4e3a9b1f881451f9, 3.166.000 documenti) su un server con filtri a 20 bit, come il giro "fin" di T1. Poi ho fatto girare lo stesso `cold_reads.py` con un seme diverso (11 invece di 7), quindi su clienti diversi. S3 simulato con `FENVIX_SIM_S3=25,0.6,90`. Tutti i numeri sotto sono MISURATI con S3 simulato, salvo dove scritto.

---

## 1. Il criterio di finito, punto per punto

**Niente strato in mezzo: PASSA.**
- Il bancone (`src/counter.rs`) è una struttura dentro il processo del cameriere: RAM più file sul disco locale.
- Non c'è nessun servizio nuovo, nessuna rete in più, nessuna copia condivisa tra camerieri.
- I tre involucri dello store (`src/simstore.rs`) sono tutti dentro il processo. Il ritardo simulato si accende solo con la variabile `FENVIX_SIM_S3` e lo scrive sullo stderr.
- Niente Page Service travestito.

**La latenza simulata di S3 è onesta: PASSA.**
- Mediana 25 ms con una coda lognormale sigma 0,6. Il p99 teorico è 25 × e^(0,6 × 2,326), cioè circa 101 ms. M31 ha misurato su S3 vero p50 24–26 e p99 58–105: la coda simulata sta in alto, quindi è prudente.
- Ogni GET aspetta in più i suoi byte a 90 MB/s. Il ritardo parte dopo aver letto il file locale, quindi si aggiunge al tempo del disco: anche questo è prudente.
- Ogni richiesta ha un suo tiro a caso, indipendente dalle altre: è il modello di M31.
- Il "prima" e il "dopo" passano per lo stesso involucro.

**Quanti viaggi fa davvero ogni lettura fredda: PASSA a p50, NON PASSA "mai due in fila" a p95.**

Il contatore conta ogni passaggio da "nessuna richiesta in volo" a "una richiesta in volo" (`trips`) e la catena più lunga di richieste una dopo l'altra (`depth`). L'ho controllato a mano con la traccia di ogni GET (`FENVIX_SIM_TRACE`). Conta bene. Su 80 clienti per ricerca (40 dormienti e 40 attivi):
- Q2: 1 viaggio in 80 casi su 80.
- Q1: 1 viaggio in 73, 2 in 7.
- Q1d: 1 viaggio in 58, 2 in 21, 3 in 1.
- Q3: 1 viaggio in 80 su 80.
- Q4: "in fila" 1 in 46, 2 in 28, 3 in 6.

Da dove viene il secondo viaggio (MISURATO con la traccia):
- In Q1 e Q1d il "secondo viaggio" è quasi sempre la lettura in anticipo. Parte 70–120 ms dopo l'inizio, cioè dopo che la risposta è già andata al cliente, e il contatore la mette nella stessa catena. Il cliente non l'ha aspettata: la latenza lo conferma, Q1d p50 81,8 ms come Q1 82,0. Qui il contatore conta più del vero, non meno.
- In Q4 invece la seconda ondata è dentro la ricerca. Parte 10–20 ms dopo la prima, appena tornano le prime risposte. È lo stesso difetto di slatedb che T1 ha trovato: il rimedio con 8 KiB in più non basta sempre.
  - Con `--next-block-kb 0`, su 12 clienti: in fila 2–5 in 12 casi su 12.
  - Con 8 KiB (il default), senza lettura in anticipo: in fila 1 in 8 casi su 12.
  - Con 64 KiB: in fila 1 in 9 casi su 12.
  - File `misure/res_nb0.json`, `res_ra0.json`, `res_nb64.json`.

Non è un trucco e non cambia una decisione, ma la prima riga del rapporto di T1 ("una lettura fredda adesso fa un viaggio solo") va letta come "nella maggior parte dei casi". Lo metto nelle Note.

**Il peso delle mappe a 1 TB: il conto di T1 regge per il disco, ma è incompleto (correzione 3).**
- MISURATO su 1 GB: 346,7 MB di mappe in RAM, 72.522 voci, 49 pile. T1 aveva misurato 346,5 MB.
- Il conto lineare a 1 TB, circa 330 GB (STIMATO), torna.
- Mancano due cose: la RAM per l'elenco delle mappe sul disco, e un limite al caricamento delle mappe. Le spiego nella correzione 3.

**Il bancone a due piani prende la grandezza dalla macchina: PASSA.**
- RAM: 4.011 MiB, cioè il 50% dei 8.022 MiB della macchina (MISURATO dal log del server).
- Disco senza `--counter-disk-gb`: 12.481 MiB, cioè l'80% dei 15.602 MiB liberi (MISURATO).

**La regola di uscita protegge i clienti attivi: PASSA, con un'eccezione (correzione 2).**

Prova in piccolo, test nuovi in `src/counter.rs`:
- 3 clienti attivi da 20 documenti, letti 3 volte. Poi un'ondata di 300 clienti di passaggio da 20 documenti mai visti, circa 12 volte la RAM. Gli attivi tornano ogni 25 clienti di passaggio.
- Alla fine gli attivi trovano 78 blocchi su 78 in RAM, sia con un'ondata letta una volta sola (`rev_passing_wave_read_once`) sia con un'ondata letta due volte di fila, come chi ricarica la pagina (`rev_passing_wave_read_twice`).

Prova da capo a fondo (`misure/wave.py`, `misure/wave.log`):
- Server vero sul database da 1 GB, bancone con 600 MiB di RAM e senza disco, S3 simulato.
- 20 clienti attivi fanno Q1 tre volte. Poi passano 800 clienti di passaggio, ognuno con Q1 e la sua lettura in anticipo da 200 documenti.
- Ogni 100 clienti di passaggio gli attivi rifanno Q1: sempre 0 richieste a S3 e 439 blocchi su 439 dalla RAM, 2,8–3,2 ms in media.
- Sono usciti 39.148 blocchi, tutti presi in anticipo e mai letti, come chiede il prompt.

L'eccezione: quando le mappe occupano quasi tutta la RAM, i clienti attivi non hanno più posto (correzione 2).

**Scaldare al primo segnale: PASSA.**
- La pagina dopo dello stesso cliente: p50 5,5 ms con 0 richieste a S3. Prima: 94,8 ms con 2 viaggi in fila.
- Il 5,5 ms comprende il driver Python. Su questa macchina anche Q1 tutto caldo "prima" fa 4,7 ms dal cliente. L'obiettivo "sotto 1–2 ms" lato server: NON SO, come scrive anche T1.

**L'obiettivo "ogni lettura fredda circa 25 ms": PASSA solo per Q2, NON PASSA per le ricerche di più documenti, come dice T1.**
- Q2 p50 30,7 ms. T1 aveva misurato 29,9; con la richiesta doppia 25,6.
- Q1 82,0, Q1d 81,8, Q3 65,5, Q4 109,1 ms.
- La spiegazione di T1 (20 GET in parallelo, si aspetta la più lenta) torna con il modello: il massimo atteso di 18 tiri dà circa 72 ms più il resto.

**Nulla si rompe: PASSA.**
- Libreria: i 19 test di prima passano, 8 ignorati come prima. Più i miei 4: 3 passano, 1 fallisce apposta (correzione 2).
- mongo-server: 73 passati, 0 falliti.
- Test ctl con `--waiter-tools` ACCESO: 69 passati, 0 falliti in 659 s (`misure/ctl_on.log`). Stessi 9 file di T1, che li aveva fatti girare con l'opzione spenta. Ho usato un piccolo involucro (`misure/ms-waiter.sh`) che aggiunge `--waiter-tools --counter-ram-mb 256` e un disco da 1 GiB a ogni server dei test. Lo stesso giro con l'opzione spenta è stato interrotto dal riavvio della macchina: non l'ho rifatto, c'è quello di T1.
- Giro piccolo dello script (`prova_infinite.py misura`, fasi Q2, Q3, Q1d, Q4, Q5, spiega) con l'opzione accesa e S3 simulato: 0 errori, tutti i controlli ok (`misure/giro.log`).

**I numeri prima e dopo, rifatti (p50 / p95 / p99 in ms):**
- Q2: prima 37,5 / 108,2 / 174,0; dopo 30,7 / 72,3 / 80,6.
- Q1: prima 222,6 / 291,5 / 334,2, con 33 GET; dopo 82,0 / 154,9 / 191,5, con 18 GET.
- Q1d: prima 204,2 / 298,2 / 314,1; dopo 81,8 / 133,3 / 147,3.
- Q3: prima 184,4 / 290,0 / 343,3; dopo 65,5 / 115,5 / 137,5.
- Q4: prima 310,2 / 594,4 / 667,6, con 80 GET; dopo 109,1 / 175,0 / 209,6, con 54 GET.
- GET su tutto il giro: prima 17.784, dopo 32.053. La differenza è la lettura in anticipo: dei blocchi presi in anticipo ne sono stati letti il 39% (9.707 su 24.824).
- Memoria del processo: prima 61 MB all'apertura e 162 a fine giro; dopo 635 e 808 MB.

Tutti nello stesso verso e della stessa grandezza di T1.

## 2. Le correzioni (errori provati)

**Correzione 1. Le mappe dei database chiusi o cancellati non escono mai dal bancone.**
- Prova (MISURATO, server con l'opzione accesa):
  - due database da 30.000 documenti, aperti;
  - mappe 6.980.320 byte;
  - `dropDatabase` del primo;
  - dopo 1, 5, 15 e 30 secondi le mappe sono ancora 6.980.432 byte, con `mapsForgotten` a 0.
- Sul giro da 1 GB: dopo il `dropDatabase` restano 346.672.910 byte di mappe e 136 MB di dati, uguali a prima, per tutta la durata della prova (`misure/probe_after.log`).
- Il perché nel codice:
  - il giro delle mappe guarda solo i database aperti (`src/maps.rs` righe 69–77, `open_writers` e `open_readers`);
  - il solo punto che dimentica le mappe è `forget_ssts`, chiamato quando cambia il manifest di un database aperto;
  - un database chiuso o cancellato resta per sempre in `MAPPED` e nel bancone, fino al riavvio del processo.
- Per il cliente il database cancellato non si legge più: `count` 0, `find` vuoto, come senza l'opzione (MISURATO). Quindi la garanzia 6 vista dal driver regge.
- Ma le chiavi degli indici del cliente cancellato restano nella memoria e sul disco del cameriere.
- E con il primo mercato (migliaia di database di agenti) le mappe di tutti i database mai aperti si accumulano. Per la regola di `balance` hanno la precedenza sui dati (correzione 2).
- Cosa fare:
  - al `dropDatabase` dimenticare subito le sue pile;
  - le mappe di un database chiuso da tempo devono poter uscire dalla RAM e poi anche dal disco, invece di restare fissate.

**Correzione 2. Quando le mappe riempiono la RAM, ai dati dei clienti attivi resta zero.**
- Il perché nel codice: in `src/counter.rs` riga 436 lo spazio per i dati è "RAM meno mappe". Le mappe escono dalla RAM solo quando da sole superano tutta la RAM (riga 449). Quindi, con mappe più grandi della RAM, i dati hanno 0 byte.
- Prova in piccolo (test `rev_maps_leave_room_for_active_data`, MISURATO):
  - bancone da 512 KiB;
  - le mappe ne prendono 480 KB;
  - dati in RAM: 3 KB; protetti: 0;
  - i clienti attivi trovano in RAM 0 blocchi su 78: tutti mancati.
- A 1 TB le mappe (circa 330 GB, STIMATO) sono più grandi di qualunque RAM. Il piano RAM serve solo mappe, e ogni blocco di dati letto passa per la RAM ed esce subito sul disco: una scrittura su NVMe per ogni lettura.
- Questo tocca la scelta 1 che T1 porta al fondatore. L'opzione (a), "mappe sempre sul disco locale", oggi il codice non la fa: le tiene in RAM per prime.
- Cosa fare: una quota minima della RAM riservata ai dati (o un tetto alle mappe in RAM), e le mappe in più sul disco.
- Il test resta sul branch e oggi fallisce: passerà quando c'è la quota.

**Correzione 3. Il conto di 1 TB dimentica la RAM dell'elenco e il caricamento non ha limiti.**

(a) La RAM per sapere dove stanno le mappe sul disco.
- Per ogni voce sul disco il bancone tiene in RAM:
  - una voce nell'indice del disco: 64 byte per voce (MISURATO con `size_of`, test `rev_bookkeeping_sizes`), più l'ingombro della tabella;
  - la chiave nella lista del segmento: 40 byte;
  - l'offset nel gruppo dei blocchi-mappa: 8 byte più l'ingombro.
- In tutto circa 120–200 byte per voce (STIMATO).
- MISURATO: 72.522 voci di mappe per 1 GB di documenti. A 1 TB sono circa 69 milioni di voci, cioè circa 8–14 GB di RAM (STIMATO).
- Questa RAM sta fuori dal 50% del bancone e non è nel conto di T1. Su una macchina da 32 GiB come la m7gd.2xlarge del rapporto ci sta a fatica; su una da 16 GiB no.

(b) Il caricamento non ha limiti.
- `src/maps.rs` riga 145 (`join_all`) carica tutte le pile nuove insieme.
- `src/simstore.rs` riga 407 (`buffered(n)`) manda tutti i pezzi di un blocco grande insieme.
- Su 1 GB: 651 richieste in volo insieme (MISURATO da T1, `maxReadsInFlight`).
- A 1 TB un cameriere che apre il database chiede tutte le circa 1,4 milioni di richieste in un colpo. I byte di ogni pila restano in memoria finché la pila non è arrivata tutta: il picco di memoria può arrivare vicino ai 330 GB (STIMATO dal codice, non misurato).
- Cosa fare: un tetto alle pile e alle richieste in volo durante il caricamento delle mappe. Poi rifare la stima dei 4–6 minuti per un cameriere nuovo con quel tetto e con il limite di S3 per prefisso.

## 3. Le dieci garanzie

1. **Nessuna scrittura confermata persa: non toccata.** Il percorso delle scritture non cambia. Con i filtri a 10 bit (default) il writer è identico; con un altro valore cambia solo la politica dei filtri delle pile nuove. Ho fatto girare con l'opzione accesa test_12_shutdown e test_41_commit_retry: passano. Le 100 prove con kill -9 non le ho fatte, perché il compito non tocca la scrittura.
2. **Atomicità: non toccata.** Stesso motivo.
3. **Leggi quello che hai scritto: provata, regge.**
   - Server con l'opzione accesa, bancone di 12 MiB senza disco (quasi ogni lettura va a S3), richiesta doppia a 4 ms, memtable da 1 MB scaricata ogni 2 s, compattazioni accese.
   - Ogni aggiornamento è riletto subito: in tre giri 10.531, 3.249 e 2.172 aggiornamenti, 0 sbagliati.
   - Le pile cambiate dalla compattazione sono dimenticate: `mapsForgotten` da 0 a 5 (MISURATO, `misure/g_run1.log`, `misure/g.log`).
4. **Indici coerenti con i dati: provata, regge.**
   - L'aggiornamento cambia anche il campo indicizzato.
   - Ogni 200 aggiornamenti si confronta la ricerca per indice con la verità; alla fine tutti i 40 valori, in due database.
   - 0 differenze.
5. **Isolamento tra clienti: provata, regge.** Due database con gli stessi `_id` e valori diversi, letti sotto stress e con S3 che sbaglia: 0 valori scambiati. Il bancone usa come chiave il nome della pila (ULID, mai riusato) e non il nome del database. È sicuro perché due database diversi non hanno mai la stessa pila.
6. **Cancellazioni complete: dal driver sì, dentro il cameriere no.** Vedi la correzione 1.
7. **Risposte uguali a un mongod vero: non toccata.**
   - Le ricerche restituiscono gli stessi documenti: cambia solo da dove arrivano i blocchi.
   - I test di compatibilità test_29 e test_32 passano con l'opzione accesa.
   - Qui non c'è un mongod per il confronto run_py.py: NON SO, come T1.
8. **Un solo scrittore per database: non toccata.** Il lettore della lettura in anticipo è un `DbReader` in modo `FollowLatest`, che non crea checkpoint e non scrive il manifest (slatedb 0.17, `db_reader.rs` riga 306).
9. **Niente dati rovinati quando S3 sbaglia: provata, regge.**
   - Per 8 finestre di 3 secondi ogni GET di una pila fallisce apposta: 78 errori di S3 iniettati.
   - Intanto il cliente legge di continuo documenti piccoli e documenti da 3 MiB (letti a pezzi).
   - 3.245 letture, 0 sbagliate. Le letture aspettano che S3 torni (slatedb riprova), non restituiscono dati rotti.
   - Sono passate per tutti i pezzi nuovi: 14.240 richieste doppie, 620 GET a pezzi, 5.642 risposte dal blocco seguente.
   - Alla fine i documenti grandi hanno tutti lo stesso SHA-256 dell'originale.
10. **Sicurezza e ripristino: non toccata nel ripristino.** test_02_fork e test_03_restore passano con l'opzione accesa. Sul disco del bancone vedi le Note.

## 4. Note (errori piccoli, non bloccano)

- La prima riga del rapporto di T1 dice "un viaggio solo". Contati (sezione 1):
  - Q1d fa 2 viaggi nel 26% dei casi (40% nel giro "fin" di T1);
  - Q4 ha 2–3 richieste in fila nel 42% dei casi (17% in T1);
  - va detto con le percentuali;
  - per Q1 e Q1d è soprattutto la lettura in anticipo contata nella stessa catena: conviene separarla nel contatore;
  - per Q4 il blocco seguente a 64 KiB aiuta (sezione 1).
- Il commento in testa a `src/counter.rs` (riga 13) dice che un blocco preso in anticipo e letto una volta viene "segnato". Il codice (riga 550) lo rimette in prova senza segno. Il comportamento va bene, il commento no.
- Richiesta doppia: se la seconda copia fallisce prima che arrivi la prima, si restituisce l'errore senza aspettare la prima (`src/simstore.rs` riga 305). Non porta dati rotti, solo un errore evitabile. Va bene che la richiesta doppia resti spenta, come propone T1.
- Il lettore della lettura in anticipo usa il tempo di controllo del manifest di default di slatedb, non `LONG_POLL` come gli altri lettori del motore: qualche richiesta in più per ogni database letto in anticipo, per 120 s.
- I blocchi sul disco del bancone non hanno un controllo (checksum) quando si rileggono: un bit rovinato sul NVMe tornerebbe al cliente. Rischio, non errore provato.
- La grandezza della RAM legge il limite del cgroup solo nella versione 2 (`/sys/fs/cgroup/memory.max`), non nella versione 1.
- Q1 "caldo", rifatto subito dopo il freddo, è più lento con l'opzione: 9,4 ms contro 4,7 su questa macchina. T1 aveva 4,4–5,5 contro 3,0. L'ipotesi di T1 (la lettura in anticipo gira sulle stesse 2 vCPU) è coerente, ma non l'ho provata: NON SO.
- `listDatabases` mostra ancora un database dopo `dropDatabase`, con o senza l'opzione (MISURATO). È del motore di prima, non di T1.
- `PROGRESSI.md`, citato nel prompt del revisore, non esiste.

## 5. Elenco esatto delle correzioni

1. Al `dropDatabase` dimenticare subito mappe e blocchi del database. Le mappe dei database chiusi devono poter uscire (prima dalla RAM, poi dal disco) invece di restare fissate. Test: dopo il drop `mapsForgotten` sale e i byte delle mappe scendono.
2. In `Counter::balance` una quota minima della RAM per i dati (o un tetto alle mappe in RAM). Le mappe in più vanno sul disco. Test: `rev_maps_leave_room_for_active_data` sul branch rev-attrezzi-cameriere deve passare.
3. Nel conto di 1 TB aggiungere la RAM dell'elenco delle mappe sul disco: circa 8–14 GB a 1 TB (STIMATO), o ridurla (per esempio tenendo le mappe sul disco per pila e non per blocco). Mettere un tetto alle pile e alle richieste in volo nel caricamento delle mappe (`src/maps.rs` riga 145, `src/simstore.rs` riga 407). Poi rifare la stima del tempo di un cameriere nuovo.
4. Nel rapporto (o nel brain): "un viaggio solo" con le percentuali vere (Q1d 2 viaggi nel 26–40%, Q4 2–3 in fila nel 17–42%), e il contatore che non metta la lettura in anticipo nella catena della ricerca.

## Link

- Branch dei miei test: https://github.com/Mircoguidetti/fenvix-engine/tree/rev-attrezzi-cameriere (commit f713515, nessuna PR).
- Branch rivisto: `attrezzi-cameriere` (733697b).
- Prove: `misure/`
  - `bench_*.log` e `res_*.json`: prima e dopo;
  - `wave.*`: ondata di clienti di passaggio;
  - `g.py` e `g*.log`: garanzie;
  - `probe_after.log`: drop sul giro da 1 GB;
  - `ctl_on.log`: test ctl con l'opzione accesa;
  - `giro.log`: giro piccolo dello script.
