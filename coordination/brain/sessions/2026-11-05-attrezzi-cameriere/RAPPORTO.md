PRONTO per la revisione, con un obiettivo mancato e detto in chiaro. I quattro attrezzi sono nel motore, provati e misurati in locale con S3 simulato. Una lettura fredda adesso fa un viaggio solo (MISURATO: "in fila" p50 1 e p95 1–2 su tutte le ricerche). I circa 25 ms li raggiunge però solo la lettura di un documento (Q2: p50 25,6–29,9 ms). Le letture di 11–60 documenti sparsi fanno un viaggio con 11–60 richieste in parallelo, quindi aspettano la più lenta: p50 63–103 ms. Le mappe pesano circa un terzo dei dati (1 TB ≈ 320–330 GB, STIMATO): non stanno in RAM, serve il disco locale. Servono due scelte del fondatore (sezione 7).

# T1 · Gli attrezzi del cameriere

Sessione di lavoro (Opus), prompt `prompts/T1-attrezzi-del-cameriere.md`. Spesa $0: niente AWS, niente Atlas, niente connettori, niente merge.
Branch `attrezzi-cameriere` di fenvix-engine, partito da `server-per-script` (c90c44f). Commit: 2c9bc13 (bancone, mappe, pezzi, metro), 7c3fbce (lettori, lettura in anticipo, richieste in parallelo, richiesta doppia, filtri, serverStatus, programma di prova), 733697b (blocco seguente). Codice nuovo tutto in inglese. Tutto è dietro l'opzione `--waiter-tools`, spenta di default finché il revisore non ha guardato: senza l'opzione il motore si comporta come prima.

---

## 1. Cosa ho costruito, un passo alla volta

**Le mappe sul bancone** (mappe = indici e filtri delle pile, cioè delle SST, più gli indici delle collezioni e i metadati). File `src/maps.rs`.
- Un giro ogni mezzo secondo guarda il foglio indice (manifest) di ogni database aperto, sia in scrittura sia in sola lettura.
- Per ogni pila nuova porta sul bancone tre cose insieme: l'indice della pila, i filtri e le statistiche. Poi porta i blocchi che contengono le chiavi diverse dai documenti: indici delle collezioni (`idx/`), metadati (`meta/`), ordine naturale (`nat/`) e le altre famiglie piccole. Tutte le pile sono caricate insieme.
- Le mappe sono fissate: i dati non le spingono mai fuori. Se la RAM è piena scendono sul disco locale, mai su S3. Si dimenticano solo quando la pila sparisce dal manifest (dopo un lavaggio).
- Una scelta che conta: il bancone non usa più l'"etichetta per apertura" di slatedb (cache scope). Una pila ha un nome che non si ripete mai e non cambia mai, quindi le mappe restano valide anche quando il database si chiude e si riapre. Nel motore di prima, a ogni riapertura il bancone era vuoto per quel database.

**Il bancone a due piani** (cache RAM più disco locale NVMe). File `src/counter.rs`.
- La grandezza viene dalla macchina: di default il 50% della RAM (o del tetto del cgroup) e l'80% dello spazio libero del disco indicato con `--counter-disk-dir`. Prima era fissa a 256 + 64 MiB.
- Regola di uscita del tipo 2Q / S3-FIFO:
  - un blocco nuovo entra "in prova";
  - un blocco letto in anticipo entra in una coda a parte ed è il primo a uscire se nessuno lo legge;
  - un blocco riletto mentre è in prova passa ai "protetti" (fino al 75% della RAM dei dati);
  - chi esce dalla RAM scende sul disco; un blocco ritrovato sul disco torna in RAM protetto.
- Test `exit_rule_protects_what_comes_back`: un cliente che torna spesso sopravvive a una scansione di 20 MB che passa per un bancone da 512 KiB (MISURATO). Test `two_floors_end_to_end`: i dati giusti dopo la discesa sul disco e il ritorno, e i blocchi presi in anticipo e mai usati escono per primi.

**Un viaggio solo.** File `src/simstore.rs`, `mongo-server/src/dbs.rs`.
- I documenti di una ricerca si chiedono tutti insieme: fino a 512 in parallelo, prima erano 32 alla volta.
- Un blocco grande si chiede a pezzi da 256 KiB in parallelo (da 512 KiB in su).
- **Difetto di slatedb 0.17 trovato e aggirato.** Se il documento cercato è il primo del suo blocco, slatedb legge prima il blocco precedente e poi quello giusto, uno dopo l'altro. Succede a circa 1 documento su 10 (MISURATO con la traccia delle GET: il secondo viaggio era sempre il blocco subito dopo). Rimedio: ogni lettura piccola di una pila si porta dietro gli 8 KiB che seguono, e la richiesta successiva si serve da lì. Stesso viaggio, qualche KiB in più. Da proporre a slatedb come correzione vera.
- Opzioni in più, provate e non tutte accese di default:
  - richiesta doppia (`--hedge-after-ms`): se una GET non risponde entro N ms se ne manda una seconda e vince la prima che arriva;
  - più bit per chiave nei filtri (`--filter-bits-per-key`).

**Scaldare al primo segnale.** File `mongo-server/src/prefetch.rs`.
- Alla prima lettura per indice di un cliente (per esempio "ultimi 20 messaggi") il cameriere porta in sottofondo i documenti delle 200 voci d'indice che seguono. Le voci sono già sulle mappe, quindi trovarle non costa richieste; i documenti arrivano in un viaggio.
- Se una lettura dopo va oltre la metà di quello che è stato portato (il cliente scorre indietro), porta le 200 successive. Senza un segnale non si legge niente in anticipo.
- Ho scelto di farlo dopo la prima risposta, non dentro di essa. Il cliente aspetta solo i suoi 20 documenti.

**Il metro.** `serverStatus` ha una sezione nuova `fenvix.waiter` con queste voci:
- viaggi;
- richieste;
- "in fila", cioè la catena più lunga di richieste dove ognuna parte dopo che un'altra è finita: è il numero di viaggi che la ricerca ha aspettato uno dopo l'altro;
- il bancone per piano;
- le mappe;
- la lettura in anticipo;
- le richieste doppie.

La latenza di S3 simulata (`FENVIX_SIM_S3=25,0.6,90`: mediana 25 ms, coda lognormale con sigma 0,6, cioè p99 circa 100 ms, più i byte a 90 MB/s, il modello di M31) si accende solo con quella variabile e lo scrive sullo stderr. Il programma della prova è `bench/waiter_tools/cold_reads.py`. Costruisce le ricerche con la funzione `compito()` di `prova_infinite.py`, importata e non toccata (md5 8396ca455557baaa4e3a9b1f881451f9).

## 2. Come ho misurato

- Macchina locale con 2 vCPU e 7 GiB di RAM. Store su disco locale con S3 simulato davanti.
- Database caricato con `prova_infinite.py carica --gb 1.0 --mb-cliente 0.25`: 3.164.500 documenti, dataSize 1,074 GB, indexSize 0,286 GB, 4.096 clienti di cui 1.228 dormienti.
- Due caricamenti uguali, uno con i filtri a 10 bit e uno a 20.
- Ogni giro parte da una copia fresca dello store, con il processo appena acceso: bancone vuoto, mappe caricate all'apertura.
- Per ogni ricerca (Q2, Q1, Q1d, Q3, Q4): 40 clienti dormienti e 40 attivi, mai letti prima, uno alla volta. Prima di ogni ricerca il programma aspetta che il server sia fermo.
- Tempi misurati dal cliente Python: comprendono il driver e la rete locale.
- "Prima" = stesso binario senza `--waiter-tools` (bancone 256 + 64 MiB come oggi). "Dopo" = con `--waiter-tools`.
- Tutti i numeri sotto sono MISURATI con S3 simulato. Per S3 vero sono STIMATI: il modello è stato confermato da M31 (GET p50 24–26 ms, p99 58–105).

## 3. I numeri, prima e dopo (filtri a 20 bit, p50 / p95 / p99 in ms)

**Q2, un documento per `_id`**
- Prima: 35,4 / 104,6 / 131,6; in fila p50 1, p95 3.
- Dopo: 29,9 / 75,1 / 84,7; in fila sempre 1 (massimo 1).
- Dopo con richiesta doppia a 40 ms: 25,6 / 59,7 / 69,3.

**Q1, ultimi 20 messaggi**
- Prima: 178,8 / 261,8 / 322,1; in fila p50 5 (p95 7); 31 GET.
- Dopo: 80,7 / 129,1 / 184,1; in fila p50 1, p95 1; 18 GET.
- Con richiesta doppia: 66,9 / 89,1 / 111,5.

**Q1d, 20 messaggi prima di una data**
- Prima: 170,0 / 238,1 / 255,5.
- Dopo: 76,1 / 150,3 / 177,4; in fila p50 1, p95 2.
- Con richiesta doppia: 65,6 / 96,3 / 104,0.

**Q3, eventi in 14 giorni**
- Prima: 160,3 / 230,1 / 276,5; in fila 5; 22 GET.
- Dopo: 68,1 / 128,5 / 190,3; in fila p50 1, p95 1; 11 GET.
- Con richiesta doppia: 62,6 / 85,7 / 95,0.

**Q4, 60 giorni, fino a 500**
- Prima: 266,4 / 522,7 / 730,2; in fila p50 8, p95 16, massimo 27; 77 GET.
- Dopo: 102,9 / 159,0 / 184,9; in fila p50 1, p95 2; 54 GET.
- Con richiesta doppia: 84,6 / 112,6 / 121,0.

**La pagina dopo dello stesso cliente** (Q1 e Q1d, il cliente scorre indietro, dopo la lettura in anticipo)
- Prima: p50 85–93 ms, con 2 viaggi in fila.
- Dopo: p50 3,1–3,3 ms, p95 3,8–4,6, nessuna richiesta a S3.

**Le stesse ricerche ripetute subito (caldo)**
- Prima: Q2 0,6, Q1 3,0, Q3 1,7, Q4 6,8.
- Dopo: Q2 0,6, Q1 4,4–5,5, Q3 1,6, Q4 6,7–7,0.
- Il caldo di Q1 dopo è circa 1,5–2 ms più lento. Causa NON SO. Ipotesi (STIMATO): la lettura in anticipo di quel cliente gira proprio in quel momento sulle 2 vCPU, oppure costa la chiave del bancone.

Con i filtri a 10 bit (primo caricamento) i numeri sono simili:
- prima: Q2 38,9, Q1 186,3, Q4 307,9;
- dopo, senza la lettura del blocco seguente: Q2 34,1, Q1 79,9, Q4 106,7, con in fila p50 2.

Il passaggio a 1 viaggio è venuto dalla lettura del blocco seguente, non dai 20 bit. Con 20 bit, senza quel rimedio, in fila restava p50 2 (giro `after20`).

**Documenti grandi** (8 documenti da 6 MiB, letti a freddo per `_id`)
- Prima: p50 138,8 ms, una GET da 6 MiB, due volte su 8 tre viaggi in fila.
- Dopo: p50 95,4 ms, 25 pezzi in un viaggio.

**Due piani con poca RAM** (giro `finram`: 450 MiB, di cui 347 di mappe)
- Tempi freddi uguali al giro con 4 GiB: Q2 26,7, Q1 66,3, Q4 81,7.
- 2.763 blocchi sono scesi sul disco, ed erano tutti blocchi presi in anticipo e mai letti: la regola di uscita fa quello che deve. 31 letture servite dal disco.
- Nessuna mappa scesa, nessuna persa.

**Memoria del processo (RSS)**
- Prima: 61 MB all'apertura, 158 MB a fine giro.
- Dopo: 679 MB all'apertura, 831 MB a fine giro. Di questi, 347 MB sono mappe e 136 MB dati sul bancone.

## 4. Quanto pesano le mappe, e a 1 TB

Misurato su 1 GB di documenti (dataSize 1,074 GB, 39–42 pile per 1,51–1,52 GB di pile):
- filtri a 10 bit: 334,0 MB in tutto. 299,2 MB sono blocchi di indici delle collezioni e metadati; 34,9 MB sono indici, filtri e statistiche delle pile;
- filtri a 20 bit: 346,5 MB. 299,2 + 47,3.

Per GB di documenti:
- circa 279 MB di indici delle collezioni;
- più 33 MB (10 bit) o 44 MB (20 bit) di mappe delle pile.

Il conto per 1 TB (STIMATO, lineare, stesso tipo di dati e stessi indici). 1.024 GB di documenti danno:
- indici delle collezioni 1.024 × 279 MB ≈ 285 GB;
- mappe delle pile 1.024 × 44 MB ≈ 45 GB;
- in tutto **circa 330 GB** (318 a 10 bit).

Da sapere:
- Il peso degli indici delle collezioni dipende dagli indici del cliente. Qui sono 4 indici secondari su documenti piccoli (310 byte in media): l'indice vale il 27% dei dati. Con documenti da 2 KB sarebbe molto meno per GB.
- Le mappe delle pile dipendono dal numero di chiavi e dai blocchi da 4 KiB. Con blocchi più grandi l'indice delle pile si riduce.
- A 1 TB le mappe non stanno in RAM. Stanno sul disco locale di una macchina con NVMe: una m7gd.2xlarge ha un disco da 474 GB (DICHIARATO, pagina AWS EC2 M7g, https://aws.amazon.com/ec2/instance-types/m7g/).
- Una prima lettura che trova le mappe sul disco invece che in RAM costa qualche decimo di ms in più, non un viaggio (STIMATO).

Problema da dire in chiaro (principio di disegno): **un cameriere nuovo deve caricare 330 GB di mappe prima di essere veloce su quel database.** Con la scheda di rete di una macchina media (circa 1–1,5 GB/s, STIMATO) servono circa 4–6 minuti.
- Oggi le mappe si caricano all'apertura del database: 1,0–1,1 s per 1 GB (MISURATO), circa 1.375 richieste.
- Il disco del bancone si svuota a ogni accensione: la persistenza non l'ho fatta.
- Per i database piccoli non cambia niente. Per quelli grandi serve una di queste scelte (sezione 7).

## 5. Perché le letture di molti documenti non scendono a 25 ms

Un viaggio solo non basta.
- Q1 chiede 20 documenti e il motore li tiene in ordine di `_id`. I 20 messaggi di un cliente stanno quindi in 20 blocchi diversi: 20 GET in parallelo.
- Il viaggio finisce quando arriva la più lenta. Con la coda di S3 (p99 circa 4 volte la mediana) la più lenta di 20 sta in media sui 70–80 ms (STIMATO dal modello, confermato dal MISURATO: Q1 p50 80,7).
- La richiesta doppia taglia la coda (Q1 p99 da 184 a 111 ms), ma costa GET in più. In questa prova sono state 9.396 su 41.043 (23%), comprese quelle della lettura in anticipo, perché la soglia fissa a 40 ms vale per tutte le richieste.
- Il rimedio vero è che i documenti di un cliente stiano vicini: il lavaggio per cliente di PLAN-LAVAGGIO. M31 con quella disposizione ha misurato Q1 a 28 ms con 1 GET. Oppure un indice che contiene i documenti.
- Questo non è un pezzo di questo compito, ma senza di esso l'obiettivo dei 25 ms vale solo per i documenti singoli.

## 6. Quanto costa in richieste a S3

Su tutto il giro (400 ricerche fredde, più le pagine dopo):
- prima: 17.284 GET;
- dopo: 31.911 GET.

Le GET delle ricerche stesse scendono da 13.844 a 9.550. Salgono per la lettura in anticipo: 49.138 documenti portati, cioè 24.590 blocchi, di cui 8.273 poi letti (34%). In questa prova ogni cliente legge una sola pagina dopo, quindi il 34% è il caso peggiore (MISURATO).

Prezzo (STIMATO, GET a 0,0004 $ ogni 1.000 come in NUMBERS.md): 24.590 GET in più sono circa 0,01 $. Va regolato il numero dei documenti presi in anticipo (`--read-ahead-docs`, oggi 200).

## 7. Scelte di disegno che servono al fondatore (le mie proposte; non le ho fatte)

1. **Mappe di un database grande.**
   - (a) Sempre tutte sul disco locale. Il cameriere che serve un database da 1 TB deve avere circa 1/3 di 1 TB di NVMe, e le mappe vanno salvate sul disco per sopravvivere a una riaccensione.
   - (b) In RAM o sul disco solo le mappe delle pile (45 GB per TB); gli indici delle collezioni nel bancone normale, con priorità alta. In questo caso la prima lettura di un cliente mai visto fa 2 viaggi.
   - (c) Comprimere i blocchi degli indici (lz4 o zstd), da misurare.
   - Proposta: (a) più (c), e il cameriere di scorta riceve le mappe da un altro cameriere invece che da S3. Si lega alla domanda già aperta in NOW sui "camerieri di scorta".
2. **Richiesta doppia.**
   - Proposta: accesa solo per le richieste dei clienti, non per la lettura in anticipo né per le mappe, con soglia adattiva (circa il p90 delle ultime GET) invece di 40 ms fissi.
   - Il codice di oggi la applica a tutto: per questo l'ho lasciata spenta di default.

## 8. Cosa ho deciso io e perché

- Tutto dietro `--waiter-tools`, spento di default: le altre sessioni e i test vedono il motore di prima. Senza l'opzione restano accesi solo i contatori e un passaggio che non fa niente.
- La chiave del bancone ignora l'etichetta per apertura di slatedb. È sicuro: le pile hanno nomi unici e immutabili, e slatedb 0.17 non mette i WAL in questa cache (lo controlla `db_cache/serde.rs`).
- La lettura in anticipo passa per un lettore a parte del database (DbReader con una vista del bancone che segna i blocchi "presi in anticipo"). Non si può fare con una variabile del task: slatedb legge i blocchi in task che lancia per conto suo (MISURATO: la prima versione segnava 0 blocchi).
- La lettura in anticipo parte dopo la prima risposta, non dentro di essa: la prima risposta non deve aspettare 200 documenti.
- `Tenant` in lettura tiene il suo DbReader in un `Arc`, così il giro delle mappe vede anche i database aperti in sola lettura. Nel motore i database si svegliano in lettura e passano in scrittura solo alla prima scrittura.

## 9. Test fatti (MISURATO)

- Libreria: 19 passati, 0 falliti, 8 ignorati come prima. Di questi, 5 sono nuovi: chiave senza etichetta, regola di uscita, due piani da capo a fondo, pezzi in parallelo, metro dei viaggi.
- mongo-server: 73 passati, 0 falliti.
- Test ctl (pytest con moto al posto di MinIO, binario nuovo, opzione spenta come i test la usano): test_02_fork, test_03_restore, test_09e_local_store, test_12_shutdown, test_26_drop_forgets, test_29_compat_data_bugs, test_32_sort_top1, test_33_metadata_cache, test_41_commit_retry. 69 passati, 0 falliti in 644 s (`misure/ctl.log`). La suite completa dei test ctl e il confronto con mongod (run_py.py) NON li ho fatti girare: NON SO.
- I giri della prova sono nella cartella `misure/`: per ognuno il JSON con ogni ricerca e il log. I nomi: `before*`, `after*`, `fin*`.

## 10. Cosa manca o cosa non so

- Le mappe sul disco non sopravvivono a una riaccensione del processo (vedi sezione 4).
- La lettura in anticipo vede le pile, non le scritture degli ultimi minuti: quelle stanno già nella memoria di chi scrive.
- I tempi sono dal cliente Python. Sotto i 2 ms per la pagina dopo non ci arrivo misurando dal cliente: 3,1–3,3 ms, con il driver Python dentro. Lato server NON SO.
- Il caldo di Q1 più lento di prima: causa NON SO (sezione 3).
- 2 vCPU: con la lettura in anticipo e il caricamento delle mappe in sottofondo la CPU è condivisa con le ricerche. Su una macchina vera NON SO.
- Il difetto di slatedb sul blocco precedente andrebbe corretto in slatedb (è una sostituzione o modifica di libreria: decide il fondatore). Il rimedio di oggi lo aggira.
- Su S3 vero non ho misurato niente (spesa $0). La prova vera è una giornata con la stessa riga di `bench/waiter_tools/cold_reads.py` contro il bucket.

## 11. File toccati (per l'unione con L1, che lavora su lavaggio e hostInfo)

- Nuovi:
  - `src/counter.rs`
  - `src/maps.rs`
  - `src/simstore.rs`
  - `mongo-server/src/prefetch.rs`
  - `bench/waiter_tools/cold_reads.py`
- Cambiati:
  - `src/lib.rs`
  - `src/meter.rs` (cache_stats legge anche il bancone nuovo)
  - `src/lifecycle.rs`:
    - `install_counter`;
    - `open_writers` e `open_readers`;
    - `Mode::Reader` con `Arc`;
    - filtri per writer e compattatore. Attenzione: una riga dentro la funzione del compattatore (`CompactorBuilder` in `compact_tenant_within`), possibile conflitto con L1.
  - `mongo-server/src/main.rs` (opzioni e collegamenti; catena dello store)
  - `mongo-server/src/coll.rs` (nome del database e aggancio della lettura in anticipo nel ramo indice ordinato con limit)
  - `mongo-server/src/dbs.rs` (letture in parallelo, nome nel ReadView)
  - `mongo-server/src/diag.rs` (solo la sezione `fenvix.waiter` di serverStatus; hostInfo non l'ho toccato)
  - `Cargo.toml` e `Cargo.lock` (bincode e libc, già nel lock)

## 12. Errori o mancanze trovati nel brain

- Il prompt dice che Q2 fa due viaggi in fila (indice, poi documento). Questo vale per il prototipo di M31 (biglietto `_id` → chiave). Nel motore `mongo-server` il documento sta sotto il suo `_id`: Q2 è una lettura sola. Prima di questo lavoro Q2 faceva 1 viaggio a p50 e 3 a p95, per i filtri non ancora caricati e per il difetto del blocco precedente.
- Il prompt mette "circa 25 ms" come obiettivo per ogni lettura fredda. Per le letture di più documenti sparsi l'obiettivo non è raggiungibile con un viaggio solo (sezione 5): andrebbe scritto in DESIGN o PLAN-LAVAGGIO che dipende dal lavaggio per cliente.

## Link

- Branch: https://github.com/Mircoguidetti/fenvix-engine/tree/attrezzi-cameriere (nessuna PR: il prompt non la chiede).
- Misure: `sessions/2026-11-05-attrezzi-cameriere/misure/`.
- Righe di comando: `misure/run_one.sh` e `misure/run_one2.sh`. Server "dopo": `mongo-server --store local --waiter-tools --counter-disk-dir DIR --counter-disk-gb 8 [--hedge-after-ms 40]`; S3 simulato con `FENVIX_SIM_S3=25,0.6,90`.

## Revisore

NON PARTITO. Ho chiamato fire_trigger sul task del revisore (trig_01DjZpgQWFpVNYLwuW5EBFed) una volta sola, dopo che il rapporto era nel brain. Il permesso è stato negato dall'ambiente ("Cowork Run Routine Now"). Il revisore va avviato a mano dal coordinatore o dal fondatore.
