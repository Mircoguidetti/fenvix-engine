# Isolamento tra database (S4, la tessera a punti)

Branch `isolamento`, ctl-test 36, codice in `mongo-server/src/fair.rs`. Macchina della prova: 2 CPU, 7 GB di RAM.
Ogni numero ha la sua etichetta: MISURATO, STIMATO, DICHIARATO (con la fonte) o NON SO.

## In breve

- La tessera a punti (unità per operazione) c'è: ogni comando paga in punti CPU, byte scritti, byte letti e richieste S3. Le stesse cifre servono alle quote e alla contabilità per database.
- Le quote per database ci sono: credito di raffica, coda propria oltre la quota, errore ritentabile oltre una profondità, impostabili per database dal pannello.
- La coda pesata si accende a 2 ms di ritardo (prima 10) e serve per primo il database che ha speso meno punti.
- Tetto di tempo JavaScript per comando: 30 s di default (`--js-command-max-ms`).
- Prova dei vicini rumorosi (50 pesanti + 1 leggero, motore su 1 CPU): il p99 del leggero passa da circa 50 ms a circa 22–25 ms, il p50 da circa 12 a circa 4 ms; i pesanti tengono il 99–102% del throughput di prima. **La soglia di 20 ms sul p99 non è raggiunta** (manca qualche millisecondo); quella dell'80% sui pesanti sì. Dettagli e causa sotto.
- Punto 3 (cgroup v2): non fatto, la macchina non ha il controller CPU in cgroup v2. Non simulato.

## 0. L'unità: i punti

Formula di un comando:

punti = 10 × ms di CPU + 0,5 × KiB scritti + 0,05 × KiB letti + 2500 × richieste S3 di tipo PUT + 200 × richieste S3 di tipo GET

Come si arriva ai pesi (si cambiano con `--unit-weights`):
- 1 punto vale circa 2 miliardesimi di dollaro, cioè il nostro costo di 0,1 ms di CPU sul nodo condiviso. STIMATO: nodo Graviton da 2 vCPU a circa $0,08 l'ora usato al 60%. Il prezzo esatto dell'istanza è NON SO (la ricerca web non ha mostrato la cifra; non ho aperto pagine).
- Una PUT costa $0,005 ogni 1000 e una GET $0,0004 ogni 1000 (DICHIARATO, listino S3 Standard, lo stesso usato in `tools/round3/common.py`): 2500 e 200 punti.
- Un KiB scritto paga le riscritture della compattazione (CPU fuori dal comando) e la memoria del memtable: 0,5 punti, STIMATO. Le richieste S3 della compattazione si pagano già come richieste.
- Un KiB letto paga la memoria della cache che occupa: 0,05 punti, STIMATO.

Cosa si conta e dove:
- CPU: il tempo dentro `poll` del comando sui thread di lavoro, più il tempo sul thread JavaScript (i comandi JavaScript girano su un thread a parte).
- Byte scritti e letti: chiavi e valori che il comando mette nei suoi lotti e prende dallo store (memtable, cache o S3).
- Richieste S3: addebitate al database dal percorso dell'oggetto (`tenants/{db}/...`), quando avvengono. Le scritture del registro, la compattazione e il rinnovo del lease avvengono dopo il comando, quindi vanno al database ma non al singolo comando.

Quanto vale, MISURATO in locale con `tools/isolamento/units.py` (store in memoria, documenti da 1 KB, 200 documenti per database, `results-isolamento/units-memory.json`):
- lettura per `_id`: 0,42 punti (37 µs di CPU, 970 byte letti);
- lettura per indice di 4 documenti: 0,9 punti;
- scansione di 200 documenti senza indice: 23 punti (1,4 ms di CPU, 195 KB letti);
- aggregazione `$group` su 200 documenti: 26 punti;
- `$where` JavaScript su 200 documenti: 82 punti (7,2 ms di CPU);
- inserimento da 1 KB, un client alla volta: 5001 punti, di cui 5000 di S3 (2 PUT per inserimento) e 1,2 di CPU e byte;
- lo stesso con 8 client insieme: 2776 punti (1,1 PUT per inserimento, grazie al commit di gruppo);
- aggiornamento per `_id`, un client: 5001 punti; con 8 client: 3192 punti;
- `insertMany` di 100 documenti: 5065 punti (51 per documento).

Cosa dicono questi numeri:
- Il costo vero di una scrittura isolata è quasi tutto S3: circa 10.000 volte una lettura per `_id`. Il commit di gruppo (più scritture per PUT) conta molto più della CPU per il prezzo per database.
- Ogni inserimento singolo fa 2 PUT (MISURATO). Perché 2 e non 1 è NON SO: da guardare (probabilmente l'oggetto del registro più un secondo oggetto per ogni scarico). Se fosse evitabile, il costo delle scritture isolate si dimezza.

Quanto pesa in RAM un database aperto (serve al prezzo per database), MISURATO con 300 database piccoli aperti uno dopo l'altro (`results-isolamento/ram-local.json`, store locale):
- aperto con una sola lettura: circa 51 KB per database in media, circa 34 KB per database dopo i primi 60 (la parte fissa del processo si paga una volta);
- aperto con tutta la collezione letta (200 KB di dati nella cache): circa 280 KB per database.
- Come affitto in punti: 50 KB per un'ora valgono circa 500 punti, cioè circa $0,0007 al mese per database sempre aperto (STIMATO, con 4 GB di RAM nel nodo da $0,08 l'ora). Il pannello lo può aggiungere ai punti delle operazioni; il motore oggi non lo addebita da solo.

## 1. Quote per database

- Ogni database ha un secchio di punti che si riempie alla sua quota (punti al secondo) fino al credito di raffica.
- Default per tutti: `--db-quota-pts` (0 = nessuna quota, il default di oggi) e `--db-burst-pts` (0 = 10 secondi di quota).
- Per database: `PUT /v1/databases/{db}/quota` con `{"pts_per_sec": N, "burst_pts": M}`, `GET` per leggerla, `DELETE` per tornare al default. La quota per database vive in memoria: un riavvio la perde e il pannello la deve rimettere (limite da sapere).
- Un comando parte se il secchio è positivo; all'inizio prende il costo atteso (media dei comandi recenti del database), alla fine si corregge con il costo vero.
- Oltre la quota il comando aspetta nella coda del suo database finché il secchio si riempie.
- Oltre `--db-quota-queue` comandi in attesa (256 di default) un comando nuovo riceve subito l'errore 462 con le etichette `SystemOverloadedError` e `RetryableError`: i driver recenti lo ritentano.
- Metriche nuove per database in `GET /v1/metrics/databases`: punti totali e per parte, punti al secondo, byte scritti e letti, PUT e GET, quota, credito, attese e rifiuti di quota, attese di scarico.

## 1b. Coda pesata sotto saturazione

- Si accende quando il ritardo dello scheduler supera 2 ms (media degli ultimi 4 giri da 10 ms) e resta accesa 500 ms dopo l'ultimo superamento.
- Accesa: al massimo 8 comandi per CPU in esecuzione e al massimo 4 database per CPU che scaricano le loro scritture insieme. Chi aspetta riparte in quest'ordine: prima chi sveglia un database, poi il database che ha speso meno punti di recente rispetto al suo peso (la sua quota). Il leggero, che spende poco, passa quasi sempre per primo.
- Un comando lascia il suo posto mentre aspetta che le sue scritture siano durevoli (non usa CPU) e prende un posto tra gli scarichi; i comandi dello stesso database condividono un solo posto di scarico (commit di gruppo).

Due difetti trovati e corretti nella coda di prima:
- Il tetto si calcolava a ogni comando con `available_parallelism`, che legge i file dei cgroup ogni volta: sotto saturazione costava circa metà del throughput dei pesanti (MISURATO: 7.200 contro 12.800 richieste al secondo, stessa prova, unica differenza il tetto scritto a mano). Ora si legge una volta.
- Un comando caduto mentre aspettava in coda (connessione chiusa) poteva lasciare occupato il suo posto per sempre. Ora il posto torna.

## 1c. Tempo massimo JavaScript per comando

- `--js-command-max-ms` (30.000 di default, 0 = nessun tetto): tempo JavaScript totale di un comando, sommando tutte le chiamate. Oltre: errore 50 `MaxTimeMSExpired` che nomina il flag.
- Perché serve: oggi ogni chiamata ha 60 s, ma un `$where` su un milione di documenti fa un milione di chiamate; e tutti i comandi JavaScript del processo girano su un solo thread, quindi un database fermerebbe il JavaScript di tutti.

## 2. Vicini rumorosi rifatti

Come (`tools/isolamento/noisy_quota.py`, risultati in `results-isolamento/noisy-finale.json`):
- stesso carico di `tools/round3/noisy.py`: 50 database pesanti, 4 connessioni ciascuno senza pause (metà letture per `_id`, metà inserimenti da 1 KB), più 1 database leggero a 2 richieste al secondo (metà e metà, arrivi casuali);
- motore da solo sulla CPU 0; client pesanti, client leggero sulla CPU 1 (il leggero con priorità alta, i pesanti bassa);
- 90 s per prova, ogni modo ripetuto 2 volte, 368 richieste del leggero per modo; "prima" = il binario di main di oggi (`71a435e`).

Tre scelte obbligate dalla macchina, da sapere:
- I client pesanti in Python (come nel round 3) su 1 sola CPU non saturano il motore (CPU del motore al 23–73%): ho aggiunto `loadgen noisy` in Rust, con lo stesso carico.
- Il finto S3 (moto, porta 9000) fa circa 130 PUT al secondo (7,5 ms per PUT, MISURATO) sulla CPU dei client: con lui il motore resta al 23% e la prova non misura l'isolamento. MinIO non si scarica (403 dal proxy, anche compilarlo da sorgente: il proxy dei moduli Go risponde Forbidden). Lo store locale passa tutte le scritture del processo da un solo lucchetto (`localcas.rs`), quindi misura quel lucchetto. Ho usato lo store in memoria: misura la contesa di CPU senza la latenza di S3. Con S3 vero ogni inserimento aggiunge la latenza di rete della PUT (che non usa CPU del nodo); le soglie sotto valgono per la parte di CPU.
- I numeri sono indicativi: 2 CPU in tutto, client e motore sulla stessa macchina.

Risultati (50 pesanti, motore su 1 CPU, CPU del motore 98–99%), MISURATO:
- Prima, coda spenta: leggero p50 11,9 ms, p99 48,8 ms; pesanti 11.695 richieste al secondo.
- Prima, coda accesa a 10 ms: leggero p50 11,4 ms, p99 52,1 ms; pesanti 12.219 al secondo.
- Nuova, senza quote: leggero p50 4,0 ms, p99 25,4 ms (le due prove: 21,0 e 25,9); pesanti 11.883 al secondo, il 99% di prima.
- Nuova con quote alte (1.000.000 di punti al secondo per database, i pesanti non le toccano): leggero p50 4,0 ms, p99 23,9 ms; pesanti 12.196 al secondo, il 102% di prima.
- Nuova con quote strette (200.000 punti al secondo, circa metà di quanto spende un pesante): leggero p50 3,3 ms, p99 21,8 ms; pesanti 10.184 al secondo, l'85% di prima, CPU del motore scesa all'82–88%.
- Nessun errore per il leggero né per i pesanti in nessuna prova.

Contro le soglie:
- p99 del leggero sotto 20 ms: **non raggiunto**. Siamo a 22–25 ms (era circa 50). Il p50 è sceso di 3 volte.
- Pesanti almeno all'80% del throughput di prima: **raggiunto** (99–102% senza quote strette, 85% con quote strette).

Dove si perdono gli ultimi millisecondi (dalle misure):
- Le attese del leggero nel cancello sono corte: massimo 1–27 ms per prova (prima, con la coda a 10 ms: 64–67 ms).
- Le richieste lente del leggero sono quasi tutte inserimenti tra 15 e 30 ms: lo scarico del suo registro passa per i task di fondo di SlateDB, che il cancello non governa e che fanno la fila con quelli dei 50 pesanti.
- Il ritardo massimo dello scheduler arriva a 70–145 ms in tutte le prove, anche prima: lavori di fondo (scarico del memtable e compattazione di 50 database che scrivono 6 MB al secondo) che tengono il thread per decine di millisecondi.
- Proposta (cambia il motore, decide il fondatore): far girare compattazione e scarichi di SlateDB su un runtime separato a priorità bassa, o con un tetto proprio. Senza questo, sotto saturazione piena, il p99 delle scritture del leggero resta sopra 20 ms su 1 CPU.

Varianti provate e scartate (60 s ciascuna, MISURATO): tetti 4/2, 6/6, 16/8, 2/1, 4/1 per comandi e scarichi (p99 del leggero 21–61 ms, nessuna sotto 20 in modo stabile); controllo della rete più frequente nel runtime (peggio: il ritardo misurato scende e il cancello non si accende più).

Cosa vale anche per i database grandi: la coda pesata e la tessera non dipendono dalla dimensione del database; un database da 1 TB con decine di migliaia di operazioni al secondo avrà una quota grande e un peso grande nella coda. Il limite trovato (lavori di fondo non governati) pesa di più proprio con i database grandi, che compattano di più.

## 3. Pesanti in processi separati con cgroup v2

Non fatto. La macchina ha cgroup v1 per la CPU; la gerarchia v2 (`/sys/fs/cgroup/unified`) offre solo `hugetlb`. Come chiede il prompt, non l'ho simulato.

## Test

- ctl-test 36 (`ctl-tests/test_36_isolation_quotas.py`): punti per parte, quota che fa aspettare e poi rifiuta in modo ritentabile senza toccare un altro database, `GET`/`PUT`/`DELETE` della quota, tetto JavaScript per comando, pesi sbagliati che fermano l'avvio.
- Test unitari in `fair.rs` (pesi, decadimento, secchio, ordine della coda) e in `js.rs` (tetto per comando su più chiamate).

## Da fare dopo

- Tetto in byte della cache dei metadati di S8 (compito a parte, non toccato).
- Le 2 PUT per inserimento singolo: capire e, se si può, toglierne una.
- Lavori di fondo di SlateDB fuori dal cancello (proposta sopra).
- Le quote per database in memoria: il pannello le deve rimettere dopo un riavvio, o vanno salvate.
- Rifare la prova con S3 vero (latenza di rete) e con più CPU.
