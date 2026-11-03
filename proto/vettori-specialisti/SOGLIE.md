# M23 · Soglie e metodo (scritti PRIMA di misurare)

Data: 2026-11-03. Sessione M23 (Opus). Prompt: `fenvix-brain/prompts/M23-vettori-specialisti.md`.
Questo file non si cambia dopo la prima misura. I cambi li scrivo in fondo, con il motivo, sotto "Cambi dopo la prima misura".

## Domanda

M20 ha misurato la nostra ricerca per significato contro Lucene (Atlas). Qui: contro i motori specializzati (Qdrant, FAISS, usearch, hnswlib, LanceDB, pgvector), usiamo meno CPU a parità di richiamo?

Nota scritta prima di misurare: "la nostra ricerca" di M20 è costruita sopra faiss (HNSW SQ8 con ricalcolo) e usearch. Quindi contro FAISS HNSW e usearch il vantaggio atteso è vicino a 1 per costruzione: le nostre leve 1 e 2 (indice in un pezzo, int8 con SIMD e ricalcolo) sono quello che i motori specializzati fanno già di serie. Lo misuro lo stesso, perché la configurazione "consigliata" di ognuno è diversa dalla nostra.

## Dati (come M20, stesso codice)

- VectorDBBench "Cohere 1M": 1.000.000 vettori da 768 dimensioni, coseno, 1.000 domande separate. Stesso download e stessa preparazione di M20 (`proto/cuochi-vettori/dati.py`, importato, non copiato).
- Vettori normalizzati; coseno = prodotto scalare. Risposte giuste con la forza bruta in float32.
- Filtro semplice: campo `cat` da 0 a 9 dall'id, filtro `cat == 3` (circa 10%). Stessa funzione di M20.

## Macchina e misura

- Runner GitHub Actions `ubuntu-24.04` (2 vCPU, circa 7,9 GB), come M20. La rete della sessione blocca i dati di VectorDBBench: le misure girano in un workflow sul branch `vettori-specialisti`. Niente segreti, ogni job sotto 120 minuti.
- Più job in parallelo, perché in un job solo non entrano tutti gli avversari. Ogni job gira anche la nostra configurazione di riferimento (faiss HNSW SQ8 + ricalcolo, la migliore di M20), sulla stessa macchina dell'avversario. **Ogni rapporto si calcola dentro lo stesso job**: avversario e noi sulla stessa macchina. I numeri assoluti tra job diversi non si confrontano direttamente (CPU diverse possibili); il nostro riferimento ripetuto in ogni job dà anche la variabilità tra macchine.
- Una domanda alla volta. "Ricerche al secondo per vCPU" = domande / secondi di CPU spesi per cercare.
  - Librerie dentro il processo (faiss, usearch, hnswlib, LanceDB): secondi di CPU di tutto il processo (`time.process_time`, somma di tutti i thread), con le librerie limitate a 1 thread dove si può. Per noi, come in M20, anche `thread_time`.
  - Server (Qdrant, PostgreSQL con pgvector): secondi di CPU del processo server (`/proc/<pid>/stat`, utime + stime, tutti i thread) prima e dopo il giro di domande. Il client Python non si conta.
  - I server pagano anche il protocollo (leggere la domanda, rispondere). Misuro questo costo con una domanda vuota (Qdrant: stessa domanda con un filtro che non trova niente su campo indicizzato; PostgreSQL: `SELECT $1::vector IS NULL`) e riporto sia il numero lordo sia quello senza protocollo. **Il verdetto usa quello senza protocollo** (il più favorevole all'avversario: anche noi avremo un protocollo).
- Riscaldamento: un giro delle 1.000 domande prima di misurare. I punti decisivi (il più veloce sopra 0,90 e sopra 0,95) rimisurati 2 volte, si tiene il migliore (come M20).

## Avversari, configurazione consigliata per produzione, un pezzo solo

- **Qdrant** (ultima versione stabile, binario ufficiale): HNSW m 16, ef_construct 100 (predefiniti), un segmento solo (`default_segment_number` 1, segmento massimo grande), vettori pieni su disco e riassunto in RAM (`on_disk` true, `always_ram` true: la configurazione che Qdrant consiglia per risparmiare memoria). Ricerca a 1 thread (`max_search_threads` 1).
  - Scalare: int8, quantile 0,99, ricalcolo acceso, sovracampionamento provato (1, 2, 3) e hnsw_ef provato.
  - Binaria: 1 bit, ricalcolo acceso, sovracampionamento provato (1–8). Se il tempo basta anche la binaria a 2 bit (consigliata da Qdrant sotto 1.024 dimensioni).
  - Filtro: indice sul campo `cat`, filtro `cat == 3`.
- **FAISS**: (a) `HNSW32,Flat` (float32, M 32, efConstruction 100), efSearch provato; (b) IVF-PQ: `IVF4096,PQ192x4fs,RFlat` (IVF 4.096 liste, PQ a 4 bit "fast scan", ricalcolo in float), nprobe e k_factor provati. Filtro con `IDSelectorBitmap`.
- **usearch** di serie: f16, connectivity 16, expansion_add 128 (predefiniti), senza ricalcolo. Senza filtro (non c'è un filtro nativo da Python).
- **hnswlib**: M 16, ef_construction 200 (l'esempio del README), spazio `ip`, float32. Senza filtro (il filtro da Python chiama una funzione Python per ogni candidato: misurerebbe Python, non hnswlib).
- **LanceDB** (formato Lance, quello su object storage): IVF_PQ con i parametri predefiniti di `create_index`, coseno, nprobes e refine_factor provati. File su disco locale, non su S3: la CPU per domanda è quella; la rete di S3 non è misurata. Filtro come pre-filtro con indice scalare su `cat`. È anche la nostra approssimazione di **turbopuffer**, che è chiuso (anche lui IVF su object storage, DICHIARATO nei suoi documenti): lo scrivo nel rapporto.
- **pgvector** (0.8.1 compilato dal sorgente, PostgreSQL 16): HNSW m 16, ef_construction 64 (predefiniti), `vector_cosine_ops`, ef_search provato; filtro con `hnsw.iterative_scan = relaxed_order`. Come riferimento, non come specialista.

## Noi

- La migliore di M20: faiss HNSW SQ8 (M 16, efConstruction 100) + ricalcolo in float (`IndexRefineFlat`), un pezzo, con e senza filtro. Stesso codice di M20 (`proto/cuochi-vettori/nostri.py`, importato).
- Nel job delle librerie anche usearch int8 + ricalcolo e usearch binario + ricalcolo, come M20.
- Riuso tra domande vicine (leva 3): rifatto con lo stesso codice di M20 se il tempo basta, riportato a parte e non contato nel verdetto (come M20: dipende da un modello di traffico; ed è uno strato sopra il motore che anche uno specialista può aggiungere).

## Il numero che decide

- S95 = (ricerche per vCPU della nostra migliore a richiamo@10 ≥ 0,95) / (ricerche per vCPU del migliore specializzato a richiamo@10 ≥ 0,95), senza filtro, ognuno contro il nostro riferimento nello stesso job. "Migliore specializzato" = il più alto tra Qdrant, FAISS, usearch, hnswlib, LanceDB (pgvector solo riferimento).
- Per confrontare avversari di job diversi uso il rapporto dentro il job (avversario / nostro riferimento nello stesso job).
- Riporto anche S90, i casi con filtro, e il confronto per famiglia (int8 contro int8, binario contro binario).

## Soglie (dal coordinatore, nel prompt)

- PRODOTTO A SÉ: S95 ≥ 2 contro il migliore specializzato, e prezzo sotto il 47% del servizio gestito migliore su un costo vero.
- FUNZIONE: S95 < 1,3.
- In mezzo: INCERTA, con cosa la deciderebbe.

## Conversione in soldi

- Prezzi DICHIARATI (pagine prezzi, lette oggi) di Qdrant Cloud, Pinecone serverless e dedicato, turbopuffer, Weaviate Cloud, S3 Vectors; clienti medio e grande come B10/B18 (50 milioni di vettori a 50/250 ricerche/s; 500 milioni a 500/2.500 ricerche/s; 1.024 dimensioni).
- Il nostro costo: le stesse regole di `soldi.py` di M20 (macchine AWS on-demand DICHIARATE, almeno 2, RAM per riassunto e grafo, NVMe per i vettori pieni, S3), con la CPU divisa per il nostro vantaggio misurato rispetto a Lucene e poi convertita contro lo specializzato. Prezzo = costo / 0,55.
- Criterio della luce: distinguo il costo vero del servizio gestito (macchine che paga anche lui, STIMATO) dal suo margine. Se il nostro prezzo è sotto il loro solo perché loro hanno margine alto, non è una luce: un concorrente lo pareggia con uno sconto.

## Cambi dopo la prima misura

(vuoto)
