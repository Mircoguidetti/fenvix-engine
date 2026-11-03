# M20 · Soglie e metodo (scritti PRIMA di misurare)

Data: 2026-11-03. Sessione M20 (Opus). Prompt: `fenvix-brain/prompts/M20-cuochi-vettori.md`.
Questo file non si cambia dopo la prima misura. Se una scelta cambia, la scrivo in fondo, con il motivo, sotto "Cambi dopo la prima misura".

## Dati

- Insieme: VectorDBBench "Cohere 1M" (`assets.zilliz.com/benchmark/cohere_medium_1m/`): 1.000.000 vettori da 768 dimensioni (embedding Cohere di Wikipedia), similarità coseno, 1.000 domande di prova separate.
- Se non si scarica: le prime 1.000.000 righe di `Cohere/wikipedia-22-12-en-embeddings` (Hugging Face, 768 dimensioni) più 1.000 righe successive come domande. Lo dico nel rapporto.
- Vettori normalizzati a lunghezza 1; coseno = prodotto scalare.
- Risposte giuste: le ricalcolo io con la forza bruta in float32 (top 100), anche quando l'insieme le ha già. Controllo che coincidano con quelle dell'insieme, se ci sono.
- Filtro semplice: ogni documento ha un campo intero `cat` da 0 a 9, assegnato in modo pseudocasuale dall'id (non legato al vettore). Filtro `cat == 3`: passa circa il 10% dei documenti. Risposte giuste del filtro: forza bruta sui soli documenti che passano.
- Perché 1 milione e non 10: la macchina di misura ha circa 7 GB di RAM (runner GitHub, o la sandbox), 10 milioni di vettori a 768 dimensioni sono 30 GB solo in float. È il limite del prompt ("1–10 milioni"), al minimo. Lo scrivo come limite nel rapporto: con 10 milioni il grafo è più profondo e Lucene a più segmenti paga di più (STIMATO), quindi 1 milione non favorisce noi.

## Macchina

- Tutte le misure di CPU di un confronto girano nello stesso processo di lavoro (stesso job GitHub Actions, stessa macchina), una dopo l'altra, mai in parallelo.
- Una ricerca alla volta, un solo thread di ricerca. "Ricerche al secondo per vCPU" = numero di domande / secondi di CPU del thread che cerca (Java: ThreadMXBean; Python: `time.thread_time`). Riporto anche il tempo da orologio.
- Riscaldamento: un giro intero delle 1.000 domande prima di misurare (JIT di Java, cache). Poi 2 giri misurati, tengo il migliore.
- Faiss e usearch costretti a 1 thread in ricerca.

## Riferimento: Lucene come Atlas (mongot)

- Versione: Lucene 10.4.0, la stessa di mongot (letto in `mongodb/mongot`, `bazel/java/generated_pom/pom.xml`, commit 7011629). Java 21 con `--add-modules=jdk.incubator.vector` (SIMD di Lucene acceso), come mongot (`.bazelrc`).
- Grafo: maxConn 16, beamWidth 100 (predefiniti di Atlas: `maxEdges` 16, `numEdgeCandidates` 100, DICHIARATO, docs "vector type").
- int8 (Atlas `scalar`): `Lucene99HnswScalarQuantizedVectorsFormat` a 7 bit, compress=false, confidence predefinita: esattamente quello che usa mongot (`MongotHnswScalarQuantizedVectorsFormat`).
- Binario (Atlas `binary`): mongot usa un suo formato a 1 bit con doppio ricalcolo; non lo posso compilare qui. Uso il binario di Lucene (BBQ, `Lucene104HnswScalarQuantizedVectorsFormat` a 1 bit, o `Lucene102HnswBinaryQuantizedVectorsFormat` se il primo non c'è) più il ricalcolo in float di tutti i numCandidates. BBQ è più preciso del semplice "segno" a 1 bit, quindi favorisce Lucene (prudente per noi).
- Domanda: `KnnFloatVectorQuery(campo, q, k = numCandidates)`, poi i primi 10 (come mongot: limit 10, numCandidates predefinito clamp(8×limit, 100, 10000) = 100). Per int8 misuro sia senza ricalcolo (come Atlas) sia con ricalcolo in float dei numCandidates, e per Lucene tengo il migliore dei due.
- numCandidates provati: 10, 15, 20, 30, 40, 60, 80, 100, 150, 200, 300, 400, 600, 800, 1000, 1500, 2000.
- Segmenti: (a) "come in produzione": 8 segmenti da 125.000 documenti, senza unioni. mongot usa TieredMergePolicy con 10 segmenti per livello e segmento massimo min(heap, 5 GB) (letto nel sorgente): il numero vero di segmenti in produzione NON SO, 8 è una mia stima per 1 milione di vettori. (b) un segmento solo (forceMerge(1)).
- Filtro: `IntPoint.newExactQuery("cat", 3)` come filtro della KnnFloatVectorQuery (pre-filtro, come mongot).

## Noi: le leve

- Leva 1, indice in un pezzo: un grafo HNSW unico su tutto il milione. Da sola: Lucene 1 segmento contro Lucene 8 segmenti (stesso motore).
- Leva 2, distanze int8 o binarie con SIMD e ricalcolo solo sui migliori: usearch (SIMD SimSIMD) e faiss (AVX2/AVX-512), stesso grafo (16, 100). Configurazioni: usearch i8 più ricalcolo in float dei primi R; usearch binario (768 bit, Hamming) più ricalcolo in float dei primi R; faiss HNSW SQ8 più ricalcolo (IndexRefineFlat). Da sola: la stessa libreria in 8 pezzi da 125.000 con unione dei risultati.
- Leve 1+2 insieme: la stessa libreria in un pezzo.
- Leva 3, riuso: sopra la migliore configurazione 1+2, una cache delle ultime domande (finestra 60 s) con i loro 100 candidati migliori. Domanda nuova uguale o vicina (coseno almeno τ) a una in cache: ricalcolo esatto solo sui candidati della vicina, senza grafo. τ scelto perché il richiamo resti almeno 0,95.
  - Sequenza "agenti" (modello mio, STIMATO, sul caso "paralleli" di M18): 200 compiti; per ogni compito 5 tentativi in parallelo; ogni tentativo fa 3 domande a pochi secondi; la prima domanda di ogni tentativo è la stessa del compito (ripetizione esatta), le altre sono "parafrasi": la domanda base più rumore casuale, con coseno 0,95 o 0,90 dalla base. Le parafrasi vere non le ho (servirebbe un modello di embedding): il rumore casuale è probabilmente più gentile delle parafrasi vere, lo dico.
  - Le ripetizioni esatte le prenderebbe anche la cache dell'app (M18): riporto il guadagno con e senza di loro. Il numero di leva 3 che conta è quello senza le ripetizioni esatte.

## Il numero che decide

- R95 = (ricerche per vCPU della nostra migliore configurazione a richiamo@10 ≥ 0,95) / (ricerche per vCPU della migliore configurazione Lucene "come in produzione", 8 segmenti, int8 o binario, a richiamo@10 ≥ 0,95). Senza filtro. Per ogni configurazione prendo il punto più veloce che arriva almeno a 0,95 (stesso per 0,90).
- Il verdetto si dà su R95 con le leve 1+2. La leva 3 la riporto a parte (dipende da un modello di traffico che non ho misurato su agenti veri).
- Riporto anche: R95 contro Lucene 1 segmento (se Atlas unisse i segmenti, quanto resta), R90, e i due casi con filtro.

## Soglie (dal coordinatore, nel prompt)

- PASSA: R95 ≥ 2 sulla stessa macchina, e prezzo al cliente sotto il 47% di Atlas sulla parte ricerca.
- MUORE: R95 < 1,3.
- In mezzo: FERITA, con cosa la deciderebbe.

## Conversione in soldi (punto 5)

- Clienti di B10: medio (50 milioni di vettori, 1.024 dimensioni, 50 ricerche/s oggi, 250 tra 2 anni) e grande (500 milioni, 500/s oggi, 2.500 tra 2 anni).
- Atlas: 10 ricerche/s per vCPU (DICHIARATO, blog sizing) sui nodi High CPU e Storage-Optimized, prezzi di B10 (DICHIARATO, us-east-1), almeno 2 nodi.
- Noi: CPU = CPU di Atlas / R95 (STIMATO: ipotesi che la regola di Atlas scali come Lucene). Macchine AWS on-demand di B10 (c7g, m7gd, i4g), almeno 2 (una per zona), RAM e disco per l'indice come B10, prezzo = costo / 0,55 (margine 45%).
- Prima riga: "su $10.000 di ricerca Atlas da noi paga $X", sul caso con più ricerche (dove conta la CPU).

## Cambi dopo la prima misura

Scritti dopo le corse di prova da 50.000 vettori (solo per provare i programmi) e PRIMA della corsa vera da 1 milione.

1. int8 di Lucene: in Lucene 10.4 il formato `Lucene99HnswScalarQuantizedVectorsFormat` sta nei codec vecchi e non sa più scrivere ("Old codecs may only be used for reading", letto nel sorgente di Lucene 10.4.0); mongot lo riscrive con codice suo che non posso compilare. Uso il formato nuovo di Lucene 10.4, `Lucene104HnswScalarQuantizedVectorsFormat` con `SEVEN_BIT` (stessi 7 bit, quantizzazione più nuova e più precisa): favorisce Lucene, quindi è prudente per noi.
2. usearch int8: se gli si danno float, usearch li normalizza e li moltiplica per 127, e con 768 dimensioni resta quasi niente (richiamo fermo a circa 0,79, MISURATO sulla prova). Quantizzo io, con una scala unica per tutti i vettori (127 / quantile 99,99% dei valori). È parte della leva 2, non un trucco: l'ordine del prodotto scalare non cambia.
3. Per stare sotto i 120 minuti: Lucene int8 con filtro solo con ricalcolo (era il migliore sulla prova); i giri si fermano a richiamo 0,975 invece di 0,985 (a noi servono 0,90 e 0,95); meno combinazioni per faiss, per usearch a 8 pezzi e per il binario.
4. Faiss con filtro: solo k_factor 2.
