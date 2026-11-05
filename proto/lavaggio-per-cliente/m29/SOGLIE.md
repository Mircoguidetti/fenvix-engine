# M29 · Soglie e metodo (scritte prima di misurare, 2026-11-04)

Prova più economica proposta dal revisore di M26. Tutto in locale, $0. Il motore non si tocca. Codice nuovo solo in questa cartella (copia del prototipo di M26 e del revisore).

## Domanda

Con clienti grandi (circa 2 MB l'uno, come 20 TB con 10 milioni di clienti), pile a gradini T = 4 e blocchi da 64 KB, la regola "una GET per pila" regge con il lettore regolato? Quante pile restano a 8 GB? Quanto tolgono le pile recenti fissate sul bancone e la compressione? Dove va il prezzo a 20 TB?

## Soglie (del revisore di M26, confermate dal coordinatore)

- REGGE: find peggiore al massimo 1,2 GET per pila con il lettore regolato, pile medie al massimo 7,8 a 8 GB, e prezzo proiettato a 20 TB sotto il 47% di Infinite (dico se arriva al 30–35%).
- FERITA: sopra una delle due soglie tecniche, con il rimedio.
- MUORE: find peggiore sopra 15 GET a 20 TB proiettati anche con il bancone, o prezzo sopra il 47% senza rimedio.

Come le leggo:
- "GET per pila" della find peggiore = GET medie di Q1 (ultimi 20 messaggi) e di Q3 (intervallo di date sugli eventi) divise per le pile della fotografia (L0 più sorted run). Con 2 MB per cliente e attività zipf ogni cliente scritto è in tutte le pile: è il caso peggiore. Prendo la peggiore tra Q1 e Q3 e la confronto con 1,2. Riporto anche la find tipica (60% Q1, 20% Q2, 20% Q3) e il p90. Q2 (per `_id`) costa 2 GET fisse (biglietto e documento) e non è "per pila".
- "Pile medie a 8 GB" = media delle pile campionate ogni 0,5 s durante il regime della prova da 8 GB, come M26 e il revisore.
- "Lettore regolato": nelle find una lettura in anticipo per pila (`max_fetch_tasks` = 1) con read-ahead di un blocco o due (64 o 128 KB); per scaldare un cliente letture grandi (4 e 16 MB) e la lettura di tutto il cliente in un colpo (read-ahead più grande del cliente: SlateDB taglia la lettura alla fine dell'intervallo usando l'indice dei blocchi, che sta sul bancone). Per confronto il lettore di M26 (64 KB, 4 letture in anticipo).

## Metodo

1. Prototipo di M26 copiato in `m29/`, stesse ricette (carico misto chat, eventi, ordini; zipf 0,7; riempimento poi regime con ritenzione; scrittore a 2 MB/s; L0 da 8 MB; WAL spento; disposizione "cliente"; filtri per prefisso sul cliente).
2. Cambi della prova:
   - **Clienti grandi**: numero di clienti = taglia / 2 MB (2.048 a 4 GB, 4.096 a 8 GB).
   - **Blocchi da 64 KB** anche nelle scatole riscritte dal lavaggio. Il compattatore separato di SlateDB 0.17 (`CompactorBuilder::build`) usa sempre il formato di serie (blocchi 4 KB, niente compressione): quindi il lavaggio gira dentro il processo del database con lo schedulatore a gradini (`DbBuilder::with_compactor_builder`), che usa il formato del database. Lo dico nel rapporto: vuol dire che M26 e il revisore hanno misurato scatole del lavaggio con blocchi da 4 KB.
   - **Documenti realistici** che si comprimono: testo di chat da un vocabolario con frequenze zipf, eventi con campi ripetuti e numeri, ordini con righe e indirizzi, più una parte di identificativi casuali. Stessa intestazione di 15 byte per la verifica delle risposte.
   - **Validate** nello schedulatore a gradini, copiato da `size_tiered_compaction.rs` di SlateDB 0.17 (righe 297–360), con test.
3. Prove principali (blocchi 64 KB, senza compressione, per confrontarle con il revisore): 4 GB e 8 GB, T = 4, regime di 1 volta la taglia (come la 8 GB del revisore). La fotografia per le GET si prende subito alla fine delle scritture, fermando il lavaggio senza aspettarlo, così le pile sono quelle del regime (caso peggiore) e non le 2–4 che restano dopo un lavaggio finale.
4. Serie della compressione a 1 GB, regime 2 volte, stessi dati (stesso seme): 64 KB senza compressione, lz4, zstd livello 1 (livello di zstd reso regolabile con una riga in una copia locale di SlateDB, `scripts_vendor.sh`; SlateDB 0.17 usa sempre il livello 3), e 4 KB senza compressione e con lz4. Misuro: rapporto di compressione (byte scritti nelle scatole senza compressione diviso con compressione), CPU del processo nel regime, byte e PUT del lavaggio, GET delle find. In più un banco della sola compressione sugli stessi documenti (MB/s per core in compressione e decompressione, blocchi da 4 e 64 KB).
5. GET con e senza bancone delle pile recenti: il finto S3 conta le GET per scatola; ogni scatola appartiene a una pila (L0 o sorted run) con la sua taglia. "Con il bancone" = le GET sulle L0 e sulle sorted run sotto la soglia non vanno su S3. Soglia proporzionale: 1% dei dati (200 GB su 20 TB del revisore), più 2% e 5% per vedere la curva. Riporto i byte che il bancone deve tenere.
6. Proiezione a 20 TB con il simulatore di M26 (`scripts/sim.py`, tarato) e lo script delle pile sul bancone del revisore, usando le GET per pila misurate qui. Prezzo con le voci di M26 e del revisore (`scripts/prezzo_finale.py`), con i numeri nuovi: GET per find, bancone fissato, compressione su PUT e GET del lavaggio e la sua CPU.

## Cosa NON misura

- Niente S3 vero né latenze vere; niente Graviton (macchina da 2 vCPU Intel).
- Taglie di GB, non di TB: 20 TB è una proiezione (STIMATO).
- Non tocca il motore e non prova crash o rami.
