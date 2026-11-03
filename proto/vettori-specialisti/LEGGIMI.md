# M23 · vettori contro gli specialisti

La nostra ricerca per significato di M20 (faiss HNSW SQ8 + ricalcolo, un pezzo) contro Qdrant (int8, binaria 1 e 2 bit), FAISS (HNSW32, IVF-PQ), hnswlib, usearch, LanceDB (IVF_PQ) e pgvector (HNSW), su Cohere 1M (768 dimensioni, coseno, 1.000 domande), runner GitHub Actions. Il motore non è toccato. Rapporto: `fenvix-brain/sessions/2026-11-03-vettori-specialisti/RAPPORTO.md`.

- `SOGLIE.md`: metodo e soglie, scritti prima di misurare (con i cambi dopo le prove da 50.000).
- `specialisti.py`: il banco. Riusa `../cuochi-vettori/dati.py` e `nostri.py` di M20. CPU del processo per le librerie, CPU del processo server (schedstat) per Qdrant e PostgreSQL, con il costo del protocollo misurato a parte.
- `analizza.py`: la migliore per configurazione a richiamo 0,90 e 0,95, e i rapporti noi/avversario dentro lo stesso job.
- `soldi.py`: prezzi DICHIARATI dei servizi gestiti contro il nostro costo (regole di M20).
- `risultati/corsa-41601736540-<job>/`: la corsa vera da 1 milione, un job per avversario (ognuno con il nostro riferimento sulla stessa macchina). Le altre due corse sono prove da 50.000.
- Il workflow è `.github/workflows/vettori-specialisti.yml`, parte con un push che cambia `corsa.json` (`jobs` sceglie i job).
