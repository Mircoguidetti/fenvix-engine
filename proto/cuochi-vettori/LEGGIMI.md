# M20 · cuochi vettori

Misura della CPU per ricerca per significato: Lucene 10.4 come mongot (Atlas Search/Vector Search) contro le nostre leve, sulla stessa macchina (runner GitHub Actions, 2 vCPU AMD EPYC 9V45, 7,9 GB). Il motore non è toccato. Rapporto: `fenvix-brain/sessions/2026-11-03-cuochi-vettori/RAPPORTO.md`.

- `SOGLIE.md`: metodo e soglie, scritti prima di misurare (con i cambi dopo le prove da 50.000).
- `dati.py`: Cohere 1M (VectorDBBench) in file grezzi, risposte giuste con la forza bruta, filtro `cat == 3` (10%), sequenze "agenti".
- `lucene/`: banco Java, Lucene 10.4.0 (`Lucene104HnswScalarQuantizedVectorsFormat`, 7 bit o 1 bit, maxConn 16, beamWidth 100), 8 segmenti poi forceMerge(1), filtro come pre-filtro, ricalcolo in float opzionale.
- `nostri.py`: faiss HNSW SQ8 + ricalcolo, usearch int8 (quantizzato da noi) e binario + ricalcolo, usearch in 8 pezzi.
- `riuso.py`: leva 3, cache delle domande degli ultimi 60 s con i candidati.
- `analizza.py`, `soldi.py`: rapporti e conversione in dollari.
- `risultati/corsa-41585160021/`: la corsa vera da 1 milione (le altre due cartelle sono prove).
- Il workflow è `.github/workflows/cuochi-vettori.yml`, parte con un push che cambia `corsa.json`.

Rifare: cambiare `corsa.json` e fare push su questo branch; circa 95 minuti.
