**Intermedio: la prova da 8 GB con clienti grandi passa le due soglie tecniche. Find peggiore 0,92 GET per pila con il lettore regolato (soglia 1,2), pile medie 7,2 a 8 GB (soglia 7,8) (MISURATO). Prezzo a 20 TB: in arrivo con la serie della compressione.**

# M29 · intermedio 1

Sessione M29 (Opus), lanciata da scheduled task il 2026-11-04. In locale, $0. Branch `clienti-grandi` di `fenvix-engine`, cartella `proto/lavaggio-per-cliente/m29/`. Soglie scritte prima in `m29/SOGLIE.md` (primo commit del branch).

Prova da 8 GB, 4.096 clienti da circa 2 MB (come 20 TB con 10 milioni di clienti), pile a gradini T = 4, blocchi da 64 KB anche nelle scatole del lavaggio, documenti realistici, scrittore a 2 MB/s, regime di 1 volta la taglia (2 ore e 23 minuti). Tutto MISURATO:
- Pile medie 7,20, p90 9, massimo 11. Amplificazione con lo scarico 16,4.
- Fotografia a fine scrittura, lavaggio fermato subito: 7 pile (3 L0).
- Lettore di M26 (64 KB, 4 letture in anticipo): Q1 9,6 GET (1,37 per pila), scaldare 22,3 GET (3,2 per pila).
- Lettore regolato (una lettura in anticipo da 64 KB nelle find; per scaldare 4 MB, 16 MB o tutto il cliente): Q1 6,46 GET (0,92 per pila), Q3 6,47 (0,92), find tipica 5,6, scaldare 6,45–6,6 (0,92–0,94 per pila).
- Pile recenti sul bancone (L0 e sorted run sotto l'1% dei dati, 76 MB su 8 GB): restano su S3 2 pile; Q1 2,0 GET, Q3 2,0, scaldare 2,0.

Trovato: il compattatore separato di SlateDB 0.17 (`CompactorBuilder::build`) scrive sempre blocchi da 4 KB senza compressione, qualunque sia l'impostazione del database. M26 e il revisore hanno quindi misurato il lavaggio con blocchi da 4 KB. Qui il lavaggio gira dentro il processo del database (`with_compactor_builder`), che usa il formato del database.

In corso: serie della compressione a 1 GB (nessuna, lz4, zstd livello 1; blocchi 64 e 4 KB), poi 4 GB. Poi il prezzo a 20 TB.
