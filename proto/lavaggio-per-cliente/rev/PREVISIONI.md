# Revisore M26 · previsioni scritte prima delle misure (2026-11-03, 16:10)

Simulatore di M26 (`scripts/sim.py --ovh 1.33 --w-mbs 2 --l0-mb 8 --modo livelli --T 4 --regime 1`):
- 8 GB: amplificazione 18,2, pile medie 4,58 (massimo 8). Con la correzione di M26 (+2,6): 7,2 pile.
- Se la correzione non cresce con la taglia, la prova vera a 8 GB deve dare circa 7,2 pile medie (6,9 a 4 GB).
- Se le pile vere crescono di 0,43 pile per raddoppio come da 0,25 a 4 GB (5,2 → 6,9), la prova dà circa 7,3.
- Cambia i conti di M26 se: pile medie vere sopra 7,6 (correzione sopra 3,0) oppure amplificazione vera sopra 18,2.

Prove rifatte (stessi parametri di M26): 1 GB T=4 (M26: amplificazione 11,0, pile 6,34, p90 8, max 9), 1 GB senza tetto (M26: 5,07, pile 8,58, p90 11, max 13).
Prova di granularità: 0,5 GB T=4 con 250 clienti (circa 2 MB per cliente, come 20 TB con 10 milioni di clienti).
Previsione: "scaldare un cliente" costa molto più di una GET per pila (circa 2 MB / 64 KB di read-ahead = circa 32 GET per pila).

## Esito (19:50)
- 8 GB T=4: pile medie 7,20 (previste 7,2), p90 9, max 11; amplificazione del lavaggio 16,0 (simulata 18,2). La correzione +2,6 regge a 8 GB.
- 1 GB T=4: 11,1 e 6,10 pile (M26 11,0 e 6,34). 1 GB senza tetto: 5,07 e 8,29 pile (M26 5,07 e 8,58).
- Clienti da 2 MB (0,5 GB, 250 clienti), 5 pile: con le impostazioni di M26 (read-ahead 64 KB, 4 fetch) find 7,0–7,2 GET invece di 4,4,
  Q1 8,8–9,2, scaldare 42–43 (p90 85–89). Con 1 fetch: find 4,41 (Q1 = Q3 = 5,0 = una per pila). Scaldare: 41,7 a 64 KB, 6,7 a 1 MB,
  5,2 a 4 MB, 5,0 a 16 MB.
