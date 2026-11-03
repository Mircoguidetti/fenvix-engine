# Revisore M26 · previsioni scritte prima delle misure (2026-11-03, 16:10)

Simulatore di M26 (`scripts/sim.py --ovh 1.33 --w-mbs 2 --l0-mb 8 --modo livelli --T 4 --regime 1`):
- 8 GB: amplificazione 18,2, pile medie 4,58 (massimo 8). Con la correzione di M26 (+2,6): 7,2 pile.
- Se la correzione non cresce con la taglia, la prova vera a 8 GB deve dare circa 7,2 pile medie (6,9 a 4 GB).
- Se le pile vere crescono di 0,43 pile per raddoppio come da 0,25 a 4 GB (5,2 → 6,9), la prova dà circa 7,3.
- Cambia i conti di M26 se: pile medie vere sopra 7,6 (correzione sopra 3,0) oppure amplificazione vera sopra 18,2.

Prove rifatte (stessi parametri di M26): 1 GB T=4 (M26: amplificazione 11,0, pile 6,34, p90 8, max 9), 1 GB senza tetto (M26: 5,07, pile 8,58, p90 11, max 13).
Prova di granularità: 0,5 GB T=4 con 250 clienti (circa 2 MB per cliente, come 20 TB con 10 milioni di clienti).
Previsione: "scaldare un cliente" costa molto più di una GET per pila (circa 2 MB / 64 KB di read-ahead = circa 32 GET per pila).
