# M10 · Soglie, scritte prima di misurare (2026-11-02, prima della prima corsa)

Dal prompt (prompts/CODA-MATTINO-2026-11-02.md, M10), con il modo in cui le leggo:

- **Vince** se sui blocchi tipici (64 KB e 1 MB) k su n (4+1 o 8+2) taglia il p99 della lettura di almeno 2 volte
  rispetto alla lettura normale **E** batte di almeno 1,3 volte (p99) la richiesta doppia **a parità di GET in più**.
- **Muore** se non batte la richiesta doppia, o se il guadagno c'è solo sopra 1 MB (cioè solo a 16 MB).

Come leggo "a parità di GET in più" (scelta mia, scritta qui prima dei numeri):
- k su n 4+1 fa 5 GET per lettura (+4), 8+2 ne fa 10 (+9): il costo S3 è per richiesta, non per byte.
- Confronto principale: k su n contro la migliore richiesta "doppia" che spende **al massimo le stesse GET**: la
  richiesta doppia dopo una soglia (circa 1,05 GET), la doppia subito (2 GET), e le copie subito (5 GET per 4+1,
  10 per 8+2). Se una qualsiasi di queste ha p99 non peggiore di p99(k su n) × 1,3, k su n non passa la seconda soglia.
- Confronto secondario (riportato, non decide): k su n contro la richiesta doppia dopo il p95 (la mossa che Infinite
  copierebbe domani, circa 1,05 GET).
- La soglia si controlla nello scenario centrale (media 30 ms, p99 100 ms) e nello scenario "uno su cento a 1 s";
  le code leggera e pesante si riportano.

Controllo in più (non nel prompt): `pezzi4`, 4 GET a intervallo senza parità, per separare il guadagno della gara
(prime k di n) da quello del trasferimento in parallelo (che si ottiene anche senza codice).
