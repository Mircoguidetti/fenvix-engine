# M9 · Misura del maître

Solo misura, nessun codice nel motore. Domanda: quante letture si potrebbero servire con una risposta già pronta?

- `analizza.py`: simulatore in un passaggio (cache dell'app 0/10/60 s, finestra del maître W, tre regole: collezione, intervallo, tetto).
- `catalogo.py`: generatore delle 86 app del catalogo del ristorante (finestra 08:00-12:00).
- `corri_catalogo.py`: corsa del catalogo (`--card`, `--zipf`, `--uniq` per la sensibilità).
- `corri_twitter.py`: analisi dei campioni pubblici di twitter/cache-trace (54 cluster, 1 milione di righe ciascuno).
- `riassunto.py`: riassunto dei JSON in `risultati/`.

Le tracce non sono nel repo (3,8 GB): `git clone --depth 1 https://github.com/twitter/cache-trace`, poi
`python3 corri_twitter.py cache-trace/samples/2020Mar`.
