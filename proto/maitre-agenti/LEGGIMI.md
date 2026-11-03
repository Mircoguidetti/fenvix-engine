# M18 · il maître con le tracce degli agenti

Misura a $0, nessun codice del motore. Stesso metodo di M9 (`misura/maitre/analizza.py`, branch `misura-maitre`):
cache dell'app di 60 s davanti a tutto (senza buttare le risposte quando arrivano scritture), poi il maître con
finestra W (10 min; anche 5 min e 1 ora) e tre regole: collezione, intervallo, tetto (correzione dal quaderno).
In più: ripetizioni "a meno di parametri" e "sotto-query condivise", e la memoria del maître.

## Fonti

- **tau2-bench** (Sierra Research), `data/tau2/results/final`, commit 19f13ea3 del repo pubblico
  `sierra-research/tau2-bench`: 12 file (4 modelli x airline, retail, telecom, 4 tentativi per compito),
  con l'ora vera di ogni chiamata. Ogni strumento dell'agente è tradotto in una query MongoDB (`tau2_eventi.py`).
  Le tracce non sono copiate qui (577 MB): si scaricano con `git clone`.
- **DAB, Data Agent Benchmark** (UC Berkeley + Hasura, arXiv 2603.20576): 13.482 conversazioni di 5 modelli,
  54 domande, 50 tentativi ciascuna, su SQLite, DuckDB, PostgreSQL e MongoDB. Scaricate da Google Drive dal
  workflow `.github/workflows/maitre-agenti-scarica.yml` e salvate in `tracce/dab-grezze/` (tar.xz in due pezzi,
  sha256 in `sha256.txt`). Senza ora: i tempi sono un modello dichiarato in `dab_eventi.py`.

## Come si rifà

    cat tracce/dab-grezze/grezze.tar.xz.part* > /tmp/g.tar.xz && mkdir -p /tmp/dab && tar xJf /tmp/g.tar.xz -C /tmp/dab
    pip install sqlglot
    python corri_dab.py /tmp/dab/all_trajectories risultati/dab.json            # circa 15 min su 2 core
    python statico_berkeley.py /tmp/dab/all_trajectories risultati/dab-statico.txt
    git clone --depth 1 https://github.com/sierra-research/tau2-bench /tmp/tau2
    python corri_tau2.py /tmp/tau2/data/tau2/results/final risultati/tau2.json   # pochi secondi

## File

- `maitre.py`: il simulatore (cache dell'app, maître, tre regole, parametri, sotto-query, memoria).
- `tau2_eventi.py`, `dab_eventi.py`: dalle traiettorie agli eventi; gli ambiti sono spiegati in testa ai file.
- `corri_tau2.py`, `corri_dab.py`, `statico_berkeley.py`: le corse.
- `risultati/`: `tau2.json/.txt`, `dab.json/.txt`, `dab-statico.txt`.
- `estrai_dab.py`: primo estrattore (formato tool_calls.jsonl), non usato: le tracce pubblicate sono run_N.json.

Nota: `tracce/dab-grezze/` pesa 82 MB. Se la PR si unisce, conviene toglierla prima e tenere solo il link.
