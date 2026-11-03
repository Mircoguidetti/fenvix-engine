# M25 · agenti e bancone

Misura a $0. Il motore non è toccato. Metodo e soglie in `SOGLIE.md` (scritto prima di misurare).

## File

- `fonti_agenti.py`: dalle traiettorie alle conversazioni (tau2 articolo e classifica 2026, tau-bench, DAB, SWE-bench).
  La proprietà dei dati di tau2 è generica (`Owners`): tabelle utenti/clienti, campi `user_id`/`customer_id`,
  id citati da un solo padrone; il resto è condiviso.
- `bancone.py`: le tre politiche (a primo segnale, b condivisi fissi, c segnale esplicito) e le combinazioni;
  latenze dello scenario medio di M15 riusando `../riscaldamento-utente/sim.py`.
- `corri.py`: tutte le corse → `risultati/tutto.json`. `analisi.py`: riassunto e conti sul profilo B →
  `risultati/riassunto.txt`.
- `ci/`: workflow `.github/workflows/agenti-bancone.yml` che scarica dal runner le traiettorie della classifica
  tau2 (S3 pubblico di Sierra, senza credenziali); estratti in `tracce/tau2-classifica/` (27 file, 46 MB).

## Come si rifà

    git clone --depth 1 https://github.com/sierra-research/tau2-bench /tmp/tau2
    git clone --depth 1 https://github.com/sierra-research/tau-bench /tmp/taub
    git clone --depth 1 https://github.com/john-b-yang/20260901_mini-v2.4.2_gemini-3-5-flash /tmp/swem
    # DAB: tracce di M18 (branch maitre-agenti, proto/maitre-agenti/tracce/dab-grezze), estratte in /tmp/dabraw
    TAUB=/tmp/taub/historical_trajectories DAB=/tmp/dabraw/all_trajectories SWE=/tmp/swem/trajs \
      python3 corri.py risultati/tutto.json      # circa 40 s
    python3 analisi.py > risultati/riassunto.txt
