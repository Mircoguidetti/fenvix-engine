# M24 · riscaldamento per utente

Misura a $0. Il motore non è toccato. Metodo e soglie in `SOGLIE.md` (scritto prima di misurare).

## File

- `sim.py`: freddezza (a tempo o bancone LRU), "preceduta" (lettore / stretta / larga), riscaldamento per gruppo,
  anticipo ingenuo per successore (M15), latenze dello scenario medio di M15.
- `fonti.py`: tau2-bench, Twitter, GH Archive, REES46 → eventi.
- `corri.py <fonte> <uscita>`: le corse. `analisi.py`: riassunto e conti sul profilo B (`risultati/riassunto.txt`).
- `soglia_segnali.py`: variante "si riscalda dopo k richieste in 10 minuti" (REES46).
- `ci/`: workflow `.github/workflows/riscaldamento-utente.yml` (GH Archive e REES46 scaricati dal runner;
  estratti, con campione per utente, in `tracce/`).

## Come si rifà

    git clone --depth 1 https://github.com/sierra-research/tau2-bench /tmp/tau2
    git clone --depth 1 https://github.com/twitter/cache-trace /tmp/twtrace
    for f in tau2 twitter gh gh_bot ecom; do python3 corri.py $f risultati/$f.json; done
    python3 soglia_segnali.py; python3 analisi.py > risultati/riassunto.txt

Byte scaricati dal runner: GH Archive 0,75 GB (72 ore, 6–8 ottobre 2026), REES46 0,70 GB (primi 12,7 giorni di
ottobre 2019, troncato). Tracce estratte: `tracce/gh/gh-eventi.tsv.gz` (5% degli utenti), `tracce/ecom/ecom-eventi.tsv.gz`
(10% degli utenti).
