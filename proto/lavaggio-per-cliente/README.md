# M26 · Quanto costa il lavaggio che ordina i dati per cliente

Prototipo in locale ($0). Non tocca il motore. Rapporto: `fenvix-brain/sessions/2026-11-03-lavaggio-per-cliente/RAPPORTO.md`. Soglie e metodo scritti prima: `SOGLIE.md`.

## Cosa c'è

- `src/main.rs`: `carica` (carico misto chat/eventi/ordini per clienti zipf, riempimento poi regime con ritenzione; conta byte e richieste del lavaggio, CPU, pile) e `misura` (find fredde Q1/Q2/Q3 e "scaldare un cliente" con il bancone S1 di M21).
- `src/livelli.rs`: lo schedulatore "pile a gradini" (livelli geometrici, parametro T), inserito in SlateDB 0.17 come compattatore esterno (`CompactorBuilder::with_scheduler_supplier`).
- `src/conta.rs`: finto S3 che conta (di M21), più le parti multiparte come PUT e le richieste sulle scatole a parte.
- `src/bancone.rs`: bancone di M21.
- `scripts/sim.py`: simulatore del lavaggio sui metadati (size-tiered di SlateDB 0.17, tetto, tetto solo sulle grandi, pile a gradini).
- `scripts/proietta.py`: GET per find a 20 TB dalle pile simulate (cliente presente in tutte, cliente qualsiasi, dormiente).
- `scripts/costi.py`, `scripts/prezzo_finale.py`: costo del lavaggio e prezzo contro Infinite.
- `scripts/riassunto.py`: riassunto delle misure vere in `risultati/riassunto.json`.
- `scripts/matrice*.sh`: le prove fatte.

## Come si rifà

```
cargo build --release
# una prova: 1 GB, cliente in testa, pile a gradini T=4, scrittore a 2 MB/s
./target/release/lavaggio-per-cliente carica --disposizione cliente --livelli 4 --ritmo-mbs 2 --taglia-gb 1 --regime 2 --dir data/x --etichetta x
./target/release/lavaggio-per-cliente misura --disposizione cliente --dir data/x --etichetta x --n 200
# opzioni: --disposizione oggi|collezione|cliente, --consolida K (tetto di SlateDB), --segmenti S, --lavaggio 0 (base senza lavaggio)
python3 scripts/riassunto.py
python3 scripts/sim.py --ovh 1.33 --taglia-gb 20480 --w-mbs 7.6 --l0-mb 256 --modo livelli --T 4
scripts/sim_20tb.sh && python3 scripts/prezzo_finale.py
```

Disco: una prova da 2 GB tiene circa 4–6 GB sul disco mentre gira; la cartella si cancella alla fine.
