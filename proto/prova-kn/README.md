# proto/prova-kn (M10)

Letture "k su n" (una scatola salvata come k fette di dati più m fette di parità Reed-Solomon, in un solo oggetto S3;
si chiedono tutte le n fette con GET a intervallo e si usano le prime k) contro la lettura normale e la richiesta
doppia, su un finto S3 con coda lunga. Rapporto: `fenvix-brain/sessions/2026-11-02-prova-kn/RAPPORTO.md`.
Soglie scritte prima di misurare: `SOGLIE.md`.

- `src/modello.rs`: tempo di una GET = TTFB lognormale (+ facoltativo "1 su 100 a 1 s") + byte / banda per richiesta.
- `src/rs.rs`: codifica e ricostruzione (libreria `reed-solomon-simd`), con test di andata e ritorno su tutte le
  scelte di k fette.
- `src/bin/proxy.rs`: proxy HTTP davanti a moto (porta 9000) che aggiunge il ritardo del modello a ogni GET.
- `src/bin/banco.rs`: carica gli oggetti e misura le strategie (`normale`, `pezzi4`, `doppia:<ms>`, `copie:<c>`,
  `kn:<k>+<m>`), con verifica byte per byte di ogni lettura.
- `src/bin/simula.rs`: le stesse strategie con lo stesso modello, 1 milione di letture, senza rete.
- `src/bin/cpu.rs`: CPU di codifica e decodifica su un core.
- `scripts/proxy.sh`, `scripts/corsa.sh`, `scripts/tutto.sh`: come sono state fatte le corse.
- `risultati/`: JSON di ogni corsa (`base.jsonl`, `uno-su-cento.jsonl`, `doppia-p50.jsonl`, `simula.jsonl`,
  `cpu.jsonl`, `zero-conc1.jsonl`).

Per rifare: `moto_server -H 127.0.0.1 -p 9000`, creare il bucket `prova-kn`, `cargo build --release`,
`scripts/proxy.sh zero`, `./target/release/banco carica --taglie 4096,65536,1048576,16777216`, `scripts/tutto.sh`.
