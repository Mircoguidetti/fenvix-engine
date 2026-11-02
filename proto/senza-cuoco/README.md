# proto/senza-cuoco (prova M8)

Due `mongo-server` servono lo stesso database sopra `proto/quaderno`, senza proprietario con lease.

- Codice nel motore: `mongo-server/src/qmode.rs` (acceso con `--quaderno host:porta,...`), più piccoli agganci in
  `dbs.rs` (letture a S, commit validato), `cmd.rs` (il comando si rifà sul conflitto), `coll.rs` (chiave di guardia
  degli indici unici), `natural.rs` (blocchi dell'ordine naturale presi da più processi), `error.rs`, `main.rs`.
- Prove: `python3 prova.py tutto` (serve `cargo build --release -p mongo-server` e `proto/quaderno` compilato,
  `pip install pymongo`). Risultati in `results/`; `results/prima-correzione-*` sono le corse fallite che hanno
  mostrato i due difetti corretti (ordine naturale, repliche morte nelle letture).
- Rapporto: `fenvix-brain/sessions/2026-11-02-senza-cuoco/RAPPORTO.md`.
