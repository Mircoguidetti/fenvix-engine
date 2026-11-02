# bench/perf: letture del motore contro mongod 8.0, 1 core

Risultati e conclusioni: `docs/perf-letture.md`. Script copiati da `bench/mongod` (branch bench-mongod) e adattati.

- `install.sh`: immagine Docker `mongo:8.0`, build release del motore, generatore di carico in `.bin/`.
- `loadgen/`: il generatore di bench-mongod (Go, driver ufficiale v2), con in più `-from` e `-no-drop` per caricare a blocchi.
- `bench.py snapshot LAYOUT`: carica il motore una volta (layout 1: 1 database x 200.000 documenti, a blocchi da 10.000
  con 12 s di pausa, perché il caricamento in un colpo solo si blocca; layout 100: 100 database x 2.000) e salva lo store
  come snapshot. `bench.py run SERVER LAYOUT WORKLOAD CLIENTS`: run su un motore nuovo partito da una copia dello snapshot,
  oppure su mongod; `bench.py sweep ...`: sweep dei client. Risultati in `$BENCH_RESULTS/raw.jsonl`.
- `ab.py BIN_A BIN_B`: confronto alternato di due binari del motore (A, B, A, B ...), mediane e guadagno.
- `final.sh`: la misura prima/dopo/mongod del documento (3 run da 60 s + 60 s per cella e server).
- `profile.sh WORKLOAD LAYOUT CLIENTS [SECONDI] [ETICHETTA]`: perf con stack DWARF, nomi decodificati con rustfilt,
  divisione per strato con `layers.py`.
- `run_ctl.sh OUTDIR`: tutta la suite `ctl-tests`, un file alla volta, con MinIO locale.
- `results/`: dati grezzi (`raw.jsonl`), log della misura finale, profili per strato, uscite della suite ctl-tests.
