# Review of K1 (server-per-script): checks for the M30 short program

- `rev_k1_m30_test.py BINARY`: drop race of the writes collection (5 copies x 16 threads insert and drop), storage
  counters against a recount; profiler level 2 against level 0 with 100 concurrent finds.
- `rev_k1_profile_check.py GIRO_FOLDER...`: from the raw phase files of prova_infinite.py, per phase and query, how many
  warm finds report bytes read from the store and how many entries are marked fenvixS3Shared.
- `m30_short_ours_steps.txt`: the m30s / m30p lines of irlanda/programmi/m30-corto/passi.txt (branch code-irlanda),
  unchanged, run with irlanda/programma.py against mongo-server --store local.

Local run used for the review (2 vCPU, 8 GB, store on local disk): databases loaded with prova_infinite.py carica,
app_data_4 (3378 clients, 1649 dormant, seed 104, stopped by hand at 2.05 GB) and app_data_6 (4950 clients, 1980
dormant, seed 106, stopped by hand at 1.81 GB).
