# Minimal kitchen (K2, corrected by K2b, 2026-11-05)

Our server (`mongo-server`, data only on S3) on EC2 in eu-west-1, measured ONE TO ONE with the Atlas M30 round from
Ireland: the same program (`programs/m30-corto/passi.txt`, code-irlanda), run by the same `programma.py`, with the
founder's script `prova_infinite.py` p1-1.6.2 unchanged (md5 8396ca455557baaa4e3a9b1f881451f9). Founder's steps and
report: fenvix-brain, `sessions/2026-11-05-k2b-correzioni/` (and `sessions/2026-11-05-k2-cucina-minima/` before it).

How one to one is kept:
- same data: `load-plans/m30-corto.txt` loads the five databases m30-corto reads with the Atlas options; `load.py`
  checks them against the Atlas state files and `load_until.py` stops each load at the fraction Atlas reached, so the
  fake year ends on the same day (the empty windows fall in the same places);
- same clients and windows: `merge_states.py` takes everything that chooses clients from the Atlas state files the
  Atlas round started from (code-irlanda, `atlas_states`, default 501c8fd, CATENA checked) and only our order `_id`s
  from our load; `compare_plans.py` runs the whole program dry on both and must say `PLANS: SAME`;
- same client machine and copies: the program's `macchina.txt` (c7i.2xlarge, 5 copies);
- cold cooks: the load runs on its own cook 0 (switched off after the load and after `settle.py`); every size runs on
  a new machine with a new process, from the same state files.

Files:
- `.github/workflows/minimal-kitchen.yml`: the workflow (trigger: commit of `kitchen/command.json`; modes
  `permissions`, `run`, `teardown`, `drop-pantry`; the fields are explained at the top of the file).
  Examples: `{"mode": "run", "sizes": ["m7i.large"]}` (load and one size, pantry deleted at the end);
  `{"mode": "run", "sizes": [], "keep_pantry": true}` then `{"mode": "run", "sizes": ["m7i.large"], "pantry":
  "fenvix-kitchen-pantry-<run>"}` (load once, then one run per size); `{"mode": "drop-pantry", "pantry": "..."}`.
- `irlanda/`, `programs/`: verbatim copies from code-irlanda 501c8fd, checked by `SOURCES.md5` (never edit them).
- Client machine: `client.sh`, `load.py`, `load_until.py`, `settle.py`, `merge_states.py`, `compare_plans.py`,
  `s3io.py`, `wait_db.py`.
- Server machine: `server.sh`, `create_user.py`, `s3_counters.py` (S3 requests for the SPESA line), `sst_count.py`
  (SSTs in the pantry at the start and the end of every cook), `server-options.txt` (empty: the engine as it is).
- `ci/`: runner side. `run.sh` (the run), `permissions.sh`, `atlas_states.sh`, `packages.sh`, `machine_types.py`
  (allowed types, vCPU, DECLARED prices), `certs.sh`, `user_data.sh`, `fetch_results.sh`, `cleanup.sh`,
  `final_check.sh`, `check_secrets.py`.
- `local-test/`: the $0 test with fake AWS (moto), a fake Atlas and our real mongo-server on a fake S3
  (`local-test.sh WORK MONGO_SERVER`, then `check_runs.py`).

The words `SPESA`, `BLOCCO`, `RIMASTO: niente`, `ALLARME`, `PERMESSI`, `FINITO` stay in Italian on purpose: they are the
words RULES.md and the founder's checks look for.
