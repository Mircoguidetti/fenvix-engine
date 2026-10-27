# compat: MongoDB 7.0 driver-surface audit

Outputs: `inventory.csv` (the denominator and the engine status of every row), `COVERAGE.md` (the report), `coverage.py` (recomputes the numbers from the CSV alone).

```
python3 compat/coverage.py            # covered/total per category and overall (only exclusion_proposed != yes rows)
python3 compat/coverage.py --detail   # per sub-category
```

Pipeline (all under `compat/tools`, run from the repo root; needs docker for `mongo:7.0`, and for the engine runs the release binary of `mongo-server`):

1. `build_inventory.py` fetches the 7.0 manual (markdown pages), the pymongo and Node API references and reads `listCommands-mongo-7.0.json` (output of `listCommands` on mongo:7.0.45) -> `inventory_base.csv`; every fetch is logged in `fetch_log.csv`.
2. `verify_urls.py`, `verify_counts.py`, `spot_check.py` — quality gate (`verify_report.txt`, `verify_counts.md`, `spot_check.md`).
3. Differential runs (results copied to `results/`): `mongo-tests/run_py.py` and `mongo-tests/node/run_node.js` as documented in `mongo-tests/run_full.sh`, with the engine started with `--store s3` against an S3-compatible endpoint (moto was used) and ttl sweep 1 s, plus a real mongod single-node replica set with `ttlMonitorSleepSecs=1` for the replset and TTL categories.
4. `probe_engine.py` (name recognition on engine and mongod), `probe_diff.py` (+ `probes/*.py`) and `probes/node_probe.js`: extra differential probes (same canonical comparison as the suite) -> `probe_results.json`, `probe_diff_results.json`, `probe_node_results.json`.
5. `map_status.py` fills `engine_status`, `evidence`, `differential_test` -> `inventory.csv` (hand decisions in `overrides.csv`; failing tests are attributed to rows only through `FAIL_ATTR` in `map_status.py`).
6. `verify_evidence.py` checks that every cited test/probe passed or failed as cited.
7. `scan_extra.py` (extra usage scan, needs `git clone` access to the 10 repos) and `report.py` -> `COVERAGE.md`.

Ports used by the probes/harnesses: real mongod 27017 (standalone), 27020 (single-node replica set `rs0`, `ttlMonitorSleepSecs=1`), engine 27018/27019 (`--replica-set rs0`).
