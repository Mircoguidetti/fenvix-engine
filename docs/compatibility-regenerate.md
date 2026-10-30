# Regenerating the compatibility page

`docs/compatibility.md` and `docs/compatibility.json` are generated. Do not edit them by hand. Every number comes from `compat/inventory.csv`.

## Commands

Run from the repository root.

```bash
python3 compat/coverage.py
```

Validates the CSV and prints the counts. It must exit 0.

```bash
python3 tools/compat-page/build.py
```

Writes `docs/compatibility.md` and `docs/compatibility.json`. It prints a `NOTE:` line for every disagreement it finds between the audit files. Read them: they are also printed at the end of the page.

```bash
python3 -m unittest discover -s tools/compat-page/tests
```

Checks the page counts against `compat/coverage.py`, that every Supported feature has evidence, the JSON schema, and that a second build gives the same bytes. It also fails when the files in `docs/` are older than the data, so run the build first.

```bash
python3 tools/compat-page/build.py --check
```

Exits 1 if `docs/` differs from what the data produces (usable in CI).

The build stops with an error, instead of guessing, when a source file changes format: the header of `compat/COVERAGE.md`, the ranking table of `compat-scan.md`, the table of `compat/exclusions_decided.md`, the bullets of `compat/UNVERIFIED.md`, or an inventory category that has no area (add it in `area_of` in `build.py`).

## Checklist after a new audit run (for example after round3)

The page is only as current as the inventory. Before regenerating, re-run the audit on the engine build you want to describe:

1. Build that engine and note its commit.
2. Run the differential suites against real mongod 7.0.x with **pymongo** and with **Node.js** (`mongo-tests/run_py.py`, `mongo-tests/node/run_node.js`, as in `mongo-tests/run_full.sh`), and the gap suites (`mongo-tests/gaps/`).
3. Re-run the probes: `compat/tools/probe_engine.py`, `compat/tools/probe_diff.py`, `compat/probes/node_probe.js`.
4. Update `compat/inventory.csv` (`compat/tools/map_status.py`, `compat/overrides.csv`, `compat/apply_gaps.py`), then run `compat/tools/verify_evidence.py` and `python3 compat/coverage.py`.
5. Regenerate `compat/COVERAGE.md` (`compat/tools/report.py`). The engine commit and audit date on the page come from its header; if the header is not regenerated, the page says so in its notes.
6. Re-run the usage scan only if you want newer usage numbers (`tools/compat-scan/scan.py`, `compat/tools/scan_extra.py`); otherwise the page keeps the dates of the old scan.
7. Run the three commands above and commit `docs/` together with the audit files.

If any step is skipped, the page must say so. It does so on its own for the two cases it can detect: a `COVERAGE.md` header that no longer matches the CSV (its engine commit is then reported as belonging to the older state).

The only value that comes from git is the date and commit of the last change to `compat/inventory.csv` (`git log -1 -- compat/inventory.csv`). It does not depend on HEAD, on the clock or on which branches a clone has. In a zip or a shallow clone the page says "not available" and the JSON has `null`.
