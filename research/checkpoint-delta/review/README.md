# Review of the checkpoint delta measurement (R-review-checkpoint-delta)

Reviewer code and results. Report: fenvix-brain `sessions/2026-11-09-review-checkpoint-delta/REPORT.md`.
Uses the owner's code in `..` (workload, savers, options) unchanged; same Postgres 16 / mongod 7.0 setup and
venv (`../.venv`). Run from this folder with `../.venv/bin/python`.

- `results/rerun/`: the owner's full pipeline rerun from scratch (`run_all.sh`, `options.py`, `log_codec.py`,
  `saver_reads.py`, `analyze.py`).
- `convert_real.py OUT`: public agent trajectories -> the owner's thread-script format. Sources ($0):
  tau-bench `historical_trajectories` (git clone of sierra-research/tau-bench into `~/data/tau-bench`) and
  SWE-agent trajectories from the public bucket `s3://swe-bench-submissions` (`lite/20250526_sweagent_claude-4-sonnet-20250514/trajs/`,
  `lite/20240620_sweagent_claude3.5sonnet/trajs/`, 80 files each, sampled with `shuf --random-source=<(yes 20261109)`,
  into `~/data/swe/<run>/`). Hugging Face is blocked from the sandbox.
- `make_twins.py DS`: scripted threads (owner's sizes) with exactly the real threads' checkpoint counts.
- `real_run.py DS [--sample N] --no-sqlite`: savers (default and DeltaChannel) and our options on one dataset,
  keys/envelope measured, every checkpoint of 3 threads rebuilt byte for byte -> `results/real/DS.json`.
- `edge_cases.py N THREADS`: summarising, editing and removing agents -> `results/edge/N.json`.
- `envelope_scripted.py`: keys/envelope bytes on the owner's scripted runs (needs the `ck_default_*` databases).
- `blocksize.py`: the log with 64 KiB blocks (today's run block size).
- `byteranges.py DB THREADS`: every message's msgpack bytes occur verbatim in the stored Mongo documents.
- `delta_reducer_parity.py`: two cases where DeltaChannel's reducer differs from `add_messages` (exits 1).
