# perf-erase: measurements (2026-11-11)

Report: fenvix-brain `sessions/2026-11-11-perf-erase/REPORT.md`.

- `results/before-small/`: integration-3's erasure path with trace lines (`FX_ERASE_TRACE`), small profile, 6 users erased.
- `results/after-small/`: the same on 99412f3, then the batch (`scripts/post.py`) and the proof.
- `results/before-small-proof/`: integration-3's path, then the proof scan (16 run objects still held erased ids after "done").
- `results/line-fenvix-log-99412f3/`, `results/line-fenvix-log-f536a76/`: the benchmark's fenvix-log line, standard profile, the
  harness of `bench-local-2026-11-11` unchanged (`lines.py run fenvix-log --profile standard --read-sample 300
  --erase-users 100 --cold-threads 200 --workers 1`).
- `results/host-standard-4c0dfdd/`: our server alone on the host (MinIO with the harness's compression and trash purge),
  standard profile, then the batch under load, the retire, the delete comparison and the proof (`proof.json` after
  "done" with the 7-day restore window, `proof-end2.json` after a restart with a 60 s window and GC).
- `results/takeovers-on-f536a76/`: 50 takeovers, log on.
- `results/judge-fast-sealed2-derive-f536a76.md`: the fast judge's verdict.
- `scripts/repro.sh` (MinIO + our server + bench.py on the host), `scripts/post.py` (the batch measured), `scripts/sampler.py`.
- The proof tool: `src/bin/fx_erase_proof.rs` (`cargo build --release --bin fx_erase_proof`).
