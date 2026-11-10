# Review of c2-delta-log (fenvix-brain prompts/R-review-c2-delta-log.md)

Report: fenvix-brain `sessions/2026-11-10-review-c2-delta-log/REPORT.md`. Judged code: `c2-delta-log` 7858d1c (3e899b0 adds results only).

- `src/fx/derive.rs`: three more planted bugs behind the test switch `PLANT` (2: a literal range one byte short; 3: a
  thread's message rows hold the previous thread's messages; 4: a thread's last message row missing). Off unless a test
  sets them; the owner's model test takes `FX_D_PLANT=n` and `FX_D_NOCRC=1`.
- `src/fx/derive_tests.rs`, section "review": `review_more_planted_bugs_are_caught`,
  `review_follower_reads_derived_documents_exactly`, `review_near_miss_shapes_are_left_untouched` (FAILS: FIX 3),
  `review_rollback_leaves_derived_rows_in_quiet_groups`.
- `read_paths.py`: the server check of every read path against mongod 7.0 (`results/read-paths.json`).
- `trial-merges/step5b`: merge with c2-step5b-reader and `review_wave_reader_rebuilds_derived_documents` (FAILS: FIX 1).
- `trial-merges/step8-7b`: merge into c2-step8-migration (step 7b's erasure) and `review_erasure_of_a_derived_thread`
  (FAILS: FIX 2).
- `results/bench-rerun.jsonl`, `results/competitors-rerun.json`: the measurements rerun on the same four real sets.

Commands:
```bash
cargo test --release --lib -- --test-threads 1 --nocapture review_
FX_D_PLANT=2 FX_D_OPS=300 cargo test --release --lib -- --test-threads 1 derived_model_with_crashes   # 3, 4 alike: fails
python research/c2-delta-log/review/read_paths.py          # MinIO on 9000, mongod rs0 on 27052
research/c2-delta-log/review/trial-merges/step5b/reproduce.sh
research/c2-delta-log/review/trial-merges/step8-7b/reproduce.sh
```
