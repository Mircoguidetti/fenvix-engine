# Compaction 2, step 2: key layout v2 and envelopes (checks)

Report: fenvix-brain `sessions/2026-11-07-c2-step2-layout/REPORT.md`. Everything here is local and $0 (`judge/setup.sh`
first: mongod 7.0 on 27020, MinIO on 9000).

- `diff_keys.sh <binary> <out> [server flags]`: both differential suites, every test, with customer keys on for the
  big collections on their most common field (often an array, a document or missing; the stress set of compaction-1).
  Run for the report with `--envelopes write`. Do not run the agent adapters' Mastra suite at the same time: both use
  port 27019.
- `stress.py`: the first-course stress of `first-course-engine` (bench/first-course/stress.py), with the first-course
  counters optional and `--ref-uri` to compare every answer with a reference mongod as well. Run for the report:
  `stress.py --uri <ours> --ref-uri mongodb://localhost:27020/?directConnection=true --rounds 60 --users 40 --idle 0`,
  once with `--envelopes read` and once with `--envelopes write` on the server.
- The ctl tests: `ctl-tests/test_44_layout_v2.py` (per-customer `_id`, the time field, layout v2 against a plain
  collection), `ctl-tests/test_45_envelope_rollback.py` (two-release rule; `E0_BIN` = a build of the E0 commit
  6fbacb9, `OLD_BIN` = a build of `compaction` a7890b3).
- Agent adapters: `bench/agent-adapters/run_suites.sh ours` with `OURS_EXTRA="--customer-key ..."` (the list is in the
  report); the 2026-11-06 results of the agent-adapters session are kept in `bench/agent-adapters/results-2026-11-06`.
