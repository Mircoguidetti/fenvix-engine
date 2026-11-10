# integration-2: results of the integration session (2026-11-09)

Report: fenvix-brain `sessions/2026-11-09-integration-2/REPORT.md`. Everything here ran locally, $0.

- `takeover-run.sh`: runs the step 9 harness (fenvix-engine branch `step9-prep` d62b9dd, `step9/local/local-run.sh`
  up to its job list, unchanged) for ONE round with only the `takeovers` program: load (shrunk Atlas databases),
  measure (cook + 3 log copies, no m30 program), extras (warm spare + takeovers). Binaries: this branch's
  `mongo-server` and `pq`. Usage: `takeover-run.sh <step9-prep worktree> <work dir> on|off 50`.
- `results/takeovers-log-on/`, `results/takeovers-log-off/`: the program's summary and raw lines, the round's console
  and runner log. `log-records-applied-at-takeover.txt`: the line each new owner printed at each takeover (log on).
- `kill-tp.sh`, `results/kill-throughput.txt`: the judge's kill phase (`loadtest/test_group_commit.py --trials 1
  --secs 15`) alone on a quiet machine, 3 trials per setting: sealed runs off, and `FENVIX_SEALED_RUNS` = 2, 10, 50, 0.
- `r-tests.sh`, `results/review-agent-lane-r1-r5.txt`: the agent-lane reviewer's R1-R5 (`loadtest/review_agent_lane/`,
  unchanged), sealed runs off and `FENVIX_SEALED_RUNS=2`.
- `results/first-course-log-on/`: `bench/first-course/stress.sh` with the local log on (`--local-log-dir`, upload every
  60 s): 1 query and 3 queries per opening, 64 KiB openings; the same with `FENVIX_SEALED_RUNS=2`; and the negative
  control (`NEGATIVE=1`), which must find wrong answers. (A first try with 4 queries and 16 KiB openings found 0 wrong but
  kept no opening at all, so the script's own check failed it; not kept.)
- `results/cargo-test-workspace-no-fail-fast.txt`: `cargo test --release --workspace --no-fail-fast` (the judge's unit
  phase stops at the first failing test binary).
- The agent adapters' results are in `bench/agent-adapters/results-2026-11-09-integration-2/`.
