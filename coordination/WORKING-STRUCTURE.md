# Working structure (agreed with the founder, 2026-11-06)

Proposed by this session, accepted by the founder ("sì, sono d'accordo"). For the coordinator to move into RULES.md and DECISIONS.md.

## The problem it fixes
The referee was a reviewer writing a report. A reviewer always finds something, and two reviewers find different things: that is the loop. The brain also holds about 190 sessions, mostly paper estimates correcting each other.

## The rules
1. The referee is the judge, not a person. `fenvix-engine/judge/` (branch `integration` today): `judge/setup.sh` once, then `judge/run.sh` runs every existing suite on the current commit and writes `judge/out/<commit>/VERDICT.md`, one line per phase, PASS or FAIL with counts. Phases: build, unit, diff-py and diff-node (same answers as a real mongod 7.0), ctl (control-layer tests), kill (SIGKILL durability with negative controls). It runs inside any Claude Code session sandbox: $0, no docker, no GitHub Actions minutes. Tools come from conda-forge (Docker Hub and fastdl.mongodb.org are blocked from the sandboxes). The real mongod is only the answer key and never runs in the product.
2. The reviewer writes tests, not opinions. A defect exists only as a test that fails in the judge. A red test gets fixed. If the reviewer cannot write the test, it is a note and never blocks.
3. One standing reviewer, always the same session, for every piece. It owns the judge and remembers what it already checked.
4. Few long-lived owners: Engine (compaction, waiter, log: one program), Kitchen and measurements (AWS, the comparison with Atlas), the standing Reviewer, plus the coordinator who talks to the founder.
5. No new paper estimate until the measurement that would replace it has been done. One page with the three numbers that decide the product (first cold read p99 through our server on real S3; S3 trips per cold find with per-customer compaction; cost at 1 TB, quiet profile, with the warm spare), updated with MEASURED labels.
6. The founder decides three things only: money, market, and when owner and reviewer disagree on whether a test is legitimate.
7. GitHub Actions minutes are kept for long AWS runs only (founder, 2026-11-06: private-repo minutes are limited).
8. Merge to main: the judge's verdict on the exact commit is all PASS, run by the owner and re-run by the reviewer.

## State at writing
- `integration` = main + T1 with R-T1's tests + L1 + Q8d + kitchen workflow + compaction prototypes. Builds.
- First judge run: build PASS; unit 101 passed, 1 failed. The failing test is R-T1's `rev_maps_leave_room_for_active_data`: maps pinned in RAM leave zero room for data, a known defect still to fix (R-T1 correction 2). Longer phases running.
