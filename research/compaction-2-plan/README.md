# Compaction 2 plan: probes

Plan: fenvix-brain `sessions/2026-11-07-compaction-2-plan/REPORT.md`. Nothing here is used by the server.

- `probe/` (standalone crate, not in the engine workspace): probe 1. Can SlateDB 0.17 stay the write path while compacted
  data lives in our own sealed runs, with SlateDB's public API only? Checks: raw L0 rows with sequence numbers
  (`SstReader::read_block`), disjoint L0 sequence ranges (a cut X separates absorbed SSTs), a merged read that takes
  SlateDB rows above X and sealed rows at or below X (equal to a model before publish, after publish, after SlateDB drops
  the absorbed rows, after a restart), a compaction job with a filter that drops rows at or below X (output empty),
  SlateDB's merged scan hiding a real tombstone (why deletes must be an envelope value), and a conditional create as the
  manifest publish primitive. Run: `cd probe && cargo run --release`. Result: `results/probe-1.txt` (PROBE PASS).

## Review (2026-11-07, branch `review-compaction-2-plan`)

- `REVIEW.md`: the review of the plan (first line: the verdict).
- `probe/src/bin/probe2.rs`: probe 2, readers and transactions kept alive across publish and drop, sorted-run growth,
  sequence numbers across restart, writer change and clone. Run: `cd probe && cargo run --release --bin probe2`
  (about 5 minutes). Result: `results/probe-2.txt`.
- `review/review_cost.py` (imports the plan's `plan_cost.py` unchanged; argument: a fenvix-brain checkout) and
  `review/delta_sim.py` (model of the delta tier): results in `review/*.txt`. Both `python3 -I`, no network.

## Round 2 (2026-11-07, branch `compaction-2-plan-r2`)

The plan owner's answer to the review's 7 fixes: fenvix-brain `sessions/2026-11-07-compaction-2-plan-r2/REPORT.md`.

- `probe/src/bin/probe3.rs`: probe 3, probe 2's sequences under the round-2 protocol (pins, a lease for readers in
  other processes, the drop floor read from the published manifest, every drop folded into the newest sorted run,
  checkpoints paired with a version under the drop lock), plus a follower case, the fail point "cut set in memory,
  publish refused", and the fix-4 invariants on a model. Controls switch the rules off and must reproduce probe 2's
  failures. Run: `cd probe && cargo run --release --bin probe3` (about 5 minutes). Result: `results/probe-3.txt`.
- `r2/r2_numbers.py` (imports the plan's `plan_cost.py` unchanged; argument: a fenvix-brain checkout): GETs per cold
  read by write pattern with the round-2 options, amplification against today's tiered compaction, monthly costs,
  hedged-GET tail, CPU per database. `python3 -I`, no network, about 50 seconds. Result: `r2/r2_numbers.txt`.
