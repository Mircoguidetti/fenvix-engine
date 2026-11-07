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
