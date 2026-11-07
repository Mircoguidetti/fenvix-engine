# Results (TLC 1.8.0, Java 21, 4 workers, 2026-11-07)

Every number here is MEASURED by TLC on this commit's spec; the outputs are in `output/`. "Distinct states" is TLC's count after the symmetry a1 <-> a2.

## The correct model, full shape (`MC-main.cfg`)

Shape: 2 groups (3 keys, 2 customers), 3 parent writes (put, delete, erasure), 2 rewrites (delta merges, reseals, group merge), 1 crash, 1 fork, 1 checkpoint, the find reader, the transaction and the follower, E0 then E1, lost and slow PUTs, owner and remote drops, GC.

PASS within the time budget: 114,291,793 distinct states (604,346,821 generated) with no violation of any invariant, 60 minutes, TLC was at depth 12 (the initial state is depth 1), so every behaviour of up to 10 steps is checked completely; 80,217,363 states were left on the queue. TLC's fingerprint collision estimate: 0.0017 (based on the actual fingerprints).

The full shape does not close: its state space grows with every level, because every component (writes, flushes, the compactor, three readers, the checkpoint, the fork, time) interleaves with every other. TLC stops it at the time budget (`StopAfter`, through `MCBudget` in `MC.tla`), so "no violation" holds for every state explored, and every behaviour shorter than the reported depth minus one is checked completely.

## The correct model, complete runs

Two configurations close:
- `CkptNoLease-ok` (1 write, 2 rewrites, 1 crash, the checkpoint, no readers): PASS, complete, 28,199,452 distinct states, depth 34, 24 min 34 s.
- `DropBeforePublish-ok` (1 write, 2 rewrites, 1 crash): PASS, complete, 4,125,682 distinct states, depth 31.

## Negative controls

Each control is one change to the spec (`Ctl`), run with only the parts its failure needs (listed in `gen.py`, `NEEDS`). Each has a twin, `MC-<control>-ok.cfg`, the same configuration with the correct model, run with a 150 s budget (`CkptNoLease-ok` runs to completion, above): every twin found no violation (the 17 budgeted twins explored 85,254,888 distinct states together, plus 28,199,452 in `CkptNoLease-ok`). A budgeted twin does not always reach its control's violation depth (for example `ReadNoCut-ok` stopped at depth 18, its control fails at 21); the full-shape run and the two complete runs cover the correct model beyond that. All 17 controls are caught. TLC prints a remote drop job and GC as `Next` and both drop jobs as `DropJob`; they are named below.

The 14 the prompt fixes:
- `DropBeforePublish`: `LatestReadOK` violated at depth 6. put, flush, absorb (PUT in flight), drop with the cut in memory.
- `GcNoGrace`: `NoBadRead` (the transaction reads a deleted run) at depth 13. put, flush, absorb, land, reseal, land, group merge (PUT in flight), the transaction pins, land, GC at once, the transaction reads.
- `ReadNoCut`: `LatestReadOK` at depth 21 (22,686,403 states). erasure, flush, put, flush, erasure, flush, three absorptions, reseal, delta merge (drops both erasures as dead rows, the put stays in its delta run), drop job after the lease: the put comes back.
- `ResealSkip`: `LatestReadOK` at depth 8. A reseal that leaves out the only delta run loses the put.
- `HoldsIgnored`: `NoBadRead` at depth 11. The transaction pins v1, absorb lands, the drop job sees v2, two ticks, the drop removes rows the transaction still needs.
- `TxnRereads`: `NoBadRead` at depth 9. The transaction begins, a put is absorbed, the transaction reads a value newer than its S.
- `LeaseFromStamp`: `NoBadRead` at depth 11. Absorb starts, the PUT takes two ticks, the follower refreshes before it lands (old version), the drop counts the lease from the stamp and drops at once, the follower reads.
- `FollowerCheckOnce`: `NoBadRead` at depth 11. The follower starts an operation, a version lands and its lease passes, the drop runs, the follower's next step reads without checking.
- `RevokeNoCheck`: `NoBadRead` at depth 12. The transaction's hold is revoked at the hold life, the drop runs, the transaction keeps reading.
- `DropRemoteShortLease`: `NoBadRead` at depth 11. A remote drop job (shown as `Next`) with the ordinary lease drops rows the transaction (age 2, hold life 3) still needs.
- `ForkCopyEenv`: `EncodingOK` at depth 7. put (E0), E1 at seq 1, checkpoint at `last_l0_seq` 0, fork, the fork's first write at seq 1 is an envelope but its copied E_env 1 says plain.
- `CkptNoLease`: `CkptReadOK` at depth 11. The checkpoint pairs with v1, absorb lands, the drop runs after the lease, SlateDB's checkpoint is taken without the rows of (X_v1, X_v2].
- `MergeConcat`: `XgOK` at depth 10. Two erasures, flush, absorb, reseal of g1 at X_d 2, group merge by concatenation: the X_g of a1 and a2 goes from 2 to 0. With three rewrites, `MergeConcat-reads` shows the read failure too: `LatestReadOK` at depth 20 (17,891,887 states), the put between two erasures comes back.
- `DeltaDropEnv`: `LatestReadOK` at depth 12. put and erasure absorbed in two runs, a delta merge drops the erasure: the put comes back.

Three more:
- `FollowerNoTpin` (review round 1, a follower reading past T_pin): `NoBadRead` at depth 11.
- `ForkNewestCut` (review round 1, a fork paired with the parent's newest cut): `ForkReadOK` at depth 9. A put in the memtable, the checkpoint taken at `last_l0_seq` 0, the put flushed and absorbed (X_d 1), the fork paired with that newest version reads the parent's later put, which its checkpoint does not hold.
- `MergeAtMaxCut` (found by this model, below): `ErasedGone` at depth 16.

## Found by the model: the group merge must rewrite at X_d

Review round 2 (fix 4, I2) says a group merge is "a rewrite at the common cut max(X_gA, X_gB)". TLC found a trace where that leaves an erased row in the new sealed run (`output/MergeAtMaxCut.txt`, 15 steps):
1. put a1 (seq 1) and erase customer A (seq 2) in one L0 SST; erase A again (seq 3) in the next.
2. Absorb the first SST: a delta run with the put and the erasure at 2. Reseal group g2 (b1's group, empty) at X_d 2.
3. Absorb the second SST; a delta merge folds the two runs. Newest row per key keeps the erasure at 3 and drops the one at 2 (allowed: both are above X_g1 = 0, and the newer one covers everything the older one did).
4. Group merge of g1 into g2 at max(X_g1 0, X_g2 2) = 2: it reads the delta rows of g1 in (0, 2], finds the put at 1 and no erasure (the one at 2 is gone, the one at 3 is above the cut), and writes the put into the new sealed run with X_g 2.

Reads stay right (the erasure at 3 is still in the delta, above X_g, and wins), so `NoBadRead` and `LatestReadOK` hold. But the erased row now sits in a sealed run whose X_g is at or above the erasure the customer asked for, until a later reseal passes seq 3. With the correct model (merge at X_d, which is at or above every delta row, as an ordinary reseal is) the same configuration finds no violation. A reseal never has this problem, because it always cuts at X_d.

The rule for the code: a group merge rewrites both groups at X_d, not at max(X_gA, X_gB). It costs the delta rows of both groups up to X_d in the same pass, which is what a reseal of each would read anyway.
