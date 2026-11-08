# Results

## Fix round (FIX 5), 2026-11-08

TLC from the tlaplus GitHub release v1.8.0 (`tla2tools.jar`, sha256 e1f11b75...dcd7f; it reports itself as build 2026-11-06), Java 21, 4 workers, 8 GB heap. Every number in this section is MEASURED by TLC on this commit's spec; the outputs are in `output/`. "Distinct states" is TLC's count after the symmetry a1 <-> a2 (no symmetry in the spanning layout).

The five review scenarios and the takeover note (fenvix-brain `sessions/2026-11-07-review-c2-step0/REPORT.md`) are now in `Compaction2.tla`, with their remedies in the correct model. Each has a negative control that is the delivered rule without its remedy. Each control runs in the reviewer's configuration (`gen.py`, `FIXNEEDS`), and its `-ok` twin is the same configuration with the correct model.

### The correct model, full shape (`MC-main.cfg`)

@@MAIN@@

### The new controls (each must fail) and their twins (each must pass)

- `NoFence-crash` (F1, a restart): `NoBadRead` violated at depth 13. put, flush, absorb (PUT in flight), crash, the dead process's PUT lands, the drop sees v2 and drops after the lease, a find begins on v1 (the version the restarted waiter knows) and reads the put as absent. Twin: PASS, complete, 1,755,573 distinct states, depth 25.
- `NoFence-timeout` (F1, no crash: found in this round): `NoBadRead` at depth 12. The same failure from a PUT that times out in a live waiter and lands later (TLC prints `PublishTimeout` as `Next`). Twin, with lost and timed-out PUTs, remote drops and GC: PASS within a 15-minute budget, 35,565,854 distinct states, depth 22, not complete.
- `FollowerOldEenv` (F2, the delivered rules): `NoBadRead` (`follower:envelope-misread`) at depth 8. E1 is announced and starts at once, an erasure is written as an envelope and flushed, and the follower, still inside T_pin, decodes it with its old E_env.
- `E1NoWait` (F2, half of the remedy: no wait after the announcement): `NoBadRead` at depth 8. A follower that refreshed before the announcement reads an envelope inside T_pin.
- `FollowerNoMarker` (F2, the other half: the follower does not read the marker): `NoBadRead` at depth 11. The follower refreshes after the announcement, E1 starts T_pin later, and the follower's next step decodes an envelope as data.
- The three F2 twins are one configuration (the correct model does not depend on the control's name): PASS, complete, 1,035,745 distinct states, depth 24.
- `EraseOneGroup` (F3): `LatestReadOK` at depth 7. With customer A over both groups, put a2 (in g2), erase customer A, flush, absorb, land: the erasure counts for g1 only, and a2 reads as present. Twin: PASS, complete, 1,761,478 distinct states, depth 22 (the reviewer's fix gave the same count).
- `CkNoRecheck` (F4): `CkptReadOK` at depth 16. The checkpoint begins, its lease expires after 3 ticks, v2 lands, the drop runs after the lease, and SlateDB's checkpoint is made and kept without the rows of (X_v1, X_v2]. Twin: PASS, complete, 196,643 distinct states, depth 22.
- `RollbackPlain` (F5, rule 21 as delivered): `EncodingOK` at depth 5. Rollback to E0 in a database with the marker, and a put is stored plain above E_env. Twin: PASS, complete, 85,161 distinct states, depth 22.
- `ZombieShortLease` (N1): `NoBadRead` at depth 12. The transaction pins v1, a takeover leaves it open, v2 lands, the new waiter's drop uses the ordinary lease, and the old waiter's transaction reads a dropped row. Twin: PASS, complete, 2,458,607 distinct states, depth 25.
- `Span-ok` (F3 with more of the system: the find reader, the transaction, a crash, two writes): PASS within a 15-minute budget, 36,062,277 distinct states, depth 15, not complete.

### The 17 earlier controls, rerun on the new spec

All 17 are still caught, each on the same invariant as before:
- `DropBeforePublish`: `LatestReadOK`, depth 7.
- `GcNoGrace`: `NoBadRead`, depth 13.
- `ReadNoCut`: `LatestReadOK`, depth 21.
- `ResealSkip`: `LatestReadOK`, depth 8.
- `HoldsIgnored`: `NoBadRead`, depth 11.
- `TxnRereads`: `NoBadRead`, depth 8.
- `LeaseFromStamp`: `NoBadRead`, depth 11.
- `FollowerCheckOnce`: `NoBadRead`, depth 11.
- `RevokeNoCheck`: `NoBadRead`, depth 12.
- `DropRemoteShortLease`: `NoBadRead`, depth 11.
- `ForkCopyEenv`: `EncodingOK`, depth 10.
- `CkptNoLease`: `CkptReadOK`, depth 13.
- `MergeConcat`: `XgOK`, depth 10.
- `DeltaDropEnv`: `LatestReadOK`, depth 12.
- `FollowerNoTpin`: `NoBadRead`, depth 11.
- `ForkNewestCut`: `ForkReadOK`, depth 10.
- `MergeAtMaxCut`: `ErasedGone`, depth 16.

`MergeConcat-reads` still shows the read failure: `LatestReadOK`, depth 20.

Some traces are one or two steps longer than before. E1 now takes an announcement and a T_pin wait (`ForkCopyEenv`), and the checkpoint takes a second step for the lease check (`CkptNoLease`).

The twins found no violation. 17 of them ran with a 150 s budget and explored 110,954,438 distinct states together (`MergeConcat-reads-ok` included). `CkptNoLease-ok` no longer closes now that the lease expires and the checkpoint can restart: within a 30-minute budget it reached 88,790,600 distinct states, depth 21, not complete.

## Delivered round, 2026-11-07 (kept as delivered; the counts are for the spec of that commit)


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
