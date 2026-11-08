# Rules for the code (build steps 1-8)

Every rule below is a guard in `Compaction2.tla`. When a rule has a negative control (in brackets), TLC finds a broken trace once that rule is removed. Rules without a control are assumptions the model makes; the code must make them true.

Fix round of the review (FIX 5, 2026-11-08): rules 5, 6, 13, 14, 17, 18, 21 and 22 changed, rules 24 to 30 are new. Each change names the review scenario that needs it (F1 to F5, N1, N5, N6 in fenvix-brain `sessions/2026-11-07-review-c2-step0/REPORT.md`) and its control.

## Reads (step 3)
1. A read takes our version and its SlateDB view as one pair, at begin, and uses that pair for every step: index scan, document fetches, every page of an export. A transaction never re-reads the manifest [TxnRereads].
2. The read rule has three bounds: SlateDB rows with X_d < seq <= S, delta rows of the key's group with X_g < seq <= X_d, the group's sealed run. All three bounds are needed [ReadNoCut]. The failure needs dead rows dropped by a delta merge, so a short test will not see it: keep the unit test from TLC's trace (20 steps, `output/ReadNoCut.txt`).
3. A delete envelope or an erasure (range tombstone over the customer's range) that wins means "absent".

## Holds (steps 3, 4)
4. A hold in the writer's process (find, transaction, export page) counts in the drop floor while it lives [HoldsIgnored].
5. A hold lives at most the hold life (120 s proposed). Then it is revoked and leaves the floor, and the reader checks its hold after every step, before any row of that step is returned to the client, and fails with a retryable error [RevokeNoCheck]. (Changed, N5: "before returning"; the model's reads are atomic steps.)
6. A follower (another process) checks the age of its last refresh before every step that opens or refreshes a SlateDB view, not once per operation [FollowerCheckOnce], and refuses to read past T_pin [FollowerNoTpin]. Each refresh also takes E1's announcement and E_env from the store (rule 26).

## Drops (step 4)
7. The drop floor is computed inside the drop job (the compaction filter), from the versions in the store, never from the cut in memory of a publish in flight [DropBeforePublish].
8. The lease of a version counts from the first time a floor computation saw it in the store, with the drop process's own clock, never from a stamp written before the PUT [LeaseFromStamp]. The model uses one shared first-sight table; per-process first sights are later, so they are safe too.
9. A crash forgets first sights: after a restart, no version is usable for the floor until it has been seen again for one lease.
10. A drop job runs in the process that holds the database's holds (the waiter); a job elsewhere uses a time-only floor with a lease longer than the hold life [DropRemoteShortLease].
11. A drop job waits while a checkpoint lease is held.
12. Every drop folds the absorbed L0 SSTs and SlateDB's sorted run into that run (one sorted run, always); rows above the floor stay in it.

## Rewrites (step 4)
13. A delta merge drops a row only at or below its group's X_g; an envelope or an erasure above X_g stays [DeltaDropEnv]. An erasure stays while it is above the X_g of any group its customer's range overlaps (changed, F3 [EraseOneGroup]).
14. A reseal of group g reads its sealed run and every delta run's rows of g in (X_g, X_d] [ResealSkip], applies erasures, and drops every envelope; X_g becomes X_d. The rows of g include every erasure whose customer range overlaps g (changed, F3 [EraseOneGroup]).
15. A group merge rewrites both groups at X_d; X_g never goes down [MergeConcat]. With concatenation, the read failure needs three rewrites (`output/MergeConcat-reads.txt`); the structural check `XgOK` catches it at the merge. The cut max(X_gA, X_gB) of review round 2 keeps reads right but can copy an erased put into the new sealed run after a delta merge folded the older erasure into a newer one [MergeAtMaxCut]; X_d removes that case.
16. Absorption takes the oldest L0 SST not absorbed yet, with no gap (I3); X_d is its highest seq.
17. A publish is a conditional create of the slot after the version the compactor built on; a PUT can land after the compactor crashed or after it timed out, and a lost PUT leaves an orphan run that GC removes. GC decides what is an orphan from the store (runs listed by no version, no checkpoint pair and no fork), never from what the publisher believes: a PUT that timed out may have landed (changed, N6, F1).

## Checkpoints, forks, PITR, export (steps 3, 7)
18. A checkpoint taken outside the waiter's process takes the drop lease in the store before it reads the version it pairs with, and releases it after SlateDB's checkpoint exists [CkptNoLease]. The pair satisfies X_d <= `last_l0_seq`. The lease has a life (a lease that never expires would block every drop forever if the control layer dies). After SlateDB's checkpoint exists, the control layer checks that the lease is still held; if it expired, it deletes that checkpoint and starts again [CkNoRecheck] (changed, F4). The check must leave a margin for clock skew between the control layer and the drop jobs (the model has one clock).
19. The pair is visible to GC from the start of the checkpoint (the lease names the version), and its runs are kept while the checkpoint lives.
20. A fork or PITR staging clone starts from a copy of the paired version, never the parent's newest one [ForkNewestCut], shares its runs by reference (GC counts them), and sets E_env = min(parent E_env, checkpoint `last_l0_seq`) [ForkCopyEenv].

## Envelopes (steps 2, 7)
21. Release E0 reads envelopes above E_env. In a database without the marker it writes none; in a database with the marker (after a rollback from E1) it writes envelopes above E_env exactly as E1 does, and it never sets the marker. Release E1 sets E_env (the marker, in the store) to the current seq once and writes envelopes above it. Every stored row decodes with its own database's E_env (`EncodingOK`). (Replaced, F5 [RollbackPlain]: the delivered text, "E0 writes none", breaks the 7-day rollback; this is step 2's rule, its `test_45`.)
22. Erasure is one range tombstone over the customer's key range (layout v2 makes it one range), carried through absorption and delta merges like an envelope, applied and dropped only by a reseal at or above its seq (`ErasedGone`). It belongs to every group its range overlaps, for reads, delta merges and reseals: a whale customer (about 95 GB at 1 TB, ESTIMATED in the plan) spans many groups, and a split by key range can cut a small customer in two (changed, F3 [EraseOneGroup]).

## GC (step 4)
23. GC deletes a retired or orphaned run only after a grace longer than the hold life [GcNoGrace], and never one listed by a checkpoint pair, its lease, or a fork.

## Knowing the newest version (steps 3, 4), new in the fix round (F1, N6)
24. A hold pins the version the waiter's process knows (in memory), and the compactor builds on that version. The waiter learns a version when its own PUT is acknowledged, or when its conditional create fails (it reloads).
25. Whenever the waiter does not know the outcome of a PUT (it has just opened after a restart or a takeover, or a PUT timed out), it publishes a fence version before it pins any hold and before it starts another publish: a copy of the version it knows, written with the same conditional create at the next slot. A late PUT of the dead process, or the timed-out PUT, then finds its slot taken and fails as an orphan; or the fence's create fails and the waiter reloads. Either way the waiter knows the newest version [NoFence: `NoFence-crash` and `NoFence-timeout`]. Cost: one small PUT per open and per timeout (ESTIMATED: about 64 KB at 10 TB, the plan's manifest size). The model takes the fence as one atomic step; in the code, holds wait until the fence's outcome is known, and a fence that times out is itself followed by another fence.

## Release E1 and followers (steps 2, 3, 7), new in the fix round (F2)
26. Release E1 starts in two steps. First it is announced in the store (no envelopes yet). Envelope writing starts, and E_env is set, only once the announcement is at least T_pin old [E1NoWait]. A follower takes the announcement and E_env with every refresh; a follower that has seen the announcement but not the value reads the marker from the store before each step, after that step's SlateDB view is open [FollowerNoMarker]. Without both, a follower decodes an envelope as data [FollowerOldEenv, the delivered rules]. The marker read happens only during the transition (a follower that has E_env in its refresh needs none), so its cost does not grow with the database. Step 2 keeps E1 off, so no code is broken today; step 3 must have this rule before E1 is turned on.

## Takeover (step 4), new in the fix round (N1)
27. After a takeover, the old waiter may be slow or cut off rather than dead, and its open transactions may keep reading until their hold life; the new waiter does not know their holds. For one hold life after it opens, the new waiter's drop jobs use the remote lease (longer than the hold life) instead of the ordinary one [ZombieShortLease]. Whether SlateDB 0.17's fenced-out writer keeps reading its old SSTs is UNKNOWN; the rule makes it not matter. Step 4 must test a takeover with an open transaction.
28. The old waiter's holds check revocation with their own clock (rule 5), so they stop at the hold life; the old waiter's compactor stops at the first fencing error and never starts a new publish after it (two live publishers are not modelled).

## Notes that stay rules for later steps
29. `ErasedGone` checks only the newest version's sealed runs; the erasure test of step 7 must also cover delta runs, SlateDB's sorted run and the 24-hour deadline (N3, not modelled: liveness).
30. A fork shares the parent's runs by reference: its own GC never deletes an object outside its own prefix, and the parent's GC counts the fork's references (N4; the fork's own GC is not modelled).
