# Rules for the code (build steps 1-8)

Every rule below is a guard in `Compaction2.tla`. When a rule has a negative control (in brackets), TLC finds a broken trace once that rule is removed. Rules without a control are assumptions the model makes; the code must make them true.

## Reads (step 3)
1. A read takes our version and its SlateDB view as one pair, at begin, and uses that pair for every step: index scan, document fetches, every page of an export. A transaction never re-reads the manifest [TxnRereads].
2. The read rule has three bounds: SlateDB rows with X_d < seq <= S, delta rows of the key's group with X_g < seq <= X_d, the group's sealed run. All three bounds are needed [ReadNoCut]. The failure needs dead rows dropped by a delta merge, so a short test will not see it: keep the unit test from TLC's trace (21 steps, `output/ReadNoCut.txt`).
3. A delete envelope or an erasure (range tombstone over the customer's range) that wins means "absent".

## Holds (steps 3, 4)
4. A hold in the writer's process (find, transaction, export page) counts in the drop floor while it lives [HoldsIgnored].
5. A hold lives at most the hold life (120 s proposed). Then it is revoked and leaves the floor, and the reader checks its hold after every step and fails with a retryable error [RevokeNoCheck].
6. A follower (another process) checks the age of its last refresh before every step that opens or refreshes a SlateDB view, not once per operation [FollowerCheckOnce], and refuses to read past T_pin [FollowerNoTpin].

## Drops (step 4)
7. The drop floor is computed inside the drop job (the compaction filter), from the versions in the store, never from the cut in memory of a publish in flight [DropBeforePublish].
8. The lease of a version counts from the first time a floor computation saw it in the store, with the drop process's own clock, never from a stamp written before the PUT [LeaseFromStamp]. The model uses one shared first-sight table; per-process first sights are later, so they are safe too.
9. A crash forgets first sights: after a restart, no version is usable for the floor until it has been seen again for one lease.
10. A drop job runs in the process that holds the database's holds (the waiter); a job elsewhere uses a time-only floor with a lease longer than the hold life [DropRemoteShortLease].
11. A drop job waits while a checkpoint lease is held.
12. Every drop folds the absorbed L0 SSTs and SlateDB's sorted run into that run (one sorted run, always); rows above the floor stay in it.

## Rewrites (step 4)
13. A delta merge drops a row only at or below its group's X_g; an envelope or an erasure above X_g stays [DeltaDropEnv].
14. A reseal of group g reads its sealed run and every delta run's rows of g in (X_g, X_d] [ResealSkip], applies erasures, and drops every envelope; X_g becomes X_d.
15. A group merge rewrites both groups at X_d; X_g never goes down [MergeConcat]. With concatenation, the read failure needs three rewrites (`output/MergeConcat-reads.txt`); the structural check `XgOK` catches it at the merge. The cut max(X_gA, X_gB) of review round 2 keeps reads right but can copy an erased put into the new sealed run after a delta merge folded the older erasure into a newer one [MergeAtMaxCut]; X_d removes that case.
16. Absorption takes the oldest L0 SST not absorbed yet, with no gap (I3); X_d is its highest seq.
17. A publish is a conditional create; a PUT can land after the compactor crashed, and a lost PUT leaves an orphan run that GC removes.

## Checkpoints, forks, PITR, export (steps 3, 7)
18. A checkpoint taken outside the waiter's process takes the drop lease in the store before it reads the version it pairs with, and releases it after SlateDB's checkpoint exists [CkptNoLease]. The pair satisfies X_d <= `last_l0_seq`.
19. The pair is visible to GC from the start of the checkpoint (the lease names the version), and its runs are kept while the checkpoint lives.
20. A fork or PITR staging clone starts from a copy of the paired version, never the parent's newest one [ForkNewestCut], shares its runs by reference (GC counts them), and sets E_env = min(parent E_env, checkpoint `last_l0_seq`) [ForkCopyEenv].

## Envelopes (steps 2, 7)
21. Release E0 reads envelopes above E_env and writes none; release E1 sets E_env to the current seq once and writes envelopes above it. Every stored row decodes with its own database's E_env (`EncodingOK`).
22. Erasure is one range tombstone over the customer's key range (layout v2 makes it one range), carried through absorption and delta merges like an envelope, applied and dropped only by a reseal at or above its seq (`ErasedGone`).

## GC (step 4)
23. GC deletes a retired or orphaned run only after a grace longer than the hold life [GcNoGrace], and never one listed by a checkpoint pair, its lease, or a fork.
