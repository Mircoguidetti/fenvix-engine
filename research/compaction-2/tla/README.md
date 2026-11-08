# Compaction 2, step 0: the TLA+ model

The paper check of way (b) (fenvix-brain DECISIONS 2026-11-07): SlateDB 0.17 keeps the write path, our own manifest, delta runs and sealed runs hold the compacted data. It extends the M28 spec (fenvix-engine branch `tla-lavaggio`, `docs/tla-lavaggio/`) to the design of the compaction 2 plan, its round 2 and both reviews (fenvix-brain `sessions/2026-11-07-*compaction-2-plan*`).

Files:
- `Compaction2.tla`: the spec. The header comment lists what is modelled.
- `MC.tla`: the model values (keys a1, a2 of customer A in group g1, b1 of customer B in group g2) and the symmetry a1 <-> a2.
- `gen.py`: writes every configuration `MC-<name>.cfg`. `MC-main.cfg` is the correct model in the shape the prompt fixes; `MC-<control>.cfg` is one negative control; `MC-<control>-ok.cfg` is the same configuration with the correct model, which must pass, so each failure is caused by the control alone.
- `run.sh`: runs TLC: `./run.sh main`, `./run.sh ReadNoCut ReadNoCut-ok`, or `./run.sh all`. Output in `output/<name>.txt` (every counterexample trace is there).
- `summarize.py output/*.txt`: each verdict, and the action sequence of each counterexample.
- `RULES-FOR-THE-CODE.md`: the rules the model shows are needed, for build steps 1-8.
- `RESULTS.md`: what TLC found, run by run.
- `review/`: the reviewer's copy of the spec (FIX 5, 2026-11-07), kept as delivered. Its six scenarios and their remedies are now in `Compaction2.tla` itself (fix round, 2026-11-08).

TLC: `tla2tools.jar` v1.8.0 from the tlaplus GitHub releases, Java 21. `TLA2TOOLS=/path/tla2tools.jar WORKERS=4 TLCARGS=-gzip ./run.sh main` (90 minutes; `-gzip` keeps the disk queue small). `./run.sh all` runs everything (about 4 hours on 4 cores).

## The model in one page

State:
- SlateDB: the memtable (kept by the WAL across a crash), L0 SSTs oldest first, one sorted run, `last_l0_seq`.
- Ours: manifest versions in the store (each with X_d, X_g per group, the delta runs, the sealed run per group, the key-to-group map), runs (live or gone), a publish in flight.
- Ghost: the value of every key after every sequence number, so every read is compared with the truth.

Actions:
- Writes: put, delete (an envelope row), erasure of a customer (a range tombstone over the customer's whole key range). Release E0 writes plain puts only; `EnableE1` sets E_env to the current seq, and every later write is an envelope.
- Flush: memtable to a new L0 SST.
- Absorb: the oldest L0 SST not yet absorbed (no gap, I3) becomes a new delta run, newest row per key; the new version has X_d = its highest seq.
- Delta merge of two runs: rows at or below their group's X_g are dead and leave; an envelope above X_g stays (I1).
- Reseal of a group at X_d: its sealed run plus its delta rows in (X_g, X_d], erasures applied, every envelope dropped; X_g = X_d.
- Group merge: a rewrite of both groups at X_d (I2). The plan's cut max(X_gA, X_gB) is the control `MergeAtMaxCut`: see `RESULTS.md`.
- Publish in two steps: the PUT starts (with the stamp), then lands with a conditional create, or is lost (the run is an orphan). Time can pass in between: a slow publish. A crash does not stop a PUT in flight.
- Drop job: folds the absorbed L0 SSTs and the sorted run into the sorted run (one sorted run, always), keeping rows above the floor. The floor is computed inside the job from the versions in the store. Every floor computation records the first sight of each version; the lease counts from it. In the hold owner's process the floor is min(X of the newest version seen at least one lease ago, X of every live hold). In the remote pool it is the X of the newest version seen at least `RemoteLease` ago, with `RemoteLease` > the hold life, and no holds. Both wait while the checkpoint lease is held.
- Holds (owner's process): a find reader and a transaction. Begin pins the newest version and S as one pair; every step reads one key with that pair; a hold reaching the hold life is revoked (it leaves the floor), and the check after each step then fails the operation.
- Follower (another process): refreshes the version it knows; every step opens a fresh SlateDB view (flushed rows, S = `last_l0_seq`) and is refused when the last refresh is `Tpin` old or older.
- Checkpoint (control layer): begin pairs with the newest version and takes the drop lease in the store; take copies SlateDB's checkpoint state (flushed rows and `last_l0_seq`) and releases the lease.
- Fork / PITR staging clone from the checkpoint (after E1): continues at the checkpoint's seq, starts from a copy of the paired version (runs shared by reference), E_env = min(parent E_env, checkpoint `last_l0_seq`); one write of its own.
- GC: a run retired from the newest version (or orphaned) for at least `Grace` > the hold life, and not listed by the checkpoint pair or the fork.
- Crash of the owner's process: first sights are lost; the memtable survives (WAL); a PUT in flight is no longer heard by the owner. "dead": the holds end. "cutoff" (a takeover of a slow process, N1): its open holds keep reading, unknown to the new waiter, until their hold life. The new waiter loads the newest version and must fence.
- Fix round (FIX 5), each with its remedy in the correct model:
  - The version the owner knows (F1): holds pin it, the compactor builds on it. A PUT can time out (`PublishTimeout`) and still land later, unheard. After a restart or a timeout the owner publishes a fence version (`Fence`) before any hold or publish.
  - E_env in the store (F2): `AnnounceE1`, then `EnableE1` at least `Tpin` later. The follower takes the announcement and E_env with each refresh, and reads the marker before a step when it knows of the announcement but not the value. A follower step that decodes an envelope as data is a failure (`follower:envelope-misread`).
  - An erasure belongs to every group its customer overlaps (F3, `InG`). `MCGrp0Span` puts customer A over both groups.
  - The checkpoint lease expires after `CkLeaseLife` (F4). `CkTake` makes SlateDB's checkpoint, and `CkCheck` keeps it only if the lease is still held; otherwise it deletes it and starts again.
  - `Rollback` from E1 to E0 and `RollForward` (F5): E0 writes envelopes above E_env once the marker is set.
  - For one hold life after a takeover, the owner's drops use `RemoteLease` (N1).
- Tick: every clock ages by one, saturating at its threshold (`Tpin` 1, `Lease` 2, hold life 3, `RemoteLease` 4, `Grace` 4).

The read rule (every reader, the checkpoint read, the fork read): for key k in group g, the newest row among SlateDB rows with X_d < seq <= S, delta rows of g with X_g < seq <= X_d, and g's sealed run; a delete envelope or an erasure covering k means "absent".

Invariants:
- `NoBadRead`: no hold or follower step ever returns a value other than the truth at its S, or touches a deleted run.
- `LatestReadOK`: the newest version with SlateDB answers every key as of the last write.
- `CkptReadOK`: the checkpoint read through its pair (export, PITR diff) equals the database at `last_l0_seq`, and the pair's X_d <= `last_l0_seq`.
- `ForkReadOK`: the fork reads as the parent at the checkpoint plus its own write.
- `EncodingOK`: every stored row decodes with its database's E_env (envelope if and only if seq > E_env).
- `XgOK`: X_g <= X_d, and the X_g of every key's group never goes down (I2).
- `ErasedGone`: a sealed run never holds a put covered by an erasure at or below its X_g.

## Negative controls (`Ctl`)

Each one is a single change, and each must make TLC report a violation:
- `DropBeforePublish`: the drop uses the cut of the absorption in flight (in memory), before the publish lands.
- `GcNoGrace`: GC with grace 0.
- `ReadNoCut`: the read rule without the cuts (the newest row of any tier wins).
- `ResealSkip`: a reseal leaves out one delta run.
- `HoldsIgnored`: the owner's drop floor ignores live holds.
- `TxnRereads`: the transaction reads with the newest version at each step (a snapshot reading a newer version).
- `LeaseFromStamp`: the lease counts from the stamp written in the version before the PUT, not from its first sight (slow publish).
- `FollowerCheckOnce`: the follower checks T_pin once per operation, not before every step.
- `RevokeNoCheck`: a revoked hold keeps reading (no check after each step).
- `DropRemoteShortLease`: a drop job outside the hold owner's process with the ordinary lease (shorter than the hold life).
- `ForkCopyEenv`: the fork copies the parent's E_env.
- `CkptNoLease`: the control layer pairs a checkpoint without the drop lease in the store.
- `MergeConcat`: a group merge by concatenation with the smaller X_g (X_g lowered).
- `DeltaDropEnv`: a delta merge drops envelopes above X_g (SlateDB's last-run rule).
- Two more from review round 1: `FollowerNoTpin` (a follower reading past T_pin) and `ForkNewestCut` (a fork paired with the parent's newest cut).
- One found by this model: `MergeAtMaxCut` (a group merge at max(X_gA, X_gB) instead of X_d).
- Fix round (FIX 5), each one the delivered rule without its remedy:
  - `NoFence` (F1; configurations `NoFence-crash` and `NoFence-timeout`).
  - `FollowerOldEenv` (F2, the delivered rules), plus its two halves, `E1NoWait` and `FollowerNoMarker`.
  - `EraseOneGroup` (F3).
  - `CkNoRecheck` (F4).
  - `RollbackPlain` (F5, rule 21 as delivered).
  - `ZombieShortLease` (N1).

## What is not modelled

- SlateDB's own version collapse in the sorted run (the model keeps every row above the floor, which is what SlateDB's snapshot retention does for snapshots; the compaction filter is the danger and it is modelled).
- SlateDB's GC of dropped SST objects: reads see SlateDB's live state, which is the worst case; `SST_GC_MIN_UNREF_SECS` >= the hold life is review round 2's rule for the code.
- Splits (a split gives both halves the parent's X_g; nothing can go down).
- Run contents at block level, checksums, multipart: step 1 and its property tests.
- Two concurrent publishers: after a crash a new compactor waits for the PUT in flight (M28 checked the conditional create with two writers). A cut-off old waiter's compactor is assumed to stop at its first fencing error (rule 28).
- The fence is one atomic step (holds and publishes wait for its outcome); clock skew between processes (one shared clock).
- In the main shape every customer sits inside one group (with symmetry). The spanning layout is checked in `EraseOneGroup`, `EraseOneGroup-ok` and `Span-ok`.
- Liveness (that drops and erasures eventually happen): safety only.
