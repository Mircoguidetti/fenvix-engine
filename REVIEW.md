FIX 5

# Review: compaction 2 step 0, the TLA+ model (`c2-step0-tla`, 4f3d889)

Reviewer session 2026-11-07, prompt fenvix-brain `prompts/R-review-c2-step0.md`. Spend: $0, local only, no AWS, nothing merged. Contract: `prompts/P-c2-step0-tla.md`. Owner's report: `sessions/2026-11-07-c2-step0-tla/REPORT.md`.

Branch `review-c2-step0` (from 4f3d889). Everything I added is in `research/compaction-2/tla/review/`:
- `Compaction2R.tla`: a copy of the delivered spec with six review scenarios (constant `Rv`) and each one's remedy (`RvFix`). With `Rv = "none"` it is the delivered spec.
- `MCR.tla`, `gen_review.py`, `run_review.sh`, `trace.py` (prints a counterexample as what changed at each step).
- `output/`: every TLC output, with every trace. `_run.log` is the summary.
- Run: `TLA2TOOLS=/path/tla2tools.jar ./run_review.sh all` (about 5 minutes on 4 cores).

Tool: TLC 1.8.0, the same `tla2tools.jar` as the owner (DECLARED, tlaplus GitHub releases), Java 21, 4 workers.

## 1. In one line

The model is careful, its controls fail for the right reason, and the group-merge finding is real. But it leaves out five things the plan's code will do, and each one breaks a delivered rule in TLC (MEASURED): a restarted waiter that does not know a version the dead one published; a follower that knows an old E_env; a whale customer spread over two groups; a checkpoint lease that expires; and a rollback from E1 to E0 under rule 21 as written. Every remedy is small, and each passes to completion in the same configuration (MEASURED). There is one more case, a NOTE: the old waiter's open transactions still reading after a takeover.

## 2. What I checked and what holds

- **The review copy is the delivered spec when `Rv = "none"`.** The owner's `DropBeforePublish-ok` configuration gives exactly the same count in my copy: 4,125,682 distinct states, complete, depth 31 (MEASURED, `output/none-DropBeforePublish-ok.txt`). So every failure below comes from the scenario I added, not from a change in the copy.
- **The owner's outputs.** I read all 17 control traces with `summarize.py`. Each one fails for the reason its name says. Some examples:
  - In `GcNoGrace`, the transaction pins the version just before the group merge lands, and GC removes the retired sealed run at once.
  - In `LeaseFromStamp`, the follower refreshes while the PUT is in flight, and the drop counts from the stamp.
  - In `ForkCopyEenv`, the checkpoint is at seq 0, so the fork's first write is an envelope that its copied E_env says is plain.

  None of them fails on an unrelated invariant.
- **The group merge at max(X_gA, X_gB).** I followed the 15-step trace by hand: it is right. A delta merge may keep only the newest erasure of a customer, and then the cut misses the older one. The rewrite at X_d fixes it.
- **A reader across two drops is reached, and holds.** The owner asked TLC for no such witness, so I added one, `WitnessTwoDrops`: a transaction still held after two drop jobs changed SlateDB, with a newer version published since its pin. TLC reaches it at depth 13 (MEASURED, `output/Cov-TwoDrops-witness.txt`): erasure, put, two absorptions, two drops while the transaction holds v2. In the same configuration, the correct model passes to completion: 142,804 distinct states, depth 19 (MEASURED, `output/Cov-TwoDrops.txt`).
- **A crash between publish and drop** is in the delivered main shape, and it is the core of the first defect below.
- **A fork during a fold** cannot go wrong in this model. The fork reads its own copy of the checkpoint rows and the paired runs, which GC keeps. It is not a gap in the model, but see note N4 on the fork's own GC.
- **The judge.** `judge/run.sh fast` on 4f3d889: see section 5. The diff against `review-compaction-2-plan-r2` touches only `research/compaction-2/tla/` and the owner's verdict file: 84 files, no engine code (MEASURED, `git diff --stat`).

## 3. The five defects (each a configuration that FAILS with the delivered rules, `Ctl = "none"`)

### F1. A restarted waiter pins a version older than the store's newest: a committed write disappears from a read (`MCR-StaleOwner.cfg`)

What the model leaves out: `RBegin` pins `Len(ver)`, the newest version in the store, without cost. Real holds pin the version the waiter's process knows, in memory (plan section 6: "pins the manifest version in memory"). The spec itself says a PUT in flight can land after the compactor crashed. When it does, the restarted waiter does not know the new version. It learns of it only when its own conditional create fails or it reloads.

TLC's trace (MEASURED, `NoBadRead` violated at depth 13, 28,615 states):
1. put a1 at seq 1, flush, absorb; the PUT of v2 starts.
2. The waiter crashes and restarts. It reads the store and knows v1.
3. The dead process's PUT lands: v2, with X_d = 1.
4. The drop job sees v2, two ticks pass, and the drop removes the put from SlateDB. This is correct by rule 7 and rule 8.
5. A find begins on v1, the version the waiter knows. v1 has no delta run, and SlateDB no longer has the row, so the find reads a1 as absent. The truth is 1.

The same gap also breaks the remote time-only floor (rule 10) and GC grace (rule 23). Both assume that no hold can pin a version older than one already seen in the store.

Remedy (PASS, complete, 692,606 distinct states, depth 23, MEASURED, `output/StaleOwner-fix.txt`): at open, before it serves any read, the waiter publishes a fence version (a copy of the newest, written with the same conditional create). A late PUT of the dead process then targets a slot that is taken, and it fails as an orphan. Or the waiter's own create fails, and it reloads. Either way, the waiter knows the newest version. The cost is one small PUT per open (ESTIMATED: about 64 KB at 10 TB, the plan's manifest size). Missing rule for the code: "a hold pins the waiter's version, and the waiter publishes a fence version at open".

### F2. A follower decodes with an old E_env: an envelope is read as data (`MCR-FollowerEenv.cfg`)

What the model leaves out: the reads never decode. `EncodingOK` checks only that each stored row's format matches its database's E_env. It never checks the E_env that a reader uses. The step 2 report says step 3 will carry E_env in the manifest and give it to followers through the T_pin refresh. I modelled exactly that best case: the follower takes E_env with each refresh.

TLC's trace (MEASURED, `NoBadRead` violated at depth 7): the follower refreshes in release E0 (E_env not set). Release E1 starts, an erasure is written as an envelope and flushed, and the follower's next step, still inside T_pin, decodes that envelope as a plain value. A refresh every T_pin bounds how old the version is. It does not stop a follower from seeing a row written after its refresh in a new format.

Remedy (in words, not modelled): a follower must know E_env before any row above it can reach its view. For example, E1 is first announced in a version, and envelope writing starts only after that version is older than T_pin. A follower that has seen the announcement reads the marker (or E_env from its version) before each step. Today step 2 keeps E1 off, so nothing is broken in code yet. But the rule is missing from `RULES-FOR-THE-CODE.md`, and step 3 must have it before E1 is turned on.

### F3. A customer whose keys sit in two groups: the erasure misses one group, and the erased data comes back (`MCR-SpanGroups.cfg`)

What the model leaves out: `GroupOf` gives an erasure (a range tombstone over a customer) one group only: the group of one chosen key. In `MC.tla` every customer sits inside one group, so this never matters. But the plan says a whale (about 95 GB at 1 TB, against groups of about 1 GB) spans many groups. A group split by key range can also cut a small customer in two.

TLC's trace (MEASURED, `LatestReadOK` violated at depth 9; keys a1 in g1 and a2 in g2, both customer A): put a2, erase customer A, flush, absorb, land. The read of a2 in g2 takes only g2's delta rows, and the erasure counts for g1 only. So a2 reads as present right after the erasure, from the newest version.

Remedy (PASS, complete, 1,761,478 distinct states, depth 22, MEASURED, `output/SpanGroups-fix.txt`), in three parts:
- An erasure belongs to every group its range overlaps, for reads and for reseals.
- A delta merge keeps it while it is above the X_g of any of those groups.
- Rules 13, 14 and 22 should say so.

### F4. A checkpoint lease that expires mid-checkpoint: the pair loses rows (`MCR-CkLeaseExpiry.cfg`)

What the model leaves out: rule 18's lease in the store never expires in the model. A real lease must expire, or a control layer that dies mid-checkpoint blocks every drop forever, and the L0 SSTs pile up.

TLC's trace (MEASURED, `CkptReadOK` violated at depth 15, lease life 3 ticks):
1. The checkpoint pairs with v1 and takes the lease.
2. v2 lands, and the checkpoint is slow.
3. The lease expires. The drop job sees v2 for one lease and drops the put.
4. SlateDB's checkpoint is taken, without the rows of (X_v1, X_v2].

Remedy (PASS, complete, 208,661 distinct states, depth 23, MEASURED, `output/CkLeaseExpiry-fix.txt`): after SlateDB's checkpoint exists, check that the lease is still held. If it expired, delete that checkpoint and start again. Rule 18 should add this check and the lease's life.

### F5. Rule 21 as written breaks a rollback from E1 to E0 (`MCR-Rollback.cfg`)

What the model leaves out: the plan's 7-day rollback goes from E1 back to E0 (plan round 2, I6; DECISIONS 2026-11-07). The model has no rollback action, and `RULES-FOR-THE-CODE.md` rule 21 says "Release E0 reads envelopes above E_env and writes none". Followed literally after a rollback, E0 writes plain rows above E_env.

TLC's trace (MEASURED, `EncodingOK` violated at depth 5): E1 is on, rollback to E0, a put at seq 1 is stored plain above E_env. With step 2's actual rule, the rollback model passes (complete, 52,979 distinct states, depth 20, MEASURED, `output/Rollback-fix.txt`). That rule is: E0 writes envelopes in any database that has the marker, and never writes the marker.

Step 2's code already does the right thing (its `test_45`). The defect is in the rules document that steps 1-8 will follow. Rule 21 should be replaced by step 2's rule, and the model should get the rollback action.

## 4. Notes (they do not block)

- **N1. The old waiter's open transactions after a takeover (`MCR-ZombieHolds.cfg`: fails, not counted).** The model's crash ends every hold. In a takeover where the old waiter is slow or cut off, not dead, its open transactions may keep reading. The new waiter's drop then uses the ordinary lease (60 s), which is shorter than the hold life (120 s). TLC breaks `NoBadRead` at depth 13 (MEASURED).
  - Whether the real system breaks depends on whether a fenced-out SlateDB 0.17 writer's open snapshot picks up the new owner's compaction, or keeps its old SSTs, which `SST_GC_MIN_UNREF_SECS` keeps for 120 s (UNKNOWN). So I do not count it.
  - Cheap remedy, PASS (complete, 284,950 distinct states, MEASURED): after a takeover, the new waiter's drops use the remote lease (longer than the hold life) until one hold life has passed. Step 4 should test the takeover case with an open transaction.
- **N2. The full shape is checked completely only up to 10 steps** (depth 12, owner's main run). Every interesting violation in the owner's controls and in mine is 6 to 21 steps deep. Each deep case is covered only in a reduced configuration. That is acceptable for a paper check, but "114 million states" should not be read as "every interleaving of all parts together". The 10-million-state target is met (MEASURED).
- **N3. `ErasedGone` checks only the newest version's sealed runs.** It does not check the delta runs, SlateDB's sorted run, or the 24-hour deadline, which is liveness and is not modelled. The erasure test of step 7 must cover those.
- **N4. A fork's own GC is not modelled** (the fork does no compaction). A fork shares the parent's runs by reference. Its GC must never delete an object outside its own prefix, and the parent's GC must count the fork's references, as the model does on the parent side.
- **N5. Reads are atomic steps in the model.** The real "check after each step" must happen before any row of that step is returned to the client. Rule 5 should say "before returning".
- **N6. Rule 17, "a lost PUT leaves an orphan run that GC removes".** GC must decide what is an orphan from the store (runs not listed by any version), never from the publisher's belief. A PUT that timed out may have landed (F1 is that case).

## 5. Judge

`judge/run.sh fast` on 4f3d889, the delivered commit (MEASURED, `judge/out/4f3d889/VERDICT-FAST.md`):
- build PASS.
- unit FAIL, 108 passed and 1 failed: `counter::tests::rev_maps_leave_room_for_active_data`, the known one.
- diff-py and diff-node FAIL, 412/413 each: the known `bucketauto` difference.
- ctl FAIL, 1 of 3 files: `test_05_erase::test_concurrent_erase_and_fork_are_serialised`. A usage file `control/usage/ef5_k/.../erase-*.json` was left behind. That is the pre-existing usage-file leak that the compaction-1 review and the step 2 report describe. The owner's run of the same engine passed it, so it is a race. It cannot come from this delivery, which changes no engine code.
- kill PASS. The kill phase rewrote the tracked `loadtest/results/gc/durability-s3-durable.json`; I restored it.

So I see no failure that this delivery added.

## 6. Corrections for fenvix-brain

- `components/compaction.md` and the step 3, 4 and 7 prompts need the five rules of section 3:
  - the fence version at open;
  - followers learn E_env before rows above it are visible;
  - an erasure belongs to every group it overlaps;
  - the checkpoint lease is checked after the checkpoint exists;
  - E0 writes envelopes in a database with the marker.
- The owner's correction stands: round 2's I2 should cut a group merge at X_d.
