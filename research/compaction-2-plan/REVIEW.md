FIX 7

# Review: compaction 2 plan (way (b)), before any build

Reviewer session 2026-11-07, prompt fenvix-brain `prompts/R-review-compaction-2-plan.md`. Reviewed: fenvix-brain `sessions/2026-11-07-compaction-2-plan/REPORT.md`, `plan_cost.py`, `plan_cost.txt`; fenvix-engine branch `compaction-2-plan` at 4c16333 (probe 1). Spend: $0, local only, no AWS, nothing merged.

What I added, on fenvix-engine branch `review-compaction-2-plan` (from `compaction-2-plan`), folder `research/compaction-2-plan/`:
- `probe/src/bin/probe2.rs` with its result in `results/probe-2.txt`. It uses the same SlateDB 0.17.0 public API and in-memory store as probe 1, but keeps a reader or a transaction alive while the steps run. Run it with `cargo run --release --bin probe2` (about 5 minutes).
- `review/review_cost.py` with `review/review_cost.txt`. It imports the plan's `plan_cost.py` and does not change it. It computes the reseal periods, today's tiered amplification and the deletion trigger.
- `review/delta_sim.py` with `review/delta_sim.txt`. It models the delta tier to count the GETs of a cold read, depending on how often the customer writes.

I re-ran probe 1 and it still passes (MEASURED). Probe 1 is correct, but every step in it runs after the previous step has finished. In the server, readers, transactions and DbReader followers stay alive while a step runs. Probe 2 tests that, and the plan's read rule breaks in 4 of its sequences.

The founder's choice of way (b) stands. Nothing found here breaks it: every fix below works with SlateDB's public API. The deletion windows stand too. Fix 5 only applies them as the decision wrote them ("a deleted customer's rows").

## The 7 changes the plan needs before step 1

1. **A pin and drop protocol.** Today the drop runs as soon as the publish succeeds (section 6, steps 2 then 3), and the read rule has no upper bound on the delta and sealed rows. Both are wrong.
   - A reader can still hold the old manifest version when the drop runs. Its rows are then gone from SlateDB and from our tiers.
   - A transaction snapshot loses its rows in the same way. SlateDB documents that compaction filters break snapshots.
   - A transaction that re-reads the newest manifest sees writes made after it started.

   All three are MEASURED in probe 2, points A, B and C. The plan needs these rules:
   - (a) A read takes our manifest version and its SlateDB view as one pair.
   - (b) SlateDB may drop rows at or below X only when no live pin, in any process, is older than the version that published X. Across processes this is enforced by a pin lifetime: a reader re-pins at least every T_pin, and the drop waits T_pin plus a margin. Inside the writer, the open snapshots set the limit.
   - (c) A transaction pins the version at begin and caps every tier at its S.
   - (d) A DbReader follower refreshes our manifest before it refreshes SlateDB's.
   - (e) The drop filter reads X from the published manifest, never from memory.

   Step 0's TLA+ gets the negative controls "drop while an older version is pinned" and "a snapshot reads a newer version".
2. **Fold every drop into one sorted run.** As written, every drop job creates a new, empty sorted run in SlateDB's manifest: 30 rounds gave 30 more (MEASURED, D). At 1 TB medium that is about 1,200 per day, forever (ESTIMATED: 38 GB a day absorbed in 32 MiB units). The fix: the sources of the job are the absorbed L0 SSTs plus the newest sorted run, and the destination is that same run. The count then stays flat and the merged read still equals the model (MEASURED, D2).
3. **Pair restore points and forks with a cut, and list every direct SlateDB reader.**
   - The plan says every server read goes through `Tenant`. That is false. These read SlateDB directly:
     - PITR: the staging clone and a raw `DbReader` diff (`mongo-server/src/ctl/pitr.rs:344-346`, `:398`, `:402-440`)
     - export (`ctl/export.rs:58`, `:99`)
     - fork (`ctl/fork.rs:229`)
     - prefetch (`mongo-server/src/prefetch.rs:163`)
     - the warm spare (`src/warm.rs:414`)
   - After a drop, these see only rows above X_d. They also see envelope values as data. So export would ship deleted documents, and a restore diff would delete keys that existed at the target time.
   - A clone continues from its checkpoint's sequence numbers, below the parent's: clone 101 against parent 200 (MEASURED, F). A fork paired with the parent's newest cut hides its own writes.
   - Rule: a checkpoint records the version it pairs with, with X_d ≤ the checkpoint's `last_l0_seq`. It is taken under the drop lock, so no drop above that X_d happens before the checkpoint is written.
4. **Write down the invariants that keep envelopes and cuts valid.**
   - A delta merge never drops an envelope above its group's X_g.
   - X_g never goes down. A group merge with different X_g values needs a rewrite at a common cut. Concatenating the two runs and taking the smaller X_g brings deleted rows back (sequence below).
   - The absorbed L0 SSTs are always the oldest, with no gap.
   - Never turn on SlateDB TTL, merge operators, segments, or `WriteOptions.seqnum` (`slatedb config.rs:535-539`). The first two hide rows from the merged scan. The last two break the "one L0 list in sequence order" assumption.
   - The old binary cannot read envelope values, so the 7-day rollback in section 8 fails by construction. Envelope reading has to ship one release before envelope writing, as compaction-1 did for lz4.
5. **Narrow the deletion trigger and include the delta tier.**
   - Section 4 rewrites a group when any delete envelope in it is older than 24 hours. The decision speaks of a deleted customer's rows, meaning erasure.
   - With ordinary deletes spread over the database, every group is rewritten every day:
     - 1 TB medium: +27 to the amplification.
     - 10 TB medium: +267, about 119 MB/s per database without pause.
     - In dollars it stays small: about 6 $ and 59 $ a month (ESTIMATED, `review_cost.txt`).
   - A group rewrite does not remove the deleted bytes from the delta runs, which cover the whole keyspace. Neither does it remove them from SlateDB's WAL and L0 objects. So guarantee 6 is broken as written.
   - Fix: trigger on erasure. Rewrite only the 256 MB delta objects whose key range holds the customer. Bound the age of the WAL and L0 objects through SlateDB's GC.
6. **Correct the headline numbers. They only hold for small databases.**
   - **Reseal period.** It is θ × D / inserted-per-day (ESTIMATED):
     - 1 TB: medium 6.7 days, busy 1.3 days.
     - 10 TB: medium 67 days, busy 13 days.

     So at 10 TB, "one GET" applies only to customers idle for about two months.
   - **GETs per cold read** (ESTIMATED, `delta_sim.txt`):
     - A customer who wrote once: 1.4-2.0.
     - A daily writer: 3.4-4.9 at 1 TB medium, 5.8-7.8 at 10 TB medium.
     - An hourly writer: 6.9-8.7 at 1 TB medium, 9.3-11.3 at 10 TB medium.

     The plan says 2.5, at most 5.
   - **Amplification.** The fair baseline is today's tiered compaction (compaction-1, T = 4): about 10 at 1 TB and 12 at 10 TB, from the same log4 formula the plan uses for its delta. It is not the reference's 14 and 25.5. Idea A writes about 30% more bytes than today (13 against 10, 15 against 12). That is the price of the sealed tier. It does not save bytes.
7. **Fix the tests that would pass while the guarantee is broken** (section B below), and the done-means numbers (section C).

## A. Questions 1 and 2: the read rule and sequence numbers

The rule of section 2 is right for a reader that pins the newest version, does its whole read, and has no open snapshot (MEASURED: probe 1, and the control of A). It breaks in these sequences.

- **A: reader pinned across a drop (MEASURED).**
  1. The reader pins v0 (X = 0).
  2. The compactor absorbs 2 L0 SSTs and publishes v1 (X = 1000).
  3. SlateDB's drop job removes rows ≤ 1000.
  4. The reader, still on v0, reads SlateDB.

  Result: 0 of 1,000 keys, 1,000 missing. The same happens in the server whenever a find does an index scan, then fetches documents, and a drop lands in between. It also happens to a DbReader follower whose SlateDB manifest refreshes before ours.
- **B: transaction snapshot across a drop (MEASURED).** Every transaction holds a SlateDB snapshot (`dbs.rs:1156`, `lifecycle.rs:999-1002`). SlateDB reads a snapshot from the live state, filtered by sequence (`db_snapshot.rs`). Its retention protects old versions from SlateDB's own merging, but the compaction filter runs after retention (`compactor_executor.rs:408-432`). SlateDB warns about exactly this in `compaction_filter.rs:7-10`.

  Result: the snapshot with its pinned v0 sees 0 of 1,000 keys.
- **C: a snapshot read with a newer version (MEASURED).**
  1. A transaction starts at S = 1000.
  2. Key 0/* is rewritten, flushed, absorbed and published with X = 1100.
  3. The transaction reads with the new version.

  Result: 100 of 1,000 keys return values written after S. The rule caps only SlateDB rows at S, not delta or sealed rows. Capping them is not enough on its own, because absorption keeps only the newest row of each key. So the old version is gone: the pin is needed (fix 1c).
- **D: sorted runs pile up (MEASURED).** Covered in fix 2.
- **Group merge lowering X_g (by construction, no probe).**
  1. Group B is sealed at X_gB = 100. Key k was written at seq 50 and deleted at seq 70, so the sealed run drops both the value and the envelope.
  2. The delta still holds the value at seq 50, a dead row kept until the next delta merge.
  3. B is merged with group A (X_gA = 40) by concatenation, with X_g = 40.

  Result: the rule takes delta rows above 40, and k comes back. Covered in fix 4.
- **A delta merge that drops envelopes like SlateDB's last-run rule (`is_dest_last_run`) (by construction).** The sealed value comes back. Covered in fix 4.

Sequence numbers: are they monotonic per database?
- **Restart: yes.** On open, the oracle starts from the manifest's `last_l0_seq` (`slatedb db.rs:140-146`), and WAL replay raises it to the highest replayed seq (`wal_replay.rs:85-88`, `:156`). Probe 2 E (MEASURED): last durable seq 60, first after restart 61.
- **Writer change: yes, for durable writes.** The new writer replays the WAL, and its first durable seq was 62. A fenced writer's write that was accepted but never durable got the same seq as the new writer's (62 and 62, MEASURED, E detail). This is harmless for cuts: a memtable reaches L0 only once it is durable (`db.rs:524-533`, `flush.rs:284`), so it never reaches our runs.
- **Fork and PITR staging: no.** A clone copies the checkpoint's manifest core, including `last_l0_seq` (`manifest/mod.rs:710-715`, `:992-1034`). So the clone continues from the checkpoint's seq, and different rows get the same numbers as the parent's later rows (clone 101, parent 200, MEASURED, F). Cuts stay valid only with the pairing of fix 3.
- **PITR on the live database: yes.** The engine's restore writes the difference forward as new writes (`pitr.rs:330-350`), so the live database's sequence keeps growing. But the difference is computed with raw readers (fix 3).
- **User-chosen sequence numbers.** `WriteOptions.seqnum` exists in 0.17. The engine does not use it today. Keep it that way, or use it only on purpose (fix 4).

## B. Question 5: guarantee tests that would pass while the guarantee is broken

- **3, read your writes.** "Write then read" always hits the memtable, so it passes even if the lower tiers lose every row (A). Fix: also read keys written before the last seal, publish and drop, through the writer and a DbReader follower, while finds are running.
- **1 and 2, durability and atomicity.** Kill tests and forced publishes do not hold a reader or a transaction open across a drop. Fix: run concurrent multi-step finds and 60-second transactions across publish and drop, and check them against a model (A, B, C).
- **6, deletions.** If the marker is written and deleted quickly, both rows land in the same L0. Absorption keeps only the envelope, so the marker never reaches a delta or sealed run and the test passes trivially. Fix:
  - let the marker age into the sealed run and the bottom delta tier, then delete it;
  - scan the delta runs, the SlateDB WAL and L0 objects and the checkpoints, not only the sealed runs;
  - use ordinary deletes as well as erasure (fix 5).
- **8, one writer.** "Two compactors race, one wins" passes even if the loser has already set the drop cut in memory, as in the probe's shared `AtomicU64`. Fix: add a fail point between "cut set" and "publish refused"; the drop must read X from the published manifest. Also, "a late publish is harmless because compaction never changes content" is false once envelopes are dropped and versions collapsed.
- **10, restore and forks.** "A fork reads as its source" passes when no compaction runs during the fork. Fix: run the fork and the restore while publish and drop are looping, and run export and PITR diff against a model after a drop (F, fix 3).
- 4, 5, 7 and 9: no gap found.

## C. Question 6: build order and done-means numbers

- **Missing work.**
  - Step 0 needs fixes 1-4 in the spec: pins, snapshots, drop folding, checkpoint pairing, X_g monotonic, envelope rules.
  - Step 3 needs the direct-reader inventory of fix 3.
  - Between steps 2 and 8: an envelope-reading release before envelope writing, or section 8's rollback cannot pass.
  - The plan never mentions the CPU cap across all databases' compactions (open from L1). At 1 TB busy that is 766 group rewrites a day per database.
- **Too easy.**
  - Step 4's amplification "at most 14 on a 10 GB replay": 10 GB gives about 4 delta tiers and an amplification near 10 (ESTIMATED), so it passes without saying anything about 1 TB or 10 TB. Measure against the formula at the same ratio of D to inserted per day, and against today's tiered compaction on the same replay.
  - Step 5's 10 GB run with "last write 1 minute to 7 days" leaves out what matters at scale: the reseal period (6.7 days at 1 TB medium, 67 at 10 TB) and frequent writers. Compress time so that θ × D / inserted-per-day matches the 1 TB and 10 TB profiles, and report GETs by write frequency (once, weekly, daily, hourly). With that, "mean at most 3, p99 at most 6" likely fails for daily and hourly writers at 10 TB (`delta_sim.txt`, ESTIMATED). That result should be reported, not tuned away.
- **Likely impossible as written.** Step 9's cold p99 of at most 150 ms when one cold read is a wave of 5-11 parallel GETs: the wave's latency is the slowest GET, so its p99 is close to a single GET's p99.9. Plan hedged GETs, and give the target per write frequency.
- **Order.** Steps 3 → 4 → 5 → 9 in sequence is right. Step 6's staged publish should come after a decision on how delta merges are warmed. At 1 TB medium a new delta run is absorbed about every 73 s and the first tier merges every few minutes (ESTIMATED), and each one replaces cached delta blocks. The plan's 10-second two-phase publish per merge is too heavy, and the plan's "recent writes are cached, 0 GETs" assumption fades as tiers merge.

## D. Questions 3, 4 and 7: cost, trips, scale

- **The arithmetic of `plan_cost.py` is right** (I re-derived it):
  - delta tiers 7, 7, 9, 8 from ceil(log4(θD/2 / 8 MiB));
  - sealed amplification (1+θ)/θ;
  - 153 and 766 group rewrites a day.
- **The comparison is not fair**, for two reasons:
  - the reference's PUT line mixes in the write path (the plan says so, and I agree: compaction-only PUTs today are UNKNOWN);
  - the reference's amplification of 14 and 25.5 is not today's engine, which runs tiered T = 4, about 10 and 12.
- **My range for compaction** (PUT + GET + CPU, a month, ESTIMATED):
  - 1 TB medium: 2.6-4.4 $ with erasure-only deletion, up to about 8.5 $ with the trigger as written.
  - 10 TB medium: 3.1-5.2 $, up to about 62 $ as written.
  - Busy: 13-22 $ at 1 TB, 15-26 $ at 10 TB, plus up to about 6 $ and 59 $ as written.
- **The real money is cold-read GETs, not compaction.** At the plan's cost, one GET per cold read is 21 $ a month at medium and 84 $ at busy (ESTIMATED).
  - With 2-5 GETs: 42-105 $ at 1 TB medium, against the reference's 84 $ at 4 GETs.
  - With 6-11 GETs (daily and hourly writers at 10 TB): 126-231 $ at medium.

  All of this is before the first course takes off its share of about 70% (ESTIMATED).
- **Trips for a customer who wrote recently:**
  - One wave holds only if the indexes of every delta run sit in RAM. At 10 TB the delta averages about 1.28 TB at 64 KiB blocks, about 20 million index entries, roughly 1-2 GB of RAM before dead rows (ESTIMATED). That fits the plan's 3 GB, but tightly on an i8g.large.
  - So: realistic at 1 TB medium and busy for customers who write rarely; one trip but 5-9 GETs for daily and hourly writers; 6-11 GETs at 10 TB medium (ESTIMATED).
- **Scale, not said in the plan:**
  - The reseal period grows with D (fix 6).
  - The deletion trigger as written grows with D (fix 5).
  - At 10 TB the bottom delta tier holds runs of hundreds of GB. One tier merge is hours of work unless it is cut into independent key-range jobs. Erasure needs per-object rewrites (fix 5).
  - The empty sorted runs grow with time (fix 2).

  The rest of section 9 holds: per-database manifests, independent group jobs, and NIC-bound cold reads at 256 KiB blocks.

## E. Notes, not blocking

- Probe 1 left an empty sorted run behind. It is harmless, but it is the first sign of fix 2.
- The plan's "0 GETs for `_id` checks with ObjectIds" assumes each run's min and max key cover a narrow time range. That is true for delta runs, but not for sealed groups of the shared `loc/` keyspace once they are rewritten. To measure in step 5.
- `components/compaction.md` should get the SlateDB facts the plan proposes, plus "drops need pins (probe 2)".
