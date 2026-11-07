FIX 1, 4 and 5: six rules are still missing (below, each with its remedy, measured or derived). They are small and fit into build steps 0, 3, 4 and 7, so steps 0, 1 and 2 can start now. There is no third round, so these become notes for the build steps.

# Review round 2: compaction 2 plan, the 7 fixes

Reviewer session 2026-11-07, prompt fenvix-brain `prompts/R-review-compaction-2-plan-r2.md`. I checked only the 7 fixes of round 1, against the owner's answer `sessions/2026-11-07-compaction-2-plan-r2/REPORT.md` (fenvix-engine `compaction-2-plan-r2`, c2b62f7). Spend: $0, local only, nothing merged. The founder's choices of 2026-11-07 stand: nothing here breaks way (b) or the deletion windows.

What I added, on fenvix-engine branch `review-compaction-2-plan-r2` (from c2b62f7), folder `research/compaction-2-plan/`:
- `probe/src/bin/probe4.rs`, result `results/probe-4.txt`. It uses the same SlateDB 0.17.0 public API, in-memory store and stand-in manifest as probe 3, and tries to break the protocol in the cases the prompt names, plus a few found while reading. Run: `cargo run --release --bin probe4`, about 5 minutes; `P4_ONLY=FOLD` runs one point.
- `results/probe-3-review-rerun.txt`: my re-run of probe 3. The owner's `probe-3.txt` is unchanged.
- `review-r2/review_r2.py`, result `review-r2/review_r2.txt`. It imports the plan's `plan_cost.py` unchanged and computes two numbers that round 2 leaves out. Run: `python3 -I review_r2.py <fenvix-brain checkout>`.

Time in probe 4 is scaled: a 6 s lease stands for the plan's 60 s (T_pin 30 s plus a 30 s margin), so 6 s of transaction stands for 60 s and 12 s of pin life for 120 s. One fact from the probe matters for the build. SlateDB 0.17 builds a job's compaction filter, and so computes our floor, when its worker picks the job up, not when the job is proposed. With the default worker poll that came up to about 5 s later (MEASURED). The floor must therefore be computed inside the filter, as probe 3 does. It must never be computed by the scheduler.

## Fix 1. Pin and drop protocol: mostly right, with four holes

Probe 3 re-run (MEASURED, `probe-3-review-rerun.txt`): every point PASS. The controls still reproduce probe 2's failures: pins ignored gives the reader and the snapshot 1,000 of 1,000 keys missing; trusting the cut in memory gives 100 of 200 missing; a follower that stopped refreshing would miss 100.

What I tried to break, probe 4 (MEASURED):
- **A drop job started before the lease ends: holds.** Jobs whose filter was built 0, 3 and 4.5 s after the publish kept the floor at 0 and all 1,000 rows. A reader that knew only v0 missed 0. Jobs built at 6.1 s or later raised the floor to X.
- **A transaction at the 60 s limit: holds.** A transaction was held 8.3 s, past the 6 s lease, while 6 versions were published and dropped. The floor stayed at its X, and 0 of 1,000 keys differed from the state at S. Past the pin life its read failed, then the drop applied (0 rows left) and a new read equalled the model.
- **Hole 1, a slow publish (BREAKS).** The lease counts from the time stamped inside the version, and the stamp is written before the PUT. Suppose the PUT takes longer than the margin: S3 retries, 503s, a stalled connection, or a client timeout after which the PUT still completes.
  - A follower refreshes just before the version becomes visible and reads inside T_pin.
  - The drop counts the lease from the stamp and has already removed the rows.
  - In the probe (PUT 7 s, margin 1 s, T_pin 6 s) the follower missed 1,000 of 1,000 keys.

  Remedy, PASS in the probe: count the lease from the first time a floor computation saw the version in the store, using the drop job's own clock. The follower then missed 0, and the drop still happened one lease later (0 rows left). This also removes any clock skew between the publisher and the drop job. In production the condition is "PUT time < margin". With a 30 s margin it is usually true, but S3 cannot promise it.
- **Hole 2, the follower check once per operation (BREAKS).** An operation that passes the T_pin check at its start and then runs longer than the margin can open or refresh a SlateDB view after the drop. Examples: a find with many fetches, an export page, a DbReader FollowLatest poll in between. Step 1 missed 0 of 500 keys, step 2 missed 500 of 500. Remedy, PASS: check the age of the last refresh before every step that opens or refreshes a SlateDB view. Step 2 was then refused and nothing wrong was returned.
- **Hole 3, a pin revoked mid-read (BREAKS).** Revocation at 120 s removes the pin from the floor, so a drop can follow at once. If the reader does not look again, its next step returns wrong rows: 500 of 500 in the probe. Remedy, PASS: the reader checks its pin after every step and fails with TransientTransactionError. That alone is enough. Old SST files stay readable for `SST_GC_MIN_UNREF_SECS` (120 s in the engine, `src/lifecycle.rs:1958`), so this setting must stay at or above the pin life.
- **Hole 4, the drop job in another process (BREAKS).** The floor counts "every version pinned in this process". Fix 7 puts "absorption and drop" in a compaction pool per cell, and step 4 prepares remote workers. A drop job in the pool cannot see the waiter's transaction pins. A transaction aged 60 s (scaled) lost 1,000 of 1,000 keys. Remedy, one of these:
  - (a) Drop jobs run only in the process that holds the database's pins, the waiter. They are small: about 0.01 cores at medium and 0.14 at busy with a 60 s lease, 0.05 with a 10 s lease (ESTIMATED, `review_r2.txt`).
  - (b) The pool uses a time-only floor whose lease exceeds the pin life: 120 s plus a margin. PASS in the probe: 0 missing.
  - (c) The waiter publishes its oldest pin at least every T_pin.

  I recommend (a), with (b) as the rule whenever the drop runs elsewhere. Fix 7's pool then holds tier merges, reseals and erasures, not drops.

Also needed: today the drop lock that pairs checkpoints (P4) is an in-process mutex. Restore points and export checkpoints are taken by the control layer (`ctl/pitr.rs:143-146`, `ctl/export.rs:97-99`). They must run in the waiter's process, or the lock must become a lease in the store. Step 0 should include it.

## Fix 2. Drops fold into one sorted run: holds under pins, but the fold rewrites

Probe 4 FOLD (MEASURED): 100 absorb, publish and fold rounds, with 26 transaction pins taken and released at random (up to 3 at once).
- Sorted runs stayed at most 1.
- Every pinned read equalled the model at its time.
- At the end 0 rows at or below X were left, and the merged read equalled the model.
- The run without pins gave the same result.

What round 2 does not count: every fold rewrites the whole run, and the run holds every row still above the floor. A row is therefore rewritten by each fold for one lease after its publish, longer while a pin holds the floor. In the probe, with one fold every 1 s and a 6 s lease, each inserted row was rewritten 5.8 times (6.8 with pins), against 1 + lease / time between folds = 7.0 (MEASURED against the formula).

At production rates (ESTIMATED, `review_r2.txt`, 60 s lease, one fold per 32 MiB absorbed):
- 1 TB medium, one fold every 70 s: +1.9 to the amplification (13 becomes 15), +0.41 $ a month.
- 1 TB busy, one fold every 14 s: +5.3 (13 becomes 18), +5.79 $ a month, +0.14 cores. Databases per c8g.xlarge at 70% go from 9 to 6.
- 10 TB: the same additions (15 becomes 17 at medium, 20 at busy).
- If a 120 s pin were held all the time at busy: +13.8.

The money is small, but it changes fix 6's amplification and fix 7's done-means. Remedies for step 4: a shorter lease (T_pin 5 s plus a 5 s margin gives +1.7 at busy, ESTIMATED), or larger absorption units at busy. In either case, add the fold term to the formula.

## Fix 3. Direct SlateDB readers and checkpoint pairing: right, two readers missing

I checked the inventory by grepping c2b62f7 for `DbReader::builder`, `.snapshot()`, `SstReader::new`, checkpoints, clones and raw scans. Every file:line in round 2's list is real, and the transaction reads it adds (`dbs.rs:688-689`, `:1100-1112`, `:1200-1201`) were the important miss of round 1. Probe 3 F re-run gives the same results as the owner's run:
- export: 0 of 99 wrong (control 99);
- PITR diff: 100 changes, the model's 100 (control 0);
- fork: its first write at seq 102, above its paired X of 101.

Two readers are missing. Both only warm caches or report, so they are not a correctness hole:
- `src/maps.rs:85`, `:142` (T1's "maps on the counter"). It warms the index and map blocks (`idx/`, `meta/`, `nat/`) of every SST in the SlateDB manifest of every open writer and reader. Under way (b) these rows move into our runs, so the maps must warm our runs, or cold index reads lose their one trip.
- `src/tiered.rs:249-253` (`sst_info`, a diagnostic) and the tiered scheduler itself. After the commit of section 8, the fold scheduler replaces compaction-1's tiered scheduler; during the 7-day rollback window the tiered scheduler stays. The plan should say so.

The judge check that round 2 proposes ("fails if `DbReader::builder`, `.snapshot()`, `SstReader::new`... appear outside `fx` and `lifecycle.rs`") should also list `maps.rs`, `warm.rs` and `tiered.rs` by name, with a reason for each.

## Fix 4. Invariants: right, one hole in the envelope marker

I1, I2 (common cut, split), I3, I4 and I5 are right. Probe 3 G re-run: PASS.

The hole is in I6. E_env is a sequence number, and forks break sequence order (probe 2 and probe 3 F MEASURED: the clone continues at 102 while the parent is at 201). Take a fork or a PITR staging clone made from a restore point taken before the parent's E_env, after release E1 is out:
- it copies the parent's E_env;
- it writes envelopes at seq numbers at or below that E_env;
- the reader then takes them as plain values: a delete envelope becomes a 1-byte document, and every live value gets a stray 0 byte in front of its BSON.

That is data corruption, by construction. The window is the 30 days of restore points after E1. Rule: a clone sets its own E_env to min(the parent's E_env, the checkpoint's `last_l0_seq`). Step 0 should get it as a negative control.

## Fix 5. Deletion: erasure is right; ordinary deletes stay longer than written

The erasure path covers what round 1 asked: envelopes, the group reseal, the delta objects holding the customer (about 27 objects, about 7 GB at 10 TB), L0 through the drop plus `SST_GC_MIN_UNREF_SECS`, and WAL kept with the 30-day restore window. The cost, about 0.0015 $ per erasure, re-derives (8 GB at 0.000189 $ per GB). Two points remain.
- **Ordinary deletes are not gone at "the group's next reseal".** A reseal rewrites the sealed run, not the delta runs. A dead row leaves a delta run only when that run's tier merges (ESTIMATED, `review_r2.txt`, tiered T = 4, 32 MiB units). So a dead row can stay after its group's reseal for up to about:
  - 1 TB: 13 days at medium, 3 days at busy;
  - 10 TB: 214 days at medium, 43 days at busy.

  Fix 7's own done-means ("0 for the ordinary one after its group's next reseal") will therefore fail by design once the marker has aged into the bottom tier. The founder should see these numbers before confirming decision 3, because many apps erase a user with `deleteMany({userId})`, not through the control API. Proposed:
  - treat a deleteMany that covers one customer's whole key range in a customer-keyed collection as an erasure;
  - change the ordinary-delete target to "0 after the next merge of the tier holding it", with that time reported.
- **An erasure needs a range rule in the read path.** "Envelopes for the customer's key range (one range with layout v2)" is either one envelope per key, which means reading a whale of about 95 GB first, or a range tombstone. A range tombstone is not in the read rule, the probes or the TLA+ list. Pick one in step 0.

## Fix 6. The corrected numbers: they re-derive, with one term missing

I re-ran `r2_numbers.py` and its output is byte-identical to `r2_numbers.txt`. I re-derived by hand:
- the reseal periods: 0.25 × 1,024 / 38.3 = 6.7 days; 66.8 days at 10 TB;
- today's tiered amplification, 1 + ceil(log4(D / 8 MiB)): 10 at 1 TB, 12 at 10 TB;
- the plan's 1 + 7 + 5 = 13;
- R2 at 10 TB medium: θ_eff = 7 × 38.3 / 10,240 = 0.026, sealed 39.2, total 48;
- one GET per cold read: 20/s × 2.63 million s × 0.0004 $ / 1,000 = 21.0 $ a month;
- the "after" line: 0.09 × 1.0 × 21.02 = 1.89 $;
- the hedged p99: 130 ms for a wave of 4, 141 ms for a wave of 9.

The GET model's reading rule, "a run costs a GET when its filter says yes and its sequence range reaches above X_g", is the right one for the plan's reader. All of these are ESTIMATED from a model.

Corrections and caveats:
- The amplification lacks the fold term of fix 2: +1.9 at medium and +5.3 at busy with a 60 s lease. So 1 TB busy is about 18, not 13 (ESTIMATED).
- R1, the small tiers on the waiter's NVMe, must be written to S3 as well: S3 is the only durable copy once SlateDB drops the rows. Its "0 GETs" holds only for the waiter that wrote them. A spare or a new waiter after failover has to fetch them, about 8 GB, and the cost is small: about 150 GB a day at 1 TB medium as whole-run GETs, well under 1 $ a month (ESTIMATED). R1 also says the waiter writes these tiers, while fix 7's pool runs "tier merges". Pick one place: the waiter, which is also where hole 4 puts the drops.
- The "after the first course" GET cost (1.9-7.6 $) counts only the cold first accesses the first course does not cover. Queries after the first 4 of a visit are not in it. Round 2 calls the 70% + 21% share an upper bound, and I agree: read the after numbers as a floor.

## Fix 7. Tests and done-means: right, with four additions

The guarantee tests now fail when the guarantee breaks, for 1, 2, 3, 6, 8 and 10: probe 3's points become tests and their controls become negative tests. Hedged GETs and the per-pattern p99 targets are in the plan, and so is the CPU cap across databases (the pool, priorities, a 70% cap, and the done-means of 9 busy databases on 4 vCPU). Additions:
- **Step 0's negative controls:** add a slow publish (lease counted from the stamp), the follower check made once per operation, a revoked pin without a check after each step, a drop job outside the pin owner's process, a fork that copies the parent's E_env, and checkpoint pairing across processes.
- **Step 4's amplification done-means** ("within 15% of the formula's 13") must use the formula with the fold term. Otherwise the measured value fails by about +2 at medium and +5 at busy, or it gets tuned to fit.
- **Step 4's CPU done-means:** count the fold's rewrite. 9 busy databases per 4 vCPU becomes about 6 with a 60 s lease (ESTIMATED).
- **Step 7's ordinary-delete target:** as in fix 5.

## Notes, not blocking

- In code, "pin" already names the keep-awake pins of `ctl/pin.rs`. Call the manifest pins something else, for example "holds", to avoid a confusing review later.
- The `probe4.rs` timings depend on `CompactionWorkerOptions::compactions_poll_interval`. The engine's separate worker (`tiered::worker_builder`) has its own poll, so measure the delay between a job being proposed and its filter being built in step 4.

## Proposed corrections to fenvix-brain

- `components/compaction.md` should get round 2's points, plus these:
  - count the lease from the first time a version was seen, not from its stamp;
  - check the pin and the T_pin age after or before every step, not once per operation;
  - drops run where the pins are;
  - the fold rewrites rows that are still above the floor;
  - a clone's E_env is min(the parent's E_env, the checkpoint seq).
- `sessions/README.md` still asks for `RAPPORTO.md` in Italian (noted three times now).
