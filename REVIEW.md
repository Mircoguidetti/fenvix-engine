APPROVED: fence-layout 670321c (verdict file a1a0cdc). No failing test. 7 notes, none blocking; notes 1, 3 and 4 matter for the waiter sizing and the cost model at 1 TB.

# Review of fence-layout (idea A, step 1)

Reviewer session, 2026-11-06. Delivery: fenvix-engine branch `fence-layout`, commit 670321c (code and results) and a1a0cdc (verdict file), base `compaction` a7890b3. Contract: `prompts/P-fence-layout-local.md` ("Done means"). Owner's report: `sessions/2026-11-06-fence-layout-local/REPORT.md`. Spend: $0, local only.

Branch note: this harness only lets me push to `claude/r-review-fence-layout-agrpwy`, so this review sits there (based on 670321c), not on `review-fence-layout`. It holds this file and my judge output only. No test was added, because no defect became a failing test.

## What I did

- Read the contract, the report and the whole diff against a7890b3: `src/fence.rs`, `src/bin/fence_layout.rs`, `bench/fence/dump.py`, `server_reads.py`, the `cold_customer.py` refactor, the new test in `custkey.rs`, and the result files.
- Re-ran `judge/run.sh fast` on 670321c.
- Mutation test of the reader. I broke `Reader::newest` and its helpers in 7 ways, one at a time, and ran the owner's `fence::` unit tests each time: stop after the first fetch, ignore a run that starts with the customer, read blocks oldest-first, drop the upper bound (next customer leaks in), drop the lower bound (previous customer leaks in), skip the first block, fetch only the last missing block. The tests caught all 7 (MEASURED).
- Checked the cold-read rows of `fence-t1.json` one by one.

## The judge (MEASURED, my run on 670321c)

- build: PASS.
- unit: FAIL, 114 passed and 1 failed: `counter::tests::rev_maps_leave_room_for_active_data`, the known waiter defect, also failing on `compaction`.
- diff-py and diff-node: FAIL, 412/413 each. The only difference is `bucketauto.infinity`, the same as on `compaction`.
- ctl: PASS (3 files). `test_05_erase` passed 19/19.
- kill: PASS.

Point 6 of the prompt: `test_05_erase` is flaky, not caused by this branch. It failed once in the owner's run, then passed 3 times in the owner's re-runs and once in mine. The branch changes no server code path: the only change under `mongo-server/` is one unit test inside `#[cfg(test)]`.

## Answers to the six questions

1. Trips. Yes, a trip is counted the same way on both sides. The prototype goes through the server's own simulated store (`simstore::wrap_sim`), and both sides use the same `TripMeter`: a trip is a wave of GETs, counted when GETs in flight go from 0 to 1. On all 400 cold rows, the prototype's own count matches the meter (MEASURED, 0 mismatches). The 1 MiB cache really makes data cold. A decoded block is just over 1 MiB, so the cache holds one block at a time, and none of the 400 cold reads hit the cache: 397 reads took 1 GET and 3 took 2 GETs (MEASURED). Long histories are still one trip, because a user's newest messages sit at the end of their range: the 200 users drawn by activity, the whales included, scored 99.0% (MEASURED). Histories cannot be scattered inside one sealed run. But in idea A the newest messages of an active user are often the young ones, still in the log and not yet in the run (note 6). A query sorted on a field that does not follow `_id` is not one trip, and the report's "most likely a second trip" is optimistic (note 2).

2. Map size. Yes, every byte the reader needs at serve time is counted. That covers the fence separators and their ends, the offsets, the run directory (path and first key per run) and the struct sizes. Only the run's last key is dropped, and the reader never uses it. Paths are counted by length, not capacity, which is a few bytes. 0.0385 MB per GB of BSON is right (MEASURED by the owner, recomputed from `fence-t1.json`: 386,692 bytes for 10.05 GiB). The run held one collection and no index entries or `_id` locator. A real run would hold those too, so the per-stored-GB figure, 0.0758 MB per GB, is the better one for sizing. It is still 6.6 times under the CONTINUE line.

3. Answers. Yes. The model in `dump.py` comes from the generator: the 20 newest by global order `n` (time order), not by `_id`. So the check is independent of the key order and really confirms that `_id` follows time in this dataset. `check()` compares the count, then each `_id` and the sha256 of each document's bytes, in order. A missing, extra, wrong or out-of-order message fails. The 400 measured users all have at least 20 messages, so the 10 GB run never checked absent users or users with fewer than 20. The unit tests cover both: users with 0, 1, 3 and 19 messages, users across 2 KiB blocks, and a customer that starts a run. The mutation test shows those tests would catch the likely bugs. No user straddles a run, by construction: buckets are cut at customer boundaries, and dump partitions are user ranges. That limit is note 3.

4. RAM per hot user. Confirmed: 392 MiB for 400 hot users with one block per read, 0.98 MiB per user. With two blocks per read it is 558 MiB, 1.4 MiB per user (MEASURED by the owner, recomputed). For an 8 GB waiter, if about 6 GB go to the block cache, that is about 6,000 hot users with one block per read, or about 4,300 with two (ESTIMATED). With 64 KiB blocks, a hot user holds 1-2 decoded blocks, 64-128 KiB, so the same 6 GB keeps about 50,000-100,000 hot users (ESTIMATED). That is 8 to 16 times fewer hot users with 1 MiB blocks. The fix the owner names, caching only the user's slice of a block, would bring this back to about the user's own bytes. It is not built. See also note 1.

5. Rewrite cost. It is only a merge of pre-sorted inputs, not a rewrite under writes. The "young" records are copies of existing messages with the top bit of the ObjectId time set, and they are appended after each customer's old ones. There are no updates, no deletes, no keys that interleave with old ones, no log to sort, no index entries to regenerate, and no reads or writes running during the rewrite. The old run is deleted right away, so a reader still holding the old fence would get a 404. A real rewrite needs a manifest swap and a delayed delete, like today's garbage collection. The request counts (32 GETs, 11 PUTs per GB of BSON) are believable for a sequential copy: the merge comparisons are the same work with updates. The CPU of 6.3 s per GB is a floor (MEASURED floor; the real figure is UNKNOWN until updates and indexes are in). The bigger missing number is how often rewrites happen (note 4).

6. Judge: see above. Flaky, not this branch.

## Notes (none can be a failing test against this contract; none blocks)

1. Cold-read bytes and network ceiling. A cold read fetched 487 KB on average (MEASURED) to return about 9.4 KB, 20 messages of about 470 B (ESTIMATED): about 50 times more bytes than the answer. Requests cost the same, but the waiter's network does not. At 10,000 cold reads per second, that is about 4.9 GB/s into one waiter, more than a 10-25 Gbit/s NIC carries (1.25-3.1 GB/s, DECLARED by AWS for the instance class). So one waiter tops out at about 2,500-6,000 cold reads per second (ESTIMATED), against roughly 15 times more with 64 KiB blocks. Block size is a knob. With 256 KiB blocks the maps would be about 0.15 MB per GB (ESTIMATED, 4 times the blocks), still under the 0.5 line, with 4 times fewer bytes per read and per hot user. Suggest measuring 256 KiB in step 2.

2. Queries not ordered by `_id`. The run holds no index entries, so today such a query on this layout is a scan of the user's whole range. For the busiest customer here, about 1 GB, that is about 1,000 GETs (ESTIMATED). With a secondary index in the run, the count is one trip for the index part, then up to 20 documents spread over up to 20 different 1 MiB blocks of a long history. That is 2 trips but up to 21 GETs and about 10 MB fetched for 9.4 KB (ESTIMATED). So the trip count holds only for this one query shape, and the bytes get much worse.

3. A single customer is never split, so a run has no upper size. Here one run is 1.5 GiB because of one customer (MEASURED). With zipf s = 1 over 20,000 customers, the top customer holds 1/H(20000), about 9.5% of the data (ESTIMATED). In a 1 TB database of the same shape, that is a run of about 95 GB that must be rewritten whole, about 13 minutes per rewrite at 0.937 Gbit/s (ESTIMATED). The reader also assumes one run per customer (`run_for` returns a single run). Splitting a whale across runs needs a reader change. This works for small databases only until a customer can span runs, and the CLAUDE.md principle asks to say so.

4. Rewrite frequency, the missing line of the cost model. If a 1 GB bucket is rewritten each time its young records reach 10%, each byte written is rewritten about 10 times (ESTIMATED). Per GB of new data that is about 320 GETs, 110 PUTs, 63 s of one core and about 10 GB moved (ESTIMATED from the owner's per-rewrite figures). Request costs stay tiny, but CPU and network should go into the cost model per GB written, not per rewrite.

5. No Bloom filters means a lookup of an absent key costs one GET of about 470 KB instead of none (ESTIMATED from the block size). The owner found that every insert into a customer-keyed collection does a uniqueness point read on the `_id` locator. If that read reaches the sealed run, 10,000 inserts per second means 10,000 GETs per second (about $0.004/s, $350/day at the DECLARED $0.0004 per 1,000 GETs) plus about 4.7 GB/s of traffic (ESTIMATED). Possible ways out: answer uniqueness from the log and the run's last key, since new ObjectIds sort after all old ones, or keep a filter only for the locator. The design should say which one before step 2.

6. Young records. In idea A, the newest messages of an active user are written after the last rewrite, so they sit in the log, not in the run. The 99.25% is for the sealed part. A real read merges log and run. If the log is local this adds no S3 trip, but that is not measured. The prompt left this out of step 1. I note it so the headline is not read as end-to-end.

7. Small things, no effect on the numbers. `rewrite` does not update `MANIFEST.json`, so after it runs, `measure` points at a deleted run. The owner ran `rewrite` last, so the results are fine, but the order matters if anyone re-runs. The baseline's 2 GiB metadata cache and the 1 MiB block cache are a fair "maps in RAM" comparison, and the report says so.

## Bottom line

The headline numbers hold. Maps are 0.0385 MB per GB (0.0758 per stored GB), one trip serves 99.25% of cold reads, and there were 0 wrong answers. The tests measure what they claim, and the judge matches `compaction` apart from one flaky ctl run. CONTINUE is justified for step 1. Before step 2, the plan should cover 1 MiB blocks against network and RAM per hot user (note 1 and answer 4), whale customers (note 3), rewrite frequency (note 4) and absent-key lookups (note 5).
