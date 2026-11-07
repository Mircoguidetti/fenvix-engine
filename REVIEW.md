FIX 1: a run's blocks are not bound to their run, so the blocks of one run under the footer of another are read as data (test `review_blocks_of_one_run_with_the_footer_of_another_are_an_error`). Everything else holds: 0 wrong reads in 1,000,000 plus 600,000 of mine, every flipped bit caught, 12 of 12 owner mutations and 11 of 14 of mine caught (the 3 missed only remove redundant checks), maps and CPU re-measured as reported.

# Review: compaction 2, step 1 (sealed-run format v1)

Reviewer session 2026-11-07, prompt fenvix-brain `prompts/R-review-c2-step1.md`. Spend: $0, local only, no AWS, nothing merged.
Delivery: fenvix-engine `c2-step1-runformat`, ee374b8 (from `compaction` a7890b3). Contract: `prompts/P-c2-step1-runformat.md`. Owner's report: `sessions/2026-11-07-c2-step1-runformat/REPORT.md`.
My branch: `review-c2-step1` (from ee374b8). What it adds:
- two tests at the end of `src/runfmt/tests.rs`;
- `research/c2-step1-runformat/review/mutations_review.py`, my mutation runner;
- `research/c2-step1-runformat/review/results/`, every output quoted here;
- `judge/out/ee374b8/VERDICT-FAST.md`, my judge run.

## The defect

**FIX 1. The blocks of one run under the footer of another are read as data.** Test: `review_blocks_of_one_run_with_the_footer_of_another_are_an_error`. It FAILS on ee374b8 (MEASURED, `results/review-tests.txt`).

What the test does:
- Run A: 400 rows. Run B: the same rows with one value changed in place, as in a reseal of the same group. B has the same block offsets and the same size as A (the test checks this first).
- The test builds an object from A's blocks followed by B's footer and trailer.
- Both readers return A's 400 rows under B's footer, with no error: the random reader and the stream.

Why it passes the checks today: the block CRC covers the block index and the block's bytes, and the footer CRC covers the footer, but nothing ties a block to the footer of its own run. So any block written at the same position in a run with the same layout passes as valid.

How it can happen:
- a path written twice, for example a retry, or a reseal that keeps the same name (the plan's names are `{X_g}-{writer epoch}`, so a rewrite at the same X_g and epoch keeps the name);
- a reader whose footer is older than the object now at its path;
- a later writer (step 4) that copies the byte ranges of unchanged blocks from the old run with UploadPartCopy and gets one range wrong.

In every case the reader returns stale rows with no error. This is the "footer from another run" case of the review prompt.

Remedy: put a random run id (16 bytes) in the footer and the trailer, and mix it into every block CRC (for example, as the CRC's starting bytes in front of the block index). It costs 16 bytes per run and no CPU. Step 3 should also carry the id in the manifest line and check it at open. Then a manifest that points to the wrong object of the same size is also an error. Today the reader trusts the (path, size) pair alone.

## The done-means, re-run on the delivered code

1. Property test, 1,000,000 reads: 0 wrong, 766,341,234 rows compared (MEASURED, `results/long-tests-rerun.txt`). This is byte for byte the owner's line.

   The model is an independent BTreeMap, and the test compares every row (key, seq, envelope), so it can fail. My own fuzz, `review_fuzz_odd_keys_match_an_exact_model`, gave 0 wrong in 600,000 reads (MEASURED, RFUZZ=2000). It covers what the owner's test leaves out:
   - keys with 0x00, 0xFF and '/' bytes, keys that are prefixes of other keys, and the empty key;
   - all three customer kinds (the owner's test uses only V2);
   - blocks of 1 to 1,000 bytes, so one row per block and restart groups cut anywhere;
   - 0 to 3 run cuts.

   I checked the customer filter against the real key builders (`layout.rs`, `custkey.rs`). The rule is a function of a prefix of the key, and every key in a one-customer range shares that prefix, so it cannot give a false "no" in either layout.
2. Flipped bits (MEASURED):
   - small run, exhaustive: 9,328 of 9,328 bits caught by both readers, and 3,016 of them at open;
   - 275 MB run: 2,000 of 2,000 flips caught, the neighbouring block read right 1,805 times, and 50 of 50 full scans were errors.

   The test opens a fresh reader for every bit and flips the bit in every GET, so it measures what it claims.
3. Mutations (MEASURED, `results/mutations-owner-rerun.txt`):
   - The owner's 12, re-applied one at a time on a clean copy: 12 of 12 caught. Number 6 is caught as a hang of more than 300 s, as reported.
   - My 14 (`results/mutations-review.txt`), run against the owner's tests only: 11 of 14 caught. Caught:
     - the block cache ignores the run;
     - the filter is used for ranges that cross customers;
     - the writer filters only the customers that start a block;
     - newest returns one row too many;
     - the separator is the previous key;
     - a point read returns the next row;
     - a point read hashes the wrong bytes;
     - a range loses its lower bound;
     - the front coding forgets a shared byte;
     - the group search starts one group late;
     - the stream drops a block.
   - Not caught, and none of them changes an answer:
     - R12, no fence check on decoded blocks;
     - R13, no count check at the end of the stream;
     - R14, the block CRC without the block index.

     These are defence in depth behind the CRC. No test reads a valid block from the wrong place, so they are untested. That is a NOTE. The remedy of FIX 1 comes with a test that also covers R14.
4. Maps: 0.2336 MB per stored GiB, 0.1197 MB per raw GiB, at 256 KiB blocks (MEASURED, `results/bench-1gib-256k-rerun.txt`). The same as the owner's.
5. Writer CPU: 3.30 s per raw GiB, 6.44 s per stored GiB, on an Intel Xeon @ 2.10 GHz, one thread (MEASURED). The judge was running at the same time; the owner measured 3.21 s. It is measured on the right thing: a current-thread runtime, `RUSAGE_THREAD`, and rows generated before the clock starts.

## Judge

`judge/run.sh fast` on ee374b8 (MEASURED, `judge/out/ee374b8/VERDICT-FAST.md`):
- build PASS;
- unit FAIL, 122 passed and 1 failed. The failure is `counter::tests::rev_maps_leave_room_for_active_data`, the known one;
- diff-py and diff-node FAIL, 412/413 each, the known `bucketauto.infinity`;
- ctl PASS;
- kill PASS.

This is the owner's verdict exactly, and the same known failures as `compaction`, none added. The kill phase rewrote `loadtest/results/gc/durability-s3-durable.json` in my judge worktree; I did not commit it.

## Notes (not blocking)

- **The maps target depends on the data, in two ways.**
  - The lz4 ratio, which the owner already reports.
  - Customers per GiB. The filter costs 1.25 bytes per customer. The bench has 7,308 customers per raw GiB (MEASURED), while the bench's own doc comment says about 2,100. At 100,000 customers per raw GiB (10 KB each, a plausible agent app) the filter is 125 KB per raw GiB. Maps would then be about 0.25 MB per raw GiB, about 0.48 MB per stored GiB at a 1.95 ratio, over the 0.3 MB target (ESTIMATED).

  State the target per raw GiB and per customer, and re-measure with a many-small-customers profile in step 2.
- **`DocV1` on a collection without a customer key** reads the first byte of the `_id` encoding as a length, so fragments of `_id`s are counted as customers. This is correct for reads (the rule is a function of a key prefix), but it can grow the filter. The writer should pass a filter kind per collection, or step 2 should make the question moot.
- **Orphaned multipart uploads cost money.** A writer dropped without `abort` (an error in `add`, a crash, a cancelled task) leaves an incomplete upload. On S3 its parts are billed until it is aborted (DECLARED, AWS S3 multipart documentation). The bucket needs an `AbortIncompleteMultipartUpload` lifecycle rule, or the compactor needs a sweep. Also, after a failed `put_part` the writer is not poisoned. A caller that ignores the error and calls `finish` relies on the store to refuse the missing part: object_store's S3 client does, but the writer should refuse by itself.
- **The random reader never checks a row's seq against the footer's min and max** (the stream does), and `parse` does not check that min_seq <= max_seq. Both are covered by CRCs today. Step 3 skips runs by max_seq, so a check is cheap insurance.
- `sessions/README.md` still asks for `RAPPORTO.md` in Italian (noted by earlier sessions).
