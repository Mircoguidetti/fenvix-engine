APPROVED: FIX 1 is fixed. My test passes unchanged, no test was weakened, and one more attack (a run rewritten at the same path) is an error everywhere.

# Review round 2: compaction 2, step 1 (FIX 1 only)

Reviewer session 2026-11-07. Spend: $0, local only, nothing merged.
I checked only FIX 1 of round 1, against fenvix-engine `c2-step1-runformat` 0d680b4 (the fix is 39849e6) and the owner's report `sessions/2026-11-07-c2-step1-fix/REPORT.md`.
My branch: `review-c2-step1`, fast-forwarded to 0d680b4, plus:
- one test;
- `judge/out/0d680b4/VERDICT-FAST.md`;
- `research/c2-step1-runformat/review/results/review-tests-r2.txt`.

## The fix

What I checked in the diff of `src/runfmt.rs`:
- Every run gets 16 random bytes, its run id.
- The id is written in the footer and in the trailer. The reader checks that the two are equal, and the footer CRC covers both.
- The id is mixed into every block CRC through one function, `block_crc`, which the writer and the reader share.
- `Run::open_expecting` refuses a run whose id differs from the one the caller recorded.

This is the remedy proposed in round 1.

## Checks

- **My round-1 test is unchanged and passes.** That is `review_blocks_of_one_run_with_the_footer_of_another_are_an_error`. In the tests file, the diff from 7b66367 only adds the owner's new test. No other test changed (MEASURED, `git diff`).
- **The attack once more: one new test, which passes.** It is `review_r2_a_run_rewritten_at_the_same_path_is_an_error`. A reader opens run A. Then the same path is rewritten with run B: the same rows with one value changed, so the same size and the same block offsets (the test checks this first). Results (MEASURED):
  - every block of B read through A's footer is an error, and so are a full range read and a point read;
  - `open_expecting` with A's recorded id is an error;
  - a fresh open of B reads B exactly;
  - B's footer with A's trailer, and A's footer with B's trailer, are errors at open.
- **All 18 runfmt tests pass** on my branch (MEASURED).
- **The mutation scripts changed in two places, and both are fine:**
  - the owner added mutation 13, "block CRC without the run id". Only my round-1 test catches it, which is right;
  - the owner re-targeted my R14 to the new `block_crc` line. It still removes only the block index, so it is the same mutation.

## Judge

`judge/run.sh fast` on 0d680b4 (MEASURED, `judge/out/0d680b4/VERDICT-FAST.md`):
- build PASS;
- unit FAIL, 125 passed and 1 failed. The failure is `counter::tests::rev_maps_leave_room_for_active_data`, the known one;
- diff-py and diff-node FAIL, 412/413 each, the known `bucketauto.infinity`;
- ctl PASS;
- kill PASS.

This is the same as the owner's verdict, and the same known failures as `compaction`, none added.

## Notes

The round-1 notes stand and stay with the build steps:
- the maps target per raw GiB and per customer;
- `DocV1` on collections without a customer key;
- orphaned multipart uploads;
- seq checks in the random reader;
- R12 and R13, defence in depth with no test.

Step 3 should use `open_expecting` with the id from the manifest line.
