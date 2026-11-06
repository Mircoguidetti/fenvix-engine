APPROVED

# Review of compaction 91414a6 (round 1: FIX 2) and 1ae8158 (round 2: APPROVED) (customer-first keys, tiered compaction)

Reviewer, 2026-11-06. Branch `review-compaction` (based on 91414a6). Base `integration` (6ff285a). Tests:
`ctl-tests/test_43_review_compaction.py` (run: `cd ctl-tests && python -m pytest -q test_43_review_compaction.py`).
Every number is labelled MEASURED (run here, on this sandbox, local MinIO), ESTIMATED, DECLARED (the owner's), or UNKNOWN.

## The two fixes (tests that fail on 91414a6)

1. **Two writes of one `_id` for two customers are both acknowledged** (guarantees 4 and 7: `_id` uniqueness, indexes
   consistent with data). The locator `loc/{coll}/{_id}` is the unique `_id` index, but `Writes::put` only checks it
   against the committed state and never locks it (`check_conflict` runs on the customer-specific document key only).
   Two transactions inserting `_id: 7` for `alice` and `bob` both commit; so does a plain insert of `_id: 8` while an
   open transaction holds `_id: 8` for another customer. Result MEASURED: two documents with the same `_id`; a scan
   returns both, `find({_id})` returns one (the locator points at the last writer), `validate` does not check the locators (code reading, `cmd_index.rs` validate).
   mongod lets only one of the two commit. The control (same test on a collection without a customer key) passes.
   Tests: `test_two_transactions_cannot_insert_the_same_id_for_two_customers[m]`,
   `test_insert_outside_a_transaction_cannot_duplicate_an_id_a_transaction_holds[m]`.
   Fix hint: `check_conflict` (lock and `modified_after`) on the locator key too, for inserts and moves.

2. **With customer keys off, the storage format still changes and the previous release can no longer read it**
   ("with the option off nothing changes"; guarantee 10, rollback). `--sst-compression lz4` and `--sst-block-kb 64` are
   the new defaults for every database, keyed or not. MEASURED: a database written by 91414a6 with no flag at all,
   then opened by the integration binary: `Data error: invalid composite filter block`. Deploying this commit makes
   every database it flushes or compacts unreadable by the release before it, with no way back but forward.
   Test: `test_with_customer_keys_off_the_stored_format_does_not_change` (checks the codec; with `INTEGRATION_BIN` set
   it also does the rollback read). Fix: defaults off (`none`, 4 KiB) until the founder decides a one-way format
   migration, or ship a release that can read lz4 first.

## Priority from the founder: `test_05_erase.py` leaves control-layer objects

- Found the cause. It is **not** caused by this branch. In `test_concurrent_erase_and_fork_are_serialised`, when the
  erase wins, the fork (`ctl/fork.rs`) takes the target's lease, finds the source gone, answers 404 and deletes the
  lease; but those requests were charged to the target's (and source's) usage counters, and the usage flusher then
  writes `control/usage/<target>/<day>/...` (and `control/usage/<source>/...`) for databases that do not exist. Nothing
  ever deletes them (guarantee 6). test_05 only sees it when a flush (every 1 s there) lands between the fork's answer
  and its listing: that is why it is flaky.
- Deterministic test: `test_a_refused_fork_leaves_no_object_of_its_target` (a fork of a source that never existed,
  wait 3 s, list). MEASURED: fails on 91414a6 **and on integration**, with the same two leftover records.
- Does the branch make it worse? MEASURED, the race of test_05 repeated 30 times per binary: the leftover is there 3 s
  later every time the erase wins (91414a6 12 of 12, integration 8 of 8); the test's own listing caught it 0 times in
  30 on 91414a6 and 1 in 30 on integration. Fork and erase times are the same (p50 110 ms both). The owner's 3 of 4
  vs 1 of 4 is within noise for 4 rounds. My own full runs of test_05 on 91414a6: 2 passed, then 1 failed in the judge.
- `tenants/<db>/_lease` left behind: not reproduced (UNKNOWN); every failure I saw was the usage records above.
- Not counted against this commit (contract point 5: identical on integration). Owner of the control layer: charge
  nothing to a database a refused fork never created (or `drop_db` both names when the fork fails).

## Judge (`judge/run.sh fast` on 91414a6, MEASURED)

Same verdict as the owner's: build PASS; unit FAIL only `counter::tests::rev_maps_leave_room_for_active_data`;
diff-py and diff-node FAIL only `bucketauto.infinity` (412/413 each); ctl FAIL only test_05_erase (above); kill PASS
(0 lost). All match integration's known failures. The 3 failures of the owner's customer-key stress run
(`cs.delete_many`, `bucketauto.POWERSOF2.decimal_special`, `bucketauto.infinity`): MEASURED, also fail with the
integration binary.

## Do the tests measure what they claim?

- **Cold-read bench (contract point 2).** Re-run at 2 GB, 20,000 customers (`review-2026-11-06/`), MEASURED: default
  settings mean 2.46 / 2.27 GETs, max 5 (owner: 2.8, max 7). The GET meter is right: over 300 finds, server meter
  1,631 vs MinIO's own getobject counter 1,630. But the measure is not of a cold find: the 256 MiB block cache keeps
  the data blocks read by the 300 warm-up finds (the bench says only indexes and filters are warm), and neighbouring
  customers share 64 KiB blocks. A mean below the number of sorted runs (3 here) shows it. With data blocks cold
  (`--block-cache-mb 1`), MEASURED here: mean 5.38, p95 6, max 7 (3 sorted runs); the owner's 4-run dataset: mean
  6.5-7.1, max 9 (6 of 400 finds over 8, DECLARED). So "at most 8" holds or not depending on how many runs the data has
  at that moment; 8 GB was not measured (DECLARED); ESTIMATED one more run per 4x of data, so about 2 GETs more at 8 GB.
  NOTE, not a fix (no deterministic failing test), but the contract's point 2 is not proven for a cold find.
- `test_index_matches_documents_during_compaction`: readers hold the customer's lock, so no read ever overlaps a write
  or a move of that customer; it checks consistency between writes, not during them. I probed the during part
  (MEASURED, 30-40 s each, 4 movers): collection scans, counts and `$natural` during 2,365 moves: 0 wrong of 410;
  index range scans during 2,783 moves: 0 duplicates or misses in 538; `find_one({_id})` on one document moved 4,092
  times: 0 misses.
- `loadtest/test_open_gc.py` (prints read violations, never fails): run with `--customer-key c=v` (every update moves
  the document), 2 trials x 60 s, MEASURED: 69,468 reads checked, 0 missing, 0 stale, 0 lost inserts or updates, both
  kills inside a cleanup pass. It hides no defect here; it should still fail on violations (judge gap, not this branch).
- Moves inside transactions (commit, abort), unique secondary index across a move, collation `_id` with a customer
  key: behave as without the key (MEASURED, probes).

## Notes (never block)

- Rule 18 (one snapshot per read): an `_id` read goes locator, then document, as two reads with up to 3 retries if the
  document moved in between (and `moved_doc` once). Not a snapshot; no wrong answer seen (above).
- The cleanup keeps the outputs of unfinished or uncommitted compactions (`Compacted` counts as active): a resumed job
  of the separate worker cannot point at deleted SSTs. Checked by reading slatedb 0.17 and `gc_tenant_ssts`.
- `stop_once` ignores a worker `stop` that times out; after `stop_limit` the runner is aborted and the worker's tasks may
  live on until their store request ends (as for the compactor before). No test shows harm.
- Contract point 1 was shown on the stress suite with the key on for some collections (63,076 documents written customer
  first, 2,249 locator lookups, DECLARED); the natural run had 1 locator lookup.

## Round 2: the owner's fixes, commit 1ae8158 (APPROVED)

Checked only the two fixes (`git diff 91414a6..1ae8158`) and my tests. Everything MEASURED here.

- My tests were not weakened: `ctl-tests/test_43_review_compaction.py` and this file are byte-identical between my
  round-1 commit (6df560c) and 1ae8158. The owner's only test change adds `--sst-compression lz4 --sst-block-kb 64` to
  `test_42_customer_compaction.py`, which checks the lz4 output: right, since lz4 is now an option.
- Fix 1 (duplicate `_id`): the locator is now locked and conflict-checked like a document key on insert, move and
  delete (`coll.rs`), and `loc/` keys are recorded in `modified` (`dbs.rs`) so a transaction's commit sees a
  plain write of the same locator. Both duplicate-`_id` tests [m] pass 3 runs of 3; the [plain] controls still pass.
- Fix 2 (format): defaults back to `none` and 4 KiB (`main.rs`, `tiered.rs`); lz4 / 64 KiB only by option. The format
  test passes 3 of 3 with `INTEGRATION_BIN` set (the integration binary reads all 5,000 documents back). Byte
  compatibility: the same 5,000 documents plus an index written by 1ae8158 and by integration, with no option: one
  compacted SST each, both 6,016,927 bytes.
- `test_a_refused_fork_leaves_no_object_of_its_target` still fails, as on integration (pre-existing; separate task).
- `judge/run.sh fast` on 1ae8158: build PASS; unit FAIL only `counter::tests::rev_maps_leave_room_for_active_data`;
  diff-py and diff-node FAIL only `bucketauto.infinity` (412/413 each); ctl FAIL only test_05_erase, by the same
  leftover usage record of a refused fork's target (`control/usage/ef1_k/...`), identical on integration; kill PASS.
  Every failure is one shown identical on integration (contract point 5).
- Not re-checked (outside round 2): the cold-read numbers. Note that with lz4 / 64 KiB now off by default, the
  owner's cold-read results (measured with them on) hold only with `--sst-compression lz4 --sst-block-kb 64`.
