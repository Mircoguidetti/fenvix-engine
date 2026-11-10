FIX 1 (fix-unique-txn c113062: 1 failing test on review-fix-unique-txn, `ctl-tests/test_42r_review_unique_build.py`)

# Review: fix-unique-txn (unique indexes across concurrent transactions)

Reviewer session of 2026-11-07. Delivery: fenvix-engine branch `fix-unique-txn`, tests ca3125a and 536655a, fix e456d5c, verdict c113062, base `integration` d6ce3d4. Contract: `prompts/P-fix-unique-in-transactions.md`. Owner's report: `sessions/2026-11-07-fix-unique-txn/REPORT.md`. Everything ran locally in one sandbox (4 CPUs), $0: no AWS, nothing merged. Reference: mongod 7.0.34 from `judge/setup.sh`.

## In one line

The transaction fix is correct and costs nothing measurable, but one path still lets writers create a duplicate under a unique index: writes made while a unique index is being built in chunks. Both writes are accepted, the build finishes, and the finished unique index holds the same value twice. The hole was already on `integration`; this fix does not add it. It is the defect the coordinator's points 1 and 4 ask about, so it blocks: FIX 1.

## The defect (a failing test)

Kitchen picture: the manager checks the new reservation book page by page (an index built in chunks). Everything on the pages already checked is trusted. A waiter who writes two reservations for the same table on a page already checked is never questioned, by the waiter or by the manager.

Real terms: an index on a collection that already has documents is built in chunks (`cmd_index.rs`, `build_step`, 2000 documents per chunk), and the database lock is released between chunks. While the index is `Building`, `Writes::put` (`coll.rs`) adds its entries but checks uniqueness only for indexes in `State::Ready`. The build checks only documents still ahead of its cursor. So a write to a document behind the cursor is checked by nobody. Two such writes with the same value (or one update to a value an already-indexed document holds) leave a finished `unique: true` index with a duplicate. This breaks guarantee 4 (indexes always consistent with the data) and the contract of a unique index. A transaction that writes between two chunks and commits before the next chunk goes through the same gap: the new commit check in `txn.rs` only catches index changes made after its snapshot.

Test: `ctl-tests/test_42r_review_unique_build.py`. It starts the server with `MONGO_BUILD_CHUNK=1` (the existing test setting) and 3000 documents with `_id` 1000 and up. It starts `create_index("k", unique=True)`, and while the build runs inserts `{_id: 1, k: -1}` and `{_id: 2, k: -1}`. MEASURED on c113062: build ok, both inserts ok, index `k_1` unique, 2 documents with `k: -1`. The test fails on ours and passes on the reference mongod. It also fails on the `integration` binary (MEASURED), so the hole was there before. The test fails on purpose if the build ends before the writes arrive, so it cannot pass without hitting the gap. What mongod does: a hybrid build accepts the concurrent writes and then fails the build with DuplicateKey (11000) (DECLARED, mongod documentation on index builds against populated collections). It never finishes a unique index with a duplicate.

Why it matters at scale: the window is the whole build. On a 1 TB collection that is hours. Inserts with increasing ObjectIds land ahead of the cursor and are caught. But updates to existing documents, and inserts with string or UUID `_id`s (agent frameworks often use those, Mastra included), land behind the cursor at random and are not checked (ESTIMATED from the code path). A possible fix, for the owner to choose: in `put`, run the same unique check (scan of the value's entries plus `claim_unique`) for a unique index in `State::Building` as for `Ready`, or fail the build at the end if any value has two entries, as mongod does.

## What I checked and found right (no failing test)

1. Two writers, every path in the coordinator's list. The owner's 45 cases pass on ours and on mongod. 21 of them fail on the `integration` binary (MEASURED, matches the report). I read the whole diff. I also probed by hand on both servers (MEASURED, same safe outcome on both, never a duplicate):
   - a value deleted and re-inserted inside one transaction, against another transaction (112 with the label) and a plain insert (11000);
   - a transaction update against a plain update into the same value: on ours the plain update wins and the transaction gets 112 at commit; on mongod the transaction gets 251. One document either way;
   - a write in a transaction that fails with 11000 aborts the transaction (`run_in_txn` treats writeErrors as failure), so it holds no claim afterwards.

   Upserts, findAndModify, bulk writes, compound, sub-document, partial, `_id` under a collation, and two databases are covered by the owner's tests. I read why each one works: they all go through `Writes::put`. Plain writes all commit through `cmd_data.rs` (and the capped conversion), which now calls `mark_unique`. The only other `Writes` users are TTL and capped trimming, which only delete. Kill -9: the owner's kill test passed in my run, 3 trials, 1174 acknowledged commits, 0 duplicates, 0 lost (MEASURED). That is far from the 100 kills of guarantee 1; see the notes.
2. Error codes and labels: 112 with TransientTransactionError for a value held by an open transaction or committed after the snapshot, 11000 for a value committed before it, 11000 outside transactions. Checked on both servers by `test_errors_like_mongod` and by my probes (MEASURED).
3. Claims are released on commit, abort, transaction timeout (`transactionLifetimeLimitSeconds=2`, then another transaction takes the value) and session end (`endSessions` from closing the client). All MEASURED on ours, same as mongod. Claims live in the same lock table as document locks and are released by the same `end_txn`.
4. The index-build fix for transactions: a transaction that wrote to a collection whose index definitions changed after its snapshot gets 112 at commit. This is correct, and the owner's test passes on both servers. As the owner reports, a build does finish while a transaction is open, unlike mongod. The transaction is the side that gives way, which is safe. The chunked-build gap above is a separate path.
5. Performance (MEASURED, 8 client threads, 20 s per run, two runs per binary alternated, collection with two unique indexes, no transactions): bulk inserts `integration` 7355 and 7595 docs/s, fix 7412 and 7398 docs/s. Single inserts `integration` 1650 and 1757/s, fix 1745 and 1673/s. With one idle transaction open on the same database the whole time (this turns on the recording of unique values): `integration` 7235 and 7262, fix 7300 and 7505 docs/s. The difference is within the run-to-run noise (about 3%) in both directions.
6. `judge/run.sh fast` on c113062: same as `integration` (see the judge section below).

## Judge

`judge/run.sh fast` on c113062 (same engine as e456d5c), MEASURED, after restarting the reference mongod with the judge's own settings (my first run used a mongod I had started without `ttlMonitorSleepSecs=1`, so 3 TTL cases differed; that was my setup, not the engine):
- build PASS;
- unit 101 passed, 1 failed: the known `counter::tests::rev_maps_leave_room_for_active_data`, as on `integration`;
- diff-py 412/413 and diff-node 412/413: the one difference on both is the known `bucketauto.infinity`, as on `integration`;
- ctl: 2 of 3 files pass, `test_05_erase` fails (`test_concurrent_erase_and_fork_are_serialised`: a usage file `control/usage/<db>_k/2026-11-07/erase-...json` is left in the bucket). It fails the same way on the `integration` binary, 3 runs out of 3 on each binary (MEASURED), so it is not caused by this delivery. The owner's run passed it. Why my sandbox differs: UNKNOWN (maybe the date or the MinIO state). It needs a look outside this review;
- kill PASS.

Verdict file: `judge/out/c113062/VERDICT-FAST.md` on this branch. Apart from the ctl file above, which fails on both, it is identical to the owner's verdict and to `integration`.

## Notes (not blocking: I could not turn these into failing tests)

- The `modified` map growth the owner reports (emptied only when no transaction is open) is real (read in `dbs.rs`), and this fix adds unique values to it. I agree with the owner's proposal of a separate task; it matters before a busy database uses overlapping transactions.
- Guarantee 1 asks for at least 100 kills at random points. The kill test here does 3 per run. It found nothing and the fix keeps no new durable state (claims are in memory, a commit is one batch), so I do not expect a difference. But the count is below the bar.
- The two known differences from mongod (a plain write and an index build win over an open transaction, and the transaction retries) are safe for correctness. They can make a transaction retry where mongod would make the plain write wait. Drivers handle this with the label.
- `locks.retain` in `end_txn` walks the whole lock table on every transaction end, and that table now also holds claims. This is fine for small transactions. It could matter with thousands of concurrent large transactions. UNKNOWN, not measured.

## Links

- fenvix-engine branch `review-fix-unique-txn` (based on c113062): the failing test `ctl-tests/test_42r_review_unique_build.py` and this file.
