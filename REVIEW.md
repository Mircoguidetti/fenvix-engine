APPROVED (fix-unique-txn bc49e06: the round-1 test passes, no failing test found)

# Review round 2: fix-unique-txn (unique index built on a live collection)

Reviewer session of 2026-11-07, round 2 only. Prompt: fenvix-brain `prompts/R-review-fix-unique-txn-r2.md`. Round 1: `REVIEW.md` at 49b4e3b (FIX 1, `ctl-tests/test_42r_review_unique_build.py`). Fix: branch `fix-unique-txn`, commits 2cbf245, bc96d4e, 2da7a79, bc49e06. Owner's report: fenvix-brain `sessions/2026-11-07-fix-unique-txn-r2/REPORT.md`. Everything ran locally in one sandbox (4 CPUs), $0: no AWS, nothing merged. Reference: mongod 7.0.34 from `judge/setup.sh`. "Old binary" means 49b4e3b.

## In one line

The hole is closed. The round-1 test passes unchanged, no test was weakened, 29 extra write paths I tried end safely and like mongod, the kill test is clean, and the judge shows only the known failures of `integration`.

## 1. The round-1 test, unchanged

- `git diff 49b4e3b bc49e06` touches only `mongo-server/src/{coll,cmd_index,index}.rs`, two new test files and the verdict file. No existing test changed, so none was weakened.
- On this sandbox the 3000-document build finishes in about the test's 0.5 s sleep, so with nothing else running the test fails on BOTH binaries with "the build finished before the writes" (MEASURED, 3 runs each). The owner saw the same. I reproduced the slower sandbox as the owner did: the test and server pinned to one CPU, with a busy loop on that CPU.
- Result (MEASURED, 5 runs per binary): fix 4 passes, 1 run missed the window. Old binary 4 failures with `{'build': 'ok', 'writes': ['ok', 'ok'], 'docs_with_k=-1': 2}` (the finished unique index holds the value twice), 1 run missed the window. mongod passes.
- Note: the test depends on machine speed (see notes).

## 2. Behaviour against mongod, and the hunt for a write path that still leaves a duplicate

I read the whole engine diff and the paths around it:
- Only `Writes::put` and `Writes::delete` write index entries (grep for `entry_key`). Every write path goes through them: insert, update, replace, upsert, findAndModify, bulk write, `$merge`, transactions.
- A write to a unique index that is `Building` looks up the value. If another document holds it, the write is still accepted and a re-check key goes into the same atomic batch. The last build step re-checks those values under the database lock and becomes `Ready` only when none are left. A plain write loads the index definitions and commits under the same lock, so it cannot see `Building` and commit after `Ready`.
- In a transaction: two open transactions cannot both add the value (claim, 112). A plain write of the same value makes the transaction fail with 112 at commit (`uniq` / `modified`, now also recorded for `Building` indexes). A transaction that spans any build step gets 112 at commit, because every step rewrites the index definition key.
- Same-database and cross-database renames refuse while a build runs. Drop and convertToCapped delete the whole `idx/{coll}/` prefix, re-check keys included.

Probes: `ctl-tests/probe_42u_review_build_paths.py` (committed on this branch). 29 cases, each run on ours (6,000 documents, one per build chunk) and on mongod (300,000 documents). The check after each case: a ready unique index must not hold a value for two documents (by aggregation, honouring partial filter and collation), and `validate` must be valid. The cases:
- findAndModify update, replace and upsert into a held value;
- findAndModify remove, then insert of the freed value;
- bulk writes: mixed insert/update/delete that resolve; ordered with two duplicates; a swap through a temporary value;
- a transient duplicate inside one command (update A into B's value, then B away);
- updateMany with a pipeline that reverses 10 values (transient duplicates, unique at the end), and updateMany `$inc` that collides;
- delete then re-insert: the same value once (ok), twice (duplicate), and the same `_id` with another document's value;
- a duplicate fixed by a later update; three holders of one value, one removed (still a duplicate);
- partial index: a document moved into the filter onto a held value, and a duplicate outside the filter;
- collation strength 2 (`"V3"` against `"v3"`);
- string prefixes (`"v1x"`, `"v"` next to `"v1"`), so no false duplicate;
- multikey: overlapping arrays, and one array that repeats its own value;
- compound with and without a duplicate;
- `$merge` of two duplicates;
- two unique indexes in one createIndexes, duplicate on the second;
- a transaction writing a duplicate;
- drop and re-create of the collection, renameCollection dropTarget onto it, and convertToCapped, all during the build.

Results (MEASURED):
- Fix: 0 holes. The 25 cases with a fixed outcome all match mongod: 14 give "writes ok, build 11000, no index" and 11 give "build ok, index ready, no duplicate". `validate` is valid in all 29.
- The transaction case differs only in the known round-1 way. Ours answers 112 while build steps run, `with_transaction` retries until the build is done, and the insert then gets 11000 on the ready index. mongod accepts the write and fails the build. No duplicate either way.
- Old binary: 13 cases end with a ready unique index holding a value twice (for example `{'k': 1, 'ids': [1000, 1001]}` after findAndModify), and its `validate` says valid. So the probes do reach the hole, and the new uniqueness check in `validate` is useful.
- mongod: no holes. My first mongod run had two probe bugs of my own (one "no duplicate" insert was a real duplicate, and the counter counted `[-5,-5]` twice for one document). After fixing them, both cases match.

## 3. Kill test

`ctl-tests/test_42t_kill_index_build.py` on bc49e06, MEASURED: 20 trials, 20 SIGKILLs, 16 of them during a build, 4 just after. 8 unique indexes kept and 12 builds failed with 11000. 0 duplicates, `validate` valid in all 20, 0 of 8,915 acknowledged inserts lost. Passed in 840 s. I read the test: it kills at a random moment, restarts, waits for the resumed build, and checks duplicates, validate and acknowledged inserts. It requires at least half of the kills to land inside a build.

## 4. Judge

`judge/run.sh fast` on bc49e06, MEASURED (`judge/out/bc49e06/VERDICT-FAST.md` on this branch):
- build PASS;
- unit 101 passed, 1 failed: the known `counter::tests::rev_maps_leave_room_for_active_data`, as on `integration`;
- diff-py 412/413 and diff-node 412/413: the one difference on both is the known `bucketauto.infinity`, as on `integration`;
- ctl PASS (3 files; `test_05_erase` passed this time);
- kill PASS.

This is the same as `integration`'s known failures and the owner's verdict on 2da7a79.

## Notes (not blocking)

- Drop, renameCollection dropTarget, and convertToCapped during a build behave differently from mongod. They are older than this fix: same on the old binary (MEASURED), and no duplicate is possible. Ours accepts the command, the index disappears, and the waiting `createIndexes` still answers ok. So a client is told a unique index was created that does not exist. mongod refuses rename and convertToCapped (12587, BackgroundOperationInProgress) and fails the build on drop (276, IndexBuildAborted). Worth a small task next to the owner's `dropIndexes` and `createIndexes` differences.
- On ours, a failed build's cleanup at one document per chunk is slow: a failed 20,000-document build took over 120 s to answer (MEASURED, client timeout in my first probe run). This is the drop cost the owner flags in section 7. The real chunk is 2000, so it is not a correctness issue, but the drop measurement the owner proposes should be done.
- The kill test checks acknowledged inserts, not acknowledged updates. A lost acknowledged update would go unseen. It is not needed for this fix (the update path writes one batch, like the insert), but guarantee 1 is about all acknowledged writes.
- `test_42r_review_unique_build.py` (my predecessor's test) depends on machine speed. It should wait until the build is under way instead of sleeping 0.5 s, so it can never pass or fail by missing the window. The owner proposed the same.

## Links

- fenvix-engine branch `review-fix-unique-txn-r2` (from bc49e06): this file, `ctl-tests/probe_42u_review_build_paths.py`, `judge/out/bc49e06/VERDICT-FAST.md`.
