FIX 1

# Review round 2 of compaction 2 step 3, `c2-step3-reader` at e7c22ef (fixes in 2a381ac)

Reviewer session, 2026-11-09. Scope, as the coordinator set it:
- my two defects of round 1;
- F6 of `sessions/2026-11-09-review-c2-step0-r2/REPORT.md`.

Owner's report: fenvix-brain `sessions/2026-11-09-c2-step3-fix/REPORT.md`. Spend: $0, local only.

Branch `review-c2-step3` is now at a8c47c7: e7c22ef plus one new failing test.

This is the last round. What remains goes to the founder.

## Verdict

- Defect 1 (the raw-reader check) is fixed and complete.
- Defect 2 (rule 25, fence pending) is fixed for every case where a version already exists in the store.
- F6 is fixed: the timeout, the startup check, and the late-answer test.

One hole is left, and it is in the new code. On a database's **first** publish, a timed-out PUT is treated as "nothing landed". The fence is skipped, and the late PUT then lands a version this waiter does not know. One failing test.

## Checks (all MEASURED)

- My round 1 tests, unchanged:
  - `raw_readers_misses.sh`: all 4 cases caught (it missed 3 on dedb4e5).
  - `review_no_hold_after_an_unknown_publish_whose_fence_failed`: passes.
- The owner's new handle check (`fix/raw_readers_handles.sh`): a stray `open_writers()` and a stray `open_readers()` are both caught.
- Controls fail without their fix:
  - `rule25_control_without_the_pending_state_breaks` passes; it asserts the violation when the pending state is switched off.
  - I removed the PUT timeout from `manifest::create` in a scratch copy, and `f6_a_put_whose_answer_is_late_is_a_timeout_and_the_next_hold_fences_first` then fails (`fix_tests.rs:208`).
  - The old raw-reader pattern misses the cases shown in round 1.
- F6 timing:
  - The PUT timeout wraps the whole `put_opts` call, so it covers the store's own retries.
  - Fences go through the same `create`, so they get the same timeout.
  - `check_config` refuses each of the four bad timings (owner's test passes), and it runs at startup whenever `--sealed-runs` is on.
- `load_fx` now checks that X_d <= SlateDB's last seq on every version its loop reads (my round 1 note 1).
- Judge fast on e7c22ef (fix code of 2a381ac), my run:
  - Sealed runs off:
    - readers PASS, 22 matches, all in named files;
    - build PASS;
    - unit 146 passed, 1 failed: the known `counter::tests::rev_maps_leave_room_for_active_data`;
    - diff-py 412/413 and diff-node 412/413, only the known `bucketauto.infinity`;
    - ctl FAIL, 1 of 3 files: the known `test_05_erase::test_concurrent_erase_and_fork_are_serialised` flake (a leftover `control/usage/` record, no `fx/` object);
    - kill PASS.
  - `JUDGE_SEALED_RUNS=2`: the same, except ctl PASS (3 files).
  - This is the same picture as the owner's two verdicts.
  - My first attempt at the off run compiled my new failing test into its unit phase, because I had written it in the judge's working tree. I stopped that run and ran both again on a clean e7c22ef; the numbers above are from the clean runs.

## Defect (a test that fails on e7c22ef)

### 1. A late PUT of the first publish lands unfenced

Test: `cargo test --release -p tenant-bench --lib fx::fix_tests::review_r2_a_late_first_publish_put_lands_unfenced`. It uses the owner's own `LandLate` store action.

What happens:
1. A database's first publish times out under F6's PUT timeout (200 ms), and the PUT is still on its way.
2. `fx_publish` sets the fence-pending state and calls `load_fx`. `load_fx` finds no version in the store, so it clears the pending state ("nothing landed, nothing to fence") and writes no fence.
3. 600 ms later the late PUT lands version 1, and nothing refuses it.
4. The waiter still knows no version, and its reads succeed.

The test fails with: "the first publish timed out, the fence was skipped (pending after the publish: false, known version: None), the late PUT landed Some((1, "publish")) unrefused, and a read succeeded with known version None".

Why it matters: F6 exists because a timed-out PUT can still land. Rule 25 says the fence must take the slot so that "either way the waiter knows the newest version". An empty listing does not show that nothing will land; it only shows that nothing has landed yet. In step 4, a drop job that reads the store sees version 1 and, one lease later, drops SlateDB rows at or below its X_d, while this waiter still reads SlateDB alone. That is the NoFence-timeout trace, with "no version" in the place of the old version.

Possible fix: when the outcome is unknown and the store holds no version, write a fence at the slot the timed-out PUT targeted. That slot is version 1 for a first publish, or `last + 1` in general. The fence copies nothing: it is a version whose kind says "no runs", or the first publish's own version re-created. Clear the pending state only once that fence is published. A simpler alternative: never clear the pending state on an empty store while a PUT with an unknown outcome targeted that slot.

## Notes (no test; for the founder)

- **A woken tenant in reader mode after a restart takes holds without a fence.** It fences only when it upgrades to a writer. Rule 25 lists "just opened after a restart" among the cases that need a fence before any hold. A reader-mode tenant runs no drop job and refreshes within T_pin, so I did not count it as a step 3 defect; step 4's drop floor should not count on it. This was outside this round's scope.
- **Reads refuse while a fence is pending, and they do not wait.** Reads fail with the retryable error (code 134), and the next exclusive step retries the fence. That meets rule 25. While the store stays down, every read of that database fails retryably, even though SlateDB could answer it. This is availability, not correctness.
- **The owner's F7 reading and step 4 preconditions look right to me.** F7: today's lease, checked against the waiter's own clock, stops an old waiter from starting reads. The step 4 preconditions: export, PITR and `--envelopes write`, plus a hold per command. I did not test them; they were outside this round's scope.

---

# Round 1

FIX 2

# Review of compaction 2 step 3 (our manifest and the merged reader), `c2-step3-reader` at dedb4e5

Reviewer session, 2026-11-08. Contract: fenvix-brain `prompts/P-c2-step3-reader.md`. Owner's report: `sessions/2026-11-08-c2-step3-reader/REPORT.md`. Rules checked against: `RULES-FOR-THE-CODE.md` version ff27fb9 on `c2-step0-tla`. Its head, ce42354, has no change to that file, so ff27fb9 is still the newest. Spend: $0, local only (the same 4 vCPU sandbox, in-memory store and local MinIO).

Branch `review-c2-step3` (based on dedb4e5) has the two failing tests below and this file.

## Verdict

The merged reader holds. I found no wrong, missing or extra row. Two defects, each with a test that fails on dedb4e5:

1. The judge's raw-reader check misses three ways of reading SlateDB directly.
2. A publish whose outcome is unknown, followed by a fence that fails, leaves the writer taking holds without a fence. That breaks rule 25.

Neither one gives a wrong read in this step, because nothing drops SlateDB rows yet. Both are guards that step 4 relies on, and the contract (defect 1) or the rules the report says it follows (defect 2) ask for them now.

## What I re-ran (all MEASURED)

- `judge/run.sh fast` on dedb4e5 with `--sealed-runs` off: readers PASS; build PASS. Unit: 140 passed, 1 failed, the known `counter::tests::rev_maps_leave_room_for_active_data`; this phase also re-ran the 100,000-operation model test, which passed. diff-py 412/413 and diff-node 412/413, only the known `bucketauto.infinity`. kill PASS. ctl: 1 of 3 files failed, `test_05_erase::test_concurrent_erase_and_fork_are_serialised`, which left `control/usage/ef2_k/...json`.
- The same with `JUDGE_SEALED_RUNS=2`: identical, line by line, including the same `test_05_erase` failure. The owner measured that flake at 1 of 4 rounds with sealed runs on and 2 of 4 with them off. It leaves a control-layer record and no `fx/` object, so I count it as the known flake, not as a sealed-runs defect. It still makes ctl FAIL in both of my runs, where the owner's 520c338 verdicts show ctl PASS. Verdicts: `judge/out/dedb4e5/VERDICT-FAST.md` and `judge/out/dedb4e5-sealed2/VERDICT-FAST.md` in my sandbox (not committed, same as the owner's).
- The model test with my own seeds, which also gives new crash points (the fail points and the random crashes come from the seed). 0 wrong in 100,000 operations each for seeds 1, 31337 and 990077:
  - 105, 127 and 121 crashes;
  - 28/18, 33/28 and 25/21 crashes at the after-runs / after-publish fail points;
  - 939, 924 and 942 publishes;
  - about 4.9 million rows answered by runs in each run.
  - Files: `research/c2-step3-reader/review/results/model-100k-seed*.txt`.

## Defects (tests that fail on dedb4e5)

### 1. The raw-reader check misses direct SlateDB readers

Test: `research/c2-step3-reader/review/raw_readers_misses.sh`. It adds one stray reader to a copy of the tree and runs `judge/raw_readers.sh` on that copy.

`judge/raw_readers.sh` passes with each of these in `mongo-server/src`:
- `slatedb::DbReader::open(...)` followed by `get`: `DbReader::open` is public in slatedb 0.17 (`db_reader.rs:910`);
- `slatedb::Db::open(...)` followed by `get`;
- `slatedb::Db::builder(...).build()` followed by a `scan`.

The control case, `DbReader::builder`, is caught.

Result: `research/c2-step3-reader/review/results/raw-readers-misses.txt`, 3 missed (MEASURED).

Why it matters: round 2's fix 3 says the check must fail on any direct SlateDB reader outside `fx` and `lifecycle.rs`. Once step 4 drops absorbed rows, a reader built one of these ways answers from SlateDB alone and misses rows, and the judge stays green.

Possible fix: add `DbReader::open`, `Db::open`, `Db::builder` and `DbBuilder` to the pattern. A grep cannot see reads through the `Db` handles that `lifecycle::open_writers()` and `open_readers()` hand out, so `maps.rs` should stay the only caller of those two.

### 2. Rule 25: a hold is pinned with no fence after an unknown publish

Test: `cargo test --release -p tenant-bench --lib fx::review_tests` (`review_no_hold_after_an_unknown_publish_whose_fence_failed`).

What the test does:
1. A store wrapper lets the publish's conditional create land and then return a timeout, so the outcome is unknown and version 2 is in the store.
2. The next manifest creates time out without landing.
3. `fx_publish` tries its fence (rule 25), and the fence times out. `fx_publish` prints the error (`eprintln!` in `Tenant::fx_publish`) and goes on.
4. The next `Tenant::get` takes a hold on version 1, while version 2 is in the store and no fence was published.

The test fails with: "rule 25: a hold was pinned on version 1 (known before the publish: 1) while version 2 is in the store and no fence was published" (MEASURED).

The server's publish in `Entry::write` also swallows the error, so the server takes the same path.

Rule 25 says: "it publishes a fence version before it pins any hold and before it starts another publish ... holds wait until the fence's outcome is known, and a fence that times out is itself followed by another fence". This is the state of TLC's `NoFence-timeout` trace. In step 4 the drop floor treats the holds and the newest version in the store as true.

Possible fix: when a fence fails, set a "fence pending" flag on the tenant. Until a fence succeeds, every hold and every publish first retries the fence, and fails with the retryable error if the fence still fails.

## Notes (risks; no failing test, so they do not block)

- **`load_fx` checks X_d once.** It checks X_d <= SlateDB's last seq only on the version it loaded first. When its fence loses the race and it reloads, it does not check the reloaded version. I believe that case cannot happen: a version needs a durable WAL flush before its PUT, and the new writer's open fences any later WAL write of the old one. So I wrote no test. Still, a version with X_d above the writer's seq would hide the writer's new rows at or below X_d, a lost write. The check costs nothing; put it inside the loop.
- **Holds only last one read.** Plain operations take one short hold per read, not one per operation. Today that is safe because the version changes only under the exclusive lock (the report says so). Some operations take the lock more than once: an aggregation with `$lookup` reads each foreign collection under a new `dbs.read`. A publish can land between those reads. That changes nothing today, because a publish does not change the logical state. Under step 4, the drop floor must cover the gap between them (rules 7 and 8, the lease).
- **No T_pin refusal without a version.** The refusal past T_pin (rule 6) runs only when the tenant already has a version. A reader in reader mode with no version and no announcement never refuses. With leases, no writer in another process serves the database, so this case is not reachable today.
- **The model test is milder than the server.** Its crash is `abandon()`, which still flushes the WAL. Its reader mode only comes after a crash, so no writer is writing at the same time. It never has two processes. The fence test and my test 2 cover part of what is missing.
- **Defects I looked for and did not find:**
  - direct writes that bypass envelopes: every write goes through `Tenant::stage` or `Tenant::put`;
  - a store wrapper that drops `PutMode::Create`: `CountingStore`, `OwnedStore`, `LocalCas` and the sim stores all pass it through;
  - `dropDatabase` or erase leaving `fx/` behind: both delete the tenant prefix;
  - a fork copying `fx/`: the fork filters it out;
  - PITR writing plain deletes: it applies its diff through `Entry::apply`, so it writes envelopes.
- **The report's own open items stand as written:**
  - export and PITR read SlateDB only;
  - `--envelopes write` skips the announcement;
  - the offline builder does not scale;
  - one JSON object per version;
  - no GC;
  - holds are per process;
  - the `moved_doc` race of step 2 is still open.

  Step 4's drop must not start before export, PITR and `--envelopes write` are fixed.

## Errors or gaps in fenvix-brain

- The prompt asks to "re-run the model test with your own seeds and crash points". The model takes its crash points only from the seed (`FX_MODEL_SEED`); there is no knob for the crash rate. New seeds were the only way to get new crash points.

---

# Earlier reviews (kept as they were on the base branch)

FIX 3

# Review: compaction 2 step 2 (key layout v2, `_id` per customer, envelope deletes)

Reviewer session 2026-11-07, prompt `prompts/R-review-c2-step2.md`. Delivery: fenvix-engine `c2-step2-layout` at 660d46e (engine of 51b9a90), and `c2-step2-layout-e0` at 5a17ea9. Contract: `prompts/P-c2-step2-layout.md`. Owner's report: fenvix-brain `sessions/2026-11-07-c2-step2-layout/REPORT.md`. Spend: $0, local only (4 vCPU sandbox, MinIO, mongod 7.0.34 from `judge/setup.sh`). Nothing merged, no branch deleted.

The failing tests are in `ctl-tests/test_46_review_c2_step2.py` on this branch (`review-c2-step2`, based on 660d46e). Run:
`cd ctl-tests && python -m pytest -q test_46_review_c2_step2.py` (MEASURED on 660d46e: 3 failed, 9 passed).

## Verdict

Viva, con correzioni (alive, with fixes). The storage work holds: the range cutting, the envelopes, the two-release rule, export, PITR and forks all survived every attack I made. The three defects are all on the same edge: places that still identify a document by `_id` alone, now that `_id` is unique per customer only. One of them hands one customer's document to another customer's watcher.

## The three failing tests (defects)

1. `test_change_stream_update_lookup_returns_the_updated_customers_document`. Alice and bob both hold `_id` 1. An update of bob's document, watched with `fullDocument: "updateLookup"`, delivers alice's document (`{'_id': 1, 'u': 'alice', 'secret': 'alice-data'}`) (MEASURED). Why it matters: it shows one customer's data to another customer's watcher, and it gives the wrong data to any app that syncs through a change stream. Cause: `cmd_changes.rs:261` calls `Entry::get_doc(coll, _id)`, which takes the first customer holding the `_id` (`dbs.rs:1239`).
2. `test_change_stream_document_key_names_the_customer`. The delete event of bob's document 1 has `documentKey: {_id: 1}` (MEASURED). The founder's rule is "like MongoDB sharded collections", and mongod puts the shard key in `documentKey` for those. Why it matters: a consumer (a cache, a search index, an agent's memory) cannot tell bob's delete from alice's document 1, so it deletes or overwrites the wrong one. The same key is also what defect 1 needs to look up the right document. Cause: `cmd_changes.rs:210` writes `{_id}` only.
3. `test_same_string_id_under_two_customers_in_a_collated_collection`. In a collection with a default collation, `{_id: "doc-1", u: "bob"}` is refused with 11000 when alice already holds `"doc-1"` (MEASURED). This breaks the done-means "the same `_id` under two customers is accepted". Why it matters: the application's write fails. The owner's report lists it as a known limit ("stricter than the founder's rule"), but nobody decided it, and the contract does not allow it. Cause: the collation `_id` index (`idc/`) is still one per collection.

A fix for 1 and 2 would be: put the customer field (and the time field, if any) into `documentKey`, as mongod does for a shard key, and do the lookup by the full key. A fix for 3 would be: key `idc/` per customer too, or else the coordinator and founder decide to refuse `customerKey` together with a default collation (that is a design change, not the reviewer's call).

## What I re-ran (the prompt's list)

- `judge/run.sh fast` on 660d46e: build PASS; unit 114 passed, 1 failed (`counter::tests::rev_maps_leave_room_for_active_data`, known on `integration`); diff-py 412/413 and diff-node 412/413 (only `bucketauto.infinity`, known); ctl PASS (3 files); kill PASS (MEASURED, `judge/out/660d46e/VERDICT-FAST.md`). Identical to the owner's verdict on 51b9a90.
- Diff suites with customer keys on and `--envelopes write` (`bench/c2-step2/diff_keys.sh`, every test): diff-py 4,124/4,127, with the same three differences as the owner (`cs.delete_many`, `bucketauto.POWERSOF2.decimal_special`, `bucketauto.infinity`) (MEASURED). diff-node 4,125/4,127, with the same two differences as the owner (MEASURED, `bench/review-c2-step2/diff-keys-envelopes-SUMMARY.txt`).
- Agent adapter suites, with customer keys declared on the adapters' collections (`bench/review-c2-step2/adapter-keys.txt`) and `--envelopes write` (MEASURED): LangGraph saver 146 passed, 0 failed; LangGraph store 29 passed, 3 failed (all three `$vectorSearch`, Atlas-only, also errors on mongod); LangGraph JS 123/0; OpenAI Agents 72/0; LlamaIndex 18/0; Mem0 23/0; Mastra 1,072 passed, 10 failed, the same 10 as mongod's run, "pass on mongod, not on ours: 0". Customer-first documents written: 61, 201, 10, 0, 34, 0 and 3,568 (Mastra, with 1,817 layout seeks), and envelope rows read in every suite that reached the server. The OpenAI Agents and Mem0 suites fake the driver, so 0 documents reach the server (as the owner says). I did not re-run the end-to-end loops (50 users × 200 turns): too long next to the rest. That is UNKNOWN from me; the owner's numbers stand on their own.
- First-course `stress.py`, 60 rounds, 40 users, against mongod: 2,574 checks, 0 wrong, 0 differing from mongod, with `--envelopes read` and with `--envelopes write` (MEASURED).
- Two-release rollback (`test_45`) with my own builds of E0 (5a17ea9) and `compaction` (a7890b3): 2 passed. The E0 binary reads everything E1 wrote, and the old binary fails with "Malformed BSON bytes" (MEASURED).
- The delivered ctl tests: test_44 5/5, test_42_customer_compaction 3/3, test_42_unique_txn 33/33, test_42s 28/28, test_42b 12/12. test_43 5/6: the failure is `test_a_refused_fork_leaves_no_object_of_its_target`, known on `integration` (MEASURED).
- The merge of `fix-unique-txn`: `test_42r_review_unique_build` fails here with its own guard ("the build finished before the writes: raise DOCS"). It fails the same way on my build of the `fix-unique-txn` tip (bc49e06) and on `compaction`, so it is the test's timing in this sandbox, not the merge. With `DOCS = 30000`, the delivered binary passes, and the `compaction` binary leaves 2 documents with the "unique" value (MEASURED). So the fix survived the merge, and the test still detects the hole. NOTE: raise DOCS in that test.

## What I attacked and could not break (the 9 passing tests, MEASURED)

- Range reads cut across the mapped and plain key spaces: 25 query shapes, run on a customer-keyed collection (with and without the time field) and on the same data in a plain collection, give equal answers. They cover ascending and descending sorts, limits, skips, hints on `_id_`, on an index led by the customer field and on one not led by it, `_id` ranges and `$in`, and customers that are arrays, missing, null or numbers, after moves between customers and in time and after deletes. Counts, distinct and `validate` agree too.
- Rename within a database and across databases, an index dropped and rebuilt, `$out`, drop and re-create of the same name, `convertToCapped`, `cloneCollectionAsCapped` and capped eviction: all equal the plain control.
- `_id` per customer in index builds and upserts: a unique co-located index refuses a build over a value one customer holds twice, and accepts it across customers. A unique index not led by the customer stays unique across customers. An upsert by `_id` alone updates one document and inserts none, and `updateMany` reaches both. 6 threads × 40 concurrent upserts of one (customer, `_id`) leave one document with n = 240. In transactions, upserts of one `_id` under two customers both commit, and under one customer at most one commits.
- Envelopes against export and PITR: a database written by E0 (restore point P0), then by E1 (marker, envelopes, delete envelopes; restore point P1). The export holds exactly the documents. A restore to P0, a past without a marker going into a live database with one, gives P0's state, and writes after it read back. A restore to P1 gives P1's state.
- Forks across the envelope boundary (round-2 note on E_env), modes `copy` and `clone`: a database without a marker, forked by an E1 process, then the fork and the source both written with envelopes. Each reads its own model after a close and reopen, and a fork of the enveloped source reads the source. The owner's argument holds: the marker lives inside the database, so a clone from before it has none.

## Notes (risks, not failing tests; they do not block)

- Reads across customers: a scan of a whole customer-keyed collection, or of a co-located index without a customer equality, opens a new iterator up to twice per customer. The owner states it. At 1 TB with millions of customers, a collection scan or an unindexed query costs millions of iterator opens (ESTIMATED; it does not scale per CLAUDE.md's principle). Step 3's merged reader should avoid it, or the planner should say so.
- `prefetch.rs` scans logical keys (`idx/...`, `doc/...`) in the physical store. For customer-keyed collections it now warms nothing: the GETs are wasted, and the results stay correct (code reading).
- `moved_doc` (`coll.rs:343`): during a concurrent move, if the document was deleted rather than moved and exactly one other customer holds its `_id`, a read through an index fetches that other customer's document. The caller re-applies the filter, so the risk is a duplicate or a foreign document only when it also matches (code reading; a race, I could not make it deterministic).
- `ReadView::segs_for` lists every collection and index of the database on each range read whose bounds do not tell the area. That is a cost on databases with many collections (code reading, not measured).
- A follower in another process (warm spare) reads E_env only when it opens. The owner states this; E1 must stay off until step 3 carries E_env in the manifest. I agree.
- Arrays as customer values are accepted. MongoDB refuses them in shard keys. `{u: "a"}` and `{u: ["a"]}` are then two customers that both match `{u: "a"}`, each with its own `_id` space. That is a product choice for the coordinator.
- Not re-run by me: the adapters' end-to-end loops, and a SlateDB sorted-run compaction between the two releases (the owner marks that UNKNOWN too).

## Corrections for fenvix-brain

- None to the owner's report beyond the three defects above. Its section 4 already names defect 3 as a limit, but it should have been a stop: the done-means says "accepted".

---

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
