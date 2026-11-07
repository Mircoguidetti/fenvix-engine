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
- Diff suites with customer keys on and `--envelopes write` (`bench/c2-step2/diff_keys.sh`, every test): diff-py 4,124/4,127, with the same three differences as the owner (`cs.delete_many`, `bucketauto.POWERSOF2.decimal_special`, `bucketauto.infinity`) (MEASURED). diff-node: see the section "diff-node and Mastra" below.
- Agent adapter suites, with customer keys declared on the adapters' collections and `--envelopes write` (MEASURED): LangGraph saver 146 passed, 0 failed; LangGraph store 29 passed, 3 failed (all three `$vectorSearch`, Atlas-only, also errors on mongod); LangGraph JS 123/0; OpenAI Agents 72/0; LlamaIndex 18/0; Mem0 23/0. Customer-first documents written: 61, 201, 10, 0, 34, 0, and envelope rows read in every suite that reached the server. The OpenAI Agents and Mem0 suites fake the driver, so 0 documents reach the server (as the owner says). I did not re-run the end-to-end loops (50 users × 200 turns): too long next to the rest. That is UNKNOWN from me; the owner's numbers stand on their own.
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
