APPROVED

# Review round 2: compaction 2 step 2, the fix round of FIX 3

Reviewer session, 2026-11-08. Scope, as the coordinator asked: only my 3 FIX items, the 2 extra places the owner fixed (`$merge` with `on: _id`, mapReduce `reduce` output), the updated E0, the two-release rollback test, the owner's claim about the 23 change-stream differences, and `judge/run.sh fast`. Delivery: fenvix-engine `c2-step2-layout` at dab7e3b (a fast-forward of my `review-c2-step2`, c844c37) and `c2-step2-layout-e0` at e9c1be6. Owner's report: fenvix-brain `sessions/2026-11-07-c2-step2-fix/REPORT.md`. Spend: $0, local only. Nothing merged, nothing deleted.

## My 3 tests and the other 9 (MEASURED)

- `ctl-tests/test_46_review_c2_step2.py` is byte-for-byte unchanged since my round 1 (`git diff c844c37 dab7e3b` on the file is empty).
- On dab7e3b, all 12 pass: the 3 that failed (updateLookup per customer, `documentKey` with the customer, the same string `_id` under two customers with a collation) and the 9 that passed before.

## The owner's new tests, and the 2 extra fixes (MEASURED)

- `ctl-tests/test_47_c2_step2_fix.py` has 7 tests: the `documentKey` shapes, updateLookup per customer, in a transaction and after a move, the collated `_id` with moves, deletes and transactions, `$merge` with `on: _id`, and mapReduce `reduce`. With my own build of the round-1 engine (c844c37) all 7 fail; on dab7e3b all 7 pass. So they measure the fixes, not something that is always true.
- I read the diff (7 files, +150 −49). The lookup takes (`_id`, customer) from the locator. `$merge` keeps only the target documents of the source's own customer. The collation `_id` index of a customer-keyed collection is now keyed per customer, like the locator. A plain collection keeps its old `idc/` bytes. I found nothing that contradicts the report.

## E0 and the two-release rollback (MEASURED)

- E0 (e9c1be6) still differs from the tip only by the `--envelopes` option (`main.rs`, 10 lines).
- `test_45` with my builds, E0 = e9c1be6 and OLD = `compaction` a7890b3: 3 passed. The old release still fails on an E1 database with "Malformed BSON bytes".
- The new rollback test with `E0_BEFORE_FIX_BIN` = my build of the round-1 E0 (5a17ea9): that build answers `[]` where 3 documents are expected. So E0 had to take the new collation format, and it has.

## The owner's claim on the 23 change-stream differences: confirmed

- Classification. With my own script on the owner's raw diff-py output (`bench/c2-step2-fix/diff-py.json.gz`): 21 of the `cs.*` differences differ only by `documentKey`, which is `{a, _id}` (the customer field first) where the unsharded mongod writes `{_id}`. 2 (`update_lookup_returns_current_document`, `update_lookup_sees_later_update`) update `a`, which moves the document, and ours then gives `fullDocument: null`. `cs.delete_many` is the old known difference (event order). Nothing else differs (MEASURED).
- mongos. I started the owner's local sharded cluster (`bench/c2-step2-fix/sharded_mongod.sh`, mongod 7.0.34, collection sharded on `a`) and ran `cs_vs_mongos.py` against it and against my own server with `--customer-key c=a`: 13/13 the same as mongos. That covers the event types, `documentKey` with its field order, `fullDocument` (null after a move included) and `updateDescription` (MEASURED).
- I read the script, and it is honest: real mongos on one side, our server on the other. One limit: it replays 13 hand-written shapes, not the 23 tests themselves. 10 of the 21 documentKey-only tests (`update_pull`, `update_pop`, `update_addtoset`, …) are not replayed one by one, but they differ in the same field the same way. ESTIMATED: they match mongos too.
- So the 23 are fix 2 itself, not a regression. Whether they become known differences with customer keys on, or go behind an option, is the coordinator's call (the owner's section 5). I agree with the owner's proposal: record them as known, with mongos sharded on the customer field as the change-stream reference when customer keys are on.

## Judge (MEASURED)

`judge/run.sh fast` on dab7e3b: build PASS; unit 114 passed, 1 failed (`counter::tests::rev_maps_leave_room_for_active_data`, known); diff-py 412/413 and diff-node 412/413 (only `bucketauto.infinity`, known); ctl PASS (3 files); kill PASS (`judge/out/dab7e3b/VERDICT-FAST.md`). Line for line the same as round 1.

## Notes (do not block)

- The `moved_doc` race stays, as both reports say. The owner proposes the natural sequence number for step 3. I agree.
- An app that updates the customer field and watches with `updateLookup` now gets `fullDocument: null` for that event, as on a sharded MongoDB. That is a behaviour change from an unsharded mongod. It belongs in the customer-key documentation.
