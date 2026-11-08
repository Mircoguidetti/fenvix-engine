APPROVED · round 2: R1-R5 pass on c195a2f and their controls behave as in round 1 · Q8d read fix: CORRECT and COMPLETE for what round 1 found (read your writes 0 misses, no dirty read through any read path I tried, the warm spare serves every acknowledged write after a live takeover with no sleep)

# Review round 2: the agent lane and the Q8d log-path fix

Reviewer session R-review-agent-lane, round 2, 2026-11-08. Delivery: fenvix-engine branch `log-path-fix` head c195a2f (engine code 56f9152), from my `review-agent-lane` 2cd218b. Owner's report: fenvix-brain `sessions/2026-11-07-log-path-fix/REPORT.md`. Scope as asked: ONLY my four FIX items (R2, R3, R4, R5) and the Q8d read fix (R1), plus the fast judge. Round 1: `sessions/2026-11-07-review-agent-lane/REPORT.md`. This branch: `review-log-path-fix` (from c195a2f). Spend $0, local only. Every number is MEASURED on this sandbox (4 cores) on the build of c195a2f.

My test files in `loadtest/review_agent_lane/` are byte-identical on c195a2f to 2cd218b (`git diff` empty): the owner did not change them.

## The five tests, unchanged

- R1 read your writes with the log on, lane off (100 rounds; nine read paths: point, index, range, count, aggregate, after update, after delete, after a transaction, inside a transaction): 0 misses on all nine. PASS (round 1: FAIL on integration d6ce3d4, PASS on 8fbdd8d).
- R2 an agent-lane write still waiting for its PUT, read with readConcern "majority" and watched by a change stream, then kill -9 (5 rounds): 0 majority reads of it, 0 change-stream events, 0 seen-then-lost. PASS (round 1: 3 of 3 seen then lost). Control `--control` (lane without the log), 3 rounds: seen 3, lost 0, PASS, as in round 1.
- R3 the spare's follower after dropDatabase and recreate: 0 dropped documents back, 0 of 5 acknowledged missing. PASS (round 1: 40 back, 5 missing). Control without the follower: PASS, as in round 1.
- R4 live takeover by the warm spare with the 3-copy log, kill at once (no sleep): 200 of 200 acknowledged default-path writes served by the spare; again with 1,000 writes: 1,000 of 1,000. PASS (round 1: 0 of 200, on 8fbdd8d and on integration).
- R5 the log's 3 copies frozen, a default-path write that the log never confirmed, majority read, kill -9 (5 rounds): 0 majority reads of it, 0 seen-then-lost. PASS (round 1: 3 seen, 2 lost).

## One more check of the dirty-read rule (new test R6, passes)

The fix is a per-key rule ("a read of a key ignores only in-flight batches that write that key"). R2 read by `_id` only, so I added `r6_visible_before_durable_scans.py`: R2 with a secondary index, and the unacknowledged write read through a full collection scan, a count by `_id`, a count of the whole collection, the secondary index and an aggregate `$match`. On c195a2f, 4 rounds: no read path returned it, 0 seen-then-lost (PASS). On 8fbdd8d, 2 rounds: 2 seen then lost (FAIL), so the test can see the hole. (A first version of my count check was wrong, it assumed the earlier ghosts survive; fixed before these runs.)

## Read your writes on the lane after the fix

The owner's t3, reduced (10,000 lane writes, 10,000 lane transactions, 2,000 default-path writes, 64 threads) on c195a2f: 0 misses each, and the spare after the takeover missing 0 (PASS). So the confirmed-state rule did not break read-your-writes for the writer's own acknowledged writes.

## The judge (lane off)

`judge/run.sh fast` on c195a2f (verdict in `judge/out/c195a2f/` of this branch): build PASS; unit 103 passed, 1 failed (`counter::tests::rev_maps_leave_room_for_active_data`, the known R-T1 test, failing on integration too); diff-py 412/413 and diff-node 412/413 (the one difference is `bucketauto.infinity` in both, as on integration); ctl PASS (3 files; the round-1 erase flake did not recur); kill PASS. The one extra passing unit test against 8fbdd8d is the fix's own unit test of the visible state. So identical to integration with the lane off.

## Notes (do not block; outside the round-2 scope, so not tested)

- The round-1 notes on cost (a transaction commit flushes at once, so about 1 / PUT latency PUTs a second, about 28 a second at 30 ms in t5, not the model's 10) and on the follower's idle GETs (about 21 $ a month per followed database even when idle, ESTIMATED) still stand; I did not re-read the cost model in this round.
- The round-1 note on `Tenant::close`'s unclean fallback with the log on was not in this round's scope; I did not check whether 56f9152 changed it.
- The fix is large (about 1,600 lines in `lifecycle.rs`, `locallog.rs`, `qlog.rs`, `dbs.rs`, `changes.rs`, `agentlane.rs`). I checked only behaviour through the tests above, not the code line by line, as the round-2 scope asks.
