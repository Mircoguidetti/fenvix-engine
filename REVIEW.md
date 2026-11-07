FIX 2

# Review of first-course-engine 847d188 (the opening kept for a returning user)

Reviewer, 2026-11-07. Branch `review-first-course-engine` (based on 847d188). Base `compaction` a7890b3. Contract: `prompts/P-first-course-engine.md` (fenvix-brain). Owner's report: `sessions/2026-11-06-first-course-engine/REPORT.md` (fenvix-brain). Spend: $0, local only.

In one line: no wrong answer found. The exactness design holds up under every check below. The two failing tests are both about memory in the 4-query variant (`--first-course-queries N`, N > 1), which is the variant the report recommends: an opening that is still being built has no size cap and is not counted in the memory budget.

## Failing tests (commit 1941519, `mongo-server/src/opening.rs`, end of the test module)

Run: `cargo test --release -p mongo-server opening::` gives 7 passed, 2 failed (MEASURED).

1. `review_a_pending_opening_obeys_the_size_cap`. With N > 1, a visit of fewer than N queries leaves its opening "pending" until the user's next visit, which can be 14 days later. Every write to the user's key ranges is applied to the pending opening, and nothing checks its size (`after_stage` trims and drops only finished openings). The test runs one query, then sends 1,000 messages of 1 KB to that user: the pending opening holds 1,068,049 bytes against a 65,536-byte cap (MEASURED). Why it matters: a user who receives a lot of data between visits keeps all of it in the waiter's memory. At 1 TB with users of 512 KB, that can be the whole user for every pending opening.
2. `review_pending_openings_stay_inside_the_memory_budget`. The budget (`--first-course-mb`, 1 GiB by default) counts and evicts only finished openings: pending ones are not in `STATS.bytes`, and `evict` does not look at them. The test runs 200 users, one query each, then 20 KB of messages each, with a 1 MiB budget: pending openings hold 4,281,800 bytes and nothing is evicted (MEASURED). Why it matters: memory grows past the budget with no limit. The report's 1 TB footprint (18-27 GB against a 29 GB budget) assumes the budget holds.

Possible fix (the owner decides): apply the same trim/drop rule to pending openings, count them in `STATS.bytes`, and let `evict` remove them (dropping a pending opening is always safe: its user just gets no opening). One more line in my commit: the existing test `a_visit_of_three_queries_is_kept_and_follows_writes` now takes a test lock, because my tests set the same global settings and cargo runs tests in parallel.

## The six questions of the review prompt

### 1. Does every write path go through the one hook?

Yes, by code inspection. I did not write a test per path, because they all end in the same function. What I checked:
- In `mongo-server`, the only call of `Tenant::stage` is `Entry::apply_mode` (`dbs.rs`), which runs the hook. `Tenant::apply` and `Tenant::put` (which also write) are never called by the server. `Entry::apply` and `Entry::apply_durable` both go through `apply_mode`.
- Transactions: commit stages the whole batch through `g.apply` (`txn.rs`). Abort stages nothing. A find inside a transaction is never answered from an opening (`cmd_data.rs`, only when `tx.is_none()`).
- Multi-document updates and deletes, update pipelines, upserts, findAndModify and bulk writes all build one `Writes` batch (`retry.rs` only adds its session record to that batch), and the batch ends in `g.apply`. The same holds for capped collections and index commands (`capped_cmds.rs`, `cmd_index.rs`). TTL deletes: `ttl.rs` calls `g.apply`. Cross-database rename and `$out`/`$merge`: `xdb.rs` calls `dst.apply`.
- Collection drop and rename, index create and drop, collection options: they write `meta/collections`, `meta/indexes/` or `meta/copt/`, and the hook then forgets every opening of the database. The list of metadata keys in `opening.rs` is the same as `is_meta_key` in `dbs.rs`.
- dropDatabase, idle close, lost lease, wake and create: all six places that set or take `Entry::tenant` call `forget_db`. PITR restore and fork first quiesce and close the tenant, so they forget too.
- Writes replayed on the spare after a failover: the spare's process never had openings, and the old owner forgets its own when it loses the lease (`abandon`) or wakes again (`ensure_open`). Local-log records replayed at restart go through `apply_durable`, so through the hook.
- What is tested end to end: `stress.py` (inserts, text updates, timestamp moves, deletes, documents moved to another user, array user fields), and the owner's negative control (hook switched off) shows the test finds wrong answers. Not tested end to end: transactions, findAndModify, upserts, bulk writes, TTL, drop, rename, index changes. They are safe only because they share the one function. NOTE: a future write path that calls `Tenant::stage` directly would bypass the hook without any test failing. A debug assertion or a comment on `Tenant::stage` would protect it.

### 2. The "complete range" rule

I found no way for a key to be missing:
- A write between recording and hook: not possible. A recording runs entirely under the database's shared lock (`first_course_end` runs before the `ReadView` is dropped), and every write stages under the exclusive lock. The hook's capture (before `stage`) and its apply (after `stage`) both run under that same exclusive lock.
- A trim cutting a range: a trimmed interval is cut to `[lo, succ(edge))`, and every key inside it is kept, because covered keys are never dropped. The randomized unit test (400 rounds, trims included) checks this against a model and passes.
- Another user's key in the middle of a recorded range: user ranges are prefix-free (`custkey::comp` is length-prefixed and gives the same encoding for 1, 1_i64 and 1.0). Other users' keys cannot land inside. A document of user V whose user field is an array that includes U puts an index entry in U's range. That entry is a write to U's range, so the hook applies it. The document itself lives outside U's range, so it is "not known" and is read from storage.
- Durable reads: an opening is answered from only when every staged write is durable, so a non-durable write is never shown.

### 3. The fix in cf25457 (writes skipped while an opening was being built)

Complete for exactness, as far as I can see. The hook now looks at pending openings too (`write_hook` no longer returns early when only pending ones exist, and `after_stage` applies writes to them). Queries of one visit that run at the same time all hold the shared lock, so they see one state, and merging their recordings is exact. A failed query, a non-durable state or a metadata write drops the pending opening. What the fix does not cover is memory: failing tests 1 and 2.

### 4. Memory

- Finished openings: the 256 KB cap, the trim and the least-recently-visited eviction work as described.
- Pending openings (N > 1): no cap, no budget, no age limit (failing tests 1 and 2).
- NOTE, cost at scale: `evict` scans every opening of every database to find the oldest one, under the global `REG` mutex, which the write hook also takes on every write of every database. With the budget full (steady state at 1 TB: the report counts 658,000 openings, ESTIMATED), each new opening triggers a full scan while all writes of the process wait. Fine for a prototype; it needs an LRU list before production.
- NOTE: the per-database `access` map and the `REG` entries of dropped databases are never removed. Small (one timestamp per user).
- Idle close: confirmed. `Entry::close` calls `forget_db`, so a database that is idle for 60 s (`--idle-secs`) loses every opening. For quiet databases this cancels the idea; the report says so. Whether a medium or busy 1 TB customer is one database that never idles, or many small databases that do: UNKNOWN. That decides whether the dollars hold.

### 5. Hot reads

Confirmed from `bench/first-course/results-2026-11-06/summary.txt` (MEASURED by the owner; I did not re-run the 2 GB bench). Single-query mode: the visit's later queries have a p50 of 11.8 ms against 3.5 ms on the baseline (short gap). The 4-query variant removes it: 1.16 ms p50, 1.98 ms p99 (short), and 0.00 GETs per later query.

Two NOTES on the numbers:
- The final code (cf25457 and 2748e34, runs `on-long-commit` and `on-long-commit2`) is worse than the report says for single-query mode on the long gap: 12.2 and 12.8 GETs per whole visit against 11.0 on the baseline, and later-query p50 15.0 and 20.3 ms against 10.1 ms. The report's "long 5.6 against 11.0" comes from the older run `on-long` (c6032e3). So, kept as specified, idea 1 saves nothing per whole visit on the final code. This makes the 4-query variant necessary, not optional.
- The 4-query numbers (`q4-short`, `q4-long`) were measured on c6032e3, before the cf25457 fix of the bug that affects exactly that variant. They showed 0 wrong answers, probably because the test users are not written during visit 1. The headline "a whole visit costs about 0 GETs" should be re-measured on the final commit, together with failing tests 1 and 2, before it goes into the numbers.

### 6. Judge

`judge/run.sh fast` on 847d188 (engine code identical to 0ca5697), MEASURED:
- build PASS.
- unit FAIL, 115 passed and 1 failed. The failure is `counter::tests::rev_maps_leave_room_for_active_data`, which is known on `compaction` (its fast verdict on 1ae8158: 108 passed, 1 failed, the same test). The 7 extra passing tests are the first course's own. So, as the owner says, "unit PASS" is not met, for a reason outside this task.
- diff-py and diff-node FAIL, 412/413 each. The one difference is `bucketauto.infinity`, the same as `compaction`.
- ctl PASS (3 files). kill PASS (0 lost inserts, 0 lost updates).
- Identical to the owner's verdict on 0ca5697. Every failure is a known one on `compaction`. My verdict file: `judge/out/847d188/VERDICT-FAST.md` on this branch. With my two tests added, unit would show 2 more failures: those are the FIX 2.

## Other notes (none blocks)

- With `--wal-flush-ms` above 0 (EXPLICIT_FLUSH off) and durable reads on, an opening never answers (`all_durable` is false). It is safe. If production uses a flush interval, the feature is off in practice. Which flush setting production will use: UNKNOWN to me.
- With the metadata cache off (`META_CACHE`), metadata reads would go through the recording and make every opening unkeepable. It is safe, just never served.
- One long-gap miss (1 of 1,000) is still unexplained by the owner. It is not a wrong answer (exact, read from storage).
- The tests do measure what they claim: the visit-2 reference read uses `{userId: {$in: [u]}}`, which `customer_value` never accepts, so it always reads storage. The second check (an exact model) does not depend on the server.
