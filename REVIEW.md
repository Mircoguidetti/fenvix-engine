APPROVED

# Review of first-course-engine, round 2 (fix round 909f445)

Reviewer, 2026-11-07. Branch `review-first-course-engine` (fast-forwarded to 909f445). Round 1 (FIX 2) is in fenvix-brain `sessions/2026-11-07-review-first-course-engine/REPORT.md`. Owner's fix report: `sessions/2026-11-07-first-course-fix/REPORT.md`. As asked, I checked only the two FIX items, the owner's new tests and the judge. Spend: $0, local only.

## The two FIX items

1. Pending openings obey the size cap: fixed. After every write batch that touches a pending opening, and after every query it records, the same rule as for finished openings now runs (`Opening::fit`: trim, then drop if still too big; `settle_pending`, `after_stage`, `finish`).
2. Pending openings are counted in and evicted by the memory budget: fixed. Their bytes are in `STATS.bytes` (`start_pending`, `take_pending`, `resize`). `evict` pops pending and finished openings from one least-recently-visited index (`LRU`). The budget is now also checked after every write batch, not only when an opening is kept. Dropping a pending opening only means the user reads storage on the next visit, so it cannot cause a wrong answer.

## My tests and the owner's tests (MEASURED)

- `cargo test --release -p mongo-server opening::` on 909f445: 11 passed, 0 failed. That includes `review_a_pending_opening_obeys_the_size_cap` and `review_pending_openings_stay_inside_the_memory_budget`.
- My two tests are unchanged: `git diff 1941519 909f445` touches none of their lines, nor their helpers (`GLOBALS`, `Restore`, `review_*`). The owner only added tests after them. No existing test was weakened or removed.
- The owner's new randomized test (`fix_random_visits_and_writes_keep_cap_budget_and_exactness`) covers 3,000 steps over 30 users: visits of 1 to 4 queries, write bursts with deletes, failed queries and metadata writes. After every step it checks four things: the budget's count equals the bytes really held, every opening is under the cap, the total is under the budget, and every answer from an opening equals a model. It also asserts that it did serve from openings and did evict pending openings, so it cannot pass by doing nothing. The owner says it fails at step 0 with the pending budget update removed (DECLARED by the owner, not re-checked by me). `fix_eviction_takes_the_least_recently_visited_first` checks the eviction order.

## Judge

`judge/run.sh fast` on 909f445, rerun from a fresh setup (`judge/out/909f445/VERDICT-FAST.md` on this branch), MEASURED:
- build PASS.
- unit FAIL, 119 passed and 1 failed. The failure is `counter::tests::rev_maps_leave_room_for_active_data`, which is known on `compaction` and also failed on 847d188. The 4 extra passing tests are my 2 and the owner's 2.
- diff-py and diff-node FAIL, 412/413 each. The one difference is `bucketauto.infinity`, the same as on `compaction`.
- ctl PASS (3 files). kill PASS (0 lost inserts, 0 lost updates).
- Same as the owner's verdict on 67711d7.

## Notes from round 1 that still stand (none blocks)

- Single-query mode saves no GETs per whole visit on the final code (long gap: 12.2-12.8 against 11.0 on the baseline). Use the 4-query variant.
- The owner re-ran the 4-query variant on the fixed code: 100% short and long, 0 wrong (MEASURED by the owner). This closes my round-1 note that those numbers came from a binary before cf25457.
- Idle close forgets openings. Whether the medium and busy 1 TB customers are one database that never idles: UNKNOWN.
- New, from the owner's own report: pending openings have no age limit (only the budget removes them), and the eviction index sits outside the budget (up to about 200 MB at 658,000 openings, ESTIMATED by the owner). Both should go into the floor budget before production.
