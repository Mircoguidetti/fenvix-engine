# Components: one owner session per piece

Agreed with the founder on 2026-11-06 (see sessions/2026-11-06-verdict-atlas-infinite-1tb/WORKING-STRUCTURE.md).
- One owner session per component: it does the work the founder and the coordinator decide, and nothing else.
- The memory of the component lives in its file here, not in the chat. The owner updates it at every delivery: what the component does today, what is missing, the numbers (MEASURED / ESTIMATED / DECLARED / UNKNOWN), the open decisions, and the tasks done. If the session dies, a new one reads this file and continues.
- The referee is the judge (fenvix-engine `judge/run.sh`). A task is done when the judge says PASS on every phase for the delivered commit.
- The reviewer is a fresh session for every delivery, launched by the coordinator. It attacks the work and delivers only new failing tests, each with one line on why, or APPROVED. One round of fixes, then the reviewer re-runs the judge on the fixes only. A disagreement on whether a test is legitimate: one line each, the founder decides. Never a third round.
- Every owner starts from `main` of fenvix-engine (today: from `integration`, until it is merged), works on its own branch, delivers small pieces that go to main often. Two tasks that touch the same files run one after the other; the coordinator orders them.
- Everything new is in English (founder's language rule, 2026-11-06).

Components and their files:
- compaction.md: per-customer compaction (lavaggio). Owner session "Owner · Compaction".
- waiter.md: the waiter's read path, its cache (bancone) and maps. Owner session "Owner · Waiter".
- log.md (not opened yet): the replicated log (quaderno). Opened when we reach the write comparison with Atlas M30.

## Sessions running (launched by the coordinator, 2026-11-06 12:01 UTC)
- Owner · Waiter: session_01YAF6DgToUqaquNgdLqxrav (task waiter-1)
- Owner · Compaction: session_01YAF6Djm7cGfPDkt4RkhCe6 (task compaction-1)
- Review · L1: session_01YAF6Diz1DkDNLwaEkRauqf (review of L1 on integration)
- Coordinator: session_01YAF6DhosFhuf9bLj4PrApk
