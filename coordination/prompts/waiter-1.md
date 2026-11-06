# Owner · Waiter, task 1: make the waiter tools safe, and choice (b)

You are "Owner · Waiter", the long-lived owner of the waiter's read path. Read `coordination/prompts/COMMON.md` first and follow it.

## The task
Starting from fenvix-engine branch `integration`, create branch `waiter` and do:
1. The three corrections of the reviewer of T1, described in `components/waiter.md` (and in full in `sessions/2026-11-05-revisore-attrezzi-cameriere/RAPPORTO.md`, section 2).
2. The founder's choice (b) of 2026-11-06 (DECISIONS.md, top): only the stack maps (SST index blocks and filters) on the waiter's NVMe; collection-index blocks live on S3 like the data and go through the normal cache on demand.
Do not touch the compaction code (another owner works on it in parallel). If you must change `src/lifecycle.rs`, keep the change small and say so in your report.

## Done means (the contract, approved by the founder)
1. The test `rev_maps_leave_room_for_active_data` passes: active data always keeps a minimum share of the RAM, whatever the maps weigh.
2. After `dropDatabase`, the dropped database's maps leave the RAM and the disk cache within seconds (a test proves it, including that none of its index keys remain); maps of a database closed for long can leave RAM and then disk.
3. Loading maps has a cap on runs and requests in flight (a test proves the cap holds with a database much larger than the cap).
4. Choice (b) in place: a test with a counting store shows the first cold read of a never-seen customer takes at most 2 S3 trips in a row, and at most 1 once its collection-index blocks are cached.
5. The judge on your delivered commit: build, unit, diff-py, diff-node, ctl, kill all PASS. If a phase fails for a reason outside your task, show it fails the same way on `integration` and say so.

## Deliver
- Your report: fenvix-engine `coordination/reports/2026-11-06-owner-waiter-1.md` on your own branch.md.
- Update `coordination/components/waiter.md` (what exists now, numbers, tasks done).
- Push branch `waiter` of fenvix-engine; commit the VERDICT.md; then message the coordinator as COMMON.md says.
- Then wait. The coordinator will send you the reviewer's failing tests, if any; you make them pass, re-run the judge, and message the coordinator again.
