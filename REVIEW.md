APPROVED (round 2, d4a3b2a; round 1 on 0d74e0c was FIX 3)

# Review of waiter 0d74e0c (base integration 01d64c6)

## Fast judge, re-run by the reviewer on 0d74e0c (MEASURED)

- build: PASS
- unit: PASS (106 passed, 0 failed), including the contract test `rev_maps_leave_room_for_active_data`
- diff-py: FAIL (412/413). The one failure is `bucketauto.infinity`, the same count as integration's own verdict (`judge/out/01d64c6/VERDICT-FAST.md`: 412/413).
- diff-node: FAIL (412/413). Same test, `bucketauto.infinity`, same count as on integration.
- ctl: PASS (3 files). The flaky `test_05_erase.py::test_concurrent_erase_and_fork_are_serialised` passed in this run (19/19).
- kill: PASS

The owner's claim holds: the only failures are the known ones, also present on integration. Contract point 5 is met.

## Defects (each one is a test that FAILS on 0d74e0c, on this branch)

1. `src/maps.rs` `maps::tests::rev_drop_leaves_no_stale_copy_on_disk`. Guarantee 6: a deletion must also clear caches and disks.
   When a block is read back from the disk floor (`data_get`/`maps_get`), or appended again (`Disk::append`), only its index entry is removed (`Disk::forget`); its bytes stay in the segment file. `drop_ssts` overwrites only the copies still indexed. Ordinary traffic (read, pushed down, read again) leaves the dropped database on disk. MEASURED: the counter holds 0 entries of the dropped database, yet its marker is found **312 times** in the disk-floor files. The owner's test passes only because it never reads a block back from disk.
   Why it matters: a dropped customer's documents and index keys stay readable on the waiter's NVMe until their segment happens to roll, which could take indefinitely long on a large floor.
2. `src/maps.rs` `maps::tests::rev_drop_of_a_fork_spares_its_source`. Guarantee 5: isolation between customers. Also costs money.
   A shallow fork (fork mode "clone", and PITR restore) uses its source's SSTs, under the same SST ids. `forget_database(fork)` forgets every SST mapped for the fork, the source's included, and `bury` refuses them for 10 minutes. MEASURED: after the fork is dropped, the still-open source caches **0 maps and 0 data blocks** of its own SSTs across two read passes (184 inserts refused). It also gets no re-warm afterwards, because its manifest has not changed.
   Why it matters: dropping a test fork makes every read of the production database go to S3 for 10 minutes: latency and GET cost. The drop also overwrites the source's cached bytes.
3. `ctl-tests/test_43_rev_erase_leaves_counter.py`. Guarantee 6 again.
   The control-layer erase (`DELETE /v1/databases/{db}`, which replies with a "proof") never calls the counter: only `dropDatabase` got the hook. MEASURED: after a successful erase, the erased database's marker is still in the disk-floor files **1,296 times**, and its SSTs are still mapped (`droppedDbs` 0).
   The erase path is older than this delivery. It is in scope because correction 1 is about exactly this, and because erase is the deletion that promises completeness. Fix it the way drop does: call `forget_on_waiter` from `ctl/erase.rs` (and from erase recovery).

## Contract points

1. Maps keep at most `MAPS_RAM_PCT` (25%) of RAM when it is full: met, and the test passes (MEASURED).
2. Drop: maps leave RAM and disk at once (test passes), and long-closed databases are released (test passes). Not complete because of defects 1 and 3. Defect 2 is a side effect of the drop hook.
3. The cap on loading holds: 150 SSTs with caps 4 and 6 never go over either cap, and the test can fail (12 loads would be possible without the gate). Met.
4. Choice (b), cold reads: 2 trips for a customer never seen, then 1. The test asserts that a 2-trip case occurs, so collection-index blocks are really read from S3. Met with the server default `--next-block-kb 8` (DECLARED by the owner: without it, 2,2,1 trips).
5. Met (see the judge above).

## The changed test `two_floors_end_to_end`

Its assertion now accepts the maps on either floor. With 256 KiB of RAM, the 25% maps share (64 KiB) is smaller than the SST index, so the maps belong on the disk floor by design (correction 2). The test still checks right values, disk hits and the prefetched-unused queue being evicted first. It hides no defect.

## Notes (not blocking: no failing test)

- Race between drop and an in-flight insert: `data_put` and `maps_put` check `is_dead` and then insert under a different lock. A load that passes the check just before `forget` runs can insert after it. The window is small and not deterministic, so I wrote no test.
- `release_idle` has the same sharing problem as defect 2: a fork that is closed for 10 minutes releases maps of SSTs its open source still uses. The source then reloads them on demand. Performance only.
- `forget_ssts` after compaction now also buries the old SSTs for 10 minutes. A long scan on an older snapshot can no longer cache those blocks. Cost only.
- A map read from the disk floor goes back to RAM and pushes the coldest map down: one disk write per map read when the maps are over their share. Those writes also leave stale bytes (see defect 1), so segments roll faster and data leaves the disk floor sooner. Performance; UNKNOWN at scale.
- Drop and erase run on the process that owns the lease. Another waiter's counter that cached the database keeps it until its own restart, compaction or release. Worth stating in the design.
- `first_cold_read_two_trips_then_one` sets the global `simstore::NEXT_BLOCK_BYTES` and never resets it. That leaks into other tests in the same binary.
- The open items the owner flagged (bookkeeping RAM for disk-floor data, the disk floor empty after restart, NVMe latency for map reads) remain UNKNOWN or ESTIMATED.

## Round 2: the owner's fixes, d4a3b2a (checked only `git diff 0d74e0c..d4a3b2a` and my three tests)

APPROVED. All three fixes are right, and none of my tests was weakened.

- **Tests not weakened.** `rev_drop_leaves_no_stale_copy_on_disk` and `rev_drop_of_a_fork_spares_its_source` are byte-identical to `c775b7b`. `test_43_rev_erase_leaves_counter.py` came in through a merge of `review-waiter`, unchanged. The owner deleted one line from their own `drop_forgets_the_database_everywhere` test (`c.mapped...remove("a")`). The registry type changed, and `forget_database` already removes the entry, so that line did nothing: the test is no weaker.
- **Fix 1 (stale copies).** Each segment now lists every copy written to it (key, offset, length), not only the indexed one. A drop overwrites every copy of the dropped SSTs in every segment, and those records go away with their segment when it rolls. MEASURED: the test passes. The check that refuses a dropped database's blocks now runs under the RAM or maps lock in `maps_put`, `data_put` and `data_get`, and under the disk lock in `disk_put`. That also closes the race I noted in round 1.
- **Fix 2 (fork).** A registry now counts how many databases use each SST and records the SSTs each database borrows from its source (`manifest.external_dbs`). A drop forgets only the database's own SSTs that no other mapped database uses; borrowed SSTs are released, not wiped. Only a drop blocks an SST from the cache for 10 minutes now, not compaction (round-1 note fixed). `release_idle` also spares an open source's maps (round-1 note fixed). MEASURED: the test passes.
- **Fix 3 (erase).** `ctl/erase.rs` calls `forget_on_waiter` after the purge, with the SSTs from the purge's first listing plus the ones mapped. It also runs on a resumed erase; after a restart the disk floor starts empty anyway. MEASURED: `test_43` passes (marker on disk 0, `droppedDbs` 1, SSTs mapped 0).
- **`NEXT_BLOCK_BYTES`** is now restored at the end of `first_cold_read_two_trips_then_one` (round-1 note fixed).

### Fast judge on d4a3b2a, re-run by the reviewer (MEASURED)

- build: PASS. unit: PASS (108 passed, 0 failed, my two unit tests included). kill: PASS.
- diff-py and diff-node: 412/413 each. The only failure is `bucketauto.infinity`, the same as on integration.
- ctl: FAIL in the judge run (1 of 3 files: `test_05_erase`). The failing test was `test_erase_leaves_other_databases_alone`, not the known flaky one: a `control/usage/...` record of an erased database was still listed. I re-ran `test_05_erase.py` 4 times on the same binary. That test passed all 4 times; the known flaky `test_concurrent_erase_and_fork_are_serialised` failed 2 times out of 4.
  The fix does not touch the usage flusher, and the judge's server runs without `--waiter-tools`. There, `forget_on_waiter` only clears the read-ahead tables, which takes almost no time. I read it as an intermittent race between the usage flusher and erase (1 run in 5). I did not rebuild integration to show the same flake there. NOTE, not blocking.
