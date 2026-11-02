# fix-lease-stall: the engine stuck after losing its lease to a store outage

Found by the review of S7 (fenvix-brain `sessions/2026-11-01-revisore-s7/RAPPORTO.md`, point 1): with the object store down
for longer than the lease (40 s against 30 s) the process loses its lease, then the database never answers again and SIGTERM
does not stop the process, even after the store is back. Same on main and on the fix-stall branch.

## Reproduction

`tools/lease-stall/repro.py`: a TCP proxy in front of the store (`tools/lease-stall/blockproxy.py`) that, while frozen, accepts
connections but forwards no byte (the store hangs, it does not refuse). 16 threads insert batches of 500 documents of ~1 KB
(`{w:1, j:true}`), the proxy freezes 40 s, lease 30 s, then 15 s of recovery and SIGTERM.

On main (0a12e39), store = moto, 2 vCPU: SIGTERM did not stop the process in 4 of 6 runs; in 2 of them no insert was
acknowledged any more after the store came back.

## Causes (two waits without an end)

Diagnosed with prints around every await of the close, abandon, upgrade and compaction paths (not committed).

1. **`Tenant::abandon` waited forever for room in L0.** When the lease lapses the tenant is abandoned (lost-lease handler,
   or `Entry::ensure_open` finding the lease invalid). `abandon` removed the writer from the L0 watch, then called
   `Db::close()`, whose default is a final memtable flush. With L0 at `l0_max_ssts` that flush waits for a compaction, which
   nobody asks for any more (the writer is out of the watch): seen as "abandon close begin" with no end. The caller holds the
   database's entry lock, so every later command on the database, and `Dbs::close_all` at SIGTERM, waited behind it.
2. **A compaction pass never ended.** `compact_tenant` runs slatedb's `Compactor::run` in a task and, after its deadline,
   calls `Compactor::stop` then awaits the task. `stop` only cancels the tasks `run` has already registered (slatedb 0.17
   `dispatcher.rs`: `cancel_task`/`join_task` are no-ops for an unknown name). With the store down, `run` was still in its
   first manifest reads when `stop` ran; it then registered its tasks and waited for them forever. The pass kept its
   maintenance worker (2 per process) and the tenant's maintenance lock, and `Maintenance::shutdown` (after SIGTERM) waited
   for it: seen as "tenants closed; stopping maintenance" with no "maintenance stopped".

## Fix (`src/lifecycle.rs`)

1. `abandon` flushes the WAL (no room in L0 needed; it still uploads what the local log or relaxed durability acknowledged before the WAL write), closes with `CloseOptions { flush_type: None }` and waits at most `ABANDON_CLOSE_TIMEOUT` (10 s), then leaves the
   database behind (counted in `tenantStats.maintenance.abandonCloseTimeouts`). Nothing acknowledged is lost: acknowledged
   writes are in durable WAL objects, abandon writes no clean marker, so the next owner replays the WAL (checked by the unit
   test and by test_31).
2. `compact_tenant` calls `stop` again every second until the `run` task has returned (at most 120 s, then the task is aborted
   and the pass fails).

## Tests

- Unit (`cargo test --lib -p tenant-bench`): `abandon_does_not_wait_for_a_full_l0` (writer with L0 full and a durable write
  in the memtable: abandon returns at once, a writer opened afterwards reads the write) and
  `compaction_pass_ends_when_stopped_while_starting` (store frozen 2 s while the pass starts, pass deadline 0.5 s: the pass ends
  once the store is back). Both fail without the fix (negative controls run by hand: both time out).
- `ctl-tests/test_31_lease_lost_store_down.py`: the repro scaled to a 6 s lease, 1 MiB memtables, 40 s outage. Checks a new
  insert after the store is back, tenantStats, SIGTERM within 30 s, and every acknowledged document after a restart. Main:
  fails 3 of 3 (the insert after the outage times out). Fix: passes.

## Not fixed here (separate)

- During a SIGTERM shutdown, commands still arriving on open connections reopen a database after `close_all` (seen as
  `writer_open` after "tenants closed" in 2 of 3 runs). The process then exits with that writer open, an unclean close (its
  WAL is replayed at the next open, nothing acknowledged is lost). Fixing it touches `dbs.rs`/`main.rs`.
- Inserts that were in flight when the database was abandoned get "Closed error: db is closed" with code 1 (InternalError),
  not a retryable code.
- The process-lease renewal PUT waits for object_store's 30 s request timeout, so with a short lease the lapse is noticed by
  the renewer only after 30 s (commands notice it earlier through `Leases::valid`).
