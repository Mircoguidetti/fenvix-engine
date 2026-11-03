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
2. `compact_tenant` calls `stop` again every second until the `run` task has returned (at most `COMPACTOR_STOP_LIMIT`,
   120 s, then the task is aborted and the pass fails). Each `stop` is itself bounded to 1 s: once `run` has registered its
   tasks, `stop` cancels them and waits for them, which lasts as long as their store request (object_store's 30 s timeout
   and its retries), so without that bound the 120 s were not a real limit (review of N4, point 2) Found with it: the
   pass's own polls of L0 (through the raw store) were not bounded by the pass deadline, and a failed poll returned from the
   function without stopping the compactor at all; now each poll is bounded by what is left of the pass and a failed poll
   goes on to the stop.

### After the review (fenvix-brain `sessions/2026-11-02-revisore-fix-lease-stall/RAPPORTO.md`)

3. **Erase and an unfinished close.** `ctl/erase.rs` abandons the tenant, then deletes every object of the database. A close
   left behind keeps slatedb's tasks alive (slatedb 0.17 has no `Drop` for `Db`), and within the same process nothing fences
   them, so a WAL object or an SST could land under `tenants/<db>/` after the purge (an orphan, replayed into a database
   created again with the same name). Now `Tenant::abandon` returns whether the close finished; a close that did not finish
   goes on in its own task (so slatedb's tasks do stop once the store answers again) and `lifecycle::close_pending(id)` is
   true until it ends (`tenantStats.maintenance.abandonClosesPending` counts them). Erase stops before touching any object
   when the close did not finish or one is still pending: `503 close_pending`, its intent record removed, the client retries.
4. **What the limits do not stop.** When a compaction pass gives up (`runner.abort()`), the pass ends (its worker and the
   maintenance lock come back), not slatedb's compactor tasks: they run on their own tokio tasks and stay alive until their
   store request ends. Likewise an abandoned close left behind stays alive until the store answers.

**Longest shutdown with a store that never comes back** (estimated from the code): `Maintenance::shutdown` waits for a
compaction pass in flight: up to its 30 s deadline, then up to `COMPACTOR_STOP_LIMIT` (120 s) plus one round (2 s), so about
152 s; the abandon of each tenant is at most 10 s, and closes left behind do not delay the exit. Not counted: the ordinary
store requests before and after the compactor runs (first L0 count, checkpoint cleanup), each bounded only by object_store's
own timeout and retries.

## Tests

- Unit, after the review: `compaction_pass_ends_when_stop_hangs` (the store freezes at the compactor's first output write,
  so `stop` waits on a request that never ends; the pass must end at its stop limit, store still down) and
  `abandon_reports_an_unfinished_close` (store up: `abandon` returns true; store down: false after 10 s and `close_pending`
  stays true until the store is back and the close ends).
- `test_31 ... test_erase_does_not_delete_while_the_close_is_unfinished`: the store holds only the WAL PUTs of the
  database (`tenants/<db>/wal/`, `tools/lease-stall/pathproxy.py`), relaxed durability keeps acknowledged writes in the WAL
  buffer; the erase must answer 503 close_pending and delete nothing, then succeed and leave nothing once the store answers.
  Negative control (erase check removed): the erase answers 200 with its proof of zero objects, and once the store answers
  a manifest and an 82 KB WAL object of the erased database appear under `tenants/<db>/`; creating the database again then
  fails with 409 (the name looks taken) and reading it fails on a missing SST.
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
