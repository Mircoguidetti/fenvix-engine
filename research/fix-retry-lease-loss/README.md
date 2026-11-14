# fix-retry-lease-loss: a retried write reported failed but applied got E11000 (night-head 36c6a3e)

## Cause

`mongo-server/src/cmd_data.rs:772-776` (`Server::commit`): `g.apply(ops)` stages the command's batch (the documents and the
session's retryable-write record, one atomic batch). For a collection created by the write, `g.collection_uuid(coll)`
follows. During a process lease loss it finds the lease invalid, abandons the opening (the abandon flushes the WAL: the
staged batch lands), reopens the database, and its own write is refused (`Reopened`, `dbs.rs:1708`). `insert` returns the
error before `b.run.apply()` (`cmd_data.rs:885-887`); the reply waits for the staged commit, which fails ("db is closed"):
134 WriteOutcomeUnknown. The session's memory (`retry.rs`, `Sess`, already `loaded`) never got the inserts nor the
staged commit (`durable`), so the driver's retry (same lsid/txnNumber) was not answered from the record: it ran again and
got E11000 on `_id: 0`. Client told nInserted=0, 100,000 documents stored. The stored record was right; the memory was not.

## Fix

`retry.rs`, `Drop for Run`: a run whose record was staged (`Run::stage`) but not applied (`Run::apply`) drops the
session's memory (`Sess::new()`, keeping the command's staged commit as `durable`). The retry then reads the record back
from the database, waits for the staged batch's durability, and, the opening having been given up, reads it again from the
next opening (the existing P-fix-slow-store item 5 path): answered from the record (`retriedStmtIds`), nothing applied
twice. Counter `retryRecordsUnapplied` (serverStatus). Negative control `FENVIX_TEST_CONTROL_UNAPPLIED_RECORD_KEPT=1`.

## Tests (local, MinIO from source)

- ctl `test_93_retry_lease_loss.py`: base 36c6a3e fails (told 0, stored 100,000) in sealed2-derive and off; fixed: 2 passed
  in each mode (the main test and the control, which shows the defect with the variable).
- `test_52r::test_split_insert_many_with_driver_retries_throttled`: fixed, passed once per mode (base: 6/6 failed).
- `test_90 -k 'retried or definite or retryable'`: 4 passed per mode.
- `cargo test --release -p mongo-server`: 135 passed (3 new in `retry::tests`).
