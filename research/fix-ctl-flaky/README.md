# fix-ctl-flaky: three unclassified ctl tests on night-head 36c6a3e

Local runs, MinIO from source, release binary of 36c6a3e.

## test_52* (sealed2-derive): ENGINE defect

`test_52r_review_c2_fix_outage_loss.py::test_split_insert_many_with_driver_retries_throttled` failed 6 of 6 runs
(the full-file run and 5 single-test reruns): "partial call 0: told nInserted=0, stored 100000". The rest of test_52 and test_52r passed.

Minimal reproduction: `repro_52r_retry.py` (3/3: sealed2-derive and FENVIX_SEALED_RUNS=off). With the process lease's renewals
delayed past the TTL, the first insert command of a new database (100,000 documents, retryable writes on) is answered
`134 WriteOutcomeUnknown ... (Closed error: db is closed)`, yet it was applied. The driver retries with the same
lsid/txnNumber; the retry is not answered from the session record (no `retriedStmtIds`) but runs again and gets E11000 on
`_id: 0`: the client is told nInserted=0 while all 100,000 documents are stored. No acknowledged write is lost; the contract
broken is "a retryable write is applied once" (`error.rs::outcome_unknown`, `retry.rs::retry_begin`). Not fixed here.

## test_77* (sealed2-derive), test_51* (off)

Local runs: test_77 (both files) 13/13 once; test_51 (all four files) 16/16 in 3 of 3 runs. The CI's failure messages
("state ran ahead", "window not reached") were not seen locally, so neither is classified yet and no test was changed.
The local repeats were stopped at the coordinator's request (02:17 UTC); the repeats were sent to CI on night-head 36c6a3e:
test_77 x10 sealed2-derive, test_51 x10 off, the test_52r retry test x5 in both modes.
