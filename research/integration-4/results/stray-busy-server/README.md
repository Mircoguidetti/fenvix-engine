# A leftover diff-suite server spinning on the store (found 2026-11-12 07:14 UTC)

The mongo-tests servers of the full diff-node run (sealed2+derive env, ports 27018 and 27019, started 04:21 UTC) were
left running after the run. At 07:14 the one on 27018 used 90% of a core with no database open and 3 idle connections.

- MISURATO: 90,964 object-store GETs in 10 s (about 9,100 per second), 191 reads in flight, 100.3 million GETs in total
  since 04:21 (serverStatus fenvix.s3.get, fenvix.waiter.*: serverStatus-two-samples.txt).
- The log (server-27018.log): the last database (dn412) dropped at 04:58:39; `LEASE process_lease_lost ... abandoned=0`
  at 05:05:12. The process lease was lost because the bucket was deleted under it: research/integration-4/clean-minio.sh
  (between long runs) deletes every bucket of the judge's MinIO and recreates "bench" empty, and the server was still
  running. Not a production event as such (a bucket does not vanish), but a store that answers NotFound for the
  database's own objects is what the engine met.
- gdb backtraces (bt-full.txt, backtraces.txt): the busy tasks are SlateDB's compactor state refresh
  (`SimpleTransactionalObject<compactor_state::Compactions>::refresh` -> `try_read_latest` -> RetryingObjectStore GET via
  `counting::MaintenanceStore`), i.e. SlateDB compactor loops of databases that were closed or dropped are still running
  and re-read their .compactions object without a pause once it is gone.
- The 27019 server (3 s idle close) was at 0.3% CPU.
- Both were stopped (SIGTERM) at 07:20 after the evidence was saved. While it ran it took CPU from the runs of the
  morning (outage loop, ballast proof, test_52/52r): their timings from 05:05 to 07:20 are on a loaded machine.

Open question (to check): does a SlateDB compactor outlive its database's close or drop when the bucket is intact? If so it
is a leak that costs GETs in production too.
