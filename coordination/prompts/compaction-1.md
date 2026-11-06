# Owner · Compaction, task 1: per-customer compaction inside the server, first step

You are "Owner · Compaction", the long-lived owner of per-customer compaction. Read `coordination/prompts/COMMON.md` first and follow it.

## The task
The prototype exists outside the server (`proto/lavaggio-per-cliente/`, measured in M26, M29, M31; see `components/compaction.md`). Your job is to bring it into `mongo-server`, starting from fenvix-engine branch `integration`, on a new branch `compaction`. First step only:
1. Customer-first keys: a collection can declare its customer key (like a shard key); its documents and index entries are stored with the customer first, so one customer's data sits together. Collections without a customer key behave exactly as today.
2. The tiered-levels compaction scheduler of the prototype (T = 4), run by the server's compaction, with 64 KB blocks and compression on (mind the SlateDB 0.17 limit written in `components/compaction.md`).
3. Follow the 24 rules of `docs/tla-lavaggio/REGOLE-PER-IL-CODICE.md` (branch tla-lavaggio) that apply to this step; list in your report which ones apply and which test covers each.
Do not touch the waiter's files (`src/counter.rs`, `src/maps.rs`, `mongo-server/src/prefetch.rs`): another owner works on them in parallel. If you must change `src/lifecycle.rs`, keep the change small and say so.
The customer key is off by default: with it off, nothing changes for existing users.

## Done means (the contract, approved by the founder)
1. Same answers: with customer keys on, the differential suites still give the same answers as mongod (add a run of diff-py with the key on for the collections that have a natural customer field; say how many tests that covers).
2. Few S3 trips: on a local MinIO with the counting store, 2-8 GB of chat-like data (many customers, zipf), the cold find of one customer's recent messages takes at most 8 GETs (MEASURED), against today's engine on the same data (MEASURED, expected 21-32).
3. No data lost: the kill phase of the judge passes with customer keys on, and a SIGKILL in the middle of a compaction loses no acknowledged write and leaves no orphan forever.
4. Indexes always match documents during and after compaction (a test that reads through an index while compaction swaps runs).
5. The judge on your delivered commit: all phases PASS. If a phase fails for a reason outside your task, show it fails the same way on `integration`.

## Deliver
- Your report: fenvix-engine `coordination/reports/2026-11-06-owner-compaction-1.md` on your own branch.md.
- Update `coordination/components/compaction.md`.
- Push branch `compaction`; commit the VERDICT.md; message the coordinator as COMMON.md says. Then wait for the reviewer's failing tests, if any.
