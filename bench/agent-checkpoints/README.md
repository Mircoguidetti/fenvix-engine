# Agent checkpoint benchmark (Part B)

What it answers: for an app that keeps every agent checkpoint forever, how fast and how expensive is Fenvix compared with
the stores the buyer actually compares us with? About 90% of LangGraph checkpointer downloads are Postgres (MEASURED via
pypistats, early Nov 2026, 30 days: postgres 6.6M, sqlite 2.2M, redis 415.8K, mongodb 273.3K), so the comparison is
Fenvix vs the Postgres checkpointer on Aurora, Neon and Supabase, and vs Atlas.

Everything goes through the **real framework packages**: `langgraph.checkpoint.mongodb.MongoDBSaver` (Fenvix, mongod,
Atlas) and `langgraph.checkpoint.postgres.PostgresSaver` (Postgres, Aurora, Neon, Supabase), driven by a **real compiled
LangGraph graph**; Mastra's `@mastra/mongodb` and `@mastra/pg` stores in `mastra/`.

## Files

- `generator.py`: the workload (fixed seed, deterministic): users, threads, turns, gaps, message contents.
- `graph_runner.py`: the LangGraph agent graph (agent + tools nodes, scripted model) and `TimedSaver`, which times every
  saver call and counts the logical bytes LangGraph hands to the saver.
- `targets.py`: how each target is built and how it reports stored bytes. `fenvix_saver(uri)` is the function a future
  thin `fenvix` package would wrap.
- `bench.py`: write, read, sizes, delta test, erase; writes `ops.jsonl` (every operation, timestamped) and `summary.json`.
- `delta_hook.py`: the "agent checkpoint delta test" (per-thread logical vs stored bytes; Fenvix part is a TODO with
  its interface, see below).
- `cost.py` + `prices.json`: cost per million steps and per year of history, arithmetic printed, every price labelled.
- `report.py`: prints one results directory with labels.
- `run_local.sh`: the local validation ($0): MinIO + mongo-server, Postgres 16, mongod 7.0, all three targets + Mastra.
- `mastra/bench.mjs`, `mastra/sizes.py`, `mastra/package.json`: the same turns through Mastra's stores.
- `runbooks/`: the AWS round, one file per target, plus common rules.
- `requirements.txt`: the Python packages (versions of the validation run).
- `results/local-2026-11-09/`: the local validation run (VALIDATION, NOT RESULTS).

## How to run

    judge/setup.sh                                    # mongod 7.0, MinIO, python 3.12 (once)
    cargo build --release -p mongo-server
    bench/agent-checkpoints/run_local.sh              # profile small, seed 42, about 4 minutes, exit 0 = all ok
    PROFILE=tiny MASTRA=0 bench/agent-checkpoints/run_local.sh

One target by hand: `python bench.py --target fenvix|mongo|postgres --uri URI --profile small --out DIR [--local]`.
Workload shape only: `python generator.py --profile standard --describe`.

## What is measured

Per operation, p50/p90/p99 (ms, client included):
- **save one step** (`save_step`): the `put_writes` calls of one LangGraph superstep plus its `put`. Also `put` and
  `put_writes` alone. `durability="sync"`, so each save is complete when timed.
- **resume a thread**: the `get_tuple` without checkpoint_id that LangGraph itself does at the start of every turn,
  split by the gap before the turn (first, none, short, long); and again in the read phase on sampled threads.
- **time travel to step k**: `get_tuple` with a checkpoint_id, 3 random steps per sampled thread.
- **list history**: `list` of the whole thread and `list(limit=10)`.
- **erase one end user**: `delete_thread` for every thread of the user, timed as a whole, then a check that nothing of
  those threads can be read back (`threads_left_readable` must be 0).
- **bytes stored per logical byte**: logical = the checkpoint, metadata and pending writes LangGraph handed to the saver,
  serialized with the saver's own serializer. Stored: Fenvix = S3 objects under `tenants/app_demo/` (live, after the
  tenant's idle close, after a clean stop); mongod/Atlas = `dbStats` storageSize + indexSize after `fsync`; Postgres =
  `pg_total_relation_size` of the 4 saver tables. Also "unique message bytes" = the final message list of each thread,
  the lower bound a perfect delta encoding would need.
- S3 PUT requests during the write phase (Fenvix, from `serverStatus.fenvix.s3`).

`bench.py` uses one client by default (`--workers N` splits users across N connections).

## Workload shapes and where they come from

- **One checkpoint per superstep, whole message list each time.** LangGraph writes a checkpoint for the input and after
  every node; `MessagesState` uses the `add_messages` reducer, a full-copy channel, so storage grows with the square of
  thread length (DECLARED, LangChain forum). Per turn: 2 + 2 x tool rounds checkpoints. Confirmed in the run: 2,551
  checkpoints carry 142 MB of logical bytes for 2.36 MB of unique message content (1.66%).
  Note: LangGraph 1.2.20 also ships a **beta `DeltaChannel`** that stores only deltas (seen in
  `langgraph/channels/delta.py`). It is not the default and is not used here; if it becomes the default the square
  growth disappears for LangGraph users, which weakens the storage argument. Worth tracking.
- **Typical long chat thread about 1.9 MB.** One team's data: 30 GB over 16,000 threads = about 1.9 MB per thread
  (DECLARED, LangChain forum case). Chat threads: turns lognormal, median 7, sigma 0.9, max 80; 0-1 tool calls per turn;
  answers 500-3,500 chars, user messages 40-600, tool outputs 300-3,000. Result for profile `standard` (ESTIMATED by
  `--describe`, content only, before serializer overhead): chat p50 0.17 MB, p90 1.64 MB, mean 0.82 MB; all threads
  together mean 1.79 MB per thread.
- **Coding-agent session 5-13 MB.** 5-13 MB per session, 0.2-1.1 GB per developer-month (ESTIMATED from one
  developer's DECLARED numbers). Coding threads (15% of threads): turns median 10, sigma 0.35, max 20; 2-4 tool calls per
  turn with outputs of 1,000-11,000 chars of code-like text. Result: p50 5.5 MB, p90 12.7 MB (ESTIMATED by `--describe`).
- **Users and threads.** Threads per user geometric, mean 2.5 (assumption). Profiles: `tiny` 4 users, `small` 20 users
  (41 threads, 599 turns, 2,551 checkpoints), `standard` 1,000 users (2,453 threads, 25,398 turns, 96,754 checkpoints,
  4.4 GB of full-copy content).
- **Most threads idle, returning users.** A thread's turns are split into sessions (mean 1.8 sessions per thread); a
  return comes after a short gap (5-60 min, 60%) or a long one (1-30 days, 40%) (assumptions). Users start over a 14-30
  day window. Events are sorted on a virtual clock and replayed interleaved, so a returning thread has been out of every
  cache for as long as the interleaving allows. Gaps are not slept on.
- **Thinking time 2.3-8 s between agent steps** (M25, MEASURED earlier) is put on the virtual clock between supersteps,
  not slept on: the benchmark measures the database, not idle time. A run that wants real idle (for example, to let our
  server close a tenant after `--idle-secs`) needs a sleep option, not built.
- **Text that compresses like text.** Word-based English-like text from a fixed vocabulary with Zipf weights, and
  code-like lines for coding tools; random bytes would make every target's compression look useless.
- AI messages carry `response_metadata` and `usage_metadata` like a real model's (model name, finish reason, tokens).

## Local validation (2026-11-09): VALIDATION, NOT RESULTS

One sandbox shared with another session's server and MinIO load (so latencies are noisy), client and databases on the
same machine, MinIO instead of S3, profile `small`, seed 42 (41 threads,
599 turns, 2,551 checkpoints, plan digest 1618df2b0b847193, identical across runs). Engine commit of `step9-prep`
(see `results/local-2026-11-09/ENVIRONMENT.txt`). Every number below is MEASURED locally and is only a check that the
harness works; `results/local-2026-11-09/REPORT.txt` has all of them.

- All three LangGraph targets: `ok=True` (no error, 0 inconsistencies between `get_tuple`, `list` and time travel,
  0 threads readable after erase). Mastra on all three: ok (save one message p50/p99: Fenvix 6.7/13.4, mongod 1.6/3.3, Postgres 1.8/5.0 ms).
- Save one step p50/p99: Fenvix 10.4/38.1 ms, mongod 2.7/9.1 ms, Postgres 4.7/19.6 ms.
- Resume (next turn) p50/p99: Fenvix 2.5/7.3, mongod 2.2/6.2, Postgres 1.6/7.5 ms. After a long gap: Fenvix 7.2/11.1,
  mongod 2.0/2.7, Postgres 0.8/1.8 ms (n=8).
- Time travel p50/p99: Fenvix 2.4/6.5, mongod 1.7/6.9, Postgres 1.0/7.7 ms.
- List whole history p50/p99: Fenvix 143/1,331, mongod 53/966, Postgres 26/958 ms (the longest threads hold up to
  tens of MB of checkpoints; `MongoDBSaver.list` also runs one writes query per checkpoint).
- Erase one end user: Fenvix 71 ms, mongod 11 ms, Postgres 7 ms (n=2, 1-2 threads each).
- Stored per logical byte: mongod 0.47 (WiredTiger, snappy), Postgres 0.39 (TOAST pglz). Fenvix: 2.08 live (WAL
  objects not yet garbage-collected; 2.96 in an earlier identical run), **1.02 after the tenant's idle close and after a clean stop** (145 MB of
  `compacted` objects for 142 MB logical): our stored data is essentially **not compressed** today. On this workload
  that makes us store 2.2x mongod's bytes and 2.6x Postgres's, before any price is applied.
- After the erase, the deleted threads' bytes stay in Fenvix's compacted SSTs until compaction drops them (live total
  296 MB -> 279 MB, all WAL GC); Postgres keeps the space (no VACUUM FULL), mongod's figure even grew 67 -> 82 MB
  (WiredTiger checkpoint timing), as expected for both.
- S3 PUTs: 4,579 during the write of 2,551 checkpoints = 1.8 PUT per checkpoint (on MinIO, single client).
- Mastra on Fenvix: 1,952 messages left 3,995 `wal` objects (6.8 MB) next to 3.9 MB compacted after a clean stop;
  WAL objects were not garbage-collected at stop time. Mastra stores each message once (append-only), so its logical
  bytes are 4.1 MB vs LangGraph's 142 MB for the same conversations.
- Cost (cost.txt, ESTIMATED from these local bytes): storage of 1 M steps kept a year: Fenvix S3 floor $15.71
  (56.9 GB x $0.023 x 12), Aurora Standard $25.97 (+ $1.20 I/O), Supabase $32.47, Aurora I/O-Optimized $58.44,
  Neon $90.91, Atlas Infinite $94.94. Compute is UNKNOWN for everyone but Atlas (instance prices not opened).
  Fenvix's figure is a cost floor, not a list price.

## What is missing

- Instance prices (Aurora, Supabase, Neon, our machine) and Atlas Dedicated extra storage: UNKNOWN; the vendor pages
  were not reachable from the sandbox (see `prices.json`). Fill before the AWS round.
- `fenvixSstInfo` does not exist: the per-thread stored bytes on Fenvix are UNKNOWN (`delta_hook.py` documents the
  interface for compaction 2 steps 4-8 and already calls it, so it starts working when the command lands).
- No compression of stored data on our server (finding above) and slow WAL garbage collection after small writes:
  both to raise with the compaction owner.
- Real idle (sleeping through gaps), concurrency beyond `--workers` threads, and LangGraph JS were not run. The
  LangGraph JS MongoDB saver is covered for correctness by branch `agent-adapters`.
- Mastra's stored-size figures are after its erase phase and include every `mastra_*` table it creates; Mastra time
  travel does not apply (no per-step checkpoints for agent memory).
- TTL: the Python `langgraph-checkpoint-mongodb` 0.5.1 used here already takes a `ttl` argument; the "1.4.0" version
  cited for TTL was not checked (it may be the JS package). Not used: the benchmark keeps everything.
