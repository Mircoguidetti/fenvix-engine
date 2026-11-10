# Agent adapters on integration-2 (2026-11-09)

Engine: fenvix-engine `integration-2` at 7d7afde (the merge commit of step 7a; judge verdicts in 3f22474).
Reference: mongod 7.0.34 from `judge/setup.sh`. Suites: LangGraph saver, LangGraph store, LangGraph JS, OpenAI Agents,
LlamaIndex, Mastra (`run_suites.sh`; Mem0 not asked for).

- `mongod/`: the reference.
- `ours/`: our server, default options.
- `ours-all-on-sealed2/`: our server with every new piece on: `--customer-key <the step 2 list> --envelopes write
  --first-course --first-course-queries 4 --sealed-runs 2 --sealed-t-pin-ms 1000 --sealed-e1-wait-ms 0`.
  Mastra: one test more fails than on mongod, "bulk status update accepts more ids than one statement can bind", a
  timeout at 30 s (1,200 sequential inserts, a publish every 2 write batches).
- `ours-all-on-sealed50-mastra/`: the same options with `--sealed-runs 50`, Mastra only: the same as mongod.

Customer keys: `checkpoints, checkpoint_writes = thread_id; agent_sessions, agent_messages, sessions, test_chats,
test_ttl_chats = session_id; mastra_messages = thread_id; mastra_threads, mastra_workflow_snapshot,
mastra_observational_memory = resourceId; mastra_resources = id` (the store's and the time-travel collections are not
declared: their names were not in the brain).
