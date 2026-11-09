# Checkpoint delta storage, local measurement ($0)

How many bytes do agent checkpoints take if the compactor stores only what changed between a
thread's checkpoints, against what the LangGraph savers store today? Prompt:
fenvix-brain `prompts/P-checkpoint-delta-local.md`. Report: fenvix-brain
`sessions/2026-11-09-checkpoint-delta-local/REPORT.md`. No engine code is touched.

## Setup

- Postgres 16.15 (Ubuntu package, `default_toast_compression = pglz`) on 127.0.0.1:5432, user
  `postgres`, password `pg`.
- mongod 7.0.34 from `judge/setup.sh`, standalone on 27017, `--wiredTigerCacheSizeGB 1`.
- Python 3.13 venv: `python3 -m venv .venv && .venv/bin/pip install langgraph==1.2.20
  langgraph-checkpoint==4.3.1 langgraph-checkpoint-postgres==3.2.0 langgraph-checkpoint-mongodb==0.5.1
  langgraph-checkpoint-sqlite==3.1.3 langchain==1.4.9 "psycopg[binary]" psycopg-pool pymongo zstandard lz4`

## Files

- `workload.py`: the corpus (Python standard library docstrings and code, so text and code are
  real), the seeded thread scripts, the LangGraph graph (scripted model, prebuilt `ToolNode`,
  `tools_condition`). Mode `default` uses `MessagesState` as shipped; mode `delta` uses the
  opt-in beta `DeltaChannel` with LangGraph's own `_messages_delta_reducer`.
- `run_savers.py MODE N THREADS`: runs the threads; a tee checkpointer sends every write to the
  real PostgresSaver, MongoDBSaver and SqliteSaver. Captures every row and document
  (`results/savers/<cell>/rows.csv.gz`) and the bytes on disk (`disk.json`).
  `run_all.sh` runs every cell (10 x 400, 40 x 150, 100 x 40, 300 x 8 threads, both modes,
  plus Postgres with lz4 TOAST).
- `options.py N`: our options on the real checkpoints of the default run (full zstd, binary
  delta, JSON-aware delta, full every K = 10 and 50, thread message log), with rebuild time and
  bytes for the latest and an old step; every rebuild is checked byte for byte.
- `log_codec.py N`: the message log with lz4 (the run format's codec today).
- `saver_reads.py N`: `graph.get_state` latency on the savers, both modes, and a check that both
  modes return the same messages.
- `saver_code.sh`: downloads the pinned saver releases and prints the quoted lines
  (`results/saver-code.txt`).
- `analyze.py`: mix, ratios and money (`results/summary.txt`, `results/summary.json`).

## Run

```bash
./run_all.sh
for n in 10 40 100 300; do .venv/bin/python options.py $n; .venv/bin/python log_codec.py $n; .venv/bin/python saver_reads.py $n; done
.venv/bin/python analyze.py
PATH=$PWD/.venv/bin:$PATH ./saver_code.sh
```

Seed 20261109. The scripts are deterministic; checkpoint ids (uuid6) and ToolNode's ToolMessage
ids are not, but their sizes are fixed.

## Message sizes (ESTIMATED)

User message median 250 B, final answer median 900 B, tool result median 1,800 B (heavy tail to
40 KB), 0-6 tool rounds per turn, 1-3 calls per round, OpenAI-style `response_metadata` and
`usage_metadata` on every model message. Check against the one public figure: the realistic mix
gives 2.9 MB per thread in the default Postgres saver, against 1.9 MB per thread in the LangChain
forum case (30 GB, 16,000 threads, DECLARED). Same order.
