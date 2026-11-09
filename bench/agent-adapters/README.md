# Agent memory adapters against mongo-server

Do the MongoDB adapters of the agent frameworks work unchanged against our server? Each adapter's own published test suite
and a small agent loop run against mongod 7.0 (the reference from `judge/setup.sh`) and against mongo-server, same machine,
same port; only the difference counts. Report: fenvix-brain `sessions/2026-11-06-agent-adapters-local/REPORT.md`.

Adapters: LangGraph `MongoDBSaver` and `MongoDBStore` (Python), `@langchain/langgraph-checkpoint-mongodb` (JS), OpenAI Agents
`MongoDBSession`, LlamaIndex mongo chat store, `@mastra/mongodb`, Mem0 `mongodb` vector store.

## Run it ($0, any session sandbox)

    judge/setup.sh                                   # mongod 7.0, MinIO, python (once)
    cargo build --release -p mongo-server
    bench/agent-adapters/install.sh                  # packages and official test suites under /home/user/adapters (not committed)
    bench/agent-adapters/run_suites.sh mongod        # each suite on a fresh mongod on 27017 -> results/mongod/<suite>.xml
    bench/agent-adapters/run_suites.sh ours          # the same on mongo-server -> results/ours/<suite>.xml
    python3 bench/agent-adapters/compare.py results/mongod/mastra.xml results/ours/mastra.xml
    bench/agent-adapters/run_e2e.sh mongod; bench/agent-adapters/run_e2e.sh ours    # 50 users x 200 turns, fake model
    python3 bench/agent-adapters/compare_e2e.py results/mongod/e2e-langgraph.json results/ours/e2e-langgraph.json

## Files

- `target.sh`: start a fresh, empty mongod or mongo-server (`PORT`, `MODE=standalone`, `KEEP=1`), stop one or all.
- `run_suites.sh`, `fake_docker.py`, `merge_junit.py`, `compare.py`: the adapters' own suites and their comparison.
- `e2e_py.py`, `e2e_js.mjs`, `run_e2e.sh`, `compare_e2e.py`, `sizes.py`: the end-to-end loop, answers and bytes stored.
- `probe_gaps.py`: minimal reproductions of the two differences found (`$toLong` of a date; unique index between concurrent transactions).
- `probe_scan.py`: latency of an unindexed collection scan (the LlamaIndex and Mem0 query shape).
- `run_vector_ours.sh`: Mastra's Atlas-only vector suite against our server (no reference possible).
- `results/`: XML, logs and JSON of the runs reported.
