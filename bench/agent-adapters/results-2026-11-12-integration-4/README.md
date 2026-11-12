# Agent adapters on integration-4 part 1 (2026-11-12)

Engine: fenvix-engine `integration-4` at 68bc94b; engine code fa4649b (292f526 plus timing log lines; later commits are
tests, scripts and results). Server binary sha256 3fa2ce6d615ce7167c44c73d923b34daae8ea4cf9b3662a5d9f83f5eae1dcb84,
product defaults (no layout options, no sealed environment variables), as on integration-3. Reference: mongod 7.0.34
from `judge/setup.sh`. Command: `run_suites.sh mongod|ours lg-py-saver lg-py-store lg-js oai llama mastra`.

Tests that pass on mongod and fail on ours (`compare.py`, compare.txt): 0 in every suite.
- LangGraph saver 146 passed; LangGraph store 29 passed, 3 failed on both ($vectorSearch), 7 skipped on both;
  LangGraph JS 123 passed, 7 skipped on both; OpenAI Agents 72 passed; LlamaIndex 18 passed;
  Mastra 1,073 passed, 10 failed on both (Atlas Local), 392 skipped on both.
- Mastra: 1,073 passed here against 1,072 on integration-3, on mongod as on ours, so not a difference of our server
  (why one more passes on both: NOT KNOWN, not investigated).
- Mem0 is not one of the six (as on integration-3).
