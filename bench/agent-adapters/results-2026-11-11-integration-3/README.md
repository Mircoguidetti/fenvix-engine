# Agent adapters on integration-3 (2026-11-11)

Engine: fenvix-engine `integration-3`, engine code a54c2fd (later commits add tests and results only). Our server with the
product defaults of integration-3 (sealed runs on, the compactor, derived documents for checkpoints and
checkpoint_writes, the one-wave reader, the reader skipping SlateDB below X_d). Reference: mongod 7.0.34 from
`judge/setup.sh`. `run_suites.sh mongod|ours lg-py-saver lg-py-store lg-js oai llama mastra`.

Tests that pass on mongod and fail on ours (`compare.py`): 0 in every suite.
- LangGraph saver 146 passed; LangGraph store 29 passed, 3 failed on both ($vectorSearch); LangGraph JS 123 passed;
  OpenAI Agents 72 passed; LlamaIndex 18 passed; Mastra 1,072 passed, 10 failed on both (Atlas Local).
- `ours-mastra-run1/`: the first Mastra run on ours had one test more failing, "keeps pins from before pin kinds as
  page pins and then takes a list pin on the same page", a 60 s timeout (the run took 98 s); the rerun (`ours/`, 44 s)
  is the same as mongod, test for test.
