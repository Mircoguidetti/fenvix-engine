# LangGraph JS loop without `saver.setup()` (stopped)

The first e2e run of the LangGraph JS checkpointer did not call `MongoDBSaver.setup()`, which is the only place the JS
package creates its two indexes (the Python package creates them in its constructor). Every `getTuple` then scanned the
collection sorted by `checkpoint_id`, while the collection grew to 1.7 GB.
- mongod 7.0: the loop finished, 50 users x 200 turns in 802 s (results/mongod/e2e-langgraph-js.json, this first run).
- ours: stopped by hand after about 50 minutes with 6,372 of 30,000 checkpoints written; 10,316 SLOW lines in the server
  log (single finds of 8 to 19 s), a count of the collection took 57 s.
The rerun calls `setup()` on both targets (e2e_js.mjs), which is what the package's README asks for.
