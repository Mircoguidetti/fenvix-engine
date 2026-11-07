# Compaction 2, step 2: fix round of the review (FIX 3)

Report: fenvix-brain `sessions/2026-11-07-c2-step2-fix/REPORT.md`. Everything is local and $0 (`judge/setup.sh` first).

- `sharded_mongod.sh`: a local sharded cluster (mongod 7.0.34, one config server, one shard, mongos on 28100), the
  reference for "like MongoDB sharded collections".
- `probe_document_key.py`, `probe_lookup_after_move.py`, `probe_merge_sharded.py` (outputs in `.txt`): what mongos does
  with `documentKey`, `updateLookup` after a shard key update, and `$merge` into a sharded target.
- `cs_vs_mongos.py` (output `cs-vs-mongos.txt`): the diff suites' change-stream tests that update a document, against
  mongos with the collection sharded on `a` and against our server with `--customer-key c=a`.
- `doc_key_only.py` (output `diff-cs-classified.txt`): classifies the change-stream differences of a diff-suite run
  with customer keys on (its reference is an unsharded mongod, whose `documentKey` is `{_id}`).
- `diff-keys-envelopes-SUMMARY.txt`, `diff-py.json.gz`, `diff-node.json.gz`: `bench/c2-step2/diff_keys.sh` on the fixed
  binary with `--envelopes write`, every test.
- `stress.txt`: `bench/c2-step2/stress.py` against mongod, 60 rounds, 40 users, envelopes read and write.
- `adapters-ours/`: the 7 adapter suites with the declared customer keys (`bench/review-c2-step2/adapter-keys.txt`) and
  `--envelopes write`; compare with `bench/agent-adapters/results-2026-11-06/mongod` (Python and JS) and
  `bench/agent-adapters/results/mongod` (Mastra).
