# Merge 1: fix-query-limits a546fcd on integration-4 (merge 70ba479), own probes once (local, MinIO, mongod 7.0.34 rs)
- in_diff.py: every $in case byte for byte as mongod; FAIL only on `$nin` (order: mongod uses the _id index, we scan in
  insertion order), the one known difference the branch's README lists. Same as on the branch.
- batch_diff.py: PASS. drivers_read.py (pymongo) and drivers_read.js (Node, NODE_PATH from the adapters' js-lg): PASS.
- tail_cs_diff.py (ours with --change-log-max-mb 1024): PASS.
- cs_edge.py is a measuring probe, not a test: it asserts on mongod's own edge too differently (cs_edge_mongod.txt); not counted.
