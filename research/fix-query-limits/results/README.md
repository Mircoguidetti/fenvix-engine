# fix-query-limits results (2026-11-11, local sandbox, MinIO, mongod 7.0.34)

- `in_reads_before.json`, `in_reads_after.json`: `in_reads.py` on the same 1,000,000-document collection (same MinIO data,
  same 100 random `_id`s per trial, seed 7), integration-3 3e4fdcc against this branch. Both runs printed these lines.
- `in_diff.py` (this branch): every `$in` case matches mongod byte for byte; the one difference left is `$nin` (order:
  mongod reads the `_id` index, we scan in insertion order), the same on integration-3, not an `$in` case.
  On integration-3: 5 cases differ (mixed types, MinKey/MaxKey, null in the list, `$in` inside `$or`, `$nin`): the order.
- `batch_diff.py` (this branch): every case has mongod's batch lengths, documents and reply sizes, largest reply
  16,777,304 bytes on both. On integration-3 the first case already fails: a first batch of 100,663,420 bytes, refused
  by pymongo ("larger than server max message size (48000000)").
- `drivers_read.py` / `drivers_read.js`: pymongo 4.19.0 and the Node driver 7.7.0 read 60,012 of 60,012 documents once,
  with find, find with batchSize 50,000 and aggregate.
