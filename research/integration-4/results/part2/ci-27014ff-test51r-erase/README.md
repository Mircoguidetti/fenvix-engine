# CI 27014ff: test_51r compactor erasure (both modes) "timed out waiting for the erasure scrubbed"
- A joint of part 2: since perf-erase an erasure is scrubbed in batches (--sealed-erasure-batch-ms, 1 hour by default, and
  the agent polls every 60 s); the test waited 90 s. It passed on 015ecb9 and 098e404, which have no perf-erase.
- Fix (test only): the test server runs with --sealed-erasure-batch-ms 0 --sealed-erasure-poll-ms 200, as test_49 and
  test_69 do. Local, 332c687 engine code: sealed2-derive 1 passed (6.3 s), off 1 passed (4.4 s).
