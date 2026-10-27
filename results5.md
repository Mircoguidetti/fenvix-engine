# A MongoDB wire-protocol server on the slatedb tenant lifecycle — results5

`mongo-server` (new workspace member) speaks the MongoDB wire protocol on `localhost:27018`. Each MongoDB database is one slatedb tenant at `tenants/{dbname}` on MinIO, driven by the results3 lifecycle: wake as a reader, upgrade to a writer on the first write, clean close after an idle period, `_clean` marker, standalone compaction and GC. `results.md` to `results3.md` are unchanged and the existing benchmark still works (default benchmark and the full lifecycle experiment re-run after the refactor: 0 lost writes, same control stall).

**Result:** the same 308 in-scope operation scripts, run through both the official Python driver (pymongo 4.19.0) and the official Node.js driver (mongodb 7.7.0) against a real `mongo:7.0` (7.0.45) and against this server, **pass 308/308 on both drivers**. 34 further scripts probe things outside the requested scope; 3 of those work and 31 are not supported (listed below). Building the suite found and fixed 11 defects in the server and one wrong test expectation (listed below), including a race in `dropDatabase` that could leave a corrupt "ghost" database behind.

## Step 0 — Findings before coding

- **Wire protocol.** The official MongoDB documentation site (`mongodb.com`) is not reachable from this sandbox (DNS failure), so I used MongoDB's own specification repository (`mongodb/specifications`, `source/message/OP_MSG.md`, fetched from raw.githubusercontent.com). From it: OP_MSG is opcode 2013; `flagBits` has `checksumPresent` (bit 0), `moreToCome` (bit 1) and `exhaustAllowed` (bit 16); a message is a kind-0 body section plus optional kind-1 document sequences (`documents`, `updates`, `deletes`) and an optional trailing CRC-32C. **I could not read the OP_QUERY/OP_REPLY layout from an official source.** I implemented it from the known layout and validated it empirically with a raw-socket test that sends a legacy `isMaster` to `admin.$cmd` and compares the OP_REPLY (opcode, `responseTo`, flags, cursor id, `numberReturned`, document fields) with real MongoDB 7.0: it matches.
- **Which driver uses which handshake transport** (from the installed driver sources). pymongo 4.19.0 sends the legacy command name `isMaster` with `helloOk: true` over OP_MSG ("always use OP_MSG for the handshake", `synchronous/pool.py`) and switches to `hello` once the server answers `helloOk`. The Node driver 7.7.0 still opens every connection with an **OP_QUERY** `isMaster` + `helloOk` (until the server's wire version is known `supportsOpMsg` is false, `cmap/connection.js`; with `serverApi` it sends `hello`), then switches to OP_MSG. So OP_QUERY/OP_REPLY is exercised by every Node connection and by the raw-socket test; pymongo exercises only OP_MSG (plus the raw-socket test).
- **Wire version.** pymongo 4.19.0: `MIN_SUPPORTED_WIRE_VERSION = 9`, `MAX_SUPPORTED_WIRE_VERSION = 29` (servers 4.4 to 9.0). Node `mongodb` 7.7.0: also 9 and 29 (`cmap/wire_protocol/constants.js`). A driver rejects a server whose `maxWireVersion` is below 9 or whose `minWireVersion` is above 29. **Chosen: `maxWireVersion = 21`, `minWireVersion = 0`**, i.e. MongoDB 7.0's own value (the reference server reports 21; `admin.hello` compares it), inside both drivers' range.
- **Commands the drivers send by themselves** (found by logging every command): `isMaster` (handshake and monitoring), `hello` (with `serverApi`), `endSessions` (on close), plus whatever the application calls (`ping`, ...). Nothing else; the server also answers `buildInfo`, `getParameter`, `listDatabases`, `listCollections`. An unknown command returns `{ok: 0, code: 59, codeName: "CommandNotFound", errmsg: "no such command: 'x'"}` and keeps the connection open (tested).
- **Crates** (latest stable on crates.io, checked 2026-10-25), pinned exactly in `mongo-server/Cargo.toml`: `bson = "=3.1.0"`, `tokio = "=1.54.1"`, plus the already pinned `slatedb = "=0.17.0"`, `object_store = "=0.14.2"`. Rust 1.97.0.
- **Docker** pulled the official image: `mongo:7.0` (digest `sha256:c53852c00c23ca9f23fc39690350ffa9a80e3642fe8f96879726d8da0abe80a7`, server version 7.0.45).

## What was built

- **Workspace.** The request-counting store and the results3 lifecycle moved into a library target of the existing package (`src/lib.rs`) so both binaries share them; the benchmark binary is otherwise unchanged. Tenant ids in the lifecycle are now strings (database names). New member: `mongo-server`.
- **Storage layout inside a tenant:** `doc/{collection}/{sortable _id}` holds the BSON document; `meta/collections` holds the collection list; `meta/indexes/{collection}` holds recorded index definitions. The `_id` encoding preserves BSON order and treats `1`, `1_i64` and `1.0` as the same key; collection names are escaped so `a` and `a/b` never share a prefix. `ObjectId` is generated when `_id` is missing; `_id` is always stored first, as MongoDB does.
- **Lifecycle.** A database is woken as a reader by its first command (read-only commands never upgrade) and upgraded on its first write; writes are acknowledged only after a WAL flush (`WriteBatch` + `flush` + `await_durable`); an idle reaper closes tenants cleanly after `--idle-secs` (60 by default; the tests also run an instance with 3). Compaction and GC run as in results3 (queue-driven standalone compactor passes, GC 30 s after the last write).
- **Commands:** `hello`/`isMaster` (OP_QUERY and OP_MSG), `ping`, `buildInfo`, `getParameter`, `listDatabases`, `listCollections`, `endSessions`, `insert`, `find`, `getMore`, `killCursors`, `update`, `delete`, `count`, `distinct`, `aggregate`, `drop`, `dropDatabase`, `create`, `createIndexes`, `listIndexes`, and a non-standard debug command `tenantAdmin` (close the tenant now, report its mode and the object-store requests made so far).
- Filters, update operators and aggregation stages are as listed in the table below. Everything is evaluated in memory over documents scanned from the tenant, except `_id` equality, which is a point read. Unique indexes are enforced by scanning; other index definitions are recorded only.
- 18 Rust unit tests cover the query engine, key encoding, update operators and aggregation.

## Compatibility table

Cells are `passed/total` differential tests per driver. "Supported, passing" means every test for that row gave identical results on real MongoDB and on this server (including error codes, and error messages where the row says so).

| Area | Feature | pymongo | Node.js | Status |
|---|---|---|---|---|
| Handshake | hello / isMaster over OP_QUERY and OP_MSG (also with serverApi) | 3/3 | 3/3 | supported, passing |
| Handshake | legacy OP_QUERY isMaster -> OP_REPLY (raw socket) | 1/1 | 1/1 | supported, passing |
| Handshake | unacknowledged writes (OP_MSG moreToCome, w:0) | 1/1 | 1/1 | supported, passing |
| Handshake | client requests compression (server does not advertise it) | 1/1 | 1/1 | supported, passing |
| Handshake | ping | 1/1 | 1/1 | supported, passing |
| Handshake | buildInfo | 1/1 | 1/1 | supported, passing |
| Handshake | getParameter (featureCompatibilityVersion) | 1/1 | 1/1 | supported, passing |
| Handshake | listDatabases (incl. nameOnly) | 2/2 | 2/2 | supported, passing |
| Handshake | listCollections (filter, nameOnly) | 3/3 | 3/3 | supported, passing |
| Handshake | endSessions / logical sessions | 1/1 | 1/1 | supported, passing |
| Handshake | unknown command -> CommandNotFound (code 59), connection stays open | 1/1 | 1/1 | supported, passing |
| Commands | insert: single, many, ordered/unordered, generated _id, 1,000 docs | 22/22 | 22/22 | supported, passing |
| Commands | insert: duplicate key (11000, MongoDB-shaped message, keyPattern/keyValue) | 9/9 | 9/9 | supported, passing |
| Commands | unique indexes: enforced on insert, update, upsert, index build | 10/10 | 10/10 | supported, passing |
| Commands | find: filter, projection, sort, skip, limit | 32/32 | 32/32 | supported, passing |
| Commands | find: batchSize, getMore paging, killCursors | 6/6 | 6/6 | supported, passing |
| Commands | update: updateOne / updateMany / replaceOne, nModified, immutable _id | 41/41 | 41/41 | supported, passing |
| Commands | update: upsert | 9/9 | 9/9 | supported, passing |
| Commands | delete: deleteOne / deleteMany | 10/10 | 10/10 | supported, passing |
| Commands | count (legacy command), countDocuments, estimatedDocumentCount | 4/4 | 4/4 | supported, passing |
| Commands | aggregate (cursor, batching, errors) | 36/36 | 36/36 | supported, passing |
| Commands | drop, create, dropDatabase | 9/9 | 9/9 | supported, passing |
| Commands | createIndexes, listIndexes (definitions recorded; queries scan) | 10/10 | 10/10 | supported, passing |
| Commands | collection names with . / % space unicode; prefix isolation | 3/3 | 3/3 | supported, passing |
| Filters | equality, $eq, $ne | 28/28 | 28/28 | supported, passing |
| Filters | $gt, $gte, $lt, $lte (incl. type bracketing, arrays, null) | 15/15 | 15/15 | supported, passing |
| Filters | $in, $nin | 10/10 | 10/10 | supported, passing |
| Filters | $exists | 5/5 | 5/5 | supported, passing |
| Filters | $and, $or, $nor | 5/5 | 5/5 | supported, passing |
| Filters | $not | 5/5 | 5/5 | supported, passing |
| Filters | dotted paths into documents and arrays of documents | 9/9 | 9/9 | supported, passing |
| Filters | equality against array elements and whole arrays | 5/5 | 5/5 | supported, passing |
| Filters | BSON comparison order for mixed types (sort and compare) | 3/3 | 3/3 | supported, passing |
| Filters | invalid operators rejected with MongoDB's error code | 3/3 | 3/3 | supported, passing |
| Aggregation | $match, $sort, $limit, $skip, $project (include/exclude/$field), $count stage | 12/12 | 12/12 | supported, passing |
| Aggregation | $group with $sum, $avg, $min, $max, $count | 20/20 | 20/20 | supported, passing |
| Update operators | $set, $unset, $inc (int32/int64/double, overflow, nested, array index) | 34/34 | 34/34 | supported, passing |
| Persistence | idle close and wake (reader on read, upgrade on first write) | 9/9 | 9/9 | supported, passing |
| Persistence | server restart (graceful and SIGKILL): acknowledged writes survive | 2/2 | 2/2 | supported, passing |
| Concurrency | parallel inserts / increments / databases / reads during writes | 4/4 | 4/4 | supported, passing |
| Beyond scope (working) | distinct | 1/1 | 1/1 | supported, passing |
| Beyond scope (working) | $group $first, $last, $push | 2/2 | 2/2 | supported, passing |
| Query operators | $regex, $elemMatch, $size, $all, $type, $mod, $expr | 0/7 | 0/7 | not supported |
| Aggregation | $addToSet, $unwind, $lookup, $addFields, arithmetic expressions, $replaceRoot, $facet | 0/7 | 0/7 | not supported |
| Update operators | $push, $pull, $addToSet, $rename, $min, $max, $mul, $currentDate, $pop, positional $, pipeline updates, arrayFilters | 0/12 | 0/12 | not supported |
| Commands | findAndModify (find_one_and_update) | 0/1 | 0/1 | not supported |
| Commands | dropIndexes, renameCollection, collStats, dbStats | 0/4 | 0/4 | not supported |

Tests not attributed to a row above: `admin.empty_db_reads`, `insert.array_id_rejected`, `find.filter.empty`, `find.filter.on_missing_collection`, `coll.create_existing_is_idempotent`, `coll.create_existing_with_different_options_error` (all pass on both drivers; they are counted in the pass rates).

### Supported with differences (known, not all covered by the tests)

I verified each of these directly against `mongo:7.0` (they are real differences, not guesses):

| Feature | Difference |
|---|---|
| `find` without `sort` | Natural order is `_id` key order, not insertion order (`[3, 1, 2]` inserted, MongoDB returns `[3, 1, 2]`, this server `[1, 2, 3]`). The tests normalise order when no sort is given, as agreed. |
| `listDatabases` | Lists tenants only: no `admin`/`config`/`local`, `sizeOnDisk` is always 0. |
| `buildInfo` | Reports a fixed version (`7.0.14`; the reference is 7.0.45); only `ok`, `bits` and `maxBsonObjectSize` are compared. |
| `create` with options | `capped`, `size`, validators, collation etc. are accepted on a new collection but ignored (`listCollections` shows `options: {}`; MongoDB shows `{capped: true, size: 4096}`). Creating an existing collection matches 7.0 (idempotent unless options differ). |
| Decimal128 | Stored and returned correctly, but not compared or sorted as numbers (a `$gt` on a Decimal128 field matches nothing; sort order differs). |
| Regular-expression operands | `{field: /re/}` is rejected with code 2; MongoDB evaluates it. |
| Indexes other than `unique` | Definitions are recorded and listed; text, TTL, partial, sparse, geo and collation options are stored but not enforced or used. Queries never use an index (they scan), so `hint` is ignored. |
| Write concern, `maxTimeMS`, `readConcern`, `collation` | Accepted and ignored. Every write is durable before the reply; `w: 0` is executed without a reply (tested). |
| Cursors | The whole result is materialised at `find`; cursors live in server memory (lost on restart, no timeout); replies are not split at 16 MB. Default first batch 101 and `batchSize`/`getMore` behaviour match (tested). |
| Concurrency | Commands on one database run one at a time (a per-database lock); different databases run in parallel. |
| Not implemented at all | auth, TLS, compression (a client requesting it works; the server does not advertise it), transactions, change streams, `OP_COMPRESSED`. |

## Pass rates

| Driver | In-scope tests (core) | Beyond-scope probes (extra) | All |
|---|---|---|---|
| pymongo | **308/308 (100.0%)** | 3/34 | 311/342 |
| Node.js mongodb | **308/308 (100.0%)** | 3/34 | 311/342 |

| Group | Tests | pymongo | Node.js |
|---|---|---|---|
| handshake and admin | 14 | 14/14 | 14/14 |
| insert | 38 | 38/38 | 38/38 |
| find: filters and operators | 77 | 77/77 | 77/77 |
| find: operators outside the supported set (beyond scope) | 7 | 0/7 | 0/7 |
| find: projection | 14 | 14/14 | 14/14 |
| find: sort, skip, limit | 16 | 16/16 | 16/16 |
| cursors: batchSize, getMore, killCursors | 7 | 7/7 | 7/7 |
| count and distinct | 4 | 4/4 | 4/4 |
| count and distinct (beyond scope) | 1 | 1/1 | 1/1 |
| aggregate | 36 | 36/36 | 36/36 |
| aggregate (outside the supported set) (beyond scope) | 9 | 2/9 | 2/9 |
| update | 52 | 52/52 | 52/52 |
| update operators outside the supported set (beyond scope) | 13 | 0/13 | 0/13 |
| delete | 10 | 10/10 | 10/10 |
| collections and indexes | 22 | 22/22 | 22/22 |
| collections and indexes (beyond scope) | 4 | 0/4 | 0/4 |
| persistence: idle close and wake | 14 | 14/14 | 14/14 |
| concurrency | 4 | 4/4 | 4/4 |

Failing in-scope tests: none. The 31 beyond-scope probes that fail are the "not supported" rows above: the server returns `CommandNotFound` or an "unknown operator/modifier/stage" error where MongoDB succeeds.

The two harnesses interpret the same generated `tests.json` (342 scripts, 308 in scope), so both drivers run the same operation sequences. Comparison rules: ObjectIds are replaced by a placeholder unless the test supplied them, top-level result lists are sorted when the test says the order is unspecified, everything else is compared exactly as canonical Extended JSON, so int32/int64/double types, field order and (where a test says so) error messages and `keyPattern`/`keyValue` must match. The Node run deserialises with `promoteValues: false`; pymongo keeps `int`/`Int64`/`float` distinct on decode. Each test uses a fresh database; a test passes only if every step result is identical on both servers.

## Latency and requests (MinIO on localhost)

Collection of 1,000 documents of about 1 KB. Latency is measured end to end through pymongo. **Cold** means the database's tenant was closed (`tenantAdmin`) right before every sample, so the sample includes the reader wake from object storage; **warm** means the tenant was already open. The reference column is real MongoDB on the same machine (warm only), a baseline for driver and protocol cost. MinIO is on localhost, so the cold numbers are a lower bound for real S3.

| Operation (1,000 docs of ~1 KB) | Tenant state | n | p50 (ms) | p99 (ms) | MongoDB 7.0 p50 / p99 (ms), warm |
|---|---|---|---|---|---|
| ping (driver + protocol baseline) | warm | 500 | 0.102 | 0.322 | 0.267 / 0.949 |
| ping (driver + protocol baseline) | cold | 100 | 0.136 | 0.581 |  |
| find by _id | warm | 500 | 0.127 | 0.443 | 0.746 / 1.968 |
| find by _id | cold | 100 | 9.893 | 19.510 |  |
| filtered find, 10 of 1,000 docs | warm | 250 | 3.144 | 5.666 | 1.262 / 2.428 |
| filtered find, 10 of 1,000 docs | cold | 100 | 17.179 | 31.465 |  |
| filtered find, 100 of 1,000 docs | warm | 100 | 3.262 | 7.816 | 1.389 / 2.427 |
| filtered find, 100 of 1,000 docs | cold | 100 | 17.030 | 25.342 |  |
| insert_one (1 KB doc, durable) | warm | 250 | 2.934 | 7.389 | 0.582 / 4.423 |
| insert_one (1 KB doc, durable) | cold | 100 | 50.200 | 175.475 |  |

Object-store requests per operation (counted at the object-store layer, exact means over 20 samples per cell), priced at S3 Standard list prices (PUT/COPY/POST/LIST $0.005 per 1,000, GET $0.0004 per 1,000):

| Operation | Tenant state | PUT/LIST-tier requests | GET-tier requests | DELETE | Request cost (S3 Standard list prices) |
|---|---|---|---|---|---|
| find by _id | warm | 0.00 | 0.00 | 0.00 | $0.00000000 |
| find by _id | cold | 1.00 | 10.00 | 0.00 | $0.00000900 |
| filtered find (10 of 1,000 docs) | warm | 0.00 | 0.00 | 0.00 | $0.00000000 |
| filtered find (10 of 1,000 docs) | cold | 1.00 | 10.00 | 0.00 | $0.00000900 |
| insert_one (1 KB, durable) | warm | 1.00 | 0.15 | 0.00 | $0.00000506 |
| insert_one (1 KB, durable) | cold | 5.00 | 27.10 | 1.00 | $0.00003584 |

A cold read costs about one LIST and ten GETs ($0.000009); a warm read costs nothing; a cold *first write* includes the writer upgrade (5 PUT/LIST-tier and 27 GET-tier requests), as in results3, and the later close costs extra.

## Defects found by the differential tests (all fixed)

1. `projection {_id: 1}` alone returned whole documents.
2. An empty array sorted tied with `null`; MongoDB sorts it below `null` in both directions.
3. `$skip`/`$limit` argument errors used the wrong codes (MongoDB: 5107200 / 5107201).
4. Duplicate-key `writeErrors` and index-build errors lacked `keyPattern`/`keyValue`; the index-build message form differed.
5. A replacement that changes `_id` used the wrong message ("After applying the update, the (immutable) field '_id' was found to have been altered to _id: 2").
6. `drop` of a missing collection must succeed on 7.0 (it returned code 26).
7. *Found only by the Node driver:* MongoDB moves `_id` to the first position on insert (the Node driver appends a generated `_id` client-side, pymongo puts it first, which hid the bug).
8. *Found only by the Node driver:* `create` on an existing collection is idempotent on 7.0 (pymongo raises client-side, Node does not).
9. **`dropDatabase` raced with background compaction and GC**: a pass could write a manifest after the drop had listed and deleted the objects, leaving a "ghost" tenant whose manifest pointed at deleted SSTs (later reads failed with `NoSuchKey`, and a re-created database of the same name failed). Reproduced 8 times in 15 rounds with `repro_drop_race.py`, fixed with a per-tenant maintenance lock shared by compaction, GC and drop (drop waits for an in-flight pass, deletes until the prefix is empty; later passes find no manifest and do nothing); 90 of 90 rounds clean afterwards, and the scenario is a permanent differential test (`persist.drop_database_races_background_compaction`). It was found by the latency benchmark, not by the functional tests, because the benchmark drops a database that was just written many times.
10. *Performance, found by counting requests:* slatedb's scan default fetches one 4 KiB block per GET, sequentially, and does not cache the blocks, so even a **warm** filtered find made 249 GETs (cold: 259, 530 ms on MinIO). Scans now read ahead 4 MiB with 4 parallel fetches and cache blocks: warm 0 GETs, cold 10 GETs, 16 ms.
11. *Performance:* a durable write waited for slatedb's 100 ms flush tick (p50 101 ms). Writes now flush immediately (p50 3 ms).
12. Test-harness only: the first `unique_on_existing_dups` expectation compared an index-build message that embeds random UUIDs; it now compares the code.

## Limits of this version

- **Every query scans the collection** (only `_id` equality is a point read), decoding every document in memory; cold filtered finds on collections much larger than 1,000 × 1 KB will cost proportionally more GETs. Unique indexes are enforced by scans and `insert` of more than 16 documents scans the collection once.
- Collections of about 1 MB were measured; larger collections, documents near 16 MB, many simultaneous connections, multi-process access to one database and long-running compaction interplay were not tested here (results3 covers the lifecycle itself).
- One server process owns a database at a time (the lifecycle's single-writer assumption); two servers on the same bucket would fence each other.
- A server killed with SIGKILL loses nothing acknowledged (tested with two kill/restart cycles), but open cursors are lost and the next wake of each database takes the slower "unclean" path.

## Reproduce

```bash
# MinIO on :9000 with bucket "bench" and the Docker daemon, as in results.md; then:
docker run -d --name mongo7 -p 27017:27017 mongo:7.0            # the reference server
python3 -m venv mongo-tests/.venv && mongo-tests/.venv/bin/pip install pymongo   # pymongo 4.19.0
(cd mongo-tests/node && npm install mongodb)                                       # mongodb 7.7.0, Node v22
cargo build --release                                           # builds tenant-bench and mongo-server
cargo test --release -p mongo-server                            # 18 unit tests
mongo-tests/run_all.sh        # starts mongo-server on :27018 (60 s idle) and :27019 (3 s idle),
                              # generates tests.json, runs run_py.py and node/run_node.js, then
                              # bench_latency.py, bench_requests.py and report.py (about 30 minutes)
mongo-tests/.venv/bin/python mongo-tests/repro_drop_race.py mongodb://localhost:27018 30   # drop race regression
```

Single tests: `mongo-tests/.venv/bin/python mongo-tests/run_py.py --only find.filter` and `node mongo-tests/node/run_node.js --only=find.filter`. The server alone: `./target/release/mongo-server --listen 127.0.0.1:27018 --idle-secs 60` (`--log-commands` logs every command, `--no-maintenance` disables compaction and GC). Raw results: `mongo-tests/out/results_py.json`, `results_node.json`, `latency.json`, `requests.json`.

## Conclusion

The server is wire-compatible with both official drivers for the requested command and operator set: all 308 in-scope differential tests give identical results on real MongoDB 7.0 and on this server, through both pymongo and the Node.js driver, including 1,000-document inserts, `getMore` paging, unique-index violations, upserts, nested documents and arrays, databases that go idle, close and are read again, and servers restarted gracefully and with SIGKILL. Its limits are performance and breadth, not correctness of what it implements: every query except `_id` equality scans the collection, a cold read of a ~1 MB collection takes ~17 ms and 11 object-store requests on local MinIO (a point read ~10 ms), a durable write ~3 ms when warm, and 31 probed commands and operators (`$regex`, `$elemMatch`, `$push`, `findAndModify`, `$lookup`, ...) are not supported.
