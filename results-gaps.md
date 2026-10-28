# results-gaps — closing the gaps found by the MongoDB 7.0 audit

Branch `compat-gaps` (from `compat-indexes`). Reference server for every comparison: **mongod 7.0.45** (official tarball from fastdl.mongodb.org, run unprivileged as a standalone on 27017 and as a single-node replica set `rs0` on 27020 with `ttlMonitorSleepSecs=1`; the suite uses its own copies on 28000+). Drivers: pymongo 4.19 and the Node.js driver (mongodb 7.x). Every "identical" below means: the same request sent to real mongod 7.0.45 and to the engine gives the same canonical result (values, BSON types, key order, error code, codeName and, where the test compares it, the message).

## Numbers (final binary)

| Run | pymongo | Node driver |
|---|---|---|
| Gap tests g01–g10 (`gaps/gaps_py.py`, `gaps/node/gaps_node.js`) | 837 / 850 identical, 13 known divergences, 0 failures | 761 / 773 identical, 12 known divergences, 0 failures |
| Existing suite (`gaps/run_suite.py --shards 6`) | @SUITE_PY@ | @SUITE_NODE@ |
| Audit probes (`compat/tools/probe_diff.py`, `node_probe.js`, corrected pairing, see "Probe harness") | 498 / 545 identical (4 invalid) | 30 / 32 identical |

Known divergences are listed with their cause in `mongo-tests/gaps/known_divergences.json`; none is hidden. They are ICU/CLDR data differences of the collation locales (12) and a 1-ulp `$sum` accumulation order (1, pymongo only counted once).

Inventory (`compat/inventory.csv`, `python3 compat/coverage.py`): **1129 / 1245 covered = 90.7 %** (was 82.2 % before items 9–10e were applied; 548 further rows are *proposed* exclusions and not in the denominator, see `compat/exclusions_proposed.md`).

## Per item

Counts are `py / Node` cases in the final runs (all identical unless noted).

### Item 1 — silent defects (g01: 33 / 25)
Broken: `createIndexes` with several indexes built some and failed others; `find` returned `_id` order instead of insertion order; `$natural` sort/hint ignored; ties between index entries came out in a different order. Built: all-or-nothing multi-index builds, natural (insertion) order, `$natural` sort and hint, index-tie order. Not testable: crash-during-build recovery (no way to kill mongod mid-build reproducibly).

### Item 2 — authentication (g02: 46 / 18)
Broken: no access control. Built: SCRAM-SHA-1/256 (`saslStart/saslContinue`), `authSource`, per-database users in `admin.system.users`, `authenticationRestrictions`, user and role commands, built-in roles taken from mongod 7.0.45, role enforcement with `--auth`, `hello saslSupportedMechs`, `connectionStatus`, `logout`. Not testable: LDAP, Kerberos, Atlas mechanisms (Enterprise/Atlas only, proposed exclusions).

### Item 3 — TLS, X.509, SRV (g03: 14 / 14)
Broken: plain TCP only. Built: TLS listener (`requireTLS/preferTLS/allowTLS`), server certificate, CA and client certificates, MONGODB-X509, `mongodb+srv://` against a local DNS server. Not tested (no case in g03 uses them): certificate revocation lists / OCSP, TLS protocol-version options, and `mongodb+srv://` with several distinct hosts (the SRV tests resolve to one server).

### Item 4 — projection (g04: 17 / 12)
Rewritten after the 7.0 behaviour: positional `arr.$`, `$elemMatch`, expression values, `$slice`, path collisions, inclusion/exclusion error cases.

### Item 5 — aggregation (g05: 22 / 20, 1 known)
Trig, bitwise and stdDev expressions, `$lookup` `let` rules, `$changeStreamSplitLargeEvent`. Known: `$sum` of doubles can differ by 1 ulp from mongod's accumulation order (`g05.sum_avg.double_accumulation`). Decimal128 trigonometry is not bit-identical in the last digit (not tested).

### Item 6 — retryable writes (g06: 32 / 22)
Replay of retried statements (`retriedStmtIds`), `TransactionTooOld`, one number space shared by retryable writes and transactions, durable per-session record. Fixed at the end: `stmtIds` was refused by the command field check added in item 9 (found by the final gap run), and the session text of `NoSuchTransaction`/`TransactionTooOld` now matches mongod (`<uuid> - <digest> -  - `, and the session's latest number counts writes as well as transactions).

### Item 7 — collation (g07: 426 + 12 known / 425 + 12 known)
ICU-57 locale table taken from mongod, all options, default collation with unique `_id`, index collation, `explain`. Fixed at the end: `zh@collation=stroke` is valid in mongod 7.0.45 (the table said invalid), and `$min`/`$max` keep the **first** of two values that compare equal under the collation. Known (12): locales whose tailoring is missing from the ICU4X data the engine uses (`lv`, `se`, `smn`, `kl`, `haw`, `en_US_POSIX`, the `search` types of `cs/de/sk`). `backwards` and `normalization` are accepted and listed but the ICU4X collator cannot apply them exactly.

### Item 8 — cross-database `$out`/`$merge` and `renameCollection` (g08: 14 / 14).

### Item 9 — capped collections, tailable cursors, diagnostics, window N-accumulators (g09: 140 / 134)
Built: capped collections (eviction, size/max, `convertToCapped`, `cloneCollectionAsCapped`), tailable/awaitData cursors, `serverStatus`, `currentOp`/`$currentOp`, `killOp`, `top`, `profile`, `dbHash`, `dataSize`, `hostInfo`, `getLog`, `getCmdLineOpts`, `connPoolStats`, `lockInfo`, `listCommands`, plan-cache commands, `reIndex`, `compact`, `validateDBMetadata`, sessions commands and stages, `getDefaultRWConcern`, `getClusterParameter`, and `$top/$bottom/$topN/$bottomN/$firstN/$lastN/$maxN/$minN` as accumulators and window functions. `serverStatus` has a `bucketCatalog` section once a time-series collection exists, like mongod.

### Item 10a — `let`, `min/max/returnKey/showRecordId`, aggregate `explain` (g10.let, g10.find, g10.explain)
### Item 10b — validators and views (g10.validator 9, g10.view 6 in pymongo)
`$jsonSchema`/query validators with `validationLevel/Action` and `errInfo`, `indexOptionDefaults`, `storageEngine`; views (`viewOn`/`pipeline`, `collMod`, queries through views, `$lookup` from views, depth/cycle checks).
### Item 10c — `collMod` index, stats, validate (g10.collmod, g10.stats, g10.validate, g10.listdb, g10.commitquorum)
`hidden`, `unique`, `prepareUnique`, `expireAfterSeconds`; `scale` for `collStats/dbStats/$collStats`; `validate` options and output; `listDatabases` filter/`nameOnly`; `commitQuorum`.
### Item 10d — change streams, cluster time, concerns, transactions (g10.cs 34 in pymongo, g10.cluster, g10.concern, g10.txn, g10.params)
`fullDocument` `whenAvailable/required`, pre/post images, `fullDocumentBeforeChange`, expanded DDL events, rename events, `startAtOperationTime`; `$clusterTime`/`operationTime` on replica-set replies; write concern (tags, `w:0`, `writeConcernError`) and read concern (`afterClusterTime`, levels); `transactionLifetimeLimitSeconds`; `getParameter/setParameter` from the captured parameter table.
### Item 10e — time series, clustered collections, encrypted fields, OP_COMPRESSED, Stable API, replica-set commands (g10.special 5, g10.wire, g10.stableapi, g10.repl)
Time series and clustered collections (emulated: documents stored flat; `expireAfterSeconds` is an internal TTL index): create options and every error found by probing, `listCollections` (`timeseries` type and `system.buckets.*` companion), `listIndexes`, insert checks and the odd `n` of failed time-series inserts, update restrictions, `collMod` granularity/`expireAfterSeconds`, field order on read, `findAndModify` refused. `encryptedFields` on `create` (validation, defaults, standalone refusal; the `enxcol_.*` side collections are not materialised). OP_COMPRESSED (zlib, snappy, zstd). `apiVersion/apiStrict`. `replSetGetStatus`, `replSetGetConfig`, `shutdown` (the connection closes without a reply and the tenants are closed cleanly). `getFreeMonitoringStatus` does not exist in mongod 7.0.45 and the engine answers "no such command" like it.

## Problems found and fixed during the final verification
- Node `g10.validator.transactions`/`g10.let.in_transaction` were not exercising transactions (the script sent the transaction number as int32 and both servers answered with a type error). Now `Int64`; this exposed the session-text and shared-number differences above, which are fixed. The Node driver numbers the transactions of its own implicit session, which differs between the two servers in a full run: those numbers are normalised in the Node comparison only (pymongo scripts keep exact numbers).
- `idx.list_missing_collection_error` (main suite, Node): `listIndexes` on a missing collection lacked `: <db>.<coll>` in the message.
- Harness: plain (non-replica-set) gap cases compare a standalone mongod with the replica-set engine; `$clusterTime`, `operationTime` and `commitQuorum` exist only on the engine side there and are ignored in those cases. `primaryOnlyServices` is compared sorted (mongod lists it in a per-process hash order). `bucketCatalog` is ignored in the `serverStatus` shape tests because mongod shows it only after a time-series collection was used in the process.
- Environment lost with the machine restart and rebuilt: the reference mongods, the auth and TLS servers, the DNS server for SRV (`dnslib`), moto on :9000.

## Probe harness (compat/probes, read-only reference)
`probe_diff.py` runs the audit probes with one engine client for both standalone and replica-set cases, while the reference mongod has two clients. A probe that sends raw transaction numbers on the pymongo implicit session then poisons later probes on the engine side only (71 probes answered `TransactionTooOld`, correct mongod behaviour for a lower number). The final pairing used for the numbers above: standalone engine (27019) against standalone mongod, replica-set engine (27018) against `rs0`, separate clients. The results are kept in `compat/gaps_status/probe_*_final.json`.

## Open (not fixed, with the reason)
- **`cs.ttl_delete_event` in pymongo**: failed once earlier in the work; not reproduced since (10 / 10 alone and 4127 / 4127 in the full suite). Cause not found. Reported as open.
- **MinIO persistence tests**: the MinIO download (dl.min.io) answers 410, so the S3 side is **moto** on :9000, not MinIO. @PERSIST@
- **Probes still failing** (inventory rows kept `partial/missing`): `mapReduce` result order and options (`sort`, `limit`, `finalize`, `scope`, `jsMode`, `maxTimeMS`, `comment`, `collation`: mongod returns reduce output in hash order), `validate` `full` (transient warnings), the 12 collation locales above, `BinData` subtype 7 (`NonConformantBSON`), change-stream `modify`/`dropIndexes` expanded events (details differ), `w:` numeric unsatisfiable and custom tag probes, `$changeStream.startAtOperationTime` probe (flaky), `$currentOp`/`$listSessions` stage probes, session/causal-consistency probes (shared-session artifact described above), `Admin.*` Node members probe.
- **Limits that no test can remove**: no multi-host SRV against distinct servers; the TLS options listed as untested in item 3; `killOp` cannot stop a write in the middle of applying it; `$planCacheStats` is empty; the `planSummary` of the profiler is approximate; JavaScript commands run serialised on one thread; the order of `listCollections`/`distinct` through a view; the local-store lease limit; the change-stream history is kept in memory only (lost on restart); `explain` `parsedQuery` normalisation (`$eq` form) and view-explain plan details; `disambiguatedPaths` is always `{}`; file-system sizes in `dbStats` are 0; the real `find` order of a time-series collection is bucket order, the engine's is insertion order; side effects of `setParameter` other than the live ones (transaction lifetime, JS limits) are not implemented; `dropDatabase` with `w:2` shows no optime in its message; session records are in memory (after an engine restart mongod would still know the session numbers).
- Rows still `untested` are driver members the two harnesses never call (GridFS legacy members, raw-batch cursors, `advanceClusterTime` …); the list is in `compat/inventory.csv`.

## Files
`compat/gaps_status/item01.csv … item08.csv, item09_10.csv` (applied with `compat/apply_gaps.py`; `item09_10.csv` is generated by `mongo-tests/gaps/gen_status.py` from the final logs), `compat/exclusions_proposed.md` (proposals for your approval; nothing decided), `mongo-tests/gaps/` (tests), `mongo-tests/gaps/known_divergences.json`.
