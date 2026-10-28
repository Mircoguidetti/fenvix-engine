# Proposed exclusions (for founder approval)

Nothing here is decided. Rows with `exclusion_proposed=yes` in `compat/inventory.csv` are proposals; they leave the coverage denominator only if approved. `exclusion_reason` in the CSV carries the full text.

| Group | Rows | Main reason |
|---|---|---|
| Client-only driver members | 269 | client-side only: the driver sends no request of its own for this member, so there is nothing for the engine to implement |
| Internal server commands | 133 | internal server-to-server command, not callable by applications; no page in the 7.0 manual (docs_url is the command index, which does not list it) |
| Client-side encryption helpers | 47 | client-side field-level encryption helper (libmongocrypt/KMS); the server only stores BSON binData subtype 6 and key-vault documents, no engine surfac |
| Sharding | 40 | sharding administration; a FENVIX database is not a sharded cluster; documented but not listed by mongod 7.0.45 listCommands (mongos/Enterprise/Atlas  |
| Replication | 18 | replica-set administration/internal replication command, not issued by application code |
| Atlas / Enterprise-only | 18 | calls only commands/stages that are themselves proposed exclusions (Atlas-only service) |
| Admin (process / cluster) | 11 | privileged process/cluster administration (not issued by application code) |
| Undocumented / not in the 7.0 manual | 8 | listed by listCommands on mongo 7.0.45 but absent from the 7.0 manual (undocumented, no driver API uses it); no page in the 7.0 manual (docs_url is th |
| Removed / 8.0-only | 3 | client-level bulkWrite needs the MongoDB 8.0 `bulkWrite` command (absent from the 7.0 manual); real mongod 7.0.45 rejects it too (probe: identical err |
| Other | 1 | mongod startup configuration option (security.javascriptEnabled), not reachable through a driver |


> **How to read the Admin / Sharding / Replication lists.** Your rule keeps these in scope unless it is shown that no driver or application sends them. The audit proposed excluding them on the argument "not issued by application code". That argument is weak for several rows below (for example `shutdown`, `setParameter`, `setDefaultRWConcern`, `fsync`, `logRotate`, `setFeatureCompatibilityVersion` are sent by ops tooling and by driver test suites). The tag `[engine today: ...]` shows what the engine does now: `handled` means the server recognises the command (it may still differ from mongod in detail). My recommendation is to approve only the rows where the engine is `unknown` *and* you accept leaving them out, and to keep the rest in scope.

## Admin (process / cluster) (11)

- `cmd:dropConnections` [engine today: unknown] — privileged process/cluster administration (not issued by application code)
- `cmd:fsync` [engine today: handled] — privileged process/cluster administration (not issued by application code)
- `cmd:fsyncUnlock` [engine today: handled] — privileged process/cluster administration (not issued by application code)
- `cmd:logRotate` [engine today: handled] — privileged process/cluster administration (not issued by application code)
- `cmd:rotateCertificates` [engine today: unknown] — privileged process/cluster administration (not issued by application code)
- `cmd:setClusterParameter` [engine today: handled] — privileged process/cluster administration (not issued by application code)
- `cmd:setDefaultRWConcern` [engine today: handled] — privileged process/cluster administration (not issued by application code)
- `cmd:setFeatureCompatibilityVersion` [engine today: unknown] — privileged process/cluster administration (not issued by application code)
- `cmd:setParameter` [engine today: handled] — privileged process/cluster administration (not issued by application code)
- `cmd:setUserWriteBlockMode` [engine today: unknown] — privileged process/cluster administration (not issued by application code)
- `cmd:shutdown` [engine today: handled] — privileged process/cluster administration (not issued by application code)

## Sharding (40)

- `cmd:addShard` [engine today: unknown] — sharding administration
- `cmd:addShardToZone` [engine today: unknown] — sharding administration
- `cmd:analyzeShardKey` [engine today: unknown] — sharding administration
- `cmd:balancerCollectionStatus` [engine today: unknown] — sharding administration
- `cmd:balancerStart` [engine today: unknown] — sharding administration
- `cmd:balancerStatus` [engine today: unknown] — sharding administration
- `cmd:balancerStop` [engine today: unknown] — sharding administration
- `cmd:checkMetadataConsistency` [engine today: unknown] — sharding administration
- `cmd:cleanupOrphaned` [engine today: unknown] — sharding administration
- `cmd:cleanupReshardCollection` [engine today: unknown] — sharding administration
- `cmd:clearJumboFlag` [engine today: unknown] — sharding administration
- `cmd:commitReshardCollection` [engine today: unknown] — sharding administration
- `cmd:configureCollectionBalancing` [engine today: unknown] — sharding administration
- `cmd:configureQueryAnalyzer` [engine today: unknown] — sharding administration
- `cmd:enableSharding` [engine today: unknown] — sharding administration
- `cmd:flushRouterConfig` [engine today: unknown] — sharding administration
- `cmd:getShardMap` [engine today: unknown] — sharding administration
- `cmd:isdbgrid` [engine today: unknown] — sharding administration
- `cmd:listShards` [engine today: unknown] — sharding administration
- `cmd:mergeAllChunksOnShard` [engine today: unknown] — sharding administration
- `cmd:mergeChunks` [engine today: unknown] — sharding administration
- `cmd:moveChunk` [engine today: unknown] — sharding administration
- `cmd:movePrimary` [engine today: unknown] — sharding administration
- `cmd:moveRange` [engine today: unknown] — sharding administration
- `cmd:refineCollectionShardKey` [engine today: unknown] — sharding administration
- `cmd:removeShard` [engine today: unknown] — sharding administration
- `cmd:removeShardFromZone` [engine today: unknown] — sharding administration
- `cmd:reshardCollection` [engine today: unknown] — sharding administration
- `cmd:setAllowMigrations` [engine today: unknown] — sharding administration
- `cmd:shardCollection` [engine today: unknown] — sharding administration
- `cmd:shardConnPoolStats` [engine today: unknown] — sharding administration
- `cmd:shardingState` [engine today: unknown] — sharding administration
- `cmd:split` [engine today: unknown] — sharding administration
- `cmd:unsetSharding` [engine today: unknown] — sharding administration
- `cmd:updateZoneKeyRange` [engine today: unknown] — sharding administration
- `cse:refineCollectionShardKey` — sharding event (only emitted on sharded clusters)
- `cse:reshardCollection` — sharding event (only emitted on sharded clusters)
- `cse:shardCollection` — sharding event (only emitted on sharded clusters)
- `stage:$listSampledQueries` — sharding/query-analyzer administration
- `stage:$shardedDataDistribution` — sharding/query-analyzer administration

## Replication (18)

- `cmd:appendOplogNote` [engine today: unknown] — replica-set administration/internal replication command, not issued by application code
- `cmd:applyOps` [engine today: unknown] — replica-set administration/internal replication command, not issued by application code
- `cmd:replSetAbortPrimaryCatchUp` [engine today: unknown] — replica-set administration/internal replication command, not issued by application code
- `cmd:replSetFreeze` [engine today: unknown] — replica-set administration/internal replication command, not issued by application code
- `cmd:replSetGetConfig` [engine today: handled] — replica-set administration/internal replication command, not issued by application code
- `cmd:replSetGetRBID` [engine today: unknown] — replica-set administration/internal replication command, not issued by application code
- `cmd:replSetGetStatus` [engine today: handled] — replica-set administration/internal replication command, not issued by application code
- `cmd:replSetHeartbeat` [engine today: unknown] — replica-set administration/internal replication command, not issued by application code
- `cmd:replSetInitiate` [engine today: unknown] — replica-set administration/internal replication command, not issued by application code
- `cmd:replSetMaintenance` [engine today: unknown] — replica-set administration/internal replication command, not issued by application code
- `cmd:replSetReconfig` [engine today: unknown] — replica-set administration/internal replication command, not issued by application code
- `cmd:replSetRequestVotes` [engine today: unknown] — replica-set administration/internal replication command, not issued by application code
- `cmd:replSetResizeOplog` [engine today: unknown] — replica-set administration/internal replication command, not issued by application code
- `cmd:replSetStepDown` [engine today: unknown] — replica-set administration/internal replication command, not issued by application code
- `cmd:replSetStepUp` [engine today: unknown] — replica-set administration/internal replication command, not issued by application code
- `cmd:replSetSyncFrom` [engine today: unknown] — replica-set administration/internal replication command, not issued by application code
- `cmd:replSetUpdatePosition` [engine today: unknown] — replica-set administration/internal replication command, not issued by application code
- `node:Admin.replSetGetStatus` — calls only commands/stages that are themselves proposed exclusions (replica-set administration/internal replication command, not issued by application code)

## Atlas / Enterprise-only (18)

- `authmech:AWS IAM (MONGODB-AWS)` — Atlas-only per the docs availability table (core/authentication.md): not available in MongoDB Community
- `authmech:Kerberos (GSSAPI)` — Enterprise-only per the docs availability table (core/authentication.md): not available in MongoDB Community
- `authmech:LDAP (PLAIN)` — Enterprise/Atlas-only per the docs availability table (core/authentication.md): not available in MongoDB Community
- `authmech:OIDC (MONGODB-OIDC)` — Enterprise/Atlas-only per the docs availability table (core/authentication.md): not available in MongoDB Community
- `cmd:createSearchIndexes` [engine today: unknown] — Atlas-only service (Atlas Search index management)
- `cmd:dropSearchIndex` [engine today: unknown] — Atlas-only service (Atlas Search index management)
- `cmd:getAuditConfig` [engine today: unknown] — auditing is an Enterprise/Atlas feature
- `cmd:logApplicationMessage` [engine today: unknown] — auditing is an Enterprise/Atlas feature
- `cmd:setAuditConfig` [engine today: unknown] — auditing is an Enterprise/Atlas feature
- `cmd:updateSearchIndex` [engine today: unknown] — Atlas-only service (Atlas Search index management)
- `node:Collection.createSearchIndex` — calls only commands/stages that are themselves proposed exclusions (Atlas-only service)
- `node:Collection.createSearchIndexes` — calls only commands/stages that are themselves proposed exclusions (Atlas-only service)
- `node:Collection.dropSearchIndex` — calls only commands/stages that are themselves proposed exclusions (Atlas-only service)
- `node:Collection.updateSearchIndex` — calls only commands/stages that are themselves proposed exclusions (Atlas-only service)
- `py:pymongo.collection.Collection.create_search_index` — calls only commands/stages that are themselves proposed exclusions (Atlas-only service)
- `py:pymongo.collection.Collection.create_search_indexes` — calls only commands/stages that are themselves proposed exclusions (Atlas-only service)
- `py:pymongo.collection.Collection.drop_search_index` — calls only commands/stages that are themselves proposed exclusions (Atlas-only service)
- `py:pymongo.collection.Collection.update_search_index` — calls only commands/stages that are themselves proposed exclusions (Atlas-only service)

## Removed / 8.0-only (3)

- `node:MongoClient.bulkWrite` — client-level bulkWrite needs the MongoDB 8.0 `bulkWrite` command (absent from the 7.0 manual)
- `py:pymongo.mongo_client.MongoClient.bulk_write` — client-level bulkWrite needs the MongoDB 8.0 `bulkWrite` command (absent from the 7.0 manual)
- `wire:OP_INSERT` — legacy opcode removed in MongoDB 5.1 (the 7.0 manual marks it removed)

## Undocumented / not in the 7.0 manual (8)

- `cmd:analyze` [engine today: unknown] — listed by listCommands on mongo 7.0.45 but absent from the 7.0 manual (undocumented, no driver API uses it)
- `cmd:bulkWrite` [engine today: unknown] — no page in the 7.0 manual (docs_url is the command index, which does not list it)
- `cmd:dbCheck` [engine today: unknown] — listed by listCommands on mongo 7.0.45 but absent from the 7.0 manual (undocumented, no driver API uses it)
- `cmd:features` [engine today: handled] — listed by listCommands on mongo 7.0.45 but absent from the 7.0 manual (undocumented, no driver API uses it)
- `cmd:getChangeStreamState` [engine today: unknown] — listed by listCommands on mongo 7.0.45 but absent from the 7.0 manual (undocumented, no driver API uses it)
- `cmd:getDiagnosticData` [engine today: unknown] — listed by listCommands on mongo 7.0.45 but absent from the 7.0 manual (undocumented, no driver API uses it)
- `cmd:setChangeStreamState` [engine today: unknown] — listed by listCommands on mongo 7.0.45 but absent from the 7.0 manual (undocumented, no driver API uses it)
- `cmd:setProfilingFilterGlobally` [engine today: unknown] — listed by listCommands on mongo 7.0.45 but absent from the 7.0 manual (undocumented, no driver API uses it)

## Other (1)

- `js:security.javascriptEnabled` — mongod startup configuration option (security.javascriptEnabled), not reachable through a driver

## Internal server commands (133)

Reason: internal server-to-server command, not callable by applications; no page in the 7.0 manual (docs_url is the command index, which does not list it)

Examples: `cmd:_addShard`, `cmd:_configsvrAbortReshardCollection`, `cmd:_configsvrAddShard`, `cmd:_configsvrAddShardToZone`, `cmd:_configsvrBalancerCollectionStatus`, `cmd:_configsvrBalancerStart`, `cmd:_configsvrBalancerStatus`, `cmd:_configsvrBalancerStop`, `cmd:_configsvrCheckClusterMetadataConsistency`, `cmd:_configsvrCheckMetadataConsistency` …

## Client-only driver members (269)

Reason: client-side only: the driver sends no request of its own for this member, so there is nothing for the engine to implement

Examples: `node:AbstractCursor.CLOSE`, `node:AbstractCursor.bufferedCount`, `node:AbstractCursor.closed`, `node:AbstractCursor.id`, `node:AbstractCursor.killed`, `node:AbstractCursor.loadBalanced`, `node:AbstractCursor.map`, `node:AbstractCursor.namespace`, `node:AbstractCursor.readBufferedDocuments`, `node:AbstractCursor.readConcern` …

## Client-side encryption helpers (47)

Reason: client-side field-level encryption helper (libmongocrypt/KMS); the server only stores BSON binData subtype 6 and key-vault documents, no engine surface of its own

Examples: `node:ClientEncryption.addKeyAltName`, `node:ClientEncryption.constructor`, `node:ClientEncryption.createDataKey`, `node:ClientEncryption.createEncryptedCollection`, `node:ClientEncryption.decrypt`, `node:ClientEncryption.deleteKey`, `node:ClientEncryption.encrypt`, `node:ClientEncryption.encryptExpression`, `node:ClientEncryption.getKey`, `node:ClientEncryption.getKeyByAltName` …


# New exclusion candidates found while closing the gaps (my proposals, NOT yet in the inventory)

These rows are still not covered. For each group I say why an exclusion could be argued; I have not marked any of them. Everything else that is not covered stays on the open list (see results-gaps.md, section 'Open').

## Atlas-only (Search / Vector Search service) (5)

- `stage:$searchMeta` (untested)
- `stage:$vectorSearch` (missing)
- `stage:$listSearchIndexes` (missing)
- `py:pymongo.collection.Collection.list_search_indexes` (missing)
- `node:Collection.listSearchIndexes` (missing)

## Client-side encryption helper command (1)

- `cmd:compactStructuredEncryptionData` (missing)

## Cannot be produced by a single-node deployment (1)

- `txn:UnknownTransactionCommitResult` (untested)

## Unsupported by design / driver-only members with no request of their own (15)

- `py:pymongo.command_cursor.RawBatchCommandCursor.close` (untested)
- `py:pymongo.command_cursor.RawBatchCommandCursor.next` (untested)
- `py:pymongo.command_cursor.RawBatchCommandCursor.to_list` (untested)
- `py:pymongo.command_cursor.RawBatchCommandCursor.try_next` (untested)
- `py:gridfs.GridFS.find` (untested)
- `py:gridfs.GridFS.find_one` (untested)
- `py:gridfs.GridFS.get_version` (untested)
- `py:gridfs.GridFS.new_file` (untested)
- `py:gridfs.grid_file.GridIn.abort` (untested)
- `py:gridfs.grid_file.GridIn.writelines` (untested)
- `py:gridfs.grid_file.GridOut.writelines` (untested)
- `node:MongoClient.connect-2` (untested)
- `node:GridFSBucketReadStream.abort` (untested)
- `node:GridFSBucketReadStream.end` (untested)
- `node:GridFSBucketReadStream.start` (untested)
