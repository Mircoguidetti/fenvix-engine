# Exclusion decisions (founder-approved)

Decision applied by `compat/apply_exclusions.py` (column `exclusion_approved` of `compat/inventory.csv`). Only the groups below are excluded; every other row, including the ones proposed earlier and put back in scope, counts in the denominator until it is covered.

| Group | Rows | Reason |
|---|---|---|
| A1 Client-only driver members | 269 | client-side only: the driver sends no request of its own for this member, so there is nothing for the engine to implement |
| A1 Client-side encryption helpers | 47 | client-side field-level encryption helper (libmongocrypt/KMS) |
| A1 Internal server-to-server commands | 133 | internal server-to-server command, not callable by applications |
| A2 Replication | 15 | replica-set administration/internal replication command, not issued by application code |
| A2 Sharding | 40 | sharding administration |
| A3 Audit (Enterprise/Atlas) | 3 | auditing is an Enterprise/Atlas feature |
| A3 Enterprise/Atlas-only authentication mechanisms | 4 | Enterprise/Atlas-only per the docs availability table (core/authentication.md): not available in MongoDB Community |
| A3 Removed in 5.1 / 8.0-only | 3 | client-level bulkWrite needs the MongoDB 8.0 `bulkWrite` command (absent from the 7.0 manual) |
| A3 Undocumented commands | 7 | listed by listCommands on mongo 7.0.45 but absent from the 7.0 manual (undocumented, no driver API uses it) |
| A3 security.javascriptEnabled (startup option) | 1 | mongod startup configuration option (security.javascriptEnabled), not reachable through a driver |
| A4 Not producible / not in 7.0 Community | 2 | A4: $vectorSearch does not exist in MongoDB 7.0 Community (Atlas Vector Search only) |
| Purely local driver member (founder-allowed, `apply_local_exclusions.py`) | 1 | `node:MongoClient.connect-2`: a second connect() on a connected client sends no request |
| **Total** | **525** | |

## Put back in scope (proposed earlier, not excluded)

Admin commands `shutdown, setParameter, setDefaultRWConcern, fsync, fsyncUnlock, logRotate, setClusterParameter, replSetGetStatus, replSetGetConfig, features`; the small admin commands `dropConnections, setFeatureCompatibilityVersion, rotateCertificates, setUserWriteBlockMode`; search-index commands `createSearchIndexes, dropSearchIndex, updateSearchIndex, listSearchIndexes` with their driver members (`create_search_index(es)`, `drop_search_index`, `update_search_index`, Node `createSearchIndex(es)`, `dropSearchIndex`, `updateSearchIndex`, `Admin.replSetGetStatus`); `$search`, `$searchMeta`; GridFS legacy and raw-batch cursor members that send requests; `compactStructuredEncryptionData`.

## A1 Client-only driver members (269)

Examples: `node:AbstractCursor.CLOSE`, `node:AbstractCursor.bufferedCount`, `node:AbstractCursor.closed`, `node:AbstractCursor.id`, `node:AbstractCursor.killed`, `node:AbstractCursor.loadBalanced`, `node:AbstractCursor.map`, `node:AbstractCursor.namespace`, `node:AbstractCursor.readBufferedDocuments`, `node:AbstractCursor.readConcern` … (full list: rows with `exclusion_approved=yes` in the inventory)

## A1 Client-side encryption helpers (47)

Examples: `node:ClientEncryption.addKeyAltName`, `node:ClientEncryption.constructor`, `node:ClientEncryption.createDataKey`, `node:ClientEncryption.createEncryptedCollection`, `node:ClientEncryption.decrypt`, `node:ClientEncryption.deleteKey`, `node:ClientEncryption.encrypt`, `node:ClientEncryption.encryptExpression`, `node:ClientEncryption.getKey`, `node:ClientEncryption.getKeyByAltName` … (full list: rows with `exclusion_approved=yes` in the inventory)

## A1 Internal server-to-server commands (133)

Examples: `cmd:_addShard`, `cmd:_configsvrAbortReshardCollection`, `cmd:_configsvrAddShard`, `cmd:_configsvrAddShardToZone`, `cmd:_configsvrBalancerCollectionStatus`, `cmd:_configsvrBalancerStart`, `cmd:_configsvrBalancerStatus`, `cmd:_configsvrBalancerStop`, `cmd:_configsvrCheckClusterMetadataConsistency`, `cmd:_configsvrCheckMetadataConsistency` … (full list: rows with `exclusion_approved=yes` in the inventory)

## A2 Replication (15)

- `cmd:appendOplogNote`
- `cmd:applyOps`
- `cmd:replSetAbortPrimaryCatchUp`
- `cmd:replSetFreeze`
- `cmd:replSetGetRBID`
- `cmd:replSetHeartbeat`
- `cmd:replSetInitiate`
- `cmd:replSetMaintenance`
- `cmd:replSetReconfig`
- `cmd:replSetRequestVotes`
- `cmd:replSetResizeOplog`
- `cmd:replSetStepDown`
- `cmd:replSetStepUp`
- `cmd:replSetSyncFrom`
- `cmd:replSetUpdatePosition`

## A2 Sharding (40)

Examples: `cmd:addShard`, `cmd:addShardToZone`, `cmd:analyzeShardKey`, `cmd:balancerCollectionStatus`, `cmd:balancerStart`, `cmd:balancerStatus`, `cmd:balancerStop`, `cmd:checkMetadataConsistency`, `cmd:cleanupOrphaned`, `cmd:cleanupReshardCollection` … (full list: rows with `exclusion_approved=yes` in the inventory)

## A3 Audit (Enterprise/Atlas) (3)

- `cmd:getAuditConfig`
- `cmd:logApplicationMessage`
- `cmd:setAuditConfig`

## A3 Enterprise/Atlas-only authentication mechanisms (4)

- `authmech:AWS IAM (MONGODB-AWS)`
- `authmech:Kerberos (GSSAPI)`
- `authmech:LDAP (PLAIN)`
- `authmech:OIDC (MONGODB-OIDC)`

## A3 Removed in 5.1 / 8.0-only (3)

- `node:MongoClient.bulkWrite`
- `py:pymongo.mongo_client.MongoClient.bulk_write`
- `wire:OP_INSERT`

## A3 Undocumented commands (7)

- `cmd:analyze`
- `cmd:bulkWrite`
- `cmd:dbCheck`
- `cmd:getChangeStreamState`
- `cmd:getDiagnosticData`
- `cmd:setChangeStreamState`
- `cmd:setProfilingFilterGlobally`

## A3 security.javascriptEnabled (startup option) (1)

- `js:security.javascriptEnabled`

## A4 Not producible / not in 7.0 Community (2)

- `stage:$vectorSearch`
- `txn:UnknownTransactionCommitResult`
