//! The fields a command accepts. mongod parses every command with a specification and refuses a field it does not know
//! (`BSON field 'count.min' is an unknown field.`, code 40415); the specifications below are those of the commands that
//! applications send most, in MongoDB 7.0.

use crate::cmd::R;
use crate::error::MongoError;
use bson::Document;

/// Fields every command takes (generic arguments); names starting with `$` are always accepted.
const GENERIC: &[&str] = &[
    "lsid", "txnNumber", "autocommit", "startTransaction", "stmtId", "readConcern", "writeConcern", "comment", "maxTimeMS", "apiVersion", "apiStrict", "apiDeprecationErrors", "mayBypassWriteBlocking", "clientOperationKey", "coordinator", "databaseVersion",
    "shardVersion", "allowImplicitCollectionCreation", "expectPrefix", "help", "maxTimeMSOpOnly", "tracking_info", "needsRetryAttempts", "deprioritizeAsPrimary", "dbVersionIsOldestForPlanning", "serializationContext", "setFeatureCompatibilityVersion",
    "lastKnownCommittedOpTime", "term", "txnRetryCounter", "mongos", "uuid_",
];

fn specific(name: &str) -> Option<&'static [&'static str]> {
    Some(match name {
        "find" => &[
            "find", "filter", "projection", "sort", "hint", "collation", "skip", "limit", "batchSize", "singleBatch", "max", "min", "returnKey", "showRecordId", "tailable", "oplogReplay", "noCursorTimeout", "awaitData", "allowPartialResults", "let", "allowDiskUse",
            "readOnce", "allowSpeculativeMajorityRead", "requestResumeToken", "resumeAfter", "runtimeConstants",
        ],
        "count" => &["count", "query", "limit", "skip", "hint", "collation"],
        "distinct" => &["distinct", "key", "query", "collation", "hint", "allowDiskUse", "runtimeConstants"],
        "aggregate" => &[
            "aggregate", "pipeline", "cursor", "explain", "allowDiskUse", "bypassDocumentValidation", "collation", "hint", "let", "fromMongos", "needsMerge", "mergeByPBRT", "exchange", "isMapReduceCommand", "runtimeConstants", "collectionUUID", "use44SortKeys",
            "encryptionInformation", "isClusterQueryWithoutShardKeyCmd", "passthroughToShard", "requestReshardingResumeToken", "generateV2ResumeTokens", "includeQueryStatsMetrics", "serializeForQueryAnalysis", "fromRouter",
        ],
        "insert" => &["insert", "documents", "ordered", "bypassDocumentValidation", "encryptionInformation", "collectionUUID", "sampleId"],
        "update" => &["update", "updates", "ordered", "bypassDocumentValidation", "let", "encryptionInformation", "runtimeConstants", "sampleId", "collectionUUID"],
        "delete" => &["delete", "deletes", "ordered", "let", "encryptionInformation", "runtimeConstants", "sampleId", "collectionUUID"],
        "findAndModify" | "findandmodify" => &[
            "findAndModify", "findandmodify", "query", "fields", "sort", "update", "remove", "new", "upsert", "bypassDocumentValidation", "collation", "arrayFilters", "hint", "let", "runtimeConstants", "encryptionInformation", "sampleId", "collectionUUID",
        ],
        "getMore" => &["getMore", "collection", "batchSize"],
        "killCursors" => &["killCursors", "cursors"],
        _ => return None,
    })
}

/// Refuse the first field of `cmd` that the command `name` does not take.
pub fn check(name: &str, cmd: &Document) -> R<()> {
    let Some(allowed) = specific(name) else { return Ok(()) };
    for k in cmd.keys() {
        if k.starts_with('$') || allowed.contains(&k.as_str()) || GENERIC.contains(&k.as_str()) {
            continue;
        }
        return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field '{name}.{k}' is an unknown field.")));
    }
    Ok(())
}
