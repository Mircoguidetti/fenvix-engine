//! `replSetGetStatus` and `replSetGetConfig` for the one-member replica set this server presents.

use crate::cmd::{Server, R};
use crate::error::MongoError;
use bson::{doc, Bson, DateTime, Document, Timestamp};

/// Set by the `shutdown` command: the accept loop stops and the tenants are closed cleanly, as on SIGTERM.
pub static SHUTDOWN: tokio::sync::Notify = tokio::sync::Notify::const_new();

impl Server {
    /// `shutdown`: checked here; the connection is closed without a reply by the connection loop (the `$shutdown` marker).
    pub(crate) fn shutdown_cmd(&self, db: &str, cmd: &Document) -> R<Document> {
        crate::fields::check("shutdown", cmd)?;
        if db != "admin" {
            return Err(MongoError::new(13, "Unauthorized", "shutdown may only be run against the admin database.".into()));
        }
        if let Some(f) = cmd.get("force") {
            if !matches!(f, Bson::Boolean(_)) {
                return Err(MongoError::type_mismatch(format!("BSON field 'shutdown.force' is the wrong type '{}', expected type 'bool'", crate::update::type_name(f))));
            }
        }
        if let Some(t) = cmd.get("timeoutSecs") {
            if !matches!(t, Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_)) {
                return Err(MongoError::type_mismatch(format!("BSON field 'shutdown.timeoutSecs' is the wrong type '{}', expected types '[long, int, decimal, double']", crate::update::type_name(t))));
            }
        }
        Ok(doc! { "$shutdown": true })
    }

    pub(crate) fn repl_cmd(&self, name: &str, db: &str) -> R<Document> {
        if db != "admin" {
            return Err(MongoError::new(13, "Unauthorized", format!("{name} may only be run against the admin database.")));
        }
        let Some((set, addr)) = self.rs.lock().unwrap().clone() else {
            return Err(MongoError::new(76, "NoReplicationEnabled", "not running with --replSet".into()));
        };
        let ts: Timestamp = self.dbs.changes.cluster_now();
        let optime = || doc! { "ts": Bson::Timestamp(ts), "t": 1_i64 };
        let now = DateTime::now();
        let wall = DateTime::from_millis(ts.time as i64 * 1000);
        let up = self.diag.start.elapsed();
        let started = DateTime::from_millis(DateTime::now().timestamp_millis() - up.as_millis() as i64);
        let started_s = DateTime::from_millis(started.timestamp_millis() / 1000 * 1000);
        if name == "replSetGetConfig" {
            static RS_ID: std::sync::OnceLock<bson::oid::ObjectId> = std::sync::OnceLock::new();
            let id = *RS_ID.get_or_init(bson::oid::ObjectId::new);
            return Ok(doc! { "config": {
                "_id": set, "version": 1_i32, "term": 1_i64,
                "members": [{ "_id": 0_i32, "host": addr, "arbiterOnly": false, "buildIndexes": true, "hidden": false, "priority": 1.0, "tags": {}, "secondaryDelaySecs": 0_i64, "votes": 1_i32 }],
                "protocolVersion": 1_i64, "writeConcernMajorityJournalDefault": true,
                "settings": { "chainingAllowed": true, "heartbeatIntervalMillis": 2000_i32, "heartbeatTimeoutSecs": 10_i32, "electionTimeoutMillis": 10000_i32, "catchUpTimeoutMillis": -1_i64, "catchUpTakeoverDelayMillis": 30000_i32, "getLastErrorModes": {}, "getLastErrorDefaults": { "w": 1_i32, "wtimeout": 0_i32 }, "replicaSetId": id },
            }, "ok": 1.0 });
        }
        Ok(doc! {
            "set": set, "date": now, "myState": 1_i32, "term": 1_i64, "syncSourceHost": "", "syncSourceId": -1_i32,
            "heartbeatIntervalMillis": 2000_i64, "majorityVoteCount": 1_i32, "writeMajorityCount": 1_i32, "votingMembersCount": 1_i32, "writableVotingMembersCount": 1_i32,
            "optimes": {
                "lastCommittedOpTime": optime(), "lastCommittedWallTime": wall, "readConcernMajorityOpTime": optime(), "appliedOpTime": optime(), "durableOpTime": optime(),
                "lastAppliedWallTime": wall, "lastDurableWallTime": wall,
            },
            "lastStableRecoveryTimestamp": Bson::Timestamp(ts),
            "electionCandidateMetrics": {
                "lastElectionReason": "electionTimeout", "lastElectionDate": started, "electionTerm": 1_i64,
                "lastCommittedOpTimeAtElection": { "ts": Bson::Timestamp(Timestamp { time: 0, increment: 0 }), "t": -1_i64 },
                "lastSeenOpTimeAtElection": { "ts": Bson::Timestamp(Timestamp { time: 0, increment: 0 }), "t": -1_i64 },
                "numVotesNeeded": 1_i32, "priorityAtElection": 1.0, "electionTimeoutMillis": 10000_i64, "newTermStartDate": started, "wMajorityWriteAvailabilityDate": started,
            },
            "members": [{
                "_id": 0_i32, "name": addr, "health": 1.0, "state": 1_i32, "stateStr": "PRIMARY", "uptime": up.as_secs() as i64,
                "optime": optime(), "optimeDate": DateTime::from_millis(ts.time as i64 * 1000), "lastAppliedWallTime": wall, "lastDurableWallTime": wall,
                "syncSourceHost": "", "syncSourceId": -1_i32, "infoMessage": "",
                "electionTime": Bson::Timestamp(Timestamp { time: (started_s.timestamp_millis() / 1000) as u32, increment: 1 }), "electionDate": started_s,
                "configVersion": 1_i32, "configTerm": 1_i32, "self": true, "lastHeartbeatMessage": "",
            }],
            "ok": 1.0,
        })
    }
}
