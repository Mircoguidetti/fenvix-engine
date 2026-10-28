//! Read concern: the `readConcern` option of the read commands. Every read sees the latest data of the one node, so a level
//! changes nothing about what is read; what matters is which combinations mongod accepts, and `afterClusterTime`.

use crate::cmd::{Server, R};
use crate::error::MongoError;
use bson::{Bson, Document};

const READS: [&str; 6] = ["find", "aggregate", "count", "distinct", "mapReduce", "mapreduce"];
const LEVELS: [&str; 5] = ["local", "majority", "linearizable", "available", "snapshot"];

impl Server {
    /// Check the `readConcern` of a read command before it runs.
    pub(crate) fn check_read_concern(&self, name: &str, cmd: &Document) -> R<()> {
        if !READS.contains(&name) || cmd.contains_key("autocommit") {
            return Ok(());
        }
        let rc = match cmd.get("readConcern") {
            Some(Bson::Document(d)) => d,
            Some(_) => return Err(MongoError::failed_to_parse("readConcern field should be an object".into())),
            None => return Ok(()),
        };
        let mut level: Option<&str> = None;
        for (k, v) in rc {
            match k.as_str() {
                "level" => match v {
                    Bson::String(s) if LEVELS.contains(&s.as_str()) => level = Some(s.as_str()),
                    Bson::String(_) => return Err(MongoError::failed_to_parse("readConcern.level must be either 'local', 'majority', 'linearizable', 'available', or 'snapshot'".into())),
                    other => return Err(MongoError::type_mismatch(format!("\"level\" had the wrong type. Expected string, found {}", crate::update::type_name(other)))),
                },
                "afterClusterTime" => match v {
                    Bson::Timestamp(t) if t.time == 0 && t.increment == 0 => return Err(MongoError::new(72, "InvalidOptions", "afterClusterTime cannot be a null timestamp".into())),
                    Bson::Timestamp(_) => {}
                    other => return Err(MongoError::type_mismatch(format!("\"afterClusterTime\" had the wrong type. Expected timestamp, found {}", crate::update::type_name(other)))),
                },
                "afterOpTime" | "atClusterTime" | "provenance" => {}
                other => return Err(MongoError::new(72, "InvalidOptions", format!("Unrecognized option in readConcern: {other}"))),
            }
        }
        let after = match rc.get("afterClusterTime") {
            Some(Bson::Timestamp(t)) => Some(*t),
            _ => None,
        };
        if after.is_some() && rc.contains_key("afterOpTime") {
            return Err(MongoError::new(72, "InvalidOptions", "Can not specify both afterClusterTime and afterOpTime".into()));
        }
        if after.is_some() && !matches!(level, None | Some("majority") | Some("local") | Some("snapshot")) {
            return Err(MongoError::new(72, "InvalidOptions", "afterClusterTime field can be set only if level is equal to majority, local, or snapshot".into()));
        }
        if name == "count" && level == Some("snapshot") {
            // (listed in mongod's own order)
            let mut shown = Document::new();
            for k in ["level", "afterClusterTime", "afterOpTime", "atClusterTime"] {
                if let Some(v) = rc.get(k) {
                    shown.insert(k, v.clone());
                }
            }
            shown.insert("provenance", rc.get("provenance").cloned().unwrap_or_else(|| Bson::String("clientSupplied".into())));
            return Err(MongoError::new(72, "InvalidOptions", format!("Command count does not support {{ readConcern: {} }} :: caused by :: read concern snapshot not supported", crate::update::fmt_doc(&shown))));
        }
        if !self.is_rs() {
            match level {
                Some("snapshot") => return Err(MongoError::new(123, "NotAReplicaSet", "node needs to be a replica set member to use readConcern: snapshot".into())),
                Some("linearizable") => return Err(MongoError::new(123, "NotAReplicaSet", "node needs to be a replica set member to use read concern".into())),
                _ => {}
            }
            if after.is_some() {
                return Err(MongoError::new(20, "IllegalOperation", "Cannot specify afterClusterTime readConcern without replication enabled".into()));
            }
            return Ok(());
        }
        if let Some(t) = after {
            let now = self.dbs.changes.cluster_now();
            if (t.time, t.increment) > (now.time, now.increment) {
                return Err(MongoError::new(
                    72,
                    "InvalidOptions",
                    format!("readConcern afterClusterTime value must not be greater than the current clusterTime. Requested clusterTime: {{ ts: Timestamp({}, {}) }}; current clusterTime: {{ ts: Timestamp({}, {}) }}", t.time, t.increment, now.time, now.increment),
                ));
            }
        }
        Ok(())
    }
}
