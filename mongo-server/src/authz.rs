//! Privileges: built-in roles, resources, and the privileges a command needs.

use bson::{doc, Bson, Document};

/// A resource a privilege applies to. `Ns { db: "", coll: "" }` is every non-system collection of every database;
/// `coll == ""` with a database is every non-system collection of that database (and the database itself as a target).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Res {
    Cluster,
    Any,
    /// `{system_buckets: ""}`: the time-series bucket collections of every database.
    Buckets,
    Ns { db: String, coll: String },
}

#[derive(Clone, Debug)]
pub struct Priv {
    pub res: Res,
    pub actions: Vec<String>,
}

pub fn ns(db: &str, coll: &str) -> Res {
    Res::Ns { db: db.to_string(), coll: coll.to_string() }
}

pub fn is_system(coll: &str) -> bool {
    coll.starts_with("system.") && coll != "system.js"
}

impl Res {
    /// Does the privilege resource `self` cover `target`?
    pub fn covers(&self, target: &Res) -> bool {
        match (self, target) {
            (Res::Any, _) => true,
            (Res::Cluster, Res::Cluster) => true,
            (Res::Buckets, Res::Ns { coll, .. }) => coll.starts_with("system.buckets."),
            (Res::Ns { db: pd, coll: pc }, Res::Ns { db, coll }) => {
                (pd.is_empty() || pd == db) && if pc.is_empty() { coll.is_empty() || !is_system(coll) } else { pc == coll }
            }
            _ => false,
        }
    }

    pub fn to_doc(&self) -> Document {
        match self {
            Res::Cluster => doc! { "cluster": true },
            Res::Any => doc! { "anyResource": true },
            Res::Buckets => doc! { "system_buckets": "" },
            Res::Ns { db, coll } => doc! { "db": db.clone(), "collection": coll.clone() },
        }
    }

    pub fn from_doc(d: &Document) -> Option<Res> {
        if d.get_bool("cluster").unwrap_or(false) {
            return Some(Res::Cluster);
        }
        if d.get_bool("anyResource").unwrap_or(false) {
            return Some(Res::Any);
        }
        if d.contains_key("system_buckets") {
            return Some(Res::Buckets);
        }
        Some(Res::Ns { db: d.get_str("db").ok()?.to_string(), coll: d.get_str("collection").ok()?.to_string() })
    }
}

impl Priv {
    pub fn to_doc(&self) -> Document {
        doc! { "resource": self.res.to_doc(), "actions": self.actions.iter().map(|a| Bson::String(a.clone())).collect::<Vec<_>>() }
    }
    pub fn from_doc(d: &Document) -> Option<Priv> {
        Some(Priv { res: Res::from_doc(d.get_document("resource").ok()?)?, actions: d.get_array("actions").ok()?.iter().filter_map(|a| a.as_str().map(String::from)).collect() })
    }
}

/// Built-in roles exactly as mongod 7.0.45 reports them (`rolesInfo` with `showPrivileges`, captured by
/// mongo-tests/gaps/dump_builtin_roles.py): `"tpl.<role>"` for roles granted on a database (the placeholder database is
/// `tpl`), `"admin.<role>"` for roles granted on `admin`. mongod lists them flattened: no role inherits another.
const BUILTIN_JSON: &str = include_str!("builtin_roles.json");

fn table() -> &'static std::collections::HashMap<String, Vec<Priv>> {
    static T: std::sync::OnceLock<std::collections::HashMap<String, Vec<Priv>>> = std::sync::OnceLock::new();
    T.get_or_init(|| {
        let v: serde_json::Value = serde_json::from_str(BUILTIN_JSON).expect("builtin_roles.json");
        let mut m = std::collections::HashMap::new();
        for (k, r) in v.as_object().unwrap() {
            let privs = r["privileges"]
                .as_array()
                .unwrap()
                .iter()
                .map(|p| {
                    let res = &p["resource"];
                    let res = if res["cluster"] == true {
                        Res::Cluster
                    } else if res["anyResource"] == true {
                        Res::Any
                    } else if res.get("system_buckets").is_some() {
                        Res::Buckets
                    } else {
                        Res::Ns { db: res["db"].as_str().unwrap().to_string(), coll: res["collection"].as_str().unwrap().to_string() }
                    };
                    Priv { res, actions: p["actions"].as_array().unwrap().iter().map(|a| a.as_str().unwrap().to_string()).collect() }
                })
                .collect();
            m.insert(k.clone(), privs);
        }
        m
    })
}

/// (privileges, inherited roles) of a built-in role granted on `db`; `None` when `name` is not a built-in role there.
pub fn builtin(name: &str, db: &str) -> Option<(Vec<Priv>, Vec<(String, String)>)> {
    let t = table();
    if db == "admin" {
        return t.get(&format!("admin.{name}")).map(|p| (p.clone(), vec![]));
    }
    let mut privs = t.get(&format!("tpl.{name}"))?.clone();
    for p in privs.iter_mut() {
        if let Res::Ns { db: d, .. } = &mut p.res {
            if d == "tpl" {
                *d = db.to_string();
            }
        }
    }
    Some((privs, vec![]))
}

pub fn is_builtin_role(name: &str, db: &str) -> bool {
    builtin(name, db).is_some()
}

/// Does `privs` allow `action` on `target`?
pub fn allows(privs: &[Priv], target: &Res, action: &str) -> bool {
    privs.iter().any(|p| p.res.covers(target) && p.actions.iter().any(|a| a == action || a == "anyAction"))
}

/// One privilege a command needs.
pub type Need = (Res, &'static str);

fn nsr(db: &str, cmd: &Document, key: &str) -> Res {
    ns(db, cmd.get_str(key).unwrap_or(""))
}

/// The privileges `name` needs (all of them). `None` for commands that need no authorization at all (the
/// handshake, the authentication commands, session bookkeeping).
pub fn required(name: &str, db: &str, cmd: &Document) -> Option<Vec<Need>> {
    let n = || nsr(db, cmd, name);
    let dbr = || ns(db, "");
    let cl = Res::Cluster;
    Some(match name {
        "hello" | "isMaster" | "ismaster" | "ping" | "buildInfo" | "buildinfo" | "saslStart" | "saslContinue" | "authenticate" | "logout" | "whatsmyuri" | "connectionStatus" | "commitTransaction" | "abortTransaction" | "getnonce" | "listCommands" | "hostInfo_" => return None,
        "find" | "count" | "distinct" => vec![(n(), "find")],
        // any authenticated user (the session or cursor must belong to the same user)
        "getMore" | "killCursors" | "endSessions" | "refreshSessions" | "startSession" | "killSessions" => vec![],
        "aggregate" => return Some(aggregate_needs(db, cmd)),
        "insert" => vec![(n(), "insert")],
        "update" => {
            let mut v = vec![(n(), "update")];
            if cmd.get_array("updates").map(|u| u.iter().any(|s| s.as_document().and_then(|d| d.get_bool("upsert").ok()).unwrap_or(false))).unwrap_or(false) {
                v.push((n(), "insert"));
            }
            v
        }
        "delete" => vec![(n(), "remove")],
        "findAndModify" | "findandmodify" => {
            let mut v = vec![(nsr(db, cmd, name), "find")];
            if cmd.get_bool("remove").unwrap_or(false) {
                v.push((nsr(db, cmd, name), "remove"));
            } else {
                v.push((nsr(db, cmd, name), "update"));
                if cmd.get_bool("upsert").unwrap_or(false) {
                    v.push((nsr(db, cmd, name), "insert"));
                }
            }
            v
        }
        "create" => vec![(n(), "createCollection")],
        "drop" => vec![(n(), "dropCollection")],
        "dropDatabase" => vec![(dbr(), "dropDatabase")],
        "createIndexes" => vec![(n(), "createIndex")],
        "dropIndexes" => vec![(n(), "dropIndex")],
        "listIndexes" => vec![(n(), "listIndexes")],
        "listCollections" => vec![(dbr(), "listCollections")],
        "collStats" => vec![(n(), "collStats")],
        "dbStats" => vec![(dbr(), "dbStats")],
        "collMod" => vec![(n(), "collMod")],
        "validate" => vec![(n(), "validate")],
        "explain" => {
            let inner = cmd.get_document("explain").ok()?;
            let iname = inner.keys().next()?.clone();
            return required(&iname, db, inner);
        }
        "renameCollection" => {
            let split = |k: &str| cmd.get_str(k).ok().and_then(|s| s.split_once('.').map(|(d, c)| (d.to_string(), c.to_string())));
            let (from, to) = (split("renameCollection")?, split("to")?);
            if from.0 == to.0 {
                vec![(ns(&from.0, &from.1), "renameCollectionSameDB")]
            } else {
                vec![(ns(&from.0, &from.1), "find"), (ns(&from.0, &from.1), "dropCollection"), (ns(&to.0, &to.1), "insert"), (ns(&to.0, &to.1), "createIndex"), (ns(&to.0, &to.1), "createCollection")]
            }
        }
        "listDatabases" => {
            // without an explicit authorizedDatabases:false a user lacking the privilege gets the databases they can access
            if cmd.get_bool("authorizedDatabases").unwrap_or(true) {
                return Some(vec![]);
            }
            vec![(cl, "listDatabases")]
        }
        "createUser" => vec![(dbr(), "createUser")],
        "updateUser" => {
            let mut v = vec![];
            if cmd.contains_key("pwd") {
                v.push((dbr(), "changePassword"));
            }
            if cmd.contains_key("customData") {
                v.push((dbr(), "changeCustomData"));
            }
            if cmd.contains_key("roles") {
                v.push((dbr(), "revokeRole"));
                v.push((dbr(), "grantRole"));
            }
            if v.is_empty() {
                v.push((dbr(), "changeCustomData"));
            }
            v
        }
        "dropUser" | "dropAllUsersFromDatabase" => vec![(dbr(), "dropUser")],
        "grantRolesToUser" => vec![(dbr(), "grantRole")],
        "revokeRolesFromUser" => vec![(dbr(), "revokeRole")],
        "usersInfo" => vec![(dbr(), "viewUser")],
        "createRole" => vec![(dbr(), "createRole")],
        "updateRole" => vec![(dbr(), "grantRole"), (dbr(), "revokeRole")],
        "dropRole" | "dropAllRolesFromDatabase" => vec![(dbr(), "dropRole")],
        "grantRolesToRole" | "grantPrivilegesToRole" => vec![(dbr(), "grantRole")],
        "revokeRolesFromRole" | "revokePrivilegesFromRole" => vec![(dbr(), "revokeRole")],
        "rolesInfo" => vec![(dbr(), "viewRole")],
        "invalidateUserCache" => vec![(cl, "invalidateUserCache")],
        "getParameter" => vec![(cl, "getParameter")],
        "serverStatus" => vec![(cl, "serverStatus")],
        "currentOp" => vec![(cl, "inprog")],
        "killOp" => vec![(cl, "killop")],
        "top" => vec![(cl, "top")],
        "hostInfo" => vec![(cl, "hostInfo")],
        "getCmdLineOpts" => vec![(cl, "getCmdLineOpts")],
        "getLog" => vec![(cl, "getLog")],
        "connPoolStats" => vec![(cl, "connPoolStats")],
        "replSetGetStatus" => vec![(cl, "replSetGetStatus")],
        "replSetGetConfig" => vec![(cl, "replSetGetConfig")],
        "shutdown" => vec![(cl, "shutdown")],
        "fsync" => vec![(cl, "fsync")],
        "logRotate" => vec![(cl, "logRotate")],
        "setParameter" => vec![(cl, "setParameter")],
        "dbHash" => vec![(dbr(), "dbHash")],
        "dataSize" | "collstats" => vec![(n(), "collStats")],
        "profile" => vec![(dbr(), "enableProfiler")],
        "compact" => vec![(n(), "compact")],
        "reIndex" => vec![(n(), "reIndex")],
        "convertToCapped" => vec![(n(), "convertToCapped")],
        _ => vec![(Res::Any, "anyAction")], // unknown commands need everything (only __system has it)
    })
}

fn aggregate_needs(db: &str, cmd: &Document) -> Vec<Need> {
    let coll = cmd.get_str("aggregate").unwrap_or("");
    let mut v: Vec<Need> = Vec::new();
    let pipeline = cmd.get_array("pipeline").ok();
    let first = pipeline.and_then(|p| p.first()).and_then(|s| s.as_document()).and_then(|d| d.keys().next().cloned());
    match first.as_deref() {
        Some("$currentOp") => v.push((Res::Cluster, "inprog")),
        Some("$listLocalSessions") | Some("$listSessions") => v.push((Res::Cluster, "listSessions")),
        Some("$documents") => {}
        _ if coll.is_empty() || cmd.get("aggregate") == Some(&Bson::Int32(1)) => {}
        _ => v.push((ns(db, coll), "find")),
    }
    for st in pipeline.into_iter().flatten().filter_map(|s| s.as_document()) {
        let Some(name) = st.keys().next() else { continue };
        match name.as_str() {
            "$out" | "$merge" => {
                let spec = st.get(name);
                let (odb, ocoll) = match spec {
                    Some(Bson::String(c)) => (db.to_string(), c.clone()),
                    Some(Bson::Document(d)) => {
                        let c = match d.get("into").or(d.get("coll")) {
                            Some(Bson::String(c)) => c.clone(),
                            Some(Bson::Document(i)) => i.get_str("coll").unwrap_or("").to_string(),
                            _ => String::new(),
                        };
                        let dbn = match d.get("into") {
                            Some(Bson::Document(i)) => i.get_str("db").ok(),
                            _ => d.get_str("db").ok(),
                        };
                        (dbn.unwrap_or(db).to_string(), c)
                    }
                    _ => (db.to_string(), String::new()),
                };
                v.push((ns(&odb, &ocoll), "insert"));
                if name == "$out" {
                    v.push((ns(&odb, &ocoll), "remove"));
                    v.push((ns(&odb, &ocoll), "createCollection"));
                } else {
                    v.push((ns(&odb, &ocoll), "update"));
                }
            }
            "$lookup" | "$graphLookup" => {
                if let Some(Bson::Document(d)) = st.get(name) {
                    let from = d.get_str("from").unwrap_or("");
                    v.push((ns(db, from), "find"));
                }
            }
            "$unionWith" => {
                let c = match st.get("$unionWith") {
                    Some(Bson::String(c)) => c.clone(),
                    Some(Bson::Document(d)) => d.get_str("coll").unwrap_or("").to_string(),
                    _ => String::new(),
                };
                if !c.is_empty() {
                    v.push((ns(db, &c), "find"));
                }
            }
            "$collStats" => v.push((ns(db, coll), "collStats")),
            "$indexStats" => v.push((ns(db, coll), "indexStats")),
            "$planCacheStats" => v.push((ns(db, coll), "planCacheRead")),
            "$listSessions" | "$listLocalSessions" => v.push((Res::Cluster, "listSessions")),
            "$currentOp" => v.push((Res::Cluster, "inprog")),
            _ => {}
        }
    }
    v
}

/// Names of the built-in roles that can be granted on a database (`admin` has more).
pub fn builtin_names(db: &str) -> Vec<String> {
    let prefix = if db == "admin" { "admin." } else { "tpl." };
    let mut v: Vec<String> = table().keys().filter_map(|k| k.strip_prefix(prefix).map(String::from)).collect();
    v.sort();
    v
}

/// Every action name mongod knows (those that occur in its built-in roles plus the ones that only custom roles use).
pub fn action_known(a: &str) -> bool {
    static ALL: std::sync::OnceLock<std::collections::HashSet<String>> = std::sync::OnceLock::new();
    ALL.get_or_init(|| {
        let mut set: std::collections::HashSet<String> = table().values().flatten().flat_map(|p| p.actions.iter().cloned()).collect();
        for x in ["anyAction", "internal", "bypassDefaultMaxTimeMS", "bypassWriteBlockingMode", "serverStatus", "setUserWriteBlockMode", "applyOps", "oidReset", "collMod", "unlock", "validate", "getDatabaseVersion", "setAuthenticationRestriction", "viewRole", "viewUser", "planCacheIndexFilter", "storageDetails", "setFeatureCompatibilityVersion", "update", "remove", "insert", "find", "listCollections", "listIndexes", "listDatabases", "killop", "inprog", "shutdown", "top", "touch", "unlock", "resync", "replSetConfigure", "replSetGetConfig", "replSetGetStatus", "replSetHeartbeat", "replSetStateChange", "reIndex", "renameCollectionSameDB", "cleanupOrphaned", "flushRouterConfig", "getCmdLineOpts", "getLog", "getParameter", "setParameter", "hostInfo", "netstat", "connPoolStats", "connPoolSync", "closeAllDatabases", "logRotate", "fsync", "dropConnections", "forceUUID", "useUUID", "authSchemaUpgrade", "invalidateUserCache", "changeStream", "startSession", "impersonate", "listSessions", "killAnySession", "killAnyCursor", "trafficRecord", "auditConfigure", "auditLogRotate", "cpuProfiler", "advanceClusterTime", "appendOplogNote", "applicationMessage", "checkFreeMonitoringStatus", "setFreeMonitoring", "cursorInfo", "diagLogging", "dropAllUsers", "indexStats", "emptycapped", "enableProfiler", "queryStatsRead", "queryStatsReadTransformed", "refineCollectionShardKey", "reshardCollection", "shardCollection", "shardedDataDistribution", "splitChunk", "splitVector", "moveChunk", "moveCollection", "unshardCollection", "addShard", "removeShard", "listShards", "getShardMap", "getShardVersion", "shardingState", "clearJumboFlag", "enableSharding", "mergeChunks", "moveChunk"] {
            set.insert(x.to_string());
        }
        set
    })
    .contains(a)
}
