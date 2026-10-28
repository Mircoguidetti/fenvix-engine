//! Small administration commands: `dropConnections`, `rotateCertificates`, `setUserWriteBlockMode`, `setFeatureCompatibilityVersion`,
//! `logRotate`, and the gate that holds or refuses write commands while the server is fsync-locked or user writes are blocked.

use crate::cmd::{int_arg, ok, Server, R};
use crate::error::MongoError;
use bson::{doc, Bson, Document};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// `setUserWriteBlockMode: {global: true}`: user writes are refused with code 371.
pub static USER_WRITES_BLOCKED: AtomicBool = AtomicBool::new(false);

fn fcv_cell() -> &'static Mutex<String> {
    static C: OnceLock<Mutex<String>> = OnceLock::new();
    C.get_or_init(|| Mutex::new("7.0".to_string()))
}

/// The feature compatibility version (`getParameter: {featureCompatibilityVersion: 1}`).
pub fn fcv() -> String {
    fcv_cell().lock().unwrap().clone()
}

fn type_err(field: &str, v: &Bson, expected: &str) -> MongoError {
    MongoError::type_mismatch(format!("BSON field '{field}' is the wrong type '{}', expected {expected}", crate::update::type_name(v)))
}

fn missing(field: &str) -> MongoError {
    MongoError::new(40414, "IDLFailedToParse", format!("BSON field '{field}' is missing but a required field"))
}

fn admin_only(db: &str, name: &str) -> R<()> {
    if db == "admin" {
        Ok(())
    } else {
        Err(MongoError::new(13, "Unauthorized", format!("{name} may only be run against the admin database.")))
    }
}

fn truthy(v: &Bson) -> bool {
    match v {
        Bson::Boolean(b) => *b,
        Bson::Int32(i) => *i != 0,
        Bson::Int64(i) => *i != 0,
        Bson::Double(f) => *f != 0.0,
        Bson::Null | Bson::Undefined => false,
        _ => true,
    }
}

/// `logRotate`: `true`/a non-zero number or `"server"`; there is no audit log.
pub fn log_rotate(db: &str, cmd: &Document) -> R<Document> {
    admin_only(db, "logRotate")?;
    match cmd.get("logRotate") {
        Some(Bson::String(s)) if s == "server" => Ok(ok()),
        Some(Bson::String(_)) => Err(MongoError::new(4, "NoSuchKey", "Log rotation failed due to one or more errors".into())),
        Some(v @ (Bson::Boolean(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_))) => {
            if truthy(v) {
                Ok(ok())
            } else {
                Err(MongoError::new(37, "FileRenameFailed", "Log rotation failed due to one or more errors".into()))
            }
        }
        Some(v) => Err(type_err("logRotate.logRotate", v, "types '[bool, string']")),
        None => Ok(ok()),
    }
}

impl Server {
    /// `compactStructuredEncryptionData`: validated like mongod; the engine keeps no ESC/ECOC side collections, so there is nothing to
    /// compact and the statistics are zeros.
    pub(crate) async fn compact_structured_encryption_data(&self, db: &str, cmd: &Document) -> R<Document> {
        crate::fields::check("compactStructuredEncryptionData", cmd)?;
        let coll = match cmd.get("compactStructuredEncryptionData") {
            Some(Bson::String(s)) => s.clone(),
            Some(v) => return Err(MongoError::new(2, "BadValue", format!("collection name has invalid type {}", crate::update::type_name(v)))),
            None => return Err(missing("compactStructuredEncryptionData.compactStructuredEncryptionData")),
        };
        let tokens = match cmd.get("compactionTokens") {
            Some(Bson::Document(d)) => d.clone(),
            Some(v) => return Err(type_err("compactStructuredEncryptionData.compactionTokens", v, "type 'object'")),
            None => return Err(missing("compactStructuredEncryptionData.compactionTokens")),
        };
        if cmd.contains_key("writeConcern") {
            return Err(MongoError::new(72, "InvalidOptions", "Command does not support writeConcern".into()));
        }
        let mut g = self.dbs.lock(db).await;
        let names = g.collections().await?;
        if names.is_empty() {
            return Err(MongoError::namespace_not_found(format!("Database '{db}' does not exist")));
        }
        if !names.iter().any(|n| n == &coll) {
            return Err(MongoError::namespace_not_found(format!("Collection '{db}.{coll}' does not exist")));
        }
        let paths: Option<Vec<String>> = g
            .coll_opts(&coll)
            .await?
            .and_then(|o| o.raw.get_document("encryptedFields").ok().and_then(|e| e.get_array("fields").ok().map(|a| a.iter().filter_map(|f| f.as_document().and_then(|d| d.get_str("path").ok().map(String::from))).collect())));
        let Some(paths) = paths else {
            return Err(MongoError::new(6346807, "Location6346807", "Target namespace is not an encrypted collection".into()));
        };
        for p in &paths {
            if !tokens.contains_key(p) {
                return Err(MongoError::new(6346806, "Location6346806", format!("Compaction tokens object is missing compaction token for the encrypted path '{p}'")));
            }
        }
        Ok(doc! { "stats": { "ecoc": { "read": Bson::Int64(0), "deleted": Bson::Int64(0) }, "esc": { "read": Bson::Int64(0), "inserted": Bson::Int64(0), "updated": Bson::Int64(0), "deleted": Bson::Int64(0) } }, "ok": 1.0 })
    }

    /// Commands that need no tenant. `None` when `name` is not one of them.
    pub(crate) fn admin_cmd(&self, name: &str, db: &str, cmd: &Document) -> Option<R<Document>> {
        Some(match name {
            "dropConnections" => self.drop_connections(db, cmd),
            "rotateCertificates" => self.rotate_certificates(db, cmd),
            "setUserWriteBlockMode" => self.set_user_write_block_mode(db, cmd),
            "setFeatureCompatibilityVersion" => self.set_fcv(db, cmd),
            _ => return None,
        })
    }

    fn drop_connections(&self, db: &str, cmd: &Document) -> R<Document> {
        crate::fields::check("dropConnections", cmd)?;
        match cmd.get("hostAndPort") {
            None => return Err(missing("dropConnections.hostAndPort")),
            Some(Bson::Array(a)) => {
                for (i, h) in a.iter().enumerate() {
                    if !matches!(h, Bson::String(_)) {
                        return Err(type_err(&format!("dropConnections.hostAndPort.{i}"), h, "type 'string'"));
                    }
                }
            }
            Some(v) => return Err(type_err("dropConnections.hostAndPort", v, "type 'array'")),
        }
        admin_only(db, "dropConnections")?;
        Ok(ok())
    }

    fn rotate_certificates(&self, db: &str, cmd: &Document) -> R<Document> {
        crate::fields::check("rotateCertificates", cmd)?;
        if let Some(m) = cmd.get("message") {
            if !matches!(m, Bson::String(_)) {
                return Err(type_err("rotateCertificates.message", m, "type 'string'"));
            }
        }
        admin_only(db, "rotateCertificates")?;
        crate::tls::rotate().map_err(|e| MongoError::new(96, "OperationFailed", format!("rotateCertificates failed, the previous certificates stay in use: {e:#}")))?;
        Ok(ok())
    }

    fn set_user_write_block_mode(&self, _db: &str, cmd: &Document) -> R<Document> {
        crate::fields::check("setUserWriteBlockMode", cmd)?;
        let global = match cmd.get("global") {
            None => return Err(missing("setUserWriteBlockMode.global")),
            Some(Bson::Boolean(b)) => *b,
            Some(v) => return Err(type_err("setUserWriteBlockMode.global", v, "type 'bool'")),
        };
        if !self.is_rs() {
            return Err(MongoError::new(20, "IllegalOperation", "setUserWriteBlockMode cannot be run on standalones".into()));
        }
        USER_WRITES_BLOCKED.store(global, Relaxed);
        Ok(ok())
    }

    fn set_fcv(&self, db: &str, cmd: &Document) -> R<Document> {
        crate::fields::check("setFeatureCompatibilityVersion", cmd)?;
        let target = match cmd.get("setFeatureCompatibilityVersion") {
            Some(Bson::String(s)) => s.clone(),
            Some(v) => return Err(type_err("setFeatureCompatibilityVersion.setFeatureCompatibilityVersion", v, "type 'string'")),
            None => return Err(missing("setFeatureCompatibilityVersion.setFeatureCompatibilityVersion")),
        };
        let confirm = match cmd.get("confirm") {
            None => false,
            Some(Bson::Boolean(b)) => *b,
            Some(v @ (Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_))) => truthy(v),
            Some(v) => return Err(type_err("setFeatureCompatibilityVersion.confirm", v, "types '[bool, long, int, decimal, double']")),
        };
        admin_only(db, "setFeatureCompatibilityVersion")?;
        let rank = |v: &str| match v {
            "6.0" => Some(0),
            "6.3" => Some(1),
            "7.0" => Some(2),
            _ => None,
        };
        let Some(t) = rank(&target) else {
            return Err(MongoError::new(4926900, "Location4926900", format!("Invalid feature compatibility version value '{target}'; expected '6.0' or '6.3' or '7.0'. See https://docs.mongodb.com/master/release-notes/6.0-compatibility/#feature-compatibility.")));
        };
        let cur = fcv();
        let c = rank(&cur).unwrap_or(2);
        if !confirm {
            let msg = if t > c {
                format!("Once you have upgraded to {target}, you will not be able to downgrade FCV and binary version without support assistance. Please re-run this command with 'confirm: true' to acknowledge this and continue with the FCV upgrade.")
            } else {
                "Once you have downgraded the FCV, if you choose to downgrade the binary version, it will require support assistance. Please re-run this command with 'confirm: true' to acknowledge this and continue with the FCV downgrade.".to_string()
            };
            return Err(MongoError::new(7369100, "Location7369100", msg));
        }
        if target == "6.3" {
            return Err(MongoError::new(5147403, "Location5147403", format!("cannot set featureCompatibilityVersion to '6.3' while featureCompatibilityVersion is '{cur}'")));
        }
        *fcv_cell().lock().unwrap() = target;
        Ok(ok())
    }

    /// Write commands wait while the server is fsync-locked (until `maxTimeMS`) and fail with 371 while user writes are blocked.
    /// `Ok(Some(reply))` is a reply that replaces the command's execution.
    pub(crate) async fn write_gate(&self, db: &str, name: &str, cmd: &Document) -> R<Option<Document>> {
        if !is_write(name, cmd) {
            return Ok(None);
        }
        if self.diag.fsync_lock_count() > 0 {
            let deadline = int_arg(cmd.get("maxTimeMS")).filter(|m| *m > 0).map(|m| Instant::now() + Duration::from_millis(m as u64));
            while self.diag.fsync_lock_count() > 0 {
                if deadline.map(|d| Instant::now() >= d).unwrap_or(false) {
                    return Err(MongoError::new(50, "MaxTimeMSExpired", "operation exceeded time limit".into()));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        if USER_WRITES_BLOCKED.load(Relaxed) && !matches!(db, "admin" | "local" | "config") {
            return blocked(db, name, cmd);
        }
        Ok(None)
    }
}

/// True for commands that write (or change the catalog).
fn is_write(name: &str, cmd: &Document) -> bool {
    match name {
        "insert" | "update" | "delete" | "findAndModify" | "findandmodify" | "create" | "drop" | "dropDatabase" | "createIndexes" | "dropIndexes" | "collMod"
        | "renameCollection" | "convertToCapped" | "cloneCollectionAsCapped" | "reIndex" | "compact" => true,
        "aggregate" => cmd.get_array("pipeline").map(|p| p.iter().any(|s| s.as_document().map(|d| d.contains_key("$out") || d.contains_key("$merge")).unwrap_or(false))).unwrap_or(false),
        "mapReduce" | "mapreduce" => !matches!(cmd.get("out"), Some(Bson::Document(d)) if d.contains_key("inline")),
        _ => false,
    }
}

fn blocked(db: &str, name: &str, cmd: &Document) -> R<Option<Document>> {
    let e = |m: &str| Err(MongoError::new(371, "UserWritesBlocked", m.to_string()));
    match name {
        "insert" | "delete" => {
            let n_stmts = cmd.get_array(if name == "insert" { "documents" } else { "deletes" }).map(|a| a.len()).unwrap_or(1);
            let ordered = !matches!(cmd.get("ordered"), Some(Bson::Boolean(false)));
            let errs: Vec<Bson> = (0..if ordered { 1 } else { n_stmts }).map(|i| Bson::Document(doc! { "index": i as i32, "code": 371, "errmsg": "User writes blocked" })).collect();
            Ok(Some(doc! { "n": 0, "writeErrors": errs, "ok": 1.0 }))
        }
        "update" => {
            let n_stmts = cmd.get_array("updates").map(|a| a.len()).unwrap_or(1);
            let ordered = !matches!(cmd.get("ordered"), Some(Bson::Boolean(false)));
            let errs: Vec<Bson> = (0..if ordered { 1 } else { n_stmts }).map(|i| Bson::Document(doc! { "index": i as i32, "code": 371, "errmsg": "User writes blocked" })).collect();
            Ok(Some(doc! { "n": 0, "writeErrors": errs, "nModified": 0, "ok": 1.0 }))
        }
        "findAndModify" | "findandmodify" => e("Plan executor error during findAndModify :: caused by :: User writes blocked"),
        "aggregate" => e("PlanExecutor error during aggregation :: caused by :: User writes blocked"),
        "createIndexes" => {
            let coll = cmd.get_str("createIndexes").unwrap_or("");
            let spec = cmd.get_array("indexes").ok().and_then(|a| a.first()).and_then(|b| b.as_document()).cloned().unwrap_or_default();
            let mut s = doc! { "v": 2 };
            s.extend(spec);
            let n = cmd.get_array("indexes").map(|a| a.len()).unwrap_or(1);
            let u = || {
                let b: [u8; 16] = rand::random();
                let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
                format!("{}-{}-{}-{}-{}", &h[..8], &h[8..12], &h[12..16], &h[16..20], &h[20..])
            };
            let (b, c) = (u(), u());
            e(&format!("Index build failed: {b}: Collection {db}.{coll} ( {c} ) :: caused by :: Caught exception during index builder ({b}) initialization on namespace{db}.{coll} ({c}). {n} index specs provided. First index spec: {} :: caused by :: User writes blocked", crate::update::fmt_doc(&s)))
        }
        _ => e("User writes blocked"),
    }
}

/// `maxTimeMS` of any command: a non-negative integral number up to 2147483647 (mongod's generic check; `aggregate` has its own IDL messages;
/// `getMore` checks it itself).
pub fn check_max_time_ms(name: &str, cmd: &Document) -> R<()> {
    let Some(v) = cmd.get("maxTimeMS") else { return Ok(()) };
    if name == "getMore" {
        return Ok(());
    }
    let generic = |m: String| MongoError::new(2, "BadValue", m);
    let f = match v {
        Bson::Int32(i) => *i as f64,
        Bson::Int64(i) => *i as f64,
        Bson::Double(d) => {
            if d.fract() != 0.0 || !d.is_finite() {
                return Err(generic("maxTimeMS has non-integral value".into()));
            }
            *d
        }
        Bson::Decimal128(d) => match d.to_string().parse::<f64>() {
            Ok(x) if x.fract() == 0.0 => x,
            _ => return Err(generic("maxTimeMS has non-integral value".into())),
        },
        other => {
            if name == "aggregate" && !matches!(other, Bson::Null | Bson::Undefined) {
                return Err(type_err("aggregate.maxTimeMS", other, "types '[long, int, decimal, double']"));
            }
            return Err(generic("maxTimeMS must be a number".into()));
        }
    };
    if f < 0.0 && name == "aggregate" {
        return Err(MongoError::new(51024, "Location51024", format!("BSON field 'maxTimeMS' value must be >= 0, actual value '{}'", f as i64)));
    }
    if f < 0.0 || f > 2147483647.0 {
        return Err(generic(format!("{} value for maxTimeMS is out of range [0, 2147483647]", f as i64)));
    }
    Ok(())
}
