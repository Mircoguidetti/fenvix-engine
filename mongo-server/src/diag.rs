//! Diagnostics and administration commands that applications, drivers and tools send: `serverStatus`, `currentOp` / `killOp`,
//! `top`, `hostInfo`, `getCmdLineOpts`, `connPoolStats`, `listCommands`, `getLog`, `lockInfo`, the read/write concern defaults,
//! cluster parameters, `profile` (with `system.profile`), `dbHash`, `dataSize`, `validateDBMetadata`, `startSession`,
//! `killAllSessions`, the plan cache commands, `reIndex`, `compact`, `filemd5` and `setIndexCommitQuorum`.
//!
//! The shape of `serverStatus`, `hostInfo`, `connPoolStats` and `listCommands` comes from templates captured from mongod 7.0.45
//! (`src/diag/*.json`, canonical extended JSON): every number in them is zero until the live counter of this server replaces it.

use crate::cmd::{bool_arg, collection_name, int_arg, ok, Server, R};
use crate::error::MongoError;
use bson::{doc, Bson, Document};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::Notify;

const SS_SA: &str = include_str!("diag/serverStatus_sa.json");
const SS_RS: &str = include_str!("diag/serverStatus_rs.json");
const HOST_INFO: &str = include_str!("diag/hostInfo_sa.json");
const CONN_POOL: &str = include_str!("diag/connPoolStats_sa.json");
const LIST_COMMANDS: &str = include_str!("diag/listCommands.json");
const FEATURES: &str = include_str!("diag/features.json");

pub const VERSION: &str = "7.0.45";

fn template(src: &str) -> Document {
    match serde_json::from_str::<Bson>(src) {
        Ok(Bson::Document(d)) => d,
        _ => Document::new(),
    }
}

/// The template with every number set to zero (keeping its type).
fn zeroed(d: &Document) -> Document {
    d.iter()
        .map(|(k, v)| {
            let z = match v {
                Bson::Int32(_) => Bson::Int32(0),
                Bson::Int64(_) => Bson::Int64(0),
                Bson::Double(_) if k != "ok" => Bson::Double(0.0),
                Bson::Document(x) => Bson::Document(zeroed(x)),
                Bson::Array(a) => Bson::Array(a.iter().map(|e| if let Bson::Document(x) = e { Bson::Document(zeroed(x)) } else { e.clone() }).collect()),
                other => other.clone(),
            };
            (k.clone(), z)
        })
        .collect()
}

fn set_path(d: &mut Document, path: &str, v: Bson) {
    match path.split_once('.') {
        None => {
            d.insert(path, v);
        }
        Some((h, t)) => {
            if let Some(Bson::Document(x)) = d.get_mut(h) {
                set_path(x, t, v);
            }
        }
    }
}

fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname").map(|s| s.trim().to_string()).ok().filter(|s| !s.is_empty()).unwrap_or_else(|| "localhost".into())
}

fn now_string() -> String {
    chrono_like(bson::DateTime::now())
}

fn chrono_like(t: bson::DateTime) -> String {
    t.try_to_rfc3339_string().map(|s| s.replace('Z', "+00:00")).unwrap_or_default()
}

struct ConnInfo {
    client: String,
    metadata: Option<Document>,
}

/// Lower-case hex of `b` (the key of a logical session).
fn hex_key(b: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push(HEX[(x >> 4) as usize] as char);
        s.push(HEX[(x & 15) as usize] as char);
    }
    s
}

struct OpRec {
    conn: i64,
    db: String,
    name: String,
    /// The command as received (not redacted: see `begin`).
    cmd: Arc<Document>,
    start: Instant,
    kill: Arc<Notify>,
}

#[derive(Default, Clone, Copy)]
struct TopCell {
    time: i64,
    count: i64,
}

#[derive(Default, Clone, Copy)]
struct TopNs {
    total: TopCell,
    read: TopCell,
    write: TopCell,
    queries: TopCell,
    getmore: TopCell,
    insert: TopCell,
    update: TopCell,
    remove: TopCell,
    commands: TopCell,
}

#[derive(Clone)]
pub struct Prof {
    level: i32,
    filter: Option<Document>,
}

impl Default for Prof {
    fn default() -> Self {
        Prof { level: 0, filter: None }
    }
}

pub struct Diag {
    pub(crate) start: Instant,
    /// insert, query, update, delete, getmore, command
    op: [AtomicU64; 6],
    /// inserted, deleted, updated, returned
    docs: [AtomicU64; 4],
    requests: AtomicU64,
    total_conns: AtomicU64,
    cmds: Mutex<BTreeMap<String, (u64, u64)>>,
    ops: Mutex<BTreeMap<i64, OpRec>>,
    next_op: AtomicI64,
    conns: Mutex<HashMap<i64, ConnInfo>>,
    seq: Mutex<HashMap<i64, Vec<String>>>,
    /// Logical sessions seen: lsid (hex) -> (the session id, last use).
    sessions: Mutex<HashMap<String, (Bson, bson::DateTime)>>,
    top: Mutex<BTreeMap<String, TopNs>>,
    prof: Mutex<HashMap<String, Prof>>,
    /// slowms and sampleRate are server-wide
    prof_global: Mutex<(i64, f64)>,
    /// Profile entries waiting to be written, by database, and the databases whose writer task runs.
    prof_queue: Mutex<(HashMap<String, Vec<Document>>, std::collections::HashSet<String>)>,
    pub filters: Mutex<HashMap<String, Vec<Document>>>,
    log: Mutex<(u64, VecDeque<String>)>,
    rwc: Mutex<Option<Document>>,
    cluster: Mutex<Option<Document>>,
    fsync_locks: AtomicI64,
}

impl Default for Diag {
    fn default() -> Self {
        let d = Diag {
            start: Instant::now(),
            op: Default::default(),
            docs: Default::default(),
            requests: AtomicU64::new(0),
            total_conns: AtomicU64::new(0),
            cmds: Default::default(),
            ops: Default::default(),
            next_op: AtomicI64::new(1000),
            conns: Default::default(),
            seq: Default::default(),
            sessions: Default::default(),
            top: Default::default(),
            prof: Default::default(),
            prof_global: Mutex::new((100, 1.0)),
            prof_queue: Default::default(),
            filters: Default::default(),
            log: Default::default(),
            rwc: Default::default(),
            cluster: Default::default(),
            fsync_locks: AtomicI64::new(0),
        };
        d.log_line("I", "CONTROL", 23403, "initandlisten", "Build Info", Some(doc! { "buildInfo": { "version": VERSION, "gitVersion": "none", "modules": [], "allocator": "system", "environment": { "distarch": "x86_64", "target_arch": "x86_64" } } }));
        d.log_line("I", "NETWORK", 23016, "listener", "Waiting for connections", Some(doc! { "port": 0, "ssl": "off" }));
        d
    }
}

impl Diag {
    pub(crate) fn fsync_lock_count(&self) -> i64 {
        self.fsync_locks.load(Relaxed)
    }

    pub fn log_line(&self, sev: &str, comp: &str, id: i32, ctx: &str, msg: &str, attr: Option<Document>) {
        let mut line = doc! { "t": { "$date": chrono_like(bson::DateTime::now()) }, "s": sev, "c": comp, "id": id, "ctx": ctx, "msg": msg };
        if let Some(a) = attr {
            line.insert("attr", a);
        }
        let s = serde_json::to_string(&Bson::Document(line).into_relaxed_extjson()).unwrap_or_default() + "\n";
        let mut l = self.log.lock().unwrap();
        l.0 += 1;
        l.1.push_back(s);
        while l.1.len() > 1024 {
            l.1.pop_front();
        }
    }

    pub fn conn_opened(&self, id: i64, peer: Option<std::net::SocketAddr>) {
        self.total_conns.fetch_add(1, Relaxed);
        self.conns.lock().unwrap().insert(id, ConnInfo { client: peer.map(|p| p.to_string()).unwrap_or_default(), metadata: None });
    }

    pub fn note_seq(&self, id: i64, keys: &[String]) {
        let mut m = self.seq.lock().unwrap();
        if keys.is_empty() {
            m.remove(&id);
        } else {
            m.insert(id, keys.to_vec());
        }
    }

    pub fn conn_closed(&self, id: i64) {
        self.conns.lock().unwrap().remove(&id);
        self.seq.lock().unwrap().remove(&id);
    }

    pub fn set_metadata(&self, id: i64, md: Document) {
        if let Some(c) = self.conns.lock().unwrap().get_mut(&id) {
            if c.metadata.is_none() {
                c.metadata = Some(md);
            }
        }
    }

    /// An operation starts: it is listed by `currentOp` until `end`.
    /// The command is shared with the running operation, not copied: `currentOp` redacts it when it shows it.
    pub fn begin(&self, conn: i64, db: &str, name: &str, cmd: &Arc<Document>) -> (i64, Arc<Notify>) {
        let id = self.next_op.fetch_add(1, Relaxed);
        if let Some(Bson::Binary(b)) = cmd.get_document("lsid").ok().and_then(|l| l.get("id")) {
            let key = hex_key(&b.bytes);
            let now = bson::DateTime::now();
            let mut sessions = self.sessions.lock().unwrap();
            match sessions.get_mut(&key) {
                Some(e) => e.1 = now,
                None => {
                    sessions.insert(key, (Bson::Binary(b.clone()), now));
                }
            }
        }
        let kill = Arc::new(Notify::new());
        self.requests.fetch_add(1, Relaxed);
        self.ops.lock().unwrap().insert(id, OpRec { conn, db: db.to_string(), name: name.to_string(), cmd: Arc::clone(cmd), start: Instant::now(), kill: Arc::clone(&kill) });
        (id, kill)
    }

    /// Record a finished operation: counters, `top`, and the reply's document counts.
    pub fn end(&self, id: i64, db: &str, name: &str, cmd: &Document, r: &R<Document>, micros: i64) {
        self.ops.lock().unwrap().remove(&id);
        KILLED.lock().unwrap().retain(|k| *k != id);
        let (cat, n) = match name {
            "insert" => (0, cmd.get_array("documents").map(|a| a.len()).unwrap_or(1)),
            "find" => (1, 1),
            "update" => (2, cmd.get_array("updates").map(|a| a.len()).unwrap_or(1)),
            "delete" => (3, cmd.get_array("deletes").map(|a| a.len()).unwrap_or(1)),
            "getMore" => (4, 1),
            _ => (5, 1),
        };
        self.op[cat].fetch_add(n as u64, Relaxed);
        {
            let mut c = self.cmds.lock().unwrap();
            let e = c.entry(name.to_string()).or_insert((0, 0));
            e.0 += 1;
            if r.is_err() {
                e.1 += 1;
            }
        }
        if let Ok(d) = r {
            let cnt = |k: &str| d.get(k).and_then(|v| int_arg(Some(v))).unwrap_or(0).max(0) as u64;
            match name {
                "insert" => {
                    self.docs[0].fetch_add(cnt("n"), Relaxed);
                }
                "delete" => {
                    self.docs[1].fetch_add(cnt("n"), Relaxed);
                }
                "update" => {
                    self.docs[2].fetch_add(cnt("nModified") + d.get_array("upserted").map(|a| a.len() as u64).unwrap_or(0), Relaxed);
                }
                "find" | "getMore" | "aggregate" => {
                    if let Ok(c) = d.get_document("cursor") {
                        let n = c.get_array("firstBatch").or_else(|_| c.get_array("nextBatch")).map(|a| a.len()).unwrap_or(0);
                        self.docs[3].fetch_add(n as u64, Relaxed);
                    }
                }
                _ => {}
            }
        }
        let coll = match name {
            "find" | "insert" | "update" | "delete" | "count" | "distinct" | "aggregate" | "findAndModify" | "findandmodify" | "mapReduce" | "createIndexes" | "drop" | "listIndexes" | "create" | "collMod" | "dropIndexes" | "getMore" | "validate" | "collStats" => cmd.get(name).and_then(|v| v.as_str()).or_else(|| cmd.get("collection").and_then(|v| v.as_str())),
            _ => None,
        };
        if let Some(c) = coll {
            let ns = format!("{db}.{c}");
            let mut t = self.top.lock().unwrap();
            let e = t.entry(ns).or_default();
            let bump = |c: &mut TopCell| {
                c.time += micros;
                c.count += 1;
            };
            bump(&mut e.total);
            let (is_write, cell): (bool, fn(&mut TopNs) -> &mut TopCell) = match name {
                "find" => (false, |e| &mut e.queries),
                "getMore" => (false, |e| &mut e.getmore),
                "insert" => (true, |e| &mut e.insert),
                "update" => (true, |e| &mut e.update),
                "delete" => (true, |e| &mut e.remove),
                "findAndModify" | "findandmodify" => (true, |e| &mut e.commands),
                _ => (false, |e| &mut e.commands),
            };
            bump(cell(e));
            if is_write || matches!(name, "createIndexes" | "drop" | "create" | "collMod" | "dropIndexes") {
                bump(&mut e.write);
            } else {
                bump(&mut e.read);
            }
        }
    }

    /// Run `fut`; when `killable` and `killOp` names this operation first, stop it with Interrupted.
    pub async fn guarded<F: std::future::Future<Output = R<Document>>>(&self, kill: Arc<Notify>, killable: bool, fut: F) -> R<Document> {
        if !killable {
            return fut.await;
        }
        tokio::select! {
            r = fut => r,
            _ = kill.notified() => Err(MongoError::new(11601, "Interrupted", "operation was interrupted".into())),
        }
    }

    fn prof_of(&self, db: &str) -> Prof {
        self.prof.lock().unwrap().get(db).cloned().unwrap_or_default()
    }
}

tokio::task_local! {
    /// The id `currentOp` shows for the command the task runs.
    static CUR_OP: i64;
}

static KILLED: Mutex<Vec<i64>> = Mutex::new(Vec::new());

/// Run a command as operation `id`.
/// (A plain function, not an `async fn`: an async wrapper would copy the whole command future into a state of its own.)
pub fn as_op<F: std::future::Future>(id: i64, fut: F) -> impl std::future::Future<Output = F::Output> {
    CUR_OP.scope(id, fut)
}

/// Has `killOp` stopped the operation running on this task? (JavaScript checks this while it runs.)
pub fn current_killed() -> bool {
    CUR_OP.try_with(|id| KILLED.lock().map(|k| k.contains(id)).unwrap_or(false)).unwrap_or(false)
}

/// Does the command run JavaScript? Such a command blocks its thread, so it gets one of its own.
pub fn needs_js(name: &str, cmd: &Document) -> bool {
    fn has(d: &Document) -> bool {
        d.iter().any(|(k, v)| {
            matches!(k.as_str(), "$where" | "$function" | "$accumulator") || match v {
                Bson::Document(x) => has(x),
                Bson::Array(a) => a.iter().any(|e| matches!(e, Bson::Document(x) if has(x))),
                _ => false,
            }
        })
    }
    matches!(name, "mapReduce" | "mapreduce") || has(cmd)
}

/// `$queryStats`: parses the stage; it only runs as the first stage of `{aggregate: 1}` on `admin` (`db` is `None` for a collection
/// pipeline). Query statistics are not collected, so the stage yields nothing.
pub fn query_stats_stage(spec: &Bson, db: Option<&str>) -> R<()> {
    let Bson::Document(o) = spec else {
        return Err(MongoError::new(9, "FailedToParse", format!("$queryStats value must be an object. Found: {}", crate::update::type_name(spec))));
    };
    for (k, v) in o {
        match k.as_str() {
            "transformIdentifiers" => {
                let Bson::Document(t) = v else {
                    return Err(MongoError::type_mismatch(format!("BSON field '$queryStats.transformIdentifiers' is the wrong type '{}', expected type 'object'", crate::update::type_name(v))));
                };
                let mut hmac = false;
                for (tk, tv) in t {
                    match (tk.as_str(), tv) {
                        ("algorithm", Bson::String(a)) if a == "hmac-sha-256" => hmac = true,
                        ("algorithm", Bson::String(a)) => return Err(MongoError::new(2, "BadValue", format!("Enumeration value '{a}' for field '$queryStats.transformIdentifiers.algorithm' is not a valid value."))),
                        ("algorithm", x) => return Err(MongoError::type_mismatch(format!("BSON field '$queryStats.transformIdentifiers.algorithm' is the wrong type '{}', expected type 'string'", crate::update::type_name(x)))),
                        ("hmacKey", Bson::Binary(b)) => {
                            use bson::spec::BinarySubtype as B;
                            if !matches!(b.subtype, B::Sensitive) {
                                let n = match b.subtype {
                                    B::Generic => "general",
                                    B::Function => "function",
                                    B::BinaryOld => "byteArrayDeprecated",
                                    B::UuidOld => "UUIDOld",
                                    B::Uuid => "UUID",
                                    B::Md5 => "MD5",
                                    B::Encrypted => "encrypt",
                                    B::Column => "bsonColumn",
                                    B::UserDefined(_) => "Custom",
                                    _ => "invalid",
                                };
                                return Err(MongoError::type_mismatch(format!("BSON field '$queryStats.transformIdentifiers.hmacKey' is the wrong binData type '{n}', expected type 'sensitive'")));
                            }
                            if b.bytes.len() < 32 {
                                return Err(MongoError::bad_value(format!("hmacKey must be greater than or equal to 32 bytes, found length: {}", b.bytes.len())));
                            }
                        }
                        ("hmacKey", x) => return Err(MongoError::type_mismatch(format!("BSON field '$queryStats.transformIdentifiers.hmacKey' is the wrong type '{}', expected type 'binData'", crate::update::type_name(x)))),
                        (other, _) => return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field '$queryStats.transformIdentifiers.{other}' is an unknown field."))),
                    }
                }
                if !t.contains_key("algorithm") {
                    return Err(MongoError::new(40414, "IDLFailedToParse", "BSON field '$queryStats.transformIdentifiers.algorithm' is missing but a required field".into()));
                }
                if hmac && !t.contains_key("hmacKey") {
                    return Err(MongoError::new(9, "FailedToParse", "The 'hmacKey' parameter of the $queryStats stage must be specified when applying the hmac-sha-256 algorithm".into()));
                }
            }
            _ => return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field '$queryStats.{k}' is an unknown field."))),
        }
    }
    if db != Some("admin") {
        return Err(MongoError::new(73, "InvalidNamespace", "$queryStats must be run against the 'admin' database with {aggregate: 1}".into()));
    }
    Ok(())
}

/// The error of `{aggregate: 1}` when the pipeline does not start with a stage that needs no collection.
pub fn db_level_error(first: Option<&str>) -> MongoError {
    const NEEDS_COLLECTION: &[&str] = &[
        "$match", "$group", "$project", "$sort", "$limit", "$skip", "$unwind", "$lookup", "$facet", "$sample", "$collStats", "$indexStats", "$count", "$addFields", "$set", "$unset", "$replaceRoot", "$replaceWith", "$bucket", "$bucketAuto", "$graphLookup", "$redact",
        "$sortByCount", "$out", "$merge", "$geoNear", "$unionWith", "$setWindowFields", "$densify", "$fill", "$planCacheStats", "$documents",
    ];
    match first {
        None => MongoError::new(73, "InvalidNamespace", "{aggregate: 1} is not valid for an empty pipeline.".into()),
        Some(n) if NEEDS_COLLECTION.contains(&n) => MongoError::new(73, "InvalidNamespace", format!("{{aggregate: 1}} is not valid for '{n}'; a collection is required.")),
        Some("$search") | Some("$listSearchIndexes") | Some("$searchMeta") | Some("$vectorSearch") => MongoError::new(6047401, "Location6047401", format!("{} stage is only allowed on MongoDB Atlas", first.unwrap())),
        Some(n) => MongoError::new(40324, "Location40324", format!("Unrecognized pipeline stage name: '{n}'")),
    }
}

type Job = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

static JS_THREAD: std::sync::OnceLock<std::sync::mpsc::Sender<Job>> = std::sync::OnceLock::new();

/// Run a command that executes JavaScript on the thread that does all of it.
pub async fn run_js<F: std::future::Future<Output = R<Document>> + Send + 'static>(fut: F) -> R<Document> {
    let tx = JS_THREAD.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel::<Job>();
        let handle = tokio::runtime::Handle::current();
        std::thread::Builder::new()
            .name("js".into())
            .stack_size(8 << 20)
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    handle.block_on(job);
                }
            })
            .expect("js thread");
        tx
    });
    let (rtx, rrx) = tokio::sync::oneshot::channel();
    let job: Job = Box::pin(async move {
        let _ = rtx.send(fut.await);
    });
    if tx.send(job).is_err() {
        return Err(MongoError::new(1, "InternalError", "the JavaScript thread is gone".into()));
    }
    rrx.await.unwrap_or_else(|_| Err(MongoError::new(1, "InternalError", "the JavaScript thread dropped the command".into())))
}

/// Commands the engine can stop with `killOp` (a write is never torn: it is applied by one atomic batch).
pub fn killable(name: &str) -> bool {
    matches!(name, "find" | "aggregate" | "count" | "distinct" | "mapReduce" | "mapreduce" | "getMore")
}

fn admin_only(db: &str, name: &str) -> R<()> {
    if db != "admin" {
        return Err(MongoError::new(13, "Unauthorized", format!("{name} may only be run against the admin database.")));
    }
    Ok(())
}

fn bad_type(v: &Bson, field: &str, want: &str) -> MongoError {
    MongoError::new(14, "TypeMismatch", format!("BSON field '{field}' is the wrong type '{}', expected type '{want}'", crate::update::type_name(v)))
}

fn md5_hex(data: &[u8]) -> String {
    use md5::{Digest, Md5};
    let mut h = Md5::new();
    h.update(data);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

impl Server {
    /// Diagnostic and administration commands; `None` when `name` is not one of them.
    /// (The commands that await are boxed: this function runs for every command, and inline they made its future, part of
    /// every command's future, about 14 KB.)
    pub(crate) async fn diag_command(&self, db: &str, name: &str, cmd: &Document) -> Option<R<Document>> {
        Some(match name {
            "serverStatus" => self.server_status(cmd),
            "hostInfo" => Ok(self.host_info()),
            "currentOp" => Box::pin(self.current_op(db, cmd)).await,
            "killOp" => self.kill_op(db, cmd),
            "top" => self.top(db),
            "getCmdLineOpts" => admin_only(db, name).map(|_| self.cmd_line_opts()),
            "connPoolStats" => Ok(self.conn_pool_stats()),
            "listCommands" => Ok(self.list_commands()),
            "features" => Ok(template(FEATURES)),
            "lockInfo" => admin_only(db, name).map(|_| doc! { "lockInfo": [], "ok": 1.0 }),
            "getLog" => self.get_log(db, cmd),
            "getDefaultRWConcern" => self.get_default_rwc(db, cmd),
            "setDefaultRWConcern" => self.set_default_rwc(db, cmd),
            "getClusterParameter" => self.get_cluster_parameter(cmd),
            "setClusterParameter" => self.set_cluster_parameter(cmd),
            "profile" => self.profile(db, cmd),
            "dbHash" => Box::pin(self.db_hash(db, cmd)).await,
            "dataSize" => Box::pin(self.data_size(cmd)).await,
            "validateDBMetadata" => Box::pin(self.validate_db_metadata(cmd)).await,
            "startSession" => {
                let r = start_session();
                if let Some(Bson::Binary(b)) = r.get_document("id").ok().and_then(|i| i.get("id").cloned()) {
                    let key = hex_key(&b.bytes);
                    self.diag.sessions.lock().unwrap().insert(key, (Bson::Binary(b), bson::DateTime::now()));
                }
                Ok(r)
            }
            "killAllSessions" | "killAllSessionsByPattern" => kill_all_sessions(name, cmd),
            "planCacheListFilters" | "planCacheSetFilter" | "planCacheClearFilters" | "planCacheClear" => Box::pin(self.plan_cache(db, name, cmd)).await,
            "reIndex" => Box::pin(self.re_index(db, cmd)).await,
            "compact" => Box::pin(self.compact(db, cmd)).await,
            "filemd5" => Box::pin(self.filemd5(db, cmd)).await,
            "setIndexCommitQuorum" => Box::pin(self.set_index_commit_quorum(db, cmd)).await,
            "logRotate" => crate::admin_cmds::log_rotate(db, cmd),
            "fsync" => self.fsync(db, cmd),
            "fsyncUnlock" => self.fsync_unlock(db),
            "cursorInfo" => Err(MongoError::command_not_found(name)),
            _ => return None,
        })
    }

    /// The documents of a stage that starts a database-level pipeline (`{aggregate: 1}`); `None` for any other stage.
    pub(crate) fn db_level_source(&self, db: &str, name: &str, spec: &Bson) -> Option<R<Vec<Document>>> {
        match name {
            "$currentOp" => Some((|| {
                if db != "admin" {
                    return Err(MongoError::new(73, "InvalidNamespace", "$currentOp must be run against the 'admin' database with {aggregate: 1}".into()));
                }
                let Bson::Document(o) = spec else {
                    return Err(MongoError::new(14, "TypeMismatch", format!("$currentOp options must be specified in an object, but found: {}", crate::update::type_name(spec))));
                };
                for k in o.keys() {
                    if !matches!(k.as_str(), "allUsers" | "idleConnections" | "idleCursors" | "idleSessions" | "localOps" | "backtrace") {
                        return Err(MongoError::new(9, "FailedToParse", format!("Unrecognized option '{k}' in $currentOp stage.")));
                    }
                }
                let mut docs = self.current_op_docs(None, false);
                if bool_arg(o.get("idleConnections"), false) {
                    let busy: Vec<i64> = self.diag.ops.lock().unwrap().values().map(|r| r.conn).collect();
                    let conns = self.diag.conns.lock().unwrap();
                    let mut ids: Vec<&i64> = conns.keys().filter(|c| !busy.contains(c)).collect();
                    ids.sort();
                    for id in ids {
                        let c = &conns[id];
                        let mut d = doc! { "type": "idleConnection", "host": self.host_port(), "desc": format!("conn{id}"), "connectionId": *id as i32, "client": c.client.clone() };
                        if let Some(m) = &c.metadata {
                            d.insert("clientMetadata", m.clone());
                        }
                        d.insert("active", false);
                        d.insert("currentOpTime", now_string());
                        d.insert("threaded", true);
                        docs.push(d);
                    }
                }
                Ok(docs)
            })()),
            "$listLocalSessions" => Some((|| {
                if let Bson::Document(o) = spec {
                    if let Some(k) = o.keys().find(|k| !matches!(k.as_str(), "allUsers" | "users")) {
                        return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field '$listLocalSessions.{k}' is an unknown field.")));
                    }
                }
                let users_only = matches!(spec, Bson::Document(o) if o.get_array("users").map(|u| !u.is_empty()).unwrap_or(false) && !bool_arg(o.get("allUsers"), false));
                if users_only {
                    return Ok(vec![]);
                }
                let mut out: Vec<Document> = Vec::new();
                let cutoff = bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 30 * 60 * 1000);
                self.diag.sessions.lock().unwrap().retain(|_, (_, last)| *last >= cutoff);
                for (_, (id, last)) in self.diag.sessions.lock().unwrap().iter() {
                    out.push(doc! { "_id": { "id": id.clone(), "uid": Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: sha_empty() }) }, "lastUse": Bson::DateTime(*last) });
                }
                Ok(out)
            })()),
            "$queryStats" => Some(query_stats_stage(spec, Some(db)).map(|_| Vec::new())),
            "$listSessions" => Some(Err(MongoError::new(73, "InvalidNamespace", "$listSessions may only be run against config.system.sessions".into()))),
            "$listCachedAndActiveUsers" => Some(Err(MongoError::new(224, "QueryFeatureNotAllowed", "$listCachedAndActiveUsers is not allowed with the current configuration. You may need to enable the corresponding feature flag".into()))),
            _ => None,
        }
    }

    pub(crate) fn is_rs(&self) -> bool {
        self.rs.lock().unwrap().is_some()
    }

    pub(crate) fn host_port_name(&self) -> String {
        hostname()
    }

    pub(crate) fn listen_port(&self) -> i32 {
        let args: Vec<String> = std::env::args().collect();
        args.iter().position(|a| a == "--listen").and_then(|i| args.get(i + 1)).and_then(|l| l.rsplit(':').next()).and_then(|p| p.parse().ok()).unwrap_or(27017)
    }

    fn host_port(&self) -> String {
        match &*self.rs.lock().unwrap() {
            Some((_, addr)) => format!("{}:{}", hostname(), addr.rsplit(':').next().unwrap_or("27017")),
            None => hostname(),
        }
    }

    pub(crate) fn server_status(&self, cmd: &Document) -> R<Document> {
        let rs = self.is_rs();
        let t = template(if rs { SS_RS } else { SS_SA });
        let mut d = zeroed(&t);
        let dg = &self.diag;
        let up = dg.start.elapsed();
        d.insert("host", self.host_port());
        d.insert("version", VERSION);
        d.insert("pid", Bson::Int64(std::process::id() as i64));
        d.insert("uptime", up.as_secs_f64().floor());
        d.insert("uptimeMillis", Bson::Int64(up.as_millis() as i64));
        d.insert("uptimeEstimate", Bson::Int64(up.as_secs() as i64));
        d.insert("localTime", Bson::DateTime(bson::DateTime::now()));
        let conns = dg.conns.lock().unwrap().len() as i32;
        let active = dg.ops.lock().unwrap().len() as i32;
        set_path(&mut d, "connections.current", Bson::Int32(conns));
        set_path(&mut d, "connections.available", Bson::Int32((51200 - conns).max(0)));
        set_path(&mut d, "connections.totalCreated", Bson::Int32(dg.total_conns.load(Relaxed) as i32));
        set_path(&mut d, "connections.active", Bson::Int32(active));
        set_path(&mut d, "connections.threaded", Bson::Int32(conns));
        set_path(&mut d, "flowControl.targetRateLimit", Bson::Int32(1000000000));
        for (i, k) in ["insert", "query", "update", "delete", "getmore", "command"].iter().enumerate() {
            set_path(&mut d, &format!("opcounters.{k}"), Bson::Int64(dg.op[i].load(Relaxed) as i64));
        }
        set_path(&mut d, "network.numRequests", Bson::Int64(dg.requests.load(Relaxed) as i64));
        for (i, k) in ["inserted", "deleted", "updated", "returned"].iter().enumerate() {
            set_path(&mut d, &format!("metrics.document.{k}"), Bson::Int64(dg.docs[i].load(Relaxed) as i64));
        }
        let mut cm = Document::new();
        for (k, (total, failed)) in dg.cmds.lock().unwrap().iter() {
            cm.insert(k.clone(), doc! { "failed": Bson::Int64(*failed as i64), "total": Bson::Int64(*total as i64) });
        }
        set_path(&mut d, "metrics.commands", Bson::Document(cm));
        let open = self.cursors_open() as i64;
        set_path(&mut d, "metrics.cursor.open.total", Bson::Int64(open));
        set_path(&mut d, "metrics.cursor.timedOut", Bson::Int64(crate::cmd::CURSORS_TIMED_OUT.load(Relaxed) as i64));
        // Live heap and what the allocator holds (jemalloc), and the memtable flushes started by age (mem-growth branch).
        let (alloc, resident) = crate::heap_stats();
        set_path(&mut d, "mem.heapAllocatedBytes", Bson::Int64(alloc as i64));
        set_path(&mut d, "mem.heapResidentBytes", Bson::Int64(resident as i64));
        set_path(&mut d, "mem.memtableAgeFlushes", Bson::Int64(tenant_bench::lifecycle::MEMTABLE_AGE_FLUSHES.load(Relaxed) as i64));
        set_path(&mut d, "mem.bits", Bson::Int32(64));
        if let Ok(s) = std::fs::read_to_string("/proc/self/statm") {
            let mut it = s.split_whitespace().filter_map(|x| x.parse::<i64>().ok());
            let (virt, res) = (it.next().unwrap_or(0), it.next().unwrap_or(0));
            set_path(&mut d, "mem.resident", Bson::Int32((res * 4096 / 1048576) as i32));
            set_path(&mut d, "mem.virtual", Bson::Int32((virt * 4096 / 1048576) as i32));
        }
        set_path(&mut d, "mem.supported", Bson::Boolean(true));
        self.real_process_and_cache_stats(&mut d);
        set_path(&mut d, "featureCompatibilityVersion.major", Bson::Int32(7));
        if rs {
            let h = self.hello_doc();
            if let Some(Bson::Document(r)) = d.get_mut("repl") {
                for k in ["hosts", "setName", "setVersion", "primary", "me", "electionId"] {
                    if let Some(v) = h.get(k) {
                        r.insert(k, v.clone());
                    }
                }
            }
        }
        if crate::special_colls::SERIES_SEEN.load(Relaxed) {
            let mut out = Document::new();
            for (k, v) in std::mem::take(&mut d) {
                if k == "catalogStats" {
                    out.insert("bucketCatalog", crate::special_colls::bucket_catalog());
                }
                out.insert(k, v);
            }
            d = out;
        }
        // `section: 0` leaves a section out
        for (k, v) in cmd.iter().skip(1) {
            if matches!(v, Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false)) || matches!(v, Bson::Double(f) if *f == 0.0) {
                d.remove(k);
            }
        }
        d.insert("ok", 1.0);
        Ok(d)
    }

    /// K1: the numbers of this process instead of the template's (which were mongod's of the recording, or zero).
    /// - `extra_info`: CPU of the process (getrusage), peak memory, page faults, context switches, threads;
    /// - `wiredTiger.cache`: the shared block and metadata cache and the object-store traffic, under the
    ///   names mongod uses, so a tool written for mongod reads them (mapping below and in `fenvix.mapping`);
    /// - `fenvix`: the same numbers under their own names, with the S3 request counts.
    fn real_process_and_cache_stats(&self, d: &mut Document) {
        let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) } == 0 {
            let us = |t: libc::timeval| t.tv_sec as i64 * 1_000_000 + t.tv_usec as i64;
            set_path(d, "extra_info.user_time_us", Bson::Int64(us(ru.ru_utime)));
            set_path(d, "extra_info.system_time_us", Bson::Int64(us(ru.ru_stime)));
            set_path(d, "extra_info.maximum_resident_set_kb", Bson::Int64(ru.ru_maxrss as i64));
            set_path(d, "extra_info.input_blocks", Bson::Int64(ru.ru_inblock as i64));
            set_path(d, "extra_info.output_blocks", Bson::Int64(ru.ru_oublock as i64));
            set_path(d, "extra_info.page_reclaims", Bson::Int64(ru.ru_minflt as i64));
            set_path(d, "extra_info.page_faults", Bson::Int64(ru.ru_majflt as i64));
            set_path(d, "extra_info.voluntary_context_switches", Bson::Int64(ru.ru_nvcsw as i64));
            set_path(d, "extra_info.involuntary_context_switches", Bson::Int64(ru.ru_nivcsw as i64));
        }
        if let Some(n) = std::fs::read_to_string("/proc/self/status").ok().and_then(|s| s.lines().find(|l| l.starts_with("Threads:")).and_then(|l| l.split_whitespace().nth(1)).and_then(|x| x.parse::<i64>().ok())) {
            set_path(d, "extra_info.threads", Bson::Int64(n));
        }
        let c = &self.dbs.global.counters;
        let ld = |a: &std::sync::atomic::AtomicU64| a.load(Relaxed) as i64;
        let gets = ld(&c.get) + ld(&c.get_range);
        let puts = ld(&c.put) + ld(&c.multipart_part);
        let cs = tenant_bench::meter::cache_stats().unwrap_or_default();
        let in_cache = (cs.block_usage + cs.meta_usage) as i64;
        let wt = [
            ("bytes currently in the cache", in_cache),
            ("maximum bytes configured", (cs.block_capacity + cs.meta_capacity) as i64),
            ("bytes read into cache", ld(&c.get_bytes)),
            ("pages read into cache", gets),
            ("bytes written from cache", ld(&c.put_bytes)),
            ("pages written from cache", puts),
            ("pages requested from the cache", (cs.hits + cs.misses) as i64),
            ("bytes belonging to page images in the cache", (cs.block_usage) as i64),
            ("pages currently held in the cache", cs.block_entries as i64),
        ];
        for (k, v) in wt {
            if let Some(Bson::Document(w)) = d.get_mut("wiredTiger") {
                if let Some(Bson::Document(cache)) = w.get_mut("cache") {
                    cache.insert(k, Bson::Int64(v));
                    continue;
                }
                w.insert("cache", doc! { k: Bson::Int64(v) });
            } else {
                d.insert("wiredTiger", doc! { "cache": { k: Bson::Int64(v) } });
            }
        }
        d.insert("fenvix", doc! {
            "cache": {
                "blockBytes": cs.block_usage as i64, "blockCapacity": cs.block_capacity as i64, "blockEntries": cs.block_entries as i64,
                "metaBytes": cs.meta_usage as i64, "metaCapacity": cs.meta_capacity as i64,
                "hits": cs.hits as i64, "misses": cs.misses as i64,
                "bytesLoadedByFetch": cs.loaded_bytes as i64, "bytesInserted": cs.inserted_bytes as i64,
            },
            "s3": {
                "get": ld(&c.get), "getRange": ld(&c.get_range), "head": ld(&c.head), "list": ld(&c.list), "put": ld(&c.put),
                "multipartPart": ld(&c.multipart_part), "copy": ld(&c.copy), "delete": ld(&c.delete),
                "getBytes": ld(&c.get_bytes), "putBytes": ld(&c.put_bytes),
            },
            // compaction passes (lifecycle.rs): running now, of which long merges without a maintenance slot; ended without
            // progress; stopped on request; GC rounds that skipped a tenant busy with a pass
            "firstCourse": {
                "enabled": crate::opening::ENABLED.load(Relaxed),
                "openings": crate::opening::STATS.openings.load(Relaxed) as i64,
                "bytes": crate::opening::STATS.bytes.load(Relaxed) as i64,
                "pending": crate::opening::STATS.pending.load(Relaxed) as i64,
                "evictedPending": crate::opening::STATS.evicted_pending.load(Relaxed) as i64,
                "droppedPendingTooBig": crate::opening::STATS.dropped_pending_too_big.load(Relaxed) as i64,
                "firstQueries": crate::opening::STATS.first_queries.load(Relaxed) as i64,
                "servedQueries": crate::opening::STATS.served_queries.load(Relaxed) as i64,
                "partialQueries": crate::opening::STATS.partial_queries.load(Relaxed) as i64,
                "missedQueries": crate::opening::STATS.missed_queries.load(Relaxed) as i64,
                "noOpening": crate::opening::STATS.no_opening.load(Relaxed) as i64,
                "notDurable": crate::opening::STATS.not_durable.load(Relaxed) as i64,
                "servedReads": crate::opening::STATS.served_reads.load(Relaxed) as i64,
                "missedReads": crate::opening::STATS.missed_reads.load(Relaxed) as i64,
                "kept": crate::opening::STATS.kept.load(Relaxed) as i64,
                "unkeepable": crate::opening::STATS.unkeepable.load(Relaxed) as i64,
                "trimmed": crate::opening::STATS.trimmed.load(Relaxed) as i64,
                "droppedTooBig": crate::opening::STATS.dropped_too_big.load(Relaxed) as i64,
                "evicted": crate::opening::STATS.evicted.load(Relaxed) as i64,
                "expired": crate::opening::STATS.expired.load(Relaxed) as i64,
                "forgotten": crate::opening::STATS.forgotten.load(Relaxed) as i64,
                "writesApplied": crate::opening::STATS.writes_applied.load(Relaxed) as i64,
                "writeHookNs": crate::opening::STATS.write_hook_ns.load(Relaxed) as i64,
                "recordNs": crate::opening::STATS.record_ns.load(Relaxed) as i64,
            },
            "customerKeys": {
                "docsWritten": crate::custkey::DOCS_WRITTEN.load(Relaxed) as i64,
                "locatorLookups": crate::custkey::LOCATOR_LOOKUPS.load(Relaxed) as i64,
                // layout v2 (layout2.rs): seeks of range reads across customers
                "layoutSeeks": crate::layout2::SEEKS.load(Relaxed) as i64,
            },
            // envelope values (envelope.rs): rows read as live and as deleted envelopes; whether this process starts them
            "envelopes": {
                "liveRead": tenant_bench::envelope::READ_LIVE.load(Relaxed) as i64,
                "deadRead": tenant_bench::envelope::READ_DEAD.load(Relaxed) as i64,
                "start": tenant_bench::envelope::START.load(Relaxed),
            },
            // compaction 2, step 3 (fx): versions published and installed, failed builds, fence versions written at
            // open, rows answered by our runs, GETs of run blocks, runs opened
            "sealedRuns": {
                "enabled": tenant_bench::fx::ENABLED.load(Relaxed),
                "publishes": tenant_bench::fx::PUBLISHES.load(Relaxed) as i64,
                "publishErrors": tenant_bench::fx::PUBLISH_ERRORS.load(Relaxed) as i64,
                "fences": tenant_bench::fx::FENCES.load(Relaxed) as i64,
                "rowsFromRuns": tenant_bench::fx::view::ROWS_FROM_RUNS.load(Relaxed) as i64,
                "runGets": tenant_bench::fx::view::RUN_GETS.load(Relaxed) as i64,
                "runOpens": tenant_bench::fx::view::RUN_OPENS.load(Relaxed) as i64,
                "blockHits": tenant_bench::fx::view::BLOCK_HITS.load(Relaxed) as i64,
                // step 6 (fx/counter.rs): slices, the new-user filter, the staged publish
                "counter": {
                    "bytes": tenant_bench::fx::counter::stats().bytes as i64,
                    "slicesMade": tenant_bench::fx::counter::SLICES_MADE.load(Relaxed) as i64,
                    "sliceHits": tenant_bench::fx::counter::SLICE_HITS.load(Relaxed) as i64,
                    "wholeHits": tenant_bench::fx::counter::WHOLE_HITS.load(Relaxed) as i64,
                    "filterChecks": tenant_bench::fx::counter::FILTER_CHECKS.load(Relaxed) as i64,
                    "filterSkips": tenant_bench::fx::counter::FILTER_SKIPS.load(Relaxed) as i64,
                    "stages": tenant_bench::fx::counter::STAGES.load(Relaxed) as i64,
                    "stageWarmed": tenant_bench::fx::counter::STAGE_WARMED.load(Relaxed) as i64,
                    "stageMissed": tenant_bench::fx::counter::STAGE_MISSED.load(Relaxed) as i64,
                    "stageGets": tenant_bench::fx::counter::STAGE_GETS.load(Relaxed) as i64,
                    "stageTimeouts": tenant_bench::fx::counter::STAGE_TIMEOUTS.load(Relaxed) as i64,
                    "trims": tenant_bench::fx::counter::TRIMS.load(Relaxed) as i64,
                    "trimDrops": tenant_bench::fx::counter::TRIM_DROPS.load(Relaxed) as i64,
                    "refetches": tenant_bench::fx::counter::REFETCHES.load(Relaxed) as i64,
                },
            },
            // compaction 2, step 4 (`--sealed-compactor`): compactors started and running, their versions, absorptions,
            // tier merges, reseals and job errors
            "compactor": {
                "enabled": tenant_bench::fx::compactor::SERVER_ON.load(Relaxed),
                "started": tenant_bench::fx::compactor::G_STARTED.load(Relaxed) as i64,
                "running": tenant_bench::fx::compactor::active_count() as i64,
                // perf-latency round 3 (review D1): run uploads aborted by a writer dropped mid-upload, aborts that failed,
                // incomplete uploads aborted by an erase or a drop; round 3 diagnostics: writes held 1 s or more,
                // closed-tenant GC passes queued again, L0 SSTs a starting compactor knew from our manifest or read
                "runUploadsAbortedOnDrop": tenant_bench::runfmt::DROPPED_UPLOADS_ABORTED.load(Relaxed) as i64,
                "runUploadAbortErrors": tenant_bench::runfmt::DROPPED_UPLOAD_ABORT_ERRORS.load(Relaxed) as i64,
                "incompleteUploadsAborted": crate::uploads::ABORTED.load(Relaxed) as i64,
                "slowStages": tenant_bench::lifecycle::SLOW_STAGES.load(Relaxed) as i64,
                "closeGcRequeued": tenant_bench::lifecycle::CLOSE_GC_REQUEUED.load(Relaxed) as i64,
                "closePassesForPending": tenant_bench::lifecycle::CLOSE_PASSES_FOR_PENDING.load(Relaxed) as i64,
                "l0RecognisedFromVersion": tenant_bench::fx::compactor::L0_RECOGNISED_FROM_VERSION.load(Relaxed) as i64,
                "l0RecognisedByRead": tenant_bench::fx::compactor::L0_RECOGNISED_BY_READ.load(Relaxed) as i64,
                "recogniseJobs": tenant_bench::fx::compactor::RECOGNISE_JOBS.load(Relaxed) as i64,
                "recogniseMsMax": tenant_bench::fx::compactor::RECOGNISE_MS_MAX.load(Relaxed) as i64,
                "firstBuilds": tenant_bench::lifecycle::FIRST_BUILDS.load(Relaxed) as i64,
                "firstInstalls": tenant_bench::lifecycle::FIRST_INSTALLS.load(Relaxed) as i64,
                "firstInstallRefused": tenant_bench::lifecycle::FIRST_INSTALL_REFUSED.load(Relaxed) as i64,
                "firstBuildMsMax": tenant_bench::lifecycle::FIRST_BUILD_MS_MAX.load(Relaxed) as i64,
                "claimsDeferred": tenant_bench::lifecycle::CLAIMS_DEFERRED.load(Relaxed) as i64,
                "l0JobsBesideMerge": tenant_bench::fx::drop::L0_JOBS_BESIDE_MERGE.load(Relaxed) as i64,
                "mergeHeld": tenant_bench::fx::drop::TEST_MERGE_HELD.load(Relaxed) as i64,
                "passGateKeptL0": tenant_bench::lifecycle::PASS_GATE_KEPT.load(Relaxed) as i64,
                "publishes": tenant_bench::fx::compactor::G_PUBLISHES.load(Relaxed) as i64,
                "absorbs": tenant_bench::fx::compactor::G_ABSORBS.load(Relaxed) as i64,
                "merges": tenant_bench::fx::compactor::G_MERGES.load(Relaxed) as i64,
                "reseals": tenant_bench::fx::compactor::G_RESEALS.load(Relaxed) as i64,
                "errors": tenant_bench::fx::compactor::G_ERRORS.load(Relaxed) as i64,
            },
            // agent checkpoints as a thread message log (`fx/derive.rs`, `--sealed-derive`): documents encoded and kept
            // plain by reseals, message rows written, rebuilds by reads and by reseals, rebuild errors (a wrong reference
            // or a damaged row: an error, never a wrong document)
            "derived": {
                "write": tenant_bench::fx::derive::WRITE.load(Relaxed),
                "encoded": tenant_bench::fx::derive::ENCODED_DOCS.load(Relaxed) as i64,
                "plain": tenant_bench::fx::derive::PLAIN_DOCS.load(Relaxed) as i64,
                "messageRows": tenant_bench::fx::derive::MESSAGE_ROWS.load(Relaxed) as i64,
                "fallbacks": tenant_bench::fx::derive::FALLBACKS.load(Relaxed) as i64,
                "rebuilds": tenant_bench::fx::derive::REBUILDS.load(Relaxed) as i64,
                "resealRebuilds": tenant_bench::fx::derive::RESEAL_REBUILDS.load(Relaxed) as i64,
                "rebuildErrors": tenant_bench::fx::derive::REBUILD_ERRORS.load(Relaxed) as i64,
            },
            "compaction": {
                "passes": tenant_bench::lifecycle::MAINT.compact_runs.load(Relaxed) as i64,
                "running": tenant_bench::lifecycle::PASSES_RUNNING.load(Relaxed) as i64,
                "longMerges": tenant_bench::lifecycle::PASSES_LONG.load(Relaxed) as i64,
                "stalled": tenant_bench::lifecycle::PASSES_STALLED.load(Relaxed) as i64,
                "preempted": tenant_bench::lifecycle::PASSES_PREEMPTED.load(Relaxed) as i64,
                "gcSkippedBusy": tenant_bench::lifecycle::GC_SKIPPED_BUSY.load(Relaxed) as i64,
            },
            // reads charged to no command: opening and reopening databases, upgrades to a writer, profiler writes
            "backgroundReads": {
                "gets": tenant_bench::meter::BACKGROUND.read().0 as i64,
                "bytes": tenant_bench::meter::BACKGROUND.read().1 as i64,
                "micros": tenant_bench::meter::BACKGROUND.read().2 as i64,
            },
            "mapping": {
                "wiredTiger.cache.bytes currently in the cache": "bytes held by the shared block + metadata cache (RAM)",
                "wiredTiger.cache.maximum bytes configured": "--block-cache-mb + --meta-cache-mb",
                "wiredTiger.cache.bytes read into cache": "bytes of S3 GET bodies (whole and ranged)",
                "wiredTiger.cache.pages read into cache": "S3 GET requests (whole and ranged)",
                "wiredTiger.cache.bytes written from cache": "bytes of S3 PUT bodies",
                "wiredTiger.cache.pages written from cache": "S3 PUT requests (and multipart parts)",
                "wiredTiger.cache.pages requested from the cache": "lookups of the shared cache (hits + misses)",
                "extra_info.user_time_us / system_time_us": "CPU of this process (getrusage)",
            },
        });
        if let Some(Bson::Document(f)) = d.get_mut("fenvix") {
            f.insert("waiter", waiter_stats());
        }
    }

    fn host_info(&self) -> Document {
        let mut d = template(HOST_INFO);
        crate::hostinfo::fill(&mut d);
        if let Some(Bson::Document(s)) = d.get_mut("system") {
            s.insert("currentTime", Bson::DateTime(bson::DateTime::now()));
            s.insert("hostname", self.host_port());
        }
        d
    }

    fn cmd_line_opts(&self) -> Document {
        let argv: Vec<Bson> = std::env::args().map(Bson::String).collect();
        let args: Vec<String> = std::env::args().collect();
        let val = |k: &str| args.iter().position(|a| a == k).and_then(|i| args.get(i + 1)).cloned();
        let mut parsed = Document::new();
        if let Some(l) = val("--listen") {
            let (ip, port) = l.rsplit_once(':').unwrap_or(("127.0.0.1", "27017"));
            parsed.insert("net", doc! { "bindIp": ip, "port": port.parse::<i32>().unwrap_or(27017) });
        }
        if let Some(r) = val("--replica-set") {
            parsed.insert("replication", doc! { "replSet": r });
        }
        doc! { "argv": argv, "parsed": parsed, "ok": 1.0 }
    }

    fn conn_pool_stats(&self) -> Document {
        let mut d = zeroed(&template(CONN_POOL));
        d.insert("numClientConnections", self.diag.conns.lock().unwrap().len() as i32);
        d
    }

    fn list_commands(&self) -> Document {
        let t = template(LIST_COMMANDS);
        let mut cmds = Document::new();
        if let Ok(all) = t.get_document("commands") {
            for (k, v) in all {
                if IMPLEMENTED.contains(&k.as_str()) {
                    cmds.insert(k.clone(), v.clone());
                }
            }
        }
        doc! { "commands": cmds, "ok": 1.0 }
    }

    fn get_log(&self, db: &str, cmd: &Document) -> R<Document> {
        admin_only(db, "getLog")?;
        match cmd.get("getLog") {
            Some(Bson::String(s)) if s == "*" => Ok(doc! { "names": ["global", "startupWarnings"], "ok": 1.0 }),
            Some(Bson::String(s)) if s == "global" || s == "startupWarnings" => {
                let l = self.diag.log.lock().unwrap();
                let lines: Vec<Bson> = if s == "global" { l.1.iter().cloned().map(Bson::String).collect() } else { vec![] };
                Ok(doc! { "totalLinesWritten": if s == "global" { l.0 as i32 } else { 0 }, "log": lines, "ok": 1.0 })
            }
            Some(Bson::String(s)) => Err(MongoError::new(96, "OperationFailed", format!("No log named '{s}'"))),
            Some(v) => Err(bad_type(v, "getLog.getLog", "string")),
            None => Err(MongoError::new(2, "BadValue", "getLog".into())),
        }
    }

    // ---- currentOp / killOp / top ---------------------------------------------------------------

    pub(crate) fn current_op_docs(&self, own: Option<i64>, all: bool) -> Vec<Document> {
        let _ = own;
        let host = self.host_port();
        let ops = self.diag.ops.lock().unwrap();
        let conns = self.diag.conns.lock().unwrap();
        let mut out = Vec::new();
        for (id, o) in ops.iter() {
            let mut d = doc! { "type": "op", "host": host.clone(), "desc": format!("conn{}", o.conn), "connectionId": o.conn as i32 };
            if let Some(c) = conns.get(&o.conn) {
                d.insert("client", c.client.clone());
                if let Some(m) = &c.metadata {
                    d.insert("clientMetadata", m.clone());
                }
            }
            let run = o.start.elapsed();
            d.insert("active", true);
            d.insert("currentOpTime", now_string());
            d.insert("threaded", true);
            d.insert("opid", *id as i32);
            let mut command = crate::auth::redact_command(&o.name, &o.cmd);
            command.remove("$clusterTime");
            command.remove("$readPreference");
            command.remove("txnNumber");
            command.remove("writeConcern");
            command.remove("readConcern");
            if let Some(Bson::Document(l)) = command.get_mut("lsid") {
                l.remove("uid");
            }
            if let Some(Bson::Document(l)) = o.cmd.get("lsid") {
                let mut l = l.clone();
                if !l.contains_key("uid") {
                    l.insert("uid", Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: sha_empty() }));
                }
                d.insert("lsid", l);
            }
            d.insert("secs_running", Bson::Int64(run.as_secs() as i64));
            d.insert("microsecs_running", Bson::Int64(run.as_micros() as i64));
            let (op, ns) = op_kind(&o.name, &o.db, &o.cmd);
            d.insert("op", op);
            d.insert("ns", ns);
            d.insert("redacted", false);
            command.insert("$db", o.db.clone());
            d.insert("command", command);
            if matches!(o.name.as_str(), "find" | "aggregate" | "count" | "distinct" | "currentOp") {
                d.insert("queryFramework", "classic");
            }
            d.insert("numYields", 0);
            d.insert("locks", doc! {});
            d.insert("waitingForLock", false);
            d.insert("lockStats", doc! {});
            d.insert("waitingForFlowControl", false);
            d.insert("flowControlStats", doc! {});
            out.push(d);
        }
        if all {
            out.push(doc! { "type": "op", "host": host.clone(), "desc": "SessionKiller", "active": true, "currentOpTime": now_string() });
        }
        out
    }

    async fn current_op(&self, db: &str, cmd: &Document) -> R<Document> {
        admin_only(db, "currentOp")?;
        let all = bool_arg(cmd.get("$all"), false);
        let mut filter = Document::new();
        for (k, v) in cmd.iter().skip(1) {
            if !k.starts_with('$') && !matches!(k.as_str(), "lsid" | "txnNumber" | "comment" | "maxTimeMS" | "readConcern" | "writeConcern" | "apiVersion" | "apiStrict" | "apiDeprecationErrors") {
                filter.insert(k.clone(), v.clone());
            }
        }
        let mut inprog = Vec::new();
        for d in self.current_op_docs(None, all) {
            if crate::query::matches(&d, &filter)? {
                inprog.push(Bson::Document(d));
            }
        }
        Ok(doc! { "inprog": inprog, "ok": 1.0 })
    }

    fn kill_op(&self, db: &str, cmd: &Document) -> R<Document> {
        admin_only(db, "killOp")?;
        let Some(op) = cmd.get("op") else { return Err(MongoError::new(4, "NoSuchKey", "Missing expected field \"op\"".into())) };
        let id = match op {
            Bson::Int32(i) => *i as i64,
            Bson::Int64(i) => *i,
            Bson::Double(f) => *f as i64,
            _ => return Err(MongoError::new(14, "TypeMismatch", "Expected field \"op\" to have numeric type, but found string".replace("string", crate::update::type_name(op)))),
        };
        if let Some(o) = self.diag.ops.lock().unwrap().get(&id) {
            KILLED.lock().unwrap().push(id);
            o.kill.notify_one();
        }
        Ok(doc! { "info": "attempting to kill op", "ok": 1.0 })
    }

    fn top(&self, db: &str) -> R<Document> {
        admin_only(db, "top")?;
        let mut totals = doc! { "note": "all times in microseconds" };
        for (ns, t) in self.diag.top.lock().unwrap().iter() {
            let c = |c: &TopCell| Bson::Document(doc! { "time": c.time as i32, "count": c.count as i32 });
            totals.insert(ns.clone(), doc! { "total": c(&t.total), "readLock": c(&t.read), "writeLock": c(&t.write), "queries": c(&t.queries), "getmore": c(&t.getmore), "insert": c(&t.insert), "update": c(&t.update), "remove": c(&t.remove), "commands": c(&t.commands) });
        }
        Ok(doc! { "totals": totals, "ok": 1.0 })
    }

    // ---- read / write concern defaults, cluster parameters ---------------------------------------

    /// The write concern that `setDefaultRWConcern` set, if any.
    pub(crate) fn default_write_concern(&self) -> Option<Document> {
        self.diag.rwc.lock().unwrap().as_ref().and_then(|s| s.get_document("defaultWriteConcern").ok().cloned())
    }

    fn get_default_rwc(&self, _db: &str, cmd: &Document) -> R<Document> {
        if !self.is_rs() {
            return Err(MongoError::new(51300, "Location51300", "'getDefaultRWConcern' is not supported on standalone nodes.".into()));
        }
        let mut d = doc! { "defaultReadConcern": { "level": "local" }, "defaultWriteConcern": { "w": "majority", "wtimeout": 0 } };
        let stored = self.diag.rwc.lock().unwrap().clone();
        let mut wsrc = "implicit";
        let mut rsrc = "implicit";
        if let Some(s) = &stored {
            if let Some(v) = s.get("defaultWriteConcern") {
                d.insert("defaultWriteConcern", v.clone());
                wsrc = "global";
            }
            if let Some(v) = s.get("defaultReadConcern") {
                d.insert("defaultReadConcern", v.clone());
                rsrc = "global";
            }
            for k in ["updateOpTime", "updateWallClockTime"] {
                if let Some(v) = s.get(k) {
                    d.insert(k, v.clone());
                }
            }
        }
        d.insert("defaultWriteConcernSource", wsrc);
        d.insert("defaultReadConcernSource", rsrc);
        if bool_arg(cmd.get("inMemory"), false) {
            d.insert("inMemory", true);
        }
        d.insert("localUpdateWallClockTime", Bson::DateTime(bson::DateTime::now()));
        d.insert("ok", 1.0);
        Ok(d)
    }

    fn set_default_rwc(&self, db: &str, cmd: &Document) -> R<Document> {
        let _ = db;
        if !self.is_rs() {
            return Err(MongoError::new(51300, "Location51300", "'setDefaultRWConcern' is not supported on standalone nodes.".into()));
        }
        let wc = cmd.get("defaultWriteConcern");
        let rc = cmd.get("defaultReadConcern");
        if wc.is_none() && rc.is_none() {
            return Err(MongoError::new(2, "BadValue", "At least one of the \"defaultReadConcern\" or \"defaultWriteConcern\" fields must be present".into()));
        }
        let mut cur = self.diag.rwc.lock().unwrap().clone().unwrap_or_default();
        if let Some(w) = wc {
            let Bson::Document(w) = w else { return Err(bad_type(w, "setDefaultRWConcern.defaultWriteConcern", "object")) };
            if w.is_empty() {
                if cur.contains_key("defaultWriteConcern") {
                    return Err(MongoError::new(20, "IllegalOperation", "The global default write concern cannot be unset once it is set.".into()));
                }
            } else {
                match w.get("w") {
                    Some(Bson::Int32(0)) | Some(Bson::Int64(0)) => return Err(MongoError::new(2, "BadValue", "Unacknowledged write concern is not suitable for the default write concern".into())),
                    None => return Err(MongoError::new(2, "BadValue", "Default write concern must have 'w' field.".into())),
                    _ => {}
                }
                let mut w2 = w.clone();
                if !w2.contains_key("wtimeout") {
                    w2.insert("wtimeout", 0);
                }
                cur.insert("defaultWriteConcern", w2);
            }
        }
        if let Some(r) = rc {
            let Bson::Document(r) = r else { return Err(bad_type(r, "setDefaultRWConcern.defaultReadConcern", "object")) };
            if r.is_empty() {
                cur.remove("defaultReadConcern");
            } else {
                match r.get("level") {
                    Some(Bson::String(l)) if matches!(l.as_str(), "local" | "available" | "majority") => {}
                    Some(Bson::String(l)) => return Err(MongoError::new(2, "BadValue", format!("level: '{l}' is not suitable for the default read concern"))),
                    _ => {
                        let k = r.keys().next().cloned().unwrap_or_default();
                        return Err(MongoError::new(72, "InvalidOptions", format!("Unrecognized option in readConcern: {k}")));
                    }
                }
                cur.insert("defaultReadConcern", r.clone());
            }
        }
        let ts = bson::Timestamp { time: (bson::DateTime::now().timestamp_millis() / 1000) as u32, increment: 1 };
        cur.insert("updateOpTime", Bson::Timestamp(ts));
        cur.insert("updateWallClockTime", Bson::DateTime(bson::DateTime::now()));
        *self.diag.rwc.lock().unwrap() = Some(cur);
        self.get_default_rwc("admin", &doc! {})
    }

    fn get_cluster_parameter(&self, cmd: &Document) -> R<Document> {
        if !self.is_rs() {
            return Err(MongoError::new(20, "IllegalOperation", "getClusterParameter cannot be run on standalones".into()));
        }
        let names: Vec<String> = match cmd.get("getClusterParameter") {
            Some(Bson::String(s)) if s == "*" => vec!["changeStreamOptions".into()],
            Some(Bson::String(s)) => vec![s.clone()],
            Some(Bson::Array(a)) => a.iter().filter_map(|b| b.as_str().map(str::to_string)).collect(),
            Some(v) => return Err(bad_type(v, "getClusterParameter.getClusterParameter", "string or array")),
            None => vec![],
        };
        let mut out = Vec::new();
        for n in names {
            if n != "changeStreamOptions" {
                return Err(MongoError::new(4, "NoSuchKey", format!("Unknown server parameter: {n}")));
            }
            let mut p = doc! { "_id": "changeStreamOptions", "clusterParameterTime": Bson::Timestamp(bson::Timestamp { time: 0, increment: 0 }), "preAndPostImages": { "expireAfterSeconds": "off" } };
            if let Some(c) = self.diag.cluster.lock().unwrap().as_ref() {
                for (k, v) in c {
                    p.insert(k.clone(), v.clone());
                }
            }
            out.push(Bson::Document(p));
        }
        Ok(doc! { "clusterParameters": out, "ok": 1.0 })
    }

    fn set_cluster_parameter(&self, cmd: &Document) -> R<Document> {
        if !self.is_rs() {
            return Err(MongoError::new(20, "IllegalOperation", "setClusterParameter cannot be run on standalones".into()));
        }
        let Some(Bson::Document(p)) = cmd.get("setClusterParameter") else {
            return Err(MongoError::new(14, "TypeMismatch", "BSON field 'setClusterParameter' is the wrong type, expected type 'object'".into()));
        };
        let Some((name, val)) = p.iter().next() else { return Err(MongoError::new(2, "BadValue", "Cluster parameter name is missing".into())) };
        if name != "changeStreamOptions" {
            return Err(MongoError::new(4, "NoSuchKey", format!("Unknown server parameter: {name}")));
        }
        let Bson::Document(v) = val else { return Err(MongoError::new(2, "BadValue", "Cluster parameter value must be an object".into())) };
        let mut st = doc! { "clusterParameterTime": Bson::Timestamp(bson::Timestamp { time: (bson::DateTime::now().timestamp_millis() / 1000) as u32, increment: 1 }) };
        if let Some(Bson::Document(i)) = v.get("preAndPostImages") {
            let mut i = i.clone();
            if let Some(n) = i.get("expireAfterSeconds").and_then(|x| int_arg(Some(x))) {
                i.insert("expireAfterSeconds", Bson::Int64(n));
            }
            st.insert("preAndPostImages", i);
        }
        *self.diag.cluster.lock().unwrap() = Some(st);
        Ok(ok())
    }

    // ---- profiler --------------------------------------------------------------------------------

    fn profile(&self, db: &str, cmd: &Document) -> R<Document> {
        let wrong = |field: &str, v: &Bson| MongoError::new(14, "TypeMismatch", format!("BSON field 'profile.{field}' is the wrong type '{}', expected types '[long, int, decimal, double']", crate::update::type_name(v)));
        let level = match cmd.get("profile") {
            Some(Bson::Int32(i)) => *i as i64,
            Some(Bson::Int64(i)) => *i,
            Some(Bson::Double(f)) => *f as i64,
            Some(v) => return Err(wrong("profile", v)),
            None => -1,
        };
        let mut g = self.diag.prof_global.lock().unwrap();
        let mut p = self.diag.prof.lock().unwrap();
        let cur = p.entry(db.to_string()).or_default();
        let before = (cur.clone(), g.0, g.1);
        if let Some(v) = cmd.get("slowms") {
            g.0 = match v {
                Bson::Int32(i) => *i as i64,
                Bson::Int64(i) => *i,
                Bson::Double(f) => *f as i64,
                other => return Err(wrong("slowms", other)),
            };
        }
        if let Some(v) = cmd.get("sampleRate") {
            let f = match v {
                Bson::Double(f) => *f,
                Bson::Int32(i) => *i as f64,
                Bson::Int64(i) => *i as f64,
                other => return Err(wrong("sampleRate", other)),
            };
            if !(0.0..=1.0).contains(&f) {
                return Err(MongoError::new(2, "BadValue", "'sampleRate' must be between 0.0 and 1.0 inclusive".to_string()));
            }
            g.1 = f;
        }
        match cmd.get("filter") {
            Some(Bson::Document(f)) => {
                crate::query::probe_filter(f)?;
                cur.filter = Some(f.clone());
            }
            Some(Bson::String(s)) if s == "unset" => cur.filter = None,
            Some(_) => return Err(MongoError::new(2, "BadValue", "Expected an object, or the string 'unset'.".to_string())),
            None => {}
        }
        let mut reply = doc! { "was": before.0.level, "slowms": before.1 as i32, "sampleRate": before.2 };
        if let Some(f) = &before.0.filter {
            reply.insert("filter", f.clone());
        }
        if (0..=2).contains(&level) {
            cur.level = level as i32;
        }
        if before.0.filter.is_some() || cmd.contains_key("filter") {
            reply.insert("note", "When a filter expression is set, slowms and sampleRate are not used for profiling and slow-query log lines.");
        }
        reply.insert("ok", 1.0);
        Ok(reply)
    }

    /// After a command ran: its profile entry, when the database profiles (written by [`Server::profile_enqueue`]).
    /// `io` is what the command read from the object store (requests, bytes, microseconds: `storage.data`), `note` the keys
    /// and documents its reads examined and the plan of the first one (K1: they were 0 and COLLSCAN).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn profile_record(&self, db: &str, name: &str, cmd: &Document, r: &R<Document>, micros: i64, conn: i64, io: ((u64, u64, u64), bool), note: &crate::coll::ExecNote) -> Option<Document> {
        let (io, io_shared) = io;
        let p = self.diag.prof_of(db);
        if p.level == 0 || db == "admin" && name == "profile" || matches!(name, "profile" | "hello" | "isMaster" | "ismaster" | "ping" | "saslStart" | "saslContinue" | "endSessions") {
            return None;
        }
        let ns_target = cmd.get(name).and_then(|v| v.as_str()).unwrap_or("");
        if ns_target == "system.profile" || (name == "getMore" && cmd.get_str("collection").ok() == Some("system.profile")) {
            return None;
        }
        let millis = micros / 1000;
        let (slowms, sample) = *self.diag.prof_global.lock().unwrap();
        if p.level == 1 && p.filter.is_none() && millis < slowms {
            return None;
        }
        let (op, ns) = op_kind(name, db, cmd);
        let reply = r.as_ref().ok();
        let count = |k: &str| reply.and_then(|d| d.get(k)).and_then(|v| int_arg(Some(v))).unwrap_or(0);
        let batch_len = reply.and_then(|d| d.get_document("cursor").ok()).map(|c| c.get_array("firstBatch").or_else(|_| c.get_array("nextBatch")).map(|a| a.len() as i64).unwrap_or(0));
        let nreturned = batch_len.unwrap_or_else(|| count("n"));
        let mut command = crate::auth::redact_command(name, cmd);
        command.remove("$clusterTime");
        command.remove("$readPreference");
        command.insert("$db", db);
        // document sequences are not shown by the profiler
        for k in self.diag.seq.lock().unwrap().get(&conn).cloned().unwrap_or_default() {
            if k == "documents" {
                command.remove(&k);
            }
        }
        if matches!(name, "update" | "delete") {
            // the profiler shows the statement, not the command
            command = cmd.get_array(if name == "update" { "updates" } else { "deletes" }).ok().and_then(|a| a.first()).and_then(|b| b.as_document()).cloned().unwrap_or_default();
            if name == "update" {
                for k in ["multi", "upsert"] {
                    if !command.contains_key(k) {
                        command.insert(k, false);
                    }
                }
            }
        }
        let client = self.diag.conns.lock().unwrap().get(&conn).map(|c| c.client.clone()).unwrap_or_default();
        let stmt_filter = |d: &Document| d.get_document("q").cloned().unwrap_or_default();
        let filter = match name {
            "find" => cmd.get_document("filter").cloned().unwrap_or_default(),
            "update" | "delete" => cmd.get_array(if name == "update" { "updates" } else { "deletes" }).ok().and_then(|a| a.first()).and_then(|b| b.as_document()).map(stmt_filter).unwrap_or_default(),
            _ => Document::new(),
        };
        let plan_guess = if filter.len() == 1 && filter.get("_id").map(|v| !matches!(v, Bson::Document(_))).unwrap_or(false) { "IDHACK" } else { "COLLSCAN" };
        let plan_s: String = note.plan.clone().unwrap_or_else(|| plan_guess.to_string());
        let plan = plan_s.as_str();
        let (keys_x, docs_x) = (note.keys as i64, note.docs as i64);
        let stage_of = |p: &str| if p.starts_with("IXSCAN") { "FETCH" } else if p == "IDHACK" { "IDHACK" } else { "COLLSCAN" };
        let hash = |salt: &str| format!("{:08X}", md5_hex(format!("{salt}{}", crate::update::fmt_doc(&filter)).as_bytes())[..8].chars().fold(0_u32, |a, c| a.wrapping_mul(31).wrapping_add(c as u32)));
        let exhausted = reply.and_then(|d| d.get_document("cursor").ok()).map(|c| matches!(c.get("id"), Some(Bson::Int64(0)))).unwrap_or(true);
        let locks_r = || Bson::Document(doc! { "FeatureCompatibilityVersion": { "acquireCount": { "r": Bson::Int64(1) } }, "Global": { "acquireCount": { "r": Bson::Int64(1) } } });
        let locks_w = || Bson::Document(doc! { "ParallelBatchWriterMode": { "acquireCount": { "r": Bson::Int64(1) } }, "FeatureCompatibilityVersion": { "acquireCount": { "w": Bson::Int64(1) } }, "ReplicationStateTransition": { "acquireCount": { "w": Bson::Int64(1) } }, "Global": { "acquireCount": { "w": Bson::Int64(1) } }, "Database": { "acquireCount": { "w": Bson::Int64(1) } }, "Collection": { "acquireCount": { "w": Bson::Int64(1) } } });
        let exec = |stage: &str, n: i64| Bson::Document(doc! { "stage": stage, "nReturned": Bson::Int64(n), "executionTimeMillisEstimate": Bson::Int64(0), "works": Bson::Int64(n + 1), "advanced": Bson::Int64(n), "needTime": Bson::Int64(0), "needYield": Bson::Int64(0), "saveState": 0, "restoreState": 0, "isEOF": 1 });
        let resp_len = reply.map(|d| d.to_vec().map(|v| v.len()).unwrap_or(0)).unwrap_or(0) as i32;
        let mut e = doc! { "op": op, "ns": ns, "command": command };
        match (op, name) {
            ("query", _) => e.extend(doc! { "keysExamined": keys_x, "docsExamined": docs_x, "nBatches": 1, "cursorExhausted": exhausted, "numYield": 0, "nreturned": nreturned as i32, "queryHash": hash("q"), "planCacheKey": hash("p"), "queryFramework": "classic", "locks": locks_r(), "flowControl": {}, "responseLength": resp_len, "protocol": "op_msg", "millis": millis as i32, "planSummary": plan, "planningTimeMicros": Bson::Int64(0), "execStats": exec(stage_of(plan), nreturned) }),
            ("insert", _) => e.extend(doc! { "ninserted": count("n") as i32, "keysInserted": Bson::Int64(count("n") * 2), "numYield": 0, "locks": locks_w(), "flowControl": { "acquireCount": Bson::Int64(1) }, "responseLength": resp_len, "protocol": "op_msg", "millis": millis as i32 }),
            ("update", _) => e.extend(doc! { "keysExamined": keys_x, "docsExamined": docs_x, "nMatched": count("n") as i32, "nModified": count("nModified") as i32, "nUpserted": reply.and_then(|d| d.get_array("upserted").ok()).map(|a| a.len()).unwrap_or(0) as i32, "keysInserted": 0, "keysDeleted": 0, "numYield": 0, "locks": locks_w(), "flowControl": { "acquireCount": Bson::Int64(1) }, "millis": millis as i32, "planSummary": plan, "planningTimeMicros": Bson::Int64(0), "execStats": exec("UPDATE", 0) }),
            ("remove", _) => e.extend(doc! { "keysExamined": keys_x, "docsExamined": docs_x, "ndeleted": count("n") as i32, "keysDeleted": Bson::Int64(count("n") * 2), "numYield": 0, "locks": locks_w(), "flowControl": { "acquireCount": Bson::Int64(1) }, "millis": millis as i32, "planSummary": plan, "planningTimeMicros": Bson::Int64(0), "execStats": exec("DELETE", 0) }),
            _ if batch_len.is_some() || matches!(name, "count" | "distinct") => e.extend(doc! { "keysExamined": keys_x, "docsExamined": docs_x, "cursorExhausted": exhausted, "numYield": 0, "nreturned": nreturned as i32, "queryHash": hash("q"), "planCacheKey": hash("p"), "queryFramework": "classic", "locks": locks_r(), "flowControl": {}, "responseLength": resp_len, "protocol": "op_msg", "millis": millis as i32, "planSummary": plan, "planningTimeMicros": Bson::Int64(0) }),
            _ => e.extend(doc! { "numYield": 0, "locks": locks_w(), "flowControl": { "acquireCount": Bson::Int64(1) }, "responseLength": resp_len, "protocol": "op_msg", "millis": millis as i32 }),
        }
        // what the operation read from the object store (S3): mongod's bytes and time read from disk into its cache.
        // (No `cpuNanos`: it was the elapsed time written as CPU time; the CPU of the process is in serverStatus.)
        e.insert("storage", doc! { "data": { "bytesRead": Bson::Int64(io.1 as i64), "timeReadingMicros": Bson::Int64(io.2 as i64) } });
        e.insert("fenvixS3Gets", Bson::Int64(io.0 as i64));
        if io_shared {
            // some reads were split with other commands running on the same database at the same time (meter.rs)
            e.insert("fenvixS3Shared", true);
        }
        e.insert("ts", Bson::DateTime(bson::DateTime::now()));
        e.insert("client", client);
        e.insert("allUsers", Vec::<Bson>::new());
        e.insert("user", "");
        if let Err(err) = r {
            e.insert("ok", 0);
            e.insert("errMsg", err.msg.clone());
            e.insert("errName", err.name.to_string());
            e.insert("errCode", err.code);
        }
        if let Some(f) = &p.filter {
            let mut probe = e.clone();
            probe.insert("millis", millis as i32);
            if !crate::query::matches(&probe, f).unwrap_or(false) {
                return None;
            }
        } else if sample < 1.0 && rand::random::<f64>() >= sample {
            return None;
        }
        Some(e)
    }

    /// Queue a profile entry; a task of its own writes the queue of the database (in order, in batches), after the
    /// replies (K1: the write was part of every profiled operation, an S3 write before the reply).
    pub(crate) fn profile_enqueue(self: &Arc<Self>, db: &str, entry: Document) {
        let start = {
            let mut q = self.diag.prof_queue.lock().unwrap();
            q.0.entry(db.to_string()).or_default().push(entry);
            q.1.insert(db.to_string())
        };
        if start {
            let me = Arc::clone(self);
            let db = db.to_string();
            tokio::spawn(async move { me.profile_drain(&db).await });
        }
    }

    async fn profile_drain(&self, db: &str) {
        loop {
            let batch = {
                let mut q = self.diag.prof_queue.lock().unwrap();
                match q.0.remove(db) {
                    Some(v) if !v.is_empty() => v,
                    _ => {
                        q.1.remove(db);
                        return;
                    }
                }
            };
            // background work of the database: its reads (the writer's open, the collection list) go to no command (meter.rs)
            if let Err(e) = tenant_bench::meter::background(db, self.write_profile(db, batch)).await {
                eprintln!("profiler: entries of {db} not written: {}", e.msg);
            }
        }
    }

    /// Wait until the profile entries queued for `db` are written (a reader of `system.profile` sees them all).
    pub(crate) async fn profile_flush(&self, db: &str) {
        let t0 = Instant::now();
        while self.diag.prof_queue.lock().unwrap().1.contains(db) && t0.elapsed() < std::time::Duration::from_secs(30) {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }

    async fn write_profile(&self, db: &str, entries: Vec<Document>) -> R<()> {
        let names = self.dbs.lock(db).await.collections().await?;
        if !names.iter().any(|n| n == "system.profile") {
            self.create(db, &doc! { "create": "system.profile", "capped": true, "size": 1048576 }).await?;
        }
        let mut none = None;
        let docs: Vec<Bson> = entries.into_iter().map(Bson::Document).collect();
        self.insert(db, &doc! { "insert": "system.profile", "documents": docs }, &mut none).await?;
        Ok(())
    }

    // ---- dbHash / dataSize / validateDBMetadata -----------------------------------------------------

    async fn db_hash(&self, db: &str, cmd: &Document) -> R<Document> {
        let started = Instant::now();
        let want: Option<Vec<String>> = match cmd.get("collections") {
            None => None,
            Some(Bson::Array(a)) => Some(a.iter().filter_map(|b| b.as_str().map(str::to_string)).collect()),
            Some(_) => None,
        };
        let mut g = self.dbs.lock(db).await;
        let mut names = g.collections().await?;
        names.sort();
        let mut colls = Document::new();
        let mut uuids = Document::new();
        let mut capped = Vec::new();
        let mut all = String::new();
        for n in names {
            if let Some(w) = &want {
                if !w.contains(&n) {
                    continue;
                }
            }
            if n == "system.profile" {
                continue;
            }
            let cap = g.coll_opts(&n).await?.map(|o| o.cap.is_some()).unwrap_or(false);
            let mut buf: Vec<u8> = Vec::new();
            if cap {
                // a capped collection is hashed in natural order
                let np = crate::natural::prefix(&n);
                let end = crate::keyenc::prefix_end(&np);
                for (_, id_enc) in g.scan_range(&np, end.as_deref(), false, None, 1 << 20).await? {
                    let mut k = crate::layout::doc_prefix(&n);
                    k.extend(id_enc.iter());
                    if let Some(v) = g.get_live(&k).await? {
                        buf.extend(crate::natural::bson_part(&v));
                    }
                }
                capped.push(Bson::String(n.clone()));
            } else {
                for (_, v) in g.scan(&crate::layout::doc_prefix(&n)).await? {
                    buf.extend(crate::natural::bson_part(&v));
                }
            }
            let h = md5_hex(&buf);
            all.push_str(&h);
            colls.insert(n.clone(), h);
            uuids.insert(n.clone(), Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: g.collection_uuid(&n).await? }));
        }
        Ok(doc! { "host": self.host_port(), "collections": colls, "capped": capped, "uuids": uuids, "md5": md5_hex(all.as_bytes()), "timeMillis": started.elapsed().as_millis() as i32, "ok": 1.0 })
    }

    async fn data_size(&self, cmd: &Document) -> R<Document> {
        let started = Instant::now();
        let ns = match cmd.get("dataSize") {
            Some(Bson::String(s)) => s.clone(),
            Some(v) => return Err(bad_type(v, "dataSize", "string")),
            None => return Err(MongoError::new(2, "BadValue", "no ns".into())),
        };
        let Some((db, coll)) = ns.split_once('.') else {
            return Err(MongoError::new(73, "InvalidNamespace", format!("Namespace {ns} is not a valid collection name")));
        };
        let estimate = bool_arg(cmd.get("estimate"), false);
        let (min, max) = (cmd.get("min"), cmd.get("max"));
        if min.is_some() && max.is_none() {
            return Err(MongoError::new(2, "BadValue", "Max must be set if min is set.".into()));
        }
        let g = self.dbs.read(db).await?;
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Ok(doc! { "size": Bson::Int64(0), "numObjects": Bson::Int64(0), "millis": started.elapsed().as_millis() as i32, "ok": 1.0 });
        }
        let mut range: Option<(Vec<String>, Document, Document)> = None;
        if let (Some(Bson::Document(lo)), Some(Bson::Document(hi))) = (min, max) {
            let kp: Document = match cmd.get("keyPattern") {
                Some(Bson::Document(k)) => k.clone(),
                _ => lo.keys().map(|k| (k.clone(), Bson::Int32(1))).collect(),
            };
            let indexes = g.indexes(coll).await?;
            let fields: Vec<String> = kp.keys().cloned().collect();
            let has = fields == ["_id"] || indexes.iter().any(|i| i.spec.get_document("key").map(|k| k.keys().cloned().collect::<Vec<_>>() == fields).unwrap_or(false));
            if !has {
                return Err(MongoError::new(96, "OperationFailed", "Couldn't find valid index containing key pattern".into()));
            }
            range = Some((fields, lo.clone(), hi.clone()));
        }
        let (mut size, mut n) = (0_i64, 0_i64);
        for (_, v) in g.scan(&crate::layout::doc_prefix(coll)).await? {
            let raw = crate::natural::bson_part(&v);
            if let Some((fields, lo, hi)) = &range {
                let d = Document::from_reader(raw)?;
                let key = |f: &str| crate::query::path_values(&d, f).into_iter().next().cloned().unwrap_or(Bson::Null);
                let bound = |b: &Document| -> Vec<Bson> { fields.iter().map(|f| b.get(f).cloned().unwrap_or(Bson::Null)).collect() };
                let k: Vec<Bson> = fields.iter().map(|f| key(f)).collect();
                let cmp = |a: &[Bson], b: &[Bson]| {
                    for (x, y) in a.iter().zip(b) {
                        let o = crate::query::compare(x, y);
                        if o != std::cmp::Ordering::Equal {
                            return o;
                        }
                    }
                    std::cmp::Ordering::Equal
                };
                if cmp(&k, &bound(lo)) == std::cmp::Ordering::Less || cmp(&k, &bound(hi)) != std::cmp::Ordering::Less {
                    continue;
                }
            }
            size += raw.len() as i64;
            n += 1;
        }
        Ok(doc! { "size": Bson::Int64(size), "numObjects": Bson::Int64(n), "millis": started.elapsed().as_millis() as i32, "estimate": estimate, "ok": 1.0 })
    }

    async fn validate_db_metadata(&self, cmd: &Document) -> R<Document> {
        let Some(Bson::Document(api)) = cmd.get("apiParameters") else {
            return Err(MongoError::new(40414, "IDLFailedToParse", "BSON field 'validateDBMetadata.apiParameters' is missing but a required field".into()));
        };
        if !api.contains_key("version") {
            return Err(MongoError::new(40414, "IDLFailedToParse", "BSON field 'validateDBMetadata.apiParameters.version' is missing but a required field".into()));
        }
        let strict = match api.get("strict") {
            None => false,
            Some(Bson::Boolean(b)) => *b,
            Some(v) => return Err(MongoError::new(14, "TypeMismatch", format!("Field 'strict' should be a boolean value, but found: {}", crate::update::type_name(v)))),
        };
        let only_db = cmd.get_str("db").ok().map(str::to_string);
        let only_coll = cmd.get_str("collection").ok().map(str::to_string);
        let dbs: Vec<String> = match &only_db {
            Some(d) => vec![d.clone()],
            None => self.dbs.list_databases().await?,
        };
        let mut errs = Vec::new();
        if strict {
            for d in dbs {
                let g = self.dbs.read(&d).await?;
                let mut names = g.collections().await?;
                names.sort();
                for c in names {
                    if only_coll.as_deref().map(|o| o != c).unwrap_or(false) {
                        continue;
                    }
                    for i in g.indexes(&c).await? {
                        let disallowed = i.spec.get_bool("sparse").unwrap_or(false) || i.spec.get_document("key").map(|k| k.values().any(|v| v.as_str() == Some("text"))).unwrap_or(false);
                        if disallowed {
                            errs.push(Bson::Document(doc! { "ns": format!("{d}.{c}"), "code": 323, "codeName": "APIStrictError", "errmsg": format!("The index with name {} is not allowed in API version 1.", i.name) }));
                        }
                    }
                }
            }
        }
        Ok(doc! { "apiVersionErrors": errs, "ok": 1.0 })
    }

    // ---- plan cache, reIndex, compact, filemd5, setIndexCommitQuorum --------------------------------------

    async fn plan_cache(&self, db: &str, name: &str, cmd: &Document) -> R<Document> {
        let coll = match cmd.get(name) {
            Some(Bson::String(s)) if !s.is_empty() => s.as_str(),
            Some(v) => return Err(MongoError::new(73, "InvalidNamespace", format!("collection name has invalid type {}", crate::update::type_name(v)))),
            None => return Err(MongoError::new(73, "InvalidNamespace", "no collection".into())),
        };
        let key = format!("{db}.{coll}");
        let exists = self.dbs.lock(db).await.collections().await?.iter().any(|n| n == coll);
        match name {
            "planCacheListFilters" => {
                let f = self.diag.filters.lock().unwrap().get(&key).cloned().unwrap_or_default();
                Ok(doc! { "filters": f.into_iter().map(Bson::Document).collect::<Vec<_>>(), "ok": 1.0 })
            }
            "planCacheSetFilter" => {
                let Some(Bson::Document(q)) = cmd.get("query") else { return Err(MongoError::new(2, "BadValue", "required field query missing".into())) };
                let Some(Bson::Array(ix)) = cmd.get("indexes") else { return Err(MongoError::new(2, "BadValue", "required field indexes missing".into())) };
                if ix.is_empty() {
                    return Err(MongoError::new(2, "BadValue", "required field indexes must contain at least one index".into()));
                }
                if !exists {
                    return Err(MongoError::new(2, "BadValue", "no such collection".into()));
                }
                let entry = doc! { "query": q.clone(), "sort": cmd.get_document("sort").cloned().unwrap_or_default(), "projection": cmd.get_document("projection").cloned().unwrap_or_default(), "indexes": ix.clone() };
                let mut m = self.diag.filters.lock().unwrap();
                let l = m.entry(key).or_default();
                l.retain(|e| e.get("query") != entry.get("query") || e.get("sort") != entry.get("sort") || e.get("projection") != entry.get("projection"));
                l.push(entry);
                Ok(ok())
            }
            "planCacheClearFilters" => {
                let mut m = self.diag.filters.lock().unwrap();
                match cmd.get("query") {
                    Some(Bson::Document(q)) => {
                        if let Some(l) = m.get_mut(&key) {
                            l.retain(|e| e.get("query") != Some(&Bson::Document(q.clone())));
                        }
                    }
                    _ => {
                        m.remove(&key);
                    }
                }
                Ok(ok())
            }
            _ => Ok(ok()),
        }
    }

    async fn re_index(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "reIndex")?;
        if self.is_rs() {
            return Err(MongoError::new(20, "IllegalOperation", format!("reIndex is only allowed on a standalone mongod instance. Cannot reIndex '{db}.{coll}' while replication is active")));
        }
        let mut g = self.dbs.lock(db).await;
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Err(MongoError::namespace_not_found("collection does not exist".into()));
        }
        let indexes = self.load_indexes(&mut g, db, coll).await?;
        let mut specs = vec![Bson::Document(doc! { "v": 2, "key": { "_id": 1 }, "name": "_id_" })];
        for i in indexes.iter().filter(|i| i.state == crate::index::State::Ready) {
            specs.push(Bson::Document(i.spec.clone()));
        }
        let n = specs.len() as i32;
        Ok(doc! { "nIndexesWas": n, "nIndexes": n, "indexes": specs, "ok": 1.0 })
    }

    async fn compact(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "compact")?;
        if self.is_rs() && !bool_arg(cmd.get("force"), false) {
            return Err(MongoError::new(20, "IllegalOperation", "will not run compact on an active replica set primary as this will slow down other running operations. use force:true to force".into()));
        }
        if !self.dbs.lock(db).await.collections().await?.iter().any(|n| n == coll) {
            return Err(MongoError::namespace_not_found("collection does not exist".into()));
        }
        Ok(doc! { "bytesFreed": 0, "ok": 1.0 })
    }

    async fn filemd5(&self, db: &str, cmd: &Document) -> R<Document> {
        let root = cmd.get_str("root").unwrap_or("fs");
        let chunks = format!("{root}.chunks");
        let Some(files_id) = cmd.get("filemd5") else { return Err(MongoError::new(2, "BadValue", "filemd5 requires an _id".into())) };
        let mut g = self.dbs.lock(db).await;
        let mut n = 0;
        let mut buf: Vec<u8> = Vec::new();
        if g.collections().await?.iter().any(|c| *c == chunks) {
            let mut docs = Vec::new();
            for (_, v) in g.scan(&crate::layout::doc_prefix(&chunks)).await? {
                let d = Document::from_reader(crate::natural::bson_part(&v))?;
                if d.get("files_id") == Some(files_id) {
                    docs.push(d);
                }
            }
            docs.sort_by_key(|d| d.get("n").and_then(|v| int_arg(Some(v))).unwrap_or(0));
            for d in docs {
                if let Some(Bson::Binary(b)) = d.get("data") {
                    buf.extend(&b.bytes);
                }
                n += 1;
            }
        }
        Ok(doc! { "numChunks": n, "md5": md5_hex(&buf), "ok": 1.0 })
    }

    async fn set_index_commit_quorum(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "setIndexCommitQuorum")?;
        let names: Vec<String> = cmd.get_array("indexNames").map(|a| a.iter().filter_map(|b| b.as_str().map(str::to_string)).collect()).unwrap_or_default();
        let _ = db;
        let _ = names;
        Err(MongoError::new(27, "IndexNotFound", format!("Cannot find an index build on collection '{db}.{coll}' with the provided index names")))
    }

    fn fsync(&self, db: &str, cmd: &Document) -> R<Document> {
        admin_only(db, "fsync")?;
        if cmd.get("lock").map(|v| !matches!(v, Bson::Boolean(false) | Bson::Null | Bson::Undefined | Bson::Int32(0) | Bson::Int64(0)) && !matches!(v, Bson::Double(f) if *f == 0.0)).unwrap_or(false) {
            let n = self.diag.fsync_locks.fetch_add(1, Relaxed) + 1;
            return Ok(doc! { "info": "now locked against writes, use db.fsyncUnlock() to unlock", "lockCount": Bson::Int64(n), "seeAlso": "http://dochub.mongodb.org/core/fsynccommand", "ok": 1.0 });
        }
        Ok(doc! { "numFiles": 1, "ok": 1.0 })
    }

    fn fsync_unlock(&self, db: &str) -> R<Document> {
        admin_only(db, "fsyncUnlock")?;
        let cur = self.diag.fsync_locks.load(Relaxed);
        if cur <= 0 {
            return Err(MongoError::new(20, "IllegalOperation", "fsyncUnlock called when not locked".into()));
        }
        let n = self.diag.fsync_locks.fetch_sub(1, Relaxed) - 1;
        Ok(doc! { "info": "fsyncUnlock completed", "lockCount": Bson::Int64(n), "ok": 1.0 })
    }
}

fn sha_empty() -> Vec<u8> {
    use sha2::{Digest, Sha256};
    Sha256::digest(b"").to_vec()
}

/// A command that reads `system.profile` (it waits for the queued profile entries first).
pub(crate) fn reads_profile(name: &str, cmd: &Document) -> bool {
    matches!(name, "find" | "aggregate" | "count" | "distinct") && cmd.get_str(name).ok() == Some("system.profile")
}

fn op_kind(name: &str, db: &str, cmd: &Document) -> (&'static str, String) {
    let coll = cmd.get(name).and_then(|v| v.as_str()).map(str::to_string);
    let ns_cmd = format!("{db}.$cmd");
    match name {
        "insert" => ("insert", format!("{db}.{}", coll.unwrap_or_default())),
        "update" => ("update", format!("{db}.{}", coll.unwrap_or_default())),
        "delete" => ("remove", format!("{db}.{}", coll.unwrap_or_default())),
        "find" => ("query", format!("{db}.{}", coll.unwrap_or_default())),
        "getMore" => ("getmore", format!("{db}.{}", cmd.get_str("collection").unwrap_or_default())),
        "currentOp" => ("command", "admin.$cmd.aggregate".to_string()),
        "aggregate" if !matches!(cmd.get("aggregate"), Some(Bson::String(_))) => ("command", format!("{db}.$cmd.aggregate")),
        _ => match coll {
            Some(c) => ("command", format!("{db}.{c}")),
            None => ("command", ns_cmd),
        },
    }
}

fn start_session() -> Document {
    let id: [u8; 16] = rand::random();
    let mut b = id.to_vec();
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    doc! { "id": { "id": Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: b }) }, "timeoutMinutes": 30, "ok": 1.0 }
}

fn kill_all_sessions(name: &str, cmd: &Document) -> R<Document> {
    match cmd.get(name) {
        Some(Bson::Array(_)) => Ok(ok()),
        Some(v) => Err(MongoError::new(14, "TypeMismatch", format!("BSON field 'KillAllSessionsCmd.{name}' is the wrong type '{}', expected type 'array'", crate::update::type_name(v)).replace("KillAllSessionsCmd", if name == "killAllSessions" { "KillAllSessionsCmd" } else { "KillAllSessionsByPatternCmd" }))),
        None => Ok(ok()),
    }
}

/// The commands this server runs (what `listCommands` lists).
pub const IMPLEMENTED: &[&str] = &[
    "abortTransaction", "aggregate", "authenticate", "buildInfo", "buildinfo", "cloneCollectionAsCapped", "collMod", "collStats", "commitTransaction", "compact", "connPoolStats", "connectionStatus", "convertToCapped", "count", "create", "createIndexes", "createRole", "createUser",
    "currentOp", "dataSize", "dbHash", "dbStats", "delete", "distinct", "drop", "dropAllRolesFromDatabase", "dropAllUsersFromDatabase", "dropDatabase", "dropIndexes", "dropRole", "dropUser", "endSessions", "explain", "features", "filemd5", "find", "findAndModify", "findandmodify", "fsync",
    "fsyncUnlock", "getCmdLineOpts", "getDefaultRWConcern", "getClusterParameter", "getLog", "getMore", "getParameter", "grantPrivilegesToRole", "grantRolesToRole", "grantRolesToUser", "hello", "hostInfo", "insert", "invalidateUserCache", "isMaster", "killAllSessions",
    "killAllSessionsByPattern", "killCursors", "killOp", "killSessions", "listCollections", "listCommands", "listDatabases", "listIndexes", "lockInfo", "logRotate", "logout", "mapReduce", "ping", "planCacheClear", "planCacheClearFilters", "planCacheListFilters", "planCacheSetFilter",
    "profile", "reIndex", "refreshSessions", "renameCollection", "revokePrivilegesFromRole", "revokeRolesFromRole", "revokeRolesFromUser", "rolesInfo", "saslContinue", "saslStart", "serverStatus", "setClusterParameter", "setDefaultRWConcern", "setIndexCommitQuorum", "setParameter",
    "startSession", "top", "update", "updateRole", "updateUser", "usersInfo", "validate", "validateDBMetadata", "whatsmyuri",
];

/// T1, the waiter's tools: trips to the object store, the two-floor counter, the maps, the read-ahead.
pub fn waiter_stats() -> Document {
    use std::sync::atomic::Ordering::Relaxed;
    use tenant_bench::simstore as ss;
    let ld = |a: &std::sync::atomic::AtomicU64| a.load(Relaxed) as i64;
    let mut w = doc! {
        "trips": ld(&ss::TRIPS.trips), "reads": ld(&ss::TRIPS.reads),
        "chains": ss::CHAIN.lock().unwrap().chains as i64, "lastChainDepth": ss::CHAIN.lock().unwrap().last_chain_depth as i64, "readsInFlight": ld(&ss::TRIPS.inflight), "maxReadsInFlight": ld(&ss::TRIPS.max_inflight),
        "splitGets": ld(&ss::SPLIT_GETS), "splitPieces": ld(&ss::SPLIT_PIECES), "hedges": ld(&ss::HEDGES), "hedgeWins": ld(&ss::HEDGE_WINS), "nextBlockHits": ld(&ss::NEXT_BLOCK_HITS),
        "readAhead": {
            "first": ld(&crate::prefetch::FIRST), "next": ld(&crate::prefetch::NEXT), "docs": ld(&crate::prefetch::DOCS),
            "fails": ld(&crate::prefetch::FAILS), "busy": ld(&crate::prefetch::BUSY),
        },
    };
    if let Some(c) = tenant_bench::counter::COUNTER.get() {
        let (mu, mn, du, dn, dp) = c.ram_usage();
        let (disk_used, disk_maps, disk_n) = c.disk_usage();
        let (ssts, sst_bytes) = tenant_bench::maps::mapped_totals();
        let m = &c.m;
        w.insert("counter", doc! {
            "ramCapacity": c.ram_capacity() as i64, "diskCapacity": c.disk_capacity() as i64,
            "mapsRamBytes": mu as i64, "mapsRamBlockBytes": c.maps_block_bytes() as i64, "mapsRamEntries": mn as i64, "mapsDiskBytes": disk_maps as i64,
            "dataRamBytes": du as i64, "dataRamBlocks": dn as i64, "dataProtectedBytes": dp as i64,
            "diskBytes": disk_used as i64, "diskEntries": disk_n as i64,
            "hitsRam": ld(&m.hits_ram), "hitsDisk": ld(&m.hits_disk), "misses": ld(&m.misses),
            "mapsHits": ld(&m.maps_hits), "mapsMisses": ld(&m.maps_misses),
            "loadedBytes": ld(&m.loaded_bytes), "insertedBytes": ld(&m.inserted_bytes),
            "prefetchedIn": ld(&m.prefetched_in), "prefetchedUsed": ld(&m.prefetched_used), "prefetchedEvictedUnused": ld(&m.prefetched_evicted_unused),
            "promoted": ld(&m.promoted), "toDisk": ld(&m.to_disk), "diskDropped": ld(&m.disk_dropped),
            "mapsToDisk": ld(&m.maps_to_disk), "mapsDropped": ld(&m.maps_dropped), "mapsForgotten": ld(&m.maps_forgotten),
        });
        w.insert("maps", doc! {
            "sstsMapped": ssts as i64, "sstBytesMapped": sst_bytes as i64,
            "sstsWarmed": ld(&tenant_bench::maps::MAPS_SSTS), "mapBlocks": ld(&tenant_bench::maps::MAPS_BLOCKS),
            "runs": ld(&tenant_bench::maps::MAPS_RUNS), "fails": ld(&tenant_bench::maps::MAPS_FAILS),
            "busy": ld(&tenant_bench::maps::MAPS_BUSY), "lastMs": ld(&tenant_bench::maps::MAPS_LAST_MS),
        });
    }
    w
}
