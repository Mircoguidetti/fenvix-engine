//! Command dispatch: handshake commands, CRUD, cursors, collection and index management.

use crate::agg::run_pipeline;
use crate::dbs::{valid_db_name, Dbs, Entry};
use crate::error::MongoError;
use crate::layout::{doc_prefix, encode_sortable, index_key};
use crate::query::{self, id_equality, matches, parse_projection, parse_sort, path_values, project, sort_docs};
use crate::update::{apply_update, fmt_doc, upsert_document};
use bson::{doc, oid::ObjectId, Bson, DateTime, Document};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicI64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use tenant_bench::lifecycle::Op;

pub(crate) type R<T> = Result<T, MongoError>;

const MAX_WIRE_VERSION: i32 = 21;
const DEFAULT_FIRST_BATCH: usize = 101;

pub struct Server {
    pub dbs: Arc<Dbs>,
    pub(crate) builds: Arc<Mutex<HashSet<String>>>,
    pub(crate) sessions: crate::txn::Sessions,
    pub(crate) next_txn_id: std::sync::atomic::AtomicU64,
    /// Replica-set name and advertised address reported by `hello` (drivers allow transactions only on replica sets).
    pub(crate) rs: Mutex<Option<(String, String)>>,
    /// Print every command name and database (used to discover what drivers send on connect).
    pub log_commands: bool,
    cursors: Mutex<HashMap<i64, Cursor>>,
    pub(crate) change_cursors: Mutex<HashMap<i64, crate::cmd_changes::ChangeCursor>>,
    next_conn: AtomicI64,
}

struct Cursor {
    ns: String,
    docs: VecDeque<Document>,
}

pub(crate) fn ok() -> Document {
    doc! { "ok": 1.0 }
}

pub(crate) fn int_arg(v: Option<&Bson>) -> Option<i64> {
    match v? {
        Bson::Int32(i) => Some(*i as i64),
        Bson::Int64(i) => Some(*i),
        Bson::Double(f) if f.fract() == 0.0 => Some(*f as i64),
        _ => None,
    }
}

pub(crate) fn bool_arg(v: Option<&Bson>, default: bool) -> bool {
    match v {
        Some(Bson::Boolean(b)) => *b,
        Some(Bson::Int32(i)) => *i != 0,
        Some(Bson::Int64(i)) => *i != 0,
        Some(Bson::Double(f)) => *f != 0.0,
        _ => default,
    }
}

pub(crate) fn collection_name<'a>(cmd: &'a Document, key: &str) -> R<&'a str> {
    match cmd.get(key) {
        Some(Bson::String(s)) if !s.is_empty() && !s.contains('\0') && !s.contains('$') => Ok(s),
        Some(Bson::String(s)) => Err(MongoError::new(73, "InvalidNamespace", format!("Invalid collection name specified '{s}'"))),
        _ => Err(MongoError::new(2, "BadValue", format!("collection name has invalid type {}", cmd.get(key).map(crate::update::type_name).unwrap_or("missing")))),
    }
}

/// Random UUID-shaped string for messages that embed a build/collection id.
pub(crate) fn fake_uuid() -> String {
    let h: u128 = rand::random();
    let s = format!("{h:032x}");
    format!("{}-{}-{}-{}-{}", &s[..8], &s[8..12], &s[12..16], &s[16..20], &s[20..])
}

pub(crate) fn write_error(index: usize, code: i32, msg: String) -> Document {
    doc! { "index": index as i32, "code": code, "errmsg": msg }
}


impl Server {
    pub fn new(dbs: Arc<Dbs>, log_commands: bool) -> Arc<Self> {
        Arc::new(Self { dbs, builds: Arc::new(Mutex::new(HashSet::new())), sessions: Default::default(), next_txn_id: std::sync::atomic::AtomicU64::new(1), rs: Mutex::new(None), log_commands, cursors: Mutex::new(HashMap::new()), change_cursors: Mutex::new(HashMap::new()), next_conn: AtomicI64::new(1) })
    }

    pub fn new_connection_id(&self) -> i64 {
        self.next_conn.fetch_add(1, Relaxed)
    }

    pub async fn handle(&self, db: &str, cmd: Document, conn_id: i64) -> Document {
        let Some(name) = cmd.keys().next().cloned() else {
            return MongoError::new(59, "CommandNotFound", "Empty command document".into()).to_reply();
        };
        if self.log_commands {
            eprintln!("CMD {name} db={db}");
        }
        let t0 = std::time::Instant::now();
        let r = self.dispatch(db, &name, &cmd, conn_id).await;
        let ms = t0.elapsed().as_millis();
        if ms >= 500 {
            eprintln!("SLOW {name} db={db} ms={ms} ok={}", r.is_ok());
        }
        match r {
            Ok(d) => d,
            Err(e) => e.to_reply(),
        }
    }

    async fn dispatch(&self, db: &str, name: &str, cmd: &Document, conn_id: i64) -> R<Document> {
        // Commands that do not touch a tenant.
        match name {
            "hello" | "isMaster" | "ismaster" => return Ok(self.hello(name, conn_id)),
            "ping" => return Ok(ok()),
            "buildInfo" | "buildinfo" => return Ok(build_info()),
            "getParameter" => return get_parameter(cmd),
            "endSessions" => return self.end_sessions(cmd).await,
            "killSessions" | "refreshSessions" => return Ok(ok()),
            "commitTransaction" => return self.commit_txn(cmd).await,
            "abortTransaction" => return self.abort_txn(cmd).await,
            "whatsmyuri" => return Ok(doc! { "you": "127.0.0.1:0", "ok": 1.0 }),
            "connectionStatus" => {
                return Ok(doc! { "authInfo": { "authenticatedUsers": [], "authenticatedUserRoles": [], "authenticatedUserPrivileges": [] }, "ok": 1.0 })
            }
            "listDatabases" => return self.list_databases(cmd).await,
            "tenantStats" => return Ok(self.tenant_stats()),
            "ttlPass" => {
                let gap = int_arg(cmd.get("minGapSecs")).unwrap_or(0) as u64;
                self.dbs.ttl_pass(std::time::Duration::from_secs(gap), int_arg(cmd.get("maxWakes")).unwrap_or(1000) as usize).await?;
                return Ok(ok());
            }
            _ => {}
        }
        if !valid_db_name(db) {
            return Err(MongoError::new(73, "InvalidNamespace", format!("Invalid database name: '{db}'")));
        }
        if let Some(tc) = crate::txn::parse(cmd)? {
            return self.run_in_txn(db, name, cmd, &tc).await;
        }
        let mut no_tx: Option<crate::txn::Txn> = None;
        let tx = &mut no_tx;
        match name {
            "insert" => self.insert(db, cmd, tx).await,
            "find" => self.find(db, cmd, tx).await,
            "getMore" => self.get_more(db, cmd).await,
            "killCursors" => self.kill_cursors(cmd),
            "update" => self.update(db, cmd, tx).await,
            "delete" => self.delete(db, cmd, tx).await,
            "findAndModify" | "findandmodify" => self.find_and_modify(db, cmd, tx).await,
            "count" => self.count(db, cmd, tx).await,
            "distinct" => self.distinct(db, cmd, tx).await,
            "aggregate" => self.aggregate(db, cmd, tx).await,
            "drop" => self.drop_collection(db, cmd).await,
            "create" => self.create(db, cmd).await,
            "dropIndexes" => self.drop_indexes(db, cmd).await,
            "collMod" => self.coll_mod(db, cmd).await,
            "explain" => self.explain(db, cmd).await,
            "validate" => self.validate(db, cmd).await,
            "collStats" => self.coll_stats(db, cmd).await,
            "dbStats" => self.db_stats(db, cmd).await,
            "createIndexes" => self.create_indexes(db, cmd).await,
            "listIndexes" => self.list_indexes(db, cmd).await,
            "listCollections" => self.list_collections(db, cmd).await,
            "dropDatabase" => {
                let colls = self.dbs.lock(db).await.collections().await?;
                self.dbs.lock(db).await.drop_database().await?;
                let mut evs: Vec<crate::changes::EvSpec> = colls.into_iter().map(|c| crate::changes::EvSpec { db: db.to_string(), coll: Some(c), op: "drop", doc_key: None, full: None, desc: None, txn: None }).collect();
                evs.push(crate::changes::EvSpec { db: db.to_string(), coll: None, op: "dropDatabase", doc_key: None, full: None, desc: None, txn: None });
                self.dbs.changes.publish(evs);
                Ok(doc! { "dropped": db, "ok": 1.0 })
            }
            "tenantAdmin" => self.tenant_admin(db, cmd).await,
            "tenantTrace" => Box::pin(self.tenant_trace(db, cmd)).await,
            other => {
                eprintln!("UNKNOWN COMMAND {other:?} db={db}");
                Err(MongoError::command_not_found(other))
            }
        }
    }

    fn hello(&self, name: &str, conn_id: i64) -> Document {
        let mut d = Document::new();
        if name == "hello" {
            d.insert("isWritablePrimary", true);
        } else {
            d.insert("ismaster", true);
        }
        d.insert("helloOk", true);
        d.insert("maxBsonObjectSize", 16_777_216_i32);
        d.insert("maxMessageSizeBytes", 48_000_000_i32);
        d.insert("maxWriteBatchSize", 100_000_i32);
        d.insert("localTime", Bson::DateTime(DateTime::now()));
        d.insert("logicalSessionTimeoutMinutes", 30_i32);
        d.insert("connectionId", conn_id as i32);
        d.insert("minWireVersion", 0_i32);
        d.insert("maxWireVersion", MAX_WIRE_VERSION);
        d.insert("readOnly", false);
        if let Some((set, addr)) = &*self.rs.lock().unwrap() {
            d.insert("hosts", vec![Bson::String(addr.clone())]);
            d.insert("setName", set.clone());
            d.insert("setVersion", 1_i32);
            d.insert("secondary", false);
            d.insert("primary", addr.clone());
            d.insert("me", addr.clone());
            d.insert("electionId", ObjectId::from_bytes([0x7f, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 1]));
        }
        d.insert("ok", 1.0);
        d
    }

    // ---- databases and collections ---------------------------------------------------------

    async fn list_databases(&self, cmd: &Document) -> R<Document> {
        let name_only = bool_arg(cmd.get("nameOnly"), false);
        let names = self.dbs.list_databases().await?;
        let dbs: Vec<Bson> = names
            .into_iter()
            .map(|n| if name_only { Bson::Document(doc! { "name": n }) } else { Bson::Document(doc! { "name": n, "sizeOnDisk": 0_i64, "empty": false }) })
            .collect();
        let mut d = doc! { "databases": dbs };
        if !name_only {
            d.insert("totalSize", 0_i64);
            d.insert("totalSizeMb", 0_i64);
        }
        d.insert("ok", 1.0);
        Ok(d)
    }

    async fn list_collections(&self, db: &str, cmd: &Document) -> R<Document> {
        let name_only = bool_arg(cmd.get("nameOnly"), false);
        let filter = cmd.get_document("filter").cloned().unwrap_or_default();
        let mut g = self.dbs.lock(db).await;
        let mut names = g.collections().await?;
        drop(g);
        names.sort();
        let mut docs = VecDeque::new();
        for n in names {
            let d = if name_only {
                doc! { "name": n, "type": "collection" }
            } else {
                doc! { "name": n, "type": "collection", "options": {}, "info": { "readOnly": false }, "idIndex": { "v": 2, "key": { "_id": 1 }, "name": "_id_" } }
            };
            if matches(&d, &filter)? {
                docs.push_back(d);
            }
        }
        let batch = cmd.get_document("cursor").ok().and_then(|c| int_arg(c.get("batchSize"))).map(|b| b as usize);
        Ok(self.cursor_reply(format!("{db}.$cmd.listCollections"), docs, batch, false))
    }

    async fn create(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "create")?;
        let mut g = self.dbs.lock(db).await;
        let mut names = g.collections().await?;
        if names.iter().any(|n| n == coll) {
            // MongoDB 7.0: creating an existing collection with the same (default) options succeeds.
            let has_options = cmd.iter().any(|(k, v)| {
                !matches!(k.as_str(), "create" | "$db" | "lsid" | "$clusterTime" | "writeConcern" | "readConcern" | "comment" | "txnNumber" | "$readPreference")
                    && !(k == "capped" && matches!(v, Bson::Boolean(false)))
            });
            if has_options {
                return Err(MongoError::namespace_exists(format!("Collection already exists. NS: {db}.{coll}")));
            }
            return Ok(ok());
        }
        names.push(coll.to_string());
        g.apply(vec![Entry::collections_op(&names)?]).await?;
        Ok(ok())
    }

    /// Process-wide gauges and counters for the load tests: open tenants and every object-store request.
    fn tenant_stats(&self) -> Document {
        use std::sync::atomic::Ordering::Relaxed;
        let s = self.dbs.global.counters.snapshot();
        let ls = self.dbs.leases.stats.lock().unwrap().clone();
        doc! {
            "holder": self.dbs.leases.holder.clone(),
            "pid": std::process::id() as i64,
            "openReaders": self.dbs.readers.load(Relaxed),
            "openWriters": self.dbs.writers.load(Relaxed),
            "entries": self.dbs.entry_count() as i64,
            "leasesHeld": self.dbs.leases.held_count() as i64,
            "leases": { "acquired": ls.acquired as i64, "takeovers": ls.takeovers as i64, "refused": ls.refused as i64, "renewals": ls.renewals as i64, "renewFailures": ls.renew_failures as i64, "lost": ls.lost as i64 },
            "ttl": {
                "passes": self.dbs.ttl.passes.load(Relaxed) as i64, "hintsListed": self.dbs.ttl.hints_listed.load(Relaxed) as i64,
                "due": self.dbs.ttl.due.load(Relaxed) as i64, "skippedOpen": self.dbs.ttl.skipped_open.load(Relaxed) as i64,
                "skippedGap": self.dbs.ttl.skipped_gap.load(Relaxed) as i64, "wakes": self.dbs.ttl.wakes.load(Relaxed) as i64,
                "sessionSweeps": self.dbs.ttl.session_sweeps.load(Relaxed) as i64, "docsExpired": self.dbs.ttl.docs_expired.load(Relaxed) as i64,
            },
            "requests": {
                "put": (s.put + s.multipart_create + s.multipart_part + s.multipart_complete) as i64,
                "copy": s.copy as i64, "list": s.list as i64,
                "get": (s.get + s.get_range) as i64, "head": s.head as i64, "delete": s.delete as i64,
                "billedPutTier": s.billed_put_tier() as i64, "billedGetTier": s.billed_get_tier() as i64,
                "putBytes": self.dbs.global.counters.put_bytes.load(Relaxed) as i64, "getBytes": self.dbs.global.counters.get_bytes.load(Relaxed) as i64,
            },
            "putByCategory": tenant_bench::counting::CATEGORIES.iter().enumerate().map(|(i, n)| (n.to_string(), Bson::Int64(self.dbs.global.counters.put_cat[i].load(Relaxed) as i64))).collect::<Document>(),
            "getByCategory": tenant_bench::counting::CATEGORIES.iter().enumerate().map(|(i, n)| (n.to_string(), Bson::Int64(self.dbs.global.counters.get_cat[i].load(Relaxed) as i64))).collect::<Document>(),
            "maintenance": {
                "compactRuns": tenant_bench::lifecycle::MAINT.compact_runs.load(Relaxed) as i64, "compactPuts": tenant_bench::lifecycle::MAINT.compact_puts.load(Relaxed) as i64,
                "compactGets": tenant_bench::lifecycle::MAINT.compact_gets.load(Relaxed) as i64, "gcRuns": tenant_bench::lifecycle::MAINT.gc_runs.load(Relaxed) as i64,
                "gcDeletes": tenant_bench::lifecycle::MAINT.gc_deletes.load(Relaxed) as i64, "gcLists": tenant_bench::lifecycle::MAINT.gc_lists.load(Relaxed) as i64,
                "gcGets": tenant_bench::lifecycle::MAINT.gc_gets.load(Relaxed) as i64,
            },
            "wakeQueue": { "queued": crate::dbs::WAKE_QUEUED.load(Relaxed), "maxQueued": crate::dbs::WAKE_MAX_QUEUED.load(Relaxed), "waits": crate::dbs::WAKE_WAITS.load(Relaxed) as i64, "waitMs": crate::dbs::WAKE_WAIT_MS.load(Relaxed) as i64 },
            "lat": {
                "put": self.dbs.global.counters.lat_put.snapshot().into_iter().map(|x| x as i64).collect::<Vec<_>>(),
                "get": self.dbs.global.counters.lat_get.snapshot().into_iter().map(|x| x as i64).collect::<Vec<_>>(),
            },
            "ok": 1.0
        }
    }

    /// Runs a command with object-store tracing on and returns every request it made, in order. For the wake
    /// breakdown: `cold` closes the tenant first, `marker` (`present` | `missing`) sets the clean marker so the wake
    /// takes the clean or the unclean path. Meant for a quiet server (the trace is process-wide).
    async fn tenant_trace(&self, db: &str, cmd: &Document) -> R<Document> {
        use object_store::ObjectStoreExt;
        let run = cmd.get_document("run").map_err(|_| MongoError::bad_value("tenantTrace needs a `run` command".into()))?.clone();
        let name = run.keys().next().cloned().unwrap_or_default();
        if bool_arg(cmd.get("cold"), true) {
            let mut g = self.dbs.lock(db).await;
            g.close().await?;
        }
        let marker = object_store::path::Path::from(format!("tenants/{db}/_clean"));
        match cmd.get_str("marker").unwrap_or("") {
            "present" => {
                self.dbs.global.put(&marker, object_store::PutPayload::from_static(b"clean")).await.map_err(|e| MongoError::internal(e.to_string()))?;
            }
            "missing" => {
                let _ = self.dbs.global.delete(&marker).await;
            }
            _ => {}
        }
        self.dbs.global.trace.start();
        let t0 = std::time::Instant::now();
        let reply = self.dispatch(db, &name, &run, 0).await;
        let elapsed = t0.elapsed();
        let events = self.dbs.global.trace.take();
        Ok(doc! {
            "elapsedUs": elapsed.as_micros() as i64,
            "innerOk": reply.is_ok(),
            "events": events.into_iter().map(|e| Bson::Document(doc! { "op": e.op, "path": e.path, "startUs": e.start_us as i64, "durUs": e.dur_us as i64, "bytes": e.bytes as i64 })).collect::<Vec<_>>(),
            "ok": 1.0,
        })
    }

    async fn tenant_admin(&self, db: &str, cmd: &Document) -> R<Document> {
        let mut g = self.dbs.lock(db).await;
        if bool_arg(cmd.get("close"), false) {
            g.close().await?;
        }
        let (put_tier, get_tier, deletes) = self.dbs.request_counts();
        Ok(doc! {
            "mode": g.mode(), "wakes": g.wakes as i64, "closes": g.closes as i64, "uncleanWake": g.unclean_wake().unwrap_or(false),
            "requests": { "putTier": put_tier as i64, "getTier": get_tier as i64, "delete": deletes as i64 },
            "ok": 1.0
        })
    }

    // ---- cursors ---------------------------------------------------------------------------

    pub(crate) fn cursor_reply(&self, ns: String, mut docs: VecDeque<Document>, batch: Option<usize>, single_batch: bool) -> Document {
        let n = batch.unwrap_or(DEFAULT_FIRST_BATCH).min(docs.len());
        let first: Vec<Bson> = docs.drain(..n).map(Bson::Document).collect();
        let id = if docs.is_empty() || single_batch { 0 } else { self.store_cursor(ns.clone(), docs) };
        doc! { "cursor": { "firstBatch": first, "id": id, "ns": ns }, "ok": 1.0 }
    }

    fn store_cursor(&self, ns: String, docs: VecDeque<Document>) -> i64 {
        let mut c = self.cursors.lock().unwrap();
        loop {
            let id = (rand::random::<i64>() & i64::MAX) | (1 << 40);
            if !c.contains_key(&id) {
                c.insert(id, Cursor { ns, docs });
                return id;
            }
        }
    }

    pub(crate) async fn get_more(&self, db: &str, cmd: &Document) -> R<Document> {
        let id = int_arg(cmd.get("getMore")).ok_or_else(|| MongoError::new(14, "TypeMismatch", "getMore's cursor id must be a long".into()))?;
        if self.change_cursors.lock().unwrap().contains_key(&id) {
            return self.change_get_more(id, cmd).await;
        }
        let batch = int_arg(cmd.get("batchSize")).map(|b| b as usize);
        let mut cursors = self.cursors.lock().unwrap();
        let c = cursors.get_mut(&id).ok_or_else(|| MongoError::cursor_not_found(id))?;
        let n = batch.unwrap_or(usize::MAX).min(c.docs.len());
        let next: Vec<Bson> = c.docs.drain(..n).map(Bson::Document).collect();
        let ns = c.ns.clone();
        let done = c.docs.is_empty();
        if done {
            cursors.remove(&id);
        }
        let _ = db;
        Ok(doc! { "cursor": { "nextBatch": next, "id": if done { 0 } else { id }, "ns": ns }, "ok": 1.0 })
    }

    fn kill_cursors(&self, cmd: &Document) -> R<Document> {
        let ids = cmd.get_array("cursors").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'killCursors.cursors' is missing but a required field".into()))?;
        let mut killed = Vec::new();
        let mut not_found = Vec::new();
        let mut cursors = self.cursors.lock().unwrap();
        for id in ids.iter().filter_map(|b| int_arg(Some(b))) {
            if cursors.remove(&id).is_some() || self.change_cursors.lock().unwrap().remove(&id).is_some() { killed.push(Bson::Int64(id)) } else { not_found.push(Bson::Int64(id)) }
        }
        Ok(doc! { "cursorsKilled": killed, "cursorsNotFound": not_found, "cursorsAlive": [], "cursorsUnknown": [], "ok": 1.0 })
    }
}

fn build_info() -> Document {
    doc! {
        "version": "7.0.14",
        "gitVersion": "none",
        "modules": [],
        "allocator": "system",
        "javascriptEngine": "none",
        "sysInfo": "deprecated",
        "versionArray": [7, 0, 14, 0],
        "openssl": { "running": "disabled", "compiled": "disabled" },
        "buildEnvironment": {},
        "bits": 64,
        "debug": false,
        "maxBsonObjectSize": 16_777_216_i32,
        "storageEngines": ["slatedb"],
        "ok": 1.0,
    }
}

fn get_parameter(cmd: &Document) -> R<Document> {
    let all = matches!(cmd.get("getParameter"), Some(Bson::String(s)) if s == "*");
    let mut reply = Document::new();
    let mut found = false;
    if all || cmd.get("featureCompatibilityVersion").is_some() {
        reply.insert("featureCompatibilityVersion", doc! { "version": "7.0" });
        found = true;
    }
    if !found {
        return Err(MongoError::new(2, "BadValue", "no option found to get".into()));
    }
    reply.insert("ok", 1.0);
    Ok(reply)
}

