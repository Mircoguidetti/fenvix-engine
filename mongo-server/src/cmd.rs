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

type R<T> = Result<T, MongoError>;

const MAX_WIRE_VERSION: i32 = 21;
const DEFAULT_FIRST_BATCH: usize = 101;

pub struct Server {
    pub dbs: Arc<Dbs>,
    /// Print every command name and database (used to discover what drivers send on connect).
    pub log_commands: bool,
    cursors: Mutex<HashMap<i64, Cursor>>,
    next_conn: AtomicI64,
}

struct Cursor {
    ns: String,
    docs: VecDeque<Document>,
}

fn ok() -> Document {
    doc! { "ok": 1.0 }
}

fn int_arg(v: Option<&Bson>) -> Option<i64> {
    match v? {
        Bson::Int32(i) => Some(*i as i64),
        Bson::Int64(i) => Some(*i),
        Bson::Double(f) if f.fract() == 0.0 => Some(*f as i64),
        _ => None,
    }
}

fn bool_arg(v: Option<&Bson>, default: bool) -> bool {
    match v {
        Some(Bson::Boolean(b)) => *b,
        Some(Bson::Int32(i)) => *i != 0,
        Some(Bson::Int64(i)) => *i != 0,
        Some(Bson::Double(f)) => *f != 0.0,
        _ => default,
    }
}

fn collection_name<'a>(cmd: &'a Document, key: &str) -> R<&'a str> {
    match cmd.get(key) {
        Some(Bson::String(s)) if !s.is_empty() && !s.contains('\0') && !s.contains('$') => Ok(s),
        Some(Bson::String(s)) => Err(MongoError::new(73, "InvalidNamespace", format!("Invalid collection name specified '{s}'"))),
        _ => Err(MongoError::new(2, "BadValue", format!("collection name has invalid type {}", cmd.get(key).map(crate::update::type_name).unwrap_or("missing")))),
    }
}

/// Random UUID-shaped string for messages that embed a build/collection id.
fn fake_uuid() -> String {
    let h: u128 = rand::random();
    let s = format!("{h:032x}");
    format!("{}-{}-{}-{}-{}", &s[..8], &s[8..12], &s[12..16], &s[16..20], &s[20..])
}

fn write_error(index: usize, code: i32, msg: String) -> Document {
    doc! { "index": index as i32, "code": code, "errmsg": msg }
}


/// Key of a document in a (possibly compound) index: encoded bytes plus the `{field: value}` form.
fn index_entry(d: &Document, pattern: &Document) -> (Vec<u8>, Document) {
    let mut bytes = Vec::new();
    let mut value = Document::new();
    for (field, _) in pattern {
        let v = path_values(d, field).first().cloned().cloned().unwrap_or(Bson::Null);
        let e = encode_sortable(&v);
        bytes.extend((e.len() as u32).to_be_bytes());
        bytes.extend(e);
        value.insert(field.clone(), v);
    }
    (bytes, value)
}

struct UniqueIndex {
    name: String,
    pattern: Document,
    owners: HashMap<Vec<u8>, Vec<u8>>, // index key -> encoded _id of the owner
}

fn unique_indexes(indexes: &[Document], docs: &[Document]) -> Vec<UniqueIndex> {
    indexes
        .iter()
        .filter(|i| bool_arg(i.get("unique"), false))
        .filter_map(|i| {
            let pattern = i.get_document("key").ok()?.clone();
            let name = i.get_str("name").ok()?.to_string();
            let mut owners = HashMap::new();
            for d in docs {
                let id = encode_sortable(d.get("_id").unwrap_or(&Bson::Null));
                owners.insert(index_entry(d, &pattern).0, id);
            }
            Some(UniqueIndex { name, pattern, owners })
        })
        .collect()
}

impl Server {
    pub fn new(dbs: Arc<Dbs>, log_commands: bool) -> Arc<Self> {
        Arc::new(Self { dbs, log_commands, cursors: Mutex::new(HashMap::new()), next_conn: AtomicI64::new(1) })
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
        // Writes staged by the command become durable here, after its locks were released; the reply waits for it.
        let mut r = crate::dbs::TOKENS
            .scope(Default::default(), async {
                let (mut r, mut commits) = crate::dbs::PENDING
                    .scope(Default::default(), async {
                        let r = self.dispatch(db, &name, &cmd, conn_id).await;
                        (r, crate::dbs::PENDING.with(|p| std::mem::take(&mut *p.borrow_mut())))
                    })
                    .await;
                if let Some(last) = commits.pop() {
                    // Later commits flush after earlier ones, so waiting for the last covers all of them.
                    if let Err(e) = last.wait().await {
                        r = Err(e.into());
                    }
                }
                r
            })
            .await;
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
            "endSessions" | "killSessions" | "refreshSessions" => return Ok(ok()),
            "whatsmyuri" => return Ok(doc! { "you": "127.0.0.1:0", "ok": 1.0 }),
            "connectionStatus" => {
                return Ok(doc! { "authInfo": { "authenticatedUsers": [], "authenticatedUserRoles": [], "authenticatedUserPrivileges": [] }, "ok": 1.0 })
            }
            "listDatabases" => return self.list_databases(cmd).await,
            "tenantStats" => return Ok(self.tenant_stats()),
            _ => {}
        }
        if !valid_db_name(db) {
            return Err(MongoError::new(73, "InvalidNamespace", format!("Invalid database name: '{db}'")));
        }
        match name {
            "insert" => self.insert(db, cmd).await,
            "find" => self.find(db, cmd).await,
            "getMore" => self.get_more(db, cmd),
            "killCursors" => self.kill_cursors(cmd),
            "update" => self.update(db, cmd).await,
            "delete" => self.delete(db, cmd).await,
            "findAndModify" | "findandmodify" => self.find_and_modify(db, cmd).await,
            "count" => self.count(db, cmd).await,
            "distinct" => self.distinct(db, cmd).await,
            "aggregate" => self.aggregate(db, cmd).await,
            "drop" => self.drop_collection(db, cmd).await,
            "create" => self.create(db, cmd).await,
            "createIndexes" => self.create_indexes(db, cmd).await,
            "listIndexes" => self.list_indexes(db, cmd).await,
            "listCollections" => self.list_collections(db, cmd).await,
            "dropDatabase" => {
                self.dbs.lock(db).await.drop_database().await?;
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

    async fn drop_collection(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "drop")?;
        let mut g = self.dbs.lock(db).await;
        let mut names = g.collections().await?;
        let Some(pos) = names.iter().position(|n| n == coll) else {
            // MongoDB 7.0: dropping a collection that does not exist succeeds.
            return Ok(ok());
        };
        let n_indexes = 1 + g.indexes(coll).await?.len() as i32;
        let rows = g.scan(&doc_prefix(coll)).await?;
        let mut ops: Vec<Op> = rows.into_iter().map(|(k, _)| Op::Delete(k.to_vec())).collect();
        ops.push(Op::Delete(index_key(coll)));
        names.remove(pos);
        ops.push(Entry::collections_op(&names)?);
        g.apply(ops).await?;
        Ok(doc! { "nIndexesWas": n_indexes, "ns": format!("{db}.{coll}"), "ok": 1.0 })
    }

    async fn list_indexes(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "listIndexes")?;
        let mut g = self.dbs.lock(db).await;
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Err(MongoError::namespace_not_found(format!("ns does not exist: {db}.{coll}")));
        }
        let mut docs: VecDeque<Document> = VecDeque::new();
        docs.push_back(doc! { "v": 2, "key": { "_id": 1 }, "name": "_id_" });
        docs.extend(g.indexes(coll).await?);
        drop(g);
        let batch = cmd.get_document("cursor").ok().and_then(|c| int_arg(c.get("batchSize"))).map(|b| b as usize);
        Ok(self.cursor_reply(format!("{db}.{coll}"), docs, batch, false))
    }

    async fn create_indexes(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "createIndexes")?;
        let specs = cmd.get_array("indexes").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'createIndexes.indexes' is missing but a required field".into()))?;
        let mut g = self.dbs.lock(db).await;
        let mut names = g.collections().await?;
        let existed = names.iter().any(|n| n == coll);
        let mut indexes = g.indexes(coll).await?;
        let before = 1 + indexes.len() as i32;
        let docs = if existed { g.scan_collection(coll).await? } else { vec![] };
        let mut changed = false;
        for spec in specs {
            let spec = spec.as_document().ok_or_else(|| MongoError::new(14, "TypeMismatch", "Each element of the 'indexes' array must be an object".into()))?;
            let key = spec.get_document("key").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'indexes.key' is missing but a required field".into()))?;
            if key.is_empty() {
                return Err(MongoError::new(67, "CannotCreateIndex", "Index keys cannot be empty.".into()));
            }
            let name = spec.get_str("name").map(str::to_string).unwrap_or_else(|_| {
                key.iter().map(|(k, v)| format!("{k}_{}", match v { Bson::String(s) => s.clone(), other => other.to_string() })).collect::<Vec<_>>().join("_")
            });
            if name == "_id_" || (key.len() == 1 && key.get("_id").is_some()) {
                continue; // the implicit _id index
            }
            if let Some(ex) = indexes.iter().find(|i| i.get_str("name").ok() == Some(name.as_str())) {
                if ex.get_document("key").map(|k| k == key).unwrap_or(false) {
                    continue;
                }
                return Err(MongoError::new(86, "IndexKeySpecsConflict", format!("An existing index has the same name as the requested index. When index names are not specified, they are auto generated and can cause conflicts. Please refer to our documentation. Requested index: {}, existing index: {}", fmt_doc(spec), fmt_doc(ex))));
            }
            if let Some(ex) = indexes.iter().find(|i| i.get_document("key").map(|k| k == key).unwrap_or(false)) {
                return Err(MongoError::new(85, "IndexOptionsConflict", format!("Index already exists with a different name: {}", ex.get_str("name").unwrap_or(""))));
            }
            let unique = bool_arg(spec.get("unique"), false);
            if unique {
                let mut seen = HashSet::new();
                for d in &docs {
                    let (b, v) = index_entry(d, key);
                    if !seen.insert(b) {
                        let mut e = MongoError::duplicate_key(&format!("{db}.{coll}"), &name, key, &v);
                        e.msg = format!("Index build failed: {}: Collection {db}.{coll} ( {} ) :: caused by :: {}", fake_uuid(), fake_uuid(), e.msg);
                        return Err(e);
                    }
                }
            }
            let mut idx = doc! { "v": 2, "key": key.clone(), "name": name };
            if unique {
                idx.insert("unique", true);
            }
            for (k, v) in spec {
                if !matches!(k.as_str(), "key" | "name" | "v" | "unique") {
                    idx.insert(k.clone(), v.clone());
                }
            }
            indexes.push(idx);
            changed = true;
        }
        let mut ops = Vec::new();
        if !existed {
            names.push(coll.to_string());
            ops.push(Entry::collections_op(&names)?);
        }
        if changed {
            ops.push(Entry::indexes_op(coll, &indexes)?);
        }
        g.apply(ops).await?;
        let mut reply = doc! { "createdCollectionAutomatically": !existed, "numIndexesBefore": before, "numIndexesAfter": 1 + indexes.len() as i32 };
        if !changed {
            reply.insert("note", "all indexes already exist");
        }
        reply.insert("ok", 1.0);
        Ok(reply)
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
            "requests": {
                "put": (s.put + s.multipart_create + s.multipart_part + s.multipart_complete) as i64,
                "copy": s.copy as i64, "list": s.list as i64,
                "get": (s.get + s.get_range) as i64, "head": s.head as i64, "delete": s.delete as i64,
                "billedPutTier": s.billed_put_tier() as i64, "billedGetTier": s.billed_get_tier() as i64,
            },
            "groupCommit": { "commits": tenant_bench::lifecycle::GROUP_COMMITS.load(Relaxed) as i64, "flushes": tenant_bench::lifecycle::GROUP_FLUSHES.load(Relaxed) as i64, "windowUs": tenant_bench::lifecycle::GROUP_WINDOW_US.load(Relaxed) as i64 },
            "walGc": { "passes": tenant_bench::lifecycle::WAL_GC_PASSES.load(Relaxed) as i64, "deleted": tenant_bench::lifecycle::WAL_GC_DELETED.load(Relaxed) as i64, "listUs": tenant_bench::lifecycle::WAL_GC_LIST_US.load(Relaxed) as i64, "deleteUs": tenant_bench::lifecycle::WAL_GC_DELETE_US.load(Relaxed) as i64, "totalUs": tenant_bench::lifecycle::WAL_GC_TOTAL_US.load(Relaxed) as i64 },
            "sstGc": { "passes": tenant_bench::lifecycle::SST_GC_PASSES.load(Relaxed) as i64, "deletedSsts": tenant_bench::lifecycle::SST_GC_DELETED_SSTS.load(Relaxed) as i64, "deletedManifests": tenant_bench::lifecycle::SST_GC_DELETED_MANIFESTS.load(Relaxed) as i64 },
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

    fn cursor_reply(&self, ns: String, mut docs: VecDeque<Document>, batch: Option<usize>, single_batch: bool) -> Document {
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

    fn get_more(&self, db: &str, cmd: &Document) -> R<Document> {
        let id = int_arg(cmd.get("getMore")).ok_or_else(|| MongoError::new(14, "TypeMismatch", "getMore's cursor id must be a long".into()))?;
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
            if cursors.remove(&id).is_some() { killed.push(Bson::Int64(id)) } else { not_found.push(Bson::Int64(id)) }
        }
        Ok(doc! { "cursorsKilled": killed, "cursorsNotFound": not_found, "cursorsAlive": [], "cursorsUnknown": [], "ok": 1.0 })
    }

    // ---- reads -----------------------------------------------------------------------------

    /// Documents of a collection matching `filter`; `_id` equality becomes a point read.
    async fn matching_docs(&self, g: &crate::dbs::ReadView, coll: &str, filter: &Document) -> R<Vec<Document>> {
        if let Some(id) = id_equality(filter) {
            return Ok(g.get_doc(coll, id).await?.into_iter().collect());
        }
        let mut out = Vec::new();
        for d in g.scan_collection(coll).await? {
            if matches(&d, filter)? {
                out.push(d);
            }
        }
        Ok(out)
    }

    async fn find(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "find")?;
        let filter = cmd.get_document("filter").cloned().unwrap_or_default();
        let sort = match cmd.get_document("sort") {
            Ok(s) => parse_sort(s)?,
            Err(_) => vec![],
        };
        let projection = match cmd.get_document("projection") {
            Ok(p) => parse_projection(p)?,
            Err(_) => None,
        };
        let skip = int_arg(cmd.get("skip")).unwrap_or(0).max(0) as usize;
        let limit = int_arg(cmd.get("limit")).unwrap_or(0);
        let batch = int_arg(cmd.get("batchSize")).map(|b| b.max(0) as usize);
        let single = bool_arg(cmd.get("singleBatch"), false);
        let mut docs = {
            let g = self.dbs.read(db).await?;
            self.matching_docs(&g, coll, &filter).await?
        };
        sort_docs(&mut docs, &sort);
        let mut docs: VecDeque<Document> = docs.into_iter().skip(skip).collect();
        if limit > 0 {
            docs.truncate(limit as usize);
        }
        if let Some(p) = &projection {
            for d in docs.iter_mut() {
                *d = project(d, p);
            }
        }
        // A limit that fits in the first batch closes the cursor.
        Ok(self.cursor_reply(format!("{db}.{coll}"), docs, batch, single))
    }

    async fn count(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "count")?;
        let filter = cmd.get_document("query").cloned().unwrap_or_default();
        let skip = int_arg(cmd.get("skip")).unwrap_or(0).max(0) as usize;
        let limit = int_arg(cmd.get("limit")).unwrap_or(0).unsigned_abs() as usize;
        let n = {
            let g = self.dbs.read(db).await?;
            self.matching_docs(&g, coll, &filter).await?.len()
        };
        let mut n = n.saturating_sub(skip);
        if limit > 0 {
            n = n.min(limit);
        }
        Ok(doc! { "n": n as i32, "ok": 1.0 })
    }

    async fn distinct(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "distinct")?;
        let key = cmd.get_str("key").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'distinct.key' is missing but a required field".into()))?;
        let filter = cmd.get_document("query").cloned().unwrap_or_default();
        let docs = {
            let g = self.dbs.read(db).await?;
            self.matching_docs(&g, coll, &filter).await?
        };
        let mut values: Vec<Bson> = Vec::new();
        for d in &docs {
            for v in path_values(d, key) {
                let items: Vec<&Bson> = match v {
                    Bson::Array(a) => a.iter().collect(),
                    other => vec![other],
                };
                for it in items {
                    if !values.iter().any(|x| query::equal(x, it)) {
                        values.push(it.clone());
                    }
                }
            }
        }
        values.sort_by(query::compare);
        Ok(doc! { "values": values, "ok": 1.0 })
    }

    async fn aggregate(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "aggregate")?;
        let pipeline = cmd.get_array("pipeline").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'aggregate.pipeline' is missing but a required field".into()))?;
        let cursor = cmd.get_document("cursor").map_err(|_| MongoError::failed_to_parse("The 'cursor' option is required, except for aggregate with the explain argument".into()))?;
        let batch = int_arg(cursor.get("batchSize")).map(|b| b.max(0) as usize);
        let docs = {
            let g = self.dbs.read(db).await?;
            g.scan_collection(coll).await?
        };
        let mut foreign = crate::agg::Foreign::new();
        for from in crate::agg::lookup_collections(pipeline) {
            let fd = {
                let g = self.dbs.read(db).await?;
                g.scan_collection(&from).await?
            };
            foreign.insert(from, fd);
        }
        let out = run_pipeline(docs, pipeline, &foreign)?;
        Ok(self.cursor_reply(format!("{db}.{coll}"), out.into(), batch, false))
    }

    // ---- writes ----------------------------------------------------------------------------

    async fn insert(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "insert")?;
        let docs = cmd.get_array("documents").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'insert.documents' is missing but a required field".into()))?;
        let ordered = bool_arg(cmd.get("ordered"), true);
        let mut g = self.dbs.lock(db).await;
        let mut names = g.collections().await?;
        let indexes = g.indexes(coll).await?;
        let has_unique = indexes.iter().any(|i| bool_arg(i.get("unique"), false));
        let scan = has_unique || docs.len() > 16;
        let existing = if scan { g.scan_collection(coll).await? } else { vec![] };
        let mut ids: HashSet<Vec<u8>> = existing.iter().map(|d| encode_sortable(d.get("_id").unwrap_or(&Bson::Null))).collect();
        let mut uniques = unique_indexes(&indexes, &existing);
        drop(existing);

        let mut ops = Vec::new();
        let mut errors: Vec<Bson> = Vec::new();
        let mut n = 0;
        for (i, raw) in docs.iter().enumerate() {
            let Some(d) = raw.as_document() else {
                errors.push(Bson::Document(write_error(i, 14, "document to insert must be an object".into())));
                if ordered { break } else { continue }
            };
            let mut d = d.clone();
            if d.get("_id").is_none() {
                let mut with_id = doc! { "_id": ObjectId::new() };
                for (k, v) in d {
                    with_id.insert(k, v);
                }
                d = with_id;
            } else if d.keys().next().map(String::as_str) != Some("_id") {
                // MongoDB always stores `_id` as the first field.
                let id = d.remove("_id").unwrap();
                let mut with_id = doc! { "_id": id };
                for (k, v) in d {
                    with_id.insert(k, v);
                }
                d = with_id;
            }
            let id = d.get("_id").unwrap().clone();
            if matches!(id, Bson::Array(_) | Bson::RegularExpression(_) | Bson::Undefined) {
                errors.push(Bson::Document(write_error(i, 53, format!("The '_id' value cannot be of type {}", crate::update::type_name(&id)))));
                if ordered { break } else { continue }
            }
            let idb = encode_sortable(&id);
            let dup_id = ids.contains(&idb) || (!scan && g.get_doc(coll, &id).await?.is_some());
            if dup_id {
                errors.push(Bson::Document(MongoError::duplicate_key(&format!("{db}.{coll}"), "_id_", &doc! { "_id": 1 }, &doc! { "_id": id }).to_write_error(i)));
                if ordered { break } else { continue }
            }
            let mut dup = None;
            for u in &uniques {
                let (b, v) = index_entry(&d, &u.pattern);
                if u.owners.contains_key(&b) {
                    dup = Some(MongoError::duplicate_key(&format!("{db}.{coll}"), &u.name, &u.pattern, &v));
                    break;
                }
            }
            if let Some(e) = dup {
                errors.push(Bson::Document(e.to_write_error(i)));
                if ordered { break } else { continue }
            }
            for u in uniques.iter_mut() {
                u.owners.insert(index_entry(&d, &u.pattern).0, idb.clone());
            }
            ids.insert(idb);
            ops.push(Entry::put_doc_op(coll, &d)?);
            n += 1;
        }
        if !names.iter().any(|x| x == coll) && n > 0 {
            names.push(coll.to_string());
            ops.push(Entry::collections_op(&names)?);
        }
        g.apply(ops).await?;
        let mut reply = doc! { "n": n as i32 };
        if !errors.is_empty() {
            reply.insert("writeErrors", errors);
        }
        reply.insert("ok", 1.0);
        Ok(reply)
    }

    async fn update(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "update")?;
        let specs = cmd.get_array("updates").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'update.updates' is missing but a required field".into()))?;
        let ordered = bool_arg(cmd.get("ordered"), true);
        let mut g = self.dbs.lock(db).await;
        let mut names = g.collections().await?;
        let indexes = g.indexes(coll).await?;
        let mut docs = g.scan_collection(coll).await?;
        let mut uniques = unique_indexes(&indexes, &docs);
        let mut id_pos: HashMap<Vec<u8>, usize> = docs.iter().enumerate().map(|(i, d)| (encode_sortable(d.get("_id").unwrap_or(&Bson::Null)), i)).collect();
        let mut puts: HashMap<usize, ()> = HashMap::new();
        let mut new_docs: Vec<usize> = Vec::new();
        let (mut n, mut n_modified) = (0_i32, 0_i32);
        let mut upserted: Vec<Bson> = Vec::new();
        let mut errors: Vec<Bson> = Vec::new();

        'specs: for (i, spec) in specs.iter().enumerate() {
            let step: R<()> = (|| {
                let spec = spec.as_document().ok_or_else(|| MongoError::new(14, "TypeMismatch", "update statement must be an object".into()))?;
                let q = spec.get_document("q").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'updates.q' is missing but a required field".into()))?;
                let u = match spec.get("u") {
                    Some(Bson::Document(u)) => u,
                    Some(Bson::Array(_)) => return Err(MongoError::new(238, "NotImplemented", "pipeline-style updates are not supported by this server".into())),
                    _ => return Err(MongoError::new(40414, "Location40414", "BSON field 'updates.u' is missing but a required field".into())),
                };
                if let Some(Bson::Array(a)) = spec.get("arrayFilters") {
                    if !a.is_empty() {
                        return Err(MongoError::new(238, "NotImplemented", "arrayFilters are not supported by this server".into()));
                    }
                }
                let multi = bool_arg(spec.get("multi"), false);
                let upsert = bool_arg(spec.get("upsert"), false);
                let mut matched = Vec::new();
                for (pos, d) in docs.iter().enumerate() {
                    if matches(d, q)? {
                        matched.push(pos);
                        if !multi {
                            break;
                        }
                    }
                }
                if matched.is_empty() {
                    if !upsert {
                        return Ok(());
                    }
                    let nd = upsert_document(q, u)?;
                    let id = nd.get("_id").cloned().unwrap();
                    let idb = encode_sortable(&id);
                    if id_pos.contains_key(&idb) {
                        return Err(MongoError::duplicate_key(&format!("{db}.{coll}"), "_id_", &doc! { "_id": 1 }, &doc! { "_id": id }));
                    }
                    for un in &uniques {
                        let (b, v) = index_entry(&nd, &un.pattern);
                        if un.owners.contains_key(&b) {
                            return Err(MongoError::duplicate_key(&format!("{db}.{coll}"), &un.name, &un.pattern, &v));
                        }
                    }
                    for un in uniques.iter_mut() {
                        un.owners.insert(index_entry(&nd, &un.pattern).0, idb.clone());
                    }
                    id_pos.insert(idb, docs.len());
                    new_docs.push(docs.len());
                    puts.insert(docs.len(), ());
                    docs.push(nd);
                    upserted.push(Bson::Document(doc! { "index": i as i32, "_id": id }));
                    n += 1;
                    return Ok(());
                }
                for pos in matched {
                    let old = docs[pos].clone();
                    let new = apply_update(&old, u)?;
                    n += 1;
                    if new.to_vec()? == old.to_vec()? {
                        continue;
                    }
                    let idb = encode_sortable(old.get("_id").unwrap_or(&Bson::Null));
                    for un in &uniques {
                        let (b, v) = index_entry(&new, &un.pattern);
                        if let Some(owner) = un.owners.get(&b) {
                            if *owner != idb {
                                return Err(MongoError::duplicate_key(&format!("{db}.{coll}"), &un.name, &un.pattern, &v));
                            }
                        }
                    }
                    for un in uniques.iter_mut() {
                        un.owners.remove(&index_entry(&old, &un.pattern).0);
                        un.owners.insert(index_entry(&new, &un.pattern).0, idb.clone());
                    }
                    docs[pos] = new;
                    puts.insert(pos, ());
                    n_modified += 1;
                }
                Ok(())
            })();
            if let Err(e) = step {
                errors.push(Bson::Document(e.to_write_error(i)));
                if ordered {
                    break 'specs;
                }
            }
        }
        let _ = new_docs;
        let mut ops = Vec::new();
        for pos in puts.keys() {
            ops.push(Entry::put_doc_op(coll, &docs[*pos])?);
        }
        if !upserted.is_empty() && !names.iter().any(|x| x == coll) {
            names.push(coll.to_string());
            ops.push(Entry::collections_op(&names)?);
        }
        g.apply(ops).await?;
        let mut reply = doc! { "n": n, "nModified": n_modified };
        if !upserted.is_empty() {
            reply.insert("upserted", upserted);
        }
        if !errors.is_empty() {
            reply.insert("writeErrors", errors);
        }
        reply.insert("ok", 1.0);
        Ok(reply)
    }

    async fn find_and_modify(&self, db: &str, cmd: &Document) -> R<Document> {
        let key = cmd.keys().next().cloned().unwrap_or_default();
        let coll = collection_name(cmd, &key)?;
        let ns = format!("{db}.{coll}");
        let q = match cmd.get("query") {
            None | Some(Bson::Null) => Document::new(),
            Some(Bson::Document(d)) => d.clone(),
            Some(_) => return Err(MongoError::type_mismatch("BSON field 'findAndModify.query' is the wrong type, expected object".into())),
        };
        let sort = match cmd.get("sort") {
            Some(Bson::Document(s)) => parse_sort(s)?,
            _ => vec![],
        };
        let remove = bool_arg(cmd.get("remove"), false);
        let want_new = bool_arg(cmd.get("new"), false);
        let upsert = bool_arg(cmd.get("upsert"), false);
        let projection = match cmd.get("fields") {
            Some(Bson::Document(p)) => parse_projection(p)?,
            _ => None,
        };
        if let Some(Bson::Array(a)) = cmd.get("arrayFilters") {
            if !a.is_empty() {
                return Err(MongoError::new(238, "NotImplemented", "arrayFilters are not supported by this server".into()));
            }
        }
        let u = match cmd.get("update") {
            None => None,
            Some(Bson::Document(u)) => Some(u),
            Some(Bson::Array(_)) => return Err(MongoError::new(238, "NotImplemented", "pipeline-style updates are not supported by this server".into())),
            Some(_) => return Err(MongoError::new(14, "TypeMismatch", "BSON field 'findAndModify.update' is the wrong type, expected object or array".into())),
        };
        if remove && u.is_some() {
            return Err(MongoError::failed_to_parse("Cannot specify both an update and remove=true".into()));
        }
        if remove && upsert {
            return Err(MongoError::failed_to_parse("Cannot specify both upsert=true and remove=true".into()));
        }
        if remove && want_new {
            return Err(MongoError::failed_to_parse("Cannot specify both new=true and remove=true; 'remove' always returns the deleted document".into()));
        }
        if !remove && u.is_none() {
            return Err(MongoError::failed_to_parse("Either an update or remove=true must be specified".into()));
        }
        let shape = |d: Option<Document>| -> Bson {
            match d {
                None => Bson::Null,
                Some(d) => Bson::Document(match &projection {
                    Some(p) => project(&d, p),
                    None => d,
                }),
            }
        };
        let mut g = self.dbs.lock(db).await;
        let mut names = g.collections().await?;
        let mut docs = g.scan_collection(coll).await?;
        let mut matched: Vec<usize> = Vec::new();
        for (pos, d) in docs.iter().enumerate() {
            if matches(d, &q)? {
                matched.push(pos);
            }
        }
        if !sort.is_empty() && matched.len() > 1 {
            let mut m: Vec<Document> = matched.iter().map(|p| docs[*p].clone()).collect();
            sort_docs(&mut m, &sort);
            let id = |d: &Document| encode_sortable(d.get("_id").unwrap_or(&Bson::Null));
            let first = id(&m[0]);
            matched = vec![docs.iter().position(|d| id(d) == first).unwrap()];
        }
        let hit = matched.first().copied();
        if remove {
            return Ok(match hit {
                None => doc! { "lastErrorObject": { "n": 0 }, "value": Bson::Null, "ok": 1.0 },
                Some(pos) => {
                    let old = docs.remove(pos);
                    g.apply(vec![Entry::delete_doc_op(coll, old.get("_id").unwrap_or(&Bson::Null))]).await?;
                    doc! { "lastErrorObject": { "n": 1 }, "value": shape(Some(old)), "ok": 1.0 }
                }
            });
        }
        let u = u.unwrap();
        let indexes = g.indexes(coll).await?;
        let uniques = unique_indexes(&indexes, &docs);
        let check = |nd: &Document, own: Option<&Document>| -> R<()> {
            let idb = encode_sortable(nd.get("_id").unwrap_or(&Bson::Null));
            if own.is_none() && docs.iter().any(|d| encode_sortable(d.get("_id").unwrap_or(&Bson::Null)) == idb) {
                return Err(MongoError::duplicate_key(&ns, "_id_", &doc! { "_id": 1 }, &doc! { "_id": nd.get("_id").cloned().unwrap_or(Bson::Null) }));
            }
            for un in &uniques {
                let (b, v) = index_entry(nd, &un.pattern);
                if let Some(owner) = un.owners.get(&b) {
                    if *owner != idb {
                        return Err(MongoError::duplicate_key(&ns, &un.name, &un.pattern, &v));
                    }
                }
            }
            Ok(())
        };
        match hit {
            Some(pos) => {
                let old = docs[pos].clone();
                let new = apply_update(&old, u)?;
                if new.to_vec()? != old.to_vec()? {
                    check(&new, Some(&old))?;
                    g.apply(vec![Entry::put_doc_op(coll, &new)?]).await?;
                }
                let value = shape(Some(if want_new { new } else { old }));
                Ok(doc! { "lastErrorObject": { "n": 1, "updatedExisting": true }, "value": value, "ok": 1.0 })
            }
            None if upsert => {
                let nd = upsert_document(&q, u)?;
                check(&nd, None)?;
                let id = nd.get("_id").cloned().unwrap();
                let mut ops = vec![Entry::put_doc_op(coll, &nd)?];
                if !names.iter().any(|x| x == coll) {
                    names.push(coll.to_string());
                    ops.push(Entry::collections_op(&names)?);
                }
                g.apply(ops).await?;
                let value = if want_new { shape(Some(nd)) } else { Bson::Null };
                Ok(doc! { "lastErrorObject": { "n": 1, "updatedExisting": false, "upserted": id }, "value": value, "ok": 1.0 })
            }
            None => Ok(doc! { "lastErrorObject": { "n": 0, "updatedExisting": false }, "value": Bson::Null, "ok": 1.0 }),
        }
    }

    async fn delete(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "delete")?;
        let specs = cmd.get_array("deletes").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'delete.deletes' is missing but a required field".into()))?;
        let mut g = self.dbs.lock(db).await;
        let mut docs = g.scan_collection(coll).await?;
        let mut ops = Vec::new();
        let mut n = 0;
        let mut errors: Vec<Bson> = Vec::new();
        let ordered = bool_arg(cmd.get("ordered"), true);
        for (i, spec) in specs.iter().enumerate() {
            let res: R<()> = (|| {
                let spec = spec.as_document().ok_or_else(|| MongoError::new(14, "TypeMismatch", "delete statement must be an object".into()))?;
                let q = spec.get_document("q").map_err(|_| MongoError::new(40414, "Location40414", "BSON field 'deletes.q' is missing but a required field".into()))?;
                let limit = int_arg(spec.get("limit")).unwrap_or(0);
                let mut keep = Vec::with_capacity(docs.len());
                let mut deleted = 0;
                for d in docs.drain(..) {
                    if (limit == 0 || deleted < limit) && matches(&d, q)? {
                        ops.push(Entry::delete_doc_op(coll, d.get("_id").unwrap_or(&Bson::Null)));
                        deleted += 1;
                    } else {
                        keep.push(d);
                    }
                }
                docs = keep;
                n += deleted as i32;
                Ok(())
            })();
            if let Err(e) = res {
                errors.push(Bson::Document(write_error(i, e.code, e.msg)));
                if ordered {
                    break;
                }
            }
        }
        g.apply(ops).await?;
        let mut reply = doc! { "n": n };
        if !errors.is_empty() {
            reply.insert("writeErrors", errors);
        }
        reply.insert("ok", 1.0);
        Ok(reply)
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
