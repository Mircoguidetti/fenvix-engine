//! PROTOTYPE, off by default: the agent lane.
//!
//! The customer puts chosen collections (for example an agent framework's checkpoints) on the "agent lane". A write
//! batch whose data keys all belong to agent-lane collections is not sent to the local or replicated log (`locallog.rs`,
//! `qlog.rs`): it stays in the writer's memory (memtable and WAL buffer) and is acknowledged only once the WAL object
//! holding it is durable in object storage. The first write of a batch opens a window (`--agent-lane-window-ms`, default
//! 100 ms); at its end one WAL flush, one PUT, covers every write staged meanwhile. A transaction's commit flushes at once.
//! Everything else (other collections, the catalog, a transaction touching both lanes) keeps the default path.
//!
//! **Setting it (MongoDB wire compatible).** Either a collection option, from any driver:
//!   `db.runCommand({collMod: "checkpoints", fenvixAgentLane: true})`   (false takes it off the lane)
//!   `db.createCollection("checkpoints", {fenvixAgentLane: true})`
//! stored with the collection's options (`meta/copt/{collection}`) and listed by `listCollections`; or, for collections
//! a framework creates by itself, the server flag `--agent-lane "*.checkpoints,*.checkpoint_writes"` (patterns
//! `database.collection`, `*` matches any run of characters). Nothing is on the lane by default.
//!
//! **Why it is safe.** slatedb writes WAL objects with consecutive numbers and create-if-absent (`If-None-Match: *`): one
//! number is written by one writer only. A new writer (takeover) writes an empty fence object at the next number and
//! replays every WAL object before it, so it sees every acknowledged agent-lane write; the fenced writer's next WAL write
//! fails, so it can never acknowledge again. A WAL object holds every batch staged before the flush, of both lanes, in
//! order, so when an agent-lane batch is durable everything written before it is durable too (the other lane's batches
//! carry their log marker, so the restart never applies them twice). Object storage is multi-zone (S3 Standard), so a
//! confirmed write survives the loss of a zone and of this machine's disk.
//!
//! **Which batches.** Keys of one collection: `doc/`, `idx/`, `idc/`, `nat/` (data, index entries, natural order),
//! `meta/sstat/`, `meta/cstat/`, `meta/nat/`, `meta/cuuid/` (its counters and its uuid). Keys of no collection, kept with the batch: `meta/rw/` (retryable-write and transaction
//! outcomes). Any other key (the catalog, index definitions, options) sends the batch to the default path.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::OnceLock;

/// Option name in `create` / `collMod` and in the stored collection options.
pub const OPTION: &str = "fenvixAgentLane";

static PATTERNS: OnceLock<Vec<(String, String)>> = OnceLock::new();
/// Set once anything can be on the lane (a pattern given, or the option seen on a collection): until then the write
/// path does not look at keys at all.
static ACTIVE: AtomicBool = AtomicBool::new(false);
/// Batches sent on the agent lane, and batches that touched an agent-lane collection but went to the default path
/// (mixed with other collections or with the catalog).
pub static AGENT_BATCHES: AtomicU64 = AtomicU64::new(0);
pub static MIXED_BATCHES: AtomicU64 = AtomicU64::new(0);
/// Memtable flushes of databases with agent-lane writes, at the uploads of the local log (see [`take_dirty`]).
pub static MEMTABLE_FLUSHES: AtomicU64 = AtomicU64::new(0);
static DIRTY: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<String>>> = std::sync::LazyLock::new(Default::default);

/// A batch of `db` went on the lane.
pub fn note_write(db: &str) {
    let mut d = DIRTY.lock().unwrap();
    if !d.contains(db) {
        d.insert(db.to_string());
    }
}

/// The databases with agent-lane writes since the last call. The local log's uploader flushes their memtables into L0
/// SSTs: each lane batch is its own small WAL object (one per window), and a takeover replays every WAL object written
/// since the last memtable flush (a reader keeps each as its own table, so its reads get slow with thousands). With the
/// flush at every upload (`--local-log-upload-secs`) a takeover replays at most about one interval of them.
pub fn take_dirty() -> Vec<String> {
    DIRTY.lock().unwrap().drain().collect()
}

/// `--agent-lane` (comma-separated `db.collection` patterns).
pub fn configure(spec: &str, window_ms: u64) -> anyhow::Result<()> {
    let mut v = Vec::new();
    for p in spec.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let Some((d, c)) = p.split_once('.') else { anyhow::bail!("--agent-lane: '{p}' is not database.collection") };
        v.push((d.to_string(), c.to_string()));
    }
    if !v.is_empty() {
        ACTIVE.store(true, Relaxed);
    }
    let _ = PATTERNS.set(v);
    anyhow::ensure!((1..=10_000).contains(&window_ms), "--agent-lane-window-ms must be between 1 and 10000");
    tenant_bench::lifecycle::AGENT_WINDOW_US.store(window_ms * 1000, Relaxed);
    if std::env::var("MONGO_TEST_AGENT_ACK_EARLY").map_or(false, |v| v == "1") {
        eprintln!("TEST: agent-lane writes acknowledged before they are durable (negative control): never use this in production");
        tenant_bench::lifecycle::AGENT_ACK_EARLY.store(true, Relaxed);
    }
    Ok(())
}

pub fn active() -> bool {
    ACTIVE.load(Relaxed)
}

/// A collection option on the lane was seen (set or read back): from now on batches are classified.
pub fn note_option() {
    ACTIVE.store(true, Relaxed);
}

fn glob(p: &str, s: &str) -> bool {
    match p.split_once('*') {
        None => p == s,
        Some((head, rest)) => {
            let Some(s) = s.strip_prefix(head) else { return false };
            (0..=s.len()).filter(|i| s.is_char_boundary(*i)).any(|i| glob(rest, &s[i..]))
        }
    }
}

/// Whether the server flag puts `db.coll` on the lane.
pub fn flag_matches(db: &str, coll: &str) -> bool {
    PATTERNS.get().map_or(false, |v| v.iter().any(|(d, c)| glob(d, db) && glob(c, coll)))
}

pub enum KeyClass {
    /// A key of this collection (escaped name).
    Coll(Vec<u8>),
    /// A key of no collection that follows its batch (`meta/rw/`).
    Neutral,
    /// Any other key: the batch takes the default path.
    Other,
}

fn first_segment(rest: &[u8]) -> Vec<u8> {
    rest.split(|b| *b == b'/').next().unwrap_or_default().to_vec()
}

pub fn classify(key: &[u8]) -> KeyClass {
    for p in [&b"doc/"[..], b"idx/", b"idc/", b"nat/"] {
        if let Some(rest) = key.strip_prefix(p) {
            return KeyClass::Coll(first_segment(rest));
        }
    }
    for p in [&b"meta/sstat/"[..], b"meta/cstat/", b"meta/nat/", b"meta/cuuid/"] {
        if let Some(rest) = key.strip_prefix(p) {
            return KeyClass::Coll(rest.to_vec());
        }
    }
    if key.starts_with(b"meta/rw/") {
        return KeyClass::Neutral;
    }
    KeyClass::Other
}

/// The collection name from its escaped form (`layout::escape_collection`).
pub fn unescape(e: &[u8]) -> String {
    let s = String::from_utf8_lossy(e);
    s.replace("%2F", "/").replace("%25", "%")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_and_classify() {
        assert!(glob("*", "anything"));
        assert!(glob("check*", "checkpoints"));
        assert!(glob("*writes", "checkpoint_writes"));
        assert!(!glob("checkpoints", "checkpoint_writes"));
        assert!(matches!(classify(b"doc/a%2Fb/xyz"), KeyClass::Coll(c) if unescape(&c) == "a/b"));
        assert!(matches!(classify(b"idx/c/name/k"), KeyClass::Coll(c) if c == b"c"));
        assert!(matches!(classify(b"meta/sstat/c"), KeyClass::Coll(c) if c == b"c"));
        assert!(matches!(classify(b"meta/rw/s1"), KeyClass::Neutral));
        assert!(matches!(classify(b"nat/c/12345678"), KeyClass::Coll(c) if c == b"c"));
        assert!(matches!(classify(b"meta/cuuid/c"), KeyClass::Coll(c) if c == b"c"));
        assert!(matches!(classify(b"meta/collections"), KeyClass::Other));
        assert!(matches!(classify(b"meta/copt/c"), KeyClass::Other));
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// The warm spare follows the lane (`--agent-lane-follow db1,db2`).
//
// A spare that takes a database over opens it as a writer, and slatedb replays every WAL object written since the last
// memtable flush before the database serves (the fence, see above). On the agent lane these are many small objects (one
// per batch window), so read one by one at takeover they cost seconds. The spare instead follows them while the primary
// writes: for each followed database it asks object storage for the next WAL number (one GET; a GET of a missing object
// costs the price of a GET, not of a LIST) and keeps every object found in memory. At takeover slatedb's replay reads them
// from this memory. WAL objects never change once written (create-if-absent), so a kept copy is always the stored one.

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, Result as OsResult,
};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// WAL objects fetched by the follower, and reads of slatedb answered from them.
pub static FOLLOWED: AtomicU64 = AtomicU64::new(0);
pub static FOLLOW_HITS: AtomicU64 = AtomicU64::new(0);
/// The follower keeps at most this many bytes of WAL objects (oldest dropped first).
const FOLLOW_MAX_BYTES: usize = 512 << 20;
const FOLLOW_POLL: Duration = Duration::from_millis(50);

#[derive(Default)]
struct Kept {
    objs: HashMap<String, (Bytes, ObjectMeta)>,
    order: VecDeque<String>,
    bytes: usize,
}

pub struct FollowStore {
    inner: Arc<dyn ObjectStore>,
    kept: Arc<Mutex<Kept>>,
}

impl std::fmt::Debug for FollowStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FollowStore({})", self.inner)
    }
}

impl std::fmt::Display for FollowStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FollowStore({})", self.inner)
    }
}

fn wal_path(db: &str, id: u64) -> Path {
    Path::from(format!("{}/wal/{id:020}.sst", tenant_bench::lifecycle::tenant_path(db)))
}

fn keep(kept: &Mutex<Kept>, path: String, body: Bytes, meta: ObjectMeta) {
    let mut k = kept.lock().unwrap();
    if k.objs.contains_key(&path) {
        return;
    }
    k.bytes += body.len();
    k.order.push_back(path.clone());
    k.objs.insert(path, (body, meta));
    while k.bytes > FOLLOW_MAX_BYTES {
        let Some(old) = k.order.pop_front() else { break };
        if let Some((b, _)) = k.objs.remove(&old) {
            k.bytes -= b.len();
        }
    }
}

/// Wraps the store with the follower when `--agent-lane-follow` names databases; otherwise returns it unchanged.
pub fn wrap_follow(inner: Arc<dyn ObjectStore>, dbs: &str) -> Arc<dyn ObjectStore> {
    let dbs: Vec<String> = dbs.split(',').map(str::trim).filter(|d| !d.is_empty()).map(String::from).collect();
    if dbs.is_empty() {
        return inner;
    }
    let kept = Arc::new(Mutex::new(Kept::default()));
    for db in dbs {
        let (inner, kept) = (Arc::clone(&inner), Arc::clone(&kept));
        tokio::spawn(async move {
            // start after the newest WAL object listed now (a new database starts at 1)
            let prefix = Path::from(format!("{}/wal/", tenant_bench::lifecycle::tenant_path(&db)));
            let mut next = 1u64;
            let listed: Vec<ObjectMeta> = inner.list(Some(&prefix)).filter_map(|m| async move { m.ok() }).collect().await;
            for m in &listed {
                if let Some(id) = m.location.filename().and_then(|f| f.strip_suffix(".sst")).and_then(|f| f.parse::<u64>().ok()) {
                    next = next.max(id + 1);
                }
            }
            eprintln!("agent lane: following the WAL of {db} from number {next}");
            loop {
                let p = wal_path(&db, next);
                match inner.get_opts(&p, GetOptions::default()).await {
                    Ok(r) => {
                        let meta = r.meta.clone();
                        match r.bytes().await {
                            Ok(body) => {
                                keep(&kept, p.to_string(), body, meta);
                                FOLLOWED.fetch_add(1, Relaxed);
                                next += 1;
                                continue;
                            }
                            Err(_) => {}
                        }
                    }
                    Err(object_store::Error::NotFound { .. }) => {
                        // a newer writer may have started further on (its fence after a gap of numbers never written
                        // cannot happen: numbers are consecutive); just wait for the next object
                    }
                    Err(_) => {}
                }
                tokio::time::sleep(FOLLOW_POLL).await;
            }
        });
    }
    Arc::new(FollowStore { inner, kept })
}

#[async_trait::async_trait]
impl ObjectStore for FollowStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        let plain = options.if_match.is_none() && options.if_none_match.is_none() && options.if_modified_since.is_none() && options.if_unmodified_since.is_none() && options.version.is_none();
        let hit = if plain { self.kept.lock().unwrap().objs.get(location.as_ref()).cloned() } else { None };
        let Some((body, meta)) = hit else { return self.inner.get_opts(location, options).await };
        let len = body.len() as u64;
        let range = match &options.range {
            None => 0..len,
            Some(GetRange::Bounded(r)) => r.start.min(len)..r.end.min(len),
            Some(GetRange::Offset(o)) => (*o).min(len)..len,
            Some(GetRange::Suffix(n)) => len.saturating_sub(*n)..len,
        };
        FOLLOW_HITS.fetch_add(1, Relaxed);
        let part = if options.head { Bytes::new() } else { body.slice(range.start as usize..range.end as usize) };
        let stream = futures::stream::once(async move { Ok(part) }).boxed();
        Ok(GetResult { payload: GetResultPayload::Stream(stream), meta, range, attributes: Default::default(), extensions: Default::default() })
    }

    fn delete_stream(&self, locations: BoxStream<'static, OsResult<Path>>) -> BoxStream<'static, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> OsResult<()> {
        self.inner.rename_opts(from, to, options).await
    }
}
