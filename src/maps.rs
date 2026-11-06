//! The maps on the counter (T1, "le mappe sul bancone"): for every SST of every open database, the waiter keeps in its
//! counter ([`crate::counter`]) the SST index, the filters and the stats (the "stack maps"). Then a cold point read asks
//! S3 only for the block that holds the key.
//!
//! Founder's choice (b), 2026-11-06: only the stack maps are maps. The blocks that hold the collections' indexes
//! (`idx/`) and metadata are data like the documents: they live on S3 and come to the counter when a read needs them.
//! The first read of a customer never seen therefore waits for two trips one after the other (its collection-index
//! block, then its documents), and for one once that index block is on the counter.
//!
//! How (only slatedb's public API): a loop looks at the manifest of every open database every `MAPS_EVERY_MS`. For
//! every SST it has not mapped yet it calls `warm_sst(Index)`, `warm_sst(Filters)` and `warm_sst(Stats)`. SSTs that
//! left the manifest (compaction) are forgotten by the counter.
//!
//! Loading is capped (R-T1, correction 3): at most `Counter::warm_ssts_cap` SSTs are warmed at once over all databases,
//! and every map load waits for one of `Counter::warm_reqs_cap` permits before it goes to the object store
//! (`counter::warming`). A database much larger than the caps is warmed a few SSTs at a time; the bytes held while
//! loading are those of the SSTs in flight, not of the whole database.
//!
//! Leaving (R-T1, correction 1):
//! - [`forget_database`] at `dropDatabase`: every map and data block of the database leaves the RAM and the disk floor
//!   at once, and its bytes on the disk floor are overwritten;
//! - a database closed for `MAPS_IDLE_SECS` has its maps released ([`release_idle`]): they become ordinary data on the
//!   disk floor and leave like it. If it opens again its maps are warmed again (from the disk floor while they are still
//!   there, otherwise from S3).

use crate::counter::{sst_of, Counter};
use futures::StreamExt;
use slatedb::manifest::SsTableHandle;
use slatedb::{CacheTarget, DbCacheManagerOps};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Keep the maps on the counter (on with the counter).
pub static MAPS_ON: AtomicBool = AtomicBool::new(false);
/// How often the manifests of the open databases are looked at.
pub static MAPS_EVERY_MS: AtomicU64 = AtomicU64::new(500);
/// A database closed for this long has its maps released (`--maps-idle-secs`).
pub static MAPS_IDLE_SECS: AtomicU64 = AtomicU64::new(600);
/// SSTs warmed, warm calls failed, refreshes that warmed something, ms of the last one.
pub static MAPS_SSTS: AtomicU64 = AtomicU64::new(0);
pub static MAPS_FAILS: AtomicU64 = AtomicU64::new(0);
pub static MAPS_RUNS: AtomicU64 = AtomicU64::new(0);
pub static MAPS_LAST_MS: AtomicU64 = AtomicU64::new(0);
/// Warm-ups running now (a test or the bench waits for 0).
pub static MAPS_BUSY: AtomicU64 = AtomicU64::new(0);
/// Databases whose maps were released (closed for long), and databases forgotten at a drop.
pub static MAPS_RELEASED_DBS: AtomicU64 = AtomicU64::new(0);
pub static MAPS_DROPPED_DBS: AtomicU64 = AtomicU64::new(0);

/// What the maps loop knows of one database (kept in `Counter::mapped`, one registry per counter).
pub struct Mapped {
    manifest: u64,
    /// SST -> its estimated size in bytes.
    ssts: HashMap<[u8; 26], u64>,
    /// Since when the database is not open (None while open).
    closed_since: Option<Instant>,
}

/// (SSTs mapped, their bytes) over all databases: the denominator of "maps per GB of data".
pub fn mapped_totals(counter: &Counter) -> (u64, u64) {
    let m = counter.mapped.lock().unwrap();
    let mut n = 0;
    let mut b = 0;
    for v in m.values() {
        n += v.ssts.len() as u64;
        b += v.ssts.values().sum::<u64>();
    }
    (n, b)
}

/// The SSTs mapped for one database.
pub fn mapped_ssts(counter: &Counter, id: &str) -> HashSet<[u8; 26]> {
    counter.mapped.lock().unwrap().get(id).map(|e| e.ssts.keys().copied().collect()).unwrap_or_default()
}

/// Start the loop.
pub fn start(counter: Arc<Counter>) {
    MAPS_ON.store(true, Relaxed);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(MAPS_EVERY_MS.load(Relaxed).max(20))).await;
            let mut open: HashSet<String> = HashSet::new();
            for (id, db) in crate::lifecycle::open_writers() {
                open.insert(id.clone());
                refresh(&counter, &id, Src::W(db));
            }
            for (id, r) in crate::lifecycle::open_readers() {
                if open.insert(id.clone()) {
                    refresh(&counter, &id, Src::R(r));
                }
            }
            release_idle(&counter, &open, Duration::from_secs(MAPS_IDLE_SECS.load(Relaxed)));
        }
    });
}

/// An open database: a writer, or a reader (a woken tenant not written yet).
#[derive(Clone)]
pub enum Src {
    W(slatedb::Db),
    R(Arc<slatedb::DbReader>),
}

impl Src {
    fn manifest(&self) -> slatedb::VersionedManifest {
        match self {
            Src::W(d) => d.manifest(),
            Src::R(r) => r.manifest(),
        }
    }
    async fn warm_sst(&self, id: slatedb::manifest::SsTableId, t: &[CacheTarget]) -> Result<(), slatedb::Error> {
        match self {
            Src::W(d) => d.warm_sst(id, t).await,
            Src::R(r) => r.warm_sst(id, t).await,
        }
    }
}

/// Look at one database's manifest; warm what is new (in the background: the handle says when it is done), forget what
/// left.
pub fn refresh(counter: &Arc<Counter>, id: &str, db: Src) -> Option<tokio::task::JoinHandle<()>> {
    let m = db.manifest();
    let mid = m.id();
    let mut now: HashMap<[u8; 26], SsTableHandle> = HashMap::new();
    for v in m.all_sst_views() {
        if let Some(s) = sst_of(&v.sst.id) {
            now.entry(s).or_insert_with(|| v.sst.clone());
        }
    }
    let (new, gone) = {
        let mut mapped = counter.mapped.lock().unwrap();
        let e = mapped.entry(id.to_string()).or_insert_with(|| Mapped { manifest: u64::MAX, ssts: HashMap::new(), closed_since: None });
        e.closed_since = None;
        if e.manifest == mid {
            return None;
        }
        e.manifest = mid;
        let new: Vec<SsTableHandle> = now.iter().filter(|(s, _)| !e.ssts.contains_key(*s)).map(|(_, h)| h.clone()).collect();
        let gone: HashSet<[u8; 26]> = e.ssts.keys().filter(|s| !now.contains_key(*s)).copied().collect();
        for s in &gone {
            e.ssts.remove(s);
        }
        for (s, h) in &now {
            e.ssts.entry(*s).or_insert_with(|| h.estimate_size());
        }
        (new, gone)
    };
    counter.forget_ssts(&gone);
    if new.is_empty() {
        return None;
    }
    let counter = Arc::clone(counter);
    let id = id.to_string();
    MAPS_BUSY.fetch_add(1, Relaxed);
    Some(tokio::spawn(async move {
        let t0 = Instant::now();
        let n = new.len();
        let cap = counter.warm_ssts_cap;
        // At most `cap` SSTs of this database at once, and at most `cap` over all databases (the semaphore).
        let results: Vec<Result<(), String>> = futures::stream::iter(new.into_iter().map(|h| {
            let (db, counter) = (db.clone(), Arc::clone(&counter));
            async move {
                let _permit = counter.warm_ssts.acquire().await.map_err(|e| e.to_string())?;
                crate::counter::warming(warm_one(&db, h)).await.map_err(|e| format!("{e:#}"))
            }
        }))
        .buffer_unordered(cap)
        .collect()
        .await;
        let mut failed = 0;
        for r in results {
            if let Err(e) = r {
                MAPS_FAILS.fetch_add(1, Relaxed);
                if failed < 3 {
                    eprintln!("maps of {id}: {e}");
                }
                failed += 1;
            }
        }
        if failed > 0 {
            // warm again at the next look
            if let Some(e) = counter.mapped.lock().unwrap().get_mut(&id) {
                e.manifest = u64::MAX;
                e.ssts.clear();
            }
        }
        MAPS_RUNS.fetch_add(1, Relaxed);
        MAPS_SSTS.fetch_add(n as u64, Relaxed);
        MAPS_LAST_MS.store(t0.elapsed().as_millis() as u64, Relaxed);
        MAPS_BUSY.fetch_sub(1, Relaxed);
    }))
}

async fn warm_one(db: &Src, h: SsTableHandle) -> Result<(), slatedb::Error> {
    // the three parts at once (warm_sst reads its targets one after the other)
    let (a, b, c) = futures::join!(db.warm_sst(h.id, &[CacheTarget::Index]), db.warm_sst(h.id, &[CacheTarget::Filters]), db.warm_sst(h.id, &[CacheTarget::Stats]));
    a?;
    b?;
    c
}

/// Release the maps of the databases not open (not in `open`) for `idle` or longer: they become ordinary data on the
/// disk floor (`Counter::release_ssts`). Returns the databases released.
pub fn release_idle(counter: &Counter, open: &HashSet<String>, idle: Duration) -> usize {
    let now = Instant::now();
    let mut out: HashSet<[u8; 26]> = HashSet::new();
    let mut dbs = 0;
    {
        let mut mapped = counter.mapped.lock().unwrap();
        mapped.retain(|id, e| {
            if open.contains(id) {
                e.closed_since = None;
                return true;
            }
            let since = *e.closed_since.get_or_insert(now);
            if now.duration_since(since) < idle {
                return true;
            }
            out.extend(e.ssts.keys().copied());
            dbs += 1;
            false
        });
    }
    if dbs > 0 {
        counter.release_ssts(&out);
        MAPS_RELEASED_DBS.fetch_add(dbs as u64, Relaxed);
    }
    dbs
}

/// The database was dropped: forget at once everything the counter holds of its SSTs (the ones mapped, plus `more`, for
/// example the SSTs found under its prefix in the store). Returns the counter entries forgotten.
pub fn forget_database(counter: &Counter, id: &str, more: impl IntoIterator<Item = [u8; 26]>) -> u64 {
    let mut ssts: HashSet<[u8; 26]> = counter.mapped.lock().unwrap().remove(id).map(|e| e.ssts.into_keys().collect()).unwrap_or_default();
    ssts.extend(more);
    MAPS_DROPPED_DBS.fetch_add(1, Relaxed);
    counter.drop_ssts(&ssts)
}

/// The counter key of the SST stored at `path` (`.../compacted/<ULID>.sst`), if it is one. The key is the ULID's
/// 26-character text (see `counter::canon`).
pub fn sst_of_path(path: &str) -> Option<[u8; 26]> {
    if !path.contains("/compacted/") {
        return None;
    }
    let stem = path.rsplit('/').next()?.strip_suffix(".sst")?;
    let b = stem.as_bytes();
    if b.len() != 26 || !b.iter().all(|c| c.is_ascii_digit() || c.is_ascii_uppercase()) {
        return None;
    }
    let mut k = [0u8; 26];
    k.copy_from_slice(b);
    Some(k)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::path::Path;
    use object_store::{CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions};
    use slatedb::config::{FlushOptions, FlushType, Settings};
    use slatedb::object_store::memory::InMemory;
    use slatedb::Db;
    use std::fmt;
    use std::sync::Mutex;

    /// A store that counts its GETs: in flight now and at most, distinct SSTs in flight at most, and trips (how many
    /// times the GETs in flight went from 0 to 1). Every GET waits `delay` first, like a request to S3.
    struct Probe {
        inner: Arc<dyn ObjectStore>,
        delay: Duration,
        on: AtomicBool,
        st: Mutex<ProbeState>,
    }

    #[derive(Default)]
    struct ProbeState {
        inflight: u64,
        max: u64,
        trips: u64,
        gets: u64,
        paths: HashMap<String, u64>,
        max_paths: u64,
    }

    impl Probe {
        fn new(inner: Arc<dyn ObjectStore>, delay_ms: u64) -> Arc<Self> {
            Arc::new(Probe { inner, delay: Duration::from_millis(delay_ms), on: AtomicBool::new(false), st: Mutex::new(ProbeState::default()) })
        }
        fn reset(&self) {
            *self.st.lock().unwrap() = ProbeState::default();
            self.on.store(true, Relaxed);
        }
        /// (most GETs in flight, most distinct SSTs in flight, trips, GETs)
        fn read(&self) -> (u64, u64, u64, u64) {
            let s = self.st.lock().unwrap();
            (s.max, s.max_paths, s.trips, s.gets)
        }
    }

    impl fmt::Debug for Probe {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Probe")
        }
    }
    impl fmt::Display for Probe {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Probe")
        }
    }

    #[async_trait]
    impl ObjectStore for Probe {
        async fn put_opts(&self, l: &Path, p: PutPayload, o: PutOptions) -> object_store::Result<PutResult> {
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(&self, l: &Path, o: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(l, o).await
        }
        async fn get_opts(&self, l: &Path, o: GetOptions) -> object_store::Result<GetResult> {
            // only the SSTs: the manifest polls of the open database are not reads of a query
            if !self.on.load(Relaxed) || !l.as_ref().contains("/compacted/") {
                return self.inner.get_opts(l, o).await;
            }
            let key = l.to_string();
            {
                let mut s = self.st.lock().unwrap();
                if s.inflight == 0 {
                    s.trips += 1;
                }
                s.inflight += 1;
                s.gets += 1;
                s.max = s.max.max(s.inflight);
                *s.paths.entry(key.clone()).or_default() += 1;
                s.max_paths = s.max_paths.max(s.paths.len() as u64);
            }
            tokio::time::sleep(self.delay).await;
            let r = self.inner.get_opts(l, o).await;
            {
                // (a GET started before a reset is not in the new counts)
                let mut s = self.st.lock().unwrap();
                s.inflight = s.inflight.saturating_sub(1);
                if let Some(n) = s.paths.get_mut(&key) {
                    *n -= 1;
                    if *n == 0 {
                        s.paths.remove(&key);
                    }
                }
            }
            r
        }
        fn delete_stream(&self, l: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(l)
        }
        fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> object_store::Result<()> {
            self.inner.copy_opts(f, t, o).await
        }
        async fn rename_opts(&self, f: &Path, t: &Path, o: RenameOptions) -> object_store::Result<()> {
            self.inner.rename_opts(f, t, o).await
        }
    }

    fn settings() -> Settings {
        let mut s = Settings::default();
        s.compactor_options = None;
        s.l0_max_ssts = 10_000;
        s.l0_max_ssts_per_key = 10_000;
        s
    }

    /// Write `batches` L0 SSTs (one memtable flush each) without any cache, then close.
    async fn write(store: &Arc<dyn ObjectStore>, path: &str, batches: Vec<Vec<(String, Vec<u8>)>>) {
        let db = Db::builder(path, Arc::clone(store)).with_settings(settings()).with_db_cache_disabled().build().await.unwrap();
        for b in batches {
            for (k, v) in b {
                db.put(k, v).await.unwrap();
            }
            db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await.unwrap();
        }
        db.close().await.unwrap();
    }

    async fn open(store: &Arc<dyn ObjectStore>, path: &str, c: &Arc<Counter>) -> Db {
        Db::builder(path, Arc::clone(store)).with_settings(settings()).with_db_cache(c.split_cache(), 1).build().await.unwrap()
    }

    async fn ssts_under(store: &Arc<dyn ObjectStore>, path: &str) -> HashSet<[u8; 26]> {
        let objs: Vec<ObjectMeta> = store.list(Some(&Path::from(path))).collect::<Vec<_>>().await.into_iter().map(|r| r.unwrap()).collect();
        objs.iter().filter_map(|m| sst_of_path(m.location.as_ref())).collect()
    }

    /// The scan options of the server's index finds (`mongo-server/src/dbs.rs` scan_range, `coll.rs` INDEX_READ_AHEAD).
    fn server_scan() -> slatedb::config::ScanOptions {
        slatedb::config::ScanOptions { read_ahead_bytes: 64 << 10, max_fetch_tasks: 4, cache_blocks: true, ..Default::default() }
    }

    fn count_in_dir(dir: &std::path::Path, needle: &[u8]) -> usize {
        let mut n = 0;
        for e in std::fs::read_dir(dir).unwrap() {
            let b = std::fs::read(e.unwrap().path()).unwrap();
            n += b.windows(needle.len()).filter(|w| *w == needle).count();
        }
        n
    }

    /// A customer database: `docs` documents per customer (1 KB each, `doc/...`) and one collection-index entry per
    /// document (`idx/...`, the document's key as value), every key and value carrying `mark`.
    fn customer_batches(mark: &str, customers: u32, docs: u32, batches: u32) -> Vec<Vec<(String, Vec<u8>)>> {
        let per = customers.div_ceil(batches);
        (0..batches)
            .map(|b| {
                let mut v = Vec::new();
                for c in (b * per)..((b + 1) * per).min(customers) {
                    for d in 0..docs {
                        let doc = format!("doc/c{c:05}/{d:03}");
                        v.push((doc.clone(), format!("{mark}{}", "x".repeat(1000)).into_bytes()));
                        v.push((format!("idx/by_customer/{mark}/c{c:05}/{d:03}"), doc.into_bytes()));
                    }
                }
                v
            })
            .collect()
    }

    /// R-T1, correction 1: after dropDatabase the dropped database's maps and blocks leave the RAM and the disk floor at
    /// once, none of its index keys stays on the disk floor (bytes overwritten), a load still in flight cannot bring them
    /// back, and the other database keeps everything.
    #[tokio::test]
    async fn drop_forgets_the_database_everywhere() {
        let dir = std::env::temp_dir().join(format!("maps-drop-{}", std::process::id()));
        // 96 KiB of RAM, 16 MiB of disk floor: most of what is read goes down to the disk floor
        let c = Counter::new(96 << 10, Some(dir.clone()), 16 << 20).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        write(&store, "a", customer_batches("SECRETAAA", 40, 10, 3)).await;
        write(&store, "b", customer_batches("KEEPBBB", 40, 10, 3)).await;
        let (da, db) = (open(&store, "a", &c).await, open(&store, "b", &c).await);
        refresh(&c, "a", Src::W(da.clone())).unwrap().await.unwrap();
        refresh(&c, "b", Src::W(db.clone())).unwrap().await.unwrap();
        for d in [&da, &db] {
            let mut it = d.scan_with_options("idx/".to_string().., &server_scan()).await.unwrap();
            while it.next().await.unwrap().is_some() {}
            for cu in (0..40).step_by(3) {
                assert!(d.get(format!("doc/c{cu:05}/004")).await.unwrap().is_some());
            }
        }
        let a_ssts = ssts_under(&store, "a").await;
        assert_eq!(a_ssts, mapped_ssts(&c, "a"), "the SSTs found under the prefix are the ones mapped (sst_of_path = sst_of)");
        let (am, ar, ad) = c.entries_of(&a_ssts);
        let before = count_in_dir(&dir, b"SECRETAAA");
        eprintln!("before the drop: a has {am} maps in RAM, {ar} data blocks in RAM, {ad} entries on disk; 'SECRETAAA' {before} times on disk");
        assert!(am + ar > 0 && ad > 0 && before > 0, "the test must start with the database on both floors");
        let b_ssts = ssts_under(&store, "b").await;
        let b_before = (c.entries_of(&b_ssts), count_in_dir(&dir, b"KEEPBBB"));

        let t0 = Instant::now();
        let n = forget_database(&c, "a", ssts_under(&store, "a").await);
        let took = t0.elapsed();
        let after = c.entries_of(&a_ssts);
        let left = count_in_dir(&dir, b"SECRETAAA");
        eprintln!("drop: {n} entries forgotten in {took:?}; after: {after:?}; 'SECRETAAA' {left} times on disk");
        assert_eq!(after, (0, 0, 0), "entries of the dropped database left on the counter");
        assert_eq!(left, 0, "index keys of the dropped database left on the disk floor");
        assert!(mapped_ssts(&c, "a").is_empty());
        assert!(took < Duration::from_secs(2));
        // a load in flight when the drop happened (here: the same database warmed and read again) cannot bring them back
        c.mapped.lock().unwrap().remove("a");
        if let Some(h) = refresh(&c, "a", Src::W(da.clone())) {
            h.await.unwrap();
        }
        let _ = da.get("doc/c00001/001").await.unwrap();
        assert_eq!(c.entries_of(&a_ssts), (0, 0, 0), "a load after the drop brought the database back");
        assert!(c.m.dead_refused.load(Relaxed) > 0);
        // the other database is untouched
        let b_after = (c.entries_of(&b_ssts), count_in_dir(&dir, b"KEEPBBB"));
        eprintln!("the other database: before {b_before:?}, after {b_after:?}");
        assert!(b_before.0 .0 + b_before.0 .1 + b_before.0 .2 > 0);
        assert_eq!(b_before, b_after, "the drop touched the other database");
        for cu in 0..40 {
            assert!(db.get(format!("doc/c{cu:05}/004")).await.unwrap().is_some());
        }
        da.close().await.unwrap();
        db.close().await.unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    /// R-T1, correction 1: the maps of a database closed for long leave the RAM (they become ordinary data on the disk
    /// floor), then leave the disk floor too as other data comes; a database still open keeps its maps.
    #[tokio::test]
    async fn maps_of_a_long_closed_database_leave_ram_then_disk() {
        let dir = std::env::temp_dir().join(format!("maps-idle-{}", std::process::id()));
        let c = Counter::new(8 << 20, Some(dir.clone()), 4 << 20).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        write(&store, "idle_x", customer_batches("XXX", 30, 10, 4)).await;
        write(&store, "open_y", customer_batches("YYY", 30, 10, 4)).await;
        write(&store, "big_z", customer_batches("ZZZ", 1500, 10, 3)).await;
        let (dx, dy) = (open(&store, "idle_x", &c).await, open(&store, "open_y", &c).await);
        refresh(&c, "idle_x", Src::W(dx.clone())).unwrap().await.unwrap();
        refresh(&c, "open_y", Src::W(dy.clone())).unwrap().await.unwrap();
        let (x, y) = (mapped_ssts(&c, "idle_x"), mapped_ssts(&c, "open_y"));
        assert!(c.entries_of(&x).0 > 0 && c.entries_of(&y).0 > 0);
        dx.close().await.unwrap();
        let open_set: HashSet<String> = ["open_y".to_string()].into();
        // closed, but not for long yet: kept
        assert_eq!(release_idle(&c, &open_set, Duration::from_millis(300)), 0);
        assert!(c.entries_of(&x).0 > 0);
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(release_idle(&c, &open_set, Duration::from_millis(300)), 1);
        let (xm, _, xd) = c.entries_of(&x);
        eprintln!("released: idle_x maps in RAM {xm}, entries on disk {xd}; open_y maps in RAM {}", c.entries_of(&y).0);
        assert_eq!(xm, 0, "the maps of the long-closed database stayed in RAM");
        assert!(c.entries_of(&y).0 > 0, "the open database lost its maps");
        assert!(mapped_ssts(&c, "idle_x").is_empty());
        // other data comes through the disk floor (a scan of a larger database): the released maps leave it
        let dz = open(&store, "big_z", &c).await;
        let mut it = dz.scan_with_options("doc/".to_string().., &server_scan()).await.unwrap();
        while it.next().await.unwrap().is_some() {}
        let (xm, xr, xd) = c.entries_of(&x);
        eprintln!("after a scan of 15 MB: idle_x entries {xm} {xr} {xd}, maps released {}", c.m.maps_released.load(Relaxed));
        assert_eq!((xm, xr, xd), (0, 0, 0), "the released maps did not leave the disk floor");
        assert!(c.entries_of(&y).0 > 0, "the open database lost its maps");
        dy.close().await.unwrap();
        dz.close().await.unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    /// R-T1, correction 3: loading the maps of a database much larger than the caps keeps at most `ssts` SSTs and `reqs`
    /// requests in flight, and still loads every map.
    #[tokio::test]
    async fn loading_maps_is_capped() {
        let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let probe = Probe::new(Arc::clone(&mem), 3);
        let store: Arc<dyn ObjectStore> = probe.clone();
        // 150 SSTs
        let batches: Vec<Vec<(String, Vec<u8>)>> = (0..150u32).map(|b| (0..20u32).map(|i| (format!("doc/b{b:04}/{i:03}"), vec![1u8; 200])).collect()).collect();
        write(&store, "cap", batches).await;
        let (ssts_cap, reqs_cap) = (4usize, 6usize);
        let c = Counter::with_caps(64 << 20, None, 0, ssts_cap, reqs_cap).unwrap();
        let db = open(&store, "cap", &c).await;
        let n_ssts = db.manifest().all_sst_views().count();
        assert!(n_ssts >= 150, "{n_ssts} SSTs");
        probe.reset();
        let t0 = Instant::now();
        refresh(&c, "cap", Src::W(db.clone())).unwrap().await.unwrap();
        let (max, max_ssts, _, gets) = probe.read();
        let all = mapped_ssts(&c, "cap");
        let (maps, _, _) = c.entries_of(&all);
        eprintln!(
            "{n_ssts} SSTs warmed in {:?}: {gets} GETs, at most {max} in flight (cap {reqs_cap}) on at most {max_ssts} SSTs (cap {ssts_cap}); {maps} maps; counter max {}",
            t0.elapsed(),
            c.m.warm_max_inflight.load(Relaxed)
        );
        assert!(max <= reqs_cap as u64, "requests in flight {max} over the cap {reqs_cap}");
        assert!(max_ssts <= ssts_cap as u64, "SSTs in flight {max_ssts} over the cap {ssts_cap}");
        assert!(max >= 2, "loading did not run in parallel at all");
        assert!(c.m.warm_max_inflight.load(Relaxed) <= reqs_cap as u64);
        assert!(maps >= 2 * n_ssts as u64, "not every map was loaded: {maps} entries for {n_ssts} SSTs");
        // every point read now needs only its data block: one GET each
        probe.reset();
        for b in (0..150u32).step_by(10) {
            db.get(format!("doc/b{b:04}/007")).await.unwrap().unwrap();
        }
        let (_, _, trips, gets) = probe.read();
        assert_eq!((trips, gets), (15, 15), "a point read asked for more than its data block");
        db.close().await.unwrap();
    }

    /// Choice (b): with only the stack maps on the counter, the first cold read of a customer never seen (its index
    /// entries, then its documents) waits for at most 2 trips one after the other, and for 1 once its collection-index
    /// block is on the counter.
    #[tokio::test]
    async fn first_cold_read_two_trips_then_one() {
        let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let probe = Probe::new(Arc::clone(&mem), 5);
        // the server's store layers above S3 (`mongo-server/src/main.rs`): large GETs in pieces and the next block read
        // along (`--next-block-kb`, 8 by default with the waiter's tools: slatedb reads the block before a key that
        // starts a block first, T1). The probe is where S3 would be.
        crate::simstore::NEXT_BLOCK_BYTES.store(8 << 10, Relaxed);
        let store: Arc<dyn ObjectStore> = crate::simstore::wrap_split(probe.clone());
        write(&store, "shop", customer_batches("S", 2000, 10, 4)).await;
        let c = Counter::new(64 << 20, None, 0).unwrap();
        let db = open(&store, "shop", &c).await;
        refresh(&c, "shop", Src::W(db.clone())).unwrap().await.unwrap();
        let maps_misses = c.m.maps_misses.load(Relaxed);
        // "the last 10 documents of customer X": its index entries, then its documents in parallel
        async fn query(db: &Db, cu: u32) -> usize {
            let mut it = db.scan_with_options(format!("idx/by_customer/S/c{cu:05}/")..format!("idx/by_customer/S/c{cu:05}0"), &server_scan()).await.unwrap();
            let mut keys = Vec::new();
            while let Some(kv) = it.next().await.unwrap() {
                keys.push(String::from_utf8(kv.value.to_vec()).unwrap());
            }
            let docs = futures::future::join_all(keys.iter().map(|k| db.get(k.clone()))).await;
            docs.into_iter().filter(|d| matches!(d, Ok(Some(_)))).count()
        }
        let mut cold = Vec::new();
        for cu in [17u32, 523, 1046, 1999] {
            probe.reset();
            assert_eq!(query(&db, cu).await, 10);
            cold.push(probe.read().2);
        }
        // a customer next to one already read: its index entries are in a block on the counter
        let mut warm_idx = Vec::new();
        for cu in [18u32, 524, 1045] {
            probe.reset();
            assert_eq!(query(&db, cu).await, 10);
            warm_idx.push(probe.read().2);
        }
        eprintln!("trips: customer never seen {cold:?}; index block on the counter {warm_idx:?}");
        assert!(cold.iter().all(|&t| t <= 2), "a first cold read took more than 2 trips: {cold:?}");
        assert!(warm_idx.iter().all(|&t| t <= 1), "a read with its index block cached took more than 1 trip: {warm_idx:?}");
        assert!(cold.iter().any(|&t| t == 2), "the collection-index block was not read from the store (is it still a map?)");
        assert_eq!(c.m.maps_misses.load(Relaxed), maps_misses, "a read had to load a stack map");
        db.close().await.unwrap();
    }
}
