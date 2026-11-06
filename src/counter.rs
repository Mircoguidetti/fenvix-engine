//! The waiter's two-floor counter (T1): one cache for every database of the process, in RAM and on the local disk
//! (NVMe), sized from the machine, that holds two kinds of things.
//!
//! **The maps** (kitchen: "le mappe sul bancone"): the index, the filters and the stats of every SST of the open
//! databases (the "stack maps", see [`crate::maps`]). Founder's choice (b), 2026-11-06: only these are maps. The blocks
//! of the collections' indexes (`idx/`) and metadata are data like the documents: they live on S3 and come to the
//! counter on demand, under the exit rule below.
//! Maps are kept before data, but within a share of the RAM: when the RAM is full and the maps take more than
//! [`MAPS_RAM_PCT`] percent of it, the coldest maps move down to the disk floor (never to S3), so the active data always
//! keeps at least the rest of the RAM, whatever the maps weigh (reviewer R-T1, correction 2). When the RAM is not full
//! the maps may use more. Maps leave the counter when their SST leaves the manifest (compaction), when their database
//! is dropped ([`Counter::drop_ssts`]: gone at once, their bytes on the disk floor overwritten), when their database has
//! been closed for long ([`Counter::release_ssts`]: they become ordinary data on the disk floor and leave like it), or
//! when even the disk floor is full (counted in `maps_dropped`).
//!
//! **The data blocks**, under an exit rule that protects what comes back (scan resistant, like 2Q / S3-FIFO):
//! - a new block enters *probation*; a block read ahead ("preso in anticipo", [`PREFETCH`]) enters the *prefetched*
//!   queue instead and is the first to leave if nobody reads it;
//! - a block read again while in probation is marked; a prefetched block read once moves to probation, unmarked (it has
//!   been read once, like a new block); when probation is full its oldest block moves to the *protected* LRU if marked,
//!   otherwise down to the disk floor;
//! - the protected LRU (at most `PROTECTED_SHARE` of the data in RAM) sends its least recently used block down to the
//!   disk floor;
//! - a block found on the disk floor comes back to RAM protected (it was read twice).
//!   A one-off scan of a large collection therefore goes through probation and to disk without touching the blocks of the
//!   clients that come back often.
//!
//! **Loading maps is capped** (R-T1, correction 3): [`crate::maps`] warms at most `warm_ssts` SSTs at once over all
//! databases, and the map loads it starts ([`warming`]) wait for one of `warm_reqs` permits before they go to the
//! object store (and are not split in pieces, `simstore.rs`): one load is one request.
//!
//! Keys: an SST is named by a ULID that is never reused and its bytes never change, so a cached entry is valid for every
//! reader of that SST. The counter keys entries by (SST, offset) and ignores slatedb's per-open cache scope: the maps
//! and blocks of a database survive its close and reopen, and readers of the same SST share them. (In slatedb 0.17 the
//! SST cache holds no WAL entries: `db_cache/serde.rs` refuses them.)
//!
//! The disk floor starts empty at every process start (no persistence yet).

use async_trait::async_trait;
use slatedb::db_cache::{CacheFetch, CacheLoader, CacheLookup, CachedEntry, CachedKey, DbCache};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

tokio::task_local! {
    /// Set around reads that fetch ahead of a request (`counter::prefetching`): their blocks enter the prefetched queue.
    pub static PREFETCH: bool;
    /// Set around the loading of maps (`counter::warming`): their loads are capped by the counter's `warm_reqs`.
    pub static WARMING: bool;
}

/// Run `f` as a load of maps: every map it loads from the object store first takes one of the counter's `warm_reqs`
/// permits, and large ranges are not split (`simstore::SplitGetStore`), so the requests in flight stay under the cap.
pub async fn warming<F: std::future::Future>(f: F) -> F::Output {
    WARMING.scope(true, f).await
}

/// Inside [`warming`].
pub fn is_warming() -> bool {
    WARMING.try_with(|w| *w).unwrap_or(false)
}

/// Most of the RAM floor the maps keep when the RAM is full (percent; the data keeps the rest). `--counter-maps-ram-pct`.
pub static MAPS_RAM_PCT: AtomicU64 = AtomicU64::new(25);
/// SSTs whose maps are loaded at once, over all databases (`--maps-ssts-in-flight`), read when a counter is made.
pub static WARM_SSTS_IN_FLIGHT: AtomicU64 = AtomicU64::new(16);
/// Map loads (object-store requests) in flight at once, over all databases (`--maps-requests-in-flight`).
pub static WARM_REQS_IN_FLIGHT: AtomicU64 = AtomicU64::new(32);
/// How long a forgotten SST stays refused (a load that was in flight when it was forgotten must not bring it back).
const DEAD_FOR: std::time::Duration = std::time::Duration::from_secs(600);

/// Run `f` as a read-ahead: the data blocks it brings into the counter are marked as taken ahead.
pub async fn prefetching<F: std::future::Future>(f: F) -> F::Output {
    PREFETCH.scope(true, f).await
}

fn is_prefetch() -> bool {
    PREFETCH.try_with(|p| *p).unwrap_or(false)
}

/// Share of the RAM data floor the protected LRU may take.
const PROTECTED_SHARE: f64 = 0.75;
/// Size of one segment file of the disk floor (1 MiB in tests, so a test can fill a small disk floor).
const SEGMENT_BYTES: u64 = if cfg!(test) { 1 << 20 } else { 64 << 20 };

/// An SST id (the ULID as slatedb serializes it) and an offset in that SST.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct Ck {
    pub sst: [u8; 26],
    pub off: u64,
}

/// The scope-free key of a slatedb cache key (bincode of `SerializedCachedKey::V3(scope, ulid, offset)`: a 4-byte
/// variant, the 8-byte scope, the ULID as a length-prefixed 26-char string, the 8-byte offset).
pub fn canon(key: &CachedKey) -> Option<Ck> {
    let b = bincode::serialize(key).ok()?;
    // 4 (variant) + 8 (scope) + 8 (string length) + 26 (ULID) + 8 (offset)
    if b.len() != 54 || u32::from_le_bytes(b[0..4].try_into().ok()?) != 2 || u64::from_le_bytes(b[12..20].try_into().ok()?) != 26 {
        return None;
    }
    let mut sst = [0u8; 26];
    sst.copy_from_slice(&b[20..46]);
    Some(Ck { sst, off: u64::from_le_bytes(b[46..54].try_into().ok()?) })
}

/// The SST part of the counter key of an SST id.
pub fn sst_of(id: &slatedb::manifest::SsTableId) -> Option<[u8; 26]> {
    canon(&CachedKey::from((*id, 0))).map(|c| c.sst)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Q {
    Prefetched,
    Probation,
    Protected,
}

struct Slot {
    entry: CachedEntry,
    size: u64,
    q: Q,
    tick: u64,
    marked: bool,
}

#[derive(Default)]
struct Ram {
    slots: HashMap<Ck, Slot>,
    prefetched: VecDeque<(Ck, u64)>,
    probation: VecDeque<(Ck, u64)>,
    protected: BTreeMap<u64, Ck>,
    tick: u64,
    used: u64,
    protected_used: u64,
}

struct MapSlot {
    entry: CachedEntry,
    size: u64,
    tick: u64,
}

#[derive(Default)]
struct Maps {
    slots: HashMap<Ck, MapSlot>,
    lru: BTreeMap<u64, Ck>,
    tick: u64,
    used: u64,
}

impl Maps {
    fn take(&mut self, k: &Ck) -> Option<MapSlot> {
        let s = self.slots.remove(k)?;
        self.used -= s.size;
        self.lru.remove(&s.tick);
        Some(s)
    }
}

#[derive(Clone, Copy)]
struct Loc {
    seg: u64,
    off: u64,
    len: u32,
    map: bool,
    hit: bool,
}

struct Disk {
    dir: std::path::PathBuf,
    cap_segments: u64,
    index: HashMap<Ck, Loc>,
    /// Segment number -> (file, every copy written in it: key, offset, length). A copy stays listed after its key moved
    /// on (read back to RAM, written again): its bytes are still in the file until the segment goes, and a drop must
    /// overwrite them too.
    segments: BTreeMap<u64, (std::fs::File, Vec<(Ck, u64, u32)>)>,
    cur: u64,
    pos: u64,
    used: u64,
    maps_used: u64,
}

/// Meters of the counter (serverStatus `fenvix.counter`).
#[derive(Default)]
pub struct CounterMeters {
    pub hits_ram: AtomicU64,
    pub hits_disk: AtomicU64,
    pub misses: AtomicU64,
    pub maps_hits: AtomicU64,
    pub maps_misses: AtomicU64,
    pub loaded_bytes: AtomicU64,
    pub inserted_bytes: AtomicU64,
    pub prefetched_in: AtomicU64,
    pub prefetched_used: AtomicU64,
    pub prefetched_evicted_unused: AtomicU64,
    pub promoted: AtomicU64,
    pub to_disk: AtomicU64,
    pub disk_dropped: AtomicU64,
    pub maps_to_disk: AtomicU64,
    pub maps_dropped: AtomicU64,
    pub maps_forgotten: AtomicU64,
    /// Map entries of long-closed databases moved down to the disk floor as ordinary data.
    pub maps_released: AtomicU64,
    /// Entries (maps and data, RAM and disk) of dropped databases forgotten, and bytes overwritten on the disk floor.
    pub dropped_entries: AtomicU64,
    pub dropped_disk_bytes: AtomicU64,
    /// Inserts refused because their SST was forgotten (a load in flight when its database was dropped or compacted).
    pub dead_refused: AtomicU64,
    /// Map loads in flight now, and the most at once.
    pub warm_inflight: AtomicU64,
    pub warm_max_inflight: AtomicU64,
}

/// The two-floor counter. Use [`Counter::split_cache`] to get the cache slatedb takes.
pub struct Counter {
    ram_bytes: u64,
    ram: Mutex<Ram>,
    maps: Mutex<Maps>,
    disk: Option<Mutex<Disk>>,
    disk_bytes: u64,
    /// The maps loop's registry of the databases it mapped (`maps.rs`).
    pub(crate) mapped: Mutex<crate::maps::Registry>,
    /// SSTs forgotten in the last `DEAD_FOR`: nothing of them enters the counter again.
    dead: Mutex<HashMap<[u8; 26], std::time::Instant>>,
    /// Caps on the loading of maps (see the module comment).
    pub warm_ssts: tokio::sync::Semaphore,
    pub warm_ssts_cap: usize,
    pub warm_reqs: tokio::sync::Semaphore,
    pub warm_reqs_cap: usize,
    inflight: Mutex<HashMap<Ck, Arc<tokio::sync::OnceCell<Option<CachedEntry>>>>>,
    pub m: CounterMeters,
}

/// Bytes of RAM the machine (or its cgroup) gives the process.
pub fn machine_ram_bytes() -> u64 {
    let total = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("MemTotal:")).and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok()))
        .map(|kb| kb * 1024)
        .unwrap_or(4 << 30);
    let cg = std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok().and_then(|s| s.trim().parse::<u64>().ok());
    cg.map_or(total, |c| c.min(total))
}

/// Free bytes of the file system of `dir`.
pub fn free_disk_bytes(dir: &std::path::Path) -> Option<u64> {
    let c = std::ffi::CString::new(dir.as_os_str().as_encoded_bytes()).ok()?;
    let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut s) } != 0 {
        return None;
    }
    Some(s.f_bavail as u64 * s.f_frsize as u64)
}

impl Counter {
    /// A counter with `ram_bytes` of RAM and, when `disk_dir` is given, `disk_bytes` on that directory (emptied first).
    /// The caps on loading maps come from [`WARM_SSTS_IN_FLIGHT`] and [`WARM_REQS_IN_FLIGHT`].
    pub fn new(ram_bytes: u64, disk_dir: Option<std::path::PathBuf>, disk_bytes: u64) -> std::io::Result<Arc<Self>> {
        let (s, r) = (WARM_SSTS_IN_FLIGHT.load(Relaxed), WARM_REQS_IN_FLIGHT.load(Relaxed));
        Self::with_caps(ram_bytes, disk_dir, disk_bytes, s as usize, r as usize)
    }

    /// [`Counter::new`] with explicit caps: at most `ssts` SSTs warmed at once and `reqs` map loads in flight.
    pub fn with_caps(ram_bytes: u64, disk_dir: Option<std::path::PathBuf>, disk_bytes: u64, ssts: usize, reqs: usize) -> std::io::Result<Arc<Self>> {
        let (ssts, reqs) = (ssts.max(1), reqs.max(1));
        let disk = match disk_dir {
            Some(dir) if disk_bytes >= 2 * SEGMENT_BYTES => {
                std::fs::create_dir_all(&dir)?;
                for e in std::fs::read_dir(&dir)? {
                    let p = e?.path();
                    if p.extension().map(|x| x == "seg").unwrap_or(false) {
                        std::fs::remove_file(p)?;
                    }
                }
                let mut d = Disk { dir, cap_segments: disk_bytes / SEGMENT_BYTES, index: HashMap::new(), segments: BTreeMap::new(), cur: 0, pos: 0, used: 0, maps_used: 0 };
                d.open_segment(0)?;
                Some(Mutex::new(d))
            }
            _ => None,
        };
        let disk_bytes = if disk.is_some() { disk_bytes } else { 0 };
        Ok(Arc::new(Counter {
            ram_bytes,
            ram: Mutex::new(Ram::default()),
            maps: Mutex::new(Maps::default()),
            disk,
            disk_bytes,
            mapped: Mutex::new(Default::default()),
            dead: Mutex::new(HashMap::new()),
            warm_ssts: tokio::sync::Semaphore::new(ssts),
            warm_ssts_cap: ssts,
            warm_reqs: tokio::sync::Semaphore::new(reqs),
            warm_reqs_cap: reqs,
            inflight: Mutex::new(HashMap::new()),
            m: CounterMeters::default(),
        }))
    }

    /// slatedb's cache: index, filters and stats go to the maps, data blocks (documents, collection indexes, metadata)
    /// to the data floors.
    pub fn split_cache(self: &Arc<Self>) -> Arc<dyn DbCache> {
        let block: Arc<dyn DbCache> = Arc::new(BlockView(Arc::clone(self), false));
        let meta: Arc<dyn DbCache> = Arc::new(MetaView(Arc::clone(self)));
        Arc::new(slatedb::db_cache::SplitCache::new().with_block_cache(Some(block)).with_meta_cache(Some(meta)).build())
    }

    /// The same cache for the read-ahead reader (`prefetch.rs`): the data blocks it loads are marked as taken ahead.
    /// (A task-local cannot do it: slatedb reads blocks in tasks it spawns.)
    pub fn prefetch_cache(self: &Arc<Self>) -> Arc<dyn DbCache> {
        let block: Arc<dyn DbCache> = Arc::new(BlockView(Arc::clone(self), true));
        let meta: Arc<dyn DbCache> = Arc::new(MetaView(Arc::clone(self)));
        Arc::new(slatedb::db_cache::SplitCache::new().with_block_cache(Some(block)).with_meta_cache(Some(meta)).build())
    }

    pub fn ram_capacity(&self) -> u64 {
        self.ram_bytes
    }
    pub fn disk_capacity(&self) -> u64 {
        self.disk_bytes
    }
    /// (maps bytes in RAM, map entries in RAM, data bytes in RAM, data blocks in RAM, of which protected bytes)
    pub fn ram_usage(&self) -> (u64, u64, u64, u64, u64) {
        let (mu, mn) = {
            let m = self.maps.lock().unwrap();
            (m.used, m.slots.len() as u64)
        };
        let r = self.ram.lock().unwrap();
        (mu, mn, r.used, r.slots.len() as u64, r.protected_used)
    }
    /// (bytes on the disk floor, of which maps, entries)
    pub fn disk_usage(&self) -> (u64, u64, u64) {
        match &self.disk {
            Some(d) => {
                let d = d.lock().unwrap();
                (d.used, d.maps_used, d.index.len() as u64)
            }
            None => (0, 0, 0),
        }
    }

    fn is_dead(&self, sst: &[u8; 26]) -> bool {
        let d = self.dead.lock().unwrap();
        !d.is_empty() && d.contains_key(sst)
    }

    fn bury(&self, ssts: &HashSet<[u8; 26]>) {
        let now = std::time::Instant::now();
        let mut d = self.dead.lock().unwrap();
        d.retain(|_, t| now.duration_since(*t) < DEAD_FOR);
        for s in ssts {
            d.insert(*s, now);
        }
    }

    /// Forget everything of these SSTs (they left the manifest): maps, data blocks, disk entries.
    pub fn forget_ssts(&self, ssts: &HashSet<[u8; 26]>) {
        self.forget(ssts, false);
    }

    /// Forget everything of these SSTs at once, because their database was dropped: maps and data, in RAM and on the
    /// disk floor, and the bytes they took on the disk floor are overwritten with zeros (the keys of a dropped database
    /// must not stay on the waiter). Loads of these SSTs still in flight are refused for a while. Returns the entries
    /// forgotten.
    pub fn drop_ssts(&self, ssts: &HashSet<[u8; 26]>) -> u64 {
        let n = self.forget(ssts, true);
        self.m.dropped_entries.fetch_add(n, Relaxed);
        n
    }

    fn forget(&self, ssts: &HashSet<[u8; 26]>, scrub: bool) -> u64 {
        if ssts.is_empty() {
            return 0;
        }
        // only a drop refuses later loads: SSTs that left a manifest (compaction) may still be read by an older
        // snapshot, and caching them then is harmless (they leave like data nobody reads)
        if scrub {
            self.bury(ssts);
        }
        let mut n = 0;
        {
            let mut m = self.maps.lock().unwrap();
            let gone: Vec<Ck> = m.slots.keys().filter(|k| ssts.contains(&k.sst)).copied().collect();
            for k in gone {
                if m.take(&k).is_some() {
                    self.m.maps_forgotten.fetch_add(1, Relaxed);
                    n += 1;
                }
            }
        }
        {
            let mut r = self.ram.lock().unwrap();
            let gone: Vec<Ck> = r.slots.keys().filter(|k| ssts.contains(&k.sst)).copied().collect();
            for k in gone {
                r.remove(&k);
                n += 1;
            }
        }
        if let Some(d) = &self.disk {
            let mut d = d.lock().unwrap();
            let gone: Vec<Ck> = d.index.keys().filter(|k| ssts.contains(&k.sst)).copied().collect();
            for k in gone {
                if let Some(l) = d.forget(&k) {
                    n += 1;
                    if l.map {
                        self.m.maps_forgotten.fetch_add(1, Relaxed);
                    }
                }
            }
            if scrub {
                // every copy still in a segment file, the indexed ones and the ones left behind when a block went back
                // to RAM or was written again
                for (f, recs) in d.segments.values_mut() {
                    recs.retain(|(k, off, len)| {
                        if !ssts.contains(&k.sst) {
                            return true;
                        }
                        if f.write_all_at(&vec![0u8; *len as usize], *off).is_ok() {
                            self.m.dropped_disk_bytes.fetch_add(*len as u64, Relaxed);
                        }
                        false
                    });
                }
            }
        }
        n
    }

    /// The maps of these SSTs (a database closed for long) stop being maps: the ones in RAM move down to the disk floor
    /// as ordinary data, the ones on the disk floor become ordinary data there. They then leave like data nobody reads
    /// (the disk floor drops them when their segment is the oldest), unless the database opens again and reads them
    /// first. Returns the map entries released.
    pub fn release_ssts(&self, ssts: &HashSet<[u8; 26]>) -> u64 {
        if ssts.is_empty() {
            return 0;
        }
        let moved: Vec<(Ck, CachedEntry)> = {
            let mut m = self.maps.lock().unwrap();
            let ks: Vec<Ck> = m.slots.keys().filter(|k| ssts.contains(&k.sst)).copied().collect();
            ks.into_iter().filter_map(|k| m.take(&k).map(|s| (k, s.entry))).collect()
        };
        let mut n = moved.len() as u64;
        for (k, e) in moved {
            self.disk_put(k, &e, false);
        }
        if let Some(d) = &self.disk {
            let mut d = d.lock().unwrap();
            let ks: Vec<Ck> = d.index.iter().filter(|(k, l)| l.map && ssts.contains(&k.sst)).map(|(k, _)| *k).collect();
            for k in ks {
                let len = {
                    let l = d.index.get_mut(&k).unwrap();
                    l.map = false;
                    l.hit = false;
                    l.len as u64
                };
                d.maps_used -= len;
                n += 1;
            }
        }
        self.m.maps_released.fetch_add(n, Relaxed);
        n
    }

    /// (map entries in RAM, data entries in RAM, entries on the disk floor) of these SSTs.
    pub fn entries_of(&self, ssts: &HashSet<[u8; 26]>) -> (u64, u64, u64) {
        let m = self.maps.lock().unwrap().slots.keys().filter(|k| ssts.contains(&k.sst)).count() as u64;
        let r = self.ram.lock().unwrap().slots.keys().filter(|k| ssts.contains(&k.sst)).count() as u64;
        let d = self.disk.as_ref().map(|d| d.lock().unwrap().index.keys().filter(|k| ssts.contains(&k.sst)).count() as u64).unwrap_or(0);
        (m, r, d)
    }

    // ---------------------------------------------------------------- maps

    fn maps_get(&self, k: &Ck) -> Option<CachedEntry> {
        {
            let mut m = self.maps.lock().unwrap();
            m.tick += 1;
            let t = m.tick;
            if let Some(s) = m.slots.get_mut(k) {
                let old = s.tick;
                s.tick = t;
                let e = s.entry.clone();
                m.lru.remove(&old);
                m.lru.insert(t, *k);
                self.m.maps_hits.fetch_add(1, Relaxed);
                return Some(e);
            }
        }
        // on the disk floor: back to RAM
        let e = self.disk_get(k)?;
        self.m.maps_hits.fetch_add(1, Relaxed);
        self.maps_put(*k, e.clone());
        Some(e)
    }

    fn maps_put(&self, k: Ck, e: CachedEntry) {
        let size = e.size() as u64 + 64;
        {
            let mut m = self.maps.lock().unwrap();
            // checked under the lock: a drop marks its SSTs dead before it takes this lock, so a put either sees the
            // mark or is in place before the drop clears it
            if self.is_dead(&k.sst) {
                self.m.dead_refused.fetch_add(1, Relaxed);
                return;
            }
            m.take(&k);
            m.tick += 1;
            let t = m.tick;
            m.slots.insert(k, MapSlot { entry: e, size, tick: t });
            m.used += size;
            m.lru.insert(t, k);
        }
        if let Some(d) = &self.disk {
            d.lock().unwrap().forget(&k);
        }
        self.balance();
    }

    fn maps_remove(&self, k: &Ck) {
        self.maps.lock().unwrap().take(k);
    }

    // ---------------------------------------------------------------- data

    fn data_get(&self, k: &Ck) -> Option<CachedEntry> {
        {
            let mut r = self.ram.lock().unwrap();
            if let Some(e) = r.hit(k, &self.m) {
                self.m.hits_ram.fetch_add(1, Relaxed);
                return Some(e);
            }
        }
        let e = self.disk_get(k)?;
        self.m.hits_disk.fetch_add(1, Relaxed);
        // read twice: back to RAM, protected (unless its database was dropped meanwhile)
        {
            let mut r = self.ram.lock().unwrap();
            if self.is_dead(&k.sst) {
                self.m.dead_refused.fetch_add(1, Relaxed);
                return Some(e);
            }
            r.insert(*k, e.clone(), Q::Protected);
        }
        if let Some(d) = &self.disk {
            d.lock().unwrap().forget(k);
        }
        self.balance();
        Some(e)
    }

    fn data_put(&self, k: Ck, e: CachedEntry, ahead: bool) {
        let q = if ahead || is_prefetch() {
            self.m.prefetched_in.fetch_add(1, Relaxed);
            Q::Prefetched
        } else {
            Q::Probation
        };
        {
            let mut r = self.ram.lock().unwrap();
            // under the lock, as in maps_put
            if self.is_dead(&k.sst) {
                self.m.dead_refused.fetch_add(1, Relaxed);
                return;
            }
            if r.slots.contains_key(&k) {
                return; // already here (a concurrent read): keep its queue
            }
            r.insert(k, e, q);
        }
        if let Some(d) = &self.disk {
            d.lock().unwrap().forget(&k);
        }
        self.balance();
    }

    /// Most bytes of maps the RAM keeps when it is full.
    fn maps_ram_cap(&self) -> u64 {
        self.ram_bytes / 100 * MAPS_RAM_PCT.load(Relaxed).min(100)
    }

    /// Keep RAM within its size. When maps and data together are over it: if the maps take more than their share
    /// ([`MAPS_RAM_PCT`]), the coldest maps go down to the disk floor; otherwise data leaves (prefetched unused, then
    /// probation, then protected). So the data always keeps at least the rest of the RAM (R-T1, correction 2).
    fn balance(&self) {
        let maps_cap = self.maps_ram_cap();
        loop {
            let maps_used = self.maps.lock().unwrap().used;
            let data_used = self.ram.lock().unwrap().used;
            if maps_used + data_used <= self.ram_bytes {
                return;
            }
            let mut out: Vec<(Ck, CachedEntry, bool)> = Vec::new();
            if maps_used > maps_cap {
                let mut m = self.maps.lock().unwrap();
                while m.used > maps_cap && m.used + data_used > self.ram_bytes && out.len() < 64 {
                    let Some((_, &k)) = m.lru.iter().next() else { break };
                    if let Some(s) = m.take(&k) {
                        out.push((k, s.entry, true));
                    }
                }
            } else {
                let mut r = self.ram.lock().unwrap();
                let budget = self.ram_bytes.saturating_sub(maps_used);
                let protected_cap = (budget as f64 * PROTECTED_SHARE) as u64;
                while r.used > budget && out.len() < 64 {
                    match r.evict_one(protected_cap, &self.m) {
                        Some((k, e)) => out.push((k, e, false)),
                        None => break,
                    }
                }
            }
            if out.is_empty() {
                return;
            }
            for (k, e, map) in out {
                if map {
                    self.m.maps_to_disk.fetch_add(1, Relaxed);
                }
                self.disk_put(k, &e, map);
            }
        }
    }

    // ---------------------------------------------------------------- disk floor

    fn disk_get(&self, k: &Ck) -> Option<CachedEntry> {
        let d = self.disk.as_ref()?;
        let (file, loc) = {
            let mut d = d.lock().unwrap();
            let loc = *d.index.get(k)?;
            if let Some(l) = d.index.get_mut(k) {
                l.hit = true;
            }
            (d.segments.get(&loc.seg)?.0.try_clone().ok()?, loc)
        };
        let mut buf = vec![0u8; loc.len as usize];
        file.read_exact_at(&mut buf, loc.off).ok()?;
        bincode::deserialize::<CachedEntry>(&buf).ok()
    }

    fn disk_put(&self, k: Ck, e: &CachedEntry, map: bool) {
        let Some(d) = &self.disk else {
            if map {
                self.m.maps_dropped.fetch_add(1, Relaxed);
            } else {
                self.m.disk_dropped.fetch_add(1, Relaxed);
            }
            return;
        };
        let Ok(buf) = bincode::serialize(e) else { return };
        let mut d = d.lock().unwrap();
        // an entry on its way down (taken out of RAM before a drop) must not land on the disk floor after the drop
        if self.is_dead(&k.sst) {
            self.m.dead_refused.fetch_add(1, Relaxed);
            return;
        }
        if !map {
            self.m.to_disk.fetch_add(1, Relaxed);
        }
        if let Err(err) = d.append(k, &buf, map, &self.m) {
            eprintln!("counter disk floor: {err}");
        }
    }
}

impl Ram {
    fn insert(&mut self, k: Ck, entry: CachedEntry, q: Q) {
        self.remove(&k);
        self.tick += 1;
        let t = self.tick;
        let size = entry.size() as u64 + 64;
        match q {
            Q::Prefetched => self.prefetched.push_back((k, t)),
            Q::Probation => self.probation.push_back((k, t)),
            Q::Protected => {
                self.protected.insert(t, k);
                self.protected_used += size;
            }
        }
        self.used += size;
        self.slots.insert(k, Slot { entry, size, q, tick: t, marked: false });
    }

    fn remove(&mut self, k: &Ck) {
        if let Some(s) = self.slots.remove(k) {
            self.used -= s.size;
            if s.q == Q::Protected {
                self.protected.remove(&s.tick);
                self.protected_used -= s.size;
            }
            // queue entries are dropped lazily (their tick no longer matches)
        }
    }

    fn hit(&mut self, k: &Ck, m: &CounterMeters) -> Option<CachedEntry> {
        self.tick += 1;
        let t = self.tick;
        let s = self.slots.get_mut(k)?;
        let e = s.entry.clone();
        match s.q {
            Q::Prefetched => {
                // read once after being taken ahead: a normal block, already read once
                m.prefetched_used.fetch_add(1, Relaxed);
                s.q = Q::Probation;
                s.tick = t;
                s.marked = false;
                self.probation.push_back((*k, t));
            }
            Q::Probation => s.marked = true,
            Q::Protected => {
                let old = s.tick;
                s.tick = t;
                self.protected.remove(&old);
                self.protected.insert(t, *k);
            }
        }
        Some(e)
    }

    /// One block out of RAM (to the disk floor), or one block moved from probation to protected (then `None` is not
    /// returned: the loop goes on). `None` when RAM holds no data.
    fn evict_one(&mut self, protected_cap: u64, m: &CounterMeters) -> Option<(Ck, CachedEntry)> {
        loop {
            // 1. taken ahead and never read
            while let Some((k, t)) = self.prefetched.pop_front() {
                if self.slots.get(&k).map(|s| s.tick == t && s.q == Q::Prefetched).unwrap_or(false) {
                    let s = self.slots.remove(&k).unwrap();
                    self.used -= s.size;
                    m.prefetched_evicted_unused.fetch_add(1, Relaxed);
                    return Some((k, s.entry));
                }
            }
            // 2. probation: marked blocks go to protected, the others leave
            while let Some((k, t)) = self.probation.pop_front() {
                let Some(s) = self.slots.get_mut(&k) else { continue };
                if s.tick != t || s.q != Q::Probation {
                    continue;
                }
                if s.marked {
                    self.tick += 1;
                    let nt = self.tick;
                    s.q = Q::Protected;
                    s.tick = nt;
                    s.marked = false;
                    let size = s.size;
                    self.protected.insert(nt, k);
                    self.protected_used += size;
                    m.promoted.fetch_add(1, Relaxed);
                    if self.protected_used > protected_cap {
                        return self.evict_protected();
                    }
                    continue;
                }
                let s = self.slots.remove(&k).unwrap();
                self.used -= s.size;
                return Some((k, s.entry));
            }
            // 3. protected LRU
            return self.evict_protected();
        }
    }

    fn evict_protected(&mut self) -> Option<(Ck, CachedEntry)> {
        let (&t, &k) = self.protected.iter().next()?;
        self.protected.remove(&t);
        let s = self.slots.remove(&k)?;
        self.used -= s.size;
        self.protected_used -= s.size;
        Some((k, s.entry))
    }
}

impl Disk {
    fn open_segment(&mut self, n: u64) -> std::io::Result<()> {
        let path = self.dir.join(format!("{n:010}.seg"));
        let f = std::fs::OpenOptions::new().create(true).truncate(true).read(true).write(true).open(path)?;
        self.segments.insert(n, (f, Vec::new()));
        self.cur = n;
        self.pos = 0;
        Ok(())
    }

    fn forget(&mut self, k: &Ck) -> Option<Loc> {
        let l = self.index.remove(k)?;
        self.used -= l.len as u64;
        if l.map {
            self.maps_used -= l.len as u64;
        }
        Some(l)
    }

    fn append(&mut self, k: Ck, buf: &[u8], map: bool, m: &CounterMeters) -> std::io::Result<()> {
        self.forget(&k);
        if buf.len() as u64 > SEGMENT_BYTES {
            return Ok(()); // larger than a segment: not kept
        }
        if self.pos + buf.len() as u64 > SEGMENT_BYTES {
            self.roll(m)?;
        }
        let seg = self.cur;
        let off = self.pos;
        self.segments.get(&seg).unwrap().0.write_all_at(buf, off)?;
        self.segments.get_mut(&seg).unwrap().1.push((k, off, buf.len() as u32));
        self.pos += buf.len() as u64;
        self.index.insert(k, Loc { seg, off, len: buf.len() as u32, map, hit: false });
        self.used += buf.len() as u64;
        if map {
            self.maps_used += buf.len() as u64;
        }
        Ok(())
    }

    /// Next segment; when the floor is full the oldest segment is emptied: its maps are written again (pinned, while
    /// they take less than half of the floor), the data read since it was written gets a second chance while the
    /// floor has room, the rest leaves.
    fn roll(&mut self, m: &CounterMeters) -> std::io::Result<()> {
        let next = self.cur + 1;
        self.open_segment(next)?;
        while self.segments.len() as u64 > self.cap_segments {
            let (&old, _) = self.segments.iter().next().unwrap();
            let (file, keys) = self.segments.remove(&old).unwrap();
            let path = self.dir.join(format!("{old:010}.seg"));
            let mut keep: Vec<(Ck, Vec<u8>, bool)> = Vec::new();
            for (k, _, _) in keys {
                let Some(l) = self.index.get(&k).copied() else { continue };
                if l.seg != old {
                    continue;
                }
                self.forget(&k);
                let second = l.map && self.maps_used < self.cap_segments * SEGMENT_BYTES / 2 || !l.map && l.hit;
                if second {
                    let mut b = vec![0u8; l.len as usize];
                    if file.read_exact_at(&mut b, l.off).is_ok() {
                        keep.push((k, b, l.map));
                        continue;
                    }
                }
                if l.map {
                    m.maps_dropped.fetch_add(1, Relaxed);
                } else {
                    m.disk_dropped.fetch_add(1, Relaxed);
                }
            }
            drop(file);
            let _ = std::fs::remove_file(path);
            for (k, b, map) in keep {
                if self.pos + b.len() as u64 > SEGMENT_BYTES {
                    // the new segment is full of second chances already: they leave
                    if map {
                        m.maps_dropped.fetch_add(1, Relaxed);
                    } else {
                        m.disk_dropped.fetch_add(1, Relaxed);
                    }
                    continue;
                }
                let seg = self.cur;
                let off = self.pos;
                self.segments.get(&seg).unwrap().0.write_all_at(&b, off)?;
                self.segments.get_mut(&seg).unwrap().1.push((k, off, b.len() as u32));
                self.pos += b.len() as u64;
                self.index.insert(k, Loc { seg, off, len: b.len() as u32, map, hit: false });
                self.used += b.len() as u64;
                if map {
                    self.maps_used += b.len() as u64;
                }
            }
        }
        Ok(())
    }
}

/// One loader per key at a time: concurrent misses of the same entry wait for the first one (one GET, not many).
async fn dedup(c: &Counter, k: Ck, loader: CacheLoader, map: bool, ahead: bool) -> Result<CacheFetch, slatedb::Error> {
    let cell = {
        let mut inf = c.inflight.lock().unwrap();
        Arc::clone(inf.entry(k).or_insert_with(|| Arc::new(tokio::sync::OnceCell::new())))
    };
    let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let err: Arc<Mutex<Option<slatedb::Error>>> = Arc::new(Mutex::new(None));
    let (ran2, err2) = (Arc::clone(&ran), Arc::clone(&err));
    let got = cell
        .get_or_init(move || async move {
            ran2.store(true, Relaxed);
            match loader().await {
                Ok(e) => Some(e),
                Err(e) => {
                    *err2.lock().unwrap() = Some(e);
                    None
                }
            }
        })
        .await
        .clone();
    let ran = ran.load(Relaxed);
    if ran {
        c.inflight.lock().unwrap().remove(&k);
    }
    let err = err.lock().unwrap().take();
    match (got, err) {
        (Some(e), _) => {
            if ran {
                c.m.loaded_bytes.fetch_add(e.size() as u64, Relaxed);
                if map {
                    c.m.maps_misses.fetch_add(1, Relaxed);
                    c.maps_put(k, e.clone());
                } else {
                    c.m.misses.fetch_add(1, Relaxed);
                    c.data_put(k, e.clone(), ahead);
                }
                Ok(CacheFetch::miss(e))
            } else {
                Ok(CacheFetch::hit(e))
            }
        }
        (None, Some(e)) => Err(e),
        (None, None) => Err(slatedb::Error::unavailable("counter: a concurrent load of the same block failed".to_string())),
    }
}

/// The maps half (index, filters, stats).
struct MetaView(Arc<Counter>);
/// The data half (data blocks: documents, collection indexes, metadata). `ahead`: the view of the read-ahead reader, whose blocks
/// enter the prefetched queue.
struct BlockView(Arc<Counter>, bool);

#[async_trait]
impl DbCache for MetaView {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(canon(key).and_then(|k| self.0.maps_get(&k)))
    }
    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        let r = canon(key).and_then(|k| self.0.maps_get(&k));
        if r.is_none() {
            self.0.m.maps_misses.fetch_add(1, Relaxed);
        }
        Ok(r)
    }
    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.get_index(key).await
    }
    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.get_index(key).await
    }
    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        if let Some(k) = canon(&key) {
            self.0.m.inserted_bytes.fetch_add(value.size() as u64, Relaxed);
            self.0.maps_put(k, value);
        }
    }
    async fn remove(&self, key: &CachedKey) {
        if let Some(k) = canon(key) {
            self.0.maps_remove(&k);
            if let Some(d) = &self.0.disk {
                d.lock().unwrap().forget(&k);
            }
        }
    }
    fn entry_count(&self) -> u64 {
        self.0.maps.lock().unwrap().slots.len() as u64
    }
    async fn fetch_index(&self, key: CachedKey, loader: CacheLoader) -> Result<CacheFetch, slatedb::Error> {
        self.fetch(key, loader).await
    }
    async fn fetch_filter(&self, key: CachedKey, loader: CacheLoader) -> Result<CacheFetch, slatedb::Error> {
        self.fetch(key, loader).await
    }
    async fn fetch_stats(&self, key: CachedKey, loader: CacheLoader) -> Result<CacheFetch, slatedb::Error> {
        self.fetch(key, loader).await
    }
    async fn fetch_block(&self, key: CachedKey, loader: CacheLoader) -> Result<CacheFetch, slatedb::Error> {
        self.fetch(key, loader).await
    }
}

impl MetaView {
    async fn fetch(&self, key: CachedKey, loader: CacheLoader) -> Result<CacheFetch, slatedb::Error> {
        let Some(k) = canon(&key) else {
            return Ok(CacheFetch::miss(loader().await?));
        };
        if let Some(e) = self.0.maps_get(&k) {
            return Ok(CacheFetch::hit(e));
        }
        let loader = if is_warming() { gated(Arc::clone(&self.0), loader) } else { loader };
        dedup(&self.0, k, loader, true, false).await
    }
}

/// A map load inside [`warming`]: it waits for one of the counter's `warm_reqs` permits before it goes to the store.
fn gated(c: Arc<Counter>, loader: CacheLoader) -> CacheLoader {
    Box::new(move || {
        Box::pin(async move {
            let _permit = c.warm_reqs.acquire().await.map_err(|_| slatedb::Error::unavailable("counter: map loads closed".to_string()))?;
            let n = c.m.warm_inflight.fetch_add(1, Relaxed) + 1;
            c.m.warm_max_inflight.fetch_max(n, Relaxed);
            let r = loader().await;
            c.m.warm_inflight.fetch_sub(1, Relaxed);
            r
        })
    })
}

#[async_trait]
impl DbCache for BlockView {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        let Some(k) = canon(key) else { return Ok(None) };
        let r = self.0.data_get(&k);
        if r.is_none() {
            self.0.m.misses.fetch_add(1, Relaxed);
        }
        Ok(r)
    }
    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.get_block(key).await
    }
    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.get_block(key).await
    }
    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.get_block(key).await
    }
    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        let Some(k) = canon(&key) else { return };
        self.0.m.inserted_bytes.fetch_add(value.size() as u64, Relaxed);
        self.0.data_put(k, value, self.1);
    }
    async fn remove(&self, key: &CachedKey) {
        if let Some(k) = canon(key) {
            self.0.maps_remove(&k);
            self.0.ram.lock().unwrap().remove(&k);
            if let Some(d) = &self.0.disk {
                d.lock().unwrap().forget(&k);
            }
        }
    }
    fn entry_count(&self) -> u64 {
        self.0.ram.lock().unwrap().slots.len() as u64
    }
    async fn fetch_block(&self, key: CachedKey, loader: CacheLoader) -> Result<CacheFetch, slatedb::Error> {
        let Some(k) = canon(&key) else {
            return Ok(CacheFetch::miss(loader().await?));
        };
        if let Some(e) = self.0.data_get(&k) {
            return Ok(CacheFetch { entry: e, lookup: CacheLookup::Hit });
        }
        dedup(&self.0, k, loader, false, self.1).await
    }
}

/// The counter of the process, when installed (`lifecycle::install_counter`).
pub static COUNTER: std::sync::OnceLock<Arc<Counter>> = std::sync::OnceLock::new();

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::ObjectStore;
    use slatedb::Db;

    #[test]
    fn canon_ignores_the_scope() {
        let id = slatedb::manifest::SsTableId::from(ulid::Ulid::new());
        let a = CachedKey::from((id, 4096));
        let mut v = serde_json::to_value(&a).unwrap();
        v["V3"][0] = 77.into();
        let b: CachedKey = serde_json::from_value(v).unwrap();
        assert_ne!(a, b);
        assert_eq!(canon(&a).unwrap(), canon(&b).unwrap());
        assert_eq!(canon(&a).unwrap().off, 4096);
        assert_eq!(sst_of(&id).unwrap(), canon(&a).unwrap().sst);
    }

    /// The exit rule: blocks read twice survive a one-off scan of many blocks read once.
    #[tokio::test]
    async fn exit_rule_protects_what_comes_back() {
        let c = Counter::new(512 << 10, None, 0).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder("t", Arc::clone(&store)).with_db_cache(c.split_cache(), 1).build().await.unwrap();
        let val = vec![3u8; 1000];
        for i in 0..20000u32 {
            db.put(format!("doc/k{i:06}"), &val).await.unwrap();
        }
        db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
        // the hot client: 40 keys read three times
        let hot: Vec<u32> = (0..40u32).map(|i| i * 13).collect();
        for _ in 0..3 {
            for &i in &hot {
                db.get(format!("doc/k{i:06}")).await.unwrap().unwrap();
            }
        }
        // a one-off scan of everything else (about 20 MB through a 512 KiB counter)
        let mut it = db.scan(format!("doc/k{:06}", 1000)..).await.unwrap();
        let mut n = 0;
        while let Some(_) = it.next().await.unwrap() {
            n += 1;
        }
        assert!(n > 15000);
        let h0 = c.m.hits_ram.load(Relaxed);
        let m0 = c.m.misses.load(Relaxed);
        for &i in &hot {
            db.get(format!("doc/k{i:06}")).await.unwrap().unwrap();
        }
        let hits = c.m.hits_ram.load(Relaxed) - h0;
        let misses = c.m.misses.load(Relaxed) - m0;
        assert!(hits >= 30 && misses <= 10, "the scan pushed out the hot client: {hits} hits, {misses} misses");
        db.close().await.unwrap();
    }

    /// Reviewer (R-T1): a wave of passing clients (each reads a few documents once, or twice in a row like a page
    /// refresh) must not push out the clients that come back often.
    async fn passing_wave(reads_per_passer: usize) -> (u64, u64) {
        passing_wave_ram(reads_per_passer, 2 << 20).await
    }

    async fn passing_wave_ram(reads_per_passer: usize, ram: u64) -> (u64, u64) {
        // 2 MiB of RAM: about 0.4 MiB of maps (index and filters of 40 MB of documents), the rest for data; the 60
        // blocks of the active clients (about 0.25 MiB) fit the protected share many times
        let c = Counter::new(ram, None, 0).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        {
            let db = Db::builder("t", Arc::clone(&store)).with_db_cache_disabled().build().await.unwrap();
            let val = vec![3u8; 1000];
            for i in 0..40000u32 {
                db.put(format!("doc/k{i:06}"), &val).await.unwrap();
            }
            db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
            db.close().await.unwrap();
        }
        let db = Db::builder("t", Arc::clone(&store)).with_db_cache(c.split_cache(), 1).build().await.unwrap();
        // 3 active clients, 20 documents each (60 blocks, about 240 KiB: fits the protected share), read 3 times
        let hot: Vec<u32> = (0..60u32).map(|i| i * 17).collect();
        for _ in 0..3 {
            for &i in &hot {
                db.get(format!("doc/k{i:06}")).await.unwrap().unwrap();
            }
        }
        // the wave: 300 passing clients, 20 documents each, never seen before
        let mut x: u64 = 12345;
        for n in 0..300 {
            let mut keys = Vec::new();
            for _ in 0..20 {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                keys.push(2000 + (x >> 33) as u32 % 38000);
            }
            for _ in 0..reads_per_passer {
                for k in &keys {
                    db.get(format!("doc/k{k:06}")).await.unwrap().unwrap();
                }
            }
            // the active clients come back now and then (every 25 passers: 500 passing blocks, about the RAM)
            if n % 25 == 24 {
                for &i in &hot {
                    db.get(format!("doc/k{i:06}")).await.unwrap().unwrap();
                }
            }
        }
        let h0 = c.m.hits_ram.load(Relaxed);
        let m0 = c.m.misses.load(Relaxed);
        for &i in &hot {
            db.get(format!("doc/k{i:06}")).await.unwrap().unwrap();
        }
        let r = (c.m.hits_ram.load(Relaxed) - h0, c.m.misses.load(Relaxed) - m0);
        let (mu, _, du, _, dp) = c.ram_usage();
        eprintln!("REV wave {reads_per_passer}: maps {mu} B, data {du} B, protected {dp} B, promoted {}", c.m.promoted.load(Relaxed));
        db.close().await.unwrap();
        r
    }

    #[tokio::test]
    async fn rev_passing_wave_read_once() {
        let (hits, misses) = passing_wave(1).await;
        eprintln!("REV wave read once: hot hits {hits} misses {misses}");
        assert!(hits >= 50, "passing clients (read once) pushed out the active ones: {hits} hits, {misses} misses");
    }

    /// Reviewer (R-T1): when the pinned maps take most of the RAM (here about 0.4 MiB of 0.5 MiB; at 1 TB they are
    /// larger than any RAM), the data of the active clients gets no room: the RAM floor serves only maps.
    #[tokio::test]
    async fn rev_maps_leave_room_for_active_data() {
        let (hits, misses) = passing_wave_ram(1, 512 << 10).await;
        eprintln!("REV maps crowd out data: hot hits {hits} misses {misses}");
        assert!(hits >= 50, "pinned maps left no RAM for the active clients: {hits} hits, {misses} misses");
    }

    #[tokio::test]
    async fn rev_passing_wave_read_twice() {
        let (hits, misses) = passing_wave(2).await;
        eprintln!("REV wave read twice: hot hits {hits} misses {misses}");
        assert!(hits >= 50, "passing clients (read twice in a row) pushed out the active ones: {hits} hits, {misses} misses");
    }

    /// Reviewer (R-T1): RAM the counter keeps per entry it knows about (disk index + segment list + map-block set).
    #[test]
    fn rev_bookkeeping_sizes() {
        eprintln!(
            "REV sizes: Ck {} Loc {} (Ck,Loc) {} MapSlot {} (Ck,MapSlot) {} Slot {}",
            std::mem::size_of::<Ck>(),
            std::mem::size_of::<Loc>(),
            std::mem::size_of::<(Ck, Loc)>(),
            std::mem::size_of::<MapSlot>(),
            std::mem::size_of::<(Ck, MapSlot)>(),
            std::mem::size_of::<Slot>()
        );
    }

    #[tokio::test]
    async fn two_floors_end_to_end() {
        // a small db through the counter: RAM tiny, disk floor on: every read value is right, maps stay pinned,
        // a block read twice comes back from disk, prefetched blocks never read leave first.
        let dir = std::env::temp_dir().join(format!("counter-test-{}", std::process::id()));
        let c = Counter::new(256 << 10, Some(dir.clone()), 256 << 20).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let val = vec![7u8; 1000];
        {
            // written without the counter: no block is cached by the flush
            let db = Db::builder("t", Arc::clone(&store)).with_db_cache_disabled().build().await.unwrap();
            for i in 0..5000u32 {
                db.put(format!("doc/k{i:06}"), &val).await.unwrap();
            }
            for i in 0..3000u32 {
                db.put(format!("doc/p{i:06}"), &val).await.unwrap();
            }
            db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
            db.close().await.unwrap();
        }
        // reopen with another scope: the counter must still serve (and be right)
        let db = Db::builder("t", Arc::clone(&store)).with_db_cache(c.split_cache(), 2).build().await.unwrap();
        for i in (0..5000u32).step_by(7) {
            let v = db.get(format!("doc/k{i:06}")).await.unwrap().unwrap();
            assert_eq!(&v[..], &val[..]);
        }
        let (mu, mn, du, _, _) = c.ram_usage();
        // maps are kept, in RAM within their share or on the disk floor (R-T1 correction 2: with 256 KiB of RAM the
        // SST index alone is over the maps' share, so it lives on the disk floor)
        let disk_maps = c.disk_usage().1;
        assert!(mn > 0 && mu > 0 || disk_maps > 0, "maps not kept: {mu} {mn} {disk_maps}");
        assert!(du <= 256 << 10, "RAM data over its size: {du}");
        let (disk_used, _, disk_n) = c.disk_usage();
        assert!(disk_used > 0 && disk_n > 0, "nothing went to the disk floor");
        // read again what went to disk: served from disk, right bytes
        let before = c.m.hits_disk.load(Relaxed);
        for i in (0..5000u32).step_by(7) {
            let v = db.get(format!("doc/k{i:06}")).await.unwrap().unwrap();
            assert_eq!(&v[..], &val[..]);
        }
        assert!(c.m.hits_disk.load(Relaxed) > before, "no hit on the disk floor");
        // prefetched blocks that nobody reads leave before the others (keys whose blocks were never read)
        let unused0 = c.m.prefetched_evicted_unused.load(Relaxed);
        {
            let mut o = slatedb::config::DbReaderOptions::default();
            o.skip_wal_replay = true;
            let ahead = slatedb::DbReader::builder("t", Arc::clone(&store)).with_db_cache(c.prefetch_cache(), 3).with_options(o).build().await.unwrap();
            for i in (0..3000u32).step_by(4) {
                ahead.get(format!("doc/p{i:06}")).await.unwrap().unwrap();
            }
            ahead.close().await.unwrap();
        }
        for i in (2..5000u32).step_by(3) {
            db.get(format!("doc/k{i:06}")).await.unwrap();
        }
        assert!(c.m.prefetched_in.load(Relaxed) > 0);
        assert!(c.m.prefetched_evicted_unused.load(Relaxed) > unused0, "prefetched-unused blocks did not leave first");
        db.close().await.unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }
}
