//! The maps on the counter (T1, "le mappe sul bancone"): for every SST of every open database, the waiter keeps in its
//! counter ([`crate::counter`]) the SST index, the filters, the stats and the data blocks of every key that is not a
//! document (collection indexes `idx/`, metadata `meta/`, natural order `nat/`, and the other small families: every key
//! from [`MAP_FROM`] up). Then a cold read of a document asks S3 only for the document's block: one trip.
//!
//! How (only slatedb's public API): a loop looks at the manifest of every open writer every `MAPS_EVERY_MS`. For every SST
//! it has not mapped yet it calls `warm_sst(Index, Filters, Stats)`, finds from the SST index which data blocks hold
//! keys from `MAP_FROM` up, tells the counter they are maps (pinned) and calls `warm_sst(Data(MAP_FROM..))`: one ranged
//! GET per SST (split in parallel pieces when large, `simstore::SplitGetStore`). All SSTs of a database are warmed at once.
//! SSTs that left the manifest (compaction) are forgotten by the counter.
//!
//! The maps of a database stay on the counter after the database closes: the counter ignores slatedb's per-open cache
//! scope, so a reopen finds them (and the loop finds nothing new to warm).

use crate::counter::{sst_of, Counter};
use object_store::ObjectStore;
use slatedb::manifest::SsTableHandle;
use slatedb::{CacheTarget, DbCacheManagerOps, SstReader};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Every key from here up is a map: `doc/` is the only family of documents and `"doc0"` is the first key after all of
/// `doc/...` (`'0'` follows `'/'`). Families below `doc/` do not exist (`layout.rs`).
pub const MAP_FROM: &[u8] = b"doc0";

/// Keep the maps on the counter (`--maps`, on with the counter).
pub static MAPS_ON: AtomicBool = AtomicBool::new(false);
/// How often the manifests of the open writers are looked at.
pub static MAPS_EVERY_MS: AtomicU64 = AtomicU64::new(500);
/// SSTs warmed, map data blocks marked, warm calls failed, refreshes that warmed something, ms of the last one.
pub static MAPS_SSTS: AtomicU64 = AtomicU64::new(0);
pub static MAPS_BLOCKS: AtomicU64 = AtomicU64::new(0);
pub static MAPS_FAILS: AtomicU64 = AtomicU64::new(0);
pub static MAPS_RUNS: AtomicU64 = AtomicU64::new(0);
pub static MAPS_LAST_MS: AtomicU64 = AtomicU64::new(0);
/// Warm-ups running now (a test or the bench waits for 0).
pub static MAPS_BUSY: AtomicU64 = AtomicU64::new(0);

struct Mapped {
    manifest: u64,
    /// SST -> its estimated size in bytes.
    ssts: HashMap<[u8; 26], u64>,
}

static MAPPED: std::sync::LazyLock<Mutex<HashMap<String, Mapped>>> = std::sync::LazyLock::new(Default::default);

/// (SSTs mapped, their bytes) over all databases: the denominator of "maps per GB of data".
pub fn mapped_totals() -> (u64, u64) {
    let m = MAPPED.lock().unwrap();
    let mut n = 0;
    let mut b = 0;
    for v in m.values() {
        n += v.ssts.len() as u64;
        b += v.ssts.values().sum::<u64>();
    }
    (n, b)
}

/// Start the loop. `store` is the lifecycle's store (the one the writers use, before the per-tenant wrapper).
pub fn start(counter: Arc<Counter>, store: Arc<dyn ObjectStore>) {
    MAPS_ON.store(true, Relaxed);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(MAPS_EVERY_MS.load(Relaxed).max(20))).await;
            let writers = crate::lifecycle::open_writers();
            let w: HashSet<String> = writers.iter().map(|(id, _)| id.clone()).collect();
            for (id, db) in writers {
                refresh(&counter, &store, &id, Src::W(db));
            }
            for (id, r) in crate::lifecycle::open_readers() {
                if !w.contains(&id) {
                    refresh(&counter, &store, &id, Src::R(r));
                }
            }
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

/// Look at one database's manifest; warm what is new, forget what left.
pub fn refresh(counter: &Arc<Counter>, store: &Arc<dyn ObjectStore>, id: &str, db: Src) {
    let m = db.manifest();
    let mid = m.id();
    let mut now: HashMap<[u8; 26], SsTableHandle> = HashMap::new();
    for v in m.all_sst_views() {
        if let Some(s) = sst_of(&v.sst.id) {
            now.entry(s).or_insert_with(|| v.sst.clone());
        }
    }
    let (new, gone) = {
        let mut mapped = MAPPED.lock().unwrap();
        let e = mapped.entry(id.to_string()).or_insert_with(|| Mapped { manifest: u64::MAX, ssts: HashMap::new() });
        if e.manifest == mid {
            return;
        }
        e.manifest = mid;
        let new: Vec<([u8; 26], SsTableHandle)> = now.iter().filter(|(s, _)| !e.ssts.contains_key(*s)).map(|(s, h)| (*s, h.clone())).collect();
        let gone: HashSet<[u8; 26]> = e.ssts.keys().filter(|s| !now.contains_key(*s)).copied().collect();
        for s in &gone {
            e.ssts.remove(s);
        }
        for (s, h) in &new {
            e.ssts.insert(*s, h.estimate_size());
        }
        (new, gone)
    };
    counter.forget_ssts(&gone);
    if new.is_empty() {
        return;
    }
    let db = db.clone();
    let counter = Arc::clone(counter);
    let store = crate::counting::owned(store, id);
    let path = crate::lifecycle::tenant_path(id);
    let id = id.to_string();
    MAPS_BUSY.fetch_add(1, Relaxed);
    tokio::spawn(async move {
        let t0 = Instant::now();
        let reader = SstReader::new(path, store, Some(counter.split_cache()), None);
        let n = new.len().max(1);
        // All SSTs together: one trip for the indexes, filters and stats, one for the map blocks.
        let results: Vec<Result<(), String>> = futures::future::join_all(new.into_iter().map(|(sst, h)| {
            let (db, counter, reader) = (&db, &counter, &reader);
            async move { warm_one(db, counter, reader, sst, h).await.map_err(|e| format!("{e:#}")) }
        }))
        .await;
        let mut failed = Vec::new();
        for (i, r) in results.into_iter().enumerate() {
            if let Err(e) = r {
                MAPS_FAILS.fetch_add(1, Relaxed);
                if i < 3 {
                    eprintln!("maps of {id}: {e}");
                }
                failed.push(i);
            }
        }
        if !failed.is_empty() {
            // warm again at the next look
            if let Some(e) = MAPPED.lock().unwrap().get_mut(&id) {
                e.manifest = u64::MAX;
                e.ssts.clear();
            }
        }
        MAPS_RUNS.fetch_add(1, Relaxed);
        MAPS_SSTS.fetch_add(n as u64, Relaxed);
        MAPS_LAST_MS.store(t0.elapsed().as_millis() as u64, Relaxed);
        MAPS_BUSY.fetch_sub(1, Relaxed);
    });
}

async fn warm_one(db: &Src, counter: &Arc<Counter>, reader: &SstReader, sst: [u8; 26], h: SsTableHandle) -> anyhow::Result<()> {
    // the three parts at once (warm_sst reads its targets one after the other)
    let (a, b, c) = futures::join!(db.warm_sst(h.id, &[CacheTarget::Index]), db.warm_sst(h.id, &[CacheTarget::Filters]), db.warm_sst(h.id, &[CacheTarget::Stats]));
    a?;
    b?;
    c?;
    // Does the SST hold any key from MAP_FROM up?
    if let Some(last) = &h.info.last_entry {
        if &last[..] < MAP_FROM {
            return Ok(());
        }
    }
    let index = reader.open_with_handle(h.clone())?.index().await?;
    let n = index.len();
    let mut offs = Vec::new();
    for i in 0..n {
        // block i spans [first(i), first(i+1)): it holds map keys when the next block starts after MAP_FROM
        let holds = match index.get(i + 1) {
            Some((_, next)) => next > MAP_FROM,
            None => true,
        };
        if holds {
            offs.push(index.get(i).unwrap().0);
        }
    }
    if offs.is_empty() {
        return Ok(());
    }
    MAPS_BLOCKS.fetch_add(offs.len() as u64, Relaxed);
    counter.mark_map_blocks(sst, offs);
    db.warm_sst(h.id, &[CacheTarget::data::<&[u8], _>(MAP_FROM..)]).await?;
    Ok(())
}
