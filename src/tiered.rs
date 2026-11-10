//! Tiered-levels compaction scheduler (the prototype's "livelli", M26/M29, `proto/lavaggio-per-cliente`), run by the
//! server's compaction pass (`lifecycle::run_pass`).
//!
//! - Once a tree has at least `min_l0` L0 SSTs, all of them become one new sorted run (as SlateDB's size-tiered does).
//! - A sorted run merges into the next older one as soon as it is at least 1/T of its size.
//!
//! So the runs stay few (about log_T(size / L0 size)) and grow with age, and each byte is rewritten about T/2 times per
//! level (M26: amplification 3.91 with customer-first keys against 3.95 without, MEASURED).
//!
//! `validate` (M29) refuses any job whose sources are not consecutive runs of one tree (newest L0 first, then the sorted
//! runs newest to oldest) or whose destination is not the oldest sorted run among them: without it a wrong job merges the
//! newest run with the oldest, skipping the one in between, and a read then returns an old version (test below).
//! SlateDB calls it when a job is proposed and again before the job's result is published.

use slatedb::compactor::{CompactionScheduler, CompactionSchedulerSupplier, CompactionSpec, CompactorStateView, SourceId};
use slatedb::config::CompactorOptions;
use slatedb::Error;
use std::collections::{HashSet, VecDeque};

#[derive(Clone, Copy, Debug)]
pub struct Tiered {
    /// Size ratio between neighbouring runs (the prototype's T; 4 was measured in M26, M29, M31).
    pub t: f64,
    /// L0 SSTs that make a new sorted run.
    pub min_l0: usize,
    /// Compactions running at once for one database.
    pub max_active: usize,
}

impl Tiered {
    pub const fn new(t: f64) -> Self {
        Tiered { t, min_l0: crate::lifecycle::L0_COMPACT_THRESHOLD, max_active: 4 }
    }
}

impl CompactionSchedulerSupplier for Tiered {
    fn compaction_scheduler(&self, _o: &CompactorOptions) -> Box<dyn CompactionScheduler + Send + Sync> {
        Box::new(*self)
    }
}

type Tree = (bytes::Bytes, Vec<SourceId>, Vec<(u32, u64)>);

impl CompactionScheduler for Tiered {
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        let m = state.manifest();
        let mut busy: HashSet<SourceId> = HashSet::new();
        let mut active = 0usize;
        let mut next_sr: u32 = 0;
        if let Some(c) = state.compactions() {
            for comp in c.recent_compactions().filter(|c| c.active()) {
                active += 1;
                for s in comp.spec().sources() {
                    busy.insert(*s);
                }
                if let Some(d) = comp.spec().destination() {
                    busy.insert(SourceId::SortedRun(d));
                    next_sr = next_sr.max(d + 1);
                }
            }
        }
        let l0 = |v: &VecDeque<slatedb::manifest::SsTableView>| v.iter().map(|x| SourceId::SstView(x.id)).collect::<Vec<_>>();
        let srs = |c: &Vec<slatedb::manifest::SortedRun>| c.iter().map(|r| (r.id, r.estimate_size())).collect::<Vec<_>>();
        let mut trees: Vec<Tree> = vec![(bytes::Bytes::new(), l0(m.l0()), srs(m.compacted()))];
        for s in m.segments() {
            trees.push((s.prefix().clone(), l0(s.l0()), srs(s.compacted())));
        }
        for (_, _, s) in &trees {
            for (id, _) in s {
                next_sr = next_sr.max(id + 1);
            }
        }
        let mut out = Vec::new();
        // the trees with the most L0 SSTs first: they hold back the writer
        trees.sort_by_key(|t| std::cmp::Reverse(t.1.len()));
        for (prefix, l0s, srs) in trees {
            if active + out.len() >= self.max_active {
                break;
            }
            if !l0s.is_empty() && l0s.len() >= self.min_l0 && l0s.iter().all(|s| !busy.contains(s)) {
                let dst = next_sr;
                next_sr += 1;
                busy.extend(l0s.iter().copied());
                busy.insert(SourceId::SortedRun(dst));
                out.push(CompactionSpec::for_segment(prefix.clone(), l0s.clone(), dst));
                if active + out.len() >= self.max_active {
                    break;
                }
            }
            // sorted runs, newest to oldest: a run merges into the next older one once it is 1/T of its size
            for i in 0..srs.len().saturating_sub(1) {
                let (a, b) = (srs[i], srs[i + 1]);
                if busy.contains(&SourceId::SortedRun(a.0)) || busy.contains(&SourceId::SortedRun(b.0)) {
                    continue;
                }
                if (a.1 as f64) * self.t >= b.1 as f64 {
                    let dst = a.0.min(b.0);
                    busy.insert(SourceId::SortedRun(a.0));
                    busy.insert(SourceId::SortedRun(b.0));
                    out.push(CompactionSpec::for_segment(prefix.clone(), vec![SourceId::SortedRun(a.0), SourceId::SortedRun(b.0)], dst));
                    if active + out.len() >= self.max_active {
                        break;
                    }
                }
            }
        }
        out
    }

    fn validate(&self, state: &CompactorStateView, spec: &CompactionSpec) -> Result<(), Error> {
        let m = state.manifest();
        let tree = if spec.segment().is_empty() {
            Some(logical_order(m.l0(), m.compacted()))
        } else {
            m.segments().iter().find(|s| s.prefix() == spec.segment()).map(|s| logical_order(s.l0(), s.compacted()))
        };
        check(tree.as_deref(), spec).map_err(Error::invalid)
    }
}

/// The sources of one tree in read order: L0 newest first, then the sorted runs newest to oldest.
pub fn logical_order(l0: &VecDeque<slatedb::manifest::SsTableView>, c: &[slatedb::manifest::SortedRun]) -> Vec<SourceId> {
    l0.iter().map(|v| SourceId::SstView(v.id)).chain(c.iter().map(|r| SourceId::SortedRun(r.id))).collect()
}

/// A job is valid when its sources are consecutive in `tree` (read order) and its destination is the oldest sorted run
/// among them (or a new run, for L0 only).
pub fn check(tree: Option<&[SourceId]>, spec: &CompactionSpec) -> Result<(), String> {
    if spec.is_drain() {
        return Ok(());
    }
    let Some(order) = tree else {
        return Err("unknown segment".into());
    };
    let n = spec.sources().len();
    if n == 0 || n > order.len() || !order.windows(n).any(|w| w == spec.sources()) {
        return Err(format!("sources not consecutive: {:?} in {:?}", spec.sources(), order));
    }
    let min_sr = spec
        .sources()
        .iter()
        .filter_map(|s| match s {
            SourceId::SortedRun(id) => Some(*id),
            _ => None,
        })
        .min();
    if let Some(min_sr) = min_sr {
        if spec.destination() != Some(min_sr) {
            return Err(format!("destination {:?} is not the oldest run {min_sr}", spec.destination()));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Settings of the server's compaction (`--sst-block-kb`, `--sst-compression`, `--compaction-tiers`)

use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};
use std::sync::Arc;

/// SST block size in KiB for the writer's flushes and the compaction worker's output. Default 4 (the format of the
/// releases before compaction-1); M29 measured 64 (`--sst-block-kb 64`).
pub static SST_BLOCK_KB: AtomicU32 = AtomicU32::new(4);
/// Block compression: 0 none (default: a release without lz4 must still read the data, review of 91414a6), 1 lz4
/// (M29: 2.41x, +18% compaction CPU; `--sst-compression lz4`), 2 zstd. Turning it on is a one-way format change.
pub static SST_COMPRESSION: AtomicU8 = AtomicU8::new(0);
/// T of the tiered scheduler; 0 = SlateDB's own size-tiered scheduler (the engine before this change).
pub static TIERS: AtomicU32 = AtomicU32::new(4);

pub fn set_compression(name: &str) -> anyhow::Result<()> {
    let v = match name {
        "none" | "off" => 0,
        "lz4" => 1,
        "zstd" => 2,
        other => anyhow::bail!("unknown --sst-compression {other} (none, lz4, zstd)"),
    };
    SST_COMPRESSION.store(v, Ordering::Relaxed);
    Ok(())
}

pub fn block_size() -> slatedb::SstBlockSize {
    use slatedb::SstBlockSize::*;
    match SST_BLOCK_KB.load(Ordering::Relaxed) {
        0..=1 => Block1Kib,
        2..=3 => Block2Kib,
        4..=7 => Block4Kib,
        8..=15 => Block8Kib,
        16..=31 => Block16Kib,
        32..=63 => Block32Kib,
        _ => Block64Kib,
    }
}

pub fn codec() -> Option<slatedb::config::CompressionCodec> {
    match SST_COMPRESSION.load(Ordering::Relaxed) {
        1 => Some(slatedb::config::CompressionCodec::Lz4),
        2 => Some(slatedb::config::CompressionCodec::Zstd),
        _ => None,
    }
}

/// The scheduler the compaction pass gives its compactor (`None`: SlateDB's default).
pub fn scheduler() -> Option<Arc<dyn CompactionSchedulerSupplier>> {
    match TIERS.load(Ordering::Relaxed) {
        0 => None,
        t => Some(Arc::new(Tiered::new(t as f64))),
    }
}

/// The worker that executes the pass's compactions. SlateDB 0.17's `CompactorBuilder::build` gives its embedded worker
/// SSTs with 4 KiB blocks and no compression whatever the settings (M29, `db/builder.rs` 1388-1392); the separate
/// worker (`CompactionWorkerBuilder`) takes block size and compression, so the pass runs its compactor without an
/// embedded worker and this one beside it, on the same store.
pub fn worker_builder(path: slatedb::object_store::path::Path, store: Arc<dyn slatedb::object_store::ObjectStore>, filters: Option<Vec<Arc<dyn slatedb::FilterPolicy>>>) -> slatedb::CompactionWorkerBuilder<slatedb::object_store::path::Path> {
    let mut o = slatedb::config::CompactionWorkerOptions::default();
    o.compactions_poll_interval = std::time::Duration::from_millis(50);
    o.compression_codec = codec();
    o.min_filter_keys = crate::lifecycle::MIN_FILTER_KEYS;
    let b = slatedb::CompactionWorkerBuilder::new(path, store).with_options(o).with_sst_block_size(block_size());
    match filters {
        Some(f) => b.with_filter_policies(f),
        None => b,
    }
}

/// Fetch tasks of a scan with a small read-ahead (at most one 64 KiB block per request: an index scan, a find's
/// documents). M29: one task per sorted run reads 0.92-1.00 GET per run on a find; four (SlateDB's default) read
/// 1.37-1.62, the extra requests fetching blocks past the end of what the find needed. `--small-scan-fetch-tasks`.
pub static SMALL_SCAN_FETCH_TASKS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);

pub fn scan_fetch_tasks(read_ahead: usize) -> usize {
    if read_ahead <= 64 << 10 {
        SMALL_SCAN_FETCH_TASKS.load(Ordering::Relaxed).max(1)
    } else {
        4
    }
}

/// One SST of a database's current manifest (`fenvixSstInfo`, tests and measurements).
#[derive(Clone, Debug)]
pub struct SstSummary {
    pub id: String,
    pub codec: &'static str,
    /// Bytes of the data blocks as stored (compressed when the SST is).
    pub data_bytes: u64,
    pub blocks: usize,
}

/// The SSTs of a database's latest manifest: the L0 SSTs (newest first) and the sorted runs (newest first).
pub async fn sst_info(store: Arc<dyn slatedb::object_store::ObjectStore>, id: &str) -> anyhow::Result<(Vec<SstSummary>, Vec<(u32, Vec<SstSummary>)>)> {
    let path = crate::lifecycle::tenant_path(id);
    let admin = slatedb::admin::Admin::builder(path.clone(), Arc::clone(&store)).build();
    let Some(m) = admin.read_manifest(None).await? else { return Ok((vec![], vec![])) };
    let reader = slatedb::SstReader::new(path, store, None, None);
    let one = |v: &slatedb::manifest::SsTableView| {
        let h = v.sst.clone();
        let r = &reader;
        async move {
            let codec = match h.info.compression_codec {
                None => "none",
                Some(slatedb::config::CompressionCodec::Lz4) => "lz4",
                Some(slatedb::config::CompressionCodec::Zstd) => "zstd",
                #[allow(unreachable_patterns)]
                Some(_) => "other",
            };
            let data_bytes = h.info.index_offset;
            let blocks = r.open_with_handle(h.clone())?.index().await?.len();
            anyhow::Ok(SstSummary { id: h.id.value().to_string(), codec, data_bytes, blocks })
        }
    };
    let mut l0 = Vec::new();
    for v in m.l0() {
        l0.push(one(v).await?);
    }
    let mut runs = Vec::new();
    for r in m.compacted() {
        let mut ssts = Vec::new();
        for v in r.sst_views() {
            ssts.push(one(v).await?);
        }
        runs.push((r.id, ssts));
    }
    Ok((l0, runs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    fn sr(i: u32) -> SourceId {
        SourceId::SortedRun(i)
    }

    #[test]
    fn check_accepts_and_refuses() {
        let a = SourceId::SstView(ulid::Ulid::from_parts(1, 1));
        let b = SourceId::SstView(ulid::Ulid::from_parts(2, 2));
        let tree = vec![b, a, sr(7), sr(5), sr(2)];
        assert!(check(Some(&tree), &CompactionSpec::new(vec![b, a], 8)).is_ok());
        assert!(check(Some(&tree), &CompactionSpec::new(vec![sr(7), sr(5)], 5)).is_ok());
        assert!(check(Some(&tree), &CompactionSpec::new(vec![sr(5), sr(2)], 2)).is_ok());
        let e = check(Some(&tree), &CompactionSpec::new(vec![sr(7), sr(2)], 2)).unwrap_err();
        assert!(e.contains("not consecutive"), "{e}");
        let e = check(Some(&tree), &CompactionSpec::new(vec![sr(5), sr(7)], 5)).unwrap_err();
        assert!(e.contains("not consecutive"), "{e}");
        let e = check(Some(&tree), &CompactionSpec::new(vec![sr(7), sr(5)], 7)).unwrap_err();
        assert!(e.contains("destination"), "{e}");
        let e = check(Some(&tree), &CompactionSpec::new(vec![a, b], 8)).unwrap_err();
        assert!(e.contains("not consecutive"), "{e}");
        let e = check(None, &CompactionSpec::new(vec![sr(7)], 7)).unwrap_err();
        assert!(e.contains("segment"), "{e}");
    }

    /// A scheduler that proposes a wrong job: with three runs, the newest merged into the oldest, skipping the middle one.
    struct Wrong {
        with_validate: bool,
    }

    impl CompactionSchedulerSupplier for Wrong {
        fn compaction_scheduler(&self, _o: &CompactorOptions) -> Box<dyn CompactionScheduler + Send + Sync> {
            Box::new(Wrong { with_validate: self.with_validate })
        }
    }

    impl CompactionScheduler for Wrong {
        fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
            let m = state.manifest();
            if let Some(c) = state.compactions() {
                if c.recent_compactions().any(|c| c.active()) {
                    return vec![];
                }
            }
            let l0: Vec<SourceId> = m.l0().iter().map(|x| SourceId::SstView(x.id)).collect();
            let next = m.compacted().iter().map(|r| r.id + 1).max().unwrap_or(0);
            if !l0.is_empty() {
                return vec![CompactionSpec::new(l0, next)];
            }
            let srs: Vec<u32> = m.compacted().iter().map(|r| r.id).collect();
            if srs.len() == 3 {
                return vec![CompactionSpec::new(vec![sr(srs[0]), sr(srs[2])], srs[2])];
            }
            vec![]
        }
        fn validate(&self, state: &CompactorStateView, spec: &CompactionSpec) -> Result<(), Error> {
            if self.with_validate {
                Tiered { t: 4.0, min_l0: 1, max_active: 1 }.validate(state, spec)
            } else {
                Ok(())
            }
        }
    }

    async fn wrong_job(with_validate: bool) -> (Vec<u8>, usize) {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let mut s = slatedb::Settings::default();
        s.compactor_options = None;
        s.l0_sst_size_bytes = 1 << 20;
        let mut co = CompactorOptions::default();
        co.poll_interval = Duration::from_millis(50);
        let cb = slatedb::CompactorBuilder::new("t", store.clone()).with_options(co).with_scheduler_supplier(Arc::new(Wrong { with_validate }));
        let db = slatedb::Db::builder("t", store.clone()).with_settings(s).with_compactor_builder(cb).build().await.unwrap();
        let wait = |db: &slatedb::Db, n_sr: usize| {
            let db = db.clone();
            async move {
                for _ in 0..200 {
                    let m = db.manifest();
                    if m.l0().is_empty() && m.compacted().len() == n_sr {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                panic!("compaction never reached {n_sr} runs");
            }
        };
        for (i, v) in [b"v1", b"v2", b"v3"].iter().enumerate() {
            db.put(b"key", *v).await.unwrap();
            db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
            wait(&db, i + 1).await;
        }
        let mut n = 3;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            n = db.manifest().compacted().len();
            if n != 3 {
                break;
            }
        }
        if with_validate {
            tokio::time::sleep(Duration::from_secs(3)).await;
            n = db.manifest().compacted().len();
        }
        let v = db.get(b"key").await.unwrap().unwrap().to_vec();
        db.close().await.unwrap();
        (v, n)
    }

    /// Negative control: without `validate` the wrong job runs and a read returns the old version.
    #[tokio::test(flavor = "multi_thread")]
    async fn without_validate_a_read_returns_an_old_version() {
        let (v, n) = wrong_job(false).await;
        assert_eq!(n, 2, "the wrong job did not run");
        assert_eq!(v, b"v2", "the read sees the middle run's version, not the newest");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn with_validate_the_wrong_job_is_refused() {
        let (v, n) = wrong_job(true).await;
        assert_eq!(n, 3, "the wrong job ran");
        assert_eq!(v, b"v3");
    }

    /// The tiered scheduler, run by a real database: runs stay ordered by age, the newest version of every key wins.
    #[tokio::test(flavor = "multi_thread")]
    async fn tiered_keeps_runs_few_and_newest_versions() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let mut s = slatedb::Settings::default();
        s.compactor_options = None;
        s.l0_sst_size_bytes = 64 << 10;
        let mut co = CompactorOptions::default();
        co.poll_interval = Duration::from_millis(20);
        let cb = slatedb::CompactorBuilder::new("t", store.clone()).with_options(co).with_scheduler_supplier(Arc::new(Tiered::new(4.0)));
        let db = slatedb::Db::builder("t", store.clone()).with_settings(s).with_compactor_builder(cb).build().await.unwrap();
        let val = |i: u32, round: u32| format!("{round:04}-{}", "x".repeat((i % 7) as usize * 40)).into_bytes();
        for round in 0..24u32 {
            for i in 0..400u32 {
                db.put(format!("k{:05}", (i * 7919 + round * 31) % 2000).as_bytes(), &val(i, round)).await.unwrap();
            }
            db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
        }
        let mut latest: std::collections::HashMap<u32, u32> = Default::default();
        for round in 0..24u32 {
            for i in 0..400u32 {
                latest.insert((i * 7919 + round * 31) % 2000, round);
            }
        }
        for _ in 0..200 {
            if db.manifest().l0().len() < 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        let runs = db.manifest().compacted().len();
        assert!(runs <= 6, "too many sorted runs: {runs}");
        for (k, round) in latest {
            let v = db.get(format!("k{k:05}").as_bytes()).await.unwrap().unwrap();
            assert_eq!(&v[..4], format!("{round:04}").as_bytes(), "key {k}");
        }
        db.close().await.unwrap();
    }
}
