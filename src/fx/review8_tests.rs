//! Review of compaction 2, step 8 (migration and rollback): failing tests.
//!
//! - `an_erasure_during_the_build_leaves_the_migrated_runs_clean`: a customer erased while its database is migrating.
//!   The erasure pass publishes a version of its own (step 3's whole build) and scrubs it, then records the erasure as
//!   done in the live runs; the migration then publishes its groups, built from the cut, which still hold the customer.
//! - `e0_open_racing_a_commit_is_refused`: a release with sealed runs off (E0) opens a database whose migration is
//!   committed by the old process at the same moment; its rollback loses the race for the state slot and the open goes
//!   on anyway (`refuse_without_sealed_runs` treats `Raced` as success).
//! - `read_ahead_memory_is_bounded`: what the migration's read-ahead keeps in memory after reading N SSTs end to end.

use super::erase::{ScrubOpts, Targets};
use super::migrate::{self, Phase};
use super::tests::{lc, SERIAL, TEST_T_PIN_MS};
use super::*;
use crate::lifecycle::{Op, Tenant};
use crate::runfmt::{CustomerKind, RunOptions};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

fn run_opts() -> RunOptions {
    RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2)
}

fn setup() {
    tests::enable();
    ENABLED.store(false, Relaxed); // the database starts on today's engine
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(3 << 10, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(64, Relaxed);
    migrate::GROUP_BYTES.store(8 << 10, Relaxed);
    migrate::READ_AHEAD.store(64 << 10, Relaxed);
    migrate::CONTROL_MARK_EARLY.store(false, Relaxed);
    migrate::E0_GUARD_OFF.store(false, Relaxed);
    GC_GRACE_MS.store(0, Relaxed);
    hold::HOLD_LIFE_MS.store(3000, Relaxed);
    view::clear_cache();
}

fn teardown() {
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(0, Relaxed);
    T_PIN_MS.store(TEST_T_PIN_MS, Relaxed);
    migrate::GROUP_BYTES.store(1 << 30, Relaxed);
    migrate::READ_AHEAD.store(8 << 20, Relaxed);
    GC_GRACE_MS.store(600_000, Relaxed);
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
    ENABLED.store(false, Relaxed);
}

const ERASED: &[u8] = b"ERASEDMARK";

fn a_key(i: usize) -> Vec<u8> {
    format!("u/CUSTA/c/d/{i:04}").into_bytes()
}

fn b_key(i: usize) -> Vec<u8> {
    format!("u/CUSTB/c/d/{i:04}").into_bytes()
}

fn a_targets() -> Targets {
    let mut t = Targets::default();
    t.ranges.push((b"u/CUSTA/".to_vec(), b"u/CUSTA0".to_vec()));
    t
}

/// The runs of the newest version that hold `needle` (decoded block by block).
async fn newest_runs_holding(store: &Arc<dyn ObjectStore>, id: &str, needle: &[u8]) -> Vec<String> {
    let v = manifest::load_newest(store, id).await.unwrap().unwrap();
    let listed: Vec<String> = v.groups.iter().filter_map(|g| g.sealed.as_ref().map(|r| r.path.clone())).chain(v.delta.iter().map(|r| r.path.clone())).collect();
    let has = |b: &[u8]| b.windows(needle.len()).any(|w| w == needle);
    let mut out = Vec::new();
    for p in listed {
        let loc = Path::from(p.as_str());
        let size = store.head(&loc).await.unwrap().size;
        let (run, _) = crate::runfmt::Run::open(store, loc.clone(), size).await.unwrap();
        let all = store.get(&loc).await.unwrap().bytes().await.unwrap();
        for i in 0..run.fence.blocks() {
            let (x, y) = run.fence.range(i, i);
            let b = crate::runfmt::decode_checked(&run, i, &all[x as usize..y as usize]).unwrap();
            if (0..b.len()).any(|j| has(b.key(j)) || has(b.env(j))) {
                out.push(p.clone());
                break;
            }
        }
    }
    out
}

async fn erase_a(t: &mut Tenant) -> Vec<erase::Erasure> {
    let ops: Vec<Op> = (0..40).map(|i| Op::Delete(a_key(i))).collect();
    t.apply(ops).await.unwrap();
    t.fx_erasure_register("c", b"CUSTA", &a_targets()).await.unwrap().expect("recorded");
    let o = ScrubOpts { sealed: run_opts(), delta: run_opts() };
    t.fx_erase_pass(&o).await.unwrap()
}

async fn begin(t: &mut Tenant) {
    for _ in 0..100 {
        if t.fx_migrate_begin().await.unwrap().is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(TEST_T_PIN_MS / 4)).await;
    }
    panic!("the migration did not begin");
}

/// A customer is erased while its database migrates (item 3 of the review prompt). The erasure promise (step 7b,
/// DECISIONS 2026-11-07): once the erasure is `live_done`, no run of the newest version holds the customer's rows at or
/// below S_e. The control (`erase_after_publish`) runs the same erasure after the migration's publish: that one holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_erasure_during_the_build_leaves_the_migrated_runs_clean() {
    let _s = SERIAL.lock().await;
    let mut results = Vec::new();
    for erase_after_publish in [true, false] {
        setup();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = if erase_after_publish { "rv8-erase-ctl" } else { "rv8-erase" };
        // today's engine: customers A and B
        let mut t = lc(&store).create(id).await.unwrap();
        for i in 0..40 {
            t.apply(vec![Op::Put(a_key(i), format!("{}-{i}-{}", std::str::from_utf8(ERASED).unwrap(), "x".repeat(60)).into_bytes())]).await.unwrap();
            t.apply(vec![Op::Put(b_key(i), format!("b-{i}-{}", "y".repeat(60)).into_bytes())]).await.unwrap();
        }
        t.abandon().await;
        // release E1 with sealed runs: the migration begins and builds its first group
        ENABLED.store(true, Relaxed);
        let mut t = lc(&store).wake(id).await.unwrap();
        t.upgrade().await.unwrap();
        begin(&mut t).await;
        migrate::build_step(&store, id, 1, &run_opts(), None).await.unwrap();
        let mut during = None;
        if !erase_after_publish {
            during = Some(erase_a(&mut t).await);
        }
        // the build goes on and the migration publishes
        while !migrate::load(&store, id).await.unwrap().unwrap().built {
            migrate::build_step(&store, id, 0, &run_opts(), None).await.unwrap();
        }
        let v = t.fx_migrate_finish(&run_opts(), None).await.unwrap();
        assert!(v.is_some(), "the migration did not publish");
        let all = match during {
            Some(x) => x,
            None => erase_a(&mut t).await,
        };
        let e = all.first().cloned().unwrap();
        let mut wrong = 0;
        for i in 0..40 {
            if t.get(a_key(i)).await.unwrap().is_some() || t.get(b_key(i)).await.unwrap().is_none() {
                wrong += 1;
            }
        }
        let holders = newest_runs_holding(&store, id, ERASED).await;
        let s = migrate::load(&store, id).await.unwrap().unwrap();
        eprintln!(
            "erasure {} the publish: erasure state {:?}, live_done {}, scrubbed version {:?}; migration {:?} version {:?}; wrong reads {wrong}; runs of the newest version holding the erased customer: {holders:?}",
            if erase_after_publish { "after" } else { "during the build, before" },
            e.state,
            e.live_done_ms.is_some(),
            e.scrubbed_version,
            s.phase,
            s.version
        );
        t.abandon().await;
        teardown();
        results.push((erase_after_publish, e.live_done_ms.is_some(), holders, wrong));
    }
    for (after, live_done, holders, wrong) in results {
        assert_eq!(wrong, 0, "wrong reads (erase after publish: {after})");
        assert!(!(live_done && !holders.is_empty()), "erasure {} the publish: recorded live_done, yet the newest version's runs hold the erased customer: {holders:?}", if after { "after" } else { "during the build, before" });
    }
}

/// A store that, once armed, lets another process's write land first in the next migration state slot: the commit of
/// the old process (its agent, at the end of the window) wins the race against E0's rollback.
#[derive(Debug)]
struct CommitFirst {
    inner: Arc<dyn ObjectStore>,
    armed: AtomicBool,
    committed: std::sync::Mutex<Option<Vec<u8>>>,
}

impl std::fmt::Display for CommitFirst {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CommitFirst")
    }
}

#[async_trait::async_trait]
impl ObjectStore for CommitFirst {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        if location.as_ref().contains("/fx/migration/") && self.armed.swap(false, Relaxed) {
            // the other process's commit lands in the same slot first
            let body = self.committed.lock().unwrap().take().unwrap();
            let n: u64 = location.filename().unwrap().parse().unwrap();
            let mut s: migrate::State = serde_json::from_slice(&body).unwrap();
            s.n = n;
            self.inner.put_opts(location, PutPayload::from(serde_json::to_vec(&s).unwrap()), PutOptions { mode: object_store::PutMode::Create, ..Default::default() }).await?;
        }
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(&self, locations: futures::stream::BoxStream<'static, object_store::Result<Path>>) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: object_store::CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, options: object_store::RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

fn state(phase: Phase) -> migrate::State {
    migrate::State {
        format: migrate::FORMAT,
        n: 0,
        gen: 1,
        phase,
        started_ms: 1,
        e_env: 1,
        checkpoint: None,
        checkpoint_manifest: 1,
        x: 1,
        group_bytes: 1,
        groups: Vec::new(),
        next_group_id: 0,
        next_lo: None,
        built: true,
        catchup: Vec::new(),
        x_caught: 1,
        round_checkpoint: None,
        round_manifest: 1,
        version: Some(1),
        published_ms: 1,
        window_ms: 1,
        ignore_upto: 0,
        committed_ms: if phase == Phase::Committed { 2 } else { 0 },
        rolled_back_ms: 0,
        note: String::new(),
    }
}

/// E0's open-time check against the old process's commit at the same instant. The report's claim: "a release with
/// sealed runs off refuses to open the database" once committed. Here the commit lands between E0's load of the state
/// (Published) and its rollback's save: the save finds the slot taken (`Raced`), and `refuse_without_sealed_runs`
/// returns Ok: E0 opens a committed database, whose drop may now run, and reads SlateDB alone.
/// Controls: no race (the rollback lands, Ok, state rolled back); committed before the open (refused).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e0_open_racing_a_commit_is_refused() {
    let id = "rv8-race";
    let mut outcomes = Vec::new();
    for case in ["no race", "committed before", "commit races the rollback"] {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut s = state(if case == "committed before" { Phase::Committed } else { Phase::Published });
        migrate::save(&inner, id, &mut s).await.unwrap();
        let cf = Arc::new(CommitFirst { inner: Arc::clone(&inner), armed: AtomicBool::new(case == "commit races the rollback"), committed: std::sync::Mutex::new(Some(serde_json::to_vec(&state(Phase::Committed)).unwrap())) });
        let store: Arc<dyn ObjectStore> = cf;
        let r = migrate::refuse_without_sealed_runs(&store, id, true).await;
        let after = migrate::load(&inner, id).await.unwrap().unwrap().phase;
        eprintln!("{case}: open {} ; state after: {after:?}", if r.is_ok() { "allowed" } else { "refused" });
        outcomes.push((case, r.is_ok(), after));
    }
    assert_eq!(outcomes[0], ("no race", true, Phase::RolledBack));
    assert_eq!(outcomes[1], ("committed before", false, Phase::Committed));
    let (_, opened, after) = outcomes[2];
    assert!(!(opened && after == Phase::Committed), "E0 opened a database whose migration is committed (the commit raced its rollback)");
}

/// An object store of synthetic SSTs: every object is `size` bytes of zeros, made on demand (no memory of its own).
#[derive(Debug)]
struct Synthetic {
    size: u64,
}

impl std::fmt::Display for Synthetic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Synthetic")
    }
}

#[async_trait::async_trait]
impl ObjectStore for Synthetic {
    async fn put_opts(&self, _: &Path, _: PutPayload, _: PutOptions) -> object_store::Result<PutResult> {
        unimplemented!()
    }
    async fn put_multipart_opts(&self, _: &Path, _: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        unimplemented!()
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let meta = ObjectMeta { location: location.clone(), last_modified: Default::default(), size: self.size, e_tag: None, version: None };
        let r = match options.range {
            _ if options.head => 0..0,
            Some(object_store::GetRange::Bounded(r)) => r,
            _ => 0..self.size,
        };
        let b = bytes::Bytes::from(vec![7u8; (r.end - r.start) as usize]);
        Ok(GetResult { payload: GetResultPayload::Stream(Box::pin(futures::stream::once(async move { Ok(b) }))), meta, range: r, attributes: Default::default(), extensions: Default::default() })
    }
    fn delete_stream(&self, _: futures::stream::BoxStream<'static, object_store::Result<Path>>) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        unimplemented!()
    }
    fn list(&self, _: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        Box::pin(futures::stream::empty())
    }
    fn list_with_offset(&self, _: Option<&Path>, _: &Path) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        Box::pin(futures::stream::empty())
    }
    async fn list_with_delimiter(&self, _: Option<&Path>) -> object_store::Result<ListResult> {
        unimplemented!()
    }
    async fn copy_opts(&self, _: &Path, _: &Path, _: object_store::CopyOptions) -> object_store::Result<()> {
        unimplemented!()
    }
    async fn rename_opts(&self, _: &Path, _: &Path, _: object_store::RenameOptions) -> object_store::Result<()> {
        unimplemented!()
    }
}

fn rss_bytes() -> u64 {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap();
    s.split_whitespace().nth(1).unwrap().parse::<u64>().unwrap() * 4096
}

/// What the migration's read-ahead (`ReadAhead`, 8 MiB per GET, the production default) keeps after one merge read N
/// SSTs of 64 MiB end to end in 4 KiB blocks, as `SstReader` does (MEASURED with the process RSS). Not an assertion of
/// a bound decided anywhere: the review reports the number. `FX_RV8_SSTS` sets N (default 160).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_ahead_memory_is_bounded() {
    let n: usize = std::env::var("FX_RV8_SSTS").ok().and_then(|s| s.parse().ok()).unwrap_or(160);
    let size = 64u64 << 20;
    let syn: Arc<dyn ObjectStore> = Arc::new(Synthetic { size });
    let ra = migrate::ReadAhead::new(syn); // READ_AHEAD default: 8 MiB
    let before = rss_bytes();
    let mut peak = before;
    for i in 0..n {
        let p = Path::from(format!("tenants/x/compacted/{i:06}.sst"));
        let mut off = 0u64;
        while off < size {
            let r = ra.get_opts(&p, GetOptions { range: Some(object_store::GetRange::Bounded(off..off + 4096)), ..Default::default() }).await.unwrap();
            let _ = r.bytes().await.unwrap();
            off += 4096;
        }
        peak = peak.max(rss_bytes());
    }
    let after = rss_bytes();
    eprintln!("read-ahead: {n} SSTs of 64 MiB read end to end; RSS before {} MiB, peak {} MiB, after {} MiB (held by one merge until it ends)", before >> 20, peak >> 20, after >> 20);
    drop(ra);
    assert!(peak - before < (512u64 << 20), "the read-ahead holds {} MiB after reading {n} SSTs: chunks of SSTs the merge has finished are kept (up to 128 SSTs x 3 chunks)", (peak - before) >> 20);
}

fn knobs(drop: bool) -> super::compactor::Knobs {
    let mut k = super::compactor::Knobs::default();
    k.theta = 0.25;
    k.tier_unit = 2 << 10;
    k.absorb_min_l0 = 1;
    k.absorb_max_l0 = 8;
    k.local_max_bytes = 0;
    k.split_bytes = 40 << 10;
    k.merge_bytes = 18 << 10;
    k.min_seal_bytes = 8 << 10;
    k.sealed = RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2);
    k.delta = RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2);
    k.drop = drop;
    k.lease = Duration::from_millis(400);
    k.remote_lease = Duration::from_millis(3600);
    k.serving_lease = Duration::from_millis(300);
    k.put_timeout = Duration::from_millis(250);
    k.gc_grace = Duration::from_millis(3800);
    k.version_retention = Duration::from_millis(0);
    k.gc_every = Duration::from_millis(500);
    k.tick = Duration::from_millis(10);
    k.read_ahead = 64 << 10;
    k
}

async fn full_check(t: &mut Tenant, m: &std::collections::BTreeMap<Vec<u8>, Vec<u8>>) -> usize {
    t.fx_sync();
    let got = tests::as_vec(t.scan_range(b"", None, false, None, 1 << 20).await.unwrap());
    let want: Vec<(Vec<u8>, Vec<u8>)> = m.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    let got_m: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = got.into_iter().collect();
    let mut bad = 0;
    for (k, v) in &want {
        if got_m.get(k) != Some(v) {
            bad += 1;
        }
    }
    bad + got_m.keys().filter(|k| !m.contains_key(*k)).count()
}

/// The server's path after an erasure during the build: the erasure's own publish (step 3's build) makes the writer
/// serve a version while the migration is still building; `pass_must_skip` answers "no" for a building migration, so
/// today's pass moves L0 SSTs above that version's X_d into a sorted run; the next write starts the compactor
/// (`start_fxc`: the writer serves a version), which absorbs only L0. Rows the pass moved are then behind X_d.
/// Control: no erasure (no version, no compactor until the migration publishes).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_erasure_during_the_build_then_todays_pass_then_the_compactor() {
    use rand::{Rng, SeedableRng};
    let _s = SERIAL.lock().await;
    let mut out = Vec::new();
    for erase in [false, true] {
        setup();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = if erase { "rv8-pass" } else { "rv8-pass-ctl" };
        let mut rng = rand::rngs::StdRng::seed_from_u64(8);
        let keys = tests::universe();
        let mut m = std::collections::BTreeMap::new();
        let mut t = lc(&store).create(id).await.unwrap();
        for (i, k) in keys.iter().enumerate() {
            if i % 3 == 0 {
                tests::apply(&mut t, &mut m, vec![(k.clone(), Some(format!("v0-{i}").into_bytes()))]).await;
            }
        }
        for i in 0..40 {
            tests::apply(&mut t, &mut m, vec![(a_key(i), Some(format!("{}-{i}", std::str::from_utf8(ERASED).unwrap()).into_bytes()))]).await;
        }
        let _ = crate::lifecycle::compact_tenant(&store, &store, id).await;
        t.abandon().await;
        ENABLED.store(true, Relaxed);
        let mut t = lc(&store).wake(id).await.unwrap();
        t.upgrade().await.unwrap();
        begin(&mut t).await;
        m.insert(crate::envelope::MARKER.to_vec(), b"E1".to_vec());
        migrate::build_step(&store, id, 1, &run_opts(), None).await.unwrap();
        if erase {
            erase_a(&mut t).await;
            for i in 0..40 {
                m.remove(&a_key(i));
            }
        }
        let served = t.fx_view().map(|v| v.version);
        // writes, L0 SSTs, today's pass (as the maintenance worker runs it)
        let skipped0 = migrate::PASSES_SKIPPED.load(Relaxed);
        for j in 0..400 {
            let k = keys[rng.gen_range(0..keys.len())].clone();
            tests::apply(&mut t, &mut m, vec![(k, Some(format!("w1-{j}-{}", "z".repeat(rng.gen_range(10..120))).into_bytes()))]).await;
            if j % 25 == 0 && std::env::var("FX_RV8_NOPASS").is_err() {
                let _ = crate::lifecycle::compact_tenant(&store, &store, id).await;
            }
        }
        let skipped = migrate::PASSES_SKIPPED.load(Relaxed) - skipped0;
        // the server's next write starts the compactor when the writer serves a version (drop off: building)
        let mut c = None;
        if t.fx_view().is_some() {
            let drop = t.fx_migration().is_some_and(|m| m.drop_allowed());
            let cc = super::compactor::Compactor::start(&mut t, Arc::clone(&store), knobs(drop), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
            cc.run();
            c = Some(cc);
        }
        for j in 0..300 {
            let k = keys[rng.gen_range(0..keys.len())].clone();
            tests::apply(&mut t, &mut m, vec![(k, Some(format!("w2-{j}").into_bytes()))]).await;
        }
        if let Some(c) = &c {
            let _ = c.drain(false).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let bad_building = full_check(&mut t, &m).await;
        if let Some(c) = c.take() {
            c.stop().await;
        }
        t.fx_detach();
        t.abandon().await;
        teardown();
        eprintln!("erasure during the build: {erase}; writer served version {served:?} while building; today's passes skipped {skipped}; acknowledged rows missing or different while building: {bad_building} of {}", m.len());
        out.push((erase, bad_building));
    }
    assert_eq!(out[0].1, 0, "the control (no erasure) must read right");
    assert_eq!(out[1].1, 0, "an erasure during the build: acknowledged rows missing or wrong");
}

async fn flush_mem(t: &Tenant) {
    if let Some(db) = t.fx_writer_handle() {
        db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
    }
}

/// A restore point (a checkpoint, as PITR's agent takes them: paired with our manifest when sealed runs are on, plain
/// otherwise), and the model at that moment.
async fn restore_point(store: &Arc<dyn ObjectStore>, id: &str, t: &Tenant, m: &std::collections::BTreeMap<Vec<u8>, Vec<u8>>, what: &str, points: &mut Vec<(String, uuid::Uuid, std::collections::BTreeMap<Vec<u8>, Vec<u8>>)>) {
    flush_mem(t).await;
    let admin = slatedb::admin::Admin::builder(crate::lifecycle::tenant_path(id), Arc::clone(store)).build();
    let opts = slatedb::config::CheckpointOptions { lifetime: Some(Duration::from_secs(3600)), source: None, name: Some(format!("ret:{what}")) };
    let (cp, pair) = pair::checkpoint_paired(store, id, || async { Ok(admin.create_detached_checkpoint(&opts).await?) }).await.unwrap();
    eprintln!("restore point {what}: sealed runs {}, paired with version {:?}", ENABLED.load(Relaxed), pair.as_ref().and_then(|p| p.version));
    points.push((what.to_string(), cp.id, m.clone()));
}

/// PITR, export and forks read a database's past through `PairedReader` (rule 20). Restore points are taken at each
/// phase of a migration: on today's engine before it, while it builds, in the window, after a rollback to E0, while
/// the second generation builds. Then, on release E1 with the second generation building, every point is read back
/// as PITR reads it and compared with the model at that moment. The window of restore points is the customer's PITR
/// promise: a migration must not shorten it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_points_across_the_migration_read_their_state() {
    use rand::{Rng, SeedableRng};
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "rv8-pitr";
    let mut rng = rand::rngs::StdRng::seed_from_u64(11);
    let keys = tests::universe();
    let mut m = std::collections::BTreeMap::new();
    let mut points = Vec::new();
    let mut writes = |n: usize, tag: &str, rng: &mut rand::rngs::StdRng| -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        (0..n).map(|j| (keys[rng.gen_range(0..keys.len())].clone(), if rng.gen_bool(0.15) { None } else { Some(format!("{tag}-{j}").into_bytes()) })).collect()
    };
    // today's engine
    let mut t = lc(&store).create(id).await.unwrap();
    for op in writes(600, "e-1", &mut rng) {
        tests::apply(&mut t, &mut m, vec![op]).await;
    }
    let _ = crate::lifecycle::compact_tenant(&store, &store, id).await;
    restore_point(&store, id, &t, &m, "today's engine, before the migration", &mut points).await;
    t.abandon().await;
    // E1: the migration builds
    ENABLED.store(true, Relaxed);
    let mut t = lc(&store).wake(id).await.unwrap();
    t.upgrade().await.unwrap();
    begin(&mut t).await;
    m.insert(crate::envelope::MARKER.to_vec(), b"E1".to_vec());
    migrate::build_step(&store, id, 1, &run_opts(), None).await.unwrap();
    for op in writes(200, "b1", &mut rng) {
        tests::apply(&mut t, &mut m, vec![op]).await;
    }
    restore_point(&store, id, &t, &m, "building", &mut points).await;
    while !migrate::load(&store, id).await.unwrap().unwrap().built {
        migrate::build_step(&store, id, 0, &run_opts(), None).await.unwrap();
    }
    assert!(t.fx_migrate_finish(&run_opts(), None).await.unwrap().is_some());
    let c = super::compactor::Compactor::start(&mut t, Arc::clone(&store), knobs(false), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    c.run();
    for op in writes(300, "w1", &mut rng) {
        tests::apply(&mut t, &mut m, vec![op]).await;
    }
    let _ = c.drain(false).await;
    restore_point(&store, id, &t, &m, "in the window", &mut points).await;
    for op in writes(200, "w2", &mut rng) {
        tests::apply(&mut t, &mut m, vec![op]).await;
    }
    let _ = c.drain(false).await;
    c.stop().await;
    t.fx_detach();
    t.abandon().await;
    // the operator deploys E0: its first writer open rolls the migration back
    ENABLED.store(false, Relaxed);
    let mut t = lc(&store).wake(id).await.unwrap();
    t.upgrade().await.unwrap();
    for op in writes(200, "e0", &mut rng) {
        tests::apply(&mut t, &mut m, vec![op]).await;
    }
    restore_point(&store, id, &t, &m, "E0 after the rollback", &mut points).await;
    t.abandon().await;
    // E1 again: the second generation begins
    ENABLED.store(true, Relaxed);
    let mut t = lc(&store).wake(id).await.unwrap();
    t.upgrade().await.unwrap();
    begin(&mut t).await;
    for op in writes(100, "g2", &mut rng) {
        tests::apply(&mut t, &mut m, vec![op]).await;
    }
    restore_point(&store, id, &t, &m, "generation 2 building", &mut points).await;
    // every point, read the way PITR's staging clone reads it (the point's checkpoint, through its pair)
    let mut bad = Vec::new();
    for (what, cp, snap) in &points {
        let r = pair::PairedReader::open_checkpoint(&store, id, *cp, slatedb::config::DbReaderOptions::default()).await;
        match r {
            Err(e) => {
                eprintln!("restore point {what}: REFUSED: {e:#}");
                bad.push(format!("{what}: refused"));
            }
            Ok(r) => {
                let got = tests::as_vec(r.scan(b"", None, false, None, 1 << 20).await.unwrap());
                let got: Vec<(Vec<u8>, Vec<u8>)> = got.into_iter().filter(|(k, _)| k.as_slice() != crate::envelope::MARKER).collect();
                let want: Vec<(Vec<u8>, Vec<u8>)> = snap.iter().filter(|(k, _)| k.as_slice() != crate::envelope::MARKER).map(|(k, v)| (k.clone(), v.clone())).collect();
                let wrong = if got == want { 0 } else { want.iter().filter(|w| !got.contains(w)).count() + got.iter().filter(|g| !want.contains(g)).count() };
                eprintln!("restore point {what}: read through version {:?}: {} rows, {wrong} wrong", r.version(), got.len());
                if wrong > 0 {
                    bad.push(format!("{what}: {wrong} wrong"));
                }
                r.close().await.unwrap();
            }
        }
    }
    t.abandon().await;
    teardown();
    assert!(bad.is_empty(), "restore points the migration broke: {bad:?}");
}
