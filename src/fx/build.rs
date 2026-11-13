//! Sealed and delta runs built offline from a loaded database (compaction 2, step 3). The compactor proper (L0
//! absorption, delta tiers, group rewrites from runs, the drop job, GC) is step 4; this builder exists so the merged
//! reader can be tested on real data, and so the server can publish versions while the judge's suites run
//! (`--sealed-runs N`: a publish every N write batches of a database).
//!
//! A build reads SlateDB's merged state at one durable cut X (a snapshot taken right after a WAL flush, so every row at
//! or below X is in a durable WAL object or SST: a crash cannot take SlateDB's sequence numbers back below a published
//! X_d). It writes:
//! - a delta run with every row whose seq is in (X_d of the current version, X], delete envelopes included;
//! - for each group it reseals, a new sealed run with the group's rows at X, envelopes dropped (rule 14), and X_g = X.
//!
//! The new version has X_d = X. Groups not resealed keep their X_g and sealed run; the delta runs answer for them above
//! X_g. Delta runs whose every row is at or below the smallest X_g are no longer read and leave the version. In this
//! step SlateDB keeps every row (no drop), which is what makes reading SlateDB here equal to reading our runs plus the
//! delta rows; step 4's reseal reads the group's sealed run and delta rows instead (rule 14).
//!
//! Groups are cut by bytes when the first version is built and stay fixed (split and merge are step 4).

use super::manifest::{self, hex, GroupLine, RunLine, Version};
use super::view::View;
use crate::runfmt::{self, CustomerKind, RunOptions, RunWriter};
use anyhow::{bail, ensure, Result};
use object_store::path::Path;
use object_store::ObjectStore;
use std::sync::Arc;

/// Which groups a build reseals.
#[derive(Clone, Debug)]
pub enum Reseal {
    /// Every group (and the delta runs leave the version).
    All,
    /// None: only a delta run.
    None,
    /// These group indexes.
    Some(Vec<usize>),
}

#[derive(Clone, Debug)]
pub struct BuildOpts {
    pub reseal: Reseal,
    /// Raw bytes per group when the first version cuts the groups (the plan's 1 GB).
    pub group_bytes: u64,
    pub sealed: RunOptions,
    pub delta: RunOptions,
}

impl Default for BuildOpts {
    fn default() -> Self {
        BuildOpts {
            reseal: Reseal::All,
            group_bytes: 1 << 30,
            // the server's keys (layout v2 with keyenc components, step 6; runs written with V2 keep reading as V2)
            sealed: RunOptions::sealed(CustomerKind::V2K),
            delta: RunOptions { block_bytes: runfmt::MIN_BLOCK, part_bytes: runfmt::DEFAULT_PART, customers: CustomerKind::V2K, codec: runfmt::BlockCodec::Lz4, derived: false },
        }
    }
}

/// What a build wrote.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct BuildReport {
    pub version: u64,
    pub x_d: u64,
    pub rows_scanned: u64,
    pub delta_rows: u64,
    pub sealed_rows: u64,
    pub runs_written: u64,
    pub groups: usize,
    pub resealed: usize,
    /// The staged publish's warm-up of the new runs (step 6).
    pub stage: Option<super::counter::StageReport>,
}

/// Versions refused at their create because the database's writer changed under the build (`fence_check`).
pub static FENCED_CREATES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Right before a version is created: the database's SlateDB writer epoch in the store must still be the build's.
/// After a takeover (this process lost its lease and another opened the database as writer) the epoch is higher, and a
/// version built on this writer's old snapshot must not land: the new owner would build its next versions on it, and
/// every row between its X_d and the new owner's would leave every read (review of a60f4fe, rule 8: acknowledged writes
/// lost when a first build finished after the lease loss). A version create already in flight can still land, as after
/// a crash: what is left is that window, not seconds.
/// Negative control of the tests: no check (the code of a60f4fe).
pub static CONTROL_NO_FENCE_CHECK: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The longest time from the start of `fence_check` to the end of the create it guards, in ms: the window in which a
/// takeover can still let this version land (serverStatus `fenceWindowMsMax`).
pub static FENCE_WINDOW_MS_MAX: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

async fn fence_check(store: &Arc<dyn ObjectStore>, id: &str, epoch: Option<u64>) -> Result<()> {
    let Some(e) = epoch else { return Ok(()) };
    if CONTROL_NO_FENCE_CHECK.load(std::sync::atomic::Ordering::Relaxed) {
        return Ok(());
    }
    let admin = slatedb::admin::Admin::builder(crate::lifecycle::tenant_path(id), Arc::clone(store)).build();
    if let Some(m) = admin.read_manifest(None).await? {
        if m.writer_epoch() != e {
            FENCED_CREATES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            anyhow::bail!("database {id}: the writer changed under this build (epoch {e}, now {}): no version created", m.writer_epoch());
        }
    }
    Ok(())
}

pub(crate) fn line(m: &runfmt::RunMeta) -> RunLine {
    RunLine { path: m.path.clone(), size: m.size, id: hex(&m.run_id), min_seq: m.min_seq, max_seq: m.max_seq, first: hex(&m.first_key), last: hex(&m.last_key), entries: m.entries, raw: m.raw_bytes, local: false }
}

/// A run writer opened on its first row (a run with no row is not written).
struct Lazy {
    store: Arc<dyn ObjectStore>,
    path: String,
    opts: RunOptions,
    w: Option<RunWriter>,
    rows: u64,
}

impl Lazy {
    fn new(store: &Arc<dyn ObjectStore>, path: String, opts: &RunOptions) -> Lazy {
        Lazy { store: Arc::clone(store), path, opts: opts.clone(), w: None, rows: 0 }
    }
    async fn add(&mut self, key: &[u8], seq: u64, env: &[u8]) -> Result<()> {
        if self.w.is_none() {
            self.w = Some(RunWriter::new(&self.store, Path::from(self.path.as_str()), self.opts.clone()).await?);
        }
        self.rows += 1;
        self.w.as_mut().unwrap().add(key, seq, env).await
    }
    async fn finish(self) -> Result<Option<RunLine>> {
        match self.w {
            Some(w) => Ok(Some(line(&w.finish().await?))),
            None => Ok(None),
        }
    }
}

/// Tests (fix-write-stalls): a build waits this long right after its snapshot, and right before its version's create, so
/// a test can write, read, close, erase or kill -9 the process while a first build runs off the database's lock. 0: no
/// wait (FENVIX_TEST_BUILD_PAUSE_SNAPSHOT_MS, FENVIX_TEST_BUILD_PAUSE_CREATE_MS in the server).
pub static PAUSE_AFTER_SNAPSHOT_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static PAUSE_BEFORE_CREATE_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

async fn pause(ms: &std::sync::atomic::AtomicU64) {
    let ms = ms.load(std::sync::atomic::Ordering::Relaxed);
    if ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
}

/// The stored envelope of a SlateDB row (rows at or below E_env, and the marker, are plain values).
fn envelope_of(e_env: u64, key: &[u8], seq: u64, value: &[u8]) -> Result<Vec<u8>> {
    if seq <= e_env || key == crate::envelope::MARKER {
        return Ok(runfmt::live(value));
    }
    match value.first() {
        Some(&crate::envelope::LIVE) => Ok(value.to_vec()),
        Some(&crate::envelope::DEAD) if value.len() == 1 => Ok(value.to_vec()),
        _ => bail!("corrupt envelope at seq {seq} (E_env {e_env})"),
    }
}

/// Build the runs of the next version from `db` and publish it. `cur` is the version the writer serves (None: the first
/// build). Returns the published version, or None when there is nothing new to publish. An error leaves only orphan
/// runs (GC's business in step 4), never a half-published version.
pub async fn build_and_publish(db: &slatedb::Db, store: &Arc<dyn ObjectStore>, id: &str, cur: Option<&View>, e_env: u64, epoch: Option<u64>, opts: &BuildOpts, fail: Option<&(dyn Fn(&str) -> Result<()> + Sync)>) -> Result<Option<(Version, BuildReport)>> {
    // fix-write-stalls: the L0 SSTs in the writer's view before the cut. Each was flushed before the snapshot below, so
    // every row it holds has a seq at or below the cut: the first version lists them as absorbed (`absorbed_l0`, with the
    // cut as their highest seq, an upper bound), and a compactor starting on it reads none of them again
    let l0_before: Vec<String> = if cur.is_none() { db.manifest().l0().iter().map(|v| v.id.to_string()).collect() } else { Vec::new() };
    // the cut: a snapshot at the last committed seq, then a WAL flush, which makes every write committed so far durable,
    // so every seq at or below the cut is durable before anything of the version is written. The order matters off the
    // database's lock (fix-write-stalls, the first build): writes go on, and with the flush first a write staged while
    // the flush was in flight entered the snapshot without being durable; after a crash the next owner reopened SlateDB
    // below X_d and its new writes got seqs at or below X_d, which reads take from our runs only (the reviewer's
    // test_68r: "manifest 1 has X_d 34 but SlateDB's last seq is 3", the database refused for good)
    let snap = db.snapshot().await?;
    let x = snap.seq();
    // c2-fix-hangs: the flush waits for the WAL PUT, which SlateDB retries without end while the store does not answer; a
    // publish under the database's exclusive lock then held every later command on the database (ctl test_31). Bounded by
    // the F6 timeout; nothing of ours is written before this point, so the publish just fails.
    let limit = std::time::Duration::from_millis(super::PUT_TIMEOUT_MS.load(std::sync::atomic::Ordering::Relaxed));
    match tokio::time::timeout(limit, db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::Wal })).await {
        Ok(r) => r?,
        Err(_) => return Err(anyhow::Error::new(super::CutNotDurable(format!("database {id}: the WAL flush of the cut did not end in {limit:?}; nothing published")))),
    }
    pause(&PAUSE_AFTER_SNAPSHOT_MS).await;
    ensure!(e_env <= x, "E_env {e_env} above the cut {x}");
    let (prev_x, reseal_all) = match cur {
        None => (0, true),
        Some(v) => (v.x_d, matches!(opts.reseal, Reseal::All)),
    };
    if let Some(v) = cur {
        ensure!(x >= v.x_d, "the cut {x} is below the current X_d {} (SlateDB lost rows?)", v.x_d);
        if x == v.x_d && matches!(opts.reseal, Reseal::None) {
            return Ok(None);
        }
    }
    let reseal = |gi: usize| -> bool {
        reseal_all
            || match &opts.reseal {
                Reseal::Some(v) => v.contains(&gi),
                _ => false,
            }
    };
    let scan_opts = slatedb::config::ScanOptions { read_ahead_bytes: 4 << 20, max_fetch_tasks: 4, cache_blocks: false, ..Default::default() };
    let mut it = snap.scan_with_options::<std::ops::RangeFull>(.., &scan_opts).await?;
    let mut rep = BuildReport { x_d: x, ..Default::default() };
    let mut delta = if cur.is_some() && x > prev_x && !reseal_all { Some(Lazy::new(store, manifest::run_path(id, "delta", x, epoch), &opts.delta)) } else { None };

    // groups: from the current version, or cut by bytes on the way
    let mut groups: Vec<GroupLine> = cur.map(|v| v.meta.groups.clone()).unwrap_or_default();
    let mut next_group_id = cur.map(|v| v.meta.next_group_id).unwrap_or(0);
    let bounds: Vec<Vec<u8>> = cur.map(|v| v.groups.iter().map(|g| g.lo.clone()).collect()).unwrap_or_default();
    let mut gi = 0usize;
    let mut g_bytes = 0u64;
    if cur.is_none() {
        groups.push(GroupLine { id: next_group_id, lo: String::new(), x_g: x, sealed: None, unsealed: 0, unsealed_since_ms: 0, sealed_ms: 0 });
        next_group_id += 1;
    }
    let path_of = |gid: u32| manifest::run_path(id, &format!("g{gid}"), x, epoch);
    let mut sealed: Option<Lazy> = if reseal(0) { Some(Lazy::new(store, path_of(groups[0].id), &opts.sealed)) } else { None };
    let mut finished: Vec<(usize, Option<RunLine>)> = Vec::new();

    while let Some(kv) = it.next().await? {
        rep.rows_scanned += 1;
        let env = envelope_of(e_env, &kv.key, kv.seq, &kv.value)?;
        // which group the key is in; leaving a group finishes its sealed run
        let target = match cur {
            Some(_) => bounds.partition_point(|lo| &lo[..] <= &kv.key[..]).saturating_sub(1),
            None => {
                if g_bytes >= opts.group_bytes {
                    groups.push(GroupLine { id: next_group_id, lo: hex(&kv.key), x_g: x, sealed: None, unsealed: 0, unsealed_since_ms: 0, sealed_ms: 0 });
                    next_group_id += 1;
                    g_bytes = 0;
                    groups.len() - 1
                } else {
                    gi
                }
            }
        };
        while gi < target {
            if let Some(s) = sealed.take() {
                finished.push((gi, s.finish().await?));
            }
            gi += 1;
            if reseal(gi) {
                sealed = Some(Lazy::new(store, path_of(groups[gi].id), &opts.sealed));
            }
        }
        g_bytes += (kv.key.len() + kv.value.len()) as u64;
        if kv.seq > prev_x {
            if let Some(d) = delta.as_mut() {
                d.add(&kv.key, kv.seq, &env).await?;
                rep.delta_rows += 1;
            }
        }
        if let Some(s) = sealed.as_mut() {
            if env.first() != Some(&runfmt::DELETED) {
                s.add(&kv.key, kv.seq, &env).await?;
                rep.sealed_rows += 1;
            }
        }
    }
    if let Some(s) = sealed.take() {
        finished.push((gi, s.finish().await?));
    }
    // groups after the last key that are resealed become empty at X
    for g in gi + 1..groups.len() {
        if reseal(g) {
            finished.push((g, None));
        }
    }
    drop(it);
    drop(snap);
    let delta_line = match delta {
        Some(d) => d.finish().await?,
        None => None,
    };
    for (g, l) in finished {
        rep.runs_written += l.is_some() as u64;
        rep.resealed += 1;
        groups[g].x_g = x;
        groups[g].sealed = l;
        groups[g].unsealed = 0;
        groups[g].unsealed_since_ms = 0;
        groups[g].sealed_ms = manifest::now_ms();
    }
    rep.runs_written += delta_line.is_some() as u64;
    if let Some(f) = fail {
        f("after_runs")?;
    }
    let mut delta_runs: Vec<RunLine> = delta_line.into_iter().collect();
    if !reseal_all {
        delta_runs.extend(cur.map(|v| v.meta.delta.clone()).unwrap_or_default());
    }
    let min_xg = groups.iter().map(|g| g.x_g).min().unwrap_or(x);
    delta_runs.retain(|d| d.max_seq > min_xg);
    let version = match cur {
        Some(v) => v.version + 1,
        None => manifest::list_versions(store, id).await?.last().map_or(1, |n| n + 1),
    };
    let absorbed_l0 = l0_before.into_iter().map(|id| (id, x)).collect();
    let v = Version { format: manifest::FORMAT, version, kind: "publish".into(), writer_epoch: epoch, x_d: x, e_env: Some(e_env), published_ms: manifest::now_ms(), groups, delta: delta_runs, next_group_id, absorbed_l0 };
    rep.version = version;
    rep.groups = v.groups.len();
    // the staged publish (step 6, `counter.rs`): before the version that retires runs becomes current, warm the new runs'
    // ranges of what this process holds of the retired ones (bounded wait). Step 4's compactor calls the same function
    // in the same place: after its runs are written, before the conditional create.
    if let Some(old) = cur {
        let staged = View::new(Arc::clone(store), v.clone(), Some(old))?;
        rep.stage = Some(super::counter::stage(old, &staged).await);
    }
    pause(&PAUSE_BEFORE_CREATE_MS).await;
    let fence_t0 = std::time::Instant::now();
    fence_check(store, id, epoch).await?;
    let created = manifest::create(store, id, &v).await;
    FENCE_WINDOW_MS_MAX.fetch_max(fence_t0.elapsed().as_millis() as u64, std::sync::atomic::Ordering::Relaxed);
    let created = match created {
        Ok(c) => c,
        // the outcome is unknown (an error, a timeout: the PUT may still land); the caller fences the slot this version
        // targeted, with this version when the store is still empty (review round 2 of step 3)
        Err(e) => return Err(anyhow::Error::new(super::UnknownPublish { version: Box::new(v), msg: format!("{e:#}") })),
    };
    if !created {
        return Err(anyhow::Error::new(super::LostRace(format!("publish of version {version} of {id} lost the race: another publisher wrote it; this build's runs are orphans"))));
    }
    Ok(Some((v, rep)))
}

// ---------------------------------------------------------------------------------------------------------------------
// Generated data (the step 9 ballast generator, `mongo-server --ballast`): rows that never went through SlateDB.
//
// The generator runs the server's own insert code (documents, index entries, locators, insertion order, counters) in the
// server's process, but instead of committing the batches it keeps their rows: the rows of the customer space (`u/`,
// layout v2) come out of it in key order, customer after customer, and are written straight into sealed runs while it
// runs ([`GroupWriter`]); every other row (the shared keyspace: locators, indexes not led by the customer, insertion
// order, counters) is sorted on local disk and handed over at the end ([`SortedRows`]). [`publish_generated`] then merges
// those with SlateDB's own rows (the catalog the server wrote through its normal commands: collections, index
// definitions, the envelope marker) into the groups before and after the customer space, and publishes one version with
// X_d at SlateDB's durable cut. Every generated row carries the seq of the cut taken before the generation started
// (`x0`), at or below X_d: it reads exactly like a row SlateDB held at X_d, absorbed into a sealed run.

/// Writes sealed runs of consecutive groups from rows given in strictly increasing key order, cutting a new group every
/// `group_bytes` raw bytes. Groups get ids from `first_id` up.
pub struct GroupWriter {
    store: Arc<dyn ObjectStore>,
    id: String,
    x: u64,
    epoch: Option<u64>,
    opts: BuildOpts,
    next_id: u32,
    groups: Vec<GroupLine>,
    cur: Option<Lazy>,
    g_bytes: u64,
    last: Option<Vec<u8>>,
    /// The first group starts at the empty key (the version's first group).
    first_at_empty: bool,
    pub rows: u64,
    pub bytes: u64,
}

impl GroupWriter {
    pub fn new(store: &Arc<dyn ObjectStore>, id: &str, x: u64, epoch: Option<u64>, opts: &BuildOpts, first_id: u32, first_at_empty: bool) -> GroupWriter {
        GroupWriter { store: Arc::clone(store), id: id.to_string(), x, epoch, opts: opts.clone(), next_id: first_id, groups: Vec::new(), cur: None, g_bytes: 0, last: None, first_at_empty, rows: 0, bytes: 0 }
    }

    pub async fn add(&mut self, key: &[u8], seq: u64, env: &[u8]) -> Result<()> {
        if let Some(l) = &self.last {
            ensure!(&l[..] < key, "generated rows out of order: {} after {}", hex(key), hex(l));
        }
        ensure!(seq <= self.x, "a generated row at seq {seq} above the cut {}", self.x);
        if self.cur.is_none() || self.g_bytes >= self.opts.group_bytes {
            if let Some(s) = self.cur.take() {
                let l = s.finish().await?;
                self.groups.last_mut().unwrap().sealed = l;
            }
            let lo = if self.groups.is_empty() && self.first_at_empty { String::new() } else { hex(key) };
            // sealed now, nothing absorbed above X_g yet (step 4's compactor reads these fields)
            self.groups.push(GroupLine { id: self.next_id, lo, x_g: self.x, sealed: None, unsealed: 0, unsealed_since_ms: 0, sealed_ms: manifest::now_ms() });
            self.cur = Some(Lazy::new(&self.store, manifest::run_path(&self.id, &format!("g{}", self.next_id), self.x, self.epoch), &self.opts.sealed));
            self.next_id += 1;
            self.g_bytes = 0;
        }
        self.g_bytes += (key.len() + env.len()) as u64;
        self.bytes += (key.len() + env.len()) as u64;
        self.rows += 1;
        self.last = Some(key.to_vec());
        self.cur.as_mut().unwrap().add(key, seq, env).await
    }

    /// The last key added, if any.
    pub fn last_key(&self) -> Option<&[u8]> {
        self.last.as_deref()
    }

    pub async fn finish(mut self) -> Result<Vec<GroupLine>> {
        if let Some(s) = self.cur.take() {
            let l = s.finish().await?;
            self.groups.last_mut().unwrap().sealed = l;
        }
        if self.groups.is_empty() && self.first_at_empty {
            // a version always has a group at the empty key
            self.groups.push(GroupLine { id: self.next_id, lo: String::new(), x_g: self.x, sealed: None, unsealed: 0, unsealed_since_ms: 0, sealed_ms: manifest::now_ms() });
        }
        Ok(self.groups)
    }
}

/// Generated rows sorted by key, one row per key (the newest write of each key), as (key, stored envelope).
pub trait SortedRows: Send {
    fn next_row(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>>;
}

/// What the generator hands to [`publish_generated`].
pub struct Generated {
    /// The cut taken before the generation (every generated row carries it as its seq).
    pub x0: u64,
    /// The groups of the customer space, already written in key order, and the last key written in them.
    pub direct: Vec<GroupLine>,
    pub direct_last: Option<Vec<u8>>,
    /// Every other generated row; a key SlateDB also holds takes the generated value (it is the newer write).
    pub sorted: Box<dyn SortedRows>,
}

/// Merge SlateDB's rows at a durable cut with the generated rows and publish the database's first version with runs.
/// The database must have no run yet (a ballast database is generated whole, once). `next_version` is the slot to create
/// (the newest version in the store plus one: fence versions written at open come before it).
#[allow(clippy::too_many_arguments)]
pub async fn publish_generated(db: &slatedb::Db, store: &Arc<dyn ObjectStore>, id: &str, cur: Option<&View>, e_env: u64, epoch: Option<u64>, opts: &BuildOpts, mut gen: Generated) -> Result<(Version, BuildReport)> {
    if let Some(v) = cur {
        ensure!(v.meta.delta.is_empty() && v.meta.groups.iter().all(|g| g.sealed.is_none()), "database {id} already has runs: a ballast database is generated whole, into an empty database");
    }
    db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::Wal }).await?;
    let snap = db.snapshot().await?;
    let x = snap.seq();
    ensure!(x >= gen.x0, "the cut {x} is below the cut {} taken before the generation (SlateDB lost rows?)", gen.x0);
    ensure!(e_env <= gen.x0, "E_env {e_env} above the generation's cut {}", gen.x0);
    let direct_lo: Option<Vec<u8>> = match gen.direct.first() {
        Some(g) => Some(manifest::unhex(&g.lo)?),
        None => None,
    };
    let direct_n = gen.direct.len() as u32;
    let mut pre = GroupWriter::new(store, id, x, epoch, opts, 0, true);
    let mut post = GroupWriter::new(store, id, x, epoch, opts, 1 + direct_n + (1 << 20), false);
    let mut rep = BuildReport { x_d: x, ..Default::default() };
    let scan_opts = slatedb::config::ScanOptions { read_ahead_bytes: 4 << 20, max_fetch_tasks: 4, cache_blocks: false, ..Default::default() };
    let mut it = snap.scan_with_options::<std::ops::RangeFull>(.., &scan_opts).await?;
    let mut s_row: Option<(Vec<u8>, u64, Vec<u8>)> = None;
    let mut g_row = gen.sorted.next_row()?;
    let mut s_done = false;
    loop {
        if s_row.is_none() && !s_done {
            match it.next().await? {
                Some(kv) => {
                    rep.rows_scanned += 1;
                    let env = envelope_of(e_env, &kv.key, kv.seq, &kv.value)?;
                    s_row = Some((kv.key.to_vec(), kv.seq, env));
                }
                None => s_done = true,
            }
        }
        // the smaller key first; on equal keys the generated row wins and SlateDB's is skipped
        let (take_gen, same) = match (&s_row, &g_row) {
            (None, None) => break,
            (None, Some(_)) => (true, false),
            (Some(_), None) => (false, false),
            (Some(s), Some(g)) => (g.0 <= s.0, g.0 == s.0),
        };
        if same {
            s_row = None;
        }
        let (k, seq, env) = if take_gen {
            let (k, v) = g_row.take().unwrap();
            g_row = gen.sorted.next_row()?;
            (k, gen.x0, v)
        } else {
            s_row.take().unwrap()
        };
        if env.first() == Some(&runfmt::DELETED) {
            continue; // a sealed run drops delete envelopes (rule 14)
        }
        let below = direct_lo.as_ref().map_or(true, |lo| &k < lo);
        if below {
            pre.add(&k, seq, &env).await?;
        } else {
            let last = gen.direct_last.as_deref().unwrap_or(&[]);
            ensure!(&k[..] > last, "row {} falls inside the generated customer space", hex(&k));
            post.add(&k, seq, &env).await?;
        }
        rep.sealed_rows += 1;
    }
    drop(it);
    drop(snap);
    rep.sealed_rows += gen.direct.iter().filter_map(|g| g.sealed.as_ref()).map(|r| r.entries).sum::<u64>();
    let mut groups = pre.finish().await?;
    // the customer-space groups follow, renumbered after the first ones; then the groups after the customer space
    let mut next_id = groups.iter().map(|g| g.id).max().map_or(0, |m| m + 1);
    for mut g in std::mem::take(&mut gen.direct) {
        g.id = next_id;
        next_id += 1;
        groups.push(g);
    }
    groups.extend(post.finish().await?);
    for w in groups.windows(2) {
        ensure!(manifest::unhex(&w[0].lo)? < manifest::unhex(&w[1].lo)?, "groups out of order");
    }
    for g in &groups {
        ensure!(g.x_g <= x, "group {} at X_g {} above X_d {x}", g.id, g.x_g);
    }
    let next_group_id = groups.iter().map(|g| g.id).max().map_or(0, |m| m + 1);
    rep.runs_written = groups.iter().filter(|g| g.sealed.is_some()).count() as u64;
    rep.groups = groups.len();
    rep.resealed = groups.len();
    let version = manifest::list_versions(store, id).await?.last().map_or(1, |n| n + 1);
    let v = Version { format: manifest::FORMAT, version, kind: "publish".into(), writer_epoch: epoch, x_d: x, e_env: Some(e_env), published_ms: manifest::now_ms(), groups, delta: Vec::new(), next_group_id, absorbed_l0: Vec::new() };
    rep.version = version;
    let fence_t0 = std::time::Instant::now();
    fence_check(store, id, epoch).await?;
    let created = manifest::create(store, id, &v).await;
    FENCE_WINDOW_MS_MAX.fetch_max(fence_t0.elapsed().as_millis() as u64, std::sync::atomic::Ordering::Relaxed);
    let created = match created {
        Ok(c) => c,
        Err(e) => return Err(anyhow::Error::new(super::UnknownPublish { version: Box::new(v), msg: format!("{e:#}") })),
    };
    if !created {
        return Err(anyhow::Error::new(super::LostRace(format!("publish of version {version} of {id} lost the race: another publisher wrote it; the generated runs are orphans"))));
    }
    Ok((v, rep))
}
