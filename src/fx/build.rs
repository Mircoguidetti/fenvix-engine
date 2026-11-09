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
            delta: RunOptions { block_bytes: runfmt::MIN_BLOCK, part_bytes: runfmt::DEFAULT_PART, customers: CustomerKind::V2K },
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

fn line(m: &runfmt::RunMeta) -> RunLine {
    RunLine { path: m.path.clone(), size: m.size, id: hex(&m.run_id), min_seq: m.min_seq, max_seq: m.max_seq, first: hex(&m.first_key), last: hex(&m.last_key), entries: m.entries }
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
    // the cut: a WAL flush, then a snapshot at the last committed seq (all durable)
    db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::Wal }).await?;
    let snap = db.snapshot().await?;
    let x = snap.seq();
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
        groups.push(GroupLine { id: next_group_id, lo: String::new(), x_g: x, sealed: None });
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
                    groups.push(GroupLine { id: next_group_id, lo: hex(&kv.key), x_g: x, sealed: None });
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
    let v = Version { format: manifest::FORMAT, version, kind: "publish".into(), writer_epoch: epoch, x_d: x, e_env: Some(e_env), published_ms: manifest::now_ms(), groups, delta: delta_runs, next_group_id };
    rep.version = version;
    rep.groups = v.groups.len();
    // the staged publish (step 6, `counter.rs`): before the version that retires runs becomes current, warm the new runs'
    // ranges of what this process holds of the retired ones (bounded wait). Step 4's compactor calls the same function
    // in the same place: after its runs are written, before the conditional create.
    if let Some(old) = cur {
        let staged = View::new(Arc::clone(store), v.clone(), Some(old))?;
        rep.stage = Some(super::counter::stage(old, &staged).await);
    }
    if !manifest::create(store, id, &v).await? {
        return Err(anyhow::Error::new(super::LostRace(format!("publish of version {version} of {id} lost the race: another publisher wrote it; this build's runs are orphans"))));
    }
    Ok(Some((v, rep)))
}
