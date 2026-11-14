//! sim-test-2 (P-sim-test-2): the scenarios the first simulator does not cover, as new event kinds on the same executor
//! and oracle (nothing of the core changes its meaning; a script of `generate` runs exactly as before).
//!
//! - restore points (`Point`, `PointPrune`) taken as `ctl/pitr.rs` `make_point` takes them: a SlateDB detached checkpoint
//!   paired with a version of ours under the drop lease (`fx/pair.rs`), with the world's version retention at 0, so only
//!   the pair keeps the version and its runs;
//! - restore to a past time (`Mark`, `Restore`): as `pitr::run_locked` stages it (a clone of the newest point at or
//!   before the time, the WAL objects written up to it copied forward, read through the paired version), checked
//!   against the model's state at the mark, then written into the live database through a writer;
//! - fork (`Fork`), clone (runs shared by reference) or copy, as `ctl/fork.rs` `fork_paired` does after its quiesce,
//!   while the other processes (zombies, a takeover) go on writing, compacting and collecting the source;
//! - the ballast generator's parallel publish (`Generate`, `publish_generated_parts`) with several writers;
//! - SlateDB's own GC (`SlateGc`) with the WAL kept as a restore window keeps it;
//! - S3 in trouble (`Storm`): many 503s and timeouts after the write landed, for a stretch of the script;
//! - many small groups (compactor settings 2, values of about 160 bytes: the shape of a 1 TB database, where an erasure
//!   and its scrub touch a few groups of many), and bursts of flushes before a takeover (the WAL replay raise).
//!
//! The oracle gains: every restore staged during the run (and every mark again at the end, after all GC) gives exactly
//! the model's state at its mark; every live restore point's paired version and runs exist; every fork reads, in a
//! fresh process, the model's state at the fork (no acknowledged write missing, nothing written after it).

use super::*;
use crate::fx::pair::{self, Follow, ForkRuns, PairedReader};
use slatedb::admin::Admin;

/// How long the driver waits for a fork, a restore or a generation before it goes on with the script (they stay in
/// flight: the other processes run meanwhile).
pub const LONG_STEP_WAIT: Duration = Duration::from_secs(4);

/// The script's additions (kept for the report; the events say everything else).
#[derive(Clone, Debug, Default)]
pub struct Ext {
    /// 0: a script of `generate`; 2: `generate2`; 3: `ext3::generate3`.
    pub flavor: u8,
    /// sim-test-3: writes go through the replicated log (`ext3.rs`), as the server with `--quaderno`.
    pub log: bool,
    /// sim-test-3: the base's rules for kept records (fb38790e, before fix-takeover-retry 759f9dca), as the engine's own
    /// control switch `MONGO_TEST_KEPT_RETRY_OLD=1` restores them. For directed stories only.
    pub old_kept: bool,
    /// sim-test-3, directed stories only: the oracle also refuses a version on top whose writer epoch is lower than a
    /// version below it (a lost writer's late publish landed; hole G's fence).
    pub strict_versions: bool,
}

/// A value of a write: padded to about 160 bytes with the many-groups settings.
pub fn value(kp: u8, s: String) -> Vec<u8> {
    if kp == 2 {
        let mut s = s;
        s.push('-');
        while s.len() < 160 {
            s.push('x');
        }
        s.into_bytes()
    } else {
        s.into_bytes()
    }
}

pub fn is_ext(e: &Ev) -> bool {
    matches!(e, Ev::Point { .. } | Ev::PointPrune { .. } | Ev::Restore { .. } | Ev::Fork { .. } | Ev::Generate { .. } | Ev::SlateGc { .. } | Ev::EraseRide { .. })
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64
}

pub struct PointRec {
    pub tag: u64,
    pub cp: uuid::Uuid,
    pub manifest_id: u64,
    /// When the checkpoint existed (after its creation returned: a restore to a time before it never uses it).
    pub at_ms: i64,
    pub version: Option<u64>,
    pub pruned: bool,
}

pub struct MarkRec {
    pub tag: u64,
    pub at_ms: i64,
    /// The model at the mark (statuses frozen then).
    pub snap: Model,
}

pub struct ForkRec {
    pub tag: u64,
    pub name: String,
    pub copy: bool,
    /// The model when the fork started (statuses frozen then) and the number of writes of each key when it ended.
    pub start: Model,
    pub end: HashMap<Vec<u8>, usize>,
}

pub struct XState {
    pub profile: Profile,
    pub points: Arc<Mutex<Vec<PointRec>>>,
    pub marks: Arc<Mutex<Vec<MarkRec>>>,
    pub forks: Arc<Mutex<Vec<ForkRec>>>,
    /// Violations seen during the run (a restore that staged a wrong state).
    pub found: Arc<Mutex<Vec<String>>>,
}

impl XState {
    pub fn new(profile: Profile) -> XState {
        XState { profile, points: Default::default(), marks: Default::default(), forks: Default::default(), found: Default::default() }
    }
}

/// S3 in trouble: many 503s, timeouts before, and timeouts after the write landed.
pub fn storm_profile() -> Profile {
    Profile { delay_ppm: 250_000, delay_max_ms: 80, fail_ppm: 60_000, timeout_before_ppm: 20_000, timeout_after_ppm: 60_000, timeout_ms: 300 }
}

fn counts(m: &Model) -> HashMap<Vec<u8>, usize> {
    m.writes.iter().map(|(k, v)| (k.clone(), v.len())).collect()
}

/// The legal answers for `key` in a state taken between `start` and `end`: the last write acknowledged at `start` (or
/// absent), and every later write issued before `end` that did not fail with a definite error (its outcome as `live`
/// knows it now).
fn legal(start: &Model, end: usize, live: &Model, key: &[u8]) -> Vec<Option<Vec<u8>>> {
    let empty = Vec::new();
    let ws = start.writes.get(key).unwrap_or(&empty);
    let lv = live.writes.get(key).unwrap_or(&empty);
    let last = ws.iter().rposition(|w| w.status.load(SeqCst) == ACKED);
    let mut out = vec![last.and_then(|i| ws[i].value.clone())];
    let from = last.map(|i| i + 1).unwrap_or(0);
    for i in from..end.min(lv.len()) {
        if lv[i].status.load(SeqCst) != FAILED_DEFINITE {
            out.push(lv[i].value.clone());
        }
    }
    // writes issued before the last acknowledged one but not ended when it started may be ordered after it
    if let Some(l) = last {
        for w in lv[..l.min(lv.len())].iter().filter(|w| w.status.load(SeqCst) != FAILED_DEFINITE && w.overlaps(ws[l].issue)) {
            out.push(w.value.clone());
        }
    }
    out
}

/// Check reads of a state taken between `start` and `end` (counts) against the model.
fn check_at(what: &str, reads: &BTreeMap<Vec<u8>, Result<Option<Vec<u8>>, String>>, start: &Model, end: &HashMap<Vec<u8>, usize>, live: &Model) -> Vec<String> {
    let mut v = Vec::new();
    for (k, got) in reads {
        let got = match got {
            Ok(g) => g,
            Err(e) => {
                v.push(format!("{what}: read error on {}: {e}", show(k)));
                continue;
            }
        };
        let ok = legal(start, end.get(k).copied().unwrap_or(0), live, k);
        if ok.contains(got) {
            continue;
        }
        let empty = Vec::new();
        let ws = start.writes.get(k).unwrap_or(&empty);
        let last = ws.iter().rposition(|w| w.status.load(SeqCst) == ACKED);
        let expected = match last {
            Some(i) => format!("{} (event {}, slot {})", show_v(&Ok(ws[i].value.clone())), ws[i].eid, ws[i].slot),
            None => "absent".into(),
        };
        let lv = live.writes.get(k).unwrap_or(&empty);
        let later = lv.iter().enumerate().find(|(_, w)| w.value.as_ref() == got.as_ref() && got.is_some()).map(|(i, w)| (i, w.eid));
        let why = match later {
            Some((i, e)) if i >= end.get(k).copied().unwrap_or(0) => format!(" [a write issued after the point in time is read: event {e}]"),
            Some((_, e)) => format!(" [an older write is read: event {e}]"),
            None if got.is_none() && last.is_some() => " [acknowledged write lost]".into(),
            None => String::new(),
        };
        v.push(format!("{what}: {} expected {expected}, read {}{why}", show(k), show_v(&Ok(got.clone()))));
    }
    v
}

/// Every run a version lists exists in the store (a missing one: a live run deleted).
async fn runs_missing(raw: &Arc<dyn ObjectStore>, id: &str, n: u64) -> Result<Vec<String>, String> {
    let v = manifest::load(raw, id, n).await.map_err(|e| short(&e))?;
    let mut out = Vec::new();
    for path in v.groups.iter().filter_map(|g| g.sealed.as_ref().map(|r| r.path.clone())).chain(v.delta.iter().map(|d| d.path.clone())) {
        if let Err(object_store::Error::NotFound { .. }) = raw.head(&object_store::path::Path::from(path.as_str())).await {
            out.push(path);
        }
    }
    Ok(out)
}

fn wal_id(p: &object_store::path::Path) -> Option<u64> {
    p.filename()?.strip_suffix(".sst")?.parse().ok()
}

/// `pitr::copy_wal_forward`: the WAL objects after the point's replay boundary written at or before `target`, copied
/// into the staging clone (a gap below the first newer object refuses the restore).
async fn copy_wal_forward(store: &Arc<dyn ObjectStore>, src: &str, staging: &str, replay_after: u64, next_wal: u64, target: i64) -> anyhow::Result<usize> {
    use futures::TryStreamExt;
    let src_dir = object_store::path::Path::from(format!("{src}/wal/"));
    let dst_dir = object_store::path::Path::from(format!("{staging}/wal/"));
    let mut objs: Vec<(u64, object_store::path::Path, i64)> = store.list(Some(&src_dir)).try_collect::<Vec<_>>().await?.into_iter().filter_map(|m| Some((wal_id(&m.location)?, m.location, m.last_modified.timestamp_millis()))).collect();
    objs.sort_by_key(|x| x.0);
    let have: std::collections::HashSet<u64> = store.list(Some(&dst_dir)).try_collect::<Vec<_>>().await?.into_iter().filter_map(|m| wal_id(&m.location)).collect();
    let mut expect = replay_after + 1;
    let mut copied = 0;
    let mut newer = false;
    for (id, path, mtime) in objs.into_iter().filter(|x| x.0 > replay_after) {
        anyhow::ensure!(id == expect, "retention gap: WAL object {expect} missing (next {id})");
        if mtime > target {
            newer = true;
            break;
        }
        expect += 1;
        if !have.contains(&id) {
            store.copy(&path, &object_store::path::Path::from(format!("{staging}/wal/{id:020}.sst"))).await?;
            copied += 1;
        }
    }
    anyhow::ensure!(newer || expect >= next_wal, "retention gap: WAL objects {expect}..{next_wal} missing");
    Ok(copied)
}

/// Stage a restore of database `id` to `target` (`pitr::run_locked` up to its diff): the reads of every key of the model
/// through the staged state. `Err` with a refusal (no harm: the server would refuse the restore).
async fn stage(store: &Arc<dyn ObjectStore>, id: &str, pt: (uuid::Uuid, u64), target: i64, staging: &str) -> anyhow::Result<(usize, Option<u64>, BTreeMap<Vec<u8>, Result<Option<Vec<u8>>, String>>)> {
    let src = crate::lifecycle::tenant_path(id);
    let admin = Admin::builder(src.clone(), Arc::clone(store)).build();
    let pm = admin.read_manifest(Some(pt.1)).await?.ok_or_else(|| anyhow::anyhow!("the manifest of the restore point is gone"))?;
    let replay_after = pm.replay_after_wal_id();
    let sadmin = Admin::builder(staging.to_string(), Arc::clone(store)).build();
    let mut spec = slatedb::CloneSourceSpec::new(src.clone());
    spec.checkpoint = Some(pt.0);
    sadmin.create_clone_builder_from_source(spec).build().await?;
    let next_wal = admin.read_manifest(None).await?.map(|m| m.next_wal_sst_id()).unwrap_or(0);
    let copied = copy_wal_forward(store, &src, staging, replay_after, next_wal, target).await?;
    let mut o = slatedb::config::DbReaderOptions::default();
    o.manifest_poll_interval = Duration::from_secs(3600);
    let past = PairedReader::open_follow(store, id, staging.to_string(), Follow::PairOf(pt.0), o).await?;
    let version = past.version();
    let mut reads = BTreeMap::new();
    for c in 0..CUSTOMERS {
        for i in 0..PER_CUSTOMER {
            let k = ckey(c, i);
            let r = past.get(&k).await.map(|v| v.map(|b| b.to_vec())).map_err(|e| short(&e));
            reads.insert(k, r);
        }
    }
    let _ = past.close().await;
    Ok((copied, version, reads))
}

struct VecRows(std::vec::IntoIter<(Vec<u8>, Vec<u8>)>);
impl crate::fx::build::SortedRows for VecRows {
    fn next_row(&mut self) -> anyhow::Result<Option<(Vec<u8>, Vec<u8>)>> {
        Ok(self.0.next())
    }
}

impl World {
    /// A point in time to restore to: the model now. A short pause on each side keeps the WAL objects of writes issued
    /// after it off its millisecond.
    pub(super) async fn mark(&mut self, tag: u64) {
        tokio::time::sleep(Duration::from_millis(3)).await;
        let at = now_ms();
        let snap = self.model.lock().unwrap().snapshot();
        self.x.marks.lock().unwrap().push(MarkRec { tag, at_ms: at, snap });
        tokio::time::sleep(Duration::from_millis(3)).await;
        self.note(format!("mark {tag} at {at}"));
    }

    pub(super) fn storm(&mut self, on: bool) {
        *self.net.profile.lock().unwrap() = if on { storm_profile() } else { self.x.profile };
        self.note(format!("storm {}", if on { "on" } else { "off" }));
    }

    pub(super) fn ext_op(&mut self, eid: u32, e: Ev) -> futures::future::BoxFuture<'static, String> {
        let p = e.slot().unwrap();
        let id = self.id.clone();
        let s = &self.slots[p];
        let (store, lc, tl, compl) = (Arc::clone(&s.store), Arc::clone(&s.lc), Arc::clone(&s.t), Arc::clone(&s.comp));
        let raw = Arc::clone(&self.raw);
        let model = Arc::clone(&self.model);
        let (points, marks, forks, found) = (Arc::clone(&self.x.points), Arc::clone(&self.x.marks), Arc::clone(&self.x.forks), Arc::clone(&self.x.found));
        let created = self.created;
        let seed = self.net.seed;
        let kp = self.knobs;
        Box::pin(async move {
            if !created {
                return "no database yet (skipped)".into();
            }
            match e {
                Ev::Point { tag, .. } => {
                    let admin = Admin::builder(crate::lifecycle::tenant_path(&id), Arc::clone(&store)).build();
                    let opts = slatedb::config::CheckpointOptions { lifetime: Some(Duration::from_secs(3600)), source: None, name: Some(format!("ret:{}:sim{tag}", now_ms())) };
                    match pair::checkpoint_paired(&store, &id, || async { Ok(admin.create_detached_checkpoint(&opts).await?) }).await {
                        Ok((cp, pr)) => {
                            let version = pr.as_ref().and_then(|p| p.version);
                            let at = now_ms();
                            points.lock().unwrap().push(PointRec { tag, cp: cp.id, manifest_id: cp.manifest_id, at_ms: at, version, pruned: false });
                            format!("restore point {tag}: checkpoint {} (manifest {}) paired with version {version:?} at {at}", cp.id, cp.manifest_id)
                        }
                        Err(e) => format!("restore point {tag} failed: {}", short(&e)),
                    }
                }
                Ev::PointPrune { tag, .. } => {
                    let cp = points.lock().unwrap().iter().find(|x| x.tag == tag && !x.pruned).map(|x| x.cp);
                    let Some(cp) = cp else { return format!("no restore point {tag} (skipped)") };
                    let admin = Admin::builder(crate::lifecycle::tenant_path(&id), Arc::clone(&store)).build();
                    if let Err(e) = admin.delete_checkpoint(cp).await {
                        return format!("prune of point {tag} failed: {e}");
                    }
                    // the checkpoint is gone: no restore uses the point any more, whatever happens to its pair
                    for x in points.lock().unwrap().iter_mut().filter(|x| x.tag == tag) {
                        x.pruned = true;
                    }
                    match pair::delete_pair(&store, &id, &cp).await {
                        Ok(()) => format!("restore point {tag} pruned"),
                        Err(e) => format!("restore point {tag} pruned, its pair left: {}", short(&e)),
                    }
                }
                Ev::Restore { p, mark, apply } => {
                    let m = marks.lock().unwrap().iter().find(|x| x.tag == mark).map(|x| (x.at_ms, x.snap.snapshot()));
                    let Some((target, snap)) = m else { return format!("no mark {mark} (skipped)") };
                    let pt = points.lock().unwrap().iter().filter(|x| !x.pruned && x.at_ms <= target).max_by_key(|x| x.at_ms).map(|x| (x.cp, x.manifest_id, x.tag, x.version));
                    let Some((cp, mid, ptag, pver)) = pt else { return format!("restore to mark {mark}: no restore point before it (skipped)") };
                    let staging = format!("control/staging/{id}/{eid}");
                    let staged = stage(&store, &id, (cp, mid), target, &staging).await;
                    let (copied, version, reads) = match staged {
                        Ok(x) => x,
                        Err(e) => return format!("restore to mark {mark} refused while staging (point {ptag}): {}", short(&e)),
                    };
                    let end = counts(&snap);
                    let live = model.lock().unwrap().snapshot();
                    let errs: Vec<&String> = reads.values().filter_map(|r| r.as_ref().err()).collect();
                    if !errs.is_empty() {
                        // the server's diff fails on a read error: no apply. A run the point's version lists that no
                        // longer exists is a loss all the same (the restore point can never be used again).
                        let gone = match pver {
                            Some(n) => runs_missing(&raw, &id, n).await,
                            None => Ok(Vec::new()),
                        };
                        let msg = format!("restore to mark {mark} (point {ptag}, version {pver:?}): {} read errors, first {}", errs.len(), errs[0]);
                        match gone {
                            Ok(g) if !g.is_empty() => {
                                found.lock().unwrap().push(format!("{msg}; the point's version lists {} runs that no longer exist, first {}", g.len(), g[0]));
                                return format!("{msg} [RUNS OF A LIVE RESTORE POINT DELETED]");
                            }
                            Err(e) => {
                                found.lock().unwrap().push(format!("{msg}; the point's version {pver:?} is unreadable: {e}"));
                                return format!("{msg} [VERSION OF A LIVE RESTORE POINT UNREADABLE]");
                            }
                            _ => return format!("{msg} (not applied)"),
                        }
                    }
                    let bad = check_at(&format!("restore to mark {mark} (point {ptag}, version {version:?}, {copied} WAL objects forward)"), &reads, &snap, &end, &live);
                    let nbad = bad.len();
                    found.lock().unwrap().extend(bad);
                    if !apply {
                        return format!("restore checked: mark {mark}, point {ptag}, version {version:?}, {copied} WAL objects forward, {nbad} wrong");
                    }
                    if nbad > 0 {
                        return format!("restore staged a wrong state ({nbad} keys): not applied");
                    }
                    // the apply: the staged state written into the live database through this process's writer
                    let Ok(mut g) = tl.try_lock() else { return "restore checked; apply: busy (skipped)".into() };
                    let Some(t) = g.as_mut() else { return "restore checked; apply: not open (skipped)".into() };
                    if !t.is_writer() {
                        return "restore checked; apply: not a writer (skipped)".into();
                    }
                    let kv: Vec<(Vec<u8>, Option<Vec<u8>>)> = reads.into_iter().map(|(k, r)| (k, r.unwrap())).collect();
                    let st = model.lock().unwrap().issue(eid, p, &kv);
                    t.fx_sync();
                    let batch: Vec<Op> = kv.iter().map(|(k, v)| match v {
                        Some(v) => Op::Put(k.clone(), v.clone()),
                        None => Op::Delete(k.clone()),
                    }).collect();
                    match t.apply(batch).await {
                        Ok(()) => {
                            st.store(ACKED, SeqCst);
                            format!("restore checked and applied: mark {mark}, point {ptag}, version {version:?}, {copied} WAL objects forward")
                        }
                        Err(e) => {
                            st.store(FAILED, SeqCst);
                            format!("restore checked; apply failed: {}", short(&e))
                        }
                    }
                }
                Ev::Fork { p: _, copy, tag } => {
                    let Ok(mut g) = tl.try_lock() else { return "busy (skipped)".into() };
                    // quiesce (`fork::quiesce`): the source closed by its writer; a database left unclean is opened as a
                    // writer (WAL replay), rewritten and closed again
                    let c = compl.lock().unwrap().take();
                    if let Some(c) = c {
                        c.stop().await;
                    }
                    let Some(t) = g.take() else { return "not open (skipped)".into() };
                    if !t.is_writer() {
                        t.abandon().await;
                        return "not a writer: tenant given up (skipped)".into();
                    }
                    if let Err(e) = t.close().await {
                        return format!("fork {tag}: quiesce close failed: {}", short(&e));
                    }
                    // as `ctl/fork.rs` `quiesce` (fix-sim-holes, hole Z): a marker counts only when it names the stored
                    // manifest's writer epoch
                    if !crate::lifecycle::marker_clean(&store, &id).await.unwrap_or(false) {
                        let r: anyhow::Result<()> = async {
                            let mut t = lc.wake(&id).await?;
                            t.upgrade().await?;
                            t.apply(vec![Op::Put(b"zz/quiesce".to_vec(), b"q".to_vec())]).await?;
                            t.close().await?;
                            anyhow::ensure!(crate::lifecycle::marker_clean(&store, &id).await?, "could not be closed cleanly");
                            Ok(())
                        }
                        .await;
                        if let Err(e) = r {
                            return format!("fork {tag}: quiesce failed: {}", short(&e));
                        }
                    }
                    let start = model.lock().unwrap().snapshot();
                    let target = format!("{id}-fork{tag}");
                    let src_path = crate::lifecycle::tenant_path(&id);
                    let src_admin = Admin::builder(src_path.clone(), Arc::clone(&store)).build();
                    // hole F's fix in `fork.rs` (sim-test-2): the writer epoch the quiesce left
                    let epoch0 = match src_admin.read_manifest(None).await {
                        Ok(m) => m.map(|m| m.writer_epoch()),
                        Err(e) => return format!("fork {tag}: manifest unreadable: {e}"),
                    };
                    let opts = slatedb::config::CheckpointOptions { name: Some(format!("fork-pairing:{target}")), ..Default::default() };
                    let (cp, pr) = match pair::checkpoint_paired(&store, &id, || async { Ok(src_admin.create_detached_checkpoint(&opts).await?) }).await {
                        Ok(x) => x,
                        Err(e) => return format!("fork {tag}: pairing failed: {}", short(&e)),
                    };
                    let r: anyhow::Result<Option<u64>> = async {
                        let tadmin = Admin::builder(crate::lifecycle::tenant_path(&target), Arc::clone(&store)).build();
                        if !copy {
                            tadmin.create_clone_builder_from_source(slatedb::CloneSourceSpec::with_checkpoint(src_path.clone(), cp.id)).build().await?;
                        } else {
                            use futures::{StreamExt, TryStreamExt};
                            let from = object_store::path::Path::from(src_path.clone());
                            let metas: Vec<object_store::ObjectMeta> = store.list(Some(&from)).try_collect().await?;
                            let pre_from = format!("{src_path}/");
                            let pre_to = format!("{}/", crate::lifecycle::tenant_path(&target));
                            let fx = format!("{pre_from}fx/");
                            let copies = metas.into_iter().filter(|m| !matches!(m.location.filename(), Some("_lease") | Some("_clean") | Some(".deleting")) && !m.location.as_ref().starts_with(&fx)).map(|m| {
                                let to = object_store::path::Path::from(format!("{pre_to}{}", m.location.as_ref().strip_prefix(&pre_from).unwrap_or(m.location.as_ref())));
                                let store = Arc::clone(&store);
                                async move { store.copy(&m.location, &to).await }
                            });
                            futures::stream::iter(copies).buffer_unordered(8).try_collect::<Vec<()>>().await?;
                            let _ = tadmin.delete_checkpoint(cp.id).await;
                        }
                        let epoch1 = src_admin.read_manifest(None).await?.map(|m| m.writer_epoch());
                        if epoch1 != epoch0 {
                            FORKS_REFUSED_EPOCH_MOVED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        anyhow::ensure!(epoch1 == epoch0, "the source was opened by another writer during the fork (writer epoch {epoch0:?} -> {epoch1:?}); rolled back");
                        let n = match &pr {
                            Some(p) => pair::seed_fork(&store, &id, &target, p, if copy { ForkRuns::Copied } else { ForkRuns::Shared }).await?,
                            None => None,
                        };
                        Ok(n)
                    }
                    .await;
                    let d = src_admin.delete_checkpoint(cp.id).await;
                    let dp = pair::delete_pair(&store, &id, &cp.id).await;
                    let n = match r {
                        Ok(n) => n,
                        Err(e) => return format!("fork {tag} failed (the server rolls it back): {}", short(&e)),
                    };
                    if let Err(e) = crate::lifecycle::mark_clean_stored(&store, &target).await {
                        return format!("fork {tag}: marker not written (the server rolls it back): {e}");
                    }
                    let end = counts(&model.lock().unwrap());
                    forks.lock().unwrap().push(ForkRec { tag, name: target.clone(), copy, start, end });
                    format!("fork {tag} done ({}): {target}, its first version {n:?}, pairing checkpoint deleted {}, pair deleted {}", if copy { "copy" } else { "clone" }, d.is_ok(), dp.is_ok())
                }
                Ev::Generate { p, writers } => {
                    let Ok(mut g) = tl.try_lock() else { return "busy (skipped)".into() };
                    let Some(t) = g.as_mut() else { return "not open (skipped)".into() };
                    let mut x0 = None;
                    for _ in 0..20 {
                        match t.fx_ballast_begin().await {
                            Ok(Some(x)) => {
                                x0 = Some(x);
                                break;
                            }
                            Ok(None) => tokio::time::sleep(Duration::from_millis(T_PIN_MS / 3)).await,
                            Err(e) => return format!("generator begin failed: {}", short(&e)),
                        }
                    }
                    let Some(x0) = x0 else { return "generator: no envelope marker (skipped)".into() };
                    let (st, dbat, epoch) = match t.fx_ballast_target() {
                        Ok(x) => x,
                        Err(e) => return format!("generator target failed: {}", short(&e)),
                    };
                    let mut kv: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
                    for c in 0..CUSTOMERS {
                        for i in 0..PER_CUSTOMER {
                            kv.push((ckey(c, i), Some(value(kp, format!("s{seed}-g{eid}-{c}-{i}")))));
                        }
                    }
                    let status = model.lock().unwrap().issue(eid, p, &kv);
                    let opts = build_opts_for(kp);
                    let w = (writers as usize).clamp(1, CUSTOMERS);
                    let mut direct = Vec::new();
                    let mut direct_last = None;
                    for wi in 0..w {
                        let (lo, hi) = (wi * CUSTOMERS / w, (wi + 1) * CUSTOMERS / w);
                        let mut gw = crate::fx::build::GroupWriter::new(&st, &dbat, x0, epoch, &opts, (wi as u32 + 1) * 1000, false);
                        for (k, v) in kv.iter().filter(|(k, _)| (lo..hi).any(|c| k.starts_with(&ckey(c, 0)[..ckey(c, 0).len() - 5]))) {
                            if let Err(e) = gw.add(k, x0, &crate::runfmt::live(v.as_ref().unwrap())).await {
                                status.store(FAILED, SeqCst);
                                return format!("generator writer {wi} failed: {}", short(&e));
                            }
                        }
                        direct_last = gw.last_key().map(|k| k.to_vec());
                        match gw.finish().await {
                            Ok(gs) => direct.extend(gs),
                            Err(e) => {
                                status.store(FAILED, SeqCst);
                                return format!("generator writer {wi} failed: {}", short(&e));
                            }
                        }
                    }
                    let parts = vec![crate::fx::build::PartRows { lo: Vec::new(), rows: Box::new(VecRows(Vec::new().into_iter())) }];
                    let ngroups = direct.len();
                    match t.fx_publish_generated_parts(&opts, x0, direct, direct_last, parts, 2).await {
                        Ok(r) => {
                            status.store(ACKED, SeqCst);
                            format!("generated published: {} writers, {ngroups} groups, x_d {}", w, r.x_d)
                        }
                        Err(e) => {
                            status.store(FAILED, SeqCst);
                            format!("generated publish failed: {}", short(&e))
                        }
                    }
                }
                Ev::EraseRide { .. } => {
                    let c = compl.lock().unwrap().clone();
                    let Some(c) = c else { return "no compactor (skipped)".into() };
                    match erase_ride(&store, &id, &c).await {
                        Ok(s) => {
                            let mut g = tl.lock().await;
                            if let Some(t) = g.as_mut() {
                                t.fx_sync();
                            }
                            s
                        }
                        Err(e) => format!("erase ride failed: {}", short(&e)),
                    }
                }
                Ev::SlateGc { .. } => {
                    let dir = slatedb::config::GarbageCollectorDirectoryOptions { interval: None, min_age: Duration::from_secs(5), dry_run: false };
                    // the WAL is kept as a restore window keeps it (`lifecycle::WAL_RETAIN`)
                    let wal = slatedb::config::GarbageCollectorDirectoryOptions { min_age: Duration::from_secs(3600), ..dir };
                    let opts = slatedb::config::GarbageCollectorOptions { manifest_options: Some(dir), wal_options: Some(wal), compacted_options: Some(dir), compactions_options: Some(dir), ..Default::default() };
                    match Admin::builder(crate::lifecycle::tenant_path(&id), Arc::clone(&store)).build().run_gc_once(opts).await {
                        Ok(()) => "slate gc pass".into(),
                        Err(e) => format!("slate gc failed: {e}"),
                    }
                }
                _ => unreachable!(),
            }
        })
    }
}

/// `ctl/erase.rs` `run_pass_ride` with every erasure due (force_all), without its timing and its agent: forced reseals
/// of the groups still owing a ready erasure (4 rounds), the delta runs owed once the groups are clear, cleanses of those
/// a probe finds a row in, the confirmation on the newest version, and `record_clean` (the retire step follows as for a
/// scrub).
async fn erase_ride(store: &Arc<dyn ObjectStore>, id: &str, c: &Arc<Compactor>) -> anyhow::Result<String> {
    use std::collections::BTreeSet;
    let mut all = erase::list(store, id).await?;
    let pending: Vec<usize> = all.iter().enumerate().filter(|(_, e)| e.state == "pending").map(|(i, _)| i).collect();
    let need = pending.iter().map(|&i| all[i].s_e).max().unwrap_or(0);
    if need == 0 {
        return Ok("no erasure pending".into());
    }
    let mut v = c.current().await;
    let ready: Vec<usize> = pending.iter().copied().filter(|&i| all[i].s_e <= v.x_d).collect();
    if ready.is_empty() {
        return Ok("no erasure ready".into());
    }
    let mut targets: BTreeMap<usize, Targets> = BTreeMap::new();
    for &i in &ready {
        match erase::targets_of(store, id, &all[i]).await? {
            Some(t) => {
                targets.insert(i, t);
            }
            None => anyhow::bail!("erasure {} lost its targets before its physical erasure", all[i].id),
        }
    }
    let (mut resealed, mut cleansed) = (0u64, 0u64);
    for _round in 0..4 {
        let mut gids: BTreeSet<u32> = BTreeSet::new();
        for &i in &ready {
            gids.extend(erase::owing_groups(&v, &targets[&i], all[i].s_e)?);
        }
        if gids.is_empty() {
            break;
        }
        for gid in gids {
            if c.force_reseal_group(gid).await? {
                resealed += 1;
            }
        }
        v = c.current().await;
    }
    for &i in &ready {
        if erase::owing_groups(&v, &targets[&i], all[i].s_e)?.is_empty() && all[i].groups_clear_version.is_none() {
            all[i].groups_clear_version = Some(v.version);
            all[i].delta_owed = erase::owed_delta(&v, &targets[&i], all[i].s_e)?;
            erase::save(store, id, &all[i]).await?;
        }
    }
    let due_clear: Vec<usize> = ready.iter().copied().filter(|&i| all[i].groups_clear_version.is_some() && !all[i].delta_owed.is_empty()).collect();
    if !due_clear.is_empty() {
        let paths: BTreeSet<String> = due_clear.iter().flat_map(|&i| all[i].delta_owed.iter().cloned()).collect();
        let items: Vec<(&Targets, u64)> = due_clear.iter().map(|&i| (&targets[&i], all[i].s_e)).collect();
        let batch = Batch::new(&items);
        let view = view::View::new(Arc::clone(store), v.clone(), None)?;
        let mut clean_paths: BTreeSet<String> = BTreeSet::new();
        for h in view.delta.iter().filter(|h| paths.contains(&h.line.path)) {
            if erase::probe_rows(store, h, &batch, 1).await?.is_empty() {
                clean_paths.insert(h.line.path.clone());
                continue;
            }
            if c.cleanse_delta(&h.line.path).await? == Some(true) {
                cleansed += 1;
            }
        }
        v = c.current().await;
        let listed: BTreeSet<String> = v.delta.iter().map(|d| d.path.clone()).collect();
        for &i in &due_clear {
            let n = all[i].delta_owed.len();
            all[i].delta_owed.retain(|p| !clean_paths.contains(p) && listed.contains(p));
            if all[i].delta_owed.len() != n {
                erase::save(store, id, &all[i]).await?;
            }
        }
    }
    let cands: Vec<usize> = ready.iter().copied().filter(|&i| all[i].groups_clear_version.is_some() && all[i].delta_owed.is_empty()).collect();
    let mut recorded = 0;
    if !cands.is_empty() {
        let items: Vec<(&Targets, u64)> = cands.iter().map(|&i| (&targets[&i], all[i].s_e)).collect();
        let view = view::View::new(Arc::clone(store), v.clone(), None)?;
        let clean = erase::confirm_clean(store, &view, &items, None).await?;
        for (k, &i) in cands.iter().enumerate() {
            if clean[k] {
                erase::record_clean(store, id, &mut all[i], v.version, "forced").await?;
                recorded += 1;
            }
        }
    }
    Ok(format!("ride of {} erasures on version {}: {resealed} forced reseals, {cleansed} cleanses, {recorded} recorded clean", ready.len(), v.version))
}

/// The oracle's additions, after the main one (the store calm, every process stopped).
pub(super) async fn oracle_ext(w: &World) -> Vec<String> {
    let mut v: Vec<String> = w.x.found.lock().unwrap().clone();
    if !w.created {
        return v;
    }
    let live = w.model.lock().unwrap().snapshot();
    let calm = w.net.proc_store(2_000_000 + w.next_life);
    // every live restore point: its pair names a version whose runs all exist
    let pts: Vec<(u64, uuid::Uuid, u64)> = w.x.points.lock().unwrap().iter().filter(|x| !x.pruned).map(|x| (x.tag, x.cp, x.manifest_id)).collect();
    for (tag, cp, _) in &pts {
        match pair::load_pair(&w.raw, &w.id, cp).await {
            Ok(Some(p)) => {
                if let Some(n) = p.version {
                    match runs_missing(&w.raw, &w.id, n).await {
                        Ok(g) if !g.is_empty() => v.push(format!("restore point {tag}: its version {n} lists {} runs that no longer exist (live run deleted), first {}", g.len(), g[0])),
                        Err(e) => v.push(format!("restore point {tag}: its version {n} is unreadable: {e}")),
                        _ => {}
                    }
                }
            }
            Ok(None) => v.push(format!("restore point {tag}: its pair is gone while its checkpoint lives")),
            Err(e) => v.push(format!("restore point {tag}: pair unreadable: {}", short(&e))),
        }
    }
    // every mark again, after all GC: a restore to it gives the model's state at it (at most 3)
    let marks: Vec<(u64, i64)> = w.x.marks.lock().unwrap().iter().map(|m| (m.tag, m.at_ms)).collect();
    let mut checked = 0;
    for (tag, at) in marks.iter().rev() {
        if checked >= 3 {
            break;
        }
        let pt = w.x.points.lock().unwrap().iter().filter(|x| !x.pruned && x.at_ms <= *at).max_by_key(|x| x.at_ms).map(|x| (x.cp, x.manifest_id, x.tag));
        let Some((cp, mid, ptag)) = pt else { continue };
        checked += 1;
        let staging = format!("control/staging/{}/final-{tag}", w.id);
        match tokio::time::timeout(Duration::from_secs(60), stage(&calm, &w.id, (cp, mid), *at, &staging)).await {
            Ok(Ok((copied, version, reads))) => {
                let m = w.x.marks.lock().unwrap();
                let mk = m.iter().find(|x| x.tag == *tag).unwrap();
                let end = counts(&mk.snap);
                v.extend(check_at(&format!("final restore to mark {tag} (point {ptag}, version {version:?}, {copied} WAL objects forward)"), &reads, &mk.snap, &end, &live));
            }
            Ok(Err(e)) => {
                let s = short(&e);
                // a gap in a WAL kept for the window, or a pair or version gone, is a restore point lost
                v.push(format!("final restore to mark {tag} (point {ptag}) refused on a calm store: {s}"));
            }
            Err(_) => v.push(format!("final restore to mark {tag} did not return in 60 s")),
        }
    }
    // every fork, in a fresh process: the model's state at the fork
    let forks: Vec<(u64, String, bool)> = w.x.forks.lock().unwrap().iter().map(|f| (f.tag, f.name.clone(), f.copy)).collect();
    for (tag, name, copy) in forks {
        view::clear_cache();
        let lc = Lifecycle::new(Arc::new(crate::counting::CountingStore::new(Arc::clone(&calm))), None);
        let what = format!("fork {tag} ({})", if copy { "copy" } else { "clone" });
        let mut t = match tokio::time::timeout(Duration::from_secs(60), lc.wake(&name)).await {
            Ok(Ok(t)) => t,
            Ok(Err(e)) => {
                v.push(format!("{what}: fresh opening failed: {}", short(&e)));
                continue;
            }
            Err(_) => {
                v.push(format!("{what}: fresh opening did not return in 60 s"));
                continue;
            }
        };
        let first = read_all(&t).await;
        let next = read_all(&t).await;
        for (k, a) in &first {
            if next.get(k) != Some(a) {
                v.push(format!("{what}: FIRST and NEXT differ on {}", show(k)));
            }
        }
        {
            let f = w.x.forks.lock().unwrap();
            let fr = f.iter().find(|x| x.tag == tag).unwrap();
            v.extend(check_at(&format!("{what} reader"), &first, &fr.start, &fr.end, &live));
        }
        match tokio::time::timeout(Duration::from_secs(60), t.upgrade()).await {
            Ok(Ok(())) => {
                let wr = read_all(&t).await;
                for (k, a) in &first {
                    if wr.get(k) != Some(a) {
                        v.push(format!("{what}: reader and writer differ on {}: reader {}, writer {}", show(k), show_v(a), show_v(wr.get(k).unwrap())));
                    }
                }
            }
            Ok(Err(e)) => v.push(format!("{what}: fresh upgrade failed: {}", short(&e))),
            Err(_) => v.push(format!("{what}: fresh upgrade did not return in 60 s")),
        }
        // the fork's own writes: every key written once more through its writer, then read back in a fresh process (a
        // write whose seq were at or below the paired X_d would be shadowed by the shared runs)
        let mut own: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        if t.is_writer() {
            let batch: Vec<Op> = (0..CUSTOMERS).flat_map(|c| (0..PER_CUSTOMER).map(move |i| (c, i))).map(|(c, i)| {
                let v = format!("fork{tag}-own-{c}-{i}").into_bytes();
                own.insert(ckey(c, i), v.clone());
                Op::Put(ckey(c, i), v)
            }).collect();
            if let Err(e) = t.apply(batch).await {
                v.push(format!("{what}: its own write failed on a calm store: {}", short(&e)));
                own.clear();
            }
        }
        if let Some(n) = t.fx_served_version() {
            match runs_missing(&w.raw, &name, n).await {
                Ok(g) if !g.is_empty() => v.push(format!("{what}: its version {n} lists {} runs that no longer exist (live run deleted), first {}", g.len(), g[0])),
                Err(e) => v.push(format!("{what}: its version {n} is unreadable: {e}")),
                _ => {}
            }
        }
        let _ = tokio::time::timeout(Duration::from_secs(20), t.close()).await;
        if !own.is_empty() {
            view::clear_cache();
            let lc = Lifecycle::new(Arc::new(crate::counting::CountingStore::new(Arc::clone(&calm))), None);
            match tokio::time::timeout(Duration::from_secs(60), lc.wake(&name)).await {
                Ok(Ok(t)) => {
                    let got = read_all(&t).await;
                    let bad: Vec<String> = own.iter().filter(|(k, want)| !matches!(got.get(*k), Some(Ok(Some(g))) if g == *want)).map(|(k, _)| show(k)).collect();
                    if !bad.is_empty() {
                        v.push(format!("{what}: {} of its own acknowledged writes not read back by a fresh opening, first {}", bad.len(), bad[0]));
                    }
                    t.abandon().await;
                }
                Ok(Err(e)) => v.push(format!("{what}: reopening after its own writes failed: {}", short(&e))),
                Err(_) => v.push(format!("{what}: reopening after its own writes did not return in 60 s")),
            }
        }
    }
    v
}

/// What the run reached of the new scenarios (for the seed's line).
pub(super) fn coverage(w: &World) -> String {
    let log = w.log.lock().unwrap();
    let n = |pat: &str| log.iter().filter(|l| l.contains(pat)).count();
    if w.x.marks.lock().unwrap().is_empty() && w.x.points.lock().unwrap().is_empty() && n("generated") == 0 && n("fork ") == 0 && n("storm") == 0 && w.knobs != 2 {
        return String::new();
    }
    format!(
        "; points {} restores {} (applied {}) forks {} generated {} slate-gc {} drops {} scrubs {} storms {} groups-profile {}",
        n("-> restore point"),
        n("-> restore checked"),
        n("checked and applied"),
        n(" done ("),
        n("-> generated published"),
        n("-> slate gc pass"),
        n("-> dropped through"),
        n("-> scrub of") + n("recorded clean"),
        n("storm on"),
        w.knobs == 2
    )
}

/// The scripts of sim-test-2: the random walk of `generate` (unchanged), with the new events woven in by a second
/// random stream of the same seed. The abstract owner is followed so that most new events go to a process that can do
/// them (any event still applies in any state: one that does not is a no-op in the trace).
pub fn generate2(seed: u64, steps: usize) -> Script {
    let base = generate(seed, steps);
    let mut r = StdRng::seed_from_u64(seed ^ 0x51_6d_2e_73_74_32);
    let knobs = if r.gen_bool(0.4) { 2 } else { base.knobs };
    let mut owner: Option<usize> = Some(0);
    let mut down: Vec<usize> = vec![1, 2];
    let mut out: Vec<Ev> = Vec::new();
    let (mut tag, mut marks, mut points) = (0u64, Vec::<u64>::new(), Vec::<u64>::new());
    let mut storm = false;
    let gen = r.gen_bool(0.2);
    let early_point = r.gen_bool(0.6);
    for (i, (_, e)) in base.events.into_iter().enumerate() {
        match &e {
            Ev::Open { p, writer: true } | Ev::Upgrade { p } => {
                owner = Some(*p);
                down.retain(|x| x != p);
            }
            Ev::Open { p, writer: false } => down.retain(|x| x != p),
            Ev::Close { p } | Ev::Abandon { p } | Ev::Kill { p } => {
                if owner == Some(*p) {
                    owner = None;
                }
                if !down.contains(p) {
                    down.push(*p);
                }
            }
            _ => {}
        }
        out.push(e);
        if i == 0 {
            if gen {
                out.push(Ev::Generate { p: 0, writers: r.gen_range(1..4) });
            }
            if early_point {
                out.push(Ev::Write { p: 0, ops: (0..PER_CUSTOMER).map(|i| (0, i, false)).collect() });
                out.push(Ev::Flush { p: 0 });
                tag += 1;
                out.push(Ev::Point { p: 0, tag });
                points.push(tag);
            }
            continue;
        }
        let o = owner.unwrap_or_else(|| r.gen_range(0..SLOTS));
        let x: u32 = r.gen_range(0..1000);
        match x {
            0..=39 => {
                tag += 1;
                out.push(Ev::Point { p: r.gen_range(0..SLOTS), tag });
                points.push(tag);
            }
            40..=74 => {
                tag += 1;
                out.push(Ev::Mark { tag });
                marks.push(tag);
            }
            75..=114 if !marks.is_empty() => {
                let m = marks[r.gen_range(0..marks.len())];
                out.push(Ev::Restore { p: o, mark: m, apply: r.gen_bool(0.5) });
            }
            115..=124 if !points.is_empty() => {
                let t = points[r.gen_range(0..points.len())];
                out.push(Ev::PointPrune { p: r.gen_range(0..SLOTS), tag: t });
            }
            125..=149 if owner.is_some() => {
                tag += 1;
                out.push(Ev::Fork { p: o, copy: r.gen_bool(0.4), tag });
            }
            150..=169 => out.push(Ev::SlateGc { p: r.gen_range(0..SLOTS) }),
            170..=199 => out.push(Ev::Gc { p: o }),
            200..=219 => out.push(Ev::Retire { p: r.gen_range(0..SLOTS) }),
            220..=239 => out.push(Ev::DropThrough { p: o }),
            240..=264 if knobs == 2 => {
                out.push(Ev::Erase { p: o, cust: r.gen_range(0..CUSTOMERS) });
                if r.gen_bool(0.6) {
                    out.push(Ev::Flush { p: o });
                    out.push(Ev::CompDrain { p: o });
                    out.push(Ev::EraseRide { p: o });
                } else {
                    out.push(Ev::ErasePass { p: o });
                }
            }
            355..=379 => out.push(Ev::EraseRide { p: o }),
            265..=279 if !storm => {
                storm = true;
                out.push(Ev::Storm { on: true });
            }
            280..=339 if storm => {
                storm = false;
                out.push(Ev::Storm { on: false });
            }
            340..=354 if owner.is_some() => {
                // a burst of small flushed writes (L0 grows past the guard), then often a takeover: the WAL replay raise
                for _ in 0..8 {
                    let c = r.gen_range(0..CUSTOMERS);
                    out.push(Ev::Write { p: o, ops: (0..3).map(|_| (c, r.gen_range(0..PER_CUSTOMER), false)).collect() });
                    out.push(Ev::Flush { p: o });
                }
                if let Some(&q) = down.first() {
                    if r.gen_bool(0.6) {
                        out.push(Ev::Write { p: o, ops: vec![(0, r.gen_range(0..PER_CUSTOMER), false)] });
                        out.push(Ev::Open { p: q, writer: true });
                        owner = Some(q);
                        down.retain(|x| *x != q);
                    }
                }
            }
            _ => {}
        }
    }
    // a restore to the newest mark near the end, when there is one
    if let Some(&m) = marks.last() {
        if let Some(o) = owner {
            out.push(Ev::Restore { p: o, mark: m, apply: false });
        }
    }
    Script { seed, profile: base.profile, knobs, events: out.into_iter().enumerate().map(|(i, e)| (i as u32, e)).collect(), ext: Ext { flavor: 2, ..Default::default() } }
}

/// fix-sim-holes, hole F: simulated forks refused because the source's writer epoch moved during them (tests).
pub static FORKS_REFUSED_EPOCH_MOVED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
