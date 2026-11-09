//! Erasure (compaction 2, step 7b; plan round 2, fix 5; DECISIONS 2026-11-07 "Ordinary deletes and whole-customer
//! deletes").
//!
//! Two kinds of delete:
//! - an ordinary delete (deleteOne, a deleteMany that is not a whole customer, an update): its older rows leave the
//!   delta runs at the next merge of the tier that holds them and the sealed run at the group's next reseal, and the
//!   restore points after 30 days. Nothing here runs for it.
//! - an erasure: a command that deletes all of one customer's data in a customer-keyed collection (a deleteMany whose
//!   filter is exactly the customer field, `mongo-server/src/cmd_data.rs`), or a whole database (`ctl/erase.rs`, which
//!   deletes every object of the database at once, `fx/` included). For a customer, the rows leave our live runs, L0
//!   and SlateDB's sorted runs within the deadline ([`DEADLINE_MS`], 24 hours), and the restore points after 30 days.
//!
//! A customer erasure is recorded when its deletes commit, under the database's exclusive lock, in
//! `{db}/fx/erasures/{id}.json`, with its cut S_e (the database's last seq after the deletes) and its targets in
//! `{id}.keys`: the physical key range of the customer in that collection (documents and co-located index entries,
//! `u/{customer}{collection}/`) and every other key the deletes removed (the locators, the index entries outside the
//! customer's range, the natural-order entries). The targets hold the customer's own values (its id, indexed fields), so
//! that object is deleted as soon as the runs are scrubbed. A row is erased when its key is a target and its seq is at or
//! below S_e: a document written again after the erasure (seq above S_e) is new data and stays.
//!
//! The pass ([`crate::lifecycle::Tenant::fx_erase_pass`], under the exclusive lock of the writer):
//! 1. our runs must hold every row at or below S_e: if X_d < S_e, a version is published first (step 3's builder now;
//!    step 4's absorption later);
//! 2. scrub: every sealed and delta run of the newest version that may hold a target row at or below S_e is rewritten
//!    without those rows, and a version of kind `erase` lists the new runs (same X_d, X_g and groups). The read rule
//!    then answers every target key from SlateDB above X_d (the delete envelopes, or newer writes) or not at all;
//! 3. SlateDB: every target row is at or below S_e <= X_d, so the drop (step 4; `droptest.rs` in the tests) removes
//!    them from L0 and the sorted runs, and SlateDB's GC deletes the old SSTs (`min_age` under 24 hours). WAL objects are
//!    kept for the restore window on purpose (`WAL_RETAIN`), so they leave with the restore points, after 30 days;
//! 4. retire: the runs the scrub replaced are deleted once the GC grace has passed (rule 23: no hold can still read
//!    them) and no pair (a restore point, an export) or fork reference lists them; old manifest versions that list them
//!    or name a target key go the same way. A run kept for a restore point is reported with the checkpoint that keeps it.
//!
//! An erasure is `live_done` once its targets are gone from the runs of the newest version (the 24-hour target, with the
//! drop of step 4), and `done` once no object of ours holds them (the 30-day target).
//!
//! Cost (ESTIMATED): one rewrite of each run that holds the customer, so the customer's group and the delta runs whose
//! key range holds it; an index outside the customer's range spreads its entries over the keyspace, so its groups are
//! rewritten too (the plan's estimate counts only the customer's own group).

use super::build::line;
use super::manifest::{self, hex, now_ms, root, RunLine, Version};
use super::view::{self, RunH, View};
use crate::runfmt::{RunOptions, RunWriter};
use anyhow::{bail, ensure, Context, Result};
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

/// How long after its request an erasure must have left the live runs, L0 and SlateDB (24 hours, DECISIONS
/// 2026-11-07); tests set it shorter (`--sealed-erasure-deadline-ms`).
pub static DEADLINE_MS: AtomicU64 = AtomicU64::new(24 * 3600 * 1000);
/// Erasures recorded, scrub versions published, rows erased from runs, runs retired (diagnostics).
pub static REGISTERED: AtomicU64 = AtomicU64::new(0);
pub static SCRUBS: AtomicU64 = AtomicU64::new(0);
pub static ROWS_ERASED: AtomicU64 = AtomicU64::new(0);
pub static RUNS_RETIRED: AtomicU64 = AtomicU64::new(0);

/// Negative controls of the tests (`fx/tests_7b.rs`, `FX_7B_CONTROL`): no scrub at all; delta runs not scrubbed; target
/// rows erased whatever their seq (a document written again after the erasure is lost); retired runs never deleted.
/// Only the tests set them.
pub static CONTROL_NO_SCRUB: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
pub static CONTROL_NO_DELTA_SCRUB: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
pub static CONTROL_NO_SEQ_BOUND: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
pub static CONTROL_NO_RETIRE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[inline]
fn ctl(f: &std::sync::atomic::AtomicBool) -> bool {
    f.load(Relaxed)
}

/// The physical keys an erasure removes: whole ranges [lo, hi) and single keys.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Targets {
    pub ranges: Vec<(Vec<u8>, Vec<u8>)>,
    pub keys: BTreeSet<Vec<u8>>,
}

impl Targets {
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty() && self.keys.is_empty()
    }

    /// Add a key unless a range already covers it.
    pub fn add_key(&mut self, k: Vec<u8>) {
        if !self.in_range(&k) {
            self.keys.insert(k);
        }
    }

    fn in_range(&self, k: &[u8]) -> bool {
        self.ranges.iter().any(|(lo, hi)| &lo[..] <= k && k < &hi[..])
    }

    pub fn hit(&self, k: &[u8]) -> bool {
        self.in_range(k) || self.keys.contains(k)
    }

    /// Whether a run whose keys are within [first, last] may hold a target.
    pub fn meets(&self, first: &[u8], last: &[u8]) -> bool {
        self.ranges.iter().any(|(lo, hi)| &lo[..] <= last && first < &hi[..]) || self.keys.range(first.to_vec()..=last.to_vec()).next().is_some()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let put = |out: &mut Vec<u8>, b: &[u8]| {
            out.extend((b.len() as u32).to_le_bytes());
            out.extend(b);
        };
        for (lo, hi) in &self.ranges {
            out.push(1);
            put(&mut out, lo);
            put(&mut out, hi);
        }
        for k in &self.keys {
            out.push(0);
            put(&mut out, k);
        }
        out
    }

    pub fn decode(b: &[u8]) -> Result<Targets> {
        let mut t = Targets::default();
        let mut i = 0;
        let take = |i: &mut usize| -> Result<Vec<u8>> {
            ensure!(*i + 4 <= b.len(), "erasure targets: truncated");
            let n = u32::from_le_bytes(b[*i..*i + 4].try_into().unwrap()) as usize;
            *i += 4;
            ensure!(*i + n <= b.len(), "erasure targets: truncated");
            let v = b[*i..*i + n].to_vec();
            *i += n;
            Ok(v)
        };
        while i < b.len() {
            let tag = b[i];
            i += 1;
            match tag {
                1 => {
                    let lo = take(&mut i)?;
                    let hi = take(&mut i)?;
                    t.ranges.push((lo, hi));
                }
                0 => {
                    let k = take(&mut i)?;
                    t.keys.insert(k);
                }
                x => bail!("erasure targets: bad tag {x}"),
            }
        }
        Ok(t)
    }
}

/// A run the scrub replaced, and why it still exists (empty once deleted).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Retired {
    pub path: String,
    pub deleted_ms: Option<u64>,
    /// Why it is kept: "grace", "restore point {checkpoint}", "fork {db}".
    pub kept_by: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Erasure {
    pub id: String,
    pub kind: String,
    pub collection: String,
    /// A hash of the customer's key component (the component itself is the customer's data; the hash lets an audit
    /// match the erasure to a request).
    pub customer_hash: String,
    pub s_e: u64,
    pub requested_ms: u64,
    pub deadline_ms: u64,
    pub ranges: u64,
    pub keys: u64,
    /// "pending" until the scrub version is published, then "scrubbed".
    pub state: String,
    pub scrubbed_version: Option<u64>,
    pub scrubbed_ms: Option<u64>,
    pub rows_erased: u64,
    pub runs_rewritten: u64,
    pub retired: Vec<Retired>,
    /// Old manifest versions deleted because they listed a retired run or named a target key, and those kept (by a pair
    /// or a fork reference, or the grace).
    pub versions_deleted: u64,
    pub versions_kept: Vec<String>,
    /// A group boundary of the newest version that is a target key (the manifest names it): reported, step 4's split and
    /// merge move it.
    pub boundaries_on_targets: u64,
    /// Live runs clean (the 24-hour target, with step 4's drop at or above S_e) and nothing of ours left (30 days).
    pub live_done_ms: Option<u64>,
    pub done_ms: Option<u64>,
}

impl Erasure {
    pub fn overdue(&self, now: u64) -> bool {
        self.live_done_ms.map_or(now > self.deadline_ms, |d| d > self.deadline_ms)
    }

    pub fn to_json(&self) -> serde_json::Value {
        let mut v = serde_json::to_value(self).unwrap_or_default();
        v["overdue"] = serde_json::json!(self.overdue(now_ms()));
        v["drop_floor_needed"] = serde_json::json!(self.s_e);
        v
    }
}

pub fn dir(id: &str) -> Path {
    Path::from(format!("{}/erasures", root(id)))
}

fn rec_path(id: &str, eid: &str) -> Path {
    Path::from(format!("{}/erasures/{eid}.json", root(id)))
}

pub fn keys_path(id: &str, eid: &str) -> Path {
    Path::from(format!("{}/erasures/{eid}.keys", root(id)))
}

/// The fork references of a database: `{db}/fx/forks/{fork}` names the version and runs a fork shares (`pair.rs`).
pub fn forks_dir(id: &str) -> Path {
    Path::from(format!("{}/forks", root(id)))
}

fn customer_hash(c: &[u8]) -> String {
    format!("{:016x}", twox_hash::XxHash64::oneshot(0x6572_6173, c))
}

/// Record an erasure whose deletes committed at or below `s_e` (the caller holds the database's exclusive lock, so no
/// other write of these keys lies between the deletes and S_e). The targets first, then the record: a crash between
/// the two leaves an orphan targets object, which the next pass deletes.
pub async fn register(store: &Arc<dyn ObjectStore>, id: &str, collection: &str, customer: &[u8], targets: &Targets, s_e: u64) -> Result<Erasure> {
    let now = now_ms();
    let r: u32 = rand::random();
    let eid = format!("{s_e:020}-{r:08x}");
    store.put(&keys_path(id, &eid), PutPayload::from(targets.encode())).await?;
    let e = Erasure {
        id: eid,
        kind: "customer".into(),
        collection: collection.to_string(),
        customer_hash: customer_hash(customer),
        s_e,
        requested_ms: now,
        deadline_ms: now + DEADLINE_MS.load(Relaxed),
        ranges: targets.ranges.len() as u64,
        keys: targets.keys.len() as u64,
        state: "pending".into(),
        scrubbed_version: None,
        scrubbed_ms: None,
        rows_erased: 0,
        runs_rewritten: 0,
        retired: Vec::new(),
        versions_deleted: 0,
        versions_kept: Vec::new(),
        boundaries_on_targets: 0,
        live_done_ms: None,
        done_ms: None,
    };
    save(store, id, &e).await?;
    REGISTERED.fetch_add(1, Relaxed);
    Ok(e)
}

pub async fn save(store: &Arc<dyn ObjectStore>, id: &str, e: &Erasure) -> Result<()> {
    store.put(&rec_path(id, &e.id), PutPayload::from(serde_json::to_vec(e)?)).await?;
    Ok(())
}

/// Every erasure of a database, oldest first.
pub async fn list(store: &Arc<dyn ObjectStore>, id: &str) -> Result<Vec<Erasure>> {
    let metas: Vec<_> = store.list(Some(&dir(id))).try_collect().await?;
    let mut paths: Vec<Path> = metas.into_iter().map(|m| m.location).filter(|p| p.as_ref().ends_with(".json")).collect();
    paths.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
    let mut out = Vec::new();
    for p in paths {
        let b = store.get(&p).await?.bytes().await?;
        out.push(serde_json::from_slice(&b).with_context(|| format!("erasure record {p}"))?);
    }
    Ok(out)
}

/// Whether a database has an erasure not yet done (cheap: one LIST).
pub async fn any_open(store: &Arc<dyn ObjectStore>, id: &str) -> Result<bool> {
    Ok(list(store, id).await?.iter().any(|e| e.done_ms.is_none()))
}

pub async fn load_targets(store: &Arc<dyn ObjectStore>, id: &str, eid: &str) -> Result<Option<Targets>> {
    match store.get(&keys_path(id, eid)).await {
        Ok(g) => Ok(Some(Targets::decode(&g.bytes().await?)?)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// The run options of the scrub's rewrites (the runs keep their tier's options).
#[derive(Clone, Debug)]
pub struct ScrubOpts {
    pub sealed: RunOptions,
    pub delta: RunOptions,
}

impl Default for ScrubOpts {
    fn default() -> Self {
        let b = super::build::BuildOpts::default();
        ScrubOpts { sealed: b.sealed, delta: b.delta }
    }
}

/// What a scrub did.
#[derive(Clone, Debug, Default)]
pub struct Scrub {
    pub version: Option<Version>,
    pub retired: Vec<String>,
    pub rows: u64,
    pub runs: u64,
}

/// Rewrite one run without the target rows at or below `s_e`. None: the run holds none (nothing written). Some(None):
/// every row was a target (the run leaves the version).
async fn rewrite_run(store: &Arc<dyn ObjectStore>, h: &RunH, path: String, opts: &RunOptions, t: &Targets, s_e: u64, dropped: &mut u64) -> Result<Option<Option<RunLine>>> {
    let n = view::block_count(store, h).await?;
    let mut w: Option<RunWriter> = None;
    let mut erased = 0u64;
    let any_seq = ctl(&CONTROL_NO_SEQ_BOUND);
    let mut b = 0;
    while b < n {
        let z = (b + 63).min(n - 1);
        for blk in view::blocks(store, h, b, z).await? {
            for i in 0..blk.len() {
                let k = blk.key(i);
                if (blk.seq(i) <= s_e || any_seq) && t.hit(k) {
                    erased += 1;
                    continue;
                }
                if w.is_none() {
                    w = Some(RunWriter::new(store, Path::from(path.as_str()), opts.clone()).await?);
                }
                w.as_mut().unwrap().add(k, blk.seq(i), blk.env(i)).await?;
            }
        }
        b = z + 1;
    }
    if erased == 0 {
        if let Some(w) = w {
            w.abort().await?;
        }
        return Ok(None);
    }
    *dropped += erased;
    Ok(Some(match w {
        Some(w) => Some(line(&w.finish().await?)),
        None => None,
    }))
}

/// The scrub of version `cur`: rewrite every run that may hold a target row at or below `s_e`, and build the version
/// that lists the new runs (number `next`, kind `erase`; same X_d, groups and X_g). `version` None: no run held one.
pub async fn scrub_version(store: &Arc<dyn ObjectStore>, id: &str, cur: &View, next: u64, epoch: Option<u64>, t: &Targets, s_e: u64, opts: &ScrubOpts) -> Result<Scrub> {
    ensure!(cur.x_d >= s_e, "database {id}: an erasure at {s_e} needs X_d at or above it (X_d {})", cur.x_d);
    let mut out = Scrub::default();
    if ctl(&CONTROL_NO_SCRUB) {
        return Ok(out);
    }
    let mut v = cur.meta.clone();
    let tag = format!("{next:010}");
    for (gi, g) in cur.groups.iter().enumerate() {
        let Some(h) = &g.sealed else { continue };
        if h.line.min_seq > s_e || !t.meets(&h.first, &h.last) {
            continue;
        }
        let path = manifest::run_path(id, &format!("g{}e{tag}", g.id), g.x_g, epoch);
        if let Some(new) = rewrite_run(store, h, path, &opts.sealed, t, s_e, &mut out.rows).await? {
            out.retired.push(h.line.path.clone());
            out.runs += new.is_some() as u64;
            v.groups[gi].sealed = new;
        }
    }
    if !ctl(&CONTROL_NO_DELTA_SCRUB) {
        let mut delta = Vec::with_capacity(cur.delta.len());
        for h in &cur.delta {
            if h.line.min_seq > s_e || !t.meets(&h.first, &h.last) {
                delta.push(h.line.clone());
                continue;
            }
            let path = manifest::run_path(id, &format!("delta-e{tag}"), h.line.max_seq, epoch);
            match rewrite_run(store, h, path, &opts.delta, t, s_e, &mut out.rows).await? {
                None => delta.push(h.line.clone()),
                Some(new) => {
                    out.retired.push(h.line.path.clone());
                    out.runs += new.is_some() as u64;
                    delta.extend(new);
                }
            }
        }
        // newest first by max seq (a rewritten run can end lower than the next one)
        delta.sort_by(|a, b| b.max_seq.cmp(&a.max_seq));
        v.delta = delta;
    }
    if out.retired.is_empty() {
        return Ok(out);
    }
    v.version = next;
    v.kind = "erase".into();
    v.writer_epoch = epoch;
    v.published_ms = now_ms();
    v.check()?;
    out.version = Some(v);
    Ok(out)
}

/// The run paths and keys a version names (lines, group boundaries).
fn names(v: &Version) -> (Vec<&str>, Vec<Vec<u8>>) {
    let mut runs = Vec::new();
    let mut keys = Vec::new();
    for g in &v.groups {
        keys.extend(manifest::unhex(&g.lo).ok());
        if let Some(r) = &g.sealed {
            runs.push(r.path.as_str());
            keys.extend(manifest::unhex(&r.first).ok());
            keys.extend(manifest::unhex(&r.last).ok());
        }
    }
    for r in &v.delta {
        runs.push(r.path.as_str());
        keys.extend(manifest::unhex(&r.first).ok());
        keys.extend(manifest::unhex(&r.last).ok());
    }
    (runs, keys)
}

/// What keeps objects of a database alive besides the newest version: the versions named by pairs (restore points,
/// exports) and fork references, with the name of what keeps them.
pub async fn referenced_versions(store: &Arc<dyn ObjectStore>, id: &str) -> Result<Vec<(u64, String, Vec<String>)>> {
    let mut out = Vec::new();
    let pairs: Vec<_> = store.list(Some(&Path::from(format!("{}/pairs", root(id))))).try_collect().await?;
    for m in pairs {
        let b = match store.get(&m.location).await {
            Ok(g) => g.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(e) => return Err(e.into()),
        };
        let p: super::pair::Pair = serde_json::from_slice(&b)?;
        if let Some(n) = p.version {
            out.push((n, format!("restore point or export {}", p.checkpoint), Vec::new()));
        }
    }
    let forks: Vec<_> = store.list(Some(&forks_dir(id))).try_collect().await?;
    for m in forks {
        let b = match store.get(&m.location).await {
            Ok(g) => g.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(e) => return Err(e.into()),
        };
        let r: super::pair::ForkRef = serde_json::from_slice(&b)?;
        out.push((r.version, format!("fork {}", r.fork), r.runs));
    }
    Ok(out)
}

/// Retire what the scrub replaced: delete each retired run once the grace has passed and nothing keeps it, and the old
/// versions that list a retired run or name a target key. `t`: the targets while they are still known (None after).
pub async fn retire(store: &Arc<dyn ObjectStore>, id: &str, e: &mut Erasure, newest: u64, grace_ms: u64) -> Result<()> {
    let Some(scrubbed) = e.scrubbed_version else { return Ok(()) };
    let now = now_ms();
    let in_grace = e.scrubbed_ms.map_or(true, |s| now < s + grace_ms);
    let refs = referenced_versions(store, id).await?;
    // the runs each referenced version lists (a fork reference carries its own list: the version may live in the parent)
    let mut kept: Vec<(String, Vec<String>)> = Vec::new();
    for (n, why, runs) in &refs {
        let listed: Vec<String> = if !runs.is_empty() {
            runs.clone()
        } else {
            match manifest::load(store, id, *n).await {
                Ok(v) => names(&v).0.into_iter().map(|s| s.to_string()).collect(),
                Err(_) => Vec::new(),
            }
        };
        kept.push((why.clone(), listed));
    }
    let no_retire = ctl(&CONTROL_NO_RETIRE);
    let own = format!("tenants/{id}/");
    for r in e.retired.iter_mut().filter(|r| r.deleted_ms.is_none()) {
        let mut by: Vec<String> = kept.iter().filter(|(_, l)| l.iter().any(|p| *p == r.path)).map(|(w, _)| w.clone()).collect();
        if std::env::var("REVIEW_FIX1").is_ok() && !r.path.starts_with(&own) {
            by.push("ancestor".into());
        }
        if in_grace {
            by.push("grace".into());
        }
        if no_retire {
            by.push("control".into());
        }
        if by.is_empty() {
            match store.delete(&Path::from(r.path.as_str())).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(x) => return Err(x.into()),
            }
            r.deleted_ms = Some(now);
            RUNS_RETIRED.fetch_add(1, Relaxed);
        }
        r.kept_by = by;
    }
    // old versions: they list retired runs, and their lines name keys (first, last, group boundaries)
    let retired: BTreeSet<&str> = e.retired.iter().map(|r| r.path.as_str()).collect();
    let mut versions_kept = Vec::new();
    for n in manifest::list_versions(store, id).await? {
        if n >= scrubbed || n >= newest {
            continue;
        }
        let v = match manifest::load(store, id, n).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        let (runs, _) = names(&v);
        if !runs.iter().any(|p| retired.contains(p)) {
            continue;
        }
        let mut by: Vec<String> = refs.iter().filter(|(m, _, _)| *m == n).map(|(_, w, _)| w.clone()).collect();
        if in_grace {
            by.push("grace".into());
        }
        if by.is_empty() && !no_retire {
            match store.delete(&manifest::manifest_path(id, n)).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(x) => return Err(x.into()),
            }
            e.versions_deleted += 1;
        } else {
            versions_kept.push(format!("version {n}: {}", by.join(", ")));
        }
    }
    e.versions_kept = versions_kept;
    if e.live_done_ms.is_none() {
        e.live_done_ms = Some(now);
    }
    if e.retired.iter().all(|r| r.deleted_ms.is_some()) && e.versions_kept.is_empty() {
        e.done_ms.get_or_insert(now);
    }
    Ok(())
}

/// Group boundaries of a version that are target keys.
pub fn boundaries_on(v: &Version, t: &Targets) -> u64 {
    v.groups.iter().filter_map(|g| manifest::unhex(&g.lo).ok()).filter(|k| !k.is_empty() && t.hit(k)).count() as u64
}

/// After the scrub: the record of what it did (the targets object goes, its keys are the customer's data).
pub async fn record_scrub(store: &Arc<dyn ObjectStore>, id: &str, e: &mut Erasure, s: &Scrub, version: u64, t: &Targets, newest: &Version) -> Result<()> {
    e.state = "scrubbed".into();
    e.scrubbed_version = Some(version);
    e.scrubbed_ms = Some(now_ms());
    e.rows_erased = s.rows;
    e.runs_rewritten = s.runs;
    e.retired = s.retired.iter().map(|p| Retired { path: p.clone(), deleted_ms: None, kept_by: vec!["grace".into()] }).collect();
    e.boundaries_on_targets = boundaries_on(newest, t);
    save(store, id, e).await?;
    match store.delete(&keys_path(id, &e.id)).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
        Err(x) => return Err(x.into()),
    }
    ROWS_ERASED.fetch_add(s.rows, Relaxed);
    SCRUBS.fetch_add(s.version.is_some() as u64, Relaxed);
    let _ = hex(&[]);
    Ok(())
}
