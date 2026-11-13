//! Our manifest (compaction 2, step 3; plan section 6).
//!
//! Versioned objects `tenants/{db}/fx/manifest/{version:020}`, each written once with a conditional create (S3
//! `If-None-Match: *`), so of two publishers of the same version exactly one wins (probe 1, point 5). A version lists:
//! - X_d: every SlateDB row with seq <= X_d is in our runs (sealed plus delta hold the newest row <= X_d of every key);
//! - the groups (key ranges, the first starting at the empty key), each with X_g <= X_d and its sealed run (every row of
//!   the group with seq <= X_g, envelopes dropped);
//! - the delta runs, over the whole keyspace, newest first; a delta row counts for a key only when X_g < seq <= X_d;
//! - E_env of the database (`envelope.rs`), so a reader decodes SlateDB rows above X_d without a guess;
//! - the writer epoch of the publisher, and the kind: `publish` (new runs) or `fence` (a copy written by a writer at
//!   open, so a late PUT of a dead process cannot land on top of a version the new writer does not know: review of
//!   step 0, F1).
//!
//! Every run line records the run id of step 1 (16 random bytes in the footer, trailer and every block CRC); a run is
//! opened with `Run::open_expecting`, so another object at the same path is an error, never data.
//!
//! Size: one JSON object per version. About 300 bytes per group, so about 0.3 MB per publish at 1 TB with 1 GB groups
//! (ESTIMATED). The plan's split into a small root and per-shard group tables (about 64 KB per publish at 10 TB) is not
//! built here; the compactor (step 4) needs it before groups are rewritten every few minutes at 10 TB.

use anyhow::{anyhow, bail, ensure, Context, Result};
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub const FORMAT: u32 = 1;

/// One run as a version lists it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunLine {
    pub path: String,
    pub size: u64,
    /// The run id (hex), checked when the run is opened.
    pub id: String,
    pub min_seq: u64,
    pub max_seq: u64,
    /// First and last key (hex), so a reader skips a run without opening it.
    pub first: String,
    pub last: String,
    pub entries: u64,
    /// Uncompressed bytes of the run's blocks (the compactor's size measure; 0 in versions of step 3).
    #[serde(default)]
    pub raw: u64,
    /// Step 4, R1: a small delta run with a copy on the waiter's NVMe (`fx/local.rs`); readers that have the copy read it
    /// with 0 GETs. The object in the store stays the durable copy.
    #[serde(default)]
    pub local: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GroupLine {
    /// Stable id (names its runs).
    pub id: u32,
    /// First key of the group (hex); the group ends where the next one starts.
    pub lo: String,
    pub x_g: u64,
    pub sealed: Option<RunLine>,
    /// Step 4: raw bytes of the group's rows absorbed above X_g since its last reseal (the theta trigger).
    #[serde(default)]
    pub unsealed: u64,
    /// Step 4: wall-clock ms of the oldest absorption above X_g (R2, the age cap), 0 when nothing is unsealed.
    #[serde(default)]
    pub unsealed_since_ms: u64,
    /// Step 4: wall-clock ms of the group's last reseal (0: never, or a version of step 3).
    #[serde(default)]
    pub sealed_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Version {
    pub format: u32,
    pub version: u64,
    /// `publish` or `fence`.
    pub kind: String,
    pub writer_epoch: Option<u64>,
    pub x_d: u64,
    pub e_env: Option<u64>,
    pub published_ms: u64,
    pub groups: Vec<GroupLine>,
    /// Newest first.
    pub delta: Vec<RunLine>,
    pub next_group_id: u32,
    /// perf-latency round 3: the L0 SSTs of the writer's SlateDB view that an absorption put into our runs, with the
    /// highest seq of each (SST id as SlateDB prints it). A compactor that starts reads only the L0 SSTs missing here to
    /// know which are absorbed, instead of every one (it ran under the database's exclusive lock). Optional: a version
    /// without it (an older release, or `build.rs`) makes the compactor read the SSTs, as before; an older release ignores
    /// it. Format 1 unchanged.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub absorbed_l0: Vec<(String, u64)>,
}

pub fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push_str(&format!("{x:02x}"));
    }
    s
}

pub fn unhex(s: &str) -> Result<Vec<u8>> {
    ensure!(s.len() % 2 == 0, "bad hex {s:?}");
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| anyhow!("bad hex {s:?}: {e}"))).collect()
}

pub fn root(id: &str) -> String {
    format!("{}/fx", crate::lifecycle::tenant_path(id))
}

pub fn manifest_dir(id: &str) -> Path {
    Path::from(format!("{}/manifest", root(id)))
}

pub fn manifest_path(id: &str, version: u64) -> Path {
    Path::from(format!("{}/manifest/{version:020}", root(id)))
}

/// A new run's path: predictable from the group and the cut (the spare can warm it from a manifest line), plus a random
/// suffix so a retried build never overwrites an object that a version may list.
pub fn run_path(id: &str, what: &str, x: u64, epoch: Option<u64>) -> String {
    let r: u32 = rand::random();
    format!("{}/run/{what}/{x:020}-{}-{r:08x}", root(id), epoch.unwrap_or(0))
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

impl Version {
    /// The structural rules of a version: groups in key order from the empty key, X_g <= X_d, sealed rows at or below
    /// their X_g, delta rows at or below X_d, E_env at or below X_d (so every SlateDB row above X_d is an envelope).
    pub fn check(&self) -> Result<()> {
        ensure!(self.format == FORMAT, "manifest format {} (this release reads {FORMAT})", self.format);
        ensure!(!self.groups.is_empty(), "manifest {} has no group", self.version);
        ensure!(self.groups[0].lo.is_empty(), "manifest {}: the first group does not start at the empty key", self.version);
        let mut prev: Option<Vec<u8>> = None;
        for g in &self.groups {
            let lo = unhex(&g.lo)?;
            if let Some(p) = &prev {
                ensure!(*p < lo, "manifest {}: groups out of order", self.version);
            }
            prev = Some(lo);
            ensure!(g.x_g <= self.x_d, "manifest {}: group {} has X_g {} above X_d {}", self.version, g.id, g.x_g, self.x_d);
            if let Some(r) = &g.sealed {
                ensure!(r.max_seq <= g.x_g, "manifest {}: sealed run of group {} has seq {} above X_g {}", self.version, g.id, r.max_seq, g.x_g);
            }
        }
        for d in &self.delta {
            ensure!(d.max_seq <= self.x_d, "manifest {}: delta run {} has seq {} above X_d {}", self.version, d.path, d.max_seq, self.x_d);
        }
        ensure!(self.delta.windows(2).all(|w| w[0].max_seq >= w[1].max_seq), "manifest {}: delta runs not newest first", self.version);
        match self.e_env {
            Some(e) => ensure!(e <= self.x_d, "manifest {}: E_env {e} above X_d {}", self.version, self.x_d),
            None => bail!("manifest {}: no E_env (sealed runs need envelope deletes above X_d)", self.version),
        }
        Ok(())
    }

    /// The fence copy a writer publishes at open (next version number, same content).
    pub fn fence(&self, epoch: Option<u64>) -> Version {
        let mut v = self.clone();
        v.version += 1;
        v.kind = "fence".into();
        v.writer_epoch = epoch;
        v.published_ms = now_ms();
        v
    }
}

/// Release E1's announcement (rule 26): written once, before envelopes start; its existence is what followers check.
pub fn announce_path(id: &str) -> Path {
    Path::from(format!("{}/e1-announced", root(id)))
}

pub async fn announced(store: &Arc<dyn ObjectStore>, id: &str) -> Result<bool> {
    match store.head(&announce_path(id)).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

pub async fn announce(store: &Arc<dyn ObjectStore>, id: &str) -> Result<()> {
    let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
    match store.put_opts(&announce_path(id), PutPayload::from(format!("{{\"ms\":{}}}", now_ms()).into_bytes()), opts).await {
        Ok(_) | Err(object_store::Error::AlreadyExists { .. }) | Err(object_store::Error::Precondition { .. }) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Version numbers present in the store, ascending.
pub async fn list_versions(store: &Arc<dyn ObjectStore>, id: &str) -> Result<Vec<u64>> {
    let metas: Vec<_> = store.list(Some(&manifest_dir(id))).try_collect().await?;
    let mut out: Vec<u64> = metas.iter().filter_map(|m| m.location.filename().and_then(|f| f.parse::<u64>().ok())).collect();
    out.sort_unstable();
    Ok(out)
}

/// Version numbers above `after`, ascending (one LIST page from the offset; `after` 0: every version). Review of step 4,
/// FIX 4: the drop floor and GC page from the newest version already seen instead of listing every retained version.
pub async fn list_versions_after(store: &Arc<dyn ObjectStore>, id: &str, after: u64) -> Result<Vec<u64>> {
    if after == 0 {
        return list_versions(store, id).await;
    }
    let metas: Vec<_> = store.list_with_offset(Some(&manifest_dir(id)), &manifest_path(id, after)).try_collect().await?;
    let mut out: Vec<u64> = metas.iter().filter_map(|m| m.location.filename().and_then(|f| f.parse::<u64>().ok())).filter(|n| *n > after).collect();
    out.sort_unstable();
    Ok(out)
}

pub async fn load(store: &Arc<dyn ObjectStore>, id: &str, version: u64) -> Result<Version> {
    let b = store.get(&manifest_path(id, version)).await?.bytes().await?;
    let v: Version = serde_json::from_slice(&b).with_context(|| format!("manifest {version} of {id}"))?;
    ensure!(v.version == version, "manifest object {version} of {id} says version {}", v.version);
    v.check()?;
    Ok(v)
}

/// The newest version, or None when the database has never published one.
pub async fn load_newest(store: &Arc<dyn ObjectStore>, id: &str) -> Result<Option<Version>> {
    // P-perf-bytes: GC keeps only the versions in use (retention 0 by default), so the newest version listed can be
    // deleted before it is read when a newer one was published meanwhile: list again (the newer one is then listed)
    let mut tries = 0;
    loop {
        let Some(&n) = list_versions(store, id).await?.last() else { return Ok(None) };
        match load(store, id, n).await {
            Ok(v) => return Ok(Some(v)),
            Err(e) if tries < 8 && e.downcast_ref::<object_store::Error>().is_some_and(|e| matches!(e, object_store::Error::NotFound { .. })) => tries += 1,
            Err(e) => return Err(e),
        }
    }
}

/// Versions an opening or a refresh passed over because an older writer published them after a newer writer's
/// (serverStatus `olderVersionsIgnored`); the negative control of the tests trusts the newest (the code before).
pub static OLDER_VERSIONS_IGNORED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static CONTROL_TRUST_NEWEST: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Whether version `v` is an older writer's late publish (review of a60f4fe, rule 8): writer epochs never go down along
/// valid versions, so a version whose writer epoch is LOWER than the version below it landed after a newer writer's
/// (a late publish of a writer that lost the database, inside `build::fence_check`'s window). Its runs hold that writer's
/// old snapshot: serving, pairing or fencing it drops every row between its X_d and the newer writer's. One GET (the
/// version below); a version below collected by GC counts as nothing to compare with.
pub async fn is_older_writers(store: &Arc<dyn ObjectStore>, id: &str, v: &Version) -> Result<bool> {
    if CONTROL_TRUST_NEWEST.load(std::sync::atomic::Ordering::Relaxed) || v.version <= 1 {
        return Ok(false);
    }
    match load(store, id, v.version - 1).await {
        Ok(below) => Ok(below.writer_epoch.unwrap_or(0) > v.writer_epoch.unwrap_or(0)),
        Err(e) if e.downcast_ref::<object_store::Error>().is_some_and(|e| matches!(e, object_store::Error::NotFound { .. })) => Ok(false),
        Err(e) => Err(e),
    }
}

/// The newest version at or below slot `n` that is not an older writer's late publish (`is_older_writers`), each one
/// passed over logged (OLDER_VERSION_IGNORED, serverStatus `olderVersionsIgnored`).
pub async fn trusted_at(store: &Arc<dyn ObjectStore>, id: &str, mut v: Version) -> Result<Version> {
    for _ in 0..64 {
        if !is_older_writers(store, id, &v).await? {
            return Ok(v);
        }
        let below = load(store, id, v.version - 1).await?;
        OLDER_VERSIONS_IGNORED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        eprintln!(
            "OLDER_VERSION_IGNORED db={id} version={} writer_epoch={:?} x_d={}: below it version {} has writer_epoch {:?} x_d {}; using that one",
            v.version, v.writer_epoch, v.x_d, below.version, below.writer_epoch, below.x_d
        );
        v = below;
    }
    Ok(v)
}

/// The newest version an opening, a refresh or a pairing may use, with the newest slot in the store: the version keeps
/// its own number (pairs, holds and GC name versions by number), and a writer places its next version, a fence first,
/// above the newest slot. One more GET than `load_newest` in the usual case.
pub async fn load_newest_trusted(store: &Arc<dyn ObjectStore>, id: &str) -> Result<Option<(Version, u64)>> {
    let Some(newest) = load_newest(store, id).await? else { return Ok(None) };
    let slot = newest.version;
    Ok(Some((trusted_at(store, id, newest).await?, slot)))
}

/// Fail point for tests: a publish whose PUT is delayed until the test lets it go (the PUT of a dead process landing
/// late, rule 17).
pub static SLOW_PUT: std::sync::Mutex<Option<Arc<tokio::sync::Notify>>> = std::sync::Mutex::new(None);
/// Publishes waiting at that fail point.
pub static SLOW_PUT_WAITING: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Create `v` conditionally. Ok(false): another publisher already wrote that version.
pub async fn create(store: &Arc<dyn ObjectStore>, id: &str, v: &Version) -> Result<bool> {
    v.check()?;
    let body = serde_json::to_vec(v)?;
    let slow = SLOW_PUT.lock().unwrap().clone();
    if let Some(n) = slow {
        let w = n.notified();
        SLOW_PUT_WAITING.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        w.await;
    }
    let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
    // F6: no answer within the PUT timeout (the store's own retries included) is a timeout: the PUT may still land,
    // so the caller treats the outcome as unknown and fences (rule 25)
    let limit = std::time::Duration::from_millis(super::PUT_TIMEOUT_MS.load(std::sync::atomic::Ordering::Relaxed));
    match tokio::time::timeout(limit, store.put_opts(&manifest_path(id, v.version), PutPayload::from(body), opts)).await {
        Err(_) => Err(anyhow::anyhow!("manifest PUT of version {} of {id} timed out after {limit:?}: outcome unknown", v.version)),
        Ok(Ok(_)) => Ok(true),
        Ok(Err(object_store::Error::AlreadyExists { .. })) | Ok(Err(object_store::Error::Precondition { .. })) => Ok(false),
        Ok(Err(e)) => Err(e.into()),
    }
}

// ---- c2-fix-clone-uploads, review FIX 1: the absorption invariant is per database
//
// A compactor absorbs from L0 only: every SlateDB row above X_d must still be in L0 when it starts. A compaction pass
// that is not gated on absorption (`lifecycle::compact_tenant_pass_with` in a process with compactors off, or a release
// before this one) folds such rows into a sorted run; the compactor's next absorption then moves X_d past them and the
// merged reader returns their older values (review: 1,333 acknowledged updates, compactor off then on again). So:
// - an ungated pass on a database with a version of our manifest writes `fx/ungated` before it compacts;
// - a compactor starts only on a database marked `fx/caught-up` and not `fx/ungated`; otherwise the builder publishes
//   first (`fx/build.rs` scans all of SlateDB, sorted runs included, so X_d then covers every row) and the marks are
//   set. A database last served by a release without the marks has no `fx/caught-up`: it is caught up once.

pub fn ungated_path(id: &str) -> Path {
    Path::from(format!("{}/ungated", root(id)))
}

pub fn caught_up_path(id: &str) -> Path {
    Path::from(format!("{}/caught-up", root(id)))
}

/// Before an ungated pass compacts database `id`: the mark, with the time the pass started.
pub async fn note_ungated(store: &Arc<dyn ObjectStore>, id: &str) -> Result<()> {
    store.put(&ungated_path(id), PutPayload::from(serde_json::to_vec(&serde_json::json!({ "at_ms": now_ms() }))?)).await?;
    Ok(())
}

/// Whether a compactor must let the builder publish first (two HEADs).
pub async fn needs_catch_up(store: &Arc<dyn ObjectStore>, id: &str) -> Result<bool> {
    let exists = |r: object_store::Result<object_store::ObjectMeta>| match r {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(e) => Err(anyhow::Error::from(e)),
    };
    Ok(!exists(store.head(&caught_up_path(id)).await)? || exists(store.head(&ungated_path(id)).await)?)
}

/// After a builder publish whose scan started at `scan_ms`: `fx/caught-up` set, and `fx/ungated` removed unless a pass
/// marked it after the scan started (that pass may have folded rows the scan did not see: the next start catches up).
pub async fn note_caught_up(store: &Arc<dyn ObjectStore>, id: &str, scan_ms: u64) -> Result<()> {
    match store.get(&ungated_path(id)).await {
        Ok(r) => {
            let v: serde_json::Value = serde_json::from_slice(&r.bytes().await?).unwrap_or_default();
            if v.get("at_ms").and_then(|x| x.as_u64()).unwrap_or(u64::MAX) >= scan_ms {
                return Ok(());
            }
            match store.delete(&ungated_path(id)).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Err(object_store::Error::NotFound { .. }) => {}
        Err(e) => return Err(e.into()),
    }
    store.put(&caught_up_path(id), PutPayload::from(serde_json::to_vec(&serde_json::json!({ "at_ms": now_ms() }))?)).await?;
    Ok(())
}
