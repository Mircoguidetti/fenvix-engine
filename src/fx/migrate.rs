//! Migration of a database written without sealed runs (today's engine: layout v1 keys, no version of our manifest) to
//! compaction 2, online, and its rollback (compaction 2, step 8; plan section 8, plan round 2 I6, DECISIONS 2026-11-07:
//! a 7-day rollback window, then one-way).
//!
//! # What moves, and when
//!
//! A migration is a sequence of steps recorded in one state object per step, `{db}/fx/migration/{n:020}`, each written
//! with a conditional create (like our manifest), so two processes that both think they drive the migration cannot both
//! advance it: the second one finds its slot taken, reloads and sees what the first did. The database serves reads and
//! writes during every step; only the last one (publish) holds its exclusive lock, for one short catch-up.
//!
//! 1. Begin (`begin`): envelopes are on (release E1's two steps, rule 26, `Tenant::ensure_marker`). SlateDB's memtable is
//!    flushed and a SlateDB checkpoint is taken: the cut. X is the checkpoint's `last_l0_seq`: every row with seq <= X is
//!    in the checkpoint's SSTs (L0 and sorted runs), and the checkpoint keeps those SSTs from SlateDB's GC for as long as
//!    the migration needs them, across crashes and takeovers. Nothing is read through our runs yet.
//! 2. Build, group by group (`build_step`): the checkpoint's SSTs are read raw (`SstReader`, rows with their seq,
//!    SlateDB tombstones included) and merged by key, newest row per key; groups are cut by bytes, each becomes one sealed
//!    run with X_g = X (envelopes dropped, rule 14). A group is recorded in the state only after its run is complete in
//!    the store (the multipart upload finished): a crash before that leaves an orphan object and the group is built again
//!    from its first key; a crash after it resumes at the next group. The planted bug of the step's tests records the
//!    group first ([`CONTROL_MARK_EARLY`]).
//! 3. Catch-up rounds (`round`): the writes made since the cut are in SSTs that the cut's checkpoint does not list (a row
//!    with seq > X was written after the checkpoint, so it can only be in an SST created after it), or still in the
//!    memtable. Each round flushes the memtable, takes a new checkpoint, and reads only the SSTs its manifest lists that the
//!    previous round's manifest did not (SlateDB's compaction outputs since then, and new L0 SSTs). It keeps the newest row
//!    per key with seq in (X_caught, last_l0_seq] and writes them, as envelopes, to one delta run. Rounds run outside the
//!    lock, as many as needed to make the last one short.
//! 4. Publish (`Tenant::fx_migrate_finish`, under the database's exclusive lock, so no write lands meanwhile): a last
//!    round, then version 1 of this generation: the sealed groups at X, the catch-up delta runs, X_d = the last round's
//!    `last_l0_seq` (every row at or below it is in our runs: in the groups, or in a catch-up run whose window holds it;
//!    a key whose row in a window was replaced by a newer one has that newer one in a later window, or above X_d in
//!    SlateDB). The compactor starts in the same exclusive section (the server does it), so every L0 SST written after
//!    X_d stays in L0 until absorbed (step 4's gate) and no row above X_d moves into a sorted run unread.
//! 5. Window: 7 days by default (`WINDOW_MS`). The compactor runs with the drop off: SlateDB keeps every row, so a release
//!    that reads SlateDB alone (E0) still reads every acknowledged write. Double storage and compaction for a week (plan
//!    section 8).
//! 6. Commit (`commit`, at the end of the window, or by an operator): the drop goes on and SlateDB's copy of rows at or
//!    below X_d leaves. From here it is one-way.
//!
//! # Rollback, and what it cannot undo
//!
//! `rollback` (or the first open of the database by a release with sealed runs off, see `Tenant::load_fx`) marks the
//! state `rolled_back` and stops the compactor; the database is read and written as release E0 does: SlateDB alone, with
//! envelopes above E_env. Every acknowledged write is there because the drop never ran. Our runs and versions stay in the
//! store, ignored by writers ([`State::writer_uses`]); a later migration starts a new generation from a new cut.
//!
//! What it cannot undo:
//! - envelopes: E_env (the marker) stays, and E0 keeps writing envelopes above it (rule 21); a release before E0 reads
//!   envelopes as plain values. E0 is the floor; it must be everywhere before a migration begins.
//! - the commit: once the drop has run, SlateDB alone no longer holds the database. The server refuses a rollback of a
//!   committed database (`rollback` errors), and a release with sealed runs off refuses to open one (`Tenant::load_fx`,
//!   [`refuse_without_sealed_runs`]).
//! - erasures and ordinary deletes done meanwhile stay done (they are writes); forks made during the window keep their
//!   own state.

use super::manifest::{self, hex, root, unhex, GroupLine, RunLine, Version};
use crate::runfmt::{self, RunOptions, RunWriter};
use anyhow::{anyhow, bail, ensure, Context, Result};
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, GetResult, GetResultPayload, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use std::collections::{BinaryHeap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

pub const FORMAT: u32 = 1;

/// The rollback window (`--sealed-migration-window-ms`, 7 days by default, DECISIONS 2026-11-07).
pub static WINDOW_MS: AtomicU64 = AtomicU64::new(7 * 24 * 3600 * 1000);
/// Raw bytes per group of a migration (the plan's 1 GB; `--sealed-group-mb`).
pub static GROUP_BYTES: AtomicU64 = AtomicU64::new(1 << 30);
/// Bytes fetched per GET when the migration reads SlateDB's SSTs (one GET per block otherwise: 4 KiB blocks).
pub static READ_AHEAD: AtomicU64 = AtomicU64::new(8 << 20);
/// The last round runs under the lock only when the previous round read at most this many SST bytes; otherwise another
/// round runs outside it first (at most `MAX_ROUNDS_BEFORE_FINISH`).
pub static FINISH_ROUND_BYTES: AtomicU64 = AtomicU64::new(256 << 20);

/// Tests of the release before step 8 (no open-time check by a release with sealed runs off).
pub static E0_GUARD_OFF: AtomicBool = AtomicBool::new(false);

/// Negative controls of step 8's tests: a catch-up round that reads new L0 SSTs only (not the sorted runs written since
/// the previous round), and today's pass that does not skip a database read through a version.
pub static CONTROL_CATCHUP_L0_ONLY: AtomicBool = AtomicBool::new(false);
pub static CONTROL_NO_PASS_SKIP: AtomicBool = AtomicBool::new(false);

/// The planted bug of step 8's tests: a group is recorded as built before its run is complete in the store.
pub static CONTROL_MARK_EARLY: AtomicBool = AtomicBool::new(false);

/// Diagnostics (serverStatus `fenvix.migration`).
pub static GROUPS_BUILT: AtomicU64 = AtomicU64::new(0);
pub static ROUNDS: AtomicU64 = AtomicU64::new(0);
pub static ROWS_READ: AtomicU64 = AtomicU64::new(0);
/// The migration's own requests: GETs and HEADs of SlateDB's SSTs (through the read-ahead), and PUT-class requests of its
/// runs (multipart create, parts, complete) and state objects.
pub static REQ_GETS: AtomicU64 = AtomicU64::new(0);
pub static REQ_HEADS: AtomicU64 = AtomicU64::new(0);
pub static REQ_PUTS: AtomicU64 = AtomicU64::new(0);
pub static SST_BYTES_READ: AtomicU64 = AtomicU64::new(0);
pub static PUBLISHED: AtomicU64 = AtomicU64::new(0);
pub static ROLLBACKS: AtomicU64 = AtomicU64::new(0);
pub static REFUSED: AtomicU64 = AtomicU64::new(0);
pub static PASSES_SKIPPED: AtomicU64 = AtomicU64::new(0);
/// The longest a publish held the database's lock, in microseconds.
pub static LOCK_US: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Building,
    Published,
    Committed,
    RolledBack,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct State {
    pub format: u32,
    /// The slot of this state object.
    pub n: u64,
    /// 1 for the first migration of the database, +1 after each rollback.
    pub gen: u32,
    pub phase: Phase,
    pub started_ms: u64,
    pub e_env: u64,
    /// The cut: a SlateDB checkpoint and the SlateDB manifest it pins.
    pub checkpoint: Option<String>,
    pub checkpoint_manifest: u64,
    pub x: u64,
    pub group_bytes: u64,
    /// Groups whose sealed run is complete in the store, in key order from the empty key.
    pub groups: Vec<GroupLine>,
    pub next_group_id: u32,
    /// First key (hex) of the next group to build; None before the first.
    pub next_lo: Option<String>,
    pub built: bool,
    /// Catch-up delta runs, newest first.
    pub catchup: Vec<RunLine>,
    /// Every row with seq in (X, x_caught] is in a catch-up run (or replaced by a newer row in a later window).
    pub x_caught: u64,
    /// The last round's checkpoint and the manifest it pins (None: the cut's).
    pub round_checkpoint: Option<String>,
    pub round_manifest: u64,
    /// The version published by this generation.
    pub version: Option<u64>,
    pub published_ms: u64,
    pub window_ms: u64,
    /// Versions at or below this belong to an earlier generation.
    pub ignore_upto: u64,
    pub committed_ms: u64,
    pub rolled_back_ms: u64,
    /// Who changed the phase last, and why (operators).
    pub note: String,
}

impl State {
    /// Whether a writer may serve with version `v`: only versions of a published or committed generation. A writer of a
    /// database still building, or rolled back, reads SlateDB alone (it has every row: the drop never ran) and starts no
    /// compactor. Readers in other processes may still read with an older version: that is right while the drop is off.
    pub fn writer_uses(&self, v: u64) -> bool {
        matches!(self.phase, Phase::Published | Phase::Committed) && v > self.ignore_upto
    }

    /// Whether the compactor of this database may drop SlateDB rows (the commit).
    pub fn drop_allowed(&self) -> bool {
        self.phase == Phase::Committed
    }

    pub fn window_ends_ms(&self) -> u64 {
        self.published_ms.saturating_add(self.window_ms)
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "generation": self.gen,
            "phase": self.phase,
            "x": self.x,
            "groupsBuilt": self.groups.len(),
            "built": self.built,
            "catchupRuns": self.catchup.len(),
            "xCaught": self.x_caught,
            "version": self.version,
            "publishedMs": self.published_ms,
            "windowEndsMs": if self.published_ms > 0 { self.window_ends_ms() } else { 0 },
            "committedMs": self.committed_ms,
            "rolledBackMs": self.rolled_back_ms,
            "note": self.note,
        })
    }
}

fn dir(id: &str) -> Path {
    Path::from(format!("{}/migration", root(id)))
}

fn slot(id: &str, n: u64) -> Path {
    Path::from(format!("{}/migration/{n:020}", root(id)))
}

/// The newest state of the database's migration, None when it never began one.
pub async fn load(store: &Arc<dyn ObjectStore>, id: &str) -> Result<Option<State>> {
    let metas: Vec<_> = store.list(Some(&dir(id))).try_collect().await?;
    let Some(n) = metas.iter().filter_map(|m| m.location.filename().and_then(|f| f.parse::<u64>().ok())).max() else { return Ok(None) };
    let b = store.get(&slot(id, n)).await?.bytes().await?;
    let s: State = serde_json::from_slice(&b).with_context(|| format!("migration state {n} of {id}"))?;
    ensure!(s.format == FORMAT && s.n == n, "migration state {n} of {id}: format {} slot {}", s.format, s.n);
    Ok(Some(s))
}

/// A save that found its slot taken: another process advanced the migration; reload.
#[derive(Debug)]
pub struct Raced(pub String);

impl std::fmt::Display for Raced {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Raced {}

/// Write `s` at the next slot (a conditional create). An error with an unknown outcome is an error: the caller reloads.
pub async fn save(store: &Arc<dyn ObjectStore>, id: &str, s: &mut State) -> Result<()> {
    s.n += 1;
    let body = serde_json::to_vec(s)?;
    let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
    REQ_PUTS.fetch_add(1, Relaxed);
    match store.put_opts(&slot(id, s.n), PutPayload::from(body), opts).await {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) | Err(object_store::Error::Precondition { .. }) => Err(anyhow::Error::new(Raced(format!("migration state {} of {id} was written by another process", s.n)))),
        Err(e) => Err(e.into()),
    }
}

fn admin(store: &Arc<dyn ObjectStore>, id: &str) -> slatedb::admin::Admin {
    slatedb::admin::Admin::builder(crate::lifecycle::tenant_path(id), Arc::clone(store)).build()
}

async fn read_manifest(store: &Arc<dyn ObjectStore>, id: &str, mid: u64) -> Result<slatedb::VersionedManifest> {
    admin(store, id).read_manifest(Some(mid)).await?.ok_or_else(|| anyhow!("database {id}: SlateDB manifest {mid} is gone"))
}

/// Flush the memtable and take a checkpoint (no expiry; the migration deletes it). Returns its id and manifest.
async fn cut(db: &slatedb::Db, store: &Arc<dyn ObjectStore>, id: &str, name: &str) -> Result<(uuid::Uuid, slatedb::VersionedManifest)> {
    db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await?;
    let c = db.create_checkpoint(slatedb::config::CheckpointScope::All, &slatedb::config::CheckpointOptions { lifetime: None, source: None, name: Some(name.into()) }).await?;
    let m = read_manifest(store, id, c.manifest_id).await?;
    Ok((c.id, m))
}

async fn delete_checkpoint(store: &Arc<dyn ObjectStore>, id: &str, c: &str) {
    if let Ok(u) = uuid::Uuid::parse_str(c) {
        if let Err(e) = admin(store, id).delete_checkpoint(u).await {
            eprintln!("migration of {id}: checkpoint {c} not deleted ({e}); it keeps SlateDB's SSTs until deleted");
        }
    }
}

/// Every SST view of a manifest (L0 and sorted runs).
fn views(m: &slatedb::VersionedManifest) -> Vec<slatedb::manifest::SsTableView> {
    m.l0().iter().cloned().chain(m.compacted().iter().flat_map(|sr| sr.sst_views().iter().cloned())).collect()
}

// ---------------------------------------------------------------------------------------------------------------------
// Begin

/// Begin a migration of a writer's database (or a new generation after a rollback). `e_env` is the database's marker
/// seq (envelopes on). Refused when a migration is under way or done, or when the database already has versions of its
/// own (it is on compaction 2 already).
pub async fn begin(store: &Arc<dyn ObjectStore>, id: &str, db: &slatedb::Db, e_env: u64, prev: Option<State>) -> Result<State> {
    let versions = manifest::list_versions(store, id).await?;
    let (gen, n, ignore_upto) = match &prev {
        None => {
            ensure!(versions.is_empty(), "database {id} has versions of our manifest and no migration: it is on compaction 2 already");
            (1, 0, 0)
        }
        Some(p) if p.phase == Phase::RolledBack => (p.gen + 1, p.n, versions.last().copied().unwrap_or(0).max(p.ignore_upto)),
        Some(p) => bail!("database {id}: a migration is {:?} (generation {})", p.phase, p.gen),
    };
    ensure!(db.manifest().external_dbs().is_empty(), "database {id} is a clone that still reads its parent's SSTs: migrate after its own compaction copied them");
    let (c, m) = cut(db, store, id, "fx-migration-cut").await?;
    let x = m.last_l0_seq();
    if e_env > x {
        delete_checkpoint(store, id, &c.to_string()).await;
        bail!("database {id}: the envelope marker (seq {e_env}) is not in an SST yet; begin again");
    }
    let mut s = State {
        format: FORMAT,
        n,
        gen,
        phase: Phase::Building,
        started_ms: manifest::now_ms(),
        e_env,
        checkpoint: Some(c.to_string()),
        checkpoint_manifest: m.id(),
        x,
        group_bytes: GROUP_BYTES.load(Relaxed),
        groups: Vec::new(),
        next_group_id: 0,
        next_lo: None,
        built: false,
        catchup: Vec::new(),
        x_caught: x,
        round_checkpoint: None,
        round_manifest: m.id(),
        version: None,
        published_ms: 0,
        window_ms: WINDOW_MS.load(Relaxed),
        ignore_upto,
        committed_ms: 0,
        rolled_back_ms: 0,
        note: "begin".into(),
    };
    if let Err(e) = save(store, id, &mut s).await {
        delete_checkpoint(store, id, &c.to_string()).await;
        return Err(e);
    }
    Ok(s)
}

// ---------------------------------------------------------------------------------------------------------------------
// The raw merged read of SlateDB's SSTs

/// An object store that reads ahead: a ranged GET of an object fetches `READ_AHEAD` bytes from where it starts, and the
/// next ranges inside that chunk come from memory. SlateDB's `SstReader` reads one block per request (4 KiB blocks in the
/// stored format); a migration reads every block in order, so this turns about 2,000 GETs into one per 8 MiB.
pub struct ReadAhead {
    inner: Arc<dyn ObjectStore>,
    chunk: u64,
    /// A few chunks per object: SlateDB's raw reader reads the SST's index (at its end) again before every block.
    cache: Mutex<std::collections::HashMap<Path, Vec<(u64, Bytes, object_store::ObjectMeta)>>>,
    order: Mutex<std::collections::VecDeque<Path>>,
    sizes: Mutex<std::collections::HashMap<Path, object_store::ObjectMeta>>,
}

impl std::fmt::Debug for ReadAhead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadAhead({})", self.inner)
    }
}

impl std::fmt::Display for ReadAhead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadAhead({})", self.inner)
    }
}

impl ReadAhead {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Arc<ReadAhead> {
        Arc::new(ReadAhead { inner, chunk: READ_AHEAD.load(Relaxed).max(64 << 10), cache: Default::default(), order: Default::default(), sizes: Default::default() })
    }

    /// Chunks kept at most (one per SST being read; the oldest goes first).
    const KEEP: usize = 128;

    async fn meta(&self, p: &Path) -> object_store::Result<object_store::ObjectMeta> {
        if let Some(m) = self.sizes.lock().unwrap().get(p) {
            return Ok(m.clone());
        }
        REQ_HEADS.fetch_add(1, Relaxed);
        let m = self.inner.head(p).await?;
        self.sizes.lock().unwrap().insert(p.clone(), m.clone());
        Ok(m)
    }

    fn result(meta: object_store::ObjectMeta, range: std::ops::Range<u64>, b: Bytes) -> GetResult {
        GetResult { payload: GetResultPayload::Stream(Box::pin(futures::stream::once(async move { Ok(b) }))), meta, range, attributes: Default::default(), extensions: Default::default() }
    }
}

#[async_trait::async_trait]
impl ObjectStore for ReadAhead {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let plain = options.if_match.is_none() && options.if_none_match.is_none() && options.if_modified_since.is_none() && options.if_unmodified_since.is_none() && options.version.is_none() && !options.head;
        let Some(GetRange::Bounded(r)) = options.range.clone().filter(|_| plain) else {
            REQ_GETS.fetch_add(1, Relaxed);
            return self.inner.get_opts(location, options).await;
        };
        if let Some(chunks) = self.cache.lock().unwrap().get(location) {
            for (start, b, meta) in chunks {
                if r.start >= *start && r.end <= start + b.len() as u64 {
                    let s = (r.start - start) as usize;
                    let e = (r.end - start) as usize;
                    return Ok(Self::result(meta.clone(), r.clone(), b.slice(s..e)));
                }
            }
        }
        let meta = self.meta(location).await?;
        let end = r.end.max(r.start + self.chunk).min(meta.size);
        REQ_GETS.fetch_add(1, Relaxed);
        if end < r.end {
            return self.inner.get_opts(location, options).await;
        }
        let got = self.inner.get_opts(location, GetOptions { range: Some(GetRange::Bounded(r.start..end)), ..Default::default() }).await?;
        let b = got.bytes().await?;
        SST_BYTES_READ.fetch_add(b.len() as u64, Relaxed);
        let s = 0usize;
        let e = (r.end - r.start) as usize;
        let out = Self::result(meta.clone(), r.clone(), b.slice(s..e));
        let mut c = self.cache.lock().unwrap();
        let mut o = self.order.lock().unwrap();
        let chunks = c.entry(location.clone()).or_default();
        let fresh = chunks.is_empty();
        chunks.push((r.start, b, meta));
        if chunks.len() > 3 {
            chunks.remove(0);
        }
        if fresh {
            o.push_back(location.clone());
            while o.len() > Self::KEEP {
                if let Some(old) = o.pop_front() {
                    c.remove(&old);
                    self.sizes.lock().unwrap().remove(&old);
                }
            }
        }
        Ok(out)
    }

    fn delete_stream(&self, locations: futures::stream::BoxStream<'static, object_store::Result<Path>>) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: object_store::CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: object_store::RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// One raw row: key, seq, and the value as stored (None: a SlateDB tombstone).
#[derive(Clone, Debug)]
pub struct RawRow {
    pub key: Bytes,
    pub seq: u64,
    pub value: Option<Bytes>,
}

/// One sorted source: an L0 SST, or the SSTs of one sorted run in key order. Reads one block at a time.
struct Src {
    ssts: Vec<slatedb::manifest::SsTableView>,
    i: usize,
    file: Option<(slatedb::SstFile, usize, usize)>,
    rows: std::collections::VecDeque<RawRow>,
    lo: Vec<u8>,
}

impl Src {
    async fn next(&mut self, reader: &slatedb::SstReader) -> Result<Option<RawRow>> {
        loop {
            if let Some(r) = self.rows.pop_front() {
                return Ok(Some(r));
            }
            if self.file.is_none() {
                let Some(view) = self.ssts.get(self.i) else { return Ok(None) };
                let f = reader.open_with_handle(view.sst.clone())?;
                let idx = f.index().await?;
                // the first block that can hold a key >= lo: the last block whose first key is <= lo
                let b0 = if self.lo.is_empty() { 0 } else { idx.partition_point(|k| k <= &self.lo[..]).saturating_sub(1) };
                let n = idx.len();
                self.file = Some((f, b0, n));
            }
            let (f, b, n) = self.file.as_mut().unwrap();
            if *b >= *n {
                self.file = None;
                self.i += 1;
                continue;
            }
            let view = &self.ssts[self.i];
            let vr = view.visible_range().map(|r| { use std::ops::RangeBounds; (r.start_bound().cloned(), r.end_bound().cloned()) });
            for e in f.read_block(*b).await? {
                if &e.key[..] < &self.lo[..] {
                    continue;
                }
                if let Some((s, t)) = &vr {
                    use std::ops::Bound::*;
                    let ok_lo = match s {
                        Included(x) => e.key >= *x,
                        Excluded(x) => e.key > *x,
                        Unbounded => true,
                    };
                    let ok_hi = match t {
                        Included(x) => e.key <= *x,
                        Excluded(x) => e.key < *x,
                        Unbounded => true,
                    };
                    if !(ok_lo && ok_hi) {
                        continue;
                    }
                }
                let value = match e.value {
                    slatedb::ValueDeletable::Value(v) => Some(v),
                    slatedb::ValueDeletable::Tombstone => None,
                    slatedb::ValueDeletable::Merge(_) => bail!("a merge operand in SST {:?} (I4: merge operators are never on)", view.sst.id),
                };
                self.rows.push_back(RawRow { key: e.key, seq: e.seq, value });
            }
            *b += 1;
        }
    }
}

/// The merge of a manifest's SSTs (or some of them) from key `lo` up, newest row per key among rows with seq in
/// (`seq_lo`, `seq_hi`]. Rows outside the window are skipped (an older row of a key in the window does not hide it).
pub struct Merge {
    reader: slatedb::SstReader,
    srcs: Vec<Src>,
    heap: BinaryHeap<std::cmp::Reverse<(Bytes, std::cmp::Reverse<u64>, usize, usize)>>,
    heads: Vec<Option<RawRow>>,
    seq_lo: u64,
    seq_hi: u64,
}

impl Merge {
    pub async fn open(store: &Arc<dyn ObjectStore>, id: &str, views: Vec<slatedb::manifest::SsTableView>, sorted_runs: Vec<Vec<slatedb::manifest::SsTableView>>, lo: &[u8], seq_lo: u64, seq_hi: u64) -> Result<Merge> {
        let ra = ReadAhead::new(Arc::clone(store));
        let ra_store: Arc<dyn ObjectStore> = ra.clone();
        // SlateDB's raw reader decodes the SST's index again for every block unless a metadata cache holds it: a small one
        // of its own (no data blocks: each is read once)
        let meta: Arc<dyn slatedb::db_cache::DbCache> = Arc::new(crate::meter::MeteredCache::new(64 << 20));
        let cache: Arc<dyn slatedb::db_cache::DbCache> = Arc::new(slatedb::db_cache::SplitCache::new().with_block_cache(None).with_meta_cache(Some(meta)).build());
        let reader = slatedb::SstReader::new(crate::lifecycle::tenant_path(id), ra_store, Some(cache), None);
        let mut srcs: Vec<Src> = views.into_iter().map(|v| Src { ssts: vec![v], i: 0, file: None, rows: Default::default(), lo: lo.to_vec() }).collect();
        for mut sr in sorted_runs {
            // skip the SSTs of a sorted run that end before lo (the next one starts at or before lo)
            let mut i = 0;
            if !lo.is_empty() {
                while i + 1 < sr.len() && sr[i + 1].sst.info.first_entry.as_ref().is_some_and(|k| &k[..] <= lo) {
                    i += 1;
                }
            }
            sr.drain(..i);
            srcs.push(Src { ssts: sr, i: 0, file: None, rows: Default::default(), lo: lo.to_vec() });
        }
        let mut m = Merge { reader, heads: (0..srcs.len()).map(|_| None).collect(), srcs, heap: BinaryHeap::new(), seq_lo, seq_hi };
        for s in 0..m.srcs.len() {
            m.pull(s).await?;
        }
        Ok(m)
    }

    async fn pull(&mut self, s: usize) -> Result<()> {
        loop {
            let r = self.srcs[s].next(&self.reader).await?;
            match r {
                Some(r) if r.seq <= self.seq_lo || r.seq > self.seq_hi => continue,
                Some(r) => {
                    ROWS_READ.fetch_add(1, Relaxed);
                    self.heap.push(std::cmp::Reverse((r.key.clone(), std::cmp::Reverse(r.seq), s, 0)));
                    self.heads[s] = Some(r);
                    return Ok(());
                }
                None => {
                    self.heads[s] = None;
                    return Ok(());
                }
            }
        }
    }

    /// The next key's newest row in the window.
    pub async fn next(&mut self) -> Result<Option<RawRow>> {
        let Some(std::cmp::Reverse((key, _, s, _))) = self.heap.pop() else { return Ok(None) };
        let best = self.heads[s].take().expect("head");
        self.pull(s).await?;
        // older rows of the same key from other sources (or the same one): skip
        while let Some(std::cmp::Reverse((k, _, s2, _))) = self.heap.peek().cloned() {
            if k != key {
                break;
            }
            self.heap.pop();
            self.heads[s2] = None;
            self.pull(s2).await?;
        }
        Ok(Some(best))
    }
}

/// The views of a manifest as merge sources: L0 SSTs one by one, sorted runs whole.
fn sources(m: &slatedb::VersionedManifest, only: Option<&HashSet<ulid::Ulid>>) -> (Vec<slatedb::manifest::SsTableView>, Vec<Vec<slatedb::manifest::SsTableView>>) {
    let keep = |v: &slatedb::manifest::SsTableView| only.is_none_or(|o| o.contains(&v.id));
    let l0 = m.l0().iter().filter(|v| keep(v)).cloned().collect();
    let srs = m.compacted().iter().map(|sr| sr.sst_views().iter().filter(|v| keep(v)).cloned().collect::<Vec<_>>()).filter(|v: &Vec<_>| !v.is_empty()).collect();
    (l0, srs)
}

/// The envelope a row is stored with in our runs (rows at or below E_env, and the marker, are plain values; a SlateDB
/// tombstone is a delete).
fn envelope(e_env: u64, r: &RawRow) -> Result<Vec<u8>> {
    let Some(v) = &r.value else { return Ok(runfmt::deleted()) };
    if r.seq <= e_env || &r.key[..] == crate::envelope::MARKER {
        return Ok(runfmt::live(v));
    }
    match v.first() {
        Some(&crate::envelope::LIVE) => Ok(v.to_vec()),
        Some(&crate::envelope::DEAD) if v.len() == 1 => Ok(v.to_vec()),
        _ => bail!("corrupt envelope at seq {} (E_env {e_env})", r.seq),
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// Build, group by group

/// Fail points of the tests: called with a point's name, an error there is a crash at that point.
pub type Fail<'a> = Option<&'a (dyn Fn(&str) -> Result<()> + Sync)>;

#[derive(Clone, Debug, Default, Serialize)]
pub struct StepReport {
    pub groups: usize,
    pub rows: u64,
    pub built: bool,
}

fn run_path(id: &str, what: &str, x: u64, gen: u32) -> String {
    let r: u32 = rand::random();
    format!("{}/run/{what}/{x:020}-m{gen}-{r:08x}", root(id))
}

/// Build at most `max_groups` more groups (0: all). Returns what was built; `built` once the whole keyspace is.
pub async fn build_step(store: &Arc<dyn ObjectStore>, id: &str, max_groups: usize, opts: &RunOptions, fail: Fail<'_>) -> Result<StepReport> {
    let mut s = load(store, id).await?.ok_or_else(|| anyhow!("database {id}: no migration"))?;
    ensure!(s.phase == Phase::Building, "database {id}: the migration is {:?}", s.phase);
    let mut rep = StepReport { built: s.built, ..Default::default() };
    if s.built {
        return Ok(rep);
    }
    let m = read_manifest(store, id, s.checkpoint_manifest).await?;
    let lo = match &s.next_lo {
        Some(h) => unhex(h)?,
        None => Vec::new(),
    };
    let (l0, srs) = sources(&m, None);
    let mut merge = Merge::open(store, id, l0, srs, &lo, 0, s.x).await?;
    let mut g_lo = lo.clone();
    let mut g_bytes = 0u64;
    let mut w: Option<RunWriter> = None;
    let mut path = String::new();
    loop {
        let r = merge.next().await?;
        // the group is full: the key r starts the next one
        let full = r.as_ref().is_some_and(|_| g_bytes >= s.group_bytes);
        if r.is_none() || full {
            // finish this group: its run complete in the store, then recorded
            let next = r.as_ref().map(|r| r.key.to_vec());
            let line_of = |m: runfmt::RunMeta| super::build::line(&m);
            let gid = s.next_group_id;
            let mut planned: Option<RunLine> = None;
            if CONTROL_MARK_EARLY.load(Relaxed) {
                // the planted bug: record the group (with the run it will have) before the upload completes
                if let Some(wr) = w.as_ref() {
                    planned = Some(RunLine { path: path.clone(), size: 0, id: hex(wr.run_id()), min_seq: 0, max_seq: 0, first: String::new(), last: String::new(), entries: 0, raw: 0, local: false });
                }
                s.groups.push(GroupLine { id: gid, lo: hex(&g_lo), x_g: s.x, sealed: planned.clone(), unsealed: 0, unsealed_since_ms: 0, sealed_ms: manifest::now_ms() });
                s.next_group_id += 1;
                s.next_lo = next.as_ref().map(|k| hex(k));
                s.built = next.is_none();
                save(store, id, &mut s).await?;
                if let Some(f) = fail {
                    f("after_mark")?;
                }
            }
            if let Some(f) = fail {
                f("before_finish")?;
            }
            let line = match w.take() {
                Some(wr) => {
                    let m = wr.finish().await?;
                    REQ_PUTS.fetch_add(m.puts, Relaxed);
                    Some(line_of(m))
                }
                None => None,
            };
            if let Some(f) = fail {
                f("after_finish")?;
            }
            if CONTROL_MARK_EARLY.load(Relaxed) {
                if let (Some(g), Some(l)) = (s.groups.last_mut(), line) {
                    g.sealed = Some(l);
                    save(store, id, &mut s).await?;
                }
            } else {
                s.groups.push(GroupLine { id: gid, lo: hex(&g_lo), x_g: s.x, sealed: line, unsealed: 0, unsealed_since_ms: 0, sealed_ms: manifest::now_ms() });
                s.next_group_id += 1;
                s.next_lo = next.as_ref().map(|k| hex(k));
                s.built = next.is_none();
                save(store, id, &mut s).await?;
            }
            GROUPS_BUILT.fetch_add(1, Relaxed);
            rep.groups += 1;
            if let Some(f) = fail {
                f("after_group")?;
            }
            rep.built = s.built;
            if s.built || (max_groups > 0 && rep.groups >= max_groups) {
                return Ok(rep);
            }
            g_lo = next.unwrap();
            g_bytes = 0;
        }
        let r = r.unwrap();
        rep.rows += 1;
        g_bytes += (r.key.len() + r.value.as_ref().map_or(0, |v| v.len())) as u64;
        let env = envelope(s.e_env, &r)?;
        if env.first() == Some(&runfmt::DELETED) {
            continue; // a sealed run holds no envelopes (rule 14)
        }
        if w.is_none() {
            path = run_path(id, &format!("g{}", s.next_group_id), s.x, s.gen);
            w = Some(RunWriter::new(store, Path::from(path.as_str()), opts.clone()).await?);
        }
        w.as_mut().unwrap().add(&r.key, r.seq, &env).await?;
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// Catch-up

#[derive(Clone, Debug, Default, Serialize)]
pub struct RoundReport {
    pub x_from: u64,
    pub x_to: u64,
    pub new_ssts: usize,
    pub sst_bytes: u64,
    pub rows: u64,
}

/// One catch-up round (see the module comment, step 3). `db` is the writer of the database (a clone of it is fine: a
/// fenced writer fails the flush, so a process that lost the database cannot add a round).
pub async fn round(store: &Arc<dyn ObjectStore>, id: &str, db: &slatedb::Db, opts: &RunOptions, fail: Fail<'_>) -> Result<RoundReport> {
    let mut s = load(store, id).await?.ok_or_else(|| anyhow!("database {id}: no migration"))?;
    ensure!(s.phase == Phase::Building, "database {id}: the migration is {:?}", s.phase);
    let prev = read_manifest(store, id, s.round_manifest).await?;
    let (c, m) = cut(db, store, id, "fx-migration-round").await?;
    let r = async {
        let seen: HashSet<slatedb::manifest::SsTableId> = views(&prev).into_iter().map(|v| v.sst.id).collect();
        let new: HashSet<ulid::Ulid> = views(&m).into_iter().filter(|v| !seen.contains(&v.sst.id)).map(|v| v.id).collect();
        let mut rep = RoundReport { x_from: s.x_caught, x_to: m.last_l0_seq(), new_ssts: new.len(), ..Default::default() };
        ensure!(rep.x_to >= rep.x_from, "database {id}: SlateDB's last L0 seq {} is below the migration's {}", rep.x_to, rep.x_from);
        let b0 = SST_BYTES_READ.load(Relaxed);
        let (l0, mut srs) = sources(&m, Some(&new));
        if CONTROL_CATCHUP_L0_ONLY.load(Relaxed) {
            srs.clear();
        }
        let mut merge = Merge::open(store, id, l0, srs, b"", s.x_caught, rep.x_to).await?;
        let mut w: Option<RunWriter> = None;
        while let Some(r) = merge.next().await? {
            rep.rows += 1;
            let env = envelope(s.e_env, &r)?;
            if w.is_none() {
                w = Some(RunWriter::new(store, Path::from(run_path(id, "delta", rep.x_to, s.gen).as_str()), opts.clone()).await?);
            }
            w.as_mut().unwrap().add(&r.key, r.seq, &env).await?;
        }
        let line = match w {
            Some(w) => {
                let m = w.finish().await?;
                REQ_PUTS.fetch_add(m.puts, Relaxed);
                Some(super::build::line(&m))
            }
            None => None,
        };
        rep.sst_bytes = SST_BYTES_READ.load(Relaxed) - b0;
        if let Some(f) = fail {
            f("round_before_save")?;
        }
        let old = s.round_checkpoint.take();
        if let Some(l) = line {
            s.catchup.insert(0, l);
        }
        s.x_caught = rep.x_to;
        s.round_checkpoint = Some(c.to_string());
        s.round_manifest = m.id();
        s.note = format!("round to {}", rep.x_to);
        save(store, id, &mut s).await?;
        if let Some(o) = old {
            delete_checkpoint(store, id, &o).await;
        }
        ROUNDS.fetch_add(1, Relaxed);
        Ok(rep)
    }
    .await;
    if r.is_err() {
        delete_checkpoint(store, id, &c.to_string()).await;
    }
    r
}

/// The run options of the server's migrations: sealed runs as the builder writes them, catch-up runs as delta runs.
pub fn sealed_opts() -> RunOptions {
    if super::compactor::SERVER_SMALL.load(Relaxed) {
        return RunOptions::unchecked(16 << 10, 1 << 20, runfmt::CustomerKind::V2);
    }
    RunOptions::sealed(runfmt::CustomerKind::V2)
}

pub fn delta_opts() -> RunOptions {
    if super::compactor::SERVER_SMALL.load(Relaxed) {
        return RunOptions::unchecked(16 << 10, 1 << 20, runfmt::CustomerKind::V2);
    }
    RunOptions { block_bytes: runfmt::MIN_BLOCK, part_bytes: runfmt::DEFAULT_PART, customers: runfmt::CustomerKind::V2 }
}

/// The version a finished build publishes: its groups, its catch-up runs, X_d = X_caught. `next` is the version number.
pub fn compose(s: &State, next: u64, epoch: Option<u64>) -> Result<Version> {
    ensure!(s.built, "the build is not finished");
    let mut delta = s.catchup.clone();
    delta.sort_by(|a, b| b.max_seq.cmp(&a.max_seq));
    let v = Version { format: manifest::FORMAT, version: next, kind: "migrate".into(), writer_epoch: epoch, x_d: s.x_caught, e_env: Some(s.e_env), published_ms: manifest::now_ms(), groups: s.groups.clone(), delta, next_group_id: s.next_group_id };
    v.check()?;
    Ok(v)
}

/// After the version is created: record the phase, then delete the checkpoints (they were only for the migration).
pub async fn published(store: &Arc<dyn ObjectStore>, id: &str, s: &mut State, version: u64) -> Result<()> {
    s.phase = Phase::Published;
    s.version = Some(version);
    s.published_ms = manifest::now_ms();
    s.note = format!("published version {version}");
    let cps: Vec<String> = s.checkpoint.take().into_iter().chain(s.round_checkpoint.take()).collect();
    save(store, id, s).await?;
    for c in cps {
        delete_checkpoint(store, id, &c).await;
    }
    PUBLISHED.fetch_add(1, Relaxed);
    Ok(())
}

/// Every run a building or published state lists exists in the store, with its id (the test of "a group is recorded
/// only once its run is durable"). Returns the problems found.
pub async fn check_runs(store: &Arc<dyn ObjectStore>, s: &State) -> Vec<String> {
    let mut out = Vec::new();
    for l in s.groups.iter().filter_map(|g| g.sealed.as_ref()).chain(s.catchup.iter()) {
        let rid: Option<[u8; runfmt::RUN_ID_LEN]> = unhex(&l.id).ok().and_then(|v| v.try_into().ok());
        let Some(rid) = rid else {
            out.push(format!("run {} listed by migration state {}: bad run id {:?}", l.path, s.n, l.id));
            continue;
        };
        match runfmt::Run::open_expecting(store, Path::from(l.path.as_str()), l.size, &rid).await {
            Ok(_) => {}
            Err(e) => out.push(format!("run {} listed by migration state {}: {e:#}", l.path, s.n)),
        }
    }
    out
}

// ---------------------------------------------------------------------------------------------------------------------
// Commit and rollback

/// The commit: from here the drop may run and the migration is one-way. Refused before the window ends unless `force`.
pub async fn commit(store: &Arc<dyn ObjectStore>, id: &str, force: bool, note: &str) -> Result<State> {
    let mut s = load(store, id).await?.ok_or_else(|| anyhow!("database {id}: no migration"))?;
    match s.phase {
        Phase::Committed => return Ok(s),
        Phase::Published => {}
        p => bail!("database {id}: cannot commit a migration that is {p:?}"),
    }
    ensure!(force || manifest::now_ms() >= s.window_ends_ms(), "database {id}: the rollback window ends at {} ms (now {}); commit before it only with force", s.window_ends_ms(), manifest::now_ms());
    s.phase = Phase::Committed;
    s.committed_ms = manifest::now_ms();
    s.note = note.into();
    save(store, id, &mut s).await?;
    Ok(s)
}

/// Why a rollback is refused: the commit happened (the drop may have run: SlateDB alone no longer has the database).
pub fn rollback_refusal(s: &State) -> Option<String> {
    match s.phase {
        Phase::Committed => Some(format!("the migration was committed at {} ms: SlateDB's copy of the rows at or below X_d may be dropped, a release without sealed runs cannot read this database any more (one-way, DECISIONS 2026-11-07)", s.committed_ms)),
        _ => None,
    }
}

/// Mark the migration rolled back (the caller stopped the compactor and serves the database from SlateDB alone). A
/// building migration is abandoned the same way. Idempotent.
pub async fn rollback(store: &Arc<dyn ObjectStore>, id: &str, note: &str) -> Result<State> {
    let mut s = load(store, id).await?.ok_or_else(|| anyhow!("database {id}: no migration"))?;
    if s.phase == Phase::RolledBack {
        return Ok(s);
    }
    if let Some(r) = rollback_refusal(&s) {
        REFUSED.fetch_add(1, Relaxed);
        bail!("database {id}: rollback refused: {r}");
    }
    s.ignore_upto = manifest::list_versions(store, id).await?.last().copied().unwrap_or(0).max(s.ignore_upto);
    s.phase = Phase::RolledBack;
    s.rolled_back_ms = manifest::now_ms();
    s.note = note.into();
    let cps: Vec<String> = s.checkpoint.take().into_iter().chain(s.round_checkpoint.take()).collect();
    save(store, id, &mut s).await?;
    for c in cps {
        delete_checkpoint(store, id, &c).await;
    }
    ROLLBACKS.fetch_add(1, Relaxed);
    Ok(s)
}

/// A release with sealed runs off opening database `id` (E0): refused when its migration is committed; a migration that
/// is building or inside its window is rolled back first (writers only), so the next release with sealed runs does not
/// resume from a version this release's writes went around. Returns the refusal, if any.
pub async fn refuse_without_sealed_runs(store: &Arc<dyn ObjectStore>, id: &str, writer: bool) -> Result<()> {
    let Some(s) = load(store, id).await? else { return Ok(()) };
    match s.phase {
        Phase::Committed => {
            REFUSED.fetch_add(1, Relaxed);
            bail!("database {id} was migrated to compaction 2 and committed at {} ms: this release reads SlateDB alone and SlateDB no longer holds every row; run a release with --sealed-runs", s.committed_ms)
        }
        Phase::Published | Phase::Building if writer => {
            rollback(store, id, "opened by a release with sealed runs off (E0)").await.map(|_| ()).or_else(|e| if e.downcast_ref::<Raced>().is_some() { Ok(()) } else { Err(e) })
        }
        _ => Ok(()),
    }
}

/// Whether today's compaction pass must leave the database alone: a published or committed migration (its compactor
/// absorbs L0 SSTs; a row above X_d moved into a sorted run before the compactor started would be skipped by every read),
/// or versions of a database never migrated (step 3's and 4's own publishes).
pub async fn pass_must_skip(store: &Arc<dyn ObjectStore>, id: &str) -> Result<bool> {
    if CONTROL_NO_PASS_SKIP.load(Relaxed) {
        return Ok(false);
    }
    let versions = manifest::list_versions(store, id).await?;
    let Some(&newest) = versions.last() else { return Ok(false) };
    Ok(match load(store, id).await? {
        // a database never migrated: its versions are step 3's offline publishes (today's pass runs beside them, no
        // absorption) unless the server runs compactors (step 4)
        None => super::compactor::SERVER_ON.load(Relaxed),
        Some(s) => s.writer_uses(newest),
    })
}
