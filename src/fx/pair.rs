//! Checkpoints paired with a version of our manifest, and the reader of a paired checkpoint (compaction 2, step 7a;
//! RULES-FOR-THE-CODE rules 1, 4, 5, 11, 18, 19, 20).
//!
//! Once step 4's drop removes SlateDB rows at or below X_d, a SlateDB checkpoint alone no longer holds the database: the
//! rows it lacks are in our runs. So every checkpoint the control layer takes (export, restore points) records the
//! version it pairs with, and every reader the control layer opens (an export's pages, PITR's diff on both sides) reads
//! through the merged rule with that version (`read.rs`).
//!
//! Pairing (rule 18), with the store as the only shared state, so it also holds for a checkpoint taken outside the
//! process that runs the drops:
//! 1. take the drop lease, `{db}/fx/drop-lease`, a conditional create with a life ([`LEASE_LIFE_MS`]); a drop job must
//!    not start while it is held (rule 11; `droptest.rs` checks it, step 4's job must);
//! 2. create SlateDB's checkpoint;
//! 3. check that the lease is still held, with a margin for clock skew ([`SKEW_MARGIN_MS`]); if it expired, delete the
//!    checkpoint and start again [CkNoRecheck];
//! 4. pick the newest version with X_d <= the checkpoint's `last_l0_seq`, and write the pair, `{db}/fx/pairs/{checkpoint}`;
//! 5. release the lease.
//!
//! While the lease is held no drop runs, so the version picked in step 4 is the newest one whose rows the checkpoint's
//! SSTs may lack. With step 4's absorption (rule 16) every version's X_d is the highest seq of an L0 SST, so the newest
//! version always qualifies. The offline builder of step 3 sets X_d to a WAL seq, so the newest version can be above
//! `last_l0_seq`; then an older one is picked, or none, and the pair says it is not exact. That is right only because
//! nothing drops rows while that builder is in use.
//!
//! GC (step 4) must keep every version and run while the lease is held (rule 19: the pair is visible from the start of
//! the checkpoint), and every version and run a pair lists while its checkpoint lives (rules 19, 23).
//!
//! All of it runs only with `--sealed-runs` on: with it off, checkpoints and readers are the ones of the releases
//! before, with no extra request.

use super::hold::{for_db, Holds};
use super::manifest::{self, now_ms, root};
use super::view::View;
use anyhow::{anyhow, bail, ensure, Context, Result};
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

/// Life of the drop lease a checkpoint takes (rule 18: a lease that never expired would block every drop forever if the
/// control layer died).
pub static LEASE_LIFE_MS: AtomicU64 = AtomicU64::new(60_000);
/// Margin for clock skew between the control layer and the drop jobs (rule 18).
pub static SKEW_MARGIN_MS: AtomicU64 = AtomicU64::new(5_000);

/// Negative control of the tests (`ctl-tests/test_47_c2_step7a_paths.py`): `FENVIX_CONTROL_RAW_PATHS=1` makes the
/// control layer's readers read SlateDB alone, as before step 7a.
pub fn control_raw() -> bool {
    static C: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *C.get_or_init(|| std::env::var("FENVIX_CONTROL_RAW_PATHS").map(|v| v == "1").unwrap_or(false))
}

fn enabled() -> bool {
    super::ENABLED.load(Relaxed) && !control_raw()
}

pub fn lease_path(id: &str) -> Path {
    Path::from(format!("{}/drop-lease", root(id)))
}

pub fn pair_path(id: &str, checkpoint: &uuid::Uuid) -> Path {
    Path::from(format!("{}/pairs/{checkpoint}", root(id)))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Lease {
    holder: String,
    nonce: u64,
    taken_ms: u64,
    expires_ms: u64,
}

/// The pair of a checkpoint.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Pair {
    pub checkpoint: String,
    /// The version read through (None: the database had no version at or below the checkpoint's `last_l0_seq`).
    pub version: Option<u64>,
    pub x_d: Option<u64>,
    pub e_env: Option<u64>,
    pub last_l0_seq: u64,
    /// False when a newer version than the paired one existed (the offline builder of step 3, see the module text).
    pub exact: bool,
    pub created_ms: u64,
}

/// Whether a checkpoint holds the drop lease of database `id` now (a drop job must wait, rule 11). An expired lease is
/// held until its expiry plus the skew margin has passed, by this clock.
pub async fn lease_held(store: &Arc<dyn ObjectStore>, id: &str) -> Result<bool> {
    match store.get(&lease_path(id)).await {
        Ok(g) => {
            let l: Lease = serde_json::from_slice(&g.bytes().await?)?;
            Ok(now_ms() < l.expires_ms + SKEW_MARGIN_MS.load(Relaxed))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Take the drop lease: a conditional create. A lease left by a process that died is taken over once it is past its
/// expiry plus the skew margin. Waits up to 30 s for a lease held by someone else.
async fn take_lease(store: &Arc<dyn ObjectStore>, id: &str) -> Result<Lease> {
    let path = lease_path(id);
    for _ in 0..300 {
        let now = now_ms();
        let l = Lease { holder: format!("pid-{}", std::process::id()), nonce: rand::random(), taken_ms: now, expires_ms: now + LEASE_LIFE_MS.load(Relaxed) };
        let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
        match store.put_opts(&path, PutPayload::from(serde_json::to_vec(&l)?), opts).await {
            Ok(_) => return Ok(l),
            Err(object_store::Error::AlreadyExists { .. }) | Err(object_store::Error::Precondition { .. }) => {
                if !lease_held(store, id).await? {
                    // a dead holder's lease: remove it and race for the slot again
                    match store.delete(&path).await {
                        Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                        Err(e) => return Err(e.into()),
                    }
                    continue;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
    bail!("database {id}: the drop lease stayed taken for 30 s")
}

/// Release the lease if it is still ours.
async fn release_lease(store: &Arc<dyn ObjectStore>, id: &str, mine: &Lease) {
    let path = lease_path(id);
    if let Ok(g) = store.get(&path).await {
        if let Ok(b) = g.bytes().await {
            if serde_json::from_slice::<Lease>(&b).map(|l| l.nonce == mine.nonce).unwrap_or(false) {
                let _ = store.delete(&path).await;
            }
        }
    }
}

/// Take a SlateDB checkpoint of database `id` with `make` and pair it with a version of our manifest (rule 18, the steps
/// in the module text). Returns the checkpoint and its pair (None with `--sealed-runs` off: no pairing, no request).
pub async fn checkpoint_paired<F, Fut>(store: &Arc<dyn ObjectStore>, id: &str, make: F) -> Result<(slatedb::CheckpointCreateResult, Option<Pair>)>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<slatedb::CheckpointCreateResult>>,
{
    if !enabled() {
        return Ok((make().await?, None));
    }
    let admin = slatedb::admin::Admin::builder(crate::lifecycle::tenant_path(id), Arc::clone(store)).build();
    for _ in 0..3 {
        // a fresh lease for every attempt: a retry after an expired lease must not run unleased
        let lease = take_lease(store, id).await?;
        let r = paired_once(store, id, &admin, &lease, &make).await;
        release_lease(store, id, &lease).await;
        if let Some(x) = r? {
            return Ok(x);
        }
    }
    bail!("database {id}: the drop lease expired three times while a checkpoint was taken")
}

/// One attempt under `lease`: None when the lease expired before the checkpoint existed (the checkpoint is deleted).
async fn paired_once<F, Fut>(store: &Arc<dyn ObjectStore>, id: &str, admin: &slatedb::admin::Admin, lease: &Lease, make: &F) -> Result<Option<(slatedb::CheckpointCreateResult, Option<Pair>)>>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<slatedb::CheckpointCreateResult>>,
{
    let cp = make().await?;
    let m = admin.read_manifest(Some(cp.manifest_id)).await?.ok_or_else(|| anyhow!("database {id}: the manifest of checkpoint {} is gone", cp.id))?;
    let last_l0 = m.last_l0_seq();
    // [CkNoRecheck]: the lease must still be ours, with the margin, now that the checkpoint exists
    if now_ms() + SKEW_MARGIN_MS.load(Relaxed) >= lease.expires_ms {
        admin.delete_checkpoint(cp.id).await?;
        return Ok(None);
    }
    let versions = manifest::list_versions(store, id).await?;
    let newest = versions.last().copied();
    let mut pick = None;
    for &n in versions.iter().rev() {
        let v = manifest::load(store, id, n).await?;
        if v.x_d <= last_l0 {
            pick = Some(v);
            break;
        }
    }
    let pair = Pair {
        checkpoint: cp.id.to_string(),
        version: pick.as_ref().map(|v| v.version),
        x_d: pick.as_ref().map(|v| v.x_d),
        e_env: pick.as_ref().and_then(|v| v.e_env),
        last_l0_seq: last_l0,
        exact: pick.as_ref().map(|v| v.version) == newest,
        created_ms: now_ms(),
    };
    store.put(&pair_path(id, &cp.id), PutPayload::from(serde_json::to_vec(&pair)?)).await?;
    Ok(Some((cp, Some(pair))))
}

/// The pair of a checkpoint (None: none recorded).
pub async fn load_pair(store: &Arc<dyn ObjectStore>, id: &str, checkpoint: &uuid::Uuid) -> Result<Option<Pair>> {
    match store.get(&pair_path(id, checkpoint)).await {
        Ok(g) => Ok(Some(serde_json::from_slice(&g.bytes().await?).with_context(|| format!("pair of checkpoint {checkpoint} of {id}"))?)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Forget the pair of a checkpoint that was deleted.
pub async fn delete_pair(store: &Arc<dyn ObjectStore>, id: &str, checkpoint: &uuid::Uuid) -> Result<()> {
    match store.delete(&pair_path(id, checkpoint)).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Which version a reader of the latest state reads through.
pub enum Follow {
    /// The newest version in the store (the database is quiesced: closed, its lease held, no compaction or drop).
    Newest,
    /// The version paired with this checkpoint of the database (PITR's staging clone of a restore point, rule 20).
    PairOf(uuid::Uuid),
}

/// A control-layer reader (export, PITR's diff): SlateDB's reader and the version paired with it, read through the
/// merged rule (rule 1: the same pair for every page). Every page takes a hold on the version in the database's holds
/// and checks it before its rows are returned (rules 4 and 5).
pub struct PairedReader {
    reader: slatedb::DbReader,
    view: Option<Arc<View>>,
    env: Option<u64>,
    holds: Arc<Holds>,
}

impl PairedReader {
    /// A checkpoint of database `id` (export).
    pub async fn open_checkpoint(store: &Arc<dyn ObjectStore>, id: &str, checkpoint: uuid::Uuid, opts: slatedb::config::DbReaderOptions) -> Result<PairedReader> {
        let reader = super::read::open_checkpoint_reader(crate::lifecycle::tenant_path(id), Arc::clone(store), checkpoint, opts).await?;
        Self::pair(store, id, reader, Some(checkpoint)).await
    }

    /// The latest state of a SlateDB database at `path` (PITR: the live database, or a staging clone), read through the
    /// version `follow` names of database `id`.
    pub async fn open_follow(store: &Arc<dyn ObjectStore>, id: &str, path: String, follow: Follow, opts: slatedb::config::DbReaderOptions) -> Result<PairedReader> {
        let reader = super::read::open_follow_reader(path, Arc::clone(store), opts).await?;
        match follow {
            Follow::Newest => Self::pair(store, id, reader, None).await,
            Follow::PairOf(cp) => Self::pair(store, id, reader, Some(cp)).await,
        }
    }

    async fn pair(store: &Arc<dyn ObjectStore>, id: &str, reader: slatedb::DbReader, checkpoint: Option<uuid::Uuid>) -> Result<PairedReader> {
        let holds = for_db(id);
        let raw = |reader: slatedb::DbReader, holds: Arc<Holds>| async move {
            let env = reader.get_key_value(crate::envelope::MARKER).await?.map(|kv| kv.seq);
            Ok::<_, anyhow::Error>(PairedReader { reader, view: None, env, holds })
        };
        if !enabled() {
            return raw(reader, holds).await;
        }
        let version = match checkpoint {
            Some(cp) => match load_pair(store, id, &cp).await? {
                Some(p) => p.version,
                None => {
                    // a checkpoint taken without pairing (before step 7a, or with --sealed-runs off): right only while no
                    // version exists, since then nothing can have dropped a row
                    ensure!(manifest::list_versions(store, id).await?.is_empty(), "database {id}: checkpoint {cp} has no pair with our manifest, and the database has sealed runs; its SlateDB rows alone may be incomplete");
                    None
                }
            },
            None => manifest::list_versions(store, id).await?.last().copied(),
        };
        let Some(n) = version else { return raw(reader, holds).await };
        let v = manifest::load(store, id, n).await?;
        // rule 20: a staging clone reads with E_env = min(the parent's E_env, the checkpoint's last_l0_seq); the paired
        // version's E_env is at or below its X_d, which is at or below last_l0_seq
        let env = v.e_env;
        let view = Arc::new(View::new(Arc::clone(store), v, None)?);
        Ok(PairedReader { reader, view: Some(view), env, holds })
    }

    /// The version read through (None: SlateDB alone).
    pub fn version(&self) -> Option<u64> {
        self.view.as_ref().map(|v| v.version)
    }

    pub fn env(&self) -> Option<u64> {
        self.env
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let hold = self.view.as_ref().map(|v| self.holds.take(Arc::clone(v)));
        let r = super::read::get(&self.reader, &slatedb::config::ReadOptions::default(), self.view.as_deref(), self.env, key).await?;
        if let Some(h) = &hold {
            h.check()?;
        }
        Ok(r)
    }

    /// One page: at most `limit` live rows of [lo, hi).
    pub async fn scan(&self, lo: &[u8], hi: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize) -> Result<Vec<(Bytes, Bytes)>> {
        let hold = self.view.as_ref().map(|v| self.holds.take(Arc::clone(v)));
        let opts = super::read::scan_options(read_ahead, reverse, false);
        let r = super::read::scan(&self.reader, &opts, self.view.as_ref(), self.env, lo, hi, reverse, limit).await?;
        if let Some(h) = &hold {
            h.check()?;
        }
        Ok(r)
    }

    pub async fn close(self) -> Result<()> {
        Ok(self.reader.close().await?)
    }
}
