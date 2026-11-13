//! P-B3 (2026-11-13, prototype): compaction off the serving process.
//!
//! A database's compactor (absorption, merges, reseals, erasure scrubs' jobs, GC, SlateDB's fold) can run in another
//! process (`mongo-server --compactor-only <db>`, meant for the warm spare or another machine) instead of inside the
//! waiter. The protocol, in short (the full text and its cases are in fenvix-brain `sessions/2026-11-13-b3-compaction`):
//!
//! - **Who compacts.** One process at a time holds the database's compactor claim, the object `{fx root}/compactor`
//!   (`ClaimDoc`), taken with a conditional create, renewed and taken over with conditional overwrites of exactly the
//!   object it read (`PutMode::Update` with its entity tag). A claim left by a process that stopped renewing is taken
//!   over once this process has seen the same object unchanged for the lease plus the margin, on its own clock (no clock
//!   is compared with another). The holder acts (publishes a version, deletes an object) only while its own deadline
//!   holds: the send time of its last successful renewal plus the lease minus the margin (`Claim::valid`). The margin is
//!   longer than the manifest PUT timeout (F6), so a publish sent before the deadline is decided before a new holder can
//!   start.
//! - **The waiter defers** (`--sealed-remote-compactor`): it starts no compactor, publishes nothing after the database's
//!   first version (and the catch-up publish a compactor start needs), runs no compaction pass on the database, and
//!   follows our manifest as a reader does: its state is renewed at half T_pin and no read step uses a state older than
//!   T_pin (`Tenant::fx_follow_remote`); its holds still end within the hold life. So for the floor and for GC the waiter
//!   is one more reader.
//! - **The remote compactor** is the same `Compactor`, started on the store alone (`Compactor::start_remote`): the
//!   L0 SSTs come from SlateDB's manifest read from the store (`slatedb::admin`), not from a writer's memory; the
//!   writer epoch it stamps is the one of SlateDB's manifest; it has no holds and serves no version, so its drop floor
//!   uses the time-only remote lease of rule 10 (the X_d of a version first seen at least the remote lease ago, longer
//!   than T_pin plus the hold life plus the PUT timeout), and its GC keeps what rule 23's grace keeps for every reader.
//! - **Fences that do not change**: the slot fence (a version is created at base + 1 with a conditional create, and
//!   counts only on the main line); the writer epoch (a version of a newer writer stops the compactor, which the remote
//!   supervisor then restarts on the newest version; an older writer's late version is never a base); trusted versions
//!   for every reader; SlateDB's own compactor epoch for the fold (a new holder's SlateDB compactor fences the old one's).
use anyhow::{anyhow, bail, Result};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// `--sealed-remote-compactor`: the waiters of this process never compact; a remote compactor does (see the module text).
pub static WAITER_DEFERS: AtomicBool = AtomicBool::new(false);
/// Remote publishes and deletes refused because the claim's deadline had passed (serverStatus, tests).
pub static PAST_DEADLINE: AtomicU64 = AtomicU64::new(0);
/// Claims taken over from a holder that stopped renewing.
pub static TAKEOVERS: AtomicU64 = AtomicU64::new(0);
/// Writers that handed their compaction to a remote compactor, and versions they installed by following the store.
pub static WAITERS_FOLLOWING: AtomicU64 = AtomicU64::new(0);
pub static FOLLOW_INSTALLS: AtomicU64 = AtomicU64::new(0);
/// Negative control (tests only): a writer under a remote compactor reads with a state of any age (no T_pin).
pub static CONTROL_WRITER_NO_T_PIN: AtomicBool = AtomicBool::new(false);
/// Negative control (tests only): a holder acts whatever its deadline says.
pub static CONTROL_NO_DEADLINE: AtomicBool = AtomicBool::new(false);

pub fn claim_path(id: &(impl crate::lifecycle::DbRef + ?Sized)) -> Path {
    Path::from(format!("{}/compactor", super::manifest::root(id)))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClaimDoc {
    pub holder: String,
    /// Goes up by one at every takeover (diagnostics: who held it when).
    pub claim: u64,
    pub lease_ms: u64,
    /// The holder's wall clock at its last renewal (diagnostics only: no decision reads it).
    pub renewed_ms: u64,
    /// Given back by its holder: anyone may take it at once.
    pub released: bool,
    /// A count of renewals, so every renewal changes the object (its entity tag) even within one millisecond.
    pub n: u64,
}

struct St {
    doc: ClaimDoc,
    version: UpdateVersion,
    /// The deadline: the send time of the last successful write of the claim plus the lease minus the margin.
    until: Instant,
    lost: bool,
}

/// A compactor claim held by this process.
pub struct Claim {
    store: Arc<dyn ObjectStore>,
    path: Path,
    pub lease: Duration,
    pub margin: Duration,
    st: Mutex<St>,
    /// One write of the claim at a time.
    writing: tokio::sync::Mutex<()>,
}

async fn read(store: &Arc<dyn ObjectStore>, path: &Path) -> Result<Option<(ClaimDoc, UpdateVersion)>> {
    match store.get(path).await {
        Ok(g) => {
            let version = UpdateVersion { e_tag: g.meta.e_tag.clone(), version: g.meta.version.clone() };
            let b = g.bytes().await?;
            Ok(Some((serde_json::from_slice(&b)?, version)))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Whether a write of the claim lost to another writer (the object changed or appeared since we read it).
fn lost_race(e: &object_store::Error) -> bool {
    matches!(e, object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. } | object_store::Error::NotFound { .. })
}

impl Claim {
    /// Take the claim of database `id` for `holder`: at once when there is none or it was released; otherwise once the
    /// same object stayed unchanged for the lease plus the margin since this process first saw it. Gives up after `wait`.
    pub async fn acquire(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized), holder: &str, lease: Duration, margin: Duration, wait: Duration) -> Result<Arc<Claim>> {
        anyhow::ensure!(margin < lease, "claim margin {margin:?} must be shorter than the lease {lease:?}");
        let path = claim_path(id);
        let t0 = Instant::now();
        // (entity tag, version, first sight) of the claim object as last read
        let mut seen: Option<(UpdateVersion, Instant)> = None;
        loop {
            anyhow::ensure!(t0.elapsed() < wait, "compactor claim of {}: still held by another process after {wait:?}", path);
            let sent = Instant::now();
            let cur = read(store, &path).await?;
            let (mode, next_claim) = match &cur {
                None => (PutMode::Create, 1),
                Some((d, v)) if d.released => (PutMode::Update(v.clone()), d.claim + 1),
                Some((d, v)) => {
                    let first = match &seen {
                        Some((sv, at)) if *sv == *v => *at,
                        _ => {
                            seen = Some((v.clone(), sent));
                            sent
                        }
                    };
                    if first.elapsed() < lease + margin {
                        tokio::time::sleep((lease / 8).max(Duration::from_millis(5))).await;
                        continue;
                    }
                    TAKEOVERS.fetch_add(1, Relaxed);
                    eprintln!("COMPACTOR_CLAIM_TAKEOVER path={path} from={} claim={} (unchanged for {:?})", d.holder, d.claim, first.elapsed());
                    (PutMode::Update(v.clone()), d.claim + 1)
                }
            };
            let doc = ClaimDoc { holder: holder.to_string(), claim: next_claim, lease_ms: lease.as_millis() as u64, renewed_ms: super::manifest::now_ms(), released: false, n: 0 };
            let opts = PutOptions { mode, ..Default::default() };
            match store.put_opts(&path, PutPayload::from(serde_json::to_vec(&doc)?), opts).await {
                Ok(r) => {
                    let version = UpdateVersion { e_tag: r.e_tag, version: r.version };
                    return Ok(Arc::new(Claim { store: Arc::clone(store), path, lease, margin, st: Mutex::new(St { doc, version, until: sent + lease - margin, lost: false }), writing: tokio::sync::Mutex::new(()) }));
                }
                Err(e) if lost_race(&e) => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Whether this process may still act as the database's compactor (publish, delete): before its deadline and not lost.
    pub fn valid(&self) -> bool {
        if CONTROL_NO_DEADLINE.load(Relaxed) {
            return true;
        }
        let st = self.st.lock().unwrap();
        !st.lost && Instant::now() < st.until
    }

    pub fn lost(&self) -> bool {
        self.st.lock().unwrap().lost
    }

    pub fn doc(&self) -> ClaimDoc {
        self.st.lock().unwrap().doc.clone()
    }

    /// Renew: a conditional overwrite of exactly the object we wrote last. Err: the claim is lost (someone took it over)
    /// or the renewal's outcome is unknown; either way `valid` stays false once the old deadline passes.
    pub async fn renew(&self) -> Result<()> {
        let _w = self.writing.lock().await;
        let (mut doc, version) = {
            let st = self.st.lock().unwrap();
            if st.lost {
                bail!("compactor claim {} lost", self.path);
            }
            (st.doc.clone(), st.version.clone())
        };
        doc.n += 1;
        doc.renewed_ms = super::manifest::now_ms();
        let sent = Instant::now();
        let opts = PutOptions { mode: PutMode::Update(version), ..Default::default() };
        match self.store.put_opts(&self.path, PutPayload::from(serde_json::to_vec(&doc)?), opts).await {
            Ok(r) => {
                let mut st = self.st.lock().unwrap();
                st.doc = doc;
                st.version = UpdateVersion { e_tag: r.e_tag, version: r.version };
                st.until = sent + self.lease - self.margin;
                Ok(())
            }
            Err(e) if lost_race(&e) => {
                self.st.lock().unwrap().lost = true;
                Err(anyhow!("compactor claim {} taken over by another process", self.path))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Give the claim back (best effort): the next taker need not wait for the lease.
    pub async fn release(&self) {
        let _w = self.writing.lock().await;
        let (mut doc, version) = {
            let mut st = self.st.lock().unwrap();
            if st.lost {
                return;
            }
            st.lost = true;
            (st.doc.clone(), st.version.clone())
        };
        doc.released = true;
        doc.n += 1;
        let opts = PutOptions { mode: PutMode::Update(version), ..Default::default() };
        if let Ok(b) = serde_json::to_vec(&doc) {
            let _ = self.store.put_opts(&self.path, PutPayload::from(b), opts).await;
        }
    }
}

/// The holder of database `id`'s compactor claim as the store has it now (None: no claim or a released one).
pub async fn holder(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized)) -> Result<Option<ClaimDoc>> {
    Ok(read(store, &claim_path(id)).await?.map(|(d, _)| d).filter(|d| !d.released))
}

/// SlateDB's state as a process without the writer reads it: the newest manifest in the store, read again at most every
/// `every` (the remote compactor's view of L0; the writer's own view is in its memory).
pub struct RemoteSlate {
    admin: slatedb::admin::Admin,
    m: Mutex<slatedb::VersionedManifest>,
    last: Mutex<Instant>,
    every: Duration,
    refreshing: AtomicBool,
    pub reads: AtomicU64,
}

impl RemoteSlate {
    pub async fn open(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized), every: Duration) -> Result<RemoteSlate> {
        let admin = slatedb::admin::Admin::builder(crate::lifecycle::tenant_path(id), Arc::clone(store)).build();
        let m = admin.read_manifest(None).await?.ok_or_else(|| anyhow!("database {}: SlateDB has no manifest", id.name()))?;
        Ok(RemoteSlate { admin, m: Mutex::new(m), last: Mutex::new(Instant::now()), every, refreshing: AtomicBool::new(false), reads: AtomicU64::new(1) })
    }

    pub fn manifest(&self) -> slatedb::VersionedManifest {
        self.m.lock().unwrap().clone()
    }

    /// Read the newest manifest now (manifests only go forward: an older one read late is not taken).
    pub async fn refresh(&self) -> Result<()> {
        if self.refreshing.swap(true, Relaxed) {
            return Ok(());
        }
        let r = self.admin.read_manifest(None).await;
        self.refreshing.store(false, Relaxed);
        self.reads.fetch_add(1, Relaxed);
        *self.last.lock().unwrap() = Instant::now();
        if let Some(m) = r? {
            let mut cur = self.m.lock().unwrap();
            if m.id() > cur.id() {
                *cur = m;
            }
        }
        Ok(())
    }

    pub fn due(&self) -> bool {
        self.last.lock().unwrap().elapsed() >= self.every
    }
}
