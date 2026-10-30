//! The object-store side the engine provides (until P6 wires the real engine): the database lease and a stand-in for
//! the database itself, both on any `object_store` with conditional writes (MinIO locally; the same code would talk to
//! S3). Layout in the bucket:
//!
//! - `lease/<db>`: `{"holder", "seq", "members", "cfg"}`. Written only with conditional PUT (create if absent, update
//!   if the ETag still matches), like round3's `lease.rs`. No time is written: another server takes over only after
//!   seeing the same content for lease + margin on its own monotonic clock (design §5.1). `seq` grows on every write; the
//!   writer's **epoch** is the `seq` of the PUT that made it the holder (design §5.1).
//! - `db/<db>/manifest`: `{"writer_epoch", "applied", "chunks"}`: the stand-in for SlateDB's manifest. Opening as the
//!   writer CASes `writer_epoch` up to the new epoch (the object-store writer fence: SlateDB fences an older writer the
//!   same way); an upload writes a chunk object, then CASes the manifest only if `writer_epoch` is still its own.
//!   `applied` is the journal marker (`sys/journal` in the real engine): the index of the last record contained.
//! - `db/<db>/chunk-<upto>-<epoch>`: the records of one upload, `[[index, write id], ...]` (barriers are not data).

use journal3z_core::lease::{holder_valid_until, LeaseView, Observer, TakeoverRule};
use journal3z_core::{Epoch, Idx, NodeId};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Lease {
    pub holder: String,
    pub seq: u64,
    pub members: Vec<NodeId>,
    pub cfg: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub writer_epoch: Epoch,
    pub applied: Idx,
    pub chunks: Vec<String>,
}

#[derive(Debug)]
pub enum MetaError {
    /// Someone else changed the object (lost CAS, newer writer).
    Conflict,
    /// Store unreachable or another I/O error: retry later.
    Unavailable(String),
}

impl std::fmt::Display for MetaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// A versioned object: its value and the ETag to CAS against (None = absent).
pub struct Versioned<T> {
    pub value: Option<T>,
    pub etag: Option<String>,
}

#[derive(Clone)]
pub struct Meta {
    pub store: Arc<dyn ObjectStore>,
    /// Negative control (builds with `mutations` only): every conditional write becomes an unconditional overwrite.
    #[cfg(feature = "mutations")]
    pub unconditional: bool,
    /// Negative control (builds with `mutations` only): upload objects named by (index, epoch) only, overwriting.
    #[cfg(feature = "mutations")]
    pub fixed_chunk_names: bool,
}

fn unavailable(e: object_store::Error) -> MetaError {
    match e {
        object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. } => MetaError::Conflict,
        e => MetaError::Unavailable(e.to_string()),
    }
}

pub fn lease_path(db: u64) -> Path {
    Path::from(format!("lease/{db}"))
}
pub fn manifest_path(db: u64) -> Path {
    Path::from(format!("db/{db}/manifest"))
}

impl Meta {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Meta {
            store,
            #[cfg(feature = "mutations")]
            unconditional: false,
            #[cfg(feature = "mutations")]
            fixed_chunk_names: false,
        }
    }

    pub async fn get<T: for<'a> Deserialize<'a>>(&self, p: &Path) -> Result<Versioned<T>, MetaError> {
        match self.store.get(p).await {
            Ok(g) => {
                let etag = g.meta.e_tag.clone();
                let b = g.bytes().await.map_err(unavailable)?;
                let v = serde_json::from_slice(&b).map_err(|e| MetaError::Unavailable(format!("bad json at {p}: {e}")))?;
                Ok(Versioned { value: Some(v), etag })
            }
            Err(object_store::Error::NotFound { .. }) => Ok(Versioned { value: None, etag: None }),
            Err(e) => Err(unavailable(e)),
        }
    }

    /// Conditional write: create if `etag` is None, else update only if the object still has that ETag.
    pub async fn cas<T: Serialize>(&self, p: &Path, etag: Option<String>, v: &T) -> Result<Option<String>, MetaError> {
        let mode = match etag {
            None => PutMode::Create,
            Some(e) => PutMode::Update(UpdateVersion { e_tag: Some(e), version: None }),
        };
        #[cfg(feature = "mutations")]
        let mode = if self.unconditional { PutMode::Overwrite } else { mode };
        let body = serde_json::to_vec(v).unwrap();
        let r = self.store.put_opts(p, PutPayload::from(body), PutOptions { mode, ..Default::default() }).await.map_err(unavailable)?;
        Ok(r.e_tag)
    }

    /// Write one upload's records as a new object. The name is unique (random suffix) and the write is "create
    /// only": an upload can never replace an earlier one. (Found by the bench, trial 3: two uploads with the same
    /// index and epoch had the same name; the second, empty one replaced the first after the journal was trimmed.)
    pub async fn put_chunk(&self, db: u64, upto: Idx, epoch: Epoch, recs: &[(Idx, u64)]) -> Result<String, MetaError> {
        #[cfg(feature = "mutations")]
        if self.fixed_chunk_names {
            // negative control: the old naming, overwriting
            let name = format!("db/{db}/chunk-{upto:016}-{epoch:010}");
            self.store.put(&Path::from(name.clone()), PutPayload::from(serde_json::to_vec(recs).unwrap())).await.map_err(unavailable)?;
            return Ok(name);
        }
        let name = format!("db/{db}/chunk-{upto:016}-{epoch:010}-{:016x}", rand::random::<u64>());
        let body = serde_json::to_vec(recs).unwrap();
        let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
        self.store.put_opts(&Path::from(name.clone()), PutPayload::from(body), opts).await.map_err(unavailable)?;
        Ok(name)
    }

    pub async fn get_chunk(&self, name: &str) -> Result<Vec<(Idx, u64)>, MetaError> {
        let b = self.store.get(&Path::from(name)).await.map_err(unavailable)?.bytes().await.map_err(unavailable)?;
        serde_json::from_slice(&b).map_err(|e| MetaError::Unavailable(e.to_string()))
    }

    /// The database's content as the store holds it: index -> write id, up to the manifest's `applied`, applying the
    /// manifest's chunks in order (a later chunk overwrites an index).
    pub async fn content(&self, db: u64) -> Result<(Manifest, std::collections::BTreeMap<Idx, u64>), MetaError> {
        let m: Manifest = self.get(&manifest_path(db)).await?.value.unwrap_or_default();
        let mut out = std::collections::BTreeMap::new();
        for c in &m.chunks {
            for (i, id) in self.get_chunk(c).await? {
                out.insert(i, id);
            }
        }
        out.retain(|i, _| *i <= m.applied);
        Ok((m, out))
    }
}

/// Lease operations (design §5.1). Times are this server's **monotonic** clock (ms): the holder may act until
/// `sent + ttl` where `sent` is when it sent its last successful acquire / renew write; another server takes over only
/// after seeing the same lease content for `ttl + margin` on its own monotonic clock (`journal3z_core::lease`).
pub struct LeaseOps<'a> {
    pub meta: &'a Meta,
    pub holder: &'a str,
    pub rule: TakeoverRule,
}

#[derive(Debug, Clone)]
pub struct Held {
    pub epoch: Epoch,
    pub seq: u64,
    pub etag: Option<String>,
    pub members: Vec<NodeId>,
    pub cfg: u64,
    /// Monotonic time (this server's clock) until which this server may act as the holder.
    pub valid_until_mono: u64,
}

fn key<T: std::hash::Hash>(x: &T) -> u64 {
    use std::hash::Hasher;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    x.hash(&mut h);
    h.finish()
}

impl LeaseOps<'_> {
    pub async fn read(&self, db: u64) -> Result<Versioned<Lease>, MetaError> {
        self.meta.get::<Lease>(&lease_path(db)).await
    }

    /// What the observer compares: the whole content (and its version tag), the holder alone, free or not.
    pub fn view(cur: &Versioned<Lease>) -> LeaseView {
        match &cur.value {
            None => LeaseView { free: true, content: 0, holder: 0 },
            Some(l) => LeaseView { free: l.holder.is_empty(), content: key(&(l, &cur.etag)), holder: key(&l.holder) },
        }
    }

    /// Is this lease ours (this process)?
    pub fn mine(&self, cur: &Versioned<Lease>) -> bool {
        cur.value.as_ref().map_or(false, |l| l.holder == self.holder)
    }

    /// Design §5.1: may this server try to take the lease it just read (`cur`)? Yes if it is free, ours, or the
    /// observer has seen it unchanged for lease + margin on our monotonic clock (`mono`; `wall` only for the
    /// `lease_wall_clock` negative control).
    pub fn may_take(&self, cur: &Versioned<Lease>, observer: &mut Observer, mono: u64, wall: u64) -> bool {
        self.mine(cur) || observer.observe(&self.rule, &Self::view(cur), mono, wall)
    }

    /// Take the lease, conditionally on the exact version `cur` that was read (the caller decided it may: free, ours,
    /// or unchanged for ttl + margin). `sent_mono`: our monotonic clock just before this call.
    pub async fn take(&self, db: u64, cur: &Versioned<Lease>, default_members: &[NodeId], sent_mono: u64) -> Result<Held, MetaError> {
        let l = cur.value.clone().unwrap_or(Lease { members: default_members.to_vec(), ..Default::default() });
        let n = Lease { holder: self.holder.to_string(), seq: l.seq + 1, members: l.members.clone(), cfg: l.cfg };
        let etag = self.meta.cas(&lease_path(db), cur.etag.clone(), &n).await?;
        Ok(Held { epoch: n.seq, seq: n.seq, etag, members: n.members, cfg: n.cfg, valid_until_mono: holder_valid_until(&self.rule, sent_mono) })
    }

    /// Renew: only if we still hold it (our ETag). The epoch does not change.
    pub async fn renew(&self, db: u64, h: &mut Held, sent_mono: u64) -> Result<(), MetaError> {
        let n = Lease { holder: self.holder.to_string(), seq: h.seq + 1, members: h.members.clone(), cfg: h.cfg };
        let etag = self.meta.cas(&lease_path(db), h.etag.clone(), &n).await?;
        h.etag = etag;
        h.seq = n.seq;
        h.valid_until_mono = h.valid_until_mono.max(holder_valid_until(&self.rule, sent_mono));
        Ok(())
    }

    pub async fn release(&self, db: u64, h: &Held) -> Result<(), MetaError> {
        let n = Lease { holder: String::new(), seq: h.seq + 1, members: h.members.clone(), cfg: h.cfg };
        self.meta.cas(&lease_path(db), h.etag.clone(), &n).await.map(|_| ())
    }

    /// Membership change (design §5.8): new members, new epoch (= the new seq), still the holder.
    pub async fn reconfigure(&self, db: u64, h: &mut Held, members: Vec<NodeId>, sent_mono: u64) -> Result<(), MetaError> {
        let n = Lease { holder: self.holder.to_string(), seq: h.seq + 1, members: members.clone(), cfg: h.cfg + 1 };
        h.etag = self.meta.cas(&lease_path(db), h.etag.clone(), &n).await?;
        h.seq = n.seq;
        h.epoch = n.seq;
        h.members = members;
        h.cfg = n.cfg;
        h.valid_until_mono = h.valid_until_mono.max(holder_valid_until(&self.rule, sent_mono));
        Ok(())
    }
}

/// Store operations of the engine stand-in.
pub struct StoreOps<'a> {
    pub meta: &'a Meta,
    /// Negative control `skip_store_fence`: no writer fence in the store at all (last writer wins).
    pub skip_fence: bool,
}

impl StoreOps<'_> {
    /// Become the store's writer at `epoch` (fences every older writer's later uploads) and read the marker.
    /// Err(Conflict) if a newer writer already opened.
    pub async fn open(&self, db: u64, epoch: Epoch) -> Result<Idx, MetaError> {
        loop {
            let cur = self.meta.get::<Manifest>(&manifest_path(db)).await?;
            let m = cur.value.clone().unwrap_or_default();
            if m.writer_epoch > epoch {
                return Err(MetaError::Conflict);
            }
            if self.skip_fence || (m.writer_epoch == epoch && cur.etag.is_some()) {
                return Ok(m.applied);
            }
            let n = Manifest { writer_epoch: epoch, ..m.clone() };
            match self.meta.cas(&manifest_path(db), cur.etag, &n).await {
                Ok(_) => return Ok(n.applied),
                Err(MetaError::Conflict) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Upload `recs` (index, write id) as the state up to `upto`. Err(Conflict) if a newer writer opened the store.
    pub async fn upload(&self, db: u64, epoch: Epoch, upto: Idx, recs: &[(Idx, u64)]) -> Result<(), MetaError> {
        let chunk = self.meta.put_chunk(db, upto, epoch, recs).await?;
        loop {
            let cur = self.meta.get::<Manifest>(&manifest_path(db)).await?;
            let m = cur.value.clone().unwrap_or_default();
            if !self.skip_fence && m.writer_epoch != epoch {
                return Err(MetaError::Conflict);
            }
            let mut n = m.clone();
            n.chunks.push(chunk.clone());
            n.applied = if self.skip_fence { upto } else { m.applied.max(upto) };
            match self.meta.cas(&manifest_path(db), cur.etag, &n).await {
                Ok(_) => return Ok(()),
                Err(MetaError::Conflict) => continue,
                Err(e) => return Err(e),
            }
        }
    }
}
