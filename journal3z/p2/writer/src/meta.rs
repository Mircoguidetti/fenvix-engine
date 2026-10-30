//! The object-store side the engine provides (until P6 wires the real engine): the database lease and a stand-in for
//! the database itself, both on any `object_store` with conditional writes (MinIO locally; the same code would talk to
//! S3). Layout in the bucket:
//!
//! - `lease/<db>`: `{"holder", "expires_ms", "seq", "members", "cfg"}`. Written only with conditional PUT (create if
//!   absent, update if the ETag still matches), like round3's `lease.rs`. `expires_ms` is in the holder's clock;
//!   another server may take over when its own clock is past `expires_ms + skew`. `seq` grows on every write; the
//!   writer's **epoch** is the `seq` of the PUT that made it the holder (design §5.1).
//! - `db/<db>/manifest`: `{"writer_epoch", "applied", "chunks"}`: the stand-in for SlateDB's manifest. Opening as the
//!   writer CASes `writer_epoch` up to the new epoch (the object-store writer fence: SlateDB fences an older writer the
//!   same way); an upload writes a chunk object, then CASes the manifest only if `writer_epoch` is still its own.
//!   `applied` is the journal marker (`sys/journal` in the real engine): the index of the last record contained.
//! - `db/<db>/chunk-<upto>-<epoch>`: the records of one upload, `[[index, write id], ...]` (barriers are not data).

use journal3z_core::{Epoch, Idx, NodeId};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Lease {
    pub holder: String,
    pub expires_ms: i64,
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

    pub async fn put_chunk(&self, db: u64, upto: Idx, epoch: Epoch, recs: &[(Idx, u64)]) -> Result<String, MetaError> {
        let name = format!("db/{db}/chunk-{upto:016}-{epoch:010}");
        let body = serde_json::to_vec(recs).unwrap();
        self.store.put(&Path::from(name.clone()), PutPayload::from(body)).await.map_err(unavailable)?;
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

/// Lease operations (design §5.1). `now_ms` is the caller's clock (possibly skewed: that is the point).
pub struct LeaseOps<'a> {
    pub meta: &'a Meta,
    pub holder: &'a str,
    pub ttl_ms: i64,
    pub skew_ms: i64,
}

#[derive(Debug, Clone)]
pub struct Held {
    pub epoch: Epoch,
    pub seq: u64,
    pub etag: Option<String>,
    pub members: Vec<NodeId>,
    pub cfg: u64,
    /// Local time (caller's clock) until which this server may act as the holder.
    pub valid_until_ms: i64,
}

impl LeaseOps<'_> {
    /// Take the lease if it is free, expired (by our clock, plus the skew allowance) or ours. `default_members` is
    /// used when the lease object does not exist yet.
    pub async fn acquire(&self, db: u64, now_ms: i64, default_members: &[NodeId]) -> Result<Held, MetaError> {
        let p = lease_path(db);
        let cur = self.meta.get::<Lease>(&p).await?;
        let l = cur.value.clone().unwrap_or(Lease { members: default_members.to_vec(), ..Default::default() });
        let free = l.holder.is_empty() || l.holder == self.holder || l.expires_ms + self.skew_ms < now_ms;
        if !free {
            return Err(MetaError::Conflict);
        }
        let n = Lease { holder: self.holder.to_string(), expires_ms: now_ms + self.ttl_ms, seq: l.seq + 1, members: l.members.clone(), cfg: l.cfg };
        let etag = self.meta.cas(&p, cur.etag, &n).await?;
        Ok(Held { epoch: n.seq, seq: n.seq, etag, members: n.members, cfg: n.cfg, valid_until_ms: now_ms + self.ttl_ms })
    }

    /// Renew: only if we still hold it (our ETag). The epoch does not change.
    pub async fn renew(&self, db: u64, h: &mut Held, now_ms: i64) -> Result<(), MetaError> {
        let n = Lease { holder: self.holder.to_string(), expires_ms: now_ms + self.ttl_ms, seq: h.seq + 1, members: h.members.clone(), cfg: h.cfg };
        let etag = self.meta.cas(&lease_path(db), h.etag.clone(), &n).await?;
        h.etag = etag;
        h.seq = n.seq;
        h.valid_until_ms = now_ms + self.ttl_ms;
        Ok(())
    }

    pub async fn release(&self, db: u64, h: &Held) -> Result<(), MetaError> {
        let n = Lease { holder: String::new(), expires_ms: 0, seq: h.seq + 1, members: h.members.clone(), cfg: h.cfg };
        self.meta.cas(&lease_path(db), h.etag.clone(), &n).await.map(|_| ())
    }

    /// Membership change (design §5.8): new members, new epoch (= the new seq), still the holder.
    pub async fn reconfigure(&self, db: u64, h: &mut Held, members: Vec<NodeId>, now_ms: i64) -> Result<(), MetaError> {
        let n = Lease { holder: self.holder.to_string(), expires_ms: now_ms + self.ttl_ms, seq: h.seq + 1, members: members.clone(), cfg: h.cfg + 1 };
        h.etag = self.meta.cas(&lease_path(db), h.etag.clone(), &n).await?;
        h.seq = n.seq;
        h.epoch = n.seq;
        h.members = members;
        h.cfg = n.cfg;
        h.valid_until_ms = now_ms + self.ttl_ms;
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
