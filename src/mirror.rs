//! The waiter's SST mirror (gp3 floor prototype, Fable idea 1): a disk floor laid out as a byte-for-byte copy of the
//! S3 objects of the databases' SSTs, one file per SST at the same relative path as the object
//! (`<dir>/tenants/<db>/compacted/<ULID>.sst`). Off by default (`--mirror-dir`); a measurement prototype.
//!
//! Why: the counter's disk floor ([`crate::counter`]) keeps one RAM entry per 4 KiB block (0.7-1.1% of the floor in RAM,
//! open item 1 of `components/waiter.md`). The mirror keeps one entry per SST: the waiter knows where every byte is from
//! the path and the offset alone, because the offsets are the S3 object's own.
//!
//! What it does:
//! - **Reads.** [`MirrorStore`] sits directly on the raw object store, under the S3 meters (trip meter, usage, counting
//!   store). A GET of an SST that is in the mirror is answered from the file with the same offsets and never reaches S3;
//!   it is not counted as an S3 request anywhere. A GET of an SST not (yet) in the mirror goes to S3 as before.
//! - **New SSTs.** A PUT (single or multipart) of an SST written by flush or compaction goes to S3 first; then, before the
//!   PUT returns, the same bytes are written to a temporary file, synced and renamed into place. slatedb publishes the
//!   manifest that names an SST only after its PUT returned, so the mirror holds every new SST before any reader can see
//!   it (the PR #47 rule). A failed mirror write only costs S3 GETs later; S3 stays the only durable copy.
//! - **Filling.** For every open database ([`on_manifest`], called by the maps loop at each new manifest), the SSTs of the
//!   manifest that are not in the mirror are copied from S3 (one GET each, through the full store stack so they are
//!   counted and billed), at most `fill_parallel` at once, optionally paced to `--mirror-fill-mbps`.
//! - **Leaving.** SSTs that leave the manifest (compaction) and that no other database still names (forks) are removed
//!   from the mirror; an SST deleted from S3 (GC) is removed too.
//! - **Drop and erase** ([`Mirror::purge_root`], from `dbs::forget_on_waiter`): every file under the database's directory,
//!   the complete ones and the temporary ones being written, is overwritten with zeros, synced and deleted, and the
//!   database's epoch moves on, so a fill or a PUT copy that was in flight cannot land a file afterwards (guarantee 6).
//!   The overwrite happens in place (no copy-on-write on ext4 or xfs), so the bytes are gone from the volume, not only
//!   from the directory.
//! - **Restart.** The files survive a process restart (the volume is meant to survive an instance replacement too):
//!   at start the directory is scanned (one entry per file), temporary files are scrubbed, and a background pass
//!   ([`Mirror::reconcile_with_store`]) scrubs the files whose object is no longer on S3 (a database dropped or erased
//!   while this process was down).
//!
//! RAM: one entry per SST in [`State::files`] and [`State::by_sst`], one per (database, SST) in the references; see
//! [`Mirror::index_bytes`]. The counter does not put on its disk floor a block whose SST is mirrored ([`covers`]): it is
//! read again from the mirror when needed, so the per-block bookkeeping of the disk floor stays empty.
//!
//! SSTs are named by ULIDs never reused and their bytes never change: a complete mirror file can never be stale.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, RenameOptions, Result, UploadPart,
};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// The mirror of this process, when `--mirror-dir` is given.
pub static MIRROR: OnceLock<Arc<Mirror>> = OnceLock::new();

pub fn get() -> Option<&'static Arc<Mirror>> {
    MIRROR.get()
}

/// True when the SST (its ULID text, the counter's key) has a complete file in the mirror.
pub fn covers(sst: &[u8; 26]) -> bool {
    match MIRROR.get() {
        Some(m) => m.st.lock().unwrap().by_sst.contains_key(sst),
        None => false,
    }
}

/// `<root>/compacted/<ULID>.sst` -> (root, ULID).
pub fn parse(path: &str) -> Option<(&str, [u8; 26])> {
    let (root, file) = path.rsplit_once("/compacted/")?;
    let stem = file.strip_suffix(".sst")?;
    let b = stem.as_bytes();
    if root.is_empty() || b.len() != 26 || !b.iter().all(|c| c.is_ascii_digit() || c.is_ascii_uppercase()) {
        return None;
    }
    let mut k = [0u8; 26];
    k.copy_from_slice(b);
    Some((root, k))
}

fn ulid_str(s: &[u8; 26]) -> &str {
    std::str::from_utf8(s).unwrap_or("?")
}

/// Meters (serverStatus `fenvix.waiter.mirror`).
#[derive(Default)]
pub struct MirrorMeters {
    /// GETs of SSTs answered from the mirror, and their bytes.
    pub hits: AtomicU64,
    pub hit_bytes: AtomicU64,
    /// GETs of SSTs not in the mirror (they went to S3).
    pub misses: AtomicU64,
    /// Mirror reads that failed (the GET then went to S3).
    pub read_fails: AtomicU64,
    /// New SSTs copied into the mirror at their PUT (flush, compaction), and their bytes; PUT copies that failed.
    pub pushed: AtomicU64,
    pub pushed_bytes: AtomicU64,
    pub push_fails: AtomicU64,
    /// SSTs copied from S3 by the filler, their bytes, failed fills, fills running now.
    pub filled: AtomicU64,
    pub filled_bytes: AtomicU64,
    pub fill_fails: AtomicU64,
    pub filling: AtomicU64,
    /// Milliseconds from the first fill of a burst to the moment no fill was running (the last burst).
    pub last_fill_ms: AtomicU64,
    /// Files removed because their SST left the manifests or S3.
    pub removed: AtomicU64,
    /// Files and bytes overwritten with zeros and deleted (drop, erase, temporary files, objects gone at start).
    pub scrubbed_files: AtomicU64,
    pub scrubbed_bytes: AtomicU64,
    /// Copies refused because their database was dropped or erased while they were in flight.
    pub stale_refused: AtomicU64,
}

#[derive(Default)]
struct State {
    /// Interned roots (`tenants/<db>`).
    roots: Vec<Arc<str>>,
    root_ids: HashMap<Arc<str>, u32>,
    /// (root, SST) -> length of the complete file. One entry per mirrored SST.
    files: HashMap<(u32, [u8; 26]), u64>,
    /// SST -> how many roots hold it (the counter asks by SST only).
    by_sst: HashMap<[u8; 26], u32>,
    /// Root -> epoch: moves on at every purge; a copy started before refuses to land.
    epochs: HashMap<u32, u64>,
    /// Database -> (last manifest seen, the (root, SST) its manifest names).
    refs: HashMap<String, (u64, HashSet<(u32, [u8; 26])>)>,
    /// Copies in flight (PUT copies and fills), so a fill does not start twice.
    inflight: HashSet<(u32, [u8; 26])>,
    bytes: u64,
}

impl State {
    fn root_id(&mut self, root: &str) -> u32 {
        if let Some(&i) = self.root_ids.get(root) {
            return i;
        }
        let i = self.roots.len() as u32;
        let a: Arc<str> = Arc::from(root);
        self.roots.push(Arc::clone(&a));
        self.root_ids.insert(a, i);
        i
    }

    fn insert(&mut self, k: (u32, [u8; 26]), len: u64) {
        if let Some(old) = self.files.insert(k, len) {
            self.bytes -= old;
        } else {
            *self.by_sst.entry(k.1).or_insert(0) += 1;
        }
        self.bytes += len;
    }

    fn remove(&mut self, k: &(u32, [u8; 26])) -> Option<u64> {
        let len = self.files.remove(k)?;
        self.bytes -= len;
        if let Some(n) = self.by_sst.get_mut(&k.1) {
            *n -= 1;
            if *n == 0 {
                self.by_sst.remove(&k.1);
            }
        }
        Some(len)
    }

    fn referenced(&self, k: &(u32, [u8; 26])) -> bool {
        self.refs.values().any(|(_, s)| s.contains(k))
    }
}

/// The mirror: a directory and its index.
pub struct Mirror {
    dir: PathBuf,
    st: Mutex<State>,
    /// The store fills read from (the full stack, so fill GETs are counted and billed); set by the server.
    fill_store: OnceLock<Arc<dyn ObjectStore>>,
    fill_sem: tokio::sync::Semaphore,
    /// Fill pacing (bytes per second, 0 = unpaced) and the next free instant of the pacer.
    fill_bps: AtomicU64,
    pacer: Mutex<Instant>,
    burst_start: Mutex<Option<Instant>>,
    pub m: MirrorMeters,
}

impl fmt::Debug for Mirror {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Mirror({})", self.dir.display())
    }
}

/// Overwrite a file with zeros, sync it, delete it. Returns the bytes overwritten.
pub fn scrub_file(p: &std::path::Path) -> std::io::Result<u64> {
    let len = std::fs::metadata(p)?.len();
    {
        let f = std::fs::OpenOptions::new().write(true).open(p)?;
        let zeros = vec![0u8; 1 << 20];
        let mut off = 0;
        while off < len {
            let n = (len - off).min(zeros.len() as u64) as usize;
            f.write_all_at(&zeros[..n], off)?;
            off += n as u64;
        }
        f.sync_data()?;
    }
    std::fs::remove_file(p)?;
    Ok(len)
}

fn walk(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else { return };
    for e in rd.flatten() {
        let p = e.path();
        match e.file_type() {
            Ok(t) if t.is_dir() => walk(&p, out),
            Ok(t) if t.is_file() => out.push(p),
            _ => {}
        }
    }
}

fn is_tmp(p: &std::path::Path) -> bool {
    p.file_name().and_then(|n| n.to_str()).map(|n| n.contains(".tmp-")).unwrap_or(false)
}

impl Mirror {
    /// Open (or create) the mirror in `dir`: complete files are indexed (one entry each), temporary ones scrubbed.
    /// `keep = false` scrubs everything first (an empty mirror).
    pub fn open(dir: PathBuf, keep: bool, fill_parallel: usize) -> std::io::Result<Arc<Mirror>> {
        std::fs::create_dir_all(&dir)?;
        let m = Mirror {
            dir: dir.clone(),
            st: Mutex::new(State::default()),
            fill_store: OnceLock::new(),
            fill_sem: tokio::sync::Semaphore::new(fill_parallel.max(1)),
            fill_bps: AtomicU64::new(0),
            pacer: Mutex::new(Instant::now()),
            burst_start: Mutex::new(None),
            m: MirrorMeters::default(),
        };
        let mut files = Vec::new();
        walk(&dir, &mut files);
        {
            let mut st = m.st.lock().unwrap();
            for p in files {
                let rel = p.strip_prefix(&dir).ok().and_then(|r| r.to_str()).map(|s| s.to_string());
                let parsed = rel.as_deref().and_then(parse).map(|(r, s)| (r.to_string(), s));
                match parsed {
                    Some((root, sst)) if keep && !is_tmp(&p) => {
                        let len = std::fs::metadata(&p)?.len();
                        let r = st.root_id(&root);
                        st.insert((r, sst), len);
                    }
                    _ => {
                        if let Ok(n) = scrub_file(&p) {
                            m.m.scrubbed_files.fetch_add(1, Relaxed);
                            m.m.scrubbed_bytes.fetch_add(n, Relaxed);
                        }
                    }
                }
            }
        }
        Ok(Arc::new(m))
    }

    /// Fills read through `store` (the server gives its full stack).
    pub fn set_fill_store(&self, store: Arc<dyn ObjectStore>) {
        let _ = self.fill_store.set(store);
    }

    /// Pace fills to this many MB/s over all fills (0 = unpaced).
    pub fn set_fill_mbps(&self, mbps: u64) {
        self.fill_bps.store(mbps * 1_000_000, Relaxed);
    }

    pub fn dir(&self) -> &std::path::Path {
        &self.dir
    }

    fn local(&self, root: &str, sst: &[u8; 26]) -> PathBuf {
        self.dir.join(root).join("compacted").join(format!("{}.sst", ulid_str(sst)))
    }

    /// (files, bytes) in the mirror.
    pub fn usage(&self) -> (u64, u64) {
        let st = self.st.lock().unwrap();
        (st.files.len() as u64, st.bytes)
    }

    /// Heap bytes of the mirror's bookkeeping (index, per-SST counts, references, roots), from the capacities of its
    /// tables: what the mirror costs in RAM. Each table slot costs its key and value plus one control byte.
    pub fn index_bytes(&self) -> u64 {
        use std::mem::size_of;
        let st = self.st.lock().unwrap();
        let slot = |kv: usize, cap: usize| (cap * (kv + 1)) as u64;
        let mut b = slot(size_of::<((u32, [u8; 26]), u64)>(), st.files.capacity());
        b += slot(size_of::<([u8; 26], u32)>(), st.by_sst.capacity());
        b += slot(size_of::<(u32, u64)>(), st.epochs.capacity());
        b += slot(size_of::<(u32, [u8; 26])>(), st.inflight.capacity());
        b += slot(size_of::<(String, (u64, HashSet<(u32, [u8; 26])>))>(), st.refs.capacity());
        for (k, (_, s)) in &st.refs {
            b += k.capacity() as u64 + slot(size_of::<(u32, [u8; 26])>(), s.capacity());
        }
        b += slot(size_of::<(Arc<str>, u32)>(), st.root_ids.capacity()) + (st.roots.capacity() * size_of::<Arc<str>>()) as u64;
        b += st.roots.iter().map(|r| r.len() as u64 + 16).sum::<u64>();
        b
    }

    /// Length of the complete file of this SST, if mirrored.
    pub fn len_of(&self, root: &str, sst: &[u8; 26]) -> Option<u64> {
        let st = self.st.lock().unwrap();
        let r = *st.root_ids.get(root)?;
        st.files.get(&(r, *sst)).copied()
    }

    /// The current epoch of a root (captured before a copy starts).
    pub fn epoch(&self, root: &str) -> u64 {
        let mut st = self.st.lock().unwrap();
        let r = st.root_id(root);
        st.epochs.get(&r).copied().unwrap_or(0)
    }

    fn tmp_path(&self, root: &str, sst: &[u8; 26]) -> std::io::Result<PathBuf> {
        let d = self.dir.join(root).join("compacted");
        std::fs::create_dir_all(&d)?;
        Ok(d.join(format!("{}.sst.tmp-{:016x}", ulid_str(sst), rand::random::<u64>())))
    }

    /// Put a complete, synced temporary file in place, unless the root was purged since `epoch`. True if it landed.
    fn commit(&self, root: &str, sst: &[u8; 26], tmp: &std::path::Path, len: u64, epoch: u64) -> bool {
        let mut st = self.st.lock().unwrap();
        let r = st.root_id(root);
        st.inflight.remove(&(r, *sst));
        if st.epochs.get(&r).copied().unwrap_or(0) != epoch {
            drop(st);
            self.m.stale_refused.fetch_add(1, Relaxed);
            if let Ok(n) = scrub_file(tmp) {
                self.m.scrubbed_files.fetch_add(1, Relaxed);
                self.m.scrubbed_bytes.fetch_add(n, Relaxed);
            }
            return false;
        }
        // under the lock: a purge (which takes it to move the epoch) cannot run between the check and the rename
        if std::fs::rename(tmp, self.local(root, sst)).is_err() {
            let _ = std::fs::remove_file(tmp);
            return false;
        }
        st.insert((r, *sst), len);
        true
    }

    fn abandon(&self, root: &str, sst: &[u8; 26], tmp: Option<&std::path::Path>) {
        {
            let mut st = self.st.lock().unwrap();
            let r = st.root_id(root);
            st.inflight.remove(&(r, *sst));
        }
        if let Some(t) = tmp {
            let _ = scrub_file(t);
        }
    }

    /// Write `payload` as the mirror file of a new SST (its PUT to S3 succeeded). Blocking file I/O.
    fn push_bytes(&self, root: &str, sst: &[u8; 26], payload: &PutPayload, epoch: u64) {
        {
            let mut st = self.st.lock().unwrap();
            let r = st.root_id(root);
            st.inflight.insert((r, *sst));
        }
        let res = (|| -> std::io::Result<(PathBuf, u64)> {
            let tmp = self.tmp_path(root, sst)?;
            let mut f = std::fs::File::create(&tmp)?;
            let mut n = 0u64;
            for b in payload.iter() {
                f.write_all(b)?;
                n += b.len() as u64;
            }
            f.sync_data()?;
            Ok((tmp, n))
        })();
        match res {
            Ok((tmp, n)) => {
                if self.commit(root, sst, &tmp, n, epoch) {
                    self.m.pushed.fetch_add(1, Relaxed);
                    self.m.pushed_bytes.fetch_add(n, Relaxed);
                }
            }
            Err(e) => {
                self.m.push_fails.fetch_add(1, Relaxed);
                eprintln!("mirror: copy of the new SST {root}/{} failed: {e}", ulid_str(sst));
                self.abandon(root, sst, None);
            }
        }
    }

    /// Remove the file of an SST (it left the manifests, or S3). Plain delete: the bytes still exist on S3.
    pub fn remove(&self, root: &str, sst: &[u8; 26]) -> bool {
        let gone = {
            let mut st = self.st.lock().unwrap();
            let Some(&r) = st.root_ids.get(root) else { return false };
            st.remove(&(r, *sst)).is_some()
        };
        if gone {
            let _ = std::fs::remove_file(self.local(root, sst));
            self.m.removed.fetch_add(1, Relaxed);
        }
        gone
    }

    /// A database was dropped or erased: every file under its root (complete or being written) is overwritten with
    /// zeros, synced and deleted, its index entries and references go, and its epoch moves on so no copy in flight can
    /// land afterwards. Returns (files, bytes) scrubbed.
    pub fn purge_root(&self, db: &str, root: &str) -> (u64, u64) {
        {
            let mut st = self.st.lock().unwrap();
            let r = st.root_id(root);
            *st.epochs.entry(r).or_insert(0) += 1;
            let ks: Vec<(u32, [u8; 26])> = st.files.keys().filter(|k| k.0 == r).copied().collect();
            for k in ks {
                st.remove(&k);
            }
            st.refs.remove(db);
            for (_, s) in st.refs.values_mut() {
                s.retain(|k| k.0 != r);
            }
        }
        let mut files = Vec::new();
        walk(&self.dir.join(root), &mut files);
        let (mut n, mut b) = (0, 0);
        for p in files {
            if let Ok(x) = scrub_file(&p) {
                n += 1;
                b += x;
            }
        }
        let _ = std::fs::remove_dir_all(self.dir.join(root));
        self.m.scrubbed_files.fetch_add(n, Relaxed);
        self.m.scrubbed_bytes.fetch_add(b, Relaxed);
        (n, b)
    }

    /// The SSTs one database's manifest names now (`refs` as (root, SST)): fill the missing ones (in the background),
    /// remove the ones that left it and that no other database names. Returns the fills started.
    pub fn track(self: &Arc<Self>, db: &str, manifest: u64, now: Vec<(String, [u8; 26])>) -> usize {
        let epochs: HashMap<String, u64> = now.iter().map(|(r, _)| (r.clone(), self.epoch(r))).collect();
        let (to_fill, to_remove) = {
            let mut st = self.st.lock().unwrap();
            if st.refs.get(db).map(|(m, _)| *m) == Some(manifest) && !now.iter().any(|(r, s)| {
                let k = st.root_ids.get(r.as_str()).map(|&i| (i, *s));
                k.map_or(true, |k| !st.files.contains_key(&k) && !st.inflight.contains(&k))
            }) {
                return 0;
            }
            let mut set = HashSet::with_capacity(now.len());
            let mut to_fill = Vec::new();
            for (root, s) in &now {
                let r = st.root_id(root);
                set.insert((r, *s));
                if !st.files.contains_key(&(r, *s)) && st.inflight.insert((r, *s)) {
                    to_fill.push((root.clone(), *s));
                }
            }
            let old = st.refs.insert(db.to_string(), (manifest, set)).map(|(_, s)| s).unwrap_or_default();
            let to_remove: Vec<(Arc<str>, [u8; 26])> = old
                .into_iter()
                .filter(|k| !st.referenced(k) && st.files.contains_key(k))
                .map(|k| (Arc::clone(&st.roots[k.0 as usize]), k.1))
                .collect();
            (to_fill, to_remove)
        };
        for (r, s) in to_remove {
            self.remove(&r, &s);
        }
        let n = to_fill.len();
        for (root, s) in to_fill {
            let me = Arc::clone(self);
            let ep = epochs.get(&root).copied().unwrap_or(0);
            tokio::spawn(async move { me.fill_one(root, s, ep).await });
        }
        n
    }

    async fn pace(&self, n: u64) {
        let bps = self.fill_bps.load(Relaxed);
        if bps == 0 {
            return;
        }
        let at = {
            let mut p = self.pacer.lock().unwrap();
            let now = Instant::now();
            let at = (*p).max(now);
            *p = at + Duration::from_secs_f64(n as f64 / bps as f64);
            at
        };
        tokio::time::sleep_until(at.into()).await;
    }

    async fn fill_one(self: Arc<Self>, root: String, sst: [u8; 26], epoch: u64) {
        let _permit = self.fill_sem.acquire().await;
        if self.m.filling.fetch_add(1, Relaxed) == 0 {
            self.burst_start.lock().unwrap().get_or_insert_with(Instant::now);
        }
        let r = self.fill_inner(&root, &sst, epoch).await;
        match r {
            Ok(Some(n)) => {
                self.m.filled.fetch_add(1, Relaxed);
                self.m.filled_bytes.fetch_add(n, Relaxed);
            }
            Ok(None) => {}
            Err(e) => {
                self.m.fill_fails.fetch_add(1, Relaxed);
                if self.m.fill_fails.load(Relaxed) <= 5 {
                    eprintln!("mirror: fill of {root}/{} failed: {e}", ulid_str(&sst));
                }
                // let the next manifest look try again
                let mut st = self.st.lock().unwrap();
                for (_, (m, _)) in st.refs.iter_mut() {
                    *m = u64::MAX;
                }
            }
        }
        if self.m.filling.fetch_sub(1, Relaxed) == 1 {
            if let Some(t0) = self.burst_start.lock().unwrap().take() {
                self.m.last_fill_ms.store(t0.elapsed().as_millis() as u64, Relaxed);
            }
        }
    }

    async fn fill_inner(&self, root: &str, sst: &[u8; 26], epoch: u64) -> std::result::Result<Option<u64>, String> {
        if self.len_of(root, sst).is_some() {
            self.abandon(root, sst, None);
            return Ok(None);
        }
        let Some(store) = self.fill_store.get() else {
            self.abandon(root, sst, None);
            return Err("no fill store".into());
        };
        let path = Path::from(format!("{root}/compacted/{}.sst", ulid_str(sst)));
        let tmp = match self.tmp_path(root, sst) {
            Ok(t) => t,
            Err(e) => {
                self.abandon(root, sst, None);
                return Err(e.to_string());
            }
        };
        let res: std::result::Result<u64, String> = async {
            let g = store.get(&path).await.map_err(|e| e.to_string())?;
            let size = g.meta.size;
            let mut stream = g.into_stream();
            let f = std::fs::File::create(&tmp).map_err(|e| e.to_string())?;
            let mut n = 0u64;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| e.to_string())?;
                self.pace(chunk.len() as u64).await;
                f.write_all_at(&chunk, n).map_err(|e| e.to_string())?;
                n += chunk.len() as u64;
            }
            if n != size {
                return Err(format!("short object: {n} of {size} bytes"));
            }
            f.sync_data().map_err(|e| e.to_string())?;
            Ok(n)
        }
        .await;
        match res {
            Ok(n) => Ok(self.commit(root, sst, &tmp, n, epoch).then_some(n)),
            Err(e) => {
                self.abandon(root, sst, Some(&tmp));
                Err(e)
            }
        }
    }

    /// After a start: scrub the files whose object is no longer on S3 (or has another size), for example a database
    /// dropped or erased while this process was down. One LIST per root. Returns the files scrubbed.
    pub async fn reconcile_with_store(&self, store: &Arc<dyn ObjectStore>) -> u64 {
        let roots: Vec<(u32, Arc<str>)> = {
            let st = self.st.lock().unwrap();
            let used: HashSet<u32> = st.files.keys().map(|k| k.0).collect();
            used.into_iter().map(|r| (r, Arc::clone(&st.roots[r as usize]))).collect()
        };
        let mut n = 0;
        for (r, root) in roots {
            let prefix = Path::from(format!("{root}/compacted"));
            let mut on_s3: HashMap<[u8; 26], u64> = HashMap::new();
            let mut s = store.list(Some(&prefix));
            let mut ok = true;
            while let Some(m) = s.next().await {
                match m {
                    Ok(m) => {
                        if let Some((_, k)) = parse(m.location.as_ref()) {
                            on_s3.insert(k, m.size);
                        }
                    }
                    Err(_) => {
                        ok = false;
                        break;
                    }
                }
            }
            if !ok {
                continue;
            }
            let stale: Vec<[u8; 26]> = {
                let st = self.st.lock().unwrap();
                st.files.iter().filter(|((rr, k), len)| *rr == r && on_s3.get(k) != Some(*len)).map(|((_, k), _)| *k).collect()
            };
            for k in stale {
                let gone = self.st.lock().unwrap().remove(&(r, k)).is_some();
                if gone {
                    if let Ok(b) = scrub_file(&self.local(&root, &k)) {
                        self.m.scrubbed_files.fetch_add(1, Relaxed);
                        self.m.scrubbed_bytes.fetch_add(b, Relaxed);
                    }
                    n += 1;
                }
            }
        }
        n
    }

    /// Answer a GET from the mirror, or None (not mirrored, conditional, or the file failed).
    async fn read(&self, location: &Path, root: &str, sst: &[u8; 26], o: &GetOptions) -> Option<GetResult> {
        if o.if_match.is_some() || o.if_none_match.is_some() || o.if_modified_since.is_some() || o.if_unmodified_since.is_some() || o.version.is_some() {
            return None;
        }
        let len = self.len_of(root, sst)?;
        let range = match &o.range {
            None => 0..len,
            Some(GetRange::Bounded(b)) => b.start..b.end.min(len),
            Some(GetRange::Offset(s)) => *s..len,
            Some(GetRange::Suffix(n)) => len.saturating_sub(*n)..len,
        };
        if range.start >= range.end && len > 0 {
            return None; // past the end: S3 answers it (an error), as without the mirror
        }
        let meta = ObjectMeta { location: location.clone(), last_modified: chrono::Utc::now(), size: len, e_tag: None, version: None };
        if o.head {
            let stream = futures::stream::empty().boxed();
            return Some(GetResult { payload: GetResultPayload::Stream(stream), meta, range, attributes: Default::default(), extensions: Default::default() });
        }
        let p = self.local(root, sst);
        let r = range.clone();
        let body = tokio::task::spawn_blocking(move || -> std::io::Result<Bytes> {
            let f = std::fs::File::open(&p)?;
            let mut buf = vec![0u8; (r.end - r.start) as usize];
            f.read_exact_at(&mut buf, r.start)?;
            Ok(Bytes::from(buf))
        })
        .await
        .ok()?;
        match body {
            Ok(b) => {
                self.m.hits.fetch_add(1, Relaxed);
                self.m.hit_bytes.fetch_add(b.len() as u64, Relaxed);
                let stream = futures::stream::once(async move { Ok(b) }).boxed();
                Some(GetResult { payload: GetResultPayload::Stream(stream), meta, range, attributes: Default::default(), extensions: Default::default() })
            }
            Err(_) => {
                self.m.read_fails.fetch_add(1, Relaxed);
                None
            }
        }
    }
}

/// Called by the maps loop at every new manifest of an open database: fill and trim the mirror.
pub fn on_manifest(id: &str, m: &slatedb::VersionedManifest) {
    let Some(mir) = get() else { return };
    mir.track(id, m.id(), manifest_refs(&crate::lifecycle::tenant_path(id), m));
}

/// The (root, SST) a manifest names: its own SSTs under `own`, the ones it borrows (a fork, a PITR restore) under the
/// root of the database they come from.
pub fn manifest_refs(own: &str, m: &slatedb::VersionedManifest) -> Vec<(String, [u8; 26])> {
    let mut ext: HashMap<[u8; 26], String> = HashMap::new();
    for x in m.external_dbs() {
        for s in &x.sst_ids {
            if let Some(k) = crate::counter::sst_of(s) {
                ext.insert(k, x.path.clone());
            }
        }
    }
    let mut now: Vec<(String, [u8; 26])> = Vec::new();
    let mut seen = HashSet::new();
    for v in m.all_sst_views() {
        if let Some(k) = crate::counter::sst_of(&v.sst.id) {
            if seen.insert(k) {
                now.push((ext.get(&k).cloned().unwrap_or_else(|| own.to_string()), k));
            }
        }
    }
    now
}

/// The object store with the mirror under it: see the module comment.
pub struct MirrorStore {
    inner: Arc<dyn ObjectStore>,
    mirror: Arc<Mirror>,
}

pub fn wrap(inner: Arc<dyn ObjectStore>, mirror: Arc<Mirror>) -> Arc<dyn ObjectStore> {
    Arc::new(MirrorStore { inner, mirror })
}

impl fmt::Debug for MirrorStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MirrorStore({:?})", self.mirror)
    }
}

impl fmt::Display for MirrorStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MirrorStore({})", self.inner)
    }
}

/// A multipart upload that also writes its parts, in order, to the mirror's temporary file.
struct TeeUpload {
    inner: Box<dyn MultipartUpload>,
    mirror: Arc<Mirror>,
    root: String,
    sst: [u8; 26],
    epoch: u64,
    tmp: Option<(std::fs::File, PathBuf)>,
    pos: u64,
}

impl fmt::Debug for TeeUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TeeUpload({}/{})", self.root, ulid_str(&self.sst))
    }
}

#[async_trait]
impl MultipartUpload for TeeUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        if let Some((f, _)) = &self.tmp {
            let mut ok = true;
            for b in data.iter() {
                if f.write_all_at(b, self.pos).is_err() {
                    ok = false;
                    break;
                }
                self.pos += b.len() as u64;
            }
            if !ok {
                self.mirror.m.push_fails.fetch_add(1, Relaxed);
                let (_, p) = self.tmp.take().unwrap();
                self.mirror.abandon(&self.root, &self.sst, Some(&p));
            }
        }
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let r = self.inner.complete().await;
        if let Some((f, p)) = self.tmp.take() {
            if r.is_ok() && f.sync_data().is_ok() {
                if self.mirror.commit(&self.root, &self.sst, &p, self.pos, self.epoch) {
                    self.mirror.m.pushed.fetch_add(1, Relaxed);
                    self.mirror.m.pushed_bytes.fetch_add(self.pos, Relaxed);
                }
            } else {
                self.mirror.abandon(&self.root, &self.sst, Some(&p));
            }
        }
        r
    }

    async fn abort(&mut self) -> Result<()> {
        if let Some((_, p)) = self.tmp.take() {
            self.mirror.abandon(&self.root, &self.sst, Some(&p));
        }
        self.inner.abort().await
    }
}

impl Drop for TeeUpload {
    fn drop(&mut self) {
        if let Some((_, p)) = self.tmp.take() {
            self.mirror.abandon(&self.root, &self.sst, Some(&p));
        }
    }
}

#[async_trait]
impl ObjectStore for MirrorStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        let Some((root, sst)) = parse(location.as_ref()) else {
            return self.inner.put_opts(location, payload, opts).await;
        };
        let epoch = self.mirror.epoch(root);
        let r = self.inner.put_opts(location, payload.clone(), opts).await?;
        // S3 has it; now the mirror, before the PUT returns (so before any manifest names it)
        let (mirror, root, p) = (Arc::clone(&self.mirror), root.to_string(), payload);
        let _ = tokio::task::spawn_blocking(move || mirror.push_bytes(&root, &sst, &p, epoch)).await;
        Ok(r)
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        let Some((root, sst)) = parse(location.as_ref()) else { return Ok(inner) };
        let epoch = self.mirror.epoch(root);
        {
            let mut st = self.mirror.st.lock().unwrap();
            let r = st.root_id(root);
            st.inflight.insert((r, sst));
        }
        let tmp = self.mirror.tmp_path(root, &sst).and_then(|p| std::fs::File::create(&p).map(|f| (f, p))).ok();
        if tmp.is_none() {
            self.mirror.m.push_fails.fetch_add(1, Relaxed);
            self.mirror.abandon(root, &sst, None);
        }
        Ok(Box::new(TeeUpload { inner, mirror: Arc::clone(&self.mirror), root: root.to_string(), sst, epoch, tmp, pos: 0 }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if let Some((root, sst)) = parse(location.as_ref()) {
            if let Some(r) = self.mirror.read(location, root, &sst, &options).await {
                return Ok(r);
            }
            self.mirror.m.misses.fetch_add(1, Relaxed);
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        let m = Arc::clone(&self.mirror);
        self.inner
            .delete_stream(locations)
            .map(move |r| {
                if let Ok(p) = &r {
                    if let Some((root, sst)) = parse(p.as_ref()) {
                        m.remove(root, &sst);
                    }
                }
                r
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        if let Some((root, sst)) = parse(to.as_ref()) {
            self.mirror.remove(root, &sst);
        }
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        for p in [from, to] {
            if let Some((root, sst)) = parse(p.as_ref()) {
                self.mirror.remove(root, &sst);
            }
        }
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use slatedb::config::{CompactorOptions, Settings};
    use slatedb::Db;
    use std::collections::BTreeMap;

    /// Under the mirror: counts the GETs of compacted SSTs that reach the store, and how many of them were for an SST the
    /// mirror held at that moment (must be 0: a mirrored SST is never read from S3).
    struct Spy {
        inner: Arc<dyn ObjectStore>,
        mirror: OnceLock<Arc<Mirror>>,
        sst_gets: AtomicU64,
        while_mirrored: AtomicU64,
    }
    impl fmt::Debug for Spy {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Spy")
        }
    }
    impl fmt::Display for Spy {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Spy")
        }
    }
    #[async_trait]
    impl ObjectStore for Spy {
        async fn put_opts(&self, l: &Path, p: PutPayload, o: PutOptions) -> Result<PutResult> {
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(&self, l: &Path, o: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(l, o).await
        }
        async fn get_opts(&self, l: &Path, o: GetOptions) -> Result<GetResult> {
            if let Some((root, sst)) = parse(l.as_ref()) {
                self.sst_gets.fetch_add(1, Relaxed);
                if let Some(m) = self.mirror.get() {
                    if m.len_of(root, &sst).is_some() {
                        self.while_mirrored.fetch_add(1, Relaxed);
                    }
                }
            }
            self.inner.get_opts(l, o).await
        }
        fn delete_stream(&self, l: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
            self.inner.delete_stream(l)
        }
        fn list(&self, p: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> Result<()> {
            self.inner.copy_opts(f, t, o).await
        }
        async fn rename_opts(&self, f: &Path, t: &Path, o: RenameOptions) -> Result<()> {
            self.inner.rename_opts(f, t, o).await
        }
    }

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("mirror-test-{tag}-{}-{:08x}", std::process::id(), rand::random::<u32>()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    fn count_in_dir(dir: &std::path::Path, needle: &[u8]) -> usize {
        let mut files = Vec::new();
        walk(dir, &mut files);
        files.iter().map(|p| std::fs::read(p).map(|b| b.windows(needle.len()).filter(|w| *w == needle).count()).unwrap_or(0)).sum()
    }

    fn settings() -> Settings {
        let mut s = Settings::default();
        s.l0_sst_size_bytes = 32 << 10;
        s.l0_max_ssts = 64;
        s.flush_interval = Some(Duration::from_millis(20));
        s.manifest_poll_interval = Duration::from_millis(50);
        s.compactor_options = Some(CompactorOptions { poll_interval: Duration::from_millis(50), ..Default::default() });
        s
    }

    async fn object_bytes(store: &Arc<dyn ObjectStore>, p: &str) -> Option<Bytes> {
        store.get(&Path::from(p)).await.ok()?.bytes().await.ok()
    }

    /// Point 3 of the gp3-floor contract. Writes, deletes and reads with compaction running during the reads, through
    /// the server's counter (tiny RAM, so blocks keep leaving it) and the mirror: every read equals an exact model; no
    /// GET reaches S3 for an SST the mirror holds; after the run every SST of the manifest is mirrored byte for byte and
    /// every SST that left the manifest has left the mirror.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn compaction_during_reads_no_stale_no_s3_for_mirrored() {
        let dir = tmpdir("fresh");
        let mirror = Mirror::open(dir.clone(), false, 4).unwrap();
        let _ = MIRROR.set(Arc::clone(&mirror)); // the counter asks the global one (covers)
        let mirror = Arc::clone(MIRROR.get().unwrap());
        let base: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let spy = Arc::new(Spy { inner: Arc::clone(&base), mirror: OnceLock::new(), sst_gets: AtomicU64::new(0), while_mirrored: AtomicU64::new(0) });
        let _ = spy.mirror.set(Arc::clone(&mirror));
        let spy_store: Arc<dyn ObjectStore> = spy.clone();
        let store = wrap(spy_store, Arc::clone(&mirror));
        mirror.set_fill_store(Arc::clone(&store));
        let counter = crate::counter::Counter::new(128 << 10, None, 0).unwrap();
        let root = "tenants/fresh";
        let db = Db::builder(root, Arc::clone(&store)).with_settings(settings()).with_db_cache(counter.split_cache(), 1).build().await.unwrap();

        // the maps loop's job in the server: look at the manifest often, fill and trim the mirror
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let tracker = {
            let (db, mirror, stop) = (db.clone(), Arc::clone(&mirror), Arc::clone(&stop));
            tokio::spawn(async move {
                while !stop.load(Relaxed) {
                    let m = db.manifest();
                    mirror.track("fresh", m.id(), manifest_refs(root, &m));
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
        };

        let mut model: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        use rand::{Rng, SeedableRng};
        let keys = 3000u32;
        let (mut reads, mut scans) = (0u64, 0u64);
        let mut compactions_seen = 0;
        let mut prev: HashSet<[u8; 26]> = HashSet::new();
        for round in 0..60u32 {
            // a burst of writes and deletes: new L0 SSTs, compaction of the old ones
            let mut batch = slatedb::WriteBatch::new();
            for _ in 0..400 {
                let k = format!("k{:05}", rng.gen_range(0..keys));
                if rng.gen_bool(0.1) {
                    batch.delete(&k);
                    model.remove(&k);
                } else {
                    let v = format!("v{round}-{}-{}", k, "x".repeat(rng.gen_range(50..400))).into_bytes();
                    batch.put(&k, &v);
                    model.insert(k, v);
                }
            }
            db.write(batch).await.unwrap();
            db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
            // reads while compaction runs: point reads and a range scan, each against the model
            for _ in 0..300 {
                let k = format!("k{:05}", rng.gen_range(0..keys));
                let got = db.get(&k).await.unwrap().map(|b| b.to_vec());
                assert_eq!(got.as_ref(), model.get(&k), "round {round}: stale or missing value for {k}");
                reads += 1;
            }
            let a = rng.gen_range(0..keys - 200);
            let (lo, hi) = (format!("k{a:05}"), format!("k{:05}", a + 200));
            let mut it = db.scan(lo.clone()..hi.clone()).await.unwrap();
            let mut got = Vec::new();
            while let Some(kv) = it.next().await.unwrap() {
                got.push((String::from_utf8(kv.key.to_vec()).unwrap(), kv.value.to_vec()));
            }
            let want: Vec<(String, Vec<u8>)> = model.range(lo..hi).map(|(k, v)| (k.clone(), v.clone())).collect();
            assert_eq!(got, want, "round {round}: scan differs from the model");
            scans += 1;
            // compaction seen: an SST of the last look left the manifest
            let now: HashSet<[u8; 26]> = manifest_refs(root, &db.manifest()).into_iter().map(|(_, s)| s).collect();
            if prev.iter().any(|s| !now.contains(s)) {
                compactions_seen += 1;
            }
            prev = now;
        }
        stop.store(true, Relaxed);
        tracker.await.unwrap();
        // a last look, then the checks on the mirror itself
        let m = db.manifest();
        let refs = manifest_refs(root, &m);
        mirror.track("fresh", m.id(), refs.clone());
        let live: HashSet<[u8; 26]> = refs.iter().map(|(_, s)| *s).collect();
        for (r, s) in &refs {
            let len = mirror.len_of(r, s).unwrap_or_else(|| panic!("SST {} of the manifest is not in the mirror", ulid_str(s)));
            let p = format!("{r}/compacted/{}.sst", ulid_str(s));
            let on_s3 = object_bytes(&base, &p).await.expect("object on S3");
            let local = std::fs::read(mirror.local(r, s)).unwrap();
            assert_eq!(len, on_s3.len() as u64);
            assert!(local == on_s3, "mirror file of {p} differs from the S3 object");
        }
        let mut files = Vec::new();
        walk(&dir, &mut files);
        for f in &files {
            let rel = f.strip_prefix(&dir).unwrap().to_str().unwrap().to_string();
            let (_, s) = parse(&rel).expect("only SST files in the mirror");
            assert!(live.contains(&s), "{rel} left the manifest but stays in the mirror");
        }
        let mm = &mirror.m;
        let (hits, pushed, removed) = (mm.hits.load(Relaxed), mm.pushed.load(Relaxed), mm.removed.load(Relaxed));
        println!(
            "reads {reads}, scans {scans}, compaction changes seen {compactions_seen}; mirror: hits {hits}, pushed {pushed}, filled {}, removed {removed}, files now {}; \
             SST GETs that reached S3 {}, of which for a mirrored SST {}; counter entries not sent to a disk floor (mirrored) {}",
            mm.filled.load(Relaxed),
            files.len(),
            spy.sst_gets.load(Relaxed),
            spy.while_mirrored.load(Relaxed),
            counter.m.mirror_skipped.load(Relaxed)
        );
        assert!(removed > 0 && compactions_seen > 0, "the test must see compaction replace SSTs");
        assert!(hits > 0 && pushed > 0);
        assert_eq!(spy.while_mirrored.load(Relaxed), 0, "a GET went to S3 for an SST already in the mirror");
        db.close().await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Point 4 at the library level: a purge overwrites and deletes every file of the root, the other root keeps its own,
    /// and a copy in flight at the purge (a PUT copy started before it) is refused and leaves nothing.
    #[tokio::test]
    async fn purge_scrubs_and_refuses_copies_in_flight() {
        let dir = tmpdir("purge");
        let mirror = Mirror::open(dir.clone(), false, 2).unwrap();
        let base: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = wrap(Arc::clone(&base), Arc::clone(&mirror));
        let id = |n: u32| format!("01JX{n:022}");
        for (db, mark) in [("a", "SECRETA1"), ("b", "KEEPB111")] {
            for n in 0..5 {
                let body = format!("{mark}-{}", "z".repeat(5000)).into_bytes();
                store.put(&Path::from(format!("tenants/{db}/compacted/{}.sst", id(n))), body.into()).await.unwrap();
            }
        }
        assert_eq!(count_in_dir(&dir, b"SECRETA1"), 5);
        // a multipart copy of db a in flight when the drop comes
        let mut up = store.put_multipart(&Path::from(format!("tenants/a/compacted/{}.sst", id(99)))).await.unwrap();
        up.put_part(PutPayload::from(b"SECRETA1-late".to_vec())).await.unwrap();
        let (n, b) = mirror.purge_root("a", "tenants/a");
        assert!(n >= 6 && b > 0, "purge scrubbed {n} files {b} bytes");
        up.complete().await.unwrap();
        assert_eq!(count_in_dir(&dir, b"SECRETA1"), 0, "bytes of the dropped database stay in the mirror");
        assert_eq!(count_in_dir(&dir, b"KEEPB111"), 5, "the purge touched the other database");
        assert_eq!(mirror.m.stale_refused.load(Relaxed), 1);
        assert!(mirror.len_of("tenants/a", &id(99).as_bytes().try_into().unwrap()).is_none());
        // a restart keeps the other database's files, and scrubs a file whose object is gone from the store
        drop(store);
        let orphan = dir.join("tenants/c/compacted");
        std::fs::create_dir_all(&orphan).unwrap();
        std::fs::write(orphan.join(format!("{}.sst", id(7))), b"SECRETC1 of a database erased while down").unwrap();
        let again = Mirror::open(dir.clone(), true, 2).unwrap();
        assert_eq!(again.usage().0, 6);
        assert_eq!(again.reconcile_with_store(&base).await, 1);
        assert_eq!(count_in_dir(&dir, b"SECRETC1"), 0);
        assert_eq!(again.usage().0, 5);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
