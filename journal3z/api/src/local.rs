//! Single-node journal on the local disk: the semantics of round3's `--local-log-dir` (`mongo-server/src/locallog.rs`
//! on branch `round3`) behind the [`Journal`] trait.
//!
//! - One shared log for every database of the process: segment files `seg-NNNNNNNNNNNN.j3z` in one directory.
//! - One writer thread appends every queued record, calls `fdatasync` once for the lot, then resolves their futures
//!   (group fsync across databases). A failed write or fsync stops the journal: every later call gets `Io`.
//! - A torn or corrupt frame ends the segment it is in (it was never acknowledged: acks follow the fsync).
//! - Records above the database's uploaded index are kept in memory and returned by `open` for replay.
//! - A closed segment is deleted once every record in it is uploaded (or its database erased): round3's low water.
//! - Epochs are checked in-process. One disk cannot fence another machine; that is what the 3-zone backend adds.
//! - Fence state (`promised`, uploaded index, erased) survives segment deletion through `fences.j3z`, rewritten
//!   atomically (tmp + fsync + rename + directory fsync) before any segment is deleted.
//!
//! Frame: `len u32 LE | crc32 u32 LE | body`; body: `kind u8 | name_len u16 | name | incarnation u64 | fields`.

use crate::{Appended, DbKey, Durable, Epoch, Index, Journal, JournalError, Opened};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{mpsc, Arc, Mutex};

const K_OPEN: u8 = 1;
const K_ENTRY: u8 = 2;
const K_UPLOADED: u8 = 3;
const K_ERASE: u8 = 4;
const K_FENCE: u8 = 5;

#[derive(Clone, Debug)]
pub struct Config {
    /// A segment is closed (and a new one opened) once it reaches this size.
    pub segment_max: u64,
    /// Per-database hard cap on bytes appended but not yet uploaded (`Quota` above it).
    pub db_hard_cap: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config { segment_max: 64 << 20, db_hard_cap: 128 << 20 }
    }
}

#[derive(Default, Debug)]
struct DbState {
    promised: Epoch,
    /// The writer of `promised` has opened (appends allowed).
    open: bool,
    uploaded: u64,
    last: u64,
    erased: bool,
    entries: BTreeMap<u64, Bytes>,
    unflushed_bytes: u64,
}

impl DbState {
    fn next(&self) -> u64 {
        self.last.max(self.uploaded) + 1
    }
    fn trim(&mut self, upto: u64) {
        while let Some((&i, b)) = self.entries.first_key_value() {
            if i > upto {
                break;
            }
            self.unflushed_bytes -= b.len() as u64;
            self.entries.pop_first();
        }
    }
}

struct State {
    dbs: HashMap<DbKey, DbState>,
    /// Closed segments: number -> highest entry index per database in it.
    closed: BTreeMap<u64, HashMap<DbKey, u64>>,
    /// The segment being written and its per-database highest index.
    current: (u64, HashMap<DbKey, u64>),
    failed: Option<String>,
}

struct Op {
    frame: Vec<u8>,
    entry: Option<(DbKey, u64)>,
    done: Option<tokio::sync::oneshot::Sender<Result<(), JournalError>>>,
}

pub struct LocalJournal {
    dir: PathBuf,
    id: String,
    cfg: Config,
    st: Arc<Mutex<State>>,
    tx: Mutex<mpsc::Sender<Op>>,
    pub records: Arc<AtomicU64>,
    pub fsyncs: Arc<AtomicU64>,
}

fn put_key(b: &mut Vec<u8>, k: &DbKey) {
    b.extend_from_slice(&(k.name.len() as u16).to_le_bytes());
    b.extend_from_slice(k.name.as_bytes());
    b.extend_from_slice(&k.incarnation.to_le_bytes());
}

fn frame(kind: u8, k: &DbKey, fields: &[u64], data: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(32 + k.name.len() + data.len());
    body.push(kind);
    put_key(&mut body, k);
    for f in fields {
        body.extend_from_slice(&f.to_le_bytes());
    }
    body.extend_from_slice(data);
    let mut out = Vec::with_capacity(body.len() + 8);
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

struct Rec {
    kind: u8,
    key: DbKey,
    f: [u64; 3],
    data: Bytes,
}

fn parse_body(b: &[u8]) -> Option<Rec> {
    let kind = *b.first()?;
    let nl = u16::from_le_bytes(b.get(1..3)?.try_into().ok()?) as usize;
    let name = std::str::from_utf8(b.get(3..3 + nl)?).ok()?.to_string();
    let mut p = 3 + nl;
    let u = |p: &mut usize| -> Option<u64> {
        let v = u64::from_le_bytes(b.get(*p..*p + 8)?.try_into().ok()?);
        *p += 8;
        Some(v)
    };
    let incarnation = u(&mut p)?;
    let n = match kind {
        K_OPEN | K_UPLOADED => 1,
        K_ENTRY => 2,
        K_ERASE => 0,
        K_FENCE => 3,
        _ => return None,
    };
    let mut f = [0u64; 3];
    for x in f.iter_mut().take(n) {
        *x = u(&mut p)?;
    }
    Some(Rec { kind, key: DbKey { name, incarnation }, f, data: Bytes::copy_from_slice(&b[p..]) })
}

/// Frames of a file up to the first torn or corrupt one. Returns (records, torn).
fn read_frames(path: &Path) -> std::io::Result<(Vec<Rec>, bool)> {
    let mut all = Vec::new();
    File::open(path)?.read_to_end(&mut all)?;
    let (mut out, mut p) = (Vec::new(), 0usize);
    while p < all.len() {
        if p + 8 > all.len() {
            return Ok((out, true));
        }
        let len = u32::from_le_bytes(all[p..p + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(all[p + 4..p + 8].try_into().unwrap());
        if len == 0 || p + 8 + len > all.len() || crc32fast::hash(&all[p + 8..p + 8 + len]) != crc {
            return Ok((out, true));
        }
        match parse_body(&all[p + 8..p + 8 + len]) {
            Some(r) => out.push(r),
            None => return Ok((out, true)),
        }
        p += 8 + len;
    }
    Ok((out, false))
}

fn seg_path(dir: &Path, n: u64) -> PathBuf {
    dir.join(format!("seg-{n:012}.j3z"))
}

fn list_segments(dir: &Path) -> std::io::Result<Vec<u64>> {
    let mut v = Vec::new();
    for e in std::fs::read_dir(dir)? {
        let p = e?.path();
        if let Some(n) = p.file_name().and_then(|f| f.to_str()).and_then(|f| f.strip_prefix("seg-")).and_then(|f| f.strip_suffix(".j3z")).and_then(|f| f.parse().ok()) {
            v.push(n);
        }
    }
    v.sort();
    Ok(v)
}

fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

fn io(e: std::io::Error) -> JournalError {
    JournalError::Io(e.to_string())
}

impl LocalJournal {
    /// Open (or create) the journal directory and rebuild the state of every database from it.
    pub fn open_dir(dir: impl AsRef<Path>, cfg: Config) -> Result<Arc<Self>, JournalError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir).map_err(io)?;
        let id_path = dir.join("JOURNAL_ID");
        let id = match std::fs::read_to_string(&id_path) {
            Ok(s) if !s.trim().is_empty() => s.trim().to_string(),
            _ => {
                let id = format!("local-{:016x}", seed_id());
                let mut f = File::create(&id_path).map_err(io)?;
                f.write_all(id.as_bytes()).map_err(io)?;
                f.sync_all().map_err(io)?;
                sync_dir(&dir).map_err(io)?;
                id
            }
        };
        let mut dbs: HashMap<DbKey, DbState> = HashMap::new();
        let fences = dir.join("fences.j3z");
        if fences.exists() {
            for r in read_frames(&fences).map_err(io)?.0 {
                if r.kind == K_FENCE {
                    let d = dbs.entry(r.key).or_default();
                    d.promised = Epoch(r.f[0]);
                    d.uploaded = r.f[1];
                    d.last = d.last.max(r.f[1]);
                    d.erased = r.f[2] != 0;
                }
            }
        }
        let segs = list_segments(&dir).map_err(io)?;
        let mut closed = BTreeMap::new();
        for &n in &segs {
            let (recs, torn) = read_frames(&seg_path(&dir, n)).map_err(io)?;
            if torn {
                eprintln!("journal3z local: segment {n} ends with a torn record (never acknowledged); ignored");
            }
            let mut map: HashMap<DbKey, u64> = HashMap::new();
            for r in recs {
                let d = dbs.entry(r.key.clone()).or_default();
                match r.kind {
                    K_OPEN => d.promised = d.promised.max(Epoch(r.f[0])),
                    K_ENTRY => {
                        let i = r.f[0];
                        let m = map.entry(r.key.clone()).or_default();
                        *m = (*m).max(i);
                        if !d.erased && i > d.uploaded {
                            d.unflushed_bytes += r.data.len() as u64;
                            if let Some(old) = d.entries.insert(i, r.data) {
                                d.unflushed_bytes -= old.len() as u64;
                            }
                        }
                        d.last = d.last.max(i);
                    }
                    K_UPLOADED => {
                        d.uploaded = d.uploaded.max(r.f[0]);
                        let u = d.uploaded;
                        d.trim(u);
                    }
                    K_ERASE => {
                        d.erased = true;
                        d.entries.clear();
                        d.unflushed_bytes = 0;
                    }
                    _ => {}
                }
            }
            closed.insert(n, map);
        }
        let first = segs.last().map(|n| n + 1).unwrap_or(1);
        let st = Arc::new(Mutex::new(State { dbs, closed, current: (first, HashMap::new()), failed: None }));
        let (tx, rx) = mpsc::channel();
        let me = Arc::new(LocalJournal { dir: dir.clone(), id, cfg, st, tx: Mutex::new(tx), records: Arc::new(AtomicU64::new(0)), fsyncs: Arc::new(AtomicU64::new(0)) });
        let f = OpenOptions::new().create(true).append(true).open(seg_path(&dir, first)).map_err(io)?;
        sync_dir(&dir).map_err(io)?;
        let w = Arc::clone(&me);
        std::thread::Builder::new().name("journal3z-local".into()).spawn(move || w.writer(rx, f, first)).map_err(io)?;
        Ok(me)
    }

    fn send(&self, op: Op) -> Result<(), JournalError> {
        self.tx.lock().unwrap().send(op).map_err(|_| JournalError::Unavailable("journal writer stopped".into()))
    }

    /// The writer thread: append every queued frame, one `fdatasync`, resolve the futures in order.
    fn writer(self: Arc<Self>, rx: mpsc::Receiver<Op>, mut f: File, mut segno: u64) {
        let mut size = 0u64;
        while let Ok(first) = rx.recv() {
            let mut batch = vec![first];
            while let Ok(more) = rx.try_recv() {
                batch.push(more);
            }
            let mut res: Result<(), JournalError> = Ok(());
            let mut buf = Vec::new();
            for op in &batch {
                buf.extend_from_slice(&op.frame);
            }
            if let Err(e) = f.write_all(&buf) {
                res = Err(JournalError::Io(format!("write: {e}")));
            } else if let Err(e) = f.sync_data() {
                res = Err(JournalError::Io(format!("fdatasync: {e}")));
            }
            self.fsyncs.fetch_add(1, Relaxed);
            self.records.fetch_add(batch.len() as u64, Relaxed);
            size += buf.len() as u64;
            {
                let mut st = self.st.lock().unwrap();
                if let Err(e) = &res {
                    st.failed = Some(e.to_string());
                }
                for op in &batch {
                    if let Some((k, i)) = &op.entry {
                        let m = st.current.1.entry(k.clone()).or_default();
                        *m = (*m).max(*i);
                    }
                }
            }
            for op in batch {
                if let Some(d) = op.done {
                    let _ = d.send(res.clone());
                }
            }
            if res.is_err() {
                // fsyncgate: after a failed write or fsync nothing more is acknowledged; drain with errors.
                while let Ok(op) = rx.recv() {
                    if let Some(d) = op.done {
                        let _ = d.send(res.clone());
                    }
                }
                return;
            }
            if size >= self.cfg.segment_max {
                let next = segno + 1;
                match OpenOptions::new().create(true).append(true).open(seg_path(&self.dir, next)).and_then(|nf| sync_dir(&self.dir).map(|_| nf)) {
                    Ok(nf) => {
                        f = nf;
                        let mut st = self.st.lock().unwrap();
                        let old = std::mem::replace(&mut st.current, (next, HashMap::new()));
                        st.closed.insert(old.0, old.1);
                        segno = next;
                        size = 0;
                    }
                    Err(e) => {
                        self.st.lock().unwrap().failed = Some(format!("segment roll: {e}"));
                        return;
                    }
                }
                let _ = self.delete_uploaded_segments();
            }
        }
    }

    /// Delete closed segments whose records are all uploaded or erased. The fence file is rewritten first, because
    /// deleted segments may hold the only `OPEN` record of a database.
    fn delete_uploaded_segments(&self) -> Result<usize, JournalError> {
        let (todo, fences) = {
            let st = self.st.lock().unwrap();
            let todo: Vec<u64> = st
                .closed
                .iter()
                .filter(|(_, m)| m.iter().all(|(k, max)| st.dbs.get(k).map_or(true, |d| d.erased || *max <= d.uploaded)))
                .map(|(n, _)| *n)
                .collect();
            if todo.is_empty() {
                return Ok(0);
            }
            let mut fences = Vec::new();
            for (k, d) in &st.dbs {
                fences.extend(frame(K_FENCE, k, &[d.promised.0, d.uploaded, d.erased as u64], &[]));
            }
            (todo, fences)
        };
        let tmp = self.dir.join("fences.j3z.tmp");
        let mut f = File::create(&tmp).map_err(io)?;
        f.write_all(&fences).map_err(io)?;
        f.sync_all().map_err(io)?;
        std::fs::rename(&tmp, self.dir.join("fences.j3z")).map_err(io)?;
        sync_dir(&self.dir).map_err(io)?;
        let mut st = self.st.lock().unwrap();
        for n in &todo {
            let _ = std::fs::remove_file(seg_path(&self.dir, *n));
            st.closed.remove(n);
        }
        Ok(todo.len())
    }

    /// Stop the writer thread once the queued records are written (a later call gets `Unavailable`). Used to simulate
    /// a process exit in tests: the directory can then be opened again.
    pub fn stop(&self) {
        let (dead, _) = mpsc::channel();
        *self.tx.lock().unwrap() = dead;
    }

    /// Number of segment files currently on disk (tests, monitoring).
    pub fn segments(&self) -> usize {
        list_segments(&self.dir).map(|v| v.len()).unwrap_or(0)
    }

    fn check_failed(st: &State) -> Result<(), JournalError> {
        match &st.failed {
            Some(e) => Err(JournalError::Io(e.clone())),
            None => Ok(()),
        }
    }
}

fn seed_id() -> u64 {
    let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0);
    t ^ (std::process::id() as u64).rotate_left(32) ^ (&t as *const u64 as u64)
}

#[async_trait::async_trait]
impl Journal for LocalJournal {
    fn id(&self) -> &str {
        &self.id
    }

    async fn open(&self, db: &DbKey, epoch: Epoch, applied: Index) -> Result<Opened, JournalError> {
        let (rx, replay, next) = {
            let mut st = self.st.lock().unwrap();
            Self::check_failed(&st)?;
            let d = st.dbs.entry(db.clone()).or_default();
            if d.erased {
                return Err(JournalError::Erased);
            }
            if epoch < d.promised {
                return Err(JournalError::Fenced { promised: d.promised });
            }
            d.promised = epoch;
            d.open = true;
            // `applied` is read from the database's own state in object storage: a fact, so it is a trim point.
            d.uploaded = d.uploaded.max(applied.0);
            let u = d.uploaded;
            d.trim(u);
            let replay: Vec<(Index, Bytes)> = d.entries.iter().map(|(i, b)| (Index(*i), b.clone())).collect();
            let next = Index(d.next());
            let (tx, rx) = Durable::channel();
            self.send(Op { frame: frame(K_OPEN, db, &[epoch.0], &[]), entry: None, done: Some(tx) })?;
            (rx, replay, next)
        };
        rx.await?;
        Ok(Opened { replay, next })
    }

    fn append(&self, db: &DbKey, epoch: Epoch, payload: Bytes) -> Result<Appended, JournalError> {
        let mut st = self.st.lock().unwrap();
        Self::check_failed(&st)?;
        let Some(d) = st.dbs.get_mut(db) else { return Err(JournalError::Invalid("append before open".into())) };
        if d.erased {
            return Err(JournalError::Erased);
        }
        if epoch < d.promised {
            return Err(JournalError::Fenced { promised: d.promised });
        }
        if epoch > d.promised || !d.open {
            return Err(JournalError::Invalid("append before open".into()));
        }
        if d.unflushed_bytes + payload.len() as u64 > self.cfg.db_hard_cap {
            return Err(JournalError::Quota);
        }
        let index = d.next();
        d.last = index;
        d.unflushed_bytes += payload.len() as u64;
        let fr = frame(K_ENTRY, db, &[index, epoch.0], &payload);
        d.entries.insert(index, payload);
        let (tx, durable) = Durable::channel();
        // Sent under the state lock: the writer thread sees the records of a database in index order.
        self.send(Op { frame: fr, entry: Some((db.clone(), index)), done: Some(tx) })?;
        Ok(Appended { index: Index(index), durable })
    }

    async fn uploaded(&self, db: &DbKey, _epoch: Epoch, index: Index) -> Result<(), JournalError> {
        {
            let mut st = self.st.lock().unwrap();
            Self::check_failed(&st)?;
            let d = st.dbs.entry(db.clone()).or_default();
            if index.0 <= d.uploaded {
                return Ok(());
            }
            // A confirmed upload is a fact whatever the caller's epoch: S3's marker only grows.
            d.uploaded = index.0;
            d.trim(index.0);
            // Not waited for: if lost in a crash, the records are replayed once more (idempotent via the marker).
            self.send(Op { frame: frame(K_UPLOADED, db, &[index.0], &[]), entry: None, done: None })?;
        }
        self.delete_uploaded_segments()?;
        Ok(())
    }

    async fn close(&self, db: &DbKey, epoch: Epoch) -> Result<(), JournalError> {
        let mut st = self.st.lock().unwrap();
        let Some(d) = st.dbs.get_mut(db) else { return Ok(()) };
        if epoch < d.promised {
            return Err(JournalError::Fenced { promised: d.promised });
        }
        if d.last > d.uploaded {
            return Err(JournalError::Invalid(format!("close with records {}..={} not uploaded", d.uploaded + 1, d.last)));
        }
        d.open = false;
        Ok(())
    }

    async fn erase(&self, db: &DbKey) -> Result<(), JournalError> {
        let rx = {
            let mut st = self.st.lock().unwrap();
            Self::check_failed(&st)?;
            let d = st.dbs.entry(db.clone()).or_default();
            d.erased = true;
            d.open = false;
            d.entries.clear();
            d.unflushed_bytes = 0;
            let (tx, rx) = Durable::channel();
            self.send(Op { frame: frame(K_ERASE, db, &[], &[]), entry: None, done: Some(tx) })?;
            rx
        };
        rx.await?;
        self.delete_uploaded_segments()?;
        Ok(())
    }

    async fn pending(&self) -> Result<Vec<(DbKey, Index)>, JournalError> {
        let st = self.st.lock().unwrap();
        let mut v: Vec<(DbKey, Index)> = st.dbs.iter().filter(|(_, d)| !d.erased && d.last > d.uploaded).map(|(k, d)| (k.clone(), Index(d.last))).collect();
        v.sort();
        Ok(v)
    }
}
