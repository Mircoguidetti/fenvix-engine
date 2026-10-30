//! Disk format of a journal3z node (design §6.3, phase-2 plan P1/P2).
//!
//! Directory of one node:
//! - `NODE_ID` — the node's identity (random, non-zero `u32`), created with the directory. A node whose directory is
//!   lost comes back with a **new** identity automatically (design §5.8: a node that lost its disk must never answer
//!   under its old identity — it would have forgotten its promises).
//! - `seg-NNNNNNNNNNNN.j3z` — segments: CRC-framed records (`j3z-codec`), appended, one `fdatasync` per batch (done by
//!   the caller with [`Store::sync`]). Only the last segment can end with a torn record (a crash during a write).
//! - `fences.j3z` — the fence table: per database `{promised, trim, erased}`, rewritten atomically (tmp + fsync + rename
//!   + directory fsync) **before** any segment is deleted, because a deleted segment may hold the only PROMISE record of
//!   a database (design §5.9: a node never forgets a fence).
//!
//! Recovery ([`Store::open`]) = fence table, then every segment in order, through the same `NodeCore::apply` the node
//! uses live; so the recovered state is the state that was durable. Segments are deleted only as a **prefix** (oldest
//! first) and only when every entry in them is at or below its database's trim point (in S3) or its database was
//! erased; a TRUNCATE record in a later segment therefore never outlives the entries it cut.
//!
//! I/O goes through [`Fs`], so tests (and the test bench) can inject fsync errors, a full disk and slow fsyncs
//! ([`FaultFs`]). After any write or fsync error the store is **poisoned**: every later call fails, and the node must
//! exit and recover from disk (fsyncgate: after an error the page cache can no longer be trusted).

use j3z_codec::{decode_rec, encode_rec, frame, read_frame, FrameRead};
use journal3z_core::{DbId, Epoch, Idx, Mutations, NodeCore, NodeId, Rec};
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::Arc;

// ======================================================================================================== Fs layer

pub trait FileW: Send {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()>;
    fn sync_data(&mut self) -> io::Result<()>;
}

pub trait Fs: Send + Sync {
    fn create_dir_all(&self, p: &Path) -> io::Result<()>;
    fn list(&self, dir: &Path) -> io::Result<Vec<String>>;
    fn read(&self, p: &Path) -> io::Result<Vec<u8>>;
    /// Open for appending (created if absent).
    fn append(&self, p: &Path) -> io::Result<Box<dyn FileW>>;
    /// Write a whole small file durably (create/truncate, write, fsync).
    fn write_file_sync(&self, p: &Path, data: &[u8]) -> io::Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn remove(&self, p: &Path) -> io::Result<()>;
    fn truncate(&self, p: &Path, len: u64) -> io::Result<()>;
    fn sync_dir(&self, p: &Path) -> io::Result<()>;
}

/// The real file system.
pub struct RealFs;

struct RealFile(std::fs::File);
impl FileW for RealFile {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        io::Write::write_all(&mut self.0, buf)
    }
    fn sync_data(&mut self) -> io::Result<()> {
        self.0.sync_data()
    }
}

impl Fs for RealFs {
    fn create_dir_all(&self, p: &Path) -> io::Result<()> {
        std::fs::create_dir_all(p)
    }
    fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
        let mut v = vec![];
        for e in std::fs::read_dir(dir)? {
            if let Some(n) = e?.file_name().to_str() {
                v.push(n.to_string());
            }
        }
        Ok(v)
    }
    fn read(&self, p: &Path) -> io::Result<Vec<u8>> {
        std::fs::read(p)
    }
    fn append(&self, p: &Path) -> io::Result<Box<dyn FileW>> {
        Ok(Box::new(RealFile(std::fs::OpenOptions::new().create(true).append(true).open(p)?)))
    }
    fn write_file_sync(&self, p: &Path, data: &[u8]) -> io::Result<()> {
        let mut f = std::fs::File::create(p)?;
        io::Write::write_all(&mut f, data)?;
        f.sync_all()
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }
    fn remove(&self, p: &Path) -> io::Result<()> {
        std::fs::remove_file(p)
    }
    fn truncate(&self, p: &Path, len: u64) -> io::Result<()> {
        let f = std::fs::OpenOptions::new().write(true).open(p)?;
        f.set_len(len)?;
        f.sync_all()
    }
    fn sync_dir(&self, p: &Path) -> io::Result<()> {
        std::fs::File::open(p)?.sync_all()
    }
}

/// Fault switches shared with whoever injects faults (tests, the node's control port).
#[derive(Default, Debug)]
pub struct Faults {
    /// The next `sync_data` fails (EIO), once.
    pub fail_next_sync: AtomicBool,
    /// While set, every write and sync fails with ENOSPC.
    pub full: AtomicBool,
    /// Extra delay before every `sync_data`, in microseconds.
    pub slow_sync_us: AtomicU64,
    /// Counters (for tests): syncs done, syncs failed.
    pub syncs: AtomicU64,
    pub sync_errors: AtomicU64,
}

/// [`RealFs`] with injectable faults.
pub struct FaultFs {
    pub faults: Arc<Faults>,
}

struct FaultFile {
    f: RealFile,
    faults: Arc<Faults>,
    path: PathBuf,
    /// File length at the last successful fsync.
    synced_len: u64,
}

fn enospc() -> io::Error {
    io::Error::from_raw_os_error(28)
}

impl FileW for FaultFile {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        if self.faults.full.load(SeqCst) {
            // a full disk may accept part of a write: write half, then fail
            let _ = self.f.write_all(&buf[..buf.len() / 2]);
            return Err(enospc());
        }
        self.f.write_all(buf)
    }
    fn sync_data(&mut self) -> io::Result<()> {
        let us = self.faults.slow_sync_us.load(SeqCst);
        if us > 0 {
            std::thread::sleep(std::time::Duration::from_micros(us));
        }
        if self.faults.full.load(SeqCst) {
            self.faults.sync_errors.fetch_add(1, SeqCst);
            return Err(enospc());
        }
        if self.faults.fail_next_sync.swap(false, SeqCst) {
            self.faults.sync_errors.fetch_add(1, SeqCst);
            // like a failed write-back on Linux: the unsynced data is dropped (the page cache marks it clean); a
            // node that ignored the error would acknowledge data that is no longer anywhere
            if let Ok(f) = std::fs::OpenOptions::new().write(true).open(&self.path) {
                let _ = f.set_len(self.synced_len);
                let _ = f.sync_all();
            }
            return Err(io::Error::from_raw_os_error(5)); // EIO
        }
        self.faults.syncs.fetch_add(1, SeqCst);
        self.f.sync_data()?;
        self.synced_len = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(self.synced_len);
        Ok(())
    }
}

impl Fs for FaultFs {
    fn create_dir_all(&self, p: &Path) -> io::Result<()> {
        RealFs.create_dir_all(p)
    }
    fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
        RealFs.list(dir)
    }
    fn read(&self, p: &Path) -> io::Result<Vec<u8>> {
        RealFs.read(p)
    }
    fn append(&self, p: &Path) -> io::Result<Box<dyn FileW>> {
        let f = std::fs::OpenOptions::new().create(true).append(true).open(p)?;
        let synced_len = f.metadata()?.len();
        Ok(Box::new(FaultFile { f: RealFile(f), faults: self.faults.clone(), path: p.to_path_buf(), synced_len }))
    }
    fn write_file_sync(&self, p: &Path, data: &[u8]) -> io::Result<()> {
        if self.faults.full.load(SeqCst) {
            return Err(enospc());
        }
        RealFs.write_file_sync(p, data)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        RealFs.rename(from, to)
    }
    fn remove(&self, p: &Path) -> io::Result<()> {
        RealFs.remove(p)
    }
    fn truncate(&self, p: &Path, len: u64) -> io::Result<()> {
        RealFs.truncate(p, len)
    }
    fn sync_dir(&self, p: &Path) -> io::Result<()> {
        RealFs.sync_dir(p)
    }
}

// ======================================================================================================== store

#[derive(Clone, Debug)]
pub struct StoreCfg {
    /// A segment is closed at the next `gc` once it reaches this size.
    pub segment_max: u64,
    /// Protocol bug switches for the negative controls (phase-2 plan §2b). All false in production; the node binary
    /// only lets them be set when built with the `mutations` feature.
    pub m: Mutations,
    /// Store-level bugs used only by this crate's tests, to show each test fails on broken code.
    #[cfg(any(test, feature = "testbugs"))]
    pub bugs: TestBugs,
}

#[cfg(any(test, feature = "testbugs"))]
#[derive(Clone, Debug, Default)]
pub struct TestBugs {
    pub no_fence_table: bool,
    pub no_tail_truncate: bool,
    pub gc_not_prefix: bool,
    pub accept_mid_corruption: bool,
    pub replay_erased: bool,
}

impl Default for StoreCfg {
    fn default() -> Self {
        StoreCfg {
            segment_max: 64 << 20,
            m: Mutations::default(),
            #[cfg(any(test, feature = "testbugs"))]
            bugs: TestBugs::default(),
        }
    }
}

#[derive(Debug)]
pub enum StoreError {
    Io(io::Error),
    /// A previous write or fsync failed: nothing may be acknowledged any more; the process must exit.
    Poisoned(String),
    /// Data that cannot come from a crash (bad record in the middle of history, a gap in a log, an unreadable
    /// node identity): the node refuses to start. Treat the node as lost (new directory, new identity).
    Corrupt(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Io(e) => write!(f, "io: {e}"),
            StoreError::Poisoned(s) => write!(f, "poisoned: {s}"),
            StoreError::Corrupt(s) => write!(f, "corrupt: {s}"),
        }
    }
}
impl std::error::Error for StoreError {}
impl From<io::Error> for StoreError {
    fn from(e: io::Error) -> Self {
        StoreError::Io(e)
    }
}

/// What recovery found (for logs and tests).
#[derive(Debug, Default, Clone)]
pub struct RecoveryReport {
    pub segments: usize,
    pub records: u64,
    pub torn_tail_bytes: u64,
    pub fences: usize,
    pub new_identity: bool,
}

/// Per segment: highest APPEND index per database (to decide when the segment is dead).
#[derive(Debug, Default, Clone)]
struct SegInfo {
    max_idx: BTreeMap<DbId, Idx>,
}

pub struct Store {
    dir: PathBuf,
    fs: Arc<dyn Fs>,
    cfg: StoreCfg,
    pub node_id: NodeId,
    cur: u64,
    file: Box<dyn FileW>,
    cur_size: u64,
    segs: BTreeMap<u64, SegInfo>,
    poisoned: Option<String>,
    unsynced: u64,
    pub records_written: u64,
    pub syncs: u64,
    pub segments_deleted: u64,
}

const FENCE_KIND: u8 = 0xF0;

fn seg_name(n: u64) -> String {
    format!("seg-{n:012}.j3z")
}

fn parse_seg(name: &str) -> Option<u64> {
    name.strip_prefix("seg-")?.strip_suffix(".j3z")?.parse().ok()
}

fn encode_fence(db: DbId, promised: Epoch, trim: Idx, erased: bool) -> Vec<u8> {
    let mut v = vec![FENCE_KIND];
    v.extend_from_slice(&db.to_le_bytes());
    v.extend_from_slice(&promised.to_le_bytes());
    v.extend_from_slice(&trim.to_le_bytes());
    v.push(erased as u8);
    frame(&v)
}

fn decode_fence(b: &[u8]) -> Option<(DbId, Epoch, Idx, bool)> {
    if b.len() != 26 || b[0] != FENCE_KIND {
        return None;
    }
    let u = |i: usize| u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
    Some((u(1), u(9), u(17), b[25] != 0))
}

/// Scan frames: (records, bytes of valid frames, Some(reason) if the data after them is not a clean torn tail).
fn scan(buf: &[u8]) -> (Vec<Rec>, usize, Option<String>) {
    let (mut recs, mut p) = (vec![], 0usize);
    while p < buf.len() {
        match read_frame(&buf[p..]) {
            FrameRead::Ok(body, used) => match decode_rec(body) {
                Ok(r) => {
                    recs.push(r);
                    p += used;
                }
                Err(e) => return (recs, p, Some(format!("undecodable record at byte {p}: {e}"))),
            },
            FrameRead::Incomplete | FrameRead::Corrupt => {
                // a torn tail has no valid frame after it; a valid frame further on means corruption in the middle
                for q in (p + 1)..buf.len() {
                    if let FrameRead::Ok(body, _) = read_frame(&buf[q..]) {
                        if decode_rec(body).is_ok() {
                            return (recs, p, Some(format!("bad frame at byte {p} followed by a valid one at byte {q}")));
                        }
                    }
                }
                return (recs, p, None);
            }
        }
    }
    (recs, p, None)
}

impl Store {
    /// Open (or create) a node directory and rebuild the node's durable state. New records go to a fresh segment.
    pub fn open(dir: impl AsRef<Path>, fs: Arc<dyn Fs>, cfg: StoreCfg) -> Result<(Store, NodeCore, RecoveryReport), StoreError> {
        let dir = dir.as_ref().to_path_buf();
        fs.create_dir_all(&dir)?;
        let mut rep = RecoveryReport::default();
        let names = fs.list(&dir)?;
        let mut segnums: Vec<u64> = names.iter().filter_map(|n| parse_seg(n)).collect();
        segnums.sort();
        let id_path = dir.join("NODE_ID");
        let node_id: NodeId = if names.iter().any(|n| n == "NODE_ID") {
            let s = String::from_utf8(fs.read(&id_path)?).map_err(|_| StoreError::Corrupt("NODE_ID not utf-8".into()))?;
            s.trim().parse().ok().filter(|x| *x != 0).ok_or_else(|| StoreError::Corrupt(format!("bad NODE_ID {s:?}")))?
        } else {
            if !segnums.is_empty() || names.iter().any(|n| n == "fences.j3z") {
                return Err(StoreError::Corrupt("segments without NODE_ID".into()));
            }
            let id = loop {
                let x: u32 = rand::random();
                if x != 0 {
                    break x;
                }
            };
            let tmp = dir.join("NODE_ID.tmp");
            fs.write_file_sync(&tmp, id.to_string().as_bytes())?;
            fs.rename(&tmp, &id_path)?;
            fs.sync_dir(&dir)?;
            rep.new_identity = true;
            id
        };
        let mut core = NodeCore::new(node_id, cfg.m);
        // databases the fence table records as erased: their records still in segments all precede the erase (an
        // erased database accepts nothing) and must not be replayed: their earlier segments may already be deleted
        let mut erased_base = std::collections::BTreeSet::new();
        // fence table first (the base), then every segment in order
        if names.iter().any(|n| n == "fences.j3z") {
            let buf = fs.read(&dir.join("fences.j3z"))?;
            let mut p = 0;
            while p < buf.len() {
                let FrameRead::Ok(body, used) = read_frame(&buf[p..]) else {
                    return Err(StoreError::Corrupt("fence table: bad frame".into()));
                };
                let (db, promised, trim, erased) = decode_fence(body).ok_or_else(|| StoreError::Corrupt("fence table: bad record".into()))?;
                let d = core.dbs.entry(db).or_default();
                d.promised = promised;
                d.trim = trim;
                d.erased = erased;
                if erased {
                    erased_base.insert(db);
                }
                rep.fences += 1;
                p += used;
            }
        }
        let mut segs = BTreeMap::new();
        for (k, &n) in segnums.iter().enumerate() {
            let path = dir.join(seg_name(n));
            let buf = fs.read(&path)?;
            let (recs, good, bad) = scan(&buf);
            let last = k + 1 == segnums.len();
            #[cfg(any(test, feature = "testbugs"))]
            let bad = if cfg.bugs.accept_mid_corruption { None } else { bad };
            if let Some(why) = bad {
                return Err(StoreError::Corrupt(format!("{}: {why}", seg_name(n))));
            }
            #[cfg(any(test, feature = "testbugs"))]
            let skip_trunc = cfg.bugs.no_tail_truncate;
            #[cfg(not(any(test, feature = "testbugs")))]
            let skip_trunc = false;
            if good < buf.len() {
                if !last {
                    return Err(StoreError::Corrupt(format!("{}: torn record in a segment that is not the last", seg_name(n))));
                }
                rep.torn_tail_bytes = (buf.len() - good) as u64;
                if !skip_trunc {
                    fs.truncate(&path, good as u64)?;
                }
            }
            let mut info = SegInfo::default();
            for r in &recs {
                if let Rec::Append { db, e } = r {
                    let m = info.max_idx.entry(*db).or_insert(0);
                    *m = (*m).max(e.idx);
                }
                #[cfg(any(test, feature = "testbugs"))]
                let skip_erased = !cfg.bugs.replay_erased;
                #[cfg(not(any(test, feature = "testbugs")))]
                let skip_erased = true;
                if skip_erased && erased_base.contains(&r.db()) {
                    continue;
                }
                core.apply(r).map_err(|e| StoreError::Corrupt(format!("{}: {e}", seg_name(n))))?;
            }
            rep.records += recs.len() as u64;
            segs.insert(n, info);
        }
        rep.segments = segnums.len();
        let cur = segnums.last().map(|n| n + 1).unwrap_or(1);
        let file = fs.append(&dir.join(seg_name(cur)))?;
        fs.sync_dir(&dir)?;
        segs.insert(cur, SegInfo::default());
        Ok((Store { dir, fs, cfg, node_id, cur, file, cur_size: 0, segs, poisoned: None, unsynced: 0, records_written: 0, syncs: 0, segments_deleted: 0 }, core, rep))
    }

    fn check(&self) -> Result<(), StoreError> {
        match &self.poisoned {
            Some(s) => Err(StoreError::Poisoned(s.clone())),
            None => Ok(()),
        }
    }

    fn poison(&mut self, what: &str, e: io::Error) -> StoreError {
        let s = format!("{what}: {e}");
        self.poisoned = Some(s.clone());
        StoreError::Poisoned(s)
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.is_some()
    }

    /// Append records to the current segment (not durable until [`Store::sync`]).
    pub fn write(&mut self, recs: &[Rec]) -> Result<(), StoreError> {
        self.check()?;
        if recs.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::with_capacity(recs.len() * 64);
        let info = self.segs.entry(self.cur).or_default();
        for r in recs {
            buf.extend_from_slice(&frame(&encode_rec(r)));
            if let Rec::Append { db, e } = r {
                let m = info.max_idx.entry(*db).or_insert(0);
                *m = (*m).max(e.idx);
            }
        }
        if let Err(e) = self.file.write_all(&buf) {
            return Err(self.poison("write", e));
        }
        self.cur_size += buf.len() as u64;
        self.unsynced += recs.len() as u64;
        self.records_written += recs.len() as u64;
        Ok(())
    }

    /// `fdatasync` of everything written. On error the store is poisoned (the caller must exit).
    pub fn sync(&mut self) -> Result<(), StoreError> {
        self.check()?;
        if let Err(e) = self.file.sync_data() {
            if self.cfg.m.ignore_fsync_error {
                // BUG (negative control): carry on as if the data were durable
                self.unsynced = 0;
                return Ok(());
            }
            return Err(self.poison("fdatasync", e));
        }
        self.syncs += 1;
        self.unsynced = 0;
        Ok(())
    }

    pub fn has_unsynced(&self) -> bool {
        self.unsynced > 0
    }

    /// Roll the segment if it is full, then delete the dead prefix of closed segments. Call only right after
    /// [`Store::sync`] (nothing unsynced), with the node's current state: the fence table written here then equals the
    /// durable state. Returns the number of segments deleted.
    pub fn gc(&mut self, core: &NodeCore) -> Result<usize, StoreError> {
        self.check()?;
        if self.unsynced > 0 {
            return Ok(0);
        }
        if self.cur_size >= self.cfg.segment_max {
            let next = self.cur + 1;
            let f = match self.fs.append(&self.dir.join(seg_name(next))) {
                Ok(f) => f,
                Err(e) => return Err(self.poison("open segment", e)),
            };
            if let Err(e) = self.fs.sync_dir(&self.dir) {
                return Err(self.poison("sync dir", e));
            }
            self.file = f;
            self.cur = next;
            self.cur_size = 0;
            self.segs.insert(next, SegInfo::default());
        }
        // the dead prefix of closed segments
        let mut dead = vec![];
        for (&n, info) in &self.segs {
            if n == self.cur {
                break;
            }
            let all_dead = info.max_idx.iter().all(|(db, max)| core.dbs.get(db).map_or(false, |d| d.erased || *max <= d.trim));
            #[cfg(any(test, feature = "testbugs"))]
            if !all_dead && self.cfg.bugs.gc_not_prefix {
                continue;
            }
            if !all_dead {
                break;
            }
            dead.push(n);
        }
        if dead.is_empty() {
            return Ok(0);
        }
        #[cfg(any(test, feature = "testbugs"))]
        let skip = self.cfg.bugs.no_fence_table;
        #[cfg(not(any(test, feature = "testbugs")))]
        let skip = false;
        if !skip {
            self.write_fences(core)?;
        }
        for n in &dead {
            if let Err(e) = self.fs.remove(&self.dir.join(seg_name(*n))) {
                return Err(self.poison("remove segment", e));
            }
            self.segs.remove(n);
        }
        if let Err(e) = self.fs.sync_dir(&self.dir) {
            return Err(self.poison("sync dir", e));
        }
        self.segments_deleted += dead.len() as u64;
        Ok(dead.len())
    }

    fn write_fences(&mut self, core: &NodeCore) -> Result<(), StoreError> {
        let mut buf = vec![];
        for (db, d) in &core.dbs {
            buf.extend_from_slice(&encode_fence(*db, d.promised, d.trim, d.erased));
        }
        let tmp = self.dir.join("fences.j3z.tmp");
        let r = self.fs.write_file_sync(&tmp, &buf).and_then(|_| self.fs.rename(&tmp, &self.dir.join("fences.j3z"))).and_then(|_| self.fs.sync_dir(&self.dir));
        if let Err(e) = r {
            return Err(self.poison("fence table", e));
        }
        Ok(())
    }

    /// Segment files currently on disk (tests, metrics).
    pub fn segment_count(&self) -> usize {
        self.segs.len()
    }

    /// Path of the segment being written (tests).
    pub fn current_segment(&self) -> PathBuf {
        self.dir.join(seg_name(self.cur))
    }
}

/// Deterministic generator of realistic record sequences, for the recovery tests (P2) and the kill -9 child: it drives
/// a real `NodeCore` with messages (fences with growing epochs, appends that extend a log, appends from a newer epoch
/// that cut a diverged tail, trims, erases) and returns the records the node produced, batch by batch. The same seed
/// always gives the same sequence.
#[cfg(any(test, feature = "testbugs"))]
pub mod testgen {
    use bytes_free::*;
    use journal3z_core::{Entry, Msg, Mutations, NodeCore, Rec};
    use rand::{Rng, SeedableRng};

    mod bytes_free {
        pub fn data(i: u64, e: u64) -> Vec<u8> {
            let mut v = i.to_le_bytes().to_vec();
            v.extend_from_slice(&e.to_le_bytes());
            v.extend(std::iter::repeat((i ^ e) as u8).take(40));
            v
        }
    }

    pub struct Gen {
        rng: rand::rngs::StdRng,
        pub core: NodeCore,
        dbs: u64,
        epoch: u64,
    }

    impl Gen {
        pub fn new(seed: u64) -> Self {
            Gen { rng: rand::rngs::StdRng::seed_from_u64(seed), core: NodeCore::new(1, Mutations::default()), dbs: 3, epoch: 1 }
        }

        fn msg(&mut self) -> Msg {
            let db = self.rng.gen_range(1..=self.dbs);
            let d = self.core.dbs.get(&db).cloned().unwrap_or_default();
            let entry = |i: u64, e: u64| Entry { idx: i, epoch: e, barrier: false, data: data(i, e).into() };
            match self.rng.gen_range(0..100) {
                0..=9 => {
                    self.epoch += 1;
                    let applied = if self.rng.gen_bool(0.3) { self.rng.gen_range(d.trim..=d.last() + 2) } else { d.trim };
                    Msg::Fence { db, epoch: self.epoch, applied }
                }
                10..=69 => {
                    // extend the log at the current promise
                    let e = d.promised.max(1);
                    let n = self.rng.gen_range(1..6);
                    let prev = d.last();
                    let pe = d.get(prev).map_or(0, |x| x.epoch);
                    Msg::Append { db, epoch: e, applied: d.trim, prev_idx: prev, prev_epoch: pe, entries: (1..=n).map(|k| entry(prev + k, e)).collect(), commit: 0 }
                }
                70..=84 => {
                    // a newer writer overwrites part of the tail (Truncate + Append)
                    if d.last() <= d.trim + 1 {
                        return Msg::Trim { db, epoch: 0, upto: d.trim };
                    }
                    self.epoch += 1;
                    let prev = self.rng.gen_range(d.trim..d.last());
                    let pe = d.get(prev).map_or(0, |x| x.epoch);
                    let n = self.rng.gen_range(1..4);
                    Msg::Append { db, epoch: self.epoch, applied: d.trim, prev_idx: prev, prev_epoch: pe, entries: (1..=n).map(|k| entry(prev + k, self.epoch)).collect(), commit: 0 }
                }
                85..=97 => {
                    let upto = self.rng.gen_range(d.trim..=d.last() + 1);
                    Msg::Trim { db, epoch: 0, upto }
                }
                _ => {
                    // erase is rare and permanent: use a fresh database id afterwards
                    self.dbs += 1;
                    Msg::Erase { db }
                }
            }
        }

        /// Handle 1..6 messages; the records produced, in order.
        pub fn next_batch(&mut self) -> Vec<Rec> {
            let mut out = vec![];
            for _ in 0..self.rng.gen_range(1..=6) {
                let m = self.msg();
                let o = self.core.handle(1000, m);
                assert!(o.broken.is_none(), "{:?}", o.broken);
                out.extend(o.recs);
            }
            out
        }

        pub fn rng(&mut self) -> &mut rand::rngs::StdRng {
            &mut self.rng
        }
    }
}

#[cfg(test)]
mod tests;
