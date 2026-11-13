//! The disk floor of our runs' blocks (P-big-speed fix 3: the customer's box is kept "in memory, then on local disk").
//!
//! The runs' block cache (`counter.rs`) is RAM, sized by the memory budget. Under it, this floor keeps the stored
//! (compressed, checksummed) bytes of every run block fetched from the object store, in segment files on a local disk,
//! up to `--box-disk-mb`. A block the RAM no longer holds is read back from here before any GET; its bytes go through
//! the same decode and checks as bytes from the store (`runfmt::decode_checked`), so a damaged file is an error caught
//! there, never a wrong answer (the read then falls back to the store).
//!
//! Exactness: a run is immutable and named by an id that is never reused, so the bytes of (run, block) never change.
//!
//! Deletion (guarantee 6: a deletion also clears caches and disks): the floor records, per segment, every copy it wrote.
//! When runs leave the version (`forget_runs`) or a database is dropped or erased (`forget_db`), every copy of their
//! blocks is overwritten with zeros in place and forgotten. A segment is deleted whole when the floor is full (oldest
//! first), and the directory is emptied at every start (no persistence: a restarted process starts with an empty floor).
//!
//! Retired runs (review of big-speed, 2026-11-13, error 4): a hold on an older version may still read a run after the
//! new version retired it and `forget_runs` wiped it. Before, that read wrote the run's blocks back into the floor and
//! nothing wiped them again (they stayed until their segment rolled out or the process restarted). Now `forget_runs`
//! also records the retired ids as tombstones (process-wide, kept even while no floor is installed), `put` never keeps
//! a block of a tombstoned run, and the run's deletion (`run_deleted`, called by `erase::delete_run`, the only path that
//! deletes runs) wipes again whatever is left and drops the tombstone. The tombstone set is bounded (`MAX_TOMBSTONES`,
//! oldest out first); a run that leaves it is long retired, and its deletion still wipes it.
//!
//! Disabled floor (`--box-disk false`, or an automatic size of 0): `install` with a cap of 0 still empties the folder,
//! so segments of an earlier process are not left on the disk.

use crate::runfmt::RUN_ID_LEN;
use std::collections::{HashMap, HashSet, VecDeque};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;

/// One segment file's size.
const SEGMENT_BYTES: u64 = 64 << 20;

pub static WRITES: AtomicU64 = AtomicU64::new(0);
pub static WRITE_BYTES: AtomicU64 = AtomicU64::new(0);
pub static HITS: AtomicU64 = AtomicU64::new(0);
pub static HIT_BYTES: AtomicU64 = AtomicU64::new(0);
pub static ZEROED: AtomicU64 = AtomicU64::new(0);
pub static SEGMENTS_DROPPED: AtomicU64 = AtomicU64::new(0);
pub static ERRORS: AtomicU64 = AtomicU64::new(0);
/// Blocks of retired runs not kept (`put` refused by a tombstone).
pub static TOMBSTONE_SKIPS: AtomicU64 = AtomicU64::new(0);

/// Most retired run ids remembered (16 bytes each: 4 MiB).
pub const MAX_TOMBSTONES: usize = 262_144;

#[derive(Default)]
struct Tombstones {
    set: HashSet<[u8; RUN_ID_LEN]>,
    order: VecDeque<[u8; RUN_ID_LEN]>,
}

static TOMBSTONES: std::sync::LazyLock<Mutex<Tombstones>> = std::sync::LazyLock::new(Default::default);

fn tombstone(ids: &HashSet<[u8; RUN_ID_LEN]>) {
    let mut t = TOMBSTONES.lock().unwrap();
    for id in ids {
        if t.set.insert(*id) {
            t.order.push_back(*id);
        }
    }
    while t.order.len() > MAX_TOMBSTONES {
        if let Some(old) = t.order.pop_front() {
            t.set.remove(&old);
        }
    }
}

fn is_tombstoned(id: &[u8; RUN_ID_LEN]) -> bool {
    TOMBSTONES.lock().unwrap().set.contains(id)
}

/// Tombstones held now (tests, serverStatus).
pub fn tombstones() -> usize {
    TOMBSTONES.lock().unwrap().set.len()
}

type Key = ([u8; RUN_ID_LEN], u32);

struct Copy {
    key: Key,
    db: String,
    path: std::sync::Arc<str>,
    off: u64,
    len: u32,
}

struct Segment {
    seq: u64,
    file: std::fs::File,
    path: PathBuf,
    used: u64,
    copies: Vec<Copy>,
}

struct Floor {
    dir: PathBuf,
    cap: u64,
    segs: VecDeque<Segment>,
    next_seq: u64,
    /// (run, block) -> (segment seq, offset, length) of the copy that is read
    index: HashMap<Key, (u64, u64, u32)>,
    used: u64,
}

static FLOOR: Mutex<Option<Floor>> = Mutex::new(None);

/// Open the floor in `dir` (emptied now) with at most `cap` bytes. `cap` 0: no floor.
pub fn install(dir: PathBuf, cap: u64) -> std::io::Result<()> {
    if cap == 0 {
        *FLOOR.lock().unwrap() = None;
        // review of big-speed (note under section 2): a disabled floor still empties the folder an earlier process used
        if dir.exists() {
            for e in std::fs::read_dir(&dir)? {
                let p = e?.path();
                if p.extension().is_some_and(|x| x == "seg") {
                    std::fs::remove_file(p)?;
                }
            }
        }
        return Ok(());
    }
    if dir.exists() {
        for e in std::fs::read_dir(&dir)? {
            let p = e?.path();
            if p.extension().is_some_and(|x| x == "seg") {
                std::fs::remove_file(p)?;
            }
        }
    } else {
        std::fs::create_dir_all(&dir)?;
    }
    *FLOOR.lock().unwrap() = Some(Floor { dir, cap: cap.max(SEGMENT_BYTES), segs: VecDeque::new(), next_seq: 0, index: HashMap::new(), used: 0 });
    Ok(())
}

pub fn enabled() -> bool {
    FLOOR.lock().unwrap().is_some()
}

/// The database of a run path (`tenants/{db}/...`).
pub fn db_of(path: &str) -> String {
    path.strip_prefix("tenants/").and_then(|r| r.split('/').next()).unwrap_or("").to_string()
}

impl Floor {
    fn new_segment(&mut self) -> std::io::Result<()> {
        let seq = self.next_seq;
        self.next_seq += 1;
        let path = self.dir.join(format!("{seq:010}.seg"));
        let file = std::fs::OpenOptions::new().create(true).read(true).write(true).truncate(true).open(&path)?;
        self.segs.push_back(Segment { seq, file, path, used: 0, copies: Vec::new() });
        Ok(())
    }

    fn drop_oldest(&mut self) {
        if let Some(s) = self.segs.pop_front() {
            for c in &s.copies {
                if self.index.get(&c.key).is_some_and(|(seq, _, _)| *seq == s.seq) {
                    self.index.remove(&c.key);
                }
            }
            self.used -= s.used;
            let _ = std::fs::remove_file(&s.path);
            SEGMENTS_DROPPED.fetch_add(1, Relaxed);
        }
    }

    fn put(&mut self, key: Key, db: &str, path: &str, bytes: &[u8]) -> std::io::Result<()> {
        if self.index.contains_key(&key) {
            return Ok(());
        }
        let len = bytes.len() as u64;
        if self.segs.back().map_or(true, |s| s.used + len > SEGMENT_BYTES) {
            while self.used + SEGMENT_BYTES > self.cap && !self.segs.is_empty() {
                self.drop_oldest();
            }
            self.new_segment()?;
        }
        let s = self.segs.back_mut().unwrap();
        let off = s.used;
        s.file.write_all_at(bytes, off)?;
        s.used += len;
        s.copies.push(Copy { key, db: db.to_string(), path: path.into(), off, len: len as u32 });
        let seq = s.seq;
        self.used += len;
        self.index.insert(key, (seq, off, len as u32));
        WRITES.fetch_add(1, Relaxed);
        WRITE_BYTES.fetch_add(len, Relaxed);
        Ok(())
    }

    fn get(&self, key: &Key) -> Option<Vec<u8>> {
        let (seq, off, len) = *self.index.get(key)?;
        let s = self.segs.iter().find(|s| s.seq == seq)?;
        let mut buf = vec![0u8; len as usize];
        match s.file.read_exact_at(&mut buf, off) {
            Ok(()) => Some(buf),
            Err(_) => {
                ERRORS.fetch_add(1, Relaxed);
                None
            }
        }
    }

    /// Overwrite with zeros, and forget, every copy for which `gone` is true.
    fn wipe(&mut self, gone: impl Fn(&Copy) -> bool) {
        for s in self.segs.iter_mut() {
            let mut keep = Vec::with_capacity(s.copies.len());
            for c in s.copies.drain(..) {
                if gone(&c) {
                    let _ = s.file.write_all_at(&vec![0u8; c.len as usize], c.off);
                    ZEROED.fetch_add(1, Relaxed);
                    if self.index.get(&c.key).is_some_and(|(seq, off, _)| *seq == s.seq && *off == c.off) {
                        self.index.remove(&c.key);
                    }
                } else {
                    keep.push(c);
                }
            }
            s.copies = keep;
            let _ = s.file.sync_data();
        }
    }
}

/// Keep the stored bytes of block `b` of run `run` (a database's run at `path`).
pub fn put(run: &[u8; RUN_ID_LEN], b: u32, path: &str, bytes: &[u8]) {
    let mut g = FLOOR.lock().unwrap();
    let Some(f) = g.as_mut() else { return };
    // a retired run is never kept again (a hold on an older version may still read it)
    if is_tombstoned(run) {
        TOMBSTONE_SKIPS.fetch_add(1, Relaxed);
        return;
    }
    if f.put((*run, b), &db_of(path), path, bytes).is_err() {
        ERRORS.fetch_add(1, Relaxed);
    }
}

/// The stored bytes of block `b` of run `run`, when the floor holds them.
pub fn get(run: &[u8; RUN_ID_LEN], b: u32) -> Option<Vec<u8>> {
    let g = FLOOR.lock().unwrap();
    let r = g.as_ref()?.get(&(*run, b));
    if let Some(v) = &r {
        HITS.fetch_add(1, Relaxed);
        HIT_BYTES.fetch_add(v.len() as u64, Relaxed);
    }
    r
}

/// Runs that left the version: their copies zeroed and forgotten.
pub fn forget_runs(ids: &HashSet<[u8; RUN_ID_LEN]>) {
    tombstone(ids);
    let mut g = FLOOR.lock().unwrap();
    if let Some(f) = g.as_mut() {
        f.wipe(|c| ids.contains(&c.key.0));
    }
}

/// The run at `path` was deleted from the store (`erase::delete_run`): every copy left of it is zeroed and forgotten,
/// and its tombstone, if any, is no longer needed (no read can fetch a deleted run).
pub fn run_deleted(path: &str) {
    let mut g = FLOOR.lock().unwrap();
    let mut ids = Vec::new();
    if let Some(f) = g.as_mut() {
        for s in &f.segs {
            for c in &s.copies {
                if &*c.path == path {
                    ids.push(c.key.0);
                }
            }
        }
        f.wipe(|c| &*c.path == path);
    }
    drop(g);
    if !ids.is_empty() {
        let mut t = TOMBSTONES.lock().unwrap();
        for id in ids {
            if t.set.remove(&id) {
                t.order.retain(|x| x != &id);
            }
        }
    }
}

/// A database dropped or erased: every copy of its blocks zeroed and forgotten.
pub fn forget_db(db: &str) {
    let mut g = FLOOR.lock().unwrap();
    if let Some(f) = g.as_mut() {
        f.wipe(|c| c.db == db);
    }
}

/// (bytes used, blocks indexed, segments).
pub fn usage() -> (u64, u64, u64) {
    let g = FLOOR.lock().unwrap();
    g.as_ref().map_or((0, 0, 0), |f| (f.used, f.index.len() as u64, f.segs.len() as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_reads_back_wipes_and_rolls() {
        // the floor is process-wide: the other fx tests (which count GETs) must not see it installed
        let _s = super::super::tests::SERIAL.blocking_lock();
        let dir = std::env::temp_dir().join(format!("fx-floor-test-{}", std::process::id()));
        install(dir.clone(), 2 * SEGMENT_BYTES).unwrap();
        let (r1, r2) = ([1u8; RUN_ID_LEN], [2u8; RUN_ID_LEN]);
        put(&r1, 0, "tenants/dba/fx/run/x", b"MARKER-A-block0");
        put(&r2, 3, "tenants/dbb/fx/run/y", b"MARKER-B-block3");
        assert_eq!(get(&r1, 0).as_deref(), Some(&b"MARKER-A-block0"[..]));
        assert_eq!(get(&r2, 3).as_deref(), Some(&b"MARKER-B-block3"[..]));
        assert!(get(&r1, 1).is_none());
        // a dropped database: gone from the index and zeroed on disk; the other database untouched
        forget_db("dba");
        assert!(get(&r1, 0).is_none());
        assert_eq!(get(&r2, 3).as_deref(), Some(&b"MARKER-B-block3"[..]));
        let on_disk: Vec<u8> = std::fs::read_dir(&dir).unwrap().flat_map(|e| std::fs::read(e.unwrap().path()).unwrap()).collect();
        assert!(!on_disk.windows(8).any(|w| w == b"MARKER-A"), "a dropped database's bytes are still on the disk");
        assert!(on_disk.windows(8).any(|w| w == b"MARKER-B"));
        forget_runs(&[r2].into_iter().collect());
        assert!(get(&r2, 3).is_none());
        // full: the oldest segment goes, the floor stays under its cap
        let big = vec![7u8; 1 << 20];
        for b in 0..300u32 {
            put(&r1, b, "tenants/dba/fx/run/x", &big);
        }
        let (used, _, segs) = usage();
        assert!(used <= 2 * SEGMENT_BYTES && segs <= 2, "used {used} segments {segs}");
        assert!(get(&r1, 299).is_some() && get(&r1, 0).is_none());
        install(dir.clone(), 0).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
