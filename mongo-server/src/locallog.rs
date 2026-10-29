//! PROTOTYPE, off by default (`--local-log-dir`): a durable log on the local disk in front of object storage.
//!
//! **Write path.** A write is applied to the database in memory (memtable and WAL buffer, as always), then its operations
//! are appended to the local log and the log is `fdatasync`ed; the write is acknowledged after the fsync. One fsync covers
//! every write that arrived meanwhile, of every database (group fsync). Nothing goes to object storage at that point.
//!
//! **Upload.** Every `--local-log-upload-secs` (30 to 60 s) each database written since the last upload flushes its WAL
//! to object storage: one PUT per database for all the writes of the interval. A database that goes idle and closes is
//! flushed by its close. A log segment is deleted once every write in it is in object storage.
//!
//! **Restart.** At start, before the server accepts connections, the records still in the log are read back (a torn
//! record at the end, from a crash in the middle of a write, is ignored: it was never acknowledged). Every database with
//! records is opened and the records not yet in object storage are applied again, durably, then the log is emptied. Each
//! batch also writes a marker (`sys/local-log` = this log's id and the record's sequence number) in the same atomic
//! batch, so the restart knows exactly which records are already in object storage (none is applied twice). If the
//! marker names another log (another process with its own local log wrote the database since), the records are not
//! applied: the segment files are kept, renamed `*.conflict`, and the restart says so.
//!
//! **What this trades.** A process crash (kill -9) loses nothing that was acknowledged: the records are on the local disk
//! and the restart applies them. Losing the disk (or the machine, for good) loses every write acknowledged since the last
//! upload of each database: up to `--local-log-upload-secs` of writes of every database of the process. Until the restart
//! on the same disk has run, another process opening one of these databases (after the lease expired) does not see those
//! writes, and if it is not itself in local-log mode its later writes can be overwritten when the old disk comes back.
//! For a comparison of request costs, not a default.

use anyhow::{anyhow, bail, Context, Result};
use bson::{doc, Bson, Document};
use sha2::Digest;
use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tenant_bench::lifecycle::Op;
use tokio::sync::{mpsc, oneshot};

/// Key of the marker written with every batch (inside the database's own keyspace; data keys start with `doc/`, `idx/`, `meta/`).
pub const MARKER_KEY: &[u8] = b"sys/local-log";
const SEGMENT_MAX: u64 = 64 << 20;

pub static RECORDS: AtomicU64 = AtomicU64::new(0);
pub static FSYNCS: AtomicU64 = AtomicU64::new(0);
pub static UPLOADS: AtomicU64 = AtomicU64::new(0);
pub static REPLAYED: AtomicU64 = AtomicU64::new(0);

static LOG: std::sync::OnceLock<Arc<LocalLog>> = std::sync::OnceLock::new();

pub fn get() -> Option<&'static Arc<LocalLog>> {
    LOG.get()
}

struct Append {
    bytes: Vec<u8>,
    seq: u64,
    done: oneshot::Sender<Result<(), String>>,
}

#[derive(Default, Clone, Copy)]
struct TenantLog {
    /// Highest sequence number appended for the database, and the highest known to be in object storage.
    staged: u64,
    uploaded: u64,
}

pub struct LocalLog {
    dir: PathBuf,
    pub id: String,
    next_seq: AtomicU64,
    tx: mpsc::UnboundedSender<Append>,
    tenants: Mutex<HashMap<String, TenantLog>>,
    /// Closed segments: number -> highest sequence number in it.
    segments: Mutex<BTreeMap<u64, u64>>,
    pub upload_every: Duration,
}

fn checksum(b: &[u8]) -> [u8; 8] {
    let h = sha2::Sha256::digest(b);
    let mut out = [0u8; 8];
    out.copy_from_slice(&h[..8]);
    out
}

fn encode(seq: u64, db: &str, ops: &[Op], drop: bool) -> Vec<u8> {
    let ops_b: Vec<Bson> = ops
        .iter()
        .map(|o| match o {
            Op::Put(k, v) => Bson::Array(vec![Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: k.clone() }), Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: v.clone() })]),
            Op::Delete(k) => Bson::Array(vec![Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: k.clone() })]),
        })
        .collect();
    let d = doc! { "s": seq as i64, "d": db, "o": ops_b, "x": drop };
    let payload = d.to_vec().expect("bson");
    let mut out = Vec::with_capacity(payload.len() + 12);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&checksum(&payload));
    out.extend_from_slice(&payload);
    out
}

struct Record {
    seq: u64,
    db: String,
    ops: Vec<Op>,
    drop: bool,
}

fn decode(payload: &[u8]) -> Result<Record> {
    let d = Document::from_reader(payload)?;
    let mut ops = Vec::new();
    for o in d.get_array("o")? {
        let a = o.as_array().ok_or_else(|| anyhow!("bad op"))?;
        let bin = |i: usize| -> Result<Vec<u8>> {
            match a.get(i) {
                Some(Bson::Binary(b)) => Ok(b.bytes.clone()),
                _ => bail!("bad op"),
            }
        };
        ops.push(if a.len() == 2 { Op::Put(bin(0)?, bin(1)?) } else { Op::Delete(bin(0)?) });
    }
    Ok(Record { seq: d.get_i64("s")? as u64, db: d.get_str("d")?.to_string(), ops, drop: d.get_bool("x").unwrap_or(false) })
}

/// Records of one segment file, up to the first torn or corrupt one.
fn read_segment(path: &Path) -> Result<(Vec<Record>, bool)> {
    let mut f = std::fs::File::open(path)?;
    let mut all = Vec::new();
    f.read_to_end(&mut all)?;
    let mut out = Vec::new();
    let mut pos = 0usize;
    let mut torn = false;
    while pos < all.len() {
        if pos + 12 > all.len() {
            torn = true;
            break;
        }
        let len = u32::from_le_bytes(all[pos..pos + 4].try_into().unwrap()) as usize;
        if pos + 12 + len > all.len() {
            torn = true;
            break;
        }
        let payload = &all[pos + 12..pos + 12 + len];
        if checksum(payload) != all[pos + 4..pos + 12] {
            torn = true;
            break;
        }
        out.push(decode(payload)?);
        pos += 12 + len;
    }
    Ok((out, torn))
}

fn segment_path(dir: &Path, n: u64) -> PathBuf {
    dir.join(format!("seg-{n:012}.log"))
}

fn list_segments(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut v = Vec::new();
    for e in std::fs::read_dir(dir)? {
        let p = e?.path();
        if let Some(n) = p.file_name().and_then(|f| f.to_str()).and_then(|f| f.strip_prefix("seg-")).and_then(|f| f.strip_suffix(".log")).and_then(|f| f.parse::<u64>().ok()) {
            v.push((n, p));
        }
    }
    v.sort();
    Ok(v)
}

fn marker_value(log_id: &str, seq: u64) -> Vec<u8> {
    doc! { "log": log_id, "seq": seq as i64 }.to_vec().expect("bson")
}

fn parse_marker(b: &[u8]) -> Option<(String, u64)> {
    let d = Document::from_reader(b).ok()?;
    Some((d.get_str("log").ok()?.to_string(), d.get_i64("seq").ok()? as u64))
}

impl LocalLog {
    /// Open (or create) the log directory. Records left by a previous run are returned for [`recover`]; the new records
    /// go to a fresh segment numbered after the old ones.
    fn open(dir: &str, upload_every: Duration) -> Result<(Arc<Self>, Vec<(u64, PathBuf)>)> {
        let dir = PathBuf::from(dir);
        std::fs::create_dir_all(&dir).with_context(|| format!("local log directory {}", dir.display()))?;
        let id_path = dir.join("LOG_ID");
        let id = match std::fs::read_to_string(&id_path) {
            Ok(s) if !s.trim().is_empty() => s.trim().to_string(),
            _ => {
                let id = format!("{:016x}", rand::random::<u64>());
                let mut f = std::fs::File::create(&id_path)?;
                f.write_all(id.as_bytes())?;
                f.sync_all()?;
                std::fs::File::open(&dir)?.sync_all()?;
                id
            }
        };
        let old = list_segments(&dir)?;
        let first = old.last().map(|(n, _)| n + 1).unwrap_or(1);
        let (tx, rx) = mpsc::unbounded_channel();
        let me = Arc::new(Self { dir: dir.clone(), id, next_seq: AtomicU64::new(1), tx, tenants: Mutex::new(HashMap::new()), segments: Mutex::new(BTreeMap::new()), upload_every });
        let w = Arc::clone(&me);
        std::thread::Builder::new().name("local-log".into()).spawn(move || w.writer_thread(rx, first))?;
        Ok((me, old))
    }

    /// The writer: appends every queued record, one `fdatasync` for the lot, then acknowledges them.
    fn writer_thread(self: Arc<Self>, mut rx: mpsc::UnboundedReceiver<Append>, mut segno: u64) {
        let open = |n: u64| std::fs::OpenOptions::new().create(true).append(true).open(segment_path(&self.dir, n));
        let mut f = match open(segno) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("local log: cannot open segment: {e}");
                return;
            }
        };
        let _ = std::fs::File::open(&self.dir).and_then(|d| d.sync_all());
        let mut size = 0u64;
        let mut seg_max = 0u64;
        while let Some(first) = rx.blocking_recv() {
            let mut batch = vec![first];
            while let Ok(more) = rx.try_recv() {
                batch.push(more);
            }
            let mut res: Result<(), String> = Ok(());
            for a in &batch {
                if let Err(e) = f.write_all(&a.bytes) {
                    res = Err(format!("local log write failed: {e}"));
                    break;
                }
                size += a.bytes.len() as u64;
                seg_max = seg_max.max(a.seq);
            }
            if res.is_ok() {
                if let Err(e) = f.sync_data() {
                    res = Err(format!("local log fsync failed: {e}"));
                }
                FSYNCS.fetch_add(1, Relaxed);
            }
            RECORDS.fetch_add(batch.len() as u64, Relaxed);
            for a in batch {
                let _ = a.done.send(res.clone());
            }
            if size >= SEGMENT_MAX {
                self.segments.lock().unwrap().insert(segno, seg_max);
                segno += 1;
                size = 0;
                seg_max = 0;
                match open(segno) {
                    Ok(nf) => f = nf,
                    Err(e) => {
                        eprintln!("local log: cannot open segment: {e}");
                        return;
                    }
                }
                let _ = std::fs::File::open(&self.dir).and_then(|d| d.sync_all());
            }
        }
    }

    /// Sequence number for the next batch of `db` (taken under the database's lock, so records of one database are in
    /// the order they were applied).
    pub fn next_seq(&self) -> u64 {
        self.next_seq.fetch_add(1, Relaxed)
    }

    pub fn marker_op(&self, seq: u64) -> Op {
        Op::Put(MARKER_KEY.to_vec(), marker_value(&self.id, seq))
    }

    /// Queue a record; the receiver resolves once it is on disk (fsync).
    pub fn append(&self, seq: u64, db: &str, ops: &[Op]) -> oneshot::Receiver<Result<(), String>> {
        self.append_rec(seq, db, ops, false)
    }

    /// The database was dropped: its earlier records must not be applied again by a restart.
    pub fn append_drop(&self, db: &str) -> oneshot::Receiver<Result<(), String>> {
        let seq = self.next_seq();
        let rx = self.append_rec(seq, db, &[], true);
        let mut t = self.tenants.lock().unwrap();
        let e = t.entry(db.to_string()).or_default();
        e.staged = seq;
        e.uploaded = seq;
        rx
    }

    fn append_rec(&self, seq: u64, db: &str, ops: &[Op], drop: bool) -> oneshot::Receiver<Result<(), String>> {
        let (done, rx) = oneshot::channel();
        {
            let mut t = self.tenants.lock().unwrap();
            let e = t.entry(db.to_string()).or_default();
            e.staged = e.staged.max(seq);
        }
        if self.tx.send(Append { bytes: encode(seq, db, ops, drop), seq, done }).is_err() {
            let (d2, rx2) = oneshot::channel();
            let _ = d2.send(Err("local log writer stopped".into()));
            return rx2;
        }
        rx
    }

    /// Records of the database that are not yet in object storage.
    pub fn has_pending(&self, db: &str) -> bool {
        self.tenants.lock().unwrap().get(db).map_or(false, |e| e.uploaded < e.staged)
    }

    /// The database closed cleanly (its close flushed everything to object storage).
    pub fn closed(&self, db: &str) {
        if let Some(e) = self.tenants.lock().unwrap().get_mut(db) {
            e.uploaded = e.staged;
        }
    }

    /// Lowest sequence number not yet in object storage (u64::MAX: everything is).
    fn low_water(&self) -> u64 {
        self.tenants.lock().unwrap().values().filter(|e| e.uploaded < e.staged).map(|e| e.uploaded + 1).min().unwrap_or(u64::MAX)
    }

    /// Delete closed segments whose records are all in object storage.
    fn trim(&self) {
        let low = self.low_water();
        let mut segs = self.segments.lock().unwrap();
        let done: Vec<u64> = segs.iter().filter(|(_, max)| **max < low).map(|(n, _)| *n).collect();
        for n in done {
            let _ = std::fs::remove_file(segment_path(&self.dir, n));
            segs.remove(&n);
        }
    }

    /// Every `upload_every`: flush the WAL of each database written since its last upload (one PUT each).
    pub fn spawn_uploader(self: &Arc<Self>) {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(me.upload_every);
            tick.tick().await;
            loop {
                tick.tick().await;
                let due: Vec<(String, u64)> = me.tenants.lock().unwrap().iter().filter(|(_, e)| e.uploaded < e.staged).map(|(k, e)| (k.clone(), e.staged)).collect();
                futures::future::join_all(due.into_iter().map(|(db, target)| {
                    let me = Arc::clone(&me);
                    async move {
                        // Every record up to `target` is staged in the writer (appended after staging), so one flush covers it.
                        match tenant_bench::lifecycle::flush_writer(&db).await {
                            Ok(false) => {} // no open writer: its close flushed it (and marked it), or will be replayed
                            Ok(true) => {
                                UPLOADS.fetch_add(1, Relaxed);
                                if let Some(e) = me.tenants.lock().unwrap().get_mut(&db) {
                                    e.uploaded = e.uploaded.max(target);
                                }
                            }
                            Err(e) => eprintln!("local log: upload of {db} failed: {e:#}"),
                        }
                    }
                }))
                .await;
                me.trim();
            }
        });
    }
}

/// Start the local log (flag set): returns the records a previous run left, for [`recover`].
pub fn install(dir: &str, upload_secs: u64) -> Result<Vec<(u64, PathBuf)>> {
    anyhow::ensure!((30..=60).contains(&upload_secs), "--local-log-upload-secs must be between 30 and 60");
    let (log, old) = LocalLog::open(dir, Duration::from_secs(upload_secs))?;
    tenant_bench::lifecycle::LOCAL_LOG_FLUSH_SECS.store(upload_secs, Relaxed);
    let _ = LOG.set(log);
    Ok(old)
}

/// Apply again, durably, the records a previous run left in the log; then delete those segments. Runs before the server
/// accepts connections. Returns (databases, records applied).
pub async fn recover(dbs: &Arc<crate::dbs::Dbs>, old: Vec<(u64, PathBuf)>) -> Result<(usize, u64)> {
    let Some(log) = get() else { return Ok((0, 0)) };
    if old.is_empty() {
        return Ok((0, 0));
    }
    let mut by_db: BTreeMap<String, Vec<Record>> = BTreeMap::new();
    let mut max_seq = 0;
    for (_, p) in &old {
        let (recs, torn) = read_segment(p)?;
        if torn {
            eprintln!("local log: {} ends with a torn record (a write that was never acknowledged); ignored", p.display());
        }
        for r in recs {
            max_seq = max_seq.max(r.seq);
            if r.drop {
                by_db.remove(&r.db);
                by_db.insert(r.db.clone(), Vec::new());
                continue;
            }
            by_db.entry(r.db.clone()).or_default().push(r);
        }
    }
    log.next_seq.store(max_seq + 1, Relaxed);
    let mut applied = 0u64;
    let mut n_dbs = 0usize;
    let mut conflicts = Vec::new();
    for (db, mut recs) in by_db {
        if recs.is_empty() {
            continue;
        }
        recs.sort_by_key(|r| r.seq);
        let mut g = dbs.lock(&db).await;
        let marker = g.get_live(MARKER_KEY).await?;
        let done_seq = match marker.as_deref().and_then(parse_marker) {
            Some((id, s)) if id == log.id => s,
            Some((other, _)) => {
                eprintln!("local log: database {db} was written by another local log ({other}) since; its {} records are NOT applied", recs.len());
                conflicts.push(db.clone());
                continue;
            }
            None => 0,
        };
        let todo: Vec<Record> = recs.into_iter().filter(|r| r.seq > done_seq).collect();
        if todo.is_empty() {
            continue;
        }
        n_dbs += 1;
        // A few hundred records per durable batch; every batch ends with the marker of its last record.
        for chunk in todo.chunks(256) {
            let mut ops: Vec<Op> = Vec::new();
            for r in chunk {
                ops.extend(r.ops.iter().map(|o| match o {
                    Op::Put(k, v) => Op::Put(k.clone(), v.clone()),
                    Op::Delete(k) => Op::Delete(k.clone()),
                }));
            }
            ops.push(log.marker_op(chunk.last().unwrap().seq));
            g.apply_durable(ops).await?;
            applied += chunk.len() as u64;
        }
        eprintln!("local log: database {db}: {} records applied again (after record {done_seq})", todo.len());
    }
    REPLAYED.fetch_add(applied, Relaxed);
    for (_, p) in old {
        if conflicts.is_empty() {
            let _ = std::fs::remove_file(&p);
        } else {
            let _ = std::fs::rename(&p, p.with_extension("conflict"));
        }
    }
    if !conflicts.is_empty() {
        eprintln!("local log: segments kept as *.conflict for {conflicts:?}");
    }
    Ok((n_dbs, applied))
}
