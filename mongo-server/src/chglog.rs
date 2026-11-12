//! Durable change history (P-durable-changelog, design C of the session report).
//!
//! **Write.** Every write batch that produces change events also carries, in the same atomic SlateDB batch, one row per
//! event (`chg/` + the event's position, 8 bytes big-endian; the value is the event in BSON with its position and cluster
//! time) and the database's history head (`sys/chg-head`). Positions are per database, taken under the database's
//! exclusive lock, so they follow commit order; the event is durable exactly when its write is (same WAL object, or the
//! same record of the local or replicated log): never for a write that is not durable, exactly once. No extra request.
//!
//! **Archive.** The owner of the database reads the durable tail rows in position order and writes them as one
//! zstd-compressed segment object `changes/{db}/{history}/{first}-{last}-{prev time}-{first time}-{last time}.seg`
//! (create-if-absent: a fenced old owner cannot replace the new owner's segment) once the tail holds
//! [`SEGMENT_BYTES`] or its oldest row is [`SEGMENT_AGE_SECS`] old; then it deletes those tail rows and moves the head's
//! `archived` mark in one batch. A crash between the two leaves both copies: the next archive adopts the segment, and
//! readers drop duplicates by position.
//!
//! **Read.** Streams read the in-memory log (`changes.rs`) while it holds their position; otherwise segments (cached, at
//! most [`CACHE_BYTES`] decoded) then tail rows. Positions are contiguous: a missing position is never skipped, it fails
//! the stream with ChangeStreamHistoryLost (286), as a position older than the oldest segment kept does.
//!
//! **Retention.** Per database, segments older than [`RETENTION_SECS`] or beyond [`RETENTION_BYTES`] (oldest first) are
//! deleted by the owner after each archive; a sweep of every database's segments ([`SWEEP_SECS`]) deletes expired ones of
//! databases nobody has open. Default 23 hours so that an erased customer's events leave the store within 24 hours.
//!
//! **Erasure.** A customer erasure records a filter (`sys/chg-erased`, durable, with the erasure's position): no stream
//! returns that customer's events up to that position, from memory, tail or segments; the segments leave the store by age.

use crate::changes::EvSpec;
use anyhow::{anyhow, bail, Context, Result};
use bson::{doc, Bson, DateTime, Document, Timestamp};
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

/// `--change-history` (default on): write events durably. Off: the in-memory log only, as before.
pub static ENABLED: AtomicBool = AtomicBool::new(true);
/// `--change-retention-secs`: segments older than this are deleted.
pub static RETENTION_SECS: AtomicU64 = AtomicU64::new(23 * 3600);
/// `--change-retention-mb`: the stored (compressed) bytes of one database's segments, oldest deleted first.
pub static RETENTION_BYTES: AtomicU64 = AtomicU64::new(50 << 30);
/// `--change-segment-mb`: raw bytes of events per segment.
pub static SEGMENT_BYTES: AtomicU64 = AtomicU64::new(32 << 20);
/// `--change-segment-secs`: a tail whose oldest row is this old is archived even if small.
pub static SEGMENT_AGE_SECS: AtomicU64 = AtomicU64::new(300);
/// `--change-sweep-secs`: how often every database's segments are checked for expiry.
pub static SWEEP_SECS: AtomicU64 = AtomicU64::new(900);
/// How often the archiver looks at the open databases (milliseconds).
pub static ARCHIVE_CHECK_MS: AtomicU64 = AtomicU64::new(500);
/// Decoded segments kept in memory for readers (bytes).
pub const CACHE_BYTES: usize = 64 << 20;

/// Counters (tenantStats `changeHistory`).
pub static ROWS: AtomicU64 = AtomicU64::new(0);
pub static ROW_BYTES: AtomicU64 = AtomicU64::new(0);
pub static SEGMENTS_PUT: AtomicU64 = AtomicU64::new(0);
pub static SEGMENT_RAW_BYTES: AtomicU64 = AtomicU64::new(0);
pub static SEGMENT_STORED_BYTES: AtomicU64 = AtomicU64::new(0);
pub static SEGMENTS_ADOPTED: AtomicU64 = AtomicU64::new(0);
pub static SEGMENT_GETS: AtomicU64 = AtomicU64::new(0);
pub static SEGMENT_LISTS: AtomicU64 = AtomicU64::new(0);
pub static SEGMENTS_DELETED: AtomicU64 = AtomicU64::new(0);
pub static TAIL_DELETES: AtomicU64 = AtomicU64::new(0);
pub static DURABLE_READS: AtomicU64 = AtomicU64::new(0);
pub static ARCHIVE_ERRORS: AtomicU64 = AtomicU64::new(0);

pub const TAIL: &[u8] = b"chg/";
/// The first key after every tail row (`/` + 1).
pub const TAIL_END: &[u8] = b"chg0";
pub const HEAD_KEY: &[u8] = b"sys/chg-head";
pub const ERASED_KEY: &[u8] = b"sys/chg-erased";

pub fn enabled() -> bool {
    ENABLED.load(Relaxed)
}

/// TEST ONLY: `FENVIX_CHG_CRASH=<step>` ends the process at once at that step (as a kill -9 there would): the kill tests
/// of the archive and the trim.
fn crash_at(step: &str) {
    static AT: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    if AT.get_or_init(|| std::env::var("FENVIX_CHG_CRASH").ok()).as_deref() == Some(step) {
        eprintln!("CHG CRASH at {step}");
        std::process::abort();
    }
}

pub fn tail_key(pos: u64) -> Vec<u8> {
    [TAIL, &pos.to_be_bytes()[..]].concat()
}

pub fn tail_pos(key: &[u8]) -> Option<u64> {
    let rest = key.strip_prefix(TAIL)?;
    Some(u64::from_be_bytes(rest.try_into().ok()?))
}

/// Keys of the history (left out of a point-in-time restore's difference: the history is not rewound).
pub fn is_history_key(k: &[u8]) -> bool {
    k.starts_with(TAIL) || k == HEAD_KEY || k == ERASED_KEY
}

pub fn pack(t: Timestamp) -> u64 {
    ((t.time as u64) << 32) | t.increment as u64
}

pub fn unpack(u: u64) -> Timestamp {
    Timestamp { time: (u >> 32) as u32, increment: u as u32 }
}

fn now_ms() -> i64 {
    DateTime::now().timestamp_millis()
}

/// The history head of a database (`sys/chg-head`), written with every batch that carries events.
#[derive(Clone, Debug, PartialEq)]
pub struct Head {
    /// Random id of this history: a new one for a new database, a fork, or a database recreated under the same name.
    pub hist: u64,
    /// The database the history belongs to (a copy fork carries its source's head: it starts a history of its own).
    pub db: String,
    /// The next position to give.
    pub next: u64,
    /// Cluster time of the last event (packed).
    pub last: u64,
    /// Every position up to this one is in a segment; its cluster time (packed).
    pub archived: u64,
    pub archived_t: u64,
    /// Wall time (ms) of the oldest tail row (0: empty tail) and the tail's raw bytes, for the archive thresholds.
    pub tail_first_ms: i64,
    pub tail_bytes: u64,
}

impl Head {
    pub fn fresh(db: &str) -> Head {
        Head { hist: rand::random::<u64>() | 1, db: db.to_string(), next: 1, last: 0, archived: 0, archived_t: 0, tail_first_ms: 0, tail_bytes: 0 }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        doc! { "h": self.hist as i64, "db": &self.db, "n": self.next as i64, "l": self.last as i64, "a": self.archived as i64, "at": self.archived_t as i64, "tf": self.tail_first_ms, "tb": self.tail_bytes as i64 }.to_vec().unwrap_or_default()
    }

    pub fn from_bytes(b: &[u8]) -> Result<Head> {
        let d = Document::from_reader(b).context("history head")?;
        let n = |k: &str| d.get_i64(k).map(|v| v as u64).map_err(|_| anyhow!("history head: field {k} missing"));
        Ok(Head { hist: n("h")?, db: d.get_str("db").unwrap_or("").to_string(), next: n("n")?, last: n("l")?, archived: n("a")?, archived_t: n("at")?, tail_first_ms: d.get_i64("tf").unwrap_or(0), tail_bytes: n("tb").unwrap_or(0) })
    }

    pub fn tail_len(&self) -> u64 {
        self.next.saturating_sub(self.archived + 1)
    }
}

/// A customer erasure (customer-keyed collection): no stream returns the customer's events at or before `at`.
#[derive(Clone, Debug, PartialEq)]
pub struct Erased {
    pub coll: String,
    pub field: String,
    pub value: Bson,
    pub at: u64,
    pub wall_ms: i64,
}

impl Erased {
    /// The rule of `ChangeLog::forget_customer`: the event's customer, or the customer of a document it carries.
    pub fn hides(&self, pos: u64, s: &EvSpec) -> bool {
        if pos > self.at || s.coll.as_deref() != Some(self.coll.as_str()) {
            return false;
        }
        let want = crate::custkey::comp(&self.value);
        let of = |d: &Option<Document>| d.as_ref().is_some_and(|d| d.get(&self.field).is_some() && crate::custkey::comp(&crate::custkey::value(&self.field, d)) == want);
        s.cust.as_ref().is_some_and(|(_, v)| crate::custkey::comp(v) == want) || of(&s.full) || of(&s.pre) || of(&s.post)
    }
}

pub fn erased_to_bytes(list: &[Erased]) -> Vec<u8> {
    let arr: Vec<Bson> = list.iter().map(|e| Bson::Document(doc! { "c": &e.coll, "f": &e.field, "v": e.value.clone(), "a": e.at as i64, "w": e.wall_ms })).collect();
    doc! { "e": arr }.to_vec().unwrap_or_default()
}

pub fn erased_from_bytes(b: &[u8]) -> Result<Vec<Erased>> {
    let d = Document::from_reader(b).context("history erasures")?;
    let mut out = Vec::new();
    for x in d.get_array("e").map_err(|_| anyhow!("history erasures: no list"))? {
        let Bson::Document(x) = x else { continue };
        out.push(Erased { coll: x.get_str("c").unwrap_or("").to_string(), field: x.get_str("f").unwrap_or("").to_string(), value: x.get("v").cloned().unwrap_or(Bson::Null), at: x.get_i64("a").unwrap_or(0) as u64, wall_ms: x.get_i64("w").unwrap_or(0) });
    }
    Ok(out)
}

/// One event of the history as stored.
#[derive(Clone, Debug)]
pub struct Row {
    pub pos: u64,
    pub time: Timestamp,
    pub wall: DateTime,
    pub spec: EvSpec,
    /// Size of the stored row.
    pub bytes: usize,
}

const OPS: &[&str] = &["insert", "update", "replace", "delete", "drop", "rename", "dropDatabase", "invalidate", "create", "createIndexes", "dropIndexes", "modify"];

fn op_static(s: &str) -> Result<&'static str> {
    OPS.iter().find(|o| **o == s).copied().ok_or_else(|| anyhow!("history row: unknown operation {s:?}"))
}

/// The stored row of an event.
pub fn encode(pos: u64, time: Timestamp, wall: DateTime, s: &EvSpec) -> Vec<u8> {
    let mut d = doc! { "s": pos as i64, "t": Bson::Timestamp(time), "w": wall, "o": s.op };
    if let Some(c) = &s.coll {
        d.insert("c", c.clone());
    }
    if let Some(k) = &s.doc_key {
        d.insert("k", k.clone());
    }
    if let Some((f, v)) = &s.cust {
        d.insert("cf", f.clone());
        d.insert("cv", v.clone());
    }
    let mut opt = |k: &str, v: &Option<Document>| {
        if let Some(v) = v {
            d.insert(k, v.clone());
        }
    };
    opt("f", &s.full);
    opt("d", &s.desc);
    opt("od", &s.op_desc);
    opt("sb", &s.state_before);
    opt("pr", &s.pre);
    opt("po", &s.post);
    if let Some((n, lsid)) = &s.txn {
        d.insert("tn", *n);
        d.insert("tl", lsid.clone());
    }
    if let Some((db, c)) = &s.to {
        d.insert("to", vec![Bson::String(db.clone()), Bson::String(c.clone())]);
    }
    if let Some(u) = &s.uuid {
        d.insert("u", Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: u.clone() }));
    }
    if s.images {
        d.insert("im", true);
    }
    if s.expanded_only {
        d.insert("x", true);
    }
    d.to_vec().unwrap_or_default()
}

pub fn decode(db: &str, b: &[u8]) -> Result<Row> {
    let d = Document::from_reader(b).context("history row")?;
    let pos = d.get_i64("s").map_err(|_| anyhow!("history row without position"))? as u64;
    let time = match d.get("t") {
        Some(Bson::Timestamp(t)) => *t,
        _ => bail!("history row {pos} without time"),
    };
    let wall = d.get_datetime("w").copied().unwrap_or(DateTime::from_millis(0));
    let doc_of = |k: &str| d.get_document(k).ok().cloned();
    let spec = EvSpec {
        db: db.to_string(),
        coll: d.get_str("c").ok().map(|s| s.to_string()),
        op: op_static(d.get_str("o").unwrap_or(""))?,
        doc_key: d.get("k").cloned(),
        cust: match (d.get_document("cf"), d.get("cv")) {
            (Ok(f), Some(v)) => Some((f.clone(), v.clone())),
            _ => None,
        },
        full: doc_of("f"),
        desc: doc_of("d"),
        txn: match (d.get_i64("tn"), d.get_document("tl")) {
            (Ok(n), Ok(l)) => Some((n, l.clone())),
            _ => None,
        },
        to: d.get_array("to").ok().and_then(|a| match (a.first(), a.get(1)) {
            (Some(Bson::String(x)), Some(Bson::String(y))) => Some((x.clone(), y.clone())),
            _ => None,
        }),
        uuid: match d.get("u") {
            Some(Bson::Binary(b)) => Some(b.bytes.clone()),
            _ => None,
        },
        op_desc: doc_of("od"),
        state_before: doc_of("sb"),
        pre: doc_of("pr"),
        post: doc_of("po"),
        images: d.get_bool("im").unwrap_or(false),
        expanded_only: d.get_bool("x").unwrap_or(false),
    };
    Ok(Row { pos, time, wall, spec, bytes: b.len() })
}

// ---- segments --------------------------------------------------------------------------------------------------------

/// What a segment's name says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegName {
    pub first: u64,
    pub last: u64,
    /// Cluster time (packed) of the event just before `first` (0: none), and of `first` and `last`.
    pub prev_t: u64,
    pub first_t: u64,
    pub last_t: u64,
}

impl SegName {
    pub fn file(&self) -> String {
        format!("{:016x}-{:016x}-{:016x}-{:016x}-{:016x}.seg", self.first, self.last, self.prev_t, self.first_t, self.last_t)
    }

    pub fn parse(file: &str) -> Option<SegName> {
        let s = file.strip_suffix(".seg")?;
        let p: Vec<u64> = s.split('-').map(|x| u64::from_str_radix(x, 16)).collect::<std::result::Result<_, _>>().ok()?;
        (p.len() == 5 && p[0] <= p[1]).then(|| SegName { first: p[0], last: p[1], prev_t: p[2], first_t: p[3], last_t: p[4] })
    }
}

pub fn db_dir(db: &str) -> String {
    format!("changes/{db}/")
}

pub fn hist_dir(db: &str, hist: u64) -> String {
    format!("changes/{db}/{hist:016x}/")
}

/// A listed segment: its name and stored size.
#[derive(Clone, Copy, Debug)]
pub struct Seg {
    pub name: SegName,
    pub size: u64,
}

/// Segments of each history, sorted by first position, as last listed (plus what this process wrote or deleted since).
static INDEX: std::sync::LazyLock<Mutex<HashMap<(String, u64), Vec<Seg>>>> = std::sync::LazyLock::new(Default::default);

pub async fn list(store: &Arc<dyn ObjectStore>, db: &str, hist: u64) -> Result<Vec<Seg>> {
    SEGMENT_LISTS.fetch_add(1, Relaxed);
    let prefix = Path::from(hist_dir(db, hist));
    let metas: Vec<object_store::ObjectMeta> = store.list(Some(&prefix)).try_collect().await?;
    let mut segs: Vec<Seg> = metas.iter().filter_map(|m| SegName::parse(m.location.filename()?).map(|name| Seg { name, size: m.size })).collect();
    segs.sort_by_key(|s| (s.name.first, s.name.last));
    INDEX.lock().unwrap().insert((db.to_string(), hist), segs.clone());
    Ok(segs)
}

fn index_cached(db: &str, hist: u64) -> Option<Vec<Seg>> {
    INDEX.lock().unwrap().get(&(db.to_string(), hist)).cloned()
}

/// The segments of a history: the cached list when it reaches `want` (a position), else a fresh listing.
pub async fn index(store: &Arc<dyn ObjectStore>, db: &str, hist: u64, want: u64) -> Result<Vec<Seg>> {
    if let Some(s) = index_cached(db, hist) {
        if s.last().is_some_and(|l| l.name.last >= want) || want == 0 {
            return Ok(s);
        }
    }
    list(store, db, hist).await
}

fn index_add(db: &str, hist: u64, seg: Seg) {
    if let Some(v) = INDEX.lock().unwrap().get_mut(&(db.to_string(), hist)) {
        if !v.iter().any(|s| s.name == seg.name) {
            v.push(seg);
            v.sort_by_key(|s| (s.name.first, s.name.last));
        }
    }
}

fn index_remove(db: &str, hist: u64, name: &SegName) {
    if let Some(v) = INDEX.lock().unwrap().get_mut(&(db.to_string(), hist)) {
        v.retain(|s| s.name != *name);
    }
}

/// Forget every cached listing of a database (dropped or erased).
pub fn forget_db(db: &str) {
    INDEX.lock().unwrap().retain(|(d, _), _| d != db);
    CACHE.lock().unwrap().retain(|(p, _, _)| !p.starts_with(&db_dir(db)));
}

static CACHE: std::sync::LazyLock<Mutex<VecDeque<(String, Arc<Vec<Row>>, usize)>>> = std::sync::LazyLock::new(Default::default);

fn compress(rows: &[(u64, Bytes)]) -> Result<Vec<u8>> {
    let mut raw = Vec::with_capacity(rows.iter().map(|r| r.1.len()).sum());
    for (_, b) in rows {
        raw.extend_from_slice(b);
    }
    Ok(zstd::bulk::compress(&raw, 3)?)
}

fn split_rows(db: &str, raw: &[u8]) -> Result<Vec<Row>> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < raw.len() {
        if raw.len() - at < 5 {
            bail!("segment: torn row at byte {at}");
        }
        let n = i32::from_le_bytes(raw[at..at + 4].try_into().unwrap()) as usize;
        if n < 5 || at + n > raw.len() {
            bail!("segment: bad row length {n} at byte {at}");
        }
        out.push(decode(db, &raw[at..at + n])?);
        at += n;
    }
    Ok(out)
}

/// The rows of a segment (cached).
pub async fn load_segment(store: &Arc<dyn ObjectStore>, db: &str, hist: u64, name: &SegName) -> Result<Arc<Vec<Row>>> {
    let path = format!("{}{}", hist_dir(db, hist), name.file());
    if let Some((_, r, _)) = CACHE.lock().unwrap().iter().find(|(p, _, _)| *p == path) {
        return Ok(Arc::clone(r));
    }
    SEGMENT_GETS.fetch_add(1, Relaxed);
    let body = store.get(&Path::from(path.clone())).await?.bytes().await?;
    let raw = zstd::bulk::decompress(&body, (SEGMENT_BYTES.load(Relaxed) as usize).max(64 << 20) * 4).context("segment: zstd")?;
    let rows = split_rows(db, &raw)?;
    if rows.first().map(|r| r.pos) != Some(name.first) || rows.last().map(|r| r.pos) != Some(name.last) || rows.windows(2).any(|w| w[1].pos != w[0].pos + 1) {
        bail!("segment {path}: positions do not match its name");
    }
    let rows = Arc::new(rows);
    let mut c = CACHE.lock().unwrap();
    c.push_back((path, Arc::clone(&rows), raw.len()));
    let mut total: usize = c.iter().map(|x| x.2).sum();
    while total > CACHE_BYTES && c.len() > 1 {
        if let Some((_, _, n)) = c.pop_front() {
            total -= n;
        }
    }
    Ok(rows)
}

/// Put a segment (create-if-absent). `Ok(None)`: written. `Ok(Some(seg))`: a segment starting at the same position was
/// already there (an earlier attempt of this process or of an older owner): it is adopted.
async fn put_segment(store: &Arc<dyn ObjectStore>, db: &str, hist: u64, name: SegName, rows: &[(u64, Bytes)]) -> Result<Option<Seg>> {
    let body = compress(rows)?;
    let size = body.len() as u64;
    let path = Path::from(format!("{}{}", hist_dir(db, hist), name.file()));
    let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
    match store.put_opts(&path, PutPayload::from(body), opts).await {
        Ok(_) => {
            SEGMENTS_PUT.fetch_add(1, Relaxed);
            SEGMENT_RAW_BYTES.fetch_add(rows.iter().map(|r| r.1.len() as u64).sum(), Relaxed);
            SEGMENT_STORED_BYTES.fetch_add(size, Relaxed);
            index_add(db, hist, Seg { name, size });
            Ok(None)
        }
        Err(object_store::Error::AlreadyExists { .. }) | Err(object_store::Error::Precondition { .. }) => {
            let segs = list(store, db, hist).await?;
            match segs.into_iter().find(|s| s.name.first == name.first) {
                Some(s) => {
                    SEGMENTS_ADOPTED.fetch_add(1, Relaxed);
                    Ok(Some(s))
                }
                None => bail!("segment {path}: exists but is not listed"),
            }
        }
        Err(e) => Err(e.into()),
    }
}

// ---- archive and retention -------------------------------------------------------------------------------------------

/// Whether the tail of `h` is due for a segment.
pub fn due(h: &Head) -> bool {
    h.tail_len() > 0 && (h.tail_bytes >= SEGMENT_BYTES.load(Relaxed) || (h.tail_first_ms > 0 && now_ms() - h.tail_first_ms >= SEGMENT_AGE_SECS.load(Relaxed) as i64 * 1000))
}

/// Archive the tail of `db` if it is due (`force`: whatever its size). Returns whether a segment was written or adopted.
pub async fn archive(dbs: &Arc<crate::dbs::Dbs>, db: &str, force: bool) -> Result<bool> {
    let store = dbs.store();
    // 1. the head and the durable tail rows, under the database's lock
    let (head, opening, rows, next_wall) = {
        let mut g = dbs.lock(db).await;
        let Some(h) = g.chg_cached().cloned() else { return Ok(false) };
        if !g.is_open_writer() || !(force && h.tail_len() > 0 || due(&h)) {
            return Ok(false);
        }
        let max = SEGMENT_BYTES.load(Relaxed) as usize;
        // (the scan stops after the row that reaches `max`: that row stays in the tail, and its wall time is the tail's
        // oldest after this archive)
        let mut rows = g.chg_scan(h.archived + 1, usize::MAX, max).await?;
        let total: usize = rows.iter().map(|r| r.1.len()).sum();
        let next_wall = if total >= max && rows.len() >= 2 { rows.pop().map(|(_, b)| decode(db, &b).map(|r| r.wall.timestamp_millis()).unwrap_or(0)) } else { None };
        (h, g.opening(), rows, next_wall)
    };
    if rows.first().map(|r| r.0) != Some(head.archived + 1) {
        return Ok(false);
    }
    let mut n = 1;
    while n < rows.len() && rows[n].0 == rows[n - 1].0 + 1 {
        n += 1;
    }
    let rows = &rows[..n];
    let first = decode(db, &rows[0].1)?;
    let last = decode(db, &rows[n - 1].1)?;
    let name = SegName { first: first.pos, last: last.pos, prev_t: head.archived_t, first_t: pack(first.time), last_t: pack(last.time) };
    // 2. the segment
    crash_at("archive.before_put");
    let (upto, upto_t) = match put_segment(&store, db, head.hist, name, rows).await? {
        None => (name.last, name.last_t),
        Some(s) => (s.name.last, s.name.last_t),
    };
    crash_at("archive.after_put");
    // 3. drop the archived tail rows and move the mark, if nothing changed meanwhile
    let mut g = dbs.lock(db).await;
    if g.opening() != opening || !g.is_open_writer() {
        return Ok(true);
    }
    let Some(h) = g.chg_cached().cloned() else { return Ok(true) };
    if h.hist != head.hist || h.archived != head.archived || upto >= h.next {
        return Ok(true);
    }
    let gone: u64 = rows.iter().filter(|r| r.0 <= upto).map(|r| r.1.len() as u64).sum();
    let mut nh = h.clone();
    nh.archived = upto;
    nh.archived_t = upto_t;
    nh.tail_bytes = nh.tail_bytes.saturating_sub(gone);
    nh.tail_first_ms = if nh.tail_len() == 0 { 0 } else { next_wall.filter(|_| upto == name.last).unwrap_or_else(now_ms) };
    let mut ops: Vec<tenant_bench::lifecycle::Op> = (head.archived + 1..=upto).map(|p| tenant_bench::lifecycle::Op::Delete(tail_key(p))).collect();
    TAIL_DELETES.fetch_add(ops.len() as u64, Relaxed);
    ops.push(tenant_bench::lifecycle::Op::Put(HEAD_KEY.to_vec(), nh.to_bytes()));
    g.chg_set(nh);
    if let Err(e) = g.apply(ops).await {
        g.chg_reset();
        return Err(e);
    }
    drop(g);
    crash_at("archive.after_tail_delete");
    trim(&store, db, head.hist, upto).await?;
    Ok(true)
}

/// Delete the segments of a history that are past retention (by age, then by bytes, oldest first).
pub async fn trim(store: &Arc<dyn ObjectStore>, db: &str, hist: u64, want: u64) -> Result<usize> {
    let segs = index(store, db, hist, want).await?;
    let horizon = (now_ms() / 1000) as u64 - RETENTION_SECS.load(Relaxed).min(now_ms() as u64 / 1000);
    let cap = RETENTION_BYTES.load(Relaxed);
    let mut total: u64 = segs.iter().map(|s| s.size).sum();
    let mut n = 0;
    for s in &segs {
        let old = (s.name.last_t >> 32) < horizon;
        if !(old || total > cap) {
            break;
        }
        let path = Path::from(format!("{}{}", hist_dir(db, hist), s.name.file()));
        match store.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
        SEGMENTS_DELETED.fetch_add(1, Relaxed);
        index_remove(db, hist, &s.name);
        total -= s.size;
        n += 1;
        crash_at("trim.after_first_delete");
    }
    Ok(n)
}

/// Every [`SWEEP_SECS`]: segments of any database whose newest event is past the age limit are deleted (databases
/// nobody has open included, so an erased customer's events leave the store in time).
pub async fn sweep(store: &Arc<dyn ObjectStore>) -> Result<usize> {
    let horizon = (now_ms() / 1000) as u64 - RETENTION_SECS.load(Relaxed).min(now_ms() as u64 / 1000);
    SEGMENT_LISTS.fetch_add(1, Relaxed);
    let metas: Vec<object_store::ObjectMeta> = store.list(Some(&Path::from("changes/"))).try_collect().await?;
    let mut n = 0;
    for m in metas {
        let Some(s) = m.location.filename().and_then(SegName::parse) else { continue };
        if (s.last_t >> 32) < horizon {
            match store.delete(&m.location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
            SEGMENTS_DELETED.fetch_add(1, Relaxed);
            let parts: Vec<String> = m.location.parts().map(|p| p.as_ref().to_string()).collect();
            if parts.len() == 4 {
                if let Ok(h) = u64::from_str_radix(&parts[2], 16) {
                    index_remove(&parts[1], h, &s);
                }
            }
            n += 1;
        }
    }
    Ok(n)
}

/// The background archiver and sweeper of this process.
pub fn spawn(dbs: &Arc<crate::dbs::Dbs>) {
    if !enabled() {
        return;
    }
    let me = Arc::clone(dbs);
    tokio::spawn(async move {
        let sem = Arc::new(tokio::sync::Semaphore::new(2));
        // one archive at a time per database; it goes on while the database's tail is due (a busy database's tail
        // must not grow faster than it is archived)
        let busy: Arc<Mutex<std::collections::HashSet<String>>> = Arc::default();
        let mut last_sweep = std::time::Instant::now();
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(ARCHIVE_CHECK_MS.load(Relaxed))).await;
            for name in me.chg_due() {
                if busy.lock().unwrap().contains(&name) {
                    continue;
                }
                let Ok(p) = Arc::clone(&sem).try_acquire_owned() else { break };
                busy.lock().unwrap().insert(name.clone());
                let (d, busy) = (Arc::clone(&me), Arc::clone(&busy));
                tokio::spawn(async move {
                    let _p = p;
                    loop {
                        match archive(&d, &name, false).await {
                            Ok(true) => continue,
                            Ok(false) => break,
                            Err(e) => {
                                ARCHIVE_ERRORS.fetch_add(1, Relaxed);
                                eprintln!("change history: archive of {name} failed: {e:#}");
                                break;
                            }
                        }
                    }
                    busy.lock().unwrap().remove(&name);
                });
            }
            if last_sweep.elapsed().as_secs() >= SWEEP_SECS.load(Relaxed) {
                last_sweep = std::time::Instant::now();
                let store = me.store();
                tokio::spawn(async move {
                    if let Err(e) = sweep(&store).await {
                        eprintln!("change history: sweep failed: {e:#}");
                    }
                });
            }
        }
    });
}

// ---- durable reads ---------------------------------------------------------------------------------------------------

fn lost(msg: String) -> crate::error::MongoError {
    crate::changes::HISTORY_LOST.fetch_add(1, Relaxed);
    crate::error::MongoError::new(286, "ChangeStreamHistoryLost", msg)
}

/// What a durable read found.
pub enum Durable {
    /// Rows after the position (contiguous from it), maybe none yet.
    Rows(Vec<Row>),
    /// The database has another history now (dropped and recreated, or its history was reset): a position of `hist` is
    /// no point of it.
    Changed,
    /// The database does not exist.
    Absent,
}

/// The history id and next position of `db` (its head, loaded if needed), `None` when the database does not exist.
pub async fn head_of(dbs: &Arc<crate::dbs::Dbs>, db: &str) -> Result<Option<Head>, crate::error::MongoError> {
    let mut g = dbs.lock(db).await;
    Ok(g.chg_head().await.map_err(crate::error::MongoError::from)?.cloned())
}

/// Durable events of history `hist` of `db` after position `after`: from segments while `after` is archived, then from the
/// tail. At most about `limit` rows. Fails with 286 when the position is no longer held (trimmed), or a position is missing.
pub async fn read(dbs: &Arc<crate::dbs::Dbs>, db: &str, hist: u64, after: u64, limit: usize) -> Result<Durable, crate::error::MongoError> {
    use crate::error::MongoError;
    DURABLE_READS.fetch_add(1, Relaxed);
    let store = dbs.store();
    for _ in 0..4 {
        let Some(h) = head_of(dbs, db).await? else { return Ok(Durable::Absent) };
        if h.hist != hist {
            return Ok(Durable::Changed);
        }
        if after + 1 >= h.next {
            return Ok(Durable::Rows(Vec::new()));
        }
        if after < h.archived {
            let segs = index(&store, db, hist, after + 1).await.map_err(MongoError::from)?;
            if let Some(o) = segs.first() {
                if after + 1 < o.name.first {
                    return Err(lost(format!("Resume of change stream was not possible: position {} of database {db} is older than its retained change history (oldest kept: {}, retention {} s / {} MB)", after + 1, o.name.first, RETENTION_SECS.load(Relaxed), RETENTION_BYTES.load(Relaxed) >> 20)));
                }
            }
            let Some(s) = segs.iter().find(|s| s.name.first <= after + 1 && after < s.name.last) else {
                if segs.is_empty() {
                    return Err(lost(format!("Resume of change stream was not possible: position {} of database {db} is older than its retained change history (no segment kept)", after + 1)));
                }
                return Err(lost(format!("Resume of change stream was not possible: position {} of database {db} is not in its change history", after + 1)));
            };
            let rows = load_segment(&store, db, hist, &s.name).await.map_err(MongoError::from)?;
            let out: Vec<Row> = rows.iter().filter(|r| r.pos > after).take(limit.max(1)).cloned().collect();
            return Ok(Durable::Rows(out));
        }
        // the tail
        let raw = {
            let mut g = dbs.lock(db).await;
            g.chg_scan(after + 1, limit.max(1), 16 << 20).await.map_err(MongoError::from)?
        };
        if raw.is_empty() {
            return Ok(Durable::Rows(Vec::new()));
        }
        if raw[0].0 != after + 1 {
            // archived meanwhile (the rows moved to a segment): read again
            continue;
        }
        let mut out = Vec::with_capacity(raw.len());
        for (i, (p, b)) in raw.iter().enumerate() {
            if *p != after + 1 + i as u64 {
                break;
            }
            out.push(decode(db, b).map_err(MongoError::from)?);
        }
        return Ok(Durable::Rows(out));
    }
    Err(lost(format!("Resume of change stream was not possible: position {} of database {db} moved while it was read", after + 1)))
}

/// Check at open that position `after` of history `hist` is still held (as mongod checks a resume token at once).
pub async fn check_position(dbs: &Arc<crate::dbs::Dbs>, db: &str, hist: u64, after: u64) -> Result<(), crate::error::MongoError> {
    let Some(h) = head_of(dbs, db).await? else { return Ok(()) };
    if h.hist != hist || after >= h.archived {
        return Ok(());
    }
    let segs = index(&dbs.store(), db, hist, after + 1).await.map_err(crate::error::MongoError::from)?;
    match segs.first() {
        Some(o) if after + 1 >= o.name.first => Ok(()),
        _ => Err(lost(format!("Resume of change stream was not possible: position {} of database {db} is older than its retained change history", after + 1))),
    }
}

/// The position just before the first durable event of `db` at or after cluster time `ts` (a stream started at `ts` reads
/// from there; events older than `ts` read on the way are dropped by the stream). 286 when `ts` is at or before an event
/// that is no longer held.
pub async fn position_at(dbs: &Arc<crate::dbs::Dbs>, db: &str, ts: Timestamp) -> Result<Option<(u64, u64)>, crate::error::MongoError> {
    use crate::error::MongoError;
    let Some(h) = head_of(dbs, db).await? else { return Ok(None) };
    let t = pack(ts);
    let store = dbs.store();
    if h.archived > 0 {
        let segs = list(&store, db, h.hist).await.map_err(MongoError::from)?;
        match segs.first() {
            Some(o) if o.name.first > 1 && t <= o.name.prev_t => {
                return Err(lost(format!("Resume of change stream was not possible: startAtOperationTime {{{}, {}}} is older than the retained change history of {db}", ts.time, ts.increment)));
            }
            None if t <= h.archived_t => {
                return Err(lost(format!("Resume of change stream was not possible: startAtOperationTime {{{}, {}}} is older than the retained change history of {db}", ts.time, ts.increment)));
            }
            _ => {}
        }
        for s in &segs {
            if s.name.last_t >= t {
                return Ok(Some((h.hist, s.name.first - 1)));
            }
        }
    }
    // the tail (cluster times grow with positions)
    let mut from = h.archived + 1;
    loop {
        let raw = {
            let mut g = dbs.lock(db).await;
            g.chg_scan(from, 4096, 8 << 20).await.map_err(MongoError::from)?
        };
        if raw.is_empty() {
            return Ok(Some((h.hist, from - 1)));
        }
        for (p, b) in &raw {
            let r = decode(db, b).map_err(MongoError::from)?;
            if pack(r.time) >= t {
                return Ok(Some((h.hist, p - 1)));
            }
        }
        from = raw.last().unwrap().0 + 1;
    }
}
