//! Derived documents: agent checkpoints stored as a thread message log inside sealed runs (fenvix-brain
//! `prompts/P-c2-delta-log.md`; the measure that chose this layout is `sessions/2026-11-09-checkpoint-delta-local`).
//!
//! An agent framework's checkpointer (LangGraph's MongoDB saver) stores the whole message list of a thread in every
//! checkpoint document, so a thread's bytes grow with the square of its length. At reseal only (cold groups, where the
//! rewrite happens anyway), for the collections registered as agent checkpoint collections (customer key `thread_id`,
//! so one customer is one thread), the compactor rewrites a thread's documents as:
//!
//! - **message rows** (run envelope 3, [`runfmt::PART`]): every distinct array element of the documents' msgpack
//!   payloads (a message, in practice) once per thread, under `{customer prefix}\0fxm/{index u32 BE}`. That key sorts
//!   before every other key of the customer (a collection name never starts with a 0 byte), so a thread's log sits
//!   right in front of its documents, in the same run.
//! - **derived documents** (run envelope 2, [`runfmt::DERIVED`]): each document as a skeleton, the list of its byte
//!   ranges: literal bytes, or a reference to a message row. The ranges are cut from the stored BSON bytes (a binary
//!   field holding msgpack, then each element of its arrays), never re-encoded, so the concatenation is the stored
//!   document by construction. The encoder rebuilds every skeleton and compares it with the original before it writes
//!   it (a difference keeps the document plain), and the skeleton carries the document's length and CRC32C, which the
//!   reader checks: a wrong reference is an error, never a wrong document.
//!
//! Reads: the merged reader rebuilds a derived document from the message rows of the same run before it leaves the
//! server ([`rebuild`]); message rows are never returned. Everything above `fx` (the server, export, PITR, forks) sees
//! the documents as they were written.
//!
//! Release rule: envelopes 2 and 3 exist only in run format v2, which a v1 reader refuses. This release reads them
//! always (E0); it writes them only with [`WRITE`] on (`--sealed-derive`, the E1 release flag). A reseal with the flag
//! off turns every derived document back into a plain one (a rollback from E1 within the window). Young data (WAL, L0,
//! delta runs) stays as the saver wrote it until its group's reseal.

use crate::runfmt::{self, DERIVED, LIVE, PART};
use anyhow::{anyhow, bail, ensure, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;

/// The E1 release flag of derived documents: reseals write them (`--sealed-derive`). Off: reseals write plain documents
/// (and turn derived ones back into plain ones).
pub static WRITE: AtomicBool = AtomicBool::new(false);
/// The reader checks the length and CRC32C of every rebuilt document (always on; tests turn it off to show that the
/// byte-for-byte comparison also catches a planted bug).
pub static CHECK_CRC: AtomicBool = AtomicBool::new(true);
/// Planted bug for the tests (0: none). 1: every skeleton with two or more references points its second reference at
/// the message after the right one (after the encoder's own check, as a bug in the serialisation would).
pub static PLANT: AtomicUsize = AtomicUsize::new(0);
// Review (fenvix-brain prompts/R-review-c2-delta-log.md) adds three more plants, also after the encoder's own check:
// 2: the first literal range of a skeleton is one byte short; 3: a thread's message rows hold the messages of the thread
// encoded before it (its references resolve to another thread's messages); 4: a thread's last message row is not
// written (a skeleton refers to a message row that is gone).
/// Review plant 3: the message table of the thread encoded last.
pub static REVIEW_PREV: std::sync::Mutex<Vec<Vec<u8>>> = std::sync::Mutex::new(Vec::new());
/// Tests: the reseal fails (as a crash would) when it is about to write its N-th encoded customer (0: never). The run
/// being written is then never completed, so nothing of it becomes visible.
pub static FAIL_AT_UNIT: AtomicU64 = AtomicU64::new(0);

/// The read side's spec (the server sets it from `--agent-checkpoints`): a point read of a checkpoint document in a
/// run with derived rows fetches the thread's message blocks together with the document's block, in one GET. Only
/// the number of GETs depends on it, never an answer. None: no prefetch.
static READ_SPEC: std::sync::RwLock<Option<Arc<dyn Spec>>> = std::sync::RwLock::new(None);
/// At most this many blocks are fetched ahead (4 MiB at 256 KiB blocks); a longer thread reads in two trips.
pub const PREFETCH_MAX_BLOCKS: usize = 16;

pub fn set_read_spec(s: Option<Arc<dyn Spec>>) {
    *READ_SPEC.write().unwrap() = s;
}

/// Blocks first..=last of `run` to fetch with a point read of `key`: from the block of the thread's first message row
/// to the key's block, when the run holds derived rows and the key is a checkpoint document.
pub fn prefetch_span(run: &runfmt::Run, key: &[u8]) -> Option<(usize, usize)> {
    if run.derived_rows == 0 {
        return None;
    }
    if let Some(r) = run.customers.customer_of(key) {
        if !run.filter.may_contain(&key[r]) {
            return None;
        }
    }
    let spec = READ_SPEC.read().unwrap().clone()?;
    if !spec.is_checkpoint_doc(key) {
        return None;
    }
    let p = spec.customer_prefix(key)?;
    let first = run.fence.count_below(&msg_key(&key[..p], 0), true).max(1) - 1;
    let last = run.fence.count_below(key, true).checked_sub(1)?;
    (last >= first && last - first < PREFETCH_MAX_BLOCKS).then_some((first, last))
}

/// Counters (diagnostics, serverStatus).
pub static ENCODED_DOCS: AtomicU64 = AtomicU64::new(0);
pub static PLAIN_DOCS: AtomicU64 = AtomicU64::new(0);
pub static MESSAGE_ROWS: AtomicU64 = AtomicU64::new(0);
pub static REFS: AtomicU64 = AtomicU64::new(0);
pub static FALLBACKS: AtomicU64 = AtomicU64::new(0);
pub static REBUILDS: AtomicU64 = AtomicU64::new(0);
pub static REBUILD_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static RESEAL_REBUILDS: AtomicU64 = AtomicU64::new(0);

/// The middle of a message row's key: `{customer prefix}` + this + `{index u32 BE}`.
pub const MSG_TAG: &[u8] = b"\x00fxm/";
/// Array elements shorter than this stay literal (a reference costs 1-3 bytes and a row about 30).
pub const MIN_PART: usize = 48;
/// A customer whose rows pass this many raw bytes in one reseal is written plain (the buffer is bounded).
pub const MAX_UNIT_BYTES: usize = 256 << 20;

/// Which keys belong to registered agent checkpoint collections (set per database by the server).
pub trait Spec: Send + Sync {
    /// The length of the customer prefix (`u/{component}`) of a key of the customer space, else None.
    fn customer_prefix(&self, key: &[u8]) -> Option<usize>;
    /// Whether `key` is a document of a registered checkpoint collection (customer key `thread_id`).
    fn is_checkpoint_doc(&self, key: &[u8]) -> bool;
}

/// A [`Spec`] for the compactor's knobs (cloneable, printable).
#[derive(Clone)]
pub struct SpecRef(pub Arc<dyn Spec>);

impl std::fmt::Debug for SpecRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("derive::Spec")
    }
}

/// The spec of keys in the runfmt test layout (`u/{length byte}{customer}{collection}/d/{id}`), for tests and benches.
pub struct LenSpec {
    /// Registered collections as they appear in the key (`{collection}/d/` follows the customer).
    pub colls: Vec<Vec<u8>>,
}

impl Spec for LenSpec {
    fn customer_prefix(&self, key: &[u8]) -> Option<usize> {
        runfmt::CustomerKind::V2.customer_of(key).map(|r| r.end)
    }
    fn is_checkpoint_doc(&self, key: &[u8]) -> bool {
        let Some(p) = self.customer_prefix(key) else { return false };
        let rest = &key[p..];
        self.colls.iter().any(|c| rest.len() > c.len() + 3 && rest.starts_with(c) && &rest[c.len()..c.len() + 3] == b"/d/")
    }
}

// ---- keys

/// The key of message row `idx` of the customer with prefix `p`.
pub fn msg_key(p: &[u8], idx: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(p.len() + MSG_TAG.len() + 4);
    k.extend_from_slice(p);
    k.extend_from_slice(MSG_TAG);
    k.extend_from_slice(&idx.to_be_bytes());
    k
}

/// (customer prefix, index) of a message row's key.
pub fn parse_msg_key(k: &[u8]) -> Result<(&[u8], u32)> {
    let n = k.len();
    ensure!(n >= MSG_TAG.len() + 4 && &k[n - 4 - MSG_TAG.len()..n - 4] == MSG_TAG, "corrupt derived document: a message row with key {}", hexs(k));
    Ok((&k[..n - 4 - MSG_TAG.len()], u32::from_be_bytes(k[n - 4..].try_into()?)))
}

fn hexs(k: &[u8]) -> String {
    k.iter().map(|b| format!("{b:02x}")).collect()
}

// ---- varints

fn put_var(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn get_var(d: &[u8], p: &mut usize) -> Result<u64> {
    let mut v = 0u64;
    let mut s = 0;
    loop {
        let b = *d.get(*p).ok_or_else(|| anyhow!("corrupt derived document: short varint"))?;
        *p += 1;
        ensure!(s < 64, "corrupt derived document: long varint");
        v |= ((b & 0x7F) as u64) << s;
        if b < 0x80 {
            return Ok(v);
        }
        s += 7;
    }
}

// ---- cutting a document into byte ranges

/// A byte range of a document: `(start, end, is a message)`.
type Cut = (usize, usize, bool);

/// The message ranges of a BSON document: every element of every msgpack array inside a top-level binary field whose
/// payload is exactly one msgpack value, when it is at least [`MIN_PART`] bytes. Sorted, disjoint. A document that is
/// not well-formed BSON gives no ranges (it stays literal).
pub fn message_ranges(doc: &[u8]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    if bson_binaries(doc, &mut |a, b| {
        let payload = &doc[a..b];
        let mut arr = Vec::new();
        if let Ok(end) = mp_value(payload, 0, 0, &mut arr) {
            if end == payload.len() {
                out.extend(arr.into_iter().filter(|(x, y)| y - x >= MIN_PART).map(|(x, y)| (a + x, a + y)));
            }
        }
    })
    .is_err()
    {
        return Vec::new();
    }
    out.sort();
    // elements of one array never overlap and nested arrays are not entered once an element is taken (mp_value takes
    // the outermost arrays' elements only), so the ranges are disjoint; check anyway
    if out.windows(2).any(|w| w[0].1 > w[1].0) {
        return Vec::new();
    }
    out
}

/// Calls `f(start, end)` with the payload range of every top-level binary field of a BSON document. The stored value
/// may carry bytes after the document (the server's natural-order trailer, `natural.rs`): they stay literal.
fn bson_binaries(d: &[u8], f: &mut dyn FnMut(usize, usize)) -> Result<()> {
    ensure!(d.len() >= 5, "short");
    let n = i32::from_le_bytes(d[..4].try_into()?) as usize;
    ensure!(n >= 5 && n <= d.len() && d[n - 1] == 0, "length");
    let d = &d[..n];
    let mut p = 4;
    while p < n - 1 {
        let t = d[p];
        p += 1;
        let z = d[p..].iter().position(|&b| b == 0).ok_or_else(|| anyhow!("name"))?;
        p += z + 1;
        let i32_at = |p: usize| -> Result<usize> {
            ensure!(p + 4 <= n, "short");
            let v = i32::from_le_bytes(d[p..p + 4].try_into()?);
            ensure!(v >= 0, "negative");
            Ok(v as usize)
        };
        let cstr = |p: usize| -> Result<usize> { Ok(p + d[p..].iter().position(|&b| b == 0).ok_or_else(|| anyhow!("cstring"))? + 1) };
        p = match t {
            0x01 | 0x09 | 0x11 | 0x12 => p + 8,
            0x02 | 0x0D | 0x0E => p + 4 + i32_at(p)?,
            0x03 | 0x04 | 0x0F => p + i32_at(p)?,
            0x05 => {
                let len = i32_at(p)?;
                let a = p + 5;
                ensure!(a + len <= n - 1, "binary");
                f(a, a + len);
                a + len
            }
            0x06 | 0x0A | 0xFF | 0x7F => p,
            0x07 => p + 12,
            0x08 => p + 1,
            0x0B => cstr(cstr(p)?)?,
            0x0C => p + 4 + i32_at(p)? + 12,
            0x10 => p + 4,
            0x13 => p + 16,
            _ => bail!("type {t}"),
        };
        ensure!(p < n, "element past the end");
    }
    ensure!(p == n - 1, "trailing");
    Ok(())
}

/// The end of the msgpack value at `p`; the element ranges of every outermost array met on the way go to `arr`.
fn mp_value(d: &[u8], p: usize, depth: usize, arr: &mut Vec<(usize, usize)>) -> Result<usize> {
    ensure!(depth < 64, "deep");
    let b = *d.get(p).ok_or_else(|| anyhow!("short"))?;
    let be = |p: usize, n: usize| -> Result<usize> {
        ensure!(p + n <= d.len(), "short");
        Ok(d[p..p + n].iter().fold(0usize, |a, &x| (a << 8) | x as usize))
    };
    let skip = |end: usize| -> Result<usize> {
        ensure!(end <= d.len(), "short");
        Ok(end)
    };
    let (count, kind, at) = match b {
        0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => return Ok(p + 1),
        0x80..=0x8f => ((b & 0x0f) as usize, 'm', p + 1),
        0x90..=0x9f => ((b & 0x0f) as usize, 'a', p + 1),
        0xa0..=0xbf => return skip(p + 1 + (b & 0x1f) as usize),
        0xc4 | 0xd9 => return skip(p + 2 + be(p + 1, 1)?),
        0xc5 | 0xda => return skip(p + 3 + be(p + 1, 2)?),
        0xc6 | 0xdb => return skip(p + 5 + be(p + 1, 4)?),
        0xc7 => return skip(p + 3 + be(p + 1, 1)?),
        0xc8 => return skip(p + 4 + be(p + 1, 2)?),
        0xc9 => return skip(p + 6 + be(p + 1, 4)?),
        0xca => return skip(p + 5),
        0xcb => return skip(p + 9),
        0xcc | 0xd0 => return skip(p + 2),
        0xcd | 0xd1 => return skip(p + 3),
        0xce | 0xd2 => return skip(p + 5),
        0xcf | 0xd3 => return skip(p + 9),
        0xd4 => return skip(p + 3),
        0xd5 => return skip(p + 4),
        0xd6 => return skip(p + 6),
        0xd7 => return skip(p + 10),
        0xd8 => return skip(p + 18),
        0xdc => (be(p + 1, 2)?, 'a', p + 3),
        0xdd => (be(p + 1, 4)?, 'a', p + 5),
        0xde => (be(p + 1, 2)?, 'm', p + 3),
        0xdf => (be(p + 1, 4)?, 'm', p + 5),
        0xc1 => bail!("reserved byte"),
    };
    let mut q = at;
    if kind == 'a' {
        let mut els = Vec::with_capacity(count.min(4096));
        let mut ignore = Vec::new();
        for _ in 0..count {
            let e = mp_value(d, q, depth + 1, &mut ignore)?;
            els.push((q, e));
            q = e;
        }
        // an array whose elements are all small is not a message list: look inside its elements instead
        if els.iter().any(|(x, y)| y - x >= MIN_PART) {
            arr.extend(els);
        } else {
            arr.extend(ignore);
        }
    } else {
        for _ in 0..count * 2 {
            q = mp_value(d, q, depth + 1, arr)?;
        }
    }
    Ok(q)
}

// ---- skeletons

/// The skeleton of a document (envelope 2 without its first byte): customer prefix length, document length, CRC32C,
/// then the ranges: `varint (len << 1)` + literal bytes, or `varint (index << 1 | 1)` for a message row.
pub fn skeleton(plen: usize, doc: &[u8], cuts: &[Cut], index_of: &dyn Fn(&[u8]) -> u32) -> Vec<u8> {
    let mut s = Vec::with_capacity(64 + doc.len() / 8);
    s.push(DERIVED);
    put_var(&mut s, plen as u64);
    put_var(&mut s, doc.len() as u64);
    s.extend(crc32c::crc32c(doc).to_le_bytes());
    put_var(&mut s, cuts.len() as u64);
    for &(a, b, m) in cuts {
        if m {
            put_var(&mut s, ((index_of(&doc[a..b]) as u64) << 1) | 1);
        } else {
            put_var(&mut s, ((b - a) as u64) << 1);
            s.extend_from_slice(&doc[a..b]);
        }
    }
    s
}

/// A parsed skeleton.
pub struct Skel<'a> {
    pub plen: usize,
    pub len: usize,
    pub crc: u32,
    /// literal bytes, or a message index
    pub parts: Vec<Result<&'a [u8], u32>>,
}

pub fn parse_skeleton(env: &[u8]) -> Result<Skel<'_>> {
    ensure!(env.first() == Some(&DERIVED), "not a derived document");
    let mut p = 1;
    let plen = get_var(env, &mut p)? as usize;
    let len = get_var(env, &mut p)? as usize;
    ensure!(p + 4 <= env.len(), "corrupt derived document: short");
    let crc = u32::from_le_bytes(env[p..p + 4].try_into()?);
    p += 4;
    let n = get_var(env, &mut p)? as usize;
    ensure!(n <= env.len(), "corrupt derived document: {n} ranges");
    let mut parts = Vec::with_capacity(n);
    for _ in 0..n {
        let v = get_var(env, &mut p)?;
        if v & 1 == 1 {
            parts.push(Err(u32::try_from(v >> 1).map_err(|_| anyhow!("corrupt derived document: index"))?));
        } else {
            let l = (v >> 1) as usize;
            ensure!(p + l <= env.len(), "corrupt derived document: literal past the end");
            parts.push(Ok(&env[p..p + l]));
            p += l;
        }
    }
    ensure!(p == env.len(), "corrupt derived document: {} trailing bytes", env.len() - p);
    Ok(Skel { plen, len, crc, parts })
}

/// The document of a skeleton, with message `i` from `msg(i)`; checks length and CRC (unless the test switch is off).
pub fn assemble(sk: &Skel<'_>, msg: &dyn Fn(u32) -> Option<Vec<u8>>) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(sk.len);
    for p in &sk.parts {
        match p {
            Ok(l) => out.extend_from_slice(l),
            Err(i) => out.extend(msg(*i).ok_or_else(|| anyhow!("corrupt derived document: message {i} is missing"))?),
        }
    }
    if CHECK_CRC.load(Relaxed) {
        ensure!(out.len() == sk.len, "corrupt derived document: rebuilt {} bytes, the skeleton says {}", out.len(), sk.len);
        ensure!(crc32c::crc32c(&out) == sk.crc, "corrupt derived document: checksum of the rebuilt document");
    }
    Ok(out)
}

// ---- the reseal side

/// One output row of the encoder.
pub type OutRow = (Vec<u8>, u64, Vec<u8>);

/// Rows ready to write. `atomic`: one customer's message rows and documents, which a split must not cut; a split
/// before it starts the new group at the customer's prefix (so later reseals still see the whole customer inside).
pub struct Out {
    pub rows: Vec<OutRow>,
    pub atomic: bool,
    pub prefix: Option<Vec<u8>>,
}

struct Unit {
    prefix: Vec<u8>,
    rows: Vec<OutRow>,
    bytes: usize,
    has_ck: bool,
}

/// Turns the winning rows of a reseal (key order, no deletes) into the rows to write. Derived documents of the old
/// sealed run are rebuilt first (from the message rows that precede them); then, with [`WRITE`] on and a spec, each
/// customer of a registered checkpoint collection whose whole key range lies in the reseal's range is re-encoded.
pub struct Encoder {
    spec: Option<Arc<dyn Spec>>,
    write: bool,
    lo: Vec<u8>,
    hi: Option<Vec<u8>>,
    /// message rows of the old sealed run, of the customer `old_prefix`
    old_prefix: Vec<u8>,
    old: HashMap<u32, Vec<u8>>,
    unit: Option<Unit>,
    /// customer whose buffer overflowed: its later rows pass plain
    overflow: Option<Vec<u8>>,
    pub stats: EncStats,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct EncStats {
    pub docs_in: u64,
    pub encoded: u64,
    pub plain_ck: u64,
    pub messages: u64,
    pub refs: u64,
    pub fallbacks: u64,
    pub rebuilt: u64,
    pub doc_bytes: u64,
    pub out_bytes: u64,
}

impl Encoder {
    /// `lo`, `hi`: the reseal's key range (a customer is encoded only when its whole range lies inside).
    pub fn new(spec: Option<Arc<dyn Spec>>, lo: &[u8], hi: Option<&[u8]>) -> Encoder {
        Encoder {
            write: WRITE.load(Relaxed) && spec.is_some(),
            spec,
            lo: lo.to_vec(),
            hi: hi.map(|h| h.to_vec()),
            old_prefix: Vec::new(),
            old: HashMap::new(),
            unit: None,
            overflow: None,
            stats: EncStats::default(),
        }
    }

    /// Whether a group boundary at `lo` cuts the range of a customer (then a reseal takes the neighbouring group along,
    /// so that the customer's rows meet in one run and can be encoded).
    pub fn cuts_customer(spec: &dyn Spec, lo: &[u8]) -> bool {
        spec.customer_prefix(lo).is_some_and(|n| n < lo.len())
    }

    /// Whether this encoder writes derived rows (the run must then be opened for them).
    pub fn writes(&self) -> bool {
        self.write
    }

    /// The plain envelope of a winning row: message rows are kept aside (None), derived documents rebuilt.
    fn resolve(&mut self, key: &[u8], env: Vec<u8>) -> Result<Option<Vec<u8>>> {
        match env.first() {
            Some(&PART) => {
                let (p, i) = parse_msg_key(key)?;
                if p != &self.old_prefix[..] {
                    self.old_prefix = p.to_vec();
                    self.old.clear();
                }
                self.old.insert(i, env[1..].to_vec());
                Ok(None)
            }
            Some(&DERIVED) => {
                let sk = parse_skeleton(&env)?;
                ensure!(sk.plen <= key.len() && key[..sk.plen] == self.old_prefix[..], "corrupt derived document: its message rows are not in front of it ({})", hexs(key));
                let old = &self.old;
                let doc = assemble(&sk, &|i| old.get(&i).cloned())?;
                self.stats.rebuilt += 1;
                RESEAL_REBUILDS.fetch_add(1, Relaxed);
                let mut e = Vec::with_capacity(doc.len() + 1);
                e.push(LIVE);
                e.extend(doc);
                Ok(Some(e))
            }
            _ => Ok(Some(env)),
        }
    }

    fn inside(&self, p: &[u8]) -> bool {
        let succ = runfmt::prefix_successor(p);
        p >= &self.lo[..]
            && match (&self.hi, succ) {
                (None, _) => true,
                (Some(h), Some(s)) => s[..] <= h[..],
                (Some(_), None) => false,
            }
    }

    /// One winning row of the reseal (key order; a live, deleted-free row of any envelope). Returns what to write now.
    pub fn push(&mut self, key: &[u8], seq: u64, env: Vec<u8>) -> Result<Vec<Out>> {
        let Some(env) = self.resolve(key, env)? else { return Ok(Vec::new()) };
        let mut out = Vec::new();
        if !self.write {
            out.push(Out { rows: vec![(key.to_vec(), seq, env)], atomic: false, prefix: None });
            return Ok(out);
        }
        let spec = self.spec.clone().expect("write implies a spec");
        let prefix = spec.customer_prefix(key).map(|n| &key[..n]);
        if let Some(u) = &self.unit {
            if prefix != Some(&u.prefix[..]) {
                out.extend(self.flush()?);
            }
        }
        let Some(p) = prefix else {
            out.push(Out { rows: vec![(key.to_vec(), seq, env)], atomic: false, prefix: None });
            return Ok(out);
        };
        if self.overflow.as_deref() == Some(p) || !self.inside(p) {
            out.push(Out { rows: vec![(key.to_vec(), seq, env)], atomic: false, prefix: None });
            return Ok(out);
        }
        let ck = spec.is_checkpoint_doc(key);
        let u = self.unit.get_or_insert_with(|| Unit { prefix: p.to_vec(), rows: Vec::new(), bytes: 0, has_ck: false });
        u.bytes += key.len() + env.len() + 8;
        u.has_ck |= ck;
        u.rows.push((key.to_vec(), seq, env));
        if u.bytes > MAX_UNIT_BYTES {
            // too big to hold: write it plain from here on (key order is kept: these rows precede every later one)
            let u = self.unit.take().unwrap();
            self.overflow = Some(u.prefix);
            out.push(Out { rows: u.rows, atomic: false, prefix: None });
        }
        Ok(out)
    }

    /// What is still buffered (end of the reseal).
    pub fn finish(&mut self) -> Result<Vec<Out>> {
        self.flush()
    }

    fn flush(&mut self) -> Result<Vec<Out>> {
        let Some(u) = self.unit.take() else { return Ok(Vec::new()) };
        if !u.has_ck {
            return Ok(vec![Out { rows: u.rows, atomic: false, prefix: None }]);
        }
        let spec = self.spec.clone().expect("write implies a spec");
        let f = FAIL_AT_UNIT.load(Relaxed);
        if f > 0 {
            if f == 1 {
                FAIL_AT_UNIT.store(0, Relaxed);
                bail!("planted crash in the middle of a reseal");
            }
            FAIL_AT_UNIT.store(f - 1, Relaxed);
        }
        Ok(vec![encode_customer(&*spec, &u.prefix, u.rows, &mut self.stats)?])
    }
}

/// Re-encodes one customer's rows (key order, plain envelopes): its checkpoint documents become skeletons over the
/// customer's message rows, which come first.
pub fn encode_customer(spec: &dyn Spec, prefix: &[u8], rows: Vec<OutRow>, st: &mut EncStats) -> Result<Out> {
    let plant = PLANT.load(Relaxed);
    // the messages, numbered in order of first use; seq of a message row = the smallest seq of a document using it
    let mut ids: HashMap<Vec<u8>, u32> = HashMap::new();
    let mut msgs: Vec<(Vec<u8>, u64)> = Vec::new();
    let mut cuts_of: Vec<Option<Vec<Cut>>> = Vec::with_capacity(rows.len());
    for (k, seq, env) in &rows {
        if env.first() != Some(&LIVE) || !spec.is_checkpoint_doc(k) {
            cuts_of.push(None);
            continue;
        }
        let doc = &env[1..];
        st.docs_in += 1;
        st.doc_bytes += doc.len() as u64;
        let ms = message_ranges(doc);
        if ms.is_empty() {
            cuts_of.push(None);
            continue;
        }
        let mut cuts = Vec::with_capacity(ms.len() * 2 + 1);
        let mut at = 0;
        for (a, b) in ms {
            if a > at {
                cuts.push((at, a, false));
            }
            cuts.push((a, b, true));
            let m = &doc[a..b];
            match ids.get(m) {
                Some(&i) => msgs[i as usize].1 = msgs[i as usize].1.min(*seq),
                None => {
                    ids.insert(m.to_vec(), msgs.len() as u32);
                    msgs.push((m.to_vec(), *seq));
                }
            }
            at = b;
        }
        if at < doc.len() {
            cuts.push((at, doc.len(), false));
        }
        cuts_of.push(Some(cuts));
    }
    let mut out: Vec<OutRow> = Vec::with_capacity(msgs.len() + rows.len());
    let prev = if plant == 3 { std::mem::replace(&mut *REVIEW_PREV.lock().unwrap(), msgs.iter().map(|m| m.0.clone()).collect()) } else { Vec::new() };
    for (i, (m, seq)) in msgs.iter().enumerate() {
        if plant == 4 && i + 1 == msgs.len() {
            continue; // review plant 4: the last message row is missing
        }
        let m = if plant == 3 && i < prev.len() { &prev[i] } else { m }; // review plant 3
        let mut e = Vec::with_capacity(m.len() + 1);
        e.push(PART);
        e.extend_from_slice(m);
        out.push((msg_key(prefix, i as u32), *seq, e));
    }
    st.messages += msgs.len() as u64;
    MESSAGE_ROWS.fetch_add(msgs.len() as u64, Relaxed);
    let index_of = |m: &[u8]| ids[m];
    for ((k, seq, env), cuts) in rows.into_iter().zip(cuts_of) {
        let Some(cuts) = cuts else {
            if env.first() == Some(&LIVE) && spec.is_checkpoint_doc(&k) {
                st.plain_ck += 1;
                PLAIN_DOCS.fetch_add(1, Relaxed);
            }
            out.push((k, seq, env));
            continue;
        };
        let doc = &env[1..];
        let mut sk = skeleton(prefix.len(), doc, &cuts, &index_of);
        // the encoder's own check: the skeleton rebuilds the document exactly, else the document stays plain
        let ok = parse_skeleton(&sk).and_then(|s| assemble(&s, &|i| msgs.get(i as usize).map(|m| m.0.clone()))).map(|d| d == doc).unwrap_or(false);
        if !ok {
            st.fallbacks += 1;
            FALLBACKS.fetch_add(1, Relaxed);
            out.push((k, seq, env));
            continue;
        }
        let nrefs = cuts.iter().filter(|c| c.2).count();
        if plant == 1 && nrefs >= 2 && msgs.len() >= 2 {
            // planted bug: the second reference points at the next message
            let n = std::cell::Cell::new(0u32);
            let nmsg = msgs.len() as u32;
            sk = skeleton(prefix.len(), doc, &cuts, &|m| {
                n.set(n.get() + 1);
                let i = ids[m];
                if n.get() == 2 {
                    (i + 1) % nmsg
                } else {
                    i
                }
            });
        }
        if plant == 2 {
            // review plant 2: the first literal range of at least 2 bytes ends one byte early
            if let Some(j) = cuts.iter().position(|c| !c.2 && c.1 - c.0 >= 2) {
                let mut c2 = cuts.clone();
                c2[j].1 -= 1;
                sk = skeleton(prefix.len(), doc, &c2, &index_of);
            }
        }
        st.encoded += 1;
        st.refs += nrefs as u64;
        st.out_bytes += sk.len() as u64;
        ENCODED_DOCS.fetch_add(1, Relaxed);
        REFS.fetch_add(nrefs as u64, Relaxed);
        out.push((k, seq, sk));
    }
    st.out_bytes += out.iter().filter(|r| r.2.first() == Some(&PART)).map(|r| r.2.len() as u64).sum::<u64>();
    Ok(Out { rows: out, atomic: true, prefix: Some(prefix.to_vec()) })
}

// ---- the read side

/// Whether a run row needs this module (a derived document or a message row).
pub fn is_derived(env: &[u8]) -> bool {
    matches!(env.first(), Some(&DERIVED) | Some(&PART))
}

/// The document of a derived row of the sealed run `h`, as a live envelope (message rows are read from the same run,
/// in as few GETs as their blocks allow: one range GET over the blocks it needs that are not cached).
pub async fn rebuild(store: &Arc<dyn object_store::ObjectStore>, h: &super::view::RunH, key: &[u8], env: &[u8]) -> Result<Vec<u8>> {
    let r = rebuild_inner(store, h, key, env).await;
    match &r {
        Ok(_) => REBUILDS.fetch_add(1, Relaxed),
        Err(_) => REBUILD_ERRORS.fetch_add(1, Relaxed),
    };
    r
}

async fn rebuild_inner(store: &Arc<dyn object_store::ObjectStore>, h: &super::view::RunH, key: &[u8], env: &[u8]) -> Result<Vec<u8>> {
    let sk = parse_skeleton(env)?;
    ensure!(sk.plen <= key.len(), "corrupt derived document: prefix length {} of a {}-byte key", sk.plen, key.len());
    let prefix = &key[..sk.plen];
    let run = h.open(store).await?.clone();
    ensure!(run.derived, "a derived document in a run without derived rows ({})", run.path);
    let mut want: Vec<u32> = sk.parts.iter().filter_map(|p| p.err()).collect();
    want.sort_unstable();
    want.dedup();
    let mut found: HashMap<u32, Vec<u8>> = HashMap::with_capacity(want.len());
    if let (Some(&a), Some(&z)) = (want.first(), want.last()) {
        let first = run.fence.count_below(&msg_key(prefix, a), true);
        let last = run.fence.count_below(&msg_key(prefix, z), true);
        ensure!(first >= 1 && last >= first, "corrupt derived document: its message rows are outside the run");
        let blocks = super::view::fetch_blocks(store, h, &run, first - 1, last - 1).await?;
        for b in &blocks {
            let lo = b.lower_bound(&msg_key(prefix, a));
            for i in lo..b.len() {
                let k = b.key(i);
                if !k.starts_with(prefix) || k.len() != prefix.len() + MSG_TAG.len() + 4 || &k[prefix.len()..prefix.len() + MSG_TAG.len()] != MSG_TAG {
                    break;
                }
                let idx = u32::from_be_bytes(k[k.len() - 4..].try_into()?);
                if idx > z {
                    break;
                }
                let e = b.env(i);
                ensure!(e.first() == Some(&PART), "corrupt derived document: row {} is not a message row", hexs(k));
                if want.binary_search(&idx).is_ok() {
                    found.insert(idx, e[1..].to_vec());
                }
            }
        }
    }
    let doc = assemble(&sk, &|i| found.get(&i).cloned())?;
    let mut out = Vec::with_capacity(doc.len() + 1);
    out.push(LIVE);
    out.extend(doc);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bson_doc(fields: &[(&str, u8, Vec<u8>)]) -> Vec<u8> {
        let mut b = Vec::new();
        for (n, t, v) in fields {
            b.push(*t);
            b.extend(n.as_bytes());
            b.push(0);
            if *t == 0x05 {
                b.extend((v.len() as i32).to_le_bytes());
                b.push(0);
            } else if *t == 0x02 {
                b.extend(((v.len() + 1) as i32).to_le_bytes());
            }
            b.extend(v);
            if *t == 0x02 {
                b.push(0);
            }
        }
        let mut d = ((b.len() + 5) as i32).to_le_bytes().to_vec();
        d.extend(b);
        d.push(0);
        d
    }

    fn mp_str(s: &[u8]) -> Vec<u8> {
        let mut v = vec![0xd9, s.len() as u8];
        v.extend(s);
        v
    }

    #[test]
    fn cuts_and_rebuilds_exactly() {
        let m1 = mp_str(&[b'a'; 60]);
        let m2 = mp_str(&[b'b'; 70]);
        let mut payload = vec![0x82, 0xa1, b'v', 0x04, 0xa8];
        payload.extend(b"messages");
        payload.push(0x93);
        payload.extend(&m1);
        payload.extend(&m2);
        payload.push(0x05); // a small element stays literal
        let doc = bson_doc(&[("thread_id", 0x02, b"t1".to_vec()), ("checkpoint", 0x05, payload.clone())]);
        let r = message_ranges(&doc);
        assert_eq!(r.len(), 2, "{r:?}");
        assert_eq!(&doc[r[0].0..r[0].1], &m1[..]);
        assert_eq!(&doc[r[1].0..r[1].1], &m2[..]);
        // not msgpack: no ranges
        let bad = bson_doc(&[("checkpoint", 0x05, vec![0xc1, 1, 2])]);
        assert!(message_ranges(&bad).is_empty());
        // the server's natural-order trailer after the document: the same ranges
        let mut with_trailer = doc.clone();
        with_trailer.extend([0xff, 0, 0, 0, 0, 0, 0, 0, 7]);
        assert_eq!(message_ranges(&with_trailer), r);
        // trailing bytes after the msgpack value: no ranges
        let mut p2 = payload.clone();
        p2.push(0);
        assert!(message_ranges(&bson_doc(&[("checkpoint", 0x05, p2)])).is_empty());
    }

    #[test]
    fn skeleton_round_trip_and_crc() {
        let spec = LenSpec { colls: vec![b"ck".to_vec()] };
        let mut key = b"u/".to_vec();
        key.push(2);
        key.extend(b"t1ck/d/1");
        let m1 = mp_str(&[b'a'; 60]);
        let mut payload = vec![0x92];
        payload.extend(&m1);
        payload.extend(&m1);
        let doc = bson_doc(&[("checkpoint", 0x05, payload)]);
        let mut env = vec![LIVE];
        env.extend(&doc);
        let mut st = EncStats::default();
        let p = spec.customer_prefix(&key).unwrap();
        let out = encode_customer(&spec, &key[..p], vec![(key.clone(), 7, env)], &mut st).unwrap();
        assert_eq!(out.rows.len(), 2, "one message row (the same message twice) and the skeleton");
        assert_eq!(out.rows[0].2[0], PART);
        assert_eq!(out.rows[1].2[0], DERIVED);
        assert!(out.rows[0].0 < out.rows[1].0);
        let sk = parse_skeleton(&out.rows[1].2).unwrap();
        let m = out.rows[0].2[1..].to_vec();
        assert_eq!(assemble(&sk, &|i| (i == 0).then(|| m.clone())).unwrap(), doc);
        // a wrong message is caught by the CRC
        let wrong = mp_str(&[b'z'; 60]);
        assert!(assemble(&sk, &|_| Some(wrong.clone())).is_err());
    }
}
