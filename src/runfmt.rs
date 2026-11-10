//! Sealed-run format v1 (compaction 2, build step 1).
//!
//! A run is one immutable object holding rows in strictly increasing key order. Under way (b) of the compaction 2 plan
//! (fenvix-brain `sessions/2026-11-07-compaction-2-plan/REPORT.md`, section 1) SlateDB keeps the write path and our
//! compactor moves older rows into runs: sealed runs (one per group, a key range of about 1 GB, 256 KiB blocks) and delta
//! runs (64 KiB blocks). This module is the format only: a multipart writer, a random reader over a set of runs with
//! disjoint key ranges (a group whose whale customer spans several runs), a streaming reader for the compactor, and the
//! fence array that stays in RAM. In this step nothing in the server uses it; only tests and `src/bin/runfmt_bench.rs`.
//!
//! It starts from `src/fence.rs` of branch `fence-layout` (the idea A prototype) and adds what the plan asks of v1: a
//! sequence number per row, the delete envelope, CRC32C per block and over the footer, a customer filter, min/max key
//! and sequence, a format version, the block-size knob (64 KiB to 1 MiB, default 256 KiB), and a streaming reader.
//!
//! Object layout (all integers little endian):
//! ```text
//! [block 0] ... [block n-1] [footer] [trailer, 24 bytes]
//! block   = lz4 (size-prepended) of entries, then crc32c u32 over (run id, block index u32, the lz4 bytes)
//! entry   = key length u16, key, seq u64, envelope length u32, envelope
//! footer  = version u32, run id [16], block bytes u32, customer kind u8, entries u64, deleted u64, raw bytes u64,
//!           min seq u64, max seq u64, first key (u32 length + bytes), last key (same),
//!           prefix (u32 length + bytes), n u32, n x (separator length u16, separator without the prefix),
//!           n + 1 block offsets u64 (the last = end of the data),
//!           customers u64, filter hashes u8, filter bits u64, filter bytes (bits / 8)
//! trailer = footer offset u64, footer length u32, version u32, run id [16], crc32c u32 over (footer, the first 32
//!           trailer bytes),
//!           magic u32 "FXR1"
//! ```
//! Every byte of the object is covered by a check: block bytes by their block CRC, footer bytes by the footer CRC, the
//! trailer's offset and length by `offset + length + 40 == size` and the CRC, its version and magic by equality. So a
//! flipped bit anywhere is an error when the reader touches that byte, never different data.
//!
//! The run id is 16 random bytes drawn by the writer, in the footer and the trailer and mixed into every block CRC, so a
//! block is valid only in its own run: the blocks of one run under the footer of another (a path written twice, a stale
//! footer, a wrong copied range) are errors, even when both runs have the same layout (review of step 1, FIX 1).
//!
//! The envelope (the value as stored): first byte 0 = a live document, followed by its bytes; first byte 1 = deleted,
//! nothing after it. SlateDB's own tombstones are invisible to merged scans (probe 1 of the plan), so deletes travel as
//! envelopes, and a run keeps them as rows: whether a delete hides an older row in another source is the merged
//! reader's business (step 3), not the format's.

use bytes::Bytes;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, MultipartUpload, ObjectStore, PutPayload};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::{Arc, Mutex};

pub const FORMAT_VERSION: u32 = 1;
pub const MAGIC: u32 = u32::from_le_bytes(*b"FXR1");
pub const TRAILER: u64 = 40;
pub const RUN_ID_LEN: usize = 16;
pub const DEFAULT_BLOCK: usize = 256 << 10;
pub const MIN_BLOCK: usize = 64 << 10;
pub const MAX_BLOCK: usize = 1 << 20;
/// S3 multipart: every part but the last must be at least 5 MiB.
pub const MIN_PART: usize = 5 << 20;
pub const DEFAULT_PART: usize = 16 << 20;
/// Bytes read from the end of a run when it is opened: the trailer and, for runs up to about 1.5 GB at 256 KiB blocks,
/// the whole footer, so opening costs one GET (two when the footer is larger).
pub const OPEN_TAIL: u64 = 256 << 10;
/// Bits per customer of a new run's customer filter (`--sealed-filter-bits`). The footer stores the filter's size, so a
/// reader reads runs written with any value. Step 1 wrote 10 (about 0.8% false positives per run); step 6 raised the
/// default to 16 (about 0.07% per run, MEASURED in `fx::counter_tests`), because an absent customer is checked against
/// every run that can hold it (the sealed run and each delta run) and the plan asks for at most 1% per read.
pub static FILTER_BITS_PER_CUSTOMER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(16);
const FILTER_SEED: u64 = 0x4658_5231_6669_6c74; // "FXR1filt"

/// The CRC of a stored block: over the run id, the block index and the lz4 bytes.
fn block_crc(run_id: &[u8; RUN_ID_LEN], idx: u32, c: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c_append(crc32c::crc32c(run_id), &idx.to_le_bytes()), c)
}

pub const LIVE: u8 = 0;
pub const DELETED: u8 = 1;

// ---- envelopes and rows

/// The envelope of a live document.
pub fn live(doc: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(doc.len() + 1);
    v.push(LIVE);
    v.extend_from_slice(doc);
    v
}

/// The envelope of a delete.
pub fn deleted() -> Vec<u8> {
    vec![DELETED]
}

fn check_envelope(env: &[u8]) -> anyhow::Result<()> {
    match env.first() {
        Some(&LIVE) => Ok(()),
        Some(&DELETED) if env.len() == 1 => Ok(()),
        Some(&DELETED) => anyhow::bail!("corrupt run: delete envelope with {} bytes", env.len()),
        Some(b) => anyhow::bail!("corrupt run: envelope byte {b}"),
        None => anyhow::bail!("corrupt run: empty envelope"),
    }
}

/// One row as stored: key, the SlateDB sequence number it was written with, and its envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    pub key: Vec<u8>,
    pub seq: u64,
    pub env: Vec<u8>,
}

impl Row {
    pub fn is_deleted(&self) -> bool {
        self.env.first() == Some(&DELETED)
    }

    /// The document bytes of a live row.
    pub fn doc(&self) -> Option<&[u8]> {
        if self.is_deleted() {
            None
        } else {
            Some(&self.env[1..])
        }
    }
}

// ---- customers: which part of a key names the customer (for the customer filter)

/// How the writer finds the customer component of a key. Stored in the footer, so the reader uses the same rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CustomerKind {
    /// No customer filter.
    None = 0,
    /// Today's customer-keyed document keys (`custkey.rs` on branch `compaction`):
    /// `doc/{escaped collection}/{component}{_id}`, component = length byte + `encode_sortable` of the customer value.
    DocV1 = 1,
    /// Key layout v2 of the plan (build step 2): `u/{component}/...`, the component read as a length byte and its bytes.
    /// The server's real components are not framed that way (see V2K); kept so runs written with it read the same.
    V2 = 2,
    /// Key layout v2 as the server writes it (`custkey.rs`, compaction 2 step 6): `u/{component}{collection}/...`, the
    /// component a `keyenc` encoding of the customer value, self-delimiting (its type byte and its body).
    V2K = 3,
}

impl CustomerKind {
    fn from_u8(b: u8) -> anyhow::Result<Self> {
        Ok(match b {
            0 => CustomerKind::None,
            1 => CustomerKind::DocV1,
            2 => CustomerKind::V2,
            3 => CustomerKind::V2K,
            _ => anyhow::bail!("corrupt run: customer kind {b}"),
        })
    }

    /// Where the customer component (length byte included) sits in `key`, or None if the key has no customer
    /// (the shared keyspace: collections without a customer key, `loc/`, global indexes).
    pub fn customer_of(self, key: &[u8]) -> Option<Range<usize>> {
        if self == CustomerKind::V2K {
            let t = key.strip_prefix(b"u/")?;
            let n = keyenc_skip(t)?;
            return (n > 0).then_some(2..2 + n);
        }
        let start = match self {
            CustomerKind::None => return None,
            CustomerKind::V2K => unreachable!(),
            CustomerKind::V2 => {
                if !key.starts_with(b"u/") {
                    return None;
                }
                2
            }
            CustomerKind::DocV1 => {
                if !key.starts_with(b"doc/") {
                    return None;
                }
                4 + key[4..].iter().position(|&b| b == b'/')? + 1
            }
        };
        let len = *key.get(start)? as usize;
        let end = start + 1 + len;
        if len == 0 || end > key.len() {
            return None;
        }
        Some(start..end)
    }
}

/// The length of the one `keyenc` component at the start of `b` (a copy of `mongo-server/src/keyenc.rs` `skip`, which
/// this crate cannot import; a test in `custkey.rs` checks the two agree on the server's keys).
pub fn keyenc_skip(b: &[u8]) -> Option<usize> {
    fn take(b: &[u8], i: &mut usize, n: usize) -> Option<()> {
        if *i + n > b.len() {
            return None;
        }
        *i += n;
        Some(())
    }
    fn escaped(b: &[u8], i: &mut usize) -> Option<()> {
        loop {
            let x = *b.get(*i)?;
            *i += 1;
            if x == 0 {
                let y = *b.get(*i)?;
                *i += 1;
                match y {
                    1 => return Some(()),
                    0xFF => continue,
                    _ => return None,
                }
            }
        }
    }
    fn val(b: &[u8], i: &mut usize) -> Option<()> {
        let rank = *b.get(*i)?;
        *i += 1;
        body(rank, b, i)
    }
    fn body(rank: u8, b: &[u8], i: &mut usize) -> Option<()> {
        match rank {
            1 | 20 => Some(()),
            2 => take(b, i, 1),
            3 => {
                let x = *b.get(*i)?;
                *i += 1;
                if x == 1 {
                    take(b, i, 16)
                } else {
                    Some(())
                }
            }
            4 | 12 | 13 | 14 | 15 => {
                escaped(b, i)?;
                if rank == 12 {
                    escaped(b, i)?;
                }
                Some(())
            }
            5 => loop {
                let r = *b.get(*i)?;
                *i += 1;
                if r == 0 {
                    return Some(());
                }
                escaped(b, i)?;
                body(r, b, i)?;
            },
            6 => loop {
                if *b.get(*i)? == 0 {
                    *i += 1;
                    return Some(());
                }
                val(b, i)?;
            },
            7 => {
                let n = u32::from_be_bytes(b.get(*i..*i + 4)?.try_into().ok()?) as usize;
                take(b, i, 4 + 1 + n)
            }
            8 => take(b, i, 12),
            9 => take(b, i, 1),
            10 | 11 => take(b, i, 8),
            _ => None,
        }
    }
    let mut i = 0;
    val(b, &mut i)?;
    Some(i)
}

fn customer_hash(comp: &[u8]) -> u64 {
    twox_hash::XxHash64::oneshot(FILTER_SEED, comp)
}

/// A Bloom filter of customer components.
#[derive(Clone)]
pub struct CustomerFilter {
    hashes: u8,
    bits: u64,
    data: Box<[u8]>,
}

impl CustomerFilter {
    fn build(hs: &[u64]) -> CustomerFilter {
        if hs.is_empty() {
            return CustomerFilter { hashes: 0, bits: 0, data: Box::new([]) };
        }
        let bits = (hs.len() as u64 * FILTER_BITS_PER_CUSTOMER.load(std::sync::atomic::Ordering::Relaxed).max(1)).max(64).div_ceil(8) * 8;
        let hashes = 7u8;
        let mut data = vec![0u8; (bits / 8) as usize];
        for &h in hs {
            for p in Self::positions(h, hashes, bits) {
                data[(p / 8) as usize] |= 1 << (p % 8);
            }
        }
        CustomerFilter { hashes, bits, data: data.into_boxed_slice() }
    }

    fn positions(h: u64, k: u8, bits: u64) -> impl Iterator<Item = u64> {
        let h1 = h;
        let h2 = h.rotate_left(32) | 1;
        (0..k as u64).map(move |i| h1.wrapping_add(i.wrapping_mul(h2)) % bits)
    }

    /// False: no row of this customer is in the run (certain). True: maybe.
    pub fn may_contain(&self, comp: &[u8]) -> bool {
        if self.bits == 0 {
            return false;
        }
        Self::positions(customer_hash(comp), self.hashes, self.bits).all(|p| self.data[(p / 8) as usize] & (1 << (p % 8)) != 0)
    }

    pub fn mem_bytes(&self) -> usize {
        self.data.len()
    }
}

// ---- keys

fn lcp(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

/// The shortest prefix of `next` that is greater than `prev` (`prev < next`).
fn separator(prev: &[u8], next: &[u8]) -> Vec<u8> {
    let n = lcp(prev, next);
    next[..(n + 1).min(next.len())].to_vec()
}

/// The smallest key greater than every key that starts with `p` (None: no such key, the range is unbounded).
pub fn prefix_successor(p: &[u8]) -> Option<Vec<u8>> {
    let mut s = p.to_vec();
    while let Some(&b) = s.last() {
        if b == 0xFF {
            s.pop();
        } else {
            *s.last_mut().unwrap() += 1;
            return Some(s);
        }
    }
    None
}

fn below(k: &[u8], hi: Option<&[u8]>) -> bool {
    hi.is_none_or(|h| k < h)
}

// ---- the writer

/// Writer settings.
#[derive(Clone, Debug)]
pub struct RunOptions {
    /// Uncompressed bytes per block (a block closes before an entry would pass it; one larger entry gets its own block).
    pub block_bytes: usize,
    /// Multipart part size.
    pub part_bytes: usize,
    pub customers: CustomerKind,
}

impl RunOptions {
    /// Checked settings: blocks 64 KiB to 1 MiB, parts at least 5 MiB (S3's minimum).
    pub fn new(block_bytes: usize, part_bytes: usize, customers: CustomerKind) -> anyhow::Result<Self> {
        anyhow::ensure!((MIN_BLOCK..=MAX_BLOCK).contains(&block_bytes), "block size {block_bytes} outside 64 KiB..1 MiB");
        anyhow::ensure!(part_bytes >= MIN_PART, "part size {part_bytes} under 5 MiB");
        Ok(RunOptions { block_bytes, part_bytes, customers })
    }

    /// The sealed-run default: 256 KiB blocks, 16 MiB parts.
    pub fn sealed(customers: CustomerKind) -> Self {
        RunOptions { block_bytes: DEFAULT_BLOCK, part_bytes: DEFAULT_PART, customers }
    }

    /// Any sizes, for tests that need many blocks from little data (stores without the 5 MiB rule).
    pub fn unchecked(block_bytes: usize, part_bytes: usize, customers: CustomerKind) -> Self {
        RunOptions { block_bytes: block_bytes.max(1), part_bytes: part_bytes.max(1), customers }
    }
}

/// What a finished run is.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RunMeta {
    pub path: String,
    /// 16 random bytes that bind every block to this run (step 3 keeps it in the manifest line)
    pub run_id: [u8; RUN_ID_LEN],
    pub size: u64,
    pub data_end: u64,
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub min_seq: u64,
    pub max_seq: u64,
    pub blocks: u64,
    /// Offsets of the blocks (blocks + 1 values; the last is `data_end`), for tests and tools.
    pub block_offsets: Vec<u64>,
    pub entries: u64,
    pub deleted: u64,
    pub customers: u64,
    pub raw_bytes: u64,
    /// PUT-class requests: CreateMultipartUpload + UploadPart x parts + CompleteMultipartUpload.
    pub puts: u64,
    pub parts: u64,
}

/// Writes one run as a multipart upload. The object becomes visible only when `finish` completes the upload.
pub struct RunWriter {
    path: Path,
    run_id: [u8; RUN_ID_LEN],
    upload: Box<dyn MultipartUpload>,
    opts: RunOptions,
    out: Vec<u8>,
    written: u64,
    raw: Vec<u8>,
    last_key: Vec<u8>,
    first_key: Option<Vec<u8>>,
    seps: Vec<Vec<u8>>,
    offs: Vec<u64>,
    entries: u64,
    deleted: u64,
    raw_bytes: u64,
    min_seq: u64,
    max_seq: u64,
    parts: u64,
    cust_hashes: Vec<u64>,
    last_cust: Vec<u8>,
}

impl RunWriter {
    pub async fn new(store: &Arc<dyn ObjectStore>, path: Path, opts: RunOptions) -> anyhow::Result<Self> {
        let upload = store.put_multipart_opts(&path, Default::default()).await?;
        Ok(RunWriter {
            path,
            run_id: rand::random(),
            upload,
            out: Vec::with_capacity(opts.part_bytes.min(64 << 20)),
            raw: Vec::with_capacity(opts.block_bytes + 4096),
            opts,
            written: 0,
            last_key: Vec::new(),
            first_key: None,
            seps: Vec::new(),
            offs: Vec::new(),
            entries: 0,
            deleted: 0,
            raw_bytes: 0,
            min_seq: u64::MAX,
            max_seq: 0,
            parts: 0,
            cust_hashes: Vec::new(),
            last_cust: Vec::new(),
        })
    }

    /// Adds one row; keys must come in strictly increasing order, `env` must be an envelope.
    pub async fn add(&mut self, key: &[u8], seq: u64, env: &[u8]) -> anyhow::Result<()> {
        anyhow::ensure!(self.first_key.is_none() || key > &self.last_key[..], "keys out of order");
        anyhow::ensure!(key.len() <= u16::MAX as usize && env.len() <= u32::MAX as usize, "entry too large");
        check_envelope(env)?;
        let len = 14 + key.len() + env.len();
        if !self.raw.is_empty() && self.raw.len() + len > self.opts.block_bytes {
            self.end_block().await?;
        }
        if self.raw.is_empty() {
            let sep = match &self.first_key {
                None => key.to_vec(),
                Some(_) => separator(&self.last_key, key),
            };
            self.seps.push(sep);
            self.offs.push(self.written + self.out.len() as u64);
        }
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        if let Some(r) = self.opts.customers.customer_of(key) {
            let c = &key[r];
            if c != &self.last_cust[..] {
                self.cust_hashes.push(customer_hash(c));
                self.last_cust.clear();
                self.last_cust.extend_from_slice(c);
            }
        }
        self.raw.extend_from_slice(&(key.len() as u16).to_le_bytes());
        self.raw.extend_from_slice(key);
        self.raw.extend_from_slice(&seq.to_le_bytes());
        self.raw.extend_from_slice(&(env.len() as u32).to_le_bytes());
        self.raw.extend_from_slice(env);
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.entries += 1;
        if env[0] == DELETED {
            self.deleted += 1;
        }
        self.min_seq = self.min_seq.min(seq);
        self.max_seq = self.max_seq.max(seq);
        Ok(())
    }

    async fn end_block(&mut self) -> anyhow::Result<()> {
        if self.raw.is_empty() {
            return Ok(());
        }
        let idx = (self.seps.len() - 1) as u32;
        let mut c = lz4_flex::compress_prepend_size(&self.raw);
        let crc = block_crc(&self.run_id, idx, &c);
        c.extend_from_slice(&crc.to_le_bytes());
        self.raw_bytes += self.raw.len() as u64;
        self.raw.clear();
        self.push_out(&c).await
    }

    async fn push_out(&mut self, mut b: &[u8]) -> anyhow::Result<()> {
        while !b.is_empty() {
            let room = self.opts.part_bytes - self.out.len();
            let n = room.min(b.len());
            self.out.extend_from_slice(&b[..n]);
            b = &b[n..];
            if self.out.len() == self.opts.part_bytes {
                self.flush_part().await?;
            }
        }
        Ok(())
    }

    async fn flush_part(&mut self) -> anyhow::Result<()> {
        if self.out.is_empty() {
            return Ok(());
        }
        let data = std::mem::replace(&mut self.out, Vec::with_capacity(self.opts.part_bytes.min(64 << 20)));
        self.written += data.len() as u64;
        self.parts += 1;
        self.upload.put_part(PutPayload::from(data)).await?;
        Ok(())
    }

    /// Drops the upload: nothing becomes visible.
    pub async fn abort(mut self) -> anyhow::Result<()> {
        self.upload.abort().await?;
        Ok(())
    }

    pub async fn finish(mut self) -> anyhow::Result<RunMeta> {
        anyhow::ensure!(self.first_key.is_some(), "an empty run");
        self.end_block().await?;
        let first_key = self.first_key.clone().unwrap();
        let data_end = self.written + self.out.len() as u64;
        let prefix_len = lcp(&first_key, &self.last_key);
        let filter = CustomerFilter::build(&self.cust_hashes);
        let mut f = Vec::with_capacity(64 + self.seps.len() * 40 + filter.data.len());
        f.extend(FORMAT_VERSION.to_le_bytes());
        f.extend(self.run_id);
        f.extend((self.opts.block_bytes as u32).to_le_bytes());
        f.push(self.opts.customers as u8);
        f.extend(self.entries.to_le_bytes());
        f.extend(self.deleted.to_le_bytes());
        f.extend(self.raw_bytes.to_le_bytes());
        f.extend(self.min_seq.to_le_bytes());
        f.extend(self.max_seq.to_le_bytes());
        for k in [&first_key, &self.last_key] {
            f.extend((k.len() as u32).to_le_bytes());
            f.extend(k.iter());
        }
        f.extend((prefix_len as u32).to_le_bytes());
        f.extend(&first_key[..prefix_len]);
        f.extend((self.seps.len() as u32).to_le_bytes());
        for s in &self.seps {
            let s = &s[prefix_len.min(s.len())..];
            f.extend((s.len() as u16).to_le_bytes());
            f.extend(s);
        }
        let mut offs = self.offs.clone();
        offs.push(data_end);
        for o in &offs {
            f.extend(o.to_le_bytes());
        }
        f.extend((self.cust_hashes.len() as u64).to_le_bytes());
        f.push(filter.hashes);
        f.extend(filter.bits.to_le_bytes());
        f.extend(filter.data.iter());
        let footer_len = f.len() as u32;
        let mut t = Vec::with_capacity(TRAILER as usize);
        t.extend(data_end.to_le_bytes());
        t.extend(footer_len.to_le_bytes());
        t.extend(FORMAT_VERSION.to_le_bytes());
        t.extend(self.run_id);
        let crc = crc32c::crc32c_append(crc32c::crc32c(&f), &t);
        t.extend(crc.to_le_bytes());
        t.extend(MAGIC.to_le_bytes());
        f.extend(t);
        self.push_out(&f).await?;
        self.flush_part().await?;
        self.upload.complete().await?;
        Ok(RunMeta {
            path: self.path.to_string(),
            run_id: self.run_id,
            size: self.written,
            data_end,
            first_key,
            last_key: self.last_key.clone(),
            min_seq: self.min_seq,
            max_seq: self.max_seq,
            blocks: self.seps.len() as u64,
            block_offsets: offs,
            entries: self.entries,
            deleted: self.deleted,
            customers: self.cust_hashes.len() as u64,
            raw_bytes: self.raw_bytes,
            puts: self.parts + 2,
            parts: self.parts,
        })
    }
}

// ---- the fence array and the run handle (what stays in RAM)

/// Separators are front-coded in RAM in groups of this many blocks (the first of each group is stored whole).
pub const RESTART: usize = 16;

fn put_varint(out: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn get_varint(d: &[u8], p: &mut usize) -> u32 {
    let mut v = 0u32;
    let mut s = 0;
    loop {
        let b = d[*p];
        *p += 1;
        v |= ((b & 0x7F) as u32) << s;
        if b < 0x80 {
            return v;
        }
        s += 7;
    }
}

/// One run's fence array: the separator and the offset of every block. Separators are kept without the prefix all keys
/// of the run share, front-coded against the separator before them (like the index of an SST), with a whole one every
/// `RESTART` blocks; offsets are u32 deltas from a u64 base per group. A lookup is a binary search over the group
/// starts, then a scan of at most `RESTART` separators.
pub struct Fence {
    pub prefix: Box<[u8]>,
    /// per block: varint shared length, varint suffix length, suffix
    data: Box<[u8]>,
    /// start of each group in `data`
    restarts: Box<[u32]>,
    /// offset of each group's first block
    base: Box<[u64]>,
    /// offset of block i minus its group's base
    rel: Box<[u32]>,
    /// end of the data (the offset after the last block)
    end: u64,
}

impl Fence {
    /// Builds the fence from separators without the prefix (increasing) and n + 1 increasing offsets.
    pub fn build(prefix: Box<[u8]>, seps: &[&[u8]], offs: &[u64]) -> anyhow::Result<Fence> {
        anyhow::ensure!(!seps.is_empty() && offs.len() == seps.len() + 1, "corrupt run: fence sizes");
        let mut data = Vec::new();
        let (mut restarts, mut base, mut rel) = (Vec::new(), Vec::new(), Vec::with_capacity(seps.len()));
        let mut prev: &[u8] = &[];
        for (i, s) in seps.iter().enumerate() {
            let shared = if i % RESTART == 0 {
                restarts.push(data.len() as u32);
                base.push(offs[i]);
                0
            } else {
                lcp(prev, s)
            };
            put_varint(&mut data, shared as u32);
            put_varint(&mut data, (s.len() - shared) as u32);
            data.extend_from_slice(&s[shared..]);
            let d = offs[i] - base[i / RESTART];
            anyhow::ensure!(d <= u32::MAX as u64, "corrupt run: a group of blocks over 4 GiB");
            rel.push(d as u32);
            prev = s;
        }
        Ok(Fence {
            prefix,
            data: data.into_boxed_slice(),
            restarts: restarts.into_boxed_slice(),
            base: base.into_boxed_slice(),
            rel: rel.into_boxed_slice(),
            end: offs[seps.len()],
        })
    }

    pub fn blocks(&self) -> usize {
        self.rel.len()
    }

    /// Calls `f(block, separator without the prefix)` for the blocks of group `g` in order, until it returns false.
    fn scan_group(&self, g: usize, mut f: impl FnMut(usize, &[u8]) -> bool) {
        let mut p = self.restarts[g] as usize;
        let end = self.restarts.get(g + 1).map_or(self.data.len(), |&e| e as usize);
        let mut cur = Vec::new();
        let mut i = g * RESTART;
        while p < end {
            let shared = get_varint(&self.data, &mut p) as usize;
            let n = get_varint(&self.data, &mut p) as usize;
            cur.truncate(shared);
            cur.extend_from_slice(&self.data[p..p + n]);
            p += n;
            if !f(i, &cur) {
                return;
            }
            i += 1;
        }
    }

    /// The separator of block i without the prefix.
    fn sep(&self, i: usize) -> Vec<u8> {
        let mut out = Vec::new();
        self.scan_group(i / RESTART, |j, s| {
            if j == i {
                out = s.to_vec();
                false
            } else {
                true
            }
        });
        out
    }

    /// The first separator of group g (stored whole).
    fn group_first(&self, g: usize) -> &[u8] {
        let mut p = self.restarts[g] as usize;
        let _shared = get_varint(&self.data, &mut p);
        let n = get_varint(&self.data, &mut p) as usize;
        &self.data[p..p + n]
    }

    /// The full separator of block i (prefix + stored part).
    pub fn separator(&self, i: usize) -> Vec<u8> {
        let mut v = self.prefix.to_vec();
        v.extend_from_slice(&self.sep(i));
        v
    }

    /// prefix + s compared with `t`.
    fn cmp_sep(&self, s: &[u8], t: &[u8]) -> Ordering {
        let p = &self.prefix[..];
        let n = p.len().min(t.len());
        match p[..n].cmp(&t[..n]) {
            Ordering::Equal if t.len() < p.len() => Ordering::Greater,
            Ordering::Equal => s.cmp(&t[p.len()..]),
            o => o,
        }
    }

    /// prefix + sep(i) compared with `t`.
    fn cmp(&self, i: usize, t: &[u8]) -> Ordering {
        self.cmp_sep(&self.sep(i), t)
    }

    /// Number of blocks whose separator is < t (or <= t when `inclusive`).
    pub fn count_below(&self, t: &[u8], inclusive: bool) -> usize {
        let cond = |s: &[u8]| {
            let c = self.cmp_sep(s, t);
            c == Ordering::Less || (inclusive && c == Ordering::Equal)
        };
        let (mut lo, mut hi) = (0, self.restarts.len());
        while lo < hi {
            let m = (lo + hi) / 2;
            if cond(self.group_first(m)) {
                lo = m + 1;
            } else {
                hi = m;
            }
        }
        if lo == 0 {
            return 0;
        }
        let g = lo - 1;
        let mut n = 0;
        self.scan_group(g, |_, s| {
            if cond(s) {
                n += 1;
                true
            } else {
                false
            }
        });
        g * RESTART + n
    }

    /// Checks that the separators increase and that the first one is `first` (without the prefix).
    fn check(&self, first: &[u8]) -> anyhow::Result<()> {
        let mut prev: Option<Vec<u8>> = None;
        let mut ok = true;
        for g in 0..self.restarts.len() {
            self.scan_group(g, |_, s| {
                ok &= match &prev {
                    None => s == first,
                    Some(p) => &p[..] < s,
                };
                prev = Some(s.to_vec());
                ok
            });
        }
        anyhow::ensure!(ok, "corrupt run: separators");
        Ok(())
    }

    /// The blocks that may hold keys in [lo, hi) (hi None: unbounded): first..=last, or None.
    pub fn blocks_for(&self, lo: &[u8], hi: Option<&[u8]>) -> Option<(usize, usize)> {
        let last = match hi {
            Some(h) => self.count_below(h, false),
            None => self.blocks(),
        };
        if last == 0 {
            return None;
        }
        let first = self.count_below(lo, true).max(1) - 1;
        if first > last - 1 {
            return None;
        }
        Some((first, last - 1))
    }

    /// Offset of block i (i = blocks(): the end of the data).
    pub fn offset(&self, i: usize) -> u64 {
        if i == self.blocks() {
            self.end
        } else {
            self.base[i / RESTART] + self.rel[i] as u64
        }
    }

    /// Byte range of blocks first..=last.
    pub fn range(&self, first: usize, last: usize) -> (u64, u64) {
        (self.offset(first), self.offset(last + 1))
    }

    pub fn mem_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.prefix.len() + self.data.len() + self.restarts.len() * 4 + self.base.len() * 8 + self.rel.len() * 4
    }
}

/// An opened run: everything the reader keeps in RAM for it.
pub struct Run {
    pub path: Path,
    /// binds the blocks to this run: mixed into every block CRC
    pub run_id: [u8; RUN_ID_LEN],
    pub size: u64,
    pub first_key: Box<[u8]>,
    pub last_key: Box<[u8]>,
    pub min_seq: u64,
    pub max_seq: u64,
    pub entries: u64,
    pub deleted: u64,
    pub raw_bytes: u64,
    pub block_bytes: u32,
    pub customers: CustomerKind,
    pub customer_count: u64,
    pub filter: CustomerFilter,
    pub fence: Fence,
}

struct Cursor<'a> {
    r: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> anyhow::Result<&'a [u8]> {
        anyhow::ensure!(self.r.len() >= n, "corrupt run: short footer");
        let (a, b) = self.r.split_at(n);
        self.r = b;
        Ok(a)
    }
    fn u8(&mut self) -> anyhow::Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> anyhow::Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into()?))
    }
    fn u32(&mut self) -> anyhow::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into()?))
    }
    fn u64(&mut self) -> anyhow::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into()?))
    }
    fn bytes32(&mut self) -> anyhow::Result<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }
}

/// Checks the trailer (the last 40 bytes of a run of `size` bytes): returns (footer offset, footer length).
pub fn parse_trailer(t: &[u8], size: u64) -> anyhow::Result<(u64, u32)> {
    anyhow::ensure!(t.len() == TRAILER as usize, "corrupt run: trailer of {} bytes", t.len());
    let foff = u64::from_le_bytes(t[0..8].try_into()?);
    let flen = u32::from_le_bytes(t[8..12].try_into()?);
    let ver = u32::from_le_bytes(t[12..16].try_into()?);
    let magic = u32::from_le_bytes(t[36..40].try_into()?);
    anyhow::ensure!(magic == MAGIC, "not a run (magic {magic:#x})");
    anyhow::ensure!(ver == FORMAT_VERSION, "run format version {ver}, this reader knows {FORMAT_VERSION}");
    anyhow::ensure!(foff.checked_add(flen as u64).and_then(|x| x.checked_add(TRAILER)) == Some(size), "corrupt run: footer at {foff}+{flen} in {size} bytes");
    Ok((foff, flen))
}

impl Run {
    /// Parses and checks a footer and its trailer (`ft` = footer bytes followed by the 40 trailer bytes).
    pub fn parse(path: Path, size: u64, ft: &[u8]) -> anyhow::Result<Run> {
        anyhow::ensure!(ft.len() >= TRAILER as usize, "corrupt run: short tail");
        let (f, t) = ft.split_at(ft.len() - TRAILER as usize);
        let (_, flen) = parse_trailer(t, size)?;
        anyhow::ensure!(f.len() == flen as usize, "corrupt run: footer length");
        let want = u32::from_le_bytes(t[32..36].try_into()?);
        let got = crc32c::crc32c_append(crc32c::crc32c(f), &t[..32]);
        anyhow::ensure!(want == got, "corrupt run: footer checksum");
        let mut c = Cursor { r: f };
        let ver = c.u32()?;
        anyhow::ensure!(ver == FORMAT_VERSION, "corrupt run: footer version {ver}");
        let run_id: [u8; RUN_ID_LEN] = c.take(RUN_ID_LEN)?.try_into()?;
        anyhow::ensure!(run_id[..] == t[16..32], "corrupt run: the footer's and the trailer's run ids differ");
        let block_bytes = c.u32()?;
        let customers = CustomerKind::from_u8(c.u8()?)?;
        let entries = c.u64()?;
        let deleted = c.u64()?;
        let raw_bytes = c.u64()?;
        let min_seq = c.u64()?;
        let max_seq = c.u64()?;
        let first_key: Box<[u8]> = c.bytes32()?.into();
        let last_key: Box<[u8]> = c.bytes32()?.into();
        let prefix: Box<[u8]> = c.bytes32()?.into();
        anyhow::ensure!(first_key.starts_with(&prefix) && last_key.starts_with(&prefix) && first_key <= last_key, "corrupt run: keys");
        let n = c.u32()? as usize;
        anyhow::ensure!(n >= 1 && n as u64 <= entries, "corrupt run: {n} blocks for {entries} entries");
        let mut seps: Vec<&[u8]> = Vec::with_capacity(n);
        for _ in 0..n {
            let l = c.u16()? as usize;
            seps.push(c.take(l)?);
        }
        let mut offs = Vec::with_capacity(n + 1);
        for _ in 0..=n {
            offs.push(c.u64()?);
        }
        anyhow::ensure!(offs[0] == 0 && offs.windows(2).all(|w| w[0] < w[1]) && offs[n] + ft.len() as u64 == size, "corrupt run: block offsets");
        let customer_count = c.u64()?;
        let hashes = c.u8()?;
        let bits = c.u64()?;
        anyhow::ensure!(bits % 8 == 0, "corrupt run: filter bits");
        let data: Box<[u8]> = c.take((bits / 8) as usize)?.into();
        anyhow::ensure!(c.r.is_empty(), "corrupt run: footer has {} extra bytes", c.r.len());
        let fence = Fence::build(prefix, &seps, &offs)?;
        // the separators must increase, the first equals the first key, and the last key is past the last separator
        fence.check(&first_key[fence.prefix.len()..])?;
        anyhow::ensure!(fence.cmp(n - 1, &last_key) != Ordering::Greater, "corrupt run: last key");
        Ok(Run {
            path,
            run_id,
            size,
            first_key,
            last_key,
            min_seq,
            max_seq,
            entries,
            deleted,
            raw_bytes,
            block_bytes,
            customers,
            customer_count,
            filter: CustomerFilter { hashes, bits, data },
            fence,
        })
    }

    /// Opens a run: one GET of the last `OPEN_TAIL` bytes, a second one only when the footer is larger.
    pub async fn open(store: &Arc<dyn ObjectStore>, path: Path, size: u64) -> anyhow::Result<(Run, u64)> {
        anyhow::ensure!(size > TRAILER, "corrupt run: {size} bytes");
        let a = size.saturating_sub(OPEN_TAIL);
        let tail = get_range(store, &path, a, size).await?;
        let (foff, _) = parse_trailer(&tail[tail.len() - TRAILER as usize..], size)?;
        if foff >= a {
            let run = Run::parse(path, size, &tail[(foff - a) as usize..])?;
            return Ok((run, 1));
        }
        let ft = get_range(store, &path, foff, size).await?;
        Ok((Run::parse(path, size, &ft)?, 2))
    }

    /// Opens a run and checks its id against the one the caller recorded (step 3: the manifest line), so a path that now
    /// holds another object of the same size is an error.
    pub async fn open_expecting(store: &Arc<dyn ObjectStore>, path: Path, size: u64, run_id: &[u8; RUN_ID_LEN]) -> anyhow::Result<(Run, u64)> {
        let (run, gets) = Run::open(store, path, size).await?;
        anyhow::ensure!(&run.run_id == run_id, "run {} has another id than expected: another object at this path", run.path);
        Ok((run, gets))
    }

    /// RAM of this run's maps: the fence array, the customer filter and the fixed fields.
    pub fn mem_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.path.as_ref().len() + self.first_key.len() + self.last_key.len() + self.fence.mem_bytes() - std::mem::size_of::<Fence>()
            + self.filter.mem_bytes()
    }

    /// False when the run certainly holds no row of the customer with component `comp`.
    pub fn may_hold_customer(&self, comp: &[u8]) -> bool {
        self.customers == CustomerKind::None || self.filter.may_contain(comp)
    }

    /// Whether keys in [lo, hi) can be in this run by its key bounds and, when the range lies inside one customer, by
    /// its customer filter.
    pub fn may_hold(&self, lo: &[u8], hi: Option<&[u8]>) -> bool {
        if !below(&self.first_key, hi) || &self.last_key[..] < lo {
            return false;
        }
        match self.customers.customer_of(lo) {
            Some(r) => {
                let comp = &lo[r.clone()];
                let in_one = match (hi, prefix_successor(&lo[..r.end])) {
                    (Some(h), Some(s)) => h <= &s[..],
                    (None, None) => true,
                    _ => false,
                };
                !in_one || self.filter.may_contain(comp)
            }
            None => true,
        }
    }
}

// ---- blocks

/// A decoded, checked block.
pub struct Block {
    raw: Vec<u8>,
    starts: Vec<u32>,
}

impl Block {
    /// Checks the CRC of stored block `idx` of the run with id `run_id`, decompresses it and checks every entry. Errors, never panics, on bad bytes.
    pub fn decode(stored: &[u8], run_id: &[u8; RUN_ID_LEN], idx: u32) -> anyhow::Result<Block> {
        anyhow::ensure!(stored.len() > 4, "corrupt run: block {idx} of {} bytes", stored.len());
        let (c, crc) = stored.split_at(stored.len() - 4);
        let want = u32::from_le_bytes(crc.try_into()?);
        let got = block_crc(run_id, idx, c);
        anyhow::ensure!(want == got, "corrupt run: block {idx} checksum");
        let raw = lz4_flex::decompress_size_prepended(c).map_err(|e| anyhow::anyhow!("corrupt run: block {idx}: {e}"))?;
        let mut starts = Vec::new();
        let mut p = 0usize;
        let mut prev: Option<&[u8]> = None;
        while p < raw.len() {
            starts.push(p as u32);
            anyhow::ensure!(p + 2 <= raw.len(), "corrupt run: block {idx} entry");
            let kl = u16::from_le_bytes([raw[p], raw[p + 1]]) as usize;
            anyhow::ensure!(p + 2 + kl + 12 <= raw.len(), "corrupt run: block {idx} entry");
            let key = &raw[p + 2..p + 2 + kl];
            let vl = u32::from_le_bytes(raw[p + 10 + kl..p + 14 + kl].try_into()?) as usize;
            anyhow::ensure!(p + 14 + kl + vl <= raw.len(), "corrupt run: block {idx} entry");
            check_envelope(&raw[p + 14 + kl..p + 14 + kl + vl])?;
            anyhow::ensure!(prev.is_none_or(|q| q < key), "corrupt run: block {idx} keys out of order");
            prev = Some(key);
            p += 14 + kl + vl;
        }
        anyhow::ensure!(!starts.is_empty(), "corrupt run: empty block {idx}");
        Ok(Block { raw, starts })
    }

    pub fn len(&self) -> usize {
        self.starts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.starts.is_empty()
    }

    fn at(&self, i: usize) -> (usize, usize) {
        let p = self.starts[i] as usize;
        (p, u16::from_le_bytes([self.raw[p], self.raw[p + 1]]) as usize)
    }

    pub fn key(&self, i: usize) -> &[u8] {
        let (p, kl) = self.at(i);
        &self.raw[p + 2..p + 2 + kl]
    }

    pub fn seq(&self, i: usize) -> u64 {
        let (p, kl) = self.at(i);
        u64::from_le_bytes(self.raw[p + 2 + kl..p + 10 + kl].try_into().unwrap())
    }

    pub fn env(&self, i: usize) -> &[u8] {
        let (p, kl) = self.at(i);
        let vl = u32::from_le_bytes(self.raw[p + 10 + kl..p + 14 + kl].try_into().unwrap()) as usize;
        &self.raw[p + 14 + kl..p + 14 + kl + vl]
    }

    pub fn row(&self, i: usize) -> Row {
        Row { key: self.key(i).to_vec(), seq: self.seq(i), env: self.env(i).to_vec() }
    }

    /// First entry with key >= t.
    pub fn lower_bound(&self, t: &[u8]) -> usize {
        let (mut lo, mut hi) = (0, self.len());
        while lo < hi {
            let m = (lo + hi) / 2;
            if self.key(m) < t {
                lo = m + 1;
            } else {
                hi = m;
            }
        }
        lo
    }

    /// First entry with key >= t, or the end when t is None.
    fn bound(&self, t: Option<&[u8]>) -> usize {
        match t {
            Some(t) => self.lower_bound(t),
            None => self.len(),
        }
    }

    pub fn mem_bytes(&self) -> usize {
        self.raw.capacity() + self.starts.capacity() * 4
    }

    /// Entries i..j as a block of their own, with only their bytes (compaction 2 step 6, physical slices: the waiter
    /// keeps a customer's rows of a block, not the block). May be empty: an exact "no row here".
    pub fn slice(&self, i: usize, j: usize) -> Block {
        let j = j.min(self.len());
        if i >= j {
            return Block { raw: Vec::new(), starts: Vec::new() };
        }
        let a = self.starts[i] as usize;
        let z = if j < self.len() { self.starts[j] as usize } else { self.raw.len() };
        let raw = self.raw[a..z].to_vec();
        let starts = self.starts[i..j].iter().map(|s| s - a as u32).collect();
        Block { raw, starts }
    }
}

/// Decodes block `i` of `run` and checks it against the fence: every key at or above its separator and below the next.
pub fn decode_checked(run: &Run, i: usize, stored: &[u8]) -> anyhow::Result<Block> {
    let b = Block::decode(stored, &run.run_id, i as u32)?;
    anyhow::ensure!(run.fence.cmp(i, b.key(0)) != Ordering::Greater, "corrupt run: block {i} starts below its separator");
    if i + 1 < run.fence.blocks() {
        anyhow::ensure!(run.fence.cmp(i + 1, b.key(b.len() - 1)) == Ordering::Greater, "corrupt run: block {i} reaches the next separator");
    } else {
        anyhow::ensure!(b.key(b.len() - 1) == &run.last_key[..], "corrupt run: last block does not end with the last key");
    }
    if i == 0 {
        anyhow::ensure!(b.key(0) == &run.first_key[..], "corrupt run: first block does not start with the first key");
    }
    Ok(b)
}

/// A byte-bounded LRU of decoded blocks. The block just inserted always stays. Capacity 0 keeps nothing.
pub struct BlockCache {
    cap: usize,
    used: usize,
    tick: u64,
    map: HashMap<(u32, u32), (Arc<Block>, u64)>,
    order: BTreeMap<u64, (u32, u32)>,
}

impl BlockCache {
    pub fn new(cap: usize) -> Self {
        BlockCache { cap, used: 0, tick: 0, map: HashMap::new(), order: BTreeMap::new() }
    }

    pub fn get(&mut self, k: (u32, u32)) -> Option<Arc<Block>> {
        self.tick += 1;
        let t = self.tick;
        let (b, old) = self.map.get_mut(&k)?;
        self.order.remove(old);
        *old = t;
        self.order.insert(t, k);
        Some(b.clone())
    }

    pub fn insert(&mut self, k: (u32, u32), b: Arc<Block>) {
        if self.cap == 0 {
            return;
        }
        self.tick += 1;
        if let Some((old, t)) = self.map.remove(&k) {
            self.order.remove(&t);
            self.used -= old.mem_bytes();
        }
        self.used += b.mem_bytes();
        self.map.insert(k, (b, self.tick));
        self.order.insert(self.tick, k);
        while self.used > self.cap && self.order.len() > 1 {
            let (&t, &victim) = self.order.iter().next().unwrap();
            self.order.remove(&t);
            if let Some((old, _)) = self.map.remove(&victim) {
                self.used -= old.mem_bytes();
            }
        }
    }

    pub fn used(&self) -> usize {
        self.used
    }
}

pub async fn get_range(store: &Arc<dyn ObjectStore>, path: &Path, a: u64, b: u64) -> anyhow::Result<Bytes> {
    let opts = GetOptions { range: Some(GetRange::Bounded(a..b)), ..Default::default() };
    let data = store.get_opts(path, opts).await?.bytes().await?;
    anyhow::ensure!(data.len() as u64 == b - a, "short read of {path}: {} of {} bytes", data.len(), b - a);
    Ok(data)
}

// ---- the random reader over a set of runs (one group: runs with disjoint key ranges)

#[derive(Default, Debug, Clone, Copy, serde::Serialize)]
pub struct ReadStats {
    /// range GETs sent
    pub gets: u64,
    /// waves of GETs waited for one after the other
    pub trips: u64,
    pub bytes: u64,
    pub blocks_fetched: u64,
    pub cache_hits: u64,
    /// runs skipped because their customer filter said no
    pub filter_skips: u64,
}

impl ReadStats {
    fn add(&mut self, o: &ReadStats) {
        self.gets += o.gets;
        self.bytes += o.bytes;
        self.blocks_fetched += o.blocks_fetched;
        self.cache_hits += o.cache_hits;
        self.filter_skips += o.filter_skips;
    }
}

pub struct RunReader {
    pub store: Arc<dyn ObjectStore>,
    /// sorted by first key; key ranges do not overlap
    pub runs: Vec<Run>,
    pub cache: Mutex<BlockCache>,
    /// blocks fetched per trip by `newest`, counting back from the customer's last block
    pub tail_blocks: usize,
}

impl RunReader {
    /// A reader over opened runs. Their key ranges must not overlap.
    pub fn new(store: Arc<dyn ObjectStore>, mut runs: Vec<Run>, cache_bytes: usize, tail_blocks: usize) -> anyhow::Result<RunReader> {
        runs.sort_by(|a, b| a.first_key.cmp(&b.first_key));
        for w in runs.windows(2) {
            anyhow::ensure!(w[0].last_key < w[1].first_key, "runs {} and {} overlap", w[0].path, w[1].path);
        }
        Ok(RunReader { store, runs, cache: Mutex::new(BlockCache::new(cache_bytes)), tail_blocks: tail_blocks.max(1) })
    }

    /// Opens runs given as (path, size). Returns the reader and the GETs sent.
    pub async fn open(store: Arc<dyn ObjectStore>, runs: &[(String, u64)], cache_bytes: usize, tail_blocks: usize) -> anyhow::Result<(RunReader, u64)> {
        let mut rs = Vec::new();
        let mut gets = 0;
        for (p, size) in runs {
            let (r, g) = Run::open(&store, Path::from(p.as_str()), *size).await?;
            gets += g;
            rs.push(r);
        }
        Ok((RunReader::new(store, rs, cache_bytes, tail_blocks)?, gets))
    }

    /// RAM of all maps: the run directory, every fence array and customer filter.
    pub fn map_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.runs.iter().map(|r| r.mem_bytes()).sum::<usize>()
    }

    /// The runs whose key range meets [lo, hi), in key order.
    fn runs_for(&self, lo: &[u8], hi: Option<&[u8]>) -> Vec<usize> {
        (0..self.runs.len()).filter(|&i| below(&self.runs[i].first_key, hi) && &self.runs[i].last_key[..] >= lo).collect()
    }

    /// Blocks first..=last of run `ri`: cached ones from the cache, the others in one range GET.
    async fn fetch(&self, ri: usize, first: usize, last: usize, st: &mut ReadStats) -> anyhow::Result<Vec<Arc<Block>>> {
        let mut out: Vec<Option<Arc<Block>>> = Vec::with_capacity(last + 1 - first);
        {
            let mut c = self.cache.lock().unwrap();
            for b in first..=last {
                let hit = c.get((ri as u32, b as u32));
                if hit.is_some() {
                    st.cache_hits += 1;
                }
                out.push(hit);
            }
        }
        let miss: Vec<usize> = (first..=last).filter(|b| out[b - first].is_none()).collect();
        if let (Some(&a), Some(&z)) = (miss.first(), miss.last()) {
            let run = &self.runs[ri];
            let (lo, hi) = run.fence.range(a, z);
            let data = get_range(&self.store, &run.path, lo, hi).await?;
            st.gets += 1;
            st.bytes += data.len() as u64;
            let mut c = self.cache.lock().unwrap();
            for b in a..=z {
                let (x, y) = run.fence.range(b, b);
                let blk = Arc::new(decode_checked(run, b, &data[(x - lo) as usize..(y - lo) as usize])?);
                st.blocks_fetched += 1;
                c.insert((ri as u32, b as u32), blk.clone());
                out[b - first] = Some(blk);
            }
        }
        out.into_iter().map(|b| b.ok_or_else(|| anyhow::anyhow!("block missing after fetch"))).collect()
    }

    /// Every block of run `ri` that may hold keys in [lo, hi), and the rows in range from them.
    async fn run_range(&self, ri: usize, lo: &[u8], hi: Option<&[u8]>) -> anyhow::Result<(Vec<Row>, ReadStats)> {
        let mut st = ReadStats::default();
        let Some((first, last)) = self.runs[ri].fence.blocks_for(lo, hi) else { return Ok((vec![], st)) };
        let blocks = self.fetch(ri, first, last, &mut st).await?;
        let mut out = Vec::new();
        for b in &blocks {
            for i in b.lower_bound(lo)..b.bound(hi) {
                out.push(b.row(i));
            }
        }
        Ok((out, st))
    }

    /// Every row with lo <= key < hi (hi None: to the end), in key order. Runs are fetched in one wave.
    pub async fn range(&self, lo: &[u8], hi: Option<&[u8]>) -> anyhow::Result<(Vec<Row>, ReadStats)> {
        let mut st = ReadStats::default();
        let mut want = Vec::new();
        for ri in self.runs_for(lo, hi) {
            if self.runs[ri].may_hold(lo, hi) {
                want.push(ri);
            } else {
                st.filter_skips += 1;
            }
        }
        let parts = futures::future::try_join_all(want.iter().map(|&ri| self.run_range(ri, lo, hi))).await?;
        let mut out = Vec::new();
        for (rows, s) in parts {
            if s.gets > 0 {
                st.trips = 1;
            }
            st.add(&s);
            out.extend(rows);
        }
        Ok((out, st))
    }

    /// The row of `key`, if a run holds it.
    pub async fn get(&self, key: &[u8]) -> anyhow::Result<(Option<Row>, ReadStats)> {
        let mut st = ReadStats::default();
        let Some(ri) = self.runs.iter().position(|r| &r.first_key[..] <= key && key <= &r.last_key[..]) else { return Ok((None, st)) };
        let run = &self.runs[ri];
        if let Some(r) = run.customers.customer_of(key) {
            if !run.filter.may_contain(&key[r]) {
                st.filter_skips += 1;
                return Ok((None, st));
            }
        }
        let i = run.fence.count_below(key, true);
        if i == 0 {
            return Ok((None, st));
        }
        let b = &self.fetch(ri, i - 1, i - 1, &mut st).await?[0];
        st.trips = (st.gets > 0) as u64;
        let j = b.lower_bound(key);
        Ok(((j < b.len() && b.key(j) == key).then(|| b.row(j)), st))
    }

    /// Every row whose key starts with `p` (a customer's range when `p` is its prefix), in key order.
    pub async fn prefix(&self, p: &[u8]) -> anyhow::Result<(Vec<Row>, ReadStats)> {
        let s = prefix_successor(p);
        self.range(p, s.as_deref()).await
    }

    /// The `limit` last rows whose key starts with `p`, last key first: a customer's newest documents when its keys
    /// follow time. Walks runs from the last and blocks from the end, `tail_blocks` blocks per GET.
    pub async fn newest(&self, p: &[u8], limit: usize) -> anyhow::Result<(Vec<Row>, ReadStats)> {
        let mut st = ReadStats::default();
        let succ = prefix_successor(p);
        let e = succ.as_deref();
        let mut got: Vec<Row> = Vec::new();
        if limit == 0 {
            return Ok((got, st));
        }
        for ri in self.runs_for(p, e).into_iter().rev() {
            if !self.runs[ri].may_hold(p, e) {
                st.filter_skips += 1;
                continue;
            }
            let Some((first, mut last)) = self.runs[ri].fence.blocks_for(p, e) else { continue };
            loop {
                let a = (last + 1).saturating_sub(self.tail_blocks).max(first);
                let g0 = st.gets;
                let blocks = self.fetch(ri, a, last, &mut st).await?;
                st.trips += (st.gets > g0) as u64;
                for b in blocks.iter().rev() {
                    let lo = b.lower_bound(p);
                    let hi = b.bound(e);
                    for i in (lo..hi).rev() {
                        if got.len() == limit {
                            break;
                        }
                        got.push(b.row(i));
                    }
                }
                if got.len() >= limit || a == first {
                    break;
                }
                last = a - 1;
            }
            if got.len() >= limit {
                break;
            }
        }
        Ok((got, st))
    }

    /// Reads one whole block of run `ri` (tools and tests).
    pub async fn read_block(&self, ri: usize, b: usize) -> anyhow::Result<Vec<Row>> {
        let mut st = ReadStats::default();
        let blk = &self.fetch(ri, b, b, &mut st).await?[0];
        Ok((0..blk.len()).map(|i| blk.row(i)).collect())
    }
}

// ---- the streaming reader (the compactor's side)

/// Reads a whole run in key order, `chunk_bytes` of blocks per range GET, checking every block, the order of keys
/// across blocks, and at the end the entry count, deletes, sequence bounds and the last key against the footer.
pub struct RunStream {
    store: Arc<dyn ObjectStore>,
    pub run: Run,
    chunk_bytes: u64,
    next_block: usize,
    blocks: std::collections::VecDeque<Block>,
    pos: usize,
    seen: u64,
    seen_deleted: u64,
    last: Option<Vec<u8>>,
    pub gets: u64,
    done: bool,
}

impl RunStream {
    pub async fn open(store: Arc<dyn ObjectStore>, path: Path, size: u64, chunk_bytes: u64) -> anyhow::Result<RunStream> {
        let (run, gets) = Run::open(&store, path, size).await?;
        Ok(RunStream {
            store,
            run,
            chunk_bytes: chunk_bytes.max(1),
            next_block: 0,
            blocks: Default::default(),
            pos: 0,
            seen: 0,
            seen_deleted: 0,
            last: None,
            gets,
            done: false,
        })
    }

    async fn fill(&mut self) -> anyhow::Result<bool> {
        let n = self.run.fence.blocks();
        if self.next_block >= n {
            return Ok(false);
        }
        let a = self.next_block;
        let mut z = a;
        while z + 1 < n && self.run.fence.offset(z + 2) - self.run.fence.offset(a) <= self.chunk_bytes {
            z += 1;
        }
        let (lo, hi) = self.run.fence.range(a, z);
        let data = get_range(&self.store, &self.run.path, lo, hi).await?;
        self.gets += 1;
        for b in a..=z {
            let (x, y) = self.run.fence.range(b, b);
            self.blocks.push_back(decode_checked(&self.run, b, &data[(x - lo) as usize..(y - lo) as usize])?);
        }
        self.next_block = z + 1;
        Ok(true)
    }

    /// The next row, or None at the end (after the end-of-run checks).
    pub async fn next(&mut self) -> anyhow::Result<Option<Row>> {
        loop {
            if self.done {
                return Ok(None);
            }
            if let Some(b) = self.blocks.front() {
                if self.pos < b.len() {
                    let row = b.row(self.pos);
                    self.pos += 1;
                    anyhow::ensure!(self.last.as_ref().is_none_or(|l| l < &row.key), "corrupt run: keys out of order across blocks");
                    anyhow::ensure!(row.seq >= self.run.min_seq && row.seq <= self.run.max_seq, "corrupt run: seq outside the footer's bounds");
                    self.seen += 1;
                    if row.is_deleted() {
                        self.seen_deleted += 1;
                    }
                    self.last = Some(row.key.clone());
                    return Ok(Some(row));
                }
                self.blocks.pop_front();
                self.pos = 0;
                continue;
            }
            if !self.fill().await? {
                anyhow::ensure!(self.seen == self.run.entries && self.seen_deleted == self.run.deleted, "corrupt run: entry count");
                anyhow::ensure!(self.last.as_deref() == Some(&self.run.last_key[..]), "corrupt run: last key");
                self.done = true;
                return Ok(None);
            }
        }
    }
}

#[cfg(test)]
mod tests;
