//! Fence layout: one sealed run per bucket, 1 MiB blocks, no filters, no levels (measurement prototype).
//!
//! Idea A, step 1 (fenvix-brain `prompts/P-fence-layout-local.md`): a bucket (a range of customers, about 1 GB of data)
//! is stored on S3 as ONE object, its keys in customer-first order (the layout of `mongo-server/src/custkey.rs`), cut into
//! blocks of about 1 MiB of entries, each block lz4-compressed. The only map the waiter keeps is the fence array: for
//! each block the shortest key that separates it from the block before (minus the prefix every key of the run shares)
//! and its offset. A cold read of one customer's newest documents is a fence lookup in RAM and one range GET.
//!
//! SlateDB 0.17 cannot be configured this way (its largest block is 64 KiB, `SstBlockSize::Other` exists only under
//! `cfg(test)`), so this is a minimal writer and reader outside SlateDB. Nothing in the server uses it: it is only built
//! into the `fence_layout` measurement binary.
//!
//! Object format:
//! ```text
//! [block 0] ... [block n-1] [footer] [trailer: footer offset u64, footer length u32, magic u32]   (little endian)
//! block   = lz4 (size-prepended) of entries: key length u16, key, value length u32, value
//! footer  = prefix length u32, prefix, n u32, n x (separator length u16, separator without the prefix),
//!           n+1 block offsets u64 (the last = end of the data), first key (u32 length + bytes), last key (same)
//! ```

use bytes::Bytes;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, MultipartUpload, ObjectStore, PutPayload};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

pub const BLOCK_RAW: usize = 1 << 20;
pub const PART: usize = 64 << 20;
const MAGIC: u32 = 0x3146_454e; // "NEF1"
const TRAILER: u64 = 16;

// ---- keys: the customer-first document key of mongo-server (custkey::doc_key) for an integer customer and an ObjectId

fn sortable_i64(v: i64) -> [u8; 8] {
    ((v as u64) ^ (1 << 63)).to_be_bytes()
}

fn sortable_f64(v: f64) -> [u8; 8] {
    let v = if v == 0.0 { 0.0 } else { v };
    let bits = v.to_bits();
    let t = if bits >> 63 == 1 { !bits } else { bits | (1 << 63) };
    t.to_be_bytes()
}

/// `doc/{coll}/` then the customer component of an integer customer (length byte, number rank 3, f64, i64), exactly as
/// `custkey::comp(Bson::Int32|Int64)` and `layout::doc_prefix` build it. Every document of the customer starts with it.
pub fn customer_prefix(coll: &str, user: i64) -> Vec<u8> {
    let mut k = b"doc/".to_vec();
    for b in coll.bytes() {
        match b {
            b'%' => k.extend_from_slice(b"%25"),
            b'/' => k.extend_from_slice(b"%2F"),
            _ => k.push(b),
        }
    }
    k.push(b'/');
    k.push(17);
    k.push(3);
    k.extend(sortable_f64(user as f64));
    k.extend(sortable_i64(user));
    k
}

/// The document key: customer prefix, then the `_id` (ObjectId rank 8, 12 bytes).
pub fn doc_key(coll: &str, user: i64, oid: &[u8; 12]) -> Vec<u8> {
    let mut k = customer_prefix(coll, user);
    k.push(8);
    k.extend_from_slice(oid);
    k
}

/// One past every key that starts with `p` (`p` followed by 0xFF: the next byte of a document key is a type rank < 0xFF).
pub fn prefix_end(p: &[u8]) -> Vec<u8> {
    let mut e = p.to_vec();
    e.push(0xFF);
    e
}

fn lcp(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

/// The shortest prefix of `next` that is greater than `prev` (`prev < next`).
fn separator(prev: &[u8], next: &[u8]) -> Vec<u8> {
    let n = lcp(prev, next);
    next[..(n + 1).min(next.len())].to_vec()
}

// ---- the writer

/// What a finished run is.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RunMeta {
    pub path: String,
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub size: u64,
    pub data_end: u64,
    pub blocks: u64,
    pub entries: u64,
    pub raw_bytes: u64,
    /// PUT-class requests sent: CreateMultipartUpload + UploadPart x parts + CompleteMultipartUpload.
    pub puts: u64,
    pub parts: u64,
}

/// Writes one run as a multipart upload in parts of `part` bytes (the last part may be smaller).
pub struct RunWriter {
    path: Path,
    upload: Box<dyn MultipartUpload>,
    part: usize,
    out: Vec<u8>,
    written: u64,
    raw: Vec<u8>,
    block_first: Vec<u8>,
    last_key: Vec<u8>,
    first_key: Option<Vec<u8>>,
    /// full separators (with the prefix) and the offset of each block
    seps: Vec<Vec<u8>>,
    offs: Vec<u64>,
    entries: u64,
    raw_bytes: u64,
    parts: u64,
    block_raw: usize,
}

impl RunWriter {
    pub async fn new(store: &Arc<dyn ObjectStore>, path: Path, part: usize, block_raw: usize) -> anyhow::Result<Self> {
        let upload = store.put_multipart_opts(&path, Default::default()).await?;
        Ok(RunWriter {
            path,
            upload,
            part,
            out: Vec::with_capacity(part),
            written: 0,
            raw: Vec::with_capacity(block_raw + 4096),
            block_first: Vec::new(),
            last_key: Vec::new(),
            first_key: None,
            seps: Vec::new(),
            offs: Vec::new(),
            entries: 0,
            raw_bytes: 0,
            parts: 0,
            block_raw,
        })
    }

    pub fn raw_bytes(&self) -> u64 {
        self.raw_bytes + self.raw.len() as u64
    }

    /// Adds one entry; keys must come in strictly increasing order.
    pub async fn add(&mut self, key: &[u8], val: &[u8]) -> anyhow::Result<()> {
        anyhow::ensure!(self.first_key.is_none() || key > &self.last_key[..], "keys out of order");
        anyhow::ensure!(key.len() <= u16::MAX as usize && val.len() <= u32::MAX as usize, "entry too large");
        let len = 6 + key.len() + val.len();
        if !self.raw.is_empty() && self.raw.len() + len > self.block_raw {
            self.end_block().await?;
        }
        if self.raw.is_empty() {
            let sep = match &self.first_key {
                None => key.to_vec(),
                Some(_) => separator(&self.last_key, key),
            };
            self.seps.push(sep);
            self.offs.push(self.written + self.out.len() as u64);
            self.block_first = key.to_vec();
        }
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        self.raw.extend_from_slice(&(key.len() as u16).to_le_bytes());
        self.raw.extend_from_slice(key);
        self.raw.extend_from_slice(&(val.len() as u32).to_le_bytes());
        self.raw.extend_from_slice(val);
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.entries += 1;
        Ok(())
    }

    async fn end_block(&mut self) -> anyhow::Result<()> {
        if self.raw.is_empty() {
            return Ok(());
        }
        let c = lz4_flex::compress_prepend_size(&self.raw);
        self.raw_bytes += self.raw.len() as u64;
        self.raw.clear();
        self.push_out(&c).await
    }

    async fn push_out(&mut self, mut b: &[u8]) -> anyhow::Result<()> {
        while !b.is_empty() {
            let room = self.part - self.out.len();
            let n = room.min(b.len());
            self.out.extend_from_slice(&b[..n]);
            b = &b[n..];
            if self.out.len() == self.part {
                self.flush_part().await?;
            }
        }
        Ok(())
    }

    async fn flush_part(&mut self) -> anyhow::Result<()> {
        if self.out.is_empty() {
            return Ok(());
        }
        let data = std::mem::replace(&mut self.out, Vec::with_capacity(self.part));
        self.written += data.len() as u64;
        self.parts += 1;
        self.upload.put_part(PutPayload::from(data)).await?;
        Ok(())
    }

    pub async fn finish(mut self) -> anyhow::Result<RunMeta> {
        self.end_block().await?;
        let first_key = self.first_key.clone().unwrap_or_default();
        let data_end = self.written + self.out.len() as u64;
        let prefix_len = lcp(&first_key, &self.last_key);
        let prefix = first_key[..prefix_len].to_vec();
        let mut f = Vec::new();
        f.extend((prefix.len() as u32).to_le_bytes());
        f.extend(&prefix);
        f.extend((self.seps.len() as u32).to_le_bytes());
        for s in &self.seps {
            let s = &s[prefix_len.min(s.len())..];
            f.extend((s.len() as u16).to_le_bytes());
            f.extend(s);
        }
        for o in &self.offs {
            f.extend(o.to_le_bytes());
        }
        f.extend(data_end.to_le_bytes());
        f.extend((first_key.len() as u32).to_le_bytes());
        f.extend(&first_key);
        f.extend((self.last_key.len() as u32).to_le_bytes());
        f.extend(&self.last_key);
        let footer_len = f.len() as u32;
        f.extend(data_end.to_le_bytes());
        f.extend(footer_len.to_le_bytes());
        f.extend(MAGIC.to_le_bytes());
        self.push_out(&f).await?;
        self.flush_part().await?;
        self.upload.complete().await?;
        Ok(RunMeta {
            path: self.path.to_string(),
            first_key,
            last_key: self.last_key.clone(),
            size: self.written,
            data_end,
            blocks: self.seps.len() as u64,
            entries: self.entries,
            raw_bytes: self.raw_bytes,
            puts: self.parts + 2,
            parts: self.parts,
        })
    }
}

// ---- the fence array (the waiter's only map)

/// One run's fence array, in RAM. Separators are stored without the prefix all keys of the run share.
pub struct Fence {
    pub prefix: Box<[u8]>,
    seps: Box<[u8]>,
    /// end of separator i in `seps` (n entries)
    sep_end: Box<[u32]>,
    /// offset of block i (n + 1 entries: the last is the end of the data)
    offs: Box<[u64]>,
}

impl Fence {
    pub fn blocks(&self) -> usize {
        self.sep_end.len()
    }

    fn sep(&self, i: usize) -> &[u8] {
        let a = if i == 0 { 0 } else { self.sep_end[i - 1] as usize };
        &self.seps[a..self.sep_end[i] as usize]
    }

    /// prefix + sep(i) compared with `t`.
    fn cmp(&self, i: usize, t: &[u8]) -> Ordering {
        let p = &self.prefix[..];
        let n = p.len().min(t.len());
        match p[..n].cmp(&t[..n]) {
            Ordering::Equal if t.len() < p.len() => Ordering::Greater,
            Ordering::Equal => self.sep(i).cmp(&t[p.len()..]),
            o => o,
        }
    }

    /// Number of blocks whose separator is < t (or <= t when `inclusive`).
    fn count_below(&self, t: &[u8], inclusive: bool) -> usize {
        let (mut lo, mut hi) = (0, self.blocks());
        while lo < hi {
            let m = (lo + hi) / 2;
            let c = self.cmp(m, t);
            if c == Ordering::Less || (inclusive && c == Ordering::Equal) {
                lo = m + 1;
            } else {
                hi = m;
            }
        }
        lo
    }

    /// The blocks that may hold keys in [lo, hi): first..=last, or None.
    pub fn blocks_for(&self, lo: &[u8], hi: &[u8]) -> Option<(usize, usize)> {
        let last = self.count_below(hi, false);
        if last == 0 {
            return None;
        }
        let first = self.count_below(lo, true).max(1) - 1;
        Some((first, last - 1))
    }

    pub fn range(&self, first: usize, last: usize) -> (u64, u64) {
        (self.offs[first], self.offs[last + 1])
    }

    /// Bytes this fence array takes in RAM (the struct and its heap slices).
    pub fn mem_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.prefix.len() + self.seps.len() + self.sep_end.len() * 4 + self.offs.len() * 8
    }

    pub fn parse(footer: &[u8]) -> anyhow::Result<(Fence, Vec<u8>, Vec<u8>)> {
        let mut r = footer;
        let mut take = |n: usize| -> anyhow::Result<&[u8]> {
            anyhow::ensure!(r.len() >= n, "short footer");
            let (a, b) = r.split_at(n);
            r = b;
            Ok(a)
        };
        let pl = u32::from_le_bytes(take(4)?.try_into()?) as usize;
        let prefix: Box<[u8]> = take(pl)?.into();
        let n = u32::from_le_bytes(take(4)?.try_into()?) as usize;
        let mut seps = Vec::new();
        let mut sep_end = Vec::with_capacity(n);
        for _ in 0..n {
            let l = u16::from_le_bytes(take(2)?.try_into()?) as usize;
            seps.extend_from_slice(take(l)?);
            sep_end.push(seps.len() as u32);
        }
        let mut offs = Vec::with_capacity(n + 1);
        for _ in 0..=n {
            offs.push(u64::from_le_bytes(take(8)?.try_into()?));
        }
        let fl = u32::from_le_bytes(take(4)?.try_into()?) as usize;
        let first = take(fl)?.to_vec();
        let ll = u32::from_le_bytes(take(4)?.try_into()?) as usize;
        let last = take(ll)?.to_vec();
        Ok((Fence { prefix, seps: seps.into_boxed_slice(), sep_end: sep_end.into_boxed_slice(), offs: offs.into_boxed_slice() }, first, last))
    }
}

// ---- decoded blocks and their cache

pub struct Block {
    raw: Vec<u8>,
    /// start of each entry in `raw`
    starts: Vec<u32>,
}

impl Block {
    pub fn decode(stored: &[u8]) -> anyhow::Result<Block> {
        let raw = lz4_flex::decompress_size_prepended(stored)?;
        let mut starts = Vec::new();
        let mut p = 0usize;
        while p < raw.len() {
            starts.push(p as u32);
            let kl = u16::from_le_bytes([raw[p], raw[p + 1]]) as usize;
            let vl = u32::from_le_bytes(raw[p + 2 + kl..p + 6 + kl].try_into()?) as usize;
            p += 6 + kl + vl;
        }
        anyhow::ensure!(p == raw.len(), "bad block");
        Ok(Block { raw, starts })
    }

    pub fn len(&self) -> usize {
        self.starts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.starts.is_empty()
    }

    pub fn key(&self, i: usize) -> &[u8] {
        let p = self.starts[i] as usize;
        let kl = u16::from_le_bytes([self.raw[p], self.raw[p + 1]]) as usize;
        &self.raw[p + 2..p + 2 + kl]
    }

    pub fn val(&self, i: usize) -> &[u8] {
        let p = self.starts[i] as usize;
        let kl = u16::from_le_bytes([self.raw[p], self.raw[p + 1]]) as usize;
        let vl = u32::from_le_bytes(self.raw[p + 2 + kl..p + 6 + kl].try_into().unwrap()) as usize;
        &self.raw[p + 6 + kl..p + 6 + kl + vl]
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

    pub fn mem_bytes(&self) -> usize {
        self.raw.capacity() + self.starts.capacity() * 4
    }
}

/// A byte-bounded LRU of decoded blocks (decoded size counted). The block just inserted always stays.
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

// ---- the reader (the waiter's side)

pub struct RunHandle {
    pub path: Path,
    pub first_key: Box<[u8]>,
    pub fence: Fence,
}

impl RunHandle {
    pub fn mem_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.path.as_ref().len() + self.first_key.len() + self.fence.mem_bytes() - std::mem::size_of::<Fence>()
    }
}

#[derive(Default, Debug, Clone, Copy, serde::Serialize)]
pub struct ReadStats {
    /// range GETs sent
    pub gets: u64,
    /// GETs waited for one after the other (all GETs of this reader are sequential, so trips = gets)
    pub trips: u64,
    pub bytes: u64,
    pub blocks_fetched: u64,
    pub cache_hits: u64,
}

pub struct Reader {
    pub store: Arc<dyn ObjectStore>,
    pub runs: Vec<RunHandle>,
    pub cache: Mutex<BlockCache>,
    /// blocks fetched per trip, counting back from the customer's last block (1 = only the block of its newest key)
    pub tail_blocks: usize,
    pub coll: String,
}

async fn get_range(store: &Arc<dyn ObjectStore>, path: &Path, a: u64, b: u64) -> anyhow::Result<Bytes> {
    let opts = GetOptions { range: Some(GetRange::Bounded(a..b)), ..Default::default() };
    Ok(store.get_opts(path, opts).await?.bytes().await?)
}

impl Reader {
    /// Loads the fence array of every run: two GETs per run (trailer, footer). Returns the GETs sent.
    pub async fn open(store: Arc<dyn ObjectStore>, runs: &[(String, u64)], cache_bytes: usize, tail_blocks: usize, coll: &str) -> anyhow::Result<(Reader, u64)> {
        let mut hs = Vec::new();
        let mut gets = 0;
        for (p, size) in runs {
            let path = Path::from(p.as_str());
            let t = get_range(&store, &path, size - TRAILER, *size).await?;
            gets += 1;
            let foff = u64::from_le_bytes(t[0..8].try_into()?);
            let flen = u32::from_le_bytes(t[8..12].try_into()?) as u64;
            anyhow::ensure!(u32::from_le_bytes(t[12..16].try_into()?) == MAGIC, "not a fence run: {p}");
            let f = get_range(&store, &path, foff, foff + flen).await?;
            gets += 1;
            let (fence, first, _last) = Fence::parse(&f)?;
            hs.push(RunHandle { path, first_key: first.into_boxed_slice(), fence });
        }
        hs.sort_by(|a, b| a.first_key.cmp(&b.first_key));
        Ok((Reader { store, runs: hs, cache: Mutex::new(BlockCache::new(cache_bytes)), tail_blocks: tail_blocks.max(1), coll: coll.to_string() }, gets))
    }

    /// RAM of all maps: the run directory and every fence array.
    pub fn map_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.runs.capacity() * std::mem::size_of::<RunHandle>() + self.runs.iter().map(|r| r.mem_bytes() - std::mem::size_of::<RunHandle>()).sum::<usize>()
    }

    pub fn blocks(&self) -> usize {
        self.runs.iter().map(|r| r.fence.blocks()).sum()
    }

    /// The run that holds keys starting with `p` (buckets are cut at customer boundaries).
    fn run_for(&self, p: &[u8]) -> Option<usize> {
        let n = self.runs.partition_point(|r| &r.first_key[..] <= p);
        let i = if n == 0 { 0 } else { n - 1 };
        // a customer whose prefix sorts before the first key of run n lives in run n - 1 or in run n (when the run starts
        // exactly with it): the first key of run n starts with p
        if n < self.runs.len() && self.runs[n].first_key.starts_with(p) {
            return Some(n);
        }
        if self.runs.is_empty() {
            None
        } else {
            Some(i)
        }
    }

    /// Fetches blocks first..=last of run `ri` (the ones not cached) in one range GET.
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
            let f = &self.runs[ri].fence;
            let (lo, hi) = f.range(a, z);
            let data = get_range(&self.store, &self.runs[ri].path, lo, hi).await?;
            st.gets += 1;
            st.trips += 1;
            st.bytes += data.len() as u64;
            let mut c = self.cache.lock().unwrap();
            for b in a..=z {
                let (x, y) = f.range(b, b);
                let blk = Arc::new(Block::decode(&data[(x - lo) as usize..(y - lo) as usize])?);
                st.blocks_fetched += 1;
                c.insert((ri as u32, b as u32), blk.clone());
                out[b - first] = Some(blk);
            }
        }
        Ok(out.into_iter().map(|b| b.unwrap()).collect())
    }

    /// The `limit` newest documents of `user` (newest first): the customer's keys are in `_id` order, and the `_id`
    /// of these messages grows with their time, so the newest are the last keys of the customer's range.
    pub async fn newest(&self, user: i64, limit: usize) -> anyhow::Result<(Vec<(Vec<u8>, Vec<u8>)>, ReadStats)> {
        let mut st = ReadStats::default();
        let p = customer_prefix(&self.coll, user);
        let e = prefix_end(&p);
        let Some(ri) = self.run_for(&p) else { return Ok((vec![], st)) };
        let Some((first, mut last)) = self.runs[ri].fence.blocks_for(&p, &e) else { return Ok((vec![], st)) };
        let mut got: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        loop {
            let a = (last + 1).saturating_sub(self.tail_blocks).max(first);
            let blocks = self.fetch(ri, a, last, &mut st).await?;
            for b in blocks.iter().rev() {
                let lo = b.lower_bound(&p);
                let hi = b.lower_bound(&e);
                for i in (lo..hi).rev() {
                    if got.len() == limit {
                        break;
                    }
                    got.push((b.key(i).to_vec(), b.val(i).to_vec()));
                }
            }
            if got.len() >= limit || a == first {
                break;
            }
            last = a - 1;
        }
        Ok((got, st))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn oid(n: u64) -> [u8; 12] {
        let mut o = [0u8; 12];
        o[0..4].copy_from_slice(&((n / 100) as u32).to_be_bytes());
        o[4..12].copy_from_slice(&n.to_be_bytes());
        o
    }

    async fn build(store: &Arc<dyn ObjectStore>, path: &str, users: &[(i64, u64)], block: usize) -> RunMeta {
        // users: (user, number of messages); message n of user u has value "u:n"
        let mut recs = Vec::new();
        let mut n = 0u64;
        for &(u, k) in users {
            for _ in 0..k {
                recs.push((doc_key("messages", u, &oid(n)), format!("{u}:{n}").into_bytes()));
                n += 1;
            }
        }
        recs.sort();
        let mut w = RunWriter::new(store, Path::from(path), 5 << 20, block).await.unwrap();
        for (k, v) in &recs {
            w.add(k, v).await.unwrap();
        }
        w.finish().await.unwrap()
    }

    #[test]
    fn customer_prefix_orders_customers_numerically() {
        let a = customer_prefix("messages", 9);
        let b = customer_prefix("messages", 10);
        let c = customer_prefix("messages", -3);
        assert!(c < a && a < b);
        assert!(doc_key("messages", 9, &[0xFF; 12]) < prefix_end(&a));
        assert!(prefix_end(&a) < b);
    }

    #[tokio::test]
    async fn newest_reads_match_an_exact_model_across_block_and_run_boundaries() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // small blocks (2 KiB) so customers span many blocks; two runs cut at a customer boundary
        let r1 = build(&store, "r/1", &[(1, 3), (2, 250), (3, 1), (5, 40)], 2048).await;
        let r2 = build(&store, "r/2", &[(6, 25), (7, 500), (9, 19)], 2048).await;
        assert!(r1.blocks > 5 && r2.blocks > 5);
        for tail in [1, 2] {
            let runs = vec![(r1.path.clone(), r1.size), (r2.path.clone(), r2.size)];
            let (rd, gets) = Reader::open(store.clone(), &runs, 1 << 20, tail, "messages").await.unwrap();
            assert_eq!(gets, 4);
            for (u, k) in [(1i64, 3usize), (2, 250), (3, 1), (5, 40), (6, 25), (7, 500), (9, 19), (4, 0), (8, 0), (100, 0), (0, 0)] {
                let (got, st) = rd.newest(u, 20).await.unwrap();
                assert_eq!(got.len(), k.min(20), "user {u}");
                // exact model: in each run, message n of a customer has key oid(n) and value "u:n", n counting from 0
                let mut want = Vec::new();
                for run in [&[(1i64, 3u64), (2, 250), (3, 1), (5, 40)][..], &[(6, 25), (7, 500), (9, 19)][..]] {
                    let mut n = 0u64;
                    for &(uu, kk) in run {
                        for _ in 0..kk {
                            if uu == u {
                                want.push((doc_key("messages", u, &oid(n)), format!("{u}:{n}").into_bytes()));
                            }
                            n += 1;
                        }
                    }
                }
                want.reverse();
                want.truncate(20);
                assert_eq!(got, want, "user {u}");
                if k > 0 {
                    assert!(st.gets >= 1 || st.cache_hits >= 1);
                }
            }
        }
    }

    #[tokio::test]
    async fn a_customer_inside_one_block_costs_one_range_get() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let r = build(&store, "r/1", &[(1, 30), (2, 30), (3, 30)], BLOCK_RAW).await;
        assert_eq!(r.blocks, 1);
        let (rd, _) = Reader::open(store.clone(), &[(r.path.clone(), r.size)], 1, 1, "messages").await.unwrap();
        let (got, st) = rd.newest(2, 20).await.unwrap();
        assert_eq!(got.len(), 20);
        assert_eq!((st.gets, st.trips), (1, 1));
    }

    #[test]
    fn separators_are_short_and_between_blocks() {
        let a = doc_key("messages", 1, &oid(5));
        let b = doc_key("messages", 1, &oid(6));
        let s = separator(&a, &b);
        assert!(s.as_slice() > a.as_slice() && s.as_slice() <= b.as_slice());
        let c = doc_key("messages", 2, &oid(0));
        let s2 = separator(&b, &c);
        assert!(s2.len() < c.len() && s2.as_slice() > b.as_slice());
    }

    #[test]
    fn cache_is_bounded_by_bytes() {
        let raw: Vec<u8> = {
            let mut v = Vec::new();
            v.extend(3u16.to_le_bytes());
            v.extend(b"abc");
            v.extend(1000u32.to_le_bytes());
            v.extend(vec![7u8; 1000]);
            v
        };
        let blk = Arc::new(Block::decode(&lz4_flex::compress_prepend_size(&raw)).unwrap());
        let mut c = BlockCache::new(2500);
        for i in 0..5 {
            c.insert((0, i), blk.clone());
        }
        assert!(c.used() <= 2500);
        assert!(c.get((0, 4)).is_some() && c.get((0, 0)).is_none());
    }
}
