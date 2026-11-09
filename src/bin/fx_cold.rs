//! Compaction 2, step 5: what a cold read costs once young data exists (prompt P-c2-step5-coldreads).
//!
//! Usage: `cargo run --release --bin fx_cold -- <profile> <r1 on|off> <r3 on|off> <out.jsonl> [D MiB] [seed]`
//! with profile `1tb-medium`, `1tb-busy` or `10tb-medium`.
//!
//! The replay is step 4's (`fx_replay.rs`): a writer tenant in process on an in-memory store, chat-like documents of about
//! 400 bytes for 4,000 background customers (Zipf 1.1) in key layout v2, D loaded and sealed first, then the writer
//! reopens with L0 SSTs scaled so the delta tier has the tiers of full size, and the step 4 compactor runs with the drop
//! on. On top of it:
//! - customers with a write pattern (idle, once, weekly, daily, session, hourly, bursty), 16 each, with 150 documents of
//!   history written during the load, and agent threads (LangGraph Mongo-saver checkpoints and writes, sizes from the
//!   checkpoint-delta harness): old ones written during the load, young ones written during the replay;
//! - time: one compressed day is D / (days of inserts per D) bytes of inserts (26.7 at 1 TB medium, 5.34 at 1 TB busy,
//!   267 at 10 TB medium), so every pattern is placed on the byte clock;
//! - background point reads at the profile's cold-read rate per GB (20/s over 1 TB medium, 80/s busy, 20/s over 10 TB),
//!   which feed R3 the way the production reads would;
//! - every 1/26 of the measured window: cold reads of pattern customers (latest 20, a wide history range, one document
//!   by id) and of agent threads (get_tuple of the latest checkpoint and of an old step), each with the run block cache
//!   cleared and SlateDB's block cache at 16 KiB, maps (run fences and filters, SlateDB indexes and filters) in RAM, an
//!   injected S3 latency per GET, every GET counted by where it went, and every answer checked against an exact model.
//!
//! R1 on: delta runs up to 256 L0 SSTs of raw bytes (2 GiB at full size) keep a copy in an in-memory "NVMe" store.
//! R3 on: the compactor reseals a group once its delta GETs pass 470 per GiB of the group.

use anyhow::{bail, Result};
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart};
use object_store::{CopyOptions, RenameOptions};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, HashMap};
use std::io::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tenant_bench::counting::CountingStore;
use tenant_bench::fx;
use tenant_bench::lifecycle::{self, Lifecycle, Op, Tenant};

// ---------------------------------------------------------------------------------------------------------------------
// Byte tags (as in fx_replay): what the writer and the compaction side wrote.

#[derive(Default)]
struct Bytes {
    m: Mutex<HashMap<(&'static str, &'static str), u64>>,
}

static BYTES: std::sync::LazyLock<Bytes> = std::sync::LazyLock::new(Bytes::default);

fn class(p: &str) -> &'static str {
    if p.contains("/fx/run/") {
        "fxrun"
    } else if p.contains("/fx/") {
        "fxmeta"
    } else if p.contains("/compacted/") {
        "sst"
    } else if p.contains("/wal/") {
        "wal"
    } else {
        "other"
    }
}

fn add(tag: &'static str, p: &str, n: u64) {
    *BYTES.m.lock().unwrap().entry((tag, class(p))).or_default() += n;
}

fn snap() -> HashMap<(&'static str, &'static str), u64> {
    BYTES.m.lock().unwrap().clone()
}

// ---------------------------------------------------------------------------------------------------------------------
// The read meter: counts the GETs of a measured read by where they go, injects the S3 latency, counts trips.

/// Where a GET went.
const SRC: [&str; 5] = ["sealed", "delta_s3", "l0", "slate_runs", "nvme"];

/// On while a measured read runs (SlateDB GETs may run in spawned tasks, so this is global; the writer is paused and the
/// compaction side uses its own store, so every counted GET on the writer's store is the read's).
static READ_ON: AtomicBool = AtomicBool::new(false);
tokio::task_local! {
    /// Set inside a measured read: the NVMe store is shared with the compactor, so its reads count only from the read's task.
    static READING: ();
}
/// Injected latency of one S3 GET, and of one NVMe read (microseconds).
static S3_LAT_US: AtomicU64 = AtomicU64::new(5_000);
static NVME_LAT_US: AtomicU64 = AtomicU64::new(100);

#[derive(Default)]
struct Meter {
    gets: [AtomicU64; 5],
    bytes: AtomicU64,
    trips: AtomicU64,
    inflight: AtomicU64,
    /// SlateDB's L0 SST ids at the start of the read (Debug strings of the ids; a path holds the ULID).
    l0: Mutex<Vec<String>>,
}

static METER: std::sync::LazyLock<Meter> = std::sync::LazyLock::new(Meter::default);

impl Meter {
    fn reset(&self, l0: Vec<String>) {
        for g in &self.gets {
            g.store(0, Relaxed);
        }
        self.bytes.store(0, Relaxed);
        self.trips.store(0, Relaxed);
        *self.l0.lock().unwrap() = l0;
    }
    fn src(&self, p: &str, nvme: bool) -> Option<usize> {
        if nvme {
            return p.contains("/fx/run/").then_some(4);
        }
        if p.contains("/fx/run/delta") {
            Some(1)
        } else if p.contains("/fx/run/") {
            Some(0)
        } else if let Some(i) = p.find("/compacted/") {
            let name = &p[i + 11..];
            let ulid = name.split('.').next().unwrap_or(name);
            Some(if self.l0.lock().unwrap().iter().any(|s| s.contains(ulid)) { 2 } else { 3 })
        } else {
            None // manifests, WAL: not part of a read
        }
    }
}

#[derive(Debug)]
struct Metered {
    inner: Arc<dyn ObjectStore>,
    nvme: bool,
}

impl std::fmt::Display for Metered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Metered({})", self.nvme)
    }
}

#[async_trait]
impl ObjectStore for Metered {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let counted = READ_ON.load(Relaxed) && (!self.nvme || READING.try_with(|_| ()).is_ok());
        let src = if counted { METER.src(location.as_ref(), self.nvme) } else { None };
        let Some(src) = src else { return self.inner.get_opts(location, options).await };
        METER.gets[src].fetch_add(1, Relaxed);
        if !self.nvme && METER.inflight.fetch_add(1, Relaxed) == 0 {
            METER.trips.fetch_add(1, Relaxed);
        }
        let lat = if self.nvme { NVME_LAT_US.load(Relaxed) } else { S3_LAT_US.load(Relaxed) };
        tokio::time::sleep(Duration::from_micros(lat)).await;
        let r = self.inner.get_opts(location, options).await;
        if !self.nvme {
            METER.inflight.fetch_sub(1, Relaxed);
            if let Ok(g) = &r {
                METER.bytes.fetch_add(g.range.end - g.range.start, Relaxed);
            }
        }
        r
    }
    fn delete_stream(&self, locations: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[derive(Debug)]
struct Tag {
    inner: Arc<dyn ObjectStore>,
    tag: &'static str,
}

impl std::fmt::Display for Tag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Tag({})", self.tag)
    }
}

#[async_trait]
impl ObjectStore for Tag {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        add(self.tag, location.as_ref(), payload.content_length() as u64);
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        let u = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(TagUpload { inner: u, tag: self.tag, path: location.to_string() }))
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(&self, locations: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[derive(Debug)]
struct TagUpload {
    inner: Box<dyn MultipartUpload>,
    tag: &'static str,
    path: String,
}

#[async_trait]
impl MultipartUpload for TagUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        add(self.tag, &self.path, data.content_length() as u64);
        self.inner.put_part(data)
    }
    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.inner.complete().await
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// Documents and keys (layout v2 as in fx_replay: u/{len}{customer}{collection}/d/{id}).

const WORDS: &[&str] = &["the", "agent", "replied", "with", "a", "summary", "of", "order", "status", "please", "check", "invoice", "shipping", "tomorrow", "thanks", "customer", "support", "ticket", "update", "refund", "account", "password", "reset", "delivery", "address", "confirmed", "pending", "error"];

fn doc(rng: &mut StdRng, n: usize) -> Vec<u8> {
    let mut d = Vec::with_capacity(n + 16);
    d.extend(b"{\"role\":\"user\",\"ts\":");
    d.extend(format!("{},\"text\":\"", rng.gen::<u32>()).as_bytes());
    while d.len() < n {
        if rng.gen_bool(0.5) {
            d.extend(WORDS[rng.gen_range(0..WORDS.len())].as_bytes());
        } else {
            for _ in 0..rng.gen_range(3..9) {
                d.push(b'a' + rng.gen_range(0..26));
            }
        }
        d.push(b' ');
    }
    d.extend(b"\"}");
    d
}

fn prefix(name: &str, coll: &str) -> Vec<u8> {
    let mut k = b"u/".to_vec();
    k.push(name.len() as u8);
    k.extend(name.as_bytes());
    k.extend(coll.as_bytes());
    k.extend(b"/d/");
    k
}

fn bg_name(c: u32) -> String {
    format!("c{c:05}")
}

fn idkey(name: &str, i: u64) -> Vec<u8> {
    let mut k = prefix(name, "msgs");
    k.extend(i.to_be_bytes());
    k
}

/// The end of a prefix range (prefix with its last byte + 1; the prefixes here never end in 0xff).
fn prefix_end(p: &[u8]) -> Vec<u8> {
    let mut e = p.to_vec();
    *e.last_mut().unwrap() += 1;
    e
}

fn h64(b: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    b.hash(&mut h);
    h.finish()
}

struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    fn new(n: usize, s: f64) -> Zipf {
        let mut cdf = Vec::with_capacity(n);
        let mut acc = 0.0;
        for i in 1..=n {
            acc += 1.0 / (i as f64).powf(s);
            cdf.push(acc);
        }
        for x in cdf.iter_mut() {
            *x /= acc;
        }
        Zipf { cdf }
    }
    fn sample(&self, rng: &mut StdRng) -> u32 {
        let u: f64 = rng.gen();
        self.cdf.partition_point(|&x| x < u) as u32
    }
}

const NBG: usize = 4000;

/// The background writer of fx_replay: 70% new documents, 25% rewrites, 5% deletes.
struct Bg {
    rng: StdRng,
    z: Zipf,
    next: Vec<u64>,
}

impl Bg {
    fn ops(&mut self, n: usize, ops: &mut Vec<Op>) -> u64 {
        let mut bytes = 0;
        for _ in 0..n {
            let c = self.z.sample(&mut self.rng);
            let r: f64 = self.rng.gen();
            let have = self.next[c as usize];
            let name = bg_name(c);
            if r < 0.70 || have == 0 {
                let k = idkey(&name, have);
                self.next[c as usize] += 1;
                let len = self.rng.gen_range(250..550);
                let v = doc(&mut self.rng, len);
                bytes += (k.len() + v.len()) as u64;
                ops.push(Op::Put(k, v));
            } else if r < 0.95 {
                let k = idkey(&name, self.rng.gen_range(0..have));
                let len = self.rng.gen_range(250..550);
                let v = doc(&mut self.rng, len);
                bytes += (k.len() + v.len()) as u64;
                ops.push(Op::Put(k, v));
            } else {
                let k = idkey(&name, self.rng.gen_range(0..have));
                bytes += k.len() as u64;
                ops.push(Op::Delete(k));
            }
        }
        bytes
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// Pattern customers and agent threads.

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Pat {
    Idle,
    Once,
    Weekly,
    Daily,
    Session,
    Hourly,
    Bursty,
}

const PATS: [Pat; 7] = [Pat::Idle, Pat::Once, Pat::Weekly, Pat::Daily, Pat::Session, Pat::Hourly, Pat::Bursty];
const PER_PAT: usize = 16;
const HISTORY: u64 = 150;

impl Pat {
    fn name(self) -> &'static str {
        match self {
            Pat::Idle => "idle",
            Pat::Once => "once",
            Pat::Weekly => "weekly",
            Pat::Daily => "daily",
            Pat::Session => "session",
            Pat::Hourly => "hourly",
            Pat::Bursty => "bursty",
        }
    }
    fn letter(self) -> char {
        match self {
            Pat::Idle => 'i',
            Pat::Once => 'o',
            Pat::Weekly => 'w',
            Pat::Daily => 'd',
            Pat::Session => 's',
            Pat::Hourly => 'h',
            Pat::Bursty => 'b',
        }
    }
    /// Write times (days from the start of the replay) over a horizon of `h` days.
    fn times(self, rng: &mut StdRng, h: f64) -> Vec<f64> {
        let mut t = Vec::new();
        let exp = |rng: &mut StdRng, mean: f64| -> f64 { -mean * (1.0 - rng.gen::<f64>()).ln() };
        match self {
            Pat::Idle => {}
            Pat::Once => t.push(rng.gen::<f64>() * h),
            Pat::Weekly => {
                let mut x = rng.gen::<f64>() * 7.0;
                while x < h {
                    t.push(x);
                    x += 7.0 + rng.gen_range(-1.0..1.0);
                }
            }
            Pat::Daily => {
                let hour = rng.gen::<f64>();
                let mut d = 0.0;
                while d < h {
                    let x = d + (hour + rng.gen_range(-0.08..0.08)).clamp(0.0, 0.999);
                    if x < h {
                        t.push(x);
                    }
                    d += 1.0;
                }
            }
            Pat::Session => {
                let hour = rng.gen::<f64>() * 0.9;
                let mut d = 0.0;
                while d < h {
                    let s = d + hour + rng.gen_range(0.0..0.05);
                    for i in 0..24 {
                        let x = s + i as f64 * 5.0 / 1440.0;
                        if x < h {
                            t.push(x);
                        }
                    }
                    d += 1.0;
                }
            }
            Pat::Hourly => {
                let mut x = rng.gen::<f64>() / 24.0;
                while x < h {
                    t.push(x);
                    x += (1.0 + rng.gen_range(-0.17..0.17)) / 24.0;
                }
            }
            Pat::Bursty => {
                let mut x = exp(rng, 3.0);
                while x < h {
                    for i in 0..30 {
                        let y = x + i as f64 * (10.0 / 30.0) / 1440.0;
                        if y < h {
                            t.push(y);
                        }
                    }
                    x += exp(rng, 3.0);
                }
            }
        }
        t
    }
}

struct Cust {
    name: String,
    pat: Pat,
    next: u64,
    /// id -> hash of the live document
    model: BTreeMap<u64, u64>,
    last_write: u64,
}

impl Cust {
    fn write(&mut self, rng: &mut StdRng, ops: &mut Vec<Op>, now: u64) -> u64 {
        let r: f64 = rng.gen();
        self.last_write = now;
        if r < 0.80 || self.model.is_empty() {
            let id = self.next;
            self.next += 1;
            let n = rng.gen_range(250..550);
            let v = doc(rng, n);
            self.model.insert(id, h64(&v));
            let k = idkey(&self.name, id);
            let b = (k.len() + v.len()) as u64;
            ops.push(Op::Put(k, v));
            b
        } else {
            let id = rng.gen_range(0..self.next);
            let k = idkey(&self.name, id);
            if r < 0.95 {
                let n = rng.gen_range(250..550);
            let v = doc(rng, n);
                self.model.insert(id, h64(&v));
                let b = (k.len() + v.len()) as u64;
                ops.push(Op::Put(k, v));
                b
            } else {
                self.model.remove(&id);
                let b = k.len() as u64;
                ops.push(Op::Delete(k));
                b
            }
        }
    }
}

/// One step of an agent thread: the checkpoint document and its pending writes (sizes from the harness).
struct Step {
    ck: String,
    ck_bytes: usize,
    writes: Vec<(String, usize)>,
}

struct Thread {
    name: String,
    steps: Vec<Step>,
    written: usize,
    young: bool,
    last_write: u64,
}

fn filler(rng: &mut StdRng, n: usize) -> Vec<u8> {
    let mut v = doc(rng, n.max(40));
    v.truncate(n.max(40));
    v
}

impl Thread {
    fn write_step(&mut self, rng: &mut StdRng, ops: &mut Vec<Op>, now: u64) -> u64 {
        let s = &self.steps[self.written];
        let mut b = 0;
        let mut k = prefix(&self.name, "checkpoints");
        k.extend(s.ck.as_bytes());
        let v = filler(rng, s.ck_bytes);
        b += (k.len() + v.len()) as u64;
        ops.push(Op::Put(k, v));
        for (w, n) in &s.writes {
            let mut k = prefix(&self.name, "checkpoint_writes");
            k.extend(s.ck.as_bytes());
            k.push(b':');
            k.extend(w.as_bytes());
            let v = filler(rng, *n);
            b += (k.len() + v.len()) as u64;
            ops.push(Op::Put(k, v));
        }
        self.written += 1;
        self.last_write = now;
        b
    }
}

fn load_threads(path: &str) -> Result<Vec<(String, Vec<Step>)>> {
    let mut out: Vec<(String, Vec<Step>)> = Vec::new();
    for line in std::fs::read_to_string(path)?.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() < 4 {
            continue;
        }
        let writes = f[4..]
            .iter()
            .map(|w| {
                let (a, b) = w.rsplit_once('=').unwrap();
                (a.to_string(), b.parse().unwrap())
            })
            .collect();
        let st = Step { ck: f[2].to_string(), ck_bytes: f[3].parse()?, writes };
        match out.last_mut() {
            Some((t, s)) if t == f[0] => s.push(st),
            _ => out.push((f[0].to_string(), vec![st])),
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------------------------------------------
// Measured reads.

struct Res {
    gets: [u64; 5],
    bytes: u64,
    trips: u64,
    ms: f64,
    ok: bool,
    rows: usize,
}

static RETRIES: AtomicU64 = AtomicU64::new(0);

async fn measured<F, Fut>(t: &Tenant, f: F) -> Result<Res>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<(bool, usize)>>,
{
    let mut attempt = 0;
    loop {
        fx::view::clear_cache();
        METER.reset(t.writer_l0_ids());
        READ_ON.store(true, Relaxed);
        let s = Instant::now();
        let r = READING.scope((), f()).await;
        let ms = s.elapsed().as_secs_f64() * 1000.0;
        READ_ON.store(false, Relaxed);
        match r {
            Ok((ok, rows)) => return Ok(Res { gets: std::array::from_fn(|i| METER.gets[i].load(Relaxed)), bytes: METER.bytes.load(Relaxed), trips: METER.trips.load(Relaxed), ms, ok, rows }),
            // a retryable error (a hold revoked): the server answers the client with a retryable error; read again
            Err(e) if attempt < 3 && e.downcast_ref::<fx::Retry>().is_some() => {
                attempt += 1;
                RETRIES.fetch_add(1, Relaxed);
                eprintln!("retry after {ms:.0} ms: {e}");
            }
            Err(e) => return Err(e),
        }
    }
}

fn id_of(k: &[u8]) -> u64 {
    u64::from_be_bytes(k[k.len() - 8..].try_into().unwrap())
}

const READ_AHEAD: usize = 256 << 10;

async fn read_latest(t: &Tenant, c: &Cust) -> Result<(bool, usize)> {
    let p = prefix(&c.name, "msgs");
    let got = t.scan_range(&p, Some(&prefix_end(&p)), true, Some(20), READ_AHEAD).await?;
    let want: Vec<(u64, u64)> = c.model.iter().rev().take(20).map(|(a, b)| (*a, *b)).collect();
    let have: Vec<(u64, u64)> = got.iter().map(|(k, v)| (id_of(k), h64(v))).collect();
    Ok((want == have, got.len()))
}

async fn read_range(t: &Tenant, c: &Cust) -> Result<(bool, usize)> {
    let lo_id = c.next / 4;
    let hi_id = (lo_id + (c.next / 2).max(1)).min(lo_id + 500);
    let lo = idkey(&c.name, lo_id);
    let hi = idkey(&c.name, hi_id);
    let got = t.scan_range(&lo, Some(&hi), false, None, READ_AHEAD).await?;
    let want: Vec<(u64, u64)> = c.model.range(lo_id..hi_id).map(|(a, b)| (*a, *b)).collect();
    let have: Vec<(u64, u64)> = got.iter().map(|(k, v)| (id_of(k), h64(v))).collect();
    Ok((want == have, got.len()))
}

async fn read_byid(t: &Tenant, c: &Cust, id: u64) -> Result<(bool, usize)> {
    let got = t.get(idkey(&c.name, id)).await?;
    let want = c.model.get(&id).copied();
    Ok((got.as_ref().map(|v| h64(v)) == want, got.is_some() as usize))
}

/// get_tuple of the Mongo saver: the checkpoint (latest: the last in id order; or one by id), then its pending writes.
async fn read_tuple(t: &Tenant, th: &Thread, step: Option<usize>) -> Result<(bool, usize)> {
    let p = prefix(&th.name, "checkpoints");
    let (ck_key, want_step) = match step {
        None => {
            let got = t.scan_range(&p, Some(&prefix_end(&p)), true, Some(1), READ_AHEAD).await?;
            let Some((k, v)) = got.into_iter().next() else { return Ok((th.written == 0, 0)) };
            let want = th.written - 1;
            if v.len() != th.steps[want].ck_bytes.max(40) {
                return Ok((false, 1));
            }
            (k.to_vec(), want)
        }
        Some(s) => {
            let mut k = p.clone();
            k.extend(th.steps[s].ck.as_bytes());
            let got = t.get(&k).await?;
            if got.map(|v| v.len()) != Some(th.steps[s].ck_bytes.max(40)) {
                return Ok((false, 0));
            }
            (k, s)
        }
    };
    let ck = &ck_key[p.len()..];
    let mut wp = prefix(&th.name, "checkpoint_writes");
    wp.extend(ck);
    wp.push(b':');
    let ws = t.scan_range(&wp, Some(&prefix_end(&wp)), false, None, READ_AHEAD).await?;
    let want = &th.steps[want_step].writes;
    let ok = ws.len() == want.len() && ws.iter().zip(want).all(|((_, v), (_, n))| v.len() == (*n).max(40));
    Ok((ok, 1 + ws.len()))
}

// ---------------------------------------------------------------------------------------------------------------------

struct Profile {
    days_per_d: f64,
    tiers_div: f64,
    warm: f64,
    window: f64,
    /// production cold reads per second over the database
    cold_per_s: f64,
    db_gb: f64,
}

fn profile(p: &str) -> Result<Profile> {
    Ok(match p {
        "1tb-medium" => Profile { days_per_d: 26.7, tiers_div: 16_384.0, warm: 0.25, window: 0.5, cold_per_s: 20.0, db_gb: 1000.0 },
        "1tb-busy" => Profile { days_per_d: 5.34, tiers_div: 16_384.0, warm: 0.6, window: 1.5, cold_per_s: 80.0, db_gb: 1000.0 },
        "10tb-medium" => Profile { days_per_d: 267.0, tiers_div: 163_840.0, warm: 0.1, window: 0.25, cold_per_s: 20.0, db_gb: 10_000.0 },
        _ => bail!("profile: 1tb-medium, 1tb-busy or 10tb-medium"),
    })
}

fn mib(x: u64) -> f64 {
    x as f64 / (1 << 20) as f64
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let a: Vec<String> = std::env::args().collect();
    if a.len() < 5 {
        bail!("usage: fx_cold <1tb-medium|1tb-busy|10tb-medium> <r1 on|off> <r3 on|off> <out.jsonl> [D MiB] [seed]");
    }
    let pname = a[1].clone();
    let pr = profile(&pname)?;
    let r1 = a[2] == "on";
    let r3 = a[3] == "on";
    let out_path = a[4].clone();
    let d_mib: u64 = a.get(5).and_then(|s| s.parse().ok()).unwrap_or(1024);
    let seed: u64 = a.get(6).and_then(|s| s.parse().ok()).unwrap_or(5);
    let wave = std::env::var("FX_WAVE").is_ok();
    let no_settle = std::env::var("FX_NO_SETTLE").is_ok();
    let as_built = std::env::var("FX_NO_RANGE_FILTER").is_ok();
    if as_built {
        fx::view::RANGE_FILTER.store(false, Relaxed);
    }
    if let Ok(x) = std::env::var("FX_S3_LAT_US") {
        S3_LAT_US.store(x.parse()?, Relaxed);
    }
    let threads_path = std::env::var("FX_THREADS").unwrap_or_else(|_| "research/c2-step5-coldreads/checkpoint-threads.txt".into());
    let theta = 0.25;
    let d = d_mib << 20;
    let l0 = ((theta * d as f64 / 2.0) / pr.tiers_div).max(512.0) as u64;
    let day = d as f64 / pr.days_per_d; // bytes of inserts per compressed day
    let horizon = (pr.warm + pr.window) * pr.days_per_d;
    let bg_reads_per_day = pr.cold_per_s * 86_400.0 / pr.db_gb * (d as f64 / (1u64 << 30) as f64);
    let tag = format!("{pname} r1 {} r3 {}{}", if r1 { "on" } else { "off" }, if r3 { "on" } else { "off" }, format!("{}{}", if wave { " wave" } else { "" }, if no_settle { " unsettled" } else { "" }) + if as_built { " no-range-filter" } else { "" } + if std::env::var("FX_MINIO").is_ok() { " minio" } else { "" });
    println!("fx_cold: {tag}, D {d_mib} MiB, L0 {l0} B, day {:.1} MiB of inserts, warm-up {:.1} days, window {:.1} days, background cold reads {bg_reads_per_day:.0} per day, S3 latency {} us, seed {seed}", day / 1048576.0, pr.warm * pr.days_per_d, pr.window * pr.days_per_d, S3_LAT_US.load(Relaxed));
    let mut out = std::fs::File::create(&out_path)?;

    tenant_bench::tiered::set_compression("lz4")?;
    fx::ENABLED.store(true, Relaxed);
    fx::E1_WAIT_MS.store(0, Relaxed);
    fx::hold::HOLD_LIFE_MS.store(10_000, Relaxed); // step 4 used 2 s; a cold read with injected latency on a loaded sandbox can pass it
    fx::T_PIN_MS.store(1000, Relaxed);
    lifecycle::WRITER_POLL_SECS.store(1, Relaxed);
    // SlateDB: maps in RAM (2 GiB of index and filter cache), data blocks cold (16 KiB of block cache)
    lifecycle::install_shared_cache(16 << 10, 2 << 30);
    fx::view::CACHE_BYTES.store(64 << 20, Relaxed);
    if wave {
        fx::view::WAVE.store(true, Relaxed);
    }
    // FX_MINIO=<bucket>: the store is a local MinIO (http://localhost:9000, minioadmin) instead of memory; the injected
    // latency still applies on top of MinIO's own.
    let mem: Arc<dyn ObjectStore> = match std::env::var("FX_MINIO") {
        Ok(bucket) => Arc::new(
            object_store::aws::AmazonS3Builder::new()
                .with_endpoint(std::env::var("FX_MINIO_URL").unwrap_or_else(|_| "http://localhost:9000".into()))
                .with_allow_http(true)
                .with_region("us-east-1")
                .with_bucket_name(bucket)
                .with_access_key_id("minioadmin")
                .with_secret_access_key("minioadmin")
                .build()?,
        ),
        Err(_) => Arc::new(InMemory::new()),
    };
    let metered: Arc<dyn ObjectStore> = Arc::new(Metered { inner: Arc::clone(&mem), nvme: false });
    let w_store: Arc<dyn ObjectStore> = Arc::new(Tag { inner: metered, tag: "w" });
    let c_store: Arc<dyn ObjectStore> = Arc::new(Tag { inner: Arc::clone(&mem), tag: "c" });
    if r1 {
        fx::local::set_store(Some(Arc::new(Metered { inner: Arc::new(InMemory::new()), nvme: true })));
    }
    let lc = Lifecycle::new(Arc::new(CountingStore::new(Arc::clone(&w_store))), None);
    let id = "cold";
    let mut rng = StdRng::seed_from_u64(seed);
    let mut bg = Bg { rng: StdRng::seed_from_u64(seed + 1), z: Zipf::new(NBG, 1.1), next: vec![0; NBG] };
    let mut custs: Vec<Cust> = Vec::new();
    for p in PATS {
        for i in 0..PER_PAT {
            let n = rng.gen_range(0..NBG as u32);
            custs.push(Cust { name: format!("c{n:05}{}{i:02}", p.letter()), pat: p, next: 0, model: BTreeMap::new(), last_write: 0 });
        }
    }
    let raw_threads = load_threads(&threads_path)?;
    // old threads: written during the load; young ones: during the replay (10-step and 40-step threads, half each)
    let mut threads: Vec<Thread> = Vec::new();
    for (i, (tname, steps)) in raw_threads.into_iter().enumerate() {
        let n = rng.gen_range(0..NBG as u32);
        let young = i % 2 == 0;
        threads.push(Thread { name: format!("c{n:05}t{tname}"), steps, written: 0, young, last_write: 0 });
    }
    println!("{} pattern customers, {} agent threads ({} young)", custs.len(), threads.len(), threads.iter().filter(|t| t.young).count());

    // 1. load D (large L0 SSTs), pattern histories and old threads spread over it, then seal it
    lifecycle::L0_SST_BYTES_OVERRIDE.store(16 << 20, Relaxed);
    lifecycle::L0_MAX_SSTS_OVERRIDE.store(10_000, Relaxed);
    let t0 = Instant::now();
    let mut t = lc.create(id).await?;
    let mut written = 0u64;
    let hist_total = custs.len() as u64 * HISTORY;
    let old_steps: usize = threads.iter().filter(|t| !t.young).map(|t| t.steps.len()).sum();
    let batches_est = (d / (256 * 420)).max(1);
    let (mut hist_done, mut steps_done) = (0u64, 0usize);
    let mut nb = 0u64;
    while written < d {
        let mut ops = Vec::new();
        written += bg.ops(256, &mut ops);
        nb += 1;
        while hist_done < hist_total && hist_done * batches_est < nb * hist_total {
            let nc = custs.len() as u64;
            let c = &mut custs[(hist_done % nc) as usize];
            let id = c.next;
            c.next += 1;
            let n = rng.gen_range(250..550);
            let v = doc(&mut rng, n);
            c.model.insert(id, h64(&v));
            let k = idkey(&c.name, id);
            written += (k.len() + v.len()) as u64;
            ops.push(Op::Put(k, v));
            hist_done += 1;
        }
        while steps_done < old_steps && (steps_done as u64) * batches_est < nb * old_steps as u64 {
            let th = threads.iter_mut().find(|t| !t.young && t.written < t.steps.len()).unwrap();
            written += th.write_step(&mut rng, &mut ops, 0);
            steps_done += 1;
        }
        t.apply(ops).await?;
    }
    // what the estimate left: finish histories and old threads
    let mut ops = Vec::new();
    while hist_done < hist_total {
        let nc = custs.len() as u64;
            let c = &mut custs[(hist_done % nc) as usize];
        written += c.write(&mut rng, &mut ops, 0);
        hist_done += 1;
    }
    for th in threads.iter_mut().filter(|t| !t.young) {
        while th.written < th.steps.len() {
            written += th.write_step(&mut rng, &mut ops, 0);
        }
    }
    if !ops.is_empty() {
        t.apply(ops).await?;
    }
    for c in custs.iter_mut() {
        c.last_write = 0;
    }
    println!("loaded {:.0} MiB in {:.1} s", mib(written), t0.elapsed().as_secs_f64());
    let groups = 64u64;
    let opts = fx::build::BuildOpts { reseal: fx::build::Reseal::All, group_bytes: d / groups, ..Default::default() };
    loop {
        if t.fx_publish(&opts, None).await?.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    println!("first version built: {} groups", t.fx_view().unwrap().groups.len());
    t.close().await?;

    // 2. reopen with small L0 SSTs and start the compactor
    lifecycle::L0_SST_BYTES_OVERRIDE.store(l0, Relaxed);
    lifecycle::L0_MAX_SSTS_OVERRIDE.store(64, Relaxed);
    let mut t = lc.wake(id).await?;
    t.upgrade().await?;
    let slots = 3;
    let rt: &'static tokio::runtime::Runtime = Box::leak(Box::new(tokio::runtime::Builder::new_multi_thread().worker_threads(slots).thread_name("fx-pool").enable_all().build()?));
    let pool = fx::pool::Pool::new(slots);
    let k = fx::compactor::Knobs {
        theta,
        r3,
        tier_unit: 4 * l0,
        absorb_min_l0: 4,
        absorb_max_l0: 8,
        local_max_bytes: if r1 { 256 * l0 } else { 0 },
        split_bytes: 2 * d / groups,
        merge_bytes: d / groups / 4,
        min_seal_bytes: d / groups / 2,
        drop: true,
        lease: Duration::from_millis(1600),
        remote_lease: Duration::from_millis(12_000), // F6: above hold life plus PUT timeout
        serving_lease: Duration::from_millis(200),
        put_timeout: Duration::from_millis(1000),
        gc_grace: Duration::from_millis(12_000),
        version_retention: Duration::ZERO,
        gc_every: Duration::from_secs(2),
        tick: Duration::from_millis(5),
        ..Default::default()
    };
    let comp = fx::compactor::Compactor::start(&mut t, Arc::clone(&c_store), k, pool, rt.handle().clone()).await?;
    comp.run();
    let gc_mem = Arc::clone(&mem);
    let gc_stop = Arc::new(AtomicBool::new(false));
    let gs = Arc::clone(&gc_stop);
    let gc_task = tokio::spawn(async move {
        while !gs.load(Relaxed) {
            tokio::time::sleep(Duration::from_secs(3)).await;
            let _ = lifecycle::gc_tenant(&gc_mem, "cold", Duration::from_secs(5)).await;
        }
    });

    // 3. the schedule: pattern writes and young agent threads on the byte clock (day 0 = this point)
    let base = written;
    let at = |days: f64| base + (days * day) as u64;
    #[derive(Clone, Copy)]
    enum Ev {
        Doc(usize),
        Step(usize),
    }
    let mut evs: Vec<(u64, Ev)> = Vec::new();
    for (i, c) in custs.iter().enumerate() {
        for x in c.pat.times(&mut rng, horizon) {
            evs.push((at(x), Ev::Doc(i)));
        }
    }
    for (i, th) in threads.iter().enumerate().filter(|(_, t)| t.young) {
        // a young thread runs once, at a random time of the horizon, one step every 20 seconds
        let s = rng.gen::<f64>() * horizon * 0.9;
        for j in 0..th.steps.len() {
            evs.push((at(s + j as f64 * 20.0 / 86_400.0), Ev::Step(i)));
        }
    }
    evs.sort_by_key(|e| e.0);
    let mut evi = 0;
    let warm_end = at(pr.warm * pr.days_per_d);
    let end = at(horizon);
    let n_ck = 26u64;
    let mut cks: Vec<u64> = (1..=n_ck).map(|i| warm_end + (end - warm_end) * i / n_ck).collect();
    cks.reverse();
    let mut phase_start = snap();
    let mut base_stats = None;
    let mut started = false;
    let mut bg_owed = 0.0f64;
    let mut bg_reads = 0u64;
    let mut wrong = 0u64;
    let mut nreads = 0u64;
    let t_start = Instant::now();
    let mut last_print = Instant::now();
    let mut max_l0 = 0usize;
    let mut ck_no = 0;
    let reseal_base = |c: &fx::compactor::Compactor| (c.sh.stats.reseals.load(Relaxed), c.sh.stats.r3_reseals.load(Relaxed), c.sh.stats.theta_reseals.load(Relaxed), c.sh.stats.reseal_raw_out.load(Relaxed), c.sh.stats.absorbed_raw.load(Relaxed), c.sh.stats.merge_raw_out.load(Relaxed));
    while written < end {
        if !started && written >= warm_end {
            started = true;
            phase_start = snap();
            base_stats = Some(reseal_base(&comp));
            println!("measured window starts at {:.0} MiB written ({:.1} days)", mib(written - base), (written - base) as f64 / day);
        }
        t.fx_sync();
        let mut ops = Vec::new();
        let mut b = bg.ops(32, &mut ops);
        while evi < evs.len() && evs[evi].0 <= written {
            match evs[evi].1 {
                Ev::Doc(i) => b += custs[i].write(&mut rng, &mut ops, written),
                Ev::Step(i) => b += threads[i].write_step(&mut rng, &mut ops, written),
            }
            evi += 1;
        }
        t.apply(ops).await?;
        written += b;
        if let Some(n) = t.writer_l0() {
            max_l0 = max_l0.max(n);
        }
        // background cold point reads (feed R3 as production reads would)
        bg_owed += b as f64 / day * bg_reads_per_day;
        while bg_owed >= 1.0 {
            bg_owed -= 1.0;
            let c = bg.z.sample(&mut rng) as usize; // a reader that follows the writers' popularity would skew; uniform is below
            let c = if rng.gen_bool(0.5) { c } else { rng.gen_range(0..NBG) };
            let have = bg.next[c];
            if have > 0 {
                fx::view::clear_cache();
                if let Err(e) = t.get(idkey(&bg_name(c as u32), rng.gen_range(0..have))).await {
                    if e.downcast_ref::<fx::Retry>().is_none() {
                        return Err(e);
                    }
                    RETRIES.fetch_add(1, Relaxed);
                }
                bg_reads += 1;
            }
        }
        if last_print.elapsed() > Duration::from_secs(20) {
            last_print = Instant::now();
            let (g, dr) = comp.sh.pending.lock().unwrap().as_ref().map_or((0, 0), |v| (v.groups.len(), v.delta.len()));
            println!("  {:.0} s: day {:.2}, groups {g}, delta runs {dr}, reseals {} (r3 {}), L0 max {max_l0}, reads {nreads}, wrong {wrong}", t_start.elapsed().as_secs_f64(), (written - base) as f64 / day, comp.sh.stats.reseals.load(Relaxed), comp.sh.stats.r3_reseals.load(Relaxed));
        }
        // a measurement point
        if cks.last().is_some_and(|&c| written >= c) {
            cks.pop();
            ck_no += 1;
            t.fx_sync();
            // Settle (default): the replay compresses time about a thousand times but the drop's lease only about 40, so
            // SlateDB holds about a compressed day of rows above the floor; in production it holds about a minute. With
            // writes paused, wait until the compactor is idle and SlateDB keeps at most the L0 SSTs not absorbed yet
            // (up to 3) and one sorted run (the envelope marker stays), or 15 s. FX_NO_SETTLE=1 reads as the replay runs.
            let settle_t = Instant::now();
            if !no_settle {
                loop {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    t.fx_sync();
                    let l0n = t.writer_l0().unwrap_or(0);
                    let srn = t.writer_sorted_runs().unwrap_or(0);
                    if (comp.idle() && l0n <= 3 && srn <= 1) || settle_t.elapsed() > Duration::from_secs(15) {
                        break;
                    }
                }
            }
            let settle_s = settle_t.elapsed().as_secs_f64();
            let now_day = (written - base) as f64 / day;
            // maps in RAM: open every run of the served version (footers, fences, filters), outside the meter
            let v = t.fx_view().unwrap().clone();
            for g in &v.groups {
                if let Some(s) = &g.sealed {
                    s.open(&v.store).await?;
                }
            }
            for dr in &v.delta {
                dr.open(&v.store).await?;
            }
            // SlateDB's indexes and filters of every SST: one unmetered point read and one scan
            let _ = t.get(idkey(&bg_name(0), 0)).await;
            let _ = t.scan_range(&prefix(&bg_name(1), "msgs"), Some(&prefix_end(&prefix(&bg_name(1), "msgs"))), true, Some(1), READ_AHEAD).await;
            let n_delta = v.delta.len();
            let n_local = v.delta.iter().filter(|d| d.line.local).count();
            let l0_now = t.writer_l0().unwrap_or(0);
            let sr_now = t.writer_sorted_runs().unwrap_or(0);
            let mut emit = |pat: &str, kind: &str, who: &str, age: f64, r: &Res| -> Result<()> {
                nreads += 1;
                if !r.ok {
                    wrong += 1;
                }
                writeln!(
                    out,
                    "{{\"cfg\":\"{tag}\",\"ck\":{ck_no},\"day\":{now_day:.3},\"pat\":\"{pat}\",\"kind\":\"{kind}\",\"who\":\"{who}\",\"age\":{age:.4},\"sealed\":{},\"delta_s3\":{},\"l0\":{},\"slate_runs\":{},\"nvme\":{},\"trips\":{},\"bytes\":{},\"ms\":{:.3},\"ok\":{},\"rows\":{},\"n_delta\":{n_delta},\"n_local\":{n_local},\"l0_ssts\":{l0_now},\"slate_srs\":{sr_now},\"settle_s\":{settle_s:.2}}}",
                    r.gets[0], r.gets[1], r.gets[2], r.gets[3], r.gets[4], r.trips, r.bytes, r.ms, r.ok, r.rows
                )?;
                Ok(())
            };
            for p in PATS {
                let idx: Vec<usize> = (0..custs.len()).filter(|&i| custs[i].pat == p).collect();
                for _ in 0..4 {
                    let i = idx[rng.gen_range(0..idx.len())];
                    let c = &custs[i];
                    let age = if c.last_write == 0 { now_day + 1.0 } else { (written - c.last_write) as f64 / day };
                    let r = measured(&t, || read_latest(&t, c)).await?;
                    emit(p.name(), "latest20", &c.name, age, &r)?;
                    let r = measured(&t, || read_range(&t, c)).await?;
                    emit(p.name(), "range", &c.name, age, &r)?;
                    let id = rng.gen_range(0..c.next);
                    let r = measured(&t, || read_byid(&t, c, id)).await?;
                    emit(p.name(), "byid", &c.name, age, &r)?;
                }
            }
            // the busiest background writers (Zipf top 20): no model, answers not checked
            for _ in 0..4 {
                let cidx = rng.gen_range(0..20u32);
                let name = bg_name(cidx);
                let p = prefix(&name, "msgs");
                let r = measured(&t, || async { Ok((true, t.scan_range(&p, Some(&prefix_end(&p)), true, Some(20), READ_AHEAD).await?.len())) }).await?;
                emit("zipftop", "latest20", &name, 0.0, &r)?;
                let have = bg.next[cidx as usize].max(1);
                let id = rng.gen_range(0..have);
                let r = measured(&t, || async { Ok((true, t.get(idkey(&name, id)).await?.is_some() as usize)) }).await?;
                emit("zipftop", "byid", &name, 0.0, &r)?;
            }
            // agent threads: get_tuple latest and an old step
            for young in [true, false] {
                let idx: Vec<usize> = (0..threads.len()).filter(|&i| threads[i].young == young && threads[i].written > 0).collect();
                if idx.is_empty() {
                    continue;
                }
                for _ in 0..4 {
                    let th = &threads[idx[rng.gen_range(0..idx.len())]];
                    let age = if th.last_write == 0 { now_day + 1.0 } else { (written - th.last_write) as f64 / day };
                    let pat = if young { "agent_young" } else { "agent_old" };
                    let size = if th.steps.len() > 20 { "40" } else { "10" };
                    let r = measured(&t, || read_tuple(&t, th, None)).await?;
                    emit(pat, &format!("tuple_latest_{size}"), &th.name, age, &r)?;
                    let s = rng.gen_range(0..th.written.max(1)).min(th.written / 4);
                    let r = measured(&t, || read_tuple(&t, th, Some(s))).await?;
                    emit(pat, &format!("tuple_old_{size}"), &th.name, age, &r)?;
                }
            }
            out.flush()?;
        }
    }
    let wall = t_start.elapsed().as_secs_f64();
    let end_snap = snap();
    let dl = |tag: &'static str, cl: &'static str| end_snap.get(&(tag, cl)).copied().unwrap_or(0) - phase_start.get(&(tag, cl)).copied().unwrap_or(0);
    let l0_bytes = dl("w", "sst");
    let amp = (l0_bytes + dl("c", "sst") + dl("c", "fxrun")) as f64 / l0_bytes.max(1) as f64;
    let s = &comp.sh.stats;
    let b0 = base_stats.unwrap();
    let ab = s.absorbed_raw.load(Relaxed) - b0.4;
    let re = s.reseal_raw_out.load(Relaxed) - b0.3;
    let me = s.merge_raw_out.load(Relaxed) - b0.5;
    println!("RESULT {tag}: reads {nreads}, wrong {wrong}, retried {}; background point reads {bg_reads}; L0 max {max_l0}; wall {wall:.0} s", RETRIES.load(Relaxed));
    println!("RESULT {tag}: in the window: reseals {} (r3 {}, theta {}), stored amplification {amp:.2} (L0 {:.1} MiB, SlateDB {:.1} MiB, our runs {:.1} MiB), raw: absorbed {:.1} MiB, merges {:.1} MiB, reseals {:.1} MiB ({:.2} per absorbed byte)", s.reseals.load(Relaxed) - b0.0, s.r3_reseals.load(Relaxed) - b0.1, s.theta_reseals.load(Relaxed) - b0.2, mib(l0_bytes), mib(dl("c", "sst")), mib(dl("c", "fxrun")), mib(ab), mib(me), mib(re), re as f64 / ab.max(1) as f64);
    writeln!(out, "{{\"summary\":true,\"cfg\":\"{tag}\",\"reads\":{nreads},\"wrong\":{wrong},\"amp\":{amp:.3},\"reseals\":{},\"r3_reseals\":{},\"theta_reseals\":{},\"absorbed_raw\":{ab},\"merge_raw\":{me},\"reseal_raw\":{re},\"l0_bytes\":{l0_bytes},\"runs_bytes\":{},\"slate_bytes\":{},\"bg_reads\":{bg_reads},\"max_l0\":{max_l0},\"wall_s\":{wall:.0},\"retries\":{}}}", s.reseals.load(Relaxed) - b0.0, s.r3_reseals.load(Relaxed) - b0.1, s.theta_reseals.load(Relaxed) - b0.2, dl("c", "fxrun"), dl("c", "sst"), RETRIES.load(Relaxed))?;
    gc_stop.store(true, Relaxed);
    let _ = gc_task.await;
    comp.stop().await;
    t.abandon().await;
    Ok(())
}
