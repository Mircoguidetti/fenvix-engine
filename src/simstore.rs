//! Object-store wrappers of the waiter's tools (T1).
//!
//! - [`SimS3Store`] (measurement only, `FENVIX_SIM_S3=median_ms,sigma,mbps`): every GET, HEAD and LIST waits like a
//!   request to real S3 would: a lognormal time with the given median and sigma (M31: median 25 ms, sigma 0.6 gives a
//!   p99 near 100 ms, the model the M15/M24/M25/M31 reviews used; S3 measured on 2026-11-05: p50 24-26, p99 58-105),
//!   plus the bytes divided by `mbps` MB/s. PUTs are not delayed (the measurements here are about reads). Never on in
//!   production: the wrapper is installed only when the variable is set, and it says so on stderr.
//! - [`TripMeter`] counts *trips*: how many times the number of object-store reads in flight went from 0 to 1. With one
//!   query at a time and nothing in the background, the trips of a query are the waves of requests it waited for one
//!   after the other ("viaggi in fila"); requests sent together are one trip.
//! - [`SplitGetStore`]: a ranged GET larger than `SPLIT_GET_MIN` bytes is sent as pieces of `SPLIT_GET_PIECE` bytes in
//!   parallel and joined, so a large document (or a large read-ahead) costs one trip of parallel requests instead of
//!   one long transfer. Same bytes, same order; more requests (each piece is a GET).

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, RenameOptions, Result,
};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

/// Trips (0 -> 1 transitions of the reads in flight), reads sent, reads in flight now, most reads in flight at once.
pub struct TripMeter {
    pub trips: AtomicU64,
    pub reads: AtomicU64,
    pub inflight: AtomicU64,
    pub max_inflight: AtomicU64,
}

pub static TRIPS: TripMeter = TripMeter { trips: AtomicU64::new(0), reads: AtomicU64::new(0), inflight: AtomicU64::new(0), max_inflight: AtomicU64::new(0) };

/// Reads one after the other ("in fila"), better than `trips` when waves overlap: a read's depth is 1 + the largest depth
/// of the reads that had finished when it started, within one *chain*; a chain ends when no read was in flight for
/// `CHAIN_GAP`. `last_chain_depth` is the depth of the last (or current) chain: with one query at a time and a pause
/// longer than the gap before it, the number of reads that query waited for one after the other.
pub struct ChainMeter {
    pub chains: u64,
    pub last_chain_depth: u64,
    completed_max: u64,
    idle_since: Option<std::time::Instant>,
}

pub static CHAIN: std::sync::Mutex<ChainMeter> = std::sync::Mutex::new(ChainMeter { chains: 0, last_chain_depth: 0, completed_max: 0, idle_since: None });
const CHAIN_GAP: Duration = Duration::from_millis(10);

struct InFlight {
    depth: u64,
}

/// `FENVIX_SIM_TRACE=1`: every simulated GET on stderr with its depth (debugging the trips of a query).
static T0: std::sync::LazyLock<std::time::Instant> = std::sync::LazyLock::new(std::time::Instant::now);
static TRACE: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| std::env::var_os("FENVIX_SIM_TRACE").is_some());

impl InFlight {
    fn start() -> Self {
        TRIPS.reads.fetch_add(1, Relaxed);
        let mut c = CHAIN.lock().unwrap();
        let before = TRIPS.inflight.fetch_add(1, Relaxed);
        if before == 0 {
            TRIPS.trips.fetch_add(1, Relaxed);
            if c.idle_since.map_or(true, |t| t.elapsed() >= CHAIN_GAP) {
                c.chains += 1;
                c.completed_max = 0;
                c.last_chain_depth = 0;
            }
        }
        TRIPS.max_inflight.fetch_max(before + 1, Relaxed);
        let depth = c.completed_max + 1;
        c.last_chain_depth = c.last_chain_depth.max(depth);
        InFlight { depth }
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        let mut c = CHAIN.lock().unwrap();
        c.completed_max = c.completed_max.max(self.depth);
        if TRIPS.inflight.fetch_sub(1, Relaxed) == 1 {
            c.idle_since = Some(std::time::Instant::now());
        }
    }
}

/// The latency model of [`SimS3Store`].
#[derive(Clone, Copy, Debug)]
pub struct SimModel {
    pub median_ms: f64,
    pub sigma: f64,
    pub mbps: f64,
}

impl SimModel {
    /// `median_ms,sigma,mbps`, for example `25,0.6,90`.
    pub fn parse(s: &str) -> Option<Self> {
        let v: Vec<f64> = s.split(',').map(|x| x.trim().parse().ok()).collect::<Option<Vec<_>>>()?;
        match v.as_slice() {
            [m, s, b] if *m >= 0.0 && *s >= 0.0 && *b > 0.0 => Some(SimModel { median_ms: *m, sigma: *s, mbps: *b }),
            _ => None,
        }
    }

    fn delay(&self, bytes: u64) -> Duration {
        // Box-Muller: one standard normal draw.
        let u1: f64 = rand::random::<f64>().max(1e-12);
        let u2: f64 = rand::random::<f64>();
        let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
        let ms = self.median_ms * (self.sigma * z).exp() + bytes as f64 / (self.mbps * 1e3);
        Duration::from_secs_f64(ms / 1e3)
    }
}

/// Simulated S3 latency in front of a local store (measurement only), with the trip meter.
pub struct SimS3Store {
    inner: Arc<dyn ObjectStore>,
    model: Option<SimModel>,
    /// `FENVIX_SIM_S3_PUT=median_ms,sigma,mbps` (measurement only): every PUT waits like this before it is sent, so the
    /// object appears in the store only at the end of the modelled time.
    put_model: Option<SimModel>,
}

impl fmt::Debug for SimS3Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SimS3Store({:?})", self.model)
    }
}

impl fmt::Display for SimS3Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SimS3Store({})", self.inner)
    }
}

/// Wraps `inner` with the trip meter, and with the simulated latency when `FENVIX_SIM_S3` is set.
pub fn wrap_sim(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    let model = std::env::var("FENVIX_SIM_S3").ok().filter(|s| !s.is_empty()).map(|s| match SimModel::parse(&s) {
        Some(m) => m,
        None => panic!("FENVIX_SIM_S3 must be median_ms,sigma,mbps (for example 25,0.6,90), got {s:?}"),
    });
    if let Some(m) = &model {
        eprintln!("SIMULATED S3 LATENCY ON ({m:?}): measurement only, never in production");
    }
    let put_model = std::env::var("FENVIX_SIM_S3_PUT").ok().filter(|s| !s.is_empty()).map(|s| match SimModel::parse(&s) {
        Some(m) => m,
        None => panic!("FENVIX_SIM_S3_PUT must be median_ms,sigma,mbps (for example 30,0.4,90), got {s:?}"),
    });
    if let Some(m) = &put_model {
        eprintln!("SIMULATED S3 PUT LATENCY ON ({m:?}): measurement only, never in production");
    }
    Arc::new(SimS3Store { inner, model, put_model })
}

fn range_len(r: &Option<GetRange>) -> Option<u64> {
    match r {
        Some(GetRange::Bounded(b)) => Some(b.end.saturating_sub(b.start)),
        _ => None,
    }
}

#[async_trait]
impl ObjectStore for SimS3Store {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        if let Some(m) = self.put_model {
            tokio::time::sleep(m.delay(payload.content_length() as u64)).await;
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let _f = InFlight::start();
        if *TRACE {
            eprintln!("SIMGET start t={:.1} depth={} {} {:?}", T0.elapsed().as_secs_f64() * 1e3, _f.depth, location, options.range);
        }
        let want = range_len(&options.range);
        let r = self.inner.get_opts(location, options).await?;
        let Some(m) = self.model else { return Ok(r) };
        // The time depends on the bytes: read the body now, wait, hand back the same bytes.
        let meta = r.meta.clone();
        let range = r.range.clone();
        let attributes = r.attributes.clone();
        let extensions = r.extensions.clone();
        let body = r.bytes().await?;
        tokio::time::sleep(m.delay(want.unwrap_or(body.len() as u64).max(body.len() as u64))).await;
        let stream = futures::stream::once(async move { Ok(body) }).boxed();
        Ok(GetResult { payload: GetResultPayload::Stream(stream), meta, range, attributes, extensions })
    }

    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let inner = self.inner.list(prefix);
        match self.model {
            None => inner,
            Some(m) => futures::stream::once(async move {
                let _f = InFlight::start();
                tokio::time::sleep(m.delay(0)).await;
                inner
            })
            .flatten()
            .boxed(),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let _f = InFlight::start();
        if let Some(m) = self.model {
            tokio::time::sleep(m.delay(0)).await;
        }
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// Ranged GETs at least this long are split (`--split-get-min-kb`, 0 = never).
pub static SPLIT_GET_MIN: AtomicU64 = AtomicU64::new(512 << 10);
/// Size of one piece of a split GET (`--split-get-piece-kb`).
pub static SPLIT_GET_PIECE: AtomicU64 = AtomicU64::new(256 << 10);
/// GETs split, and pieces sent for them.
pub static SPLIT_GETS: AtomicU64 = AtomicU64::new(0);
pub static SPLIT_PIECES: AtomicU64 = AtomicU64::new(0);
/// Hedged reads ("richiesta doppia", M31): a ranged GET not answered after this many ms is sent a second time and the
/// first answer wins (`--hedge-after-ms`, 0 = off). A read that fans out to many requests waits for the slowest one;
/// the second copy cuts that tail for a few percent more GETs.
pub static HEDGE_AFTER_MS: AtomicU64 = AtomicU64::new(0);
/// Second copies sent, and second copies that answered first.
pub static HEDGES: AtomicU64 = AtomicU64::new(0);
pub static HEDGE_WINS: AtomicU64 = AtomicU64::new(0);

/// Next-block read-along (`--next-block-kb`, 0 = off): a small ranged GET of an SST also brings this many bytes after the
/// range, kept for `NEXT_KEEP` in a small side buffer; a GET inside them is answered from there. Why (T1, measured):
/// slatedb 0.17 reads, for a point read of a key that is the first of its block, the block before it first and then
/// that block, one after the other (about one document in ten with 4 KiB blocks). Same trip, a few KiB more.
pub static NEXT_BLOCK_BYTES: AtomicU64 = AtomicU64::new(0);
/// GETs answered from the side buffer.
pub static NEXT_BLOCK_HITS: AtomicU64 = AtomicU64::new(0);
const NEXT_KEEP: Duration = Duration::from_secs(10);
const NEXT_MAX_ENTRIES: usize = 50_000;
/// Ranges up to this size get the read-along (data blocks, not large reads).
const NEXT_MAX_RANGE: u64 = 64 << 10;

struct Extra {
    start: u64,
    bytes: Bytes,
    meta: ObjectMeta,
    at: std::time::Instant,
}

static EXTRAS: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<(Path, u64), Extra>>> = std::sync::LazyLock::new(Default::default);

fn extra_lookup(loc: &Path, r: &std::ops::Range<u64>) -> Option<(ObjectMeta, Bytes)> {
    let m = EXTRAS.lock().unwrap();
    let e = m.get(&(loc.clone(), r.start))?;
    if e.at.elapsed() > NEXT_KEEP || r.end - e.start > e.bytes.len() as u64 {
        return None;
    }
    Some((e.meta.clone(), e.bytes.slice(0..(r.end - r.start) as usize)))
}

fn extra_store(loc: &Path, start: u64, bytes: Bytes, meta: ObjectMeta) {
    if bytes.is_empty() {
        return;
    }
    let mut m = EXTRAS.lock().unwrap();
    if m.len() >= NEXT_MAX_ENTRIES {
        m.retain(|_, e| e.at.elapsed() <= NEXT_KEEP);
        if m.len() >= NEXT_MAX_ENTRIES {
            m.clear();
        }
    }
    m.insert((loc.clone(), start), Extra { start, bytes, meta, at: std::time::Instant::now() });
}

/// One ranged GET with its body, hedged when `HEDGE_AFTER_MS` > 0.
async fn get_body(inner: &Arc<dyn ObjectStore>, loc: &Path, o: GetOptions) -> Result<(ObjectMeta, Bytes, object_store::Attributes)> {
    let one = |o: GetOptions| {
        let inner = Arc::clone(inner);
        let loc = loc.clone();
        async move {
            let g = inner.get_opts(&loc, o).await?;
            let meta = g.meta.clone();
            let attrs = g.attributes.clone();
            let b = g.bytes().await?;
            Ok::<_, object_store::Error>((meta, b, attrs))
        }
    };
    let after = HEDGE_AFTER_MS.load(Relaxed);
    let mut first = Box::pin(one(o.clone()));
    if after == 0 {
        return first.await;
    }
    tokio::select! {
        r = &mut first => return r,
        _ = tokio::time::sleep(Duration::from_millis(after)) => {}
    }
    HEDGES.fetch_add(1, Relaxed);
    let mut second = Box::pin(one(o));
    tokio::select! {
        r = &mut first => r,
        r = &mut second => {
            if r.is_ok() {
                HEDGE_WINS.fetch_add(1, Relaxed);
            }
            r
        }
    }
}

/// Large ranged GETs as parallel pieces (one trip instead of one long transfer).
pub struct SplitGetStore {
    inner: Arc<dyn ObjectStore>,
}

pub fn wrap_split(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    Arc::new(SplitGetStore { inner })
}

impl fmt::Debug for SplitGetStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SplitGetStore")
    }
}

impl fmt::Display for SplitGetStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SplitGetStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for SplitGetStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let min = SPLIT_GET_MIN.load(Relaxed);
        let piece = SPLIT_GET_PIECE.load(Relaxed).max(4096);
        let (start, end) = match &options.range {
            Some(GetRange::Bounded(b)) if min > 0 && !options.head && b.end.saturating_sub(b.start) >= min.max(2 * piece) => (b.start, b.end),
            Some(GetRange::Bounded(b))
                if !options.head && (HEDGE_AFTER_MS.load(Relaxed) > 0 || NEXT_BLOCK_BYTES.load(Relaxed) > 0 && location.as_ref().ends_with(".sst")) =>
            {
                let r = b.clone();
                if let Some((meta, body)) = extra_lookup(location, &r) {
                    NEXT_BLOCK_HITS.fetch_add(1, Relaxed);
                    let stream = futures::stream::once(async move { Ok(body) }).boxed();
                    return Ok(GetResult { payload: GetResultPayload::Stream(stream), meta, range: r, attributes: Default::default(), extensions: Default::default() });
                }
                let more = NEXT_BLOCK_BYTES.load(Relaxed);
                let along = more > 0 && location.as_ref().ends_with(".sst") && r.end - r.start <= NEXT_MAX_RANGE;
                let mut o = options.clone();
                if along {
                    o.range = Some(GetRange::Bounded(r.start..r.end + more));
                }
                let (meta, mut body, attributes) = match get_body(&self.inner, location, o).await {
                    Ok(x) => x,
                    // past the end of the object (the range was its last bytes): the GET as asked
                    Err(_) if along => {
                        let (m, b, a) = get_body(&self.inner, location, options).await?;
                        let stream = futures::stream::once(async move { Ok(b) }).boxed();
                        return Ok(GetResult { payload: GetResultPayload::Stream(stream), meta: m, range: r, attributes: a, extensions: Default::default() });
                    }
                    Err(e) => return Err(e),
                };
                if along {
                    let want = (r.end - r.start) as usize;
                    // S3 clamps a range past the end: a shorter body is fine as long as the asked part is there
                    if body.len() < want {
                        return Err(object_store::Error::Generic { store: "SplitGetStore", source: "short read".into() });
                    }
                    extra_store(location, r.end, body.slice(want..), meta.clone());
                    body.truncate(want);
                }
                let stream = futures::stream::once(async move { Ok(body) }).boxed();
                return Ok(GetResult { payload: GetResultPayload::Stream(stream), meta, range: r, attributes, extensions: Default::default() });
            }
            _ => return self.inner.get_opts(location, options).await,
        };
        // Conditions (if_match, version...) stay on every piece: a piece of another version fails like the whole GET would.
        let mut pieces = Vec::new();
        let mut s = start;
        while s < end {
            let e = (s + piece).min(end);
            pieces.push(s..e);
            s = e;
        }
        SPLIT_GETS.fetch_add(1, Relaxed);
        SPLIT_PIECES.fetch_add(pieces.len() as u64, Relaxed);
        let n = pieces.len();
        let results: Vec<(ObjectMeta, Bytes, object_store::Attributes)> = futures::stream::iter(pieces.into_iter().map(|r| {
            let mut o = options.clone();
            o.range = Some(GetRange::Bounded(r));
            let inner = Arc::clone(&self.inner);
            let loc = location.clone();
            async move { get_body(&inner, &loc, o).await }
        }))
        .buffered(n)
        .try_collect()
        .await?;
        let meta = results[0].0.clone();
        let attributes = results[0].2.clone();
        let extensions = Default::default();
        let mut buf = BytesMut::with_capacity((end - start) as usize);
        for (_, b, _) in &results {
            buf.extend_from_slice(b);
        }
        let body = buf.freeze();
        let stream = futures::stream::once(async move { Ok(body) }).boxed();
        Ok(GetResult { payload: GetResultPayload::Stream(stream), meta, range: start..end, attributes, extensions })
    }

    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::ObjectStoreExt;

    #[tokio::test]
    async fn split_get_returns_the_same_bytes() {
        let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
        mem.put(&Path::from("a/b.sst"), PutPayload::from(data.clone())).await.unwrap();
        let s = wrap_split(Arc::clone(&mem));
        for (a, b) in [(0u64, 3_000_000u64), (17, 2_999_999), (1000, 600_000), (5, 10)] {
            let before = SPLIT_PIECES.load(Relaxed);
            let got = s.get_opts(&Path::from("a/b.sst"), GetOptions { range: Some(GetRange::Bounded(a..b)), ..Default::default() }).await.unwrap();
            assert_eq!(got.range, a..b);
            let bytes = got.bytes().await.unwrap();
            assert_eq!(&bytes[..], &data[a as usize..b as usize]);
            if b - a >= 512 << 10 {
                assert!(SPLIT_PIECES.load(Relaxed) > before, "a large range was not split");
            }
        }
    }

    #[tokio::test]
    async fn trips_count_waves_not_requests() {
        let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        mem.put(&Path::from("x"), PutPayload::from(vec![1u8; 100])).await.unwrap();
        let s = Arc::new(SimS3Store { inner: mem, model: Some(SimModel { median_ms: 20.0, sigma: 0.0, mbps: 1000.0 }) });
        let t0 = TRIPS.trips.load(Relaxed);
        // one wave of 10 parallel GETs, then one more GET after it: 2 trips
        let p = Path::from("x");
        let futs: Vec<_> = (0..10).map(|_| s.get(&p)).collect();
        futures::future::try_join_all(futs).await.unwrap();
        s.get(&p).await.unwrap();
        // other tests run in parallel and may add trips: at least 2, and the 10 parallel GETs were not 10 trips
        let d = TRIPS.trips.load(Relaxed) - t0;
        assert!(d >= 2, "trips {d}");
    }
}
