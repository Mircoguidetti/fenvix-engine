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

struct InFlight;

impl InFlight {
    fn start() -> Self {
        TRIPS.reads.fetch_add(1, Relaxed);
        let before = TRIPS.inflight.fetch_add(1, Relaxed);
        if before == 0 {
            TRIPS.trips.fetch_add(1, Relaxed);
        }
        TRIPS.max_inflight.fetch_max(before + 1, Relaxed);
        InFlight
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        TRIPS.inflight.fetch_sub(1, Relaxed);
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
    Arc::new(SimS3Store { inner, model })
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
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let _f = InFlight::start();
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
            async move {
                let g = inner.get_opts(&loc, o).await?;
                let meta = g.meta.clone();
                let attrs = g.attributes.clone();
                let b = g.bytes().await?;
                Ok::<_, object_store::Error>((meta, b, attrs))
            }
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
