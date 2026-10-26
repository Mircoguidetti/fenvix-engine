//! An `ObjectStore` wrapper that counts every request it forwards, so request
//! costs can be computed from real call counts.

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result, UploadPart,
};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// S3 returns at most 1000 keys per LIST page.
const LIST_PAGE_SIZE: u64 = 1000;

/// Latency of object-store calls in power-of-two microsecond buckets (bucket `i` counts calls that took
/// less than `2^i` microseconds and at least `2^(i-1)`): enough to see where tail latency comes from.
#[derive(Debug)]
pub struct LatBuckets {
    pub b: [AtomicU64; 32],
}

impl Default for LatBuckets {
    fn default() -> Self {
        Self { b: std::array::from_fn(|_| AtomicU64::new(0)) }
    }
}

impl LatBuckets {
    pub fn record(&self, d: std::time::Duration) {
        let us = d.as_micros() as u64;
        let i = (64 - us.leading_zeros() as usize).min(31);
        self.b[i].fetch_add(1, Relaxed);
    }

    pub fn snapshot(&self) -> Vec<u64> {
        self.b.iter().map(|x| x.load(Relaxed)).collect()
    }
}

#[derive(Debug, Default)]
pub struct Counters {
    pub lat_put: LatBuckets,
    pub lat_get: LatBuckets,
    pub put: AtomicU64,
    pub multipart_create: AtomicU64,
    pub multipart_part: AtomicU64,
    pub multipart_complete: AtomicU64,
    pub copy: AtomicU64,
    pub get: AtomicU64,
    pub get_range: AtomicU64,
    pub head: AtomicU64,
    pub list: AtomicU64,
    pub delete: AtomicU64,
}

/// A point-in-time copy of the counters.
#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
pub struct Snapshot {
    pub put: u64,
    pub multipart_create: u64,
    pub multipart_part: u64,
    pub multipart_complete: u64,
    pub copy: u64,
    pub get: u64,
    pub get_range: u64,
    pub head: u64,
    pub list: u64,
    pub delete: u64,
}

impl Counters {
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            put: self.put.load(Relaxed),
            multipart_create: self.multipart_create.load(Relaxed),
            multipart_part: self.multipart_part.load(Relaxed),
            multipart_complete: self.multipart_complete.load(Relaxed),
            copy: self.copy.load(Relaxed),
            get: self.get.load(Relaxed),
            get_range: self.get_range.load(Relaxed),
            head: self.head.load(Relaxed),
            list: self.list.load(Relaxed),
            delete: self.delete.load(Relaxed),
        }
    }
}

impl Snapshot {
    pub fn since(&self, earlier: &Snapshot) -> Snapshot {
        Snapshot {
            put: self.put - earlier.put,
            multipart_create: self.multipart_create - earlier.multipart_create,
            multipart_part: self.multipart_part - earlier.multipart_part,
            multipart_complete: self.multipart_complete - earlier.multipart_complete,
            copy: self.copy - earlier.copy,
            get: self.get - earlier.get,
            get_range: self.get_range - earlier.get_range,
            head: self.head - earlier.head,
            list: self.list - earlier.list,
            delete: self.delete - earlier.delete,
        }
    }

    /// PUT-type calls as defined for this benchmark: put, put_multipart, copy.
    pub fn put_type(&self) -> u64 {
        self.put + self.multipart_create + self.multipart_part + self.multipart_complete + self.copy
    }

    /// GET-type calls as defined for this benchmark: get, get_range(s), head, list.
    pub fn get_type(&self) -> u64 {
        self.get + self.get_range + self.head + self.list
    }

    /// Requests billed at the S3 "PUT, COPY, POST, LIST" tier.
    pub fn billed_put_tier(&self) -> u64 {
        self.put_type() + self.list
    }

    /// Requests billed at the S3 "GET, SELECT and all other" tier (HEAD included).
    pub fn billed_get_tier(&self) -> u64 {
        self.get + self.get_range + self.head
    }

    pub fn total(&self) -> u64 {
        self.put_type() + self.get_type() + self.delete
    }
}

/// One recorded object-store call (trace mode only).
#[derive(Debug, Clone, serde::Serialize)]
pub struct TraceEvent {
    pub op: String,
    pub path: String,
    pub start_us: u64,
    pub dur_us: u64,
    pub bytes: u64,
    /// True if any other traced call was in flight at some point during this one.
    pub overlapped: bool,
}

#[derive(Debug, Default)]
struct TraceState {
    origin: Option<Instant>,
    /// Bumped on every `start`, so a call that outlives its trace cannot write into the next one.
    generation: u64,
    events: Vec<TraceEvent>,
    inflight: Vec<usize>,
}

/// Identifies one in-flight traced call: (trace generation, event index).
type Token = (u64, usize);

/// Records every call in order. Disabled by default: when off, `begin` is a
/// single atomic load and the store behaves exactly as without tracing.
#[derive(Debug, Default)]
pub struct Trace {
    enabled: AtomicBool,
    state: Mutex<TraceState>,
}

impl Trace {
    pub fn start(&self) {
        let mut st = self.state.lock().unwrap();
        let generation = st.generation + 1;
        *st = TraceState { origin: Some(Instant::now()), generation, ..Default::default() };
        self.enabled.store(true, Relaxed);
    }

    pub fn take(&self) -> Vec<TraceEvent> {
        self.enabled.store(false, Relaxed);
        let mut st = self.state.lock().unwrap();
        // Calls still in flight keep their start time; give them the time elapsed so far.
        let now = st.origin.map(|o| o.elapsed().as_micros() as u64).unwrap_or(0);
        for i in std::mem::take(&mut st.inflight) {
            let ev = &mut st.events[i];
            ev.dur_us = now.saturating_sub(ev.start_us);
        }
        st.generation += 1;
        std::mem::take(&mut st.events)
    }

    fn begin(&self, op: &str, path: &str) -> Option<Token> {
        if !self.enabled.load(Relaxed) {
            return None;
        }
        let mut st = self.state.lock().unwrap();
        let origin = st.origin?;
        let idx = st.events.len();
        let overlapped = !st.inflight.is_empty();
        for i in st.inflight.clone() {
            st.events[i].overlapped = true;
        }
        st.events.push(TraceEvent {
            op: op.to_string(),
            path: path.to_string(),
            start_us: origin.elapsed().as_micros() as u64,
            dur_us: 0,
            bytes: 0,
            overlapped,
        });
        st.inflight.push(idx);
        Some((st.generation, idx))
    }

    fn end(&self, token: Option<Token>, bytes: u64) {
        let Some((generation, idx)) = token else { return };
        let mut st = self.state.lock().unwrap();
        if st.generation != generation {
            return;
        }
        let Some(origin) = st.origin else { return };
        let now = origin.elapsed().as_micros() as u64;
        if let Some(ev) = st.events.get_mut(idx) {
            ev.dur_us = now.saturating_sub(ev.start_us);
            ev.bytes = bytes;
        }
        st.inflight.retain(|&i| i != idx);
    }

    fn is_on(&self) -> bool {
        self.enabled.load(Relaxed)
    }
}

#[derive(Debug)]
pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    pub counters: Arc<Counters>,
    pub trace: Arc<Trace>,
}

impl CountingStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            counters: Arc::new(Counters::default()),
            trace: Arc::new(Trace::default()),
        }
    }

    /// In trace mode, a list call is timed from its first poll until every page
    /// has been read, so the stream is drained before it is handed back.
    fn trace_list(
        &self,
        op: &'static str,
        prefix: Option<&Path>,
        stream: BoxStream<'static, Result<ObjectMeta>>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        if !self.trace.is_on() {
            return stream;
        }
        let trace = Arc::clone(&self.trace);
        let path = prefix.map(|p| p.to_string()).unwrap_or_default();
        futures::stream::once(async move {
            let token = trace.begin(op, &path);
            let items: Vec<Result<ObjectMeta>> = stream.collect().await;
            trace.end(token, 0);
            items
        })
        .flat_map(futures::stream::iter)
        .boxed()
    }

    /// Counts one LIST request when the stream is created and one more for
    /// every additional 1000-key page it yields.
    fn count_list(
        &self,
        stream: BoxStream<'static, Result<ObjectMeta>>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let counters = Arc::clone(&self.counters);
        counters.list.fetch_add(1, Relaxed);
        let mut seen = 0u64;
        stream
            .inspect(move |_| {
                seen += 1;
                if seen > LIST_PAGE_SIZE && (seen - 1) % LIST_PAGE_SIZE == 0 {
                    counters.list.fetch_add(1, Relaxed);
                }
            })
            .boxed()
    }
}

impl fmt::Display for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        self.counters.put.fetch_add(1, Relaxed);
        let op = match opts.mode {
            PutMode::Create => "PUT(create)",
            PutMode::Update(_) => "PUT(update)",
            PutMode::Overwrite => "PUT",
        };
        let token = self.trace.begin(op, location.as_ref());
        let bytes = payload.content_length() as u64;
        let t0 = Instant::now();
        let res = self.inner.put_opts(location, payload, opts).await;
        self.counters.lat_put.record(t0.elapsed());
        self.trace.end(token, bytes);
        res
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.counters.multipart_create.fetch_add(1, Relaxed);
        let token = self.trace.begin("PUT_MULTIPART_CREATE", location.as_ref());
        let inner = self.inner.put_multipart_opts(location, opts).await;
        self.trace.end(token, 0);
        Ok(Box::new(CountingUpload { inner: inner?, counters: Arc::clone(&self.counters) }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let counter = if options.head {
            &self.counters.head
        } else if options.range.is_some() {
            &self.counters.get_range
        } else {
            &self.counters.get
        };
        counter.fetch_add(1, Relaxed);
        let op = if options.head {
            "HEAD"
        } else if options.range.is_some() {
            "GET_RANGE"
        } else {
            "GET"
        };
        let token = self.trace.begin(op, location.as_ref());
        let t0 = Instant::now();
        let mut res = self.inner.get_opts(location, options).await;
        self.counters.lat_get.record(t0.elapsed());
        if token.is_some() {
            // Read the body inside the timed window so the duration covers the
            // whole transfer, not just the response headers.
            if let Ok(r) = res.as_mut() {
                let payload = std::mem::replace(
                    &mut r.payload,
                    GetResultPayload::Stream(futures::stream::empty().boxed()),
                );
                if let GetResultPayload::Stream(body) = payload {
                    match body.try_collect::<Vec<_>>().await {
                        Ok(chunks) => {
                            r.payload = GetResultPayload::Stream(
                                futures::stream::iter(chunks.into_iter().map(Ok)).boxed(),
                            );
                        }
                        Err(e) => res = Err(e),
                    }
                }
            }
        }
        let bytes = res.as_ref().map(|r| r.range.end - r.range.start).unwrap_or(0);
        self.trace.end(token, bytes);
        res
    }

    // `get_ranges` is intentionally not overridden: the default implementation
    // coalesces ranges and calls `self.get_range` for each resulting request,
    // so every HTTP GET is counted individually in `get_opts`.

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let counters = Arc::clone(&self.counters);
        let trace = Arc::clone(&self.trace);
        let locations = locations
            .inspect(move |p| {
                counters.delete.fetch_add(1, Relaxed);
                if let Ok(p) = p {
                    // Deletes are instantaneous from the caller's view; record them as events.
                    let t = trace.begin("DELETE", p.as_ref());
                    trace.end(t, 0);
                }
            })
            .boxed();
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let stream = self.count_list(self.inner.list(prefix));
        self.trace_list("LIST", prefix, stream)
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, Result<ObjectMeta>> {
        let stream = self.count_list(self.inner.list_with_offset(prefix, offset));
        self.trace_list("LIST", prefix, stream)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.counters.list.fetch_add(1, Relaxed);
        let token = self.trace.begin("LIST", &prefix.map(|p| p.to_string()).unwrap_or_default());
        let res = self.inner.list_with_delimiter(prefix).await;
        self.trace.end(token, 0);
        res
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.counters.copy.fetch_add(1, Relaxed);
        let token = self.trace.begin("COPY", to.as_ref());
        let res = self.inner.copy_opts(from, to, options).await;
        self.trace.end(token, 0);
        res
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        // S3 has no rename: object_store issues a copy followed by a delete.
        self.counters.copy.fetch_add(1, Relaxed);
        self.counters.delete.fetch_add(1, Relaxed);
        self.inner.rename_opts(from, to, options).await
    }
}

#[derive(Debug)]
struct CountingUpload {
    inner: Box<dyn MultipartUpload>,
    counters: Arc<Counters>,
}

#[async_trait]
impl MultipartUpload for CountingUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.counters.multipart_part.fetch_add(1, Relaxed);
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        self.counters.multipart_complete.fetch_add(1, Relaxed);
        self.inner.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}
