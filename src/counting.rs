//! An `ObjectStore` wrapper that counts every request it forwards, so request
//! costs can be computed from real call counts.

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result, UploadPart,
};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

/// S3 returns at most 1000 keys per LIST page.
const LIST_PAGE_SIZE: u64 = 1000;

#[derive(Debug, Default)]
pub struct Counters {
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

#[derive(Debug)]
pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    pub counters: Arc<Counters>,
}

impl CountingStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner, counters: Arc::new(Counters::default()) }
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
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.counters.multipart_create.fetch_add(1, Relaxed);
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(CountingUpload { inner, counters: Arc::clone(&self.counters) }))
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
        self.inner.get_opts(location, options).await
    }

    // `get_ranges` is intentionally not overridden: the default implementation
    // coalesces ranges and calls `self.get_range` for each resulting request,
    // so every HTTP GET is counted individually in `get_opts`.

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let counters = Arc::clone(&self.counters);
        let locations = locations
            .inspect(move |_| {
                counters.delete.fetch_add(1, Relaxed);
            })
            .boxed();
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.count_list(self.inner.list(prefix))
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, Result<ObjectMeta>> {
        self.count_list(self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.counters.list.fetch_add(1, Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.counters.copy.fetch_add(1, Relaxed);
        self.inner.copy_opts(from, to, options).await
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
