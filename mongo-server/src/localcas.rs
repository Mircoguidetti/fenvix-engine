//! `--store local`: conditional overwrite for the file store.
//!
//! The control layer and the leases replace small objects only if nobody changed them since they were read (`PutMode::Update`,
//! S3's `If-Match: <etag>`). object_store's `LocalFileSystem` does not implement that mode, so every such write failed with
//! "PutMode::Update not yet implemented". This wrapper adds it: under one lock it compares the current entity tag with the one
//! the caller read and only then overwrites. The lock covers every write of the store, so the check and the write cannot be split
//! by another write of this process.
//!
//! Limit: the lock is inside the process. Two processes on the same directory are not protected against each other (S3 does this
//! on the server side, per object, and the engine relies on it for leases when several processes share a bucket). The local
//! store is for one engine process: development, tests, the `compat-gaps` harness.

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as OsResult,
};
use std::fmt;
use std::sync::Arc;

pub struct LocalCas {
    inner: Arc<dyn ObjectStore>,
    write_lock: tokio::sync::Mutex<()>,
}

impl LocalCas {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner, write_lock: tokio::sync::Mutex::new(()) }
    }
}

impl fmt::Debug for LocalCas {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalCas({})", self.inner)
    }
}

impl fmt::Display for LocalCas {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalCas({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for LocalCas {
    async fn put_opts(&self, location: &Path, payload: PutPayload, mut opts: PutOptions) -> OsResult<PutResult> {
        let _g = self.write_lock.lock().await;
        if let PutMode::Update(want) = &opts.mode {
            let stale = |why: &str| object_store::Error::Precondition { path: location.to_string(), source: why.to_string().into() };
            let head = match self.inner.get_opts(location, GetOptions { head: true, ..Default::default() }).await {
                Ok(r) => r.meta,
                Err(object_store::Error::NotFound { .. }) => return Err(stale("the object does not exist")),
                Err(e) => return Err(e),
            };
            let Some(want_tag) = want.e_tag.as_deref() else {
                return Err(object_store::Error::Generic { store: "LocalCas", source: "ETag required for conditional put".into() });
            };
            if head.e_tag.as_deref() != Some(want_tag) {
                return Err(stale("the entity tag changed"));
            }
            opts.mode = PutMode::Overwrite;
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(&self, locations: BoxStream<'static, OsResult<Path>>) -> BoxStream<'static, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> OsResult<()> {
        self.inner.rename_opts(from, to, options).await
    }
}
