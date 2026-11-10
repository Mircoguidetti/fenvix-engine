//! R1 (plan round 2, fix 6): the small delta tiers keep a copy on the waiter's NVMe.
//!
//! A small delta run (at most `Knobs::local_max_bytes`, about 2 GiB in production: tiers 0-3) is written once, as one
//! stream, to two places at the same time (`Tee`): the object store, which stays the durable copy (SlateDB drops the
//! absorbed L0 SSTs, so the store's copy is what keeps the rows after a crash or a takeover), and the NVMe store of this
//! process, under the same path. Its manifest line says `local: true`. A reader that has an NVMe store opens the run
//! there first (`view.rs`, `RunH::open`) and reads every block of it with 0 GETs; a reader without the copy (another
//! waiter after a takeover, a restart on a fresh disk) reads the store's copy, as for any run. The copy is only a cache:
//! GC deletes it once no version this process serves lists the run.
//!
//! The NVMe store is process-wide (`--sealed-local-dir`); tests use an in-memory store.

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart};
use object_store::{CopyOptions, RenameOptions};
use std::sync::{Arc, Mutex};

static LOCAL: Mutex<Option<Arc<dyn ObjectStore>>> = Mutex::new(None);

/// The NVMe store of this process, if any.
pub fn store() -> Option<Arc<dyn ObjectStore>> {
    LOCAL.lock().unwrap().clone()
}

/// Install (or remove) the NVMe store.
pub fn set_store(s: Option<Arc<dyn ObjectStore>>) {
    *LOCAL.lock().unwrap() = s;
}

/// An NVMe store on a local directory.
pub fn dir_store(dir: &std::path::Path) -> anyhow::Result<Arc<dyn ObjectStore>> {
    std::fs::create_dir_all(dir)?;
    Ok(Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir)?))
}

/// Writes a multipart upload to the primary store and to the mirror in one pass; every other call goes to the primary.
/// The run is durable once the primary completes. A mirror that fails is dropped silently: the copy is only a cache.
#[derive(Debug)]
pub struct Tee {
    pub primary: Arc<dyn ObjectStore>,
    pub mirror: Arc<dyn ObjectStore>,
}

impl std::fmt::Display for Tee {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Tee({}, {})", self.primary, self.mirror)
    }
}

#[async_trait]
impl ObjectStore for Tee {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        self.primary.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        let p = self.primary.put_multipart_opts(location, opts).await?;
        let m = self.mirror.put_multipart_opts(location, PutMultipartOptions::default()).await.ok();
        Ok(Box::new(TeeUpload { p, m: Arc::new(tokio::sync::Mutex::new(m)) }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.primary.get_opts(location, options).await
    }

    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        self.primary.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.primary.list(prefix)
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, Result<ObjectMeta>> {
        self.primary.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.primary.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.primary.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.primary.rename_opts(from, to, options).await
    }
}

#[derive(Debug)]
struct TeeUpload {
    p: Box<dyn MultipartUpload>,
    m: Arc<tokio::sync::Mutex<Option<Box<dyn MultipartUpload>>>>,
}

#[async_trait]
impl MultipartUpload for TeeUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        // the mirror's part is started in order (it holds the lock while the part is queued), then awaited with the
        // primary's; a mirror error drops the mirror
        let mirror = match self.m.try_lock() {
            Ok(mut g) => g.as_mut().map(|u| u.put_part(data.clone())),
            Err(_) => None,
        };
        let primary = self.p.put_part(data);
        let m = Arc::clone(&self.m);
        Box::pin(async move {
            if let Some(f) = mirror {
                if f.await.is_err() {
                    *m.lock().await = None;
                }
            }
            primary.await
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let r = self.p.complete().await?;
        if let Some(mut u) = self.m.lock().await.take() {
            let _ = u.complete().await;
        }
        Ok(r)
    }

    async fn abort(&mut self) -> Result<()> {
        if let Some(mut u) = self.m.lock().await.take() {
            let _ = u.abort().await;
        }
        self.p.abort().await
    }
}
