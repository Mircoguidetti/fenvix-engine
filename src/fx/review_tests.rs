//! Review of compaction 2 step 3 (branch `review-c2-step3`): tests that fail on the delivered commit.

use super::build::{BuildOpts, Reseal};
use super::*;
use crate::counting::CountingStore;
use crate::lifecycle::{Lifecycle, Op, Tenant};
use crate::runfmt::{CustomerKind, RunOptions};
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};

/// What the store does with the next conditional creates of a manifest version.
#[derive(Clone, Copy, Debug)]
enum Act {
    /// The PUT lands, then the caller gets an error (a timeout after the object was written: outcome unknown).
    LandThenErr,
    /// The PUT does not land, and the caller gets an error (a timeout before the object was written).
    Err,
}

/// An in-memory store whose manifest creates fail as told.
#[derive(Debug)]
struct Flaky {
    inner: Arc<dyn ObjectStore>,
    acts: Mutex<std::collections::VecDeque<Act>>,
}

impl std::fmt::Display for Flaky {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Flaky({})", self.inner)
    }
}

fn timeout(p: &Path) -> object_store::Error {
    object_store::Error::Generic { store: "Flaky", source: format!("request timed out: PUT {p}").into() }
}

#[async_trait]
impl ObjectStore for Flaky {
    async fn put_opts(&self, l: &Path, p: PutPayload, o: PutOptions) -> object_store::Result<PutResult> {
        if matches!(o.mode, PutMode::Create) && l.as_ref().contains("/fx/manifest/") {
            let act = self.acts.lock().unwrap().pop_front();
            match act {
                Some(Act::LandThenErr) => {
                    self.inner.put_opts(l, p, o).await?;
                    return Err(timeout(l));
                }
                Some(Act::Err) => return Err(timeout(l)),
                None => {}
            }
        }
        self.inner.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(&self, l: &Path, o: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, l: &Path, o: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(l, o).await
    }
    fn delete_stream(&self, s: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(s)
    }
    fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(p)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, o: object_store::CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, o).await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, o: object_store::RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, o).await
    }
}

fn opts(reseal: Reseal) -> BuildOpts {
    BuildOpts { reseal, group_bytes: 16 << 10, sealed: RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2), delta: RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2) }
}

fn key(i: usize) -> Vec<u8> {
    format!("doc/review/{i:05}").into_bytes()
}

async fn put(t: &mut Tenant, i: usize, v: &[u8]) {
    t.apply(vec![Op::Put(key(i), v.to_vec())]).await.unwrap();
}

/// Rule 25 (RULES-FOR-THE-CODE, version ff27fb9): "whenever the waiter does not know the outcome of a PUT (... a PUT timed
/// out), it publishes a fence version before it pins any hold and before it starts another publish ... a fence that
/// times out is itself followed by another fence".
///
/// Here a publish's PUT lands and then times out (outcome unknown), and the fence that `fx_publish` writes after it
/// times out too. The delivered code prints the fence's error and carries on: the next read pins a hold on the version
/// it knew before the publish, while a newer version (the landed PUT) is in the store and no fence was published. Why it
/// matters: rule 24 and the NoFence traces of step 0 need the waiter to know the newest version before any hold; step 4's
/// drop floor takes the holds and the newest version in the store as true, so this is the state NoFence-timeout breaks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn review_no_hold_after_an_unknown_publish_whose_fence_failed() {
    let _s = super::tests::SERIAL.lock().await;
    ENABLED.store(true, Relaxed);
    PUBLISH_EVERY.store(0, Relaxed);
    T_PIN_MS.store(200, Relaxed);
    E1_WAIT_MS.store(0, Relaxed);
    let flaky = Arc::new(Flaky { inner: Arc::new(InMemory::new()), acts: Mutex::new(Default::default()) });
    let store: Arc<dyn ObjectStore> = flaky.clone();
    let id = "fx-review-fence";
    let lc = Lifecycle::new(Arc::new(CountingStore::new(Arc::clone(&store))), None);
    let mut t = lc.create(id).await.unwrap();
    for i in 0..60 {
        put(&mut t, i, b"one").await;
    }
    while t.fx_publish(&opts(Reseal::All), None).await.unwrap().is_none() {}
    for i in 0..60 {
        put(&mut t, i, b"two").await;
    }
    // the publish's PUT lands and times out; every fence after it times out too
    flaky.acts.lock().unwrap().extend([Act::LandThenErr, Act::Err, Act::Err, Act::Err, Act::Err, Act::Err, Act::Err, Act::Err, Act::Err, Act::Err]);
    let r = t.fx_publish(&opts(Reseal::None), None).await;
    assert!(r.is_err(), "the publish's PUT timed out");
    let newest = manifest::load_newest(&store, id).await.unwrap().unwrap().version;
    let known = t.fx_view().unwrap().version;
    // the next read: it must refuse (retryable) until a fence succeeds, or read with the newest version
    let read = t.get(key(1)).await;
    let known_after = t.fx_view().unwrap().version;
    E1_WAIT_MS.store(u64::MAX, Relaxed);
    flaky.acts.lock().unwrap().clear();
    t.abandon().await;
    assert!(
        read.is_err() || known_after == newest,
        "rule 25: a hold was pinned on version {known_after} (known before the publish: {known}) while version {newest} is in the store and no fence was published"
    );
}
