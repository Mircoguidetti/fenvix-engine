//! fix-retire-read-error (RULE 8; gen-2 seeds 104673, 200400, 202173 of the night-head gate): the erasure retire took a
//! version it could not read (a store timeout, a 503) as listing no runs, and deleted a run that a restore point's
//! version still lists. Here the retire reads through a store that fails the first two reads of the pinned version: it
//! must delete nothing (and fail), and the run must still be there.

use super::erase;
use super::manifest;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult};
use futures::stream::BoxStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

#[derive(Debug)]
struct FailVersion {
    inner: Arc<dyn ObjectStore>,
    path: String,
    on: AtomicBool,
    /// how many reads of `path` fail before they succeed again (the seed: the retire's first two reads were dropped)
    fails_left: AtomicU64,
}

impl std::fmt::Display for FailVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FailVersion({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for FailVersion {
    async fn put_opts(&self, l: &Path, p: PutPayload, o: PutOptions) -> object_store::Result<PutResult> {
        self.inner.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(&self, l: &Path, o: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, l: &Path, o: GetOptions) -> object_store::Result<GetResult> {
        if self.on.load(Relaxed) && l.as_ref() == self.path && self.fails_left.fetch_update(Relaxed, Relaxed, |n| n.checked_sub(1)).is_ok() {
            return Err(object_store::Error::Generic { store: "test", source: "503 slow down (injected)".into() });
        }
        self.inner.get_opts(l, o).await
    }
    fn delete_stream(&self, s: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(s)
    }
    fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(p)
    }
    fn list_with_offset(&self, p: Option<&Path>, o: &Path) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(p, o)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, f: &Path, t: &Path, o: object_store::CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(f, t, o).await
    }
    async fn rename_opts(&self, f: &Path, t: &Path, o: object_store::RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(f, t, o).await
    }
}

fn run_line(path: &str) -> serde_json::Value {
    serde_json::json!({ "path": path, "size": 1, "id": "00", "min_seq": 1, "max_seq": 1, "first": "61", "last": "61", "entries": 1 })
}

fn version(n: u64, run: &str) -> serde_json::Value {
    serde_json::json!({
        "format": manifest::FORMAT, "version": n, "kind": "publish", "writer_epoch": 1, "x_d": n, "e_env": 0,
        "published_ms": 0, "groups": [{ "id": 0, "lo": "", "x_g": n, "sealed": run_line(run) }], "delta": [], "next_group_id": 1
    })
}

async fn case(control: bool) -> (bool, bool) {
    erase::CONTROL_RETIRE_READ_ERRORS_AS_GONE.store(control, Relaxed);
    let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "retire-read-error";
    let root = manifest::root(&id);
    let run_a = format!("{root}/run/g0/00000000000000000001-1-aaaaaaaa");
    let run_b = format!("{root}/run/g0/00000000000000000002-1-bbbbbbbb");
    for r in [&run_a, &run_b] {
        raw.put(&Path::from(r.as_str()), PutPayload::from_static(b"run")).await.unwrap();
    }
    // version 1 lists run A; version 2 (the newest, the erasure's scrub) replaced it with run B
    raw.put(&manifest::manifest_path(&id, 1), PutPayload::from(serde_json::to_vec(&version(1, &run_a)).unwrap())).await.unwrap();
    raw.put(&manifest::manifest_path(&id, 2), PutPayload::from(serde_json::to_vec(&version(2, &run_b)).unwrap())).await.unwrap();
    // a restore point pins version 1
    let cp = uuid::Uuid::new_v4();
    let pair = serde_json::json!({ "checkpoint": cp.to_string(), "version": 1, "x_d": 1, "e_env": 0, "last_l0_seq": 1, "exact": true, "created_ms": 0 });
    raw.put(&super::pair::pair_path(&id, &cp), PutPayload::from(serde_json::to_vec(&pair).unwrap())).await.unwrap();
    let mut e: erase::Erasure = serde_json::from_value(serde_json::json!({
        "id": "e1", "kind": "customer", "collection": "msgs", "customer_hash": "x", "s_e": 1, "requested_ms": 0,
        "deadline_ms": 0, "ranges": 1, "keys": 1, "state": "scrubbed", "scrubbed_version": 2, "scrubbed_ms": 0,
        "rows_erased": 1, "runs_rewritten": 1, "retired": [{ "path": run_a, "deleted_ms": null, "kept_by": [] }],
        "versions_deleted": 0, "versions_kept": [], "boundaries_on_targets": 0, "live_done_ms": null, "done_ms": null
    }))
    .unwrap();
    // the first two reads of the pinned version fail (dropped by a store under load), later ones succeed
    let store: Arc<dyn ObjectStore> = Arc::new(FailVersion { inner: Arc::clone(&raw), path: manifest::manifest_path(&id, 1).to_string(), on: AtomicBool::new(true), fails_left: AtomicU64::new(2) });
    let r = erase::retire_all(&store, &id, &mut [&mut e], 2, 2, 0).await;
    let still = raw.head(&Path::from(run_a.as_str())).await.is_ok();
    eprintln!("control {control}: retire {:?}; kept_by {:?}; deleted {:?}", r.as_ref().err().map(|e| format!("{e:#}")), e.retired[0].kept_by, e.retired[0].deleted_ms);
    erase::CONTROL_RETIRE_READ_ERRORS_AS_GONE.store(false, Relaxed);
    (r.is_err(), still)
}

/// The restore point's run survives a retire that cannot read the point's version; the negative control (errors read as
/// "gone", the code before) deletes it.
#[tokio::test]
async fn retire_keeps_a_pinned_run_when_its_version_cannot_be_read() {
    let _s = super::tests::SERIAL.lock().await;
    let (failed, still) = case(false).await;
    assert!(still, "the retire deleted a run the restore point's version lists, after a failed read of that version");
    assert!(failed, "the retire must fail (and be retried) when it cannot read a referenced version");
    let (_, still_control) = case(true).await;
    assert!(!still_control, "negative control: with read errors taken as 'gone' the run is deleted (the hole)");
}
