//! integration-4: joint B between c2-fix-hangs and c2-fix-clone-uploads, at the level where it lives.
//!
//! c2-fix-hangs makes a database's compactor work maintenance (counted in its points, never drawn from its quota) by
//! starting the compactor on `counting::maintenance(store)`: every request of that store runs under the MAINTENANCE
//! mark. The clone-uploads fix moved the compactor's start into `Entry::maybe_start_fxc()` and made the absorption read
//! each L0 SST whole (`read_l0_whole`, also from a close: `flush_then_stop_fxc` calls `absorb_once`) and cancel jobs at
//! stop. The merge passes the maintenance store in `maybe_start_fxc()`. Here: a compactor started on the maintenance
//! store absorbs flushed L0 SSTs, merges, reseals and stops; every request it makes (GET of the L0 SSTs, PUTs of runs and
//! manifests, LISTs) must carry the mark. Control: the same compactor on the plain store makes unmarked requests, so the
//! test sees the difference.

use super::compactor::{self, Compactor, Knobs};
use super::tests::*;
use super::*;
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};

/// Records every request: (kind, path, made under the maintenance mark).
#[derive(Debug)]
struct Recorder {
    inner: Arc<dyn ObjectStore>,
    log: Arc<Mutex<Vec<(&'static str, String, bool)>>>,
    on: Arc<std::sync::atomic::AtomicBool>,
}

impl Recorder {
    fn note(&self, kind: &'static str, p: &str) {
        if self.on.load(Relaxed) {
            self.log.lock().unwrap().push((kind, p.to_string(), crate::counting::is_maintenance_request()));
        }
    }
}

impl std::fmt::Display for Recorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Recorder({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for Recorder {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        self.note("put", location.as_ref());
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.note("multipart", location.as_ref());
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.note("get", location.as_ref());
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(&self, locations: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.note("list", prefix.map(|p| p.as_ref()).unwrap_or(""));
        self.inner.list(prefix)
    }
    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.note("list", prefix.map(|p| p.as_ref()).unwrap_or(""));
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.note("list", prefix.map(|p| p.as_ref()).unwrap_or(""));
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.note("copy", to.as_ref());
        self.inner.copy_opts(from, to, options).await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> object_store::Result<()> {
        self.note("rename", to.as_ref());
        self.inner.rename_opts(from, to, options).await
    }
}

fn knobs() -> Knobs {
    let mut k = compactor::server_knobs();
    k.lease = std::time::Duration::from_secs(60);
    k
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn i4_compactor_requests_are_maintenance() {
    let _s = SERIAL.lock().await;
    enable();
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(3 << 10, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(32, Relaxed);
    let mut results = Vec::new();
    for maintenance in [true, false] {
        let log = Arc::new(Mutex::new(Vec::new()));
        let on = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let rec: Arc<dyn ObjectStore> = Arc::new(Recorder { inner: Arc::new(InMemory::new()), log: Arc::clone(&log), on: Arc::clone(&on) });
        let id = if maintenance { "fx-i4-maint" } else { "fx-i4-maint-ctl" };
        let mut t = lc(&rec).create(id).await.unwrap();
        let mut m = BTreeMap::new();
        for i in 0..300 {
            apply(&mut t, &mut m, vec![(ckey(i % 7, i), Some(vec![b'a'; 60]))]).await;
        }
        publish(&mut t, &small_opts(build::Reseal::All)).await;
        // as Entry::maybe_start_fxc() starts it on integration-4 (the control: the plain store, as before c2-fix-hangs)
        let store = if maintenance { crate::counting::maintenance(&rec) } else { Arc::clone(&rec) };
        let c = Compactor::start(&mut t, store, knobs(), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
        for i in 0..300 {
            apply(&mut t, &mut m, vec![(ckey(i % 7, i), Some(vec![b'b'; 60]))]).await;
        }
        t.fx_writer_db().unwrap().flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
        // the compactor's own work only, as a close runs it: absorb every flushed L0 SST, merge, reseal, stop
        on.store(true, Relaxed);
        let mut absorbed = 0;
        loop {
            let n = c.absorb_once().await.unwrap();
            if n == 0 {
                break;
            }
            absorbed += n;
        }
        while c.merge_once().await.unwrap() {}
        let _ = c.reseal_once(Some(0)).await;
        c.stop().await;
        on.store(false, Relaxed);
        let rows = log.lock().unwrap().clone();
        let unmarked: Vec<_> = rows.iter().filter(|(_, _, mk)| !mk).collect();
        let l0_gets = rows.iter().filter(|(k, p, _)| *k == "get" && p.contains("/compacted/")).count();
        eprintln!("compactor on the {} store: {absorbed} L0 SSTs absorbed, {} requests, {l0_gets} GETs of L0 SSTs, {} without the maintenance mark (first: {:?})",
                  if maintenance { "maintenance" } else { "plain" }, rows.len(), unmarked.len(), unmarked.first());
        results.push((maintenance, absorbed, rows.len(), l0_gets, unmarked.len()));
        t.abandon().await;
    }
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(0, Relaxed);
    let (_, absorbed, n, l0_gets, unmarked) = results[0];
    assert!(absorbed > 0 && l0_gets > 0 && n > 0, "the absorption did not run: {results:?}");
    assert_eq!(unmarked, 0, "requests of the compactor on the maintenance store without the mark: {results:?}");
    let (_, _, n_ctl, _, unmarked_ctl) = results[1];
    assert!(n_ctl > 0 && unmarked_ctl == n_ctl, "control: on the plain store no request carries the mark: {results:?}");
}
