//! Wake bundle (EXPLORATION PROTOTYPE, branch `wake-bundle-explore`; not for production, off by default).
//!
//! A clean close already writes one object, the clean marker `tenants/{id}/_clean`. The prototype puts in that same
//! object (no extra PUT) what a reader needs to open: the bytes of the latest manifest and the small SST ranges read or
//! written during the session (SST index blocks, and whole SSTs up to [`SST_CAP`]). See docs/wake-bundle-exploration.md.
//!
//! At the next wake the marker is read with a GET instead of a HEAD, in parallel with the lease. After the lease is
//! taken, ONE request checks the bundle is still current: a HEAD of manifest `N+1` (SlateDB manifest ids are
//! consecutive, `slatedb-txn-obj` `object_store.rs` "a missing successor means the cached object is the latest
//! version"; every writer open and every compaction writes a new manifest). If it is absent, the reader is opened through
//! [`BundleStore`], which answers SlateDB's open-time reads (LIST of `manifest/`, GET of manifest N, GET of N+1, GET of
//! `gc/manifest.boundary`) and the SST ranges it has, from the bundle. Anything else goes to the real store.
//!
//! Safety rules (each one has a test below or in `lifecycle`):
//! * the bundle is only trusted after the lease is held and the `N+1` probe found nothing; otherwise the normal path runs;
//! * only immutable things are served without a request: a manifest version (written once, `PutMode::Create`) and
//!   ranges of compacted SSTs (ULID names, written once). The mutable answers ("N is the latest", "no boundary file")
//!   are served only for [`SERVE_WINDOW`] after the probe, and never after [`BundleStore::stop_serving`] (called before
//!   a writer opens);
//! * a marker that was read before the lease was taken is only used together with the probe (a writer that opened in
//!   between wrote manifest `N+1`). A marker without a bundle (the old `clean` content) is read again after the lease, as
//!   before.

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// First bytes of a marker that carries a bundle (the old marker content is `clean`).
pub const MAGIC: &[u8] = b"FXWB1\n";
/// Whole SSTs up to this size are kept in the bundle (a small tenant then needs no GET at all for its first read).
pub const SST_CAP: usize = 64 << 10;
/// Upper bound of the bytes of SST ranges in one bundle.
pub const RANGES_CAP: usize = 256 << 10;
/// How long after the probe the mutable answers ("N is the latest manifest") are served from the bundle. Covers the
/// open and the reader's first manifest poll (SlateDB's poller ticks once immediately, `dispatcher.rs`).
pub const SERVE_WINDOW: Duration = Duration::from_secs(2);

/// What the marker of a clean close carries.
#[derive(Clone, Debug, PartialEq)]
pub struct Bundle {
    pub manifest_id: u64,
    pub manifest: Bytes,
    /// (path relative to the tenant root, offset, bytes) of compacted SST ranges.
    pub ranges: Vec<(String, u64, Bytes)>,
}

impl Bundle {
    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_slice(MAGIC);
        b.put_u64(self.manifest_id);
        b.put_u32(self.manifest.len() as u32);
        b.put_slice(&self.manifest);
        b.put_u32(self.ranges.len() as u32);
        for (p, off, data) in &self.ranges {
            b.put_u16(p.len() as u16);
            b.put_slice(p.as_bytes());
            b.put_u64(*off);
            b.put_u32(data.len() as u32);
            b.put_slice(data);
        }
        b.freeze()
    }

    /// None for a marker without a bundle (the old `clean` content) or a damaged one: the caller takes the normal path.
    pub fn decode(raw: &Bytes) -> Option<Bundle> {
        if !raw.starts_with(MAGIC) {
            return None;
        }
        let mut b = raw.slice(MAGIC.len()..);
        let take = |b: &mut Bytes, n: usize| -> Option<Bytes> { (b.remaining() >= n).then(|| b.split_to(n)) };
        if b.remaining() < 12 {
            return None;
        }
        let manifest_id = b.get_u64();
        let n = b.get_u32() as usize;
        let manifest = take(&mut b, n)?;
        if b.remaining() < 4 {
            return None;
        }
        let count = b.get_u32() as usize;
        let mut ranges = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            if b.remaining() < 2 {
                return None;
            }
            let pl = b.get_u16() as usize;
            let p = String::from_utf8(take(&mut b, pl)?.to_vec()).ok()?;
            if b.remaining() < 12 {
                return None;
            }
            let off = b.get_u64();
            let dl = b.get_u32() as usize;
            ranges.push((p, off, take(&mut b, dl)?));
        }
        Some(Bundle { manifest_id, manifest, ranges })
    }
}

/// Manifest object name of version `id` (`slatedb-txn-obj` `object_store.rs` `path_for`: `{:020}.manifest`).
pub fn manifest_name(root: &str, id: u64) -> Path {
    Path::from(format!("{root}/manifest/{id:020}.manifest"))
}

struct Serving {
    bundle: Bundle,
    until: Instant,
}

/// Object-store wrapper of one tenant: records what a bundle needs while the tenant is open, and answers the open-time
/// reads of a validated bundle.
pub struct BundleStore {
    inner: Arc<dyn ObjectStore>,
    root: String,
    manifests: Mutex<BTreeMap<u64, Bytes>>,
    ranges: Mutex<(Vec<(String, u64, Bytes)>, usize)>,
    serving: Mutex<Option<Serving>>,
    /// Requests answered from the bundle (for tests and the trace).
    pub served: std::sync::atomic::AtomicU64,
}

impl BundleStore {
    pub fn new(inner: Arc<dyn ObjectStore>, root: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            root: root.into(),
            manifests: Mutex::new(BTreeMap::new()),
            ranges: Mutex::new((Vec::new(), 0)),
            serving: Mutex::new(None),
            served: Default::default(),
        })
    }

    /// Serve `bundle` (already validated: the lease is held and manifest N+1 was absent at the probe).
    pub fn serve(&self, bundle: Bundle) {
        self.manifests.lock().unwrap().insert(bundle.manifest_id, bundle.manifest.clone());
        {
            let mut r = self.ranges.lock().unwrap();
            for x in &bundle.ranges {
                r.1 += x.2.len();
                r.0.push(x.clone());
            }
        }
        *self.serving.lock().unwrap() = Some(Serving { bundle, until: Instant::now() + SERVE_WINDOW });
    }

    /// Stop answering anything mutable from the bundle (before a writer opens: it must see the real manifests).
    pub fn stop_serving(&self) {
        *self.serving.lock().unwrap() = None;
    }

    fn rel(&self, location: &Path) -> Option<String> {
        location.as_ref().strip_prefix(self.root.as_str()).and_then(|s| s.strip_prefix('/')).map(|s| s.to_string())
    }

    fn manifest_id_of(rel: &str) -> Option<u64> {
        rel.strip_prefix("manifest/")?.strip_suffix(".manifest")?.parse().ok()
    }

    /// The bundle for the newest manifest this session wrote or read, if it is at least `min_id` (the writer's own view
    /// after `close`; the close may write one more version than that view shows). None: write the old marker. A newer
    /// manifest written by someone else is caught by the N+1 probe at the next wake.
    pub fn bundle_for(&self, min_id: u64) -> Option<Bundle> {
        let (id, manifest) = self.manifests.lock().unwrap().iter().next_back().map(|(k, v)| (*k, v.clone()))?;
        if id < min_id {
            return None;
        }
        let ranges = self.ranges.lock().unwrap().0.clone();
        Some(Bundle { manifest_id: id, manifest, ranges })
    }

    fn note_range(&self, rel: &str, off: u64, data: &Bytes) {
        if !rel.starts_with("compacted/") || data.len() > SST_CAP {
            return;
        }
        let mut r = self.ranges.lock().unwrap();
        if r.0.iter().any(|(p, o, d)| p == rel && *o <= off && off + data.len() as u64 <= o + d.len() as u64) {
            return;
        }
        if r.1 + data.len() > RANGES_CAP {
            return;
        }
        r.1 += data.len();
        r.0.push((rel.to_string(), off, data.clone()));
    }

    fn find_range(&self, rel: &str, want: &std::ops::Range<u64>) -> Option<Bytes> {
        let r = self.ranges.lock().unwrap();
        r.0.iter().find_map(|(p, o, d)| {
            (p == rel && *o <= want.start && want.end <= o + d.len() as u64).then(|| d.slice((want.start - o) as usize..(want.end - o) as usize))
        })
    }

    fn serving_manifest(&self) -> Option<(u64, Bytes)> {
        let s = self.serving.lock().unwrap();
        s.as_ref().filter(|s| Instant::now() < s.until).map(|s| (s.bundle.manifest_id, s.bundle.manifest.clone()))
    }

    fn served(&self) {
        self.served.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn meta(location: &Path, size: u64) -> ObjectMeta {
        ObjectMeta { location: location.clone(), last_modified: Default::default(), size, e_tag: None, version: None }
    }

    fn result(location: &Path, data: Bytes, range: std::ops::Range<u64>, size: u64, head: bool) -> GetResult {
        let body = if head { Bytes::new() } else { data };
        GetResult {
            payload: GetResultPayload::Stream(futures::stream::once(async move { Ok(body) }).boxed()),
            meta: Self::meta(location, size),
            range,
            attributes: Attributes::default(),
            extensions: Default::default(),
        }
    }

    fn not_found(location: &Path) -> object_store::Error {
        object_store::Error::NotFound { path: location.to_string(), source: "wake bundle: absent at the probe".into() }
    }
}

impl fmt::Debug for BundleStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BundleStore({})", self.root)
    }
}

impl fmt::Display for BundleStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BundleStore({}, {})", self.root, self.inner)
    }
}

#[async_trait]
impl ObjectStore for BundleStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        let data: Bytes = payload.clone().into();
        let r = self.inner.put_opts(location, payload, opts).await?;
        if let Some(rel) = self.rel(location) {
            if let Some(id) = Self::manifest_id_of(&rel) {
                let mut m = self.manifests.lock().unwrap();
                m.insert(id, data);
                // Only the newest few versions can be the final one.
                while m.len() > 4 {
                    let first = *m.keys().next().unwrap();
                    m.remove(&first);
                }
            } else {
                self.note_range(&rel, 0, &data);
            }
        }
        Ok(r)
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let rel = self.rel(location);
        if let (Some(rel), Some((n, bytes))) = (rel.as_deref(), self.serving_manifest()) {
            if let Some(id) = Self::manifest_id_of(rel) {
                if id == n && options.range.is_none() {
                    self.served();
                    let len = bytes.len() as u64;
                    return Ok(Self::result(location, bytes, 0..len, len, options.head));
                }
                if id > n {
                    // Probed absent after the lease was taken (N+1); later ids cannot exist without N+1.
                    self.served();
                    return Err(Self::not_found(location));
                }
            }
            if rel == "gc/manifest.boundary" {
                // The boundary only ever moves to manifests GC deletes, never to the latest one (manifest_gc.rs), so the
                // check `N > boundary` holds whatever the file holds.
                self.served();
                return Err(Self::not_found(location));
            }
        }
        // Immutable SST ranges: served whenever present (not only in the window).
        if let (Some(rel), Some(GetRange::Bounded(want))) = (rel.as_deref(), options.range.as_ref()) {
            if !options.head {
                if let Some(d) = self.find_range(rel, want) {
                    self.served();
                    return Ok(Self::result(location, d, want.clone(), want.end, false));
                }
            }
        }
        let head = options.head;
        let mut r = self.inner.get_opts(location, options).await?;
        if head {
            return Ok(r);
        }
        if let Some(rel) = rel {
            let is_manifest = Self::manifest_id_of(&rel);
            if is_manifest.is_some() || rel.starts_with("compacted/") {
                let payload = std::mem::replace(&mut r.payload, GetResultPayload::Stream(futures::stream::empty().boxed()));
                let data = match payload {
                    GetResultPayload::Stream(s) => {
                        let chunks: Vec<Bytes> = s.try_collect().await?;
                        let mut b = BytesMut::new();
                        for c in chunks {
                            b.put_slice(&c);
                        }
                        b.freeze()
                    }
                    #[allow(unreachable_patterns)]
                    other => {
                        r.payload = other;
                        return Ok(r);
                    }
                };
                match is_manifest {
                    Some(id) => {
                        self.manifests.lock().unwrap().insert(id, data.clone());
                    }
                    None => self.note_range(&rel, r.range.start, &data),
                }
                r.payload = GetResultPayload::Stream(futures::stream::once(async move { Ok(data) }).boxed());
            }
        }
        Ok(r)
    }

    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        if let (Some(p), Some((n, bytes))) = (prefix, self.serving_manifest()) {
            if self.rel(p).as_deref() == Some("manifest") {
                self.served();
                let m = Self::meta(&manifest_name(&self.root, n), bytes.len() as u64);
                return futures::stream::once(async move { Ok(m) }).boxed();
            }
        }
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

    #[test]
    fn encode_decode_roundtrip() {
        let b = Bundle { manifest_id: 7, manifest: Bytes::from_static(b"m"), ranges: vec![("compacted/x.sst".into(), 10, Bytes::from_static(b"abc"))] };
        assert_eq!(Bundle::decode(&b.encode()), Some(b));
        assert_eq!(Bundle::decode(&Bytes::from_static(b"clean")), None);
        // A truncated bundle is not trusted.
        let e = Bundle { manifest_id: 7, manifest: Bytes::from_static(b"mm"), ranges: vec![] }.encode();
        assert_eq!(Bundle::decode(&e.slice(..e.len() - 3)), None);
    }

    use crate::counting::CountingStore;
    use crate::lifecycle::{Lifecycle, Op, Prefetch, WAKE_BUNDLE};

    fn setup() -> (Arc<CountingStore>, Arc<Lifecycle>) {
        WAKE_BUNDLE.store(true, std::sync::atomic::Ordering::Relaxed);
        let counting = Arc::new(CountingStore::new(Arc::new(object_store::memory::InMemory::new())));
        let lc = Lifecycle::with_guard(Arc::clone(&counting), None, false);
        (counting, lc)
    }

    fn put(k: &str, v: &str) -> Vec<Op> {
        vec![Op::Put(k.as_bytes().to_vec(), v.as_bytes().to_vec())]
    }

    async fn get(t: &crate::lifecycle::Tenant, k: &str) -> Option<String> {
        t.get(k.as_bytes()).await.unwrap().map(|b| String::from_utf8(b.to_vec()).unwrap())
    }

    /// A clean close writes a bundle; the next wake makes one request (the N+1 probe) for the open and the first read.
    #[tokio::test]
    async fn bundle_wake_needs_only_the_probe() {
        let (c, lc) = setup();
        let mut t = lc.create("a").await.unwrap();
        t.apply(put("k", "v1")).await.unwrap();
        t.close().await.unwrap();
        let pre = lc.prefetch_marker("a").await;
        assert!(matches!(pre, Prefetch::Bundle(_)), "the clean close must leave a bundle in the marker");
        let before = c.counters.snapshot();
        c.trace.start();
        let t = lc.wake_with("a", pre).await.unwrap();
        assert_eq!(get(&t, "k").await.as_deref(), Some("v1"));
        let after = c.counters.snapshot();
        for e in c.trace.take() {
            eprintln!("TRACE {} {}", e.op, e.path);
        }
        assert_eq!(after.head - before.head, 1, "only the probe");
        assert_eq!((after.get - before.get, after.get_range - before.get_range, after.list - before.list), (0, 0, 0));
        assert!(!t.unclean);
        t.close().await.unwrap();
    }

    /// A bundle read before a writer changed the database is not used: the wake sees the newer data.
    #[tokio::test]
    async fn stale_bundle_after_a_writer_is_not_used() {
        let (_c, lc) = setup();
        let mut t = lc.create("b").await.unwrap();
        t.apply(put("k", "v1")).await.unwrap();
        t.close().await.unwrap();
        let old = lc.prefetch_marker("b").await;
        let mut t = lc.wake("b").await.unwrap();
        t.apply(put("k", "v2")).await.unwrap();
        t.close().await.unwrap();
        let t = lc.wake_with("b", old).await.unwrap();
        assert_eq!(get(&t, "k").await.as_deref(), Some("v2"));
        t.close().await.unwrap();
    }

    /// The marker (with its bundle) was read, then a writer acknowledged a write and crashed before closing (no marker,
    /// the write only in the WAL). The old bundle must not hide the write: the probe sees the writer's manifest and the
    /// wake replays the WAL.
    #[tokio::test]
    async fn crash_after_the_bundle_replays_the_wal() {
        let (_c, lc) = setup();
        let mut t = lc.create("c").await.unwrap();
        t.apply(put("k", "v1")).await.unwrap();
        t.close().await.unwrap();
        let old = lc.prefetch_marker("c").await;
        let mut t = lc.wake("c").await.unwrap();
        t.apply(put("k", "v2")).await.unwrap();
        t.abandon().await; // crash: no final flush, no marker
        let t = lc.wake_with("c", old).await.unwrap();
        assert_eq!(get(&t, "k").await.as_deref(), Some("v2"));
        assert!(t.unclean);
        t.close().await.unwrap();
        // And with a fresh read of the marker (absent): replay at once.
        let pre = lc.prefetch_marker("c").await;
        assert!(matches!(pre, Prefetch::Absent));
        let t = lc.wake_with("c", pre).await.unwrap();
        assert_eq!(get(&t, "k").await.as_deref(), Some("v2"));
        t.close().await.unwrap();
    }

    /// A write right after a bundle wake (inside the serving window) opens the writer on the real manifests.
    #[tokio::test]
    async fn writer_after_a_bundle_wake_sees_the_real_manifests() {
        let (_c, lc) = setup();
        let mut t = lc.create("d").await.unwrap();
        t.apply(put("k", "v1")).await.unwrap();
        t.close().await.unwrap();
        let pre = lc.prefetch_marker("d").await;
        let mut t = lc.wake_with("d", pre).await.unwrap();
        // Inside the serving window, someone else (a checkpoint, like a compactor would) writes manifest N+1.
        let admin = slatedb::admin::Admin::builder(crate::lifecycle::tenant_path("d"), Arc::clone(&lc.store)).build();
        admin.create_detached_checkpoint(&slatedb::config::CheckpointOptions::default()).await.unwrap();
        // Without `stop_serving` the writer would still be correct (its manifest PUT is a conditional create) but would
        // retry against the bundle's answers until the serving window ends.
        let t0 = Instant::now();
        t.apply(put("k", "v2")).await.unwrap();
        assert!(t0.elapsed() < Duration::from_millis(1000), "the writer waited for the serving window");
        t.close().await.unwrap();
        for _ in 0..2 {
            let pre = lc.prefetch_marker("d").await;
            assert!(matches!(pre, Prefetch::Bundle(_)));
            let mut t = lc.wake_with("d", pre).await.unwrap();
            assert_eq!(get(&t, "k").await.as_deref(), Some("v2"));
            t.apply(put("k2", "x")).await.unwrap();
            t.close().await.unwrap();
        }
        let t = lc.wake("d").await.unwrap();
        assert_eq!(get(&t, "k2").await.as_deref(), Some("x"));
        t.close().await.unwrap();
    }

    /// A manifest written without the lease (compaction, checkpoint) after the close: the bundle is stale, the data is
    /// still read correctly through the normal path.
    #[tokio::test]
    async fn manifest_change_without_a_writer_falls_back() {
        let (_c, lc) = setup();
        let mut t = lc.create("e").await.unwrap();
        t.apply(put("k", "v1")).await.unwrap();
        t.close().await.unwrap();
        let pre = lc.prefetch_marker("e").await;
        let admin = slatedb::admin::Admin::builder(crate::lifecycle::tenant_path("e"), Arc::clone(&lc.store)).build();
        admin.create_detached_checkpoint(&slatedb::config::CheckpointOptions::default()).await.unwrap();
        let stale_before = crate::lifecycle::BUNDLE_STALE.load(std::sync::atomic::Ordering::Relaxed);
        let t = lc.wake_with("e", pre).await.unwrap();
        assert!(crate::lifecycle::BUNDLE_STALE.load(std::sync::atomic::Ordering::Relaxed) > stale_before);
        assert_eq!(get(&t, "k").await.as_deref(), Some("v1"));
        t.close().await.unwrap();
    }
}
