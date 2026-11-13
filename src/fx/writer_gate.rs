//! fix-idle-poll: an open writer's manifest poll, held while its database is idle.
//!
//! With our compactor on, the server sets SlateDB's writer `manifest_poll_interval` to 2 s (`WRITER_POLL_SECS`, step 4
//! report section 4: a memtable flush that stalled on a full L0 retries only at the writer's manifest poll, and the
//! folds free L0 within a second or two). Each poll reads the store: the next manifest (mostly NotFound) and its GC
//! boundary. MEASURED on an idle open writer, the server's defaults: 1 GET per second per database, every second, with
//! no request at all (research/fix-idle-poll). ESTIMATED: about 2.6 million GETs a month per open database, about $1 a
//! month at S3's DECLARED $0.0004 per 1,000 GETs; with thousands of open agent databases it is the largest idle cost.
//!
//! What the writer's poll is for, and why holding it while idle loses none of it:
//! - it learns of the compactor's work (L0 SSTs folded, a stalled flush can go on): every such change is made by our
//!   compactor in this process, which wakes the gate ([`wake`]: `DropCtl::wake_slate`, and before every explicit
//!   `refresh_manifest`); a wake opens a window of `ACTIVE_WINDOW` in which the writer polls at its 2 s pace again;
//! - it drops its references to SSTs the compactor replaced before the SST cleanup may delete them: the cleanup waits
//!   `--sst-gc-min-unref-secs` (120 s), and a held poll waits at most `--writer-poll-secs` (60 s, checked below 120 s
//!   at start), the pace of a writer with our compactor off;
//! - it learns that another process fenced it (a newer writer epoch in the manifest): an idle writer acknowledges
//!   nothing, and a write is refused by its own conditional writes and the process lease, never by this poll (the poll
//!   was 2 s, so a write inside those 2 s always depended on them already). `ctl-tests/test_92_idle_poll.py` checks it.
//!
//! A held read reaches the store unchanged, later: a slower poll, never a changed answer. Any other request through
//! the writer's store (a WAL or SST write, a read of data) is activity: it releases the held poll and keeps the 2 s pace
//! for `ACTIVE_WINDOW`. Before a writer closes (`lifecycle::writer_closed`) its gate stops holding for good, so a close
//! never waits for a held poll (SlateDB's close waits for the poll in progress and reads the manifest itself).
//! `FENVIX_SLATE_IDLE_POLL_MS=0` turns this off with the compactor's gate (the negative control of the tests).

use async_trait::async_trait;
use object_store::path::Path;
use object_store::{GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

/// The slowest writer poll while idle, in seconds: `--writer-poll-secs` (the server sets it; it must stay below
/// `--sst-gc-min-unref-secs`), and never more than the compactor's `slate_idle_poll_ms`.
pub static WRITER_IDLE_POLL_SECS: AtomicU64 = AtomicU64::new(60);
/// After a wake or any other request through the writer's store, the writer polls at its own pace for this long.
pub const ACTIVE_WINDOW: Duration = Duration::from_secs(10);
/// Writer poll reads held while idle, and those of them a wake released early (serverStatus).
pub static WRITER_POLLS_HELD: AtomicU64 = AtomicU64::new(0);
pub static WRITER_POLLS_WOKEN: AtomicU64 = AtomicU64::new(0);

static T0: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);
static GATES: std::sync::LazyLock<Mutex<HashMap<String, Weak<State>>>> = std::sync::LazyLock::new(Default::default);

fn now_ms() -> u64 {
    T0.elapsed().as_millis() as u64
}

/// How long a writer's poll may be held: `None` (no gate) when the compactor's gate is off or the writer's own pace is
/// already the idle one.
pub fn idle_hold() -> Option<Duration> {
    let slate = super::drop::slate_idle_poll_ms();
    let idle_ms = WRITER_IDLE_POLL_SECS.load(Relaxed).saturating_mul(1000).min(slate);
    let pace_ms = crate::lifecycle::WRITER_POLL_SECS.load(Relaxed).saturating_mul(1000);
    (slate > 0 && idle_ms > pace_ms).then(|| Duration::from_millis(idle_ms))
}

#[derive(Debug)]
pub(crate) struct State {
    last_ms: AtomicU64,
    inflight: AtomicU64,
    stopping: AtomicBool,
    wake: tokio::sync::Notify,
}

impl State {
    fn touch(&self) {
        self.last_ms.store(now_ms(), Relaxed);
        self.wake.notify_waiters();
    }

    fn busy(&self) -> bool {
        self.stopping.load(Relaxed) || self.inflight.load(Relaxed) > 0 || now_ms().saturating_sub(self.last_ms.load(Relaxed)) < ACTIVE_WINDOW.as_millis() as u64
    }
}

/// Something may have changed the writer's manifest, or will (a compaction job proposed, ended, a refresh asked): the
/// writer of `id` polls at its own pace again, now and for `ACTIVE_WINDOW`.
pub fn wake(id: &str) {
    let s = GATES.lock().unwrap().get(id).and_then(Weak::upgrade);
    if let Some(s) = s {
        s.touch();
    }
}

/// The writer of `id` is closing: its gate never holds a poll again (`lifecycle::writer_closed`).
pub fn stopping(id: &str) {
    let s = GATES.lock().unwrap().remove(id).and_then(|w| w.upgrade());
    if let Some(s) = s {
        s.stopping.store(true, Relaxed);
        s.wake.notify_waiters();
    }
}

/// The writer's store of database `id`, gated when [`idle_hold`] says so.
pub(crate) fn wrap(id: &str, inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    if idle_hold().is_none() {
        return inner;
    }
    let st = Arc::new(State { last_ms: AtomicU64::new(now_ms()), inflight: AtomicU64::new(0), stopping: AtomicBool::new(false), wake: tokio::sync::Notify::new() });
    GATES.lock().unwrap().insert(id.to_string(), Arc::downgrade(&st));
    Arc::new(WriterGate { inner, st })
}

/// The reads of the writer's manifest poll: the manifest files and the manifest's GC boundary (not ours under `fx/`).
fn polled(p: &Path) -> bool {
    let s = p.as_ref();
    !s.contains("/fx/") && ((s.contains("/manifest/") && s.ends_with(".manifest")) || s.ends_with("/gc/manifest.boundary"))
}

#[derive(Debug)]
struct WriterGate {
    inner: Arc<dyn ObjectStore>,
    st: Arc<State>,
}

impl std::fmt::Display for WriterGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WriterGate({})", self.inner)
    }
}

/// Marks a request through the writer's store as activity for as long as it runs.
struct Active<'a>(&'a State);

impl<'a> Active<'a> {
    fn new(s: &'a State) -> Self {
        s.inflight.fetch_add(1, Relaxed);
        s.touch();
        Active(s)
    }
}

impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.last_ms.store(now_ms(), Relaxed);
        self.0.inflight.fetch_sub(1, Relaxed);
    }
}

impl WriterGate {
    async fn hold(&self) {
        let Some(max) = idle_hold() else { return };
        let woken = self.st.wake.notified();
        tokio::pin!(woken);
        woken.as_mut().enable();
        if self.st.busy() {
            return;
        }
        WRITER_POLLS_HELD.fetch_add(1, Relaxed);
        tokio::select! {
            _ = woken => { WRITER_POLLS_WOKEN.fetch_add(1, Relaxed); }
            _ = tokio::time::sleep(max) => {}
        }
    }
}

#[async_trait]
impl ObjectStore for WriterGate {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        let _a = Active::new(&self.st);
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        let _a = Active::new(&self.st);
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if polled(location) {
            self.hold().await;
            return self.inner.get_opts(location, options).await;
        }
        let _a = Active::new(&self.st);
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(&self, locations: futures::stream::BoxStream<'static, Result<Path>>) -> futures::stream::BoxStream<'static, Result<Path>> {
        self.st.touch();
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, Result<ObjectMeta>> {
        // a LIST of the manifest folder is the poll's fallback (more than a few versions behind), not activity
        if !prefix.is_some_and(|p| p.as_ref().ends_with("/manifest") || p.as_ref().ends_with("/manifest/")) {
            self.st.touch();
        }
        self.inner.list(prefix)
    }
    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> futures::stream::BoxStream<'static, Result<ObjectMeta>> {
        if !prefix.is_some_and(|p| p.as_ref().ends_with("/manifest") || p.as_ref().ends_with("/manifest/")) {
            self.st.touch();
        }
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let _a = Active::new(&self.st);
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: object_store::CopyOptions) -> Result<()> {
        let _a = Active::new(&self.st);
        self.inner.copy_opts(from, to, options).await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, options: object_store::RenameOptions) -> Result<()> {
        let _a = Active::new(&self.st);
        self.inner.rename_opts(from, to, options).await
    }
}
