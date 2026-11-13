//! The simulated store (P-sim-test step 1): every request of every simulated process goes through one seeded scheduler.
//!
//! - Each simulated process ("life": a process start; a restart is a new life) sees the shared store through its own
//!   [`ProcStore`]. Requests are classified by what they touch (`Class`): our manifest versions, our runs, erasure
//!   records, our marks, SlateDB's WAL, SlateDB's manifest, SlateDB's SSTs, other.
//! - The fault of a request is a pure function of (seed, life, op, class, n), where n counts that life's requests of that
//!   op and class: delay (0 to `delay_max_ms`), 503 before the request reaches the store, a timeout before, or a timeout
//!   AFTER the write landed (PUT, multipart complete, DELETE). The same seed gives the same decision to the same request.
//! - A hold rule (`hold`) keeps the next matching request of a life on its way to the store until the driver releases
//!   the rule (`release`), then lands it or drops it (a request sent before a crash lands or not; S3 allows both).
//! - A killed life (`kill`): its new requests fail at once (a dead process sends nothing). Its held requests still land
//!   or not as their rule says: they were already on the wire.
//! - `calm`: no more faults or delays (the settle phase and the oracle's fresh opening).
//!
//! Reordering: S3 is strongly consistent per object and requests in flight at the same time may complete in any order.
//! Delays and holds give exactly that: two requests in flight complete in the order their delays end.

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result, UploadPart,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    Put,
    Get,
    List,
    Delete,
    Multipart,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Class {
    /// `fx/manifest/`: our manifest versions.
    FxVersion,
    /// `fx/run/`: our runs.
    FxRun,
    /// `fx/erasures`: erasure records.
    FxErasure,
    /// other objects under `fx/` (marks, announcements, pairs, references).
    FxOther,
    /// SlateDB's WAL objects.
    Wal,
    /// SlateDB's manifest.
    SlateManifest,
    /// SlateDB's SSTs (L0 and compacted) and anything else.
    Other,
}

pub fn classify(p: &str) -> Class {
    if p.contains("/fx/manifest/") {
        Class::FxVersion
    } else if p.contains("/fx/run/") {
        Class::FxRun
    } else if p.contains("/fx/erasures") {
        Class::FxErasure
    } else if p.contains("/fx/") {
        Class::FxOther
    } else if p.contains("/wal/") {
        Class::Wal
    } else if p.contains("/manifest/") {
        Class::SlateManifest
    } else {
        Class::Other
    }
}

/// The faults of a run, chosen from the seed.
#[derive(Clone, Copy, Debug)]
pub struct Profile {
    /// Probability (per million) that a request is delayed, and the longest delay.
    pub delay_ppm: u64,
    pub delay_max_ms: u64,
    /// Per million: a 503 before the store; a timeout before; a timeout after a write landed.
    pub fail_ppm: u64,
    pub timeout_before_ppm: u64,
    pub timeout_after_ppm: u64,
    /// How long a simulated timeout takes to answer.
    pub timeout_ms: u64,
}

impl Profile {
    pub fn none() -> Profile {
        Profile { delay_ppm: 0, delay_max_ms: 0, fail_ppm: 0, timeout_before_ppm: 0, timeout_after_ppm: 0, timeout_ms: 0 }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    Fail,
    TimeoutBefore,
    TimeoutAfter,
}

/// A hold rule: the `skip`+1-th next request of `life` with this op and class (any op when `op` is None) waits until the
/// rule is released, then lands (`land`) or is dropped with an error (a request lost with its process).
#[derive(Clone, Debug)]
pub struct Rule {
    pub id: u64,
    pub life: u64,
    pub op: Option<Op>,
    pub class: Class,
    pub skip: u64,
    pub land: bool,
    /// Holds every matching request (a slow process), not only one.
    pub all: bool,
}

struct RuleState {
    rule: Rule,
    seen: u64,
    released: bool,
    /// requests held by it now
    waiting: u64,
    /// requests it has held so far
    caught: u64,
}

pub struct Net {
    pub inner: Arc<dyn ObjectStore>,
    pub seed: u64,
    pub profile: Mutex<Profile>,
    calm: AtomicBool,
    dead: Mutex<std::collections::HashSet<u64>>,
    counters: Mutex<HashMap<(u64, Op, Class), u64>>,
    rules: Mutex<Vec<RuleState>>,
    wake: tokio::sync::Notify,
    /// The driver's step now (stamped on trace lines).
    pub step: AtomicU64,
    trace: Mutex<Vec<String>>,
    pub requests: AtomicU64,
    pub faults: AtomicU64,
    /// The longest a held request stays on its way (a request in flight lands within this, as a PUT within its timeout).
    pub max_hold: Duration,
}

fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

pub fn hash(parts: &[u64]) -> u64 {
    parts.iter().fold(0x1234_5678_9abc_def0u64, |h, p| mix(h ^ mix(*p)))
}

fn sim_err(what: &str) -> object_store::Error {
    object_store::Error::Generic { store: "sim", source: what.to_string().into() }
}

impl Net {
    pub fn new(inner: Arc<dyn ObjectStore>, seed: u64, profile: Profile) -> Arc<Net> {
        Arc::new(Net {
            inner,
            seed,
            profile: Mutex::new(profile),
            calm: AtomicBool::new(false),
            dead: Default::default(),
            counters: Default::default(),
            rules: Default::default(),
            wake: tokio::sync::Notify::new(),
            step: AtomicU64::new(0),
            trace: Default::default(),
            requests: AtomicU64::new(0),
            faults: AtomicU64::new(0),
            max_hold: Duration::from_millis(1000),
        })
    }

    pub fn proc_store(self: &Arc<Self>, life: u64) -> Arc<dyn ObjectStore> {
        Arc::new(ProcStore { net: Arc::clone(self), life })
    }

    pub fn note(&self, s: String) {
        let step = self.step.load(SeqCst);
        self.trace.lock().unwrap().push(format!("[{step}] {s}"));
    }

    pub fn trace(&self) -> Vec<String> {
        self.trace.lock().unwrap().clone()
    }

    pub fn set_calm(&self) {
        self.calm.store(true, SeqCst);
    }

    pub fn kill(&self, life: u64) {
        self.dead.lock().unwrap().insert(life);
    }

    pub fn is_dead(&self, life: u64) -> bool {
        self.dead.lock().unwrap().contains(&life)
    }

    pub fn hold(&self, r: Rule) {
        self.note(format!("hold rule {} armed: life {} {:?} {:?} skip {} land {} all {}", r.id, r.life, r.op, r.class, r.skip, r.land, r.all));
        self.rules.lock().unwrap().push(RuleState { rule: r, seen: 0, released: false, waiting: 0, caught: 0 });
    }

    /// Release a rule: its held requests go on (land or drop), and it holds nothing more.
    pub fn release(&self, id: u64) {
        let mut n = 0;
        for r in self.rules.lock().unwrap().iter_mut().filter(|r| r.rule.id == id) {
            r.released = true;
            n = r.waiting;
        }
        self.note(format!("hold rule {id} released ({n} waiting)"));
        self.wake.notify_waiters();
    }

    pub fn release_all(&self) {
        for r in self.rules.lock().unwrap().iter_mut() {
            r.released = true;
        }
        self.wake.notify_waiters();
    }

    /// Requests held by rule `id` now, and requests it caught so far.
    pub fn held(&self, id: u64) -> (u64, u64) {
        self.rules.lock().unwrap().iter().find(|r| r.rule.id == id).map(|r| (r.waiting, r.caught)).unwrap_or((0, 0))
    }

    /// Decide what happens to one request (before it reaches the store). Returns the fault and whether the request must
    /// be dropped (a held request of a rule with `land` false).
    async fn admit(&self, life: u64, op: Op, path: &str) -> Result<Fault> {
        self.requests.fetch_add(1, SeqCst);
        let class = classify(path);
        if self.dead.lock().unwrap().contains(&life) {
            return Err(sim_err("process killed"));
        }
        let n = {
            let mut c = self.counters.lock().unwrap();
            let e = c.entry((life, op, class)).or_insert(0);
            *e += 1;
            *e - 1
        };
        // a hold rule that matches
        let hold = {
            let mut rules = self.rules.lock().unwrap();
            let mut found = None;
            for r in rules.iter_mut() {
                if r.released || r.rule.life != life || r.rule.class != class || r.rule.op.is_some_and(|o| o != op) {
                    continue;
                }
                if !r.rule.all && r.caught > 0 {
                    continue;
                }
                r.seen += 1;
                if r.seen <= r.rule.skip {
                    continue;
                }
                r.waiting += 1;
                r.caught += 1;
                found = Some((r.rule.id, r.rule.land));
                break;
            }
            found
        };
        if let Some((id, land)) = hold {
            self.note(format!("held by rule {id}: life {life} {op:?} {path}"));
            let t0 = tokio::time::Instant::now();
            loop {
                let w = self.wake.notified();
                let released = self.rules.lock().unwrap().iter().any(|r| r.rule.id == id && r.released);
                if released {
                    break;
                }
                let left = self.max_hold.saturating_sub(t0.elapsed());
                if left.is_zero() || tokio::time::timeout(left, w).await.is_err() {
                    break;
                }
            }
            for r in self.rules.lock().unwrap().iter_mut().filter(|r| r.rule.id == id) {
                r.waiting -= 1;
            }
            self.note(format!("rule {id} lets go after {} ms: life {life} {op:?} {path} {}", t0.elapsed().as_millis(), if land { "lands" } else { "dropped" }));
            if !land {
                return Err(sim_err("held request dropped"));
            }
            return Ok(Fault::None);
        }
        if self.calm.load(SeqCst) {
            return Ok(Fault::None);
        }
        let p = *self.profile.lock().unwrap();
        let h = hash(&[self.seed, life, op as u64, class as u64, n]);
        let roll = |salt: u64| hash(&[h, salt]) % 1_000_000;
        if p.delay_ppm > 0 && roll(1) < p.delay_ppm && p.delay_max_ms > 0 {
            let ms = hash(&[h, 2]) % (p.delay_max_ms + 1);
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
        let writes = matches!(op, Op::Put | Op::Delete | Op::Multipart);
        let f = roll(3);
        let fault = if f < p.fail_ppm {
            Fault::Fail
        } else if f < p.fail_ppm + p.timeout_before_ppm {
            Fault::TimeoutBefore
        } else if writes && f < p.fail_ppm + p.timeout_before_ppm + p.timeout_after_ppm {
            Fault::TimeoutAfter
        } else {
            Fault::None
        };
        if fault != Fault::None {
            self.faults.fetch_add(1, SeqCst);
            self.note(format!("fault {fault:?}: life {life} {op:?} {path}"));
        }
        match fault {
            Fault::Fail => Err(sim_err("503 slow down (simulated)")),
            Fault::TimeoutBefore => {
                tokio::time::sleep(Duration::from_millis(p.timeout_ms)).await;
                Err(sim_err("timeout before the store (simulated)"))
            }
            f => Ok(f),
        }
    }

    async fn after(&self, f: Fault) -> Result<()> {
        if f == Fault::TimeoutAfter {
            let ms = self.profile.lock().unwrap().timeout_ms;
            tokio::time::sleep(Duration::from_millis(ms)).await;
            return Err(sim_err("timeout after the write landed (simulated)"));
        }
        Ok(())
    }
}

impl std::fmt::Debug for Net {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SimNet(seed {})", self.seed)
    }
}

#[derive(Clone)]
pub struct ProcStore {
    net: Arc<Net>,
    life: u64,
}

impl std::fmt::Debug for ProcStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SimProcStore(life {})", self.life)
    }
}

impl std::fmt::Display for ProcStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SimProcStore(life {})", self.life)
    }
}

#[derive(Debug)]
struct SimUpload {
    net: Arc<Net>,
    life: u64,
    path: String,
    inner: Box<dyn MultipartUpload>,
}

#[async_trait]
impl MultipartUpload for SimUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        if self.net.is_dead(self.life) {
            return async { Err(sim_err("process killed")) }.boxed();
        }
        self.inner.put_part(data)
    }
    async fn complete(&mut self) -> Result<PutResult> {
        let f = self.net.admit(self.life, Op::Multipart, &self.path).await?;
        let r = self.inner.complete().await?;
        self.net.after(f).await?;
        Ok(r)
    }
    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

#[async_trait]
impl ObjectStore for ProcStore {
    async fn put_opts(&self, l: &Path, p: PutPayload, o: PutOptions) -> Result<PutResult> {
        let f = self.net.admit(self.life, Op::Put, l.as_ref()).await?;
        let r = self.net.inner.put_opts(l, p, o).await?;
        self.net.after(f).await?;
        Ok(r)
    }
    async fn put_multipart_opts(&self, l: &Path, o: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        if self.net.is_dead(self.life) {
            return Err(sim_err("process killed"));
        }
        let inner = self.net.inner.put_multipart_opts(l, o).await?;
        Ok(Box::new(SimUpload { net: Arc::clone(&self.net), life: self.life, path: l.to_string(), inner }))
    }
    async fn get_opts(&self, l: &Path, o: GetOptions) -> Result<GetResult> {
        self.net.admit(self.life, Op::Get, l.as_ref()).await?;
        self.net.inner.get_opts(l, o).await
    }
    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        let net = Arc::clone(&self.net);
        let life = self.life;
        locations
            .then(move |r| {
                let net = Arc::clone(&net);
                async move {
                    let p = r?;
                    let f = net.admit(life, Op::Delete, p.as_ref()).await?;
                    match net.inner.delete(&p).await {
                        Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                        Err(e) => return Err(e),
                    }
                    net.after(f).await?;
                    Ok(p)
                }
            })
            .boxed()
    }
    fn list(&self, p: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let net = Arc::clone(&self.net);
        let life = self.life;
        let p = p.cloned();
        futures::stream::once(async move {
            let s = p.as_ref().map(|x| x.to_string()).unwrap_or_default();
            match net.admit(life, Op::List, &format!("{s}/")).await {
                Ok(_) => net.inner.list(p.as_ref()),
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            }
        })
        .flatten()
        .boxed()
    }
    fn list_with_offset(&self, p: Option<&Path>, o: &Path) -> BoxStream<'static, Result<ObjectMeta>> {
        let net = Arc::clone(&self.net);
        let life = self.life;
        let p = p.cloned();
        let o = o.clone();
        futures::stream::once(async move {
            let s = p.as_ref().map(|x| x.to_string()).unwrap_or_default();
            match net.admit(life, Op::List, &format!("{s}/")).await {
                Ok(_) => net.inner.list_with_offset(p.as_ref(), &o),
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            }
        })
        .flatten()
        .boxed()
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> Result<ListResult> {
        let s = p.map(|x| x.to_string()).unwrap_or_default();
        self.net.admit(self.life, Op::List, &format!("{s}/")).await?;
        self.net.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> Result<()> {
        let fault = self.net.admit(self.life, Op::Put, t.as_ref()).await?;
        self.net.inner.copy_opts(f, t, o).await?;
        self.net.after(fault).await
    }
    async fn rename_opts(&self, f: &Path, t: &Path, o: RenameOptions) -> Result<()> {
        let fault = self.net.admit(self.life, Op::Put, t.as_ref()).await?;
        self.net.inner.rename_opts(f, t, o).await?;
        self.net.after(fault).await
    }
}
