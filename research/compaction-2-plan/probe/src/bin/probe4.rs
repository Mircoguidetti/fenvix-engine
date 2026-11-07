//! Compaction 2 plan, review round 2 (fenvix-brain `prompts/R-review-compaction-2-plan-r2.md`): probe 4.
//!
//! Probe 3 (the owner's round 2) checks the pin and drop protocol in the sequences of probe 2. This probe tries to break
//! the same protocol in the cases the review prompt names, and a few more found while reading it. Same SlateDB 0.17.0
//! public API, in-memory store and stand-in manifest as probe 3 (the helpers are copied from `probe3.rs`, with the lease
//! made a per-database field, a revocable pin and a counter of the rows each drop job keeps).
//!
//! Points:
//!   EARLY   a drop job started before the lease expires (no pins: only the lease protects a reader that knows v0).
//!   SLOW    a reader in another process that refreshed just before a slow publish became visible (the PUT took longer
//!           than the margin). As written, the lease counts from the stamp inside the version; the variant counts it
//!           from the first time the drop job's floor computation saw the version in the store.
//!   STEP    a reader in another process whose operation passed the T_pin check at its start, then took longer: its
//!           second step reads a SlateDB view refreshed after the drop. Checked per operation, then per step.
//!   REVOKE  an in-process pin revoked (past its 120 s life) between two steps of a read: without a check after each
//!           step the read returns wrong rows; with it the read fails.
//!   TXN60   a transaction held past the lease (60 s against a 60 s lease, scaled) while 5 versions are published and
//!           dropped; then held past the pin's life (120 s, scaled), when it must fail.
//!   REMOTE  the drop job runs in another process than the pins (fix 7's compaction pool): the writer's transaction
//!           pins are invisible to its floor. Then the same with a time-only floor whose lease exceeds a pin's life.
//!   FOLD    fix 2 under pins: 100 rounds with transaction pins taken and released at random; the sorted-run count,
//!           every pinned read against the model at its time, and the rows each fold rewrites (rows kept per row
//!           inserted), against the prediction 1 + lease / (time between folds).
//!
//! Time is scaled: a 6 s lease stands for the plan's 60 s (T_pin 30 s + margin 30 s), so 6 s of transaction = 60 s and
//! 12 s of pin life = 120 s.
//!
//! Run: `cargo run --release --bin probe4` (about 5 minutes; `P4_ONLY=FOLD` runs one point). Result: `../results/probe-4.txt`.

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use slatedb::admin::Admin;
use slatedb::compactor::{CompactionScheduler, CompactionSchedulerSupplier, CompactionSpec, CompactorStateView, SourceId};
use slatedb::config::{CompactionWorkerOptions, CompactorOptions, DbReaderOptions, FlushOptions, FlushType, Settings};
use slatedb::{CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier, CompactionJobContext, DbReader, DbReaderMode, RowEntry, ValueDeletable};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const LIVE: u8 = 0;
const DEAD: u8 = 1;
/// Pin life (the plan's 120 s), scaled like the lease.
const PIN_LIFE: Duration = Duration::from_millis(12_000);

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

fn live(v: &[u8]) -> Bytes {
    let mut b = vec![LIVE];
    b.extend_from_slice(v);
    Bytes::from(b)
}

fn key(c: u32, i: u32) -> Vec<u8> {
    format!("u/{c:03}/d/{i:04}").into_bytes()
}

#[derive(Clone, Default, Debug)]
struct Version {
    id: u64,
    x: u64,
    ms: u64,
    sealed: BTreeMap<Vec<u8>, (u64, Bytes)>,
}

impl Version {
    fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        for n in [self.id, self.x, self.ms, self.sealed.len() as u64] {
            b.extend_from_slice(&n.to_le_bytes());
        }
        for (k, (s, v)) in &self.sealed {
            b.extend_from_slice(&(k.len() as u64).to_le_bytes());
            b.extend_from_slice(k);
            b.extend_from_slice(&s.to_le_bytes());
            b.extend_from_slice(&(v.len() as u64).to_le_bytes());
            b.extend_from_slice(v);
        }
        b
    }
    fn decode(b: &[u8]) -> Version {
        let rd = |p: usize| u64::from_le_bytes(b[p..p + 8].try_into().unwrap());
        let (id, x, ms, n) = (rd(0), rd(8), rd(16), rd(24));
        let mut v = Version { id, x, ms, sealed: BTreeMap::new() };
        let mut p = 32;
        for _ in 0..n {
            let kl = rd(p) as usize;
            p += 8;
            let k = b[p..p + kl].to_vec();
            p += kl;
            let s = rd(p);
            p += 8;
            let vl = rd(p) as usize;
            p += 8;
            let val = Bytes::copy_from_slice(&b[p..p + vl]);
            p += vl;
            v.sealed.insert(k, (s, val));
        }
        v
    }
}

fn manifest_path(db: &str, id: u64) -> Path {
    Path::from(format!("{db}/fx/manifest/{id:020}"))
}

async fn load_version(store: &Arc<dyn ObjectStore>, db: &str, id: u64) -> Result<Option<Version>> {
    match store.get(&manifest_path(db, id)).await {
        Ok(r) => Ok(Some(Version::decode(&r.bytes().await?))),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

async fn published(store: &Arc<dyn ObjectStore>, db: &str) -> Result<Vec<Version>> {
    let mut out = Vec::new();
    while let Some(v) = load_version(store, db, out.len() as u64).await? {
        out.push(v);
    }
    Ok(out)
}

/// One process's view of a database's protocol. Two of them on the same database stand for two processes.
struct Ctl {
    store: Arc<dyn ObjectStore>,
    db: String,
    lease_ms: u64,
    cur: Mutex<Arc<Version>>,
    pins: Mutex<BTreeMap<u64, u64>>,
    revoked: Mutex<HashSet<u64>>,
    next_pin: AtomicU64,
    absorbed: Mutex<HashSet<ulid::Ulid>>,
    refold: AtomicBool,
    started: AtomicU64,
    done: AtomicU64,
    last_floor: AtomicU64,
    /// When the last drop job built its filter (ms), and when the last job was proposed (ms).
    last_floor_at: AtomicU64,
    proposed_at: AtomicU64,
    /// Rows seen and kept by every drop job of this process (the fold's rewrite).
    seen: AtomicU64,
    kept: AtomicU64,
    /// Count the lease from the first time a floor computation saw a version (true) or from its stamp (false).
    from_observed: AtomicBool,
    observed: Mutex<HashMap<u64, u64>>,
}

impl Ctl {
    fn new(store: &Arc<dyn ObjectStore>, db: &str, lease: Duration) -> Arc<Ctl> {
        Arc::new(Ctl {
            store: store.clone(),
            db: db.to_string(),
            lease_ms: lease.as_millis() as u64,
            cur: Mutex::new(Arc::new(Version::default())),
            pins: Mutex::default(),
            revoked: Mutex::default(),
            next_pin: AtomicU64::new(0),
            absorbed: Mutex::default(),
            refold: AtomicBool::new(false),
            started: AtomicU64::new(0),
            done: AtomicU64::new(0),
            last_floor: AtomicU64::new(0),
            last_floor_at: AtomicU64::new(0),
            proposed_at: AtomicU64::new(0),
            seen: AtomicU64::new(0),
            kept: AtomicU64::new(0),
            from_observed: AtomicBool::new(false),
            observed: Mutex::default(),
        })
    }

    /// P2 of the plan r2: the floor from the published manifest objects and this process's live pins.
    async fn floor(&self) -> u64 {
        let now = now_ms();
        let obs = self.from_observed.load(Ordering::SeqCst);
        let mut f = 0;
        for v in published(&self.store, &self.db).await.unwrap_or_default() {
            let t0 = if obs { *self.observed.lock().unwrap().entry(v.id).or_insert(now) } else { v.ms };
            if now.saturating_sub(t0) >= self.lease_ms {
                f = f.max(v.x);
            }
        }
        for x in self.pins.lock().unwrap().values() {
            f = f.min(*x);
        }
        f
    }

    fn current(&self) -> Arc<Version> {
        self.cur.lock().unwrap().clone()
    }
}

struct Pin {
    ctl: Arc<Ctl>,
    id: u64,
    v: Arc<Version>,
    at: Instant,
}

fn pin(ctl: &Arc<Ctl>) -> Pin {
    let mut pins = ctl.pins.lock().unwrap();
    let v = ctl.current();
    let id = ctl.next_pin.fetch_add(1, Ordering::SeqCst);
    pins.insert(id, v.x);
    Pin { ctl: ctl.clone(), id, v, at: Instant::now() }
}

impl Pin {
    /// Revoke: the pin stops holding the floor down (its life is over).
    fn revoke(&self) {
        self.ctl.pins.lock().unwrap().remove(&self.id);
        self.ctl.revoked.lock().unwrap().insert(self.id);
    }
    /// The check a reader makes after each step: a revoked pin (or one past its life) fails the read.
    fn check(&self) -> Result<()> {
        if self.at.elapsed() > PIN_LIFE {
            self.revoke();
        }
        if self.ctl.revoked.lock().unwrap().contains(&self.id) {
            anyhow::bail!("pin revoked: TransientTransactionError");
        }
        Ok(())
    }
}

impl Drop for Pin {
    fn drop(&mut self) {
        self.ctl.pins.lock().unwrap().remove(&self.id);
    }
}

#[derive(Clone)]
struct Sup(Arc<Ctl>);

impl CompactionSchedulerSupplier for Sup {
    fn compaction_scheduler(&self, _o: &CompactorOptions) -> Box<dyn CompactionScheduler + Send + Sync> {
        Box::new(self.clone())
    }
}

impl CompactionScheduler for Sup {
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        if let Some(c) = state.compactions() {
            if c.recent_compactions().any(|c| c.active()) {
                return vec![];
            }
        }
        let m = state.manifest();
        let absorbed = self.0.absorbed.lock().unwrap().clone();
        let mut src: Vec<SourceId> = m.l0().iter().filter(|v| absorbed.contains(&v.id)).map(|v| SourceId::SstView(v.id)).collect();
        let newest = m.compacted().iter().max_by_key(|r| r.id).map(|r| r.id);
        if src.is_empty() {
            match (newest, self.0.refold.swap(false, Ordering::SeqCst)) {
                (Some(id), true) => {
                    self.0.proposed_at.store(now_ms(), Ordering::SeqCst);
                    self.0.started.fetch_add(1, Ordering::SeqCst);
                    return vec![CompactionSpec::new(vec![SourceId::SortedRun(id)], id)];
                }
                _ => return vec![],
            }
        }
        self.0.proposed_at.store(now_ms(), Ordering::SeqCst);
        self.0.started.fetch_add(1, Ordering::SeqCst);
        match newest {
            Some(id) => {
                src.push(SourceId::SortedRun(id));
                vec![CompactionSpec::new(src, id)]
            }
            None => vec![CompactionSpec::new(src, 0)],
        }
    }
}

struct Filter {
    floor: u64,
    ctl: Arc<Ctl>,
}

#[async_trait]
impl CompactionFilter for Filter {
    async fn filter(&mut self, entry: &RowEntry) -> Result<CompactionFilterDecision, CompactionFilterError> {
        self.ctl.seen.fetch_add(1, Ordering::Relaxed);
        Ok(if entry.seq <= self.floor {
            CompactionFilterDecision::Drop
        } else {
            // Count only the rows the FOLD rounds insert (customers 100 and up), not the initial load.
            if entry.key.starts_with(b"u/1") {
                self.ctl.kept.fetch_add(1, Ordering::Relaxed);
            }
            CompactionFilterDecision::Keep
        })
    }
    async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
        self.ctl.done.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl CompactionFilterSupplier for Sup {
    async fn create_compaction_filter(&self, _c: &CompactionJobContext) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
        let floor = self.0.floor().await;
        self.0.last_floor.store(floor, Ordering::SeqCst);
        self.0.last_floor_at.store(now_ms(), Ordering::SeqCst);
        Ok(Box::new(Filter { floor, ctl: self.0.clone() }))
    }
}

/// Open the writer with its drop jobs run by `comp` (the same Ctl as the pins: one process; another one: a pool).
async fn open_with(comp: &Arc<Ctl>) -> Result<slatedb::Db> {
    let sup = Sup(comp.clone());
    let mut s = Settings::default();
    s.compactor_options = None;
    s.l0_max_ssts = 256;
    let mut co = CompactorOptions::default();
    co.poll_interval = Duration::from_millis(20);
    co.commit_compacted_interval = Duration::from_millis(100);
    // The embedded worker builds the job's filter (and so computes the floor) when it picks the job up: poll often, so
    // the probe's timings are not dominated by the worker's default poll.
    let mut wo = CompactionWorkerOptions::default();
    wo.compactions_poll_interval = Duration::from_millis(50);
    co.worker = Some(wo);
    let cb = slatedb::CompactorBuilder::new(comp.db.clone(), comp.store.clone())
        .with_options(co)
        .with_scheduler_supplier(Arc::new(sup.clone()))
        .with_compaction_filter_supplier(Arc::new(sup));
    Ok(slatedb::Db::builder(comp.db.clone(), comp.store.clone()).with_settings(s).with_compactor_builder(cb).build().await?)
}

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn finish(out: BTreeMap<Vec<u8>, (u64, Bytes)>) -> Model {
    out.into_iter().filter(|(_, (_, v))| v[0] == LIVE).map(|(k, (_, v))| (k, v[1..].to_vec())).collect()
}

macro_rules! merged {
    ($view:expr, $v:expr) => {{
        let v: &Version = $v;
        let mut out = v.sealed.clone();
        let mut it = $view.scan(b"u/".to_vec()..).await?;
        while let Some(kv) = it.next().await? {
            if kv.seq > v.x {
                out.insert(kv.key.to_vec(), (kv.seq, kv.value.clone()));
            }
        }
        finish(out)
    }};
}

async fn count_le(db: &slatedb::Db, x: u64) -> Result<usize> {
    let mut n = 0;
    let mut it = db.scan(b"u/".to_vec()..).await?;
    while let Some(kv) = it.next().await? {
        if kv.seq <= x {
            n += 1;
        }
    }
    Ok(n)
}

async fn flush(db: &slatedb::Db) -> Result<()> {
    db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await?;
    Ok(())
}

async fn absorb(db: &slatedb::Db, ctl: &Ctl, prev: &Version) -> Result<(Version, Vec<ulid::Ulid>)> {
    let m = db.manifest();
    let views: Vec<_> = m.l0().iter().cloned().collect();
    let reader = slatedb::SstReader::new(ctl.db.clone(), ctl.store.clone(), None, None);
    let mut v = prev.clone();
    v.id = prev.id + 1;
    for view in &views {
        let f = reader.open_with_handle(view.sst.clone())?;
        for b in 0..f.index().await?.len() {
            for r in f.read_block(b).await? {
                if r.seq <= prev.x {
                    continue;
                }
                let val = match &r.value {
                    ValueDeletable::Value(x) => x.clone(),
                    _ => Bytes::from_static(&[DEAD]),
                };
                v.x = v.x.max(r.seq);
                let e = v.sealed.entry(r.key.to_vec()).or_insert((0, Bytes::new()));
                if r.seq >= e.0 {
                    *e = (r.seq, val);
                }
            }
        }
    }
    Ok((v, views.iter().map(|v| v.id).collect()))
}

/// Publish with a conditional create. `put_takes` stands for a slow PUT: the version is stamped, then becomes visible
/// only after this delay (S3 retries, a slow network).
async fn publish_slow(ctl: &Ctl, mut v: Version, put_takes: Duration) -> Result<Arc<Version>> {
    v.ms = now_ms();
    tokio::time::sleep(put_takes).await;
    let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
    ctl.store.put_opts(&manifest_path(&ctl.db, v.id), PutPayload::from(v.encode()), opts).await?;
    let v = Arc::new(v);
    *ctl.cur.lock().unwrap() = v.clone();
    Ok(v)
}

async fn publish(ctl: &Ctl, v: Version) -> Result<Arc<Version>> {
    publish_slow(ctl, v, Duration::ZERO).await
}

async fn wait_jobs(ctl: &Ctl) -> Result<()> {
    for _ in 0..1500 {
        if ctl.done.load(Ordering::SeqCst) >= ctl.started.load(Ordering::SeqCst) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    anyhow::bail!("drop job never finished")
}

async fn fold(db: &slatedb::Db, comp: &Ctl, ids: &[ulid::Ulid]) -> Result<()> {
    comp.absorbed.lock().unwrap().extend(ids.iter().copied());
    for _ in 0..1500 {
        if !db.manifest().l0().iter().any(|v| ids.contains(&v.id)) {
            return wait_jobs(comp).await;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    anyhow::bail!("drop job never ran")
}

async fn refold(comp: &Ctl) -> Result<()> {
    let before = comp.started.load(Ordering::SeqCst);
    comp.refold.store(true, Ordering::SeqCst);
    for _ in 0..1500 {
        if comp.started.load(Ordering::SeqCst) > before {
            return wait_jobs(comp).await;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    anyhow::bail!("refold never started")
}

async fn settle() {
    tokio::time::sleep(Duration::from_millis(1500)).await;
}

async fn sleep_until(t: Instant) {
    let now = Instant::now();
    if t > now {
        tokio::time::sleep(t - now).await;
    }
}

fn verdict(ok: bool) -> &'static str {
    if ok {
        "PASS"
    } else {
        "BREAKS"
    }
}

fn missing(model: &Model, got: &Model) -> usize {
    model.iter().filter(|(k, v)| got.get(*k) != Some(*v)).count() + got.keys().filter(|k| !model.contains_key(*k)).count()
}

/// Rows of `m` whose customer is in `c`.
fn part(m: &Model, c: std::ops::Range<u32>) -> Model {
    m.iter().filter(|(k, _)| c.contains(&std::str::from_utf8(&k[2..5]).unwrap().parse::<u32>().unwrap())).map(|(k, v)| (k.clone(), v.clone())).collect()
}

async fn load(db: &slatedb::Db, model: &mut Model, c: std::ops::Range<u32>, tag: &str) -> Result<()> {
    for c in c {
        for i in 0..100 {
            let v = format!("{tag}-{c}-{i}");
            db.put(&key(c, i), &live(v.as_bytes())).await?;
            model.insert(key(c, i), v.into_bytes());
        }
    }
    flush(db).await
}

async fn follower(store: &Arc<dyn ObjectStore>, db: &str) -> Result<DbReader> {
    let mut o = DbReaderOptions::default();
    o.skip_wal_replay = true;
    Ok(DbReader::builder(db.to_string(), store.clone()).with_reader_mode(DbReaderMode::FollowLatest).with_options(o).build().await?)
}

/// SLOW: returns (the version the follower knew, keys the follower misses, floor of the second drop job, X of v1).
async fn scenario_slow(store: &Arc<dyn ObjectStore>, name: &str, from_observed: bool) -> Result<(u64, usize, u64, u64, usize)> {
    let t_pin = Duration::from_millis(6000);
    let ctl = Ctl::new(store, name, t_pin + Duration::from_millis(1000));
    ctl.from_observed.store(from_observed, Ordering::SeqCst);
    let db = open_with(&ctl).await?;
    publish(&ctl, Version::default()).await?;
    let mut model = Model::new();
    load(&db, &mut model, 0..10, "s1").await?;
    let (v1, ids) = absorb(&db, &ctl, &ctl.current()).await?;
    let t0 = Instant::now();
    let c2 = ctl.clone();
    let slow = tokio::spawn(async move { publish_slow(&c2, v1, Duration::from_millis(7000)).await });
    // The follower's periodic refresh, 6.8 s after the stamp: v1 is not visible yet.
    sleep_until(t0 + Duration::from_millis(6800)).await;
    let known = published(store, name).await?.pop().unwrap();
    let refreshed = Instant::now();
    let v1 = slow.await??;
    fold(&db, &ctl, &ids).await?; // the drop job right after v1 became visible: the lease has passed since the stamp
    let floor = ctl.last_floor.load(Ordering::SeqCst);
    settle().await;
    // The follower reads: its refresh is younger than T_pin, so the rule lets it read with the version it knows.
    anyhow::ensure!(refreshed.elapsed() < t_pin, "probe timing: the follower's refresh is already too old");
    let f = follower(store, name).await?;
    let got = merged!(f, &known);
    f.close().await?;
    // Eventually the drop happens with either rule.
    tokio::time::sleep(Duration::from_millis(ctl.lease_ms + 100)).await;
    refold(&ctl).await?;
    settle().await;
    let left = count_le(&db, v1.x).await?;
    db.close().await?;
    Ok((known.id, missing(&model, &got), floor, v1.x, left))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let t_start = Instant::now();
    let mut lines: Vec<String> = Vec::new();
    let mut say = |s: String| {
        println!("{s}");
        lines.push(s);
    };
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let lease = Duration::from_millis(6000);
    let only = std::env::var("P4_ONLY").unwrap_or_default();
    let run = |n: &str| only.is_empty() || only.split(',').any(|x| x == n);

    // ---------- EARLY ----------
    if run("EARLY") {
        let ctl = Ctl::new(&store, "e", lease);
        let db = open_with(&ctl).await?;
        let v0 = publish(&ctl, Version::default()).await?;
        let mut model = Model::new();
        load(&db, &mut model, 0..10, "e1").await?;
        let (v1, ids) = absorb(&db, &ctl, &v0).await?;
        let v1 = publish(&ctl, v1).await?;
        let mut cases = Vec::new();
        let mut ok = true;
        // Jobs proposed at once and at 3, 4, 5 and 5.5 s after the stamp, then one after the lease.
        for (i, at) in [0u64, 3000, 4000, 5000, 5500, 6200].iter().enumerate() {
            let t = v1.ms + at;
            let now = now_ms();
            if t > now {
                tokio::time::sleep(Duration::from_millis(t - now)).await;
            }
            if i == 0 {
                fold(&db, &ctl, &ids).await?;
            } else {
                refold(&ctl).await?;
            }
            let (f, f_at, p_at) = (ctl.last_floor.load(Ordering::SeqCst), ctl.last_floor_at.load(Ordering::SeqCst), ctl.proposed_at.load(Ordering::SeqCst));
            settle().await;
            let kept = count_le(&db, v1.x).await?;
            let built = f_at - v1.ms;
            let legal = if built >= lease.as_millis() as u64 { f == v1.x || f == 0 } else { f == 0 };
            let reader_ok = built >= lease.as_millis() as u64 || missing(&model, &merged!(db, &v0)) == 0;
            ok &= legal && reader_ok && (f == 0) == (kept == 1000);
            cases.push(format!("proposed at {:.1} s, filter built at {:.1} s, floor {f}, rows <= X kept {kept}", (p_at - v1.ms) as f64 / 1000.0, built as f64 / 1000.0));
        }
        let end_ok = missing(&model, &merged!(db, &v1)) == 0 && count_le(&db, v1.x).await? == 0;
        say(format!(
            "EARLY drop jobs proposed before the lease (6 s) ends, no pins, a reader elsewhere knows only v0 (X of v1 = {}): {}; the floor rises only for a job whose filter is built after the lease, and a v0 read misses 0 before it; after the lease a v1 read = model {end_ok}: {}",
            v1.x,
            cases.join("; "),
            verdict(ok && end_ok)
        ));
        db.close().await?;
    }

    // ---------- SLOW ----------
    if run("SLOW") {
    let (known, miss, floor, x1, left) = scenario_slow(&store, "s", false).await?;
    say(format!(
        "SLOW as written (lease counted from the stamp): the PUT of v1 (X = {x1}) took 7 s, the margin is 1 s (T_pin 6 s); a follower refreshed 6.8 s after the stamp (knows v{known}) and reads about 3 s later, inside T_pin; the drop job's floor was {floor}: the follower misses {miss} of 1000 keys: {}",
        if miss > 0 { "BREAKS" } else { "PASS" }
    ));
    let (known, miss, floor, x1, left) = {
        let _ = (known, miss, floor, x1, left);
        scenario_slow(&store, "s2", true).await?
    };
    say(format!(
        "SLOW, lease counted from the first time the drop job saw the version in the store: same timings, the follower knows v{known}, the floor was {floor}: it misses {miss} of 1000 keys; one lease later the rows <= X = {x1} left in SlateDB: {left}: {}",
        verdict(miss == 0 && floor == 0 && left == 0)
    ));
    }

    // ---------- STEP ----------
    if run("STEP") {
        let t_pin = Duration::from_millis(3000);
        let ctl = Ctl::new(&store, "m", lease);
        let db = open_with(&ctl).await?;
        publish(&ctl, Version::default()).await?;
        let mut model = Model::new();
        load(&db, &mut model, 0..10, "m1").await?;
        let (v1, ids) = absorb(&db, &ctl, &ctl.current()).await?;
        let known = published(&store, "m").await?.pop().unwrap();
        let refreshed = Instant::now();
        let t_pub = Instant::now();
        publish(&ctl, v1).await?; // published just after the follower's refresh
        // The operation starts at once: the T_pin check passes.
        let start_ok = refreshed.elapsed() < t_pin;
        let f = follower(&store, "m").await?;
        let step1 = part(&merged!(f, &known), 0..5);
        f.close().await?;
        // It is a long operation (a find with many fetches, an export page); meanwhile the lease ends and the drop runs.
        fold(&db, &ctl, &ids).await?;
        sleep_until(t_pub + lease + Duration::from_millis(100)).await;
        refold(&ctl).await?;
        settle().await;
        // Step 2 reads a SlateDB view refreshed since (a FollowLatest poll, or a new DbReader).
        let f = follower(&store, "m").await?;
        let step2 = part(&merged!(f, &known), 5..10);
        f.close().await?;
        let m1 = missing(&part(&model, 0..5), &step1);
        let m2 = missing(&part(&model, 5..10), &step2);
        let step_refused = refreshed.elapsed() > t_pin;
        say(format!(
            "STEP a follower operation that passed the T_pin check at its start ({start_ok}) and lasted past the drop: step 1 misses {m1} of 500, step 2 (on a view refreshed after the drop, with the same known version) misses {m2} of 500 with the check once per operation: {}; with the check before every step that opens or refreshes a SlateDB view, step 2 is refused ({step_refused}) and nothing wrong is returned: {}",
            if m2 > 0 { "BREAKS" } else { "PASS" },
            verdict(step_refused && m1 == 0)
        ));
        db.close().await?;
    }

    // ---------- REVOKE ----------
    if run("REVOKE") {
        let ctl = Ctl::new(&store, "r", lease);
        let db = open_with(&ctl).await?;
        publish(&ctl, Version::default()).await?;
        let mut model = Model::new();
        load(&db, &mut model, 0..10, "r1").await?;
        let reader = pin(&ctl);
        let step1 = part(&merged!(db, &reader.v), 0..5);
        let c1 = reader.check().is_ok();
        reader.revoke(); // its 120 s are over (a stuck client, a leaked cursor)
        let (v1, ids) = absorb(&db, &ctl, &ctl.current()).await?;
        publish(&ctl, v1).await?;
        fold(&db, &ctl, &ids).await?;
        tokio::time::sleep(lease + Duration::from_millis(100)).await;
        refold(&ctl).await?;
        settle().await;
        let step2 = part(&merged!(db, &reader.v), 5..10);
        let wrong = missing(&part(&model, 5..10), &step2);
        let after = reader.check();
        say(format!(
            "REVOKE an in-process pin revoked between two steps of a read (step 1 checked ok {c1}, misses {}): step 2 after the drop misses {wrong} of 500; without a check after the step those rows are returned: {}; with the check after every step the read fails ({}) and returns nothing: {}",
            missing(&part(&model, 0..5), &step1),
            if wrong > 0 { "BREAKS" } else { "PASS" },
            after.as_ref().err().map(|e| e.to_string()).unwrap_or_else(|| "no error".into()),
            verdict(after.is_err())
        ));
        drop(reader);
        db.close().await?;
    }

    // ---------- TXN60 ----------
    if run("TXN60") {
        let ctl = Ctl::new(&store, "t", lease);
        let db = open_with(&ctl).await?;
        publish(&ctl, Version::default()).await?;
        let mut model = Model::new();
        load(&db, &mut model, 0..10, "t1").await?;
        let (v1, ids) = absorb(&db, &ctl, &ctl.current()).await?;
        publish(&ctl, v1).await?;
        fold(&db, &ctl, &ids).await?;
        let txn = pin(&ctl);
        let snap = db.snapshot().await?;
        let at_s = model.clone();
        let mut rounds = 0;
        while txn.at.elapsed() < lease + Duration::from_millis(500) {
            let c = rounds % 10;
            for i in 0..100 {
                let v = format!("t2-{rounds}-{c}-{i}");
                db.put(&key(c, i), &live(v.as_bytes())).await?;
                model.insert(key(c, i), v.into_bytes());
            }
            flush(&db).await?;
            let (nv, ids) = absorb(&db, &ctl, &ctl.current()).await?;
            publish(&ctl, nv).await?;
            fold(&db, &ctl, &ids).await?;
            rounds += 1;
            tokio::time::sleep(Duration::from_millis(700)).await;
        }
        refold(&ctl).await?;
        settle().await;
        let held = txn.at.elapsed();
        let floor = ctl.last_floor.load(Ordering::SeqCst);
        let r = merged!(snap, &txn.v);
        let ok_at_limit = txn.check().is_ok();
        let differ = missing(&at_s, &r);
        // Past the pin's life the transaction must fail, and the floor must move up.
        sleep_until(txn.at + PIN_LIFE + Duration::from_millis(100)).await;
        let fails = txn.check().is_err();
        refold(&ctl).await?;
        settle().await;
        let x = ctl.current().x;
        let left = count_le(&db, x).await?;
        let p = pin(&ctl);
        let now_ok = merged!(db, &p.v) == model;
        drop(p);
        say(format!(
            "TXN60 a transaction held {:.1} s (the lease is {:.0} s) while {rounds} versions were published and dropped: floor held at its X = {} (floor {floor}), {differ} of 1000 keys differ from the state at S, check ok {ok_at_limit}; past the pin life ({:.0} s) the read fails {fails}, then the drop applies (rows <= X left {left}) and a new read = model {now_ok}: {}",
            held.as_secs_f64(),
            lease.as_secs_f64(),
            txn.v.x,
            PIN_LIFE.as_secs_f64(),
            verdict(differ == 0 && ok_at_limit && floor == txn.v.x && fails && left == 0 && now_ok)
        ));
        drop(snap);
        drop(txn);
        db.close().await?;
    }

    // ---------- REMOTE ----------
    for (name, comp_lease, label) in if !run("REMOTE") { vec![] } else { vec![("x1", lease, "floor with this process's pins and a 6 s lease"), ("x2", PIN_LIFE + Duration::from_millis(1000), "time-only floor, lease = pin life + margin (13 s)")] } {
        let w = Ctl::new(&store, name, lease); // the writer process: pins, publishes
        let comp = Ctl::new(&store, name, comp_lease); // the compaction pool: drop jobs, no view of the writer's pins
        let db = open_with(&comp).await?;
        publish(&w, Version::default()).await?;
        let mut model = Model::new();
        load(&db, &mut model, 0..10, "x1").await?;
        let txn = pin(&w);
        let snap = db.snapshot().await?;
        let at_s = model.clone();
        let (v1, ids) = absorb(&db, &w, &w.current()).await?;
        let v1 = publish(&w, v1).await?;
        fold(&db, &comp, &ids).await?;
        tokio::time::sleep(lease + Duration::from_millis(600)).await; // the transaction is 60 s old (scaled)
        refold(&comp).await?;
        settle().await;
        let floor = comp.last_floor.load(Ordering::SeqCst);
        let ok = txn.check().is_ok();
        let differ = missing(&at_s, &merged!(snap, &txn.v));
        say(format!(
            "REMOTE drop jobs in another process than the pins ({label}): a transaction on v0 aged {:.1} s (check ok {ok}), the pool's floor {floor} (X of v1 = {}): the snapshot misses {differ} of 1000 keys: {}",
            txn.at.elapsed().as_secs_f64(),
            v1.x,
            verdict(differ == 0)
        ));
        drop(snap);
        drop(txn);
        db.close().await?;
    }

    // ---------- FOLD ----------
    for (fname, pin_pct) in if run("FOLD") { vec![("f0", 0u64), ("f", 3u64)] } else { vec![] } {
        let ctl = Ctl::new(&store, fname, lease);
        let db = open_with(&ctl).await?;
        publish(&ctl, Version::default()).await?;
        let mut model = Model::new();
        load(&db, &mut model, 0..10, "f0").await?;
        let (v1, ids) = absorb(&db, &ctl, &ctl.current()).await?;
        publish(&ctl, v1).await?;
        fold(&db, &ctl, &ids).await?;
        let admin = Admin::builder(fname, store.clone()).build();
        let mut rng: u64 = 0x2545_F491_4F6C_DD1D;
        let mut next = |n: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % n
        };
        struct Held {
            p: Pin,
            snap: Arc<slatedb::DbSnapshot>,
            at: Model,
            until: u32,
        }
        let mut held: Vec<Held> = Vec::new();
        let (mut taken, mut wrong, mut max_runs, mut max_held) = (0, 0, 0usize, 0usize);
        let kept0 = ctl.kept.load(Ordering::SeqCst);
        let t_rounds = Instant::now();
        let rounds = 100u32;
        for r in 0..rounds {
            for i in 0..10 {
                let v = format!("f-{r}-{i}");
                db.put(&key(100 + r, i), &live(v.as_bytes())).await?;
                model.insert(key(100 + r, i), v.into_bytes());
            }
            flush(&db).await?;
            let (nv, ids) = absorb(&db, &ctl, &ctl.current()).await?;
            publish(&ctl, nv).await?;
            fold(&db, &ctl, &ids).await?;
            if next(10) < pin_pct {
                let p = pin(&ctl);
                let snap = db.snapshot().await?;
                held.push(Held { p, snap, at: model.clone(), until: r + 1 + next(15) as u32 });
                taken += 1;
            }
            let mut keep = Vec::new();
            for h in held.drain(..) {
                if h.until <= r {
                    wrong += missing(&h.at, &merged!(h.snap, &h.p.v));
                } else {
                    keep.push(h);
                }
            }
            held = keep;
            max_held = max_held.max(held.len());
            let runs = admin.read_manifest(None).await?.map(|m| m.compacted().len()).unwrap_or(0);
            max_runs = max_runs.max(runs);
        }
        let dt = t_rounds.elapsed().as_secs_f64() / rounds as f64;
        let kept = ctl.kept.load(Ordering::SeqCst) - kept0;
        for h in held.drain(..) {
            wrong += missing(&h.at, &merged!(h.snap, &h.p.v));
        }
        tokio::time::sleep(lease + Duration::from_millis(100)).await;
        refold(&ctl).await?;
        settle().await;
        let x = ctl.current().x;
        let left = count_le(&db, x).await?;
        let runs = admin.read_manifest(None).await?.map(|m| m.compacted().len()).unwrap_or(0);
        let p = pin(&ctl);
        let end_ok = merged!(db, &p.v) == model;
        drop(p);
        let inserted = rounds as u64 * 10;
        let per_row = kept as f64 / inserted as f64;
        let predicted = 1.0 + lease.as_secs_f64() / dt;
        say(format!(
            "FOLD ({fname}) {rounds} rounds, {taken} transaction pins taken and released at random (up to {max_held} held at once): sorted runs at most {max_runs}, {runs} at the end; pinned reads that differ from the model at their time {wrong}; rows <= X left at the end {left}; merged read = model {end_ok}: {}",
            verdict(max_runs <= 1 && runs <= 1 && wrong == 0 && left == 0 && end_ok)
        ));
        say(format!(
            "FOLD ({fname}) rewrite: one fold every {:.0} ms; the folds kept (rewrote) {kept} rows for {inserted} rows inserted, {per_row:.1} per row, against 1 + lease / time between folds = {predicted:.1} without pins: every row is rewritten by each fold until the floor passes it",
            dt * 1000.0
        ));
        db.close().await?;
    }

    say(format!("probe 4 finished in {:.0} s", t_start.elapsed().as_secs_f64()));
    let out = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../results/probe-4.txt");
    std::fs::write(out, lines.join("\n") + "\n")?;
    Ok(())
}
