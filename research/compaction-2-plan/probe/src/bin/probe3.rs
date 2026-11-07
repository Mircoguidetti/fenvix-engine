//! Compaction 2 plan, round 2 (fenvix-brain `prompts/P-compaction-2-plan-fix.md`): probe 3.
//!
//! Probe 2 (the review) showed the read rule breaking when a reader or a transaction stays alive across a drop (A, B),
//! when a transaction reads with a newer manifest version (C), when every drop adds a sorted run (D) and when a fork is
//! paired with the parent's newest cut (F). This probe runs the same sequences with the round-2 protocol and prints PASS
//! or BREAKS for each. Same SlateDB 0.17.0 public API and in-memory store as probes 1 and 2.
//!
//! The protocol under test (plan r2, section 2):
//!   P1 Every read takes one pair: a manifest version of ours plus a SlateDB view (the live db, a snapshot, a DbReader).
//!      In process it registers a pin; a transaction pins at begin, then takes its SlateDB snapshot (so X_v <= S), and
//!      keeps that version until it ends.
//!   P2 The drop filter computes its floor when the job starts, from the published manifest objects in the store (never
//!      from memory): floor = min(X of the newest version published at least T_pin + margin ago, X of every pinned
//!      version in this process). It drops only rows with seq <= floor. Readers in other processes refresh the version
//!      they know at least every T_pin and refuse to read when their last refresh is older (fail closed).
//!   P3 Every drop job folds into the newest sorted run (sources: absorbed L0 SSTs + that run; destination: that run).
//!   P4 A checkpoint is taken under the drop lock and records the version it pairs with (X <= its last_l0_seq). A fork,
//!      an export and a PITR diff read through the merged rule with that version.
//!
//! Points: A (pinned reader), B (transaction snapshot), C (transaction keeps its version), D2 (drops fold into one run),
//! FOLLOW (a reader in another process, rule d), H (fail point: a cut set in memory, publish refused), F (fork, export,
//! PITR diff through the pairing), G (fix 4 invariants on a model: group merge cut, envelopes above X_g). Points A and H
//! also run a control with the rule switched off, which must reproduce the failure.
//!
//! Run: `cargo run --release --bin probe3` (about 5 minutes). Result: `../results/probe-3.txt`.

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use slatedb::admin::Admin;
use slatedb::compactor::{CompactionScheduler, CompactionSchedulerSupplier, CompactionSpec, CompactorStateView, SourceId};
use slatedb::config::{CheckpointOptions, CompactorOptions, DbReaderOptions, FlushOptions, FlushType, Settings};
use slatedb::{CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier, CompactionJobContext, DbReader, DbReaderMode, RowEntry, ValueDeletable};
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const LIVE: u8 = 0;
const DEAD: u8 = 1;
/// T_pin and the margin of P2, shortened for the probe (the plan proposes 30 s + 30 s). They must exceed the delay
/// between a publish and the start of SlateDB's drop job (about 1 s here, MEASURED by an earlier run of this probe with
/// 0.3 s + 0.3 s, where the follower's version had already expired when the job started).
const T_PIN: Duration = Duration::from_millis(3000);
const MARGIN: Duration = Duration::from_millis(3000);

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

/// Our stand-in manifest version: id, cut X, publish time, and the sealed rows (newest row <= X of every key).
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
        let mut p = 0;
        let mut u = || {
            let n = u64::from_le_bytes(b[p..p + 8].try_into().unwrap());
            p += 8;
            n
        };
        let (id, x, ms, n) = (u(), u(), u(), u());
        let mut v = Version { id, x, ms, sealed: BTreeMap::new() };
        let mut p = 32;
        for _ in 0..n {
            let kl = u64::from_le_bytes(b[p..p + 8].try_into().unwrap()) as usize;
            p += 8;
            let k = b[p..p + kl].to_vec();
            p += kl;
            let s = u64::from_le_bytes(b[p..p + 8].try_into().unwrap());
            p += 8;
            let vl = u64::from_le_bytes(b[p..p + 8].try_into().unwrap()) as usize;
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

/// Every published version, oldest first (versions are numbered 0, 1, 2, ...).
async fn published(store: &Arc<dyn ObjectStore>, db: &str) -> Result<Vec<Version>> {
    let mut out = Vec::new();
    while let Some(v) = load_version(store, db, out.len() as u64).await? {
        out.push(v);
    }
    Ok(out)
}

/// Shared state of one database's protocol: the in-process pins, the absorbed L0 SSTs, the drop lock.
struct Ctl {
    store: Arc<dyn ObjectStore>,
    db: String,
    cur: Mutex<Arc<Version>>,
    pins: Mutex<BTreeMap<u64, u64>>,
    next_pin: AtomicU64,
    absorbed: Mutex<HashSet<ulid::Ulid>>,
    /// The in-memory cut a compactor sets before it publishes (what probe 2 trusted). Read only by the control of H.
    hint: AtomicU64,
    trust_memory: AtomicBool,
    ignore_pins: AtomicBool,
    refold: AtomicBool,
    started: AtomicU64,
    done: AtomicU64,
    last_floor: AtomicU64,
    drop_lock: Arc<tokio::sync::Mutex<()>>,
}

impl Ctl {
    fn new(store: &Arc<dyn ObjectStore>, db: &str) -> Arc<Ctl> {
        Arc::new(Ctl {
            store: store.clone(),
            db: db.to_string(),
            cur: Mutex::new(Arc::new(Version::default())),
            pins: Mutex::default(),
            next_pin: AtomicU64::new(0),
            absorbed: Mutex::default(),
            hint: AtomicU64::new(0),
            trust_memory: AtomicBool::new(false),
            ignore_pins: AtomicBool::new(false),
            refold: AtomicBool::new(false),
            started: AtomicU64::new(0),
            done: AtomicU64::new(0),
            last_floor: AtomicU64::new(0),
            drop_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// P2: the floor of a drop job, from the published manifest objects and the live pins.
    async fn floor(&self) -> u64 {
        if self.trust_memory.load(Ordering::SeqCst) {
            return self.hint.load(Ordering::SeqCst);
        }
        let lease = (T_PIN + MARGIN).as_millis() as u64;
        let now = now_ms();
        let mut f = 0;
        for v in published(&self.store, &self.db).await.unwrap_or_default() {
            if now.saturating_sub(v.ms) >= lease {
                f = f.max(v.x);
            }
        }
        if !self.ignore_pins.load(Ordering::SeqCst) {
            for x in self.pins.lock().unwrap().values() {
                f = f.min(*x);
            }
        }
        f
    }

    fn current(&self) -> Arc<Version> {
        self.cur.lock().unwrap().clone()
    }
}

/// P1: an in-process pin of one version, released on drop.
struct Pin {
    ctl: Arc<Ctl>,
    id: u64,
    v: Arc<Version>,
}

fn pin(ctl: &Arc<Ctl>) -> Pin {
    let v = ctl.current();
    let id = ctl.next_pin.fetch_add(1, Ordering::SeqCst);
    ctl.pins.lock().unwrap().insert(id, v.x);
    Pin { ctl: ctl.clone(), id, v }
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
            // A re-fold of the newest run alone, so a floor that moved up (pins released) is applied without new L0s.
            match (newest, self.0.refold.swap(false, Ordering::SeqCst)) {
                (Some(id), true) => {
                    self.0.started.fetch_add(1, Ordering::SeqCst);
                    return vec![CompactionSpec::new(vec![SourceId::SortedRun(id)], id)];
                }
                _ => return vec![],
            }
        }
        self.0.started.fetch_add(1, Ordering::SeqCst);
        // P3: fold into the newest sorted run.
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
    _lock: Option<tokio::sync::OwnedMutexGuard<()>>,
}

#[async_trait]
impl CompactionFilter for Filter {
    async fn filter(&mut self, entry: &RowEntry) -> Result<CompactionFilterDecision, CompactionFilterError> {
        Ok(if entry.seq <= self.floor { CompactionFilterDecision::Drop } else { CompactionFilterDecision::Keep })
    }
    async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
        self._lock = None;
        self.ctl.done.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl CompactionFilterSupplier for Sup {
    async fn create_compaction_filter(&self, _c: &CompactionJobContext) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
        // P4: a drop job holds the drop lock for its whole run; a checkpoint takes the same lock.
        let lock = self.0.drop_lock.clone().lock_owned().await;
        let floor = self.0.floor().await;
        self.0.last_floor.store(floor, Ordering::SeqCst);
        Ok(Box::new(Filter { floor, ctl: self.0.clone(), _lock: Some(lock) }))
    }
}

async fn open_with_protocol(ctl: &Arc<Ctl>) -> Result<slatedb::Db> {
    let sup = Sup(ctl.clone());
    let mut s = Settings::default();
    s.compactor_options = None;
    s.l0_max_ssts = 256;
    let mut co = CompactorOptions::default();
    co.poll_interval = Duration::from_millis(20);
    let cb = slatedb::CompactorBuilder::new(ctl.db.clone(), ctl.store.clone())
        .with_options(co)
        .with_scheduler_supplier(Arc::new(sup.clone()))
        .with_compaction_filter_supplier(Arc::new(sup));
    Ok(slatedb::Db::builder(ctl.db.clone(), ctl.store.clone()).with_settings(s).with_compactor_builder(cb).build().await?)
}

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn finish(out: BTreeMap<Vec<u8>, (u64, Bytes)>) -> Model {
    out.into_iter().filter(|(_, (_, v))| v[0] == LIVE).map(|(k, (_, v))| (k, v[1..].to_vec())).collect()
}

/// The merged rule on any SlateDB view: SlateDB rows above X, else the version's sealed rows.
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

/// What today's code paths do (export, PITR diff): every SlateDB value is data, no version.
macro_rules! raw {
    ($view:expr) => {{
        let mut out = Model::new();
        let mut it = $view.scan(b"u/".to_vec()..).await?;
        while let Some(kv) = it.next().await? {
            out.insert(kv.key.to_vec(), kv.value.to_vec());
        }
        out
    }};
}

/// SlateDB rows still visible with seq <= x (0 once a drop at floor >= x has run).
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

/// Absorb every L0 SST in SlateDB's manifest into the next version (not published yet).
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

/// Publish with a conditional create; on success the version becomes current in this process.
async fn publish(ctl: &Ctl, mut v: Version) -> Result<Arc<Version>> {
    v.ms = now_ms();
    let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
    ctl.store.put_opts(&manifest_path(&ctl.db, v.id), PutPayload::from(v.encode()), opts).await?;
    let v = Arc::new(v);
    *ctl.cur.lock().unwrap() = v.clone();
    Ok(v)
}

async fn wait_jobs(ctl: &Ctl) -> Result<()> {
    for _ in 0..800 {
        if ctl.done.load(Ordering::SeqCst) >= ctl.started.load(Ordering::SeqCst) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    anyhow::bail!("drop job never finished")
}

/// Hand the absorbed L0 SSTs to the drop job and wait until they leave SlateDB's L0 and the job ends.
async fn fold(db: &slatedb::Db, ctl: &Ctl, ids: &[ulid::Ulid]) -> Result<()> {
    ctl.absorbed.lock().unwrap().extend(ids.iter().copied());
    for _ in 0..800 {
        if !db.manifest().l0().iter().any(|v| ids.contains(&v.id)) {
            return wait_jobs(ctl).await;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    anyhow::bail!("drop job never ran")
}

/// Re-fold the newest sorted run alone, so the drop applies the current floor.
async fn refold(ctl: &Ctl) -> Result<()> {
    let before = ctl.started.load(Ordering::SeqCst);
    ctl.refold.store(true, Ordering::SeqCst);
    for _ in 0..800 {
        if ctl.started.load(Ordering::SeqCst) > before {
            return wait_jobs(ctl).await;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    anyhow::bail!("refold never started")
}

/// The writer's in-memory copy of SlateDB's manifest follows the compactor's output on its poll (about 1 s): wait for it
/// before counting the rows a drop left.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(1500)).await;
}

async fn lease() {
    tokio::time::sleep(T_PIN + MARGIN + Duration::from_millis(50)).await;
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

/// A and B: a reader and a transaction pinned on v0 while v1 is published and the drop job runs twice.
/// Returns (reader missing, snapshot missing, rows <= X left while pinned, rows <= X left after release, merged read
/// with v1 after release equals the model).
async fn scenario_ab(store: &Arc<dyn ObjectStore>, db_path: &str, ignore_pins: bool) -> Result<(usize, usize, usize, usize, bool)> {
    let ctl = Ctl::new(store, db_path);
    ctl.ignore_pins.store(ignore_pins, Ordering::SeqCst);
    let db = open_with_protocol(&ctl).await?;
    publish(&ctl, Version::default()).await?;
    let mut model = Model::new();
    load(&db, &mut model, 0..10, "r1").await?;
    let reader = pin(&ctl); // P1: a find pins its version for every step
    let txn = pin(&ctl); // P1: a transaction pins, then takes its snapshot
    let snap = db.snapshot().await?;
    let (v1, ids) = absorb(&db, &ctl, &txn.v).await?;
    let v1 = publish(&ctl, v1).await?;
    fold(&db, &ctl, &ids).await?;
    lease().await;
    refold(&ctl).await?; // the lease has passed: only the pins hold the floor down now
    settle().await;
    let left_pinned = count_le(&db, v1.x).await?;
    let a = merged!(db, &reader.v);
    let b = merged!(snap, &txn.v);
    let (am, bm) = (missing(&model, &a), missing(&model, &b));
    drop(reader);
    drop(txn);
    drop(snap);
    refold(&ctl).await?;
    settle().await;
    let left_after = count_le(&db, v1.x).await?;
    let p = pin(&ctl);
    let ok = merged!(db, &p.v) == model;
    drop(p);
    db.close().await?;
    Ok((am, bm, left_pinned, left_after, ok))
}

/// H: a compactor sets its cut in memory, then its publish is refused. Returns rows missing from the winner's read.
async fn scenario_h(store: &Arc<dyn ObjectStore>, db_path: &str, trust_memory: bool) -> Result<(usize, u64, u64, bool)> {
    let ctl = Ctl::new(store, db_path);
    ctl.trust_memory.store(trust_memory, Ordering::SeqCst);
    let db = open_with_protocol(&ctl).await?;
    let v0 = publish(&ctl, Version::default()).await?;
    let mut model = Model::new();
    load(&db, &mut model, 0..1, "h1").await?;
    let (win, win_ids) = absorb(&db, &ctl, &v0).await?; // the winner read the first L0 only
    load(&db, &mut model, 1..2, "h2").await?;
    let (lose, lose_ids) = absorb(&db, &ctl, &v0).await?; // the loser read both L0 SSTs: same version id, higher X
    ctl.hint.store(lose.x, Ordering::SeqCst); // fail point: the loser's cut is set in memory ...
    let win = publish(&ctl, win).await?;
    let refused = publish(&ctl, lose.clone()).await.is_err(); // ... and its publish is refused
    *ctl.cur.lock().unwrap() = win.clone();
    // Worst case: the loser also handed its L0 SSTs to the drop job.
    let mut ids = win_ids;
    ids.extend(lose_ids);
    lease().await;
    fold(&db, &ctl, &ids).await?;
    let p = pin(&ctl);
    let got = merged!(db, &p.v);
    drop(p);
    db.close().await?;
    Ok((missing(&model, &got), win.x, lose.x, refused))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let t0 = Instant::now();
    let mut lines: Vec<String> = Vec::new();
    let mut say = |s: String| {
        println!("{s}");
        lines.push(s);
    };
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());

    // ---------- A and B, with the rule and with the pins ignored (control) ----------
    let (am, bm, lp, la, ok) = scenario_ab(&store, "a", false).await?;
    say(format!(
        "A pinned reader across publish + drop (pins on): the reader on v0 misses {am} of 1000 keys; SlateDB rows <= X left while pinned {lp}, after release {la}; read with v1 after release = model {ok}: {}",
        verdict(am == 0 && lp > 0 && la == 0 && ok)
    ));
    say(format!(
        "B transaction snapshot across publish + drop (pins on): the snapshot with its pinned v0 misses {bm} of 1000 keys: {}",
        verdict(bm == 0)
    ));
    if std::env::var("P3_ONLY_A").is_ok() {
        return Ok(());
    }
    let (am0, bm0, _, _, _) = scenario_ab(&store, "a0", true).await?;
    say(format!(
        "A/B control (pins ignored, probe 2's protocol): the reader misses {am0} of 1000, the snapshot {bm0} of 1000: {}",
        if am0 > 0 && bm0 > 0 { "reproduces the failure, as expected" } else { "DOES NOT REPRODUCE (the probe would not detect the bug)" }
    ));

    // ---------- C, D2 and the follower on one database ----------
    {
        let ctl = Ctl::new(&store, "c");
        let db = open_with_protocol(&ctl).await?;
        publish(&ctl, Version::default()).await?;
        let mut model = Model::new();
        load(&db, &mut model, 0..10, "r1").await?;
        let (v1, ids) = absorb(&db, &ctl, &ctl.current()).await?;
        publish(&ctl, v1).await?;
        fold(&db, &ctl, &ids).await?;

        // C: the transaction pins v1 then snapshots; key 0/* is rewritten, absorbed and published with X above S.
        let txn = pin(&ctl);
        let snap = db.snapshot().await?;
        let s = snap.seq();
        let at_s = model.clone();
        for i in 0..100 {
            let v = format!("r2-0-{i}");
            db.put(&key(0, i), &live(v.as_bytes())).await?;
            model.insert(key(0, i), v.into_bytes());
        }
        flush(&db).await?;
        let (v2, ids) = absorb(&db, &ctl, &ctl.current()).await?;
        let v2 = publish(&ctl, v2).await?;
        fold(&db, &ctl, &ids).await?;
        lease().await;
        refold(&ctl).await?;
        let c_pinned = merged!(snap, &txn.v);
        let c_newest = merged!(snap, &v2);
        let after = pin(&ctl);
        let ryw = merged!(db, &after.v) == model;
        drop(after);
        say(format!(
            "C transaction at S = {s} keeps its version (X = {} <= S) while v2 (X = {} > S) is published and dropped: {} of 1000 keys differ from the state at S (control, re-reading the newest version: {}); a read after the write sees it = {ryw}: {}",
            txn.v.x,
            v2.x,
            missing(&at_s, &c_pinned),
            missing(&at_s, &c_newest),
            verdict(txn.v.x <= s && missing(&at_s, &c_pinned) == 0 && ryw)
        ));
        drop(txn);
        drop(snap);

        // D2: 30 absorb + publish + drop rounds, each folded into the newest run.
        let srs_before = db.manifest().compacted().len();
        for r in 0..30 {
            for i in 0..10 {
                let v = format!("d-{r}-{i}");
                db.put(&key(100 + r, i), &live(v.as_bytes())).await?;
                model.insert(key(100 + r, i), v.into_bytes());
            }
            flush(&db).await?;
            let (nv, ids) = absorb(&db, &ctl, &ctl.current()).await?;
            publish(&ctl, nv).await?;
            fold(&db, &ctl, &ids).await?;
        }
        lease().await;
        refold(&ctl).await?;
        settle().await;
        let srs_after = db.manifest().compacted().len();
        let x = ctl.current().x;
        let left = count_le(&db, x).await?;
        let p = pin(&ctl);
        let d2 = merged!(db, &p.v) == model;
        drop(p);
        say(format!(
            "D2 30 absorb + publish + drop rounds folded into the newest run, with pins and the lease: sorted runs {srs_before} -> {srs_after}; SlateDB rows <= X left at the end {left}; merged read = model {d2}: {}",
            verdict(srs_after <= srs_before.max(1) && left == 0 && d2)
        ));

        // FOLLOW (rule d): a reader in another process (a DbReader: the warm spare, a prefetch reader, a waiter that
        // follows). It knows the versions it refreshed within T_pin and refuses to read with an older refresh. Each
        // case opens a fresh DbReader, so its SlateDB view is the newest one (the hazard: SlateDB's view refreshed
        // before ours).
        async fn open_follower(store: &Arc<dyn ObjectStore>) -> Result<DbReader> {
            Ok(DbReader::builder("c".to_string(), store.clone()).with_reader_mode(DbReaderMode::FollowLatest).build().await?)
        }
        struct Known {
            v: Version,
            at: Instant,
        }
        async fn refresh(store: &Arc<dyn ObjectStore>) -> Result<Known> {
            Ok(Known { v: published(store, "c").await?.pop().unwrap(), at: Instant::now() })
        }
        let mut known = refresh(&store).await?;
        let stale = Known { v: known.v.clone(), at: known.at };
        load(&db, &mut model, 200..201, "f1").await?; // 100 new keys
        let (nv, ids) = absorb(&db, &ctl, &ctl.current()).await?;
        let nv = publish(&ctl, nv).await?;
        fold(&db, &ctl, &ids).await?;
        // Case 1: newest SlateDB view, older known version (refreshed less than T_pin ago), before the lease ends.
        let f = open_follower(&store).await?;
        let view_newer = raw!(f).contains_key(&key(200, 99)) && known.v.x < nv.x;
        let f1m = missing(&model, &merged!(f, &known.v));
        let age_ms = known.at.elapsed().as_millis();
        let floor_held = ctl.last_floor.load(Ordering::SeqCst) <= known.v.x;
        f.close().await?;
        // The lease runs out (the follower keeps refreshing meanwhile); then the drop applies X of nv.
        lease().await;
        refold(&ctl).await?;
        settle().await;
        let dropped = count_le(&db, nv.x).await?;
        // Case 2: after the drop, newest SlateDB view, the refreshed version.
        known = refresh(&store).await?; // its periodic refresh (at least every T_pin)
        let f = open_follower(&store).await?;
        let fresh = known.at.elapsed() <= T_PIN && known.v.x >= nv.x;
        let f2m = missing(&model, &merged!(f, &known.v));
        // Control: a follower that stopped refreshing; the rule refuses it. Had it read, the new keys are gone.
        let stale_refused = stale.at.elapsed() > T_PIN;
        let stale_m = missing(&model, &merged!(f, &stale.v));
        f.close().await?;
        say(format!(
            "FOLLOW a DbReader in another process: newest SlateDB view with an older known version, before the lease ends (view newer {view_newer}; drop floor held at or below the known X {floor_held}; known version refreshed {age_ms} ms ago): misses {f1m} keys; after the lease and the drop (SlateDB rows <= X left {dropped}), with its refreshed version: misses {f2m}; a follower that stopped refreshing is refused {stale_refused} (control: had it read, it would miss {stale_m}): {}",
            verdict(view_newer && floor_held && f1m == 0 && fresh && f2m == 0 && dropped == 0 && stale_refused && stale_m > 0)
        ));
        db.close().await?;
    }

    // ---------- H: fail point between "cut set in memory" and "publish refused" ----------
    let (hm, wx, lx, refused) = scenario_h(&store, "h", false).await?;
    say(format!(
        "H fail point: the loser set its cut X = {lx} in memory and its publish was refused {refused}; the winner published X = {wx}; the drop reads the floor from the published manifest: the winner's read misses {hm} of 200 keys: {}",
        verdict(refused && hm == 0)
    ));
    let (hm0, _, _, _) = scenario_h(&store, "h0", true).await?;
    say(format!(
        "H control (the drop trusts the cut in memory, probe 2's AtomicU64): the winner's read misses {hm0} of 200 keys: {}",
        if hm0 > 0 { "reproduces the failure, as expected" } else { "DOES NOT REPRODUCE" }
    ));

    // ---------- F: checkpoint paired with a version under the drop lock; fork, export, PITR diff ----------
    {
        let ctl = Ctl::new(&store, "p");
        let db = open_with_protocol(&ctl).await?;
        publish(&ctl, Version::default()).await?;
        let mut model = Model::new();
        load(&db, &mut model, 1..2, "p1").await?;
        db.put(&key(1, 5), &Bytes::from_static(&[DEAD])).await?; // an ordinary delete, as an envelope
        model.remove(&key(1, 5));
        flush(&db).await?;
        let (v1, ids) = absorb(&db, &ctl, &ctl.current()).await?;
        publish(&ctl, v1).await?;
        lease().await;
        fold(&db, &ctl, &ids).await?; // rows <= X of v1 leave SlateDB before the checkpoint
        let model_cp = model.clone();
        // P4: under the drop lock, read the current version, take the checkpoint, record the pair.
        let (cp, paired) = {
            let _g = ctl.drop_lock.lock().await;
            let paired = ctl.current();
            let cp = Admin::builder("p", store.clone()).build().create_detached_checkpoint(&CheckpointOptions::default()).await?;
            store.put(&Path::from(format!("p/fx/pairs/{}", cp.id)), PutPayload::from(paired.id.to_le_bytes().to_vec())).await?;
            (cp, paired)
        };
        let admin = Admin::builder("p", store.clone()).build();
        let cp_seq = admin.read_manifest(Some(cp.manifest_id)).await?.map(|m| m.last_l0_seq()).unwrap_or(0);
        // The parent goes on: new rows, a new version, a drop.
        load(&db, &mut model, 2..3, "p2").await?;
        let (v2, ids) = absorb(&db, &ctl, &ctl.current()).await?;
        let v2 = publish(&ctl, v2).await?;
        lease().await;
        fold(&db, &ctl, &ids).await?;

        // Export through the pairing: a DbReader on the checkpoint plus the paired version.
        let pair_id = u64::from_le_bytes(store.get(&Path::from(format!("p/fx/pairs/{}", cp.id))).await?.bytes().await?[..8].try_into()?);
        let pv = load_version(&store, "p", pair_id).await?.unwrap();
        let mut o = DbReaderOptions::default();
        o.skip_wal_replay = true;
        let past = DbReader::builder("p".to_string(), store.clone()).with_reader_mode(DbReaderMode::Checkpoint(cp.id)).with_options(o).build().await?;
        let export = merged!(past, &pv);
        let export_raw = raw!(past);
        let raw_wrong = missing(&model_cp, &export_raw.iter().filter(|(_, v)| v[0] == LIVE).map(|(k, v)| (k.clone(), v[1..].to_vec())).collect());
        say(format!(
            "F export from a checkpoint (seq {cp_seq}) paired with version {pair_id} (X = {} <= {cp_seq}): {} of {} documents differ from the model at the checkpoint, the deleted one included (control, today's raw export: {raw_wrong} wrong): {}",
            pv.x,
            missing(&model_cp, &export),
            model_cp.len(),
            verdict(pv.x <= cp_seq && missing(&model_cp, &export) == 0 && pv.id == paired.id)
        ));

        // PITR diff through the pairing: past = checkpoint + its version, live = the database + its current version.
        let p = pin(&ctl);
        let live_now = merged!(db, &p.v);
        drop(p);
        let diff = |a: &Model, b: &Model| -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
            let mut d = BTreeMap::new();
            for (k, v) in a {
                if b.get(k) != Some(v) {
                    d.insert(k.clone(), Some(v.clone()));
                }
            }
            for k in b.keys() {
                if !a.contains_key(k) {
                    d.insert(k.clone(), None);
                }
            }
            d
        };
        let want = diff(&model_cp, &model);
        let got = diff(&export, &live_now);
        let raw_live = raw!(db);
        let raw_got = diff(&export_raw, &raw_live);
        say(format!(
            "F PITR diff (restore the live database to the checkpoint) through the pairing: {} changes, the model's {}; equal {}. Control, today's raw diff: {} changes: {}",
            got.len(),
            want.len(),
            got == want,
            raw_got.len(),
            verdict(got == want)
        ));
        past.close().await?;
        db.close().await?;

        // Fork: clone from the checkpoint; the fork's first manifest is a copy of the paired version.
        let cadmin = Admin::builder("c2", store.clone()).build();
        cadmin.create_clone_builder_from_source(slatedb::CloneSourceSpec::with_checkpoint("p", cp.id)).build().await?;
        let mut fv = pv.clone();
        fv.id = 0;
        store.put(&manifest_path("c2", 0), PutPayload::from(fv.encode())).await?;
        let mut s = Settings::default();
        s.compactor_options = None;
        let clone = slatedb::Db::builder("c2", store.clone()).with_settings(s).build().await?;
        let fork_seq = clone.put(&key(3, 0), &live(b"fork")).await?.seqnum();
        let fv = load_version(&store, "c2", 0).await?.unwrap();
        let mut fork_model = model_cp.clone();
        fork_model.insert(key(3, 0), b"fork".to_vec());
        let fork = merged!(clone, &fv);
        let with_parent_newest = merged!(clone, &v2).contains_key(&key(3, 0));
        say(format!(
            "F fork from the checkpoint: the fork's first write gets seq {fork_seq}, above its paired X = {}; fork read = model at the checkpoint plus its write {} (control, paired with the parent's newest X = {}: the fork's write visible {with_parent_newest}): {}",
            fv.x,
            fork == fork_model,
            v2.x,
            verdict(fork_seq > fv.x && fork == fork_model)
        ));
        clone.close().await?;
    }

    // ---------- G: fix 4 invariants on a model (no SlateDB) ----------
    {
        type Rows = BTreeMap<Vec<u8>, (u64, u8)>; // newest row per key: (seq, LIVE or DEAD)
        fn read(sealed: &Rows, delta: &[(Vec<u8>, u64, u8)], xg: u64, k: &[u8]) -> bool {
            let mut best = sealed.get(k).copied();
            for (dk, s, t) in delta {
                if dk == k && *s > xg && best.map(|b| *s > b.0).unwrap_or(true) {
                    best = Some((*s, *t));
                }
            }
            matches!(best, Some((_, LIVE)))
        }
        let k = b"u/007/d/0001".to_vec();
        // Group B sealed at X_g = 100: k written at 50 and deleted at 70, so the sealed run holds nothing for k.
        // The delta still holds the dead value at 50 (not merged out yet). Group A has X_g = 40.
        let sealed_b = Rows::new();
        let delta = vec![(k.clone(), 50, LIVE)];
        let concat_min = read(&sealed_b, &delta, 40, &k); // concatenate A and B, X_g = min = 40
        // Rule: a group merge with different X_g rewrites both at a common cut, max(X_gA, X_gB) = 100.
        let common = read(&sealed_b, &delta, 100, &k);
        // Delta merge: sealed k@30 (X_g = 40), delta envelope k@60. A merge that drops envelopes (SlateDB's last-run rule)
        // against one that keeps every envelope above X_g.
        let mut sealed = Rows::new();
        sealed.insert(k.clone(), (30, LIVE));
        let dropped_env = read(&sealed, &[], 40, &k);
        let kept_env = read(&sealed, &[(k.clone(), 60, DEAD)], 40, &k);
        say(format!(
            "G fix 4 on a model: group merge by concatenation with the smaller X_g brings a deleted key back {concat_min}, rewrite at the common cut {common}; a delta merge that drops an envelope above X_g brings the sealed value back {dropped_env}, keeping it {kept_env}: {}",
            verdict(concat_min && !common && dropped_env && !kept_env)
        ));
    }

    say(format!("probe 3 finished in {:.0} s", t0.elapsed().as_secs_f64()));
    let out = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../results/probe-3.txt");
    std::fs::write(out, lines.join("\n") + "\n")?;
    Ok(())
}
