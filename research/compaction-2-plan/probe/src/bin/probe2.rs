//! Review of the compaction 2 plan, probe 2 (fenvix-brain `prompts/R-review-compaction-2-plan.md`, questions 1 and 2).
//!
//! Probe 1 checked the read rule of section 2 one step at a time, with no reader alive across a step. This probe keeps a
//! reader (or a transaction) alive across the steps, as the server does, and checks sequence numbers across restart,
//! writer change and clone. Each check prints PASS (the plan holds) or BREAKS (a sequence that returns a wrong or missing
//! row under the read rule as written). Same SlateDB 0.17.0 public API, in-memory store, no network.
//!
//!   A. A reader pins manifest version v0 (X = 0), then the compactor publishes v1 (X) and SlateDB runs the drop job, then
//!      the reader reads SlateDB. Section 6 drops right after publish, with no wait for pinned readers.
//!   B. A transaction snapshot (Tenant::snapshot, used by every transaction) taken before the drop: SlateDB documents
//!      that compaction filters break snapshots, and the drop job is a compaction filter.
//!   C. The read rule caps SlateDB rows at S but not delta or sealed rows: a snapshot at S reads with a newer manifest
//!      whose X is above S.
//!   D. Every drop job outputs a new sorted run: do empty runs pile up in SlateDB's manifest?
//!   E. Sequence numbers across restart and writer change (expected to hold).
//!   F. Sequence numbers in a clone (fork, PITR staging): the clone continues from the checkpoint, below the parent's.
//!
//! Run: `cargo run --release --bin probe2`. Result: `../results/probe-2.txt`.

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use object_store::ObjectStore;
use slatedb::admin::Admin;
use slatedb::compactor::{CompactionScheduler, CompactionSchedulerSupplier, CompactionSpec, CompactorStateView, SourceId};
use slatedb::config::{CheckpointOptions, CompactorOptions, FlushOptions, FlushType, Settings};
use slatedb::{CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier, CompactionJobContext, RowEntry, ValueDeletable};
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const LIVE: u8 = 0;

#[derive(Default)]
struct Absorbed {
    x: AtomicU64,
    views: Mutex<HashSet<ulid::Ulid>>,
    jobs: AtomicU64,
    /// D2: fold the drop into the existing sorted run (same id) instead of a new one.
    reuse: std::sync::atomic::AtomicBool,
}

#[derive(Clone)]
struct Drop_(Arc<Absorbed>);

impl CompactionSchedulerSupplier for Drop_ {
    fn compaction_scheduler(&self, _o: &CompactorOptions) -> Box<dyn CompactionScheduler + Send + Sync> {
        Box::new(self.clone())
    }
}

impl CompactionScheduler for Drop_ {
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        if let Some(c) = state.compactions() {
            if c.recent_compactions().any(|c| c.active()) {
                return vec![];
            }
        }
        let m = state.manifest();
        let absorbed = self.0.views.lock().unwrap().clone();
        let src: Vec<SourceId> = m.l0().iter().filter(|v| absorbed.contains(&v.id)).map(|v| SourceId::SstView(v.id)).collect();
        if src.is_empty() {
            return vec![];
        }
        self.0.jobs.fetch_add(1, Ordering::Relaxed);
        if self.0.reuse.load(Ordering::SeqCst) {
            if let Some(sr) = m.compacted().iter().max_by_key(|r| r.id) {
                let mut src = src;
                src.push(SourceId::SortedRun(sr.id));
                return vec![CompactionSpec::new(src, sr.id)];
            }
        }
        let next = m.compacted().iter().map(|r| r.id + 1).max().unwrap_or(0);
        vec![CompactionSpec::new(src, next)]
    }
}

struct DropSealed(Arc<Absorbed>);

#[async_trait]
impl CompactionFilter for DropSealed {
    async fn filter(&mut self, entry: &RowEntry) -> Result<CompactionFilterDecision, CompactionFilterError> {
        Ok(if entry.seq <= self.0.x.load(Ordering::SeqCst) { CompactionFilterDecision::Drop } else { CompactionFilterDecision::Keep })
    }
    async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
        Ok(())
    }
}

#[async_trait]
impl CompactionFilterSupplier for Drop_ {
    async fn create_compaction_filter(&self, _c: &CompactionJobContext) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
        Ok(Box::new(DropSealed(self.0.clone())))
    }
}

fn live(v: &[u8]) -> Bytes {
    let mut b = vec![LIVE];
    b.extend_from_slice(v);
    Bytes::from(b)
}

fn key(c: u32, i: u32) -> Vec<u8> {
    format!("u/{c:03}/d/{i:04}").into_bytes()
}

/// Our stand-in manifest version: cut X and the sealed rows (newest row <= X of every key).
#[derive(Clone, Default)]
struct Version {
    x: u64,
    sealed: BTreeMap<Vec<u8>, (u64, Bytes)>,
}

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn finish(out: BTreeMap<Vec<u8>, (u64, Bytes)>) -> Model {
    out.into_iter().filter(|(_, (_, v))| v[0] == LIVE).map(|(k, (_, v))| (k, v[1..].to_vec())).collect()
}

/// Section 2's rule on a plain read: SlateDB rows with seq > X, else sealed rows.
async fn read_rule(db: &slatedb::Db, v: &Version) -> Result<Model> {
    let mut out = v.sealed.clone();
    let mut it = db.scan(b"u/".to_vec()..).await?;
    while let Some(kv) = it.next().await? {
        if kv.seq > v.x {
            out.insert(kv.key.to_vec(), (kv.seq, kv.value.clone()));
        }
    }
    Ok(finish(out))
}

/// The same rule on a snapshot (a transaction): SlateDB rows come from the snapshot (seq <= S).
async fn read_rule_snap(s: &slatedb::DbSnapshot, v: &Version) -> Result<Model> {
    let mut out = v.sealed.clone();
    let mut it = s.scan(b"u/".to_vec()..).await?;
    while let Some(kv) = it.next().await? {
        if kv.seq > v.x {
            out.insert(kv.key.to_vec(), (kv.seq, kv.value.clone()));
        }
    }
    Ok(finish(out))
}

async fn flush(db: &slatedb::Db) -> Result<()> {
    db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await?;
    Ok(())
}

/// Absorb every L0 SST now in SlateDB's manifest into a new version (what our compactor does), without dropping yet.
async fn absorb(db: &slatedb::Db, store: &Arc<dyn ObjectStore>, path: &str, prev: &Version) -> Result<(Version, Vec<ulid::Ulid>)> {
    let m = db.manifest();
    let views: Vec<_> = m.l0().iter().cloned().collect();
    let reader = slatedb::SstReader::new(path, store.clone(), None, None);
    let mut v = prev.clone();
    for view in &views {
        let f = reader.open_with_handle(view.sst.clone())?;
        for b in 0..f.index().await?.len() {
            for r in f.read_block(b).await? {
                if r.seq <= prev.x {
                    continue;
                }
                let val = match &r.value {
                    ValueDeletable::Value(x) => x.clone(),
                    _ => Bytes::from_static(&[1]),
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

/// Tell the compactor to drop rows <= x of these L0 SSTs and wait until they leave SlateDB's manifest.
async fn drop_absorbed(db: &slatedb::Db, abs: &Absorbed, x: u64, ids: &[ulid::Ulid]) -> Result<()> {
    abs.x.store(x, Ordering::SeqCst);
    abs.views.lock().unwrap().extend(ids.iter().copied());
    for _ in 0..400 {
        if !db.manifest().l0().iter().any(|v| ids.contains(&v.id)) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    anyhow::bail!("drop job never ran")
}

async fn open_with_drop(store: &Arc<dyn ObjectStore>, path: &str, abs: &Arc<Absorbed>) -> Result<slatedb::Db> {
    let sup = Drop_(abs.clone());
    let mut s = Settings::default();
    s.compactor_options = None;
    s.l0_max_ssts = 256;
    let mut co = CompactorOptions::default();
    co.poll_interval = Duration::from_millis(20);
    let cb = slatedb::CompactorBuilder::new(path, store.clone())
        .with_options(co)
        .with_scheduler_supplier(Arc::new(sup.clone()))
        .with_compaction_filter_supplier(Arc::new(sup));
    Ok(slatedb::Db::builder(path, store.clone()).with_settings(s).with_compactor_builder(cb).build().await?)
}

fn verdict(ok: bool) -> &'static str {
    if ok {
        "PASS"
    } else {
        "BREAKS"
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut lines: Vec<String> = Vec::new();
    let mut say = |s: String| {
        println!("{s}");
        lines.push(s);
    };

    // ---------- A, B, C on one database ----------
    {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let abs = Arc::new(Absorbed::default());
        let db = open_with_drop(&store, "a", &abs).await?;
        let mut model = Model::new();
        for c in 0..10 {
            for i in 0..100 {
                let v = format!("r1-{c}-{i}");
                db.put(&key(c, i), &live(v.as_bytes())).await?;
                model.insert(key(c, i), v.into_bytes());
            }
        }
        flush(&db).await?;

        // A: reader pins v0 (nothing sealed). B: a transaction snapshot taken at the same moment.
        let v0 = Version::default();
        let snap = db.snapshot().await?;
        let s0 = snap.seq();
        let a_before = read_rule(&db, &v0).await? == model;
        // The compactor absorbs, publishes v1, and SlateDB drops (section 6, steps 2 then 3, no wait).
        let (v1, ids) = absorb(&db, &store, "a", &v0).await?;
        drop_absorbed(&db, &abs, v1.x, &ids).await?;
        let a_after = read_rule(&db, &v0).await?;
        let a_missing = model.keys().filter(|k| !a_after.contains_key(*k)).count();
        say(format!(
            "A pinned reader across publish + drop: before the drop = model {a_before}; after, the reader still on v0 (X = 0) sees {} of {} keys, {a_missing} missing: {}",
            a_after.len(),
            model.len(),
            verdict(a_missing == 0)
        ));
        // With v1 the same read is right (the plan's rule holds for a reader that re-pins).
        let a_v1 = read_rule(&db, &v1).await? == model;
        say(format!("A control: the same read with v1 (X = {}) = model {a_v1}", v1.x));

        // B: the transaction snapshot taken before the drop, read with the version it pinned (v0).
        let b0 = read_rule_snap(&snap, &v0).await?;
        let b_missing = model.keys().filter(|k| !b0.contains_key(*k)).count();
        // And read with v1 (the transaction re-reads the newest manifest).
        let b1 = read_rule_snap(&snap, &v1).await? == model;
        say(format!(
            "B transaction snapshot (seq {s0}) across the drop: with its pinned v0 it sees {} of {} keys, {b_missing} missing: {}; with v1 = model {b1}",
            b0.len(),
            model.len(),
            verdict(b_missing == 0)
        ));

        // C: snapshot at S, then newer writes are flushed, absorbed and published with X above S.
        let snap2 = db.snapshot().await?;
        let s2 = snap2.seq();
        let at_s2 = model.clone();
        for i in 0..100 {
            let v = format!("r2-0-{i}");
            db.put(&key(0, i), &live(v.as_bytes())).await?;
            model.insert(key(0, i), v.into_bytes());
        }
        flush(&db).await?;
        let (v2, ids2) = absorb(&db, &store, "a", &v1).await?;
        drop_absorbed(&db, &abs, v2.x, &ids2).await?;
        let c_read = read_rule_snap(&snap2, &v2).await?;
        let c_future = at_s2.iter().filter(|(k, v)| c_read.get(*k) != Some(*v)).count();
        say(format!(
            "C snapshot at S = {s2}, manifest v2 with X = {} > S: {c_future} of {} keys return a value written after S (a transaction reads the future): {}",
            v2.x,
            at_s2.len(),
            verdict(c_future == 0)
        ));
        drop(snap);
        drop(snap2);

        // D: many absorb + drop rounds; count sorted runs in SlateDB's manifest.
        let srs_before = db.manifest().compacted().len();
        let mut v = v2;
        let rounds = 30;
        for r in 0..rounds {
            for i in 0..10 {
                db.put(&key(50 + r, i), &live(b"d")).await?;
            }
            flush(&db).await?;
            let (nv, ids) = absorb(&db, &store, "a", &v).await?;
            drop_absorbed(&db, &abs, nv.x, &ids).await?;
            v = nv;
        }
        let srs_after = db.manifest().compacted().len();
        let empty = db.manifest().compacted().iter().filter(|sr| sr.sst_views().is_empty()).count();
        say(format!(
            "D {rounds} absorb + drop rounds: SlateDB sorted runs {srs_before} -> {srs_after} ({empty} with no SST), drop jobs {}: {}",
            abs.jobs.load(Ordering::Relaxed),
            verdict(srs_after <= srs_before + 1)
        ));
        // D2: the same rounds with the drop folded into the newest sorted run (sources = absorbed L0s + that run).
        abs.reuse.store(true, Ordering::SeqCst);
        let before2 = db.manifest().compacted().len();
        for r in 0..rounds {
            for i in 0..10 {
                db.put(&key(100 + r, i), &live(b"d2")).await?;
            }
            flush(&db).await?;
            let (nv, ids) = absorb(&db, &store, "a", &v).await?;
            if std::env::var("P2_DEBUG").is_ok() { eprintln!("D2 round {r}: srs {} l0 {}", db.manifest().compacted().len(), db.manifest().l0().len()); }
            drop_absorbed(&db, &abs, nv.x, &ids).await?;
            v = nv;
        }
        let after2 = db.manifest().compacted().len();
        let mut full = model.clone();
        for r in 0..rounds {
            for i in 0..10 {
                full.insert(key(50 + r, i), b"d".to_vec());
                full.insert(key(100 + r, i), b"d2".to_vec());
            }
        }
        let d2_ok = read_rule(&db, &v).await? == full;
        say(format!(
            "D2 {rounds} more rounds folding each drop into the newest sorted run: sorted runs {before2} -> {after2}; merged read = model {d2_ok}: {}",
            verdict(after2 <= before2 && d2_ok)
        ));
        db.close().await?;
    }

    // ---------- E: restart and writer change ----------
    {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let mut s = Settings::default();
        s.compactor_options = None;
        let db = slatedb::Db::builder("e", store.clone()).with_settings(s.clone()).build().await?;
        let mut last = 0;
        for i in 0..50 {
            last = db.put(&key(1, i), &live(b"e")).await?.seqnum();
        }
        flush(&db).await?;
        for i in 50..60 {
            let h = db.put(&key(1, i), &live(b"e")).await?; // in WAL only
            h.await_durable().await?;
            last = h.seqnum();
        }
        db.close().await?;
        let db = slatedb::Db::builder("e", store.clone()).with_settings(s.clone()).build().await?;
        let h = db.put(&key(2, 0), &live(b"e")).await?;
        h.await_durable().await?; // the engine acknowledges only durable writes
        let after_restart = h.seqnum();
        // Writer change: a second writer opens without the first closing (fences it).
        let db2 = slatedb::Db::builder("e", store.clone()).with_settings(s.clone()).build().await?;
        let after_fence = db2.put(&key(2, 1), &live(b"e")).await?.seqnum();
        let old_put = db.put(&key(2, 2), &live(b"old")).await;
        let old_seq = old_put.as_ref().map(|h| h.seqnum()).ok();
        let old_fenced = old_put.is_err();
        let old_flush = flush(&db).await.is_err();
        let new_put = db2.put(&key(2, 3), &live(b"new")).await?.seqnum();
        db2.close().await?;
        let db3 = slatedb::Db::builder("e", store.clone()).with_settings(s.clone()).build().await?;
        let old_survived = db3.get(&key(2, 2)).await?.is_some();
        db3.close().await?;
        say(format!(
            "E detail: the fenced writer's put returned {} (seq {old_seq:?}); its flush failed {old_flush}; the new writer's next seq {new_put}; the old writer's row present after reopen: {old_survived}",
            if old_fenced { "an error" } else { "Ok" }
        ));
        let old_fenced = old_fenced || (old_flush && !old_survived);
        say(format!(
            "E restart: last seq before {last}, first after restart {after_restart}; writer change: first seq of the new writer {after_fence}, old writer fenced {old_fenced}: {}",
            verdict(after_restart > last && after_fence > after_restart && old_fenced)
        ));
    }

    // ---------- F: clone from a checkpoint (fork, PITR staging) ----------
    {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let mut s = Settings::default();
        s.compactor_options = None;
        let db = slatedb::Db::builder("p", store.clone()).with_settings(s.clone()).build().await?;
        for i in 0..100 {
            db.put(&key(1, i), &live(b"p1")).await?;
        }
        flush(&db).await?;
        let admin = Admin::builder("p", store.clone()).build();
        let cp = admin.create_detached_checkpoint(&CheckpointOptions::default()).await?;
        let cp_seq = admin.read_manifest(Some(cp.manifest_id)).await?.map(|m| m.last_l0_seq()).unwrap_or(0);
        let mut parent_last = 0;
        for i in 0..100 {
            parent_last = db.put(&key(2, i), &live(b"p2")).await?.seqnum();
        }
        flush(&db).await?;
        db.close().await?;
        let cadmin = Admin::builder("c", store.clone()).build();
        cadmin.create_clone_builder_from_source(slatedb::CloneSourceSpec::with_checkpoint("p", cp.id)).build().await?;
        let clone = slatedb::Db::builder("c", store.clone()).with_settings(s.clone()).build().await?;
        let fork_first = clone.put(&key(3, 0), &live(b"fork")).await?.seqnum();
        // If the fork were paired with the parent's newest manifest (X_d = parent_last), the rule hides the fork's write.
        let v = Version { x: parent_last, sealed: BTreeMap::new() };
        let seen = read_rule(&clone, &v).await?.contains_key(&key(3, 0));
        say(format!(
            "F clone from a checkpoint at seq {cp_seq}: the parent went on to seq {parent_last}; the clone's first write gets seq {fork_first}. Paired with the parent's newest cut (X_d = {parent_last}) the fork's own write is visible: {seen}: {}",
            verdict(fork_first > parent_last || seen)
        ));
        clone.close().await?;
    }

    let out = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../results/probe-2.txt");
    std::fs::write(out, lines.join("\n") + "\n")?;
    Ok(())
}
