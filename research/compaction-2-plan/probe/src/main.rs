//! Compaction 2 plan, probe 1 (fenvix-brain `prompts/P-compaction-2-plan.md`, question 1, way (b)).
//!
//! Question: can SlateDB 0.17 stay the write path (memtable, WAL, L0 flush) while compacted data moves into our own
//! sealed runs, using only SlateDB's public API and no fork? Way (b) needs four things from SlateDB:
//!   1. read every row of an L0 SST raw, with its sequence number (our compactor consumes L0 directly);
//!   2. a merged read that returns each key's sequence number, so a read can take SlateDB's rows above a cut X and our
//!      sealed rows at or below X (no row counted twice, none missing);
//!   3. a way to remove the absorbed L0 SSTs from SlateDB's manifest after our manifest has them (a compaction job whose
//!      filter drops every row at or below X: its output is empty);
//!   4. deletes that survive (2): SlateDB's merged iterator hides tombstones, so a delete of a sealed key must be visible
//!      to the merge. The probe writes deletes as an envelope value (first byte 1 = deleted, 0 = live value), never as a
//!      SlateDB tombstone, and checks that a real SlateDB tombstone would have been invisible.
//! Plus 5: the conditional create (`PutMode::Create`, S3 `If-None-Match: *`) that the plan uses to publish manifest
//! versions, against the in-memory store.
//!
//! Every read is compared with a model (a BTreeMap of the latest value of each key). Run:
//! `cargo run --release` in this folder. Exit code 0 and "PROBE PASS" on success.

use anyhow::{anyhow, ensure, Result};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use slatedb::compactor::{CompactionScheduler, CompactionSchedulerSupplier, CompactionSpec, CompactorStateView, SourceId};
use slatedb::config::{CompactorOptions, FlushOptions, FlushType, Settings};
use slatedb::{CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier, CompactionJobContext, RowEntry, ValueDeletable};
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const PATH: &str = "db";
const LIVE: u8 = 0;
const DEAD: u8 = 1;

/// Shared between the test and the compactor: the cut X and the L0 views our "sealed run" absorbed.
#[derive(Default)]
struct Absorbed {
    x: AtomicU64,
    views: Mutex<HashSet<ulid::Ulid>>,
    jobs_proposed: AtomicU64,
    rows_dropped: AtomicU64,
    rows_kept: AtomicU64,
}

#[derive(Clone)]
struct Drop_(Arc<Absorbed>);

impl CompactionSchedulerSupplier for Drop_ {
    fn compaction_scheduler(&self, _o: &CompactorOptions) -> Box<dyn CompactionScheduler + Send + Sync> {
        Box::new(self.clone())
    }
}

impl CompactionScheduler for Drop_ {
    /// Propose one job: the absorbed L0 views (they are the oldest L0 SSTs) into a new sorted run.
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
        let next = m.compacted().iter().map(|r| r.id + 1).max().unwrap_or(0);
        self.0.jobs_proposed.fetch_add(1, Ordering::Relaxed);
        vec![CompactionSpec::new(src, next)]
    }
}

struct DropSealed(Arc<Absorbed>);

#[async_trait]
impl CompactionFilter for DropSealed {
    async fn filter(&mut self, entry: &RowEntry) -> Result<CompactionFilterDecision, CompactionFilterError> {
        if entry.seq <= self.0.x.load(Ordering::SeqCst) {
            self.0.rows_dropped.fetch_add(1, Ordering::Relaxed);
            Ok(CompactionFilterDecision::Drop)
        } else {
            self.0.rows_kept.fetch_add(1, Ordering::Relaxed);
            Ok(CompactionFilterDecision::Keep)
        }
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

fn dead() -> Bytes {
    Bytes::from_static(&[DEAD])
}

fn key(c: u32, i: u32) -> Vec<u8> {
    format!("u/{c:03}/d/{i:04}").into_bytes()
}

/// Our stand-in sealed run: the newest row at or below X of every key, as read raw from the absorbed L0 SSTs.
type Sealed = BTreeMap<Vec<u8>, (u64, Bytes)>;

/// The merged read at one position: SlateDB rows with seq > X, else our sealed rows; envelope deletes removed.
async fn merged(db: &slatedb::Db, sealed: &Sealed, x: u64) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    let mut out: BTreeMap<Vec<u8>, (u64, Bytes)> = sealed.clone();
    let mut it = db.scan(b"u/".to_vec()..).await?;
    while let Some(kv) = it.next().await? {
        if kv.seq > x {
            out.insert(kv.key.to_vec(), (kv.seq, kv.value.clone()));
        }
    }
    Ok(out.into_iter().filter(|(_, (_, v))| v[0] == LIVE).map(|(k, (_, v))| (k, v[1..].to_vec())).collect())
}

async fn flush(db: &slatedb::Db) -> Result<()> {
    db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let abs = Arc::new(Absorbed::default());
    let sup = Drop_(abs.clone());

    let mut s = Settings::default();
    s.compactor_options = None;
    s.l0_sst_size_bytes = 1 << 20;
    s.l0_max_ssts = 64;
    let mut co = CompactorOptions::default();
    co.poll_interval = Duration::from_millis(50);
    let cb = slatedb::CompactorBuilder::new(PATH, store.clone())
        .with_options(co)
        .with_scheduler_supplier(Arc::new(sup.clone()))
        .with_compaction_filter_supplier(Arc::new(sup.clone()));
    let db = slatedb::Db::builder(PATH, store.clone()).with_settings(s).with_compactor_builder(cb).build().await?;
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    // Round 1: 20 customers x 50 documents, then L0 #1.
    for c in 0..20 {
        for i in 0..50 {
            let v = format!("r1-{c}-{i}");
            db.put(&key(c, i), &live(v.as_bytes())).await?;
            model.insert(key(c, i), v.into_bytes());
        }
    }
    flush(&db).await?;
    // Round 2: overwrite customer 3, delete customer 5 (envelope), one real SlateDB tombstone on customer 7, then L0 #2.
    for i in 0..50 {
        let v = format!("r2-3-{i}");
        db.put(&key(3, i), &live(v.as_bytes())).await?;
        model.insert(key(3, i), v.into_bytes());
        db.put(&key(5, i), &dead()).await?;
        model.remove(&key(5, i));
    }
    db.delete(&key(7, 0)).await?;
    model.remove(&key(7, 0));
    flush(&db).await?;

    // Point 4 control: SlateDB's merged scan hides the real tombstone (nothing tells our merge that 7/0 is gone).
    let mut seen_7_0 = false;
    let mut it = db.scan(b"u/".to_vec()..).await?;
    while let Some(kv) = it.next().await? {
        if kv.key.as_ref() == key(7, 0).as_slice() {
            seen_7_0 = true;
        }
    }
    ensure!(!seen_7_0, "a SlateDB tombstone was visible in the merged scan");

    // Point 1: read both L0 SSTs raw (public SstReader), with sequence numbers and tombstones.
    let m = db.manifest();
    let views: Vec<_> = m.l0().iter().cloned().collect();
    ensure!(views.len() == 2, "expected 2 L0 SSTs, found {}", views.len());
    let reader = slatedb::SstReader::new(PATH, store.clone(), None, None);
    let mut sealed: Sealed = BTreeMap::new();
    let mut raw_rows = 0usize;
    let mut raw_tombstones = 0usize;
    let mut ranges = Vec::new();
    let mut real_tombstone_keys: Vec<Vec<u8>> = Vec::new();
    for v in &views {
        let f = reader.open_with_handle(v.sst.clone())?;
        let n = f.index().await?.len();
        let (mut lo, mut hi) = (u64::MAX, 0u64);
        for b in 0..n {
            for r in f.read_block(b).await? {
                raw_rows += 1;
                lo = lo.min(r.seq);
                hi = hi.max(r.seq);
                let val = match &r.value {
                    ValueDeletable::Value(v) => v.clone(),
                    ValueDeletable::Tombstone => {
                        raw_tombstones += 1;
                        real_tombstone_keys.push(r.key.to_vec());
                        dead() // our compactor turns a stray real tombstone into an envelope delete
                    }
                    ValueDeletable::Merge(_) => return Err(anyhow!("unexpected merge operand")),
                };
                let e = sealed.entry(r.key.to_vec()).or_insert((0, Bytes::new()));
                if r.seq >= e.0 {
                    *e = (r.seq, val);
                }
            }
        }
        ranges.push((lo, hi));
    }
    ensure!(raw_tombstones == 1, "raw tombstones seen: {raw_tombstones}");
    // L0 sequence ranges are disjoint: the cut X separates absorbed SSTs from everything newer.
    ranges.sort();
    ensure!(ranges[0].1 < ranges[1].0, "L0 seq ranges overlap: {ranges:?}");
    let x = ranges.iter().map(|r| r.1).max().unwrap();
    println!("point 1: {raw_rows} raw rows from 2 L0 SSTs, seq ranges {ranges:?}, 1 raw tombstone seen; cut X = {x}");

    // Writes after the seal: memtable rows above X (overwrite a sealed key, delete a sealed key, a new customer).
    db.put(&key(1, 1), &live(b"r3-1-1")).await?;
    model.insert(key(1, 1), b"r3-1-1".to_vec());
    db.put(&key(2, 2), &dead()).await?;
    model.remove(&key(2, 2));
    db.put(&key(30, 0), &live(b"r3-30-0")).await?;
    model.insert(key(30, 0), b"r3-30-0".to_vec());

    // Point 2, before our manifest publishes the seal: X_old = 0, nothing sealed, SlateDB alone answers.
    let before = merged(&db, &BTreeMap::new(), 0).await?;
    // The real tombstone case: SlateDB alone is right here because 7/0 lives only in SlateDB.
    ensure!(before == model, "merged read before publish differs from the model");
    // Point 2, after publish (our manifest says: sealed run holds every row <= X), SlateDB still holds them too.
    let after_publish = merged(&db, &sealed, x).await?;
    ensure!(after_publish == model, "merged read after publish differs from the model");
    // Snapshot reads also carry the sequence number.
    let snap = db.snapshot().await?;
    let mut it = snap.scan(b"u/".to_vec()..).await?;
    let mut snap_above = 0;
    while let Some(kv) = it.next().await? {
        if kv.seq > x {
            snap_above += 1;
        }
    }
    ensure!(snap_above == 3, "snapshot rows above X: {snap_above}");
    println!("point 2: merged read = model before publish ({} keys) and after publish; snapshot rows carry seq ({snap_above} above X)", model.len());

    // Point 3: SlateDB drops the absorbed L0 SSTs: a job whose filter drops rows <= X.
    abs.x.store(x, Ordering::SeqCst);
    abs.views.lock().unwrap().extend(views.iter().map(|v| v.id));
    let mut gone = false;
    for _ in 0..200 {
        let m = db.manifest();
        if !m.l0().iter().any(|v| views.iter().any(|a| a.id == v.id)) {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    ensure!(gone, "the absorbed L0 SSTs never left SlateDB's manifest");
    let after_drop = merged(&db, &sealed, x).await?;
    ensure!(after_drop == model, "merged read after the drop differs from the model");
    // SlateDB alone now holds only the 3 rows above X (and the drop job kept nothing).
    let mut slatedb_rows = 0;
    let mut it = db.scan(b"u/".to_vec()..).await?;
    while let Some(kv) = it.next().await? {
        ensure!(kv.seq > x, "a row <= X is still in SlateDB");
        slatedb_rows += 1;
    }
    let srs = db.manifest().compacted().len();
    println!(
        "point 3: absorbed L0 SSTs left SlateDB's manifest; drop jobs {}, rows dropped {}, rows kept {}; SlateDB alone holds {slatedb_rows} rows; sorted runs {srs}; merged read = model",
        abs.jobs_proposed.load(Ordering::Relaxed),
        abs.rows_dropped.load(Ordering::Relaxed),
        abs.rows_kept.load(Ordering::Relaxed)
    );
    ensure!(abs.rows_kept.load(Ordering::Relaxed) == 0, "the drop job kept rows");
    ensure!(slatedb_rows == 3, "SlateDB rows after the drop: {slatedb_rows}");

    // Restart: close and reopen (WAL replay); the merged read with the persisted cut is still the model.
    db.close().await?;
    let mut s = Settings::default();
    s.compactor_options = None;
    let db = slatedb::Db::builder(PATH, store.clone()).with_settings(s).build().await?;
    let after_restart = merged(&db, &sealed, x).await?;
    ensure!(after_restart == model, "merged read after restart differs from the model");
    let restart_ok = after_restart.len();
    db.close().await?;
    println!("restart: merged read = model ({restart_ok} keys)");

    // Point 5: conditional create, the manifest publish primitive (two publishers race for version 7, one wins).
    let p = Path::from("db/fenvix/manifest/00000000000000000007");
    let opts = || PutOptions { mode: PutMode::Create, ..Default::default() };
    let a = store.put_opts(&p, PutPayload::from_static(b"A"), opts()).await;
    let b = store.put_opts(&p, PutPayload::from_static(b"B"), opts()).await;
    ensure!(a.is_ok() && b.is_err(), "conditional create: first {:?}, second {:?}", a.is_ok(), b.is_ok());
    println!("point 5: conditional create: first publisher wins, second refused ({})", b.unwrap_err());

    let _ = real_tombstone_keys;
    println!("PROBE PASS");
    Ok(())
}
