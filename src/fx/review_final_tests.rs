//! R-final-engine (2026-11-13): holes found by the independent review of integration-5-stack, each a deterministic test
//! that fails on the head 359f09b3 and passes once the proposed fix is in.

use super::build::{GroupWriter, PartRows, SortedRows};
use super::tests::SERIAL;
use super::*;
use crate::counting::CountingStore;
use crate::lifecycle::{Lifecycle, Op, Tenant};
use crate::runfmt::{CustomerKind, RunOptions};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

const T_PIN: u64 = 200;

struct VecRows(std::vec::IntoIter<(Vec<u8>, Vec<u8>)>);
impl SortedRows for VecRows {
    fn next_row(&mut self) -> anyhow::Result<Option<(Vec<u8>, Vec<u8>)>> {
        Ok(self.0.next().map(|(k, v)| (k, crate::runfmt::live(&v))))
    }
}

fn opts() -> build::BuildOpts {
    build::BuildOpts { group_bytes: 8 << 10, sealed: RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2), ..Default::default() }
}

fn ckey(c: usize, i: usize) -> Vec<u8> {
    let name = format!("c{c:03}");
    let mut k = b"u/".to_vec();
    k.push(name.len() as u8);
    k.extend(name.as_bytes());
    k.extend(format!("msgs/d/{i:05}").as_bytes());
    k
}

async fn begin(t: &mut Tenant) -> u64 {
    for _ in 0..50 {
        if let Some(x) = t.fx_ballast_begin().await.unwrap() {
            return x;
        }
        tokio::time::sleep(std::time::Duration::from_millis(T_PIN / 4)).await;
    }
    panic!("no envelope marker");
}

/// Hole G: the 1 TB generator's parallel publish (`build::publish_generated_parts`, merged from generator-1tb) has no
/// writer-epoch fence (`build::fence_check`), which its single-writer twin `publish_generated` and every build have since
/// fix-compactor-start. A generator process that lost the database (another writer opened it: SlateDB's writer epoch
/// moved) still creates a version. Control: the same takeover refuses `publish_generated`.
/// Returns (versions created by the old writer's parts publish, the single publish refused).
async fn generator_after_takeover(parts: bool) -> (usize, bool) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = if parts { "rf-gen-parts" } else { "rf-gen-single" };
    let lc1 = Lifecycle::new(Arc::new(CountingStore::new(Arc::clone(&store))), None);
    let mut t = lc1.create(id).await.unwrap();
    t.apply(vec![Op::Put(b"meta/collections".to_vec(), b"msgs".to_vec())]).await.unwrap();
    let x0 = begin(&mut t).await;
    let (st, gid, epoch) = t.fx_ballast_target().unwrap();
    let o = build::BuildOpts { group_bytes: u64::MAX, ..opts() };
    let mut gw = GroupWriter::new(&st, &gid, x0, epoch, &o, 1000, false);
    for c in 0..10 {
        for i in 0..20 {
            gw.add(&ckey(c, i), x0, &crate::runfmt::live(format!("doc {c} {i}").as_bytes())).await.unwrap();
        }
    }
    let direct_last = gw.last_key().map(|k| k.to_vec());
    let direct = gw.finish().await.unwrap();
    // the takeover: another process opens the database as its writer (SlateDB's writer epoch moves) and writes
    let lc2 = Lifecycle::new(Arc::new(CountingStore::new(Arc::clone(&store))), None);
    let mut t2 = lc2.wake(id).await.unwrap();
    t2.apply(vec![Op::Put(b"zz/after-takeover".to_vec(), b"acked".to_vec())]).await.unwrap();
    let before = manifest::list_versions(&store, id).await.unwrap().len();
    let r = if parts {
        let shared = vec![(b"doc/plain/1".to_vec(), b"p".to_vec())];
        let p = vec![PartRows { lo: Vec::new(), rows: Box::new(VecRows(shared.into_iter())) }, PartRows { lo: b"u/".to_vec(), rows: Box::new(VecRows(Vec::new().into_iter())) }];
        t.fx_publish_generated_parts(&opts(), x0, direct, direct_last, p, 2).await.map(|_| ())
    } else {
        let gen = build::Generated { x0, direct, direct_last, sorted: Box::new(VecRows(vec![(b"doc/plain/1".to_vec(), b"p".to_vec())].into_iter())) };
        t.fx_publish_generated(&opts(), gen).await.map(|_| ())
    };
    let after = manifest::list_versions(&store, id).await.unwrap().len();
    t2.abandon().await;
    t.abandon().await;
    (after - before, r.is_err())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf_generator_parts_publish_is_fenced_by_the_writer_epoch() {
    let _s = SERIAL.lock().await;
    ENABLED.store(true, Relaxed);
    PUBLISH_EVERY.store(0, Relaxed);
    T_PIN_MS.store(T_PIN, Relaxed);
    let (single_created, single_refused) = generator_after_takeover(false).await;
    let (parts_created, parts_refused) = generator_after_takeover(true).await;
    ENABLED.store(false, Relaxed);
    eprintln!("rf generator: single publish created {single_created} refused {single_refused}; parts publish created {parts_created} refused {parts_refused}");
    assert!(single_refused && single_created == 0, "control: the single publish must be refused by the fence (created {single_created})");
    assert!(parts_refused, "the parts publish of a writer that lost the database must be refused");
    assert_eq!(parts_created, 0, "the parts publish of a writer that lost the database created a version");
}

// ---------------------------------------------------------------------------------------------------------------------
// Hole C: the close-time compaction gate trusts the plain newest version, the readers the trusted one.

const HOLD_MS: u64 = 3000;

fn gate_knobs() -> compactor::Knobs {
    compactor::Knobs {
        theta: 0.25,
        r3: false,
        r3_gets_per_gib: 470.0,
        r2_age: None,
        tier_t: 4,
        tier_unit: 2 << 10,
        absorb_min_l0: 1,
        absorb_max_l0: 8,
        local_max_bytes: 0,
        split_bytes: 40 << 10,
        merge_bytes: 18 << 10,
        min_seal_bytes: 8 << 10,
        sealed: RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2),
        delta: RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2),
        drop: false,
        fold_one_run: false,
        fold_max_runs: 6,
        fold_at_l0: 6,
        lease: std::time::Duration::from_millis(400),
        remote_lease: std::time::Duration::from_millis(HOLD_MS + 600),
        serving_lease: std::time::Duration::from_millis(300),
        put_timeout: std::time::Duration::from_millis(250),
        gc_grace: std::time::Duration::from_millis(HOLD_MS + 800),
        version_retention: std::time::Duration::from_millis(0),
        gc_every: std::time::Duration::from_secs(3600),
        tick: std::time::Duration::from_millis(10),
        reseal_parallel: 1,
        read_ahead: 64 << 10,
        derive: None,
        recognise_in_background: false,
    }
}

/// The compactor's start as the server does it (`dbs.rs` `maybe_start_fxc`): the builder catches up first when marked.
async fn gate_start(t: &mut Tenant, store: &Arc<dyn ObjectStore>, k: &compactor::Knobs) -> Arc<compactor::Compactor> {
    if manifest::needs_catch_up(store, t.id()).await.unwrap() {
        let scan_ms = manifest::now_ms();
        let mut o = t.fx_auto_opts();
        o.reseal = build::Reseal::None;
        let _ = t.fx_publish(&o, None).await.unwrap();
        manifest::note_caught_up(store, t.id(), scan_ms).await.unwrap();
    }
    let c = compactor::Compactor::start(t, Arc::clone(store), k.clone(), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    c.run();
    c
}

async fn gate_flush(t: &Tenant) {
    let db = t.fx_writer_db().unwrap();
    db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
}

/// A database with compactors on is closed with L0 SSTs its compactor never absorbed (the gap: 800 acknowledged rows).
/// Then an older writer's version lands late at the next slot (lower writer epoch, X_d up to the L0's last seq: the
/// write-stalls tests plant it the same way). Readers and the next compactor pass over it (`trusted_at`); the
/// close-time compaction pass (`lifecycle::compact_tenant_pass_with`) gates on `load_newest`, takes that X_d, and folds
/// the unabsorbed L0 into a sorted run. After the reopen the compactor absorbs past the gap rows, which are then read
/// from neither side. `late` false is the control: no late version, the gate holds, nothing is lost.
/// Returns (L0 at the close, L0 after the pass, gap rows read wrong, gap rows).
async fn gate_scenario(late: bool) -> (usize, usize, usize, usize) {
    compactor::SERVER_ON.store(true, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = if late { "rf-gate-late" } else { "rf-gate-control" };
    let mut t = super::tests::lc(&store).create(id).await.unwrap();
    let mut m = std::collections::BTreeMap::new();
    for i in 0..100 {
        super::tests::apply(&mut t, &mut m, vec![(super::tests::ckey(0, i % 40), Some(vec![b'a'; 40]))]).await;
    }
    super::tests::publish(&mut t, &super::tests::small_opts(build::Reseal::All)).await;
    let mut k = gate_knobs();
    k.absorb_min_l0 = 1000; // nothing is absorbed before the close
    let c = gate_start(&mut t, &store, &k).await;
    c.paused.store(true, Relaxed);
    for r in 0..10 {
        for i in 0..80 {
            super::tests::apply(&mut t, &mut m, vec![(super::tests::ckey(20 + r, i), Some(format!("gap-{r}-{i}").into_bytes()))]).await;
        }
        gate_flush(&t).await;
    }
    let l0_at_close = t.writer_l0().unwrap();
    c.stop().await;
    t.close().await.unwrap();
    if late {
        // an older writer's version lands late at the next slot (as `write_stalls_tests::older_version_scenario` plants it)
        let cur = manifest::load_newest(&store, id).await.unwrap().unwrap();
        let admin = slatedb::admin::Admin::builder(crate::lifecycle::tenant_path(id), Arc::clone(&store)).build();
        let last_l0 = admin.read_manifest(None).await.unwrap().unwrap().last_l0_seq();
        let mut stale = cur.clone();
        stale.version = cur.version + 1;
        stale.kind = "publish".into();
        stale.writer_epoch = Some(cur.writer_epoch.unwrap_or(1) - 1);
        stale.x_d = last_l0;
        assert!(stale.x_d > cur.x_d, "setup: the late version's X_d must pass the trusted one");
        assert!(manifest::create(&store, id, &stale).await.unwrap());
        let trusted = manifest::load_newest_trusted(&store, id).await.unwrap().unwrap().0;
        assert_eq!(trusted.version, cur.version, "setup: readers must pass over the late version");
    }
    // the close-time pass, as the maintenance queue runs it
    let _ = crate::lifecycle::compact_tenant(&store, &store, id).await.unwrap();
    let l0_after_pass = crate::lifecycle::l0_count(&store, id).await.unwrap();
    // reopen: new rows, absorbed, then every gap key read
    let mut t = super::tests::lc(&store).wake(id).await.unwrap();
    t.upgrade().await.unwrap();
    k.absorb_min_l0 = 1;
    let c = gate_start(&mut t, &store, &k).await;
    for i in 0..200 {
        super::tests::apply(&mut t, &mut m, vec![(super::tests::ckey(30, i % 70), Some(format!("late-{i}").into_bytes()))]).await;
    }
    gate_flush(&t).await;
    c.drain(false).await.unwrap();
    t.fx_sync();
    let gap_keys: Vec<Vec<u8>> = m.keys().filter(|key| (20..30).any(|r| key.starts_with(&super::tests::ckey(r, 0)[..super::tests::ckey(r, 0).len() - 5]))).cloned().collect();
    let mut gap_wrong = 0;
    for key in &gap_keys {
        if t.get(key).await.unwrap().as_deref() != m.get(key).map(|v| &v[..]) {
            gap_wrong += 1;
        }
    }
    eprintln!("rf gate (late {late}): L0 at the close {l0_at_close}, after the pass {l0_after_pass}; gap rows {} wrong {gap_wrong}", gap_keys.len());
    c.stop().await;
    t.close().await.unwrap();
    compactor::SERVER_ON.store(false, Relaxed);
    (l0_at_close, l0_after_pass, gap_wrong, gap_keys.len())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf_close_pass_gate_uses_the_trusted_version() {
    let _s = SERIAL.lock().await;
    super::tests::enable();
    hold::HOLD_LIFE_MS.store(HOLD_MS, Relaxed);
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(3 << 10, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(64, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(1, Relaxed);
    view::clear_cache();
    let control = gate_scenario(false).await;
    let late = gate_scenario(true).await;
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(30, Relaxed);
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
    assert!(control.0 >= crate::lifecycle::L0_COMPACT_THRESHOLD, "setup: L0 at the close {}", control.0);
    assert_eq!(control.3, 800, "setup: 800 gap rows");
    assert_eq!(control.2, 0, "control: without a late version nothing may be lost");
    assert_eq!(late.1, late.0, "the pass moved L0 SSTs above the trusted X_d ({} -> {})", late.0, late.1);
    assert_eq!(late.2, 0, "{} of {} acknowledged gap rows read wrong after the pass gated on an older writer's late version", late.2, late.3);
}
