//! Review of compaction 2 step 5b: the reader skipping SlateDB SSTs at or below X_d, and the seq and customer filters.
//!
//! - `r5b_plant_newest_cut_txn`: planted bug, the reader skips with the newest version's X_d instead of the one it
//!   holds (the drop has not run, so the rows between the two cuts are only in SlateDB). A transaction on the older
//!   version must lose them with the plant, and read right without it.
//! - `r5b_old_ssts_without_filters`: SSTs written by the release before step 5b (Bloom only, none under 1,000 keys),
//!   read after a restart by the new binary, mixed with new SSTs, then by a rolled-back binary: never skipped wrongly.
//! - `r5b_every_sst_has_the_filters`: every SST of the writer's manifest (L0 flushes, the drop's folds, after a crash and
//!   a reopen) carries the seq filter: a full scan asks it once per SST.

use super::tests::*;
use super::*;
use crate::lifecycle::Tenant;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Duration;

// the suite's lock (super::tests::SERIAL): these tests change the same globals (the NVMe store, the L0 knobs)

fn setup() {
    enable();
    hold::HOLD_LIFE_MS.store(60_000, Relaxed);
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(4 << 20, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(1, Relaxed);
    local::set_store(Some(Arc::new(InMemory::new())));
    view::clear_cache();
    seqfilter::CONTROL_EXTRA.store(0, Relaxed);
    seqfilter::PLANT_NEWEST.store(false, Relaxed);
    seqfilter::PLANT_MIN.store(false, Relaxed);
    seqfilter::NEWEST_XD.store(0, Relaxed);
    crate::lifecycle::LEGACY_FILTERS.store(false, Relaxed);
}

fn teardown() {
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(30, Relaxed);
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
    T_PIN_MS.store(TEST_T_PIN_MS, Relaxed);
    local::set_store(None);
    seqfilter::CONTROL_EXTRA.store(0, Relaxed);
    seqfilter::PLANT_NEWEST.store(false, Relaxed);
    seqfilter::PLANT_MIN.store(false, Relaxed);
    crate::lifecycle::LEGACY_FILTERS.store(false, Relaxed);
}

async fn flush(t: &Tenant) {
    let db = t.fx_writer_db().unwrap();
    db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
}

fn crange(c: usize) -> (Vec<u8>, Vec<u8>) {
    let name = format!("c{c:02}");
    let mut lo = b"u/".to_vec();
    lo.push(name.len() as u8);
    lo.extend(name.as_bytes());
    let mut hi = lo.clone();
    *hi.last_mut().unwrap() += 1;
    (lo, hi)
}

fn val(rng: &mut StdRng) -> Vec<u8> {
    (0..rng.gen_range(1..80)).map(|_| rng.gen()).collect()
}

/// Every key by a point read, every customer by a range read both ways, and the whole keyspace: the number of answers
/// that differ from the model.
async fn read_all(t: &Tenant, m: &BTreeMap<Vec<u8>, Vec<u8>>, keys: &[Vec<u8>]) -> u64 {
    let mut wrong = 0;
    for k in keys {
        if t.get(k).await.unwrap().map(|b| b.to_vec()) != m.get(k).cloned() {
            wrong += 1;
        }
    }
    for c in 0..40 {
        let (lo, hi) = crange(c);
        for rev in [false, true] {
            let got = as_vec(t.scan_range(&lo, Some(&hi), rev, None, 1 << 20).await.unwrap());
            if got != model_range(m, &lo, Some(&hi), rev, None) {
                wrong += 1;
            }
        }
    }
    let got = as_vec(t.scan_range(b"", None, false, None, 1 << 20).await.unwrap());
    if got != model_range(m, b"", None, false, None) {
        wrong += 1;
    }
    wrong
}

/// Planted bug 2: a transaction holds version 1; rows written before it began sit in SlateDB above X_1; version 2 absorbs
/// them (no drop runs). Skipping with version 2's cut loses them for the transaction, which reads version 1's runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r5b_plant_newest_cut_txn() {
    let _s = SERIAL.lock().await;
    setup();
    let mut rng = StdRng::seed_from_u64(91);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "r5b-newest";
    let keys = universe();
    let l = lc(&store);
    let mut t = l.create(id).await.unwrap();
    let mut m: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for k in keys.iter().step_by(2) {
        let v = val(&mut rng);
        apply(&mut t, &mut m, vec![(k.clone(), Some(v))]).await;
    }
    flush(&t).await;
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    m.insert(crate::envelope::MARKER.to_vec(), b"E1".to_vec());
    let x1 = t.fx_view().unwrap().x_d;
    // rows above X_1: rewrites, new keys, deletes, flushed to L0
    for k in keys.iter().step_by(3) {
        let v = if rng.gen_bool(0.2) { None } else { Some(val(&mut rng)) };
        apply(&mut t, &mut m, vec![(k.clone(), v)]).await;
    }
    flush(&t).await;
    // the transaction begins: it holds version 1 and SlateDB's snapshot, which has the rows above X_1
    let snap = t.txn_snapshot().await.unwrap();
    let sm = m.clone();
    // version 2 absorbs them; nothing drops them from SlateDB
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let x2 = t.fx_view().unwrap().x_d;
    assert!(x2 > x1, "version 2 is newer: {x1} {x2}");
    let txn_wrong = |plant: bool| {
        let snap = Arc::clone(&snap);
        let sm = sm.clone();
        let keys = keys.clone();
        async move {
            seqfilter::PLANT_NEWEST.store(plant, Relaxed);
            let mut wrong = 0u64;
            for k in &keys {
                if snap.get(k).await.unwrap().map(|b| b.to_vec()) != sm.get(k).cloned() {
                    wrong += 1;
                }
            }
            for c in 0..40 {
                let (lo, hi) = crange(c);
                let got = as_vec(snap.scan(&lo, Some(&hi), false, None, 1 << 20).await.unwrap());
                if got != model_range(&sm, &lo, Some(&hi), false, None) {
                    wrong += 1;
                }
            }
            seqfilter::PLANT_NEWEST.store(false, Relaxed);
            wrong
        }
    };
    let right = txn_wrong(false).await;
    let planted = txn_wrong(true).await;
    eprintln!("r5b newest cut: X_1 {x1}, X_2 {x2}; transaction on version 1: wrong {right} with its own cut, {planted} with version 2's cut (planted)");
    assert_eq!(right, 0, "the held version's cut reads right");
    assert!(planted > 0, "the planted cut must lose the rows between X_1 and X_2");
    // the writer itself, on version 2: right either way
    assert_eq!(read_all(&t, &m, &keys).await, 0);
    drop(snap);
    t.abandon().await;
    teardown();
}

/// SSTs written before the filters existed: the release before step 5b wrote SlateDB's default Bloom filter only, and no
/// filter at all under 1,000 keys. After an upgrade (a restart into the new binary), a database holds such SSTs at and
/// above X_d, with new ones beside them. None of the old ones may be skipped; the new ones at or below X_d are. Then a
/// rollback: the old binary reads the new SSTs (it ignores the unknown filters). Control: a cut far above X_d skips the
/// new SSTs and loses rows, but never touches an old one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r5b_old_ssts_without_filters() {
    let _s = SERIAL.lock().await;
    setup();
    let mut rng = StdRng::seed_from_u64(92);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "r5b-legacy";
    let keys = universe();
    let mut m: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    // 1. the old binary: a big SST (Bloom only) at or below X_1, a small one (no filter) above it, rows in the memtable
    crate::lifecycle::LEGACY_FILTERS.store(true, Relaxed);
    let mut t = lc(&store).create(id).await.unwrap();
    for k in keys.iter().step_by(2) {
        let v = val(&mut rng);
        apply(&mut t, &mut m, vec![(k.clone(), Some(v))]).await;
    }
    flush(&t).await;
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    m.insert(crate::envelope::MARKER.to_vec(), b"E1".to_vec());
    for k in keys.iter().step_by(7) {
        let v = if rng.gen_bool(0.2) { None } else { Some(val(&mut rng)) };
        apply(&mut t, &mut m, vec![(k.clone(), v)]).await;
    }
    flush(&t).await;
    // more old SSTs, then version 2 over them all (old SSTs at or below X_2, Bloom-only and filterless)
    for k in keys.iter().skip(1).step_by(5) {
        let v = if rng.gen_bool(0.2) { None } else { Some(val(&mut rng)) };
        apply(&mut t, &mut m, vec![(k.clone(), v)]).await;
    }
    flush(&t).await;
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    for k in keys.iter().skip(2).step_by(11) {
        let v = if rng.gen_bool(0.2) { None } else { Some(val(&mut rng)) };
        apply(&mut t, &mut m, vec![(k.clone(), v)]).await;
    }
    flush(&t).await;
    for k in keys.iter().skip(3).step_by(13) {
        let v = Some(val(&mut rng));
        apply(&mut t, &mut m, vec![(k.clone(), v)]).await; // left in the memtable and the WAL
    }
    assert_eq!(read_all(&t, &m, &keys).await, 0, "the old binary reads right");
    let old_ssts = t.writer_sst_count().unwrap();
    t.abandon().await;
    // 2. the upgrade: the new binary reopens the database (WAL replayed), every old SST is still there
    crate::lifecycle::LEGACY_FILTERS.store(false, Relaxed);
    view::clear_cache();
    let mut t = lc(&store).wake(id).await.unwrap();
    t.upgrade().await.unwrap();
    let (a0, s0) = (seqfilter::ASKED.load(Relaxed), seqfilter::SKIPPED.load(Relaxed));
    let wrong = read_all(&t, &m, &keys).await;
    let (asked, skipped) = (seqfilter::ASKED.load(Relaxed) - a0, seqfilter::SKIPPED.load(Relaxed) - s0);
    eprintln!("r5b legacy: {old_ssts} old SSTs; new binary after the restart: wrong {wrong}, seq filter asked {asked}, skipped {skipped}, SSTs now {:?}", t.writer_sst_count());
    assert_eq!(wrong, 0);
    // a control that skips everything it can still cannot skip an SST without the filter: every old row is read
    seqfilter::CONTROL_EXTRA.store(1 << 40, Relaxed);
    let s1 = seqfilter::SKIPPED.load(Relaxed);
    let wrong_ctl_old = read_all(&t, &m, &keys).await;
    let skipped_ctl_old = seqfilter::SKIPPED.load(Relaxed) - s1;
    seqfilter::CONTROL_EXTRA.store(0, Relaxed);
    eprintln!("r5b legacy: control on old SSTs only (plus the replayed memtable's flush, if any): wrong {wrong_ctl_old}, skipped {skipped_ctl_old}");
    // 3. new SSTs at and above X_d beside the old ones
    for k in keys.iter().skip(4).step_by(3) {
        let v = if rng.gen_bool(0.2) { None } else { Some(val(&mut rng)) };
        apply(&mut t, &mut m, vec![(k.clone(), v)]).await;
    }
    flush(&t).await;
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    for k in keys.iter().skip(5).step_by(4) {
        let v = if rng.gen_bool(0.2) { None } else { Some(val(&mut rng)) };
        apply(&mut t, &mut m, vec![(k.clone(), v)]).await;
    }
    flush(&t).await;
    let (a0, s0) = (seqfilter::ASKED.load(Relaxed), seqfilter::SKIPPED.load(Relaxed));
    let wrong = read_all(&t, &m, &keys).await;
    let (asked, skipped) = (seqfilter::ASKED.load(Relaxed) - a0, seqfilter::SKIPPED.load(Relaxed) - s0);
    eprintln!("r5b legacy: mixed old and new SSTs ({:?}): wrong {wrong}, asked {asked}, skipped {skipped}", t.writer_sst_count());
    assert_eq!(wrong, 0);
    assert!(skipped > 0, "the new SSTs at or below X_d are skipped");
    seqfilter::CONTROL_EXTRA.store(1 << 40, Relaxed);
    let wrong_ctl = read_all(&t, &m, &keys).await;
    seqfilter::CONTROL_EXTRA.store(0, Relaxed);
    eprintln!("r5b legacy: control, cut far above X_d on the mixed database: wrong {wrong_ctl}");
    assert!(wrong_ctl > 0, "the control loses the new rows above X_d");
    t.abandon().await;
    // 4. rollback: the old binary reopens the database written by the new one
    crate::lifecycle::LEGACY_FILTERS.store(true, Relaxed);
    view::clear_cache();
    let mut t = lc(&store).wake(id).await.unwrap();
    t.upgrade().await.unwrap();
    let (a0, s0) = (seqfilter::ASKED.load(Relaxed), seqfilter::SKIPPED.load(Relaxed));
    let wrong = read_all(&t, &m, &keys).await;
    eprintln!("r5b legacy: rollback reader: wrong {wrong}, seq filter asked {}, skipped {}", seqfilter::ASKED.load(Relaxed) - a0, seqfilter::SKIPPED.load(Relaxed) - s0);
    assert_eq!(wrong, 0);
    assert_eq!(seqfilter::ASKED.load(Relaxed) - a0, 0, "a binary without the policy never asks the filter");
    t.abandon().await;
    teardown();
}

/// Every SST the writer and the drop's compaction worker write carries the seq filter, also after a crash and a reopen:
/// a full scan with a cut far above X_d asks the filter of every SST in the writer's manifest exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r5b_every_sst_has_the_filters() {
    let _s = SERIAL.lock().await;
    setup();
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(32, Relaxed);
    hold::HOLD_LIFE_MS.store(3000, Relaxed); // as the model test: below the knobs' remote lease (F6)
    let mut rng = StdRng::seed_from_u64(93);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "r5b-every";
    let keys = universe();
    let l = lc(&store);
    let mut t = l.create(id).await.unwrap();
    let mut m: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for k in keys.iter().step_by(2) {
        let v = val(&mut rng);
        apply(&mut t, &mut m, vec![(k.clone(), Some(v))]).await;
    }
    flush(&t).await;
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    m.insert(crate::envelope::MARKER.to_vec(), b"E1".to_vec());
    eprintln!("r5b every SST: published");
    let k = super::compactor_tests::test_knobs(true);
    let pool = pool::Pool::new(2);
    let mut c = compactor::Compactor::start(&mut t, Arc::clone(&store), k.clone(), pool, tokio::runtime::Handle::current()).await.unwrap();
    c.run();
    // small L0 SSTs from here on (the compactor absorbs them), so the drop folds many
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(3 << 10, Relaxed);
    eprintln!("r5b every SST: compactor started");
    for round in 0..3 {
        if round == 2 {
            // past the takeover window (rule 27: one serving lease plus one hold life), so the drop drops rows again
            tokio::time::sleep(Duration::from_millis(4000)).await;
        }
        for i in 0..1500 {
            if i % 300 == 0 {
                eprintln!("r5b every SST: round {round} op {i}, L0 {:?}", t.writer_l0());
            }
            let n = rng.gen_range(1..4);
            let ops: Vec<_> = (0..n).map(|_| (keys[rng.gen_range(0..keys.len())].clone(), if rng.gen_bool(0.2) { None } else { Some(val(&mut rng)) })).collect();
            apply(&mut t, &mut m, ops).await;
            t.fx_sync();
        }
        eprintln!("r5b every SST: round {round} written, L0 {:?}", t.writer_l0());
        if round == 0 {
            // a crash and a reopen
            c.stop().await;
            t.abandon().await;
            t = lc(&store).wake(id).await.unwrap();
            t.upgrade().await.unwrap();
            let pool = pool::Pool::new(2);
            c = compactor::Compactor::start(&mut t, Arc::clone(&store), k.clone(), pool, tokio::runtime::Handle::current()).await.unwrap();
            c.run();
        }
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;
    c.stop().await;
    t.fx_sync();
    flush(&t).await;
    let drops = c.drop.rows_dropped.load(Relaxed);
    let srs = t.writer_sorted_runs().unwrap();
    let n = t.writer_sst_count().unwrap();
    let a0 = seqfilter::ASKED.load(Relaxed);
    seqfilter::CONTROL_EXTRA.store(1 << 40, Relaxed);
    let _ = t.scan_range(b"", None, false, None, 1 << 20).await.unwrap();
    seqfilter::CONTROL_EXTRA.store(0, Relaxed);
    let asked = seqfilter::ASKED.load(Relaxed) - a0;
    let ca0 = seqfilter::CUST_ASKED.load(Relaxed);
    let wrong = read_all(&t, &m, &keys).await;
    let cust_asked = seqfilter::CUST_ASKED.load(Relaxed) - ca0;
    eprintln!("r5b every SST: {n} SSTs in the manifest ({srs} sorted runs, rows dropped by the drop {drops}), seq filter asked {asked} by one full scan; customer filter asked {cust_asked}; wrong {wrong}");
    assert_eq!(wrong, 0);
    assert!(srs > 0 && drops > 0, "the drop folded");
    assert_eq!(asked as usize, n, "every SST has the seq filter");
    assert!(cust_asked > 0);
    t.abandon().await;
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(0, Relaxed);
    teardown();
}
