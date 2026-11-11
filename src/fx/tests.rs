//! Tests of compaction 2, step 3: the manifest, the merged reader, holds, the offline builder.
//!
//! The model test (`model_publish_every_100_ops_with_crashes`) is the done-means of the step: 100,000 operations
//! against an exact model (a BTreeMap), a publish every 100 operations (reseal all, a delta run only, or some groups),
//! crash points (before the publish, after the publish before the tenant reads with it, and at random), reads in
//! writer and reader mode, and transaction snapshots held across publishes. `FX_MODEL_OPS` changes the count.

use super::build::{BuildOpts, Reseal};
use super::*;
use crate::counting::CountingStore;
use crate::lifecycle::{Lifecycle, Op, Tenant};
use crate::runfmt::{CustomerKind, RunOptions};
use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

/// The fx tests change process-wide settings (ENABLED, hold life, T_pin): one at a time.
pub(super) static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(super) fn small_opts(reseal: Reseal) -> BuildOpts {
    BuildOpts {
        reseal,
        group_bytes: 16 << 10,
        sealed: RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2),
        delta: RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2),
    }
}

pub(super) fn lc(store: &Arc<dyn ObjectStore>) -> Arc<Lifecycle> {
    Lifecycle::new(Arc::new(CountingStore::new(Arc::clone(store))), None)
}

pub(super) fn ckey(c: usize, i: usize) -> Vec<u8> {
    let name = format!("c{c:02}");
    let mut k = b"u/".to_vec();
    k.push(name.len() as u8);
    k.extend(name.as_bytes());
    k.extend(format!("msgs/d/{i:05}").as_bytes());
    k
}

/// The key universe: layout v2 customer keys and shared keyspace keys.
pub(super) fn universe() -> Vec<Vec<u8>> {
    let mut u = Vec::new();
    for c in 0..40 {
        for i in 0..50 {
            u.push(ckey(c, i));
        }
    }
    for i in 0..500 {
        u.push(format!("doc/plain/{i:05}").into_bytes());
    }
    for i in 0..300 {
        u.push(format!("idx/x/{i:05}").into_bytes());
    }
    u.sort();
    u
}

pub(super) fn model_range(m: &BTreeMap<Vec<u8>, Vec<u8>>, lo: &[u8], hi: Option<&[u8]>, rev: bool, limit: Option<usize>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let it: Box<dyn Iterator<Item = (&Vec<u8>, &Vec<u8>)>> = match hi {
        Some(h) if lo >= h => return vec![],
        Some(h) => Box::new(m.range(lo.to_vec()..h.to_vec())),
        None => Box::new(m.range(lo.to_vec()..)),
    };
    let mut v: Vec<(Vec<u8>, Vec<u8>)> = it.map(|(k, v)| (k.clone(), v.clone())).collect();
    if rev {
        v.reverse();
    }
    if let Some(l) = limit {
        v.truncate(l);
    }
    v
}

pub(super) fn as_vec(r: Vec<(Bytes, Bytes)>) -> Vec<(Vec<u8>, Vec<u8>)> {
    r.into_iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect()
}

pub(super) async fn apply(t: &mut Tenant, m: &mut BTreeMap<Vec<u8>, Vec<u8>>, ops: Vec<(Vec<u8>, Option<Vec<u8>>)>) {
    let mut batch = Vec::new();
    for (k, v) in &ops {
        match v {
            Some(v) => batch.push(Op::Put(k.clone(), v.clone())),
            None => batch.push(Op::Delete(k.clone())),
        }
    }
    t.apply(batch).await.unwrap();
    for (k, v) in ops {
        match v {
            Some(v) => m.insert(k, v),
            None => m.remove(&k),
        };
    }
}

/// T_pin of these tests: envelopes start one T_pin after E1's announcement (rule 26), and a reader refreshes its
/// version every half T_pin.
pub(super) const TEST_T_PIN_MS: u64 = 200;

pub(super) fn enable() {
    ENABLED.store(true, Relaxed);
    PUBLISH_EVERY.store(0, Relaxed);
    T_PIN_MS.store(TEST_T_PIN_MS, Relaxed);
}

/// Publish, waiting out E1's announcement the first time (rule 26).
pub(super) async fn publish(t: &mut Tenant, o: &BuildOpts) -> super::build::BuildReport {
    for _ in 0..50 {
        if let Some(r) = t.fx_publish(o, None).await.unwrap() {
            return r;
        }
        assert!(t.envelope_seq().is_none(), "nothing to publish");
        tokio::time::sleep(std::time::Duration::from_millis(TEST_T_PIN_MS / 4)).await;
    }
    panic!("no publish");
}

/// Rows at or below X_d are answered by our runs: the read equals the model, and once the sealed run is gone, a read
/// that needs it is an error, never an answer from SlateDB's older copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rows_at_or_below_xd_come_from_the_runs() {
    let _s = SERIAL.lock().await;
    enable();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let l = lc(&store);
    let mut t = l.create("fx-basic").await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..300 {
        apply(&mut t, &mut m, vec![(ckey(i % 7, i), Some(format!("v{i}").into_bytes()))]).await;
    }
    let rep = publish(&mut t, &small_opts(Reseal::All)).await;
    assert_eq!(rep.sealed_rows, 301, "300 rows and the envelope marker"); // the marker is a plain row of the database
    let before = view::ROWS_FROM_RUNS.load(Relaxed);
    for (k, v) in &m {
        assert_eq!(t.get(k).await.unwrap().as_deref(), Some(&v[..]));
    }
    assert!(view::ROWS_FROM_RUNS.load(Relaxed) - before >= 300);
    let all = t.scan_range(b"u/", Some(b"u0"), false, None, 4096).await.unwrap();
    assert_eq!(as_vec(all), model_range(&m, b"u/", Some(b"u0"), false, None));
    // a newer write is SlateDB's (above X_d)
    apply(&mut t, &mut m, vec![(ckey(0, 0), Some(b"new".to_vec()))]).await;
    assert_eq!(t.get(ckey(0, 0)).await.unwrap().as_deref(), Some(&b"new"[..]));
    // remove every sealed run object and the cached blocks: reads of old rows fail
    let v = t.fx_view().unwrap().clone();
    for g in &v.groups {
        if let Some(s) = &g.sealed {
            store.delete(&object_store::path::Path::from(s.line.path.as_str())).await.unwrap();
        }
    }
    view::clear_cache();
    let mut t2 = lc(&store).wake("fx-basic").await;
    assert!(t2.is_err() || t2.as_mut().unwrap().get(ckey(3, 3)).await.is_err(), "a read below X_d must need the run");
    t.abandon().await;
}

/// The unit test from TLC's ReadNoCut trace (RULES-FOR-THE-CODE rule 2): a delta run still listed for another group
/// holds a put of key a1 at or below a1's group's X_g, and the reseal of a1's group applied a later delete. With the X_g
/// bound a1 is absent; without it (the control) the stale put comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_no_cut_trace() {
    let _s = SERIAL.lock().await;
    enable();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let l = lc(&store);
    let mut t = l.create("fx-nocut").await.unwrap();
    let mut m = BTreeMap::new();
    // two groups: a1 in the first, b1 in the second (group bytes small, filler keys between them)
    let a1 = b"a/1".to_vec();
    let b1 = b"z/1".to_vec();
    apply(&mut t, &mut m, vec![(a1.clone(), Some(b"x".to_vec()))]).await;
    for i in 0..200 {
        apply(&mut t, &mut m, vec![(format!("m/{i:04}").into_bytes(), Some(vec![7u8; 200]))]).await;
    }
    apply(&mut t, &mut m, vec![(b1.clone(), Some(b"y".to_vec()))]).await;
    let mut o = small_opts(Reseal::All);
    o.group_bytes = 20 << 10;
    publish(&mut t, &o).await;
    let gs = t.fx_view().unwrap().groups.len();
    assert!(gs >= 2, "{gs} groups");
    // delete a1 (delta 1), put a1 again (delta 2), delete a1 and reseal only a1's group
    apply(&mut t, &mut m, vec![(a1.clone(), None)]).await;
    publish(&mut t, &small_opts(Reseal::None)).await;
    apply(&mut t, &mut m, vec![(a1.clone(), Some(b"stale".to_vec()))]).await;
    publish(&mut t, &small_opts(Reseal::None)).await;
    apply(&mut t, &mut m, vec![(a1.clone(), None)]).await;
    publish(&mut t, &small_opts(Reseal::Some(vec![0]))).await;
    // a delta merge (step 4) may drop a row at or below its group's X_g (rule 13): the newest delta run holds only the
    // delete of a1, at a1's group's new X_g, so the merged version lists the older delta runs without it
    let mut merged = t.fx_view().unwrap().meta.clone();
    assert_eq!(merged.delta[0].entries, 1);
    assert_eq!(merged.delta[0].max_seq, merged.groups[0].x_g);
    merged.delta.remove(0);
    merged.version += 1;
    assert!(manifest::create(&store, "fx-nocut", &merged).await.unwrap());
    t.fx_refresh().await.unwrap();
    let v = t.fx_view().unwrap().clone();
    assert_eq!(v.groups[0].x_g, v.x_d);
    assert!(v.groups[1].x_g < v.x_d, "the second group keeps its older X_g");
    assert!(v.delta.len() >= 2, "the delta runs stay listed for the second group");
    assert_eq!(t.get(&a1).await.unwrap(), None);
    assert_eq!(as_vec(t.scan_range(b"a/", Some(b"b"), false, None, 4096).await.unwrap()), vec![]);
    view::CONTROL_NO_CUT.store(true, Relaxed);
    let bad_get = t.get(&a1).await.unwrap();
    let bad_scan = t.scan_range(b"a/", Some(b"b"), false, None, 4096).await.unwrap();
    view::CONTROL_NO_CUT.store(false, Relaxed);
    assert_eq!(bad_get.as_deref(), Some(&b"stale"[..]), "the control brings the stale put back");
    assert_eq!(bad_scan.len(), 1);
    t.abandon().await;
}

/// Review of step 0, F1: a writer that opens publishes a fence version, so the PUT of a dead process that lands late
/// is refused, and the new writer reads every committed write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_put_of_a_dead_process_is_refused_by_the_fence() {
    let _s = SERIAL.lock().await;
    enable();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-fence";
    let mut a = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..50 {
        apply(&mut a, &mut m, vec![(ckey(1, i), Some(b"a".to_vec()))]).await;
    }
    publish(&mut a, &small_opts(Reseal::All)).await;
    for i in 0..50 {
        apply(&mut a, &mut m, vec![(ckey(2, i), Some(b"b".to_vec()))]).await;
    }
    // A's next publish stalls in its PUT
    let gate = Arc::new(tokio::sync::Notify::new());
    *manifest::SLOW_PUT.lock().unwrap() = Some(gate.clone());
    let pubtask = tokio::spawn(async move {
        let r = a.fx_publish(&small_opts(Reseal::None), None).await;
        (r.map(|_| ()), a)
    });
    while manifest::SLOW_PUT_WAITING.load(std::sync::atomic::Ordering::SeqCst) == 0 {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    *manifest::SLOW_PUT.lock().unwrap() = None;
    // B takes over: its writer open fences the next version number
    let mut b = lc(&store).wake(id).await.unwrap();
    b.upgrade().await.unwrap();
    let fenced = b.fx_view().unwrap().version;
    assert_eq!(b.fx_view().unwrap().meta.kind, "fence");
    gate.notify_one();
    let (r, a) = pubtask.await.unwrap();
    assert!(r.is_err(), "the dead process's late publish must be refused");
    assert_eq!(manifest::load_newest(&store, id).await.unwrap().unwrap().version, fenced);
    for (k, v) in &m {
        assert_eq!(b.get(k).await.unwrap().as_deref(), Some(&v[..]));
    }
    a.abandon().await;
    b.abandon().await;
}

/// The run id recorded in the manifest line is checked when the run is opened: another object at the same path, even
/// of the same size, is an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_replaced_at_its_path_is_an_error() {
    let _s = SERIAL.lock().await;
    enable();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-runid";
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..40 {
        apply(&mut t, &mut m, vec![(ckey(4, i), Some(b"same".to_vec()))]).await;
    }
    publish(&mut t, &small_opts(Reseal::All)).await;
    let v = t.fx_view().unwrap().clone();
    let line = v.groups.iter().find_map(|g| g.sealed.clone()).unwrap().line.clone();
    // write another run with the same rows (same size, another random id) and copy it over the listed path
    let mut w = crate::runfmt::RunWriter::new(&store, object_store::path::Path::from("tmp/other"), small_opts(Reseal::All).sealed).await.unwrap();
    let src = crate::runfmt::RunStream::open(Arc::clone(&store), object_store::path::Path::from(line.path.as_str()), line.size, 1 << 20).await.unwrap();
    let mut src = src;
    while let Some(r) = src.next().await.unwrap() {
        w.add(&r.key, r.seq, &r.env).await.unwrap();
    }
    let meta = w.finish().await.unwrap();
    assert_eq!(meta.size, line.size);
    store.copy(&object_store::path::Path::from("tmp/other"), &object_store::path::Path::from(line.path.as_str())).await.unwrap();
    view::clear_cache();
    t.abandon().await;
    let t2 = lc(&store).wake(id).await.unwrap();
    let e = t2.get(ckey(4, 1)).await.unwrap_err();
    assert!(format!("{e:#}").contains("another id"), "{e:#}");
    t2.abandon().await;
}

/// Holds: the floor is the smallest X_d held; a hold past the hold life is revoked, leaves the floor, and its reader's
/// next step fails with a retryable error (rule 5). A transaction snapshot keeps its version across publishes (rule 1,
/// probe 3 C): it reads the state at its S.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn holds_floor_revocation_and_snapshots_across_publishes() {
    let _s = SERIAL.lock().await;
    enable();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-holds";
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..100 {
        apply(&mut t, &mut m, vec![(ckey(5, i), Some(format!("a{i}").into_bytes()))]).await;
    }
    publish(&mut t, &small_opts(Reseal::All)).await;
    let x1 = t.fx_view().unwrap().x_d;
    let snap = t.txn_snapshot().await.unwrap();
    let at_s = m.clone();
    assert_eq!(t.fx_holds().floor(), Some(x1));
    for i in 0..100 {
        let v = if i % 3 == 0 { None } else { Some(format!("b{i}").into_bytes()) };
        apply(&mut t, &mut m, vec![(ckey(5, i), v)]).await;
    }
    publish(&mut t, &small_opts(Reseal::All)).await;
    for i in 0..50 {
        apply(&mut t, &mut m, vec![(ckey(6, i), Some(b"c".to_vec()))]).await;
    }
    publish(&mut t, &small_opts(Reseal::None)).await;
    assert!(t.fx_view().unwrap().x_d > snap.seq());
    assert_eq!(t.fx_holds().floor(), Some(x1), "the snapshot's hold keeps the floor at its version");
    for k in universe() {
        assert_eq!(snap.get(&k).await.unwrap().map(|b| b.to_vec()), at_s.get(&k).cloned(), "snapshot read of {:?}", String::from_utf8_lossy(&k));
    }
    assert_eq!(as_vec(snap.scan(b"u/", None, true, Some(30), 4096).await.unwrap()), model_range(&at_s, b"u/", None, true, Some(30)));
    for k in universe() {
        assert_eq!(t.get(&k).await.unwrap().map(|b| b.to_vec()), m.get(&k).cloned());
    }
    // revocation
    hold::HOLD_LIFE_MS.store(50, Relaxed);
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    assert_eq!(t.fx_holds().floor(), None, "a revoked hold leaves the floor");
    let e = snap.get(&ckey(5, 1)).await.unwrap_err();
    assert!(e.downcast_ref::<Retry>().is_some(), "{e:#}");
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
    drop(snap);
    assert_eq!(t.fx_holds().count(), 0);
    t.abandon().await;
}

/// A tenant in reader mode refuses to read with a version older than T_pin, and reads again after a refresh (rule 6).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reader_refuses_past_t_pin_and_refreshes() {
    let _s = SERIAL.lock().await;
    enable();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-tpin";
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..30 {
        apply(&mut t, &mut m, vec![(ckey(8, i), Some(b"v".to_vec()))]).await;
    }
    publish(&mut t, &small_opts(Reseal::All)).await;
    t.close().await.unwrap();
    // c2-fix-long-reads: a read step renews a stale state before it reads; this test checks the guard behind the renewal
    CONTROL_NO_RENEW.store(true, Relaxed);
    let mut r = lc(&store).wake(id).await.unwrap();
    assert!(!r.is_writer() && r.fx_view().is_some());
    assert_eq!(r.get(ckey(8, 3)).await.unwrap().as_deref(), Some(&b"v"[..]));
    T_PIN_MS.store(40, Relaxed);
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    assert!(r.fx_refresh_due());
    let e = r.get(ckey(8, 3)).await.unwrap_err();
    assert!(e.downcast_ref::<Retry>().is_some(), "{e:#}");
    r.fx_refresh().await.unwrap();
    assert_eq!(r.get(ckey(8, 3)).await.unwrap().as_deref(), Some(&b"v"[..]));
    T_PIN_MS.store(TEST_T_PIN_MS, Relaxed);
    CONTROL_NO_RENEW.store(false, Relaxed);
    r.abandon().await;
}

/// Rule 26: envelopes start in two steps. The first publish only announces release E1 in the store; the marker (E_env)
/// is written, and the version published, once this process saw the announcement at least T_pin ago.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn envelopes_start_one_t_pin_after_the_announcement() {
    let _s = SERIAL.lock().await;
    enable();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-e1";
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    apply(&mut t, &mut m, vec![(ckey(9, 1), Some(b"v".to_vec()))]).await;
    let t0 = std::time::Instant::now();
    assert!(t.fx_publish(&small_opts(Reseal::All), None).await.unwrap().is_none());
    assert!(manifest::announced(&store, id).await.unwrap());
    assert_eq!(t.envelope_seq(), None, "no marker before one T_pin");
    assert!(manifest::list_versions(&store, id).await.unwrap().is_empty());
    publish(&mut t, &small_opts(Reseal::All)).await;
    assert!(t0.elapsed() >= std::time::Duration::from_millis(TEST_T_PIN_MS));
    assert!(t.envelope_seq().is_some());
    assert_eq!(t.get(ckey(9, 1)).await.unwrap().as_deref(), Some(&b"v"[..]));
    t.abandon().await;
}

/// The structural rules of a version.
#[test]
fn version_check_refuses_broken_versions() {
    let ok = manifest::Version { format: 1, version: 3, kind: "publish".into(), writer_epoch: Some(1), x_d: 10, e_env: Some(2), published_ms: 0, groups: vec![manifest::GroupLine { id: 0, lo: String::new(), x_g: 8, sealed: None, unsealed: 0, unsealed_since_ms: 0, sealed_ms: 0 }], delta: vec![], next_group_id: 1 };
    ok.check().unwrap();
    let mut v = ok.clone();
    v.e_env = None;
    assert!(v.check().is_err(), "no E_env");
    let mut v = ok.clone();
    v.e_env = Some(11);
    assert!(v.check().is_err(), "E_env above X_d");
    let mut v = ok.clone();
    v.groups[0].x_g = 11;
    assert!(v.check().is_err(), "X_g above X_d");
    let mut v = ok.clone();
    v.groups[0].lo = "61".into();
    assert!(v.check().is_err(), "first group not at the empty key");
}

#[derive(Default, Debug)]
struct Stats {
    ops: u64,
    writes: u64,
    gets: u64,
    scans: u64,
    snap_reads: u64,
    reader_mode_reads: u64,
    publishes: u64,
    crashes: u64,
    fail_after_runs: u64,
    fail_after_publish: u64,
    wrong: u64,
    retries: u64,
    compactions: u64,
}

/// The step's done-means: 0 wrong in 100,000 operations, a publish every 100 operations, crash points.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn model_publish_every_100_ops_with_crashes() {
    let _s = SERIAL.lock().await;
    enable();
    let n_ops: u64 = std::env::var("FX_MODEL_OPS").ok().and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let seed: u64 = std::env::var("FX_MODEL_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(20261108);
    let mut rng = StdRng::seed_from_u64(seed);
    // FX_MODEL_CONTROL=nocut|skipdelta: a broken reader, which the model must catch
    let control = std::env::var("FX_MODEL_CONTROL").unwrap_or_default();
    // FX_SLICES=0|1|2: what the counter keeps of a fetched block (step 6: whole blocks, the customer's rows, the area's)
    let slices_before = counter::SLICES.load(Relaxed);
    if let Some(m) = std::env::var("FX_SLICES").ok().and_then(|s| s.parse::<u8>().ok()) {
        counter::SLICES.store(m, Relaxed);
    }
    // FX_TRIM_MS=n: slices are trimmed to what reads relied on n ms after they are made (0: at the next insert)
    let trim_before = counter::TRIM_MS.load(Relaxed);
    if let Some(ms) = std::env::var("FX_TRIM_MS").ok().and_then(|s| s.parse::<u64>().ok()) {
        counter::TRIM_MS.store(ms, Relaxed);
    }
    eprintln!("model test: slice mode {:?}, trim after {} ms", counter::slice_mode(), counter::TRIM_MS.load(Relaxed));
    match control.as_str() {
        "nocut" => view::CONTROL_NO_CUT.store(true, Relaxed),
        "skipdelta" => view::CONTROL_SKIP_DELTA.store(true, Relaxed),
        _ => {}
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-model";
    let keys = universe();
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut snaps: Vec<(Arc<read::Snap>, BTreeMap<Vec<u8>, Vec<u8>>, u64)> = Vec::new();
    let mut st = Stats::default();
    let runs_before = view::ROWS_FROM_RUNS.load(Relaxed);
    let bound = |rng: &mut StdRng, keys: &[Vec<u8>]| -> Vec<u8> {
        match rng.gen_range(0..10) {
            0 => b"u/".to_vec(),
            1 => b"doc/".to_vec(),
            2 => vec![],
            3 => {
                let mut k = keys[rng.gen_range(0..keys.len())].clone();
                k.push(0);
                k
            }
            4 => {
                let k = &keys[rng.gen_range(0..keys.len())];
                k[..rng.gen_range(0..=k.len())].to_vec()
            }
            _ => keys[rng.gen_range(0..keys.len())].clone(),
        }
    };
    macro_rules! check {
        ($what:expr, $got:expr, $want:expr) => {
            if $got != $want {
                st.wrong += 1;
                if st.wrong <= 5 {
                    eprintln!("WRONG at op {}: {}", st.ops, $what);
                }
            }
        };
    }
    // load the database first, so the first version cuts many groups (they stay fixed in this step)
    for i in 0..keys.len() {
        if i % 3 == 0 {
            let len = rng.gen_range(0..200);
            let v: Vec<u8> = (0..len).map(|_| rng.gen()).collect();
            apply(&mut t, &mut m, vec![(keys[i].clone(), Some(v))]).await;
        }
    }
    // after a crash, some operations read before any write upgrades the tenant (reads in reader mode)
    let mut read_only = 0u32;
    for op in 0..n_ops {
        st.ops = op + 1;
        // a publish every 100 operations, with crash points
        if op > 0 && op % 100 == 0 {
            let reseal = match rng.gen_range(0..10) {
                0..=2 => Reseal::All,
                3..=6 => Reseal::None,
                _ => {
                    let g = t.fx_view().map(|v| v.groups.len()).unwrap_or(1);
                    Reseal::Some((0..g).filter(|_| rng.gen_bool(0.4)).collect())
                }
            };
            let fail_at = match rng.gen_range(0..40) {
                0 => Some("after_runs"),
                1 => Some("after_publish"),
                _ => None,
            };
            let f = move |p: &str| -> anyhow::Result<()> {
                if Some(p) == fail_at {
                    anyhow::bail!("fail point {p}");
                }
                Ok(())
            };
            let r = t.fx_publish(&small_opts(reseal), Some(&f)).await;
            // the publish that starts envelopes writes the marker, a plain row of the keyspace (envelope.rs)
            if t.envelope_seq().is_some() {
                m.insert(crate::envelope::MARKER.to_vec(), b"E1".to_vec());
            }
            match r {
                Ok(Some(_)) => st.publishes += 1,
                Ok(None) => {}
                Err(e) => {
                    assert!(fail_at.is_some(), "publish failed without a fail point: {e:#}");
                    if fail_at == Some("after_runs") {
                        st.fail_after_runs += 1;
                    } else {
                        st.fail_after_publish += 1;
                    }
                    // the process dies at the fail point
                    snaps.clear();
                    t.abandon().await;
                    t = lc(&store).wake(id).await.unwrap();
                    st.crashes += 1;
                    read_only = 30;
                }
            }
        }
        // SlateDB's own compaction (compaction-1's pass, outside the writer) folds L0 into sorted runs: it keeps every
        // key's newest row, so the cut still holds
        if op > 0 && op % 2500 == 0 {
            if t.writer_l0().unwrap_or(0) >= 1 {
                crate::lifecycle::compact_tenant(&store, &store, id).await.unwrap();
                st.compactions += 1;
            }
        }
        // a crash at a random point
        if rng.gen_range(0..1500) == 0 {
            snaps.clear();
            t.abandon().await;
            t = lc(&store).wake(id).await.unwrap();
            st.crashes += 1;
            read_only = 30;
        }
        // a reader refreshes its version before T_pin, as the server does under the exclusive lock (`Dbs::read`)
        if t.fx_refresh_due() {
            t.fx_refresh().await.unwrap();
            st.retries += 1;
        }
        let reader_mode = !t.is_writer();
        let pick = if read_only > 0 {
            read_only -= 1;
            rng.gen_range(40..93)
        } else {
            rng.gen_range(0..100)
        };
        match pick {
            0..=39 => {
                let n = rng.gen_range(1..=4);
                let mut ops = Vec::new();
                for _ in 0..n {
                    let k = keys[rng.gen_range(0..keys.len())].clone();
                    if rng.gen_bool(0.3) {
                        ops.push((k, None));
                    } else {
                        let len = if rng.gen_range(0..50) == 0 { 3000 } else { rng.gen_range(0..200) };
                        let v: Vec<u8> = (0..len).map(|_| rng.gen()).collect();
                        ops.push((k, Some(v)));
                    }
                }
                apply(&mut t, &mut m, ops).await;
                st.writes += 1;
            }
            40..=64 => {
                let k = if rng.gen_bool(0.9) { keys[rng.gen_range(0..keys.len())].clone() } else { bound(&mut rng, &keys) };
                let got = if rng.gen_bool(0.2) { t.get_durable(&k).await } else { t.get(&k).await };
                let got = got.unwrap().map(|b| b.to_vec());
                check!(format!("get {:?}", String::from_utf8_lossy(&k)), got, m.get(&k).cloned());
                st.gets += 1;
                st.reader_mode_reads += reader_mode as u64;
            }
            65..=89 => {
                let lo = bound(&mut rng, &keys);
                let hi = if rng.gen_bool(0.1) { None } else { Some(bound(&mut rng, &keys)) };
                let rev = rng.gen_bool(0.4);
                let limit = match rng.gen_range(0..4) {
                    0 => None,
                    1 => Some(1),
                    2 => Some(rng.gen_range(2..20)),
                    _ => Some(rng.gen_range(20..400)),
                };
                let ra = if rng.gen_bool(0.5) { 4096 } else { 1 << 20 };
                let got = as_vec(t.scan_range(&lo, hi.as_deref(), rev, limit, ra).await.unwrap());
                check!(format!("scan {:?}..{:?} rev {rev} limit {limit:?}", String::from_utf8_lossy(&lo), hi.as_ref().map(|h| String::from_utf8_lossy(h).to_string())), got, model_range(&m, &lo, hi.as_deref(), rev, limit));
                st.scans += 1;
                st.reader_mode_reads += reader_mode as u64;
            }
            90..=92 => {
                let p = bound(&mut rng, &keys);
                let got = as_vec(t.scan_prefix(&p).await.unwrap());
                let hi = crate::runfmt::prefix_successor(&p);
                check!(format!("prefix {:?}", String::from_utf8_lossy(&p)), got, model_range(&m, &p, hi.as_deref(), false, None));
                st.scans += 1;
            }
            93..=94 => {
                if snaps.len() < 3 {
                    let s = t.txn_snapshot().await.unwrap();
                    snaps.push((s, m.clone(), op));
                }
            }
            _ => {
                if snaps.is_empty() {
                    continue;
                }
                let i = rng.gen_range(0..snaps.len());
                let (s, sm, _) = &snaps[i];
                if rng.gen_bool(0.5) {
                    let k = keys[rng.gen_range(0..keys.len())].clone();
                    let got = s.get(&k).await.unwrap().map(|b| b.to_vec());
                    check!(format!("snapshot get {:?}", String::from_utf8_lossy(&k)), got, sm.get(&k).cloned());
                } else {
                    let lo = bound(&mut rng, &keys);
                    let rev = rng.gen_bool(0.5);
                    let limit = Some(rng.gen_range(1..100));
                    let got = as_vec(s.scan(&lo, None, rev, limit, 4096).await.unwrap());
                    check!("snapshot scan", got, model_range(sm, &lo, None, rev, limit));
                }
                st.snap_reads += 1;
                // snapshots end after a while (a transaction's 60 s)
                snaps.retain(|(_, _, at)| op - at < 1500);
            }
        }
    }
    // a full check at the end, then after a clean restart
    for k in &keys {
        let got = t.get(k).await.unwrap().map(|b| b.to_vec());
        check!("final get", got, m.get(k).cloned());
    }
    t.abandon().await;
    let t = lc(&store).wake(id).await.unwrap();
    let got = as_vec(t.scan_range(b"", None, false, None, 1 << 20).await.unwrap());
    check!("final scan after restart", got, model_range(&m, b"", None, false, None));
    let v = t.fx_view().unwrap();
    let from_runs = view::ROWS_FROM_RUNS.load(Relaxed) - runs_before;
    view::CONTROL_NO_CUT.store(false, Relaxed);
    view::CONTROL_SKIP_DELTA.store(false, Relaxed);
    let mode_used = format!("{:?} trim_ms={} trims={} trim_drops={} refetches={}", counter::slice_mode(), counter::TRIM_MS.load(Relaxed), counter::TRIMS.load(Relaxed), counter::TRIM_DROPS.load(Relaxed), counter::REFETCHES.load(Relaxed));
    counter::SLICES.store(slices_before, Relaxed);
    counter::TRIM_MS.store(trim_before, Relaxed);
    let summary = format!(
        "FX_MODEL control={control:?} slices={:?} seed={seed} ops={} wrong={} writes={} gets={} scans={} snapshot_reads={} reader_mode_reads={} publishes={} crashes={} fail_after_runs={} fail_after_publish={} refreshes={} slatedb_compactions={} final_version={} groups={} delta_runs={} rows_from_runs={from_runs} versions_in_store={}",
        mode_used, st.ops, st.wrong, st.writes, st.gets, st.scans, st.snap_reads, st.reader_mode_reads, st.publishes, st.crashes, st.fail_after_runs, st.fail_after_publish, st.retries, st.compactions, v.version, v.groups.len(), v.delta.len(),
        manifest::list_versions(&store, id).await.unwrap().len()
    );
    eprintln!("{summary}");
    if let Ok(p) = std::env::var("FX_MODEL_OUT") {
        std::fs::write(p, format!("{summary}\n")).unwrap();
    }
    assert!(from_runs > 0);
    if !control.is_empty() {
        assert!(st.wrong > 0, "the control {control} must be caught: {st:?}");
        t.abandon().await;
        return;
    }
    if n_ops >= 20_000 {
        assert!(st.crashes > 0 && st.fail_after_publish > 0 && st.fail_after_runs > 0 && st.reader_mode_reads > 0, "{st:?}");
    }
    assert_eq!(st.wrong, 0, "{st:?}");
    t.abandon().await;
}

/// Hot point reads inside the process (no network, no server): p50 of `Tenant::get` on 2,000 hot keys of 30,000, today's
/// path (no version) against the merged reader (every key at or below X_d, answered by a sealed run, blocks cached).
/// Run: cargo test --release -p tenant-bench --lib fx::tests::hot_get_p50 -- --ignored --nocapture
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn hot_get_p50() {
    let _s = SERIAL.lock().await;
    enable();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut t = lc(&store).create("fx-hot").await.unwrap();
    let mut m = BTreeMap::new();
    let pad = vec![b'x'; 300];
    for b in 0..300 {
        let ops = (0..100).map(|i| (format!("doc/d/{:06}", b * 100 + i).into_bytes(), Some(pad.clone()))).collect();
        apply(&mut t, &mut m, ops).await;
    }
    let mut rng = StdRng::seed_from_u64(5);
    let hot: Vec<Vec<u8>> = (0..2000).map(|_| format!("doc/d/{:06}", rng.gen_range(0..30_000)).into_bytes()).collect();
    async fn measure(t: &Tenant, hot: &[Vec<u8>], rng: &mut StdRng) -> (f64, f64) {
        for k in hot {
            t.get(k).await.unwrap().unwrap();
        }
        let mut lat = Vec::with_capacity(100_000);
        for _ in 0..100_000 {
            let k = &hot[rng.gen_range(0..hot.len())];
            let s = std::time::Instant::now();
            let v = t.get(k).await.unwrap();
            lat.push(s.elapsed().as_nanos() as f64 / 1000.0);
            assert!(v.is_some());
        }
        lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
        (lat[lat.len() / 2], lat[lat.len() * 99 / 100])
    }
    // the same database first without a version (today's path), then with one; three alternations
    let mut out = Vec::new();
    let before_pub = measure(&t, &hot, &mut rng).await;
    out.push(("off", before_pub));
    publish(&mut t, &small_opts(Reseal::All)).await;
    let v = t.fx_view().cloned();
    for round in 0..3 {
        let on = measure(&t, &hot, &mut rng).await;
        out.push(("on", on));
        // the same tenant with the version taken away reads SlateDB alone (it still holds every row in this step)
        ENABLED.store(false, Relaxed);
        t.fx_refresh().await.unwrap();
        let off = measure(&t, &hot, &mut rng).await;
        out.push(("off", off));
        ENABLED.store(true, Relaxed);
        t.fx_refresh().await.unwrap();
        let _ = round;
    }
    let med = |w: &str| {
        let mut p: Vec<f64> = out.iter().filter(|(m, _)| *m == w).map(|(_, (p50, _))| *p50).collect();
        p.sort_by(|a, b| a.partial_cmp(b).unwrap());
        p[p.len() / 2]
    };
    let line = format!("HOT_GET {out:?} median_p50_off_us={:.2} median_p50_on_us={:.2} ratio={:.3} version={:?}", med("off"), med("on"), med("on") / med("off"), v.map(|v| v.version));
    eprintln!("{line}");
    if let Ok(p) = std::env::var("FX_HOT_OUT") {
        std::fs::write(p, format!("{line}\n")).unwrap();
    }
    t.abandon().await;
}
