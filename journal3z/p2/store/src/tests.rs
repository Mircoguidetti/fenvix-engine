//! P1 tests. Every check that guards against a bug has a twin run with that bug switched on (`TestBugs`, or the
//! protocol `Mutations`), which must make the check fail: a test that also passes on broken code proves nothing.
use super::*;
use bytes::Bytes;
use journal3z_core::{Entry, Mutations};

fn ent(idx: Idx, epoch: Epoch, v: u8) -> Rec {
    Rec::Append { db: 1, e: Entry { idx, epoch, barrier: false, data: Bytes::from(vec![v; 100]) } }
}

fn open(dir: &Path, cfg: StoreCfg) -> Result<(Store, NodeCore, RecoveryReport), StoreError> {
    Store::open(dir, Arc::new(RealFs), cfg)
}

/// Apply live (as the node does) and persist.
fn put(st: &mut Store, core: &mut NodeCore, recs: Vec<Rec>) {
    for r in &recs {
        core.apply(r).unwrap();
    }
    st.write(&recs).unwrap();
}

fn small() -> StoreCfg {
    StoreCfg { segment_max: 2000, ..Default::default() }
}

#[test]
fn roundtrip_and_new_identity() {
    let d = tempfile::tempdir().unwrap();
    let (mut st, mut core, rep) = open(d.path(), small()).unwrap();
    assert!(rep.new_identity);
    let id = st.node_id;
    put(&mut st, &mut core, vec![Rec::Promise { db: 1, epoch: 7 }]);
    put(&mut st, &mut core, (1..=30).map(|i| ent(i, 7, i as u8)).collect());
    put(&mut st, &mut core, vec![Rec::Trim { db: 1, upto: 3 }, Rec::Erase { db: 9 }]);
    st.sync().unwrap();
    drop(st);
    let (st2, core2, rep2) = open(d.path(), small()).unwrap();
    assert!(!rep2.new_identity);
    assert_eq!(st2.node_id, id);
    assert_eq!(core2.dbs, core.dbs);
    // a lost directory comes back with a new identity
    drop(st2);
    std::fs::remove_dir_all(d.path()).unwrap();
    let (st3, core3, rep3) = open(d.path(), small()).unwrap();
    assert!(rep3.new_identity && st3.node_id != id && core3.dbs.is_empty());
}

#[test]
fn segments_without_identity_refused() {
    let d = tempfile::tempdir().unwrap();
    let (mut st, mut core, _) = open(d.path(), small()).unwrap();
    put(&mut st, &mut core, vec![ent(1, 1, 1)]);
    st.sync().unwrap();
    drop(st);
    std::fs::remove_file(d.path().join("NODE_ID")).unwrap();
    assert!(matches!(open(d.path(), small()), Err(StoreError::Corrupt(_))));
}

fn torn_tail_case(cfg: StoreCfg) -> Result<usize, StoreError> {
    let d = tempfile::tempdir().unwrap();
    let (mut st, mut core, _) = open(d.path(), cfg.clone()).unwrap();
    put(&mut st, &mut core, (1..=5).map(|i| ent(i, 1, i as u8)).collect());
    st.sync().unwrap();
    // half of a record that was being written when the process died (never synced, never acknowledged)
    let half = frame(&encode_rec(&ent(6, 1, 6)));
    st.file.write_all(&half[..half.len() / 2]).unwrap();
    drop(st);
    let (st, core1, rep) = open(d.path(), cfg.clone())?;
    assert_eq!(rep.torn_tail_bytes, (half.len() / 2) as u64);
    assert_eq!(core1.dbs[&1].last(), 5);
    drop(st);
    // a second restart: the old segment is no longer the last one
    let (_, core2, _) = open(d.path(), cfg)?;
    Ok(core2.dbs[&1].entries.len())
}

#[test]
fn torn_tail_ignored_and_cut() {
    assert_eq!(torn_tail_case(small()).unwrap(), 5);
    // twin: without cutting the torn tail, the next restart finds a torn record in a non-last segment and refuses
    let mut bad = small();
    bad.bugs.no_tail_truncate = true;
    assert!(matches!(torn_tail_case(bad), Err(StoreError::Corrupt(_))));
}

fn mid_corruption_case(cfg: StoreCfg) -> Result<(), StoreError> {
    let d = tempfile::tempdir().unwrap();
    let (mut st, mut core, _) = open(d.path(), cfg.clone()).unwrap();
    put(&mut st, &mut core, (1..=5).map(|i| ent(i, 1, i as u8)).collect());
    st.sync().unwrap();
    drop(st);
    // flip one byte inside the 2nd record: records 3..5 (synced, maybe acknowledged) follow it
    let seg = d.path().join(seg_name(1));
    let mut b = std::fs::read(&seg).unwrap();
    let one = frame(&encode_rec(&ent(1, 1, 1))).len();
    b[one + 20] ^= 0x40;
    std::fs::write(&seg, &b).unwrap();
    open(d.path(), cfg).map(|_| ())
}

#[test]
fn corruption_in_the_middle_refused() {
    assert!(matches!(mid_corruption_case(small()), Err(StoreError::Corrupt(_))));
    // twin: treating it as a torn tail would silently drop the synced records 2..5
    let mut bad = small();
    bad.bugs.accept_mid_corruption = true;
    assert!(mid_corruption_case(bad).is_ok());
}

/// Promise in an old segment; the segment is deleted after everything is trimmed; the promise must survive.
fn fence_case(cfg: StoreCfg) -> Epoch {
    let d = tempfile::tempdir().unwrap();
    let (mut st, mut core, _) = open(d.path(), cfg.clone()).unwrap();
    put(&mut st, &mut core, vec![Rec::Promise { db: 1, epoch: 42 }]);
    put(&mut st, &mut core, (1..=30).map(|i| ent(i, 42, 1)).collect());
    st.sync().unwrap();
    st.gc(&core).unwrap(); // rolls (segment full)
    put(&mut st, &mut core, vec![Rec::Trim { db: 1, upto: 30 }]);
    st.sync().unwrap();
    let deleted = st.gc(&core).unwrap();
    assert!(deleted >= 1, "the first segment must be deleted");
    drop(st);
    let (_, core2, _) = open(d.path(), cfg).unwrap();
    core2.dbs.get(&1).map_or(0, |d| d.promised)
}

#[test]
fn fence_survives_segment_deletion() {
    assert_eq!(fence_case(small()), 42);
    let mut bad = small();
    bad.bugs.no_fence_table = true;
    assert_ne!(fence_case(bad), 42, "twin: without the fence table the promise is forgotten");
}

/// Segment 1: entries 1..10 (epoch 1). Segment 2: a new writer truncates from 8 and writes 8..9 (epoch 2); trim to 9.
/// Segment 2 alone is "dead" (all its entries ≤ 9) but segment 1 is not (entry 10 > 9 is still there on disk).
fn prefix_case(cfg: StoreCfg) -> (NodeCore, NodeCore) {
    let d = tempfile::tempdir().unwrap();
    let mut cfg = cfg;
    cfg.segment_max = 1;
    let (mut st, mut core, _) = open(d.path(), cfg.clone()).unwrap();
    put(&mut st, &mut core, (1..=10).map(|i| ent(i, 1, 1)).collect());
    st.sync().unwrap();
    st.gc(&core).unwrap();
    put(&mut st, &mut core, vec![Rec::Truncate { db: 1, from: 8 }, ent(8, 2, 2), ent(9, 2, 2)]);
    st.sync().unwrap();
    st.gc(&core).unwrap();
    put(&mut st, &mut core, vec![Rec::Trim { db: 1, upto: 9 }]);
    st.sync().unwrap();
    st.gc(&core).unwrap();
    drop(st);
    let (_, core2, _) = open(d.path(), cfg).unwrap();
    (core, core2)
}

#[test]
fn segments_deleted_only_as_a_prefix() {
    let (live, recovered) = prefix_case(small());
    assert_eq!(live.dbs, recovered.dbs);
    let mut bad = small();
    bad.bugs.gc_not_prefix = true;
    let (live, recovered) = prefix_case(bad);
    assert_ne!(live.dbs, recovered.dbs, "twin: deleting segment 2 alone resurrects entry 10 of epoch 1");
    assert_eq!(recovered.dbs[&1].last(), 10);
}

fn fault_fs() -> (Arc<dyn Fs>, Arc<Faults>) {
    let f = Arc::new(Faults::default());
    (Arc::new(FaultFs { faults: f.clone() }), f)
}

#[test]
fn fsync_error_poisons() {
    let d = tempfile::tempdir().unwrap();
    let (fs, faults) = fault_fs();
    let (mut st, mut core, _) = Store::open(d.path(), fs.clone(), small()).unwrap();
    put(&mut st, &mut core, vec![ent(1, 1, 1)]);
    faults.fail_next_sync.store(true, SeqCst);
    assert!(matches!(st.sync(), Err(StoreError::Poisoned(_))));
    // even though the next fsync would work, nothing more is accepted: the process must exit and recover
    assert!(matches!(st.write(&[ent(2, 1, 1)]), Err(StoreError::Poisoned(_))));
    assert!(matches!(st.sync(), Err(StoreError::Poisoned(_))));
    // twin: with the ignore_fsync_error mutation the store carries on
    let d2 = tempfile::tempdir().unwrap();
    let cfg = StoreCfg { m: Mutations { ignore_fsync_error: true, ..Default::default() }, ..small() };
    let (mut st, mut core, _) = Store::open(d2.path(), fs, cfg).unwrap();
    put(&mut st, &mut core, vec![ent(1, 1, 1)]);
    faults.fail_next_sync.store(true, SeqCst);
    assert!(st.sync().is_ok());
    assert!(st.write(&[ent(2, 1, 1)]).is_ok());
}

#[test]
fn disk_full_poisons() {
    let d = tempfile::tempdir().unwrap();
    let (fs, faults) = fault_fs();
    let (mut st, mut core, _) = Store::open(d.path(), fs.clone(), small()).unwrap();
    put(&mut st, &mut core, vec![ent(1, 1, 1)]);
    st.sync().unwrap();
    faults.full.store(true, SeqCst);
    assert!(matches!(st.write(&[ent(2, 1, 2)]), Err(StoreError::Poisoned(_))));
    faults.full.store(false, SeqCst);
    assert!(matches!(st.sync(), Err(StoreError::Poisoned(_))));
    drop(st);
    // the half-written record is a torn tail: recovery keeps record 1 only
    let (_, core2, rep) = Store::open(d.path(), fs, small()).unwrap();
    assert!(rep.torn_tail_bytes > 0);
    assert_eq!(core2.dbs[&1].last(), 1);
}
