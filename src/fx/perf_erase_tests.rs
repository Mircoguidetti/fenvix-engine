//! perf-erase: the erasure batch (several erasures, one scrub) and its commit through the compactor.
//!
//! - `a_batch_keeps_a_document_written_after_its_own_erasure`: in a batch, each erasure keeps its own S_e: a customer
//!   written again after its erasure (seq above its S_e, below another erasure's) keeps the new document. Control: the
//!   same batch built with the highest S_e for every erasure erases it (an acknowledged write lost).
//! - `a_reseal_planned_before_a_scrub_cannot_bring_erased_rows_back` (both orders): a reseal planned on a version from
//!   before the erasure's deletes were absorbed reads the customer's documents live; committed after the scrub (which
//!   removed their delete envelopes) it would make them readable again; committed before it, the scrub must not publish
//!   over its sealed run. Control: `SCRUB_GUARD_OFF` makes the erased document readable again in both orders.

use super::compactor::{self, Compactor, Knobs};
use super::erase::{self, Batch, Targets};
use super::tests::*;
use super::*;
use crate::lifecycle::{Op, Tenant};
use crate::runfmt::{CustomerKind, RunOptions};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Duration;

fn knobs() -> Knobs {
    Knobs {
        theta: 1e9,
        r3: false,
        r3_gets_per_gib: 470.0,
        r2_age: None,
        tier_t: 64,
        tier_unit: 2 << 10,
        absorb_min_l0: 1_000,
        absorb_max_l0: 64,
        local_max_bytes: 0,
        split_bytes: 1 << 30,
        merge_bytes: 0,
        min_seal_bytes: 0,
        sealed: RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2),
        delta: RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2),
        drop: false,
        fold_one_run: false,
        fold_max_runs: 6,
        fold_at_l0: 6,
        lease: Duration::from_millis(400),
        remote_lease: Duration::from_millis(3600),
        serving_lease: Duration::from_millis(300),
        put_timeout: Duration::from_millis(250),
        gc_grace: Duration::from_secs(3600),
        version_retention: Duration::from_millis(0),
        gc_every: Duration::from_secs(3600),
        tick: Duration::from_millis(10),
        reseal_parallel: 1,
        read_ahead: 64 << 10,
        derive: None,
    }
}

fn cust_range(c: usize) -> (Vec<u8>, Vec<u8>) {
    let k = ckey(c, 0);
    let lo = k[..k.len() - "msgs/d/00000".len()].to_vec();
    let mut hi = lo.clone();
    *hi.last_mut().unwrap() += 1;
    (lo, hi)
}

fn targets(c: usize) -> Targets {
    let mut t = Targets::default();
    t.ranges.push(cust_range(c));
    t
}

async fn flush_absorb(t: &mut Tenant, c: &Compactor) {
    t.fx_writer_db().unwrap().flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
    while c.absorb_once().await.unwrap() > 0 {}
    t.fx_sync();
}

/// A writer with a first version of several groups (customers 5..40 sealed), and its compactor, paused (the test runs
/// every job itself).
async fn setup(store: &Arc<dyn ObjectStore>, id: &str) -> (Tenant, Arc<Compactor>) {
    enable();
    hold::HOLD_LIFE_MS.store(3000, Relaxed);
    view::clear_cache();
    let mut t = lc(store).create(id).await.unwrap();
    let mut ops = Vec::new();
    for c in 5..40 {
        for i in 0..20 {
            ops.push(Op::Put(ckey(c, i), vec![b'b'; 100]));
        }
    }
    t.apply(ops).await.unwrap();
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let c = Compactor::start(&mut t, Arc::clone(store), knobs(), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    c.paused.store(true, Relaxed);
    assert!(t.fx_view().unwrap().groups.len() >= 2, "the test needs several groups");
    (t, c)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_bounds_are_per_erasure() {
    let mut a = Targets::default();
    a.ranges.push((b"a".to_vec(), b"c".to_vec()));
    a.add_key(b"x1".to_vec());
    let mut b = Targets::default();
    b.ranges.push((b"b".to_vec(), b"d".to_vec()));
    b.add_key(b"x1".to_vec());
    b.add_key(b"y".to_vec());
    let bt = Batch::new(&[(&a, 10), (&b, 20)]);
    assert_eq!(bt.max_s_e, 20);
    assert_eq!(bt.bound(b"a5"), Some(10));
    assert_eq!(bt.bound(b"b5"), Some(20)); // both ranges: the highest
    assert_eq!(bt.bound(b"c5"), Some(20));
    assert_eq!(bt.bound(b"d"), None);
    assert_eq!(bt.bound(b"x1"), Some(20));
    assert_eq!(bt.bound(b"y"), Some(20));
    assert_eq!(bt.bound(b"z"), None);
    assert!(bt.hit(b"a5", 10) && !bt.hit(b"a5", 11) && bt.hit(b"b5", 15));
    assert!(bt.meets(b"0", b"a") && bt.meets(b"c9", b"c9") && bt.meets(b"x0", b"x2") && !bt.meets(b"d", b"x0") && !bt.meets(b"y0", b"z"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_keeps_a_document_written_after_its_own_erasure() {
    let _s = SERIAL.lock().await;
    for control in [false, true] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = if control { "fx-pe-batch-ctl" } else { "fx-pe-batch" };
        let (mut t, c) = setup(&store, id).await;
        // customers 0 and 1 written, absorbed
        let mut ops = Vec::new();
        for cu in 0..2 {
            for i in 0..10 {
                ops.push(Op::Put(ckey(cu, i), vec![b'a'; 80]));
            }
        }
        t.apply(ops).await.unwrap();
        flush_absorb(&mut t, &c).await;
        // erase customer 0, then it writes again, then erase customer 1 (a higher S_e)
        t.apply((0..10).map(|i| Op::Delete(ckey(0, i))).collect()).await.unwrap();
        let e0 = t.fx_erasure_register("msgs", b"c00", &targets(0), None).await.unwrap().unwrap();
        t.apply(vec![Op::Put(ckey(0, 99), b"written again after its erasure".to_vec())]).await.unwrap();
        t.apply((0..10).map(|i| Op::Delete(ckey(1, i))).collect()).await.unwrap();
        let e1 = t.fx_erasure_register("msgs", b"c01", &targets(1), None).await.unwrap().unwrap();
        assert!(e0.s_e < e1.s_e);
        flush_absorb(&mut t, &c).await;
        let base = c.current().await;
        assert!(base.x_d >= e1.s_e);
        let (t0, t1) = (targets(0), targets(1));
        let batch = if control { Batch::new(&[(&t0, e1.s_e), (&t1, e1.s_e)]) } else { Batch::new(&[(&t0, e0.s_e), (&t1, e1.s_e)]) };
        let v = view::View::new(Arc::clone(&store), base.clone(), None).unwrap();
        let s = erase::scrub_version(&store, id, &v, base.version + 1, base.writer_epoch, &batch, &erase::ScrubOpts::default()).await.unwrap();
        assert!(s.rows > 0, "the batch erased nothing");
        let paths: std::collections::HashSet<&String> = s.retired.iter().collect();
        assert_eq!(paths.len(), s.retired.len(), "a run rewritten twice in one batch");
        assert!(c.commit_scrub(&base, &s, batch.max_s_e).await.unwrap().is_some());
        t.fx_sync();
        let again = t.get(ckey(0, 99)).await.unwrap();
        let erased0 = t.get(ckey(0, 3)).await.unwrap();
        let erased1 = t.get(ckey(1, 3)).await.unwrap();
        c.stop().await;
        assert!(erased0.is_none() && erased1.is_none(), "an erased document is readable");
        if control {
            assert!(again.is_none(), "control: the highest S_e for every erasure must erase the document written after customer 0's erasure");
        } else {
            assert_eq!(again.as_deref(), Some(&b"written again after its erasure"[..]), "the document written after its own erasure was erased (an acknowledged write lost)");
        }
    }
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
}

/// `scrub_first`: the scrub commits before the old reseal (the reseal must abandon); else the reseal commits first and
/// the scrub, made on the version before it, must abandon.
async fn race(scrub_first: bool, guard_off: bool) -> (Option<bytes::Bytes>, bool, bool) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = format!("fx-pe-race-{scrub_first}-{guard_off}");
    let (mut t, c) = setup(&store, &id).await;
    // customer 0 (the first group) written after the seal: only in delta runs
    t.apply((0..10).map(|i| Op::Put(ckey(0, i), vec![b'e'; 80])).collect()).await.unwrap();
    flush_absorb(&mut t, &c).await;
    let v0 = c.current().await; // a reseal planned now reads customer 0 live
    t.apply((0..10).map(|i| Op::Delete(ckey(0, i))).collect()).await.unwrap();
    let e = t.fx_erasure_register("msgs", b"c00", &targets(0), None).await.unwrap().unwrap();
    flush_absorb(&mut t, &c).await;
    let base = c.current().await;
    assert!(base.x_d >= e.s_e);
    let tg = targets(0);
    let batch = Batch::single(&tg, e.s_e);
    let v = view::View::new(Arc::clone(&store), base.clone(), None).unwrap();
    let s = erase::scrub_version(&store, &id, &v, base.version + 1, base.writer_epoch, &batch, &erase::ScrubOpts::default()).await.unwrap();
    assert!(s.rows > 0);
    compactor::SCRUB_GUARD_OFF.store(guard_off, Relaxed);
    let (scrubbed, resealed) = if scrub_first {
        let a = c.commit_scrub(&base, &s, batch.max_s_e).await.unwrap().is_some();
        let b = c.reseal_planned_on(&v0, 0).await.unwrap();
        (a, b)
    } else {
        let b = c.reseal_planned_on(&v0, 0).await.unwrap();
        let a = c.commit_scrub(&base, &s, batch.max_s_e).await.unwrap().is_some();
        (a, b)
    };
    compactor::SCRUB_GUARD_OFF.store(false, Relaxed);
    t.fx_sync();
    let got = t.get(ckey(0, 3)).await.unwrap();
    c.stop().await;
    (got, scrubbed, resealed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reseal_planned_before_a_scrub_cannot_bring_erased_rows_back() {
    let _s = SERIAL.lock().await;
    // the scrub first: the old reseal abandons
    let (got, scrubbed, resealed) = race(true, false).await;
    assert!(scrubbed && !resealed && got.is_none(), "scrub first: scrubbed {scrubbed}, old reseal committed {resealed}, erased document readable {}", got.is_some());
    // the old reseal first: the scrub, made before it, abandons (the next pass scrubs the new version)
    let (got, scrubbed, resealed) = race(false, false).await;
    assert!(!scrubbed && resealed && got.is_none(), "reseal first: scrubbed {scrubbed}, old reseal committed {resealed}, erased document readable {}", got.is_some());
    // controls: without the guard the erased document is readable again, in both orders
    let (got, scrubbed, resealed) = race(true, true).await;
    assert!(scrubbed && resealed && got.is_some(), "control (scrub first, no guard): the test must see the erased document come back");
    let (got, scrubbed, resealed) = race(false, true).await;
    assert!(scrubbed && resealed && got.is_some(), "control (reseal first, no guard): the test must see the erased document come back");
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
}

/// A scrub in parts (a budget that stops it after each run) ends like a whole one: every part is committed, the next
/// starts from the version just published, and at the end no run of the newest version holds the erased customers,
/// while their neighbours and the document written after its own erasure are all there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scrub_in_parts_ends_like_a_whole_one() {
    let _s = SERIAL.lock().await;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-pe-parts";
    let (mut t, c) = setup(&store, id).await;
    // two customers in different groups, in sealed runs and in delta runs
    let erased = [7usize, 30];
    let mut ops = Vec::new();
    for cu in erased {
        for i in 20..30 {
            ops.push(Op::Put(ckey(cu, i), vec![b'n'; 60]));
        }
    }
    t.apply(ops).await.unwrap();
    flush_absorb(&mut t, &c).await;
    let mut items = Vec::new();
    for cu in erased {
        t.apply((0..30).map(|i| Op::Delete(ckey(cu, i))).collect()).await.unwrap();
        let e = t.fx_erasure_register("msgs", format!("c{cu:02}").as_bytes(), &targets(cu), None).await.unwrap().unwrap();
        items.push((targets(cu), e.s_e));
    }
    t.apply(vec![Op::Put(ckey(7, 99), b"after".to_vec())]).await.unwrap();
    flush_absorb(&mut t, &c).await;
    let refs: Vec<(&Targets, u64)> = items.iter().map(|(t, s)| (t, *s)).collect();
    let batch = Batch::new(&refs);
    let mut opts = erase::ScrubOpts::default();
    opts.budget = Some(Duration::from_nanos(1));
    let mut parts = 0;
    loop {
        let base = c.current().await;
        let v = view::View::new(Arc::clone(&store), base.clone(), None).unwrap();
        let s = erase::scrub_version(&store, id, &v, base.version + 1, base.writer_epoch, &batch, &opts).await.unwrap();
        if s.version.is_some() {
            assert!(c.commit_scrub(&base, &s, batch.max_s_e).await.unwrap().is_some(), "a part was refused with nothing else running");
        }
        parts += 1;
        if s.complete {
            break;
        }
        assert!(parts < 200);
    }
    assert!(parts >= 3, "the budget should have cut the scrub into parts ({parts})");
    t.fx_sync();
    let newest = c.current().await;
    for l in newest.groups.iter().filter_map(|g| g.sealed.as_ref()).chain(newest.delta.iter()) {
        for r in compactor::run_rows(&store, l).await.unwrap() {
            assert!(!(batch.hit(&r.key, r.seq)), "a row of an erased customer is left in run {}", l.path);
        }
    }
    for cu in erased {
        for i in 0..30 {
            assert!(t.get(ckey(cu, i)).await.unwrap().is_none());
        }
    }
    assert_eq!(t.get(ckey(7, 99)).await.unwrap().as_deref(), Some(&b"after"[..]));
    for cu in [5usize, 6, 8, 29, 31, 39] {
        for i in 0..20 {
            assert!(t.get(ckey(cu, i)).await.unwrap().is_some(), "a neighbour's document is gone");
        }
    }
    c.stop().await;
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
}

/// erase-cost-1tb: a scrub in parts must never make an erased document readable between two parts. A customer written
/// after the seal has live rows in an older delta run; its erasure's delete envelopes land in a newer delta run. On
/// integration-4 a part rewrote the newer delta run (newest first) without the envelopes while the older one still held
/// the live rows: the published version made the erased documents readable again until the next part (MEASURED by this
/// test before the fix: rows 20..29 readable after the second part, index 1). Now the parts run in two phases (every target row but the
/// envelopes, then the envelopes). Control: `CONTROL_PARTS_DROP_ENVELOPES` (one phase, as before) must see them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_part_of_a_scrub_makes_an_erased_document_readable() {
    let _s = SERIAL.lock().await;
    for control in [false, true] {
        erase::CONTROL_PARTS_DROP_ENVELOPES.store(control, Relaxed);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = if control { "fx-pe-parts-read-ctl" } else { "fx-pe-parts-read" };
        let (mut t, c) = setup(&store, id).await;
        let cu = 7usize;
        // rows 20..30 written after the seal: live in an older delta run
        t.apply((20..30).map(|i| Op::Put(ckey(cu, i), vec![b'n'; 60])).collect()).await.unwrap();
        flush_absorb(&mut t, &c).await;
        // the erasure: its envelopes land in a newer delta run
        t.apply((0..30).map(|i| Op::Delete(ckey(cu, i))).collect()).await.unwrap();
        let e = t.fx_erasure_register("msgs", format!("c{cu:02}").as_bytes(), &targets(cu), None).await.unwrap().unwrap();
        flush_absorb(&mut t, &c).await;
        assert!(c.current().await.delta.len() >= 2, "the test needs two delta runs");
        let tg = targets(cu);
        let batch = Batch::single(&tg, e.s_e);
        let mut opts = erase::ScrubOpts::default();
        opts.budget = Some(Duration::from_nanos(1));
        opts.keep_envelopes = true;
        let mut readable = Vec::new();
        for part in 0..200 {
            let base = c.current().await;
            let v = view::View::new(Arc::clone(&store), base.clone(), None).unwrap();
            let s = erase::scrub_version(&store, id, &v, base.version + 1, base.writer_epoch, &batch, &opts).await.unwrap();
            if s.version.is_some() {
                assert!(c.commit_scrub(&base, &s, batch.max_s_e).await.unwrap().is_some());
            }
            t.fx_sync();
            for i in 0..30 {
                if t.get(ckey(cu, i)).await.unwrap().is_some() {
                    readable.push((part, i));
                }
            }
            if s.complete {
                if !opts.keep_envelopes {
                    break;
                }
                opts.keep_envelopes = false;
            }
        }
        // at the end no row of the customer is left in the newest version
        let newest = c.current().await;
        for l in newest.groups.iter().filter_map(|g| g.sealed.as_ref()).chain(newest.delta.iter()) {
            for r in compactor::run_rows(&store, l).await.unwrap() {
                assert!(!batch.hit(&r.key, r.seq), "a row of the erased customer is left in run {}", l.path);
            }
        }
        c.stop().await;
        erase::CONTROL_PARTS_DROP_ENVELOPES.store(false, Relaxed);
        if control {
            assert!(!readable.is_empty(), "control: one phase (envelopes dropped with the rest) must make an erased document readable between parts");
        } else {
            assert!(readable.is_empty(), "erased documents readable between two parts of a scrub (part, row): {readable:?}");
        }
    }
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
}

/// erase-cost-1tb: the ride and the forced path, step by step, on one writer. Customer 7 has rows in its group's sealed
/// run and live rows in an older delta run; its erasure's envelopes land in a newer delta run; then it writes again
/// (seq above S_e, new data). Customer 30 (another group) is erased too, with one S_e higher. At every step: no erased
/// document readable, the document written after the erasure and every neighbour readable.
/// 1. the groups owing them are resealed (the forced reseal = an ordinary reseal brought forward): no group owes them;
/// 2. the delta runs listed then that may hold their dead rows are cleansed: no run holds a target row, the probe says
///    clean.
/// Controls: a cleanse that also drops envelopes above X_g, run before the reseals, makes erased documents readable; a
/// cleanse that keeps the dead rows leaves the probe finding them.
async fn ride_case(drop_live_control: bool, keep_dead_control: bool) -> (usize, Vec<bool>) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = format!("fx-ride-{drop_live_control}-{keep_dead_control}");
    let (mut t, c) = setup(&store, &id).await;
    let erased = [7usize, 30];
    t.apply(erased.iter().flat_map(|&cu| (20..30).map(move |i| Op::Put(ckey(cu, i), vec![b'n'; 60]))).collect()).await.unwrap();
    flush_absorb(&mut t, &c).await;
    let mut items: Vec<(Targets, u64)> = Vec::new();
    for cu in erased {
        t.apply((0..30).map(|i| Op::Delete(ckey(cu, i))).collect()).await.unwrap();
        let e = t.fx_erasure_register("msgs", format!("c{cu:02}").as_bytes(), &targets(cu), None).await.unwrap().unwrap();
        items.push((targets(cu), e.s_e));
    }
    // the race: the customer writes again right after its erasure
    t.apply(vec![Op::Put(ckey(7, 99), b"after".to_vec())]).await.unwrap();
    flush_absorb(&mut t, &c).await;
    let mut readable = 0usize;
    async fn reads(t: &mut Tenant, erased: &[usize]) -> usize {
        t.fx_sync();
        let mut n = 0;
        for &cu in erased {
            for i in 0..30 {
                n += t.get(ckey(cu, i)).await.unwrap().is_some() as usize;
            }
        }
        assert_eq!(t.get(ckey(7, 99)).await.unwrap().as_deref(), Some(&b"after"[..]), "the document written after its own erasure is gone (an acknowledged write lost)");
        for cu in [5usize, 6, 8, 29, 31, 39] {
            for i in 0..20 {
                assert!(t.get(ckey(cu, i)).await.unwrap().is_some(), "a neighbour's document is gone");
            }
        }
        n
    }
    readable += reads(&mut t, &erased).await;
    let v = c.current().await;
    let mut gids = std::collections::BTreeSet::new();
    for (tg, s) in &items {
        gids.extend(erase::owing_groups(&v, tg, *s).unwrap());
    }
    assert!(!gids.is_empty(), "the erased customers' groups must owe them before any reseal");
    if drop_live_control {
        compactor::CONTROL_CLEANSE_DROPS_LIVE.store(true, Relaxed);
        for d in c.current().await.delta.iter().map(|d| d.path.clone()).collect::<Vec<_>>() {
            c.cleanse_delta(&d).await.unwrap();
        }
        compactor::CONTROL_CLEANSE_DROPS_LIVE.store(false, Relaxed);
        readable += reads(&mut t, &erased).await;
    }
    for gid in gids {
        assert!(c.force_reseal_group(gid).await.unwrap(), "the forced reseal of group {gid} did not commit");
        readable += reads(&mut t, &erased).await;
    }
    let v = c.current().await;
    let mut owed = std::collections::BTreeSet::new();
    for (tg, s) in &items {
        assert!(erase::owing_groups(&v, tg, *s).unwrap().is_empty(), "a group still owes an erasure after its forced reseal");
        owed.extend(erase::owed_delta(&v, tg, *s).unwrap());
    }
    compactor::CONTROL_CLEANSE_KEEPS_DEAD.store(keep_dead_control, Relaxed);
    for p in owed {
        c.cleanse_delta(&p).await.unwrap();
        readable += reads(&mut t, &erased).await;
    }
    compactor::CONTROL_CLEANSE_KEEPS_DEAD.store(false, Relaxed);
    let v = c.current().await;
    let view = view::View::new(Arc::clone(&store), v.clone(), None).unwrap();
    let refs: Vec<(&Targets, u64)> = items.iter().map(|(t, s)| (t, *s)).collect();
    let clean = erase::confirm_clean(&store, &view, &refs, None).await.unwrap();
    if !keep_dead_control && !drop_live_control {
        // the proof by reading every row of every run of the newest version
        let batch = Batch::new(&refs);
        for l in v.groups.iter().filter_map(|g| g.sealed.as_ref()).chain(v.delta.iter()) {
            for r in compactor::run_rows(&store, l).await.unwrap() {
                assert!(!batch.hit(&r.key, r.seq), "a row of an erased customer is left in run {}", l.path);
            }
        }
        assert!(owed_after(&v, &items).is_empty());
    }
    c.stop().await;
    (readable, clean)
}

fn owed_after(v: &manifest::Version, items: &[(Targets, u64)]) -> Vec<String> {
    items.iter().flat_map(|(t, s)| erase::owed_delta(v, t, *s).unwrap()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forced_reseals_and_cleanses_erase_without_a_scrub() {
    let _s = SERIAL.lock().await;
    let (readable, clean) = ride_case(false, false).await;
    assert_eq!(readable, 0, "an erased document was readable during the forced path");
    assert_eq!(clean, vec![true, true], "the probe still finds rows of the erased customers");
    // controls
    let (readable, _) = ride_case(true, false).await;
    assert!(readable > 0, "control: a cleanse that drops envelopes above X_g must make erased documents readable");
    let (_, clean) = ride_case(false, true).await;
    assert!(clean.iter().any(|c| !c), "control: a cleanse that keeps the dead rows must leave the probe finding them");
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
}

/// erase-cost-1tb: the bookkeeping is per erasure: an erasure whose group was resealed is clear while another, in a
/// group not resealed, still owes; the probe attributes rows to the right erasure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owing_groups_and_the_probe_are_per_erasure() {
    let _s = SERIAL.lock().await;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-ride-per-erasure";
    let (mut t, c) = setup(&store, id).await;
    let mut items: Vec<(Targets, u64)> = Vec::new();
    for cu in [7usize, 30] {
        t.apply((0..20).map(|i| Op::Delete(ckey(cu, i))).collect()).await.unwrap();
        let e = t.fx_erasure_register("msgs", format!("c{cu:02}").as_bytes(), &targets(cu), None).await.unwrap().unwrap();
        items.push((targets(cu), e.s_e));
    }
    flush_absorb(&mut t, &c).await;
    let v = c.current().await;
    let g7 = erase::owing_groups(&v, &items[0].0, items[0].1).unwrap();
    let g30 = erase::owing_groups(&v, &items[1].0, items[1].1).unwrap();
    assert_eq!(g7.len(), 1);
    assert_eq!(g30.len(), 1);
    assert_ne!(g7, g30, "the test needs the two customers in two groups");
    assert!(c.force_reseal_group(g7[0]).await.unwrap());
    let v = c.current().await;
    assert!(erase::owing_groups(&v, &items[0].0, items[0].1).unwrap().is_empty());
    assert_eq!(erase::owing_groups(&v, &items[1].0, items[1].1).unwrap(), g30);
    let view = view::View::new(Arc::clone(&store), v.clone(), None).unwrap();
    let refs: Vec<(&Targets, u64)> = items.iter().map(|(t, s)| (t, *s)).collect();
    let clean = erase::confirm_clean(&store, &view, &refs, None).await.unwrap();
    // customer 7: its group resealed, its envelopes dead in the delta run (the probe still sees them there, at or below
    // S_e: not clean until the run is cleansed); customer 30: its sealed rows are still there
    assert_eq!(clean, vec![false, false]);
    for p in erase::owed_delta(&v, &items[0].0, items[0].1).unwrap() {
        c.cleanse_delta(&p).await.unwrap();
    }
    let v = c.current().await;
    let view = view::View::new(Arc::clone(&store), v.clone(), None).unwrap();
    let clean = erase::confirm_clean(&store, &view, &refs, None).await.unwrap();
    assert_eq!(clean, vec![true, false], "customer 7 clean, customer 30 not");
    t.fx_sync();
    for i in 0..20 {
        assert!(t.get(ckey(7, i)).await.unwrap().is_none() && t.get(ckey(30, i)).await.unwrap().is_none());
    }
    c.stop().await;
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
}

/// Review of erase-cost-1tb (2026-11-13): the two-phase scrub in parts protects only the erasures that went through
/// phase 1. `run_pass_scrub` (mongo-server/src/ctl/erase.rs) sets `keep_env = true` once per pass and re-lists the
/// pending erasures at every part, so an erasure absorbed while phase 1 runs joins the batch directly in phase 2
/// (`keep_envelopes = false`). If phase 2 then needs several parts, the original defect returns for it: a part drops its
/// envelopes from the newer delta run while the older one still holds its live rows. This test mirrors that loop:
/// customer 7 runs phase 1 to its end, then customer 30 is written, erased and absorbed, then phase 2 runs in parts for
/// both.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn review_an_erasure_joining_in_phase_two_is_not_protected() {
    // erase-cost-1tb fix: the passes follow `erase::PartsPlan` (the batch frozen at its first part), as
    // `run_pass_scrub` does. Control `CONTROL_UNFROZEN_BATCH`: the reviewer's sequence (B joins in phase 2) must show
    // B's rows readable between two parts.
    let _s = SERIAL.lock().await;
    for control in [false, true] {
        erase::CONTROL_UNFROZEN_BATCH.store(control, Relaxed);
        let readable = phase2_join_case(control).await;
        erase::CONTROL_UNFROZEN_BATCH.store(false, Relaxed);
        if control {
            assert!(!readable.is_empty(), "control: an erasure joining in phase 2 must be readable between parts");
        } else {
            assert!(readable.is_empty(), "erased documents readable between two parts (pass, part, customer, row): {readable:?}");
        }
    }
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
}

async fn phase2_join_case(control: bool) -> Vec<(usize, usize, usize, usize)> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = if control { "fx-review-phase2-join-ctl" } else { "fx-review-phase2-join" };
    let (mut t, c) = setup(&store, id).await;
    let (a, b) = (7usize, 30usize);
    // customer A: live rows in an older delta run, its envelopes in a newer one
    t.apply((20..30).map(|i| Op::Put(ckey(a, i), vec![b'n'; 60])).collect()).await.unwrap();
    flush_absorb(&mut t, &c).await;
    t.apply((0..30).map(|i| Op::Delete(ckey(a, i))).collect()).await.unwrap();
    let ea = t.fx_erasure_register("msgs", format!("c{a:02}").as_bytes(), &targets(a), None).await.unwrap().unwrap();
    flush_absorb(&mut t, &c).await;
    let (ta, tb) = (targets(a), targets(b));
    let mut eb: Option<erase::Erasure> = None;
    let mut readable = Vec::new();
    let mut done: std::collections::BTreeSet<String> = Default::default();
    // passes until every erasure is scrubbed (each pass: phase 1 then phase 2, in parts of one run)
    for pass in 0..4 {
        let mut plan = erase::PartsPlan::default();
        let mut opts = erase::ScrubOpts::default();
        opts.budget = Some(Duration::from_nanos(1));
        for part in 0..400 {
            // customer B arrives while A's phase 1 ran: live rows in an older delta run, envelopes in a newer one
            if eb.is_none() && pass == 0 && !plan.keep_envelopes {
                t.apply((20..30).map(|i| Op::Put(ckey(b, i), vec![b'n'; 60])).collect()).await.unwrap();
                flush_absorb(&mut t, &c).await;
                t.apply((0..30).map(|i| Op::Delete(ckey(b, i))).collect()).await.unwrap();
                eb = Some(t.fx_erasure_register("msgs", format!("c{b:02}").as_bytes(), &targets(b), None).await.unwrap().unwrap());
                flush_absorb(&mut t, &c).await;
            }
            let mut ready = vec![ea.id.clone()];
            ready.extend(eb.as_ref().map(|e| e.id.clone()));
            ready.retain(|x| !done.contains(x));
            let admitted = plan.admit(&ready);
            if admitted.is_empty() {
                break;
            }
            let mut items: Vec<(&Targets, u64)> = Vec::new();
            if admitted.contains(&ea.id) {
                items.push((&ta, ea.s_e));
            }
            if let Some(e) = eb.as_ref().filter(|e| admitted.contains(&e.id)) {
                items.push((&tb, e.s_e));
            }
            let batch = Batch::new(&items);
            opts.keep_envelopes = plan.keep_envelopes;
            let base = c.current().await;
            let v = view::View::new(Arc::clone(&store), base.clone(), None).unwrap();
            let s = erase::scrub_version(&store, id, &v, base.version + 1, base.writer_epoch, &batch, &opts).await.unwrap();
            if s.version.is_some() {
                assert!(c.commit_scrub(&base, &s, batch.max_s_e).await.unwrap().is_some());
            }
            t.fx_sync();
            for cu in [a, b] {
                for i in 0..30 {
                    if (cu == a || eb.is_some()) && t.get(ckey(cu, i)).await.unwrap().is_some() {
                        readable.push((pass, part, cu, i));
                    }
                }
            }
            if s.complete && !plan.keep_envelopes {
                done.extend(admitted);
                break;
            }
            plan.after_part(s.complete);
        }
        if eb.as_ref().is_some_and(|e| done.contains(&e.id)) && done.contains(&ea.id) {
            break;
        }
    }
    assert!(done.contains(&ea.id) && eb.as_ref().is_some_and(|e| done.contains(&e.id)), "both erasures must end scrubbed");
    c.stop().await;
    readable
}
