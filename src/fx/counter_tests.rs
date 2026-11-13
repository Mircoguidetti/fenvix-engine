//! Tests of compaction 2, step 6 (`counter.rs`): physical slices, the new-user filter and the staged publish of a
//! reseal. The reseal trigger of step 4 is stubbed: the tests call `Tenant::fx_publish` with `Reseal::All` or
//! `Reseal::Some`, which retires the groups' sealed runs exactly as a reseal of step 4 will.

use super::build::{BuildOpts, Reseal};
use super::counter::{self, SliceMode};
use super::tests::{apply, as_vec, enable, lc, model_range, publish, SERIAL};
use super::view;
use crate::lifecycle::Tenant;
use crate::runfmt::{CustomerKind, RunOptions};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

/// The customer component of customer `c` (layout v2: a length byte, then the bytes).
fn comp(c: usize) -> Vec<u8> {
    let name = format!("c{c:06}");
    let mut k = vec![name.len() as u8];
    k.extend(name.as_bytes());
    k
}

/// `u/{customer}msgs/d/{i}`: a document of customer c.
fn doc(c: usize, i: usize) -> Vec<u8> {
    let mut k = b"u/".to_vec();
    k.extend(comp(c));
    k.extend(format!("msgs/d/{i:06}").as_bytes());
    k
}

/// `u/{customer}msgs/x/ts/{i}`: the index entry of document i (index {customer, ts}).
fn idx(c: usize, i: usize) -> Vec<u8> {
    let mut k = b"u/".to_vec();
    k.extend(comp(c));
    k.extend(format!("msgs/x/ts/{i:06}").as_bytes());
    k
}

fn area(c: usize, a: &str) -> (Vec<u8>, Vec<u8>) {
    let mut lo = b"u/".to_vec();
    lo.extend(comp(c));
    lo.extend(a.as_bytes());
    let hi = crate::runfmt::prefix_successor(&lo).unwrap();
    (lo, hi)
}

fn cust_range(c: usize) -> (Vec<u8>, Vec<u8>) {
    area(c, "")
}

fn opts(reseal: Reseal, block: usize) -> BuildOpts {
    BuildOpts { reseal, group_bytes: 1 << 30, sealed: RunOptions::unchecked(block, 1 << 20, CustomerKind::V2), delta: RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2) }
}

/// Load `n` customers with `per` documents of `bytes` each, plus their index entries.
async fn load(t: &mut Tenant, m: &mut BTreeMap<Vec<u8>, Vec<u8>>, n: usize, per: usize, bytes: usize) {
    for c in 0..n {
        let mut ops = Vec::new();
        for i in 0..per {
            ops.push((doc(c, i), Some(vec![(c % 251) as u8; bytes])));
            ops.push((idx(c, i), Some(doc(c, i))));
        }
        apply(t, m, ops).await;
    }
}

/// The visit's read: the `k` newest documents of customer c, through the index, as the server's find does (an index
/// scan, then the documents by key). Returns the documents and checks every step against the model.
async fn newest(t: &Tenant, m: &BTreeMap<Vec<u8>, Vec<u8>>, c: usize, k: usize) -> usize {
    let (lo, hi) = area(c, "msgs/x/ts/");
    let ix = t.scan_range(&lo, Some(&hi), true, Some(k), 64 << 10).await.unwrap();
    assert_eq!(as_vec(ix.clone()), model_range(m, &lo, Some(&hi), true, Some(k)), "index scan of customer {c}");
    for (_, d) in &ix {
        let got = t.get(&d[..]).await.unwrap();
        assert_eq!(got.as_deref(), m.get(&d[..]).map(|v| &v[..]), "document of customer {c}");
    }
    ix.len()
}

struct Mode(u8);
impl Mode {
    fn set(m: SliceMode) -> Mode {
        let old = counter::SLICES.swap(m as u8, Relaxed);
        Mode(old)
    }
}
impl Drop for Mode {
    fn drop(&mut self) {
        counter::SLICES.store(self.0, Relaxed);
    }
}

/// Slices keep only the read's rows of a fetched block, answer later reads exactly with 0 GETs, and hold far less RAM
/// per customer than whole blocks. Whole blocks (mode Off) are the control: the same reads, more RAM.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slices_keep_only_the_read_area_and_answer_exactly() {
    let _s = SERIAL.lock().await;
    enable();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut t = lc(&store).create("fx-slices").await.unwrap();
    let mut m = BTreeMap::new();
    load(&mut t, &mut m, 120, 30, 200).await;
    publish(&mut t, &opts(Reseal::All, 32 << 10)).await;
    let hot: Vec<usize> = (0..120).step_by(6).collect();
    let mut per_mode = Vec::new();
    for mode in [SliceMode::Off, SliceMode::Customer, SliceMode::Area] {
        let _m = Mode::set(mode);
        view::clear_cache();
        for &c in &hot {
            assert_eq!(newest(&t, &m, c, 5).await, 5);
        }
        // the same reads again: everything from the counter
        let g0 = view::RUN_GETS.load(Relaxed);
        for &c in &hot {
            newest(&t, &m, c, 5).await;
        }
        assert_eq!(view::RUN_GETS.load(Relaxed) - g0, 0, "{mode:?}: hot reads need no GET");
        if mode != SliceMode::Off {
            // trimmed to what the visit relied on, the slices still answer the same visit with 0 GETs
            counter::trim_all();
            let g2 = view::RUN_GETS.load(Relaxed);
            for &c in &hot {
                newest(&t, &m, c, 5).await;
            }
            assert_eq!(view::RUN_GETS.load(Relaxed) - g2, 0, "{mode:?}: trimmed slices answer the same reads");
        }
        let hot_set: std::collections::HashSet<Vec<u8>> = hot.iter().map(|c| comp(*c)).collect();
        let per = counter::bytes_per_customer(CustomerKind::V2, Some(&hot_set));
        let held: f64 = hot.iter().map(|c| per.get(&comp(*c)).copied().unwrap_or(0.0)).sum::<f64>() / hot.len() as f64;
        let st = counter::stats();
        if mode != SliceMode::Off {
            assert_eq!(st.blocks, 0, "{mode:?}: one-customer reads keep no whole block");
            // every slice holds rows of one customer only
            let total_rows: f64 = per.values().sum();
            let hot_rows: f64 = hot.iter().map(|c| per.get(&comp(*c)).copied().unwrap_or(0.0)).sum();
            assert!((total_rows - hot_rows).abs() < 1.0, "{mode:?}: slices of other customers held");
        }
        eprintln!("mode {mode:?}: {held:.0} bytes per hot customer, cache {st:?}");
        per_mode.push(held);
    }
    // a customer has 30 x (200 + about 30) bytes of rows; whole 32 KiB blocks hold about 5 customers each; the visit
    // relies on 5 documents and 5 index entries of them
    assert!(per_mode[1] < per_mode[0] / 4.0, "customer slices {} against whole blocks {}", per_mode[1], per_mode[0]);
    // here a customer is one collection read whole (index and documents): area slices hold the same rows, in two
    // entries per block instead of one
    assert!(per_mode[2] <= per_mode[1] + 2.0 * (counter::ENTRY_OVERHEAD as f64 + 32.0), "area slices {} against customer slices {}", per_mode[2], per_mode[1]);
    t.abandon().await;
}

/// Random reads in every slice mode against the model, with writes and publishes (some groups resealed) between them:
/// 0 wrong. Ranges inside one area, inside one customer, across customers, and point reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slices_are_exact_under_writes_and_reseals() {
    let _s = SERIAL.lock().await;
    enable();
    let trim0 = counter::TRIM_MS.swap(u64::MAX, Relaxed);
    for (mode, trim) in [(SliceMode::Off, false), (SliceMode::Customer, false), (SliceMode::Area, false), (SliceMode::Customer, true), (SliceMode::Area, true)] {
        let _m = Mode::set(mode);
        view::clear_cache();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut t = lc(&store).create("fx-slices-model").await.unwrap();
        let mut m = BTreeMap::new();
        let mut rng = StdRng::seed_from_u64(6 + mode as u64);
        load(&mut t, &mut m, 60, 12, 120).await;
        let mut o = opts(Reseal::All, 4096);
        o.group_bytes = 24 << 10;
        publish(&mut t, &o).await;
        let mut wrong = 0;
        let mut reads = 0;
        for step in 0..3000 {
            match rng.gen_range(0..20) {
                0..=3 => {
                    // writes: new documents, rewrites, deletes
                    let c = rng.gen_range(0..70);
                    let i = rng.gen_range(0..20);
                    let v = if rng.gen_range(0..5) == 0 { None } else { Some(vec![rng.gen::<u8>(); rng.gen_range(1..300)]) };
                    apply(&mut t, &mut m, vec![(doc(c, i), v.clone()), (idx(c, i), v.map(|_| doc(c, i)))]).await;
                }
                4 => {
                    let ngroups = t.fx_view().map_or(1, |v| v.groups.len());
                    let r = match rng.gen_range(0..3) {
                        0 => Reseal::All,
                        1 => Reseal::None,
                        _ => Reseal::Some(vec![rng.gen_range(0..ngroups)]),
                    };
                    let mut o = opts(r, 4096);
                    o.group_bytes = 24 << 10;
                    let _ = t.fx_publish(&o, None).await.unwrap();
                }
                5..=9 => {
                    let c = rng.gen_range(0..70);
                    let k = if rng.gen() { doc(c, rng.gen_range(0..20)) } else { idx(c, rng.gen_range(0..20)) };
                    reads += 1;
                    if t.get(&k).await.unwrap().map(|b| b.to_vec()) != m.get(&k).cloned() {
                        wrong += 1;
                    }
                }
                _ => {
                    let c = rng.gen_range(0..70);
                    let (lo, hi) = match rng.gen_range(0..4) {
                        0 => area(c, "msgs/d/"),
                        1 => area(c, "msgs/x/ts/"),
                        2 => cust_range(c),
                        _ => (cust_range(c).0, cust_range((c + rng.gen_range(1..4)).min(75)).1),
                    };
                    let rev = rng.gen();
                    let lim = if rng.gen() { Some(rng.gen_range(1..15)) } else { None };
                    reads += 1;
                    let got = as_vec(t.scan_range(&lo, Some(&hi), rev, lim, rng.gen_range(1..8) * 4096).await.unwrap());
                    if got != model_range(&m, &lo, Some(&hi), rev, lim) {
                        wrong += 1;
                    }
                }
            }
            if trim {
                // slices trimmed to what the reads so far relied on: later reads meet trimmed slices and refetch
                counter::trim_all();
            }
            if step % 500 == 499 {
                view::clear_cache();
            }
        }
        eprintln!("mode {mode:?}, trimmed after every step {trim}: {reads} reads, {wrong} wrong, cache {:?}, refetches {}", counter::stats(), counter::REFETCHES.load(Relaxed));
        assert_eq!(wrong, 0, "{mode:?} trim {trim}");
        t.abandon().await;
    }
    counter::TRIM_MS.store(trim0, Relaxed);
}

/// The new-user filter: customers that no run holds cost 0 GETs on our runs, point and range reads alike, except the
/// filter's false positives. Every absent customer sorts inside the runs' key ranges and is checked against two runs
/// (the sealed run and a delta run). With step 1's 10 bits per customer (about 0.8% per run) the measured rate is
/// reported; with step 6's default of 16 bits it must be at most 1% per read. Present customers are never missed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absent_customers_cost_no_gets() {
    let _s = SERIAL.lock().await;
    enable();
    let _m = Mode::set(SliceMode::Area);
    let bits_before = crate::runfmt::FILTER_BITS_PER_CUSTOMER.load(Relaxed);
    for bits in [10u64, 16] {
        crate::runfmt::FILTER_BITS_PER_CUSTOMER.store(bits, Relaxed);
        let fp = absent_case(bits).await;
        if bits >= 16 {
            assert!(fp <= 0.01, "{bits} bits: false positives {:.2}%", fp * 100.0);
        }
    }
    crate::runfmt::FILTER_BITS_PER_CUSTOMER.store(bits_before, Relaxed);
}

async fn absent_case(bits: u64) -> f64 {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut t = lc(&store).create("fx-filter").await.unwrap();
    let mut m = BTreeMap::new();
    // present customers have even numbers, absent ones odd numbers: every absent customer sorts inside the runs' key
    // ranges, so only the filter can spare the GET
    for c in (0..20_000).step_by(2) {
        apply(&mut t, &mut m, vec![(doc(c, 0), Some(vec![2u8; 60])), (idx(c, 0), Some(doc(c, 0))), (doc(c, 1), Some(vec![3u8; 60]))]).await;
    }
    publish(&mut t, &opts(Reseal::All, 4096)).await;
    // more customers in a delta run that spans the whole key range, so an absent customer meets two runs
    let mut ops = Vec::new();
    for c in (0..20_000).step_by(40) {
        ops.push((doc(c, 7), Some(vec![4u8; 60])));
    }
    apply(&mut t, &mut m, ops).await;
    publish(&mut t, &opts(Reseal::None, 4096)).await;
    // open every run's footer first (the maps are in RAM, as on a warm waiter)
    for c in [0usize, 19_998] {
        let (lo, hi) = cust_range(c);
        t.scan_range(&lo, Some(&hi), false, None, 4096).await.unwrap();
    }
    view::clear_cache();
    let (c0, s0) = (counter::FILTER_CHECKS.load(Relaxed), counter::FILTER_SKIPS.load(Relaxed));
    let mut n = 0;
    let mut with_get = 0;
    for c in (1..20_000).step_by(2) {
        n += 1;
        let g0 = view::RUN_GETS.load(Relaxed);
        assert!(t.get(&doc(c, 0)).await.unwrap().is_none());
        let (lo, hi) = area(c, "msgs/x/ts/");
        assert!(t.scan_range(&lo, Some(&hi), true, Some(20), 64 << 10).await.unwrap().is_empty());
        let (lo, hi) = cust_range(c);
        assert!(t.scan_range(&lo, Some(&hi), false, None, 64 << 10).await.unwrap().is_empty());
        if view::RUN_GETS.load(Relaxed) > g0 {
            with_get += 1;
        }
    }
    let fp = with_get as f64 / n as f64;
    eprintln!(
        "absent customers, {bits} bits per customer: {with_get} of {n} touched a run with a GET ({:.2}%); filter checks {}, said no {}",
        fp * 100.0,
        counter::FILTER_CHECKS.load(Relaxed) - c0,
        counter::FILTER_SKIPS.load(Relaxed) - s0
    );
    // present customers: every row found
    for c in (0..20_000).step_by(74) {
        let (lo, hi) = cust_range(c);
        assert_eq!(as_vec(t.scan_range(&lo, Some(&hi), false, None, 4096).await.unwrap()), model_range(&m, &lo, Some(&hi), false, None));
    }
    t.abandon().await;
    fp
}

/// Hot customers read again after a reseal: with the staged publish the new sealed run's slices are in the counter
/// before the version is current, so the reads cost 0 GETs; the control (no staging) pays a GET per hot customer.
/// The reseal is step 4's trigger, stubbed: `fx_publish` with `Reseal::All`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_publish_keeps_hot_customers_warm_across_a_reseal() {
    let _s = SERIAL.lock().await;
    enable();
    let _m = Mode::set(SliceMode::Area);
    // fix-box-first-trip: a run of at most OPEN_TAIL bytes keeps its blocks from its open (`counter::seed_whole`),
    // which would answer these reads without the mechanism under test: off here, its own test is
    // `big_speed_tests::bft_open_of_a_small_run_keeps_its_blocks`
    counter::SEED_WHOLE.store(false, Relaxed);
    let staged_before = counter::STAGED.load(Relaxed);
    for staged in [true, false] {
        counter::STAGED.store(staged, Relaxed);
        view::clear_cache();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut t = lc(&store).create("fx-staged").await.unwrap();
        let mut m = BTreeMap::new();
        load(&mut t, &mut m, 200, 10, 300).await;
        publish(&mut t, &opts(Reseal::All, 16 << 10)).await;
        let hot: Vec<usize> = (0..200).step_by(4).collect();
        for &c in &hot {
            newest(&t, &m, c, 5).await;
        }
        counter::trim_all();
        // new rows for some hot customers and some others, then the reseal (step 4's trigger, stubbed)
        let mut ops = Vec::new();
        for c in (0..200).step_by(3) {
            ops.push((doc(c, 100), Some(vec![9u8; 300])));
            ops.push((idx(c, 100), Some(doc(c, 100))));
        }
        apply(&mut t, &mut m, ops).await;
        let rep = publish(&mut t, &opts(Reseal::All, 16 << 10)).await;
        let st = rep.stage.clone().unwrap();
        eprintln!("staged={staged}: {st:?}");
        let mut missed = 0;
        for &c in &hot {
            let g0 = view::RUN_GETS.load(Relaxed);
            newest(&t, &m, c, 5).await;
            missed += (view::RUN_GETS.load(Relaxed) > g0) as usize;
        }
        eprintln!("staged={staged}: {missed} of {} hot customers needed a GET after the reseal", hot.len());
        if staged {
            assert!(st.retired_runs > 0 && st.warmed > 0 && st.missed == 0 && !st.timed_out, "{st:?}");
            assert_eq!(missed, 0);
            // the retired run's slices left the counter
            assert!(counter::stats().slices <= st.warmed + hot.len() * 2, "{:?}", counter::stats());
        } else {
            assert_eq!(st.warmed, 0);
            assert_eq!(missed, hot.len());
        }
        t.abandon().await;
    }
    counter::STAGED.store(staged_before, Relaxed);
    counter::SEED_WHOLE.store(true, Relaxed);
}

/// A staged publish that cannot finish within its deadline publishes anyway and counts what it did not warm; the reads
/// stay exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_publish_past_its_deadline_publishes_anyway() {
    let _s = SERIAL.lock().await;
    enable();
    let _m = Mode::set(SliceMode::Area);
    let deadline_before = counter::STAGE_DEADLINE_MS.swap(20, Relaxed);
    counter::TEST_WARM_DELAY_MS.store(50, Relaxed);
    view::clear_cache();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    // fix-box-first-trip: a run of at most OPEN_TAIL bytes keeps its blocks from its open (`counter::seed_whole`),
    // which would answer these reads without the mechanism under test: off here, its own test is
    // `big_speed_tests::bft_open_of_a_small_run_keeps_its_blocks`
    counter::SEED_WHOLE.store(false, Relaxed);
    let mut t = lc(&store).create("fx-deadline").await.unwrap();
    let mut m = BTreeMap::new();
    load(&mut t, &mut m, 100, 10, 300).await;
    publish(&mut t, &opts(Reseal::All, 16 << 10)).await;
    for c in 0..100 {
        newest(&t, &m, c, 5).await;
    }
    let v0 = t.fx_view().unwrap().version;
    let rep = publish(&mut t, &opts(Reseal::All, 16 << 10)).await;
    let st = rep.stage.clone().unwrap();
    eprintln!("deadline 20 ms, 50 ms per warm-up GET: {st:?}");
    assert!(st.timed_out && st.missed > 0, "{st:?}");
    assert!(t.fx_view().unwrap().version > v0, "published anyway");
    for c in 0..100 {
        newest(&t, &m, c, 5).await;
    }
    counter::STAGE_DEADLINE_MS.store(deadline_before, Relaxed);
    counter::TEST_WARM_DELAY_MS.store(0, Relaxed);
    t.abandon().await;
    counter::SEED_WHOLE.store(true, Relaxed);
}
