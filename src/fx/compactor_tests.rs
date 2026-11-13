//! Tests of compaction 2, step 4: the compactor, the fold drop, GC, the takeover (rule 27 with F7).
//!
//! The model test (`compactor_model_drop_on`) runs writes, reads and held transactions against an exact model while the
//! compactor runs in the background with the drop on: absorption, tier merges, reseals with splits and group merges,
//! erasures, GC, and crashes of the waiter (the tenant abandoned and the compactor stopped mid-job). Every read is
//! compared with the model; at the end every erased customer is checked absent from every run of the newest version.
//! `FX_C_OPS` sets the number of operations, `FX_C_SEED` the seed.

use super::compactor::{self, Compactor, Knobs};
use super::tests::*;
use super::*;
use crate::lifecycle::{Op, Tenant};
use crate::runfmt::{CustomerKind, RunOptions};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::{Duration, Instant};

const HOLD_MS: u64 = 3000;

pub(super) fn test_knobs(drop: bool) -> Knobs {
    Knobs {
        theta: 0.25,
        r3: true,
        r3_gets_per_gib: 470.0,
        r2_age: None,
        tier_t: 4,
        tier_unit: 2 << 10,
        absorb_min_l0: 1,
        absorb_max_l0: 8,
        recognise_in_background: false,
        local_max_bytes: 16 << 10,
        split_bytes: 40 << 10,
        merge_bytes: 18 << 10,
        min_seal_bytes: 8 << 10,
        sealed: RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2),
        delta: RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2),
        drop,
        fold_one_run: std::env::var("FX_FOLD_ONE").is_ok(),
        fold_max_runs: 6,
        fold_at_l0: 6,
        lease: Duration::from_millis(400),
        remote_lease: Duration::from_millis(HOLD_MS + 600),
        serving_lease: Duration::from_millis(300),
        put_timeout: Duration::from_millis(250),
        gc_grace: Duration::from_millis(HOLD_MS + 800),
        version_retention: Duration::from_millis(0),
        gc_every: Duration::from_millis(500),
        tick: Duration::from_millis(10),
        reseal_parallel: 1,
        read_ahead: 64 << 10,
        derive: None,
    }
}

pub(super) fn setup() {
    enable();
    compactor::REVIEW_PLANT.store(std::env::var("FX_REVIEW_PLANT").ok().and_then(|s| s.parse().ok()).unwrap_or(0), Relaxed);
    hold::HOLD_LIFE_MS.store(HOLD_MS, Relaxed);
    // small L0 SSTs, so the writes of a test make many of them
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(3 << 10, Relaxed);
    // SlateDB stalls writes at 32 L0 SSTs here, not 8: "L0 never above 8" is then a measure, not a stall
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(32, Relaxed);
    // a stalled memtable flush retries at the writer's manifest poll (30 s by default, `--writer-poll-secs`)
    crate::lifecycle::WRITER_POLL_SECS.store(1, Relaxed);
    local::set_store(Some(Arc::new(InMemory::new())));
    view::clear_cache();
}

pub(super) fn teardown() {
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(30, Relaxed);
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
    T_PIN_MS.store(TEST_T_PIN_MS, Relaxed);
    local::set_store(None);
}

/// Open (or reopen) the writer of `id` with a version, and start its compactor.
async fn start(t: &mut Tenant, store: &Arc<dyn ObjectStore>, k: &Knobs) -> Arc<Compactor> {
    let pool = pool::Pool::new(2);
    let c = Compactor::start(t, Arc::clone(store), k.clone(), pool, tokio::runtime::Handle::current()).await.unwrap();
    c.run();
    c
}

/// The customer range of customer `c` (layout v2: `u/{len}{name}...`).
fn crange(c: usize) -> (Vec<u8>, Vec<u8>) {
    let name = format!("c{c:02}");
    let mut lo = b"u/".to_vec();
    lo.push(name.len() as u8);
    lo.extend(name.as_bytes());
    let mut hi = lo.clone();
    *hi.last_mut().unwrap() += 1;
    (lo, hi)
}

#[derive(Default, Debug)]
struct St {
    ops: u64,
    wrong: u64,
    writes: u64,
    reads: u64,
    ranges: u64,
    txn_reads: u64,
    retries: u64,
    crashes: u64,
    erasures: u64,
    max_l0: usize,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compactor_model_drop_on() {
    let _s = SERIAL.lock().await;
    setup();
    let n_ops: u64 = std::env::var("FX_C_OPS").ok().and_then(|s| s.parse().ok()).unwrap_or(20_000);
    let seed: u64 = std::env::var("FX_C_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(20261109);
    compactor::REVIEW_PLANT.store(std::env::var("FX_REVIEW_PLANT").ok().and_then(|s| s.parse().ok()).unwrap_or(0), Relaxed);
    // step 5: FX_WAVE=1 runs the same model test with the one-wave reader
    let wave = std::env::var("FX_WAVE").is_ok();
    crate::fx::view::WAVE.store(wave, std::sync::atomic::Ordering::Relaxed);
    // step 5b: the reader skips SlateDB SSTs at or below its X_d (on by default). FX_SKIP_PLANT=n plants the bug the
    // tests must catch: the reader skips with a cut n above the X_d its runs hold, so rows only SlateDB holds are lost.
    let plant: u64 = std::env::var("FX_SKIP_PLANT").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    super::seqfilter::CONTROL_EXTRA.store(plant, Relaxed);
    let (skip_asked0, skip_skipped0) = (super::seqfilter::ASKED.load(Relaxed), super::seqfilter::SKIPPED.load(Relaxed));
    let mut rng = StdRng::seed_from_u64(seed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-cmodel";
    let keys = universe();
    let l = lc(&store);
    let mut t = l.create(id).await.unwrap();
    let mut m: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for i in 0..keys.len() {
        if i % 4 == 0 {
            let v: Vec<u8> = (0..rng.gen_range(0..120)).map(|_| rng.gen()).collect();
            apply(&mut t, &mut m, vec![(keys[i].clone(), Some(v))]).await;
        }
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    m.insert(crate::envelope::MARKER.to_vec(), b"E1".to_vec()); // the envelope marker is a row of the keyspace
    T_PIN_MS.store(350, Relaxed); // the follower below keeps a version up to 330 ms (under T_pin, under the 400 ms lease)
    let k = test_knobs(true);
    let mut c = start(&mut t, &store, &k).await;
    let mut st = St::default();
    let mut snaps: Vec<(Arc<read::Snap>, BTreeMap<Vec<u8>, Vec<u8>>, Instant, Vec<Vec<u8>>)> = Vec::new();
    // keys written lately: transactions read them (the rows a floor that ignores holds would drop, review of step 4, Q3)
    let mut recent: std::collections::VecDeque<Vec<u8>> = std::collections::VecDeque::new();
    // a follower in another process (review of step 4, Q3): its own SlateDB reader, which re-reads SlateDB's manifest
    // every 20 ms, and a version it keeps for less than T_pin. Its answer for a key nobody wrote since it took that
    // version must not change: a row dropped under it (a floor that ignored the lease) would change it.
    let mut fopts = slatedb::config::DbReaderOptions::default();
    fopts.manifest_poll_interval = Duration::from_millis(20);
    fopts.skip_wal_replay = true;
    let freader = read::open_follow_reader(crate::lifecycle::tenant_path(id), Arc::clone(&store), fopts).await.unwrap();
    let mut follower: Option<(Arc<view::View>, Instant, BTreeMap<Vec<u8>, Vec<u8>>, std::collections::HashSet<Vec<u8>>)> = None;
    let (mut f_reads, mut f_wrong, mut f_opens) = (0u64, 0u64, 0u64);
    // the versions the tenant served, with when it installed them: a follower refreshed up to T_pin ago reads with an
    // older version than the newest, so it takes the one that was newest 200 ms ago
    let mut served: std::collections::VecDeque<(Instant, manifest::Version)> = std::collections::VecDeque::new();
    let mut erased: Vec<(Vec<u8>, Vec<u8>, u64)> = Vec::new();
    let (mut drops, mut gcs, mut stats_absorbs, mut stats_merges, mut stats_reseals, mut splits, mut gmerges) = (0u64, 0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
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
    let mut collect = |c: &Compactor| {
        let s = &c.sh.stats;
        drops += c.drop.rows_dropped.load(Relaxed);
        gcs += c.gc.runs_deleted.load(Relaxed);
        stats_absorbs += s.absorbs.load(Relaxed);
        stats_merges += s.merges.load(Relaxed);
        stats_reseals += s.reseals.load(Relaxed);
        splits += s.splits.load(Relaxed);
        gmerges += s.group_merges.load(Relaxed);
    };
    for op in 0..n_ops {
        st.ops = op + 1;
        t.fx_sync(); // the server's exclusive step installs the compactor's newest version
        if let Some(v) = t.fx_view() {
            if served.back().is_none_or(|(_, b)| b.version != v.version) {
                served.push_back((Instant::now(), v.meta.clone()));
            }
            while served.len() > 2 && served[1].0.elapsed() > Duration::from_millis(400) {
                served.pop_front();
            }
        }
        if let Some(n) = t.writer_l0() {
            st.max_l0 = st.max_l0.max(n);
        }
        let r = rng.gen_range(0..1000);
        if r < 450 {
            // a write batch: puts and deletes (layout v2 customers and the shared keyspace)
            let n = rng.gen_range(1..4);
            let mut ops = Vec::new();
            for _ in 0..n {
                let key = keys[rng.gen_range(0..keys.len())].clone();
                if rng.gen_bool(0.2) {
                    ops.push((key, None));
                } else {
                    let v: Vec<u8> = (0..rng.gen_range(0..160)).map(|_| rng.gen()).collect();
                    ops.push((key, Some(v)));
                }
            }
            for (k, _) in &ops {
                recent.push_back(k.clone());
                if let Some(f) = follower.as_mut() {
                    f.3.insert(k.clone());
                }
            }
            while recent.len() > 300 {
                recent.pop_front();
            }
            apply(&mut t, &mut m, ops).await;
            st.writes += 1;
        } else if r < 700 {
            let key = &keys[rng.gen_range(0..keys.len())];
            match t.get(key).await {
                Ok(got) => check!(format!("get {:?}", String::from_utf8_lossy(key)), got.map(|b| b.to_vec()), m.get(key).cloned()),
                Err(e) if e.downcast_ref::<Retry>().is_some() => st.retries += 1,
                Err(e) => panic!("get: {e:#}"),
            }
            st.reads += 1;
        } else if r < 800 {
            let a = &keys[rng.gen_range(0..keys.len())];
            let b = &keys[rng.gen_range(0..keys.len())];
            let (lo, hi) = if a <= b { (a.clone(), b.clone()) } else { (b.clone(), a.clone()) };
            let rev = rng.gen_bool(0.3);
            let limit = if rng.gen_bool(0.5) { Some(rng.gen_range(1..60)) } else { None };
            match t.scan_range(&lo, Some(&hi), rev, limit, 64 << 10).await {
                Ok(got) => {
                    let (got, want) = (as_vec(got), model_range(&m, &lo, Some(&hi), rev, limit));
                    if got != want && st.wrong < 3 {
                        let gk: Vec<_> = got.iter().map(|x| String::from_utf8_lossy(&x.0).to_string()).collect();
                        let wk: Vec<_> = want.iter().map(|x| String::from_utf8_lossy(&x.0).to_string()).collect();
                        let first = (0..gk.len().max(wk.len())).find(|&i| got.get(i) != want.get(i)).unwrap();
                        eprintln!("range diff: rev {rev} limit {limit:?} got {} rows want {}; first diff at {first}: got {:?} want {:?}; version {:?}", gk.len(), wk.len(), gk.get(first), wk.get(first), t.fx_view().map(|v| (v.version, v.x_d, v.groups.len(), v.delta.len())));
                    }
                    check!("range", got, want)
                }
                Err(e) if e.downcast_ref::<Retry>().is_some() => st.retries += 1,
                Err(e) => panic!("scan: {e:#}"),
            }
            st.ranges += 1;
        } else if r < 900 {
            // transactions: open one, or read through a held one and compare with the model at its begin
            if snaps.len() < 3 && rng.gen_bool(0.5) {
                let s = t.txn_snapshot().await.unwrap();
                // the last 30 keys written: their rows are still above the transaction's X_d (memtable, L0 not absorbed)
                snaps.push((s, m.clone(), Instant::now(), recent.iter().rev().take(30).cloned().collect()));
            } else if !snaps.is_empty() {
                let i = rng.gen_range(0..snaps.len());
                let (s, sm, at, rec) = &snaps[i];
                let key = if !rec.is_empty() && rng.gen_bool(0.6) { &rec[rng.gen_range(0..rec.len())] } else { &keys[rng.gen_range(0..keys.len())] };
                match s.get(key).await {
                    Ok(got) => check!(format!("txn get (age {:?})", at.elapsed()), got.map(|b| b.to_vec()), sm.get(key).cloned()),
                    Err(e) if e.downcast_ref::<Retry>().is_some() => {
                        st.retries += 1;
                        snaps.remove(i);
                        continue;
                    }
                    Err(e) => panic!("txn get: {e:#}"),
                }
                st.txn_reads += 1;
                if at.elapsed() > Duration::from_millis(HOLD_MS - 500) || rng.gen_bool(0.02) {
                    snaps.remove(i);
                }
            }
        } else if r < 903 {
            // an erasure: delete every key of one customer, then ask the compactor to erase the range
            let cust = rng.gen_range(0..40);
            let (lo, hi) = crange(cust);
            let gone: Vec<Vec<u8>> = m.range(lo.clone()..hi.clone()).map(|(k, _)| k.clone()).collect();
            if gone.is_empty() {
                continue;
            }
            let commit = t.stage(gone.iter().map(|k| Op::Delete(k.clone())).collect()).await.unwrap().unwrap();
            let seq = commit.seqnum();
            commit.wait().await.unwrap();
            for k in &gone {
                m.remove(k);
                if let Some(f) = follower.as_mut() {
                    f.3.insert(k.clone());
                }
            }
            c.erase(lo.clone(), hi.clone(), seq);
            erased.push((lo, hi, seq));
            st.erasures += 1;
        } else if r < 905 && op > 500 && op < n_ops / 2 {
            // crashes in the first half only: after each one the takeover window (rule 27) keeps the floor low for one
            // serving lease plus one hold life, so the second half runs with drops working (review of step 4, Q3)
            // a crash of the waiter: the compactor stops mid-job, the tenant is abandoned, a new writer opens
            c.stop().await;
            collect(&c);
            snaps.clear();
            t.abandon().await;
            t = lc(&store).wake(id).await.unwrap();
            t.upgrade().await.unwrap();
            c = start(&mut t, &store, &k).await;
            // erasures not done before the crash are asked again (the request is the control layer's, it retries)
            for (lo, hi, seq) in &erased {
                c.erase(lo.clone(), hi.clone(), *seq);
            }
            st.crashes += 1;
        } else if r < 990 {
            // the follower: take the newest version from the store (as a refresh would) at most every 150 ms, under T_pin
            // (the model's 200 ms) and the lease (400 ms); read its answers for every key once, then re-read keys nobody
            // wrote since
            if follower.as_ref().is_none_or(|f| f.1.elapsed() > Duration::from_millis(330)) {
                follower = None;
                // the version that was newest 200 ms ago, and the time the follower took it (its T_pin counts from then)
                let pick = served.iter().rev().find(|(at, _)| at.elapsed() >= Duration::from_millis(200)).or(served.front()).cloned();
                if let Some((taken, v)) = pick.filter(|(at, _)| at.elapsed() < Duration::from_millis(300)) {
                    let env = v.e_env;
                    let view = Arc::new(view::View::new(Arc::clone(&store), v, None).unwrap());
                    let opts = read::scan_options(1 << 20, false, false);
                    match read::scan(&freader, &opts, Some(&view), env, b"", None, false, None).await {
                        Ok(rows) => {
                            let base: BTreeMap<Vec<u8>, Vec<u8>> = rows.into_iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
                            // keys whose answer already differs from the model lag (their newest row is not flushed yet,
                            // and this reader replays no WAL): they are not checked, as if written since
                            let mut lagging: std::collections::HashSet<Vec<u8>> = Default::default();
                            for k in m.keys().chain(base.keys()) {
                                if m.get(k) != base.get(k) {
                                    lagging.insert(k.clone());
                                }
                            }
                            follower = Some((view, taken, base, lagging));
                            f_opens += 1;
                        }
                        Err(e) => panic!("follower scan: {e:#}"),
                    }
                }
            } else if let Some((view, _, base, written)) = follower.as_ref() {
                let key = if !recent.is_empty() && rng.gen_bool(0.7) { recent[rng.gen_range(0..recent.len())].clone() } else { keys[rng.gen_range(0..keys.len())].clone() };
                if !written.contains(&key) {
                    let got = read::get(&freader, &slatedb::config::ReadOptions::default(), Some(view), view.e_env, &key).await.unwrap();
                    f_reads += 1;
                    if got.as_deref() != base.get(&key).map(|v| &v[..]) {
                        f_wrong += 1;
                        st.wrong += 1;
                        if f_wrong <= 3 {
                            eprintln!("WRONG at op {}: follower answer for {:?} changed under its version {}", st.ops, String::from_utf8_lossy(&key), view.version);
                        }
                    }
                }
            }
        } else {
            // let the background jobs run
            tokio::time::sleep(Duration::from_millis(rng.gen_range(0..4))).await;
        }
    }
    // let the erasures and the drop finish, then check everything
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(20) {
        t.fx_sync();
        let pending = c.sh.erasures.lock().unwrap().iter().filter(|e| e.done.is_none()).count();
        if pending == 0 && c.drop.rows_dropped.load(Relaxed) > 0 {
            break;
        }
        // writes keep L0 moving so the last erasures' deletes get absorbed
        apply(&mut t, &mut m, vec![(keys[0].clone(), Some(b"tick".to_vec()))]).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    t.fx_sync();
    let all = t.scan_range(b"", None, false, None, 1 << 20).await.unwrap();
    check!("full scan at the end", as_vec(all), model_range(&m, b"", None, false, None));
    for key in &keys {
        let got = t.get(key).await.unwrap();
        check!("final get", got.map(|b| b.to_vec()), m.get(key).cloned());
    }
    // erased customers: no row at or below the erasure's seq in any run of the newest version
    let v = c.current().await;
    let mut leftovers = 0;
    let lines: Vec<manifest::RunLine> = v.groups.iter().filter_map(|g| g.sealed.clone()).chain(v.delta.iter().cloned()).collect();
    for line in &lines {
        for row in compactor::run_rows(&store, line).await.unwrap() {
            for (lo, hi, seq) in &erased {
                if row.key >= *lo && row.key < *hi && row.seq <= *seq && v.x_d >= *seq {
                    leftovers += 1;
                }
            }
        }
    }
    let pending = c.sh.erasures.lock().unwrap().iter().filter(|e| e.done.is_none()).count();
    // GC after its grace (crashes above restart its count), then every read again: GC kept what is live
    gc::gc_once(&c).await.unwrap();
    tokio::time::sleep(k.gc_grace + Duration::from_millis(300)).await;
    gc::gc_once(&c).await.unwrap();
    view::clear_cache();
    t.fx_sync();
    let all = t.scan_range(b"", None, false, None, 1 << 20).await.unwrap();
    check!("full scan after GC", as_vec(all), model_range(&m, b"", None, false, None));
    for key in &keys {
        let got = t.get(key).await.unwrap();
        check!("get after GC", got.map(|b| b.to_vec()), m.get(key).cloned());
    }
    c.stop().await;
    collect(&c);
    t.abandon().await;
    teardown();
    eprintln!("compactor model: {st:?}");
    eprintln!("compactor model: follower versions {f_opens}, follower reads {f_reads}, follower answers that changed {f_wrong}");
    eprintln!("compactor model: absorbs {stats_absorbs}, merges {stats_merges}, reseals {stats_reseals} (splits {splits}, group merges {gmerges}), rows dropped from SlateDB {drops}, runs deleted by GC {gcs}, erasure leftovers {leftovers}, erasures pending {pending}, groups {}, delta runs {}", v.groups.len(), v.delta.len());
    let (asked, skipped) = (super::seqfilter::ASKED.load(Relaxed) - skip_asked0, super::seqfilter::SKIPPED.load(Relaxed) - skip_skipped0);
    eprintln!("compactor model: SlateDB SSTs asked by the seq filter {asked}, skipped {skipped}, planted extra {plant}");
    eprintln!("compactor model: SlateDB SSTs asked by the customer filter {}, skipped {}", super::seqfilter::CUST_ASKED.load(Relaxed), super::seqfilter::CUST_SKIPPED.load(Relaxed));
    super::seqfilter::CONTROL_EXTRA.store(0, Relaxed);
    if plant > 0 {
        assert!(st.wrong + f_wrong > 0, "the planted skip must give wrong answers");
        return;
    }
    assert!(skipped > 0, "the reader skipped SlateDB SSTs");
    assert_eq!(st.wrong, 0, "wrong answers");
    assert_eq!(leftovers, 0, "erased rows left in live runs");
    assert_eq!(pending, 0, "erasures not done");
    assert!(drops > 0 && gcs > 0 && stats_merges > 0 && stats_reseals > 0 && splits > 0 && gmerges > 0, "every mechanism ran");
}

/// A transaction held across the compactor's work (absorption, reseals, the fold drop, GC) reads its snapshot: GC keeps
/// every version from the oldest one held in this process. A follower in another process (its holds are not ours)
/// refreshes its version, then the writer retires every run of it; the follower still reads within T_pin, thanks to
/// GC's grace (rule 23). With a grace shorter than that (the control), the follower's reads fail: an error, never a
/// wrong answer, since runs are opened by id.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_reads_across_drop_and_gc() {
    let _s = SERIAL.lock().await;
    setup();
    for control in [false, true] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = if control { "fx-held-ctl" } else { "fx-held" };
        let mut t = lc(&store).create(id).await.unwrap();
        let mut m = BTreeMap::new();
        for i in 0..400 {
            apply(&mut t, &mut m, vec![(ckey(i % 9, i), Some(vec![b'a'; 60]))]).await;
        }
        publish(&mut t, &small_opts(build::Reseal::All)).await;
        T_PIN_MS.store(350, Relaxed); // under the lease (400 ms)
        let mut k = test_knobs(true);
        if control {
            k.put_timeout = Duration::from_millis(50);
            k.gc_grace = Duration::from_millis(60);
            hold::HOLD_LIFE_MS.store(0, Relaxed); // lets the knob check pass with a grace below the real hold life
        }
        let c = Compactor::start(&mut t, Arc::clone(&store), k.clone(), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
        hold::HOLD_LIFE_MS.store(HOLD_MS, Relaxed);
        c.run();
        let snap = t.txn_snapshot().await.unwrap();
        let at = m.clone();
        // a rewrite round, absorbed: delta runs exist
        for i in 0..400 {
            apply(&mut t, &mut m, vec![(ckey(i % 9, i), Some(vec![b'b'; 60]))]).await;
        }
        flush(&t).await;
        let t0 = Instant::now();
        while c.absorbed.lock().unwrap().len() < t.writer_l0().unwrap_or(0) && t0.elapsed() < Duration::from_secs(5) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        t.fx_sync();
        // a follower in another process: its own lifecycle, reader mode, the newest version
        let mut follower = lc(&store).wake(id).await.unwrap();
        follower.fx_refresh().await.unwrap();
        // c2-fix-long-reads: the follower keeps the version it loaded (a read step would renew it past half T_pin)
        CONTROL_NO_RENEW.store(true, Relaxed);
        let loaded = Instant::now();
        let fm = m.clone();
        // the writer retires every run of that version: every group resealed, merges, then GC
        let v = c.current().await;
        for gi in 0..v.groups.len() {
            c.reseal_once(Some(gi)).await.unwrap();
        }
        while c.merge_once().await.unwrap() {}
        t.fx_sync();
        // the transaction reads first; in the control it ends before GC (its hold would keep its versions' runs)
        let (mut wrong, mut errors) = (0, 0);
        let check_txn = |snap: &Arc<read::Snap>| {
            let snap = Arc::clone(snap);
            let at = at.clone();
            async move {
                let (mut w, mut e) = (0, 0);
                for (key, want) in &at {
                    match snap.get(key).await {
                        Ok(got) => w += (got.as_deref() != Some(&want[..])) as u64,
                        Err(_) => e += 1,
                    }
                }
                (w, e)
            }
        };
        let mut snap = Some(snap);
        if control {
            let (w, e) = check_txn(snap.as_ref().unwrap()).await;
            wrong += w;
            errors += e;
            snap = None;
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
        gc::gc_once(&c).await.unwrap();
        tokio::time::sleep(Duration::from_millis(70)).await;
        gc::gc_once(&c).await.unwrap();
        view::clear_cache();
        let read_at = loaded.elapsed();
        let (mut f_wrong, mut f_errors) = (0, 0);
        let mut first = String::new();
        for (key, want) in &fm {
            match follower.get(key).await {
                Ok(got) => f_wrong += (got.as_deref() != Some(&want[..])) as u64,
                Err(e) => {
                    if first.is_empty() {
                        first = format!("{e:#}");
                    }
                    f_errors += 1
                }
            }
        }
        if let Some(s) = &snap {
            let (w, e) = check_txn(s).await;
            wrong += w;
            errors += e;
        }
        let deleted = c.gc.runs_deleted.load(Relaxed);
        eprintln!("held reads (control {control}): transaction wrong {wrong} errors {errors}; follower (reads {read_at:?} after its refresh) wrong {f_wrong} errors {f_errors} ({first}); runs deleted {deleted}");
        assert_eq!(wrong + f_wrong, 0, "a wrong answer");
        assert_eq!(errors, 0, "the held transaction lost a run");
        assert!(read_at < Duration::from_millis(350), "the follower read too late for the test");
        if control {
            assert!(f_errors > 0, "the control should delete runs the follower still reads");
        } else {
            assert_eq!(f_errors, 0);
        }
        drop(snap);
        CONTROL_NO_RENEW.store(false, Relaxed);
        follower.abandon().await;
        c.stop().await;
        t.abandon().await;
        T_PIN_MS.store(TEST_T_PIN_MS, Relaxed);
    }
    teardown();
}

/// Rule 4 control: the floor ignores holds and the served version; a transaction held across a fold then misses rows
/// that only SlateDB had (rows above its version's X_d dropped before the transaction's version knew them).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn floor_respects_holds_and_control_breaks() {
    let _s = SERIAL.lock().await;
    setup();
    for control in [false, true] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = if control { "fx-floor-ctl" } else { "fx-floor" };
        let mut t = lc(&store).create(id).await.unwrap();
        let mut m = BTreeMap::new();
        for i in 0..50 {
            apply(&mut t, &mut m, vec![(ckey(1, i), Some(b"old".to_vec()))]).await;
        }
        publish(&mut t, &small_opts(build::Reseal::All)).await;
        let mut k = test_knobs(true);
        // the one-run fold drops row by row, so the floor alone decides what goes (aged runs leave only whole, once the
        // floor passes their highest seq, which a held transaction prevents here anyway)
        k.fold_one_run = std::env::var("R4_AGED").is_err(); // review: R4_AGED=1 keeps the default aged runs
        let c = Compactor::start(&mut t, Arc::clone(&store), k, pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
        hold::HOLD_LIFE_MS.store(60_000, Relaxed); // the transaction lives through the wait below
        c.drop.control_ignore_holds.store(control, Relaxed);
        c.drop.control_no_takeover_window.store(true, Relaxed);
        // new rows, flushed to L0 but not absorbed yet, then a transaction: its version's X_d is below them
        for i in 0..200 {
            apply(&mut t, &mut m, vec![(ckey(2, i), Some(vec![b'n'; 40]))]).await;
        }
        flush(&t).await;
        let snap = t.txn_snapshot().await.unwrap();
        let at = m.clone();
        // absorb and publish (the tenant does not install: it keeps serving the older version), then let the lease pass
        // and fold: the floor must stay at the held version's X_d
        c.drain(false).await.unwrap();
        tokio::time::sleep(k_lease() + Duration::from_millis(200)).await;
        c.drop.request_refold();
        // until the drop has removed rows (the floor reached the held version's X_d), at most 15 s
        let t0 = Instant::now();
        while (c.drop.rows_dropped.load(Relaxed) == 0 || t0.elapsed() < Duration::from_secs(2)) && t0.elapsed() < Duration::from_secs(15) {
            c.drop.maybe_refold();
            c.drop.request_refold();
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        // the writer sees the fold (the compactor's loop refreshes it; this test drives the jobs by hand)
        t.fx_writer_db().unwrap().refresh_manifest().await.unwrap();
        let mut wrong = 0;
        for (key, want) in &at {
            if snap.get(key).await.unwrap().as_deref() != Some(&want[..]) {
                wrong += 1;
            }
        }
        eprintln!("floor with holds (control {control}): wrong {wrong}, last floor {}, snapshot seq {}, rows dropped {}", c.drop.last_floor.load(Relaxed), snap.seq(), c.drop.rows_dropped.load(Relaxed));
        assert!(c.drop.rows_dropped.load(Relaxed) > 0, "the drop removed nothing: the test proves nothing");
        if control {
            assert!(wrong > 0, "the control should miss rows");
        } else {
            assert_eq!(wrong, 0);
        }
        drop(snap);
        hold::HOLD_LIFE_MS.store(HOLD_MS, Relaxed);
        c.stop().await;
        t.abandon().await;
    }
    teardown();
}

pub(super) async fn flush(t: &Tenant) {
    let db = t.fx_writer_db().unwrap();
    db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
}

fn k_lease() -> Duration {
    test_knobs(true).lease
}

/// Rule 27 with F7: a takeover while the old waiter has an open transaction. The new waiter's drop uses the remote
/// lease for one serving lease plus one hold life, so its floor never passes the old transaction's X_d during that
/// window; the old waiter's compactor stops at its first lost race (rule 28); once the old waiter's serving lease has
/// expired it opens no new transaction (F7, the takeover test of the step 4 prompt). Control: without the window, the
/// new waiter's floor passes the old transaction's X_d while it still reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn takeover_with_an_open_transaction() {
    let _s = SERIAL.lock().await;
    setup();
    for control in [false, true] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = if control { "fx-take-ctl" } else { "fx-take" };
        let mut a = lc(&store).create(id).await.unwrap();
        let mut m = BTreeMap::new();
        for i in 0..100 {
            apply(&mut a, &mut m, vec![(ckey(3, i), Some(b"a".to_vec()))]).await;
        }
        publish(&mut a, &small_opts(build::Reseal::All)).await;
        let k = test_knobs(true);
        let ca = start(&mut a, &store, &k).await;
        a.set_serving_until(Some(Instant::now() + k.serving_lease));
        let txn = a.txn_snapshot().await.unwrap();
        let held_xd = a.fx_view().unwrap().x_d;
        let at = m.clone();
        // the takeover: B opens the database (fences A's SlateDB writer and our manifest), starts its compactor
        let mut b = lc(&store).wake(id).await.unwrap();
        b.fx_own_holds(); // B is another process: A's holds are not B's (step 7a shares them inside one process)
        b.upgrade().await.unwrap();
        let cb = start(&mut b, &store, &k).await;
        cb.drop.control_no_takeover_window.store(control, Relaxed);
        // B writes, absorbs, publishes and folds while A's transaction is open
        let t0 = Instant::now();
        let mut floor_passed = false;
        let mut i = 0;
        while t0.elapsed() < Duration::from_millis(HOLD_MS - 400) {
            apply(&mut b, &mut m, vec![(ckey(4, i % 300), Some(vec![b'b'; 50]))]).await;
            i += 1;
            b.fx_sync();
            if i % 50 == 0 {
                cb.drop.request_refold();
                if let Ok(f) = cb.drop.floor().await {
                    floor_passed |= f > held_xd;
                }
            }
        }
        // A's transaction reads its snapshot or fails; never a wrong answer. (SlateDB 0.17 closes a writer that detects a
        // newer one, and its snapshot's reads then fail with "detected newer DB client": MEASURED here.)
        let mut wrong = 0;
        let mut errors = 0;
        let mut first_err = String::new();
        for (key, want) in &at {
            match txn.get(key).await {
                Ok(got) => wrong += (got.as_deref() != Some(&want[..])) as u64,
                Err(e) => {
                    errors += 1;
                    if first_err.is_empty() {
                        first_err = format!("{e:#}");
                    }
                }
            }
        }
        // F7: A's serving lease has expired; A opens no new transaction
        let refused = match a.txn_snapshot().await {
            Err(e) => e.downcast_ref::<Retry>().is_some() || format!("{e:#}").contains("fenced") || format!("{e:#}").contains("Fenced"),
            Ok(_) => false,
        };
        // rule 28: A's compactor stopped at its first lost race (or never published again)
        apply(&mut b, &mut m, vec![(ckey(5, 1), Some(b"x".to_vec()))]).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let a_stopped = ca.stopped.load(Relaxed) || ca.sh.stats.publishes.load(Relaxed) == 0 || ca.current().await.writer_epoch < cb.current().await.writer_epoch;
        eprintln!("takeover (control {control}): old transaction wrong {wrong}, errors {errors} ({first_err}), new transaction refused {refused}, B's floor passed A's X_d {held_xd} within the window: {floor_passed}, A's compactor stopped or outdated {a_stopped}, lease now {:?}", cb.drop.lease_now());
        assert_eq!(wrong, 0);
        assert!(refused, "the old waiter opened a transaction after its serving lease expired");
        if control {
            assert!(floor_passed, "the control's floor should pass the old transaction's X_d");
        } else {
            assert!(!floor_passed, "the floor passed the old transaction's X_d inside the takeover window");
        }
        drop(txn);
        ca.stop().await;
        cb.stop().await;
        a.abandon().await;
        b.abandon().await;
    }
    teardown();
}

/// Rule 25 and F6 in the compactor: a manifest PUT that lands but answers too late counts as timed out; the next commit
/// first publishes a fence, which finds the slot taken by the landed version and adopts it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_put_answer_is_a_timeout_then_a_fence() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-late";
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..100 {
        apply(&mut t, &mut m, vec![(ckey(6, i), Some(b"v".to_vec()))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let c = Compactor::start(&mut t, Arc::clone(&store), test_knobs(false), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    for i in 0..200 {
        apply(&mut t, &mut m, vec![(ckey(7, i), Some(vec![b'w'; 40]))]).await;
    }
    // the next manifest create waits at the fail point (its PUT has not been sent); the commit times out
    flush(&t).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    *manifest::SLOW_PUT.lock().unwrap() = Some(Arc::clone(&gate));
    let before = c.current().await.version;
    let r = c.absorb_once().await;
    let msg = match &r {
        Ok(n) => format!("Ok({n})"),
        Err(e) => format!("{e:#}"),
    };
    assert!(msg.contains("timed out"), "the commit must time out: {msg}");
    *manifest::SLOW_PUT.lock().unwrap() = None;
    manifest::SLOW_PUT_WAITING.store(0, std::sync::atomic::Ordering::SeqCst); // step 3's tests count from 0
    assert!(c.fence_pending());
    // the late PUT lands now (the waiting create goes on in the background)
    gate.notify_waiters();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let landed = manifest::list_versions(&store, id).await.unwrap();
    // the next commit fences first: the slot after `before` is taken by the landed version, so it adopts it
    for i in 0..100 {
        apply(&mut t, &mut m, vec![(ckey(8, i), Some(vec![b'z'; 40]))]).await;
    }
    flush(&t).await;
    c.absorb_once().await.unwrap(); // fences first, adopts the landed version, and abandons its own plan
    assert!(!c.fence_pending());
    c.absorb_once().await.unwrap();
    let now = c.current().await.version;
    eprintln!("late put: before {before}, landed versions {landed:?}, now {now}, fences {}", c.sh.stats.fences.load(Relaxed));
    assert!(now > before + 1);
    t.fx_sync();
    for (key, want) in &m {
        assert_eq!(t.get(key).await.unwrap().as_deref(), Some(&want[..]));
    }
    c.stop().await;
    t.abandon().await;
    teardown();
}

/// R1: small delta runs get an NVMe copy and are read there with 0 GETs; a reader without the copy reads the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_delta_runs_read_from_nvme() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-r1";
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..100 {
        apply(&mut t, &mut m, vec![(ckey(9, i), Some(b"v".to_vec()))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let mut k = test_knobs(false);
    k.theta = 1000.0; // no reseal: the rows stay in delta runs
    k.r3 = false;
    k.local_max_bytes = 1 << 20;
    let c = Compactor::start(&mut t, Arc::clone(&store), k, pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    c.run();
    for i in 0..300 {
        apply(&mut t, &mut m, vec![(ckey(10, i), Some(vec![b'n'; 30]))]).await;
        if i % 50 == 0 {
            eprintln!("r1 write {i}: L0 {:?}, absorbs {}, drop jobs {}/{}, errors {}", t.writer_l0(), c.sh.stats.absorbs.load(Relaxed), c.drop.jobs_done.load(Relaxed), c.drop.jobs.load(Relaxed), c.sh.stats.errors.load(Relaxed));
        }
    }
    flush(&t).await;
    c.drain(false).await.unwrap();
    t.fx_sync();
    let v = c.current().await;
    assert!(v.delta.iter().any(|d| d.local), "no delta run has an NVMe copy");
    view::clear_cache();
    let (g0, l0) = (view::RUN_GETS.load(Relaxed), view::LOCAL_READS.load(Relaxed));
    for i in 0..300 {
        assert_eq!(t.get(ckey(10, i)).await.unwrap().as_deref(), Some(&vec![b'n'; 30][..]));
    }
    let (g1, l1) = (view::RUN_GETS.load(Relaxed), view::LOCAL_READS.load(Relaxed));
    eprintln!("R1: store GETs {} and NVMe reads {} for 300 point reads of delta rows", g1 - g0, l1 - l0);
    assert_eq!(g1 - g0, 0, "delta rows read from the store although the NVMe copy exists");
    assert!(l1 > l0);
    // without the NVMe store (another waiter): the same reads go to the store and agree
    local::set_store(None);
    let r = lc(&store).wake(id).await.unwrap();
    view::clear_cache();
    for i in 0..300 {
        assert_eq!(r.get(ckey(10, i)).await.unwrap().as_deref(), Some(&vec![b'n'; 30][..]));
    }
    assert!(view::RUN_GETS.load(Relaxed) > g1);
    r.abandon().await;
    c.stop().await;
    t.abandon().await;
    teardown();
}

/// R3: point reads that pay GETs in delta runs make a group due before theta.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r3_reseals_a_group_whose_reads_pay() {
    let _s = SERIAL.lock().await;
    setup();
    local::set_store(None); // delta reads must pay GETs
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-r3";
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    for i in 0..2000 {
        apply(&mut t, &mut m, vec![(ckey(11, i), Some(vec![b'v'; 40]))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let mut k = test_knobs(false);
    k.theta = 100.0; // theta never fires
    k.min_seal_bytes = 1 << 30;
    let c = Compactor::start(&mut t, Arc::clone(&store), k, pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    c.run();
    for i in 0..40 {
        apply(&mut t, &mut m, vec![(ckey(11, i * 7), Some(vec![b'n'; 40]))]).await;
    }
    flush(&t).await;
    c.drain(true).await.unwrap();
    t.fx_sync();
    assert_eq!(c.sh.stats.reseals.load(Relaxed), 0);
    for _ in 0..4 {
        view::clear_cache();
        for i in 0..40 {
            t.get(ckey(11, i * 7)).await.unwrap();
        }
    }
    c.drain(true).await.unwrap();
    eprintln!("R3: gets noted, reseals {} (r3 {})", c.sh.stats.reseals.load(Relaxed), c.sh.stats.r3_reseals.load(Relaxed));
    assert!(c.sh.stats.r3_reseals.load(Relaxed) >= 1);
    c.stop().await;
    t.abandon().await;
    teardown();
}

/// Integration-3 (step 4 with step 7b): step 7b writes a `clone` fork's reference at `{source}/fx/forks/{fork}` naming
/// the source's version and runs the fork's first version lists. Step 4's GC must keep those runs although no newer
/// version of the source lists them; it read only `fx/pairs` and `fx/refs`, so the fork lost every run it shares (a
/// read of the fork then fails: data loss for the fork). The control writes no reference: GC deletes the same runs in
/// the same window, so the test can see a deletion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_keeps_the_runs_a_fork_reference_names() {
    let _s = SERIAL.lock().await;
    setup();
    for control in [false, true] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = if control { "fx-forkgc-ctl" } else { "fx-forkgc" };
        let mut t = lc(&store).create(id).await.unwrap();
        let mut m = BTreeMap::new();
        for i in 0..400 {
            apply(&mut t, &mut m, vec![(ckey(i % 9, i), Some(vec![b'a'; 60]))]).await;
        }
        publish(&mut t, &small_opts(build::Reseal::All)).await;
        let v1 = t.fx_view().unwrap().meta.clone();
        let runs: Vec<String> = v1.groups.iter().filter_map(|g| g.sealed.as_ref().map(|s| s.path.clone())).chain(v1.delta.iter().map(|d| d.path.clone())).collect();
        assert!(!runs.is_empty());
        if !control {
            let r = pair::ForkRef { fork: "kid".into(), source: id.into(), version: v1.version, runs: runs.clone(), created_ms: manifest::now_ms() };
            object_store::ObjectStoreExt::put(&*store, &pair::fork_ref_path(id, "kid"), serde_json::to_vec(&r).unwrap().into()).await.unwrap();
        }
        let c = Compactor::start(&mut t, Arc::clone(&store), test_knobs(false), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
        // a rewrite round, absorbed, then every group resealed and the delta runs merged: no newer version lists v1's runs
        for i in 0..400 {
            apply(&mut t, &mut m, vec![(ckey(i % 9, i), Some(vec![b'b'; 60]))]).await;
        }
        flush(&t).await;
        while c.absorb_once().await.unwrap() > 0 {}
        let v = c.current().await;
        for gi in 0..v.groups.len() {
            c.reseal_once(Some(gi)).await.unwrap();
        }
        while c.merge_once().await.unwrap() {}
        t.fx_sync();
        let newest = c.current().await;
        let still_listed = runs.iter().filter(|p| newest.groups.iter().any(|g| g.sealed.as_ref().is_some_and(|s| &s.path == *p)) || newest.delta.iter().any(|d| &d.path == *p)).count();
        assert_eq!(still_listed, 0, "the newest version should list none of v1's runs");
        gc::gc_once(&c).await.unwrap();
        tokio::time::sleep(test_knobs(false).gc_grace + Duration::from_millis(300)).await;
        gc::gc_once(&c).await.unwrap();
        let mut missing = 0;
        for p in &runs {
            if object_store::ObjectStoreExt::head(&*store, &object_store::path::Path::from(p.as_str())).await.is_err() {
                missing += 1;
            }
        }
        eprintln!("fork reference and GC (control {control}): {missing} of {} runs of v1 deleted; GC deleted {} runs in all", runs.len(), c.gc.runs_deleted.load(Relaxed));
        if control {
            assert!(missing > 0, "the control should see GC delete v1's runs");
        } else {
            assert_eq!(missing, 0, "GC deleted runs a fork's reference names");
        }
        c.stop().await;
        t.abandon().await;
    }
    teardown();
}

/// Step 5b: the merged reader asks SlateDB to skip the SSTs whose rows are all at or below the held version's X_d. A
/// version is published over the first writes, newer writes go to L0 SSTs above X_d (no compactor, so SlateDB still
/// holds every row): every key and range reads as the model, the old SSTs are skipped, the new ones are read. Control: a
/// reader that skips with a cut above its X_d (a cut whose runs it does not hold) loses the newer rows only SlateDB holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skip_below_xd_and_control_breaks() {
    let _s = SERIAL.lock().await;
    setup();
    // no compactor here: L0 SSTs come only from the explicit flushes, so SlateDB never stalls on a full L0
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(4 << 20, Relaxed);
    let mut rng = StdRng::seed_from_u64(55);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-skip";
    let keys = universe();
    let l = lc(&store);
    let mut t = l.create(id).await.unwrap();
    let mut m: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for k in keys.iter().step_by(2) {
        let v: Vec<u8> = (0..rng.gen_range(1..80)).map(|_| rng.gen()).collect();
        apply(&mut t, &mut m, vec![(k.clone(), Some(v))]).await;
    }
    flush(&t).await;
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    m.insert(crate::envelope::MARKER.to_vec(), b"E1".to_vec());
    // newer rows, above X_d: rewrites, new keys and deletes, flushed to L0 SSTs
    for _ in 0..3 {
        for k in keys.iter().skip(rng.gen_range(0..3)).step_by(3) {
            let v = if rng.gen_bool(0.2) { None } else { Some((0..rng.gen_range(1..80)).map(|_| rng.gen()).collect::<Vec<u8>>()) };
            apply(&mut t, &mut m, vec![(k.clone(), v)]).await;
        }
        flush(&t).await;
    }
    let s0 = super::seqfilter::SKIPPED.load(Relaxed);
    let wrong = skip_read_all(&t, &m, &keys).await;
    let skipped = super::seqfilter::SKIPPED.load(Relaxed) - s0;
    eprintln!("skip below X_d: wrong {wrong}, SlateDB SSTs skipped {skipped}");
    assert_eq!(wrong, 0);
    assert!(skipped > 0, "the SSTs at or below X_d are skipped");
    // control: a cut far above X_d
    super::seqfilter::CONTROL_EXTRA.store(1 << 40, Relaxed);
    let s1 = super::seqfilter::SKIPPED.load(Relaxed);
    let wrong = skip_read_all(&t, &m, &keys).await;
    eprintln!("control: L0 {:?}, runs {:?}, skipped {}", t.writer_l0(), t.writer_sorted_runs(), super::seqfilter::SKIPPED.load(Relaxed) - s1);
    super::seqfilter::CONTROL_EXTRA.store(0, Relaxed);
    eprintln!("skip below X_d, control (cut above X_d): wrong {wrong}");
    assert!(wrong > 0, "the control must lose the rows above X_d");
    // the boundary, exactly: one row written alone, in an SST of its own, at seq S. A cut at S - 1 reads it (the SST
    // holds a row above the cut); a cut at S skips it, and the read returns the older value.
    let key = "doc/plain/00007";
    let h = t.put(key, b"newest").await.unwrap();
    let seq = h.seqnum();
    flush(&t).await;
    let x_d = t.fx_view().unwrap().x_d;
    super::seqfilter::CONTROL_EXTRA.store(seq - 1 - x_d, Relaxed);
    let below = t.get(key.as_bytes()).await.unwrap().map(|b| b.to_vec());
    super::seqfilter::CONTROL_EXTRA.store(seq - x_d, Relaxed);
    let at = t.get(key.as_bytes()).await.unwrap().map(|b| b.to_vec());
    super::seqfilter::CONTROL_EXTRA.store(0, Relaxed);
    eprintln!("boundary: seq {seq}, X_d {x_d}, cut S-1 reads {:?}, cut S reads {:?}", below.as_deref().map(String::from_utf8_lossy), at.as_deref().map(String::from_utf8_lossy));
    assert_eq!(below.as_deref(), Some(&b"newest"[..]), "a cut below the row's seq keeps its SST");
    assert_ne!(at.as_deref(), Some(&b"newest"[..]), "a cut at the row's seq skips its SST (the control)");
    m.insert(key.as_bytes().to_vec(), b"newest".to_vec());
    // the customer filter: range reads inside one customer skip L0 SSTs without it (the rows above X_d); control: a
    // filter that says "not here" for every customer loses those rows
    let (a0, s0) = (super::seqfilter::CUST_ASKED.load(Relaxed), super::seqfilter::CUST_SKIPPED.load(Relaxed));
    let wrong = skip_read_all(&t, &m, &keys).await;
    eprintln!("customer filter: wrong {wrong}, asked {}, skipped {}", super::seqfilter::CUST_ASKED.load(Relaxed) - a0, super::seqfilter::CUST_SKIPPED.load(Relaxed) - s0);
    assert_eq!(wrong, 0);
    super::seqfilter::CONTROL_CUST_NONE.store(true, Relaxed);
    let wrong = skip_read_all(&t, &m, &keys).await;
    super::seqfilter::CONTROL_CUST_NONE.store(false, Relaxed);
    eprintln!("customer filter, control (always 'not here'): wrong {wrong}");
    assert!(wrong > 0, "the customer-filter control must lose rows");
    // off: the same answers
    super::seqfilter::SKIP_BELOW_XD.store(false, Relaxed);
    let wrong = skip_read_all(&t, &m, &keys).await;
    super::seqfilter::SKIP_BELOW_XD.store(true, Relaxed);
    assert_eq!(wrong, 0);
    t.abandon().await;
    teardown();
}

async fn skip_read_all(t: &Tenant, m: &BTreeMap<Vec<u8>, Vec<u8>>, keys: &[Vec<u8>]) -> u64 {
    let mut wrong = 0;
    for k in keys {
        if t.get(k).await.unwrap().map(|b| b.to_vec()) != m.get(k).cloned() {
            wrong += 1;
        }
    }
    for c in 0..8 {
        let (lo, hi) = crange(c);
        for rev in [false, true] {
            let got = as_vec(t.scan_range(&lo, Some(&hi), rev, None, 1 << 20).await.unwrap());
            if got != model_range(m, &lo, Some(&hi), rev, None) {
                wrong += 1;
            }
        }
    }
    wrong
}

/// P-perf-bytes, DECISIONS 2026-11-09 (option (b)), the reader-mode case of the reviewer's list with the REAL drop: a
/// follower (another process: its own lifecycle, reader mode) runs one command of two reads (one customer's range, then
/// another's); between them the writer absorbs, publishes, installs and its real drop job removes SlateDB's rows at or
/// below the new X_d. The follower refreshes between the reads as `Dbs::read` does (`fx_refresh_due`), so its second
/// read takes the new version and loses nothing. Control (`CONTROL_KEEP_VIEW`): the follower's command keeps the view of
/// its first read, as code that kept a view across steps would; the real drop must make it miss rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn perf_bytes_follower_reads_across_a_real_drop() {
    let _s = SERIAL.lock().await;
    setup();
    for control in [false, true] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = if control { "fx-pb-follow-ctl" } else { "fx-pb-follow" };
        let mut t = lc(&store).create(id).await.unwrap();
        let mut m = BTreeMap::new();
        for i in 0..360 {
            apply(&mut t, &mut m, vec![(ckey(i % 9, i), Some(vec![b'a'; 60]))]).await;
        }
        publish(&mut t, &small_opts(build::Reseal::All)).await;
        T_PIN_MS.store(350, Relaxed); // under the lease (400 ms)
        let c = start(&mut t, &store, &test_knobs(true)).await;
        let mut f = lc(&store).wake(id).await.unwrap();
        f.fx_refresh().await.unwrap();
        let (mut wrong, mut reads, mut dropped, mut errors) = (0u64, 0u64, 0u64, 0u64);
        for round in 0..6usize {
            let (a, b) = (round % 9, (round + 4) % 9);
            let res = hold::op_scope(async {
                hold::CONTROL_KEEP_VIEW.store(control, Relaxed);
                let (lo, hi) = crange(a);
                let first = f.scan_range(&lo, Some(&hi), false, None, 64 << 10).await;
                // the writer moves on: new rows of every customer, only in SlateDB until the absorption
                let mut mm = m.clone();
                for i in 0..90 {
                    apply(&mut t, &mut mm, vec![(ckey(i % 9, 1000 * (round + 1) + i), Some(vec![b'b' + round as u8; 60]))]).await;
                }
                flush(&t).await;
                while c.absorb_once().await.unwrap() > 0 {}
                t.fx_sync();
                let x_d = c.current().await.x_d;
                let (d, _) = c.drop_through(x_d, Duration::from_secs(20)).await.unwrap();
                // between the reads, as the server's `Dbs::read` does
                if f.fx_refresh_due() {
                    f.fx_refresh().await.unwrap();
                }
                let (lo, hi) = crange(b);
                let second = f.scan_range(&lo, Some(&hi), false, None, 64 << 10).await;
                hold::CONTROL_KEEP_VIEW.store(false, Relaxed);
                (first, second, mm, d)
            })
            .await;
            let (first, second, mm, d) = res;
            dropped += d;
            for (r, cust, model) in [(first, a, &m), (second, b, &mm)] {
                reads += 1;
                let (lo, hi) = crange(cust);
                match r {
                    Ok(got) => wrong += (as_vec(got) != model_range(model, &lo, Some(&hi), false, None)) as u64,
                    Err(_) => errors += 1,
                }
            }
            m = mm;
        }
        eprintln!("perf-bytes follower (control {control}): {reads} reads, {wrong} wrong, {errors} errors, {dropped} rows dropped by the real drop");
        assert!(dropped > 0, "the real drop removed nothing: the test proves nothing");
        if control {
            assert!(wrong > 0, "control: a follower that kept its first view across a real drop must miss rows");
        } else {
            assert_eq!((wrong, errors), (0, 0));
        }
        c.stop().await;
        let _ = f.abandon().await;
        t.abandon().await;
    }
    teardown();
}

/// A store that counts the GETs of SlateDB's polled objects (its manifest and `.compactions` files and their GC
/// boundaries), and passes everything on.
#[derive(Debug)]
struct PollCount {
    inner: Arc<dyn ObjectStore>,
    polls: std::sync::atomic::AtomicU64,
}

impl std::fmt::Display for PollCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PollCount")
    }
}

#[async_trait::async_trait]
impl ObjectStore for PollCount {
    async fn put_opts(&self, l: &object_store::path::Path, p: object_store::PutPayload, o: object_store::PutOptions) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(&self, l: &object_store::path::Path, o: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, l: &object_store::path::Path, o: object_store::GetOptions) -> object_store::Result<object_store::GetResult> {
        let s = l.as_ref();
        if !s.contains("/fx/") && (s.contains("/manifest/") || s.contains("/compactions/") || s.ends_with(".boundary")) {
            self.polls.fetch_add(1, Relaxed);
        }
        self.inner.get_opts(l, o).await
    }
    fn delete_stream(&self, l: futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(l)
    }
    fn list(&self, p: Option<&object_store::path::Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(p)
    }
    fn list_with_offset(&self, p: Option<&object_store::path::Path>, o: &object_store::path::Path) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list_with_offset(p, o)
    }
    async fn list_with_delimiter(&self, p: Option<&object_store::path::Path>) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, f: &object_store::path::Path, t: &object_store::path::Path, o: object_store::CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(f, t, o).await
    }
    async fn rename_opts(&self, f: &object_store::path::Path, t: &object_store::path::Path, o: object_store::RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(f, t, o).await
    }
}

/// fix-box-first-trip, `drop::SlateGate`: an idle writer's SlateDB compactor and worker poll the store at most every
/// `SLATE_IDLE_POLL_MS`, instead of every 20 and 10 ms; writes still fold (their absorbed L0 SSTs leave L0, the reads stay
/// exact); and the stop is not held by a held poll. Control: `SLATE_IDLE_POLL_MS` 0 (no gate, the code before).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bft_idle_slatedb_polls_are_held() {
    let _s = SERIAL.lock().await;
    setup();
    let mut seen = Vec::new();
    for idle_ms in [5_000u64, 0] {
        super::drop::SLATE_IDLE_POLL_MS.store(idle_ms, Relaxed);
        let pc = Arc::new(PollCount { inner: Arc::new(InMemory::new()), polls: Default::default() });
        let store: Arc<dyn ObjectStore> = pc.clone();
        let id = format!("fx-bft-poll-{idle_ms}");
        let mut t = lc(&store).create(&id).await.unwrap();
        let mut m = BTreeMap::new();
        for i in 0..100 {
            apply(&mut t, &mut m, vec![(ckey(5, i), Some(b"v".to_vec()))]).await;
        }
        publish(&mut t, &small_opts(build::Reseal::All)).await;
        let c = Compactor::start(&mut t, Arc::clone(&store), test_knobs(true), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
        // writes, absorbed and folded: their L0 SSTs leave L0 while the gate is on
        for i in 0..200 {
            apply(&mut t, &mut m, vec![(ckey(6, i), Some(vec![b'w'; 40]))]).await;
        }
        flush(&t).await;
        c.absorb_once().await.unwrap();
        let t0 = Instant::now();
        while t.writer_l0().unwrap_or(0) > 0 && t0.elapsed() < Duration::from_secs(10) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let fold = t0.elapsed();
        let l0_left = t.writer_l0().unwrap_or(0);
        // idle for 2 s: SlateDB's polls
        tokio::time::sleep(Duration::from_millis(300)).await;
        let p0 = pc.polls.load(Relaxed);
        tokio::time::sleep(Duration::from_secs(2)).await;
        let idle_polls = pc.polls.load(Relaxed) - p0;
        for (key, want) in &m {
            assert_eq!(t.get(key).await.unwrap().as_deref(), Some(&want[..]));
        }
        let t1 = Instant::now();
        c.stop().await;
        let stop = t1.elapsed();
        t.abandon().await;
        eprintln!("idle poll {idle_ms} ms: L0 folded in {fold:?} ({l0_left} left), {idle_polls} polled GETs in 2 s idle, stop {stop:?}");
        seen.push((idle_ms, l0_left, idle_polls, stop));
    }
    super::drop::SLATE_IDLE_POLL_MS.store(120_000, Relaxed);
    teardown();
    let (_, l0_gate, polls_gate, stop_gate) = seen[0];
    let (_, _, polls_control, _) = seen[1];
    assert_eq!(l0_gate, 0, "with the gate the absorbed L0 SSTs are folded");
    assert!(polls_gate <= 8, "with the gate an idle database polls at most every 5 s ({polls_gate} GETs in 2 s)");
    assert!(polls_control >= 100, "control: without the gate it polls every 10-20 ms ({polls_control} GETs in 2 s)");
    assert!(stop_gate < Duration::from_secs(2), "the stop is not held by a held poll ({stop_gate:?})");
}

/// fix-idle-poll, `writer_gate`: an idle writer's manifest poll (every `WRITER_POLL_SECS`, 1 s here) is held once nothing
/// went through its store for `ACTIVE_WINDOW`, up to `WRITER_IDLE_POLL_SECS`; the next write releases it at once and
/// reads stay exact. Control: `SLATE_IDLE_POLL_MS` 0 (no gate): the writer keeps polling every second.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fip_idle_writer_manifest_poll_is_held() {
    let _s = SERIAL.lock().await;
    setup();
    let mut seen = Vec::new();
    for idle_ms in [120_000u64, 0] {
        super::drop::SLATE_IDLE_POLL_MS.store(idle_ms, Relaxed);
        let pc = Arc::new(PollCount { inner: Arc::new(InMemory::new()), polls: Default::default() });
        let store: Arc<dyn ObjectStore> = pc.clone();
        let id = format!("fx-fip-writer-{idle_ms}");
        let mut t = lc(&store).create(&id).await.unwrap();
        let mut m = BTreeMap::new();
        for i in 0..20 {
            apply(&mut t, &mut m, vec![(ckey(7, i), Some(b"v".to_vec()))]).await;
        }
        flush(&t).await;
        // past the active window: then 4 s of idle polls counted
        let held0 = super::writer_gate::WRITER_POLLS_HELD.load(Relaxed);
        let woken0 = super::writer_gate::WRITER_POLLS_WOKEN.load(Relaxed);
        tokio::time::sleep(super::writer_gate::ACTIVE_WINDOW + Duration::from_secs(1)).await;
        let p0 = pc.polls.load(Relaxed);
        tokio::time::sleep(Duration::from_secs(4)).await;
        let idle_polls = pc.polls.load(Relaxed) - p0;
        // a write releases the held poll
        apply(&mut t, &mut m, vec![(ckey(7, 99), Some(b"w".to_vec()))]).await;
        flush(&t).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let woken = super::writer_gate::WRITER_POLLS_WOKEN.load(Relaxed) - woken0;
        let held = super::writer_gate::WRITER_POLLS_HELD.load(Relaxed) - held0;
        for (key, want) in &m {
            assert_eq!(t.get(key).await.unwrap().as_deref(), Some(&want[..]));
        }
        let t1 = Instant::now();
        t.abandon().await;
        let close = t1.elapsed();
        eprintln!("idle hold {idle_ms} ms: {idle_polls} writer poll GETs in 4 s idle, held {held}, woken {woken}, close {close:?}");
        seen.push((idle_polls, held, woken, close));
    }
    super::drop::SLATE_IDLE_POLL_MS.store(120_000, Relaxed);
    teardown();
    let (polls_gate, held_gate, woken_gate, close_gate) = seen[0];
    let (polls_control, held_control, _, _) = seen[1];
    assert!(polls_gate <= 2, "with the gate an idle writer does not poll every second ({polls_gate} GETs in 4 s)");
    assert!(held_gate >= 1 && woken_gate >= 1, "the poll was held ({held_gate}) and the write released it ({woken_gate})");
    assert!(close_gate < Duration::from_secs(5), "the close is not held by a held poll ({close_gate:?})");
    assert!(polls_control >= 6, "control: without the gate the writer polls every second ({polls_control} GETs in 4 s)");
    assert_eq!(held_control, 0);
}

/// A store that counts the GETs of the drop lease (`{db}/fx/drop-lease`), and passes everything on.
#[derive(Debug)]
struct LeaseCount {
    inner: Arc<dyn ObjectStore>,
    reads: std::sync::atomic::AtomicU64,
}

impl std::fmt::Display for LeaseCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LeaseCount")
    }
}

#[async_trait::async_trait]
impl ObjectStore for LeaseCount {
    async fn put_opts(&self, l: &object_store::path::Path, p: object_store::PutPayload, o: object_store::PutOptions) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(&self, l: &object_store::path::Path, o: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, l: &object_store::path::Path, o: object_store::GetOptions) -> object_store::Result<object_store::GetResult> {
        if l.as_ref().ends_with("/fx/drop-lease") {
            self.reads.fetch_add(1, Relaxed);
        }
        self.inner.get_opts(l, o).await
    }
    fn delete_stream(&self, l: futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(l)
    }
    fn list(&self, p: Option<&object_store::path::Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(p)
    }
    fn list_with_offset(&self, p: Option<&object_store::path::Path>, o: &object_store::path::Path) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list_with_offset(p, o)
    }
    async fn list_with_delimiter(&self, p: Option<&object_store::path::Path>) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, f: &object_store::path::Path, t: &object_store::path::Path, o: object_store::CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(f, t, o).await
    }
    async fn rename_opts(&self, f: &object_store::path::Path, t: &object_store::path::Path, o: object_store::RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(f, t, o).await
    }
}

/// fix-idle-poll-2: while a checkpoint holds the drop lease the floor is 0 and cannot rise; an idle database observes
/// the floor (a drop-lease GET and a LIST of our manifest's versions) at most once per lease life, not every 5 s.
/// Control: `SLATE_IDLE_POLL_MS` 0 (no idle holds, the code before).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fip2_floor_not_polled_while_the_drop_lease_is_held() {
    let _s = SERIAL.lock().await;
    setup();
    let mut seen = Vec::new();
    for idle_ms in [120_000u64, 0] {
        super::drop::SLATE_IDLE_POLL_MS.store(idle_ms, Relaxed);
        let lc_store = Arc::new(LeaseCount { inner: Arc::new(InMemory::new()), reads: Default::default() });
        let store: Arc<dyn ObjectStore> = lc_store.clone();
        let id = format!("fx-fip2-lease-{idle_ms}");
        let mut t = lc(&store).create(&id).await.unwrap();
        let mut m = BTreeMap::new();
        for i in 0..50 {
            apply(&mut t, &mut m, vec![(ckey(8, i), Some(b"v".to_vec()))]).await;
        }
        publish(&mut t, &small_opts(build::Reseal::All)).await;
        let c = Compactor::start(&mut t, Arc::clone(&store), test_knobs(true), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
        // rows absorbed above the floor: the floor would rise once the lease ends (not settled without the lease rule)
        for i in 0..50 {
            apply(&mut t, &mut m, vec![(ckey(9, i), Some(b"w".to_vec()))]).await;
        }
        flush(&t).await;
        c.absorb_once().await.unwrap();
        assert!(c.drop.known_xd.load(Relaxed) > 0);
        let _lease = super::pair::take_lease(&store, &id).await.unwrap();
        // the first observations see the lease held; then 12 s counted
        // the compactor's planner, as its run loop drives it (every 50 ms)
        let ticks = |secs: u64| {
            let c = Arc::clone(&c);
            async move {
                let t0 = Instant::now();
                while t0.elapsed() < Duration::from_secs(secs) {
                    c.tick();
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        };
        ticks(6).await;
        let r0 = lc_store.reads.load(Relaxed);
        ticks(12).await;
        let reads = lc_store.reads.load(Relaxed) - r0;
        for (key, want) in &m {
            assert_eq!(t.get(key).await.unwrap().as_deref(), Some(&want[..]));
        }
        c.stop().await;
        t.abandon().await;
        eprintln!("idle hold {idle_ms} ms: {reads} drop-lease GETs in 12 s with the lease held");
        seen.push(reads);
    }
    super::drop::SLATE_IDLE_POLL_MS.store(120_000, Relaxed);
    teardown();
    assert!(seen[0] <= 1, "with the lease held an idle database read it {} times in 12 s", seen[0]);
    assert!(seen[1] >= 2, "control: the lease is read every few seconds ({} in 12 s)", seen[1]);
}
