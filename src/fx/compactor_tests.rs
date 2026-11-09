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

fn test_knobs(drop: bool) -> Knobs {
    Knobs {
        theta: 0.25,
        r3: true,
        r3_gets_per_gib: 470.0,
        r2_age: None,
        tier_t: 4,
        tier_unit: 2 << 10,
        absorb_min_l0: 1,
        absorb_max_l0: 8,
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
    }
}

fn setup() {
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

fn teardown() {
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

async fn flush(t: &Tenant) {
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
