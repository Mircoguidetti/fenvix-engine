//! Tests of compaction 2, step 8: migration of a database written without sealed runs, and its rollback.
//!
//! `migration_model_with_crashes_and_takeovers` is the model test of the step: a database written by today's engine
//! (sealed runs off, plain values and SlateDB tombstones, today's compaction pass making sorted runs) moves to compaction
//! 2 while it serves writes and reads checked against an exact model, with crashes at named points inside every
//! migration step and takeovers by a new process (a new `Lifecycle`) at random; then it is rolled back to release E0
//! (sealed runs off), written there, migrated again (a second generation), committed (the drop on), and a release
//! without sealed runs is refused. Every acknowledged write is in the model; 0 wrong answers and 0 invariant violations
//! are required. After every crash, every run the migration state lists must open with its id ("recorded only once
//! durable").
//!
//! `planted_bug_mark_early_is_caught`: the same with `CONTROL_MARK_EARLY` (a group recorded before its run is complete)
//! and a crash at that point: the invariant check must find it.

use super::compactor::{Compactor, Knobs};
use super::migrate::{self, Phase};
use super::tests::{apply, as_vec, lc, model_range, universe, SERIAL, TEST_T_PIN_MS};
use super::*;
use crate::lifecycle::Tenant;
use crate::runfmt::{CustomerKind, RunOptions};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Duration;

const HOLD_MS: u64 = 3000;

fn knobs(drop: bool) -> Knobs {
    let mut k = Knobs::default();
    k.theta = 0.25;
    k.tier_unit = 2 << 10;
    k.absorb_min_l0 = 1;
    k.absorb_max_l0 = 8;
    k.local_max_bytes = 0;
    k.split_bytes = 40 << 10;
    k.merge_bytes = 18 << 10;
    k.min_seal_bytes = 8 << 10;
    k.sealed = RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2);
    k.delta = RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2);
    k.drop = drop;
    k.lease = Duration::from_millis(400);
    k.remote_lease = Duration::from_millis(HOLD_MS + 600);
    k.serving_lease = Duration::from_millis(300);
    k.put_timeout = Duration::from_millis(250);
    k.gc_grace = Duration::from_millis(HOLD_MS + 800);
    k.version_retention = Duration::from_millis(0);
    k.gc_every = Duration::from_millis(500);
    k.tick = Duration::from_millis(10);
    k.read_ahead = 64 << 10;
    k
}

fn run_opts() -> RunOptions {
    RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2)
}

fn setup() {
    tests::enable();
    ENABLED.store(false, Relaxed); // the database starts on today's engine
    hold::HOLD_LIFE_MS.store(HOLD_MS, Relaxed);
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(3 << 10, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(64, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(1, Relaxed);
    migrate::GROUP_BYTES.store(8 << 10, Relaxed);
    migrate::READ_AHEAD.store(64 << 10, Relaxed);
    migrate::CONTROL_MARK_EARLY.store(false, Relaxed);
    migrate::E0_GUARD_OFF.store(false, Relaxed);
    view::clear_cache();
}

fn teardown() {
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(30, Relaxed);
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
    T_PIN_MS.store(TEST_T_PIN_MS, Relaxed);
    migrate::CONTROL_MARK_EARLY.store(false, Relaxed);
    migrate::GROUP_BYTES.store(1 << 30, Relaxed);
    migrate::READ_AHEAD.store(8 << 20, Relaxed);
    ENABLED.store(false, Relaxed);
}

/// The compactor the server starts on a writer that serves a version: drop on only once the migration is committed.
async fn start_compactor(t: &mut Tenant, store: &Arc<dyn ObjectStore>) -> Option<Arc<Compactor>> {
    if !ENABLED.load(Relaxed) || t.fx_view().is_none() {
        return None;
    }
    let drop = t.fx_migration().is_some_and(|m| m.drop_allowed());
    let c = Compactor::start(t, Arc::clone(store), knobs(drop), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    c.run();
    Some(c)
}

#[derive(Default, Debug)]
struct St {
    ops: u64,
    wrong: u64,
    writes: u64,
    reads: u64,
    ranges: u64,
    retries: u64,
    crashes: u64,
    fail_points: u64,
    takeovers: u64,
    zombie_rounds_refused: u64,
    groups: u64,
    rounds: u64,
    publishes: u64,
    invariant: u64,
    passes: u64,
    e0_refused: u64,
}

struct World {
    store: Arc<dyn ObjectStore>,
    id: String,
    t: Option<Tenant>,
    c: Option<Arc<Compactor>>,
    m: BTreeMap<Vec<u8>, Vec<u8>>,
    keys: Vec<Vec<u8>>,
    rng: StdRng,
    st: St,
    fail_p: f64,
    trace: Vec<String>,
}

impl World {
    async fn reopen(&mut self, takeover: bool) {
        if let Some(c) = self.c.take() {
            c.stop().await;
        }
        let old = self.t.take().unwrap();
        let zombie = old.fx_writer_handle();
        old.abandon().await;
        // a takeover: another process (its own Lifecycle) opens the database while the old one may still be alive
        let mut nt = lc(&self.store).wake(&self.id).await.unwrap();
        nt.upgrade().await.unwrap();
        self.t = Some(nt);
        if takeover {
            self.st.takeovers += 1;
            // the old process's writer tries one more catch-up round: SlateDB fenced it, so it must fail
            if let Some(z) = zombie {
                if ENABLED.load(Relaxed) && migrate::load(&self.store, &self.id).await.unwrap().is_some_and(|s| s.phase == Phase::Building && s.built) {
                    match migrate::round(&self.store, &self.id, &z, &run_opts(), None).await {
                        Err(_) => self.st.zombie_rounds_refused += 1,
                        Ok(_) => self.trace.push("a zombie round succeeded".into()),
                    }
                }
            }
        } else {
            self.st.crashes += 1;
        }
        // as in the server: the compactor starts at the first write; reads, and today's pass on the L0 the dead process
        // left, may come first
        if self.rng.gen_bool(0.7) {
            for _ in 0..3 {
                self.pass().await;
                self.read().await;
            }
        }
        self.c = start_compactor(self.t.as_mut().unwrap(), &self.store).await;
        self.check_invariant().await;
    }

    async fn check_invariant(&mut self) {
        if let Some(s) = migrate::load(&self.store, &self.id).await.unwrap() {
            let bad = migrate::check_runs(&self.store, &s).await;
            if !bad.is_empty() {
                self.st.invariant += bad.len() as u64;
                if self.st.invariant <= 3 {
                    eprintln!("INVARIANT: {}", bad[0]);
                }
            }
        }
    }

    async fn write(&mut self) {
        let n = self.rng.gen_range(1..4);
        let mut ops = Vec::new();
        for _ in 0..n {
            let key = self.keys[self.rng.gen_range(0..self.keys.len())].clone();
            if self.rng.gen_bool(0.2) {
                ops.push((key, None));
            } else {
                let v: Vec<u8> = (0..self.rng.gen_range(0..160)).map(|_| self.rng.gen()).collect();
                ops.push((key, Some(v)));
            }
        }
        apply(self.t.as_mut().unwrap(), &mut self.m, ops).await;
        self.st.writes += 1;
    }

    async fn read(&mut self) {
        if let Some(c) = &self.c {
            let _ = c;
        }
        self.t.as_mut().unwrap().fx_sync();
        if self.rng.gen_bool(0.7) {
            let key = self.keys[self.rng.gen_range(0..self.keys.len())].clone();
            match self.t.as_mut().unwrap().get(&key).await {
                Ok(got) => {
                    if got.map(|b| b.to_vec()) != self.m.get(&key).cloned() {
                        self.st.wrong += 1;
                        if self.st.wrong <= 5 {
                            eprintln!("WRONG get {:?} at op {} (version {:?}, migration {:?})", String::from_utf8_lossy(&key), self.st.ops, self.t.as_mut().unwrap().fx_view().map(|v| (v.version, v.x_d)), self.t.as_mut().unwrap().fx_migration().map(|m| m.phase));
                        }
                    }
                }
                Err(e) if e.downcast_ref::<Retry>().is_some() => self.st.retries += 1,
                Err(e) => panic!("get: {e:#}"),
            }
            self.st.reads += 1;
        } else {
            let a = &self.keys[self.rng.gen_range(0..self.keys.len())];
            let b = &self.keys[self.rng.gen_range(0..self.keys.len())];
            let (lo, hi) = if a <= b { (a.clone(), b.clone()) } else { (b.clone(), a.clone()) };
            let rev = self.rng.gen_bool(0.3);
            let limit = if self.rng.gen_bool(0.5) { Some(self.rng.gen_range(1..60)) } else { None };
            match self.t.as_mut().unwrap().scan_range(&lo, Some(&hi), rev, limit, 64 << 10).await {
                Ok(got) => {
                    if as_vec(got) != model_range(&self.m, &lo, Some(&hi), rev, limit) {
                        self.st.wrong += 1;
                        if self.st.wrong <= 5 {
                            eprintln!("WRONG range at op {}", self.st.ops);
                        }
                    }
                }
                Err(e) if e.downcast_ref::<Retry>().is_some() => self.st.retries += 1,
                Err(e) => panic!("scan: {e:#}"),
            }
            self.st.ranges += 1;
        }
    }

    async fn full_check(&mut self, what: &str) {
        self.t.as_mut().unwrap().fx_sync();
        let got = as_vec(self.t.as_mut().unwrap().scan_range(b"", None, false, None, 1 << 20).await.unwrap());
        let want: Vec<(Vec<u8>, Vec<u8>)> = self.m.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        if got != want {
            let missing = want.iter().filter(|w| !got.contains(w)).count();
            let extra = got.iter().filter(|g| !want.contains(g)).count();
            eprintln!("FULL CHECK {what}: {missing} acknowledged rows missing or different, {extra} extra");
            self.st.wrong += 1 + missing as u64;
        }
    }

    async fn pass(&mut self) {
        let _ = crate::lifecycle::compact_tenant(&self.store, &self.store, &self.id).await;
        self.st.passes += 1;
    }

    fn fail_closure(&mut self) -> (Arc<std::sync::Mutex<Option<String>>>, u64) {
        (Arc::new(std::sync::Mutex::new(None)), self.rng.gen())
    }

    /// One step of the migration toward `target`, with a fail point picked at random; a failure there is a crash.
    async fn migrate_step(&mut self) {
        let s = migrate::load(&self.store, &self.id).await.unwrap();
        let (hit, seed) = self.fail_closure();
        let p = self.fail_p;
        let rng_cell = std::sync::Mutex::new(StdRng::seed_from_u64(seed));
        let hit2 = Arc::clone(&hit);
        let fail = move |at: &str| -> anyhow::Result<()> {
            if rng_cell.lock().unwrap().gen_bool(p) {
                *hit2.lock().unwrap() = Some(at.to_string());
                anyhow::bail!("fail point {at}");
            }
            Ok(())
        };
        let r: anyhow::Result<()> = match s.as_ref().map(|s| (s.phase, s.built)) {
            None | Some((Phase::RolledBack, _)) => match self.t.as_mut().unwrap().fx_migrate_begin().await {
                Ok(Some(_)) => {
                    self.m.insert(crate::envelope::MARKER.to_vec(), b"E1".to_vec());
                    Ok(())
                }
                Ok(None) => Ok(()),
                Err(e) => Err(e),
            },
            Some((Phase::Building, false)) => migrate::build_step(&self.store, &self.id, 1, &run_opts(), Some(&fail)).await.map(|r| self.st.groups += r.groups as u64),
            Some((Phase::Building, true)) => {
                if self.rng.gen_bool(0.5) {
                    let db = self.t.as_mut().unwrap().fx_writer_handle().unwrap();
                    migrate::round(&self.store, &self.id, &db, &run_opts(), Some(&fail)).await.map(|_| self.st.rounds += 1)
                } else {
                    match self.t.as_mut().unwrap().fx_migrate_finish(&run_opts(), Some(&fail)).await {
                        Ok(Some(v)) => {
                            self.st.publishes += 1;
                            self.trace.push(format!("published version {v} at op {}", self.st.ops));
                            // the server starts the compactor in the same exclusive section
                            self.c = start_compactor(self.t.as_mut().unwrap(), &self.store).await;
                            Ok(())
                        }
                        Ok(None) => Ok(()),
                        Err(e) => Err(e),
                    }
                }
            }
            _ => Ok(()),
        };
        if let Err(e) = r {
            if hit.lock().unwrap().is_some() {
                self.st.fail_points += 1;
                // the process dies at the fail point; another one takes over
                let tk = self.rng.gen_bool(0.5);
                self.reopen(tk).await;
            } else if e.downcast_ref::<migrate::Raced>().is_some() || format!("{e:#}").contains("taken meanwhile") {
                self.trace.push(format!("race: {e:#}"));
            } else {
                panic!("migration step: {e:#}");
            }
        }
    }

    async fn ops(&mut self, n: u64, migrate_until: Option<Phase>, crash_p: f64) {
        for _ in 0..n {
            self.st.ops += 1;
            let r = self.rng.gen_range(0..1000);
            if r < 450 {
                self.write().await;
            } else if r < 800 {
                self.read().await;
            } else if r < 840 {
                self.pass().await;
            } else if r < 990 {
                if let Some(target) = migrate_until {
                    let s = migrate::load(&self.store, &self.id).await.unwrap();
                    if s.as_ref().is_none_or(|s| s.phase != target) {
                        self.migrate_step().await;
                    }
                }
            } else if self.rng.gen_bool(crash_p) {
                let tk = self.rng.gen_bool(0.5);
                self.reopen(tk).await;
            }
        }
    }
}

async fn run_model(seed: u64, ops_per_phase: u64, fail_p: f64) -> (St, Vec<String>) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = format!("fx-m8-{seed}");
    let l = lc(&store);
    let t = l.create(&id).await.unwrap();
    let mut w = World { store: Arc::clone(&store), id: id.clone(), t: Some(t), c: None, m: BTreeMap::new(), keys: universe(), rng: StdRng::seed_from_u64(seed), st: St::default(), fail_p, trace: Vec::new() };
    // 1. today's engine: plain values, SlateDB tombstones, sorted runs from today's pass
    for i in 0..w.keys.len() {
        if i % 3 == 0 {
            let v: Vec<u8> = (0..w.rng.gen_range(0..120)).map(|_| w.rng.gen()).collect();
            let k = w.keys[i].clone();
            apply(w.t.as_mut().unwrap(), &mut w.m, vec![(k, Some(v))]).await;
        }
        if i % 200 == 0 {
            w.pass().await;
        }
    }
    w.ops(ops_per_phase / 2, None, 0.5).await;
    w.full_check("before the migration").await;
    // 2. release E1 with sealed runs: the migration, with crashes and takeovers, until published
    ENABLED.store(true, Relaxed);
    w.reopen(false).await;
    let mut guard = 0;
    while migrate::load(&store, &id).await.unwrap().is_none_or(|s| s.phase != Phase::Published) {
        w.ops(200, Some(Phase::Published), 0.3).await;
        guard += 1;
        assert!(guard < 200, "the migration did not publish: {:?}", migrate::load(&store, &id).await.unwrap().map(|s| (s.phase, s.built, s.groups.len())));
    }
    w.full_check("published").await;
    // 3. the window: the compactor runs with the drop off; crashes and takeovers
    w.ops(ops_per_phase, None, 0.5).await;
    w.full_check("in the window").await;
    // 4. rollback to E0. Even seeds: the control layer's rollback (stop the compactor, mark rolled back), then a release
    // with sealed runs off serves the database. Odd seeds: the operator just deploys E0; its first open rolls back.
    if seed % 2 == 0 {
        if let Some(c) = w.c.take() {
            c.stop().await;
        }
        w.t.as_mut().unwrap().fx_detach();
        w.t.as_mut().unwrap().fx_migrate_rollback("test").await.unwrap();
        w.full_check("rolled back, same process").await;
    }
    ENABLED.store(false, Relaxed);
    w.reopen(true).await;
    w.trace.push(format!("after E0 open: migration {:?}", migrate::load(&store, &id).await.unwrap().map(|s| s.phase)));
    w.full_check("E0 after rollback").await;
    w.ops(ops_per_phase / 2, None, 0.5).await;
    w.full_check("E0, written").await;
    // 5. back on E1: the old versions are ignored; a second generation, with crashes, until published
    ENABLED.store(true, Relaxed);
    w.reopen(true).await;
    assert!(w.t.as_mut().unwrap().fx_view().is_none(), "a writer serves a version of a rolled-back generation");
    let mut guard = 0;
    while migrate::load(&store, &id).await.unwrap().is_none_or(|s| s.phase != Phase::Published || s.gen < 2) {
        w.ops(200, Some(Phase::Published), 0.3).await;
        guard += 1;
        assert!(guard < 200, "the second generation did not publish");
    }
    w.full_check("generation 2 published").await;
    w.ops(ops_per_phase / 2, None, 0.5).await;
    // 6. commit: the drop on, crashes and takeovers
    if let Some(c) = w.c.take() {
        c.stop().await;
    }
    w.t.as_mut().unwrap().fx_detach();
    migrate::commit(&store, &id, true, "test").await.unwrap();
    w.reopen(false).await;
    assert!(w.c.as_ref().is_some_and(|_| true), "no compactor after the commit");
    w.ops(ops_per_phase, None, 0.5).await;
    if let Some(c) = &w.c {
        c.drain(false).await.unwrap();
    }
    w.full_check("committed").await;
    // a rollback is refused once committed
    if let Some(c) = w.c.take() {
        c.stop().await;
    }
    w.t.as_mut().unwrap().fx_detach();
    assert!(w.t.as_mut().unwrap().fx_migrate_rollback("test").await.is_err(), "a committed migration rolled back");
    // a release with sealed runs off refuses to open it
    ENABLED.store(false, Relaxed);
    let r = lc(&store).wake(&id).await;
    match r {
        Err(e) => {
            assert!(format!("{e:#}").contains("committed"), "{e:#}");
            w.st.e0_refused += 1;
        }
        Ok(_) => panic!("a release without sealed runs opened a committed database"),
    }
    ENABLED.store(true, Relaxed);
    w.reopen(false).await;
    w.full_check("end").await;
    if let Some(c) = w.c.take() {
        c.stop().await;
    }
    (w.st, w.trace)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn migration_model_with_crashes_and_takeovers() {
    let _s = SERIAL.lock().await;
    setup();
    let seeds: u64 = std::env::var("FX_8_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(2);
    let ops: u64 = std::env::var("FX_8_OPS").ok().and_then(|s| s.parse().ok()).unwrap_or(2000);
    let control = std::env::var("FX_8_CONTROL").unwrap_or_default();
    migrate::CONTROL_CATCHUP_L0_ONLY.store(control == "catchup_l0", Relaxed);
    migrate::CONTROL_NO_PASS_SKIP.store(control == "no_pass_skip", Relaxed);
    migrate::E0_GUARD_OFF.store(control == "e0_guard_off", Relaxed);
    let mut total = St::default();
    for i in 0..seeds {
        let r = tokio::spawn(run_model(20261110 + i, ops, 0.15)).await;
        let (st, trace) = match r {
            Ok(x) => x,
            Err(e) if !control.is_empty() => {
                // a control may also break an assertion of the scenario (a panic): that is caught too
                eprintln!("seed {i}: the scenario failed under control {control}: {e}");
                total.wrong += 1;
                continue;
            }
            Err(e) => panic!("seed {i}: {e}"),
        };
        eprintln!("seed {i}: {st:?}");
        for t in trace.iter().take(8) {
            eprintln!("  {t}");
        }
        total.wrong += st.wrong;
        total.invariant += st.invariant;
        total.crashes += st.crashes;
        total.takeovers += st.takeovers;
        total.fail_points += st.fail_points;
        total.ops += st.ops;
    }
    migrate::CONTROL_CATCHUP_L0_ONLY.store(false, Relaxed);
    migrate::CONTROL_NO_PASS_SKIP.store(false, Relaxed);
    migrate::E0_GUARD_OFF.store(false, Relaxed);
    teardown();
    eprintln!("control {control:?}: total: {} ops, {} crashes, {} takeovers, {} fail points, {} wrong, {} invariant violations", total.ops, total.crashes, total.takeovers, total.fail_points, total.wrong, total.invariant);
    if !control.is_empty() {
        // a negative control must be caught: wrong answers, missing writes or a broken scenario
        assert!(total.wrong + total.invariant > 0, "control {control} was not caught");
        return;
    }
    assert_eq!(total.invariant, 0, "a migration state listed a run that is not in the store");
    assert_eq!(total.wrong, 0, "wrong answers or acknowledged writes missing");
}

/// The planted bug: a group recorded before its run is complete in the store, and a crash right then.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn planted_bug_mark_early_is_caught() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-m8-plant";
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    let keys = universe();
    for (i, k) in keys.iter().enumerate() {
        apply(&mut t, &mut m, vec![(k.clone(), Some(format!("v{i}").into_bytes()))]).await;
    }
    ENABLED.store(true, Relaxed);
    t.abandon().await;
    let mut t = lc(&store).wake(id).await.unwrap();
    t.upgrade().await.unwrap();
    for _ in 0..50 {
        if t.fx_migrate_begin().await.unwrap().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(TEST_T_PIN_MS / 4)).await;
    }
    let mut caught = Vec::new();
    for control in [false, true] {
        migrate::CONTROL_MARK_EARLY.store(control, Relaxed);
        // build two groups, then crash at "before_finish" of the third
        migrate::build_step(&store, id, 2, &run_opts(), None).await.unwrap();
        let fail = |at: &str| -> anyhow::Result<()> {
            if at == "before_finish" {
                anyhow::bail!("crash");
            }
            Ok(())
        };
        assert!(migrate::build_step(&store, id, 1, &run_opts(), Some(&fail)).await.is_err());
        let s = migrate::load(&store, id).await.unwrap().unwrap();
        let bad = migrate::check_runs(&store, &s).await;
        eprintln!("control {control}: {} problems; {:?}", bad.len(), bad.first());
        caught.push(!bad.is_empty());
    }
    migrate::CONTROL_MARK_EARLY.store(false, Relaxed);
    // and with the bug, the reads after the publish go wrong too (the version lists a run that does not exist)
    migrate::CONTROL_MARK_EARLY.store(true, Relaxed);
    while !migrate::load(&store, id).await.unwrap().unwrap().built {
        migrate::build_step(&store, id, 0, &run_opts(), None).await.unwrap();
    }
    migrate::CONTROL_MARK_EARLY.store(false, Relaxed);
    let published = t.fx_migrate_finish(&run_opts(), None).await;
    let mut read_errors = 0;
    if published.is_ok() {
        for k in &keys {
            match t.get(k).await {
                Ok(v) if v.as_ref().map(|b| b.to_vec()) == m.get(k).cloned() => {}
                _ => read_errors += 1,
            }
        }
    }
    eprintln!("planted bug: publish {:?}, wrong or failed reads {read_errors} of {}", published.as_ref().map_err(|e| format!("{e:#}")), keys.len());
    teardown();
    assert!(!caught[0], "the check found a problem without the bug");
    assert!(caught[1], "the planted bug was not caught");
    assert!(published.is_err() || read_errors > 0, "the planted bug left no trace in the reads");
}
