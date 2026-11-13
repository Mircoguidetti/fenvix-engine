//! P-sim-test (2026-11-13): a seeded simulation that searches the interleavings of late writes, takeovers, kills, GC,
//! erasure, compaction and publishes, instead of finding them one hand-written case at a time.
//!
//! - `net`: the simulated store (step 1).
//! - `cluster`: the simulated cluster, the script of a seed, the oracle, the minimizer (steps 2 and 3).
//!
//! Run (step 4): `SIM_SEEDS=0..200 SIM_PAR=4 cargo test --release --lib fx::sim::tests::sim_seeds -- --ignored --nocapture`.
//! Replay one seed (the minimal script with `SIM_KEEP`): `SIM_SEED=17 SIM_KEEP=0,4,9 cargo test --release --lib
//! fx::sim::tests::sim_replay -- --nocapture`.

pub mod cluster;
pub mod net;

#[cfg(test)]
mod tests {
    use super::cluster::*;
    use std::io::Write as _;
    use std::sync::Arc;

    fn env_u64(k: &str, d: u64) -> u64 {
        std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
    }

    fn seeds() -> Vec<u64> {
        let s = std::env::var("SIM_SEEDS").unwrap_or_else(|_| "0..8".into());
        if let Some((a, b)) = s.split_once("..") {
            (a.parse().unwrap()..b.parse().unwrap()).collect()
        } else {
            s.split(',').map(|x| x.trim().parse().unwrap()).collect()
        }
    }

    /// The script of a seed: `SIM_GEN=2` gives sim-test-2's scripts (`cluster::ext::generate2`: restore points, restores,
    /// forks, the generator, SlateDB's GC, storms, many groups); otherwise the scripts of sim-test.
    fn gen(seed: u64, steps: usize) -> Script {
        if env_u64("SIM_GEN", 1) == 2 {
            super::cluster::ext::generate2(seed, steps)
        } else {
            generate(seed, steps)
        }
    }

    /// fix-sim-holes' hole W counter, when that fix is in the build (0 otherwise).
    fn reseals_refused() -> u64 {
        0
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread().worker_threads(env_u64("SIM_THREADS", 4) as usize).enable_all().build().unwrap()
    }

    /// Run every seed of `SIM_SEEDS`, `SIM_PAR` at a time. A seed with a violation is replayed (`SIM_REPLAYS`) and
    /// minimized (`SIM_MIN_BUDGET` runs); its report goes to `SIM_OUT` (default `sim-out/`). Fails if any seed found one.
    /// Ignored in the ordinary suite (it runs minutes): `cargo test --release --lib fx::sim::tests::sim_seeds -- --ignored`.
    #[test]
    #[ignore]
    fn sim_seeds() {
        let rt = runtime();
        let _s = rt.block_on(crate::fx::tests::SERIAL.lock());
        let _w = setup_world();
        let steps = env_u64("SIM_STEPS", 90) as usize;
        let min_max = env_u64("SIM_MIN_MAX", 3) as usize;
        let par = env_u64("SIM_PAR", 4) as usize;
        let replays = env_u64("SIM_REPLAYS", 3) as usize;
        let budget = env_u64("SIM_MIN_BUDGET", 24) as usize;
        let out = std::env::var("SIM_OUT").unwrap_or_else(|_| "sim-out".into());
        std::fs::create_dir_all(&out).unwrap();
        let seeds = seeds();
        let total = seeds.len();
        let t0 = std::time::Instant::now();
        let failed: Vec<(u64, Vec<String>)> = rt.block_on(async {
            let sem = Arc::new(tokio::sync::Semaphore::new(par));
            let mut hs = Vec::new();
            for seed in seeds {
                let sem = Arc::clone(&sem);
                hs.push(tokio::spawn(async move {
                    let _p = sem.acquire_owned().await.unwrap();
                    let s = gen(seed, steps);
                    let r = run_script(&s).await;
                    eprintln!("SIM seed {seed}: {} violations; {}", r.violations.len(), r.stats);
                    (s, r)
                }));
            }
            let mut failed = Vec::new();
            for h in hs {
                let (s, r) = h.await.unwrap();
                if r.violations.is_empty() {
                    continue;
                }
                failed.push((s, r));
            }
            let mut out_v = Vec::new();
            for (n, (s, r)) in failed.into_iter().enumerate() {
                let budget = if n < min_max { budget } else { 0 };
                // replay the same seed: does it fail again?
                let mut again = 0;
                for _ in 0..replays {
                    if !run_script(&s).await.violations.is_empty() {
                        again += 1;
                    }
                }
                let (min, mr) = if budget > 0 { minimize(&s, 2, budget).await } else { (s.clone(), run_script(&s).await) };
                let mut rep = String::new();
                rep.push_str(&format!("SEED {} (steps {}): {} violations; replays failing {again} of {replays}\n{}\n\nVIOLATIONS\n", s.seed, s.events.len(), r.violations.len(), r.stats));
                for v in r.violations.iter().take(40) {
                    rep.push_str(&format!("  {v}\n"));
                }
                rep.push_str(&format!("\nMINIMAL SCRIPT ({} events; SIM_KEEP={}), its run: {} violations\n", min.events.len(), min.events.iter().map(|(i, _)| i.to_string()).collect::<Vec<_>>().join(","), mr.violations.len()));
                rep.push_str(&describe(&min));
                for v in mr.violations.iter().take(20) {
                    rep.push_str(&format!("  ! {v}\n"));
                }
                rep.push_str("\nTRACE OF THE MINIMAL RUN\n");
                for l in &mr.trace {
                    rep.push_str(&format!("  {l}\n"));
                }
                rep.push_str("\nFULL SCRIPT\n");
                rep.push_str(&describe(&s));
                let path = format!("{out}/seed-{}.txt", s.seed);
                std::fs::File::create(&path).unwrap().write_all(rep.as_bytes()).unwrap();
                eprintln!("SIM VIOLATION seed {}: replays failing {again}/{replays}; minimal {} events; report {path}; first: {}", s.seed, min.events.len(), r.violations.first().cloned().unwrap_or_default());
                out_v.push((s.seed, r.violations));
            }
            out_v
        });
        use std::sync::atomic::Ordering::Relaxed;
        eprintln!(
            "SIM SUMMARY: seeds {total}, seeds with a violation {}, {:.0} s; process-wide: WAL replay raises {}, older versions ignored {}, builder reseals refused {}",
            failed.len(),
            t0.elapsed().as_secs_f64(),
            crate::lifecycle::REPLAY_RAISES.load(Relaxed),
            crate::fx::manifest::OLDER_VERSIONS_IGNORED.load(Relaxed),
            reseals_refused()
        );
        assert!(failed.is_empty(), "seeds with a violation: {:?}", failed.iter().map(|(s, _)| *s).collect::<Vec<_>>());
    }

    /// A hand-written script through the same executor and oracle: the audit's case 14 (a zombie's erasure scrub
    /// commits into a slot the new owner's GC freed, the retire deletes live runs). Shows the oracle sees a known hole.
    #[test]
    fn sim_story_zombie_scrub() {
        let rt = runtime();
        let _s = rt.block_on(crate::fx::tests::SERIAL.lock());
        let _w = setup_world();
        use super::net::Profile;
        use Ev::*;
        let w = |p: usize, c: usize| Write { p, ops: (0..PER_CUSTOMER).map(|i| (c, i, false)).collect() };
        let mut ev = vec![Open { p: 0, writer: true }];
        for c in 0..CUSTOMERS {
            ev.push(w(0, c));
        }
        ev.extend([Publish { p: 0 }, CompStart { p: 0 }, Erase { p: 0, cust: 1 }, Flush { p: 0 }, CompDrain { p: 0 }, Open { p: 1, writer: true }, CompStart { p: 1 }, w(1, 4), Flush { p: 1 }, CompDrain { p: 1 }, w(1, 5), Flush { p: 1 }, CompDrain { p: 1 }, Gc { p: 1 }, ErasePass { p: 0 }, Sleep { ms: 2600 }, Retire { p: 1 }, Abandon { p: 0 }]);
        let s = Script { seed: 0, profile: Profile::none(), knobs: env_u64("SIM_KNOBS", 1) as u8, events: ev.into_iter().enumerate().map(|(i, e)| (i as u32, e)).collect(), ext: Default::default() };
        let n = env_u64("SIM_REPLAYS", 3);
        let mut failing = 0;
        for _ in 0..n {
            let r = rt.block_on(run_script(&s));
            for l in r.trace.iter().filter(|l| l.contains("->")) {
                eprintln!("  {l}");
            }
            for v in r.violations.iter().take(10) {
                eprintln!("VIOLATION {v}");
            }
            eprintln!("SIM STORY zombie scrub: {} violations; {}", r.violations.len(), r.stats);
            failing += (!r.violations.is_empty()) as u64;
        }
        eprintln!("SIM STORY zombie scrub: {failing} of {n} runs with a violation");
    }

    /// New hole Z (sim-test): a process that lost the database without noticing it (a zombie) closes it, as the idle
    /// reaper or a shutdown does. `Tenant::close` logs its failed memtable flush (fenced by the new writer) and still
    /// writes the clean marker. The new owner's acknowledged writes sit only in its WAL (it crashed, or abandoned);
    /// the next opening sees the marker, skips the WAL replay, and a reader misses them (until a writer opens).
    /// Fixed on fix-sim-holes: the marker names the writer epoch that wrote it and is trusted only when that is the stored
    /// manifest's (`lifecycle::marker_trusted`). Fails on integration-5-stack 837f969 and fb38790.
    #[test]
    fn sim_hole_zombie_close_writes_the_clean_marker() {
        let rt = runtime();
        let _s = rt.block_on(crate::fx::tests::SERIAL.lock());
        let _w = setup_world();
        use super::net::Profile;
        use Ev::*;
        let ev = vec![
            Open { p: 0, writer: true },
            Write { p: 0, ops: (0..10).map(|i| (0, i, false)).collect() },
            Flush { p: 0 },
            // the takeover: p0 has not noticed it
            Open { p: 1, writer: true },
            Write { p: 1, ops: (0..10).map(|i| (1, i, false)).collect() },
            // the new owner dies with its last writes only in its WAL
            Kill { p: 1 },
            // the zombie closes the database (idle reaper, or shutdown)
            Close { p: 0 },
        ];
        let s = Script { seed: 1, profile: Profile::none(), knobs: 1, events: ev.into_iter().enumerate().map(|(i, e)| (i as u32, e)).collect(), ext: Default::default() };
        let r = rt.block_on(run_script(&s));
        for l in r.trace.iter().filter(|l| l.contains("->")) {
            eprintln!("  {l}");
        }
        for v in r.violations.iter().take(6) {
            eprintln!("VIOLATION {v}");
        }
        eprintln!("SIM HOLE Z: {} violations; {}", r.violations.len(), r.stats);
        assert!(r.violations.is_empty(), "hole Z: a zombie's close left the clean marker and a reader misses acknowledged writes ({} violations)", r.violations.len());
    }

    /// New hole W (sim-test): a builder publish with `Reseal::All` (what `Entry`'s write path publishes, every
    /// `RESEAL_EVERY`-th build, while a database has no compactor: after a takeover or a reopen whose compactor start is
    /// deferred) builds every sealed run from SlateDB's rows only and leaves the base's delta runs out. Once a compactor
    /// with the drop has run, rows at or below its X_d live only in our runs: the publish loses them.
    /// Fixed on fix-sim-holes: with compactors on the builder never reseals over a base with runs (`build::
    /// allowed_reseal`). Fails on integration-5-stack 837f969 and fb38790.
    #[test]
    fn sim_hole_builder_reseal_after_the_drop() {
        let rt = runtime();
        let _s = rt.block_on(crate::fx::tests::SERIAL.lock());
        let _w = setup_world();
        use super::net::Profile;
        use Ev::*;
        let mut ev = vec![Open { p: 0, writer: true }, Write { p: 0, ops: (0..PER_CUSTOMER).map(|i| (0, i, false)).collect() }, Publish { p: 0 }, Write { p: 0, ops: vec![(0, 0, false)] }, CompStart { p: 0 }];
        // rows absorbed by the compactor, flushed often enough for SlateDB's fold (with the drop) to run
        for c in 1..CUSTOMERS {
            ev.push(Write { p: 0, ops: (0..PER_CUSTOMER).map(|i| (c, i, false)).collect() });
            ev.push(Flush { p: 0 });
            ev.push(CompDrain { p: 0 });
            ev.push(Sleep { ms: 300 });
        }
        // SlateDB's compaction with our drop: the rows at or below X_d now live only in our runs
        ev.push(DropThrough { p: 0 });
        // the database moves to another process, whose compactor has not started yet; its write path publishes
        ev.extend([Close { p: 0 }, Open { p: 1, writer: true }, Write { p: 1, ops: vec![(0, 1, false)] }, Publish { p: 1 }]);
        let s = Script { seed: 2, profile: Profile::none(), knobs: env_u64("SIM_KNOBS", 0) as u8, events: ev.into_iter().enumerate().map(|(i, e)| (i as u32, e)).collect(), ext: Default::default() };
        let refused = crate::fx::build::RESEALS_REFUSED.load(std::sync::atomic::Ordering::Relaxed);
        let r = rt.block_on(run_script(&s));
        let refused = crate::fx::build::RESEALS_REFUSED.load(std::sync::atomic::Ordering::Relaxed) - refused;
        eprintln!("SIM HOLE W: builder reseals refused {refused}");
        for l in r.trace.iter().filter(|l| l.contains("->")) {
            eprintln!("  {l}");
        }
        for v in r.violations.iter().take(6) {
            eprintln!("VIOLATION {v}");
        }
        eprintln!("SIM HOLE W: {} violations; {}", r.violations.len(), r.stats);
        assert!(r.violations.is_empty(), "hole W: a builder reseal after the drop lost rows ({} violations)", r.violations.len());
        assert!(refused > 0, "the script's builder publish over a base with runs was a reseal: it must be refused");
    }

    /// Known hole (audit case 1, and the a60f4fe review): the old owner's first build takes its version slot from a
    /// LIST that answers after a takeover (held here, as a slow answer or a frozen task), so its version lands ON TOP
    /// of the new owner's; on code without the reader-side guards the new owner's compactor builds on it. With
    /// `SIM_STORY_ABANDON=1` the old owner abandons the database instead of polling its build (a60f4fe waited for it).
    /// Run with `SIM_MAX_HOLD_MS=4000`. A check of the oracle on the old commits, not a test of the head.
    #[test]
    #[ignore]
    fn sim_story_first_build_lands_on_top() {
        let rt = runtime();
        let _s = rt.block_on(crate::fx::tests::SERIAL.lock());
        let _w = setup_world();
        use super::net::{Class, Op as NOp, Profile};
        use Ev::*;
        let mut ev = vec![Open { p: 0, writer: true }];
        for c in 0..3 {
            ev.push(Write { p: 0, ops: (0..PER_CUSTOMER).map(|i| (c, i, false)).collect() });
        }
        ev.push(Hold { p: 0, class: Class::FxVersion, op: Some(NOp::List), skip: 0, land: true, all: false, rid: 1 });
        ev.push(FirstStart { p: 0 });
        // the takeover: the new owner publishes, starts its compactor (after its catch-up publish) and absorbs
        ev.extend([
            Open { p: 1, writer: true },
            Write { p: 1, ops: (0..PER_CUSTOMER).map(|i| (3, i, false)).collect() },
            Publish { p: 1 },
            Write { p: 1, ops: (0..PER_CUSTOMER).map(|i| (4, i, false)).collect() },
            CompStart { p: 1 },
            Write { p: 1, ops: (0..PER_CUSTOMER).map(|i| (5, i, false)).collect() },
            Flush { p: 1 },
            CompDrain { p: 1 },
        ]);
        // the old owner's LIST answers now: its version lands on top
        ev.push(Release { rid: 1 });
        ev.push(Sleep { ms: 500 });
        if std::env::var_os("SIM_STORY_ABANDON").is_some() {
            ev.push(Abandon { p: 0 });
        } else {
            ev.push(FirstPoll { p: 0 });
        }
        // the new owner goes on: its compactor commits after the late version
        for c in [2, 1, 0] {
            ev.extend([Write { p: 1, ops: (0..PER_CUSTOMER).map(|i| (c, i, false)).collect() }, Flush { p: 1 }, CompDrain { p: 1 }]);
        }
        ev.push(DropThrough { p: 1 });
        let s = Script { seed: 3, profile: Profile::none(), knobs: env_u64("SIM_KNOBS", 1) as u8, events: ev.into_iter().enumerate().map(|(i, e)| (i as u32, e)).collect(), ext: Default::default() };
        let n = env_u64("SIM_REPLAYS", 1);
        let mut failing = 0;
        for _ in 0..n {
            let r = rt.block_on(run_script(&s));
            for l in r.trace.iter().filter(|l| l.contains("->") || l.contains("rule")) {
                eprintln!("  {l}");
            }
            for v in r.violations.iter().take(6) {
                eprintln!("VIOLATION {v}");
            }
            eprintln!("SIM STORY first build on top: {} violations; {}", r.violations.len(), r.stats);
            failing += (!r.violations.is_empty()) as u64;
        }
        eprintln!("SIM STORY first build on top: {failing} of {n} runs with a violation");
    }

    /// Run a hand-written script `SIM_REPLAYS` times (default 1); print its trace and violations; the number failing.
    fn run_story(rt: &tokio::runtime::Runtime, name: &str, knobs: u8, ev: Vec<Ev>) -> (u64, Vec<String>) {
        use super::net::Profile;
        let s = Script { seed: 7, profile: Profile::none(), knobs, events: ev.into_iter().enumerate().map(|(i, e)| (i as u32, e)).collect(), ext: Default::default() };
        let n = env_u64("SIM_REPLAYS", 1);
        let mut failing = 0;
        let mut last = Vec::new();
        for _ in 0..n {
            let r = rt.block_on(run_script(&s));
            let all = std::env::var_os("SIM_STORY_ALL").is_some();
            for l in r.trace.iter().filter(|l| all || l.contains("->") || l.contains("mark") || l.contains("storm")) {
                eprintln!("  {l}");
            }
            for v in r.violations.iter().take(8) {
                eprintln!("VIOLATION {v}");
            }
            eprintln!("SIM STORY {name}: {} violations; {}", r.violations.len(), r.stats);
            failing += (!r.violations.is_empty()) as u64;
            last = r.violations;
        }
        eprintln!("SIM STORY {name}: {failing} of {n} runs with a violation");
        (failing, last)
    }

    fn fill(p: usize, c: usize, tag: usize) -> Ev {
        let _ = tag;
        Ev::Write { p, ops: (0..PER_CUSTOMER).map(|i| (c, i, false)).collect() }
    }

    /// sim-test-2: a restore point, then the database goes on (writes, reseals and merges of the compactor, our GC with
    /// retention 0, an erasure scrubbed and retired, SlateDB's GC, the drop); a restore to a mark just after the point
    /// must give the state at the mark, during the run and at the end.
    #[test]
    fn sim_story_restore_after_gc_with_retention_0() {
        let rt = runtime();
        let _s = rt.block_on(crate::fx::tests::SERIAL.lock());
        let _w = setup_world();
        use Ev::*;
        let mut ev = vec![Open { p: 0, writer: true }];
        for c in 0..3 {
            ev.push(fill(0, c, 0));
        }
        // (a compactor starts after a write that follows the publish: the catch-up publish needs something new)
        ev.extend([Publish { p: 0 }, Write { p: 0, ops: vec![(5, 0, false)] }, CompStart { p: 0 }, Flush { p: 0 }, CompDrain { p: 0 }, Point { p: 1, tag: 1 }, Write { p: 0, ops: vec![(0, 1, false), (1, 2, true)] }, Mark { tag: 2 }]);
        for c in 3..CUSTOMERS {
            ev.extend([fill(0, c, 0), Flush { p: 0 }, CompDrain { p: 0 }]);
        }
        ev.extend([Erase { p: 0, cust: 2 }, Flush { p: 0 }, CompDrain { p: 0 }, ErasePass { p: 0 }, Gc { p: 0 }, Sleep { ms: 2700 }, Retire { p: 0 }, Gc { p: 0 }, DropThrough { p: 0 }, Gc { p: 0 }, SlateGc { p: 2 }, Restore { p: 0, mark: 2, apply: true }, fill(0, 4, 0)]);
        let (f, v) = run_story(&rt, "restore after GC with retention 0", env_u64("SIM_KNOBS", 0) as u8, ev);
        assert_eq!(f, 0, "{v:?}");
    }

    /// sim-test-2: a clone fork, then its source erases a customer (scrub, retire), reseals, collects (our GC with
    /// retention 0, SlateDB's GC, the drop), while a zombie of the source writes; the fork must read its state at the fork.
    #[test]
    fn sim_story_fork_then_source_collects() {
        let rt = runtime();
        let _s = rt.block_on(crate::fx::tests::SERIAL.lock());
        let _w = setup_world();
        use Ev::*;
        let copy = std::env::var_os("SIM_STORY_COPY").is_some();
        let mut ev = vec![Open { p: 0, writer: true }];
        for c in 0..4 {
            ev.push(fill(0, c, 0));
        }
        ev.extend([Publish { p: 0 }, Write { p: 0, ops: vec![(5, 0, false)] }, CompStart { p: 0 }, Flush { p: 0 }, CompDrain { p: 0 }, Fork { p: 0, copy, tag: 1 }, Open { p: 1, writer: true }, Write { p: 1, ops: vec![(5, 1, false)] }, CompStart { p: 1 }]);
        ev.extend([Erase { p: 1, cust: 1 }, fill(1, 4, 0), Flush { p: 1 }, CompDrain { p: 1 }, ErasePass { p: 1 }, fill(1, 5, 0), Flush { p: 1 }, CompDrain { p: 1 }, Gc { p: 1 }, Sleep { ms: 2700 }, Retire { p: 1 }, Gc { p: 1 }, DropThrough { p: 1 }, Gc { p: 1 }, SlateGc { p: 2 }, GcClosed { p: 2 }]);
        let (f, v) = run_story(&rt, "fork then the source collects", env_u64("SIM_KNOBS", 0) as u8, ev);
        assert_eq!(f, 0, "{v:?}");
    }

    /// sim-test-2: hole W through a second door. The ballast generator's parallel publish (3 writers,
    /// `publish_generated_parts`) puts every customer row in sealed runs only: SlateDB never holds them. The database
    /// then moves to another process whose compactor has not started; its write path's builder publish with
    /// `Reseal::All` rebuilds every group from SlateDB's rows and leaves the generated ones out: all of them are lost at
    /// once (no drop needed, unlike the first door). Fails on integration-5-stack fea18a34 (234 violations, MEASURED);
    /// passes with fix-sim-holes' W fix c94a477 (0 violations, MEASURED). Fixed on fix-sim-holes: a normal test.
    #[test]
    fn sim_hole_w_generated_rows_then_a_builder_reseal() {
        hole_w_generated(true);
    }

    /// fix-sim-holes: the second door with compactors off (`--no-sealed-compactor`) and the drop off: SlateDB never drops
    /// a row, yet the generated rows are in runs only. c94a477's refusal (compactors on or the drop on) let this reseal
    /// through; the generated version's `runs_only` mark refuses it.
    #[test]
    fn sim_hole_w_generated_rows_then_a_builder_reseal_without_compactors() {
        hole_w_generated(false);
    }

    fn hole_w_generated(compactors: bool) {
        let rt = runtime();
        let _s = rt.block_on(crate::fx::tests::SERIAL.lock());
        let _w = setup_world();
        // the world guard restores SERVER_ON; this one restores DROP_DEFAULT (also after a failed assert)
        struct DropDefault(bool);
        impl Drop for DropDefault {
            fn drop(&mut self) {
                crate::fx::compactor::DROP_DEFAULT.store(self.0, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let _d = DropDefault(crate::fx::compactor::DROP_DEFAULT.load(std::sync::atomic::Ordering::Relaxed));
        if !compactors {
            crate::fx::compactor::SERVER_ON.store(false, std::sync::atomic::Ordering::Relaxed);
            crate::fx::compactor::DROP_DEFAULT.store(false, std::sync::atomic::Ordering::Relaxed);
        }
        let refused = crate::fx::build::RESEALS_REFUSED.load(std::sync::atomic::Ordering::Relaxed);
        use super::net::Class;
        use Ev::*;
        let ev = vec![
            Open { p: 0, writer: true },
            // the generated version's create held on its way (a slow PUT): the takeover happens meanwhile
            Hold { p: 0, class: Class::FxVersion, op: Some(super::net::Op::Put), skip: 0, land: true, all: false, rid: 1 },
            Generate { p: 0, writers: 3 },
            Open { p: 1, writer: true },
            Write { p: 1, ops: vec![(1, 1, false), (2, 2, true)] },
            Publish { p: 1 },
            Release { rid: 1 },
            Sleep { ms: 1500 },
            Write { p: 1, ops: vec![(4, 4, false)] },
        ];
        let mut ev = ev;
        if compactors {
            ev.extend([CompStart { p: 1 }, fill(1, 3, 0), Flush { p: 1 }, CompDrain { p: 1 }]);
        } else {
            // no compactor ever: the write path's builder publishes again (a reseal every few builds)
            ev.extend([fill(1, 3, 0), Flush { p: 1 }, Publish { p: 1 }]);
        }
        let (f, v) = run_story(&rt, if compactors { "hole W: generated rows, then a builder reseal" } else { "hole W: generated rows, then a builder reseal, no compactors" }, env_u64("SIM_KNOBS", 0) as u8, ev);
        let refused = crate::fx::build::RESEALS_REFUSED.load(std::sync::atomic::Ordering::Relaxed) - refused;
        eprintln!("SIM HOLE W2 (compactors {compactors}): builder reseals refused {refused}");
        assert_eq!(f, 0, "{v:?}");
        assert!(refused > 0, "the builder publish over the generated runs was a reseal: it must be refused");
    }

    /// sim-test-2 candidate: GC decides from the store (drop lease free, no pair yet), then a restore point pairs with a
    /// version that is not the newest (the newest is a builder version whose X_d is above the checkpoint's last L0 seq),
    /// then GC deletes what it decided: the paired version and its runs. `SIM_STORY_LIVE=1`: the live GC of a zombie's
    /// compactor instead of the closed GC.
    #[test]
    fn sim_story_gc_decides_before_a_point_pairs() {
        let rt = runtime();
        let _s = rt.block_on(crate::fx::tests::SERIAL.lock());
        let _w = setup_world();
        use super::net::{Class, Op as NOp};
        use Ev::*;
        let mut ev = vec![Open { p: 0, writer: true }];
        for c in 0..3 {
            ev.push(fill(0, c, 0));
        }
        ev.extend([Publish { p: 0 }, Write { p: 0, ops: vec![(5, 0, false)] }, CompStart { p: 0 }, fill(0, 3, 0), Flush { p: 0 }, CompDrain { p: 0 }]);
        ev.extend([Close { p: 0 }, Open { p: 1, writer: true }, fill(1, 4, 0), Publish { p: 1 }, Abandon { p: 1 }, Sleep { ms: 2700 }]);
        ev.extend([
            Hold { p: 2, class: Class::FxVersion, op: Some(NOp::Delete), skip: 0, land: true, all: false, rid: 1 },
            GcClosed { p: 2 },
            Point { p: 0, tag: 1 },
            Release { rid: 1 },
            Sleep { ms: 1000 },
            Mark { tag: 2 },
            Restore { p: 0, mark: 2, apply: false },
        ]);
        let (f, v) = run_story(&rt, "GC decides before a point pairs", env_u64("SIM_KNOBS", 0) as u8, ev);
        assert_eq!(f, 0, "{v:?}");
    }

    /// New hole P (sim-test-2): a restore point pairs below the drop. The compactor's drop removed from SlateDB the rows
    /// at or below X_d (they live only in our runs). The database then moves to a process whose compactor has not started
    /// (or is starting: its catch-up publish); its builder publishes a version whose X_d is a WAL seq above the flushed L0.
    /// `pair::pair_checkpoint` picks the newest version with X_d <= the checkpoint's last L0 seq, so it passes over the
    /// newest and pairs with an older version (pinned by an earlier restore point), or with none once GC (retention 0,
    /// the server's default) deleted the older ones. `PairedReader` then reads SlateDB alone, or through a version that
    /// lacks the rows dropped after it: a restore to any time after the point misses them (and its apply deletes them
    /// from the live database). Two cases, each one run: no version (194 violations on head+ZW, MEASURED) and a pinned
    /// older version (96, MEASURED). Fails on integration-5-stack fea18a34 + fix-sim-holes' Z and W fixes; ignored until
    /// the fix is in: `--ignored`. (Hole W hides it on the bare head: run on a build with W fixed.) `SIM_STORY_FORK=1`: a
    /// fork instead of a restore point (safe: its quiesce flushes, so the newest version qualifies). Fixed on
    /// fix-sim-holes (`pair::pair_checkpoint` refuses to pair below the newest trusted version): a normal test.
    #[test]
    fn sim_hole_p_point_pairs_below_the_drop() {
        let rt = runtime();
        let _s = rt.block_on(crate::fx::tests::SERIAL.lock());
        let _w = setup_world();
        use Ev::*;
        let fork = std::env::var_os("SIM_STORY_FORK").is_some();
        let refused = crate::fx::pair::PAIRS_REFUSED_BELOW_NEWEST.load(std::sync::atomic::Ordering::Relaxed);
        let mut total = Vec::new();
        for pinned in [false, true] {
            // `pinned`: an earlier restore point pins an older version; the new point pairs with it, below the drop
            let mut ev = vec![Open { p: 0, writer: true }, fill(0, 0, 0), Publish { p: 0 }, Write { p: 0, ops: vec![(5, 0, false)] }, CompStart { p: 0 }];
            for c in 1..4 {
                ev.extend([fill(0, c, 0), Flush { p: 0 }, CompDrain { p: 0 }, Sleep { ms: 300 }]);
                if pinned && c == 1 {
                    ev.push(Point { p: 2, tag: 9 });
                }
            }
            // the compactor's drop: rows at or below X_d now live only in our runs
            ev.extend([DropThrough { p: 0 }, Close { p: 0 }]);
            // another process, no compactor yet: its builder publishes above the flushed L0, then it dies
            ev.extend([Open { p: 1, writer: true }, Write { p: 1, ops: vec![(4, 0, false), (4, 1, false)] }, Publish { p: 1 }, Abandon { p: 1 }]);
            // GC of the closed database, retention 0: only the newest version (and the pinned one) stays
            ev.extend([Sleep { ms: 2700 }, GcClosed { p: 2 }]);
            if fork {
                ev.extend([Open { p: 2, writer: true }, Fork { p: 2, copy: false, tag: 1 }]);
            } else {
                ev.extend([Point { p: 2, tag: 1 }, Sleep { ms: 50 }, Mark { tag: 2 }, Restore { p: 2, mark: 2, apply: false }]);
            }
            let (_, v) = run_story(&rt, if pinned { "hole P, a pinned older version" } else { "hole P, no version" }, env_u64("SIM_KNOBS", 0) as u8, ev);
            total.extend(v);
        }
        assert!(total.is_empty(), "hole P: a restore point paired below the drop ({} violations): {:?}", total.len(), total.iter().take(4).collect::<Vec<_>>());
        let refused = crate::fx::pair::PAIRS_REFUSED_BELOW_NEWEST.load(std::sync::atomic::Ordering::Relaxed) - refused;
        eprintln!("SIM HOLE P: pairings refused {refused}");
        if !fork {
            assert!(refused >= 2, "each case's restore point would pair below the newest version: it must be refused ({refused})");
        }
    }

    /// sim-test-2: the server's default erasure path (the ride: forced reseals, cleanses, the confirmation) with many
    /// small groups, then a takeover while the old owner (a zombie with its compactor) rides a second erasure and the new
    /// owner rides, collects and retires.
    #[test]
    fn sim_story_erase_ride_many_groups() {
        let rt = runtime();
        let _s = rt.block_on(crate::fx::tests::SERIAL.lock());
        let _w = setup_world();
        use Ev::*;
        let mut ev = vec![Open { p: 0, writer: true }];
        for c in 0..CUSTOMERS {
            ev.push(fill(0, c, 0));
        }
        ev.extend([Publish { p: 0 }, Write { p: 0, ops: vec![(5, 0, false)] }, CompStart { p: 0 }, Flush { p: 0 }, CompDrain { p: 0 }]);
        ev.extend([Erase { p: 0, cust: 1 }, Erase { p: 0, cust: 3 }, Flush { p: 0 }, CompDrain { p: 0 }, EraseRide { p: 0 }, Gc { p: 0 }, Sleep { ms: 2700 }, Retire { p: 0 }, Gc { p: 0 }]);
        ev.extend([fill(0, 1, 0), Erase { p: 0, cust: 4 }, Flush { p: 0 }, CompDrain { p: 0 }, Open { p: 1, writer: true }, Write { p: 1, ops: vec![(2, 2, false)] }, CompStart { p: 1 }, EraseRide { p: 0 }, Erase { p: 1, cust: 0 }, Flush { p: 1 }, CompDrain { p: 1 }, EraseRide { p: 1 }, DropThrough { p: 1 }, Gc { p: 1 }, Sleep { ms: 2700 }, Retire { p: 1 }, Gc { p: 1 }, Abandon { p: 0 }]);
        let (f, v) = run_story(&rt, "erase ride, many groups", env_u64("SIM_KNOBS", 2) as u8, ev);
        assert_eq!(f, 0, "{v:?}");
    }

    /// sim-test-2: the WAL replay raise (runwriter-and-stall, R-final-engine). L0 past the guard (8 flushed batches, no
    /// compactor), acknowledged writes only in the WAL, the owner killed; the next writer's open raises its unflushed
    /// limit by the WAL's size and replays; a zombie of it then writes and closes. Nothing acknowledged may be lost.
    #[test]
    fn sim_story_wal_replay_raise() {
        let rt = runtime();
        let _s = rt.block_on(crate::fx::tests::SERIAL.lock());
        let _w = setup_world();
        use Ev::*;
        let before = crate::lifecycle::REPLAY_RAISES.load(std::sync::atomic::Ordering::Relaxed);
        let mut ev = vec![Open { p: 0, writer: true }, fill(0, 0, 0), Publish { p: 0 }];
        for c in 1..CUSTOMERS {
            ev.extend([fill(0, c, 0), Flush { p: 0 }]);
        }
        for i in 0..4 {
            ev.extend([Write { p: 0, ops: vec![(1, i, false), (2, i, true)] }, Flush { p: 0 }]);
        }
        ev.extend([Write { p: 0, ops: vec![(3, 3, false), (4, 4, true)] }, Kill { p: 0 }, Open { p: 1, writer: true }, Write { p: 1, ops: vec![(5, 5, false)] }, Open { p: 2, writer: true }, Write { p: 1, ops: vec![(0, 1, false)] }, Close { p: 1 }, Write { p: 2, ops: vec![(0, 2, false)] }, CompStart { p: 2 }, Flush { p: 2 }, CompDrain { p: 2 }]);
        let (f, v) = run_story(&rt, "WAL replay raise", env_u64("SIM_KNOBS", 1) as u8, ev);
        let raises = crate::lifecycle::REPLAY_RAISES.load(std::sync::atomic::Ordering::Relaxed) - before;
        eprintln!("SIM STORY WAL replay raise: {raises} raises");
        assert_eq!(f, 0, "{v:?}");
    }

    /// sim-test-2: restore points around a takeover: the old owner (a zombie with its compactor) keeps committing (fenced
    /// or late), the new owner publishes with its builder, then starts its compactor; points are taken all along, and
    /// restores to marks between them must give the state at each mark.
    #[test]
    fn sim_story_points_around_a_takeover() {
        let rt = runtime();
        let _s = rt.block_on(crate::fx::tests::SERIAL.lock());
        let _w = setup_world();
        use super::net::{Class, Op as NOp};
        use Ev::*;
        let mut ev = vec![Open { p: 0, writer: true }];
        for c in 0..3 {
            ev.push(fill(0, c, 0));
        }
        ev.extend([Publish { p: 0 }, Write { p: 0, ops: vec![(5, 0, false)] }, CompStart { p: 0 }, Flush { p: 0 }, CompDrain { p: 0 }, Point { p: 2, tag: 1 }, Mark { tag: 2 }]);
        ev.extend([fill(0, 3, 0), Flush { p: 0 }, Hold { p: 0, class: Class::FxVersion, op: Some(NOp::Put), skip: 0, land: true, all: false, rid: 1 }, CompDrain { p: 0 }]);
        ev.extend([Open { p: 1, writer: true }, Write { p: 1, ops: vec![(4, 0, false), (1, 1, true)] }, Publish { p: 1 }, Point { p: 2, tag: 3 }, Release { rid: 1 }, Sleep { ms: 300 }, Mark { tag: 4 }, Point { p: 2, tag: 5 }]);
        ev.extend([Write { p: 1, ops: vec![(4, 2, false)] }, CompStart { p: 1 }, fill(1, 4, 0), Flush { p: 1 }, CompDrain { p: 1 }, DropThrough { p: 1 }, Gc { p: 1 }, Point { p: 2, tag: 6 }, Mark { tag: 7 }, Gc { p: 0 }, Abandon { p: 0 }, Sleep { ms: 2700 }, Gc { p: 1 }]);
        ev.extend([Restore { p: 1, mark: 2, apply: false }, Restore { p: 1, mark: 4, apply: false }, Restore { p: 1, mark: 7, apply: true }]);
        let (f, v) = run_story(&rt, "points around a takeover", env_u64("SIM_KNOBS", 0) as u8, ev);
        assert_eq!(f, 0, "{v:?}");
    }

    /// Hole Z through the fork's door (found by random seed 2117 of sim-test-2; fix-sim-holes item 5 changed
    /// `ctl/fork.rs` `quiesce` for it, without a test that runs a fork). A writer that lost the database without noticing
    /// it (p0) is asked to fork: the quiesce closes its tenant (an empty memtable: the close never reaches the store and
    /// writes the clean marker) and trusts the marker by a HEAD, so it skips the reopen that replays the WAL. The new
    /// owner's acknowledged writes, still only in its WAL, are missing from the fork. The simulated fork mirrors the
    /// `fork.rs` of its build: on `sim-test-2` the head's HEAD check (fails: 48 violations, MEASURED, 1 run); with
    /// fix-sim-holes' Z fix and `lifecycle::marker_clean` in the simulated quiesce: passes (MEASURED, 1 run).
    /// Fixed on fix-sim-holes (c7490ec's `marker_clean` in `fork.rs`, and in the simulated quiesce): a normal test.
    #[test]
    fn sim_hole_z_fork_trusts_a_fenced_writers_marker() {
        let rt = runtime();
        let _s = rt.block_on(crate::fx::tests::SERIAL.lock());
        let _w = setup_world();
        use Ev::*;
        let ev = vec![
            Open { p: 0, writer: true },
            fill(0, 0, 0),
            Flush { p: 0 },
            // the takeover: p0 has not noticed it
            Open { p: 1, writer: true },
            fill(1, 1, 0),
            fill(1, 2, 0),
            // the fork is asked of p0 (its process still believes it serves the database)
            Fork { p: 0, copy: false, tag: 1 },
        ];
        let (f, v) = run_story(&rt, "hole Z through the fork", 0, ev);
        assert_eq!(f, 0, "hole Z through the fork: {} violations: {:?}", v.len(), v.iter().take(3).collect::<Vec<_>>());
    }

    /// New hole F (sim-test-2, random seed 2296): a fork whose source is opened by another writer between its quiesce
    /// and its copy. `ctl/fork.rs` holds the source's lease for the whole fork, so this needs the forking process to lose
    /// that lease (a pause, as for every zombie case). The copy lists the source's live objects and takes the new
    /// writer's WAL, newer than the checkpoint; the fork's clean marker then makes the fork's readers skip that WAL while
    /// its writer replays it: a row the fork's readers saw vanishes when a writer opens it (deleted by the source's new
    /// owner), and rows never written to the fork appear. On the head: copy mode, 3 keys where the fork's reader and
    /// writer differ (MEASURED, 1 run; also on head + fix-sim-holes' Z and W fixes). Clone mode: 0 (MEASURED, 1 run).
    /// Proposed fix (`reviews/sim-test-2/hole-f-fork-source-written-during-fork.patch`, in `fork.rs` and in the simulated
    /// fork that mirrors it): the fork compares the source's writer epoch after the quiesce and after the copy, and rolls
    /// back when it moved. Fixed on fix-sim-holes: a normal test; it also checks that the copy-mode fork was refused.
    #[test]
    fn sim_hole_f_fork_source_written_after_its_quiesce() {
        let rt = runtime();
        let _s = rt.block_on(crate::fx::tests::SERIAL.lock());
        let _w = setup_world();
        use super::net::{Class, Op as NOp};
        use Ev::*;
        // the fork's drop-lease PUT stays on the wire for 4 s (longer than the step wait): the takeover happens meanwhile
        let old = std::env::var("SIM_MAX_HOLD_MS").ok();
        std::env::set_var("SIM_MAX_HOLD_MS", "5000");
        let refused0 = super::cluster::ext::FORKS_REFUSED_EPOCH_MOVED.load(std::sync::atomic::Ordering::Relaxed);
        let mut total = Vec::new();
        for copy in [true, false] {
            let ev = vec![
                Open { p: 0, writer: true },
                fill(0, 0, 0),
                Publish { p: 0 },
                Hold { p: 0, class: Class::FxOther, op: Some(NOp::Put), skip: 0, land: true, all: false, rid: 1 },
                Fork { p: 0, copy, tag: 1 },
                // meanwhile another process opens the source and writes (WAL only)
                Open { p: 1, writer: true },
                Write { p: 1, ops: vec![(1, 1, false), (2, 2, false), (0, 3, true)] },
                Release { rid: 1 },
                Sleep { ms: 3000 },
            ];
            let (_, v) = run_story(&rt, if copy { "hole F, copy" } else { "hole F, clone" }, 0, ev);
            total.extend(v);
        }
        match old {
            Some(v) => std::env::set_var("SIM_MAX_HOLD_MS", v),
            None => std::env::remove_var("SIM_MAX_HOLD_MS"),
        }
        assert!(total.is_empty(), "hole F: {} violations: {:?}", total.len(), total.iter().take(3).collect::<Vec<_>>());
        let refused = super::cluster::ext::FORKS_REFUSED_EPOCH_MOVED.load(std::sync::atomic::Ordering::Relaxed) - refused0;
        eprintln!("SIM HOLE F: forks refused {refused}");
        assert!(refused >= 1, "the copy-mode fork's source was opened by another writer during it: it must be refused");
    }

    /// Replay one seed (`SIM_SEED`), optionally only the events of `SIM_KEEP`, and print its trace.
    #[test]
    fn sim_replay() {
        let Some(seed) = std::env::var("SIM_SEED").ok().and_then(|v| v.parse().ok()) else { return };
        let rt = runtime();
        let _s = rt.block_on(crate::fx::tests::SERIAL.lock());
        let _w = setup_world();
        let mut s = gen(seed, env_u64("SIM_STEPS", 90) as usize);
        if let Ok(k) = std::env::var("SIM_KEEP") {
            let keep: Vec<u32> = k.split(',').map(|x| x.trim().parse().unwrap()).collect();
            s.events.retain(|(i, _)| keep.contains(i));
        }
        let n = env_u64("SIM_REPLAYS", 1);
        let mut failing = 0;
        for _ in 0..n {
            let r = rt.block_on(run_script(&s));
            eprintln!("{}", describe(&s));
            for l in &r.trace {
                eprintln!("  {l}");
            }
            for v in &r.violations {
                eprintln!("VIOLATION {v}");
            }
            eprintln!("SIM REPLAY seed {seed}: {} violations; {}", r.violations.len(), r.stats);
            if !r.violations.is_empty() {
                failing += 1;
            }
        }
        eprintln!("SIM REPLAY seed {seed}: {failing} of {n} runs with a violation");
    }
}
