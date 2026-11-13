//! P-sim-test (2026-11-13): a seeded simulation that searches the interleavings of late writes, takeovers, kills, GC,
//! erasure, compaction and publishes, instead of finding them one hand-written case at a time.
//!
//! - `net`: the simulated store (step 1).
//! - `cluster`: the simulated cluster, the script of a seed, the oracle, the minimizer (steps 2 and 3).
//!
//! Run (step 4): `SIM_SEEDS=0..200 SIM_PAR=4 cargo test --release --lib fx::sim::tests::sim_seeds -- --nocapture`.
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

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread().worker_threads(env_u64("SIM_THREADS", 4) as usize).enable_all().build().unwrap()
    }

    /// Run every seed of `SIM_SEEDS`, `SIM_PAR` at a time. A seed with a violation is replayed (`SIM_REPLAYS`) and
    /// minimized (`SIM_MIN_BUDGET` runs); its report goes to `SIM_OUT` (default `sim-out/`). Fails if any seed found one.
    #[test]
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
        eprintln!("SIM SUMMARY: seeds {total}, seeds with a violation {}, {:.0} s", failed.len(), t0.elapsed().as_secs_f64());
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
            for l in r.trace.iter().filter(|l| l.contains("->") || l.contains("mark") || l.contains("storm")) {
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
    /// passes with fix-sim-holes' W fix c94a477 (0 violations, MEASURED). Ignored until that fix is in: `--ignored`.
    #[test]
    #[ignore]
    fn sim_hole_w_generated_rows_then_a_builder_reseal() {
        let rt = runtime();
        let _s = rt.block_on(crate::fx::tests::SERIAL.lock());
        let _w = setup_world();
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
            CompStart { p: 1 },
            fill(1, 3, 0),
            Flush { p: 1 },
            CompDrain { p: 1 },
        ];
        let (f, v) = run_story(&rt, "hole W: generated rows, then a builder reseal", env_u64("SIM_KNOBS", 0) as u8, ev);
        assert_eq!(f, 0, "{v:?}");
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
