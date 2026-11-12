//! Compaction 2, step 4: the CPU pool test (plan round 2, fix 7, "the CPU cap across databases").
//!
//! Usage: `cargo run --release --bin fx_pool -- [databases 9] [D MiB per database 64] [insert MB/s per database 2.0]
//! [compressed days 6] [L0 KiB 64] [pool slots 3] [seed]`
//!
//! N databases in one process (in-memory store), each with its writer and its compactor (drop on), all compactors in
//! one pool (`fx/pool.rs`) whose jobs run on a runtime of `slots` threads (the 70% cap on 4 vCPU: 3 threads is 75%,
//! 2 is 50%). Each database is a busy profile, scaled: D bytes, and inserts so that D is 5.8 days of inserts (1 TB at
//! 5,000 writes/s of about 400 bytes, ESTIMATED in the plan), at `rate` MB/s, which sets the length of a compressed day.
//! Erasures: about 2 per compressed day per database (a random customer's documents deleted, then the compactor asked
//! to erase the customer's range), each due within one compressed day (the 24-hour deadline).
//!
//! Measured: the highest L0 of every database (SlateDB's stall limit is raised to 32, so L0 above 8 is visible, not a
//! stall), every erasure's time to done, the longest wait from "a reseal is due" to its commit, the reseals still due
//! at the end, the CPU of the pool's threads and of the whole process, the pool's waits by priority.

use anyhow::Result;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tenant_bench::counting::CountingStore;
use tenant_bench::fx;
use tenant_bench::lifecycle::{self, Lifecycle, Op};

const WORDS: &[&str] = &["the", "agent", "replied", "with", "a", "summary", "of", "order", "status", "please", "check", "invoice", "shipping", "tomorrow", "thanks", "customer", "support", "ticket", "update", "refund"];

fn doc(rng: &mut StdRng, n: usize) -> Vec<u8> {
    let mut d = Vec::with_capacity(n + 16);
    d.extend(format!("{{\"ts\":{},\"text\":\"", rng.gen::<u32>()).as_bytes());
    while d.len() < n {
        if rng.gen_bool(0.5) {
            d.extend(WORDS[rng.gen_range(0..WORDS.len())].as_bytes());
        } else {
            for _ in 0..rng.gen_range(3..9) {
                d.push(b'a' + rng.gen_range(0..26));
            }
        }
        d.push(b' ');
    }
    d.extend(b"\"}");
    d
}

fn cname(c: u32) -> Vec<u8> {
    let name = format!("c{c:05}");
    let mut k = b"u/".to_vec();
    k.push(name.len() as u8);
    k.extend(name.as_bytes());
    k
}

fn ckey(c: u32, i: u64) -> Vec<u8> {
    let mut k = cname(c);
    k.extend(b"msgs/d/");
    k.extend(i.to_be_bytes());
    k
}

struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    fn new(n: usize, s: f64) -> Zipf {
        let mut cdf = Vec::with_capacity(n);
        let mut acc = 0.0;
        for i in 1..=n {
            acc += 1.0 / (i as f64).powf(s);
            cdf.push(acc);
        }
        for x in cdf.iter_mut() {
            *x /= acc;
        }
        Zipf { cdf }
    }
    fn sample(&self, rng: &mut StdRng) -> u32 {
        let u: f64 = rng.gen();
        self.cdf.partition_point(|&x| x < u) as u32
    }
}

const CUSTOMERS: usize = 2000;

struct Gen {
    rng: StdRng,
    z: Zipf,
    next: Vec<u64>,
    written: u64,
}

impl Gen {
    fn batch(&mut self, n: usize) -> Vec<Op> {
        let mut ops = Vec::with_capacity(n);
        for _ in 0..n {
            let c = self.z.sample(&mut self.rng);
            let have = self.next[c as usize];
            let r: f64 = self.rng.gen();
            let len = self.rng.gen_range(250..550);
            if r < 0.75 || have == 0 {
                let k = ckey(c, have);
                self.next[c as usize] += 1;
                let v = doc(&mut self.rng, len);
                self.written += (k.len() + v.len()) as u64;
                ops.push(Op::Put(k, v));
            } else {
                let k = ckey(c, self.rng.gen_range(0..have));
                let v = doc(&mut self.rng, len);
                self.written += (k.len() + v.len()) as u64;
                ops.push(Op::Put(k, v));
            }
        }
        ops
    }
}

#[derive(Default)]
struct DbOut {
    max_l0: AtomicU64,
    max_write_ms: AtomicU64,
    writes: AtomicU64,
    erasures: Mutex<Vec<Duration>>,
}

fn cpu_of_threads(prefix: &str) -> f64 {
    // CPU seconds of this process's threads whose name starts with `prefix` (from /proc)
    let mut total = 0.0;
    let tick = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    if let Ok(rd) = std::fs::read_dir("/proc/self/task") {
        for e in rd.flatten() {
            let p = e.path();
            let name = std::fs::read_to_string(p.join("comm")).unwrap_or_default();
            if !name.starts_with(prefix) {
                continue;
            }
            if let Ok(stat) = std::fs::read_to_string(p.join("stat")) {
                let after = &stat[stat.rfind(')').map(|i| i + 2).unwrap_or(0)..];
                let f: Vec<&str> = after.split_whitespace().collect();
                if f.len() > 13 {
                    total += (f[11].parse::<f64>().unwrap_or(0.0) + f[12].parse::<f64>().unwrap_or(0.0)) / tick;
                }
            }
        }
    }
    total
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let n: usize = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(9);
    let d_mib: u64 = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(64);
    let rate: f64 = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(2.0);
    let days: f64 = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(6.0);
    let l0_kib: u64 = a.get(5).and_then(|s| s.parse().ok()).unwrap_or(64);
    let slots: usize = a.get(6).and_then(|s| s.parse().ok()).unwrap_or(3);
    let seed: u64 = a.get(7).and_then(|s| s.parse().ok()).unwrap_or(7);
    let d = d_mib << 20;
    let day = Duration::from_secs_f64((d as f64 / 5.8) / (rate * 1e6));
    println!("pool: {n} databases of {d_mib} MiB, {rate} MB/s of inserts each, L0 {l0_kib} KiB, {slots} pool threads, a compressed day is {day:?}, {days} days");
    tenant_bench::tiered::set_compression("lz4")?;
    fx::ENABLED.store(true, Relaxed);
    fx::E1_WAIT_MS.store(0, Relaxed);
    fx::T_PIN_MS.store(1000, Relaxed);
    fx::hold::HOLD_LIFE_MS.store(2000, Relaxed);
    lifecycle::WRITER_POLL_SECS.store(1, Relaxed);
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let lc = Lifecycle::new(Arc::new(CountingStore::new(Arc::clone(&mem))), None);
    let groups = 16u64;

    // load and seal every database with large L0 SSTs
    lifecycle::L0_SST_BYTES_OVERRIDE.store(16 << 20, Relaxed);
    lifecycle::L0_MAX_SSTS_OVERRIDE.store(10_000, Relaxed);
    let mut gens = Vec::new();
    for i in 0..n {
        let id = format!("busy{i}");
        let mut g = Gen { rng: StdRng::seed_from_u64(seed + i as u64), z: Zipf::new(CUSTOMERS, 1.1), next: vec![0; CUSTOMERS], written: 0 };
        let mut t = lc.create(&id).await?;
        while g.written < d {
            t.apply(g.batch(256)).await?;
        }
        let opts = fx::build::BuildOpts { reseal: fx::build::Reseal::All, group_bytes: d / groups, ..Default::default() };
        while t.fx_publish(&opts, None).await?.is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        t.close().await?;
        gens.push(g);
    }
    println!("loaded and sealed {n} databases");
    lifecycle::L0_SST_BYTES_OVERRIDE.store(l0_kib << 10, Relaxed);
    lifecycle::L0_MAX_SSTS_OVERRIDE.store(32, Relaxed);
    let rt: &'static tokio::runtime::Runtime = Box::leak(Box::new(tokio::runtime::Builder::new_multi_thread().worker_threads(slots).thread_name("fx-pool").enable_all().build()?)); // lives for the whole process
    let pool = fx::pool::Pool::new(slots);
    let day_ms = day.as_millis() as u64;
    let knobs = fx::compactor::Knobs {
        theta: 0.25,
        r3: false,
        tier_unit: 4 * (l0_kib << 10),
        absorb_min_l0: 2,
        absorb_max_l0: 8,
        recognise_in_background: false,
        local_max_bytes: 0,
        split_bytes: 2 * d / groups,
        merge_bytes: d / groups / 4,
        min_seal_bytes: d / groups / 2,
        drop: true,
        lease: Duration::from_millis(1500),
        remote_lease: Duration::from_millis(3500),
        serving_lease: Duration::from_millis(200),
        put_timeout: Duration::from_millis(1000),
        gc_grace: Duration::from_millis(3600),
        version_retention: Duration::ZERO,
        gc_every: Duration::from_millis((day_ms / 4).max(500)),
        tick: Duration::from_millis(5),
        ..Default::default()
    };
    let stop = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    let mut comps = Vec::new();
    let outs: Vec<Arc<DbOut>> = (0..n).map(|_| Arc::new(DbOut::default())).collect();
    for (i, mut g) in gens.into_iter().enumerate() {
        let id = format!("busy{i}");
        let mut t = lc.wake(&id).await?;
        t.upgrade().await?;
        let c = fx::compactor::Compactor::start(&mut t, Arc::clone(&mem), knobs.clone(), Arc::clone(&pool), rt.handle().clone()).await?;
        c.run();
        comps.push(Arc::clone(&c));
        let (stop, out) = (Arc::clone(&stop), Arc::clone(&outs[i]));
        let per_sec = rate * 1e6;
        writers.push(tokio::spawn(async move {
            let t0 = Instant::now();
            let start_written = g.written;
            let mut erase_rng = StdRng::seed_from_u64(i as u64 * 977);
            let mut next_erase = t0 + Duration::from_millis(erase_rng.gen_range(0..day_ms.max(1)) / 2);
            while !stop.load(Relaxed) {
                // pace: the bytes written so far may not run ahead of the rate
                let due = (g.written - start_written) as f64 / per_sec;
                let el = t0.elapsed().as_secs_f64();
                if due > el {
                    tokio::time::sleep(Duration::from_secs_f64(due - el)).await;
                }
                t.fx_sync();
                let w0 = Instant::now();
                t.apply(g.batch(32)).await?;
                out.max_write_ms.fetch_max(w0.elapsed().as_millis() as u64, Relaxed);
                out.writes.fetch_add(1, Relaxed);
                if let Some(l) = t.writer_l0() {
                    out.max_l0.fetch_max(l as u64, Relaxed);
                }
                if Instant::now() >= next_erase {
                    // an erasure: one customer's documents deleted, then the compactor asked to erase its range
                    let cust = erase_rng.gen_range(0..CUSTOMERS as u32);
                    let have = g.next[cust as usize];
                    if have > 0 {
                        let ops: Vec<Op> = (0..have).map(|j| Op::Delete(ckey(cust, j))).collect();
                        let commit = t.stage(ops).await?.unwrap();
                        let seq = commit.seqnum();
                        commit.wait().await?;
                        let lo = cname(cust);
                        let mut hi = lo.clone();
                        *hi.last_mut().unwrap() += 1;
                        c.erase(lo, hi, seq);
                        g.next[cust as usize] = 0;
                    }
                    next_erase = Instant::now() + Duration::from_millis(day_ms / 2);
                }
            }
            t.fx_sync();
            anyhow::Ok(t)
        }));
    }
    let cpu0 = fx::compactor::process_cpu_us();
    let pool_cpu0 = cpu_of_threads("fx-pool");
    let t0 = Instant::now();
    let run_for = Duration::from_secs_f64(day.as_secs_f64() * days);
    let mut last = Instant::now();
    while t0.elapsed() < run_for {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if last.elapsed() > Duration::from_secs(15) {
            last = Instant::now();
            let l0: Vec<u64> = outs.iter().map(|o| o.max_l0.load(Relaxed)).collect();
            let due: u64 = comps.iter().map(|c| c.sh.stats.reseals_due.load(Relaxed)).sum();
            println!("  {:.0} s ({:.1} days): max L0 {:?}, reseals due now {due}, pool busy {} waiting {}", t0.elapsed().as_secs_f64(), t0.elapsed().as_secs_f64() / day.as_secs_f64(), l0, pool.busy(), pool.waiting());
        }
    }
    stop.store(true, Relaxed);
    let wall = t0.elapsed().as_secs_f64();
    let cpu = (fx::compactor::process_cpu_us() - cpu0) as f64 / 1e6;
    let pool_cpu = cpu_of_threads("fx-pool") - pool_cpu0;
    let mut tenants = Vec::new();
    for w in writers {
        tenants.push(w.await??);
    }
    // catch-up: the reseals and erasures left when the writes stop, given one compressed day
    let settle = Instant::now();
    loop {
        let due: u64 = comps.iter().map(|c| c.sh.stats.reseals_due.load(Relaxed)).sum();
        let pending: usize = comps.iter().map(|c| c.sh.erasures.lock().unwrap().iter().filter(|e| e.done.is_none()).count()).sum();
        if (due == 0 && pending == 0) || settle.elapsed() > day {
            println!("after the writes stopped: reseals due {due}, erasures pending {pending}, after {:?}", settle.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut late = 0;
    let mut erasures = 0;
    let mut max_erase = Duration::ZERO;
    let mut max_lag = 0u64;
    let mut reseals = 0u64;
    let mut inserted = 0f64;
    for (i, c) in comps.iter().enumerate() {
        let s = &c.sh.stats;
        let e = c.sh.erasures.lock().unwrap().clone();
        for x in &e {
            erasures += 1;
            match x.done {
                Some(dn) => {
                    max_erase = max_erase.max(dn);
                    late += (dn > day) as u64;
                }
                None => late += 1,
            }
        }
        max_lag = max_lag.max(s.max_reseal_lag_us.load(Relaxed));
        reseals += s.reseals.load(Relaxed);
        inserted += s.absorbed_raw.load(Relaxed) as f64;
        println!("db {i}: max L0 {}, longest write {} ms, absorbs {}, merges {}, reseals {} (splits {}, group merges {}), erasures {} done {}, longest reseal wait {:.2} days, drop jobs {} rows dropped {}, job CPU {:.1} s", outs[i].max_l0.load(Relaxed), outs[i].max_write_ms.load(Relaxed), s.absorbs.load(Relaxed), s.merges.load(Relaxed), s.reseals.load(Relaxed), s.splits.load(Relaxed), s.group_merges.load(Relaxed), e.len(), s.erasures_done.load(Relaxed), s.max_reseal_lag_us.load(Relaxed) as f64 / 1e6 / day.as_secs_f64(), c.drop.jobs_done.load(Relaxed), c.drop.rows_dropped.load(Relaxed), s.job_cpu_us.load(Relaxed) as f64 / 1e6);
    }
    let max_l0 = outs.iter().map(|o| o.max_l0.load(Relaxed)).max().unwrap_or(0);
    println!("RESULT {n} databases, {slots} pool threads: max L0 over all databases {max_l0}; erasures {erasures}, late (over one compressed day) {late}, longest {:.2} days; longest reseal wait {:.2} days; reseals {reseals}", max_erase.as_secs_f64() / day.as_secs_f64(), max_lag as f64 / 1e6 / day.as_secs_f64());
    println!("RESULT CPU over {wall:.0} s: process {:.2} cores, pool threads {:.2} cores ({:.3} cores per database); inserted {:.1} MiB raw ({:.2} MB/s per database)", cpu / wall, pool_cpu / wall, pool_cpu / wall / n as f64, inserted / (1u64 << 20) as f64, inserted / wall / n as f64 / 1e6);
    for p in fx::pool::PRIOS {
        let s = &pool.stats[p as usize];
        let jobs = s.jobs.load(Relaxed).max(1);
        println!("RESULT pool {p:?}: jobs {}, mean wait {:.1} ms, longest wait {:.0} ms, mean held {:.1} ms", s.jobs.load(Relaxed), s.wait_us.load(Relaxed) as f64 / jobs as f64 / 1000.0, s.max_wait_us.load(Relaxed) as f64 / 1000.0, s.held_us.load(Relaxed) as f64 / jobs as f64 / 1000.0);
    }
    for c in &comps {
        c.stop().await;
    }
    for t in tenants {
        t.abandon().await;
    }
    Ok(())
}
