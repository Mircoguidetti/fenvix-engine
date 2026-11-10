//! Compaction 2, step 8: the migration measured (time, S3 requests per GB, write and read latency during it).
//!
//! Usage: `cargo run --release --bin fx_migrate_bench -- [D MiB 1024] [write MB/s during the migration 0.4] [group KiB 1024] [seed]`
//!
//! One database in process, in-memory store behind a request counter. It is loaded by today's engine (sealed runs off,
//! today's compaction pass running), then migrated online while a writer inserts and rewrites chat-like documents at
//! `rate` MB/s and a reader makes point reads, with today's pass running every second as in the server.
//!
//! The 1 TB ratio, time-compressed: what decides the catch-up is the bytes written during the migration against the
//! bytes migrated. At 1 TB that ratio is the write rate over the migration's rate, whatever D is, so the bench writes at
//! the real rates of the plan's profiles (medium 1,000 writes/s of about 400 bytes = 0.4 MB/s, busy 2 MB/s) while it
//! migrates a smaller D, and groups are scaled like D (1 GiB groups at 1 TB = 1 MiB groups at 1 GiB).
//!
//! Printed: migration time by phase, rows and bytes per second, the store's requests during the migration per GB of D
//! (GET, range GET, HEAD, LIST, PUT, multipart parts), the lock held by the publish, write and read latency (p50, p99,
//! max) before and during the migration, and a check of every key against the model after it.

use anyhow::Result;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tenant_bench::counting::CountingStore;
use tenant_bench::fx::{self, migrate};
use tenant_bench::lifecycle::{self, Lifecycle, Op, Tenant};
use tokio::sync::RwLock;

fn ckey(c: u32, i: u64) -> Vec<u8> {
    let name = format!("c{c:05}");
    let mut k = b"u/".to_vec();
    k.push(name.len() as u8);
    k.extend(name.as_bytes());
    k.extend(b"msgs/d/");
    k.extend(i.to_be_bytes());
    k
}

fn doc(rng: &mut StdRng, n: usize) -> Vec<u8> {
    const W: &[&str] = &["the", "agent", "replied", "with", "a", "summary", "of", "order", "status", "please", "check", "invoice", "refund"];
    let mut d = Vec::with_capacity(n + 8);
    while d.len() < n {
        d.extend(W[rng.gen_range(0..W.len())].as_bytes());
        d.push(b' ');
    }
    d
}

fn h(v: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut s = std::collections::hash_map::DefaultHasher::new();
    v.hash(&mut s);
    s.finish()
}

fn pct(v: &mut Vec<f64>, p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

struct Load {
    stop: AtomicBool,
    w_lat: Mutex<Vec<f64>>,
    r_lat: Mutex<Vec<f64>>,
}

type Model = Arc<Mutex<HashMap<Vec<u8>, u64>>>;

/// Writes at `rate` MB/s (70% new documents, 30% rewrites) and point reads, until stopped.
async fn load(t: Arc<RwLock<Tenant>>, model: Model, keys: Arc<Mutex<Vec<Vec<u8>>>>, rate: f64, l: Arc<Load>, seed: u64) {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut next: u64 = 1 << 40;
    let start = Instant::now();
    let mut bytes = 0f64;
    while !l.stop.load(Relaxed) {
        // pace: bytes written so far against the rate
        let due = bytes / (rate * 1e6);
        let el = start.elapsed().as_secs_f64();
        if due > el {
            tokio::time::sleep(Duration::from_secs_f64((due - el).min(0.05))).await;
        }
        let mut ops = Vec::new();
        let mut m = Vec::new();
        for _ in 0..4 {
            let k = if rng.gen_bool(0.3) {
                let ks = keys.lock().unwrap();
                ks[rng.gen_range(0..ks.len())].clone()
            } else {
                next += 1;
                let k = ckey(rng.gen_range(0..4000), next);
                keys.lock().unwrap().push(k.clone());
                k
            };
            let v = doc(&mut rng, 400);
            bytes += (k.len() + v.len()) as f64;
            m.push((k.clone(), h(&v)));
            ops.push(Op::Put(k, v));
        }
        let t0 = Instant::now();
        t.write().await.apply(ops).await.expect("write");
        l.w_lat.lock().unwrap().push(t0.elapsed().as_secs_f64() * 1e3);
        model.lock().unwrap().extend(m);
        // two point reads per write batch
        for _ in 0..2 {
            let k = {
                let ks = keys.lock().unwrap();
                ks[rng.gen_range(0..ks.len())].clone()
            };
            let t0 = Instant::now();
            let got = t.read().await.get(&k).await;
            l.r_lat.lock().unwrap().push(t0.elapsed().as_secs_f64() * 1e3);
            let want = model.lock().unwrap().get(&k).copied();
            if let (Ok(Some(v)), Some(w)) = (&got, want) {
                assert_eq!(h(v), w, "wrong read during the load");
            }
        }
    }
}

async fn passes(store: Arc<dyn ObjectStore>, id: String, stop: Arc<AtomicBool>) {
    while !stop.load(Relaxed) {
        let _ = lifecycle::compact_tenant(&store, &store, &id).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn snap_line(s: &tenant_bench::counting::Snapshot, gb: f64) -> String {
    format!(
        "GET {} (range GETs among them {}), HEAD {}, LIST {}, PUT {}, multipart parts {} (creates {}, completes {}), DELETE {}; per GB of D: GET {:.0}, HEAD {:.0}, LIST {:.0}, PUT-class {:.0}",
        s.get, s.get_range, s.head, s.list, s.put, s.multipart_part, s.multipart_create, s.multipart_complete, s.delete,
        s.get as f64 / gb, s.head as f64 / gb, s.list as f64 / gb, (s.put + s.multipart_part + s.multipart_create + s.multipart_complete) as f64 / gb
    )
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let d_mib: u64 = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(1024);
    let rate: f64 = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(0.4);
    let group_kib: u64 = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(1024);
    let seed: u64 = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(8);
    println!("D {d_mib} MiB, writes during the migration {rate} MB/s, groups {group_kib} KiB, seed {seed}");
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cs = Arc::new(CountingStore::new(Arc::clone(&mem)));
    let store: Arc<dyn ObjectStore> = cs.clone();
    let lc = Lifecycle::new(Arc::new(CountingStore::new(Arc::clone(&store))), None);
    let id = "mig-bench".to_string();
    fx::ENABLED.store(false, Relaxed);
    fx::T_PIN_MS.store(1000, Relaxed);
    fx::E1_WAIT_MS.store(1000, Relaxed);
    fx::E1_WAIT_TEST.store(true, Relaxed);
    migrate::GROUP_BYTES.store(group_kib << 10, Relaxed);

    // 1. today's engine loads D
    let mut rng = StdRng::seed_from_u64(seed);
    let model: Model = Arc::new(Mutex::new(HashMap::new()));
    let keys: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let mut t = lc.create(&id).await?;
    let t0 = Instant::now();
    let mut loaded = 0u64;
    let mut i = 0u64;
    let mut batches = 0u64;
    while loaded < d_mib << 20 {
        let mut ops = Vec::new();
        for _ in 0..200 {
            // Zipf-like customers: a few large, many small
            let c = ((rng.gen::<f64>().powf(2.5)) * 4000.0) as u32;
            let k = ckey(c, i);
            i += 1;
            let v = doc(&mut rng, 400);
            loaded += (k.len() + v.len()) as u64;
            model.lock().unwrap().insert(k.clone(), h(&v));
            keys.lock().unwrap().push(k.clone());
            ops.push(Op::Put(k, v));
        }
        t.apply(ops).await?;
        batches += 1;
        if batches % 200 == 0 {
            let _ = lifecycle::compact_tenant(&store, &store, &id).await;
        }
    }
    while lifecycle::compact_tenant(&store, &store, &id).await?.is_some() {}
    println!("loaded {} MiB raw ({} rows) in {:.1} s", loaded >> 20, i, t0.elapsed().as_secs_f64());
    let t = Arc::new(RwLock::new(t));

    // 2. baseline: 20 s of load before the migration, today's engine
    let stop_p = Arc::new(AtomicBool::new(false));
    let pass_task = tokio::spawn(passes(Arc::clone(&store), id.clone(), Arc::clone(&stop_p)));
    let l0 = Arc::new(Load { stop: AtomicBool::new(false), w_lat: Default::default(), r_lat: Default::default() });
    let lt = tokio::spawn(load(Arc::clone(&t), Arc::clone(&model), Arc::clone(&keys), rate, Arc::clone(&l0), seed + 1));
    tokio::time::sleep(Duration::from_secs(20)).await;
    l0.stop.store(true, Relaxed);
    lt.await?;
    let (mut bw, mut br) = (l0.w_lat.lock().unwrap().clone(), l0.r_lat.lock().unwrap().clone());

    // 3. release E1 with sealed runs: the migration, online
    fx::ENABLED.store(true, Relaxed);
    {
        let mut g = t.write().await;
        let old = std::mem::replace(&mut *g, lc.wake(&id).await?);
        drop(old);
        g.upgrade().await?;
    }
    let l1 = Arc::new(Load { stop: AtomicBool::new(false), w_lat: Default::default(), r_lat: Default::default() });
    let lt = tokio::spawn(load(Arc::clone(&t), Arc::clone(&model), Arc::clone(&keys), rate, Arc::clone(&l1), seed + 2));
    let c0 = cs.counters.snapshot();
    let b0 = migrate::SST_BYTES_READ.load(Relaxed);
    let m0 = Instant::now();
    loop {
        if t.write().await.fx_migrate_begin().await?.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let t_begin = m0.elapsed();
    let opts = migrate::sealed_opts();
    let mut rows = 0u64;
    loop {
        let r = migrate::build_step(&store, &id, 16, &opts, None).await?;
        rows += r.rows;
        if r.built {
            break;
        }
    }
    let t_build = m0.elapsed();
    let c_build = cs.counters.snapshot();
    let mut rounds = Vec::new();
    loop {
        let db = t.read().await.fx_writer_handle().unwrap();
        let r = migrate::round(&store, &id, &db, &migrate::delta_opts(), None).await?;
        let small = r.sst_bytes <= migrate::FINISH_ROUND_BYTES.load(Relaxed);
        rounds.push(r);
        if small || rounds.len() >= 6 {
            break;
        }
    }
    let t_rounds = m0.elapsed();
    let lk = Instant::now();
    let v = {
        let mut g = t.write().await;
        g.fx_migrate_finish(&migrate::delta_opts(), None).await?
    };
    let lock = lk.elapsed();
    let total = m0.elapsed();
    let c1 = cs.counters.snapshot().since(&c0);
    let sst_read = migrate::SST_BYTES_READ.load(Relaxed) - b0;
    tokio::time::sleep(Duration::from_secs(5)).await; // a little load after the publish (no compactor in this bench)
    l1.stop.store(true, Relaxed);
    lt.await?;
    stop_p.store(true, Relaxed);
    pass_task.await?;
    let gb = (d_mib as f64) / 1024.0;
    println!("published version {v:?}: begin {:.2} s (E1's wait included), build {:.2} s, rounds {:.2} s, publish under the lock {:.3} s; total {:.2} s",
        t_begin.as_secs_f64(), (t_build - t_begin).as_secs_f64(), (t_rounds - t_build).as_secs_f64(), lock.as_secs_f64(), total.as_secs_f64());
    let build_s = (t_build - t_begin).as_secs_f64();
    println!("build: {rows} rows, {:.0} rows/s, {:.1} s per GB of D, SlateDB SST bytes read during the whole migration {:.0} MiB ({:.2} x D)", rows as f64 / build_s, build_s / gb, sst_read as f64 / 1048576.0, sst_read as f64 / (d_mib << 20) as f64);
    for r in &rounds {
        println!("round: seq {} -> {}, {} new SSTs, {:.1} MiB of SSTs read, {} rows", r.x_from, r.x_to, r.new_ssts, r.sst_bytes as f64 / 1048576.0, r.rows);
    }
    println!("requests during the build: {}", snap_line(&c_build.since(&c0), gb));
    println!("requests during the whole migration (the load's own writes included): {}", snap_line(&c1, gb));
    let mut dw = l1.w_lat.lock().unwrap().clone();
    let mut dr = l1.r_lat.lock().unwrap().clone();
    println!("write latency ms, before: p50 {:.2} p99 {:.2} max {:.2} ({} batches); during: p50 {:.2} p99 {:.2} max {:.2} ({} batches)",
        pct(&mut bw, 0.5), pct(&mut bw, 0.99), pct(&mut bw, 1.0), bw.len(), pct(&mut dw, 0.5), pct(&mut dw, 0.99), pct(&mut dw, 1.0), dw.len());
    println!("read latency ms, before: p50 {:.3} p99 {:.3} max {:.3} ({} reads); during: p50 {:.3} p99 {:.3} max {:.3} ({} reads)",
        pct(&mut br, 0.5), pct(&mut br, 0.99), pct(&mut br, 1.0), br.len(), pct(&mut dr, 0.5), pct(&mut dr, 0.99), pct(&mut dr, 1.0), dr.len());
    // every key against the model, through the merged reader
    let g = t.read().await;
    let m = model.lock().unwrap().clone();
    let mut wrong = 0u64;
    let all = g.scan_range(b"", None, false, None, 8 << 20).await?;
    let mut seen = 0u64;
    for (k, v) in &all {
        if &k[..] == tenant_bench::envelope::MARKER {
            continue;
        }
        seen += 1;
        if m.get(&k[..]) != Some(&h(v)) {
            wrong += 1;
        }
    }
    let missing = m.len() as u64 - seen.min(m.len() as u64);
    println!("check after the publish: {} keys in the model, {} read, {} wrong, {} missing; served version {:?}", m.len(), seen, wrong, missing, g.fx_view().map(|v| (v.version, v.x_d, v.groups.len(), v.delta.len())));
    anyhow::ensure!(wrong == 0 && missing == 0, "wrong answers after the migration");
    Ok(())
}
