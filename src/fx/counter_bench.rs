//! Measurements of compaction 2, step 6 (ignored tests, run by hand; results in `research/c2-step6-counter/results/`).
//!
//! `counter_ram`: RAM per hot end customer, before slices (whole blocks) and after (customer and area slices), at a
//! given block size. A chat-like database (messages of about 470 bytes, customers drawn zipf s = 1, an index
//! {customer, ts} co-located with the documents as in layout v2) is loaded into a tenant on an in-memory store and
//! sealed into runs. The visit's read is the server's "20 newest messages": a reverse scan of the customer's index area,
//! then the 20 documents by key. Hot customers: half drawn by activity, half uniformly (as in the fence-layout test).
//! Every answer is checked against the generator.
//!
//! `counter_reseal`: misses in the window after a reseal. A hot set is read in a stream with writes and some cold
//! customers; a miss is a read that needed a GET on our runs. The window is the reads of 10 minutes at the medium 1 TB
//! profile, scaled per hot customer (2,000 reads/s over 36,000 hot customers: 33 reads per hot customer). Three windows
//! on the same tenant: no reseal, a reseal with the staged publish, a reseal without it (the control).
//!
//! Environment: CB_MB (raw MB of messages, default 1024), CB_USERS (2000), CB_BLOCK_KB (256), CB_HOT (400), CB_SEED,
//! CB_OUT (a JSON file for the results).

use super::build::{BuildOpts, Reseal};
use super::counter::{self, SliceMode};
use super::tests::{enable, lc, publish, SERIAL};
use super::view;
use crate::lifecycle::{Op, Tenant};
use crate::runfmt::{CustomerKind, RunOptions};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

fn env(k: &str, d: u64) -> u64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn comp(c: usize) -> Vec<u8> {
    let name = format!("user{c:07}");
    let mut k = vec![name.len() as u8];
    k.extend(name.as_bytes());
    k
}

fn pre(c: usize) -> Vec<u8> {
    let mut k = b"u/".to_vec();
    k.extend(comp(c));
    k.extend(b"messages/");
    k
}

fn doc_key(c: usize, n: u64) -> Vec<u8> {
    let mut k = pre(c);
    k.extend(b"d/");
    k.extend(n.to_be_bytes());
    k
}

fn idx_key(c: usize, n: u64) -> Vec<u8> {
    let mut k = pre(c);
    k.extend(b"x/ts/");
    k.extend(n.to_be_bytes());
    k.extend(n.to_be_bytes());
    k
}

/// The document of message n of customer c: about 470 bytes, text-like (lz4 about 2x, like the chat bench).
fn body(c: usize, n: u64) -> Vec<u8> {
    const WORDS: [&str; 16] = ["hello", "agent", "memory", "step", "thread", "the", "user", "said", "tool", "call", "result", "and", "then", "plan", "reply", "ok"];
    let mut r = StdRng::seed_from_u64(n ^ ((c as u64) << 40));
    let mut v = format!("{{u:{c},n:{n},text:\"").into_bytes();
    while v.len() < 440 {
        v.extend(WORDS[r.gen_range(0..16)].as_bytes());
        v.push(b' ');
    }
    v.extend(b"\"}");
    v
}

struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    fn new(n: usize) -> Zipf {
        let mut cdf = Vec::with_capacity(n);
        let mut s = 0.0;
        for i in 0..n {
            s += 1.0 / (i + 1) as f64;
            cdf.push(s);
        }
        for x in cdf.iter_mut() {
            *x /= s;
        }
        Zipf { cdf }
    }
    fn sample(&self, r: &mut StdRng) -> usize {
        let u: f64 = r.gen();
        self.cdf.partition_point(|&x| x < u).min(self.cdf.len() - 1)
    }
}

/// The dataset: per customer, its message numbers in time order.
struct Data {
    users: usize,
    msgs: Vec<Vec<u64>>,
    next: u64,
    raw: u64,
}

async fn load(t: &mut Tenant, store: &Arc<dyn ObjectStore>, id: &str, mb: u64, users: usize, seed: u64) -> Data {
    let z = Zipf::new(users);
    let mut r = StdRng::seed_from_u64(seed);
    // user ids shuffled, so the heavy users are spread over the key space
    let mut ids: Vec<usize> = (0..users).collect();
    for i in (1..users).rev() {
        ids.swap(i, r.gen_range(0..=i));
    }
    let mut d = Data { users, msgs: vec![Vec::new(); users], next: 0, raw: 0 };
    let target = mb << 20;
    let t0 = std::time::Instant::now();
    while d.raw < target {
        let mut batch = Vec::with_capacity(2000);
        for _ in 0..1000 {
            let c = ids[z.sample(&mut r)];
            let n = d.next;
            d.next += 1;
            let b = body(c, n);
            d.raw += (b.len() + 2 * doc_key(c, n).len() + 8) as u64;
            batch.push(Op::Put(doc_key(c, n), b));
            batch.push(Op::Put(idx_key(c, n), n.to_be_bytes().to_vec()));
            d.msgs[c].push(n);
        }
        t.apply(batch).await.unwrap();
        if d.next % 50_000 == 0 {
            // SlateDB's own compaction (compaction-1's pass), or L0 fills up and writes stall
            crate::lifecycle::compact_tenant(store, store, id).await.unwrap();
        }
    }
    eprintln!("loaded {} messages, {} raw MB, {} users, {:.0} s", d.next, d.raw >> 20, users, t0.elapsed().as_secs_f64());
    d
}

/// The 20 newest messages of customer c, checked against the data. Returns true when the read needed a GET on our runs.
async fn newest(t: &Tenant, d: &Data, c: usize) -> bool {
    let g0 = view::RUN_GETS.load(Relaxed);
    let mut lo = pre(c);
    lo.extend(b"x/ts/");
    let hi = crate::runfmt::prefix_successor(&lo).unwrap();
    let ix = t.scan_range(&lo, Some(&hi), true, Some(20), 64 << 10).await.unwrap();
    let want: Vec<u64> = d.msgs[c].iter().rev().take(20).copied().collect();
    let got: Vec<u64> = ix.iter().map(|(_, v)| u64::from_be_bytes(v[..8].try_into().unwrap())).collect();
    assert_eq!(got, want, "index of customer {c}");
    for n in want {
        let v = t.get(doc_key(c, n)).await.unwrap();
        assert_eq!(v.as_deref(), Some(&body(c, n)[..]), "message {n} of customer {c}");
    }
    view::RUN_GETS.load(Relaxed) > g0
}

fn hot_set(d: &Data, k: usize, r: &mut StdRng) -> Vec<usize> {
    // half by activity (a random message's author), half uniformly, distinct, only customers with a message
    let all: Vec<(usize, u64)> = d.msgs.iter().enumerate().flat_map(|(c, v)| v.iter().map(move |n| (c, *n))).collect();
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    while out.len() < k / 2 {
        let c = all[r.gen_range(0..all.len())].0;
        if seen.insert(c) {
            out.push(c);
        }
    }
    while out.len() < k {
        let c = r.gen_range(0..d.users);
        if !d.msgs[c].is_empty() && seen.insert(c) {
            out.push(c);
        }
    }
    out
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if v.is_empty() {
        return 0.0;
    }
    v[((v.len() - 1) as f64 * p).round() as usize]
}

fn opts(reseal: Reseal, block_kb: u64) -> BuildOpts {
    BuildOpts { reseal, sealed: RunOptions::unchecked((block_kb as usize) << 10, 16 << 20, CustomerKind::V2), ..Default::default() }
}

fn write_out(v: serde_json::Value) {
    eprintln!("{}", serde_json::to_string_pretty(&v).unwrap());
    if let Ok(p) = std::env::var("CB_OUT") {
        std::fs::write(p, serde_json::to_string_pretty(&v).unwrap()).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn counter_ram() {
    let _s = SERIAL.lock().await;
    enable();
    let (mb, users, block_kb, hot_n, seed) = (env("CB_MB", 1024), env("CB_USERS", 2000) as usize, env("CB_BLOCK_KB", 256), env("CB_HOT", 400) as usize, env("CB_SEED", 7));
    let cap0 = view::CACHE_BYTES.swap(16 << 30, Relaxed);
    let mode0 = counter::SLICES.load(Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut t = lc(&store).create("bench-ram").await.unwrap();
    let d = load(&mut t, &store, "bench-ram", mb, users, seed).await;
    let t0 = std::time::Instant::now();
    let rep = publish(&mut t, &opts(Reseal::All, block_kb)).await;
    eprintln!("sealed in {:.0} s: {rep:?}", t0.elapsed().as_secs_f64());
    let v = t.fx_view().unwrap().clone();
    let mut maps = 0usize;
    let mut sizes: Vec<f64> = d.msgs.iter().filter(|m| !m.is_empty()).map(|m| m.len() as f64 * 470.0).collect();
    let users_with_data = sizes.len();
    let size_median = pct(&mut sizes, 0.5);
    let mut r = StdRng::seed_from_u64(seed + 1);
    let hot = hot_set(&d, hot_n, &mut r);
    let hot_comps: HashSet<Vec<u8>> = hot.iter().map(|c| comp(*c)).collect();
    let mut results = Vec::new();
    for mode in [SliceMode::Off, SliceMode::Customer, SliceMode::Area] {
        counter::SLICES.store(mode as u8, Relaxed);
        view::clear_cache();
        // the maps (run footers) are loaded by the first read and stay in RAM, outside the counter
        newest(&t, &d, hot[0]).await;
        view::clear_cache();
        let g0 = view::RUN_GETS.load(Relaxed);
        let mut cold_gets = Vec::new();
        for &c in &hot {
            let g = view::RUN_GETS.load(Relaxed);
            newest(&t, &d, c).await;
            cold_gets.push((view::RUN_GETS.load(Relaxed) - g) as f64);
        }
        let gets = view::RUN_GETS.load(Relaxed) - g0;
        let untrimmed = counter::stats();
        // between two visits the slices are trimmed to what the visit relied on (`TRIM_MS` after they were made)
        counter::trim_all();
        let g1 = view::RUN_GETS.load(Relaxed);
        for &c in &hot {
            newest(&t, &d, c).await;
        }
        let hot_gets = view::RUN_GETS.load(Relaxed) - g1;
        counter::trim_all();
        let st = counter::stats();
        let per = counter::bytes_per_customer(CustomerKind::V2, Some(&hot_comps));
        let mut each: Vec<f64> = hot.iter().map(|c| per.get(&comp(*c)).copied().unwrap_or(0.0)).collect();
        let mean_total = st.bytes as f64 / hot.len() as f64;
        let mean_each = each.iter().sum::<f64>() / each.len() as f64;
        let (p50, p95, max) = (pct(&mut each, 0.5), pct(&mut each, 0.95), pct(&mut each, 1.0));
        let mut cg = cold_gets.clone();
        let line = serde_json::json!({
            "mode": format!("{mode:?}"), "hot_customers": hot.len(), "counter_bytes": st.bytes, "counter_bytes_before_trim": untrimmed.bytes,
            "ram_per_hot_customer_before_trim": untrimmed.bytes as f64 / hot.len() as f64, "blocks": st.blocks, "slices": st.slices,
            "ram_per_hot_customer_mean": mean_total, "charged_mean": mean_each, "charged_p50": p50, "charged_p95": p95, "charged_max": max,
            "cold_gets_per_read_mean": gets as f64 / hot.len() as f64, "cold_gets_per_read_p99": pct(&mut cg, 0.99), "hot_gets": hot_gets,
        });
        eprintln!("{line}");
        results.push(line);
        if maps == 0 {
            maps = v.groups.iter().filter_map(|g| g.sealed.as_ref()).map(|_| 1).sum();
        }
    }
    counter::SLICES.store(mode0, Relaxed);
    view::CACHE_BYTES.store(cap0, Relaxed);
    write_out(serde_json::json!({
        "test": "counter_ram", "raw_mb": d.raw >> 20, "messages": d.next, "users": users, "users_with_data": users_with_data,
        "customer_bytes_median_estimated": size_median, "block_kb": block_kb, "sealed_runs": maps, "seed": seed,
        "build": format!("{rep:?}"), "modes": results,
    }));
    t.abandon().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn counter_reseal() {
    let _s = SERIAL.lock().await;
    enable();
    let (mb, users, block_kb, hot_n, seed) = (env("CB_MB", 1024), env("CB_USERS", 2000) as usize, env("CB_BLOCK_KB", 256), env("CB_HOT", 400) as usize, env("CB_SEED", 7));
    let per_hot = env("CB_READS_PER_HOT", 33) as usize;
    let cap0 = view::CACHE_BYTES.swap(16 << 30, Relaxed);
    let mode0 = counter::SLICES.swap(SliceMode::Area as u8, Relaxed);
    let staged0 = counter::STAGED.load(Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut t = lc(&store).create("bench-reseal").await.unwrap();
    let mut d = load(&mut t, &store, "bench-reseal", mb, users, seed).await;
    publish(&mut t, &opts(Reseal::All, block_kb)).await;
    let mut r = StdRng::seed_from_u64(seed + 2);
    let hot = hot_set(&d, hot_n, &mut r);
    let hot_set_: HashSet<usize> = hot.iter().copied().collect();
    let cold: Vec<usize> = (0..users).filter(|c| !d.msgs[*c].is_empty() && !hot_set_.contains(c)).collect();
    view::clear_cache();
    for &c in &hot {
        newest(&t, &d, c).await;
    }
    counter::trim_all();
    let z = Zipf::new(users);
    let window = per_hot * hot.len();
    let mut out = Vec::new();
    for variant in ["none", "staged", "unstaged"] {
        // the writes of the window before the reseal (medium profile: one write per two reads), then the reseal
        let mut batch = Vec::new();
        for _ in 0..window / 2 {
            let c = z.sample(&mut r) % users;
            let n = d.next;
            d.next += 1;
            batch.push(Op::Put(doc_key(c, n), body(c, n)));
            batch.push(Op::Put(idx_key(c, n), n.to_be_bytes().to_vec()));
            d.msgs[c].push(n);
            if batch.len() >= 2000 {
                t.apply(std::mem::take(&mut batch)).await.unwrap();
                if d.next % 50_000 == 0 {
                    crate::lifecycle::compact_tenant(&store, &store, "bench-reseal").await.unwrap();
                }
            }
        }
        if !batch.is_empty() {
            t.apply(batch).await.unwrap();
        }
        let mut stage = None;
        if variant != "none" {
            counter::STAGED.store(variant == "staged", Relaxed);
            let rep = publish(&mut t, &opts(Reseal::All, block_kb)).await;
            stage = rep.stage.clone();
            eprintln!("{variant}: reseal {rep:?}");
        }
        // the window: 99% reads of the hot set, 1% of cold customers (the medium profile's 20 cold reads in 2,000)
        let (mut hot_reads, mut hot_miss, mut cold_reads, mut cold_miss) = (0usize, 0usize, 0usize, 0usize);
        let mut miss_first_tenth = 0usize;
        for i in 0..window {
            let is_cold = r.gen_range(0..100) == 0;
            let c = if is_cold { cold[r.gen_range(0..cold.len())] } else { hot[r.gen_range(0..hot.len())] };
            let m = newest(&t, &d, c).await;
            if i % 100 == 99 {
                counter::trim_all(); // TRIM_MS of the server, compressed with the window
            }
            if is_cold {
                cold_reads += 1;
                cold_miss += m as usize;
            } else {
                hot_reads += 1;
                hot_miss += m as usize;
                if i < window / 10 {
                    miss_first_tenth += m as usize;
                }
            }
        }
        let line = serde_json::json!({
            "variant": variant, "window_reads": window, "hot_reads": hot_reads, "hot_misses": hot_miss,
            "hot_miss_pct": 100.0 * hot_miss as f64 / hot_reads.max(1) as f64, "hot_misses_first_tenth": miss_first_tenth,
            "cold_reads": cold_reads, "cold_misses": cold_miss,
            "all_miss_pct": 100.0 * (hot_miss + cold_miss) as f64 / window as f64, "stage": stage, "counter_bytes": counter::stats().bytes,
        });
        eprintln!("{line}");
        out.push(line);
    }
    counter::STAGED.store(staged0, Relaxed);
    counter::SLICES.store(mode0, Relaxed);
    view::CACHE_BYTES.store(cap0, Relaxed);
    let _ = HashMap::<u8, u8>::new();
    write_out(serde_json::json!({
        "test": "counter_reseal", "raw_mb_loaded": mb, "users": users, "block_kb": block_kb, "hot_customers": hot.len(),
        "reads_per_hot_customer_in_window": per_hot, "seed": seed, "windows": out,
    }));
    t.abandon().await;
}
