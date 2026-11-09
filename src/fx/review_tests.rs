//! Reviewer's tests of compaction 2 step 6 (review-c2-step6, 2026-11-09). Not part of the owner's delivery.
//!
//! - `review_ram_alloc_and_patterns` (ignored, a measurement): the counter's own byte count against what the allocator
//!   really frees when the counter is dropped (glibc `mallinfo2`, allocator overhead included), for the owner's visit and
//!   for two other read patterns of the same customers: the newest 20 and the oldest 20, and the newest 20 and a random
//!   older page of 20.
//! - `review_agent_ram` (ignored, a measurement): RAM per hot agent thread for LangGraph's MongoDB saver layout, with the
//!   real checkpoint and pending-write sizes recorded by the checkpoint-delta harness (`RV_THREADS`, a TSV made from its
//!   `rows.csv.gz`).
//! - `review_filter_false_positives_8_runs`: absent customers checked against a sealed run and 7 delta runs that all span
//!   the key range, at 10 and 16 bits per customer.
//! - `review_sibling_slice_with_unparsed_area` (FAILS on 675c6dc, FIX 1): a customer with a collection whose name holds
//!   a '/', read through an area slice, then read whole: rows missing.
//! - `review_reseal_patterns` (ignored, a measurement): the counter right after a staged reseal and after the next
//!   visit, for the newest-20 visit and for newest + oldest.
//! - `review_staged_publish_warms_only_what_was_held` (FAILS on 675c6dc, FIX 2): a staged reseal of customers that read
//!   their newest and oldest rows fetches and keeps their whole histories.

use super::build::Reseal;
use super::counter::{self, SliceMode};
use super::counter_bench as cb;
use super::tests::{apply, as_vec, enable, lc, model_range, publish, SERIAL};
use super::view;
use crate::lifecycle::{Op, Tenant};
use crate::runfmt::{CustomerKind, RunOptions};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

/// Bytes the allocator has handed out and not taken back (glibc, every arena, mmapped chunks included).
fn heap() -> i64 {
    let m = unsafe { libc::mallinfo2() };
    (m.uordblks + m.hblkhd) as i64
}

/// What the allocator gets back when the counter is dropped (the counter is emptied by this call).
fn counter_heap_bytes() -> i64 {
    let a = heap();
    counter::clear();
    let b = heap();
    a - b
}

struct Mode(u8);
impl Mode {
    fn set(m: SliceMode) -> Mode {
        Mode(counter::SLICES.swap(m as u8, Relaxed))
    }
}
impl Drop for Mode {
    fn drop(&mut self) {
        counter::SLICES.store(self.0, Relaxed);
    }
}

/// A range of customer c's index, checked against the data: `oldest`: the k oldest; else the k newest below `skip`
/// newest ones. Then the documents by key.
async fn page(t: &Tenant, d: &cb::Data, c: usize, k: usize, oldest: bool, skip: usize) {
    let mut lo = cb::pre(c);
    lo.extend(b"x/ts/");
    let hi = crate::runfmt::prefix_successor(&lo).unwrap();
    let want: Vec<u64> = if oldest { d.msgs[c].iter().take(k).copied().collect() } else { d.msgs[c].iter().rev().skip(skip).take(k).copied().collect() };
    let got: Vec<u64> = if oldest {
        let ix = t.scan_range(&lo, Some(&hi), false, Some(k), 64 << 10).await.unwrap();
        ix.iter().map(|(_, v)| u64::from_be_bytes(v[..8].try_into().unwrap())).collect()
    } else {
        let ix = t.scan_range(&lo, Some(&hi), true, Some(k + skip), 64 << 10).await.unwrap();
        ix.iter().skip(skip).map(|(_, v)| u64::from_be_bytes(v[..8].try_into().unwrap())).collect()
    };
    assert_eq!(got, want, "index page of customer {c}");
    for n in want {
        let v = t.get(cb::doc_key(c, n)).await.unwrap();
        assert_eq!(v.as_deref(), Some(&cb::body(c, n)[..]), "message {n} of customer {c}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn review_ram_alloc_and_patterns() {
    let _s = SERIAL.lock().await;
    enable();
    let (mb, users, block_kb, hot_n, seed) = (cb::env("CB_MB", 1024), cb::env("CB_USERS", 2000) as usize, cb::env("CB_BLOCK_KB", 256), cb::env("CB_HOT", 400) as usize, cb::env("CB_SEED", 7));
    let cap0 = view::CACHE_BYTES.swap(16 << 30, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut t = lc(&store).create("review-ram").await.unwrap();
    let d = cb::load(&mut t, &store, "review-ram", mb, users, seed).await;
    publish(&mut t, &cb::opts(Reseal::All, block_kb)).await;
    let mut r = StdRng::seed_from_u64(seed + 1);
    let hot = cb::hot_set(&d, hot_n, &mut r);
    let hot_comps: HashSet<Vec<u8>> = hot.iter().map(|c| cb::comp(*c)).collect();
    let mut out = Vec::new();
    for pattern in ["newest20", "newest20+oldest20", "newest20+older-page"] {
        for mode in [SliceMode::Off, SliceMode::Area] {
            let _m = Mode::set(mode);
            view::clear_cache();
            cb::newest(&t, &d, hot[0]).await; // the run footers, outside the counter
            view::clear_cache();
            let mut pr = StdRng::seed_from_u64(seed + 9);
            // the older page of each customer is fixed, so visit 2 repeats visit 1
            let skips: Vec<usize> = hot.iter().map(|c| { let n = d.msgs[*c].len(); if n > 40 { pr.gen_range(20..n - 20) } else { 20 } }).collect();
            for _visit in 0..2 {
                for (i, &c) in hot.iter().enumerate() {
                    cb::newest(&t, &d, c).await;
                    match pattern {
                        "newest20+oldest20" => page(&t, &d, c, 20, true, 0).await,
                        "newest20+older-page" => page(&t, &d, c, 20, false, skips[i]).await,
                        _ => {}
                    }
                }
                counter::trim_all();
            }
            let st = counter::stats();
            let per = counter::bytes_per_customer(CustomerKind::V2, Some(&hot_comps));
            let mut each: Vec<f64> = hot.iter().map(|c| per.get(&cb::comp(*c)).copied().unwrap_or(0.0)).collect();
            let mean = each.iter().sum::<f64>() / each.len() as f64;
            let p50 = cb::pct(&mut each, 0.5);
            let p95 = cb::pct(&mut each, 0.95);
            // what the allocator really gets back when the counter goes
            let real = counter_heap_bytes();
            let line = serde_json::json!({
                "pattern": pattern, "mode": format!("{mode:?}"), "hot": hot.len(), "counted_bytes": st.bytes, "counted_per_hot": st.bytes as f64 / hot.len() as f64,
                "charged_mean": mean, "charged_p50": p50, "charged_p95": p95, "slices": st.slices, "blocks": st.blocks,
                "allocator_freed_bytes": real, "allocator_per_hot": real as f64 / hot.len() as f64, "allocator_over_counted": real as f64 / st.bytes.max(1) as f64,
            });
            eprintln!("{line}");
            out.push(line);
        }
    }
    view::CACHE_BYTES.store(cap0, Relaxed);
    cb::write_out(serde_json::json!({"test": "review_ram_alloc_and_patterns", "raw_mb": d.raw >> 20, "users": users, "block_kb": block_kb, "results": out}));
    t.abandon().await;
}

// ---- the agent pattern: LangGraph's MongoDB saver, one customer per thread

fn tcomp(t: usize) -> Vec<u8> {
    let name = format!("thread{t:06}");
    let mut k = vec![name.len() as u8];
    k.extend(name.as_bytes());
    k
}

fn tkey(t: usize, area: &str, tail: &[u8]) -> Vec<u8> {
    let mut k = b"u/".to_vec();
    k.extend(tcomp(t));
    k.extend(area.as_bytes());
    k.extend(tail);
    k
}

/// Text-like bytes of a given size (compresses like the chat bodies).
fn filler(seed: u64, n: usize) -> Vec<u8> {
    const WORDS: [&str; 16] = ["hello", "agent", "memory", "step", "thread", "the", "user", "said", "tool", "call", "result", "and", "then", "plan", "reply", "ok"];
    let mut r = StdRng::seed_from_u64(seed);
    let mut v = Vec::with_capacity(n + 8);
    while v.len() < n {
        v.extend(WORDS[r.gen_range(0..16)].as_bytes());
        v.push(b' ');
    }
    v.truncate(n);
    v
}

struct Thread {
    len: usize,
    cks: Vec<usize>,
    writes: Vec<Vec<usize>>,
}

/// The agent's resume: the latest checkpoint (index `checkpoints/x/ck/` reversed, limit 1, then the document) and its
/// pending writes (a range read of `checkpoint_writes/d/{checkpoint}`). `old`: also one past step, a quarter into the
/// thread (time travel). Checked against the sizes written.
async fn resume(t: &Tenant, th: &Thread, id: usize, old: bool) {
    let lo = tkey(id, "checkpoints/x/ck/", b"");
    let hi = crate::runfmt::prefix_successor(&lo).unwrap();
    let ix = t.scan_range(&lo, Some(&hi), true, Some(1), 64 << 10).await.unwrap();
    assert_eq!(ix.len(), 1);
    let last = th.cks.len() - 1;
    let mut steps = vec![last];
    if old {
        steps.push(last / 4);
    }
    for s in steps {
        let doc = t.get(tkey(id, "checkpoints/d/", &(s as u32).to_be_bytes())).await.unwrap().unwrap();
        assert_eq!(doc.len(), th.cks[s], "checkpoint {s} of thread {id}");
        let wlo = tkey(id, "checkpoint_writes/d/", &(s as u32).to_be_bytes());
        let whi = crate::runfmt::prefix_successor(&wlo).unwrap();
        let w = t.scan_range(&wlo, Some(&whi), false, None, 64 << 10).await.unwrap();
        assert_eq!(w.len(), th.writes[s].len(), "writes of checkpoint {s} of thread {id}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn review_agent_ram() {
    let _s = SERIAL.lock().await;
    enable();
    let path = std::env::var("RV_THREADS").expect("RV_THREADS: the TSV of thread sizes");
    // how many threads of each length to load (the mix weights them afterwards)
    let take: BTreeMap<usize, usize> = [(10, cb::env("RV_T10", 200) as usize), (40, cb::env("RV_T40", 80) as usize), (100, cb::env("RV_T100", 20) as usize), (300, cb::env("RV_T300", 4) as usize)].into();
    let block_kb = cb::env("CB_BLOCK_KB", 256);
    let mut threads: Vec<Thread> = Vec::new();
    let mut seen: BTreeMap<usize, usize> = BTreeMap::new();
    for line in std::fs::read_to_string(&path).unwrap().lines() {
        let f: Vec<&str> = line.split('\t').collect();
        let len: usize = f[0].parse().unwrap();
        let n = seen.entry(len).or_default();
        if *n >= take[&len] {
            continue;
        }
        *n += 1;
        let cks: Vec<usize> = f[2].split(',').map(|x| x.parse().unwrap()).collect();
        let writes: Vec<Vec<usize>> = f[3].split(';').map(|g| g.split(',').filter(|x| !x.is_empty()).map(|x| x.parse().unwrap()).collect()).collect();
        assert_eq!(cks.len(), writes.len());
        threads.push(Thread { len, cks, writes });
    }
    // threads interleaved over the key space (the ids are shuffled), then loaded step by step as the agents ran
    let mut r = StdRng::seed_from_u64(11);
    let mut ids: Vec<usize> = (0..threads.len()).map(|i| i * 7 + 3).collect();
    for i in (1..ids.len()).rev() {
        ids.swap(i, r.gen_range(0..=i));
    }
    let cap0 = view::CACHE_BYTES.swap(16 << 30, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut t = lc(&store).create("review-agent").await.unwrap();
    let max_steps = threads.iter().map(|x| x.cks.len()).max().unwrap();
    let mut raw = 0usize;
    for s in 0..max_steps {
        let mut batch = Vec::new();
        for (i, th) in threads.iter().enumerate() {
            if s >= th.cks.len() {
                continue;
            }
            let id = ids[i];
            let sk = (s as u32).to_be_bytes();
            batch.push(Op::Put(tkey(id, "checkpoints/d/", &sk), filler((id * 100_000 + s) as u64, th.cks[s])));
            batch.push(Op::Put(tkey(id, "checkpoints/x/ck/", &[&sk[..], &sk[..]].concat()), sk.to_vec()));
            raw += th.cks[s];
            for (w, &sz) in th.writes[s].iter().enumerate() {
                batch.push(Op::Put(tkey(id, "checkpoint_writes/d/", &[&sk[..], &(w as u32).to_be_bytes()[..]].concat()), filler((id * 100_000 + s) as u64 ^ 0xabc ^ w as u64, sz)));
                raw += sz;
            }
            if batch.len() > 400 {
                t.apply(std::mem::take(&mut batch)).await.unwrap();
            }
        }
        if !batch.is_empty() {
            t.apply(batch).await.unwrap();
        }
        if s % 10 == 9 {
            crate::lifecycle::compact_tenant(&store, &store, "review-agent").await.unwrap();
        }
    }
    eprintln!("agent data: {} threads, {} raw MB", threads.len(), raw >> 20);
    let o = super::build::BuildOpts { reseal: Reseal::All, sealed: RunOptions::unchecked((block_kb as usize) << 10, 16 << 20, CustomerKind::V2), delta: RunOptions::unchecked(64 << 10, 16 << 20, CustomerKind::V2), ..Default::default() };
    publish(&mut t, &o).await;
    let mut out = Vec::new();
    for pattern in ["resume", "resume+old-step"] {
        for mode in [SliceMode::Off, SliceMode::Area] {
            let _m = Mode::set(mode);
            view::clear_cache();
            resume(&t, &threads[0], ids[0], false).await;
            view::clear_cache();
            let g0 = view::RUN_GETS.load(Relaxed);
            for _visit in 0..2 {
                for (i, th) in threads.iter().enumerate() {
                    resume(&t, th, ids[i], pattern != "resume").await;
                }
                counter::trim_all();
            }
            let gets = view::RUN_GETS.load(Relaxed) - g0;
            let per = counter::bytes_per_customer(CustomerKind::V2, None);
            let mut by_len: BTreeMap<usize, Vec<f64>> = BTreeMap::new();
            for (i, th) in threads.iter().enumerate() {
                by_len.entry(th.len).or_default().push(per.get(&tcomp(ids[i])).copied().unwrap_or(0.0));
            }
            let st = counter::stats();
            let real = counter_heap_bytes();
            let mix = [(10usize, 0.40), (40, 0.35), (100, 0.20), (300, 0.05)];
            let mut lens = serde_json::Map::new();
            let mut mix_mean = 0.0;
            for (l, v) in by_len.iter_mut() {
                let mean = v.iter().sum::<f64>() / v.len() as f64;
                let p50 = cb::pct(v, 0.5);
                mix_mean += mean * mix.iter().find(|x| x.0 == *l).unwrap().1;
                lens.insert(l.to_string(), serde_json::json!({"threads": v.len(), "mean": mean, "p50": p50}));
            }
            let line = serde_json::json!({
                "pattern": pattern, "mode": format!("{mode:?}"), "by_length": lens, "mix_mean_per_hot_thread": mix_mean,
                "counted_bytes": st.bytes, "allocator_freed_bytes": real, "allocator_over_counted": real as f64 / st.bytes.max(1) as f64,
                "gets_two_visits": gets, "slices": st.slices, "blocks": st.blocks,
            });
            eprintln!("{line}");
            out.push(line);
        }
    }
    view::CACHE_BYTES.store(cap0, Relaxed);
    cb::write_out(serde_json::json!({"test": "review_agent_ram", "threads": threads.len(), "raw_mb": raw >> 20, "block_kb": block_kb, "results": out}));
    t.abandon().await;
}

// ---- the new-user filter against 8 runs

fn comp(c: usize) -> Vec<u8> {
    let name = format!("c{c:06}");
    let mut k = vec![name.len() as u8];
    k.extend(name.as_bytes());
    k
}

fn key(c: usize, a: &str) -> Vec<u8> {
    let mut k = b"u/".to_vec();
    k.extend(comp(c));
    k.extend(a.as_bytes());
    k
}

fn small(reseal: Reseal) -> super::build::BuildOpts {
    super::build::BuildOpts { reseal, group_bytes: 1 << 30, sealed: RunOptions::unchecked(4096, 1 << 20, CustomerKind::V2), delta: RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2) }
}

/// Absent customers against a sealed run and 7 delta runs, each spanning the whole key range (the 1 TB case of a cold
/// read meeting about 8 runs). Returns the share of absent customers whose point read paid a GET on our runs.
async fn fp_8_runs(bits: u64) -> f64 {
    crate::runfmt::FILTER_BITS_PER_CUSTOMER.store(bits, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut t = lc(&store).create("review-filter").await.unwrap();
    let mut m = BTreeMap::new();
    for c in (0..40_000).step_by(2) {
        apply(&mut t, &mut m, vec![(key(c, "m/d/0"), Some(vec![1u8; 40]))]).await;
    }
    publish(&mut t, &small(Reseal::All)).await;
    for d in 0..7 {
        let mut ops = Vec::new();
        for c in (2 * d..40_000).step_by(16) {
            ops.push((key(c, "m/d/1"), Some(vec![2u8; 40])));
        }
        apply(&mut t, &mut m, ops).await;
        publish(&mut t, &small(Reseal::None)).await;
    }
    let runs = t.fx_view().map(|v| v.delta.len() + v.groups.iter().filter(|g| g.sealed.is_some()).count()).unwrap();
    // every footer open first
    for c in [0usize, 39_998] {
        t.get(&key(c, "m/d/0")).await.unwrap();
    }
    view::clear_cache();
    let (mut n, mut paid) = (0, 0);
    for c in (1..40_000).step_by(2) {
        let g0 = view::RUN_GETS.load(Relaxed);
        assert!(t.get(&key(c, "m/d/0")).await.unwrap().is_none());
        n += 1;
        paid += (view::RUN_GETS.load(Relaxed) > g0) as usize;
    }
    let fp = paid as f64 / n as f64;
    eprintln!("{bits} bits per customer, {runs} runs: {paid} of {n} absent customers paid a GET ({:.3}%)", fp * 100.0);
    t.abandon().await;
    fp
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn review_filter_false_positives_8_runs() {
    let _s = SERIAL.lock().await;
    enable();
    let _m = Mode::set(SliceMode::Area);
    let before = crate::runfmt::FILTER_BITS_PER_CUSTOMER.load(Relaxed);
    let at10 = fp_8_runs(10).await;
    let at16 = fp_8_runs(16).await;
    crate::runfmt::FILTER_BITS_PER_CUSTOMER.store(before, Relaxed);
    // Bloom with k = 7: about 0.82% per run at 10 bits, 0.070% at 16 (ESTIMATED); across 8 runs about 6.4% and 0.56%
    assert!(at16 <= 0.01, "16 bits, 8 runs: {:.3}%", at16 * 100.0);
    assert!(at10 > 0.01, "10 bits, 8 runs: {:.3}% (expected over 1%)", at10 * 100.0);
}

// ---- area slices of a customer with a collection whose name holds a '/'

/// Layout v2 puts `u/{customer}{collection}/d/...` and `u/{customer}{collection}/x/{index}/...` under the customer, and
/// `counter.rs` finds the area at the first '/' after the customer. A collection named `zz/q` has no area that way, so
/// its rows fall back to the customer prefix. When an area read of `msgs` keeps the customer's other areas of the block
/// (`keep`, the sibling loop), it caches a slice keyed by the whole customer that starts at the first `zz/q` row: the
/// customer's `msgs` rows before it are missing. A read of the whole customer within the trim delay then misses them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn review_sibling_slice_with_unparsed_area() {
    let _s = SERIAL.lock().await;
    enable();
    let _m = Mode::set(SliceMode::Area);
    let trim0 = counter::TRIM_MS.swap(u64::MAX, Relaxed);
    view::clear_cache();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut t = lc(&store).create("review-sibling").await.unwrap();
    let mut m = BTreeMap::new();
    for c in 0..20 {
        let mut ops = Vec::new();
        for i in 0..5 {
            ops.push((key(c, &format!("msgs/d/{i:04}")), Some(vec![b'a'; 50])));
            ops.push((key(c, &format!("msgs/x/ts/{i:04}")), Some(key(c, &format!("msgs/d/{i:04}")))));
            ops.push((key(c, &format!("zz/q/d/{i:04}")), Some(vec![b'z'; 50])));
        }
        apply(&mut t, &mut m, ops).await;
    }
    // 32 KiB blocks: the whole customer (about 1 KB) sits in one block
    let mut o = small(Reseal::All);
    o.sealed = RunOptions::unchecked(32 << 10, 1 << 20, CustomerKind::V2);
    publish(&mut t, &o).await;
    let c = 7;
    // the visit: the index area of msgs (an area slice; the customer's other rows of the block are kept as siblings)
    let lo = key(c, "msgs/x/ts/");
    let hi = crate::runfmt::prefix_successor(&lo).unwrap();
    let got = as_vec(t.scan_range(&lo, Some(&hi), true, Some(3), 64 << 10).await.unwrap());
    assert_eq!(got, model_range(&m, &lo, Some(&hi), true, Some(3)));
    // then the whole customer
    let lo = key(c, "");
    let hi = crate::runfmt::prefix_successor(&lo).unwrap();
    let got = as_vec(t.scan_range(&lo, Some(&hi), false, None, 64 << 10).await.unwrap());
    let want = model_range(&m, &lo, Some(&hi), false, None);
    counter::TRIM_MS.store(trim0, Relaxed);
    t.abandon().await;
    assert_eq!(got.len(), want.len(), "whole-customer read after an area read: {} rows, the model has {}", got.len(), want.len());
    assert_eq!(got, want);
}

// ---- the staged publish with other read patterns

/// The staged publish warms each held slice's interval open upward to the end of its area. A customer that read its
/// oldest rows holds a slice whose interval starts at its oldest row, so the reseal warms its whole history. Measures
/// the counter right after a staged reseal and after the next visit, for the newest-20 visit and for newest + oldest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn review_reseal_patterns() {
    let _s = SERIAL.lock().await;
    enable();
    let (mb, users, block_kb, hot_n, seed) = (cb::env("CB_MB", 1024), cb::env("CB_USERS", 2000) as usize, cb::env("CB_BLOCK_KB", 256), cb::env("CB_HOT", 400) as usize, cb::env("CB_SEED", 7));
    let cap0 = view::CACHE_BYTES.swap(16 << 30, Relaxed);
    let _m = Mode::set(SliceMode::Area);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut t = lc(&store).create("review-reseal").await.unwrap();
    let mut d = cb::load(&mut t, &store, "review-reseal", mb, users, seed).await;
    publish(&mut t, &cb::opts(Reseal::All, block_kb)).await;
    let mut r = StdRng::seed_from_u64(seed + 1);
    let hot = cb::hot_set(&d, hot_n, &mut r);
    let mut out = Vec::new();
    for pattern in ["newest20", "newest20+oldest20"] {
        view::clear_cache();
        for _ in 0..2 {
            for &c in &hot {
                cb::newest(&t, &d, c).await;
                if pattern != "newest20" {
                    page(&t, &d, c, 20, true, 0).await;
                }
            }
            counter::trim_all();
        }
        let before = counter::stats().bytes;
        // new messages for some customers, then a staged reseal of every group
        let mut batch = Vec::new();
        for c in (0..users).step_by(3) {
            let n = d.next;
            d.next += 1;
            batch.push(Op::Put(cb::doc_key(c, n), cb::body(c, n)));
            batch.push(Op::Put(cb::idx_key(c, n), n.to_be_bytes().to_vec()));
            d.msgs[c].push(n);
        }
        t.apply(batch).await.unwrap();
        let g0 = view::RUN_GETS.load(Relaxed);
        let rep = publish(&mut t, &cb::opts(Reseal::All, block_kb)).await;
        let st = rep.stage.clone().unwrap();
        let stage_gets = view::RUN_GETS.load(Relaxed) - g0;
        let after_stage = counter::stats().bytes;
        let g1 = view::RUN_GETS.load(Relaxed);
        for &c in &hot {
            cb::newest(&t, &d, c).await;
            if pattern != "newest20" {
                page(&t, &d, c, 20, true, 0).await;
            }
        }
        let visit_gets = view::RUN_GETS.load(Relaxed) - g1;
        counter::trim_all();
        let after_visit = counter::stats().bytes;
        let line = serde_json::json!({
            "pattern": pattern, "hot": hot.len(), "per_hot_before": before as f64 / hot.len() as f64, "per_hot_after_stage": after_stage as f64 / hot.len() as f64,
            "per_hot_after_next_visit_trimmed": after_visit as f64 / hot.len() as f64, "stage_gets": stage_gets, "stage": st, "next_visit_gets": visit_gets,
        });
        eprintln!("{line}");
        out.push(line);
    }
    view::CACHE_BYTES.store(cap0, Relaxed);
    cb::write_out(serde_json::json!({"test": "review_reseal_patterns", "users": users, "block_kb": block_kb, "results": out}));
    t.abandon().await;
}

/// The 5 newest and the 5 oldest index entries of customer c, then their documents, checked against the model.
async fn newest_and_oldest(t: &Tenant, m: &BTreeMap<Vec<u8>, Vec<u8>>, c: usize) {
    let lo = key(c, "msgs/x/ts/");
    let hi = crate::runfmt::prefix_successor(&lo).unwrap();
    for rev in [true, false] {
        let ix = t.scan_range(&lo, Some(&hi), rev, Some(5), 64 << 10).await.unwrap();
        assert_eq!(as_vec(ix.clone()), model_range(m, &lo, Some(&hi), rev, Some(5)));
        for (_, d) in &ix {
            assert_eq!(t.get(&d[..]).await.unwrap().as_deref(), m.get(&d[..]).map(|v| &v[..]));
        }
    }
}

/// The staged publish warms each held slice from its lowest key to the end of its area. A customer whose visit read its
/// newest and its oldest messages holds a slice starting at its oldest index entry, so a reseal fetches and keeps its
/// whole history, every block between, though no read relied on those rows. Here: 40 customers of 300 messages
/// spread over many 4 KiB blocks, a visit of the 5 newest and the 5 oldest; after a staged reseal the counter should
/// hold about what it held before (plus the new rows), not the customers' whole histories.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn review_staged_publish_warms_only_what_was_held() {
    let _s = SERIAL.lock().await;
    enable();
    let _m = Mode::set(SliceMode::Area);
    let staged0 = counter::STAGED.swap(true, Relaxed);
    view::clear_cache();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut t = lc(&store).create("review-staged-hull").await.unwrap();
    let mut m = BTreeMap::new();
    let doc = |c: usize, i: usize| key(c, &format!("msgs/d/{i:06}"));
    let idx = |c: usize, i: usize| key(c, &format!("msgs/x/ts/{i:06}"));
    for c in 0..40 {
        let mut ops = Vec::new();
        for i in 0..300 {
            ops.push((doc(c, i), Some(vec![(c % 251) as u8; 200])));
            ops.push((idx(c, i), Some(doc(c, i))));
        }
        apply(&mut t, &mut m, ops).await;
    }
    let mut o = small(Reseal::All);
    o.sealed = RunOptions::unchecked(4096, 1 << 20, CustomerKind::V2);
    publish(&mut t, &o).await;
    for _ in 0..2 {
        for c in 0..40 {
            newest_and_oldest(&t, &m, c).await;
        }
        counter::trim_all();
    }
    let before = counter::stats().bytes;
    let mut ops = Vec::new();
    for c in 0..40 {
        ops.push((doc(c, 300), Some(vec![9u8; 200])));
        ops.push((idx(c, 300), Some(doc(c, 300))));
    }
    apply(&mut t, &mut m, ops).await;
    let rep = publish(&mut t, &o).await;
    let after = counter::stats().bytes;
    eprintln!("counter before the reseal {before} bytes, right after the staged reseal {after} bytes, stage {:?}", rep.stage);
    counter::STAGED.store(staged0, Relaxed);
    t.abandon().await;
    // the new rows are 40 x (200 + 30) bytes; allow three times what was held, plus them
    assert!(after <= 3 * before + 40 * 230 * 2, "a staged reseal grew the counter from {before} to {after} bytes");
}
