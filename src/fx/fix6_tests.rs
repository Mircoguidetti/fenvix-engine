//! The step 6 fix round: each fix of the review (`sessions/2026-11-09-review-c2-step6/REPORT.md`) has a control that
//! fails without it. A control runs the reviewer's test, unchanged, in a child process of this test binary with the
//! test-only switch that restores the behaviour as delivered (`counter::control`); the test must fail there.

fn reviewer_test_with(test: &str, control: &str) -> (bool, String) {
    let exe = std::env::current_exe().unwrap();
    let out = std::process::Command::new(exe).args([test, "--exact", "--test-threads", "1", "--nocapture"]).env(control, "1").output().unwrap();
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(text.contains("running 1 test"), "{test} did not run: {text}");
    (out.status.success(), text)
}

/// FIX 1: with sibling slices of unreadable areas kept as delivered, the reviewer's whole-customer read misses rows.
#[test]
fn control_sibling_slice_with_unparsed_area_fails_without_the_fix() {
    let (ok, text) = reviewer_test_with("fx::review6_tests::review_sibling_slice_with_unparsed_area", "FX_CONTROL_SIBLING");
    assert!(!ok, "the control must fail: {text}");
    assert!(text.contains("the model has 15"), "{text}");
}

/// FIX 2: with one hull per prefix, open upward and uncapped, as delivered, the reviewer's staged reseal multiplies the
/// counter.
#[test]
fn control_staged_publish_hull_fails_without_the_fix() {
    let (ok, text) = reviewer_test_with("fx::review6_tests::review_staged_publish_warms_only_what_was_held", "FX_CONTROL_STAGE_HULL");
    assert!(!ok, "the control must fail: {text}");
    assert!(text.contains("a staged reseal grew the counter"), "{text}");
}

// ---- RAM during a reseal for an agent resuming a LangGraph thread (measurement, ignored)
//
// The data and the resume are the reviewer's (`review6_tests.rs`: the LangGraph MongoDB saver layout, one customer
// per thread, checkpoint sizes of the checkpoint-delta harness); the helpers are copied here so that file stays as the
// reviewer wrote it. The measure: two visits (a resume each), trimmed; then every thread takes one more step (a new
// checkpoint and its writes, as an agent that goes on), a staged reseal of every group, and the next resume.

use super::build::Reseal;
use super::counter::{self, SliceMode};
use super::counter_bench as cb;
use super::tests::{enable, lc, publish, SERIAL};
use super::view;
use crate::lifecycle::{Op, Tenant};
use crate::runfmt::{CustomerKind, RunOptions};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

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

fn step_ops(th: &Thread, id: usize, s: usize) -> Vec<Op> {
    let sk = (s as u32).to_be_bytes();
    let mut v = vec![Op::Put(tkey(id, "checkpoints/d/", &sk), filler((id * 100_000 + s) as u64, th.cks[s])), Op::Put(tkey(id, "checkpoints/x/ck/", &[&sk[..], &sk[..]].concat()), sk.to_vec())];
    for (w, &sz) in th.writes[s].iter().enumerate() {
        v.push(Op::Put(tkey(id, "checkpoint_writes/d/", &[&sk[..], &(w as u32).to_be_bytes()[..]].concat()), filler((id * 100_000 + s) as u64 ^ 0xabc ^ w as u64, sz)));
    }
    v
}

async fn resume(t: &Tenant, th: &Thread, id: usize) {
    let lo = tkey(id, "checkpoints/x/ck/", b"");
    let hi = crate::runfmt::prefix_successor(&lo).unwrap();
    let ix = t.scan_range(&lo, Some(&hi), true, Some(1), 64 << 10).await.unwrap();
    assert_eq!(ix.len(), 1);
    let s = th.cks.len() - 1;
    assert_eq!(&ix[0].1[..], &(s as u32).to_be_bytes()[..], "latest checkpoint of thread {id}");
    let doc = t.get(tkey(id, "checkpoints/d/", &(s as u32).to_be_bytes())).await.unwrap().unwrap();
    assert_eq!(doc.len(), th.cks[s], "checkpoint {s} of thread {id}");
    let wlo = tkey(id, "checkpoint_writes/d/", &(s as u32).to_be_bytes());
    let whi = crate::runfmt::prefix_successor(&wlo).unwrap();
    let w = t.scan_range(&wlo, Some(&whi), false, None, 64 << 10).await.unwrap();
    assert_eq!(w.len(), th.writes[s].len(), "writes of checkpoint {s} of thread {id}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn fix6_agent_reseal_ram() {
    let _s = SERIAL.lock().await;
    enable();
    let path = std::env::var("RV_THREADS").expect("RV_THREADS: the TSV of thread sizes");
    let take: BTreeMap<usize, usize> = [(10, 200), (40, 80), (100, 20), (300, 4)].into();
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
        threads.push(Thread { len, cks, writes });
    }
    let mut r = StdRng::seed_from_u64(11);
    let mut ids: Vec<usize> = (0..threads.len()).map(|i| i * 7 + 3).collect();
    for i in (1..ids.len()).rev() {
        ids.swap(i, r.gen_range(0..=i));
    }
    // the last step of every thread is held back: it is the step the agent takes after the visits
    let full: Vec<usize> = threads.iter().map(|t| t.cks.len()).collect();
    let cap0 = view::CACHE_BYTES.swap(16 << 30, Relaxed);
    let mode0 = counter::SLICES.swap(SliceMode::Area as u8, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut t = lc(&store).create("fix6-agent").await.unwrap();
    let max_steps = full.iter().max().copied().unwrap();
    for s in 0..max_steps - 1 {
        let mut batch = Vec::new();
        for (i, th) in threads.iter().enumerate() {
            if s + 1 < full[i] {
                batch.extend(step_ops(th, ids[i], s));
            }
            if batch.len() > 400 {
                t.apply(std::mem::take(&mut batch)).await.unwrap();
            }
        }
        if !batch.is_empty() {
            t.apply(batch).await.unwrap();
        }
        if s % 10 == 9 {
            crate::lifecycle::compact_tenant(&store, &store, "fix6-agent").await.unwrap();
        }
    }
    let o = super::build::BuildOpts { reseal: Reseal::All, sealed: RunOptions::unchecked((block_kb as usize) << 10, 16 << 20, CustomerKind::V2), delta: RunOptions::unchecked(64 << 10, 16 << 20, CustomerKind::V2), ..Default::default() };
    publish(&mut t, &o).await;
    // the threads as they stand (all but their last step)
    let mut cur: Vec<Thread> = threads.iter().zip(&full).map(|(th, &n)| Thread { len: th.len, cks: th.cks[..n - 1].to_vec(), writes: th.writes[..n - 1].to_vec() }).collect();
    view::clear_cache();
    for _ in 0..2 {
        for (i, th) in cur.iter().enumerate() {
            resume(&t, th, ids[i]).await;
        }
        counter::trim_all();
    }
    let before = counter::stats().bytes;
    // every agent takes its next step, then a staged reseal of every group
    let mut batch = Vec::new();
    for (i, th) in threads.iter().enumerate() {
        batch.extend(step_ops(th, ids[i], full[i] - 1));
    }
    t.apply(batch).await.unwrap();
    for (i, th) in cur.iter_mut().enumerate() {
        th.cks.push(threads[i].cks[full[i] - 1]);
        th.writes.push(threads[i].writes[full[i] - 1].clone());
    }
    let rep = publish(&mut t, &o).await;
    let after_stage = counter::stats().bytes;
    let g0 = view::RUN_GETS.load(Relaxed);
    for (i, th) in cur.iter().enumerate() {
        resume(&t, th, ids[i]).await;
    }
    let visit_gets = view::RUN_GETS.load(Relaxed) - g0;
    counter::trim_all();
    let after_visit = counter::stats().bytes;
    let n = threads.len() as f64;
    let out = serde_json::json!({
        "test": "fix6_agent_reseal_ram", "threads": threads.len(), "block_kb": block_kb,
        "per_thread_before": before as f64 / n, "per_thread_after_stage": after_stage as f64 / n, "per_thread_after_next_visit_trimmed": after_visit as f64 / n,
        "growth_at_stage": after_stage as f64 / before.max(1) as f64, "next_visit_gets": visit_gets, "threads_needing_a_get_max": visit_gets, "stage": rep.stage,
    });
    cb::write_out(out);
    counter::SLICES.store(mode0, Relaxed);
    view::CACHE_BYTES.store(cap0, Relaxed);
    t.abandon().await;
}
