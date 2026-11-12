//! Tests of the sealed-run format v1. The long runs are the same tests with larger settings:
//!   RUNFMT_READS=1000000     reads of the property test (default 30,000)
//!   RUNFMT_LARGE_MB=256 RUNFMT_LARGE_FLIPS=2000 RUNFMT_LARGE_SCANS=50   the random flips on a large run (default 8 MB, 300, 5)
//! Each prints one `RUNFMT ...` line with its counts (`cargo test --release --lib runfmt -- --nocapture`).

use super::*;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::{CopyOptions, ObjectStoreExt, GetResult, GetResultPayload, ListResult, ObjectMeta, PutMultipartOptions, PutOptions, PutResult, RenameOptions};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

fn env_or(name: &str, d: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

// ---- keys of layout v2 for an integer customer

fn sortable_i64(v: i64) -> [u8; 8] {
    ((v as u64) ^ (1 << 63)).to_be_bytes()
}

fn sortable_f64(v: f64) -> [u8; 8] {
    let bits = v.to_bits();
    let t = if bits >> 63 == 1 { !bits } else { bits | (1 << 63) };
    t.to_be_bytes()
}

/// `u/` + the customer component (length 17, number rank 3, f64, i64), as custkey::comp builds it for an integer.
fn cust(u: i64) -> Vec<u8> {
    let mut k = b"u/".to_vec();
    k.push(17);
    k.push(3);
    k.extend(sortable_f64(u as f64));
    k.extend(sortable_i64(u));
    k
}

fn doc_key(u: i64, coll: &str, n: u64) -> Vec<u8> {
    let mut k = cust(u);
    k.push(b'/');
    k.extend(coll.as_bytes());
    k.extend(b"/d/");
    k.extend(n.to_be_bytes());
    k
}

type Model = BTreeMap<Vec<u8>, (u64, Vec<u8>)>;

fn model_rows<'a>(it: impl Iterator<Item = (&'a Vec<u8>, &'a (u64, Vec<u8>))>) -> Vec<Row> {
    it.map(|(k, (s, e))| Row { key: k.clone(), seq: *s, env: e.clone() }).collect()
}

fn model_range(m: &Model, lo: &[u8], hi: Option<&[u8]>) -> Vec<Row> {
    use std::ops::Bound::*;
    if hi.is_some_and(|h| h <= lo) {
        return vec![];
    }
    let r = (Included(lo.to_vec()), hi.map_or(Unbounded, |h| Excluded(h.to_vec())));
    model_rows(m.range(r))
}

fn model_newest(m: &Model, p: &[u8], limit: usize) -> Vec<Row> {
    use std::ops::Bound::*;
    let r = (Included(p.to_vec()), prefix_successor(p).map_or(Unbounded, Excluded));
    model_rows(m.range(r).rev().take(limit))
}

fn random_env(rng: &mut StdRng) -> Vec<u8> {
    if rng.gen_bool(0.1) {
        return deleted();
    }
    let n = if rng.gen_bool(0.01) { rng.gen_range(3000..9000) } else { rng.gen_range(0..200) };
    let mut d = vec![0u8; n];
    for (i, b) in d.iter_mut().enumerate() {
        *b = if i % 3 == 0 { rng.gen() } else { b"abcdefgh"[i % 8] };
    }
    live(&d)
}

/// The customers of the property test: (customer, rows) by category: absent, 1, 19, 20 or 21, many, a few.
fn customer_plan(rng: &mut StdRng, n: i64) -> Vec<(i64, usize)> {
    (0..n)
        .map(|u| {
            let k = match u % 7 {
                0 => 0,
                1 => 1,
                2 => 19,
                3 => rng.gen_range(20..22),
                4 => rng.gen_range(300..2000),
                _ => rng.gen_range(2..60),
            };
            (u - n / 3, k)
        })
        .collect()
}

/// Builds the model: customer rows in two collections, plus shared-keyspace rows (no customer).
fn build_model(rng: &mut StdRng, plan: &[(i64, usize)], shared: usize) -> Model {
    let mut m = Model::new();
    let mut n = 0u64;
    for &(u, k) in plan {
        for _ in 0..k {
            let coll = if rng.gen_bool(0.7) { "a" } else { "b" };
            m.insert(doc_key(u, coll, n), (rng.gen(), random_env(rng)));
            n += 1;
        }
    }
    for i in 0..shared {
        let mut k = b"loc/m/".to_vec();
        k.extend((i as u64 * 7919).to_be_bytes());
        m.insert(k, (rng.gen(), random_env(rng)));
    }
    m
}

/// Writes the model as runs cut at the given row indexes (cuts may fall inside a customer).
async fn write_runs(store: &Arc<dyn ObjectStore>, m: &Model, cuts: &[usize], opts: &RunOptions, name: &str) -> Vec<RunMeta> {
    let mut metas = Vec::new();
    let mut bounds = vec![0];
    bounds.extend(cuts.iter().copied());
    bounds.push(m.len());
    let rows: Vec<_> = m.iter().collect();
    for (i, w) in bounds.windows(2).enumerate() {
        if w[0] == w[1] {
            continue;
        }
        let mut wr = RunWriter::new(store, Path::from(format!("{name}/{i:04}")), opts.clone()).await.unwrap();
        for (k, (s, e)) in &rows[w[0]..w[1]] {
            wr.add(k, *s, e).await.unwrap();
        }
        metas.push(wr.finish().await.unwrap());
    }
    metas
}

async fn open_all(store: &Arc<dyn ObjectStore>, metas: &[RunMeta], cache: usize, tail: usize) -> RunReader {
    let runs: Vec<(String, u64)> = metas.iter().map(|m| (m.path.clone(), m.size)).collect();
    RunReader::open(store.clone(), &runs, cache, tail).await.unwrap().0
}

/// A probe key around `k`: itself, shorter, longer, or one byte changed.
fn perturb(rng: &mut StdRng, k: &[u8]) -> Vec<u8> {
    let mut v = k.to_vec();
    match rng.gen_range(0..5) {
        0 => {}
        1 => v.truncate(rng.gen_range(0..=k.len())),
        2 => v.push(rng.gen()),
        3 => {
            if !v.is_empty() {
                let i = rng.gen_range(0..v.len());
                v[i] = v[i].wrapping_add(if rng.gen() { 1 } else { 255 });
            }
        }
        _ => v = prefix_successor(k).unwrap_or_default(),
    }
    v
}

// ---- unit tests

#[test]
fn envelopes_and_successors() {
    assert!(check_envelope(&live(b"x")).is_ok() && check_envelope(&live(b"")).is_ok() && check_envelope(&deleted()).is_ok());
    assert!(check_envelope(&[]).is_err() && check_envelope(&[2]).is_err() && check_envelope(&[1, 0]).is_err());
    let r = Row { key: vec![1], seq: 3, env: live(b"ab") };
    assert_eq!(r.doc(), Some(&b"ab"[..]));
    assert!(Row { key: vec![1], seq: 3, env: deleted() }.is_deleted());
    assert_eq!(prefix_successor(b"ab"), Some(b"ac".to_vec()));
    assert_eq!(prefix_successor(&[1, 0xFF, 0xFF]), Some(vec![2]));
    assert_eq!(prefix_successor(&[0xFF]), None);
    assert_eq!(prefix_successor(b""), None);
}

#[test]
fn block_size_knob_is_checked() {
    assert!(RunOptions::new(64 << 10, MIN_PART, CustomerKind::V2).is_ok());
    assert!(RunOptions::new(1 << 20, MIN_PART, CustomerKind::V2).is_ok());
    assert!(RunOptions::new((64 << 10) - 1, MIN_PART, CustomerKind::V2).is_err());
    assert!(RunOptions::new((1 << 20) + 1, MIN_PART, CustomerKind::V2).is_err());
    assert!(RunOptions::new(DEFAULT_BLOCK, MIN_PART - 1, CustomerKind::V2).is_err());
    assert_eq!(RunOptions::sealed(CustomerKind::V2).block_bytes, 256 << 10);
}

#[test]
fn customer_components_are_found_in_both_layouts() {
    let k = doc_key(42, "a", 7);
    let r = CustomerKind::V2.customer_of(&k).unwrap();
    assert_eq!(&k[r], &cust(42)[2..]);
    assert_eq!(CustomerKind::V2.customer_of(b"loc/m/1"), None);
    assert_eq!(CustomerKind::V2.customer_of(b"u/"), None);
    assert_eq!(CustomerKind::V2.customer_of(&[b'u', b'/', 5, 1]), None);
    // layout v1: doc/{coll}/{component}{_id}
    let mut v1 = b"doc/msg%2Fx/".to_vec();
    v1.extend(&cust(42)[2..]);
    v1.extend([8, 1, 2, 3]);
    let r = CustomerKind::DocV1.customer_of(&v1).unwrap();
    assert_eq!(&v1[r], &cust(42)[2..]);
    assert_eq!(CustomerKind::DocV1.customer_of(b"idx/x"), None);
    assert_eq!(CustomerKind::None.customer_of(&v1), None);
}

#[test]
fn customer_filter_has_no_false_negatives_and_few_false_positives() {
    let comps: Vec<Vec<u8>> = (0..20_000).map(|u| cust(u)[2..].to_vec()).collect();
    let f = CustomerFilter::build(&comps.iter().map(|c| customer_hash(c)).collect::<Vec<_>>());
    assert!(comps.iter().all(|c| f.may_contain(c)));
    let fp = (1_000_000..1_100_000).filter(|&u| f.may_contain(&cust(u)[2..])).count();
    // 10 bits and 7 hashes: about 0.8% (ESTIMATED from the Bloom formula); MEASURED here
    println!("RUNFMT filter false positives: {fp} of 100000 absent customers ({:.3}%)", fp as f64 / 1000.0);
    assert!(fp < 1500, "false positives {fp}");
    assert!(!CustomerFilter::build(&[]).may_contain(b"x"));
}

#[tokio::test]
async fn writer_checks_order_and_envelopes() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut w = RunWriter::new(&store, Path::from("o/1"), RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2)).await.unwrap();
    w.add(b"b", 1, &live(b"x")).await.unwrap();
    assert!(w.add(b"b", 2, &live(b"x")).await.is_err());
    assert!(w.add(b"a", 2, &live(b"x")).await.is_err());
    assert!(w.add(b"c", 2, &[7]).await.is_err());
    assert!(w.add(b"c", 2, &[]).await.is_err());
    w.abort().await.unwrap();
    assert!(store.head(&Path::from("o/1")).await.is_err(), "an aborted run is never visible");
    let w = RunWriter::new(&store, Path::from("o/2"), RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2)).await.unwrap();
    assert!(w.finish().await.is_err(), "an empty run is refused");
}

#[tokio::test]
async fn multipart_parts_and_visibility() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut rng = StdRng::seed_from_u64(5);
    let opts = RunOptions::unchecked(4096, 64 << 10, CustomerKind::V2);
    let mut w = RunWriter::new(&store, Path::from("p/1"), opts).await.unwrap();
    for n in 0..3000u64 {
        let doc: Vec<u8> = (0..100).map(|_| rng.gen()).collect();
        w.add(&doc_key((n / 100) as i64, "a", n), n, &live(&doc)).await.unwrap();
        if n == 2000 {
            assert!(store.head(&Path::from("p/1")).await.is_err(), "not visible before the upload completes");
        }
    }
    let m = w.finish().await.unwrap();
    assert_eq!(m.parts, m.size.div_ceil(64 << 10));
    assert_eq!(m.puts, m.parts + 2);
    assert!(m.parts >= 3);
    assert_eq!(store.head(&Path::from("p/1")).await.unwrap().size, m.size);
    assert_eq!((m.entries, m.customers, m.min_seq, m.max_seq), (3000, 30, 0, 2999));
    // the stream reads it back exactly, in chunks
    let mut s = RunStream::open(store.clone(), Path::from("p/1"), m.size, 20_000).await.unwrap();
    let mut n = 0u64;
    while let Some(r) = s.next().await.unwrap() {
        assert_eq!((r.key, r.seq), (doc_key((n / 100) as i64, "a", n), n));
        n += 1;
    }
    assert_eq!(n, 3000);
    assert!(s.gets > 3);
}

#[tokio::test]
async fn default_and_64k_blocks_round_trip() {
    for block in [DEFAULT_BLOCK, MIN_BLOCK, MAX_BLOCK] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut rng = StdRng::seed_from_u64(block as u64);
        let plan = customer_plan(&mut rng, 300);
        let m = build_model(&mut rng, &plan, 200);
        let opts = RunOptions::new(block, MIN_PART, CustomerKind::V2).unwrap();
        let metas = write_runs(&store, &m, &[m.len() / 2], &opts, "d").await;
        let raw: u64 = metas.iter().map(|x| x.raw_bytes).sum();
        let blocks: u64 = metas.iter().map(|x| x.blocks).sum();
        assert!(raw / blocks <= block as u64 && raw / blocks > block as u64 / 2, "blocks of {} bytes", raw / blocks);
        let rd = open_all(&store, &metas, 0, 1).await;
        for (u, _) in plan.iter().step_by(5) {
            let (got, _) = rd.prefix(&cust(*u)).await.unwrap();
            assert_eq!(got, model_range(&m, &cust(*u), prefix_successor(&cust(*u)).as_deref()));
        }
        let (all, _) = rd.range(b"", None).await.unwrap();
        assert_eq!(all, model_range(&m, b"", None));
    }
}

/// The fence-layout test, kept: newest reads across block and run boundaries, customers with 0, 1, 19 and many rows,
/// a run that starts with a customer, and a whale that spans runs.
#[tokio::test]
async fn newest_reads_match_an_exact_model_across_block_and_run_boundaries() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut m = Model::new();
    let mut n = 0u64;
    for (u, k) in [(1i64, 3u64), (2, 250), (3, 1), (5, 40), (6, 25), (7, 500), (9, 19)] {
        for _ in 0..k {
            m.insert(doc_key(u, "a", n), (n, live(format!("{u}:{n}").as_bytes())));
            n += 1;
        }
    }
    // runs: [1..5] | [6 ...] (a run starting exactly with customer 6) | customer 7 split in the middle | [.. 9]
    let keys: Vec<_> = m.keys().cloned().collect();
    let first6 = keys.iter().position(|k| k.starts_with(&cust(6))).unwrap();
    let mid7 = keys.iter().position(|k| k.starts_with(&cust(7))).unwrap() + 250;
    let opts = RunOptions::unchecked(512, 1 << 20, CustomerKind::V2);
    let metas = write_runs(&store, &m, &[first6, mid7], &opts, "f").await;
    assert_eq!(metas.len(), 3);
    assert!(metas.iter().all(|x| x.blocks > 5));
    for tail in [1, 2, 3] {
        for cache in [0, 1 << 20] {
            let rd = open_all(&store, &metas, cache, tail).await;
            for u in [1i64, 2, 3, 5, 6, 7, 9, 4, 8, 100, 0, -1] {
                for limit in [1, 19, 20, 21, 260, 1000] {
                    let (got, _) = rd.newest(&cust(u), limit).await.unwrap();
                    assert_eq!(got, model_newest(&m, &cust(u), limit), "customer {u} limit {limit} tail {tail}");
                }
            }
        }
    }
}

#[tokio::test]
async fn a_customer_inside_one_block_costs_one_get_and_an_absent_one_none() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut m = Model::new();
    for u in [1i64, 2, 3] {
        for n in 0..30 {
            m.insert(doc_key(u * 2, "a", n), (n, live(b"0123456789")));
        }
    }
    let metas = write_runs(&store, &m, &[], &RunOptions::sealed(CustomerKind::V2), "o").await;
    assert_eq!(metas[0].blocks, 1);
    let rd = open_all(&store, &metas, 0, 1).await;
    let (got, st) = rd.newest(&cust(4), 20).await.unwrap();
    assert_eq!(got.len(), 20);
    assert_eq!((st.gets, st.trips), (1, 1));
    // customer 3 sits between 2 and 4: its filter says no, 0 GETs
    let (got, st) = rd.newest(&cust(3), 20).await.unwrap();
    assert!(got.is_empty());
    assert_eq!((st.gets, st.filter_skips), (0, 1));
    let (got, st) = rd.get(&doc_key(3, "a", 1)).await.unwrap();
    assert!(got.is_none() && st.gets == 0);
    let (got, st) = rd.prefix(&cust(5)).await.unwrap();
    assert!(got.is_empty() && st.gets == 0);
}

#[tokio::test]
async fn opening_costs_one_get_for_a_footer_under_the_open_tail() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut rng = StdRng::seed_from_u64(9);
    let plan = customer_plan(&mut rng, 100);
    let m = build_model(&mut rng, &plan, 0);
    let small = write_runs(&store, &m, &[], &RunOptions::unchecked(4096, 1 << 20, CustomerKind::V2), "s").await;
    let (_, gets) = Run::open(&store, Path::from(small[0].path.as_str()), small[0].size).await.unwrap();
    assert_eq!(gets, 1);
    // tiny blocks make a footer larger than OPEN_TAIL: two GETs
    let big = write_runs(&store, &m, &[], &RunOptions::unchecked(16, 1 << 20, CustomerKind::V2), "b").await;
    let (r, gets) = Run::open(&store, Path::from(big[0].path.as_str()), big[0].size).await.unwrap();
    assert!(big[0].size - big[0].data_end > OPEN_TAIL);
    assert_eq!(gets, 2);
    assert_eq!(r.entries, m.len() as u64);
}

#[tokio::test]
async fn runs_get_distinct_ids_and_open_expecting_checks_them() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut m = Model::new();
    for n in 0..50u64 {
        m.insert(doc_key(1, "a", n), (n, live(b"v")));
    }
    let o = RunOptions::unchecked(256, 1 << 20, CustomerKind::V2);
    let a = write_runs(&store, &m, &[], &o, "ida").await.remove(0);
    let b = write_runs(&store, &m, &[], &o, "idb").await.remove(0);
    assert_ne!(a.run_id, b.run_id);
    let (r, _) = Run::open(&store, Path::from(a.path.as_str()), a.size).await.unwrap();
    assert_eq!(r.run_id, a.run_id);
    assert!(Run::open_expecting(&store, Path::from(a.path.as_str()), a.size, &a.run_id).await.is_ok());
    assert!(Run::open_expecting(&store, Path::from(a.path.as_str()), a.size, &b.run_id).await.is_err());
}

#[tokio::test]
async fn overlapping_runs_are_refused() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut a = Model::new();
    a.insert(b"a".to_vec(), (1, live(b"")));
    a.insert(b"c".to_vec(), (1, live(b"")));
    let mut b = Model::new();
    b.insert(b"b".to_vec(), (1, live(b"")));
    let o = RunOptions::unchecked(64, 1 << 20, CustomerKind::None);
    let mut ms = write_runs(&store, &a, &[], &o, "x").await;
    ms.extend(write_runs(&store, &b, &[], &o, "y").await);
    let runs: Vec<(String, u64)> = ms.iter().map(|m| (m.path.clone(), m.size)).collect();
    assert!(RunReader::open(store.clone(), &runs, 0, 1).await.is_err());
}

// ---- the property test against an exact model

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn random_reads_match_an_exact_model() {
    let reads = env_or("RUNFMT_READS", 30_000);
    let mut rng = StdRng::seed_from_u64(env_or("RUNFMT_SEED", 20261107));
    let plan = customer_plan(&mut rng, 420);
    let m = build_model(&mut rng, &plan, 3000);
    let keys: Vec<Vec<u8>> = m.keys().cloned().collect();
    // three layouts of the same rows: tiny blocks in 6 runs, 4 KiB blocks in 3 runs, 64 KiB blocks (the smallest
    // checked knob) in one run; cuts at random rows, so they fall inside customers
    let mut layouts = Vec::new();
    for (block, nruns) in [(700usize, 6usize), (4096, 3), (MIN_BLOCK, 1)] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cuts: Vec<usize> = (1..nruns).map(|_| rng.gen_range(1..m.len())).collect();
        cuts.sort();
        cuts.dedup();
        let metas = write_runs(&store, &m, &cuts, &RunOptions::unchecked(block, 1 << 20, CustomerKind::V2), "pt").await;
        let blocks: u64 = metas.iter().map(|x| x.blocks).sum();
        for (cache, tail) in [(0usize, 1usize), (256 << 10, 2), (0, 4)] {
            layouts.push((open_all(&store, &metas, cache, tail).await, format!("block {block}, {} runs, {blocks} blocks, cache {cache}, tail {tail}", metas.len())));
        }
    }
    let (mut wrong, mut by_kind) = (0u64, [0u64; 6]);
    let (mut absent_reads, mut absent_with_get) = (0u64, 0u64);
    let mut rows_compared = 0u64;
    for i in 0..reads {
        let (rd, name) = &layouts[(i % layouts.len() as u64) as usize];
        let kind = rng.gen_range(0..6);
        by_kind[kind] += 1;
        let (got, want, st) = match kind {
            // a point read of a present key, or of a key near one
            0 | 1 => {
                let k = keys[rng.gen_range(0..keys.len())].clone();
                let k = if kind == 0 { k } else { perturb(&mut rng, &k) };
                let (g, st) = rd.get(&k).await.unwrap();
                (g.into_iter().collect::<Vec<_>>(), m.get(&k).map(|(s, e)| vec![Row { key: k.clone(), seq: *s, env: e.clone() }]).unwrap_or_default(), st)
            }
            // a range between random bounds near present keys (across blocks and runs), sometimes to the end
            2 => {
                let a = rng.gen_range(0..keys.len());
                let lo = perturb(&mut rng, &keys[a]);
                let b = (a + rng.gen_range(0..2500)).min(keys.len() - 1);
                let hi = if rng.gen_bool(0.03) { None } else { Some(perturb(&mut rng, &keys[b])) };
                let (g, st) = rd.range(&lo, hi.as_deref()).await.unwrap();
                (g, model_range(&m, &lo, hi.as_deref()), st)
            }
            // a customer's whole range, or one collection of it
            3 | 4 => {
                let (u, n) = plan[rng.gen_range(0..plan.len())];
                let mut p = cust(u);
                if kind == 4 {
                    p.extend(b"/a/");
                }
                let (g, st) = rd.prefix(&p).await.unwrap();
                if n == 0 {
                    absent_reads += 1;
                    absent_with_get += (st.gets + st.cache_hits > 0) as u64;
                }
                (g, model_range(&m, &p, prefix_successor(&p).as_deref()), st)
            }
            // a customer's newest rows
            _ => {
                let (u, _) = plan[rng.gen_range(0..plan.len())];
                let limit = [1usize, 5, 19, 20, 21, 100, 5000][rng.gen_range(0..7)];
                let p = if rng.gen_bool(0.8) { cust(u) } else { [cust(u), b"/b/".to_vec()].concat() };
                let (g, st) = rd.newest(&p, limit).await.unwrap();
                (g, model_newest(&m, &p, limit), st)
            }
        };
        rows_compared += want.len() as u64;
        if got != want {
            wrong += 1;
            if wrong <= 5 {
                eprintln!("WRONG read {i} kind {kind} ({name}): got {} rows, want {}", got.len(), want.len());
            }
        }
        let _ = st;
    }
    println!(
        "RUNFMT property: {reads} reads, {wrong} wrong, {rows_compared} rows compared; by kind (get, near-get, range, customer, customer+coll, newest) {by_kind:?}; rows {}, customers {} ({} absent); absent-customer reads touching a block {absent_with_get} of {absent_reads}",
        m.len(),
        plan.len(),
        plan.iter().filter(|p| p.1 == 0).count()
    );
    for (_, name) in &layouts {
        println!("RUNFMT layout: {name}");
    }
    assert_eq!(wrong, 0);
}

// ---- flipped bits

/// An object store that flips one bit of one object in every GET that covers it.
struct FlipStore {
    inner: Arc<dyn ObjectStore>,
    /// bit index + 1 (0 = off)
    flip: AtomicU64,
}

impl fmt::Debug for FlipStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FlipStore")
    }
}

impl fmt::Display for FlipStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FlipStore")
    }
}

#[async_trait]
impl ObjectStore for FlipStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let r = self.inner.get_opts(location, options).await?;
        let f = self.flip.load(Relaxed);
        if f == 0 {
            return Ok(r);
        }
        let bit = f - 1;
        let (meta, range, attributes, extensions) = (r.meta.clone(), r.range.clone(), r.attributes.clone(), r.extensions.clone());
        let mut body = r.bytes().await?.to_vec();
        let byte = bit / 8;
        if byte >= range.start && byte < range.end {
            body[(byte - range.start) as usize] ^= 1 << (bit % 8);
        }
        let stream = futures::stream::once(async move { Ok(Bytes::from(body)) }).boxed();
        Ok(GetResult { payload: GetResultPayload::Stream(stream), meta, range, attributes, extensions })
    }
    fn delete_stream(&self, locations: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// Reads the whole run with the streaming reader.
async fn stream_all(store: &Arc<dyn ObjectStore>, meta: &RunMeta, chunk: u64) -> anyhow::Result<Vec<Row>> {
    let mut s = RunStream::open(store.clone(), Path::from(meta.path.as_str()), meta.size, chunk).await?;
    let mut out = Vec::new();
    while let Some(r) = s.next().await? {
        out.push(r);
    }
    Ok(out)
}

/// Reads the whole run with the random reader.
async fn random_all(store: &Arc<dyn ObjectStore>, meta: &RunMeta) -> anyhow::Result<Vec<Row>> {
    let (rd, _) = RunReader::open(store.clone(), &[(meta.path.clone(), meta.size)], 0, 1).await?;
    Ok(rd.range(b"", None).await?.0)
}

#[tokio::test]
async fn every_flipped_bit_of_a_small_run_is_an_error() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fs = Arc::new(FlipStore { inner: inner.clone(), flip: AtomicU64::new(0) });
    let store: Arc<dyn ObjectStore> = fs.clone();
    let mut rng = StdRng::seed_from_u64(77);
    let mut m = Model::new();
    for u in 0..6i64 {
        for n in 0..rng.gen_range(1..8u64) {
            let env = if rng.gen_bool(0.2) { deleted() } else { live(&[b'x'; 20][..rng.gen_range(0..20)]) };
            m.insert(doc_key(u * 3, if n % 2 == 0 { "a" } else { "b" }, n), (rng.gen(), env));
        }
    }
    let meta = write_runs(&store, &m, &[], &RunOptions::unchecked(300, 1 << 20, CustomerKind::V2), "small").await.remove(0);
    assert!(meta.blocks >= 4, "{} blocks", meta.blocks);
    let want = model_range(&m, b"", None);
    assert_eq!(stream_all(&store, &meta, 1000).await.unwrap(), want);
    assert_eq!(random_all(&store, &meta).await.unwrap(), want);
    let bits = meta.size * 8;
    let (mut caught_stream, mut caught_random, mut caught_open) = (0u64, 0u64, 0u64);
    for bit in 0..bits {
        fs.flip.store(bit + 1, Relaxed);
        if Run::open(&store, Path::from(meta.path.as_str()), meta.size).await.is_err() {
            caught_open += 1;
        }
        match stream_all(&store, &meta, 1000).await {
            Err(_) => caught_stream += 1,
            Ok(rows) => panic!("bit {bit} (byte {} of {}): the stream returned {} rows, equal to the model: {}", bit / 8, meta.size, rows.len(), rows == want),
        }
        match random_all(&store, &meta).await {
            Err(_) => caught_random += 1,
            Ok(rows) => panic!("bit {bit} (byte {} of {}): the random reader returned {} rows, equal to the model: {}", bit / 8, meta.size, rows.len(), rows == want),
        }
    }
    fs.flip.store(0, Relaxed);
    let footer_bits = (meta.size - meta.data_end) * 8;
    println!(
        "RUNFMT flips exhaustive: run of {} bytes ({} blocks, {} rows), {bits} bits flipped one at a time: stream caught {caught_stream}, random reader caught {caught_random}; open alone caught {caught_open} (the {footer_bits} footer and trailer bits are caught at open)",
        meta.size,
        meta.blocks,
        meta.entries
    );
    assert_eq!((caught_stream, caught_random), (bits, bits));
    assert_eq!(caught_open, footer_bits);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn random_flipped_bits_of_a_large_run_are_errors() {
    let target = env_or("RUNFMT_LARGE_MB", 8) << 20;
    let flips = env_or("RUNFMT_LARGE_FLIPS", 300);
    let scans = env_or("RUNFMT_LARGE_SCANS", 5);
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fs = Arc::new(FlipStore { inner: inner.clone(), flip: AtomicU64::new(0) });
    let store: Arc<dyn ObjectStore> = fs.clone();
    let mut rng = StdRng::seed_from_u64(1107);
    // default sealed settings: 256 KiB blocks, 16 MiB parts
    let mut w = RunWriter::new(&store, Path::from("large/0"), RunOptions::sealed(CustomerKind::V2)).await.unwrap();
    let (mut n, mut u) = (0u64, 0i64);
    let mut stored = 0u64;
    while stored < target {
        for _ in 0..rng.gen_range(1..400) {
            w.add(&doc_key(u, "m", n), n, &random_env(&mut rng)).await.unwrap();
            n += 1;
            stored += 120;
        }
        u += 1;
    }
    let meta = w.finish().await.unwrap();
    let (rd, _) = RunReader::open(store.clone(), &[(meta.path.clone(), meta.size)], 0, 1).await.unwrap();
    let clean: Vec<Vec<Row>> = {
        let mut v = Vec::new();
        for b in 0..meta.blocks as usize {
            v.push(rd.read_block(0, b).await.unwrap());
        }
        v
    };
    let block_of = |byte: u64| meta.block_offsets.partition_point(|&o| o <= byte) - 1;
    let (mut caught, mut in_footer, mut other_ok) = (0u64, 0u64, 0u64);
    for _ in 0..flips {
        // one flip in ten lands in the footer or trailer, the rest anywhere
        let bit = if rng.gen_bool(0.1) { rng.gen_range(meta.data_end * 8..meta.size * 8) } else { rng.gen_range(0..meta.size * 8) };
        fs.flip.store(bit + 1, Relaxed);
        let byte = bit / 8;
        if byte >= meta.data_end {
            in_footer += 1;
            assert!(Run::open(&store, Path::from(meta.path.as_str()), meta.size).await.is_err(), "footer bit {bit} not caught at open");
            caught += 1;
        } else {
            let b = block_of(byte);
            assert!(rd.read_block(0, b).await.is_err(), "bit {bit} in block {b} not caught");
            caught += 1;
            // the block next to it still reads, and right
            let o = if b + 1 < meta.blocks as usize { b + 1 } else { b.saturating_sub(1) };
            if o != b {
                assert_eq!(rd.read_block(0, o).await.unwrap(), clean[o]);
                other_ok += 1;
            }
        }
    }
    let mut scans_caught = 0;
    for _ in 0..scans {
        let bit = rng.gen_range(0..meta.size * 8);
        fs.flip.store(bit + 1, Relaxed);
        assert!(stream_all(&store, &meta, 8 << 20).await.is_err(), "bit {bit}: the stream returned data");
        scans_caught += 1;
    }
    fs.flip.store(0, Relaxed);
    println!(
        "RUNFMT flips large: run of {} bytes ({} blocks of 256 KiB, {} rows): {caught} of {flips} random flips caught ({in_footer} in the footer or trailer, caught at open; the others by reading their block, and the neighbouring block read right {other_ok} times); full streaming scans with a random flip: {scans_caught} of {scans} were errors",
        meta.size,
        meta.blocks,
        meta.entries
    );
    assert_eq!(caught, flips);
}

// ---- review of step 1 (branch review-c2-step1)

/// Review: a fuzz over odd key shapes (0x00, 0xFF and '/' bytes, keys that are prefixes of others, the empty key), all
/// three customer kinds, blocks of 1 to 1,000 bytes (one row per block, restart groups cut anywhere) and 0 to 3 run cuts.
/// It passes on the delivered commit; it is here to judge the reviewer's extra mutations. RFUZZ=2000 for 600,000 reads.
fn rkey(rng: &mut StdRng, kind: CustomerKind) -> Vec<u8> {
    const A: &[u8] = &[0, 1, b'/', b'u', b'd', 0xFE, 0xFF, 3, 2];
    let tail: Vec<u8> = (0..rng.gen_range(0..6)).map(|_| A[rng.gen_range(0..A.len())]).collect();
    match (kind, rng.gen_range(0..4)) {
        (CustomerKind::V2, 0..=2) => {
            let l = rng.gen_range(1..4u8);
            let mut k = b"u/".to_vec();
            k.push(l);
            for _ in 0..l { k.push(A[rng.gen_range(0..A.len())]); }
            k.extend(tail);
            k
        }
        (CustomerKind::DocV1, 0..=2) => {
            let mut k = b"doc/".to_vec();
            k.extend([b"a".as_slice(), b"bb", b""][rng.gen_range(0..3)]);
            k.push(b'/');
            let l = rng.gen_range(1..4u8);
            k.push(l);
            for _ in 0..l { k.push(A[rng.gen_range(0..A.len())]); }
            k.extend(tail);
            k
        }
        _ => {
            let mut k: Vec<u8> = (0..rng.gen_range(0..5)).map(|_| A[rng.gen_range(0..A.len())]).collect();
            k.extend(tail);
            k
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn review_fuzz_odd_keys_match_an_exact_model() {
    let iters = env_or("RFUZZ", 300);
    let mut wrong = 0;
    for it in 0..iters {
        let mut rng = StdRng::seed_from_u64(it);
        let kind = [CustomerKind::None, CustomerKind::V2, CustomerKind::DocV1][(it % 3) as usize];
        let mut m = Model::new();
        for _ in 0..rng.gen_range(1..400) {
            let k = rkey(&mut rng, kind);
            m.insert(k, (rng.gen_range(0..1000), if rng.gen_bool(0.2) { deleted() } else { live(&vec![7u8; rng.gen_range(0..40)]) }));
        }
        let keys: Vec<Vec<u8>> = m.keys().cloned().collect();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ncut = rng.gen_range(0..4);
        let mut cuts: Vec<usize> = (0..ncut).map(|_| rng.gen_range(1..m.len().max(2))).filter(|&c| c < m.len()).collect();
        cuts.sort(); cuts.dedup();
        let block = [1usize, 20, 60, 200, 1000][rng.gen_range(0..5)];
        let metas = write_runs(&store, &m, &cuts, &RunOptions::unchecked(block, 1 << 20, kind), "z").await;
        let rd = open_all(&store, &metas, if rng.gen() { 0 } else { 1 << 16 }, rng.gen_range(1..4)).await;
        for _ in 0..300 {
            let probe = |rng: &mut StdRng| if rng.gen_bool(0.6) && !keys.is_empty() { let i = rng.gen_range(0..keys.len()); perturb(rng, &keys[i]) } else { rkey(rng, kind) };
            let op = rng.gen_range(0..4);
            let (got, want) = match op {
                0 => { let k = probe(&mut rng); (rd.get(&k).await.unwrap().0.into_iter().collect::<Vec<_>>(), m.get(&k).map(|(s, e)| vec![Row { key: k.clone(), seq: *s, env: e.clone() }]).unwrap_or_default()) }
                1 => { let lo = probe(&mut rng); let hi = if rng.gen_bool(0.1) { None } else { Some(probe(&mut rng)) }; (rd.range(&lo, hi.as_deref()).await.unwrap().0, model_range(&m, &lo, hi.as_deref())) }
                2 => { let mut p = probe(&mut rng); p.truncate(rng.gen_range(0..=p.len())); (rd.prefix(&p).await.unwrap().0, model_range(&m, &p, prefix_successor(&p).as_deref())) }
                _ => { let mut p = probe(&mut rng); p.truncate(rng.gen_range(0..=p.len())); let l = rng.gen_range(1..50); (rd.newest(&p, l).await.unwrap().0, model_newest(&m, &p, l)) }
            };
            if got != want { wrong += 1; if wrong < 5 { eprintln!("WRONG it {it} kind {kind:?} op {op} block {block}: got {} want {}", got.len(), want.len()); } }
        }
        for mt in &metas { assert_eq!(stream_all(&store, mt, 100).await.unwrap().len() as u64, mt.entries); }
    }
    assert_eq!(wrong, 0);
}

/// Review, FIX 1: a run's blocks are not bound to the run. The block CRC covers the block index and its bytes, the
/// footer CRC covers the footer, and nothing ties one to the other. So an object made of the blocks of run A and the
/// footer of run B (same key layout, one value changed in place, so the same block offsets) is read without an error,
/// and returns A's value under B's footer. This is the case of a path written twice (a retry, or a reseal that keeps the
/// same name), of a reader whose footer is older than the object at its path, or of a later writer that copies byte
/// ranges of unchanged blocks from the old run (UploadPartCopy) and gets one range wrong: stale rows, no error.
/// Expected: an error. A remedy: a random run id in the footer and the trailer, mixed into every block CRC.
#[tokio::test]
async fn review_blocks_of_one_run_with_the_footer_of_another_are_an_error() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut rng = StdRng::seed_from_u64(3);
    let mut a = Model::new();
    for n in 0..400u64 {
        let v: Vec<u8> = (0..60).map(|_| rng.gen()).collect();
        a.insert(doc_key((n / 40) as i64, "a", n), (n, live(&v)));
    }
    let opts = RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2);
    let ma = write_runs(&store, &a, &[], &opts, "A").await.remove(0);
    // run B: the same rows, one value changed in place (a reseal of the same group)
    let k = a.keys().nth(123).unwrap().clone();
    let mut b = a.clone();
    b.get_mut(&k).unwrap().1[1] ^= 0x55;
    let mb = write_runs(&store, &b, &[], &opts, "B").await.remove(0);
    assert!(ma.blocks > 10);
    assert_eq!((ma.block_offsets.clone(), ma.size), (mb.block_offsets.clone(), mb.size), "precondition: the same layout");
    // the object: A's blocks, then B's footer and trailer
    let oa = store.get(&Path::from(ma.path.as_str())).await.unwrap().bytes().await.unwrap();
    let ob = store.get(&Path::from(mb.path.as_str())).await.unwrap().bytes().await.unwrap();
    assert_ne!(oa[..ma.data_end as usize], ob[..mb.data_end as usize]);
    let mut spliced = oa[..ma.data_end as usize].to_vec();
    spliced.extend_from_slice(&ob[mb.data_end as usize..]);
    store.put(&Path::from("S/0000"), PutPayload::from(spliced)).await.unwrap();
    let ms = RunMeta { path: "S/0000".into(), ..mb.clone() };
    let random = random_all(&store, &ms).await;
    let stream = stream_all(&store, &ms, 4096).await;
    let show = |r: &anyhow::Result<Vec<Row>>| match r {
        Ok(rows) => format!("{} rows, equal to run A: {}, equal to run B: {}", rows.len(), rows == &model_range(&a, b"", None), rows == &model_range(&b, b"", None)),
        Err(e) => format!("error: {e}"),
    };
    assert!(random.is_err() && stream.is_err(), "blocks of A under the footer of B: random reader {}; stream {}", show(&random), show(&stream));
}

// ---- c2-fix-clone-uploads: an unfinished writer aborts its upload

/// Counts the uploads started, completed and aborted; a part fails while `fail_parts` is set.
struct UploadStore {
    inner: Arc<dyn ObjectStore>,
    started: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    aborted: Arc<AtomicU64>,
    fail_parts: Arc<std::sync::atomic::AtomicBool>,
}

impl fmt::Debug for UploadStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UploadStore")
    }
}

impl fmt::Display for UploadStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UploadStore")
    }
}

#[derive(Debug)]
struct CountedUpload {
    inner: Box<dyn MultipartUpload>,
    completed: Arc<AtomicU64>,
    aborted: Arc<AtomicU64>,
    fail_parts: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait]
impl MultipartUpload for CountedUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        if self.fail_parts.load(Relaxed) {
            return Box::pin(async { Err(object_store::Error::Generic { store: "test", source: "injected part failure".into() }) });
        }
        self.inner.put_part(data)
    }
    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let r = self.inner.complete().await;
        self.completed.fetch_add(1, Relaxed);
        r
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        self.aborted.fetch_add(1, Relaxed);
        self.inner.abort().await
    }
}

#[async_trait]
impl ObjectStore for UploadStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        let u = self.inner.put_multipart_opts(location, opts).await?;
        self.started.fetch_add(1, Relaxed);
        Ok(Box::new(CountedUpload { inner: u, completed: Arc::clone(&self.completed), aborted: Arc::clone(&self.aborted), fail_parts: Arc::clone(&self.fail_parts) }))
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(&self, locations: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// Every upload a run writer starts ends completed or aborted: dropped mid-run (a job cancelled or given up), failed on a
/// part, or refused at `finish` (an empty run). Before c2-fix-clone-uploads the three left the upload open.
#[tokio::test]
async fn every_upload_is_completed_or_aborted() {
    let u = UploadStore {
        inner: Arc::new(InMemory::new()),
        started: Default::default(),
        completed: Default::default(),
        aborted: Default::default(),
        fail_parts: Default::default(),
    };
    let (started, completed, aborted, fail) = (Arc::clone(&u.started), Arc::clone(&u.completed), Arc::clone(&u.aborted), Arc::clone(&u.fail_parts));
    let store: Arc<dyn ObjectStore> = Arc::new(u);
    let opts = RunOptions::unchecked(4096, 64 << 10, CustomerKind::V2);
    let mut rng = StdRng::seed_from_u64(11);
    let doc: Vec<u8> = (0..300).map(|_| rng.gen()).collect();
    // 1. dropped after parts were sent
    let mut w = RunWriter::new(&store, Path::from("u/1"), opts.clone()).await.unwrap();
    for n in 0..2000u64 {
        w.add(&doc_key((n / 100) as i64, "a", n), n, &live(&doc)).await.unwrap();
    }
    drop(w);
    // 2. a part fails
    let mut w = RunWriter::new(&store, Path::from("u/2"), opts.clone()).await.unwrap();
    fail.store(true, Relaxed);
    let mut failed = false;
    for n in 0..2000u64 {
        if w.add(&doc_key((n / 100) as i64, "a", n), n, &live(&doc)).await.is_err() {
            failed = true;
            break;
        }
    }
    assert!(failed, "the injected part failure never reached the writer");
    drop(w);
    fail.store(false, Relaxed);
    // 3. an empty run is refused at finish
    let w = RunWriter::new(&store, Path::from("u/3"), opts.clone()).await.unwrap();
    assert!(w.finish().await.is_err());
    // 4. a run that finishes is completed and not aborted
    let mut w = RunWriter::new(&store, Path::from("u/4"), opts.clone()).await.unwrap();
    w.add(b"k", 1, &live(b"x")).await.unwrap();
    w.finish().await.unwrap();
    // 5. an explicit abort is one abort
    let mut w = RunWriter::new(&store, Path::from("u/5"), opts).await.unwrap();
    w.add(b"k", 1, &live(b"x")).await.unwrap();
    w.abort().await.unwrap();
    let t0 = std::time::Instant::now();
    while aborted.load(Relaxed) < 4 && t0.elapsed() < std::time::Duration::from_secs(5) {
        tokio::task::yield_now().await;
    }
    let (s, c, a) = (started.load(Relaxed), completed.load(Relaxed), aborted.load(Relaxed));
    println!("RUNFMT uploads: started {s}, completed {c}, aborted {a}");
    assert_eq!(s, 5);
    assert_eq!(c, 1, "only the finished run completes");
    assert_eq!(a, 4, "the dropped, failed, refused and aborted runs abort, once each");
    assert!(store.head(&Path::from("u/4")).await.is_ok());
    for p in ["u/1", "u/2", "u/3", "u/5"] {
        assert!(store.head(&Path::from(p)).await.is_err(), "{p} is visible");
    }
}
